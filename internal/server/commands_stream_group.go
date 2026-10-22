package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// xgroupHelp is the help of the XGROUP command: Redis answers with one
// element per line.
var xgroupHelp = []string{
	"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CREATE <key> <groupname> <id|$> [option]",
	"    Create a new consumer group. Options are:",
	"    * MKSTREAM",
	"      Create the empty stream if it does not exist.",
	"    * ENTRIESREAD entries_read",
	"      Set the group's entries_read counter (internal use).",
	"CREATECONSUMER <key> <groupname> <consumer>",
	"    Create a new consumer in the specified group.",
	"DELCONSUMER <key> <groupname> <consumer>",
	"    Remove the specified consumer.",
	"DESTROY <key> <groupname>",
	"    Remove the specified group.",
	"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
	"    Set the current group ID and entries_read counter.",
	"HELP",
	"    Print this help.",
}

// xinfoHelp is the help of the XINFO command: Redis answers with one element
// per line.
var xinfoHelp = []string{
	"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CONSUMERS <key> <groupname>",
	"    Show consumers of <groupname>.",
	"GROUPS <key>",
	"    Show the stream consumer groups.",
	"STREAM <key> [FULL [COUNT <count>]",
	"    Show information about the stream.",
	"HELP",
	"    Print this help.",
}

// writeHelp writes a help text: one element per line, like Redis does.
func writeHelp(w *resp.Writer, lines []string) error {
	if err := w.WriteArrayHeader(len(lines)); err != nil {
		return err
	}
	for _, line := range lines {
		if err := w.WriteSimpleString(line); err != nil {
			return err
		}
	}
	return nil
}

// cmdXGroup manages the consumer groups of a stream.
func cmdXGroup(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "xgroup")
		return nil
	}
	sub := strings.ToUpper(string(ctx.Args[1]))
	switch sub {
	case "HELP":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "xgroup")
			return nil
		}
		return writeHelp(ctx.W, xgroupHelp)
	case "CREATE":
		return xGroupCreate(ctx)
	case "DESTROY":
		return xGroupDestroy(ctx)
	case "CREATECONSUMER":
		return xGroupCreateConsumer(ctx)
	case "DELCONSUMER":
		return xGroupDelConsumer(ctx)
	case "SETID":
		return xGroupSetID(ctx)
	default:
		return ctx.W.WriteError("ERR Unknown XGROUP subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}

// xGroupCreate implements XGROUP CREATE.
func xGroupCreate(ctx *Ctx) error {
	cmd, err := store.ParseStreamGroupArgs(ctx.Args[1:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	id := cmd.ID
	if cmd.IDIsDollar {
		last, ok, err := ctx.Store.StreamLastID(cmd.Key)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		if ok {
			id = last
		}
	}
	created, err := ctx.Store.StreamGroupCreate(cmd.Key, cmd.Group, id, cmd.EntriesRead, cmd.SetEntriesRead, cmd.MkStream)
	switch err {
	case nil:
	case store.ErrStreamNotFound:
		return ctx.W.WriteError("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if !created {
		return ctx.W.WriteError("BUSYGROUP Consumer Group name already exists")
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// xGroupDestroy implements XGROUP DESTROY.
func xGroupDestroy(ctx *Ctx) error {
	cmd, err := store.ParseStreamGroupArgs(ctx.Args[1:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	destroyed, err := ctx.Store.StreamGroupDestroy(cmd.Key, cmd.Group)
	switch err {
	case nil:
	case store.ErrStreamNotFound:
		return ctx.W.WriteError(errXGroupNeedsKey)
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if destroyed {
		ctx.propagateCmd()
		return ctx.W.WriteInteger(1)
	}
	return ctx.W.WriteInteger(0)
}

// xGroupCreateConsumer implements XGROUP CREATECONSUMER.
func xGroupCreateConsumer(ctx *Ctx) error {
	cmd, err := store.ParseStreamGroupArgs(ctx.Args[1:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	created, err := ctx.Store.StreamGroupCreateConsumer(cmd.Key, cmd.Group, cmd.Consumer)
	switch err {
	case nil:
	case store.ErrStreamNotFound:
		return ctx.W.WriteError(errXGroupNeedsKey)
	case store.ErrGroupNotFound:
		return ctx.W.WriteError(errNoGroup(cmd.Key, cmd.Group))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if created {
		ctx.propagateCmd()
		return ctx.W.WriteInteger(1)
	}
	return ctx.W.WriteInteger(0)
}

// xGroupDelConsumer implements XGROUP DELCONSUMER.
func xGroupDelConsumer(ctx *Ctx) error {
	cmd, err := store.ParseStreamGroupArgs(ctx.Args[1:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	pending, err := ctx.Store.StreamGroupDelConsumer(cmd.Key, cmd.Group, cmd.Consumer)
	switch err {
	case nil:
	case store.ErrStreamNotFound:
		return ctx.W.WriteError(errXGroupNeedsKey)
	case store.ErrGroupNotFound:
		return ctx.W.WriteError(errNoGroup(cmd.Key, cmd.Group))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if pending > 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(pending))
}

// xGroupSetID implements XGROUP SETID.
func xGroupSetID(ctx *Ctx) error {
	cmd, err := store.ParseStreamGroupArgs(ctx.Args[1:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	id := cmd.ID
	if cmd.IDIsDollar {
		last, ok, err := ctx.Store.StreamLastID(cmd.Key)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		if ok {
			id = last
		}
	}
	if err := ctx.Store.StreamGroupSetID(cmd.Key, cmd.Group, id, cmd.EntriesRead, cmd.SetEntriesRead); err != nil {
		switch err {
		case store.ErrStreamNotFound:
			return ctx.W.WriteError(errXGroupNeedsKey)
		case store.ErrGroupNotFound:
			return ctx.W.WriteError(errNoGroup(cmd.Key, cmd.Group))
		case store.ErrWrongType:
			return writeWrongType(ctx.W)
		default:
			return ctx.W.WriteError("ERR " + err.Error())
		}
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// errXGroupNeedsKey is the error of the XGROUP subcommands that need the key
// of the stream to exist.
const errXGroupNeedsKey = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."

// errNoGroup returns the error of the commands that need a consumer group.
func errNoGroup(key []byte, group string) string {
	return "NOGROUP No such consumer group '" + group + "' for key name '" + string(key) + "'"
}

// errNoGroupKey returns the error of the commands that need a stream with a
// consumer group.
func errNoGroupKey(key []byte, group string) string {
	return "NOGROUP No such key '" + string(key) + "' or consumer group '" + group + "'"
}

// cmdXAck acknowledges the entries of a consumer group.
func cmdXAck(ctx *Ctx) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, "xack")
		return nil
	}
	key := ctx.Args[1]
	group := string(ctx.Args[2])
	ids := make([]store.StreamID, 0, len(ctx.Args)-3)
	for _, arg := range ctx.Args[3:] {
		id, ok := store.ParseStreamIDStrict(string(arg), 0, nil)
		if !ok {
			return ctx.W.WriteError(errInvalidStreamID.Error())
		}
		ids = append(ids, id)
	}
	n, err := ctx.Store.StreamAck(key, group, ids)
	switch err {
	case nil:
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if n > 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdXClaim transfers the pending entries of a consumer group to another
// consumer.
func cmdXClaim(ctx *Ctx) error {
	cmd, err := store.ParseStreamClaimArgs(ctx.Args[1:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	res, err := ctx.Store.StreamClaim(cmd.Key, cmd.Group, cmd.Consumer, cmd.MinIdle, cmd.IDs, cmd.Options)
	switch err {
	case nil:
	case store.ErrStreamNotFound, store.ErrGroupNotFound:
		return ctx.W.WriteError(errNoGroupKey(cmd.Key, cmd.Group))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	// The changes of the pending list are written to the AOF.
	propagateClaims(ctx, cmd.Key, cmd.Group, res.Pending, res.Deleted)
	if res.LastIDChanged {
		propagateGroupID(ctx, cmd.Key, cmd.Group, res.GroupLastID, res.EntriesRead)
	}
	if cmd.Options.JustID {
		if err := ctx.W.WriteArrayHeader(len(res.IDs)); err != nil {
			return err
		}
		for _, id := range res.IDs {
			if err := ctx.W.WriteBulkString([]byte(id.String())); err != nil {
				return err
			}
		}
		return nil
	}
	if err := ctx.W.WriteArrayHeader(len(res.Entries)); err != nil {
		return err
	}
	for _, entry := range res.Entries {
		if err := writeStreamEntry(ctx.W, entry); err != nil {
			return err
		}
	}
	return nil
}

// cmdXAutoClaim claims the pending entries of a consumer group that have been
// idle for too long.
func cmdXAutoClaim(ctx *Ctx) error {
	if len(ctx.Args) < 6 {
		wrongArgs(ctx.W, "xautoclaim")
		return nil
	}
	key := ctx.Args[1]
	group := string(ctx.Args[2])
	consumer := string(ctx.Args[3])
	minIdle, err := strconv.ParseInt(string(ctx.Args[4]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR Invalid min-idle-time argument for XAUTOCLAIM")
	}
	if minIdle < 0 {
		minIdle = 0
	}
	start, startEx, err := parseStreamInterval(ctx.Args[5], 0)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	if startEx {
		start = streamIDNext(start)
	}
	count := 100
	justID := false
	for i := 6; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "COUNT":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError(errStreamSyntax.Error())
			}
			n, err := strconv.ParseInt(string(ctx.Args[i+1]), 10, 64)
			if err != nil {
				return ctx.W.WriteError("ERR value is not an integer or out of range")
			}
			if n < 1 {
				return ctx.W.WriteError("ERR COUNT must be > 0")
			}
			count = int(n)
			i += 2
		case "JUSTID":
			justID = true
			i++
		default:
			return ctx.W.WriteError(errStreamSyntax.Error())
		}
	}
	res, err := ctx.Store.StreamAutoClaim(key, group, consumer, time.Duration(minIdle)*time.Millisecond, start, count, justID)
	switch err {
	case nil:
	case store.ErrStreamNotFound, store.ErrGroupNotFound:
		return ctx.W.WriteError(errNoGroupKey(key, group))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if res.ConsumerCreated {
		ctx.propagate([][]byte{[]byte("XGROUP"), []byte("CREATECONSUMER"), key, []byte(group), []byte(consumer)})
	}
	propagateClaims(ctx, key, group, res.Pending, res.Deleted)

	// The reply holds the cursor, the claimed entries and the deleted ones.
	if err := ctx.W.WriteArrayHeader(3); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte(res.Cursor.String())); err != nil {
		return err
	}
	if justID {
		if err := ctx.W.WriteArrayHeader(len(res.IDs)); err != nil {
			return err
		}
		for _, id := range res.IDs {
			if err := ctx.W.WriteBulkString([]byte(id.String())); err != nil {
				return err
			}
		}
	} else {
		if err := ctx.W.WriteArrayHeader(len(res.Entries)); err != nil {
			return err
		}
		for _, entry := range res.Entries {
			if err := writeStreamEntry(ctx.W, entry); err != nil {
				return err
			}
		}
	}
	if err := ctx.W.WriteArrayHeader(len(res.Deleted)); err != nil {
		return err
	}
	for _, id := range res.Deleted {
		if err := ctx.W.WriteBulkString([]byte(id.ID.String())); err != nil {
			return err
		}
	}
	return nil
}

// propagateClaims writes the changes of the pending list to the AOF, the way
// Redis does: one XCLAIM per entry.
func propagateClaims(ctx *Ctx, key []byte, group string, pending []store.StreamPendingEntry, deleted []store.StreamPendingEntry) {
	for _, p := range deleted {
		consumer := p.Consumer
		if consumer == "" {
			consumer = "-"
		}
		ctx.propagate([][]byte{
			[]byte("XCLAIM"), key, []byte(group), []byte(consumer), []byte("0"),
			[]byte(p.ID.String()),
			[]byte("TIME"), []byte(strconv.FormatInt(p.DeliveryTime.UnixMilli(), 10)),
			[]byte("RETRYCOUNT"), []byte(strconv.FormatInt(p.DeliveryCount, 10)),
			[]byte("FORCE"), []byte("JUSTID"),
		})
	}
	for _, p := range pending {
		ctx.propagate([][]byte{
			[]byte("XCLAIM"), key, []byte(group), []byte(p.Consumer), []byte("0"),
			[]byte(p.ID.String()),
			[]byte("TIME"), []byte(strconv.FormatInt(p.DeliveryTime.UnixMilli(), 10)),
			[]byte("RETRYCOUNT"), []byte(strconv.FormatInt(p.DeliveryCount, 10)),
			[]byte("FORCE"), []byte("JUSTID"),
		})
	}
}

// propagateGroupID writes the identifier of the last entry delivered to a
// group to the AOF.
func propagateGroupID(ctx *Ctx, key []byte, group string, id store.StreamID, entriesRead uint64) {
	ctx.propagate([][]byte{
		[]byte("XGROUP"), []byte("SETID"), key, []byte(group), []byte(id.String()),
		[]byte("ENTRIESREAD"), []byte(strconv.FormatUint(entriesRead, 10)),
	})
}

// cmdXPending reports the pending entries of a consumer group.
func cmdXPending(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "xpending")
		return nil
	}
	key := ctx.Args[1]
	group := string(ctx.Args[2])
	// XPENDING key group: the summary of the pending list.
	if len(ctx.Args) == 3 {
		info, ok, err := ctx.Store.StreamPendingInfo(key, group)
		switch err {
		case nil:
		case store.ErrGroupNotFound:
			return ctx.W.WriteError(errNoGroupKey(key, group))
		case store.ErrWrongType:
			return writeWrongType(ctx.W)
		default:
			return ctx.W.WriteError("ERR " + err.Error())
		}
		if !ok {
			return ctx.W.WriteError(errNoGroupKey(key, group))
		}
		if err := ctx.W.WriteArrayHeader(4); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(int64(info.Count)); err != nil {
			return err
		}
		if info.Count == 0 {
			if err := ctx.W.WriteNullBulk(); err != nil {
				return err
			}
			if err := ctx.W.WriteNullBulk(); err != nil {
				return err
			}
			return ctx.W.WriteNullArray()
		}
		if err := ctx.W.WriteBulkString([]byte(info.Min.String())); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(info.Max.String())); err != nil {
			return err
		}
		if err := ctx.W.WriteArrayHeader(len(info.Consumers)); err != nil {
			return err
		}
		for _, c := range info.Consumers {
			if err := ctx.W.WriteArrayHeader(2); err != nil {
				return err
			}
			if err := ctx.W.WriteBulkString([]byte(c.Name)); err != nil {
				return err
			}
			// Redis writes the number of pending entries as a bulk string.
			if err := ctx.W.WriteBulkString([]byte(strconv.Itoa(c.Count))); err != nil {
				return err
			}
		}
		return nil
	}
	// XPENDING key group [IDLE ms] start end count [consumer]
	minIdle := int64(0)
	i := 3
	if strings.EqualFold(string(ctx.Args[i]), "IDLE") {
		if i+1 >= len(ctx.Args) {
			return ctx.W.WriteError(errStreamSyntax.Error())
		}
		n, err := strconv.ParseInt(string(ctx.Args[i+1]), 10, 64)
		if err != nil {
			return ctx.W.WriteError("ERR value is not an integer or out of range")
		}
		minIdle = n
		i += 2
	}
	if i+2 >= len(ctx.Args) {
		return ctx.W.WriteError(errStreamSyntax.Error())
	}
	start, startEx, err := parseStreamInterval(ctx.Args[i], 0)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	end, endEx, err := parseStreamInterval(ctx.Args[i+1], ^uint64(0))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	if startEx {
		start = streamIDNext(start)
	}
	if endEx {
		end = streamIDPrev(end)
	}
	count, err := strconv.ParseInt(string(ctx.Args[i+2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	if count < 0 {
		count = 0
	}
	consumer := ""
	if i+3 < len(ctx.Args) {
		consumer = string(ctx.Args[i+3])
	}
	entries, ok, err := ctx.Store.StreamPendingRange(key, group, start, end, int(count), consumer, time.Duration(minIdle)*time.Millisecond)
	switch err {
	case nil:
	case store.ErrGroupNotFound:
		return ctx.W.WriteError(errNoGroupKey(key, group))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if !ok {
		return ctx.W.WriteError(errNoGroupKey(key, group))
	}
	if err := ctx.W.WriteArrayHeader(len(entries)); err != nil {
		return err
	}
	now := ctx.Store.Now()
	for _, e := range entries {
		if err := ctx.W.WriteArrayHeader(4); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(e.ID.String())); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(e.Consumer)); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(int64(now.Sub(e.DeliveryTime) / time.Millisecond)); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(e.DeliveryCount); err != nil {
			return err
		}
	}
	return nil
}

// cmdXInfo reports the state of a stream, of its groups or of their
// consumers.
func cmdXInfo(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "xinfo")
		return nil
	}
	switch strings.ToUpper(string(ctx.Args[1])) {
	case "HELP":
		return writeHelp(ctx.W, xinfoHelp)
	case "STREAM":
		return xInfoStream(ctx)
	case "GROUPS":
		return xInfoGroups(ctx)
	case "CONSUMERS":
		return xInfoConsumers(ctx)
	default:
		return ctx.W.WriteError("ERR Unknown XINFO subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}

// xInfoStream implements XINFO STREAM.
func xInfoStream(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "xinfo")
		return nil
	}
	key := ctx.Args[2]
	full := false
	count := 0
	if len(ctx.Args) > 3 {
		if !strings.EqualFold(string(ctx.Args[3]), "FULL") {
			return ctx.W.WriteError(errStreamSyntax.Error())
		}
		full = true
		if len(ctx.Args) > 4 {
			if !strings.EqualFold(string(ctx.Args[4]), "COUNT") || len(ctx.Args) < 6 {
				return ctx.W.WriteError(errStreamSyntax.Error())
			}
			n, err := strconv.Atoi(string(ctx.Args[5]))
			if err != nil {
				return ctx.W.WriteError("ERR value is not an integer or out of range")
			}
			if n < 0 {
				n = 10
			}
			count = n
		}
	}
	info, ok, err := ctx.Store.StreamInfo(key)
	switch err {
	case nil:
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if !ok {
		return ctx.W.WriteError("ERR no such key")
	}
	if !full {
		fields := [][2]interface{}{
			{"length", int64(info.Length)},
			{"radix-tree-keys", int64(radixTreeKeys(info.Length))},
			{"radix-tree-nodes", int64(radixTreeNodes(info.Length))},
			{"last-generated-id", info.LastID.String()},
			{"max-deleted-entry-id", info.MaxDeletedID.String()},
			{"entries-added", int64(info.EntriesAdded)},
			{"recorded-first-entry-id", info.RecordedFirstID.String()},
			{"groups", int64(info.Groups)},
		}
		if err := ctx.W.WriteArrayHeader(len(fields)*2 + 4); err != nil {
			return err
		}
		for _, f := range fields {
			if err := ctx.W.WriteBulkString([]byte(f[0].(string))); err != nil {
				return err
			}
			if err := writeStreamInfoValue(ctx, f[1]); err != nil {
				return err
			}
		}
		// The first and the last entry of the stream.
		if err := ctx.W.WriteBulkString([]byte("first-entry")); err != nil {
			return err
		}
		if err := writeStreamEntryOrNull(ctx, info.FirstEntry); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("last-entry")); err != nil {
			return err
		}
		return writeStreamEntryOrNull(ctx, info.LastEntry)
	}
	// XINFO STREAM key FULL [COUNT count]
	entries, err := ctx.Store.StreamRange(key, store.StreamID{}, store.StreamMaxID, count, false)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if err := ctx.W.WriteArrayHeader(18); err != nil {
		return err
	}
	fields := [][2]interface{}{
		{"length", int64(info.Length)},
		{"radix-tree-keys", int64(radixTreeKeys(info.Length))},
		{"radix-tree-nodes", int64(radixTreeNodes(info.Length))},
		{"last-generated-id", info.LastID.String()},
		{"max-deleted-entry-id", info.MaxDeletedID.String()},
		{"entries-added", int64(info.EntriesAdded)},
		{"recorded-first-entry-id", info.RecordedFirstID.String()},
	}
	for _, f := range fields {
		if err := ctx.W.WriteBulkString([]byte(f[0].(string))); err != nil {
			return err
		}
		if err := writeStreamInfoValue(ctx, f[1]); err != nil {
			return err
		}
	}
	// The entries of the stream.
	if err := ctx.W.WriteBulkString([]byte("entries")); err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(len(entries)); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeStreamEntry(ctx.W, entry); err != nil {
			return err
		}
	}
	// The consumer groups with their pending entries.
	groups, _, err := ctx.Store.StreamGroupsInfo(key)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if err := ctx.W.WriteBulkString([]byte("groups")); err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(len(groups)); err != nil {
		return err
	}
	for _, g := range groups {
		if err := writeStreamFullGroup(ctx, key, g, count); err != nil {
			return err
		}
	}
	return nil
}

// radixTreeKeys returns the number of the macro nodes of a stream. Redis keeps
// the entries in a radix tree of listpacks; we keep them in a single slice, so
// the value matches Redis for the small streams.
func radixTreeKeys(length int) int {
	if length == 0 {
		return 0
	}
	return 1
}

// radixTreeNodes returns the number of the nodes of the radix tree of a
// stream.
func radixTreeNodes(length int) int {
	return radixTreeKeys(length) + 1
}

// writeStreamInfoValue writes a value of the XINFO reply.
func writeStreamInfoValue(ctx *Ctx, v interface{}) error {
	switch value := v.(type) {
	case int64:
		return ctx.W.WriteInteger(value)
	case string:
		return ctx.W.WriteBulkString([]byte(value))
	default:
		return ctx.W.WriteBulkString([]byte(""))
	}
}

// writeStreamEntryOrNull writes an entry of a stream, or a null when the
// stream is empty.
func writeStreamEntryOrNull(ctx *Ctx, entry *store.StreamEntry) error {
	if entry == nil {
		return ctx.W.WriteNullBulk()
	}
	return writeStreamEntry(ctx.W, *entry)
}

// writeStreamFullGroup writes a consumer group with its pending entries and
// its consumers (the XINFO STREAM FULL reply).
func writeStreamFullGroup(ctx *Ctx, key []byte, g store.StreamGroupInfo, count int) error {
	pending, _, err := ctx.Store.StreamPendingRange(key, g.Name, store.StreamID{}, store.StreamMaxID, count, "", 0)
	if err != nil {
		return err
	}
	consumers, _, err := ctx.Store.StreamConsumersInfo(key, g.Name)
	if err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(14); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte("name")); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte(g.Name)); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte("last-delivered-id")); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte(g.LastID.String())); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte("entries-read")); err != nil {
		return err
	}
	if g.EntriesReadValid {
		if err := ctx.W.WriteInteger(int64(g.EntriesRead)); err != nil {
			return err
		}
	} else if err := ctx.W.WriteNullBulk(); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte("lag")); err != nil {
		return err
	}
	if g.LagValid {
		if err := ctx.W.WriteInteger(g.Lag); err != nil {
			return err
		}
	} else if err := ctx.W.WriteNullBulk(); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte("pel-count")); err != nil {
		return err
	}
	if err := ctx.W.WriteInteger(int64(g.Pending)); err != nil {
		return err
	}
	// The pending entries of the group.
	if err := ctx.W.WriteBulkString([]byte("pending")); err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(len(pending)); err != nil {
		return err
	}
	for _, p := range pending {
		if err := ctx.W.WriteArrayHeader(4); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(p.ID.String())); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(p.Consumer)); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(p.DeliveryTime.UnixMilli()); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(p.DeliveryCount); err != nil {
			return err
		}
	}
	// The consumers of the group.
	if err := ctx.W.WriteBulkString([]byte("consumers")); err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(len(consumers)); err != nil {
		return err
	}
	for _, c := range consumers {
		cp, _, err := ctx.Store.StreamConsumerPending(key, g.Name, c.Name, count)
		if err != nil {
			return err
		}
		if err := ctx.W.WriteArrayHeader(10); err != nil {
			return err
		}
		activeTime := int64(-1)
		if !c.ActiveTime.IsZero() {
			activeTime = c.ActiveTime.UnixMilli()
		}
		cf := [][2]interface{}{
			{"name", c.Name},
			{"seen-time", c.SeenTime.UnixMilli()},
			{"active-time", activeTime},
			{"pel-count", int64(c.Pending)},
		}
		for _, f := range cf {
			if err := ctx.W.WriteBulkString([]byte(f[0].(string))); err != nil {
				return err
			}
			if err := writeStreamInfoValue(ctx, f[1]); err != nil {
				return err
			}
		}
		if err := ctx.W.WriteBulkString([]byte("pending")); err != nil {
			return err
		}
		if err := ctx.W.WriteArrayHeader(len(cp)); err != nil {
			return err
		}
		for _, p := range cp {
			if err := ctx.W.WriteArrayHeader(3); err != nil {
				return err
			}
			if err := ctx.W.WriteBulkString([]byte(p.ID.String())); err != nil {
				return err
			}
			if err := ctx.W.WriteInteger(p.DeliveryTime.UnixMilli()); err != nil {
				return err
			}
			if err := ctx.W.WriteInteger(p.DeliveryCount); err != nil {
				return err
			}
		}
	}
	return nil
}

// xInfoGroups implements XINFO GROUPS.
func xInfoGroups(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "xinfo")
		return nil
	}
	key := ctx.Args[2]
	groups, ok, err := ctx.Store.StreamGroupsInfo(key)
	switch err {
	case nil:
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if !ok {
		return ctx.W.WriteError("ERR no such key")
	}
	if err := ctx.W.WriteArrayHeader(len(groups)); err != nil {
		return err
	}
	for _, g := range groups {
		if err := ctx.W.WriteArrayHeader(12); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("name")); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(g.Name)); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("consumers")); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(int64(g.Consumers)); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("pending")); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(int64(g.Pending)); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("last-delivered-id")); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(g.LastID.String())); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("entries-read")); err != nil {
			return err
		}
		if g.EntriesReadValid {
			if err := ctx.W.WriteInteger(int64(g.EntriesRead)); err != nil {
				return err
			}
		} else if err := ctx.W.WriteNullBulk(); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("lag")); err != nil {
			return err
		}
		if g.LagValid {
			if err := ctx.W.WriteInteger(g.Lag); err != nil {
				return err
			}
		} else if err := ctx.W.WriteNullBulk(); err != nil {
			return err
		}
	}
	return nil
}

// xInfoConsumers implements XINFO CONSUMERS.
func xInfoConsumers(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "xinfo")
		return nil
	}
	key := ctx.Args[2]
	group := string(ctx.Args[3])
	consumers, ok, err := ctx.Store.StreamConsumersInfo(key, group)
	switch err {
	case nil:
	case store.ErrGroupNotFound:
		return ctx.W.WriteError(errNoGroup(key, group))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if !ok {
		return ctx.W.WriteError("ERR no such key")
	}
	if err := ctx.W.WriteArrayHeader(len(consumers)); err != nil {
		return err
	}
	for _, c := range consumers {
		if err := ctx.W.WriteArrayHeader(8); err != nil {
			return err
		}
		fields := [][2]interface{}{
			{"name", c.Name},
			{"pending", int64(c.Pending)},
			{"idle", c.Idle.Milliseconds()},
			{"inactive", c.Inactive.Milliseconds()},
		}
		for _, f := range fields {
			if err := ctx.W.WriteBulkString([]byte(f[0].(string))); err != nil {
				return err
			}
			if err := writeStreamInfoValue(ctx, f[1]); err != nil {
				return err
			}
		}
	}
	return nil
}
