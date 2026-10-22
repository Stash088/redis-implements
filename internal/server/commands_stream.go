package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// errInvalidStreamID is the error Redis replies with for a malformed stream
// identifier.
var errInvalidStreamID = errors.New("ERR Invalid stream ID specified as stream command argument")

// errStreamSyntax is the generic syntax error of the stream commands.
var errStreamSyntax = errors.New("ERR syntax error")

// writeStreamError replies with the error of a stream command, mapping the
// errors of the store to the messages Redis uses.
func writeStreamError(w *resp.Writer, err error) error {
	switch err {
	case store.ErrInvalidStreamID:
		return w.WriteError(errInvalidStreamID.Error())
	case store.ErrStreamSyntax:
		return w.WriteError(errStreamSyntax.Error())
	case store.ErrStreamNotInteger:
		return w.WriteError("ERR value is not an integer or out of range")
	case store.ErrStreamMaxLenNegative:
		return w.WriteError("ERR The MAXLEN argument must be >= 0.")
	case store.ErrStreamLimitNegative:
		return w.WriteError("ERR The LIMIT argument must be >= 0.")
	case store.ErrStreamTrimConflict:
		return w.WriteError("ERR syntax error, MAXLEN and MINID options at the same time are not compatible")
	case store.ErrStreamLimitNoStrategy:
		return w.WriteError("ERR syntax error, LIMIT cannot be used without specifying a trimming strategy")
	case store.ErrStreamLimitNoApprox:
		return w.WriteError("ERR syntax error, LIMIT cannot be used without the special ~ option")
	case store.ErrStreamNoStrategy:
		return w.WriteError("ERR syntax error, XTRIM must be called with a trimming strategy")
	case store.ErrStreamEntriesAddedTooSmall:
		return w.WriteError("ERR The entries_added specified in XSETID is smaller than the target stream length")
	case store.ErrStreamMaxDeletedTooSmall:
		return w.WriteError("ERR The ID specified in XSETID is smaller than current max_deleted_entry_id")
	case store.ErrStreamSetIDMaxDeleted:
		return w.WriteError("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id")
	case store.ErrWrongType:
		return writeWrongType(w)
	default:
		return w.WriteError("ERR " + err.Error())
	}
}

// parseStreamInterval parses the boundary of a range query. The exclusive form
// is written as "(" followed by the identifier, like "(5-0".
func parseStreamInterval(arg []byte, missingSeq uint64) (store.StreamID, bool, error) {
	s := string(arg)
	exclusive := strings.HasPrefix(s, "(")
	if exclusive {
		s = s[1:]
	}
	var (
		id store.StreamID
		ok bool
	)
	if exclusive {
		id, ok = store.ParseStreamIDStrict(s, missingSeq, nil)
	} else {
		id, ok = store.ParseStreamID(s, missingSeq)
	}
	if !ok {
		return id, false, errInvalidStreamID
	}
	return id, exclusive, nil
}

// cmdXAdd appends an entry to a stream.
func cmdXAdd(ctx *Ctx) error {
	if len(ctx.Args) < 5 {
		wrongArgs(ctx.W, "xadd")
		return nil
	}
	key := ctx.Args[1]
	args, err := store.ParseStreamAddArgs(ctx.Args[2:])
	if err != nil {
		if err == store.ErrStreamSyntax {
			wrongArgs(ctx.W, "xadd")
			return nil
		}
		return writeStreamError(ctx.W, err)
	}
	// Redis refuses the identifier 0-0 before touching the dataset.
	if args.Mode == store.StreamIDExplicit && args.ID.MS == 0 && args.ID.Seq == 0 {
		return ctx.W.WriteError("ERR The ID specified in XADD must be greater than 0-0")
	}

	newID, err := ctx.Store.StreamAdd(key, args.Mode, args.ID, args.Fields, args.NoMkStream)
	switch err {
	case nil:
	case store.ErrStreamNotCreated:
		return ctx.W.WriteNullBulk()
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrStreamIDNotGreater:
		return ctx.W.WriteError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}

	// Trim the stream, when the command asked for it.
	propTrim := args.Trim
	if args.HasTrim {
		removed, err := ctx.Store.StreamTrim(key, args.Trim)
		if err != nil && err != store.ErrWrongType {
			return ctx.W.WriteError("ERR " + err.Error())
		}
		if args.Trim.Approximate {
			// An approximate trimming removes whole chunks of entries, so the
			// result depends on the layout of the stream. Like Redis, the AOF
			// gets the exact trimming that has the same effect.
			propTrim = equivalentTrim(ctx, key, args.Trim, removed)
		}
	}

	// The AOF gets the identifier that was actually used, so that the replay
	// produces the same stream.
	prop := make([][]byte, 0, len(ctx.Args))
	prop = append(prop, []byte("XADD"), key)
	prop = append(prop, streamTrimArgs(propTrim, args.HasTrim)...)
	prop = append(prop, []byte(newID.String()))
	prop = append(prop, args.Fields...)
	ctx.propagate(prop)

	return ctx.W.WriteBulkString([]byte(newID.String()))
}

// equivalentTrim returns the exact trimming that has the same effect as the
// approximate trimming that was just applied to the stream.
func equivalentTrim(ctx *Ctx, key []byte, trim store.StreamTrim, removed int) store.StreamTrim {
	out := store.StreamTrim{}
	switch {
	case trim.MaxLen != nil:
		n, ok, err := ctx.Store.StreamLen(key)
		if err != nil || !ok {
			return trim
		}
		length := uint64(n)
		out.MaxLen = &length
	case trim.MinID != nil:
		entries, err := ctx.Store.StreamRange(key, store.StreamID{}, store.StreamMaxID, 1, false)
		if err != nil || len(entries) == 0 {
			// The stream is empty: any identifier removes nothing.
			out.MinID = trim.MinID
			return out
		}
		id := entries[0].ID
		out.MinID = &id
	default:
		return trim
	}
	return out
}

// streamTrimArgs returns the trimming options of a command, as they are
// written to the AOF.
func streamTrimArgs(trim store.StreamTrim, hasTrim bool) [][]byte {
	if !hasTrim {
		return nil
	}
	var args [][]byte
	switch {
	case trim.MaxLen != nil:
		args = append(args, []byte("MAXLEN"), []byte("="), []byte(strconv.FormatUint(*trim.MaxLen, 10)))
	case trim.MinID != nil:
		args = append(args, []byte("MINID"), []byte("="), []byte(trim.MinID.String()))
	}
	if trim.Limit > 0 {
		args = append(args, []byte("LIMIT"), []byte(strconv.Itoa(trim.Limit)))
	}
	return args
}

// cmdXLen returns the number of entries of a stream.
func cmdXLen(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "xlen")
		return nil
	}
	n, ok, err := ctx.Store.StreamLen(ctx.Args[1])
	switch err {
	case nil:
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if !ok {
		return ctx.W.WriteInteger(0)
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdXRange returns a range of entries of a stream.
func cmdXRange(ctx *Ctx) error {
	return xRangeCommon(ctx, "xrange", false)
}

// cmdXRevRange returns a range of entries of a stream, in reverse order.
func cmdXRevRange(ctx *Ctx) error {
	return xRangeCommon(ctx, "xrevrange", true)
}

// xRangeCommon implements XRANGE and XREVRANGE.
func xRangeCommon(ctx *Ctx, cmd string, rev bool) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	key := ctx.Args[1]
	// XREVRANGE takes the boundaries in the opposite order.
	first, second := ctx.Args[2], ctx.Args[3]
	if rev {
		first, second = second, first
	}
	start, startEx, err := parseStreamInterval(first, 0)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	end, endEx, err := parseStreamInterval(second, ^uint64(0))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	if startEx {
		start = streamIDNext(start)
	}
	if endEx {
		end = streamIDPrev(end)
	}

	count := -1
	for i := 4; i < len(ctx.Args); i++ {
		if !strings.EqualFold(string(ctx.Args[i]), "COUNT") || i+1 >= len(ctx.Args) {
			return ctx.W.WriteError(errStreamSyntax.Error())
		}
		n, err := strconv.ParseInt(string(ctx.Args[i+1]), 10, 64)
		if err != nil {
			return ctx.W.WriteError("ERR value is not an integer or out of range")
		}
		if n < 0 {
			n = 0
		}
		count = int(n)
		i++
	}
	if count == 0 {
		return ctx.W.WriteNullArray()
	}
	if count < 0 {
		count = 0
	}

	entries, err := ctx.Store.StreamRange(key, start, end, count, rev)
	switch err {
	case nil:
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
	if err := ctx.W.WriteArrayHeader(len(entries)); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeStreamEntry(ctx.W, entry); err != nil {
			return err
		}
	}
	return nil
}

// writeStreamEntry writes one entry of a stream: the identifier and the list
// of the field-value pairs.
func writeStreamEntry(w *resp.Writer, entry store.StreamEntry) error {
	if err := w.WriteArrayHeader(2); err != nil {
		return err
	}
	if err := w.WriteBulkString([]byte(entry.ID.String())); err != nil {
		return err
	}
	if err := w.WriteArrayHeader(len(entry.Fields)); err != nil {
		return err
	}
	for _, f := range entry.Fields {
		if err := w.WriteBulkString(f); err != nil {
			return err
		}
	}
	return nil
}

// streamIDNext returns the identifier that follows id.
func streamIDNext(id store.StreamID) store.StreamID {
	if id.Seq == ^uint64(0) {
		if id.MS == ^uint64(0) {
			return store.StreamMaxID
		}
		return store.StreamID{MS: id.MS + 1}
	}
	return store.StreamID{MS: id.MS, Seq: id.Seq + 1}
}

// streamIDPrev returns the identifier that precedes id.
func streamIDPrev(id store.StreamID) store.StreamID {
	if id.Seq == 0 {
		if id.MS == 0 {
			return store.StreamID{}
		}
		return store.StreamID{MS: id.MS - 1, Seq: ^uint64(0)}
	}
	return store.StreamID{MS: id.MS, Seq: id.Seq - 1}
}

// cmdXDel removes entries from a stream.
func cmdXDel(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "xdel")
		return nil
	}
	key := ctx.Args[1]
	ids := make([]store.StreamID, 0, len(ctx.Args)-2)
	for _, arg := range ctx.Args[2:] {
		id, ok := store.ParseStreamIDStrict(string(arg), 0, nil)
		if !ok {
			return ctx.W.WriteError(errInvalidStreamID.Error())
		}
		ids = append(ids, id)
	}
	n, err := ctx.Store.StreamDelete(key, ids)
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

// cmdXTrim removes the entries of a stream that exceed a limit.
func cmdXTrim(ctx *Ctx) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, "xtrim")
		return nil
	}
	key := ctx.Args[1]
	trim, err := store.ParseStreamTrimArgs(ctx.Args[2:])
	if err != nil {
		return writeStreamError(ctx.W, err)
	}
	n, err := ctx.Store.StreamTrim(key, trim)
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

// cmdXSetID sets the identifier of the last entry of a stream.
func cmdXSetID(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "xsetid")
		return nil
	}
	id, ok := store.ParseStreamIDStrict(string(ctx.Args[2]), 0, nil)
	if !ok {
		return ctx.W.WriteError(errInvalidStreamID.Error())
	}
	entriesAdded := uint64(0)
	setEntriesAdded := false
	var maxDeletedID *store.StreamID
	for i := 3; i < len(ctx.Args); i++ {
		opt := strings.ToUpper(string(ctx.Args[i]))
		if i+1 >= len(ctx.Args) {
			return ctx.W.WriteError(errStreamSyntax.Error())
		}
		switch opt {
		case "ENTRIESADDED":
			n, err := strconv.ParseUint(string(ctx.Args[i+1]), 10, 64)
			if err != nil {
				return ctx.W.WriteError("ERR value is not an integer or out of range")
			}
			entriesAdded = n
			setEntriesAdded = true
		case "MAXDELETEDID":
			id, ok := store.ParseStreamIDStrict(string(ctx.Args[i+1]), 0, nil)
			if !ok {
				return ctx.W.WriteError(errInvalidStreamID.Error())
			}
			maxDeletedID = &id
		default:
			return ctx.W.WriteError(errStreamSyntax.Error())
		}
		i++
	}
	err := ctx.Store.StreamSetID(ctx.Args[1], id, entriesAdded, setEntriesAdded, maxDeletedID)
	switch err {
	case nil:
	case store.ErrStreamNotFound:
		return ctx.W.WriteError("ERR no such key")
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrStreamIDNotGreater:
		return ctx.W.WriteError("ERR The ID specified in XSETID is smaller than the target stream top item")
	default:
		return writeStreamError(ctx.W, err)
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}
