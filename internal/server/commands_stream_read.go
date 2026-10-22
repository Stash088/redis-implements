package server

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// xreadReply holds the entries read from one stream.
type xreadReply struct {
	key     []byte
	entries []store.StreamEntry
}

// xreadOptions holds the parsed options of the XREAD and XREADGROUP
// commands.
type xreadOptions struct {
	// count limits the number of entries of the whole reply.
	count int
	// timeout is the time to wait; a negative value means no BLOCK option.
	timeout time.Duration
	// blocked tells whether the BLOCK option was given.
	blocked bool
	// noack asks not to add the delivered entries to the pending list.
	noack bool
	// group and consumer name the group of the XREADGROUP command.
	group    string
	consumer string
	// hasGroup tells whether the GROUP option was given.
	hasGroup bool
	// keys and ids are the streams to read and the identifiers to read from.
	keys [][]byte
	ids  [][]byte
}

// cmdXRead reads the entries of one or more streams.
func cmdXRead(ctx *Ctx) error {
	return xReadCommon(ctx, "xread", false)
}

// cmdXReadGroup reads the entries of one or more streams as a consumer of a
// consumer group.
func cmdXReadGroup(ctx *Ctx) error {
	return xReadCommon(ctx, "xreadgroup", true)
}

// xReadCommon implements XREAD and XREADGROUP.
func xReadCommon(ctx *Ctx, cmd string, group bool) error {
	opts, err := parseXReadOptions(ctx.Args, cmd, group)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	if group && !opts.hasGroup {
		return ctx.W.WriteError("ERR Missing GROUP option for XREADGROUP")
	}
	if len(opts.keys) != len(opts.ids) {
		return ctx.W.WriteError("ERR Unbalanced '" + strings.ToUpper(cmd) + "' list of streams: for each stream key an ID, '+', or '$' must be specified.")
	}

	// Resolve the identifiers of the streams.
	type target struct {
		key  []byte
		id   store.StreamID
		mode store.StreamReadMode
	}
	targets := make([]target, 0, len(opts.keys))
	for i, key := range opts.keys {
		arg := string(opts.ids[i])
		switch arg {
		case "$":
			if group {
				return ctx.W.WriteError("ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.")
			}
			last, ok, err := ctx.Store.StreamLastID(key)
			if err != nil {
				return writeWrongType(ctx.W)
			}
			if !ok {
				last = store.StreamID{}
			}
			targets = append(targets, target{key: key, id: last})
		case "+":
			if group {
				return ctx.W.WriteError("ERR The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The + ID would just return an empty result set.")
			}
			last, ok, err := ctx.Store.StreamLastID(key)
			if err != nil {
				return writeWrongType(ctx.W)
			}
			if !ok {
				last = store.StreamID{}
			}
			// The last entry of the stream is included.
			targets = append(targets, target{key: key, id: streamIDPrev(last)})
		case ">":
			if !group {
				return ctx.W.WriteError("ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.")
			}
			targets = append(targets, target{key: key, mode: store.StreamReadNew})
		default:
			id, ok := store.ParseStreamIDStrict(arg, 0, nil)
			if !ok {
				return ctx.W.WriteError(errInvalidStreamID.Error())
			}
			targets = append(targets, target{key: key, id: id, mode: store.StreamReadHistory})
		}
	}

	// Read the entries of every stream.
	replies := make([]xreadReply, 0, len(targets))
	remaining := opts.count
	for _, t := range targets {
		var entries []store.StreamEntry
		if opts.hasGroup {
			delivery, err := ctx.Store.StreamGroupRead(t.key, opts.group, opts.consumer, t.mode, t.id, remaining, opts.noack)
			switch err {
			case nil:
			case store.ErrStreamNotFound:
				return ctx.W.WriteError(errNoGroupKey(t.key, opts.group))
			case store.ErrGroupNotFound:
				return ctx.W.WriteError(errNoGroupKey(t.key, opts.group))
			case store.ErrWrongType:
				return writeWrongType(ctx.W)
			default:
				return ctx.W.WriteError("ERR " + err.Error())
			}
			entries = delivery.Entries
			// The changes of the pending list are written to the AOF.
			if delivery.ConsumerCreated {
				ctx.propagate([][]byte{[]byte("XGROUP"), []byte("CREATECONSUMER"), t.key, []byte(opts.group), []byte(opts.consumer)})
			}
			propagateClaims(ctx, t.key, opts.group, delivery.Pending, nil)
			if delivery.LastIDChanged {
				propagateGroupID(ctx, t.key, opts.group, delivery.LastID, delivery.EntriesRead)
			}
		} else {
			got, _, err := ctx.Store.StreamRead(t.key, t.id, remaining)
			switch err {
			case nil:
			case store.ErrWrongType:
				return writeWrongType(ctx.W)
			default:
				return ctx.W.WriteError("ERR " + err.Error())
			}
			entries = got
		}
		if len(entries) == 0 {
			// The history of a consumer is always answered, even when the
			// consumer has no pending entry.
			if opts.hasGroup && t.mode == store.StreamReadHistory {
				replies = append(replies, xreadReply{key: t.key})
			}
			continue
		}
		replies = append(replies, xreadReply{key: t.key, entries: entries})
		if remaining > 0 {
			remaining -= len(entries)
			if remaining <= 0 {
				break
			}
		}
	}

	if len(replies) == 0 {
		if opts.blocked {
			// Nothing to read: wait for the streams to receive new entries.
			ctx.wait = &waitRequest{keys: opts.keys, kind: popStream, timeout: opts.timeout}
			return nil
		}
		return ctx.W.WriteNullArray()
	}
	return writeXReadReply(ctx, replies)
}

// writeXReadReply writes the reply of the XREAD and XREADGROUP commands: in
// RESP2 an array of pairs (the key of the stream and its entries), in RESP3 a
// map from the key of the stream to its entries.
func writeXReadReply(ctx *Ctx, replies []xreadReply) error {
	if ctx.proto3() {
		if err := ctx.W.WriteMapHeader(len(replies)); err != nil {
			return err
		}
		for _, r := range replies {
			if err := ctx.W.WriteBulkString(r.key); err != nil {
				return err
			}
			if err := writeStreamEntries(ctx, r.entries); err != nil {
				return err
			}
		}
		return nil
	}
	if err := ctx.W.WriteArrayHeader(len(replies)); err != nil {
		return err
	}
	for _, r := range replies {
		if err := ctx.W.WriteArrayHeader(2); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString(r.key); err != nil {
			return err
		}
		if err := writeStreamEntries(ctx, r.entries); err != nil {
			return err
		}
	}
	return nil
}

// writeStreamEntries writes the entries of a stream.
func writeStreamEntries(ctx *Ctx, entries []store.StreamEntry) error {
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

// parseXReadOptions parses the arguments of the XREAD and XREADGROUP
// commands.
func parseXReadOptions(args [][]byte, cmd string, group bool) (xreadOptions, error) {
	var opts xreadOptions
	opts.timeout = -1
	streamsArg := -1
	i := 1
	for ; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		more := len(args) - i - 1
		switch opt {
		case "COUNT":
			if more < 1 {
				return opts, errStreamSyntax
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 0 {
				return opts, errStreamSyntax
			}
			opts.count = n
			i++
		case "BLOCK":
			if more < 1 {
				return opts, errStreamSyntax
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return opts, errStreamSyntax
			}
			opts.timeout = time.Duration(ms) * time.Millisecond
			opts.blocked = true
			i++
		case "GROUP":
			if !group {
				return opts, errors.New("ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.")
			}
			if more < 2 {
				return opts, errStreamSyntax
			}
			opts.group = string(args[i+1])
			opts.consumer = string(args[i+2])
			opts.hasGroup = true
			i += 2
		case "NOACK":
			if !group {
				return opts, errors.New("ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.")
			}
			opts.noack = true
		case "STREAMS":
			if more < 1 {
				return opts, errStreamSyntax
			}
			streamsArg = i + 1
			i = len(args)
		default:
			return opts, errStreamSyntax
		}
	}
	if streamsArg < 0 {
		return opts, errStreamSyntax
	}
	rest := args[streamsArg:]
	if len(rest)%2 != 0 {
		return opts, errStreamSyntax
	}
	opts.keys = rest[:len(rest)/2]
	opts.ids = rest[len(rest)/2:]
	return opts, nil
}
