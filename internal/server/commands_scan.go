package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/store"
)

// scanOptions holds the options of a SCAN-like command.
type scanOptions struct {
	cursor   uint64
	count    int
	pattern  string
	noValues bool
}

// parseScanOptions parses the cursor and the MATCH/COUNT/NOVALUES arguments of
// a SCAN-like command. The arguments start after the key. NOVALUES is only
// accepted by the commands that support it.
func parseScanOptions(ctx *Ctx, cmd string, args [][]byte, allowNoValues bool) (scanOptions, bool) {
	opts := scanOptions{count: 10}
	if len(args) < 1 {
		wrongArgs(ctx.W, cmd)
		return opts, false
	}
	cursor, err := strconv.ParseUint(string(args[0]), 10, 64)
	if err != nil {
		ctx.W.WriteError("ERR invalid cursor")
		return opts, false
	}
	opts.cursor = cursor
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				ctx.W.WriteError("ERR syntax error")
				return opts, false
			}
			opts.pattern = string(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				ctx.W.WriteError("ERR syntax error")
				return opts, false
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				ctx.W.WriteError("ERR COUNT must be a positive integer")
				return opts, false
			}
			opts.count = n
			i++
		case "NOVALUES":
			if !allowNoValues {
				// Only HSCAN supports NOVALUES, like in Redis.
				ctx.W.WriteError("ERR NOVALUES option can only be used in HSCAN")
				return opts, false
			}
			opts.noValues = true
		default:
			ctx.W.WriteError("ERR syntax error")
			return opts, false
		}
	}
	return opts, true
}

// scanRange computes the range of the snapshot to return and the next cursor.
// The cursor indexes a sorted snapshot, so a full iteration returns every
// element exactly once.
func scanRange(total int, opts scanOptions) (start, end int, next uint64) {
	start = int(opts.cursor)
	if start > total {
		start = total
	}
	end = start + opts.count
	if end > total {
		end = total
	}
	next = uint64(end)
	if next == uint64(total) {
		next = 0
	}
	return start, end, next
}

// writeScanReply writes the [cursor, [items...]] reply of a SCAN-like
// command.
func writeScanReply(ctx *Ctx, next uint64, writeItem func() error) error {
	if err := ctx.W.WriteArrayHeader(2); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte(strconv.FormatUint(next, 10))); err != nil {
		return err
	}
	// The items are counted before they are written, so the reply is a
	// two-element array whose second element is the item list.
	return writeItem()
}

// cmdHScan iterates the fields of a hash.
func cmdHScan(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "hscan")
		return nil
	}
	opts, ok := parseScanOptions(ctx, "hscan", ctx.Args[2:], true)
	if !ok {
		return nil
	}
	fields, found, err := ctx.Store.ScanHash(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		fields = nil
	}
	start, end, next := scanRange(len(fields), opts)

	matched := make([]store.FieldValue, 0, end-start)
	for _, fv := range fields[start:end] {
		if opts.pattern == "" || globMatch(opts.pattern, string(fv.Field)) {
			matched = append(matched, fv)
		}
	}
	return writeScanReply(ctx, next, func() error {
		n := len(matched)
		if !opts.noValues {
			n *= 2
		}
		if err := ctx.W.WriteArrayHeader(n); err != nil {
			return err
		}
		for _, fv := range matched {
			if err := ctx.W.WriteBulkString(fv.Field); err != nil {
				return err
			}
			if opts.noValues {
				continue
			}
			if err := ctx.W.WriteBulkString(fv.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// cmdSScan iterates the members of a set.
func cmdSScan(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "sscan")
		return nil
	}
	opts, ok := parseScanOptions(ctx, "sscan", ctx.Args[2:], false)
	if !ok {
		return nil
	}
	members, found, err := ctx.Store.ScanSet(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		members = nil
	}
	start, end, next := scanRange(len(members), opts)

	matched := make([][]byte, 0, end-start)
	for _, m := range members[start:end] {
		if opts.pattern == "" || globMatch(opts.pattern, string(m)) {
			matched = append(matched, m)
		}
	}
	return writeScanReply(ctx, next, func() error {
		if err := ctx.W.WriteArrayHeader(len(matched)); err != nil {
			return err
		}
		for _, m := range matched {
			if err := ctx.W.WriteBulkString(m); err != nil {
				return err
			}
		}
		return nil
	})
}

// cmdZScan iterates the members of a sorted set.
func cmdZScan(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "zscan")
		return nil
	}
	opts, ok := parseScanOptions(ctx, "zscan", ctx.Args[2:], false)
	if !ok {
		return nil
	}
	entries, found, err := ctx.Store.ScanZSet(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		entries = nil
	}
	start, end, next := scanRange(len(entries), opts)

	matched := make([]store.ZEntry, 0, end-start)
	for _, e := range entries[start:end] {
		if opts.pattern == "" || globMatch(opts.pattern, string(e.Member)) {
			matched = append(matched, e)
		}
	}
	return writeScanReply(ctx, next, func() error {
		n := len(matched)
		if !opts.noValues {
			n *= 2
		}
		if err := ctx.W.WriteArrayHeader(n); err != nil {
			return err
		}
		for _, e := range matched {
			if err := ctx.W.WriteBulkString(e.Member); err != nil {
				return err
			}
			if opts.noValues {
				continue
			}
			if err := ctx.W.WriteBulkString([]byte(formatScore(e.Score))); err != nil {
				return err
			}
		}
		return nil
	})
}
