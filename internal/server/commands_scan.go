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

// scanBounds returns the offset a page starts at and the cursor of the call
// that follows it. The cursor indexes a snapshot of the collection, so a full
// iteration returns every element exactly once. A cursor of zero means that
// the traversal is over.
func scanBounds(cursor uint64, pageLen, total int) (start int, next uint64) {
	start = int(cursor)
	if start > total {
		start = total
	}
	end := start + pageLen
	if end > total {
		end = total
	}
	if end >= total {
		return start, 0
	}
	return start, uint64(end)
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
	// The store returns one page of the fields: the snapshot of the hash is
	// built once and reused by the following calls of the traversal.
	page, total, found, err := ctx.Store.ScanHashPage(ctx.Args[1], int(opts.cursor), opts.count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		page, total = nil, 0
	}
	_, next := scanBounds(opts.cursor, len(page), total)

	matched := make([]store.FieldValue, 0, len(page))
	for _, fv := range page {
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
	page, total, found, err := ctx.Store.ScanSetPage(ctx.Args[1], int(opts.cursor), opts.count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		page, total = nil, 0
	}
	_, next := scanBounds(opts.cursor, len(page), total)

	matched := make([][]byte, 0, len(page))
	for _, m := range page {
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
	page, total, found, err := ctx.Store.ScanZSetPage(ctx.Args[1], int(opts.cursor), opts.count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		page, total = nil, 0
	}
	_, next := scanBounds(opts.cursor, len(page), total)

	matched := make([]store.ZEntry, 0, len(page))
	for _, e := range page {
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
