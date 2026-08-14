package server

import (
	"strconv"
)

func cmdLPush(ctx *Ctx) error { return pushCommon(ctx, "lpush", true) }
func cmdRPush(ctx *Ctx) error { return pushCommon(ctx, "rpush", false) }

func pushCommon(ctx *Ctx, cmd string, left bool) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	n, err := ctx.Store.ListPush(ctx.Args[1], left, ctx.Args[2:]...)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

func cmdLPop(ctx *Ctx) error { return popCommon(ctx, "lpop", true) }
func cmdRPop(ctx *Ctx) error { return popCommon(ctx, "rpop", false) }

func popCommon(ctx *Ctx, cmd string, left bool) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	v, ok, err := ctx.Store.ListPop(ctx.Args[1], left)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkString(v)
}

func cmdLLen(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "llen")
		return nil
	}
	n, _, err := ctx.Store.ListLen(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

func cmdLRange(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "lrange")
		return nil
	}
	start, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(string(ctx.Args[3]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	vals, err := ctx.Store.ListRange(ctx.Args[1], start, stop)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return writeBulkArray(ctx.W, vals)
}

func cmdLIndex(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "lindex")
		return nil
	}
	i, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	v, ok, err := ctx.Store.ListIndex(ctx.Args[1], i)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkString(v)
}

func cmdLSet(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "lset")
		return nil
	}
	i, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	ok, err := ctx.Store.ListSet(ctx.Args[1], i, ctx.Args[3])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteError("ERR index out of range")
	}
	return ctx.W.WriteSimpleString("OK")
}

// writeBulkArray writes an array of bulk strings.
func writeBulkArray(w interface {
	WriteArrayHeader(int) error
	WriteBulkString([]byte) error
}, vals [][]byte) error {
	if err := w.WriteArrayHeader(len(vals)); err != nil {
		return err
	}
	for _, v := range vals {
		if err := w.WriteBulkString(v); err != nil {
			return err
		}
	}
	return nil
}
