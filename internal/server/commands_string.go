package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

func cmdSet(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "set")
		return nil
	}
	key, val := ctx.Args[1], ctx.Args[2]

	var ttl time.Duration
	var nx, xx bool
	for i := 3; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "EX":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			sec, err := strconv.ParseInt(string(ctx.Args[i+1]), 10, 64)
			if err != nil || sec < 0 {
				return ctx.W.WriteError("ERR invalid expire time in 'set' command")
			}
			ttl = time.Duration(sec) * time.Second
			i += 2
		case "PX":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			ms, err := strconv.ParseInt(string(ctx.Args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return ctx.W.WriteError("ERR invalid expire time in 'set' command")
			}
			ttl = time.Duration(ms) * time.Millisecond
			i += 2
		case "NX":
			nx = true
			i++
		case "XX":
			xx = true
			i++
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}
	if nx && xx {
		return ctx.W.WriteError("ERR syntax error")
	}

	if nx && ctx.Store.Exists(key) == 1 {
		return ctx.W.WriteNullBulk()
	}
	if xx && ctx.Store.Exists(key) == 0 {
		return ctx.W.WriteNullBulk()
	}

	if ttl > 0 {
		ctx.Store.SetStringWithTTL(key, val, ttl)
	} else {
		ctx.Store.SetString(key, val)
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

func cmdGet(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "get")
		return nil
	}
	v, ok, err := ctx.Store.GetString(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkLarge(v)
}

func cmdAppend(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "append")
		return nil
	}
	n, err := ctx.Store.Append(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

func cmdStrlen(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "strlen")
		return nil
	}
	n, _, err := ctx.Store.StrLen(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

func cmdIncr(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "incr")
		return nil
	}
	return incrApply(ctx, 1)
}

func cmdDecr(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "decr")
		return nil
	}
	return incrApply(ctx, -1)
}

func cmdIncrBy(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "incrby")
		return nil
	}
	delta, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	return incrApply(ctx, delta)
}

func cmdDecrBy(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "decrby")
		return nil
	}
	delta, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	return incrApply(ctx, -delta)
}

func incrApply(ctx *Ctx, delta int64) error {
	n, err := ctx.Store.IncrBy(ctx.Args[1], delta)
	switch err {
	case nil:
		return ctx.W.WriteInteger(n)
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrNotInteger:
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	case store.ErrOverflow:
		return ctx.W.WriteError("ERR increment or decrement would overflow")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}
