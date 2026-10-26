package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// cmdMSet sets several keys at once.
func cmdMSet(ctx *Ctx) error {
	if len(ctx.Args) < 3 || len(ctx.Args)%2 == 0 {
		wrongArgs(ctx.W, "mset")
		return nil
	}
	ctx.Store.SetStrings(ctx.Args[1:], false)
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdMSetNX sets several keys, but only when none of them exists.
func cmdMSetNX(ctx *Ctx) error {
	if len(ctx.Args) < 3 || len(ctx.Args)%2 == 0 {
		wrongArgs(ctx.W, "msetnx")
		return nil
	}
	if !ctx.Store.SetStrings(ctx.Args[1:], true) {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdMGet returns the values of the given keys.
func cmdMGet(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "mget")
		return nil
	}
	values := ctx.Store.GetStrings(ctx.Args[1:]...)
	if err := ctx.W.WriteArrayHeader(len(values)); err != nil {
		return err
	}
	for _, v := range values {
		if err := ctx.W.WriteBulkLarge(v); err != nil {
			return err
		}
	}
	return nil
}

// cmdGetSet sets the value of a key and returns its previous value.
func cmdGetSet(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "getset")
		return nil
	}
	old, ok, err := ctx.Store.GetSet(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	ctx.propagateCmd()
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkString(old)
}

// cmdGetDel returns the value of a key and deletes it.
func cmdGetDel(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "getdel")
		return nil
	}
	v, ok, err := ctx.Store.GetDel(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	ctx.propagateCmd()
	return ctx.W.WriteBulkString(v)
}

// cmdGetEx returns the value of a key and optionally changes its expiration.
func cmdGetEx(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "getex")
		return nil
	}
	var expiresAt time.Time
	clearTTL := false
	for i := 2; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "EX":
			sec, err := expiryArg(ctx, "getex", ctx.Args, i)
			if err != nil {
				return err
			}
			expiresAt = ctx.Store.Now().Add(time.Duration(sec) * time.Second)
			i += 2
		case "PX":
			ms, err := expiryArg(ctx, "getex", ctx.Args, i)
			if err != nil {
				return err
			}
			expiresAt = ctx.Store.Now().Add(time.Duration(ms) * time.Millisecond)
			i += 2
		case "EXAT":
			ts, err := expiryArg(ctx, "getex", ctx.Args, i)
			if err != nil {
				return err
			}
			expiresAt = time.Unix(ts, 0)
			i += 2
		case "PXAT":
			ms, err := expiryArg(ctx, "getex", ctx.Args, i)
			if err != nil {
				return err
			}
			expiresAt = time.UnixMilli(ms)
			i += 2
		case "PERSIST":
			clearTTL = true
			i++
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}
	v, ok, err := ctx.Store.GetEx(ctx.Args[1], expiresAt, clearTTL)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	// Without an expiry option the TTL is removed, and the journal must say
	// so: a PEXPIREAT with an empty instant would expire the key at once
	// on the replay.
	if clearTTL || expiresAt.IsZero() {
		ctx.propagate([][]byte{[]byte("persist"), ctx.Args[1]})
	} else {
		ctx.propagate([][]byte{[]byte("pexpireat"), ctx.Args[1],
			[]byte(strconv.FormatInt(expiresAt.UnixMilli(), 10))})
	}
	return ctx.W.WriteBulkString(v)
}

// cmdSetEx sets a key with an expiration in seconds.
func cmdSetEx(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "setex")
		return nil
	}
	sec, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil || sec <= 0 {
		return ctx.W.WriteError("ERR invalid expire time in 'setex' command")
	}
	ctx.Store.SetEx(ctx.Args[1], ctx.Args[3], time.Duration(sec)*time.Second)
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdPSetEx sets a key with an expiration in milliseconds.
func cmdPSetEx(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "psetex")
		return nil
	}
	ms, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil || ms <= 0 {
		return ctx.W.WriteError("ERR invalid expire time in 'psetex' command")
	}
	ctx.Store.SetEx(ctx.Args[1], ctx.Args[3], time.Duration(ms)*time.Millisecond)
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdSetNX sets a key only when it does not exist.
func cmdSetNX(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "setnx")
		return nil
	}
	if !ctx.Store.SetNX(ctx.Args[1], ctx.Args[2]) {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdGetRange returns a substring of the value of a key.
func cmdGetRange(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "getrange")
		return nil
	}
	start, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(ctx.Args[3]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	v, ok, err := ctx.Store.GetRange(ctx.Args[1], start, stop)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkString(v)
}

// cmdSetRange overwrites part of the value of a key.
func cmdSetRange(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "setrange")
		return nil
	}
	offset, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	n, err := ctx.Store.SetRange(ctx.Args[1], offset, ctx.Args[3])
	switch err {
	case nil:
		ctx.propagateCmd()
		return ctx.W.WriteInteger(int64(n))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrOffsetOutOfRange:
		return ctx.W.WriteError("ERR offset is out of range")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// cmdIncrByFloat increments the value of a key by a float amount.
func cmdIncrByFloat(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "incrbyfloat")
		return nil
	}
	delta, err := strconv.ParseFloat(string(ctx.Args[2]), 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not a valid float")
	}
	v, err := ctx.Store.IncrByFloat(ctx.Args[1], delta)
	switch err {
	case nil:
		ctx.propagateCmd()
		return ctx.W.WriteBulkString([]byte(store.FormatFloat(v)))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrNotFloat:
		return ctx.W.WriteError("ERR value is not a valid float")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// expiryArg parses the expiration argument of a command.
func expiryArg(ctx *Ctx, cmd string, args [][]byte, i int) (int64, error) {
	if i+1 >= len(args) {
		return 0, ctx.W.WriteError("ERR syntax error")
	}
	n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
	if err != nil {
		return 0, ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	return n, nil
}
