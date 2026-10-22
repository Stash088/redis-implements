package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// cmdHIncrBy increments the integer value of a hash field.
func cmdHIncrBy(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "hincrby")
		return nil
	}
	delta, err := strconv.ParseInt(string(ctx.Args[3]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	n, err := ctx.Store.HashIncrBy(ctx.Args[1], ctx.Args[2], delta)
	switch err {
	case nil:
		ctx.propagateCmd()
		return ctx.W.WriteInteger(n)
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrNotInteger:
		return ctx.W.WriteError("ERR hash value is not an integer")
	case store.ErrOverflow:
		return ctx.W.WriteError("ERR increment or decrement would overflow")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// cmdHIncrByFloat increments the float value of a hash field.
func cmdHIncrByFloat(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "hincrbyfloat")
		return nil
	}
	delta, err := strconv.ParseFloat(string(ctx.Args[3]), 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not a valid float")
	}
	v, err := ctx.Store.HashIncrByFloat(ctx.Args[1], ctx.Args[2], delta)
	switch err {
	case nil:
		ctx.propagateCmd()
		return ctx.W.WriteBulkString([]byte(store.FormatFloat(v)))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrNotFloat:
		return ctx.W.WriteError("ERR hash value is not a float")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// cmdHStrLen returns the length of the value of a hash field.
func cmdHStrLen(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "hstrlen")
		return nil
	}
	n, _, err := ctx.Store.HashStrLen(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdHMGet returns the values of the given hash fields.
func cmdHMGet(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "hmget")
		return nil
	}
	values, err := ctx.Store.HashMGet(ctx.Args[1], ctx.Args[2:])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if err := ctx.W.WriteArrayHeader(len(values)); err != nil {
		return err
	}
	for _, v := range values {
		if err := ctx.W.WriteBulkString(v); err != nil {
			return err
		}
	}
	return nil
}

// cmdHMSet sets several hash fields at once.
func cmdHMSet(ctx *Ctx) error {
	if len(ctx.Args) < 4 || len(ctx.Args)%2 != 0 {
		wrongArgs(ctx.W, "hmset")
		return nil
	}
	n := (len(ctx.Args) - 2) / 2
	fields := make([][]byte, 0, n)
	values := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		fields = append(fields, ctx.Args[2+2*i])
		values = append(values, ctx.Args[3+2*i])
	}
	if _, err := ctx.Store.HashSetMany(ctx.Args[1], fields, values); err != nil {
		return writeWrongType(ctx.W)
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdHSetNX sets a hash field only when it does not exist.
func cmdHSetNX(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "hsetnx")
		return nil
	}
	ok, err := ctx.Store.HashSetNX(ctx.Args[1], ctx.Args[2], ctx.Args[3])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdHRandField returns random fields of a hash.
func cmdHRandField(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "hrandfield")
		return nil
	}
	// HRANDFIELD key [count [WITHVALUES]]
	withValues := false
	count := 0
	hasCount := false
	for i := 2; i < len(ctx.Args); {
		if !hasCount {
			n, err := strconv.Atoi(string(ctx.Args[i]))
			if err != nil {
				return ctx.W.WriteError("ERR value is not an integer or out of range")
			}
			count = n
			hasCount = true
			i++
			continue
		}
		if strings.EqualFold(string(ctx.Args[i]), "WITHVALUES") {
			withValues = true
			i++
			continue
		}
		return ctx.W.WriteError("ERR syntax error")
	}

	if !hasCount {
		field, ok, err := ctx.Store.HashRandomField(ctx.Args[1])
		if err != nil {
			return writeWrongType(ctx.W)
		}
		if !ok {
			return ctx.W.WriteNullBulk()
		}
		return ctx.W.WriteBulkString(field)
	}

	fields, ok, err := ctx.Store.HashRandomFields(ctx.Args[1], count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteValue(resp.NewArray(nil))
	}
	values, err := ctx.Store.HashMGet(ctx.Args[1], fieldNames(fields))
	if err != nil {
		return writeWrongType(ctx.W)
	}
	n := len(fields)
	if withValues {
		n *= 2
	}
	if err := ctx.W.WriteArrayHeader(n); err != nil {
		return err
	}
	for i, fv := range fields {
		if err := ctx.W.WriteBulkString(fv.Field); err != nil {
			return err
		}
		if withValues {
			if err := ctx.W.WriteBulkString(values[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// fieldNames returns the field names of a list of hash fields.
func fieldNames(fields []store.FieldValue) [][]byte {
	out := make([][]byte, 0, len(fields))
	for _, fv := range fields {
		out = append(out, fv.Field)
	}
	return out
}

// cmdHGetDel removes the given hash fields and returns how many were removed.
// Redis 8 expects the FIELDS form: HGETDEL key FIELDS numfields field [field ...].
func cmdHGetDel(ctx *Ctx) error {
	if len(ctx.Args) < 5 {
		wrongArgs(ctx.W, "hgetdel")
		return nil
	}
	if !strings.EqualFold(string(ctx.Args[2]), "FIELDS") {
		return ctx.W.WriteError("ERR syntax error")
	}
	n, err := strconv.Atoi(string(ctx.Args[3]))
	if err != nil || n < 1 {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	fields := ctx.Args[4:]
	if len(fields) != n {
		return ctx.W.WriteError("ERR The `numfields` parameter must match the number of arguments")
	}
	values, err := ctx.Store.HashGetDel(ctx.Args[1], fields...)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	removed := 0
	for _, v := range values {
		if v != nil {
			removed++
		}
	}
	if removed > 0 {
		ctx.propagateCmd()
	}
	// Redis 8 answers with the values of the removed fields.
	if err := ctx.W.WriteArrayHeader(len(values)); err != nil {
		return err
	}
	for _, v := range values {
		if err := ctx.W.WriteBulkString(v); err != nil {
			return err
		}
	}
	return nil
}
