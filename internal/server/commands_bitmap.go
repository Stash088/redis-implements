package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/store"
)

// Handlers of the bitmap commands: SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP,
// BITFIELD and BITFIELD_RO. The reply strings mirror the reference Redis 8.2
// word for word; the semantics live in the store layer
// (internal/store/bitmap.go).

// writeBitError maps the errors of the bitmap operations to the replies the
// reference implementation sends.
func writeBitError(ctx *Ctx, err error) error {
	switch err {
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrBitOffset:
		return ctx.W.WriteError("ERR bit offset is not an integer or out of range")
	case store.ErrBitValue:
		return ctx.W.WriteError("ERR bit is not an integer or out of range")
	case store.ErrBitFieldType:
		return ctx.W.WriteError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
	case store.ErrOverflowType:
		return ctx.W.WriteError("ERR Invalid OVERFLOW type specified")
	case store.ErrNotInteger:
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	case store.ErrSyntax:
		return ctx.W.WriteError("ERR syntax error")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// cmdSetBit implements SETBIT key offset value.
func cmdSetBit(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "setbit")
		return nil
	}
	offset, err := store.ParseBitOffset(ctx.Args[2], false, 0)
	if err != nil {
		return writeBitError(ctx, err)
	}
	bit, err := store.ParseBitValue(ctx.Args[3])
	if err != nil {
		return writeBitError(ctx, err)
	}
	old, err := ctx.Store.SetBit(ctx.Args[1], offset, int(bit))
	if err != nil {
		return writeBitError(ctx, err)
	}
	return ctx.W.WriteInteger(int64(old))
}

// cmdGetBit implements GETBIT key offset.
func cmdGetBit(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "getbit")
		return nil
	}
	offset, err := store.ParseBitOffset(ctx.Args[2], false, 0)
	if err != nil {
		return writeBitError(ctx, err)
	}
	bit, _, err := ctx.Store.GetBit(ctx.Args[1], offset)
	if err != nil {
		return writeBitError(ctx, err)
	}
	// A missing key reads as a zero bit.
	return ctx.W.WriteInteger(int64(bit))
}

// cmdBitCount implements BITCOUNT key [start end [BYTE|BIT]].
func cmdBitCount(ctx *Ctx) error {
	var start, end int64
	var hasRange, isbit bool
	switch len(ctx.Args) {
	case 2: // the whole value
	case 4, 5:
		var err error
		start, err = strconv.ParseInt(string(ctx.Args[2]), 10, 64)
		if err != nil {
			return writeBitError(ctx, store.ErrNotInteger)
		}
		end, err = strconv.ParseInt(string(ctx.Args[3]), 10, 64)
		if err != nil {
			return writeBitError(ctx, store.ErrNotInteger)
		}
		if len(ctx.Args) == 5 {
			switch strings.ToLower(string(ctx.Args[4])) {
			case "bit":
				isbit = true
			case "byte":
			default:
				return writeBitError(ctx, store.ErrSyntax)
			}
		}
		hasRange = true
	default:
		return writeBitError(ctx, store.ErrSyntax)
	}
	n, err := ctx.Store.BitCount(ctx.Args[1], start, end, hasRange, isbit)
	if err != nil {
		return writeBitError(ctx, err)
	}
	return ctx.W.WriteInteger(n)
}

// cmdBitPos implements BITPOS key bit [start [end [BYTE|BIT]]].
func cmdBitPos(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "bitpos")
		return nil
	}
	bit, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return writeBitError(ctx, store.ErrNotInteger)
	}
	if bit != 0 && bit != 1 {
		return ctx.W.WriteError("ERR The bit argument must be 1 or 0.")
	}
	var start, end int64
	var hasStart, hasEnd, isbit bool
	switch len(ctx.Args) {
	case 3: // the whole value
	case 4, 5, 6:
		start, err = strconv.ParseInt(string(ctx.Args[3]), 10, 64)
		if err != nil {
			return writeBitError(ctx, store.ErrNotInteger)
		}
		hasStart = true
		if len(ctx.Args) >= 5 {
			end, err = strconv.ParseInt(string(ctx.Args[4]), 10, 64)
			if err != nil {
				return writeBitError(ctx, store.ErrNotInteger)
			}
			hasEnd = true
		}
		if len(ctx.Args) == 6 {
			switch strings.ToLower(string(ctx.Args[5])) {
			case "bit":
				isbit = true
			case "byte":
			default:
				return writeBitError(ctx, store.ErrSyntax)
			}
		}
	default:
		return writeBitError(ctx, store.ErrSyntax)
	}
	pos, err := ctx.Store.BitPos(ctx.Args[1], int(bit), start, end, hasStart, hasEnd, isbit)
	if err != nil {
		return writeBitError(ctx, err)
	}
	return ctx.W.WriteInteger(pos)
}

// cmdBitOp implements BITOP AND|OR|XOR|NOT|DIFF|DIFF1|ANDOR|ONE dest src...
func cmdBitOp(ctx *Ctx) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, "bitop")
		return nil
	}
	op, ok := store.ParseBitOp(ctx.Args[1])
	if !ok {
		return writeBitError(ctx, store.ErrSyntax)
	}
	// The source count of NOT and of the Redis 8.2 operations is part of the
	// grammar: the reference checks it before looking at any key.
	if op == store.BitOpNot && len(ctx.Args) != 4 {
		return ctx.W.WriteError("ERR BITOP NOT must be called with a single source key.")
	}
	if (op == store.BitOpDiff || op == store.BitOpDiff1 || op == store.BitOpAndor) && len(ctx.Args) < 5 {
		name := strings.ToUpper(string(ctx.Args[1]))
		return ctx.W.WriteError("ERR BITOP " + name + " must be called with at least two source keys.")
	}
	n, err := ctx.Store.BitOp(op, ctx.Args[2], ctx.Args[3:])
	if err != nil {
		return writeBitError(ctx, err)
	}
	return ctx.W.WriteInteger(n)
}

// cmdBitField implements BITFIELD key [GET ...][SET ...][INCRBY ...][OVERFLOW ...].
func cmdBitField(ctx *Ctx) error {
	return bitFieldCommon(ctx, false)
}

// cmdBitFieldRO implements BITFIELD_RO key GET type offset: the read-only
// variant that accepts GET only.
func cmdBitFieldRO(ctx *Ctx) error {
	return bitFieldCommon(ctx, true)
}

// bitFieldCommon runs the argument grammar of BITFIELD and BITFIELD_RO. The
// parse errors take precedence over the rejection of a write subcommand on
// the read-only variant, like in the reference implementation.
func bitFieldCommon(ctx *Ctx, readonlyOnly bool) error {
	ops, err := store.ParseBitFieldOps(ctx.Args[2:])
	if err != nil {
		return writeBitError(ctx, err)
	}
	if readonlyOnly {
		for i := range ops {
			if ops[i].Op != store.BitFieldGet {
				return ctx.W.WriteError("ERR BITFIELD_RO only supports the GET subcommand")
			}
		}
	}
	results, err := ctx.Store.BitField(ctx.Args[1], ops)
	if err != nil {
		return writeBitError(ctx, err)
	}
	if err := ctx.W.WriteArrayHeader(len(results)); err != nil {
		return err
	}
	for _, r := range results {
		if r.Null {
			if err := writeNullElement(ctx); err != nil {
				return err
			}
			continue
		}
		if err := ctx.W.WriteInteger(r.Val); err != nil {
			return err
		}
	}
	return nil
}

// writeNullElement writes one null element of an array: a RESP2 client gets
// a null bulk string, a RESP3 client the dedicated null type.
func writeNullElement(ctx *Ctx) error {
	if ctx.proto3() {
		return ctx.W.WriteNull()
	}
	return ctx.W.WriteNullBulk()
}
