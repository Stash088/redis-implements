package server

import (
	"strconv"
	"strings"
)

// cmdLInsert inserts an element before or after the first occurrence of a
// pivot element.
func cmdLInsert(ctx *Ctx) error {
	if len(ctx.Args) != 5 {
		wrongArgs(ctx.W, "linsert")
		return nil
	}
	var before bool
	switch strings.ToLower(string(ctx.Args[2])) {
	case "before":
		before = true
	case "after":
		before = false
	default:
		return ctx.W.WriteError("ERR syntax error")
	}
	n, err := ctx.Store.ListInsert(ctx.Args[1], before, ctx.Args[3], ctx.Args[4])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if n >= 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdLRem removes the occurrences of an element from a list.
func cmdLRem(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "lrem")
		return nil
	}
	count, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	n, err := ctx.Store.ListRemove(ctx.Args[1], count, ctx.Args[3])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if n > 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdLTrim removes the elements outside the given range.
func cmdLTrim(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "ltrim")
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
	if err := ctx.Store.ListTrim(ctx.Args[1], start, stop); err != nil {
		return writeWrongType(ctx.W)
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdLPos returns the position of the occurrences of an element in a list.
func cmdLPos(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "lpos")
		return nil
	}
	rank, count, maxlen := 0, 0, 0
	hasCount := false
	for i := 3; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "RANK":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil || n == 0 {
				return ctx.W.WriteError("ERR RANK can't be zero. Use 1 to start searching from the first matching element in the head of the list or a negative rank to start searching from the tail. A rank larger than the list size is also acceptable.")
			}
			rank = n
			i += 2
		case "COUNT":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil {
				return ctx.W.WriteError("ERR value is not an integer or out of range")
			}
			if n < 0 {
				return ctx.W.WriteError("ERR COUNT can't be negative")
			}
			count = n
			hasCount = true
			i += 2
		case "MAXLEN":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil || n < 0 {
				return ctx.W.WriteError("ERR MAXLEN can't be negative")
			}
			maxlen = n
			i += 2
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}
	positions, err := ctx.Store.ListPosition(ctx.Args[1], ctx.Args[2], rank, count, maxlen)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if len(positions) == 0 {
		return ctx.W.WriteNullBulk()
	}
	// Without the COUNT option the reply is a single index; with it (even
	// COUNT 0, which means "all matches") the reply is an array.
	if !hasCount {
		return ctx.W.WriteInteger(int64(positions[0]))
	}
	if err := ctx.W.WriteArrayHeader(len(positions)); err != nil {
		return err
	}
	for _, p := range positions {
		if err := ctx.W.WriteInteger(int64(p)); err != nil {
			return err
		}
	}
	return nil
}

// cmdLMove pops an element from one list and pushes it to another.
func cmdLMove(ctx *Ctx) error {
	if len(ctx.Args) != 5 {
		wrongArgs(ctx.W, "lmove")
		return nil
	}
	fromLeft, ok := parseSide(ctx, "lmove", string(ctx.Args[3]))
	if !ok {
		return nil
	}
	toLeft, ok := parseSide(ctx, "lmove", string(ctx.Args[4]))
	if !ok {
		return nil
	}
	v, moved, err := ctx.Store.ListMove(ctx.Args[1], ctx.Args[2], fromLeft, toLeft)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !moved {
		return ctx.W.WriteNullBulk()
	}
	ctx.propagateCmd()
	return ctx.W.WriteBulkString(v)
}

// cmdRPopLPush pops the last element of a list and pushes it to another.
func cmdRPopLPush(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "rpoplpush")
		return nil
	}
	v, moved, err := ctx.Store.ListMove(ctx.Args[1], ctx.Args[2], false, true)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !moved {
		return ctx.W.WriteNullBulk()
	}
	ctx.propagateCmd()
	return ctx.W.WriteBulkString(v)
}

// parseSide parses the LEFT/RIGHT argument of the list move commands.
func parseSide(ctx *Ctx, cmd, s string) (bool, bool) {
	switch strings.ToLower(s) {
	case "left":
		return true, true
	case "right":
		return false, true
	}
	ctx.W.WriteError("ERR syntax error")
	return false, false
}

// cmdLPushX pushes elements to an existing list.
func cmdLPushX(ctx *Ctx) error {
	return listPushExist(ctx, "lpushx", true)
}

// cmdRPushX pushes elements to an existing list.
func cmdRPushX(ctx *Ctx) error {
	return listPushExist(ctx, "rpushx", false)
}

func listPushExist(ctx *Ctx, cmd string, left bool) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	n, ok, err := ctx.Store.ListPushExist(ctx.Args[1], left, ctx.Args[2:])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(int64(n))
}
