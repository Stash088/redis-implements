package server

import (
	"strconv"
	"strings"
	"time"
)

// cmdBLPop removes and returns the first element of the first non-empty list
// of the given keys, waiting for them if needed.
func cmdBLPop(ctx *Ctx) error {
	return blockingListPop(ctx, "blpop", popLeft)
}

// cmdBRPop removes and returns the last element of the first non-empty list
// of the given keys, waiting for them if needed.
func cmdBRPop(ctx *Ctx) error {
	return blockingListPop(ctx, "brpop", popRight)
}

func blockingListPop(ctx *Ctx, cmd string, kind popKind) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	keys := ctx.Args[1 : len(ctx.Args)-1]
	timeout, err := parseBlockingTimeout(ctx, cmd, string(ctx.Args[len(ctx.Args)-1]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	left := kind == popLeft
	for {
		key, v, ok, err := ctx.Store.ListPopAny(keys, left)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		if ok {
			// BLPOP and BRPOP answer with the key and the value.
			if err := ctx.W.WriteArrayHeader(2); err != nil {
				return err
			}
			if err := ctx.W.WriteBulkString(key); err != nil {
				return err
			}
			return ctx.W.WriteBulkString(v)
		}
		// Nothing to pop: wait for a key to become available.
		ctx.wait = &waitRequest{keys: keys, kind: kind, timeout: timeout}
		return nil
	}
}

// cmdBLMove pops an element from a list and pushes it to another list,
// waiting for the source list to become non-empty if needed.
func cmdBLMove(ctx *Ctx) error {
	if len(ctx.Args) != 6 {
		wrongArgs(ctx.W, "blmove")
		return nil
	}
	fromLeft, ok := parseSide(ctx, "blmove", string(ctx.Args[3]))
	if !ok {
		return nil
	}
	toLeft, ok := parseSide(ctx, "blmove", string(ctx.Args[4]))
	if !ok {
		return nil
	}
	timeout, err := parseBlockingTimeout(ctx, "blmove", string(ctx.Args[5]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	source, dest := ctx.Args[1], ctx.Args[2]
	for {
		v, moved, err := ctx.Store.ListMove(source, dest, fromLeft, toLeft)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		if moved {
			ctx.propagateCmd()
			return ctx.W.WriteBulkString(v)
		}
		ctx.wait = &waitRequest{keys: [][]byte{source}, kind: popLeft, timeout: timeout, dest: dest}
		return nil
	}
}

// cmdBZPopMin removes and returns the member with the lowest score of the
// first non-empty sorted set, waiting for them if needed.
func cmdBZPopMin(ctx *Ctx) error {
	return blockingZPop(ctx, "bzpopmin", popZMin)
}

// cmdBZPopMax removes and returns the member with the highest score of the
// first non-empty sorted set, waiting for them if needed.
func cmdBZPopMax(ctx *Ctx) error {
	return blockingZPop(ctx, "bzpopmax", popZMax)
}

func blockingZPop(ctx *Ctx, cmd string, kind popKind) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	keys := ctx.Args[1 : len(ctx.Args)-1]
	timeout, err := parseBlockingTimeout(ctx, cmd, string(ctx.Args[len(ctx.Args)-1]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	for {
		key, entry, ok, err := ctx.Store.ZPopAny(keys, kind == popZMin)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		if ok {
			ctx.propagateCmd()
			// BZPOPMIN and BZPOPMAX answer with the key, the member and
			// its score.
			if err := ctx.W.WriteArrayHeader(3); err != nil {
				return err
			}
			if err := ctx.W.WriteBulkString(key); err != nil {
				return err
			}
			if err := ctx.W.WriteBulkString(entry.Member); err != nil {
				return err
			}
			return ctx.W.WriteBulkString([]byte(formatScore(entry.Score)))
		}
		ctx.wait = &waitRequest{keys: keys, kind: kind, timeout: timeout}
		return nil
	}
}

// parseBlockingTimeout parses the timeout of a blocking command. A zero
// timeout means "wait forever", like in Redis.
func parseBlockingTimeout(ctx *Ctx, cmd, s string) (time.Duration, error) {
	if strings.EqualFold(s, "0") {
		return 0, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		return 0, &rangeError{"ERR timeout is not a float or out of range"}
	}
	return time.Duration(v * float64(time.Second)), nil
}
