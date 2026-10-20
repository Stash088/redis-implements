package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
)

// cmdSInterCard returns the cardinality of the intersection of the sets.
func cmdSInterCard(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "sintercard")
		return nil
	}
	numKeys, err := strconv.Atoi(string(ctx.Args[1]))
	if err != nil {
		return ctx.W.WriteError("ERR numkeys should be greater than 0")
	}
	if numKeys < 1 {
		return ctx.W.WriteError("ERR numkeys should be greater than 0")
	}
	keys := ctx.Args[2 : 2+numKeys]
	if len(keys) < numKeys {
		return ctx.W.WriteError("ERR numkeys should be greater than 0")
	}
	limit := 0
	for i := 2 + numKeys; i < len(ctx.Args); {
		if strings.EqualFold(string(ctx.Args[i]), "LIMIT") {
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil || n < 0 {
				return ctx.W.WriteError("ERR LIMIT can't be negative")
			}
			limit = n
			i += 2
			continue
		}
		return ctx.W.WriteError("ERR syntax error")
	}
	n, err := ctx.Store.SetIntersectCard(keys, limit)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdSMove moves a member from one set to another.
func cmdSMove(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "smove")
		return nil
	}
	moved, err := ctx.Store.SetMove(ctx.Args[1], ctx.Args[2], ctx.Args[3])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !moved {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdSPop removes and returns random members of a set.
func cmdSPop(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "spop")
		return nil
	}
	count := 0
	hasCount := false
	if len(ctx.Args) == 3 {
		n, err := strconv.Atoi(string(ctx.Args[2]))
		if err != nil || n < 0 {
			return ctx.W.WriteError("ERR value is out of range, must be positive")
		}
		count = n
		hasCount = true
	} else if len(ctx.Args) > 3 {
		wrongArgs(ctx.W, "spop")
		return nil
	}
	members, err := ctx.Store.SetPop(ctx.Args[1], count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if len(members) == 0 {
		if hasCount {
			return ctx.W.WriteValue(resp.NewArray(nil))
		}
		return ctx.W.WriteNullBulk()
	}
	if !hasCount {
		ctx.propagateCmd()
		return ctx.W.WriteBulkString(members[0])
	}
	ctx.propagateCmd()
	return writeMemberList(ctx, members)
}

// cmdSRandMember returns random members of a set.
func cmdSRandMember(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "srandmember")
		return nil
	}
	count := 0
	hasCount := false
	if len(ctx.Args) == 3 {
		n, err := strconv.Atoi(string(ctx.Args[2]))
		if err != nil {
			return ctx.W.WriteError("ERR value is not an integer or out of range")
		}
		count = n
		hasCount = true
	} else if len(ctx.Args) > 3 {
		wrongArgs(ctx.W, "srandmember")
		return nil
	}
	members, err := ctx.Store.SetRandomMembers(ctx.Args[1], count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !hasCount {
		if len(members) == 0 {
			return ctx.W.WriteNullBulk()
		}
		return ctx.W.WriteBulkString(members[0])
	}
	return writeMemberList(ctx, members)
}

// cmdSMIsMember reports for every member whether it belongs to the set.
func cmdSMIsMember(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "smismember")
		return nil
	}
	found, err := ctx.Store.SetIsMemberMany(ctx.Args[1], ctx.Args[2:])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if err := ctx.W.WriteArrayHeader(len(found)); err != nil {
		return err
	}
	for _, f := range found {
		n := int64(0)
		if f {
			n = 1
		}
		if err := ctx.W.WriteInteger(n); err != nil {
			return err
		}
	}
	return nil
}

// setOp implements SDIFF/SINTER/SUNION and their _STORE variants.
func setOp(ctx *Ctx, cmd string, storeDest bool) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	keys := ctx.Args[1:]
	dest := []byte(nil)
	if storeDest {
		if len(ctx.Args) < 3 {
			wrongArgs(ctx.W, cmd)
			return nil
		}
		dest = ctx.Args[1]
		keys = ctx.Args[2:]
	}
	var members [][]byte
	var err error
	switch cmd {
	case "sdiff", "sdiffstore":
		members, err = ctx.Store.SetDiff(keys)
	case "sinter", "sinterstore":
		members, err = ctx.Store.SetInter(keys)
	case "sunion", "sunionstore":
		members, err = ctx.Store.SetUnion(keys)
	}
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if storeDest {
		n, err := ctx.Store.SetStore(dest, members)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		ctx.propagateCmd()
		return ctx.W.WriteInteger(int64(n))
	}
	return writeMemberList(ctx, members)
}

func cmdSDiff(ctx *Ctx) error      { return setOp(ctx, "sdiff", false) }
func cmdSDiffStore(ctx *Ctx) error { return setOp(ctx, "sdiffstore", true) }
func cmdSInter(ctx *Ctx) error     { return setOp(ctx, "sinter", false) }
func cmdSInterStore(ctx *Ctx) error {
	return setOp(ctx, "sinterstore", true)
}
func cmdSUnion(ctx *Ctx) error      { return setOp(ctx, "sunion", false) }
func cmdSUnionStore(ctx *Ctx) error { return setOp(ctx, "sunionstore", true) }

// writeMemberList writes a list of members: a set in RESP3, an array in
// RESP2, like Redis does.
func writeMemberList(ctx *Ctx, members [][]byte) error {
	if ctx.proto3() {
		if err := ctx.W.WriteSetHeader(len(members)); err != nil {
			return err
		}
	} else if err := ctx.W.WriteArrayHeader(len(members)); err != nil {
		return err
	}
	for _, m := range members {
		if err := ctx.W.WriteBulkString(m); err != nil {
			return err
		}
	}
	return nil
}
