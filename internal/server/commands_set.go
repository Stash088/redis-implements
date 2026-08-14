package server

func cmdSAdd(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "sadd")
		return nil
	}
	added, err := ctx.Store.SetAdd(ctx.Args[1], ctx.Args[2:]...)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(added))
}

func cmdSRem(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "srem")
		return nil
	}
	removed, err := ctx.Store.SetRem(ctx.Args[1], ctx.Args[2:]...)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(removed))
}

func cmdSMembers(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "smembers")
		return nil
	}
	members, err := ctx.Store.SetMembers(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return writeBulkArray(ctx.W, members)
}

func cmdSIsMember(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "sismember")
		return nil
	}
	isMember, err := ctx.Store.SetIsMember(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if isMember {
		return ctx.W.WriteInteger(1)
	}
	return ctx.W.WriteInteger(0)
}

func cmdSCard(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "scard")
		return nil
	}
	n, _, err := ctx.Store.SetCard(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}
