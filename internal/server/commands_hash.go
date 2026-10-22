package server

func cmdHSet(ctx *Ctx) error {
	// HSET key field value [field value ...]
	if len(ctx.Args) < 4 || len(ctx.Args)%2 != 0 {
		wrongArgs(ctx.W, "hset")
		return nil
	}
	n := (len(ctx.Args) - 2) / 2
	fields := make([][]byte, 0, n)
	values := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		fields = append(fields, ctx.Args[2+2*i])
		values = append(values, ctx.Args[3+2*i])
	}
	added, err := ctx.Store.HashSetMany(ctx.Args[1], fields, values)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(added))
}

func cmdHGet(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "hget")
		return nil
	}
	v, ok, err := ctx.Store.HashGet(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkString(v)
}

func cmdHGetAll(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "hgetall")
		return nil
	}
	pairs, err := ctx.Store.HashGetAll(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	// In RESP3 the hash is a map, in RESP2 a flat array.
	if ctx.proto3() {
		if err := ctx.W.WriteMapHeader(len(pairs) / 2); err != nil {
			return err
		}
		for _, p := range pairs {
			if err := ctx.W.WriteBulkString(p); err != nil {
				return err
			}
		}
		return nil
	}
	return writeBulkArray(ctx.W, pairs)
}

func cmdHDel(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "hdel")
		return nil
	}
	removed, err := ctx.Store.HashDel(ctx.Args[1], ctx.Args[2:]...)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(removed))
}

func cmdHLen(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "hlen")
		return nil
	}
	n, _, err := ctx.Store.HashLen(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

func cmdHExists(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "hexists")
		return nil
	}
	exists, err := ctx.Store.HashExists(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if exists {
		return ctx.W.WriteInteger(1)
	}
	return ctx.W.WriteInteger(0)
}

func cmdHKeys(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "hkeys")
		return nil
	}
	keys, err := ctx.Store.HashKeys(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return writeBulkArray(ctx.W, keys)
}

func cmdHVals(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "hvals")
		return nil
	}
	vals, err := ctx.Store.HashVals(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return writeBulkArray(ctx.W, vals)
}
