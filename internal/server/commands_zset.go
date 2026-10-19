package server

import (
	"strconv"
	"strings"
)

type scorePair struct {
	score  float64
	member []byte
}

func cmdZAdd(ctx *Ctx) error {
	if len(ctx.Args) < 4 || len(ctx.Args)%2 != 0 {
		wrongArgs(ctx.W, "zadd")
		return nil
	}
	key := ctx.Args[1]
	pairs := make([]scorePair, 0, (len(ctx.Args)-2)/2)
	for i := 2; i < len(ctx.Args); i += 2 {
		score, ok := parseFloat(ctx.Args[i])
		if !ok {
			return ctx.W.WriteError("ERR value is not a valid float")
		}
		pairs = append(pairs, scorePair{score: score, member: ctx.Args[i+1]})
	}
	added := 0
	for _, p := range pairs {
		n, err := ctx.Store.ZAdd(key, p.score, p.member)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		added += n
	}
	return ctx.W.WriteInteger(int64(added))
}

func cmdZRange(ctx *Ctx) error    { return zrangeCommon(ctx, "zrange", false) }
func cmdZRevRange(ctx *Ctx) error { return zrangeCommon(ctx, "zrevrange", true) }

func zrangeCommon(ctx *Ctx, cmd string, reverse bool) error {
	if len(ctx.Args) < 4 || len(ctx.Args) > 5 {
		wrongArgs(ctx.W, cmd)
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
	withScores := false
	if len(ctx.Args) == 5 {
		if !strings.EqualFold(string(ctx.Args[4]), "WITHSCORES") {
			return ctx.W.WriteError("ERR syntax error")
		}
		withScores = true
	}
	entries, err := ctx.Store.ZRange(ctx.Args[1], start, stop, reverse)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !withScores {
		members := make([][]byte, len(entries))
		for i, e := range entries {
			members[i] = e.Member
		}
		return writeBulkArray(ctx.W, members)
	}
	// In RESP3 the scores come as pairs with a double score, in RESP2 as a
	// flat array of alternating members and score strings.
	if ctx.proto3() {
		if err := ctx.W.WriteArrayHeader(len(entries)); err != nil {
			return err
		}
		for _, e := range entries {
			if err := ctx.W.WriteArrayHeader(2); err != nil {
				return err
			}
			if err := ctx.W.WriteBulkString(e.Member); err != nil {
				return err
			}
			if err := ctx.W.WriteDouble(e.Score); err != nil {
				return err
			}
		}
		return nil
	}
	if err := ctx.W.WriteArrayHeader(len(entries) * 2); err != nil {
		return err
	}
	for _, e := range entries {
		if err := ctx.W.WriteBulkString(e.Member); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(formatScore(e.Score))); err != nil {
			return err
		}
	}
	return nil
}

func cmdZScore(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "zscore")
		return nil
	}
	score, ok, err := ctx.Store.ZScore(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	// In RESP3 the score is a double, in RESP2 a bulk string.
	if ctx.proto3() {
		return ctx.W.WriteDouble(score)
	}
	return ctx.W.WriteBulkString([]byte(formatScore(score)))
}

func cmdZRem(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "zrem")
		return nil
	}
	removed, err := ctx.Store.ZRem(ctx.Args[1], ctx.Args[2:]...)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(removed))
}

func cmdZCard(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "zcard")
		return nil
	}
	n, _, err := ctx.Store.ZCard(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

func cmdZRank(ctx *Ctx) error {
	return zrankCommon(ctx, "zrank", false)
}

func cmdZRevRank(ctx *Ctx) error {
	return zrankCommon(ctx, "zrevrank", true)
}

func zrankCommon(ctx *Ctx, cmd string, reverse bool) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	rank, found, err := ctx.Store.ZRank(ctx.Args[1], ctx.Args[2], reverse)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !found {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteInteger(rank)
}

func formatScore(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
