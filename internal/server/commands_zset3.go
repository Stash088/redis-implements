package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// cmdZUnionStore stores the union of the given sets in a destination key.
func cmdZUnionStore(ctx *Ctx) error {
	return zsetOpCommon(ctx, "zunionstore", true)
}

// cmdZInterStore stores the intersection of the given sets in a destination
// key.
func cmdZInterStore(ctx *Ctx) error {
	return zsetOpCommon(ctx, "zinterstore", true)
}

// cmdZDiff returns the difference of the given sets.
func cmdZDiff(ctx *Ctx) error {
	return zsetOpCommon(ctx, "zdiff", false)
}

// cmdZDiffStore stores the difference of the given sets in a destination key.
func cmdZDiffStore(ctx *Ctx) error {
	return zsetOpCommon(ctx, "zdiffstore", true)
}

// zsetOpCommon implements the set operations of the sorted sets. The syntax is
// common: [DESTINATION] numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...].
func zsetOpCommon(ctx *Ctx, cmd string, storeDest bool) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	args := ctx.Args[1:]
	var dest []byte
	if storeDest {
		if len(args) < 3 {
			wrongArgs(ctx.W, cmd)
			return nil
		}
		dest = args[0]
		args = args[1:]
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	if numKeys < 1 || len(args) < 1+numKeys {
		return ctx.W.WriteError("ERR at least 1 input key is needed for '" + cmd + "' command")
	}
	keys := args[1 : 1+numKeys]
	rest := args[1+numKeys:]

	var weights []float64
	agg := store.AggregateSum
	for i := 0; i < len(rest); {
		switch strings.ToUpper(string(rest[i])) {
		case "WEIGHTS":
			if i+numKeys >= len(rest) {
				return ctx.W.WriteError("ERR syntax error")
			}
			weights = make([]float64, 0, numKeys)
			for j := 0; j < numKeys; j++ {
				if i+1+j >= len(rest) {
					return ctx.W.WriteError("ERR syntax error")
				}
				w, err := strconv.ParseFloat(string(rest[i+1+j]), 64)
				if err != nil {
					return ctx.W.WriteError("ERR weight value is not a float")
				}
				weights = append(weights, w)
			}
			i += 1 + numKeys
		case "AGGREGATE":
			if i+1 >= len(rest) {
				return ctx.W.WriteError("ERR syntax error")
			}
			switch strings.ToUpper(string(rest[i+1])) {
			case "SUM":
				agg = store.AggregateSum
			case "MIN":
				agg = store.AggregateMin
			case "MAX":
				agg = store.AggregateMax
			default:
				return ctx.W.WriteError("ERR syntax error")
			}
			i += 2
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}

	switch cmd {
	case "zunionstore":
		n, err := ctx.Store.ZUnionStore(dest, keys, weights, agg)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		ctx.propagateCmd()
		return ctx.W.WriteInteger(int64(n))
	case "zinterstore":
		n, err := ctx.Store.ZInterStore(dest, keys, weights, agg)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		ctx.propagateCmd()
		return ctx.W.WriteInteger(int64(n))
	case "zdiffstore":
		n, err := ctx.Store.ZDiffStore(dest, keys)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		ctx.propagateCmd()
		return ctx.W.WriteInteger(int64(n))
	default:
		entries, err := ctx.Store.ZDiff(keys)
		if err != nil {
			return writeWrongType(ctx.W)
		}
		return writeZEntries(ctx, entries, false)
	}
}

// cmdZInterCard returns the cardinality of the intersection of the sets.
func cmdZInterCard(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "zintercard")
		return nil
	}
	numKeys, err := strconv.Atoi(string(ctx.Args[1]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	if numKeys < 1 || len(ctx.Args) < 2+numKeys {
		return ctx.W.WriteError("ERR at least 1 input key is needed for 'zintercard' command")
	}
	keys := ctx.Args[2 : 2+numKeys]
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
	n, err := ctx.Store.ZInterCard(keys, limit)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdZRemRangeByRank removes the members of a rank range.
func cmdZRemRangeByRank(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "zremrangebyrank")
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
	n, err := ctx.Store.ZRemRangeByRank(ctx.Args[1], start, stop)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if n > 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdZRemRangeByScore removes the members of a score range.
func cmdZRemRangeByScore(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "zremrangebyscore")
		return nil
	}
	min, err := parseScoreBound(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	max, err := parseScoreBound(string(ctx.Args[3]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	n, err := ctx.Store.ZRemRangeByScore(ctx.Args[1], min.value, max.value, min.exclusive, max.exclusive)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if n > 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdZRemRangeByLex removes the members of a lexicographical range.
func cmdZRemRangeByLex(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "zremrangebylex")
		return nil
	}
	min, err := parseLexBound(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	max, err := parseLexBound(string(ctx.Args[3]))
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	var minVal, maxVal []byte
	if !min.all {
		minVal = min.value
	}
	if !max.all {
		maxVal = max.value
	}
	n, err := ctx.Store.ZRemRangeByLex(ctx.Args[1], minVal, maxVal, min.exclusive, max.exclusive)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if n > 0 {
		ctx.propagateCmd()
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdZPopMin removes and returns the members with the lowest scores.
func cmdZPopMin(ctx *Ctx) error { return zpopCommon(ctx, "zpopmin", true) }

// cmdZPopMax removes and returns the members with the highest scores.
func cmdZPopMax(ctx *Ctx) error { return zpopCommon(ctx, "zpopmax", false) }

func zpopCommon(ctx *Ctx, cmd string, lowest bool) error {
	if len(ctx.Args) < 2 || len(ctx.Args) > 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	count := 0
	if len(ctx.Args) == 3 {
		n, err := strconv.Atoi(string(ctx.Args[2]))
		if err != nil || n < 0 {
			return ctx.W.WriteError("ERR value is out of range, must be positive")
		}
		count = n
	}
	entries, err := ctx.Store.ZPop(ctx.Args[1], count, lowest)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if len(entries) == 0 {
		return ctx.W.WriteValue(resp.NewArray(nil))
	}
	if count == 0 && len(entries) == 1 {
		// Without a count a single member with its score is returned.
		ctx.propagateCmd()
		return writeZEntries(ctx, entries, true)
	}
	ctx.propagateCmd()
	return writeZEntries(ctx, entries, true)
}

// cmdZRandMember returns random members of a sorted set.
func cmdZRandMember(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "zrandmember")
		return nil
	}
	count := 0
	hasCount := false
	withScores := false
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
		if strings.EqualFold(string(ctx.Args[i]), "WITHSCORES") {
			withScores = true
			i++
			continue
		}
		return ctx.W.WriteError("ERR syntax error")
	}
	entries, err := ctx.Store.ZRandomMembers(ctx.Args[1], count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !hasCount {
		if len(entries) == 0 {
			return ctx.W.WriteNullBulk()
		}
		return ctx.W.WriteBulkString(entries[0].Member)
	}
	return writeZEntries(ctx, entries, withScores)
}

// cmdZRangeStore stores a range of a sorted set in a destination key.
func cmdZRangeStore(ctx *Ctx) error {
	if len(ctx.Args) < 5 {
		wrongArgs(ctx.W, "zrangestore")
		return nil
	}
	dst, src := ctx.Args[1], ctx.Args[2]
	minArg, maxArg := string(ctx.Args[3]), string(ctx.Args[4])
	byLex := false
	byScore := false
	withScores := false
	offset, count := 0, 0
	for i := 5; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "BYSCORE":
			byScore = true
			i++
		case "BYLEX":
			byLex = true
			i++
		case "WITHSCORES":
			withScores = true
			i++
		case "LIMIT":
			var err error
			offset, count, i, err = limitArgs(ctx.Args, i)
			if err != nil {
				return ctx.W.WriteError(err.Error())
			}
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}
	_ = withScores
	var n int
	var err error
	switch {
	case byLex:
		min, err1 := parseLexBound(minArg)
		max, err2 := parseLexBound(maxArg)
		if err1 != nil {
			return ctx.W.WriteError(err1.Error())
		}
		if err2 != nil {
			return ctx.W.WriteError(err2.Error())
		}
		var minVal, maxVal []byte
		if !min.all {
			minVal = min.value
		}
		if !max.all {
			maxVal = max.value
		}
		n, err = ctx.Store.ZRangeStoreByLex(dst, src, minVal, maxVal, min.exclusive, max.exclusive, offset, count)
	case byScore:
		min, err1 := parseScoreBound(minArg)
		max, err2 := parseScoreBound(maxArg)
		if err1 != nil {
			return ctx.W.WriteError(err1.Error())
		}
		if err2 != nil {
			return ctx.W.WriteError(err2.Error())
		}
		n, err = ctx.Store.ZRangeStoreByScore(dst, src, min.value, max.value, min.exclusive, max.exclusive, offset, count)
	default:
		// Without BYSCORE or BYLEX the bounds are rank indexes.
		start, err1 := strconv.Atoi(minArg)
		stop, err2 := strconv.Atoi(maxArg)
		if err1 != nil {
			return ctx.W.WriteError("ERR value is not an integer or out of range")
		}
		if err2 != nil {
			return ctx.W.WriteError("ERR value is not an integer or out of range")
		}
		n, err = ctx.Store.ZRangeStoreByRank(dst, src, start, stop, offset, count)
	}
	if err != nil {
		return writeWrongType(ctx.W)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(int64(n))
}
