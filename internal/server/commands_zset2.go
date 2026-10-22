package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/store"
)

// scoreBound is one boundary of a score range.
type scoreBound struct {
	value     float64
	exclusive bool
}

// parseScoreBound parses a score boundary: a number ("1", "-1.5"), an
// exclusive number ("(1"), or an infinity ("-inf", "+inf").
func parseScoreBound(s string) (scoreBound, error) {
	if strings.HasPrefix(s, "(") {
		v, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return scoreBound{}, errBadScoreBound
		}
		return scoreBound{value: v, exclusive: true}, nil
	}
	switch strings.ToLower(s) {
	case "-inf":
		return scoreBound{value: math.Inf(-1)}, nil
	case "+inf", "inf":
		return scoreBound{value: math.Inf(1)}, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return scoreBound{}, errBadScoreBound
	}
	return scoreBound{value: v}, nil
}

// lexBound is one boundary of a lexicographical range.
type lexBound struct {
	value     []byte
	exclusive bool
	// all marks the "-" and "+" bounds, which cover the whole range.
	all bool
}

// parseLexBound parses a lexicographical boundary: "[value", "(value"), or
// "-" / "+" for the whole range.
func parseLexBound(s string) (lexBound, error) {
	if s == "-" {
		return lexBound{all: true}, nil
	}
	if s == "+" {
		return lexBound{all: true}, nil
	}
	if len(s) == 0 {
		return lexBound{}, errBadLexBound
	}
	switch s[0] {
	case '[':
		return lexBound{value: []byte(s[1:])}, nil
	case '(':
		return lexBound{value: []byte(s[1:]), exclusive: true}, nil
	}
	return lexBound{}, errBadLexBound
}

// Errors of the range parsing, as in Redis.
var (
	errBadScoreBound = &rangeError{"ERR min or max is not a float"}
	errBadLexBound   = &rangeError{"ERR min or max not valid string range item"}
)

// rangeError is an error with a fixed message.
type rangeError struct{ msg string }

func (e *rangeError) Error() string { return e.msg }

// limitArgs parses the optional LIMIT offset count arguments.
func limitArgs(args [][]byte, i int) (offset, count int, next int, err error) {
	offset, count = 0, 0
	if i+1 >= len(args) {
		return 0, 0, i, errBadLimit
	}
	offset, err = strconv.Atoi(string(args[i+1]))
	if err != nil || offset < 0 {
		return 0, 0, i, errBadLimit
	}
	if i+2 >= len(args) {
		return 0, 0, i, errBadLimit
	}
	count, err = strconv.Atoi(string(args[i+2]))
	if err != nil {
		return 0, 0, i, errBadLimit
	}
	return offset, count, i + 3, nil
}

// errBadLimit is the error of a malformed LIMIT argument.
var errBadLimit = &rangeError{"ERR syntax error"}

// cmdZIncrBy increments the score of a member of a sorted set.
func cmdZIncrBy(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "zincrby")
		return nil
	}
	delta, err := strconv.ParseFloat(string(ctx.Args[2]), 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not a valid float")
	}
	score, err := ctx.Store.ZIncrBy(ctx.Args[1], delta, ctx.Args[3])
	switch err {
	case nil:
		ctx.propagateCmd()
		return ctx.W.WriteBulkString([]byte(formatScore(score)))
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrNotFloat:
		return ctx.W.WriteError("ERR value is not a valid float")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// cmdZCount returns the number of members with a score in the given range.
func cmdZCount(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "zcount")
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
	n, err := ctx.Store.ZCount(ctx.Args[1], min.value, max.value, min.exclusive, max.exclusive)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

// cmdZMScore returns the scores of the given members.
func cmdZMScore(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "zmscore")
		return nil
	}
	scores, err := ctx.Store.ZMScore(ctx.Args[1], ctx.Args[2:])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if err := ctx.W.WriteArrayHeader(len(scores)); err != nil {
		return err
	}
	for _, s := range scores {
		if s == nil {
			if err := ctx.W.WriteNullBulk(); err != nil {
				return err
			}
			continue
		}
		if err := ctx.W.WriteBulkString([]byte(formatScore(*s))); err != nil {
			return err
		}
	}
	return nil
}

// cmdZRangeByScore returns the members with a score in the given range.
func cmdZRangeByScore(ctx *Ctx) error {
	return zrangeByScoreCommon(ctx, "zrangebyscore", false)
}

// cmdZRevRangeByScore returns the members with a score in the given range,
// ordered from the highest score.
func cmdZRevRangeByScore(ctx *Ctx) error {
	return zrangeByScoreCommon(ctx, "zrevrangebyscore", true)
}

func zrangeByScoreCommon(ctx *Ctx, cmd string, reverse bool) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	// The bounds are swapped for the reversed variant.
	first, second := string(ctx.Args[2]), string(ctx.Args[3])
	if reverse {
		first, second = second, first
	}
	min, err := parseScoreBound(first)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	max, err := parseScoreBound(second)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	withScores := false
	offset, count := 0, 0
	for i := 4; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "WITHSCORES":
			withScores = true
			i++
		case "LIMIT":
			offset, count, i, err = limitArgs(ctx.Args, i)
			if err != nil {
				return ctx.W.WriteError(err.Error())
			}
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}
	entries, err := ctx.Store.ZRangeByScore(ctx.Args[1], min.value, max.value, min.exclusive, max.exclusive, offset, count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if reverse {
		entries = reverseEntries(entries)
	}
	return writeZEntries(ctx, entries, withScores)
}

// cmdZRangeByLex returns the members in the given lexicographical range.
func cmdZRangeByLex(ctx *Ctx) error {
	return zrangeByLexCommon(ctx, "zrangebylex", false)
}

// cmdZRevRangeByLex returns the members in the given lexicographical range,
// in reverse order.
func cmdZRevRangeByLex(ctx *Ctx) error {
	return zrangeByLexCommon(ctx, "zrevrangebylex", true)
}

func zrangeByLexCommon(ctx *Ctx, cmd string, reverse bool) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	first, second := string(ctx.Args[2]), string(ctx.Args[3])
	if reverse {
		first, second = second, first
	}
	min, err := parseLexBound(first)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	max, err := parseLexBound(second)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	offset, count := 0, 0
	for i := 4; i < len(ctx.Args); {
		if strings.EqualFold(string(ctx.Args[i]), "LIMIT") {
			offset, count, i, err = limitArgs(ctx.Args, i)
			if err != nil {
				return ctx.W.WriteError(err.Error())
			}
			continue
		}
		return ctx.W.WriteError("ERR syntax error")
	}
	var minVal, maxVal []byte
	if !min.all {
		minVal = min.value
	}
	if !max.all {
		maxVal = max.value
	}
	entries, err := ctx.Store.ZRangeByLex(ctx.Args[1], minVal, maxVal, min.exclusive, max.exclusive, offset, count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if reverse {
		entries = reverseEntries(entries)
	}
	return writeZEntries(ctx, entries, false)
}

// cmdZLexCount returns the number of members in the given lexicographical
// range.
func cmdZLexCount(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "zlexcount")
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
	n, err := ctx.Store.ZLexCount(ctx.Args[1], minVal, maxVal, min.exclusive, max.exclusive)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	return ctx.W.WriteInteger(int64(n))
}

// zrangeByScoreInRange implements ZRANGE key min max BYSCORE. With REV the
// bounds are swapped (the first one is the maximum) and the members come in
// descending order, like in Redis.
func zrangeByScoreInRange(ctx *Ctx, withScores, rev bool, offset, count int) error {
	first, second := string(ctx.Args[2]), string(ctx.Args[3])
	if rev {
		first, second = second, first
	}
	min, err := parseScoreBound(first)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	max, err := parseScoreBound(second)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	entries, err := ctx.Store.ZRangeByScore(ctx.Args[1], min.value, max.value, min.exclusive, max.exclusive, offset, count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if rev {
		entries = reverseEntries(entries)
	}
	return writeZEntries(ctx, entries, withScores)
}

// zrangeByLexInRange implements ZRANGE key min max BYLEX. With REV the bounds
// are swapped, like in Redis.
func zrangeByLexInRange(ctx *Ctx, withScores, rev bool, offset, count int) error {
	first, second := string(ctx.Args[2]), string(ctx.Args[3])
	if rev {
		first, second = second, first
	}
	min, err := parseLexBound(first)
	if err != nil {
		return ctx.W.WriteError(err.Error())
	}
	max, err := parseLexBound(second)
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
	entries, err := ctx.Store.ZRangeByLex(ctx.Args[1], minVal, maxVal, min.exclusive, max.exclusive, offset, count)
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if rev {
		entries = reverseEntries(entries)
	}
	return writeZEntries(ctx, entries, withScores)
}

// reverseEntries returns the entries in reverse order.
func reverseEntries(entries []store.ZEntry) []store.ZEntry {
	out := make([]store.ZEntry, len(entries))
	for i, e := range entries {
		out[len(entries)-1-i] = e
	}
	return out
}

// writeZEntries writes the members of a sorted set, with their scores when
// requested.
func writeZEntries(ctx *Ctx, entries []store.ZEntry, withScores bool) error {
	if err := ctx.W.WriteArrayHeader(len(entries) * map[bool]int{true: 2, false: 1}[withScores]); err != nil {
		return err
	}
	for _, e := range entries {
		if err := ctx.W.WriteBulkString(e.Member); err != nil {
			return err
		}
		if withScores {
			if err := ctx.W.WriteBulkString([]byte(formatScore(e.Score))); err != nil {
				return err
			}
		}
	}
	return nil
}
