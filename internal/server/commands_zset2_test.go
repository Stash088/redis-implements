package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestZIncrBy(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if got := string(c.bulkWant("ZINCRBY", "z", "1.5", "m")); got != "1.5" {
		t.Errorf("ZINCRBY on a new member: got %q, want 1.5", got)
	}
	if got := string(c.bulkWant("ZINCRBY", "z", "2", "m")); got != "3.5" {
		t.Errorf("ZINCRBY: got %q, want 3.5", got)
	}
	// A member that is not a number is an error.
	c.intWant("HSET", "h", "f", "v")
	if v := c.cmd("ZINCRBY", "h", "1", "m"); v.Kind != resp.KindError {
		t.Errorf("ZINCRBY on a hash: got %+v, want an error", v)
	}
}

func TestZCountAndZMScore(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	if n := c.intWant("ZCOUNT", "z", "2", "4"); n != 3 {
		t.Errorf("ZCOUNT: got %d, want 3", n)
	}
	// An exclusive bound excludes the boundary.
	if n := c.intWant("ZCOUNT", "z", "(2", "+inf"); n != 2 {
		t.Errorf("ZCOUNT with an exclusive bound: got %d, want 2", n)
	}
	// A malformed bound is an error.
	if msg := c.expectError("ZCOUNT", "z", "bad", "4"); msg == "" {
		t.Error("ZCOUNT with a bad bound: want an error")
	}
	arr := c.arrWant("ZMSCORE", "z", "a", "b", "nosuch")
	if len(arr) != 3 {
		t.Fatalf("ZMSCORE: got %d elements, want 3", len(arr))
	}
	if string(arr[0].Str) != "1" || string(arr[1].Str) != "2" {
		t.Errorf("ZMSCORE: got %+v", arr)
	}
	if !arr[2].IsNull {
		t.Errorf("ZMSCORE of a missing member: got %+v, want null", arr[2])
	}
}

func TestZRangeByScore(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	arr := c.arrWant("ZRANGEBYSCORE", "z", "2", "3")
	if len(arr) != 2 || string(arr[0].Str) != "b" || string(arr[1].Str) != "c" {
		t.Errorf("ZRANGEBYSCORE: got %+v, want [b c]", arr)
	}
	// An exclusive bound.
	arr = c.arrWant("ZRANGEBYSCORE", "z", "(2", "3")
	if len(arr) != 1 || string(arr[0].Str) != "c" {
		t.Errorf("ZRANGEBYSCORE with an exclusive bound: got %+v, want [c]", arr)
	}
	// WITHSCORES.
	arr = c.arrWant("ZRANGEBYSCORE", "z", "2", "2", "WITHSCORES")
	if len(arr) != 2 || string(arr[0].Str) != "b" || string(arr[1].Str) != "2" {
		t.Errorf("ZRANGEBYSCORE WITHSCORES: got %+v", arr)
	}
	// LIMIT.
	arr = c.arrWant("ZRANGEBYSCORE", "z", "1", "4", "LIMIT", "1", "2")
	if len(arr) != 2 || string(arr[0].Str) != "b" {
		t.Errorf("ZRANGEBYSCORE LIMIT: got %+v, want [b c]", arr)
	}
	// The reversed variant.
	arr = c.arrWant("ZREVRANGEBYSCORE", "z", "3", "2")
	if len(arr) != 2 || string(arr[0].Str) != "c" {
		t.Errorf("ZREVRANGEBYSCORE: got %+v, want [c b]", arr)
	}
}

func TestZRangeByScoreVariants(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	// ZRANGE with BYSCORE takes the bounds before the option.
	arr := c.arrWant("ZRANGE", "z", "2", "3", "BYSCORE")
	if len(arr) != 2 || string(arr[0].Str) != "b" {
		t.Errorf("ZRANGE BYSCORE: got %+v, want [b c]", arr)
	}
	// With REV the bounds are swapped.
	arr = c.arrWant("ZRANGE", "z", "3", "2", "BYSCORE", "REV")
	if len(arr) != 2 || string(arr[0].Str) != "c" {
		t.Errorf("ZRANGE BYSCORE REV: got %+v, want [c b]", arr)
	}
	// REV without BYSCORE applies the indexes to the reversed order.
	arr = c.arrWant("ZRANGE", "z", "0", "1", "REV")
	if len(arr) != 2 || string(arr[0].Str) != "d" {
		t.Errorf("ZRANGE REV: got %+v, want [d c]", arr)
	}
}

func TestZRangeByLex(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	// A lexicographical range only makes sense with one score.
	c.intWant("ZADD", "z", "0", "a", "0", "aa", "0", "b", "0", "bb", "0", "c")
	arr := c.arrWant("ZRANGEBYLEX", "z", "[a", "(c")
	if len(arr) != 4 {
		t.Errorf("ZRANGEBYLEX: got %+v, want 4 elements", arr)
	}
	if n := c.intWant("ZLEXCOUNT", "z", "[a", "[b"); n != 3 {
		t.Errorf("ZLEXCOUNT: got %d, want 3", n)
	}
	// LIMIT.
	arr = c.arrWant("ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "2")
	if len(arr) != 2 || string(arr[0].Str) != "aa" {
		t.Errorf("ZRANGEBYLEX LIMIT: got %+v, want [aa b]", arr)
	}
	// A malformed bound is an error.
	if msg := c.expectError("ZRANGEBYLEX", "z", "a", "b"); msg == "" {
		t.Error("ZRANGEBYLEX with a bad bound: want an error")
	}
}

// TestZSetCommandsWrongType checks that the sorted set commands report a
// wrong type for keys of another type.
func TestZSetCommandsWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"ZINCRBY", "str", "1", "m"},
		{"ZCOUNT", "str", "1", "2"},
		{"ZMSCORE", "str", "m"},
		{"ZRANGEBYSCORE", "str", "1", "2"},
		{"ZREVRANGEBYSCORE", "str", "2", "1"},
		{"ZRANGEBYLEX", "str", "-", "+"},
		{"ZLEXCOUNT", "str", "-", "+"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
