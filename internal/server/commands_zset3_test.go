package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestZSetOperations(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "a", "1", "x", "2", "y")
	c.intWant("ZADD", "b", "10", "x", "20", "z")

	if n := c.intWant("ZUNIONSTORE", "u", "2", "a", "b"); n != 3 {
		t.Errorf("ZUNIONSTORE: got %d, want 3", n)
	}
	// The scores of the union are summed.
	arr := c.arrWant("ZRANGE", "u", "0", "-1", "WITHSCORES")
	if len(arr) != 6 {
		t.Fatalf("ZRANGE of the union: got %d elements, want 6", len(arr))
	}
	if string(arr[3].Str) != "11" {
		t.Errorf("the score of x in the union: got %q, want 11", arr[3].Str)
	}
	// WEIGHTS scales the scores.
	c.intWant("ZUNIONSTORE", "uw", "2", "a", "b", "WEIGHTS", "2", "3")
	arr = c.arrWant("ZRANGE", "uw", "0", "-1", "WITHSCORES")
	if len(arr) != 6 || string(arr[3].Str) != "32" {
		t.Errorf("ZUNIONSTORE with weights: got %+v", arr)
	}
	// AGGREGATE MAX keeps the biggest score.
	c.intWant("ZUNIONSTORE", "um", "2", "a", "b", "AGGREGATE", "MAX")
	arr = c.arrWant("ZRANGE", "um", "0", "-1", "WITHSCORES")
	if len(arr) != 6 || string(arr[3].Str) != "10" {
		t.Errorf("ZUNIONSTORE with AGGREGATE MAX: got %+v", arr)
	}
	// The intersection holds the common members only.
	if n := c.intWant("ZINTERSTORE", "i", "2", "a", "b"); n != 1 {
		t.Errorf("ZINTERSTORE: got %d, want 1", n)
	}
	// The difference holds the members of the first set only.
	arr = c.arrWant("ZDIFF", "2", "a", "b")
	if len(arr) != 1 || string(arr[0].Str) != "y" {
		t.Errorf("ZDIFF: got %+v, want [y]", arr)
	}
	if n := c.intWant("ZDIFFSTORE", "d", "2", "a", "b"); n != 1 {
		t.Errorf("ZDIFFSTORE: got %d, want 1", n)
	}
	if n := c.intWant("ZINTERCARD", "2", "a", "b"); n != 1 {
		t.Errorf("ZINTERCARD: got %d, want 1", n)
	}
	// The errors of Redis.
	if msg := c.expectError("ZUNIONSTORE", "u", "0", "a"); msg == "" {
		t.Error("ZUNIONSTORE without keys: want an error")
	}
	if msg := c.expectError("ZUNIONSTORE", "u", "2", "a", "b", "AGGREGATE", "BOGUS"); msg == "" {
		t.Error("ZUNIONSTORE with a bad aggregate: want an error")
	}
}

func TestZRemRange(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	if n := c.intWant("ZREMRANGEBYRANK", "z", "0", "1"); n != 2 {
		t.Errorf("ZREMRANGEBYRANK: got %d, want 2", n)
	}
	arr := c.arrWant("ZRANGE", "z", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "c" {
		t.Errorf("ZRANGE after ZREMRANGEBYRANK: got %+v", arr)
	}
	if n := c.intWant("ZREMRANGEBYSCORE", "z", "3", "4"); n != 2 {
		t.Errorf("ZREMRANGEBYSCORE: got %d, want 2", n)
	}
	// A lexicographical range.
	c.intWant("ZADD", "lex", "0", "a", "0", "b", "0", "c")
	if n := c.intWant("ZREMRANGEBYLEX", "lex", "[a", "(c"); n != 2 {
		t.Errorf("ZREMRANGEBYLEX: got %d, want 2", n)
	}
}

func TestZPopAndZRandMember(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c")
	// ZPOPMIN returns the member with its score.
	arr := c.arrWant("ZPOPMIN", "z")
	if len(arr) != 2 || string(arr[0].Str) != "a" || string(arr[1].Str) != "1" {
		t.Errorf("ZPOPMIN: got %+v, want [a 1]", arr)
	}
	// ZPOPMAX with a count.
	arr = c.arrWant("ZPOPMAX", "z", "2")
	if len(arr) != 4 || string(arr[0].Str) != "c" {
		t.Errorf("ZPOPMAX with a count: got %+v", arr)
	}
	// An empty set pops nothing.
	if v := c.cmd("ZPOPMIN", "z"); v.Kind != resp.KindArray || len(v.Array) != 0 {
		t.Errorf("ZPOPMIN of an empty set: got %+v, want an empty array", v)
	}
	// ZRANDMEMBER does not remove anything.
	c.intWant("ZADD", "z2", "1", "a", "2", "b")
	c.bulkWant("ZRANDMEMBER", "z2")
	if n := c.intWant("ZCARD", "z2"); n != 2 {
		t.Errorf("ZCARD after ZRANDMEMBER: got %d, want 2", n)
	}
	arr = c.arrWant("ZRANDMEMBER", "z2", "1", "WITHSCORES")
	if len(arr) != 2 {
		t.Errorf("ZRANDMEMBER with WITHSCORES: got %+v", arr)
	}
}

func TestZRangeStore(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "src", "1", "a", "2", "b", "3", "c")
	// Without BYSCORE or BYLEX the bounds are rank indexes.
	if n := c.intWant("ZRANGESTORE", "dst", "src", "0", "1"); n != 2 {
		t.Errorf("ZRANGESTORE: got %d, want 2", n)
	}
	arr := c.arrWant("ZRANGE", "dst", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "a" {
		t.Errorf("ZRANGE of the stored range: got %+v", arr)
	}
	// With BYSCORE the bounds are scores.
	if n := c.intWant("ZRANGESTORE", "dst2", "src", "2", "3", "BYSCORE"); n != 2 {
		t.Errorf("ZRANGESTORE BYSCORE: got %d, want 2", n)
	}
	arr = c.arrWant("ZRANGE", "dst2", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "b" {
		t.Errorf("ZRANGE of the stored score range: got %+v", arr)
	}
}

// TestZSetOperationsWrongType checks that the sorted set operations report a
// wrong type for keys of another type.
func TestZSetOperationsWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"ZUNIONSTORE", "dst", "1", "str"},
		{"ZINTERSTORE", "dst", "1", "str"},
		{"ZDIFF", "1", "str"},
		{"ZDIFFSTORE", "dst", "1", "str"},
		{"ZINTERCARD", "1", "str"},
		{"ZREMRANGEBYRANK", "str", "0", "-1"},
		{"ZREMRANGEBYSCORE", "str", "0", "1"},
		{"ZREMRANGEBYLEX", "str", "-", "+"},
		{"ZPOPMIN", "str"},
		{"ZRANDMEMBER", "str"},
		{"ZRANGESTORE", "dst", "str", "0", "-1"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
