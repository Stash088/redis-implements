package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestSetOperations(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "a", "x", "y", "z")
	c.intWant("SADD", "b", "y", "z", "w")

	diff := c.arrWant("SDIFF", "a", "b")
	if len(diff) != 1 || string(diff[0].Str) != "x" {
		t.Errorf("SDIFF: got %+v, want [x]", diff)
	}
	inter := c.arrWant("SINTER", "a", "b")
	if len(inter) != 2 {
		t.Errorf("SINTER: got %+v, want 2 elements", inter)
	}
	union := c.arrWant("SUNION", "a", "b")
	if len(union) != 4 {
		t.Errorf("SUNION: got %+v, want 4 elements", union)
	}
	if n := c.intWant("SINTERCARD", "2", "a", "b"); n != 2 {
		t.Errorf("SINTERCARD: got %d, want 2", n)
	}
	// LIMIT stops the counting.
	if n := c.intWant("SINTERCARD", "2", "a", "b", "LIMIT", "1"); n != 1 {
		t.Errorf("SINTERCARD with LIMIT: got %d, want 1", n)
	}
}

func TestSetStoreOperations(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "a", "x", "y", "z")
	c.intWant("SADD", "b", "y", "z", "w")

	if n := c.intWant("SDIFFSTORE", "d", "a", "b"); n != 1 {
		t.Errorf("SDIFFSTORE: got %d, want 1", n)
	}
	if n := c.intWant("SINTERSTORE", "i", "a", "b"); n != 2 {
		t.Errorf("SINTERSTORE: got %d, want 2", n)
	}
	if n := c.intWant("SUNIONSTORE", "u", "a", "b"); n != 4 {
		t.Errorf("SUNIONSTORE: got %d, want 4", n)
	}
	// The stored sets are readable.
	if got := c.arrWant("SMEMBERS", "i"); len(got) != 2 {
		t.Errorf("SMEMBERS of the intersection: got %+v", got)
	}
}

func TestSMove(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "a", "x", "y")
	c.intWant("SADD", "b", "y")
	if n := c.intWant("SMOVE", "a", "b", "x"); n != 1 {
		t.Errorf("SMOVE: got %d, want 1", n)
	}
	if n := c.intWant("SMOVE", "a", "b", "nosuch"); n != 0 {
		t.Errorf("SMOVE of a missing member: got %d, want 0", n)
	}
	if got := c.arrWant("SMEMBERS", "a"); len(got) != 1 {
		t.Errorf("SMEMBERS after SMOVE: got %+v, want 1 member", got)
	}
	if got := c.arrWant("SMEMBERS", "b"); len(got) != 2 {
		t.Errorf("SMEMBERS of the destination: got %+v, want 2 members", got)
	}
}

func TestSPopAndSRandMember(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "s", "m1", "m2", "m3")

	member := c.bulkWant("SPOP", "s")
	if string(member) == "" {
		t.Error("SPOP: got an empty member")
	}
	// The set lost a member.
	if n := c.intWant("SCARD", "s"); n != 2 {
		t.Errorf("SCARD after SPOP: got %d, want 2", n)
	}
	// SPOP with a count returns an array.
	arr := c.arrWant("SPOP", "s", "2")
	if len(arr) != 2 {
		t.Errorf("SPOP with a count: got %+v, want 2 elements", arr)
	}
	// An empty set pops nothing.
	c.intWant("SREM", "s", "m1", "m2", "m3")
	if v := c.cmd("SPOP", "s"); !isNullBulk(v) {
		t.Errorf("SPOP of an empty set: got %+v, want null", v)
	}
	if v := c.cmd("SPOP", "s", "2"); v.Kind != resp.KindArray || len(v.Array) != 0 {
		t.Errorf("SPOP with a count of an empty set: got %+v, want an empty array", v)
	}
	// SRANDMEMBER does not remove anything.
	c.intWant("SADD", "s2", "a", "b")
	c.bulkWant("SRANDMEMBER", "s2")
	if n := c.intWant("SCARD", "s2"); n != 2 {
		t.Errorf("SCARD after SRANDMEMBER: got %d, want 2", n)
	}
	arr = c.arrWant("SRANDMEMBER", "s2", "5")
	if len(arr) != 2 {
		t.Errorf("SRANDMEMBER with a big count: got %+v, want 2 elements", arr)
	}
}

func TestSMIsMember(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "s", "a")
	arr := c.arrWant("SMISMEMBER", "s", "a", "b")
	if len(arr) != 2 {
		t.Fatalf("SMISMEMBER: got %d elements, want 2", len(arr))
	}
	if arr[0].Int != 1 || arr[1].Int != 0 {
		t.Errorf("SMISMEMBER: got %+v, want 1 and 0", arr)
	}
}

// TestSetCommandsWrongType checks that the set commands report a wrong type
// for keys of another type.
func TestSetCommandsWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"SINTERCARD", "1", "str"},
		{"SMOVE", "str", "other", "m"},
		{"SPOP", "str"},
		{"SRANDMEMBER", "str"},
		{"SMISMEMBER", "str", "m"},
		{"SDIFF", "str"},
		{"SINTER", "str"},
		{"SUNION", "str"},
		{"SDIFFSTORE", "dest", "str"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
