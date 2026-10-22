package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestLInsert(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a", "b", "c")
	if n := c.intWant("LINSERT", "l", "BEFORE", "b", "X"); n != 4 {
		t.Errorf("LINSERT BEFORE: got %d, want 4", n)
	}
	if n := c.intWant("LINSERT", "l", "AFTER", "c", "Y"); n != 5 {
		t.Errorf("LINSERT AFTER: got %d, want 5", n)
	}
	arr := c.arrWant("LRANGE", "l", "0", "-1")
	want := []string{"a", "X", "b", "c", "Y"}
	if len(arr) != len(want) {
		t.Fatalf("LRANGE: got %d elements, want %d", len(arr), len(want))
	}
	for i, w := range want {
		if string(arr[i].Str) != w {
			t.Errorf("LRANGE[%d]: got %q, want %q", i, arr[i].Str, w)
		}
	}
	// A missing pivot gives -1, a missing key gives 0, like in Redis.
	if n := c.intWant("LINSERT", "l", "BEFORE", "nosuch", "Z"); n != -1 {
		t.Errorf("LINSERT with a missing pivot: got %d, want -1", n)
	}
	if n := c.intWant("LINSERT", "nosuchkey", "BEFORE", "a", "Z"); n != 0 {
		t.Errorf("LINSERT with a missing key: got %d, want 0", n)
	}
}

func TestLRemAndLTrim(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a", "b", "a", "c", "a")
	// A positive count removes from the head.
	if n := c.intWant("LREM", "l", "2", "a"); n != 2 {
		t.Errorf("LREM: got %d, want 2", n)
	}
	arr := c.arrWant("LRANGE", "l", "0", "-1")
	if len(arr) != 3 {
		t.Errorf("LRANGE after LREM: got %+v, want 3 elements", arr)
	}
	// A count of zero removes all of them.
	c.intWant("RPUSH", "l2", "x", "y", "x")
	if n := c.intWant("LREM", "l2", "0", "x"); n != 2 {
		t.Errorf("LREM 0: got %d, want 2", n)
	}
	// LTRIM keeps the range.
	c.intWant("RPUSH", "l3", "1", "2", "3", "4", "5")
	c.ok("LTRIM", "l3", "1", "-2")
	arr = c.arrWant("LRANGE", "l3", "0", "-1")
	if len(arr) != 3 || string(arr[0].Str) != "2" {
		t.Errorf("LRANGE after LTRIM: got %+v", arr)
	}
}

func TestLPos(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a", "b", "a", "c", "a")
	// A single position without COUNT.
	if n := c.intWant("LPOS", "l", "a"); n != 0 {
		t.Errorf("LPOS: got %d, want 0", n)
	}
	// A missing element gives nil.
	if v := c.cmd("LPOS", "l", "zzz"); !isNullBulk(v) {
		t.Errorf("LPOS of a missing element: got %+v, want null", v)
	}
	// COUNT returns several positions.
	arr := c.arrWant("LPOS", "l", "a", "COUNT", "2")
	if len(arr) != 2 || arr[0].Int != 0 || arr[1].Int != 2 {
		t.Errorf("LPOS COUNT: got %+v, want [0 2]", arr)
	}
	// RANK -1 starts from the tail.
	if n := c.intWant("LPOS", "l", "a", "RANK", "-1"); n != 4 {
		t.Errorf("LPOS RANK -1: got %d, want 4", n)
	}
	// MAXLEN limits how many elements are scanned, not the result.
	arr = c.arrWant("LPOS", "l", "a", "COUNT", "0", "MAXLEN", "2")
	if len(arr) != 1 || arr[0].Int != 0 {
		t.Errorf("LPOS MAXLEN: got %+v, want [0]", arr)
	}
	// A negative COUNT or MAXLEN is an error.
	if msg := c.expectError("LPOS", "l", "a", "COUNT", "-1"); msg == "" {
		t.Error("LPOS with a negative COUNT: want an error")
	}
	if msg := c.expectError("LPOS", "l", "a", "MAXLEN", "-1"); msg == "" {
		t.Error("LPOS with a negative MAXLEN: want an error")
	}
}

func TestLMoveAndRPopLPush(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "src", "a", "b", "c")
	if got := string(c.bulkWant("LMOVE", "src", "dst", "LEFT", "RIGHT")); got != "a" {
		t.Errorf("LMOVE: got %q, want a", got)
	}
	arr := c.arrWant("LRANGE", "dst", "0", "-1")
	if len(arr) != 1 || string(arr[0].Str) != "a" {
		t.Errorf("the destination list: got %+v", arr)
	}
	// RPOPLPUSH moves the tail to the head.
	if got := string(c.bulkWant("RPOPLPUSH", "src", "dst2")); got != "c" {
		t.Errorf("RPOPLPUSH: got %q, want c", got)
	}
	arr = c.arrWant("LRANGE", "dst2", "0", "-1")
	if len(arr) != 1 || string(arr[0].Str) != "c" {
		t.Errorf("the destination list: got %+v", arr)
	}
	// Moving from an empty list gives nil.
	if v := c.cmd("LMOVE", "nosuchkey", "dst", "LEFT", "RIGHT"); !isNullBulk(v) {
		t.Errorf("LMOVE from a missing list: got %+v, want null", v)
	}
}

func TestLPushXAndRPushX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a")
	if n := c.intWant("LPUSHX", "l", "X"); n != 2 {
		t.Errorf("LPUSHX: got %d, want 2", n)
	}
	arr := c.arrWant("LRANGE", "l", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "X" {
		t.Errorf("LRANGE after LPUSHX: got %+v", arr)
	}
	if n := c.intWant("RPUSHX", "l", "Y"); n != 3 {
		t.Errorf("RPUSHX: got %d, want 3", n)
	}
	// A missing key is not created.
	if n := c.intWant("LPUSHX", "nosuchkey", "X"); n != 0 {
		t.Errorf("LPUSHX on a missing key: got %d, want 0", n)
	}
	if n := c.intWant("EXISTS", "nosuchkey"); n != 0 {
		t.Errorf("LPUSHX must not create the key: EXISTS = %d", n)
	}
}

// TestListCommandsWrongType checks that the list commands report a wrong type
// for keys of another type.
func TestListCommandsWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"LINSERT", "str", "BEFORE", "a", "b"},
		{"LREM", "str", "0", "a"},
		{"LTRIM", "str", "0", "-1"},
		{"LPOS", "str", "a"},
		{"LMOVE", "str", "dst", "LEFT", "RIGHT"},
		{"RPOPLPUSH", "str", "dst"},
		{"LPUSHX", "str", "a"},
		{"RPUSHX", "str", "a"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
