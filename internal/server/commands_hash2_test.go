package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestHIncrBy(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if n := c.intWant("HINCRBY", "h", "f", "5"); n != 5 {
		t.Errorf("HINCRBY: got %d, want 5", n)
	}
	if n := c.intWant("HINCRBY", "h", "f", "-2"); n != 3 {
		t.Errorf("HINCRBY: got %d, want 3", n)
	}
	// A missing field starts from zero.
	if n := c.intWant("HINCRBY", "h", "new", "3"); n != 3 {
		t.Errorf("HINCRBY on a new field: got %d, want 3", n)
	}
	// A field that is not a number is an error.
	c.intWant("HSET", "h", "s", "abc")
	if msg := c.expectError("HINCRBY", "h", "s", "1"); msg == "" {
		t.Error("HINCRBY on a non-number: want an error")
	}
}

func TestHIncrByFloat(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if got := string(c.bulkWant("HINCRBYFLOAT", "h", "f", "1.5")); got != "1.5" {
		t.Errorf("HINCRBYFLOAT: got %q, want 1.5", got)
	}
	if got := string(c.bulkWant("HINCRBYFLOAT", "h", "f", "1.0")); got != "2.5" {
		t.Errorf("HINCRBYFLOAT: got %q, want 2.5", got)
	}
	// A field that is not a number is an error.
	c.intWant("HSET", "h", "s", "abc")
	if msg := c.expectError("HINCRBYFLOAT", "h", "s", "1.0"); msg == "" {
		t.Error("HINCRBYFLOAT on a non-number: want an error")
	}
}

func TestHStrLenAndHMGet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("HSET", "h", "f", "value")
	if n := c.intWant("HSTRLEN", "h", "f"); n != 5 {
		t.Errorf("HSTRLEN: got %d, want 5", n)
	}
	// A missing field has a length of zero.
	if n := c.intWant("HSTRLEN", "h", "nofield"); n != 0 {
		t.Errorf("HSTRLEN of a missing field: got %d, want 0", n)
	}
	arr := c.arrWant("HMGET", "h", "f", "nofield")
	if len(arr) != 2 {
		t.Fatalf("HMGET: got %d elements, want 2", len(arr))
	}
	if string(arr[0].Str) != "value" {
		t.Errorf("HMGET: got %q, want value", arr[0].Str)
	}
	if !arr[1].IsNull {
		t.Errorf("HMGET of a missing field: got %+v, want null", arr[1])
	}
}

func TestHMSetAndHSetNX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("HMSET", "h", "a", "1", "b", "2")
	if got := string(c.bulkWant("HGET", "h", "a")); got != "1" {
		t.Errorf("HMGET after HMSET: got %q, want 1", got)
	}
	// HSETNX does not overwrite an existing field.
	if n := c.intWant("HSETNX", "h", "a", "9"); n != 0 {
		t.Errorf("HSETNX on an existing field: got %d, want 0", n)
	}
	if got := string(c.bulkWant("HGET", "h", "a")); got != "1" {
		t.Errorf("HSETNX must not overwrite: got %q, want 1", got)
	}
	if n := c.intWant("HSETNX", "h", "c", "3"); n != 1 {
		t.Errorf("HSETNX on a new field: got %d, want 1", n)
	}
}

func TestHRandField(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for _, f := range []string{"a", "b", "c"} {
		c.intWant("HSET", "h", f, "v"+f)
	}
	// One random field.
	field := c.bulkWant("HRANDFIELD", "h")
	if string(field) == "" {
		t.Error("HRANDFIELD: got an empty field")
	}
	// A missing key has no random field.
	if v := c.cmd("HRANDFIELD", "nosuch"); !isNullBulk(v) {
		t.Errorf("HRANDFIELD of a missing key: got %+v, want null", v)
	}
	// A count returns that many fields.
	arr := c.arrWant("HRANDFIELD", "h", "2")
	if len(arr) != 2 {
		t.Errorf("HRANDFIELD with a count: got %d elements, want 2", len(arr))
	}
	// WITHVALUES adds the values.
	arr = c.arrWant("HRANDFIELD", "h", "2", "WITHVALUES")
	if len(arr) != 4 {
		t.Errorf("HRANDFIELD WITHVALUES: got %d elements, want 4", len(arr))
	}
}

func TestHGetDel(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("HSET", "h", "a", "1")
	c.intWant("HSET", "h", "b", "2")
	// Redis 8 expects the FIELDS form and answers with the removed values.
	arr := c.arrWant("HGETDEL", "h", "FIELDS", "1", "a")
	if len(arr) != 1 || string(arr[0].Str) != "1" {
		t.Errorf("HGETDEL: got %+v, want [1]", arr)
	}
	// A missing field yields nil.
	arr = c.arrWant("HGETDEL", "h", "FIELDS", "1", "nosuch")
	if len(arr) != 1 || !arr[0].IsNull {
		t.Errorf("HGETDEL of a missing field: got %+v, want [nil]", arr)
	}
	if v := c.cmd("HGET", "h", "a"); !isNullBulk(v) {
		t.Errorf("HGET after HGETDEL: got %+v, want null", v)
	}
	// The number of fields must match.
	if msg := c.expectError("HGETDEL", "h", "FIELDS", "2", "b"); msg == "" {
		t.Error("HGETDEL with a wrong field count: want an error")
	}
}

// TestHashCommandsWrongType checks that the hash commands report a wrong type
// for keys of another type.
func TestHashCommandsWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"HINCRBY", "str", "f", "1"},
		{"HINCRBYFLOAT", "str", "f", "1.0"},
		{"HSTRLEN", "str", "f"},
		{"HMGET", "str", "f"},
		{"HMSET", "str", "f", "v"},
		{"HSETNX", "str", "f", "v"},
		{"HRANDFIELD", "str"},
		{"HGETDEL", "str", "FIELDS", "1", "f"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
