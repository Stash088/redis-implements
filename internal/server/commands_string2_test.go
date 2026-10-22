package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestMSetMGet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MSET", "a", "1", "b", "2", "c", "3")
	arr := c.arrWant("MGET", "a", "b", "c")
	if len(arr) != 3 {
		t.Fatalf("MGET: got %d elements, want 3", len(arr))
	}
	for i, want := range []string{"1", "2", "3"} {
		if string(arr[i].Str) != want {
			t.Errorf("MGET[%d]: got %q, want %q", i, arr[i].Str, want)
		}
	}
	// A missing key yields nil.
	arr = c.arrWant("MGET", "a", "nosuch")
	if len(arr) != 2 || !arr[1].IsNull {
		t.Errorf("MGET with a missing key: got %+v", arr)
	}
	// A key of another type yields nil too, not an error.
	c.intWant("RPUSH", "l", "x")
	arr = c.arrWant("MGET", "l")
	if len(arr) != 1 || !arr[0].IsNull {
		t.Errorf("MGET of a list: got %+v, want nil", arr)
	}
}

func TestMSetNX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if n := c.intWant("MSETNX", "a", "1", "b", "2"); n != 1 {
		t.Errorf("MSETNX: got %d, want 1", n)
	}
	// One of the keys exists: nothing is written.
	if n := c.intWant("MSETNX", "a", "9", "c", "3"); n != 0 {
		t.Errorf("MSETNX with an existing key: got %d, want 0", n)
	}
	if v := c.cmd("GET", "c"); !isNullBulk(v) {
		t.Errorf("MSETNX must not write anything: GET c = %+v", v)
	}
}

func TestGetSetGetDel(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if got := string(c.bulkWant("GETSET", "k", "v2")); got != "v" {
		t.Errorf("GETSET: got %q, want v", got)
	}
	if got := string(c.bulkWant("GET", "k")); got != "v2" {
		t.Errorf("GET after GETSET: got %q, want v2", got)
	}
	if got := string(c.bulkWant("GETDEL", "k")); got != "v2" {
		t.Errorf("GETDEL: got %q, want v2", got)
	}
	if v := c.cmd("GET", "k"); !isNullBulk(v) {
		t.Errorf("GET after GETDEL: got %+v, want null", v)
	}
	// GETSET on a missing key returns nil.
	if v := c.cmd("GETSET", "new", "v"); !isNullBulk(v) {
		t.Errorf("GETSET on a missing key: got %+v, want null", v)
	}
}

func TestGetEx(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if got := string(c.bulkWant("GETEX", "k", "EX", "100")); got != "v" {
		t.Errorf("GETEX: got %q, want v", got)
	}
	ttl := c.intWant("TTL", "k")
	if ttl <= 0 || ttl > 100 {
		t.Errorf("TTL after GETEX EX: got %d, want 0 < ttl <= 100", ttl)
	}
	// PERSIST removes the expiration.
	c.bulkWant("GETEX", "k", "PERSIST")
	if ttl := c.intWant("TTL", "k"); ttl != -1 {
		t.Errorf("TTL after GETEX PERSIST: got %d, want -1", ttl)
	}
}

func TestSetExSetNX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SETEX", "k", "100", "v")
	if ttl := c.intWant("TTL", "k"); ttl <= 0 || ttl > 100 {
		t.Errorf("TTL after SETEX: got %d", ttl)
	}
	c.ok("PSETEX", "k2", "100000", "v")
	if ttl := c.intWant("TTL", "k2"); ttl <= 0 || ttl > 100 {
		t.Errorf("TTL after PSETEX: got %d", ttl)
	}
	// A non-positive expiration is an error.
	if msg := c.expectError("SETEX", "k3", "0", "v"); msg == "" {
		t.Error("SETEX with 0: want an error")
	}
	// SETNX writes only when the key is missing.
	if n := c.intWant("SETNX", "k4", "v"); n != 1 {
		t.Errorf("SETNX: got %d, want 1", n)
	}
	if n := c.intWant("SETNX", "k4", "v2"); n != 0 {
		t.Errorf("SETNX on an existing key: got %d, want 0", n)
	}
}

func TestGetRangeSetRange(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "Hello World")
	if got := string(c.bulkWant("GETRANGE", "k", "0", "4")); got != "Hello" {
		t.Errorf("GETRANGE: got %q, want Hello", got)
	}
	if got := string(c.bulkWant("GETRANGE", "k", "-5", "-1")); got != "World" {
		t.Errorf("GETRANGE with negative indexes: got %q, want World", got)
	}
	if got := string(c.bulkWant("GETRANGE", "k", "10", "100")); got != "d" {
		t.Errorf("GETRANGE out of range: got %q, want d", got)
	}
	// SETRANGE pads the string with zero bytes.
	if n := c.intWant("SETRANGE", "k2", "5", "hello"); n != 10 {
		t.Errorf("SETRANGE: got length %d, want 10", n)
	}
	if n := c.intWant("STRLEN", "k2"); n != 10 {
		t.Errorf("STRLEN after SETRANGE: got %d, want 10", n)
	}
	if msg := c.expectError("SETRANGE", "k3", "-1", "x"); msg == "" {
		t.Error("SETRANGE with a negative offset: want an error")
	}
}

func TestIncrByFloat(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "f", "10.5")
	// Redis prints the exact decimal value with up to 17 significant digits.
	if got := string(c.bulkWant("INCRBYFLOAT", "f", "0.1")); got != "10.59999999999999964" {
		t.Errorf("INCRBYFLOAT: got %q", got)
	}
	if got := string(c.bulkWant("INCRBYFLOAT", "f", "-2.5")); got != "8.09999999999999964" {
		t.Errorf("INCRBYFLOAT: got %q", got)
	}
	// A value that is not a number is an error.
	c.ok("SET", "s", "abc")
	if msg := c.expectError("INCRBYFLOAT", "s", "1.0"); msg == "" {
		t.Error("INCRBYFLOAT on a non-number: want an error")
	}
	// The TTL is preserved, like in Redis.
	c.ok("SET", "t", "1.0", "EX", "100")
	c.bulkWant("INCRBYFLOAT", "t", "1.0")
	if ttl := c.intWant("TTL", "t"); ttl <= 0 {
		t.Errorf("TTL after INCRBYFLOAT: got %d, want > 0", ttl)
	}
}

// TestStringCommandsWrongType checks that the string commands report a
// wrong type for keys of other types.
func TestStringCommandsWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "x")
	for _, cmd := range [][]string{
		{"GET", "l"},
		{"GETRANGE", "l", "0", "1"},
		{"SETRANGE", "l", "0", "x"},
		{"INCRBYFLOAT", "l", "1"},
		{"GETSET", "l", "v"},
		{"APPEND", "l", "v"},
		{"STRLEN", "l"},
	} {
		v := c.cmd(cmd...)
		if v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
