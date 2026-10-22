package server_test

import (
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestXAddAndXLen(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if got := string(c.bulkWant("XADD", "s", "1-0", "f", "v")); got != "1-0" {
		t.Errorf("XADD: got %q, want 1-0", got)
	}
	if got := string(c.bulkWant("XADD", "s", "2-0", "f", "v2")); got != "2-0" {
		t.Errorf("XADD: got %q, want 2-0", got)
	}
	if n := c.intWant("XLEN", "s"); n != 2 {
		t.Errorf("XLEN: got %d, want 2", n)
	}
	// A missing stream has no entries.
	if n := c.intWant("XLEN", "nosuch"); n != 0 {
		t.Errorf("XLEN of a missing stream: got %d, want 0", n)
	}
	// The type of the key is stream.
	if got := string(c.cmd("TYPE", "s").Str); got != "stream" {
		t.Errorf("TYPE: got %q, want stream", got)
	}
}

func TestXAddAutoID(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	first := string(c.bulkWant("XADD", "s", "*", "f", "v"))
	if !strings.Contains(first, "-") {
		t.Fatalf("XADD with *: got %q, want an identifier", first)
	}
	// Two entries added in the same millisecond get different sequence
	// numbers.
	second := string(c.bulkWant("XADD", "s", "*", "f", "v2"))
	if first == second {
		t.Errorf("XADD with *: got the same identifier %q twice", first)
	}
	if n := c.intWant("XLEN", "s"); n != 2 {
		t.Errorf("XLEN: got %d, want 2", n)
	}
}

func TestXAddErrors(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if msg := c.expectError("XADD", "k", "1-0", "f", "v"); !strings.Contains(msg, "WRONGTYPE") {
		t.Errorf("XADD on a string: got %q, want WRONGTYPE", msg)
	}
	if msg := c.expectError("XADD", "s", "abc", "f", "v"); !strings.Contains(msg, "Invalid stream ID") {
		t.Errorf("XADD with a bad identifier: got %q, want an invalid ID error", msg)
	}
	if msg := c.expectError("XADD", "s", "0-0", "f", "v"); !strings.Contains(msg, "must be greater than 0-0") {
		t.Errorf("XADD with 0-0: got %q, want the 0-0 error", msg)
	}
	// The identifier must be greater than the last entry.
	c.bulkWant("XADD", "s", "5-0", "f", "v")
	if msg := c.expectError("XADD", "s", "5-0", "f", "v"); !strings.Contains(msg, "equal or smaller") {
		t.Errorf("XADD with an old identifier: got %q, want the monotonicity error", msg)
	}
	// A field without a value is an error.
	if msg := c.expectError("XADD", "s", "6-0", "f"); msg == "" {
		t.Error("XADD with an odd number of arguments: want an error")
	}
}

func TestXAddNoMkStream(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	v := c.cmd("XADD", "nosuch", "NOMKSTREAM", "*", "f", "v")
	if !v.IsNull {
		t.Errorf("XADD with NOMKSTREAM: got %+v, want null", v)
	}
	if n := c.intWant("XLEN", "nosuch"); n != 0 {
		t.Errorf("XLEN: got %d, want 0", n)
	}
}

func TestXRange(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.bulkWant("XADD", "s", "2-0", "f", "v2")
	c.bulkWant("XADD", "s", "3-0", "f", "v3")

	arr := c.arrWant("XRANGE", "s", "-", "+")
	if len(arr) != 3 {
		t.Fatalf("XRANGE: got %d entries, want 3", len(arr))
	}
	// Every entry is a pair: the identifier and the list of the fields.
	if len(arr[0].Array) != 2 {
		t.Fatalf("XRANGE: got %d elements in an entry, want 2", len(arr[0].Array))
	}
	if string(arr[0].Array[0].Str) != "1-0" {
		t.Errorf("XRANGE: got the identifier %q, want 1-0", arr[0].Array[0].Str)
	}
	fields := arr[0].Array[1].Array
	if len(fields) != 2 || string(fields[0].Str) != "f" || string(fields[1].Str) != "v1" {
		t.Errorf("XRANGE: got the fields %+v, want f v1", fields)
	}
	// The range is inclusive.
	arr = c.arrWant("XRANGE", "s", "2-0", "2-0")
	if len(arr) != 1 {
		t.Errorf("XRANGE: got %d entries, want 1", len(arr))
	}
	// The count limits the number of entries.
	arr = c.arrWant("XRANGE", "s", "-", "+", "COUNT", "2")
	if len(arr) != 2 {
		t.Errorf("XRANGE with COUNT: got %d entries, want 2", len(arr))
	}
	// The reverse order starts at the end.
	arr = c.arrWant("XREVRANGE", "s", "+", "-")
	if len(arr) != 3 || string(arr[0].Array[0].Str) != "3-0" {
		t.Errorf("XREVRANGE: got %d entries starting at %q, want 3 starting at 3-0", len(arr), arr[0].Array[0].Str)
	}
	// A missing stream has no entries.
	arr = c.arrWant("XRANGE", "nosuch", "-", "+")
	if len(arr) != 0 {
		t.Errorf("XRANGE of a missing stream: got %d entries, want 0", len(arr))
	}
}

func TestXDel(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.bulkWant("XADD", "s", "2-0", "f", "v2")
	if n := c.intWant("XDEL", "s", "1-0"); n != 1 {
		t.Errorf("XDEL: got %d, want 1", n)
	}
	// Deleting a missing entry removes nothing.
	if n := c.intWant("XDEL", "s", "9-0"); n != 0 {
		t.Errorf("XDEL of a missing entry: got %d, want 0", n)
	}
	if n := c.intWant("XLEN", "s"); n != 1 {
		t.Errorf("XLEN: got %d, want 1", n)
	}
}

func TestXTrim(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for i := 1; i <= 5; i++ {
		c.bulkWant("XADD", "s", string(rune('0'+i))+"-0", "f", "v")
	}
	if n := c.intWant("XTRIM", "s", "MAXLEN", "=", "2"); n != 3 {
		t.Errorf("XTRIM MAXLEN: got %d, want 3", n)
	}
	if n := c.intWant("XLEN", "s"); n != 2 {
		t.Errorf("XLEN: got %d, want 2", n)
	}
	// Trimming a missing stream removes nothing.
	if n := c.intWant("XTRIM", "nosuch", "MAXLEN", "=", "2"); n != 0 {
		t.Errorf("XTRIM of a missing stream: got %d, want 0", n)
	}
	// A trimming strategy is required.
	if msg := c.expectError("XTRIM", "s", "FOO", "2"); msg == "" {
		t.Error("XTRIM without a strategy: want an error")
	}
}

func TestXSetID(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v")
	c.ok("XSETID", "s", "10-0")
	// The identifier cannot be smaller than the top entry.
	if msg := c.expectError("XSETID", "s", "0-5"); !strings.Contains(msg, "smaller than the target stream top item") {
		t.Errorf("XSETID with a small identifier: got %q, want the monotonicity error", msg)
	}
	// A missing stream is an error.
	if msg := c.expectError("XSETID", "nosuch", "1-0"); !strings.Contains(msg, "no such key") {
		t.Errorf("XSETID of a missing stream: got %q, want no such key", msg)
	}
}

func TestStreamWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if msg := c.expectError("XLEN", "k"); !strings.Contains(msg, "WRONGTYPE") {
		t.Errorf("XLEN on a string: got %q, want WRONGTYPE", msg)
	}
	if msg := c.expectError("XRANGE", "k", "-", "+"); !strings.Contains(msg, "WRONGTYPE") {
		t.Errorf("XRANGE on a string: got %q, want WRONGTYPE", msg)
	}
	if msg := c.expectError("XDEL", "k", "1-0"); !strings.Contains(msg, "WRONGTYPE") {
		t.Errorf("XDEL on a string: got %q, want WRONGTYPE", msg)
	}
	if msg := c.expectError("XTRIM", "k", "MAXLEN", "=", "1"); !strings.Contains(msg, "WRONGTYPE") {
		t.Errorf("XTRIM on a string: got %q, want WRONGTYPE", msg)
	}
}

func TestStreamInTransaction(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")
	if v := c.cmd("XADD", "s", "1-0", "f", "v"); v.Kind != resp.KindSimpleString || string(v.Str) != "QUEUED" {
		t.Fatalf("queued XADD: got %+v, want +QUEUED", v)
	}
	arr := c.arrWant("EXEC")
	if len(arr) != 1 {
		t.Fatalf("EXEC: got %d elements, want 1", len(arr))
	}
	if arr[0].Kind != resp.KindBulkString || string(arr[0].Str) != "1-0" {
		t.Errorf("EXEC: got %+v, want 1-0", arr[0])
	}
	if n := c.intWant("XLEN", "s"); n != 1 {
		t.Errorf("XLEN: got %d, want 1", n)
	}
}
