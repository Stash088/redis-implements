package server_test

import (
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

// TestLoopModeCommands checks that the event loop serves the commands like
// the goroutine model: the dispatch is the same, only the I/O differs.
func TestLoopModeCommands(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)

	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Fatalf("PING: got %+v", v)
	}
	c.ok("SET", "k", "v")
	if v := c.bulkWant("GET", "k"); string(v) != "v" {
		t.Errorf("GET: got %q, want v", v)
	}
	if n := c.intWant("STRLEN", "k"); n != 1 {
		t.Errorf("STRLEN: got %d, want 1", n)
	}
	if n := c.intWant("INCR", "counter"); n != 1 {
		t.Errorf("INCR: got %d, want 1", n)
	}
	c.intWant("HSET", "h", "f", "v")
	arr := c.arrWant("HGETALL", "h")
	if len(arr) != 2 || string(arr[0].Str) != "f" || string(arr[1].Str) != "v" {
		t.Errorf("HGETALL: got %+v", arr)
	}
	c.intWant("SADD", "s", "m")
	if n := c.intWant("SCARD", "s"); n != 1 {
		t.Errorf("SCARD: got %d, want 1", n)
	}
	c.intWant("LPUSH", "l", "a", "b")
	arr = c.arrWant("LRANGE", "l", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "b" || string(arr[1].Str) != "a" {
		t.Errorf("LRANGE: got %+v", arr)
	}
	if n := c.intWant("DEL", "k"); n != 1 {
		t.Errorf("DEL: got %d, want 1", n)
	}
	if v := c.cmd("GET", "k"); !v.IsNull {
		t.Errorf("GET of a missing key: got %+v, want null", v)
	}
}

// TestLoopModePipeline checks that the commands of a pipeline are answered in
// one write: the event loop runs the commands of the buffer and writes the
// replies together.
func TestLoopModePipeline(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	conn, r, w := c.conn, c.r, c.w
	defer conn.Close()

	for i := 0; i < 100; i++ {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("SET")),
			resp.NewBulkString([]byte("key")),
			resp.NewBulkString([]byte("value")),
		})
		if err := w.WriteValue(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		v, err := r.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
			t.Fatalf("the answer %d is %+v", i, v)
		}
	}
}

// TestLoopModeProtocolError checks that a wrong protocol is answered and the
// connection is closed, like in the goroutine model.
func TestLoopModeProtocolError(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	conn, r, w := c.conn, c.r, c.w
	defer conn.Close()

	if err := w.WriteRaw([]byte("+PING\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "Protocol error") {
		t.Fatalf("got %+v, want a protocol error", v)
	}
}

// TestLoopModeNotServedCommands checks that the commands that need a
// goroutine of their own are answered with an error in the event loop mode:
// they are not served yet.
func TestLoopModeNotServedCommands(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	for _, args := range [][]string{
		{"BLPOP", "l", "0"},
		{"SUBSCRIBE", "ch"},
	} {
		v := c.cmd(args...)
		if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "event loop") {
			t.Errorf("%v: got %+v, want an error", args, v)
		}
	}
}
