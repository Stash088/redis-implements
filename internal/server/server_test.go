package server_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	return startTestServerStore(t, store.New(), true)
}

// startTestServerLoop runs a server that serves the connections with an event
// loop instead of a goroutine each.
func startTestServerLoop(t *testing.T) string {
	t.Helper()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()
	srv, err := server.NewLoop("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		t.Fatalf("server.NewLoop: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

// startTestServerStore runs a server against the given store. expirer enables
// the background expiry loop; tests using a fake clock disable it.
func startTestServerStore(t *testing.T, st *store.Store, expirer bool) string {
	t.Helper()
	if expirer {
		exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
		exp.Start()
		srv, err := server.New("127.0.0.1:0", []*store.Store{st})
		if err != nil {
			t.Fatalf("server.New: %v", err)
		}
		srv.SetLogger(log.New(io.Discard, "", 0))
		srv.Expirer = exp
		go srv.Serve()
		t.Cleanup(func() { srv.Close() })
		return srv.Addr().String()
	}
	srv, err := server.New("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

// startTestServerDBs runs a server with the given number of databases.
func startTestServerDBs(t *testing.T, n int) string {
	t.Helper()
	dbs := make([]*store.Store, n)
	for i := range dbs {
		dbs[i] = store.New()
	}
	exp := store.NewExpirer(dbs, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", dbs)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
	w    *resp.Writer
}

func newClient(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn, r: resp.NewReader(conn), w: resp.NewWriter(conn)}
}

// send writes a command without reading its reply (used to check that the
// server suppresses answers).
func (c *client) send(args ...string) {
	c.t.Helper()
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString([]byte(a))
	}
	if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
		c.t.Fatalf("write command: %v", err)
	}
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("flush command: %v", err)
	}
}

// cmd sends a command and returns the decoded response.
func (c *client) cmd(args ...string) resp.Value {
	c.t.Helper()
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString([]byte(a))
	}
	if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
		c.t.Fatalf("write command: %v", err)
	}
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("flush command: %v", err)
	}
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read response to %v: %v", args, err)
	}
	return v
}

func (c *client) ok(args ...string) {
	c.t.Helper()
	v := c.cmd(args...)
	if v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		c.t.Errorf("cmd %v: got %+v, want +OK", args, v)
	}
}

func (c *client) intWant(args ...string) int64 {
	c.t.Helper()
	v := c.cmd(args...)
	if v.Kind != resp.KindInteger {
		c.t.Fatalf("cmd %v: got kind %v, want integer", args, v.Kind)
	}
	return v.Int
}

func (c *client) bulkWant(args ...string) []byte {
	c.t.Helper()
	v := c.cmd(args...)
	if v.Kind != resp.KindBulkString || v.IsNull {
		c.t.Fatalf("cmd %v: got %+v, want non-null bulk string", args, v)
	}
	return v.Str
}

func (c *client) expectError(args ...string) string {
	c.t.Helper()
	v := c.cmd(args...)
	if v.Kind != resp.KindError {
		c.t.Fatalf("cmd %v: got %+v, want error", args, v)
	}
	return string(v.Str)
}

// arrWant asserts the response is an array and returns its elements.
func (c *client) arrWant(args ...string) []resp.Value {
	c.t.Helper()
	v := c.cmd(args...)
	if v.Kind != resp.KindArray || v.IsNull {
		c.t.Fatalf("cmd %v: got %+v, want non-null array", args, v)
	}
	return v.Array
}

func TestPing(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Errorf("PING: got %+v, want PONG", v)
	}
	if got := c.bulkWant("PING", "hello"); string(got) != "hello" {
		t.Errorf("PING hello: got %q", got)
	}
}

func TestEcho(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if got := c.bulkWant("ECHO", "hello world"); string(got) != "hello world" {
		t.Errorf("ECHO: got %q", got)
	}
}

func TestSetGet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "foo", "bar")
	if got := c.bulkWant("GET", "foo"); string(got) != "bar" {
		t.Errorf("GET foo: got %q", got)
	}
	v := c.cmd("GET", "missing")
	if !v.IsNil() {
		t.Errorf("GET missing: got %+v, want null", v)
	}
}

func TestSetOverwriteClearsTTL(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v", "EX", "100")
	if got := c.intWant("TTL", "k"); got <= 0 || got > 100 {
		t.Errorf("TTL after SET EX: got %d", got)
	}
	c.ok("SET", "k", "v2")
	if got := c.intWant("TTL", "k"); got != -1 {
		t.Errorf("TTL after plain SET: got %d, want -1", got)
	}
}

func TestSetNX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v1", "NX")
	if v := c.cmd("SET", "k", "v2", "NX"); !v.IsNil() {
		t.Errorf("SET NX on existing: got %+v, want null", v)
	}
	c.ok("SET", "other", "x", "NX")
}

func TestSetXX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if v := c.cmd("SET", "k", "v", "XX"); !v.IsNil() {
		t.Errorf("SET XX on missing: got %+v, want null", v)
	}
	c.ok("SET", "k", "v1")
	c.ok("SET", "k", "v2", "XX")
	if got := c.bulkWant("GET", "k"); string(got) != "v2" {
		t.Errorf("GET: got %q", got)
	}
}

func TestSetNXWithXXRejected(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	err := c.expectError("SET", "k", "v", "NX", "XX")
	if !strings.Contains(err, "syntax error") {
		t.Errorf("got %q, want syntax error", err)
	}
}

func TestSetBadOption(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	err := c.expectError("SET", "k", "v", "BOGUS")
	if !strings.Contains(err, "syntax error") {
		t.Errorf("got %q, want syntax error", err)
	}
}

func TestExpireFlow(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if got := c.intWant("EXPIRE", "k", "100"); got != 1 {
		t.Errorf("EXPIRE: got %d, want 1", got)
	}
	if got := c.intWant("TTL", "k"); got <= 0 || got > 100 {
		t.Errorf("TTL: got %d", got)
	}
	if got := c.intWant("PERSIST", "k"); got != 1 {
		t.Errorf("PERSIST: got %d, want 1", got)
	}
	if got := c.intWant("TTL", "k"); got != -1 {
		t.Errorf("TTL after PERSIST: got %d, want -1", got)
	}
	if got := c.intWant("TTL", "missing"); got != -2 {
		t.Errorf("TTL missing: got %d, want -2", got)
	}
}

func TestExpireWithZeroDeletes(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if got := c.intWant("EXPIRE", "k", "0"); got != 1 {
		t.Errorf("EXPIRE 0: got %d, want 1", got)
	}
	if got := c.intWant("EXISTS", "k"); got != 0 {
		t.Errorf("key should be deleted")
	}
}

func TestExpireNonPositiveDeletes(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	c.intWant("EXPIRE", "k", "-5")
	if got := c.intWant("EXISTS", "k"); got != 0 {
		t.Errorf("key should be deleted")
	}
}

func TestIncrDecr(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if got := c.intWant("INCR", "n"); got != 1 {
		t.Errorf("INCR: got %d", got)
	}
	if got := c.intWant("INCRBY", "n", "4"); got != 5 {
		t.Errorf("INCRBY: got %d", got)
	}
	if got := c.intWant("DECR", "n"); got != 4 {
		t.Errorf("DECR: got %d", got)
	}
	if got := c.intWant("DECRBY", "n", "4"); got != 0 {
		t.Errorf("DECRBY: got %d", got)
	}
}

func TestIncrOnNonNumeric(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "hello")
	err := c.expectError("INCR", "k")
	if !strings.Contains(err, "not an integer") {
		t.Errorf("got %q", err)
	}
}

func TestIncrKeepsTTL(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "1", "EX", "100")
	c.intWant("INCR", "k")
	if got := c.intWant("TTL", "k"); got <= 0 {
		t.Errorf("INCR should preserve TTL, got %d", got)
	}
}

func TestDelExists(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "a", "1")
	c.ok("SET", "b", "2")
	if got := c.intWant("EXISTS", "a", "b", "c"); got != 2 {
		t.Errorf("EXISTS: got %d", got)
	}
	if got := c.intWant("DEL", "a", "b", "c"); got != 2 {
		t.Errorf("DEL: got %d", got)
	}
	if got := c.intWant("DBSIZE"); got != 0 {
		t.Errorf("DBSIZE: got %d", got)
	}
}

func TestAppendStrlen(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if got := c.intWant("APPEND", "k", "foo"); got != 3 {
		t.Errorf("APPEND: got %d", got)
	}
	if got := c.intWant("APPEND", "k", "bar"); got != 6 {
		t.Errorf("APPEND: got %d", got)
	}
	if got := c.intWant("STRLEN", "k"); got != 6 {
		t.Errorf("STRLEN: got %d", got)
	}
	if got := c.bulkWant("GET", "k"); string(got) != "foobar" {
		t.Errorf("GET: got %q", got)
	}
}

func TestUnknownCommand(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	err := c.expectError("BOGUS", "a", "b")
	if !strings.Contains(err, "unknown command 'BOGUS'") {
		t.Errorf("got %q", err)
	}
}

func TestWrongArgs(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	err := c.expectError("GET")
	if !strings.Contains(err, "wrong number of arguments") {
		t.Errorf("got %q", err)
	}
	err = c.expectError("SET", "k")
	if !strings.Contains(err, "wrong number of arguments") {
		t.Errorf("got %q", err)
	}
}

func TestCaseInsensitiveCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("set", "k", "v")
	if got := c.bulkWant("Get", "k"); string(got) != "v" {
		t.Errorf("GET: got %q", got)
	}
}

func TestFlushDB(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "a", "1")
	c.ok("FLUSHDB")
	if got := c.intWant("DBSIZE"); got != 0 {
		t.Errorf("DBSIZE after FLUSHDB: got %d", got)
	}
}

func TestInfo(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	v := c.cmd("INFO")
	if v.Kind != resp.KindBulkString || !strings.Contains(string(v.Str), "redis_version:1.0.0") {
		t.Errorf("INFO: got %+v", v)
	}
}

func TestPipelinedCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	elems := []resp.Value{
		resp.NewBulkString([]byte("SET")), resp.NewBulkString([]byte("a")), resp.NewBulkString([]byte("1")),
	}
	if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
		t.Fatal(err)
	}
	elems = []resp.Value{
		resp.NewBulkString([]byte("SET")), resp.NewBulkString([]byte("b")), resp.NewBulkString([]byte("2")),
	}
	if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
		t.Fatal(err)
	}
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}
	if v, err := c.r.ReadValue(); err != nil || v.Kind != resp.KindSimpleString {
		t.Errorf("first pipelined response: %+v, %v", v, err)
	}
	if v, err := c.r.ReadValue(); err != nil || v.Kind != resp.KindSimpleString {
		t.Errorf("second pipelined response: %+v, %v", v, err)
	}
	if got := c.intWant("DBSIZE"); got != 2 {
		t.Errorf("DBSIZE: got %d", got)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if v := c.cmd("QUIT"); v.Kind != resp.KindSimpleString {
		t.Errorf("QUIT: got %+v", v)
	}
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadValue(); err == nil {
		t.Errorf("expected connection to close after QUIT")
	}
}

func TestConcurrentClients(t *testing.T) {
	addr := startTestServer(t)
	done := make(chan struct{})
	for w := 0; w < 8; w++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			c := newClient(t, addr)
			for i := 0; i < 100; i++ {
				key := string(rune('a'+id)) + string(rune('0'+i%10))
				c.ok("SET", key, "v")
				if got := c.bulkWant("GET", key); string(got) != "v" {
					t.Errorf("GET %s: got %q", key, got)
					return
				}
			}
		}(w)
	}
	for w := 0; w < 8; w++ {
		<-done
	}
}

func TestExpiredKeyGoneFromGet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "temp", "v", "EX", "1")
	time.Sleep(1100 * time.Millisecond)
	if v := c.cmd("GET", "temp"); !v.IsNil() {
		t.Errorf("expected key to expire, got %+v", v)
	}
}

// TestPipelinedBatch sends many commands in a single write and checks that
// every answer comes back, in order. The server batches the replies of
// pipelined commands into one write, so this also guards the flush logic.
func TestPipelinedBatch(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	const n = 50
	for i := 0; i < n; i++ {
		elems := []resp.Value{
			resp.NewBulkString([]byte("SET")),
			resp.NewBulkString([]byte(fmt.Sprintf("k%d", i))),
			resp.NewBulkString([]byte(fmt.Sprintf("v%d", i))),
		}
		if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
			t.Fatal(err)
		}
		// A GET in between makes the server interleave different reply
		// types in the same batch.
		elems = []resp.Value{
			resp.NewBulkString([]byte("GET")),
			resp.NewBulkString([]byte(fmt.Sprintf("k%d", i))),
		}
		if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		v, err := c.r.ReadValue()
		if err != nil {
			t.Fatalf("read response %d: %v", i, err)
		}
		if v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
			t.Fatalf("response %d: got %+v, want +OK", i, v)
		}
		v, err = c.r.ReadValue()
		if err != nil {
			t.Fatalf("read value %d: %v", i, err)
		}
		if got := string(v.Str); got != fmt.Sprintf("v%d", i) {
			t.Errorf("value %d: got %q", i, got)
		}
	}
}

// TestGetLargeBetweenSmallReplies pipelines a GET of a value bigger than the
// write buffer between two small commands and checks that the replies come
// back complete and in order: the direct writev path of the large bulk must
// not reorder or split the stream.
func TestGetLargeBetweenSmallReplies(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	large := bytes.Repeat([]byte("z"), 128*1024)
	if v := c.cmd("SET", "big", string(large)); string(v.Str) != "OK" {
		t.Fatalf("SET big: got %+v", v)
	}

	// One batch: small, large, small.
	write := func(args ...string) {
		t.Helper()
		elems := make([]resp.Value, len(args))
		for i, a := range args {
			elems[i] = resp.NewBulkString([]byte(a))
		}
		if err := c.w.WriteValue(resp.NewArray(elems)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	write("SET", "k1", "v1")
	write("GET", "big")
	write("SET", "k2", "v2")
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}

	read := func() resp.Value {
		t.Helper()
		v, err := c.r.ReadValue()
		if err != nil {
			t.Fatalf("read reply: %v", err)
		}
		return v
	}
	if v := read(); string(v.Str) != "OK" {
		t.Fatalf("first reply %+v, want OK", v)
	}
	v := read()
	if v.Kind != resp.KindBulkString || !bytes.Equal(v.Str, large) {
		t.Fatalf("large reply corrupted: got %d bytes, want %d", len(v.Str), len(large))
	}
	if v := read(); string(v.Str) != "OK" {
		t.Fatalf("last reply %+v, want OK", v)
	}
	if v := c.cmd("GET", "k1"); string(v.Str) != "v1" {
		t.Fatalf("GET k1 after the batch: got %q", v.Str)
	}
	if v := c.cmd("GET", "k2"); string(v.Str) != "v2" {
		t.Fatalf("GET k2 after the batch: got %q", v.Str)
	}
}
