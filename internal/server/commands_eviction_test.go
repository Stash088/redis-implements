package server_test

import (
	"bytes"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// startServerWithLimits builds a server (no expirer) and applies fn to it
// before serving, for maxmemory/slowlog configuration.
func startServerWithLimits(t *testing.T, st *store.Store, fn func(s *server.Server)) string {
	t.Helper()
	srv, err := server.New("127.0.0.1:0", st)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	if fn != nil {
		fn(srv)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

func fakeClockStore() (*store.Store, func(d time.Duration)) {
	var nanos atomic.Int64
	nanos.Store(time.Now().UnixNano())
	st := store.New()
	st.SetClock(func() time.Time { return time.Unix(0, nanos.Load()) })
	return st, func(d time.Duration) { nanos.Add(int64(d)) }
}

func TestNoevictionOOM(t *testing.T) {
	st, _ := fakeClockStore()
	addr := startServerWithLimits(t, st, func(s *server.Server) {
		s.SetMaxMemory(200)
	})
	c := newClient(t, addr)

	c.ok("SET", "fill", "0123456789")

	// This write pushes the store over the limit and is allowed (the limit is
	// checked before execution); the NEXT memory-adding command gets OOM.
	c.ok("SET", "big", string(make([]byte, 300)))
	v := c.cmd("SET", "another", "x")
	if v.Kind != resp.KindError || !bytes.HasPrefix(v.Str, []byte("OOM")) {
		t.Fatalf("SET over limit: got %+v, want OOM error", v)
	}
	if got := c.bulkWant("GET", "fill"); string(got) != "0123456789" {
		t.Errorf("GET after OOM: got %q, want fill value", got)
	}
	if n := c.intWant("DEL", "big"); n != 1 {
		t.Errorf("DEL over limit: got %d, want 1 (deletes are allowed)", n)
	}
	// After deletion the store is under the limit again.
	c.ok("SET", "another", "x")
}

func TestAllkeysLRUEviction(t *testing.T) {
	st, advance := fakeClockStore()
	addr := startServerWithLimits(t, st, nil)
	c := newClient(t, addr)

	value := string(make([]byte, 100))
	c.ok("SET", "a", value)
	advance(time.Millisecond)
	c.ok("SET", "b", value)
	advance(time.Millisecond)
	c.ok("SET", "c", value)

	c.cmd("CONFIG", "SET", "maxmemory", "250")
	c.cmd("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
	advance(time.Millisecond)
	_ = c.bulkWant("GET", "a")
	advance(time.Millisecond)

	// SET d pushes over the limit: b (oldest) and c get evicted.
	c.ok("SET", "d", value)
	if v := c.cmd("GET", "b"); !v.IsNull {
		t.Errorf("GET b: got %+v, want null (evicted)", v)
	}
	if v := c.cmd("GET", "c"); !v.IsNull {
		t.Errorf("GET c: got %+v, want null (evicted)", v)
	}
	if v := c.cmd("GET", "a"); v.IsNull {
		t.Error("GET a: null, want value (recently used)")
	}
	if v := c.cmd("GET", "d"); v.IsNull {
		t.Error("GET d: null, want value (just written)")
	}
}

func TestVolatileTTLEviction(t *testing.T) {
	st, advance := fakeClockStore()
	addr := startServerWithLimits(t, st, func(s *server.Server) {
		s.SetMaxMemoryPolicy("volatile-ttl")
	})
	c := newClient(t, addr)

	value := string(make([]byte, 100))
	c.ok("SET", "perm", value)
	advance(time.Millisecond)
	c.ok("SET", "b", value, "EX", "1000")
	advance(time.Millisecond)
	c.ok("SET", "c", value, "EX", "500")

	// Apply the limit, then trigger eviction with a small write: the volatile
	// policy evicts TTL keys soonest first; the key without TTL survives.
	c.cmd("CONFIG", "SET", "maxmemory", "150")
	c.ok("SET", "trigger", "x")
	if v := c.cmd("GET", "c"); !v.IsNull {
		t.Errorf("GET c: got %+v, want null (soonest TTL evicted)", v)
	}
	if v := c.cmd("GET", "b"); !v.IsNull {
		t.Errorf("GET b: got %+v, want null (TTL evicted)", v)
	}
	if v := c.cmd("GET", "perm"); v.IsNull {
		t.Error("GET perm: null, want value (no TTL, survives volatile policy)")
	}
}

func TestEvictionInsideMulti(t *testing.T) {
	st, advance := fakeClockStore()
	addr := startServerWithLimits(t, st, func(s *server.Server) {
		s.SetMaxMemoryPolicy("allkeys-lru")
	})
	c := newClient(t, addr)

	value := string(make([]byte, 100))
	c.ok("SET", "a", value)
	c.cmd("CONFIG", "SET", "maxmemory", "250")
	advance(time.Millisecond)

	// Transaction whose queued command grows the store: executed and then
	// evicted down to the limit.
	c.ok("MULTI")
	c.cmd("SET", "big", string(make([]byte, 200)))
	v := c.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 1 || string(v.Array[0].Str) != "OK" {
		t.Fatalf("EXEC: got %+v, want [OK]", v)
	}
	// "a" was evicted (older than "big"); "big" remains.
	if v := c.cmd("GET", "a"); !v.IsNull {
		t.Errorf("GET a: got %+v, want null (evicted)", v)
	}
	if v := c.cmd("GET", "big"); v.IsNull {
		t.Error("GET big: null, want value")
	}
}
