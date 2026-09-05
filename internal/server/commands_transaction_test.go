package server_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

func TestMultiExec(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if v := c.cmd("MULTI"); v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		t.Fatalf("MULTI: got %+v, want OK", v)
	}
	for _, q := range [][]string{
		{"SET", "foo", "bar"},
		{"GET", "foo"},
		{"INCR", "n"},
	} {
		if v := c.cmd(q...); v.Kind != resp.KindSimpleString || string(v.Str) != "QUEUED" {
			t.Fatalf("queue %v: got %+v, want QUEUED", q, v)
		}
	}
	v := c.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 3 {
		t.Fatalf("EXEC: got %+v, want 3-element array", v)
	}
	if string(v.Array[0].Str) != "OK" {
		t.Errorf("EXEC[0]: got %+v, want OK", v.Array[0])
	}
	if string(v.Array[1].Str) != "bar" {
		t.Errorf("EXEC[1]: got %+v, want bar", v.Array[1])
	}
	if v.Array[2].Kind != resp.KindInteger || v.Array[2].Int != 1 {
		t.Errorf("EXEC[2]: got %+v, want :1", v.Array[2])
	}

	// Back to normal mode.
	if v := c.cmd("GET", "foo"); string(v.Str) != "bar" {
		t.Errorf("GET after EXEC: got %+v", v)
	}
}

func TestMultiEmptyExec(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")
	v := c.cmd("EXEC")
	if v.Kind != resp.KindArray || v.IsNull || len(v.Array) != 0 {
		t.Errorf("EXEC empty: got %+v, want empty array", v)
	}
}

func TestExecWithoutMulti(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if v := c.cmd("EXEC"); v.Kind != resp.KindError || !strings.Contains(string(v.Str), "EXEC without MULTI") {
		t.Errorf("EXEC: got %+v", v)
	}
	if v := c.cmd("DISCARD"); v.Kind != resp.KindError || !strings.Contains(string(v.Str), "DISCARD without MULTI") {
		t.Errorf("DISCARD: got %+v", v)
	}
}

func TestNestedMulti(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")
	if v := c.cmd("MULTI"); v.Kind != resp.KindError || !strings.Contains(string(v.Str), "nested") {
		t.Errorf("nested MULTI: got %+v", v)
	}
	// Still in MULTI mode: SET is queued afterwards.
	if v := c.cmd("SET", "a", "1"); string(v.Str) != "QUEUED" {
		t.Errorf("SET after nested MULTI error: got %+v", v)
	}
}

func TestDiscard(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")
	if v := c.cmd("SET", "foo", "bar"); string(v.Str) != "QUEUED" {
		t.Fatalf("queue: got %+v", v)
	}
	c.ok("DISCARD")
	// Nothing was executed and normal mode resumed.
	if v := c.cmd("GET", "foo"); !v.IsNull {
		t.Errorf("GET after DISCARD: got %+v, want null", v)
	}
	if v := c.cmd("EXEC"); v.Kind != resp.KindError {
		t.Errorf("EXEC after DISCARD: got %+v, want error", v)
	}
}

func TestQueueErrorAbortsExec(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")

	if v := c.cmd("NOSUCHCMD", "x"); v.Kind != resp.KindError {
		t.Fatalf("unknown cmd: got %+v, want error", v)
	}
	if v := c.cmd("SET", "foo"); !strings.Contains(string(v.Str), "wrong number of arguments") {
		t.Fatalf("wrong arity: got %+v", v)
	}
	if v := c.cmd("GET", "foo"); string(v.Str) != "QUEUED" {
		t.Fatalf("queue after error: got %+v", v)
	}

	v := c.cmd("EXEC")
	if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "EXECABORT") {
		t.Fatalf("EXEC: got %+v, want EXECABORT", v)
	}
	// Transaction was discarded and the state reset.
	if v := c.cmd("GET", "foo"); !v.IsNull {
		t.Errorf("GET after EXECABORT: got %+v, want null", v)
	}
}

func TestSubscribeNotAllowedInMulti(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")
	if v := c.cmd("SUBSCRIBE", "ch"); v.Kind != resp.KindError || !strings.Contains(string(v.Str), "not allowed in transactions") {
		t.Fatalf("SUBSCRIBE in MULTI: got %+v", v)
	}
	if v := c.cmd("EXEC"); v.Kind != resp.KindError || !strings.Contains(string(v.Str), "EXECABORT") {
		t.Errorf("EXEC: got %+v, want EXECABORT", v)
	}
}

func TestRuntimeErrorInsideExecContinues(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "foo", "bar")
	if n := c.intWant("RPUSH", "mylist", "x"); n != 1 {
		t.Fatalf("RPUSH: got %d, want 1", n)
	}
	c.ok("MULTI")
	c.cmd("INCR", "mylist") // WRONGTYPE at exec time
	c.cmd("GET", "foo")
	v := c.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 2 {
		t.Fatalf("EXEC: got %+v", v)
	}
	if v.Array[0].Kind != resp.KindError || !strings.HasPrefix(string(v.Array[0].Str), "WRONGTYPE") {
		t.Errorf("EXEC[0]: got %+v, want WRONGTYPE", v.Array[0])
	}
	if string(v.Array[1].Str) != "bar" {
		t.Errorf("EXEC[1]: got %+v, want bar", v.Array[1])
	}
}

func TestWatchDirtyAbort(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)

	c1.ok("SET", "k", "v1")
	c1.ok("WATCH", "k")
	c1.ok("MULTI")
	c1.cmd("SET", "k", "from-txn")
	c2.ok("SET", "k", "changed")
	v := c1.cmd("EXEC")
	if !v.IsNull {
		t.Errorf("EXEC after dirty WATCH: got %+v, want null array", v)
	}
	// The transaction was not applied.
	if got := c1.bulkWant("GET", "k"); string(got) != "changed" {
		t.Errorf("GET: got %q, want changed", got)
	}
}

func TestWatchCleanExec(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)

	c1.ok("WATCH", "k")
	c1.ok("MULTI")
	c1.cmd("SET", "k", "from-txn")
	c2.ok("SET", "other", "x") // different key: no abort
	v := c1.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 1 || string(v.Array[0].Str) != "OK" {
		t.Errorf("EXEC: got %+v, want [OK]", v)
	}
}

func TestWatchDeleteAborts(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)

	c1.ok("SET", "k", "v")
	c1.ok("WATCH", "k")
	c1.ok("MULTI")
	if n := c2.intWant("DEL", "k"); n != 1 {
		t.Fatalf("DEL: got %d, want 1", n)
	}
	if v := c1.cmd("EXEC"); !v.IsNull {
		t.Errorf("EXEC after watched key deleted: got %+v, want null", v)
	}
}

func TestUnwatchAllowsExec(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)

	c1.ok("WATCH", "k")
	c2.ok("SET", "k", "x")
	c1.ok("UNWATCH")
	c1.ok("MULTI")
	c1.cmd("SET", "k", "txn")
	v := c1.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 1 || string(v.Array[0].Str) != "OK" {
		t.Errorf("EXEC after UNWATCH: got %+v, want [OK]", v)
	}
}

func TestWatchRetryPattern(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)

	c1.ok("SET", "counter", "0")
	// First attempt: another client changes the key -> abort.
	c1.ok("WATCH", "counter")
	c2.ok("SET", "counter", "1")
	c1.ok("MULTI")
	c1.cmd("INCR", "counter")
	if v := c1.cmd("EXEC"); !v.IsNull {
		t.Fatalf("first attempt: got %+v, want null", v)
	}
	// Retry with a fresh WATCH must succeed.
	c1.ok("WATCH", "counter")
	c1.ok("MULTI")
	c1.cmd("INCR", "counter")
	v := c1.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 1 || v.Array[0].Int != 2 {
		t.Errorf("retry EXEC: got %+v, want [:2]", v)
	}
}

func TestWatchExpiredKeyAborts(t *testing.T) {
	// Fake clock without a background expirer: the key stays in the store
	// but logically expired when WATCH sees it (like Redis watchKey).
	var nanos atomic.Int64
	nanos.Store(time.Now().UnixNano())
	st := store.New()
	st.SetClock(func() time.Time { return time.Unix(0, nanos.Load()) })
	addr := startTestServerStore(t, st, false)

	c := newClient(t, addr)
	c.ok("SET", "k", "v", "PX", "10")
	nanos.Add(100 * time.Millisecond.Nanoseconds())
	c.ok("WATCH", "k")
	c.ok("MULTI")
	c.cmd("SET", "k", "txn")
	if v := c.cmd("EXEC"); !v.IsNull {
		t.Errorf("EXEC after watching expired key: got %+v, want null", v)
	}
}

func TestQueuedSubscribeConfirm(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("MULTI")
	if v := c.cmd("SET", "a", "b"); string(v.Str) != "QUEUED" {
		t.Fatalf("SET: got %+v", v)
	}
	v := c.cmd("EXEC")
	if v.Kind != resp.KindArray || len(v.Array) != 1 || string(v.Array[0].Str) != "OK" {
		t.Errorf("EXEC: got %+v", v)
	}
}

func TestConcurrentWatchExecRace(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		m := newClient(t, addr)
		for i := 0; i < 200; i++ {
			m.cmd("SET", "race", "x")
			m.cmd("DEL", "race")
		}
	}()
	c.cmd("SET", "race", "init")
	for i := 0; i < 100; i++ {
		c.cmd("WATCH", "race")
		c.cmd("MULTI")
		c.cmd("INCR", "other")
		c.cmd("EXEC")
	}
	<-stop
}
