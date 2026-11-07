package server_test

import (
	"io"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// startTestServerLoops runs a server that serves the connections with n
// event loops: every loop binds the port with SO_REUSEPORT and the kernel
// spreads the connections between the loops.
func startTestServerLoops(t *testing.T, n int) string {
	t.Helper()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()
	srv, err := server.NewLoopN("127.0.0.1:0", []*store.Store{st}, n)
	if err != nil {
		t.Fatalf("server.NewLoopN: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

// TestLoopMultiSetGet checks the basic commands on a server with several
// loops: every client talks to the loop the kernel gave it to, and the keys
// are visible to all of them.
func TestLoopMultiSetGet(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	for i := 0; i < 8; i++ {
		c := newClient(t, addr)
		c.ok("SET", "key", "value")
		if got := c.bulkWant("GET", "key"); string(got) != "value" {
			t.Errorf("GET: got %q", got)
		}
		if got := c.intWant("DBSIZE"); got != 1 {
			t.Errorf("DBSIZE: got %d, want 1", got)
		}
	}
}

// TestLoopMultiConcurrent checks that the loops run at the same time without
// losing a write: several clients increment one shared counter from the
// loops the kernel spread them over.
func TestLoopMultiConcurrent(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	const (
		clients   = 8
		perClient = 50
	)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := newClient(t, addr)
			for j := 0; j < perClient; j++ {
				c.intWant("INCR", "shared")
				// A key of the client itself: the loops must not mix
				// the data of the connections.
				c.ok("SET", "own", "own")
			}
		}(i)
	}
	wg.Wait()
	c := newClient(t, addr)
	got := c.bulkWant("GET", "shared")
	if string(got) != strconv.Itoa(clients*perClient) {
		t.Errorf("shared counter: got %q, want %d", got, clients*perClient)
	}
}

// TestLoopMultiBlockingWokenUp checks that a client blocked on a key is woken
// up by a push from another connection. The connections may belong to
// different loops: the wake-up goes through the mutex of the loop that
// serves the blocked client.
func TestLoopMultiBlockingWokenUp(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	blocked := newClient(t, addr)
	pusher := newClient(t, addr)

	go func() {
		time.Sleep(100 * time.Millisecond)
		pusher.intWant("RPUSH", "l", "value")
	}()
	arr := blocked.arrWant("BLPOP", "l", "5")
	if len(arr) != 2 || string(arr[0].Str) != "l" || string(arr[1].Str) != "value" {
		t.Errorf("BLPOP after a push: got %+v, want [l value]", arr)
	}
}

// TestLoopMultiBlockingTimeout checks that the timeout of a blocked client
// still works when the loops are several: the loop of the client expires
// the wait by itself.
func TestLoopMultiBlockingTimeout(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	c := newClient(t, addr)
	start := time.Now()
	v := c.cmd("BLPOP", "nolist", "0.2")
	if v.Kind != resp.KindArray || !v.IsNull {
		t.Errorf("BLPOP timeout: got %+v, want a null array", v)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Errorf("BLPOP returned after %v, want at least the timeout", d)
	}
}

// TestLoopMultiClientUnblock checks that a blocked client of one loop is
// unblocked with CLIENT UNBLOCK from another connection.
func TestLoopMultiClientUnblock(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	blocked := newClient(t, addr)
	unblocker := newClient(t, addr)

	id := blocked.intWant("CLIENT", "ID")
	go func() {
		time.Sleep(100 * time.Millisecond)
		if n := unblocker.intWant("CLIENT", "UNBLOCK", "ID", strconv.FormatInt(id, 10)); n != 1 {
			unblocker.t.Errorf("CLIENT UNBLOCK: got %d, want 1", n)
		}
	}()
	v := blocked.cmd("BLPOP", "nolist", "5")
	if v.Kind != resp.KindError {
		blocked.t.Errorf("BLPOP unblocked: got %+v, want an error", v)
	}
}

// TestLoopMultiPubSub checks the Pub/Sub on a server with several loops: the
// subscriber and the publisher may live on the different loops, and the
// message goes out through the queue of the loop that serves the subscriber.
func TestLoopMultiPubSub(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 1 {
		t.Errorf("PUBLISH: got %d, want 1", n)
	}
	expectPubMessage(t, sub.push(), "ch", []byte("hello"))
}

// TestLoopMultiClientKill checks that a connection can be killed by its
// address even when the two clients are served by the different loops.
func TestLoopMultiClientKill(t *testing.T) {
	addr := startTestServerLoops(t, 4)
	killer := newClient(t, addr)
	victim := newClient(t, addr)

	// The address of the victim as the server sees it is the local
	// address of its connection.
	victimAddr := victim.conn.LocalAddr().String()
	if n := killer.intWant("CLIENT", "KILL", "ADDR", victimAddr); n != 1 {
		t.Fatalf("CLIENT KILL: got %d, want 1", n)
	}
	// The connection of the victim is closed.
	if err := victim.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	if _, err := victim.r.ReadValue(); err == nil {
		t.Errorf("the killed connection still reads replies")
	}
}

// TestLoopMultiShutdownWithBlocked checks that the server with several loops
// stops promptly even when a client is blocked: the loops close their
// connections and the serving returns.
func TestLoopMultiShutdownWithBlocked(t *testing.T) {
	st := store.New()
	srv, err := server.NewLoopN("127.0.0.1:0", []*store.Store{st}, 4)
	if err != nil {
		t.Fatalf("server.NewLoopN: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	addr := srv.Addr().String()
	blocked := newClient(t, addr)
	fine := newClient(t, addr)
	_ = fine

	// The client waits forever: only the stop of the server can end it.
	blocked.send("BLPOP", "nolist", "0")

	if err := srv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the server did not stop with a blocked client")
	}
}
