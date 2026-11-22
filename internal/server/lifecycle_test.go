package server_test

import (
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// TestCloseBeforeServeMultiLoop checks that a server can be closed before its
// event loops ever started: Close releases the listeners and the persistence
// resources, a second Close is harmless, and a later Serve returns instead of
// waiting for loops that do not exist.
func TestCloseBeforeServeMultiLoop(t *testing.T) {
	dir := t.TempDir()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()

	srv, err := server.NewLoopN("127.0.0.1:0", []*store.Store{st}, 4)
	if err != nil {
		t.Fatalf("server.NewLoopN: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp

	aofPath := filepath.Join(dir, "appendonly.aof")
	a, err := persist.OpenAppender(aofPath, persist.FsyncNo)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	srv.AOF = a
	srv.AOFPath = aofPath

	// The loops were never started: Close owns the four listener
	// descriptors. A second Close and a Shutdown must not stop the same
	// Expirer or close the same AOF again.
	if err := srv.Close(); err != nil {
		t.Fatalf("close before Serve: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := srv.Shutdown(); err != nil {
		t.Fatalf("Shutdown after Close: %v", err)
	}

	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve after Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve after Close did not return")
	}
}

// TestServeCloseConcurrentMultiLoop starts and stops the loops concurrently.
// Close may win before Serve takes the listeners, or it may arrive while the
// loop states are being constructed: either way Serve must return and the
// server must not wait for a loop or an error that will never come.
func TestServeCloseConcurrentMultiLoop(t *testing.T) {
	for i := 0; i < 25; i++ {
		st := store.New()
		srv, err := server.NewLoopN("127.0.0.1:0", []*store.Store{st}, 4)
		if err != nil {
			t.Fatalf("iteration %d: server.NewLoopN: %v", i, err)
		}
		srv.SetLogger(log.New(io.Discard, "", 0))

		serveErr := make(chan error, 1)
		closeErr := make(chan error, 1)
		start := make(chan struct{})
		go func() {
			<-start
			serveErr <- srv.Serve()
		}()
		go func() {
			<-start
			closeErr <- srv.Close()
		}()
		close(start)

		for name, ch := range map[string]<-chan error{
			"Serve": serveErr,
			"Close": closeErr,
		} {
			select {
			case err := <-ch:
				if err != nil {
					t.Fatalf("iteration %d: %s: %v", i, name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("iteration %d: %s did not return", i, name)
			}
		}
	}
}

// TestShutdownWithActiveAndBlockedClients checks the graceful path while a
// client is still sending commands and another one waits in a blocking
// command: Shutdown stops both, saves the RDB and closes the AOF, and the
// files can be loaded again.
func TestShutdownWithActiveAndBlockedClients(t *testing.T) {
	dir := t.TempDir()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()

	srv, err := server.NewLoopN("127.0.0.1:0", []*store.Store{st}, 2)
	if err != nil {
		t.Fatalf("server.NewLoopN: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	dbPath := filepath.Join(dir, "dump.rdb")
	aofPath := filepath.Join(dir, "appendonly.aof")
	srv.DBPath = dbPath
	srv.AOFPath = aofPath
	a, err := persist.OpenAppender(aofPath, persist.FsyncNo)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	srv.AOF = a
	srv.StartSavePolicy(1, 1)

	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	addr := srv.Addr().String()

	stable := newClient(t, addr)
	stable.ok("SET", "stable", "before")

	blocked := newClient(t, addr)
	blocked.send("BLPOP", "never", "0")
	waitBlocked(t, stable, 1)

	// A second connection keeps executing commands while Shutdown begins:
	// its handler must be drained before the AOF is closed.
	activeErr := make(chan error, 1)
	activeStarted := make(chan struct{})
	go runActiveWriter(addr, activeStarted, activeErr)
	select {
	case <-activeStarted:
	case err := <-activeErr:
		t.Fatalf("active client: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the active client did not start")
	}

	if err := srv.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-activeErr:
	case <-time.After(5 * time.Second):
		t.Fatal("the active client was not disconnected")
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}

	// The blocked client is disconnected instead of waiting forever.
	blocked.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, readErr := blocked.r.ReadValue()
	if ne, ok := readErr.(net.Error); ok && ne.Timeout() {
		t.Fatal("the blocked client is still waiting after Shutdown")
	}

	// A later Close must not close the Expirer or the AOF a second time.
	if err := srv.Close(); err != nil {
		t.Fatalf("Close after Shutdown: %v", err)
	}

	// The final RDB holds the committed keys.
	f, err := os.Open(dbPath)
	if err != nil {
		t.Fatalf("open rdb: %v", err)
	}
	rdb := []*store.Store{store.New()}
	err = persist.Load(f, rdb)
	f.Close()
	if err != nil {
		t.Fatalf("load rdb: %v", err)
	}
	if got, _, _ := rdb[0].GetString([]byte("stable")); string(got) != "before" {
		t.Errorf("the rdb holds stable=%q, want before", got)
	}

	// The AOF was flushed and closed by Shutdown.
	f, err = os.Open(aofPath)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	aofDBs := []*store.Store{store.New()}
	err = persist.LoadAOF(f, aofDBs)
	f.Close()
	if err != nil {
		t.Fatalf("load aof: %v", err)
	}
	if got, _, _ := aofDBs[0].GetString([]byte("stable")); string(got) != "before" {
		t.Errorf("the aof holds stable=%q, want before", got)
	}
}

// runActiveWriter executes INCR commands on its own connection until the
// server goes away. It reports the first error (the expected one when the
// shutdown closes the socket) and closes started after the first reply.
func runActiveWriter(addr string, started chan<- struct{}, errc chan<- error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		errc <- err
		return
	}
	defer conn.Close()
	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	first := true
	for {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("INCR")),
			resp.NewBulkString([]byte("active")),
		})
		if err := w.WriteValue(cmd); err != nil {
			errc <- err
			return
		}
		if err := w.Flush(); err != nil {
			errc <- err
			return
		}
		if _, err := r.ReadValue(); err != nil {
			errc <- err
			return
		}
		if first {
			first = false
			close(started)
		}
	}
}
