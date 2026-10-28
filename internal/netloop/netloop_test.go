package netloop_test

import (
	"net"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/netloop"
)

// TestPollerRoundTrip checks the poller: a socket that becomes readable is
// reported, and the data is read from it.
func TestPollerRoundTrip(t *testing.T) {
	p, err := netloop.NewPoller()
	if err != nil {
		t.Skipf("the poller is not supported here: %v", err)
	}
	ln, err := netloop.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer netloop.Close(ln)
	addr, err := netloop.Addr(ln)
	if err != nil {
		t.Fatalf("addr: %v", err)
	}
	if err := p.AddRead(ln); err != nil {
		t.Fatalf("watch the listener: %v", err)
	}

	// The client connects and writes.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The loop reports the listener and then the connection.
	deadline := time.Now().Add(5 * time.Second)
	var cfd int
	for cfd == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the listener was not reported")
		}
		events, err := p.Wait(5 * time.Second)
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
		for _, ev := range events {
			if ev.FD == ln {
				fd, err := netloop.Accept(ln)
				if err != nil {
					continue
				}
				cfd = fd
				if err := p.AddRead(cfd); err != nil {
					t.Fatalf("watch the connection: %v", err)
				}
			}
		}
	}
	defer netloop.Close(cfd)

	buf := make([]byte, 16)
	got := 0
	for got < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("the data was not reported")
		}
		events, err := p.Wait(5 * time.Second)
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
		for _, ev := range events {
			if ev.FD != cfd || !ev.Readable {
				continue
			}
			n, err := netloop.Read(cfd, buf[got:])
			if n > 0 {
				got += n
			}
			if err != nil && !netloop.WouldBlock(err) {
				t.Fatalf("read: %v", err)
			}
		}
	}
	if string(buf[:got]) != "hello" {
		t.Fatalf("got %q, want hello", buf[:got])
	}
}

// TestWouldBlock checks that the "would block" error is recognized.
func TestWouldBlock(t *testing.T) {
	p, err := netloop.NewPoller()
	if err != nil {
		t.Skipf("the poller is not supported here: %v", err)
	}
	ln, err := netloop.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer netloop.Close(ln)
	addr, err := netloop.Addr(ln)
	if err != nil {
		t.Fatalf("addr: %v", err)
	}
	if err := p.AddRead(ln); err != nil {
		t.Fatalf("watch the listener: %v", err)
	}
	// A connected socket without data: the read must report that it would
	// block.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cfd, err := netloop.Accept(ln)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer netloop.Close(cfd)
	buf := make([]byte, 16)
	_, err = netloop.Read(cfd, buf)
	if !netloop.WouldBlock(err) {
		t.Fatalf("read: got %v, want the would block error", err)
	}
}

// TestPollerTimeout checks that the wait returns when the timeout expires,
// even when nothing happens.
func TestPollerTimeout(t *testing.T) {
	p, err := netloop.NewPoller()
	if err != nil {
		t.Skipf("the poller is not supported here: %v", err)
	}
	defer p.Close()

	start := time.Now()
	events, err := p.Wait(50 * time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("got %d events, want none", len(events))
	}
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("the wait returned too early: %v", elapsed)
	}
}

// TestPollerNotify checks that a notification wakes a poller that waits: the
// event may come from another goroutine, not from a socket.
func TestPollerNotify(t *testing.T) {
	p, err := netloop.NewPoller()
	if err != nil {
		t.Skipf("the poller is not supported here: %v", err)
	}
	defer p.Close()

	done := make(chan []netloop.Event, 1)
	go func() {
		events, err := p.Wait(5 * time.Second)
		if err != nil {
			done <- nil
			return
		}
		done <- events
	}()

	// The notification goes out while the poller waits.
	time.Sleep(20 * time.Millisecond)
	if err := p.Notify(); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case events := <-done:
		if len(events) != 1 || !events[0].Wakeup {
			t.Fatalf("got %+v, want a wakeup event", events)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the notification did not wake the poller")
	}
}
