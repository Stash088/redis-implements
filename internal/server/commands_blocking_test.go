package server_test

import (
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

// TestBLPopReady checks BLPOP with a list that is not empty.
func TestBLPopReady(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a", "b")
	arr := c.arrWant("BLPOP", "l", "0")
	if len(arr) != 2 {
		t.Fatalf("BLPOP: got %d elements, want 2", len(arr))
	}
	if string(arr[0].Str) != "l" || string(arr[1].Str) != "a" {
		t.Errorf("BLPOP: got %+v, want [l a]", arr)
	}
	// BRPOP takes the last element.
	c.intWant("RPUSH", "l", "x", "y")
	arr = c.arrWant("BRPOP", "l", "0")
	if len(arr) != 2 || string(arr[1].Str) != "y" {
		t.Errorf("BRPOP: got %+v, want [l y]", arr)
	}
}

// TestBLPopTimeout checks that BLPOP gives up after the timeout.
func TestBLPopTimeout(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	start := time.Now()
	// A timeout answers with a null array, like in Redis.
	v := c.cmd("BLPOP", "nolist", "0.2")
	if v.Kind != resp.KindArray || !v.IsNull {
		t.Errorf("BLPOP timeout: got %+v, want a null array", v)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Errorf("BLPOP returned after %v, want at least the timeout", d)
	}
}

// TestBLPopWokenUp checks that a blocked client is woken up by a push from
// another connection.
func TestBLPopWokenUp(t *testing.T) {
	addr := startTestServer(t)
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

// TestBLPopMultipleWaiters checks that several blocked clients are served.
func TestBLPopMultipleWaiters(t *testing.T) {
	addr := startTestServer(t)
	pusher := newClient(t, addr)

	results := make(chan []byte, 2)
	for i := 0; i < 2; i++ {
		go func() {
			c := newClient(t, addr)
			arr := c.arrWant("BLPOP", "l", "5")
			if len(arr) == 2 {
				results <- arr[1].Str
			} else {
				results <- nil
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	pusher.intWant("RPUSH", "l", "v1")
	pusher.intWant("RPUSH", "l", "v2")

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case v := <-results:
			seen[string(v)] = true
		case <-time.After(5 * time.Second):
			t.Fatal("a blocked client was not woken up")
		}
	}
	if !seen["v1"] || !seen["v2"] {
		t.Errorf("the blocked clients got %v, want v1 and v2", seen)
	}
}

// TestBZPopMin checks the blocking pop of a sorted set.
func TestBZPopMin(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "m1", "2", "m2")
	arr := c.arrWant("BZPOPMIN", "z", "0")
	if len(arr) != 3 {
		t.Fatalf("BZPOPMIN: got %d elements, want 3", len(arr))
	}
	if string(arr[0].Str) != "z" || string(arr[1].Str) != "m1" || string(arr[2].Str) != "1" {
		t.Errorf("BZPOPMIN: got %+v, want [z m1 1]", arr)
	}
	// BZPOPMAX takes the highest score.
	arr = c.arrWant("BZPOPMAX", "z", "0")
	if len(arr) != 3 || string(arr[1].Str) != "m2" {
		t.Errorf("BZPOPMAX: got %+v, want [z m2 2]", arr)
	}
}

// TestBLMove checks the blocking move between two lists.
func TestBLMove(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "src", "m")
	if got := string(c.bulkWant("BLMOVE", "src", "dst", "LEFT", "RIGHT", "0")); got != "m" {
		t.Errorf("BLMOVE: got %q, want m", got)
	}
	arr := c.arrWant("LRANGE", "dst", "0", "-1")
	if len(arr) != 1 || string(arr[0].Str) != "m" {
		t.Errorf("the destination list: got %+v", arr)
	}
}

// TestBlockingWrongType checks that the blocking commands report a wrong type
// for keys of another type.
func TestBlockingWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"BLPOP", "str", "0"},
		{"BRPOP", "str", "0"},
		{"BZPOPMIN", "str", "0"},
		{"BZPOPMAX", "str", "0"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}
