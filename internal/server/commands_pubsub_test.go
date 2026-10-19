package server_test

import (
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

// push reads a server-pushed message (Pub/Sub frame) from the client.
func (c *client) push() resp.Value {
	c.t.Helper()
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read push message: %v", err)
	}
	return v
}

func expectConfirm(t *testing.T, v resp.Value, kind, channel string, count int64) {
	t.Helper()
	// In RESP3 the confirmation is a push frame, in RESP2 a plain array.
	if (v.Kind != resp.KindArray && v.Kind != resp.KindPush) || len(v.Array) != 3 {
		t.Fatalf("confirm: got %+v, want a 3-element array or push", v)
	}
	if string(v.Array[0].Str) != kind {
		t.Errorf("confirm kind: got %q, want %q", v.Array[0].Str, kind)
	}
	if string(v.Array[1].Str) != channel {
		t.Errorf("confirm channel: got %q, want %q", v.Array[1].Str, channel)
	}
	if v.Array[2].Kind != resp.KindInteger || v.Array[2].Int != count {
		t.Errorf("confirm count: got %+v, want %d", v.Array[2], count)
	}
}

func expectPubMessage(t *testing.T, v resp.Value, channel string, payload []byte) {
	t.Helper()
	if v.Kind != resp.KindArray || len(v.Array) != 3 {
		t.Fatalf("message: got %+v, want 3-element array", v)
	}
	if string(v.Array[0].Str) != "message" {
		t.Errorf("message kind: got %q, want %q", v.Array[0].Str, "message")
	}
	if string(v.Array[1].Str) != channel {
		t.Errorf("message channel: got %q, want %q", v.Array[1].Str, channel)
	}
	if string(v.Array[2].Str) != string(payload) {
		t.Errorf("message payload: got %q, want %q", v.Array[2].Str, payload)
	}
}

func TestSubscribePublish(t *testing.T) {
	addr := startTestServer(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)

	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 1 {
		t.Errorf("PUBLISH: got %d, want 1", n)
	}
	expectPubMessage(t, sub.push(), "ch", []byte("hello"))

	// No subscribers after unsubscribe.
	expectConfirm(t, sub.cmd("UNSUBSCRIBE", "ch"), "unsubscribe", "ch", 0)
	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 0 {
		t.Errorf("PUBLISH after unsubscribe: got %d, want 0", n)
	}
}

func TestPublishMultipleSubscribers(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, c1.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	expectConfirm(t, c2.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)

	if n := pub.intWant("PUBLISH", "ch", "x"); n != 2 {
		t.Errorf("PUBLISH: got %d, want 2", n)
	}
	expectPubMessage(t, c1.push(), "ch", []byte("x"))
	expectPubMessage(t, c2.push(), "ch", []byte("x"))
}

func TestPatternSubscribe(t *testing.T) {
	addr := startTestServer(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("PSUBSCRIBE", "news.*"), "psubscribe", "news.*", 1)

	if n := pub.intWant("PUBLISH", "news.ai", "story"); n != 1 {
		t.Errorf("PUBLISH: got %d, want 1", n)
	}
	v := sub.push()
	if v.Kind != resp.KindArray || len(v.Array) != 4 {
		t.Fatalf("pmessage: got %+v, want 4-element array", v)
	}
	if string(v.Array[0].Str) != "pmessage" || string(v.Array[1].Str) != "news.*" ||
		string(v.Array[2].Str) != "news.ai" || string(v.Array[3].Str) != "story" {
		t.Errorf("pmessage: got %+v", v.Array)
	}

	// Non-matching channel.
	if n := pub.intWant("PUBLISH", "other", "x"); n != 0 {
		t.Errorf("PUBLISH non-matching: got %d, want 0", n)
	}
}

func TestBothChannelAndPatternCounted(t *testing.T) {
	addr := startTestServer(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	expectConfirm(t, sub.cmd("PSUBSCRIBE", "c*"), "psubscribe", "c*", 2)

	// Same client gets both deliveries; PUBLISH counts each delivery.
	if n := pub.intWant("PUBLISH", "ch", "x"); n != 2 {
		t.Errorf("PUBLISH: got %d, want 2", n)
	}
	expectPubMessage(t, sub.push(), "ch", []byte("x"))
	v := sub.push()
	if v.Kind != resp.KindArray || len(v.Array) != 4 {
		t.Fatalf("pmessage: got %+v, want 4-element array", v)
	}
}

func TestSubscribeModeRestriction(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	expectConfirm(t, c.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)

	v := c.cmd("GET", "foo")
	if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "Can't execute 'get'") {
		t.Errorf("GET in subscribe mode: got %+v, want Can't execute error", v)
	}

	// PING is allowed and answered with ["pong", ""].
	v = c.cmd("PING")
	if v.Kind != resp.KindArray || len(v.Array) != 2 ||
		string(v.Array[0].Str) != "pong" || string(v.Array[1].Str) != "" {
		t.Errorf("PING in subscribe mode: got %+v, want [pong, \"\"]", v)
	}
	v = c.cmd("PING", "hello")
	if v.Kind != resp.KindArray || len(v.Array) != 2 ||
		string(v.Array[0].Str) != "pong" || string(v.Array[1].Str) != "hello" {
		t.Errorf("PING msg in subscribe mode: got %+v", v)
	}
}

func TestUnsubscribeWithoutSubscriptions(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	v := c.cmd("UNSUBSCRIBE")
	if v.Kind != resp.KindArray || len(v.Array) != 3 ||
		string(v.Array[0].Str) != "unsubscribe" || !v.Array[1].IsNull || v.Array[2].Int != 0 {
		t.Errorf("UNSUBSCRIBE: got %+v, want [unsubscribe, nil, 0]", v)
	}
}

func TestSubscribeCleanupOnDisconnect(t *testing.T) {
	addr := startTestServer(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("SUBSCRIBE", "gone"), "subscribe", "gone", 1)
	// Drop the subscriber connection abruptly.
	sub.conn.Close()

	// The server learns about the closed connection asynchronously: the
	// reader goroutine has to observe the EOF before it can reap the
	// subscription. Until that happens the subscriber is still registered
	// and a publish may still reach it — real Redis behaves the same way.
	// So poll until the subscriber is gone instead of racing the reaper.
	deadline := time.Now().Add(2 * time.Second)
	for {
		n := pub.intWant("PUBLISH", "gone", "x")
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("PUBLISH after disconnect: got %d, want 0 (subscriber was never reaped)", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSubscribeQuit(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	expectConfirm(t, c.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	if v := c.cmd("QUIT"); v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		t.Errorf("QUIT: got %+v, want OK", v)
	}
}
