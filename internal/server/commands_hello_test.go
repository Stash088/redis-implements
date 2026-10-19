package server_test

import (
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

// helloMap parses a HELLO reply (a RESP3 map) into a key/value lookup.
func helloMap(t *testing.T, v resp.Value) map[string]resp.Value {
	t.Helper()
	if v.Kind != resp.KindMap {
		t.Fatalf("HELLO reply: got kind %v, want a map", v.Kind)
	}
	if len(v.Array)%2 != 0 {
		t.Fatalf("HELLO reply: odd number of elements")
	}
	out := make(map[string]resp.Value, len(v.Array)/2)
	for i := 0; i < len(v.Array); i += 2 {
		out[string(v.Array[i].Str)] = v.Array[i+1]
	}
	return out
}

func TestHelloNegotiatesRESP3(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	v := c.cmd("HELLO", "3")
	fields := helloMap(t, v)
	if got := string(fields["proto"].Str); got == "" {
		if fields["proto"].Int != 3 {
			t.Errorf("proto: got %+v, want 3", fields["proto"])
		}
	}
	if fields["proto"].Int != 3 {
		t.Errorf("proto: got %+v, want 3", fields["proto"])
	}
	if got := string(fields["server"].Str); got == "" {
		t.Errorf("server: got %+v", fields["server"])
	}
	if fields["id"].Int == 0 {
		t.Errorf("id: got %+v, want a non-zero id", fields["id"])
	}
}

func TestHelloRESP2IsFlatArray(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	v := c.cmd("HELLO", "2")
	if v.Kind != resp.KindArray {
		t.Fatalf("HELLO 2: got kind %v, want an array", v.Kind)
	}
	if len(v.Array) != 14 {
		t.Errorf("HELLO 2: got %d elements, want 14", len(v.Array))
	}
	if string(v.Array[0].Str) != "server" {
		t.Errorf("HELLO 2: first element %q, want server", v.Array[0].Str)
	}
}

func TestHelloUnsupportedVersion(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	msg := c.expectError("HELLO", "4")
	if !strings.Contains(msg, "NOPROTO") {
		t.Errorf("HELLO 4: got %q, want a NOPROTO error", msg)
	}
	// The connection stays on RESP2.
	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Errorf("PING after a failed HELLO: got %+v", v)
	}
}

// TestRESP3HashIsMap checks that HGETALL answers with a map in RESP3 and
// with a flat array in RESP2.
func TestRESP3HashIsMap(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if n := c.intWant("HSET", "h", "f1", "v1", "f2", "v2"); n != 2 {
		t.Fatalf("HSET: got %d new fields, want 2", n)
	}

	if v := c.cmd("HGETALL", "h"); v.Kind != resp.KindArray {
		t.Errorf("HGETALL in RESP2: got kind %v, want an array", v.Kind)
	}

	c.cmd("HELLO", "3")
	v := c.cmd("HGETALL", "h")
	if v.Kind != resp.KindMap {
		t.Fatalf("HGETALL in RESP3: got kind %v, want a map", v.Kind)
	}
	if len(v.Array) != 4 {
		t.Errorf("HGETALL in RESP3: got %d elements, want 4", len(v.Array))
	}
}

// TestRESP3ScoreIsDouble checks that ZSCORE answers with a double in RESP3.
func TestRESP3ScoreIsDouble(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if n := c.intWant("ZADD", "z", "1.5", "m"); n != 1 {
		t.Fatalf("ZADD: got %d, want 1", n)
	}

	if v := c.cmd("ZSCORE", "z", "m"); v.Kind != resp.KindBulkString {
		t.Errorf("ZSCORE in RESP2: got kind %v, want a bulk string", v.Kind)
	}

	c.cmd("HELLO", "3")
	v := c.cmd("ZSCORE", "z", "m")
	if v.Kind != resp.KindDouble {
		t.Fatalf("ZSCORE in RESP3: got kind %v, want a double", v.Kind)
	}
	if v.Float != 1.5 {
		t.Errorf("ZSCORE in RESP3: got %v, want 1.5", v.Float)
	}
}

// TestRESP3PushMessages checks that Pub/Sub messages are push frames in
// RESP3 and plain arrays in RESP2.
func TestRESP3PushMessages(t *testing.T) {
	addr := startTestServer(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	sub.cmd("HELLO", "3")
	expectConfirm(t, sub.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 1 {
		t.Fatalf("PUBLISH: got %d, want 1", n)
	}
	v := sub.push()
	if v.Kind != resp.KindPush {
		t.Errorf("message in RESP3: got kind %v, want a push", v.Kind)
	}
	if len(v.Array) != 3 || string(v.Array[0].Str) != "message" {
		t.Errorf("message in RESP3: got %+v", v.Array)
	}

	// In RESP2 the same message is a plain array.
	sub2 := newClient(t, addr)
	expectConfirm(t, sub2.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 2 {
		t.Fatalf("PUBLISH: got %d, want 2", n)
	}
	v2 := sub2.push()
	if v2.Kind != resp.KindArray {
		t.Errorf("message in RESP2: got kind %v, want an array", v2.Kind)
	}
}

// TestRESP3ZRangeWithScores checks the RESP3 shape of ZRANGE WITHSCORES: an
// array of [member, score] pairs with the score as a double.
func TestRESP3ZRangeWithScores(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1.5", "m")

	if v := c.cmd("ZRANGE", "z", "0", "-1", "WITHSCORES"); v.Kind != resp.KindArray || len(v.Array) != 2 {
		t.Fatalf("ZRANGE in RESP2: got %+v, want a flat 2-element array", v)
	}

	c.cmd("HELLO", "3")
	v := c.cmd("ZRANGE", "z", "0", "-1", "WITHSCORES")
	if v.Kind != resp.KindArray || len(v.Array) != 1 {
		t.Fatalf("ZRANGE in RESP3: got %+v, want a 1-element array", v)
	}
	pair := v.Array[0]
	if pair.Kind != resp.KindArray || len(pair.Array) != 2 {
		t.Fatalf("ZRANGE pair in RESP3: got %+v", pair)
	}
	if string(pair.Array[0].Str) != "m" {
		t.Errorf("member: got %q, want m", pair.Array[0].Str)
	}
	if pair.Array[1].Kind != resp.KindDouble || pair.Array[1].Float != 1.5 {
		t.Errorf("score: got %+v, want the double 1.5", pair.Array[1])
	}
}

// TestRESP3ConfigGetIsMap checks that CONFIG GET answers with a map in RESP3.
func TestRESP3ConfigGetIsMap(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if v := c.cmd("CONFIG", "GET", "maxmemory"); v.Kind != resp.KindArray {
		t.Errorf("CONFIG GET in RESP2: got kind %v, want an array", v.Kind)
	}

	c.cmd("HELLO", "3")
	v := c.cmd("CONFIG", "GET", "maxmemory")
	if v.Kind != resp.KindMap {
		t.Fatalf("CONFIG GET in RESP3: got kind %v, want a map", v.Kind)
	}
	if len(v.Array) != 2 || string(v.Array[0].Str) != "maxmemory" {
		t.Errorf("CONFIG GET in RESP3: got %+v", v.Array)
	}
}

// TestRESP3SMembersIsSet checks that SMEMBERS answers with a set in RESP3.
func TestRESP3SMembersIsSet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "s", "m1", "m2")

	if v := c.cmd("SMEMBERS", "s"); v.Kind != resp.KindArray {
		t.Errorf("SMEMBERS in RESP2: got kind %v, want an array", v.Kind)
	}

	c.cmd("HELLO", "3")
	v := c.cmd("SMEMBERS", "s")
	if v.Kind != resp.KindSet {
		t.Fatalf("SMEMBERS in RESP3: got kind %v, want a set", v.Kind)
	}
	if len(v.Array) != 2 {
		t.Errorf("SMEMBERS in RESP3: got %d members, want 2", len(v.Array))
	}
}
