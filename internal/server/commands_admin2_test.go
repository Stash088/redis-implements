package server_test

import (
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestCommandCount(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	n := c.intWant("COMMAND", "COUNT")
	if n < 10 {
		t.Errorf("COMMAND COUNT: got %d, want at least 10", n)
	}
}

func TestCommandInfo(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	// The reply holds one entry per requested command.
	outer := c.arrWant("COMMAND", "INFO", "get")
	if len(outer) != 1 {
		t.Fatalf("COMMAND INFO get: got %d entries, want 1", len(outer))
	}
	arr := outer[0].Array
	if len(arr) != 6 {
		t.Fatalf("COMMAND INFO get: got %d elements, want 6", len(arr))
	}
	if string(arr[0].Str) != "get" {
		t.Errorf("name: got %q, want get", arr[0].Str)
	}
	if arr[1].Kind != resp.KindInteger || arr[1].Int != 2 {
		t.Errorf("arity: got %+v, want 2", arr[1])
	}
	// The flags are a set in RESP3 and an array in RESP2.
	flags := arr[2]
	if flags.Kind != resp.KindArray && flags.Kind != resp.KindSet {
		t.Fatalf("flags: got kind %v, want an array or a set", flags.Kind)
	}
	if len(flags.Array) == 0 {
		t.Error("flags: want at least one flag")
	}
	// An unknown command is nil.
	arr2 := c.arrWant("COMMAND", "INFO", "nosuchcommand")
	if len(arr2) != 1 || !arr2[0].IsNull {
		t.Errorf("COMMAND INFO of an unknown command: got %+v, want a nil", arr2)
	}
}

func TestCommandListAndDocs(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	names := c.arrWant("COMMAND", "LIST")
	found := false
	for _, n := range names {
		if string(n.Str) == "get" {
			found = true
		}
	}
	if !found {
		t.Error("COMMAND LIST: get is missing")
	}

	docs := c.arrWant("COMMAND", "DOCS", "get")
	if len(docs) < 2 || string(docs[0].Str) != "get" {
		t.Fatalf("COMMAND DOCS get: got %+v", docs)
	}
}

func TestPubSubIntrospection(t *testing.T) {
	addr := startTestServer(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("SUBSCRIBE", "news"), "subscribe", "news", 1)
	expectConfirm(t, sub.cmd("PSUBSCRIBE", "sport:*"), "psubscribe", "sport:*", 2)

	channels := pub.arrWant("PUBSUB", "CHANNELS")
	if len(channels) != 1 || string(channels[0].Str) != "news" {
		t.Errorf("PUBSUB CHANNELS: got %+v, want [news]", channels)
	}
	if n := pub.intWant("PUBSUB", "NUMPAT"); n != 1 {
		t.Errorf("PUBSUB NUMPAT: got %d, want 1", n)
	}
	numsub := pub.arrWant("PUBSUB", "NUMSUB", "news")
	if len(numsub) != 2 || numsub[1].Int != 1 {
		t.Errorf("PUBSUB NUMSUB news: got %+v, want [news, 1]", numsub)
	}
}

func TestResetCommand(t *testing.T) {
	addr := startTestServerDBs(t, 3)
	c := newClient(t, addr)
	c.ok("SELECT", "2")
	c.ok("CLIENT", "SETNAME", "worker")
	c.ok("MULTI")
	if v := c.cmd("SET", "k", "v"); v.Kind != resp.KindSimpleString || string(v.Str) != "QUEUED" {
		t.Fatalf("queued SET: got %+v, want +QUEUED", v)
	}
	// RESET is not allowed inside a transaction, like in Redis.
	if msg := c.expectError("RESET"); !strings.Contains(msg, "MULTI") {
		t.Errorf("RESET inside MULTI: got %q, want an error", msg)
	}
	c.ok("DISCARD")

	if v := c.cmd("RESET"); v.Kind != resp.KindSimpleString || string(v.Str) != "RESET" {
		t.Fatalf("RESET: got %+v, want +RESET", v)
	}
	// The name is cleared and the connection is back in database 0.
	if v := c.cmd("CLIENT", "GETNAME"); string(v.Str) != "" {
		t.Errorf("name after RESET: got %q, want empty", v.Str)
	}
	if v := c.cmd("CLIENT", "INFO"); !strings.Contains(string(v.Str), "db=0") {
		t.Errorf("CLIENT INFO after RESET: got %q, want db=0", v.Str)
	}
	// The transaction was discarded: the queued SET never ran.
	if v := c.cmd("GET", "k"); !isNullBulk(v) {
		t.Errorf("GET after RESET: got %+v, want null", v)
	}
}

// TestResetUnsubscribes checks that RESET removes the subscriptions.
func TestResetUnsubscribes(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	expectConfirm(t, c.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	// RESET is allowed in subscribe mode.
	if v := c.cmd("RESET"); v.Kind != resp.KindSimpleString || string(v.Str) != "RESET" {
		t.Fatalf("RESET: got %+v, want +RESET", v)
	}
	// The client left the subscribe mode and can run normal commands.
	c.ok("SET", "k", "v")
}

func TestClientInfoAndKill(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	info := c.bulkWant("CLIENT", "INFO")
	if len(info) == 0 {
		t.Error("CLIENT INFO: empty reply")
	}
	id := c.intWant("CLIENT", "ID")
	if msg := c.expectError("CLIENT", "KILL", "ID", "999999"); msg == "" {
		t.Error("CLIENT KILL of an unknown id: want an error")
	}
	_ = id
}

func TestClientReplyModes(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	// SKIP suppresses the answer to the next command only.
	c.ok("CLIENT", "REPLY", "SKIP")
	c.send("PING") // the answer is skipped
	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Errorf("PING after a skipped one: got %+v, want +PONG", v)
	}

	// OFF suppresses everything until ON; the answer to the OFF command
	// itself is not sent either.
	c.send("CLIENT", "REPLY", "OFF")
	c.send("PING")
	// Turning the replies back on answers the command itself.
	if v := c.cmd("CLIENT", "REPLY", "ON"); v.Kind != resp.KindSimpleString {
		t.Fatalf("CLIENT REPLY ON: got %+v, want +OK", v)
	}
	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Errorf("PING after CLIENT REPLY ON: got %+v, want +PONG", v)
	}
}

func TestTimeAndLastSave(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	arr := c.arrWant("TIME")
	if len(arr) != 2 {
		t.Errorf("TIME: got %d elements, want 2", len(arr))
	}
	if n := c.cmd("LASTSAVE"); n.Kind != resp.KindInteger {
		t.Errorf("LASTSAVE: got kind %v, want an integer", n.Kind)
	}
}

func TestRandomKeyTouchUnlink(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if got := string(c.bulkWant("RANDOMKEY")); got != "k" {
		t.Errorf("RANDOMKEY: got %q, want k", got)
	}
	if n := c.intWant("TOUCH", "k", "missing"); n != 1 {
		t.Errorf("TOUCH: got %d, want 1", n)
	}
	if n := c.intWant("UNLINK", "k"); n != 1 {
		t.Errorf("UNLINK: got %d, want 1", n)
	}
	if v := c.cmd("GET", "k"); !isNullBulk(v) {
		t.Errorf("GET after UNLINK: got %+v, want null", v)
	}
	// An empty database has no random key.
	if v := c.cmd("RANDOMKEY"); !isNullBulk(v) {
		t.Errorf("RANDOMKEY on an empty database: got %+v, want null", v)
	}
}
