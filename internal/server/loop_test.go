package server_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

// TestLoopModeCommands checks that the event loop serves the commands like
// the goroutine model: the dispatch is the same, only the I/O differs.
func TestLoopModeCommands(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)

	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Fatalf("PING: got %+v", v)
	}
	c.ok("SET", "k", "v")
	if v := c.bulkWant("GET", "k"); string(v) != "v" {
		t.Errorf("GET: got %q, want v", v)
	}
	if n := c.intWant("STRLEN", "k"); n != 1 {
		t.Errorf("STRLEN: got %d, want 1", n)
	}
	if n := c.intWant("INCR", "counter"); n != 1 {
		t.Errorf("INCR: got %d, want 1", n)
	}
	c.intWant("HSET", "h", "f", "v")
	arr := c.arrWant("HGETALL", "h")
	if len(arr) != 2 || string(arr[0].Str) != "f" || string(arr[1].Str) != "v" {
		t.Errorf("HGETALL: got %+v", arr)
	}
	c.intWant("SADD", "s", "m")
	if n := c.intWant("SCARD", "s"); n != 1 {
		t.Errorf("SCARD: got %d, want 1", n)
	}
	c.intWant("LPUSH", "l", "a", "b")
	arr = c.arrWant("LRANGE", "l", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "b" || string(arr[1].Str) != "a" {
		t.Errorf("LRANGE: got %+v", arr)
	}
	if n := c.intWant("DEL", "k"); n != 1 {
		t.Errorf("DEL: got %d, want 1", n)
	}
	if v := c.cmd("GET", "k"); !v.IsNull {
		t.Errorf("GET of a missing key: got %+v, want null", v)
	}
}

// TestLoopModePipeline checks that the commands of a pipeline are answered in
// one write: the event loop runs the commands of the buffer and writes the
// replies together.
func TestLoopModePipeline(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	conn, r, w := c.conn, c.r, c.w
	defer conn.Close()

	for i := 0; i < 100; i++ {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("SET")),
			resp.NewBulkString([]byte("key")),
			resp.NewBulkString([]byte("value")),
		})
		if err := w.WriteValue(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		v, err := r.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
			t.Fatalf("the answer %d is %+v", i, v)
		}
	}
}

// TestLoopModeProtocolError checks that a wrong protocol is answered and the
// connection is closed, like in the goroutine model.
func TestLoopModeProtocolError(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	conn, r, w := c.conn, c.r, c.w
	defer conn.Close()

	if err := w.WriteRaw([]byte("+PING\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "Protocol error") {
		t.Fatalf("got %+v, want a protocol error", v)
	}
}

// TestLoopModeBlocking checks that a blocking command waits for a key without
// holding the loop: the other connections are served while it waits.
func TestLoopModeBlocking(t *testing.T) {
	addr := startTestServerLoop(t)
	blocked := newClient(t, addr)
	other := newClient(t, addr)

	// The client waits for the key.
	blocked.send("BLPOP", "l", "0")

	// The loop goes on with the other connections.
	if v := other.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Fatalf("PING while a client waits: got %+v", v)
	}

	// The value wakes the waiting client.
	other.intWant("LPUSH", "l", "v")
	arr := readArray(t, blocked)
	if len(arr) != 2 || string(arr[0].Str) != "l" || string(arr[1].Str) != "v" {
		t.Fatalf("BLPOP after a push: got %+v, want [l v]", arr)
	}
}

// TestLoopModeBlockingTimeout checks that a blocking command gives up after
// the timeout and answers with a null array, like in Redis.
func TestLoopModeBlockingTimeout(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)

	start := time.Now()
	c.send("BLPOP", "nolist", "0.2")
	v := readValue(t, c)
	if v.Kind != resp.KindArray || !v.IsNull {
		t.Fatalf("BLPOP timeout: got %+v, want a null array", v)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Errorf("BLPOP returned after %v, want at least the timeout", d)
	}
}

// TestLoopModeBlockingZSet checks the blocking pop of a sorted set.
func TestLoopModeBlockingZSet(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "m1", "2", "m2")
	arr := c.arrWant("BZPOPMIN", "z", "0")
	if len(arr) != 3 || string(arr[0].Str) != "z" || string(arr[1].Str) != "m1" {
		t.Errorf("BZPOPMIN: got %+v, want [z m1 1]", arr)
	}
}

// TestLoopModeBlockingMove checks the blocking move between two lists.
func TestLoopModeBlockingMove(t *testing.T) {
	addr := startTestServerLoop(t)
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

// TestLoopModeBlockingWrongType checks that the blocking commands report a
// wrong type for keys of another type.
func TestLoopModeBlockingWrongType(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range [][]string{
		{"BLPOP", "str", "0"},
		{"BRPOP", "str", "0"},
		{"BZPOPMIN", "str", "0"},
	} {
		if v := c.cmd(cmd...); v.Kind != resp.KindError {
			t.Errorf("%v: got %+v, want an error", cmd, v)
		}
	}
}

// TestLoopModePubSub checks that the messages of the subscribers are
// delivered: the event loop writes them with the next replies.
func TestLoopModePubSub(t *testing.T) {
	addr := startTestServerLoop(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("SUBSCRIBE", "ch"), "subscribe", "ch", 1)
	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 1 {
		t.Fatalf("PUBLISH: got %d, want 1", n)
	}
	expectPubMessage(t, sub.push(), "ch", []byte("hello"))

	// The subscriber leaves: the messages stop.
	expectConfirm(t, sub.cmd("UNSUBSCRIBE", "ch"), "unsubscribe", "ch", 0)
	if n := pub.intWant("PUBLISH", "ch", "hello"); n != 0 {
		t.Errorf("PUBLISH after unsubscribe: got %d, want 0", n)
	}
}

// TestLoopModePubSubPattern checks the delivery to the pattern subscribers.
func TestLoopModePubSubPattern(t *testing.T) {
	addr := startTestServerLoop(t)
	sub := newClient(t, addr)
	pub := newClient(t, addr)

	expectConfirm(t, sub.cmd("PSUBSCRIBE", "news.*"), "psubscribe", "news.*", 1)
	if n := pub.intWant("PUBLISH", "news.ai", "story"); n != 1 {
		t.Fatalf("PUBLISH: got %d, want 1", n)
	}
	v := sub.push()
	if v.Kind != resp.KindArray || len(v.Array) != 4 {
		t.Fatalf("pmessage: got %+v, want a 4-element array", v)
	}
	if string(v.Array[0].Str) != "pmessage" || string(v.Array[1].Str) != "news.*" ||
		string(v.Array[2].Str) != "news.ai" || string(v.Array[3].Str) != "story" {
		t.Errorf("pmessage: got %+v", v.Array)
	}
}

// TestLoopModeClientList checks that the clients of the event loop are
// visible in the client list.
func TestLoopModeClientList(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	other := newClient(t, addr)
	if v := other.cmd("PING"); v.Kind != resp.KindSimpleString {
		t.Fatalf("PING: got %+v", v)
	}

	list := string(c.bulkWant("CLIENT", "LIST"))
	if !strings.Contains(list, "cmd=ping") {
		t.Errorf("CLIENT LIST: got %q, want the last command of the client", list)
	}
	lines := strings.Split(list, "\n")
	if len(lines) != 2 {
		t.Errorf("CLIENT LIST: got %d clients, want 2: %q", len(lines), list)
	}
}

// TestLoopModeClientKill checks that a client of the event loop can be killed.
func TestLoopModeClientKill(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	victim := newClient(t, addr)
	if v := victim.cmd("PING"); v.Kind != resp.KindSimpleString {
		t.Fatalf("PING: got %+v", v)
	}

	id := victim.intWant("CLIENT", "ID")
	if n := c.intWant("CLIENT", "KILL", "ID", fmt.Sprint(id)); n != 1 {
		t.Fatalf("CLIENT KILL: got %d, want 1", n)
	}
	// The victim is disconnected: the read fails.
	victim.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := victim.r.ReadValue(); err == nil {
		t.Errorf("the killed client is still connected")
	}
}

// TestLoopModeClientReply checks that CLIENT REPLY OFF and SKIP suppress the
// answers, like in the goroutine model.
func TestLoopModeClientReply(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	conn := c.conn
	defer conn.Close()

	// The answer of CLIENT REPLY OFF is suppressed as well.
	c.send("CLIENT", "REPLY", "OFF")
	// The answer of the next command is suppressed too.
	c.send("PING")
	// The answer of this one is not.
	c.send("CLIENT", "REPLY", "ON")

	// Only the answer of the last command comes.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	v, err := c.r.ReadValue()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		t.Errorf("got %+v, want OK", v)
	}
	// Nothing else is coming.
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := c.r.ReadValue(); err == nil {
		t.Errorf("the suppressed answers were sent")
	}
}

// TestLoopModeQuit checks that QUIT is answered and the connection is closed.
func TestLoopModeQuit(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)
	if v := c.cmd("QUIT"); v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		t.Fatalf("QUIT: got %+v, want OK", v)
	}
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadValue(); err == nil {
		t.Errorf("the connection is still open after QUIT")
	}
}

// TestLoopModeLargeReply checks that a reply bigger than the socket buffer is
// delivered whole: the connection is watched for writing until the rest of
// the answer goes out.
func TestLoopModeLargeReply(t *testing.T) {
	addr := startTestServerLoop(t)
	c := newClient(t, addr)

	value := strings.Repeat("x", 4<<20)
	c.ok("SET", "big", value)
	got := c.bulkWant("GET", "big")
	if len(got) != len(value) {
		t.Fatalf("GET: got %d bytes, want %d", len(got), len(value))
	}
}

// TestLoopModeParity runs the same commands against the goroutine model and
// the event loop and compares the answers: the two models must behave the
// same.
func TestLoopModeParity(t *testing.T) {
	commands := [][]string{
		{"SET", "k", "v"},
		{"GET", "k"},
		{"STRLEN", "k"},
		{"INCR", "n"},
		{"HSET", "h", "f", "v"},
		{"HGETALL", "h"},
		{"SADD", "s", "m"},
		{"SMEMBERS", "s"},
		{"LPUSH", "l", "a", "b"},
		{"LRANGE", "l", "0", "-1"},
		{"ZADD", "z", "1", "m"},
		{"ZRANGE", "z", "0", "-1"},
		{"TYPE", "k"},
		{"EXISTS", "k"},
		{"TTL", "k"},
		{"DBSIZE"},
		{"BLPOP", "nolist", "0.05"},
		{"PING"},
		{"ECHO", "hi"},
		{"DEL", "k"},
		{"GET", "k"},
	}
	answers := map[string][]string{}
	for _, mode := range []struct {
		name  string
		start func(t *testing.T) string
	}{
		{"goroutine", startTestServer},
		{"loop", startTestServerLoop},
	} {
		addr := mode.start(t)
		c := newClient(t, addr)
		out := make([]string, 0, len(commands))
		for _, cmd := range commands {
			v := c.cmd(cmd...)
			out = append(out, fmt.Sprintf("%v -> %+v", cmd, v))
		}
		answers[mode.name] = out
	}
	for i, cmd := range commands {
		if answers["goroutine"][i] != answers["loop"][i] {
			t.Errorf("%v: goroutine %s, loop %s", cmd, answers["goroutine"][i], answers["loop"][i])
		}
	}
}

// readValue reads the next answer of a client.
func readValue(t *testing.T, c *client) resp.Value {
	t.Helper()
	v, err := c.r.ReadValue()
	if err != nil {
		t.Fatalf("read answer: %v", err)
	}
	return v
}

// readArray reads the next answer of a client and checks that it is an array.
func readArray(t *testing.T, c *client) []resp.Value {
	t.Helper()
	v := readValue(t, c)
	if v.Kind != resp.KindArray {
		t.Fatalf("got %+v, want an array", v)
	}
	return v.Array
}
