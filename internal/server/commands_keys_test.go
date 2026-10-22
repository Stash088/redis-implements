package server_test

import (
	"testing"
)

func TestRenameAndRenameNX(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	c.ok("RENAME", "k", "k2")
	if got := string(c.bulkWant("GET", "k2")); got != "v" {
		t.Errorf("GET after RENAME: got %q, want v", got)
	}
	// A missing source is an error.
	if msg := c.expectError("RENAME", "nosuch", "x"); msg == "" {
		t.Error("RENAME of a missing key: want an error")
	}
	// RENAMENX does not overwrite an existing destination.
	c.ok("SET", "a", "1")
	c.ok("SET", "b", "2")
	if n := c.intWant("RENAMENX", "a", "b"); n != 0 {
		t.Errorf("RENAMENX onto an existing key: got %d, want 0", n)
	}
	if got := string(c.bulkWant("GET", "b")); got != "2" {
		t.Errorf("RENAMENX must not overwrite: got %q, want 2", got)
	}
	if n := c.intWant("RENAMENX", "a", "c"); n != 1 {
		t.Errorf("RENAMENX: got %d, want 1", n)
	}
}

func TestCopyAndMove(t *testing.T) {
	addr := startTestServerDBs(t, 3)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	if n := c.intWant("COPY", "k", "k2"); n != 1 {
		t.Errorf("COPY: got %d, want 1", n)
	}
	if got := string(c.bulkWant("GET", "k2")); got != "v" {
		t.Errorf("GET after COPY: got %q, want v", got)
	}
	// Without REPLACE an existing destination is kept.
	if n := c.intWant("COPY", "k", "k2"); n != 0 {
		t.Errorf("COPY onto an existing key: got %d, want 0", n)
	}
	if n := c.intWant("COPY", "k", "k2", "REPLACE"); n != 1 {
		t.Errorf("COPY with REPLACE: got %d, want 1", n)
	}
	// A copy into another database.
	if n := c.intWant("COPY", "k", "k3", "DB", "2"); n != 1 {
		t.Errorf("COPY into another database: got %d, want 1", n)
	}
	c.ok("SELECT", "2")
	if got := string(c.bulkWant("GET", "k3")); got != "v" {
		t.Errorf("the copy in the other database: got %q, want v", got)
	}
	c.ok("SELECT", "0")
	// MOVE moves the key to another database.
	if n := c.intWant("MOVE", "k2", "1"); n != 1 {
		t.Errorf("MOVE: got %d, want 1", n)
	}
	if v := c.cmd("GET", "k2"); !isNullBulk(v) {
		t.Errorf("the key must be gone from the source database: %+v", v)
	}
	c.ok("SELECT", "1")
	if got := string(c.bulkWant("GET", "k2")); got != "v" {
		t.Errorf("the moved key: got %q, want v", got)
	}
	// Moving into the same database is an error.
	if msg := c.expectError("MOVE", "k2", "1"); msg == "" {
		t.Error("MOVE into the same database: want an error")
	}
}

func TestDumpRestore(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "s", "hello")
	c.intWant("RPUSH", "l", "a", "b")
	c.intWant("HSET", "h", "f", "v")

	// The payload of a string.
	payload := c.bulkWant("DUMP", "s")
	if len(payload) == 0 {
		t.Fatal("DUMP: empty payload")
	}
	c.ok("RESTORE", "s2", "0", string(payload))
	if got := string(c.bulkWant("GET", "s2")); got != "hello" {
		t.Errorf("GET after RESTORE: got %q, want hello", got)
	}
	// An existing destination is refused without REPLACE.
	if msg := c.expectError("RESTORE", "s2", "0", string(payload)); msg == "" {
		t.Error("RESTORE onto an existing key: want an error")
	}
	c.ok("RESTORE", "s2", "0", string(payload), "REPLACE")
	// The payload of a collection.
	payload = c.bulkWant("DUMP", "l")
	c.ok("RESTORE", "l2", "0", string(payload))
	arr := c.arrWant("LRANGE", "l2", "0", "-1")
	if len(arr) != 2 || string(arr[0].Str) != "a" {
		t.Errorf("LRANGE after RESTORE: got %+v", arr)
	}
	// A missing key has no payload.
	if v := c.cmd("DUMP", "nosuch"); !isNullBulk(v) {
		t.Errorf("DUMP of a missing key: got %+v, want null", v)
	}
}

func TestExpireTimeAndSwapDB(t *testing.T) {
	addr := startTestServerDBs(t, 3)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	// A key without a TTL.
	if n := c.intWant("EXPIRETIME", "k"); n != -1 {
		t.Errorf("EXPIRETIME of a key without a TTL: got %d, want -1", n)
	}
	// A key with a TTL.
	c.ok("SET", "k2", "v", "EX", "100")
	ts := c.intWant("EXPIRETIME", "k2")
	if ts <= 0 {
		t.Errorf("EXPIRETIME: got %d, want a timestamp", ts)
	}
	pts := c.intWant("PEXPIRETIME", "k2")
	if pts < ts*1000 {
		t.Errorf("PEXPIRETIME: got %d, want about %d", pts, ts*1000)
	}
	// A missing key.
	if n := c.intWant("EXPIRETIME", "nosuch"); n != -2 {
		t.Errorf("EXPIRETIME of a missing key: got %d, want -2", n)
	}
	// SWAPDB exchanges the databases.
	c.ok("SET", "only0", "1")
	c.ok("SELECT", "1")
	c.ok("SET", "only1", "2")
	c.ok("SWAPDB", "0", "1")
	// The databases exchanged their content; the client is still on db1,
	// which now holds the key written in db0.
	if got := string(c.bulkWant("GET", "only0")); got != "1" {
		t.Errorf("after SWAPDB: got %q, want 1", got)
	}
	c.ok("SELECT", "0")
	if got := string(c.bulkWant("GET", "only1")); got != "2" {
		t.Errorf("after SWAPDB: got %q, want 2", got)
	}
}

func TestObjectEncoding(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "s", "v")
	c.intWant("RPUSH", "l", "a")
	c.intWant("HSET", "h", "f", "v")
	c.intWant("SADD", "st", "m")
	c.intWant("ZADD", "z", "1", "m")
	for _, key := range []string{"s", "l", "h", "st", "z"} {
		if v := c.bulkWant("OBJECT", "ENCODING", key); len(v) == 0 {
			t.Errorf("OBJECT ENCODING %s: empty reply", key)
		}
	}
	// A missing key has no encoding.
	if v := c.cmd("OBJECT", "ENCODING", "nosuch"); !isNullBulk(v) {
		t.Errorf("OBJECT ENCODING of a missing key: got %+v, want null", v)
	}
}

func TestSort(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "c", "a", "b")
	// Without ALPHA the elements must be numbers.
	if msg := c.expectError("SORT", "l"); msg == "" {
		t.Error("SORT of non-numeric elements: want an error")
	}
	arr := c.arrWant("SORT", "l", "ALPHA")
	if len(arr) != 3 || string(arr[0].Str) != "a" {
		t.Errorf("SORT ALPHA: got %+v, want [a b c]", arr)
	}
	arr = c.arrWant("SORT", "l", "ALPHA", "DESC")
	if len(arr) != 3 || string(arr[0].Str) != "c" {
		t.Errorf("SORT ALPHA DESC: got %+v, want [c b a]", arr)
	}
	arr = c.arrWant("SORT", "l", "ALPHA", "LIMIT", "0", "2")
	if len(arr) != 2 {
		t.Errorf("SORT with LIMIT: got %+v, want 2 elements", arr)
	}
	// Numeric sorting.
	c.intWant("RPUSH", "num", "3", "1", "2")
	arr = c.arrWant("SORT", "num")
	if len(arr) != 3 || string(arr[0].Str) != "1" {
		t.Errorf("SORT: got %+v, want [1 2 3]", arr)
	}
	// STORE writes the result to a key.
	if n := c.intWant("SORT", "num", "STORE", "sorted"); n != 3 {
		t.Errorf("SORT STORE: got %d, want 3", n)
	}
	arr = c.arrWant("LRANGE", "sorted", "0", "-1")
	if len(arr) != 3 || string(arr[0].Str) != "1" {
		t.Errorf("the stored result: got %+v", arr)
	}
	// BY sorts the elements by the value of the looked up key.
	c.intWant("RPUSH", "w", "a", "b", "c")
	c.ok("SET", "weight_a", "5")
	c.ok("SET", "weight_b", "1")
	arr = c.arrWant("SORT", "w", "BY", "weight_*")
	if len(arr) != 3 || string(arr[0].Str) != "c" {
		t.Errorf("SORT BY: got %+v, want the elements ordered by weight", arr)
	}
	// GET replaces the elements by the values they point at. Without BY the
	// elements are still compared numerically, so they must be numbers.
	c.intWant("RPUSH", "wn", "1", "2", "3")
	c.ok("SET", "data_1", "AAA")
	c.ok("SET", "data_2", "BBB")
	arr = c.arrWant("SORT", "wn", "GET", "data_*")
	if len(arr) != 3 || string(arr[0].Str) != "AAA" {
		t.Errorf("SORT GET: got %+v, want the values of the data keys", arr)
	}
}
