package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

// isNullBulk reports whether v is a null bulk string (the reply for a missing
// key).
func isNullBulk(v resp.Value) bool {
	return v.Kind == resp.KindBulkString && v.IsNull
}

// TestSelectSwitchesDatabase checks that SELECT moves the connection to
// another keyspace and that the databases stay isolated.
func TestSelectSwitchesDatabase(t *testing.T) {
	addr := startTestServerDBs(t, 4)
	c := newClient(t, addr)

	c.ok("SET", "k", "db0")
	if v := c.cmd("SELECT", "1"); v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		t.Fatalf("SELECT: got %+v, want +OK", v)
	}
	c.ok("SET", "k", "db1")

	// In db1 the key holds the value written there, not the one from db0.
	if got := string(c.bulkWant("GET", "k")); got != "db1" {
		t.Errorf("GET in db1: got %q, want db1", got)
	}

	// Back to db0: the original value is there.
	c.ok("SELECT", "0")
	if got := string(c.bulkWant("GET", "k")); got != "db0" {
		t.Errorf("GET in db0: got %q, want db0", got)
	}

	// The selection is per connection: a fresh one starts in db0 and sees
	// the db0 value, not the one written in db1.
	other := newClient(t, addr)
	if got := string(other.bulkWant("GET", "k")); got != "db0" {
		t.Errorf("GET on a fresh connection: got %q, want db0", got)
	}
}

// TestSelectOutOfRange checks the error for an unknown database index.
func TestSelectOutOfRange(t *testing.T) {
	addr := startTestServerDBs(t, 2)
	c := newClient(t, addr)
	if msg := c.expectError("SELECT", "5"); msg == "" {
		t.Error("SELECT out of range: want an error")
	}
	if msg := c.expectError("SELECT", "-1"); msg == "" {
		t.Error("SELECT negative: want an error")
	}
	if msg := c.expectError("SELECT", "abc"); msg == "" {
		t.Error("SELECT non-numeric: want an error")
	}
	// The connection stays usable.
	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
		t.Errorf("PING after a failed SELECT: got %+v, want +PONG", v)
	}
}

// TestFlushAll checks that FLUSHALL empties every database.
func TestFlushAll(t *testing.T) {
	addr := startTestServerDBs(t, 3)
	c := newClient(t, addr)
	c.ok("SET", "a", "1")
	c.ok("SELECT", "2")
	c.ok("SET", "b", "2")
	c.ok("FLUSHALL")

	c.ok("SELECT", "0")
	if v := c.cmd("GET", "a"); !isNullBulk(v) {
		t.Errorf("db0 after FLUSHALL: got %+v, want null", v)
	}
	c.ok("SELECT", "2")
	if v := c.cmd("GET", "b"); !isNullBulk(v) {
		t.Errorf("db2 after FLUSHALL: got %+v, want null", v)
	}
}

// TestFlushDBOnlyCurrent checks that FLUSHDB touches only the current
// database.
func TestFlushDBOnlyCurrent(t *testing.T) {
	addr := startTestServerDBs(t, 3)
	c := newClient(t, addr)
	c.ok("SET", "a", "1")
	c.ok("SELECT", "1")
	c.ok("SET", "b", "2")
	c.ok("FLUSHDB")

	c.ok("SELECT", "0")
	if got := string(c.bulkWant("GET", "a")); got != "1" {
		t.Errorf("db0 key after FLUSHDB in db1: got %q, want 1", got)
	}
	c.ok("SELECT", "1")
	if v := c.cmd("GET", "b"); !isNullBulk(v) {
		t.Errorf("db1 key after FLUSHDB: got %+v, want null", v)
	}
}

// TestSelectInTransaction checks that a queued SELECT switches the database
// for the commands that follow it inside the same transaction.
func TestSelectInTransaction(t *testing.T) {
	addr := startTestServerDBs(t, 3)
	c := newClient(t, addr)
	c.ok("SELECT", "2")
	c.ok("MULTI")
	if v := c.cmd("SET", "k", "in-db2"); v.Kind != resp.KindSimpleString {
		t.Fatalf("queued SET: got %+v, want +QUEUED", v)
	}
	if v := c.cmd("EXEC"); v.Kind != resp.KindArray || len(v.Array) != 1 {
		t.Fatalf("EXEC: got %+v, want a 1-element array", v)
	}
	if got := string(c.bulkWant("GET", "k")); got != "in-db2" {
		t.Errorf("GET after SELECT inside MULTI: got %q", got)
	}
	c.ok("SELECT", "0")
	if v := c.cmd("GET", "k"); !isNullBulk(v) {
		t.Errorf("db0 must not see the db2 key: %+v", v)
	}
}
