package server_test

import (
	"testing"
	"time"
)

// TestPersistenceStreamRDBReload checks that a stream saved in an RDB
// snapshot survives a restart: the entries, the groups, the consumers and
// the pending list.
func TestPersistenceStreamRDBReload(t *testing.T) {
	dir := t.TempDir()
	addr, rdbPath, _ := startPersistServer(t, dir, false)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.bulkWant("XADD", "s", "2-0", "f", "v2")
	c.ok("XGROUP", "CREATE", "s", "grp", "0")
	c.intWant("XGROUP", "CREATECONSUMER", "s", "grp", "consumer")
	// The consumer reads the first entry: it goes to the pending list.
	c.arrWant("XREADGROUP", "GROUP", "grp", "consumer", "COUNT", "1", "STREAMS", "s", ">")
	c.ok("SAVE")

	addr2 := startFromFiles(t, rdbPath, false)
	c2 := newClient(t, addr2)
	if n := c2.intWant("XLEN", "s"); n != 2 {
		t.Errorf("XLEN after the reload: got %d, want 2", n)
	}
	arr := c2.arrWant("XRANGE", "s", "-", "+")
	if len(arr) != 2 {
		t.Fatalf("XRANGE after the reload: got %d entries, want 2", len(arr))
	}
	if string(arr[0].Array[0].Str) != "1-0" || string(arr[1].Array[0].Str) != "2-0" {
		t.Errorf("XRANGE after the reload: got %q and %q, want 1-0 and 2-0",
			arr[0].Array[0].Str, arr[1].Array[0].Str)
	}
	// The group, its consumer and the pending entry survive.
	groups := c2.arrWant("XINFO", "GROUPS", "s")
	if len(groups) == 0 {
		t.Fatal("XINFO GROUPS after the reload: no groups")
	}
	consumers := c2.arrWant("XINFO", "CONSUMERS", "s", "grp")
	if len(consumers) == 0 {
		t.Fatal("XINFO CONSUMERS after the reload: no consumers")
	}
	// The summary of XPENDING: the count, the smallest and the greatest
	// identifier and the consumers.
	pending := c2.arrWant("XPENDING", "s", "grp")
	if len(pending) != 4 {
		t.Fatalf("XPENDING after the reload: got %d elements, want 4", len(pending))
	}
	if pending[0].Int != 1 {
		t.Errorf("XPENDING after the reload: %d pending entries, want 1", pending[0].Int)
	}
	if string(pending[1].Str) != "1-0" {
		t.Errorf("XPENDING after the reload: the smallest id is %q, want 1-0", pending[1].Str)
	}
	if len(pending[3].Array) == 0 || string(pending[3].Array[0].Array[0].Str) != "consumer" {
		t.Errorf("XPENDING after the reload: the consumers are %+v, want consumer", pending[3].Array)
	}
}

// TestPersistenceStreamRDBReloadManyEntries checks a stream that is longer
// than one node of the RDB format.
func TestPersistenceStreamRDBReloadManyEntries(t *testing.T) {
	dir := t.TempDir()
	addr, rdbPath, _ := startPersistServer(t, dir, false)
	c := newClient(t, addr)
	for i := 1; i <= 150; i++ {
		c.bulkWant("XADD", "s", itoaTest(int64(i))+"-0", "f", "v")
	}
	c.ok("SAVE")

	addr2 := startFromFiles(t, rdbPath, false)
	c2 := newClient(t, addr2)
	if n := c2.intWant("XLEN", "s"); n != 150 {
		t.Errorf("XLEN after the reload: got %d, want 150", n)
	}
	arr := c2.arrWant("XRANGE", "s", "-", "+")
	if len(arr) != 150 {
		t.Fatalf("XRANGE after the reload: got %d entries, want 150", len(arr))
	}
	if string(arr[0].Array[0].Str) != "1-0" || string(arr[149].Array[0].Str) != "150-0" {
		t.Errorf("XRANGE after the reload: got %q and %q, want 1-0 and 150-0",
			arr[0].Array[0].Str, arr[149].Array[0].Str)
	}
}

// TestPersistenceStreamDUMP checks that a stream can be dumped and restored
// with the DUMP and RESTORE commands.
func TestPersistenceStreamDUMP(t *testing.T) {
	dir := t.TempDir()
	addr, _, _ := startPersistServer(t, dir, false)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.bulkWant("XADD", "s", "2-0", "f", "v2")
	payload := c.bulkWant("DUMP", "s")
	c.ok("RESTORE", "copy", "0", string(payload))
	if n := c.intWant("XLEN", "copy"); n != 2 {
		t.Errorf("XLEN of the restored stream: got %d, want 2", n)
	}
	arr := c.arrWant("XRANGE", "copy", "-", "+")
	if len(arr) != 2 {
		t.Fatalf("XRANGE of the restored stream: got %d entries, want 2", len(arr))
	}
	if string(arr[0].Array[0].Str) != "1-0" {
		t.Errorf("the restored stream starts with %q, want 1-0", arr[0].Array[0].Str)
	}
}

// itoaTest formats an integer for the test.
func itoaTest(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestPersistenceStreamSavePolicyReload checks that the periodic snapshot
// keeps the streams.
func TestPersistenceStreamSavePolicyReload(t *testing.T) {
	dir := t.TempDir()
	addr, rdbPath, _ := startPersistServer(t, dir, false)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.ok("SAVE")
	time.Sleep(20 * time.Millisecond)
	addr2 := startFromFiles(t, rdbPath, false)
	c2 := newClient(t, addr2)
	if n := c2.intWant("XLEN", "s"); n != 1 {
		t.Errorf("XLEN after the reload: got %d, want 1", n)
	}
}
