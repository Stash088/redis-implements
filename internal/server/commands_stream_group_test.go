package server_test

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

func TestXGroupCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("XGROUP", "CREATE", "s", "g", "0", "MKSTREAM")
	// The group exists now.
	if msg := c.expectError("XGROUP", "CREATE", "s", "g", "0"); !strings.Contains(msg, "BUSYGROUP") {
		t.Errorf("XGROUP CREATE of an existing group: got %q, want BUSYGROUP", msg)
	}
	// A missing key is an error.
	if msg := c.expectError("XGROUP", "CREATE", "nosuch", "g", "0"); !strings.Contains(msg, "requires the key to exist") {
		t.Errorf("XGROUP CREATE of a missing key: got %q, want the key error", msg)
	}
	// A missing group is an error.
	if msg := c.expectError("XGROUP", "SETID", "s", "nosuch", "1-0"); !strings.Contains(msg, "NOGROUP") {
		t.Errorf("XGROUP SETID of a missing group: got %q, want NOGROUP", msg)
	}
	// The consumer of the group.
	if n := c.intWant("XGROUP", "CREATECONSUMER", "s", "g", "c1"); n != 1 {
		t.Errorf("XGROUP CREATECONSUMER: got %d, want 1", n)
	}
	if n := c.intWant("XGROUP", "CREATECONSUMER", "s", "g", "c1"); n != 0 {
		t.Errorf("the second XGROUP CREATECONSUMER: got %d, want 0", n)
	}
	if n := c.intWant("XGROUP", "DELCONSUMER", "s", "g", "c1"); n != 0 {
		t.Errorf("XGROUP DELCONSUMER: got %d, want 0", n)
	}
	// The identifier of the group.
	c.ok("XGROUP", "SETID", "s", "g", "5-0")
	arr := c.arrWant("XINFO", "GROUPS", "s")
	if len(arr) != 1 {
		t.Fatalf("XINFO GROUPS: got %d groups, want 1", len(arr))
	}
	fields := arr[0].Array
	if len(fields) != 12 {
		t.Fatalf("XINFO GROUPS: got %d elements, want 12", len(fields))
	}
	if string(fields[6].Str) != "last-delivered-id" || string(fields[7].Str) != "5-0" {
		t.Errorf("XINFO GROUPS: got %q %q, want last-delivered-id 5-0", fields[6].Str, fields[7].Str)
	}
	// The group is destroyed.
	if n := c.intWant("XGROUP", "DESTROY", "s", "g"); n != 1 {
		t.Errorf("XGROUP DESTROY: got %d, want 1", n)
	}
	if n := c.intWant("XGROUP", "DESTROY", "s", "g"); n != 0 {
		t.Errorf("the second XGROUP DESTROY: got %d, want 0", n)
	}
}

func TestXReadAndXReadGroup(t *testing.T) {
	dbgSt := store.New()
	dbgSrv, err := server.New("127.0.0.1:0", []*store.Store{dbgSt})
	if err != nil {
		t.Fatal(err)
	}
	dbgSrv.SetLogger(log.New(os.Stderr, "DBG ", 0))
	go dbgSrv.Serve()
	t.Cleanup(func() { dbgSrv.Close() })
	addr := dbgSrv.Addr().String()
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.bulkWant("XADD", "s", "2-0", "f", "v2")

	// XREAD returns the entries after the given identifier.
	arr := c.arrWant("XREAD", "COUNT", "10", "STREAMS", "s", "0")
	if len(arr) != 1 {
		t.Fatalf("XREAD: got %d streams, want 1", len(arr))
	}
	if string(arr[0].Array[0].Str) != "s" {
		t.Errorf("XREAD: got the key %q, want s", arr[0].Array[0].Str)
	}
	entries := arr[0].Array[1].Array
	if len(entries) != 2 {
		t.Fatalf("XREAD: got %d entries, want 2", len(entries))
	}
	if string(entries[0].Array[0].Str) != "1-0" {
		t.Errorf("XREAD: got the identifier %q, want 1-0", entries[0].Array[0].Str)
	}
	// The "$" identifier means the new entries only.
	if v := c.cmd("XREAD", "COUNT", "10", "STREAMS", "s", "$"); !v.IsNull {
		t.Errorf("XREAD with $: got %+v, want null", v)
	}
	// The count limits the number of entries.
	arr = c.arrWant("XREAD", "COUNT", "1", "STREAMS", "s", "0")
	if len(arr[0].Array[1].Array) != 1 {
		t.Errorf("XREAD with COUNT: got %d entries, want 1", len(arr[0].Array[1].Array))
	}

	// XREADGROUP delivers the new entries to the consumer.
	c.ok("XGROUP", "CREATE", "s", "g", "0")
	arr = c.arrWant("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">")
	if len(arr) != 1 || len(arr[0].Array[1].Array) != 2 {
		t.Fatalf("XREADGROUP: got %+v, want 2 entries", arr)
	}
	// The entries are pending now.
	if v := c.cmd("XPENDING", "s", "g"); v.Kind != resp.KindArray || len(v.Array) != 4 || v.Array[0].Int != 2 {
		t.Errorf("XPENDING: got %+v, want 2 pending entries", v)
	}
	// The group does not deliver them again.
	if v := c.cmd("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">"); !v.IsNull {
		t.Errorf("the second XREADGROUP: got %+v, want null", v)
	}
	// The history of the consumer holds the pending entries.
	arr = c.arrWant("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", "0")
	if len(arr) != 1 || len(arr[0].Array[1].Array) != 2 {
		t.Errorf("the history of the consumer: got %+v, want 2 entries", arr)
	}
	// The history of a consumer without pending entry is answered as well.
	if n := c.intWant("XGROUP", "CREATECONSUMER", "s", "g", "c2"); n != 1 {
		t.Errorf("XGROUP CREATECONSUMER: got %d, want 1", n)
	}
	arr = c.arrWant("XREADGROUP", "GROUP", "g", "c2", "COUNT", "10", "STREAMS", "s", "0")
	if len(arr) != 1 || len(arr[0].Array[1].Array) != 0 {
		t.Errorf("the empty history: got %+v, want an empty list of entries", arr)
	}
	// The "$" and "+" identifiers do not work with XREADGROUP.
	if msg := c.expectError("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "$"); !strings.Contains(msg, "meaningless") {
		t.Errorf("XREADGROUP with $: got %q, want the error about the $ identifier", msg)
	}
	// A missing group is an error.
	if msg := c.expectError("XREADGROUP", "GROUP", "nosuch", "c1", "STREAMS", "s", ">"); !strings.Contains(msg, "NOGROUP") {
		t.Errorf("XREADGROUP with a missing group: got %q, want NOGROUP", msg)
	}
}

func TestXAckAndPending(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.bulkWant("XADD", "s", "2-0", "f", "v2")
	c.ok("XGROUP", "CREATE", "s", "g", "0")
	c.arrWant("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">")

	// The summary of the pending list.
	arr := c.arrWant("XPENDING", "s", "g")
	if len(arr) != 4 {
		t.Fatalf("XPENDING: got %d elements, want 4", len(arr))
	}
	if arr[0].Int != 2 {
		t.Errorf("XPENDING: got %d pending entries, want 2", arr[0].Int)
	}
	if string(arr[1].Str) != "1-0" || string(arr[2].Str) != "2-0" {
		t.Errorf("XPENDING: got %q and %q, want 1-0 and 2-0", arr[1].Str, arr[2].Str)
	}
	// The pending entries with their owner.
	arr = c.arrWant("XPENDING", "s", "g", "-", "+", "10")
	if len(arr) != 2 {
		t.Fatalf("XPENDING with a range: got %d entries, want 2", len(arr))
	}
	if len(arr[0].Array) != 4 {
		t.Fatalf("XPENDING with a range: got %d elements, want 4", len(arr[0].Array))
	}
	if string(arr[0].Array[0].Str) != "1-0" || string(arr[0].Array[1].Str) != "c1" {
		t.Errorf("XPENDING with a range: got %q and %q, want 1-0 and c1", arr[0].Array[0].Str, arr[0].Array[1].Str)
	}
	// The entries are acknowledged.
	if n := c.intWant("XACK", "s", "g", "1-0", "2-0"); n != 2 {
		t.Errorf("XACK: got %d, want 2", n)
	}
	if v := c.cmd("XPENDING", "s", "g"); v.Kind != resp.KindArray || v.Array[0].Int != 0 {
		t.Errorf("XPENDING after the acknowledgement: got %+v, want no pending entry", v)
	}
	// Acknowledging a missing group is not an error.
	if n := c.intWant("XACK", "s", "nosuch", "1-0"); n != 0 {
		t.Errorf("XACK of a missing group: got %d, want 0", n)
	}
}

func TestXClaimAndAutoClaim(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.ok("XGROUP", "CREATE", "s", "g", "0")
	c.arrWant("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">")

	// The entry is claimed by another consumer.
	arr := c.arrWant("XCLAIM", "s", "g", "c2", "0", "1-0")
	if len(arr) != 1 {
		t.Fatalf("XCLAIM: got %d entries, want 1", len(arr))
	}
	if string(arr[0].Array[0].Str) != "1-0" {
		t.Errorf("XCLAIM: got the identifier %q, want 1-0", arr[0].Array[0].Str)
	}
	// The pending entry belongs to the new consumer.
	arr = c.arrWant("XPENDING", "s", "g", "-", "+", "10")
	if string(arr[0].Array[1].Str) != "c2" {
		t.Errorf("XPENDING: got the consumer %q, want c2", arr[0].Array[1].Str)
	}
	// JUSTID returns the identifiers only.
	arr = c.arrWant("XCLAIM", "s", "g", "c3", "0", "1-0", "JUSTID")
	if len(arr) != 1 || arr[0].Kind != resp.KindBulkString || string(arr[0].Str) != "1-0" {
		t.Errorf("XCLAIM with JUSTID: got %+v, want 1-0", arr)
	}
	// The automatic claim takes the entries that were not acknowledged.
	arr = c.arrWant("XAUTOCLAIM", "s", "g", "c4", "0", "0")
	if len(arr) != 3 {
		t.Fatalf("XAUTOCLAIM: got %d elements, want 3", len(arr))
	}
	if len(arr[1].Array) != 1 {
		t.Errorf("XAUTOCLAIM: got %d entries, want 1", len(arr[1].Array))
	}
}

func TestXInfoConsumers(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.ok("XGROUP", "CREATE", "s", "g", "0")
	c.arrWant("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">")

	arr := c.arrWant("XINFO", "CONSUMERS", "s", "g")
	if len(arr) != 1 {
		t.Fatalf("XINFO CONSUMERS: got %d consumers, want 1", len(arr))
	}
	fields := arr[0].Array
	if len(fields) != 8 {
		t.Fatalf("XINFO CONSUMERS: got %d elements, want 8", len(fields))
	}
	if string(fields[0].Str) != "name" || string(fields[1].Str) != "c1" {
		t.Errorf("XINFO CONSUMERS: got %q %q, want name c1", fields[0].Str, fields[1].Str)
	}
	if string(fields[2].Str) != "pending" || fields[3].Int != 1 {
		t.Errorf("XINFO CONSUMERS: got %q %d, want pending 1", fields[2].Str, fields[3].Int)
	}
	// A missing key is an error.
	if msg := c.expectError("XINFO", "CONSUMERS", "nosuch", "g"); !strings.Contains(msg, "no such key") {
		t.Errorf("XINFO CONSUMERS of a missing key: got %q, want no such key", msg)
	}
	// A missing group is an error.
	if msg := c.expectError("XINFO", "CONSUMERS", "s", "nosuch"); !strings.Contains(msg, "NOGROUP") {
		t.Errorf("XINFO CONSUMERS of a missing group: got %q, want NOGROUP", msg)
	}
}

func TestXInfoStreamFull(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.bulkWant("XADD", "s", "1-0", "f", "v1")
	c.ok("XGROUP", "CREATE", "s", "g", "0")
	c.arrWant("XREADGROUP", "GROUP", "g", "c1", "COUNT", "10", "STREAMS", "s", ">")

	arr := c.arrWant("XINFO", "STREAM", "s", "FULL")
	// The full form holds nine pairs: the fields of the stream, its entries
	// and its groups.
	if len(arr) != 18 {
		t.Fatalf("XINFO STREAM FULL: got %d elements, want 18", len(arr))
	}
	if string(arr[14].Str) != "entries" {
		t.Errorf("XINFO STREAM FULL: got %q, want entries", arr[14].Str)
	}
	if len(arr[15].Array) != 1 || string(arr[15].Array[0].Array[0].Str) != "1-0" {
		t.Errorf("XINFO STREAM FULL: got the entries %+v, want the entry 1-0", arr[15].Array)
	}
	if string(arr[16].Str) != "groups" {
		t.Errorf("XINFO STREAM FULL: got %q, want groups", arr[16].Str)
	}
	// The group holds the pending entry and its consumer.
	group := arr[17].Array[0].Array
	if len(group) != 14 {
		t.Fatalf("XINFO STREAM FULL: got %d elements in the group, want 14", len(group))
	}
	if string(group[0].Str) != "name" || string(group[1].Str) != "g" {
		t.Errorf("XINFO STREAM FULL: got %q %q, want name g", group[0].Str, group[1].Str)
	}
	if string(group[10].Str) != "pending" {
		t.Errorf("XINFO STREAM FULL: got %q, want pending", group[10].Str)
	}
	// The consumer holds its own pending list.
	consumers := group[13].Array
	if len(consumers) != 1 {
		t.Fatalf("XINFO STREAM FULL: got %d consumers, want 1", len(consumers))
	}
	if len(consumers[0].Array) != 10 {
		t.Errorf("XINFO STREAM FULL: got %d elements in the consumer, want 10", len(consumers[0].Array))
	}
}

func TestXReadBlocking(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	done := make(chan resp.Value, 1)
	go func() {
		done <- c.cmd("XREAD", "BLOCK", "2000", "COUNT", "10", "STREAMS", "s", "0")
	}()
	// The client waits for the entry.
	time.Sleep(100 * time.Millisecond)
	w := newClient(t, addr)
	w.bulkWant("XADD", "s", "1-0", "f", "v1")
	select {
	case v := <-done:
		if v.Kind != resp.KindArray || len(v.Array) != 1 {
			t.Fatalf("the blocked XREAD: got %+v, want one stream", v)
		}
		entries := v.Array[0].Array[1].Array
		if len(entries) != 1 || string(entries[0].Array[0].Str) != "1-0" {
			t.Errorf("the blocked XREAD: got %+v, want the entry 1-0", entries)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the blocked XREAD did not wake up")
	}
}

func TestXReadBlockingTimeout(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	start := time.Now()
	v := c.cmd("XREAD", "BLOCK", "100", "COUNT", "10", "STREAMS", "s", "0")
	if !v.IsNull {
		t.Errorf("the timed out XREAD: got %+v, want null", v)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("the timed out XREAD took %s, want less than 2s", d)
	}
}
