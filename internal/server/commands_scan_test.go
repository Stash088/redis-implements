package server_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// scanAll walks a collection with a SCAN-like command and returns the
// elements it produced.
func scanAll(t *testing.T, c *client, cmd string, args ...string) []string {
	t.Helper()
	var out []string
	cursor := "0"
	for i := 0; i < 1000; i++ {
		full := append([]string{cmd, args[0], cursor}, args[1:]...)
		v := c.arrWant(full...)
		if len(v) != 2 {
			t.Fatalf("%s: got %d elements, want 2", cmd, len(v))
		}
		next := string(v[0].Str)
		for _, item := range v[1].Array {
			out = append(out, string(item.Str))
		}
		if next == "0" {
			return out
		}
		cursor = next
	}
	t.Fatalf("%s: the iteration did not finish", cmd)
	return nil
}

func TestHScan(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for _, f := range []string{"f1", "f2", "f3"} {
		c.intWant("HSET", "h", f, "v"+f)
	}
	// A full iteration returns every field and value exactly once.
	items := scanAll(t, c, "HSCAN", "h")
	if len(items) != 6 {
		t.Fatalf("HSCAN: got %d items, want 6: %v", len(items), items)
	}
	seen := map[string]bool{}
	for i := 0; i < len(items); i += 2 {
		if seen[items[i]] {
			t.Errorf("HSCAN: duplicate field %q", items[i])
		}
		seen[items[i]] = true
		if items[i+1] != "v"+items[i] {
			t.Errorf("HSCAN: field %q has the value %q", items[i], items[i+1])
		}
	}

	// NOVALUES returns the fields only.
	arr := c.arrWant("HSCAN", "h", "0", "NOVALUES")
	if len(arr) != 2 || len(arr[1].Array) != 3 {
		t.Errorf("HSCAN NOVALUES: got %+v", arr)
	}

	// MATCH filters the fields.
	arr = c.arrWant("HSCAN", "h", "0", "MATCH", "f[12]")
	if len(arr) != 2 || len(arr[1].Array) != 4 {
		t.Errorf("HSCAN MATCH: got %+v", arr)
	}

	// A missing key yields an empty result.
	arr = c.arrWant("HSCAN", "nosuch", "0")
	if len(arr) != 2 || len(arr[1].Array) != 0 {
		t.Errorf("HSCAN of a missing key: got %+v", arr)
	}
}

func TestSScan(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("SADD", "s", "m1", "m2", "m3")
	items := scanAll(t, c, "SSCAN", "s")
	if len(items) != 3 {
		t.Fatalf("SSCAN: got %d items, want 3: %v", len(items), items)
	}
	seen := map[string]bool{}
	for _, m := range items {
		if seen[m] {
			t.Errorf("SSCAN: duplicate member %q", m)
		}
		seen[m] = true
	}
	// MATCH filters the members.
	arr := c.arrWant("SSCAN", "s", "0", "MATCH", "m[12]")
	if len(arr) != 2 || len(arr[1].Array) != 2 {
		t.Errorf("SSCAN MATCH: got %+v", arr)
	}
}

func TestZScan(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c")
	items := scanAll(t, c, "ZSCAN", "z")
	if len(items) != 6 {
		t.Fatalf("ZSCAN: got %d items, want 6: %v", len(items), items)
	}
	// The members come in score order, each with its score.
	if items[0] != "a" || items[1] != "1" || items[2] != "b" || items[3] != "2" {
		t.Errorf("ZSCAN: got %v, want the members in score order", items)
	}
	// NOVALUES is not supported by ZSCAN, like in Redis.
	if v := c.cmd("ZSCAN", "z", "0", "NOVALUES"); v.Kind != resp.KindError {
		t.Errorf("ZSCAN NOVALUES: got %+v, want an error", v)
	}
}

// TestScanWrongType checks that the collection scans report a wrong type for
// keys of another type.
func TestScanWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "str", "v")
	for _, cmd := range []string{"HSCAN", "SSCAN", "ZSCAN"} {
		if v := c.cmd(cmd, "str", "0"); v.Kind != resp.KindError {
			t.Errorf("%s on a string: got %+v, want an error", cmd, v)
		}
	}
}

// TestScanCount checks that COUNT splits the iteration into batches.
func TestScanCount(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for _, f := range []string{"f1", "f2", "f3", "f4", "f5"} {
		c.intWant("HSET", "h", f, "v")
	}
	// The first batch holds COUNT fields.
	arr := c.arrWant("HSCAN", "h", "0", "COUNT", "2")
	if len(arr) != 2 || len(arr[1].Array) != 4 {
		t.Fatalf("HSCAN COUNT 2: got %+v, want 2 fields", arr)
	}
	if string(arr[0].Str) == "0" {
		t.Error("HSCAN COUNT 2: the cursor must not be 0 while the scan is unfinished")
	}
	// The rest comes with the next call.
	arr = c.arrWant("HSCAN", "h", string(arr[0].Str), "COUNT", "2")
	if len(arr) != 2 || len(arr[1].Array) != 4 {
		t.Errorf("the second HSCAN batch: got %+v", arr)
	}
}

// TestScanTraversalSharded walks a keyspace spread over all shards and
// checks that every key comes back exactly once, that the traversal
// terminates and that keys arrive shard by shard (sorted within a shard).
func TestScanTraversalSharded(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	const n = 600
	for i := 0; i < n; i++ {
		c.ok("SET", fmt.Sprintf("scan:key:%04d", i), "v")
	}

	seen := make(map[string]int)
	var order []string
	cursor := "0"
	steps := 0
	for {
		arr := c.arrWant("SCAN", cursor, "COUNT", "50")
		cursor = string(arr[0].Str)
		for _, item := range arr[1].Array {
			k := string(item.Str)
			seen[k]++
			order = append(order, k)
		}
		steps++
		if cursor == "0" {
			break
		}
		if steps > n+50 {
			t.Fatal("SCAN does not terminate")
		}
	}
	if len(seen) != n {
		t.Fatalf("traversal returned %d distinct keys, want %d", len(seen), n)
	}
	for k, count := range seen {
		if count != 1 {
			t.Fatalf("key %q returned %d times", k, count)
		}
	}
	// Keys within one shard are sorted; shard boundaries may interrupt
	// the global order, so the traversal may "reset" at most once per
	// shard boundary.
	descents := 0
	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			descents++
		}
	}
	if descents >= store.ShardCount {
		t.Fatalf("keys within shards are not sorted: %d descents over %d keys", descents, len(order))
	}
}

// TestScanCursorBounds checks that out-of-range cursors finish the scan
// gracefully instead of looping or erroring.
func TestScanCursorBounds(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for i := 0; i < 10; i++ {
		c.ok("SET", fmt.Sprintf("k%d", i), "v")
	}
	// A shard index beyond the shard count: no keys, cursor 0.
	arr := c.arrWant("SCAN", "18446744073709551615") // shard 255
	if string(arr[0].Str) != "0" || len(arr[1].Array) != 0 {
		t.Fatalf("out-of-range cursor: got %+v, want cursor 0 and no keys", arr)
	}
	// A huge offset inside a valid shard: the shard counts as exhausted
	// and the scan continues with the next one (no error, no loop).
	arr = c.arrWant("SCAN", "72057594037927935") // shard 0, offset 2^56-1
	keys := map[string]bool{}
	cursor := string(arr[0].Str)
	for _, item := range arr[1].Array {
		keys[string(item.Str)] = true
	}
	for steps := 0; cursor != "0"; steps++ {
		if steps > 20 {
			t.Fatal("huge-offset scan does not terminate")
		}
		arr = c.arrWant("SCAN", cursor)
		cursor = string(arr[0].Str)
		for _, item := range arr[1].Array {
			k := string(item.Str)
			if keys[k] {
				t.Fatalf("key %q returned twice after a huge offset", k)
			}
			keys[k] = true
		}
	}
	// Shard 0 was skipped, so not every key has to come back, but only
	// keys of this keyspace may appear.
	for k := range keys {
		if len(k) < 2 || k[:1] != "k" {
			t.Fatalf("stray key %q in the huge-offset traversal", k)
		}
	}
}

// TestScanTraversalMatch walks the keyspace with MATCH and checks the filter.
func TestScanTraversalMatch(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for i := 0; i < 100; i++ {
		c.ok("SET", fmt.Sprintf("match:key:%d", i), "v")
		c.ok("SET", fmt.Sprintf("other:%d", i), "v")
	}
	seen := make(map[string]int)
	cursor := "0"
	for {
		arr := c.arrWant("SCAN", cursor, "COUNT", "20", "MATCH", "match:*")
		cursor = string(arr[0].Str)
		for _, item := range arr[1].Array {
			seen[string(item.Str)]++
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 100 {
		t.Fatalf("MATCH returned %d keys, want 100", len(seen))
	}
	for k := range seen {
		if !strings.HasPrefix(k, "match:") {
			t.Fatalf("MATCH let %q through", k)
		}
	}
}

// TestHScanFollowsChanges checks that a change of the hash between two calls
// of a traversal is visible: the snapshot the traversal walks is built again
// when the collection changes.
func TestHScanFollowsChanges(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("HSET", "h", "f1", "v1")
	c.intWant("HSET", "h", "f2", "v2")

	// The first page builds the snapshot of the hash.
	arr := c.arrWant("HSCAN", "h", "0", "COUNT", "1")
	if len(arr) != 2 || len(arr[1].Array) != 2 {
		t.Fatalf("HSCAN: got %+v", arr)
	}
	if string(arr[1].Array[0].Str) != "f1" {
		t.Fatalf("HSCAN: got the field %q, want f1", arr[1].Array[0].Str)
	}
	cursor := string(arr[0].Str)
	if cursor == "0" {
		t.Fatalf("HSCAN: the traversal ended too early")
	}

	// A new field invalidates the snapshot: the rest of the traversal shows
	// it, and every field is returned exactly once.
	c.intWant("HSET", "h", "f3", "v3")
	arr = c.arrWant("HSCAN", "h", cursor, "COUNT", "10")
	if len(arr) != 2 {
		t.Fatalf("HSCAN: got %+v", arr)
	}
	got := map[string]bool{}
	for i := 0; i < len(arr[1].Array); i += 2 {
		got[string(arr[1].Array[i].Str)] = true
	}
	if !got["f2"] || !got["f3"] {
		t.Fatalf("HSCAN: got the fields %v, want f2 and f3", got)
	}
	if string(arr[0].Str) != "0" {
		t.Fatalf("HSCAN: got the cursor %q, want 0", arr[0].Str)
	}
}
