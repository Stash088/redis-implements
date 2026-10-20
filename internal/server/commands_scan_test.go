package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
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
