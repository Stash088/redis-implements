package server_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestKeysGlob(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for _, k := range []string{"alpha", "beta", "user:1", "user:22", "plain"} {
		c.ok("SET", k, "v")
	}

	v := c.cmd("KEYS", "*")
	if v.Kind != resp.KindArray || len(v.Array) != 5 {
		t.Fatalf("KEYS *: got %+v", v)
	}
	got := map[string]bool{}
	for _, e := range v.Array {
		got[string(e.Str)] = true
	}
	for _, k := range []string{"alpha", "beta", "user:1", "user:22", "plain"} {
		if !got[k] {
			t.Errorf("KEYS *: missing %q", k)
		}
	}

	v = c.cmd("KEYS", "user:*")
	if v.Kind != resp.KindArray || len(v.Array) != 2 {
		t.Fatalf("KEYS user:*: got %+v", v)
	}
	v = c.cmd("KEYS", "[ab]*")
	if v.Kind != resp.KindArray || len(v.Array) != 2 {
		t.Fatalf("KEYS [ab]*: got %+v", v)
	}
	v = c.cmd("KEYS", "nomatch*")
	if v.Kind != resp.KindArray || len(v.Array) != 0 {
		t.Fatalf("KEYS nomatch*: got %+v", v)
	}
}

func TestScanFullCoverage(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	want := map[string]bool{}
	for i := 0; i < 25; i++ {
		k := "key" + string(rune('a'+i))
		c.ok("SET", k, "v")
		want[k] = true
	}

	seen := map[string]bool{}
	cursor := "0"
	rounds := 0
	for {
		v := c.cmd("SCAN", cursor, "COUNT", "10")
		if v.Kind != resp.KindArray || len(v.Array) != 2 {
			t.Fatalf("SCAN: got %+v", v)
		}
		if v.Array[0].Kind != resp.KindBulkString {
			t.Fatalf("SCAN cursor: got %+v", v.Array[0])
		}
		cursor = string(v.Array[0].Str)
		if v.Array[1].Kind != resp.KindArray {
			t.Fatalf("SCAN elements: got %+v", v.Array[1])
		}
		for _, e := range v.Array[1].Array {
			k := string(e.Str)
			if seen[k] {
				t.Errorf("duplicate key %q in SCAN results", k)
			}
			seen[k] = true
		}
		rounds++
		if cursor == "0" {
			break
		}
		if rounds > 10 {
			t.Fatal("SCAN did not terminate")
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("SCAN saw %d keys, want %d", len(seen), len(want))
	}
	for k := range want {
		if !seen[k] {
			t.Errorf("SCAN missed %q", k)
		}
	}
}

func TestScanMatch(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for _, k := range []string{"user:1", "user:2", "order:1"} {
		c.ok("SET", k, "v")
	}
	v := c.cmd("SCAN", "0", "COUNT", "100", "MATCH", "user:*")
	if v.Kind != resp.KindArray || len(v.Array) != 2 {
		t.Fatalf("SCAN: got %+v", v)
	}
	if len(v.Array[1].Array) != 2 {
		t.Fatalf("SCAN MATCH: got %d keys, want 2: %+v", len(v.Array[1].Array), v.Array[1])
	}
	// Cursor "0" over a full page means iteration is complete.
	if string(v.Array[0].Str) != "0" {
		t.Errorf("next cursor: got %q, want 0", v.Array[0].Str)
	}
}

func TestScanInvalidCursor(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	v := c.cmd("SCAN", "notanumber")
	if v.Kind != resp.KindError {
		t.Errorf("SCAN bad cursor: got %+v, want error", v)
	}
}

func TestConfigGet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	v := c.cmd("CONFIG", "GET", "*")
	if v.Kind != resp.KindArray || len(v.Array)%2 != 0 {
		t.Fatalf("CONFIG GET *: got %+v", v)
	}
	params := map[string]string{}
	for i := 0; i < len(v.Array); i += 2 {
		params[string(v.Array[i].Str)] = string(v.Array[i+1].Str)
	}
	for _, name := range []string{"maxmemory", "maxmemory-policy", "save", "appendonly", "dir", "port", "slowlog-max-len"} {
		if _, ok := params[name]; !ok {
			t.Errorf("CONFIG GET *: missing %q", name)
		}
	}
	if params["maxmemory-policy"] != "noeviction" {
		t.Errorf("maxmemory-policy: got %q, want noeviction", params["maxmemory-policy"])
	}

	v = c.cmd("CONFIG", "GET", "maxmemory-policy")
	if v.Kind != resp.KindArray || len(v.Array) != 2 ||
		string(v.Array[0].Str) != "maxmemory-policy" || string(v.Array[1].Str) != "noeviction" {
		t.Errorf("CONFIG GET exact: got %+v", v)
	}
}

func TestConfigSet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	c.ok("CONFIG", "SET", "maxmemory", "1kb")
	v := c.cmd("CONFIG", "GET", "maxmemory")
	if string(v.Array[1].Str) != "1024" {
		t.Errorf("maxmemory after set: got %q, want 1024", v.Array[1].Str)
	}

	c.ok("CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
	v = c.cmd("CONFIG", "GET", "maxmemory-policy")
	if string(v.Array[1].Str) != "allkeys-lru" {
		t.Errorf("policy after set: got %q", v.Array[1].Str)
	}

	// Bad policy is rejected.
	v = c.cmd("CONFIG", "SET", "maxmemory-policy", "bogus")
	if v.Kind != resp.KindError {
		t.Errorf("bad policy: got %+v, want error", v)
	}
	// Unknown parameter is rejected.
	v = c.cmd("CONFIG", "SET", "nosuchparam", "1")
	if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "Unsupported") {
		t.Errorf("unknown param: got %+v", v)
	}
}

func TestClientCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	v := c.cmd("CLIENT", "ID")
	if v.Kind != resp.KindInteger || v.Int < 1 {
		t.Errorf("CLIENT ID: got %+v", v)
	}

	c.ok("CLIENT", "SETNAME", "my-client")
	if got := c.bulkWant("CLIENT", "GETNAME"); string(got) != "my-client" {
		t.Errorf("CLIENT GETNAME: got %q", got)
	}
	// Names with spaces are rejected.
	if v := c.cmd("CLIENT", "SETNAME", "has space"); v.Kind != resp.KindError {
		t.Errorf("CLIENT SETNAME space: got %+v, want error", v)
	}

	c.ok("SET", "k", "v")
	v = c.cmd("CLIENT", "LIST")
	if v.Kind != resp.KindBulkString {
		t.Fatalf("CLIENT LIST: got %+v", v)
	}
	line := string(v.Str)
	for _, want := range []string{"id=", "addr=", "name=my-client", "cmd=client"} {
		if !strings.Contains(line, want) {
			t.Errorf("CLIENT LIST line missing %q: %q", want, line)
		}
	}
}

func TestSlowlog(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	// Threshold 0: every command is recorded, including SLOWLOG itself.
	c.ok("CONFIG", "SET", "slowlog-log-slower-than", "0")
	c.ok("SET", "slow", "value")
	_ = c.bulkWant("GET", "slow")

	v := c.cmd("SLOWLOG", "LEN")
	if v.Kind != resp.KindInteger || v.Int < 2 {
		t.Fatalf("SLOWLOG LEN: got %+v, want >= 2", v)
	}

	v = c.cmd("SLOWLOG", "GET", "1")
	if v.Kind != resp.KindArray || len(v.Array) != 1 {
		t.Fatalf("SLOWLOG GET 1: got %+v", v)
	}
	e := v.Array[0]
	if e.Kind != resp.KindArray || len(e.Array) != 6 {
		t.Fatalf("slowlog entry: got %+v, want 6 elements", e)
	}
	// Newest entry first; the last executed command before this GET was
	// SLOWLOG LEN.
	argv := e.Array[3]
	if argv.Kind != resp.KindArray || len(argv.Array) == 0 ||
		!strings.EqualFold(string(argv.Array[0].Str), "slowlog") {
		t.Errorf("newest entry argv: got %+v, want SLOWLOG ...", argv)
	}
	// Entry payload fields.
	if e.Array[4].Kind != resp.KindBulkString || !strings.Contains(string(e.Array[4].Str), "127.0.0.1") {
		t.Errorf("entry addr: got %+v", e.Array[4])
	}

	// Disable recording, then reset: neither RESET nor LEN record themselves.
	c.ok("CONFIG", "SET", "slowlog-log-slower-than", "-1")
	c.ok("SLOWLOG", "RESET")
	v = c.cmd("SLOWLOG", "LEN")
	if v.Int != 0 {
		t.Errorf("SLOWLOG LEN after reset: got %d, want 0", v.Int)
	}
}

func TestSortedKeysOutputStable(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	for _, k := range []string{"b", "a", "c"} {
		c.ok("SET", k, "v")
	}
	v := c.cmd("KEYS", "*")
	var got []string
	for _, e := range v.Array {
		got = append(got, string(e.Str))
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "a,b,c" {
		t.Errorf("KEYS: got %v", got)
	}
}
