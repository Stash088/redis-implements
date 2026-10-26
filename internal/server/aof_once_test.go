package server_test

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// aofCommands reads the commands written to an AOF file and returns their
// names in the order they were written.
func aofCommands(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the aof: %v", err)
	}
	r := resp.NewReaderSize(strings.NewReader(string(data)), 64*1024)
	var names []string
	for {
		v, err := r.ReadValue()
		if err != nil {
			break // the end of the file
		}
		if v.Kind != resp.KindArray || len(v.Array) == 0 {
			t.Fatalf("the aof holds a value that is not a command: %+v", v)
		}
		names = append(names, strings.ToLower(string(v.Array[0].Str)))
	}
	return names
}

// startAOFServer starts a server with an AOF attached to a fresh file and
// returns its address, the path of the journal and the appender (so the test
// can write the buffer out before reading the file).
func startAOFServer(t *testing.T, dir string) (string, string, *persist.Appender) {
	t.Helper()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	aofPath := filepath.Join(dir, "appendonly.aof")
	a, err := persist.OpenAppender(aofPath, persist.FsyncNo)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	srv.AOF = a
	srv.AOFPath = aofPath
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String(), aofPath, a
}

// aofCase is one write command and the commands it must leave in the journal.
type aofCase struct {
	args []string
	want []string
}

// TestAOFWritesEveryCommandOnce checks that a write command lands in the
// journal exactly once. A command written twice would be applied twice on
// the replay, and the journal would grow twice as fast.
func TestAOFWritesEveryCommandOnce(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)

	cases := []aofCase{
		{[]string{"SET", "k", "v"}, []string{"set"}},
		{[]string{"SETNX", "k2", "v"}, []string{"setnx"}},
		{[]string{"SETEX", "k3", "100", "v"}, []string{"set", "pexpireat"}},
		{[]string{"PSETEX", "k4", "100000", "v"}, []string{"set", "pexpireat"}},
		{[]string{"APPEND", "k5", "v"}, []string{"append"}},
		{[]string{"INCR", "counter"}, []string{"incr"}},
		{[]string{"INCRBY", "counter", "2"}, []string{"incrby"}},
		{[]string{"DECR", "counter"}, []string{"decr"}},
		{[]string{"DECRBY", "counter", "2"}, []string{"decrby"}},
		{[]string{"GETSET", "k", "v2"}, []string{"getset"}},
		{[]string{"GETDEL", "k"}, []string{"getdel"}},
		{[]string{"MSET", "m1", "v", "m2", "v"}, []string{"mset"}},
		{[]string{"MSETNX", "m3", "v"}, []string{"msetnx"}},
		{[]string{"HSET", "h", "f", "v"}, []string{"hset"}},
		{[]string{"HMSET", "h", "f2", "v2"}, []string{"hmset"}},
		{[]string{"HSETNX", "h", "f3", "v3"}, []string{"hsetnx"}},
		{[]string{"HINCRBY", "h", "f4", "1"}, []string{"hincrby"}},
		{[]string{"HINCRBYFLOAT", "h", "f5", "1.5"}, []string{"hincrbyfloat"}},
		{[]string{"HDEL", "h", "f"}, []string{"hdel"}},
		{[]string{"HGETDEL", "h", "f2"}, []string{"hgetdel"}},
		{[]string{"LPUSH", "l", "a"}, []string{"lpush"}},
		{[]string{"RPUSH", "l", "b"}, []string{"rpush"}},
		{[]string{"LPOP", "l"}, []string{"lpop"}},
		{[]string{"RPOP", "l"}, []string{"rpop"}},
		{[]string{"LPUSH", "l", "c"}, []string{"lpush"}},
		{[]string{"LSET", "l", "0", "c"}, []string{"lset"}},
		{[]string{"LINSERT", "l", "BEFORE", "c", "x"}, []string{"linsert"}},
		{[]string{"LREM", "l", "x"}, []string{"lrem"}},
		{[]string{"LTRIM", "l", "0", "0"}, []string{"ltrim"}},
		{[]string{"SADD", "s", "m"}, []string{"sadd"}},
		{[]string{"SREM", "s", "m"}, []string{"srem"}},
		{[]string{"SPOP", "s"}, []string{"spop"}},
		{[]string{"ZADD", "z", "1", "m"}, []string{"zadd"}},
		{[]string{"ZINCRBY", "z", "1", "m"}, []string{"zincrby"}},
		{[]string{"ZREM", "z", "m"}, []string{"zrem"}},
		{[]string{"ZPOPMIN", "z"}, []string{"zpopmin"}},
		{[]string{"ZPOPMAX", "z"}, []string{"zpopmax"}},
		{[]string{"ZREMRANGEBYRANK", "z", "0", "0"}, []string{"zremrangebyrank"}},
		{[]string{"ZREMRANGEBYSCORE", "z", "-inf", "+inf"}, []string{"zremrangebyscore"}},
		{[]string{"ZREMRANGEBYLEX", "z", "-", "+"}, []string{"zremrangebylex"}},
		{[]string{"DEL", "k2"}, []string{"del"}},
		{[]string{"RENAME", "k3", "k3b"}, []string{"rename"}},
		{[]string{"RENAMENX", "k4", "k4b"}, []string{"renamenx"}},
		{[]string{"EXPIRE", "k5", "100"}, []string{"pexpireat"}},
		{[]string{"PEXPIRE", "k5", "100000"}, []string{"pexpireat"}},
		{[]string{"PERSIST", "k5"}, []string{"persist"}},
		{[]string{"EXPIREAT", "k5", "9999999999"}, []string{"expireat"}},
		{[]string{"PEXPIREAT", "k5", "9999999999000"}, []string{"pexpireat"}},
		{[]string{"XADD", "stream", "1-0", "f", "v"}, []string{"xadd"}},
		{[]string{"XGROUP", "stream", "CREATE", "g", "0"}, []string{"xgroup"}},
		{[]string{"XACK", "stream", "g", "1-0"}, []string{"xack"}},
		{[]string{"XDEL", "stream", "1-0"}, []string{"xdel"}},
		{[]string{"XSETID", "stream", "5-0"}, []string{"xsetid"}},
		{[]string{"XTRIM", "stream", "MAXLEN", "0"}, []string{"xtrim"}},
		{[]string{"ZUNIONSTORE", "zdst", "2", "z", "z"}, []string{"zunionstore"}},
		{[]string{"ZINTERSTORE", "zdst2", "2", "z", "z"}, []string{"zinterstore"}},
		{[]string{"ZRANGESTORE", "zdst3", "z", "0", "-1"}, []string{"zrangestore"}},
		{[]string{"UNLINK", "m2"}, []string{"unlink"}},
	}

	written := 0
	for _, tc := range cases {
		before := len(aofCommands(t, aofPath))
		c.cmd(tc.args...)
		if err := a.Flush(); err != nil {
			t.Fatalf("flush the aof: %v", err)
		}
		names := aofCommands(t, aofPath)
		got := names[before:]
		if len(got) != len(tc.want) {
			t.Errorf("%v: the aof holds %v, want %v", tc.args, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%v: the aof holds %v, want %v", tc.args, got, tc.want)
				break
			}
		}
		written += len(got)
	}
	if written == 0 {
		t.Fatalf("the journal is empty")
	}
}

// TestAOFGetExKeepsTheKey checks that GETEX without an expiry option (which
// removes the TTL) writes PERSIST to the journal: a PEXPIREAT with an empty
// instant would expire the key at once on the replay.
func TestAOFGetExKeepsTheKey(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	c.bulkWant("GETEX", "k")
	if err := a.Flush(); err != nil {
		t.Fatalf("flush the aof: %v", err)
	}

	names := aofCommands(t, aofPath)
	last := names[len(names)-1]
	if last != "persist" {
		t.Fatalf("the aof ends with %q, want persist", last)
	}
	// The key must survive the reload.
	addr2 := startFromFiles(t, aofPath, true)
	c2 := newClient(t, addr2)
	if v := c2.bulkWant("GET", "k"); string(v) != "v" {
		t.Fatalf("after the reload: got %q, want v", v)
	}
}

// TestAOFRenameWrittenOnce is a small guard for the commands whose handler
// writes to the journal itself: the dispatcher must not write them again.
func TestAOFRenameWrittenOnce(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	c.ok("RENAME", "k", "k2")
	if err := a.Flush(); err != nil {
		t.Fatalf("flush the aof: %v", err)
	}

	names := aofCommands(t, aofPath)
	renames := 0
	for _, n := range names {
		if n == "rename" {
			renames++
		}
	}
	if renames != 1 {
		t.Fatalf("the aof holds %d renames, want 1: %v", renames, names)
	}
	// The file must be where the server expects it.
	if _, err := os.Stat(filepath.Clean(aofPath)); err != nil {
		t.Fatalf("the aof is missing: %v", err)
	}
}

// TestAOFFlushesWithoutAWrite makes sure the journal is written out when the
// server is closed, even if the buffer was not full.
func TestAOFFlushesWithoutAWrite(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	time.Sleep(20 * time.Millisecond)
	if err := a.Flush(); err != nil {
		t.Fatalf("flush the aof: %v", err)
	}
	if names := aofCommands(t, aofPath); len(names) != 1 {
		t.Fatalf("the aof holds %v, want one command", names)
	}
}
