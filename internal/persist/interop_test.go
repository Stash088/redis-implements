package persist

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

func findRedis(t *testing.T) (serverBin, cliBin string) {
	t.Helper()
	s, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not in PATH; skipping real-Redis interop test")
	}
	c, err := exec.LookPath("redis-cli")
	if err != nil {
		t.Skip("redis-cli not in PATH; skipping real-Redis interop test")
	}
	return s, c
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func startRealRedis(t *testing.T, serverBin string, dir string) (port int) {
	t.Helper()
	port = freePort(t)
	cmd := exec.Command(serverBin,
		"--port", strconv.Itoa(port),
		"--bind", "127.0.0.1",
		"--dir", dir,
		"--dbfilename", "dump.rdb",
		"--save", "",
		"--appendonly", "no",
		"--daemonize", "yes",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot start redis-server: %v: %s", err, out)
	}
	// Best-effort shutdown on cleanup.
	t.Cleanup(func() {
		if cli, err := exec.LookPath("redis-cli"); err == nil {
			exec.Command(cli, "-p", strconv.Itoa(port), "shutdown", "nosave").Run()
		}
	})
	// Wait until it accepts connections.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			c.Close()
			return port
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Skip("redis-server did not become ready")
	return 0
}

func redisCli(t *testing.T, cliBin string, port int, args ...string) string {
	t.Helper()
	full := append([]string{"-p", strconv.Itoa(port)}, args...)
	out, err := exec.Command(cliBin, full...).CombinedOutput()
	if err != nil {
		t.Fatalf("redis-cli %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestInteropRealRedisLoadsOurDump writes an RDB with our writer and verifies
// that a real redis-server can load it and serve the values.
func TestInteropRealRedisLoadsOurDump(t *testing.T) {
	serverBin, cliBin := findRedis(t)
	dir := t.TempDir()

	st := store.New()
	st.SetString([]byte("str"), []byte("hello"))
	st.SetString([]byte("neg"), []byte("-42"))
	st.SetString([]byte("big"), []byte(strings.Repeat("x", 70000)))
	st.SetStringWithTTL([]byte("ttl"), []byte("v"), 10*time.Minute)
	st.ListPush([]byte("list"), false, []byte("a"), []byte("b"), []byte("c"))
	st.HashSet([]byte("hash"), []byte("f1"), []byte("v1"))
	st.HashSet([]byte("hash"), []byte("f2"), []byte("v2"))
	st.SetAdd([]byte("set"), []byte("m1"), []byte("m2"))
	st.ZAdd([]byte("zset"), 1.5, []byte("a"))
	st.ZAdd([]byte("zset"), 2.5, []byte("b"))

	path := filepath.Join(dir, "dump.rdb")
	if err := Save(path, []*store.Store{st}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	port := startRealRedis(t, serverBin, dir)

	if got := redisCli(t, cliBin, port, "GET", "str"); got != "hello" {
		t.Errorf("GET str = %q", got)
	}
	if got := redisCli(t, cliBin, port, "GET", "neg"); got != "-42" {
		t.Errorf("GET neg = %q", got)
	}
	if got := redisCli(t, cliBin, port, "STRLEN", "big"); got != "70000" {
		t.Errorf("STRLEN big = %q", got)
	}
	if got := redisCli(t, cliBin, port, "TTL", "ttl"); got == "-2" || got == "-1" {
		t.Errorf("TTL ttl = %q", got)
	}
	if got := redisCli(t, cliBin, port, "LRANGE", "list", "0", "-1"); got != "a\nb\nc" {
		t.Errorf("LRANGE = %q", got)
	}
	if got := redisCli(t, cliBin, port, "HGETALL", "hash"); got != "f1\nv1\nf2\nv2" && got != "f2\nv2\nf1\nv1" {
		t.Errorf("HGETALL = %q", got)
	}
	if got := redisCli(t, cliBin, port, "SMEMBERS", "set"); got != "m1\nm2" && got != "m2\nm1" {
		t.Errorf("SMEMBERS = %q", got)
	}
	if got := redisCli(t, cliBin, port, "ZRANGE", "zset", "0", "-1", "WITHSCORES"); got != "a\n1.5\nb\n2.5" {
		t.Errorf("ZRANGE = %q", got)
	}
}

// TestInteropWeLoadRealRedisDump has a real redis-server SAVE its dataset and
// verifies our loader restores it (covering listpack, int and LZF encodings).
func TestInteropWeLoadRealRedisDump(t *testing.T) {
	serverBin, cliBin := findRedis(t)
	dir := t.TempDir()

	port := startRealRedis(t, serverBin, dir)
	redisCli(t, cliBin, port, "SET", "str", "hello")
	redisCli(t, cliBin, port, "SET", "neg", "-42")
	redisCli(t, cliBin, port, "SET", "big", strings.Repeat("y", 50000))
	redisCli(t, cliBin, port, "RPUSH", "list", "a", "b", "c")
	redisCli(t, cliBin, port, "HSET", "hash", "f1", "v1")
	redisCli(t, cliBin, port, "HSET", "hash", "f2", "v2")
	redisCli(t, cliBin, port, "SADD", "set", "m1", "m2")
	redisCli(t, cliBin, port, "ZADD", "zset", "1.5", "a", "2.5", "b")
	redisCli(t, cliBin, port, "SET", "ex", "v", "EX", "100")
	redisCli(t, cliBin, port, "SAVE")

	f, err := os.Open(filepath.Join(dir, "dump.rdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st := store.New()
	if err := Load(f, []*store.Store{st}); err != nil {
		t.Fatalf("Load real redis dump: %v", err)
	}

	if v, ok, _ := st.GetString([]byte("str")); !ok || string(v) != "hello" {
		t.Errorf("str = %q ok=%v", v, ok)
	}
	if v, ok, _ := st.GetString([]byte("neg")); !ok || string(v) != "-42" {
		t.Errorf("neg = %q ok=%v", v, ok)
	}
	if v, ok, _ := st.GetString([]byte("big")); !ok || len(v) != 50000 {
		t.Errorf("big len = %d ok=%v", len(v), ok)
	}
	if lr, _ := st.ListRange([]byte("list"), 0, -1); len(lr) != 3 {
		t.Errorf("list = %d elems", len(lr))
	}
	if v, _, _ := st.HashGet([]byte("hash"), []byte("f2")); string(v) != "v2" {
		t.Errorf("hash f2 = %q", v)
	}
	if m, _ := st.SetMembers([]byte("set")); len(m) != 2 {
		t.Errorf("set = %d members", len(m))
	}
	if z, _ := st.ZRange([]byte("zset"), 0, -1, false); len(z) != 2 || z[1].Score != 2.5 {
		t.Errorf("zset = %+v", z)
	}
	if _, ok, _ := st.GetString([]byte("ex")); !ok {
		t.Errorf("ex missing")
	}
}
