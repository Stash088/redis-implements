package server_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// The interop tests check the replication against a real redis-server 8.2 in
// both directions: our replica syncs from a real master, and a real replica
// syncs from our master. They follow the pattern of the persistence interop
// tests and skip when redis-server is not installed.

func findRedisRepl(t *testing.T) (serverBin, cliBin string) {
	t.Helper()
	s, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not in PATH; skipping the replication interop test")
	}
	c, err := exec.LookPath("redis-cli")
	if err != nil {
		t.Skip("redis-cli not in PATH; skipping the replication interop test")
	}
	return s, c
}

func freePortRepl(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// startRealRedisRepl starts a real redis-server on the given port (or an
// empty one when the port is 0).
func startRealRedisRepl(t *testing.T, serverBin string, dir string, port int, replicaOf string) int {
	t.Helper()
	if port == 0 {
		port = freePortRepl(t)
	}
	args := []string{
		"--port", strconv.Itoa(port),
		"--bind", "127.0.0.1",
		"--dir", dir,
		"--save", "",
		"--appendonly", "no",
		"--daemonize", "yes",
	}
	if replicaOf != "" {
		fields := strings.Fields(replicaOf)
		if len(fields) == 1 {
			// "host:port" → "host port"
			idx := strings.LastIndex(fields[0], ":")
			fields = []string{fields[0][:idx], fields[0][idx+1:]}
		}
		args = append(args, "--replicaof")
		args = append(args, fields...)
	}
	if out, err := exec.Command(serverBin, args...).CombinedOutput(); err != nil {
		t.Skipf("cannot start redis-server: %v: %s", err, out)
	}
	t.Cleanup(func() {
		if cli, err := exec.LookPath("redis-cli"); err == nil {
			exec.Command(cli, "-p", strconv.Itoa(port), "shutdown", "nosave").Run() //nolint:errcheck
		}
	})
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

func redisCliRepl(t *testing.T, cliBin string, port int, args ...string) string {
	t.Helper()
	full := append([]string{"-p", strconv.Itoa(port)}, args...)
	out, err := exec.Command(cliBin, full...).CombinedOutput()
	if err != nil {
		t.Fatalf("redis-cli %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// startReplicaServerForInterop starts one of our servers as a replica of the
// given master port (the goroutine model: the tests of both models run in
// the package already).
func startReplicaServerForInterop(t *testing.T, dbs []*store.Store, masterPort int) string {
	t.Helper()
	srv, err := server.New("127.0.0.1:0", dbs)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.SetReplicaOf("127.0.0.1", masterPort, "")
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

// newInteropMaster starts one of our servers in the event loop model (the
// production default) for a real replica to attach to.
func newInteropMaster(t *testing.T, dbs []*store.Store) (*server.Server, error) {
	t.Helper()
	srv, err := server.NewLoop("127.0.0.1:0", dbs)
	if err != nil {
		return nil, err
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	return srv, nil
}

// waitReplBulkInterop waits until the command returns the expected bulk
// string.
func waitReplBulkInterop(t *testing.T, c *client, cmd string, key string, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v := c.cmd(cmd, key)
		if v.Kind == resp.KindBulkString && !v.IsNull && string(v.Str) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	v := c.cmd(cmd, key)
	t.Fatalf("%s %s still returns %+v, want %q", cmd, key, v, want)
}

// waitReplAbsentInterop waits until the key disappears.
func waitReplAbsentInterop(t *testing.T, c *client, key string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v := c.cmd("EXISTS", key); v.Kind == resp.KindInteger && v.Int == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the key %q still exists", key)
}

// TestInteropOurReplicaSyncsFromRealMaster makes our server a replica of a
// real redis master: the full sync (the RDB of the real master) and the
// command stream that follows must land in our stores.
func TestInteropOurReplicaSyncsFromRealMaster(t *testing.T) {
	serverBin, cliBin := findRedisRepl(t)
	dir := t.TempDir()
	mport := startRealRedisRepl(t, serverBin, dir, 0, "")

	// A dataset of every type on the real master, with a TTL to check the
	// PXAT propagation of the real master.
	redisCliRepl(t, cliBin, mport, "SET", "str", "hello")
	redisCliRepl(t, cliBin, mport, "SET", "ex", "val", "EX", "3000")
	redisCliRepl(t, cliBin, mport, "RPUSH", "list", "a", "b", "c")
	redisCliRepl(t, cliBin, mport, "HSET", "hash", "f1", "v1", "f2", "v2")
	redisCliRepl(t, cliBin, mport, "SADD", "set", "m1", "m2")
	redisCliRepl(t, cliBin, mport, "ZADD", "zset", "1.5", "one", "2.5", "two")
	redisCliRepl(t, cliBin, mport, "XADD", "stream", "1-1", "color", "red")

	// Our server as a replica of the real master.
	dbs := make([]*store.Store, 16)
	for i := range dbs {
		dbs[i] = store.New()
	}
	srv := startReplicaServerForInterop(t, dbs, mport)
	c := newClient(t, srv)

	waitReplBulkInterop(t, c, "GET", "str", "hello")

	if got := string(c.cmd("TYPE", "stream").Str); got != "stream" {
		t.Errorf("TYPE stream = %q, want stream", got)
	}
	if got := c.cmd("LRANGE", "list", "0", "-1"); len(got.Array) != 3 {
		t.Errorf("LRANGE list = %+v", got)
	}
	if got := c.cmd("HGETALL", "hash"); len(got.Array) != 4 {
		t.Errorf("HGETALL hash = %+v", got)
	}
	if got := c.cmd("SMEMBERS", "set"); len(got.Array) != 2 {
		t.Errorf("SMEMBERS set = %+v", got)
	}
	if got := c.cmd("ZRANGE", "zset", "0", "-1", "WITHSCORES"); len(got.Array) != 4 {
		t.Errorf("ZRANGE zset = %+v", got)
	}
	if got := c.cmd("XRANGE", "stream", "-", "+"); len(got.Array) != 1 {
		t.Errorf("XRANGE stream = %+v", got)
	}
	// The TTL survived the sync (the master propagated SET ... PXAT).
	if v := c.cmd("TTL", "ex"); v.Kind != resp.KindInteger || v.Int <= 0 {
		t.Errorf("TTL ex = %+v, want positive", v)
	}

	// The live stream: a write on the real master reaches us.
	redisCliRepl(t, cliBin, mport, "SET", "live", "streamed")
	waitReplBulkInterop(t, c, "GET", "live", "streamed")
	redisCliRepl(t, cliBin, mport, "DEL", "str")
	waitReplAbsentInterop(t, c, "str")
}

// TestInteropRealReplicaSyncsFromUs makes a real redis replica of our
// master: the replica reads our FULLRESYNC reply, the RDB and the command
// stream.
func TestInteropRealReplicaSyncsFromUs(t *testing.T) {
	serverBin, cliBin := findRedisRepl(t)
	dir := t.TempDir()

	dbs := make([]*store.Store, 4)
	for i := range dbs {
		dbs[i] = store.New()
	}
	exp := store.NewExpirer(dbs, 10*time.Millisecond)
	exp.Start()
	master, err := newInteropMaster(t, dbs)
	if err != nil {
		t.Fatalf("master: %v", err)
	}
	master.Expirer = exp
	go master.Serve()
	t.Cleanup(func() { master.Close() })
	mAddr := master.Addr().String()
	mport := atoiSafe(mAddr[strings.LastIndex(mAddr, ":")+1:])

	mc := newClient(t, mAddr)
	mc.cmd("SET", "str", "hello")
	mc.cmd("RPUSH", "list", "a", "b", "c")
	mc.cmd("HSET", "hash", "f1", "v1")
	mc.cmd("ZADD", "zset", "1.5", "one")
	mc.cmd("XADD", "stream", "1-1", "color", "red")

	rport := startRealRedisRepl(t, serverBin, dir, 0, fmt.Sprintf("127.0.0.1:%d", mport))

	// The replica syncs: its INFO reports the link up and the dataset
	// arrives.
	deadline := time.Now().Add(15 * time.Second)
	up := false
	for time.Now().Before(deadline) {
		info := redisCliRepl(t, cliBin, rport, "INFO", "replication")
		if strings.Contains(info, "master_link_status:up") {
			up = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !up {
		t.Fatalf("the real replica did not sync with our master in time:\n%s",
			redisCliRepl(t, cliBin, rport, "INFO", "replication"))
	}
	if got := redisCliRepl(t, cliBin, rport, "GET", "str"); got != "hello" {
		t.Errorf("the replica holds GET str = %q, want hello", got)
	}
	if got := redisCliRepl(t, cliBin, rport, "TYPE", "stream"); got != "stream" {
		t.Errorf("the replica holds TYPE stream = %q", got)
	}

	// The live stream: a write on our master reaches the real replica.
	mc.cmd("SET", "live", "streamed")
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := redisCliRepl(t, cliBin, rport, "GET", "live"); got == "streamed" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the live write did not reach the real replica: GET live = %q",
		redisCliRepl(t, cliBin, rport, "GET", "live"))
}
