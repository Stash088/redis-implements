package server_test

import (
	"bufio"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// replPair is a master and its replica, both served by the same process.
type replPair struct {
	master  *server.Server
	replica *server.Server
	mAddr   string
	rAddr   string
	mc      *client
	rc      *client
}

// startReplPair starts a master and a replica of it. The replica starts
// syncing right away, like a server started with --replicaof does. The
// loopMode flag picks the I/O model of both servers.
func startReplPair(t *testing.T, loopMode bool) *replPair {
	t.Helper()
	dbs := make([]*store.Store, 4)
	for i := range dbs {
		dbs[i] = store.New()
	}
	exp := store.NewExpirer(dbs, 10*time.Millisecond)
	exp.Start()

	var master, replica *server.Server
	var err error
	if loopMode {
		master, err = server.NewLoop("127.0.0.1:0", dbs)
	} else {
		master, err = server.New("127.0.0.1:0", dbs)
	}
	if err != nil {
		t.Fatalf("master: %v", err)
	}
	master.SetLogger(log.New(io.Discard, "", 0))
	master.Expirer = exp

	rdbs := make([]*store.Store, 4)
	for i := range rdbs {
		rdbs[i] = store.New()
	}
	rexp := store.NewExpirer(rdbs, 10*time.Millisecond)
	rexp.Start()
	if loopMode {
		replica, err = server.NewLoop("127.0.0.1:0", rdbs)
	} else {
		replica, err = server.New("127.0.0.1:0", rdbs)
	}
	if err != nil {
		t.Fatalf("replica: %v", err)
	}
	replica.SetLogger(log.New(io.Discard, "", 0))
	replica.Expirer = rexp

	p := &replPair{master: master, replica: replica, mAddr: master.Addr().String(), rAddr: replica.Addr().String()}
	t.Cleanup(func() {
		replica.Close()
		master.Close()
	})

	replica.SetReplicaOf("127.0.0.1", atoiSafe(p.mAddr[strings.LastIndex(p.mAddr, ":")+1:]), "")

	go master.Serve()
	go replica.Serve()

	p.mc = newClient(t, p.mAddr)
	p.rc = newClient(t, p.rAddr)
	return p
}

func atoiSafe(s string) int {
	n := 0
	for i := 0; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

// waitLinkUp waits until the replica reports its master link as up.
func (p *replPair) waitLinkUp(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		info := string(p.rc.bulkWant("INFO", "replication"))
		if strings.Contains(info, "master_link_status:up") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the replica did not sync with the master in time")
}

// waitReplicaBulk waits until the replica serves the expected string value.
func (p *replPair) waitReplicaBulk(t *testing.T, cmd []string, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v := p.rc.cmd(cmd...)
		if v.Kind == resp.KindBulkString && !v.IsNull && string(v.Str) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	v := p.rc.cmd(cmd...)
	t.Fatalf("%v on the replica still returns %+v, want %q", cmd, v, want)
}

// waitReplicaInt waits until the replica serves the expected integer value.
func (p *replPair) waitReplicaInt(t *testing.T, cmd []string, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v := p.rc.cmd(cmd...)
		if v.Kind == resp.KindInteger && v.Int == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	v := p.rc.cmd(cmd...)
	t.Fatalf("%v on the replica still returns %+v, want %d", cmd, v, want)
}

// seed writes a dataset of every type on the master. The replies are not
// checked here: assertSeed verifies the dataset on the replica.
func (p *replPair) seed(t *testing.T) {
	t.Helper()
	mc := p.mc
	mc.cmd("SET", "str", "hello")
	mc.cmd("APPEND", "str", "-world")
	mc.cmd("SET", "ttl", "gone", "PX", "600000")
	mc.cmd("RPUSH", "list", "a", "b", "c")
	mc.cmd("HSET", "hash", "f1", "v1", "f2", "v2")
	mc.cmd("SADD", "set", "m1", "m2")
	mc.cmd("ZADD", "zset", "1.5", "one", "2.5", "two")
	mc.cmd("XADD", "stream", "1-1", "color", "red")
	mc.cmd("SELECT", "2")
	mc.cmd("SET", "db2", "yes")
	mc.cmd("SELECT", "0")
}

// assertSeed verifies the seeded dataset on the replica.
func (p *replPair) assertSeed(t *testing.T) {
	t.Helper()
	rc := p.rc
	if got := string(rc.bulkWant("GET", "str")); got != "hello-world" {
		t.Errorf("GET str = %q", got)
	}
	if got := rc.intWant("TTL", "ttl"); got <= 0 {
		t.Errorf("TTL ttl = %d, want positive", got)
	}
	if lr := rc.cmd("LRANGE", "list", "0", "-1"); len(lr.Array) != 3 || string(lr.Array[0].Str) != "a" {
		t.Errorf("LRANGE list = %+v", lr)
	}
	if h := rc.cmd("HGETALL", "hash"); len(h.Array) != 4 {
		t.Errorf("HGETALL hash = %+v", h)
	}
	if s := rc.cmd("SMEMBERS", "set"); len(s.Array) != 2 {
		t.Errorf("SMEMBERS set = %+v", s)
	}
	if z := rc.cmd("ZRANGE", "zset", "0", "-1", "WITHSCORES"); len(z.Array) != 4 {
		t.Errorf("ZRANGE zset = %+v", z)
	}
	if x := rc.cmd("XRANGE", "stream", "-", "+"); len(x.Array) != 1 {
		t.Errorf("XRANGE stream = %+v", x)
	}
	if got := string(rc.cmd("TYPE", "stream").Str); got != "stream" {
		t.Errorf("TYPE stream = %q", got)
	}
	rc.ok("SELECT", "2")
	if got := string(rc.bulkWant("GET", "db2")); got != "yes" {
		t.Errorf("GET db2 = %q", got)
	}
	rc.ok("SELECT", "0")
}

// TestReplicationFullSyncBothModels runs the full synchronization test in
// both I/O models: the dataset must arrive complete, the streams of the
// commands must keep following.
func TestReplicationFullSyncBothModels(t *testing.T) {
	for _, loopMode := range []bool{false, true} {
		name := "goroutine"
		if loopMode {
			name = "loop"
		}
		t.Run(name, func(t *testing.T) {
			p := startReplPair(t, loopMode)
			p.seed(t)
			p.waitLinkUp(t)
			p.assertSeed(t)
		})
	}
}

// TestReplicationStream follows the live commands of the master: the writes
// after the synchronization (including the effects of the blocking pops and
// the cross-database commands) must arrive on the replica.
func TestReplicationStream(t *testing.T) {
	for _, loopMode := range []bool{false, true} {
		name := "goroutine"
		if loopMode {
			name = "loop"
		}
		t.Run(name, func(t *testing.T) {
			p := startReplPair(t, loopMode)
			p.waitLinkUp(t)

			p.mc.cmd("SET", "k1", "v1")
			p.waitReplicaBulk(t, []string{"GET", "k1"}, "v1")

			p.mc.cmd("SET", "ex", "val", "EX", "500")
			p.waitReplicaBulk(t, []string{"GET", "ex"}, "val")

			p.mc.cmd("RPUSH", "bl", "a", "b")
			p.mc.cmd("BLPOP", "bl", "0") // the stream must hold the effect (LPOP)
			p.waitReplicaInt(t, []string{"LLEN", "bl"}, 1)

			p.mc.cmd("INCR", "hits")
			p.mc.cmd("INCR", "hits")
			p.waitReplicaBulk(t, []string{"GET", "hits"}, "2")

			// Multi-database writes interleave: the stream must keep the
			// SELECT records straight.
			p.mc.ok("SELECT", "3")
			p.mc.ok("SET", "three", "333")
			p.mc.ok("SELECT", "0")
			p.mc.ok("SET", "zero", "000")
			p.waitReplicaBulk(t, []string{"GET", "zero"}, "000")
			p.rc.ok("SELECT", "3")
			p.waitReplicaBulk(t, []string{"GET", "three"}, "333")
			p.rc.ok("SELECT", "0")

			// The cross-database commands are part of the replay: COPY with
			// a target database, MOVE and SWAPDB.
			p.mc.cmd("SET", "src", "copied")
			p.mc.cmd("COPY", "src", "dst", "DB", "1")
			p.mc.cmd("MOVE", "src", "2")
			p.mc.cmd("SWAPDB", "1", "2")
			// After the swap: dst (written to db1) lives in db2, src (moved
			// to db2) lives in db1.
			p.rc.ok("SELECT", "2")
			p.waitReplicaBulk(t, []string{"GET", "dst"}, "copied")
			p.rc.ok("SELECT", "1")
			p.waitReplicaBulk(t, []string{"GET", "src"}, "copied")
			p.rc.ok("SELECT", "0")

			// Streams follow as well.
			p.mc.cmd("XADD", "stream", "*", "f", "v2")
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if n := p.rc.intWant("XLEN", "stream"); n == 1 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if n := p.rc.intWant("XLEN", "stream"); n != 1 {
				t.Errorf("XLEN stream on the replica = %d, want 1", n)
			}

			// The deleted keys disappear on the replica too.
			p.mc.cmd("DEL", "k1")
			deadline = time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				v := p.rc.cmd("EXISTS", "k1")
				if v.Kind == resp.KindInteger && v.Int == 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("DEL k1 did not reach the replica")
		})
	}
}

// TestReplicaReadOnly rejects the writes of the clients of a replica and
// allows them again after REPLICAOF NO ONE.
func TestReplicaReadOnly(t *testing.T) {
	for _, loopMode := range []bool{false, true} {
		name := "goroutine"
		if loopMode {
			name = "loop"
		}
		t.Run(name, func(t *testing.T) {
			p := startReplPair(t, loopMode)
			p.waitLinkUp(t)

			if err := p.rc.expectError("SET", "k", "v"); !strings.Contains(err, "READONLY") {
				t.Errorf("SET on the replica: got %q, want a READONLY error", err)
			}
			if err := p.rc.expectError("DEL", "k"); !strings.Contains(err, "READONLY") {
				t.Errorf("DEL on the replica: got %q, want a READONLY error", err)
			}

			// ROLE and INFO report the replica.
			role := p.rc.cmd("ROLE")
			if len(role.Array) != 5 || string(role.Array[0].Str) != "slave" {
				t.Errorf("ROLE on the replica = %+v, want a slave reply", role)
			}
			info := string(p.rc.bulkWant("INFO", "replication"))
			if !strings.Contains(info, "role:slave") || !strings.Contains(info, "master_host:127.0.0.1") {
				t.Errorf("INFO replication on the replica lacks the slave fields: %q", info)
			}

			// Promotion: writes work again, the role is master, the data of
			// the master is still there.
			p.mc.ok("SET", "inherited", "yes")
			p.waitReplicaBulk(t, []string{"GET", "inherited"}, "yes")
			p.rc.ok("REPLICAOF", "NO", "ONE")
			p.rc.ok("SET", "own", "write")
			role = p.rc.cmd("ROLE")
			if len(role.Array) != 3 || string(role.Array[0].Str) != "master" {
				t.Errorf("ROLE after promotion = %+v, want a master reply", role)
			}
			if got := string(p.rc.bulkWant("GET", "inherited")); got != "yes" {
				t.Errorf("GET inherited after promotion = %q", got)
			}
		})
	}
}

// TestRoleMasterShape checks the ROLE reply of a master with a connected
// replica.
func TestRoleMasterShape(t *testing.T) {
	p := startReplPair(t, false)
	p.waitLinkUp(t)

	role := p.mc.cmd("ROLE")
	if len(role.Array) != 3 || string(role.Array[0].Str) != "master" {
		t.Fatalf("ROLE on the master = %+v", role)
	}
	slaves := role.Array[2].Array
	if len(slaves) != 1 {
		t.Fatalf("ROLE on the master lists %+v replicas, want one", role.Array[2])
	}
	if got := string(slaves[0].Array[0].Str); got != "127.0.0.1" {
		t.Errorf("the address of the replica = %q", got)
	}
	if slaves[0].Array[1].Kind != resp.KindInteger || slaves[0].Array[1].Int <= 0 {
		t.Errorf("the listening port of the replica = %+v, want the announced one", slaves[0].Array[1])
	}

	info := string(p.mc.bulkWant("INFO", "replication"))
	if !strings.Contains(info, "role:master") ||
		!strings.Contains(info, "connected_slaves:1") ||
		!strings.Contains(info, "master_replid:") ||
		!strings.Contains(info, "repl_backlog_size:") {
		t.Errorf("INFO replication on the master lacks the master fields: %q", info)
	}

	// HELLO reports the role as well: the value sits behind the "role" key.
	hello := p.mc.cmd("HELLO")
	if len(hello.Array) < 12 || string(hello.Array[10].Str) != "role" || string(hello.Array[11].Str) != "master" {
		t.Errorf("HELLO on the master reports role %+v/%+v", hello.Array[10], hello.Array[11])
	}
	hello = p.rc.cmd("HELLO")
	if len(hello.Array) < 12 || string(hello.Array[10].Str) != "role" || string(hello.Array[11].Str) != "slave" {
		t.Errorf("HELLO on the replica reports role %+v/%+v", hello.Array[10], hello.Array[11])
	}
}

// TestReplicaOfCommandSwitchesMaster attaches the replica to the master with
// the REPLICAOF command instead of the startup flag.
func TestReplicaOfCommandSwitchesMaster(t *testing.T) {
	p := startReplPair(t, false)
	// Detach: the replica is a master of its own.
	p.rc.ok("REPLICAOF", "NO", "ONE")

	p.mc.ok("SET", "mk", "mv")
	p.rc.ok("SET", "rk", "rv")

	// Attach through the command: the dataset of the master replaces ours.
	p.rc.ok("REPLICAOF", "127.0.0.1", p.mAddr[strings.LastIndex(p.mAddr, ":")+1:])
	p.waitLinkUp(t)
	p.waitReplicaBulk(t, []string{"GET", "mk"}, "mv")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v := p.rc.cmd("EXISTS", "rk")
		if v.Kind == resp.KindInteger && v.Int == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the local key rk survived the resynchronization")
}

// TestReplConf checks the REPLCONF behavior: the handshake options are +OK,
// an unknown option is an error and the ACK replies with nothing.
func TestReplConf(t *testing.T) {
	p := startReplPair(t, false)
	if err := p.mc.expectError("REPLCONF", "bogus"); !strings.Contains(err, "syntax error") {
		t.Errorf("REPLCONF with a dangling option = %q, want a syntax error", err)
	}
	if err := p.mc.expectError("REPLCONF", "bogus", "value"); !strings.Contains(err, "Unrecognized REPLCONF option") {
		t.Errorf("REPLCONF bogus value = %q, want an unknown-option error", err)
	}
	if err := p.mc.expectError("REPLCONF", "opt", "value", "opt2"); !strings.Contains(err, "syntax error") {
		t.Errorf("REPLCONF with an even number of arguments = %q, want a syntax error", err)
	}
	p.mc.ok("REPLCONF", "listening-port", "7000", "capa", "eof", "capa", "psync2", "ip-address", "10.0.0.1")

	// The ACK must not produce a reply: a PING pipelined after it answers
	// with exactly one PONG.
	p.mc.send("REPLCONF", "ack", "0")
	p.mc.send("PING")
	if v := p.mc.cmd("PING"); string(v.Str) != "PONG" {
		t.Fatalf("the reply after REPLCONF ACK is %+v, want PONG", v)
	}
}

// TestPSyncRawHandshake walks the handshake of a replica by hand: REPLCONF,
// PSYNC ? -1, the FULLRESYNC line, the length-prefixed RDB and the command
// stream that follows.
func TestPSyncRawHandshake(t *testing.T) {
	p := startReplPair(t, false)
	p.mc.cmd("SET", "a", "1")
	p.mc.cmd("RPUSH", "l", "x", "y")

	conn, err := net.Dial("tcp", p.mAddr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	// One buffered reader serves the whole handshake, the RDB payload and
	// the stream: the bytes it prefetched are never lost.
	br := bufio.NewReader(conn)
	bw := resp.NewWriter(conn)
	send := func(args ...string) {
		t.Helper()
		elems := make([]resp.Value, len(args))
		for i, a := range args {
			elems[i] = resp.NewBulkString([]byte(a))
		}
		if err := bw.WriteValue(resp.NewArray(elems)); err != nil {
			t.Fatalf("write %v: %v", args, err)
		}
		if err := bw.Flush(); err != nil {
			t.Fatalf("flush %v: %v", args, err)
		}
	}
	readLine := func() string {
		t.Helper()
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read a line: %v", err)
		}
		return strings.TrimRight(line, "\r\n")
	}

	send("REPLCONF", "listening-port", "6550")
	if line := readLine(); line != "+OK" {
		t.Fatalf("REPLCONF listening-port = %q, want +OK", line)
	}
	send("REPLCONF", "capa", "psync2")
	if line := readLine(); line != "+OK" {
		t.Fatalf("REPLCONF capa = %q, want +OK", line)
	}
	send("PSYNC", "?", "-1")
	line := readLine()
	if !strings.HasPrefix(line, "+FULLRESYNC ") {
		t.Fatalf("the PSYNC reply = %q, want +FULLRESYNC", line)
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || len(fields[1]) != 40 {
		t.Fatalf("the FULLRESYNC reply %q: want a 40-character replication id and an offset", line)
	}

	// The RDB arrives as a raw bulk: "$<len>\r\n<rdb bytes>" without the
	// trailing CRLF.
	preamble := readLine()
	if !strings.HasPrefix(preamble, "$") {
		t.Fatalf("the RDB preamble %q does not start with $", preamble)
	}
	n, err := strconv.Atoi(preamble[1:])
	if err != nil || n <= 0 {
		t.Fatalf("the RDB length %q is not positive", preamble)
	}
	rdb := make([]byte, n)
	if _, err := io.ReadFull(br, rdb); err != nil {
		t.Fatalf("read the RDB: %v", err)
	}
	if string(rdb[:5]) != "REDIS" {
		t.Fatalf("the payload is not an RDB file")
	}

	// The master keeps streaming: a write now must reach this connection.
	// The stream starts with a re-emitted SELECT (the fresh replica does not
	// know which database the stream was in); the frames after it carry the
	// commands.
	p.mc.cmd("SET", "after", "sync")
	rd := resp.NewReaderSize(br, resp.DefaultReadBufferSize)
	for {
		v, err := rd.ReadValue()
		if err != nil {
			t.Fatalf("read the streamed command: %v", err)
		}
		if v.Kind != resp.KindArray || len(v.Array) == 0 {
			t.Fatalf("the streamed frame = %+v, want a command array", v)
		}
		name := string(v.Array[0].Str)
		if name == "select" || name == "ping" {
			continue
		}
		if name != "set" || len(v.Array) < 3 || string(v.Array[1].Str) != "after" {
			t.Fatalf("the streamed command = %+v, want [set, after, sync]", v)
		}
		break
	}
}
