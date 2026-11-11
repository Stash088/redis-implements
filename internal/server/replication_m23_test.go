package server_test

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

// TestPartialResyncProtocol handshakes a second replica with the replication
// ID and offset the first one reached: the master accepts the partial resync
// (+CONTINUE) and replays exactly the missed bytes of the backlog.
func TestPartialResyncProtocol(t *testing.T) {
	p := startReplPair(t, false)
	p.waitLinkUp(t)

	p.mc.cmd("SET", "a", "1")
	p.waitReplicaBulk(t, []string{"GET", "a"}, "1")
	info := string(p.mc.bulkWant("INFO", "replication"))
	replid, offset := parseReplIDOffset(info)
	if replid == "" || offset <= 0 {
		t.Fatalf("INFO replication holds no replication id/offset: %q", info)
	}

	// More writes land in the backlog after the offset the first replica
	// acknowledged.
	p.mc.cmd("SET", "b", "2")
	p.mc.cmd("RPUSH", "l", "x", "y")

	// A second replica asks to continue from the first one's offset.
	conn := dialRaw(t, p.mAddr)
	br := bufio.NewReader(conn)
	bw := resp.NewWriter(conn)
	sendRaw(t, bw, "REPLCONF", "listening-port", "6551")
	expectLine(t, br, "+OK")
	sendRaw(t, bw, "REPLCONF", "capa", "psync2")
	expectLine(t, br, "+OK")
	sendRaw(t, bw, "PSYNC", replid, itoaRepl(offset))
	line := readRawLine(t, br)
	if !strings.HasPrefix(line, "+CONTINUE "+replid) {
		t.Fatalf("the PSYNC reply = %q, want +CONTINUE %s", line, replid)
	}

	// The replay of the backlog arrives as RESP commands: apply them by
	// hand and check the result equals the state of the master.
	rd := resp.NewReaderSize(br, resp.DefaultReadBufferSize)
	applied := 0
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && applied < 2 {
		v, err := rd.ReadValue()
		if err != nil {
			t.Fatalf("read the replay: %v", err)
		}
		if v.Kind != resp.KindArray || len(v.Array) == 0 {
			t.Fatalf("the replay frame = %+v, want a command", v)
		}
		name := strings.ToLower(string(v.Array[0].Str))
		if name == "ping" {
			continue
		}
		applied++
		switch applied {
		case 1:
			if name != "set" || string(v.Array[1].Str) != "b" {
				t.Fatalf("the first replayed command = %+v, want [set, b, 2]", v.Array)
			}
		case 2:
			if name != "rpush" || len(v.Array) != 4 {
				t.Fatalf("the second replayed command = %+v, want [rpush, l, x, y]", v.Array)
			}
		}
	}
	if applied < 2 {
		t.Fatalf("the replay carried %d commands, want 2", applied)
	}
}

// TestReplicaReconnectsPartially kills the connection of the replica from
// the master side: the replica reconnects and continues from the offset it
// reached without repeating the full synchronization.
func TestReplicaReconnectsPartially(t *testing.T) {
	p := startReplPair(t, false)
	p.waitLinkUp(t)
	p.mc.cmd("SET", "before", "sync")
	p.waitReplicaBulk(t, []string{"GET", "before"}, "sync")

	// The master keeps the log of the resyncs: the partial acceptance is
	// asserted below.
	var logMu sync.Mutex
	logBuf := &bytes.Buffer{}
	p.master.SetLogger(log.New(&lockedWriter{buf: logBuf, mu: &logMu}, "", 0))

	// The address of the replica connection on the master side: CLIENT LIST
	// shows the connection whose last command was the handshake.
	info := string(p.mc.bulkWant("CLIENT", "LIST"))
	addr := parseReplicaConnAddr(info)
	if addr == "" {
		t.Fatalf("CLIENT LIST holds no replica connection: %q", info)
	}
	if killed := p.mc.intWant("CLIENT", "KILL", "ADDR", addr); killed != 1 {
		t.Fatalf("CLIENT KILL %s = %d, want 1", addr, killed)
	}

	// The replica reconnects: the link is up again and the missed commands
	// arrive.
	p.mc.cmd("SET", "after", "sync")
	waitReplBulkWithDeadline(t, p, "GET", "after", "sync", 15*time.Second)
	if got := string(p.rc.cmd("GET", "before").Str); got != "sync" {
		t.Errorf("GET before = %q, want sync (the dataset survived the reconnect)", got)
	}

	// The reconnect must have been a partial resync: the replication ID and
	// the backlog survived, no full synchronization happened.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		logMu.Lock()
		logged := logBuf.String()
		logMu.Unlock()
		if strings.Contains(logged, "partial resync accepted") {
			if strings.Contains(logged, "full resync") {
				t.Fatalf("the reconnect repeated the full sync:\n%s", logged)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	logMu.Lock()
	defer logMu.Unlock()
	t.Fatalf("the reconnect was not a partial resync:\n%s", logBuf.String())
}

// TestWaitCommand checks WAIT in both I/O models: with one replica the
// command returns 1 as soon as the replica acknowledged, and 1 again (the
// current count) when an unreachable count times out.
func TestWaitCommand(t *testing.T) {
	for _, loopMode := range []bool{false, true} {
		name := "goroutine"
		if loopMode {
			name = "loop"
		}
		t.Run(name, func(t *testing.T) {
			p := startReplPair(t, loopMode)
			p.waitLinkUp(t)

			// WAIT on a replica is an error.
			if err := p.rc.expectError("WAIT", "1", "100"); !strings.Contains(err, "WAIT cannot be used with replica instances") {
				t.Errorf("WAIT on the replica = %q", err)
			}

			// Two replicas are asked, one is connected: the command times
			// out and answers the current count.
			started := time.Now()
			if n := p.mc.intWant("WAIT", "2", "300"); n != 1 {
				t.Errorf("WAIT 2 300 = %d, want 1 (the current count on a timeout)", n)
			}
			if time.Since(started) > 2500*time.Millisecond {
				t.Errorf("WAIT 2 300 took %v, want about 300ms", time.Since(started))
			}

			// A write is satisfied by the single replica promptly.
			p.mc.cmd("SET", "w", "1")
			started = time.Now()
			if n := p.mc.intWant("WAIT", "1", "10000"); n != 1 {
				t.Errorf("WAIT 1 10000 = %d, want 1", n)
			}
			if elapsed := time.Since(started); elapsed > 3*time.Second {
				t.Errorf("WAIT 1 took %v, want well below the timeout", elapsed)
			}
		})
	}
}

// TestWaitAofCommand checks WAITAOF: without the journal numlocal must be 0,
// with the journal the local part is satisfied synchronously.
func TestWaitAofCommand(t *testing.T) {
	p := startReplPair(t, false)
	p.waitLinkUp(t)

	if err := p.rc.expectError("WAITAOF", "0", "1", "100"); !strings.Contains(err, "WAITAOF cannot be used with replica instances") {
		t.Errorf("WAITAOF on the replica = %q", err)
	}
	if err := p.mc.expectError("WAITAOF", "1", "1", "100"); !strings.Contains(err, "appendonly is disabled") {
		t.Errorf("WAITAOF 1 without the journal = %q", err)
	}

	p.mc.cmd("SET", "wa", "1")
	v := p.mc.cmd("WAITAOF", "0", "1", "10000")
	if v.Kind != resp.KindArray || len(v.Array) != 2 || v.Array[0].Int != 0 || v.Array[1].Int != 1 {
		t.Errorf("WAITAOF 0 1 10000 = %+v, want [0, 1]", v)
	}
	v = p.mc.cmd("WAITAOF", "0", "2", "200")
	if v.Kind != resp.KindArray || len(v.Array) != 2 || v.Array[1].Int != 1 {
		t.Errorf("WAITAOF 0 2 200 = %+v, want [0, 1] on a timeout", v)
	}
}

// dialRaw opens a raw connection to the server.
func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// sendRaw writes a command to the raw connection.
func sendRaw(t *testing.T, bw *resp.Writer, args ...string) {
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

// expectLine reads one status line and compares it.
func expectLine(t *testing.T, br *bufio.Reader, want string) {
	t.Helper()
	line := readRawLine(t, br)
	if line != want {
		t.Fatalf("the reply = %q, want %q", line, want)
	}
}

// readRawLine reads one CRLF line from the buffered reader.
func readRawLine(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read a line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

// parseReplIDOffset pulls master_replid and master_repl_offset out of INFO.
func parseReplIDOffset(info string) (string, int64) {
	var id string
	var off int64
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "master_replid:") && id == "" {
			id = strings.TrimPrefix(line, "master_replid:")
		}
		if strings.HasPrefix(line, "master_repl_offset:") {
			off = atoiRepl(strings.TrimPrefix(line, "master_repl_offset:"))
		}
	}
	return id, off
}

// parseReplicaConnAddr pulls the addr= of the replica connection (the client
// whose last command is the handshake) out of CLIENT LIST.
func parseReplicaConnAddr(list string) string {
	for _, line := range strings.Split(list, "\n") {
		if !strings.Contains(line, "cmd=psync") && !strings.Contains(line, "cmd=replconf") {
			continue
		}
		for _, kv := range strings.Fields(line) {
			if strings.HasPrefix(kv, "addr=") {
				return strings.TrimPrefix(kv, "addr=")
			}
		}
	}
	return ""
}

// lockedWriter is a log writer safe for concurrent use.
type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// waitReplBulkWithDeadline is waitReplicaBulk with a custom deadline.
func waitReplBulkWithDeadline(t *testing.T, p *replPair, cmd, key, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		v := p.rc.cmd(cmd, key)
		if v.Kind == resp.KindBulkString && !v.IsNull && string(v.Str) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	v := p.rc.cmd(cmd, key)
	t.Fatalf("%s %s still returns %+v, want %q", cmd, key, v, want)
}

func itoaRepl(n int64) string {
	return strconv.FormatInt(n, 10)
}

func atoiRepl(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
