package server_test

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

func readStoreHLLFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("../store/testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func TestPFAddCountMergeCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if n := c.intWant("PFADD", "h", "a", "b", "c"); n != 1 {
		t.Errorf("PFADD h a b c = %d, want 1", n)
	}
	if n := c.intWant("PFADD", "h", "a"); n != 0 {
		t.Errorf("PFADD repeated element = %d, want 0", n)
	}
	if n := c.intWant("PFADD", "h"); n != 0 {
		t.Errorf("PFADD existing key without elements = %d, want 0", n)
	}
	if n := c.intWant("PFCOUNT", "h"); n != 3 {
		t.Errorf("PFCOUNT h = %d, want 3", n)
	}
	if got := c.cmd("TYPE", "h"); got.Kind != resp.KindSimpleString || string(got.Str) != "string" {
		t.Errorf("TYPE h = %+v, want string", got)
	}
	if n := c.intWant("PFCOUNT", "missing"); n != 0 {
		t.Errorf("PFCOUNT missing = %d, want 0", n)
	}
	if n := c.intWant("PFADD", "empty"); n != 1 {
		t.Errorf("PFADD new key without elements = %d, want 1", n)
	}
	if n := c.intWant("PFADD", "empty"); n != 0 {
		t.Errorf("PFADD empty existing key = %d, want 0", n)
	}
	if n := c.intWant("PFCOUNT", "empty"); n != 0 {
		t.Errorf("PFCOUNT empty = %d, want 0", n)
	}

	// Binary elements are passed through unchanged.
	binary := string([]byte{0, 1, 2, 0xff, 0xfe, 'x', 0})
	if n := c.intWant("PFADD", "bin", binary); n != 1 {
		t.Errorf("PFADD binary element = %d, want 1", n)
	}
	if n := c.intWant("PFADD", "bin", binary); n != 0 {
		t.Errorf("PFADD repeated binary element = %d, want 0", n)
	}
	if n := c.intWant("PFCOUNT", "bin"); n != 1 {
		t.Errorf("PFCOUNT bin = %d, want 1", n)
	}

	if got := c.cmd("PFMERGE", "d", "h", "bin"); got.Kind != resp.KindSimpleString || string(got.Str) != "OK" {
		t.Errorf("PFMERGE d h bin = %+v, want OK", got)
	}
	if n := c.intWant("PFCOUNT", "d"); n != 4 {
		t.Errorf("PFCOUNT after PFMERGE = %d, want 4", n)
	}
	// The destination is also an input; aliasing is allowed.
	if got := c.cmd("PFMERGE", "h", "h"); got.Kind != resp.KindSimpleString || string(got.Str) != "OK" {
		t.Errorf("PFMERGE h h = %+v, want OK", got)
	}
	if n := c.intWant("PFCOUNT", "h"); n != 3 {
		t.Errorf("PFCOUNT after aliased PFMERGE = %d, want 3", n)
	}
	// A missing destination is created and missing sources are empty.
	c.cmd("PFMERGE", "new", "h", "missing")
	if n := c.intWant("PFCOUNT", "new"); n != 3 {
		t.Errorf("PFCOUNT new = %d, want 3", n)
	}
}

func TestPFAddCountMergeErrors(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	c.ok("SET", "bad", "abc")
	if got := c.expectError("PFCOUNT", "bad"); got != "WRONGTYPE Key is not a valid HyperLogLog string value." {
		t.Errorf("PFCOUNT invalid header = %q", got)
	}
	if got := c.expectError("PFADD", "bad", "x"); got != "WRONGTYPE Key is not a valid HyperLogLog string value." {
		t.Errorf("PFADD invalid header = %q", got)
	}
	if got := c.expectError("PFMERGE", "out", "bad"); got != "WRONGTYPE Key is not a valid HyperLogLog string value." {
		t.Errorf("PFMERGE invalid header = %q", got)
	}
	if n := c.intWant("EXISTS", "out"); n != 0 {
		t.Errorf("EXISTS out after failed PFMERGE = %d, want 0", n)
	}

	corrupt := readStoreHLLFixture(t, "hll_sparse_invalid.bin")
	if len(corrupt) > 0 {
		corrupt = corrupt[:len(corrupt)-1]
	}
	c.ok("SET", "corrupt", string(corrupt))
	if got := c.expectError("PFCOUNT", "corrupt"); got != "INVALIDOBJ Corrupted HLL object detected" {
		t.Errorf("PFCOUNT corrupt sparse = %q", got)
	}
	if got := c.expectError("PFADD", "corrupt", "x"); got != "INVALIDOBJ Corrupted HLL object detected" {
		t.Errorf("PFADD corrupt sparse = %q", got)
	}
	if got := c.expectError("PFMERGE", "out2", "corrupt"); got != "INVALIDOBJ Corrupted HLL object detected" {
		t.Errorf("PFMERGE corrupt sparse = %q", got)
	}

	c.intWant("RPUSH", "l", "x")
	if got := c.expectError("PFADD", "l", "x"); got != wrongTypeErr {
		t.Errorf("PFADD on a list = %q", got)
	}
	if got := c.expectError("PFCOUNT", "l"); got != wrongTypeErr {
		t.Errorf("PFCOUNT on a list = %q", got)
	}
	if got := c.expectError("PFMERGE", "d", "l"); got != wrongTypeErr {
		t.Errorf("PFMERGE source list = %q", got)
	}

	if got := c.expectError("PFADD"); got != "ERR wrong number of arguments for 'pfadd' command" {
		t.Errorf("PFADD arity = %q", got)
	}
	if got := c.expectError("PFCOUNT"); got != "ERR wrong number of arguments for 'pfcount' command" {
		t.Errorf("PFCOUNT arity = %q", got)
	}
	if got := c.expectError("PFMERGE"); got != "ERR wrong number of arguments for 'pfmerge' command" {
		t.Errorf("PFMERGE arity = %q", got)
	}
}

func TestPFAddMergePreserveTTL(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	c.intWant("PFADD", "src", "a")
	c.intWant("PEXPIRE", "src", "10000")
	c.intWant("PFADD", "src", "b")
	if ttl := c.intWant("PTTL", "src"); ttl <= 0 {
		t.Errorf("PTTL src after PFADD = %d, want positive", ttl)
	}
	c.cmd("PFMERGE", "permanent", "src")
	if ttl := c.intWant("TTL", "permanent"); ttl != -1 {
		t.Errorf("TTL permanent = %d, want -1", ttl)
	}
	c.intWant("PFADD", "dest", "c")
	c.intWant("PEXPIRE", "dest", "10000")
	c.cmd("PFMERGE", "dest", "src")
	if ttl := c.intWant("PTTL", "dest"); ttl <= 0 {
		t.Errorf("PTTL dest after PFMERGE = %d, want positive", ttl)
	}
	if n := c.intWant("PFCOUNT", "dest"); n != 3 {
		t.Errorf("PFCOUNT dest = %d, want 3", n)
	}
}

func TestPFCountWatchCacheMutation(t *testing.T) {
	addr := startTestServer(t)
	c1 := newClient(t, addr)
	c2 := newClient(t, addr)
	c3 := newClient(t, addr)

	// An uncached single-key PFCOUNT refreshes the value and must abort a
	// WATCH held by another client.
	c1.intWant("PFADD", "h", "a", "b", "c")
	c2.ok("WATCH", "h")
	c2.ok("MULTI")
	if v := c2.cmd("SET", "other", "1"); string(v.Str) != "QUEUED" {
		t.Fatalf("queued SET = %+v", v)
	}
	c3.intWant("PFCOUNT", "h")
	if v := c2.cmd("EXEC"); !v.IsNull {
		t.Fatalf("EXEC after a PFCOUNT cache refresh = %+v, want null array", v)
	}

	// A cached PFCOUNT is a pure read and must not abort the transaction.
	c1.intWant("PFADD", "h2", "a", "b", "c")
	c1.intWant("PFCOUNT", "h2")
	c2.ok("WATCH", "h2")
	c2.ok("MULTI")
	c2.cmd("SET", "other2", "1")
	c3.intWant("PFCOUNT", "h2")
	if v := c2.cmd("EXEC"); v.IsNull {
		t.Fatal("EXEC aborted on a cached PFCOUNT")
	}

	// The multi-key form never updates a cache, so it is also a pure read.
	c1.intWant("PFADD", "h3", "a")
	c1.intWant("PFADD", "h4", "b")
	c2.ok("WATCH", "h3")
	c2.ok("MULTI")
	c2.cmd("SET", "other3", "1")
	c3.intWant("PFCOUNT", "h3", "h4")
	if v := c2.cmd("EXEC"); v.IsNull {
		t.Fatal("EXEC aborted on a multi-key PFCOUNT")
	}
}

func TestPFCountReplicaReadAndCachePropagation(t *testing.T) {
	p := startReplPair(t, false)
	p.waitLinkUp(t)

	p.mc.intWant("PFADD", "h", "a", "b", "c")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.rc.intWant("EXISTS", "h") == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// PFCOUNT is read-only: it runs on the replica, while writes are
	// rejected there.
	if n := p.rc.intWant("PFCOUNT", "h"); n != 3 {
		t.Errorf("PFCOUNT on replica = %d, want 3", n)
	}
	if err := p.rc.expectError("PFADD", "h", "d"); !strings.Contains(err, "READONLY") {
		t.Errorf("PFADD on replica = %q, want READONLY", err)
	}
	if err := p.rc.expectError("PFMERGE", "x", "h"); !strings.Contains(err, "READONLY") {
		t.Errorf("PFMERGE on replica = %q, want READONLY", err)
	}

	// A cache refresh on the master propagates the raw PFCOUNT: the replica
	// ends up with exactly the same bytes (valid cached cardinality). hc is
	// never read with PFCOUNT on the replica, so only the propagated command
	// can make its cache valid there.
	p.mc.intWant("PFADD", "hc", "a", "b", "c")
	p.mc.intWant("PFCOUNT", "hc")
	masterValue := p.mc.bulkWant("GET", "hc")
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v := p.rc.cmd("GET", "hc")
		if v.Kind == resp.KindBulkString && !v.IsNull && bytes.Equal(v.Str, masterValue) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := p.rc.bulkWant("GET", "hc"); !bytes.Equal(got, masterValue) {
		t.Fatalf("replica HLL bytes did not follow the master cache refresh")
	}

	// A cached PFCOUNT and the multi-key form do not append to the stream.
	p.mc.intWant("PFCOUNT", "h") // refresh h first
	_, before := parseReplIDOffset(string(p.mc.bulkWant("INFO", "replication")))
	p.mc.intWant("PFCOUNT", "h")
	_, after := parseReplIDOffset(string(p.mc.bulkWant("INFO", "replication")))
	if after != before {
		t.Errorf("cached PFCOUNT changed master_repl_offset: %d -> %d", before, after)
	}

	p.mc.intWant("PFADD", "h5", "a")
	p.mc.intWant("PFADD", "h6", "b")
	_, before = parseReplIDOffset(string(p.mc.bulkWant("INFO", "replication")))
	p.mc.intWant("PFCOUNT", "h5", "h6")
	_, after = parseReplIDOffset(string(p.mc.bulkWant("INFO", "replication")))
	if after != before {
		t.Errorf("multi-key PFCOUNT changed master_repl_offset: %d -> %d", before, after)
	}

	// A refresh is an effective mutation: it is propagated and advances the
	// offset.
	p.mc.intWant("PFADD", "h7", "z")
	_, before = parseReplIDOffset(string(p.mc.bulkWant("INFO", "replication")))
	p.mc.intWant("PFCOUNT", "h7")
	_, after = parseReplIDOffset(string(p.mc.bulkWant("INFO", "replication")))
	if after <= before {
		t.Errorf("uncached PFCOUNT did not propagate: offset %d -> %d", before, after)
	}
}

func TestPFCountCommandMetadata(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	arr := c.arrWant("COMMAND", "INFO", "pfcount")
	if len(arr) != 1 || len(arr[0].Array) != 6 {
		t.Fatalf("COMMAND INFO pfcount = %+v", arr)
	}
	flags := arr[0].Array[2]
	if flags.Kind != resp.KindSet && flags.Kind != resp.KindArray {
		t.Fatalf("COMMAND INFO pfcount flags kind = %v", flags.Kind)
	}
	var readonly, mayReplicate bool
	for _, f := range flags.Array {
		switch string(f.Str) {
		case "readonly":
			readonly = true
		case "may_replicate":
			mayReplicate = true
		}
	}
	if !readonly || !mayReplicate {
		t.Errorf("COMMAND INFO pfcount flags = %+v, want readonly and may_replicate", flags.Array)
	}
}

// pfaddWorker drives one connection through independent PFADD commands. It
// avoids the testing.T helpers, which must not be used from a goroutine.
func pfaddWorker(addr string, id, count int) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	w := resp.NewWriter(conn)
	r := resp.NewReader(conn)
	for i := 0; i < count; i++ {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("PFADD")),
			resp.NewBulkString([]byte("hll")),
			resp.NewBulkString([]byte(fmt.Sprintf("worker-%d-element-%d", id, i))),
		})
		if err := w.WriteValue(cmd); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		v, err := r.ReadValue()
		if err != nil {
			return err
		}
		if v.Kind == resp.KindError {
			return fmt.Errorf("PFADD worker %d: %s", id, v.Str)
		}
	}
	return nil
}

// TestHLLConcurrentAOFReplay runs concurrent PFADDs on an expiring HLL in the
// goroutine mode and replays the journal: the last journaled snapshot must be
// the last live mutation, with no update lost to append reordering.
func TestHLLConcurrentAOFReplay(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)

	c.intWant("PFADD", "hll", "base")
	c.intWant("PEXPIRE", "hll", "3600000")

	const (
		workers   = 6
		perWorker = 40
	)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for id := 0; id < workers; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := pfaddWorker(addr, id, perWorker); err != nil {
				errs <- err
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Refresh the cache after the concurrent writes, then compare the exact
	// live value with the value rebuilt from the journal.
	c.intWant("PFCOUNT", "hll")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	liveValue := append([]byte(nil), c.bulkWant("GET", "hll")...)
	liveCount := c.intWant("PFCOUNT", "hll")

	got := store.New()
	f, err := os.Open(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := persist.LoadAOF(f, []*store.Store{got}); err != nil {
		t.Fatalf("load the aof: %v", err)
	}
	v, ok, err := got.GetString([]byte("hll"))
	if err != nil || !ok {
		t.Fatalf("GET hll after replay: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(v, liveValue) {
		t.Fatal("replayed HLL bytes differ from the live value")
	}
	card, _, _, err := got.PFCountKey([]byte("hll"))
	if err != nil || card != uint64(liveCount) {
		t.Fatalf("PFCOUNT hll after replay = %d, err=%v; want %d", card, err, liveCount)
	}
}

func aofFrames(t *testing.T, path string) [][][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the aof: %v", err)
	}
	r := resp.NewReaderSize(strings.NewReader(string(data)), 64*1024)
	var frames [][][]byte
	for {
		v, err := r.ReadValue()
		if err != nil {
			break
		}
		if v.Kind != resp.KindArray || len(v.Array) == 0 {
			t.Fatalf("the aof holds a value that is not a command: %+v", v)
		}
		args := make([][]byte, len(v.Array))
		for i := range v.Array {
			args[i] = v.Array[i].Str
		}
		frames = append(frames, args)
	}
	return frames
}

func TestHLLCommandFormsInAOF(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)

	c.ok("SET", "primed", "1")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}

	// A persistent PFADD keeps the compact raw command; a no-op writes
	// nothing.
	base := len(aofFrames(t, aofPath))
	c.intWant("PFADD", "p", "a", "b")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	frames := aofFrames(t, aofPath)[base:]
	if len(frames) != 1 || strings.ToLower(string(frames[0][0])) != "pfadd" {
		t.Fatalf("persistent PFADD journal = %v, want one pfadd", frames)
	}
	base = len(aofFrames(t, aofPath))
	c.intWant("PFADD", "p", "a")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if frames = aofFrames(t, aofPath)[base:]; len(frames) != 0 {
		t.Fatalf("no-op PFADD journal = %v, want nothing", frames)
	}

	// Updating an expiring value is journaled as the resulting SET ... PXAT.
	c.intWant("PFADD", "e", "a")
	c.intWant("PEXPIRE", "e", "10000")
	beforeTTL := c.intWant("PTTL", "e")
	base = len(aofFrames(t, aofPath))
	c.intWant("PFADD", "e", "b")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	frames = aofFrames(t, aofPath)[base:]
	if len(frames) != 1 || strings.ToLower(string(frames[0][0])) != "set" {
		t.Fatalf("expiring PFADD journal = %v, want one set", frames)
	}
	if string(frames[0][1]) != "e" || !bytes.Equal(frames[0][2], c.bulkWant("GET", "e")) {
		t.Fatalf("expiring PFADD journal holds the wrong key/value: %v", frames[0])
	}
	if len(frames[0]) != 5 || strings.ToUpper(string(frames[0][3])) != "PXAT" {
		t.Fatalf("expiring PFADD journal = %v, want SET key value PXAT ms", frames[0])
	}
	if pttl := c.intWant("PTTL", "e"); pttl <= 0 || pttl > beforeTTL {
		t.Errorf("PTTL e after PFADD = %d, before = %d", pttl, beforeTTL)
	}

	// PFMERGE journals the computed destination, including the PXAT of an
	// expiring destination.
	base = len(aofFrames(t, aofPath))
	c.cmd("PFMERGE", "m", "e")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	frames = aofFrames(t, aofPath)[base:]
	if len(frames) != 1 || strings.ToLower(string(frames[0][0])) != "set" {
		t.Fatalf("PFMERGE journal = %v, want one set", frames)
	}
	if string(frames[0][1]) != "m" || !bytes.Equal(frames[0][2], c.bulkWant("GET", "m")) {
		t.Fatalf("PFMERGE journal holds the wrong key/value: %v", frames[0])
	}
	if len(frames[0]) != 3 {
		t.Fatalf("persistent PFMERGE journal = %v, want plain SET", frames[0])
	}

	base = len(aofFrames(t, aofPath))
	c.cmd("PFMERGE", "e", "m")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	frames = aofFrames(t, aofPath)[base:]
	if len(frames) != 1 || strings.ToLower(string(frames[0][0])) != "set" ||
		len(frames[0]) != 5 || strings.ToUpper(string(frames[0][3])) != "PXAT" {
		t.Fatalf("expiring PFMERGE journal = %v, want SET key value PXAT ms", frames)
	}

	// A failed PFMERGE writes nothing.
	c.ok("SET", "invalid", "bad", "PX", "10000")
	base = len(aofFrames(t, aofPath))
	if got := c.expectError("PFMERGE", "phantom", "invalid"); got != "WRONGTYPE Key is not a valid HyperLogLog string value." {
		t.Fatalf("PFMERGE invalid = %q", got)
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if frames = aofFrames(t, aofPath)[base:]; len(frames) != 0 {
		t.Fatalf("failed PFMERGE journal = %v, want nothing", frames)
	}

	// A cache refresh is journaled once; a cached or multi-key read is not.
	c.intWant("PFADD", "c", "a")
	base = len(aofFrames(t, aofPath))
	c.intWant("PFCOUNT", "c")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	frames = aofFrames(t, aofPath)[base:]
	if len(frames) != 1 || strings.ToLower(string(frames[0][0])) != "pfcount" {
		t.Fatalf("cache refresh journal = %v, want one pfcount", frames)
	}
	base = len(aofFrames(t, aofPath))
	c.intWant("PFCOUNT", "c")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if frames = aofFrames(t, aofPath)[base:]; len(frames) != 0 {
		t.Fatalf("cached PFCOUNT journal = %v, want nothing", frames)
	}
	c.intWant("PFADD", "c2", "a")
	base = len(aofFrames(t, aofPath))
	c.intWant("PFCOUNT", "c", "c2")
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if frames = aofFrames(t, aofPath)[base:]; len(frames) != 0 {
		t.Fatalf("multi-key PFCOUNT journal = %v, want nothing", frames)
	}
}

// TestHLLPFMergeAOFReplayAfterExpiry is the regression for the review's AOF
// data-integrity failure: replaying the journal after the inputs expired must
// keep the expired source expired, keep PFMERGE's contribution, and not create
// the destination of a failed PFMERGE.
func TestHLLPFMergeAOFReplayAfterExpiry(t *testing.T) {
	dir := t.TempDir()
	addr, aofPath, a := startAOFServer(t, dir)
	c := newClient(t, addr)

	if n := c.intWant("PFADD", "source", "old"); n != 1 {
		t.Fatalf("PFADD source old = %d", n)
	}
	if n := c.intWant("PEXPIRE", "source", "1000"); n != 1 {
		t.Fatalf("PEXPIRE source = %d", n)
	}
	if n := c.intWant("PFADD", "source", "new"); n != 1 {
		t.Fatalf("PFADD source new = %d", n)
	}
	c.cmd("PFMERGE", "permanent", "source")
	if n := c.intWant("PFCOUNT", "permanent"); n != 2 {
		t.Fatalf("PFCOUNT permanent = %d, want 2", n)
	}
	c.ok("SET", "invalid", "bad", "PX", "1000")
	if got := c.expectError("PFMERGE", "phantom", "invalid"); got != "WRONGTYPE Key is not a valid HyperLogLog string value." {
		t.Fatalf("PFMERGE phantom invalid = %q", got)
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}

	// Replay at a clock past the original TTL, as a restart after the
	// deadline would.
	dbs := []*store.Store{store.New()}
	forever := time.Now().Add(2 * time.Second)
	dbs[0].SetClock(func() time.Time { return forever })
	f, err := os.Open(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := persist.LoadAOF(f, dbs); err != nil {
		t.Fatalf("load the aof: %v", err)
	}

	if n := dbs[0].Exists([]byte("source")); n != 0 {
		t.Errorf("EXISTS source after replay = %d, want 0", n)
	}
	card, _, _, err := dbs[0].PFCountKey([]byte("permanent"))
	if err != nil || card != 2 {
		t.Errorf("PFCOUNT permanent after replay = %d, err=%v; want 2", card, err)
	}
	if n := dbs[0].Exists([]byte("phantom")); n != 0 {
		t.Errorf("EXISTS phantom after replay = %d, want 0", n)
	}
}
