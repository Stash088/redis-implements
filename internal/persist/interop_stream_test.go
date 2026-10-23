package persist

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/store"
)

// TestInteropRealRedisLoadsOurStreamDump writes an RDB with a stream and
// verifies that a real redis-server can load it and serve the stream.
func TestInteropRealRedisLoadsOurStreamDump(t *testing.T) {
	serverBin, cliBin := findRedis(t)
	dir := t.TempDir()

	st := store.New()
	key := []byte("st")
	if _, err := st.StreamAdd(key, store.StreamIDExplicit, store.StreamID{MS: 1, Seq: 1}, [][]byte{[]byte("fld"), []byte("v1")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamAdd(key, store.StreamIDExplicit, store.StreamID{MS: 2, Seq: 1}, [][]byte{[]byte("fld"), []byte("v2")}, false); err != nil {
		t.Fatal(err)
	}
	// The group starts from the beginning, so the read delivers the first
	// entry and it goes to the pending list.
	if _, err := st.StreamGroupCreate(key, "grp", store.StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamGroupCreateConsumer(key, "grp", "consumer"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamGroupRead(key, "grp", "consumer", store.StreamReadNew, store.StreamID{}, 10, false); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "dump.rdb")
	if err := Save(path, []*store.Store{st}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	port := startRealRedis(t, serverBin, dir)

	if got := redisCli(t, cliBin, port, "XLEN", "st"); got != "2" {
		t.Errorf("XLEN st = %q, want 2", got)
	}
	// The entries keep their identifiers and their fields.
	got := redisCli(t, cliBin, port, "XRANGE", "st", "-", "+")
	if !strings.Contains(got, "1-1") || !strings.Contains(got, "v1") || !strings.Contains(got, "2-1") || !strings.Contains(got, "v2") {
		t.Errorf("XRANGE st = %q", got)
	}
	// The group, its consumer and the pending entry survive.
	if got := redisCli(t, cliBin, port, "XINFO", "GROUPS", "st"); !strings.Contains(got, "grp") {
		t.Errorf("XINFO GROUPS st = %q", got)
	}
	if got := redisCli(t, cliBin, port, "XINFO", "CONSUMERS", "st", "grp"); !strings.Contains(got, "consumer") {
		t.Errorf("XINFO CONSUMERS st grp = %q", got)
	}
	if got := redisCli(t, cliBin, port, "XPENDING", "st", "grp"); !strings.Contains(got, "1-1") {
		t.Errorf("XPENDING st grp = %q", got)
	}
}

// TestInteropWeLoadRealRedisStreamDump has a real redis-server SAVE a stream
// and verifies that our loader restores it.
func TestInteropWeLoadRealRedisStreamDump(t *testing.T) {
	serverBin, cliBin := findRedis(t)
	dir := t.TempDir()

	port := startRealRedis(t, serverBin, dir)
	redisCli(t, cliBin, port, "XADD", "st", "1-1", "fld", "v1")
	redisCli(t, cliBin, port, "XADD", "st", "2-1", "fld", "v2")
	redisCli(t, cliBin, port, "XADD", "st", "3-1", "other", "v3")
	redisCli(t, cliBin, port, "XGROUP", "CREATE", "st", "grp", "0")
	redisCli(t, cliBin, port, "XGROUP", "CREATECONSUMER", "st", "grp", "consumer")
	redisCli(t, cliBin, port, "XREADGROUP", "GROUP", "grp", "consumer", "COUNT", "1", "STREAMS", "st", ">")
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

	snap, ok, err := st.StreamSnapshot([]byte("st"))
	if !ok || err != nil {
		t.Fatalf("the stream is missing after the reload (ok=%v, err=%v)", ok, err)
	}
	if len(snap.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(snap.Entries))
	}
	if snap.Entries[0].ID != (store.StreamID{MS: 1, Seq: 1}) {
		t.Errorf("the first entry has the id %s, want 1-1", snap.Entries[0].ID)
	}
	if string(snap.Entries[0].Fields[0]) != "fld" || string(snap.Entries[0].Fields[1]) != "v1" {
		t.Errorf("the first entry has the fields %q, want fld v1", snap.Entries[0].Fields)
	}
	if string(snap.Entries[2].Fields[0]) != "other" || string(snap.Entries[2].Fields[1]) != "v3" {
		t.Errorf("the third entry has the fields %q, want other v3", snap.Entries[2].Fields)
	}
	if len(snap.Groups) != 1 || snap.Groups[0].Name != "grp" {
		t.Fatalf("the groups are %+v, want one named grp", snap.Groups)
	}
	g := snap.Groups[0]
	if len(g.Consumers) != 1 || g.Consumers[0].Name != "consumer" {
		t.Errorf("the consumers are %+v, want one named consumer", g.Consumers)
	}
	if len(g.Pending) != 1 {
		t.Fatalf("got %d pending entries, want 1", len(g.Pending))
	}
	if g.Pending[0].ID != (store.StreamID{MS: 1, Seq: 1}) {
		t.Errorf("the pending entry has the id %s, want 1-1", g.Pending[0].ID)
	}
	if g.Pending[0].Consumer != "consumer" {
		t.Errorf("the pending entry belongs to %q, want consumer", g.Pending[0].Consumer)
	}
}
