package persist

import (
	"bytes"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// buildStreamStore returns a store with a stream that has entries, a group,
// a consumer and a pending entry.
func buildStreamStore(t *testing.T) *store.Store {
	t.Helper()
	st := store.New()
	key := []byte("st")
	if _, err := st.StreamAdd(key, store.StreamIDExplicit, store.StreamID{MS: 1, Seq: 1}, [][]byte{[]byte("fld"), []byte("v1")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamAdd(key, store.StreamIDExplicit, store.StreamID{MS: 2, Seq: 1}, [][]byte{[]byte("fld"), []byte("v2")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamGroupCreate(key, "grp", store.StreamID{MS: 1, Seq: 1}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamGroupCreateConsumer(key, "grp", "consumer"); err != nil {
		t.Fatal(err)
	}
	// The consumer receives the first entry: it goes to the pending list.
	if _, err := st.StreamGroupRead(key, "grp", "consumer", store.StreamReadNew, store.StreamID{}, 10, false); err != nil {
		t.Fatal(err)
	}
	return st
}

// TestRDBStreamRoundTrip checks that a stream survives a save and a load.
func TestRDBStreamRoundTrip(t *testing.T) {
	st := buildStreamStore(t)
	var buf bytes.Buffer
	if err := WriteRDB(&buf, []*store.Store{st}); err != nil {
		t.Fatalf("WriteRDB: %v", err)
	}
	loaded := store.New()
	if err := LoadBytes(buf.Bytes(), []*store.Store{loaded}); err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	snap, ok, err := loaded.StreamSnapshot([]byte("st"))
	if !ok || err != nil {
		t.Fatalf("the stream is missing after the reload (ok=%v, err=%v)", ok, err)
	}
	if len(snap.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(snap.Entries))
	}
	if snap.Entries[0].ID != (store.StreamID{MS: 1, Seq: 1}) {
		t.Errorf("the first entry has the id %s, want 1-1", snap.Entries[0].ID)
	}
	if string(snap.Entries[1].Fields[1]) != "v2" {
		t.Errorf("the second entry has the value %q, want v2", snap.Entries[1].Fields[1])
	}
	if len(snap.Groups) != 1 {
		t.Fatalf("got %d groups, want 1", len(snap.Groups))
	}
	g := snap.Groups[0]
	if g.Name != "grp" {
		t.Errorf("the group is named %q, want grp", g.Name)
	}
	if len(g.Pending) != 1 {
		t.Fatalf("got %d pending entries, want 1", len(g.Pending))
	}
	if g.Pending[0].Consumer != "consumer" {
		t.Errorf("the pending entry belongs to %q, want consumer", g.Pending[0].Consumer)
	}
	if len(g.Consumers) != 1 || g.Consumers[0].Name != "consumer" {
		t.Errorf("the group has the consumers %+v, want one named consumer", g.Consumers)
	}
}

// TestRDBStreamManyEntries checks a stream that is longer than one node: the
// entries are split into the nodes of a hundred records.
func TestRDBStreamManyEntries(t *testing.T) {
	st := store.New()
	key := []byte("st")
	const n = 250
	for i := 0; i < n; i++ {
		id := store.StreamID{MS: uint64(i + 1), Seq: 0}
		if _, err := st.StreamAdd(key, store.StreamIDExplicit, id, [][]byte{[]byte("f"), []byte("v")}, false); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := WriteRDB(&buf, []*store.Store{st}); err != nil {
		t.Fatalf("WriteRDB: %v", err)
	}
	loaded := store.New()
	if err := LoadBytes(buf.Bytes(), []*store.Store{loaded}); err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	snap, ok, _ := loaded.StreamSnapshot(key)
	if !ok {
		t.Fatal("the stream is missing after the reload")
	}
	if len(snap.Entries) != n {
		t.Fatalf("got %d entries, want %d", len(snap.Entries), n)
	}
	for i, e := range snap.Entries {
		if e.ID.MS != uint64(i+1) {
			t.Fatalf("the entry %d has the id %s, want %d-0", i, e.ID, i+1)
		}
	}
}

// TestRDBStreamDifferentFields checks a stream whose entries have different
// fields: the entries that do not match the master keep their field names.
func TestRDBStreamDifferentFields(t *testing.T) {
	st := store.New()
	key := []byte("st")
	if _, err := st.StreamAdd(key, store.StreamIDExplicit, store.StreamID{MS: 1}, [][]byte{[]byte("a"), []byte("1")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StreamAdd(key, store.StreamIDExplicit, store.StreamID{MS: 2}, [][]byte{[]byte("b"), []byte("2")}, false); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteRDB(&buf, []*store.Store{st}); err != nil {
		t.Fatalf("WriteRDB: %v", err)
	}
	loaded := store.New()
	if err := LoadBytes(buf.Bytes(), []*store.Store{loaded}); err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	snap, ok, _ := loaded.StreamSnapshot(key)
	if !ok {
		t.Fatal("the stream is missing after the reload")
	}
	if len(snap.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(snap.Entries))
	}
	if string(snap.Entries[0].Fields[0]) != "a" || string(snap.Entries[0].Fields[1]) != "1" {
		t.Errorf("the first entry has the fields %q, want a 1", snap.Entries[0].Fields)
	}
	if string(snap.Entries[1].Fields[0]) != "b" || string(snap.Entries[1].Fields[1]) != "2" {
		t.Errorf("the second entry has the fields %q, want b 2", snap.Entries[1].Fields)
	}
}

// TestRDBStreamPendingTimes checks that the times of the pending entries and
// of the consumers survive the round trip.
func TestRDBStreamPendingTimes(t *testing.T) {
	st := buildStreamStore(t)
	snap, ok, _ := st.StreamSnapshot([]byte("st"))
	if !ok {
		t.Fatal("the stream is missing")
	}
	delivery := snap.Groups[0].Pending[0].DeliveryTime
	if delivery.IsZero() {
		t.Skip("the delivery time is not set")
	}
	var buf bytes.Buffer
	if err := WriteRDB(&buf, []*store.Store{st}); err != nil {
		t.Fatal(err)
	}
	loaded := store.New()
	if err := LoadBytes(buf.Bytes(), []*store.Store{loaded}); err != nil {
		t.Fatal(err)
	}
	loadedSnap, ok, _ := loaded.StreamSnapshot([]byte("st"))
	if !ok {
		t.Fatal("the stream is missing after the reload")
	}
	got := loadedSnap.Groups[0].Pending[0].DeliveryTime
	if got.UnixMilli() != delivery.UnixMilli() {
		t.Errorf("the delivery time is %s, want %s", got, delivery)
	}
	seen := loadedSnap.Groups[0].Consumers[0].SeenTime
	if seen.IsZero() {
		t.Error("the seen time of the consumer is missing")
	}
	if time.Since(seen) > time.Hour {
		t.Errorf("the seen time of the consumer is %s, want a recent one", seen)
	}
}
