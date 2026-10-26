package store

import (
	"testing"
)

// TestCopyEntryKeepsStreamGroups checks that copying a stream keeps its
// consumer groups: COPY, MOVE and DUMP use it.
func TestCopyEntryKeepsStreamGroups(t *testing.T) {
	s := New()
	key := []byte("st")
	if _, err := s.StreamAdd(key, StreamIDExplicit, StreamID{MS: 1}, [][]byte{[]byte("f"), []byte("v")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupCreate(key, "grp", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupCreateConsumer(key, "grp", "consumer"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupRead(key, "grp", "consumer", StreamReadNew, StreamID{}, 10, false); err != nil {
		t.Fatal(err)
	}
	copied, ok := s.CopyEntry(key)
	if !ok {
		t.Fatal("the stream was not copied")
	}
	original, ok, err := s.StreamSnapshot(key)
	if !ok || err != nil {
		t.Fatalf("the original stream is missing: %v", err)
	}
	groups := copied.Stream.Snapshot().Groups
	if len(groups) != len(original.Groups) {
		t.Fatalf("the copy has %d groups, want %d", len(groups), len(original.Groups))
	}
	if groups[0].Name != "grp" {
		t.Errorf("the copied group is named %q, want grp", groups[0].Name)
	}
	if len(groups[0].Consumers) != 1 || groups[0].Consumers[0].Name != "consumer" {
		t.Errorf("the copied group has the consumers %+v, want one named consumer", groups[0].Consumers)
	}
	if len(groups[0].Pending) != 1 {
		t.Errorf("the copied group has %d pending entries, want 1", len(groups[0].Pending))
	}
}

// TestStreamSnapshotRestore checks that a stream restored from a snapshot is
// the same as the original one.
func TestStreamSnapshotRestore(t *testing.T) {
	s := New()
	key := []byte("st")
	if _, err := s.StreamAdd(key, StreamIDExplicit, StreamID{MS: 1, Seq: 1}, [][]byte{[]byte("f"), []byte("v1")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamAdd(key, StreamIDExplicit, StreamID{MS: 2, Seq: 1}, [][]byte{[]byte("f"), []byte("v2")}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupCreate(key, "grp", StreamID{MS: 1, Seq: 1}, 5, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupCreateConsumer(key, "grp", "consumer"); err != nil {
		t.Fatal(err)
	}

	snap, ok, err := s.StreamSnapshot(key)
	if !ok || err != nil {
		t.Fatalf("the snapshot is missing: %v", err)
	}
	restored := New()
	if err := restored.RestoreStream([]byte("copy"), snap); err != nil {
		t.Fatalf("RestoreStream: %v", err)
	}
	got, ok, err := restored.StreamSnapshot([]byte("copy"))
	if !ok || err != nil {
		t.Fatalf("the restored stream is missing: %v", err)
	}
	if len(got.Entries) != len(snap.Entries) {
		t.Errorf("the restored stream has %d entries, want %d", len(got.Entries), len(snap.Entries))
	}
	if got.LastID != snap.LastID {
		t.Errorf("the restored stream ends with %s, want %s", got.LastID, snap.LastID)
	}
	if len(got.Groups) != 1 {
		t.Fatalf("the restored stream has %d groups, want 1", len(got.Groups))
	}
	if got.Groups[0].Name != "grp" {
		t.Errorf("the restored group is named %q, want grp", got.Groups[0].Name)
	}
	// The counter of the group is limited by the number of the entries of
	// the stream, so it is compared with the original one.
	if got.Groups[0].EntriesRead != snap.Groups[0].EntriesRead ||
		got.Groups[0].EntriesReadValid != snap.Groups[0].EntriesReadValid {
		t.Errorf("the restored group read %d entries (valid=%v), want %d (valid=%v)",
			got.Groups[0].EntriesRead, got.Groups[0].EntriesReadValid,
			snap.Groups[0].EntriesRead, snap.Groups[0].EntriesReadValid)
	}
	if len(got.Groups[0].Consumers) != 1 || got.Groups[0].Consumers[0].Name != "consumer" {
		t.Errorf("the restored group has the consumers %+v, want one named consumer", got.Groups[0].Consumers)
	}
}
