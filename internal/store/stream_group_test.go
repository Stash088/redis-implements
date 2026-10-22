package store

import (
	"testing"
	"time"
)

// streamWithEntries creates a stream with the given identifiers.
func streamWithEntries(t *testing.T, ids ...uint64) *Store {
	t.Helper()
	s := New()
	s.SetClock(fixedClock(1000))
	for _, id := range ids {
		if _, err := s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: id}, [][]byte{[]byte("f"), []byte("v")}, false); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestStreamGroupCreate(t *testing.T) {
	s := New()
	s.SetClock(fixedClock(1000))
	created, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{MS: 5}, 0, false, true)
	if err != nil || !created {
		t.Fatalf("StreamGroupCreate: %v %v", created, err)
	}
	// The group exists now.
	created, err = s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, true)
	if err != nil || created {
		t.Errorf("the second create: %v %v, want false nil", created, err)
	}
	// The stream was created as well.
	if n, ok, _ := s.StreamLen([]byte("s")); !ok || n != 0 {
		t.Errorf("StreamLen: %d %v, want 0 true", n, ok)
	}
	// Without MKSTREAM a missing stream is an error.
	if _, err := s.StreamGroupCreate([]byte("nosuch"), "g", StreamID{}, 0, false, false); err != ErrStreamNotFound {
		t.Errorf("got %v, want ErrStreamNotFound", err)
	}
	// A group of another type is an error.
	s.SetString([]byte("k"), []byte("v"))
	if _, err := s.StreamGroupCreate([]byte("k"), "g", StreamID{}, 0, false, true); err != ErrWrongType {
		t.Errorf("got %v, want ErrWrongType", err)
	}
}

func TestStreamGroupDestroyAndSetID(t *testing.T) {
	s := streamWithEntries(t, 1, 2, 3)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if err := s.StreamGroupSetID([]byte("s"), "g", StreamID{MS: 2}, 0, false); err != nil {
		t.Fatal(err)
	}
	groups, ok, err := s.StreamGroupsInfo([]byte("s"))
	if err != nil || !ok || len(groups) != 1 {
		t.Fatalf("StreamGroupsInfo: %v %v %v", groups, ok, err)
	}
	if groups[0].LastID.MS != 2 {
		t.Errorf("got the last id %v, want 2-0", groups[0].LastID)
	}
	// The counter of the entries read is unknown until the group reads.
	if groups[0].EntriesReadValid {
		t.Error("the counter of the entries read must be unknown")
	}
	destroyed, err := s.StreamGroupDestroy([]byte("s"), "g")
	if err != nil || !destroyed {
		t.Errorf("StreamGroupDestroy: %v %v, want true nil", destroyed, err)
	}
	destroyed, err = s.StreamGroupDestroy([]byte("s"), "g")
	if err != nil || destroyed {
		t.Errorf("the second destroy: %v %v, want false nil", destroyed, err)
	}
}

func TestStreamGroupRead(t *testing.T) {
	s := streamWithEntries(t, 1, 2, 3)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	delivery, err := s.StreamGroupRead([]byte("s"), "g", "c", StreamReadNew, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(delivery.Entries))
	}
	if !delivery.LastIDChanged || delivery.LastID.MS != 3 {
		t.Errorf("got the last id %v, want 3-0", delivery.LastID)
	}
	if !delivery.ConsumerCreated {
		t.Error("the consumer must be created")
	}
	// The delivered entries are pending.
	if len(delivery.Pending) != 3 {
		t.Errorf("got %d pending entries, want 3", len(delivery.Pending))
	}
	info, ok, err := s.StreamPendingInfo([]byte("s"), "g")
	if err != nil || !ok {
		t.Fatalf("StreamPendingInfo: %v %v", ok, err)
	}
	if info.Count != 3 || info.Min.MS != 1 || info.Max.MS != 3 {
		t.Errorf("got %+v, want 3 entries from 1-0 to 3-0", info)
	}
	if len(info.Consumers) != 1 || info.Consumers[0].Name != "c" || info.Consumers[0].Count != 3 {
		t.Errorf("got the consumers %+v, want c with 3 entries", info.Consumers)
	}
	// A second read delivers nothing new.
	delivery, err = s.StreamGroupRead([]byte("s"), "g", "c", StreamReadNew, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Entries) != 0 {
		t.Errorf("got %d entries, want none", len(delivery.Entries))
	}
	// The history of the consumer holds the pending entries.
	delivery, err = s.StreamGroupRead([]byte("s"), "g", "c", StreamReadHistory, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Entries) != 3 {
		t.Errorf("got %d entries of the history, want 3", len(delivery.Entries))
	}
	// The history of another consumer is empty.
	if _, err := s.StreamGroupCreateConsumer([]byte("s"), "g", "other"); err != nil {
		t.Fatal(err)
	}
	delivery, err = s.StreamGroupRead([]byte("s"), "g", "other", StreamReadHistory, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Entries) != 0 {
		t.Errorf("got %d entries of the history, want none", len(delivery.Entries))
	}
}

func TestStreamGroupReadNoAck(t *testing.T) {
	s := streamWithEntries(t, 1, 2)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	delivery, err := s.StreamGroupRead([]byte("s"), "g", "c", StreamReadNew, StreamID{}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(delivery.Entries))
	}
	if len(delivery.Pending) != 0 {
		t.Errorf("got %d pending entries, want none", len(delivery.Pending))
	}
	// The identifier of the group moved anyway.
	if !delivery.LastIDChanged || delivery.LastID.MS != 2 {
		t.Errorf("got the last id %v, want 2-0", delivery.LastID)
	}
}

func TestStreamAck(t *testing.T) {
	s := streamWithEntries(t, 1, 2, 3)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupRead([]byte("s"), "g", "c", StreamReadNew, StreamID{}, 0, false); err != nil {
		t.Fatal(err)
	}
	n, err := s.StreamAck([]byte("s"), "g", []StreamID{{MS: 1}, {MS: 9}})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("got %d acknowledged, want 1", n)
	}
	info, _, _ := s.StreamPendingInfo([]byte("s"), "g")
	if info.Count != 2 {
		t.Errorf("got %d pending entries, want 2", info.Count)
	}
	// A missing group acknowledges nothing.
	n, err = s.StreamAck([]byte("s"), "nosuch", []StreamID{{MS: 1}})
	if err != nil || n != 0 {
		t.Errorf("got %d %v, want 0 nil", n, err)
	}
}

func TestStreamClaim(t *testing.T) {
	s := streamWithEntries(t, 1, 2, 3)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupRead([]byte("s"), "g", "c1", StreamReadNew, StreamID{}, 0, false); err != nil {
		t.Fatal(err)
	}
	res, err := s.StreamClaim([]byte("s"), "g", "c2", 0, []StreamID{{MS: 1}}, StreamClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(res.Entries))
	}
	// The entry belongs to the new consumer now.
	info, _, _ := s.StreamPendingInfo([]byte("s"), "g")
	if len(info.Consumers) != 2 || info.Consumers[0].Name != "c1" || info.Consumers[0].Count != 2 {
		t.Errorf("got the consumers %+v, want c1 with 2 entries", info.Consumers)
	}
	// An entry that is not pending is not claimed without FORCE.
	res, err = s.StreamClaim([]byte("s"), "g", "c2", 0, []StreamID{{MS: 9}}, StreamClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("got %d entries, want none", len(res.Entries))
	}
	// With FORCE the entry of the stream becomes pending.
	res, err = s.StreamClaim([]byte("s"), "g", "c2", 0, []StreamID{{MS: 2}}, StreamClaimOptions{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pending) != 1 {
		t.Errorf("got %d pending entries, want 1", len(res.Pending))
	}
	// The idle time keeps the entry away.
	res, err = s.StreamClaim([]byte("s"), "g", "c2", time.Hour, []StreamID{{MS: 3}}, StreamClaimOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("got %d entries, want none", len(res.Entries))
	}
}

func TestStreamAutoClaim(t *testing.T) {
	s := streamWithEntries(t, 1, 2, 3)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupRead([]byte("s"), "g", "c1", StreamReadNew, StreamID{}, 0, false); err != nil {
		t.Fatal(err)
	}
	res, err := s.StreamAutoClaim([]byte("s"), "g", "c2", 0, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(res.Entries))
	}
	// The entries belong to the new consumer now.
	info, _, _ := s.StreamPendingInfo([]byte("s"), "g")
	if len(info.Consumers) != 1 || info.Consumers[0].Name != "c2" || info.Consumers[0].Count != 3 {
		t.Errorf("got the consumers %+v, want c2 with 3 entries", info.Consumers)
	}
	// The entries are still pending: another claim takes them over.
	res, err = s.StreamAutoClaim([]byte("s"), "g", "c3", 0, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(res.Entries))
	}
	info, _, _ = s.StreamPendingInfo([]byte("s"), "g")
	if len(info.Consumers) != 1 || info.Consumers[0].Name != "c3" {
		t.Errorf("got the consumers %+v, want c3", info.Consumers)
	}
	// The entries that are gone from the stream are dropped from the pending
	// list.
	if _, err := s.StreamDelete([]byte("s"), []StreamID{{MS: 1}, {MS: 2}, {MS: 3}}); err != nil {
		t.Fatal(err)
	}
	res, err = s.StreamAutoClaim([]byte("s"), "g", "c4", 0, StreamID{}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 3 {
		t.Errorf("got %d deleted entries, want 3", len(res.Deleted))
	}
}

func TestStreamPendingRange(t *testing.T) {
	s := streamWithEntries(t, 1, 2, 3)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupRead([]byte("s"), "g", "c", StreamReadNew, StreamID{}, 0, false); err != nil {
		t.Fatal(err)
	}
	entries, ok, err := s.StreamPendingRange([]byte("s"), "g", StreamID{}, StreamMaxID, 0, "", 0)
	if err != nil || !ok {
		t.Fatalf("StreamPendingRange: %v %v", ok, err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if entries[0].ID.MS != 1 || entries[2].ID.MS != 3 {
		t.Errorf("got the entries %v and %v, want 1-0 and 3-0", entries[0].ID, entries[2].ID)
	}
	// The range is inclusive.
	entries, _, _ = s.StreamPendingRange([]byte("s"), "g", StreamID{MS: 2}, StreamID{MS: 2}, 0, "", 0)
	if len(entries) != 1 {
		t.Errorf("got %d entries, want 1", len(entries))
	}
	// The count limits the number of entries.
	entries, _, _ = s.StreamPendingRange([]byte("s"), "g", StreamID{}, StreamMaxID, 2, "", 0)
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}
	// An unknown consumer has no pending entry.
	entries, _, _ = s.StreamPendingRange([]byte("s"), "g", StreamID{}, StreamMaxID, 0, "nosuch", 0)
	if len(entries) != 0 {
		t.Errorf("got %d entries, want none", len(entries))
	}
}

func TestStreamGroupDelConsumer(t *testing.T) {
	s := streamWithEntries(t, 1, 2)
	if _, err := s.StreamGroupCreate([]byte("s"), "g", StreamID{}, 0, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StreamGroupRead([]byte("s"), "g", "c", StreamReadNew, StreamID{}, 0, false); err != nil {
		t.Fatal(err)
	}
	pending, err := s.StreamGroupDelConsumer([]byte("s"), "g", "c")
	if err != nil {
		t.Fatal(err)
	}
	if pending != 2 {
		t.Errorf("got %d pending entries, want 2", pending)
	}
	// The entries are gone from the pending list of the group.
	info, _, _ := s.StreamPendingInfo([]byte("s"), "g")
	if info.Count != 0 {
		t.Errorf("got %d pending entries, want none", info.Count)
	}
	// A missing consumer has nothing pending.
	pending, err = s.StreamGroupDelConsumer([]byte("s"), "g", "nosuch")
	if err != nil || pending != 0 {
		t.Errorf("got %d %v, want 0 nil", pending, err)
	}
}

func TestParseStreamGroupArgs(t *testing.T) {
	cmd, err := ParseStreamGroupArgs([][]byte{[]byte("CREATE"), []byte("s"), []byte("g"), []byte("5-0"), []byte("MKSTREAM")})
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Sub != "CREATE" || cmd.Group != "g" || !cmd.MkStream || cmd.ID.MS != 5 {
		t.Errorf("got %+v", cmd)
	}
	// The "$" identifier is recognized.
	cmd, err = ParseStreamGroupArgs([][]byte{[]byte("CREATE"), []byte("s"), []byte("g"), []byte("$")})
	if err != nil || !cmd.IDIsDollar {
		t.Errorf("got %+v %v", cmd, err)
	}
	// An unknown subcommand is an error.
	if _, err := ParseStreamGroupArgs([][]byte{[]byte("NOPE"), []byte("s"), []byte("g"), []byte("0")}); err == nil {
		t.Error("an unknown subcommand must be an error")
	}
}

func TestParseStreamClaimArgs(t *testing.T) {
	cmd, err := ParseStreamClaimArgs([][]byte{
		[]byte("s"), []byte("g"), []byte("c"), []byte("0"), []byte("1-0"), []byte("2-0"),
		[]byte("FORCE"), []byte("JUSTID"), []byte("RETRYCOUNT"), []byte("3"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cmd.IDs) != 2 || !cmd.Options.Force || !cmd.Options.JustID || cmd.Options.RetryCount != 3 {
		t.Errorf("got %+v", cmd)
	}
}
