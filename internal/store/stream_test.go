package store

import (
	"testing"
	"time"
)

func TestParseStreamID(t *testing.T) {
	cases := []struct {
		in     string
		strict bool
		miss   uint64
		want   StreamID
		ok     bool
	}{
		{in: "1-2", want: StreamID{MS: 1, Seq: 2}, ok: true},
		{in: "1", want: StreamID{MS: 1}, ok: true},
		{in: "1", miss: 9, want: StreamID{MS: 1, Seq: 9}, ok: true},
		{in: "0-0", want: StreamID{}, ok: true},
		{in: "-", want: StreamID{}, ok: true},
		{in: "+", want: StreamMaxID, ok: true},
		{in: "-", strict: true, ok: false},
		{in: "+", strict: true, ok: false},
		{in: "abc", ok: false},
		{in: "1-2-3", ok: false},
		{in: "1-x", ok: false},
		{in: "-1", ok: false},
		{in: "01", ok: false},
		{in: "", ok: false},
	}
	for _, c := range cases {
		var (
			id  StreamID
			ok  bool
			err error
		)
		if c.strict {
			id, ok = ParseStreamIDStrict(c.in, c.miss, nil)
		} else {
			id, ok = ParseStreamID(c.in, c.miss)
		}
		if ok != c.ok {
			t.Errorf("ParseStreamID(%q): got ok=%v, want %v", c.in, ok, c.ok)
			continue
		}
		if ok && id != c.want {
			t.Errorf("ParseStreamID(%q): got %v, want %v", c.in, id, c.want)
		}
		_ = err
	}
}

func TestParseStreamIDSeqGiven(t *testing.T) {
	seqGiven := true
	if _, ok := ParseStreamIDStrict("5-3", 0, &seqGiven); !ok || !seqGiven {
		t.Errorf("5-3: ok=%v seqGiven=%v, want true true", ok, seqGiven)
	}
	seqGiven = true
	if _, ok := ParseStreamIDStrict("5-*", 0, &seqGiven); !ok || seqGiven {
		t.Errorf("5-*: ok=%v seqGiven=%v, want true false", ok, seqGiven)
	}
}

// fixedClock returns a clock that always reports the same instant.
func fixedClock(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

func TestStreamAddAndLen(t *testing.T) {
	s := New()
	s.SetClock(fixedClock(1000))
	id, err := s.StreamAdd([]byte("s"), StreamIDAuto, StreamID{}, [][]byte{[]byte("f"), []byte("v")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if id.MS != 1000 || id.Seq != 0 {
		t.Errorf("got %v, want 1000-0", id)
	}
	n, ok, err := s.StreamLen([]byte("s"))
	if err != nil || !ok || n != 1 {
		t.Errorf("StreamLen: got %d %v %v, want 1 true nil", n, ok, err)
	}
	// A second entry in the same millisecond increments the sequence.
	id, err = s.StreamAdd([]byte("s"), StreamIDAuto, StreamID{}, [][]byte{[]byte("f"), []byte("v2")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if id.MS != 1000 || id.Seq != 1 {
		t.Errorf("got %v, want 1000-1", id)
	}
	// A later millisecond starts the sequence over.
	s.SetClock(fixedClock(2000))
	id, err = s.StreamAdd([]byte("s"), StreamIDAuto, StreamID{}, [][]byte{[]byte("f"), []byte("v3")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if id.MS != 2000 || id.Seq != 0 {
		t.Errorf("got %v, want 2000-0", id)
	}
}

func TestStreamAddExplicitID(t *testing.T) {
	s := New()
	s.SetClock(fixedClock(1000))
	if _, err := s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: 5, Seq: 3}, [][]byte{[]byte("f"), []byte("v")}, false); err != nil {
		t.Fatal(err)
	}
	// An identifier that is not greater than the last one is refused.
	if _, err := s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: 5, Seq: 3}, nil, false); err != ErrStreamIDNotGreater {
		t.Errorf("got %v, want ErrStreamIDNotGreater", err)
	}
	// The "ms-*" form generates the sequence of the last entry.
	id, err := s.StreamAdd([]byte("s"), StreamIDAutoSeq, StreamID{MS: 5}, [][]byte{[]byte("f"), []byte("v")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if id.MS != 5 || id.Seq != 4 {
		t.Errorf("got %v, want 5-4", id)
	}
	// The "ms-*" form with an older millisecond is refused.
	if _, err := s.StreamAdd([]byte("s"), StreamIDAutoSeq, StreamID{MS: 4}, nil, false); err != ErrStreamIDNotGreater {
		t.Errorf("got %v, want ErrStreamIDNotGreater", err)
	}
}

func TestStreamAddNoMkStream(t *testing.T) {
	s := New()
	_, err := s.StreamAdd([]byte("s"), StreamIDAuto, StreamID{}, nil, true)
	if err != ErrStreamNotCreated {
		t.Errorf("got %v, want ErrStreamNotCreated", err)
	}
	if s.Exists([]byte("s")) != 0 {
		t.Error("the stream must not be created")
	}
}

func TestStreamAddWrongType(t *testing.T) {
	s := New()
	s.SetString([]byte("k"), []byte("v"))
	if _, err := s.StreamAdd([]byte("k"), StreamIDAuto, StreamID{}, nil, false); err != ErrWrongType {
		t.Errorf("got %v, want ErrWrongType", err)
	}
}

func TestStreamRange(t *testing.T) {
	s := New()
	for i := 1; i <= 5; i++ {
		if _, err := s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: uint64(i)}, [][]byte{[]byte("f"), []byte{byte('0' + i)}}, false); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := s.StreamRange([]byte("s"), StreamID{MS: 2}, StreamID{MS: 4}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if entries[0].ID.MS != 2 || entries[2].ID.MS != 4 {
		t.Errorf("got %v..%v, want 2..4", entries[0].ID, entries[2].ID)
	}
	// The count limits the number of entries.
	entries, err = s.StreamRange([]byte("s"), StreamID{MS: 1}, StreamID{MS: 5}, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].ID.MS != 1 {
		t.Errorf("got %d entries starting at %v, want 2 starting at 1", len(entries), entries[0].ID)
	}
	// The reverse order starts at the end of the range.
	entries, err = s.StreamRange([]byte("s"), StreamID{MS: 1}, StreamID{MS: 5}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 || entries[0].ID.MS != 5 {
		t.Errorf("got %d entries starting at %v, want 5 starting at 5", len(entries), entries[0].ID)
	}
	// A range that ends outside of the stream returns what exists.
	entries, err = s.StreamRange([]byte("s"), StreamID{MS: 4}, StreamID{MS: 99}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}
	// A missing stream has no entries.
	entries, err = s.StreamRange([]byte("nosuch"), StreamID{}, StreamMaxID, 0, false)
	if err != nil || len(entries) != 0 {
		t.Errorf("got %d entries, %v; want 0, nil", len(entries), err)
	}
}

func TestStreamDelete(t *testing.T) {
	s := New()
	for i := 1; i <= 3; i++ {
		s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: uint64(i)}, [][]byte{[]byte("f"), []byte("v")}, false) //nolint:errcheck
	}
	n, err := s.StreamDelete([]byte("s"), []StreamID{{MS: 2}, {MS: 9}})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("got %d removed, want 1", n)
	}
	length, _, _ := s.StreamLen([]byte("s"))
	if length != 2 {
		t.Errorf("got length %d, want 2", length)
	}
	// The highest deleted identifier is remembered.
	last, ok, err := s.StreamLastID([]byte("s"))
	if err != nil || !ok {
		t.Fatalf("StreamLastID: %v %v", ok, err)
	}
	if last.MS != 3 {
		t.Errorf("got last %v, want 3-0", last)
	}
}

func TestStreamTrim(t *testing.T) {
	s := New()
	for i := 1; i <= 5; i++ {
		s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: uint64(i)}, [][]byte{[]byte("f"), []byte("v")}, false) //nolint:errcheck
	}
	maxLen := uint64(2)
	n, err := s.StreamTrim([]byte("s"), StreamTrim{MaxLen: &maxLen})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("got %d removed, want 3", n)
	}
	length, _, _ := s.StreamLen([]byte("s"))
	if length != 2 {
		t.Errorf("got length %d, want 2", length)
	}
	// Trimming by identifier removes the older entries.
	minID := StreamID{MS: 5}
	n, err = s.StreamTrim([]byte("s"), StreamTrim{MinID: &minID})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("got %d removed, want 1", n)
	}
	// An approximate trim removes whole chunks only: the stream is short, so
	// nothing is removed.
	maxLen = 1
	n, err = s.StreamTrim([]byte("s"), StreamTrim{MaxLen: &maxLen, Approximate: true})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("got %d removed, want 0", n)
	}
	// The limit caps the number of removed entries.
	maxLen = 0
	n, err = s.StreamTrim([]byte("s"), StreamTrim{MaxLen: &maxLen, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("got %d removed, want 1", n)
	}
}

func TestStreamSetID(t *testing.T) {
	s := New()
	s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: 5}, [][]byte{[]byte("f"), []byte("v")}, false) //nolint:errcheck
	// An identifier below the top entry is refused.
	if err := s.StreamSetID([]byte("s"), StreamID{MS: 1}, 0, false, nil); err != ErrStreamIDNotGreater {
		t.Errorf("got %v, want ErrStreamIDNotGreater", err)
	}
	// A missing stream is reported.
	if err := s.StreamSetID([]byte("nosuch"), StreamID{MS: 1}, 0, false, nil); err != ErrStreamNotFound {
		t.Errorf("got %v, want ErrStreamNotFound", err)
	}
	// The identifier of the last entry is raised.
	if err := s.StreamSetID([]byte("s"), StreamID{MS: 50}, 10, true, nil); err != nil {
		t.Fatal(err)
	}
	last, _, _ := s.StreamLastID([]byte("s"))
	if last.MS != 50 {
		t.Errorf("got last %v, want 50-0", last)
	}
	// The number of entries added cannot be smaller than the length.
	if err := s.StreamSetID([]byte("s"), StreamID{MS: 60}, 0, true, nil); err != ErrStreamEntriesAddedTooSmall {
		t.Errorf("got %v, want ErrStreamEntriesAddedTooSmall", err)
	}
}

func TestStreamCopyEntry(t *testing.T) {
	s := New()
	s.StreamAdd([]byte("s"), StreamIDExplicit, StreamID{MS: 1}, [][]byte{[]byte("f"), []byte("v")}, false) //nolint:errcheck
	e, ok := s.EntryType([]byte("s"))
	if !ok || e != TypeStream {
		t.Fatalf("EntryType: got %v %v, want stream true", e, ok)
	}
	// The copy of a stream holds the same entries.
	copied := copyEntry(s.shardOfStr("s").data["s"])
	if copied.Stream.Len() != 1 {
		t.Errorf("got %d entries in the copy, want 1", copied.Stream.Len())
	}
	if copied.Stream.entries[0].Fields[1][0] != 'v' {
		t.Errorf("got %q, want v", copied.Stream.entries[0].Fields[1])
	}
}

func TestStreamEntryEncoding(t *testing.T) {
	s := New()
	s.StreamAdd([]byte("s"), StreamIDAuto, StreamID{}, nil, false) //nolint:errcheck
	name, ok := s.EntryEncoding([]byte("s"))
	if !ok || name != "stream" {
		t.Errorf("got %q %v, want stream true", name, ok)
	}
}
