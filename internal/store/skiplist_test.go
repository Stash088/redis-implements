package store

import "testing"

func TestSkiplistInsertOrder(t *testing.T) {
	sl := newSkiplist()
	sl.Insert(3, []byte("c"))
	sl.Insert(1, []byte("a"))
	sl.Insert(2, []byte("b"))
	sl.Insert(2, []byte("aa")) // same score, lexicographic member order

	if sl.length != 4 {
		t.Fatalf("length: got %d, want 4", sl.length)
	}
	wantMembers := []string{"a", "aa", "b", "c"}
	for i, want := range wantMembers {
		node := sl.nodeByRank(int64(i + 1))
		if node == nil || string(node.member) != want {
			t.Errorf("rank %d: got %v, want %s", i, node, want)
		}
	}
}

func TestSkiplistRank(t *testing.T) {
	sl := newSkiplist()
	sl.Insert(5, []byte("e"))
	sl.Insert(1, []byte("a"))
	sl.Insert(3, []byte("c"))
	if r := sl.GetRank(3, []byte("c")); r != 2 {
		t.Errorf("rank of c: got %d, want 2", r)
	}
	if r := sl.GetRank(5, []byte("e")); r != 3 {
		t.Errorf("rank of e: got %d, want 3", r)
	}
	if r := sl.GetRank(9, []byte("zz")); r != 0 {
		t.Errorf("missing: got %d, want 0", r)
	}
}

func TestSkiplistDelete(t *testing.T) {
	sl := newSkiplist()
	sl.Insert(1, []byte("a"))
	sl.Insert(2, []byte("b"))
	sl.Insert(2, []byte("bb"))
	sl.Insert(3, []byte("c"))

	if !sl.Delete(2, []byte("b")) {
		t.Fatalf("expected delete to succeed")
	}
	if sl.length != 3 {
		t.Fatalf("length after delete: got %d, want 3", sl.length)
	}
	if sl.GetRank(2, []byte("bb")) != 2 {
		t.Errorf("bb should now be rank 2")
	}
	if sl.Delete(2, []byte("b")) {
		t.Errorf("second delete should fail")
	}
	if !sl.Delete(1, []byte("a")) {
		t.Errorf("delete a failed")
	}
	// tail should be updated correctly
	if sl.tail == nil || string(sl.tail.member) != "c" {
		t.Errorf("tail should be c")
	}
}

func TestSkiplistInsertUpdateLevel(t *testing.T) {
	// many inserts to exercise higher levels
	sl := newSkiplist()
	for i := 0; i < 100; i++ {
		sl.Insert(float64(i), []byte{byte(i)})
	}
	if sl.length != 100 {
		t.Fatalf("length: got %d", sl.length)
	}
	for i := 0; i < 100; i++ {
		node := sl.nodeByRank(int64(i + 1))
		if node == nil || node.score != float64(i) {
			t.Errorf("rank %d: got %v", i, node)
		}
	}
	// delete from both ends
	sl.Delete(0, []byte{0})
	sl.Delete(99, []byte{99})
	if sl.length != 98 {
		t.Errorf("length after deletes: got %d", sl.length)
	}
	if node := sl.nodeByRank(1); node.score != 1 {
		t.Errorf("head after delete: got %v", node)
	}
}

func TestZSetAddUpdateScore(t *testing.T) {
	z := newZSet()
	if !z.Add(1, []byte("m")) {
		t.Errorf("expected new member")
	}
	if z.Add(2, []byte("m")) {
		t.Errorf("expected update, not new")
	}
	if s, ok := z.Score([]byte("m")); !ok || s != 2 {
		t.Errorf("score: got %v, %v", s, ok)
	}
	if z.Len() != 1 {
		t.Errorf("len: got %d", z.Len())
	}
}

func TestZSetRange(t *testing.T) {
	z := newZSet()
	z.Add(3, []byte("c"))
	z.Add(1, []byte("a"))
	z.Add(2, []byte("b"))

	got := z.Range(0, -1, false)
	want := []string{"a", "b", "c"}
	if len(got) != 3 {
		t.Fatalf("range: got %d entries", len(got))
	}
	for i := range want {
		if string(got[i].Member) != want[i] {
			t.Errorf("pos %d: got %s, want %s", i, got[i].Member, want[i])
		}
	}
	// subrange
	got = z.Range(1, 1, false)
	if len(got) != 1 || string(got[0].Member) != "b" {
		t.Errorf("subrange: got %v", got)
	}
	// reverse
	got = z.Range(0, -1, true)
	if string(got[0].Member) != "c" || string(got[2].Member) != "a" {
		t.Errorf("reverse: got %v", got)
	}
	// out of range
	if got = z.Range(5, 10, false); len(got) != 0 {
		t.Errorf("expected empty")
	}
}

func TestZSetRemove(t *testing.T) {
	z := newZSet()
	z.Add(1, []byte("a"))
	z.Add(2, []byte("b"))
	if !z.Remove([]byte("a")) {
		t.Errorf("remove should succeed")
	}
	if _, ok := z.Score([]byte("a")); ok {
		t.Errorf("a should be gone")
	}
	if z.Len() != 1 {
		t.Errorf("len: got %d", z.Len())
	}
}

func TestZSetRank(t *testing.T) {
	z := newZSet()
	z.Add(10, []byte("high"))
	z.Add(5, []byte("mid"))
	z.Add(1, []byte("low"))

	if r, ok := z.Rank([]byte("low"), false); !ok || r != 0 {
		t.Errorf("rank low: got %d, %v", r, ok)
	}
	if r, ok := z.Rank([]byte("high"), false); !ok || r != 2 {
		t.Errorf("rank high: got %d, %v", r, ok)
	}
	if r, ok := z.Rank([]byte("low"), true); !ok || r != 2 {
		t.Errorf("revrank low: got %d, %v", r, ok)
	}
	if _, ok := z.Rank([]byte("zz"), false); ok {
		t.Errorf("missing member should not have rank")
	}
}

func TestZSetStore(t *testing.T) {
	s := New()
	if added, err := s.ZAdd([]byte("z"), 2, []byte("b")); err != nil || added != 1 {
		t.Errorf("ZAdd: got %d, %v", added, err)
	}
	if added, _ := s.ZAdd([]byte("z"), 1, []byte("a")); added != 1 {
		t.Errorf("ZAdd a: got %d", added)
	}
	if n, ok, _ := s.ZCard([]byte("z")); !ok || n != 2 {
		t.Errorf("ZCard: got %d, %v", n, ok)
	}
	if score, ok, _ := s.ZScore([]byte("z"), []byte("b")); !ok || score != 2 {
		t.Errorf("ZScore: got %v, %v", score, ok)
	}
	entries, err := s.ZRange([]byte("z"), 0, -1, false)
	if err != nil || len(entries) != 2 {
		t.Fatalf("ZRange: got %v, %v", entries, err)
	}
	if string(entries[0].Member) != "a" || entries[0].Score != 1 {
		t.Errorf("first entry: %+v", entries[0])
	}
	if removed, _ := s.ZRem([]byte("z"), []byte("a"), []byte("z")); removed != 1 {
		t.Errorf("ZRem: got %d, want 1", removed)
	}
	if removed, _ := s.ZRem([]byte("z"), []byte("b")); removed != 1 {
		t.Errorf("ZRem b: got %d", removed)
	}
	if s.Exists([]byte("z")) != 0 {
		t.Errorf("expected key deleted when zset emptied")
	}
}
