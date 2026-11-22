package store

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func readHLLFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func mustString(t *testing.T, st *Store, key string) []byte {
	t.Helper()
	v, ok, err := st.GetString([]byte(key))
	if err != nil || !ok {
		t.Fatalf("GET %s: ok=%v err=%v", key, ok, err)
	}
	return v
}

type hllWatcher struct {
	dirty int
}

func (w *hllWatcher) MarkDirty() { w.dirty++ }

func TestHLLPFAddMatchesRedisSparse(t *testing.T) {
	st := New()
	wantInvalid := readHLLFixture(t, "hll_sparse_invalid.bin")
	wantValid := readHLLFixture(t, "hll_sparse_valid.bin")

	elements := [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"),
		[]byte("f"), []byte("g"), []byte("h"), []byte("i"), []byte("j"),
	}
	changed, err := st.PFAdd([]byte("h"), elements...)
	if err != nil || !changed {
		t.Fatalf("PFAdd: changed=%v err=%v", changed, err)
	}
	got := mustString(t, st, "h")
	if !bytes.Equal(got, wantInvalid) {
		t.Fatalf("PFADD bytes differ from Redis:\n got %x\nwant %x", got, wantInvalid)
	}
	if got[4] != hllSparseEncoding {
		t.Fatalf("encoding = %d, want sparse", got[4])
	}

	card, cacheUpdated, exists, err := st.PFCountKey([]byte("h"))
	if err != nil || !exists || !cacheUpdated || card != 10 {
		t.Fatalf("PFCountKey: card=%d updated=%v exists=%v err=%v", card, cacheUpdated, exists, err)
	}
	got = mustString(t, st, "h")
	if !bytes.Equal(got, wantValid) {
		t.Fatalf("cached bytes differ from Redis:\n got %x\nwant %x", got, wantValid)
	}

	changed, err = st.PFAdd([]byte("h"), elements...)
	if err != nil || changed {
		t.Fatalf("repeated PFAdd: changed=%v err=%v", changed, err)
	}
	if got := mustString(t, st, "h"); !bytes.Equal(got, wantValid) {
		t.Fatalf("a no-op PFADD changed the value")
	}
}

func TestHLLDenseFixtureAndEstimator(t *testing.T) {
	st := New()
	const n = 5000
	for i := 1; i <= n; i++ {
		if _, err := st.PFAdd([]byte("d"), []byte(fmt.Sprintf("elem-%d", i))); err != nil {
			t.Fatalf("PFAdd %d: %v", i, err)
		}
	}
	v := mustString(t, st, "d")
	if len(v) != hllDenseSize || v[4] != hllDenseEncoding {
		t.Fatalf("after %d elements: len=%d encoding=%d, want dense %d", n, len(v), v[4], hllDenseSize)
	}
	card, cacheUpdated, _, err := st.PFCountKey([]byte("d"))
	if err != nil || !cacheUpdated {
		t.Fatalf("PFCountKey: card=%d updated=%v err=%v", card, cacheUpdated, err)
	}
	if card != 5052 {
		t.Fatalf("PFCountKey = %d, want 5052 (Redis 8.2)", card)
	}
	want := readHLLFixture(t, "hll_dense.bin")
	if got := mustString(t, st, "d"); !bytes.Equal(got, want) {
		t.Fatalf("dense bytes differ from Redis:\n got %x\nwant %x", got, want)
	}
}

func TestHLLCountMultiAndMissing(t *testing.T) {
	st := New()
	if _, err := st.PFAdd([]byte("a"), []byte("x"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PFAdd([]byte("b"), []byte("y"), []byte("z")); err != nil {
		t.Fatal(err)
	}
	card, err := st.PFCount([]byte("a"), []byte("b"))
	if err != nil || card != 3 {
		t.Fatalf("PFCount union = %d, err=%v; want 3", card, err)
	}
	card, err = st.PFCount([]byte("a"), []byte("missing"))
	if err != nil || card != 2 {
		t.Fatalf("PFCount a+missing = %d, err=%v; want 2", card, err)
	}
	if card, err = st.PFCount([]byte("missing")); err != nil || card != 0 {
		t.Fatalf("PFCount missing = %d, err=%v; want 0", card, err)
	}
}

func TestHLLMergeAliasAndTTL(t *testing.T) {
	st := New()
	if _, err := st.PFAdd([]byte("a"), []byte("x"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PFAdd([]byte("b"), []byte("y"), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if !st.Expire([]byte("a"), time.Hour) {
		t.Fatal("Expire")
	}
	if err := st.PFMerge([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("PFMerge: %v", err)
	}
	card, _, _, err := st.PFCountKey([]byte("a"))
	if err != nil || card != 3 {
		t.Fatalf("merged count = %d, err=%v; want 3", card, err)
	}
	if ttl, ok := st.TTL([]byte("a")); !ok || ttl <= 0 {
		t.Fatalf("PFMERGE dropped the destination TTL: ttl=%v ok=%v", ttl, ok)
	}

	// The destination is also an input: aliasing must not deadlock or lose
	// registers.
	if err := st.PFMerge([]byte("a"), []byte("a")); err != nil {
		t.Fatalf("PFMerge alias: %v", err)
	}
	if card, _, _, err = st.PFCountKey([]byte("a")); err != nil || card != 3 {
		t.Fatalf("count after alias merge = %d, err=%v; want 3", card, err)
	}

	// A missing destination is created; missing sources are empty HLLs.
	if err := st.PFMerge([]byte("c"), []byte("a"), []byte("missing")); err != nil {
		t.Fatalf("PFMerge new destination: %v", err)
	}
	if card, _, _, err = st.PFCountKey([]byte("c")); err != nil || card != 3 {
		t.Fatalf("new destination count = %d, err=%v; want 3", card, err)
	}
}

func TestHLLMergeUsesOneDestinationClockRead(t *testing.T) {
	t0 := time.Unix(1000, 0)
	t1 := t0.Add(time.Second)
	t2 := t0.Add(2 * time.Second)

	st := New()
	st.SetClock(func() time.Time { return t0 })
	if _, err := st.PFAdd([]byte("dest"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PFAdd([]byte("src"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if !st.ExpireAt([]byte("dest"), t1) {
		t.Fatal("ExpireAt dest")
	}

	// The first clock read sees the live destination. The second read is past
	// its TTL; PFMERGE must not use it to decide that the destination is
	// missing (which would include its registers but drop its expiry).
	calls := 0
	st.SetClock(func() time.Time {
		calls++
		if calls == 1 {
			return t0
		}
		return t2
	})
	m, err := st.PFMergeValue([]byte("dest"), []byte("src"))
	if err != nil {
		t.Fatalf("PFMergeValue: %v", err)
	}
	if !m.ExpiresAt.Equal(t1) {
		t.Fatalf("PFMERGE expiry = %v, want %v (captured from the first scan)", m.ExpiresAt, t1)
	}
}

func TestHLLValidateBeforeMergeMutation(t *testing.T) {
	st := New()
	good := readHLLFixture(t, "hll_sparse_invalid.bin")
	corrupt := append([]byte(nil), good[:len(good)-1]...) // drop the final opcode

	if _, err := st.PFAdd([]byte("dest"), []byte("keep")); err != nil {
		t.Fatal(err)
	}
	st.Expire([]byte("dest"), time.Hour)
	before := append([]byte(nil), mustString(t, st, "dest")...)
	st.SetString([]byte("bad"), corrupt)

	if err := st.PFMerge([]byte("dest"), []byte("bad")); err != ErrHLLCorrupt {
		t.Fatalf("PFMerge corrupt source error = %v, want ErrHLLCorrupt", err)
	}
	if got := mustString(t, st, "dest"); !bytes.Equal(got, before) {
		t.Fatalf("destination changed after a failed PFMERGE")
	}
	if ttl, ok := st.TTL([]byte("dest")); !ok || ttl <= 0 {
		t.Fatalf("destination TTL changed after a failed PFMERGE: ttl=%v ok=%v", ttl, ok)
	}

	if _, err := st.PFAdd([]byte("bad"), []byte("x")); err != ErrHLLCorrupt {
		t.Fatalf("PFAdd corrupt value error = %v, want ErrHLLCorrupt", err)
	}
	if _, err := st.PFCount([]byte("dest"), []byte("bad")); err != ErrHLLCorrupt {
		t.Fatalf("PFCount corrupt value error = %v, want ErrHLLCorrupt", err)
	}
}

func TestHLLWatchAndCacheSemantics(t *testing.T) {
	st := New()
	if _, err := st.PFAdd([]byte("h"), []byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	st.Expire([]byte("h"), time.Hour)

	w := &hllWatcher{}
	st.AddWatch([]byte("h"), w)

	// PFADD invalidated the cache: the first read refreshes it and must mark
	// the key as modified.
	card, updated, _, err := st.PFCountKey([]byte("h"))
	if err != nil || !updated || card != 2 {
		t.Fatalf("first PFCountKey: card=%d updated=%v err=%v", card, updated, err)
	}
	if w.dirty != 1 {
		t.Fatalf("cache refresh marked dirty %d times, want 1", w.dirty)
	}
	if ttl, ok := st.TTL([]byte("h")); !ok || ttl <= 0 {
		t.Fatalf("cache refresh dropped the TTL: ttl=%v ok=%v", ttl, ok)
	}

	w.dirty = 0
	if _, updated, _, err = st.PFCountKey([]byte("h")); err != nil || updated {
		t.Fatalf("second PFCountKey: updated=%v err=%v", updated, err)
	}
	if w.dirty != 0 {
		t.Fatalf("a cached PFCOUNT marked dirty %d times", w.dirty)
	}

	if changed, err := st.PFAdd([]byte("h"), []byte("a")); err != nil || changed {
		t.Fatalf("no-op PFAdd: changed=%v err=%v", changed, err)
	}
	if w.dirty != 0 {
		t.Fatalf("a no-op PFADD marked dirty %d times", w.dirty)
	}
	if changed, err := st.PFAdd([]byte("h"), []byte("c")); err != nil || !changed {
		t.Fatalf("effective PFAdd: changed=%v err=%v", changed, err)
	}
	if w.dirty != 1 {
		t.Fatalf("effective PFADD marked dirty %d times, want 1", w.dirty)
	}
}

func TestHLLCopyOnWrite(t *testing.T) {
	st := New()
	if _, err := st.PFAdd([]byte("h"), []byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	old := mustString(t, st, "h")
	snapshot := append([]byte(nil), old...)

	if _, err := st.PFAdd([]byte("h"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, snapshot) {
		t.Fatal("PFADD modified the previously published buffer in place")
	}

	old = mustString(t, st, "h")
	snapshot = append([]byte(nil), old...)
	if _, _, _, err := st.PFCountKey([]byte("h")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, snapshot) {
		t.Fatal("the PFCOUNT cache refresh modified the published buffer in place")
	}
}

func TestHLLInvalidValues(t *testing.T) {
	valid := readHLLFixture(t, "hll_sparse_invalid.bin")
	dense := readHLLFixture(t, "hll_dense.bin")
	cases := map[string][]byte{
		"short":        []byte("HYLL"),
		"bad magic":    append([]byte("NOPE"), valid[4:]...),
		"bad encoding": append(append([]byte(nil), valid[:4]...), append([]byte{2}, valid[5:]...)...),
		"dense length": dense[:len(dense)-1],
	}
	for name, value := range cases {
		st := New()
		st.SetString([]byte("h"), value)
		if _, _, _, err := st.PFCountKey([]byte("h")); err != ErrHLLInvalid {
			t.Errorf("%s: PFCountKey error = %v, want ErrHLLInvalid", name, err)
		}
		if _, err := st.PFAdd([]byte("h"), []byte("x")); err != ErrHLLInvalid {
			t.Errorf("%s: PFAdd error = %v, want ErrHLLInvalid", name, err)
		}
	}

	// The dense representation must have the exact fixed length.
	st := New()
	st.SetString([]byte("d"), dense[:len(dense)-1])
	if _, err := st.PFAdd([]byte("d"), []byte("x")); err != ErrHLLInvalid {
		t.Errorf("short dense: PFAdd error = %v, want ErrHLLInvalid", err)
	}

	// A sparse value that claims 16383 registers instead of 16384 is
	// structurally corrupt, not a bad header.
	overrun := append([]byte(nil), valid...)
	overrun[hllHdrSize] = byte((16382 >> 8) | 0x40)
	overrun[hllHdrSize+1] = byte(16382 & 0xff)
	st = New()
	st.SetString([]byte("o"), overrun)
	if _, err := st.PFAdd([]byte("o"), []byte("x")); err != ErrHLLCorrupt {
		t.Errorf("sparse coverage: PFAdd error = %v, want ErrHLLCorrupt", err)
	}
	if _, err := st.PFCount([]byte("o")); err != ErrHLLCorrupt {
		t.Errorf("sparse coverage: PFCount error = %v, want ErrHLLCorrupt", err)
	}

	// A structurally corrupt sparse value (invalid cache) is reported
	// separately by the operations that walk the registers.
	st = New()
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-1] ^= 0xff
	st.SetString([]byte("c"), corrupt)
	if _, _, _, err := st.PFCountKey([]byte("c")); err != ErrHLLCorrupt {
		t.Errorf("PFCountKey corrupt sparse error = %v, want ErrHLLCorrupt", err)
	}
	if _, err := st.PFCount([]byte("c")); err != ErrHLLCorrupt {
		t.Errorf("PFCount corrupt sparse error = %v, want ErrHLLCorrupt", err)
	}
}

func TestHLLWrongType(t *testing.T) {
	st := New()
	st.ListPush([]byte("l"), false, []byte("x"))
	if _, err := st.PFAdd([]byte("l"), []byte("a")); err != ErrWrongType {
		t.Errorf("PFAdd on a list = %v, want ErrWrongType", err)
	}
	if _, _, _, err := st.PFCountKey([]byte("l")); err != ErrWrongType {
		t.Errorf("PFCountKey on a list = %v, want ErrWrongType", err)
	}
	if _, err := st.PFCount([]byte("l")); err != ErrWrongType {
		t.Errorf("PFCount on a list = %v, want ErrWrongType", err)
	}
	if err := st.PFMerge([]byte("d"), []byte("l")); err != ErrWrongType {
		t.Errorf("PFMerge source list = %v, want ErrWrongType", err)
	}
	st.ListPush([]byte("d"), false, []byte("y"))
	if err := st.PFMerge([]byte("d"), []byte("l")); err != ErrWrongType {
		t.Errorf("PFMerge destination list = %v, want ErrWrongType", err)
	}
}

func TestHLLEmptyCreation(t *testing.T) {
	st := New()
	changed, err := st.PFAdd([]byte("empty"))
	if err != nil || !changed {
		t.Fatalf("PFAdd new key without elements: changed=%v err=%v", changed, err)
	}
	want := hllNew()
	hllInvalidateCache(want)
	v := mustString(t, st, "empty")
	if !bytes.Equal(v, want) {
		t.Fatalf("empty HLL bytes = %x, want %x", v, want)
	}
	changed, err = st.PFAdd([]byte("empty"))
	if err != nil || changed {
		t.Fatalf("PFAdd empty existing key: changed=%v err=%v", changed, err)
	}
	if card, _, _, err := st.PFCountKey([]byte("empty")); err != nil || card != 0 {
		t.Fatalf("empty PFCountKey = %d, err=%v", card, err)
	}
}
