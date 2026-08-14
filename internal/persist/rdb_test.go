package persist

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

func roundTrip(t *testing.T, st *store.Store) *store.Store {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteRDB(&buf, st); err != nil {
		t.Fatalf("WriteRDB: %v", err)
	}
	got := store.New()
	if err := LoadBytes(buf.Bytes(), got); err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	return got
}

func buildStore(t *testing.T) *store.Store {
	t.Helper()
	st := store.New()
	st.SetString([]byte("str"), []byte("hello"))
	st.SetStringWithTTL([]byte("ttl"), []byte("expiring"), 10*time.Minute)
	st.SetString([]byte("big"), bytes.Repeat([]byte("x"), 70000)) // 64-bit length path
	st.ListPush([]byte("list"), false, []byte("a"), []byte("b"), []byte("c"))
	st.HashSet([]byte("hash"), []byte("f1"), []byte("v1"))
	st.HashSet([]byte("hash"), []byte("f2"), []byte("v2"))
	st.SetAdd([]byte("set"), []byte("m1"), []byte("m2"), []byte("m3"))
	st.ZAdd([]byte("zset"), 1.5, []byte("a"))
	st.ZAdd([]byte("zset"), 2.25, []byte("b"))
	return st
}

func TestRDBRoundTripAllTypes(t *testing.T) {
	src := buildStore(t)
	got := roundTrip(t, src)

	if v, ok, _ := got.GetString([]byte("str")); !ok || string(v) != "hello" {
		t.Errorf("str = %q, ok=%v", v, ok)
	}
	if d, ok := got.TTL([]byte("ttl")); !ok || d <= 0 || d > 10*time.Minute {
		t.Errorf("ttl = %v, ok=%v", d, ok)
	}
	if v, ok, _ := got.GetString([]byte("big")); !ok || len(v) != 70000 {
		t.Errorf("big len = %d, ok=%v", len(v), ok)
	}
	lr, _ := got.ListRange([]byte("list"), 0, -1)
	if len(lr) != 3 || string(lr[0]) != "a" || string(lr[2]) != "c" {
		t.Errorf("list = %q", lr)
	}
	if v, _, _ := got.HashGet([]byte("hash"), []byte("f2")); string(v) != "v2" {
		t.Errorf("hash f2 = %q", v)
	}
	members, _ := got.SetMembers([]byte("set"))
	if len(members) != 3 {
		t.Errorf("set members = %d", len(members))
	}
	z, _ := got.ZRange([]byte("zset"), 0, -1, false)
	if len(z) != 2 || string(z[0].Member) != "a" || z[0].Score != 1.5 || z[1].Score != 2.25 {
		t.Errorf("zset = %+v", z)
	}
}

func TestRDBEmptyStore(t *testing.T) {
	got := roundTrip(t, store.New())
	if got.Len() != 0 {
		t.Errorf("expected empty store, got %d keys", got.Len())
	}
}

func TestRDBExpiredKeysSkipped(t *testing.T) {
	st := store.New()
	st.SetStringWithExpiryAt([]byte("past"), []byte("gone"), time.Now().Add(-time.Minute))
	st.SetString([]byte("ok"), []byte("here"))
	got := roundTrip(t, st)
	if got.Len() != 1 {
		t.Fatalf("expected 1 key, got %d", got.Len())
	}
	if _, ok, _ := got.GetString([]byte("past")); ok {
		t.Error("expired key should not be restored")
	}
}

func TestRDBAtomicSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.rdb")
	src := buildStore(t)
	if err := Save(path, src); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		t.Fatalf("dump file missing/empty: %v", err)
	}
	got := store.New()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := Load(f, got); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if v, ok, _ := got.GetString([]byte("str")); !ok || string(v) != "hello" {
		t.Errorf("loaded str = %q, ok=%v", v, ok)
	}
}

func TestRDBBadHeader(t *testing.T) {
	st := store.New()
	if err := LoadBytes([]byte("NOPE..."), st); err == nil {
		t.Fatal("expected error for bad header")
	}
}

func TestRDBTruncated(t *testing.T) {
	var buf bytes.Buffer
	st := buildStore(t)
	if err := WriteRDB(&buf, st); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	for _, cut := range []int{1, len(data) / 2, len(data) - 8, len(data) - 1} {
		got := store.New()
		if err := LoadBytes(data[:cut], got); err == nil {
			t.Errorf("expected error for truncated dump (cut at %d/%d)", cut, len(data))
		}
	}
}

func TestRDBChecksumCorruption(t *testing.T) {
	var buf bytes.Buffer
	st := buildStore(t)
	if err := WriteRDB(&buf, st); err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), buf.Bytes()...)
	// flip a byte in the middle of the payload
	data[len(data)/2] ^= 0xFF
	if err := LoadBytes(data, store.New()); err == nil {
		t.Fatal("expected checksum mismatch error")
	}
}

func TestRDBChecksumZeroSkipsVerification(t *testing.T) {
	// Zero checksum must load fine (written with checksum disabled, like Redis).
	var buf bytes.Buffer
	st := buildStore(t)
	if err := WriteRDB(&buf, st); err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), buf.Bytes()...)
	copy(data[len(data)-8:], make([]byte, 8))
	got := store.New()
	if err := LoadBytes(data, got); err != nil {
		t.Fatalf("zero-checksum load: %v", err)
	}
	if got.Len() == 0 {
		t.Error("expected keys restored")
	}
}

func TestRDBZSetBinaryScores(t *testing.T) {
	st := store.New()
	if _, err := st.ZAdd([]byte("z"), -0.5, []byte("neg")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ZAdd([]byte("z"), math.MaxFloat64/2, []byte("big")); err != nil {
		t.Fatal(err)
	}
	got := roundTrip(t, st)
	z, _ := got.ZRange([]byte("z"), 0, -1, false)
	if len(z) != 2 {
		t.Fatalf("zset len = %d", len(z))
	}
	if z[0].Score != -0.5 || z[1].Score != math.MaxFloat64/2 {
		t.Errorf("scores = %v, %v", z[0].Score, z[1].Score)
	}
}

func TestRDBCorruptLength(t *testing.T) {
	// A stream that starts with a valid header but a truncated body.
	data := []byte("REDIS0009" + string(rune(rdbOpcodeSelectDB)) + "\x00" + string(rune(rdbTypeString)) + "\x05key\x03")
	if err := LoadBytes(data, store.New()); err == nil {
		t.Fatal("expected error for truncated value")
	}
}

func TestLoadEmptyReader(t *testing.T) {
	if err := Load(bytes.NewReader(nil), store.New()); err == nil {
		t.Fatal("expected error for empty input")
	}
}
