package replay_test

import (
	"bytes"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/replay"
	"github.com/amir/redis-implements/internal/store"
)

func apply(t *testing.T, a *replay.Applier, args ...string) {
	t.Helper()
	cmd := make([][]byte, len(args))
	for i, arg := range args {
		cmd[i] = []byte(arg)
	}
	if err := a.Apply(cmd); err != nil {
		t.Fatalf("Apply(%q): %v", args, err)
	}
}

func TestApplierDatabases(t *testing.T) {
	dbs := []*store.Store{store.New(), store.New(), store.New()}
	a := replay.NewApplier(dbs, nil)

	apply(t, a, "SET", "zero", "0")
	apply(t, a, "SELECT", "1")
	if a.DB() != 1 {
		t.Fatalf("DB = %d, want 1", a.DB())
	}
	apply(t, a, "SET", "one", "1")
	apply(t, a, "COPY", "one", "copied", "DB", "2")
	apply(t, a, "MOVE", "one", "2")
	apply(t, a, "SELECT", "2")

	if n := dbs[0].Exists([]byte("zero")); n != 1 {
		t.Errorf("db0.zero exists = %d, want 1", n)
	}
	if n := dbs[1].Exists([]byte("one")); n != 0 {
		t.Errorf("db1.one exists = %d after MOVE, want 0", n)
	}
	if n := dbs[2].Exists([]byte("one"), []byte("copied")); n != 2 {
		t.Errorf("db2 keys exist = %d, want 2", n)
	}

	apply(t, a, "FLUSHALL")
	for i, db := range dbs {
		if db.Len() != 0 {
			t.Errorf("db%d has %d keys after FLUSHALL", i, db.Len())
		}
	}

	apply(t, a, "SELECT", "0")
	apply(t, a, "SET", "a", "A")
	apply(t, a, "SELECT", "1")
	apply(t, a, "SET", "b", "B")
	calls := 0
	a.SetSwapDBs(func(stores []*store.Store, x, y int) []*store.Store {
		calls++
		stores[x], stores[y] = stores[y], stores[x]
		return stores
	})
	apply(t, a, "SWAPDB", "0", "1")
	if calls != 1 {
		t.Fatalf("swap hook calls = %d, want 1", calls)
	}
	if v, ok, _ := dbs[0].GetString([]byte("b")); !ok || string(v) != "B" {
		t.Errorf("db0.b = %q ok=%v, want B", v, ok)
	}
	if v, ok, _ := dbs[1].GetString([]byte("a")); !ok || string(v) != "A" {
		t.Errorf("db1.a = %q ok=%v, want A", v, ok)
	}

	// Unknown/unsupported commands stay permissive.
	apply(t, a, "NOT-A-COMMAND", "x")
}

func TestApplierRestoreDecoder(t *testing.T) {
	db := store.New()
	injected := errors.New("bad payload")
	called := 0
	a := replay.NewApplier([]*store.Store{db}, func(data []byte) (*store.Entry, error) {
		called++
		if string(data) == "broken" {
			return nil, injected
		}
		return &store.Entry{Type: store.TypeString, Str: append([]byte(nil), data...)}, nil
	})

	apply(t, a, "RESTORE", "bad", "0", "broken")
	if called != 1 {
		t.Fatalf("decoder calls = %d, want 1", called)
	}
	if n := db.Exists([]byte("bad")); n != 0 {
		t.Errorf("a rejected RESTORE created the key")
	}

	apply(t, a, "RESTORE", "good", "2500", "value")
	v, ok, err := db.GetString([]byte("good"))
	if err != nil || !ok || string(v) != "value" {
		t.Fatalf("restored value = %q ok=%v err=%v", v, ok, err)
	}
	if ttl, ok := db.TTL([]byte("good")); !ok || ttl <= 0 || ttl > 2500*time.Millisecond {
		t.Errorf("restored TTL = %v ok=%v, want (0, 2.5s]", ttl, ok)
	}
}

func TestApplierHyperLogLogRawAndNormalized(t *testing.T) {
	db := store.New()
	a := replay.NewApplier([]*store.Store{db}, nil)

	// Raw forms stay replayable for streams recorded by Redis.
	apply(t, a, "PFADD", "source", "a", "b")
	apply(t, a, "PFCOUNT", "source")
	apply(t, a, "PFMERGE", "merged", "source")
	if card, _, _, err := db.PFCountKey([]byte("merged")); err != nil || card != 2 {
		t.Fatalf("PFCOUNT merged = %d err=%v, want 2", card, err)
	}

	// The normalized form is the resulting HLL string in a SET.
	fixture, err := os.ReadFile("../store/testdata/hll_sparse_valid.bin")
	if err != nil {
		t.Fatalf("read HLL fixture: %v", err)
	}
	expires := time.Now().Add(time.Minute)
	if err := a.Apply([][]byte{[]byte("SET"), []byte("normalized"), fixture,
		[]byte("PXAT"), []byte(strconv.FormatInt(expires.UnixMilli(), 10))}); err != nil {
		t.Fatalf("Apply normalized SET: %v", err)
	}
	v, ok, err := db.GetString([]byte("normalized"))
	if err != nil || !ok || !bytes.Equal(v, fixture) {
		t.Fatalf("normalized HLL bytes changed: ok=%v err=%v", ok, err)
	}
	if card, _, _, err := db.PFCountKey([]byte("normalized")); err != nil || card != 10 {
		t.Errorf("PFCOUNT normalized = %d err=%v, want 10", card, err)
	}
	if _, ok := db.TTL([]byte("normalized")); !ok {
		t.Error("normalized HLL lost its expiry")
	}
}
