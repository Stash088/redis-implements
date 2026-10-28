package store

import (
	"fmt"
	"testing"
	"time"
)

// checkConsistency asserts that usedBytes equals the recomputed sum of entry
// costs in every shard (plus dangling-watch costs are impossible: all data is
// in the shards' maps).
func checkConsistency(t *testing.T, s *Store) {
	t.Helper()
	total := int64(0)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sum := 0
		for k, e := range sh.data {
			sum += s.entryCostLocked(k, e)
		}
		if sum != int(sh.usedBytes.Load()) {
			t.Fatalf("shard %d: usedBytes=%d, recomputed=%d", i, sh.usedBytes.Load(), sum)
		}
		total += sh.usedBytes.Load()
		sh.mu.Unlock()
	}
	// The total of the store must match the sum of the shards: the server
	// reads it on the path of every write command.
	if total != s.totalBytes.Load() {
		t.Fatalf("the store holds %d bytes, the shards hold %d", s.totalBytes.Load(), total)
	}
}

func TestUsedBytesAccounting(t *testing.T) {
	s := New()
	checkConsistency(t, s)

	s.SetString([]byte("a"), []byte("hello"))
	checkConsistency(t, s)
	if s.UsedBytes() != len("a")+len("hello") {
		t.Fatalf("string cost: got %d, want %d", s.UsedBytes(), len("a")+len("hello"))
	}

	// Overwrite: delta, not addition.
	s.SetString([]byte("a"), []byte("hi"))
	if s.UsedBytes() != len("a")+len("hi") {
		t.Fatalf("overwrite cost: got %d, want %d", s.UsedBytes(), len("a")+len("hi"))
	}

	// Collections.
	s.ListPush([]byte("l"), true, []byte("one"), []byte("two"))
	s.HashSet([]byte("h"), []byte("f"), []byte("v"))
	s.SetAdd([]byte("s"), []byte("m1"), []byte("m2"))
	s.ZAdd([]byte("z"), 1, []byte("member"))
	checkConsistency(t, s)

	// Pops and deletions subtract.
	if _, ok, _ := s.ListPop([]byte("l"), true); !ok {
		t.Fatal("ListPop failed")
	}
	s.HashDel([]byte("h"), []byte("f"))
	s.SetRem([]byte("s"), []byte("m1"))
	s.ZRem([]byte("z"), []byte("member"))
	checkConsistency(t, s)

	s.Delete([]byte("a"), []byte("l"), []byte("h"), []byte("s"), []byte("z"))
	if s.UsedBytes() != 0 {
		t.Fatalf("after delete: usedBytes=%d, want 0", s.UsedBytes())
	}

	// Flush resets the counter.
	s.SetString([]byte("x"), []byte("y"))
	s.Flush()
	if s.UsedBytes() != 0 {
		t.Fatalf("after flush: usedBytes=%d, want 0", s.UsedBytes())
	}

	// RestoreEntry (persistence loader path).
	s.RestoreEntry("r", &Entry{Type: TypeString, Str: []byte("value")})
	if s.UsedBytes() != len("r")+len("value") {
		t.Fatalf("after restore: usedBytes=%d, want %d", s.UsedBytes(), len("r")+len("value"))
	}
}

func TestEvictCycleLRU(t *testing.T) {
	now := time.Now()
	s := New()
	s.SetClock(func() time.Time { return now })

	s.SetString([]byte("old"), make([]byte, 100))
	now = now.Add(time.Second)
	s.SetString([]byte("mid"), make([]byte, 100))
	now = now.Add(time.Second)
	s.SetString([]byte("new"), make([]byte, 100))
	now = now.Add(time.Second)
	// Refresh "old" so "mid" becomes the LRU victim.
	s.GetString([]byte("old"))
	now = now.Add(time.Second)

	limit := int64(len("old") + 100 + len("mid") + 100 + len("new") + 100 - 50)
	n := s.EvictCycle("allkeys-lru", 5, limit)
	if n != 1 {
		t.Fatalf("evicted %d keys, want 1", n)
	}
	if s.Exists([]byte("mid")) == 1 {
		t.Error("mid should have been evicted (least recently used)")
	}
	for _, k := range []string{"old", "new"} {
		if s.Exists([]byte(k)) != 1 {
			t.Errorf("%s should survive", k)
		}
	}
	checkConsistency(t, s)
}

func TestEvictCycleVolatileTTL(t *testing.T) {
	now := time.Now()
	s := New()
	s.SetClock(func() time.Time { return now })

	s.SetString([]byte("perm"), make([]byte, 100)) // no TTL
	s.SetStringWithTTL([]byte("far"), make([]byte, 100), 10*time.Second)
	s.SetStringWithTTL([]byte("soon"), make([]byte, 100), time.Second)

	limit := int64(len("perm") + 100)
	n := s.EvictCycle("volatile-ttl", 5, limit)
	if n != 2 {
		t.Fatalf("evicted %d keys, want 2", n)
	}
	if s.Exists([]byte("soon")) != 0 || s.Exists([]byte("far")) != 0 {
		t.Error("TTL keys should be evicted (soonest first, but both over limit)")
	}
	if s.Exists([]byte("perm")) != 1 {
		t.Error("key without TTL must survive under volatile policy")
	}
	checkConsistency(t, s)
}

func TestEvictCycleVolatileNoCandidates(t *testing.T) {
	s := New()
	s.SetString([]byte("a"), make([]byte, 100))
	n := s.EvictCycle("volatile-lru", 5, 10)
	if n != 0 {
		t.Fatalf("evicted %d keys, want 0 (no TTL keys)", n)
	}
	if s.Exists([]byte("a")) != 1 {
		t.Error("key must survive when no volatile candidates exist")
	}
}

func TestEvictCycleRandom(t *testing.T) {
	s := New()
	for i := 0; i < 50; i++ {
		s.SetString([]byte(fmt.Sprintf("key%d", i)), make([]byte, 10))
	}
	total := s.UsedBytes()
	n := s.EvictCycle("allkeys-random", 5, int64(total-200))
	if n == 0 {
		t.Fatal("expected evictions")
	}
	if int64(s.UsedBytes()) > int64(total-200) {
		t.Errorf("still over limit: %d > %d", s.UsedBytes(), total-200)
	}
	checkConsistency(t, s)
}

func TestEvictCycleStopsWhenEmpty(t *testing.T) {
	s := New()
	s.SetString([]byte("only"), []byte("v"))
	n := s.EvictCycle("allkeys-lru", 5, 1)
	if n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if s.Len() != 0 || s.UsedBytes() != 0 {
		t.Errorf("store should be empty: len=%d bytes=%d", s.Len(), s.UsedBytes())
	}
}
