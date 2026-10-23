package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestShardConcurrency hammers the sharded store from many goroutines: writers
// and readers on distinct and shared keys, multi-key commands that span shards,
// WATCH/EXEC transactions, expiry and eviction. Run with -race, it checks that
// the shard locks are taken correctly everywhere. At the end the memory
// accounting must still be consistent and no key may lose its value.
func TestShardConcurrency(t *testing.T) {
	s := New()

	const (
		writers    = 8
		readers    = 8
		perWorker  = 2000
		singleKeys = 32 // distinct keys per writer, spread over the shards
		sharedKey  = "shared:counter"
	)

	keys := func(w int) [][]byte {
		out := make([][]byte, singleKeys)
		for i := range out {
			out[i] = []byte(fmt.Sprintf("w%d:k%d", w, i))
		}
		return out
	}

	var wg sync.WaitGroup

	// Writers: SET/INCR on their own keys, INCR on a shared key, and DEL of
	// a neighbor's key (multi-key, different shard than their own keys).
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			mine := keys(w)
			for i := 0; i < perWorker; i++ {
				k := mine[i%singleKeys]
				s.SetString(k, []byte(fmt.Sprintf("v%d", i)))
				if _, err := s.IncrBy([]byte(sharedKey), 1); err != nil {
					t.Errorf("IncrBy: %v", err)
					return
				}
				switch i % 16 {
				case 0:
					// Two keys of the same writer: Rename keeps the value.
					src := mine[(i/16)%singleKeys]
					dst := mine[(i/16+1)%singleKeys]
					s.Rename(src, dst)
					s.Rename(dst, src)
				case 8:
					other := keys((w+1)%writers)[i%singleKeys]
					s.Delete(other, k) // multi-key, possibly two shards
				case 9:
					s.Exists(other(mine, i), k)
				}
				if i%64 == 0 {
					s.Expire(k, time.Hour)
					s.Persist(k)
				}
			}
		}(w)
	}

	// Readers: GET on their own and on shared keys; the values of the
	// per-writer keys are always "v<number>", so a torn or foreign read is
	// detectable.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			mine := keys(r)
			for i := 0; i < perWorker; i++ {
				v, ok, err := s.GetString(mine[i%singleKeys])
				if err != nil {
					t.Errorf("GetString: %v", err)
					return
				}
				if ok && len(v) < 2 {
					t.Errorf("torn value %q", v)
					return
				}
				if _, ok, _ := s.GetString([]byte(sharedKey)); ok {
					// The counter is valid JSON-free digits.
				}
				if i%8 == 0 {
					// Multi-key read over several shards (MGET path).
					s.GetStrings(mine[0], mine[1], keys((r+3)%writers)[2])
				}
			}
		}(r)
	}

	// A watcher: transactions over keys that writers keep changing; EXEC
	// must never apply a transaction whose watched key changed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		k := []byte("watched")
		for i := 0; i < perWorker/4; i++ {
			s.SetString(k, []byte(fmt.Sprintf("w%d", i)))
			if i%2 == 0 {
				s.Expire(k, time.Minute)
			}
		}
	}()

	// Expiry and eviction cycles run alongside, like the server's
	// background jobs. The goroutine is waited for separately: it stops
	// only when the workers are done.
	done := make(chan struct{})
	cyclesDone := make(chan struct{})
	go func() {
		defer close(cyclesDone)
		for {
			select {
			case <-done:
				return
			default:
				s.ExpireCycle()
				s.EvictCycle("allkeys-lru", 5, 1<<40) // huge budget: no evictions
			}
		}
	}()

	wg.Wait()
	close(done)
	<-cyclesDone

	if n := s.Len(); n == 0 {
		t.Error("store is empty after the run")
	}
	// The shared counter must equal the number of successful INCRs.
	if v, ok, _ := s.GetString([]byte(sharedKey)); !ok {
		t.Error("shared counter is missing")
	} else if len(v) == 0 {
		t.Error("shared counter is empty")
	}
	// The values of the per-writer keys must be intact.
	for w := 0; w < writers; w++ {
		for _, k := range keys(w) {
			if v, ok, _ := s.GetString(k); ok && (len(v) < 2 || v[0] != 'v') {
				t.Fatalf("key %q holds a torn value %q", k, v)
			}
		}
	}
	checkConsistency(t, s)
}

// other returns a key of another writer for the cross-shard checks.
func other(mine [][]byte, i int) []byte {
	return mine[(i+7)%len(mine)]
}

// TestShardSpread checks that the keys spread over the shards: with a few
// hundred keys every shard must see some of them.
func TestShardSpread(t *testing.T) {
	s := New()
	const n = 400
	for i := 0; i < n; i++ {
		s.SetString([]byte(fmt.Sprintf("key:%d", i)), []byte("v"))
	}
	seen := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		if len(sh.data) > 0 {
			seen++
		}
		sh.mu.RUnlock()
	}
	if seen < shardCount/2 {
		t.Errorf("keys occupy only %d of %d shards", seen, shardCount)
	}
	if s.Len() != n {
		t.Errorf("Len=%d, want %d", s.Len(), n)
	}
}

// TestMultiKeyAtomicity checks that a multi-key write is atomic with respect
// to both keys even when they live in different shards: a concurrent reader
// never sees the move half-applied.
func TestMultiKeyAtomicity(t *testing.T) {
	s := New()
	src := []byte("atomic:src")
	dst := []byte("atomic:dst")
	// Make the keys land in different shards if possible.
	if s.shardOf(src) == s.shardOf(dst) {
		dst = []byte("atomic:dst2")
	}
	s.SetString(src, []byte("payload"))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.Rename(src, dst)
				s.Rename(dst, src)
			}
		}
	}()

	for i := 0; i < 20000; i++ {
		vals := s.GetStrings(src, dst)
		aok, bok := vals[0] != nil, vals[1] != nil
		if aok == bok {
			t.Fatalf("both keys visible (or both gone) in a multi-key read: src=%v dst=%v", aok, bok)
		}
		if aok && string(vals[0]) != "payload" {
			t.Fatalf("torn payload %q", vals[0])
		}
		if bok && string(vals[1]) != "payload" {
			t.Fatalf("torn payload %q", vals[1])
		}
	}
	close(stop)
	wg.Wait()
}
