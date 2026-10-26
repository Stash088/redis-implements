package store

import "sync"

// The store is sharded: every shard is an independently locked slice of the
// keyspace. Commands that touch one key take the lock of its shard only, so
// writers on different shards never contend (the profile of the pre-sharding
// store showed the single write lock as the only measurable source of mutex
// contention under load). Commands that touch several keys lock the shards of
// all of them in ascending index order, which makes overlapping multi-key
// operations deadlock-free.
//
// Whole-keyspace operations (KEYS, SCAN, SAVE, FLUSH, the expiry and eviction
// cycles) visit the shards one at a time: their result may interleave with
// concurrent writes, which matches the semantics of Redis, where such
// operations also observe a moving dataset.

const (
	// shardCount is the number of shards of a Store. It is a power of two,
	// so the shard of a key is the bitwise AND of its hash. 16 spreads the
	// write contention wide enough for the single-digit core counts the
	// server is aimed at, while keeping the fixed cost of a shard (a mutex,
	// a map header and the bookkeeping of the memory accounting) small.
	shardCount = 16
	// shardMask selects the shard out of a key hash.
	shardMask = shardCount - 1
)

// shard is one independently locked slice of the keyspace.
type shard struct {
	mu        sync.RWMutex
	data      map[string]*Entry
	watches   map[string]map[Watcher]struct{} // WATCH/CAS registry
	usedBytes int                             // sum of entry costs, guarded by mu
	// pad grows the shard to a full cache line, so that neighbouring
	// shards of the array do not share one: the read and write locks are
	// hot atomics, and two shards bouncing the same line would give back
	// the contention the sharding removed.
	pad [16]byte
}

func (sh *shard) init() {
	sh.data = make(map[string]*Entry)
	sh.watches = make(map[string]map[Watcher]struct{})
}

// hashKey is FNV-1a over the key bytes: allocation-free and it spreads the
// keyspace evenly over the shards.
func hashKey(key []byte) uint32 {
	h := uint32(2166136261)
	for _, b := range key {
		h ^= uint32(b)
		h *= 16777619
	}
	return h
}

// hashKeyString is hashKey for a key that is already a string.
func hashKeyString(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// shardOf returns the shard that owns key.
func (s *Store) shardOf(key []byte) *shard {
	return &s.shards[hashKey(key)&shardMask]
}

// shardOfStr is shardOf for a key that is already a string.
func (s *Store) shardOfStr(key string) *shard {
	return &s.shards[hashKeyString(key)&shardMask]
}

// lockWrite locks the shards of all keys for writing, in ascending shard
// order, and returns the unlock function. Callers must pass at least one key.
//
// The ascending order guarantees that two multi-key operations with overlapping
// keys cannot deadlock. While the locks are held, the operation is atomic with
// respect to every key it touches: a concurrent command on one of these keys
// waits for the same locks.
func (s *Store) lockWrite(keys ...[]byte) func() {
	first := s.shardOf(keys[0])
	if s.singleShard(keys, first) {
		first.mu.Lock()
		return func() { first.mu.Unlock() }
	}
	idx := s.distinctShards(keys)
	for _, i := range idx {
		s.shards[i].mu.Lock()
	}
	return func() {
		for i := len(idx) - 1; i >= 0; i-- {
			s.shards[idx[i]].mu.Unlock()
		}
	}
}

// lockRead is lockWrite for read-only multi-key operations: it takes the read
// lock of each shard, so readers on different shards (and single-key readers)
// still run in parallel.
func (s *Store) lockRead(keys ...[]byte) func() {
	first := s.shardOf(keys[0])
	if s.singleShard(keys, first) {
		first.mu.RLock()
		return func() { first.mu.RUnlock() }
	}
	idx := s.distinctShards(keys)
	for _, i := range idx {
		s.shards[i].mu.RLock()
	}
	return func() {
		for i := len(idx) - 1; i >= 0; i-- {
			s.shards[idx[i]].mu.RUnlock()
		}
	}
}

// singleShard reports whether all keys map to the given shard.
func (s *Store) singleShard(keys [][]byte, first *shard) bool {
	for _, k := range keys[1:] {
		if s.shardOf(k) != first {
			return false
		}
	}
	return true
}

// distinctShards returns the indexes of the shards of the keys, without
// duplicates, sorted ascending.
func (s *Store) distinctShards(keys [][]byte) []int {
	var buf [shardCount]int
	idx := buf[:0]
	for _, k := range keys {
		i := int(hashKey(k) & shardMask)
		seen := false
		for _, j := range idx {
			if j == i {
				seen = true
				break
			}
		}
		if !seen {
			idx = append(idx, i)
		}
	}
	// Insertion sort: at most shardCount tiny elements.
	for i := 1; i < len(idx); i++ {
		for j := i; j > 0 && idx[j] < idx[j-1]; j-- {
			idx[j], idx[j-1] = idx[j-1], idx[j]
		}
	}
	return idx
}
