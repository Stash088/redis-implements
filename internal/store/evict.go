package store

import "math/rand"

// elemOverhead approximates the per-element memory overhead of a container
// member (map/skiplist bookkeeping).
const elemOverhead = 8

// EvictCycle evicts keys until the store is within maxBytes or no victim can
// be found, returning the number of evicted keys. policy is one of
// allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl;
// each cycle samples `samples` random keys (randomized Go map iteration, the
// shards are walked from a random offset) and picks the victim per policy,
// mirroring Redis maxmemory-samples.
func (s *Store) EvictCycle(policy string, samples int, maxBytes int64) int {
	if samples < 1 {
		samples = 5
	}
	total := s.UsedBytes()
	evicted := 0
	// consecutiveFails bounds the loop when the sampled victims keep
	// disappearing (removed concurrently between the sample and the
	// removal), mirroring the "sample found nothing" exit of a single-map
	// store.
	consecutiveFails := 0
	for int64(total) > maxBytes && consecutiveFails < shardCount {
		sh, victim, ok := s.pickVictim(policy, samples)
		if !ok {
			break
		}
		sh.mu.Lock()
		// The victim may have been modified or removed while the lock was
		// not held; only a present entry is a valid victim.
		if e, present := sh.data[victim.key]; present && e == victim.e {
			sh.usedBytes -= e.cost
			s.touchLocked(sh, victim.key)
			delete(sh.data, victim.key)
			total -= e.cost
			evicted++
			consecutiveFails = 0
		} else {
			consecutiveFails++
		}
		sh.mu.Unlock()
	}
	return evicted
}

// victim is a sampled eviction candidate.
type victim struct {
	key string
	e   *Entry
}

// pickVictim samples candidate keys across the shards (starting at a random
// shard, like the expiry cycle) until the sample holds `samples` candidates
// or the keyspace is exhausted, then returns the victim per policy. The
// candidate's shard is returned so the caller can remove the victim under its
// write lock; the caller must re-validate the victim once the lock is held.
func (s *Store) pickVictim(policy string, samples int) (*shard, *victim, bool) {
	volatile := policy == "volatile-lru" || policy == "volatile-random" || policy == "volatile-ttl"
	sample := make([]victim, 0, samples)
	owners := make([]int, 0, samples) // shard index of every candidate
	start := rand.Intn(shardCount)
	for i := 0; i < shardCount && len(sample) < samples; i++ {
		si := (start + i) & shardMask
		sh := &s.shards[si]
		sh.mu.RLock()
		for k, e := range sh.data {
			sample = append(sample, victim{k, e})
			owners = append(owners, si)
			if len(sample) >= samples {
				break
			}
		}
		sh.mu.RUnlock()
	}
	best := selectVictim(sample, volatile, policy)
	if best < 0 {
		return nil, nil, false
	}
	return &s.shards[owners[best]], &sample[best], true
}

// selectVictim returns the index of the victim in the sample according to the
// policy, or -1 when no candidate qualifies.
func selectVictim(sample []victim, volatile bool, policy string) int {
	best := -1
	for i := range sample {
		c := &sample[i]
		if volatile && c.e.ExpiresAt.IsZero() {
			continue
		}
		switch policy {
		case "volatile-ttl":
			if best < 0 || c.e.ExpiresAt.Before(sample[best].e.ExpiresAt) {
				best = i
			}
		case "allkeys-lru", "volatile-lru":
			if best < 0 || c.e.lastAccess.Load() < sample[best].e.lastAccess.Load() {
				best = i
			}
		case "allkeys-random", "volatile-random":
			if best < 0 {
				best = i
			}
		default:
			// Unknown policy (e.g. noeviction): never evict.
			return -1
		}
	}
	return best
}

// entryCostLocked computes the approximate footprint of e under key:
// key length plus payload (elements get an overhead allowance).
func (s *Store) entryCostLocked(key string, e *Entry) int {
	cost := len(key)
	switch e.Type {
	case TypeString:
		cost += len(e.Str)
	case TypeList:
		for _, v := range e.List.Items() {
			cost += len(v) + elemOverhead
		}
	case TypeHash:
		for f, v := range e.Hash {
			cost += len(f) + len(v)
		}
	case TypeSet:
		for m := range e.Set {
			cost += len(m) + elemOverhead
		}
	case TypeZSet:
		for m := range e.ZSet.dict {
			cost += len(m) + elemOverhead
		}
	case TypeStream:
		for _, entry := range e.Stream.entries {
			cost += entryCost(entry)
		}
	}
	return cost
}

// markAccess updates the entry's last-access timestamp (allkeys-lru).
func (s *Store) markAccess(e *Entry) {
	e.lastAccess.Store(s.clock().UnixNano())
}

// addCost applies a memory delta to the entry and the shard total.
// Callers must hold the write lock of the shard that owns the entry.
func (s *Store) addCost(sh *shard, e *Entry, delta int) {
	e.cost += delta
	sh.usedBytes += delta
}
