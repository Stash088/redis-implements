package store

// elemOverhead approximates the per-element memory overhead of a container
// member (map/skiplist bookkeeping).
const elemOverhead = 8

// EvictCycle evicts keys until the store is within maxBytes or no victim can
// be found, returning the number of evicted keys. policy is one of
// allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl;
// each cycle samples `samples` random keys (randomized Go map iteration) and
// picks the victim per policy, mirroring Redis maxmemory-samples.
func (s *Store) EvictCycle(policy string, samples int, maxBytes int64) int {
	if samples < 1 {
		samples = 5
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for int64(s.usedBytes) > maxBytes {
		if !s.evictOneLocked(policy, samples) {
			break
		}
		evicted++
	}
	return evicted
}

// evictOneLocked evicts a single victim according to policy and reports
// whether one was found. Callers must hold s.mu for writing.
func (s *Store) evictOneLocked(policy string, samples int) bool {
	volatile := policy == "volatile-lru" || policy == "volatile-random" || policy == "volatile-ttl"
	type cand struct {
		key string
		e   *Entry
	}
	sample := make([]cand, 0, samples)
	for k, e := range s.data {
		sample = append(sample, cand{k, e})
		if len(sample) >= samples {
			break
		}
	}
	if len(sample) == 0 {
		return false
	}
	var victim *cand
	for i := range sample {
		c := &sample[i]
		if volatile && c.e.ExpiresAt.IsZero() {
			continue
		}
		switch policy {
		case "volatile-ttl":
			if victim == nil || c.e.ExpiresAt.Before(victim.e.ExpiresAt) {
				victim = c
			}
		case "allkeys-lru", "volatile-lru":
			if victim == nil || c.e.lastAccess < victim.e.lastAccess {
				victim = c
			}
		case "allkeys-random", "volatile-random":
			if victim == nil {
				victim = c
			}
		default:
			// Unknown policy (e.g. noeviction): never evict.
			return false
		}
	}
	if victim == nil {
		return false
	}
	s.usedBytes -= victim.e.cost
	s.touchLocked(victim.key)
	delete(s.data, victim.key)
	return true
}

// entryCostLocked computes the approximate footprint of e under key:
// key length plus payload (elements get an overhead allowance).
// Callers must hold s.mu.
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
	}
	return cost
}

// markAccess updates the entry's last-access timestamp (allkeys-lru).
// Callers must hold s.mu.
func (s *Store) markAccess(e *Entry) {
	e.lastAccess = s.clock().UnixNano()
}

// addCost applies a memory delta to the entry and the store total.
// Callers must hold s.mu.
func (s *Store) addCost(e *Entry, delta int) {
	e.cost += delta
	s.usedBytes += delta
}
