package store

import (
	"math/rand"
	"sort"
)

// Aggregate describes how the scores of a set operation are combined.
type Aggregate uint8

const (
	// AggregateSum adds the scores of the members (the default).
	AggregateSum Aggregate = iota
	// AggregateMin keeps the smallest score.
	AggregateMin
	// AggregateMax keeps the biggest score.
	AggregateMax
)

// zsetOp accumulates the scores of a member across the sets of an operation.
type zsetOp struct {
	score  float64
	weight float64
	count  int
}

// combine merges a score into the accumulator with the given aggregate.
func (o *zsetOp) combine(score, weight float64, agg Aggregate) {
	v := score * weight
	switch agg {
	case AggregateMin:
		if o.count == 0 || v < o.score {
			o.score = v
		}
	case AggregateMax:
		if o.count == 0 || v > o.score {
			o.score = v
		}
	default:
		o.score += v
	}
	o.count++
}

// ZUnionStore stores the union of the given sets in dst and returns its size.
// The scores are combined with the given aggregate and weights.
func (s *Store) ZUnionStore(dst []byte, keys [][]byte, weights []float64, agg Aggregate) (int, error) {
	sets, err := s.zsetsOf(keys)
	if err != nil {
		return 0, err
	}
	acc := make(map[string]*zsetOp)
	for i, set := range sets {
		w := weightAt(weights, i)
		for m, score := range set {
			o, ok := acc[m]
			if !ok {
				o = &zsetOp{}
				acc[m] = o
			}
			o.combine(score, w, agg)
		}
	}
	return s.storeZSet(dst, acc)
}

// ZInterStore stores the intersection of the given sets in dst and returns its
// size.
func (s *Store) ZInterStore(dst []byte, keys [][]byte, weights []float64, agg Aggregate) (int, error) {
	sets, err := s.zsetsOf(keys)
	if err != nil {
		return 0, err
	}
	if len(sets) == 0 {
		return 0, nil
	}
	acc := make(map[string]*zsetOp)
	// Start with the smallest set.
	smallest := 0
	for i, set := range sets {
		if len(set) < len(sets[smallest]) {
			smallest = i
		}
	}
	for m := range sets[smallest] {
		inAll := true
		for i, set := range sets {
			if i == smallest {
				continue
			}
			if _, ok := set[m]; !ok {
				inAll = false
				break
			}
		}
		if !inAll {
			continue
		}
		o := &zsetOp{}
		for i, set := range sets {
			if s, ok := set[m]; ok {
				o.combine(s, weightAt(weights, i), agg)
			}
		}
		acc[m] = o
	}
	return s.storeZSet(dst, acc)
}

// ZDiff returns the members of the first set that are not in the others.
func (s *Store) ZDiff(keys [][]byte) ([]ZEntry, error) {
	sets, err := s.zsetsOf(keys)
	if err != nil {
		return nil, err
	}
	if len(sets) == 0 {
		return nil, nil
	}
	others := make(map[string]struct{})
	for _, set := range sets[1:] {
		for m := range set {
			others[m] = struct{}{}
		}
	}
	out := make([]ZEntry, 0, len(sets[0]))
	for m, score := range sets[0] {
		if _, ok := others[m]; ok {
			continue
		}
		out = append(out, ZEntry{Member: []byte(m), Score: score})
	}
	sortZEntries(out)
	return out, nil
}

// ZDiffStore stores the difference of the given sets in dst and returns its
// size.
func (s *Store) ZDiffStore(dst []byte, keys [][]byte) (int, error) {
	entries, err := s.ZDiff(keys)
	if err != nil {
		return 0, err
	}
	acc := make(map[string]*zsetOp, len(entries))
	for _, e := range entries {
		acc[string(e.Member)] = &zsetOp{score: e.Score, count: 1}
	}
	return s.storeZSet(dst, acc)
}

// ZInterCard returns the cardinality of the intersection of the given sets.
func (s *Store) ZInterCard(keys [][]byte, limit int) (int, error) {
	sets, err := s.zsetsOf(keys)
	if err != nil {
		return 0, err
	}
	if len(sets) == 0 {
		return 0, nil
	}
	smallest := 0
	for i, set := range sets {
		if len(set) < len(sets[smallest]) {
			smallest = i
		}
	}
	n := 0
	for m := range sets[smallest] {
		inAll := true
		for i, set := range sets {
			if i == smallest {
				continue
			}
			if _, ok := set[m]; !ok {
				inAll = false
				break
			}
		}
		if !inAll {
			continue
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n, nil
}

// zsetsOf returns the members and scores of the given sets.
func (s *Store) zsetsOf(keys [][]byte) ([]map[string]float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	out := make([]map[string]float64, 0, len(keys))
	for _, k := range keys {
		e, ok := s.data[string(k)]
		if !ok || e.Expired(now) {
			out = append(out, map[string]float64{})
			continue
		}
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		out = append(out, e.ZSet.dict)
	}
	return out, nil
}

// weightAt returns the weight of the i-th set (1 when there are no weights).
func weightAt(weights []float64, i int) float64 {
	if i < len(weights) {
		return weights[i]
	}
	return 1
}

// storeZSet replaces the sorted set under dst with the accumulated members.
func (s *Store) storeZSet(dst []byte, acc map[string]*zsetOp) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if e, ok := s.data[string(dst)]; ok {
		if !e.Expired(now) && e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		s.usedBytes -= e.cost
		delete(s.data, string(dst))
	}
	z := newZSet()
	cost := len(string(dst))
	for m, o := range acc {
		z.Add(o.score, []byte(m))
		cost += len(m) + elemOverhead
	}
	e := &Entry{Type: TypeZSet, ZSet: z, cost: cost}
	e.lastAccess.Store(now.UnixNano())
	s.usedBytes += cost
	s.data[string(dst)] = e
	return len(acc), nil
}

// ZRemRangeByRank removes the members with a rank in the inclusive range
// [start, stop] and returns the number of removed members.
func (s *Store) ZRemRangeByRank(key []byte, start, stop int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeZSet {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	n := e.ZSet.Len()
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return 0, nil
	}
	removed := 0
	for i := stop; i >= start; i-- {
		entry := e.ZSet.entryAt(i)
		if entry == nil {
			break
		}
		e.ZSet.Remove(entry.Member)
		s.addCost(e, -(len(entry.Member) + elemOverhead))
		removed++
	}
	if e.ZSet.Len() == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return removed, nil
}

// ZRemRangeByScore removes the members whose score is in the given range and
// returns the number of removed members.
func (s *Store) ZRemRangeByScore(key []byte, min, max float64, minExcl, maxExcl bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeZSet {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	entries := e.ZSet.RangeByScore(min, max, minExcl, maxExcl, 0, 0)
	for _, entry := range entries {
		e.ZSet.Remove(entry.Member)
		s.addCost(e, -(len(entry.Member) + elemOverhead))
	}
	if e.ZSet.Len() == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return len(entries), nil
}

// ZRemRangeByLex removes the members in the given lexicographical range and
// returns the number of removed members.
func (s *Store) ZRemRangeByLex(key []byte, min, max []byte, minExcl, maxExcl bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeZSet {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	entries := e.ZSet.RangeByLex(min, max, minExcl, maxExcl, 0, 0)
	for _, entry := range entries {
		e.ZSet.Remove(entry.Member)
		s.addCost(e, -(len(entry.Member) + elemOverhead))
	}
	if e.ZSet.Len() == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return len(entries), nil
}

// ZPop removes and returns the members with the lowest (or highest) scores.
// A count of zero pops a single member.
func (s *Store) ZPop(key []byte, count int, lowest bool) ([]ZEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeZSet {
		return nil, ErrWrongType
	}
	s.touchLocked(string(key))
	n := e.ZSet.Len()
	if n == 0 {
		return nil, nil
	}
	if count <= 0 {
		count = 1
	}
	if count > n {
		count = n
	}
	out := make([]ZEntry, 0, count)
	for i := 0; i < count; i++ {
		idx := 0
		if !lowest {
			idx = n - 1 - i
		}
		entry := e.ZSet.entryAt(idx)
		if entry == nil {
			break
		}
		e.ZSet.Remove(entry.Member)
		s.addCost(e, -(len(entry.Member) + elemOverhead))
		out = append(out, *entry)
	}
	if e.ZSet.Len() == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return out, nil
}

// ZPopAny pops the member with the lowest (or highest) score from the first
// non-empty sorted set of the given keys.
func (s *Store) ZPopAny(keys [][]byte, lowest bool) (key []byte, entry ZEntry, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	for _, k := range keys {
		e, ok := s.data[string(k)]
		if !ok || e.Expired(now) {
			continue
		}
		if e.Type != TypeZSet {
			return nil, ZEntry{}, false, ErrWrongType
		}
		if e.ZSet.Len() == 0 {
			continue
		}
		idx := 0
		if !lowest {
			idx = e.ZSet.Len() - 1
		}
		entry := e.ZSet.entryAt(idx)
		if entry == nil {
			continue
		}
		s.touchLocked(string(k))
		e.ZSet.Remove(entry.Member)
		s.addCost(e, -(len(entry.Member) + elemOverhead))
		if e.ZSet.Len() == 0 {
			s.usedBytes -= e.cost
			delete(s.data, string(k))
		}
		return k, *entry, true, nil
	}
	return nil, ZEntry{}, false, nil
}

// ZRandomMembers returns random members of the sorted set. A negative count
// returns |count| members, possibly with repetitions.
func (s *Store) ZRandomMembers(key []byte, count int) ([]ZEntry, error) {
	entries, ok, err := readEntry(s, key, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return e.ZSet.Range(0, -1, false), nil
	})
	if err != nil {
		return nil, err
	}
	if !ok || len(entries) == 0 {
		return nil, nil
	}
	if count == 0 {
		return []ZEntry{entries[rand.Intn(len(entries))]}, nil
	}
	if count < 0 {
		out := make([]ZEntry, 0, -count)
		for i := 0; i < -count; i++ {
			out = append(out, entries[rand.Intn(len(entries))])
		}
		return out, nil
	}
	if count > len(entries) {
		count = len(entries)
	}
	perm := rand.Perm(len(entries))
	out := make([]ZEntry, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, entries[perm[i]])
	}
	return out, nil
}

// ZRangeStoreByScore stores the members of src whose score is in the given
// range in dst and returns the number of stored members.
func (s *Store) ZRangeStoreByScore(dst, src []byte, min, max float64, minExcl, maxExcl bool, offset, count int) (int, error) {
	entries, ok, err := readEntry(s, src, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return e.ZSet.RangeByScore(min, max, minExcl, maxExcl, offset, count), nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return s.storeEntries(dst, entries)
}

// ZRangeStoreByLex stores the members of src in the given lexicographical
// range in dst and returns the number of stored members.
func (s *Store) ZRangeStoreByLex(dst, src []byte, min, max []byte, minExcl, maxExcl bool, offset, count int) (int, error) {
	entries, ok, err := readEntry(s, src, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return e.ZSet.RangeByLex(min, max, minExcl, maxExcl, offset, count), nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return s.storeEntries(dst, entries)
}

// ZRangeStoreByRank stores the members of src in the given rank range in dst
// and returns the number of stored members.
func (s *Store) ZRangeStoreByRank(dst, src []byte, start, stop, offset, count int) (int, error) {
	entries, ok, err := readEntry(s, src, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return e.ZSet.Range(start, stop, false), nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	if offset > 0 {
		if offset >= len(entries) {
			entries = nil
		} else {
			entries = entries[offset:]
		}
	}
	if count > 0 && count < len(entries) {
		entries = entries[:count]
	}
	return s.storeEntries(dst, entries)
}

// storeEntries stores the given entries in the sorted set under dst.
func (s *Store) storeEntries(dst []byte, entries []ZEntry) (int, error) {
	acc := make(map[string]*zsetOp, len(entries))
	for _, e := range entries {
		acc[string(e.Member)] = &zsetOp{score: e.Score, count: 1}
	}
	return s.storeZSet(dst, acc)
}

// sortZEntries sorts the entries by score and then by member.
func sortZEntries(entries []ZEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}
		return string(entries[i].Member) < string(entries[j].Member)
	})
}
