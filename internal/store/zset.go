package store

// ZEntry is one member/score pair returned by ZSet range operations.
type ZEntry struct {
	Member []byte
	Score  float64
}

// ZSet is a sorted set: a dict for O(1) score lookup plus a skiplist for
// O(log N) ordered access, mirroring Redis zset.
type ZSet struct {
	dict map[string]float64
	sl   *skiplist
}

func newZSet() *ZSet {
	return &ZSet{dict: make(map[string]float64), sl: newSkiplist()}
}

// Add inserts or updates member with score. It returns true when member was
// newly added.
func (z *ZSet) Add(score float64, member []byte) bool {
	if old, exists := z.dict[string(member)]; exists {
		if old != score {
			z.sl.Delete(old, member)
			z.sl.Insert(score, member)
		}
		z.dict[string(member)] = score
		return false
	}
	z.sl.Insert(score, member)
	z.dict[string(member)] = score
	return true
}

// Remove deletes member, reporting whether it existed.
func (z *ZSet) Remove(member []byte) bool {
	score, exists := z.dict[string(member)]
	if !exists {
		return false
	}
	z.sl.Delete(score, member)
	delete(z.dict, string(member))
	return true
}

// Score returns the score of member.
func (z *ZSet) Score(member []byte) (float64, bool) {
	s, ok := z.dict[string(member)]
	return s, ok
}

// Len returns the number of members.
func (z *ZSet) Len() int { return len(z.dict) }

// Range returns entries in the inclusive rank range [start, stop]. Ranks are
// 0-based. Negative ranks count from the end (highest score). When reverse is
// true the result is returned from highest to lowest score.
func (z *ZSet) Range(start, stop int, reverse bool) []ZEntry {
	n := z.Len()
	if n == 0 {
		return nil
	}
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if start > stop || start >= n {
		return nil
	}
	if stop >= n {
		stop = n - 1
	}
	count := stop - start + 1
	out := make([]ZEntry, 0, count)
	if reverse {
		for i := start; i <= stop; i++ {
			node := z.sl.nodeByRank(int64(n - i))
			out = append(out, ZEntry{Member: node.member, Score: node.score})
		}
	} else {
		for i := start; i <= stop; i++ {
			node := z.sl.nodeByRank(int64(i + 1))
			out = append(out, ZEntry{Member: node.member, Score: node.score})
		}
	}
	return out
}

// entryAt returns the entry at the given rank (0-based), or nil when the rank
// is out of range.
func (z *ZSet) entryAt(rank int) *ZEntry {
	if rank < 0 || rank >= z.Len() {
		return nil
	}
	node := z.sl.nodeByRank(int64(rank + 1))
	if node == nil {
		return nil
	}
	return &ZEntry{Member: node.member, Score: node.score}
}

// Rank returns the 0-based rank of member from lowest score (or from highest
// when reverse is true), and false when member is not in the set.
func (z *ZSet) Rank(member []byte, reverse bool) (int64, bool) {
	score, exists := z.dict[string(member)]
	if !exists {
		return 0, false
	}
	rank := z.sl.GetRank(score, member)
	if rank == 0 {
		return 0, false
	}
	if reverse {
		return int64(z.Len()) - rank, true
	}
	return rank - 1, true
}

// ZAdd inserts or updates member with score in the zset under key, creating it
// if missing. It returns 1 when member was newly added.
func (s *Store) ZAdd(key []byte, score float64, member []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		e = &Entry{Type: TypeZSet, ZSet: newZSet(), cost: len(string(key))}
		s.usedBytes += len(string(key))
		s.data[string(key)] = e
	} else if e.Type != TypeZSet {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)
	if _, exists := e.ZSet.dict[string(member)]; !exists {
		s.addCost(e, len(member)+elemOverhead)
	}
	if e.ZSet.Add(score, member) {
		return 1, nil
	}
	return 0, nil
}

// ZScore returns the score of member in the zset under key. ok is false when
// the member or key is missing.
func (s *Store) ZScore(key, member []byte) (float64, bool, error) {
	score, ok, err := readEntry(s, key, func(e *Entry) (float64, error) {
		if e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		sc, exists := e.ZSet.Score(member)
		if !exists {
			return 0, errNotFound
		}
		return sc, nil
	})
	if err == errNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, ok, nil
}

// ZRem removes members from the zset under key, returning the number removed.
// The key is deleted when its zset becomes empty.
func (s *Store) ZRem(key []byte, members ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeZSet {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(key)
	removed := 0
	for _, m := range members {
		if e.ZSet.Remove(m) {
			s.addCost(e, -(len(m) + elemOverhead))
			removed++
		}
	}
	if e.ZSet.Len() == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return removed, nil
}

// ZCard returns the cardinality of the zset under key. ok is false when
// missing.
func (s *Store) ZCard(key []byte) (int, bool, error) {
	return readEntry(s, key, func(e *Entry) (int, error) {
		if e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		return e.ZSet.Len(), nil
	})
}

// ZRange returns entries in the inclusive rank range [start, stop]. When
// reverse is true the result is ordered from highest to lowest score.
func (s *Store) ZRange(key []byte, start, stop int, reverse bool) ([]ZEntry, error) {
	v, _, err := readEntry(s, key, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return e.ZSet.Range(start, stop, reverse), nil
	})
	return v, err
}

// ZRank returns the 0-based rank of member. When reverse is true the rank is
// counted from the highest score. ok is false when member or key is missing.
func (s *Store) ZRank(key, member []byte, reverse bool) (int64, bool, error) {
	rank, ok, err := readEntry(s, key, func(e *Entry) (int64, error) {
		if e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		r, found := e.ZSet.Rank(member, reverse)
		if !found {
			return 0, errNotFound
		}
		return r, nil
	})
	if err == errNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return rank, ok, nil
}
