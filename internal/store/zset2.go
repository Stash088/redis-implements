package store

import (
	"math"
	"sort"
)

// ZLen returns the number of members of the zset under key.
func (s *Store) ZLen(key []byte) (int, error) {
	n, ok, err := readEntry(s, key, func(e *Entry) (int, error) {
		if e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		return e.ZSet.Len(), nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return n, nil
}

// ZRangeByScore returns the members of the zset whose score is in the given
// range, ordered by score. An exclusive bound excludes the boundary value.
// A positive count limits the number of returned members; offset skips
// members at the beginning.
func (s *Store) ZRangeByScore(key []byte, min, max float64, minExcl, maxExcl bool, offset, count int) ([]ZEntry, error) {
	return s.zsetEntries(key, func(z *ZSet) []ZEntry {
		return z.RangeByScore(min, max, minExcl, maxExcl, offset, count)
	})
}

// ZRangeByLex returns the members of the zset in the given lexicographical
// range, ordered lexicographically.
func (s *Store) ZRangeByLex(key []byte, min, max []byte, minExcl, maxExcl bool, offset, count int) ([]ZEntry, error) {
	return s.zsetEntries(key, func(z *ZSet) []ZEntry {
		return z.RangeByLex(min, max, minExcl, maxExcl, offset, count)
	})
}

// zsetEntries applies fn to the zset under key.
func (s *Store) zsetEntries(key []byte, fn func(z *ZSet) []ZEntry) ([]ZEntry, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return fn(e.ZSet), nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return out, nil
}

// ZCount returns the number of members whose score is in the given range.
func (s *Store) ZCount(key []byte, min, max float64, minExcl, maxExcl bool) (int, error) {
	n, ok, err := readEntry(s, key, func(e *Entry) (int, error) {
		if e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		return e.ZSet.CountByScore(min, max, minExcl, maxExcl), nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return n, nil
}

// ZLexCount returns the number of members in the given lexicographical range.
func (s *Store) ZLexCount(key []byte, min, max []byte, minExcl, maxExcl bool) (int, error) {
	n, ok, err := readEntry(s, key, func(e *Entry) (int, error) {
		if e.Type != TypeZSet {
			return 0, ErrWrongType
		}
		return e.ZSet.CountByLex(min, max, minExcl, maxExcl), nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return n, nil
}

// ZIncrBy increments the score of a member and returns the new score. The
// member is created with the score 0 when it does not exist.
func (s *Store) ZIncrBy(key []byte, delta float64, member []byte) (float64, error) {
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
	old, exists := e.ZSet.Score(member)
	if !exists {
		old = 0
		s.addCost(e, len(member)+elemOverhead)
	}
	score := old + delta
	if math.IsInf(score, 0) || math.IsNaN(score) {
		return 0, ErrNotFloat
	}
	e.ZSet.Add(score, member)
	return score, nil
}

// ZMScore returns the scores of the given members. A missing member yields a
// nil score.
func (s *Store) ZMScore(key []byte, members [][]byte) ([]*float64, error) {
	out := make([]*float64, len(members))
	_, _, err := readEntry(s, key, func(e *Entry) (bool, error) {
		if e.Type != TypeZSet {
			return false, ErrWrongType
		}
		for i, m := range members {
			if score, ok := e.ZSet.Score(m); ok {
				v := score
				out[i] = &v
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RangeByScore returns the members whose score is in [min, max], ordered by
// score. The bounds may be exclusive.
func (z *ZSet) RangeByScore(min, max float64, minExcl, maxExcl bool, offset, count int) []ZEntry {
	n := z.Len()
	out := make([]ZEntry, 0, n)
	skipped := 0
	for i := 0; i < n; i++ {
		node := z.sl.nodeByRank(int64(i + 1))
		score := node.score
		if minExcl {
			if score <= min {
				continue
			}
		} else if score < min {
			continue
		}
		if maxExcl {
			if score >= max {
				break
			}
		} else if score > max {
			break
		}
		if skipped < offset {
			skipped++
			continue
		}
		out = append(out, ZEntry{Member: node.member, Score: score})
		if count > 0 && len(out) >= count {
			break
		}
	}
	return out
}

// CountByScore returns the number of members whose score is in the range.
func (z *ZSet) CountByScore(min, max float64, minExcl, maxExcl bool) int {
	n := z.Len()
	count := 0
	for i := 0; i < n; i++ {
		node := z.sl.nodeByRank(int64(i + 1))
		score := node.score
		if minExcl {
			if score <= min {
				continue
			}
		} else if score < min {
			continue
		}
		if maxExcl {
			if score >= max {
				break
			}
		} else if score > max {
			break
		}
		count++
	}
	return count
}

// RangeByLex returns the members in the lexicographical range, ordered
// lexicographically.
func (z *ZSet) RangeByLex(min, max []byte, minExcl, maxExcl bool, offset, count int) []ZEntry {
	entries := z.Range(0, -1, false)
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Member) < string(entries[j].Member)
	})
	out := make([]ZEntry, 0, len(entries))
	skipped := 0
	for _, e := range entries {
		if min != nil {
			cmp := compareBytes(e.Member, min)
			if minExcl {
				if cmp <= 0 {
					continue
				}
			} else if cmp < 0 {
				continue
			}
		}
		if max != nil {
			cmp := compareBytes(e.Member, max)
			if maxExcl {
				if cmp >= 0 {
					break
				}
			} else if cmp > 0 {
				break
			}
		}
		if skipped < offset {
			skipped++
			continue
		}
		out = append(out, e)
		if count > 0 && len(out) >= count {
			break
		}
	}
	return out
}

// CountByLex returns the number of members in the lexicographical range.
func (z *ZSet) CountByLex(min, max []byte, minExcl, maxExcl bool) int {
	return len(z.RangeByLex(min, max, minExcl, maxExcl, 0, 0))
}

// compareBytes compares two byte slices lexicographically.
func compareBytes(a, b []byte) int {
	switch {
	case string(a) < string(b):
		return -1
	case string(a) > string(b):
		return 1
	}
	return 0
}
