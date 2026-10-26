package store

import "time"

// Rename moves the value of a key to another key. It reports whether the
// source key existed. The shards of both keys are locked, so the rename is
// atomic with respect to both.
func (s *Store) Rename(key, newKey []byte) (bool, error) {
	unlock := s.lockWrite(key, newKey)
	defer unlock()
	srcSh := s.shardOf(key)
	e, ok := s.entryLocked(srcSh, key)
	if !ok {
		return false, nil
	}
	old := string(key)
	dst := string(newKey)
	if old != dst {
		dstSh := s.shardOfStr(dst)
		s.touchLocked(srcSh, old)
		s.touchLocked(dstSh, dst)
		// The destination is replaced.
		if prev, exists := dstSh.data[dst]; exists {
			s.addBytes(dstSh, -int64(prev.cost))
		}
		dstSh.data[dst] = e
		delete(srcSh.data, old)
		s.addBytes(srcSh, -int64(e.cost))
		s.addBytes(dstSh, int64(e.cost))
	}
	return true, nil
}

// RenameNX moves the value of a key to another key, but only when the
// destination does not exist. It reports whether the key was renamed.
func (s *Store) RenameNX(key, newKey []byte) (bool, error) {
	unlock := s.lockWrite(key, newKey)
	defer unlock()
	srcSh := s.shardOf(key)
	e, ok := s.entryLocked(srcSh, key)
	if !ok {
		return false, nil
	}
	old := string(key)
	dst := string(newKey)
	if old == dst {
		return true, nil
	}
	now := s.clock()
	dstSh := s.shardOfStr(dst)
	if prev, exists := dstSh.data[dst]; exists && !prev.Expired(now) {
		return false, nil
	}
	s.touchLocked(srcSh, old)
	s.touchLocked(dstSh, dst)
	if prev, exists := dstSh.data[dst]; exists {
		s.addBytes(dstSh, -int64(prev.cost))
	}
	dstSh.data[dst] = e
	delete(srcSh.data, old)
	s.addBytes(srcSh, -int64(e.cost))
	s.addBytes(dstSh, int64(e.cost))
	return true, nil
}

// CopyEntry returns a copy of the entry under key, or nil when the key does
// not exist. The copy is deep, so the caller may store it in another store.
func (s *Store) CopyEntry(key []byte) (*Entry, bool) {
	sh := s.shardOf(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := s.entryLockedRO(sh, key)
	if !ok {
		return nil, false
	}
	return copyEntry(e), true
}

// entryLockedRO returns the live entry for key without removing expired ones.
// Callers must hold the read (or write) lock of the shard that owns the key.
func (s *Store) entryLockedRO(sh *shard, key []byte) (*Entry, bool) {
	e, ok := sh.data[string(key)]
	if !ok {
		return nil, false
	}
	if e.Expired(s.clock()) {
		return nil, false
	}
	return e, true
}

// copyEntry returns a deep copy of an entry.
func copyEntry(e *Entry) *Entry {
	out := &Entry{
		Type:      e.Type,
		ExpiresAt: e.ExpiresAt,
		cost:      e.cost,
	}
	out.lastAccess.Store(e.lastAccess.Load())
	switch e.Type {
	case TypeString:
		out.Str = append([]byte(nil), e.Str...)
	case TypeList:
		l := NewList()
		for _, v := range e.List.Items() {
			l.PushBack(append([]byte(nil), v...))
		}
		out.List = l
	case TypeHash:
		out.Hash = make(map[string][]byte, len(e.Hash))
		for f, v := range e.Hash {
			out.Hash[f] = append([]byte(nil), v...)
		}
	case TypeSet:
		out.Set = make(map[string]struct{}, len(e.Set))
		for m := range e.Set {
			out.Set[m] = struct{}{}
		}
	case TypeZSet:
		z := NewZSet()
		for m, score := range e.ZSet.dict {
			z.Add(score, []byte(m))
		}
		out.ZSet = z
	case TypeStream:
		// The snapshot carries the groups and the consumers too: without
		// them COPY, MOVE and DUMP would lose them.
		snap := e.Stream.Snapshot()
		out.Stream = NewStream(snap)
	}
	return out
}

// StoreEntry stores a copy of the given entry under a key. It replaces the
// existing value.
func (s *Store) StoreEntry(key []byte, e *Entry) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.touchKeyLocked(sh, key)
	cost := s.entryCostLocked(string(key), e)
	if old, ok := sh.data[string(key)]; ok {
		s.addBytes(sh, -int64(old.cost))
	}
	e.cost = cost
	e.lastAccess.Store(s.clock().UnixNano())
	s.addBytes(sh, int64(cost))
	sh.data[string(key)] = e
}

// ExpireTime returns the absolute expiration of a key as a Unix timestamp in
// seconds (-1 when the key has no TTL) and whether the key exists.
func (s *Store) ExpireTime(key []byte) (int64, bool) {
	sh := s.shardOf(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := s.entryLockedRO(sh, key)
	if !ok {
		return -2, false
	}
	if e.ExpiresAt.IsZero() {
		return -1, true
	}
	return e.ExpiresAt.Unix(), true
}

// PExpireTime returns the absolute expiration of a key as a Unix timestamp in
// milliseconds.
func (s *Store) PExpireTime(key []byte) (int64, bool) {
	sh := s.shardOf(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := s.entryLockedRO(sh, key)
	if !ok {
		return -2, false
	}
	if e.ExpiresAt.IsZero() {
		return -1, true
	}
	return e.ExpiresAt.UnixMilli(), true
}

// MoveEntry moves the value of a key to another store. It reports whether the
// source key existed. The two stores are independent, so the move is not
// atomic (like Redis, which copies and deletes in two steps too).
func (s *Store) MoveEntry(key []byte, dest *Store) (bool, error) {
	e, ok := s.CopyEntry(key)
	if !ok {
		return false, nil
	}
	s.Delete(key)
	dest.StoreEntry(key, e)
	return true, nil
}

// EntryType returns the type of the value under key.
func (s *Store) EntryType(key []byte) (Type, bool) {
	sh := s.shardOf(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := s.entryLockedRO(sh, key)
	if !ok {
		return 0, false
	}
	return e.Type, true
}

// EntryEncoding returns the name of the encoding of the value under key, as
// the OBJECT ENCODING command reports it.
func (s *Store) EntryEncoding(key []byte) (string, bool) {
	t, ok := s.EntryType(key)
	if !ok {
		return "", false
	}
	switch t {
	case TypeString:
		return "embstr", true
	case TypeList:
		return "listpack", true
	case TypeHash:
		return "listpack", true
	case TypeSet:
		return "intset", true
	case TypeZSet:
		return "listpack", true
	case TypeStream:
		return "stream", true
	}
	return "none", true
}

// SortElements returns the elements of the list, set or sorted set under key,
// so they can be sorted (the SORT command).
func (s *Store) SortElements(key []byte) ([][]byte, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([][]byte, error) {
		switch e.Type {
		case TypeList:
			return e.List.Range(0, -1), nil
		case TypeSet:
			members := make([][]byte, 0, len(e.Set))
			for m := range e.Set {
				members = append(members, []byte(m))
			}
			return members, nil
		case TypeZSet:
			entries := e.ZSet.Range(0, -1, false)
			members := make([][]byte, 0, len(entries))
			for _, ze := range entries {
				members = append(members, ze.Member)
			}
			return members, nil
		default:
			return nil, ErrWrongType
		}
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return out, nil
}

// SetList replaces the list under key with the given elements.
func (s *Store) SetList(key []byte, elems [][]byte) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.clock()
	if e, ok := sh.data[string(key)]; ok {
		if !e.Expired(now) && e.Type != TypeList {
			return 0, ErrWrongType
		}
		s.addBytes(sh, -int64(e.cost))
		delete(sh.data, string(key))
	}
	l := NewList()
	cost := len(string(key))
	for _, v := range elems {
		l.PushBack(v)
		cost += len(v) + elemOverhead
	}
	e := &Entry{Type: TypeList, List: l, cost: cost}
	e.lastAccess.Store(now.UnixNano())
	s.addBytes(sh, int64(cost))
	sh.data[string(key)] = e
	return len(elems), nil
}

// RestoreEntryWithTTL stores an entry under a key with the given time to
// live. A ttl of zero means no expiration.
func (s *Store) RestoreEntryWithTTL(key []byte, e *Entry, ttl time.Duration) {
	if ttl > 0 {
		e.ExpiresAt = s.Now().Add(ttl)
	} else {
		e.ExpiresAt = time.Time{}
	}
	s.StoreEntry(key, e)
}
