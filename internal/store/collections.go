package store

// ListPush inserts one or more values at the front (left=true) or back
// (left=false) of the list under key, creating it if missing. It returns the
// new list length.
func (s *Store) ListPush(key []byte, left bool, vals ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		e = &Entry{Type: TypeList, List: newList(), cost: len(string(key))}
		s.usedBytes += len(string(key))
		s.data[string(key)] = e
	} else if e.Type != TypeList {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	s.markAccess(e)
	if left {
		for i := range vals {
			e.List.PushFront(vals[i])
			s.addCost(e, len(vals[i])+elemOverhead)
		}
	} else {
		for i := range vals {
			e.List.PushBack(vals[i])
			s.addCost(e, len(vals[i])+elemOverhead)
		}
	}
	return e.List.Len(), nil
}

// ListPop removes and returns the front (left=true) or back (left=false)
// element. ok is false when the list is empty or the key is missing. The key is
// deleted once its list becomes empty.
func (s *Store) ListPop(key []byte, left bool) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, false, nil
	}
	if e.Type != TypeList {
		return nil, false, ErrWrongType
	}
	var v []byte
	if left {
		v, ok = e.List.PopFront()
	} else {
		v, ok = e.List.PopBack()
	}
	if !ok {
		return nil, false, nil
	}
	s.addCost(e, -(len(v) + elemOverhead))
	if e.List.Len() == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return v, true, nil
}

// ListLen returns the length of the list under key. ok is false when missing.
func (s *Store) ListLen(key []byte) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, false, nil
	}
	if e.Type != TypeList {
		return 0, false, ErrWrongType
	}
	return e.List.Len(), true, nil
}

// ListRange returns the elements in the inclusive index range [start, stop].
func (s *Store) ListRange(key []byte, start, stop int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeList {
		return nil, ErrWrongType
	}
	return e.List.Range(start, stop), nil
}

// ListIndex returns the element at index i. ok is false when out of range or
// the key is missing.
func (s *Store) ListIndex(key []byte, i int) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, false, nil
	}
	if e.Type != TypeList {
		return nil, false, ErrWrongType
	}
	v, ok := e.List.Index(i)
	return v, ok, nil
}

// ListSet replaces the element at index i. It returns false when out of range.
func (s *Store) ListSet(key []byte, i int, v []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false, nil
	}
	if e.Type != TypeList {
		return false, ErrWrongType
	}
	old, _ := e.List.Index(i)
	s.touchLocked(string(key))
	okSet := e.List.Set(i, v)
	if okSet {
		s.addCost(e, len(v)-len(old))
	}
	return okSet, nil
}

// HashSet sets field to value in the hash under key, creating it if missing.
// It returns 1 when the field is new and 0 when it already existed.
func (s *Store) HashSet(key, field, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		e = &Entry{Type: TypeHash, Hash: make(map[string][]byte), cost: len(string(key))}
		s.usedBytes += len(string(key))
		s.data[string(key)] = e
	} else if e.Type != TypeHash {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	s.markAccess(e)
	f := string(field)
	if old, exists := e.Hash[f]; exists {
		e.Hash[f] = value
		s.addCost(e, len(value)-len(old))
		return 0, nil
	}
	e.Hash[f] = value
	s.addCost(e, len(f)+len(value))
	return 1, nil
}

// HashGet returns the value of field in the hash under key. ok is false when
// the field (or key) is missing.
func (s *Store) HashGet(key, field []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, false, nil
	}
	if e.Type != TypeHash {
		return nil, false, ErrWrongType
	}
	v, ok := e.Hash[string(field)]
	return v, ok, nil
}

// HashGetAll returns all field/value pairs as a flat slice
// (field, value, field, value, ...).
func (s *Store) HashGetAll(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	out := make([][]byte, 0, len(e.Hash)*2)
	for f, v := range e.Hash {
		out = append(out, []byte(f), v)
	}
	return out, nil
}

// HashDel removes fields from the hash under key, returning the number removed.
// The key is deleted when its hash becomes empty.
func (s *Store) HashDel(key []byte, fields ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeHash {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	removed := 0
	for _, f := range fields {
		if old, exists := e.Hash[string(f)]; exists {
			delete(e.Hash, string(f))
			s.addCost(e, -(len(string(f)) + len(old)))
			removed++
		}
	}
	if len(e.Hash) == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return removed, nil
}

// HashLen returns the number of fields in the hash under key. ok is false when
// missing.
func (s *Store) HashLen(key []byte) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, false, nil
	}
	if e.Type != TypeHash {
		return 0, false, ErrWrongType
	}
	return len(e.Hash), true, nil
}

// HashExists reports whether field exists in the hash under key.
func (s *Store) HashExists(key, field []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false, nil
	}
	if e.Type != TypeHash {
		return false, ErrWrongType
	}
	_, exists := e.Hash[string(field)]
	return exists, nil
}

// HashKeys returns all field names in the hash under key.
func (s *Store) HashKeys(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	out := make([][]byte, 0, len(e.Hash))
	for f := range e.Hash {
		out = append(out, []byte(f))
	}
	return out, nil
}

// HashVals returns all values in the hash under key.
func (s *Store) HashVals(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	out := make([][]byte, 0, len(e.Hash))
	for _, v := range e.Hash {
		out = append(out, v)
	}
	return out, nil
}

// SetAdd adds members to the set under key, creating it if missing, and returns
// the number of newly added members.
func (s *Store) SetAdd(key []byte, members ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		e = &Entry{Type: TypeSet, Set: make(map[string]struct{}), cost: len(string(key))}
		s.usedBytes += len(string(key))
		s.data[string(key)] = e
	} else if e.Type != TypeSet {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	s.markAccess(e)
	added := 0
	for _, m := range members {
		if _, exists := e.Set[string(m)]; !exists {
			e.Set[string(m)] = struct{}{}
			s.addCost(e, len(m)+elemOverhead)
			added++
		}
	}
	return added, nil
}

// SetRem removes members from the set under key, returning the number removed.
// The key is deleted when its set becomes empty.
func (s *Store) SetRem(key []byte, members ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeSet {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))
	removed := 0
	for _, m := range members {
		if _, exists := e.Set[string(m)]; exists {
			delete(e.Set, string(m))
			s.addCost(e, -(len(m) + elemOverhead))
			removed++
		}
	}
	if len(e.Set) == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return removed, nil
}

// SetMembers returns all members of the set under key.
func (s *Store) SetMembers(key []byte) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeSet {
		return nil, ErrWrongType
	}
	out := make([][]byte, 0, len(e.Set))
	for m := range e.Set {
		out = append(out, []byte(m))
	}
	return out, nil
}

// SetIsMember reports whether member is in the set under key.
func (s *Store) SetIsMember(key, member []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false, nil
	}
	if e.Type != TypeSet {
		return false, ErrWrongType
	}
	_, exists := e.Set[string(member)]
	return exists, nil
}

// SetCard returns the cardinality of the set under key. ok is false when
// missing.
func (s *Store) SetCard(key []byte) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, false, nil
	}
	if e.Type != TypeSet {
		return 0, false, ErrWrongType
	}
	return len(e.Set), true, nil
}
