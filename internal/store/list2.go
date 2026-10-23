package store

import "bytes"

// ListInsert inserts value before or after the first occurrence of pivot in
// the list under key. It returns the new length, -1 when the pivot was not
// found and 0 when the key does not exist, like Redis does.
func (s *Store) ListInsert(key []byte, before bool, pivot, value []byte) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeList {
		return 0, ErrWrongType
	}
	idx := e.List.IndexOfValue(pivot)
	if idx < 0 {
		return -1, nil
	}
	s.touchKeyLocked(sh, key)
	if before {
		e.List.InsertBefore(idx, value)
	} else {
		e.List.InsertAfter(idx, value)
	}
	s.addCost(sh, e, len(value)+elemOverhead)
	return e.List.Len(), nil
}

// ListRemove removes the occurrences of value from the list under key. A
// positive count removes from the head, a negative one from the tail and
// zero removes all of them. It returns the number of removed elements.
func (s *Store) ListRemove(key []byte, count int, value []byte) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeList {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	removed := 0
	if count >= 0 {
		for i := 0; i < e.List.Len(); i++ {
			if count > 0 && removed >= count {
				break
			}
			v, _ := e.List.Index(i)
			if !bytes.Equal(v, value) {
				continue
			}
			e.List.RemoveAt(i)
			s.addCost(sh, e, -(len(value) + elemOverhead))
			removed++
			i--
		}
	} else {
		for i := e.List.Len() - 1; i >= 0; i-- {
			if removed >= -count {
				break
			}
			v, _ := e.List.Index(i)
			if !bytes.Equal(v, value) {
				continue
			}
			e.List.RemoveAt(i)
			s.addCost(sh, e, -(len(value) + elemOverhead))
			removed++
		}
	}
	if e.List.Len() == 0 {
		sh.usedBytes -= e.cost
		delete(sh.data, string(key))
	}
	return removed, nil
}

// ListTrim removes the elements outside the inclusive range [start, stop].
func (s *Store) ListTrim(key []byte, start, stop int) error {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return nil
	}
	if e.Type != TypeList {
		return ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	n := e.List.Len()
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
	// Remove the tail first so the indexes of the head stay valid.
	for i := n - 1; i > stop; i-- {
		v, _ := e.List.Index(i)
		e.List.RemoveAt(i)
		s.addCost(sh, e, -(len(v) + elemOverhead))
	}
	for i := 0; i < start && e.List.Len() > 0; i++ {
		v, _ := e.List.Index(0)
		e.List.RemoveAt(0)
		s.addCost(sh, e, -(len(v) + elemOverhead))
	}
	if e.List.Len() == 0 {
		sh.usedBytes -= e.cost
		delete(sh.data, string(key))
	}
	return nil
}

// ListPosition returns the positions of the occurrences of value in the list
// under key. A rank of zero starts from the head, a negative rank counts from
// the tail. A count of zero returns every position, a positive count the
// first ones. maxlen limits the number of elements that are scanned.
func (s *Store) ListPosition(key []byte, value []byte, rank, count, maxlen int) ([]int, error) {
	items, ok, err := readEntry(s, key, func(e *Entry) ([][]byte, error) {
		if e.Type != TypeList {
			return nil, ErrWrongType
		}
		return e.List.Range(0, -1), nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	// The search window: MAXLEN limits how many elements are scanned.
	start, end := 0, len(items)
	if maxlen > 0 && end > maxlen {
		end = maxlen
	}
	// Collect the positions of the matches.
	matches := make([]int, 0, len(items))
	for i := start; i < end; i++ {
		if bytes.Equal(items[i], value) {
			matches = append(matches, i)
		}
	}
	if len(matches) == 0 {
		return nil, nil
	}
	// RANK selects the starting match.
	if rank != 0 {
		if rank > 0 {
			if rank > len(matches) {
				return nil, nil
			}
			matches = matches[rank-1:]
		} else {
			if -rank > len(matches) {
				return nil, nil
			}
			matches = matches[len(matches)+rank:]
		}
	}
	// COUNT limits the number of matches.
	if count > 0 && count < len(matches) {
		matches = matches[:count]
	}
	return matches, nil
}

// ListMove pops an element from one list and pushes it to another. It
// reports whether an element was moved.
func (s *Store) ListMove(source, dest []byte, fromLeft, toLeft bool) ([]byte, bool, error) {
	unlock := s.lockWrite(source, dest)
	defer unlock()
	srcSh := s.shardOf(source)
	se, ok := s.entryLocked(srcSh, source)
	if !ok {
		return nil, false, nil
	}
	if se.Type != TypeList {
		return nil, false, ErrWrongType
	}
	dstSh := s.shardOf(dest)
	de, ok := s.entryLocked(dstSh, dest)
	if !ok {
		de = &Entry{Type: TypeList, List: newList(), cost: len(string(dest))}
		dstSh.usedBytes += len(string(dest))
		dstSh.data[string(dest)] = de
	} else if de.Type != TypeList {
		return nil, false, ErrWrongType
	}
	var v []byte
	if fromLeft {
		v, _ = se.List.PopFront()
	} else {
		v, _ = se.List.PopBack()
	}
	if v == nil {
		return nil, false, nil
	}
	s.touchLocked(srcSh, string(source))
	s.touchLocked(dstSh, string(dest))
	s.addCost(srcSh, se, -(len(v) + elemOverhead))
	if toLeft {
		de.List.PushFront(v)
	} else {
		de.List.PushBack(v)
	}
	s.addCost(dstSh, de, len(v)+elemOverhead)
	if se.List.Len() == 0 {
		srcSh.usedBytes -= se.cost
		delete(srcSh.data, string(source))
	}
	return v, true, nil
}

// ListPopAny pops an element from the first non-empty list of the given
// keys. It returns the key the element came from, and reports whether an
// element was popped.
func (s *Store) ListPopAny(keys [][]byte, left bool) (key []byte, value []byte, ok bool, err error) {
	if len(keys) == 0 {
		return nil, nil, false, nil
	}
	unlock := s.lockWrite(keys...)
	defer unlock()
	now := s.clock()
	for _, k := range keys {
		sh := s.shardOf(k)
		e, ok := sh.data[string(k)]
		if !ok || e.Expired(now) {
			continue
		}
		if e.Type != TypeList {
			return nil, nil, false, ErrWrongType
		}
		if e.List.Len() == 0 {
			continue
		}
		var v []byte
		if left {
			v, _ = e.List.PopFront()
		} else {
			v, _ = e.List.PopBack()
		}
		s.touchLocked(sh, string(k))
		s.addCost(sh, e, -(len(v) + elemOverhead))
		if e.List.Len() == 0 {
			sh.usedBytes -= e.cost
			delete(sh.data, string(k))
		}
		return k, v, true, nil
	}
	return nil, nil, false, nil
}

// ListPushExist pushes values to an existing list. It reports whether the
// list existed.
func (s *Store) ListPushExist(key []byte, left bool, values [][]byte) (int, bool, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return 0, false, nil
	}
	if e.Type != TypeList {
		return 0, false, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	s.markAccess(e)
	for _, v := range values {
		if left {
			e.List.PushFront(v)
		} else {
			e.List.PushBack(v)
		}
		s.addCost(sh, e, len(v)+elemOverhead)
	}
	return e.List.Len(), true, nil
}
