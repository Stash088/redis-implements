package store

import "sort"

// FieldValue is one field of a hash together with its value.
type FieldValue struct {
	Field []byte
	Value []byte
}

// scanSnapshot is a snapshot of the members of a collection. The scan
// commands (HSCAN, SSCAN, ZSCAN) walk it by the index of the member, so a
// whole traversal costs one snapshot instead of one per call.
//
// The snapshot is built once and reused by the following calls of the same
// traversal. It becomes stale when the collection changes: the version of the
// collection is stored in the snapshot and compared with the current one.
type scanSnapshot struct {
	// version is the version of the collection the snapshot was built from.
	version uint64
	// names holds the members of a hash (its field names) or of a set,
	// sorted by name.
	names []string
	// members and scores hold the members of a sorted set, in score order.
	// The member bytes are the ones of the collection: they are never
	// modified in place, so the snapshot may keep them.
	members [][]byte
	scores  []float64
}

// hashSnapshot returns the snapshot of the fields of a hash, building it when
// the collection changed since the last one.
//
// The caller must hold the lock of the shard that owns the entry.
func (e *Entry) hashSnapshot() *scanSnapshot {
	if c := e.scanCache.Load(); c != nil && c.version == e.version {
		return c
	}
	names := make([]string, 0, len(e.Hash))
	for f := range e.Hash {
		names = append(names, f)
	}
	sort.Strings(names)
	snap := &scanSnapshot{version: e.version, names: names}
	e.scanCache.Store(snap)
	return snap
}

// setSnapshot returns the snapshot of the members of a set, building it when
// the collection changed since the last one.
//
// The caller must hold the lock of the shard that owns the entry.
func (e *Entry) setSnapshot() *scanSnapshot {
	if c := e.scanCache.Load(); c != nil && c.version == e.version {
		return c
	}
	names := make([]string, 0, len(e.Set))
	for m := range e.Set {
		names = append(names, m)
	}
	sort.Strings(names)
	snap := &scanSnapshot{version: e.version, names: names}
	e.scanCache.Store(snap)
	return snap
}

// zsetSnapshot returns the snapshot of the members of a sorted set, building
// it when the collection changed since the last one.
//
// The caller must hold the lock of the shard that owns the entry.
func (e *Entry) zsetSnapshot() *scanSnapshot {
	if c := e.scanCache.Load(); c != nil && c.version == e.ZSet.version {
		return c
	}
	entries := e.ZSet.Range(0, -1, false)
	members := make([][]byte, 0, len(entries))
	scores := make([]float64, 0, len(entries))
	for _, en := range entries {
		members = append(members, en.Member)
		scores = append(scores, en.Score)
	}
	snap := &scanSnapshot{version: e.ZSet.version, members: members, scores: scores}
	e.scanCache.Store(snap)
	return snap
}

// pageBounds returns the bounds of a page of count elements that starts at
// the offset start, for a collection of total elements.
func pageBounds(start, count, total int) (int, int) {
	if start < 0 {
		start = 0
	}
	if start > total {
		start = total
	}
	if count < 0 {
		count = 0
	}
	hi := start + count
	if hi > total {
		hi = total
	}
	return start, hi
}

// ScanHashPage returns a page of the fields of the hash under key, sorted by
// name. The page starts at the offset start and holds at most count fields.
// total is the number of fields of the hash.
//
// The values are read when the page is built, so a page never shows a stale
// value. The order of the fields is the order of the snapshot, which stays
// the same during a traversal.
func (s *Store) ScanHashPage(key []byte, start, count int) ([]FieldValue, int, bool, error) {
	var total int
	page, found, err := readEntry(s, key, func(e *Entry) ([]FieldValue, error) {
		if e.Type != TypeHash {
			return nil, ErrWrongType
		}
		snap := e.hashSnapshot()
		total = len(snap.names)
		lo, hi := pageBounds(start, count, total)
		out := make([]FieldValue, 0, hi-lo)
		for _, name := range snap.names[lo:hi] {
			out = append(out, FieldValue{Field: []byte(name), Value: e.Hash[name]})
		}
		if hi == total {
			// The traversal reached the end of the collection: the
			// snapshot is not needed any more.
			e.scanCache.Store(nil)
		}
		return out, nil
	})
	if err != nil || !found {
		return nil, 0, found, err
	}
	return page, total, true, nil
}

// ScanSetPage returns a page of the members of the set under key, sorted by
// name. The page starts at the offset start and holds at most count members.
// total is the number of members of the set.
func (s *Store) ScanSetPage(key []byte, start, count int) ([][]byte, int, bool, error) {
	var total int
	page, found, err := readEntry(s, key, func(e *Entry) ([][]byte, error) {
		if e.Type != TypeSet {
			return nil, ErrWrongType
		}
		snap := e.setSnapshot()
		total = len(snap.names)
		lo, hi := pageBounds(start, count, total)
		out := make([][]byte, 0, hi-lo)
		for _, name := range snap.names[lo:hi] {
			out = append(out, []byte(name))
		}
		if hi == total {
			e.scanCache.Store(nil)
		}
		return out, nil
	})
	if err != nil || !found {
		return nil, 0, found, err
	}
	return page, total, true, nil
}

// ScanZSetPage returns a page of the members of the sorted set under key, in
// score order. The page starts at the offset start and holds at most count
// members. total is the number of members of the set.
func (s *Store) ScanZSetPage(key []byte, start, count int) ([]ZEntry, int, bool, error) {
	var total int
	page, found, err := readEntry(s, key, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		snap := e.zsetSnapshot()
		total = len(snap.members)
		lo, hi := pageBounds(start, count, total)
		out := make([]ZEntry, 0, hi-lo)
		for i := lo; i < hi; i++ {
			out = append(out, ZEntry{Member: snap.members[i], Score: snap.scores[i]})
		}
		if hi == total {
			e.scanCache.Store(nil)
		}
		return out, nil
	})
	if err != nil || !found {
		return nil, 0, found, err
	}
	return page, total, true, nil
}
