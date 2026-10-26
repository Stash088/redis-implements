package store

import (
	"math"
	"math/rand"
	"strconv"
)

// HashIncrBy increments the integer value of a hash field by delta. The field
// is created with the value 0 when it does not exist.
func (s *Store) HashIncrBy(key, field []byte, delta int64) (int64, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, err := s.hashEntryForWriteLocked(sh, key)
	if err != nil {
		return 0, err
	}
	f := string(field)
	var current int64
	if v, ok := e.Hash[f]; ok {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		current = n
	}
	if delta > 0 && current > math.MaxInt64-delta {
		return 0, ErrOverflow
	}
	if delta < 0 && current < math.MinInt64-delta {
		return 0, ErrOverflow
	}
	current += delta
	s.setHashFieldLocked(sh, e, f, []byte(strconv.FormatInt(current, 10)))
	return current, nil
}

// HashIncrByFloat increments the float value of a hash field by delta. The
// field is created with the value 0 when it does not exist.
func (s *Store) HashIncrByFloat(key, field []byte, delta float64) (float64, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, err := s.hashEntryForWriteLocked(sh, key)
	if err != nil {
		return 0, err
	}
	f := string(field)
	var current float64
	if v, ok := e.Hash[f]; ok {
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, ErrNotFloat
		}
		current = n
	}
	result := current + delta
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return 0, ErrNotFloat
	}
	s.setHashFieldLocked(sh, e, f, []byte(FormatFloat(result)))
	return result, nil
}

// hashEntryForWriteLocked returns the hash under key, creating it when it is
// missing. Callers must hold the write lock of the shard that owns the key.
func (s *Store) hashEntryForWriteLocked(sh *shard, key []byte) (*Entry, error) {
	e, ok := s.entryLocked(sh, key)
	if !ok {
		e = &Entry{Type: TypeHash, Hash: make(map[string][]byte), cost: len(string(key))}
		s.addBytes(sh, int64(len(string(key))))
		sh.data[string(key)] = e
		return e, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	s.markAccess(e)
	return e, nil
}

// setHashFieldLocked stores a field of a hash, updating the memory accounting.
// Callers must hold the write lock of the shard that owns the key.
func (s *Store) setHashFieldLocked(sh *shard, e *Entry, field string, value []byte) {
	if old, ok := e.Hash[field]; ok {
		e.Hash[field] = value
		s.addCost(sh, e, len(value)-len(old))
		return
	}
	e.Hash[field] = value
	// A new field changes the set of the fields: the snapshot of the scan
	// commands does not hold it any more.
	e.bumpVersion()
	s.addCost(sh, e, len(field)+len(value))
}

// HashStrLen returns the length of the value of a hash field. It is 0 when the
// field does not exist.
func (s *Store) HashStrLen(key, field []byte) (int, bool, error) {
	v, ok, err := readEntry(s, key, func(e *Entry) ([]byte, error) {
		if e.Type != TypeHash {
			return nil, ErrWrongType
		}
		val, exists := e.Hash[string(field)]
		if !exists {
			return nil, errNotFound
		}
		return val, nil
	})
	if err == errNotFound {
		return 0, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return len(v), true, nil
}

// HashMGet returns the values of the given hash fields. A missing field
// yields a nil slice.
func (s *Store) HashMGet(key []byte, fields [][]byte) ([][]byte, error) {
	out := make([][]byte, len(fields))
	_, _, err := readEntry(s, key, func(e *Entry) (bool, error) {
		if e.Type != TypeHash {
			return false, ErrWrongType
		}
		for i, f := range fields {
			if v, ok := e.Hash[string(f)]; ok {
				out[i] = v
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// HashSetNX sets the value of a hash field only when the field does not exist.
// It reports whether the field was set.
func (s *Store) HashSetNX(key, field, value []byte) (bool, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, err := s.hashEntryForWriteLocked(sh, key)
	if err != nil {
		return false, err
	}
	f := string(field)
	if _, exists := e.Hash[f]; exists {
		return false, nil
	}
	s.setHashFieldLocked(sh, e, f, value)
	return true, nil
}

// HashRandomField returns a random field of the hash. ok is false when the
// hash is empty.
func (s *Store) HashRandomField(key []byte) ([]byte, bool, error) {
	fields, err := s.hashFieldList(key)
	if err != nil {
		return nil, false, err
	}
	if len(fields) == 0 {
		return nil, false, nil
	}
	return fields[rand.Intn(len(fields))], true, nil
}

// HashRandomFields returns count random fields of the hash. A positive count
// returns distinct fields, a negative one returns |count| fields, possibly
// with repetitions, like Redis does.
func (s *Store) HashRandomFields(key []byte, count int) ([]FieldValue, bool, error) {
	fields, err := s.hashFieldList(key)
	if err != nil {
		return nil, false, err
	}
	if len(fields) == 0 {
		return nil, false, nil
	}
	out := make([]FieldValue, 0, len(fields))
	if count < 0 {
		for i := 0; i < -count; i++ {
			f := fields[rand.Intn(len(fields))]
			out = append(out, FieldValue{Field: f})
		}
		return out, true, nil
	}
	if count > len(fields) {
		count = len(fields)
	}
	// Pick distinct fields.
	perm := rand.Perm(len(fields))
	for i := 0; i < count; i++ {
		out = append(out, FieldValue{Field: fields[perm[i]]})
	}
	return out, true, nil
}

// hashFieldList returns the fields of the hash under key.
func (s *Store) hashFieldList(key []byte) ([][]byte, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([][]byte, error) {
		if e.Type != TypeHash {
			return nil, ErrWrongType
		}
		fields := make([][]byte, 0, len(e.Hash))
		for f := range e.Hash {
			fields = append(fields, []byte(f))
		}
		return fields, nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return out, nil
}

// HashGetDel removes the given fields of the hash and returns their values. A
// field that does not exist yields a nil value, like Redis does.
func (s *Store) HashGetDel(key []byte, fields ...[]byte) ([][]byte, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	out := make([][]byte, len(fields))
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return out, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	for i, f := range fields {
		name := string(f)
		old, exists := e.Hash[name]
		if !exists {
			continue
		}
		out[i] = old
		delete(e.Hash, name)
		// The field is gone: the snapshot of the scan commands does not
		// hold it any more.
		e.bumpVersion()
		s.addCost(sh, e, -(len(name) + len(old)))
	}
	if len(e.Hash) == 0 {
		s.addBytes(sh, -int64(e.cost))
		delete(sh.data, string(key))
	}
	return out, nil
}
