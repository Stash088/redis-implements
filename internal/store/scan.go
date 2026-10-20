package store

import "sort"

// FieldValue is one field of a hash together with its value.
type FieldValue struct {
	Field []byte
	Value []byte
}

// ScanHash returns the fields of the hash under key, sorted by name, with
// their values. It is the backing of the HSCAN command.
func (s *Store) ScanHash(key []byte) ([]FieldValue, bool, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([]FieldValue, error) {
		if e.Type != TypeHash {
			return nil, ErrWrongType
		}
		fields := make([]FieldValue, 0, len(e.Hash))
		for f, v := range e.Hash {
			fields = append(fields, FieldValue{Field: []byte(f), Value: v})
		}
		sort.Slice(fields, func(i, j int) bool {
			return string(fields[i].Field) < string(fields[j].Field)
		})
		return fields, nil
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return out, true, nil
}

// ScanSet returns the members of the set under key, sorted. It is the
// backing of the SSCAN command.
func (s *Store) ScanSet(key []byte) ([][]byte, bool, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([][]byte, error) {
		if e.Type != TypeSet {
			return nil, ErrWrongType
		}
		members := make([][]byte, 0, len(e.Set))
		for m := range e.Set {
			members = append(members, []byte(m))
		}
		sort.Slice(members, func(i, j int) bool {
			return string(members[i]) < string(members[j])
		})
		return members, nil
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return out, true, nil
}

// ScanZSet returns the members of the zset under key ordered by score (and
// lexicographically for equal scores). It is the backing of the ZSCAN
// command.
func (s *Store) ScanZSet(key []byte) ([]ZEntry, bool, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([]ZEntry, error) {
		if e.Type != TypeZSet {
			return nil, ErrWrongType
		}
		return e.ZSet.Range(0, -1, false), nil
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return out, true, nil
}
