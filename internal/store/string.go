package store

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// FormatFloat renders a float the way Redis does for INCRBYFLOAT: the exact
// decimal value with up to 17 significant digits, without an exponent and
// without trailing zeros.
func FormatFloat(f float64) string {
	s := strconv.FormatFloat(f, 'f', 17, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimSuffix(s, ".")
	if s == "" || s == "-" {
		return "0"
	}
	return s
}

// GetStrings returns the values of the given keys. A missing key and a key
// holding a non-string value both yield a nil slice, like Redis does for
// MGET.
func (s *Store) GetStrings(keys ...[]byte) [][]byte {
	out := make([][]byte, len(keys))
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	for i, key := range keys {
		e, ok := s.data[string(key)]
		if !ok || e.Expired(now) || e.Type != TypeString {
			continue
		}
		e.lastAccess.Store(now.UnixNano())
		out[i] = e.Str
	}
	return out
}

// SetStrings sets several keys at once. It reports whether all the keys were
// set (false when MSETNX found one of them existing).
func (s *Store) SetStrings(pairs [][]byte, onlyIfAbsent bool) bool {
	if len(pairs)%2 != 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if onlyIfAbsent {
		for i := 0; i < len(pairs); i += 2 {
			if e, ok := s.data[string(pairs[i])]; ok && !e.Expired(now) {
				return false
			}
		}
	}
	for i := 0; i < len(pairs); i += 2 {
		s.setStringLocked(pairs[i], pairs[i+1], time.Time{})
	}
	return true
}

// setStringLocked stores a string value, replacing whatever was under key.
// Callers must hold s.mu.
func (s *Store) setStringLocked(key, value []byte, expiresAt time.Time) {
	k := string(key)
	s.touchLocked(k)
	cost := len(k) + len(value)
	s.usedBytes += cost
	if e, ok := s.data[k]; ok {
		s.usedBytes -= e.cost
	}
	e := &Entry{Type: TypeString, Str: value, ExpiresAt: expiresAt, cost: cost}
	e.lastAccess.Store(s.clock().UnixNano())
	s.data[k] = e
}

// GetSet sets the value of key and returns the previous value. The TTL of the
// key is cleared, like Redis does.
func (s *Store) GetSet(key, value []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var old []byte
	if e, ok := s.entryLocked(key); ok {
		if e.Type != TypeString {
			return nil, false, ErrWrongType
		}
		old = e.Str
	}
	s.setStringLocked(key, value, time.Time{})
	return old, old != nil, nil
}

// GetDel returns the value of key and deletes it.
func (s *Store) GetDel(key []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, false, nil
	}
	if e.Type != TypeString {
		return nil, false, ErrWrongType
	}
	v := e.Str
	s.touchLocked(string(key))
	s.usedBytes -= e.cost
	delete(s.data, string(key))
	return v, true, nil
}

// GetEx returns the value of key and optionally changes its expiration:
// expiresAt is the new absolute expiration, and clearTTL removes it.
func (s *Store) GetEx(key []byte, expiresAt time.Time, clearTTL bool) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, false, nil
	}
	if e.Type != TypeString {
		return nil, false, ErrWrongType
	}
	v := e.Str
	s.touchLocked(string(key))
	if clearTTL {
		e.ExpiresAt = time.Time{}
	} else {
		e.ExpiresAt = expiresAt
	}
	return v, true, nil
}

// SetEx sets the value of key with a TTL.
func (s *Store) SetEx(key, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStringLocked(key, value, s.clock().Add(ttl))
}

// SetNX sets the value of key only when it does not exist. It reports whether
// the value was set.
func (s *Store) SetNX(key, value []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if e, ok := s.data[string(key)]; ok && !e.Expired(now) {
		return false
	}
	s.setStringLocked(key, value, time.Time{})
	return true
}

// GetRange returns the substring of the value of key in the inclusive range
// [start, stop]. Negative indices count from the end; an empty or out-of-range
// range yields an empty slice.
func (s *Store) GetRange(key []byte, start, stop int64) ([]byte, bool, error) {
	v, ok, err := readEntry(s, key, func(e *Entry) ([]byte, error) {
		if e.Type != TypeString {
			return nil, ErrWrongType
		}
		return e.Str, nil
	})
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	n := int64(len(v))
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
	if start > stop || start >= n || n == 0 {
		return []byte{}, true, nil
	}
	return v[start : stop+1], true, nil
}

// SetRange overwrites part of the string stored at key, padding it with zero
// bytes when needed. It returns the length of the string after the operation.
func (s *Store) SetRange(key []byte, offset int64, value []byte) (int, error) {
	if offset < 0 {
		return 0, ErrOffsetOutOfRange
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		e = &Entry{Type: TypeString, Str: []byte{}, cost: len(string(key))}
		s.usedBytes += len(string(key))
		s.data[string(key)] = e
	} else if e.Type != TypeString {
		return 0, ErrWrongType
	}
	s.touchLocked(string(key))

	end := offset + int64(len(value))
	if end > int64(len(e.Str)) {
		// Grow the string with zero bytes.
		grown := make([]byte, end)
		copy(grown, e.Str)
		s.addCost(e, int(end)-len(e.Str))
		e.Str = grown
	}
	copy(e.Str[offset:], value)
	return len(e.Str), nil
}

// IncrByFloat increments the value of key by delta and stores the result as a
// string. It returns the new value.
func (s *Store) IncrByFloat(key []byte, delta float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var current float64
	var expiresAt time.Time
	if e, ok := s.entryLocked(key); ok {
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		v, err := strconv.ParseFloat(string(e.Str), 64)
		if err != nil {
			return 0, ErrNotFloat
		}
		current = v
		expiresAt = e.ExpiresAt
	}
	result := current + delta
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return 0, ErrNotFloat
	}
	s.setStringLocked(key, []byte(FormatFloat(result)), expiresAt)
	return result, nil
}
