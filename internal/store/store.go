package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

// ErrNotInteger is returned when INCR/DECR is applied to a non-numeric value.
var ErrNotInteger = errors.New("store: value is not an integer")

// ErrOverflow is returned when INCR/DECR would overflow an int64.
var ErrOverflow = errors.New("store: increment would overflow int64")

// ErrWrongType is returned when a command is applied to a key holding a value
// of an incompatible type.
var ErrWrongType = errors.New("store: wrong type")

// Type identifies the kind of value stored in an Entry.
type Type uint8

const (
	TypeString Type = iota + 1
	TypeList
	TypeHash
	TypeSet
	TypeZSet
)

// String returns the lowercase name used by the TYPE command.
func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	case TypeHash:
		return "hash"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	default:
		return "none"
	}
}

// Entry is a single key/value pair in the store. Exactly one container field is
// meaningful, selected by Type.
type Entry struct {
	Type       Type
	Str        []byte              // TypeString
	List       *List               // TypeList
	Hash       map[string][]byte   // TypeHash
	Set        map[string]struct{} // TypeSet
	ZSet       *ZSet               // TypeZSet
	ExpiresAt  time.Time           // zero value means no expiry
	cost       int                 // approximate memory footprint, incl. key
	lastAccess int64               // nanos, updated on access (allkeys-lru)
}

// Expired reports whether the entry has a TTL that has passed.
func (e *Entry) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)
}

// Store is a thread-safe in-memory key/value store.
type Store struct {
	mu        sync.RWMutex
	data      map[string]*Entry
	watches   map[string]map[Watcher]struct{} // WATCH/CAS registry
	usedBytes int                             // sum of entry costs
	clock     func() time.Time                // injectable for tests
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		data:    make(map[string]*Entry),
		watches: make(map[string]map[Watcher]struct{}),
		clock:   time.Now,
	}
}

// UsedBytes returns the approximate memory footprint of live entries.
func (s *Store) UsedBytes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usedBytes
}

// Len returns the number of live keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	n := 0
	for _, e := range s.data {
		if !e.Expired(now) {
			n++
		}
	}
	return n
}

// entryLocked returns the live entry for key, or nil when missing/expired
// (removing it in the latter case). Callers must hold s.mu.
func (s *Store) entryLocked(key []byte) (*Entry, bool) {
	now := s.clock()
	k := string(key)
	e, ok := s.data[k]
	if !ok {
		return nil, false
	}
	if e.Expired(now) {
		// Lazy expiration counts as a modification (like Redis), so WATCH
		// clients get their transaction aborted.
		s.touchLocked(k)
		s.usedBytes -= e.cost
		delete(s.data, k)
		return nil, false
	}
	e.lastAccess = now.UnixNano()
	return e, true
}

// newStringEntry replaces whatever is stored under key with a string entry.
// Callers must hold s.mu.
func (s *Store) newStringEntry(key []byte, value []byte, expiresAt time.Time) {
	s.touchLocked(string(key))
	k := string(key)
	if value == nil {
		if e, ok := s.data[k]; ok {
			s.usedBytes -= e.cost
			delete(s.data, k)
		}
		return
	}
	cost := len(k) + len(value)
	s.usedBytes += cost
	if e, ok := s.data[k]; ok {
		s.usedBytes -= e.cost
	}
	s.data[k] = &Entry{Type: TypeString, Str: value, ExpiresAt: expiresAt, cost: cost, lastAccess: s.clock().UnixNano()}
}

// GetString returns the string value stored under key. ok is false when the
// key is missing. ErrWrongType is returned when the key holds a non-string.
func (s *Store) GetString(key []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, false, nil
	}
	if e.Type != TypeString {
		return nil, false, ErrWrongType
	}
	return e.Str, true, nil
}

// GetType returns the type of the value stored under key, or (0, false) when
// missing.
func (s *Store) GetType(key []byte) (Type, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, false
	}
	return e.Type, true
}

// SetString stores a string value under key, overwriting any existing type.
func (s *Store) SetString(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.newStringEntry(key, value, time.Time{})
}

// SetStringWithTTL stores a string value under key with a TTL, overwriting any
// existing type.
func (s *Store) SetStringWithTTL(key, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.newStringEntry(key, value, s.clock().Add(ttl))
}

// SetStringWithExpiryAt stores a string value under key, expiring at the given
// instant.
func (s *Store) SetStringWithExpiryAt(key, value []byte, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.newStringEntry(key, value, expiresAt)
}

// Delete removes one or more keys, returning the number of removed keys.
func (s *Store) Delete(keys ...[]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	removed := 0
	for _, key := range keys {
		k := string(key)
		if e, ok := s.data[k]; ok {
			if !e.Expired(now) {
				removed++
			}
			s.touchLocked(k)
			s.usedBytes -= e.cost
			delete(s.data, k)
		}
	}
	return removed
}

// Exists reports how many of the given keys exist (and are not expired).
func (s *Store) Exists(keys ...[]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	n := 0
	for _, key := range keys {
		if e, ok := s.data[string(key)]; ok && !e.Expired(now) {
			n++
		}
	}
	return n
}

// Expire sets a TTL on key. ok is false when the key does not exist.
func (s *Store) Expire(key []byte, ttl time.Duration) bool {
	return s.ExpireAt(key, s.clock().Add(ttl))
}

// ExpireAt sets an absolute expiry instant on key. ok is false when the key
// does not exist.
func (s *Store) ExpireAt(key []byte, expiresAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false
	}
	e.ExpiresAt = expiresAt
	s.touchLocked(string(key))
	return true
}

// TTL returns the remaining time-to-live of key. The second return value is
// false when the key does not exist.
func (s *Store) TTL(key []byte) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, false
	}
	if e.ExpiresAt.IsZero() {
		return -1, true // no expiry (Redis: -1)
	}
	return e.ExpiresAt.Sub(s.clock()), true
}

// Persist removes any TTL on key. ok is false when the key does not exist.
func (s *Store) Persist(key []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false
	}
	e.ExpiresAt = time.Time{}
	s.touchLocked(string(key))
	return true
}

// IncrBy atomically increments the numeric string value of key by delta. When
// key is missing it is treated as 0. A non-string value returns ErrWrongType, a
// non-numeric one ErrNotInteger, and overflow ErrOverflow.
func (s *Store) IncrBy(key []byte, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var current int64
	var expiresAt time.Time
	if e, ok := s.entryLocked(key); ok {
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		n, err := parseInt(e.Str)
		if err != nil {
			return 0, ErrNotInteger
		}
		current = n
		expiresAt = e.ExpiresAt // preserve TTL, like Redis
	}
	if delta > 0 && current > math.MaxInt64-delta {
		return 0, ErrOverflow
	}
	if delta < 0 && current < math.MinInt64-delta {
		return 0, ErrOverflow
	}
	current += delta
	s.newStringEntry(key, strconv.AppendInt(nil, current, 10), expiresAt)
	return current, nil
}

// Append appends suffix to the string value of key, creating it if missing, and
// returns the new length.
func (s *Store) Append(key, suffix []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchLocked(string(key))
	if e, ok := s.entryLocked(key); ok {
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		e.Str = append(e.Str, suffix...)
		return len(e.Str), nil
	}
	s.newStringEntry(key, append([]byte(nil), suffix...), time.Time{})
	return len(suffix), nil
}

// StrLen returns the length of the string value under key. ok is false when
// missing.
func (s *Store) StrLen(key []byte) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, false, nil
	}
	if e.Type != TypeString {
		return 0, false, ErrWrongType
	}
	return len(e.Str), true, nil
}

// Keys returns all live keys.
func (s *Store) Keys() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	keys := make([][]byte, 0, len(s.data))
	for k, e := range s.data {
		if e.Expired(now) {
			continue
		}
		keys = append(keys, []byte(k))
	}
	return keys
}

// Flush removes all keys and returns the number removed.
func (s *Store) Flush() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.lenLocked()
	for k := range s.data {
		s.touchLocked(k)
	}
	s.data = make(map[string]*Entry)
	s.usedBytes = 0
	return n
}

// ExpireCycle performs one active-expiration pass, removing expired keys.
// It returns the number of keys removed.
func (s *Store) ExpireCycle() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	n := 0
	for k, e := range s.data {
		if e.Expired(now) {
			s.touchLocked(k)
			s.usedBytes -= e.cost
			delete(s.data, k)
			n++
		}
	}
	return n
}

func (s *Store) lenLocked() int {
	now := s.clock()
	n := 0
	for _, e := range s.data {
		if !e.Expired(now) {
			n++
		}
	}
	return n
}

// SetClock overrides the time source (used by tests).
func (s *Store) SetClock(f func() time.Time) { s.clock = f }

func parseInt(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}
