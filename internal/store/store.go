package store

import (
	"errors"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNotInteger is returned when INCR/DECR is applied to a non-numeric value.
var ErrNotInteger = errors.New("store: value is not an integer")

// ErrOverflow is returned when INCR/DECR would overflow an int64.
var ErrOverflow = errors.New("store: increment would overflow int64")

// ErrWrongType is returned when a command is applied to a key holding a value
// of an incompatible type.
var ErrWrongType = errors.New("store: wrong type")

// errNotFound is an internal marker used by read-only helpers to report that
// the requested element does not exist (as opposed to a missing key).
var errNotFound = errors.New("store: not found")

// ErrNotFloat is returned when a float operation is applied to a value that
// is not a number.
var ErrNotFloat = errors.New("store: value is not a valid float")

// ErrOffsetOutOfRange is returned by SETRANGE for a negative offset.
var ErrOffsetOutOfRange = errors.New("store: offset is out of range")

// Type identifies the kind of value stored in an Entry.
type Type uint8

const (
	TypeString Type = iota + 1
	TypeList
	TypeHash
	TypeSet
	TypeZSet
	TypeStream
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
	case TypeStream:
		return "stream"
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
	Stream     *Stream             // TypeStream
	ExpiresAt  time.Time           // zero value means no expiry
	cost       int                 // approximate memory footprint, incl. key
	lastAccess atomic.Int64        // nanos, updated on access (allkeys-lru)
}

// Invariant: the byte buffers reachable from an entry are never modified in
// place. A command that changes a value (APPEND, SETRANGE) builds a new
// buffer and publishes it, so a reader that already got a reference to the
// old buffer can keep using it after the store lock is released.

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

// Touch updates the last access time of the given keys and returns the
// number of keys that exist.
func (s *Store) Touch(keys ...[]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	n := 0
	for _, key := range keys {
		e, ok := s.data[string(key)]
		if !ok || e.Expired(now) {
			continue
		}
		e.lastAccess.Store(now.UnixNano())
		n++
	}
	return n
}

// RandomKey returns a random live key. ok is false when the store is empty.
func (s *Store) RandomKey() ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	keys := make([][]byte, 0, len(s.data))
	for k, e := range s.data {
		if e.Expired(now) {
			continue
		}
		keys = append(keys, []byte(k))
	}
	if len(keys) == 0 {
		return nil, false
	}
	return keys[rand.Intn(len(keys))], true
}

// Stats returns the number of live keys and the number of keys that carry a
// TTL. It backs the "# Keyspace" section of INFO.
func (s *Store) Stats() (keys, expires int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	for _, e := range s.data {
		if e.Expired(now) {
			continue
		}
		keys++
		if !e.ExpiresAt.IsZero() {
			expires++
		}
	}
	return keys, expires
}

// entryLocked returns the live entry for key, or nil when missing/expired
// (removing it in the latter case). Callers must hold s.mu for writing.
func (s *Store) entryLocked(key []byte) (*Entry, bool) {
	now := s.clock()
	// The lookup with a []byte key does not copy the bytes.
	e, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}
	if e.Expired(now) {
		// Lazy expiration counts as a modification (like Redis), so WATCH
		// clients get their transaction aborted.
		s.touchKeyLocked(key)
		s.usedBytes -= e.cost
		delete(s.data, string(key))
		return nil, false
	}
	e.lastAccess.Store(now.UnixNano())
	return e, true
}

// readEntry runs a read-only operation on the entry stored under key while
// the store's read lock is held, so several readers can work in parallel.
// It reports ok=false when the key is missing or its TTL has passed.
//
// The closure must not modify the entry. The slices it returns stay valid
// after the lock is released: stored buffers are never modified in place
// (see the Entry invariant), so a reader may keep using them.
func readEntry[T any](s *Store, key []byte, fn func(e *Entry) (T, error)) (T, bool, error) {
	var zero T
	// The lookup with a []byte key does not copy the bytes.
	s.mu.RLock()
	e, ok := s.data[string(key)]
	if !ok {
		s.mu.RUnlock()
		return zero, false, nil
	}
	now := s.clock()
	if e.Expired(now) {
		s.mu.RUnlock()
		// The key is gone as far as the caller is concerned; drop it from
		// the map (and notify its watchers) in a separate write-locked
		// step, since the read lock cannot be upgraded.
		s.expireKey(string(key))
		return zero, false, nil
	}
	e.lastAccess.Store(now.UnixNano())
	v, err := fn(e)
	s.mu.RUnlock()
	return v, true, err
}

// expireKey removes an expired entry from the map and notifies its watchers,
// like Redis does on a lazy lookup. It takes the write lock, so callers must
// not hold any lock.
func (s *Store) expireKey(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[k]
	if !ok || !e.Expired(s.clock()) {
		return
	}
	s.touchLocked(k)
	s.usedBytes -= e.cost
	delete(s.data, k)
}

// copyByteSlices returns a copy of the slice of byte slices.
//
// The store keeps the byte buffers it is given (the RESP reader allocates a
// fresh buffer per argument, so those are safe to keep), but not the slice
// itself: the caller may reuse it for the next command.
func copyByteSlices(vals [][]byte) [][]byte {
	if len(vals) == 0 {
		return nil
	}
	out := make([][]byte, len(vals))
	copy(out, vals)
	return out
}

// newStringEntry replaces whatever is stored under key with a string entry.
// Callers must hold s.mu.
func (s *Store) newStringEntry(key []byte, value []byte, expiresAt time.Time) {
	s.touchKeyLocked(key)
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
	e := &Entry{Type: TypeString, Str: value, ExpiresAt: expiresAt, cost: cost}
	e.lastAccess.Store(s.clock().UnixNano())
	s.data[k] = e
}

// GetString returns the string value stored under key. ok is false when the
// key is missing. ErrWrongType is returned when the key holds a non-string.
func (s *Store) GetString(key []byte) ([]byte, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]byte, error) {
		if e.Type != TypeString {
			return nil, ErrWrongType
		}
		return e.Str, nil
	})
}

// GetType returns the type of the value stored under key, or (0, false) when
// missing.
func (s *Store) GetType(key []byte) (Type, bool) {
	t, ok, err := readEntry(s, key, func(e *Entry) (Type, error) {
		return e.Type, nil
	})
	if err != nil {
		return 0, false
	}
	return t, ok
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
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.touchKeyLocked(key)
	return true
}

// TTL returns the remaining time-to-live of key. The second return value is
// false when the key does not exist.
func (s *Store) TTL(key []byte) (time.Duration, bool) {
	d, ok, err := readEntry(s, key, func(e *Entry) (time.Duration, error) {
		if e.ExpiresAt.IsZero() {
			return -1, nil // no expiry (Redis: -1)
		}
		return e.ExpiresAt.Sub(s.clock()), nil
	})
	if err != nil || !ok {
		return 0, false
	}
	return d, true
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
	s.touchKeyLocked(key)
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
//
// The stored buffer is never modified in place: readers may still be copying
// the old value into a socket, so APPEND builds a new buffer and publishes it.
func (s *Store) Append(key, suffix []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchKeyLocked(key)
	if e, ok := s.entryLocked(key); ok {
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		grown := make([]byte, 0, len(e.Str)+len(suffix))
		grown = append(grown, e.Str...)
		grown = append(grown, suffix...)
		s.addCost(e, len(suffix))
		e.Str = grown
		return len(e.Str), nil
	}
	s.newStringEntry(key, append([]byte(nil), suffix...), time.Time{})
	return len(suffix), nil
}

// StrLen returns the length of the string value under key. ok is false when
// missing.
func (s *Store) StrLen(key []byte) (int, bool, error) {
	return readEntry(s, key, func(e *Entry) (int, error) {
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		return len(e.Str), nil
	})
}

// Keys returns all live keys.
func (s *Store) Keys() [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
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

// Active expiration works like Redis: every pass checks a sample of the
// keyspace instead of scanning all of it. A full scan under the write lock
// would delay every client of the database for the time of the scan, which
// shows up as a p99 spike.
const (
	// expireSample is the number of keys checked per sample.
	expireSample = 20
	// expireCycleBudget is the time one active-expiration pass may spend.
	expireCycleBudget = time.Millisecond
)

// ExpireCycle performs one active-expiration pass, removing expired keys.
// It returns the number of keys removed.
//
// The pass samples the keyspace (like Redis does) and works within a time
// budget: a full scan of the keyspace would block the writers of the
// database for as long as it takes. Expired keys are also removed lazily
// when they are accessed.
func (s *Store) ExpireCycle() int {
	deadline := time.Now().Add(expireCycleBudget)
	removed := 0
	for {
		expired := s.sampleExpired(deadline)
		if len(expired) == 0 {
			return removed
		}
		removed += s.removeExpired(expired)
		if !time.Now().Before(deadline) {
			return removed
		}
	}
}

// sampleExpired returns up to expireSample expired keys. The keys are picked
// from a random position of the map (the iteration order of a Go map is
// randomized), so the sample is spread over the keyspace.
func (s *Store) sampleExpired(deadline time.Time) []string {
	now := s.clock()
	var expired []string
	s.mu.RLock()
	sampled := 0
	for k, e := range s.data {
		sampled++
		if e.Expired(now) {
			expired = append(expired, k)
		}
		if sampled >= expireSample {
			break
		}
	}
	s.mu.RUnlock()
	return expired
}

// removeExpired deletes the given keys, one at a time, so that the write
// lock is held for as short a time as possible.
func (s *Store) removeExpired(keys []string) int {
	removed := 0
	for _, k := range keys {
		s.mu.Lock()
		e, ok := s.data[k]
		if ok && e.Expired(s.clock()) {
			s.touchLocked(k)
			s.usedBytes -= e.cost
			delete(s.data, k)
			removed++
		}
		s.mu.Unlock()
	}
	return removed
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

// Copy returns a copy of the entry that can be read without the lock of the
// store.
//
// The values (the bytes of a string, the elements of a list, the members of
// a set) are shared: they are never modified in place, so a copy of the
// pointer is enough. The structures that are changed in place (the list, the
// hash, the set, the sorted set and the stream) are copied.
//
// The persistence layer uses it to take a snapshot of the dataset: without it
// the serialization of a snapshot would race with the commands that change
// the collections.
func (e *Entry) Copy() *Entry {
	out := &Entry{
		Type:      e.Type,
		Str:       e.Str,
		ExpiresAt: e.ExpiresAt,
		cost:      e.cost,
	}
	out.lastAccess.Store(e.lastAccess.Load())
	if e.List != nil {
		out.List = e.List.Clone()
	}
	if e.Hash != nil {
		out.Hash = make(map[string][]byte, len(e.Hash))
		for k, v := range e.Hash {
			out.Hash[k] = v
		}
	}
	if e.Set != nil {
		out.Set = make(map[string]struct{}, len(e.Set))
		for k := range e.Set {
			out.Set[k] = struct{}{}
		}
	}
	if e.ZSet != nil {
		out.ZSet = e.ZSet.Clone()
	}
	if e.Stream != nil {
		out.Stream = NewStream(e.Stream.Snapshot())
	}
	return out
}
