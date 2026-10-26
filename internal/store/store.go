package store

import (
	"errors"
	"math"
	"math/rand"
	"strconv"
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

	// version counts the changes of the collection the entry holds: it says
	// whether the snapshot of the scan commands is still valid. It is
	// protected by the lock of the shard that owns the entry.
	version uint64
	// scanCache is the snapshot of the collection that the scan commands
	// (HSCAN, SSCAN, ZSCAN) walk. It is published atomically, so a reader
	// can build it without blocking the other readers of the shard.
	scanCache atomic.Pointer[scanSnapshot]
}

// Invariant: the byte buffers reachable from an entry are never modified in
// place. A command that changes a value (APPEND, SETRANGE) builds a new
// buffer and publishes it, so a reader that already got a reference to the
// old buffer can keep using it after the store lock is released.

// Expired reports whether the entry has a TTL that has passed.
func (e *Entry) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)
}

// bumpVersion marks the collection of the entry as changed: the snapshot the
// scan commands walk becomes stale and is built again on the next call.
//
// Callers must hold the write lock of the shard that owns the entry.
func (e *Entry) bumpVersion() { e.version++ }

// Store is a thread-safe in-memory key/value store. The keyspace is split
// into independently locked shards (see shard.go): a command takes the lock of
// the shard of every key it touches, so commands on different shards run
// without contending on a mutex.
type Store struct {
	shards [shardCount]shard
	clock  func() time.Time // injectable for tests
	// totalBytes is the sum of the costs of the entries of all the shards.
	// It is updated with every change, so the memory accounting of the
	// server does not need the locks of the shards.
	totalBytes atomic.Int64
}

// New returns an empty Store.
func New() *Store {
	s := &Store{clock: time.Now}
	for i := range s.shards {
		s.shards[i].init()
	}
	return s
}

// UsedBytes returns the approximate memory footprint of live entries. The
// total of the store is kept up to date by the writes, so the number is
// read without taking the locks of the shards: the server checks it on the
// path of every write command.
func (s *Store) UsedBytes() int {
	return int(s.totalBytes.Load())
}

// addBytes applies a delta to the memory accounting of a shard and to the
// total of the store.
//
// The caller must hold the write lock of the shard.
func (s *Store) addBytes(sh *shard, delta int64) {
	sh.usedBytes.Add(delta)
	s.totalBytes.Add(delta)
}

// clearBytes drops the memory accounting of a shard that was emptied.
//
// The caller must hold the write lock of the shard.
func (s *Store) clearBytes(sh *shard) {
	s.addBytes(sh, -sh.usedBytes.Load())
	sh.usedBytes.Store(0)
}

// Len returns the number of live keys.
func (s *Store) Len() int {
	now := s.clock()
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, e := range sh.data {
			if !e.Expired(now) {
				n++
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// Touch updates the last access time of the given keys and returns the
// number of keys that exist.
func (s *Store) Touch(keys ...[]byte) int {
	if len(keys) == 0 {
		return 0
	}
	unlock := s.lockWrite(keys...)
	defer unlock()
	now := s.clock()
	n := 0
	for _, key := range keys {
		sh := s.shardOf(key)
		e, ok := sh.data[string(key)]
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
	now := s.clock()
	start := rand.Intn(shardCount)
	for i := 0; i < shardCount; i++ {
		sh := &s.shards[(start+i)&shardMask]
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.Expired(now) {
				continue
			}
			key := []byte(k)
			sh.mu.RUnlock()
			return key, true
		}
		sh.mu.RUnlock()
	}
	return nil, false
}

// Stats returns the number of live keys and the number of keys that carry a
// TTL. It backs the "# Keyspace" section of INFO.
func (s *Store) Stats() (keys, expires int) {
	now := s.clock()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, e := range sh.data {
			if e.Expired(now) {
				continue
			}
			keys++
			if !e.ExpiresAt.IsZero() {
				expires++
			}
		}
		sh.mu.RUnlock()
	}
	return keys, expires
}

// entryLocked returns the live entry for key, or nil when missing/expired
// (removing it in the latter case). Callers must hold the write lock of the
// shard that owns the key.
func (s *Store) entryLocked(sh *shard, key []byte) (*Entry, bool) {
	now := s.clock()
	// The lookup with a []byte key does not copy the bytes.
	e, ok := sh.data[string(key)]
	if !ok {
		return nil, false
	}
	if e.Expired(now) {
		// Lazy expiration counts as a modification (like Redis), so WATCH
		// clients get their transaction aborted.
		s.touchKeyLocked(sh, key)
		s.addBytes(sh, -int64(e.cost))
		delete(sh.data, string(key))
		return nil, false
	}
	e.lastAccess.Store(now.UnixNano())
	return e, true
}

// readEntry runs a read-only operation on the entry stored under key while
// the read lock of its shard is held, so several readers can work in parallel.
// It reports ok=false when the key is missing or its TTL has passed.
//
// The closure must not modify the entry. The slices it returns stay valid
// after the lock is released: stored buffers are never modified in place
// (see the Entry invariant), so a reader may keep using them.
func readEntry[T any](s *Store, key []byte, fn func(e *Entry) (T, error)) (T, bool, error) {
	var zero T
	sh := s.shardOf(key)
	// The lookup with a []byte key does not copy the bytes.
	sh.mu.RLock()
	e, ok := sh.data[string(key)]
	if !ok {
		sh.mu.RUnlock()
		return zero, false, nil
	}
	now := s.clock()
	if e.Expired(now) {
		sh.mu.RUnlock()
		// The key is gone as far as the caller is concerned; drop it from
		// the map (and notify its watchers) in a separate write-locked
		// step, since the read lock cannot be upgraded.
		s.expireKey(string(key))
		return zero, false, nil
	}
	e.lastAccess.Store(now.UnixNano())
	v, err := fn(e)
	sh.mu.RUnlock()
	return v, true, err
}

// expireKey removes an expired entry from the map and notifies its watchers,
// like Redis does on a lazy lookup. It takes the write lock of the shard, so
// callers must not hold any lock.
func (s *Store) expireKey(k string) {
	sh := s.shardOfStr(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.data[k]
	if !ok || !e.Expired(s.clock()) {
		return
	}
	s.touchLocked(sh, k)
	s.addBytes(sh, -int64(e.cost))
	delete(sh.data, k)
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
// Callers must hold the write lock of the shard that owns the key.
func (s *Store) newStringEntry(sh *shard, key []byte, value []byte, expiresAt time.Time) {
	s.touchKeyLocked(sh, key)
	k := string(key)
	if value == nil {
		if e, ok := sh.data[k]; ok {
			s.addBytes(sh, -int64(e.cost))
			delete(sh.data, k)
		}
		return
	}
	cost := len(k) + len(value)
	s.addBytes(sh, int64(cost))
	if e, ok := sh.data[k]; ok {
		s.addBytes(sh, -int64(e.cost))
	}
	e := &Entry{Type: TypeString, Str: value, ExpiresAt: expiresAt, cost: cost}
	e.lastAccess.Store(s.clock().UnixNano())
	sh.data[k] = e
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
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.newStringEntry(sh, key, value, time.Time{})
}

// SetStringWithTTL stores a string value under key with a TTL, overwriting any
// existing type.
func (s *Store) SetStringWithTTL(key, value []byte, ttl time.Duration) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.newStringEntry(sh, key, value, s.clock().Add(ttl))
}

// SetStringWithExpiryAt stores a string value under key, expiring at the given
// instant.
func (s *Store) SetStringWithExpiryAt(key, value []byte, expiresAt time.Time) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.newStringEntry(sh, key, value, expiresAt)
}

// Delete removes one or more keys, returning the number of removed keys.
func (s *Store) Delete(keys ...[]byte) int {
	if len(keys) == 0 {
		return 0
	}
	unlock := s.lockWrite(keys...)
	defer unlock()
	now := s.clock()
	removed := 0
	for _, key := range keys {
		sh := s.shardOf(key)
		k := string(key)
		if e, ok := sh.data[k]; ok {
			if !e.Expired(now) {
				removed++
			}
			s.touchLocked(sh, k)
			s.addBytes(sh, -int64(e.cost))
			delete(sh.data, k)
		}
	}
	return removed
}

// Exists reports how many of the given keys exist (and are not expired).
func (s *Store) Exists(keys ...[]byte) int {
	if len(keys) == 0 {
		return 0
	}
	unlock := s.lockRead(keys...)
	defer unlock()
	now := s.clock()
	n := 0
	for _, key := range keys {
		sh := s.shardOf(key)
		if e, ok := sh.data[string(key)]; ok && !e.Expired(now) {
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
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return false
	}
	e.ExpiresAt = expiresAt
	s.touchKeyLocked(sh, key)
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
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return false
	}
	e.ExpiresAt = time.Time{}
	s.touchKeyLocked(sh, key)
	return true
}

// IncrBy atomically increments the numeric string value of key by delta. When
// key is missing it is treated as 0. A non-string value returns ErrWrongType, a
// non-numeric one ErrNotInteger, and overflow ErrOverflow.
func (s *Store) IncrBy(key []byte, delta int64) (int64, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	var current int64
	var expiresAt time.Time
	if e, ok := s.entryLocked(sh, key); ok {
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
	s.newStringEntry(sh, key, strconv.AppendInt(nil, current, 10), expiresAt)
	return current, nil
}

// Append appends suffix to the string value of key, creating it if missing, and
// returns the new length.
//
// The stored buffer is never modified in place: readers may still be copying
// the old value into a socket, so APPEND builds a new buffer and publishes it.
func (s *Store) Append(key, suffix []byte) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s.touchKeyLocked(sh, key)
	if e, ok := s.entryLocked(sh, key); ok {
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		grown := make([]byte, 0, len(e.Str)+len(suffix))
		grown = append(grown, e.Str...)
		grown = append(grown, suffix...)
		s.addCost(sh, e, len(suffix))
		e.Str = grown
		return len(e.Str), nil
	}
	s.newStringEntry(sh, key, append([]byte(nil), suffix...), time.Time{})
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

// Keys returns all live keys. The shards are visited one at a time, so the
// result may interleave with concurrent writes (like KEYS in Redis, which also
// runs over a moving dataset).
func (s *Store) Keys() [][]byte {
	now := s.clock()
	keys := make([][]byte, 0, s.approxLen())
	for i := range s.shards {
		keys = append(keys, s.keysOfShard(i, now)...)
	}
	return keys
}

// KeysOfShard returns the live keys of the shard with the given index. It
// backs the sharded SCAN: one call visits one shard, so the per-call cost of
// building the batch is a fraction of a full Keys.
func (s *Store) KeysOfShard(i int) [][]byte {
	return s.keysOfShard(i, s.clock())
}

// keysOfShard is KeysOfShard with a shared now timestamp.
func (s *Store) keysOfShard(i int, now time.Time) [][]byte {
	sh := &s.shards[i]
	sh.mu.RLock()
	keys := make([][]byte, 0, len(sh.data))
	for k, e := range sh.data {
		if e.Expired(now) {
			continue
		}
		keys = append(keys, []byte(k))
	}
	sh.mu.RUnlock()
	return keys
}

// approxLen returns the total number of entries (including expired ones, which
// have not been collected yet) across the shards; it is a capacity hint.
func (s *Store) approxLen() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

// Flush removes all keys and returns the number removed. The shards are
// cleared one at a time.
func (s *Store) Flush() int {
	n := 0
	now := s.clock()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k := range sh.data {
			s.touchLocked(sh, k)
		}
		n += s.lenLocked(sh, now)
		sh.data = make(map[string]*Entry)
		s.clearBytes(sh)
		sh.mu.Unlock()
	}
	return n
}

// Active expiration works like Redis: every pass checks a sample of the
// keyspace instead of scanning all of it. A full scan under the write locks
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
// from a random shard, at a random position of its map (the iteration order of
// a Go map is randomized), and the walk continues over the following shards
// until the sample is full, so the sample is spread over the keyspace.
func (s *Store) sampleExpired(deadline time.Time) []string {
	now := s.clock()
	var expired []string
	sampled := 0
	start := rand.Intn(shardCount)
	for i := 0; i < shardCount && sampled < expireSample; i++ {
		sh := &s.shards[(start+i)&shardMask]
		sh.mu.RLock()
		for k, e := range sh.data {
			sampled++
			if e.Expired(now) {
				expired = append(expired, k)
			}
			if sampled >= expireSample {
				break
			}
		}
		sh.mu.RUnlock()
	}
	return expired
}

// removeExpired deletes the given keys, one at a time, so that a shard's write
// lock is held for as short a time as possible.
func (s *Store) removeExpired(keys []string) int {
	removed := 0
	for _, k := range keys {
		sh := s.shardOfStr(k)
		sh.mu.Lock()
		e, ok := sh.data[k]
		if ok && e.Expired(s.clock()) {
			s.touchLocked(sh, k)
			s.addBytes(sh, -int64(e.cost))
			delete(sh.data, k)
			removed++
		}
		sh.mu.Unlock()
	}
	return removed
}

// lenLocked counts the live entries of a shard. Callers must hold the write
// lock of the shard (a read lock works too; it is only called under write
// locks today).
func (s *Store) lenLocked(sh *shard, now time.Time) int {
	n := 0
	for _, e := range sh.data {
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
