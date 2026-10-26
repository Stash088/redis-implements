package store

import "time"

// ForEach calls fn for every live key/value pair. The shards are visited one
// at a time under their read lock, so fn may observe a keyspace that moves
// under it (like KEYS in Redis); it must not mutate the store.
func (s *Store) ForEach(fn func(key string, e *Entry) error) error {
	now := s.clock()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.Expired(now) {
				continue
			}
			if err := fn(k, e); err != nil {
				sh.mu.RUnlock()
				return err
			}
		}
		sh.mu.RUnlock()
	}
	return nil
}

// RestoreEntry inserts an entry into the store, replacing any existing value.
// It is used by persistence loaders to rebuild the dataset.
func (s *Store) RestoreEntry(key string, e *Entry) {
	sh := s.shardOfStr(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cost := s.entryCostLocked(key, e)
	if old, ok := sh.data[key]; ok {
		s.addBytes(sh, -int64(old.cost))
	}
	e.cost = cost
	e.lastAccess.Store(s.clock().UnixNano())
	s.addBytes(sh, int64(cost))
	sh.data[key] = e
}

// Now returns the store's current clock time.
func (s *Store) Now() time.Time { return s.clock() }

// NewList returns an empty List.
func NewList() *List { return &List{} }

// NewZSet returns an empty ZSet with a dict and skiplist ready for use.
func NewZSet() *ZSet { return &ZSet{dict: make(map[string]float64), sl: newSkiplist()} }
