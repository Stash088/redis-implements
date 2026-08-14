package store

import "time"

// ForEach calls fn for every live key/value pair while holding a read lock.
// The store must not be mutated from within fn.
func (s *Store) ForEach(fn func(key string, e *Entry) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	for k, e := range s.data {
		if e.Expired(now) {
			continue
		}
		if err := fn(k, e); err != nil {
			return err
		}
	}
	return nil
}

// RestoreEntry inserts an entry into the store, replacing any existing value.
// It is used by persistence loaders to rebuild the dataset.
func (s *Store) RestoreEntry(key string, e *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = e
}

// Now returns the store's current clock time.
func (s *Store) Now() time.Time { return s.clock() }

// NewList returns an empty List.
func NewList() *List { return &List{} }

// NewZSet returns an empty ZSet with a dict and skiplist ready for use.
func NewZSet() *ZSet { return &ZSet{dict: make(map[string]float64), sl: newSkiplist()} }
