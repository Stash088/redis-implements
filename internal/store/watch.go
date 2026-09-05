package store

// Watcher is notified when a key it watches is modified or removed. It is
// used by WATCH/EXEC (check-and-set) transactions.
type Watcher interface {
	MarkDirty()
}

// AddWatch registers w as a watcher of key.
func (s *Store) AddWatch(key []byte, w Watcher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := string(key)
	ws, ok := s.watches[k]
	if !ok {
		ws = make(map[Watcher]struct{})
		s.watches[k] = ws
	}
	ws[w] = struct{}{}
}

// RemoveWatch unregisters w from key.
func (s *Store) RemoveWatch(key []byte, w Watcher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := string(key)
	ws, ok := s.watches[k]
	if !ok {
		return
	}
	delete(ws, w)
	if len(ws) == 0 {
		delete(s.watches, k)
	}
}

// IsExpired reports whether key is present in the store but its TTL has
// already passed. Unlike the read accessors it does not perform lazy
// deletion, so it is safe for pre-checks (WATCH marks such keys dirty).
func (s *Store) IsExpired(key []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[string(key)]
	return ok && e.Expired(s.clock())
}

// touchLocked notifies every watcher of key that it changed. Callers must
// hold s.mu for writing. It is a no-op when nobody watches anything, so the
// common path pays only a map-length check.
func (s *Store) touchLocked(key string) {
	if len(s.watches) == 0 {
		return
	}
	if ws, ok := s.watches[key]; ok {
		for w := range ws {
			w.MarkDirty()
		}
	}
}
