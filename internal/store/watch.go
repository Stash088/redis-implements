package store

// Watcher is notified when a key it watches is modified or removed. It is
// used by WATCH/EXEC (check-and-set) transactions.
type Watcher interface {
	MarkDirty()
}

// AddWatch registers w as a watcher of key.
func (s *Store) AddWatch(key []byte, w Watcher) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	ws, ok := sh.watches[k]
	if !ok {
		ws = make(map[Watcher]struct{})
		sh.watches[k] = ws
	}
	ws[w] = struct{}{}
}

// RemoveWatch unregisters w from key.
func (s *Store) RemoveWatch(key []byte, w Watcher) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	k := string(key)
	ws, ok := sh.watches[k]
	if !ok {
		return
	}
	delete(ws, w)
	if len(ws) == 0 {
		delete(sh.watches, k)
	}
}

// IsExpired reports whether key is present in the store but its TTL has
// already passed. Unlike the read accessors it does not perform lazy
// deletion, so it is safe for pre-checks (WATCH marks such keys dirty).
func (s *Store) IsExpired(key []byte) bool {
	sh := s.shardOf(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := sh.data[string(key)]
	return ok && e.Expired(s.clock())
}

// touchLocked notifies every watcher of key that it changed. Callers must hold
// the write lock of the shard that owns the key. It is a no-op when nobody
// watches anything, so the common path pays only a map-length check.
func (s *Store) touchLocked(sh *shard, key string) {
	if len(sh.watches) == 0 {
		return
	}
	if ws, ok := sh.watches[key]; ok {
		for w := range ws {
			w.MarkDirty()
		}
	}
}

// touchKeyLocked is touchLocked for a key that is still a byte slice: the
// string is only built when somebody watches the key, so the hot path does
// not allocate one.
//
// Callers must hold the write lock of the shard that owns the key.
func (s *Store) touchKeyLocked(sh *shard, key []byte) {
	if len(sh.watches) == 0 {
		return
	}
	if ws, ok := sh.watches[string(key)]; ok {
		for w := range ws {
			w.MarkDirty()
		}
	}
}
