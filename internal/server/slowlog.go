package server

import (
	"sync"
	"sync/atomic"
	"time"
)

// slowlogEntry is one slow command record.
type slowlogEntry struct {
	id   uint64
	at   time.Time
	dur  time.Duration
	args [][]byte
	addr string
	name string
}

// slowlog is a bounded ring of slow command entries. threshold is in
// microseconds; negative disables recording, zero length disables retention.
type slowlog struct {
	mu      sync.Mutex
	entries []slowlogEntry // newest last
	maxLen  int
	thresh  int64
	nextID  atomic.Uint64
}

func newSlowlog(threshold int64, maxLen int) *slowlog {
	if maxLen < 0 {
		maxLen = 0
	}
	return &slowlog{maxLen: maxLen, thresh: threshold}
}

// setConfig updates the threshold and capacity, truncating entries to the new
// capacity when needed.
func (sl *slowlog) setConfig(threshold int64, maxLen int) {
	if maxLen < 0 {
		maxLen = 0
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.thresh = threshold
	sl.maxLen = maxLen
	if len(sl.entries) > maxLen {
		sl.entries = sl.entries[len(sl.entries)-maxLen:]
	}
}

// config returns the current threshold and capacity.
func (sl *slowlog) config() (int64, int) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.thresh, sl.maxLen
}

// maybeAdd records the command when it ran longer than the threshold.
func (sl *slowlog) maybeAdd(args [][]byte, addr, name string, dur time.Duration) {
	if sl == nil || sl.maxLen == 0 || sl.thresh < 0 {
		return
	}
	if int64(dur.Microseconds()) < sl.thresh {
		return
	}
	// The connection reuses its argument buffer, so the entry keeps a copy.
	kept := make([][]byte, len(args))
	for i, a := range args {
		kept[i] = append([]byte(nil), a...)
	}
	e := slowlogEntry{
		id:   sl.nextID.Add(1),
		at:   time.Now(),
		dur:  dur,
		args: kept,
		addr: addr,
		name: name,
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(sl.entries) >= sl.maxLen {
		copy(sl.entries, sl.entries[1:])
		sl.entries[len(sl.entries)-1] = e
	} else {
		sl.entries = append(sl.entries, e)
	}
}

// get returns up to n newest entries (n <= 0 means all).
func (sl *slowlog) get(n int) []slowlogEntry {
	if sl == nil {
		return nil
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	entries := sl.entries
	if n > 0 && n < len(entries) {
		entries = entries[len(entries)-n:]
	}
	out := make([]slowlogEntry, len(entries))
	for i, e := range entries {
		out[len(entries)-1-i] = e // newest first
	}
	return out
}

func (sl *slowlog) length() int {
	if sl == nil {
		return 0
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return len(sl.entries)
}

func (sl *slowlog) reset() {
	if sl == nil {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.entries = nil
}
