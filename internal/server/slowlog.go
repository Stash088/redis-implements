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
//
// The configuration is read on the path of every command, so it lives in
// atomics: the slow log is reconfigured while the server runs, and the
// command loop must not wait for the lock of the ring.
type slowlog struct {
	mu      sync.Mutex
	entries []slowlogEntry // newest last
	maxLen  atomic.Int64
	thresh  atomic.Int64
	nextID  atomic.Uint64
}

func newSlowlog(threshold int64, maxLen int) *slowlog {
	if maxLen < 0 {
		maxLen = 0
	}
	sl := &slowlog{}
	sl.maxLen.Store(int64(maxLen))
	sl.thresh.Store(threshold)
	return sl
}

// setConfig updates the threshold and capacity, truncating entries to the new
// capacity when needed.
func (sl *slowlog) setConfig(threshold int64, maxLen int) {
	if maxLen < 0 {
		maxLen = 0
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.thresh.Store(threshold)
	sl.maxLen.Store(int64(maxLen))
	if len(sl.entries) > maxLen {
		sl.entries = sl.entries[len(sl.entries)-maxLen:]
	}
}

// config returns the current threshold and capacity.
func (sl *slowlog) config() (int64, int) {
	return sl.thresh.Load(), int(sl.maxLen.Load())
}

// maybeAdd records the command when it ran longer than the threshold.
//
// The client is passed instead of its address and name: those are read only
// for a command that is actually recorded, so the hot path does not take the
// locks that guard them.
func (sl *slowlog) maybeAdd(args [][]byte, c *client, dur time.Duration) {
	if sl == nil {
		return
	}
	maxLen := sl.maxLen.Load()
	thresh := sl.thresh.Load()
	if maxLen == 0 || thresh < 0 {
		return
	}
	if int64(dur.Microseconds()) < thresh {
		return
	}
	addr, name := c.addr, c.getName()
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
	if int64(len(sl.entries)) >= sl.maxLen.Load() {
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
