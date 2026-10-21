package server

import (
	"sync"
	"time"
)

// popKind tells the blocking commands which side of the collection to pop
// from.
type popKind uint8

const (
	// popLeft pops from the head of a list (BLPOP).
	popLeft popKind = iota
	// popRight pops from the tail of a list (BRPOP).
	popRight
	// popZMin pops the member with the lowest score (BZPOPMIN).
	popZMin
	// popZMax pops the member with the highest score (BZPOPMAX).
	popZMax
)

// waiter is a client blocked on a set of keys.
type waiter struct {
	c    *client
	keys [][]byte
	kind popKind
	// dest is the destination key of a blocking move (BLMOVE).
	dest []byte
	// ready is closed when one of the keys may have become available.
	ready chan struct{}
	// done is closed when the waiter gives up (timeout or disconnect).
	done chan struct{}
	once sync.Once
}

// wake signals the waiter that one of its keys may be available.
func (w *waiter) wake() {
	w.once.Do(func() { close(w.ready) })
}

// giveUp releases the waiter.
func (w *waiter) giveUp() {
	w.once.Do(func() { close(w.ready) })
}

// waiters holds the clients blocked on the keys of the server.
type waiters struct {
	mu sync.Mutex
	// byKey maps a key to the waiters interested in it.
	byKey map[string][]*waiter
}

func newWaiters() *waiters {
	return &waiters{byKey: make(map[string][]*waiter)}
}

// add registers a waiter for all its keys.
func (w *waiters) add(wt *waiter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, k := range wt.keys {
		key := string(k)
		w.byKey[key] = append(w.byKey[key], wt)
	}
}

// remove unregisters a waiter from all its keys.
func (w *waiters) remove(wt *waiter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, k := range wt.keys {
		key := string(k)
		list := w.byKey[key]
		for i, other := range list {
			if other == wt {
				w.byKey[key] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(w.byKey[key]) == 0 {
			delete(w.byKey, key)
		}
	}
}

// signal wakes the waiters of a key: a command may have made it available.
func (w *waiters) signal(key string) {
	w.mu.Lock()
	list := w.byKey[key]
	if len(list) == 0 {
		w.mu.Unlock()
		return
	}
	// The waiters are removed from the registry: the woken client retries the
	// command and registers again if it still has to wait.
	delete(w.byKey, key)
	for _, other := range list {
		for _, k := range other.keys {
			k2 := string(k)
			l2 := w.byKey[k2]
			for i, o := range l2 {
				if o == other {
					w.byKey[k2] = append(l2[:i], l2[i+1:]...)
					break
				}
			}
			if len(w.byKey[k2]) == 0 {
				delete(w.byKey, k2)
			}
		}
	}
	w.mu.Unlock()
	for _, other := range list {
		other.wake()
	}
}

// count returns the number of blocked waiters.
func (w *waiters) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, list := range w.byKey {
		n += len(list)
	}
	return n
}

// waitRequest describes a command that has to wait for a key to become
// available.
type waitRequest struct {
	keys    [][]byte
	kind    popKind
	timeout time.Duration
	// dest is the destination key of a blocking move.
	dest []byte
}

// waitForKeys registers the client as a waiter and waits until one of the keys
// may have become available, the timeout expires or the connection is gone.
// It returns false when the wait timed out.
func (s *Server) waitForKeys(c *client, req *waitRequest) bool {
	wt := &waiter{
		c:     c,
		keys:  req.keys,
		kind:  req.kind,
		dest:  req.dest,
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.waiters.add(wt)
	defer s.waiters.remove(wt)

	var timer *time.Timer
	var timeout <-chan time.Time
	if req.timeout > 0 {
		timer = time.NewTimer(req.timeout)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-wt.ready:
		return true
	case <-timeout:
		return false
	case <-c.done:
		return false
	}
}
