package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// errUnblocked is the answer of a client that was unblocked with
// CLIENT UNBLOCK.
var errUnblocked = errors.New("ERR UNBLOCKED client unblocked via CLIENT UNBLOCK")

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
	// popStream waits for new entries of a stream (XREAD, XREADGROUP).
	popStream
)

// waiter is a client blocked on a set of keys.
type waiter struct {
	c    *client
	keys [][]byte
	kind popKind
	// dest is the destination key of a blocking move (BLMOVE).
	dest []byte
	// ready is closed when one of the keys may have become available. It
	// is used by the goroutine model: there a goroutine waits on it. The
	// event loop runs the command again itself.
	ready chan struct{}
	done  chan struct{}
	once  sync.Once
	// loop is set when the client is served by the event loop.
	loop *loopConn
	// errMu guards err.
	errMu sync.Mutex
	// err is the error the client is answered with when it is unblocked
	// with CLIENT UNBLOCK.
	err error
}

// wake signals the waiter that one of its keys may be available.
func (w *waiter) wake() {
	if w.loop != nil {
		// The event loop retries the command itself: the connection is
		// marked as ready to run it again.
		w.loop.wakeup()
		return
	}
	w.once.Do(func() { close(w.ready) })
}

// unblock releases the waiter with an error: the client is answered with it
// instead of the result of the command.
func (w *waiter) unblock(err error) {
	w.errMu.Lock()
	w.err = err
	w.errMu.Unlock()
	w.wake()
}

// takeError returns the error of the unblocking, if there was one.
func (w *waiter) takeError() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	err := w.err
	w.err = nil
	return err
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
	// n is the number of registered waiters. Every write command checks it,
	// so it has to be readable without the lock: when nobody waits, the
	// write path must not touch the mutex at all.
	n atomic.Int64
}

func newWaiters() *waiters {
	return &waiters{byKey: make(map[string][]*waiter)}
}

// add registers a waiter for all its keys.
func (w *waiters) add(wt *waiter) {
	w.mu.Lock()
	for _, k := range wt.keys {
		key := string(k)
		w.byKey[key] = append(w.byKey[key], wt)
	}
	w.mu.Unlock()
	w.n.Add(1)
}

// remove unregisters a waiter from all its keys.
func (w *waiters) remove(wt *waiter) {
	w.mu.Lock()
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
	w.mu.Unlock()
	w.n.Add(-1)
}

// signal wakes the waiters of a key: a command may have made it available.
// The key is given as bytes: the string is only built when there is
// somebody to wake up.
func (w *waiters) signal(key []byte) {
	if w.n.Load() == 0 {
		return
	}
	w.mu.Lock()
	list := w.byKey[string(key)]
	if len(list) == 0 {
		w.mu.Unlock()
		return
	}
	// The waiters are removed from the registry: the woken client retries the
	// command and registers again if it still has to wait.
	delete(w.byKey, string(key))
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

// unblock releases the client that waits: it is answered with an error
// instead of the result of the command. The client is found by its id or by
// its address.
func (w *waiters) unblock(id uint64, addr string) bool {
	w.mu.Lock()
	var found *waiter
	for _, list := range w.byKey {
		for _, wt := range list {
			if (id != 0 && wt.c.id == id) || (addr != "" && wt.c.addr == addr) {
				found = wt
				break
			}
		}
		if found != nil {
			break
		}
	}
	w.mu.Unlock()
	if found == nil {
		return false
	}
	found.unblock(errUnblocked)
	return true
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
// It returns false when the wait timed out, and the error when the client was
// unblocked with CLIENT UNBLOCK.
func (s *Server) waitForKeys(c *client, req *waitRequest) (bool, error) {
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
		return true, wt.takeError()
	case <-timeout:
		return false, nil
	case <-c.done:
		return false, nil
	}
}
