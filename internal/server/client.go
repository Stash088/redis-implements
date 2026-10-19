package server

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// clientSeq generates monotonically increasing client IDs (CLIENT ID).
var clientSeq atomic.Uint64

// pubsubQueueSize bounds pending Pub/Sub messages per client. When it fills
// up, the client is disconnected (like Redis dropping a slow subscriber).
const pubsubQueueSize = 256

// client carries per-connection state: write serialization between the
// command loop and the Pub/Sub pump, subscriptions, the MULTI/EXEC queue
// and WATCH bookkeeping.
type client struct {
	id   uint64
	conn net.Conn
	addr string

	// writeMu serializes access to the connection's resp.Writer between the
	// command loop and the Pub/Sub pump goroutine, which share the bufio.
	writeMu sync.Mutex

	// Pub/Sub state.
	channels map[string]struct{}
	patterns map[string]struct{}
	msgCh    chan resp.Value
	done     chan struct{} // closed when the connection is gone; stops the pump
	pumpOnce sync.Once

	// Database selected with SELECT.
	db int

	// authenticated is set once the client passed the AUTH command.
	authenticated bool

	// Protocol version negotiated with HELLO (2 or 3). It is read by other
	// goroutines (the Pub/Sub hub), so it is accessed atomically.
	proto atomic.Int32

	// Transaction state.
	inMulti  bool
	queue    [][][]byte
	abort    bool        // a queueing error occurred; EXEC must abort
	dirtyCAS atomic.Bool // a watched key was modified since WATCH

	// statMu guards fields read cross-goroutine by CLIENT LIST:
	// watched, inMulti, lastCmd. Mutations happen on the connection's own
	// goroutine; the lock is uncontended in practice.
	statMu  sync.Mutex
	watched map[string]struct{}
	nameMu  sync.Mutex
	name    string
	created time.Time
	lastCmd string
}

func newClient(conn net.Conn) *client {
	c := &client{
		id:       clientSeq.Add(1),
		conn:     conn,
		addr:     conn.RemoteAddr().String(),
		channels: make(map[string]struct{}),
		patterns: make(map[string]struct{}),
		msgCh:    make(chan resp.Value, pubsubQueueSize),
		done:     make(chan struct{}),
		watched:  make(map[string]struct{}),
		created:  time.Now(),
	}
	c.proto.Store(2)
	return c
}

// Proto returns the protocol version negotiated with HELLO (2 or 3).
func (c *client) Proto() int { return int(c.proto.Load()) }

// startPump launches the push-message goroutine once. It drains msgCh and
// writes each message to the connection under writeMu; it exits when the
// connection is closed (done) or a write fails.
func (c *client) startPump(w *resp.Writer) {
	c.pumpOnce.Do(func() {
		go func() {
			for {
				select {
				case <-c.done:
					return
				case v := <-c.msgCh:
					c.writeMu.Lock()
					err := w.WriteValue(v)
					if err == nil {
						err = w.Flush()
					}
					c.writeMu.Unlock()
					if err != nil {
						c.conn.Close()
						return
					}
				}
			}
		}()
	})
}

// subscribed reports whether the client has active channel or pattern
// subscriptions.
func (c *client) subscribed() bool {
	return len(c.channels) > 0 || len(c.patterns) > 0
}

// MarkDirty implements store.Watcher: a watched key was modified, so a
// pending EXEC must abort with a null reply.
func (c *client) MarkDirty() { c.dirtyCAS.Store(true) }

// unwatchAll clears every WATCH this client holds and resets the CAS flag.
func (c *client) unwatchAll(st *store.Store) {
	c.statMu.Lock()
	keys := c.watched
	c.watched = make(map[string]struct{})
	c.dirtyCAS.Store(false)
	c.statMu.Unlock()
	for k := range keys {
		st.RemoveWatch([]byte(k), c)
	}
}

// setName updates the client name (CLIENT SETNAME).
func (c *client) setName(name string) {
	c.nameMu.Lock()
	c.name = name
	c.nameMu.Unlock()
}

// getName returns the client name (CLIENT GETNAME / LIST).
func (c *client) getName() string {
	c.nameMu.Lock()
	defer c.nameMu.Unlock()
	return c.name
}

// setLastCmd records the last executed command name (CLIENT LIST).
func (c *client) setLastCmd(name string) {
	c.statMu.Lock()
	c.lastCmd = name
	c.statMu.Unlock()
}

// addWatchedKey registers a watched key (cmdWatch).
func (c *client) addWatchedKey(key string) {
	c.statMu.Lock()
	c.watched[key] = struct{}{}
	c.statMu.Unlock()
}

// txnState snapshots fields shown by CLIENT LIST.
func (c *client) txnState() (inMulti int, watchCount int) {
	c.statMu.Lock()
	defer c.statMu.Unlock()
	if c.inMulti {
		inMulti = 1
	}
	return inMulti, len(c.watched)
}

// beginMulti switches the client into transaction-queueing mode.
func (c *client) beginMulti() {
	c.statMu.Lock()
	c.inMulti = true
	c.queue = nil
	c.abort = false
	c.statMu.Unlock()
}

// endMulti leaves MULTI mode, returning the queued commands and the abort
// flag; it also drops every WATCH (Redis semantics for EXEC).
func (c *client) endMulti(st *store.Store) (queue [][][]byte, abort, dirty bool) {
	c.statMu.Lock()
	queue = c.queue
	abort = c.abort
	dirty = c.dirtyCAS.Load()
	c.inMulti = false
	c.queue = nil
	c.abort = false
	keys := c.watched
	c.watched = make(map[string]struct{})
	c.dirtyCAS.Store(false)
	c.statMu.Unlock()
	for k := range keys {
		st.RemoveWatch([]byte(k), c)
	}
	return queue, abort, dirty
}
