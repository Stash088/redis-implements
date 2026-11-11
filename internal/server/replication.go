package server

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
)

// replIDLen is the length of a replication ID: 40 hex characters, the same
// as the run ID of Redis.
const replIDLen = 40

// DefaultReplBacklogSize is the capacity of the replication backlog: the
// number of the stream bytes a reconnecting replica may catch up with.
const DefaultReplBacklogSize = 1 << 20 // 1 MB, like Redis

// newReplID returns a random replication ID (40 hex characters).
func newReplID() string {
	b := make([]byte, replIDLen/2)
	rand.Read(b) //nolint:errcheck — crypto/rand does not fail in practice
	return hex.EncodeToString(b)
}

// replBacklog keeps the last bytes of the replication stream, so a replica
// that reconnects may continue from the offset it reached instead of
// repeating the full synchronization. It is a sliding window: when it fills
// up, the oldest bytes drop off the front.
type replBacklog struct {
	buf  []byte
	off  int64 // the stream offset of buf[0]
	size int   // the capacity
}

func newReplBacklog(size int) *replBacklog {
	if size < 1024 {
		size = 1024
	}
	return &replBacklog{size: size}
}

// append adds p to the backlog; base is the stream offset p starts at (the
// offset before the append).
func (b *replBacklog) append(p []byte, base int64) {
	if len(p) >= b.size {
		// The command alone fills the window: only its tail is kept.
		b.buf = append(b.buf[:0], p[len(p)-b.size:]...)
		b.off = base + int64(len(p)-b.size)
		return
	}
	drop := len(b.buf) + len(p) - b.size
	if drop > 0 {
		// Slide in chunks of an eighth of the window, so that a steady
		// stream of small commands does not shift the buffer on every
		// append.
		if room := b.size / 8; drop < room {
			drop = room
		}
		if drop > len(b.buf) {
			drop = len(b.buf)
		}
		n := copy(b.buf, b.buf[drop:])
		b.buf = append(b.buf[:n], p...)
		b.off += int64(drop)
		return
	}
	b.buf = append(b.buf, p...)
}

// since returns a copy of the stream bytes from the given offset. ok is
// false when the offset is before the window (the bytes are gone) or beyond
// its end.
func (b *replBacklog) since(off int64) ([]byte, bool) {
	idx := off - b.off
	if idx < 0 || idx > int64(len(b.buf)) {
		return nil, false
	}
	out := make([]byte, int64(len(b.buf))-idx)
	copy(out, b.buf[idx:])
	return out, true
}

func (b *replBacklog) firstOffset() int64 { return b.off }
func (b *replBacklog) histlen() int64     { return int64(len(b.buf)) }

// replica is a replica connected to this server (the master side of the
// link). After the synchronization the replica receives every command the
// server propagates.
type replica struct {
	c    *client
	port int // the port of REPLCONF listening-port (for ROLE and INFO)
	// ackOff is the offset of the last REPLCONF ACK of the replica: the
	// position of the stream it processed.
	ackOff atomic.Int64
	// ackAt is the unix time of the last REPLCONF ACK (the lag of INFO).
	ackAt atomic.Int64
}

// linkState is the state of the link between this server (as a replica) and
// its master.
type linkState int32

const (
	linkConnect linkState = iota // the handshake is in progress
	linkSync                     // the RDB is on its way
	linkUp                       // the command stream is live
	linkDown                     // the master is lost, reconnecting
)

func (s linkState) String() string {
	switch s {
	case linkConnect:
		return "connect"
	case linkSync:
		return "sync"
	case linkUp:
		return "up"
	default:
		return "down"
	}
}

// roleState maps the link state to the ROLE reply of a replica.
func (s linkState) roleState() string {
	switch s {
	case linkSync:
		return "sync"
	case linkUp:
		return "connected"
	default:
		return "connect"
	}
}

// status maps the link state to the master_link_status field of INFO.
func (s linkState) status() string {
	if s == linkUp {
		return "up"
	}
	return "down"
}

// replState is the replication state of the server. When the server is a
// master it owns the replication stream: the offset, the backlog and the
// connected replicas. When the server is a replica (REPLICAOF), link
// describes the master and the client goroutine that streams from it.
type replState struct {
	// id is the replication ID of this history. A fresh master gets a
	// random one; a replica inherits the ID of its master with the full
	// synchronization and keeps it when it is promoted back to a master
	// (id2 then remembers the previous history up to id2Off).
	id  string
	id2 string
	// id2Off is the offset up to which id2 is a valid history.
	id2Off int64

	// offset counts the bytes of the replication stream: as a master, the
	// bytes the server fed; as a replica, the bytes it consumed. The
	// replicas count the bytes they process and reply with the number in
	// REPLCONF ACK, so the offset is what WAIT measures and what a partial
	// resync continues from.
	offset atomic.Int64

	mu      sync.Mutex
	db      int // the database the stream is in; -1 re-emits SELECT
	backlog *replBacklog
	reps    map[*client]*replica

	// link is set when the server is a replica. Its state and the time of
	// the last word from the master are read by ROLE and INFO.
	link   *replicaClient
	state  atomic.Int32 // linkState of the link
	lastIO atomic.Int64 // unix seconds of the last reply of the master
}

func newReplState(backlogSize int) *replState {
	return &replState{
		id:      newReplID(),
		id2:     string(make([]byte, replIDLen)), // 40 zeros, like Redis
		db:      -1,
		backlog: newReplBacklog(backlogSize),
		reps:    make(map[*client]*replica),
	}
}

// isReplica reports whether the server replicates from a master.
func (s *Server) isReplica() bool { return s.repl.link != nil }

// roleName returns the role the server reports about itself ("master" or
// "slave", the spelling Redis uses).
func (s *Server) roleName() string {
	if s.isReplica() {
		return "slave"
	}
	return "master"
}

// setReplicaOf makes the server a replica of the given master: the link
// client starts and syncs in the background. The dataset is replaced when
// the synchronization completes, like in Redis.
func (s *Server) setReplicaOf(host string, port int, auth string) {
	s.repl.mu.Lock()
	old := s.repl.link
	s.repl.mu.Unlock()
	if old != nil {
		old.stopAndWait()
	}
	rc := newReplicaClient(s, host, port, auth)
	s.repl.mu.Lock()
	s.repl.link = rc
	s.repl.mu.Unlock()
	rc.start()
}

// SetReplicaOf makes the server a replica of the given master. It is the
// startup form of the REPLICAOF command: call it before Serve.
func (s *Server) SetReplicaOf(host string, port int, auth string) {
	s.setReplicaOf(host, port, auth)
}

// clearReplicaOf promotes the server back to a master: the link is dropped
// and the replication history shifts (id2 remembers the inherited history,
// so the replicas of the former master may still partially resync).
func (s *Server) clearReplicaOf() {
	s.repl.mu.Lock()
	old := s.repl.link
	s.repl.link = nil
	s.repl.mu.Unlock()
	if old != nil {
		old.stopAndWait()
	}
	r := s.repl
	r.mu.Lock()
	r.id2 = r.id
	r.id2Off = r.offset.Load() + 1
	r.mu.Unlock()
	r.id = newReplID()
}

// appendStream feeds one propagated command into the replication stream: the
// re-emitted SELECT, the backlog and the connected replicas. db is the
// database the command ran in; -1 marks protocol traffic (PING, REPLCONF
// GETACK) that must not trigger a SELECT.
//
// The offset grows by exactly the number of the bytes the stream emits. The
// replicas count the same bytes on their side, so the offsets stay in sync.
//
// The stream mutex is held while the commands are appended: a full
// synchronization cuts its offset under the same mutex, so a replica never
// misses a command that was appended before its cut.
func (s *Server) appendStream(args [][]byte, db int) {
	r := s.repl
	if r.link != nil {
		// The server is a replica: it proxies nothing (writes are rejected
		// anyway) and must not grow its own stream accounting.
		return
	}
	r.mu.Lock()
	var sel []byte
	if db >= 0 && r.db != db {
		sel = persist.EncodeCommand([][]byte{[]byte("select"), []byte(strconv.Itoa(db))})
		r.emitLocked(sel)
		r.db = db
	}
	enc := persist.EncodeCommand(args)
	r.emitLocked(enc)
	reps := make([]*replica, 0, len(r.reps))
	for _, rep := range r.reps {
		reps = append(reps, rep)
	}
	r.mu.Unlock()
	if len(reps) == 0 {
		return
	}
	if sel != nil {
		frame := commandFrame([][]byte{[]byte("select"), []byte(strconv.Itoa(db))})
		for _, rep := range reps {
			deliver(rep.c, frame)
		}
	}
	frame := commandFrame(args)
	for _, rep := range reps {
		deliver(rep.c, frame)
	}
}

// emitLocked appends the encoded command to the backlog and counts its bytes
// into the offset. The caller must hold r.mu.
func (r *replState) emitLocked(enc []byte) {
	if r.backlog != nil {
		r.backlog.append(enc, r.offset.Load())
	}
	r.offset.Add(int64(len(enc)))
}

// unregisterReplica drops the client from the replicas of the server (the
// connection is gone).
func (s *Server) unregisterReplica(c *client) {
	s.repl.mu.Lock()
	delete(s.repl.reps, c)
	s.repl.mu.Unlock()
}

// tryPartial reports whether the server can continue the stream of a replica
// from the given offset: the replication ID must match this history (or the
// previous one, up to its offset) and the bytes must still be in the
// backlog. It returns the bytes to replay.
func (s *Server) tryPartial(id string, off int64) ([]byte, bool) {
	r := s.repl
	r.mu.Lock()
	defer r.mu.Unlock()
	match := id == r.id || (r.id2Off > 0 && id == r.id2 && off <= r.id2Off)
	if !match {
		return nil, false
	}
	if off > r.offset.Load() {
		return nil, false
	}
	return r.backlog.since(off)
}

// countAcks returns the replicas that processed at least off bytes of the
// stream.
func (s *Server) countAcks(off int64) int {
	r := s.repl
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rep := range r.reps {
		if rep.ackOff.Load() >= off {
			n++
		}
	}
	return n
}

// replicaAddrs returns the connected replicas for ROLE and INFO: the
// address, the announced port, the acknowledged offset and the lag in
// seconds.
func (s *Server) replicaAddrs() []replicaAddr {
	r := s.repl
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().Unix()
	out := make([]replicaAddr, 0, len(r.reps))
	for _, rep := range r.reps {
		host, _, err := splitHostPort(rep.c.addr)
		if err != nil {
			host = rep.c.addr
		}
		lag := now - rep.ackAt.Load()
		if lag < 0 {
			lag = 0
		}
		out = append(out, replicaAddr{
			host:   host,
			port:   rep.port,
			offset: rep.ackOff.Load(),
			lag:    lag,
		})
	}
	return out
}

// replicaAddr is one entry of the ROLE reply and of the INFO replication
// section.
type replicaAddr struct {
	host   string
	port   int
	offset int64
	lag    int64
}

// masterInfo returns the master this server replicates from: the address,
// the state of the link and the time of the last word from it. ok is false
// when the server is a master.
func (s *Server) masterInfo() (host string, port int, st linkState, lastIO int64, ok bool) {
	r := s.repl
	r.mu.Lock()
	link := r.link
	r.mu.Unlock()
	if link == nil {
		return "", 0, 0, 0, false
	}
	return link.host, link.port, linkState(r.state.Load()), r.lastIO.Load(), true
}

// splitHostPort splits "host:port". The whole address comes back as the
// host when there is no port.
func splitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

// hasStream reports whether the stream is alive: a replica is attached or
// the backlog holds bytes worth keeping fresh.
func (s *Server) hasStream() bool {
	r := s.repl
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reps) > 0 || r.backlog.histlen() > 0
}

// replPingLoop feeds a PING into the stream once a second when it is alive:
// an idle stream would otherwise let the replicas time their master out, and
// the backlog would not advance. It is started with Serve and stopped with
// the server.
func (s *Server) replPingLoop() {
	stop := s.replStop
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if s.isReplica() {
				// The offset of a replica counts the bytes of its master:
				// its own stream accounting must stay still.
				continue
			}
			if s.hasStream() {
				s.appendStream([][]byte{[]byte("ping")}, -1)
			}
		}
	}
}

// commandFrame builds the RESP frame of a command: an array of bulk strings.
// The payloads are shared with the arguments, which are fresh buffers of the
// connection: the frame may outlive the command.
func commandFrame(args [][]byte) resp.Value {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString(a)
	}
	return resp.NewArray(elems)
}
