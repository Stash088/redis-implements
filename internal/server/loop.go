package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amir/redis-implements/internal/netloop"
	"github.com/amir/redis-implements/internal/resp"
)

// The event loop serves the connections without a goroutine each: the loop
// waits for the readiness of the sockets, reads the requests, runs the
// commands and writes the replies.
//
// The commands are executed by the same code as the goroutine model (the
// dispatch of the server), so the two models behave the same. The difference
// is the I/O: the sockets are watched in the level-triggered mode, so a
// request costs neither the extra read syscall nor the goroutine wake-up that
// the goroutine model pays.
//
// The commands that would hold the loop (the blocking ones and the
// subscriptions) do not hold it:
//
//   - a client that waits for a key goes into the waiting list, and the loop
//     goes on with the other connections; the command is run again when the
//     key may have become available or when the timeout expires;
//   - the messages of the subscribers are queued and written with the next
//     replies of the connection.

// loopState is the state of one event loop.
type loopState struct {
	srv    *Server
	poller netloop.Poller
	// ln is the descriptor of the listener.
	ln int
	// conns are the connections the loop serves, by descriptor.
	conns map[int]*loopConn
	// runnable holds the connections that must be served again: the blocked
	// ones that were woken up.
	runnable []*loopConn
	// push holds the connections with the Pub/Sub messages waiting to be
	// written.
	push []*loopConn
	// toWrite holds the connections whose replies are waiting to be sent.
	toWrite []*loopConn
	// spare is the buffer of the previous round of toWrite: the lists
	// alternate, so the loop does not allocate on every round.
	spare []*loopConn
	// toClose holds the connections that must be closed.
	toClose []*loopConn
	// blocked holds the connections that wait for a key.
	blocked map[*loopConn]struct{}
	// mu guards the queues that may be filled from another goroutine: the
	// messages of the subscribers and the woken up clients.
	mu sync.Mutex
	// stopped is set when the server is closed, finished when the loop has
	// left, destroyed when the loop released its poller and its listener.
	stopped      bool
	finished     bool
	destroyed    bool
	pollerClosed bool
}

// loopConn is the state of a connection served by the event loop.
type loopConn struct {
	fd   int
	loop *loopState
	// in is the read buffer, pos the offset of the unread bytes.
	in  []byte
	pos int
	// args is reused by the commands.
	args [][]byte
	// writer is the reply writer of the connection: it writes into the
	// sink, whose buffer holds the replies until they are written to the
	// socket.
	writer *resp.Writer
	sink   *loopSink
	// client and ctx are the state of the connection: they are reused by
	// the commands, like in the goroutine model.
	client *client
	ctx    Ctx
	// pending is set when the socket did not accept all the replies: the
	// connection is watched for writing until the replies are out.
	pending bool
	// reading is set when the poller watches the connection for reading.
	reading bool
	// writing is set when the poller watches the connection for writing.
	writing bool
	// queued is set when the connection is in the list of the connections
	// whose replies are waiting to be sent.
	queued bool
	// kill is set when the connection must be closed. It is set from
	// another goroutine (a slow subscriber), so it is atomic.
	kill atomic.Bool
	// closed is set when the connection is already closed.
	closed bool
	// blocked is set when the client waits for a key to become available.
	blocked *loopBlock
	// out holds the Pub/Sub messages waiting to be written.
	out []resp.Value
	// pushQueued is set when the connection is in the list of the
	// connections with the messages waiting to be written.
	pushQueued bool
}

// loopBlock is a client that waits for a key to become available.
type loopBlock struct {
	// args is the command the client waits with: the arguments are copied,
	// because the connection reuses its buffer for the next command.
	args [][]byte
	// deadline is when the wait expires (zero: the client waits forever).
	deadline time.Time
	// waiter is the registration of the client in the waiters registry.
	waiter *waiter
}

// loopSink collects the replies of a connection.
type loopSink struct {
	buf []byte
}

func (s *loopSink) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	return len(p), nil
}

// newLoopConn creates the state of a connection.
func newLoopConn(fd int, l *loopState) *loopConn {
	c := &loopConn{fd: fd, loop: l}
	c.in = make([]byte, 0, resp.DefaultReadBufferSize)
	c.sink = &loopSink{buf: make([]byte, 0, 4096)}
	c.writer = resp.NewWriterSize(c.sink, l.srv.writeBufferSize())
	c.client = newLoopClient(l.srv, c)
	return c
}

// newLoopClient creates the state of a client served by the event loop. The
// client has no socket of its own: the loop writes the replies.
func newLoopClient(s *Server, conn *loopConn) *client {
	id := clientSeq.Add(1)
	c := &client{
		id:       id,
		slot:     int(id % counterSlots),
		channels: make(map[string]struct{}),
		patterns: make(map[string]struct{}),
		// The queue of the messages is not needed: the loop writes them
		// itself, and the pump goroutine is not started.
		done:    make(chan struct{}),
		watched: make(map[string]struct{}),
		created: time.Now(),
		loop:    conn,
	}
	c.proto.Store(2)
	if addr, err := netloop.PeerAddr(conn.fd); err == nil {
		c.addr = addr
	}
	// The connection is closed by the loop: the client is marked, and the
	// loop closes it in its own thread. The close comes from another
	// goroutine, so it also wakes a loop that is waiting in the poller.
	c.closeFn = func() {
		conn.loop.markKillNotify(conn)
	}
	return c
}

// serveLoops runs the event loops: one goroutine per loop, every loop
// accepts and serves the connections of its own listener. The loops share
// nothing but the server: the kernel spreads the connections between the
// listeners, and the queues that may be filled from another loop (the
// Pub/Sub messages, the woken up clients) are under the mutex of the loop
// that serves them.
//
// The loop states are created and published under s.mu before any goroutine
// starts. Close takes the same mutex: it either sees no loops and owns the
// listeners (the start has not begun), or it sees the complete set and stops
// every loop, even the ones whose goroutine has not started yet. There is no
// window in which the listener descriptors are detached but the loops are not
// published.
//
// It returns when every loop has left. The first error stops the rest of the
// loops.
func (s *Server) serveLoops() error {
	s.mu.Lock()
	if s.stopped {
		// Close ran first: the listeners were closed by stopServing and
		// there is nothing to serve.
		s.mu.Unlock()
		return nil
	}
	fds := s.loopFDs
	s.loopFDs = nil

	loops := make([]*loopState, 0, len(fds))
	for _, fd := range fds {
		l, err := newLoopState(s, fd)
		if err != nil {
			s.mu.Unlock()
			// The loops that were created own their pollers and
			// listeners. The listeners whose loop was not created are
			// still unowned: close them here.
			for _, l := range loops {
				l.destroy()
			}
			for _, fd := range fds[len(loops):] {
				netloop.Close(fd)
			}
			return err
		}
		loops = append(loops, l)
	}
	s.loops = loops
	s.mu.Unlock()

	if len(loops) == 0 {
		// No listener is left (Close already took them): do not wait for
		// an error that nobody will send.
		return nil
	}

	errs := make(chan error, len(loops))
	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Add(1)
		go func(l *loopState) {
			defer wg.Done()
			errs <- l.run()
		}(l)
	}
	// A loop that leaves with an error stops the rest: the server cannot
	// serve without all of its listeners.
	err := <-errs
	for _, l := range loops {
		l.stop()
	}
	wg.Wait()
	s.mu.Lock()
	s.loops = nil
	s.mu.Unlock()
	// The loops leave with nil when the server is stopped: the error of
	// the run is reported even when another loop left first.
	for err == nil && len(errs) > 0 {
		err = <-errs
	}
	return err
}

// newLoopState creates the state of one event loop over the given listener.
func newLoopState(s *Server, ln int) (*loopState, error) {
	p, err := netloop.NewPoller()
	if err != nil {
		return nil, err
	}
	return &loopState{
		srv:     s,
		poller:  p,
		ln:      ln,
		conns:   make(map[int]*loopConn),
		blocked: make(map[*loopConn]struct{}),
	}, nil
}

// run serves the connections of the loop until the loop is stopped. The loop
// owns the listener: it closes the listener when it leaves.
func (l *loopState) run() error {
	defer func() {
		// The loop has left: the connections and the listener are closed,
		// so the server does not accept the connections any more.
		l.closeAll()
		l.mu.Lock()
		l.finished = true
		l.closePollerLocked()
		l.destroyed = true
		l.mu.Unlock()
		if l.ln > 0 {
			netloop.Close(l.ln)
			l.ln = 0
		}
	}()
	if err := l.poller.AddRead(l.ln); err != nil {
		return err
	}
	for {
		if l.isStopped() {
			return nil
		}
		events, err := l.poller.Wait(l.nextTimeout())
		if err != nil {
			if l.isStopped() {
				return nil
			}
			return err
		}
		l.handleEvents(events)
		l.serveRunnable()
		l.expireBlocked()
		l.flushPush()
		l.flushWrites()
		l.closeKilled()
	}
}

// closePollerLocked releases the poller. The caller holds l.mu: a notifier
// holds the same mutex while it writes to the wakeup channel, so the
// notification can never reach a closed (and possibly reused) descriptor.
func (l *loopState) closePollerLocked() {
	if l.pollerClosed {
		return
	}
	l.pollerClosed = true
	l.poller.Close()
}

// notifyLocked wakes the poller if it is still alive. The caller holds l.mu.
func (l *loopState) notifyLocked() {
	if l.pollerClosed {
		return
	}
	l.poller.Notify()
}

// destroy releases the resources of a loop that did not run: the poller and
// the listener. It is safe on a loop that has left: the loop released its
// resources and marked itself destroyed.
func (l *loopState) destroy() {
	l.mu.Lock()
	if l.destroyed {
		l.mu.Unlock()
		return
	}
	l.closePollerLocked()
	l.destroyed = true
	l.mu.Unlock()
	if l.ln > 0 {
		netloop.Close(l.ln)
		l.ln = 0
	}
}

// isStopped reports whether the server was closed.
func (l *loopState) isStopped() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stopped
}

// stop asks the loop to finish: it wakes the poller, so the wait returns and
// the loop leaves.
func (l *loopState) stop() {
	l.mu.Lock()
	if l.stopped || l.finished {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	l.notifyLocked()
	l.mu.Unlock()
}

// handleEvents serves the events of the ready descriptors.
func (l *loopState) handleEvents(events []netloop.Event) {
	for _, ev := range events {
		if ev.Wakeup {
			// The notification is not a socket event: the queues it
			// filled are served below.
			continue
		}
		if ev.FD == l.ln {
			l.accept()
			continue
		}
		c := l.conns[ev.FD]
		if c == nil {
			continue
		}
		if ev.Readable {
			l.read(c)
		}
		if c.closed {
			continue
		}
		if ev.Writable {
			// The socket has room for the replies that did not fit
			// before.
			l.write(c)
		}
	}
}

// accept takes the connections the listener accepted.
func (l *loopState) accept() {
	for {
		fd, err := netloop.Accept(l.ln)
		if err != nil {
			return
		}
		if l.isStopped() {
			// The server began to stop while the listener was ready: the
			// connection is not registered.
			netloop.Close(fd)
			return
		}
		c := newLoopConn(fd, l)
		l.conns[fd] = c
		if !l.srv.addLoopClient(c.client) {
			// Close began between Accept and the registration.
			delete(l.conns, fd)
			netloop.Close(fd)
			c.client.closeDone()
			return
		}
		l.srv.countConn(1)
		l.watchRead(c)
	}
}

// read reads the requests of a connection and runs the commands.
func (l *loopState) read(c *loopConn) {
	if l.loopRead(c) {
		// The client closed the connection: it is closed after the last
		// answers went out.
		l.markKill(c)
	}
	l.markWrite(c)
}

// loopRead reads the requests of a connection and runs the commands. It
// reports whether the connection must be closed.
//
// The data that came with the end of the file is run before the connection is
// closed: the client waits for the last answer.
func (l *loopState) loopRead(c *loopConn) (close bool) {
	// Read the data that is waiting.
	for {
		if c.pos == len(c.in) {
			c.in = c.in[:0]
			c.pos = 0
		}
		if len(c.in) == cap(c.in) {
			// The buffer holds a command that is not complete and there
			// is no room left: the buffer is grown.
			grown := make([]byte, len(c.in), cap(c.in)*2)
			copy(grown, c.in)
			c.in = grown
		}
		n, err := netloop.Read(c.fd, c.in[len(c.in):cap(c.in)])
		if n > 0 {
			c.in = c.in[:len(c.in)+n]
			continue
		}
		if netloop.WouldBlock(err) {
			break
		}
		// The client closed the connection or it is broken. The data that
		// was read before is still run.
		close = true
		break
	}
	// Run the commands of the buffer.
	for {
		args, used, complete, err := resp.ParseCommandLimits(c.in[c.pos:], c.args, resp.DefaultMaxBulkLen, resp.DefaultMaxArrayLen)
		if err != nil {
			// A protocol error: the answer goes out and the connection is
			// closed, like in the goroutine model.
			l.loopProtocolError(c, err)
			return true
		}
		if !complete {
			// The command is not complete: wait for the rest of it.
			return close
		}
		c.pos += used
		c.args = args
		l.loopCommand(c, args)
	}
}

// loopCommand runs one command of a connection.
func (l *loopState) loopCommand(c *loopConn, args [][]byte) {
	s := l.srv
	var ing commandIngress
	ing.begin(s, c.client, &c.ctx, args, c.writer)

	if ing.route() {
		c.writer.Flush()
		l.markWrite(c)
		return
	}

	ing.execute()
	if req := ing.waiting(); req != nil {
		// A blocking command found nothing to pop: the client waits for a
		// key to become available, and the loop goes on with the other
		// connections.
		if ing.suppressed() {
			ing.flushShadow()
		} else {
			// Whatever the command wrote before it started waiting goes
			// out now: the client is not answered until the command
			// finishes, but the buffer must not hold a stale reply.
			c.writer.Flush()
		}
		l.block(c, args, req)
		return
	}
	switch ing.finish() {
	case endSuppressed:
		c.writer.Flush()
		l.markWrite(c)
		return
	case endQuit:
		c.writer.Flush()
		l.markWrite(c)
		l.markKill(c)
		return
	}
	c.writer.Flush()
	l.markWrite(c)
}

// loopProtocolError writes the answer of a protocol error.
func (l *loopState) loopProtocolError(c *loopConn, err error) {
	switch {
	case errors.Is(err, resp.ErrNotArray):
		c.writer.WriteError("ERR Protocol error: expected command array")
	case errors.Is(err, resp.ErrNotBulk):
		c.writer.WriteError("ERR Protocol error: command arguments must be bulk strings")
	default:
		c.writer.WriteError("ERR Protocol error: " + err.Error())
	}
	c.writer.Flush()
	l.markWrite(c)
}

// block puts a client that waits for a key into the waiting list: the loop
// goes on with the other connections, and the command is run again when the
// key may have become available. A client that waits for the replicas
// (WAIT/WAITAOF) parks the same way; its waiter lives in the waitAck list of
// the replication state instead of the key registry.
func (l *loopState) block(c *loopConn, args [][]byte, req *waitRequest) {
	bl := &loopBlock{args: copyArgs(args)}
	if req.timeout > 0 {
		bl.deadline = time.Now().Add(req.timeout)
	}
	if req.replicasWait {
		bl.waiter = &waiter{
			c:        c.client,
			ready:    make(chan struct{}),
			done:     make(chan struct{}),
			loop:     c,
			replWait: true,
			woff:     req.woff,
			num:      req.num,
			waitAof:  req.waitAof,
			ackLocal: req.ackLocal,
		}
		c.blocked = bl
		l.blocked[c] = struct{}{}
		l.srv.addWaitAck(bl.waiter)
		// The client waits: the connection is not read until it is woken up.
		l.unwatchRead(c)
		return
	}
	keys := copyArgs(req.keys)
	var dest []byte
	if req.dest != nil {
		dest = append([]byte(nil), req.dest...)
	}
	bl.waiter = &waiter{
		c:    c.client,
		keys: keys,
		kind: req.kind,
		dest: dest,
		loop: c,
	}
	c.blocked = bl
	l.blocked[c] = struct{}{}
	l.srv.waiters.add(bl.waiter)
	// The client waits: the connection is not read until it is woken up.
	l.unwatchRead(c)
}

// copyArgs copies the arguments of a command: they outlive the command,
// because the connection reuses its buffer for the next one.
func copyArgs(args [][]byte) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = append([]byte(nil), a...)
	}
	return out
}

// serveRunnable runs the commands of the clients that were woken up.
func (l *loopState) serveRunnable() {
	l.mu.Lock()
	list := l.runnable
	l.runnable = nil
	l.mu.Unlock()
	for _, c := range list {
		l.serveBlocked(c)
	}
}

// serveBlocked runs the command of a client that was waiting for a key.
func (l *loopState) serveBlocked(c *loopConn) {
	bl := c.blocked
	if bl == nil {
		return
	}
	// The waiter leaves the registry: the client registers again if it has
	// to wait more.
	if bl.waiter.replWait {
		l.srv.removeWaitAck(bl.waiter)
	} else {
		l.srv.waiters.remove(bl.waiter)
	}
	delete(l.blocked, c)
	c.blocked = nil
	// The connection is read again.
	l.watchRead(c)
	if err := bl.waiter.takeError(); err != nil {
		// The client was unblocked with CLIENT UNBLOCK: the answer is the
		// error, not the result of the command.
		c.writer.WriteError(err.Error())
		c.writer.Flush()
		l.markWrite(c)
		return
	}
	l.loopCommand(c, bl.args)
}

// expireBlocked answers the clients whose wait expired.
func (l *loopState) expireBlocked() {
	if len(l.blocked) == 0 {
		return
	}
	now := time.Now()
	for c := range l.blocked {
		bl := c.blocked
		if bl == nil || bl.deadline.IsZero() || now.Before(bl.deadline) {
			continue
		}
		if bl.waiter.replWait {
			// WAIT/WAITAOF answers with what it has: the number of the
			// replicas that caught up (the local count of WAITAOF was
			// computed before the wait).
			l.srv.removeWaitAck(bl.waiter)
			if bl.waiter.waitAof {
				c.writer.WriteValue(waitAofReply(bl.waiter.ackLocal, int64(l.srv.countAcks(bl.waiter.woff))))
			} else {
				c.writer.WriteInteger(int64(l.srv.countAcks(bl.waiter.woff)))
			}
		} else {
			l.srv.waiters.remove(bl.waiter)
			// The timeout expired: the answer is a null array.
			c.writer.WriteNullArray()
		}
		delete(l.blocked, c)
		c.blocked = nil
		c.writer.Flush()
		l.markWrite(c)
		// The connection is read again.
		l.watchRead(c)
	}
}

// nextTimeout returns the time the loop may wait: the nearest deadline of the
// blocked clients. A negative timeout means "wait forever".
func (l *loopState) nextTimeout() time.Duration {
	if len(l.blocked) == 0 {
		return -1
	}
	now := time.Now()
	var nearest time.Time
	for c := range l.blocked {
		bl := c.blocked
		if bl == nil || bl.deadline.IsZero() {
			continue
		}
		if nearest.IsZero() || bl.deadline.Before(nearest) {
			nearest = bl.deadline
		}
	}
	if nearest.IsZero() {
		return -1
	}
	d := nearest.Sub(now)
	if d <= 0 {
		return 0
	}
	return d
}

// wakeup marks a blocked client as ready to run its command again.
func (c *loopConn) wakeup() {
	l := c.loop
	l.mu.Lock()
	l.runnable = append(l.runnable, c)
	// The loop may be waiting for the readiness of the sockets: the
	// notification wakes it. It is sent under the same mutex that closes
	// the poller, so it cannot reach a closed descriptor.
	l.notifyLocked()
	l.mu.Unlock()
}

// pushFrame queues a Pub/Sub message for the connection: the loop writes it
// with the next replies. It reports false when the subscriber fell behind
// and the connection was closed.
func (c *loopConn) pushFrame(frame resp.Value) bool {
	l := c.loop
	l.mu.Lock()
	if len(c.out) >= pubsubQueueSize {
		l.mu.Unlock()
		// The subscriber fell behind: the connection is closed, like Redis
		// drops a client with a full output buffer. The publisher may be
		// another goroutine, so wake the loop to close it.
		l.markKillNotify(c)
		return false
	}
	c.out = append(c.out, frame)
	if !c.pushQueued {
		c.pushQueued = true
		l.push = append(l.push, c)
	}
	// The loop may be waiting for the readiness of the sockets: the
	// notification wakes it. It is sent under the same mutex that closes
	// the poller, so it cannot reach a closed descriptor.
	l.notifyLocked()
	l.mu.Unlock()
	return true
}

// flushPush writes the Pub/Sub messages that are waiting.
func (l *loopState) flushPush() {
	l.mu.Lock()
	list := l.push
	l.push = nil
	l.mu.Unlock()
	for _, c := range list {
		l.mu.Lock()
		frames := c.out
		c.out = nil
		c.pushQueued = false
		l.mu.Unlock()
		for _, f := range frames {
			c.writer.WriteValue(f)
		}
		c.writer.Flush()
		l.markWrite(c)
	}
}

// markWrite remembers that the connection has replies waiting to be sent.
func (l *loopState) markWrite(c *loopConn) {
	if c.queued {
		return
	}
	c.queued = true
	l.toWrite = append(l.toWrite, c)
}

// flushWrites sends the replies of the connections that have them.
func (l *loopState) flushWrites() {
	for len(l.toWrite) > 0 {
		list := l.toWrite
		l.toWrite = l.spare[:0]
		l.spare = list
		for _, c := range list {
			c.queued = false
			l.flushConn(c)
		}
	}
}

// flushConn writes the replies of a connection to the socket. The journal is
// flushed first: a client must not see the answer of a command that is not
// in the log yet.
func (l *loopState) flushConn(c *loopConn) {
	if c.closed || len(c.sink.buf) == 0 {
		return
	}
	if s := l.srv; s.AOF != nil {
		if err := s.AOF.Flush(); err != nil {
			s.Logger.Printf("server: aof flush: %v", err)
		}
	}
	l.write(c)
}

// write sends the replies of a connection to the socket. The replies that
// did not fit stay in the buffer: the connection is watched for writing until
// they are out.
func (l *loopState) write(c *loopConn) {
	for len(c.sink.buf) > 0 {
		n, err := netloop.Write(c.fd, c.sink.buf)
		if n > 0 {
			c.sink.buf = c.sink.buf[n:]
			continue
		}
		if netloop.WouldBlock(err) {
			// The socket is full: the connection is watched for writing
			// and not read until the replies are out.
			c.pending = true
			l.unwatchRead(c)
			l.watchWrite(c)
			return
		}
		// The socket is broken: the connection is closed.
		l.closeConn(c)
		return
	}
	// Everything went out.
	if c.pending {
		c.pending = false
		l.unwatchWrite(c)
		l.watchRead(c)
	}
}

// watchRead starts watching the connection for reading.
func (l *loopState) watchRead(c *loopConn) {
	if c.reading || c.pending || c.blocked != nil || c.kill.Load() || c.closed {
		return
	}
	if err := l.poller.AddRead(c.fd); err == nil {
		c.reading = true
	}
}

// unwatchRead stops watching the connection for reading.
func (l *loopState) unwatchRead(c *loopConn) {
	if !c.reading {
		return
	}
	l.poller.DelRead(c.fd)
	c.reading = false
}

// watchWrite starts watching the connection for writing.
func (l *loopState) watchWrite(c *loopConn) {
	if c.writing {
		return
	}
	if err := l.poller.AddWrite(c.fd); err == nil {
		c.writing = true
	}
}

// unwatchWrite stops watching the connection for writing.
func (l *loopState) unwatchWrite(c *loopConn) {
	if !c.writing {
		return
	}
	l.poller.DelWrite(c.fd)
	c.writing = false
}

// markKill marks a connection to be closed when the loop itself noticed the
// condition (EOF, QUIT). The loop is already running, so it does not need a
// poller notification.
func (l *loopState) markKill(c *loopConn) {
	l.markKillFrom(c, false)
}

// markKillNotify marks a connection from another goroutine (a client close, a
// slow subscriber): the loop may be waiting in the poller, so it is woken.
func (l *loopState) markKillNotify(c *loopConn) {
	l.markKillFrom(c, true)
}

// markKillFrom appends the connection to the close list on the first mark.
// Only the transition notifies, and the notification is sent under the mutex
// that guards the poller lifetime, so it never reaches a closed descriptor.
func (l *loopState) markKillFrom(c *loopConn, notify bool) {
	if c.kill.Swap(true) {
		return
	}
	l.mu.Lock()
	l.toClose = append(l.toClose, c)
	if notify {
		l.notifyLocked()
	}
	l.mu.Unlock()
}

// closeKilled closes the connections that must be closed.
func (l *loopState) closeKilled() {
	l.mu.Lock()
	list := l.toClose
	l.toClose = nil
	l.mu.Unlock()
	for _, c := range list {
		l.closeConn(c)
	}
}

// closeAll closes every connection of the loop: the loop has left.
func (l *loopState) closeAll() {
	for _, c := range l.conns {
		l.closeConn(c)
	}
}

// closeConn closes a connection: the descriptor is released and the client
// leaves the registries.
func (l *loopState) closeConn(c *loopConn) {
	if c.closed {
		return
	}
	c.closed = true
	l.unwatchRead(c)
	l.unwatchWrite(c)
	netloop.Close(c.fd)
	delete(l.conns, c.fd)
	if c.blocked != nil {
		if c.blocked.waiter.replWait {
			l.srv.removeWaitAck(c.blocked.waiter)
		} else {
			l.srv.waiters.remove(c.blocked.waiter)
		}
		delete(l.blocked, c)
		c.blocked = nil
	}
	if c.client != nil {
		l.srv.removeLoopClient(c.client)
		l.srv.PubSub.removeClient(c.client)
		l.srv.unregisterReplica(c.client)
		c.client.unwatchAll(l.srv.DB(c.client.db))
		c.client.closeDone()
	}
	l.srv.countConn(-1)
}
