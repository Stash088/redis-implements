package server

import (
	"errors"
	"strings"
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
// The commands that need a goroutine of their own (the blocking ones and the
// subscriptions) are not served in this mode yet: see cmdNeedsGoroutine.

// errLoopMode reports that a command is not served by the event loop.
var errLoopMode = errors.New("ERR the command is not served in the event loop mode")

// loopConn is the state of a connection served by the event loop.
type loopConn struct {
	fd int
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
func newLoopConn(fd int, s *Server) *loopConn {
	c := &loopConn{fd: fd}
	c.in = make([]byte, 0, resp.DefaultReadBufferSize)
	c.sink = &loopSink{buf: make([]byte, 0, 4096)}
	c.writer = resp.NewWriterSize(c.sink, s.writeBufferSize())
	c.client = newLoopClient(s)
	return c
}

// newLoopClient creates the state of a client served by the event loop. The
// client has no socket of its own: the loop writes the replies.
func newLoopClient(s *Server) *client {
	id := clientSeq.Add(1)
	c := &client{
		id:       id,
		slot:     int(id % counterSlots),
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

// serveLoop serves the connections with one event loop.
func (s *Server) serveLoop() error {
	p, err := netloop.NewPoller()
	if err != nil {
		return err
	}
	ln := s.loopFD
	if err := p.AddRead(ln); err != nil {
		return err
	}
	conns := make(map[int]*loopConn)
	for {
		events, err := p.Wait()
		if err != nil {
			return err
		}
		for _, ev := range events {
			if ev.FD == ln {
				for {
					fd, err := netloop.Accept(ln)
					if err != nil {
						break
					}
					conns[fd] = newLoopConn(fd, s)
					if err := p.AddRead(fd); err != nil {
						return err
					}
				}
				continue
			}
			c := conns[ev.FD]
			if c == nil {
				continue
			}
			closed := false
			if ev.Readable {
				var ok bool
				ok, closed = s.loopRead(c)
				if !ok {
					// The protocol is wrong: the connection is closed
					// after the answer goes out.
					closed = true
				}
			}
			if len(c.sink.buf) == 0 {
				continue
			}
			// The replies are written to the socket. The journal is
			// flushed first: a client must not see the answer of a
			// command that is not in the log yet.
			if s.AOF != nil {
				if err := s.AOF.Flush(); err != nil {
					s.Logger.Printf("server: aof flush: %v", err)
				}
			}
			for len(c.sink.buf) > 0 {
				n, err := netloop.Write(c.fd, c.sink.buf)
				if n > 0 {
					c.sink.buf = c.sink.buf[n:]
					continue
				}
				if netloop.WouldBlock(err) {
					// The socket is full: the connection is watched
					// for writing, and not read until the replies are
					// out.
					c.pending = true
					p.DelRead(c.fd)
					p.AddWrite(c.fd)
					break
				}
				p.DelRead(c.fd)
				p.DelWrite(c.fd)
				netloop.Close(c.fd)
				delete(conns, c.fd)
				break
			}
			if c.pending && len(c.sink.buf) == 0 {
				c.pending = false
				p.DelWrite(c.fd)
				p.AddRead(c.fd)
			}
			if closed {
				// The client closed the connection: it is closed after
				// the last answers went out.
				p.DelRead(c.fd)
				p.DelWrite(c.fd)
				netloop.Close(c.fd)
				delete(conns, c.fd)
			}
		}
	}
}

// loopRead reads the requests of a connection and runs the commands. It
// reports whether the connection may go on and whether the client closed it.
//
// The data that came with the end of the file is run before the connection is
// closed: the client waits for the last answer.
func (s *Server) loopRead(c *loopConn) (ok bool, closed bool) {
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
		closed = true
		break
	}
	// Run the commands of the buffer.
	for {
		args, used, complete, err := resp.ParseCommandLimits(c.in[c.pos:], c.args, resp.DefaultMaxBulkLen, resp.DefaultMaxArrayLen)
		if err != nil {
			// A protocol error: the answer goes out and the connection is
			// closed, like in the goroutine model.
			s.loopProtocolError(c, err)
			return false, true
		}
		if !complete {
			// The command is not complete: wait for the rest of it.
			return true, closed
		}
		c.pos += used
		c.args = args
		if !s.loopCommand(c, args) {
			return false, true
		}
	}
}

// loopCommand runs one command of a connection.
func (s *Server) loopCommand(c *loopConn, args [][]byte) bool {
	s.received[c.client.slot].Add(1)
	ctx := &c.ctx
	*ctx = Ctx{Store: s.DB(c.client.db), Args: args, W: c.writer, Srv: s, Client: c.client}

	spec, ok := s.lookupCommand(args[0])
	if !ok {
		// Unknown command: the name is only needed for the error message,
		// so the string is built on this cold path.
		unknown := strings.ToLower(string(args[0]))
		ctx.cmdName = unknown
		ctx.cmdNamePtr = &unknown
	} else {
		ctx.cmdName = spec.name
		ctx.cmdNamePtr = spec.namePtr
		ctx.spec = spec
	}
	name := ctx.cmdName
	c.client.setLastCmd(ctx.cmdNamePtr)

	if cmdNeedsGoroutine(ctx.cmdName) {
		// The command needs a goroutine of its own: the event loop does
		// not serve it yet.
		ctx.W.WriteError(errLoopMode.Error())
		c.writer.Flush()
		return true
	}

	if c.client.inMulti && !txnImmediate[name] {
		s.queueForMulti(ctx, name)
		c.writer.Flush()
		return true
	}

	started := time.Now()
	err := s.executeCommand(ctx)
	s.sl.maybeAdd(args, c.client, time.Since(started))
	if spec.write && len(args) > 1 {
		// A write may have made a key available for a blocked client.
		s.waiters.signal(args[1])
	}
	if err == errQuit {
		c.writer.Flush()
		return false
	}
	c.writer.Flush()
	return true
}

// loopProtocolError writes the answer of a protocol error.
func (s *Server) loopProtocolError(c *loopConn, err error) {
	switch {
	case errors.Is(err, resp.ErrNotArray):
		c.writer.WriteError("ERR Protocol error: expected command array")
	case errors.Is(err, resp.ErrNotBulk):
		c.writer.WriteError("ERR Protocol error: command arguments must be bulk strings")
	default:
		c.writer.WriteError("ERR Protocol error: " + err.Error())
	}
	c.writer.Flush()
}

// cmdNeedsGoroutine reports whether the command needs a goroutine of its own
// and is therefore not served by the event loop.
func cmdNeedsGoroutine(name string) bool {
	switch name {
	case "blpop", "brpop", "blmove", "bzpopmin", "bzpopmax",
		"subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		return true
	}
	return false
}
