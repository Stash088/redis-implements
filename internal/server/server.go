package server

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// errQuit signals handleConn to close the client connection after QUIT.
var errQuit = errors.New("QUIT")

// Ctx carries the state needed by a single command invocation.
type Ctx struct {
	Store  *store.Store
	Args   [][]byte
	W      *resp.Writer
	Srv    *Server
	Client *client
}

// proto3 reports whether the client speaks RESP3.
func (c *Ctx) proto3() bool {
	return c.Client != nil && c.Client.Proto() >= 3
}

// Handler implements a single Redis command.
type Handler func(*Ctx) error

// Server is a TCP server that speaks the RESP protocol against a set of
// keyspaces (the Redis databases). Each client selects its current database
// with SELECT; commands always run against the client's database.
type Server struct {
	dbs     []*store.Store
	Expirer *store.Expirer
	AOF     *persist.Appender
	PubSub  *PubSub
	Logger  *log.Logger
	cmds    map[string]cmdSpec

	// Persistence paths (empty disables the related feature).
	DBPath  string
	AOFPath string

	saveMu   sync.Mutex
	aofMu    sync.Mutex
	dirty    atomic.Int64
	lastSave atomic.Int64 // unix seconds of the last successful save
	saveStop chan struct{}

	// Memory management (see evict.go in store): --maxmemory and policy.
	maxMemory        atomic.Int64
	maxMemoryPolicy  atomic.Pointer[string]
	maxMemorySamples atomic.Int64
	evicted          atomic.Uint64

	// Slow log.
	sl *slowlog

	// Password clients must authenticate with (empty disables it).
	requirepass atomic.Pointer[string]

	// Server metadata for INFO/CONFIG (set at startup, read-only afterwards).
	appendFsync string
	saveSeconds int
	saveChanges int

	ln       net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	clients  map[net.Conn]*client
	started  time.Time
	received uint64
	connsNum int64
}

// propagate appends args to the AOF when one is enabled.
func (c *Ctx) propagate(args ...[][]byte) {
	if c.Srv == nil || c.Srv.AOF == nil {
		return
	}
	for _, a := range args {
		if err := c.Srv.AOF.Append(a); err != nil {
			c.Srv.Logger.Printf("server: aof append: %v", err)
		}
	}
}

// propagateCmd writes the current command to the AOF, rewriting relative TTLs
// to absolute expiry. Handlers call it only after a mutation took effect.
func (c *Ctx) propagateCmd() {
	if c.Srv == nil {
		return
	}
	c.Srv.dirty.Add(1)
	if c.Srv.AOF == nil {
		return
	}
	for _, cmd := range persist.RewriteForAOF(c.Args, c.Store.Now()) {
		c.propagate(cmd)
	}
}

// propagateFromDispatch logs a write command to the AOF after execution. The
// command must already be known to be a write command handled centrally.
func (s *Server) propagateFromDispatch(args [][]byte) {
	if s.AOF == nil {
		return
	}
	for _, c := range persist.RewriteForAOF(args, s.dbs[0].Now()) {
		if err := s.AOF.Append(c); err != nil {
			s.Logger.Printf("server: aof append: %v", err)
		}
	}
}

// SetLogger overrides the logger used for connection lifecycle messages.
func (s *Server) SetLogger(l *log.Logger) {
	if l != nil {
		s.Logger = l
	}
}

// New creates a Server bound to the given address, serving the given
// databases (the first one is the default database). The listener is not
// opened until Listen is called.
func New(addr string, dbs []*store.Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		dbs = []*store.Store{store.New()}
	}
	s := &Server{
		dbs:     dbs,
		Logger:  log.Default(),
		cmds:    make(map[string]cmdSpec),
		conns:   make(map[net.Conn]struct{}),
		clients: make(map[net.Conn]*client),
		PubSub:  NewPubSub(),
		ln:      ln,
		started: time.Now(),
	}
	def := "noeviction"
	s.maxMemoryPolicy.Store(&def)
	s.maxMemorySamples.Store(5)
	s.sl = newSlowlog(10000, 128)
	s.registerCommands()
	return s, nil
}

// SetSlowlog configures the slow log threshold (microseconds) and capacity.
func (s *Server) SetSlowlog(threshold int64, maxLen int) {
	s.sl.setConfig(threshold, maxLen)
}

// SetAppendFsync records the AOF fsync policy name for CONFIG GET/INFO.
// Call before Serve.
func (s *Server) SetAppendFsync(name string) { s.appendFsync = name }

// SetMaxMemory configures the maxmemory limit (bytes, 0 disables eviction).
func (s *Server) SetMaxMemory(n int64) { s.maxMemory.Store(n) }

// SetMaxMemoryPolicy configures the eviction policy.
func (s *Server) SetMaxMemoryPolicy(p string) { s.maxMemoryPolicy.Store(&p) }

// SetMaxMemorySamples configures how many keys are sampled per eviction cycle.
func (s *Server) SetMaxMemorySamples(n int64) { s.maxMemorySamples.Store(n) }

// maxMemoryPolicyName returns the active eviction policy.
func (s *Server) maxMemoryPolicyName() string {
	if p := s.maxMemoryPolicy.Load(); p != nil {
		return *p
	}
	return "noeviction"
}

// Addr returns the bound listener address (useful with port 0).
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// DB returns the store of the database with the given index. Out-of-range
// indexes fall back to the first database.
func (s *Server) DB(i int) *store.Store {
	if i < 0 || i >= len(s.dbs) {
		return s.dbs[0]
	}
	return s.dbs[i]
}

// DBCount returns the number of databases.
func (s *Server) DBCount() int { return len(s.dbs) }

// usedBytes returns the approximate memory footprint of all databases.
func (s *Server) usedBytes() int {
	n := 0
	for _, db := range s.dbs {
		n += db.UsedBytes()
	}
	return n
}

// Close stops the server and closes all active connections.
func (s *Server) Close() error {
	if s.saveStop != nil {
		close(s.saveStop)
		s.saveStop = nil
	}
	if s.Expirer != nil {
		s.Expirer.Stop()
	}
	if s.AOF != nil {
		s.AOF.Close()
		s.AOF = nil
	}
	err := s.ln.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.conns = make(map[net.Conn]struct{})
	s.mu.Unlock()
	return err
}

// Serve accepts connections until the listener is closed.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		atomic.AddInt64(&s.connsNum, 1)
		c := newClient(conn)
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.clients[conn] = c
		s.mu.Unlock()
		go s.handleConn(conn, c)
	}
}

func (s *Server) handleConn(conn net.Conn, c *client) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		delete(s.clients, conn)
		s.mu.Unlock()
		atomic.AddInt64(&s.connsNum, -1)
		s.PubSub.removeClient(c)
		c.unwatchAll(s.DB(c.db))
		close(c.done)
		conn.Close()
		s.Logger.Printf("server: client disconnected: %s", conn.RemoteAddr())
	}()

	s.Logger.Printf("server: client connected: %s", conn.RemoteAddr())
	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)

	for {
		// Send the answers we owe the client before blocking on the socket.
		// When more pipelined commands are already buffered the replies stay
		// in the write buffer and leave in a single write, so a pipeline of
		// N commands costs one syscall instead of N.
		c.writeMu.Lock()
		if !r.HasBufferedValue() {
			if err := w.Flush(); err != nil {
				c.writeMu.Unlock()
				s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
		c.writeMu.Unlock()

		v, err := r.ReadValue()
		if err != nil {
			if err != io.EOF {
				s.Logger.Printf("server: read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if v.Kind != resp.KindArray || v.IsNull {
			c.writeMu.Lock()
			w.WriteError("ERR Protocol error: expected command array")
			w.Flush()
			c.writeMu.Unlock()
			return
		}
		args := make([][]byte, 0, len(v.Array))
		for i := range v.Array {
			el := v.Array[i]
			if el.Kind != resp.KindBulkString || el.IsNull {
				c.writeMu.Lock()
				w.WriteError("ERR Protocol error: command arguments must be bulk strings")
				w.Flush()
				c.writeMu.Unlock()
				return
			}
			args = append(args, el.Str)
		}
		if len(args) == 0 {
			c.writeMu.Lock()
			w.WriteError("ERR empty command")
			w.Flush()
			c.writeMu.Unlock()
			continue
		}

		atomic.AddUint64(&s.received, 1)
		ctx := &Ctx{Store: s.DB(c.db), Args: args, W: w, Srv: s, Client: c}
		name := strings.ToLower(string(args[0]))

		// The Pub/Sub pump goroutine shares the connection's bufio with the
		// command loop, so command execution and the final Flush must both
		// happen under writeMu.
		c.writeMu.Lock()
		c.setLastCmd(name)
		if c.inMulti && !txnImmediate[name] {
			s.queueForMulti(ctx, name)
			if err := w.Flush(); err != nil {
				c.writeMu.Unlock()
				s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
				return
			}
			c.writeMu.Unlock()
			continue
		}
		// CLIENT REPLY OFF/SKIP suppresses the answer: the command runs
		// against a throwaway writer, so nothing reaches the client. A
		// command that turns the replies back on (CLIENT REPLY ON) still
		// gets its answer, like in Redis.
		mode := c.replyMode
		suppress := mode != replyOn
		realW := ctx.W
		var shadow *resp.Writer
		var shadowBuf *bytes.Buffer
		if suppress {
			shadowBuf = &bytes.Buffer{}
			shadow = resp.NewWriter(shadowBuf)
			ctx.W = shadow
		}
		started := time.Now()
		err = s.executeCommand(ctx)
		s.sl.maybeAdd(args, c.addr, c.name, time.Since(started))
		if suppress {
			shadow.Flush()
			if mode == replySkip {
				c.replyMode = replyOn
			}
			if mode == replyOff && c.replyMode == replyOn {
				realW.WriteRaw(shadowBuf.Bytes())
			}
			c.writeMu.Unlock()
			continue
		}
		if err == errQuit {
			w.Flush()
			c.writeMu.Unlock()
			return
		}
		c.writeMu.Unlock()
	}
}

// executeCommand dispatches a single command and performs the bookkeeping
// that surrounds execution: maxmemory enforcement, AOF propagation and the
// dirty counter. It is used both by the connection loop and by EXEC when
// replaying the transaction queue.
func (s *Server) executeCommand(ctx *Ctx) error {
	name := strings.ToLower(string(ctx.Args[0]))
	spec, _ := s.cmds[name]

	// Deny memory-adding commands under noeviction once the store is over
	// the limit, like Redis.
	if m := s.maxMemory.Load(); m > 0 && spec.memAdd && s.maxMemoryPolicyName() == "noeviction" {
		if int64(s.usedBytes()) > m {
			return ctx.W.WriteError("OOM command not allowed when used memory > 'maxmemory'.")
		}
	}

	err := s.dispatch(ctx)
	if spec.write && !spec.aofSelf {
		s.propagateFromDispatch(ctx.Args)
	}
	if spec.write {
		s.dirty.Add(1)
		// Evict only when a policy is configured; noeviction is handled by
		// the pre-execution check above.
		if m := s.maxMemory.Load(); m > 0 && s.maxMemoryPolicyName() != "noeviction" && int64(s.usedBytes()) > m {
			if n := s.evictCycle(m); n > 0 {
				s.evicted.Add(uint64(n))
			}
		}
	}
	return err
}

func (s *Server) dispatch(ctx *Ctx) error {
	name := strings.ToLower(string(ctx.Args[0]))
	// Without authentication only a few commands are allowed.
	if s.authRequired() && ctx.Client != nil && !ctx.Client.authenticated && !authExempt[name] {
		return ctx.W.WriteError(noAuthError)
	}
	if ctx.Client != nil && ctx.Client.subscribed() && !allowedInSubscribeMode[name] {
		return ctx.W.WriteError("ERR Can't execute '" + name + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
	}
	spec, ok := s.cmds[name]
	if !ok {
		return writeUnknownCommand(ctx)
	}
	return spec.handler(ctx)
}

func writeUnknownCommand(ctx *Ctx) error {
	var b strings.Builder
	for _, a := range ctx.Args[1:] {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString("'")
		b.Write(a)
		b.WriteString("'")
	}
	ctx.W.WriteError("ERR unknown command '" + string(ctx.Args[0]) + "', with args beginning with: " + b.String())
	return nil
}
