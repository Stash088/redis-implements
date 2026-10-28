package server

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amir/redis-implements/internal/netloop"
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

	// spec is the resolved command and cmdName its canonical (lower case)
	// name. The connection loop fills them in once per command so that the
	// dispatch path does not have to look the command up again. cmdNamePtr
	// points to the same name and is stable: the client keeps it to show
	// the last command it ran.
	spec       cmdSpec
	cmdName    string
	cmdNamePtr *string

	// wait is set by a blocking command that found nothing to pop: the
	// connection loop waits for a key to become available and retries.
	wait *waitRequest
}

// proto3 reports whether the client speaks RESP3.
func (c *Ctx) proto3() bool {
	return c.Client != nil && c.Client.Proto() >= 3
}

// counterSlot returns the slot of the counters the connection writes to. The
// counters are spread over the slots, so the connections do not share a
// cache line.
func (c *Ctx) counterSlot() int {
	if c.Client == nil {
		return 0
	}
	return c.Client.slot
}

// Handler implements a single Redis command.
type Handler func(*Ctx) error

// Server is a TCP server that speaks the RESP protocol against a set of
// keyspaces (the Redis databases). Each client selects its current database
// with SELECT; commands always run against the client's database.
type Server struct {
	dbs       []*store.Store
	Expirer   *store.Expirer
	AOF       *persist.Appender
	PubSub    *PubSub
	Logger    *log.Logger
	cmds      map[string]cmdSpec
	cmdsUpper map[string]cmdSpec // the same commands in upper case: a lookup by
	// the bytes the client sent must not allocate a string

	// received counts the commands processed. Every connection writes to
	// the counter of its own slot: the counters are padded to a cache line
	// and spread over the slots, so the connections do not fight for the
	// same one.
	received [counterSlots]commandsCounter

	// Sizes of the socket buffers of a connection (0 means the default).
	bufReadSize  int
	bufWriteSize int

	// Persistence paths (empty disables the related feature).
	DBPath  string
	AOFPath string

	saveMu   sync.Mutex
	aofMu    sync.Mutex
	dirty    [counterSlots]counter // writes since the last save
	lastSave atomic.Int64          // unix seconds of the last successful save
	saveStop chan struct{}

	// Memory management (see evict.go in store): --maxmemory and policy.
	maxMemory        atomic.Int64
	maxMemoryPolicy  atomic.Pointer[string]
	maxMemorySamples atomic.Int64
	evicted          atomic.Uint64

	// Slow log.
	sl *slowlog

	// Clients blocked on the keys of the blocking commands.
	waiters *waiters

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
	connsNum int64

	// loopMode is set when the server serves the connections with an event
	// loop instead of a goroutine each. loopAddr is the address it listens
	// on and loopFD the descriptor of the listener.
	loopMode bool
	loopAddr string
	loopFD   int
}

// cacheLineSize is the size of a cache line on the usual architectures. A
// counter that is written by every connection is padded to a whole line, so
// that the connections do not fight for the same one.
const cacheLineSize = 64

// counterSlots is the number of counters the command counters are spread
// over. Every connection writes to the counter of its own slot, so the
// connections do not share a cache line: the counters are written on the path
// of every command, and a shared line would bounce between the cores.
const counterSlots = 16

// counter is an atomic counter padded to a cache line.
type counter struct {
	v atomic.Int64
	_ [cacheLineSize - 8]byte
}

func (c *counter) Add(n int64)        { c.v.Add(n) }
func (c *counter) Load() int64        { return c.v.Load() }
func (c *counter) Store(n int64)      { c.v.Store(n) }
func (c *counter) Swap(n int64) int64 { return c.v.Swap(n) }

// commandsCounter counts the commands processed by the server. Every
// connection writes to the counter of its own slot, so it is padded to a
// cache line.
type commandsCounter struct {
	v atomic.Uint64
	_ [cacheLineSize - 8]byte
}

func (c *commandsCounter) Add(n uint64)         { c.v.Add(n) }
func (c *commandsCounter) Load() uint64         { return c.v.Load() }
func (c *commandsCounter) Store(n uint64)       { c.v.Store(n) }
func (c *commandsCounter) Swap(n uint64) uint64 { return c.v.Swap(n) }

// receivedTotal returns the number of the commands processed by the server.
func (s *Server) receivedTotal() uint64 {
	var n uint64
	for i := range s.received {
		n += s.received[i].Load()
	}
	return n
}

// dirtyTotal returns the number of the writes since the last save.
func (s *Server) dirtyTotal() int64 {
	var n int64
	for i := range s.dirty {
		n += s.dirty[i].Load()
	}
	return n
}

// resetDirty sets the number of the writes since the last save back to zero.
func (s *Server) resetDirty() {
	for i := range s.dirty {
		s.dirty[i].Store(0)
	}
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
//
// The dispatcher writes the commands that propagate themselves to the AOF
// (the ones whose AOF entry depends on whether they took effect), so a
// command that is written here must not be written there as well: a command
// written twice would be applied twice on the replay.
func (c *Ctx) propagateCmd() {
	if c.Srv == nil {
		return
	}
	if c.spec.write && !c.spec.aofSelf {
		return
	}
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
	return newServer(dbs, ln), nil
}

// newServer creates a server around the given listener. The listener is nil
// in the event loop mode: there the connections are served by the loop.
func newServer(dbs []*store.Store, ln net.Listener) *Server {
	if len(dbs) == 0 {
		dbs = []*store.Store{store.New()}
	}
	s := &Server{
		dbs:       dbs,
		Logger:    log.Default(),
		cmds:      make(map[string]cmdSpec),
		cmdsUpper: make(map[string]cmdSpec),
		conns:     make(map[net.Conn]struct{}),
		clients:   make(map[net.Conn]*client),
		PubSub:    NewPubSub(),
		ln:        ln,
		started:   time.Now(),
	}
	def := "noeviction"
	s.maxMemoryPolicy.Store(&def)
	s.maxMemorySamples.Store(5)
	s.waiters = newWaiters()
	s.sl = newSlowlog(10000, 128)
	s.registerCommands()
	return s
}

// SetSlowlog configures the slow log threshold (microseconds) and capacity.
func (s *Server) SetSlowlog(threshold int64, maxLen int) {
	s.sl.setConfig(threshold, maxLen)
}

// SetAppendFsync records the AOF fsync policy name for CONFIG GET/INFO.
// Call before Serve.
func (s *Server) SetAppendFsync(name string) { s.appendFsync = name }

// readBufferSize returns the size of the socket read buffer of a connection.
func (s *Server) readBufferSize() int {
	if s.bufReadSize > 0 {
		return s.bufReadSize
	}
	return resp.DefaultReadBufferSize
}

// writeBufferSize returns the size of the socket write buffer of a connection.
func (s *Server) writeBufferSize() int {
	if s.bufWriteSize > 0 {
		return s.bufWriteSize
	}
	return resp.DefaultWriteBufferSize
}

// SetBufferSizes configures the socket buffers of a connection. Zero means
// the default size.
//
// A bigger read buffer pays off when a client pipelines large commands: they
// arrive in fewer read syscalls. A bigger write buffer pays off when the
// replies of a pipeline do not fit into the default one.
func (s *Server) SetBufferSizes(read, write int) {
	s.bufReadSize = read
	s.bufWriteSize = write
}

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

// port returns the port the server listens on.
func (s *Server) port() int {
	if s.loopMode {
		if _, p, err := net.SplitHostPort(s.loopAddr); err == nil {
			n, _ := strconv.Atoi(p)
			return n
		}
		return 0
	}
	return portOf(s.ln.Addr())
}

// Addr returns the bound listener address (useful with port 0).
func (s *Server) Addr() net.Addr {
	if s.loopMode {
		return loopAddr(s.loopAddr)
	}
	return s.ln.Addr()
}

// loopAddr is the address of the event loop mode.
type loopAddr string

func (a loopAddr) Network() string { return "tcp" }
func (a loopAddr) String() string  { return string(a) }

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

// SwapDBs swaps two databases.
func (s *Server) SwapDBs(a, b int) {
	if a < 0 || a >= len(s.dbs) || b < 0 || b >= len(s.dbs) {
		return
	}
	s.dbs[a], s.dbs[b] = s.dbs[b], s.dbs[a]
}

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
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	if s.loopMode && s.loopFD > 0 {
		netloop.Close(s.loopFD)
		s.loopFD = 0
	}
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.conns = make(map[net.Conn]struct{})
	s.mu.Unlock()
	return err
}

// NewLoop creates a Server that serves the connections with an event loop
// instead of a goroutine each. The commands are executed by the same code as
// the goroutine model; the difference is the I/O.
//
// The event loop does not serve the commands that need a goroutine of their
// own yet (the blocking ones and the subscriptions): they answer with an
// error.
func NewLoop(addr string, dbs []*store.Store) (*Server, error) {
	fd, err := netloop.Listen(addr)
	if err != nil {
		return nil, err
	}
	bound, err := netloop.Addr(fd)
	if err != nil {
		netloop.Close(fd)
		return nil, err
	}
	s := newServer(dbs, nil)
	s.loopMode = true
	s.loopAddr = bound
	s.loopFD = fd
	return s, nil
}

// Serve accepts connections until the listener is closed.
func (s *Server) Serve() error {
	if s.loopMode {
		return s.serveLoop()
	}
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
	r := resp.NewReaderSize(conn, s.readBufferSize())
	w := resp.NewWriterSize(conn, s.writeBufferSize())

	for {
		// Send the answers we owe the client before blocking on the socket.
		// When more pipelined commands are already buffered the replies stay
		// in the write buffer and leave in a single write, so a pipeline of
		// N commands costs one syscall instead of N.
		c.writeMu.Lock()
		if !r.HasBufferedValue() {
			// The AOF is written out before the replies leave: a client
			// must not see an answer for a command that is not in the
			// log yet. The buffer is shared by all the connections, so
			// this also writes the commands of the other connections
			// that are waiting for their replies.
			if s.AOF != nil {
				if err := s.AOF.Flush(); err != nil {
					c.writeMu.Unlock()
					s.Logger.Printf("server: aof flush: %v", err)
					return
				}
			}
			if err := w.Flush(); err != nil {
				c.writeMu.Unlock()
				s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
		c.writeMu.Unlock()

		// The command is read straight into the connection's argument
		// buffer: the generic value reader would build an intermediate
		// slice of values for every command.
		args, err := r.ReadCommand(c.args)
		c.args = args
		if err != nil {
			switch {
			case errors.Is(err, resp.ErrNotArray):
				c.writeMu.Lock()
				w.WriteError("ERR Protocol error: expected command array")
				w.Flush()
				c.writeMu.Unlock()
			case errors.Is(err, resp.ErrNotBulk):
				c.writeMu.Lock()
				w.WriteError("ERR Protocol error: command arguments must be bulk strings")
				w.Flush()
				c.writeMu.Unlock()
			default:
				if err != io.EOF {
					s.Logger.Printf("server: read error from %s: %v", conn.RemoteAddr(), err)
				}
			}
			return
		}
		if len(args) == 0 {
			c.writeMu.Lock()
			w.WriteError("ERR empty command")
			w.Flush()
			c.writeMu.Unlock()
			continue
		}

		s.received[c.slot].Add(1)
		ctx := &c.ctx
		*ctx = Ctx{Store: s.DB(c.db), Args: args, W: w, Srv: s, Client: c}
		// The command is resolved once per command: the lookup works on the
		// bytes the client sent and does not allocate.
		spec, ok := s.lookupCommand(args[0])
		if !ok {
			// Unknown command: the name is only needed for the error
			// message, so the string is built on this cold path.
			unknown := strings.ToLower(string(args[0]))
			ctx.cmdName = unknown
			ctx.cmdNamePtr = &unknown
		} else {
			ctx.cmdName = spec.name
			ctx.cmdNamePtr = spec.namePtr
			ctx.spec = spec
		}
		name := ctx.cmdName

		// The Pub/Sub pump goroutine shares the connection's bufio with the
		// command loop, so command execution and the final Flush must both
		// happen under writeMu.
		c.writeMu.Lock()
		c.setLastCmd(ctx.cmdNamePtr)
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
			// A small buffer: the shadow writer holds one discarded reply.
			shadow = resp.NewWriterSize(shadowBuf, 4096)
			ctx.W = shadow
		}
		started := time.Now()
		err = s.executeCommand(ctx)
		s.sl.maybeAdd(args, c, time.Since(started))
		if spec.write && len(args) > 1 {
			// A write may have made a key available for a blocked client.
			s.waiters.signal(args[1])
		}
		if ctx.wait != nil {
			// A blocking command found nothing to pop: release the write
			// lock (the Pub/Sub pump needs it) and wait for a key to become
			// available, then retry the command.
			req := ctx.wait
			ctx.wait = nil
			c.writeMu.Unlock()
			woken := s.waitForKeys(c, req)
			c.writeMu.Lock()
			if !woken {
				// The timeout expired: reply with a null array.
				if err := ctx.W.WriteNullArray(); err != nil {
					c.writeMu.Unlock()
					s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
					return
				}
				c.writeMu.Unlock()
				continue
			}
			for {
				ctx.wait = nil
				err = s.executeCommand(ctx)
				if ctx.wait == nil {
					break
				}
				// Still nothing: wait again.
				req = ctx.wait
				ctx.wait = nil
				c.writeMu.Unlock()
				woken = s.waitForKeys(c, req)
				c.writeMu.Lock()
				if !woken {
					if err := ctx.W.WriteNullArray(); err != nil {
						c.writeMu.Unlock()
						s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
						return
					}
					break
				}
			}
			if err == errQuit {
				w.Flush()
				c.writeMu.Unlock()
				return
			}
			c.writeMu.Unlock()
			continue
		}
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
	spec := ctx.spec
	if spec.handler == nil {
		spec, _ = s.lookupCommand(ctx.Args[0])
		// The handler may need the metadata of the command (the AOF
		// propagation, for one), so the resolved command is kept.
		ctx.spec = spec
	}

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
		s.dirty[ctx.counterSlot()].Add(1)
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
	spec := ctx.spec
	ok := spec.handler != nil
	if !ok {
		spec, ok = s.lookupCommand(ctx.Args[0])
	}
	name := ctx.cmdName
	if name == "" {
		name = strings.ToLower(string(ctx.Args[0]))
	}
	// Without authentication only a few commands are allowed.
	if s.authRequired() && ctx.Client != nil && !ctx.Client.authenticated && !authExempt[name] {
		return ctx.W.WriteError(noAuthError)
	}
	if ctx.Client != nil && ctx.Client.subscribed() && !allowedInSubscribeMode[name] {
		return ctx.W.WriteError("ERR Can't execute '" + name + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
	}
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
