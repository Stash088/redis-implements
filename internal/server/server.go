package server

import (
	"errors"
	"io"
	"log"
	"net"
	"strconv"
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
	// dbsRef holds the slice of the databases. Every command reads it and
	// SWAPDB replaces it: the slice lives behind an atomic pointer, so the
	// commands see the old or the new slice as a whole and never a
	// half-swapped pair.
	dbsRef    atomic.Pointer[[]*store.Store]
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

	saveMu sync.Mutex
	aofMu  sync.Mutex
	// hllMu orders HLL mutations with their journal/replica propagation. A
	// snapshot captured under a shard lock is immutable, but without this
	// mutex write A could publish its snapshot before write B and then append
	// it after B, losing B on replay. It guards PFADD, PFMERGE and the
	// single-key PFCOUNT cache refresh; multi-key PFCOUNT is a pure read.
	hllMu    sync.Mutex
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

	// Password this server authenticates to its own master with (when it is
	// a replica).
	masterAuth atomic.Pointer[string]

	// Replication: the stream, the backlog, the replicas and the master
	// link. replStop stops the background ping; Close and Shutdown stop it
	// once through their shared shutdown.
	repl     *replState
	replStop chan struct{}

	// Server metadata for INFO/CONFIG (set at startup, read-only afterwards).
	appendFsync string
	saveSeconds int
	saveChanges int

	ln       net.Listener
	mu       sync.Mutex
	clients  map[net.Conn]*client
	started  time.Time
	connsNum int64

	// Lifecycle state. stopped is set under mu when Close (or Shutdown)
	// begins: from then on Serve does not accept and no connection is
	// registered. The stop is performed once; a second Close or Shutdown
	// waits for the first one and returns its result.
	stopped  bool
	stopOnce sync.Once
	stopErr  error

	// serveDone is non-nil once Serve has started and is closed when it
	// returns. Close uses it to wait for Serve after stopping the listener
	// and the loops. Serve may be called once (a second call returns an
	// error while the server is still running).
	serveDone chan struct{}
	// connWG tracks the connection handlers of the goroutine model: Close
	// waits for them to leave before it stops the persistence resources.
	connWG sync.WaitGroup

	// Background workers owned by the server (see persist.go and Close).
	saveWG     sync.WaitGroup
	replPingWG sync.WaitGroup
	// finalizeMu serializes the final RDB save and the close of the AOF.
	finalizeMu sync.Mutex

	// loopClients are the clients served by the event loop. The clients of
	// the goroutine model are in s.clients: they are keyed by the
	// connection, and a client of the event loop has no socket.
	loopClients map[*client]struct{}

	// loops are the states of the event loops: one loop serves the
	// connections by itself, several loops share the port between them.
	// They are set when the loops start and they are used to stop them
	// when the server is closed.
	loops []*loopState

	// loopMode is set when the server serves the connections with event
	// loops instead of a goroutine each. loopAddr is the address they
	// listen on.
	loopMode bool
	loopAddr string

	// loopFDs are the descriptors of the listeners of the event loops, one
	// per loop. They are created when the server is constructed: every
	// loop takes its own when it starts, and the ones that are left are
	// closed when the server is closed before the loops started.
	loopFDs []int
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

// propagate writes one normalized command to the journal (when it is on)
// and feeds it to the replication stream: the backlog and the connected
// replicas. db is the database the command ran in: the journal and the
// stream re-emit SELECT before it when their own database differs.
func (s *Server) propagate(args [][]byte, db int) {
	if s.AOF != nil {
		if err := s.AOF.AppendInDB(db, args); err != nil {
			s.Logger.Printf("server: aof append: %v", err)
		}
	}
	s.appendStream(args, db)
}

// propagate appends args to the AOF and the replication stream: the database
// of the command is the database of the connection.
func (c *Ctx) propagate(args ...[][]byte) {
	if c.Srv == nil {
		return
	}
	db := 0
	if c.Client != nil {
		db = c.Client.db
	}
	for _, a := range args {
		c.Srv.propagate(a, db)
	}
}

// propagateCmd writes the current command to the journal and the replication
// stream, rewriting relative TTLs to absolute expiry. Handlers call it only
// after a mutation took effect.
//
// The dispatcher writes the commands that do not propagate themselves (the
// ones whose AOF entry does not depend on whether they took effect), so a
// command that is written here must not be written there as well: a command
// written twice would be applied twice on the replay.
func (c *Ctx) propagateCmd() {
	if c.Srv == nil {
		return
	}
	if c.spec.write && !c.spec.aofSelf {
		return
	}
	db := 0
	if c.Client != nil {
		db = c.Client.db
	}
	for _, cmd := range persist.RewriteForAOF(c.Args, c.Store.Now()) {
		c.Srv.propagate(cmd, db)
	}
}

// propagateFromDispatch logs a write command to the journal and the
// replication stream after execution. The command must already be known to
// be a write command handled centrally.
func (s *Server) propagateFromDispatch(args [][]byte, db int) {
	for _, c := range persist.RewriteForAOF(args, s.dbsList()[0].Now()) {
		s.propagate(c, db)
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
		Logger:      log.Default(),
		cmds:        make(map[string]cmdSpec),
		cmdsUpper:   make(map[string]cmdSpec),
		clients:     make(map[net.Conn]*client),
		loopClients: make(map[*client]struct{}),
		PubSub:      NewPubSub(),
		ln:          ln,
		started:     time.Now(),
		repl:        newReplState(DefaultReplBacklogSize),
		replStop:    make(chan struct{}),
	}
	s.dbsRef.Store(&dbs)
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

// SetMasterAuth records the password this server authenticates to its own
// master with (REPLICAOF makes it a replica). Call before Serve.
func (s *Server) SetMasterAuth(pass string) { s.masterAuth.Store(&pass) }

// masterAuth returns the password for the link to the master ("" disables
// the authentication).
func (s *Server) masterAuthPass() string {
	if p := s.masterAuth.Load(); p != nil {
		return *p
	}
	return ""
}

// SetReplBacklogSize sets the capacity of the replication backlog (bytes).
// Call before Serve.
func (s *Server) SetReplBacklogSize(n int) {
	if n >= 1024 {
		s.repl.mu.Lock()
		s.repl.backlog = newReplBacklog(n)
		s.repl.mu.Unlock()
	}
}

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

// dbsList returns the databases the server serves: one store per keyspace.
func (s *Server) dbsList() []*store.Store { return *s.dbsRef.Load() }

// DB returns the store of the database with the given index. Out-of-range
// indexes fall back to the first database.
func (s *Server) DB(i int) *store.Store {
	dbs := s.dbsList()
	if i < 0 || i >= len(dbs) {
		return dbs[0]
	}
	return dbs[i]
}

// DBCount returns the number of databases.
func (s *Server) DBCount() int { return len(s.dbsList()) }

// SwapDBs swaps two databases. The slice of the databases is replaced
// atomically: the commands of the other connections serve from the old or
// the new slice as a whole, and never see a half-swapped pair.
func (s *Server) SwapDBs(a, b int) {
	dbs := s.dbsList()
	if a < 0 || a >= len(dbs) || b < 0 || b >= len(dbs) {
		return
	}
	cp := make([]*store.Store, len(dbs))
	copy(cp, dbs)
	cp[a], cp[b] = cp[b], cp[a]
	s.dbsRef.Store(&cp)
}

// usedBytes returns the approximate memory footprint of all databases.
func (s *Server) usedBytes() int {
	n := 0
	for _, db := range s.dbsList() {
		n += db.UsedBytes()
	}
	return n
}

// Close stops the server and closes all active connections. It is safe to
// call more than once and before Serve: the first call performs the shutdown,
// the later ones wait for it and return the same result. Closing the server
// does not save the dataset; Shutdown does the final RDB save before the
// persistence resources are released.
func (s *Server) Close() error {
	s.stopOnce.Do(func() { s.stopErr = s.stopServing() })
	s.finalizePersistence(false)
	return s.stopErr
}

// stopServing performs the shutdown in the ownership order:
//
//  1. stop accepting (the listener and the loop listeners),
//  2. wake the event loops and the clients blocked in a command,
//  3. wait for Serve and for the connection handlers to leave,
//  4. stop the background expiry, the save policy and the replication
//     (including the link to the master).
//
// The AOF and the final save are left to finalizePersistence, so Shutdown can
// save after every command has left. s.mu is never held while waiting for a
// goroutine that needs it for cleanup.
func (s *Server) stopServing() error {
	s.mu.Lock()
	s.stopped = true
	ln := s.ln
	loops := s.loops
	fds := s.loopFDs
	s.loopFDs = nil
	serveDone := s.serveDone
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()

	// Stop accepting first. A connection that Accept already returned is
	// rejected by the stopped check when Serve registers it.
	var err error
	if ln != nil {
		if e := ln.Close(); e != nil && !errors.Is(e, net.ErrClosed) {
			err = e
		}
	}
	// The loops close their own listeners and connections when they leave.
	for _, l := range loops {
		l.stop()
	}
	// A listener left before its loop could take it is unowned: close it.
	for _, fd := range fds {
		if fd > 0 {
			netloop.Close(fd)
		}
	}
	// The handlers of the goroutine model wait for the socket or for their
	// done channel: close the socket and publish the done state. The done
	// channel is idempotent, so the deferred cleanup of handleConn may close
	// it again.
	for _, c := range clients {
		c.closeDone()
		c.closeConn()
	}
	if serveDone != nil {
		<-serveDone
	}
	// The handlers need s.mu (and Pub/Sub, WATCH and replica cleanup) when
	// they leave, so they are drained before any resource they use is
	// released.
	s.connWG.Wait()
	s.stopBackground()
	return err
}

// closeLoopFDs closes the listeners the loops did not take yet: the server
// is closed before the loops started, so nobody owns the descriptors.
func (s *Server) closeLoopFDs() {
	s.mu.Lock()
	fds := s.loopFDs
	s.loopFDs = nil
	s.mu.Unlock()
	for _, fd := range fds {
		if fd > 0 {
			netloop.Close(fd)
		}
	}
}

// addLoopClient registers a client served by the event loop. It reports false
// when the shutdown has already begun: the connection was accepted in the
// race with Close and must not be registered.
func (s *Server) addLoopClient(c *client) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	s.loopClients[c] = struct{}{}
	return true
}

// removeLoopClient drops a client served by the event loop.
func (s *Server) removeLoopClient(c *client) {
	s.mu.Lock()
	delete(s.loopClients, c)
	s.mu.Unlock()
}

// allClients returns the clients of the server: the ones of the goroutine
// model and the ones of the event loop.
func (s *Server) allClients() []*client {
	s.mu.Lock()
	out := make([]*client, 0, len(s.clients)+len(s.loopClients))
	for _, c := range s.clients {
		out = append(out, c)
	}
	for c := range s.loopClients {
		out = append(out, c)
	}
	s.mu.Unlock()
	return out
}

// countConn counts the connections of the server.
func (s *Server) countConn(n int64) {
	atomic.AddInt64(&s.connsNum, n)
}

// NewLoop creates a Server that serves the connections with one event loop
// instead of a goroutine each. The commands are executed by the same code as
// the goroutine model; the difference is the I/O.
func NewLoop(addr string, dbs []*store.Store) (*Server, error) {
	return NewLoopN(addr, dbs, 1)
}

// NewLoopN creates a Server that serves the connections with n event loops
// instead of a goroutine each. Every loop holds a listening socket of its
// own: the sockets bind the same address with SO_REUSEPORT, and the kernel
// spreads the connections between them, so the loops do not fight for one
// accept queue. The commands are executed by the same code as the goroutine
// model; the difference is the I/O.
func NewLoopN(addr string, dbs []*store.Store, n int) (*Server, error) {
	if n < 1 {
		return nil, errors.New("the number of the event loops must be at least one")
	}
	s := newServer(dbs, nil)
	s.loopMode = true
	// The sockets bind the same address. When the port is not chosen yet,
	// the rest of the loops bind the port the first socket got: every bind
	// of port 0 would get a port of its own.
	bind := addr
	for i := 0; i < n; i++ {
		fd, err := netloop.ListenReusePort(bind)
		if err != nil {
			s.closeLoopFDs()
			return nil, err
		}
		if i == 0 {
			bound, err := netloop.Addr(fd)
			if err != nil {
				netloop.Close(fd)
				s.closeLoopFDs()
				return nil, err
			}
			s.loopAddr = bound
			bind = bound
		}
		s.mu.Lock()
		s.loopFDs = append(s.loopFDs, fd)
		s.mu.Unlock()
	}
	return s, nil
}

// Serve accepts connections until the listener is closed. Serve is meant to
// be called once; a second concurrent call returns an error. Calling it after
// Close returns nil at once, so a shutdown that raced the start is safe.
func (s *Server) Serve() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	if s.serveDone != nil {
		s.mu.Unlock()
		return errors.New("server: Serve called more than once")
	}
	done := make(chan struct{})
	s.serveDone = done
	s.replPingWG.Add(1)
	s.mu.Unlock()
	defer close(done)

	go func() {
		defer s.replPingWG.Done()
		s.replPingLoop()
	}()
	if s.loopMode {
		return s.serveLoops()
	}
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		c := newClient(conn)
		s.mu.Lock()
		if s.stopped {
			// Close began after Accept returned: the connection is not
			// registered and the client is not served.
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		s.clients[conn] = c
		s.connWG.Add(1)
		s.mu.Unlock()
		s.countConn(1)
		go s.handleConn(conn, c)
	}
}

func (s *Server) handleConn(conn net.Conn, c *client) {
	defer s.connWG.Done()
	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		s.countConn(-1)
		s.PubSub.removeClient(c)
		s.unregisterReplica(c)
		c.unwatchAll(s.DB(c.db))
		c.closeDone()
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

		ctx := &c.ctx
		var ing commandIngress
		ing.begin(s, c, ctx, args, w)

		// The Pub/Sub pump goroutine shares the connection's bufio with the
		// command loop, so command execution and the final Flush must both
		// happen under writeMu.
		c.writeMu.Lock()
		if ing.route() {
			if err := w.Flush(); err != nil {
				c.writeMu.Unlock()
				s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
				return
			}
			c.writeMu.Unlock()
			continue
		}
		ing.execute()
		if req := ing.waiting(); req != nil {
			// A blocking command found nothing to pop: release the write
			// lock (the Pub/Sub pump needs it) and wait for a key to become
			// available, then retry the command.
			c.writeMu.Unlock()
			woken, werr := s.waitForKeys(c, req)
			c.writeMu.Lock()
			if werr != nil {
				// The client was unblocked with CLIENT UNBLOCK: the
				// answer is the error, not the result of the command.
				if err := ctx.W.WriteError(werr.Error()); err != nil {
					c.writeMu.Unlock()
					s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
					return
				}
				c.writeMu.Unlock()
				continue
			}
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
				woken, werr = s.waitForKeys(c, req)
				c.writeMu.Lock()
				if werr != nil {
					if err := ctx.W.WriteError(werr.Error()); err != nil {
						c.writeMu.Unlock()
						s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
						return
					}
					break
				}
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
			// The command has finished: a write may have made a key
			// available for another blocked client. The signal comes
			// after the command: a command that only started waiting has
			// changed nothing.
			ing.signalWaiters()
			c.writeMu.Unlock()
			continue
		}
		switch ing.finish() {
		case endSuppressed:
			c.writeMu.Unlock()
			continue
		case endQuit:
			w.Flush()
			c.writeMu.Unlock()
			return
		}
		c.writeMu.Unlock()
	}
}
