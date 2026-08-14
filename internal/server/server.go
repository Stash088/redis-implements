package server

import (
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
	Store *store.Store
	Args  [][]byte
	W     *resp.Writer
	Srv   *Server
}

// Handler implements a single Redis command.
type Handler func(*Ctx) error

// Server is a TCP server that speaks the RESP protocol against a store.
type Server struct {
	Store   *store.Store
	Expirer *store.Expirer
	AOF     *persist.Appender
	Logger  *log.Logger
	cmds    map[string]Handler

	// Persistence paths (empty disables the related feature).
	DBPath  string
	AOFPath string

	saveMu   sync.Mutex
	aofMu    sync.Mutex
	dirty    atomic.Int64
	lastSave atomic.Int64 // unix seconds of the last successful save
	saveStop chan struct{}

	ln       net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
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

// aofWriteCommands are commands that mutate state and therefore belong in the
// AOF. Commands handled inside their handler (relative TTLs) are excluded here.
var aofWriteCommands = map[string]bool{
	"append":  true,
	"decr":    true,
	"decrby":  true,
	"del":     true,
	"expire":  true,
	"expireat": true,
	"flushdb": true,
	"hdel":    true,
	"hset":    true,
	"incr":    true,
	"incrby":  true,
	"lpop":    true,
	"lpush":   true,
	"lset":    true,
	"persist": true,
	"pexpire": true,
	"pexpireat": true,
	"rpop":    true,
	"rpush":   true,
	"sadd":    true,
	"set":     true,
	"srem":    true,
	"zadd":    true,
	"zrem":    true,
}

// aofHandlerCommands propagate from within their handlers because the rewrite
// (relative -> absolute TTL) depends on whether the command took effect.
var aofHandlerCommands = map[string]bool{
	"set":     true,
	"expire":  true,
	"pexpire": true,
}

// propagateFromDispatch logs a write command to the AOF after execution. The
// command must already be known to be a write command handled centrally.
func (s *Server) propagateFromDispatch(args [][]byte) {
	if s.AOF == nil {
		return
	}
	for _, c := range persist.RewriteForAOF(args, s.Store.Now()) {
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

// New creates a Server bound to the given address. The listener is not
// opened until Listen is called.
func New(addr string, st *store.Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{
		Store:   st,
		Logger:  log.Default(),
		cmds:    make(map[string]Handler),
		conns:   make(map[net.Conn]struct{}),
		ln:      ln,
		started: time.Now(),
	}
	s.registerCommands()
	return s, nil
}

// Addr returns the bound listener address (useful with port 0).
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

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
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		atomic.AddInt64(&s.connsNum, -1)
		conn.Close()
		s.Logger.Printf("server: client disconnected: %s", conn.RemoteAddr())
	}()

	s.Logger.Printf("server: client connected: %s", conn.RemoteAddr())
	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)

	for {
		v, err := r.ReadValue()
		if err != nil {
			if err != io.EOF {
				s.Logger.Printf("server: read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if v.Kind != resp.KindArray || v.IsNull {
			w.WriteError("ERR Protocol error: expected command array")
			w.Flush()
			return
		}
		args := make([][]byte, 0, len(v.Array))
		for i := range v.Array {
			el := v.Array[i]
			if el.Kind != resp.KindBulkString || el.IsNull {
				w.WriteError("ERR Protocol error: command arguments must be bulk strings")
				w.Flush()
				return
			}
			args = append(args, el.Str)
		}
		if len(args) == 0 {
			w.WriteError("ERR empty command")
			w.Flush()
			continue
		}

		atomic.AddUint64(&s.received, 1)
		ctx := &Ctx{Store: s.Store, Args: args, W: w, Srv: s}
		if err := s.dispatch(ctx); err == errQuit {
			w.Flush()
			return
		}
		name := strings.ToLower(string(args[0]))
		if aofWriteCommands[name] && !aofHandlerCommands[name] {
			s.propagateFromDispatch(args)
		}
		if aofWriteCommands[name] {
			s.dirty.Add(1)
		}
		if err := w.Flush(); err != nil {
			s.Logger.Printf("server: write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (s *Server) dispatch(ctx *Ctx) error {
	name := strings.ToLower(string(ctx.Args[0]))
	f, ok := s.cmds[name]
	if !ok {
		return writeUnknownCommand(ctx)
	}
	return f(ctx)
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

// register adds a command to the registry.
func (s *Server) register(name string, f Handler) {
	s.cmds[strings.ToLower(name)] = f
}
