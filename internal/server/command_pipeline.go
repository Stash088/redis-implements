package server

import (
	"bytes"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

// The command pipeline is the transport-independent front half of the
// handling of one command. Both the goroutine model (Server.handleConn) and
// the event loop (loopState.loopCommand) run every command through it:
//
//   - begin counts the command and fills the reused Ctx, resolving the
//     command name and spec once;
//   - route records the command as the client's last command, then either
//     queues it in MULTI or sets up the CLIENT REPLY OFF/SKIP shadow writer;
//   - execute runs the command through Server.executeCommand and records the
//     slow log;
//   - finish signals the clients blocked on the written key and applies the
//     CLIENT REPLY state transition.
//
// The transports keep their own I/O and scheduling: the goroutine model owns
// writeMu, the socket writer and the wait/retry of a blocked client; the
// event loop owns its poller, writer buffer, flush bookkeeping and the
// block/unblock queues. The ingress borrows the connection state for exactly
// one command and does not own the connection lifecycle.
//
// A retried blocking command follows the transport's pre-existing path: the
// goroutine model retries with Server.executeCommand (so the ingress and the
// received/slowlog bookkeeping see the command once), while the event loop
// runs loopCommand again when the client is woken (so it counts and slow-logs
// the retry again, as before). That difference predates the ingress and is
// preserved, as is the reply suppression behavior of the two timeout paths.
// The ingress does not handle the EXEC replay of a transaction queue: EXEC
// calls Server.executeCommand directly.

// commandIngress carries the transport-independent state of one command.
type commandIngress struct {
	ctx *Ctx

	// replyMode is the CLIENT REPLY mode the command started in. When the
	// answer must not reach the client, the command writes into shadow,
	// which accumulates the discarded reply in shadowBuf. realW is the
	// writer of the connection.
	replyMode int
	realW     *resp.Writer
	shadow    *resp.Writer
	shadowBuf *bytes.Buffer

	err error
}

// commandEnd is the result of commandIngress.finish.
type commandEnd int

const (
	endContinue commandEnd = iota
	endSuppressed
	endQuit
)

// begin counts args, fills the reused ctx and resolves the command. It runs
// before the transport enters the critical section that orders its writes:
// begin does not touch the state of the client.
func (in *commandIngress) begin(s *Server, c *client, ctx *Ctx, args [][]byte, w *resp.Writer) {
	s.received[c.slot].Add(1)
	*ctx = Ctx{Store: s.DB(c.db), Args: args, W: w, Srv: s, Client: c}

	// The command is resolved once per command: the lookup works on the
	// bytes the client sent and does not allocate.
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
	*in = commandIngress{ctx: ctx}
}

// route records the command as the last command of the client and chooses
// between the MULTI queue and execution. The caller must hold the lock that
// orders writes to the client (writeMu in the goroutine model); the event loop
// is single-threaded and holds no lock. It reports whether the command was
// queued: a queued command writes its answer through ctx.W, and the caller
// flushes it with the rest of the connection's replies.
func (in *commandIngress) route() bool {
	c := in.ctx.Client
	c.setLastCmd(in.ctx.cmdNamePtr)
	if c.inMulti && !txnImmediate[in.ctx.cmdName] {
		in.ctx.Srv.queueForMulti(in.ctx, in.ctx.cmdName)
		return true
	}
	// CLIENT REPLY OFF/SKIP suppresses the answer: the command runs
	// against a throwaway writer, so nothing reaches the client. A command
	// that turns the replies back on (CLIENT REPLY ON) still gets its
	// answer, like in Redis.
	in.replyMode = c.replyMode
	in.realW = in.ctx.W
	if in.replyMode != replyOn {
		in.shadowBuf = &bytes.Buffer{}
		// A small buffer: the shadow writer holds one discarded reply.
		in.shadow = resp.NewWriterSize(in.shadowBuf, 4096)
		in.ctx.W = in.shadow
	}
	return false
}

// execute runs the command and records its slow-log entry. A command that
// starts waiting leaves its request in ctx.wait: the transport handles the
// wait, because only it knows how to park its connection.
func (in *commandIngress) execute() {
	started := time.Now()
	in.err = in.ctx.Srv.executeCommand(in.ctx)
	in.ctx.Srv.sl.maybeAdd(in.ctx.Args, in.ctx.Client, time.Since(started))
}

// waiting returns the wait request of a blocking command that found nothing
// to pop, after clearing it from the context. The transport retries the
// command with Server.executeCommand when the wait is over.
func (in *commandIngress) waiting() *waitRequest {
	req := in.ctx.wait
	if req != nil {
		in.ctx.wait = nil
	}
	return req
}

// suppressed reports whether the command writes into the shadow writer.
func (in *commandIngress) suppressed() bool { return in.shadow != nil }

// flushShadow discards the answer accumulated by a suppressed command. The
// event loop calls it when it parks a blocked command; the goroutine model
// keeps the shadow writer until the command finishes.
func (in *commandIngress) flushShadow() {
	if in.shadow != nil {
		in.shadow.Flush()
	}
}

// signalWaiters notifies the clients blocked on the key the command wrote: a
// write may have made a key available. It runs only after the command
// completed: a command that only started waiting has changed nothing.
func (in *commandIngress) signalWaiters() {
	if in.ctx.spec.write && len(in.ctx.Args) > 1 {
		in.ctx.Srv.waiters.signal(in.ctx.Args[1])
	}
}

// finish completes a command that did not wait: it signals the blocked
// clients and applies the CLIENT REPLY OFF/SKIP transition. The transport
// flushes its writer afterwards, as its I/O model requires.
func (in *commandIngress) finish() commandEnd {
	in.signalWaiters()
	if in.shadow != nil {
		in.shadow.Flush()
		if in.replyMode == replySkip {
			in.ctx.Client.replyMode = replyOn
		}
		if in.replyMode == replyOff && in.ctx.Client.replyMode == replyOn {
			in.realW.WriteRaw(in.shadowBuf.Bytes())
		}
		return endSuppressed
	}
	if in.err == errQuit {
		return endQuit
	}
	return endContinue
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
	if spec.write {
		db := 0
		if ctx.Client != nil {
			db = ctx.Client.db
		}
		if !spec.aofSelf {
			s.propagateFromDispatch(ctx.Args, db)
		}
		s.dirty[ctx.counterSlot()].Add(1)
		// WAIT measures the replicas against the position of the stream
		// after the last write of this client.
		if ctx.Client != nil {
			ctx.Client.woff.Store(s.repl.offset.Load())
		}
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
	// A replica rejects the writes of its clients: they would diverge from
	// the master and be discarded by the next synchronization.
	if spec.write && s.isReplica() {
		return ctx.W.WriteError("READONLY You can't write against a read only replica.")
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
