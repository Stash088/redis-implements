package server

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
)

// txnImmediate lists commands that execute right away even inside MULTI
// (instead of being queued).
var txnImmediate = map[string]bool{
	"multi":   true,
	"exec":    true,
	"discard": true,
	"watch":   true,
	"unwatch": true,
	"quit":    true,
}

// noMultiCommands lists commands Redis forbids inside a transaction because
// they change the connection state rather than the dataset.
var noMultiCommands = map[string]bool{
	"subscribe":    true,
	"unsubscribe":  true,
	"psubscribe":   true,
	"punsubscribe": true,
}

// queueForMulti appends the command to the client's transaction queue and
// replies +QUEUED, or writes the error and marks the transaction aborted.
// It mirrors Redis's queue-time validation: only unknown commands, wrong
// arity and connection-state commands are rejected here; runtime errors
// surface in the EXEC reply array.
func (s *Server) queueForMulti(ctx *Ctx, name string) {
	c := ctx.Client
	if noMultiCommands[name] {
		ctx.W.WriteError(fmt.Sprintf("ERR %s is not allowed in transactions", strings.ToUpper(name)))
		c.abort = true
		return
	}
	if spec, ok := s.cmds[name]; !ok {
		writeUnknownCommand(ctx)
		c.abort = true
		return
	} else if n := spec.arity; (n > 0 && len(ctx.Args) != n) || (n < 0 && len(ctx.Args) < -n) {
		wrongArgs(ctx.W, name)
		c.abort = true
		return
	}
	c.queue = append(c.queue, ctx.Args)
	ctx.W.WriteSimpleString("QUEUED")
}

func cmdMulti(ctx *Ctx) error {
	c := ctx.Client
	if c.inMulti {
		return ctx.W.WriteError("ERR MULTI calls can not be nested")
	}
	c.beginMulti()
	return ctx.W.WriteSimpleString("OK")
}

func cmdExec(ctx *Ctx) error {
	c := ctx.Client
	if !c.inMulti {
		return ctx.W.WriteError("ERR EXEC without MULTI")
	}
	queue, abort, dirty := c.endMulti(ctx.Store)

	if abort {
		return ctx.W.WriteError("EXECABORT Transaction discarded because of previous errors")
	}
	if dirty {
		// A watched key was modified between WATCH and EXEC: abort.
		return ctx.W.WriteNullArray()
	}

	// EXEC replies with an array holding one reply per queued command. Each
	// command runs through the normal execution path (so AOF propagation and
	// eviction happen) into a capture buffer, which is spliced into the array.
	if err := ctx.W.WriteArrayHeader(len(queue)); err != nil {
		return err
	}
	quit := false
	for _, args := range queue {
		var buf bytes.Buffer
		ew := resp.NewWriter(&buf)
		// The store is resolved per command: a queued SELECT switches the
		// database for the commands that follow it.
		ec := &Ctx{Store: ctx.Srv.DB(c.db), Args: args, W: ew, Srv: ctx.Srv, Client: c}
		err := ctx.Srv.executeCommand(ec)
		if ferr := ew.Flush(); ferr == nil {
			ctx.W.WriteRaw(buf.Bytes())
		}
		if err == errQuit {
			quit = true
		}
	}
	if quit {
		return errQuit
	}
	return nil
}

func cmdDiscard(ctx *Ctx) error {
	c := ctx.Client
	if !c.inMulti {
		return ctx.W.WriteError("ERR DISCARD without MULTI")
	}
	c.endMulti(ctx.Store)
	return ctx.W.WriteSimpleString("OK")
}

func cmdWatch(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "watch")
		return nil
	}
	c := ctx.Client
	if c.inMulti {
		return ctx.W.WriteError("ERR WATCH inside MULTI is not allowed")
	}
	// Watching establishes a fresh check point: previous CAS dirtiness no
	// longer applies (this is what makes retry loops work).
	c.dirtyCAS.Store(false)
	for _, k := range ctx.Args[1:] {
		c.addWatchedKey(string(k))
		ctx.Store.AddWatch(k, c)
		// Like Redis watchKey: watching an already-expired key counts as
		// modified and aborts a pending EXEC.
		if ctx.Store.IsExpired(k) {
			c.MarkDirty()
		}
	}
	return ctx.W.WriteSimpleString("OK")
}

func cmdUnwatch(ctx *Ctx) error {
	ctx.Client.unwatchAll(ctx.Store)
	return ctx.W.WriteSimpleString("OK")
}
