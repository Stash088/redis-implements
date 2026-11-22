package server

import (
	"strconv"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// Handlers of the HyperLogLog commands. The binary representation and the
// estimator live in internal/store/hyperloglog.go; these handlers only map the
// store errors to the replies of Redis 8.2.

// writeHLLError maps the errors of the HyperLogLog operations to the replies
// the reference implementation sends.
func writeHLLError(ctx *Ctx, err error) error {
	switch err {
	case store.ErrWrongType:
		return writeWrongType(ctx.W)
	case store.ErrHLLInvalid:
		return ctx.W.WriteError("WRONGTYPE Key is not a valid HyperLogLog string value.")
	case store.ErrHLLCorrupt:
		return ctx.W.WriteError("INVALIDOBJ Corrupted HLL object detected")
	default:
		return ctx.W.WriteError("ERR " + err.Error())
	}
}

// propagateHLLSet journals the resulting HLL string as a self-contained SET.
// An expiring key carries its original absolute expiry, so a replay that runs
// after the deadline does not recreate the key. This is the form used by
// PFMERGE and by PFADD when it updates an expiring value; persistent PFADD
// keeps its compact raw command.
func (c *Ctx) propagateHLLSet(key, value []byte, expiresAt time.Time) {
	args := [][]byte{[]byte("set"), key, value}
	if !expiresAt.IsZero() {
		args = append(args, []byte("PXAT"), []byte(strconv.FormatInt(expiresAt.UnixMilli(), 10)))
	}
	c.propagate(args)
}

// noteHLLReadMutation accounts for the effective mutation of a read command
// (the PFCOUNT cache refresh) in the dirty counter and in the replication
// position used by WAIT/WAITAOF. It runs after propagation, so the position
// includes the command that was just written.
func (c *Ctx) noteHLLReadMutation() {
	if c.Srv == nil {
		return
	}
	c.Srv.dirty[c.counterSlot()].Add(1)
	if c.Client != nil {
		c.Client.woff.Store(c.Srv.repl.offset.Load())
	}
}

// lockHLL serializes the HLL mutations with their outbound propagation. The
// store publishes the immutable value under its shard lock, but the order of
// the journal/replica snapshots must match the order of the mutations.
func (c *Ctx) lockHLL() func() {
	if c.Srv == nil {
		return func() {}
	}
	c.Srv.hllMu.Lock()
	return c.Srv.hllMu.Unlock
}

// cmdPFAdd implements PFADD key [element ...].
func cmdPFAdd(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "pfadd")
		return nil
	}
	unlock := ctx.lockHLL()

	m, err := ctx.Store.PFAddValue(ctx.Args[1], ctx.Args[2:]...)
	if err != nil {
		unlock()
		return writeHLLError(ctx, err)
	}
	if m.Changed {
		if m.ExpiresAt.IsZero() {
			// A persistent key keeps the compact command: replaying PFADD
			// has the same effect as replaying the resulting value.
			ctx.propagateCmd()
		} else {
			ctx.propagateHLLSet(ctx.Args[1], m.Value, m.ExpiresAt)
		}
		unlock()
		return ctx.W.WriteInteger(1)
	}
	unlock()
	return ctx.W.WriteInteger(0)
}

// cmdPFCount implements PFCOUNT key [key ...].
func cmdPFCount(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "pfcount")
		return nil
	}
	if len(ctx.Args) == 2 {
		unlock := ctx.lockHLL()

		card, updated, _, err := ctx.Store.PFCountKey(ctx.Args[1])
		if err != nil {
			unlock()
			return writeHLLError(ctx, err)
		}
		if updated {
			// The cached cardinality is part of the string value: Redis
			// signals the key as modified and propagates the command so the
			// cache update reaches replicas and the AOF.
			ctx.propagateCmd()
			ctx.noteHLLReadMutation()
		}
		unlock()
		return ctx.W.WriteInteger(int64(card))
	}
	card, err := ctx.Store.PFCount(ctx.Args[1:]...)
	if err != nil {
		return writeHLLError(ctx, err)
	}
	return ctx.W.WriteInteger(int64(card))
}

// cmdPFMerge implements PFMERGE destkey [sourcekey ...].
func cmdPFMerge(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "pfmerge")
		return nil
	}
	unlock := ctx.lockHLL()

	m, err := ctx.Store.PFMergeValue(ctx.Args[1], ctx.Args[2:]...)
	if err != nil {
		unlock()
		return writeHLLError(ctx, err)
	}
	// Journal the computed destination, not the inputs: a source may expire
	// before the replay reads it. The command propagates itself, so a failed
	// PFMERGE (above) writes nothing.
	ctx.propagateHLLSet(ctx.Args[1], m.Value, m.ExpiresAt)
	unlock()
	return ctx.W.WriteSimpleString("OK")
}
