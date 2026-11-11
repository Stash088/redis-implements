package server

import (
	"strconv"
)

// cmdSelect switches the connection to another database. The choice is
// per-connection, like Redis does. The journal and the replication stream
// track their own database: the next command of this connection is preceded
// by a SELECT when its database differs from the one the streams are in.
func cmdSelect(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "select")
		return nil
	}
	n, err := strconv.Atoi(string(ctx.Args[1]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	if n < 0 || n >= ctx.Srv.DBCount() {
		return ctx.W.WriteError("ERR DB index is out of range")
	}
	ctx.Client.db = n
	return ctx.W.WriteSimpleString("OK")
}

// cmdFlushAll removes the keys of every database.
func cmdFlushAll(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "flushall")
		return nil
	}
	for _, db := range ctx.Srv.dbsList() {
		db.Flush()
	}
	return ctx.W.WriteSimpleString("OK")
}

// evictCycle evicts keys until the total footprint of all databases fits
// into maxBytes. Every database gets a share of the budget proportional to its
// size, so the eviction is spread over the keyspaces.
func (s *Server) evictCycle(maxBytes int64) int {
	policy := s.maxMemoryPolicyName()
	samples := int(s.maxMemorySamples.Load())
	evicted := 0
	for _, db := range s.dbsList() {
		total := s.usedBytes()
		if int64(total) <= maxBytes {
			break
		}
		// This database's share of the budget.
		target := int64(0)
		if total > 0 {
			target = int64(float64(maxBytes) * float64(db.UsedBytes()) / float64(total))
		}
		if n := db.EvictCycle(policy, samples, target); n > 0 {
			evicted += n
		}
	}
	return evicted
}
