package server

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
)

// cmdReplicaOf implements REPLICAOF/SLAVEOF: "host port" turns the server
// into a replica of the master, "NO ONE" promotes it back. The dataset is
// replaced when the synchronization with the new master completes, like in
// Redis.
func cmdReplicaOf(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "replicaof")
		return nil
	}
	s := ctx.Srv
	if strings.EqualFold(string(ctx.Args[1]), "no") && strings.EqualFold(string(ctx.Args[2]), "one") {
		s.clearReplicaOf()
		return ctx.W.WriteSimpleString("OK")
	}
	host := string(ctx.Args[1])
	port, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil || port < 1 || port > 65535 {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	s.setReplicaOf(host, port, s.masterAuthPass())
	return ctx.W.WriteSimpleString("OK")
}

// cmdReplConf implements REPLCONF: the options of the handshake between a
// replica and its master. ACK replies with nothing at all, like in Redis.
func cmdReplConf(ctx *Ctx) error {
	if len(ctx.Args)%2 == 0 {
		return ctx.W.WriteError("ERR syntax error")
	}
	for i := 1; i < len(ctx.Args); i += 2 {
		opt := string(ctx.Args[i])
		value := string(ctx.Args[i+1])
		switch strings.ToLower(opt) {
		case "listening-port":
			port, err := strconv.Atoi(value)
			if err != nil || port < 0 || port > 65535 {
				return ctx.W.WriteError("ERR Invalid listening port")
			}
			// The port is announced before the replica runs PSYNC: it goes
			// to the client and moves into the replica entry at the
			// registration.
			ctx.Client.replPort = port
		case "ip-address":
			// Accepted for the handshake of real replicas; the address of
			// the connection itself is used instead.
		case "capa":
			// The capabilities are accepted but none of them changes the
			// transfer: the RDB always goes out with a length prefix.
		case "ack":
			// The replica reports the offset it processed. No reply.
			if rep := ctx.Client.repl; rep != nil {
				if off, err := strconv.ParseInt(value, 10, 64); err == nil && off > rep.ackOff.Load() {
					rep.ackOff.Store(off)
				}
				rep.ackAt.Store(time.Now().Unix())
			}
			return nil
		case "getack":
			// A replica of ours forwards the request of its master. We have
			// no master to report to. No reply.
			return nil
		default:
			return ctx.W.WriteError("ERR Unrecognized REPLCONF option: " + opt)
		}
	}
	return ctx.W.WriteSimpleString("OK")
}

// cmdPSync implements PSYNC: the replica asks to continue a stream it knows
// (its replication ID and offset) or to start a fresh one ("?" and -1).
func cmdPSync(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "psync")
		return nil
	}
	id := string(ctx.Args[1])
	off, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	return ctx.Srv.startReplicaStream(ctx, id, off, true)
}

// cmdSync is the pre-PSYNC handshake: the master replies with the RDB only,
// without the FULLRESYNC line.
func cmdSync(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "sync")
		return nil
	}
	return ctx.Srv.startReplicaStream(ctx, "?", -1, false)
}

// startReplicaStream turns the client into a replica of the server: it
// receives the full dataset (an RDB snapshot) and follows the stream from
// then on.
//
// The reply and the payload are written raw: they are not replies in the
// usual sense, and nothing else may be written to this connection until the
// replica follows the stream.
func (s *Server) startReplicaStream(ctx *Ctx, id string, off int64, announce bool) error {
	// A replica does not serve replicas: the commands it would proxy are
	// the ones it rejects from its own clients.
	if s.isReplica() {
		return ctx.W.WriteError("NOMASTERLINK Can't PSYNC while the server replicates from a master")
	}
	_ = id
	_ = off

	// The full resync: the dataset is serialized under the stream mutex (see
	// emitLocked), so the snapshot and the cut offset are atomic with
	// respect to the propagated commands: a command that mutates the store
	// during the snapshot either made it into it or is appended (and
	// delivered to this replica) after the registration.
	var rdb bytes.Buffer
	r := s.repl
	r.mu.Lock()
	err := persist.WriteRDB(&rdb, s.dbsList())
	cut := int64(0)
	rep := &replica{c: ctx.Client, port: ctx.Client.replPort}
	if err == nil {
		rep.ackAt.Store(time.Now().Unix())
		// The snapshot of the fresh replica does not know which database the
		// stream was in: the next command re-emits SELECT.
		r.db = -1
		r.reps[ctx.Client] = rep
		ctx.Client.repl = rep
		cut = r.offset.Load()
	}
	r.mu.Unlock()
	if err != nil {
		return ctx.W.WriteError("ERR " + err.Error())
	}

	if announce {
		ctx.W.WriteRaw([]byte("+FULLRESYNC " + s.repl.id + " " + strconv.FormatInt(cut, 10) + "\r\n"))
	}
	// The RDB goes out as "$<len>\r\n" and exactly len bytes: no trailing
	// CRLF, the command stream continues right after the payload.
	ctx.W.WriteRaw([]byte("$" + strconv.Itoa(rdb.Len()) + "\r\n"))
	ctx.W.WriteRaw(rdb.Bytes())
	// The frames of the commands that propagate from now on are queued for
	// the connection: the pump (or the loop) writes them after the RDB.
	ctx.Client.startPump(ctx.W)
	s.Logger.Printf("replica: full resync for %s at offset %d (%d bytes of RDB)", ctx.Client.addr, cut, rdb.Len())
	return nil
}

// cmdRole implements ROLE: the role of the server and, for a master, its
// replicas; for a replica, its master and the state of the link.
func cmdRole(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "role")
		return nil
	}
	s := ctx.Srv
	if host, port, st, _, ok := s.masterInfo(); ok {
		// ["slave", host, port, state, offset]
		off := s.repl.offset.Load()
		if st != linkUp {
			off = -1
		}
		elems := []resp.Value{
			resp.NewBulkString([]byte("slave")),
			resp.NewBulkString([]byte(host)),
			resp.NewInteger(int64(port)),
			resp.NewBulkString([]byte(st.roleState())),
			resp.NewInteger(off),
		}
		return ctx.W.WriteValue(resp.NewArray(elems))
	}
	// ["master", offset, [[addr, port, offset], ...]]
	reps := s.replicaAddrs()
	if err := ctx.W.WriteArrayHeader(3); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte("master")); err != nil {
		return err
	}
	if err := ctx.W.WriteInteger(s.repl.offset.Load()); err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(len(reps)); err != nil {
		return err
	}
	for _, rep := range reps {
		if err := ctx.W.WriteArrayHeader(3); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte(rep.host)); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(int64(rep.port)); err != nil {
			return err
		}
		if err := ctx.W.WriteInteger(rep.offset); err != nil {
			return err
		}
	}
	return nil
}
