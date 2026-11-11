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
	s := ctx.Srv
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
			s.wakeAckWaiters()
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
// (its replication ID and offset) or to start a fresh one ("?" and -1). A
// history the server still holds in the backlog continues from the offset;
// anything else starts a full resynchronization.
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

	// The partial resync: the replication ID matches this history and the
	// bytes from the offset are still in the backlog. The backlog bytes are
	// copied under the stream mutex: the appends reuse the buffer.
	if id != "?" {
		if replay, ok := s.tryPartial(id, off); ok {
			rep := &replica{c: ctx.Client, port: ctx.Client.replPort}
			rep.ackOff.Store(off)
			rep.ackAt.Store(time.Now().Unix())
			s.registerReplicaPartial(ctx.Client, rep)
			ctx.Client.repl = rep
			if announce {
				ctx.W.WriteRaw([]byte("+CONTINUE " + s.repl.id + "\r\n"))
			}
			// The stream continues with the bytes the replica missed: the
			// frames of the commands that propagate from now on are queued
			// behind them.
			ctx.W.WriteRaw(replay)
			ctx.Client.startPump(ctx.W)
			s.Logger.Printf("replica: partial resync accepted for %s from offset %d (%d bytes of backlog)",
				ctx.Client.addr, off, len(replay))
			return nil
		}
		if id != "?" {
			s.Logger.Printf("replica: partial resync refused for %s (id %s, offset %d)",
				ctx.Client.addr, id, off)
		}
	}

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

// cmdWait implements WAIT: the client waits until numreplicas replicas
// processed its last write (the position of the stream it left the client
// at), or until the timeout in milliseconds expires. A timeout of 0 waits
// forever. The reply is the number of replicas that caught up, also on a
// timeout, like in Redis.
func cmdWait(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "wait")
		return nil
	}
	if ctx.Srv.isReplica() {
		return ctx.W.WriteError("WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.")
	}
	num, err := strconv.ParseInt(string(ctx.Args[1]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	timeoutMs, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil || timeoutMs < 0 {
		return ctx.W.WriteError("ERR timeout is not an integer or out of range")
	}
	s := ctx.Srv
	woff := ctx.Client.woff.Load()
	if n := int64(s.countAcks(woff)); n >= num {
		return ctx.W.WriteInteger(n)
	}
	// The replicas report their offsets right away instead of waiting for
	// the heartbeat.
	s.requestAcks()

	var deadline time.Time
	if timeoutMs > 0 {
		deadline = time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	}
	if ctx.Client.loop != nil {
		// The event loop parks the connection and retries the command when
		// the replicas caught up or the deadline expired (like the blocking
		// commands do).
		ctx.wait = &waitRequest{
			replicasWait: true,
			woff:         woff,
			num:          num,
			timeout:      time.Until(deadline),
		}
		return nil
	}
	// The goroutine model: the connection waits in its own goroutine.
	t := time.NewTicker(200 * time.Microsecond)
	defer t.Stop()
	for {
		if n := int64(s.countAcks(woff)); n >= num {
			return ctx.W.WriteInteger(n)
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return ctx.W.WriteInteger(int64(s.countAcks(woff)))
		}
		select {
		case <-ctx.Client.done:
			return ctx.W.WriteInteger(int64(s.countAcks(woff)))
		case <-t.C:
		}
	}
}

// cmdWaitAof implements WAITAOF: it waits until the local journal (numlocal)
// and numreplicas replicas are durable through the client's last write.
//
// The local journal is flushed and fsynced right here, so numlocal is
// satisfied synchronously. The replicas are counted by the offset they
// processed (REPLCONF ACK): the fsync state of their journals is not
// tracked yet, so "durable on the replica" means "applied by the replica"
// — a documented approximation until the replicas report their own journal
// offsets (REPLCONF ACK FACK).
func cmdWaitAof(ctx *Ctx) error {
	if len(ctx.Args) != 4 {
		wrongArgs(ctx.W, "waitaof")
		return nil
	}
	if ctx.Srv.isReplica() {
		return ctx.W.WriteError("WAITAOF cannot be used with replica instances. Please also note that writes to replicas are just local and are not propagated.")
	}
	numLocal, err := strconv.Atoi(string(ctx.Args[1]))
	if err != nil || numLocal < 0 || numLocal > 1 {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	num, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil || num < 0 {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	timeoutMs, err := strconv.ParseInt(string(ctx.Args[3]), 10, 64)
	if err != nil || timeoutMs < 0 {
		return ctx.W.WriteError("ERR timeout is not an integer or out of range")
	}
	s := ctx.Srv
	if numLocal > 0 && s.AOF == nil {
		return ctx.W.WriteError("WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
	}
	woff := ctx.Client.woff.Load()
	// The local part is satisfied here: the commands are already in the
	// buffer of the journal (it is written out before the replies leave),
	// the fsync makes them durable.
	ackLocal := int64(numLocal)
	if numLocal > 0 {
		if err := s.AOF.Sync(); err != nil {
			return ctx.W.WriteError("ERR " + err.Error())
		}
	}
	askReplicas := num > 0
	if !askReplicas {
		return ctx.W.WriteValue(waitAofReply(ackLocal, 0))
	}
	if n := int64(s.countAcks(woff)); n >= num {
		return ctx.W.WriteValue(waitAofReply(ackLocal, n))
	}
	s.requestAcks()

	var deadline time.Time
	if timeoutMs > 0 {
		deadline = time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	}
	if ctx.Client.loop != nil {
		// Park until the replicas catch up or the deadline expires. The
		// parked retry runs WAITAOF again; the local fsync above is
		// idempotent and the reply keeps the local count it computed.
		ctx.wait = &waitRequest{
			replicasWait: true,
			woff:         woff,
			num:          num,
			timeout:      time.Until(deadline),
			waitAof:      true,
			ackLocal:     ackLocal,
		}
		return nil
	}
	t := time.NewTicker(200 * time.Microsecond)
	defer t.Stop()
	for {
		if n := int64(s.countAcks(woff)); n >= num {
			return ctx.W.WriteValue(waitAofReply(ackLocal, n))
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return ctx.W.WriteValue(waitAofReply(ackLocal, int64(s.countAcks(woff))))
		}
		select {
		case <-ctx.Client.done:
			return ctx.W.WriteValue(waitAofReply(ackLocal, int64(s.countAcks(woff))))
		case <-t.C:
		}
	}
}

// waitAofReply builds the [local, replicas] reply of WAITAOF.
func waitAofReply(local, replicas int64) resp.Value {
	return resp.NewArray([]resp.Value{resp.NewInteger(local), resp.NewInteger(replicas)})
}
