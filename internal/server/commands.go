package server

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

func (s *Server) registerCommands() {
	// Read-only commands.
	s.reg("PING", -1, cmdPing)
	s.reg("ECHO", 2, cmdEcho)
	s.reg("TYPE", 2, cmdType)
	s.reg("GET", 2, cmdGet)
	s.reg("EXISTS", -2, cmdExists)
	s.reg("STRLEN", 2, cmdStrlen)
	s.reg("TTL", 2, cmdTTL)
	s.reg("PTTL", 2, cmdPTTL)
	s.reg("DBSIZE", 1, cmdDBSize)
	s.reg("INFO", -1, cmdInfo)
	s.reg("QUIT", -1, cmdQuit)
	s.reg("KEYS", 2, cmdKeys)
	s.reg("SCAN", -2, cmdScan)
	s.reg("HSCAN", -3, cmdHScan)
	s.reg("SSCAN", -3, cmdSScan)
	s.reg("ZSCAN", -3, cmdZScan)
	s.reg("CONFIG", -2, cmdConfig)
	s.reg("CLIENT", -1, cmdClient)
	s.reg("SLOWLOG", -2, cmdSlowlog)
	s.reg("LRANGE", 4, cmdLRange)
	s.reg("LLEN", 2, cmdLLen)
	s.reg("LINDEX", 3, cmdLIndex)
	s.reg("HGET", 3, cmdHGet)
	s.reg("HGETALL", 2, cmdHGetAll)
	s.reg("HLEN", 2, cmdHLen)
	s.reg("HEXISTS", 3, cmdHExists)
	s.reg("HKEYS", 2, cmdHKeys)
	s.reg("HVALS", 2, cmdHVals)
	s.reg("SMEMBERS", 2, cmdSMembers)
	s.reg("SISMEMBER", 3, cmdSIsMember)
	s.reg("SCARD", 2, cmdSCard)
	s.reg("ZRANGE", -4, cmdZRange)
	s.reg("ZREVRANGE", -4, cmdZRevRange)
	s.reg("ZSCORE", 3, cmdZScore)
	s.reg("ZCARD", 2, cmdZCard)
	s.reg("ZRANK", -3, cmdZRank)
	s.reg("ZREVRANK", -3, cmdZRevRank)

	// Pub/Sub commands.
	s.reg("SUBSCRIBE", -2, cmdSubscribe)
	s.reg("UNSUBSCRIBE", -1, cmdUnsubscribe)
	s.reg("PSUBSCRIBE", -2, cmdPSubscribe)
	s.reg("PUNSUBSCRIBE", -1, cmdPUnsubscribe)
	s.reg("PUBLISH", 3, cmdPublish)

	// Transaction commands.
	s.reg("MULTI", 1, cmdMulti)
	s.reg("EXEC", 1, cmdExec)
	s.reg("DISCARD", 1, cmdDiscard)
	s.reg("WATCH", -2, cmdWatch)
	s.reg("UNWATCH", 1, cmdUnwatch)

	// Write commands: they mutate the dataset and are propagated to the AOF.
	s.regW("DEL", -2, cmdDel)
	s.regW("FLUSHDB", -1, cmdFlushDB)
	s.regW("HDEL", -3, cmdHDel)
	s.regW("LPOP", -2, cmdLPop)
	s.regW("RPOP", -2, cmdRPop)
	s.regW("PERSIST", 2, cmdPersist)
	s.regW("SREM", -3, cmdSRem)
	s.regW("ZREM", -3, cmdZRem)
	s.regW("EXPIREAT", 3, cmdExpireAt)
	s.regW("PEXPIREAT", 3, cmdPExpireAt)

	// Write commands that can grow the dataset: under noeviction they are
	// denied once the store is over maxmemory.
	s.regWG("APPEND", 3, cmdAppend)
	s.regWG("DECR", 2, cmdDecr)
	s.regWG("DECRBY", 3, cmdDecrBy)
	s.regWG("HSET", -4, cmdHSet)
	s.regWG("INCR", 2, cmdIncr)
	s.regWG("INCRBY", 3, cmdIncrBy)
	s.regWG("LPUSH", -3, cmdLPush)
	s.regWG("LSET", 4, cmdLSet)
	s.regWG("RPUSH", -3, cmdRPush)
	s.regWG("SADD", -3, cmdSAdd)
	s.regWG("ZADD", -4, cmdZAdd)

	// Write commands that propagate themselves to the AOF, because the
	// rewrite (relative -> absolute TTL) depends on whether the command
	// took effect.
	s.regWS("EXPIRE", 3, cmdExpire)
	s.regWS("PEXPIRE", 3, cmdPExpire)
	s.regWSG("SET", -3, cmdSet)

	// Persistence commands: they touch the dataset only indirectly (they
	// write files), so they are not propagated to the AOF.
	s.reg("SAVE", 1, cmdSave)
	s.reg("BGSAVE", -1, cmdBGSAVE)
	s.reg("BGREWRITEAOF", 1, cmdBGREWRITEAOF)

	// Protocol negotiation.
	s.reg("HELLO", -1, cmdHello)

	// Sorted set commands (third batch: set operations and removals).
	s.regW("ZUNIONSTORE", -4, cmdZUnionStore)
	s.regW("ZINTERSTORE", -4, cmdZInterStore)
	s.reg("ZDIFF", -3, cmdZDiff)
	s.regW("ZDIFFSTORE", -4, cmdZDiffStore)
	s.reg("ZINTERCARD", -3, cmdZInterCard)
	s.regW("ZREMRANGEBYRANK", 4, cmdZRemRangeByRank)
	s.regW("ZREMRANGEBYSCORE", 4, cmdZRemRangeByScore)
	s.regW("ZREMRANGEBYLEX", 4, cmdZRemRangeByLex)
	s.regW("ZPOPMIN", -2, cmdZPopMin)
	s.regW("ZPOPMAX", -2, cmdZPopMax)
	s.reg("ZRANDMEMBER", -2, cmdZRandMember)
	s.regW("ZRANGESTORE", -5, cmdZRangeStore)

	// Sorted set commands (second batch).
	s.regWG("ZINCRBY", 4, cmdZIncrBy)
	s.reg("ZCOUNT", 4, cmdZCount)
	s.reg("ZMSCORE", -3, cmdZMScore)
	s.reg("ZRANGEBYSCORE", -4, cmdZRangeByScore)
	s.reg("ZREVRANGEBYSCORE", -4, cmdZRevRangeByScore)
	s.reg("ZRANGEBYLEX", -4, cmdZRangeByLex)
	s.reg("ZREVRANGEBYLEX", -4, cmdZRevRangeByLex)
	s.reg("ZLEXCOUNT", 4, cmdZLexCount)

	// List commands (second batch).
	s.regW("LINSERT", 5, cmdLInsert)
	s.regW("LREM", 4, cmdLRem)
	s.regW("LTRIM", 4, cmdLTrim)
	s.reg("LPOS", -3, cmdLPos)
	s.regW("LMOVE", 5, cmdLMove)
	s.regW("RPOPLPUSH", 3, cmdRPopLPush)
	s.regWG("LPUSHX", -3, cmdLPushX)
	s.regWG("RPUSHX", -3, cmdRPushX)

	// Set commands (second batch).
	s.reg("SINTERCARD", -3, cmdSInterCard)
	s.regW("SMOVE", 4, cmdSMove)
	s.regW("SPOP", -2, cmdSPop)
	s.reg("SRANDMEMBER", -2, cmdSRandMember)
	s.reg("SMISMEMBER", -3, cmdSMIsMember)
	s.reg("SDIFF", -2, cmdSDiff)
	s.regW("SDIFFSTORE", -3, cmdSDiffStore)
	s.reg("SINTER", -2, cmdSInter)
	s.regW("SINTERSTORE", -3, cmdSInterStore)
	s.reg("SUNION", -2, cmdSUnion)
	s.regW("SUNIONSTORE", -3, cmdSUnionStore)

	// Hash commands (second batch).
	s.regWG("HINCRBY", 4, cmdHIncrBy)
	s.regWG("HINCRBYFLOAT", 4, cmdHIncrByFloat)
	s.reg("HSTRLEN", 3, cmdHStrLen)
	s.reg("HMGET", -3, cmdHMGet)
	s.regWG("HMSET", -4, cmdHMSet)
	s.regWG("HSETNX", 4, cmdHSetNX)
	s.reg("HRANDFIELD", -2, cmdHRandField)
	s.regW("HGETDEL", -5, cmdHGetDel)

	// String commands (second batch).
	s.regWSG("MSET", -3, cmdMSet)
	s.regWSG("MSETNX", -3, cmdMSetNX)
	s.reg("MGET", -2, cmdMGet)
	s.regWSG("GETSET", 3, cmdGetSet)
	s.regWS("GETDEL", 2, cmdGetDel)
	s.reg("GETEX", -2, cmdGetEx)
	s.regWSG("SETEX", 4, cmdSetEx)
	s.regWSG("PSETEX", 4, cmdPSetEx)
	s.regWSG("SETNX", 3, cmdSetNX)
	s.reg("GETRANGE", 4, cmdGetRange)
	s.regWSG("SETRANGE", 4, cmdSetRange)
	s.regWG("INCRBYFLOAT", 3, cmdIncrByFloat)

	// Blocking commands.
	s.regW("BLPOP", -3, cmdBLPop)
	s.regW("BRPOP", -3, cmdBRPop)
	s.regW("BLMOVE", 6, cmdBLMove)
	s.regW("BZPOPMIN", -3, cmdBZPopMin)
	s.regW("BZPOPMAX", -3, cmdBZPopMax)

	// Key management commands.
	s.regW("RENAME", 3, cmdRename)
	s.regW("RENAMENX", 3, cmdRenameNX)
	s.regW("COPY", -3, cmdCopy)
	s.regW("MOVE", 3, cmdMove)
	s.reg("DUMP", 2, cmdDump)
	s.regW("RESTORE", -4, cmdRestore)
	s.reg("EXPIRETIME", 2, cmdExpireTime)
	s.reg("PEXPIRETIME", 2, cmdPExpireTime)
	s.regW("SWAPDB", 3, cmdSwapDB)
	s.reg("OBJECT", -2, cmdObject)
	s.reg("SORT", -2, cmdSort)

	// Administration commands.
	s.reg("COMMAND", -1, cmdCommand)
	s.reg("RESET", 1, cmdReset)
	s.reg("PUBSUB", -2, cmdPubSub)
	s.reg("TIME", 1, cmdTime)
	s.reg("LASTSAVE", 1, cmdLastSave)
	s.reg("RANDOMKEY", 1, cmdRandomKey)
	s.reg("DEBUG", -2, cmdDebug)
	s.reg("TOUCH", -2, cmdTouch)
	s.regW("UNLINK", -2, cmdUnlink)

	// Authentication.
	s.reg("AUTH", -2, cmdAuth)

	// Database commands. SELECT only changes the connection's database, so it
	// propagates itself to the AOF and does not dirty the dataset.
	s.reg("SELECT", 2, cmdSelect)
	s.regW("FLUSHALL", -1, cmdFlushAll)

	// Stream commands. XADD propagates itself to the AOF, because the
	// identifier it writes must be the one that was actually generated.
	s.regWSG("XADD", -5, cmdXAdd)
	s.reg("XLEN", 2, cmdXLen)
	s.reg("XRANGE", -4, cmdXRange)
	s.reg("XREVRANGE", -4, cmdXRevRange)
	s.regW("XDEL", -3, cmdXDel)
	s.regW("XTRIM", -4, cmdXTrim)
	s.regW("XSETID", -3, cmdXSetID)

	// Stream commands: the consumer groups and the reading. XREADGROUP
	// propagates itself to the AOF, because the pending list of the group is
	// written as a series of XCLAIM commands.
	s.regW("XGROUP", -2, cmdXGroup)
	s.regW("XACK", -4, cmdXAck)
	s.regW("XCLAIM", -6, cmdXClaim)
	s.regW("XAUTOCLAIM", -7, cmdXAutoClaim)
	s.reg("XPENDING", -3, cmdXPending)
	s.reg("XINFO", -2, cmdXInfo)
	s.reg("XREAD", -4, cmdXRead)
	s.regWS("XREADGROUP", -7, cmdXReadGroup)
}

// cmdSpec describes a single command: its handler and its metadata.
//
// Arity follows Redis: a positive n means exactly n arguments (including the
// command name itself), a negative n means at least |n| arguments. It backs
// the queue-time validation of commands inside MULTI, like Redis does.
type cmdSpec struct {
	name string // canonical (lower case) name of the command
	// namePtr points to the same name: the connection loop keeps the
	// pointer to show the last command of a client (CLIENT LIST) without
	// allocating on the path of every command.
	namePtr *string
	handler Handler
	arity   int
	write   bool // mutates the dataset: the command is propagated to the AOF
	aofSelf bool // the handler propagates the command to the AOF itself
	memAdd  bool // can grow the dataset: denied under noeviction when over the limit
}

// register stores a command in the registry under both its lower case and its
// upper case spelling.
func (s *Server) register(spec cmdSpec) {
	s.cmds[spec.name] = spec
	s.cmdsUpper[strings.ToUpper(spec.name)] = spec
}

// lookupCommand finds a command by the bytes the client sent, without
// allocating. Clients may spell a command in any case; the two common
// spellings are covered by two maps, so the hot path never builds a string.
func (s *Server) lookupCommand(cmd []byte) (cmdSpec, bool) {
	// A map lookup with a []byte key does not copy the bytes.
	if sp, ok := s.cmdsUpper[string(cmd)]; ok {
		return sp, true
	}
	if sp, ok := s.cmds[string(cmd)]; ok {
		return sp, true
	}
	// Mixed case: rare, fall back to the slow path.
	if sp, ok := s.cmds[strings.ToLower(string(cmd))]; ok {
		return sp, true
	}
	return cmdSpec{}, false
}

// registerCmd registers a command under its lower case name.
func (s *Server) registerCmd(name string, arity int, h Handler, write, aofSelf, memAdd bool) {
	lower := strings.ToLower(name)
	// The pointer to the name stays valid for the whole life of the
	// server: the registry keeps it.
	s.register(cmdSpec{
		name:    lower,
		namePtr: &lower,
		handler: h,
		arity:   arity,
		write:   write,
		aofSelf: aofSelf,
		memAdd:  memAdd,
	})
}

// reg registers a read-only command.
func (s *Server) reg(name string, arity int, h Handler) {
	s.registerCmd(name, arity, h, false, false, false)
}

// regW registers a command that mutates the dataset: it is propagated to the
// AOF after execution.
func (s *Server) regW(name string, arity int, h Handler) {
	s.registerCmd(name, arity, h, true, false, false)
}

// regWG registers a command that mutates the dataset and can grow it.
func (s *Server) regWG(name string, arity int, h Handler) {
	s.registerCmd(name, arity, h, true, false, true)
}

// regWS registers a command that mutates the dataset and propagates itself
// to the AOF.
func (s *Server) regWS(name string, arity int, h Handler) {
	s.registerCmd(name, arity, h, true, true, false)
}

// regWSG registers a command that mutates the dataset, can grow it and
// propagates itself to the AOF.
func (s *Server) regWSG(name string, arity int, h Handler) {
	s.registerCmd(name, arity, h, true, true, true)
}

func wrongArgs(w *resp.Writer, cmd string) {
	w.WriteError("ERR wrong number of arguments for '" + cmd + "' command")
}

// writeWrongType writes the standard WRONGTYPE error.
func writeWrongType(w *resp.Writer) error {
	return w.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
}

func writeWrongTypeIf(w *resp.Writer, err error) error {
	if err != nil {
		return writeWrongType(w)
	}
	return nil
}

func parseFloat(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func cmdPing(ctx *Ctx) error {
	// In subscribe mode Redis replies with ["pong", message].
	if ctx.Client != nil && ctx.Client.subscribed() {
		if len(ctx.Args) > 2 {
			wrongArgs(ctx.W, "ping")
			return nil
		}
		msg := []byte("")
		if len(ctx.Args) == 2 {
			msg = ctx.Args[1]
		}
		if err := ctx.W.WriteArrayHeader(2); err != nil {
			return err
		}
		if err := ctx.W.WriteSimpleString("pong"); err != nil {
			return err
		}
		return ctx.W.WriteBulkString(msg)
	}
	switch len(ctx.Args) {
	case 1:
		return ctx.W.WriteSimpleString("PONG")
	case 2:
		return ctx.W.WriteBulkString(ctx.Args[1])
	default:
		wrongArgs(ctx.W, "ping")
		return nil
	}
}

func cmdEcho(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "echo")
		return nil
	}
	return ctx.W.WriteBulkString(ctx.Args[1])
}

func cmdType(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "type")
		return nil
	}
	t, ok := ctx.Store.GetType(ctx.Args[1])
	if !ok {
		return ctx.W.WriteSimpleString("none")
	}
	return ctx.W.WriteSimpleString(t.String())
}

func cmdDel(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "del")
		return nil
	}
	n := ctx.Store.Delete(ctx.Args[1:]...)
	return ctx.W.WriteInteger(int64(n))
}

func cmdExists(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "exists")
		return nil
	}
	n := ctx.Store.Exists(ctx.Args[1:]...)
	return ctx.W.WriteInteger(int64(n))
}

func cmdExpire(ctx *Ctx) error { return expireCommon(ctx, "expire", time.Second) }

func cmdPExpire(ctx *Ctx) error {
	return expireCommon(ctx, "pexpire", time.Millisecond)
}

func cmdExpireAt(ctx *Ctx) error {
	return expireAtCommon(ctx, "expireat", time.Second)
}

func cmdPExpireAt(ctx *Ctx) error {
	return expireAtCommon(ctx, "pexpireat", time.Millisecond)
}

func expireAtCommon(ctx *Ctx, cmd string, unit time.Duration) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	ts, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	var at time.Time
	if unit == time.Millisecond {
		at = time.UnixMilli(ts)
	} else {
		at = time.Unix(ts, 0)
	}
	if !ctx.Store.ExpireAt(ctx.Args[1], at) {
		return ctx.W.WriteInteger(0)
	}
	return ctx.W.WriteInteger(1)
}

func expireCommon(ctx *Ctx, cmd string, unit time.Duration) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	ttl, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	d := time.Duration(ttl) * unit
	if d <= 0 {
		ctx.Store.Delete(ctx.Args[1])
		ctx.propagateCmd()
		return ctx.W.WriteInteger(1)
	}
	ok := ctx.Store.Expire(ctx.Args[1], d)
	if ok {
		ctx.propagateCmd()
		return ctx.W.WriteInteger(1)
	}
	return ctx.W.WriteInteger(0)
}

func cmdTTL(ctx *Ctx) error {
	return ttlCommon(ctx, "ttl", time.Second)
}

func cmdPTTL(ctx *Ctx) error {
	return ttlCommon(ctx, "pttl", time.Millisecond)
}

func ttlCommon(ctx *Ctx, cmd string, unit time.Duration) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	d, ok := ctx.Store.TTL(ctx.Args[1])
	if !ok {
		return ctx.W.WriteInteger(-2)
	}
	if d < 0 {
		return ctx.W.WriteInteger(-1)
	}
	if unit == time.Second {
		// Redis rounds the remaining time to the nearest second.
		return ctx.W.WriteInteger(int64((d + 500*time.Millisecond) / time.Second))
	}
	return ctx.W.WriteInteger(int64(d / unit))
}

func cmdPersist(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "persist")
		return nil
	}
	if ctx.Store.Persist(ctx.Args[1]) {
		return ctx.W.WriteInteger(1)
	}
	return ctx.W.WriteInteger(0)
}

func cmdDBSize(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "dbsize")
		return nil
	}
	return ctx.W.WriteInteger(int64(ctx.Store.Len()))
}

func cmdFlushDB(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "flushdb")
		return nil
	}
	ctx.Store.Flush()
	return ctx.W.WriteSimpleString("OK")
}

func cmdQuit(ctx *Ctx) error {
	ctx.W.WriteSimpleString("OK")
	return errQuit
}

func cmdInfo(ctx *Ctx) error {
	s := ctx.Srv
	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\n")
	fmt.Fprintf(&b, "redis_version:1.0.0\r\n")
	fmt.Fprintf(&b, "redis_mode:standalone\r\n")
	fmt.Fprintf(&b, "os:%s\r\n", runtime.GOOS)
	fmt.Fprintf(&b, "tcp_port:%d\r\n", portOf(s.ln.Addr()))
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(time.Since(s.started).Seconds()))
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "# Clients\r\n")
	fmt.Fprintf(&b, "connected_clients:%d\r\n", s.connectedClients())
	subCh, subPat := s.PubSub.Counts()
	fmt.Fprintf(&b, "pubsub_channels:%d\r\n", subCh)
	fmt.Fprintf(&b, "pubsub_patterns:%d\r\n", subPat)
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "# Memory\r\n")
	used := ctx.Store.UsedBytes()
	fmt.Fprintf(&b, "used_memory:%d\r\n", used)
	fmt.Fprintf(&b, "maxmemory:%d\r\n", s.maxMemory.Load())
	fmt.Fprintf(&b, "maxmemory_policy:%s\r\n", s.maxMemoryPolicyName())
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "# Stats\r\n")
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", s.receivedTotal())
	fmt.Fprintf(&b, "evicted_keys:%d\r\n", s.evicted.Load())
	fmt.Fprintf(&b, "pubsub_messages:%d\r\n", s.PubSub.Messages())
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "# Keyspace\r\n")
	for i, db := range s.dbs {
		keys, expires := db.Stats()
		fmt.Fprintf(&b, "db%d:keys=%d,expires=%d,avg_ttl=0\r\n", i, keys, expires)
	}
	return ctx.W.WriteBulkString([]byte(b.String()))
}

func (s *Server) connectedClients() int64 {
	return atomic.LoadInt64(&s.connsNum)
}

func portOf(a interface{ String() string }) int {
	str := a.String()
	idx := strings.LastIndex(str, ":")
	if idx < 0 {
		return 0
	}
	n, _ := strconv.Atoi(str[idx+1:])
	return n
}
