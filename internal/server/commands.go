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
	s.register("PING", cmdPing)
	s.register("ECHO", cmdEcho)
	s.register("TYPE", cmdType)
	s.register("SET", cmdSet)
	s.register("GET", cmdGet)
	s.register("DEL", cmdDel)
	s.register("EXISTS", cmdExists)
	s.register("APPEND", cmdAppend)
	s.register("STRLEN", cmdStrlen)
	s.register("INCR", cmdIncr)
	s.register("DECR", cmdDecr)
	s.register("INCRBY", cmdIncrBy)
	s.register("DECRBY", cmdDecrBy)
	s.register("EXPIRE", cmdExpire)
	s.register("PEXPIRE", cmdPExpire)
	s.register("EXPIREAT", cmdExpireAt)
	s.register("PEXPIREAT", cmdPExpireAt)
	s.register("TTL", cmdTTL)
	s.register("PTTL", cmdPTTL)
	s.register("PERSIST", cmdPersist)
	s.register("DBSIZE", cmdDBSize)
	s.register("FLUSHDB", cmdFlushDB)
	s.register("SAVE", cmdSave)
	s.register("BGSAVE", cmdBGSAVE)
	s.register("BGREWRITEAOF", cmdBGREWRITEAOF)
	s.register("INFO", cmdInfo)
	s.register("QUIT", cmdQuit)

	s.register("SUBSCRIBE", cmdSubscribe)
	s.register("UNSUBSCRIBE", cmdUnsubscribe)
	s.register("PSUBSCRIBE", cmdPSubscribe)
	s.register("PUNSUBSCRIBE", cmdPUnsubscribe)
	s.register("PUBLISH", cmdPublish)

	s.register("MULTI", cmdMulti)
	s.register("EXEC", cmdExec)
	s.register("DISCARD", cmdDiscard)
	s.register("WATCH", cmdWatch)
	s.register("UNWATCH", cmdUnwatch)

	s.register("KEYS", cmdKeys)
	s.register("SCAN", cmdScan)
	s.register("CONFIG", cmdConfig)
	s.register("CLIENT", cmdClient)
	s.register("SLOWLOG", cmdSlowlog)

	s.register("LPUSH", cmdLPush)
	s.register("RPUSH", cmdRPush)
	s.register("LPOP", cmdLPop)
	s.register("RPOP", cmdRPop)
	s.register("LRANGE", cmdLRange)
	s.register("LLEN", cmdLLen)
	s.register("LINDEX", cmdLIndex)
	s.register("LSET", cmdLSet)

	s.register("HSET", cmdHSet)
	s.register("HGET", cmdHGet)
	s.register("HGETALL", cmdHGetAll)
	s.register("HDEL", cmdHDel)
	s.register("HLEN", cmdHLen)
	s.register("HEXISTS", cmdHExists)
	s.register("HKEYS", cmdHKeys)
	s.register("HVALS", cmdHVals)

	s.register("SADD", cmdSAdd)
	s.register("SREM", cmdSRem)
	s.register("SMEMBERS", cmdSMembers)
	s.register("SISMEMBER", cmdSIsMember)
	s.register("SCARD", cmdSCard)

	s.register("ZADD", cmdZAdd)
	s.register("ZRANGE", cmdZRange)
	s.register("ZREVRANGE", cmdZRevRange)
	s.register("ZSCORE", cmdZScore)
	s.register("ZREM", cmdZRem)
	s.register("ZCARD", cmdZCard)
	s.register("ZRANK", cmdZRank)
	s.register("ZREVRANK", cmdZRevRank)
}

// arity maps command names to their Redis arity: a positive n means exactly
// n arguments (including the command name), a negative n means at least |n|.
// It backs queue-time validation of commands inside MULTI, like Redis does.
var arity = map[string]int{
	// general
	"ping": -1, "echo": 2, "type": 2, "dbsize": 1, "flushdb": -1,
	"save": 1, "bgsave": -1, "bgrewriteaof": 1, "info": -1, "quit": -1,
	// string
	"set": -3, "get": 2, "del": -2, "exists": -2, "append": 3, "strlen": 2,
	"incr": 2, "decr": 2, "incrby": 3, "decrby": 3,
	// keyspace
	"expire": 3, "pexpire": 3, "expireat": 3, "pexpireat": 3,
	"ttl": 2, "pttl": 2, "persist": 2,
	// pub/sub
	"subscribe": -2, "unsubscribe": -1, "psubscribe": -2, "punsubscribe": -1, "publish": 3,
	// transactions
	"multi": 1, "exec": 1, "discard": 1, "watch": -2, "unwatch": 1,
	// server admin
	"keys": 2, "scan": -2, "config": -2, "client": -1, "slowlog": -2,
	// list
	"lpush": -3, "rpush": -3, "lpop": -2, "rpop": -2, "lrange": 4, "llen": 2,
	"lindex": 3, "lset": 4,
	// hash
	"hset": -4, "hget": 3, "hgetall": 2, "hdel": -3, "hlen": 2,
	"hexists": 3, "hkeys": 2, "hvals": 2,
	// set
	"sadd": -3, "srem": -3, "smembers": 2, "sismember": 3, "scard": 2,
	// zset
	"zadd": -4, "zrange": -4, "zrevrange": -4, "zscore": 3, "zrem": -3,
	"zcard": 2, "zrank": -3, "zrevrank": -3,
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
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", s.received)
	fmt.Fprintf(&b, "evicted_keys:%d\r\n", s.evicted.Load())
	fmt.Fprintf(&b, "pubsub_messages:%d\r\n", s.PubSub.Messages())
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "# Keyspace\r\n")
	fmt.Fprintf(&b, "db0:keys=%d,expires=%d,avg_ttl=0\r\n", ctx.Store.Len(), 0)
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
