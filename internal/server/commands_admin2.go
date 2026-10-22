package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// cmdReset returns the connection to its initial state: the transaction is
// discarded, the subscriptions are removed, the name and the reply mode are
// reset and the connection goes back to database 0.
func cmdReset(ctx *Ctx) error {
	c := ctx.Client
	if c.inMulti {
		c.endMulti(ctx.Store)
	}
	ctx.Srv.PubSub.removeClient(c)
	c.setName("")
	c.db = 0
	c.replyMode = replyOn
	return ctx.W.WriteSimpleString("RESET")
}

// cmdPubSub inspects the Pub/Sub subsystem: CHANNELS, NUMSUB and NUMPAT.
func cmdPubSub(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "pubsub")
		return nil
	}
	switch strings.ToLower(string(ctx.Args[1])) {
	case "channels":
		if len(ctx.Args) > 3 {
			wrongArgs(ctx.W, "pubsub channels")
			return nil
		}
		pattern := ""
		if len(ctx.Args) == 3 {
			pattern = string(ctx.Args[2])
		}
		channels := ctx.Srv.PubSub.channelNames(pattern)
		elems := make([]resp.Value, 0, len(channels))
		for _, ch := range channels {
			elems = append(elems, resp.NewBulkString([]byte(ch)))
		}
		return ctx.W.WriteValue(resp.NewArray(elems))
	case "numsub":
		if len(ctx.Args) < 2 {
			wrongArgs(ctx.W, "pubsub numsub")
			return nil
		}
		elems := make([]resp.Value, 0, (len(ctx.Args)-2)*2)
		for _, ch := range ctx.Args[2:] {
			elems = append(elems,
				resp.NewBulkString(ch),
				resp.NewInteger(int64(ctx.Srv.PubSub.numSub(string(ch)))))
		}
		return ctx.W.WriteValue(resp.NewArray(elems))
	case "numpat":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "pubsub numpat")
			return nil
		}
		return ctx.W.WriteInteger(int64(ctx.Srv.PubSub.patternCount()))
	default:
		return ctx.W.WriteError("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}

// cmdUnlink removes the given keys. Like DEL, but the deletion is done
// "in the background" in Redis; here the keys are removed right away.
func cmdUnlink(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "unlink")
		return nil
	}
	n := ctx.Store.Delete(ctx.Args[1:]...)
	return ctx.W.WriteInteger(int64(n))
}

// cmdTouch updates the last access time of the given keys and returns the
// number of keys that exist.
func cmdTouch(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "touch")
		return nil
	}
	n := ctx.Store.Touch(ctx.Args[1:]...)
	return ctx.W.WriteInteger(int64(n))
}

// cmdRandomKey returns a random key of the current database.
func cmdRandomKey(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "randomkey")
		return nil
	}
	key, ok := ctx.Store.RandomKey()
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	return ctx.W.WriteBulkString(key)
}

// cmdLastSave returns the UNIX timestamp of the last successful save.
func cmdLastSave(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "lastsave")
		return nil
	}
	return ctx.W.WriteInteger(ctx.Srv.lastSave.Load())
}

// cmdTime returns the current server time as [seconds, microseconds].
func cmdTime(ctx *Ctx) error {
	if len(ctx.Args) != 1 {
		wrongArgs(ctx.W, "time")
		return nil
	}
	now := time.Now()
	elems := []resp.Value{
		resp.NewBulkString([]byte(strconv.FormatInt(now.Unix(), 10))),
		resp.NewBulkString([]byte(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))),
	}
	return ctx.W.WriteValue(resp.NewArray(elems))
}

// cmdDebug implements the debugging commands we support: SLEEP, JMAP,
// OBJECT and SET-ACTIVE-EXPIRE.
func cmdDebug(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "debug")
		return nil
	}
	switch strings.ToLower(string(ctx.Args[1])) {
	case "sleep":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "debug sleep")
			return nil
		}
		sec, err := strconv.ParseFloat(string(ctx.Args[2]), 64)
		if err != nil {
			return ctx.W.WriteError("ERR timeout is not a float or out of range")
		}
		if sec > 0 {
			time.Sleep(time.Duration(sec * float64(time.Second)))
		}
		return ctx.W.WriteSimpleString("OK")
	case "jmap":
		return ctx.W.WriteSimpleString("OK")
	case "object":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "debug object")
			return nil
		}
		t, ok := ctx.Store.GetType(ctx.Args[2])
		if !ok {
			return ctx.W.WriteError("ERR no such key")
		}
		desc := "Value at:0x0"
		return ctx.W.WriteBulkString([]byte(desc + " refcount:1 encoding:" + encodingName(t) + " serializedlength:0 lru:0 lru_seconds_idle:0"))
	case "set-active-expire":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "debug set-active-expire")
			return nil
		}
		return ctx.W.WriteSimpleString("OK")
	default:
		return ctx.W.WriteError("ERR DEBUG subcommand '" + string(ctx.Args[1]) + "' not supported")
	}
}

// encodingName returns the encoding name of a type, like Redis reports it.
func encodingName(t store.Type) string {
	switch t {
	case store.TypeString:
		return "embstr"
	case store.TypeList:
		return "listpack"
	case store.TypeHash:
		return "listpack"
	case store.TypeSet:
		return "intset"
	case store.TypeZSet:
		return "listpack"
	}
	return "none"
}

// clientKill kills the client with the given id or address.
func (s *Server) clientKill(id uint64, addr string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	killed := 0
	for conn, c := range s.clients {
		match := false
		if id != 0 && c.id == id {
			match = true
		}
		if addr != "" && c.addr == addr {
			match = true
		}
		if match {
			conn.Close()
			killed++
		}
	}
	return killed, nil
}

// clientInfoValue returns the CLIENT INFO line of a client.
func clientInfoValue(c *client, now time.Time) resp.Value {
	inMulti, watchCount := c.txnState()
	sub, psub := c.subCount(), 0
	return resp.NewBulkString([]byte(strings.Join([]string{
		"id=" + strconv.FormatUint(c.id, 10),
		"addr=" + c.addr,
		"name=" + c.getName(),
		"age=" + strconv.FormatInt(int64(now.Sub(c.created).Seconds()), 10),
		"db=" + strconv.Itoa(c.db),
		"sub=" + strconv.Itoa(sub),
		"psub=" + strconv.Itoa(psub),
		"multi=" + strconv.Itoa(inMulti),
		"watch=" + strconv.Itoa(watchCount),
		"cmd=" + c.lastCmd,
	}, " ")))
}
