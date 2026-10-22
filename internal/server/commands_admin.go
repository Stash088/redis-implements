package server

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/config"
)

func cmdKeys(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "keys")
		return nil
	}
	pattern := string(ctx.Args[1])
	keys := ctx.Store.Keys()
	var out [][]byte
	for _, k := range keys {
		if globMatch(pattern, string(k)) {
			out = append(out, k)
		}
	}
	if err := ctx.W.WriteArrayHeader(len(out)); err != nil {
		return err
	}
	for _, k := range out {
		if err := ctx.W.WriteBulkString(k); err != nil {
			return err
		}
	}
	return nil
}

func cmdScan(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "scan")
		return nil
	}
	cursor, err := strconv.ParseUint(string(ctx.Args[1]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR invalid cursor")
	}
	count := 10
	pattern := ""
	for i := 2; i < len(ctx.Args); i++ {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "MATCH":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			pattern = string(ctx.Args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil || n <= 0 {
				return ctx.W.WriteError("ERR COUNT must be a positive integer")
			}
			count = n
			i++
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}

	// Cursor indexes a sorted snapshot of live keys: full coverage with no
	// duplicates (a superset of Redis's guarantees, which allow both).
	keys := ctx.Store.Keys()
	sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
	start := cursor
	if start > uint64(len(keys)) {
		start = uint64(len(keys))
	}
	end := start + uint64(count)
	if end > uint64(len(keys)) {
		end = uint64(len(keys))
	}
	next := end
	if next == uint64(len(keys)) {
		next = 0
	}

	var matched [][]byte
	for _, k := range keys[start:end] {
		if pattern == "" || globMatch(pattern, string(k)) {
			matched = append(matched, k)
		}
	}

	if err := ctx.W.WriteArrayHeader(2); err != nil {
		return err
	}
	if err := ctx.W.WriteBulkString([]byte(strconv.FormatUint(next, 10))); err != nil {
		return err
	}
	if err := ctx.W.WriteArrayHeader(len(matched)); err != nil {
		return err
	}
	for _, k := range matched {
		if err := ctx.W.WriteBulkString(k); err != nil {
			return err
		}
	}
	return nil
}

func cmdConfig(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "config")
		return nil
	}
	switch strings.ToLower(string(ctx.Args[1])) {
	case "get":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "config get")
			return nil
		}
		return configGet(ctx, string(ctx.Args[2]))
	case "set":
		return configSet(ctx, ctx.Args[2:])
	default:
		return ctx.W.WriteError("ERR Unknown CONFIG subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}

// configParams returns the configurable parameters as name/value pairs.
func (s *Server) configParams() [][2]string {
	thresh, maxLen := s.sl.config()
	return [][2]string{
		{"maxmemory", strconv.FormatInt(s.maxMemory.Load(), 10)},
		{"maxmemory-policy", s.maxMemoryPolicyName()},
		{"maxmemory-samples", strconv.FormatInt(s.maxMemorySamples.Load(), 10)},
		{"save", s.savePolicyString()},
		{"appendonly", yesNo(s.AOF != nil)},
		{"appendfsync", s.appendFsync},
		{"dbfilename", filepath.Base(s.DBPath)},
		{"appendfilename", filepath.Base(s.AOFPath)},
		{"dir", filepath.Dir(s.DBPath)},
		{"port", strconv.Itoa(portOf(s.ln.Addr()))},
		{"slowlog-log-slower-than", strconv.FormatInt(thresh, 10)},
		{"slowlog-max-len", strconv.Itoa(maxLen)},
		{"requirepass", s.requirePass()},
	}
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// savePolicyString formats the save policy as "seconds changes" (Redis style)
// or "" when disabled.
func (s *Server) savePolicyString() string {
	if s.saveSeconds <= 0 {
		return ""
	}
	return strconv.Itoa(s.saveSeconds) + " " + strconv.Itoa(s.saveChanges)
}

func configGet(ctx *Ctx, pattern string) error {
	params := ctx.Srv.configParams()
	var out [][2]string
	for _, p := range params {
		if globMatch(pattern, p[0]) {
			out = append(out, p)
		}
	}
	w := ctx.W
	// In RESP3 the parameters come as a map, in RESP2 as a flat array.
	if ctx.proto3() {
		if err := w.WriteMapHeader(len(out)); err != nil {
			return err
		}
	} else if err := w.WriteArrayHeader(len(out) * 2); err != nil {
		return err
	}
	for _, p := range out {
		if err := w.WriteBulkString([]byte(p[0])); err != nil {
			return err
		}
		if err := w.WriteBulkString([]byte(p[1])); err != nil {
			return err
		}
	}
	return nil
}

func configSet(ctx *Ctx, args [][]byte) error {
	if len(args) < 2 || len(args)%2 != 0 {
		wrongArgs(ctx.W, "config set")
		return nil
	}
	s := ctx.Srv
	for i := 0; i < len(args); i += 2 {
		name := strings.ToLower(string(args[i]))
		value := string(args[i+1])
		switch name {
		case "maxmemory":
			n, err := config.ParseBytes(value)
			if err != nil {
				return ctx.W.WriteError("ERR CONFIG SET failed for '" + name + "': " + err.Error())
			}
			s.SetMaxMemory(n)
		case "maxmemory-policy":
			switch value {
			case "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random", "volatile-ttl":
			default:
				return ctx.W.WriteError("ERR CONFIG SET failed for 'maxmemory-policy': unknown policy " + value)
			}
			s.SetMaxMemoryPolicy(value)
		case "maxmemory-samples":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return ctx.W.WriteError("ERR CONFIG SET failed for 'maxmemory-samples': invalid value")
			}
			s.SetMaxMemorySamples(int64(n))
		case "slowlog-log-slower-than":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ctx.W.WriteError("ERR CONFIG SET failed for 'slowlog-log-slower-than': invalid value")
			}
			thresh, maxLen := s.sl.config()
			s.sl.setConfig(n, maxLen)
			_ = thresh
		case "requirepass":
			// An empty password disables authentication.
			s.SetRequirePass(value)
		default:
			return ctx.W.WriteError("ERR Unsupported CONFIG parameter: " + name)
		}
	}
	return ctx.W.WriteSimpleString("OK")
}

func cmdClient(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "client")
		return nil
	}
	c := ctx.Client
	switch strings.ToLower(string(ctx.Args[1])) {
	case "id":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "client id")
			return nil
		}
		return ctx.W.WriteInteger(int64(c.id))
	case "getname":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "client getname")
			return nil
		}
		return ctx.W.WriteBulkString([]byte(c.getName()))
	case "setname":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "client setname")
			return nil
		}
		name := string(ctx.Args[2])
		if strings.ContainsAny(name, " \r\n") {
			return ctx.W.WriteError("ERR Client names cannot contain spaces, newlines or special characters.")
		}
		c.setName(name)
		return ctx.W.WriteSimpleString("OK")
	case "list":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "client list")
			return nil
		}
		return writeClientList(ctx)
	default:
		return ctx.W.WriteError("ERR Unknown CLIENT subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}

func writeClientList(ctx *Ctx) error {
	s := ctx.Srv
	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()

	now := time.Now()
	var b strings.Builder
	for i, c := range clients {
		inMulti, watchCount := c.txnState()
		sub, psub := s.PubSub.clientSubs(c)
		fmt.Fprintf(&b, "id=%d addr=%s name=%s age=%d db=%d sub=%d psub=%d multi=%d watch=%d cmd=%s",
			c.id, c.addr, c.getName(), int64(now.Sub(c.created).Seconds()), c.db,
			sub, psub, inMulti, watchCount, c.lastCmd)
		if i < len(clients)-1 {
			b.WriteString("\n")
		}
	}
	return ctx.W.WriteBulkString([]byte(b.String()))
}

func cmdSlowlog(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "slowlog")
		return nil
	}
	switch strings.ToLower(string(ctx.Args[1])) {
	case "get":
		n := 0
		if len(ctx.Args) > 2 {
			v, err := strconv.Atoi(string(ctx.Args[2]))
			if err != nil {
				return ctx.W.WriteError("ERR invalid SLOWLOG GET count")
			}
			n = v
		}
		entries := ctx.Srv.sl.get(n)
		w := ctx.W
		if err := w.WriteArrayHeader(len(entries)); err != nil {
			return err
		}
		for _, e := range entries {
			if err := w.WriteArrayHeader(6); err != nil {
				return err
			}
			if err := w.WriteInteger(int64(e.id)); err != nil {
				return err
			}
			if err := w.WriteInteger(e.at.Unix()); err != nil {
				return err
			}
			if err := w.WriteInteger(e.dur.Microseconds()); err != nil {
				return err
			}
			if err := w.WriteArrayHeader(len(e.args)); err != nil {
				return err
			}
			for _, a := range e.args {
				if err := w.WriteBulkString(a); err != nil {
					return err
				}
			}
			if err := w.WriteBulkString([]byte(e.addr)); err != nil {
				return err
			}
			if err := w.WriteBulkString([]byte(e.name)); err != nil {
				return err
			}
		}
		return nil
	case "len":
		return ctx.W.WriteInteger(int64(ctx.Srv.sl.length()))
	case "reset":
		ctx.Srv.sl.reset()
		return ctx.W.WriteSimpleString("OK")
	default:
		return ctx.W.WriteError("ERR Unknown SLOWLOG subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}
