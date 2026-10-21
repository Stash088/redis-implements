package persist

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// FsyncPolicy controls how often the AOF is synced to disk.
type FsyncPolicy int

const (
	// FsyncAlways syncs after every appended command (slowest, safest).
	FsyncAlways FsyncPolicy = iota
	// FsyncEverySec syncs at most once per second from a background goroutine.
	FsyncEverySec
	// FsyncNo leaves flushing to the OS.
	FsyncNo
)

// Appender appends RESP-encoded write commands to an append-only file.
type Appender struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	policy FsyncPolicy
	stop   chan struct{}
	done   chan struct{}
}

// OpenAppender opens (creating if needed) the AOF file at path.
func OpenAppender(path string, policy FsyncPolicy) (*Appender, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("persist: open aof: %w", err)
	}
	a := &Appender{f: f, path: path, policy: policy, stop: make(chan struct{}), done: make(chan struct{})}
	if policy == FsyncEverySec {
		go a.fsyncLoop()
	}
	return a, nil
}

// Append writes one command (as RESP) to the file. With FsyncAlways it also
// syncs before returning.
func (a *Appender) Append(args [][]byte) error {
	buf := EncodeCommand(args)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.f.Write(buf); err != nil {
		return fmt.Errorf("persist: aof write: %w", err)
	}
	if a.policy == FsyncAlways {
		if err := a.f.Sync(); err != nil {
			return fmt.Errorf("persist: aof fsync: %w", err)
		}
	}
	return nil
}

// Sync flushes the file to disk.
func (a *Appender) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Sync()
}

// Close stops background syncing and closes the file.
func (a *Appender) Close() error {
	if a.policy == FsyncEverySec {
		close(a.stop)
		<-a.done
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Close()
}

func (a *Appender) fsyncLoop() {
	defer close(a.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.Sync()
		}
	}
}

// EncodeCommand serializes a command as a RESP array of bulk strings.
func EncodeCommand(args [][]byte) []byte {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	w.WriteArrayHeader(len(args))
	for _, a := range args {
		w.WriteBulkString(a)
	}
	w.Flush()
	return buf.Bytes()
}

// RewriteForAOF normalizes a write command for safe replay. Relative TTLs
// (SET EX/PX, EXPIRE, PEXPIRE) are converted to absolute PEXPIREAT so replay
// reproduces the original expiry instead of re-arming it from load time.
func RewriteForAOF(args [][]byte, now time.Time) [][][]byte {
	if len(args) == 0 {
		return nil
	}
	switch strings.ToLower(string(args[0])) {
	case "set":
		if len(args) < 3 {
			return [][][]byte{args}
		}
		key, val := args[1], args[2]
		var ttl time.Duration
		var flags [][]byte
		for i := 3; i < len(args); i++ {
			switch strings.ToUpper(string(args[i])) {
			case "EX":
				if i+1 < len(args) {
					if sec, err := strconv.ParseInt(string(args[i+1]), 10, 64); err == nil && sec > 0 {
						ttl = time.Duration(sec) * time.Second
					}
					i++
				}
			case "PX":
				if i+1 < len(args) {
					if ms, err := strconv.ParseInt(string(args[i+1]), 10, 64); err == nil && ms > 0 {
						ttl = time.Duration(ms) * time.Millisecond
					}
					i++
				}
			case "NX", "XX":
				flags = append(flags, []byte(strings.ToUpper(string(args[i]))))
			}
		}
		base := append([][]byte{[]byte("set"), key, val}, flags...)
		if ttl > 0 {
			return [][][]byte{
				base,
				{[]byte("pexpireat"), key, itoa(now.Add(ttl).UnixMilli())},
			}
		}
		return [][][]byte{base}
	case "setex":
		// SETEX key seconds value -> SET key value + PEXPIREAT
		if len(args) < 4 {
			return [][][]byte{args}
		}
		sec, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil || sec <= 0 {
			return [][][]byte{args}
		}
		return [][][]byte{
			{[]byte("set"), args[1], args[3]},
			{[]byte("pexpireat"), args[1], itoa(now.Add(time.Duration(sec) * time.Second).UnixMilli())},
		}
	case "psetex":
		// PSETEX key ms value -> SET key value + PEXPIREAT
		if len(args) < 4 {
			return [][][]byte{args}
		}
		ms, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil || ms <= 0 {
			return [][][]byte{args}
		}
		return [][][]byte{
			{[]byte("set"), args[1], args[3]},
			{[]byte("pexpireat"), args[1], itoa(now.Add(time.Duration(ms) * time.Millisecond).UnixMilli())},
		}
	case "expire":
		if len(args) < 3 {
			return [][][]byte{args}
		}
		sec, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return [][][]byte{args}
		}
		if sec <= 0 {
			return [][][]byte{{[]byte("del"), args[1]}}
		}
		return [][][]byte{{[]byte("pexpireat"), args[1], itoa(now.Add(time.Duration(sec) * time.Second).UnixMilli())}}
	case "pexpire":
		if len(args) < 3 {
			return [][][]byte{args}
		}
		ms, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return [][][]byte{args}
		}
		if ms <= 0 {
			return [][][]byte{{[]byte("del"), args[1]}}
		}
		return [][][]byte{{[]byte("pexpireat"), args[1], itoa(now.Add(time.Duration(ms) * time.Millisecond).UnixMilli())}}
	default:
		return [][][]byte{args}
	}
}

// LoadAOF replays a RESP command log into the given stores. A SELECT command
// switches the target database, like Redis does. A truncated trailing command
// is tolerated; other read errors abort the load.
func LoadAOF(r io.Reader, stores []*store.Store) error {
	rd := resp.NewReader(r)
	cur := 0
	for {
		v, err := rd.ReadValue()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end or truncated tail
			}
			return fmt.Errorf("persist: aof read: %w", err)
		}
		if v.Kind != resp.KindArray || v.IsNull {
			return fmt.Errorf("persist: aof contains a non-command frame")
		}
		args := make([][]byte, 0, len(v.Array))
		for i := range v.Array {
			el := v.Array[i]
			if el.Kind != resp.KindBulkString || el.IsNull {
				return fmt.Errorf("persist: aof command arguments must be bulk strings")
			}
			args = append(args, el.Str)
		}
		if cur >= len(stores) {
			cur = 0
		}
		if len(args) > 0 && strings.EqualFold(string(args[0]), "flushall") {
			for _, st := range stores {
				st.Flush()
			}
			continue
		}
		if len(args) > 0 && strings.EqualFold(string(args[0]), "select") {
			n, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return fmt.Errorf("persist: aof: bad SELECT index")
			}
			if n >= 0 && n < len(stores) {
				cur = n
			}
			continue
		}
		if err := applyAOFCommand(stores[cur], args); err != nil {
			return err
		}
	}
}

// applyAOFCommand applies one replayed command to the store. Store-level errors
// (e.g. WRONGTYPE) are ignored because replay is expected to reproduce the
// exact original execution, including failures.
func applyAOFCommand(st *store.Store, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToLower(string(args[0]))
	key := func() []byte {
		if len(args) > 1 {
			return args[1]
		}
		return nil
	}
	switch name {
	case "mset":
		// MSET key value [key value ...]
		if len(args) >= 3 && len(args)%2 == 1 {
			st.SetStrings(args[1:], false)
		}
	case "msetnx":
		if len(args) >= 3 && len(args)%2 == 1 {
			st.SetStrings(args[1:], true)
		}
	case "getset":
		if len(args) >= 3 {
			st.GetSet(key(), args[2]) //nolint:errcheck
		}
	case "getdel":
		if len(args) >= 2 {
			st.GetDel(key()) //nolint:errcheck
		}
	case "getex":
		if len(args) >= 2 {
			st.GetEx(key(), time.Time{}, false) //nolint:errcheck
		}
	case "setex":
		if len(args) >= 4 {
			sec, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err == nil && sec > 0 {
				st.SetEx(key(), args[3], time.Duration(sec)*time.Second)
			}
		}
	case "psetex":
		if len(args) >= 4 {
			ms, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err == nil && ms > 0 {
				st.SetEx(key(), args[3], time.Duration(ms)*time.Millisecond)
			}
		}
	case "setnx":
		if len(args) >= 3 {
			st.SetNX(key(), args[2])
		}
	case "setrange":
		if len(args) >= 4 {
			off, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err == nil {
				st.SetRange(key(), off, args[3])
			}
		}
	case "incrbyfloat":
		if len(args) >= 3 {
			if d, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
				st.IncrByFloat(key(), d)
			}
		}
	case "set":
		if len(args) < 3 {
			return nil
		}
		var nx, xx bool
		for i := 3; i < len(args); i++ {
			switch strings.ToUpper(string(args[i])) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			}
		}
		if nx && st.Exists(key()) == 1 {
			return nil
		}
		if xx && st.Exists(key()) == 0 {
			return nil
		}
		st.SetString(key(), args[2])
	case "append":
		if len(args) == 3 {
			st.Append(key(), args[2])
		}
	case "incr":
		st.IncrBy(key(), 1)
	case "decr":
		st.IncrBy(key(), -1)
	case "incrby":
		if len(args) == 3 {
			if d, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.IncrBy(key(), d)
			}
		}
	case "decrby":
		if len(args) == 3 {
			if d, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.IncrBy(key(), -d)
			}
		}
	case "del":
		st.Delete(args[1:]...)
	case "expire":
		if len(args) == 3 {
			if s, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				if s <= 0 {
					st.Delete(key())
				} else {
					st.Expire(key(), time.Duration(s)*time.Second)
				}
			}
		}
	case "pexpire":
		if len(args) == 3 {
			if ms, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				if ms <= 0 {
					st.Delete(key())
				} else {
					st.Expire(key(), time.Duration(ms)*time.Millisecond)
				}
			}
		}
	case "expireat":
		if len(args) == 3 {
			if ts, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.ExpireAt(key(), time.Unix(ts, 0))
			}
		}
	case "pexpireat":
		if len(args) == 3 {
			if ts, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.ExpireAt(key(), time.UnixMilli(ts))
			}
		}
	case "persist":
		st.Persist(key())
	case "flushdb":
		st.Flush()
	case "lpush":
		st.ListPush(key(), true, args[2:]...)
	case "rpush":
		st.ListPush(key(), false, args[2:]...)
	case "linsert":
		// LINSERT key BEFORE|AFTER pivot element
		if len(args) == 5 {
			before := strings.EqualFold(string(args[2]), "BEFORE")
			st.ListInsert(key(), before, args[3], args[4]) //nolint:errcheck
		}
	case "lrem":
		// LREM key count element
		if len(args) == 4 {
			if n, err := strconv.Atoi(string(args[2])); err == nil {
				st.ListRemove(key(), n, args[3]) //nolint:errcheck
			}
		}
	case "ltrim":
		// LTRIM key start stop
		if len(args) == 4 {
			start, err1 := strconv.Atoi(string(args[2]))
			stop, err2 := strconv.Atoi(string(args[3]))
			if err1 == nil && err2 == nil {
				st.ListTrim(key(), start, stop) //nolint:errcheck
			}
		}
	case "lmove":
		// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
		if len(args) == 5 {
			fromLeft := strings.EqualFold(string(args[3]), "LEFT")
			toLeft := strings.EqualFold(string(args[4]), "LEFT")
			st.ListMove(key(), args[2], fromLeft, toLeft) //nolint:errcheck
		}
	case "rpoplpush":
		// RPOPLPUSH source destination
		if len(args) == 3 {
			st.ListMove(key(), args[2], false, true) //nolint:errcheck
		}
	case "lpushx":
		// LPUSHX key element [element ...]
		if len(args) >= 3 {
			st.ListPushExist(key(), true, args[2:]) //nolint:errcheck
		}
	case "rpushx":
		// RPUSHX key element [element ...]
		if len(args) >= 3 {
			st.ListPushExist(key(), false, args[2:]) //nolint:errcheck
		}
	case "lpop":
		if len(args) == 2 {
			st.ListPop(key(), true)
		}
	case "rpop":
		if len(args) == 2 {
			st.ListPop(key(), false)
		}
	case "lset":
		if len(args) == 4 {
			if i, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.ListSet(key(), int(i), args[3])
			}
		}
	case "hset":
		if len(args) >= 4 && len(args)%2 == 0 {
			for i := 2; i+1 < len(args); i += 2 {
				st.HashSet(key(), args[i], args[i+1])
			}
		}
	case "hdel":
		st.HashDel(key(), args[2:]...)
	case "smove":
		if len(args) == 4 {
			st.SetMove(key(), args[2], args[3]) //nolint:errcheck
		}
	case "spop":
		if len(args) >= 2 {
			count := 0
			if len(args) == 3 {
				if n, err := strconv.Atoi(string(args[2])); err == nil {
					count = n
				}
			}
			st.SetPop(key(), count) //nolint:errcheck
		}
	case "sdiffstore", "sinterstore", "sunionstore":
		if len(args) >= 3 {
			var members [][]byte
			var err error
			switch name {
			case "sdiffstore":
				members, err = st.SetDiff(args[2:])
			case "sinterstore":
				members, err = st.SetInter(args[2:])
			default:
				members, err = st.SetUnion(args[2:])
			}
			if err == nil {
				st.SetStore(args[1], members) //nolint:errcheck
			}
		}
	case "sadd":
		st.SetAdd(key(), args[2:]...)
	case "srem":
		st.SetRem(key(), args[2:]...)
	case "zpopmin", "zpopmax":
		// ZPOPMIN|ZPOPMAX key [count]
		if len(args) >= 2 {
			count := 0
			if len(args) == 3 {
				if n, err := strconv.Atoi(string(args[2])); err == nil {
					count = n
				}
			}
			st.ZPop(key(), count, name == "zpopmin") //nolint:errcheck
		}
	case "zremrangebyrank":
		// ZREMRANGEBYRANK key start stop
		if len(args) == 4 {
			start, err1 := strconv.Atoi(string(args[2]))
			stop, err2 := strconv.Atoi(string(args[3]))
			if err1 == nil && err2 == nil {
				st.ZRemRangeByRank(key(), start, stop) //nolint:errcheck
			}
		}
	case "zunionstore", "zinterstore":
		// ZUNIONSTORE|ZINTERSTORE dest numkeys key [key ...]
		if len(args) >= 4 {
			if n, err := strconv.Atoi(string(args[2])); err == nil && n >= 1 && len(args) >= 3+n {
				keys := args[3 : 3+n]
				if name == "zunionstore" {
					st.ZUnionStore(args[1], keys, nil, store.AggregateSum) //nolint:errcheck
				} else {
					st.ZInterStore(args[1], keys, nil, store.AggregateSum) //nolint:errcheck
				}
			}
		}
	case "zdiffstore":
		// ZDIFFSTORE dest numkeys key [key ...]
		if len(args) >= 4 {
			if n, err := strconv.Atoi(string(args[2])); err == nil && n >= 1 && len(args) >= 3+n {
				st.ZDiffStore(args[1], args[3:3+n]) //nolint:errcheck
			}
		}
	case "zincrby":
		// ZINCRBY key increment member
		if len(args) == 4 {
			if d, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
				st.ZIncrBy(key(), d, args[3]) //nolint:errcheck
			}
		}
	case "zadd":
		if len(args) >= 4 && len(args)%2 == 0 {
			for i := 2; i+1 < len(args); i += 2 {
				if score, err := strconv.ParseFloat(string(args[i]), 64); err == nil {
					st.ZAdd(key(), score, args[i+1])
				}
			}
		}
	case "zrem":
		st.ZRem(key(), args[2:]...)
	}
	return nil
}

// RewriteAOF writes a compact command stream representing the current dataset
// to path (atomically). The stream starts with FLUSHALL so replay produces the
// dataset regardless of prior state.
func RewriteAOF(path string, stores []*store.Store, now time.Time) error {
	var buf bytes.Buffer
	if err := DatasetCommands(&buf, stores, now); err != nil {
		return err
	}
	return writeFileAtomic(path, buf.Bytes())
}

// DatasetCommands writes the command stream that rebuilds the given stores
// (starting with FLUSHALL) into w. Every database is preceded by a SELECT,
// so the replay restores all of them.
func DatasetCommands(w io.Writer, stores []*store.Store, now time.Time) error {
	if _, err := w.Write(EncodeCommand([][]byte{[]byte("FLUSHALL")})); err != nil {
		return err
	}
	for i, st := range stores {
		if err := datasetCommandsFor(w, st, i, now); err != nil {
			return err
		}
	}
	return nil
}

// datasetCommandsFor writes the commands that rebuild a single database.
func datasetCommandsFor(w io.Writer, st *store.Store, db int, now time.Time) error {
	if _, err := w.Write(EncodeCommand([][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))})); err != nil {
		return err
	}
	return st.ForEach(func(key string, e *store.Entry) error {
		switch e.Type {
		case store.TypeString:
			if _, err := w.Write(EncodeCommand([][]byte{[]byte("SET"), []byte(key), e.Str})); err != nil {
				return err
			}
		case store.TypeList:
			elems := e.List.Range(0, -1)
			for _, chunk := range chunkBytes(elems, 64) {
				args := [][]byte{[]byte("RPUSH"), []byte(key)}
				args = append(args, chunk...)
				if _, err := w.Write(EncodeCommand(args)); err != nil {
					return err
				}
			}
		case store.TypeHash:
			flat := make([][]byte, 0, len(e.Hash)*2)
			for f, v := range e.Hash {
				flat = append(flat, []byte(f), v)
			}
			for _, chunk := range chunkBytes(flat, 64) {
				args := [][]byte{[]byte("HSET"), []byte(key)}
				args = append(args, chunk...)
				if _, err := w.Write(EncodeCommand(args)); err != nil {
					return err
				}
			}
		case store.TypeSet:
			members := make([][]byte, 0, len(e.Set))
			for m := range e.Set {
				members = append(members, []byte(m))
			}
			for _, chunk := range chunkBytes(members, 64) {
				args := [][]byte{[]byte("SADD"), []byte(key)}
				args = append(args, chunk...)
				if _, err := w.Write(EncodeCommand(args)); err != nil {
					return err
				}
			}
		case store.TypeZSet:
			entries := e.ZSet.Range(0, -1, false)
			pairs := make([][]byte, 0, len(entries)*2)
			for _, ze := range entries {
				pairs = append(pairs, []byte(strconv.FormatFloat(ze.Score, 'g', -1, 64)), ze.Member)
			}
			for _, chunk := range chunkBytes(pairs, 64) {
				args := [][]byte{[]byte("ZADD"), []byte(key)}
				args = append(args, chunk...)
				if _, err := w.Write(EncodeCommand(args)); err != nil {
					return err
				}
			}
		}
		if !e.ExpiresAt.IsZero() {
			if _, err := w.Write(EncodeCommand([][]byte{[]byte("PEXPIREAT"), []byte(key), itoa(e.ExpiresAt.UnixMilli())})); err != nil {
				return err
			}
		}
		return nil
	})
}

// Rewrite replaces the AOF file with the content produced by gen while
// blocking concurrent appends, then reopens the file so new appends land in
// the rewritten file.
func (a *Appender) Rewrite(gen func(io.Writer) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var buf bytes.Buffer
	if err := gen(&buf); err != nil {
		return err
	}
	if err := writeFileAtomic(a.path, buf.Bytes()); err != nil {
		return err
	}
	if a.f != nil {
		a.f.Close()
	}
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("persist: reopen aof: %w", err)
	}
	a.f = f
	return nil
}

// writeFileAtomic writes data to path via a temp file, fsync and rename.
func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("persist: create temp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("persist: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("persist: rename: %w", err)
	}
	return nil
}

// chunkBytes splits b into chunks of at most n elements.
func chunkBytes(b [][]byte, n int) [][][]byte {
	if n <= 0 {
		return [][][]byte{b}
	}
	var out [][][]byte
	for len(b) > n {
		out = append(out, b[:n])
		b = b[n:]
	}
	if len(b) > 0 {
		out = append(out, b)
	}
	return out
}
