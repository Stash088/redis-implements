package persist

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
//
// The commands are encoded into an internal buffer and written to the file in
// batches. Writing every command separately costs a system call per command,
// which dominated the cost of the write path: with the AOF enabled a
// pipelined batch ran at a third of the speed. Redis writes its AOF once per
// event loop iteration, which is the same idea.
type Appender struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	policy FsyncPolicy
	stop   chan struct{}
	done   chan struct{}

	// buf holds the commands that are encoded but not written to the file
	// yet.
	buf []byte
	// bufLimit is the size at which the buffer is written out.
	bufLimit int
	// flushEvery is the interval of the background flush. Zero writes the
	// commands only when the buffer is full.
	flushEvery time.Duration
	// curDB is the database the journal is in: when a command of another
	// database is appended, a SELECT is written before it. -1 forces the
	// SELECT (a fresh file and a rewrite do not have a database yet).
	curDB int
}

// DefaultAOFBufferSize is the size of the AOF write buffer.
const DefaultAOFBufferSize = 64 * 1024

// DefaultAOFFlushInterval is how often the buffered commands are written to
// the file. It is the window in which a crash can lose the commands that
// were not synced yet — Redis loses up to a second with the same policy.
const DefaultAOFFlushInterval = 100 * time.Millisecond

// OpenAppender opens (creating if needed) the AOF file at path.
func OpenAppender(path string, policy FsyncPolicy) (*Appender, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("persist: open aof: %w", err)
	}
	a := &Appender{
		f:          f,
		path:       path,
		policy:     policy,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		bufLimit:   DefaultAOFBufferSize,
		flushEvery: DefaultAOFFlushInterval,
		curDB:      -1,
	}
	go a.background()
	return a, nil
}

// SetFlushInterval sets how often the buffered commands are written to the
// file. Zero disables the background flush: the commands are written when the
// buffer is full (and on close).
func (a *Appender) SetFlushInterval(d time.Duration) {
	a.mu.Lock()
	a.flushEvery = d
	a.mu.Unlock()
}

// SetBufferSize sets the size at which the buffer is written out.
func (a *Appender) SetBufferSize(n int) {
	a.mu.Lock()
	if n > 0 {
		a.bufLimit = n
	}
	a.mu.Unlock()
}

// Append encodes one command and adds it to the write buffer.
//
// With FsyncAlways the command is written and synced before returning, so
// the durability guarantee does not depend on the buffer. With the other
// policies the command reaches the file with the next flush.
func (a *Appender) Append(args [][]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appendLocked(args)
	return a.appendFlushLocked()
}

// AppendInDB appends a command of the given database. The journal keeps the
// database of the last appended command: a SELECT is written before the
// command when it belongs to another one, so the replay follows the same
// order of databases the live server had. This replaces the SELECT records
// of the client connections: the journal is one ordered stream, and its
// database is the one of the last written command.
func (a *Appender) AppendInDB(db int, args [][]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if db != a.curDB {
		a.appendLocked([][]byte{[]byte("select"), []byte(strconv.Itoa(db))})
		a.curDB = db
	}
	a.appendLocked(args)
	return a.appendFlushLocked()
}

// appendLocked encodes one command into the buffer. The caller must hold
// a.mu.
func (a *Appender) appendLocked(args [][]byte) {
	a.buf = resp.AppendCommand(a.buf, args)
}

// appendFlushLocked writes the buffer out per the fsync policy. The caller
// must hold a.mu.
func (a *Appender) appendFlushLocked() error {
	if a.policy == FsyncAlways {
		if err := a.flushLocked(); err != nil {
			return err
		}
		if err := a.f.Sync(); err != nil {
			return fmt.Errorf("persist: aof fsync: %w", err)
		}
		return nil
	}
	if len(a.buf) >= a.bufLimit {
		return a.flushLocked()
	}
	return nil
}

// Flush writes the buffered commands to the file.
func (a *Appender) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.flushLocked()
}

// flushLocked writes the buffer out. The caller must hold a.mu.
func (a *Appender) flushLocked() error {
	if len(a.buf) == 0 {
		return nil
	}
	if _, err := a.f.Write(a.buf); err != nil {
		return fmt.Errorf("persist: aof write: %w", err)
	}
	a.buf = a.buf[:0]
	return nil
}

// Sync writes the buffered commands to the file and flushes the file to
// disk.
func (a *Appender) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.flushLocked(); err != nil {
		return err
	}
	return a.f.Sync()
}

// Close stops the background goroutine, writes the buffered commands and
// closes the file.
func (a *Appender) Close() error {
	close(a.stop)
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.flushLocked(); err != nil {
		a.f.Close()
		return err
	}
	return a.f.Close()
}

// background writes the buffer out and syncs the file, as the policy asks.
func (a *Appender) background() {
	defer close(a.done)
	var flushC <-chan time.Time
	if d := a.flushInterval(); d > 0 {
		t := time.NewTicker(d)
		defer t.Stop()
		flushC = t.C
	}
	var syncC <-chan time.Time
	if a.policy == FsyncEverySec {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		syncC = t.C
	}
	for {
		select {
		case <-a.stop:
			return
		case <-flushC:
			if err := a.Flush(); err != nil {
				log.Printf("persist: aof flush: %v", err)
			}
		case <-syncC:
			if err := a.Sync(); err != nil {
				log.Printf("persist: aof sync: %v", err)
			}
		}
	}
}

func (a *Appender) flushInterval() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.flushEvery
}

// RewriteForAOF normalizes a write command for safe replay. Relative TTLs
// (SET EX/PX, EXPIRE, PEXPIRE) are converted to absolute PEXPIREAT so replay
// reproduces the original expiry instead of re-arming it from load time.
//
// The command name and its options are compared as bytes: this runs for every
// write command, and strings.ToLower would allocate a string each time.
func RewriteForAOF(args [][]byte, now time.Time) [][][]byte {
	if len(args) == 0 {
		return nil
	}
	name := args[0]
	switch {
	case equalFoldASCII(name, "set"):
		if len(args) < 3 {
			return [][][]byte{args}
		}
		key, val := args[1], args[2]
		var ttl time.Duration
		var flags [][]byte
		for i := 3; i < len(args); i++ {
			switch {
			case equalFoldASCII(args[i], "EX"):
				if i+1 < len(args) {
					if sec, err := strconv.ParseInt(string(args[i+1]), 10, 64); err == nil && sec > 0 {
						ttl = time.Duration(sec) * time.Second
					}
					i++
				}
			case equalFoldASCII(args[i], "PX"):
				if i+1 < len(args) {
					if ms, err := strconv.ParseInt(string(args[i+1]), 10, 64); err == nil && ms > 0 {
						ttl = time.Duration(ms) * time.Millisecond
					}
					i++
				}
			case equalFoldASCII(args[i], "NX"), equalFoldASCII(args[i], "XX"),
				equalFoldASCII(args[i], "KEEPTTL"):
				// KEEPTTL is state-relevant: without it the replay would drop
				// the expiry of the key.
				flags = append(flags, upperASCII(args[i]))
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
	case equalFoldASCII(name, "setex"):
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
	case equalFoldASCII(name, "psetex"):
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
	case equalFoldASCII(name, "expire"):
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
	case equalFoldASCII(name, "pexpire"):
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

// equalFoldASCII compares b with s, ignoring the case of the ASCII letters.
// It does not allocate, unlike strings.ToLower or strings.EqualFold on a
// string built from the bytes.
func equalFoldASCII(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		d := s[i]
		if d >= 'A' && d <= 'Z' {
			d += 'a' - 'A'
		}
		if c != d {
			return false
		}
	}
	return true
}

// upperASCII returns the upper case spelling of b. The original slice is
// returned when it is already upper case, so the common case does not
// allocate.
func upperASCII(b []byte) []byte {
	upper := true
	for _, c := range b {
		if c >= 'a' && c <= 'z' {
			upper = false
			break
		}
	}
	if upper {
		return b
	}
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out[i] = c
	}
	return out
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
	if _, err := w.Write(resp.EncodeCommand([][]byte{[]byte("FLUSHALL")})); err != nil {
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
	if _, err := w.Write(resp.EncodeCommand([][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))})); err != nil {
		return err
	}
	return st.ForEach(func(key string, e *store.Entry) error {
		switch e.Type {
		case store.TypeString:
			if _, err := w.Write(resp.EncodeCommand([][]byte{[]byte("SET"), []byte(key), e.Str})); err != nil {
				return err
			}
		case store.TypeList:
			elems := e.List.Range(0, -1)
			for _, chunk := range chunkBytes(elems, 64) {
				args := [][]byte{[]byte("RPUSH"), []byte(key)}
				args = append(args, chunk...)
				if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
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
				if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
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
				if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
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
				if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
					return err
				}
			}
		case store.TypeStream:
			// Every entry is written as an XADD with an explicit identifier,
			// so that the replay rebuilds the same stream.
			for _, entry := range e.Stream.Entries() {
				args := [][]byte{[]byte("XADD"), []byte(key), []byte(entry.ID.String())}
				args = append(args, entry.Fields...)
				if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
					return err
				}
			}
			// The consumer groups are rebuilt as well: the group with its
			// identifier and the entries of its pending list.
			groups, _, err := st.StreamGroups([]byte(key))
			if err != nil {
				return err
			}
			for _, g := range groups {
				create := [][]byte{
					[]byte("XGROUP"), []byte("CREATE"), []byte(key), []byte(g.Info.Name),
					[]byte(g.Info.LastID.String()),
					[]byte("ENTRIESREAD"), []byte(strconv.FormatUint(g.Info.EntriesRead, 10)),
				}
				if _, err := w.Write(resp.EncodeCommand(create)); err != nil {
					return err
				}
				for _, p := range g.Pending {
					claim := [][]byte{
						[]byte("XCLAIM"), []byte(key), []byte(g.Info.Name), []byte(p.Consumer), []byte("0"),
						[]byte(p.ID.String()),
						[]byte("TIME"), []byte(strconv.FormatInt(p.DeliveryTime.UnixMilli(), 10)),
						[]byte("RETRYCOUNT"), []byte(strconv.FormatInt(p.DeliveryCount, 10)),
						[]byte("FORCE"), []byte("JUSTID"),
					}
					if _, err := w.Write(resp.EncodeCommand(claim)); err != nil {
						return err
					}
				}
			}
		}
		if !e.ExpiresAt.IsZero() {
			if _, err := w.Write(resp.EncodeCommand([][]byte{[]byte("PEXPIREAT"), []byte(key), itoa(e.ExpiresAt.UnixMilli())})); err != nil {
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
	// The pending commands are part of the dataset the rewrite generates, so
	// the buffer is dropped: writing it after the rewrite would duplicate
	// them.
	if err := a.flushLocked(); err != nil {
		return err
	}
	a.buf = a.buf[:0]
	// The rewritten journal starts with FLUSHALL and its own SELECT records:
	// the next appended command is preceded by a SELECT again.
	a.curDB = -1
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
