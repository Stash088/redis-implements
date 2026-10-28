// Command eventloop-proto measures the cost of the I/O model.
//
// It serves a few commands in two ways:
//
//	goroutine — a goroutine per connection over net.Conn, like the server does;
//	loop     — one event loop over kqueue (BSD/macOS) or epoll (Linux).
//
// Everything else is shared: the same RESP parser, the same commands, the same
// replies, the same data. The difference between the two runs is therefore the
// cost of the model itself: the extra read syscall and the goroutine wake-up
// that the goroutine model pays on every request.
//
// The tool is an experiment, not part of the server.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

func main() {
	model := flag.String("model", "goroutine", "I/O model: goroutine or loop")
	addr := flag.String("addr", "127.0.0.1:6400", "address to listen on")
	flag.Parse()

	// The counters of the process are reported when the tool is stopped:
	// they give the cost of one command without the noise of ps.
	report = reportUsage
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		reportUsage()
		os.Exit(0)
	}()

	k := &kv{m: make(map[string]string)}
	var err error
	switch *model {
	case "goroutine":
		err = serveGoroutine(*addr, k)
	case "loop":
		err = serveLoop(*addr, k)
	default:
		err = fmt.Errorf("unknown model %q", *model)
	}
	if err != nil {
		log.Fatalf("eventloop-proto: %v", err)
	}
	os.Exit(0)
}

// kv is a tiny key/value store: the same work per command in both models.
type kv struct {
	mu sync.RWMutex
	m  map[string]string
}

func (k *kv) set(key, val string) {
	k.mu.Lock()
	k.m[key] = val
	k.mu.Unlock()
}

func (k *kv) get(key string) (string, bool) {
	k.mu.RLock()
	v, ok := k.m[key]
	k.mu.RUnlock()
	return v, ok
}

// bufSize is the size of the read buffer of a connection: the same as the
// server uses.
const bufSize = 64 * 1024

// connState is the state of a connection: the read buffer, the reply buffer
// and the argument buffer. Both models use it.
type connState struct {
	in   []byte
	pos  int
	out  []byte
	args [][]byte
	// n counts the commands of the connection: the counters are per
	// connection, so they do not add contention to the measurement.
	n int
}

func newConnState() *connState {
	return &connState{in: make([]byte, 0, bufSize), out: make([]byte, 0, 4096)}
}

// compact moves the unread bytes to the front of the buffer.
func (c *connState) compact() {
	if c.pos == 0 {
		return
	}
	copy(c.in, c.in[c.pos:])
	c.in = c.in[:len(c.in)-c.pos]
	c.pos = 0
}

// parseCommand parses the next command of the buffer: a RESP array of bulk
// strings. The arguments are copied out, because the store may keep them.
//
// ok is false when the buffer holds only a part of a command.
func parseCommand(buf []byte, dst [][]byte) (args [][]byte, n int, ok bool, err error) {
	p := 0
	if len(buf) < 1 {
		return nil, 0, false, nil
	}
	if buf[p] != '*' {
		return nil, 0, false, errors.New("expected an array")
	}
	eol := bytes.IndexByte(buf[p:], '\n')
	if eol < 0 {
		return nil, 0, false, nil
	}
	if eol < 2 || buf[p+eol-1] != '\r' {
		return nil, 0, false, errors.New("expected CRLF")
	}
	count, err := strconv.Atoi(string(buf[p+1 : p+eol-1]))
	if err != nil {
		return nil, 0, false, errors.New("bad array length")
	}
	p += eol + 1
	dst = dst[:0]
	for i := 0; i < count; i++ {
		if len(buf)-p < 1 {
			return nil, 0, false, nil
		}
		if buf[p] != '$' {
			return nil, 0, false, errors.New("expected a bulk string")
		}
		eol := bytes.IndexByte(buf[p:], '\n')
		if eol < 0 {
			return nil, 0, false, nil
		}
		size, err := strconv.Atoi(string(buf[p+1 : p+eol-1]))
		if err != nil {
			return nil, 0, false, errors.New("bad bulk length")
		}
		p += eol + 1
		if len(buf)-p < size+2 {
			return nil, 0, false, nil
		}
		arg := make([]byte, size)
		copy(arg, buf[p:p+size])
		p += size + 2
		dst = append(dst, arg)
	}
	return dst, p, true, nil
}

// execute runs a command and appends the reply to the buffer.
func execute(k *kv, args [][]byte, out []byte) []byte {
	if len(args) == 0 {
		return append(out, "-ERR empty command\r\n"...)
	}
	switch name := string(args[0]); {
	case equalFold(name, "PING"):
		out = append(out, "+PONG\r\n"...)
	case equalFold(name, "SET") && len(args) >= 3:
		k.set(string(args[1]), string(args[2]))
		out = append(out, "+OK\r\n"...)
	case equalFold(name, "GET") && len(args) >= 2:
		v, ok := k.get(string(args[1]))
		if !ok {
			return append(out, "$-1\r\n"...)
		}
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(v)), 10)
		out = append(out, '\r', '\n')
		out = append(out, v...)
		out = append(out, '\r', '\n')
	default:
		out = append(out, "-ERR unknown command\r\n"...)
	}
	return out
}

// process runs the commands of the buffer and writes the replies to the reply
// buffer. It returns false when the connection must be closed.
func process(k *kv, c *connState) bool {
	for {
		args, used, ok, err := parseCommand(c.in[c.pos:], c.args)
		if err != nil {
			return false
		}
		if !ok {
			return true
		}
		c.args = args
		c.pos += used
		c.n++
		commands.Add(1)
		c.out = execute(k, args, c.out)
	}
}

// equalFold compares a command name with the wanted one, ignoring the case:
// the commands of Redis are case-insensitive.
func equalFold(name, want string) bool {
	if len(name) != len(want) {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != want[i] {
			return false
		}
	}
	return true
}

// commands counts the commands served by the process.
var commands atomic.Int64

// report is called when the tool stops: it prints the counters of the
// process, so the cost of a command can be measured without ps.
var report func()

// reportUsage prints the number of the commands and the CPU time of the
// process.
func reportUsage() {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		fmt.Printf("commands=%d rusage: %v\n", commands.Load(), err)
		return
	}
	user := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	fmt.Printf("commands=%d user=%.3fs sys=%.3fs cpu=%.3fs\n", commands.Load(), user, sys, user+sys)
}
