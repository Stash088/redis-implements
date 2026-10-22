package main

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/resp"
)

// readTimeout bounds how long the comparison waits for a reply, so a blocking
// command cannot hang the run.
const readTimeout = 10 * time.Second

// conn is a connection to a Redis server used by the comparison.
type conn struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer
}

// dial connects to a server and negotiates the protocol version.
func dial(addr string, proto int) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	co := &conn{c: c, r: resp.NewReader(c), w: resp.NewWriter(c)}
	if proto > 2 {
		if err := co.commandErr([]string{"HELLO", fmt.Sprint(proto)}); err != nil {
			c.Close()
			return nil, err
		}
	}
	return co, nil
}

// Close closes the connection.
func (co *conn) Close() { co.c.Close() }

// command sends a command and returns the decoded reply. An error reply of
// the server is returned as a Go error.
func (co *conn) command(args []string) (resp.Value, error) {
	if err := co.write(args); err != nil {
		return resp.Value{}, err
	}
	v, err := co.read()
	if err != nil {
		return resp.Value{}, err
	}
	if v.Kind == resp.KindError {
		return v, fmt.Errorf("%s", strings.TrimSpace(string(v.Str)))
	}
	return v, nil
}

// commandErr sends a command and reads its reply, turning an error reply into
// a Go error.
func (co *conn) commandErr(args []string) error {
	if err := co.write(args); err != nil {
		return err
	}
	v, err := co.read()
	if err != nil {
		return err
	}
	if v.Kind == resp.KindError {
		return fmt.Errorf("%s", strings.TrimSpace(string(v.Str)))
	}
	return nil
}

// write sends a command.
func (co *conn) write(args []string) error {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString([]byte(a))
	}
	if err := co.w.WriteValue(resp.NewArray(elems)); err != nil {
		return err
	}
	return co.w.Flush()
}

// read reads one reply.
func (co *conn) read() (resp.Value, error) {
	if err := co.c.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return resp.Value{}, err
	}
	return co.r.ReadValue()
}
