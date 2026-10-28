package main

import (
	"net"
)

// serveGoroutine serves the commands with a goroutine per connection, like
// the server does: the goroutine sleeps in the read of the socket, and the
// runtime wakes it up when the data arrives.
//
// The read goes through the runtime's netpoller, which registers the socket in
// the edge-triggered mode: the read returns EAGAIN first, and only then the
// goroutine parks and waits for the readiness.
func serveGoroutine(addr string, k *kv) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			defer c.Close()
			st := newConnState()
			for {
				// Read: the runtime's netpoller waits for the data here.
				if st.pos == len(st.in) {
					st.compact()
				}
				if len(st.in) == cap(st.in) {
					// The buffer holds a command that is not complete and
					// there is no room left: grow it.
					grown := make([]byte, len(st.in), cap(st.in)*2)
					copy(grown, st.in)
					st.in = grown
				}
				n, err := c.Read(st.in[len(st.in):cap(st.in)])
				if n > 0 {
					st.in = st.in[:len(st.in)+n]
				}
				if !process(k, st) {
					return
				}
				if len(st.out) > 0 {
					if _, err := c.Write(st.out); err != nil {
						return
					}
					st.out = st.out[:0]
				}
				if err != nil {
					return
				}
			}
		}(conn)
	}
}
