package main

import (
	"net"
	"strconv"
	"syscall"
)

// event is one readiness event of the poller.
type event struct {
	fd       int
	readable bool
	writable bool
}

// poller waits for the readiness of the descriptors. The implementation is
// kqueue on BSD and macOS and epoll on Linux; both are used in the
// level-triggered mode, so a descriptor that stays ready is reported again.
type poller interface {
	// addRead starts watching the descriptor for reading.
	addRead(fd int) error
	// delRead stops watching the descriptor for reading.
	delRead(fd int) error
	// addWrite starts watching the descriptor for writing.
	addWrite(fd int) error
	// delWrite stops watching the descriptor for writing.
	delWrite(fd int) error
	// wait blocks until a descriptor is ready.
	wait() ([]event, error)
}

// serveLoop serves the commands from one event loop: the loop waits for the
// readiness of the sockets, reads the requests, runs the commands and writes
// the replies.
//
// The difference with the goroutine model: the thread stays in the wait of
// the poller, so a request costs no goroutine wake-up and no extra read
// syscall (the socket is watched in the level-triggered mode, so the read
// finds the data at once).
func serveLoop(addr string, k *kv) error {
	p, err := newPoller()
	if err != nil {
		return err
	}
	ln, err := listenSocket(addr)
	if err != nil {
		return err
	}
	if err := p.addRead(ln); err != nil {
		return err
	}
	conns := make(map[int]*connState)
	pending := make(map[int]bool) // the connections with an unfinished write
	for {
		events, err := p.wait()
		if err != nil {
			return err
		}
		for _, ev := range events {
			fd := ev.fd
			if fd == ln {
				for {
					cfd, err := accept(ln)
					if err != nil {
						break
					}
					conns[cfd] = newConnState()
					if err := p.addRead(cfd); err != nil {
						return err
					}
				}
				continue
			}
			st := conns[fd]
			if st == nil {
				continue
			}
			if ev.readable {
				// Read until the socket is drained: the poller watches it
				// in the level-triggered mode, so the data that is already
				// there must be taken at once.
				for {
					if st.pos == len(st.in) {
						st.compact()
					}
					if len(st.in) == cap(st.in) {
						grown := make([]byte, len(st.in), cap(st.in)*2)
						copy(grown, st.in)
						st.in = grown
					}
					n, err := syscall.Read(fd, st.in[len(st.in):cap(st.in)])
					if n > 0 {
						st.in = st.in[:len(st.in)+n]
						continue
					}
					if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
						break
					}
					// The connection is closed or broken.
					syscall.Close(fd)
					p.delRead(fd)
					delete(conns, fd)
					st = nil
					break
				}
				if st == nil {
					continue
				}
				if !process(k, st) {
					syscall.Close(fd)
					p.delRead(fd)
					delete(conns, fd)
					continue
				}
			}
			if len(st.out) == 0 {
				continue
			}
			// Write the replies.
			for len(st.out) > 0 {
				n, err := syscall.Write(fd, st.out)
				if n > 0 {
					st.out = st.out[n:]
					continue
				}
				if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
					// The socket is full: watch it for writing and stop
					// reading, so the replies do not pile up.
					pending[fd] = true
					p.delRead(fd)
					p.addWrite(fd)
					break
				}
				syscall.Close(fd)
				p.delRead(fd)
				delete(conns, fd)
				break
			}
			if pending[fd] && len(st.out) == 0 {
				// Everything is written: watch the socket for reading
				// again.
				delete(pending, fd)
				p.delWrite(fd)
				p.addRead(fd)
			}
		}
	}
}

// listenSocket creates a listening TCP socket in the non-blocking mode.
func listenSocket(addr string) (int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return -1, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return -1, err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return -1, &net.AddrError{Err: "the address must be an IP", Addr: host}
	}
	var domain int
	var sockaddr syscall.Sockaddr
	if v4 := ip.To4(); v4 != nil {
		domain = syscall.AF_INET
		sockaddr = &syscall.SockaddrInet4{Port: port}
		copy(sockaddr.(*syscall.SockaddrInet4).Addr[:], v4)
	} else {
		domain = syscall.AF_INET6
		sockaddr = &syscall.SockaddrInet6{Port: port}
		copy(sockaddr.(*syscall.SockaddrInet6).Addr[:], ip.To16())
	}
	fd, err := syscall.Socket(domain, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		return -1, err
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	if err := syscall.Bind(fd, sockaddr); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	if err := syscall.Listen(fd, 511); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}
