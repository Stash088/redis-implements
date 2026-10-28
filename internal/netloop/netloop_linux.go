//go:build linux

package netloop

import (
	"net"
	"strconv"
	"syscall"
	"time"
)

// epollPoller waits for the readiness of the descriptors with epoll.
//
// The descriptors are watched in the level-triggered mode (EPOLLIN without
// EPOLLET): a socket that stays readable is reported again, so the loop can
// read the data at once instead of trying the read until it fails. The
// netpoller of the runtime uses the edge-triggered mode, and that is why the
// goroutine model pays an extra read syscall per request.
type epollPoller struct {
	fd int
	// wake wakes the poller from another goroutine.
	wake *wakeup
}

// NewPoller returns a poller over epoll.
func NewPoller() (Poller, error) {
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	w, err := newWakeup()
	if err != nil {
		syscall.Close(fd)
		return nil, err
	}
	p := &epollPoller{fd: fd, wake: w}
	if err := p.AddRead(w.read); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Close releases the poller.
func (p *epollPoller) Close() error {
	p.wake.close()
	return syscall.Close(p.fd)
}

// Notify wakes a poller that waits.
func (p *epollPoller) Notify() error {
	p.wake.notify()
	return nil
}

func (p *epollPoller) AddRead(fd int) error {
	return p.change(fd, syscall.EPOLLIN, syscall.EPOLL_CTL_ADD)
}

func (p *epollPoller) DelRead(fd int) error {
	return p.change(fd, syscall.EPOLLIN, syscall.EPOLL_CTL_DEL)
}

func (p *epollPoller) AddWrite(fd int) error {
	return p.change(fd, syscall.EPOLLOUT, syscall.EPOLL_CTL_ADD)
}

func (p *epollPoller) DelWrite(fd int) error {
	return p.change(fd, syscall.EPOLLOUT, syscall.EPOLL_CTL_DEL)
}

func (p *epollPoller) change(fd int, events uint32, op int) error {
	return syscall.EpollCtl(p.fd, op, fd, &syscall.EpollEvent{Events: events, Fd: int32(fd)})
}

func (p *epollPoller) Wait(timeout time.Duration) ([]Event, error) {
	events := make([]syscall.EpollEvent, 64)
	for {
		n, err := syscall.EpollWait(p.fd, events, waitMillis(timeout))
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return nil, err
		}
		out := make([]Event, 0, n)
		for i := 0; i < n; i++ {
			e := events[i]
			ev := Event{FD: int(e.Fd)}
			if e.Events&syscall.EPOLLIN != 0 {
				ev.Readable = true
			}
			if e.Events&syscall.EPOLLOUT != 0 {
				ev.Writable = true
			}
			if ev.FD == p.wake.read {
				// The notification is not a socket event: the poller
				// reads it and reports it as a wakeup.
				p.wake.drain()
				ev.Readable = false
				ev.Wakeup = true
			}
			out = append(out, ev)
		}
		return out, nil
	}
}

// listen creates a listening TCP socket in the non-blocking mode.
func listen(addr string) (int, error) {
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
		sa := &syscall.SockaddrInet4{Port: port}
		copy(sa.Addr[:], v4)
		sockaddr = sa
	} else {
		domain = syscall.AF_INET6
		sa := &syscall.SockaddrInet6{Port: port}
		copy(sa.Addr[:], ip.To16())
		sockaddr = sa
	}
	fd, err := syscall.Socket(domain, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, syscall.IPPROTO_TCP)
	if err != nil {
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

// accept accepts a connection of the listener.
func accept(fd int) (int, error) {
	cfd, _, err := syscall.Accept4(fd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
	if err != nil {
		return -1, err
	}
	return cfd, nil
}
