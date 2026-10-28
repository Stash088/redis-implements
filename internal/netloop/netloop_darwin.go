//go:build darwin

package netloop

import (
	"net"
	"strconv"
	"syscall"
	"time"
)

// kqueuePoller waits for the readiness of the descriptors with kqueue.
//
// The descriptors are watched in the level-triggered mode (EV_ADD without
// EV_CLEAR): a socket that stays readable is reported again, so the loop can
// read the data at once instead of trying the read until it fails. The
// netpoller of the runtime uses the edge-triggered mode, and that is why the
// goroutine model pays an extra read syscall per request.
type kqueuePoller struct {
	fd int
	// wake wakes the poller from another goroutine.
	wake *wakeup
}

// NewPoller returns a poller over kqueue.
func NewPoller() (Poller, error) {
	fd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	w, err := newWakeup()
	if err != nil {
		syscall.Close(fd)
		return nil, err
	}
	p := &kqueuePoller{fd: fd, wake: w}
	if err := p.AddRead(w.read); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Close releases the poller.
func (p *kqueuePoller) Close() error {
	p.wake.close()
	return syscall.Close(p.fd)
}

// Notify wakes a poller that waits.
func (p *kqueuePoller) Notify() error {
	p.wake.notify()
	return nil
}

func (p *kqueuePoller) AddRead(fd int) error {
	return p.change(fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_ENABLE)
}

func (p *kqueuePoller) DelRead(fd int) error {
	return p.change(fd, syscall.EVFILT_READ, syscall.EV_DELETE)
}

func (p *kqueuePoller) AddWrite(fd int) error {
	return p.change(fd, syscall.EVFILT_WRITE, syscall.EV_ADD|syscall.EV_ENABLE)
}

func (p *kqueuePoller) DelWrite(fd int) error {
	return p.change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
}

func (p *kqueuePoller) change(fd int, filter int16, flags uint16) error {
	ev := syscall.Kevent_t{Ident: uint64(fd), Filter: filter, Flags: flags}
	_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

func (p *kqueuePoller) Wait(timeout time.Duration) ([]Event, error) {
	events := make([]syscall.Kevent_t, 64)
	for {
		n, err := syscall.Kevent(p.fd, nil, events, waitTimespec(timeout))
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return nil, err
		}
		out := make([]Event, 0, n)
		for i := 0; i < n; i++ {
			e := events[i]
			ev := Event{FD: int(e.Ident)}
			switch e.Filter {
			case syscall.EVFILT_READ:
				ev.Readable = true
			case syscall.EVFILT_WRITE:
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

// waitTimespec converts a timeout into the timespec of kevent. A negative
// timeout means "wait forever" (no timespec at all).
func waitTimespec(timeout time.Duration) *syscall.Timespec {
	if timeout < 0 {
		return nil
	}
	ts := syscall.NsecToTimespec(timeout.Nanoseconds())
	return &ts
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

// accept accepts a connection of the listener.
func accept(fd int) (int, error) {
	cfd, _, err := syscall.Accept(fd)
	if err != nil {
		return -1, err
	}
	if err := syscall.SetNonblock(cfd, true); err != nil {
		syscall.Close(cfd)
		return -1, err
	}
	return cfd, nil
}
