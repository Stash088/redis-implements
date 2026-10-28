//go:build darwin

package main

import (
	"syscall"
)

// kqueuePoller waits for the readiness of the descriptors with kqueue.
//
// The descriptors are watched in the level-triggered mode (EV_ADD without
// EV_CLEAR): a socket that stays readable is reported again, so the loop can
// read the data at once instead of trying the read until it fails.
type kqueuePoller struct {
	fd int
}

func newPoller() (poller, error) {
	fd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	return &kqueuePoller{fd: fd}, nil
}

func (p *kqueuePoller) addRead(fd int) error {
	return p.change(fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_ENABLE)
}

func (p *kqueuePoller) delRead(fd int) error {
	return p.change(fd, syscall.EVFILT_READ, syscall.EV_DELETE)
}

func (p *kqueuePoller) addWrite(fd int) error {
	return p.change(fd, syscall.EVFILT_WRITE, syscall.EV_ADD|syscall.EV_ENABLE)
}

func (p *kqueuePoller) delWrite(fd int) error {
	return p.change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
}

func (p *kqueuePoller) change(fd int, filter int16, flags uint16) error {
	ev := syscall.Kevent_t{Ident: uint64(fd), Filter: filter, Flags: flags}
	_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{ev}, nil, nil)
	return err
}

func (p *kqueuePoller) wait() ([]event, error) {
	events := make([]syscall.Kevent_t, 64)
	for {
		n, err := syscall.Kevent(p.fd, nil, events, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return nil, err
		}
		out := make([]event, 0, n)
		for i := 0; i < n; i++ {
			e := events[i]
			ev := event{fd: int(e.Ident)}
			switch e.Filter {
			case syscall.EVFILT_READ:
				ev.readable = true
			case syscall.EVFILT_WRITE:
				ev.writable = true
			}
			out = append(out, ev)
		}
		return out, nil
	}
}

// accept accepts a connection of the listener. It returns an error when there
// is no connection to accept.
func accept(fd int) (int, error) {
	cfd, _, err := syscall.Accept(fd)
	if err != nil {
		return -1, err
	}
	return cfd, nil
}
