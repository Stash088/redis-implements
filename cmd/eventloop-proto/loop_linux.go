//go:build linux

package main

import (
	"syscall"
)

// epollPoller waits for the readiness of the descriptors with epoll.
//
// The descriptors are watched in the level-triggered mode (EPOLLIN without
// EPOLLET): a socket that stays readable is reported again, so the loop can
// read the data at once instead of trying the read until it fails.
type epollPoller struct {
	fd int
}

func newPoller() (poller, error) {
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &epollPoller{fd: fd}, nil
}

func (p *epollPoller) addRead(fd int) error {
	return p.change(fd, syscall.EPOLLIN, syscall.EPOLL_CTL_ADD)
}

func (p *epollPoller) delRead(fd int) error {
	return p.change(fd, syscall.EPOLLIN, syscall.EPOLL_CTL_DEL)
}

func (p *epollPoller) addWrite(fd int) error {
	return p.change(fd, syscall.EPOLLOUT, syscall.EPOLL_CTL_ADD)
}

func (p *epollPoller) delWrite(fd int) error {
	return p.change(fd, syscall.EPOLLOUT, syscall.EPOLL_CTL_DEL)
}

func (p *epollPoller) change(fd int, events uint32, op int) error {
	return syscall.EpollCtl(p.fd, op, fd, &syscall.EpollEvent{Events: events, Fd: int32(fd)})
}

func (p *epollPoller) wait() ([]event, error) {
	events := make([]syscall.EpollEvent, 64)
	for {
		n, err := syscall.EpollWait(p.fd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return nil, err
		}
		out := make([]event, 0, n)
		for i := 0; i < n; i++ {
			e := events[i]
			ev := event{fd: int(e.Fd)}
			if e.Events&syscall.EPOLLIN != 0 {
				ev.readable = true
			}
			if e.Events&syscall.EPOLLOUT != 0 {
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
	cfd, _, err := syscall.Accept4(fd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
	if err != nil {
		return -1, err
	}
	return cfd, nil
}
