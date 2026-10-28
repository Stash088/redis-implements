//go:build !darwin && !linux

package main

import "errors"

// newPoller reports that the event loop is not supported on this system.
func newPoller() (poller, error) {
	return nil, errors.New("the event loop is supported on macOS (kqueue) and Linux (epoll) only")
}

// accept is not reached: newPoller fails first.
func accept(fd int) (int, error) {
	return -1, errors.New("the event loop is supported on macOS (kqueue) and Linux (epoll) only")
}
