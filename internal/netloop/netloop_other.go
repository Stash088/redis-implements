//go:build !darwin && !linux

package netloop

import "errors"

// NewPoller reports that the poller is not supported on this system.
func NewPoller() (Poller, error) {
	return nil, errors.New("netloop: the poller is supported on macOS (kqueue) and Linux (epoll) only")
}
