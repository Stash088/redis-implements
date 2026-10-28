//go:build darwin || linux

package netloop

import "syscall"

// wakeup is the channel that wakes a poller waiting for the readiness of the
// descriptors.
//
// The event the loop waits for may come from another goroutine: a message of
// a subscriber, a client that was unblocked. The poller has to return to the
// caller, so the notification travels through a socket pair: the reading end is
// watched by the poller, the writing end is used to notify it.
type wakeup struct {
	read  int
	write int
}

// newWakeup creates the channel that wakes the poller.
func newWakeup() (*wakeup, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	for _, fd := range fds {
		syscall.CloseOnExec(fd)
		// The channel must not block the writer: a notification that
		// nobody reads yet is dropped instead of stopping the goroutine.
		if err := syscall.SetNonblock(fd, true); err != nil {
			syscall.Close(fds[0])
			syscall.Close(fds[1])
			return nil, err
		}
	}
	return &wakeup{read: fds[0], write: fds[1]}, nil
}

// notify wakes the poller.
//
// The channel holds a single byte: a notification that arrives while the
// previous one was not read yet is not lost, because the poller reads the
// byte and the reader of the channel sees the data that is left.
func (w *wakeup) notify() {
	var b [1]byte
	for {
		_, err := syscall.Write(w.write, b[:])
		if err == nil {
			return
		}
		if err == syscall.EINTR {
			continue
		}
		// The channel is full: a notification is already pending, so the
		// poller will wake up anyway.
		return
	}
}

// drain reads the pending notifications.
func (w *wakeup) drain() {
	var b [64]byte
	for {
		n, err := syscall.Read(w.read, b[:])
		if n <= 0 || err != nil {
			return
		}
	}
}

// close closes both ends of the channel.
func (w *wakeup) close() {
	syscall.Close(w.read)
	syscall.Close(w.write)
}
