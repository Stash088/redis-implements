// Package netloop waits for the readiness of the sockets.
//
// It is the foundation of the event loop: the loop reads the requests of the
// ready sockets, runs the commands and writes the replies. The sockets are
// watched in the level-triggered mode, so a socket that stays ready is
// reported again and the loop can read the data at once — the goroutine model
// pays an extra read syscall and a goroutine wake-up on every request.
//
// The implementation is kqueue on BSD and macOS and epoll on Linux.
package netloop

import (
	"errors"
	"net"
	"strconv"
	"syscall"
)

// Event is one readiness event of the poller.
type Event struct {
	// FD is the descriptor the event is about.
	FD int
	// Readable is set when the descriptor has data to read.
	Readable bool
	// Writable is set when the descriptor is ready to accept the data.
	Writable bool
}

// Poller waits for the readiness of the descriptors.
type Poller interface {
	// AddRead starts watching the descriptor for reading.
	AddRead(fd int) error
	// DelRead stops watching the descriptor for reading.
	DelRead(fd int) error
	// AddWrite starts watching the descriptor for writing.
	AddWrite(fd int) error
	// DelWrite stops watching the descriptor for writing.
	DelWrite(fd int) error
	// Wait blocks until a descriptor is ready. It returns the events of the
	// ready descriptors.
	Wait() ([]Event, error)
}

// Listen creates a listening TCP socket in the non-blocking mode.
func Listen(addr string) (int, error) {
	return listen(addr)
}

// Accept accepts a connection of the listener. It returns an error when there
// is no connection to accept: the listener is in the non-blocking mode, so
// the error is the "would block" one.
func Accept(fd int) (int, error) {
	return accept(fd)
}

// Read reads the data of the socket into buf. It returns the "would block"
// error when there is no data.
func Read(fd int, buf []byte) (int, error) {
	return syscall.Read(fd, buf)
}

// Write writes the data to the socket. It returns the "would block" error
// when the socket is full.
func Write(fd int, buf []byte) (int, error) {
	return syscall.Write(fd, buf)
}

// Close closes the descriptor.
func Close(fd int) error { return syscall.Close(fd) }

// WouldBlock reports whether the error means that the operation would block:
// the caller waits for the readiness instead of retrying.
func WouldBlock(err error) bool {
	return err == syscall.EAGAIN || err == syscall.EWOULDBLOCK
}

// Addr returns the address of a socket.
func Addr(fd int) (string, error) {
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		return "", err
	}
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return net.JoinHostPort(net.IP(a.Addr[:]).String(), strconv.Itoa(a.Port)), nil
	case *syscall.SockaddrInet6:
		return net.JoinHostPort(net.IP(a.Addr[:]).String(), strconv.Itoa(a.Port)), nil
	default:
		return "", errors.New("netloop: unknown socket address")
	}
}
