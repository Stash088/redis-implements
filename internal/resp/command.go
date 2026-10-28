package resp

import (
	"strconv"
)

// ParseCommand parses a command from the buffer: a RESP array of bulk
// strings. It returns the arguments, the number of bytes the command takes
// and ok, which is false when the buffer holds only a part of a command.
//
// The payload of every argument is copied into a fresh buffer, because the
// store may keep it. The arguments are appended to dst, which the caller
// reuses between the commands.
//
// It is the counterpart of Reader.ReadCommand for the callers that read into
// their own buffer: the event loop reads a socket into a buffer and parses the
// commands from it.
func ParseCommand(buf []byte, dst [][]byte) ([][]byte, int, bool, error) {
	return ParseCommandLimits(buf, dst, DefaultMaxBulkLen, DefaultMaxArrayLen)
}

// ParseCommandLimits is ParseCommand with the given limits: a command longer
// than maxArrayLen arguments or an argument longer than maxBulkLen bytes is
// rejected.
func ParseCommandLimits(buf []byte, dst [][]byte, maxBulkLen, maxArrayLen int) (args [][]byte, n int, ok bool, err error) {
	if len(buf) < 1 {
		return nil, 0, false, nil
	}
	// The array header: *<count>\r\n
	if buf[0] != '*' {
		return nil, 0, false, ErrNotArray
	}
	line, used, ok := lineAt(buf[1:])
	if !ok {
		return nil, 0, false, nil
	}
	count, err := strconv.Atoi(string(line))
	if err != nil {
		return nil, 0, false, ErrInvalid
	}
	if count == -1 {
		// A null array is not a command.
		return nil, 0, false, ErrNotArray
	}
	if count < -1 {
		return nil, 0, false, ErrInvalid
	}
	if count > maxArrayLen {
		return nil, 0, false, ErrTooBig
	}
	p := 1 + used
	// The arguments: $<len>\r\n<payload>\r\n
	dst = dst[:0]
	for i := 0; i < count; i++ {
		if len(buf)-p < 1 {
			return nil, 0, false, nil
		}
		if buf[p] != '$' {
			return nil, 0, false, ErrNotBulk
		}
		line, used, ok := lineAt(buf[p+1:])
		if !ok {
			return nil, 0, false, nil
		}
		size, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, 0, false, ErrInvalid
		}
		if size == -1 {
			return nil, 0, false, ErrNotBulk
		}
		if size < -1 {
			return nil, 0, false, ErrInvalid
		}
		if size > maxBulkLen {
			return nil, 0, false, ErrTooBig
		}
		p += 1 + used
		if len(buf)-p < size+2 {
			return nil, 0, false, nil
		}
		arg := make([]byte, size)
		copy(arg, buf[p:p+size])
		p += size + 2
		dst = append(dst, arg)
	}
	return dst, p, true, nil
}

// lineAt returns the line at the start of buf (without the CRLF) and the
// number of bytes it takes with the CRLF. ok is false when the buffer holds
// no complete line.
func lineAt(buf []byte) (line []byte, consumed int, ok bool) {
	for i := 0; i+1 < len(buf); i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return buf[:i], i + 2, true
		}
	}
	return nil, 0, false
}
