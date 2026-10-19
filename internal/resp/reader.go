package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Default limits chosen to match Redis behaviour while keeping a guard against
// malicious clients.
const (
	DefaultMaxBulkLen  = 512 * 1024 * 1024 // 512 MB, same as proto-max-bulk-len
	DefaultMaxLineLen  = 64 * 1024         // 64 KB, same as proto-max-query-buf-len
	DefaultMaxArrayLen = 1024 * 1024
)

var (
	// ErrTooBig is returned when an incoming message exceeds configured limits.
	ErrTooBig = errors.New("resp: message exceeds configured limit")
	// ErrInvalid is returned when an incoming message is malformed.
	ErrInvalid = errors.New("resp: malformed message")
)

// Reader decodes RESP values from an underlying reader.
type Reader struct {
	br          *bufio.Reader
	maxBulkLen  int
	maxLineLen  int
	maxArrayLen int
}

// NewReader returns a Reader that reads RESP values from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		br:          bufio.NewReader(r),
		maxBulkLen:  DefaultMaxBulkLen,
		maxLineLen:  DefaultMaxLineLen,
		maxArrayLen: DefaultMaxArrayLen,
	}
}

// SetMaxBulkLen overrides the maximum length of a single bulk string.
func (r *Reader) SetMaxBulkLen(n int) { r.maxBulkLen = n }

// SetMaxLineLen overrides the maximum length of a line (simple string, integer, header).
func (r *Reader) SetMaxLineLen(n int) { r.maxLineLen = n }

// SetMaxArrayLen overrides the maximum number of elements in an array.
func (r *Reader) SetMaxArrayLen(n int) { r.maxArrayLen = n }

// Buffered returns the number of bytes that are buffered but not yet read.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// HasBufferedValue reports whether the reader's buffer already holds a
// complete RESP value. The server uses it to batch the replies of pipelined
// commands into a single write: while more commands are already buffered
// there is no reason to flush the answers yet, and the client is not
// waiting for them either.
func (r *Reader) HasBufferedValue() bool {
	n := r.br.Buffered()
	if n == 0 {
		return false
	}
	// Peek never blocks here: n does not exceed the buffered bytes.
	b, err := r.br.Peek(n)
	if err != nil {
		return false
	}
	return valueLen(b) > 0
}

// valueLen returns the length of the first complete RESP value in b, or 0 when
// b holds only a fragment of one.
func valueLen(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	switch b[0] {
	case '+', '-', ':':
		i := indexCRLF(b)
		if i < 0 {
			return 0
		}
		return i + 2
	case '$':
		i := indexCRLF(b)
		if i < 0 {
			return 0
		}
		n, err := strconv.Atoi(string(b[1:i]))
		if err != nil {
			return 0
		}
		if n < 0 {
			return i + 2 // null bulk string
		}
		total := i + 2 + n + 2
		if len(b) < total {
			return 0
		}
		return total
	case '*':
		i := indexCRLF(b)
		if i < 0 {
			return 0
		}
		n, err := strconv.Atoi(string(b[1:i]))
		if err != nil {
			return 0
		}
		if n < 0 {
			return i + 2 // null array
		}
		pos := i + 2
		for k := 0; k < n; k++ {
			m := valueLen(b[pos:])
			if m == 0 {
				return 0
			}
			pos += m
		}
		return pos
	default:
		return 0
	}
}

// indexCRLF returns the index of the CR of the first CRLF in b, or -1 when b
// holds no complete line.
func indexCRLF(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' {
			return i
		}
	}
	return -1
}

// ReadValue reads a single RESP value from the stream.
func (r *Reader) ReadValue() (Value, error) {
	typ, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Kind(typ) {
	case KindSimpleString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return NewSimpleString(string(line)), nil
	case KindError:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return NewError(string(line)), nil
	case KindInteger:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%w: bad integer %q", ErrInvalid, line)
		}
		return NewInteger(n), nil
	case KindBulkString:
		return r.readBulk()
	case KindArray:
		return r.readArray(0)
	default:
		return Value{}, fmt.Errorf("%w: unknown type byte %q", ErrInvalid, typ)
	}
}

func (r *Reader) readBulk() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.Atoi(string(line))
	if err != nil {
		return Value{}, fmt.Errorf("%w: bad bulk length %q", ErrInvalid, line)
	}
	if n == -1 {
		return NewBulkString(nil), nil
	}
	if n < -1 {
		return Value{}, fmt.Errorf("%w: negative bulk length %d", ErrInvalid, n)
	}
	if n > r.maxBulkLen {
		return Value{}, fmt.Errorf("%w: bulk length %d exceeds limit %d", ErrTooBig, n, r.maxBulkLen)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return Value{}, err
	}
	if err := r.consumeCRLF(); err != nil {
		return Value{}, err
	}
	return NewBulkString(buf), nil
}

func (r *Reader) readArray(depth int) (Value, error) {
	if depth > 1024 {
		return Value{}, fmt.Errorf("%w: array nesting too deep", ErrInvalid)
	}
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.Atoi(string(line))
	if err != nil {
		return Value{}, fmt.Errorf("%w: bad array length %q", ErrInvalid, line)
	}
	if n == -1 {
		return NewArray(nil), nil
	}
	if n < -1 {
		return Value{}, fmt.Errorf("%w: negative array length %d", ErrInvalid, n)
	}
	if n > r.maxArrayLen {
		return Value{}, fmt.Errorf("%w: array length %d exceeds limit %d", ErrTooBig, n, r.maxArrayLen)
	}
	elems := make([]Value, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadValue()
		if err != nil {
			return Value{}, err
		}
		elems = append(elems, v)
	}
	return NewArray(elems), nil
}

// readLine reads up to and including CRLF, returning the line without the CRLF.
//
// The returned slice points into the reader's internal buffer and stays valid
// only until the next read, so every caller must copy the bytes it keeps.
// (All of them do: they convert the line to a string right away.)
func (r *Reader) readLine() ([]byte, error) {
	chunk, err := r.br.ReadSlice('\n')
	if err == nil {
		// The whole line fits in the buffer: return it without copying.
		if len(chunk) < 2 || chunk[len(chunk)-2] != '\r' {
			return nil, fmt.Errorf("%w: line not terminated by CRLF", ErrInvalid)
		}
		if len(chunk) > r.maxLineLen {
			return nil, ErrTooBig
		}
		return chunk[:len(chunk)-2], nil
	}
	if err != bufio.ErrBufferFull {
		return nil, err
	}
	// The line is longer than the buffer: accumulate the fragments.
	line := make([]byte, 0, len(chunk))
	line = append(line, chunk...)
	for {
		if len(line) > r.maxLineLen {
			return nil, ErrTooBig
		}
		chunk, err = r.br.ReadSlice('\n')
		if err == nil {
			line = append(line, chunk...)
			break
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
		line = append(line, chunk...)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: line not terminated by CRLF", ErrInvalid)
	}
	if len(line) > r.maxLineLen {
		return nil, ErrTooBig
	}
	return line[:len(line)-2], nil
}

func (r *Reader) consumeCRLF() error {
	cr, err := r.br.ReadByte()
	if err != nil {
		return err
	}
	if cr != '\r' {
		return fmt.Errorf("%w: expected CR after bulk payload", ErrInvalid)
	}
	lf, err := r.br.ReadByte()
	if err != nil {
		return err
	}
	if lf != '\n' {
		return fmt.Errorf("%w: expected LF after CR", ErrInvalid)
	}
	return nil
}
