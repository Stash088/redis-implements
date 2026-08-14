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
func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.br.ReadSlice('\n')
		if err == nil {
			line = append(line, chunk...)
			break
		}
		if err == bufio.ErrBufferFull {
			line = append(line, chunk...)
			if len(line) > r.maxLineLen {
				return nil, ErrTooBig
			}
			continue
		}
		return nil, err
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
