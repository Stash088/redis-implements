package resp

import (
	"bufio"
	"io"
	"math"
	"net"
	"strconv"
)

// DefaultWriteBufferSize is the size of the socket write buffer. It is bigger
// than the read buffer on purpose: the replies of a pipelined batch are
// usually larger than the requests, and one write syscall per batch instead
// of one per few kilobytes is what makes pipelines pay off.
const DefaultWriteBufferSize = 64 * 1024

// bulkCRLF is the reply terminator reused by the direct bulk write.
var bulkCRLF = []byte("\r\n")

// Writer serializes RESP values to an underlying writer.
type Writer struct {
	bw *bufio.Writer
	// raw is the underlying writer (the connection): WriteBulkLarge sends
	// large payloads straight to it, bypassing the buffer.
	raw io.Writer
	// bufSize is the capacity of the buffer; payloads of this size and
	// bigger take the direct path.
	bufSize int
}

// NewWriter returns a Writer that writes RESP values to w using a default-sized
// buffer.
func NewWriter(w io.Writer) *Writer {
	return NewWriterSize(w, DefaultWriteBufferSize)
}

// NewWriterSize returns a Writer that writes RESP values to w using a buffer of
// the given size. Larger buffers reduce the number of underlying writes when
// many values are buffered between Flush calls.
func NewWriterSize(w io.Writer, size int) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, size), raw: w, bufSize: size}
}

// Flush flushes any buffered data to the underlying writer.
func (w *Writer) Flush() error {
	return w.bw.Flush()
}

// WriteSimpleString writes "+OK\r\n"-style values.
func (w *Writer) WriteSimpleString(s string) error {
	if err := w.bw.WriteByte('+'); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteError writes "-ERR ...\r\n"-style values.
func (w *Writer) WriteError(msg string) error {
	if err := w.bw.WriteByte('-'); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(msg); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteInteger writes ":123\r\n"-style values.
func (w *Writer) WriteInteger(n int64) error {
	if err := w.bw.WriteByte(':'); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(strconv.FormatInt(n, 10)); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteBulkString writes "$len\r\n<data>\r\n". A nil payload is written as a
// null bulk string ("$-1\r\n").
func (w *Writer) WriteBulkString(b []byte) error {
	if b == nil {
		return w.WriteNullBulk()
	}
	if err := w.writeLen('$', int64(len(b))); err != nil {
		return err
	}
	if _, err := w.bw.Write(b); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteBulkLarge writes a bulk string whose payload is large enough that
// copying it into the buffer would dominate the cost (a GET of a 64 KB value).
// The buffered data is flushed first to keep the byte order, then the header,
// the payload and the CRLF go to the underlying writer as one vector: on a
// TCP connection this is a single writev syscall and the payload is never
// copied into the buffer. Smaller payloads fall back to WriteBulkString.
func (w *Writer) WriteBulkLarge(b []byte) error {
	if len(b) < w.bufSize {
		return w.WriteBulkString(b)
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	var hdr [72]byte
	h := hdr[:0]
	h = append(h, '$')
	h = strconv.AppendInt(h, int64(len(b)), 10)
	h = append(h, '\r', '\n')
	bufs := net.Buffers{h, b, bulkCRLF}
	_, err := bufs.WriteTo(w.raw)
	return err
}

// WriteNullBulk writes "$-1\r\n".
func (w *Writer) WriteNullBulk() error {
	if _, err := w.bw.WriteString("$-1\r\n"); err != nil {
		return err
	}
	return nil
}

// WriteNullArray writes "*-1\r\n".
func (w *Writer) WriteNullArray() error {
	if _, err := w.bw.WriteString("*-1\r\n"); err != nil {
		return err
	}
	return nil
}

// WriteRaw writes pre-encoded RESP bytes directly to the buffer. It is used
// to splice responses (e.g. EXEC array elements) produced by other writers.
func (w *Writer) WriteRaw(b []byte) error {
	_, err := w.bw.Write(b)
	return err
}

// WriteArrayHeader writes the header "*N\r\n" of an array. Elements must be
// written next.
func (w *Writer) WriteArrayHeader(n int) error {
	return w.writeLen('*', int64(n))
}

// WriteMapHeader writes the header "%N\r\n" of a RESP3 map with n key/value
// pairs. The 2n elements must be written next.
func (w *Writer) WriteMapHeader(n int) error {
	return w.writeLen('%', int64(n))
}

// WriteSetHeader writes the header "~N\r\n" of a RESP3 set with n elements.
func (w *Writer) WriteSetHeader(n int) error {
	return w.writeLen('~', int64(n))
}

// WritePushHeader writes the header ">N\r\n" of a RESP3 push message.
func (w *Writer) WritePushHeader(n int) error {
	return w.writeLen('>', int64(n))
}

// WriteDouble writes a RESP3 double ",1.5\r\n". Infinities and NaN use the
// spellings Redis uses.
func (w *Writer) WriteDouble(f float64) error {
	var s string
	switch {
	case math.IsNaN(f):
		s = "nan"
	case math.IsInf(f, 1):
		s = "inf"
	case math.IsInf(f, -1):
		s = "-inf"
	default:
		s = strconv.FormatFloat(f, 'g', 17, 64)
	}
	if err := w.bw.WriteByte(','); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteBigNumber writes a RESP3 big number "(123...\r\n". The argument must
// hold the decimal digits.
func (w *Writer) WriteBigNumber(s string) error {
	if err := w.bw.WriteByte('('); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteVerbatim writes a RESP3 verbatim string "=N\r\n<payload>\r\n". The
// payload must include its format prefix ("txt:", "mkd:" or "json:").
func (w *Writer) WriteVerbatim(s string) error {
	if err := w.writeLen('=', int64(len(s))); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.writeCRLF()
}

// WriteValue writes any RESP value.
func (w *Writer) WriteValue(v Value) error {
	switch v.Kind {
	case KindSimpleString:
		return w.WriteSimpleString(string(v.Str))
	case KindError:
		return w.WriteError(string(v.Str))
	case KindInteger:
		return w.WriteInteger(v.Int)
	case KindBulkString:
		return w.WriteBulkString(v.Str)
	case KindArray:
		if v.IsNull {
			return w.WriteNullArray()
		}
		if err := w.WriteArrayHeader(len(v.Array)); err != nil {
			return err
		}
		for i := range v.Array {
			if err := w.WriteValue(v.Array[i]); err != nil {
				return err
			}
		}
		return nil
	case KindMap:
		if err := w.WriteMapHeader(len(v.Array) / 2); err != nil {
			return err
		}
		for i := range v.Array {
			if err := w.WriteValue(v.Array[i]); err != nil {
				return err
			}
		}
		return nil
	case KindSet:
		if err := w.WriteSetHeader(len(v.Array)); err != nil {
			return err
		}
		for i := range v.Array {
			if err := w.WriteValue(v.Array[i]); err != nil {
				return err
			}
		}
		return nil
	case KindPush:
		if err := w.WritePushHeader(len(v.Array)); err != nil {
			return err
		}
		for i := range v.Array {
			if err := w.WriteValue(v.Array[i]); err != nil {
				return err
			}
		}
		return nil
	case KindDouble:
		return w.WriteDouble(v.Float)
	case KindBigNumber:
		return w.WriteBigNumber(string(v.Str))
	case KindVerbatim:
		return w.WriteVerbatim(string(v.Str))
	default:
		return ErrInvalid
	}
}

func (w *Writer) writeLen(prefix byte, n int64) error {
	if err := w.bw.WriteByte(prefix); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(strconv.FormatInt(n, 10)); err != nil {
		return err
	}
	return w.writeCRLF()
}

func (w *Writer) writeCRLF() error {
	if err := w.bw.WriteByte('\r'); err != nil {
		return err
	}
	return w.bw.WriteByte('\n')
}
