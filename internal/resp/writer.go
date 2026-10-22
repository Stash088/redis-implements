package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer serializes RESP values to an underlying writer.
type Writer struct {
	bw *bufio.Writer
}

// NewWriter returns a Writer that writes RESP values to w using a default-sized
// buffer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// NewWriterSize returns a Writer that writes RESP values to w using a buffer of
// the given size. Larger buffers reduce the number of underlying writes when
// many values are buffered between Flush calls.
func NewWriterSize(w io.Writer, size int) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, size)}
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
