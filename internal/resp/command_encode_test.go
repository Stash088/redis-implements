package resp

import (
	"bytes"
	"testing"
)

// TestEncodeCommand checks the wire bytes of the command encoder.
func TestEncodeCommand(t *testing.T) {
	b := EncodeCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	if !bytes.HasPrefix(b, []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")) {
		t.Errorf("bad encoding: %q", b)
	}
}

// TestAppendCommandMatchesWriter checks that the hand written encoder
// produces the same bytes as the RESP writer.
func TestAppendCommandMatchesWriter(t *testing.T) {
	cases := [][][]byte{
		{[]byte("SET"), []byte("k"), []byte("v")},
		{[]byte("SET"), []byte("k"), []byte("")},
		{[]byte("RPUSH"), []byte("l"), []byte("a"), []byte("b")},
		{[]byte("SET"), []byte("k"), []byte("v\x00\r\n")},
	}
	for _, c := range cases {
		got := AppendCommand(nil, c)
		var buf bytes.Buffer
		w := NewWriter(&buf)
		elems := make([]Value, len(c))
		for i, a := range c {
			elems[i] = NewBulkString(a)
		}
		if err := w.WriteValue(NewArray(elems)); err != nil {
			t.Fatalf("WriteValue(%q): %v", c, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush(%q): %v", c, err)
		}
		if want := buf.Bytes(); !bytes.Equal(got, want) {
			t.Errorf("encoding mismatch for %q: got %q, want %q", c, got, want)
		}
	}
}

// TestAppendCommandReusesBuffer checks that the append form extends the
// caller's buffer instead of allocating a fresh one.
func TestAppendCommandReusesBuffer(t *testing.T) {
	buf := make([]byte, 1, 128)
	base := &buf[0]
	buf = buf[:0]
	buf = AppendCommand(buf, [][]byte{[]byte("PING")})
	buf = AppendCommand(buf, [][]byte{[]byte("GET"), []byte("k")})
	if &buf[0] != base {
		t.Error("AppendCommand replaced the caller's buffer")
	}
	if want := []byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"); !bytes.Equal(buf, want) {
		t.Errorf("got %q, want %q", buf, want)
	}
}
