package resp

import (
	"bytes"
	"strings"
	"testing"
)

func encode(t *testing.T, v Value) string {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteValue(v); err != nil {
		t.Fatalf("WriteValue(%+v) error: %v", v, err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush error: %v", err)
	}
	return buf.String()
}

func TestWriteSimpleString(t *testing.T) {
	if got := encode(t, NewSimpleString("OK")); got != "+OK\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteError(t *testing.T) {
	if got := encode(t, NewError("ERR unknown command")); got != "-ERR unknown command\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteInteger(t *testing.T) {
	if got := encode(t, NewInteger(42)); got != ":42\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteIntegerNegative(t *testing.T) {
	if got := encode(t, NewInteger(-5)); got != ":-5\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteBulkString(t *testing.T) {
	if got := encode(t, NewBulkString([]byte("foobar"))); got != "$6\r\nfoobar\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteBulkStringEmpty(t *testing.T) {
	if got := encode(t, NewBulkString([]byte{})); got != "$0\r\n\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteNullBulk(t *testing.T) {
	if got := encode(t, NewBulkString(nil)); got != "$-1\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteNullArray(t *testing.T) {
	if got := encode(t, NewArray(nil)); got != "*-1\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteArray(t *testing.T) {
	v := NewArray([]Value{NewBulkString([]byte("foo")), NewInteger(1)})
	want := "*2\r\n$3\r\nfoo\r\n:1\r\n"
	if got := encode(t, v); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestWriteEmptyArray(t *testing.T) {
	if got := encode(t, NewArray([]Value{})); got != "*0\r\n" {
		t.Errorf("got %q", got)
	}
}

func TestWriteNestedArray(t *testing.T) {
	v := NewArray([]Value{
		NewArray([]Value{NewInteger(1), NewInteger(2)}),
		NewBulkString([]byte("x")),
	})
	want := "*2\r\n*2\r\n:1\r\n:2\r\n$1\r\nx\r\n"
	if got := encode(t, v); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestWriteValueUnknownKind(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	if err := w.WriteValue(Value{Kind: '?'}); err == nil {
		t.Errorf("expected error for unknown kind")
	}
}

func TestRoundTrip(t *testing.T) {
	values := []Value{
		NewSimpleString("OK"),
		NewError("ERR boom"),
		NewInteger(123456),
		NewBulkString([]byte("hello world")),
		NewBulkString(nil),
		NewArray([]Value{NewBulkString([]byte("SET")), NewBulkString([]byte("k")), NewBulkString([]byte("v"))}),
		NewArray([]Value{NewInteger(1), NewArray([]Value{NewInteger(2)})}),
		NewArray(nil),
	}
	for _, v := range values {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		if err := w.WriteValue(v); err != nil {
			t.Fatalf("WriteValue(%+v): %v", v, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		r := NewReader(strings.NewReader(buf.String()))
		got, err := r.ReadValue()
		if err != nil {
			t.Fatalf("ReadValue of %q: %v", buf.String(), err)
		}
		if !valuesEqual(v, got) {
			t.Errorf("round-trip mismatch: got %+v, want %+v (wire %q)", got, v, buf.String())
		}
	}
}

func valuesEqual(a, b Value) bool {
	if a.Kind != b.Kind || a.Int != b.Int || a.IsNull != b.IsNull {
		return false
	}
	if !bytes.Equal(a.Str, b.Str) {
		return false
	}
	if len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !valuesEqual(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}
