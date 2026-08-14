package resp

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
)

func decode(t *testing.T, input string) Value {
	t.Helper()
	r := NewReader(strings.NewReader(input))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue(%q) error: %v", input, err)
	}
	return v
}

func TestReaderSimpleString(t *testing.T) {
	v := decode(t, "+OK\r\n")
	want := Value{Kind: KindSimpleString, Str: []byte("OK")}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
}

func TestReaderSimpleStringEmpty(t *testing.T) {
	v := decode(t, "+\r\n")
	if v.Kind != KindSimpleString || len(v.Str) != 0 {
		t.Errorf("got %+v, want empty simple string", v)
	}
}

func TestReaderError(t *testing.T) {
	v := decode(t, "-ERR unknown command\r\n")
	if v.Kind != KindError || string(v.Str) != "ERR unknown command" {
		t.Errorf("got %+v", v)
	}
}

func TestReaderInteger(t *testing.T) {
	v := decode(t, ":1000\r\n")
	if v.Kind != KindInteger || v.Int != 1000 {
		t.Errorf("got %+v", v)
	}
}

func TestReaderIntegerNegative(t *testing.T) {
	v := decode(t, ":-7\r\n")
	if v.Kind != KindInteger || v.Int != -7 {
		t.Errorf("got %+v", v)
	}
}

func TestReaderBulkString(t *testing.T) {
	v := decode(t, "$6\r\nfoobar\r\n")
	want := Value{Kind: KindBulkString, Str: []byte("foobar")}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
}

func TestReaderBulkStringEmpty(t *testing.T) {
	v := decode(t, "$0\r\n\r\n")
	if v.Kind != KindBulkString || len(v.Str) != 0 || v.IsNull {
		t.Errorf("got %+v, want empty non-null bulk string", v)
	}
}

func TestReaderNullBulk(t *testing.T) {
	v := decode(t, "$-1\r\n")
	if !v.IsNull || v.Kind != KindBulkString {
		t.Errorf("got %+v, want null bulk string", v)
	}
}

func TestReaderArray(t *testing.T) {
	v := decode(t, "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	want := Value{
		Kind: KindArray,
		Array: []Value{
			{Kind: KindBulkString, Str: []byte("foo")},
			{Kind: KindBulkString, Str: []byte("bar")},
		},
	}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
}

func TestReaderArrayMixedTypes(t *testing.T) {
	v := decode(t, "*3\r\n:1\r\n+OK\r\n-ERR boom\r\n")
	want := Value{
		Kind: KindArray,
		Array: []Value{
			{Kind: KindInteger, Int: 1},
			{Kind: KindSimpleString, Str: []byte("OK")},
			{Kind: KindError, Str: []byte("ERR boom")},
		},
	}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
}

func TestReaderNestedArray(t *testing.T) {
	v := decode(t, "*2\r\n*1\r\n:5\r\n*0\r\n")
	want := Value{
		Kind: KindArray,
		Array: []Value{
			{Kind: KindArray, Array: []Value{{Kind: KindInteger, Int: 5}}},
			{Kind: KindArray, Array: []Value{}},
		},
	}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
}

func TestReaderNullArray(t *testing.T) {
	v := decode(t, "*-1\r\n")
	if !v.IsNull || v.Kind != KindArray {
		t.Errorf("got %+v, want null array", v)
	}
}

func TestReaderEmptyArray(t *testing.T) {
	v := decode(t, "*0\r\n")
	if v.Kind != KindArray || len(v.Array) != 0 || v.IsNull {
		t.Errorf("got %+v, want empty non-null array", v)
	}
}

func TestReaderChunkedInput(t *testing.T) {
	// Simulate network chunks: bytes arrive in pieces across Read calls.
	raw := []byte("*2\r\n$3\r\nfoo\r\n$5\r\nhello\r\n")
	chunks := []int{1, 2, 3, 5, 8, 13, 21}
	var data []byte
	pos := 0
	for _, c := range chunks {
		data = append(data, raw[pos:c]...)
		pos = c
	}
	data = append(data, raw[pos:]...)
	r := NewReader(bytes.NewReader(data))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue error: %v", err)
	}
	want := Value{
		Kind: KindArray,
		Array: []Value{
			{Kind: KindBulkString, Str: []byte("foo")},
			{Kind: KindBulkString, Str: []byte("hello")},
		},
	}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("got %+v, want %+v", v, want)
	}
}

func TestReaderMultipleValues(t *testing.T) {
	r := NewReader(strings.NewReader("+OK\r\n:42\r\n"))
	v1, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	v2, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v1.Kind != KindSimpleString || v2.Kind != KindInteger || v2.Int != 42 {
		t.Errorf("got %+v, %+v", v1, v2)
	}
}

func TestReaderEOF(t *testing.T) {
	r := NewReader(strings.NewReader(""))
	if _, err := r.ReadValue(); err != io.EOF {
		t.Errorf("got %v, want io.EOF", err)
	}
}

func TestReaderTruncatedBulk(t *testing.T) {
	r := NewReader(strings.NewReader("$5\r\nabc"))
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected error for truncated bulk string, got nil")
	}
}

func TestReaderBulkTooBig(t *testing.T) {
	r := NewReader(strings.NewReader("$9999999999999\r\n"))
	r.SetMaxBulkLen(1024)
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected ErrTooBig, got nil")
	}
}

func TestReaderBadBulkLen(t *testing.T) {
	r := NewReader(strings.NewReader("$abc\r\n"))
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected error for bad bulk length")
	}
}

func TestReaderNegativeBulkLenBelowMinusOne(t *testing.T) {
	r := NewReader(strings.NewReader("$-5\r\n"))
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected error for bulk length < -1")
	}
}

func TestReaderArrayTooBig(t *testing.T) {
	r := NewReader(strings.NewReader("*999999999\r\n"))
	r.SetMaxArrayLen(1024)
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected ErrTooBig, got nil")
	}
}

func TestReaderLineTooLong(t *testing.T) {
	long := strings.Repeat("x", 100)
	r := NewReader(strings.NewReader("+" + long + "\r\n"))
	r.SetMaxLineLen(50)
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected ErrTooBig, got nil")
	}
}

func TestReaderUnknownType(t *testing.T) {
	r := NewReader(strings.NewReader("?hello\r\n"))
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected error for unknown type byte")
	}
}

func TestReaderMissingCRLF(t *testing.T) {
	r := NewReader(strings.NewReader("+OK\n"))
	if _, err := r.ReadValue(); err == nil {
		t.Errorf("expected error for missing CR")
	}
}
