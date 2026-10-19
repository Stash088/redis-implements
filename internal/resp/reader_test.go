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

func TestValueLen(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"+OK\r\n", 5},
		{"-ERR bad\r\n", 10},
		{":42\r\n", 5},
		{"$3\r\nabc\r\n", 9},
		{"$0\r\n\r\n", 6},
		{"$-1\r\n", 5},
		{"*2\r\n$1\r\na\r\n$1\r\nb\r\n", 18},
		{"*-1\r\n", 5},
		{"*0\r\n", 4},
		// Fragments: the value is not complete yet.
		{"", 0},
		{"+OK", 0},
		{"+OK\r", 0},
		{"$3\r\nab", 0},
		{"$3\r\nabc", 0},
		{"*2\r\n$1\r\na\r\n", 0},
		{"*1\r\n", 0},
		{"?bad\r\n", 0},
	}
	for _, c := range cases {
		if got := valueLen([]byte(c.in)); got != c.want {
			t.Errorf("valueLen(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestHasBufferedValue(t *testing.T) {
	// A complete command in the buffer is detected...
	r := NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"))
	if r.HasBufferedValue() {
		t.Error("empty buffer: want no buffered value")
	}
	if _, err := r.ReadValue(); err != nil {
		t.Fatal(err)
	}
	if !r.HasBufferedValue() {
		t.Error("second pipelined command is buffered: want a buffered value")
	}

	// ...and a fragment of one is not.
	r2 := NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI"))
	if _, err := r2.ReadValue(); err != nil {
		t.Fatal(err)
	}
	if r2.HasBufferedValue() {
		t.Error("fragmentary command: want no buffered value")
	}
}

func TestReaderRESP3Types(t *testing.T) {
	// Map: a count of pairs followed by the elements.
	m := decode(t, "%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n")
	if m.Kind != KindMap || len(m.Array) != 4 {
		t.Fatalf("map: got %+v", m)
	}
	if m.Array[1].Int != 1 {
		t.Errorf("map value: got %+v", m.Array[1])
	}

	// Set.
	s := decode(t, "~2\r\n$1\r\na\r\n$1\r\nb\r\n")
	if s.Kind != KindSet || len(s.Array) != 2 {
		t.Fatalf("set: got %+v", s)
	}

	// Double.
	d := decode(t, ",1.5\r\n")
	if d.Kind != KindDouble || d.Float != 1.5 {
		t.Fatalf("double: got %+v", d)
	}

	// Big number.
	bn := decode(t, "(3492890328409238509324850943850943825024385\r\n")
	if bn.Kind != KindBigNumber || string(bn.Str) != "3492890328409238509324850943850943825024385" {
		t.Fatalf("big number: got %+v", bn)
	}

	// Verbatim string.
	vb := decode(t, "=9\r\ntxt:hello\r\n")
	if vb.Kind != KindVerbatim || string(vb.Str) != "txt:hello" {
		t.Fatalf("verbatim: got %+v", vb)
	}

	// Push.
	p := decode(t, ">2\r\n$7\r\nmessage\r\n$2\r\nch\r\n")
	if p.Kind != KindPush || len(p.Array) != 2 {
		t.Fatalf("push: got %+v", p)
	}
}

func TestWriterRESP3Types(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteValue(NewMap([]Value{
		NewBulkString([]byte("a")), NewInteger(1),
		NewBulkString([]byte("b")), NewInteger(2),
	})); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteValue(NewSet([]Value{NewBulkString([]byte("x"))})); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteValue(NewDouble(1.5)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteValue(NewBigNumber("12345678901234567890")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteValue(NewVerbatim("txt:hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteValue(NewPush([]Value{NewBulkString([]byte("m"))})); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	want := "%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n" +
		"~1\r\n$1\r\nx\r\n" +
		",1.5\r\n" +
		"(12345678901234567890\r\n" +
		"=9\r\ntxt:hello\r\n" +
		">1\r\n$1\r\nm\r\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
