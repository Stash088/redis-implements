package persist

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// streamListpackElements builds the elements of the listpack Redis 8.2 wrote
// for a stream node: the master entry (the field "fld") and two entries that
// share its fields and carry 200 byte values.
func streamListpackElements() []listpackElem {
	return []listpackElem{
		{Int: 2, IsInt: true}, // the number of entries in the node
		{Int: 0, IsInt: true}, // the deleted entries of the master
		{Int: 1, IsInt: true}, // the number of the master's fields
		{Str: []byte("fld")},  // the master's field name
		{Int: 0, IsInt: true}, // the master's terminator
		{Int: 2, IsInt: true}, // the first entry: the same fields as the master
		{Int: 0, IsInt: true}, // the ms delta
		{Int: 0, IsInt: true}, // the seq delta
		{Str: bytes.Repeat([]byte("z"), 200)},
		{Int: 4, IsInt: true}, // the number of the listpack pieces of the entry
		{Int: 2, IsInt: true}, // the second entry: the same fields
		{Int: 1, IsInt: true}, // the ms delta
		{Int: 0, IsInt: true}, // the seq delta
		{Str: bytes.Repeat([]byte("z"), 200)},
		{Int: 4, IsInt: true},
	}
}

// TestEncodeListpackMatchesRedis checks the encoder against the bytes Redis
// 8.2 wrote for the same stream node.
//
// The reference is the plaintext of the listpack Redis stored in a real dump
// (the RDB blob itself was LZF compressed).
func TestEncodeListpackMatchesRedis(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("testdata", "stream_listpack.bin"))
	if err != nil {
		t.Fatalf("read the reference listpack: %v", err)
	}
	got := encodeListpack(nil, streamListpackElements())
	if !bytes.Equal(got, want) {
		t.Fatalf("the listpack differs from the one Redis wrote:\n got %x\nwant %x", got, want)
	}
}

// TestDecodeListpackTypedReference decodes the reference listpack and checks
// that the elements keep their type and value.
func TestDecodeListpackTypedReference(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("testdata", "stream_listpack.bin"))
	if err != nil {
		t.Fatalf("read the reference listpack: %v", err)
	}
	got, err := decodeListpackTyped(want)
	if err != nil {
		t.Fatalf("decodeListpackTyped: %v", err)
	}
	if len(got) != 15 {
		t.Fatalf("got %d elements, want 15", len(got))
	}
	checks := []struct {
		index int
		isInt bool
		num   int64
		str   string
	}{
		{0, true, 2, ""}, // the entries of the node
		{3, false, 0, "fld"},
		{5, true, 2, ""}, // the flags of the first entry (the same fields)
		{8, false, 0, string(bytes.Repeat([]byte("z"), 200))},
		{9, true, 4, ""},  // the number of the listpack pieces
		{11, true, 1, ""}, // the ms delta of the second entry
	}
	for _, c := range checks {
		e := got[c.index]
		if e.IsInt != c.isInt {
			t.Errorf("element %d: the type differs (int=%v)", c.index, e.IsInt)
			continue
		}
		if c.isInt && e.Int != c.num {
			t.Errorf("element %d: got %d, want %d", c.index, e.Int, c.num)
		}
		if !c.isInt && string(e.Str) != c.str {
			t.Errorf("element %d: got %q, want %q", c.index, e.Str, c.str)
		}
	}
}

// TestEncodeListpackRoundTrip checks that the encoder and the decoder agree.
func TestEncodeListpackRoundTrip(t *testing.T) {
	cases := [][]listpackElem{
		{{Int: 1, IsInt: true}},
		{{Str: []byte("a")}},
		{{Int: 0, IsInt: true}, {Int: 127, IsInt: true}, {Int: 128, IsInt: true}},
		{{Int: -1, IsInt: true}, {Int: -4096, IsInt: true}, {Int: -4097, IsInt: true}},
		{{Int: 32767, IsInt: true}, {Int: 32768, IsInt: true}, {Int: -8388608, IsInt: true}},
		{{Str: bytes.Repeat([]byte("x"), 63)}, {Str: bytes.Repeat([]byte("y"), 64)}, {Str: bytes.Repeat([]byte("z"), 4095)}},
		{{Str: bytes.Repeat([]byte("q"), 4096)}},
	}
	for _, want := range cases {
		enc := encodeListpack(nil, want)
		got, err := decodeListpackTyped(enc)
		if err != nil {
			t.Fatalf("decodeListpackTyped: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("got %d elements, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i].IsInt != want[i].IsInt {
				t.Errorf("element %d: the type differs", i)
				continue
			}
			if want[i].IsInt {
				if got[i].Int != want[i].Int {
					t.Errorf("element %d: got %d, want %d", i, got[i].Int, want[i].Int)
				}
				continue
			}
			if !bytes.Equal(got[i].Str, want[i].Str) {
				t.Errorf("element %d: got %q, want %q", i, got[i].Str, want[i].Str)
			}
		}
	}
}

// TestDecodeListpackTypedKeepsType checks that the decoder tells the
// integers from the strings: the stream listpacks hold both.
func TestDecodeListpackTypedKeepsType(t *testing.T) {
	enc := encodeListpack(nil, []listpackElem{
		{Int: 42, IsInt: true},
		{Str: []byte("42")},
	})
	got, err := decodeListpackTyped(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d elements, want 2", len(got))
	}
	if !got[0].IsInt || got[0].Int != 42 {
		t.Errorf("the first element is %+v, want the integer 42", got[0])
	}
	if got[1].IsInt || string(got[1].Str) != "42" {
		t.Errorf("the second element is %+v, want the string 42", got[1])
	}
}
