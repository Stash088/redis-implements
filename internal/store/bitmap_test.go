package store

import (
	"testing"
	"time"
)

// setBit is a helper that ignores the old bit of SetBit.
func setBit(t *testing.T, s *Store, key string, offset uint64, bit int) {
	t.Helper()
	if _, err := s.SetBit([]byte(key), offset, bit); err != nil {
		t.Fatalf("SETBIT %s %d %d: %v", key, offset, bit, err)
	}
}

func bitValue(t *testing.T, s *Store, key string, offset uint64) int {
	t.Helper()
	bit, ok, err := s.GetBit([]byte(key), offset)
	if err != nil || !ok {
		t.Fatalf("GETBIT %s %d: ok=%v err=%v", key, offset, ok, err)
	}
	return bit
}

func TestSetBitGetBit(t *testing.T) {
	s := New()

	// A bit set on a missing key: the previous bit is 0 and the value grows.
	if old, err := s.SetBit([]byte("bm"), 0, 1); err != nil || old != 0 {
		t.Fatalf("SETBIT fresh: old=%d err=%v", old, err)
	}
	if n, _, err := s.StrLen([]byte("bm")); err != nil || n != 1 {
		t.Fatalf("STRLEN bm after SETBIT 0: n=%d err=%v", n, err)
	}
	// A far bit pads the value with zero bytes.
	if old, err := s.SetBit([]byte("bm"), 999, 1); err != nil || old != 0 {
		t.Fatalf("SETBIT far: old=%d err=%v", old, err)
	}
	if n, _, err := s.StrLen([]byte("bm")); err != nil || n != 125 {
		t.Fatalf("STRLEN bm after SETBIT 999: n=%d err=%v", n, err)
	}
	if got := bitValue(t, s, "bm", 0); got != 1 {
		t.Errorf("bit 0: got %d, want 1", got)
	}
	if got := bitValue(t, s, "bm", 998); got != 0 {
		t.Errorf("bit 998 (padding): got %d, want 0", got)
	}
	// Overwriting returns the previous bit.
	if old, err := s.SetBit([]byte("bm"), 0, 0); err != nil || old != 1 {
		t.Errorf("SETBIT overwrite: old=%d err=%v", old, err)
	}
	if got := bitValue(t, s, "bm", 0); got != 0 {
		t.Errorf("bit 0 after clear: got %d, want 0", got)
	}
	// Setting the same bit again returns the old bit and grows nothing.
	n0, _, _ := s.StrLen([]byte("bm"))
	if old, err := s.SetBit([]byte("bm"), 999, 1); err != nil || old != 1 {
		t.Errorf("SETBIT same: old=%d err=%v", old, err)
	}
	if n, _, _ := s.StrLen([]byte("bm")); n != n0 {
		t.Errorf("STRLEN changed on a no-op SETBIT: %d -> %d", n0, n)
	}

	// A bit past the end of an existing string value reads as zero.
	s.SetString([]byte("str"), []byte("a"))
	if got, ok, err := s.GetBit([]byte("str"), 100); err != nil || !ok || got != 0 {
		t.Errorf("GETBIT past the end: got=%d ok=%v err=%v", got, ok, err)
	}
	// A missing key reads as zero with ok=false.
	if _, ok, err := s.GetBit([]byte("nosuch"), 0); err != nil || ok {
		t.Errorf("GETBIT missing: ok=%v err=%v", ok, err)
	}

	// A key of another type is a wrong type.
	s.ListPush([]byte("l"), false, []byte("x"))
	if _, err := s.SetBit([]byte("l"), 0, 1); err != ErrWrongType {
		t.Errorf("SETBIT on a list: err=%v, want ErrWrongType", err)
	}
	if _, _, err := s.GetBit([]byte("l"), 0); err != ErrWrongType {
		t.Errorf("GETBIT on a list: err=%v, want ErrWrongType", err)
	}
}

func TestSetBitKeepsTTL(t *testing.T) {
	s := New()
	s.SetStringWithTTL([]byte("k"), []byte("x"), time.Minute)
	if _, err := s.SetBit([]byte("k"), 15, 1); err != nil {
		t.Fatalf("SETBIT: %v", err)
	}
	if d, ok := s.TTL([]byte("k")); !ok || d <= 0 {
		t.Errorf("TTL after SETBIT: d=%v ok=%v, want the TTL kept", d, ok)
	}
}

func TestBitCount(t *testing.T) {
	s := New()
	s.SetString([]byte("k"), []byte("foobar")) // 4,6,6,3,3,4 bits per byte

	cases := []struct {
		name       string
		start, end int64
		hasRange   bool
		isbit      bool
		want       int64
	}{
		{"whole", 0, 0, false, false, 26},
		{"byte 1 1", 1, 1, true, false, 6},
		{"byte 1 -1", 1, -1, true, false, 22},
		{"byte -5 -2", -5, -2, true, false, 18},
		{"byte 3 1", 3, 1, true, false, 0},
		{"byte 5 100 clamped", 5, 100, true, false, 4},
		{"byte 100 200 past", 100, 200, true, false, 0},
		{"byte -100 -200 both neg reversed", -100, -200, true, false, 0},
		{"bit 8 15", 8, 15, true, true, 6},
		{"bit 10 13", 10, 13, true, true, 3},
		{"bit 0 47", 0, 47, true, true, 26},
		{"bit 6 9 crosses bytes", 6, 9, true, true, 2},
	}
	for _, tc := range cases {
		got, err := s.BitCount([]byte("k"), tc.start, tc.end, tc.hasRange, tc.isbit)
		if err != nil {
			t.Errorf("%s: err=%v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}

	// A missing key counts zero bits.
	if got, err := s.BitCount([]byte("nosuch"), 0, 0, true, false); err != nil || got != 0 {
		t.Errorf("BITCOUNT missing: got %d err=%v", got, err)
	}
	// A key holding an empty value counts zero bits too.
	s.SetString([]byte("empty"), []byte{})
	if got, err := s.BitCount([]byte("empty"), 0, 0, false, false); err != nil || got != 0 {
		t.Errorf("BITCOUNT empty: got %d err=%v", got, err)
	}
	// A key of another type is a wrong type.
	s.ListPush([]byte("l"), false, []byte("x"))
	if _, err := s.BitCount([]byte("l"), 0, 0, false, false); err != ErrWrongType {
		t.Errorf("BITCOUNT on a list: err=%v, want ErrWrongType", err)
	}
}

func TestBitPos(t *testing.T) {
	s := New()

	s.SetString([]byte("ones2"), []byte("\xff\xf0"))
	if got, _ := s.BitPos([]byte("ones2"), 1, 0, 0, false, false, false); got != 0 {
		t.Errorf("BITPOS 1 ff f0: got %d, want 0", got)
	}
	if got, _ := s.BitPos([]byte("ones2"), 0, 0, 0, false, false, false); got != 12 {
		t.Errorf("BITPOS 0 ff f0: got %d, want 12", got)
	}

	s.SetString([]byte("mixed"), []byte("\x00\xff\xf0"))
	if got, _ := s.BitPos([]byte("mixed"), 1, 0, 0, false, false, false); got != 8 {
		t.Errorf("BITPOS 1 00 ff f0: got %d, want 8", got)
	}
	if got, _ := s.BitPos([]byte("mixed"), 0, 0, 0, false, false, false); got != 0 {
		t.Errorf("BITPOS 0 00 ff f0: got %d, want 0", got)
	}

	// A value of all ones: the padding yields the first clear bit past the
	// end, unless the range was given with an explicit end.
	s.SetString([]byte("allones"), []byte("\xff\xff\xff"))
	if got, _ := s.BitPos([]byte("allones"), 0, 0, 0, false, false, false); got != 24 {
		t.Errorf("BITPOS 0 all ones: got %d, want 24", got)
	}
	if got, _ := s.BitPos([]byte("allones"), 0, 0, 2, true, true, false); got != -1 {
		t.Errorf("BITPOS 0 all ones explicit end: got %d, want -1", got)
	}
	// Only a start: the padding past the value is still consulted, the
	// position counts from the start of the value.
	if got, _ := s.BitPos([]byte("allones"), 0, 1, 0, true, false, false); got != 24 {
		t.Errorf("BITPOS 0 all ones start 1: got %d, want 24", got)
	}
	if got, _ := s.BitPos([]byte("allones"), 1, 0, 0, false, false, false); got != 0 {
		t.Errorf("BITPOS 1 all ones: got %d, want 0", got)
	}

	// A missing key is an infinite run of zeroes.
	if got, _ := s.BitPos([]byte("nosuch"), 0, 0, 0, false, false, false); got != 0 {
		t.Errorf("BITPOS 0 missing: got %d, want 0", got)
	}
	if got, _ := s.BitPos([]byte("nosuch"), 1, 0, 0, false, false, false); got != -1 {
		t.Errorf("BITPOS 1 missing: got %d, want -1", got)
	}
	// Even a reversed range of a missing key answers the missing-key reply.
	if got, _ := s.BitPos([]byte("nosuch"), 0, 5, 3, true, true, false); got != 0 {
		t.Errorf("BITPOS 0 missing reversed range: got %d, want 0", got)
	}

	// An existing but empty value holds an empty range.
	s.SetString([]byte("empty"), []byte{})
	if got, _ := s.BitPos([]byte("empty"), 0, 0, 0, false, false, false); got != -1 {
		t.Errorf("BITPOS 0 empty: got %d, want -1", got)
	}
	if got, _ := s.BitPos([]byte("empty"), 1, 0, 0, false, false, false); got != -1 {
		t.Errorf("BITPOS 1 empty: got %d, want -1", got)
	}

	// Bit unit with ranges that cover parts of bytes.
	s.SetString([]byte("onebyte"), []byte("\x00"))
	if got, _ := s.BitPos([]byte("onebyte"), 1, 0, 7, true, true, true); got != -1 {
		t.Errorf("BITPOS 1 bit 0..7 of zeroes: got %d, want -1", got)
	}
	if got, _ := s.BitPos([]byte("onebyte"), 0, 0, 7, true, true, true); got != 0 {
		t.Errorf("BITPOS 0 bit 0..7 of zeroes: got %d, want 0", got)
	}
	if got, _ := s.BitPos([]byte("onebyte"), 1, 8, 15, true, true, true); got != -1 {
		t.Errorf("BITPOS 1 bit 8..15 of a one-byte value: got %d, want -1", got)
	}

	// A range crossing bytes with partial edges.
	s.SetString([]byte("edges"), []byte("\xff\x00"))
	if got, _ := s.BitPos([]byte("edges"), 0, 5, 10, true, true, true); got != 8 {
		t.Errorf("BITPOS 0 bit 5..10 of ff 00: got %d, want 8", got)
	}
	if got, _ := s.BitPos([]byte("edges"), 1, 5, 10, true, true, true); got != 5 {
		t.Errorf("BITPOS 1 bit 5..10 of ff 00: got %d, want 5", got)
	}

	// A key of another type is a wrong type.
	s.ListPush([]byte("l"), false, []byte("x"))
	if _, err := s.BitPos([]byte("l"), 0, 0, 0, false, false, false); err != ErrWrongType {
		t.Errorf("BITPOS on a list: err=%v, want ErrWrongType", err)
	}
}

func TestBitOp(t *testing.T) {
	s := New()
	s.SetString([]byte("k1"), []byte("foobar"))
	s.SetString([]byte("k2"), []byte("abcdef"))

	cases := []struct {
		name string
		op   byte
		srcs []string
		want string
	}{
		{"and", BitOpAnd, []string{"k1", "k2"}, "`bc`ab"},
		{"or", BitOpOr, []string{"k1", "k2"}, "goofev"},
		{"xor", BitOpXor, []string{"k1", "k2"}, "\x07\x0d\x0c\x06\x04\x14"},
		{"not", BitOpNot, []string{"k1"}, "\x99\x90\x90\x9d\x9e\x8d"},
	}
	for _, tc := range cases {
		srcs := make([][]byte, len(tc.srcs))
		for i, k := range tc.srcs {
			srcs[i] = []byte(k)
		}
		n, err := s.BitOp(tc.op, []byte("dest"), srcs)
		if err != nil {
			t.Errorf("%s: err=%v", tc.name, err)
			continue
		}
		if n != int64(len(tc.want)) {
			t.Errorf("%s: length %d, want %d", tc.name, n, len(tc.want))
		}
		v, ok, _ := s.GetString([]byte("dest"))
		if !ok || string(v) != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, v, tc.want)
		}
	}

	// Sources of different lengths: the shorter ones count as zeroes.
	s.SetString([]byte("short"), []byte("abc"))
	s.Delete([]byte("empty"))
	if n, _ := s.BitOp(BitOpXor, []byte("dest"), [][]byte{[]byte("short"), []byte("nosuch")}); n != 3 {
		t.Errorf("XOR with a missing source: length %d, want 3", n)
	}
	if v, _, _ := s.GetString([]byte("dest")); string(v) != "abc" {
		t.Errorf("XOR with a missing source: got %q, want abc", v)
	}
	// AND with a missing source produces zero bytes of the longest length.
	if n, _ := s.BitOp(BitOpAnd, []byte("dest"), [][]byte{[]byte("nosuch"), []byte("short")}); n != 3 {
		t.Errorf("AND with a missing source: length %d, want 3", n)
	}
	if v, _, _ := s.GetString([]byte("dest")); v == nil || string(v) != "\x00\x00\x00" {
		t.Errorf("AND with a missing source: got %q", v)
	}

	// All sources empty: the destination key is removed and the reply is 0.
	if n, err := s.BitOp(BitOpOr, []byte("dest"), [][]byte{[]byte("nosuch"), []byte("alsonot")}); err != nil || n != 0 {
		t.Errorf("BITOP over missing keys: n=%d err=%v", n, err)
	}
	if s.Exists([]byte("dest")) != 0 {
		t.Error("BITOP over missing keys must remove the destination")
	}

	// The result replaces whatever type the destination held and drops its TTL.
	s.ListPush([]byte("list"), false, []byte("x"))
	s.SetStringWithTTL([]byte("withttl"), []byte("x"), time.Minute)
	if n, err := s.BitOp(BitOpNot, []byte("list"), [][]byte{[]byte("short")}); err != nil || n != 3 {
		t.Errorf("BITOP NOT into a list key: n=%d err=%v", n, err)
	}
	if typ, _ := s.GetType([]byte("list")); typ != TypeString {
		t.Errorf("TYPE after BITOP into a list key: %v, want string", typ)
	}
	if n, err := s.BitOp(BitOpNot, []byte("withttl"), [][]byte{[]byte("short")}); err != nil || n != 3 {
		t.Errorf("BITOP NOT into a key with TTL: n=%d err=%v", n, err)
	}
	if d, ok := s.TTL([]byte("withttl")); !ok || d >= 0 {
		t.Errorf("TTL after BITOP: d=%v ok=%v, want the TTL dropped", d, ok)
	}

	// A non-string source is a wrong type and nothing is written.
	s.ListPush([]byte("l"), false, []byte("x"))
	if _, err := s.BitOp(BitOpOr, []byte("dest"), [][]byte{[]byte("l"), []byte("short")}); err != ErrWrongType {
		t.Errorf("BITOP with a list source: err=%v, want ErrWrongType", err)
	}
	if s.Exists([]byte("dest")) != 0 {
		t.Error("BITOP with a wrong type must not write the destination")
	}

	// DIFF, DIFF1, ANDOR and ONE (the Redis 8.2 operations), on bitmaps
	// built with SETBIT: X = 10000100, A = 11000000, B = 01100000.
	setBit(t, s, "X", 0, 1)
	setBit(t, s, "X", 5, 1)
	setBit(t, s, "A", 0, 1)
	setBit(t, s, "A", 1, 1)
	setBit(t, s, "B", 1, 1)
	setBit(t, s, "B", 2, 1)

	cases82 := []struct {
		name string
		op   byte
		srcs []string
		want []int // the positions of the set bits of the result
	}{
		{"diff", BitOpDiff, []string{"X", "A", "B"}, []int{5}},
		{"diff1", BitOpDiff1, []string{"X", "A", "B"}, []int{1, 2}},
		{"andor", BitOpAndor, []string{"X", "A", "B"}, []int{0}},
		{"one", BitOpOne, []string{"A", "B"}, []int{0, 2}},
	}
	for _, tc := range cases82 {
		srcs := make([][]byte, len(tc.srcs))
		for i, k := range tc.srcs {
			srcs[i] = []byte(k)
		}
		if _, err := s.BitOp(tc.op, []byte("d82"), srcs); err != nil {
			t.Errorf("%s: err=%v", tc.name, err)
			continue
		}
		for i := 0; i < 8; i++ {
			want := 0
			for _, p := range tc.want {
				if p == i {
					want = 1
					break
				}
			}
			if got := bitValue(t, s, "d82", uint64(i)); got != want {
				t.Errorf("%s: bit %d: got %d, want %d", tc.name, i, got, want)
			}
		}
	}

	// The destination may be one of the sources.
	if n, err := s.BitOp(BitOpOr, []byte("A"), [][]byte{[]byte("A"), []byte("B")}); err != nil || n != 1 {
		t.Errorf("BITOP with the destination among the sources: n=%d err=%v", n, err)
	}
	if got := bitValue(t, s, "A", 0); got != 1 {
		t.Errorf("bit 0 of A|B: got %d, want 1", got)
	}
	if got := bitValue(t, s, "A", 2); got != 1 {
		t.Errorf("bit 2 of A|B: got %d, want 1", got)
	}
}

func TestBitField(t *testing.T) {
	s := New()

	// SET returns the previous value, GET reads what was written.
	res := bitFieldRun(t, s, "bf",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 3},
		BitFieldOp{Op: BitFieldGet, Offset: 0, Bits: 4},
		BitFieldOp{Op: BitFieldGet, Offset: 4, Bits: 4},
	)
	want := []BitFieldResult{{Val: 0}, {Val: 3}, {Val: 0}}
	if !bitFieldEqual(res, want) {
		t.Errorf("SET/GET u4: got %+v, want %+v", res, want)
	}

	// The "#n" offset addresses whole fields of the given width.
	res = bitFieldRun(t, s, "bf",
		BitFieldOp{Op: BitFieldSet, Offset: 8, Bits: 8, Signed: true, Value: 100},
		BitFieldOp{Op: BitFieldGet, Offset: 8, Bits: 8, Signed: true},
	)
	want = []BitFieldResult{{Val: 0}, {Val: 100}}
	if !bitFieldEqual(res, want) {
		t.Errorf("i8 #1: got %+v, want %+v", res, want)
	}

	// Signed and unsigned reads of the same bits (on a fresh key).
	res = bitFieldRun(t, s, "sign",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 8, Signed: true, Value: -1},
		BitFieldOp{Op: BitFieldGet, Offset: 0, Bits: 8, Signed: true},
		BitFieldOp{Op: BitFieldGet, Offset: 0, Bits: 8},
	)
	want = []BitFieldResult{{Val: 0}, {Val: -1}, {Val: 255}}
	if !bitFieldEqual(res, want) {
		t.Errorf("signed -1: got %+v, want %+v", res, want)
	}

	// INCRBY wraps by default.
	res = bitFieldRun(t, s, "wbf",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 15},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: 1},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: 1},
	)
	want = []BitFieldResult{{Val: 0}, {Val: 0}, {Val: 1}}
	if !bitFieldEqual(res, want) {
		t.Errorf("INCRBY wrap up: got %+v, want %+v", res, want)
	}
	// Wrapping below zero.
	res = bitFieldRun(t, s, "wdown",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 0},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: -1},
	)
	want = []BitFieldResult{{Val: 0}, {Val: 15}}
	if !bitFieldEqual(res, want) {
		t.Errorf("INCRBY wrap down: got %+v, want %+v", res, want)
	}

	// OVERFLOW SAT saturates at the limits of the type.
	res = bitFieldRun(t, s, "sat",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 15},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: 1, Over: OverflowSat},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: -5, Over: OverflowSat},
	)
	want = []BitFieldResult{{Val: 0}, {Val: 15}, {Val: 10}}
	if !bitFieldEqual(res, want) {
		t.Errorf("OVERFLOW SAT: got %+v, want %+v", res, want)
	}
	// Saturation on the underflow side.
	res = bitFieldRun(t, s, "sat2",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 0},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: -1, Over: OverflowSat},
	)
	want = []BitFieldResult{{Val: 0}, {Val: 0}}
	if !bitFieldEqual(res, want) {
		t.Errorf("OVERFLOW SAT underflow: got %+v, want %+v", res, want)
	}

	// OVERFLOW FAIL discards the operation and reports null.
	res = bitFieldRun(t, s, "fail",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 15},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 4, Value: 1, Over: OverflowFail},
		BitFieldOp{Op: BitFieldGet, Offset: 0, Bits: 4},
	)
	want = []BitFieldResult{{Val: 0}, {Null: true}, {Val: 15}}
	if !bitFieldEqual(res, want) {
		t.Errorf("OVERFLOW FAIL: got %+v, want %+v", res, want)
	}

	// A SET value out of range wraps into the width of the field.
	res = bitFieldRun(t, s, "setwrap",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 4, Value: 300},
		BitFieldOp{Op: BitFieldGet, Offset: 0, Bits: 4},
	)
	want = []BitFieldResult{{Val: 0}, {Val: 300 % 16}}
	if !bitFieldEqual(res, want) {
		t.Errorf("SET out of range: got %+v, want %+v", res, want)
	}

	// Signed overflow wraps into the sign.
	res = bitFieldRun(t, s, "swrap",
		BitFieldOp{Op: BitFieldSet, Offset: 0, Bits: 8, Signed: true, Value: 127},
		BitFieldOp{Op: BitFieldIncrby, Offset: 0, Bits: 8, Signed: true, Value: 1},
	)
	want = []BitFieldResult{{Val: 0}, {Val: -128}}
	if !bitFieldEqual(res, want) {
		t.Errorf("i8 INCRBY wrap: got %+v, want %+v", res, want)
	}

	// A GET on a missing key reads zero; an empty operation list yields an
	// empty reply.
	res = bitFieldRun(t, s, "nosuch")
	if len(res) != 0 {
		t.Errorf("BITFIELD with no operations: got %+v, want empty", res)
	}
	res = bitFieldRun(t, s, "nosuch", BitFieldOp{Op: BitFieldGet, Offset: 0, Bits: 8})
	if !bitFieldEqual(res, []BitFieldResult{{Val: 0}}) {
		t.Errorf("GET on a missing key: got %+v, want [0]", res)
	}

	// A far write pads the value.
	bitFieldRun(t, s, "far", BitFieldOp{Op: BitFieldSet, Offset: 1000, Bits: 8})
	if n, _, _ := s.StrLen([]byte("far")); n != 126 {
		t.Errorf("STRLEN after a far SET: n=%d, want 126", n)
	}

	// A key of another type is a wrong type.
	s.ListPush([]byte("l"), false, []byte("x"))
	if _, err := s.BitField([]byte("l"), []BitFieldOp{{Op: BitFieldGet, Offset: 0, Bits: 8}}); err != ErrWrongType {
		t.Errorf("BITFIELD on a list: err=%v, want ErrWrongType", err)
	}
	if _, err := s.BitField([]byte("l"), []BitFieldOp{{Op: BitFieldSet, Offset: 0, Bits: 8}}); err != ErrWrongType {
		t.Errorf("BITFIELD SET on a list: err=%v, want ErrWrongType", err)
	}
}

func TestBitFieldKeepsTTL(t *testing.T) {
	s := New()
	s.SetStringWithTTL([]byte("k"), []byte("x"), time.Minute)
	bitFieldRun(t, s, "k", BitFieldOp{Op: BitFieldSet, Offset: 8, Bits: 8, Value: 1})
	if d, ok := s.TTL([]byte("k")); !ok || d <= 0 {
		t.Errorf("TTL after BITFIELD: d=%v ok=%v, want the TTL kept", d, ok)
	}
}

// bitFieldRun runs the given operations on the key, reporting failures.
func bitFieldRun(t *testing.T, s *Store, key string, ops ...BitFieldOp) []BitFieldResult {
	t.Helper()
	res, err := s.BitField([]byte(key), ops)
	if err != nil {
		t.Fatalf("BITFIELD %s: %v", key, err)
	}
	return res
}

func bitFieldEqual(got, want []BitFieldResult) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestParseBitHelpers(t *testing.T) {
	// Bit offsets.
	if _, err := ParseBitOffset([]byte("-1"), false, 0); err != ErrBitOffset {
		t.Errorf("ParseBitOffset -1: err=%v", err)
	}
	if _, err := ParseBitOffset([]byte("abc"), false, 0); err != ErrBitOffset {
		t.Errorf("ParseBitOffset abc: err=%v", err)
	}
	if _, err := ParseBitOffset([]byte("1<<32"), false, 0); err == nil {
		t.Error("ParseBitOffset garbage must fail")
	}
	if off, err := ParseBitOffset([]byte("4294967295"), false, 0); err != nil || off != 4294967295 {
		t.Errorf("ParseBitOffset max: off=%d err=%v", off, err)
	}
	if _, err := ParseBitOffset([]byte("4294967296"), false, 0); err != ErrBitOffset {
		t.Errorf("ParseBitOffset over max: err=%v", err)
	}
	// The "#n" form.
	if off, err := ParseBitOffset([]byte("#3"), true, 8); err != nil || off != 24 {
		t.Errorf("ParseBitOffset #3 x8: off=%d err=%v", off, err)
	}
	if _, err := ParseBitOffset([]byte("#3"), false, 0); err != ErrBitOffset {
		t.Errorf("ParseBitOffset #3 without hash: err=%v", err)
	}

	// The 0/1 value of SETBIT: the reference number grammar rejects a
	// leading zero, a bare '-' and "-0".
	if _, err := ParseBitValue([]byte("0")); err != nil {
		t.Errorf("ParseBitValue 0: err=%v", err)
	}
	if _, err := ParseBitValue([]byte("1")); err != nil {
		t.Errorf("ParseBitValue 1: err=%v", err)
	}
	for _, bad := range []string{"2", "-1", "01", "-0", "007", "1 ", "abc", "", "+1", "99999999999999999999999"} {
		if _, err := ParseBitValue([]byte(bad)); err != ErrBitValue {
			t.Errorf("ParseBitValue %q: err=%v, want ErrBitValue", bad, err)
		}
	}

	// Bitfield types.
	if signed, width, err := ParseBitFieldType([]byte("i64")); err != nil || !signed || width != 64 {
		t.Errorf("ParseBitFieldType i64: signed=%v width=%d err=%v", signed, width, err)
	}
	if signed, width, err := ParseBitFieldType([]byte("u63")); err != nil || signed || width != 63 {
		t.Errorf("ParseBitFieldType u63: signed=%v width=%d err=%v", signed, width, err)
	}
	for _, bad := range []string{"u64", "i0", "i65", "x8", "i", "I8", "i8x"} {
		if _, _, err := ParseBitFieldType([]byte(bad)); err != ErrBitFieldType {
			t.Errorf("ParseBitFieldType %q: err=%v, want ErrBitFieldType", bad, err)
		}
	}

	// The operation names of BITOP: lower case and mixed case spellings.
	if op, ok := ParseBitOp([]byte("XoR")); !ok || op != BitOpXor {
		t.Errorf("ParseBitOp XoR: op=%d ok=%v", op, ok)
	}
	for _, name := range []string{"and", "or", "xor", "not", "diff", "diff1", "andor", "one"} {
		if _, ok := ParseBitOp([]byte(name)); !ok {
			t.Errorf("ParseBitOp %s: not recognized", name)
		}
	}
	if _, ok := ParseBitOp([]byte("nand")); ok {
		t.Error("ParseBitOp nand must fail")
	}

	// A sequence of BITFIELD subcommands with the OVERFLOW policy.
	ops, err := ParseBitFieldOps([][]byte{
		[]byte("SET"), []byte("u4"), []byte("0"), []byte("15"),
		[]byte("OVERFLOW"), []byte("FAIL"),
		[]byte("INCRBY"), []byte("u4"), []byte("#0"), []byte("1"),
		[]byte("OVERFLOW"), []byte("WRAP"),
		[]byte("GET"), []byte("i8"), []byte("#1"),
	})
	if err != nil {
		t.Fatalf("ParseBitFieldOps: %v", err)
	}
	if len(ops) != 3 {
		t.Fatalf("ParseBitFieldOps: %d ops, want 3", len(ops))
	}
	if ops[0].Op != BitFieldSet || ops[0].Bits != 4 || ops[0].Over != OverflowWrap {
		t.Errorf("op 0: %+v", ops[0])
	}
	if ops[1].Op != BitFieldIncrby || ops[1].Offset != 0 || ops[1].Over != OverflowFail {
		t.Errorf("op 1: %+v", ops[1])
	}
	if ops[2].Op != BitFieldGet || !ops[2].Signed || ops[2].Offset != 8 || ops[2].Over != OverflowWrap {
		t.Errorf("op 2: %+v", ops[2])
	}

	// Grammar failures.
	for _, bad := range [][][]byte{
		{[]byte("GET"), []byte("u8")},                           // not enough args
		{[]byte("SET"), []byte("u8"), []byte("0")},              // no value
		{[]byte("SET"), []byte("u8"), []byte("0"), []byte("x")}, // bad value
		{[]byte("OVERFLOW"), []byte("BOOM")},                    // bad policy
		{[]byte("WHAT")},                                        // unknown subcommand
	} {
		if _, err := ParseBitFieldOps(bad); err == nil {
			t.Errorf("ParseBitFieldOps %v must fail", bad)
		}
	}
}

// TestBitFieldOverflowArithmetic pins the wrap/sat arithmetic of the bitfield
// engine on values near the edges of the widths.
func TestBitFieldOverflowArithmetic(t *testing.T) {
	// Unsigned wrap.
	var limit uint64
	if ov := checkUnsignedBitfieldOverflow(255, 1, 8, OverflowWrap, &limit); ov != 1 || limit != 0 {
		t.Errorf("u8 255+1 wrap: ov=%d limit=%d, want 1/0", ov, limit)
	}
	if ov := checkUnsignedBitfieldOverflow(0, -1, 8, OverflowWrap, &limit); ov != -1 || limit != 255 {
		t.Errorf("u8 0-1 wrap: ov=%d limit=%d, want -1/255", ov, limit)
	}
	if ov := checkUnsignedBitfieldOverflow(0, -1, 4, OverflowWrap, &limit); ov != -1 || limit != 15 {
		t.Errorf("u4 0-1 wrap: ov=%d limit=%d, want -1/15", ov, limit)
	}
	// Unsigned saturation.
	if ov := checkUnsignedBitfieldOverflow(255, 1, 8, OverflowSat, &limit); ov != 1 || limit != 255 {
		t.Errorf("u8 255+1 sat: ov=%d limit=%d", ov, limit)
	}
	if ov := checkUnsignedBitfieldOverflow(0, -1, 8, OverflowSat, &limit); ov != -1 || limit != 0 {
		t.Errorf("u8 0-1 sat: ov=%d limit=%d", ov, limit)
	}
	// Unsigned in range.
	if ov := checkUnsignedBitfieldOverflow(10, 5, 8, OverflowWrap, &limit); ov != 0 {
		t.Errorf("u8 10+5: ov=%d, want 0", ov)
	}

	// Signed wrap.
	var slimit int64
	if ov := checkSignedBitfieldOverflow(127, 1, 8, OverflowWrap, &slimit); ov != 1 || slimit != -128 {
		t.Errorf("i8 127+1 wrap: ov=%d limit=%d", ov, slimit)
	}
	if ov := checkSignedBitfieldOverflow(-128, -1, 8, OverflowWrap, &slimit); ov != -1 || slimit != 127 {
		t.Errorf("i8 -128-1 wrap: ov=%d limit=%d", ov, slimit)
	}
	// Signed saturation.
	if ov := checkSignedBitfieldOverflow(100, 100, 8, OverflowSat, &slimit); ov != 1 || slimit != 127 {
		t.Errorf("i8 100+100 sat: ov=%d limit=%d", ov, slimit)
	}
	// Signed in range.
	if ov := checkSignedBitfieldOverflow(-128, 1, 8, OverflowWrap, &slimit); ov != 0 {
		t.Errorf("i8 -128+1: ov=%d, want 0", ov)
	}
	// i64 wrap.
	if ov := checkSignedBitfieldOverflow(1<<62, 1<<62, 64, OverflowWrap, &slimit); ov != 1 || slimit != -(1<<63) {
		t.Errorf("i64 max wrap: ov=%d limit=%d", ov, slimit)
	}
}
