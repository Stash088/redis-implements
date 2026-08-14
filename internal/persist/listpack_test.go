package persist

import (
	"testing"
)

// Byte sequences captured from a real Redis 8.2 dump (validation of the
// listpack backlen format).
func TestDecodeListpack(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want []string
	}{
		{
			name: "single 6-bit string",
			in:   []byte{0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x81, 0x61, 0x02, 0xff},
			want: []string{"a"},
		},
		{
			name: "two 6-bit strings",
			in:   []byte{0x0f, 0x00, 0x00, 0x00, 0x02, 0x00, 0x82, 0x6d, 0x31, 0x03, 0x82, 0x6d, 0x32, 0x03, 0xff},
			want: []string{"m1", "m2"},
		},
		{
			name: "7-bit ints and strings",
			in:   []byte{0x0d, 0x00, 0x00, 0x00, 0x02, 0x00, 0x05, 0x01, 0x82, 0x68, 0x69, 0x03, 0xff},
			want: []string{"5", "hi"},
		},
		{
			name: "13-bit int",
			// 0xcf = 0xC0|0x0F, value ((0x0F)<<8|0xA1) = 4001
			in:   []byte{0x0d, 0x00, 0x00, 0x00, 0x02, 0x00, 0xcf, 0xa1, 0x02, 0x81, 0x78, 0x02, 0xff},
			want: []string{"4001", "x"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeListpack(c.in)
			if err != nil {
				t.Fatalf("decodeListpack: %v", err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("got %d elements, want %d: %q", len(got), len(c.want), got)
			}
			for i := range got {
				if string(got[i]) != c.want[i] {
					t.Errorf("elem %d = %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

func TestDecodeListpackCorrupt(t *testing.T) {
	if _, err := decodeListpack([]byte{0x01, 0x00}); err == nil {
		t.Error("expected error for short listpack")
	}
	if _, err := decodeListpack([]byte{0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x81, 0x61, 0xff, 0x00}); err == nil {
		t.Error("expected error for missing backlen")
	}
}
