package persist

import "testing"

func TestCRC64Vector(t *testing.T) {
	// Reference value from Redis crc64.c: crc64(0, "123456789") == 0xe9c6d914c4b8d9ca.
	if got := crc64Final(crc64Update(0, []byte("123456789"))); got != 0xe9c6d914c4b8d9ca {
		t.Errorf("crc64(123456789) = %016x, want e9c6d914c4b8d9ca", got)
	}
}

func TestCRC64TableMatchesReference(t *testing.T) {
	// The byte-at-a-time table must agree with the raw bit-by-bit algorithm on
	// arbitrary input.
	ref := func(crc uint64, data []byte) uint64 {
		for _, c := range data {
			for bit := uint8(0x01); bit != 0; bit <<= 1 {
				msb := crc & (1 << 63)
				if c&bit != 0 {
					msb = (msb ^ (1 << 63))
				}
				crc <<= 1
				if msb != 0 {
					crc ^= crc64Poly
				}
			}
		}
		return crc
	}
	data := []byte("the quick brown fox jumps over the lazy dog 0123456789")
	if got, want := crc64Update(0, data), ref(0, data); got != want {
		t.Errorf("table crc = %016x, bit-by-bit = %016x", got, want)
	}
}
