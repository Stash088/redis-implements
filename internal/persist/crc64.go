package persist

import "math/bits"

// CRC-64 as implemented by Redis for its RDB checksums (crc64.c, poly
// 0xad93d23594c935a9, reflected output). The byte-at-a-time table below is
// derived from Redis's bit-by-bit algorithm and verified against the reference
// value crc64(0, "123456789") == 0xe9c6d914c4b8d9ca.
const crc64Poly = 0xad93d23594c935a9

var (
	// crc64Table[i] is the 8-step register contribution of byte i.
	crc64Table [256]uint64
	// crc64Rev[i] is i with its bits reversed.
	crc64Rev [256]byte
)

func init() {
	for i := 0; i < 256; i++ {
		crc64Rev[i] = reverse8(uint8(i))
		crc := uint64(0)
		for bit := uint8(0x01); bit != 0; bit <<= 1 {
			msb := crc & (1 << 63)
			if i&int(bit) != 0 {
				msb ^= 1 << 63
			}
			crc <<= 1
			if msb != 0 {
				crc ^= crc64Poly
			}
		}
		crc64Table[i] = crc
	}
}

func reverse8(b byte) byte {
	b = b>>4 | b<<4
	b = (b>>2)&0x33 | (b<<2)&0xCC
	b = (b>>1)&0x55 | (b<<1)&0xAA
	return b
}

// crc64Update extends the running checksum over b using Redis's inner algorithm.
func crc64Update(crc uint64, b []byte) uint64 {
	for _, c := range b {
		crc = crc<<8 ^ crc64Table[crc64Rev[byte(crc>>56)]^c]
	}
	return crc
}

// crc64Final applies the output reflection Redis performs on the final value.
func crc64Final(crc uint64) uint64 { return bits.Reverse64(crc) }
