package persist

import (
	"encoding/binary"
	"fmt"
)

// decodeListpack decodes a listpack buffer (the compact serialization Redis
// uses for small hash/list/set/zset containers) into its elements as byte
// strings. Integer elements are converted to their decimal representation.
// Each listpack entry is terminated by a "backlen" suffix encoding the entry's
// total length; it is skipped as the elements are walked forward.
func decodeListpack(b []byte) ([][]byte, error) {
	if len(b) < 7 {
		return nil, fmt.Errorf("persist: listpack too short (%d bytes)", len(b))
	}
	if total := binary.LittleEndian.Uint32(b[0:4]); int(total) != len(b) {
		return nil, fmt.Errorf("persist: listpack length mismatch (%d != %d)", total, len(b))
	}
	count := int(binary.LittleEndian.Uint16(b[4:6]))
	if b[len(b)-1] != 0xFF {
		return nil, fmt.Errorf("persist: listpack missing EOF byte")
	}
	out := make([][]byte, 0, count)
	pos := 6
	for i := 0; i < count; i++ {
		if pos >= len(b) {
			return nil, fmt.Errorf("persist: listpack truncated")
		}
		ele := b[pos]
		var v []byte
		var consumed int
		switch {
		case ele&0x80 == 0: // 7-bit uint (0..127)
			v = itoa(int64(ele))
			consumed = 1
		case ele&0xC0 == 0x80: // 6-bit string
			n := int(ele & 0x3F)
			if pos+1+n > len(b) {
				return nil, fmt.Errorf("persist: listpack string overrun")
			}
			v = b[pos+1 : pos+1+n]
			consumed = 1 + n
		case ele&0xE0 == 0xC0: // 13-bit signed int
			if pos+1 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int13")
			}
			val := int64(ele&0x1F)<<8 | int64(b[pos+1])
			if val&0x1000 != 0 {
				val -= 0x2000
			}
			v = itoa(val)
			consumed = 2
		case ele&0xF0 == 0xE0: // 12-bit string
			if pos+1 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated string header")
			}
			n := int(ele&0x0F)<<8 | int(b[pos+1])
			if pos+2+n > len(b) {
				return nil, fmt.Errorf("persist: listpack string overrun")
			}
			v = b[pos+2 : pos+2+n]
			consumed = 2 + n
		case ele == 0xF0: // 32-bit string
			if pos+4 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated 32-bit string header")
			}
			n := int(binary.LittleEndian.Uint32(b[pos+1 : pos+5]))
			if pos+5+n > len(b) {
				return nil, fmt.Errorf("persist: listpack string overrun")
			}
			v = b[pos+5 : pos+5+n]
			consumed = 5 + n
		case ele == 0xF1: // int16
			if pos+2 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int16")
			}
			v = itoa(int64(int16(binary.LittleEndian.Uint16(b[pos+1 : pos+3]))))
			consumed = 3
		case ele == 0xF2: // int24
			if pos+3 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int24")
			}
			val := int64(uint32(b[pos+1]) | uint32(b[pos+2])<<8 | uint32(b[pos+3])<<16)
			if val&0x800000 != 0 {
				val -= 0x1000000
			}
			v = itoa(val)
			consumed = 4
		case ele == 0xF3: // int32
			if pos+4 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int32")
			}
			v = itoa(int64(int32(binary.LittleEndian.Uint32(b[pos+1 : pos+5]))))
			consumed = 5
		case ele == 0xF4: // int64
			if pos+8 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int64")
			}
			v = itoa(int64(binary.LittleEndian.Uint64(b[pos+1 : pos+9])))
			consumed = 9
		default:
			return nil, fmt.Errorf("persist: unknown listpack encoding byte 0x%02x", ele)
		}
		out = append(out, v)

		// Skip the entry's backlen suffix. The backlen encodes the total entry
		// length minus one (header + value + backlen), so we can find the
		// number of backlen bytes from the consumed payload length alone.
		total := consumed
		for k := 1; k <= 5; k++ {
			if listpackBacklenSize(total+k) == k {
				total += k
				break
			}
		}
		if pos+total > len(b) {
			return nil, fmt.Errorf("persist: listpack backlen overrun")
		}
		if got, want := b[pos+consumed:pos+total], listpackBacklen(total); string(got) != string(want) {
			return nil, fmt.Errorf("persist: listpack backlen mismatch")
		}
		pos += total
	}
	return out, nil
}

// listpackBacklenSize returns the number of bytes used by the listpack
// backlen encoding for an entry of total length t (including the backlen).
func listpackBacklenSize(t int) int {
	switch {
	case t <= 127:
		return 1
	case t < 16384:
		return 2
	case t < 2097152:
		return 3
	case t < 268435456:
		return 4
	default:
		return 5
	}
}

// listpackBacklen returns the on-disk backlen bytes for an entry of total
// length t. The stored value is t-1, split into 7-bit groups most-significant
// first with the low groups carrying the high bit.
func listpackBacklen(t int) []byte {
	switch {
	case t <= 127:
		return []byte{byte(t - 1)}
	case t < 16384:
		return []byte{byte((t >> 7) - 1), byte(t&127) | 0x80}
	case t < 2097152:
		return []byte{byte((t >> 14) - 1), byte((t>>7)&127) | 0x80, byte(t&127) | 0x80}
	case t < 268435456:
		return []byte{byte((t >> 21) - 1), byte((t>>14)&127) | 0x80, byte((t>>7)&127) | 0x80, byte(t&127) | 0x80}
	default:
		return []byte{byte((t >> 28) - 1), byte((t>>21)&127) | 0x80, byte((t>>14)&127) | 0x80, byte((t>>7)&127) | 0x80, byte(t&127) | 0x80}
	}
}
