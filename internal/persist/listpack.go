package persist

import (
	"encoding/binary"
	"fmt"
)

// decodeListpack decodes a listpack buffer (the compact serialization Redis
// uses for small hash/list/set/zset containers) into its elements as byte
// strings. Integer elements are converted to their decimal representation.
// Each listpack entry is terminated by a "backlen" suffix encoding the entry's
// payload length; it is skipped as the elements are walked forward.
func decodeListpack(b []byte) ([][]byte, error) {
	elems, err := decodeListpackTyped(b)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(elems))
	for _, e := range elems {
		if e.IsInt {
			out = append(out, []byte(itoa(e.Int)))
			continue
		}
		out = append(out, e.Str)
	}
	return out, nil
}

// listpackElem is one element of a listpack: either an integer or a string.
// The type matters for the streams: their listpacks hold both the numeric
// fields (the identifier deltas, the counters) and the field names.
type listpackElem struct {
	Int   int64
	Str   []byte
	IsInt bool
}

// decodeListpackTyped decodes a listpack buffer into its elements, keeping
// the type of every element.
func decodeListpackTyped(b []byte) ([]listpackElem, error) {
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
	out := make([]listpackElem, 0, count)
	pos := 6
	for i := 0; i < count; i++ {
		if pos >= len(b) {
			return nil, fmt.Errorf("persist: listpack truncated")
		}
		ele := b[pos]
		var v listpackElem
		var consumed int
		switch {
		case ele&0x80 == 0: // 7-bit uint (0..127)
			v = listpackElem{Int: int64(ele), IsInt: true}
			consumed = 1
		case ele&0xC0 == 0x80: // 6-bit string
			n := int(ele & 0x3F)
			if pos+1+n > len(b) {
				return nil, fmt.Errorf("persist: listpack string overrun")
			}
			v = listpackElem{Str: b[pos+1 : pos+1+n]}
			consumed = 1 + n
		case ele&0xE0 == 0xC0: // 13-bit signed int
			if pos+1 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int13")
			}
			val := int64(ele&0x1F)<<8 | int64(b[pos+1])
			if val&0x1000 != 0 {
				val -= 0x2000
			}
			v = listpackElem{Int: val, IsInt: true}
			consumed = 2
		case ele&0xF0 == 0xE0: // 12-bit string
			if pos+1 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated string header")
			}
			n := int(ele&0x0F)<<8 | int(b[pos+1])
			if pos+2+n > len(b) {
				return nil, fmt.Errorf("persist: listpack string overrun")
			}
			v = listpackElem{Str: b[pos+2 : pos+2+n]}
			consumed = 2 + n
		case ele == 0xF0: // 32-bit string
			if pos+4 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated 32-bit string header")
			}
			n := int(binary.LittleEndian.Uint32(b[pos+1 : pos+5]))
			if pos+5+n > len(b) {
				return nil, fmt.Errorf("persist: listpack string overrun")
			}
			v = listpackElem{Str: b[pos+5 : pos+5+n]}
			consumed = 5 + n
		case ele == 0xF1: // int16
			if pos+2 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int16")
			}
			v = listpackElem{Int: int64(int16(binary.LittleEndian.Uint16(b[pos+1 : pos+3]))), IsInt: true}
			consumed = 3
		case ele == 0xF2: // int24
			if pos+3 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int24")
			}
			val := int64(uint32(b[pos+1]) | uint32(b[pos+2])<<8 | uint32(b[pos+3])<<16)
			if val&0x800000 != 0 {
				val -= 0x1000000
			}
			v = listpackElem{Int: val, IsInt: true}
			consumed = 4
		case ele == 0xF3: // int32
			if pos+4 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int32")
			}
			v = listpackElem{Int: int64(int32(binary.LittleEndian.Uint32(b[pos+1 : pos+5]))), IsInt: true}
			consumed = 5
		case ele == 0xF4: // int64
			if pos+8 >= len(b) {
				return nil, fmt.Errorf("persist: listpack truncated int64")
			}
			v = listpackElem{Int: int64(binary.LittleEndian.Uint64(b[pos+1 : pos+9])), IsInt: true}
			consumed = 9
		default:
			return nil, fmt.Errorf("persist: unknown listpack encoding byte 0x%02x", ele)
		}
		out = append(out, v)

		// Skip the entry's backlen suffix. It encodes the length of the
		// payload (not of the whole entry), so the number of backlen bytes
		// follows from the payload length alone.
		k := listpackBacklenSize(consumed)
		if pos+consumed+k > len(b) {
			return nil, fmt.Errorf("persist: listpack backlen overrun")
		}
		if got, want := b[pos+consumed:pos+consumed+k], listpackBacklen(consumed); string(got) != string(want) {
			return nil, fmt.Errorf("persist: listpack backlen mismatch")
		}
		pos += consumed + k
	}
	return out, nil
}

// encodeListpack appends the listpack encoding of elems to dst and returns
// the extended buffer.
//
// The layout is the one Redis uses: a 4 byte total size, a 2 byte element
// count, the elements themselves (each followed by its "backlen" suffix) and
// the 0xFF terminator.
func encodeListpack(dst []byte, elems []listpackElem) []byte {
	start := len(dst)
	// The header is patched in at the end, when the sizes are known.
	dst = append(dst, 0, 0, 0, 0, 0, 0)
	for _, e := range elems {
		payload := encodeListpackElem(e)
		dst = append(dst, payload...)
		dst = append(dst, listpackBacklen(len(payload))...)
	}
	dst = append(dst, 0xFF)
	binary.LittleEndian.PutUint32(dst[start:start+4], uint32(len(dst)-start))
	binary.LittleEndian.PutUint16(dst[start+4:start+6], uint16(len(elems)))
	return dst
}

// encodeListpackElem returns the encoded form of one listpack element: an
// integer or a string.
func encodeListpackElem(e listpackElem) []byte {
	if e.IsInt {
		return encodeListpackInt(e.Int)
	}
	return encodeListpackString(e.Str)
}

// encodeListpackInt encodes an integer the way Redis does: the smallest
// encoding that fits the value.
func encodeListpackInt(v int64) []byte {
	switch {
	case v >= 0 && v <= 127:
		return []byte{byte(v)}
	case v >= -4096 && v <= 4095:
		u := v
		if u < 0 {
			u += 1 << 13
		}
		return []byte{byte(u>>8) | 0xC0, byte(u)}
	case v >= -32768 && v <= 32767:
		u := v
		if u < 0 {
			u += 1 << 16
		}
		return []byte{0xF1, byte(u), byte(u >> 8)}
	case v >= -8388608 && v <= 8388607:
		u := v
		if u < 0 {
			u += 1 << 24
		}
		return []byte{0xF2, byte(u), byte(u >> 8), byte(u >> 16)}
	case v >= -2147483648 && v <= 2147483647:
		u := uint32(v)
		return []byte{0xF3, byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24)}
	default:
		u := uint64(v)
		return []byte{0xF4, byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24),
			byte(u >> 32), byte(u >> 40), byte(u >> 48), byte(u >> 56)}
	}
}

// encodeListpackString encodes a string the way Redis does: a 6 bit length
// for the short strings, a 12 bit length for the medium ones and a 32 bit
// length for the rest.
func encodeListpackString(s []byte) []byte {
	n := len(s)
	switch {
	case n < 64:
		out := make([]byte, 0, 1+n)
		out = append(out, 0x80|byte(n))
		return append(out, s...)
	case n < 4096:
		out := make([]byte, 0, 2+n)
		out = append(out, 0xE0|byte((n>>8)&0x0F), byte(n))
		return append(out, s...)
	default:
		out := make([]byte, 0, 5+n)
		out = append(out, 0xF0, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
		return append(out, s...)
	}
}

// listpackBacklenSize returns the number of bytes used by the listpack
// backlen encoding for an entry with a payload of t bytes.
func listpackBacklenSize(t int) int {
	switch {
	case t <= 127:
		return 1
	case t < 16383:
		return 2
	case t < 2097151:
		return 3
	case t < 268435455:
		return 4
	default:
		return 5
	}
}

// listpackBacklen returns the on-disk backlen bytes for an entry with a
// payload of t bytes. The stored value is the payload length, split into
// 7-bit groups: the most significant group first, the least significant one
// last with the high bit set.
func listpackBacklen(t int) []byte {
	switch {
	case t <= 127:
		return []byte{byte(t)}
	case t < 16383:
		return []byte{byte(t >> 7), byte(t&127) | 0x80}
	case t < 2097151:
		return []byte{byte(t >> 14), byte((t>>7)&127) | 0x80, byte(t&127) | 0x80}
	case t < 268435455:
		return []byte{byte(t >> 21), byte((t>>14)&127) | 0x80, byte((t>>7)&127) | 0x80, byte(t&127) | 0x80}
	default:
		return []byte{byte(t >> 28), byte((t>>21)&127) | 0x80, byte((t>>14)&127) | 0x80,
			byte((t>>7)&127) | 0x80, byte(t&127) | 0x80}
	}
}
