package store

import (
	"errors"
	"math"
	"math/bits"

	"strings"
	"time"
)

// The bitmap commands treat the string value of a key as an array of bits:
// bit 0 is the most significant bit of the first byte, bit 7 its least
// significant one, bit 8 the most significant bit of the second byte. The
// semantics follow the reference implementation (Redis 8.2, src/bitops.c),
// including the reply quirks of BITPOS on all-ones ranges and the overflow
// arithmetic of BITFIELD.

const (
	// MaxBitOffset is the biggest bit offset the bitmap commands accept: the
	// value must stay under the bulk limit (512 MB, like proto-max-bulk-len
	// of the reference implementation), so the offset may address the last
	// bit of a 512 MB string.
	MaxBitOffset = int64(1)<<32 - 1

	// maxBulkLen is the string size the offsets are validated against.
	maxBulkLen = int64(512) << 20
)

// Parse failures of the bitmap grammar; the server maps them to the reply
// strings of the reference implementation.
var (
	// ErrBitOffset marks a bit offset that is not an integer or is out of range.
	ErrBitOffset = errors.New("store: bit offset is not an integer or out of range")
	// ErrBitValue marks a SETBIT value that is not 0 or 1.
	ErrBitValue = errors.New("store: bit is not an integer or out of range")
	// ErrBitFieldType marks an invalid BITFIELD type.
	ErrBitFieldType = errors.New("store: invalid bitfield type")
	// ErrOverflowType marks an invalid OVERFLOW type.
	ErrOverflowType = errors.New("store: invalid OVERFLOW type")
	// ErrSyntax marks an argument sequence the command grammar does not accept.
	ErrSyntax = errors.New("store: syntax error")
)

// The operations of BITOP. DIFF, DIFF1, ANDOR and ONE are the Redis 8.2
// extensions, byte-wise over the sources:
//
//	DIFF(X, A1..An)  = X & !(A1 | ... | An)
//	DIFF1(X, A1..An) = !X & (A1 | ... | An)
//	ANDOR(X, A1..An) = X & (A1 | ... | An)
//	ONE(A1..An)      = the bits set in exactly one source
const (
	BitOpAnd byte = iota
	BitOpOr
	BitOpXor
	BitOpNot
	BitOpDiff
	BitOpDiff1
	BitOpAndor
	BitOpOne
)

// The subcommands of BITFIELD.
const (
	BitFieldGet uint8 = iota
	BitFieldSet
	BitFieldIncrby
)

// The OVERFLOW policies of BITFIELD. WRAP is the default: the arithmetic
// wraps around; SAT saturates at the minimum or maximum of the type; FAIL
// discards the operation and replies with null.
const (
	OverflowWrap uint8 = iota
	OverflowSat
	OverflowFail
)

// BitFieldOp is one parsed GET/SET/INCRBY operation of BITFIELD.
type BitFieldOp struct {
	Op     uint8
	Offset uint64
	// Value is the SET value or the INCRBY increment.
	Value  int64
	Bits   int
	Signed bool
	Over   uint8
}

// BitFieldResult is one reply element of BITFIELD. Null is set when an
// OVERFLOW FAIL discarded the operation.
type BitFieldResult struct {
	Val  int64
	Null bool
}

// ParseBitOp maps the operation name of BITOP to its code. ok is false for a
// name the command does not know.
func ParseBitOp(name []byte) (byte, bool) {
	switch strings.ToLower(string(name)) {
	case "and":
		return BitOpAnd, true
	case "or":
		return BitOpOr, true
	case "xor":
		return BitOpXor, true
	case "not":
		return BitOpNot, true
	case "diff":
		return BitOpDiff, true
	case "diff1":
		return BitOpDiff1, true
	case "andor":
		return BitOpAndor, true
	case "one":
		return BitOpOne, true
	}
	return 0, false
}

// parseStrictLL parses s the way the reference implementation's string2ll
// does: the sign is only '-', a lone "0" is fine but leading zeros are not,
// the digits carry no trailing bytes and the value fits an int64. This keeps
// the bitmap arguments as strict as Redis's ("01" and "-0" fail).
func parseStrictLL(s []byte) (int64, bool) {
	// The reference rejects numbers of LONG_STR_SIZE (21) digits or more.
	const longStrSize = 21
	if len(s) == 0 || len(s) >= longStrSize {
		return 0, false
	}
	if len(s) == 1 && s[0] == '0' {
		return 0, true
	}
	neg := false
	i := 0
	if s[0] == '-' {
		neg = true
		i++
	}
	// The first digit must be 1-9: "-0" and "-01" fail.
	if i >= len(s) || s[i] < '1' || s[i] > '9' {
		return 0, false
	}
	var v uint64 = uint64(s[i] - '0')
	i++
	for ; i < len(s); i++ {
		d := s[i]
		if d < '0' || d > '9' {
			return 0, false
		}
		if v > math.MaxUint64/10 {
			return 0, false
		}
		v *= 10
		if v > math.MaxUint64-uint64(d-'0') {
			return 0, false
		}
		v += uint64(d - '0')
	}
	if neg {
		if v > uint64(1)<<63 {
			return 0, false
		}
		return -int64(v-1) - 1, true
	}
	if v > math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

// ParseBitOffset parses the offset argument of SETBIT/GETBIT and of the
// BITFIELD subcommands. When hash is set and the argument starts with '#',
// the number is multiplied by bits (the "#n" form of BITFIELD, which addresses
// the n-th field of the given width). The offset must address a bit inside the
// bulk limit.
func ParseBitOffset(b []byte, hash bool, bitCount int) (uint64, error) {
	body := b
	if hash && bitCount > 0 && len(b) > 0 && b[0] == '#' {
		body = b[1:]
	}
	off, ok := parseStrictLL(body)
	if !ok {
		return 0, ErrBitOffset
	}
	if len(body) != len(b) {
		off *= int64(bitCount)
	}
	if off < 0 || off>>3 >= maxBulkLen {
		return 0, ErrBitOffset
	}
	return uint64(off), nil
}

// ParseBitValue parses the 0/1 value argument of SETBIT.
func ParseBitValue(b []byte) (int64, error) {
	n, ok := parseStrictLL(b)
	if !ok || n&^1 != 0 {
		return 0, ErrBitValue
	}
	return n, nil
}

// ParseBitFieldType parses a BITFIELD type: 'i' or 'u' followed by the width.
// The spelling of the sign letter is case sensitive, like in the reference
// implementation. u64 is rejected: the protocol cannot carry unsigned 64-bit
// values; i64 is fine.
func ParseBitFieldType(b []byte) (signed bool, width int, err error) {
	if len(b) == 0 {
		return false, 0, ErrBitFieldType
	}
	switch b[0] {
	case 'i':
		signed = true
	case 'u':
		signed = false
	default:
		return false, 0, ErrBitFieldType
	}
	n, ok := parseStrictLL(b[1:])
	if !ok || n < 1 || (signed && n > 64) || (!signed && n > 63) {
		return false, 0, ErrBitFieldType
	}
	return signed, int(n), nil
}

// ParseBitFieldOps parses the subcommand sequence of BITFIELD and
// BITFIELD_RO: a series of GET/SET/INCRBY operations with the OVERFLOW policy
// applied to the operations that follow it (WRAP until told otherwise).
func ParseBitFieldOps(args [][]byte) ([]BitFieldOp, error) {
	over := OverflowWrap
	var ops []BitFieldOp
	for i := 0; i < len(args); i++ {
		rem := len(args) - i - 1
		sub := strings.ToLower(string(args[i]))
		var op uint8
		switch {
		case sub == "get" && rem >= 2:
			op = BitFieldGet
		case sub == "set" && rem >= 3:
			op = BitFieldSet
		case sub == "incrby" && rem >= 3:
			op = BitFieldIncrby
		case sub == "overflow" && rem >= 1:
			switch strings.ToLower(string(args[i+1])) {
			case "wrap":
				over = OverflowWrap
			case "sat":
				over = OverflowSat
			case "fail":
				over = OverflowFail
			default:
				return nil, ErrOverflowType
			}
			i++
			continue
		default:
			return nil, ErrSyntax
		}
		signed, width, err := ParseBitFieldType(args[i+1])
		if err != nil {
			return nil, err
		}
		offset, err := ParseBitOffset(args[i+2], true, width)
		if err != nil {
			return nil, err
		}
		bf := BitFieldOp{Op: op, Offset: offset, Bits: width, Signed: signed, Over: over}
		if op != BitFieldGet {
			v, ok := parseStrictLL(args[i+3])
			if !ok {
				return nil, ErrNotInteger
			}
			bf.Value = v
		}
		ops = append(ops, bf)
		if op == BitFieldGet {
			i += 2
		} else {
			i += 3
		}
	}
	return ops, nil
}

// SetBit sets or clears the bit at offset in the value of key and returns the
// previous bit. The value is padded with zero bytes up to the addressed byte
// even when the bit does not change, like in the reference implementation.
// The TTL of the key is kept; a non-string value yields ErrWrongType.
func (s *Store) SetBit(key []byte, offset uint64, bit int) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if ok && e.Type != TypeString {
		return 0, ErrWrongType
	}

	byteIdx := int(offset >> 3)
	bitIdx := 7 - int(offset&7)

	// Build the value the bit is set in: a missing or too short value is
	// padded with zeroes, an existing one is copied (the stored buffers are
	// never modified in place).
	var buf []byte
	oldLen := 0
	if !ok {
		buf = make([]byte, byteIdx+1)
	} else if byteIdx < len(e.Str) {
		buf, oldLen = e.Str, len(e.Str)
	} else {
		grown := make([]byte, byteIdx+1)
		copy(grown, e.Str)
		buf, oldLen = grown, len(e.Str)
	}

	old := int((buf[byteIdx] >> uint(bitIdx)) & 1)
	if !ok || byteIdx >= oldLen || old != bit {
		out := buf
		if ok && byteIdx < oldLen {
			// The bit of an existing, long enough value changes: publish a
			// new buffer, readers may still hold the old one.
			out = make([]byte, oldLen)
			copy(out, buf)
		}
		out[byteIdx] &^= 1 << uint(bitIdx)
		if bit == 1 {
			out[byteIdx] |= 1 << uint(bitIdx)
		}
		if ok {
			s.addCost(sh, e, len(out)-oldLen)
			e.Str = out
			s.touchKeyLocked(sh, key)
		} else {
			s.newStringEntry(sh, key, out, time.Time{})
		}
	}
	return old, nil
}

// GetBit returns the bit at offset of the value of key. ok is false when the
// key is missing or its TTL has passed; a non-string value yields
// ErrWrongType. Bits past the end of the value are zero.
func (s *Store) GetBit(key []byte, offset uint64) (int, bool, error) {
	v, ok, err := readEntry(s, key, func(e *Entry) ([]byte, error) {
		if e.Type != TypeString {
			return nil, ErrWrongType
		}
		return e.Str, nil
	})
	if err != nil || !ok {
		return 0, false, err
	}
	byteIdx := int(offset >> 3)
	if byteIdx >= len(v) {
		return 0, true, nil
	}
	return int((v[byteIdx] >> (7 - int(offset&7))) & 1), true, nil
}

// BitCount counts the bits set to 1 in the value of key. With hasRange the
// inclusive indices start..end address bytes, or bits when isbit; negative
// indices count from the end. A missing key counts zero bits; a non-string
// value yields ErrWrongType.
func (s *Store) BitCount(key []byte, start, end int64, hasRange, isbit bool) (int64, error) {
	v, ok, err := readEntry(s, key, func(e *Entry) ([]byte, error) {
		if e.Type != TypeString {
			return nil, ErrWrongType
		}
		return e.Str, nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}

	totlen := int64(len(v))
	var firstMask, lastMask byte
	if !hasRange {
		start, end = 0, totlen-1
	} else {
		if start < 0 && end < 0 && start > end {
			return 0, nil
		}
		if isbit {
			totlen <<= 3
		}
		if start < 0 {
			start = totlen + start
		}
		if end < 0 {
			end = totlen + end
		}
		if start < 0 {
			start = 0
		}
		if end < 0 {
			end = 0
		}
		if end >= totlen {
			end = totlen - 1
		}
		if isbit && start <= end {
			// The range may cover parts of the first and the last byte only:
			// remember the bits outside it, and exclude them from the count.
			firstMask = byte(^((1 << uint(8-(start&7))) - 1) & 0xFF)
			lastMask = byte((1 << uint(7-(end&7))) - 1)
			start >>= 3
			end >>= 3
		}
	}
	if start > end {
		return 0, nil
	}
	count := popcount(v[start : end+1])
	if firstMask != 0 || lastMask != 0 {
		var outside [2]byte
		if firstMask != 0 {
			outside[0] = v[start] & firstMask
		}
		if lastMask != 0 {
			outside[1] = v[end] & lastMask
		}
		count -= popcount(outside[:])
	}
	return count, nil
}

// BitPos returns the position of the first bit equal to bit in the value of
// key. The inclusive indices start..end address bytes, or bits when isbit;
// hasStart/hasEnd tell whether the caller gave them (the reply differs: an
// explicit end turns the zero padding past the value into "not found").
// A missing key is an infinite run of zeroes: the first clear bit is 0 and
// the first set bit does not exist (-1).
func (s *Store) BitPos(key []byte, bit int, start, end int64, hasStart, hasEnd, isbit bool) (int64, error) {
	v, ok, err := readEntry(s, key, func(e *Entry) ([]byte, error) {
		if e.Type != TypeString {
			return nil, ErrWrongType
		}
		return e.Str, nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		if bit == 1 {
			return -1, nil
		}
		return 0, nil
	}

	totlen := int64(len(v))
	var firstMask, lastMask byte
	if !hasStart {
		start, end = 0, totlen-1
	} else {
		if isbit {
			totlen <<= 3
		}
		if !hasEnd {
			// Without an explicit end the value is zero padded on the right.
			end = totlen - 1
		}
		if start < 0 {
			start = totlen + start
		}
		if end < 0 {
			end = totlen + end
		}
		if start < 0 {
			start = 0
		}
		if end < 0 {
			end = 0
		}
		if end >= totlen {
			end = totlen - 1
		}
		if isbit && start <= end {
			firstMask = byte(^((1 << uint(8-(start&7))) - 1) & 0xFF)
			lastMask = byte((1 << uint(7-(end&7))) - 1)
			start >>= 3
			end >>= 3
		}
	}

	// An empty range contains neither a set nor a clear bit.
	if start > end {
		return -1, nil
	}

	rangeBytes := end - start + 1
	var pos int64
	done := false
	if firstMask != 0 {
		tmp := v[start]
		if bit == 1 {
			tmp &^= firstMask
		} else {
			tmp |= firstMask
		}
		if lastMask != 0 && rangeBytes == 1 {
			if bit == 1 {
				tmp &^= lastMask
			} else {
				tmp |= lastMask
			}
		}
		pos = bitpos([]byte{tmp}, bit)
		if rangeBytes == 1 || (pos != -1 && pos != 8) {
			done = true
		} else {
			start++
			rangeBytes--
		}
	}
	if !done {
		cur := rangeBytes
		if lastMask != 0 {
			cur--
		}
		if cur > 0 {
			pos = bitpos(v[start:start+cur], bit)
			if rangeBytes == cur || (pos != -1 && pos != cur<<3) {
				done = true
			} else {
				start += cur
				rangeBytes -= cur
			}
		}
	}
	if !done {
		tmp := v[end]
		if bit == 1 {
			tmp &^= lastMask
		} else {
			tmp |= lastMask
		}
		pos = bitpos([]byte{tmp}, bit)
	}

	// With an explicit end the value is not zero padded past the range: a
	// result that lands past it means no such bit inside the range.
	if hasEnd && bit == 0 && pos == rangeBytes<<3 {
		return -1, nil
	}
	if pos != -1 {
		pos += start << 3
	}
	return pos, nil
}

// BitOp computes the bitwise operation op over the values of srcs and stores
// the result under dest, replacing whatever the key held (like SET, including
// the removal of its TTL). Sources that do not exist count as empty values;
// the result has the length of the longest source, and when all of them are
// empty the destination key is removed. It returns the length in bytes of the
// result. A non-string source yields ErrWrongType and nothing is written.
func (s *Store) BitOp(op byte, dest []byte, srcs [][]byte) (int64, error) {
	keys := make([][]byte, 0, len(srcs)+1)
	keys = append(keys, dest)
	keys = append(keys, srcs...)
	unlock := s.lockWrite(keys...)
	defer unlock()

	now := s.clock()
	vals := make([][]byte, len(srcs))
	maxlen := 0
	for i, src := range srcs {
		sh := s.shardOf(src)
		e, ok := sh.data[string(src)]
		if ok && e.Expired(now) {
			ok = false
		}
		if !ok {
			continue
		}
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		e.lastAccess.Store(now.UnixNano())
		vals[i] = e.Str
		if len(e.Str) > maxlen {
			maxlen = len(e.Str)
		}
	}

	sh := s.shardOf(dest)
	k := string(dest)
	if maxlen > 0 {
		s.setStringLocked(sh, dest, bitOpCompute(op, vals, maxlen), time.Time{})
	} else if e, ok := sh.data[k]; ok {
		s.touchLocked(sh, k)
		s.addBytes(sh, -int64(e.cost))
		delete(sh.data, k)
	}
	return int64(maxlen), nil
}

// bitOpCompute applies op to the sources over maxlen bytes; the bytes past
// the end of a value count as zeroes.
func bitOpCompute(op byte, srcs [][]byte, maxlen int) []byte {
	res := make([]byte, maxlen)
	byteAt := func(src []byte, i int) byte {
		if i < len(src) {
			return src[i]
		}
		return 0
	}
	for i := 0; i < maxlen; i++ {
		out := byteAt(srcs[0], i)
		var disj, common byte
		for _, src := range srcs[1:] {
			b := byteAt(src, i)
			switch op {
			case BitOpAnd:
				out &= b
			case BitOpOr:
				out |= b
			case BitOpXor:
				out ^= b
			case BitOpDiff, BitOpDiff1, BitOpAndor:
				disj |= b
			case BitOpOne:
				common |= out & b
				out ^= b
				out &^= common
			}
		}
		switch op {
		case BitOpNot:
			out = ^out
		case BitOpDiff:
			out &^= disj
		case BitOpDiff1:
			out = ^out & disj
		case BitOpAndor:
			out &= disj
		}
		res[i] = out
	}
	return res
}

// BitField executes the GET/SET/INCRBY operations on the bitmap value of key,
// in order, and returns one result per operation. The reply of SET is the
// previous value, the reply of INCRBY the new one; OVERFLOW FAIL discards the
// operation and reports Null. The value is padded up to the highest bit any
// write operation touches; a non-string value yields ErrWrongType.
func (s *Store) BitField(key []byte, ops []BitFieldOp) ([]BitFieldResult, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if ok && e.Type != TypeString {
		return nil, ErrWrongType
	}

	readonly := true
	var highest uint64
	for i := range ops {
		if ops[i].Op != BitFieldGet {
			readonly = false
			if hi := ops[i].Offset + uint64(ops[i].Bits) - 1; hi > highest {
				highest = hi
			}
		}
	}

	// The buffer the operations run on. The stored buffers are never modified
	// in place: a write command always works on a fresh copy, padded up to
	// the highest bit it may touch.
	var buf []byte
	oldLen := 0
	grew := false
	if readonly {
		if ok {
			buf = e.Str
		}
	} else {
		if ok {
			oldLen = len(e.Str)
		}
		need := int(highest>>3) + 1
		if need < oldLen {
			need = oldLen
		}
		buf = make([]byte, need)
		if ok {
			copy(buf, e.Str)
		}
		grew = need > oldLen
	}

	changes := grew
	out := make([]BitFieldResult, 0, len(ops))
	for i := range ops {
		op := ops[i]
		switch op.Op {
		case BitFieldGet:
			out = append(out, BitFieldResult{Val: bitFieldGet(buf, op.Offset, op.Bits, op.Signed)})

		case BitFieldSet, BitFieldIncrby:
			var overflow int
			var newv int64
			if op.Signed {
				old := getSignedBitfield(buf, op.Offset, op.Bits)
				var wrapped int64
				if op.Op == BitFieldIncrby {
					overflow = checkSignedBitfieldOverflow(old, op.Value, op.Bits, op.Over, &wrapped)
					if overflow != 0 {
						newv = wrapped
					} else {
						newv = old + op.Value
					}
					if overflow == 0 || op.Over != OverflowFail {
						out = append(out, BitFieldResult{Val: newv})
					} else {
						out = append(out, BitFieldResult{Null: true})
					}
				} else {
					newv = op.Value
					overflow = checkSignedBitfieldOverflow(newv, 0, op.Bits, op.Over, &wrapped)
					if overflow != 0 {
						newv = wrapped
					}
					if overflow == 0 || op.Over != OverflowFail {
						out = append(out, BitFieldResult{Val: old})
					} else {
						out = append(out, BitFieldResult{Null: true})
					}
				}
				if overflow == 0 || op.Over != OverflowFail {
					setSignedBitfield(buf, op.Offset, op.Bits, newv)
					if grew || old != newv {
						changes = true
					}
				}
			} else {
				old := getUnsignedBitfield(buf, op.Offset, op.Bits)
				var wrapped uint64
				var newv uint64
				if op.Op == BitFieldIncrby {
					overflow = checkUnsignedBitfieldOverflow(old, op.Value, op.Bits, op.Over, &wrapped)
					if overflow != 0 {
						newv = wrapped
					} else {
						newv = old + uint64(op.Value)
					}
					if overflow == 0 || op.Over != OverflowFail {
						out = append(out, BitFieldResult{Val: int64(newv)})
					} else {
						out = append(out, BitFieldResult{Null: true})
					}
				} else {
					newv = uint64(op.Value)
					overflow = checkUnsignedBitfieldOverflow(newv, 0, op.Bits, op.Over, &wrapped)
					if overflow != 0 {
						newv = wrapped
					}
					if overflow == 0 || op.Over != OverflowFail {
						out = append(out, BitFieldResult{Val: int64(old)})
					} else {
						out = append(out, BitFieldResult{Null: true})
					}
				}
				if overflow == 0 || op.Over != OverflowFail {
					setUnsignedBitfield(buf, op.Offset, op.Bits, newv)
					if grew || old != newv {
						changes = true
					}
				}
			}
		}
	}

	// Publish the buffer only when something actually changed: an all-GET
	// command and a sequence of no-op writes leave the entry untouched.
	if !readonly && changes {
		if ok {
			s.addCost(sh, e, len(buf)-oldLen)
			e.Str = buf
			s.touchKeyLocked(sh, key)
		} else {
			s.newStringEntry(sh, key, buf, time.Time{})
		}
	}
	return out, nil
}

// bitFieldGet reads a signed or unsigned field out of src. The bytes past the
// end of src count as zeroes: the read goes through a zero-padded window, the
// way the reference implementation does it.
func bitFieldGet(src []byte, offset uint64, width int, signed bool) int64 {
	var window [9]byte
	byteIdx := offset >> 3
	for i := 0; i < 9; i++ {
		if int64(byteIdx)+int64(i) >= int64(len(src)) {
			break
		}
		window[i] = src[byteIdx+uint64(i)]
	}
	off := offset - byteIdx*8
	if signed {
		return getSignedBitfield(window[:], off, width)
	}
	return int64(getUnsignedBitfield(window[:], off, width))
}

// popcount counts the bits set to 1 in b.
func popcount(b []byte) int64 {
	var n int64
	for _, v := range b {
		n += int64(bits.OnesCount8(v))
	}
	return n
}

// bitpos returns the position of the first bit equal to bit in b, counting
// from the most significant bit of the first byte. When b holds no such bit
// it returns -1 for bit=1 and len(b)*8 for bit=0: the caller treats the space
// past the buffer as zero padded unless it gave an explicit end.
func bitpos(b []byte, bit int) int64 {
	for i, v := range b {
		if (bit == 1 && v != 0) || (bit == 0 && v != 0xff) {
			for j := 0; j < 8; j++ {
				if int(v>>uint(7-j))&1 == bit {
					return int64(i)*8 + int64(j)
				}
			}
		}
	}
	if bit == 1 {
		return -1
	}
	return int64(len(b)) * 8
}

// getUnsignedBitfield reads an unsigned integer of width bits starting at
// the bit offset. The buffer must be long enough.
func getUnsignedBitfield(p []byte, offset uint64, width int) uint64 {
	var value uint64
	for j := 0; j < width; j++ {
		bit := (p[offset>>3] >> (7 - offset&7)) & 1
		value = value<<1 | uint64(bit)
		offset++
	}
	return value
}

// getSignedBitfield reads a two's complement signed integer of width bits.
func getSignedBitfield(p []byte, offset uint64, width int) int64 {
	value := int64(getUnsignedBitfield(p, offset, width))
	if width < 64 && value&(int64(1)<<uint(width-1)) != 0 {
		value |= -1 << uint(width)
	}
	return value
}

// setUnsignedBitfield writes an unsigned integer of width bits at the bit
// offset. The buffer must be long enough.
func setUnsignedBitfield(p []byte, offset uint64, width int, value uint64) {
	for j := 0; j < width; j++ {
		bitVal := (value >> uint(width-1-j)) & 1
		byteIdx := offset >> 3
		bitIdx := 7 - offset&7
		b := p[byteIdx]
		b &^= 1 << bitIdx
		b |= byte(bitVal << bitIdx)
		p[byteIdx] = b
		offset++
	}
}

// setSignedBitfield writes a signed integer: the two's complement bits of the
// value.
func setSignedBitfield(p []byte, offset uint64, width int, value int64) {
	setUnsignedBitfield(p, offset, width, uint64(value))
}

// checkUnsignedBitfieldOverflow reports whether the arithmetic of value and
// incr fits an unsigned integer of width bits: 1 on overflow, -1 on
// underflow, 0 when it fits. On overflow limit receives the result the
// WRAP and SAT policies produce (FAIL keeps it untouched).
func checkUnsignedBitfieldOverflow(value uint64, incr int64, width int, over uint8, limit *uint64) int {
	var max uint64
	if width == 64 {
		max = math.MaxUint64
	} else {
		max = uint64(1)<<uint(width) - 1
	}
	maxincr := int64(max - value)
	minincr := int64(-value)

	overflow := 0
	if value > max || (incr > 0 && incr > maxincr) {
		overflow = 1
	} else if incr < 0 && incr < minincr {
		overflow = -1
	}
	if overflow == 0 {
		return 0
	}
	if limit != nil {
		switch over {
		case OverflowWrap:
			mask := ^uint64(0) << uint(width)
			*limit = (value + uint64(incr)) &^ mask
		case OverflowSat:
			if overflow == 1 {
				*limit = max
			} else {
				*limit = 0
			}
		}
	}
	return overflow
}

// checkSignedBitfieldOverflow is checkUnsignedBitfieldOverflow for two's
// complement signed integers of width bits.
func checkSignedBitfieldOverflow(value int64, incr int64, width int, over uint8, limit *int64) int {
	var max int64
	if width == 64 {
		max = math.MaxInt64
	} else {
		max = int64(1)<<uint(width-1) - 1
	}
	min := -max - 1
	maxincr := int64(uint64(max) - uint64(value))
	minincr := min - value

	overflow := 0
	if value > max || (width != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
		overflow = 1
	} else if value < min || (width != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr) {
		overflow = -1
	}
	if overflow == 0 {
		return 0
	}
	if limit != nil {
		switch over {
		case OverflowWrap:
			c := uint64(value) + uint64(incr)
			if width < 64 {
				msb := uint64(1) << uint(width-1)
				mask := ^uint64(0) << uint(width)
				if c&msb != 0 {
					c |= mask
				} else {
					c &^= mask
				}
			}
			*limit = int64(c)
		case OverflowSat:
			if overflow == 1 {
				*limit = max
			} else {
				*limit = min
			}
		}
	}
	return overflow
}
