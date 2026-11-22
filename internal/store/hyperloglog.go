package store

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"time"
)

// HyperLogLog, compatible with the Redis implementation (src/hyperloglog.c).
//
// The value is an ordinary string with a 16-byte header:
//
//	"HYLL" | encoding | 3 reserved bytes | cached cardinality (little endian)
//
// A set bit in the most significant bit of the cached cardinality marks the
// cache as invalid. The registers are either 16384 six-bit counters (dense) or
// a run-length encoded sequence of ZERO/XZERO/VAL opcodes (sparse). Keeping the
// representation byte-for-byte compatible means values written by Redis are
// accepted through SET and RDB, and values we write can be read by Redis.

const (
	hllHdrSize         = 16
	hllDenseEncoding   = 0
	hllSparseEncoding  = 1
	hllRegisters       = 1 << 14
	hllRegisterMask    = hllRegisters - 1
	hllBits            = 6
	hllRegisterMax     = (1 << hllBits) - 1
	hllDenseSize       = hllHdrSize + (hllRegisters*hllBits+7)/8
	hllSparseMaxBytes  = 3000
	hllSparseValMax    = 32
	hllSparseValMaxLen = 4
	hllQ               = 64 - 14
	hllAlphaInf        = 0.721347520444481703680
)

var (
	// ErrHLLInvalid means the value has the wrong magic, encoding or length.
	// Redis reports it as a WRONGTYPE-style error.
	ErrHLLInvalid = errors.New("store: invalid HyperLogLog value")
	// ErrHLLCorrupt means the sparse opcode stream does not describe exactly
	// 16384 registers.
	ErrHLLCorrupt = errors.New("store: corrupt HyperLogLog value")
)

// hllHeader validates the fixed part of an HLL value and returns its encoding.
func hllHeader(v []byte) (byte, error) {
	if len(v) < hllHdrSize || v[0] != 'H' || v[1] != 'Y' || v[2] != 'L' || v[3] != 'L' {
		return 0, ErrHLLInvalid
	}
	enc := v[4]
	if enc > hllSparseEncoding {
		return 0, ErrHLLInvalid
	}
	if enc == hllDenseEncoding && len(v) != hllDenseSize {
		return 0, ErrHLLInvalid
	}
	return enc, nil
}

// hllValidate validates the header and, for a sparse value, the whole opcode
// stream. It is used before a mutation is published: a corrupt value must not
// leave a partially updated key behind.
func hllValidate(v []byte) (byte, error) {
	enc, err := hllHeader(v)
	if err != nil {
		return 0, err
	}
	if enc == hllSparseEncoding {
		if err := hllSparseWalk(v, nil); err != nil {
			return 0, err
		}
	}
	return enc, nil
}

// hllNew returns an empty sparse HLL: XZERO 16384.
func hllNew() []byte {
	v := make([]byte, hllHdrSize+2)
	copy(v, "HYLL")
	v[4] = hllSparseEncoding
	// XZERO with length 16384: (len-1) = 0x3fff.
	v[hllHdrSize] = byte((16383 >> 8) | 0x40)
	v[hllHdrSize+1] = byte(16383 & 0xff)
	return v
}

// hllInvalidateCache marks the cached cardinality as stale.
func hllInvalidateCache(v []byte) {
	v[15] |= 0x80
}

// hllCacheValid reports whether the cached cardinality can be trusted.
func hllCacheValid(v []byte) bool {
	return v[15]&0x80 == 0
}

// hllCachedCard returns the cached cardinality. The caller must check
// hllCacheValid first.
func hllCachedCard(v []byte) uint64 {
	var card uint64
	for i := 0; i < 8; i++ {
		card |= uint64(v[8+i]) << (8 * i)
	}
	return card
}

// hllStoreCard writes the cardinality into the header and clears the invalid
// bit.
func hllStoreCard(v []byte, card uint64) {
	for i := 0; i < 8; i++ {
		v[8+i] = byte(card >> (8 * i))
	}
}

// hllSparseWalk decodes a sparse opcode stream and calls fn for every opcode.
// It checks that the opcodes cover exactly 16384 registers.
func hllSparseWalk(v []byte, fn func(idx int, val byte, run int)) error {
	regs := v[hllHdrSize:]
	idx, p := 0, 0
	for p < len(regs) {
		b := regs[p]
		p++
		var run int
		var val byte
		switch {
		case b&0xc0 == 0: // ZERO 00xxxxxx
			run = int(b&0x3f) + 1
		case b&0xc0 == 0x40: // XZERO 01xxxxxx yyyyyyyy
			if p >= len(regs) {
				return ErrHLLCorrupt
			}
			run = int(uint16(b&0x3f)<<8|uint16(regs[p])) + 1
			p++
		default: // VAL 1vvvvvxx
			val = ((b >> 2) & 0x1f) + 1
			run = int(b&0x3) + 1
		}
		if idx+run > hllRegisters {
			return ErrHLLCorrupt
		}
		if fn != nil {
			fn(idx, val, run)
		}
		idx += run
	}
	if idx != hllRegisters {
		return ErrHLLCorrupt
	}
	return nil
}

// hllDenseGet reads one six-bit register.
func hllDenseGet(regs []byte, reg int) byte {
	byteIdx := reg * hllBits / 8
	fb := uint(reg * hllBits & 7)
	var b1 byte
	if byteIdx+1 < len(regs) {
		b1 = regs[byteIdx+1]
	}
	return byte((uint32(regs[byteIdx])>>fb | uint32(b1)<<(8-fb)) & hllRegisterMax)
}

// hllDenseSet stores one six-bit register.
func hllDenseSet(regs []byte, reg int, val byte) {
	byteIdx := reg * hllBits / 8
	fb := uint(reg * hllBits & 7)
	fb8 := 8 - fb
	regs[byteIdx] &= ^byte(uint16(hllRegisterMax) << fb)
	regs[byteIdx] |= val << fb
	if byteIdx+1 < len(regs) {
		regs[byteIdx+1] &= ^byte(uint16(hllRegisterMax) >> fb8)
		regs[byteIdx+1] |= val >> fb8
	}
}

// hllSparseToDense converts a sparse representation to the dense one. The
// cached cardinality of the source is copied, like Redis does.
func hllSparseToDense(v []byte) ([]byte, error) {
	hdr, err := hllHeader(v)
	if err != nil {
		return nil, err
	}
	if hdr == hllDenseEncoding {
		return v, nil
	}
	dense := make([]byte, hllDenseSize)
	copy(dense, v[:hllHdrSize])
	dense[4] = hllDenseEncoding
	regs := dense[hllHdrSize:]
	err = hllSparseWalk(v, func(idx int, val byte, run int) {
		for i := 0; i < run; i++ {
			if val != 0 {
				hllDenseSet(regs, idx+i, val)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return dense, nil
}

// hllSparseMerge joins adjacent VAL opcodes with the same value, starting at
// start (or at the beginning when start is negative), scanning at most five
// opcodes. It mirrors the merge step of hllSparseSet in Redis, so the sparse
// representation stays in canonical form. The value is edited in place: the
// caller owns the buffer.
func hllSparseMerge(v []byte, start int) []byte {
	regs := v[hllHdrSize:]
	p := start
	if p < 0 {
		p = 0
	}
	end := len(regs)
	for scan := 5; p < end && scan > 0; scan-- {
		b := regs[p]
		if b&0xc0 == 0x40 { // XZERO
			p += 2
			continue
		}
		if b&0xc0 == 0 { // ZERO
			p++
			continue
		}
		// VAL followed by another VAL with the same value can be merged if
		// the combined run still fits in the two-bit length field.
		if p+1 < end && regs[p+1]&0x80 != 0 {
			v1 := ((regs[p] >> 2) & 0x1f) + 1
			v2 := ((regs[p+1] >> 2) & 0x1f) + 1
			if v1 == v2 {
				l := int(regs[p]&0x3) + 1 + int(regs[p+1]&0x3) + 1
				if l <= hllSparseValMaxLen {
					regs[p+1] = ((v1 - 1) << 2) | byte(l-1) | 0x80
					copy(regs[p:], regs[p+1:end])
					regs = regs[:end-1]
					end--
					continue
				}
			}
		}
		p++
	}
	return v[:hllHdrSize+len(regs)]
}

// hllSparseSet updates one register of a sparse HLL. v is owned by the caller
// (it must be a copy: the store never mutates a published buffer in place).
func hllSparseSet(v []byte, index int, count byte) ([]byte, bool, error) {
	if count > hllSparseValMax {
		return hllPromoteAdd(v, index, count)
	}
	regs := v[hllHdrSize:]
	end := len(regs)
	p, prev, first, span := 0, -1, 0, 0
	for p < end {
		b := regs[p]
		oplen := 1
		switch {
		case b&0xc0 == 0: // ZERO
			span = int(b&0x3f) + 1
		case b&0xc0 == 0x40: // XZERO
			if p+1 >= end {
				return v, false, ErrHLLCorrupt
			}
			span = int(uint16(b&0x3f)<<8|uint16(regs[p+1])) + 1
			oplen = 2
		default: // VAL
			span = int(b&0x3) + 1
		}
		if index <= first+span-1 {
			break
		}
		prev = p
		p += oplen
		first += span
	}
	if span == 0 || p >= end {
		return v, false, ErrHLLCorrupt
	}
	next := p + 1
	if regs[p]&0xc0 == 0x40 {
		next = p + 2
	}
	if next >= end {
		next = -1
	}
	isZero := regs[p]&0xc0 == 0
	isXZero := regs[p]&0xc0 == 0x40
	isVal := regs[p]&0x80 != 0
	runlen := span

	updated := false
	if isVal {
		old := ((regs[p] >> 2) & 0x1f) + 1
		if old >= count {
			return v, false, nil
		}
		if runlen == 1 {
			regs[p] = ((count - 1) << 2) | 0x80
			updated = true
		}
	}
	if !updated && isZero && runlen == 1 {
		regs[p] = ((count - 1) << 2) | 0x80
		updated = true
	}
	if !updated {
		// Split the current opcode into ZERO/VAL/ZERO or VAL/VAL/VAL.
		last := first + span - 1
		seq := make([]byte, 0, 5)
		if isZero || isXZero {
			if index != first {
				l := index - first
				if l > 64 {
					seq = append(seq, byte((l-1)>>8)|0x40, byte((l-1)&0xff))
				} else {
					seq = append(seq, byte(l-1))
				}
			}
			seq = append(seq, ((count-1)<<2)|0x80)
			if index != last {
				l := last - index
				if l > 64 {
					seq = append(seq, byte((l-1)>>8)|0x40, byte((l-1)&0xff))
				} else {
					seq = append(seq, byte(l-1))
				}
			}
		} else {
			cur := ((regs[p] >> 2) & 0x1f) + 1
			if index != first {
				seq = append(seq, ((cur-1)<<2)|byte(index-first-1)|0x80)
			}
			seq = append(seq, ((count-1)<<2)|0x80)
			if index != last {
				seq = append(seq, ((cur-1)<<2)|byte(last-index-1)|0x80)
			}
		}
		oldlen := 1
		if isXZero {
			oldlen = 2
		}
		delta := len(seq) - oldlen
		if delta > 0 && len(v)+delta > hllSparseMaxBytes {
			return hllPromoteAdd(v, index, count)
		}
		if delta > 0 {
			regs = append(regs, make([]byte, delta)...)
			if next >= 0 {
				copy(regs[next+delta:], regs[next:end])
			}
			copy(regs[p:], seq)
		} else if delta < 0 {
			if next >= 0 {
				copy(regs[next+delta:], regs[next:end])
			}
			regs = regs[:end+delta]
			copy(regs[p:], seq)
		} else {
			copy(regs[p:], seq)
		}
		merged := make([]byte, hllHdrSize+len(regs))
		copy(merged, v[:hllHdrSize])
		copy(merged[hllHdrSize:], regs)
		v = merged
	}
	return hllSparseMerge(v, prev), true, nil
}

// hllPromoteAdd converts a sparse HLL to dense and applies one register
// update.
func hllPromoteAdd(v []byte, index int, count byte) ([]byte, bool, error) {
	dense, err := hllSparseToDense(v)
	if err != nil {
		return v, false, err
	}
	regs := dense[hllHdrSize:]
	old := hllDenseGet(regs, index)
	if count <= old {
		return dense, false, nil
	}
	hllDenseSet(regs, index, count)
	return dense, true, nil
}

// hllMurmurHash64A is the endian-neutral 64-bit MurmurHash2 used by Redis.
func hllMurmurHash64A(data []byte, seed uint64) uint64 {
	const m uint64 = 0xc6a4a7935bd1e995
	const r = 47
	h := seed ^ (uint64(len(data)) * m)
	n := len(data) / 8
	for i := 0; i < n; i++ {
		k := binary.LittleEndian.Uint64(data[i*8:])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}
	tail := data[n*8:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen returns the register index and the length of the zero run of an
// element hash.
func hllPatLen(ele []byte) (index int, count byte) {
	hash := hllMurmurHash64A(ele, 0xadc83b19)
	index = int(hash & hllRegisterMask)
	hash >>= 14
	hash |= uint64(1) << hllQ
	return index, byte(bits.TrailingZeros64(hash) + 1)
}

// hllAdd adds an element to an HLL value. v is owned by the caller.
func hllAdd(v, ele []byte) ([]byte, bool, error) {
	enc, err := hllHeader(v)
	if err != nil {
		return v, false, err
	}
	index, count := hllPatLen(ele)
	if enc == hllDenseEncoding {
		regs := v[hllHdrSize:]
		if count <= hllDenseGet(regs, index) {
			return v, false, nil
		}
		hllDenseSet(regs, index, count)
		return v, true, nil
	}
	return hllSparseSet(v, index, count)
}

// hllMergeInto folds the registers of v into max (element-wise maximum).
func hllMergeInto(max *[hllRegisters]byte, v []byte) error {
	enc, err := hllHeader(v)
	if err != nil {
		return err
	}
	if enc == hllDenseEncoding {
		regs := v[hllHdrSize:]
		for i := 0; i < hllRegisters; i++ {
			if val := hllDenseGet(regs, i); val > max[i] {
				max[i] = val
			}
		}
		return nil
	}
	return hllSparseWalk(v, func(idx int, val byte, run int) {
		if val == 0 {
			return
		}
		for i := 0; i < run; i++ {
			if val > max[idx+i] {
				max[idx+i] = val
			}
		}
	})
}

// hllSigma is the helper from Ertl's cardinality estimator.
func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		zPrime := z
		z += x * y
		y += y
		if zPrime == z {
			return z
		}
	}
}

// hllTau is the helper from Ertl's cardinality estimator.
func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		zPrime := z
		y *= 0.5
		z -= math.Pow(1-x, 2) * y
		if zPrime == z {
			return z / 3
		}
	}
}

// hllEstimate computes the cardinality from a register histogram.
func hllEstimate(histo *[64]int) uint64 {
	m := float64(hllRegisters)
	z := m * hllTau((m-float64(histo[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(histo[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(histo[0])/m)
	e := math.Round(hllAlphaInf * m * m / z)
	if e < 0 {
		return 0
	}
	return uint64(e)
}

// hllCount computes the cardinality of a dense or sparse HLL.
func hllCount(v []byte) (uint64, error) {
	enc, err := hllHeader(v)
	if err != nil {
		return 0, err
	}
	var histo [64]int
	if enc == hllDenseEncoding {
		regs := v[hllHdrSize:]
		for i := 0; i < hllRegisters; i++ {
			histo[hllDenseGet(regs, i)]++
		}
	} else {
		err = hllSparseWalk(v, func(_ int, val byte, run int) {
			histo[val] += run
		})
		if err != nil {
			return 0, err
		}
	}
	return hllEstimate(&histo), nil
}

// hllCountRaw computes the cardinality of a raw register array.
func hllCountRaw(max *[hllRegisters]byte) uint64 {
	var histo [64]int
	for _, val := range max {
		histo[val]++
	}
	return hllEstimate(&histo)
}

// hllMergeDestination writes max into base and returns the new value. When
// useDense is set, the destination is converted to the dense representation;
// otherwise an existing representation is kept and a new destination starts
// sparse, like Redis does.
func hllMergeDestination(base []byte, max *[hllRegisters]byte, useDense bool) ([]byte, error) {
	out := append([]byte(nil), base...)
	if useDense {
		var err error
		out, err = hllSparseToDense(out)
		if err != nil {
			return nil, err
		}
		regs := out[hllHdrSize:]
		for i := 0; i < hllRegisters; i++ {
			hllDenseSet(regs, i, max[i])
		}
		hllInvalidateCache(out)
		return out, nil
	}
	for i := 0; i < hllRegisters; i++ {
		if max[i] == 0 {
			continue
		}
		enc, err := hllHeader(out)
		if err != nil {
			return nil, err
		}
		if enc == hllDenseEncoding {
			regs := out[hllHdrSize:]
			if max[i] > hllDenseGet(regs, i) {
				hllDenseSet(regs, i, max[i])
			}
			continue
		}
		var updated bool
		out, updated, err = hllSparseSet(out, i, max[i])
		if err != nil {
			return nil, err
		}
		_ = updated
	}
	hllInvalidateCache(out)
	return out, nil
}

// HLLMutation is the result of a HyperLogLog write. Value and ExpiresAt are
// captured while the key's shard lock is held, so the caller can propagate the
// resulting string without looking the key up again (another client may have
// changed it in the meantime).
type HLLMutation struct {
	Changed   bool
	Value     []byte
	ExpiresAt time.Time
}

// PFAdd adds elements to the HyperLogLog stored under key, creating it when
// missing. It reports whether the key or any register changed. The value is
// mutated copy-on-write: readers that already hold the old buffer keep it.
func (s *Store) PFAdd(key []byte, elements ...[]byte) (bool, error) {
	m, err := s.PFAddValue(key, elements...)
	return m.Changed, err
}

// PFAddValue is PFAdd returning the resulting value and expiry. The result is
// needed to journal an update of an expiring key as a self-contained SET: a
// replayed raw PFADD would recreate the key if it expired before the replay.
func (s *Store) PFAddValue(key []byte, elements ...[]byte) (HLLMutation, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := s.entryLocked(sh, key)
	if !exists {
		value := hllNew()
		for _, ele := range elements {
			var err error
			var updated bool
			value, updated, err = hllAdd(value, ele)
			if err != nil {
				return HLLMutation{}, err
			}
			_ = updated
		}
		hllInvalidateCache(value)
		s.newStringEntry(sh, key, value, time.Time{})
		return HLLMutation{Changed: true, Value: value}, nil
	}
	if e.Type != TypeString {
		return HLLMutation{}, ErrWrongType
	}
	if _, err := hllValidate(e.Str); err != nil {
		return HLLMutation{}, err
	}
	value := append([]byte(nil), e.Str...)
	changed := false
	for _, ele := range elements {
		var updated bool
		var err error
		value, updated, err = hllAdd(value, ele)
		if err != nil {
			return HLLMutation{}, err
		}
		changed = changed || updated
	}
	if !changed {
		return HLLMutation{}, nil
	}
	hllInvalidateCache(value)
	expiresAt := e.ExpiresAt
	s.newStringEntry(sh, key, value, expiresAt)
	return HLLMutation{Changed: true, Value: value, ExpiresAt: expiresAt}, nil
}

// PFCountKey returns the cardinality of a single key. When the cached value
// is stale the cache is recomputed and published, which is a mutation as far
// as WATCH and the journal are concerned; cacheUpdated reports that.
func (s *Store) PFCountKey(key []byte) (card uint64, cacheUpdated bool, exists bool, err error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := s.entryLocked(sh, key)
	if !ok {
		return 0, false, false, nil
	}
	if e.Type != TypeString {
		return 0, false, false, ErrWrongType
	}
	if _, err := hllHeader(e.Str); err != nil {
		return 0, false, false, err
	}
	if hllCacheValid(e.Str) {
		return hllCachedCard(e.Str), false, true, nil
	}
	card, err = hllCount(e.Str)
	if err != nil {
		return 0, false, false, err
	}
	value := append([]byte(nil), e.Str...)
	hllStoreCard(value, card)
	s.newStringEntry(sh, key, value, e.ExpiresAt)
	return card, true, true, nil
}

// PFCount returns the cardinality of the union of the given keys. Missing keys
// are empty HLLs. Unlike the single-key form it never updates a cache.
func (s *Store) PFCount(keys ...[]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	unlock := s.lockRead(keys...)
	defer unlock()

	now := s.clock()
	var max [hllRegisters]byte
	for _, key := range keys {
		sh := s.shardOf(key)
		e, ok := sh.data[string(key)]
		if !ok || e.Expired(now) {
			continue
		}
		e.lastAccess.Store(now.UnixNano())
		if e.Type != TypeString {
			return 0, ErrWrongType
		}
		if err := hllMergeInto(&max, e.Str); err != nil {
			return 0, err
		}
	}
	return hllCountRaw(&max), nil
}

// PFMerge merges the destination and the sources into the destination. The
// destination is also an input, like in Redis (PFMERGE dest src... includes
// dest in the union). All inputs are validated before the destination is
// touched.
func (s *Store) PFMerge(dest []byte, sources ...[]byte) error {
	_, err := s.PFMergeValue(dest, sources...)
	return err
}

// PFMergeValue is PFMerge returning the resulting value and expiry, captured
// under the shard lock. PFMERGE journals that value as a SET so a replay does
// not depend on inputs whose TTL has expired.
func (s *Store) PFMergeValue(dest []byte, sources ...[]byte) (HLLMutation, error) {
	keys := make([][]byte, 0, len(sources)+1)
	keys = append(keys, dest)
	keys = append(keys, sources...)
	unlock := s.lockWrite(keys...)
	defer unlock()

	now := s.clock()
	var max [hllRegisters]byte
	var destEntry *Entry
	useDense := false
	for i, key := range keys {
		sh := s.shardOf(key)
		e, ok := sh.data[string(key)]
		if !ok || e.Expired(now) {
			continue
		}
		e.lastAccess.Store(now.UnixNano())
		if e.Type != TypeString {
			return HLLMutation{}, ErrWrongType
		}
		if i == 0 {
			// The destination is the first operand. Keeping the live entry
			// (and its TTL) from this same scan avoids a second clock read:
			// if the key expired between the two reads, its registers would
			// be in max but the result would have been made persistent.
			destEntry = e
		}
		// hllMergeInto walks (and validates) the whole sparse stream, so a
		// separate hllValidate pass would read it twice.
		enc, err := hllHeader(e.Str)
		if err != nil {
			return HLLMutation{}, err
		}
		if enc == hllDenseEncoding {
			useDense = true
		}
		if err := hllMergeInto(&max, e.Str); err != nil {
			return HLLMutation{}, err
		}
	}

	sh := s.shardOf(dest)
	base := hllNew()
	var expiresAt time.Time
	if destEntry != nil {
		base = destEntry.Str
		expiresAt = destEntry.ExpiresAt
	}
	value, err := hllMergeDestination(base, &max, useDense)
	if err != nil {
		return HLLMutation{}, err
	}
	s.newStringEntry(sh, dest, value, expiresAt)
	return HLLMutation{Changed: true, Value: value, ExpiresAt: expiresAt}, nil
}
