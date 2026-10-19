package persist

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// RDB format constants (subset of the real Redis RDB format, version 9).
const (
	rdbVersion = "0009"

	rdbOpcodeAux          = 0xFA
	rdbOpcodeResizeDB     = 0xFB
	rdbOpcodeExpireTimeMS = 0xFC
	rdbOpcodeExpireTime   = 0xFD
	rdbOpcodeSelectDB     = 0xFE
	rdbOpcodeEOF          = 0xFF

	rdbTypeString = 0
	rdbTypeList   = 1
	rdbTypeSet    = 2
	rdbTypeZSet   = 3 // legacy, ASCII double scores
	rdbTypeHash   = 4
	rdbTypeZSet2  = 5 // binary double scores

	// Listpack-backed container types written by modern Redis (7+).
	rdbTypeHashListpack   = 16
	rdbTypeZSetListpack   = 17
	rdbTypeListQuicklist2 = 18
	rdbTypeSetListpack    = 20
	// Legacy types Redis 8 no longer writes (older dumps).
	rdbTypeHashZiplist   = 13
	rdbTypeListZiplist   = 10
	rdbTypeSetIntset     = 11
	rdbTypeZSetZiplist   = 12
	rdbTypeListQuicklist = 14
	// Newer hash types with per-field TTLs (not supported).
	rdbTypeHashMetadata   = 25
	rdbTypeHashListpackEx = 24

	// quicklist node container types (RDB_TYPE_LIST_QUICKLIST_2).
	quicklistNodePlain  = 1
	quicklistNodePacked = 2

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3
	rdbEncInt64 = 4
)

// rdbEncoding identifies a special string encoding returned by readLenEnc.
type rdbEncoding int

const (
	encNone  rdbEncoding = -1
	encInt8  rdbEncoding = rdbEncInt8
	encInt16 rdbEncoding = rdbEncInt16
	encInt32 rdbEncoding = rdbEncInt32
	encLZF   rdbEncoding = rdbEncLZF
	encInt64 rdbEncoding = rdbEncInt64
)

// Save writes an RDB snapshot of st to path atomically (tmp file + fsync +
// rename). Files are created with 0600 permissions.
func Save(path string, st *store.Store) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("persist: create temp: %w", err)
	}
	tmp := f.Name()
	if err := WriteRDB(f, st); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("persist: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("persist: rename: %w", err)
	}
	return nil
}

// WriteRDB serializes the live dataset of st as an RDB snapshot into w.
func WriteRDB(w io.Writer, st *store.Store) error {
	var entries []struct {
		key string
		e   *store.Entry
	}
	err := st.ForEach(func(key string, e *store.Entry) error {
		entries = append(entries, struct {
			key string
			e   *store.Entry
		}{key, e})
		return nil
	})
	if err != nil {
		return err
	}

	buf := newRDBBuf()
	buf.str("REDIS" + rdbVersion)
	buf.byte(rdbOpcodeSelectDB)
	buf.writeLen(0)
	buf.byte(rdbOpcodeResizeDB)
	buf.writeLen(uint64(len(entries)))
	expires := 0
	for _, se := range entries {
		if !se.e.ExpiresAt.IsZero() {
			expires++
		}
	}
	buf.writeLen(uint64(expires))
	for _, se := range entries {
		if !se.e.ExpiresAt.IsZero() {
			buf.byte(rdbOpcodeExpireTimeMS)
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(se.e.ExpiresAt.UnixMilli()))
			buf.raw(b[:])
		}
		if err := rdbWriteValue(buf, se.e, []byte(se.key)); err != nil {
			return err
		}
	}
	buf.byte(rdbOpcodeEOF)
	// Checksum: CRC64 over everything up to (and including) the EOF opcode.
	crc := crc64Final(crc64Update(0, buf.b))
	var cb [8]byte
	binary.LittleEndian.PutUint64(cb[:], crc)
	buf.raw(cb[:])
	_, err = w.Write(buf.b)
	return err
}

// rdbBuf is a small accumulating buffer with length-encoding helpers.
type rdbBuf struct {
	b []byte
}

func newRDBBuf() *rdbBuf { return &rdbBuf{} }

func (b *rdbBuf) byte(v byte)  { b.b = append(b.b, v) }
func (b *rdbBuf) raw(v []byte) { b.b = append(b.b, v...) }
func (b *rdbBuf) str(v string) { b.b = append(b.b, v...) }
func (b *rdbBuf) writeLen(n uint64) {
	switch {
	case n < 1<<6:
		b.byte(byte(n))
	case n < 1<<14:
		b.byte(byte(n>>8) | 0x40)
		b.byte(byte(n))
	case n <= math.MaxUint32:
		b.byte(0x80)
		var t [4]byte
		binary.BigEndian.PutUint32(t[:], uint32(n))
		b.raw(t[:])
	default:
		b.byte(0x81)
		var t [8]byte
		binary.BigEndian.PutUint64(t[:], n)
		b.raw(t[:])
	}
}

func writeStringBuf(b *rdbBuf, v []byte) {
	b.writeLen(uint64(len(v)))
	b.raw(v)
}

// rdbWriteValue serializes a key record: the value-type byte, the key string
// and then the container payload.
func rdbWriteValue(b *rdbBuf, e *store.Entry, key []byte) error {
	var typeByte byte
	switch e.Type {
	case store.TypeString:
		typeByte = rdbTypeString
	case store.TypeList:
		typeByte = rdbTypeList
	case store.TypeSet:
		typeByte = rdbTypeSet
	case store.TypeHash:
		typeByte = rdbTypeHash
	case store.TypeZSet:
		typeByte = rdbTypeZSet2
	default:
		return fmt.Errorf("persist: cannot serialize entry type %v", e.Type)
	}
	b.byte(typeByte)
	writeStringBuf(b, key)
	switch e.Type {
	case store.TypeString:
		writeStringBuf(b, e.Str)
	case store.TypeList:
		elems := e.List.Range(0, -1)
		b.writeLen(uint64(len(elems)))
		for _, v := range elems {
			writeStringBuf(b, v)
		}
	case store.TypeSet:
		b.writeLen(uint64(len(e.Set)))
		for m := range e.Set {
			writeStringBuf(b, []byte(m))
		}
	case store.TypeHash:
		b.writeLen(uint64(len(e.Hash)))
		for f, v := range e.Hash {
			writeStringBuf(b, []byte(f))
			writeStringBuf(b, v)
		}
	case store.TypeZSet:
		entries := e.ZSet.Range(0, -1, false)
		b.writeLen(uint64(len(entries)))
		for _, ze := range entries {
			writeStringBuf(b, ze.Member)
			var d [8]byte
			binary.LittleEndian.PutUint64(d[:], math.Float64bits(ze.Score))
			b.raw(d[:])
		}
	default:
		return fmt.Errorf("persist: cannot serialize entry type %v", e.Type)
	}
	return nil
}

// Load reads an RDB snapshot from r into st. Keys whose expiry has already
// passed (per the store clock) are skipped.
func Load(r io.Reader, st *store.Store) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("persist: read rdb: %w", err)
	}
	return LoadBytes(data, st)
}

// LoadBytes parses an RDB snapshot held in memory.
func LoadBytes(data []byte, st *store.Store) error {
	d := &rdbDecoder{data: data, st: st}
	if err := d.header(); err != nil {
		return err
	}
	if err := d.body(); err != nil {
		return err
	}
	return d.checksum()
}

type rdbDecoder struct {
	data []byte
	pos  int
	st   *store.Store
	exp  time.Time // pending expiry from EXPIRETIME_MS opcode
}

func (d *rdbDecoder) header() error {
	if len(d.data) < 9 || string(d.data[:5]) != "REDIS" {
		return fmt.Errorf("persist: not an RDB file (bad header)")
	}
	d.pos = 9
	return nil
}

func (d *rdbDecoder) body() error {
	for {
		op, err := d.byte()
		if err != nil {
			return err
		}
		switch {
		case op == rdbOpcodeEOF:
			return nil
		case op == rdbOpcodeSelectDB:
			if _, err := d.readLen(); err != nil {
				return err
			}
		case op == rdbOpcodeResizeDB:
			if _, err := d.readLen(); err != nil {
				return err
			}
			if _, err := d.readLen(); err != nil {
				return err
			}
		case op == rdbOpcodeAux:
			if _, err := d.readString(); err != nil {
				return err
			}
			if _, err := d.readString(); err != nil {
				return err
			}
		case op == rdbOpcodeExpireTimeMS:
			var b [8]byte
			if err := d.read(b[:]); err != nil {
				return err
			}
			d.exp = time.UnixMilli(int64(binary.LittleEndian.Uint64(b[:])))
		case op == rdbOpcodeExpireTime:
			var b [4]byte
			if err := d.read(b[:]); err != nil {
				return err
			}
			d.exp = time.Unix(int64(binary.LittleEndian.Uint32(b[:])), 0)
		default:
			if op >= 0x80 {
				return fmt.Errorf("persist: unsupported RDB value type %d", op)
			}
			if err := d.keyValue(op); err != nil {
				return err
			}
		}
	}
}

func (d *rdbDecoder) keyValue(op byte) error {
	key, err := d.readString()
	if err != nil {
		return err
	}
	e := &store.Entry{ExpiresAt: d.exp}
	d.exp = time.Time{}
	switch op {
	case rdbTypeString:
		e.Type = store.TypeString
		if e.Str, err = d.readString(); err != nil {
			return err
		}
	case rdbTypeList:
		e.Type = store.TypeList
		n, err := d.readLen()
		if err != nil {
			return err
		}
		l := store.NewList()
		for i := uint64(0); i < n; i++ {
			v, err := d.readString()
			if err != nil {
				return err
			}
			l.PushBack(v)
		}
		e.List = l
	case rdbTypeSet:
		e.Type = store.TypeSet
		n, err := d.readLen()
		if err != nil {
			return err
		}
		set := make(map[string]struct{}, n)
		for i := uint64(0); i < n; i++ {
			v, err := d.readString()
			if err != nil {
				return err
			}
			set[string(v)] = struct{}{}
		}
		e.Set = set
	case rdbTypeHash:
		e.Type = store.TypeHash
		n, err := d.readLen()
		if err != nil {
			return err
		}
		h := make(map[string][]byte, n)
		for i := uint64(0); i < n; i++ {
			f, err := d.readString()
			if err != nil {
				return err
			}
			v, err := d.readString()
			if err != nil {
				return err
			}
			h[string(f)] = v
		}
		e.Hash = h
	case rdbTypeZSet, rdbTypeZSet2:
		e.Type = store.TypeZSet
		n, err := d.readLen()
		if err != nil {
			return err
		}
		z := store.NewZSet()
		for i := uint64(0); i < n; i++ {
			m, err := d.readString()
			if err != nil {
				return err
			}
			var score float64
			if op == rdbTypeZSet2 {
				var s [8]byte
				if err := d.read(s[:]); err != nil {
					return err
				}
				score = math.Float64frombits(binary.LittleEndian.Uint64(s[:]))
			} else {
				sv, err := d.readString()
				if err != nil {
					return err
				}
				score, err = parseFloatBytes(sv)
				if err != nil {
					return fmt.Errorf("persist: bad zset score: %w", err)
				}
			}
			z.Add(score, m)
		}
		e.ZSet = z
	case rdbTypeHashListpack:
		e.Type = store.TypeHash
		buf, err := d.readString()
		if err != nil {
			return err
		}
		elems, err := decodeListpack(buf)
		if err != nil {
			return err
		}
		if len(elems)%2 != 0 {
			return fmt.Errorf("persist: hash listpack has odd element count")
		}
		h := make(map[string][]byte, len(elems)/2)
		for i := 0; i < len(elems); i += 2 {
			h[string(elems[i])] = elems[i+1]
		}
		e.Hash = h
	case rdbTypeSetListpack:
		e.Type = store.TypeSet
		buf, err := d.readString()
		if err != nil {
			return err
		}
		elems, err := decodeListpack(buf)
		if err != nil {
			return err
		}
		set := make(map[string]struct{}, len(elems))
		for _, m := range elems {
			set[string(m)] = struct{}{}
		}
		e.Set = set
	case rdbTypeZSetListpack:
		e.Type = store.TypeZSet
		buf, err := d.readString()
		if err != nil {
			return err
		}
		elems, err := decodeListpack(buf)
		if err != nil {
			return err
		}
		if len(elems)%2 != 0 {
			return fmt.Errorf("persist: zset listpack has odd element count")
		}
		z := store.NewZSet()
		for i := 0; i < len(elems); i += 2 {
			score, err := parseFloatBytes(elems[i+1])
			if err != nil {
				return fmt.Errorf("persist: bad zset score: %w", err)
			}
			z.Add(score, elems[i])
		}
		e.ZSet = z
	case rdbTypeListQuicklist2:
		e.Type = store.TypeList
		nodes, err := d.readLen()
		if err != nil {
			return err
		}
		l := store.NewList()
		for i := uint64(0); i < nodes; i++ {
			container, err := d.readLen()
			if err != nil {
				return err
			}
			switch container {
			case quicklistNodePlain: // a single plain element
				v, err := d.readString()
				if err != nil {
					return err
				}
				l.PushBack(v)
			case quicklistNodePacked: // a listpack node
				node, err := d.readString()
				if err != nil {
					return err
				}
				elems, err := decodeListpack(node)
				if err != nil {
					return err
				}
				for _, v := range elems {
					l.PushBack(v)
				}
			default:
				return fmt.Errorf("persist: unknown quicklist node container %d", container)
			}
		}
		e.List = l
	case rdbTypeSetIntset, rdbTypeHashZiplist, rdbTypeZSetZiplist, rdbTypeListZiplist, rdbTypeListQuicklist,
		rdbTypeHashMetadata, rdbTypeHashListpackEx:
		return fmt.Errorf("persist: RDB type %d (ziplist/intset/hash-TTL) is not supported", op)
	default:
		return fmt.Errorf("persist: unsupported RDB value type %d", op)
	}
	if !e.ExpiresAt.IsZero() && d.st.Now().After(e.ExpiresAt) {
		return nil // expired while the snapshot was in flight
	}
	d.st.RestoreEntry(string(key), e)
	return nil
}

// checksum verifies the trailing CRC64. A stored checksum of 0 means the file
// was written without one and verification is skipped (like Redis).
func (d *rdbDecoder) checksum() error {
	if len(d.data)-d.pos != 8 {
		return fmt.Errorf("persist: missing rdb checksum (have %d trailing bytes)", len(d.data)-d.pos)
	}
	stored := binary.LittleEndian.Uint64(d.data[d.pos:])
	if stored == 0 {
		return nil
	}
	if crc64Final(crc64Update(0, d.data[:d.pos])) != stored {
		return fmt.Errorf("persist: rdb checksum mismatch")
	}
	return nil
}

func (d *rdbDecoder) byte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := d.data[d.pos]
	d.pos++
	return v, nil
}

func (d *rdbDecoder) read(b []byte) error {
	if len(d.data)-d.pos < len(b) {
		return io.ErrUnexpectedEOF
	}
	copy(b, d.data[d.pos:d.pos+len(b)])
	d.pos += len(b)
	return nil
}

// readLen returns a plain length or a special encoding marker (enc != encNone).
func (d *rdbDecoder) readLen() (uint64, error) {
	b, err := d.byte()
	if err != nil {
		return 0, err
	}
	switch b >> 6 {
	case 0:
		return uint64(b & 0x3F), nil
	case 1:
		b2, err := d.byte()
		if err != nil {
			return 0, err
		}
		return uint64(b&0x3F)<<8 | uint64(b2), nil
	case 2:
		// 0x80 = 32-bit length, 0x81 = 64-bit length.
		switch b {
		case 0x80:
			var t [4]byte
			if err := d.read(t[:]); err != nil {
				return 0, err
			}
			return uint64(binary.BigEndian.Uint32(t[:])), nil
		case 0x81:
			var t [8]byte
			if err := d.read(t[:]); err != nil {
				return 0, err
			}
			return binary.BigEndian.Uint64(t[:]), nil
		default:
			return 0, fmt.Errorf("persist: unknown length encoding byte 0x%02x", b)
		}
	default:
		return 0, fmt.Errorf("persist: unexpected length encoding %d", b)
	}
}

// readLenEnc is like readLen but surfaces special (integer/LZF) encodings.
func (d *rdbDecoder) readLenEnc() (uint64, rdbEncoding, error) {
	b, err := d.byte()
	if err != nil {
		return 0, encNone, err
	}
	switch b >> 6 {
	case 0:
		return uint64(b & 0x3F), encNone, nil
	case 1:
		b2, err := d.byte()
		if err != nil {
			return 0, encNone, err
		}
		return uint64(b&0x3F)<<8 | uint64(b2), encNone, nil
	case 2:
		switch b {
		case 0x80:
			var t [4]byte
			if err := d.read(t[:]); err != nil {
				return 0, encNone, err
			}
			return uint64(binary.BigEndian.Uint32(t[:])), encNone, nil
		case 0x81:
			var t [8]byte
			if err := d.read(t[:]); err != nil {
				return 0, encNone, err
			}
			return binary.BigEndian.Uint64(t[:]), encNone, nil
		default:
			return 0, encNone, fmt.Errorf("persist: unknown length encoding byte 0x%02x", b)
		}
	default:
		return 0, rdbEncoding(b & 0x3F), nil
	}
}

// readString reads a string value, decoding integer and LZF encodings.
func (d *rdbDecoder) readString() ([]byte, error) {
	n, enc, err := d.readLenEnc()
	if err != nil {
		return nil, err
	}
	switch enc {
	case encNone:
		if len(d.data)-d.pos < int(n) {
			return nil, io.ErrUnexpectedEOF
		}
		v := d.data[d.pos : d.pos+int(n)]
		d.pos += int(n)
		return v, nil
	case encInt8, encInt16, encInt32, encInt64:
		var sz int
		switch enc {
		case encInt8:
			sz = 1
		case encInt16:
			sz = 2
		case encInt32:
			sz = 4
		case encInt64:
			sz = 8
		}
		var b [8]byte
		if err := d.read(b[:sz]); err != nil {
			return nil, err
		}
		var v int64
		switch sz {
		case 1:
			v = int64(int8(b[0]))
		case 2:
			v = int64(int16(binary.LittleEndian.Uint16(b[:2])))
		case 4:
			v = int64(int32(binary.LittleEndian.Uint32(b[:4])))
		case 8:
			v = int64(binary.LittleEndian.Uint64(b[:8]))
		}
		return itoa(v), nil
	case encLZF:
		clen, err := d.readLen()
		if err != nil {
			return nil, err
		}
		ulen, err := d.readLen()
		if err != nil {
			return nil, err
		}
		if len(d.data)-d.pos < int(clen) {
			return nil, io.ErrUnexpectedEOF
		}
		in := d.data[d.pos : d.pos+int(clen)]
		d.pos += int(clen)
		out := make([]byte, ulen)
		if err := lzfDecompress(in, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("persist: unknown string encoding %d", enc)
	}
}

func parseFloatBytes(b []byte) (float64, error) {
	var s string
	if len(b) == 0 {
		s = "0"
	} else {
		s = string(b)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return f, nil
}

// itoa renders v as a decimal byte string.
func itoa(v int64) []byte {
	return []byte(fmt.Sprintf("%d", v))
}
