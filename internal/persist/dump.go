package persist

import (
	"encoding/binary"
	"fmt"

	"github.com/amir/redis-implements/internal/store"
)

// DumpValue serializes a value into a self-contained payload, like the one
// the DUMP command returns. The payload holds the RDB encoding of the value
// followed by a CRC64 checksum.
//
// The payload is our own format: the real Redis cannot restore it (its DUMP
// payload is a full RDB chunk with a different layout).
func DumpValue(e *store.Entry) ([]byte, error) {
	buf := newRDBBuf()
	if err := rdbWriteValue(buf, e, nil); err != nil {
		return nil, err
	}
	crc := crc64Final(crc64Update(0, buf.b))
	var sum [8]byte
	binary.LittleEndian.PutUint64(sum[:], crc)
	return append(buf.b, sum[:]...), nil
}

// RestoreValue parses a payload produced by DumpValue and returns the entry it
// holds.
func RestoreValue(data []byte) (*store.Entry, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("persist: dump payload is too short (%d bytes)", len(data))
	}
	body, sum := data[:len(data)-8], data[len(data)-8:]
	if crc64Final(crc64Update(0, body)) != binary.LittleEndian.Uint64(sum) {
		return nil, fmt.Errorf("persist: dump payload checksum mismatch")
	}
	d := &rdbDecoder{data: body}
	op, err := d.byte()
	if err != nil {
		return nil, err
	}
	// The key of the payload is empty: skip it.
	if _, err := d.readString(); err != nil {
		return nil, err
	}
	return d.readValue(op)
}
