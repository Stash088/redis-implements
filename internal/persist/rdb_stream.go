package persist

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// The RDB format of the streams is the one Redis 7 introduced and Redis 8.2
// writes: RDB_TYPE_STREAM_LISTPACKS_3 (21). The reader understands the
// older spellings too (15 and 19), so the dumps of Redis 7.x load as well.
//
// The layout of the type:
//
//	number of listpacks
//	  for every node of the stream:
//	    the identifier of the node (16 raw bytes, big endian)
//	    the listpack with the entries of the node
//	length
//	last identifier (ms, seq)
//	first identifier (ms, seq)
//	highest deleted identifier (ms, seq)
//	number of entries ever added
//	number of consumer groups
//	  for every group:
//	    the name of the group
//	    last identifier (ms, seq)
//	    the number of entries read by the group
//	    the pending list of the group: count, then for every entry the
//	      identifier (16 raw bytes), the delivery time and the delivery count
//	    the consumers of the group: count, then for every consumer the name,
//	      the seen time, the active time and its pending list (the count and
//	      the identifiers)
const (
	rdbTypeStreamListpacks   = 15
	rdbTypeStreamListpacks2  = 19
	rdbTypeStreamListpacks3  = 21
	streamNodeEntriesInRDB   = 100 // the entries per node, as in Redis
	streamIDSize             = 16  // the size of a raw stream identifier
	streamItemFlagSameFields = 2   // the entry has the fields of the master
)

// writeStreamValue writes a stream in the RDB format.
func writeStreamValue(b *rdbBuf, e *store.Entry) error {
	if e.Stream == nil {
		return fmt.Errorf("persist: cannot serialize an empty stream")
	}
	snap := e.Stream.Snapshot()
	nodes := streamNodes(snap.Entries)
	b.writeLen(uint64(len(nodes)))
	for _, node := range nodes {
		writeStringBuf(b, encodeStreamID(node.master))
		lp := encodeStreamListpack(node)
		writeStringBuf(b, lp)
	}
	b.writeLen(uint64(len(snap.Entries)))
	// The last identifier.
	b.writeLen(snap.LastID.MS)
	b.writeLen(snap.LastID.Seq)
	// The first identifier: the first entry of the stream (zero for an empty
	// stream, as in Redis).
	first := store.StreamID{}
	if len(snap.Entries) > 0 {
		first = snap.Entries[0].ID
	}
	b.writeLen(first.MS)
	b.writeLen(first.Seq)
	// The highest deleted identifier.
	b.writeLen(snap.MaxDeletedID.MS)
	b.writeLen(snap.MaxDeletedID.Seq)
	// The number of entries ever added.
	b.writeLen(snap.EntriesAdded)

	b.writeLen(uint64(len(snap.Groups)))
	for _, g := range snap.Groups {
		writeStringBuf(b, []byte(g.Name))
		b.writeLen(g.LastID.MS)
		b.writeLen(g.LastID.Seq)
		b.writeLen(g.EntriesRead)
		// The pending list of the group.
		b.writeLen(uint64(len(g.Pending)))
		for _, p := range g.Pending {
			b.raw(encodeStreamID(p.ID))
			b.raw(encodeMillisecondTime(p.DeliveryTime))
			b.writeLen(uint64(p.DeliveryCount))
		}
		// The consumers of the group.
		b.writeLen(uint64(len(g.Consumers)))
		for _, c := range g.Consumers {
			writeStringBuf(b, []byte(c.Name))
			b.raw(encodeMillisecondTime(c.SeenTime))
			b.raw(encodeMillisecondTime(c.ActiveTime))
			b.writeLen(uint64(len(c.Pending)))
			for _, id := range c.Pending {
				b.raw(encodeStreamID(id))
			}
		}
	}
	return nil
}

// streamNode is a group of entries of a stream: the entries of one node of
// the radix tree Redis keeps them in.
type streamNode struct {
	master  store.StreamID
	fields  [][]byte // the field names of the master entry
	entries []store.StreamEntry
}

// streamNodes splits the entries of a stream into the nodes Redis uses.
func streamNodes(entries []store.StreamEntry) []streamNode {
	var nodes []streamNode
	for start := 0; start < len(entries); start += streamNodeEntriesInRDB {
		end := start + streamNodeEntriesInRDB
		if end > len(entries) {
			end = len(entries)
		}
		chunk := entries[start:end]
		node := streamNode{master: chunk[0].ID}
		// The master entry holds the field names, the other entries may have
		// their own.
		node.fields = fieldNames(chunk[0].Fields)
		node.entries = chunk
		nodes = append(nodes, node)
	}
	return nodes
}

// fieldNames returns the names of the fields of an entry: the values are at
// the odd positions of the flat list.
func fieldNames(fields [][]byte) [][]byte {
	names := make([][]byte, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		names = append(names, fields[i])
	}
	return names
}

// fieldValues returns the values of the fields of an entry.
func fieldValues(fields [][]byte) [][]byte {
	values := make([][]byte, 0, len(fields)/2)
	for i := 1; i < len(fields); i += 2 {
		values = append(values, fields[i])
	}
	return values
}

// encodeStreamListpack builds the listpack of a node: the master entry with
// the field names, then every entry with its values.
func encodeStreamListpack(node streamNode) []byte {
	elems := make([]listpackElem, 0, 5+len(node.entries)*3)
	// The header of the node: the number of entries, the number of the
	// deleted entries (we keep no tombstones), the number of the fields of
	// the master entry and the field names themselves.
	elems = append(elems,
		listpackElem{Int: int64(len(node.entries)), IsInt: true},
		listpackElem{Int: 0, IsInt: true},
		listpackElem{Int: int64(len(node.fields)), IsInt: true},
	)
	for _, name := range node.fields {
		elems = append(elems, listpackElem{Str: name})
	}
	// The terminator of the master entry.
	elems = append(elems, listpackElem{Int: 0, IsInt: true})

	for _, entry := range node.entries {
		values := fieldValues(entry.Fields)
		sameFields := sameFieldNames(entry.Fields, node.fields)
		flags := int64(0)
		if sameFields {
			flags = streamItemFlagSameFields
		}
		elems = append(elems,
			listpackElem{Int: flags, IsInt: true},
			listpackElem{Int: int64(entry.ID.MS - node.master.MS), IsInt: true},
			listpackElem{Int: int64(entry.ID.Seq - node.master.Seq), IsInt: true},
		)
		if !sameFields {
			elems = append(elems, listpackElem{Int: int64(len(values)), IsInt: true})
			for _, name := range fieldNames(entry.Fields) {
				elems = append(elems, listpackElem{Str: name})
			}
		}
		for _, v := range values {
			elems = append(elems, listpackElem{Str: v})
		}
		// The number of the pieces of the entry: it lets Redis walk the
		// listpack backwards.
		count := int64(3 + len(values))
		if !sameFields {
			count += int64(len(values) + 1)
		}
		elems = append(elems, listpackElem{Int: count, IsInt: true})
	}
	return encodeListpack(nil, elems)
}

// sameFieldNames reports whether the fields of an entry are the fields of the
// master entry (the names, in the same order).
func sameFieldNames(fields [][]byte, master [][]byte) bool {
	names := fieldNames(fields)
	if len(names) != len(master) {
		return false
	}
	for i := range names {
		if string(names[i]) != string(master[i]) {
			return false
		}
	}
	return true
}

// encodeStreamID returns the raw form of an identifier: two big endian
// 64 bit numbers, as Redis stores them.
func encodeStreamID(id store.StreamID) []byte {
	var b [streamIDSize]byte
	binary.BigEndian.PutUint64(b[0:8], id.MS)
	binary.BigEndian.PutUint64(b[8:16], id.Seq)
	return b[:]
}

// decodeStreamID reads the raw form of an identifier.
func decodeStreamID(b []byte) store.StreamID {
	return store.StreamID{
		MS:  binary.BigEndian.Uint64(b[0:8]),
		Seq: binary.BigEndian.Uint64(b[8:16]),
	}
}

// encodeMillisecondTime returns the time as the 8 byte millisecond count
// Redis stores.
func encodeMillisecondTime(t time.Time) []byte {
	var b [8]byte
	ms := t.UnixMilli()
	if t.IsZero() {
		ms = 0
	}
	binary.LittleEndian.PutUint64(b[:], uint64(ms))
	return b[:]
}

// decodeMillisecondTime reads a millisecond time.
func decodeMillisecondTime(b []byte) time.Time {
	ms := int64(binary.LittleEndian.Uint64(b))
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// readStreamValue reads a stream in the RDB format.
func (d *rdbDecoder) readStreamValue(op byte) (*store.StreamSnapshot, error) {
	snap := &store.StreamSnapshot{}
	nodes, err := d.readLen()
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < nodes; i++ {
		key, err := d.readString()
		if err != nil {
			return nil, err
		}
		if len(key) != streamIDSize {
			return nil, fmt.Errorf("persist: the stream node key is %d bytes, want %d", len(key), streamIDSize)
		}
		master := decodeStreamID(key)
		blob, err := d.readString()
		if err != nil {
			return nil, err
		}
		entries, err := decodeStreamListpack(master, blob)
		if err != nil {
			return nil, err
		}
		snap.Entries = append(snap.Entries, entries...)
	}
	length, err := d.readLen()
	if err != nil {
		return nil, err
	}
	if snap.LastID.MS, err = d.readLen(); err != nil {
		return nil, err
	}
	if snap.LastID.Seq, err = d.readLen(); err != nil {
		return nil, err
	}
	if op >= rdbTypeStreamListpacks2 {
		// The first identifier: we derive it from the entries.
		if _, err = d.readLen(); err != nil {
			return nil, err
		}
		if _, err = d.readLen(); err != nil {
			return nil, err
		}
		if snap.MaxDeletedID.MS, err = d.readLen(); err != nil {
			return nil, err
		}
		if snap.MaxDeletedID.Seq, err = d.readLen(); err != nil {
			return nil, err
		}
		if snap.EntriesAdded, err = d.readLen(); err != nil {
			return nil, err
		}
	} else {
		// The older format does not keep the counters.
		snap.EntriesAdded = length
	}
	groups, err := d.readLen()
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < groups; i++ {
		g, err := d.readStreamGroup()
		if err != nil {
			return nil, err
		}
		snap.Groups = append(snap.Groups, *g)
	}
	return snap, nil
}

// readStreamGroup reads a consumer group of a stream.
func (d *rdbDecoder) readStreamGroup() (*store.StreamGroupSnapshot, error) {
	name, err := d.readString()
	if err != nil {
		return nil, err
	}
	g := &store.StreamGroupSnapshot{Name: string(name)}
	if g.LastID.MS, err = d.readLen(); err != nil {
		return nil, err
	}
	if g.LastID.Seq, err = d.readLen(); err != nil {
		return nil, err
	}
	entriesRead, err := d.readLen()
	if err != nil {
		return nil, err
	}
	g.EntriesRead = entriesRead
	g.EntriesReadValid = true
	// The pending list of the group.
	pel, err := d.readLen()
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < pel; i++ {
		var id [streamIDSize]byte
		if err := d.read(id[:]); err != nil {
			return nil, err
		}
		var at [8]byte
		if err := d.read(at[:]); err != nil {
			return nil, err
		}
		count, err := d.readLen()
		if err != nil {
			return nil, err
		}
		g.Pending = append(g.Pending, store.StreamPendingSnapshot{
			ID:            decodeStreamID(id[:]),
			DeliveryTime:  decodeMillisecondTime(at[:]),
			DeliveryCount: int64(count),
		})
	}
	// The consumers of the group.
	consumers, err := d.readLen()
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < consumers; i++ {
		cname, err := d.readString()
		if err != nil {
			return nil, err
		}
		var seen [8]byte
		if err := d.read(seen[:]); err != nil {
			return nil, err
		}
		var active [8]byte
		if err := d.read(active[:]); err != nil {
			return nil, err
		}
		c := store.StreamConsumerSnapshot{
			Name:       string(cname),
			SeenTime:   decodeMillisecondTime(seen[:]),
			ActiveTime: decodeMillisecondTime(active[:]),
		}
		pending, err := d.readLen()
		if err != nil {
			return nil, err
		}
		for j := uint64(0); j < pending; j++ {
			var id [streamIDSize]byte
			if err := d.read(id[:]); err != nil {
				return nil, err
			}
			c.Pending = append(c.Pending, decodeStreamID(id[:]))
		}
		g.Consumers = append(g.Consumers, c)
	}
	// The pending list of the group does not name the consumers: the owner
	// of an entry is the consumer whose pending list holds it.
	owners := make(map[store.StreamID]string)
	for _, c := range g.Consumers {
		for _, id := range c.Pending {
			owners[id] = c.Name
		}
	}
	for i := range g.Pending {
		if owner, ok := owners[g.Pending[i].ID]; ok {
			g.Pending[i].Consumer = owner
		}
	}
	return g, nil
}

// decodeStreamListpack parses the listpack of a node of a stream.
func decodeStreamListpack(master store.StreamID, blob []byte) ([]store.StreamEntry, error) {
	elems, err := decodeListpackTyped(blob)
	if err != nil {
		return nil, err
	}
	// The header of the node: the number of the entries, the number of the
	// deleted entries, the number of the fields of the master entry, the
	// field names and the terminator.
	if len(elems) < 4 {
		return nil, fmt.Errorf("persist: the stream listpack is too short")
	}
	numFields := int(elems[2].Int)
	pos := 3 + numFields
	if pos >= len(elems) {
		return nil, fmt.Errorf("persist: the stream listpack has no terminator")
	}
	masterFields := make([][]byte, 0, numFields)
	for i := 0; i < numFields; i++ {
		masterFields = append(masterFields, elems[3+i].Str)
	}
	pos++ // the terminator of the master entry

	var entries []store.StreamEntry
	for pos < len(elems) {
		if pos+3 > len(elems) {
			return nil, fmt.Errorf("persist: the stream listpack is truncated")
		}
		flags := elems[pos].Int
		msDelta := elems[pos+1].Int
		seqDelta := elems[pos+2].Int
		pos += 3
		fields := make([][]byte, 0, 2*numFields)
		numValues := numFields
		if flags&streamItemFlagSameFields == 0 {
			if pos >= len(elems) {
				return nil, fmt.Errorf("persist: the stream listpack is truncated")
			}
			numValues = int(elems[pos].Int)
			pos++
			for i := 0; i < numValues; i++ {
				if pos >= len(elems) {
					return nil, fmt.Errorf("persist: the stream listpack is truncated")
				}
				fields = append(fields, elems[pos].Str)
				pos++
			}
		} else {
			fields = append(fields, masterFields...)
		}
		for i := 0; i < numValues; i++ {
			if pos >= len(elems) {
				return nil, fmt.Errorf("persist: the stream listpack is truncated")
			}
			fields = append(fields, elems[pos].Str)
			pos++
		}
		if pos >= len(elems) {
			return nil, fmt.Errorf("persist: the stream listpack has no entry length")
		}
		pos++ // the number of the pieces of the entry
		entries = append(entries, store.StreamEntry{
			ID:     store.StreamID{MS: master.MS + uint64(msDelta), Seq: master.Seq + uint64(seqDelta)},
			Fields: fields,
		})
	}
	return entries, nil
}
