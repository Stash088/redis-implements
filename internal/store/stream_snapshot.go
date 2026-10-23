package store

import (
	"sort"
	"time"
)

// The persistence layer (the RDB format of the streams) needs a copy of a
// stream and a way to build one from such a copy. The fields of the stream
// and of its groups are private, so the copy is made here.

// StreamSnapshot is a copy of a stream: the entries, the metadata Redis keeps
// about the stream and its consumer groups.
//
// The copy is taken under the store lock, so the persistence layer can
// serialize it without racing with the commands that change the stream.
type StreamSnapshot struct {
	Entries      []StreamEntry
	LastID       StreamID
	MaxDeletedID StreamID
	EntriesAdded uint64
	Groups       []StreamGroupSnapshot
}

// StreamGroupSnapshot is a copy of a consumer group of a stream.
type StreamGroupSnapshot struct {
	Name             string
	LastID           StreamID
	EntriesRead      uint64
	EntriesReadValid bool
	// Pending holds the entries that were delivered but not acknowledged.
	Pending []StreamPendingSnapshot
	// Consumers holds the consumers of the group.
	Consumers []StreamConsumerSnapshot
}

// StreamConsumerSnapshot is a copy of a consumer of a group.
type StreamConsumerSnapshot struct {
	Name       string
	SeenTime   time.Time
	ActiveTime time.Time
	// Pending holds the identifiers of the entries the consumer received and
	// did not acknowledge.
	Pending []StreamID
}

// StreamPendingSnapshot is a copy of an entry of the pending list of a group.
type StreamPendingSnapshot struct {
	ID            StreamID
	Consumer      string
	DeliveryTime  time.Time
	DeliveryCount int64
}

// sortedGroupNames returns the names of the groups in a stable order.
func sortedGroupNames(groups map[string]*StreamGroup) []string {
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// sortedConsumerNames returns the names of the consumers in a stable order.
func sortedConsumerNames(consumers map[string]*StreamConsumer) []string {
	names := make([]string, 0, len(consumers))
	for name := range consumers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Snapshot returns a copy of the stream and its consumer groups.
func (st *Stream) Snapshot() *StreamSnapshot { return snapshotStream(st) }

// StreamSnapshot returns a copy of the stream stored under key. ok is false
// when the key is missing or holds another type.
func (s *Store) StreamSnapshot(key []byte) (*StreamSnapshot, bool, error) {
	var snap *StreamSnapshot
	_, ok, err := readEntry(s, key, func(e *Entry) (bool, error) {
		if e.Type != TypeStream {
			return false, nil
		}
		snap = snapshotStream(e.Stream)
		return true, nil
	})
	if err != nil || !ok || snap == nil {
		return nil, false, err
	}
	return snap, true, nil
}

// snapshotStream copies a stream and its groups.
func snapshotStream(st *Stream) *StreamSnapshot {
	snap := &StreamSnapshot{
		LastID:       st.lastID,
		MaxDeletedID: st.maxDeletedID,
		EntriesAdded: st.entriesAdded,
	}
	snap.Entries = make([]StreamEntry, len(st.entries))
	for i, e := range st.entries {
		// The fields of an entry are never modified in place, so the slice
		// of the field names and values can be shared.
		snap.Entries[i] = StreamEntry{ID: e.ID, Fields: e.Fields}
	}
	for _, name := range sortedGroupNames(st.groups) {
		cg := st.groups[name]
		gs := StreamGroupSnapshot{
			Name:             cg.name,
			LastID:           cg.lastID,
			EntriesRead:      cg.entriesRead,
			EntriesReadValid: cg.entriesReadValid,
		}
		for _, id := range sortedPELIDs(cg.pel) {
			nack := cg.pel[id]
			gs.Pending = append(gs.Pending, StreamPendingSnapshot{
				ID:            nack.ID,
				Consumer:      nack.Consumer.Name(),
				DeliveryTime:  nack.DeliveryTime,
				DeliveryCount: nack.DeliveryCount,
			})
		}
		for _, cname := range sortedConsumerNames(cg.consumers) {
			c := cg.consumers[cname]
			cs := StreamConsumerSnapshot{
				Name:       c.name,
				SeenTime:   c.seenTime,
				ActiveTime: c.activeTime,
			}
			for _, id := range sortedPELIDs(c.pel) {
				cs.Pending = append(cs.Pending, id)
			}
			gs.Consumers = append(gs.Consumers, cs)
		}
		snap.Groups = append(snap.Groups, gs)
	}
	return snap
}

// NewStream builds a stream from a snapshot.
func NewStream(snap *StreamSnapshot) *Stream {
	if snap == nil {
		return nil
	}
	st := &Stream{
		lastID:       snap.LastID,
		maxDeletedID: snap.MaxDeletedID,
		entriesAdded: snap.EntriesAdded,
		groups:       make(map[string]*StreamGroup),
	}
	st.entries = make([]StreamEntry, len(snap.Entries))
	copy(st.entries, snap.Entries)

	for _, gs := range snap.Groups {
		cg := &StreamGroup{
			name:             gs.Name,
			lastID:           gs.LastID,
			entriesRead:      gs.EntriesRead,
			entriesReadValid: gs.EntriesReadValid,
			consumers:        make(map[string]*StreamConsumer),
			pel:              make(map[StreamID]*StreamNack),
		}
		consumers := make(map[string]*StreamConsumer, len(gs.Consumers))
		for _, cs := range gs.Consumers {
			consumers[cs.Name] = &StreamConsumer{
				name:       cs.Name,
				seenTime:   cs.SeenTime,
				activeTime: cs.ActiveTime,
				pel:        make(map[StreamID]*StreamNack),
			}
		}
		for _, ps := range gs.Pending {
			consumer, ok := consumers[ps.Consumer]
			if !ok {
				consumer = &StreamConsumer{
					name: ps.Consumer,
					pel:  make(map[StreamID]*StreamNack),
				}
				consumers[ps.Consumer] = consumer
			}
			nack := &StreamNack{
				ID:            ps.ID,
				Consumer:      consumer,
				DeliveryTime:  ps.DeliveryTime,
				DeliveryCount: ps.DeliveryCount,
			}
			cg.pel[ps.ID] = nack
			consumer.pel[ps.ID] = nack
		}
		cg.consumers = consumers
		st.groups[cg.name] = cg
	}
	return st
}

// RestoreStream stores a stream built from a snapshot under key, replacing
// whatever was there.
func (s *Store) RestoreStream(key []byte, snap *StreamSnapshot) error {
	if snap == nil {
		return nil
	}
	st := NewStream(snap)
	if st == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cost := len(string(key))
	s.touchKeyLocked(key)
	if old, ok := s.data[string(key)]; ok {
		s.usedBytes -= old.cost
	}
	s.usedBytes += cost + streamCost(st)
	s.data[string(key)] = &Entry{Type: TypeStream, Stream: st, cost: cost}
	return nil
}

// streamCost returns the approximate memory footprint of a stream.
func streamCost(st *Stream) int {
	cost := 0
	for _, e := range st.entries {
		cost += entryCost(e)
	}
	return cost
}
