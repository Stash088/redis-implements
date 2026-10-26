package store

import (
	"errors"
	"sort"
	"time"
)

// ErrGroupNotFound is returned when a consumer group does not exist.
var ErrGroupNotFound = errors.New("store: no such consumer group")

// StreamGroup is a consumer group of a stream: the consumers that read it and
// the entries that were delivered but not acknowledged yet.
type StreamGroup struct {
	name        string
	lastID      StreamID
	entriesRead uint64
	// entriesReadValid tells whether the counter of the entries read by the
	// group is known: a group created without the ENTRIESREAD option does
	// not have it until it reads something.
	entriesReadValid bool
	consumers        map[string]*StreamConsumer
	pel              map[StreamID]*StreamNack
}

// Name returns the name of the group.
func (cg *StreamGroup) Name() string { return cg.name }

// LastID returns the identifier of the last entry delivered to the group.
func (cg *StreamGroup) LastID() StreamID { return cg.lastID }

// EntriesRead returns the number of entries read by the group.
func (cg *StreamGroup) EntriesRead() uint64 { return cg.entriesRead }

// StreamConsumer is a consumer of a consumer group.
type StreamConsumer struct {
	name     string
	seenTime time.Time
	// activeTime is the moment of the last activity of the consumer; the
	// zero value means that it never did anything.
	activeTime time.Time
	pel        map[StreamID]*StreamNack
}

// Name returns the name of the consumer.
func (c *StreamConsumer) Name() string { return c.name }

// SeenTime returns the time of the last activity of the consumer.
func (c *StreamConsumer) SeenTime() time.Time { return c.seenTime }

// StreamNack is an entry of the pending list of a consumer group: it was
// delivered to a consumer but not acknowledged yet.
type StreamNack struct {
	ID            StreamID
	Consumer      *StreamConsumer
	DeliveryTime  time.Time
	DeliveryCount int64
}

// StreamReadMode tells which entries a group read delivers.
type StreamReadMode uint8

const (
	// StreamReadNew delivers the entries of the stream that the group has not
	// delivered yet (the XREADGROUP ">" form).
	StreamReadNew StreamReadMode = iota
	// StreamReadHistory delivers the pending entries of the consumer with an
	// identifier greater than the given one (the XREADGROUP "<id>" form).
	StreamReadHistory
)

// StreamPendingEntry describes an entry of the pending list of a group.
type StreamPendingEntry struct {
	ID            StreamID
	Consumer      string
	DeliveryTime  time.Time
	DeliveryCount int64
}

// StreamDelivery is the result of a group read.
type StreamDelivery struct {
	// Entries holds the entries delivered to the consumer.
	Entries []StreamEntry
	// Pending holds the entries that were added to the pending list of the
	// group: they have to be propagated to the AOF.
	Pending []StreamPendingEntry
	// LastID is the identifier of the last delivered entry.
	LastID StreamID
	// LastIDChanged reports whether the identifier of the group moved.
	LastIDChanged bool
	// ConsumerCreated reports whether the consumer had to be created.
	ConsumerCreated bool
	// EntriesRead is the number of entries read by the group.
	EntriesRead uint64
}

// StreamClaimOptions holds the options of a claim.
type StreamClaimOptions struct {
	// Idle sets the idle time of the claimed entries.
	Idle time.Duration
	// HasIdle tells whether the idle time was given.
	HasIdle bool
	// Time sets the delivery time of the claimed entries.
	Time time.Time
	// HasTime tells whether the delivery time was given.
	HasTime bool
	// RetryCount sets the number of deliveries of the claimed entries.
	RetryCount int64
	// HasRetryCount tells whether the number of deliveries was given.
	HasRetryCount bool
	// Force claims the entries that are not in the pending list yet.
	Force bool
	// JustID asks for the identifiers only.
	JustID bool
	// LastID moves the identifier of the last delivered entry of the group.
	LastID *StreamID
}

// StreamClaimResult is the result of a claim.
type StreamClaimResult struct {
	// Entries holds the claimed entries.
	Entries []StreamEntry
	// IDs holds the identifiers of the claimed entries.
	IDs []StreamID
	// Pending holds the entries whose pending state changed: they have to be
	// propagated to the AOF.
	Pending []StreamPendingEntry
	// Deleted holds the entries that were removed from the pending list
	// because they no longer exist in the stream.
	Deleted []StreamPendingEntry
	// LastIDChanged reports whether the identifier of the group moved.
	LastIDChanged bool
	// GroupLastID is the identifier of the last entry delivered to the group.
	GroupLastID StreamID
	// EntriesRead is the number of entries read by the group.
	EntriesRead uint64
}

// StreamAutoClaimResult is the result of an automatic claim.
type StreamAutoClaimResult struct {
	// Cursor is the identifier to start the next scan from.
	Cursor StreamID
	// Entries holds the claimed entries.
	Entries []StreamEntry
	// IDs holds the identifiers of the claimed entries.
	IDs []StreamID
	// Deleted holds the entries that were removed from the pending list
	// because they no longer exist in the stream.
	Deleted []StreamPendingEntry
	// Pending holds the entries whose pending state changed: they have to be
	// propagated to the AOF.
	Pending []StreamPendingEntry
	// ConsumerCreated reports whether the consumer had to be created.
	ConsumerCreated bool
}

// StreamPendingInfo describes the pending list of a group.
type StreamPendingInfo struct {
	// Count is the number of pending entries.
	Count int
	// Min is the smallest pending identifier.
	Min StreamID
	// Max is the largest pending identifier.
	Max StreamID
	// Consumers holds the number of pending entries of every consumer.
	Consumers []StreamConsumerPending
}

// StreamConsumerPending is the number of pending entries of a consumer.
type StreamConsumerPending struct {
	Name  string
	Count int
}

// StreamGroupInfo describes a consumer group (the XINFO GROUPS reply).
type StreamGroupInfo struct {
	Name        string
	Consumers   int
	Pending     int
	LastID      StreamID
	EntriesRead uint64
	// EntriesReadValid tells whether the counter of the entries read by the
	// group is known.
	EntriesReadValid bool
	Lag              int64
	// LagValid tells whether the lag of the group is known.
	LagValid bool
}

// StreamConsumerInfo describes a consumer (the XINFO CONSUMERS reply).
type StreamConsumerInfo struct {
	Name    string
	Pending int
	Idle    time.Duration
	// Inactive is the time since the consumer did something. It is negative
	// when the consumer never did anything, like in Redis.
	Inactive time.Duration
	// SeenTime is the moment of the last activity of the consumer.
	SeenTime time.Time
	// ActiveTime is the moment of the last activity of the consumer; it is
	// the zero value when the consumer never did anything.
	ActiveTime time.Time
}

// StreamInfo describes a stream (the XINFO STREAM reply).
type StreamInfo struct {
	Length          int
	LastID          StreamID
	MaxDeletedID    StreamID
	EntriesAdded    uint64
	RecordedFirstID StreamID
	Groups          int
	FirstEntry      *StreamEntry
	LastEntry       *StreamEntry
}

// groups returns the map of the consumer groups of the stream, creating it
// when it is missing.
func (st *Stream) groupsMap() map[string]*StreamGroup {
	if st.groups == nil {
		st.groups = make(map[string]*StreamGroup)
	}
	return st.groups
}

// sortedPELIDs returns the identifiers of a pending list in ascending order.
func sortedPELIDs(pel map[StreamID]*StreamNack) []StreamID {
	ids := make([]StreamID, 0, len(pel))
	for id := range pel {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].Compare(ids[j]) < 0 })
	return ids
}

// entryByID returns the entry of the stream with the given identifier.
func (st *Stream) entryByID(id StreamID) (StreamEntry, bool) {
	i := st.search(id)
	if i >= len(st.entries) || st.entries[i].ID.Compare(id) != 0 {
		return StreamEntry{}, false
	}
	return st.entries[i], true
}

// addPending adds an entry to the pending list of the group, assigning it to
// the given consumer. An entry that is already pending is moved to the new
// consumer, like Redis does.
func (cg *StreamGroup) addPending(cons *StreamConsumer, id StreamID, now time.Time) *StreamNack {
	if nack, ok := cg.pel[id]; ok {
		if nack.Consumer != nil && nack.Consumer != cons {
			delete(nack.Consumer.pel, id)
		}
		nack.Consumer = cons
		nack.DeliveryTime = now
		nack.DeliveryCount = 1
		cons.pel[id] = nack
		return nack
	}
	nack := &StreamNack{ID: id, Consumer: cons, DeliveryTime: now, DeliveryCount: 1}
	cg.pel[id] = nack
	cons.pel[id] = nack
	return nack
}

// removePending removes an entry from the pending list of the group and from
// the pending list of its consumer.
func (cg *StreamGroup) removePending(id StreamID) bool {
	nack, ok := cg.pel[id]
	if !ok {
		return false
	}
	delete(cg.pel, id)
	if nack.Consumer != nil {
		delete(nack.Consumer.pel, id)
	}
	return true
}

// StreamGroupCreate creates a consumer group on the stream under key. The
// identifier is the last entry delivered to the group. It reports false when
// the group already exists.
//
// The stream is created when it is missing and mkStream is set.
func (s *Store) StreamGroupCreate(key []byte, group string, id StreamID, entriesRead uint64, setEntriesRead bool, mkStream bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		if !mkStream {
			return false, ErrStreamNotFound
		}
		e = &Entry{Type: TypeStream, Stream: &Stream{}, cost: len(string(key))}
		s.usedBytes += len(string(key))
		s.data[string(key)] = e
	} else if e.Type != TypeStream {
		return false, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	st := e.Stream
	if _, exists := st.groupsMap()[group]; exists {
		return false, nil
	}
	cg := &StreamGroup{
		name:      group,
		lastID:    id,
		consumers: make(map[string]*StreamConsumer),
		pel:       make(map[StreamID]*StreamNack),
	}
	cg.entriesReadValid = setEntriesRead
	if setEntriesRead {
		cg.entriesRead = entriesRead
		if cg.entriesRead > st.entriesAdded {
			cg.entriesRead = st.entriesAdded
		}
	}
	st.groupsMap()[group] = cg
	return true, nil
}

// StreamGroupDestroy removes the consumer group of the stream under key. It
// reports whether the group existed.
func (s *Store) StreamGroupDestroy(key []byte, group string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false, ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return false, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	st := e.Stream
	if _, exists := st.groups[group]; !exists {
		return false, nil
	}
	delete(st.groups, group)
	return true, nil
}

// StreamGroupSetID sets the identifier of the last entry delivered to the
// consumer group of the stream under key.
func (s *Store) StreamGroupSetID(key []byte, group string, id StreamID, entriesRead uint64, setEntriesRead bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	cg := e.Stream.groups[group]
	if cg == nil {
		return ErrGroupNotFound
	}
	cg.lastID = id
	cg.entriesReadValid = setEntriesRead
	if setEntriesRead {
		cg.entriesRead = entriesRead
		if cg.entriesRead > e.Stream.entriesAdded {
			cg.entriesRead = e.Stream.entriesAdded
		}
	}
	return nil
}

// StreamGroupCreateConsumer creates a consumer in the consumer group of the
// stream under key. It reports false when the consumer already exists.
func (s *Store) StreamGroupCreateConsumer(key []byte, group, consumer string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return false, ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return false, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	cg := e.Stream.groups[group]
	if cg == nil {
		return false, ErrGroupNotFound
	}
	if _, exists := cg.consumers[consumer]; exists {
		return false, nil
	}
	cg.consumers[consumer] = &StreamConsumer{
		name:     consumer,
		seenTime: s.clock(),
		pel:      make(map[StreamID]*StreamNack),
	}
	return true, nil
}

// StreamGroupDelConsumer removes a consumer of the consumer group of the
// stream under key and returns the number of its pending entries.
func (s *Store) StreamGroupDelConsumer(key []byte, group, consumer string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	cg := e.Stream.groups[group]
	if cg == nil {
		return 0, ErrGroupNotFound
	}
	cons, exists := cg.consumers[consumer]
	if !exists {
		return 0, nil
	}
	pending := len(cons.pel)
	for id := range cons.pel {
		delete(cg.pel, id)
	}
	delete(cg.consumers, consumer)
	return pending, nil
}

// StreamGroupRead delivers the entries of the stream to a consumer of a group.
//
// In the StreamReadNew mode the entries that the group has not delivered yet
// are returned and added to the pending list of the group and of the consumer.
// In the StreamReadHistory mode the pending entries of the consumer with an
// identifier greater than the given one are returned.
func (s *Store) StreamGroupRead(key []byte, group, consumer string, mode StreamReadMode, id StreamID, count int, noack bool) (StreamDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return StreamDelivery{}, ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return StreamDelivery{}, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	st := e.Stream
	cg := st.groups[group]
	if cg == nil {
		return StreamDelivery{}, ErrGroupNotFound
	}
	now := s.clock()
	var out StreamDelivery
	cons, exists := cg.consumers[consumer]
	if !exists {
		cons = &StreamConsumer{name: consumer, seenTime: now, pel: make(map[StreamID]*StreamNack)}
		cg.consumers[consumer] = cons
		out.ConsumerCreated = true
	}
	cons.seenTime = now

	switch mode {
	case StreamReadHistory:
		for _, pid := range sortedPELIDs(cons.pel) {
			if pid.Compare(id) <= 0 {
				continue
			}
			entry, exists := st.entryByID(pid)
			if !exists {
				continue
			}
			out.Entries = append(out.Entries, entry)
			// The delivery of a pending entry counts as a new delivery, like
			// in Redis.
			if nack, ok := cg.pel[pid]; ok {
				nack.DeliveryTime = now
				nack.DeliveryCount++
				out.Pending = append(out.Pending, StreamPendingEntry{
					ID:            pid,
					Consumer:      cons.name,
					DeliveryTime:  nack.DeliveryTime,
					DeliveryCount: nack.DeliveryCount,
				})
			}
			if count > 0 && len(out.Entries) >= count {
				break
			}
		}
	default:
		start := cg.lastID
		for i := st.search(start); i < len(st.entries); i++ {
			entry := st.entries[i]
			if entry.ID.Compare(start) <= 0 {
				continue
			}
			out.Entries = append(out.Entries, entry)
			if count > 0 && len(out.Entries) >= count {
				break
			}
		}
		for _, entry := range out.Entries {
			cons.activeTime = now
			if !noack {
				nack := cg.addPending(cons, entry.ID, now)
				out.Pending = append(out.Pending, StreamPendingEntry{
					ID:            entry.ID,
					Consumer:      cons.name,
					DeliveryTime:  nack.DeliveryTime,
					DeliveryCount: nack.DeliveryCount,
				})
			}
			cg.lastID = entry.ID
			if cg.entriesReadValid {
				cg.entriesRead++
			} else {
				// The counter was unknown: it becomes the number of the
				// entries of the stream up to this one.
				cg.entriesRead = uint64(st.search(entry.ID) + 1)
				cg.entriesReadValid = true
			}
			out.LastID = entry.ID
			out.LastIDChanged = true
		}
	}
	out.EntriesRead = cg.entriesRead
	return out, nil
}

// StreamRead returns the entries of the stream under key with an identifier
// greater than the given one.
func (s *Store) StreamRead(key []byte, id StreamID, count int) ([]StreamEntry, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]StreamEntry, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		st := e.Stream
		out := make([]StreamEntry, 0, len(st.entries))
		for i := st.search(id); i < len(st.entries); i++ {
			entry := st.entries[i]
			if entry.ID.Compare(id) <= 0 {
				continue
			}
			out = append(out, entry)
			if count > 0 && len(out) >= count {
				break
			}
		}
		return out, nil
	})
}

// StreamAck acknowledges the entries of the consumer group of the stream under
// key and returns the number of acknowledged entries. A missing key or group
// acknowledges nothing.
func (s *Store) StreamAck(key []byte, group string, ids []StreamID) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeStream {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	cg := e.Stream.groups[group]
	if cg == nil {
		return 0, nil
	}
	acked := 0
	for _, id := range ids {
		if cg.removePending(id) {
			acked++
		}
	}
	return acked, nil
}

// StreamClaim claims the pending entries of the consumer group of the stream
// under key for the given consumer.
func (s *Store) StreamClaim(key []byte, group, consumer string, minIdle time.Duration, ids []StreamID, opts StreamClaimOptions) (StreamClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return StreamClaimResult{}, ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return StreamClaimResult{}, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	st := e.Stream
	cg := st.groups[group]
	if cg == nil {
		return StreamClaimResult{}, ErrGroupNotFound
	}
	now := s.clock()

	var out StreamClaimResult
	if opts.LastID != nil && opts.LastID.Compare(cg.lastID) > 0 {
		cg.lastID = *opts.LastID
		out.LastIDChanged = true
	}
	out.GroupLastID = cg.lastID
	out.EntriesRead = cg.entriesRead
	deliveryTime := now
	switch {
	case opts.HasTime:
		deliveryTime = opts.Time
	case opts.HasIdle:
		deliveryTime = now.Add(-opts.Idle)
	}
	if deliveryTime.After(now) {
		deliveryTime = now
	}

	cons, exists := cg.consumers[consumer]
	if !exists {
		cons = &StreamConsumer{name: consumer, seenTime: now, pel: make(map[StreamID]*StreamNack)}
		cg.consumers[consumer] = cons
	}
	cons.seenTime = now
	cons.activeTime = now

	for _, id := range ids {
		nack, inPEL := cg.pel[id]
		entry, inStream := st.entryByID(id)
		if !inStream {
			// The entry is gone: drop it from the pending list.
			if inPEL {
				owner := ""
				if nack.Consumer != nil {
					owner = nack.Consumer.name
				}
				cg.removePending(id)
				out.Deleted = append(out.Deleted, StreamPendingEntry{ID: id, Consumer: owner})
			}
			continue
		}
		if !inPEL {
			if !opts.Force {
				continue
			}
			nack = &StreamNack{ID: id, DeliveryTime: deliveryTime}
			cg.pel[id] = nack
		}
		if nack.Consumer != nil && minIdle > 0 {
			if now.Sub(nack.DeliveryTime) < minIdle {
				continue
			}
		}
		if nack.Consumer != nil && nack.Consumer != cons {
			delete(nack.Consumer.pel, id)
		}
		nack.DeliveryTime = deliveryTime
		switch {
		case opts.HasRetryCount:
			nack.DeliveryCount = opts.RetryCount
		case !opts.JustID:
			nack.DeliveryCount++
		}
		if nack.Consumer != cons {
			cons.pel[id] = nack
			nack.Consumer = cons
		}
		out.Entries = append(out.Entries, entry)
		out.IDs = append(out.IDs, id)
		out.Pending = append(out.Pending, StreamPendingEntry{
			ID:            id,
			Consumer:      cons.name,
			DeliveryTime:  nack.DeliveryTime,
			DeliveryCount: nack.DeliveryCount,
		})
	}
	return out, nil
}

// StreamAutoClaim claims the pending entries of the consumer group of the
// stream under key that have been idle for at least minIdle, starting from
// the given identifier.
func (s *Store) StreamAutoClaim(key []byte, group, consumer string, minIdle time.Duration, start StreamID, count int, justID bool) (StreamAutoClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return StreamAutoClaimResult{}, ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return StreamAutoClaimResult{}, ErrWrongType
	}
	s.touchKeyLocked(key)
	s.markAccess(e)

	st := e.Stream
	cg := st.groups[group]
	if cg == nil {
		return StreamAutoClaimResult{}, ErrGroupNotFound
	}
	now := s.clock()

	var out StreamAutoClaimResult
	cons, exists := cg.consumers[consumer]
	if !exists {
		cons = &StreamConsumer{name: consumer, seenTime: now, pel: make(map[StreamID]*StreamNack)}
		cg.consumers[consumer] = cons
		out.ConsumerCreated = true
	}
	cons.seenTime = now
	cons.activeTime = now

	ids := sortedPELIDs(cg.pel)
	remaining := count
	for _, id := range ids {
		if id.Compare(start) < 0 {
			continue
		}
		if remaining > 0 && len(out.Entries)+len(out.Deleted) >= remaining {
			break
		}
		nack := cg.pel[id]
		entry, inStream := st.entryByID(id)
		if !inStream {
			// The entry is gone: drop it from the pending list.
			owner := ""
			if nack.Consumer != nil {
				owner = nack.Consumer.name
			}
			cg.removePending(id)
			out.Deleted = append(out.Deleted, StreamPendingEntry{ID: id, Consumer: owner})
			continue
		}
		if minIdle > 0 && now.Sub(nack.DeliveryTime) < minIdle {
			continue
		}
		if nack.Consumer != nil && nack.Consumer != cons {
			delete(nack.Consumer.pel, id)
		}
		nack.DeliveryTime = now
		if !justID {
			nack.DeliveryCount++
		}
		if nack.Consumer != cons {
			cons.pel[id] = nack
			nack.Consumer = cons
		}
		out.Entries = append(out.Entries, entry)
		out.IDs = append(out.IDs, id)
		out.Pending = append(out.Pending, StreamPendingEntry{
			ID:            id,
			Consumer:      cons.name,
			DeliveryTime:  nack.DeliveryTime,
			DeliveryCount: nack.DeliveryCount,
		})
	}
	// The cursor points at the next pending entry to scan.
	out.Cursor = StreamID{}
	for _, id := range ids {
		if id.Compare(start) >= 0 {
			if _, claimed := cg.pel[id]; !claimed {
				out.Cursor = id
				break
			}
		}
	}
	return out, nil
}

// StreamPendingInfo describes the pending list of the consumer group of the
// stream under key.
func (s *Store) StreamPendingInfo(key []byte, group string) (StreamPendingInfo, bool, error) {
	return readEntry(s, key, func(e *Entry) (StreamPendingInfo, error) {
		if e.Type != TypeStream {
			return StreamPendingInfo{}, ErrWrongType
		}
		cg := e.Stream.groups[group]
		if cg == nil {
			return StreamPendingInfo{}, ErrGroupNotFound
		}
		info := StreamPendingInfo{Count: len(cg.pel)}
		ids := sortedPELIDs(cg.pel)
		if len(ids) > 0 {
			info.Min = ids[0]
			info.Max = ids[len(ids)-1]
		}
		names := make([]string, 0, len(cg.consumers))
		for name := range cg.consumers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			n := len(cg.consumers[name].pel)
			if n == 0 {
				continue
			}
			info.Consumers = append(info.Consumers, StreamConsumerPending{Name: name, Count: n})
		}
		return info, nil
	})
}

// StreamPendingRange returns the pending entries of the consumer group of the
// stream under key in the inclusive range [start, end].
func (s *Store) StreamPendingRange(key []byte, group string, start, end StreamID, count int, consumer string, minIdle time.Duration) ([]StreamPendingEntry, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]StreamPendingEntry, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		cg := e.Stream.groups[group]
		if cg == nil {
			return nil, ErrGroupNotFound
		}
		now := s.clock()
		out := make([]StreamPendingEntry, 0, len(cg.pel))
		for _, id := range sortedPELIDs(cg.pel) {
			if id.Compare(start) < 0 {
				continue
			}
			if id.Compare(end) > 0 {
				break
			}
			nack := cg.pel[id]
			if consumer != "" && (nack.Consumer == nil || nack.Consumer.name != consumer) {
				continue
			}
			if minIdle > 0 && now.Sub(nack.DeliveryTime) < minIdle {
				continue
			}
			name := ""
			if nack.Consumer != nil {
				name = nack.Consumer.name
			}
			out = append(out, StreamPendingEntry{
				ID:            id,
				Consumer:      name,
				DeliveryTime:  nack.DeliveryTime,
				DeliveryCount: nack.DeliveryCount,
			})
			if count > 0 && len(out) >= count {
				break
			}
		}
		return out, nil
	})
}

// StreamConsumerPending returns the pending entries of a consumer of a
// consumer group of the stream under key.
func (s *Store) StreamConsumerPending(key []byte, group, consumer string, count int) ([]StreamPendingEntry, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]StreamPendingEntry, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		cg := e.Stream.groups[group]
		if cg == nil {
			return nil, ErrGroupNotFound
		}
		cons, exists := cg.consumers[consumer]
		if !exists {
			return nil, nil
		}
		out := make([]StreamPendingEntry, 0, len(cons.pel))
		for _, id := range sortedPELIDs(cons.pel) {
			nack := cons.pel[id]
			out = append(out, StreamPendingEntry{
				ID:            id,
				Consumer:      cons.name,
				DeliveryTime:  nack.DeliveryTime,
				DeliveryCount: nack.DeliveryCount,
			})
			if count > 0 && len(out) >= count {
				break
			}
		}
		return out, nil
	})
}

// StreamGroupState describes a consumer group with the entries of its pending
// list. It is used to rebuild the groups of a stream.
type StreamGroupState struct {
	Info    StreamGroupInfo
	Pending []StreamPendingEntry
}

// StreamGroups returns the consumer groups of the stream under key with the
// entries of their pending lists.
func (s *Store) StreamGroups(key []byte) ([]StreamGroupState, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]StreamGroupState, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		st := e.Stream
		names := make([]string, 0, len(st.groups))
		for name := range st.groups {
			names = append(names, name)
		}
		sort.Strings(names)
		out := make([]StreamGroupState, 0, len(names))
		for _, name := range names {
			cg := st.groups[name]
			state := StreamGroupState{
				Info: StreamGroupInfo{
					Name:             name,
					Consumers:        len(cg.consumers),
					Pending:          len(cg.pel),
					LastID:           cg.lastID,
					EntriesRead:      cg.entriesRead,
					EntriesReadValid: cg.entriesReadValid,
				},
			}
			switch {
			case st.entriesAdded == 0 || st.Len() == 0:
				state.Info.Lag = 0
				state.Info.LagValid = true
			case cg.lastID.Compare(st.firstID()) < 0 && st.maxDeletedID.Compare(st.firstID()) < 0:
				state.Info.Lag = int64(st.Len())
				state.Info.LagValid = true
			case cg.entriesReadValid:
				if cg.entriesRead < st.entriesAdded {
					state.Info.Lag = int64(st.entriesAdded - cg.entriesRead)
				}
				state.Info.LagValid = true
			}
			for _, id := range sortedPELIDs(cg.pel) {
				nack := cg.pel[id]
				owner := ""
				if nack.Consumer != nil {
					owner = nack.Consumer.name
				}
				state.Pending = append(state.Pending, StreamPendingEntry{
					ID:            id,
					Consumer:      owner,
					DeliveryTime:  nack.DeliveryTime,
					DeliveryCount: nack.DeliveryCount,
				})
			}
			out = append(out, state)
		}
		return out, nil
	})
}

// StreamInfo describes the stream under key.
func (s *Store) StreamInfo(key []byte) (StreamInfo, bool, error) {
	return readEntry(s, key, func(e *Entry) (StreamInfo, error) {
		if e.Type != TypeStream {
			return StreamInfo{}, ErrWrongType
		}
		st := e.Stream
		info := StreamInfo{
			Length:          st.Len(),
			LastID:          st.lastID,
			MaxDeletedID:    st.maxDeletedID,
			EntriesAdded:    st.entriesAdded,
			RecordedFirstID: st.lastID,
			Groups:          len(st.groups),
		}
		if len(st.entries) > 0 {
			first := st.entries[0]
			last := st.entries[len(st.entries)-1]
			info.RecordedFirstID = first.ID
			info.FirstEntry = &first
			info.LastEntry = &last
		}
		return info, nil
	})
}

// StreamGroupsInfo describes the consumer groups of the stream under key.
func (s *Store) StreamGroupsInfo(key []byte) ([]StreamGroupInfo, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]StreamGroupInfo, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		st := e.Stream
		names := make([]string, 0, len(st.groups))
		for name := range st.groups {
			names = append(names, name)
		}
		sort.Strings(names)
		out := make([]StreamGroupInfo, 0, len(names))
		for _, name := range names {
			cg := st.groups[name]
			info := StreamGroupInfo{
				Name:             name,
				Consumers:        len(cg.consumers),
				Pending:          len(cg.pel),
				LastID:           cg.lastID,
				EntriesRead:      cg.entriesRead,
				EntriesReadValid: cg.entriesReadValid,
			}
			switch {
			case st.entriesAdded == 0 || st.Len() == 0:
				info.Lag = 0
				info.LagValid = true
			case cg.lastID.Compare(st.firstID()) < 0 && st.maxDeletedID.Compare(st.firstID()) < 0:
				// The group is behind the whole stream: the lag is the
				// number of the entries of the stream.
				info.Lag = int64(st.Len())
				info.LagValid = true
			case cg.entriesReadValid:
				if cg.entriesRead < st.entriesAdded {
					info.Lag = int64(st.entriesAdded - cg.entriesRead)
				}
				info.LagValid = true
			}
			out = append(out, info)
		}
		return out, nil
	})
}

// StreamConsumersInfo describes the consumers of a consumer group of the
// stream under key.
func (s *Store) StreamConsumersInfo(key []byte, group string) ([]StreamConsumerInfo, bool, error) {
	return readEntry(s, key, func(e *Entry) ([]StreamConsumerInfo, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		cg := e.Stream.groups[group]
		if cg == nil {
			return nil, ErrGroupNotFound
		}
		now := s.clock()
		names := make([]string, 0, len(cg.consumers))
		for name := range cg.consumers {
			names = append(names, name)
		}
		sort.Strings(names)
		out := make([]StreamConsumerInfo, 0, len(names))
		for _, name := range names {
			cons := cg.consumers[name]
			out = append(out, StreamConsumerInfo{
				Name:       name,
				Pending:    len(cons.pel),
				Idle:       now.Sub(cons.seenTime),
				SeenTime:   cons.seenTime,
				ActiveTime: cons.activeTime,
			})
			if cons.activeTime.IsZero() {
				out[len(out)-1].Inactive = -1
			} else {
				out[len(out)-1].Inactive = now.Sub(cons.activeTime)
			}
		}
		return out, nil
	})
}
