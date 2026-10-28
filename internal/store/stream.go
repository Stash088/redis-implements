package store

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidStreamID is returned when a stream identifier cannot be parsed.
var ErrInvalidStreamID = errors.New("store: invalid stream ID")

// ErrStreamIDNotGreater is returned when the identifier of a new entry is not
// greater than the identifier of the last entry of the stream.
var ErrStreamIDNotGreater = errors.New("store: stream ID is not greater than the last entry")

// ErrStreamNotCreated is returned by StreamAdd when the stream is missing and
// the caller asked not to create it (the NOMKSTREAM option of XADD).
var ErrStreamNotCreated = errors.New("store: stream not created")

// ErrStreamNotFound is returned when a stream is expected to exist but the key
// holds no stream.
var ErrStreamNotFound = errors.New("store: no such key")

// ErrStreamSyntax is returned when the arguments of a stream command do not
// follow its syntax.
var ErrStreamSyntax = errors.New("store: syntax error")

// ErrStreamNotInteger is returned when a numeric argument of a stream command
// is not a number.
var ErrStreamNotInteger = errors.New("store: value is not an integer or out of range")

// ErrStreamMaxLenNegative is returned when the MAXLEN of a stream command is
// negative.
var ErrStreamMaxLenNegative = errors.New("store: the MAXLEN argument must be >= 0")

// ErrStreamLimitNegative is returned when the LIMIT of a stream command is
// negative.
var ErrStreamLimitNegative = errors.New("store: the LIMIT argument must be >= 0")

// ErrStreamTrimConflict is returned when both MAXLEN and MINID are given.
var ErrStreamTrimConflict = errors.New("store: MAXLEN and MINID options are not compatible")

// ErrStreamLimitNoStrategy is returned when LIMIT is given without a trimming
// strategy.
var ErrStreamLimitNoStrategy = errors.New("store: LIMIT cannot be used without specifying a trimming strategy")

// ErrStreamLimitNoApprox is returned when LIMIT is given without the
// approximate trimming option.
var ErrStreamLimitNoApprox = errors.New("store: LIMIT cannot be used without the special ~ option")

// ErrStreamNoStrategy is returned when XTRIM is called without a trimming
// strategy.
var ErrStreamNoStrategy = errors.New("store: XTRIM must be called with a trimming strategy")

// ErrStreamEntriesAddedTooSmall is returned when the number of entries added
// given to XSETID is smaller than the length of the stream.
var ErrStreamEntriesAddedTooSmall = errors.New("store: the entries_added is smaller than the stream length")

// ErrStreamMaxDeletedTooSmall is returned when the identifier given to XSETID
// is smaller than the highest deleted identifier of the stream.
var ErrStreamMaxDeletedTooSmall = errors.New("store: the ID is smaller than the max deleted entry ID")

// ErrStreamSetIDMaxDeleted is returned when the identifier given to XSETID is
// smaller than the one given as MAXDELETEDID.
var ErrStreamSetIDMaxDeleted = errors.New("store: the ID is smaller than the provided max deleted entry ID")

// StreamMaxID is the identifier of the last possible entry of a stream.
var StreamMaxID = StreamID{MS: math.MaxUint64, Seq: math.MaxUint64}

// streamNodeEntries is the number of entries Redis puts in one node of a
// stream. The approximate trimming removes whole nodes, so we use the same
// granularity to behave like Redis does.
const streamNodeEntries = 100

// StreamID identifies an entry of a stream: the millisecond timestamp of the
// entry and a sequence number, like Redis uses.
type StreamID struct {
	MS  uint64
	Seq uint64
}

// Compare returns -1 when id is smaller than other, 0 when they are equal and
// 1 when id is greater.
func (id StreamID) Compare(other StreamID) int {
	switch {
	case id.MS < other.MS:
		return -1
	case id.MS > other.MS:
		return 1
	case id.Seq < other.Seq:
		return -1
	case id.Seq > other.Seq:
		return 1
	}
	return 0
}

// String returns the "ms-seq" form of the identifier.
func (id StreamID) String() string {
	return strconv.FormatUint(id.MS, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// IsZero reports whether the identifier is the lowest possible one.
func (id StreamID) IsZero() bool { return id.MS == 0 && id.Seq == 0 }

// StreamIDMode tells how the identifier of a new stream entry is chosen.
type StreamIDMode uint8

const (
	// StreamIDAuto generates the identifier from the current time.
	StreamIDAuto StreamIDMode = iota
	// StreamIDAutoSeq keeps the millisecond part and generates the sequence.
	StreamIDAutoSeq
	// StreamIDExplicit uses the identifier given by the caller.
	StreamIDExplicit
)

// StreamEntry is a single record of a stream: its identifier and the
// alternating field names and values.
type StreamEntry struct {
	ID     StreamID
	Fields [][]byte
}

// Stream is the value of a stream key: the entries in identifier order plus
// the metadata Redis keeps about the stream.
type Stream struct {
	entries      []StreamEntry
	lastID       StreamID
	maxDeletedID StreamID
	entriesAdded uint64
	groups       map[string]*StreamGroup
}

// Len returns the number of entries of the stream.
func (st *Stream) Len() int { return len(st.entries) }

// LastID returns the identifier of the last entry of the stream.
func (st *Stream) LastID() StreamID { return st.lastID }

// MaxDeletedID returns the identifier of the highest deleted entry.
func (st *Stream) MaxDeletedID() StreamID { return st.maxDeletedID }

// EntriesAdded returns the number of entries ever added to the stream.
func (st *Stream) EntriesAdded() uint64 { return st.entriesAdded }

// Entries returns the entries of the stream in identifier order.
func (st *Stream) Entries() []StreamEntry { return st.entries }

// search returns the index of the first entry whose identifier is not smaller
// than id.
func (st *Stream) search(id StreamID) int {
	lo, hi := 0, len(st.entries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if st.entries[mid].ID.Compare(id) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// lastIndexLE returns the index of the last entry whose identifier is not
// greater than id, or -1 when there is no such entry.
func (st *Stream) lastIndexLE(id StreamID) int {
	i := st.search(id)
	if i < len(st.entries) && st.entries[i].ID.Compare(id) == 0 {
		return i
	}
	return i - 1
}

// firstID returns the identifier of the first entry of the stream.
func (st *Stream) firstID() StreamID {
	if len(st.entries) == 0 {
		return StreamID{}
	}
	return st.entries[0].ID
}

// topID returns the identifier of the last entry of the stream, like Redis
// does when it checks the identifier given to XSETID.
func (st *Stream) topID() StreamID {
	if len(st.entries) == 0 {
		return StreamID{}
	}
	return st.entries[len(st.entries)-1].ID
}

// entryCost returns the approximate footprint of an entry.
func entryCost(entry StreamEntry) int {
	cost := 0
	for _, f := range entry.Fields {
		cost += len(f) + elemOverhead
	}
	return cost
}

// ParseStreamID parses a stream identifier of the "ms-seq" form. The sequence
// part may be omitted, in which case missingSeq is used. The special
// identifiers "-" and "+" stand for the lowest and the highest identifier.
//
// It reports false when the identifier is malformed.
func ParseStreamID(s string, missingSeq uint64) (StreamID, bool) {
	return parseStreamID(s, missingSeq, false, nil)
}

// ParseStreamIDStrict parses a stream identifier in the strict form: the
// special identifiers "-" and "+" are rejected and the "ms-*" form is
// recognized. When seqGiven is not nil it reports whether the sequence part
// was given explicitly: the "ms-*" form leaves it false.
//
// It reports false when the identifier is malformed.
func ParseStreamIDStrict(s string, missingSeq uint64, seqGiven *bool) (StreamID, bool) {
	return parseStreamID(s, missingSeq, true, seqGiven)
}

// parseStreamID parses a stream identifier. See ParseStreamID and
// ParseStreamIDStrict.
func parseStreamID(s string, missingSeq uint64, strict bool, seqGiven *bool) (StreamID, bool) {
	if seqGiven != nil {
		*seqGiven = true
	}
	if s == "-" {
		if strict {
			return StreamID{}, false
		}
		return StreamID{}, true
	}
	if s == "+" {
		if strict {
			return StreamID{}, false
		}
		return StreamMaxID, true
	}
	ms, seq, hasSeq := strings.Cut(s, "-")
	if !hasSeq {
		v, ok := parseStreamUint(ms)
		if !ok {
			return StreamID{}, false
		}
		return StreamID{MS: v, Seq: missingSeq}, true
	}
	m, ok := parseStreamUint(ms)
	if !ok {
		return StreamID{}, false
	}
	if hasSeq && seq == "*" {
		if seqGiven != nil {
			*seqGiven = false
		}
		return StreamID{MS: m}, true
	}
	q, ok := parseStreamUint(seq)
	if !ok {
		return StreamID{}, false
	}
	return StreamID{MS: m, Seq: q}, true
}

// parseStreamUint parses an unsigned number the way Redis does: digits only,
// without a sign and without leading zeroes.
func parseStreamUint(s string) (uint64, bool) {
	if s == "" || len(s) > 20 {
		return 0, false
	}
	if len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	var v uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + uint64(c-'0')
		if v > math.MaxUint64 {
			return 0, false
		}
	}
	return v, true
}

// StreamTrim describes a trimming request, as the XTRIM command and the
// trimming options of XADD build it.
type StreamTrim struct {
	// MaxLen, when set, keeps at most that many entries.
	MaxLen *uint64
	// MinID, when set, removes the entries with a smaller identifier.
	MinID *StreamID
	// Approximate trims whole chunks of entries only, like Redis does.
	Approximate bool
	// Limit caps the number of removed entries (zero means no limit).
	Limit int
}

// StreamAddArgs holds the parsed arguments of the XADD command.
type StreamAddArgs struct {
	// Mode tells how the identifier of the new entry is chosen.
	Mode StreamIDMode
	// ID is the identifier given by the caller. It is used by the explicit
	// and by the "ms-*" forms.
	ID StreamID
	// Trim is the trimming to apply once the entry is added.
	Trim StreamTrim
	// HasTrim tells whether a trimming strategy was given.
	HasTrim bool
	// NoMkStream asks not to create the stream when the key is missing.
	NoMkStream bool
	// Fields holds the field names and values of the new entry.
	Fields [][]byte
}

// ParseStreamAddArgs parses the arguments of the XADD command that follow the
// key: the options, the identifier and the fields of the new entry.
func ParseStreamAddArgs(args [][]byte) (StreamAddArgs, error) {
	var out StreamAddArgs
	trim, hasTrim, limitGiven, noMkStream, idx, err := parseStreamOptions(args, true)
	if err != nil {
		return out, err
	}
	if limitGiven && trim.Limit > 0 && !hasTrim {
		return out, ErrStreamLimitNoStrategy
	}
	if limitGiven && trim.Limit > 0 && !trim.Approximate {
		return out, ErrStreamLimitNoApprox
	}
	out.Trim = trim
	out.HasTrim = hasTrim
	out.NoMkStream = noMkStream
	if idx >= len(args) {
		return out, ErrStreamSyntax
	}
	// The identifier is either "*" (generated), "ms-*" (the sequence is
	// generated) or an explicit one.
	if string(args[idx]) == "*" {
		out.Mode = StreamIDAuto
	} else {
		seqGiven := true
		id, ok := ParseStreamIDStrict(string(args[idx]), 0, &seqGiven)
		if !ok {
			return out, ErrInvalidStreamID
		}
		out.ID = id
		if seqGiven {
			out.Mode = StreamIDExplicit
		} else {
			out.Mode = StreamIDAutoSeq
		}
	}
	fields := args[idx+1:]
	if len(fields) < 2 || len(fields)%2 == 1 {
		return out, ErrStreamSyntax
	}
	out.Fields = fields
	return out, nil
}

// ParseStreamTrimArgs parses the arguments of the XTRIM command that follow
// the key.
func ParseStreamTrimArgs(args [][]byte) (StreamTrim, error) {
	trim, hasTrim, limitGiven, _, idx, err := parseStreamOptions(args, false)
	if err != nil {
		return trim, err
	}
	// Every argument of XTRIM is an option: an unknown one is a syntax error.
	if idx < len(args) {
		return trim, ErrStreamSyntax
	}
	if limitGiven && trim.Limit > 0 && !hasTrim {
		return trim, ErrStreamLimitNoStrategy
	}
	if limitGiven && trim.Limit > 0 && !trim.Approximate {
		return trim, ErrStreamLimitNoApprox
	}
	if !hasTrim {
		return trim, ErrStreamNoStrategy
	}
	return trim, nil
}

// parseStreamOptions parses the options shared by XADD and XTRIM. It returns
// the trimming options, whether a strategy was given, whether LIMIT was given,
// whether NOMKSTREAM was given and the index of the first argument that is not
// an option.
func parseStreamOptions(args [][]byte, xadd bool) (StreamTrim, bool, bool, bool, int, error) {
	var trim StreamTrim
	hasTrim := false
	limitGiven := false
	noMkStream := false
	i := 0
	for ; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		more := len(args) - i - 1
		switch opt {
		case "MAXLEN", "MINID":
			if more < 1 {
				return trim, false, false, false, 0, ErrStreamSyntax
			}
			// The optional "~" (approximate) or "=" (exact) marker is only
			// recognized when the value follows it.
			approx := false
			val := args[i+1]
			if more >= 2 {
				switch string(val) {
				case "~":
					approx = true
					i++
					val = args[i+1]
				case "=":
					i++
					val = args[i+1]
				}
			}
			if hasTrim {
				return trim, false, false, false, 0, ErrStreamTrimConflict
			}
			if opt == "MAXLEN" {
				n, err := strconv.ParseInt(string(val), 10, 64)
				if err != nil {
					return trim, false, false, false, 0, ErrStreamNotInteger
				}
				if n < 0 {
					return trim, false, false, false, 0, ErrStreamMaxLenNegative
				}
				u := uint64(n)
				trim.MaxLen = &u
			} else {
				id, ok := ParseStreamIDStrict(string(val), 0, nil)
				if !ok {
					return trim, false, false, false, 0, ErrInvalidStreamID
				}
				trim.MinID = &id
			}
			trim.Approximate = approx
			hasTrim = true
			i++
		case "LIMIT":
			if more < 1 {
				return trim, false, false, false, 0, ErrStreamSyntax
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return trim, false, false, false, 0, ErrStreamNotInteger
			}
			if n < 0 {
				return trim, false, false, false, 0, ErrStreamLimitNegative
			}
			trim.Limit = int(n)
			limitGiven = true
			i++
		case "NOMKSTREAM":
			if !xadd {
				return trim, false, false, false, 0, ErrStreamSyntax
			}
			noMkStream = true
		default:
			// Not an option: the caller decides what the argument is.
			return trim, hasTrim, limitGiven, noMkStream, i, nil
		}
	}
	return trim, hasTrim, limitGiven, noMkStream, i, nil
}

// StreamAdd appends an entry to the stream under key, creating the stream
// when it is missing. It returns the identifier of the new entry.
//
// The identifier is chosen according to mode: automatically from the current
// time, automatically from the millisecond part of id, or as given by the
// caller. ErrStreamIDNotGreater is returned when the identifier is not greater
// than the one of the last entry. ErrStreamNotCreated is returned when the
// stream is missing and noMkStream is set.
func (s *Store) StreamAdd(key []byte, mode StreamIDMode, id StreamID, fields [][]byte, noMkStream bool) (StreamID, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		if noMkStream {
			return StreamID{}, ErrStreamNotCreated
		}
		e = &Entry{Type: TypeStream, Stream: &Stream{}, cost: len(string(key))}
		s.addBytes(sh, int64(len(string(key))))
		sh.data[string(key)] = e
	} else if e.Type != TypeStream {
		return StreamID{}, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	s.markAccess(e)

	st := e.Stream
	newID, err := streamNextID(st, mode, id, s.clock())
	if err != nil {
		return StreamID{}, err
	}
	// The field list is copied: the caller reuses its argument buffer, and
	// the entry has to own the slice it keeps.
	entry := StreamEntry{ID: newID, Fields: copyByteSlices(fields)}
	st.entries = append(st.entries, entry)
	st.lastID = newID
	st.entriesAdded++
	s.addCost(sh, e, entryCost(entry))
	return newID, nil
}

// streamNextID computes the identifier of the next entry of the stream.
func streamNextID(st *Stream, mode StreamIDMode, id StreamID, now time.Time) (StreamID, error) {
	var next StreamID
	switch mode {
	case StreamIDAuto:
		ms := uint64(now.UnixMilli())
		if ms > st.lastID.MS {
			next = StreamID{MS: ms}
		} else {
			next = StreamID{MS: st.lastID.MS, Seq: st.lastID.Seq + 1}
		}
	case StreamIDAutoSeq:
		if st.lastID.MS == id.MS {
			next = StreamID{MS: id.MS, Seq: st.lastID.Seq + 1}
		} else {
			next = StreamID{MS: id.MS}
		}
	default:
		next = id
	}
	if next.Compare(st.lastID) <= 0 {
		return StreamID{}, ErrStreamIDNotGreater
	}
	return next, nil
}

// StreamLen returns the number of entries of the stream under key. ok is
// false when the key is missing.
func (s *Store) StreamLen(key []byte) (int, bool, error) {
	return readEntry(s, key, func(e *Entry) (int, error) {
		if e.Type != TypeStream {
			return 0, ErrWrongType
		}
		return e.Stream.Len(), nil
	})
}

// StreamRange returns the entries of the stream under key in the inclusive
// range [start, end]. The entries are returned in ascending order, or in
// descending order when rev is set. A count of zero or less returns them all.
func (s *Store) StreamRange(key []byte, start, end StreamID, count int, rev bool) ([]StreamEntry, error) {
	entries, ok, err := readEntry(s, key, func(e *Entry) ([]StreamEntry, error) {
		if e.Type != TypeStream {
			return nil, ErrWrongType
		}
		st := e.Stream
		out := make([]StreamEntry, 0, len(st.entries))
		if !rev {
			for i := st.search(start); i < len(st.entries); i++ {
				if st.entries[i].ID.Compare(end) > 0 {
					break
				}
				out = append(out, st.entries[i])
				if count > 0 && len(out) >= count {
					break
				}
			}
			return out, nil
		}
		for i := st.lastIndexLE(end); i >= 0; i-- {
			if st.entries[i].ID.Compare(start) < 0 {
				break
			}
			out = append(out, st.entries[i])
			if count > 0 && len(out) >= count {
				break
			}
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return entries, nil
}

// StreamDelete removes the entries of the stream under key with the given
// identifiers. It returns the number of removed entries.
func (s *Store) StreamDelete(key []byte, ids []StreamID) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeStream {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	s.markAccess(e)

	st := e.Stream
	deleted := 0
	for _, id := range ids {
		idx := st.search(id)
		if idx >= len(st.entries) || st.entries[idx].ID.Compare(id) != 0 {
			continue
		}
		s.addCost(sh, e, -entryCost(st.entries[idx]))
		st.entries = append(st.entries[:idx], st.entries[idx+1:]...)
		if id.Compare(st.maxDeletedID) > 0 {
			st.maxDeletedID = id
		}
		deleted++
	}
	return deleted, nil
}

// StreamTrim removes the entries of the stream under key that exceed the
// given limit. It returns the number of removed entries.
func (s *Store) StreamTrim(key []byte, t StreamTrim) (int, error) {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return 0, nil
	}
	if e.Type != TypeStream {
		return 0, ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	s.markAccess(e)

	st := e.Stream
	cut := 0
	switch {
	case t.MaxLen != nil:
		maxLen := int(*t.MaxLen)
		if len(st.entries) > maxLen {
			cut = len(st.entries) - maxLen
		}
	case t.MinID != nil:
		cut = st.search(*t.MinID)
	}
	if cut <= 0 {
		return 0, nil
	}
	if t.Approximate {
		cut = approximateCut(st, t, cut)
	}
	if t.Limit > 0 && cut > t.Limit {
		cut = t.Limit
	}
	if cut <= 0 {
		return 0, nil
	}
	for i := 0; i < cut; i++ {
		s.addCost(sh, e, -entryCost(st.entries[i]))
	}
	st.entries = append(st.entries[:0], st.entries[cut:]...)
	return cut, nil
}

// approximateCut returns the number of entries an approximate trimming
// removes. Like Redis, it removes whole chunks of entries only, so the stream
// may keep more entries than the limit asks for.
func approximateCut(st *Stream, t StreamTrim, exact int) int {
	n := len(st.entries)
	removed := 0
	for removed < n {
		size := streamNodeEntries
		if n-removed < size {
			size = n - removed
		}
		switch {
		case t.MaxLen != nil:
			if n-removed-size < int(*t.MaxLen) {
				return removed
			}
		case t.MinID != nil:
			if st.entries[removed+size-1].ID.Compare(*t.MinID) >= 0 {
				return removed
			}
		}
		removed += size
	}
	if removed > exact {
		return exact
	}
	return removed
}

// StreamSetID sets the identifier of the last entry of the stream under key,
// like the XSETID command does. When setEntriesAdded is set, the number of
// entries ever added to the stream is updated as well. When maxDeletedID is
// not nil, the highest deleted identifier of the stream is updated too.
func (s *Store) StreamSetID(key []byte, id StreamID, entriesAdded uint64, setEntriesAdded bool, maxDeletedID *StreamID) error {
	sh := s.shardOf(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := s.entryLocked(sh, key)
	if !ok {
		return ErrStreamNotFound
	}
	if e.Type != TypeStream {
		return ErrWrongType
	}
	s.touchKeyLocked(sh, key)
	s.markAccess(e)

	st := e.Stream
	if maxDeletedID != nil && id.Compare(*maxDeletedID) < 0 {
		return ErrStreamSetIDMaxDeleted
	}
	if id.Compare(st.maxDeletedID) < 0 {
		return ErrStreamMaxDeletedTooSmall
	}
	// Redis compares the identifier with the last entry of the stream, not
	// with the identifier of the last entry: XSETID may have raised it.
	if st.Len() > 0 && id.Compare(st.topID()) < 0 {
		return ErrStreamIDNotGreater
	}
	if setEntriesAdded && entriesAdded < uint64(st.Len()) {
		return ErrStreamEntriesAddedTooSmall
	}
	st.lastID = id
	if setEntriesAdded {
		st.entriesAdded = entriesAdded
	}
	if maxDeletedID != nil && !maxDeletedID.IsZero() {
		st.maxDeletedID = *maxDeletedID
	}
	return nil
}

// StreamLastID returns the identifier of the last entry of the stream under
// key. ok is false when the key is missing.
func (s *Store) StreamLastID(key []byte) (StreamID, bool, error) {
	return readEntry(s, key, func(e *Entry) (StreamID, error) {
		if e.Type != TypeStream {
			return StreamID{}, ErrWrongType
		}
		return e.Stream.LastID(), nil
	})
}
