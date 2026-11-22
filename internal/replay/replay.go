// Package replay applies recorded write commands to a set of databases.
//
// It is the stream-local half of both persistence paths: LoadAOF reads the
// bytes of the append-only file and hands the commands to an Applier, and a
// replica reads its master's stream and does the same. The package only knows
// the store and the standard library: the RESP framing belongs to the callers.
package replay

import (
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// RestoreFunc decodes the payload of a RESTORE command into the entry it
// holds. The persistence package supplies its RestoreValue, which keeps the
// replay free of the dump encoding.
type RestoreFunc func(data []byte) (*store.Entry, error)

// Applier replays normalized write commands against a set of stores, tracking
// the current database across SELECT.
//
// The replay of the journal and the replication stream of a replica share it:
// a replica applies the commands of its master with the same semantics the
// replay of the journal has.
type Applier struct {
	stores []*store.Store
	db     int
	// swapDBs swaps two databases and returns the slice that owns the
	// result. The default swaps the slice of the applier in place (the AOF
	// load runs before anybody is served); a server that keeps the slice
	// shared with its readers replaces it atomically and hands the new
	// slice back.
	swapDBs func(stores []*store.Store, a, b int) []*store.Store
	// restore decodes the value of a RESTORE command. It is injected by the
	// caller so that this package does not depend on the persistence
	// encoding.
	restore RestoreFunc
}

// NewApplier returns an applier over the given databases. The replay starts
// in the first database, like the servers do. restore decodes the payload of
// the RESTORE commands of the stream.
func NewApplier(stores []*store.Store, restore RestoreFunc) *Applier {
	return &Applier{stores: stores, swapDBs: swapStoresInPlace, restore: restore}
}

// SetSwapDBs replaces the way SWAPDB exchanges two databases. The server
// hands the hook over when its slice of the stores is shared with the
// readers.
func (a *Applier) SetSwapDBs(fn func(stores []*store.Store, a, b int) []*store.Store) {
	a.swapDBs = fn
}

// swapStoresInPlace exchanges two entries of the given slice. The slice is
// shared with the caller, so the swap is visible there as well.
func swapStoresInPlace(stores []*store.Store, a, b int) []*store.Store {
	if a >= 0 && a < len(stores) && b >= 0 && b < len(stores) {
		stores[a], stores[b] = stores[b], stores[a]
	}
	return stores
}

// DB returns the database the replay is in (the last SELECT it applied).
func (a *Applier) DB() int { return a.db }

// Apply applies one normalized command. The commands that are not bound to
// the current database (SELECT, FLUSHALL, SWAPDB) and the ones that address
// another database (COPY DB, MOVE) are handled here; the rest go to the
// single-database replay.
func (a *Applier) Apply(args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	switch strings.ToLower(string(args[0])) {
	case "select":
		if len(args) == 2 {
			if n, err := strconv.Atoi(string(args[1])); err == nil && n >= 0 && n < len(a.stores) {
				a.db = n
			}
		}
		return nil
	case "flushall":
		for _, st := range a.stores {
			st.Flush()
		}
		return nil
	case "swapdb":
		// SWAPDB a b: the owner of the slice performs the swap.
		if len(args) == 3 {
			x, err1 := strconv.Atoi(string(args[1]))
			y, err2 := strconv.Atoi(string(args[2]))
			if err1 == nil && err2 == nil {
				a.stores = a.swapDBs(a.stores, x, y)
			}
		}
		return nil
	case "copy":
		a.applyCopy(args)
		return nil
	case "move":
		a.applyMove(args)
		return nil
	default:
		if a.db >= len(a.stores) {
			a.db = 0
		}
		return a.applyCommand(a.stores[a.db], args)
	}
}

// applyCopy replays COPY key dst [DB n] [REPLACE]: the source is read from
// the database the replay is in, the destination is written to the one the
// command named. It mirrors the semantics of the command handler.
func (a *Applier) applyCopy(args [][]byte) {
	if len(args) < 3 {
		return
	}
	destDB := a.db
	replace := false
	for i := 3; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "DB":
			if i+1 >= len(args) {
				return
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 0 || n >= len(a.stores) {
				return
			}
			destDB = n
			i += 2
		case "REPLACE":
			replace = true
			i++
		default:
			return
		}
	}
	src := a.stores[a.db]
	dst := a.stores[destDB]
	e, ok := src.CopyEntry(args[1])
	if !ok {
		return
	}
	if dst == src {
		if _, exists := dst.CopyEntry(args[2]); exists && !replace {
			return
		}
	}
	dst.StoreEntry(args[2], e)
}

// applyMove replays MOVE key db: the key leaves the database the replay is
// in and appears in the target one.
func (a *Applier) applyMove(args [][]byte) {
	if len(args) != 3 {
		return
	}
	n, err := strconv.Atoi(string(args[2]))
	if err != nil || n < 0 || n >= len(a.stores) || n == a.db {
		return
	}
	a.stores[a.db].MoveEntry(args[1], a.stores[n]) //nolint:errcheck
}

// applyCommand applies one replayed command to a single store. Store-level
// errors (e.g. WRONGTYPE) are ignored because replay is expected to reproduce
// the exact original execution, including failures.
func (a *Applier) applyCommand(st *store.Store, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToLower(string(args[0]))
	key := func() []byte {
		if len(args) > 1 {
			return args[1]
		}
		return nil
	}
	switch name {
	case "mset":
		// MSET key value [key value ...]
		if len(args) >= 3 && len(args)%2 == 1 {
			st.SetStrings(args[1:], false)
		}
	case "msetnx":
		if len(args) >= 3 && len(args)%2 == 1 {
			st.SetStrings(args[1:], true)
		}
	case "getset":
		if len(args) >= 3 {
			st.GetSet(key(), args[2]) //nolint:errcheck
		}
	case "getdel":
		if len(args) >= 2 {
			st.GetDel(key()) //nolint:errcheck
		}
	case "getex":
		if len(args) >= 2 {
			st.GetEx(key(), time.Time{}, false) //nolint:errcheck
		}
	case "setex":
		if len(args) >= 4 {
			sec, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err == nil && sec > 0 {
				st.SetEx(key(), args[3], time.Duration(sec)*time.Second)
			}
		}
	case "psetex":
		if len(args) >= 4 {
			ms, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err == nil && ms > 0 {
				st.SetEx(key(), args[3], time.Duration(ms)*time.Millisecond)
			}
		}
	case "setnx":
		if len(args) >= 3 {
			st.SetNX(key(), args[2])
		}
	case "setrange":
		if len(args) >= 4 {
			off, err := strconv.ParseInt(string(args[2]), 10, 64)
			if err == nil {
				st.SetRange(key(), off, args[3])
			}
		}
	case "setbit":
		if len(args) == 4 {
			if off, err := strconv.ParseUint(string(args[2]), 10, 64); err == nil {
				if bit, err := strconv.ParseInt(string(args[3]), 10, 64); err == nil && (bit == 0 || bit == 1) {
					st.SetBit(key(), off, int(bit)) //nolint:errcheck
				}
			}
		}
	case "bitop":
		if len(args) >= 4 {
			if op, ok := store.ParseBitOp(args[1]); ok {
				st.BitOp(op, args[2], args[3:]) //nolint:errcheck
			}
		}
	case "bitfield":
		if len(args) >= 2 {
			if ops, err := store.ParseBitFieldOps(args[2:]); err == nil {
				st.BitField(key(), ops) //nolint:errcheck
			}
		}
	case "pfadd":
		if len(args) >= 2 {
			st.PFAdd(key(), args[2:]...) //nolint:errcheck
		}
	case "pfcount":
		if len(args) == 2 {
			// The single-key form may refresh the cached cardinality: the
			// replay reproduces that mutation, like Redis does.
			st.PFCountKey(key()) //nolint:errcheck
		} else if len(args) > 2 {
			st.PFCount(args[1:]...) //nolint:errcheck
		}
	case "pfmerge":
		if len(args) >= 2 {
			st.PFMerge(key(), args[2:]...) //nolint:errcheck
		}
	case "incrbyfloat":
		if len(args) >= 3 {
			if d, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
				st.IncrByFloat(key(), d)
			}
		}
	case "set":
		if len(args) < 3 {
			return nil
		}
		// The flags of SET (and of the commands propagated in its shape):
		// NX/XX gate the write, KEEPTTL keeps the expiry of the key, and
		// the expiry options arm a TTL. Redis propagates the relative
		// expiry options (EX/PX) of SET as an absolute PXAT, so the replay
		// reproduces the same moment of expiry.
		var nx, xx, keepttl bool
		var exSec, pxMs, exAtSec, pxAtMs int64
		for i := 3; i < len(args); i++ {
			switch strings.ToUpper(string(args[i])) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			case "KEEPTTL":
				keepttl = true
			case "GET":
				// The reply carries the old value; the state is the same.
			case "EX":
				if i+1 < len(args) {
					exSec, _ = strconv.ParseInt(string(args[i+1]), 10, 64)
					i++
				}
			case "PX":
				if i+1 < len(args) {
					pxMs, _ = strconv.ParseInt(string(args[i+1]), 10, 64)
					i++
				}
			case "EXAT":
				if i+1 < len(args) {
					exAtSec, _ = strconv.ParseInt(string(args[i+1]), 10, 64)
					i++
				}
			case "PXAT":
				if i+1 < len(args) {
					pxAtMs, _ = strconv.ParseInt(string(args[i+1]), 10, 64)
					i++
				}
			}
		}
		if nx && st.Exists(key()) == 1 {
			return nil
		}
		if xx && st.Exists(key()) == 0 {
			return nil
		}
		if keepttl {
			// The value is replaced and the remaining time of the key is
			// armed again (the store drops the TTL with the entry).
			if d, ok := st.TTL(key()); ok && d > 0 {
				st.SetString(key(), args[2])
				st.Expire(key(), d) //nolint:errcheck
			} else {
				st.SetString(key(), args[2])
			}
		} else {
			// Without KEEPTTL the SET replaces the entry: the TTL of the
			// old value is gone.
			st.SetString(key(), args[2])
		}
		switch {
		case pxAtMs != 0:
			st.ExpireAt(key(), time.UnixMilli(pxAtMs)) //nolint:errcheck
		case exAtSec != 0:
			st.ExpireAt(key(), time.Unix(exAtSec, 0)) //nolint:errcheck
		case pxMs > 0:
			st.Expire(key(), time.Duration(pxMs)*time.Millisecond) //nolint:errcheck
		case exSec > 0:
			st.Expire(key(), time.Duration(exSec)*time.Second) //nolint:errcheck
		}
	case "append":
		if len(args) == 3 {
			st.Append(key(), args[2])
		}
	case "incr":
		st.IncrBy(key(), 1)
	case "decr":
		st.IncrBy(key(), -1)
	case "incrby":
		if len(args) == 3 {
			if d, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.IncrBy(key(), d)
			}
		}
	case "decrby":
		if len(args) == 3 {
			if d, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.IncrBy(key(), -d)
			}
		}
	case "del":
		st.Delete(args[1:]...)
	case "rename":
		if len(args) == 3 {
			st.Rename(key(), args[2]) //nolint:errcheck
		}
	case "renamenx":
		if len(args) == 3 {
			st.RenameNX(key(), args[2]) //nolint:errcheck
		}
	case "restore":
		// RESTORE key ttl serialized-value
		if len(args) >= 4 && a.restore != nil {
			if e, err := a.restore(args[3]); err == nil {
				ttl, _ := strconv.ParseInt(string(args[2]), 10, 64)
				st.RestoreEntryWithTTL(key(), e, time.Duration(ttl)*time.Millisecond)
			}
		}
	case "expire":
		if len(args) == 3 {
			if s, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				if s <= 0 {
					st.Delete(key())
				} else {
					st.Expire(key(), time.Duration(s)*time.Second)
				}
			}
		}
	case "pexpire":
		if len(args) == 3 {
			if ms, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				if ms <= 0 {
					st.Delete(key())
				} else {
					st.Expire(key(), time.Duration(ms)*time.Millisecond)
				}
			}
		}
	case "expireat":
		if len(args) == 3 {
			if ts, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.ExpireAt(key(), time.Unix(ts, 0))
			}
		}
	case "pexpireat":
		if len(args) == 3 {
			if ts, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.ExpireAt(key(), time.UnixMilli(ts))
			}
		}
	case "persist":
		st.Persist(key())
	case "flushdb":
		st.Flush()
	case "lpush":
		st.ListPush(key(), true, args[2:]...)
	case "rpush":
		st.ListPush(key(), false, args[2:]...)
	case "linsert":
		// LINSERT key BEFORE|AFTER pivot element
		if len(args) == 5 {
			before := strings.EqualFold(string(args[2]), "BEFORE")
			st.ListInsert(key(), before, args[3], args[4]) //nolint:errcheck
		}
	case "lrem":
		// LREM key count element
		if len(args) == 4 {
			if n, err := strconv.Atoi(string(args[2])); err == nil {
				st.ListRemove(key(), n, args[3]) //nolint:errcheck
			}
		}
	case "ltrim":
		// LTRIM key start stop
		if len(args) == 4 {
			start, err1 := strconv.Atoi(string(args[2]))
			stop, err2 := strconv.Atoi(string(args[3]))
			if err1 == nil && err2 == nil {
				st.ListTrim(key(), start, stop) //nolint:errcheck
			}
		}
	case "lmove":
		// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
		if len(args) == 5 {
			fromLeft := strings.EqualFold(string(args[3]), "LEFT")
			toLeft := strings.EqualFold(string(args[4]), "LEFT")
			st.ListMove(key(), args[2], fromLeft, toLeft) //nolint:errcheck
		}
	case "rpoplpush":
		// RPOPLPUSH source destination
		if len(args) == 3 {
			st.ListMove(key(), args[2], false, true) //nolint:errcheck
		}
	case "lpushx":
		// LPUSHX key element [element ...]
		if len(args) >= 3 {
			st.ListPushExist(key(), true, args[2:]) //nolint:errcheck
		}
	case "rpushx":
		// RPUSHX key element [element ...]
		if len(args) >= 3 {
			st.ListPushExist(key(), false, args[2:]) //nolint:errcheck
		}
	case "lpop":
		if len(args) == 2 {
			st.ListPop(key(), true)
		}
	case "rpop":
		if len(args) == 2 {
			st.ListPop(key(), false)
		}
	case "lset":
		if len(args) == 4 {
			if i, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
				st.ListSet(key(), int(i), args[3])
			}
		}
	case "hset":
		if len(args) >= 4 && len(args)%2 == 0 {
			for i := 2; i+1 < len(args); i += 2 {
				st.HashSet(key(), args[i], args[i+1])
			}
		}
	case "hdel":
		st.HashDel(key(), args[2:]...)
	case "smove":
		if len(args) == 4 {
			st.SetMove(key(), args[2], args[3]) //nolint:errcheck
		}
	case "spop":
		// The command reaches the journal either as SPOP key [count] (the
		// journals of the older servers) or as SPOP key member... (the
		// effect the command propagates now, the way Redis propagates
		// SREM operations).
		if len(args) > 2 {
			if n, err := strconv.Atoi(string(args[2])); err == nil && len(args) == 3 {
				st.SetPop(key(), n) //nolint:errcheck
			} else {
				st.SetRem(key(), args[2:]...) //nolint:errcheck
			}
		} else {
			st.SetPop(key(), 0) //nolint:errcheck
		}
	case "sdiffstore", "sinterstore", "sunionstore":
		if len(args) >= 3 {
			var members [][]byte
			var err error
			switch name {
			case "sdiffstore":
				members, err = st.SetDiff(args[2:])
			case "sinterstore":
				members, err = st.SetInter(args[2:])
			default:
				members, err = st.SetUnion(args[2:])
			}
			if err == nil {
				st.SetStore(args[1], members) //nolint:errcheck
			}
		}
	case "sadd":
		st.SetAdd(key(), args[2:]...)
	case "srem":
		st.SetRem(key(), args[2:]...)
	case "zpopmin", "zpopmax":
		// ZPOPMIN|ZPOPMAX key [count]
		if len(args) >= 2 {
			count := 0
			if len(args) == 3 {
				if n, err := strconv.Atoi(string(args[2])); err == nil {
					count = n
				}
			}
			st.ZPop(key(), count, name == "zpopmin") //nolint:errcheck
		}
	case "zremrangebyrank":
		// ZREMRANGEBYRANK key start stop
		if len(args) == 4 {
			start, err1 := strconv.Atoi(string(args[2]))
			stop, err2 := strconv.Atoi(string(args[3]))
			if err1 == nil && err2 == nil {
				st.ZRemRangeByRank(key(), start, stop) //nolint:errcheck
			}
		}
	case "zunionstore", "zinterstore":
		// ZUNIONSTORE|ZINTERSTORE dest numkeys key [key ...]
		if len(args) >= 4 {
			if n, err := strconv.Atoi(string(args[2])); err == nil && n >= 1 && len(args) >= 3+n {
				keys := args[3 : 3+n]
				if name == "zunionstore" {
					st.ZUnionStore(args[1], keys, nil, store.AggregateSum) //nolint:errcheck
				} else {
					st.ZInterStore(args[1], keys, nil, store.AggregateSum) //nolint:errcheck
				}
			}
		}
	case "zdiffstore":
		// ZDIFFSTORE dest numkeys key [key ...]
		if len(args) >= 4 {
			if n, err := strconv.Atoi(string(args[2])); err == nil && n >= 1 && len(args) >= 3+n {
				st.ZDiffStore(args[1], args[3:3+n]) //nolint:errcheck
			}
		}
	case "zincrby":
		// ZINCRBY key increment member
		if len(args) == 4 {
			if d, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
				st.ZIncrBy(key(), d, args[3]) //nolint:errcheck
			}
		}
	case "zadd":
		if len(args) >= 4 && len(args)%2 == 0 {
			for i := 2; i+1 < len(args); i += 2 {
				if score, err := strconv.ParseFloat(string(args[i]), 64); err == nil {
					st.ZAdd(key(), score, args[i+1])
				}
			}
		}
	case "zrem":
		st.ZRem(key(), args[2:]...)
	case "xadd":
		applyStreamAdd(st, args)
	case "xdel":
		applyStreamDel(st, args)
	case "xtrim":
		applyStreamTrim(st, args)
	case "xsetid":
		applyStreamSetID(st, args)
	case "xgroup":
		applyStreamGroup(st, args)
	case "xack":
		applyStreamAck(st, args)
	case "xclaim":
		applyStreamClaim(st, args)
	}
	return nil
}

// applyStreamGroup replays an XGROUP command.
func applyStreamGroup(st *store.Store, args [][]byte) {
	cmd, err := store.ParseStreamGroupArgs(args[1:])
	if err != nil {
		return
	}
	id := cmd.ID
	if cmd.IDIsDollar {
		last, ok, err := st.StreamLastID(cmd.Key)
		if err == nil && ok {
			id = last
		}
	}
	switch cmd.Sub {
	case "CREATE":
		st.StreamGroupCreate(cmd.Key, cmd.Group, id, cmd.EntriesRead, cmd.SetEntriesRead, cmd.MkStream) //nolint:errcheck
	case "DESTROY":
		st.StreamGroupDestroy(cmd.Key, cmd.Group) //nolint:errcheck
	case "CREATECONSUMER":
		st.StreamGroupCreateConsumer(cmd.Key, cmd.Group, cmd.Consumer) //nolint:errcheck
	case "DELCONSUMER":
		st.StreamGroupDelConsumer(cmd.Key, cmd.Group, cmd.Consumer) //nolint:errcheck
	case "SETID":
		st.StreamGroupSetID(cmd.Key, cmd.Group, id, cmd.EntriesRead, cmd.SetEntriesRead) //nolint:errcheck
	}
}

// applyStreamAck replays an XACK command.
func applyStreamAck(st *store.Store, args [][]byte) {
	// XACK key group id [id ...]
	if len(args) < 4 {
		return
	}
	ids := make([]store.StreamID, 0, len(args)-3)
	for _, arg := range args[3:] {
		id, ok := store.ParseStreamIDStrict(string(arg), 0, nil)
		if !ok {
			return
		}
		ids = append(ids, id)
	}
	st.StreamAck(args[1], string(args[2]), ids) //nolint:errcheck
}

// applyStreamClaim replays an XCLAIM command.
func applyStreamClaim(st *store.Store, args [][]byte) {
	cmd, err := store.ParseStreamClaimArgs(args[1:])
	if err != nil {
		return
	}
	st.StreamClaim(cmd.Key, cmd.Group, cmd.Consumer, cmd.MinIdle, cmd.IDs, cmd.Options) //nolint:errcheck
}

// applyStreamAdd replays an XADD command.
func applyStreamAdd(st *store.Store, args [][]byte) {
	// XADD key [options] id field value ...
	if len(args) < 5 {
		return
	}
	parsed, err := store.ParseStreamAddArgs(args[2:])
	if err != nil {
		return
	}
	st.StreamAdd(args[1], parsed.Mode, parsed.ID, parsed.Fields, parsed.NoMkStream) //nolint:errcheck
	if parsed.HasTrim {
		st.StreamTrim(args[1], parsed.Trim) //nolint:errcheck
	}
}

// applyStreamDel replays an XDEL command.
func applyStreamDel(st *store.Store, args [][]byte) {
	// XDEL key id [id ...]
	if len(args) < 3 {
		return
	}
	ids := make([]store.StreamID, 0, len(args)-2)
	for _, arg := range args[2:] {
		id, ok := store.ParseStreamIDStrict(string(arg), 0, nil)
		if !ok {
			return
		}
		ids = append(ids, id)
	}
	st.StreamDelete(args[1], ids) //nolint:errcheck
}

// applyStreamTrim replays an XTRIM command.
func applyStreamTrim(st *store.Store, args [][]byte) {
	// XTRIM key MAXLEN|MINID [~|=] threshold [LIMIT count]
	if len(args) < 4 {
		return
	}
	trim, err := store.ParseStreamTrimArgs(args[2:])
	if err != nil {
		return
	}
	st.StreamTrim(args[1], trim) //nolint:errcheck
}

// applyStreamSetID replays an XSETID command.
func applyStreamSetID(st *store.Store, args [][]byte) {
	// XSETID key id [ENTRIESADDED count]
	if len(args) < 3 {
		return
	}
	id, ok := store.ParseStreamIDStrict(string(args[2]), 0, nil)
	if !ok {
		return
	}
	entriesAdded := uint64(0)
	setEntriesAdded := false
	var maxDeletedID *store.StreamID
	for i := 3; i+1 < len(args); i += 2 {
		switch strings.ToUpper(string(args[i])) {
		case "ENTRIESADDED":
			if n, err := strconv.ParseUint(string(args[i+1]), 10, 64); err == nil {
				entriesAdded = n
				setEntriesAdded = true
			}
		case "MAXDELETEDID":
			if m, ok := store.ParseStreamIDStrict(string(args[i+1]), 0, nil); ok {
				maxDeletedID = &m
			}
		}
	}
	st.StreamSetID(args[1], id, entriesAdded, setEntriesAdded, maxDeletedID) //nolint:errcheck
}
