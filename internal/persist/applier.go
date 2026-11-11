package persist

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// Applier replays AOF-style commands (the normalized write commands the
// server propagates) against a set of stores, tracking the current database
// across SELECT.
//
// The replay of the journal and the replication stream of a replica share
// it: a replica applies the commands of its master with the same semantics
// the replay of the journal has.
type Applier struct {
	stores []*store.Store
	db     int
	// swapDBs swaps two databases and returns the slice that owns the
	// result. The default swaps the slice of the applier in place (the AOF
	// load runs before anybody is served); a server that keeps the slice
	// shared with its readers replaces it atomically and hands the new
	// slice back.
	swapDBs func(stores []*store.Store, a, b int) []*store.Store
}

// NewApplier returns an applier over the given databases. The replay starts
// in the first database, like the servers do.
func NewApplier(stores []*store.Store) *Applier {
	return &Applier{stores: stores, swapDBs: swapStoresInPlace}
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
		return applyAOFCommand(a.stores[a.db], args)
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

// LoadAOF replays a RESP command log into the given stores. A truncated
// trailing command is tolerated; other read errors abort the load.
func LoadAOF(r io.Reader, stores []*store.Store) error {
	rd := resp.NewReader(r)
	ap := NewApplier(stores)
	for {
		v, err := rd.ReadValue()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end or truncated tail
			}
			return fmt.Errorf("persist: aof read: %w", err)
		}
		if v.Kind != resp.KindArray || v.IsNull {
			return fmt.Errorf("persist: aof contains a non-command frame")
		}
		args := make([][]byte, 0, len(v.Array))
		for i := range v.Array {
			el := v.Array[i]
			if el.Kind != resp.KindBulkString || el.IsNull {
				return fmt.Errorf("persist: aof command arguments must be bulk strings")
			}
			args = append(args, el.Str)
		}
		if err := ap.Apply(args); err != nil {
			return err
		}
	}
}
