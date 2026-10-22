package server

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amir/redis-implements/internal/persist"
)

// cmdRename moves the value of a key to another key.
func cmdRename(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "rename")
		return nil
	}
	ok, err := ctx.Store.Rename(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteError("ERR no such key")
	}
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdRenameNX moves the value of a key, but only when the destination does
// not exist.
func cmdRenameNX(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "renamenx")
		return nil
	}
	ok, err := ctx.Store.RenameNX(ctx.Args[1], ctx.Args[2])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdCopy copies the value of a key to another key (of the same or another
// database).
func cmdCopy(ctx *Ctx) error {
	if len(ctx.Args) < 3 {
		wrongArgs(ctx.W, "copy")
		return nil
	}
	destDB := ctx.Client.db
	replace := false
	for i := 3; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "DB":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil {
				return ctx.W.WriteError("ERR value is not an integer or out of range")
			}
			if n < 0 || n >= ctx.Srv.DBCount() {
				return ctx.W.WriteError("ERR DB index is out of range")
			}
			destDB = n
			i += 2
		case "REPLACE":
			replace = true
			i++
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}
	src := ctx.Store
	dst := ctx.Srv.DB(destDB)
	e, ok := src.CopyEntry(ctx.Args[1])
	if !ok {
		return ctx.W.WriteInteger(0)
	}
	if dst == src {
		if _, exists := dst.CopyEntry(ctx.Args[2]); exists && !replace {
			return ctx.W.WriteInteger(0)
		}
	}
	dst.StoreEntry(ctx.Args[2], e)
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdMove moves a key to another database.
func cmdMove(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "move")
		return nil
	}
	n, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError("ERR value is not an integer or out of range")
	}
	if n < 0 || n >= ctx.Srv.DBCount() {
		return ctx.W.WriteError("ERR DB index is out of range")
	}
	if n == ctx.Client.db {
		return ctx.W.WriteError("ERR source and destination objects are the same")
	}
	ok, err := ctx.Store.MoveEntry(ctx.Args[1], ctx.Srv.DB(n))
	if err != nil {
		return writeWrongType(ctx.W)
	}
	if !ok {
		return ctx.W.WriteInteger(0)
	}
	ctx.propagateCmd()
	return ctx.W.WriteInteger(1)
}

// cmdDump returns the serialized value of a key.
func cmdDump(ctx *Ctx) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, "dump")
		return nil
	}
	e, ok := ctx.Store.CopyEntry(ctx.Args[1])
	if !ok {
		return ctx.W.WriteNullBulk()
	}
	payload, err := persist.DumpValue(e)
	if err != nil {
		return ctx.W.WriteError("ERR " + err.Error())
	}
	return ctx.W.WriteBulkString(payload)
}

// cmdRestore creates a key from a serialized value.
func cmdRestore(ctx *Ctx) error {
	if len(ctx.Args) < 4 {
		wrongArgs(ctx.W, "restore")
		return nil
	}
	ttl, err := strconv.ParseInt(string(ctx.Args[2]), 10, 64)
	if err != nil {
		return ctx.W.WriteError("ERR Invalid TTL value, must be >= 0")
	}
	replace := false
	for i := 4; i < len(ctx.Args); {
		if strings.EqualFold(string(ctx.Args[i]), "REPLACE") {
			replace = true
			i++
			continue
		}
		return ctx.W.WriteError("ERR syntax error")
	}
	if !replace {
		if _, exists := ctx.Store.CopyEntry(ctx.Args[1]); exists {
			return ctx.W.WriteError("BUSYKEY Target key name already exists.")
		}
	}
	e, err := persist.RestoreValue(ctx.Args[3])
	if err != nil {
		return ctx.W.WriteError("ERR " + err.Error())
	}
	ctx.Store.RestoreEntryWithTTL(ctx.Args[1], e, time.Duration(ttl)*time.Millisecond)
	ctx.propagateCmd()
	return ctx.W.WriteSimpleString("OK")
}

// cmdExpireTime returns the expiration of a key as a Unix timestamp.
func cmdExpireTime(ctx *Ctx) error {
	return expireTimeCommon(ctx, "expiretime", false)
}

// cmdPExpireTime returns the expiration of a key as a Unix timestamp in
// milliseconds.
func cmdPExpireTime(ctx *Ctx) error {
	return expireTimeCommon(ctx, "pexpiretime", true)
}

func expireTimeCommon(ctx *Ctx, cmd string, millis bool) error {
	if len(ctx.Args) != 2 {
		wrongArgs(ctx.W, cmd)
		return nil
	}
	if millis {
		ts, ok := ctx.Store.PExpireTime(ctx.Args[1])
		if !ok {
			return ctx.W.WriteInteger(-2)
		}
		return ctx.W.WriteInteger(ts)
	}
	ts, ok := ctx.Store.ExpireTime(ctx.Args[1])
	if !ok {
		return ctx.W.WriteInteger(-2)
	}
	return ctx.W.WriteInteger(ts)
}

// cmdSwapDB swaps two databases.
func cmdSwapDB(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "swapdb")
		return nil
	}
	a, err := strconv.Atoi(string(ctx.Args[1]))
	if err != nil {
		return ctx.W.WriteError("ERR invalid first DB index")
	}
	b, err := strconv.Atoi(string(ctx.Args[2]))
	if err != nil {
		return ctx.W.WriteError("ERR invalid second DB index")
	}
	if a < 0 || a >= ctx.Srv.DBCount() || b < 0 || b >= ctx.Srv.DBCount() {
		return ctx.W.WriteError("ERR DB index is out of range")
	}
	if a == b {
		return ctx.W.WriteSimpleString("OK")
	}
	ctx.Srv.SwapDBs(a, b)
	return ctx.W.WriteSimpleString("OK")
}

// cmdObject implements the OBJECT subcommands.
func cmdObject(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "object")
		return nil
	}
	switch strings.ToLower(string(ctx.Args[1])) {
	case "encoding":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "object encoding")
			return nil
		}
		enc, ok := ctx.Store.EntryEncoding(ctx.Args[2])
		if !ok {
			return ctx.W.WriteNullBulk()
		}
		return ctx.W.WriteBulkString([]byte(enc))
	case "help":
		return ctx.W.WriteBulkString([]byte("OBJECT <subcommand> [<arg> [value] [opt] ...]. " +
			"Supported subcommands are: ENCODING, FREQ, IDLETIME, REFCOUNT, HELP"))
	case "refcount", "idletime", "freq":
		if len(ctx.Args) != 3 {
			wrongArgs(ctx.W, "object "+string(ctx.Args[1]))
			return nil
		}
		if _, ok := ctx.Store.EntryType(ctx.Args[2]); !ok {
			return ctx.W.WriteError("ERR no such key")
		}
		return ctx.W.WriteInteger(1)
	default:
		return ctx.W.WriteError("ERR Unknown OBJECT subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'")
	}
}

// cmdSort sorts the elements of a list, a set or a sorted set.
func cmdSort(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "sort")
		return nil
	}
	alpha := false
	desc := false
	storeDest := []byte(nil)
	byPattern := ""
	var getPatterns []string
	limitOffset, limitCount := 0, 0
	hasLimit := false
	for i := 2; i < len(ctx.Args); {
		switch strings.ToUpper(string(ctx.Args[i])) {
		case "ALPHA":
			alpha = true
			i++
		case "ASC":
			desc = false
			i++
		case "DESC":
			desc = true
			i++
		case "LIMIT":
			if i+2 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			off, err := strconv.Atoi(string(ctx.Args[i+1]))
			if err != nil || off < 0 {
				return ctx.W.WriteError("ERR syntax error")
			}
			cnt, err := strconv.Atoi(string(ctx.Args[i+2]))
			if err != nil {
				return ctx.W.WriteError("ERR syntax error")
			}
			limitOffset, limitCount = off, cnt
			hasLimit = true
			i += 3
		case "STORE":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			storeDest = ctx.Args[i+1]
			i += 2
		case "BY":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			byPattern = string(ctx.Args[i+1])
			i += 2
		case "GET":
			if i+1 >= len(ctx.Args) {
				return ctx.W.WriteError("ERR syntax error")
			}
			getPatterns = append(getPatterns, string(ctx.Args[i+1]))
			i += 2
		default:
			return ctx.W.WriteError("ERR syntax error")
		}
	}

	elems, err := ctx.Store.SortElements(ctx.Args[1])
	if err != nil {
		return writeWrongType(ctx.W)
	}
	// The value the elements are sorted by: the weight looked up with the BY
	// pattern, or the element itself.
	sortValues := make([][]byte, len(elems))
	for i, e := range elems {
		if byPattern == "" {
			sortValues[i] = e
			continue
		}
		v, ok := ctx.lookupSortValue(byPattern, e)
		if !ok {
			// A missing weight key: the element itself is the weight.
			sortValues[i] = e
			continue
		}
		sortValues[i] = v
	}
	if byPattern == "" && !alpha {
		// Without BY the elements themselves are compared, and they must be
		// numbers.
		for _, e := range sortValues {
			if _, ok := parseFloat(e); !ok {
				return ctx.W.WriteError("ERR One or more scores can't be converted into double")
			}
		}
	}

	idx := make([]int, len(elems))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		c := compareSortValues(sortValues[idx[a]], sortValues[idx[b]], alpha)
		if desc {
			return c > 0
		}
		return c < 0
	})
	sorted := make([][]byte, len(elems))
	for i, j := range idx {
		sorted[i] = elems[j]
	}
	if hasLimit {
		if limitOffset >= len(sorted) {
			sorted = nil
		} else {
			sorted = sorted[limitOffset:]
		}
		if limitCount >= 0 && limitCount < len(sorted) {
			sorted = sorted[:limitCount]
		}
	}

	// The GET patterns replace the elements by the values they point at.
	if len(getPatterns) > 0 {
		out := make([][]byte, 0, len(sorted)*len(getPatterns))
		for _, e := range sorted {
			for _, pattern := range getPatterns {
				v, _ := ctx.lookupSortValue(pattern, e)
				out = append(out, v)
			}
		}
		sorted = out
	}

	if storeDest != nil {
		if _, err := ctx.Store.SetList(storeDest, sorted); err != nil {
			return writeWrongType(ctx.W)
		}
		// The result is stored, so the AOF gets the write commands that
		// reproduce it (SORT itself is a read command).
		ctx.propagate([][]byte{[]byte("del"), storeDest})
		for _, chunk := range chunkElements(sorted, 64) {
			args := append([][]byte{[]byte("rpush"), storeDest}, chunk...)
			ctx.propagate(args)
		}
		return ctx.W.WriteInteger(int64(len(sorted)))
	}
	if err := ctx.W.WriteArrayHeader(len(sorted)); err != nil {
		return err
	}
	for _, e := range sorted {
		if err := ctx.W.WriteBulkString(e); err != nil {
			return err
		}
	}
	return nil
}

// lookupSortValue looks up the key a SORT pattern points at. The element
// replaces the "*" of the pattern. ok is false when the key does not exist.
func (ctx *Ctx) lookupSortValue(pattern string, element []byte) ([]byte, bool) {
	key := pattern
	if strings.Contains(pattern, "*") {
		key = strings.Replace(pattern, "*", string(element), 1)
	}
	v, ok, err := ctx.Store.GetString([]byte(key))
	if err != nil || !ok {
		return nil, false
	}
	return v, true
}

// compareSortValues compares two sort values. Without ALPHA the values are
// compared as numbers, and a value that is not a number counts as zero, like
// in Redis.
func compareSortValues(a, b []byte, alpha bool) int {
	if alpha {
		return strings.Compare(string(a), string(b))
	}
	na, aok := parseFloat(a)
	nb, bok := parseFloat(b)
	if !aok {
		na = 0
	}
	if !bok {
		nb = 0
	}
	switch {
	case na < nb:
		return -1
	case na > nb:
		return 1
	}
	return 0
}

// chunkElements splits the elements into chunks of at most n.
func chunkElements(elems [][]byte, n int) [][][]byte {
	if n <= 0 {
		return [][][]byte{elems}
	}
	var out [][][]byte
	for len(elems) > n {
		out = append(out, elems[:n])
		elems = elems[n:]
	}
	if len(elems) > 0 {
		out = append(out, elems)
	}
	return out
}
