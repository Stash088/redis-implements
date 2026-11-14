package server_test

import (
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

// wrongTypeErr is the WRONGTYPE error the write commands reply with.
const wrongTypeErr = "WRONGTYPE Operation against a key holding the wrong kind of value"

// setBitmap builds a bitmap value with SETBIT for the given bit positions.
func setBitmap(t *testing.T, c *client, key string, bits ...int) {
	t.Helper()
	for _, b := range bits {
		c.intWant("SETBIT", key, itoa(b), "1")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func TestSetBitGetBit(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if n := c.intWant("SETBIT", "bm", "0", "1"); n != 0 {
		t.Errorf("SETBIT fresh: got %d, want 0", n)
	}
	if n := c.intWant("SETBIT", "bm", "999", "1"); n != 0 {
		t.Errorf("SETBIT far: got %d, want 0", n)
	}
	if n := c.intWant("STRLEN", "bm"); n != 125 {
		t.Errorf("STRLEN bm: got %d, want 125", n)
	}
	if n := c.intWant("GETBIT", "bm", "0"); n != 1 {
		t.Errorf("GETBIT 0: got %d, want 1", n)
	}
	if n := c.intWant("GETBIT", "bm", "998"); n != 0 {
		t.Errorf("GETBIT 998 (padding): got %d, want 0", n)
	}
	if n := c.intWant("GETBIT", "bm", "5000"); n != 0 {
		t.Errorf("GETBIT past the end: got %d, want 0", n)
	}
	// Overwriting returns the old bit.
	if n := c.intWant("SETBIT", "bm", "0", "0"); n != 1 {
		t.Errorf("SETBIT overwrite: got %d, want 1", n)
	}
	// A missing key reads as zero.
	if n := c.intWant("GETBIT", "nosuch", "5"); n != 0 {
		t.Errorf("GETBIT missing: got %d, want 0", n)
	}

	// Errors.
	if got := c.expectError("SETBIT", "bm", "0", "2"); got != "ERR bit is not an integer or out of range" {
		t.Errorf("SETBIT value 2: got %q", got)
	}
	if got := c.expectError("SETBIT", "bm", "-1", "1"); got != "ERR bit offset is not an integer or out of range" {
		t.Errorf("SETBIT offset -1: got %q", got)
	}
	if got := c.expectError("SETBIT", "bm", "abc", "1"); got != "ERR bit offset is not an integer or out of range" {
		t.Errorf("SETBIT offset abc: got %q", got)
	}
	if got := c.expectError("SETBIT", "bm", "0", "01"); got != "ERR bit is not an integer or out of range" {
		t.Errorf("SETBIT value 01: got %q", got)
	}
	if got := c.expectError("GETBIT", "bm", "-5"); got != "ERR bit offset is not an integer or out of range" {
		t.Errorf("GETBIT offset -5: got %q", got)
	}
	// A key of another type.
	c.intWant("RPUSH", "l", "x")
	if got := c.expectError("SETBIT", "l", "0", "1"); got != wrongTypeErr {
		t.Errorf("SETBIT on a list: got %q", got)
	}
	if got := c.expectError("GETBIT", "l", "0"); got != wrongTypeErr {
		t.Errorf("GETBIT on a list: got %q", got)
	}
	// Arity.
	if got := c.expectError("SETBIT", "bm", "0"); got != "ERR wrong number of arguments for 'setbit' command" {
		t.Errorf("SETBIT arity: got %q", got)
	}
}

func TestBitCountCommand(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "foobar")

	if n := c.intWant("BITCOUNT", "k"); n != 26 {
		t.Errorf("BITCOUNT k: got %d, want 26", n)
	}
	if n := c.intWant("BITCOUNT", "k", "1", "1"); n != 6 {
		t.Errorf("BITCOUNT k 1 1: got %d, want 6", n)
	}
	if n := c.intWant("BITCOUNT", "k", "1", "-1"); n != 22 {
		t.Errorf("BITCOUNT k 1 -1: got %d, want 22", n)
	}
	if n := c.intWant("BITCOUNT", "k", "8", "15", "BIT"); n != 6 {
		t.Errorf("BITCOUNT k 8 15 BIT: got %d, want 6", n)
	}
	if n := c.intWant("BITCOUNT", "k", "10", "13", "bit"); n != 3 {
		t.Errorf("BITCOUNT k 10 13 bit: got %d, want 3", n)
	}
	if n := c.intWant("BITCOUNT", "nosuch"); n != 0 {
		t.Errorf("BITCOUNT missing: got %d, want 0", n)
	}
	if got := c.expectError("BITCOUNT", "k", "0"); got != "ERR syntax error" {
		t.Errorf("BITCOUNT k 0: got %q", got)
	}
	if got := c.expectError("BITCOUNT", "k", "0", "1", "NIBBLE"); got != "ERR syntax error" {
		t.Errorf("BITCOUNT bad unit: got %q", got)
	}
	if got := c.expectError("BITCOUNT", "k", "a", "1"); got != "ERR value is not an integer or out of range" {
		t.Errorf("BITCOUNT bad start: got %q", got)
	}
	c.intWant("RPUSH", "l", "x")
	if got := c.expectError("BITCOUNT", "l"); got != wrongTypeErr {
		t.Errorf("BITCOUNT on a list: got %q", got)
	}
}

func TestBitPosCommand(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "ff", "\xff\xf0")
	c.ok("SET", "mixed", "\x00\xff\xf0")
	c.ok("SET", "allones", "\xff\xff\xff")

	if n := c.intWant("BITPOS", "ff", "1"); n != 0 {
		t.Errorf("BITPOS ff 1: got %d, want 0", n)
	}
	if n := c.intWant("BITPOS", "ff", "0"); n != 12 {
		t.Errorf("BITPOS ff 0: got %d, want 12", n)
	}
	if n := c.intWant("BITPOS", "mixed", "1"); n != 8 {
		t.Errorf("BITPOS mixed 1: got %d, want 8", n)
	}
	if n := c.intWant("BITPOS", "mixed", "0"); n != 0 {
		t.Errorf("BITPOS mixed 0: got %d, want 0", n)
	}
	// The padding quirk of all-ones values.
	if n := c.intWant("BITPOS", "allones", "0"); n != 24 {
		t.Errorf("BITPOS allones 0: got %d, want 24", n)
	}
	if n := c.intWant("BITPOS", "allones", "0", "0", "2"); n != -1 {
		t.Errorf("BITPOS allones 0 0 2: got %d, want -1", n)
	}
	if n := c.intWant("BITPOS", "allones", "0", "1"); n != 24 {
		t.Errorf("BITPOS allones 0 1: got %d, want 24", n)
	}
	// A missing key.
	if n := c.intWant("BITPOS", "nosuch", "0"); n != 0 {
		t.Errorf("BITPOS missing 0: got %d, want 0", n)
	}
	if n := c.intWant("BITPOS", "nosuch", "1"); n != -1 {
		t.Errorf("BITPOS missing 1: got %d, want -1", n)
	}
	// Errors.
	if got := c.expectError("BITPOS", "ff", "5"); got != "ERR The bit argument must be 1 or 0." {
		t.Errorf("BITPOS bit 5: got %q", got)
	}
	if got := c.expectError("BITPOS", "ff", "x"); got != "ERR value is not an integer or out of range" {
		t.Errorf("BITPOS bit x: got %q", got)
	}
	if got := c.expectError("BITPOS", "ff", "0", "0", "1", "NIBBLE"); got != "ERR syntax error" {
		t.Errorf("BITPOS bad unit: got %q", got)
	}
	c.intWant("RPUSH", "l", "x")
	if got := c.expectError("BITPOS", "l", "0"); got != wrongTypeErr {
		t.Errorf("BITPOS on a list: got %q", got)
	}
}

func TestBitOpCommand(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k1", "foobar")
	c.ok("SET", "k2", "abcdef")

	if n := c.intWant("BITOP", "AND", "dest1", "k1", "k2"); n != 6 {
		t.Errorf("BITOP AND: got %d, want 6", n)
	}
	if got := string(c.bulkWant("GET", "dest1")); got != "`bc`ab" {
		t.Errorf("GET dest1: got %q", got)
	}
	if n := c.intWant("BITOP", "OR", "dest2", "k1", "k2"); n != 6 {
		t.Errorf("BITOP OR: got %d, want 6", n)
	}
	if got := string(c.bulkWant("GET", "dest2")); got != "goofev" {
		t.Errorf("GET dest2: got %q", got)
	}
	if n := c.intWant("BITOP", "XOR", "dest3", "k1", "k2"); n != 6 {
		t.Errorf("BITOP XOR: got %d, want 6", n)
	}
	if n := c.intWant("BITOP", "NOT", "dest4", "k1"); n != 6 {
		t.Errorf("BITOP NOT: got %d, want 6", n)
	}
	if got := string(c.bulkWant("GET", "dest4")); got != "\x99\x90\x90\x9d\x9e\x8d" {
		t.Errorf("GET dest4: got %q", got)
	}

	// A missing source counts as an empty value; the reply is the longest.
	if n := c.intWant("BITOP", "XOR", "dest5", "k1", "nosuch"); n != 6 {
		t.Errorf("BITOP XOR with a missing source: got %d, want 6", n)
	}
	// All sources missing: the destination is removed.
	if n := c.intWant("BITOP", "OR", "dest6", "nosuch", "alsonot"); n != 0 {
		t.Errorf("BITOP over missing keys: got %d, want 0", n)
	}
	if n := c.intWant("EXISTS", "dest6"); n != 0 {
		t.Errorf("EXISTS dest6 after BITOP: got %d, want 0", n)
	}

	// The Redis 8.2 operations.
	setBitmap(t, c, "X", 0, 5)
	setBitmap(t, c, "A", 0, 1)
	setBitmap(t, c, "B", 1, 2)
	if n := c.intWant("BITOP", "DIFF", "d", "X", "A", "B"); n != 1 {
		t.Errorf("BITOP DIFF: got %d, want 1", n)
	}
	if n := c.intWant("GETBIT", "d", "5"); n != 1 {
		t.Errorf("DIFF bit 5: got %d, want 1", n)
	}
	if n := c.intWant("BITOP", "DIFF1", "d", "X", "A", "B"); n != 1 {
		t.Errorf("BITOP DIFF1: got %d, want 1", n)
	}
	if n := c.intWant("GETBIT", "d", "1"); n != 1 {
		t.Errorf("DIFF1 bit 1: got %d, want 1", n)
	}
	if n := c.intWant("BITOP", "ANDOR", "d", "X", "A", "B"); n != 1 {
		t.Errorf("BITOP ANDOR: got %d, want 1", n)
	}
	if n := c.intWant("GETBIT", "d", "0"); n != 1 {
		t.Errorf("ANDOR bit 0: got %d, want 1", n)
	}
	if n := c.intWant("BITOP", "ONE", "d", "A", "B"); n != 1 {
		t.Errorf("BITOP ONE: got %d, want 1", n)
	}
	if n := c.intWant("GETBIT", "d", "2"); n != 1 {
		t.Errorf("ONE bit 2: got %d, want 1", n)
	}
	if n := c.intWant("GETBIT", "d", "1"); n != 0 {
		t.Errorf("ONE bit 1 (common): got %d, want 0", n)
	}

	// Errors.
	if got := c.expectError("BITOP", "NOT", "d", "a", "b"); got != "ERR BITOP NOT must be called with a single source key." {
		t.Errorf("BITOP NOT with two sources: got %q", got)
	}
	if got := c.expectError("BITOP", "DIFF", "d", "a"); got != "ERR BITOP DIFF must be called with at least two source keys." {
		t.Errorf("BITOP DIFF with one source: got %q", got)
	}
	if got := c.expectError("BITOP", "NAND", "d", "a", "b"); got != "ERR syntax error" {
		t.Errorf("BITOP NAND: got %q", got)
	}
	c.intWant("RPUSH", "l", "x")
	if got := c.expectError("BITOP", "OR", "d", "l", "k1"); got != wrongTypeErr {
		t.Errorf("BITOP with a list source: got %q", got)
	}
}

func TestBitFieldCommand(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	// The doc example: SET returns the old value, INCRBY wraps.
	arr := c.arrWant("BITFIELD", "bits", "SET", "u4", "0", "3", "GET", "u4", "0")
	if len(arr) != 2 || arr[0].Int != 0 || arr[1].Int != 3 {
		t.Errorf("BITFIELD SET/GET: got %+v", arr)
	}
	arr = c.arrWant("BITFIELD", "bits", "SET", "u4", "0", "15", "INCRBY", "u4", "0", "1")
	if len(arr) != 2 || arr[0].Int != 3 || arr[1].Int != 0 {
		t.Errorf("BITFIELD wrap: got %+v", arr)
	}
	// OVERFLOW SAT on a fresh key: 0+15 fits, 15+1 saturates at 15.
	arr = c.arrWant("BITFIELD", "satk", "OVERFLOW", "SAT", "INCRBY", "u4", "0", "15", "INCRBY", "u4", "0", "1")
	if len(arr) != 2 || arr[0].Int != 15 || arr[1].Int != 15 {
		t.Errorf("BITFIELD sat: got %+v", arr)
	}
	// OVERFLOW FAIL replies null for the discarded operation (RESP2 null bulk).
	arr = c.arrWant("BITFIELD", "bits", "SET", "u4", "0", "15", "OVERFLOW", "FAIL", "INCRBY", "u4", "0", "1")
	if len(arr) != 2 || !arr[1].IsNull {
		t.Errorf("BITFIELD fail: got %+v", arr)
	}
	// The "#n" offsets and a signed type.
	arr = c.arrWant("BITFIELD", "bits", "SET", "i8", "#1", "100", "GET", "i8", "#1", "GET", "u8", "#1")
	if len(arr) != 3 || arr[0].Int != 0 || arr[1].Int != 100 || arr[2].Int != 100 {
		t.Errorf("BITFIELD #1: got %+v", arr)
	}
	// A GET on a missing key reads zero.
	arr = c.arrWant("BITFIELD", "nosuch", "GET", "u8", "0")
	if len(arr) != 1 || arr[0].Int != 0 {
		t.Errorf("BITFIELD GET on a missing key: got %+v", arr)
	}
	// An empty operation list yields an empty array.
	arr = c.arrWant("BITFIELD", "bits")
	if len(arr) != 0 {
		t.Errorf("BITFIELD with no subcommands: got %+v", arr)
	}

	// Errors.
	if got := c.expectError("BITFIELD", "bits", "SET", "u64", "0", "1"); got != "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is." {
		t.Errorf("BITFIELD u64: got %q", got)
	}
	if got := c.expectError("BITFIELD", "bits", "WHAT", "u8", "0"); got != "ERR syntax error" {
		t.Errorf("BITFIELD unknown subcommand: got %q", got)
	}
	if got := c.expectError("BITFIELD", "bits", "OVERFLOW", "BOOM"); got != "ERR Invalid OVERFLOW type specified" {
		t.Errorf("BITFIELD bad overflow: got %q", got)
	}
	if got := c.expectError("BITFIELD", "bits", "GET", "u8"); got != "ERR syntax error" {
		t.Errorf("BITFIELD GET without offset: got %q", got)
	}
	if got := c.expectError("BITFIELD", "bits", "INCRBY", "u8", "0", "x"); got != "ERR value is not an integer or out of range" {
		t.Errorf("BITFIELD bad increment: got %q", got)
	}

	// BITFIELD_RO accepts GET only.
	arr = c.arrWant("BITFIELD_RO", "bits", "GET", "u4", "#0")
	if len(arr) != 1 || arr[0].Int != 15 {
		t.Errorf("BITFIELD_RO GET: got %+v", arr)
	}
	if got := c.expectError("BITFIELD_RO", "bits", "SET", "u4", "0", "1"); got != "ERR BITFIELD_RO only supports the GET subcommand" {
		t.Errorf("BITFIELD_RO SET: got %q", got)
	}
	// A parse error takes precedence over the read-only rejection.
	if got := c.expectError("BITFIELD_RO", "bits", "SET", "q8", "0", "1"); got != "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is." {
		t.Errorf("BITFIELD_RO bad type: got %q", got)
	}

	// A key of another type.
	c.intWant("RPUSH", "l", "x")
	if got := c.expectError("BITFIELD", "l", "GET", "u8", "0"); got != wrongTypeErr {
		t.Errorf("BITFIELD on a list: got %q", got)
	}
	if got := c.expectError("BITFIELD", "l", "SET", "u8", "0", "1"); got != wrongTypeErr {
		t.Errorf("BITFIELD SET on a list: got %q", got)
	}
}

func TestBitFieldFailResp3(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	// Negotiate RESP3 and check that a discarded OVERFLOW FAIL element is
	// the dedicated null type, not a null bulk string.
	v := c.cmd("HELLO", "3")
	if v.Kind != resp.KindMap {
		t.Fatalf("HELLO 3: got %+v, want a map", v)
	}
	c.intWant("SETBIT", "bf3", "0", "1") // u4 #0 = 8
	arr := c.arrWant("BITFIELD", "bf3", "SET", "u4", "0", "8", "OVERFLOW", "FAIL", "INCRBY", "u4", "0", "9")
	if len(arr) != 2 {
		t.Fatalf("BITFIELD fail resp3: got %+v", arr)
	}
	if arr[0].Int != 8 {
		t.Errorf("BITFIELD fail resp3 old: got %d, want 8", arr[0].Int)
	}
	if arr[1].Kind != resp.KindNull || !arr[1].IsNull {
		t.Errorf("BITFIELD fail resp3 null: got %+v, want the RESP3 null", arr[1])
	}
}

func TestBitmapAofReplay(t *testing.T) {
	dir := t.TempDir()
	addr, _, aofPath := startPersistServer(t, dir, true)
	c := newClient(t, addr)

	// Bitmap writes of every kind, then a restart from the AOF.
	c.intWant("SETBIT", "bm", "0", "1")
	c.intWant("SETBIT", "bm", "9", "1")
	c.ok("SET", "s1", "foobar")
	c.ok("SET", "s2", "abcdef")
	c.intWant("BITOP", "XOR", "xored", "s1", "s2")
	c.intWant("BITOP", "DIFF", "diffed", "s1", "s2")
	c.arrWant("BITFIELD", "fielded", "SET", "i8", "#1", "-13", "INCRBY", "u4", "0", "5")
	wantXored := string(c.bulkWant("GET", "xored"))
	wantDiffed := string(c.bulkWant("GET", "diffed"))
	wantFielded := string(c.bulkWant("GETRANGE", "fielded", "0", "-1"))

	c2 := newClient(t, startFromFiles(t, aofPath, true))
	if got := string(c2.bulkWant("GET", "bm")); got != "\x80\x40" {
		t.Errorf("bm after the replay: got %q, want \\x80\\x40", got)
	}
	if got := string(c2.bulkWant("GET", "xored")); got != wantXored {
		t.Errorf("xored after the replay: got %q, want %q", got, wantXored)
	}
	if got := string(c2.bulkWant("GET", "diffed")); got != wantDiffed {
		t.Errorf("diffed after the replay: got %q, want %q", got, wantDiffed)
	}
	if got := string(c2.bulkWant("GETRANGE", "fielded", "0", "-1")); got != wantFielded {
		t.Errorf("fielded after the replay: got %q, want %q", got, wantFielded)
	}
	if n := c2.intWant("GETBIT", "bm", "9"); n != 1 {
		t.Errorf("GETBIT 9 after the replay: got %d, want 1", n)
	}
	if n := c2.intWant("BITCOUNT", "bm"); n != 2 {
		t.Errorf("BITCOUNT bm after the replay: got %d, want 2", n)
	}
}

func TestBitmapCommandCount(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	// 185 commands of the previous milestones (replication included) + the
	// 7 bitmap ones.
	if n := c.intWant("COMMAND", "COUNT"); n != 192 {
		t.Errorf("COMMAND COUNT: got %d, want 192", n)
	}
}
