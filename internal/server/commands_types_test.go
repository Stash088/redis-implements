package server_test

import (
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func TestListCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if got := c.intWant("LPUSH", "l", "b", "a"); got != 2 {
		t.Errorf("LPUSH: got %d", got)
	}
	if got := c.intWant("RPUSH", "l", "c"); got != 3 {
		t.Errorf("RPUSH: got %d", got)
	}
	// list: [a b c]
	if got := c.intWant("LLEN", "l"); got != 3 {
		t.Errorf("LLEN: got %d", got)
	}
	if got := c.bulkWant("LPOP", "l"); string(got) != "a" {
		t.Errorf("LPOP: got %q", got)
	}
	if got := c.bulkWant("RPOP", "l"); string(got) != "c" {
		t.Errorf("RPOP: got %q", got)
	}
	if got := c.bulkWant("LINDEX", "l", "0"); string(got) != "b" {
		t.Errorf("LINDEX: got %q", got)
	}
	got := c.arrWant("LRANGE", "l", "0", "-1")
	if len(got) != 1 || string(got[0].Str) != "b" {
		t.Errorf("LRANGE: got %v", got)
	}
	c.ok("LSET", "l", "0", "z")
	if got := c.bulkWant("LINDEX", "l", "0"); string(got) != "z" {
		t.Errorf("LINDEX after LSET: got %q", got)
	}
}

func TestListEmptyBecomesNil(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("LPUSH", "l", "a")
	if v := c.cmd("LPOP", "l"); v.IsNil() {
		t.Errorf("LPOP on last elem should return value")
	}
	if v := c.cmd("LPOP", "l"); !v.IsNil() {
		t.Errorf("LPOP on empty should return null")
	}
	if v := c.cmd("TYPE", "l"); v.Kind != resp.KindSimpleString || string(v.Str) != "none" {
		t.Errorf("TYPE after list emptied: got %+v, want none", v)
	}
}

func TestListWrongType(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.ok("SET", "k", "v")
	err := c.expectError("LPUSH", "k", "x")
	if !strings.Contains(err, "WRONGTYPE") {
		t.Errorf("got %q, want WRONGTYPE", err)
	}
}

func TestHashCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if got := c.intWant("HSET", "h", "f1", "v1"); got != 1 {
		t.Errorf("HSET new: got %d", got)
	}
	if got := c.intWant("HSET", "h", "f1", "v1b"); got != 0 {
		t.Errorf("HSET existing: got %d", got)
	}
	if got := c.intWant("HLEN", "h"); got != 1 {
		t.Errorf("HLEN: got %d", got)
	}
	if got := c.bulkWant("HGET", "h", "f1"); string(got) != "v1b" {
		t.Errorf("HGET: got %q", got)
	}
	if got := c.intWant("HEXISTS", "h", "f1"); got != 1 {
		t.Errorf("HEXISTS: got %d", got)
	}
	c.intWant("HSET", "h", "f2", "v2")
	pairs := c.arrWant("HGETALL", "h")
	if len(pairs) != 4 {
		t.Fatalf("HGETALL: got %d elements", len(pairs))
	}
	keys := c.arrWant("HKEYS", "h")
	if len(keys) != 2 {
		t.Errorf("HKEYS: got %v", keys)
	}
	vals := c.arrWant("HVALS", "h")
	if len(vals) != 2 {
		t.Errorf("HVALS: got %v", vals)
	}
	if got := c.intWant("HDEL", "h", "f1"); got != 1 {
		t.Errorf("HDEL: got %d", got)
	}
}

func TestHashGetAllEmpty(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if arr := c.arrWant("HGETALL", "missing"); len(arr) != 0 {
		t.Errorf("HGETALL missing: got %v", arr)
	}
}

func TestSetCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if got := c.intWant("SADD", "st", "a", "b", "a"); got != 2 {
		t.Errorf("SADD: got %d", got)
	}
	if got := c.intWant("SCARD", "st"); got != 2 {
		t.Errorf("SCARD: got %d", got)
	}
	if got := c.intWant("SISMEMBER", "st", "a"); got != 1 {
		t.Errorf("SISMEMBER present: got %d", got)
	}
	if got := c.intWant("SISMEMBER", "st", "z"); got != 0 {
		t.Errorf("SISMEMBER absent: got %d", got)
	}
	members := c.arrWant("SMEMBERS", "st")
	if len(members) != 2 {
		t.Errorf("SMEMBERS: got %v", members)
	}
	if got := c.intWant("SREM", "st", "a"); got != 1 {
		t.Errorf("SREM: got %d", got)
	}
}

func TestZSetCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	if got := c.intWant("ZADD", "z", "1", "a", "2", "b", "3", "c"); got != 3 {
		t.Errorf("ZADD: got %d", got)
	}
	if got := c.intWant("ZCARD", "z"); got != 3 {
		t.Errorf("ZCARD: got %d", got)
	}
	if got := c.intWant("ZADD", "z", "1.5", "a"); got != 0 {
		t.Errorf("ZADD update: got %d", got)
	}
	if got := c.bulkWant("ZSCORE", "z", "a"); string(got) != "1.5" {
		t.Errorf("ZSCORE: got %q", got)
	}
	if v := c.cmd("ZSCORE", "z", "zz"); !v.IsNil() {
		t.Errorf("ZSCORE missing: got %v", v)
	}

	got := c.arrWant("ZRANGE", "z", "0", "-1")
	wantOrder := []string{"a", "b", "c"}
	if len(got) != 3 {
		t.Fatalf("ZRANGE: got %v", got)
	}
	for i, w := range wantOrder {
		if string(got[i].Str) != w {
			t.Errorf("ZRANGE pos %d: got %q, want %q", i, got[i].Str, w)
		}
	}
	// withscores -> flat member,score list
	withScores := c.arrWant("ZRANGE", "z", "0", "-1", "WITHSCORES")
	if len(withScores) != 6 {
		t.Fatalf("ZRANGE WITHSCORES: got %d elements", len(withScores))
	}
	if string(withScores[0].Str) != "a" || string(withScores[1].Str) != "1.5" {
		t.Errorf("WITHSCORES first pair: %q %q", withScores[0].Str, withScores[1].Str)
	}
	// reverse
	rev := c.arrWant("ZREVRANGE", "z", "0", "0")
	if len(rev) != 1 || string(rev[0].Str) != "c" {
		t.Errorf("ZREVRANGE: got %v", rev)
	}
	// ranks
	if got := c.intWant("ZRANK", "z", "a"); got != 0 {
		t.Errorf("ZRANK a: got %d", got)
	}
	if got := c.intWant("ZREVRANK", "z", "a"); got != 2 {
		t.Errorf("ZREVRANK a: got %d", got)
	}
	if v := c.cmd("ZRANK", "z", "zz"); !v.IsNil() {
		t.Errorf("ZRANK missing: got %v", v)
	}
	// remove
	if got := c.intWant("ZREM", "z", "a", "zz"); got != 1 {
		t.Errorf("ZREM: got %d", got)
	}
	if got := c.intWant("ZCARD", "z"); got != 2 {
		t.Errorf("ZCARD after ZREM: got %d", got)
	}
}

func TestWrongTypeAcrossCommands(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a")

	cases := [][]string{
		{"GET", "l"},
		{"APPEND", "l", "x"},
		{"STRLEN", "l"},
		{"INCR", "l"},
		{"HSET", "l", "f", "v"},
		{"SADD", "l", "m"},
		{"ZADD", "l", "1", "m"},
		{"LPUSH", "l", "x"}, // list on list is fine, not an error
	}
	for _, args := range cases {
		v := c.cmd(args...)
		if args[0] == "LPUSH" {
			if v.Kind != resp.KindInteger {
				t.Errorf("%v: got %+v, want integer", args, v)
			}
			continue
		}
		if v.Kind != resp.KindError || !strings.Contains(string(v.Str), "WRONGTYPE") {
			t.Errorf("%v: got %+v, want WRONGTYPE", args, v)
		}
	}
}

func TestTypeCommand(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)

	typeCase := func(key, want string) {
		v := c.cmd("TYPE", key)
		if v.Kind != resp.KindSimpleString || string(v.Str) != want {
			t.Errorf("TYPE %s: got %+v, want %s", key, v, want)
		}
	}
	typeCase("missing", "none")
	c.ok("SET", "s", "v")
	typeCase("s", "string")
	c.intWant("RPUSH", "l", "a")
	typeCase("l", "list")
	c.intWant("HSET", "h", "f", "v")
	typeCase("h", "hash")
	c.intWant("SADD", "st", "m")
	typeCase("st", "set")
	c.intWant("ZADD", "z", "1", "m")
	typeCase("z", "zset")
}

func TestMultiTypeExpireAndDelete(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	c.intWant("RPUSH", "l", "a", "b")
	c.intWant("HSET", "h", "f", "v")
	c.intWant("SADD", "st", "m")
	c.intWant("ZADD", "z", "1", "m")

	if got := c.intWant("EXPIRE", "l", "100"); got != 1 {
		t.Errorf("EXPIRE list: got %d", got)
	}
	if got := c.intWant("TTL", "l"); got <= 0 {
		t.Errorf("TTL list: got %d", got)
	}
	if got := c.intWant("DEL", "h", "st", "z"); got != 3 {
		t.Errorf("DEL: got %d", got)
	}
	if got := c.intWant("DBSIZE"); got != 1 {
		t.Errorf("DBSIZE: got %d", got)
	}
}
