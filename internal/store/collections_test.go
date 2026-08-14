package store

import (
	"reflect"
	"testing"
	"time"
)

func TestListPushPop(t *testing.T) {
	s := New()
	if n, err := s.ListPush([]byte("l"), true, []byte("a"), []byte("b")); err != nil || n != 2 {
		t.Errorf("LPUSH: got %d, %v", n, err)
	}
	if n, err := s.ListPush([]byte("l"), false, []byte("c")); err != nil || n != 3 {
		t.Errorf("RPUSH: got %d, %v", n, err)
	}
	// list is now [b a c]
	if v, ok, _ := s.ListPop([]byte("l"), true); !ok || string(v) != "b" {
		t.Errorf("LPOP: got %q, %v", v, ok)
	}
	if v, ok, _ := s.ListPop([]byte("l"), false); !ok || string(v) != "c" {
		t.Errorf("RPOP: got %q, %v", v, ok)
	}
	if v, ok, _ := s.ListPop([]byte("l"), false); !ok || string(v) != "a" {
		t.Errorf("RPOP: got %q, %v", v, ok)
	}
	if _, ok, _ := s.ListPop([]byte("l"), true); ok {
		t.Errorf("expected empty list")
	}
	if s.Exists([]byte("l")) != 0 {
		t.Errorf("expected key deleted when list emptied")
	}
}

func TestListRange(t *testing.T) {
	s := New()
	s.ListPush([]byte("l"), false, []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))
	got, err := s.ListRange([]byte("l"), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{[]byte("b"), []byte("c"), []byte("d")}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// negative indices
	got, _ = s.ListRange([]byte("l"), -2, -1)
	want = [][]byte{[]byte("d"), []byte("e")}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("negative: got %v, want %v", got, want)
	}
	// out of range
	if got, _ = s.ListRange([]byte("l"), 0, 100); len(got) != 5 {
		t.Errorf("clamp stop: got %d elements", len(got))
	}
	if got, _ = s.ListRange([]byte("l"), 10, 20); len(got) != 0 {
		t.Errorf("expected empty range")
	}
	if got, _ = s.ListRange([]byte("l"), 4, 2); len(got) != 0 {
		t.Errorf("expected empty reversed range")
	}
}

func TestListIndexSet(t *testing.T) {
	s := New()
	s.ListPush([]byte("l"), false, []byte("a"), []byte("b"), []byte("c"))
	if v, ok, _ := s.ListIndex([]byte("l"), 0); !ok || string(v) != "a" {
		t.Errorf("LINDEX 0: got %q, %v", v, ok)
	}
	if v, ok, _ := s.ListIndex([]byte("l"), -1); !ok || string(v) != "c" {
		t.Errorf("LINDEX -1: got %q, %v", v, ok)
	}
	if _, ok, _ := s.ListIndex([]byte("l"), 5); ok {
		t.Errorf("expected out of range")
	}
	if ok, _ := s.ListSet([]byte("l"), 1, []byte("X")); !ok {
		t.Errorf("LSET should succeed")
	}
	if v, _, _ := s.ListIndex([]byte("l"), 1); string(v) != "X" {
		t.Errorf("LSET: got %q", v)
	}
	if ok, _ := s.ListSet([]byte("l"), 10, []byte("Y")); ok {
		t.Errorf("LSET out of range should fail")
	}
}

func TestListLen(t *testing.T) {
	s := New()
	if n, ok, _ := s.ListLen([]byte("missing")); ok || n != 0 {
		t.Errorf("missing: got %d, %v", n, ok)
	}
	s.ListPush([]byte("l"), false, []byte("a"), []byte("b"))
	if n, ok, _ := s.ListLen([]byte("l")); !ok || n != 2 {
		t.Errorf("got %d, %v", n, ok)
	}
}

func TestHashSetGet(t *testing.T) {
	s := New()
	if added, _ := s.HashSet([]byte("h"), []byte("f1"), []byte("v1")); added != 1 {
		t.Errorf("new field: got %d, want 1", added)
	}
	if added, _ := s.HashSet([]byte("h"), []byte("f1"), []byte("v1x")); added != 0 {
		t.Errorf("existing field: got %d, want 0", added)
	}
	if v, ok, _ := s.HashGet([]byte("h"), []byte("f1")); !ok || string(v) != "v1x" {
		t.Errorf("HGET: got %q, %v", v, ok)
	}
	if _, ok, _ := s.HashGet([]byte("h"), []byte("nope")); ok {
		t.Errorf("expected missing field")
	}
	if n, ok, _ := s.HashLen([]byte("h")); !ok || n != 1 {
		t.Errorf("HLEN: got %d, %v", n, ok)
	}
}

func TestHashGetAllDel(t *testing.T) {
	s := New()
	s.HashSet([]byte("h"), []byte("a"), []byte("1"))
	s.HashSet([]byte("h"), []byte("b"), []byte("2"))
	pairs, err := s.HashGetAll([]byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 4 {
		t.Errorf("HGETALL: got %d elements, want 4", len(pairs))
	}
	// fields must include a and b, values 1 and 2
	m := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		m[string(pairs[i])] = string(pairs[i+1])
	}
	if m["a"] != "1" || m["b"] != "2" {
		t.Errorf("HGETALL contents: %v", m)
	}
	if removed, _ := s.HashDel([]byte("h"), []byte("a"), []byte("x")); removed != 1 {
		t.Errorf("HDEL: got %d, want 1", removed)
	}
	if exists, _ := s.HashExists([]byte("h"), []byte("a")); exists {
		t.Errorf("field a should be gone")
	}
	if keys, _ := s.HashKeys([]byte("h")); len(keys) != 1 || string(keys[0]) != "b" {
		t.Errorf("HKEYS: got %v", keys)
	}
	if vals, _ := s.HashVals([]byte("h")); len(vals) != 1 || string(vals[0]) != "2" {
		t.Errorf("HVALS: got %v", vals)
	}
	// deleting last field removes the key
	s.HashDel([]byte("h"), []byte("b"))
	if s.Exists([]byte("h")) != 0 {
		t.Errorf("expected key deleted when hash emptied")
	}
}

func TestSetAddRem(t *testing.T) {
	s := New()
	if added, _ := s.SetAdd([]byte("st"), []byte("a"), []byte("b"), []byte("a")); added != 2 {
		t.Errorf("SADD: got %d, want 2", added)
	}
	if n, ok, _ := s.SetCard([]byte("st")); !ok || n != 2 {
		t.Errorf("SCARD: got %d, %v", n, ok)
	}
	if is, _ := s.SetIsMember([]byte("st"), []byte("a")); !is {
		t.Errorf("a should be a member")
	}
	if is, _ := s.SetIsMember([]byte("st"), []byte("z")); is {
		t.Errorf("z should not be a member")
	}
	if removed, _ := s.SetRem([]byte("st"), []byte("a"), []byte("z")); removed != 1 {
		t.Errorf("SREM: got %d, want 1", removed)
	}
	members, _ := s.SetMembers([]byte("st"))
	if len(members) != 1 || string(members[0]) != "b" {
		t.Errorf("SMEMBERS: got %v", members)
	}
	s.SetRem([]byte("st"), []byte("b"))
	if s.Exists([]byte("st")) != 0 {
		t.Errorf("expected key deleted when set emptied")
	}
}

func TestWrongType(t *testing.T) {
	s := New()
	s.SetString([]byte("k"), []byte("v"))

	if _, _, err := s.GetString([]byte("k")); err != nil {
		t.Errorf("GetString on string should not error, got %v", err)
	}
	if _, err := s.ListPush([]byte("k"), true, []byte("x")); err != ErrWrongType {
		t.Errorf("ListPush on string: got %v, want ErrWrongType", err)
	}
	if _, _, err := s.ListLen([]byte("k")); err != ErrWrongType {
		t.Errorf("ListLen on string: got %v", err)
	}
	if _, err := s.HashSet([]byte("k"), []byte("f"), []byte("v")); err != ErrWrongType {
		t.Errorf("HashSet on string: got %v", err)
	}
	if _, err := s.SetAdd([]byte("k"), []byte("m")); err != ErrWrongType {
		t.Errorf("SetAdd on string: got %v", err)
	}
	if _, err := s.ZAdd([]byte("k"), 1, []byte("m")); err != ErrWrongType {
		t.Errorf("ZAdd on string: got %v", err)
	}

	// IncrBy on a non-numeric string is ErrNotInteger; on a non-string type it
	// is ErrWrongType.
	if _, err := s.IncrBy([]byte("k"), 1); err != ErrNotInteger {
		t.Errorf("IncrBy on string 'v': got %v, want ErrNotInteger", err)
	}
	s.ListPush([]byte("l"), true, []byte("x"))
	if _, err := s.IncrBy([]byte("l"), 1); err != ErrWrongType {
		t.Errorf("IncrBy on list: got %v, want ErrWrongType", err)
	}
}

func TestSetOverwritesOtherTypes(t *testing.T) {
	s := New()
	s.ListPush([]byte("k"), true, []byte("a"))
	s.SetString([]byte("k"), []byte("now-string"))
	v, ok, err := s.GetString([]byte("k"))
	if err != nil || !ok || string(v) != "now-string" {
		t.Errorf("got %q, %v, %v", v, ok, err)
	}
}

func TestExpireWorksOnAnyType(t *testing.T) {
	s := New()
	s.HashSet([]byte("h"), []byte("f"), []byte("v"))
	if !s.Expire([]byte("h"), time.Hour) {
		t.Errorf("Expire on hash should succeed")
	}
	if _, ok := s.TTL([]byte("h")); !ok {
		t.Errorf("expected TTL on hash")
	}
}
