package store

import (
	"bytes"
	"math"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.SetString([]byte("foo"), []byte("bar"))
	v, ok, err := s.GetString([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(v, []byte("bar")) {
		t.Errorf("got %q, %v; want bar, true", v, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	if _, ok, err := s.GetString([]byte("nope")); ok || err != nil {
		t.Errorf("expected missing key, got ok=%v err=%v", ok, err)
	}
}

func TestSetOverwrite(t *testing.T) {
	s := New()
	s.SetString([]byte("k"), []byte("v1"))
	s.SetString([]byte("k"), []byte("v2"))
	v, _, _ := s.GetString([]byte("k"))
	if string(v) != "v2" {
		t.Errorf("got %q, want v2", v)
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.SetString([]byte("a"), []byte("1"))
	s.SetString([]byte("b"), []byte("2"))
	s.SetString([]byte("c"), []byte("3"))
	if n := s.Delete([]byte("a"), []byte("b"), []byte("missing")); n != 2 {
		t.Errorf("got %d removed, want 2", n)
	}
	if s.Len() != 1 {
		t.Errorf("got len %d, want 1", s.Len())
	}
}

func TestExists(t *testing.T) {
	s := New()
	s.SetString([]byte("a"), []byte("1"))
	if n := s.Exists([]byte("a"), []byte("b")); n != 1 {
		t.Errorf("got %d, want 1", n)
	}
}

func TestIncrBy(t *testing.T) {
	s := New()
	n, err := s.IncrBy([]byte("counter"), 5)
	if err != nil || n != 5 {
		t.Errorf("got %d, %v; want 5, nil", n, err)
	}
	n, err = s.IncrBy([]byte("counter"), -3)
	if err != nil || n != 2 {
		t.Errorf("got %d, %v; want 2, nil", n, err)
	}
}

func TestIncrByNotInteger(t *testing.T) {
	s := New()
	s.SetString([]byte("k"), []byte("hello"))
	if _, err := s.IncrBy([]byte("k"), 1); err != ErrNotInteger {
		t.Errorf("got %v, want ErrNotInteger", err)
	}
}

func TestIncrByOverflow(t *testing.T) {
	s := New()
	if _, err := s.IncrBy([]byte("k"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IncrBy([]byte("k"), math.MaxInt64); err != ErrOverflow {
		t.Errorf("got %v, want ErrOverflow on overflow", err)
	}
	if _, err := s.IncrBy([]byte("neg"), -1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IncrBy([]byte("neg"), math.MinInt64); err != ErrOverflow {
		t.Errorf("got %v, want ErrOverflow on underflow", err)
	}
}

func TestAppend(t *testing.T) {
	s := New()
	if n, err := s.Append([]byte("k"), []byte("foo")); err != nil || n != 3 {
		t.Errorf("got %d, %v; want 3, nil", n, err)
	}
	if n, err := s.Append([]byte("k"), []byte("bar")); err != nil || n != 6 {
		t.Errorf("got %d, %v; want 6, nil", n, err)
	}
	v, _, _ := s.GetString([]byte("k"))
	if string(v) != "foobar" {
		t.Errorf("got %q, want foobar", v)
	}
}

func TestStrLen(t *testing.T) {
	s := New()
	if _, ok, _ := s.StrLen([]byte("missing")); ok {
		t.Errorf("expected missing")
	}
	s.SetString([]byte("k"), []byte("hello"))
	n, ok, err := s.StrLen([]byte("k"))
	if err != nil || !ok || n != 5 {
		t.Errorf("got %d, %v, %v; want 5, true, nil", n, ok, err)
	}
}

func TestKeysAndFlush(t *testing.T) {
	s := New()
	s.SetString([]byte("a"), []byte("1"))
	s.SetString([]byte("b"), []byte("2"))
	if n := len(s.Keys()); n != 2 {
		t.Errorf("got %d keys, want 2", n)
	}
	if n := s.Flush(); n != 2 {
		t.Errorf("got %d flushed, want 2", n)
	}
	if s.Len() != 0 {
		t.Errorf("expected empty store")
	}
}

func TestTTL(t *testing.T) {
	s := New()
	s.SetStringWithTTL([]byte("k"), []byte("v"), 10*time.Second)
	d, ok := s.TTL([]byte("k"))
	if !ok || d <= 0 || d > 10*time.Second {
		t.Errorf("got %v, %v; want (0,10s], true", d, ok)
	}
	s.Persist([]byte("k"))
	d, _ = s.TTL([]byte("k"))
	if d != -1 {
		t.Errorf("got %v, want -1 for persistent key", d)
	}
}

func TestTTLMissing(t *testing.T) {
	s := New()
	if _, ok := s.TTL([]byte("missing")); ok {
		t.Errorf("expected missing key")
	}
}

func TestExpirePersist(t *testing.T) {
	s := New()
	s.SetString([]byte("k"), []byte("v"))
	if !s.Expire([]byte("k"), time.Hour) {
		t.Errorf("expected Expire to succeed")
	}
	if _, ok := s.TTL([]byte("k")); !ok {
		t.Errorf("expected TTL present")
	}
	if !s.Persist([]byte("k")) {
		t.Errorf("expected Persist to succeed")
	}
	if d, _ := s.TTL([]byte("k")); d != -1 {
		t.Errorf("got %v, want -1", d)
	}
}

func TestExpireMissing(t *testing.T) {
	s := New()
	if s.Expire([]byte("missing"), time.Hour) {
		t.Errorf("expected Expire to fail on missing key")
	}
}

func TestLazyExpiry(t *testing.T) {
	s := New()
	frozen := time.Now()
	s.SetClock(func() time.Time { return frozen })
	s.SetStringWithTTL([]byte("k"), []byte("v"), time.Second)

	frozen = frozen.Add(2 * time.Second)
	if _, ok, _ := s.GetString([]byte("k")); ok {
		t.Errorf("expected key to be expired")
	}
	if s.Exists([]byte("k")) != 0 {
		t.Errorf("expected expired key gone")
	}
}

func TestLazyExpiryRemovesOnGet(t *testing.T) {
	s := New()
	frozen := time.Now()
	s.SetClock(func() time.Time { return frozen })
	s.SetStringWithTTL([]byte("k"), []byte("v"), time.Second)
	frozen = frozen.Add(2 * time.Second)
	s.GetString([]byte("k"))
	if s.Len() != 0 {
		t.Errorf("expected key physically removed")
	}
}

func TestActiveExpiry(t *testing.T) {
	s := New()
	frozen := time.Now()
	s.SetClock(func() time.Time { return frozen })
	s.SetStringWithTTL([]byte("expiring"), []byte("v"), time.Second)
	s.SetString([]byte("stays"), []byte("v"))
	frozen = frozen.Add(2 * time.Second)
	if n := s.ExpireCycle(); n != 1 {
		t.Errorf("got %d removed, want 1", n)
	}
	if s.Exists([]byte("stays")) != 1 {
		t.Errorf("expected persistent key to remain")
	}
}

func TestIncrKeepsTTL(t *testing.T) {
	s := New()
	s.SetStringWithTTL([]byte("k"), []byte("1"), time.Hour)
	s.IncrBy([]byte("k"), 1)
	d, ok := s.TTL([]byte("k"))
	if !ok || d <= 0 {
		t.Errorf("INCR should preserve TTL, got %v, %v", d, ok)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte{byte('a' + id)}
			for i := 0; i < 1000; i++ {
				s.SetString(key, []byte("v"))
				s.GetString(key)
				s.IncrBy([]byte("counter"), 1)
			}
		}(w)
	}
	wg.Wait()
	n, err := s.IncrBy([]byte("counter"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 8000 {
		t.Errorf("got %d, want 8000", n)
	}
}

func TestConcurrentExpiry(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				s.SetStringWithTTL([]byte("k"), []byte("v"), time.Millisecond)
				s.ExpireCycle()
				s.GetString([]byte("k"))
			}
		}()
	}
	wg.Wait()
}

func TestGetType(t *testing.T) {
	s := New()
	s.SetString([]byte("s"), []byte("v"))
	s.ListPush([]byte("l"), true, []byte("a"))
	s.HashSet([]byte("h"), []byte("f"), []byte("v"))
	s.SetAdd([]byte("st"), []byte("m"))
	s.ZAdd([]byte("z"), 1, []byte("m"))

	cases := []struct {
		key  string
		want Type
	}{
		{"s", TypeString},
		{"l", TypeList},
		{"h", TypeHash},
		{"st", TypeSet},
		{"z", TypeZSet},
	}
	for _, c := range cases {
		got, ok := s.GetType([]byte(c.key))
		if !ok || got != c.want {
			t.Errorf("GetType(%s): got %v, %v; want %v, true", c.key, got, ok, c.want)
		}
	}
	if _, ok := s.GetType([]byte("missing")); ok {
		t.Errorf("expected missing type")
	}
}
