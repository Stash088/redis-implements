package store

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReadWriteStringValue checks the copy-on-write invariant: a
// reader that got a reference to a stored value may keep using it after the
// store lock is released, even if another goroutine changes the value at the
// same time.
//
// Before the invariant was introduced, APPEND and SETRANGE modified the stored
// buffer in place, so a concurrent GET could observe a torn value (and the
// race detector flagged it).
func TestConcurrentReadWriteStringValue(t *testing.T) {
	s := New()
	key := []byte("k")
	// A value with spare capacity: an in-place APPEND would write into the
	// same array the reader is looking at.
	value := make([]byte, 10, 64)
	copy(value, "0123456789")
	s.SetString(key, value)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: grows and overwrites the value in place.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if _, err := s.Append(key, []byte("x")); err != nil {
				t.Error(err)
				return
			}
			if _, err := s.SetRange(key, 0, []byte("yyyy")); err != nil {
				t.Error(err)
				return
			}
		}
		close(stop)
	}()

	// Readers: keep the value they got and check it is still consistent.
	wg.Add(2)
	for r := 0; r < 2; r++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok, err := s.GetString(key)
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					continue
				}
				// The snapshot must be one of the values the store
				// published: either the original digits, the "yyyy"
				// prefix or the appended "x" run.
				if !bytes.HasPrefix(v, []byte("0123456789")) &&
					!bytes.HasPrefix(v, []byte("yyyy")) {
					t.Errorf("torn value read: %q", v)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestGetRangeSnapshotKeepsOldBuffer checks that a value read with GETRANGE
// stays valid after the string is overwritten.
func TestGetRangeSnapshotKeepsOldBuffer(t *testing.T) {
	s := New()
	key := []byte("k")
	s.SetString(key, []byte("abcdef"))
	snap, _, err := s.GetRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetRange(key, 0, []byte("ZZZZZZ")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snap, []byte("abcdef")) {
		t.Errorf("snapshot changed: got %q, want abcdef", snap)
	}
}

// TestSetStringReusesTheEntry checks that overwriting a string updates the
// entry in place: the readers that already got the old value keep it (the
// value buffers are never modified in place), and the key holds the new
// value.
func TestSetStringReusesTheEntry(t *testing.T) {
	s := New()
	key := []byte("k")
	s.SetString(key, []byte("first"))

	old, ok, err := s.GetString(key)
	if err != nil || !ok {
		t.Fatalf("GetString: %v %v", err, ok)
	}

	// The overwrite must not touch the buffer the reader holds.
	s.SetString(key, []byte("second"))
	if string(old) != "first" {
		t.Fatalf("the old value became %q, want first", old)
	}

	cur, ok, err := s.GetString(key)
	if err != nil || !ok {
		t.Fatalf("GetString: %v %v", err, ok)
	}
	if string(cur) != "second" {
		t.Fatalf("the value is %q, want second", cur)
	}
	// The TTL of the key is reset by the overwrite, like in Redis.
	s.SetStringWithTTL(key, []byte("third"), time.Hour)
	s.SetString(key, []byte("fourth"))
	if ttl, ok := s.TTL(key); !ok || ttl != -1 {
		t.Fatalf("the TTL is %v (%v), want no expiry", ttl, ok)
	}
}
