package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestScanHashPageIsStable(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		if _, err := s.HashSet([]byte("h"), []byte(fmt.Sprintf("f%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	first, total, found, err := s.ScanHashPage([]byte("h"), 0, 100)
	if err != nil || !found {
		t.Fatalf("ScanHashPage: %v %v", err, found)
	}
	if total != 10 || len(first) != 10 {
		t.Fatalf("got %d fields (total %d), want 10", len(first), total)
	}
	// The fields come in the order of their names.
	for i, fv := range first {
		want := fmt.Sprintf("f%d", i)
		if string(fv.Field) != want {
			t.Errorf("field %d: got %q, want %q", i, fv.Field, want)
		}
	}
	// The second call returns the same fields: the snapshot is reused.
	second, _, _, err := s.ScanHashPage([]byte("h"), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := range first {
		if string(second[i].Field) != string(first[i].Field) {
			t.Fatalf("call %d returned %q, want %q", i, second[i].Field, first[i].Field)
		}
	}
}

func TestScanHashPageFollowsChanges(t *testing.T) {
	s := New()
	s.HashSet([]byte("h"), []byte("a"), []byte("1"))
	s.HashSet([]byte("h"), []byte("b"), []byte("2"))
	// A first page builds the snapshot.
	page, total, _, err := s.ScanHashPage([]byte("h"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || total != 2 {
		t.Fatalf("got %d fields (total %d), want 2", len(page), total)
	}
	// A new field changes the collection: the snapshot is built again and
	// the new field is visible.
	s.HashSet([]byte("h"), []byte("c"), []byte("3"))
	page, total, _, err = s.ScanHashPage([]byte("h"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 3 || total != 3 {
		t.Fatalf("after HSET: got %d fields (total %d), want 3", len(page), total)
	}
	// A removed field disappears from the snapshot.
	s.HashDel([]byte("h"), []byte("a"))
	page, total, _, err = s.ScanHashPage([]byte("h"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || total != 2 {
		t.Fatalf("after HDEL: got %d fields (total %d), want 2", len(page), total)
	}
	for _, fv := range page {
		if string(fv.Field) == "a" {
			t.Fatalf("the removed field is still in the snapshot")
		}
	}
}

func TestScanHashPageShowsFreshValues(t *testing.T) {
	s := New()
	s.HashSet([]byte("h"), []byte("a"), []byte("1"))
	page, _, _, err := s.ScanHashPage([]byte("h"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if string(page[0].Value) != "1" {
		t.Fatalf("got %q, want 1", page[0].Value)
	}
	// Overwriting a value does not change the set of the fields, but the
	// page must show the new value.
	s.HashSet([]byte("h"), []byte("a"), []byte("2"))
	page, _, _, err = s.ScanHashPage([]byte("h"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || string(page[0].Value) != "2" {
		t.Fatalf("got %v, want the value 2", page)
	}
}

func TestScanSetPage(t *testing.T) {
	s := New()
	for _, m := range []string{"c", "a", "b"} {
		s.SetAdd([]byte("s"), []byte(m))
	}
	page, total, found, err := s.ScanSetPage([]byte("s"), 0, 2)
	if err != nil || !found {
		t.Fatalf("ScanSetPage: %v %v", err, found)
	}
	if total != 3 || len(page) != 2 {
		t.Fatalf("got %d members (total %d), want 2 of 3", len(page), total)
	}
	if string(page[0]) != "a" || string(page[1]) != "b" {
		t.Fatalf("got %q %q, want a b", page[0], page[1])
	}
	// The rest of the collection.
	page, total, _, err = s.ScanSetPage([]byte("s"), 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(page) != 1 || string(page[0]) != "c" {
		t.Fatalf("got %v (total %d), want c", page, total)
	}
	// A removed member leaves the snapshot.
	s.SetRem([]byte("s"), []byte("a"))
	page, total, _, err = s.ScanSetPage([]byte("s"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(page) != 2 {
		t.Fatalf("after SREM: got %d members (total %d), want 2", len(page), total)
	}
	for _, m := range page {
		if string(m) == "a" {
			t.Fatalf("the removed member is still in the snapshot")
		}
	}
}

func TestScanZSetPage(t *testing.T) {
	s := New()
	s.ZAdd([]byte("z"), 2, []byte("b"))
	s.ZAdd([]byte("z"), 1, []byte("a"))
	s.ZAdd([]byte("z"), 3, []byte("c"))
	page, total, found, err := s.ScanZSetPage([]byte("z"), 0, 2)
	if err != nil || !found {
		t.Fatalf("ScanZSetPage: %v %v", err, found)
	}
	if total != 3 || len(page) != 2 {
		t.Fatalf("got %d members (total %d), want 2 of 3", len(page), total)
	}
	// The members come in the order of their score.
	if string(page[0].Member) != "a" || string(page[1].Member) != "b" {
		t.Fatalf("got %q %q, want a b", page[0].Member, page[1].Member)
	}
	if page[0].Score != 1 || page[1].Score != 2 {
		t.Fatalf("got the scores %v %v, want 1 2", page[0].Score, page[1].Score)
	}
	// A changed score changes the order: the snapshot is built again.
	s.ZIncrBy([]byte("z"), 10, []byte("c"))
	page, total, _, err = s.ScanZSetPage([]byte("z"), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(page) != 3 {
		t.Fatalf("got %d members (total %d), want 3", len(page), total)
	}
	if string(page[2].Member) != "c" || page[2].Score != 13 {
		t.Fatalf("got %q (%v), want c (13)", page[2].Member, page[2].Score)
	}
}

func TestScanHashPageWalksEveryFieldOnce(t *testing.T) {
	s := New()
	const n = 100
	for i := 0; i < n; i++ {
		s.HashSet([]byte("h"), []byte(fmt.Sprintf("f%03d", i)), []byte("v"))
	}
	seen := map[string]bool{}
	start := 0
	for {
		page, total, found, err := s.ScanHashPage([]byte("h"), start, 7)
		if err != nil || !found {
			t.Fatalf("ScanHashPage: %v %v", err, found)
		}
		for _, fv := range page {
			if seen[string(fv.Field)] {
				t.Fatalf("the field %q was returned twice", fv.Field)
			}
			seen[string(fv.Field)] = true
		}
		if len(page) == 0 || start+len(page) >= total {
			break
		}
		start += len(page)
	}
	if len(seen) != n {
		t.Fatalf("the traversal returned %d fields, want %d", len(seen), n)
	}
}

// TestScanHashPageConcurrent checks that the scans and the changes of the
// collection do not race: the snapshot is published atomically and the
// version of the collection tells whether it is still valid.
func TestScanHashPageConcurrent(t *testing.T) {
	s := New()
	for i := 0; i < 50; i++ {
		s.HashSet([]byte("h"), []byte(fmt.Sprintf("f%d", i)), []byte("v"))
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// The writer changes the collection while the readers walk it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s.HashSet([]byte("h"), []byte(fmt.Sprintf("f%d", i%100)), []byte("v"))
			s.HashDel([]byte("h"), []byte(fmt.Sprintf("f%d", (i+1)%100)))
		}
	}()
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				page, _, _, err := s.ScanHashPage([]byte("h"), 0, 10)
				if err != nil {
					t.Errorf("ScanHashPage: %v", err)
					return
				}
				for _, fv := range page {
					if string(fv.Value) != "v" {
						t.Errorf("got the value %q, want v", fv.Value)
						return
					}
				}
			}
		}()
	}
	// Let the readers finish, then stop the writer.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(stop)
	}()
	wg.Wait()
}
