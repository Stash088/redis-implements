package persist

import (
	"bytes"
	"sync"
	"testing"

	"github.com/amir/redis-implements/internal/store"
)

// TestSaveWhileWriting checks that a snapshot can be taken while the
// collections are being changed: before the entries were copied under the
// lock, the serialization raced with the writers.
func TestSaveWhileWriting(t *testing.T) {
	st := store.New()
	st.ListPush([]byte("list"), false, []byte("a"), []byte("b"), []byte("c"))
	st.HashSet([]byte("hash"), []byte("f1"), []byte("v1"))
	st.HashSet([]byte("hash"), []byte("f2"), []byte("v2"))
	st.SetAdd([]byte("set"), []byte("m1"), []byte("m2"))
	st.ZAdd([]byte("zset"), 1.5, []byte("a"))
	st.ZAdd([]byte("zset"), 2.5, []byte("b"))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// The writers change the collections while the snapshots are taken.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				st.ListPush([]byte("list"), false, []byte("x"))
				st.ListPop([]byte("list"), false)
				st.HashSet([]byte("hash"), []byte("f3"), []byte("v3"))
				st.HashDel([]byte("hash"), []byte("f3"))
				st.SetAdd([]byte("set"), []byte("m3"))
				st.SetRem([]byte("set"), []byte("m3"))
				st.ZAdd([]byte("zset"), 3.5, []byte("c"))
			}
		}()
	}
	// The snapshots.
	for i := 0; i < 20; i++ {
		var buf bytes.Buffer
		if err := WriteRDB(&buf, []*store.Store{st}); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("WriteRDB: %v", err)
		}
		if err := LoadBytes(buf.Bytes(), []*store.Store{store.New()}); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("the snapshot does not load: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
