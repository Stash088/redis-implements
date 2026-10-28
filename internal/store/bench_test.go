package store_test

import (
	"strconv"
	"testing"

	"github.com/amir/redis-implements/internal/store"
)

func BenchmarkStoreSet(b *testing.B) {
	st := store.New()
	key := []byte("key")
	val := []byte("value")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.SetString(key, val)
	}
}

func BenchmarkStoreSetUniqueKeys(b *testing.B) {
	st := store.New()
	val := []byte("value")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.SetString([]byte("key:"+strconv.Itoa(i)), val)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	st := store.New()
	key := []byte("key")
	st.SetString(key, []byte("value"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.GetString(key)
	}
}

func BenchmarkStoreIncr(b *testing.B) {
	st := store.New()
	key := []byte("counter")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := st.IncrBy(key, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreExpireCycle(b *testing.B) {
	st := store.New()
	for i := 0; i < 1000; i++ {
		st.SetStringWithTTL([]byte("k:"+strconv.Itoa(i)), []byte("v"), 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.ExpireCycle()
	}
}

func BenchmarkStoreListPushPop(b *testing.B) {
	st := store.New()
	key := []byte("l")
	val := []byte("element")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.ListPush(key, false, val)
	}
	for i := 0; i < b.N; i++ {
		st.ListPop(key, true)
	}
}

func BenchmarkStoreHashSetGet(b *testing.B) {
	st := store.New()
	key := []byte("h")
	field := []byte("field")
	val := []byte("value")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.HashSet(key, field, val)
	}
	for i := 0; i < b.N; i++ {
		st.HashGet(key, field)
	}
}

func BenchmarkStoreSetAddMembers(b *testing.B) {
	st := store.New()
	key := []byte("s")
	member := []byte("member")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st.SetAdd(key, member)
	}
	for i := 0; i < b.N; i++ {
		st.SetMembers(key)
	}
}

func BenchmarkStoreZAdd(b *testing.B) {
	st := store.New()
	key := []byte("z")
	member := []byte("member")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := st.ZAdd(key, float64(i%100), member); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreZRange(b *testing.B) {
	st := store.New()
	key := []byte("z")
	for i := 0; i < 1000; i++ {
		st.ZAdd(key, float64(i), []byte("member:"+strconv.Itoa(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.ZRange(key, 0, 99, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreKeys(b *testing.B) {
	st := store.New()
	for i := 0; i < 1000; i++ {
		st.SetString([]byte("k:"+strconv.Itoa(i)), []byte("v"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.Keys()
	}
}

// BenchmarkHashSnapshot measures the cost of the snapshot the scan commands
// walk: it is built once per traversal, so its cost is paid by the first
// call of the traversal only.
func BenchmarkHashSnapshot(b *testing.B) {
	st := store.New()
	fields := 100000
	for i := 0; i < fields; i++ {
		st.HashSet([]byte("h"), []byte("field"+strconv.Itoa(i)), []byte("value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, total, found, err := st.ScanHashPage([]byte("h"), 0, 100)
		if err != nil || !found {
			b.Fatalf("ScanHashPage: %v %v", err, found)
		}
		if len(page) != 100 || total != fields {
			b.Fatalf("got %d fields (total %d)", len(page), total)
		}
		// The snapshot is dropped when the traversal reaches the end of
		// the collection, so the next call builds it again.
		st.ScanHashPage([]byte("h"), fields-100, 100)
	}
}
