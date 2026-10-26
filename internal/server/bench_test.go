package server_test

import (
	"bytes"
	"io"
	"log"
	"net"
	"strconv"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// benchServer starts a quiet server and returns its address. It does not stop
// the server; use cleanup to close it.
func benchServer(b *testing.B, cleanup func()) string {
	b.Helper()
	st := store.New()
	srv, err := server.New("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		b.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	go srv.Serve()
	cleanup()
	return srv.Addr().String()
}

func benchClient(b *testing.B, addr string) (net.Conn, *resp.Reader, *resp.Writer) {
	b.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	return conn, resp.NewReader(conn), resp.NewWriter(conn)
}

func BenchmarkServerPipelinedSetGet(b *testing.B) {
	addr := benchServer(b, func() {})
	conn, r, w := benchClient(b, addr)
	defer conn.Close()

	payload := make([]resp.Value, 100)
	for i := range payload {
		payload[i] = resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("SET")),
			resp.NewBulkString([]byte("key")),
			resp.NewBulkString([]byte("value")),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(payload); j++ {
			if err := w.WriteValue(payload[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < len(payload); j++ {
			if _, err := r.ReadValue(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkServerRoundTripSet(b *testing.B) {
	addr := benchServer(b, func() {})
	conn, r, w := benchClient(b, addr)
	defer conn.Close()

	cmd := resp.NewArray([]resp.Value{
		resp.NewBulkString([]byte("SET")),
		resp.NewBulkString([]byte("key")),
		resp.NewBulkString([]byte("value")),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteValue(cmd); err != nil {
			b.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerRoundTripGetLarge(b *testing.B) {
	addr := benchServer(b, func() {})
	conn, r, w := benchClient(b, addr)
	defer conn.Close()

	val := bytes.Repeat([]byte("x"), 64*1024)
	set := resp.NewArray([]resp.Value{
		resp.NewBulkString([]byte("SET")),
		resp.NewBulkString([]byte("key")),
		resp.NewBulkString(val),
	})
	w.WriteValue(set)
	w.Flush()
	r.ReadValue()

	get := resp.NewArray([]resp.Value{
		resp.NewBulkString([]byte("GET")),
		resp.NewBulkString([]byte("key")),
	})
	b.ReportAllocs()
	b.SetBytes(int64(len(val)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteValue(get); err != nil {
			b.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServerPipelinedMediumGet(b *testing.B) {
	addr := benchServer(b, func() {})
	conn, r, w := benchClient(b, addr)
	defer conn.Close()

	val := bytes.Repeat([]byte("y"), 2048)
	// Store 100 keys with 2KB values.
	for i := 0; i < 100; i++ {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("SET")),
			resp.NewBulkString([]byte("key:" + strconv.Itoa(i))),
			resp.NewBulkString(val),
		})
		w.WriteValue(cmd)
	}
	w.Flush()
	for i := 0; i < 100; i++ {
		r.ReadValue()
	}

	get := make([]resp.Value, 100)
	for i := range get {
		get[i] = resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("GET")),
			resp.NewBulkString([]byte("key:" + strconv.Itoa(i))),
		})
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(val) * len(get)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range get {
			if err := w.WriteValue(get[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		for range get {
			if _, err := r.ReadValue(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkServerRoundTripGet(b *testing.B) {
	addr := benchServer(b, func() {})
	conn, r, w := benchClient(b, addr)
	defer conn.Close()

	set := resp.NewArray([]resp.Value{
		resp.NewBulkString([]byte("SET")),
		resp.NewBulkString([]byte("key")),
		resp.NewBulkString([]byte("value")),
	})
	w.WriteValue(set)
	w.Flush()
	r.ReadValue()

	get := resp.NewArray([]resp.Value{
		resp.NewBulkString([]byte("GET")),
		resp.NewBulkString([]byte("key")),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteValue(get); err != nil {
			b.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanTraversal measures a full SCAN traversal of the keyspace
// (1000 keys, COUNT=100): the total cost the client pays to walk the keys.
func BenchmarkScanTraversal(b *testing.B) {
	addr := benchServer(b, func() {})
	conn, r, w := benchClient(b, addr)
	defer conn.Close()

	const keys = 1000
	for i := 0; i < keys; i++ {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("SET")),
			resp.NewBulkString([]byte("scan:key:" + strconv.Itoa(i))),
			resp.NewBulkString([]byte("v")),
		})
		if err := w.WriteValue(cmd); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < keys; i++ {
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}

	scan := func(cursor string) (resp.Value, error) {
		cmd := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("SCAN")),
			resp.NewBulkString([]byte(cursor)),
			resp.NewBulkString([]byte("COUNT")),
			resp.NewBulkString([]byte("100")),
		})
		if err := w.WriteValue(cmd); err != nil {
			return resp.Value{}, err
		}
		if err := w.Flush(); err != nil {
			return resp.Value{}, err
		}
		return r.ReadValue()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor := "0"
		steps := 0
		seen := 0
		for {
			v, err := scan(cursor)
			if err != nil {
				b.Fatal(err)
			}
			if v.Kind != resp.KindArray || len(v.Array) != 2 {
				b.Fatalf("bad SCAN reply")
			}
			cursor = string(v.Array[0].Str)
			seen += len(v.Array[1].Array)
			steps++
			if cursor == "0" {
				break
			}
			if steps > keys+10 {
				b.Fatalf("SCAN does not terminate")
			}
		}
		if seen != keys {
			b.Fatalf("traversal returned %d keys, want %d", seen, keys)
		}
	}
}
