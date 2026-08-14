package resp_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

func BenchmarkWriterSimpleString(b *testing.B) {
	w := resp.NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteSimpleString("PONG")
	}
}

func BenchmarkWriterBulkSmall(b *testing.B) {
	w := resp.NewWriter(io.Discard)
	val := []byte("hello world")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteBulkString(val)
	}
}

func BenchmarkWriterBulkLarge(b *testing.B) {
	w := resp.NewWriter(io.Discard)
	val := bytes.Repeat([]byte("x"), 64*1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(val)))
	for i := 0; i < b.N; i++ {
		w.WriteBulkString(val)
	}
}

func BenchmarkWriterArrayOfBulks(b *testing.B) {
	w := resp.NewWriter(io.Discard)
	elems := []resp.Value{
		resp.NewBulkString([]byte("SET")),
		resp.NewBulkString([]byte("key")),
		resp.NewBulkString([]byte("value")),
	}
	msg := resp.NewArray(elems)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteValue(msg)
	}
}

func BenchmarkWriterInteger(b *testing.B) {
	w := resp.NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteInteger(int64(i))
	}
}

func BenchmarkReaderSmallArray(b *testing.B) {
	// Serialize a small command once, then repeatedly decode it.
	payload := bytes.NewBuffer(nil)
	w := resp.NewWriter(payload)
	elems := []resp.Value{
		resp.NewBulkString([]byte("SET")),
		resp.NewBulkString([]byte("foo")),
		resp.NewBulkString([]byte("bar")),
	}
	w.WriteValue(resp.NewArray(elems))
	w.Flush()
	raw := payload.Bytes()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := resp.NewReader(bytes.NewReader(raw))
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderLargeBulk(b *testing.B) {
	payload := bytes.NewBuffer(nil)
	w := resp.NewWriter(payload)
	val := bytes.Repeat([]byte("x"), 1024)
	w.WriteBulkString(val)
	w.Flush()
	raw := payload.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		r := resp.NewReader(bytes.NewReader(raw))
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriterFlush(b *testing.B) {
	w := resp.NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteSimpleString("PONG")
		w.Flush()
	}
}
