package persist

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

// TestAOFBufferFlushesOnInterval checks that the buffered commands reach the
// file even when nothing else writes them.
func TestAOFBufferFlushesOnInterval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	a, err := OpenAppender(path, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetFlushInterval(10 * time.Millisecond)

	if err := a.Append([][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
		t.Fatal(err)
	}
	// The command is in the buffer, not in the file yet.
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		t.Errorf("the command was written before the flush: %q", data)
	}
	// The background flush writes it out.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			if !strings.Contains(string(data), "SET") {
				t.Errorf("unexpected file content: %q", data)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the command was not flushed to the file")
}

// TestAOFFlushWritesBuffer checks that Flush writes the buffered commands.
func TestAOFFlushWritesBuffer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	a, err := OpenAppender(path, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	// No background flush: only the explicit one writes the buffer.
	a.SetFlushInterval(0)

	for i := 0; i < 3; i++ {
		if err := a.Append([][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "SET"); got != 3 {
		t.Errorf("got %d commands in the file, want 3", got)
	}
}

// TestAOFAlwaysWritesImmediately checks that the safest policy does not
// depend on the buffer: the command is on disk when Append returns.
func TestAOFAlwaysWritesImmediately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	a, err := OpenAppender(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetFlushInterval(time.Hour) // the background flush must not matter

	if err := a.Append([][]byte{[]byte("SET"), []byte("k"), []byte("v")}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "SET") {
		t.Errorf("the command is not in the file: %q", data)
	}
}

// TestAOFBatchWritesOneCommand checks that a batch of commands is written as
// one chunk: the file must not contain a partial command.
func TestAOFBatchWritesOneCommand(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	a, err := OpenAppender(path, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetFlushInterval(0)

	for i := 0; i < 100; i++ {
		key := []byte("key:" + string(itoa(int64(i))))
		if err := a.Append([][]byte{[]byte("SET"), key, []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Every command must be complete: the file has to replay cleanly.
	st := store.New()
	if err := LoadAOF(strings.NewReader(string(data)), []*store.Store{st}); err != nil {
		t.Fatalf("the file does not replay: %v", err)
	}
	if got := st.Len(); got != 100 {
		t.Errorf("got %d keys after the replay, want 100", got)
	}
}

// TestAOFEncodingMatchesWriter checks that the hand written encoder produces
// the same bytes as the RESP writer.
func TestAOFEncodingMatchesWriter(t *testing.T) {
	cases := [][][]byte{
		{[]byte("SET"), []byte("k"), []byte("v")},
		{[]byte("SET"), []byte("k"), []byte("")},
		{[]byte("RPUSH"), []byte("l"), []byte("a"), []byte("b")},
		{[]byte("SET"), []byte("k"), []byte("v\x00\r\n")},
	}
	for _, c := range cases {
		got := appendCommand(nil, c)
		want := EncodeCommand(c)
		if string(got) != string(want) {
			t.Errorf("encoding mismatch for %q: got %q, want %q", c, got, want)
		}
	}
}
