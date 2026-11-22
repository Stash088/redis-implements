package server

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/store"
)

// TestStartSavePolicyRepeatIsNoop checks that the save policy is startup-only:
// a repeated call, even with disabled settings, must not replace the stop
// channel or change the settings of the worker that is already running.
func TestStartSavePolicyRepeatIsNoop(t *testing.T) {
	srv := newServer([]*store.Store{store.New()}, nil)
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.DBPath = filepath.Join(t.TempDir(), "dump.rdb")
	srv.StartSavePolicy(7, 3)
	srv.StartSavePolicy(0, 0)

	srv.mu.Lock()
	seconds, changes := srv.saveSeconds, srv.saveChanges
	started := srv.saveStop != nil
	srv.mu.Unlock()
	if !started {
		t.Fatal("the save policy worker did not start")
	}
	if seconds != 7 || changes != 3 {
		t.Fatalf("repeated call changed the settings to %d/%d, want 7/3", seconds, changes)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for a save policy worker that was replaced")
	}
}

// TestSaveRDBKeepsWritesDuringSnapshotPending checks that a write counted
// while a snapshot is in flight stays pending after the save succeeds: it may
// or may not be in that snapshot, so the next save (Shutdown here) must run.
func TestSaveRDBKeepsWritesDuringSnapshotPending(t *testing.T) {
	dir := t.TempDir()
	st := store.New()
	st.SetStrings([][]byte{[]byte("before"), []byte("value")}, false)
	srv := newServer([]*store.Store{st}, nil)
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.DBPath = filepath.Join(dir, "dump.rdb")

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	st.SetClock(func() time.Time {
		once.Do(func() {
			close(entered)
			<-release
		})
		return time.Now()
	})

	srv.saveMu.Lock()
	saved := make(chan error, 1)
	go func() {
		defer srv.saveMu.Unlock()
		saved <- srv.saveRDB()
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot did not start")
	}

	// A write lands after the counters were captured for the snapshot.
	srv.dirty[0].Add(1)
	close(release)
	if err := <-saved; err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := srv.dirtyTotal(); got != 1 {
		t.Fatalf("pending writes after the save: got %d, want 1", got)
	}

	// The pending count makes Shutdown take a final snapshot that includes
	// the new write.
	st.SetStrings([][]byte{[]byte("after"), []byte("saved")}, false)
	srv.dirty[0].Add(1)
	if err := srv.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := srv.dirtyTotal(); got != 0 {
		t.Fatalf("pending writes after Shutdown: got %d, want 0", got)
	}

	f, err := os.Open(srv.DBPath)
	if err != nil {
		t.Fatalf("open rdb: %v", err)
	}
	dbs := []*store.Store{store.New()}
	err = persist.Load(f, dbs)
	f.Close()
	if err != nil {
		t.Fatalf("load rdb: %v", err)
	}
	if got, _, _ := dbs[0].GetString([]byte("after")); string(got) != "saved" {
		t.Errorf("the shutdown snapshot holds after=%q, want saved", got)
	}
}

// TestSaveRDBFailureKeepsPendingWrites checks that a failed save does not
// consume the write counts that were captured before it tried.
func TestSaveRDBFailureKeepsPendingWrites(t *testing.T) {
	srv := newServer([]*store.Store{store.New()}, nil)
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.DBPath = filepath.Join(t.TempDir(), "missing", "dump.rdb")
	srv.dirty[2].Add(3)

	srv.saveMu.Lock()
	err := srv.saveRDB()
	srv.saveMu.Unlock()
	if err == nil {
		t.Fatal("the save succeeded in a missing directory")
	}
	if got := srv.dirtyTotal(); got != 3 {
		t.Fatalf("pending writes after a failed save: got %d, want 3", got)
	}
	srv.Close()
}
