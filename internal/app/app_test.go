package app

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/config"
	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

func testLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

func parseConfig(t *testing.T, args ...string) config.Config {
	t.Helper()
	cfg, err := config.Parse(args)
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}
	return cfg
}

func newDBs(n int) []*store.Store {
	dbs := make([]*store.Store, n)
	for i := range dbs {
		dbs[i] = store.New()
	}
	return dbs
}

func writeRDBValue(t *testing.T, path, key, value string, databases int) {
	t.Helper()
	dbs := newDBs(databases)
	dbs[0].SetString([]byte(key), []byte(value))
	if err := persist.Save(path, dbs); err != nil {
		t.Fatalf("persist.Save: %v", err)
	}
}

func writeAOF(t *testing.T, path string, args ...string) {
	t.Helper()
	cmd := make([][]byte, len(args))
	for i, a := range args {
		cmd[i] = []byte(a)
	}
	if err := os.WriteFile(path, resp.EncodeCommand(cmd), 0o600); err != nil {
		t.Fatalf("write aof: %v", err)
	}
}

func mustString(t *testing.T, db *store.Store, key string) string {
	t.Helper()
	v, ok, err := db.GetString([]byte(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if !ok {
		return ""
	}
	return string(v)
}

func TestRestorePrefersEnabledAOF(t *testing.T) {
	dir := t.TempDir()
	cfg := parseConfig(t, "--dir", dir, "--appendonly")
	dbPath := filepath.Join(dir, cfg.DBFilename)
	aofPath := filepath.Join(dir, cfg.AppendFilename)
	writeRDBValue(t, dbPath, "rdb-key", "from-rdb", cfg.Databases)
	writeAOF(t, aofPath, "SET", "aof-key", "from-aof")

	dbs := newDBs(cfg.Databases)
	aofExists, err := restoreDataset(cfg, dbs, dbPath, aofPath)
	if err != nil {
		t.Fatalf("restoreDataset: %v", err)
	}
	if !aofExists {
		t.Fatal("aofExists = false, want true")
	}
	if got := mustString(t, dbs[0], "aof-key"); got != "from-aof" {
		t.Fatalf("aof-key = %q, want from-aof", got)
	}
	if got := mustString(t, dbs[0], "rdb-key"); got != "" {
		t.Fatalf("rdb-key = %q, want it not to be loaded when the AOF wins", got)
	}
}

func TestRestoreFallsBackToRDBWithoutAOF(t *testing.T) {
	dir := t.TempDir()
	cfg := parseConfig(t, "--dir", dir, "--appendonly")
	dbPath := filepath.Join(dir, cfg.DBFilename)
	aofPath := filepath.Join(dir, cfg.AppendFilename)
	writeRDBValue(t, dbPath, "rdb-key", "from-rdb", cfg.Databases)

	dbs := newDBs(cfg.Databases)
	aofExists, err := restoreDataset(cfg, dbs, dbPath, aofPath)
	if err != nil {
		t.Fatalf("restoreDataset: %v", err)
	}
	if aofExists {
		t.Fatal("aofExists = true, want false")
	}
	if got := mustString(t, dbs[0], "rdb-key"); got != "from-rdb" {
		t.Fatalf("rdb-key = %q, want from-rdb", got)
	}
}

func TestRestoreCorruptAOFDoesNotFallBackToRDB(t *testing.T) {
	dir := t.TempDir()
	cfg := parseConfig(t, "--dir", dir, "--appendonly")
	dbPath := filepath.Join(dir, cfg.DBFilename)
	aofPath := filepath.Join(dir, cfg.AppendFilename)
	writeRDBValue(t, dbPath, "rdb-key", "from-rdb", cfg.Databases)
	if err := os.WriteFile(aofPath, []byte("+OK\r\n"), 0o600); err != nil {
		t.Fatalf("write corrupt aof: %v", err)
	}

	dbs := newDBs(cfg.Databases)
	_, err := restoreDataset(cfg, dbs, dbPath, aofPath)
	if err == nil {
		t.Fatal("restoreDataset succeeded on a corrupt AOF, want an error")
	}
	if !strings.Contains(err.Error(), "aof load:") {
		t.Fatalf("error = %q, want the aof load prefix", err)
	}
	if got := mustString(t, dbs[0], "rdb-key"); got != "" {
		t.Fatalf("rdb-key = %q, want no RDB fallback after a corrupt AOF", got)
	}
}

func TestOpenAOFSeedsNewFileFromLoadedRDB(t *testing.T) {
	dir := t.TempDir()
	cfg := parseConfig(t, "--dir", dir, "--appendonly")
	dbPath := filepath.Join(dir, cfg.DBFilename)
	aofPath := filepath.Join(dir, cfg.AppendFilename)
	writeRDBValue(t, dbPath, "rdb-key", "from-rdb", cfg.Databases)

	dbs := newDBs(cfg.Databases)
	aofExists, err := restoreDataset(cfg, dbs, dbPath, aofPath)
	if err != nil {
		t.Fatalf("restoreDataset: %v", err)
	}
	if aofExists {
		t.Fatal("aofExists = true, want false")
	}
	aof, err := openAOF(cfg, dbs, aofPath, aofExists)
	if err != nil {
		t.Fatalf("openAOF: %v", err)
	}
	if aof == nil {
		t.Fatal("openAOF = nil, want an appender")
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("aof.Close: %v", err)
	}

	f, err := os.Open(aofPath)
	if err != nil {
		t.Fatalf("open seeded aof: %v", err)
	}
	defer f.Close()
	replayed := newDBs(cfg.Databases)
	if err := persist.LoadAOF(f, replayed); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if got := mustString(t, replayed[0], "rdb-key"); got != "from-rdb" {
		t.Fatalf("replayed rdb-key = %q, want from-rdb", got)
	}
}

func TestOpenAOFDoesNotRewriteExistingAOF(t *testing.T) {
	dir := t.TempDir()
	cfg := parseConfig(t, "--dir", dir, "--appendonly")
	aofPath := filepath.Join(dir, cfg.AppendFilename)
	writeAOF(t, aofPath, "SET", "existing", "keep")
	before, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("read existing aof: %v", err)
	}

	// The stores hold live data, but an existing AOF is authoritative: the
	// startup must open it as-is and never rewrite it.
	dbs := newDBs(cfg.Databases)
	dbs[0].SetString([]byte("memory-only"), []byte("value"))
	aof, err := openAOF(cfg, dbs, aofPath, true)
	if err != nil {
		t.Fatalf("openAOF: %v", err)
	}
	if aof == nil {
		t.Fatal("openAOF = nil, want an appender")
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("aof.Close: %v", err)
	}

	after, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("read existing aof after open: %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("existing AOF changed at startup:\nbefore %q\nafter  %q", before, after)
	}
}

func TestRunAlreadyCanceledShutsDownCleanly(t *testing.T) {
	for _, mode := range []string{"loop", "goroutine"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cfg := parseConfig(t,
				"--dir", dir,
				"--port", strconv.Itoa(freePort(t)),
				"--io-mode", mode,
				"--appendonly",
				"--save=",
			)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			done := make(chan error, 1)
			go func() {
				done <- Run(ctx, cfg, testLogger())
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return for an already-canceled context")
			}
		})
	}
}

func TestRunCancellationFlushesPersistence(t *testing.T) {
	dir := t.TempDir()
	cfg := parseConfig(t,
		"--dir", dir,
		"--port", strconv.Itoa(freePort(t)),
		"--appendonly",
		"--save=",
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, cfg, testLogger())
	}()

	waitForServer(t, cfg.Addr())

	conn, err := net.DialTimeout("tcp", cfg.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	cmd := resp.EncodeCommand([][]byte{[]byte("SET"), []byte("cancel-key"), []byte("value")})
	if _, err := conn.Write(cmd); err != nil {
		t.Fatalf("write SET: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read SET reply: %v", err)
	}
	if line != "+OK\r\n" {
		t.Fatalf("SET reply = %q, want +OK", line)
	}
	conn.Close()

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	rdbPath := filepath.Join(dir, cfg.DBFilename)
	dbs := newDBs(cfg.Databases)
	if err := loadRDB(rdbPath, dbs); err != nil {
		t.Fatalf("load final RDB: %v", err)
	}
	if got := mustString(t, dbs[0], "cancel-key"); got != "value" {
		t.Fatalf("final RDB cancel-key = %q, want value", got)
	}

	aofPath := filepath.Join(dir, cfg.AppendFilename)
	aofDBs := newDBs(cfg.Databases)
	if err := loadAOF(aofPath, aofDBs); err != nil {
		t.Fatalf("load final AOF: %v", err)
	}
	if got := mustString(t, aofDBs[0], "cancel-key"); got != "value" {
		t.Fatalf("final AOF cancel-key = %q, want value", got)
	}
}

func TestRunListenFailureClosesAOF(t *testing.T) {
	for _, mode := range []string{"loop", "goroutine"} {
		t.Run(mode, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("reserve port: %v", err)
			}
			defer ln.Close()
			port := ln.Addr().(*net.TCPAddr).Port

			cfg := parseConfig(t,
				"--dir", t.TempDir(),
				"--port", strconv.Itoa(port),
				"--io-mode", mode,
				"--appendonly",
				"--save=",
			)
			err = Run(context.Background(), cfg, testLogger())
			if err == nil {
				t.Fatal("Run succeeded on an occupied port, want an error")
			}
			if !strings.Contains(err.Error(), "server: failed to listen on") {
				t.Fatalf("error = %q, want the listen failure prefix", err)
			}
			if hasAppenderGoroutine() {
				t.Fatal("the AOF background goroutine outlived the failed startup")
			}
		})
	}
}

func hasAppenderGoroutine() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Contains(string(buf[:n]), "persist.(*Appender).background")
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
			if _, err := io.WriteString(conn, "*1\r\n$4\r\nPING\r\n"); err == nil {
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err == nil && line == "+PONG\r\n" {
					conn.Close()
					return
				}
			}
			conn.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s did not answer PING", addr)
}
