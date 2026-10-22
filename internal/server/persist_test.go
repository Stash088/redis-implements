package server_test

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// startPersistServer starts a server with persistence paths configured. When
// enableAOF is true an AOF appender is attached to a fresh file.
func startPersistServer(t *testing.T, dir string, enableAOF bool) (string, string, string) {
	t.Helper()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	dbPath := filepath.Join(dir, "dump.rdb")
	aofPath := filepath.Join(dir, "appendonly.aof")
	srv.DBPath = dbPath
	if enableAOF {
		a, err := persist.OpenAppender(aofPath, persist.FsyncNo)
		if err != nil {
			t.Fatalf("open aof: %v", err)
		}
		srv.AOF = a
		srv.AOFPath = aofPath
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String(), dbPath, aofPath
}

// startPersistServerDBs starts a server with n databases and persistence
// paths configured.
func startPersistServerDBs(t *testing.T, dir string, enableAOF bool, n int) (string, string, string) {
	t.Helper()
	dbs := make([]*store.Store, n)
	for i := range dbs {
		dbs[i] = store.New()
	}
	exp := store.NewExpirer(dbs, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", dbs)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	dbPath := filepath.Join(dir, "dump.rdb")
	aofPath := filepath.Join(dir, "appendonly.aof")
	srv.DBPath = dbPath
	if enableAOF {
		a, err := persist.OpenAppender(aofPath, persist.FsyncNo)
		if err != nil {
			t.Fatalf("open aof: %v", err)
		}
		srv.AOF = a
		srv.AOFPath = aofPath
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String(), dbPath, aofPath
}

// startFromFilesDBs starts a fresh server with n databases whose stores are
// loaded from the given persistence file (AOF when loadAOF, else RDB).
func startFromFilesDBs(t *testing.T, path string, loadAOF bool, n int) string {
	t.Helper()
	dbs := make([]*store.Store, n)
	for i := range dbs {
		dbs[i] = store.New()
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if loadAOF {
		if err := persist.LoadAOF(f, dbs); err != nil {
			t.Fatalf("load aof: %v", err)
		}
	} else {
		if err := persist.Load(f, dbs); err != nil {
			t.Fatalf("load rdb: %v", err)
		}
	}
	exp := store.NewExpirer(dbs, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", dbs)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

// startFromFiles starts a fresh server whose store is loaded from the given
// persistence file (AOF when loadAOF, else RDB).
func startFromFiles(t *testing.T, path string, loadAOF bool) string {
	t.Helper()
	st := store.New()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if loadAOF {
		if err := persist.LoadAOF(f, []*store.Store{st}); err != nil {
			t.Fatalf("load aof: %v", err)
		}
	} else {
		if err := persist.Load(f, []*store.Store{st}); err != nil {
			t.Fatalf("load rdb: %v", err)
		}
	}
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv.Addr().String()
}

func writeSampleData(t *testing.T, addr string) {
	t.Helper()
	c := newClient(t, addr)
	c.ok("SET", "str", "hello")
	c.intWant("RPUSH", "list", "a", "b", "c")
	c.intWant("HSET", "hash", "f1", "v1")
	c.intWant("HSET", "hash", "f2", "v2")
	c.intWant("SADD", "set", "m1", "m2")
	c.intWant("ZADD", "zset", "1.5", "a", "2.5", "b")
	c.intWant("INCR", "counter")
}

func verifySampleData(t *testing.T, addr string) {
	t.Helper()
	c := newClient(t, addr)
	if got := c.bulkWant("GET", "str"); string(got) != "hello" {
		t.Errorf("GET str = %q", got)
	}
	arr := c.arrWant("LRANGE", "list", "0", "-1")
	if len(arr) != 3 || string(arr[0].Str) != "a" {
		t.Errorf("LRANGE = %v", arr)
	}
	if v := c.bulkWant("HGET", "hash", "f2"); string(v) != "v2" {
		t.Errorf("HGET = %q", v)
	}
	if v := c.cmd("SCARD", "set"); v.Kind != resp.KindInteger || v.Int != 2 {
		t.Errorf("SCARD = %v", v)
	}
	if v := c.cmd("ZSCORE", "zset", "b"); v.Kind != resp.KindBulkString || string(v.Str) != "2.5" {
		t.Errorf("ZSCORE = %v", v)
	}
	if v := c.bulkWant("GET", "counter"); string(v) != "1" {
		t.Errorf("counter = %q", v)
	}
}

func TestPersistenceAOFReload(t *testing.T) {
	dir := t.TempDir()
	addr, _, aofPath := startPersistServer(t, dir, true)
	writeSampleData(t, addr)
	// Let the file settle, then reload from it.
	time.Sleep(20 * time.Millisecond)
	addr2 := startFromFiles(t, aofPath, true)
	verifySampleData(t, addr2)
}

func TestPersistenceRDBViaSave(t *testing.T) {
	dir := t.TempDir()
	addr, dbPath, _ := startPersistServer(t, dir, false)
	writeSampleData(t, addr)
	c := newClient(t, addr)
	c.ok("SAVE")
	addr2 := startFromFiles(t, dbPath, false)
	verifySampleData(t, addr2)
}

func TestPersistenceRDBViaBGSAVE(t *testing.T) {
	dir := t.TempDir()
	addr, dbPath, _ := startPersistServer(t, dir, false)
	writeSampleData(t, addr)
	c := newClient(t, addr)
	if v := c.cmd("BGSAVE"); v.Kind != resp.KindSimpleString {
		t.Fatalf("BGSAVE: %v", v)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(dbPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("BGSAVE did not produce the RDB file")
		}
		time.Sleep(20 * time.Millisecond)
	}
	addr2 := startFromFiles(t, dbPath, false)
	verifySampleData(t, addr2)
}

func TestPersistenceBGREWRITEAOF(t *testing.T) {
	dir := t.TempDir()
	addr, _, aofPath := startPersistServer(t, dir, true)
	c := newClient(t, addr)
	// A bunch of churn so the AOF is longer than the rewritten form.
	for i := 0; i < 50; i++ {
		c.ok("SET", "k", "v")
		c.intWant("DEL", "k")
	}
	c.ok("SET", "keep", "yes")
	if v := c.cmd("BGREWRITEAOF"); v.Kind != resp.KindSimpleString {
		t.Fatalf("BGREWRITEAOF: %v", v)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		fi, err := os.Stat(aofPath)
		if err == nil && fi.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("BGREWRITEAOF did not rewrite the AOF")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	addr2 := startFromFiles(t, aofPath, true)
	c2 := newClient(t, addr2)
	if v := c2.bulkWant("GET", "keep"); string(v) != "yes" {
		t.Errorf("keep = %q", v)
	}
}

func TestPersistenceAOFExpiryRewrite(t *testing.T) {
	dir := t.TempDir()
	addr, _, aofPath := startPersistServer(t, dir, true)
	c := newClient(t, addr)
	c.ok("SET", "k", "v", "EX", "5")
	time.Sleep(200 * time.Millisecond)
	addr2 := startFromFiles(t, aofPath, true)
	c2 := newClient(t, addr2)
	if v := c2.bulkWant("GET", "k"); string(v) != "v" {
		t.Fatalf("k = %q", v)
	}
	if v := c2.cmd("TTL", "k"); v.Kind != resp.KindInteger || v.Int <= 0 || v.Int >= 5 {
		t.Errorf("TTL should be ~5s minus elapsed, got %v", v)
	}
}

func TestPersistenceSAVEDisabledError(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	if v := c.cmd("SAVE"); v.Kind != resp.KindError {
		t.Errorf("expected error when persistence disabled, got %v", v)
	}
}

// TestPersistenceMultipleDatabases checks that the databases survive a
// restart: the RDB stores them under their own SELECTDB opcode and the AOF
// records the SELECT commands.
func TestPersistenceMultipleDatabases(t *testing.T) {
	for _, mode := range []string{"rdb", "aof"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			useAOF := mode == "aof"
			addr, dbPath, aofPath := startPersistServerDBs(t, dir, useAOF, 3)
			c := newClient(t, addr)
			c.ok("SET", "k", "db0")
			c.ok("SELECT", "1")
			c.ok("SET", "k", "db1")
			c.ok("SELECT", "2")
			c.ok("SET", "k", "db2")

			path := dbPath
			if useAOF {
				path = aofPath
				time.Sleep(20 * time.Millisecond)
			} else {
				c.ok("SAVE")
			}

			addr2 := startFromFilesDBs(t, path, useAOF, 3)
			c2 := newClient(t, addr2)
			for db, want := range []string{"db0", "db1", "db2"} {
				c2.ok("SELECT", strconv.Itoa(db))
				if got := string(c2.bulkWant("GET", "k")); got != want {
					t.Errorf("db%d after reload: got %q, want %q", db, got, want)
				}
			}
		})
	}
}

// TestPersistenceDatabaseCount checks that a snapshot with more databases
// than the server has does not break the load: the extra ones are ignored.
func TestPersistenceDatabaseCount(t *testing.T) {
	dir := t.TempDir()
	addr, dbPath, _ := startPersistServerDBs(t, dir, false, 3)
	c := newClient(t, addr)
	c.ok("SELECT", "2")
	c.ok("SET", "k", "db2")
	c.ok("SAVE")

	// Reload into a server with a single database.
	addr2 := startFromFilesDBs(t, dbPath, false, 1)
	c2 := newClient(t, addr2)
	if v := c2.cmd("DBSIZE"); v.Kind != resp.KindInteger || v.Int != 0 {
		t.Errorf("DBSIZE in a 1-database server: got %v, want 0", v)
	}
}
