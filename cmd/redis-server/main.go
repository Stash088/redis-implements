package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/amir/redis-implements/internal/config"
	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// One store per database (keyspace).
	dbs := make([]*store.Store, cfg.Databases)
	for i := range dbs {
		dbs[i] = store.New()
	}

	dbPath := filepath.Join(cfg.Dir, cfg.DBFilename)
	aofPath := filepath.Join(cfg.Dir, cfg.AppendFilename)
	aofExists := fileExists(aofPath)

	// Restore the dataset: prefer the AOF when it is enabled and present,
	// otherwise fall back to the RDB snapshot.
	switch {
	case cfg.AppendOnly && aofExists:
		if err := loadAOF(aofPath, dbs); err != nil {
			log.Fatalf("aof load: %v", err)
		}
	case fileExists(dbPath):
		if err := loadRDB(dbPath, dbs); err != nil {
			log.Fatalf("rdb load: %v", err)
		}
	}

	var aof *persist.Appender
	if cfg.AppendOnly {
		aof, err = persist.OpenAppender(aofPath, fsyncPolicy(cfg.AppendFsync))
		if err != nil {
			log.Fatalf("aof: %v", err)
		}
		// The commands are written to the file in batches: the interval
		// is the window in which a crash can lose the commands that were
		// not synced yet.
		aof.SetFlushInterval(time.Duration(cfg.AOFFlushMS) * time.Millisecond)
		// Seed the AOF with the loaded dataset when it was created fresh.
		if !aofExists && totalKeys(dbs) > 0 {
			if err := aof.Rewrite(func(w io.Writer) error {
				return persist.DatasetCommands(w, dbs, dbs[0].Now())
			}); err != nil {
				log.Fatalf("aof seed: %v", err)
			}
		}
	}

	exp := store.NewExpirer(dbs, 0)
	exp.Start()

	var srv *server.Server
	if cfg.IOMode == "loop" {
		srv, err = server.NewLoop(cfg.Addr(), dbs)
	} else {
		srv, err = server.New(cfg.Addr(), dbs)
	}
	if err != nil {
		log.Fatalf("server: failed to listen on %s: %v", cfg.Addr(), err)
	}
	srv.Expirer = exp
	srv.AOF = aof
	srv.DBPath = dbPath
	srv.AOFPath = aofPath
	srv.StartSavePolicy(cfg.SaveSeconds, cfg.SaveChanges)
	srv.SetRequirePass(cfg.RequirePass)
	srv.SetMaxMemory(cfg.MaxMemory)
	srv.SetMaxMemoryPolicy(cfg.MaxMemoryPolicy)
	srv.SetMaxMemorySamples(int64(cfg.MaxMemorySamples))
	srv.SetSlowlog(cfg.SlowlogSlowerThan, cfg.SlowlogMaxLen)
	srv.SetAppendFsync(cfg.AppendFsync)
	srv.SetBufferSizes(cfg.BufReadSize, cfg.BufWriteSize)
	log.Printf("server: listening on %s (dir=%s)", srv.Addr(), cfg.Dir)

	if cfg.PprofPort > 0 {
		// The pprof endpoints make it possible to profile the running
		// server under load, not only the Go benchmarks. Mutex profiling
		// is off by default: without the fraction the /debug/pprof/mutex
		// endpoint always reports an empty profile.
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(100)
		go func() {
			log.Printf("server: pprof on http://127.0.0.1:%d/debug/pprof/", cfg.PprofPort)
			if err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", cfg.PprofPort), nil); err != nil {
				log.Printf("server: pprof: %v", err)
			}
		}()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigs
		log.Printf("server: received %v, shutting down", sig)
		if err := srv.Shutdown(); err != nil {
			log.Printf("server: shutdown: %v", err)
		}
	}()

	if err := srv.Serve(); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func fsyncPolicy(s string) persist.FsyncPolicy {
	switch s {
	case "always":
		return persist.FsyncAlways
	case "no":
		return persist.FsyncNo
	default:
		return persist.FsyncEverySec
	}
}

func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir()
}

func loadRDB(path string, dbs []*store.Store) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return persist.Load(f, dbs)
}

func loadAOF(path string, dbs []*store.Store) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return persist.LoadAOF(f, dbs)
}

// totalKeys returns the number of live keys across all databases.
func totalKeys(dbs []*store.Store) int {
	n := 0
	for _, db := range dbs {
		n += db.Len()
	}
	return n
}
