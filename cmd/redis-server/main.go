package main

import (
	"io"
	"log"
	"os"
	"path/filepath"

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

	st := store.New()

	dbPath := filepath.Join(cfg.Dir, cfg.DBFilename)
	aofPath := filepath.Join(cfg.Dir, cfg.AppendFilename)
	aofExists := fileExists(aofPath)

	// Restore the dataset: prefer the AOF when it is enabled and present,
	// otherwise fall back to the RDB snapshot.
	switch {
	case cfg.AppendOnly && aofExists:
		if err := loadAOF(aofPath, st); err != nil {
			log.Fatalf("aof load: %v", err)
		}
	case fileExists(dbPath):
		if err := loadRDB(dbPath, st); err != nil {
			log.Fatalf("rdb load: %v", err)
		}
	}

	var aof *persist.Appender
	if cfg.AppendOnly {
		aof, err = persist.OpenAppender(aofPath, fsyncPolicy(cfg.AppendFsync))
		if err != nil {
			log.Fatalf("aof: %v", err)
		}
		// Seed the AOF with the loaded dataset when it was created fresh.
		if !aofExists && st.Len() > 0 {
			if err := aof.Rewrite(func(w io.Writer) error {
				return persist.DatasetCommands(w, st, st.Now())
			}); err != nil {
				log.Fatalf("aof seed: %v", err)
			}
		}
	}

	exp := store.NewExpirer(st, 0)
	exp.Start()

	srv, err := server.New(cfg.Addr(), st)
	if err != nil {
		log.Fatalf("server: failed to listen on %s: %v", cfg.Addr(), err)
	}
	srv.Expirer = exp
	srv.AOF = aof
	srv.DBPath = dbPath
	srv.AOFPath = aofPath
	srv.StartSavePolicy(cfg.SaveSeconds, cfg.SaveChanges)
	log.Printf("server: listening on %s (dir=%s)", srv.Addr(), cfg.Dir)

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

func loadRDB(path string, st *store.Store) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return persist.Load(f, st)
}

func loadAOF(path string, st *store.Store) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return persist.LoadAOF(f, st)
}
