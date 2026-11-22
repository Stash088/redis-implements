// Package app composes the server's runtime: the stores, the startup
// restore, the optional AOF, the listeners, the background workers, the
// optional replication link and the optional pprof endpoints.
package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/amir/redis-implements/internal/config"
	"github.com/amir/redis-implements/internal/persist"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// Run composes and runs the server until ctx is canceled or Serve fails. The
// caller owns the process-level concerns: parsing the flags, the signal
// context and turning the returned error into the exit status.
func Run(ctx context.Context, cfg config.Config, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}

	dbs := make([]*store.Store, cfg.Databases)
	for i := range dbs {
		dbs[i] = store.New()
	}

	dbPath := filepath.Join(cfg.Dir, cfg.DBFilename)
	aofPath := filepath.Join(cfg.Dir, cfg.AppendFilename)

	aofExists, err := restoreDataset(cfg, dbs, dbPath, aofPath)
	if err != nil {
		return err
	}

	// Validate the upstream address before acquiring any resource: a typo
	// must not leave an open AOF or a bound listener behind.
	var masterHost string
	var masterPort int
	if cfg.ReplicaOf != "" {
		masterHost, masterPort, err = config.ParseReplicaOf(cfg.ReplicaOf)
		if err != nil {
			return fmt.Errorf("replicaof: %w", err)
		}
	}

	aof, err := openAOF(cfg, dbs, aofPath, aofExists)
	if err != nil {
		return err
	}

	srv, err := newServer(cfg, dbs)
	if err != nil {
		if aof != nil {
			_ = aof.Close()
		}
		return fmt.Errorf("server: failed to listen on %s: %w", cfg.Addr(), err)
	}

	// From here on the server owns the AOF and the expirer. It closes both
	// exactly once, in Shutdown/Close: the composition must not close them
	// itself as well.
	srv.AOF = aof
	srv.DBPath = dbPath
	srv.AOFPath = aofPath
	srv.Expirer = store.NewExpirer(dbs, 0)
	srv.SetLogger(logger)
	srv.SetRequirePass(cfg.RequirePass)
	srv.SetMaxMemory(cfg.MaxMemory)
	srv.SetMaxMemoryPolicy(cfg.MaxMemoryPolicy)
	srv.SetMaxMemorySamples(int64(cfg.MaxMemorySamples))
	srv.SetSlowlog(cfg.SlowlogSlowerThan, cfg.SlowlogMaxLen)
	srv.SetAppendFsync(cfg.AppendFsync)
	srv.SetBufferSizes(cfg.BufReadSize, cfg.BufWriteSize)
	if cfg.ReplBacklogSize > 0 {
		srv.SetReplBacklogSize(cfg.ReplBacklogSize)
	}
	if cfg.MasterAuth != "" {
		srv.SetMasterAuth(cfg.MasterAuth)
	}
	if cfg.ReplicaOf != "" {
		logger.Printf("server: replicating from %s:%d", masterHost, masterPort)
		srv.SetReplicaOf(masterHost, masterPort, cfg.MasterAuth)
	}

	// Starting the workers is the last step that can precede serving: there
	// is no fallible setup left to do after it.
	srv.StartSavePolicy(cfg.SaveSeconds, cfg.SaveChanges)
	srv.Expirer.Start()
	logger.Printf("server: listening on %s (dir=%s)", srv.Addr(), cfg.Dir)

	// An already-canceled context still gets the graceful shutdown: the
	// AOF is flushed and the final RDB save runs. Serve is not started:
	// there is nothing to unblock, and the event-loop mode must not be
	// closed before its loops have taken the listeners.
	if ctx.Err() != nil {
		logger.Printf("server: shutting down")
		if err := srv.Shutdown(); err != nil {
			logger.Printf("server: shutdown: %v", err)
		}
		return nil
	}

	var pprofSrv *http.Server
	if cfg.PprofPort > 0 {
		pprofSrv = startPprof(cfg.PprofPort, logger)
	}
	err = serveUntilCanceled(ctx, srv, logger)
	if pprofSrv != nil {
		_ = pprofSrv.Close()
	}
	return err
}

// restoreDataset loads the startup dataset according to the existing
// precedence: an enabled, present AOF wins; otherwise the RDB is loaded;
// otherwise the stores stay empty. A corrupt AOF is an error, never a reason
// to fall back to the RDB.
func restoreDataset(cfg config.Config, dbs []*store.Store, dbPath, aofPath string) (bool, error) {
	aofExists := fileExists(aofPath)
	switch {
	case cfg.AppendOnly && aofExists:
		if err := loadAOF(aofPath, dbs); err != nil {
			return aofExists, fmt.Errorf("aof load: %w", err)
		}
	case fileExists(dbPath):
		if err := loadRDB(dbPath, dbs); err != nil {
			return aofExists, fmt.Errorf("rdb load: %w", err)
		}
	}
	return aofExists, nil
}

// openAOF opens the append-only file when it is enabled. A fresh AOF is
// written from the dataset loaded from the RDB before the appender opens it:
// an existing AOF is never rewritten at startup.
func openAOF(cfg config.Config, dbs []*store.Store, aofPath string, aofExists bool) (*persist.Appender, error) {
	if !cfg.AppendOnly {
		return nil, nil
	}
	if !aofExists && totalKeys(dbs) > 0 {
		if err := persist.RewriteAOF(aofPath, dbs, dbs[0].Now()); err != nil {
			return nil, fmt.Errorf("aof seed: %w", err)
		}
	}
	aof, err := persist.OpenAppender(aofPath, fsyncPolicy(cfg.AppendFsync))
	if err != nil {
		return nil, fmt.Errorf("aof: %w", err)
	}
	// The commands are written to the file in batches: the interval is the
	// window in which a crash can lose the commands that were not synced
	// yet.
	aof.SetFlushInterval(time.Duration(cfg.AOFFlushMS) * time.Millisecond)
	return aof, nil
}

func newServer(cfg config.Config, dbs []*store.Store) (*server.Server, error) {
	if cfg.IOMode == "loop" {
		return server.NewLoopN(cfg.Addr(), dbs, cfg.Loops)
	}
	return server.New(cfg.Addr(), dbs)
}

// serveUntilCanceled serves the listener and shuts the server down exactly
// once. On cancellation it waits for Shutdown to finish before waiting for
// Serve to observe the closed listeners; on an unexpected Serve return it
// still performs the cleanup.
func serveUntilCanceled(ctx context.Context, srv *server.Server, logger *log.Logger) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve()
	}()

	select {
	case err := <-serveErr:
		// Serve stopped without a cancellation: the listeners are already
		// gone or broken, but the workers and the AOF still need stopping.
		if shutdownErr := srv.Shutdown(); shutdownErr != nil {
			logger.Printf("server: shutdown: %v", shutdownErr)
		}
		if err != nil {
			return fmt.Errorf("server: %w", err)
		}
		return nil
	case <-ctx.Done():
		logger.Printf("server: shutting down")
		shutdownErr := srv.Shutdown()
		// Do not return as soon as Serve unblocks: Shutdown has to finish
		// flushing the AOF and stopping the workers first.
		serveErrAfter := <-serveErr
		if shutdownErr != nil {
			// The process is already going down: keep the old behavior of
			// logging the shutdown failure rather than making it fatal.
			logger.Printf("server: shutdown: %v", shutdownErr)
		}
		if serveErrAfter != nil {
			return fmt.Errorf("server: %w", serveErrAfter)
		}
		return nil
	}
}

func startPprof(port int, logger *log.Logger) *http.Server {
	// Mutex profiling is off by default: without the fraction the
	// /debug/pprof/mutex endpoint always reports an empty profile.
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(100)
	// A nil handler serves the standard mux, where importing net/http/pprof
	// registers the endpoints.
	pprofSrv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", port)}
	go func() {
		logger.Printf("server: pprof on http://127.0.0.1:%d/debug/pprof/", port)
		if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("server: pprof: %v", err)
		}
	}()
	return pprofSrv
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
