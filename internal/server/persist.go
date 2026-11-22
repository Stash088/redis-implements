package server

import (
	"io"
	"time"

	"github.com/amir/redis-implements/internal/persist"
)

// StartSavePolicy launches a background goroutine that saves the RDB when at
// least changes modifications happened within the last seconds. It is a no-op
// when seconds <= 0 or no RDB path is configured. Call it before Serve: the
// goroutine is stopped and joined by Close (and Shutdown). The policy is
// startup-only: once a worker has started, later calls are ignored, including
// their settings.
func (s *Server) StartSavePolicy(seconds, changes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.saveStop != nil {
		return
	}
	s.saveSeconds = seconds
	s.saveChanges = changes
	if seconds <= 0 || s.DBPath == "" {
		return
	}
	stop := make(chan struct{})
	s.saveStop = stop
	s.saveWG.Add(1)
	go func() {
		defer s.saveWG.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if time.Since(time.Unix(s.lastSave.Load(), 0)) >= time.Duration(seconds)*time.Second &&
					s.dirtyTotal() >= int64(changes) {
					s.saveMu.Lock()
					err := s.saveRDB()
					s.saveMu.Unlock()
					if err != nil {
						s.Logger.Printf("server: save policy: %v", err)
					}
				}
			}
		}
	}()
}

// stopBackground stops and joins the background workers the server owns: the
// active expiry, the save policy, the replication ping and the link to the
// master. It is called once, after the command ingress has been drained, so
// none of them can touch the dataset while it is saved.
func (s *Server) stopBackground() {
	if s.Expirer != nil {
		s.Expirer.Stop()
	}
	s.mu.Lock()
	stop := s.saveStop
	s.mu.Unlock()
	if stop != nil {
		close(stop)
	}
	s.saveWG.Wait()
	if s.replStop != nil {
		close(s.replStop)
	}
	s.replPingWG.Wait()
	// The link to the master (when this server is a replica) is torn down
	// before the dataset is saved: it would otherwise keep applying the
	// commands of the master.
	if s.repl != nil {
		if link := s.repl.link.Swap(nil); link != nil {
			link.stopAndWait()
		}
	}
}

// saveRDB writes the RDB snapshot. All callers hold saveMu.
//
// The dirty counters are captured and cleared before the snapshot is taken:
// mutations that happen during the save increment the cleared counters and
// stay pending, even if the snapshot happened to include them. This is
// conservative on purpose: the next save simply writes them again. If the
// save fails, the captured counts are added back to the concurrent ones.
func (s *Server) saveRDB() error {
	if s.DBPath == "" {
		return nil
	}
	var captured [counterSlots]int64
	for i := range s.dirty {
		captured[i] = s.dirty[i].Swap(0)
	}
	if err := persist.Save(s.DBPath, s.dbsList()); err != nil {
		for i := range s.dirty {
			if captured[i] != 0 {
				s.dirty[i].Add(captured[i])
			}
		}
		return err
	}
	s.lastSave.Store(time.Now().Unix())
	return nil
}

// Shutdown drains the server, performs a final RDB save (if changes are
// pending) and then flushes and closes the AOF. It is used for graceful
// container shutdown; closing the listener makes Serve return. It is safe to
// call Close as well: the Expirer and the AOF are released exactly once.
func (s *Server) Shutdown() error {
	s.stopOnce.Do(func() { s.stopErr = s.stopServing() })
	s.finalizePersistence(true)
	return s.stopErr
}

// finalizePersistence waits for the background saves and rewrites that the
// commands started, optionally writes the final RDB and releases the AOF.
// The command ingress has been drained by stopServing before this runs, so no
// handler can still use the AOF. finalizeMu serializes Close and Shutdown when
// they are called at the same time.
func (s *Server) finalizePersistence(save bool) {
	s.finalizeMu.Lock()
	defer s.finalizeMu.Unlock()

	// BGSAVE and BGREWRITEAOF take their locks before they spawn their
	// goroutine: taking the locks here waits for the in-flight work.
	s.saveMu.Lock()
	if save && s.DBPath != "" && s.dirtyTotal() > 0 {
		if err := s.saveRDB(); err != nil {
			s.Logger.Printf("server: shutdown save: %v", err)
		}
	}
	s.saveMu.Unlock()

	s.aofMu.Lock()
	if s.AOF != nil {
		if err := s.AOF.Close(); err != nil {
			s.Logger.Printf("server: aof close: %v", err)
		}
		s.AOF = nil
	}
	s.aofMu.Unlock()
}

func cmdSave(ctx *Ctx) error {
	if ctx.Srv.DBPath == "" {
		return ctx.W.WriteError("ERR persistence is disabled")
	}
	ctx.Srv.saveMu.Lock()
	defer ctx.Srv.saveMu.Unlock()
	if err := ctx.Srv.saveRDB(); err != nil {
		return ctx.W.WriteError("ERR " + err.Error())
	}
	return ctx.W.WriteSimpleString("OK")
}

func cmdBGSAVE(ctx *Ctx) error {
	if ctx.Srv.DBPath == "" {
		return ctx.W.WriteError("ERR persistence is disabled")
	}
	srv := ctx.Srv
	if !srv.saveMu.TryLock() {
		return ctx.W.WriteError("ERR Background save already in progress")
	}
	go func() {
		defer srv.saveMu.Unlock()
		if err := srv.saveRDB(); err != nil {
			srv.Logger.Printf("server: bgsave: %v", err)
		}
	}()
	return ctx.W.WriteSimpleString("Background saving started")
}

func cmdBGREWRITEAOF(ctx *Ctx) error {
	if ctx.Srv.AOF == nil {
		return ctx.W.WriteError("ERR Append Only File is disabled")
	}
	srv := ctx.Srv
	aof := srv.AOF
	if !srv.aofMu.TryLock() {
		return ctx.W.WriteError("ERR Background append only file rewriting already in progress")
	}
	go func() {
		defer srv.aofMu.Unlock()
		if err := aof.Rewrite(func(w io.Writer) error {
			return persist.DatasetCommands(w, srv.dbsList(), srv.dbsList()[0].Now())
		}); err != nil {
			srv.Logger.Printf("server: bgrewriteaof: %v", err)
		}
	}()
	return ctx.W.WriteSimpleString("Background append only file rewriting started")
}
