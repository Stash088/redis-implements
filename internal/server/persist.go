package server

import (
	"io"
	"time"

	"github.com/amir/redis-implements/internal/persist"
)

// StartSavePolicy launches a background goroutine that saves the RDB when at
// least changes modifications happened within the last seconds. It is a no-op
// when seconds <= 0 or no RDB path is configured.
func (s *Server) StartSavePolicy(seconds, changes int) {
	s.saveSeconds = seconds
	s.saveChanges = changes
	if seconds <= 0 || s.DBPath == "" {
		return
	}
	s.saveStop = make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-s.saveStop:
				return
			case <-t.C:
				if time.Since(time.Unix(s.lastSave.Load(), 0)) >= time.Duration(seconds)*time.Second &&
					s.dirty.Load() >= int64(changes) {
					saveMu := &s.saveMu
					saveMu.Lock()
					err := s.saveRDB()
					saveMu.Unlock()
					if err == nil {
						s.dirty.Store(0)
						s.lastSave.Store(time.Now().Unix())
					}
				}
			}
		}
	}()
}

func (s *Server) saveRDB() error {
	if s.DBPath == "" {
		return nil
	}
	return persist.Save(s.DBPath, s.Store)
}

// Shutdown performs a final RDB save (if changes are pending) and closes the
// server: save policy, expirer, AOF and the listener. It is used for graceful
// container shutdown; closing the listener makes Serve return.
func (s *Server) Shutdown() error {
	if s.DBPath != "" && s.dirty.Load() > 0 {
		s.saveMu.Lock()
		err := s.saveRDB()
		s.saveMu.Unlock()
		if err != nil {
			s.Logger.Printf("server: shutdown save: %v", err)
		} else {
			s.dirty.Store(0)
			s.lastSave.Store(time.Now().Unix())
		}
	}
	return s.Close()
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
	ctx.Srv.dirty.Store(0)
	ctx.Srv.lastSave.Store(time.Now().Unix())
	return ctx.W.WriteSimpleString("OK")
}

func cmdBGSAVE(ctx *Ctx) error {
	if ctx.Srv.DBPath == "" {
		return ctx.W.WriteError("ERR persistence is disabled")
	}
	if !ctx.Srv.saveMu.TryLock() {
		return ctx.W.WriteError("ERR Background save already in progress")
	}
	go func() {
		defer ctx.Srv.saveMu.Unlock()
		if err := ctx.Srv.saveRDB(); err != nil {
			ctx.Srv.Logger.Printf("server: bgsave: %v", err)
			return
		}
		ctx.Srv.dirty.Store(0)
		ctx.Srv.lastSave.Store(time.Now().Unix())
	}()
	return ctx.W.WriteSimpleString("Background saving started")
}

func cmdBGREWRITEAOF(ctx *Ctx) error {
	if ctx.Srv.AOF == nil {
		return ctx.W.WriteError("ERR Append Only File is disabled")
	}
	if !ctx.Srv.aofMu.TryLock() {
		return ctx.W.WriteError("ERR Background append only file rewriting already in progress")
	}
	go func() {
		defer ctx.Srv.aofMu.Unlock()
		if err := ctx.Srv.AOF.Rewrite(func(w io.Writer) error {
			return persist.DatasetCommands(w, ctx.Srv.Store, ctx.Srv.Store.Now())
		}); err != nil {
			ctx.Srv.Logger.Printf("server: bgrewriteaof: %v", err)
		}
	}()
	return ctx.W.WriteSimpleString("Background append only file rewriting started")
}
