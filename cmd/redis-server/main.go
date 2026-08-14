package main

import (
	"log"
	"os"

	"github.com/amir/redis-implements/internal/config"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	st := store.New()
	exp := store.NewExpirer(st, 0)
	exp.Start()

	srv, err := server.New(cfg.Addr(), st)
	if err != nil {
		log.Fatalf("server: failed to listen on %s: %v", cfg.Addr(), err)
	}
	srv.Expirer = exp
	log.Printf("server: listening on %s (dir=%s)", srv.Addr(), cfg.Dir)

	if err := srv.Serve(); err != nil {
		log.Fatalf("server: %v", err)
	}
}
