package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/amir/redis-implements/internal/app"
	"github.com/amir/redis-implements/internal/config"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	go func() {
		sig := <-sigs
		log.Printf("server: received %v, shutting down", sig)
		cancel()
	}()

	if err := app.Run(ctx, cfg, log.Default()); err != nil {
		log.Fatalf("%v", err)
	}
}
