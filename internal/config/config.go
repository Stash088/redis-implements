package config

import (
	"flag"
	"fmt"
	"path/filepath"
)

// Config holds the server runtime configuration.
type Config struct {
	Host string
	Port int
	Dir  string
}

// Addr returns the "host:port" listen address.
func (c Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Parse reads configuration from command-line flags.
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("redis-server", flag.ContinueOnError)
	c := Config{Host: "127.0.0.1", Port: 6379}
	fs.StringVar(&c.Host, "host", c.Host, "interface to bind to")
	fs.IntVar(&c.Port, "port", c.Port, "TCP port to listen on")
	fs.StringVar(&c.Dir, "dir", ".", "working directory for persistence files")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if c.Port < 1 || c.Port > 65535 {
		return Config{}, fmt.Errorf("invalid port %d", c.Port)
	}
	abs, err := filepath.Abs(c.Dir)
	if err != nil {
		return Config{}, fmt.Errorf("resolving --dir: %w", err)
	}
	c.Dir = abs
	return c, nil
}
