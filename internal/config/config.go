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

	// Persistence.
	AppendOnly     bool
	AppendFsync    string
	DBFilename     string
	AppendFilename string
	SaveSeconds    int
	SaveChanges    int
}

// Addr returns the "host:port" listen address.
func (c Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Parse reads configuration from command-line flags.
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("redis-server", flag.ContinueOnError)
	c := Config{
		Host:           "127.0.0.1",
		Port:           6379,
		AppendFsync:    "everysec",
		DBFilename:     "dump.rdb",
		AppendFilename: "appendonly.aof",
	}
	fs.StringVar(&c.Host, "host", c.Host, "interface to bind to")
	fs.IntVar(&c.Port, "port", c.Port, "TCP port to listen on")
	fs.StringVar(&c.Dir, "dir", ".", "working directory for persistence files")
	fs.BoolVar(&c.AppendOnly, "appendonly", c.AppendOnly, "enable the append-only file")
	fs.StringVar(&c.AppendFsync, "appendfsync", c.AppendFsync, "fsync policy: always, everysec or no")
	fs.StringVar(&c.DBFilename, "dbfilename", c.DBFilename, "RDB snapshot file name")
	fs.StringVar(&c.AppendFilename, "appendfilename", c.AppendFilename, "AOF file name")
	fs.IntVar(&c.SaveSeconds, "save", c.SaveSeconds, "seconds between RDB saves (0 disables)")
	fs.IntVar(&c.SaveChanges, "save-changes", c.SaveChanges, "minimum changes to trigger an RDB save")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if c.Port < 1 || c.Port > 65535 {
		return Config{}, fmt.Errorf("invalid port %d", c.Port)
	}
	if c.SaveSeconds < 0 || c.SaveChanges < 0 {
		return Config{}, fmt.Errorf("invalid save policy %d/%d", c.SaveSeconds, c.SaveChanges)
	}
	if c.DBFilename == "" || filepath.Base(c.DBFilename) != c.DBFilename {
		return Config{}, fmt.Errorf("invalid --dbfilename %q", c.DBFilename)
	}
	if c.AppendFilename == "" || filepath.Base(c.AppendFilename) != c.AppendFilename {
		return Config{}, fmt.Errorf("invalid --appendfilename %q", c.AppendFilename)
	}
	switch c.AppendFsync {
	case "always", "everysec", "no":
	default:
		return Config{}, fmt.Errorf("invalid --appendfsync %q (want always, everysec or no)", c.AppendFsync)
	}
	abs, err := filepath.Abs(c.Dir)
	if err != nil {
		return Config{}, fmt.Errorf("resolving --dir: %w", err)
	}
	c.Dir = abs
	return c, nil
}
