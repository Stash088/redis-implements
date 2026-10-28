package config

import (
	"flag"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
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

	// Memory management.
	MaxMemory        int64
	MaxMemoryPolicy  string
	MaxMemorySamples int

	// Slow log.
	SlowlogSlowerThan int64 // microseconds, -1 disables
	SlowlogMaxLen     int

	// Databases: how many keyspaces the server offers (Redis default: 16).
	Databases int

	// RequirePass is the password clients must authenticate with.
	RequirePass string

	// PprofPort enables the net/http/pprof endpoints on 127.0.0.1
	// (0 disables them).
	PprofPort int

	// AOFFlushMS is how often the buffered AOF commands are written to the
	// file, in milliseconds. Zero writes them as soon as they arrive.
	AOFFlushMS int

	// BufReadSize and BufWriteSize are the sizes of the socket buffers of
	// a connection, in bytes. Zero means the default.
	BufReadSize  int
	BufWriteSize int
}

// maxMemoryFlag parses --maxmemory accepting Redis-style size suffixes
// (plain bytes, k/kb, m/mb, g/gb — case-insensitive).
type maxMemoryFlag struct{ v *int64 }

func (f *maxMemoryFlag) String() string {
	// The flag package calls String on the zero value of the flag when it
	// prints the usage: a nil pointer must not panic the server.
	if f == nil || f.v == nil {
		return "0"
	}
	return strconv.FormatInt(*f.v, 10)
}

func (f *maxMemoryFlag) Set(s string) error {
	n, err := ParseBytes(s)
	if err != nil {
		return fmt.Errorf("invalid --maxmemory %q", s)
	}
	*f.v = n
	return nil
}

// saveFlag parses --save. Redis accepts an empty value to disable the
// snapshots, and the number of seconds between them.
type saveFlag struct{ v *int }

func (f *saveFlag) String() string {
	if f == nil || f.v == nil {
		return "0"
	}
	return strconv.Itoa(*f.v)
}

func (f *saveFlag) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		// An empty value disables the snapshots, like in Redis.
		*f.v = 0
		return nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 0 {
		return fmt.Errorf("invalid --save %q", s)
	}
	*f.v = n
	return nil
}

// ParseBytes parses a size with optional k/m/g suffix into bytes.
func ParseBytes(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	suffixes := []struct {
		suf string
		m   int64
	}{
		{"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10},
		{"g", 1 << 30}, {"m", 1 << 20}, {"k", 1 << 10}, {"b", 1},
	}
	mult := int64(1)
	for _, sf := range suffixes {
		if strings.HasSuffix(t, sf.suf) && len(t) > len(sf.suf) {
			mult = sf.m
			t = strings.TrimSpace(strings.TrimSuffix(t, sf.suf))
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}

// Addr returns the "host:port" listen address.
func (c Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Parse reads configuration from command-line flags.
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("redis-server", flag.ContinueOnError)
	c := Config{
		Host:              "127.0.0.1",
		Port:              6379,
		AppendFsync:       "everysec",
		DBFilename:        "dump.rdb",
		AppendFilename:    "appendonly.aof",
		MaxMemoryPolicy:   "noeviction",
		MaxMemorySamples:  5,
		SlowlogSlowerThan: 10000,
		SlowlogMaxLen:     128,
		Databases:         16,
	}
	fs.StringVar(&c.Host, "host", c.Host, "interface to bind to")
	fs.IntVar(&c.Port, "port", c.Port, "TCP port to listen on")
	fs.StringVar(&c.Dir, "dir", ".", "working directory for persistence files")
	fs.BoolVar(&c.AppendOnly, "appendonly", c.AppendOnly, "enable the append-only file")
	fs.StringVar(&c.AppendFsync, "appendfsync", c.AppendFsync, "fsync policy: always, everysec or no")
	fs.StringVar(&c.DBFilename, "dbfilename", c.DBFilename, "RDB snapshot file name")
	fs.StringVar(&c.AppendFilename, "appendfilename", c.AppendFilename, "AOF file name")
	fs.Var(&saveFlag{&c.SaveSeconds}, "save", "seconds between RDB saves (0 disables, an empty value disables too)")
	fs.IntVar(&c.SaveChanges, "save-changes", c.SaveChanges, "minimum changes to trigger an RDB save")
	fs.Var(&maxMemoryFlag{&c.MaxMemory}, "maxmemory", "memory limit in bytes (suffixes k/m/g allowed), 0 disables")
	fs.StringVar(&c.MaxMemoryPolicy, "maxmemory-policy", c.MaxMemoryPolicy, "eviction policy: noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl")
	fs.IntVar(&c.MaxMemorySamples, "maxmemory-samples", c.MaxMemorySamples, "keys to sample per eviction cycle")
	fs.Int64Var(&c.SlowlogSlowerThan, "slowlog-log-slower-than", c.SlowlogSlowerThan, "log commands slower than this many microseconds (-1 disables)")
	fs.IntVar(&c.SlowlogMaxLen, "slowlog-max-len", c.SlowlogMaxLen, "number of slow log entries to keep")
	fs.IntVar(&c.Databases, "databases", c.Databases, "number of keyspaces (databases)")
	fs.StringVar(&c.RequirePass, "requirepass", c.RequirePass, "password clients must authenticate with (empty disables it)")
	fs.IntVar(&c.PprofPort, "pprof", c.PprofPort, "port for the pprof endpoints on 127.0.0.1 (0 disables them)")
	fs.IntVar(&c.AOFFlushMS, "aof-flush-interval", c.AOFFlushMS, "how often the buffered AOF commands are written to the file, in milliseconds (0 writes them at once)")
	fs.IntVar(&c.BufReadSize, "buf-read-size", c.BufReadSize, "size of the socket read buffer of a connection, in bytes (0 uses the default)")
	fs.IntVar(&c.BufWriteSize, "buf-write-size", c.BufWriteSize, "size of the socket write buffer of a connection, in bytes (0 uses the default)")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if c.Port < 1 || c.Port > 65535 {
		return Config{}, fmt.Errorf("invalid port %d", c.Port)
	}
	if c.PprofPort < 0 || c.PprofPort > 65535 {
		return Config{}, fmt.Errorf("invalid --pprof port %d", c.PprofPort)
	}
	if c.AOFFlushMS < 0 {
		return Config{}, fmt.Errorf("invalid --aof-flush-interval %d", c.AOFFlushMS)
	}
	if c.BufReadSize < 0 || c.BufWriteSize < 0 {
		return Config{}, fmt.Errorf("invalid buffer size %d/%d", c.BufReadSize, c.BufWriteSize)
	}
	if c.SaveSeconds < 0 || c.SaveChanges < 0 {
		return Config{}, fmt.Errorf("invalid save policy %d/%d", c.SaveSeconds, c.SaveChanges)
	}
	switch c.MaxMemoryPolicy {
	case "noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random", "volatile-ttl":
	default:
		return Config{}, fmt.Errorf("invalid --maxmemory-policy %q", c.MaxMemoryPolicy)
	}
	if c.MaxMemory < 0 {
		return Config{}, fmt.Errorf("invalid --maxmemory %d", c.MaxMemory)
	}
	if c.MaxMemorySamples < 1 {
		return Config{}, fmt.Errorf("invalid --maxmemory-samples %d", c.MaxMemorySamples)
	}
	if c.SlowlogMaxLen < 0 {
		return Config{}, fmt.Errorf("invalid --slowlog-max-len %d", c.SlowlogMaxLen)
	}
	if c.Databases < 1 {
		return Config{}, fmt.Errorf("invalid --databases %d", c.Databases)
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
