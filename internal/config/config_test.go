package config

import "testing"

func TestParseDefaults(t *testing.T) {
	c, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Host != "127.0.0.1" || c.Port != 6379 {
		t.Errorf("got %+v, want default host/port", c)
	}
	if c.Addr() != "127.0.0.1:6379" {
		t.Errorf("Addr: got %q", c.Addr())
	}
}

func TestParseFlags(t *testing.T) {
	c, err := Parse([]string{"--host", "0.0.0.0", "--port", "7777"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr() != "0.0.0.0:7777" {
		t.Errorf("Addr: got %q", c.Addr())
	}
}

func TestParseBadPort(t *testing.T) {
	if _, err := Parse([]string{"--port", "99999"}); err == nil {
		t.Errorf("expected error for out-of-range port")
	}
}

func TestParseResolvesDir(t *testing.T) {
	c, err := Parse([]string{"--dir", "."})
	if err != nil {
		t.Fatal(err)
	}
	if c.Dir == "" || c.Dir == "." {
		t.Errorf("expected --dir to be resolved to an absolute path, got %q", c.Dir)
	}
}

func TestParsePersistenceFlags(t *testing.T) {
	c, err := Parse([]string{
		"--appendonly", "--appendfsync", "always",
		"--dbfilename", "snap.rdb", "--appendfilename", "log.aof",
		"--save", "60", "--save-changes", "1000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !c.AppendOnly || c.AppendFsync != "always" {
		t.Errorf("appendonly=%v fsync=%q", c.AppendOnly, c.AppendFsync)
	}
	if c.DBFilename != "snap.rdb" || c.AppendFilename != "log.aof" {
		t.Errorf("files: %q %q", c.DBFilename, c.AppendFilename)
	}
	if c.SaveSeconds != 60 || c.SaveChanges != 1000 {
		t.Errorf("save policy: %d/%d", c.SaveSeconds, c.SaveChanges)
	}
}

func TestParseBadAppendFsync(t *testing.T) {
	if _, err := Parse([]string{"--appendfsync", "sometimes"}); err == nil {
		t.Error("expected error for invalid appendfsync")
	}
}

func TestParseBadDBFilename(t *testing.T) {
	if _, err := Parse([]string{"--dbfilename", "sub/dir.rdb"}); err == nil {
		t.Error("expected error for path in dbfilename")
	}
}

func TestParseDefaultsPersistence(t *testing.T) {
	c, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.AppendOnly || c.SaveSeconds != 0 {
		t.Errorf("expected persistence disabled by default, got %+v", c)
	}
	if c.AppendFsync != "everysec" || c.DBFilename != "dump.rdb" || c.AppendFilename != "appendonly.aof" {
		t.Errorf("defaults: %+v", c)
	}
}

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1k", 1 << 10},
		{"5kb", 5 << 10},
		{"2m", 2 << 20},
		{"3mb", 3 << 20},
		{"1g", 1 << 30},
		{"10gb", 10 << 30},
		{"1B", 1},
		{" 4 KB ", 4 << 10},
	}
	for _, tc := range cases {
		got, err := ParseBytes(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseBytes(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []string{"", "abc", "-5", "1kbx"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q): expected error", bad)
		}
	}
}

func TestParseMemoryFlags(t *testing.T) {
	c, err := Parse([]string{
		"--maxmemory", "100mb", "--maxmemory-policy", "allkeys-lru",
		"--maxmemory-samples", "10",
		"--slowlog-log-slower-than", "5000", "--slowlog-max-len", "64",
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxMemory != 100<<20 {
		t.Errorf("MaxMemory: got %d, want %d", c.MaxMemory, 100<<20)
	}
	if c.MaxMemoryPolicy != "allkeys-lru" || c.MaxMemorySamples != 10 {
		t.Errorf("policy/samples: %q/%d", c.MaxMemoryPolicy, c.MaxMemorySamples)
	}
	if c.SlowlogSlowerThan != 5000 || c.SlowlogMaxLen != 64 {
		t.Errorf("slowlog: %d/%d", c.SlowlogSlowerThan, c.SlowlogMaxLen)
	}
}

func TestParseBadMemoryFlags(t *testing.T) {
	if _, err := Parse([]string{"--maxmemory-policy", "allkeys-lfu"}); err == nil {
		t.Error("expected error for invalid maxmemory-policy")
	}
	if _, err := Parse([]string{"--maxmemory", "lots"}); err == nil {
		t.Error("expected error for invalid maxmemory value")
	}
	if _, err := Parse([]string{"--maxmemory-samples", "0"}); err == nil {
		t.Error("expected error for zero maxmemory-samples")
	}
}
