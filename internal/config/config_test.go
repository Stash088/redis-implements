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
