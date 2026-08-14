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
