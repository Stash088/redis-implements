package persist

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/store"
)

func appendCmds(t *testing.T, a *Appender, cmds [][][]byte) {
	t.Helper()
	for _, c := range cmds {
		if err := a.Append(c); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func TestAOFAppendLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	a, err := OpenAppender(path, FsyncNo)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	appendCmds(t, a, [][][]byte{
		{[]byte("SET"), []byte("s"), []byte("hello")},
		{[]byte("RPUSH"), []byte("list"), []byte("a"), []byte("b")},
		{[]byte("HSET"), []byte("h"), []byte("f"), []byte("v")},
		{[]byte("SADD"), []byte("set"), []byte("m")},
		{[]byte("ZADD"), []byte("z"), []byte("1.5"), []byte("a")},
		{[]byte("PEXPIREAT"), []byte("s"), itoa(now.Add(time.Minute).UnixMilli())},
	})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	st := store.New()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := LoadAOF(f, st); err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}

	if v, ok, _ := st.GetString([]byte("s")); !ok || string(v) != "hello" {
		t.Errorf("s = %q ok=%v", v, ok)
	}
	if d, ok := st.TTL([]byte("s")); !ok || d > time.Minute || d <= 0 {
		t.Errorf("s ttl = %v ok=%v", d, ok)
	}
	if lr, _ := st.ListRange([]byte("list"), 0, -1); len(lr) != 2 || string(lr[1]) != "b" {
		t.Errorf("list = %q", lr)
	}
	if v, _, _ := st.HashGet([]byte("h"), []byte("f")); string(v) != "v" {
		t.Errorf("h.f = %q", v)
	}
	if m, _ := st.SetMembers([]byte("set")); len(m) != 1 {
		t.Errorf("set = %d", len(m))
	}
	if z, _ := st.ZRange([]byte("z"), 0, -1, false); len(z) != 1 || z[0].Score != 1.5 {
		t.Errorf("z = %+v", z)
	}
}

func TestAOFTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aof")
	a, _ := OpenAppender(path, FsyncNo)
	appendCmds(t, a, [][][]byte{
		{[]byte("SET"), []byte("k1"), []byte("v1")},
		{[]byte("SET"), []byte("k2"), []byte("v2")},
	})
	a.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// cut the last command in half
	if err := os.WriteFile(path, data[:len(data)-6], 0o600); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	st := store.New()
	if err := LoadAOF(f, st); err != nil {
		t.Fatalf("truncated tail should load, got: %v", err)
	}
	if _, ok, _ := st.GetString([]byte("k1")); !ok {
		t.Error("k1 should be present")
	}
}

func TestRewriteForAOFSetTTL(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cmds := RewriteForAOF([][]byte{[]byte("SET"), []byte("k"), []byte("v"), []byte("EX"), []byte("10"), []byte("NX")}, now)
	if len(cmds) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(cmds))
	}
	if len(cmds[0]) != 4 || string(cmds[0][0]) != "set" {
		t.Errorf("base cmd = %q", cmds[0])
	}
	if string(cmds[1][0]) != "pexpireat" || string(cmds[1][2]) != string(itoa(now.Add(10*time.Second).UnixMilli())) {
		t.Errorf("pexpireat cmd = %q", cmds[1])
	}

	plain := RewriteForAOF([][]byte{[]byte("SET"), []byte("k"), []byte("v")}, now)
	if len(plain) != 1 || len(plain[0]) != 3 {
		t.Errorf("plain set = %q", plain)
	}

	exp := RewriteForAOF([][]byte{[]byte("EXPIRE"), []byte("k"), []byte("60")}, now)
	if len(exp) != 1 || string(exp[0][0]) != "pexpireat" || string(exp[0][2]) != string(itoa(now.Add(60*time.Second).UnixMilli())) {
		t.Errorf("expire rewrite = %q", exp)
	}

	del := RewriteForAOF([][]byte{[]byte("PEXPIRE"), []byte("k"), []byte("-5")}, now)
	if len(del) != 1 || string(del[0][0]) != "del" {
		t.Errorf("negative ttl rewrite = %q", del)
	}
}

func TestRewriteAOFFullDataset(t *testing.T) {
	st := store.New()
	st.SetString([]byte("str"), []byte("hello"))
	st.SetStringWithTTL([]byte("ttl"), []byte("x"), 5*time.Minute)
	st.ListPush([]byte("list"), false, []byte("a"), []byte("b"))
	st.HashSet([]byte("hash"), []byte("f"), []byte("v"))
	st.SetAdd([]byte("set"), []byte("m1"), []byte("m2"))
	st.ZAdd([]byte("zset"), -3.5, []byte("neg"))

	path := filepath.Join(t.TempDir(), "appendonly.aof")
	now := time.Now()
	if err := RewriteAOF(path, st, now); err != nil {
		t.Fatalf("RewriteAOF: %v", err)
	}

	got := store.New()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := LoadAOF(f, got); err != nil {
		t.Fatalf("LoadAOF after rewrite: %v", err)
	}
	if got.Len() != 6 {
		t.Fatalf("expected 6 keys, got %d", got.Len())
	}
	if v, ok, _ := got.GetString([]byte("str")); !ok || string(v) != "hello" {
		t.Errorf("str = %q", v)
	}
	if d, ok := got.TTL([]byte("ttl")); !ok || d <= 0 || d > 5*time.Minute {
		t.Errorf("ttl ttl = %v ok=%v", d, ok)
	}
	if lr, _ := got.ListRange([]byte("list"), 0, -1); len(lr) != 2 || string(lr[0]) != "a" {
		t.Errorf("list = %q", lr)
	}
	if v, _, _ := got.HashGet([]byte("hash"), []byte("f")); string(v) != "v" {
		t.Errorf("hash = %q", v)
	}
	if m, _ := got.SetMembers([]byte("set")); len(m) != 2 {
		t.Errorf("set = %d", len(m))
	}
	if z, _ := got.ZRange([]byte("zset"), 0, -1, false); len(z) != 1 || z[0].Score != -3.5 {
		t.Errorf("zset = %+v", z)
	}
}

func TestAppenderPolicies(t *testing.T) {
	for _, p := range []FsyncPolicy{FsyncAlways, FsyncEverySec, FsyncNo} {
		path := filepath.Join(t.TempDir(), "aof")
		a, err := OpenAppender(path, p)
		if err != nil {
			t.Fatal(err)
		}
		appendCmds(t, a, [][][]byte{{[]byte("SET"), []byte("k"), []byte("v")}})
		if err := a.Close(); err != nil {
			t.Fatalf("policy %d close: %v", p, err)
		}
		data, _ := os.ReadFile(path)
		if !bytes.Contains(data, []byte("SET")) {
			t.Errorf("policy %d: command missing", p)
		}
	}
}

func TestEncodeCommand(t *testing.T) {
	b := EncodeCommand([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	if !strings.HasPrefix(string(b), "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n") {
		t.Errorf("bad encoding: %q", b)
	}
}
