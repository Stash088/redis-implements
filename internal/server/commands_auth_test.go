package server_test

import (
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/server"
	"github.com/amir/redis-implements/internal/store"
)

// startTestServerWithPassword runs a server that requires the given
// password.
func startTestServerWithPassword(t *testing.T, pass string) (*server.Server, string) {
	t.Helper()
	st := store.New()
	exp := store.NewExpirer([]*store.Store{st}, 10*time.Millisecond)
	exp.Start()
	srv, err := server.New("127.0.0.1:0", []*store.Store{st})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetLogger(log.New(io.Discard, "", 0))
	srv.Expirer = exp
	srv.SetRequirePass(pass)
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, srv.Addr().String()
}

// TestAuthRequired checks that commands are refused until the client
// authenticates.
func TestAuthRequired(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	// No password is configured: everything is allowed.
	if v := c.cmd("PING"); v.Kind != resp.KindSimpleString {
		t.Fatalf("PING without a password: got %+v", v)
	}
}

// TestAuthFlow checks the whole authentication flow on a server with a
// password.
func TestAuthFlow(t *testing.T) {
	srv, addr := startTestServerWithPassword(t, "s3cret")
	_ = srv
	c := newClient(t, addr)

	// Commands are refused before authentication.
	msg := c.expectError("GET", "k")
	if !strings.Contains(msg, "NOAUTH") {
		t.Errorf("GET before AUTH: got %q, want a NOAUTH error", msg)
	}
	// PING is refused as well.
	if msg := c.expectError("PING"); !strings.Contains(msg, "NOAUTH") {
		t.Errorf("PING before AUTH: got %q, want a NOAUTH error", msg)
	}

	// A wrong password is rejected.
	if msg := c.expectError("AUTH", "wrong"); !strings.Contains(msg, "WRONGPASS") {
		t.Errorf("AUTH with a wrong password: got %q, want WRONGPASS", msg)
	}
	// An unknown user is rejected too.
	if msg := c.expectError("AUTH", "admin", "s3cret"); !strings.Contains(msg, "WRONGPASS") {
		t.Errorf("AUTH with an unknown user: got %q, want WRONGPASS", msg)
	}

	// The right password works, and the connection can use commands again.
	if v := c.cmd("AUTH", "s3cret"); v.Kind != resp.KindSimpleString || string(v.Str) != "OK" {
		t.Fatalf("AUTH: got %+v, want +OK", v)
	}
	c.ok("SET", "k", "v")
	if got := string(c.bulkWant("GET", "k")); got != "v" {
		t.Errorf("GET after AUTH: got %q, want v", got)
	}

	// AUTH with an explicit user name works for the default user.
	c2 := newClient(t, addr)
	if v := c2.cmd("AUTH", "default", "s3cret"); v.Kind != resp.KindSimpleString {
		t.Errorf("AUTH default: got %+v, want +OK", v)
	}
}

// TestAuthNoPasswordSet checks the error when AUTH is used but no password is
// configured.
func TestAuthNoPasswordSet(t *testing.T) {
	addr := startTestServer(t)
	c := newClient(t, addr)
	msg := c.expectError("AUTH", "anything")
	if !strings.Contains(msg, "no password is set") {
		t.Errorf("AUTH without a password: got %q", msg)
	}
}

// TestAuthConfigSet checks that the password can be changed at runtime.
func TestAuthConfigSet(t *testing.T) {
	_, addr := startTestServerWithPassword(t, "first")
	c := newClient(t, addr)
	c.ok("AUTH", "first")

	// Change the password: the old one stops working.
	c.ok("CONFIG", "SET", "requirepass", "second")
	if msg := c.expectError("AUTH", "first"); !strings.Contains(msg, "WRONGPASS") {
		t.Errorf("AUTH with the old password: got %q, want WRONGPASS", msg)
	}
	c.ok("AUTH", "second")

	// CONFIG GET shows the current password.
	v := c.arrWant("CONFIG", "GET", "requirepass")
	if len(v) != 2 || string(v[0].Str) != "requirepass" || string(v[1].Str) != "second" {
		t.Errorf("CONFIG GET requirepass: got %+v", v)
	}
}

// TestAuthHello checks the AUTH argument of HELLO.
func TestAuthHello(t *testing.T) {
	_, addr := startTestServerWithPassword(t, "pw")
	c := newClient(t, addr)
	if msg := c.expectError("GET", "k"); !strings.Contains(msg, "NOAUTH") {
		t.Errorf("GET before HELLO AUTH: got %q, msg", msg)
	}
	v := c.cmd("HELLO", "3", "AUTH", "default", "pw")
	if v.Kind != resp.KindMap {
		t.Fatalf("HELLO 3 AUTH: got %+v, want a map", v)
	}
	// The connection is authenticated now.
	c.ok("SET", "k", "v")
}
