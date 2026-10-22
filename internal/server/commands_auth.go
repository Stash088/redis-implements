package server

import (
	"errors"
	"strings"
)

// errWrongPass is returned when the credentials do not match.
var errWrongPass = errors.New("WRONGPASS invalid username-password pair or user is disabled.")

// errNoPassword is returned when AUTH is used but no password is configured.
var errNoPassword = errors.New("ERR Client sent AUTH, but no password is set")

// noAuthError is the reply for a command that needs authentication.
const noAuthError = "NOAUTH Authentication required."

// cmdAuth authenticates the connection. It accepts AUTH password and
// AUTH username password; only the "default" user exists.
func cmdAuth(ctx *Ctx) error {
	if len(ctx.Args) < 2 || len(ctx.Args) > 3 {
		wrongArgs(ctx.W, "auth")
		return nil
	}
	var user, pass string
	if len(ctx.Args) == 2 {
		pass = string(ctx.Args[1])
	} else {
		user = string(ctx.Args[1])
		pass = string(ctx.Args[2])
	}
	if err := ctx.Srv.authenticate(ctx.Client, user, pass); err != nil {
		return ctx.W.WriteError(err.Error())
	}
	return ctx.W.WriteSimpleString("OK")
}

// authenticate checks the credentials against the configured password and
// marks the client as authenticated.
func (s *Server) authenticate(c *client, user, pass string) error {
	if user != "" && !strings.EqualFold(user, "default") {
		return errWrongPass
	}
	want := s.requirePass()
	if want == "" {
		return errNoPassword
	}
	if want != pass {
		return errWrongPass
	}
	c.authenticated = true
	return nil
}

// requirePass returns the configured password (empty when authentication is
// disabled).
func (s *Server) requirePass() string {
	if p := s.requirepass.Load(); p != nil {
		return *p
	}
	return ""
}

// SetRequirePass sets the password clients must authenticate with. An empty
// password disables authentication.
func (s *Server) SetRequirePass(pass string) { s.requirepass.Store(&pass) }

// authRequired reports whether clients have to authenticate before running
// commands.
func (s *Server) authRequired() bool { return s.requirePass() != "" }

// authExempt lists the commands a client may run before authenticating.
var authExempt = map[string]bool{
	"auth":  true,
	"hello": true,
	"quit":  true,
	"reset": true,
}
