package server_test

import (
	"strings"
	"testing"

	"github.com/amir/redis-implements/internal/resp"
)

// TestIngressLastCommandAcrossTransports checks the last-command bookkeeping
// that the shared command ingress performs: the name is canonical for a
// mixed-case command, lower-cased for an unknown command, and recorded even
// when MULTI queues the command instead of executing it. A second connection
// observes it in CLIENT LIST in both serving models.
func TestIngressLastCommandAcrossTransports(t *testing.T) {
	transports := []struct {
		name  string
		start func(*testing.T) string
	}{
		{"goroutine", startTestServer},
		{"loop", startTestServerLoop},
	}
	for _, tr := range transports {
		t.Run(tr.name, func(t *testing.T) {
			addr := tr.start(t)
			observer := newClient(t, addr)
			worker := newClient(t, addr)
			self := worker.conn.LocalAddr().String()

			check := func(want string) {
				t.Helper()
				list := string(observer.bulkWant("CLIENT", "LIST"))
				line := ""
				for _, l := range strings.Split(list, "\n") {
					if strings.Contains(l, "addr="+self+" ") {
						line = l
						break
					}
				}
				if line == "" {
					t.Fatalf("CLIENT LIST has no line for %s: %q", self, list)
				}
				if !strings.Contains(line, "cmd="+want) {
					t.Errorf("last command: got %q, want cmd=%s", line, want)
				}
			}

			if v := worker.cmd("PiNg"); v.Kind != resp.KindSimpleString || string(v.Str) != "PONG" {
				t.Fatalf("mixed-case PING: got %+v", v)
			}
			check("ping")

			if msg := worker.expectError("NoSuchCmd", "a"); !strings.Contains(msg, "unknown command") {
				t.Fatalf("unknown command: got %q", msg)
			}
			check("nosuchcmd")

			if v := worker.cmd("MULTI"); v.Kind != resp.KindSimpleString {
				t.Fatalf("MULTI: got %+v", v)
			}
			if v := worker.cmd("SeT", "k", "v"); v.Kind != resp.KindSimpleString || string(v.Str) != "QUEUED" {
				t.Fatalf("queued SET: got %+v", v)
			}
			check("set")
			worker.ok("DISCARD")
		})
	}
}
