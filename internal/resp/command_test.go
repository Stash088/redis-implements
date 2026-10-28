package resp

import (
	"bytes"
	"strings"
	"testing"
)

// TestParseCommandMatchesReadCommand checks that the parser of the buffer and
// the reader of the stream understand the same commands: the server uses the
// reader in the goroutine model and the parser in the event loop.
func TestParseCommandMatchesReadCommand(t *testing.T) {
	commands := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n",
		"*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n",
	}
	for _, cmd := range commands {
		// The stream reader.
		r := NewReader(strings.NewReader(cmd))
		streamArgs, err := r.ReadCommand(nil)
		if err != nil {
			t.Fatalf("%q: ReadCommand: %v", cmd, err)
		}
		// The parser of the buffer.
		bufArgs, n, ok, err := ParseCommand([]byte(cmd), nil)
		if err != nil || !ok {
			t.Fatalf("%q: ParseCommand: %v %v", cmd, err, ok)
		}
		if n != len(cmd) {
			t.Errorf("%q: the command takes %d bytes, want %d", cmd, n, len(cmd))
		}
		if len(bufArgs) != len(streamArgs) {
			t.Fatalf("%q: %d arguments, want %d", cmd, len(bufArgs), len(streamArgs))
		}
		for i := range bufArgs {
			if !bytes.Equal(bufArgs[i], streamArgs[i]) {
				t.Errorf("%q: the argument %d is %q, want %q", cmd, i, bufArgs[i], streamArgs[i])
			}
		}
	}
}

// TestParseCommandIncomplete checks that a partial command is reported as
// such and not as an error: the caller waits for the rest of it.
func TestParseCommandIncomplete(t *testing.T) {
	full := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	for i := 0; i < len(full); i++ {
		args, n, ok, err := ParseCommand([]byte(full[:i]), nil)
		if err != nil {
			t.Errorf("%q: %v", full[:i], err)
			continue
		}
		if ok {
			t.Errorf("%q: the parser accepted a partial command (%d bytes)", full[:i], n)
		}
		if args != nil {
			t.Errorf("%q: the parser returned the arguments of a partial command", full[:i])
		}
	}
}

// TestParseCommandErrors checks the errors of a malformed command.
func TestParseCommandErrors(t *testing.T) {
	cases := []struct {
		in   string
		want error
	}{
		{"+OK\r\n", ErrNotArray},
		{"$-1\r\n", ErrNotArray},
		{"*1\r\n+OK\r\n", ErrNotBulk},
		{"*1\r\n$-1\r\n", ErrNotBulk},
		{"*abc\r\n", ErrInvalid},
		{"*1\r\n$abc\r\n", ErrInvalid},
	}
	for _, tc := range cases {
		_, _, _, err := ParseCommand([]byte(tc.in), nil)
		if err != tc.want {
			t.Errorf("%q: got %v, want %v", tc.in, err, tc.want)
		}
	}
}

// TestParseCommandLimits checks that the limits of the reader hold for the
// parser of the buffer.
func TestParseCommandLimits(t *testing.T) {
	if _, _, _, err := ParseCommandLimits([]byte("*1\r\n$5\r\nhello\r\n"), nil, 4, DefaultMaxArrayLen); err != ErrTooBig {
		t.Errorf("a long argument: got %v, want %v", err, ErrTooBig)
	}
	if _, _, _, err := ParseCommandLimits([]byte("*100\r\n"), nil, DefaultMaxBulkLen, 10); err != ErrTooBig {
		t.Errorf("a long array: got %v, want %v", err, ErrTooBig)
	}
}

// TestParseCommandTwice checks that the parser returns the commands of a
// buffer one by one.
func TestParseCommandTwice(t *testing.T) {
	buf := []byte("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	args, n, ok, err := ParseCommand(buf, nil)
	if err != nil || !ok {
		t.Fatalf("the first command: %v %v", err, ok)
	}
	if len(args) != 1 || string(args[0]) != "PING" {
		t.Fatalf("the first command: %q", args)
	}
	if n != 14 {
		t.Fatalf("the first command takes %d bytes, want 14", n)
	}
	args, n, ok, err = ParseCommand(buf[n:], nil)
	if err != nil || !ok {
		t.Fatalf("the second command: %v %v", err, ok)
	}
	if len(args) != 2 || string(args[0]) != "GET" || string(args[1]) != "k" {
		t.Fatalf("the second command: %q", args)
	}
	if n != 20 {
		t.Fatalf("the second command takes %d bytes, want 20", n)
	}
}
