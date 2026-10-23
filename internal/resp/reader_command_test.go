package resp

import (
	"errors"
	"strings"
	"testing"
)

func TestReadCommand(t *testing.T) {
	r := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
	args, err := r.ReadCommand(nil)
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	want := []string{"SET", "key", "value"}
	if len(args) != len(want) {
		t.Fatalf("got %d arguments, want %d", len(args), len(want))
	}
	for i, a := range args {
		if string(a) != want[i] {
			t.Errorf("argument %d: got %q, want %q", i, a, want[i])
		}
	}
}

func TestReadCommandEmpty(t *testing.T) {
	r := NewReader(strings.NewReader("*0\r\n"))
	args, err := r.ReadCommand(nil)
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("got %d arguments, want none", len(args))
	}
}

func TestReadCommandNotArray(t *testing.T) {
	r := NewReader(strings.NewReader("+PING\r\n"))
	if _, err := r.ReadCommand(nil); !errors.Is(err, ErrNotArray) {
		t.Errorf("got %v, want ErrNotArray", err)
	}
}

func TestReadCommandNullArray(t *testing.T) {
	r := NewReader(strings.NewReader("*-1\r\n"))
	if _, err := r.ReadCommand(nil); !errors.Is(err, ErrNotArray) {
		t.Errorf("got %v, want ErrNotArray", err)
	}
}

func TestReadCommandArgumentNotBulk(t *testing.T) {
	r := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n:42\r\n"))
	if _, err := r.ReadCommand(nil); !errors.Is(err, ErrNotBulk) {
		t.Errorf("got %v, want ErrNotBulk", err)
	}
}

func TestReadCommandNullArgument(t *testing.T) {
	r := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$-1\r\n"))
	if _, err := r.ReadCommand(nil); !errors.Is(err, ErrNotBulk) {
		t.Errorf("got %v, want ErrNotBulk", err)
	}
}

// TestReadCommandReusesBuffer checks that the argument slice is reused: the
// server reads one command after another without allocating a slice each
// time.
func TestReadCommandReusesBuffer(t *testing.T) {
	r := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n"))
	var dst [][]byte
	dst, err := r.ReadCommand(dst)
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	first := &dst[0]
	dst, err = r.ReadCommand(dst)
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if &dst[0] != first {
		t.Error("the argument buffer was not reused")
	}
	if string(dst[1]) != "b" {
		t.Errorf("got %q, want b", dst[1])
	}
}
