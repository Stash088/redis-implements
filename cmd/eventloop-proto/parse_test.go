package main

import "testing"

func TestParse(t *testing.T) {
	buf := []byte("*1\r\n$4\r\nPING\r\n")
	args, used, ok, err := parseCommand(buf, nil)
	t.Logf("args=%q used=%d ok=%v err=%v", args, used, ok, err)
	if ok {
		t.Logf("reply=%q", execute(&kv{m: map[string]string{}}, args, nil))
	}
}
