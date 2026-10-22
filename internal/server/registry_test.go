package server

import (
	"testing"
)

// aofWriteCommandList is the set of commands that mutate the dataset and are
// therefore propagated to the AOF. It guards the command metadata: a command
// that mutates the dataset but is missing here would silently lose data on
// AOF replay.
var aofWriteCommandList = []string{
	"append", "decr", "decrby", "del", "expire", "expireat", "flushall",
	"flushdb", "hdel", "hset", "incr", "incrby", "lpop", "lpush", "lset",
	"hgetdel", "hincrby", "hincrbyfloat", "hmset", "hsetnx", "incrbyfloat",
	"linsert", "lmove", "lrem", "ltrim", "lpushx", "persist", "pexpire", "pexpireat",
	"rpop", "rpoplpush", "rpush", "rpushx", "sadd", "sdiffstore", "set", "sinterstore",
	"smove", "spop", "srem", "sunionstore", "unlink", "zadd", "zincrby", "zrem",
}

// memoryAddCommandList is the set of commands that can grow the dataset; they
// are denied under noeviction once the store is over maxmemory.
var memoryAddCommandList = []string{
	"append", "decr", "decrby", "hincrby", "hincrbyfloat", "hmset", "hset",
	"hsetnx", "incr", "incrby", "incrbyfloat", "lpush", "lpushx", "lset",
	"rpush", "rpushx", "sadd", "set", "zadd", "zincrby",
}

func newTestRegistry() map[string]cmdSpec {
	srv := &Server{cmds: make(map[string]cmdSpec)}
	srv.registerCommands()
	return srv.cmds
}

func TestCommandRegistryArity(t *testing.T) {
	for name, spec := range newTestRegistry() {
		if spec.handler == nil {
			t.Errorf("command %q has no handler", name)
		}
		if spec.arity == 0 {
			t.Errorf("command %q has no arity", name)
		}
	}
}

// TestCommandRegistryWriteSet checks that exactly the expected commands are
// propagated to the AOF.
func TestCommandRegistryWriteSet(t *testing.T) {
	cmds := newTestRegistry()
	want := make(map[string]bool, len(aofWriteCommandList))
	for _, n := range aofWriteCommandList {
		want[n] = true
	}
	for name, spec := range cmds {
		if spec.write != want[name] {
			t.Errorf("command %q: write=%v, want %v", name, spec.write, want[name])
		}
	}
	for _, n := range aofWriteCommandList {
		if _, ok := cmds[n]; !ok {
			t.Errorf("write command %q is not registered", n)
		}
	}
}

// TestCommandRegistryMemoryAddSet checks the commands denied under noeviction.
func TestCommandRegistryMemoryAddSet(t *testing.T) {
	cmds := newTestRegistry()
	want := make(map[string]bool, len(memoryAddCommandList))
	for _, n := range memoryAddCommandList {
		want[n] = true
	}
	for name, spec := range cmds {
		if spec.memAdd != want[name] {
			t.Errorf("command %q: memAdd=%v, want %v", name, spec.memAdd, want[name])
		}
	}
}

// TestCommandRegistryAofSelf checks that the commands propagating themselves
// to the AOF are exactly those whose AOF rewrite depends on whether the
// command took effect.
func TestCommandRegistryAofSelf(t *testing.T) {
	cmds := newTestRegistry()
	want := map[string]bool{"set": true, "expire": true, "pexpire": true}
	for name, spec := range cmds {
		if spec.aofSelf != want[name] {
			t.Errorf("command %q: aofSelf=%v, want %v", name, spec.aofSelf, want[name])
		}
	}
}
