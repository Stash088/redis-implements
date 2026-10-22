package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
)

// cmdCommand answers with the details of the server commands, like Redis
// does: COMMAND COUNT, COMMAND INFO, COMMAND LIST and COMMAND DOCS.
func cmdCommand(ctx *Ctx) error {
	if len(ctx.Args) == 1 {
		return ctx.W.WriteValue(ctx.Srv.commandInfoValue(nil))
	}
	switch strings.ToLower(string(ctx.Args[1])) {
	case "count":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "command count")
			return nil
		}
		return ctx.W.WriteInteger(int64(len(ctx.Srv.cmds)))
	case "info":
		if len(ctx.Args) < 2 {
			wrongArgs(ctx.W, "command info")
			return nil
		}
		names := make([]string, 0, len(ctx.Args)-2)
		for _, a := range ctx.Args[2:] {
			names = append(names, strings.ToLower(string(a)))
		}
		return ctx.W.WriteValue(ctx.Srv.commandInfoValue(names))
	case "list":
		if len(ctx.Args) != 2 {
			wrongArgs(ctx.W, "command list")
			return nil
		}
		return ctx.W.WriteValue(ctx.Srv.commandListValue())
	case "docs":
		if len(ctx.Args) < 2 {
			wrongArgs(ctx.W, "command docs")
			return nil
		}
		names := make([]string, 0, len(ctx.Args)-2)
		for _, a := range ctx.Args[2:] {
			names = append(names, strings.ToLower(string(a)))
		}
		return ctx.Srv.writeCommandDocs(ctx, names)
	default:
		return ctx.W.WriteError("ERR Unknown subcommand or wrong number of arguments for '" + string(ctx.Args[1]) + "'. Try COMMAND HELP.")
	}
}

// commandNames returns the names of all the registered commands, sorted.
func (s *Server) commandNames() []string {
	names := make([]string, 0, len(s.cmds))
	for name := range s.cmds {
		names = append(names, name)
	}
	sortStrings(names)
	return names
}

// commandInfoValue builds the reply of COMMAND INFO: for every name an array
// [name, arity, flags, first-key, last-key, step]. Unknown commands are nil.
func (s *Server) commandInfoValue(names []string) resp.Value {
	if names == nil {
		names = s.commandNames()
	}
	elems := make([]resp.Value, 0, len(names))
	for _, name := range names {
		spec, ok := s.cmds[name]
		if !ok {
			elems = append(elems, resp.NewArray(nil))
			continue
		}
		meta := metaFor(name)
		flags := make([]resp.Value, 0, len(meta.flags))
		for _, f := range meta.flags {
			flags = append(flags, resp.NewBulkString([]byte(f)))
		}
		elems = append(elems, resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte(name)),
			resp.NewInteger(int64(spec.arity)),
			resp.NewSet(flags),
			resp.NewInteger(int64(meta.firstKey)),
			resp.NewInteger(int64(meta.lastKey)),
			resp.NewInteger(int64(meta.step)),
		}))
	}
	return resp.NewArray(elems)
}

// commandListValue builds the reply of COMMAND LIST: the names of all the
// commands.
func (s *Server) commandListValue() resp.Value {
	names := s.commandNames()
	elems := make([]resp.Value, 0, len(names))
	for _, name := range names {
		elems = append(elems, resp.NewBulkString([]byte(name)))
	}
	return resp.NewArray(elems)
}

// writeCommandDocs writes the documentation of the given commands. The reply
// is a map in RESP3 and a flat array in RESP2.
func (s *Server) writeCommandDocs(ctx *Ctx, names []string) error {
	w := ctx.W
	if ctx.proto3() {
		if err := w.WriteMapHeader(len(names)); err != nil {
			return err
		}
	} else if err := w.WriteArrayHeader(len(names) * 2); err != nil {
		return err
	}
	for _, name := range names {
		if err := w.WriteBulkString([]byte(name)); err != nil {
			return err
		}
		if err := s.writeCommandDoc(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// writeCommandDoc writes the documentation of a single command.
func (s *Server) writeCommandDoc(ctx *Ctx, name string) error {
	w := ctx.W
	spec, ok := s.cmds[name]
	if !ok {
		return w.WriteNullArray()
	}
	meta := metaFor(name)
	fields := []resp.Value{
		resp.NewBulkString([]byte("summary")), resp.NewBulkString([]byte(meta.summary)),
		resp.NewBulkString([]byte("since")), resp.NewBulkString([]byte(serverVersion)),
		resp.NewBulkString([]byte("group")), resp.NewBulkString([]byte(meta.group)),
		resp.NewBulkString([]byte("complexity")), resp.NewBulkString([]byte("O(1)")),
		resp.NewBulkString([]byte("arity")), resp.NewInteger(int64(spec.arity)),
		resp.NewBulkString([]byte("key_specs")), resp.NewArray(nil),
	}
	if ctx.proto3() {
		return w.WriteValue(resp.NewMap(fields))
	}
	return w.WriteValue(resp.NewArray(fields))
}

// sortStrings sorts a slice of strings in place.
func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// formatInt renders an int for the INFO output.
func formatInt(n int64) string { return strconv.FormatInt(n, 10) }
