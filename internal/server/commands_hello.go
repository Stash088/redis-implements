package server

import (
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
)

// serverVersion is the version reported by HELLO and INFO.
const serverVersion = "1.0.0"

// cmdHello negotiates the protocol version and answers with the server's
// description. HELLO 3 switches the connection to RESP3, HELLO 2 (or no
// argument) keeps RESP2.
//
// The reply is a map in RESP3 and a flat array in RESP2, like Redis does.
func cmdHello(ctx *Ctx) error {
	c := ctx.Client
	proto := 2
	args := ctx.Args[1:]
	if len(args) > 0 {
		if n, err := strconv.Atoi(string(args[0])); err == nil {
			if n < 2 || n > 3 {
				return ctx.W.WriteError("NOPROTO unsupported protocol version")
			}
			proto = n
			args = args[1:]
		}
	}
	// The optional AUTH and SETNAME arguments, like Redis accepts.
	for len(args) > 0 {
		switch strings.ToLower(string(args[0])) {
		case "auth":
			if len(args) < 3 {
				return ctx.W.WriteError("ERR Protocol error: unexpected argument type")
			}
			if err := ctx.Srv.authenticate(c, string(args[1]), string(args[2])); err != nil {
				return ctx.W.WriteError(err.Error())
			}
			args = args[3:]
		case "setname":
			if len(args) < 2 {
				return ctx.W.WriteError("ERR Protocol error: unexpected argument type")
			}
			c.setName(string(args[1]))
			args = args[2:]
		default:
			return ctx.W.WriteError("ERR Protocol error: unexpected argument type")
		}
	}
	c.proto.Store(int32(proto))

	fields := []resp.Value{
		resp.NewBulkString([]byte("server")), resp.NewBulkString([]byte("redis-implements")),
		resp.NewBulkString([]byte("version")), resp.NewBulkString([]byte(serverVersion)),
		resp.NewBulkString([]byte("proto")), resp.NewInteger(int64(proto)),
		resp.NewBulkString([]byte("id")), resp.NewInteger(int64(c.id)),
		resp.NewBulkString([]byte("mode")), resp.NewBulkString([]byte("standalone")),
		resp.NewBulkString([]byte("role")), resp.NewBulkString([]byte("master")),
		resp.NewBulkString([]byte("modules")), resp.NewArray(nil),
	}
	if proto >= 3 {
		return ctx.W.WriteValue(resp.NewMap(fields))
	}
	return ctx.W.WriteValue(resp.NewArray(fields))
}
