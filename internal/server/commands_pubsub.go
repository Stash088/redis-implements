package server

import (
	"github.com/amir/redis-implements/internal/resp"
)

// allowedInSubscribeMode lists the only commands a subscribed client may run.
var allowedInSubscribeMode = map[string]bool{
	"subscribe":    true,
	"unsubscribe":  true,
	"psubscribe":   true,
	"punsubscribe": true,
	"ping":         true,
	"quit":         true,
}

// subCount is the total number of the client's channel and pattern
// subscriptions, used as the third element of subscription confirmations.
func (c *client) subCount() int {
	return len(c.channels) + len(c.patterns)
}

// writeSubConfirm writes ["kind", channel, count] like Redis subscription
// confirmations. In RESP3 the confirmation is a push message.
func writeSubConfirm(w *resp.Writer, proto int, kind string, channel []byte, n int) error {
	if proto >= 3 {
		if err := w.WritePushHeader(3); err != nil {
			return err
		}
	} else if err := w.WriteArrayHeader(3); err != nil {
		return err
	}
	if err := w.WriteBulkString([]byte(kind)); err != nil {
		return err
	}
	if err := w.WriteBulkString(channel); err != nil {
		return err
	}
	return w.WriteInteger(int64(n))
}

func cmdSubscribe(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "subscribe")
		return nil
	}
	c := ctx.Client
	c.startPump(ctx.W)
	for _, ch := range ctx.Args[1:] {
		ctx.Srv.PubSub.subscribe(c, string(ch))
		if err := writeSubConfirm(ctx.W, c.Proto(), "subscribe", ch, c.subCount()); err != nil {
			return err
		}
	}
	return nil
}

func cmdUnsubscribe(ctx *Ctx) error {
	c := ctx.Client
	channels := ctx.Args[1:]
	if len(channels) == 0 {
		for ch := range c.channels {
			channels = append(channels, []byte(ch))
		}
	}
	if len(channels) == 0 {
		if c.Proto() >= 3 {
			if err := ctx.W.WritePushHeader(3); err != nil {
				return err
			}
		} else if err := ctx.W.WriteArrayHeader(3); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("unsubscribe")); err != nil {
			return err
		}
		if err := ctx.W.WriteNullBulk(); err != nil {
			return err
		}
		return ctx.W.WriteInteger(0)
	}
	for _, ch := range channels {
		ctx.Srv.PubSub.unsubscribe(c, string(ch))
		if err := writeSubConfirm(ctx.W, c.Proto(), "unsubscribe", ch, c.subCount()); err != nil {
			return err
		}
	}
	return nil
}

func cmdPSubscribe(ctx *Ctx) error {
	if len(ctx.Args) < 2 {
		wrongArgs(ctx.W, "psubscribe")
		return nil
	}
	c := ctx.Client
	c.startPump(ctx.W)
	for _, p := range ctx.Args[1:] {
		ctx.Srv.PubSub.subscribePattern(c, string(p))
		if err := writeSubConfirm(ctx.W, c.Proto(), "psubscribe", p, c.subCount()); err != nil {
			return err
		}
	}
	return nil
}

func cmdPUnsubscribe(ctx *Ctx) error {
	c := ctx.Client
	patterns := ctx.Args[1:]
	if len(patterns) == 0 {
		for p := range c.patterns {
			patterns = append(patterns, []byte(p))
		}
	}
	if len(patterns) == 0 {
		if c.Proto() >= 3 {
			if err := ctx.W.WritePushHeader(3); err != nil {
				return err
			}
		} else if err := ctx.W.WriteArrayHeader(3); err != nil {
			return err
		}
		if err := ctx.W.WriteBulkString([]byte("punsubscribe")); err != nil {
			return err
		}
		if err := ctx.W.WriteNullBulk(); err != nil {
			return err
		}
		return ctx.W.WriteInteger(0)
	}
	for _, p := range patterns {
		ctx.Srv.PubSub.unsubscribePattern(c, string(p))
		if err := writeSubConfirm(ctx.W, c.Proto(), "punsubscribe", p, c.subCount()); err != nil {
			return err
		}
	}
	return nil
}

func cmdPublish(ctx *Ctx) error {
	if len(ctx.Args) != 3 {
		wrongArgs(ctx.W, "publish")
		return nil
	}
	n := ctx.Srv.PubSub.publish(string(ctx.Args[1]), ctx.Args[2])
	return ctx.W.WriteInteger(int64(n))
}
