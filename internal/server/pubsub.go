package server

import (
	"sync"
	"sync/atomic"

	"github.com/amir/redis-implements/internal/resp"
)

// PubSub is the publish/subscribe hub. It tracks channel subscribers and
// pattern (glob) subscribers. Delivery never blocks the publisher: messages
// go into each subscriber's bounded queue; a subscriber that falls behind is
// disconnected (like Redis dropping a client with a full output buffer).
type PubSub struct {
	mu       sync.RWMutex
	channels map[string]map[*client]struct{}
	patterns map[string]map[*client]struct{}
	messages atomic.Uint64 // messages published
}

// NewPubSub returns an empty hub.
func NewPubSub() *PubSub {
	return &PubSub{
		channels: make(map[string]map[*client]struct{}),
		patterns: make(map[string]map[*client]struct{}),
	}
}

// subscribe adds the client to a channel.
func (ps *PubSub) subscribe(c *client, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	subs, ok := ps.channels[channel]
	if !ok {
		subs = make(map[*client]struct{})
		ps.channels[channel] = subs
	}
	subs[c] = struct{}{}
	c.channels[channel] = struct{}{}
}

// unsubscribe removes the client from a channel. ok is false when the client
// was not subscribed.
func (ps *PubSub) unsubscribe(c *client, channel string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	subs, ok := ps.channels[channel]
	if !ok {
		return false
	}
	if _, ok := subs[c]; !ok {
		return false
	}
	delete(subs, c)
	delete(c.channels, channel)
	if len(subs) == 0 {
		delete(ps.channels, channel)
	}
	return true
}

// subscribePattern adds the client to a glob pattern.
func (ps *PubSub) subscribePattern(c *client, pattern string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	subs, ok := ps.patterns[pattern]
	if !ok {
		subs = make(map[*client]struct{})
		ps.patterns[pattern] = subs
	}
	subs[c] = struct{}{}
	c.patterns[pattern] = struct{}{}
}

// unsubscribePattern removes the client from a glob pattern. ok is false when
// the client was not subscribed.
func (ps *PubSub) unsubscribePattern(c *client, pattern string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	subs, ok := ps.patterns[pattern]
	if !ok {
		return false
	}
	if _, ok := subs[c]; !ok {
		return false
	}
	delete(subs, c)
	delete(c.patterns, pattern)
	if len(subs) == 0 {
		delete(ps.patterns, pattern)
	}
	return true
}

// removeClient drops the client from every channel and pattern (connection
// cleanup).
func (ps *PubSub) removeClient(c *client) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for ch := range c.channels {
		if subs, ok := ps.channels[ch]; ok {
			delete(subs, c)
			if len(subs) == 0 {
				delete(ps.channels, ch)
			}
		}
	}
	c.channels = make(map[string]struct{})
	for p := range c.patterns {
		if subs, ok := ps.patterns[p]; ok {
			delete(subs, c)
			if len(subs) == 0 {
				delete(ps.patterns, p)
			}
		}
	}
	c.patterns = make(map[string]struct{})
}

// publish delivers payload on channel to all channel subscribers and to all
// clients whose patterns match. It returns the number of deliveries.
func (ps *PubSub) publish(channel string, payload []byte) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	ps.messages.Add(1)
	count := 0
	if subs, ok := ps.channels[channel]; ok {
		frame := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("message")),
			resp.NewBulkString([]byte(channel)),
			resp.NewBulkString(payload),
		})
		for c := range subs {
			if deliver(c, frame) {
				count++
			}
		}
	}
	for pattern, subs := range ps.patterns {
		if !globMatch(pattern, channel) {
			continue
		}
		frame := resp.NewArray([]resp.Value{
			resp.NewBulkString([]byte("pmessage")),
			resp.NewBulkString([]byte(pattern)),
			resp.NewBulkString([]byte(channel)),
			resp.NewBulkString(payload),
		})
		for c := range subs {
			if deliver(c, frame) {
				count++
			}
		}
	}
	return count
}

// Counts returns the number of active channels and patterns.
func (ps *PubSub) Counts() (channels, patterns int) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.channels), len(ps.patterns)
}

// clientSubs returns the client's subscription counts (read under ps.mu,
// which guards the maps holding the client's membership).
func (ps *PubSub) clientSubs(c *client) (channels, patterns int) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(c.channels), len(c.patterns)
}

// Messages returns the total number of publish operations.
func (ps *PubSub) Messages() uint64 { return ps.messages.Load() }

// deliver queues the frame for the subscriber's pump goroutine. It returns
// false when the queue is full and the slow subscriber was disconnected.
func deliver(c *client, frame resp.Value) bool {
	select {
	case c.msgCh <- frame:
		return true
	default:
		// Slow subscriber: drop the connection, like Redis does.
		c.conn.Close()
		return false
	}
}
