package gosync

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/avenka29/gosync/internal/socketio"
)

type (
	Ack          func(context.Context, ...any) error
	EventHandler func(context.Context, *Socket, []any, Ack) error
	Middleware   func(context.Context, *Socket) error
)

type Namespace struct {
	name       string
	server     *Server
	mu         sync.RWMutex
	sockets    map[string]*Socket
	handlers   map[string]EventHandler
	middleware []Middleware
	onConnect  func(context.Context, *Socket)
}

func newNamespace(s *Server, name string) *Namespace {
	return &Namespace{
		name:     name,
		server:   s,
		sockets:  make(map[string]*Socket),
		handlers: make(map[string]EventHandler),
	}
}

func (n *Namespace) Name() string {
	return n.name
}

func (n *Namespace) OnConnect(fn func(context.Context, *Socket)) {
	n.mu.Lock()
	n.onConnect = fn
	n.mu.Unlock()
}

func (n *Namespace) Use(m Middleware) {
	if m != nil {
		n.mu.Lock()
		n.middleware = append(n.middleware, m)
		n.mu.Unlock()
	}
}

func (n *Namespace) On(event string, h EventHandler) error {
	if !validEvent(event) || h == nil {
		return ErrInvalidEvent
	}
	n.mu.Lock()
	n.handlers[event] = h
	n.mu.Unlock()
	return nil
}

func (n *Namespace) Emit(ctx context.Context, event string, args ...any) error {
	return n.To().Emit(ctx, event, args...)
}

func (n *Namespace) To(rooms ...string) *Broadcast {
	return &Broadcast{namespace: n, rooms: append([]string(nil), rooms...)}
}

func (n *Namespace) Except(rooms ...string) *Broadcast {
	return n.To().Except(rooms...)
}

func (n *Namespace) Sockets() []*Socket {
	n.mu.RLock()
	defer n.mu.RUnlock()
	sockets := make([]*Socket, 0, len(n.sockets))
	for _, s := range n.sockets {
		sockets = append(sockets, s)
	}
	return sockets
}

// Broadcast is an immutable room selector.
type Broadcast struct {
	local     bool
	namespace *Namespace
	rooms     []string
	except    []string
	sender    string
}

func (b *Broadcast) To(rooms ...string) *Broadcast {
	next := *b
	next.rooms = append(append([]string(nil), b.rooms...), rooms...)
	return &next
}

func (b *Broadcast) Except(rooms ...string) *Broadcast {
	next := *b
	next.except = append(append([]string(nil), b.except...), rooms...)
	return &next
}

func (b *Broadcast) selected() []*Socket {
	var result []*Socket
	for _, s := range b.namespace.Sockets() {
		if s.id == b.sender {
			continue
		}
		s.mu.Lock()
		include := len(b.rooms) == 0
		for _, r := range b.rooms {
			if _, ok := s.rooms[r]; ok {
				include = true
			}
		}
		for _, r := range b.except {
			if _, ok := s.rooms[r]; ok {
				include = false
				break
			}
		}
		closed := s.closed
		s.mu.Unlock()
		if include && !closed {
			result = append(result, s)
		}
	}
	return result
}

func newEventPacket(namespace, event string, args []any, id *uint64) socketio.Packet {
	return socketio.Packet{
		Type:      socketio.PacketEvent,
		Namespace: namespace,
		ID:        id,
		Data:      append([]any{event}, args...),
	}
}

func (b *Broadcast) Emit(ctx context.Context, event string, args ...any) error {
	if !validEvent(event) {
		return ErrInvalidEvent
	}
	var clusterError error
	if broker := b.namespace.server.config.Broker; broker != nil && !b.local {
		message, err := b.clusterMessage(event, args)
		if err != nil {
			return err
		}
		clusterError = broker.Publish(ctx, message)
	}
	b.namespace.server.deliveryMu.Lock()
	defer b.namespace.server.deliveryMu.Unlock()
	b.namespace.server.recovery.broadcast(b, event, args)
	var errs []error
	for _, s := range b.selected() {
		if err := s.Emit(ctx, event, args...); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(append(errs, clusterError)...)
}

// EmitWithAck waits for one acknowledgement from each selected socket.
func (b *Broadcast) EmitWithAck(ctx context.Context, event string, args ...any) (map[string][]any, error) {
	if !validEvent(event) {
		return nil, ErrInvalidEvent
	}
	if b.namespace.server.config.Broker != nil && !b.local {
		return b.clusterAck(ctx, event, args)
	}
	results := make(map[string][]any)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	for _, s := range b.selected() {
		wg.Add(1)
		go func(s *Socket) {
			defer wg.Done()
			data, err := s.EmitWithAck(ctx, event, args...)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			} else {
				results[s.id] = data
			}
		}(s)
	}
	wg.Wait()
	return results, errors.Join(errs...)
}

func validName(name string, namespace bool) bool {
	if name == "" || len(name) > 256 || !utf8.ValidString(name) {
		return false
	}
	if namespace && (name[0] != '/' || strings.ContainsAny(name, ",?#")) {
		return false
	}
	for _, r := range name {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func validEvent(event string) bool {
	if !validName(event, false) {
		return false
	}
	switch event {
	case "connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener":
		return false
	}
	return true
}
