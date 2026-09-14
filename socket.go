package gosync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/avenka29/gosync/internal/socketio"
)

type ackResult struct {
	arguments []any
}

const maxAcknowledgementID = 1<<53 - 1

// Socket represents one client connection to one namespace.
type Socket struct {
	deliveryMu   sync.Mutex
	pid          string
	recovered    bool
	id           string
	namespace    *Namespace
	connection   *connection
	ctx          context.Context
	cancel       context.CancelFunc
	auth         any
	mu           sync.Mutex
	closed       bool
	rooms        map[string]struct{}
	handlers     map[string]EventHandler
	onDisconnect func(string)
	nextID       uint64
	pending      map[uint64]chan ackResult
}

func (s *Socket) Recovered() bool {
	return s.recovered
}

func (s *Socket) ID() string {
	return s.id
}

func (s *Socket) Namespace() *Namespace {
	return s.namespace
}

func (s *Socket) Context() context.Context {
	return s.ctx
}

// Auth returns the decoded namespace connection payload.
func (s *Socket) Auth() any {
	return s.auth
}

// Request returns a body-free copy of the Engine.IO handshake request.
func (s *Socket) Request() *http.Request {
	return s.connection.request.Clone(s.ctx)
}

func (s *Socket) On(event string, h EventHandler) error {
	if !validEvent(event) || h == nil {
		return ErrInvalidEvent
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrConnectionClosed
	}
	s.handlers[event] = h
	return nil
}

func (s *Socket) OnDisconnect(fn func(string)) {
	s.mu.Lock()
	s.onDisconnect = fn
	s.mu.Unlock()
}

func (s *Socket) Join(rooms ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrConnectionClosed
	}
	added := make(map[string]struct{})
	for _, r := range rooms {
		if !validName(r, false) {
			return ErrInvalidRoom
		}
		if _, ok := s.rooms[r]; !ok {
			added[r] = struct{}{}
		}
	}
	if len(s.rooms)+len(added) > s.connection.server.config.MaxRoomsPerSocket {
		return ErrRoomLimit
	}
	for r := range added {
		s.rooms[r] = struct{}{}
	}
	return nil
}

func (s *Socket) Leave(rooms ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range rooms {
		delete(s.rooms, r)
	}
}

func (s *Socket) Rooms() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	rooms := make([]string, 0, len(s.rooms))
	for r := range s.rooms {
		rooms = append(rooms, r)
	}
	sort.Strings(rooms)
	return rooms
}

func (s *Socket) To(rooms ...string) *Broadcast {
	b := s.namespace.To(rooms...)
	b.sender = s.id
	return b
}

func (s *Socket) Broadcast() *Broadcast {
	return s.To()
}

func (s *Socket) Emit(ctx context.Context, event string, args ...any) error {
	if !validEvent(event) {
		return ErrInvalidEvent
	}
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	packet, err := s.connection.server.recovery.record(s.pid, newEventPacket(s.namespace.name, event, args, nil), s.connection.server.codec)
	if err != nil {
		return err
	}
	return s.sendPacket(ctx, packet)
}

func (s *Socket) send(ctx context.Context, p socketio.Packet) error {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	return s.sendPacket(ctx, p)
}

func (s *Socket) sendPacket(ctx context.Context, p socketio.Packet) error {
	if s.ctx.Err() != nil {
		return ErrConnectionClosed
	}
	p.Namespace = s.namespace.name
	combined, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	return s.connection.session.Send(combined, p)
}

func (s *Socket) EmitWithAck(ctx context.Context, event string, args ...any) ([]any, error) {
	if !validEvent(event) {
		return nil, ErrInvalidEvent
	}
	ctx, cancel := context.WithTimeout(ctx, s.connection.server.config.AckTimeout)
	defer cancel()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if len(s.pending) >= s.connection.server.config.MaxPendingAcks {
		s.mu.Unlock()
		return nil, ErrAckLimit
	}
	id := s.nextAcknowledgementID()
	result := make(chan ackResult, 1)
	s.pending[id] = result
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}()
	if err := s.send(ctx, newEventPacket(s.namespace.name, event, args, &id)); err != nil {
		return nil, err
	}
	select {
	case acknowledgement := <-result:
		return acknowledgement.arguments, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, ErrConnectionClosed
	}
}

func (s *Socket) nextAcknowledgementID() uint64 {
	for {
		id := s.nextID
		s.nextID++
		if s.nextID > maxAcknowledgementID {
			s.nextID = 0
		}
		if s.pending[id] == nil {
			return id
		}
	}
}

func (s *Socket) acknowledge(p socketio.Packet) {
	if p.ID == nil {
		return
	}
	s.mu.Lock()
	result := s.pending[*p.ID]
	delete(s.pending, *p.ID)
	s.mu.Unlock()
	if result != nil {
		result <- ackResult{arguments: p.Data.([]any)}
	}
}

func (s *Socket) dispatch(p socketio.Packet) error {
	args := p.Data.([]any)
	event := args[0].(string)
	if !validEvent(event) {
		_ = s.connection.server.engine.Disconnect(s.connection.id)
		return ErrInvalidEvent
	}
	s.mu.Lock()
	h := s.handlers[event]
	s.mu.Unlock()
	if h == nil {
		s.namespace.mu.RLock()
		h = s.namespace.handlers[event]
		s.namespace.mu.RUnlock()
	}
	if h == nil {
		return nil
	}
	var ack Ack
	if p.ID != nil {
		var used atomic.Bool
		ack = func(ctx context.Context, data ...any) error {
			if !used.CompareAndSwap(false, true) {
				return ErrAlreadyAcknowledged
			}
			if data == nil {
				data = []any{}
			}
			return s.send(ctx, socketio.Packet{Type: socketio.PacketAck, ID: p.ID, Data: data})
		}
	}
	return h(s.ctx, s, args[1:], ack)
}

// Disconnect leaves the namespace and optionally closes the transport.
func (s *Socket) Disconnect(ctx context.Context, closeTransport bool) error {
	err := s.send(ctx, socketio.Packet{Type: socketio.PacketDisconnect})
	s.close("server namespace disconnect")
	if closeTransport {
		return errors.Join(err, s.connection.server.engine.Disconnect(s.connection.id))
	}
	return err
}

func (s *Socket) close(reason string) {
	s.connection.server.deliveryMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.connection.server.deliveryMu.Unlock()
		return
	}
	s.closed = true
	rooms := make([]string, 0, len(s.rooms))
	for room := range s.rooms {
		rooms = append(rooms, room)
	}
	s.connection.server.recovery.detach(s, rooms, reason == "transport close" && s.connection.server.ctx.Err() == nil)
	s.rooms = make(map[string]struct{})
	s.pending = make(map[uint64]chan ackResult)
	fn := s.onDisconnect
	s.mu.Unlock()
	s.cancel()
	s.namespace.mu.Lock()
	delete(s.namespace.sockets, s.id)
	s.namespace.mu.Unlock()
	s.connection.mu.Lock()
	if s.connection.sockets[s.namespace.name] == s {
		delete(s.connection.sockets, s.namespace.name)
	}
	s.connection.mu.Unlock()
	s.connection.server.deliveryMu.Unlock()
	if fn != nil {
		func() {
			defer func() {
				if p := recover(); p != nil {
					s.connection.server.report(fmt.Errorf("gosync: disconnect callback panic: %v", p))
				}
			}()
			fn(reason)
		}()
	}
}
