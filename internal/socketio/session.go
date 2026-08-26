package socketio

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/avenka29/gosync/internal/engineio"
)

var (
	ErrExpectedTextHeader = errors.New("socketio: expected a text packet header")
	ErrUnexpectedBinary   = errors.New("socketio: unexpected binary attachment")
	ErrInterleavedPacket  = errors.New("socketio: packet received while binary attachments are pending")
)

// PacketHandler receives complete Socket.IO packets sequentially.
type PacketHandler func(context.Context, Packet) error

// PacketSender sends one Engine.IO message packet.
type PacketSender func(context.Context, engineio.Packet) error

// Session converts the ordered Engine.IO message stream into complete Socket.IO
// packets. A Session belongs to exactly one Engine.IO session.
type Session struct {
	codec   Codec
	handler PacketHandler
	sender  PacketSender

	sendMu  sync.Mutex
	pending *Packet
	buffers [][]byte
}

// NewSession constructs a Socket.IO packet session.
func NewSession(codec Codec, sender PacketSender, handler PacketHandler) (*Session, error) {
	if sender == nil {
		return nil, errors.New("socketio: packet sender is required")
	}
	if handler == nil {
		handler = func(context.Context, Packet) error { return nil }
	}

	return &Session{codec: codec, sender: sender, handler: handler}, nil
}

// Handle accepts one Engine.IO message packet. Engine.IO control packets must be
// consumed by the Engine.IO session and never passed here.
func (s *Session) Handle(ctx context.Context, packet engineio.Packet) error {
	if packet.Type != engineio.PacketMessage {
		return fmt.Errorf("%w: got Engine.IO %s packet", ErrExpectedTextHeader, packet.Type)
	}

	if packet.Binary {
		return s.handleBinary(ctx, packet.Data)
	}
	if s.pending != nil {
		return ErrInterleavedPacket
	}

	decoded, err := s.codec.DecodeHeader(packet.Data)
	if err != nil {
		return err
	}
	if decoded.Type.Binary() {
		s.pending = &decoded
		s.buffers = make([][]byte, 0, decoded.Attachments)
		return nil
	}

	return s.handler(ctx, decoded)
}

// Send encodes and atomically writes one Socket.IO packet and all of its binary
// attachments. Concurrent callers cannot interleave packet sequences.
func (s *Session) Send(ctx context.Context, packet Packet) error {
	encoded, err := s.codec.Encode(packet)
	if err != nil {
		return err
	}

	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	if err := s.sender(ctx, engineio.Packet{Type: engineio.PacketMessage, Data: encoded.Header}); err != nil {
		return err
	}
	for _, attachment := range encoded.Attachments {
		if err := s.sender(ctx, engineio.Packet{
			Type:   engineio.PacketMessage,
			Data:   attachment,
			Binary: true,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) handleBinary(ctx context.Context, data []byte) error {
	if s.pending == nil {
		return ErrUnexpectedBinary
	}

	s.buffers = append(s.buffers, cloneBytes(data))
	if len(s.buffers) < s.pending.Attachments {
		return nil
	}

	reconstructed, err := s.codec.Reconstruct(*s.pending, s.buffers)
	s.pending = nil
	s.buffers = nil
	if err != nil {
		return err
	}
	return s.handler(ctx, reconstructed)
}
