package socketio

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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

// Session converts Engine.IO messages into complete Socket.IO packets.
type Session struct {
	codec   Codec
	handler PacketHandler
	sender  PacketSender

	sendMu       sync.Mutex
	pending      *Packet
	buffers      [][]byte
	maxBytes     int64
	pendingBytes int64
	broken       atomic.Bool
	onFailure    func()
}

// NewSession constructs a Socket.IO packet session.
func NewSession(codec Codec, sender PacketSender, handler PacketHandler) (*Session, error) {
	if sender == nil {
		return nil, errors.New("socketio: packet sender is required")
	}
	if handler == nil {
		handler = func(context.Context, Packet) error { return nil }
	}

	return &Session{codec: codec, sender: sender, handler: handler, maxBytes: 1_000_000}, nil
}

// Handle accepts one Engine.IO message packet.
func (s *Session) Handle(ctx context.Context, packet engineio.Packet) error {
	if int64(len(packet.Data)) > s.maxBytes {
		return engineio.ErrPayloadTooLarge
	}
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
		s.pendingBytes = int64(len(packet.Data))
		s.buffers = make([][]byte, 0, decoded.Attachments)
		return nil
	}

	return s.handler(ctx, decoded)
}

// Send atomically writes one Socket.IO packet and its binary attachments.
func (s *Session) Send(ctx context.Context, packet Packet) error {
	encoded, err := s.codec.Encode(packet)
	if err != nil {
		return err
	}

	var size int64 = int64(len(encoded.Header))
	for _, b := range encoded.Attachments {
		size += int64(len(b))
	}
	if size > s.maxBytes {
		return engineio.ErrPayloadTooLarge
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.broken.Load() {
		return engineio.ErrSessionClosed
	}

	if err := s.sender(ctx, engineio.Packet{Type: engineio.PacketMessage, Data: encoded.Header}); err != nil {
		s.fail()
		return err
	}
	for _, attachment := range encoded.Attachments {
		if err := s.sender(ctx, engineio.Packet{
			Type:   engineio.PacketMessage,
			Data:   attachment,
			Binary: true,
		}); err != nil {
			s.fail()
			return err
		}
	}
	return nil
}

func (s *Session) handleBinary(ctx context.Context, data []byte) error {
	if s.pending == nil {
		return ErrUnexpectedBinary
	}

	s.pendingBytes += int64(len(data))
	if s.pendingBytes > s.maxBytes {
		return engineio.ErrPayloadTooLarge
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

// SetLimits configures the aggregate packet size before use.
func (s *Session) SetLimits(maxBytes int64) {
	if maxBytes > 0 {
		s.maxBytes = maxBytes
	}
}

// SetOnFailure registers transport cleanup for a partial write.
func (s *Session) SetOnFailure(fn func()) {
	s.onFailure = fn
}

func (s *Session) fail() {
	if s.broken.CompareAndSwap(false, true) && s.onFailure != nil {
		s.onFailure()
	}
}
