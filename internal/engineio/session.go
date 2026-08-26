package engineio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultPingInterval   = 25 * time.Second
	DefaultPingTimeout    = 20 * time.Second
	DefaultMaxPayload     = int64(1_000_000)
	DefaultInboundBuffer  = 64
	DefaultOutboundBuffer = 64
)

var (
	ErrInvalidSessionConfig = errors.New("engineio: invalid session configuration")
	ErrSessionClosed        = errors.New("engineio: session closed")
	ErrPayloadTooLarge      = errors.New("engineio: payload exceeds configured limit")
	ErrInboundQueueFull     = errors.New("engineio: inbound message queue is full")
	ErrHeartbeatTimeout     = errors.New("engineio: heartbeat timeout")
	ErrUnexpectedPacket     = errors.New("engineio: unexpected packet")
)

// Transport carries already-demarcated Engine.IO frames. An implementation must
// support one concurrent reader and one writer, and Close must unblock Read.
type Transport interface {
	Read(context.Context) (Frame, error)
	Write(context.Context, Frame) error
	Close() error
}

// MessageHandler receives Engine.IO message packets sequentially. It must not
// retain packet.Data without copying it. Transport lifecycle processing remains
// independent while the handler runs.
type MessageHandler func(context.Context, Packet) error

// SessionConfig contains validated limits for one physical Engine.IO session.
type SessionConfig struct {
	ID             string
	Upgrades       []string
	PingInterval   time.Duration
	PingTimeout    time.Duration
	MaxPayload     int64
	InboundBuffer  int
	OutboundBuffer int

	newTimer timerFactory
}

// Session owns the lifecycle and ordered writes of one Engine.IO connection.
// Run must be called exactly once.
type Session struct {
	config    SessionConfig
	transport Transport
	handler   MessageHandler

	outbound chan sendRequest
	done     chan struct{}
	runOnce  sync.Once
}

type sendRequest struct {
	packet Packet
	result chan error
}

type readResult struct {
	frame Frame
	err   error
}

type handshake struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int64    `json:"pingInterval"`
	PingTimeout  int64    `json:"pingTimeout"`
	MaxPayload   int64    `json:"maxPayload"`
}

// NewSession validates configuration and constructs an Engine.IO session.
func NewSession(config SessionConfig, transport Transport, handler MessageHandler) (*Session, error) {
	if transport == nil || config.ID == "" {
		return nil, fmt.Errorf("%w: transport and session ID are required", ErrInvalidSessionConfig)
	}
	if config.PingInterval <= 0 {
		config.PingInterval = DefaultPingInterval
	}
	if config.PingTimeout <= 0 {
		config.PingTimeout = DefaultPingTimeout
	}
	if config.MaxPayload <= 0 {
		config.MaxPayload = DefaultMaxPayload
	}
	if config.InboundBuffer <= 0 {
		config.InboundBuffer = DefaultInboundBuffer
	}
	if config.OutboundBuffer <= 0 {
		config.OutboundBuffer = DefaultOutboundBuffer
	}
	if config.Upgrades == nil {
		config.Upgrades = []string{}
	} else {
		config.Upgrades = cloneStrings(config.Upgrades)
	}
	if config.newTimer == nil {
		config.newTimer = newRealTimer
	}
	if handler == nil {
		handler = func(context.Context, Packet) error { return nil }
	}

	return &Session{
		config:    config,
		transport: transport,
		handler:   handler,
		outbound:  make(chan sendRequest, config.OutboundBuffer),
		done:      make(chan struct{}),
	}, nil
}

// Send queues one application message for an ordered transport write.
func (s *Session) Send(ctx context.Context, packet Packet) error {
	if packet.Type != PacketMessage {
		return fmt.Errorf("%w: Send accepts message packets only", ErrUnexpectedPacket)
	}

	request := sendRequest{packet: packet, result: make(chan error, 1)}
	select {
	case s.outbound <- request:
	case <-s.done:
		return ErrSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-request.result:
		return err
	case <-s.done:
		return ErrSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done is closed when Run exits.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// Run performs the handshake and owns session state until closure. Calling Run
// more than once returns ErrSessionClosed.
func (s *Session) Run(parent context.Context) error {
	ran := false
	s.runOnce.Do(func() { ran = true })
	if !ran {
		return ErrSessionClosed
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer close(s.done)
	defer s.transport.Close()

	if err := s.writeHandshake(ctx); err != nil {
		return err
	}

	reads := make(chan readResult, 1)
	go s.readLoop(ctx, reads)

	inbound := make(chan Packet, s.config.InboundBuffer)
	handlerErrors := make(chan error, 1)
	go s.handlerLoop(ctx, inbound, handlerErrors)

	pingTimer := s.config.newTimer(s.config.PingInterval)
	defer pingTimer.Stop()
	var pongTimer sessionTimer
	awaitingPong := false

	for {
		var pongTimeout <-chan time.Time
		if pongTimer != nil {
			pongTimeout = pongTimer.Channel()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-reads:
			if result.err != nil {
				return result.err
			}
			if int64(len(result.frame.Payload)) > s.config.MaxPayload {
				return ErrPayloadTooLarge
			}
			packet, err := DecodeFrame(result.frame)
			if err != nil {
				return err
			}

			switch packet.Type {
			case PacketPong:
				if awaitingPong {
					awaitingPong = false
					if pongTimer != nil {
						pongTimer.Stop()
						pongTimer = nil
					}
					pingTimer.Reset(s.config.PingInterval)
				}
			case PacketMessage:
				select {
				case inbound <- packet:
				default:
					return ErrInboundQueueFull
				}
			case PacketClose:
				return nil
			case PacketNoop:
				// Noop is valid during transport coordination and has no payload.
			default:
				return fmt.Errorf("%w: received %s from client", ErrUnexpectedPacket, packet.Type)
			}
		case request := <-s.outbound:
			request.result <- s.writePacket(ctx, request.packet)
		case <-pingTimer.Channel():
			if awaitingPong {
				return ErrHeartbeatTimeout
			}
			if err := s.writePacket(ctx, Packet{Type: PacketPing}); err != nil {
				return err
			}
			awaitingPong = true
			pongTimer = s.config.newTimer(s.config.PingTimeout)
		case <-pongTimeout:
			return ErrHeartbeatTimeout
		case err := <-handlerErrors:
			return err
		}
	}
}

func (s *Session) writeHandshake(ctx context.Context) error {
	payload, err := json.Marshal(handshake{
		SID:          s.config.ID,
		Upgrades:     cloneStrings(s.config.Upgrades),
		PingInterval: s.config.PingInterval.Milliseconds(),
		PingTimeout:  s.config.PingTimeout.Milliseconds(),
		MaxPayload:   s.config.MaxPayload,
	})
	if err != nil {
		return fmt.Errorf("engineio: encode handshake: %w", err)
	}
	return s.writePacket(ctx, Packet{Type: PacketOpen, Data: payload})
}

func (s *Session) writePacket(ctx context.Context, packet Packet) error {
	frame, err := EncodeFrame(packet)
	if err != nil {
		return err
	}
	return s.transport.Write(ctx, frame)
}

func (s *Session) readLoop(ctx context.Context, results chan<- readResult) {
	for {
		frame, err := s.transport.Read(ctx)
		select {
		case results <- readResult{frame: frame, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *Session) handlerLoop(ctx context.Context, messages <-chan Packet, results chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-messages:
			if err := s.handler(ctx, packet); err != nil {
				select {
				case results <- err:
				case <-ctx.Done():
				}
				return
			}
		}
	}
}

type sessionTimer interface {
	Channel() <-chan time.Time
	Reset(time.Duration)
	Stop()
}

type timerFactory func(time.Duration) sessionTimer

type realTimer struct {
	timer *time.Timer
}

func newRealTimer(duration time.Duration) sessionTimer {
	return &realTimer{timer: time.NewTimer(duration)}
}

func (t *realTimer) Channel() <-chan time.Time {
	return t.timer.C
}

func (t *realTimer) Reset(duration time.Duration) {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(duration)
}

func (t *realTimer) Stop() {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
}

func cloneStrings(source []string) []string {
	cloned := make([]string, len(source))
	copy(cloned, source)
	return cloned
}
