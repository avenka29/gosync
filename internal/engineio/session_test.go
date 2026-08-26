package engineio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestSessionHandshakeHeartbeatAndMessaging(t *testing.T) {
	t.Parallel()

	transport := newFakeTransport()
	timers := newManualTimerFactory()
	inbound := make(chan Packet, 1)
	session, err := NewSession(SessionConfig{
		ID:           "engine-session",
		PingInterval: 25 * time.Second,
		PingTimeout:  20 * time.Second,
		MaxPayload:   1024,
		newTimer:     timers.New,
	}, transport, func(_ context.Context, packet Packet) error {
		inbound <- packet
		return nil
	})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	runResult := make(chan error, 1)
	go func() { runResult <- session.Run(context.Background()) }()

	handshakeFrame := receiveFrame(t, transport.writes)
	handshakePacket, err := DecodeFrame(handshakeFrame)
	if err != nil {
		t.Fatalf("DecodeFrame(handshake) error = %v", err)
	}
	if handshakePacket.Type != PacketOpen {
		t.Fatalf("handshake type = %s, want open", handshakePacket.Type)
	}
	var opened handshake
	if err := json.Unmarshal(handshakePacket.Data, &opened); err != nil {
		t.Fatalf("handshake JSON error = %v", err)
	}
	if opened.SID != "engine-session" || opened.PingInterval != 25_000 || opened.PingTimeout != 20_000 || opened.MaxPayload != 1024 {
		t.Fatalf("handshake = %#v", opened)
	}
	if opened.Upgrades == nil || len(opened.Upgrades) != 0 {
		t.Fatalf("handshake upgrades = %#v, want empty array", opened.Upgrades)
	}

	pingTimer := timers.Receive(t)
	pingTimer.Fire()
	pingFrame := receiveFrame(t, transport.writes)
	pingPacket, err := DecodeFrame(pingFrame)
	if err != nil || pingPacket.Type != PacketPing {
		t.Fatalf("heartbeat packet = %#v, error = %v", pingPacket, err)
	}
	pongTimeout := timers.Receive(t)
	transport.reads <- Frame{Payload: []byte("3")}
	pingTimer.ExpectReset(t, 25*time.Second)
	if pongTimeout.Active() {
		t.Fatal("pong timeout remained active after receiving pong")
	}

	transport.reads <- Frame{Payload: []byte("4inbound")}
	select {
	case packet := <-inbound:
		if packet.Type != PacketMessage || string(packet.Data) != "inbound" {
			t.Fatalf("handler packet = %#v", packet)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for inbound message")
	}

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- session.Send(context.Background(), Packet{Type: PacketMessage, Data: []byte("outbound")})
	}()
	outboundFrame := receiveFrame(t, transport.writes)
	if got := string(outboundFrame.Payload); got != "4outbound" {
		t.Fatalf("outbound frame = %q, want %q", got, "4outbound")
	}
	if err := <-sendResult; err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	transport.reads <- Frame{Payload: []byte("1")}
	if err := receiveError(t, runResult); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if err := session.Send(context.Background(), Packet{Type: PacketMessage}); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Send() after close error = %v, want ErrSessionClosed", err)
	}
}

func TestSessionHeartbeatTimeout(t *testing.T) {
	t.Parallel()

	transport := newFakeTransport()
	timers := newManualTimerFactory()
	session, err := NewSession(SessionConfig{ID: "sid", newTimer: timers.New}, transport, nil)
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- session.Run(context.Background()) }()
	receiveFrame(t, transport.writes) // open

	pingTimer := timers.Receive(t)
	pingTimer.Fire()
	receiveFrame(t, transport.writes) // ping
	pongTimeout := timers.Receive(t)
	pongTimeout.Fire()

	if err := receiveError(t, result); !errors.Is(err, ErrHeartbeatTimeout) {
		t.Fatalf("Run() error = %v, want ErrHeartbeatTimeout", err)
	}
}

func TestSessionRejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	transport := newFakeTransport()
	timers := newManualTimerFactory()
	session, err := NewSession(SessionConfig{ID: "sid", MaxPayload: 4, newTimer: timers.New}, transport, nil)
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- session.Run(context.Background()) }()
	receiveFrame(t, transport.writes)
	timers.Receive(t)
	transport.reads <- Frame{Payload: []byte("4large")}

	if err := receiveError(t, result); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Run() error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestSessionConfigurationAndRunOnce(t *testing.T) {
	t.Parallel()

	if _, err := NewSession(SessionConfig{}, nil, nil); !errors.Is(err, ErrInvalidSessionConfig) {
		t.Fatalf("NewSession() error = %v, want ErrInvalidSessionConfig", err)
	}

	transport := newFakeTransport()
	timers := newManualTimerFactory()
	session, err := NewSession(SessionConfig{ID: "sid", newTimer: timers.New}, transport, nil)
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	transport.reads <- Frame{Payload: []byte("1")}
	if err := session.Run(context.Background()); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if err := session.Run(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("second Run() error = %v, want ErrSessionClosed", err)
	}
}

type fakeTransport struct {
	reads  chan Frame
	writes chan Frame
	closed chan struct{}
	once   sync.Once
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		reads:  make(chan Frame, 16),
		writes: make(chan Frame, 16),
		closed: make(chan struct{}),
	}
}

func (t *fakeTransport) Read(ctx context.Context) (Frame, error) {
	select {
	case frame := <-t.reads:
		return Frame{Payload: append([]byte(nil), frame.Payload...), Binary: frame.Binary}, nil
	case <-t.closed:
		return Frame{}, io.EOF
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}

func (t *fakeTransport) Write(ctx context.Context, frame Frame) error {
	copyOfFrame := Frame{Payload: append([]byte(nil), frame.Payload...), Binary: frame.Binary}
	select {
	case t.writes <- copyOfFrame:
		return nil
	case <-t.closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *fakeTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

type manualTimerFactory struct {
	created chan *manualTimer
}

func newManualTimerFactory() *manualTimerFactory {
	return &manualTimerFactory{created: make(chan *manualTimer, 16)}
}

func (f *manualTimerFactory) New(duration time.Duration) sessionTimer {
	timer := &manualTimer{
		channel: make(chan time.Time, 1),
		resets:  make(chan time.Duration, 4),
		active:  true,
	}
	f.created <- timer
	return timer
}

func (f *manualTimerFactory) Receive(t *testing.T) *manualTimer {
	t.Helper()
	select {
	case timer := <-f.created:
		return timer
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for timer creation")
		return nil
	}
}

type manualTimer struct {
	channel chan time.Time
	resets  chan time.Duration
	mu      sync.Mutex
	active  bool
}

func (t *manualTimer) Channel() <-chan time.Time {
	return t.channel
}

func (t *manualTimer) Reset(duration time.Duration) {
	t.mu.Lock()
	t.active = true
	t.mu.Unlock()
	t.resets <- duration
}

func (t *manualTimer) Stop() {
	t.mu.Lock()
	t.active = false
	t.mu.Unlock()
}

func (t *manualTimer) Fire() {
	t.channel <- time.Now()
}

func (t *manualTimer) Active() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

func (t *manualTimer) ExpectReset(testingT *testing.T, duration time.Duration) {
	testingT.Helper()
	select {
	case got := <-t.resets:
		if got != duration {
			testingT.Fatalf("timer reset duration = %v, want %v", got, duration)
		}
	case <-time.After(time.Second):
		testingT.Fatal("timed out waiting for timer reset")
	}
}

func receiveFrame(t *testing.T, frames <-chan Frame) Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for frame")
		return Frame{}
	}
}

func receiveError(t *testing.T, errorsChannel <-chan error) error {
	t.Helper()
	select {
	case err := <-errorsChannel:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
		return nil
	}
}

func TestHandshakeEncodingIsStable(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(handshake{SID: "sid", Upgrades: []string{}, PingInterval: 1, PingTimeout: 2, MaxPayload: 3})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	want := []byte(`{"sid":"sid","upgrades":[],"pingInterval":1,"pingTimeout":2,"maxPayload":3}`)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("handshake JSON = %s, want %s", encoded, want)
	}
}
