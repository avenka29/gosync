package gosync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestMalformedPacketsAndAssemblyLimits(t *testing.T) {
	for name, packet := range map[string]string{
		"deep JSON":               `42["event",` + strings.Repeat("[", 8) + `0` + strings.Repeat("]", 8) + `]`,
		"signed attachments":      `45+1-["event",{"_placeholder":true,"num":0}]`,
		"bad placeholder":         `451-["event",{"_placeholder":true,"num":2}]`,
		"unsafe integer":          `429007199254740992["event"]`,
		"reserved event":          `42["disconnect"]`,
		"binary assembly timeout": `451-["event",{"_placeholder":true,"num":0}]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, url := testServer(t, Config{MaxJSONDepth: 4, AssemblyTimeout: 20 * time.Millisecond})
			c := dialTest(t, url)
			connectTest(t, c, "/")
			writeTest(t, c, packet)
			if _, _, err := c.ReadMessage(); err == nil {
				t.Fatal("invalid/incomplete packet accepted")
			}
		})
	}
}

func TestConnectionAdmissionAndNamespaceLimits(t *testing.T) {
	s, url := testServer(t, Config{MaxConnections: 1, MaxNamespacesPerConnection: 1})
	if _, err := s.Of("/other"); err != nil {
		t.Fatal(err)
	}
	first := dialTest(t, url)
	connectTest(t, first, "/")
	second, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil {
		defer second.Close()
		_ = second.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := second.ReadMessage(); err == nil {
			t.Fatal("connection cap bypassed")
		}
	}
	if s.SessionCount() != 1 {
		t.Fatal(s.SessionCount())
	}
	writeTest(t, first, "40/other,")
	if readTest(t, first).Data.(map[string]any)["message"] != "Namespace limit reached" {
		t.Fatal("namespace cap bypassed")
	}
}

func TestPublicLifecycleAndCallbacks(t *testing.T) {
	reports := make(chan error, 5)
	s, url := testServer(t, Config{OnError: func(err error) { reports <- err }})
	if _, err := s.Of(""); err != nil {
		t.Fatal(err)
	}
	s.Default().Use(nil)
	if s.Default().Name() != "/" {
		t.Fatal("namespace name")
	}
	if err := s.Default().On("disconnect", func(context.Context, *Socket, []any, Ack) error { return nil }); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	c := dialTest(t, url)
	connectTest(t, c, "/")
	socket := s.Default().Sockets()[0]
	if socket.Namespace() != s.Default() || socket.Context().Err() != nil || socket.Request().URL.Query().Get("EIO") != "4" {
		t.Fatal("socket metadata")
	}
	if err := socket.On("local", func(ctx context.Context, socket *Socket, args []any, ack Ack) error { return socket.Emit(ctx, "reply") }); err != nil {
		t.Fatal(err)
	}
	writeTest(t, c, `42["local"]`)
	if readTest(t, c).Data.([]any)[0] != "reply" {
		t.Fatal("local handler")
	}
	if err := socket.Join("a", "b"); err != nil {
		t.Fatal(err)
	}
	socket.Leave("a")
	if len(socket.Rooms()) != 2 {
		t.Fatal(socket.Rooms())
	}
	if err := s.Default().Except("a").To("b").Emit(context.Background(), "target"); err != nil {
		t.Fatal(err)
	}
	_ = readTest(t, c)
	if err := s.Default().Emit(context.Background(), "all"); err != nil {
		t.Fatal(err)
	}
	_ = readTest(t, c)
	if err := socket.Broadcast().Emit(context.Background(), "excluded-self"); err != nil {
		t.Fatal(err)
	}
	if err := socket.On("disconnect", nil); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if err := socket.Emit(context.Background(), "disconnect"); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err := socket.EmitWithAck(context.Background(), "disconnect"); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	socket.OnDisconnect(func(string) { panic("disconnect panic") })
	if err := socket.Disconnect(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reports:
	case <-time.After(time.Second):
		t.Fatal("panic not reported")
	}
	if err := socket.On("event", func(context.Context, *Socket, []any, Ack) error { return nil }); !errors.Is(err, ErrConnectionClosed) {
		t.Fatal(err)
	}
	if _, err := socket.EmitWithAck(context.Background(), "event"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatal(err)
	}
}

func TestHandlerPanicAndCooperativeShutdown(t *testing.T) {
	s, url := testServer(t, Config{})
	if err := s.Default().On("panic", func(context.Context, *Socket, []any, Ack) error { panic("application panic") }); err != nil {
		t.Fatal(err)
	}
	c := dialTest(t, url)
	connectTest(t, c, "/")
	writeTest(t, c, `42["panic"]`)
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("panic connection open")
	}
	s.Default().OnConnect(func(ctx context.Context, _ *Socket) { <-ctx.Done() })
	c = dialTest(t, url)
	connectTest(t, c, "/")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Of("/new"); !errors.Is(err, ErrServerClosed) {
		t.Fatal(err)
	}
	if s.SessionCount() != 0 {
		t.Fatal("sessions retained")
	}
}

func TestAckLimitAndEventQueueBackpressure(t *testing.T) {
	s, url := testServer(t, Config{MaxPendingAcks: 1, InboundBuffer: 1, AckTimeout: time.Second})
	c := dialTest(t, url)
	connectTest(t, c, "/")
	socket := s.Default().Sockets()[0]
	done := make(chan error, 1)
	go func() { _, err := socket.EmitWithAck(context.Background(), "first"); done <- err }()
	_ = readTest(t, c)
	if _, err := socket.EmitWithAck(context.Background(), "second"); !errors.Is(err, ErrAckLimit) {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	if err := socket.On("block", func(ctx context.Context, _ *Socket, _ []any, _ Ack) error {
		close(entered)
		<-ctx.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	writeTest(t, c, `42["block"]`)
	<-entered
	for i := 0; i < 20; i++ {
		if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`42["overflow",%d]`, i))); err != nil {
			break
		}
	}
	if _, _, err := c.ReadMessage(); err == nil {
		t.Fatal("overloaded connection open")
	}
	if err := <-done; err == nil {
		t.Fatal("pending ack not cancelled")
	}
}

type closeErrorBroker struct {
	closeCalls atomic.Int32
	err        error
}

func (*closeErrorBroker) Start(context.Context, string, func(ClusterMessage)) error { return nil }
func (*closeErrorBroker) Publish(context.Context, ClusterMessage) error             { return nil }
func (*closeErrorBroker) Nodes(context.Context) ([]string, error)                   { return nil, nil }

func (broker *closeErrorBroker) Close(context.Context) error {
	if broker.closeCalls.Add(1) == 1 {
		return broker.err
	}
	return nil
}

func TestCloseDrainsConnectionsAfterBrokerError(t *testing.T) {
	brokerError := errors.New("broker close failed")
	broker := &closeErrorBroker{err: brokerError}
	server, url := testServer(t, Config{Broker: broker})
	connection := dialTest(t, url)
	connectTest(t, connection, "/")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(ctx); !errors.Is(err, brokerError) {
		t.Fatalf("Close() error = %v", err)
	}
	if server.SessionCount() != 0 {
		t.Fatalf("SessionCount() = %d", server.SessionCount())
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("connection remained open")
	}
}

func TestRoomErrorsAreStable(t *testing.T) {
	server, url := testServer(t, Config{MaxRoomsPerSocket: 1})
	connection := dialTest(t, url)
	connectTest(t, connection, "/")
	socket := server.Default().Sockets()[0]
	if err := socket.Join(""); !errors.Is(err, ErrInvalidRoom) {
		t.Fatalf("Join(empty) error = %v", err)
	}
	if err := socket.Join("extra"); !errors.Is(err, ErrRoomLimit) {
		t.Fatalf("Join(extra) error = %v", err)
	}
}

func TestConfigurationRejectsEveryNegativeLimit(t *testing.T) {
	tests := map[string]func(*Config){
		"assembly timeout": func(config *Config) { config.AssemblyTimeout = -1 },
		"ping interval":    func(config *Config) { config.PingInterval = -1 },
		"ping timeout":     func(config *Config) { config.PingTimeout = -1 },
		"upgrade timeout":  func(config *Config) { config.UpgradeTimeout = -1 },
		"connect timeout":  func(config *Config) { config.ConnectTimeout = -1 },
		"ack timeout":      func(config *Config) { config.AckTimeout = -1 },
		"payload":          func(config *Config) { config.MaxPayload = -1 },
		"inbound buffer":   func(config *Config) { config.InboundBuffer = -1 },
		"outbound buffer":  func(config *Config) { config.OutboundBuffer = -1 },
		"read buffer":      func(config *Config) { config.ReadBufferSize = -1 },
		"write buffer":     func(config *Config) { config.WriteBufferSize = -1 },
		"attachments":      func(config *Config) { config.MaxAttachments = -1 },
		"JSON depth":       func(config *Config) { config.MaxJSONDepth = -1 },
		"connections":      func(config *Config) { config.MaxConnections = -1 },
		"namespaces":       func(config *Config) { config.MaxNamespacesPerConnection = -1 },
		"rooms":            func(config *Config) { config.MaxRoomsPerSocket = -1 },
		"acks":             func(config *Config) { config.MaxPendingAcks = -1 },
	}
	for name, makeInvalid := range tests {
		t.Run(name, func(t *testing.T) {
			config := Config{}
			makeInvalid(&config)
			if _, err := NewServer(config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewServer() error = %v", err)
			}
		})
	}
	for name, recovery := range map[string]RecoveryConfig{
		"duration": {MaxDisconnectionDuration: -1},
		"sessions": {MaxSessions: -1},
		"packets":  {MaxPacketsPerSession: -1},
		"bytes":    {MaxBytes: -1},
	} {
		t.Run("recovery "+name, func(t *testing.T) {
			if _, err := NewServer(Config{Recovery: &recovery}); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewServer() error = %v", err)
			}
		})
	}
}

func TestRoomDeduplicationAndAtomicLimit(t *testing.T) {
	server, url := testServer(t, Config{MaxRoomsPerSocket: 2})
	connection := dialTest(t, url)
	connectTest(t, connection, "/")
	socket := server.Default().Sockets()[0]
	if err := socket.Join("room", "room"); err != nil {
		t.Fatal(err)
	}
	if rooms := socket.Rooms(); len(rooms) != 2 {
		t.Fatalf("Rooms() = %v", rooms)
	}
	if err := socket.Join("room"); err != nil {
		t.Fatal(err)
	}
	if err := socket.Join("overflow", "room"); !errors.Is(err, ErrRoomLimit) {
		t.Fatalf("Join() error = %v", err)
	}
	if rooms := socket.Rooms(); len(rooms) != 2 {
		t.Fatalf("failed join changed rooms: %v", rooms)
	}
}

func TestAcknowledgementIDWrapsAtJavaScriptSafeInteger(t *testing.T) {
	server, url := testServer(t, Config{AckTimeout: time.Second})
	connection := dialTest(t, url)
	connectTest(t, connection, "/")
	socket := server.Default().Sockets()[0]
	socket.nextID = maxAcknowledgementID

	for _, expectedID := range []uint64{maxAcknowledgementID, 0} {
		result := make(chan error, 1)
		go func() {
			_, err := socket.EmitWithAck(context.Background(), "edge")
			result <- err
		}()
		packet := readTest(t, connection)
		if packet.ID == nil || *packet.ID != expectedID {
			t.Fatalf("packet ID = %v, want %d", packet.ID, expectedID)
		}
		writeTest(t, connection, "43"+strconv.FormatUint(expectedID, 10)+"[]")
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}

	socket.mu.Lock()
	socket.nextID = maxAcknowledgementID
	socket.pending[maxAcknowledgementID] = make(chan ackResult, 1)
	socket.pending[0] = make(chan ackResult, 1)
	socket.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		_, err := socket.EmitWithAck(context.Background(), "edge")
		result <- err
	}()
	packet := readTest(t, connection)
	if packet.ID == nil || *packet.ID != 1 {
		t.Fatalf("packet ID = %v, want 1 after occupied wrap", packet.ID)
	}
	writeTest(t, connection, "431[]")
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	socket.mu.Lock()
	delete(socket.pending, maxAcknowledgementID)
	delete(socket.pending, 0)
	socket.mu.Unlock()
}

func TestConcurrentServerCloseIsSafe(t *testing.T) {
	server, url := testServer(t, Config{})
	for range 4 {
		connection := dialTest(t, url)
		connectTest(t, connection, "/")
	}

	const callers = 32
	errorsFound := make(chan error, callers)
	var waitGroup sync.WaitGroup
	for range callers {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			errorsFound <- server.Close(ctx)
		}()
	}
	waitGroup.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	if count := server.SessionCount(); count != 0 {
		t.Fatalf("SessionCount() = %d", count)
	}
}
