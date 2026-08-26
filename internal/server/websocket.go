package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/avenka29/gosync/internal/transport"
	"github.com/gorilla/websocket"
)

const sessionIDBytes = 18

var ErrServerClosed = errors.New("gosync: server closed")

// Sender writes an Engine.IO message through its owning session.
type Sender func(context.Context, engineio.Packet) error

// HandlerFactory constructs the message handler for a newly opened Engine.IO
// session. The factory runs before the Engine.IO open packet is written.
type HandlerFactory func(Sender) engineio.MessageHandler

// Config controls the WebSocket-only Engine.IO server milestone.
type Config struct {
	PingInterval    time.Duration
	PingTimeout     time.Duration
	MaxPayload      int64
	InboundBuffer   int
	OutboundBuffer  int
	ReadBufferSize  int
	WriteBufferSize int

	// CheckOrigin uses Gorilla WebSocket's same-origin default when nil.
	CheckOrigin func(*http.Request) bool

	// OnError receives asynchronous session failures outside internal locks.
	OnError func(error)

	newSessionID func() (string, error)
}

// WebSocketServer accepts direct Engine.IO v4 WebSocket sessions. Polling and
// transport upgrades are deliberately outside this milestone.
type WebSocketServer struct {
	config   Config
	factory  HandlerFactory
	upgrader websocket.Upgrader

	context context.Context
	cancel  context.CancelFunc

	mutex     sync.Mutex
	closed    bool
	active    sync.WaitGroup
	drainOnce sync.Once
	drained   chan struct{}
}

// NewWebSocketServer constructs a WebSocket-only Engine.IO v4 server.
func NewWebSocketServer(config Config, factory HandlerFactory) *WebSocketServer {
	if config.PingInterval <= 0 {
		config.PingInterval = engineio.DefaultPingInterval
	}
	if config.PingTimeout <= 0 {
		config.PingTimeout = engineio.DefaultPingTimeout
	}
	if config.MaxPayload <= 0 {
		config.MaxPayload = engineio.DefaultMaxPayload
	}
	if config.InboundBuffer <= 0 {
		config.InboundBuffer = engineio.DefaultInboundBuffer
	}
	if config.OutboundBuffer <= 0 {
		config.OutboundBuffer = engineio.DefaultOutboundBuffer
	}
	if config.newSessionID == nil {
		config.newSessionID = generateSessionID
	}
	if factory == nil {
		factory = func(Sender) engineio.MessageHandler { return nil }
	}

	serverContext, cancel := context.WithCancel(context.Background())
	return &WebSocketServer{
		config:  config,
		factory: factory,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  config.ReadBufferSize,
			WriteBufferSize: config.WriteBufferSize,
			CheckOrigin:     config.CheckOrigin,
		},
		context: serverContext,
		cancel:  cancel,
		drained: make(chan struct{}),
	}
}

// ServeHTTP validates the Engine.IO v4 direct-WebSocket handshake and owns the
// request until its session closes.
func (server *WebSocketServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query := request.URL.Query()
	if query.Get("EIO") != "4" || query.Get("transport") != "websocket" || query.Has("sid") {
		http.Error(writer, "invalid Engine.IO v4 WebSocket handshake", http.StatusBadRequest)
		return
	}

	server.mutex.Lock()
	if server.closed {
		server.mutex.Unlock()
		http.Error(writer, ErrServerClosed.Error(), http.StatusServiceUnavailable)
		return
	}
	server.active.Add(1)
	server.mutex.Unlock()
	defer server.active.Done()

	connection, err := server.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		server.report(err)
		return
	}
	webSocketTransport, err := transport.NewWebSocket(connection)
	if err != nil {
		server.report(err)
		connection.Close()
		return
	}

	sessionID, err := server.config.newSessionID()
	if err != nil {
		server.report(err)
		webSocketTransport.Close()
		return
	}

	var session *engineio.Session
	handler := server.factory(func(ctx context.Context, packet engineio.Packet) error {
		if session == nil {
			return engineio.ErrSessionClosed
		}
		return session.Send(ctx, packet)
	})
	session, err = engineio.NewSession(engineio.SessionConfig{
		ID:             sessionID,
		PingInterval:   server.config.PingInterval,
		PingTimeout:    server.config.PingTimeout,
		MaxPayload:     server.config.MaxPayload,
		InboundBuffer:  server.config.InboundBuffer,
		OutboundBuffer: server.config.OutboundBuffer,
	}, webSocketTransport, handler)
	if err != nil {
		server.report(err)
		webSocketTransport.Close()
		return
	}

	if err := session.Run(server.context); err != nil && !errors.Is(err, context.Canceled) {
		server.report(err)
	}
}

// Close stops admission, cancels all active sessions, and waits for their
// transports to close or for ctx to expire.
func (server *WebSocketServer) Close(ctx context.Context) error {
	server.mutex.Lock()
	if !server.closed {
		server.closed = true
		server.cancel()
	}
	server.mutex.Unlock()

	server.drainOnce.Do(func() {
		go func() {
			server.active.Wait()
			close(server.drained)
		}()
	})

	select {
	case <-server.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (server *WebSocketServer) report(err error) {
	if server.config.OnError != nil {
		server.config.OnError(err)
	}
}

func generateSessionID() (string, error) {
	random := make([]byte, sessionIDBytes)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(random), nil
}
