package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/avenka29/gosync/internal/transport"
	"github.com/gorilla/websocket"
)

const (
	sessionIDBytes       = 18
	maxSessionIDAttempts = 8
)

var (
	ErrServerClosed      = errors.New("gosync: server closed")
	ErrUnknownSession    = errors.New("gosync: unknown session")
	ErrSessionIDConflict = errors.New("gosync: could not allocate a unique session ID")
)

const (
	engineErrorUnknownTransport = iota
	engineErrorUnknownSession
	engineErrorBadHandshakeMethod
	engineErrorBadRequest
	engineErrorForbidden
	engineErrorUnsupportedVersion
)

var engineErrorMessages = [...]string{
	"Transport unknown",
	"Session ID unknown",
	"Bad handshake method",
	"Bad request",
	"Forbidden",
	"Unsupported protocol version",
}

// Sender writes an Engine.IO message through its owning session.
type Sender func(context.Context, engineio.Packet) error

// HandlerFactory creates a handler before the open packet is sent.
type HandlerFactory func(Sender) engineio.MessageHandler

// Config controls Engine.IO sessions and their HTTP transports.
type Config struct {
	EnableWebTransport bool
	PingInterval       time.Duration
	PingTimeout        time.Duration
	MaxPayload         int64
	InboundBuffer      int
	OutboundBuffer     int
	ReadBufferSize     int
	WriteBufferSize    int

	CheckOrigin func(*http.Request) bool

	OnError func(error)

	MaxConnections int
	UpgradeTimeout time.Duration
	OnSessionClose func(string, error)
	OnOpen         func(string, Sender, *http.Request) engineio.MessageHandler
	newSessionID   func() (string, error)
}

// Server serves Engine.IO 4 over polling, WebSocket, and WebTransport.
type Server struct {
	config       Config
	factory      HandlerFactory
	upgrader     websocket.Upgrader
	allowPolling bool

	context context.Context
	cancel  context.CancelFunc

	createMu    sync.Mutex
	mutex       sync.Mutex
	closed      bool
	sessions    map[string]*managedSession
	closedPolls map[string]time.Time
	active      sync.WaitGroup
	drainOnce   sync.Once
	drained     chan struct{}
}

// WebSocketServer is the direct-WebSocket-only server alias.
type WebSocketServer = Server

type managedSession struct {
	id        string
	session   *engineio.Session
	transport engineio.Transport
	polling   *transport.Polling
	done      chan struct{}
	switching *transport.Switching
	upgrading bool
}

// NewServer creates a server with polling and transport upgrades.
func NewServer(config Config, factory HandlerFactory) *Server {
	return newServer(config, factory, true)
}

// NewWebSocketServer constructs a direct-WebSocket-only Engine.IO v4 server.
func NewWebSocketServer(config Config, factory HandlerFactory) *WebSocketServer {
	return newServer(config, factory, false)
}

func newServer(config Config, factory HandlerFactory, allowPolling bool) *Server {
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
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10000
	}
	if config.UpgradeTimeout <= 0 {
		config.UpgradeTimeout = 10 * time.Second
	}
	if config.newSessionID == nil {
		config.newSessionID = generateSessionID
	}
	if factory == nil {
		factory = func(Sender) engineio.MessageHandler { return nil }
	}

	serverContext, cancel := context.WithCancel(context.Background())
	checkOrigin := config.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = sameOrigin
	}
	return &Server{
		config:       config,
		factory:      factory,
		allowPolling: allowPolling,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  config.ReadBufferSize,
			WriteBufferSize: config.WriteBufferSize,
			CheckOrigin:     checkOrigin,
		},
		context:     serverContext,
		cancel:      cancel,
		sessions:    make(map[string]*managedSession),
		closedPolls: make(map[string]time.Time),
		drained:     make(chan struct{}),
	}
}

// ServeHTTP validates and dispatches an Engine.IO request.
func (server *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Body != nil && request.Body != http.NoBody {
		defer request.Body.Close()
	}
	server.mutex.Lock()
	if server.closed {
		server.mutex.Unlock()
		writeEngineError(writer, http.StatusServiceUnavailable, engineErrorBadRequest)
		return
	}
	server.active.Add(1)
	server.mutex.Unlock()
	defer server.active.Done()
	query := request.URL.Query()
	if !hasSingleQueryValue(query, "EIO", "4") {
		writeEngineError(writer, http.StatusBadRequest, engineErrorUnsupportedVersion)
		return
	}
	transportName, ok := singleQueryValue(query, "transport")
	if !ok {
		writeEngineError(writer, http.StatusBadRequest, engineErrorUnknownTransport)
		return
	}
	if values, exists := query["sid"]; exists && (len(values) != 1 || values[0] == "") {
		writeEngineError(writer, http.StatusBadRequest, engineErrorUnknownSession)
		return
	}

	switch transportName {
	case "websocket":
		if request.Method != http.MethodGet {
			writeEngineError(writer, http.StatusBadRequest, engineErrorBadHandshakeMethod)
			return
		}
		if query.Has("sid") {
			server.upgradeWebSocket(writer, request)
			return
		}
		server.serveWebSocket(writer, request)
	case "polling":
		if !server.allowPolling {
			writeEngineError(writer, http.StatusBadRequest, engineErrorUnknownTransport)
			return
		}
		if !server.checkOrigin(request) {
			writeEngineError(writer, http.StatusForbidden, engineErrorForbidden)
			return
		}
		server.servePolling(writer, request)
	default:
		writeEngineError(writer, http.StatusBadRequest, engineErrorUnknownTransport)
	}
}

// Close stops admission and waits for active sessions until ctx expires.
func (server *Server) Close(ctx context.Context) error {
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

// SessionCount returns the number of active Engine.IO sessions.
func (server *Server) SessionCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return len(server.sessions)
}

func (server *Server) createSession(engineTransport engineio.Transport, polling *transport.Polling, upgrades []string, requests ...*http.Request) (*managedSession, error) {
	server.createMu.Lock()
	defer server.createMu.Unlock()
	for attempt := 0; attempt < maxSessionIDAttempts; attempt++ {
		sessionID, err := server.config.newSessionID()
		if err != nil {
			return nil, err
		}

		server.mutex.Lock()
		_, exists := server.sessions[sessionID]
		full := len(server.sessions) >= server.config.MaxConnections
		closed := server.closed
		server.mutex.Unlock()
		if closed {
			return nil, ErrServerClosed
		}
		if full {
			return nil, errors.New("gosync: connection limit reached")
		}
		if exists {
			continue
		}
		sender := &sessionSender{}
		handler, err := server.createHandler(sessionID, sender.Send, requests)
		if err != nil {
			return nil, err
		}
		session, err := engineio.NewSession(engineio.SessionConfig{
			ID:             sessionID,
			Upgrades:       upgrades,
			PingInterval:   server.config.PingInterval,
			PingTimeout:    server.config.PingTimeout,
			MaxPayload:     server.config.MaxPayload,
			InboundBuffer:  server.config.InboundBuffer,
			OutboundBuffer: server.config.OutboundBuffer,
		}, engineTransport, handler)
		if err != nil {
			return nil, err
		}
		sender.Bind(session)

		entry := &managedSession{
			id:        sessionID,
			session:   session,
			transport: engineTransport,
			polling:   polling,
			done:      make(chan struct{}),
		}
		if switching, ok := engineTransport.(*transport.Switching); ok {
			entry.switching = switching
		}
		if server.register(entry) {
			return entry, nil
		}
		server.notifySessionClose(sessionID, ErrServerClosed)
		return nil, ErrServerClosed
	}
	return nil, ErrSessionIDConflict
}

func (server *Server) createHandler(id string, send Sender, requests []*http.Request) (handler engineio.MessageHandler, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("gosync: open callback panic: %v", recovered)
		}
	}()
	handler = server.factory(send)
	if server.config.OnOpen != nil && len(requests) > 0 {
		handler = server.config.OnOpen(id, send, requests[0])
	}
	return handler, nil
}

type sessionSender struct {
	mutex   sync.RWMutex
	session *engineio.Session
}

func (sender *sessionSender) Bind(session *engineio.Session) {
	sender.mutex.Lock()
	sender.session = session
	sender.mutex.Unlock()
}

func (sender *sessionSender) Send(ctx context.Context, packet engineio.Packet) error {
	sender.mutex.RLock()
	session := sender.session
	sender.mutex.RUnlock()
	if session == nil {
		return engineio.ErrSessionClosed
	}
	return session.Send(ctx, packet)
}

func (server *Server) register(entry *managedSession) bool {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.closed {
		return false
	}
	if _, exists := server.sessions[entry.id]; exists {
		return false
	}
	server.sessions[entry.id] = entry
	server.active.Add(1)
	return true
}

func (server *Server) runSession(entry *managedSession) {
	defer server.active.Done()
	defer close(entry.done)
	defer server.remove(entry)

	err := entry.session.Run(server.context)
	server.notifySessionClose(entry.id, err)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) && !errors.Is(err, transport.ErrPollingClosed) {
		server.report(err)
	}
}

func (server *Server) find(sessionID string) (*managedSession, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	entry, ok := server.sessions[sessionID]
	if !ok {
		return nil, ErrUnknownSession
	}
	return entry, nil
}

func (server *Server) remove(entry *managedSession) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if current, ok := server.sessions[entry.id]; ok && current == entry {
		delete(server.sessions, entry.id)
		if entry.polling != nil && entry.polling.ClientClosed() {
			now := time.Now()
			for id, expires := range server.closedPolls {
				if now.After(expires) {
					delete(server.closedPolls, id)
				}
			}
			if len(server.closedPolls) < server.config.MaxConnections {
				server.closedPolls[entry.id] = now.Add(5 * time.Second)
			}
		}
	}
}

func (server *Server) checkOrigin(request *http.Request) bool {
	if server.config.CheckOrigin != nil {
		return server.config.CheckOrigin(request)
	}
	return sameOrigin(request)
}

func (server *Server) report(err error) {
	if err == nil || server.config.OnError == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	server.config.OnError(err)
}

func (server *Server) notifySessionClose(id string, err error) {
	if server.config.OnSessionClose == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			server.report(fmt.Errorf("gosync: close callback panic: %v", recovered))
		}
	}()
	server.config.OnSessionClose(id, err)
}

func sameOrigin(request *http.Request) bool {
	origins := request.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	requestScheme := "http"
	if request.TLS != nil {
		requestScheme = "https"
	}
	if request.URL.Scheme != "" {
		requestScheme = request.URL.Scheme
	}
	requestOrigin, ok := canonicalOrigin(requestScheme, request.Host)
	if !ok {
		return false
	}
	for _, origin := range origins {
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
			!strings.EqualFold(parsed.Scheme, requestScheme) {
			return false
		}
		originKey, ok := canonicalOrigin(parsed.Scheme, parsed.Host)
		if !ok || originKey != requestOrigin {
			return false
		}
	}
	return true
}

func canonicalOrigin(scheme, authority string) (string, bool) {
	if !strings.EqualFold(scheme, "http") && !strings.EqualFold(scheme, "https") {
		return "", false
	}
	parsed, err := url.Parse(strings.ToLower(scheme) + "://" + authority)
	if err != nil || parsed.User != nil || parsed.Hostname() == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false
	}
	port := parsed.Port()
	if port == "" {
		if strings.EqualFold(scheme, "https") {
			port = "443"
		} else {
			port = "80"
		}
	}
	return strings.ToLower(scheme) + "://" + net.JoinHostPort(strings.ToLower(parsed.Hostname()), port), true
}

func singleQueryValue(query url.Values, key string) (string, bool) {
	values, ok := query[key]
	if !ok || len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func hasSingleQueryValue(query url.Values, key, want string) bool {
	value, ok := singleQueryValue(query, key)
	return ok && value == want
}

func writeEngineError(writer http.ResponseWriter, status, code int) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: code, Message: engineErrorMessages[code]})
}

func generateSessionID() (string, error) {
	random := make([]byte, sessionIDBytes)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate session ID: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(random), nil
}

// Disconnect terminates one physical connection and unblocks its transport.
func (server *Server) Disconnect(id string) error {
	entry, err := server.find(id)
	if err != nil {
		return err
	}
	return entry.transport.Close()
}

func (server *Server) consumeClosedPoll(id string) bool {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	expires, ok := server.closedPolls[id]
	delete(server.closedPolls, id)
	return ok && time.Now().Before(expires)
}
