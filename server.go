// Package gosync implements a Socket.IO protocol 5 server over Engine.IO 4.
package gosync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	engineServer "github.com/avenka29/gosync/internal/server"
	"github.com/avenka29/gosync/internal/socketio"
)

var (
	ErrInvalidConfig       = errors.New("gosync: invalid configuration")
	ErrConnectionClosed    = errors.New("gosync: connection closed")
	ErrServerClosed        = engineServer.ErrServerClosed
	ErrInvalidEvent        = errors.New("gosync: invalid or reserved event name")
	ErrAlreadyAcknowledged = errors.New("gosync: event already acknowledged")
	ErrAckLimit            = errors.New("gosync: pending acknowledgement limit reached")
	ErrProtocol            = errors.New("gosync: invalid packet for connection state")
	ErrQueueFull           = errors.New("gosync: event queue full")
	ErrInvalidRoom         = errors.New("gosync: invalid room")
	ErrRoomLimit           = errors.New("gosync: room limit reached")
)

// Config sets server limits; zero values use bounded defaults.
type Config struct {
	EnableWebTransport         bool
	Broker                     Broker
	Recovery                   *RecoveryConfig
	AssemblyTimeout            time.Duration
	PingInterval               time.Duration
	PingTimeout                time.Duration
	UpgradeTimeout             time.Duration
	ConnectTimeout             time.Duration
	AckTimeout                 time.Duration
	MaxPayload                 int64
	InboundBuffer              int
	OutboundBuffer             int
	ReadBufferSize             int
	WriteBufferSize            int
	MaxAttachments             int
	MaxJSONDepth               int
	MaxConnections             int
	MaxNamespacesPerConnection int
	MaxRoomsPerSocket          int
	MaxPendingAcks             int
	CheckOrigin                func(*http.Request) bool
	AllowCredentials           bool
	OnError                    func(error)
}

type Server struct {
	deliveryMu     sync.Mutex
	nodeID         string
	clusterMu      sync.Mutex
	clusterPending map[string]*clusterWait
	clusterSlots   chan struct{}
	clusterWorkers sync.WaitGroup
	recovery       *recoveryStore
	codec          socketio.Codec
	engine         *engineServer.Server
	config         Config
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	namespaces     map[string]*Namespace
	connections    map[string]*connection
	workers        sync.WaitGroup
}

type connection struct {
	id            string
	server        *Server
	session       *socketio.Session
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	sockets       map[string]*Socket
	jobs          chan func()
	timer         *time.Timer
	assemblyTimer *time.Timer
	request       *http.Request
}

// NewServer creates a server. Register namespaces and handlers before serving.
func NewServer(config Config) (*Server, error) {
	if err := config.defaults(); err != nil {
		return nil, err
	}
	recovery, err := newRecovery(config.Recovery)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		recovery:    recovery,
		codec:       socketio.NewCodec(config.MaxAttachments, config.MaxJSONDepth),
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		namespaces:  make(map[string]*Namespace),
		connections: make(map[string]*connection),
	}
	server.namespaces["/"] = newNamespace(server, "/")
	server.nodeID, err = newID()
	if err != nil {
		cancel()
		return nil, err
	}
	server.clusterPending = make(map[string]*clusterWait)
	server.clusterSlots = make(chan struct{}, config.MaxPendingAcks)
	server.engine = engineServer.NewServer(engineServer.Config{
		EnableWebTransport: config.EnableWebTransport,
		PingInterval:       config.PingInterval,
		PingTimeout:        config.PingTimeout,
		MaxPayload:         config.MaxPayload,
		InboundBuffer:      config.InboundBuffer,
		OutboundBuffer:     config.OutboundBuffer,
		ReadBufferSize:     config.ReadBufferSize,
		WriteBufferSize:    config.WriteBufferSize,
		MaxConnections:     config.MaxConnections,
		UpgradeTimeout:     config.UpgradeTimeout,
		CheckOrigin:        config.CheckOrigin,
		OnError:            server.report,
		OnOpen:             server.open,
		OnSessionClose:     server.closed,
	}, nil)
	if config.Broker != nil {
		if err := config.Broker.Start(ctx, server.nodeID, server.receiveCluster); err != nil {
			cancel()
			return nil, err
		}
	}
	if recovery != nil {
		server.workers.Add(1)
		go func() {
			defer server.workers.Done()
			interval := recovery.config.MaxDisconnectionDuration / 2
			if interval > time.Minute {
				interval = time.Minute
			}
			if interval < time.Millisecond {
				interval = time.Millisecond
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					recovery.mu.Lock()
					recovery.expire()
					recovery.mu.Unlock()
				}
			}
		}()
	}
	return server, nil
}

func (config *Config) defaults() error {
	for _, value := range []time.Duration{
		config.AssemblyTimeout,
		config.PingInterval,
		config.PingTimeout,
		config.UpgradeTimeout,
		config.ConnectTimeout,
		config.AckTimeout,
	} {
		if value < 0 {
			return ErrInvalidConfig
		}
	}
	for _, value := range []int{
		config.InboundBuffer,
		config.OutboundBuffer,
		config.ReadBufferSize,
		config.WriteBufferSize,
		config.MaxAttachments,
		config.MaxJSONDepth,
		config.MaxConnections,
		config.MaxNamespacesPerConnection,
		config.MaxRoomsPerSocket,
		config.MaxPendingAcks,
	} {
		if value < 0 {
			return ErrInvalidConfig
		}
	}
	if config.MaxPayload < 0 {
		return ErrInvalidConfig
	}
	config.AssemblyTimeout = durationOr(config.AssemblyTimeout, 10*time.Second)
	config.PingInterval = durationOr(config.PingInterval, 25*time.Second)
	config.PingTimeout = durationOr(config.PingTimeout, 20*time.Second)
	config.UpgradeTimeout = durationOr(config.UpgradeTimeout, 10*time.Second)
	config.ConnectTimeout = durationOr(config.ConnectTimeout, 20*time.Second)
	config.AckTimeout = durationOr(config.AckTimeout, 10*time.Second)
	config.MaxPayload = int64Or(config.MaxPayload, 1_000_000)
	config.InboundBuffer = intOr(config.InboundBuffer, 64)
	config.OutboundBuffer = intOr(config.OutboundBuffer, 64)
	config.MaxConnections = intOr(config.MaxConnections, 10_000)
	config.MaxNamespacesPerConnection = intOr(config.MaxNamespacesPerConnection, 32)
	config.MaxRoomsPerSocket = intOr(config.MaxRoomsPerSocket, 128)
	config.MaxPendingAcks = intOr(config.MaxPendingAcks, 128)
	return nil
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

func intOr(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func int64Or(value, fallback int64) int64 {
	if value == 0 {
		return fallback
	}
	return value
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.config.CheckOrigin != nil && r.Header.Get("Origin") != "" {
		w.Header().Add("Vary", "Origin")
		if !s.config.CheckOrigin(r) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		if s.config.AllowCredentials {
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	s.engine.ServeHTTP(w, r)
}

// Of registers or retrieves a namespace. Invalid names return an error.
func (s *Server) Of(name string) (*Namespace, error) {
	if name == "" {
		name = "/"
	}
	if !validName(name, true) {
		return nil, socketio.ErrInvalidNamespace
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, ErrServerClosed
	}
	if namespace := s.namespaces[name]; namespace != nil {
		return namespace, nil
	}
	namespace := newNamespace(s, name)
	s.namespaces[name] = namespace
	return namespace, nil
}

func (s *Server) Default() *Namespace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespaces["/"]
}

func (s *Server) SessionCount() int {
	return s.engine.SessionCount()
}

// Close stops admission and waits for transports, brokers, and handlers.
func (s *Server) Close(ctx context.Context) error {
	s.cancel()
	componentCount := 1
	results := make(chan error, 2)
	go func() {
		results <- s.engine.Close(ctx)
	}()
	if s.config.Broker != nil {
		componentCount++
		go func() {
			results <- s.config.Broker.Close(ctx)
		}()
	}
	var errs []error
	for range componentCount {
		select {
		case err := <-results:
			errs = append(errs, err)
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		}
	}
	done := make(chan struct{})
	go func() {
		s.clusterWorkers.Wait()
		s.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return errors.Join(errs...)
	case <-ctx.Done():
		return errors.Join(append(errs, ctx.Err())...)
	}
}

func (s *Server) report(err error) {
	if err == nil || s.config.OnError == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	s.config.OnError(err)
}

func (s *Server) open(id string, sender engineServer.Sender, r *http.Request) engineio.MessageHandler {
	s.mu.Lock()
	if s.ctx.Err() != nil || len(s.connections) >= s.config.MaxConnections {
		s.mu.Unlock()
		return func(context.Context, engineio.Packet) error { return ErrConnectionClosed }
	}
	ctx, cancel := context.WithCancel(s.ctx)
	connection := &connection{
		id:      id,
		server:  s,
		ctx:     ctx,
		cancel:  cancel,
		sockets: make(map[string]*Socket),
		jobs:    make(chan func(), s.config.InboundBuffer),
		request: r.Clone(ctx),
	}
	connection.request.Body = http.NoBody
	session, err := socketio.NewSession(s.codec, socketio.PacketSender(sender), connection.handle)
	if err != nil {
		cancel()
		s.mu.Unlock()
		return func(context.Context, engineio.Packet) error { return err }
	}
	connection.session = session
	connection.session.SetLimits(s.config.MaxPayload)
	connection.session.SetOnFailure(func() { _ = s.engine.Disconnect(id) })
	s.connections[id] = connection
	s.workers.Add(1)
	s.mu.Unlock()
	go connection.run()
	connection.mu.Lock()
	connection.timer = time.AfterFunc(s.config.ConnectTimeout, func() {
		connection.cancel()
		_ = s.engine.Disconnect(id)
	})
	connection.mu.Unlock()
	return func(ctx context.Context, p engineio.Packet) error {
		if !p.Binary && len(p.Data) > 0 && (p.Data[0] == '5' || p.Data[0] == '6') {
			connection.mu.Lock()
			if connection.assemblyTimer == nil {
				connection.assemblyTimer = time.AfterFunc(s.config.AssemblyTimeout, func() { _ = s.engine.Disconnect(id) })
			}
			connection.mu.Unlock()
		}
		return connection.session.Handle(ctx, p)
	}
}

func (s *Server) closed(id string, reason error) {
	s.mu.Lock()
	c := s.connections[id]
	delete(s.connections, id)
	s.mu.Unlock()
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.assemblyTimer != nil {
		c.assemblyTimer.Stop()
	}
	c.mu.Unlock()
	c.cancel()
}

func (c *connection) run() {
	defer c.server.workers.Done()
	defer func() {
		if p := recover(); p != nil {
			c.server.report(fmt.Errorf("gosync: callback panic: %v", p))
			_ = c.server.engine.Disconnect(c.id)
		}
		c.cancel()
		c.mu.Lock()
		sockets := make([]*Socket, 0, len(c.sockets))
		for _, s := range c.sockets {
			sockets = append(sockets, s)
		}
		c.mu.Unlock()
		for _, s := range sockets {
			s.close("transport close")
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case job := <-c.jobs:
			if c.ctx.Err() != nil {
				return
			}
			job()
		}
	}
}

func (c *connection) enqueue(job func()) error {
	select {
	case <-c.ctx.Done():
		return ErrConnectionClosed
	default:
	}
	select {
	case c.jobs <- job:
		return nil
	case <-c.ctx.Done():
		return ErrConnectionClosed
	default:
		return ErrQueueFull
	}
}

func (c *connection) handle(ctx context.Context, p socketio.Packet) error {
	c.mu.Lock()
	if c.assemblyTimer != nil {
		c.assemblyTimer.Stop()
		c.assemblyTimer = nil
	}
	socket := c.sockets[p.Namespace]
	c.mu.Unlock()
	switch p.Type {
	case socketio.PacketConnect:
		if socket != nil {
			return ErrProtocol
		}
		return c.enqueue(func() {
			if err := c.connect(p); err != nil {
				c.server.report(err)
				_ = c.server.engine.Disconnect(c.id)
			}
		})
	case socketio.PacketAck, socketio.PacketBinaryAck:
		if socket == nil {
			return ErrProtocol
		}
		socket.acknowledge(p)
		return nil
	case socketio.PacketEvent, socketio.PacketBinaryEvent, socketio.PacketDisconnect:
		return c.enqueue(func() {
			c.mu.Lock()
			socket := c.sockets[p.Namespace]
			c.mu.Unlock()
			if socket == nil {
				_ = c.server.engine.Disconnect(c.id)
				return
			}
			if p.Type == socketio.PacketDisconnect {
				socket.close("client namespace disconnect")
				return
			}
			if err := socket.dispatch(p); err != nil {
				c.server.report(err)
			}
		})
	default:
		return ErrProtocol
	}
}

func (c *connection) connect(p socketio.Packet) error {
	c.mu.Lock()
	existing := c.sockets[p.Namespace]
	count := len(c.sockets)
	c.mu.Unlock()
	if existing != nil {
		return ErrProtocol
	}
	c.server.mu.RLock()
	n := c.server.namespaces[p.Namespace]
	c.server.mu.RUnlock()
	reject := func(message string) error {
		return c.session.Send(c.ctx, socketio.Packet{Type: socketio.PacketConnectError, Namespace: p.Namespace, Data: map[string]any{"message": message}})
	}
	if n == nil {
		return reject("Invalid namespace")
	}
	if count >= c.server.config.MaxNamespacesPerConnection {
		return reject("Namespace limit reached")
	}
	id, err := newID()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(c.ctx)
	socket := &Socket{id: id, namespace: n, connection: c, ctx: ctx, cancel: cancel, auth: p.Data, handlers: make(map[string]EventHandler), pending: make(map[uint64]chan ackResult), rooms: make(map[string]struct{})}
	n.mu.RLock()
	middleware := append([]Middleware(nil), n.middleware...)
	onConnect := n.onConnect
	n.mu.RUnlock()
	for _, m := range middleware {
		if err := m(ctx, socket); err != nil {
			cancel()
			return reject("Not authorized")
		}
	}
	if err := ctx.Err(); err != nil {
		cancel()
		return err
	}
	c.mu.Lock()
	c.sockets[p.Namespace] = socket
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.assemblyTimer != nil {
		c.assemblyTimer.Stop()
	}
	c.mu.Unlock()
	c.server.deliveryMu.Lock()
	replay := c.server.recovery.attach(socket)
	socket.deliveryMu.Lock()
	n.mu.Lock()
	n.sockets[socket.id] = socket
	n.mu.Unlock()
	socket.rooms[socket.id] = struct{}{}
	data := map[string]any{"sid": socket.id}
	if socket.pid != "" {
		data["pid"] = socket.pid
	}
	if err := c.session.Send(ctx, socketio.Packet{Type: socketio.PacketConnect, Namespace: p.Namespace, Data: data}); err != nil {
		socket.deliveryMu.Unlock()
		c.server.deliveryMu.Unlock()
		socket.close("connect error")
		return err
	}
	for _, saved := range replay {
		packet, err := c.server.codec.DecodeHeader(saved.encoded.Header)
		if err == nil && packet.Type.Binary() {
			packet, err = c.server.codec.Reconstruct(packet, saved.encoded.Attachments)
		}
		if err == nil {
			err = c.session.Send(ctx, packet)
		}
		if err != nil {
			socket.deliveryMu.Unlock()
			c.server.deliveryMu.Unlock()
			socket.close("connect error")
			return err
		}
	}
	socket.deliveryMu.Unlock()
	c.server.deliveryMu.Unlock()
	if onConnect != nil {
		onConnect(ctx, socket)
	}
	return nil
}

func newID() (string, error) {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
