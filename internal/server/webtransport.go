package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/avenka29/gosync/internal/transport"
	wt "github.com/quic-go/webtransport-go"
)

// ServeWebTransport handles an Engine.IO WebTransport request.
func (server *Server) ServeWebTransport(writer http.ResponseWriter, request *http.Request, webTransportServer *wt.Server) {
	if !server.config.EnableWebTransport || !server.checkOrigin(request) {
		http.Error(writer, "Forbidden", http.StatusForbidden)
		return
	}
	query := request.URL.Query()
	if query.Has("EIO") && !hasSingleQueryValue(query, "EIO", "4") ||
		query.Has("transport") && !hasSingleQueryValue(query, "transport", "webtransport") {
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
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

	webTransportSession, err := webTransportServer.Upgrade(writer, request)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(server.context, server.config.UpgradeTimeout)
	defer cancel()
	committed := false
	defer func() {
		if !committed {
			_ = webTransportSession.CloseWithError(0, "")
		}
	}()
	stop := context.AfterFunc(ctx, func() { _ = webTransportSession.CloseWithError(0, "") })
	defer stop()
	stream, err := webTransportSession.AcceptStream(ctx)
	if err != nil {
		return
	}
	next := transport.NewWebTransport(stream, server.config.MaxPayload, func() error {
		return webTransportSession.CloseWithError(0, "")
	})
	frame, err := next.Read(ctx)
	if err != nil || frame.Binary || len(frame.Payload) == 0 || frame.Payload[0] != '0' {
		return
	}
	if len(frame.Payload) == 1 {
		committed = server.serveDirectWebTransport(ctx, request, stream, next, stop)
		return
	}
	committed = server.upgradeWebTransport(ctx, frame.Payload[1:], stream, next, stop)
}

func (server *Server) serveDirectWebTransport(ctx context.Context, request *http.Request, stream transport.DeadlineStream, next *transport.WebTransport, stop func() bool) bool {
	entry, err := server.createSession(next, nil, nil, request)
	if err != nil || !stop() {
		if entry != nil {
			_ = entry.transport.Close()
		}
		return false
	}
	if err := stream.SetReadDeadline(time.Time{}); err != nil {
		_ = entry.transport.Close()
		return false
	}
	server.runSession(entry)
	return true
}

func (server *Server) upgradeWebTransport(ctx context.Context, openPayload []byte, stream transport.DeadlineStream, next *transport.WebTransport, stop func() bool) (committed bool) {
	var open struct {
		SID string `json:"sid"`
	}
	if json.Unmarshal(openPayload, &open) != nil || open.SID == "" {
		return false
	}
	server.mutex.Lock()
	entry := server.sessions[open.SID]
	if entry == nil || entry.switching == nil || entry.upgrading {
		server.mutex.Unlock()
		return false
	}
	entry.upgrading = true
	server.mutex.Unlock()
	defer func() {
		if !committed {
			server.mutex.Lock()
			entry.upgrading = false
			server.mutex.Unlock()
		}
	}()
	committed = server.completeTransportUpgrade(ctx, entry, next, func() error {
		return stream.SetReadDeadline(time.Time{})
	}, stop)
	return committed
}
