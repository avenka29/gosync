package server

import (
	"context"
	"net/http"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/avenka29/gosync/internal/transport"
)

func (server *Server) upgradeWebSocket(writer http.ResponseWriter, request *http.Request) {
	server.mutex.Lock()
	entry := server.sessions[request.URL.Query().Get("sid")]
	if entry == nil || entry.switching == nil || entry.upgrading {
		duplicate := entry != nil && entry.upgrading
		server.mutex.Unlock()
		if duplicate {
			connection, err := server.upgrader.Upgrade(writer, request, nil)
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
	}
	entry.upgrading = true
	server.mutex.Unlock()
	committed := false
	defer func() {
		if !committed {
			server.mutex.Lock()
			entry.upgrading = false
			server.mutex.Unlock()
		}
	}()
	connection, err := server.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	next, err := transport.NewWebSocket(connection)
	if err != nil {
		_ = connection.Close()
		return
	}
	defer func() {
		if !committed {
			_ = next.Close()
		}
	}()
	connection.SetReadLimit(server.config.MaxPayload)
	ctx, cancel := context.WithTimeout(server.context, server.config.UpgradeTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = next.Close() })
	defer stop()
	committed = server.completeTransportUpgrade(ctx, entry, next, func() error {
		return connection.SetReadDeadline(time.Time{})
	}, stop)
}

func (server *Server) completeTransportUpgrade(ctx context.Context, entry *managedSession, next engineio.Transport, clearDeadline func() error, stop func() bool) bool {
	frame, err := next.Read(ctx)
	if err != nil || frame.Binary || string(frame.Payload) != "2probe" {
		return false
	}
	if err := next.Write(ctx, engineio.Frame{Payload: []byte("3probe")}); err != nil {
		return false
	}
	if err := entry.polling.Write(ctx, engineio.Frame{Payload: []byte("6")}); err != nil {
		return false
	}
	frame, err = next.Read(ctx)
	if err != nil || frame.Binary || string(frame.Payload) != "5" {
		return false
	}
	if err := clearDeadline(); err != nil {
		return false
	}
	if err := entry.switching.Upgrade(ctx, next); err != nil {
		_ = entry.transport.Close()
		return false
	}
	if !stop() {
		_ = entry.transport.Close()
		return false
	}
	return true
}
