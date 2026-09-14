package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/gorilla/websocket"
)

var ErrUnsupportedWebSocketMessage = errors.New("transport: unsupported WebSocket message type")

// WebSocket adapts a Gorilla connection to Engine.IO frames.
type WebSocket struct {
	connection *websocket.Conn
	closeOnce  sync.Once
	closeError error
}

// NewWebSocket takes ownership of connection.
func NewWebSocket(connection *websocket.Conn) (*WebSocket, error) {
	if connection == nil {
		return nil, errors.New("transport: WebSocket connection is required")
	}
	return &WebSocket{connection: connection}, nil
}

// Read reads one complete Engine.IO frame.
func (transport *WebSocket) Read(ctx context.Context) (engineio.Frame, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := transport.connection.SetReadDeadline(deadline); err != nil {
			return engineio.Frame{}, err
		}
	}

	messageType, payload, err := transport.connection.ReadMessage()
	if err != nil {
		return engineio.Frame{}, err
	}

	switch messageType {
	case websocket.TextMessage:
		return engineio.Frame{Payload: payload}, nil
	case websocket.BinaryMessage:
		return engineio.Frame{Payload: payload, Binary: true}, nil
	default:
		return engineio.Frame{}, fmt.Errorf("%w: %d", ErrUnsupportedWebSocketMessage, messageType)
	}
}

// Write writes one complete Engine.IO frame.
func (transport *WebSocket) Write(ctx context.Context, frame engineio.Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := transport.connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = transport.Close() })
	defer stop()

	messageType := websocket.TextMessage
	if frame.Binary {
		messageType = websocket.BinaryMessage
	}
	return transport.connection.WriteMessage(messageType, frame.Payload)
}

// Close closes the underlying connection once.
func (transport *WebSocket) Close() error {
	transport.closeOnce.Do(func() {
		transport.closeError = transport.connection.Close()
	})
	return transport.closeError
}
