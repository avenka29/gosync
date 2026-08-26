package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/gorilla/websocket"
)

var ErrUnsupportedWebSocketMessage = errors.New("transport: unsupported WebSocket message type")

// WebSocket adapts a Gorilla WebSocket connection to an Engine.IO transport.
// Engine.IO heartbeats are text packets and are independent from WebSocket
// control-frame ping and pong messages.
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
	if deadline, ok := ctx.Deadline(); ok {
		if err := transport.connection.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}

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
