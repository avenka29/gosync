package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	protocol "github.com/avenka29/gosync/internal/socketio"
	"github.com/gorilla/websocket"
)

func TestWebSocketEngineIOSession(t *testing.T) {
	t.Parallel()

	serverErrors := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}

		webSocketTransport, err := NewWebSocket(connection)
		if err != nil {
			serverErrors <- err
			return
		}

		var session *engineio.Session
		session, err = engineio.NewSession(engineio.SessionConfig{
			ID:           "integration-session",
			PingInterval: time.Hour,
			PingTimeout:  time.Hour,
		}, webSocketTransport, func(ctx context.Context, packet engineio.Packet) error {
			return session.Send(ctx, packet)
		})
		if err != nil {
			serverErrors <- err
			return
		}

		serverErrors <- session.Run(request.Context())
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	connection, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	messageType, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage(handshake) error = %v", err)
	}
	if messageType != websocket.TextMessage || !strings.HasPrefix(string(payload), `0{"sid":"integration-session"`) {
		t.Fatalf("handshake = type %d payload %q", messageType, payload)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte("4hello")); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	messageType, payload, err = connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage(echo) error = %v", err)
	}
	if messageType != websocket.TextMessage || string(payload) != "4hello" {
		t.Fatalf("echo = type %d payload %q", messageType, payload)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte("1")); err != nil {
		t.Fatalf("WriteMessage(close) error = %v", err)
	}
	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("session.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session shutdown")
	}
}

func TestSocketIOConnectAndEventOverWebSocket(t *testing.T) {
	t.Parallel()

	serverErrors := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		webSocketTransport, err := NewWebSocket(connection)
		if err != nil {
			serverErrors <- err
			return
		}

		var engineSession *engineio.Session
		var socketSession *protocol.Session
		socketSession, err = protocol.NewSession(
			protocol.NewCodec(0, 0),
			func(ctx context.Context, packet engineio.Packet) error {
				return engineSession.Send(ctx, packet)
			},
			func(ctx context.Context, packet protocol.Packet) error {
				switch packet.Type {
				case protocol.PacketConnect:
					return socketSession.Send(ctx, protocol.Packet{
						Type:      protocol.PacketConnect,
						Namespace: packet.Namespace,
						Data:      map[string]any{"sid": "socket-session"},
					})
				case protocol.PacketEvent:
					return socketSession.Send(ctx, packet)
				default:
					return nil
				}
			},
		)
		if err != nil {
			serverErrors <- err
			return
		}

		engineSession, err = engineio.NewSession(engineio.SessionConfig{
			ID:           "engine-session",
			PingInterval: time.Hour,
			PingTimeout:  time.Hour,
		}, webSocketTransport, socketSession.Handle)
		if err != nil {
			serverErrors <- err
			return
		}
		serverErrors <- engineSession.Run(request.Context())
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	connection, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer connection.Close()

	_, handshakePayload, err := connection.ReadMessage()
	if err != nil || !strings.HasPrefix(string(handshakePayload), `0{"sid":"engine-session"`) {
		t.Fatalf("Engine.IO handshake = %q, error = %v", handshakePayload, err)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		t.Fatalf("WriteMessage(connect) error = %v", err)
	}
	_, connectPayload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage(connect) error = %v", err)
	}
	if got, want := string(connectPayload), `40{"sid":"socket-session"}`; got != want {
		t.Fatalf("Socket.IO connect = %q, want %q", got, want)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte(`42["echo","hello"]`)); err != nil {
		t.Fatalf("WriteMessage(event) error = %v", err)
	}
	_, eventPayload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage(event) error = %v", err)
	}
	if got, want := string(eventPayload), `42["echo","hello"]`; got != want {
		t.Fatalf("Socket.IO event = %q, want %q", got, want)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte("1")); err != nil {
		t.Fatalf("WriteMessage(close) error = %v", err)
	}
	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("session.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session shutdown")
	}
}
