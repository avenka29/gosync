package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/gorilla/websocket"
)

func TestWebSocketServerHandshakeAndShutdown(t *testing.T) {
	t.Parallel()

	server := NewWebSocketServer(Config{
		PingInterval: time.Hour,
		PingTimeout:  time.Hour,
		newSessionID: func() (string, error) { return "known-session", nil },
	}, func(sender Sender) engineio.MessageHandler {
		return func(ctx context.Context, packet engineio.Packet) error {
			return sender(ctx, packet)
		}
	})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "?EIO=4&transport=websocket"
	connection, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}

	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage(open) error = %v", err)
	}
	if !strings.HasPrefix(string(payload), `0{"sid":"known-session"`) {
		t.Fatalf("open packet = %q", payload)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte("4hello")); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	_, payload, err = connection.ReadMessage()
	if err != nil || string(payload) != "4hello" {
		t.Fatalf("echo packet = %q, error = %v", payload, err)
	}

	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(closeContext); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("connection remained readable after server shutdown")
	}
}

func TestWebSocketServerRejectsInvalidHandshakes(t *testing.T) {
	t.Parallel()

	server := NewWebSocketServer(Config{}, nil)
	tests := []struct {
		name   string
		method string
		query  string
		status int
	}{
		{name: "method", method: http.MethodPost, query: "?EIO=4&transport=websocket", status: http.StatusMethodNotAllowed},
		{name: "missing query", method: http.MethodGet, status: http.StatusBadRequest},
		{name: "old revision", method: http.MethodGet, query: "?EIO=3&transport=websocket", status: http.StatusBadRequest},
		{name: "unsupported polling", method: http.MethodGet, query: "?EIO=4&transport=polling", status: http.StatusBadRequest},
		{name: "unknown upgrade session", method: http.MethodGet, query: "?EIO=4&transport=websocket&sid=unknown", status: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(test.method, "/socket.io/"+test.query, nil)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("ServeHTTP() status = %d, want %d", response.Code, test.status)
			}
		})
	}
}
