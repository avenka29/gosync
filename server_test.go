package gosync

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBroadcastEvent_Validation(t *testing.T) {
	server := NewServer()

	tests := []struct {
		name    string
		evtName string
		data    interface{}
		wantErr error
	}{
		{
			name:    "Empty Event Name",
			evtName: "",
			data:    "some data",
			wantErr: ErrServerMsgInvalid,
		},
		{
			name:    "Nil Data",
			evtName: "chat",
			data:    nil,
			wantErr: ErrServerMsgInvalid,
		},
		{
			name:    "Non-Serializable Data (Function)",
			evtName: "chat",
			data:    func() {}, // Functions cannot be marshaled to JSON
			wantErr: ErrServerMsgInvalid,
		},
		{
			name:    "Valid Data",
			evtName: "chat",
			data:    "hello",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.BroadcastEvent(tt.evtName, tt.data)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("BroadcastEvent() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Errorf("BroadcastEvent() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestServer_Integration(t *testing.T) {
	// 1. Setup the Server
	gs := NewServer()
	ts := httptest.NewServer(gs)
	defer ts.Close()

	// Convert http URL to ws URL
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	// 2. Connect a client
	dialer := websocket.Dialer{}
	ws, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to connect to websocket: %v", err)
	}
	defer ws.Close()

	// Give the hub a moment to register the client
	time.Sleep(50 * time.Millisecond)

	t.Run("Broadcast Flow", func(t *testing.T) {
		testData := map[string]string{"msg": "integration test"}
		err := gs.BroadcastEvent("test-event", testData)
		if err != nil {
			t.Fatalf("Broadcast failed: %v", err)
		}

		// Read from websocket
		_, message, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("Failed to read message: %v", err)
		}

		var received Event
		if err := json.Unmarshal(message, &received); err != nil {
			t.Fatalf("Failed to unmarshal received message: %v", err)
		}

		if received.Name != "test-event" {
			t.Errorf("Expected event name test-event, got %s", received.Name)
		}
	})

	t.Run("Inbound Flow", func(t *testing.T) {
		clientMsg := Event{
			Name: "client-message",
			Data: json.RawMessage(`{"text":"hello server"}`),
		}
		msgBytes, _ := json.Marshal(clientMsg)

		if err := ws.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
			t.Fatalf("Failed to write message: %v", err)
		}

		// Wait for the message to appear in the Events() pipe
		select {
		case ctx := <-gs.Events():
			if ctx.Event.Name != "client-message" {
				t.Errorf("Expected event client-message, got %s", ctx.Event.Name)
			}
			var data map[string]string
			json.Unmarshal(ctx.Event.Data, &data)
			if data["text"] != "hello server" {
				t.Errorf("Expected text 'hello server', got '%s'", data["text"])
			}
		case <-time.After(500 * time.Millisecond):
			t.Error("Timed out waiting for inbound event")
		}
	})
}
