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

func TestServer_Rooms(t *testing.T) {
	// 1. Setup the Server
	gs := NewServer()
	ts := httptest.NewServer(gs)
	defer ts.Close()

	// Convert http URL to ws URL
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	// 2. Connect client 1
	dialer := websocket.Dialer{}
	ws1, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to connect ws1: %v", err)
	}
	defer ws1.Close()

	// 3. Connect client 2
	ws2, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to connect ws2: %v", err)
	}
	defer ws2.Close()

	// Give the hub a moment to register both clients
	time.Sleep(50 * time.Millisecond)

	// Send identify message from client 1
	clientMsg1 := Event{Name: "id1", Data: json.RawMessage(`{}`)}
	msgBytes1, _ := json.Marshal(clientMsg1)
	ws1.WriteMessage(websocket.TextMessage, msgBytes1)

	// Send identify message from client 2
	clientMsg2 := Event{Name: "id2", Data: json.RawMessage(`{}`)}
	msgBytes2, _ := json.Marshal(clientMsg2)
	ws2.WriteMessage(websocket.TextMessage, msgBytes2)

	var client1, client2 *Client
	for i := 0; i < 2; i++ {
		select {
		case ctx := <-gs.Events():
			if ctx.Event.Name == "id1" {
				client1 = ctx.Client
			} else if ctx.Event.Name == "id2" {
				client2 = ctx.Client
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Timed out waiting for client identity messages")
		}
	}

	if client1 == nil || client2 == nil {
		t.Fatal("Failed to obtain client pointers")
	}

	// 4. Join room-a for client1
	gs.JoinRoom(client1, "room-a")
	time.Sleep(20 * time.Millisecond)

	// 5. Broadcast to room-a
	testData := map[string]string{"msg": "room only"}
	err = gs.BroadcastToRoom("room-a", "room-event", testData)
	if err != nil {
		t.Fatalf("BroadcastToRoom failed: %v", err)
	}

	// Client 1 should receive the message
	_, message1, err := ws1.ReadMessage()
	if err != nil {
		t.Fatalf("ws1 failed to read: %v", err)
	}
	var rec1 Event
	json.Unmarshal(message1, &rec1)
	if rec1.Name != "room-event" {
		t.Errorf("Expected room-event, got %s", rec1.Name)
	}

	// Client 2 should NOT receive the message
	ws2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	_, _, err = ws2.ReadMessage()
	if err == nil {
		t.Error("Client 2 received message, but it was not in room-a")
	}
	// Reset deadline for ws2
	ws2.SetReadDeadline(time.Time{})

	// 6. Join room-a for client2
	gs.JoinRoom(client2, "room-a")
	time.Sleep(20 * time.Millisecond)

	// 7. Broadcast to room-a
	gs.BroadcastToRoom("room-a", "room-event-2", testData)

	// Both should receive it
	_, message1, _ = ws1.ReadMessage()
	json.Unmarshal(message1, &rec1)
	if rec1.Name != "room-event-2" {
		t.Errorf("Expected room-event-2 for client1, got %s", rec1.Name)
	}

	_, message2, err := ws2.ReadMessage()
	if err != nil {
		t.Fatalf("ws2 failed to read: %v", err)
	}
	var rec2 Event
	json.Unmarshal(message2, &rec2)
	if rec2.Name != "room-event-2" {
		t.Errorf("Expected room-event-2 for client2, got %s", rec2.Name)
	}

	// 8. Leave room-a for client1
	gs.LeaveRoom(client1, "room-a")
	time.Sleep(20 * time.Millisecond)

	// 9. Broadcast to room-a
	gs.BroadcastToRoom("room-a", "room-event-3", testData)

	// Client 2 should receive it
	_, message2, _ = ws2.ReadMessage()
	json.Unmarshal(message2, &rec2)
	if rec2.Name != "room-event-3" {
		t.Errorf("Expected room-event-3 for client2, got %s", rec2.Name)
	}

	// Client 1 should NOT receive it
	ws1.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	_, _, err = ws1.ReadMessage()
	if err == nil {
		t.Error("Client 1 received message after leaving room-a")
	}
	ws1.SetReadDeadline(time.Time{})
}
