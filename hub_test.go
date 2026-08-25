package gosync

import (
	"testing"
	"time"
)

func TestHub_RegisterUnregister(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	// Create a mock client
	client := NewClient(hub, nil)

	// 1. Test Registration
	hub.register <- client
	
	// Give the hub a moment to process the channel
	time.Sleep(10 * time.Millisecond)

	if _, ok := hub.clients[client]; !ok {
		t.Error("Client failed to register with hub")
	}

	// 2. Test Unregistration
	hub.unregister <- client
	
	time.Sleep(10 * time.Millisecond)

	if _, ok := hub.clients[client]; ok {
		t.Error("Client failed to unregister from hub")
	}
}

func TestHub_Broadcast(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	// Create two mock clients
	client1 := NewClient(hub, nil)
	client2 := NewClient(hub, nil)

	hub.register <- client1
	hub.register <- client2
	
	time.Sleep(10 * time.Millisecond)

	// Create a test event
	testCtx := &EventContext{
		Event: &Event{Name: "test", Data: []byte(`"hello"`)},
		Raw:   []byte(`{"name":"test","data":"hello"}`),
	}

	// Broadcast the event
	hub.broadcast <- testCtx

	// Verify both clients received the event
	select {
	case received := <-client1.send:
		if string(received.Raw) != string(testCtx.Raw) {
			t.Errorf("Client 1 received wrong data: %s", string(received.Raw))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 1 timed out waiting for broadcast")
	}

	select {
	case received := <-client2.send:
		if string(received.Raw) != string(testCtx.Raw) {
			t.Errorf("Client 2 received wrong data: %s", string(received.Raw))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 2 timed out waiting for broadcast")
	}
}

func TestHub_SlowConsumer(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	// Create a client with a small buffer and fill it
	client := NewClient(hub, nil)
	hub.register <- client
	time.Sleep(10 * time.Millisecond)

	// Fill the client's send channel (buffer is 256)
	for i := 0; i < 256; i++ {
		client.send <- &EventContext{}
	}

	// Now broadcast one more message. The Hub should drop this client
	// instead of blocking the whole broadcast loop.
	hub.broadcast <- &EventContext{Raw: []byte("overflow")}
	
	time.Sleep(10 * time.Millisecond)

	if _, ok := hub.clients[client]; ok {
		t.Error("Slow consumer client should have been removed from hub")
	}
}

func TestHub_Rooms(t *testing.T) {
	hub := NewHub()
	go hub.Run()

	client1 := NewClient(hub, nil)
	client2 := NewClient(hub, nil)

	hub.register <- client1
	hub.register <- client2
	time.Sleep(10 * time.Millisecond)

	// 1. Join room-1 for client1
	hub.joinRoom <- roomOp{client: client1, room: "room-1"}
	time.Sleep(10 * time.Millisecond)

	testCtx := &EventContext{
		Event: &Event{Name: "test", Data: []byte(`"hello"`)},
		Raw:   []byte(`{"name":"test","data":"hello"}`),
		Room:  "room-1",
	}

	// 2. Broadcast to room-1
	hub.broadcast <- testCtx

	// client1 should receive it
	select {
	case received := <-client1.send:
		if string(received.Raw) != string(testCtx.Raw) {
			t.Errorf("Client 1 received wrong data: %s", string(received.Raw))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 1 timed out waiting for room broadcast")
	}

	// client2 should NOT receive it
	select {
	case <-client2.send:
		t.Error("Client 2 received message sent to room-1, but is not in room-1")
	case <-time.After(50 * time.Millisecond):
		// Success: client2 did not receive the message
	}

	// 3. Leave room-1 for client1
	hub.leaveRoom <- roomOp{client: client1, room: "room-1"}
	time.Sleep(10 * time.Millisecond)

	// Broadcast again to room-1
	hub.broadcast <- testCtx

	// client1 should NOT receive it now
	select {
	case <-client1.send:
		t.Error("Client 1 received message sent to room-1 after leaving")
	case <-time.After(50 * time.Millisecond):
		// Success
	}
}
