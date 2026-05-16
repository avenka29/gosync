package gosync

import (
	"encoding/json"

	"github.com/gorilla/websocket"
)

// Internal hub that contains in memory state and the central coroutine
// Maintains list of registered clients, and for clients channels to register/unregister, and communicate with the central coroutine
type Hub struct {

	// Central state with all clients
	clients map[*Client]bool

	// Incoming messages from clients
	broadcast chan *Event

	// Incoming connection requests
	register chan *Client

	//Incoming disconnect requests
	unregister chan *Client

	// Contains incoming events from clients
	EventPipe chan *EventContext
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *Event),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		EventPipe: make(chan *EventContext, 1024),
	}
}

// Adds a client to the hub's client map
func (h *Hub) registerClient(client *Client) {
	h.clients[client] = true
}

// Removes a client from the hub's client map
func (h *Hub) unregisterClient(client *Client) {
	delete(h.clients, client)
}

// broadcastMessage sends a json payload to all registered clients through channel
func (h *Hub) broadcastMessage(event Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	for client := range h.clients {
		select {
		case client.send <- payload:
			//Message sent
		default: //Unregister problematic clients for now
			close(client.send)
			h.unregisterClient(client)
		}
	}
}

// HandleConnection creates a new client, registers it with the hub, and starts its pumps.
func (h *Hub) handleConnection(conn *websocket.Conn) {
	client := NewClient(h, conn)
	h.register <- client

	go client.writeMessage()
	go client.readMessage()
}

// Main couroutine that runs the hub, handling incoming messages and client registrations/unregistrations
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)
		case client := <-h.unregister:
			h.unregisterClient(client)
		case message := <-h.broadcast:
			h.broadcastMessage(*message)
		}
	}
}
