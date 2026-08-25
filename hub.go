package gosync

import (
	"github.com/gorilla/websocket"
)

// Event pipe size
const EVENT_PIPE_SIZE = 1024

type roomOp struct {
	client *Client
	room   string
}

// Internal hub that contains in memory state and the central coroutine
// Maintains list of registered clients, and for clients channels to register/unregister, and communicate with the central coroutine
type Hub struct {

	// Central state with all clients
	clients map[*Client]bool

	// Incoming messages from clients
	broadcast chan *EventContext

	// Incoming connection requests
	register chan *Client

	//Incoming disconnect requests
	unregister chan *Client

	// Contains incoming events from clients
	EventPipe chan *EventContext

	// Central state mapping room names to member clients
	rooms map[string]map[*Client]bool

	// Channel for clients to join a room
	joinRoom chan roomOp

	// Channel for clients to leave a room
	leaveRoom chan roomOp
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *EventContext),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		EventPipe:  make(chan *EventContext, EVENT_PIPE_SIZE),
		rooms:      make(map[string]map[*Client]bool),
		joinRoom:   make(chan roomOp),
		leaveRoom:  make(chan roomOp),
	}
}

// Adds a client to the hub's client map
func (h *Hub) registerClient(client *Client) {
	h.clients[client] = true
}

// Registers a client to a specific room
func (h *Hub) registerClientToRoom(client *Client, room string) {
	if h.rooms[room] == nil {
		h.rooms[room] = make(map[*Client]bool)
	}
	h.rooms[room][client] = true
	client.rooms[room] = true
}

// Unregisters a client from a specific room
func (h *Hub) unregisterClientFromRoom(client *Client, room string) {
	if clients, exists := h.rooms[room]; exists {
		delete(clients, client)
		if len(clients) == 0 {
			delete(h.rooms, room)
		}
	}
	delete(client.rooms, room)
}

// Removes a client from the hub's client map and all rooms
func (h *Hub) unregisterClient(client *Client) {
	for room := range client.rooms {
		h.unregisterClientFromRoom(client, room)
	}
	delete(h.clients, client)
}

// broadcastMessage sends pointer to event context to the client websockets
func (h *Hub) broadcastMessage(event *EventContext) {
	if event.Room != "" {
		// Broadcast to a specific room
		clients, exists := h.rooms[event.Room]
		if !exists {
			return
		}
		for client := range clients {
			select {
			case client.send <- event:
				//Message sent
			default:
				close(client.send)
				h.unregisterClient(client)
			}
		}
		return
	}

	// Global broadcast (Room is empty)
	for client := range h.clients {
		select {
		case client.send <- event:
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
		case op := <-h.joinRoom:
			h.registerClientToRoom(op.client, op.room)
		case op := <-h.leaveRoom:
			h.unregisterClientFromRoom(op.client, op.room)
		case message := <-h.broadcast:
			h.broadcastMessage(message)
		}
	}
}
