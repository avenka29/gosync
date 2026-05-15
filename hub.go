package gosync

// Internal hub that contains in memory state and the central coroutine
// Maintains list of registered clients, and for clients channels to register/unregister, and communicate with the central coroutine
type Hub struct {

	// Central state with all clients
	clients map[*Client]bool

	// Incoming messages from clients
	broadcast chan []byte

	// Incoming connection requests
	register chan *Client

	//Incoming disconnect requests
	unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan (*Client)),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}
