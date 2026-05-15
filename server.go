package gosync

import (
	"net/http"

	"github.com/gorilla/websocket"

	"log"
)

type Server struct {

	// Central hub for internal state and operation management
	hub *Hub

	// Websocket upgrader that turns intiial http -> Websocket
	upgrader websocket.Upgrader
}

// Constructor to create a new server
func NewServer() *Server {

	hub := NewHub()

	websocketUpgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool { // [TODO] CHANGE THIS TO MORE ADVANCED CORS SETUP
			return true
		},
	}

	return &Server{
		hub:      hub,
		upgrader: websocketUpgrader,
	}

}

// ServeHTTP acts as the entry point when requests hit the registered route.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// Attempt to upgrade the http request to a websocket connection
	conn, err := s.upgrader.Upgrade(w, r, nil)

	if err != nil {
		http.Error(w, "Websocket Handshake Failed: "+err.Error(), http.StatusBadRequest)
		log.Printf("Upgrade error: %v", nil)
	}

}
