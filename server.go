package gosync

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/websocket"

	"log"
)

// Read and write buffer sizes for websocket connections
const READ_BUFFER_SIZE = 1024
const WRITE_BUFFER_SIZE = 1024

type Server struct {

	// Central hub for internal state and operation management
	hub *Hub

	// Websocket upgrader that turns intiial http -> Websocket
	upgrader websocket.Upgrader
}

// Constructor to create a new server
func NewServer() *Server {

	hub := NewHub()
	go hub.Run()

	websocketUpgrader := websocket.Upgrader{
		ReadBufferSize:  READ_BUFFER_SIZE,
		WriteBufferSize: WRITE_BUFFER_SIZE,
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
		log.Printf("Upgrade error: %v", err)
		return
	}

	// Hand the connection over to the hub to manage
	s.hub.handleConnection(conn)
}

// Given an interface form of an event, broadcast it to all connected clients
func (s *Server) BroadcastEvent(name string, data interface{}) error {
	eventContext, err := createEventContext(name, data)

	if err != nil {
		return ErrServerMsgInvalid
	}
	s.hub.broadcast <- eventContext

	return nil
}

// Creates internal event context object, along with the external event
// Along with a json represenation of the external event
func createEventContext(name string, data interface{}) (*EventContext, error) {
	payload, err := json.Marshal(data)

	if err != nil {
		return nil, err
	}

	event := &Event{
		Name: name,
		Data: payload,
	}

	event_json, error := json.Marshal(event)

	if error != nil {
		return nil, error
	}

	eventContext := &EventContext{
		Event:  event,
		Raw:    event_json,
		Client: nil,
	}

	return eventContext, nil

}

// Public read only channel for incoming events from clients
func (s *Server) Events() <-chan *EventContext {
	return s.hub.EventPipe
}
