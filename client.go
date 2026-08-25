package gosync

import (
	"encoding/json"

	"github.com/gorilla/websocket"
)

// Max number of messages a client can have in their send channel
const MESSAGE_LIMIT = 256

// Client represents a single websocket client connection
type Client struct {
	// Central hub for managing connections
	hub *Hub

	// The websocket connection for this client
	conn *websocket.Conn

	// The channel for sending messages to the hub
	send chan *EventContext

	// Rooms the client is currently in
	rooms map[string]bool
}

func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:   hub,
		conn:  conn,
		send:  make(chan *EventContext, 256),
		rooms: make(map[string]bool),
	}
}

// readMessage pumps messages from the websocket connection to the hub.
func (c *Client) readMessage() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		_, rawMessage, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		// Unmarshal raw message in the pipe into an Event struct
		var event Event
		err = json.Unmarshal(rawMessage, &event)
		if err != nil {
			continue
		}

		eventContext := &EventContext{
			Event:  &event,
			Raw:    rawMessage,
			Client: c,
		}

		c.hub.EventPipe <- eventContext
	}
}

// writeMessage pumps messages from the hub to the websocket connection.
func (c *Client) writeMessage() {
	defer func() {
		c.conn.Close()
	}()

	// Hub sends EventContext objects to the client via the channel, client sends the pre-marshalled over socket
	for eventContext := range c.send {
		err := c.conn.WriteMessage(websocket.TextMessage, eventContext.Raw)
		if err != nil {
			return
		}
	}
}
