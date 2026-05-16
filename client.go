package gosync

import (
	"encoding/json"

	"github.com/gorilla/websocket"
)

// Client represents a single websocket client connection
type Client struct {
	// Central hub for managing connections
	hub *Hub

	// The websocket connection for this client
	conn *websocket.Conn

	// The channel for sending messages to the hub
	send chan []byte
}

func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 256),
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
		if err != nil{
			continue
		}

		c.hub.EventPipe <- &EventContext{Event: event, Client: c}
	}
}

// writeMessage pumps messages from the hub to the websocket connection.
func (c *Client) writeMessage() {
	defer func() {
		c.conn.Close()
	}()

	// Hub sends Event payloads to the client via the channel, so the client does not need to marshall the data
	for message := range c.send {
		err := c.conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			return
		}
	}
}
