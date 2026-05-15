package gosync

import (
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
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		c.hub.broadcast <- message
	}
}

// writeMessage pumps messages from the hub to the websocket connection.
func (c *Client) writeMessage() {
	defer func() {
		c.conn.Close()
	}()

	for message := range c.send {
		err := c.conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			return
		}
	}
}
