package gosync

import (
	"github.com/gorilla/websocket"
)


// Client represents a single websocket client connection
type Client struct {

	//Central hub for managing connections
	hub *Hub

	//The websocket connection for this client
	conn *websocket.Conn

	//The channel for sending messages to the hub
	send chan []byte
}

//Setups up client by adding it to the hub and starting the read/write goroutines
func setupClient(hub *Hub, conn *websocket.Conn) {

	client := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte),
	}

	hub.register <- client
}