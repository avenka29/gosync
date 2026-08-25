package gosync

import "encoding/json"

// Event represents a websocket event with a name and data payload.
type Event struct {
	Name string          `json:"name"`
	Data json.RawMessage `json:"data"`
}

// Additional internal context per event
type EventContext struct {
	Event  *Event
	Raw    []byte // Raw json representation of message
	Client *Client
	Room   string // Target room for the event (empty for global broadcast)
}

