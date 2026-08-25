package gosync

import(
	"errors"

	"time"
) 

// Internal Event Struct

type ErrorContext struct {
	Error error
	Client *Client
	Time time.Time
}

// Message errors
var (

	// This message occurs when the client does not send an appropriate json object
	// That can be marshalled into an event struct
	// Check to ensure that the client is sending formatted Event
	ErrServerMsgInvalid = errors.New("gosync: invalid server-side json message trying to be sent")

	
)