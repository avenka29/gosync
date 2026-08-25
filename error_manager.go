package gosync

// Size of the internal error channel
const INTERNAL_ERROR_CHANNEL_SIZE = 1024


// Size of the public error pipe channel
const ERROR_PIPE_CHANNEL_SIZE = 256


// This error manager runs a dedicated goroutine that will ingest errors acros
// the system, and ensure that users can handle errors
type ErrorManager struct{

	// Public error channel that takes in all errors across the system
	ErrorPipe chan *ErrorContext

	// Internal error channel that is a landing zone for errors
	internalErrorChannel chan *ErrorContext
}

func NewErrorManager() *ErrorManager{
	return &ErrorManager{
		ErrorPipe: make(chan *ErrorContext, 256),
		internalErrorChannel: make(chan *ErrorContext, 1024),
	}
}

// Main goroutine for handling threads
// The server calls this method to start the error manager
func (em *ErrorManager) Run(){
}


