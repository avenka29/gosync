# Getting started

GoSync accepts Socket.IO 4.x clients over HTTP long-polling and WebSocket. A
client normally starts with polling and upgrades to WebSocket when both sides
allow it.

## Install

Use the Go version declared in `go.mod`, then add GoSync to your module:

```sh
go get github.com/avenka29/gosync
```

For a JavaScript client:

```sh
npm install socket.io-client@4
```

## Run a server

```go
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/avenka29/gosync"
)

func main() {
	server, err := gosync.NewServer(gosync.Config{})
	if err != nil {
		log.Fatal(err)
	}

	err = server.Default().On("echo", func(
		ctx context.Context,
		socket *gosync.Socket,
		args []any,
		ack gosync.Ack,
	) error {
		if ack != nil {
			return ack(ctx, args...)
		}
		return socket.Emit(ctx, "echo", args...)
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)
	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Fatal(httpServer.ListenAndServe())
}
```

Start it with:

```sh
go run .
```

The repository includes a similar server in `cmd/main.go` with signal handling
and graceful shutdown.

## Connect from JavaScript

```js
import { io } from "socket.io-client";

const socket = io("http://localhost:8080", {
  auth: { token: "example" },
});

socket.emit("echo", "hello", (reply) => {
  console.log(reply);
});
```

The standard Socket.IO client uses `/socket.io/` automatically. If a reverse
proxy changes that path, configure the same path on the client and mount the Go
handler there.

## Shut down

Stop accepting HTTP traffic and close the Socket.IO server with a deadline:

```go
shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := server.Close(shutdownContext); err != nil {
	log.Printf("Socket.IO shutdown: %v", err)
}
if err := httpServer.Shutdown(shutdownContext); err != nil {
	log.Printf("HTTP shutdown: %v", err)
}
```

`Server.Close` stops new sessions, disconnects current sessions, closes the
configured broker, and waits for handlers until the context expires.
