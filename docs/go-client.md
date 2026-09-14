# Go client

The `client` package provides the Socket.IO Go client used by the integration
tests. It is a small facade over `github.com/zishang520/socket.io` v3.

## Connect with polling and WebSocket

```go
package main

import (
	"log"
	"time"

	"github.com/avenka29/gosync/client"
	"github.com/zishang520/socket.io/clients/engine/v3/transports"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

func main() {
	options := client.DefaultOptions()
	options.SetAutoConnect(false)
	options.SetReconnection(true)
	options.SetTimeout(5 * time.Second)
	options.SetTransports(types.NewSet(transports.Polling, transports.WebSocket))

	socket, err := client.Connect("http://localhost:8080", options)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()

	done := make(chan error, 1)
	socket.On("connect", func(...any) {
		socket.Timeout(3*time.Second).EmitWithAck("echo", "hello")(
			func(args []any, err error) {
				if err == nil {
					log.Printf("reply: %#v", args)
				}
				done <- err
			},
		)
	})
	socket.On("connect_error", func(args ...any) {
		log.Printf("connect error: %#v", args)
	})

	socket.Connect()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
}
```

The transport set above starts with polling and allows an upgrade to WebSocket.
Use `types.NewSet(transports.WebSocket)` for a direct WebSocket connection.

## Connect to a namespace

Include the namespace in the URL:

```go
socket, err := client.Connect("http://localhost:8080/admin", options)
```

For several namespaces on one underlying connection, create a manager and ask
it for sockets:

```go
managerOptions := client.DefaultManagerOptions()
managerOptions.SetAutoConnect(false)
manager := client.NewManager("http://localhost:8080", managerOptions)

root := manager.Socket("/", client.DefaultSocketOptions())
admin := manager.Socket("/admin", client.DefaultSocketOptions())
root.Connect()
admin.Connect()
```

The facade follows the upstream client's API and release lifecycle. List the
re-exported entry points with:

```sh
go doc github.com/avenka29/gosync/client
```
