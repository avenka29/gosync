# GoSync

[![CI](https://github.com/avenka29/gosync/actions/workflows/ci.yml/badge.svg)](https://github.com/avenka29/gosync/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/avenka29/gosync.svg)](https://pkg.go.dev/github.com/avenka29/gosync)
[![Go version](https://img.shields.io/badge/Go-1.26.6-00ADD8?logo=go&logoColor=white)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

GoSync is a Socket.IO server for Go. It implements Engine.IO 4 and Socket.IO 5
for compatibility with Socket.IO 4.x clients. Its API follows Go conventions
instead of reproducing the Node.js server API.

The server supports HTTP polling, WebSocket, polling upgrades, namespaces,
middleware, acknowledgements, binary events, rooms, broadcasts, connection
recovery, and graceful shutdown. WebTransport and Redis-backed broadcasts are
optional. The `client` package provides a compatible Go client.

## Requirements

- Go 1.26.6 or later
- Redis 8 when using the Redis adapter
- TLS and HTTP/3 when using WebTransport

## Install

```sh
go get github.com/avenka29/gosync
```

## Example

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

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(httpServer.ListenAndServe())
}
```

Mount the server at `/socket.io/` when sharing an HTTP mux with other handlers.
Call `Server.Close` during shutdown so sessions and broker workers can finish.

The default origin check accepts non-browser requests and same-origin browser
requests. Configure `CheckOrigin` for other browser origins. Production servers
should use TLS and explicit HTTP timeouts.

Connection recovery is stored in memory and requires sticky routing. The Redis
adapter distributes live events and acknowledgements between GoSync nodes. It
does not replicate recovery state, and its wire format is specific to GoSync.

## Documentation

- [Getting started](docs/getting-started.md)
- [Configuration](docs/configuration.md)
- [Events and rooms](docs/events-and-rooms.md)
- [Recovery and Redis](docs/recovery-and-redis.md)
- [Go client](docs/go-client.md)
- [WebTransport](docs/webtransport.md)
- [Protocol support](docs/protocol-support.md)
- [Testing](docs/testing-plan.md)

The [documentation index](docs/README.md) links to every guide.

## Development

```sh
go mod verify
go test -race ./...
go vet ./...
```

The test suite includes fuzz targets, Redis and WebTransport integration tests,
the official Engine.IO and Socket.IO protocol suites, and JavaScript, Go, and
C++ client checks. See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete local
verification commands.

## Versioning

GoSync follows semantic versioning. The first stable release is `v1.0.0`.
Protocol compatibility and known differences are recorded in
[protocol support](docs/protocol-support.md).

## License

GoSync is available under the [MIT License](LICENSE).
