# Documentation

GoSync is a Socket.IO server written in Go. It implements Engine.IO 4 and
Socket.IO protocol 5 for Socket.IO 4.x clients.

Start here:

- [Getting started](getting-started.md) explains installation and runs a small
  server.
- [How it works](how-it-works.md) follows a connection from HTTP to an event
  handler.
- [Events and rooms](events-and-rooms.md) covers namespaces, middleware,
  acknowledgements, binary values, rooms, and broadcasts.
- [Configuration](configuration.md) lists every server option and its default.
- [Recovery and Redis](recovery-and-redis.md) covers temporary disconnections
  and multi-process broadcasts.
- [Go client](go-client.md) shows how to connect from another Go program.
- [WebTransport](webtransport.md) explains the optional HTTP/3 transport.

Reference material:

- [Protocol support](protocol-support.md)
- [Error handling](error-handling.md)
- [Testing](testing-plan.md)
- [Implementation status](implementation-status.md)

The public packages can also be inspected with Go documentation:

```sh
go doc github.com/avenka29/gosync
go doc github.com/avenka29/gosync/client
go doc github.com/avenka29/gosync/adapter/redis
```
