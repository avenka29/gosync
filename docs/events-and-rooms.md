# Events and rooms

## Namespaces

The default namespace is available from `server.Default()`. Register another
namespace before clients connect:

```go
admin, err := server.Of("/admin")
if err != nil {
	return err
}
```

Namespace names begin with `/`. Unknown namespaces receive a Socket.IO
connection error.

## Authentication and middleware

Middleware runs before a socket joins a namespace. `Socket.Auth()` contains the
decoded authentication value sent by the client.

```go
admin.Use(func(ctx context.Context, socket *gosync.Socket) error {
	auth, ok := socket.Auth().(map[string]any)
	if !ok || auth["token"] != expectedToken {
		return errors.New("invalid token")
	}
	return nil
})
```

A middleware error rejects the namespace connection with a generic `Not
authorized` message. The original error is available through `Config.OnError`
without exposing authentication details to the client.

## Connection lifecycle

```go
admin.OnConnect(func(ctx context.Context, socket *gosync.Socket) {
	log.Printf("connected %s", socket.ID())

	socket.OnDisconnect(func(reason string) {
		log.Printf("disconnected %s: %s", socket.ID(), reason)
	})
})
```

`Socket.Context()` is cancelled when that namespace connection closes. Use it
for work that should stop with the socket.

`Socket.Request()` returns a body-free copy of the original Engine.IO handshake
request. It can be used to inspect headers or remote address information.

## Receiving events

Register a handler on a namespace for all its sockets:

```go
err := server.Default().On("save", func(
	ctx context.Context,
	socket *gosync.Socket,
	args []any,
	ack gosync.Ack,
) error {
	if ack != nil {
		return ack(ctx, map[string]any{"saved": true})
	}
	return nil
})
```

`args` contains the values after the event name. `ack` is nil when the client
did not request an acknowledgement. An acknowledgement can be called once.

Use `socket.On` to install a handler for one socket. A socket handler takes
precedence over a namespace handler with the same event name.

The reserved event names `connect`, `connect_error`, `disconnect`,
`disconnecting`, `newListener`, and `removeListener` cannot be registered or
emitted.

## Sending events and acknowledgements

```go
if err := socket.Emit(ctx, "updated", record); err != nil {
	return err
}

reply, err := socket.EmitWithAck(ctx, "confirm", record.ID)
if err != nil {
	return err
}
```

`EmitWithAck` also applies the server's `AckTimeout`. Cancellation, timeout, or
disconnection removes the pending acknowledgement.

Byte slices are sent as Socket.IO binary attachments. Maps, slices, strings,
numbers, booleans, nil values, and structs supported by `encoding/json` can be
used as event arguments.

## Rooms and broadcasts

Every socket starts in a private room named after its socket ID.

```go
if err := socket.Join("account:42", "updates"); err != nil {
	return err
}
socket.Leave("updates")
rooms := socket.Rooms()
```

The private room counts toward `MaxRoomsPerSocket`. `Rooms` returns a sorted
snapshot.

Broadcast to every socket in a namespace:

```go
err := server.Default().Emit(ctx, "news", article)
```

Select rooms or exclusions:

```go
err := server.Default().
	To("account:42", "staff").
	Except("muted").
	Emit(ctx, "news", article)
```

Multiple `To` rooms use union semantics: a socket is selected when it belongs
to at least one room. Any matching exclusion removes it from the selection.

A socket can broadcast without sending back to itself:

```go
err := socket.To("updates").Emit(ctx, "changed", record)
err = socket.Broadcast().Emit(ctx, "presence", socket.ID())
```

Broadcast acknowledgements return one response per socket ID:

```go
responses, err := server.Default().To("workers").EmitWithAck(ctx, "health")
```

## Disconnecting

```go
err := socket.Disconnect(ctx, false)
```

Passing `false` closes only that namespace. Passing `true` closes the underlying
Engine.IO connection and every namespace on it.
