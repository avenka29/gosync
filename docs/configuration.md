# Configuration

Create a server with `gosync.NewServer(gosync.Config{...})`. Zero values use the
defaults below. Negative durations and limits return `gosync.ErrInvalidConfig`.

## Transport and timing

| Field | Default | Purpose |
| --- | ---: | --- |
| `EnableWebTransport` | `false` | Advertise and accept the optional WebTransport transport. |
| `PingInterval` | 25 seconds | Time between Engine.IO ping packets. |
| `PingTimeout` | 20 seconds | Time allowed for the matching pong. |
| `UpgradeTimeout` | 10 seconds | Time allowed to complete a transport upgrade. |
| `ConnectTimeout` | 20 seconds | Time allowed for the first Socket.IO namespace connection. |
| `AssemblyTimeout` | 10 seconds | Time allowed to receive all binary attachments. |
| `AckTimeout` | 10 seconds | Maximum wait for event acknowledgements. |
| `ReadBufferSize` | transport default | WebSocket read buffer size. |
| `WriteBufferSize` | transport default | WebSocket write buffer size. |

## Resource limits

| Field | Default | Purpose |
| --- | ---: | --- |
| `MaxPayload` | 1,000,000 bytes | Maximum Engine.IO payload and assembled Socket.IO packet size. |
| `InboundBuffer` | 64 | Queued incoming packets or handler jobs per connection. |
| `OutboundBuffer` | 64 | Queued outgoing Engine.IO packets per connection. |
| `MaxAttachments` | 32 | Binary attachments allowed in one Socket.IO packet. |
| `MaxJSONDepth` | 100 | Nested JSON containers allowed in one packet. |
| `MaxConnections` | 10,000 | Concurrent Engine.IO sessions. |
| `MaxNamespacesPerConnection` | 32 | Namespace connections on one Engine.IO session. |
| `MaxRoomsPerSocket` | 128 | Rooms per socket, including its private ID room. |
| `MaxPendingAcks` | 128 | Pending acknowledgements and cluster acknowledgement work. |

Limits close or reject the affected operation instead of allowing unbounded
queues or packet assembly. Choose smaller limits when the application's event
shape is known.

## Origins and CORS

With no `CheckOrigin` callback, GoSync accepts requests without an `Origin`
header and browser requests whose origin matches the request scheme, host, and
port.

Use an explicit allowlist when the browser application is hosted elsewhere:

```go
server, err := gosync.NewServer(gosync.Config{
	CheckOrigin: func(request *http.Request) bool {
		switch request.Header.Get("Origin") {
		case "https://app.example.com", "https://admin.example.com":
			return true
		default:
			return false
		}
	},
	AllowCredentials: true,
})
```

`AllowCredentials` adds the CORS credentials response header for origins
accepted by the custom callback. Avoid a callback that accepts every origin
when cookies or authorization headers are used.

## Error reporting

`OnError` receives errors from handlers, broker work, transport callbacks, and
recovered callback panics:

```go
server, err := gosync.NewServer(gosync.Config{
	OnError: func(err error) {
		log.Printf("socket.io: %v", err)
	},
})
```

Keep the callback quick and avoid logging authentication values, cookies,
authorization headers, or complete event payloads.

## Optional components

`Recovery` enables bounded local connection recovery. `Broker` connects server
processes for live broadcasts. Their configuration is covered in
[Recovery and Redis](recovery-and-redis.md).
