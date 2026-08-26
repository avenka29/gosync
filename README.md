GoSync

GoSync is a Go server for Socket.IO 4.x clients. It implements Engine.IO
revision 4 and Socket.IO protocol revision 5.

The server currently supports Engine.IO text and binary packets over WebSocket,
the Engine.IO handshake and heartbeat, Socket.IO packets and acknowledgements,
namespaces in the packet codec, and binary attachments. Integration tests cover
the WebSocket handshake, Socket.IO connection, and event exchange.

The public Socket.IO event API, namespace lifecycle, rooms, HTTP long-polling,
and polling-to-WebSocket upgrades are still being built. Compatibility will not
be claimed until the official protocol tests and JavaScript and C++ client tests
pass.

Run these checks before submitting a change:

    go test ./...
    go test -race ./...
    go vet ./...

The supported protocol versions and planned features are listed in
docs/protocol-support.md.
