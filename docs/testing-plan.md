Testing

Packet codecs are tested without network connections. Tests cover valid packets,
invalid packet types, malformed JSON, size limits, namespaces, acknowledgements,
and binary attachments. Fuzz tests check that malformed input returns an error
instead of causing a panic.

Session tests cover the handshake, heartbeat, packet ordering, queue limits,
shutdown, and errors from the transport. Timers and transports should be
controlled by the test so timing assertions do not depend on sleeps.

HTTP and WebSocket integration tests cover handshake validation, message
exchange, rejected protocol revisions, origin checks, and server shutdown.
Polling and transport-upgrade tests will be added with those transports.

The complete test suite must pass with go test, the race detector, and go vet.
Compatibility tests will use the official Engine.IO and Socket.IO suites plus
current JavaScript and C++ Socket.IO clients.
