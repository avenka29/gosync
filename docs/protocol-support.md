# Protocol support

GoSync targets Socket.IO 4.x clients: Engine.IO revision 4 and Socket.IO
protocol revision 5. Older protocol revisions and custom packet parsers are not
supported.

Protocol compatibility means Socket.IO 4.x clients can complete the transport
handshake and exchange every packet type defined by Engine.IO 4 and Socket.IO
5. The public API follows Go conventions rather than copying the Node.js server
API method for method.

Supported Engine.IO behavior:

- HTTP long-polling and direct WebSocket sessions
- polling-to-WebSocket upgrades with probe, pause, and ordered handoff
- optional direct WebTransport and polling-to-WebTransport upgrades
- server-driven heartbeat and timeout handling
- text and binary packets, polling batching, and base64 polling payloads
- payload limits, same-origin validation, session cleanup, and graceful shutdown

Supported Socket.IO behavior:

- explicit default and custom namespace connections
- authentication data, authorization middleware, and connect errors
- events, binary events, acknowledgements, and binary acknowledgements
- socket and namespace handlers, room membership, targeted broadcasts,
  exclusions, sender exclusion, and broadcast acknowledgements
- per-namespace socket IDs and independent namespace disconnection
- bounded in-memory connection state recovery with room restoration and missed
  non-ack event replay
- optional Redis-backed multi-node broadcasts and acknowledgement collection

Recovery is local to one GoSync process and requires sticky routing. The Redis
broker carries ephemeral GoSync cluster messages and does not persist recovery
state. Delivery follows Socket.IO's default at-most-once behavior. Exactly-once
delivery, durable history, and database-backed replay belong in the application.

The Go client package is a facade over `github.com/zishang520/socket.io` v3. It
supports the same Socket.IO 4.x protocol but follows that project's API and
release lifecycle.

The following optional Node.js server features are outside the current API:

- dynamic namespaces selected by a regular expression or callback
- catch-all listeners and listener-removal helpers
- per-packet socket middleware and per-emission volatile or compression flags
- cluster-wide socket inspection, room mutation, forced disconnection, and
  server-side events
- custom parsers, bundled browser assets, and Admin UI integration

These API differences do not prevent standard Socket.IO 4.x clients from using
the supported namespaces, events, acknowledgements, binary data, rooms,
broadcasts, recovery, or transports described above.

Compatibility is checked against pinned official Engine.IO and Socket.IO test
suites, Socket.IO JavaScript client 4.8.3, the Go client, and the official C++
client with the repository's Engine.IO 4 binary-opcode patch.
