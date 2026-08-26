Protocol support

GoSync is a server for Socket.IO 4.x clients. It supports Engine.IO revision 4
and Socket.IO protocol revision 5. Older Engine.IO and Socket.IO revisions are
not supported.

Version 1 will include WebSocket and HTTP long-polling transports, transport
upgrades, JSON events, binary attachments, namespaces, acknowledgements, rooms,
ordered delivery on each connection, and standard net/http integration.

The first release will not include a Go client, custom parsers, durable delivery,
exactly-once delivery, a distributed room backend, or connection-state recovery.

Before compatibility is announced, the codecs must pass tests based on the
official protocol examples. The transports and Socket.IO server must pass the
official test suites. The server must also be tested with Socket.IO 4.x
JavaScript clients and the C++ client. All Go tests must pass with the race
detector and goroutine leak checks enabled.
