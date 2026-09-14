# Implementation status

GoSync implements Engine.IO revision 4 and Socket.IO protocol revision 5 for
Socket.IO 4.x clients. The implementation includes polling, WebSocket,
polling-to-WebSocket upgrades, optional WebTransport, namespaces,
authorization middleware, events, acknowledgements, binary attachments, rooms,
broadcasts, graceful shutdown, bounded local recovery, a Redis broker, and a Go
client facade.

The verification baseline on September 13, 2026 is:

- 88 Go test cases and three decoder fuzz targets
- 25 consecutive shuffled, race-enabled runs of every Go package, including
  the real Redis integration and direct and upgraded WebTransport tests
- 100 consecutive race-enabled runs of the shutdown, acknowledgement,
  recovery, room-limit, origin, and cluster boundary regressions
- CGO-free builds for Linux amd64, Linux arm64, Linux 386, and Windows amd64
- 24/24 pinned official Engine.IO protocol checks
- 32/32 pinned official Socket.IO protocol checks
- 6/6 Socket.IO JavaScript 4.8.3 client scenarios
- Go client polling-upgrade and direct-WebSocket scenarios
- patched official C++ client namespace and binary-acknowledgement scenario
- 83.1% aggregate statement coverage with Redis integration enabled
- clean gofumpt, `go vet`, Staticcheck, GoCritic, complexity, dead-code, gosec,
  Actionlint, module checksum verification, npm audit, and govulncheck results
- refreshed Graphify code graph with 592 nodes and 1,403 edges, followed by
  transport, delivery, recovery, clustering, and security path queries
- 2,109,352 Engine.IO frame fuzz executions, 4,813,941 Engine.IO payload fuzz
  executions, and 840,814 Socket.IO header fuzz executions in 60-second runs

The vulnerability scan reports no reachable vulnerabilities. Security still
depends on deployment choices: use TLS, authenticate application connections,
set an explicit cross-origin policy when needed, protect Redis, keep dependency
scanning enabled, and set HTTP server deadlines.

The Go client dependency still uses the WebTransport `Dialer` API. GoSync pins
quic-go 0.60 and webtransport-go 0.11 because the next releases change or remove
that API and do not compile with the current client. Other direct and transitive
dependencies use their current compatible releases.

The Redis wire format is specific to GoSync and is not compatible with the
Node.js Redis adapter. Recovery state is local and needs sticky routing. Event
delivery remains at most once unless the application adds durable storage and
deduplication.
