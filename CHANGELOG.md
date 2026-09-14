# Changelog

This file records user-visible changes to GoSync. Releases follow semantic
versioning.

## 1.0.0 - 2026-09-13

### Added

- Engine.IO 4 polling, WebSocket, upgrade, and WebTransport transports
- Socket.IO 5 namespaces, middleware, events, binary payloads, and acknowledgements
- Room, broadcast, timeout, and connection state recovery APIs
- Redis-backed broadcasts and acknowledgements across GoSync servers
- Go, JavaScript, and C++ client compatibility checks
- Protocol conformance, race, fuzz, integration, and security tests

### Security

- Same-origin browser policy with configurable origin validation
- Request, payload, queue, namespace, and event limits
- Constant-time recovery token comparison
- Bounded acknowledgement tracking and shutdown cleanup
