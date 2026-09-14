# Testing

`go test -race ./...` covers codecs, polling, WebSocket, WebTransport over a
real HTTP/3 listener, transport upgrades, heartbeats, namespace lifecycle,
authorization, rooms, broadcasts, acknowledgements, binary data, recovery,
backpressure, resource limits, callback panics, shutdown, and the Go client.

The Redis integration test requires a real Redis instance:

```sh
docker run --rm -p 127.0.0.1:16379:6379 redis:8-alpine
go test -race ./adapter/redis -args -redis-addr=127.0.0.1:16379
```

Protocol decoders have fuzz targets. A local extended run is:

```sh
go test ./internal/engineio -run='^$' -fuzz=FuzzDecodeFrame -fuzztime=30s
go test ./internal/engineio -run='^$' -fuzz=FuzzDecodePayload -fuzztime=30s
go test ./internal/socketio -run='^$' -fuzz=FuzzDecodeHeader -fuzztime=30s
```

Install JavaScript integration dependencies, build the server fixture, and run
all external compatibility checks with:

```sh
(cd integration && npm ci --ignore-scripts && npm audit --audit-level=low)
bash scripts/build-cpp-client.sh /tmp/gosync-cpp
go build -o /tmp/gosync-fixture ./cmd/conformance
bash scripts/conformance.sh /tmp/gosync-fixture /tmp/gosync-cpp/cpp-check
```

The scripts clone exact upstream commits so results do not silently change. The
C++ build applies `integration/cpp-engineio4.patch`, which makes the client use
the WebSocket opcode to recognize Engine.IO 4 binary attachments.

CI repeats race tests three times, enforces at least 80% cross-package coverage,
and runs vet, Staticcheck, GoCritic, complexity and dead-code analysis, gosec,
Actionlint, the Go vulnerability scanner, decoder fuzzing, npm audit, both
official protocol suites, and real JavaScript, Go, Redis, WebTransport, and C++
client checks.
