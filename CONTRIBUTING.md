# Contributing

GoSync accepts bug fixes, tests, documentation improvements, and focused
features that fit the supported protocol scope.

## Setup

Install Go 1.26.6, Node.js 24, npm, Git, CMake, ShellCheck, and Redis 8. Clone
the repository, then download the Go modules and JavaScript test dependencies.

```sh
go mod download
npm ci --ignore-scripts --prefix integration
```

## Development checks

Run these checks before opening a pull request.

```sh
go run mvdan.cc/gofumpt@v0.12.0 -w .
go mod tidy
go mod verify
go test -race ./...
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
go run github.com/go-critic/go-critic/cmd/gocritic@v0.15.0 check ./...
go run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 -over 35 .
go run golang.org/x/tools/cmd/deadcode@v0.49.0 -test ./...
go run github.com/securego/gosec/v2/cmd/gosec@v2.29.0 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
shellcheck scripts/*.sh
npm audit --audit-level=low --prefix integration
```

Start Redis on `127.0.0.1:6379` and set `GOSYNC_REDIS_ADDR` to include the Redis
integration tests. Run `bash scripts/conformance.sh` to clone and execute the
pinned official protocol suites. The CI workflow also builds the pinned C++
client fixture.

Add tests for observable behavior and boundary conditions. Keep exported APIs
documented, errors actionable, and comments limited to facts the code cannot
express clearly.

## Pull requests

Keep each pull request focused. Explain the behavior change, compatibility
impact, and commands used to verify it. Update the guides and changelog when an
API or protocol behavior changes.

Report security issues using the process in [SECURITY.md](SECURITY.md).
