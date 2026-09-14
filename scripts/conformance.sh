#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/.." && pwd)
fixture=${1:?Pass the built cmd/conformance executable}
fixture=$(cd "$(dirname "$fixture")" && pwd)/$(basename "$fixture")
cpp=${2:-}
work=$(mktemp -d "${TMPDIR:-/tmp}/gosync-conformance.XXXXXX")
fixture_pid=

cleanup() {
  if [[ -n "$fixture_pid" ]]; then
    kill -TERM "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

node <<'NODE'
const net = require("node:net");
const server = net.createServer();
server.on("error", () => {
  console.error("Port 3000 is occupied");
  process.exit(1);
});
server.listen(3000, "127.0.0.1", () => server.close());
NODE

fetch_suite() {
  local name=$1
  local commit=$2
  git clone --quiet "https://github.com/socketio/$name-protocol.git" "$work/$name"
  git -C "$work/$name" checkout --quiet --detach "$commit"
  ln -s "$project_dir/integration/node_modules" "$work/$name/test-suite/node_modules"
}

fetch_suite engine.io f21de7b00ed09b3bbad2807db718ea5d6bc36aba
fetch_suite socket.io 4f633dd6a4445492d57af3b27ddd2d336014f1f1

start_fixture() {
  "$fixture" "$@" >"$work/fixture.log" 2>&1 &
  fixture_pid=$!
  node --input-type=module <<'NODE'
for (let attempt = 0; attempt < 100; attempt++) {
  try {
    const response = await fetch("http://127.0.0.1:3000/?EIO=4&transport=polling");
    if (response.status === 200) {
      process.exit(0);
    }
  } catch {}
  await new Promise((resolve) => setTimeout(resolve, 50));
}
process.exit(1);
NODE
  kill -0 "$fixture_pid"
}

stop_fixture() {
  kill -TERM "$fixture_pid"
  wait "$fixture_pid"
  fixture_pid=
}

start_fixture -engine
(
  cd "$work/engine.io/test-suite"
  "$project_dir/integration/node_modules/.bin/mocha" test-suite.js
)
stop_fixture

start_fixture
(
  cd "$work/socket.io/test-suite"
  "$project_dir/integration/node_modules/.bin/mocha" test-suite.js
)
(
  cd "$project_dir/integration"
  npm test
)
if [[ -n "$cpp" ]]; then
  "$cpp"
fi
stop_fixture

printf 'Protocol suites and client integration passed. Fixture logs: %s\n' "$work"
