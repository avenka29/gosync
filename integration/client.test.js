import assert from "node:assert/strict";
import { test } from "node:test";

import { io } from "socket.io-client";

const baseURL = process.env.GOSYNC_URL || "http://127.0.0.1:3000";

function waitForEvent(socket, event, timeout = 3000) {
  return new Promise((resolve, reject) => {
    const listener = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    const timer = setTimeout(() => {
      socket.off(event, listener);
      reject(new Error(`timeout: ${event}`));
    }, timeout);
    socket.once(event, listener);
  });
}

for (const transports of [
  ["polling"],
  ["websocket"],
  ["polling", "websocket"],
]) {
  test(`events, binary ACKs, heartbeats: ${transports}`, { timeout: 10000 }, async (t) => {
    const socket = io(baseURL, {
      transports,
      autoConnect: false,
      reconnection: false,
      auth: { token: "client" },
    });
    t.after(() => socket.close());

    const connected = waitForEvent(socket, "connect");
    const auth = waitForEvent(socket, "auth");
    socket.connect();
    await connected;
    assert.deepEqual((await auth)[0], { token: "client" });

    if (transports.length > 1 && socket.io.engine.transport.name !== "websocket") {
      await waitForEvent(socket.io.engine, "upgrade");
    }

    const binary = Buffer.from([0, 1, 127, 255]);
    const response = await socket
      .timeout(3000)
      .emitWithAck("message-with-ack", binary, { nested: [true, "hello"] });
    assert.deepEqual(response, binary);

    for (let index = 0; index < 30; index++) {
      const reply = waitForEvent(socket, "message-back");
      socket.emit("message", index);
      assert.equal((await reply)[0], index);
    }

    await new Promise((resolve) => setTimeout(resolve, 1200));
    assert.equal(socket.connected, true);
  });
}

test("multiplex namespaces and independent disconnect", { timeout: 5000 }, async (t) => {
  const root = io(baseURL, { autoConnect: false, reconnection: false });
  const custom = root.io.socket("/custom", { auth: { role: "test" } });
  t.after(() => {
    custom.close();
    root.close();
  });

  const rootConnected = waitForEvent(root, "connect");
  const customConnected = waitForEvent(custom, "connect");
  root.connect();
  custom.connect();
  await Promise.all([rootConnected, customConnected]);
  assert.notEqual(root.id, custom.id);

  custom.disconnect();
  const reply = waitForEvent(root, "message-back");
  root.emit("message", "still alive");
  assert.equal((await reply)[0], "still alive");
  root.disconnect();
});

test("unknown namespace rejected", { timeout: 5000 }, async (t) => {
  const socket = io(`${baseURL}/unknown`, {
    autoConnect: false,
    reconnection: false,
  });
  t.after(() => socket.close());

  const rejected = waitForEvent(socket, "connect_error");
  socket.connect();
  assert.equal((await rejected)[0].message, "Invalid namespace");
});

test("automatic reconnect obtains a fresh namespace", { timeout: 6000 }, async (t) => {
  const socket = io(baseURL, {
    autoConnect: false,
    reconnectionDelay: 20,
    reconnectionDelayMax: 20,
  });
  t.after(() => socket.close());

  let connected = waitForEvent(socket, "connect");
  socket.connect();
  await connected;
  const previousID = socket.id;

  connected = waitForEvent(socket, "connect");
  socket.io.engine.close();
  await connected;
  assert.notEqual(socket.id, previousID);

  const response = await socket
    .timeout(2000)
    .emitWithAck("message-with-ack", "reconnected");
  assert.equal(response, "reconnected");
});
