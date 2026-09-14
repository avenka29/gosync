# How it works

GoSync is an `http.Handler`. Mount it at `/socket.io/` and let the standard Go
HTTP server handle TCP connections and TLS.

A connection moves through these layers:

1. Engine.IO validates the request and opens a polling, WebSocket, or optional
   WebTransport session.
2. Engine.IO sends the handshake and runs the ping/pong heartbeat.
3. The client sends a Socket.IO connect packet for `/` or another registered
   namespace.
4. Namespace middleware checks the connection and its authentication payload.
5. GoSync creates a `Socket`, sends the namespace connection response, and runs
   the namespace connection callback.
6. Events are decoded and passed to a socket handler or namespace handler.
7. Outgoing events are encoded, split into binary attachments when needed, and
   written in order.

Each Engine.IO connection can contain several Socket.IO namespace connections.
Each namespace connection has its own `Socket` ID, handlers, rooms,
acknowledgements, and cancellation context.

Incoming event handlers run sequentially for one Engine.IO connection. Separate
connections can run at the same time. Outgoing packets are synchronized so a
binary packet's header and attachments cannot interleave with another packet.

Polling clients may upgrade to WebSocket or WebTransport. During an upgrade,
GoSync completes the Engine.IO probe and transfers queued packets in order. A
transport error closes the physical connection and all namespaces using it.

Connection recovery can retain a namespace ID, rooms, and missed events after an
unexpected transport loss. Redis can forward live broadcasts between GoSync
processes. These features are independent: recovery is local, while the Redis
broker handles current cluster traffic.
