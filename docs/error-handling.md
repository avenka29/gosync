# Error handling

Errors caused by invalid configuration or an invalid method call are returned to
the caller. Protocol errors close or reject the affected connection. Transport
errors end the affected session. Failures in background session work are sent to
the server error callback when one is configured.

The server should not panic because a client sent an invalid packet or closed a
connection. Error reporting must not block packet processing. Logs must not
include credentials, cookies, authorization headers, or complete event payloads.

Operations that can wait should accept a context and return an error. Shutdown
must stop new connections, cancel active sessions, and wait for them until the
provided context expires.

An event acknowledgement may be called once. Later calls return
`ErrAlreadyAcknowledged`. Pending acknowledgements are bounded and removed on
response, timeout, or disconnect. A partial binary write poisons and closes the
connection because continuing could cause the peer to attach later frames to the
wrong packet.

Middleware failures send a generic connect error so private authorization
details do not cross the network. Callback panics are recovered, reported to
`OnError`, and close the affected connection. `OnError` itself is isolated from
panics. Applications should keep it non-blocking.
