# WebTransport

WebTransport is optional. It carries Engine.IO over HTTP/3 and therefore needs
a UDP listener and a TLS certificate. Polling and WebSocket continue to use the
normal HTTP handler.

Enable it in the GoSync configuration:

```go
server, err := gosync.NewServer(gosync.Config{
	EnableWebTransport: true,
})
```

Create an HTTP/3 and WebTransport server using the same GoSync instance:

```go
tlsConfig := &tls.Config{
	MinVersion: tls.VersionTLS13,
}
h3Server := &http3.Server{
	Addr:      ":443",
	TLSConfig: tlsConfig,
}
webTransportServer := &webtransport.Server{
	H3: h3Server,
}
h3Server.Handler = server.WebTransportHandler(webTransportServer)

if err := webTransportServer.ListenAndServeTLS("cert.pem", "key.pem"); err != nil {
	log.Printf("WebTransport: %v", err)
}
```

The imports used by this example are:

```go
import (
	"crypto/tls"
	"log"

	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
)
```

The HTTP/3 handler uses the same `/socket.io/` path. Requests pass both the
GoSync origin check and the `webtransport.Server` origin check. When allowing a
cross-origin application, assign the same allowlist callback to
`gosync.Config.CheckOrigin` and `webtransport.Server.CheckOrigin`. A nil or
incomplete WebTransport server returns HTTP 503.

Close both components during shutdown:

```go
if err := server.Close(shutdownContext); err != nil {
	log.Printf("Socket.IO shutdown: %v", err)
}
if err := webTransportServer.Close(); err != nil {
	log.Printf("WebTransport shutdown: %v", err)
}
```

Socket.IO clients that support WebTransport can choose it directly or allow it
as an upgrade transport. Browser support, network policy, UDP reachability, and
certificate trust still determine whether the transport can connect.
