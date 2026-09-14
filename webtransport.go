package gosync

import (
	"net/http"

	wt "github.com/quic-go/webtransport-go"
)

// WebTransportHandler serves Engine.IO on a configured HTTP/3 server.
func (server *Server) WebTransportHandler(webTransportServer *wt.Server) http.Handler {
	available := webTransportServer != nil && webTransportServer.H3 != nil
	if available {
		wt.ConfigureHTTP3Server(webTransportServer.H3)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available {
			http.Error(w, "WebTransport unavailable", http.StatusServiceUnavailable)
			return
		}
		server.engine.ServeWebTransport(w, r, webTransportServer)
	})
}
