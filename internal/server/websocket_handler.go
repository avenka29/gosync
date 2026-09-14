package server

import (
	"net/http"

	"github.com/avenka29/gosync/internal/transport"
)

func (server *Server) serveWebSocket(writer http.ResponseWriter, request *http.Request) {
	connection, err := server.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		server.report(err)
		return
	}
	connection.SetReadLimit(server.config.MaxPayload)

	webSocketTransport, err := transport.NewWebSocket(connection)
	if err != nil {
		server.report(err)
		_ = connection.Close()
		return
	}
	entry, err := server.createSession(webSocketTransport, nil, nil, request)
	if err != nil {
		server.report(err)
		_ = webSocketTransport.Close()
		return
	}
	server.runSession(entry)
}
