package server

import (
	"context"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"

	"github.com/avenka29/gosync/internal/transport"
)

func (server *Server) servePolling(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	if !query.Has("sid") {
		if request.Method != http.MethodGet {
			writeEngineError(writer, http.StatusBadRequest, engineErrorBadHandshakeMethod)
			return
		}
		server.openPolling(writer, request)
		return
	}
	sessionID := query.Get("sid")

	entry, err := server.find(sessionID)
	if err != nil && request.Method == http.MethodGet && server.consumeClosedPoll(sessionID) {
		writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		_, _ = writer.Write([]byte("6"))
		return
	}
	if err != nil || entry.polling == nil {
		writeEngineError(writer, http.StatusBadRequest, engineErrorUnknownSession)
		return
	}

	switch request.Method {
	case http.MethodGet:
		server.poll(writer, request, entry)
	case http.MethodPost:
		server.post(writer, request, entry)
	default:
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
	}
}

func (server *Server) openPolling(writer http.ResponseWriter, request *http.Request) {
	pollingTransport, err := transport.NewPolling(
		server.config.MaxPayload,
		server.config.InboundBuffer,
		server.config.OutboundBuffer,
	)
	if err != nil {
		writeEngineError(writer, http.StatusInternalServerError, engineErrorBadRequest)
		server.report(err)
		return
	}
	upgrades := []string{"websocket"}
	if server.config.EnableWebTransport {
		upgrades = append(upgrades, "webtransport")
	}
	entry, err := server.createSession(transport.NewSwitching(pollingTransport), pollingTransport, upgrades, request)
	if err != nil {
		_ = pollingTransport.Close()
		status := http.StatusInternalServerError
		if errors.Is(err, ErrServerClosed) {
			status = http.StatusServiceUnavailable
		}
		writeEngineError(writer, status, engineErrorBadRequest)
		return
	}

	go server.runSession(entry)
	server.poll(writer, request, entry)
}

func (server *Server) poll(writer http.ResponseWriter, request *http.Request, entry *managedSession) {
	payload, err := entry.polling.Poll(request.Context())
	if err != nil {
		if entry.polling.ClientClosed() {
			select {
			case <-entry.done:
				server.consumeClosedPoll(entry.id)
			case <-request.Context().Done():
				return
			}
			writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			_, _ = writer.Write([]byte("6"))
			return
		}
		if request.Context().Err() != nil {
			return
		}
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
	}

	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	// #nosec G705 -- Engine.IO requires the validated payload verbatim as text/plain.
	if _, err := writer.Write(payload); err != nil {
		_ = entry.transport.Close()
		server.report(err)
	}
}

func (server *Server) post(writer http.ResponseWriter, request *http.Request, entry *managedSession) {
	if !entry.polling.BeginPost() {
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
	}
	defer entry.polling.EndPost()
	if !validPollingContentType(request.Header.Get("Content-Type")) {
		_ = entry.transport.Close()
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
	}
	if request.ContentLength > server.config.MaxPayload {
		_ = entry.transport.Close()
		writeEngineError(writer, http.StatusRequestEntityTooLarge, engineErrorBadRequest)
		return
	}

	payload, tooLarge, err := readBounded(request.Body, server.config.MaxPayload)
	if err != nil {
		_ = entry.transport.Close()
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
	}
	if tooLarge {
		_ = entry.transport.Close()
		writeEngineError(writer, http.StatusRequestEntityTooLarge, engineErrorBadRequest)
		return
	}
	if err := entry.polling.SubmitReserved(request.Context(), payload); err != nil {
		if errors.Is(err, context.Canceled) || request.Context().Err() != nil {
			return
		}
		writeEngineError(writer, http.StatusBadRequest, engineErrorBadRequest)
		return
	}

	writer.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, "ok")
}

func validPollingContentType(value string) bool {
	if strings.TrimSpace(value) == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "text/plain")
}

func readBounded(reader io.Reader, limit int64) ([]byte, bool, error) {
	readLimit := limit
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	limited := &io.LimitedReader{R: reader, N: readLimit}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}
	if int64(len(payload)) > limit {
		return nil, true, nil
	}
	return payload, false, nil
}
