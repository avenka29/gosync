package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	protocol "github.com/avenka29/gosync/internal/socketio"
)

func TestPollingServerHandshakeMessagingAndCleanup(t *testing.T) {
	t.Parallel()

	server := NewServer(Config{
		PingInterval: time.Hour,
		PingTimeout:  time.Hour,
		newSessionID: func() (string, error) { return "polling-session", nil },
	}, func(sender Sender) engineio.MessageHandler {
		return func(ctx context.Context, packet engineio.Packet) error {
			return sender(ctx, packet)
		}
	})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	baseURL := httpServer.URL + "/socket.io/?EIO=4&transport=polling"
	response, err := http.Get(baseURL)
	if err != nil {
		t.Fatalf("initial GET error = %v", err)
	}
	openPayload := readResponse(t, response, http.StatusOK)
	frames, err := engineio.DecodePayload(openPayload, engineio.DefaultMaxPayload)
	if err != nil || len(frames) != 1 {
		t.Fatalf("open payload = %q, frames = %#v, error = %v", openPayload, frames, err)
	}
	packet, err := engineio.DecodeFrame(frames[0])
	if err != nil || packet.Type != engineio.PacketOpen {
		t.Fatalf("open packet = %#v, error = %v", packet, err)
	}
	var handshake struct {
		SID      string   `json:"sid"`
		Upgrades []string `json:"upgrades"`
	}
	if err := json.Unmarshal(packet.Data, &handshake); err != nil {
		t.Fatalf("open JSON error = %v", err)
	}
	if handshake.SID != "polling-session" || len(handshake.Upgrades) != 1 || handshake.Upgrades[0] != "websocket" {
		t.Fatalf("handshake = %#v", handshake)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "text/plain; charset=UTF-8" {
		t.Fatalf("Content-Type = %q", contentType)
	}

	sessionURL := baseURL + "&sid=" + handshake.SID
	response, err = http.Post(sessionURL, "text/plain; charset=UTF-8", strings.NewReader("4hello\x1ebAQID"))
	if err != nil {
		t.Fatalf("POST error = %v", err)
	}
	if got := string(readResponse(t, response, http.StatusOK)); got != "ok" {
		t.Fatalf("POST response = %q, want ok", got)
	}

	response, err = http.Get(sessionURL)
	if err != nil {
		t.Fatalf("message GET error = %v", err)
	}
	messagePayload := readResponse(t, response, http.StatusOK)
	if got, want := string(messagePayload), "4hello\x1ebAQID"; got != want {
		t.Fatalf("message payload = %q, want %q", got, want)
	}

	entry, err := server.find(handshake.SID)
	if err != nil {
		t.Fatalf("find() error = %v", err)
	}
	response, err = http.Post(sessionURL, "text/plain", strings.NewReader("1"))
	if err != nil {
		t.Fatalf("close POST error = %v", err)
	}
	readResponse(t, response, http.StatusOK)
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("polling session was not removed after close packet")
	}
	if server.SessionCount() != 0 {
		t.Fatalf("SessionCount() = %d, want 0", server.SessionCount())
	}

	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(closeContext); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestSocketIOConnectAndEventOverPolling(t *testing.T) {
	t.Parallel()

	server := NewServer(Config{
		PingInterval: time.Hour,
		PingTimeout:  time.Hour,
		newSessionID: func() (string, error) { return "engine-session", nil },
	}, func(sender Sender) engineio.MessageHandler {
		var socketSession *protocol.Session
		socketSession, err := protocol.NewSession(
			protocol.NewCodec(0, 0),
			protocol.PacketSender(sender),
			func(ctx context.Context, packet protocol.Packet) error {
				switch packet.Type {
				case protocol.PacketConnect:
					return socketSession.Send(ctx, protocol.Packet{
						Type:      protocol.PacketConnect,
						Namespace: packet.Namespace,
						Data:      map[string]any{"sid": "socket-session"},
					})
				case protocol.PacketEvent:
					return socketSession.Send(ctx, packet)
				default:
					return nil
				}
			},
		)
		if err != nil {
			return func(context.Context, engineio.Packet) error { return err }
		}
		return socketSession.Handle
	})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	baseURL := httpServer.URL + "/socket.io/?EIO=4&transport=polling"
	response, err := http.Get(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	readResponse(t, response, http.StatusOK)
	sessionURL := baseURL + "&sid=engine-session"

	response, err = http.Post(sessionURL, "text/plain", strings.NewReader("40"))
	if err != nil {
		t.Fatal(err)
	}
	readResponse(t, response, http.StatusOK)
	response, err = http.Get(sessionURL)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(readResponse(t, response, http.StatusOK)), `40{"sid":"socket-session"}`; got != want {
		t.Fatalf("Socket.IO connect = %q, want %q", got, want)
	}

	response, err = http.Post(sessionURL, "text/plain", strings.NewReader(`42["echo","hello"]`))
	if err != nil {
		t.Fatal(err)
	}
	readResponse(t, response, http.StatusOK)
	response, err = http.Get(sessionURL)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(readResponse(t, response, http.StatusOK)), `42["echo","hello"]`; got != want {
		t.Fatalf("Socket.IO event = %q, want %q", got, want)
	}

	response, err = http.Post(sessionURL, "text/plain", strings.NewReader("1"))
	if err != nil {
		t.Fatal(err)
	}
	readResponse(t, response, http.StatusOK)
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(closeContext); err != nil {
		t.Fatal(err)
	}
}

func TestPollingServerRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	server := NewServer(Config{MaxPayload: 8}, nil)
	tests := []struct {
		name   string
		method string
		query  string
		body   string
		origin string
		status int
	}{
		{name: "old revision", method: http.MethodGet, query: "?EIO=3&transport=polling", status: http.StatusBadRequest},
		{name: "repeated revision", method: http.MethodGet, query: "?EIO=4&EIO=4&transport=polling", status: http.StatusBadRequest},
		{name: "missing transport", method: http.MethodGet, query: "?EIO=4", status: http.StatusBadRequest},
		{name: "repeated transport", method: http.MethodGet, query: "?EIO=4&transport=polling&transport=polling", status: http.StatusBadRequest},
		{name: "unknown transport", method: http.MethodGet, query: "?EIO=4&transport=other", status: http.StatusBadRequest},
		{name: "post without session", method: http.MethodPost, query: "?EIO=4&transport=polling", status: http.StatusBadRequest},
		{name: "unknown session", method: http.MethodGet, query: "?EIO=4&transport=polling&sid=missing", status: http.StatusBadRequest},
		{name: "empty session", method: http.MethodGet, query: "?EIO=4&transport=polling&sid=", status: http.StatusBadRequest},
		{name: "repeated session", method: http.MethodGet, query: "?EIO=4&transport=polling&sid=one&sid=two", status: http.StatusBadRequest},
		{name: "cross origin", method: http.MethodGet, query: "?EIO=4&transport=polling", origin: "https://example.com", status: http.StatusForbidden},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(test.method, "http://gosync.test/socket.io/"+test.query, strings.NewReader(test.body))
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("ServeHTTP() status = %d, want %d", response.Code, test.status)
			}
			if contentType := response.Header().Get("Content-Type"); contentType != "application/json; charset=utf-8" {
				t.Fatalf("Content-Type = %q", contentType)
			}
			var engineError struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &engineError); err != nil || engineError.Message == "" {
				t.Fatalf("error response = %q, error = %v", response.Body.Bytes(), err)
			}
		})
	}
}

func TestServerRejectsNewRequestsAfterClose(t *testing.T) {
	t.Parallel()

	server := NewServer(Config{}, nil)
	closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(closeContext); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/socket.io/?EIO=4&transport=polling", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ServeHTTP() status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestPollingServerRejectsOversizedPostAndRemovesSession(t *testing.T) {
	t.Parallel()

	var nextID atomic.Int64
	server := NewServer(Config{
		MaxPayload:   128,
		PingInterval: time.Hour,
		PingTimeout:  time.Hour,
		newSessionID: func() (string, error) {
			return fmt.Sprintf("session-%d", nextID.Add(1)), nil
		},
	}, nil)
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	baseURL := httpServer.URL + "/socket.io/?EIO=4&transport=polling"
	response, err := http.Get(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	openPayload := readResponse(t, response, http.StatusOK)
	frames, err := engineio.DecodePayload(openPayload, 1024)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := engineio.DecodeFrame(frames[0])
	if err != nil {
		t.Fatal(err)
	}
	var opened struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(packet.Data, &opened); err != nil {
		t.Fatal(err)
	}
	entry, err := server.find(opened.SID)
	if err != nil {
		t.Fatal(err)
	}

	response, err = http.Post(baseURL+"&sid="+opened.SID, "text/plain", bytes.NewReader(bytes.Repeat([]byte("x"), 129)))
	if err != nil {
		t.Fatal(err)
	}
	readResponse(t, response, http.StatusRequestEntityTooLarge)
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("oversized POST did not close its session")
	}
}

func TestPollingPostValidationClosesBodyAndSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "empty body", contentType: "text/plain", wantStatus: http.StatusBadRequest},
		{name: "malformed packet", contentType: "text/plain", body: "abc", wantStatus: http.StatusBadRequest},
		{name: "invalid base64", contentType: "text/plain", body: "b!", wantStatus: http.StatusBadRequest},
		{name: "binary content type CVE-2026-59725", contentType: "application/octet-stream", body: "abc", wantStatus: http.StatusBadRequest},
		{name: "unsupported content type", contentType: "application/json", body: `"4hello"`, wantStatus: http.StatusBadRequest},
		{name: "malformed content type", contentType: "text/plain; charset", body: "4hello", wantStatus: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server, sessionURL, entry := newPollingTestSession(t, 1024)
			body := &trackingReadCloser{Reader: strings.NewReader(test.body)}
			request := httptest.NewRequest(http.MethodPost, sessionURL, body)
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()

			server.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("ServeHTTP() status = %d, want %d; body = %q", response.Code, test.wantStatus, response.Body.Bytes())
			}
			if !body.closed.Load() {
				t.Fatal("request body was not closed")
			}
			select {
			case <-entry.done:
			case <-time.After(time.Second):
				t.Fatal("invalid POST did not close its session")
			}
		})
	}
}

func TestPollingPostPayloadBoundary(t *testing.T) {
	t.Parallel()

	const maxPayload = int64(256)
	server, sessionURL, _ := newPollingTestSession(t, maxPayload)
	valid := "4" + strings.Repeat("x", int(maxPayload)-1)
	request := httptest.NewRequest(http.MethodPost, sessionURL, strings.NewReader(valid))
	request.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "ok" {
		t.Fatalf("exact-limit POST = %d %q", response.Code, response.Body.String())
	}

	server, sessionURL, entry := newPollingTestSession(t, maxPayload)
	request = httptest.NewRequest(http.MethodPost, sessionURL, strings.NewReader(valid+"x"))
	request.ContentLength = -1 // exercise the streaming one-byte overflow check
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over-limit POST status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("over-limit POST did not close its session")
	}
}

func TestPollingPostRejectsDeclaredOversizeWithoutReading(t *testing.T) {
	t.Parallel()

	server, sessionURL, entry := newPollingTestSession(t, 256)
	body := &trackingReadCloser{Reader: &failReader{err: errors.New("body must not be read")}}
	request := httptest.NewRequest(http.MethodPost, sessionURL, body)
	request.Header.Set("Content-Type", "text/plain")
	request.ContentLength = 257
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("ServeHTTP() status = %d", response.Code)
	}
	if !body.closed.Load() {
		t.Fatal("request body was not closed")
	}
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("declared oversize POST did not close its session")
	}
}

func TestPollingPostReadFailureCompletesResponse(t *testing.T) {
	t.Parallel()

	server, sessionURL, entry := newPollingTestSession(t, 256)
	body := &trackingReadCloser{Reader: &failReader{err: errors.New("read failed")}}
	request := httptest.NewRequest(http.MethodPost, sessionURL, body)
	request.Header.Set("Content-Type", "text/plain")
	request.ContentLength = -1
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || response.Body.Len() == 0 {
		t.Fatalf("ServeHTTP() = %d %q", response.Code, response.Body.Bytes())
	}
	if !body.closed.Load() {
		t.Fatal("request body was not closed")
	}
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("read failure did not close its session")
	}
}

func TestPollingDuplicateGETClosesSessionAndBothResponsesComplete(t *testing.T) {
	t.Parallel()

	server, sessionURL, entry := newPollingTestSession(t, 1024)
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	pollContext := newPollingSelectContext()
	go func() {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, sessionURL, nil).WithContext(pollContext)
		server.ServeHTTP(response, request)
		firstDone <- response
	}()
	<-pollContext.entered
	second := httptest.NewRecorder()
	server.ServeHTTP(second, httptest.NewRequest(http.MethodGet, sessionURL, nil))
	if second.Code != http.StatusBadRequest {
		t.Fatalf("duplicate GET status = %d", second.Code)
	}
	select {
	case first := <-firstDone:
		if first.Code != http.StatusOK || first.Body.String() != "1" {
			t.Fatalf("first GET = %d %q", first.Code, first.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("first GET response did not complete")
	}
	select {
	case <-entry.done:
	case <-time.After(time.Second):
		t.Fatal("duplicate GET did not remove session")
	}
}

func TestSameOriginRequiresMatchingSchemeAndHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		origin string
		secure bool
		want   bool
	}{
		{name: "no origin", want: true},
		{name: "http match", origin: "http://gosync.test", want: true},
		{name: "case insensitive match", origin: "HTTP://GOSYNC.TEST", want: true},
		{name: "default HTTP port", origin: "http://gosync.test:80", want: true},
		{name: "HTTPS match", origin: "https://gosync.test", secure: true, want: true},
		{name: "default HTTPS port", origin: "https://gosync.test:443", secure: true, want: true},
		{name: "secure scheme mismatch", origin: "http://gosync.test", secure: true},
		{name: "scheme mismatch", origin: "https://gosync.test"},
		{name: "port mismatch", origin: "http://gosync.test:8080"},
		{name: "malformed", origin: "://"},
		{name: "null", origin: "null"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := "http"
			if test.secure {
				scheme = "https"
			}
			request := httptest.NewRequest(http.MethodGet, scheme+"://gosync.test/socket.io/", nil)
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if got := sameOrigin(request); got != test.want {
				t.Fatalf("sameOrigin() = %t, want %t", got, test.want)
			}
		})
	}
	request := httptest.NewRequest(http.MethodGet, "http://gosync.test/socket.io/", nil)
	request.Header.Add("Origin", "http://gosync.test")
	request.Header.Add("Origin", "http://attacker.test")
	if sameOrigin(request) {
		t.Fatal("accepted multiple origins containing a cross-origin value")
	}
}

type trackingReadCloser struct {
	io.Reader
	closed atomic.Bool
}

func (reader *trackingReadCloser) Close() error {
	reader.closed.Store(true)
	return nil
}

type failReader struct{ err error }

func (reader *failReader) Read([]byte) (int, error) { return 0, reader.err }

func newPollingTestSession(t *testing.T, maxPayload int64) (*Server, string, *managedSession) {
	t.Helper()
	server := NewServer(Config{
		MaxPayload:   maxPayload,
		PingInterval: time.Hour,
		PingTimeout:  time.Hour,
	}, nil)
	request := httptest.NewRequest(http.MethodGet, "http://gosync.test/socket.io/?EIO=4&transport=polling", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("handshake status = %d; body = %q", response.Code, response.Body.Bytes())
	}
	frames, err := engineio.DecodePayload(response.Body.Bytes(), maxPayload)
	if err != nil {
		t.Fatalf("decode handshake: %v", err)
	}
	packet, err := engineio.DecodeFrame(frames[0])
	if err != nil {
		t.Fatalf("decode open packet: %v", err)
	}
	var handshake struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(packet.Data, &handshake); err != nil {
		t.Fatalf("decode handshake JSON: %v", err)
	}
	entry, err := server.find(handshake.SID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Close(ctx)
	})
	return server, "http://gosync.test/socket.io/?EIO=4&transport=polling&sid=" + handshake.SID, entry
}

type pollingSelectContext struct {
	done    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newPollingSelectContext() *pollingSelectContext {
	return &pollingSelectContext{done: make(chan struct{}), entered: make(chan struct{})}
}

func (ctx *pollingSelectContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (ctx *pollingSelectContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.done
}

func (ctx *pollingSelectContext) Err() error { return nil }

func (ctx *pollingSelectContext) Value(any) any { return nil }

func readResponse(t *testing.T, response *http.Response, wantStatus int) []byte {
	t.Helper()
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response error = %v", err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("response status = %d, want %d; body = %q", response.StatusCode, wantStatus, payload)
	}
	return payload
}
