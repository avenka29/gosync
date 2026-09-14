package gosync

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
	"github.com/avenka29/gosync/internal/transport"
	"github.com/quic-go/quic-go/http3"
	wt "github.com/quic-go/webtransport-go"
)

func TestWebTransportDirectAndPollingUpgrade(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "upgrade"}[upgrade], func(t *testing.T) {
			server, err := NewServer(Config{EnableWebTransport: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := server.Default().On("echo", func(ctx context.Context, socket *Socket, args []any, ack Ack) error {
				return ack(ctx, args...)
			}); err != nil {
				t.Fatal(err)
			}
			certServer := httptest.NewTLSServer(http.NotFoundHandler())
			cert := certServer.TLS.Certificates[0]
			pool := x509.NewCertPool()
			pool.AddCert(certServer.Certificate())
			certServer.Close()
			h3 := &http3.Server{TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, NextProtos: []string{http3.NextProtoH3}}}
			webTransportServer := &wt.Server{H3: h3}
			h3.Handler = server.WebTransportHandler(webTransportServer)
			packetConnection, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- webTransportServer.Serve(packetConnection) }()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := server.Close(ctx); err != nil {
					t.Error(err)
				}
				_ = webTransportServer.Close()
				_ = packetConnection.Close()
				<-served
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dialer := wt.Dialer{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}}
			defer dialer.Close()
			_, session, err := dialer.Dial(ctx, "https://"+packetConnection.LocalAddr().String()+"/socket.io/", nil)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := session.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			peer := transport.NewWebTransport(stream, 1_000_000, func() error { return session.CloseWithError(0, "") })
			defer peer.Close()
			initial := "0"
			if upgrade {
				httpServer := httptest.NewServer(server)
				defer httpServer.Close()
				response, err := http.Get(httpServer.URL + "/socket.io/?EIO=4&transport=polling")
				if err != nil {
					t.Fatal(err)
				}
				payload, err := io.ReadAll(response.Body)
				if closeErr := response.Body.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != http.StatusOK || len(payload) < 2 {
					t.Fatalf("polling handshake status %d: %q", response.StatusCode, payload)
				}
				var opened struct {
					SID string `json:"sid"`
				}
				if err := json.Unmarshal(payload[1:], &opened); err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(map[string]string{"sid": opened.SID})
				if err != nil {
					t.Fatal(err)
				}
				initial += string(data)
			}
			if err := peer.Write(ctx, engineio.Frame{Payload: []byte(initial)}); err != nil {
				t.Fatal(err)
			}
			if upgrade {
				if err := peer.Write(ctx, engineio.Frame{Payload: []byte("2probe")}); err != nil {
					t.Fatal(err)
				}
				frame, err := peer.Read(ctx)
				if err != nil || string(frame.Payload) != "3probe" {
					t.Fatalf("probe %q %v", frame.Payload, err)
				}
				if err := peer.Write(ctx, engineio.Frame{Payload: []byte("5")}); err != nil {
					t.Fatal(err)
				}
			} else {
				frame, err := peer.Read(ctx)
				if err != nil || !strings.HasPrefix(string(frame.Payload), "0{") {
					t.Fatalf("handshake %q %v", frame.Payload, err)
				}
			}
			if err := peer.Write(ctx, engineio.Frame{Payload: []byte("40")}); err != nil {
				t.Fatal(err)
			}
			var frame engineio.Frame
			for {
				frame, err = peer.Read(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if string(frame.Payload) != "6" {
					break
				}
			}
			if !strings.HasPrefix(string(frame.Payload), "40{") {
				t.Fatalf("connect %q", frame.Payload)
			}
			if err := peer.Write(ctx, engineio.Frame{Payload: []byte(`421["echo","webtransport"]`)}); err != nil {
				t.Fatal(err)
			}
			frame, err = peer.Read(ctx)
			if err != nil || string(frame.Payload) != `431["webtransport"]` {
				t.Fatalf("ack %q %v", frame.Payload, err)
			}
		})
	}
}

func TestWebTransportHandlerRejectsMissingHTTP3Server(t *testing.T) {
	server, err := NewServer(Config{EnableWebTransport: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, webTransportServer := range []*wt.Server{nil, {}} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodConnect, "https://example.com/socket.io/", nil)
		server.WebTransportHandler(webTransportServer).ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d", response.Code)
		}
	}
}
