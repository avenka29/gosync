package gosync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/socketio"
	"github.com/gorilla/websocket"
)

func testServer(t *testing.T, config Config) (*Server, string) {
	t.Helper()
	server, err := NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Error(err)
		}
		httpServer.Close()
	})
	return server, "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/socket.io/?EIO=4&transport=websocket"
}

func dialTest(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	connection, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, payload, err := connection.ReadMessage()
	if err != nil || len(payload) == 0 || payload[0] != '0' {
		t.Fatalf("handshake %q %v", payload, err)
	}
	return connection
}

func writeTest(t *testing.T, connection *websocket.Conn, text string) {
	t.Helper()
	if err := connection.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
		t.Fatal(err)
	}
}

func readTest(t *testing.T, connection *websocket.Conn) socketio.Packet {
	t.Helper()
	_, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) == 0 || payload[0] != '4' {
		t.Fatalf("message %q", payload)
	}
	codec := socketio.NewCodec(0, 0)
	packet, err := codec.DecodeHeader(payload[1:])
	if err != nil {
		t.Fatalf("decode %q: %v", payload, err)
	}
	if packet.Type.Binary() {
		var buffers [][]byte
		for range packet.Attachments {
			kind, attachment, err := connection.ReadMessage()
			if err != nil || kind != websocket.BinaryMessage {
				t.Fatalf("attachment %d %v", kind, err)
			}
			buffers = append(buffers, attachment)
		}
		packet, err = codec.Reconstruct(packet, buffers)
		if err != nil {
			t.Fatal(err)
		}
	}
	return packet
}

func connectTest(t *testing.T, connection *websocket.Conn, namespace string) {
	t.Helper()
	writeTest(t, connection, "40"+namespace+",")
	packet := readTest(t, connection)
	if packet.Type != socketio.PacketConnect {
		t.Fatalf("connect %#v", packet)
	}
}

func TestNamespaceAuthenticationLifecycle(t *testing.T) {
	s, url := testServer(t, Config{})
	n, err := s.Of("/private")
	if err != nil {
		t.Fatal(err)
	}
	n.Use(func(_ context.Context, socket *Socket) error {
		auth, _ := socket.Auth().(map[string]any)
		if auth["token"] != "allowed" {
			return errors.New("secret internal reason")
		}
		return nil
	})
	c := dialTest(t, url)
	writeTest(t, c, `40/private,{"token":"wrong"}`)
	p := readTest(t, c)
	if p.Type != socketio.PacketConnectError || strings.Contains(p.Data.(map[string]any)["message"].(string), "secret") {
		t.Fatal(p)
	}
	writeTest(t, c, `40/private,{"token":"allowed"}`)
	private := readTest(t, c)
	if private.Type != socketio.PacketConnect {
		t.Fatal(private)
	}
	connectTest(t, c, "/")
	if len(n.Sockets()) != 1 || len(s.Default().Sockets()) != 1 {
		t.Fatal("missing membership")
	}
	if n.Sockets()[0].ID() == s.Default().Sockets()[0].ID() {
		t.Fatal("namespace IDs reused")
	}
	if err := s.Default().On("echo", func(ctx context.Context, socket *Socket, args []any, ack Ack) error {
		return socket.Emit(ctx, "echo", args...)
	}); err != nil {
		t.Fatal(err)
	}
	writeTest(t, c, "41/private,")
	writeTest(t, c, `42["echo","alive"]`)
	if readTest(t, c).Data.([]any)[1] != "alive" {
		t.Fatal("other namespace disconnected")
	}
	if len(n.Sockets()) != 0 {
		t.Fatal("namespace retained")
	}
}

func TestEventBeforeConnectAndReservedEventsClose(t *testing.T) {
	for _, packet := range []string{`42["hello"]`, `42["disconnect"]`, `40/unknown,`} {
		t.Run(packet, func(t *testing.T) {
			_, url := testServer(t, Config{})
			c := dialTest(t, url)
			if packet == `42["disconnect"]` {
				connectTest(t, c, "/")
			}
			writeTest(t, c, packet)
			if packet == `40/unknown,` {
				if readTest(t, c).Type != socketio.PacketConnectError {
					t.Fatal("not rejected")
				}
				return
			}
			if _, _, err := c.ReadMessage(); err == nil {
				t.Fatal("protocol violation kept open")
			}
		})
	}
}

func TestEventsBinaryAndAcks(t *testing.T) {
	s, url := testServer(t, Config{})
	duplicate := make(chan error, 1)
	if err := s.Default().On("echo", func(ctx context.Context, socket *Socket, args []any, ack Ack) error {
		if ack != nil {
			if err := ack(ctx, args...); err != nil {
				return err
			}
			duplicate <- ack(ctx)
			return nil
		}
		return socket.Emit(ctx, "echo", args...)
	}); err != nil {
		t.Fatal(err)
	}
	c := dialTest(t, url)
	connectTest(t, c, "/")
	writeTest(t, c, `4212["echo",1,"text"]`)
	p := readTest(t, c)
	if p.Type != socketio.PacketAck || *p.ID != 12 || len(p.Data.([]any)) != 2 {
		t.Fatal(p)
	}
	if err := <-duplicate; !errors.Is(err, ErrAlreadyAcknowledged) {
		t.Fatal(err)
	}
	writeTest(t, c, `451-["echo",{"_placeholder":true,"num":0}]`)
	if err := c.WriteMessage(websocket.BinaryMessage, []byte{0, 1, 255}); err != nil {
		t.Fatal(err)
	}
	p = readTest(t, c)
	if string(p.Data.([]any)[1].([]byte)) != string([]byte{0, 1, 255}) {
		t.Fatal(p)
	}
}

func TestSynchronousAckInsideConnectHandler(t *testing.T) {
	s, url := testServer(t, Config{AckTimeout: time.Second})
	done := make(chan error, 1)
	s.Default().OnConnect(func(ctx context.Context, socket *Socket) {
		args, err := socket.EmitWithAck(ctx, "question", "answer?")
		if err == nil && args[0] != "yes" {
			err = errors.New("wrong ack")
		}
		done <- err
	})
	c := dialTest(t, url)
	connectTest(t, c, "/")
	p := readTest(t, c)
	encoded, err := socketio.NewCodec(0, 0).Encode(socketio.Packet{Type: socketio.PacketAck, ID: p.ID, Data: []any{"yes"}})
	if err != nil {
		t.Fatal(err)
	}
	writeTest(t, c, "4"+string(encoded.Header))
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRoomsBroadcastAndAckCleanup(t *testing.T) {
	s, url := testServer(t, Config{AckTimeout: 20 * time.Millisecond})
	sockets := make(chan *Socket, 3)
	s.Default().OnConnect(func(_ context.Context, socket *Socket) { sockets <- socket })
	clients := make([]*websocket.Conn, 3)
	members := make([]*Socket, 3)
	for i := range clients {
		clients[i] = dialTest(t, url)
		connectTest(t, clients[i], "/")
		members[i] = <-sockets
	}
	if err := members[0].Join("a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := members[1].Join("b"); err != nil {
		t.Fatal(err)
	}
	if err := members[2].Join("excluded"); err != nil {
		t.Fatal(err)
	}
	if err := s.Default().To("a", "b").Except("excluded").Emit(context.Background(), "news", 7); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if readTest(t, clients[i]).Data.([]any)[0] != "news" {
			t.Fatal("missing broadcast")
		}
	}
	if err := members[0].To("b").Emit(context.Background(), "next"); err != nil {
		t.Fatal(err)
	}
	if readTest(t, clients[1]).Data.([]any)[0] != "next" {
		t.Fatal("duplicate broadcast")
	}
	_, err := members[0].EmitWithAck(context.Background(), "timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	members[0].mu.Lock()
	pending := len(members[0].pending)
	members[0].mu.Unlock()
	if pending != 0 {
		t.Fatal("leaked ack")
	}
	if err := members[0].Disconnect(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if len(members[0].Rooms()) != 0 {
		t.Fatal("rooms retained")
	}
	if err := members[0].Join("x"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatal(err)
	}
}

func TestCORSConfigAndLimits(t *testing.T) {
	if _, err := NewServer(Config{MaxPayload: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	s, url := testServer(t, Config{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://allowed.test" }, AllowCredentials: true, MaxRoomsPerSocket: 2})
	req := httptest.NewRequest("OPTIONS", "http://server/socket.io/?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "https://allowed.test")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != 204 || w.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatal(w)
	}
	req.Header.Set("Origin", "https://forbidden.test")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal(w)
	}
	c, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": []string{"https://allowed.test"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	connectTest(t, c, "/")
	socket := s.Default().Sockets()[0]
	if err := socket.Join("one", "two"); err == nil {
		t.Fatal("room cap bypassed")
	}
	if len(socket.Rooms()) != 1 {
		t.Fatal("partial join")
	}
	if _, err := s.Of("/bad,namespace"); err == nil {
		t.Fatal("invalid namespace")
	}
}

func TestConcurrentEmitsDoNotInterleaveBinary(t *testing.T) {
	s, url := testServer(t, Config{})
	c := dialTest(t, url)
	connectTest(t, c, "/")
	socket := s.Default().Sockets()[0]
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs <- socket.Emit(context.Background(), "binary", i, []byte{byte(i)}) }(i)
	}
	seen := map[string]bool{}
	for range 20 {
		p := readTest(t, c)
		args := p.Data.([]any)
		key := args[1].(json.Number).String()
		if seen[key] {
			t.Fatal("duplicate")
		}
		seen[key] = true
	}
	wg.Wait()
	for range 20 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
