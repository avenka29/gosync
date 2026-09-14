package gosync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/socketio"
)

func TestRecoveryRestoresRoomsAndMissedPackets(t *testing.T) {
	s, url := testServer(t, Config{Recovery: &RecoveryConfig{MaxDisconnectionDuration: time.Second}})
	sockets := make(chan *Socket, 2)
	s.Default().OnConnect(func(_ context.Context, socket *Socket) { sockets <- socket })
	c := dialTest(t, url)
	writeTest(t, c, "40")
	opened := readTest(t, c)
	data := opened.Data.(map[string]any)
	first := <-sockets
	if err := first.Join("room"); err != nil {
		t.Fatal(err)
	}
	if err := first.Emit(context.Background(), "initial"); err != nil {
		t.Fatal(err)
	}
	event := readTest(t, c).Data.([]any)
	offset := event[len(event)-1]
	closed := make(chan struct{})
	first.OnDisconnect(func(string) { close(closed) })
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	<-closed
	if err := s.Default().To("room").Emit(context.Background(), "missed", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	again := dialTest(t, url)
	auth, err := json.Marshal(map[string]any{"pid": data["pid"], "offset": offset})
	if err != nil {
		t.Fatal(err)
	}
	writeTest(t, again, "40"+string(auth))
	reconnected := readTest(t, again)
	if reconnected.Data.(map[string]any)["sid"] != data["sid"] {
		t.Fatal("ID not restored")
	}
	replay := readTest(t, again).Data.([]any)
	if replay[0] != "missed" || string(replay[1].([]byte)) != string([]byte{1, 2, 3}) {
		t.Fatal(replay)
	}
	restored := <-sockets
	if !restored.Recovered() {
		t.Fatal("missing recovered flag")
	}
	if len(restored.Rooms()) != 2 {
		t.Fatal(restored.Rooms())
	}
}

func TestRecoveryExpiredAndUnknownOffsetStartFresh(t *testing.T) {
	s, url := testServer(t, Config{Recovery: &RecoveryConfig{MaxDisconnectionDuration: 10 * time.Millisecond, MaxPacketsPerSession: 1, MaxBytes: 200}})
	c := dialTest(t, url)
	writeTest(t, c, "40")
	data := readTest(t, c).Data.(map[string]any)
	socket := s.Default().Sockets()[0]
	if err := socket.Emit(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	event := readTest(t, c).Data.([]any)
	done := make(chan struct{})
	socket.OnDisconnect(func(string) { close(done) })
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	time.Sleep(25 * time.Millisecond)
	c = dialTest(t, url)
	auth, err := json.Marshal(map[string]any{"pid": data["pid"], "offset": event[len(event)-1]})
	if err != nil {
		t.Fatal(err)
	}
	writeTest(t, c, "40"+string(auth))
	reopened := readTest(t, c).Data.(map[string]any)
	if reopened["sid"] == data["sid"] {
		t.Fatal("expired state recovered")
	}
}

func TestRecoveryCapacityAndPacketEviction(t *testing.T) {
	store, err := newRecovery(&RecoveryConfig{
		MaxDisconnectionDuration: time.Minute,
		MaxSessions:              1,
		MaxPacketsPerSession:     1,
		MaxBytes:                 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	namespace := &Namespace{name: "/"}
	first := &Socket{id: "first", namespace: namespace, rooms: make(map[string]struct{})}
	store.attach(first)
	if first.pid == "" {
		t.Fatal("first recovery session was not allocated")
	}
	second := &Socket{id: "second", namespace: namespace, rooms: make(map[string]struct{})}
	store.attach(second)
	if second.pid != "" {
		t.Fatal("session capacity was exceeded")
	}

	codec := socketio.NewCodec(0, 0)
	for _, value := range []string{"first", "second"} {
		if _, err := store.record(first.pid, newEventPacket("/", "event", []any{value}, nil), codec); err != nil {
			t.Fatal(err)
		}
	}
	state := store.sessions[first.pid]
	if state == nil || len(state.packets) != 1 {
		t.Fatalf("retained packets = %#v", state)
	}
	if _, err := store.record(first.pid, newEventPacket("/", "event", []any{string(make([]byte, 256))}, nil), codec); err != nil {
		t.Fatal(err)
	}
	if store.sessions[first.pid] != nil || store.bytes != 0 {
		t.Fatalf("oversized recovery state retained: sessions=%d bytes=%d", len(store.sessions), store.bytes)
	}
}
