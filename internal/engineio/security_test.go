package engineio

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionPanicAndUnexpectedControlCloseWorkers(t *testing.T) {
	for _, panicHandler := range []bool{false, true} {
		tr := newFakeTransport()
		session, err := NewSession(SessionConfig{ID: "security", PingInterval: time.Hour}, tr, func(context.Context, Packet) error { panic("handler") })
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- session.Run(context.Background()) }()
		<-tr.writes
		if panicHandler {
			tr.reads <- Frame{Payload: []byte("4event")}
		} else {
			tr.reads <- Frame{Payload: []byte("5")}
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("missing error")
			}
		case <-time.After(time.Second):
			t.Fatal("session stuck")
		}
		select {
		case <-session.Done():
		default:
			t.Fatal("workers not done")
		}
	}
}

func TestPacketNamesAndCancelledSend(t *testing.T) {
	for i, want := range []string{"open", "close", "ping", "pong", "message", "upgrade", "noop", "unknown(7)"} {
		if PacketType(i).String() != want {
			t.Fatal(i)
		}
	}
	session, err := NewSession(SessionConfig{ID: "cancel"}, newFakeTransport(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Send(context.Background(), Packet{Type: PacketPing}); !errors.Is(err, ErrUnexpectedPacket) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.Send(ctx, Packet{Type: PacketMessage}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	timer := newRealTimer(time.Hour)
	timer.Reset(time.Hour)
	timer.Stop()
}
