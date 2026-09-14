package socketio

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/avenka29/gosync/internal/engineio"
)

func TestPartialBinarySendClosesAndPoisonsStream(t *testing.T) {
	calls, closed := 0, 0
	s, err := NewSession(NewCodec(0, 0), func(context.Context, engineio.Packet) error {
		calls++
		if calls == 2 {
			return io.ErrClosedPipe
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.SetOnFailure(func() { closed++ })
	p := Packet{Type: PacketEvent, Data: []any{"binary", []byte{1}}}
	if err := s.Send(context.Background(), p); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), p); !errors.Is(err, engineio.ErrSessionClosed) {
		t.Fatal(err)
	}
	if closed != 1 || calls != 2 {
		t.Fatalf("closed=%d writes=%d", closed, calls)
	}
}

func TestPacketSizeIncludesAllAttachments(t *testing.T) {
	s, err := NewSession(NewCodec(0, 0), func(context.Context, engineio.Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.SetLimits(128)
	if err := s.Handle(context.Background(), engineio.Packet{Type: engineio.PacketMessage, Data: []byte(strings.Repeat("x", 129))}); !errors.Is(err, engineio.ErrPayloadTooLarge) {
		t.Fatal(err)
	}
	header := []byte(`51-["binary",{"_placeholder":true,"num":0}]`)
	if err := s.Handle(context.Background(), engineio.Packet{Type: engineio.PacketMessage, Data: header}); err != nil {
		t.Fatal(err)
	}
	if err := s.Handle(context.Background(), engineio.Packet{Type: engineio.PacketMessage, Binary: true, Data: make([]byte, 100)}); !errors.Is(err, engineio.ErrPayloadTooLarge) {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), Packet{Type: PacketEvent, Data: []any{"large", make([]byte, 200)}}); !errors.Is(err, engineio.ErrPayloadTooLarge) {
		t.Fatal(err)
	}
}

func TestCancelledSendDoesNotPoisonStream(t *testing.T) {
	s, err := NewSession(NewCodec(0, 0), func(context.Context, engineio.Packet) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := Packet{Type: PacketEvent, Data: []any{"event"}}
	if err := s.Send(ctx, p); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedBinaryReferencesDoNotAmplifyMemory(t *testing.T) {
	c := NewCodec(0, 0)
	p, err := c.DecodeHeader([]byte(`51-["data",{"_placeholder":true,"num":0},{"_placeholder":true,"num":0}]`))
	if err != nil {
		t.Fatal(err)
	}
	original := []byte{1, 2, 3}
	p, err = c.Reconstruct(p, [][]byte{original})
	if err != nil {
		t.Fatal(err)
	}
	values := p.Data.([]any)
	a, b := values[1].([]byte), values[2].([]byte)
	if &a[0] != &b[0] {
		t.Fatal("attachment copied for every placeholder")
	}
	original[0] = 9
	if a[0] != 1 {
		t.Fatal("input retained")
	}
}

func TestPacketTypeNames(t *testing.T) {
	for i, want := range []string{"connect", "disconnect", "event", "ack", "connect_error", "binary_event", "binary_ack", "unknown(7)"} {
		if PacketType(i).String() != want {
			t.Fatal(i)
		}
	}
	if _, err := NewSession(NewCodec(0, 0), nil, nil); err == nil {
		t.Fatal("nil sender accepted")
	}
}
