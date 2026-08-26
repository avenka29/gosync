package engineio

import (
	"bytes"
	"errors"
	"testing"
)

func TestTextFrameRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		packet Packet
		wire   string
	}{
		{name: "open", packet: Packet{Type: PacketOpen, Data: []byte(`{"sid":"abc"}`)}, wire: `0{"sid":"abc"}`},
		{name: "close", packet: Packet{Type: PacketClose}, wire: "1"},
		{name: "ping", packet: Packet{Type: PacketPing}, wire: "2"},
		{name: "pong payload", packet: Packet{Type: PacketPong, Data: []byte("probe")}, wire: "3probe"},
		{name: "message", packet: Packet{Type: PacketMessage, Data: []byte("hello")}, wire: "4hello"},
		{name: "upgrade", packet: Packet{Type: PacketUpgrade}, wire: "5"},
		{name: "noop", packet: Packet{Type: PacketNoop}, wire: "6"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			frame, err := EncodeFrame(test.packet)
			if err != nil {
				t.Fatalf("EncodeFrame() error = %v", err)
			}
			if frame.Binary {
				t.Fatal("EncodeFrame() unexpectedly returned a binary frame")
			}
			if got := string(frame.Payload); got != test.wire {
				t.Fatalf("EncodeFrame() = %q, want %q", got, test.wire)
			}

			decoded, err := DecodeFrame(frame)
			if err != nil {
				t.Fatalf("DecodeFrame() error = %v", err)
			}
			if decoded.Type != test.packet.Type || decoded.Binary != test.packet.Binary || !bytes.Equal(decoded.Data, test.packet.Data) {
				t.Fatalf("DecodeFrame() = %#v, want %#v", decoded, test.packet)
			}
		})
	}
}

func TestBinaryFrameRoundTripAndOwnership(t *testing.T) {
	t.Parallel()

	original := []byte{0x01, 0x02, 0x03}
	frame, err := EncodeFrame(Packet{Type: PacketMessage, Data: original, Binary: true})
	if err != nil {
		t.Fatalf("EncodeFrame() error = %v", err)
	}
	original[0] = 0xff
	if frame.Payload[0] != 0x01 {
		t.Fatal("EncodeFrame() retained mutable caller storage")
	}

	decoded, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame() error = %v", err)
	}
	frame.Payload[1] = 0xff
	if decoded.Data[1] != 0x02 {
		t.Fatal("DecodeFrame() retained mutable frame storage")
	}
}

func TestFrameValidation(t *testing.T) {
	t.Parallel()

	if _, err := DecodeFrame(Frame{}); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("DecodeFrame(empty) error = %v, want ErrEmptyFrame", err)
	}
	if _, err := DecodeFrame(Frame{Payload: []byte("9bad")}); !errors.Is(err, ErrInvalidPacketType) {
		t.Fatalf("DecodeFrame(invalid) error = %v, want ErrInvalidPacketType", err)
	}
	if _, err := EncodeFrame(Packet{Type: PacketPing, Binary: true}); !errors.Is(err, ErrInvalidBinaryPacket) {
		t.Fatalf("EncodeFrame(binary ping) error = %v, want ErrInvalidBinaryPacket", err)
	}
	if _, err := EncodeFrame(Packet{Type: PacketType(99)}); !errors.Is(err, ErrInvalidPacketType) {
		t.Fatalf("EncodeFrame(invalid) error = %v, want ErrInvalidPacketType", err)
	}
}

func FuzzDecodeFrame(f *testing.F) {
	f.Add(false, []byte("4hello"))
	f.Add(false, []byte("0{}"))
	f.Add(true, []byte{0x01, 0x02})
	f.Add(false, []byte{})

	f.Fuzz(func(t *testing.T, binary bool, payload []byte) {
		packet, err := DecodeFrame(Frame{Binary: binary, Payload: payload})
		if err != nil {
			return
		}

		frame, err := EncodeFrame(packet)
		if err != nil {
			t.Fatalf("successfully decoded packet could not be encoded: %v", err)
		}
		if frame.Binary != binary || !bytes.Equal(frame.Payload, payload) {
			t.Fatalf("round trip = %#v, want binary=%v payload=%x", frame, binary, payload)
		}
	})
}
