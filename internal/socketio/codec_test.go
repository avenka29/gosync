package socketio

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestEncodeProtocolExamples(t *testing.T) {
	t.Parallel()

	codec := NewCodec(0, 0)
	tests := []struct {
		name        string
		packet      Packet
		header      string
		attachments [][]byte
	}{
		{
			name:   "connect default namespace",
			packet: Packet{Type: PacketConnect},
			header: "0",
		},
		{
			name: "connect custom namespace with auth",
			packet: Packet{
				Type:      PacketConnect,
				Namespace: "/admin",
				Data:      map[string]any{"token": "123"},
			},
			header: `0/admin,{"token":"123"}`,
		},
		{
			name:   "event default namespace",
			packet: Packet{Type: PacketEvent, Data: []any{"foo"}},
			header: `2["foo"]`,
		},
		{
			name: "event custom namespace",
			packet: Packet{
				Type:      PacketEvent,
				Namespace: "/admin",
				Data:      []any{"bar"},
			},
			header: `2/admin,["bar"]`,
		},
		{
			name:   "event requesting acknowledgement",
			packet: Packet{Type: PacketEvent, ID: uint64Pointer(12), Data: []any{"foo"}},
			header: `212["foo"]`,
		},
		{
			name: "custom namespace acknowledgement",
			packet: Packet{
				Type:      PacketAck,
				Namespace: "/admin",
				ID:        uint64Pointer(13),
				Data:      []any{"bar"},
			},
			header: `3/admin,13["bar"]`,
		},
		{
			name: "binary event",
			packet: Packet{
				Type: PacketEvent,
				Data: []any{"baz", []byte{0x01, 0x02}, []byte{0x03, 0x04}},
			},
			header:      `52-["baz",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1}]`,
			attachments: [][]byte{{0x01, 0x02}, {0x03, 0x04}},
		},
		{
			name: "binary acknowledgement",
			packet: Packet{
				Type: PacketAck,
				ID:   uint64Pointer(15),
				Data: []any{"bar", []byte{0x01, 0x02, 0x03, 0x04}},
			},
			header:      `61-15["bar",{"_placeholder":true,"num":0}]`,
			attachments: [][]byte{{0x01, 0x02, 0x03, 0x04}},
		},
		{
			name:   "disconnect custom namespace",
			packet: Packet{Type: PacketDisconnect, Namespace: "/admin"},
			header: `1/admin,`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := codec.Encode(test.packet)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			if got := string(encoded.Header); got != test.header {
				t.Fatalf("Encode() header = %q, want %q", got, test.header)
			}
			if !reflect.DeepEqual(encoded.Attachments, test.attachments) {
				t.Fatalf("Encode() attachments = %v, want %v", encoded.Attachments, test.attachments)
			}
		})
	}
}

func TestDecodeAndReconstructBinaryEvent(t *testing.T) {
	t.Parallel()

	codec := NewCodec(0, 0)
	packet, err := codec.DecodeHeader([]byte(`52-/admin,7["upload",{"file":{"_placeholder":true,"num":0}}]`))
	if err != nil {
		t.Fatalf("DecodeHeader() error = %v", err)
	}
	if packet.Type != PacketBinaryEvent || packet.Namespace != "/admin" || packet.ID == nil || *packet.ID != 7 || packet.Attachments != 2 {
		t.Fatalf("DecodeHeader() metadata = %#v", packet)
	}

	if _, err := codec.Reconstruct(packet, [][]byte{{0x01}}); !errors.Is(err, ErrInvalidAttachments) {
		t.Fatalf("Reconstruct(short attachments) error = %v, want ErrInvalidAttachments", err)
	}

	// Change the header to declare the one attachment it actually references.
	packet.Attachments = 1
	attachment := []byte{0x01, 0x02}
	reconstructed, err := codec.Reconstruct(packet, [][]byte{attachment})
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
	attachment[0] = 0xff

	values := reconstructed.Data.([]any)
	object := values[1].(map[string]any)
	file := object["file"].([]byte)
	if !bytes.Equal(file, []byte{0x01, 0x02}) {
		t.Fatalf("Reconstruct() binary = %x, want 0102", file)
	}
}

func TestEncodeDoesNotRetainBinaryStorage(t *testing.T) {
	t.Parallel()

	codec := NewCodec(0, 0)
	binary := []byte{0x01, 0x02}
	encoded, err := codec.Encode(Packet{Type: PacketEvent, Data: []any{"file", binary}})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	binary[0] = 0xff
	if encoded.Attachments[0][0] != 0x01 {
		t.Fatal("Encode() retained mutable caller storage")
	}
}

func TestDecodeValidation(t *testing.T) {
	t.Parallel()

	codec := NewCodec(2, 8)
	tests := []struct {
		name   string
		header string
		target error
	}{
		{name: "empty", header: "", target: ErrEmptyPacket},
		{name: "unknown type", header: "9", target: ErrInvalidPacketType},
		{name: "event missing data", header: "2", target: ErrInvalidPayload},
		{name: "event name not string", header: `2[1,"value"]`, target: ErrInvalidPayload},
		{name: "ack missing id", header: `3["value"]`, target: ErrInvalidPacketID},
		{name: "binary count missing", header: `5-["event"]`, target: ErrInvalidAttachments},
		{name: "binary count zero", header: `50-["event"]`, target: ErrInvalidAttachments},
		{name: "attachment limit", header: `53-["event"]`, target: ErrAttachmentLimit},
		{name: "namespace delimiter", header: `2/admin`, target: ErrInvalidNamespace},
		{name: "invalid JSON", header: `2["event",]`, target: ErrInvalidPayload},
		{name: "disconnect with data", header: `1["data"]`, target: ErrInvalidPayload},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := codec.DecodeHeader([]byte(test.header)); !errors.Is(err, test.target) {
				t.Fatalf("DecodeHeader(%q) error = %v, want %v", test.header, err, test.target)
			}
		})
	}
}

func TestEncodeValidation(t *testing.T) {
	t.Parallel()

	codec := NewCodec(1, 4)
	tests := []struct {
		name   string
		packet Packet
		target error
	}{
		{name: "namespace", packet: Packet{Type: PacketConnect, Namespace: "admin"}, target: ErrInvalidNamespace},
		{name: "unsupported value", packet: Packet{Type: PacketEvent, Data: []string{"event"}}, target: ErrUnsupportedDataType},
		{name: "too many attachments", packet: Packet{Type: PacketEvent, Data: []any{"event", []byte{1}, []byte{2}}}, target: ErrAttachmentLimit},
		{name: "ack id", packet: Packet{Type: PacketAck, Data: []any{}}, target: ErrInvalidPacketID},
		{name: "connect payload", packet: Packet{Type: PacketConnect, Data: []any{}}, target: ErrInvalidPayload},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := codec.Encode(test.packet); !errors.Is(err, test.target) {
				t.Fatalf("Encode(%#v) error = %v, want %v", test.packet, err, test.target)
			}
		})
	}
}

func TestRawJSONNormalization(t *testing.T) {
	t.Parallel()

	codec := NewCodec(0, 0)
	encoded, err := codec.Encode(Packet{
		Type: PacketEvent,
		Data: []any{"event", json.RawMessage(`{"answer":42}`)},
	})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if got, want := string(encoded.Header), `2["event",{"answer":42}]`; got != want {
		t.Fatalf("Encode() = %q, want %q", got, want)
	}
}

func FuzzDecodeHeader(f *testing.F) {
	f.Add([]byte(`2["event","value"]`))
	f.Add([]byte(`0/admin,{"token":"123"}`))
	f.Add([]byte(`51-["file",{"_placeholder":true,"num":0}]`))
	f.Add([]byte{})

	codec := NewCodec(8, 16)
	f.Fuzz(func(t *testing.T, header []byte) {
		packet, err := codec.DecodeHeader(header)
		if err != nil || packet.Type.Binary() {
			return
		}

		encoded, err := codec.Encode(packet)
		if err != nil {
			t.Fatalf("successfully decoded packet could not be encoded: %v", err)
		}
		if len(encoded.Attachments) != 0 {
			t.Fatal("non-binary decoded packet unexpectedly encoded attachments")
		}
	})
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
