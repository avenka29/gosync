package socketio

import "fmt"

// PacketType identifies a Socket.IO protocol revision 5 packet.
type PacketType byte

const (
	PacketConnect PacketType = iota
	PacketDisconnect
	PacketEvent
	PacketAck
	PacketConnectError
	PacketBinaryEvent
	PacketBinaryAck
)

// Valid reports whether the packet type is defined by Socket.IO revision 5.
func (t PacketType) Valid() bool {
	return t >= PacketConnect && t <= PacketBinaryAck
}

// Binary reports whether the packet is followed by binary attachments.
func (t PacketType) Binary() bool {
	return t == PacketBinaryEvent || t == PacketBinaryAck
}

func (t PacketType) String() string {
	switch t {
	case PacketConnect:
		return "connect"
	case PacketDisconnect:
		return "disconnect"
	case PacketEvent:
		return "event"
	case PacketAck:
		return "ack"
	case PacketConnectError:
		return "connect_error"
	case PacketBinaryEvent:
		return "binary_event"
	case PacketBinaryAck:
		return "binary_ack"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// Packet is the transport-independent representation of one Socket.IO packet.
// Data must be a normalized JSON value: nil, bool, string, json.Number, a numeric
// Go value, []any, map[string]any, json.RawMessage, or []byte for binary data.
type Packet struct {
	Type PacketType

	// Namespace defaults to "/" when empty.
	Namespace string

	// ID is present when an event requests an acknowledgement or an ACK answers
	// one. A pointer distinguishes packet ID zero from an absent packet ID.
	ID *uint64

	Data any

	// Attachments is populated while decoding a binary packet header. Call
	// Codec.Reconstruct after receiving exactly this many binary attachments.
	Attachments int
}

// EncodedPacket contains one text header and any binary Engine.IO message
// payloads that must immediately follow it.
type EncodedPacket struct {
	Header      []byte
	Attachments [][]byte
}
