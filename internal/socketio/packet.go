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

// Packet is one transport-independent Socket.IO packet.
type Packet struct {
	Type PacketType

	Namespace string

	ID *uint64

	Data any

	Attachments int
}

// EncodedPacket contains a text header followed by binary attachments.
type EncodedPacket struct {
	Header      []byte
	Attachments [][]byte
}
