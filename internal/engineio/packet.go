package engineio

import "fmt"

// PacketType identifies an Engine.IO protocol revision 4 packet.
type PacketType byte

const (
	PacketOpen PacketType = iota
	PacketClose
	PacketPing
	PacketPong
	PacketMessage
	PacketUpgrade
	PacketNoop
)

// Valid reports whether the packet type is defined by Engine.IO revision 4.
func (t PacketType) Valid() bool {
	return t >= PacketOpen && t <= PacketNoop
}

func (t PacketType) String() string {
	switch t {
	case PacketOpen:
		return "open"
	case PacketClose:
		return "close"
	case PacketPing:
		return "ping"
	case PacketPong:
		return "pong"
	case PacketMessage:
		return "message"
	case PacketUpgrade:
		return "upgrade"
	case PacketNoop:
		return "noop"
	default:
		return fmt.Sprintf("unknown(%d)", byte(t))
	}
}

// Packet is the transport-independent representation of one Engine.IO packet.
// Binary is valid only for message packets.
type Packet struct {
	Type   PacketType
	Data   []byte
	Binary bool
}

// Frame is one WebSocket frame. Payload is text when Binary is false and raw
// binary data when Binary is true.
type Frame struct {
	Payload []byte
	Binary  bool
}
