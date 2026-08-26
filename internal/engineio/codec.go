package engineio

import (
	"errors"
	"fmt"
)

var (
	// ErrEmptyFrame indicates that a text frame did not include a packet type.
	ErrEmptyFrame = errors.New("engineio: empty frame")

	// ErrInvalidPacketType indicates an unknown Engine.IO packet type.
	ErrInvalidPacketType = errors.New("engineio: invalid packet type")

	// ErrInvalidBinaryPacket indicates an attempt to represent a non-message
	// packet as a binary WebSocket frame.
	ErrInvalidBinaryPacket = errors.New("engineio: only message packets may be binary")
)

// EncodeFrame encodes a packet as one Engine.IO WebSocket frame.
func EncodeFrame(packet Packet) (Frame, error) {
	if !packet.Type.Valid() {
		return Frame{}, fmt.Errorf("%w: %d", ErrInvalidPacketType, packet.Type)
	}

	if packet.Binary {
		if packet.Type != PacketMessage {
			return Frame{}, ErrInvalidBinaryPacket
		}

		return Frame{Payload: cloneBytes(packet.Data), Binary: true}, nil
	}

	payload := make([]byte, 1+len(packet.Data))
	payload[0] = byte(packet.Type) + '0'
	copy(payload[1:], packet.Data)

	return Frame{Payload: payload}, nil
}

// DecodeFrame decodes one Engine.IO WebSocket frame. A binary WebSocket frame
// is an Engine.IO message packet whose payload is the frame bytes themselves.
func DecodeFrame(frame Frame) (Packet, error) {
	if frame.Binary {
		return Packet{
			Type:   PacketMessage,
			Data:   cloneBytes(frame.Payload),
			Binary: true,
		}, nil
	}

	if len(frame.Payload) == 0 {
		return Packet{}, ErrEmptyFrame
	}

	packetType := PacketType(frame.Payload[0] - '0')
	if frame.Payload[0] < '0' || frame.Payload[0] > '6' || !packetType.Valid() {
		return Packet{}, fmt.Errorf("%w: %q", ErrInvalidPacketType, frame.Payload[0])
	}

	return Packet{
		Type: packetType,
		Data: cloneBytes(frame.Payload[1:]),
	}, nil
}

func cloneBytes(source []byte) []byte {
	if source == nil {
		return nil
	}

	cloned := make([]byte, len(source))
	copy(cloned, source)
	return cloned
}
