package engineio

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
)

const pollingPacketSeparator byte = 0x1e

var (
	ErrEmptyPayload            = errors.New("engineio: empty payload")
	ErrPacketContainsSeparator = errors.New("engineio: text packet contains the polling separator")
	ErrInvalidBase64Packet     = errors.New("engineio: invalid base64 packet")
)

// EncodePayload encodes one or more Engine.IO frames for HTTP long-polling.
func EncodePayload(frames []Frame, maxPayload int64) ([]byte, error) {
	if len(frames) == 0 {
		return nil, ErrEmptyPayload
	}

	var payload bytes.Buffer
	for index, frame := range frames {
		encoded, err := encodePayloadFrame(frame)
		if err != nil {
			return nil, err
		}
		additional := len(encoded)
		if index > 0 {
			additional++
		}
		if maxPayload > 0 && int64(payload.Len()+additional) > maxPayload {
			return nil, ErrPayloadTooLarge
		}
		if index > 0 {
			payload.WriteByte(pollingPacketSeparator)
		}
		payload.Write(encoded)
	}

	return payload.Bytes(), nil
}

// DecodePayload decodes an Engine.IO HTTP long-polling request body.
func DecodePayload(payload []byte, maxPayload int64) ([]Frame, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyPayload
	}
	if maxPayload > 0 && int64(len(payload)) > maxPayload {
		return nil, ErrPayloadTooLarge
	}

	parts := bytes.Split(payload, []byte{pollingPacketSeparator})
	frames := make([]Frame, 0, len(parts))
	for _, part := range parts {
		if len(part) == 0 {
			return nil, ErrEmptyPayload
		}

		if part[0] == 'b' {
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(part)-1))
			n, err := base64.StdEncoding.Strict().Decode(decoded, part[1:])
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidBase64Packet, err)
			}
			frames = append(frames, Frame{Payload: decoded[:n], Binary: true})
			continue
		}

		frame := Frame{Payload: cloneBytes(part)}
		if _, err := DecodeFrame(frame); err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}

	return frames, nil
}

func encodePayloadFrame(frame Frame) ([]byte, error) {
	if frame.Binary {
		encoded := make([]byte, 1+base64.StdEncoding.EncodedLen(len(frame.Payload)))
		encoded[0] = 'b'
		base64.StdEncoding.Encode(encoded[1:], frame.Payload)
		return encoded, nil
	}
	if bytes.IndexByte(frame.Payload, pollingPacketSeparator) >= 0 {
		return nil, ErrPacketContainsSeparator
	}
	if _, err := DecodeFrame(frame); err != nil {
		return nil, err
	}
	return cloneBytes(frame.Payload), nil
}
