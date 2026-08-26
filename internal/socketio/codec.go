package socketio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	DefaultMaxAttachments = 32
	DefaultMaxDepth       = 100
)

var (
	ErrEmptyPacket         = errors.New("socketio: empty packet")
	ErrInvalidPacketType   = errors.New("socketio: invalid packet type")
	ErrInvalidNamespace    = errors.New("socketio: invalid namespace")
	ErrInvalidPacketID     = errors.New("socketio: invalid packet id")
	ErrInvalidPayload      = errors.New("socketio: invalid packet payload")
	ErrInvalidAttachments  = errors.New("socketio: invalid binary attachments")
	ErrAttachmentLimit     = errors.New("socketio: binary attachment limit exceeded")
	ErrNestingLimit        = errors.New("socketio: payload nesting limit exceeded")
	ErrUnsupportedDataType = errors.New("socketio: unsupported normalized data type")
)

// Codec validates and transforms Socket.IO protocol revision 5 packets.
type Codec struct {
	maxAttachments int
	maxDepth       int
}

// NewCodec constructs a codec. Non-positive limits use secure defaults.
func NewCodec(maxAttachments, maxDepth int) Codec {
	if maxAttachments <= 0 {
		maxAttachments = DefaultMaxAttachments
	}
	if maxDepth <= 0 {
		maxDepth = DefaultMaxDepth
	}

	return Codec{maxAttachments: maxAttachments, maxDepth: maxDepth}
}

// Encode validates and encodes one Socket.IO packet. The returned header is the
// payload of a text Engine.IO message packet; it does not include Engine.IO's
// leading "4" packet type.
func (c Codec) Encode(packet Packet) (EncodedPacket, error) {
	if !packet.Type.Valid() {
		return EncodedPacket{}, fmt.Errorf("%w: %d", ErrInvalidPacketType, packet.Type)
	}

	namespace, err := normalizeNamespace(packet.Namespace)
	if err != nil {
		return EncodedPacket{}, err
	}
	packet.Namespace = namespace

	var attachments [][]byte
	data, err := c.deconstruct(packet.Data, &attachments, 0)
	if err != nil {
		return EncodedPacket{}, err
	}
	packet.Data = data

	switch packet.Type {
	case PacketEvent:
		if len(attachments) > 0 {
			packet.Type = PacketBinaryEvent
		}
	case PacketAck:
		if len(attachments) > 0 {
			packet.Type = PacketBinaryAck
		}
	case PacketBinaryEvent, PacketBinaryAck:
		if len(attachments) == 0 {
			return EncodedPacket{}, fmt.Errorf("%w: binary packet contains no binary value", ErrInvalidAttachments)
		}
	default:
		if len(attachments) > 0 {
			return EncodedPacket{}, fmt.Errorf("%w: %s packets cannot contain binary values", ErrInvalidPayload, packet.Type)
		}
	}

	packet.Attachments = len(attachments)
	if err := validatePacket(packet); err != nil {
		return EncodedPacket{}, err
	}

	header := make([]byte, 0, 64)
	header = append(header, byte(packet.Type)+'0')
	if packet.Type.Binary() {
		header = strconv.AppendInt(header, int64(len(attachments)), 10)
		header = append(header, '-')
	}
	if namespace != "/" {
		header = append(header, namespace...)
		header = append(header, ',')
	}
	if packet.ID != nil {
		header = strconv.AppendUint(header, *packet.ID, 10)
	}
	if packet.Data != nil {
		encodedData, err := json.Marshal(packet.Data)
		if err != nil {
			return EncodedPacket{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		header = append(header, encodedData...)
	}

	return EncodedPacket{Header: header, Attachments: attachments}, nil
}

// DecodeHeader validates and decodes one Socket.IO text header. Binary packet
// data still contains placeholders until Reconstruct is called.
func (c Codec) DecodeHeader(header []byte) (Packet, error) {
	if len(header) == 0 {
		return Packet{}, ErrEmptyPacket
	}
	if header[0] < '0' || header[0] > '6' {
		return Packet{}, fmt.Errorf("%w: %q", ErrInvalidPacketType, header[0])
	}

	packet := Packet{Type: PacketType(header[0] - '0'), Namespace: "/"}
	position := 1

	if packet.Type.Binary() {
		dash := bytes.IndexByte(header[position:], '-')
		if dash < 1 {
			return Packet{}, fmt.Errorf("%w: missing attachment count", ErrInvalidAttachments)
		}
		dash += position
		count, err := strconv.ParseUint(string(header[position:dash]), 10, 31)
		if err != nil || count == 0 {
			return Packet{}, fmt.Errorf("%w: invalid attachment count", ErrInvalidAttachments)
		}
		if count > uint64(c.maxAttachments) {
			return Packet{}, fmt.Errorf("%w: %d > %d", ErrAttachmentLimit, count, c.maxAttachments)
		}
		packet.Attachments = int(count)
		position = dash + 1
	}

	if position < len(header) && header[position] == '/' {
		comma := bytes.IndexByte(header[position:], ',')
		if comma < 0 {
			return Packet{}, fmt.Errorf("%w: custom namespace is missing delimiter", ErrInvalidNamespace)
		}
		comma += position
		packet.Namespace = string(header[position:comma])
		position = comma + 1
	}
	if _, err := normalizeNamespace(packet.Namespace); err != nil {
		return Packet{}, err
	}

	idStart := position
	for position < len(header) && header[position] >= '0' && header[position] <= '9' {
		position++
	}
	if position > idStart {
		id, err := strconv.ParseUint(string(header[idStart:position]), 10, 64)
		if err != nil {
			return Packet{}, fmt.Errorf("%w: %v", ErrInvalidPacketID, err)
		}
		packet.ID = &id
	}

	if position < len(header) {
		if header[position] != '[' && header[position] != '{' {
			return Packet{}, fmt.Errorf("%w: JSON payload must be an array or object", ErrInvalidPayload)
		}

		decoder := json.NewDecoder(bytes.NewReader(header[position:]))
		decoder.UseNumber()
		if err := decoder.Decode(&packet.Data); err != nil {
			return Packet{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return Packet{}, fmt.Errorf("%w: trailing JSON data", ErrInvalidPayload)
			}
			return Packet{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
	}

	if err := validatePacket(packet); err != nil {
		return Packet{}, err
	}

	return packet, nil
}

// Reconstruct replaces binary placeholders with owned attachment bytes.
func (c Codec) Reconstruct(packet Packet, attachments [][]byte) (Packet, error) {
	if !packet.Type.Binary() || packet.Attachments <= 0 {
		return Packet{}, fmt.Errorf("%w: packet is not awaiting binary data", ErrInvalidAttachments)
	}
	if packet.Attachments != len(attachments) {
		return Packet{}, fmt.Errorf("%w: got %d attachments, want %d", ErrInvalidAttachments, len(attachments), packet.Attachments)
	}
	if len(attachments) > c.maxAttachments {
		return Packet{}, fmt.Errorf("%w: %d > %d", ErrAttachmentLimit, len(attachments), c.maxAttachments)
	}

	used := make([]bool, len(attachments))
	data, err := c.reconstructValue(packet.Data, attachments, used, 0)
	if err != nil {
		return Packet{}, err
	}
	for index, referenced := range used {
		if !referenced {
			return Packet{}, fmt.Errorf("%w: attachment %d is not referenced", ErrInvalidAttachments, index)
		}
	}

	packet.Data = data
	packet.Attachments = 0
	return packet, nil
}

func (c Codec) deconstruct(value any, attachments *[][]byte, depth int) (any, error) {
	if depth > c.maxDepth {
		return nil, ErrNestingLimit
	}

	switch typed := value.(type) {
	case nil, bool, string, json.Number,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return typed, nil
	case []byte:
		if len(*attachments) >= c.maxAttachments {
			return nil, fmt.Errorf("%w: maximum is %d", ErrAttachmentLimit, c.maxAttachments)
		}
		index := len(*attachments)
		*attachments = append(*attachments, cloneBytes(typed))
		return map[string]any{"_placeholder": true, "num": index}, nil
	case json.RawMessage:
		decoder := json.NewDecoder(bytes.NewReader(typed))
		decoder.UseNumber()
		var normalized any
		if err := decoder.Decode(&normalized); err != nil {
			return nil, fmt.Errorf("%w: invalid raw JSON: %v", ErrInvalidPayload, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return nil, fmt.Errorf("%w: trailing raw JSON data", ErrInvalidPayload)
			}
			return nil, fmt.Errorf("%w: invalid raw JSON: %v", ErrInvalidPayload, err)
		}
		return c.deconstruct(normalized, attachments, depth+1)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			converted, err := c.deconstruct(item, attachments, depth+1)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := c.deconstruct(item, attachments, depth+1)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedDataType, value)
	}
}

func (c Codec) reconstructValue(value any, attachments [][]byte, used []bool, depth int) (any, error) {
	if depth > c.maxDepth {
		return nil, ErrNestingLimit
	}

	switch typed := value.(type) {
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			converted, err := c.reconstructValue(item, attachments, used, depth+1)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case map[string]any:
		if placeholder, ok := typed["_placeholder"].(bool); ok && placeholder {
			index, err := placeholderIndex(typed["num"])
			if err != nil || index < 0 || index >= len(attachments) {
				return nil, fmt.Errorf("%w: invalid placeholder index", ErrInvalidAttachments)
			}
			used[index] = true
			return cloneBytes(attachments[index]), nil
		}

		result := make(map[string]any, len(typed))
		for key, item := range typed {
			converted, err := c.reconstructValue(item, attachments, used, depth+1)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return typed, nil
	}
}

func validatePacket(packet Packet) error {
	if !packet.Type.Valid() {
		return fmt.Errorf("%w: %d", ErrInvalidPacketType, packet.Type)
	}
	if _, err := normalizeNamespace(packet.Namespace); err != nil {
		return err
	}

	switch packet.Type {
	case PacketConnect:
		if packet.ID != nil || (packet.Data != nil && !isObject(packet.Data)) {
			return fmt.Errorf("%w: connect data must be an object and cannot have an id", ErrInvalidPayload)
		}
	case PacketDisconnect:
		if packet.ID != nil || packet.Data != nil {
			return fmt.Errorf("%w: disconnect packets cannot contain data or an id", ErrInvalidPayload)
		}
	case PacketEvent, PacketBinaryEvent:
		values, ok := packet.Data.([]any)
		if !ok || len(values) == 0 {
			return fmt.Errorf("%w: event data must be a non-empty array", ErrInvalidPayload)
		}
		if _, ok := values[0].(string); !ok {
			return fmt.Errorf("%w: first event value must be a string", ErrInvalidPayload)
		}
	case PacketAck, PacketBinaryAck:
		if packet.ID == nil {
			return fmt.Errorf("%w: acknowledgement packet requires an id", ErrInvalidPacketID)
		}
		if _, ok := packet.Data.([]any); !ok {
			return fmt.Errorf("%w: acknowledgement data must be an array", ErrInvalidPayload)
		}
	case PacketConnectError:
		if packet.ID != nil || !isObject(packet.Data) {
			return fmt.Errorf("%w: connect error data must be an object and cannot have an id", ErrInvalidPayload)
		}
	}

	if packet.Type.Binary() && packet.Attachments <= 0 {
		return fmt.Errorf("%w: binary packet requires attachments", ErrInvalidAttachments)
	}

	return nil
}

func normalizeNamespace(namespace string) (string, error) {
	if namespace == "" {
		return "/", nil
	}
	if namespace[0] != '/' || strings.ContainsRune(namespace, ',') {
		return "", fmt.Errorf("%w: %q", ErrInvalidNamespace, namespace)
	}
	return namespace, nil
}

func isObject(value any) bool {
	_, ok := value.(map[string]any)
	return ok
}

func placeholderIndex(value any) (int, error) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := strconv.ParseInt(string(typed), 10, 32)
		return int(parsed), err
	case int:
		return typed, nil
	case float64:
		if typed != float64(int(typed)) {
			return 0, ErrInvalidAttachments
		}
		return int(typed), nil
	default:
		return 0, ErrInvalidAttachments
	}
}

func cloneBytes(source []byte) []byte {
	if source == nil {
		return nil
	}
	cloned := make([]byte, len(source))
	copy(cloned, source)
	return cloned
}
