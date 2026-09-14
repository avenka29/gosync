package engineio

import (
	"bytes"
	"errors"
	"testing"
)

func TestPollingPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	frames := []Frame{
		{Payload: []byte("4hello")},
		{Payload: []byte("2")},
		{Payload: []byte{1, 2, 3, 4}, Binary: true},
	}
	payload, err := EncodePayload(frames, 1024)
	if err != nil {
		t.Fatalf("EncodePayload() error = %v", err)
	}
	wantPayload := []byte("4hello\x1e2\x1ebAQIDBA==")
	if !bytes.Equal(payload, wantPayload) {
		t.Fatalf("EncodePayload() = %q, want %q", payload, wantPayload)
	}

	decoded, err := DecodePayload(payload, 1024)
	if err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}
	if len(decoded) != len(frames) {
		t.Fatalf("DecodePayload() returned %d frames, want %d", len(decoded), len(frames))
	}
	for index := range frames {
		if decoded[index].Binary != frames[index].Binary || !bytes.Equal(decoded[index].Payload, frames[index].Payload) {
			t.Fatalf("frame %d = %#v, want %#v", index, decoded[index], frames[index])
		}
	}
}

func TestPollingPayloadRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		max     int64
		want    error
	}{
		{name: "empty", max: 10, want: ErrEmptyPayload},
		{name: "empty packet", payload: []byte("4x\x1e"), max: 10, want: ErrEmptyPayload},
		{name: "invalid packet", payload: []byte("9x"), max: 10, want: ErrInvalidPacketType},
		{name: "invalid UTF-8", payload: []byte{'4', 0xff}, max: 10, want: ErrInvalidTextEncoding},
		{name: "invalid base64", payload: []byte("b!"), max: 10, want: ErrInvalidBase64Packet},
		{name: "too large", payload: []byte("4large"), max: 3, want: ErrPayloadTooLarge},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodePayload(test.payload, test.max); !errors.Is(err, test.want) {
				t.Fatalf("DecodePayload() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestEncodePollingPayloadRejectsInvalidFramesAndSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		frames []Frame
		max    int64
		want   error
	}{
		{name: "empty", max: 10, want: ErrEmptyPayload},
		{name: "invalid frame", frames: []Frame{{Payload: []byte("9x")}}, max: 10, want: ErrInvalidPacketType},
		{name: "separator", frames: []Frame{{Payload: []byte("4x\x1ey")}}, max: 10, want: ErrPacketContainsSeparator},
		{name: "too large", frames: []Frame{{Payload: []byte("4large")}}, max: 3, want: ErrPayloadTooLarge},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := EncodePayload(test.frames, test.max); !errors.Is(err, test.want) {
				t.Fatalf("EncodePayload() error = %v, want %v", err, test.want)
			}
		})
	}
}

func FuzzDecodePayload(f *testing.F) {
	f.Add([]byte("4hello\x1e2"))
	f.Add([]byte("bAQIDBA=="))
	f.Add([]byte("4x\x1e"))

	f.Fuzz(func(t *testing.T, payload []byte) {
		_, _ = DecodePayload(payload, 4096)
	})
}
