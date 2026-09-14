package transport

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
)

// DeadlineStream is a WebTransport bidirectional stream (or a test pipe).
type DeadlineStream interface {
	io.ReadWriteCloser
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}
type WebTransport struct {
	stream       DeadlineStream
	maxPayload   int64
	closeSession func() error
	once         sync.Once
	closeErr     error
}

func NewWebTransport(stream DeadlineStream, maxPayload int64, closeSession func() error) *WebTransport {
	if maxPayload <= 0 {
		maxPayload = engineio.DefaultMaxPayload
	}
	if strconv.IntSize == 32 && maxPayload > math.MaxInt32 {
		maxPayload = math.MaxInt32
	}
	return &WebTransport{stream: stream, maxPayload: maxPayload, closeSession: closeSession}
}

func (t *WebTransport) Read(ctx context.Context) (engineio.Frame, error) {
	if err := ctx.Err(); err != nil {
		return engineio.Frame{}, err
	}
	deadline, _ := ctx.Deadline()
	if err := t.stream.SetReadDeadline(deadline); err != nil {
		return engineio.Frame{}, err
	}
	var header [9]byte
	if _, err := io.ReadFull(t.stream, header[:1]); err != nil {
		return engineio.Frame{}, err
	}
	binaryPayload := header[0]&128 != 0
	length := uint64(header[0] & 127)
	switch length {
	case 126:
		if _, err := io.ReadFull(t.stream, header[:2]); err != nil {
			return engineio.Frame{}, err
		}
		length = uint64(binary.BigEndian.Uint16(header[:2]))
	case 127:
		if _, err := io.ReadFull(t.stream, header[:8]); err != nil {
			return engineio.Frame{}, err
		}
		length = binary.BigEndian.Uint64(header[:8])
	}
	if length > math.MaxInt64 || int64(length) > t.maxPayload {
		return engineio.Frame{}, engineio.ErrPayloadTooLarge
	}
	payload := make([]byte, int(length))
	_, err := io.ReadFull(t.stream, payload)
	return engineio.Frame{Payload: payload, Binary: binaryPayload}, err
}

func (t *WebTransport) Write(ctx context.Context, frame engineio.Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if int64(len(frame.Payload)) > t.maxPayload {
		return engineio.ErrPayloadTooLarge
	}
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := t.stream.SetWriteDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = t.Close() })
	defer stop()
	var header [9]byte
	n := 1
	length := len(frame.Payload)
	switch {
	case length < 126:
		header[0] = byte(length)
	case length <= 65535:
		header[0] = 126
		binary.BigEndian.PutUint16(header[1:3], uint16(length))
		n = 3
	default:
		header[0] = 127
		binary.BigEndian.PutUint64(header[1:9], uint64(length))
		n = 9
	}
	if frame.Binary {
		header[0] |= 128
	}
	if err := writeAll(t.stream, header[:n]); err != nil {
		return err
	}
	return writeAll(t.stream, frame.Payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (t *WebTransport) Close() error {
	t.once.Do(func() {
		if t.closeSession != nil {
			t.closeErr = t.closeSession()
		} else {
			t.closeErr = t.stream.Close()
		}
	})
	return t.closeErr
}
