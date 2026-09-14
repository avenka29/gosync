package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
)

func TestWebTransportFramingLengthsAndBinary(t *testing.T) {
	for _, size := range []int{0, 1, 125, 126, 65535, 65536} {
		for _, isBinary := range []bool{false, true} {
			left, right := net.Pipe()
			a := NewWebTransport(left, 0, nil)
			b := NewWebTransport(right, 0, nil)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			payload := bytes.Repeat([]byte{'a'}, size)
			done := make(chan error, 1)
			go func() { done <- a.Write(ctx, engineio.Frame{Payload: payload, Binary: isBinary}) }()
			got, err := b.Read(ctx)
			if err != nil || got.Binary != isBinary || !bytes.Equal(got.Payload, payload) {
				t.Fatalf("size=%d binary=%v err=%v", size, isBinary, err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			cancel()
			_ = a.Close()
			_ = b.Close()
		}
	}
}

func TestWebTransportRejectsOversizeBeforeAllocation(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	tr := NewWebTransport(right, 128, nil)
	header := make([]byte, 9)
	header[0] = 127
	binary.BigEndian.PutUint64(header[1:], ^uint64(0))
	go func() { _, _ = left.Write(header) }()
	if _, err := tr.Read(context.Background()); !errors.Is(err, engineio.ErrPayloadTooLarge) {
		t.Fatal(err)
	}
	if err := tr.Write(context.Background(), engineio.Frame{Payload: make([]byte, 129)}); !errors.Is(err, engineio.ErrPayloadTooLarge) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tr.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := tr.Write(ctx, engineio.Frame{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := writeAll(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }
