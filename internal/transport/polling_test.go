package transport

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/engineio"
)

func TestPollingCarriesPacketsBetweenRequestsAndSession(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 4, 4)
	if err != nil {
		t.Fatalf("NewPolling() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := polling.Write(ctx, engineio.Frame{Payload: []byte("4first")}); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}
	if err := polling.Write(ctx, engineio.Frame{Payload: []byte{1, 2, 3}, Binary: true}); err != nil {
		t.Fatalf("Write(binary) error = %v", err)
	}
	payload, err := polling.Poll(ctx)
	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}
	if got, want := string(payload), "4first\x1ebAQID"; got != want {
		t.Fatalf("Poll() = %q, want %q", got, want)
	}

	if err := polling.Submit(ctx, []byte("4second\x1e3")); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	first, err := polling.Read(ctx)
	if err != nil || string(first.Payload) != "4second" {
		t.Fatalf("Read(first) = %#v, %v", first, err)
	}
	second, err := polling.Read(ctx)
	if err != nil || string(second.Payload) != "3" {
		t.Fatalf("Read(second) = %#v, %v", second, err)
	}
}

func TestPollingKeepsOversizedBatchPacketForNextRequest(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(7, 1, 2)
	if err != nil {
		t.Fatalf("NewPolling() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := polling.Write(ctx, engineio.Frame{Payload: []byte("4one")}); err != nil {
		t.Fatal(err)
	}
	if err := polling.Write(ctx, engineio.Frame{Payload: []byte("4two")}); err != nil {
		t.Fatal(err)
	}
	first, err := polling.Poll(ctx)
	if err != nil || string(first) != "4one" {
		t.Fatalf("first Poll() = %q, %v", first, err)
	}
	second, err := polling.Poll(ctx)
	if err != nil || string(second) != "4two" {
		t.Fatalf("second Poll() = %q, %v", second, err)
	}
}

func TestPollingRejectsConcurrentRequestsAndCloses(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatalf("NewPolling() error = %v", err)
	}
	firstResult := make(chan error, 1)
	go func() {
		_, err := polling.Poll(context.Background())
		firstResult <- err
	}()

	deadline := time.Now().Add(time.Second)
	for !polling.pollActive.Load() {
		if time.Now().After(deadline) {
			t.Fatal("first Poll() did not become active")
		}
		runtime.Gosched()
	}

	if _, err := polling.Poll(context.Background()); !errors.Is(err, ErrConcurrentPoll) {
		t.Fatalf("second Poll() error = %v, want ErrConcurrentPoll", err)
	}
	if err := <-firstResult; err != nil {
		t.Fatalf("first Poll() error = %v", err)
	}
	if _, err := polling.Read(context.Background()); !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("Read() after overlap error = %v, want ErrPollingClosed", err)
	}
}

func TestPollingRejectsConcurrentPostsAndCloses(t *testing.T) {
	t.Parallel()

	polling := &Polling{
		maxPayload: 1024,
		inbound:    make(chan engineio.Frame),
		outbound:   make(chan engineio.Frame, 1),
		closed:     make(chan struct{}),
	}
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- polling.Submit(context.Background(), []byte("4first"))
	}()

	deadline := time.Now().Add(time.Second)
	for !polling.postActive.Load() {
		if time.Now().After(deadline) {
			t.Fatal("first Submit() did not become active")
		}
		runtime.Gosched()
	}

	if err := polling.Submit(context.Background(), []byte("4second")); !errors.Is(err, ErrConcurrentPost) {
		t.Fatalf("second Submit() error = %v, want ErrConcurrentPost", err)
	}
	if err := <-firstResult; !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("first Submit() error = %v, want ErrPollingClosed", err)
	}
}

func TestPollingRejectsInvalidConfigurationAndFullOutput(t *testing.T) {
	t.Parallel()

	if _, err := NewPolling(0, 1, 1); !errors.Is(err, ErrInvalidPollingConfig) {
		t.Fatalf("NewPolling() error = %v, want ErrInvalidPollingConfig", err)
	}
	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := polling.Write(context.Background(), engineio.Frame{Payload: []byte("4one")}); err != nil {
		t.Fatal(err)
	}
	if err := polling.Write(context.Background(), engineio.Frame{Payload: []byte("4two")}); !errors.Is(err, ErrPollingQueueFull) {
		t.Fatalf("Write() error = %v, want ErrPollingQueueFull", err)
	}
}

func TestPollingReadCancellationClosureAndCopies(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := polling.Read(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read(canceled) error = %v", err)
	}

	source := []byte("4copy")
	if err := polling.Submit(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	source[1] = 'X'
	frame, err := polling.Read(context.Background())
	if err != nil || string(frame.Payload) != "4copy" {
		t.Fatalf("Read() = %q, %v", frame.Payload, err)
	}
	frame.Payload[1] = 'Y'

	if err := polling.Close(); err != nil {
		t.Fatal(err)
	}
	if err := polling.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := polling.Read(context.Background()); !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("Read(closed) error = %v", err)
	}
}

func TestPollingBlockedReadUnblocksOnCancellationAndClose(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := newSelectContext()
	canceledResult := make(chan error, 1)
	go func() {
		_, err := polling.Read(ctx)
		canceledResult <- err
	}()
	<-ctx.entered
	ctx.cancel()
	if err := <-canceledResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Read() error = %v, want context.Canceled", err)
	}

	polling, err = NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	closeContext := newSelectContext()
	closedResult := make(chan error, 1)
	go func() {
		_, err := polling.Read(closeContext)
		closedResult <- err
	}()
	<-closeContext.entered
	polling.Close()
	if err := <-closedResult; !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("blocked Read() error = %v, want ErrPollingClosed", err)
	}
}

func TestPollingWriteValidationCancellationClosureAndCopies(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(8, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := polling.Write(context.Background(), engineio.Frame{Payload: []byte("9bad")}); !errors.Is(err, engineio.ErrInvalidPacketType) {
		t.Fatalf("Write(invalid) error = %v", err)
	}
	if err := polling.Write(context.Background(), engineio.Frame{Payload: []byte("4too-long")}); !errors.Is(err, engineio.ErrPayloadTooLarge) {
		t.Fatalf("Write(oversize) error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := polling.Write(canceled, engineio.Frame{Payload: []byte("4x")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write(canceled) error = %v", err)
	}

	source := []byte("4copy")
	if err := polling.Write(context.Background(), engineio.Frame{Payload: source}); err != nil {
		t.Fatal(err)
	}
	source[1] = 'X'
	payload, err := polling.Poll(context.Background())
	if err != nil || string(payload) != "4copy" {
		t.Fatalf("Poll() = %q, %v", payload, err)
	}

	polling.Close()
	if err := polling.Write(context.Background(), engineio.Frame{Payload: []byte("4x")}); !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("Write(closed) error = %v", err)
	}
}

func TestPollingPollCancellationAndInvalidQueuedFrames(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := polling.Poll(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll(canceled) error = %v", err)
	}

	polling.outbound <- engineio.Frame{Payload: []byte("9bad")}
	if _, err := polling.Poll(context.Background()); !errors.Is(err, engineio.ErrInvalidPacketType) {
		t.Fatalf("Poll(invalid first frame) error = %v", err)
	}
	polling.outbound <- engineio.Frame{Payload: []byte("4ok")}
	polling.outbound <- engineio.Frame{Payload: []byte("9bad")}
	if _, err := polling.Poll(context.Background()); !errors.Is(err, engineio.ErrInvalidPacketType) {
		t.Fatalf("Poll(invalid batched frame) error = %v", err)
	}

	polling.Close()
	if _, err := polling.Poll(context.Background()); !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("Poll(closed) error = %v", err)
	}
}

func TestPollingBlockedPollUnblocksOnCancellation(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := newSelectContext()
	result := make(chan error, 1)
	go func() {
		_, err := polling.Poll(ctx)
		result <- err
	}()
	<-ctx.entered
	ctx.cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll() error = %v, want context.Canceled", err)
	}
}

func TestPollingSubmitValidationCancellationAndClosure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		max     int64
		want    error
	}{
		{name: "empty", max: 8, want: engineio.ErrEmptyPayload},
		{name: "malformed", payload: []byte("9bad"), max: 8, want: engineio.ErrInvalidPacketType},
		{name: "base64", payload: []byte("b!"), max: 8, want: engineio.ErrInvalidBase64Packet},
		{name: "oversize", payload: []byte("4toolong"), max: 4, want: engineio.ErrPayloadTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			polling, err := NewPolling(test.max, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := polling.Submit(context.Background(), test.payload); !errors.Is(err, test.want) {
				t.Fatalf("Submit() error = %v, want %v", err, test.want)
			}
			if _, err := polling.Read(context.Background()); !errors.Is(err, ErrPollingClosed) {
				t.Fatalf("Read() after invalid Submit = %v", err)
			}
		})
	}

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := polling.Submit(canceled, []byte("4x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit(canceled) error = %v", err)
	}
	polling.Close()
	if err := polling.Submit(context.Background(), []byte("4x")); !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("Submit(closed) error = %v", err)
	}
}

func TestPollingSubmitCancellationWhileQueueIsFull(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- polling.Submit(ctx, []byte("4first\x1e4second"))
	}()
	deadline := time.Now().Add(time.Second)
	for len(polling.inbound) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("Submit() did not fill inbound queue")
		}
		runtime.Gosched()
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit() error = %v, want context.Canceled", err)
	}
	if _, err := polling.Read(context.Background()); !errors.Is(err, ErrPollingClosed) {
		t.Fatalf("Read() after canceled Submit = %v", err)
	}
}

func TestPollingBinaryPayloadCopies(t *testing.T) {
	t.Parallel()

	polling, err := NewPolling(1024, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{1, 2, 3, 4}
	if err := polling.Submit(context.Background(), []byte("bAQIDBA==")); err != nil {
		t.Fatal(err)
	}
	frame, err := polling.Read(context.Background())
	if err != nil || !frame.Binary || !bytes.Equal(frame.Payload, want) {
		t.Fatalf("Read() = %#v, %v", frame, err)
	}
	if polling.pollActive.Load() {
		t.Fatal("poll unexpectedly active")
	}
}

type selectContext struct {
	done    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func newSelectContext() *selectContext {
	return &selectContext{done: make(chan struct{}), entered: make(chan struct{})}
}

func (ctx *selectContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (ctx *selectContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.done
}

func (ctx *selectContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx *selectContext) Value(any) any { return nil }

func (ctx *selectContext) cancel() { close(ctx.done) }
