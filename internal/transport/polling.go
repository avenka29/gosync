package transport

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/avenka29/gosync/internal/engineio"
)

var (
	ErrInvalidPollingConfig = errors.New("transport: invalid polling configuration")
	ErrPollingClosed        = errors.New("transport: polling session closed")
	ErrConcurrentPoll       = errors.New("transport: concurrent polling GET requests")
	ErrConcurrentPost       = errors.New("transport: concurrent polling POST requests")
	ErrPollingQueueFull     = errors.New("transport: polling output queue is full")
)

// Polling joins HTTP long-poll requests into one Engine.IO transport.
type Polling struct {
	maxPayload int64
	inbound    chan engineio.Frame
	outbound   chan engineio.Frame
	closed     chan struct{}
	closeOnce  sync.Once
	pollClash  chan struct{}
	clashOnce  sync.Once

	pollActive   atomic.Bool
	pollClashed  atomic.Bool
	postActive   atomic.Bool
	upgraded     atomic.Bool
	clientClosed atomic.Bool
	pending      *engineio.Frame
}

func NewPolling(maxPayload int64, inboundBuffer, outboundBuffer int) (*Polling, error) {
	if maxPayload <= 0 || inboundBuffer <= 0 || outboundBuffer <= 0 {
		return nil, ErrInvalidPollingConfig
	}
	return &Polling{
		maxPayload: maxPayload,
		inbound:    make(chan engineio.Frame, inboundBuffer),
		outbound:   make(chan engineio.Frame, outboundBuffer),
		closed:     make(chan struct{}),
		pollClash:  make(chan struct{}),
	}, nil
}

func (polling *Polling) Read(ctx context.Context) (engineio.Frame, error) {
	if err := ctx.Err(); err != nil {
		return engineio.Frame{}, err
	}
	select {
	case <-polling.closed:
		if polling.upgraded.Load() {
			select {
			case frame := <-polling.inbound:
				return cloneFrame(frame), nil
			default:
			}
		}
		return engineio.Frame{}, ErrPollingClosed
	default:
	}

	select {
	case frame := <-polling.inbound:
		return cloneFrame(frame), nil
	case <-polling.closed:
		if polling.upgraded.Load() {
			select {
			case frame := <-polling.inbound:
				return cloneFrame(frame), nil
			default:
			}
		}
		return engineio.Frame{}, ErrPollingClosed
	case <-ctx.Done():
		return engineio.Frame{}, ctx.Err()
	}
}

func (polling *Polling) Write(ctx context.Context, frame engineio.Frame) error {
	if _, err := engineio.EncodePayload([]engineio.Frame{frame}, polling.maxPayload); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-polling.closed:
		return ErrPollingClosed
	default:
	}

	select {
	case polling.outbound <- cloneFrame(frame):
		return nil
	case <-polling.closed:
		return ErrPollingClosed
	default:
		_ = polling.Close()
		return ErrPollingQueueFull
	}
}

// Poll waits for the next payload and rejects overlapping GET requests.
func (polling *Polling) Poll(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !polling.pollActive.CompareAndSwap(false, true) {
		polling.pollClashed.Store(true)
		polling.clashOnce.Do(func() { close(polling.pollClash) })
		return nil, ErrConcurrentPoll
	}
	defer func() {
		polling.pollActive.Store(false)
		if polling.pollClashed.Load() {
			_ = polling.Close()
		}
	}()

	first, err := polling.nextOutbound(ctx)
	if err != nil {
		return nil, err
	}
	if polling.pollClashed.Load() {
		return engineio.EncodePayload([]engineio.Frame{{Payload: []byte("1")}}, polling.maxPayload)
	}
	frames := []engineio.Frame{first}

	for {
		if polling.pollClashed.Load() {
			return engineio.EncodePayload([]engineio.Frame{{Payload: []byte("1")}}, polling.maxPayload)
		}
		select {
		case frame := <-polling.outbound:
			frames = append(frames, frame)
			if _, err := engineio.EncodePayload(frames, polling.maxPayload); err != nil {
				frames = frames[:len(frames)-1]
				if errors.Is(err, engineio.ErrPayloadTooLarge) {
					stored := cloneFrame(frame)
					polling.pending = &stored
					return engineio.EncodePayload(frames, polling.maxPayload)
				}
				return nil, err
			}
		default:
			return engineio.EncodePayload(frames, polling.maxPayload)
		}
	}
}

// Submit accepts one client payload and rejects overlapping POST requests.
func (polling *Polling) Submit(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !polling.postActive.CompareAndSwap(false, true) {
		_ = polling.Close()
		return ErrConcurrentPost
	}
	defer polling.postActive.Store(false)
	return polling.submit(ctx, payload)
}

func (polling *Polling) submit(ctx context.Context, payload []byte) error {
	select {
	case <-polling.closed:
		return ErrPollingClosed
	default:
	}

	frames, err := engineio.DecodePayload(payload, polling.maxPayload)
	if err != nil {
		_ = polling.Close()
		return err
	}
	for _, frame := range frames {
		select {
		case polling.inbound <- cloneFrame(frame):
		case <-polling.closed:
			return ErrPollingClosed
		case <-ctx.Done():
			_ = polling.Close()
			return ctx.Err()
		}
	}
	return nil
}

func (polling *Polling) Close() error {
	polling.closeOnce.Do(func() { close(polling.closed) })
	return nil
}

func (polling *Polling) nextOutbound(ctx context.Context) (engineio.Frame, error) {
	select {
	case <-polling.closed:
		if polling.upgraded.Load() {
			select {
			case frame := <-polling.inbound:
				return cloneFrame(frame), nil
			default:
			}
		}
		return engineio.Frame{}, ErrPollingClosed
	default:
	}
	if polling.pending != nil {
		frame := *polling.pending
		polling.pending = nil
		return cloneFrame(frame), nil
	}

	select {
	case frame := <-polling.outbound:
		return cloneFrame(frame), nil
	case <-polling.pollClash:
		return engineio.Frame{Payload: []byte("1")}, nil
	case <-polling.closed:
		if polling.upgraded.Load() {
			select {
			case frame := <-polling.inbound:
				return cloneFrame(frame), nil
			default:
			}
		}
		return engineio.Frame{}, ErrPollingClosed
	case <-ctx.Done():
		return engineio.Frame{}, ctx.Err()
	}
}

func cloneFrame(frame engineio.Frame) engineio.Frame {
	return engineio.Frame{Payload: append([]byte(nil), frame.Payload...), Binary: frame.Binary}
}

var (
	_ engineio.Transport = (*Polling)(nil)
	_ io.Closer          = (*Polling)(nil)
)

// Detach reserves both request slots and drains queued output for an upgrade.
func (polling *Polling) Detach() ([]engineio.Frame, error) {
	if !polling.pollActive.CompareAndSwap(false, true) {
		return nil, ErrConcurrentPoll
	}
	if !polling.postActive.CompareAndSwap(false, true) {
		polling.pollActive.Store(false)
		return nil, ErrConcurrentPost
	}
	var frames []engineio.Frame
	if polling.pending != nil {
		frames = append(frames, *polling.pending)
		polling.pending = nil
	}
	for {
		select {
		case frame := <-polling.outbound:
			frames = append(frames, frame)
		default:
			return frames, nil
		}
	}
}

// BeginPost guards the entire HTTP body read, including slow uploads.
func (polling *Polling) BeginPost() bool {
	if !polling.postActive.CompareAndSwap(false, true) {
		_ = polling.Close()
		return false
	}
	return true
}

func (polling *Polling) EndPost() {
	polling.postActive.Store(false)
}

func (polling *Polling) SubmitReserved(ctx context.Context, payload []byte) error {
	return polling.submit(ctx, payload)
}

func (polling *Polling) ClientClose() {
	polling.clientClosed.Store(true)
	_ = polling.Close()
}

func (polling *Polling) ClientClosed() bool {
	return polling.clientClosed.Load()
}
