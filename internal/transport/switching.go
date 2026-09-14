package transport

import (
	"context"
	"sync"

	"github.com/avenka29/gosync/internal/engineio"
)

// Switching moves an Engine.IO session from polling to another transport.
type Switching struct {
	mu          sync.RWMutex
	writeMu     sync.Mutex
	polling     *Polling
	current     engineio.Transport
	readCurrent engineio.Transport
	closed      bool
}

func NewSwitching(polling *Polling) *Switching {
	return &Switching{polling: polling, current: polling, readCurrent: polling}
}

func (s *Switching) Read(ctx context.Context) (engineio.Frame, error) {
	for {
		current := s.readCurrent
		frame, err := current.Read(ctx)
		if err == nil {
			return frame, nil
		}
		s.mu.RLock()
		changed := current != s.current && !s.closed
		s.mu.RUnlock()
		if !changed {
			return frame, err
		}
		s.mu.RLock()
		s.readCurrent = s.current
		s.mu.RUnlock()
	}
}

func (s *Switching) Write(ctx context.Context, frame engineio.Frame) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	current, closed := s.current, s.closed
	s.mu.RUnlock()
	if closed {
		return ErrPollingClosed
	}
	return current.Write(ctx, frame)
}

func (s *Switching) Upgrade(ctx context.Context, next engineio.Transport) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.current != s.polling {
		return ErrPollingClosed
	}
	frames, err := s.polling.Detach()
	if err != nil {
		return err
	}
	for _, frame := range frames {
		if err := next.Write(ctx, frame); err != nil {
			return err
		}
	}
	s.current = next
	s.polling.upgraded.Store(true)
	return s.polling.Close()
}

func (s *Switching) Close() error {
	s.mu.Lock()
	s.closed = true
	current := s.current
	s.mu.Unlock()
	_ = s.polling.Close()
	return current.Close()
}

func (s *Switching) ClientClose() {
	s.polling.ClientClose()
}
