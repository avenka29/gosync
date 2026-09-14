// Package redis connects GoSync servers through bounded Redis pub/sub messages.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/avenka29/gosync"
	redis "github.com/redis/go-redis/v9"
)

type Broker struct {
	client   redis.UniversalClient
	prefix   string
	maxBytes int
	mu       sync.Mutex
	node     string
	cancel   context.CancelFunc
	done     chan struct{}
}

// New uses a caller-owned Redis client, which is not closed by Broker.Close.
func New(client redis.UniversalClient, prefix string, maxMessageBytes int) (*Broker, error) {
	if client == nil || prefix == "" || len(prefix) > 128 || maxMessageBytes < 0 {
		return nil, errors.New("gosync redis: invalid configuration")
	}
	if maxMessageBytes == 0 {
		maxMessageBytes = 2 << 20
	}
	return &Broker{client: client, prefix: prefix, maxBytes: maxMessageBytes}, nil
}

func (b *Broker) Start(parent context.Context, node string, receive func(gosync.ClusterMessage)) error {
	if node == "" || receive == nil {
		return errors.New("gosync redis: node and receiver are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done != nil {
		return errors.New("gosync redis: already started")
	}
	ctx, cancel := context.WithCancel(parent)
	sub := b.client.Subscribe(ctx, b.prefix+":events")
	if _, err := sub.Receive(ctx); err != nil {
		cancel()
		_ = sub.Close()
		return err
	}
	if err := b.heartbeat(ctx, node); err != nil {
		cancel()
		_ = sub.Close()
		return err
	}
	b.node = node
	b.cancel = cancel
	b.done = make(chan struct{})
	go func() {
		defer close(b.done)
		defer sub.Close()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		messages := sub.Channel(redis.WithChannelSize(128))
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = b.heartbeat(ctx, node)
			case redisMessage, ok := <-messages:
				if !ok {
					return
				}
				if len(redisMessage.Payload) > b.maxBytes {
					continue
				}
				var message gosync.ClusterMessage
				if json.Unmarshal([]byte(redisMessage.Payload), &message) == nil {
					receive(message)
				}
			}
		}
	}()
	return nil
}

func (b *Broker) heartbeat(ctx context.Context, node string) error {
	return b.client.ZAdd(ctx, b.prefix+":nodes", redis.Z{Score: float64(time.Now().Unix()), Member: node}).Err()
}

func (b *Broker) Publish(ctx context.Context, message gosync.ClusterMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if len(data) > b.maxBytes {
		return errors.New("gosync redis: message too large")
	}
	return b.client.Publish(ctx, b.prefix+":events", data).Err()
}

func (b *Broker) Nodes(ctx context.Context) ([]string, error) {
	cutoff := strconv.FormatInt(time.Now().Add(-20*time.Second).Unix(), 10)
	if err := b.client.ZRemRangeByScore(ctx, b.prefix+":nodes", "-inf", cutoff).Err(); err != nil {
		return nil, err
	}
	return b.client.ZRangeByScore(ctx, b.prefix+":nodes", &redis.ZRangeBy{Min: "(" + cutoff, Max: "+inf"}).Result()
}

func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.cancel == nil {
		b.mu.Unlock()
		return nil
	}
	b.cancel()
	done, node := b.done, b.node
	b.mu.Unlock()
	select {
	case <-done:
		return b.client.ZRem(ctx, b.prefix+":nodes", node).Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
