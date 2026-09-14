package gosync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/avenka29/gosync/internal/socketio"
)

type edgeBroker struct {
	receive   func(ClusterMessage)
	published chan ClusterMessage
	nodes     []string
}

func (broker *edgeBroker) Start(_ context.Context, _ string, receive func(ClusterMessage)) error {
	broker.receive = receive
	return nil
}

func (broker *edgeBroker) Publish(_ context.Context, message ClusterMessage) error {
	broker.published <- message
	return nil
}

func (broker *edgeBroker) Nodes(context.Context) ([]string, error) {
	return append([]string(nil), broker.nodes...), nil
}
func (*edgeBroker) Close(context.Context) error { return nil }

func TestClusterRejectsMalformedRequestsWithoutWaiting(t *testing.T) {
	broker := &edgeBroker{published: make(chan ClusterMessage, 4)}
	server, err := NewServer(Config{Broker: broker, MaxRoomsPerSocket: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	requests := []ClusterMessage{
		{Kind: "request", ID: "rooms", Origin: "remote", Namespace: "/", Rooms: []string{"one", "two"}},
		{Kind: "request", ID: "packet", Origin: "remote", Namespace: "/", Header: []byte("invalid")},
	}
	for _, request := range requests {
		broker.receive(request)
		select {
		case reply := <-broker.published:
			if reply.Kind != "reply" || reply.ID != request.ID || reply.Target != request.Origin || reply.Error == "" {
				t.Fatalf("reply = %#v", reply)
			}
		case <-time.After(time.Second):
			t.Fatal("malformed cluster request did not receive an error reply")
		}
	}

	broker.receive(ClusterMessage{Kind: "request", ID: "wrong-target", Origin: "remote", Target: "someone-else"})
	select {
	case reply := <-broker.published:
		t.Fatalf("wrong-target request produced reply %#v", reply)
	default:
	}
}

func TestClusterNodeAndReplyBoundaries(t *testing.T) {
	broker := &edgeBroker{published: make(chan ClusterMessage, 1), nodes: make([]string, 1025)}
	server, err := NewServer(Config{Broker: broker})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.Cleanup(func() {
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := server.Close(shutdownContext); err != nil {
			t.Error(err)
		}
	})
	if _, err := server.Default().To().EmitWithAck(ctx, "event"); !errors.Is(err, ErrAckLimit) {
		t.Fatalf("EmitWithAck() error = %v", err)
	}

	zero := uint64(0)
	encoded, err := server.codec.Encode(socketio.Packet{Type: socketio.PacketAck, ID: &zero, Data: []any{[]byte{1, 2}}})
	if err != nil {
		t.Fatal(err)
	}
	results := make(map[string][]any)
	reply := ClusterMessage{Responses: map[string][][]byte{
		"valid":   append([][]byte{encoded.Header}, encoded.Attachments...),
		"empty":   nil,
		"invalid": {[]byte("invalid")},
	}}
	if err := server.mergeClusterReply(results, reply); err == nil {
		t.Fatal("malformed reply was accepted")
	}
	if binary, ok := results["valid"][0].([]byte); !ok || len(binary) != 2 || binary[0] != 1 || binary[1] != 2 {
		t.Fatalf("valid binary reply = %#v", results["valid"])
	}
}
