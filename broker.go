package gosync

import (
	"context"
	"errors"
	"sync"

	"github.com/avenka29/gosync/internal/socketio"
)

// Broker connects GoSync servers using the GoSync cluster protocol.
type Broker interface {
	Start(context.Context, string, func(ClusterMessage)) error
	Publish(context.Context, ClusterMessage) error
	Nodes(context.Context) ([]string, error)
	Close(context.Context) error
}

// ClusterMessage carries one encoded cluster event, request, or reply.
type ClusterMessage struct {
	Kind        string
	ID          string
	Origin      string
	Target      string
	Namespace   string
	Sender      string
	Rooms       []string
	Except      []string
	Header      []byte
	Attachments [][]byte
	Responses   map[string][][]byte
	Error       string
}
type clusterWait struct {
	mu      sync.Mutex
	nodes   map[string]bool
	replies chan ClusterMessage
}

func (s *Server) receiveCluster(message ClusterMessage) {
	if message.Origin == s.nodeID || message.Target != "" && message.Target != s.nodeID {
		return
	}
	switch message.Kind {
	case "reply":
		s.receiveClusterReply(message)
	case "event", "request":
		s.receiveClusterEvent(message)
	}
}

func (s *Server) receiveClusterReply(message ClusterMessage) {
	s.clusterMu.Lock()
	waiting := s.clusterPending[message.ID]
	s.clusterMu.Unlock()
	if waiting == nil {
		return
	}
	waiting.mu.Lock()
	expected := waiting.nodes[message.Origin]
	delete(waiting.nodes, message.Origin)
	waiting.mu.Unlock()
	if !expected {
		return
	}
	select {
	case waiting.replies <- message:
	default:
	}
}

func (s *Server) receiveClusterEvent(message ClusterMessage) {
	s.mu.RLock()
	namespace := s.namespaces[message.Namespace]
	s.mu.RUnlock()
	if namespace == nil {
		if message.Kind == "request" {
			s.replyCluster(message, map[string][][]byte{}, nil)
		}
		return
	}
	if len(message.Rooms) > s.config.MaxRoomsPerSocket || len(message.Except) > s.config.MaxRoomsPerSocket {
		s.rejectClusterRequest(message)
		return
	}
	size := int64(len(message.Header))
	for _, b := range message.Attachments {
		size += int64(len(b))
	}
	if size > s.config.MaxPayload {
		s.rejectClusterRequest(message)
		return
	}
	packet, err := s.codec.DecodeHeader(message.Header)
	if err == nil && packet.Type.Binary() {
		packet, err = s.codec.Reconstruct(packet, message.Attachments)
	}
	if err != nil || packet.Namespace != namespace.name || (packet.Type != socketio.PacketEvent && packet.Type != socketio.PacketBinaryEvent) {
		s.rejectClusterRequest(message)
		return
	}
	data := packet.Data.([]any)
	event := data[0].(string)
	if !validEvent(event) {
		s.rejectClusterRequest(message)
		return
	}
	broadcast := &Broadcast{namespace: namespace, rooms: message.Rooms, except: message.Except, sender: message.Sender, local: true}
	if message.Kind == "event" {
		s.report(broadcast.Emit(s.ctx, event, data[1:]...))
		return
	}
	select {
	case s.clusterSlots <- struct{}{}:
	default:
		s.replyCluster(message, nil, ErrQueueFull)
		return
	}
	s.clusterWorkers.Add(1)
	go func() {
		defer s.clusterWorkers.Done()
		defer func() {
			<-s.clusterSlots
		}()
		responses, err := broadcast.EmitWithAck(s.ctx, event, data[1:]...)
		encoded := make(map[string][][]byte)
		zero := uint64(0)
		for id, args := range responses {
			packet, encodeErr := s.codec.Encode(socketio.Packet{Type: socketio.PacketAck, ID: &zero, Data: args})
			if encodeErr != nil {
				err = errors.Join(err, encodeErr)
				continue
			}
			encoded[id] = append([][]byte{packet.Header}, packet.Attachments...)
		}
		s.replyCluster(message, encoded, err)
	}()
}

func (s *Server) rejectClusterRequest(message ClusterMessage) {
	if message.Kind == "request" {
		s.replyCluster(message, nil, ErrProtocol)
	}
}

func (s *Server) replyCluster(request ClusterMessage, responses map[string][][]byte, err error) {
	message := ClusterMessage{Kind: "reply", ID: request.ID, Origin: s.nodeID, Target: request.Origin, Responses: responses}
	if err != nil {
		message.Error = err.Error()
	}
	s.report(s.config.Broker.Publish(s.ctx, message))
}

func (b *Broadcast) clusterMessage(event string, args []any) (ClusterMessage, error) {
	packet, err := b.namespace.server.codec.Encode(newEventPacket(b.namespace.name, event, args, nil))
	if err != nil {
		return ClusterMessage{}, err
	}
	size := int64(len(packet.Header))
	for _, attachment := range packet.Attachments {
		size += int64(len(attachment))
	}
	if size > b.namespace.server.config.MaxPayload {
		return ClusterMessage{}, errors.New("gosync: cluster payload too large")
	}
	return ClusterMessage{
		Kind:        "event",
		Origin:      b.namespace.server.nodeID,
		Namespace:   b.namespace.name,
		Sender:      b.sender,
		Rooms:       b.rooms,
		Except:      b.except,
		Header:      packet.Header,
		Attachments: packet.Attachments,
	}, nil
}

func (b *Broadcast) clusterAck(ctx context.Context, event string, args []any) (map[string][]any, error) {
	server := b.namespace.server
	ctx, cancel := context.WithTimeout(ctx, server.config.AckTimeout)
	defer cancel()
	nodes, err := server.config.Broker.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(nodes) > 1024 {
		return nil, ErrAckLimit
	}
	waiting := &clusterWait{nodes: make(map[string]bool), replies: make(chan ClusterMessage, len(nodes))}
	for _, node := range nodes {
		if node != server.nodeID {
			waiting.nodes[node] = true
		}
	}
	remaining := len(waiting.nodes)
	message, err := b.clusterMessage(event, args)
	if err != nil {
		return nil, err
	}
	message.Kind = "request"
	message.ID, err = newID()
	if err != nil {
		return nil, err
	}
	server.clusterMu.Lock()
	if len(server.clusterPending) >= server.config.MaxPendingAcks {
		server.clusterMu.Unlock()
		return nil, ErrAckLimit
	}
	server.clusterPending[message.ID] = waiting
	server.clusterMu.Unlock()
	defer func() {
		server.clusterMu.Lock()
		delete(server.clusterPending, message.ID)
		server.clusterMu.Unlock()
	}()
	if remaining > 0 {
		if err := server.config.Broker.Publish(ctx, message); err != nil {
			return nil, err
		}
	}
	local := *b
	local.local = true
	results, localErr := local.EmitWithAck(ctx, event, args...)
	for range remaining {
		select {
		case reply := <-waiting.replies:
			localErr = errors.Join(localErr, server.mergeClusterReply(results, reply))
		case <-ctx.Done():
			return results, errors.Join(localErr, ctx.Err())
		case <-server.ctx.Done():
			return results, ErrServerClosed
		}
	}
	return results, localErr
}

func (s *Server) mergeClusterReply(results map[string][]any, reply ClusterMessage) error {
	var errs []error
	if reply.Error != "" {
		errs = append(errs, errors.New(reply.Error))
	}
	for id, frames := range reply.Responses {
		if len(frames) == 0 {
			continue
		}
		packet, err := s.codec.DecodeHeader(frames[0])
		if err == nil && packet.Type.Binary() {
			packet, err = s.codec.Reconstruct(packet, frames[1:])
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if packet.Type == socketio.PacketAck || packet.Type == socketio.PacketBinaryAck {
			results[id] = packet.Data.([]any)
		}
	}
	return errors.Join(errs...)
}
