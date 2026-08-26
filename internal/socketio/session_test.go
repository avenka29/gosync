package socketio

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/avenka29/gosync/internal/engineio"
)

func TestSessionReceivesTextAndBinaryPackets(t *testing.T) {
	t.Parallel()

	received := make(chan Packet, 2)
	session, err := NewSession(NewCodec(0, 0), func(context.Context, engineio.Packet) error {
		return nil
	}, func(_ context.Context, packet Packet) error {
		received <- packet
		return nil
	})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	if err := session.Handle(context.Background(), engineio.Packet{
		Type: engineio.PacketMessage,
		Data: []byte(`2["message","hello"]`),
	}); err != nil {
		t.Fatalf("Handle(text) error = %v", err)
	}
	textPacket := <-received
	if textPacket.Type != PacketEvent || textPacket.Data.([]any)[1] != "hello" {
		t.Fatalf("text packet = %#v", textPacket)
	}

	if err := session.Handle(context.Background(), engineio.Packet{
		Type: engineio.PacketMessage,
		Data: []byte(`51-["file",{"_placeholder":true,"num":0}]`),
	}); err != nil {
		t.Fatalf("Handle(binary header) error = %v", err)
	}
	if err := session.Handle(context.Background(), engineio.Packet{
		Type:   engineio.PacketMessage,
		Data:   []byte{0x01, 0x02},
		Binary: true,
	}); err != nil {
		t.Fatalf("Handle(binary attachment) error = %v", err)
	}
	binaryPacket := <-received
	if binaryPacket.Type != PacketBinaryEvent || !bytes.Equal(binaryPacket.Data.([]any)[1].([]byte), []byte{0x01, 0x02}) {
		t.Fatalf("binary packet = %#v", binaryPacket)
	}
}

func TestSessionRejectsInvalidOrdering(t *testing.T) {
	t.Parallel()

	session, err := NewSession(NewCodec(0, 0), func(context.Context, engineio.Packet) error { return nil }, nil)
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	if err := session.Handle(context.Background(), engineio.Packet{
		Type:   engineio.PacketMessage,
		Binary: true,
		Data:   []byte{0x01},
	}); !errors.Is(err, ErrUnexpectedBinary) {
		t.Fatalf("Handle(unexpected binary) error = %v, want ErrUnexpectedBinary", err)
	}

	if err := session.Handle(context.Background(), engineio.Packet{
		Type: engineio.PacketMessage,
		Data: []byte(`51-["file",{"_placeholder":true,"num":0}]`),
	}); err != nil {
		t.Fatalf("Handle(binary header) error = %v", err)
	}
	if err := session.Handle(context.Background(), engineio.Packet{
		Type: engineio.PacketMessage,
		Data: []byte(`2["interleaved"]`),
	}); !errors.Is(err, ErrInterleavedPacket) {
		t.Fatalf("Handle(interleaved) error = %v, want ErrInterleavedPacket", err)
	}
}

func TestSessionSendsBinaryPacketWithoutInterleaving(t *testing.T) {
	t.Parallel()

	var mutex sync.Mutex
	var sent []engineio.Packet
	session, err := NewSession(NewCodec(0, 0), func(_ context.Context, packet engineio.Packet) error {
		mutex.Lock()
		sent = append(sent, packet)
		mutex.Unlock()
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	packets := []Packet{
		{Type: PacketEvent, Data: []any{"first", []byte{0x01}}},
		{Type: PacketEvent, Data: []any{"second", []byte{0x02}}},
	}
	var waitGroup sync.WaitGroup
	for _, packet := range packets {
		packet := packet
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := session.Send(context.Background(), packet); err != nil {
				t.Errorf("Send() error = %v", err)
			}
		}()
	}
	waitGroup.Wait()

	mutex.Lock()
	defer mutex.Unlock()
	if len(sent) != 4 {
		t.Fatalf("sent %d Engine.IO packets, want 4", len(sent))
	}
	for index := 0; index < len(sent); index += 2 {
		if sent[index].Binary || !sent[index+1].Binary {
			t.Fatalf("packet sequence at %d is interleaved: %#v %#v", index, sent[index], sent[index+1])
		}
	}
}
