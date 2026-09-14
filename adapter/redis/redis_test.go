package redis_test

import (
	"context"
	"encoding/json"
	"flag"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/avenka29/gosync"
	adapter "github.com/avenka29/gosync/adapter/redis"
	"github.com/gorilla/websocket"
	redis "github.com/redis/go-redis/v9"
)

var redisAddress = flag.String("redis-addr", os.Getenv("GOSYNC_REDIS_ADDR"), "real Redis integration address")

func TestRedisCrossNodeBroadcastAndAcknowledgements(t *testing.T) {
	if *redisAddress == "" {
		t.Skip("set GOSYNC_REDIS_ADDR or -redis-addr to run real Redis tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := redis.NewClient(&redis.Options{Addr: *redisAddress})
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	prefix := "gosync:test:" + time.Now().Format("150405.000000000")
	servers := make([]*gosync.Server, 2)
	peers := make([]*websocket.Conn, 2)
	for i := range servers {
		broker, err := adapter.New(db, prefix, 0)
		if err != nil {
			t.Fatal(err)
		}
		servers[i], err = gosync.NewServer(gosync.Config{Broker: broker, AckTimeout: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		server := servers[i]
		ready := make(chan error, 1)
		server.Default().OnConnect(func(_ context.Context, socket *gosync.Socket) {
			ready <- socket.Join("shared")
		})
		httpServer := httptest.NewServer(server)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := server.Close(ctx); err != nil {
				t.Error(err)
			}
			httpServer.Close()
		})
		peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http")+"/?EIO=4&transport=websocket", nil)
		if err != nil {
			t.Fatal(err)
		}
		peers[i] = peer
		defer peer.Close()
		if err := peer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := peer.ReadMessage(); err != nil {
			t.Fatal(err)
		}
		if err := peer.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
			t.Fatal(err)
		}
		_, _, err = peer.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if err := <-ready; err != nil {
			t.Fatal(err)
		}
	}
	if err := servers[0].Default().To("shared").Emit(ctx, "cluster", "hello"); err != nil {
		t.Fatal(err)
	}
	for _, peer := range peers {
		_, data, err := peer.ReadMessage()
		if err != nil || string(data) != `42["cluster","hello"]` {
			t.Fatalf("broadcast %q %v", data, err)
		}
	}
	done := make(chan error, 1)
	go func() {
		responses, err := servers[0].Default().To("shared").EmitWithAck(ctx, "question")
		if len(responses) != 2 {
			t.Errorf("responses %#v", responses)
		}
		done <- err
	}()
	for _, peer := range peers {
		_, data, err := peer.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		end := strings.IndexByte(string(data), '[')
		if end < 3 {
			t.Fatalf("missing ack id %q", data)
		}
		id := string(data[2:end])
		if err := peer.WriteMessage(websocket.TextMessage, []byte("43"+id+`["answer"]`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := db.Publish(ctx, prefix+":events", "not JSON").Err(); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(gosync.ClusterMessage{Kind: "event", Origin: "test", Namespace: "/", Header: []byte(`2{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Publish(ctx, prefix+":events", payload).Err(); err != nil {
		t.Fatal(err)
	}
	if err := servers[1].Default().To("shared").Emit(ctx, "still-alive"); err != nil {
		t.Fatal(err)
	}
	for _, peer := range peers {
		_, data, err := peer.ReadMessage()
		if err != nil || string(data) != `42["still-alive"]` {
			t.Fatalf("after invalid packet %q %v", data, err)
		}
	}
}

func TestBrokerRejectsInvalidStartup(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	broker, err := adapter.New(client, "gosync:test", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Start(context.Background(), "", func(gosync.ClusterMessage) {}); err == nil {
		t.Fatal("empty node accepted")
	}
	if err := broker.Start(context.Background(), "node", nil); err == nil {
		t.Fatal("nil receiver accepted")
	}
}
