package client_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/avenka29/gosync"
	"github.com/avenka29/gosync/client"
	"github.com/zishang520/socket.io/clients/engine/v3/transports"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

func TestGoClientPollingAndWebSocket(t *testing.T) {
	for _, websocketOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "polling-upgrade", true: "websocket"}[websocketOnly], func(t *testing.T) {
			server, err := gosync.NewServer(gosync.Config{})
			if err != nil {
				t.Fatal(err)
			}
			namespace, err := server.Of("/custom")
			if err != nil {
				t.Fatal(err)
			}
			if err := namespace.On("echo", func(ctx context.Context, _ *gosync.Socket, args []any, ack gosync.Ack) error {
				return ack(ctx, args...)
			}); err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(server)
			defer httpServer.Close()
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := server.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			var socket *client.Socket
			if websocketOnly {
				managerOptions := client.DefaultManagerOptions()
				managerOptions.SetAutoConnect(false)
				managerOptions.SetForceNew(true)
				managerOptions.SetReconnection(false)
				managerOptions.SetTimeout(3 * time.Second)
				managerOptions.SetTransports(types.NewSet(transports.WebSocket))
				manager := client.NewManager(httpServer.URL, managerOptions)
				socketOptions := client.DefaultSocketOptions()
				socket = manager.Socket("/custom", socketOptions)
			} else {
				options := client.DefaultOptions()
				options.SetAutoConnect(false)
				options.SetForceNew(true)
				options.SetReconnection(false)
				options.SetTimeout(3 * time.Second)
				options.SetTransports(types.NewSet(transports.Polling, transports.WebSocket))
				socket, err = client.Connect(httpServer.URL+"/custom", options)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer socket.Close()
			done := make(chan error, 1)
			socket.On("connect", func(...any) {
				socket.Timeout(time.Second).EmitWithAck("echo", "golang")(func(args []any, err error) {
					if err == nil && (len(args) != 1 || args[0] != "golang") {
						t.Errorf("ack %#v", args)
					}
					done <- err
				})
			})
			socket.On("connect_error", func(args ...any) { t.Logf("connect error: %#v", args) })
			socket.Connect()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Go client timeout")
			}
		})
	}
}
