package client_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
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
				managerOptions.SetTimeout(10 * time.Second)
				managerOptions.SetTransports(types.NewSet(transports.WebSocket))
				manager := client.NewManager(httpServer.URL, managerOptions)
				socketOptions := client.DefaultSocketOptions()
				socket = manager.Socket("/custom", socketOptions)
			} else {
				options := client.DefaultOptions()
				options.SetAutoConnect(false)
				options.SetForceNew(true)
				options.SetReconnection(false)
				options.SetTimeout(10 * time.Second)
				options.SetTransports(types.NewSet(transports.Polling, transports.WebSocket))
				socket, err = client.Connect(httpServer.URL+"/custom", options)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer socket.Close()
			done := make(chan error, 1)
			var doneOnce sync.Once
			finish := func(err error) {
				doneOnce.Do(func() { done <- err })
			}
			socket.On("connect", func(...any) {
				socket.Timeout(5*time.Second).EmitWithAck("echo", "golang")(func(args []any, err error) {
					if err != nil {
						finish(err)
						return
					}
					if len(args) != 1 || args[0] != "golang" {
						finish(fmt.Errorf("unexpected acknowledgement: %#v", args))
						return
					}
					finish(nil)
				})
			})
			socket.On("connect_error", func(args ...any) {
				finish(fmt.Errorf("connect error: %v", args))
			})
			socket.Connect()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("Go client timeout")
			}
		})
	}
}
