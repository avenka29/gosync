// conformance runs the fixture expected by the official protocol test suites.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/avenka29/gosync"
	"github.com/avenka29/gosync/internal/engineio"
	engineserver "github.com/avenka29/gosync/internal/server"
)

func main() {
	engineOnly := flag.Bool("engine", false, "Engine.IO echo fixture")
	addr := flag.String("addr", "127.0.0.1:3000", "listen address")
	flag.Parse()
	var handler http.Handler
	var closeServer func(context.Context) error
	if *engineOnly {
		server := engineserver.NewServer(engineserver.Config{
			PingInterval: 300 * time.Millisecond,
			PingTimeout:  200 * time.Millisecond,
			CheckOrigin: func(*http.Request) bool {
				return true
			},
		}, func(send engineserver.Sender) engineio.MessageHandler {
			return func(ctx context.Context, packet engineio.Packet) error { return send(ctx, packet) }
		})
		handler = server
		closeServer = server.Close
	} else {
		server, err := gosync.NewServer(gosync.Config{
			PingInterval: 300 * time.Millisecond,
			PingTimeout:  200 * time.Millisecond,
			CheckOrigin: func(*http.Request) bool {
				return true
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		for _, name := range []string{"/", "/custom"} {
			namespace, err := server.Of(name)
			if err != nil {
				log.Fatal(err)
			}
			namespace.OnConnect(func(ctx context.Context, socket *gosync.Socket) {
				listenerReadyDelay := time.NewTimer(time.Millisecond)
				defer listenerReadyDelay.Stop()
				select {
				case <-ctx.Done():
					return
				case <-listenerReadyDelay.C:
				}
				auth := socket.Auth()
				if auth == nil {
					auth = map[string]any{}
				}
				if err := socket.Emit(ctx, "auth", auth); err != nil && ctx.Err() == nil {
					log.Printf("send auth: %v", err)
				}
			})
			if err := namespace.On("message", func(ctx context.Context, socket *gosync.Socket, args []any, _ gosync.Ack) error {
				return socket.Emit(ctx, "message-back", args...)
			}); err != nil {
				log.Fatal(err)
			}
			if err := namespace.On("message-with-ack", func(ctx context.Context, _ *gosync.Socket, args []any, ack gosync.Ack) error {
				if ack != nil {
					return ack(ctx, args...)
				}
				return nil
			}); err != nil {
				log.Fatal(err)
			}
		}
		handler = server
		closeServer = server.Close
	}
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		log.Printf("fixture listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := closeServer(shutdown); err != nil {
		log.Printf("fixture shutdown: %v", err)
	}
	if err := httpServer.Shutdown(shutdown); err != nil {
		log.Printf("HTTP shutdown: %v", err)
	}
}
