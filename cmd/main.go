package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/avenka29/gosync"
)

func main() {
	server, err := gosync.NewServer(gosync.Config{})
	if err != nil {
		log.Fatal(err)
	}

	if err := server.Default().On("echo", func(ctx context.Context, socket *gosync.Socket, args []any, ack gosync.Ack) error {
		if ack != nil {
			return ack(ctx, args...)
		}
		return socket.Emit(ctx, "echo", args...)
	}); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)
	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Close(shutdownContext); err != nil {
		log.Printf("Socket.IO shutdown: %v", err)
	}
	if err := httpServer.Shutdown(shutdownContext); err != nil {
		log.Printf("HTTP shutdown: %v", err)
	}
}
