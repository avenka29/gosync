package main

import (
	"log"
	"net/http"
	"github.com/avenka29/gosync"
)

func main() {
	server := gosync.NewServer()

	// 1. Start a goroutine to handle the Passive Pipe
	go func() {
		log.Println("Gosync Event Processor started...")
		for ctx := range server.Events() {
			event := ctx.Event
			
			log.Printf("Received Event: %s", event.Name)

			// 2. Logic: If it's a chat message, broadcast it to everyone
			if event.Name == "chat" {
				// We just pass the raw data through
				server.BroadcastEvent("chat", event.Data)
			}
		}
	}()

	http.Handle("/ws", server)

	log.Println("Gosync Server running on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe Error: ", err)
	}
}
