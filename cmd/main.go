package main

import (
	"log"
	"net/http"
	"time"
	"math/rand"
	"github.com/avenka29/gosync"
)

func main() {
	server := gosync.NewServer()

	// 1. Handle incoming events from the pipe
	go func() {
		for ctx := range server.Events() {
			event := ctx.Event
			if event.Name == "chat" {
				err := server.BroadcastEvent("chat", event.Data)
				if err != nil {
					log.Printf("Error broadcasting chat: %v", err)
				}
			}
		}
	}()

	// 2. Periodically broadcast a "color-change" event every 5 seconds
	go func() {
		colors := []string{"#ffadad", "#ffd6a5", "#fdffb6", "#caffbf", "#9bf6ff", "#a0c4ff", "#bdb2ff", "#ffc6ff"}
		ticker := time.NewTicker(5 * time.Second)
		
		for range ticker.C {
			randomColor := colors[rand.Intn(len(colors))]
			log.Printf("Broadcasting new color: %s", randomColor)
			
			// We send the "color-change" event to all connected browsers
			err := server.BroadcastEvent("color-change", randomColor)
			if err != nil {
				log.Printf("Error broadcasting color-change: %v", err)
			}
		}
	}()

	// Serve the static index.html file
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	http.Handle("/ws", server)

	log.Println("Gosync Color-Changer Server running on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe Error: ", err)
	}
}
