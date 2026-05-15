package main

import (
	"log"
	"net/http"

	"github.com/avenka29/gosync"
)

func main() {
	// 1. Initialize our Gosync server (this also starts the Hub goroutine)
	server := gosync.NewServer()

	// 2. Register the /ws route to our server
	http.Handle("/ws", server)

	// 3. Start the HTTP server
	log.Println("Gosync MVP server started on :8080")
	log.Println("Connect to ws://localhost:8080/ws to start broadcasting")
	
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe Error: ", err)
	}
}
