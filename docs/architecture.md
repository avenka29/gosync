# Gosync Architecture

This diagram illustrates how a message flows from one browser through the Go backend to all other connected clients.

```mermaid
sequenceDiagram
    participant B1 as Browser 1
    participant C1 as Client 1 (Go Struct)
    participant H as Hub (Central Brain)
    participant C2 as Client 2 (Go Struct)
    participant B2 as Browser 2

    Note over B1, B2: Connection Phase
    B1->>+H: HTTP Upgrade to WebSocket
    H->>C1: Create Client 1 & Start Pumps
    H->>H: Register Client 1 in Map

    Note over B1, B2: Message Broadcast Flow
    B1->>C1: Send Message (Raw WS)
    C1->>C1: readMessage() goroutine
    C1->>H: Message -> hub.broadcast (Channel)
    
    H->>H: Loop through all clients in map
    
    H->>C2: Message -> client2.send (Channel)
    C2->>C2: writeMessage() goroutine
    C2->>B2: Send Message (Raw WS)

    Note over B1, B2: Disconnect Phase
    B1--xH: Connection Closed
    C1->>H: hub.unregister (Channel)
    H->>H: Delete Client 1 from Map
```

## Component Breakdown

1.  **Server (`server.go`)**: The entry point. It upgrades standard HTTP requests into WebSocket connections and initializes a `Client`.
2.  **Hub (`hub.go`)**: The central dispatcher. It runs in its own goroutine and manages the state of all active connections using channels.
3.  **Client (`client.go`)**: The bridge. Every connection has its own `Client` struct with two dedicated goroutines:
    *   `readMessage`: Pulls data from the network and pushes it to the Hub.
    *   `writeMessage`: Pulls data from the Hub and pushes it to the network.
4.  **Channels**: The glue. We use Go channels (`register`, `unregister`, `broadcast`, and `send`) to ensure thread-safe communication between components without complex locking.
