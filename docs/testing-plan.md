# Gosync Testing Plan

This document outlines the strategy for verifying the `gosync` library. Since deferred errors are not yet implemented, this phase focuses on **API Correctness**, **Routing Integrity**, and **High-Performance Delivery**.

## 1. Unit Testing (Synchronous Logic)
Focus on the "Fail-Fast" mechanisms in the `Server`.

### 1.1 Broadcast Validation
- **Test:** `BroadcastEvent` with empty name.
  - **Expected:** Returns `ErrServerMsgInvalid`.
- **Test:** `BroadcastEvent` with `nil` data.
  - **Expected:** Returns `ErrServerMsgInvalid`.
- **Test:** `BroadcastEvent` with non-serializable data (e.g., a function).
  - **Expected:** Returns a JSON marshalling error.

### 1.2 Event Context Creation
- **Test:** `createEventContext` internal method.
  - **Expected:** The `Raw` byte slice must be a valid JSON representation of the `Event` struct, not just the data payload.
  - **Expected:** `Client` field must be `nil` for broadcasts.

## 2. Integration Testing (Asynchronous Flow)
Focus on the interaction between `Server`, `Hub`, and `Client`.

### 2.1 The "Happy Path" Broadcast
1. Start a `NewServer`.
2. Mock multiple websocket connections.
3. Call `server.BroadcastEvent("test", "hello")`.
4. **Verification:** Ensure every mock client receives the exact same bytes on their wire.

### 2.2 Inbound Event Pipe
1. Start a `NewServer`.
2. Have a mock client send a JSON event: `{"name": "ping", "data": "pong"}`.
3. **Verification:** Read from `server.Events()`. Ensure the `EventContext` contains the correct `Event` struct and a pointer to the correct `Client`.

### 2.3 Concurrent Broadcasts
- **Test:** 100 goroutines calling `BroadcastEvent` simultaneously.
  - **Verification:** Ensure no race conditions occur and that all messages are eventually delivered to a test client.

## 3. Performance & Stress Testing
Verify the "Thin Hub" design benefits.

### 3.1 Serial vs. Parallel Marshalling
- **Metric:** Time taken for the `Server` to return from `BroadcastEvent` vs. time taken for the `Hub` to finish the loop.
- **Verification:** `BroadcastEvent` should return nearly instantly even if the Hub is busy, as the work is done on the caller's thread.

## 4. Connection Lifecycle
- **Test:** Rapid Register/Unregister.
  - **Expected:** The `Hub.clients` map should accurately reflect the number of connected clients without leaking memory or causing panics.

## 5. Excluded for Now
- **Deferred Error Propagation:** Testing if a connection drop mid-write correctly reaches the `ErrorPipe` will be added once the `ErrorManager` is fully integrated.
