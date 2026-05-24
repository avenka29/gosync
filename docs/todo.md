# Gosync Roadmap & TODO

This document tracks the progress of `gosync` towards its v1.0.0 release.

## Phase 1: Core Architecture (Completed ✅)
- [x] **Fail-Fast Validation:** Immediate error feedback for invalid inputs.
- [x] **Thin Hub Pattern:** Moved JSON marshalling to the caller thread.
- [x] **Universal Envelope:** High-performance `EventContext` to prevent re-marshalling.
- [x] **Basic Integration Tests:** Verified Hub routing and full-duplex communication.
- [x] **Initial CI/CD:** Basic GitHub Actions for automated testing.

## Phase 2: Production Readiness (High Priority)
- [ ] **Heartbeats (Ping/Pong) with Jitter:**
    - [ ] Implement automated heartbeat loop in `Client`.
    - [ ] Add random Jitter to prevent "Thundering Herd" CPU spikes.
    - [ ] Auto-disconnect silent/ghost clients.
- [ ] **Thundering Herd Protection (Admission Control):**
    - [ ] Implement a connection semaphore to rate-limit WebSocket upgrades.
    - [ ] Add "Backpressure" monitoring to Hub channels.
- [ ] **Rooms / Namespaces:** 
    - [ ] Implement `JoinRoom(client, roomName)` and `LeaveRoom(client, roomName)`.
    - [ ] Implement `BroadcastToRoom(roomName, event)`.
    - [ ] Refactor Hub into **Registry** (State) and **Dispatcher** (Fan-out).
- [ ] **Graceful Shutdown:**
    - [ ] Implement `Server.Close()` to safely terminate all goroutines.
    - [ ] Send WebSocket close frames to all active clients.

## Phase 3: Developer Experience & Metadata
- [ ] **Client Metadata:** 
    - [ ] Allow attaching custom attributes (e.g., `UserID`) to the `Client` struct.
    - [ ] Add `GetClientByID(id)` or similar lookup helpers.
- [ ] **Error Manager Plugins:**
    - [ ] Finalize the `ErrorManager` supervisor logic.
    - [ ] Create a plugin interface for external logging (Prometheus, Postgres, etc.).

## Phase 4: Benchmarking & Resume metrics
- [ ] **Benchmark Suite:**
    - [ ] Fan-out latency test (1 to 10k clients).
    - [ ] Memory footprint analysis (per 1k connections).
    - [ ] "Slow Consumer" stress test.
- [ ] **Public Documentation:**
    - [ ] Comprehensive `README.md` with "Quick Start".
    - [ ] `examples/chat-app` directory.

## Phase 5: v1.0.0 Release
- [ ] Tag `v1.0.0`.
- [ ] Add GoReleaser for automated changelogs.
- [ ] SLSA Provenance for supply-chain security.
