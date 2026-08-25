# Error Handling Design

## Overview

In `gosync`, error handling follows a **Two-Tier Architecture** designed to balance high-performance broadcasting with reliable message tracking.

### The Golden Rule
*   **Immediate Errors:** Occur on the **caller's thread** before the message is queued. They catch **Logic/Input bugs**.
*   **Deferred Errors:** Occur on a **background thread** after the message is queued. They catch **Network/State bugs**.

---

## The Two-Tier Paradigm

### 1. Immediate Errors (Synchronous)
**When:** During the initial library method call (e.g., `BroadcastEvent`).
**Handling:** Returned as standard Go `error` values.
**Scenarios:**
- Malformed JSON/marshalling failures.
- Empty event names or invalid parameters.
- System-level state issues (e.g., Server is closed).

```go
err := server.BroadcastEvent("chat", data)
if err != nil { /* Fix code logic */ }
```

### 2. Deferred Errors (Asynchronous)
**When:** After the message is accepted, during background processing or network I/O.
**Handling:** Pushed into the **Error Pipe**. Context **must** include the original `Event`.
**Scenarios:**
- Browser sends invalid protocol JSON (caught in `readMessage`).
- Client connection drops mid-transmission (caught in `writeMessage`).
- Network timeouts or buffer overflows.

```go
for errCtx := range server.Errors() {
    // Handle network reality: retry or log failure
}
```

**Error Manager:** The error manager will handle deffered errors. It will own two error channels: internal and external. The internal channel is used as a landing page for all deffered errors acorss the system. The external channel acts as the public api for users to access the error messages. 

This design allows library side self healing mechanisms, while still providing metrics and logging. For example, during a thundering herd scenario, where 10,000 clients are disconnected simultaneously, the internal pipe will take these raw requests, act as a buffer, and can expose a couple errors/logs to the public channel, rather than just overwhelming the public channel. 

---

## Error Handling Matrix

| Situation | Responsibility | Error Type | Action |
| :--- | :--- | :--- | :--- |
| Invalid API usage (Bad JSON) | Developer | **Immediate** | Return `error` |
| Client sends garbage data | Client | **Deferred** | Send to `ErrorPipe` |
| Connection reset by peer | Network | **Deferred** | Send to `ErrorPipe` |

---

## Design Rationale

1.  **Server Integrity:** Background errors (network/client) must never crash the server.
2.  **Developer Feedback:** Immediate errors provide instant feedback for code-level bugs.
3.  **Traceability:** The `ErrorPipe` provides the `FailedEvent`, turning "noise" into actionable data for retries and recovery.
