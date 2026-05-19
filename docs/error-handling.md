# Error Handling Design

## Overview

In `gosync`, error handling is accomlished through a centralized, asynchronous mechanism. Rather than handling complex error logic within every individual goroutine, we delegate error reporting to a dedicated routine. This ensures that the system remains responsive and that error logging, client notifications, and state recovery are handled consistently.

## Centralized Error Hub Routine

Gosync uses a central error manager that will own an event pipe. Similar to `hub.go` for broadcasting and connections. The error manager will own an `Event Pipe` channel, which will contain 
`Error Context` objects. These objects contain the error itself, plus additional context like the`Client` pointer and timestamps. 

### Key Principles:
- **Contextual Awareness:** Every error is wrapped in an `ErrorContext`, providing metadata such as the originating `Client`, a timestamp, and the raw `error`.
- **Single Source of Truth:** A dedicated loop within the Hub (or a specialized Error Supervisor routine) is strictly concerned with monitoring the `EventPipe`. This routine is responsible for:
    - Categorizing errors (e.g., protocol violations vs. network timeouts).
    - Deciding when to gracefully disconnect a client vs. attempting a retry.
    - Providing unified logging and metrics for system health.
- **Encapslation:** We are separating responsibilities. Rather than having the main hub share the error handling, we are creating a separate dedicated goroutine, ensuring cleaner implementation
- **Performance:** The main hub will now be able to better focus on connections and message handling, rather than also having to focus on errors. In the case of a single error prone user, the impact will be isolated to the error handling system, and not bring down the server overall

## Proposed Implementation

### 1. The Error Manager Structure
The `ErrorManager` acts as the supervisor, holding the receive-end of the `ErrorPipe`.

```go
type ErrorManager struct {
    // ErrorPipe receives contexts from throughout the system
    ErrorPipe chan *ErrorContext
    // Reference to hub to perform actions like unregistering clients
    hub *Hub
}
```

### 2. The Error Handling Loop
A dedicated goroutine runs this loop, performing pattern matching on error types.

```go
func (em *ErrorManager) Run() {
}
```

### 3. Reporting an Error
Components like `readPump` or `writePump` simply dispatch to the pipe without waiting. Clients will store pointers to the error pipe channel

```go
if err := json.Unmarshal(message, &event); err != nil {
    em.ErrorPipe <- &ErrorContext{
        Error:  ErrMsgInvalidJSON,
        Client: c,
        Time:   time.Now(),
    }
    return
}

## Design Decision Rationale

### 1. Why a Dedicated Error Manager Component?
Separating error handling into its own component provides several key benefits:
- **Performance & Responsiveness:** The `Hub`'s primary job is high-speed message broadcasting and connection management. Moving error processing to a separate goroutine ensures that a "storm" of client errors cannot block the main broadcast loop.
- **Decoupling:** The `Hub` doesn't need to know *how* an error is handled (e.g., logged to a file, sent to a metrics server, or handled via a retry). It only needs to know that the error has been handed off.
- **Extensibility:** A dedicated component makes it easy to plug in future features like Prometheus metrics, structured logging (Zap/Logrus), or even an external alerting system (Sentry/PagerDuty) without touching the core messaging logic.

### 2. Why Pointer to Channel (`chan<- *ErrorContext`) instead of the `ErrorManager`?
- **Principle of Least Privilege:** A `Client` should only have the ability to *report* an error. Giving it a pointer to the entire `ErrorManager` would allow it to potentially access internal state or methods it has no business calling.
- **Interface Flexibility:** By depending only on a channel, we could theoretically swap the `ErrorManager` for a simple log-wrapper or a mock channel during testing without the `Client` ever knowing the difference.

### 3. Use of `ErrorContext` Wrapper
Instead of sending raw `error` types, we use an `ErrorContext` struct. This is essential because:
- **Traceability:** Errors in a concurrent system are often useless without knowing *which* client triggered them.
- **Metadata:** Including timestamps and potential event IDs allows for better debugging and timeline reconstruction during post-mortem analysis.
```

