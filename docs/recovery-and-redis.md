# Recovery and Redis

Connection recovery and Redis solve different problems. Recovery restores one
client after a short transport interruption. Redis forwards current broadcasts
between server processes.

## Connection recovery

Enable recovery when creating the server:

```go
server, err := gosync.NewServer(gosync.Config{
	Recovery: &gosync.RecoveryConfig{
		MaxDisconnectionDuration: 2 * time.Minute,
		MaxSessions:              1_000,
		MaxPacketsPerSession:     256,
		MaxBytes:                 8 << 20,
	},
})
```

Those values are also the defaults when a `RecoveryConfig` field is zero.

After an unexpected transport close, GoSync retains the socket ID, namespace,
rooms, and eligible missed events. A reconnecting Socket.IO client supplies its
private recovery ID and last received offset. When both values are still valid,
GoSync restores the state before calling `OnConnect`.

```go
namespace.OnConnect(func(ctx context.Context, socket *gosync.Socket) {
	if socket.Recovered() {
		log.Printf("restored %s", socket.ID())
	}
})
```

Events that request acknowledgements are not replayed. Explicit namespace or
server disconnections are not recoverable. Expired sessions, unknown offsets,
and capacity limits produce a fresh socket connection.

Recovery is held in memory inside one process. A load balancer must route a
reconnecting client to the same process. Restarting the process clears the
recovery state.

## Redis broadcasts

The Redis adapter uses Redis Pub/Sub for events and a sorted set for live node
membership.

```go
package main

import (
	"context"
	"log"

	"github.com/avenka29/gosync"
	redisadapter "github.com/avenka29/gosync/adapter/redis"
	"github.com/redis/go-redis/v9"
)

func newServer() (*gosync.Server, *redis.Client) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatal(err)
	}

	broker, err := redisadapter.New(redisClient, "my-service", 0)
	if err != nil {
		log.Fatal(err)
	}
	server, err := gosync.NewServer(gosync.Config{
		Broker: broker,
	})
	if err != nil {
		log.Fatal(err)
	}
	return server, redisClient
}
```

All processes in one cluster must use the same prefix and register the same
namespaces and handlers. Use a different prefix for each application or
environment. A zero message limit uses the adapter default of 2 MiB.

Once configured, normal broadcasts and broadcast acknowledgements include
sockets connected to other GoSync processes:

```go
err := server.Default().To("updates").Emit(ctx, "changed", value)
responses, err := server.Default().To("workers").EmitWithAck(ctx, "health")
```

`Server.Close` stops the broker, but the Redis client belongs to the caller and
must be closed separately.

The broker wire format is specific to GoSync and is not compatible with the
Node.js Redis adapter. It does not persist messages or share recovery state.
Delivery remains at most once. Applications that need durable or exactly-once
processing should store events and use application-level deduplication.
