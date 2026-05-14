# socket.io-cluster

[简体中文](./README.CN.md)

`socket.io-cluster` is a Go Socket.IO server library for building Socket.IO v4 / Engine.IO v4 applications that can run as a single node or as a built-in multi-node cluster.

It focuses on the server-side developer experience: mount it as an `http.Handler`, register event handlers, use rooms and ACKs, and enable cross-node delivery without adding Redis, NATS, or another external message bus.

## Features

- Socket.IO v4 / Engine.IO v4 server support.
- WebSocket and polling transports, including WebSocket upgrade.
- Namespaces, rooms, broadcast, `except`, local-only broadcast, ACK callbacks, binary events, and binary ACKs.
- Built-in peer-to-peer cluster fanout on the same Socket.IO path.
- Static peer discovery and Kubernetes headless DNS discovery without Kubernetes API permissions.
- Connection State Recovery (CSR) with same-node restore and best-effort cross-node recovery pull.
- Prometheus-friendly metrics through a dependency-free `Server.Metrics()` snapshot API.

## Installation

```bash
go get github.com/faceair/socket.io-cluster
```

## Quick start

`ServerConfig.Port` must match the real HTTP listening port. The library does not bind the port for you; it uses this value to build the address other cluster nodes should call.

```go
package main

import (
    "log"
    "net/http"

    sio "github.com/faceair/socket.io-cluster"
)

func main() {
    server, err := sio.NewServer(&sio.ServerConfig{
        Port:               "3000",
        AcceptAnyNamespace: true,
        OnError:            func(err error) { log.Println(err) },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer func() { _ = server.Close() }()

    server.OnConnection(func(socket sio.ServerSocket) {
        log.Println("connected", socket.ID())

        socket.OnEvent("hello", func(name string, ack func(string)) {
            ack("hello " + name)
        })
    })

    http.Handle("/socket.io/", server)
    log.Fatal(http.ListenAndServe(":3000", nil))
}
```

A Socket.IO JavaScript client can connect normally:

```js
import { io } from "socket.io-client";

const socket = io("http://localhost:3000", { transports: ["websocket"] });

socket.emit("hello", "alice", (reply) => {
  console.log(reply); // "hello alice"
});
```

## Configuration at a glance

### Required

You must provide one of the following:

- `ServerConfig.Port`, for example `"3000"` when your process listens on `:3000`.
- A full `Cluster.AdvertiseURL`, for example `"http://10.0.0.9:3000"`.
- An environment-provided port: `SIO_CLUSTER_PORT`, `SOCKETIO_CLUSTER_PORT`, `SOCKETIO_PORT`, `PORT`, `HTTP_PORT`, or Kubernetes `<SERVICE>_SERVICE_PORT`.

If neither a port nor a full advertise URL is available, `NewServer` returns an error.

### Optional

| Option | Purpose | Default |
| --- | --- | --- |
| `Path` | Socket.IO endpoint path | `/socket.io/` |
| `Cluster.NodeID` | Stable node identity | `POD_NAME`, `HOSTNAME`, `os.Hostname()`, then random |
| `Cluster.AdvertiseURL` | Full URL peers use to call this node | Built from host + port |
| `Cluster.Peers` | Static peer endpoints | `SIO_CLUSTER_PEERS`, `SOCKETIO_CLUSTER_PEERS` |
| `Cluster.HeadlessDNS` | DNS names to resolve peer IPs from | `SIO_CLUSTER_HEADLESS_DNS`, `SOCKETIO_CLUSTER_HEADLESS_DNS`, service env, or inferred from `POD_NAME` |
| `Cluster.RequestTimeout` | Peer request timeout | `2s` |
| `Cluster.HeartbeatInterval` | DNS refresh interval | `30s` |
| `Cluster.FanoutWorkers` | Cross-node fanout workers | `8` |

### Port vs. AdvertiseURL

`Port` is the simplest option for most deployments:

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
```

`AdvertiseURL` is useful when peers must call a different host or scheme than the local listener:

```go
server, err := sio.NewServer(&sio.ServerConfig{
    Cluster: sio.ClusterConfig{
        AdvertiseURL: "https://socket-0.example.internal:443",
    },
})
```

When `AdvertiseURL` is set, it must include scheme, host, and port.

## Events, ACKs, and binary payloads

Handlers are regular Go functions. The last function argument is treated as the ACK callback.

```go
server.OnConnection(func(socket sio.ServerSocket) {
    socket.OnEvent("profile:update", func(userID string, attrs map[string]any, ack func(string)) {
        // Save user profile here.
        ack("ok")
    })

    socket.OnEvent("file:upload", func(name string, data []byte, ack func(sio.Binary)) {
        // Echo the uploaded bytes back as a binary ACK.
        ack(sio.Binary(data))
    })

    socket.Emit("server:ready", map[string]any{"id": socket.ID()})
})
```

Use `Timeout` when you expect an ACK from the client:

```go
socket.Timeout(5 * time.Second).Emit("question", "continue?", func(err error, answer string) {
    if err != nil {
        log.Println("client did not ACK in time", err)
        return
    }
    log.Println("client answered", answer)
})
```

## Rooms and broadcast

Every socket can join rooms. Broadcast APIs work locally and across configured peers by default.

```go
server.OnConnection(func(socket sio.ServerSocket) {
    socket.OnEvent("room:join", func(room string) {
        socket.Join(sio.Room(room))
    })
})

server.To("room-a").Emit("news", "hello room-a")
server.Except("muted").Emit("news", "everyone except muted")
server.Local().Emit("maintenance", "only this node")
```

You can query sockets and mutate rooms across the cluster:

```go
sockets := server.To("room-a").FetchSockets()
for _, socket := range sockets {
    log.Println(socket.ID(), socket.Rooms())
}

server.To("vip").SocketsJoin("priority")
server.To("leaving").DisconnectSockets(false)
```

## Cluster setup

Cluster transport is enabled by default and always mounted at the normal Socket.IO path with `transport=cluster`. Minimal setup is just the server port:

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
```

No `ClusterConfig` is required for single-node use. To talk to other nodes, add one discovery source: static peers, `SIO_CLUSTER_PEERS`, `Cluster.HeadlessDNS`, `SIO_CLUSTER_HEADLESS_DNS`, or a Kubernetes service name. If no peers or DNS discovery are configured, the server behaves as a single node and simply has no remote targets.

### Static peers

```go
server, err := sio.NewServer(&sio.ServerConfig{
    Port: "3000",
    Cluster: sio.ClusterConfig{
        NodeID: "socket-a",
        Peers: []string{
            "http://10.0.0.11:3000/socket.io/?transport=cluster",
            "10.0.0.12:3000", // normalized automatically
        },
    },
})
```

The peer list can also come from `SIO_CLUSTER_PEERS` or `SOCKETIO_CLUSTER_PEERS` as a comma-separated list.

### Kubernetes headless DNS

For Kubernetes headless DNS discovery, provide pod identity. If your headless service name matches the workload name, the library can infer it from `POD_NAME`:

- StatefulSet pod `socketio-0` infers service `socketio`.
- Deployment pod `socketio-api-7d9d8d8f6c-k2abc` infers service `socketio-api`.

If `ServerConfig.Port` is set in code, `SIO_CLUSTER_PORT` is not needed.

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
```

The application stays minimal and provides the real listen port in code:

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
```

The inferred service name resolves `<service>.<namespace>.svc`; each resolved IP becomes a peer endpoint. Set `SIO_CLUSTER_SERVICE` only when the service name cannot be inferred from the pod name or does not match the workload name. You can also omit it when using static peers or `Cluster.HeadlessDNS` / `SIO_CLUSTER_HEADLESS_DNS` directly. No Kubernetes API watch or RBAC permission is needed.

## Connection State Recovery

Enable CSR when clients may reconnect and should keep room membership and missed broadcasts:

```go
server, err := sio.NewServer(&sio.ServerConfig{
    Port: "3000",
    ConnectionStateRecovery: sio.ConnectionStateRecoveryConfig{
        Enabled:                  true,
        MaxDisconnectionDuration: time.Minute,
        SkipMiddlewaresOnReconnect: true,
    },
})
```

When a client reconnects to the same node, the server restores its session and replays missed broadcasts. If it reconnects to another node, that node asks peers for the recovery state. If no peer has the state anymore, the connection falls back to a normal fresh connection.

## Observability

The library does not require Prometheus. It exposes metrics as a snapshot:

```go
snapshot := server.Metrics()
for _, sample := range snapshot.Samples {
    log.Println(sample.Name, sample.Kind, sample.Value, sample.Labels)
}
```

See [Prometheus integration](./docs/prometheus.md) for a complete collector example and recommended alerts. Chinese version: [Prometheus integration guide](./docs/prometheus.CN.md).

## Testing

```bash
go test ./...
SIO_JS_E2E=1 go test ./... -run TestSocketIOClientE2E -count=1 -v -timeout=30s
go test -race ./...
golangci-lint run ./...
```

The Socket.IO JavaScript e2e files live under `test/e2e/`.

## License

MIT. See [LICENSE](./LICENSE).
