# socket.io-cluster

[English](./README.md)

`socket.io-cluster` 是一个 Go Socket.IO server library，实现 Socket.IO protocol v5 over Engine.IO protocol v4；同一套代码可以单节点运行，也可以直接作为内置多节点集群运行。

它面向服务端使用者：作为 `http.Handler` 挂载，注册事件处理器，使用 room、ACK、二进制事件，并在不额外引入 Redis、NATS 等消息总线的情况下完成跨节点投递。

## 特性

- 支持 Socket.IO protocol v5 over Engine.IO protocol v4。
- 兼容 Socket.IO JavaScript client v4.x，包括已测试的 `socket.io-client@4.8.x` 系列。
- 支持 WebSocket、polling 和 WebSocket upgrade。
- 支持 namespace、room、broadcast、`except`、本地广播、ACK 回调、二进制事件和二进制 ACK。
- 内置 peer-to-peer 集群 fanout，复用同一个 Socket.IO path。
- 支持静态 peers 和 Kubernetes headless DNS 发现，不需要 Kubernetes API 权限。
- 支持 Connection State Recovery (CSR)：同节点恢复，并在跨节点重连时尝试从 peers 拉取恢复状态。
- 通过无 Prometheus 依赖的 `Server.Metrics()` snapshot 暴露可观测指标。

## 安装

```bash
go get github.com/faceair/socket.io-cluster
```

## 快速开始

`ServerConfig.Port` 必须和当前进程真实监听的 HTTP 端口一致。库不会替你监听端口；这个值用于生成其他集群节点访问当前节点的地址。

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

Socket.IO JavaScript client v4.x 可以正常连接：

```js
import { io } from "socket.io-client";

const socket = io("http://localhost:3000", { transports: ["websocket"] });

socket.emit("hello", "alice", (reply) => {
  console.log(reply); // "hello alice"
});
```

## 配置概览

### 必配项

必须提供以下任意一种：

- `ServerConfig.Port`，例如进程监听 `:3000` 时传 `"3000"`。
- 完整的 `Cluster.AdvertiseURL`，例如 `"http://10.0.0.9:3000"`。
- 通过环境变量提供端口：`SIO_CLUSTER_PORT`、`SOCKETIO_CLUSTER_PORT`、`SOCKETIO_PORT`、`PORT`、`HTTP_PORT` 或 Kubernetes `<SERVICE>_SERVICE_PORT`。

如果既没有端口，也没有完整的 advertise URL，`NewServer` 会返回错误。

### 选配项

| 配置 | 作用 | 默认值 |
| --- | --- | --- |
| `Path` | Socket.IO endpoint path | `/socket.io/` |
| `Cluster.NodeID` | 稳定节点身份 | `POD_NAME`、`HOSTNAME`、`os.Hostname()`，最后随机生成 |
| `Cluster.AdvertiseURL` | peers 访问当前节点的完整 URL | 由 host + port 自动拼接 |
| `Cluster.Peers` | 静态 peer endpoints | `SIO_CLUSTER_PEERS`、`SOCKETIO_CLUSTER_PEERS` |
| `Cluster.HeadlessDNS` | 用于解析 peer IP 的 DNS 名称 | `SIO_CLUSTER_HEADLESS_DNS`、`SOCKETIO_CLUSTER_HEADLESS_DNS`、service env，或从 `POD_NAME` 推断 |
| `Cluster.RequestTimeout` | peer 请求超时 | `2s` |
| `Cluster.HeartbeatInterval` | DNS 刷新间隔 | `30s` |
| `Cluster.FanoutWorkers` | 跨节点 fanout worker 数 | `8` |

### Port 和 AdvertiseURL

多数部署只需要配置 `Port`：

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
```

如果 peers 需要通过不同的 host 或 scheme 访问当前节点，可以直接配置 `AdvertiseURL`：

```go
server, err := sio.NewServer(&sio.ServerConfig{
    Cluster: sio.ClusterConfig{
        AdvertiseURL: "https://socket-0.example.internal:443",
    },
})
```

配置 `AdvertiseURL` 时必须包含 scheme、host 和 port。

## 事件、ACK 和二进制数据

handler 是普通 Go 函数。最后一个函数参数会被识别为 ACK callback。

```go
server.OnConnection(func(socket sio.ServerSocket) {
    socket.OnEvent("profile:update", func(userID string, attrs map[string]any, ack func(string)) {
        // 在这里保存用户资料。
        ack("ok")
    })

    socket.OnEvent("file:upload", func(name string, data []byte, ack func(sio.Binary)) {
        // 通过二进制 ACK 回传上传内容。
        ack(sio.Binary(data))
    })

    socket.Emit("server:ready", map[string]any{"id": socket.ID()})
})
```

如果需要等待 client ACK，可以使用 `Timeout`：

```go
socket.Timeout(5 * time.Second).Emit("question", "continue?", func(err error, answer string) {
    if err != nil {
        log.Println("client did not ACK in time", err)
        return
    }
    log.Println("client answered", answer)
})
```

## Rooms 和广播

每个 socket 都可以加入 room。默认情况下，broadcast API 会同时投递到本节点和已配置的 peers。

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

也可以跨集群查询 socket 和修改 room：

```go
sockets := server.To("room-a").FetchSockets()
for _, socket := range sockets {
    log.Println(socket.ID(), socket.Rooms())
}

server.To("vip").SocketsJoin("priority")
server.To("leaving").DisconnectSockets(false)
```

## 集群配置

cluster 默认开启，control transport 总是挂在正常 Socket.IO path 下，并通过 `transport=cluster` 访问。最小配置只需要提供 server 端口：

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
```

单节点使用不需要配置 `ClusterConfig`。如果要连接其他节点，只需要增加一种发现来源：静态 peers、`SIO_CLUSTER_PEERS`、`Cluster.HeadlessDNS`、`SIO_CLUSTER_HEADLESS_DNS`，或 Kubernetes service 名。没有配置 peers 或 DNS discovery 时，server 会以单节点方式运行，不会向远端投递。

### 静态 peers

```go
server, err := sio.NewServer(&sio.ServerConfig{
    Port: "3000",
    Cluster: sio.ClusterConfig{
        NodeID: "socket-a",
        Peers: []string{
            "http://10.0.0.11:3000/socket.io/?transport=cluster",
            "10.0.0.12:3000", // 会自动补齐
        },
    },
})
```

也可以通过 `SIO_CLUSTER_PEERS` 或 `SOCKETIO_CLUSTER_PEERS` 传入逗号分隔的 peer 列表。

### Kubernetes headless DNS

在 Kubernetes 中，如果要用 headless DNS 发现，提供 pod 身份即可。如果 headless service 名和 workload 名一致，库可以从 `POD_NAME` 自动推断：

- StatefulSet pod `socketio-0` 会推断 service `socketio`。
- Deployment pod `socketio-api-7d9d8d8f6c-k2abc` 会推断 service `socketio-api`。

如果代码里已经设置了 `ServerConfig.Port`，就不需要再设置 `SIO_CLUSTER_PORT`。

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

应用代码保持简单，并在代码里传入真实监听端口：

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
```

推断出的 service 名会解析 `<service>.<namespace>.svc`，每个解析出的 IP 都会成为一个 peer endpoint。只有 service 名无法从 pod 名推断，或者 service 名和 workload 名不一致时，才需要设置 `SIO_CLUSTER_SERVICE`。如果使用静态 peers，或直接配置 `Cluster.HeadlessDNS` / `SIO_CLUSTER_HEADLESS_DNS`，也可以不配它。不需要 Kubernetes API watch 或 RBAC 权限。

## Connection State Recovery

如果希望客户端重连后保留 room membership 并补收错过的 broadcast，可以启用 CSR：

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

客户端重连到同一节点时，server 会恢复 session 并重放错过的 broadcast。客户端重连到其他节点时，该节点会向 peers 查询恢复状态。如果没有 peer 仍保存该状态，连接会退化为普通新连接。

## 可观测性

主库不要求引入 Prometheus。它只暴露 metrics snapshot：

```go
snapshot := server.Metrics()
for _, sample := range snapshot.Samples {
    log.Println(sample.Name, sample.Kind, sample.Value, sample.Labels)
}
```

完整 collector 示例和推荐告警见 [Prometheus integration](./docs/prometheus.md)。中文文档见 [Prometheus 集成指南](./docs/prometheus.CN.md)。

## 测试

```bash
go test ./...
SIO_JS_E2E=1 go test ./... -run TestSocketIOClientE2E -count=1 -v -timeout=30s
go test -race ./...
golangci-lint run ./...
```

Socket.IO JavaScript e2e 文件位于 `test/e2e/`。

Kubernetes cluster e2e 清单和可复用验证流程位于
[`test/k8s/`](./test/k8s/)，英文文档见
[`test/k8s/README.md`](./test/k8s/README.md)。

## License

MIT。见 [LICENSE](./LICENSE)。
