# Prometheus 集成

[English](./prometheus.md)

`socket.io-cluster` 不直接依赖 Prometheus。应用可以把 `Server.Metrics()` 暴露给 Prometheus、OpenTelemetry 或内部监控系统。

本文展示 Prometheus 集成方式。

## 最小 collector

完整可运行示例见 [`examples/prometheus/main.go`](../examples/prometheus/main.go)。基本模式是：

1. 创建 Socket.IO server。
2. 实现一个 Prometheus `Collector`，在 `Collect` 中读取 `server.Metrics()`。
3. 将每个 `MetricSample` 转换为 `prometheus.MustNewConstMetric`。
4. 用 `promhttp.HandlerFor` 暴露 `/metrics`。

```go
type sioCollector struct {
    server *sio.Server
}

func (c sioCollector) Describe(ch chan<- *prometheus.Desc) {}

func (c sioCollector) Collect(ch chan<- prometheus.Metric) {
    snapshot := c.server.Metrics()
    for _, sample := range snapshot.Samples {
        labels := sample.Labels
        labelNames := make([]string, 0, len(labels))
        labelValues := make([]string, 0, len(labels))
        for k, v := range labels {
            labelNames = append(labelNames, k)
            labelValues = append(labelValues, v)
        }

        valueType := prometheus.GaugeValue
        if sample.Kind == sio.MetricCounter {
            valueType = prometheus.CounterValue
        }

        desc := prometheus.NewDesc(sample.Name, sample.Help, labelNames, nil)
        ch <- prometheus.MustNewConstMetric(desc, valueType, sample.Value, labelValues...)
    }
}
```

和 Socket.IO server 一起注册：

```go
server, err := sio.NewServer(&sio.ServerConfig{Port: "3000"})
if err != nil {
    log.Fatal(err)
}

registry := prometheus.NewRegistry()
registry.MustRegister(sioCollector{server: server})

http.Handle("/socket.io/", server)
http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
log.Fatal(http.ListenAndServe(":3000", nil))
```

## 建议关注的指标

### 连接和 socket

- `sio_engine_connections_active`：当前 Engine.IO 连接数。
- `sio_engine_connections_opened_total` / `sio_engine_connections_closed_total`：Engine.IO 连接生命周期计数。
- `sio_sockets_active`：当前 Socket.IO namespace socket 数。
- `sio_sockets_connected_total` / `sio_sockets_closed_total`：Socket.IO socket 生命周期计数。
- `sio_sockets_recovered_total`：通过 Connection State Recovery 恢复的 socket 数。
- `sio_namespace_sockets{namespace}`：按 namespace 统计的当前 socket 数。

### packet 和二进制数据

- `sio_packets_received_total{type}` / `sio_packets_sent_total{type}`：按 Socket.IO packet type 统计的收发计数。
- `sio_packet_bytes_received_total` / `sio_packet_bytes_sent_total`：传输层 framing 之前的 Socket.IO packet 字节数。
- `sio_binary_attachments_received_total` / `sio_binary_attachments_sent_total`：二进制 attachment 计数。
- `sio_parser_errors_total`：无法解析的 packet 数。

### ACK 和广播

- `sio_emits_total`：server 直接向 socket emit 的次数。
- `sio_broadcasts_total`：broadcast emit 次数。
- `sio_acks_registered_total`：server emit 注册的 ACK 回调数。
- `sio_acks_resolved_total`：超时前完成的 ACK 回调数。
- `sio_acks_timed_out_total`：超时的 ACK 回调数。

### cluster

- `sio_cluster_peers`：当前已知 peer endpoint 数。
- `sio_cluster_fanout_workers`：配置的跨节点 fanout worker 数。
- `sio_cluster_requests_received_total`：从 peers 收到的 cluster 控制请求数。
- `sio_cluster_requests_sent_total`：发给 peers 的 cluster 控制请求数。
- `sio_cluster_requests_failed_total`：未收到响应的 peer 请求数。
- `sio_cluster_responses_failed_total`：HTTP status >= 300 的 peer 响应数。

### rooms

- `sio_namespace_rooms{namespace}`：每个 namespace 当前 room 数，包含 socket-id room。
- `sio_namespace_room_memberships{namespace}`：每个 namespace 当前 room membership 数。

## 常用 PromQL

当前连接规模：

```promql
sum(sio_engine_connections_active)
sum by (namespace) (sio_namespace_sockets)
```

消息吞吐：

```promql
sum by (type) (rate(sio_packets_received_total[5m]))
sum by (type) (rate(sio_packets_sent_total[5m]))
```

解析错误比例：

```promql
sum(rate(sio_parser_errors_total[5m]))
/
clamp_min(sum(rate(sio_packets_received_total[5m])), 1)
```

ACK timeout 比例：

```promql
sum(rate(sio_acks_timed_out_total[5m]))
/
clamp_min(sum(rate(sio_acks_registered_total[5m])), 1)
```

cluster 请求失败比例：

```promql
sum(rate(sio_cluster_requests_failed_total[5m]))
/
clamp_min(sum(rate(sio_cluster_requests_sent_total[5m])), 1)
```

意外单节点模式：

```promql
sio_cluster_peers < 1
```

room membership 趋势：

```promql
sum by (namespace) (sio_namespace_room_memberships)
```

## 如何理解常见变化

- `sio_parser_errors_total` 持续上涨通常表示客户端协议版本不匹配、packet 格式异常，或者代理错误切分/改写了 payload。
- `sio_acks_timed_out_total` 上涨可能是客户端 handler 慢、客户端断开、网络延迟，或 broadcast fanout 超过了超时预算。
- `sio_cluster_requests_failed_total` 上涨通常指向 peer 不可达、DNS 结果过期、滚动发布窗口，或 advertise 地址配置错误。
- Kubernetes 中 `sio_cluster_peers` 低于预期时，优先检查 headless service、Pod readiness、`POD_IP`、`ServerConfig.Port` / 端口环境变量，以及 `SIO_CLUSTER_SERVICE`。
- `sio_namespace_room_memberships` 长期只涨不降可能是长生命周期 room 的正常现象，也可能提示 socket 泄漏或业务没有离开 room。
