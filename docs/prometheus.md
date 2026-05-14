# Prometheus integration

[简体中文](./prometheus.CN.md)

`socket.io-cluster` does not depend on Prometheus. Applications can expose `Server.Metrics()` through Prometheus, OpenTelemetry, or an internal metrics system.

This guide shows the Prometheus path.

## Minimal collector

A complete runnable example is available in [`examples/prometheus/main.go`](../examples/prometheus/main.go). The pattern is:

1. Create the Socket.IO server.
2. Implement a Prometheus `Collector` that reads `server.Metrics()` in `Collect`.
3. Convert every `MetricSample` into `prometheus.MustNewConstMetric`.
4. Serve `/metrics` with `promhttp.HandlerFor`.

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

Register it next to your Socket.IO server:

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

## Metrics to watch

### Connections and sockets

- `sio_engine_connections_active`: current Engine.IO connections.
- `sio_engine_connections_opened_total` / `sio_engine_connections_closed_total`: Engine.IO connection lifecycle counters.
- `sio_sockets_active`: current Socket.IO namespace sockets.
- `sio_sockets_connected_total` / `sio_sockets_closed_total`: Socket.IO socket lifecycle counters.
- `sio_sockets_recovered_total`: sockets restored through Connection State Recovery.
- `sio_namespace_sockets{namespace}`: current sockets per namespace.

### Packets and binary payloads

- `sio_packets_received_total{type}` / `sio_packets_sent_total{type}`: Socket.IO packet counters by packet type.
- `sio_packet_bytes_received_total` / `sio_packet_bytes_sent_total`: Socket.IO packet bytes before transport framing.
- `sio_binary_attachments_received_total` / `sio_binary_attachments_sent_total`: binary attachment counters.
- `sio_parser_errors_total`: packets that could not be parsed.

### ACKs and broadcasts

- `sio_emits_total`: direct server-to-socket emits.
- `sio_broadcasts_total`: broadcast emits.
- `sio_acks_registered_total`: ACK callbacks registered by server emits.
- `sio_acks_resolved_total`: ACK callbacks completed before timeout.
- `sio_acks_timed_out_total`: ACK callbacks that timed out.

### Cluster

- `sio_cluster_peers`: current known peer endpoints.
- `sio_cluster_fanout_workers`: configured cross-node fanout workers.
- `sio_cluster_requests_received_total`: cluster control requests received from peers.
- `sio_cluster_requests_sent_total`: cluster control requests sent to peers.
- `sio_cluster_requests_failed_total`: peer requests that failed before a response was received.
- `sio_cluster_responses_failed_total`: peer responses with HTTP status >= 300.

### Rooms

- `sio_namespace_rooms{namespace}`: current room count per namespace, including socket-id rooms.
- `sio_namespace_room_memberships{namespace}`: current room memberships per namespace.

## Useful PromQL

Current connection scale:

```promql
sum(sio_engine_connections_active)
sum by (namespace) (sio_namespace_sockets)
```

Packet throughput:

```promql
sum by (type) (rate(sio_packets_received_total[5m]))
sum by (type) (rate(sio_packets_sent_total[5m]))
```

Parser error ratio:

```promql
sum(rate(sio_parser_errors_total[5m]))
/
clamp_min(sum(rate(sio_packets_received_total[5m])), 1)
```

ACK timeout ratio:

```promql
sum(rate(sio_acks_timed_out_total[5m]))
/
clamp_min(sum(rate(sio_acks_registered_total[5m])), 1)
```

Cluster request failure ratio:

```promql
sum(rate(sio_cluster_requests_failed_total[5m]))
/
clamp_min(sum(rate(sio_cluster_requests_sent_total[5m])), 1)
```

Unexpected single-node mode:

```promql
sio_cluster_peers < 1
```

Room membership trend:

```promql
sum by (namespace) (sio_namespace_room_memberships)
```

## How to interpret common changes

- A rising `sio_parser_errors_total` usually means incompatible clients, malformed packets, or a proxy that splits or rewrites payloads incorrectly.
- A rising `sio_acks_timed_out_total` can mean slow client handlers, client disconnects, network latency, or a broadcast fanout that is larger than the timeout budget.
- A rising `sio_cluster_requests_failed_total` usually points to unreachable peers, stale DNS results, rolling deployment windows, or an incorrect advertise address.
- `sio_cluster_peers` lower than expected in Kubernetes usually means the headless service, pod readiness, `POD_IP`, `ServerConfig.Port` / port env, or `SIO_CLUSTER_SERVICE` should be checked first.
- `sio_namespace_room_memberships` growing without falling can be normal for long-lived rooms, but it is also a useful signal for leaked sockets or rooms that are never left.
