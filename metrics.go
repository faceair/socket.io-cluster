package sio

import (
	"strconv"
	"sync/atomic"
	"time"
)

type MetricKind string

const (
	MetricCounter MetricKind = "counter"
	MetricGauge   MetricKind = "gauge"
)

type MetricSample struct {
	Name   string
	Help   string
	Kind   MetricKind
	Value  float64
	Labels map[string]string
}

type MetricsSnapshot struct {
	GeneratedAt time.Time
	Samples     []MetricSample
}

type metricsRecorder struct {
	engineConnectionsOpened atomic.Uint64
	engineConnectionsClosed atomic.Uint64
	engineConnectionsActive atomic.Int64

	socketsConnected atomic.Uint64
	socketsClosed    atomic.Uint64
	socketsActive    atomic.Int64
	socketsRecovered atomic.Uint64

	packetsIn       [7]atomic.Uint64
	packetsOut      [7]atomic.Uint64
	packetBytesIn   atomic.Uint64
	packetBytesOut  atomic.Uint64
	binaryIn        atomic.Uint64
	binaryOut       atomic.Uint64
	parserErrors    atomic.Uint64
	emitsTotal      atomic.Uint64
	broadcastsTotal atomic.Uint64

	acksRegistered atomic.Uint64
	acksResolved   atomic.Uint64
	acksTimedOut   atomic.Uint64

	clusterRequestsReceived atomic.Uint64
	clusterRequestsSent     atomic.Uint64
	clusterRequestsFailed   atomic.Uint64
	clusterResponsesFailed  atomic.Uint64
}

func newMetricsRecorder() *metricsRecorder { return &metricsRecorder{} }

func (m *metricsRecorder) snapshot(s *Server) MetricsSnapshot {
	samples := make([]MetricSample, 0, 64)
	add := func(name, help string, kind MetricKind, value float64, labels map[string]string) {
		samples = append(samples, MetricSample{Name: name, Help: help, Kind: kind, Value: value, Labels: labels})
	}
	add("sio_engine_connections_active", "Current Engine.IO connections.", MetricGauge, float64(m.engineConnectionsActive.Load()), nil)
	add("sio_engine_connections_opened_total", "Engine.IO connections opened.", MetricCounter, float64(m.engineConnectionsOpened.Load()), nil)
	add("sio_engine_connections_closed_total", "Engine.IO connections closed.", MetricCounter, float64(m.engineConnectionsClosed.Load()), nil)
	add("sio_sockets_active", "Current Socket.IO namespace sockets.", MetricGauge, float64(m.socketsActive.Load()), nil)
	add("sio_sockets_connected_total", "Socket.IO namespace sockets connected.", MetricCounter, float64(m.socketsConnected.Load()), nil)
	add("sio_sockets_closed_total", "Socket.IO namespace sockets closed.", MetricCounter, float64(m.socketsClosed.Load()), nil)
	add("sio_sockets_recovered_total", "Socket.IO sockets restored through connection state recovery.", MetricCounter, float64(m.socketsRecovered.Load()), nil)
	add("sio_packet_bytes_received_total", "Engine.IO message bytes received by Socket.IO parser.", MetricCounter, float64(m.packetBytesIn.Load()), nil)
	add("sio_packet_bytes_sent_total", "Socket.IO packet bytes sent before Engine.IO framing.", MetricCounter, float64(m.packetBytesOut.Load()), nil)
	add("sio_binary_attachments_received_total", "Socket.IO binary attachments received.", MetricCounter, float64(m.binaryIn.Load()), nil)
	add("sio_binary_attachments_sent_total", "Socket.IO binary attachments sent.", MetricCounter, float64(m.binaryOut.Load()), nil)
	add("sio_parser_errors_total", "Socket.IO packet parser errors.", MetricCounter, float64(m.parserErrors.Load()), nil)
	add("sio_emits_total", "Direct socket emits created by the server.", MetricCounter, float64(m.emitsTotal.Load()), nil)
	add("sio_broadcasts_total", "Broadcast emits created by the server.", MetricCounter, float64(m.broadcastsTotal.Load()), nil)
	add("sio_acks_registered_total", "ACK callbacks registered by server emits.", MetricCounter, float64(m.acksRegistered.Load()), nil)
	add("sio_acks_resolved_total", "ACK callbacks resolved before timeout.", MetricCounter, float64(m.acksResolved.Load()), nil)
	add("sio_acks_timed_out_total", "ACK callbacks timed out.", MetricCounter, float64(m.acksTimedOut.Load()), nil)
	add("sio_cluster_requests_received_total", "Cluster control requests received from peers.", MetricCounter, float64(m.clusterRequestsReceived.Load()), nil)
	add("sio_cluster_requests_sent_total", "Cluster control requests sent to peers.", MetricCounter, float64(m.clusterRequestsSent.Load()), nil)
	add("sio_cluster_requests_failed_total", "Cluster requests that failed before receiving a response.", MetricCounter, float64(m.clusterRequestsFailed.Load()), nil)
	add("sio_cluster_responses_failed_total", "Cluster peer responses with HTTP status >= 300.", MetricCounter, float64(m.clusterResponsesFailed.Load()), nil)
	for i := range m.packetsIn {
		typ := PacketType(i)
		labels := map[string]string{"type": typ.String()}
		add("sio_packets_received_total", "Socket.IO packets received, partitioned by packet type.", MetricCounter, float64(m.packetsIn[i].Load()), labels)
	}
	for i := range m.packetsOut {
		typ := PacketType(i)
		labels := map[string]string{"type": typ.String()}
		add("sio_packets_sent_total", "Socket.IO packets sent, partitioned by packet type.", MetricCounter, float64(m.packetsOut[i].Load()), labels)
	}
	for _, stat := range s.namespaceStats() {
		labels := map[string]string{"namespace": stat.namespace}
		add("sio_namespace_sockets", "Current sockets by namespace.", MetricGauge, float64(stat.sockets), labels)
		add("sio_namespace_rooms", "Current rooms by namespace, including socket id rooms.", MetricGauge, float64(stat.rooms), labels)
		add("sio_namespace_room_memberships", "Current room memberships by namespace.", MetricGauge, float64(stat.memberships), labels)
	}
	if s.cluster != nil {
		peers := s.cluster.peerSnapshot()
		add("sio_cluster_peers", "Current cluster peer endpoints known by this node.", MetricGauge, float64(len(peers)), nil)
		add("sio_cluster_fanout_workers", "Configured cluster fanout worker count.", MetricGauge, float64(s.cluster.workerCount), nil)
	}
	return MetricsSnapshot{GeneratedAt: time.Now(), Samples: samples}
}

func (s *Server) Metrics() MetricsSnapshot { return s.metrics.snapshot(s) }

func (t PacketType) String() string {
	switch t {
	case PacketConnect:
		return "connect"
	case PacketDisconnect:
		return "disconnect"
	case PacketEvent:
		return "event"
	case PacketAck:
		return "ack"
	case PacketConnectError:
		return "connect_error"
	case PacketBinaryEvent:
		return "binary_event"
	case PacketBinaryAck:
		return "binary_ack"
	default:
		return "unknown_" + strconv.Itoa(int(t))
	}
}
