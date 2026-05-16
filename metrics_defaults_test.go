package sio

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDefaultClusterNodeIDUsesPodName(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("POD_NAME", "socket-0")
	s := mustNewServer(t, &ServerConfig{Port: "3000"})
	defer func() { _ = s.Close() }()
	if s.ids.node != "socket-0" {
		t.Fatalf("node id = %q, want pod name", s.ids.node)
	}
	if s.cluster == nil {
		t.Fatal("cluster should be enabled by default")
	}
	if s.cluster.workerCount != 8 {
		t.Fatalf("fanout workers = %d, want 8", s.cluster.workerCount)
	}
	if s.cluster.requestTimeout != time.Second {
		t.Fatalf("request timeout = %s, want 1s", s.cluster.requestTimeout)
	}
	if s.cluster.heartbeatInterval != time.Second {
		t.Fatalf("heartbeat interval = %s, want 1s", s.cluster.heartbeatInterval)
	}
	req := httptest.NewRequest(http.MethodPost, "/socket.io/?transport=cluster&op=fetch&nsp=/", nil)
	req.Header.Set(clusterSecretHeader, "test-secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cluster transport status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestClusterDefaultsFromEnvironment(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("SIO_CLUSTER_PEERS", "10.0.0.1:8080, http://10.0.0.2:8080/socket.io/?transport=cluster")
	t.Setenv("SIO_CLUSTER_ADVERTISE_URL", "http://10.0.0.9:8080")
	s := mustNewServer(t, nil)
	defer func() { _ = s.Close() }()
	peers := s.cluster.peerSnapshot()
	if len(peers) != 2 {
		t.Fatalf("peers len=%d", len(peers))
	}
	if peers[0] != "http://10.0.0.1:8080/socket.io/?transport=cluster" {
		t.Fatalf("peer[0]=%q", peers[0])
	}
	if s.cluster.advertiseURL != "http://10.0.0.9:8080" {
		t.Fatalf("advertiseURL=%q", s.cluster.advertiseURL)
	}
}

func TestRunRequiresServerSecret(t *testing.T) {
	clearClusterEnv(t)
	server := NewServer(&ServerConfig{Port: "3000"})
	defer func() { _ = server.Close() }()
	err := server.Run()
	if err == nil {
		t.Fatal("expected missing ServerConfig.Secret error")
	}
	if !strings.Contains(err.Error(), "ServerConfig.Secret is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterDefaultAdvertiseURLFromPodIPAndPort(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("POD_IP", "10.0.0.9")
	t.Setenv("SIO_CLUSTER_PORT", "3000")
	got, err := defaultAdvertiseURL("", ClusterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://10.0.0.9:3000" {
		t.Fatalf("advertiseURL=%q", got)
	}
}

func TestClusterDefaultAdvertiseURLFromPodDNSAndServicePort(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("POD_NAME", "socket-0")
	t.Setenv("SIO_CLUSTER_SERVICE", "socket-headless")
	t.Setenv("POD_NAMESPACE", "default")
	t.Setenv("SOCKET_HEADLESS_SERVICE_PORT", "3000")
	got, err := defaultAdvertiseURL("", ClusterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://socket-0.socket-headless.default.svc:3000" {
		t.Fatalf("advertiseURL=%q", got)
	}
}

func TestClusterDefaultHeadlessDNSFromServiceNamespace(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("SIO_CLUSTER_SERVICE", "socket-headless")
	t.Setenv("POD_NAMESPACE", "default")
	names := defaultHeadlessDNS(nil)
	if len(names) != 1 || names[0] != "socket-headless.default.svc" {
		t.Fatalf("headless dns=%v", names)
	}
}

func TestClusterInfersHeadlessDNSFromStatefulSetPodName(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("POD_NAME", "socketio-0")
	t.Setenv("POD_NAMESPACE", "default")
	names := defaultHeadlessDNS(nil)
	if len(names) != 1 || names[0] != "socketio.default.svc" {
		t.Fatalf("headless dns=%v", names)
	}
	got, err := defaultAdvertiseURL("3000", ClusterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://socketio-0.socketio.default.svc:3000" {
		t.Fatalf("advertiseURL=%q", got)
	}
}

func TestClusterInfersHeadlessDNSFromDeploymentPodName(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("POD_NAME", "socketio-api-7d9d8d8f6c-k2abc")
	t.Setenv("POD_NAMESPACE", "default")
	names := defaultHeadlessDNS(nil)
	if len(names) != 1 || names[0] != "socketio-api.default.svc" {
		t.Fatalf("headless dns=%v", names)
	}
}

func TestRunRequiresClusterPortOrAdvertiseURL(t *testing.T) {
	clearClusterEnv(t)
	t.Setenv("POD_IP", "10.0.0.9")
	server := NewServer(&ServerConfig{Secret: "test-secret"})
	defer func() { _ = server.Close() }()
	err := server.Run()
	if err == nil {
		t.Fatal("expected missing cluster port error")
	}
	if !strings.Contains(err.Error(), "ServerConfig.Port is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMetricsSnapshotContainsCoreSamples(t *testing.T) {
	clearClusterEnv(t)
	s := mustNewServer(t, &ServerConfig{Port: "3000"})
	defer func() { _ = s.Close() }()
	s.metrics.engineConnectionsOpened.Add(2)
	s.metrics.engineConnectionsActive.Add(1)
	metrics := s.Metrics()
	if metrics.GeneratedAt.IsZero() || len(metrics.Samples) == 0 {
		t.Fatalf("empty metrics snapshot: %#v", metrics)
	}
	if !hasMetric(metrics, "sio_engine_connections_opened_total") || !hasMetric(metrics, "sio_cluster_peers") {
		t.Fatalf("missing core metrics: %#v", metrics.Samples)
	}
}

func hasMetric(snapshot MetricsSnapshot, name string) bool {
	for _, sample := range snapshot.Samples {
		if sample.Name == name {
			return true
		}
	}
	return false
}

func clearClusterEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SIO_CLUSTER_PEERS",
		"SOCKETIO_CLUSTER_PEERS",
		"SIO_CLUSTER_ADVERTISE_URL",
		"SOCKETIO_CLUSTER_ADVERTISE_URL",
		"SOCKETIO_ADVERTISE_URL",
		"SIO_CLUSTER_PORT",
		"SOCKETIO_CLUSTER_PORT",
		"SOCKETIO_PORT",
		"PORT",
		"HTTP_PORT",
		"SIO_CLUSTER_HOST",
		"SOCKETIO_CLUSTER_HOST",
		"POD_IP",
		"HOST_IP",
		"SIO_CLUSTER_SERVICE",
		"SOCKETIO_CLUSTER_SERVICE",
		"SERVICE_NAME",
		"SIO_CLUSTER_HEADLESS_DNS",
		"SOCKETIO_CLUSTER_HEADLESS_DNS",
		"POD_NAMESPACE",
		"NAMESPACE",
		"KUBERNETES_NAMESPACE",
		"SIO_CLUSTER_SCHEME",
		"SOCKETIO_CLUSTER_SCHEME",
	} {
		t.Setenv(key, "")
	}
}

func TestServeHTTPRequiresRun(t *testing.T) {
	clearClusterEnv(t)
	s := NewServer(&ServerConfig{Port: "3000", Secret: "test-secret"})
	defer func() { _ = s.Close() }()
	req := httptest.NewRequest(http.MethodGet, "/socket.io/?EIO=4&transport=polling", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "call Run") {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestRunAfterCloseFails(t *testing.T) {
	clearClusterEnv(t)
	s := NewServer(&ServerConfig{Port: "3000", Secret: "test-secret"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	err := s.Run()
	if err == nil {
		t.Fatal("expected Run after Close error")
	}
	if !strings.Contains(err.Error(), "cannot be restarted") {
		t.Fatalf("unexpected error: %v", err)
	}
}
