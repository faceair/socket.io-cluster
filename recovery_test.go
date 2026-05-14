package sio

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func TestConnectionStateRecoveryReplaysMissedBroadcast(t *testing.T) {
	server := mustNewServer(t, &ServerConfig{
		AcceptAnyNamespace: true,
		Port:               "3000",
		ConnectionStateRecovery: ConnectionStateRecoveryConfig{
			Enabled:                  true,
			MaxDisconnectionDuration: time.Minute,
		},
	})
	server.OnConnection(func(socket ServerSocket) { socket.Join("room") })
	ts := httptest.NewServer(server)
	defer ts.Close()
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, pid := connectRecoveryClient(t, ctx, ts.URL, "")
	server.To("room").Emit("csr", "before")
	packet := readSocketEvent(t, ctx, ws)
	offset := eventOffset(t, packet)
	if err := ws.Close(websocket.StatusNormalClosure, "transport close"); err != nil {
		t.Fatal(err)
	}
	waitRecoverySession(t, server, pid)

	server.To("room").Emit("csr", "missed")
	recovered, _ := connectRecoveryClient(t, ctx, ts.URL, `{"pid":"`+pid+`","offset":"`+offset+`"}`)
	defer func() { _ = recovered.Close(websocket.StatusNormalClosure, "") }()
	replay := readSocketEvent(t, ctx, recovered)
	if !strings.Contains(replay, `"csr"`) || !strings.Contains(replay, `"missed"`) {
		t.Fatalf("unexpected replay packet %q", replay)
	}
}

func connectRecoveryClient(t *testing.T, ctx context.Context, base string, auth string) (*websocket.Conn, string) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/socket.io/?EIO=4&transport=websocket"
	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, data, err := ws.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || data[0] != '0' {
		t.Fatalf("bad open packet %q", data)
	}
	connect := []byte("40")
	if auth != "" {
		connect = append(connect, auth...)
	}
	if err := ws.Write(ctx, websocket.MessageText, connect); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, err = ws.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) == "2" {
			_ = ws.Write(ctx, websocket.MessageText, []byte("3"))
			continue
		}
		if strings.HasPrefix(string(data), "40") {
			var payload struct {
				PID string `json:"pid"`
			}
			if err := json.Unmarshal(data[2:], &payload); err != nil {
				t.Fatal(err)
			}
			if payload.PID == "" {
				t.Fatalf("connect packet missing pid: %q", data)
			}
			return ws, payload.PID
		}
	}
}

func eventOffset(t *testing.T, packet string) string {
	t.Helper()
	if !strings.HasPrefix(packet, "42") {
		t.Fatalf("not event packet %q", packet)
	}
	var values []any
	if err := json.Unmarshal([]byte(packet[2:]), &values); err != nil {
		t.Fatal(err)
	}
	if len(values) == 0 {
		t.Fatalf("event packet has no values: %q", packet)
	}
	offset, ok := values[len(values)-1].(string)
	if !ok || offset == "" {
		t.Fatalf("event packet has no offset: %q", packet)
	}
	return offset
}

func waitRecoverySession(t *testing.T, server *Server, pid string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.recovery.mu.Lock()
		_, ok := server.recovery.sessions[recoveryKey("/", pid)]
		server.recovery.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("recovery session %q was not saved", pid)
}

func TestClusterConnectionStateRecoveryBroadcastPull(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr1 := ln1.Addr().String()
	_ = ln1.Close()
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr2 := ln2.Addr().String()
	_ = ln2.Close()

	config1 := &ServerConfig{
		AcceptAnyNamespace:      true,
		Cluster:                 ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}},
		ConnectionStateRecovery: ConnectionStateRecoveryConfig{Enabled: true, MaxDisconnectionDuration: time.Minute},
	}
	config2 := &ServerConfig{
		AcceptAnyNamespace:      true,
		Cluster:                 ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}},
		ConnectionStateRecovery: ConnectionStateRecoveryConfig{Enabled: true, MaxDisconnectionDuration: time.Minute},
	}
	s1 := mustNewServer(t, config1)
	s2 := mustNewServer(t, config2)
	s1.OnConnection(func(socket ServerSocket) { socket.Join("room") })
	s2.OnConnection(func(socket ServerSocket) { socket.Join("room") })
	http1 := &http.Server{Handler: s1}
	http2 := &http.Server{Handler: s2}
	l1, err := net.Listen("tcp", addr1)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := net.Listen("tcp", addr2)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = http1.Serve(l1) }()
	go func() { _ = http2.Serve(l2) }()
	defer func() { _ = http1.Shutdown(context.Background()) }()
	defer func() { _ = http2.Shutdown(context.Background()) }()
	defer func() { _ = s1.Close() }()
	defer func() { _ = s2.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, pid := connectRecoveryClient(t, ctx, "http://"+addr1, "")
	s1.To("room").Emit("csr", "before")
	offset := eventOffset(t, readSocketEvent(t, ctx, ws))
	if err := ws.Close(websocket.StatusNormalClosure, "transport close"); err != nil {
		t.Fatal(err)
	}
	waitRecoverySession(t, s1, pid)

	s2.To("room").Emit("csr", "missed")
	recovered, _ := connectRecoveryClient(t, ctx, "http://"+addr2, `{"pid":"`+pid+`","offset":"`+offset+`"}`)
	defer func() { _ = recovered.Close(websocket.StatusNormalClosure, "") }()
	replay := readSocketEvent(t, ctx, recovered)
	if !strings.Contains(replay, `"csr"`) || !strings.Contains(replay, `"missed"`) {
		t.Fatalf("unexpected cross-node replay packet %q", replay)
	}

	s1.recovery.mu.Lock()
	_, stillCached := s1.recovery.sessions[recoveryKey("/", pid)]
	s1.recovery.mu.Unlock()
	if stillCached {
		t.Fatalf("remote CSR pull did not clear owner session cache for pid %q", pid)
	}
}
