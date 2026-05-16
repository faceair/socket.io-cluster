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

func TestClusterBroadcastFanout(t *testing.T) {
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

	s1 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}}})
	s2 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}}})
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
	ws := connectSocketClient(t, ctx, "http://"+addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	s1.Emit("cluster-event", "ok")
	packet := readSocketEvent(t, ctx, ws)
	if !strings.Contains(packet, `"cluster-event"`) || !strings.Contains(packet, `"ok"`) {
		t.Fatalf("unexpected cluster packet %q", packet)
	}
}

func TestClusterSocketsJoinThenRoomBroadcast(t *testing.T) {
	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	addr1 := ln1.Addr().String()
	_ = ln1.Close()
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	addr2 := ln2.Addr().String()
	_ = ln2.Close()
	s1 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}}})
	s2 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}}})
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
	ws := connectSocketClient(t, ctx, "http://"+addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()
	s1.SocketsJoin("room-a")
	s1.To("room-a").Emit("room-event", 7)
	packet := readSocketEvent(t, ctx, ws)
	if !strings.Contains(packet, `"room-event"`) || !strings.Contains(packet, `7`) {
		t.Fatalf("unexpected packet %q", packet)
	}
}

func connectSocketClient(t *testing.T, ctx context.Context, base string) *websocket.Conn {
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
	if err := ws.Write(ctx, websocket.MessageText, []byte("40")); err != nil {
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
			return ws
		}
	}
}

func readSocketEvent(t *testing.T, ctx context.Context, ws *websocket.Conn) string {
	t.Helper()
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) == "2" {
			_ = ws.Write(ctx, websocket.MessageText, []byte("3"))
			continue
		}
		if strings.HasPrefix(string(data), "42") {
			return string(data)
		}
		var open map[string]any
		_ = json.Unmarshal(data[1:], &open)
	}
}

func TestClusterBroadcastAck(t *testing.T) {
	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	addr1 := ln1.Addr().String()
	_ = ln1.Close()
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	addr2 := ln2.Addr().String()
	_ = ln2.Close()
	s1 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}}})
	s2 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}}})
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
	ws := connectSocketClient(t, ctx, "http://"+addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	done := make(chan error, 1)
	s1.Timeout(time.Second).Emit("ack-event", func(err error) { done <- err })
	packet := readSocketEvent(t, ctx, ws)
	id := socketPacketAckID(t, packet)
	if err := ws.Write(ctx, websocket.MessageText, []byte("43"+id+"[\"pong\"]")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected ack error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("broadcast ack timeout")
	}
}

func socketPacketAckID(t *testing.T, packet string) string {
	t.Helper()
	if !strings.HasPrefix(packet, "42") {
		t.Fatalf("not event packet: %q", packet)
	}
	i := 2
	for i < len(packet) && packet[i] >= '0' && packet[i] <= '9' {
		i++
	}
	if i == 2 {
		t.Fatalf("packet has no ack id: %q", packet)
	}
	return packet[2:i]
}

func TestClusterFetchSocketsAndServerSideEmit(t *testing.T) {
	ln1, _ := net.Listen("tcp", "127.0.0.1:0")
	addr1 := ln1.Addr().String()
	_ = ln1.Close()
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	addr2 := ln2.Addr().String()
	_ = ln2.Close()
	s1 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}}})
	s2 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: "test-secret", Cluster: ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}}})
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
	ws := connectSocketClient(t, ctx, "http://"+addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	// Wait until the connection handler path has populated the room indexes.
	deadline := time.Now().Add(time.Second)
	var sockets []ServerSocket
	for time.Now().Before(deadline) {
		sockets = s1.FetchSockets()
		if len(sockets) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(sockets) != 1 {
		t.Fatalf("fetch sockets len=%d", len(sockets))
	}
	if sockets[0].ID() == "" {
		t.Fatal("remote socket id is empty")
	}

	done := make(chan string, 1)
	s2.OnServerSideEmit("control", func(value string) { done <- value })
	s1.ServerSideEmit("control", "hello")
	select {
	case got := <-done:
		if got != "hello" {
			t.Fatalf("serverSideEmit got %q", got)
		}
	case <-ctx.Done():
		t.Fatal("serverSideEmit timeout")
	}
}

func TestClusterPeerSecretProtectsTransportAndBypassesEIOAuthenticator(t *testing.T) {
	s := mustNewServer(t, &ServerConfig{
		Port:    "3000",
		Secret:  "shared-secret",
		EIO:     EngineIOConfig{Authenticator: func(http.ResponseWriter, *http.Request) bool { return false }},
		Cluster: ClusterConfig{},
	})
	defer func() { _ = s.Close() }()

	for name, secret := range map[string]string{"missing": "", "wrong": "bad-secret"} {
		req := httptest.NewRequest(http.MethodPost, "/socket.io/?transport=cluster&op=fetch&nsp=/", nil)
		if secret != "" {
			req.Header.Set(clusterSecretHeader, secret)
		}
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s secret status=%d body=%q", name, rec.Code, rec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/socket.io/?transport=cluster&op=fetch&nsp=/", nil)
	req.Header.Set(clusterSecretHeader, "shared-secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid secret status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestClusterBroadcastFanoutWithPeerSecret(t *testing.T) {
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

	secret := "shared-secret"
	s1 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: secret, Cluster: ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}}})
	s2 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: secret, Cluster: ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}}})
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
	ws := connectSocketClient(t, ctx, "http://"+addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	s1.Emit("secret-cluster-event", "ok")
	packet := readSocketEvent(t, ctx, ws)
	if !strings.Contains(packet, `"secret-cluster-event"`) || !strings.Contains(packet, `"ok"`) {
		t.Fatalf("unexpected cluster packet %q", packet)
	}
}
