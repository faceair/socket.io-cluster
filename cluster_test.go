package sio

import (
	"context"
	"encoding/json"
	"errors"
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
	return connectSocketClientWithAuth(t, ctx, base, "/", "")
}

func connectSocketClientWithAuth(t *testing.T, ctx context.Context, base, namespace, auth string) *websocket.Conn {
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
	if namespace != "" && namespace != "/" {
		connect = append(connect, namespace...)
		connect = append(connect, ',')
	}
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
			return ws
		}
		if strings.HasPrefix(string(data), "44") {
			t.Fatalf("connect rejected: %q", data)
		}
	}
}

func connectSocketClientExpectError(t *testing.T, ctx context.Context, base, namespace, auth string) string {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/socket.io/?EIO=4&transport=websocket"
	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()
	_, data, err := ws.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || data[0] != '0' {
		t.Fatalf("bad open packet %q", data)
	}
	connect := []byte("40")
	if namespace != "" && namespace != "/" {
		connect = append(connect, namespace...)
		connect = append(connect, ',')
	}
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
		if strings.HasPrefix(string(data), "44") {
			var payload struct {
				Message string `json:"message"`
			}
			body := socketPayloadAfterHeaderForTest(data[2:])
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("bad connect_error %q: %v", data, err)
			}
			return payload.Message
		}
		if strings.HasPrefix(string(data), "40") {
			t.Fatalf("unexpected connect success: %q", data)
		}
	}
}

func socketPayloadAfterHeaderForTest(data []byte) []byte {
	if len(data) > 0 && data[0] == '/' {
		if idx := strings.IndexByte(string(data), ','); idx >= 0 {
			data = data[idx+1:]
		}
	}
	for len(data) > 0 && data[0] >= '0' && data[0] <= '9' {
		data = data[1:]
	}
	return data
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

func expectNoSocketEvent(t *testing.T, ws *websocket.Conn, duration time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			t.Fatal(err)
		}
		if string(data) == "2" {
			_ = ws.Write(context.Background(), websocket.MessageText, []byte("3"))
			continue
		}
		if strings.HasPrefix(string(data), "42") {
			t.Fatalf("unexpected socket event %q", data)
		}
	}
}

func readSocketDisconnect(t *testing.T, ctx context.Context, ws *websocket.Conn) string {
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
		if strings.HasPrefix(string(data), "41") {
			return string(data)
		}
	}
}

type clusterPair struct {
	addr1 string
	addr2 string
	s1    *Server
	s2    *Server
}

func startClusterPair(t *testing.T, secret string) clusterPair {
	t.Helper()
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = l1.Close()
		t.Fatal(err)
	}
	addr1 := l1.Addr().String()
	addr2 := l2.Addr().String()
	s1 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: secret, Cluster: ClusterConfig{NodeID: "n1", AdvertiseURL: "http://" + addr1, Peers: []string{"http://" + addr2 + DefaultPath + "?transport=cluster"}}})
	s2 := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Secret: secret, Cluster: ClusterConfig{NodeID: "n2", AdvertiseURL: "http://" + addr2, Peers: []string{"http://" + addr1 + DefaultPath + "?transport=cluster"}}})
	http1 := &http.Server{Handler: s1}
	http2 := &http.Server{Handler: s2}
	go func() { _ = http1.Serve(l1) }()
	go func() { _ = http2.Serve(l2) }()
	t.Cleanup(func() { _ = http1.Shutdown(context.Background()) })
	t.Cleanup(func() { _ = http2.Shutdown(context.Background()) })
	t.Cleanup(func() { _ = s1.Close() })
	t.Cleanup(func() { _ = s2.Close() })
	return clusterPair{addr1: addr1, addr2: addr2, s1: s1, s2: s2}
}

func waitFetchCount(t *testing.T, server *Server, room Room, want int) []ServerSocket {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sockets := server.FetchSockets(room)
		if len(sockets) == want {
			return sockets
		}
		time.Sleep(10 * time.Millisecond)
	}
	sockets := server.FetchSockets(room)
	t.Fatalf("fetch sockets for room %q len=%d, want %d", room, len(sockets), want)
	return nil
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

func TestClusterSocketsLeaveRemovesRemoteRoomMembership(t *testing.T) {
	pair := startClusterPair(t, "shared-secret")
	pair.s2.OnConnection(func(socket ServerSocket) { socket.Join("room-a") })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws := connectSocketClient(t, ctx, "http://"+pair.addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()
	waitFetchCount(t, pair.s1, "room-a", 1)

	pair.s1.In("room-a").SocketsLeave("room-a")
	waitFetchCount(t, pair.s1, "room-a", 0)
	pair.s1.To("room-a").Emit("room-after-leave", "unexpected")
	expectNoSocketEvent(t, ws, 200*time.Millisecond)
}

func TestClusterDisconnectSocketsDisconnectsRemoteNamespace(t *testing.T) {
	pair := startClusterPair(t, "shared-secret")
	pair.s2.OnConnection(func(socket ServerSocket) { socket.Join("room-a") })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws := connectSocketClient(t, ctx, "http://"+pair.addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()
	waitFetchCount(t, pair.s1, "room-a", 1)

	pair.s1.In("room-a").DisconnectSockets(false)
	packet := readSocketDisconnect(t, ctx, ws)
	if packet != "41" {
		t.Fatalf("unexpected disconnect packet %q", packet)
	}
	waitFetchCount(t, pair.s1, "room-a", 0)
}

func TestClusterBroadcastAckTimeoutCleansTracker(t *testing.T) {
	pair := startClusterPair(t, "shared-secret")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws := connectSocketClient(t, ctx, "http://"+pair.addr2)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	done := make(chan error, 1)
	pair.s1.Timeout(100*time.Millisecond).Emit("ack-timeout", func(err error) { done <- err })
	packet := readSocketEvent(t, ctx, ws)
	if !strings.Contains(packet, `"ack-timeout"`) {
		t.Fatalf("unexpected ack packet %q", packet)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrAckTimeout) {
			t.Fatalf("ack error = %v, want %v", err, ErrAckTimeout)
		}
	case <-ctx.Done():
		t.Fatal("ack timeout callback did not fire")
	}
	pair.s1.ackMu.Lock()
	pending := len(pair.s1.broadcasts)
	pair.s1.ackMu.Unlock()
	if pending != 0 {
		t.Fatalf("pending broadcast ack trackers = %d, want 0", pending)
	}
}
