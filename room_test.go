package sio

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func TestBroadcastOperatorRoomUnionExceptDeduplicates(t *testing.T) {
	server := mustNewServer(t, &ServerConfig{Port: "3000", Secret: "test-secret"})
	var connected atomic.Int32
	server.OnConnection(func(socket ServerSocket) {
		switch connected.Add(1) {
		case 1:
			socket.Join("r1", "r2")
		case 2:
			socket.Join("r2", "r3")
		case 3:
			socket.Join("r3")
		}
	})
	ts := httptest.NewServer(server)
	defer ts.Close()
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws1 := connectSocketClient(t, ctx, ts.URL)
	defer func() { _ = ws1.Close(websocket.StatusNormalClosure, "") }()
	ws2 := connectSocketClient(t, ctx, ts.URL)
	defer func() { _ = ws2.Close(websocket.StatusNormalClosure, "") }()
	ws3 := connectSocketClient(t, ctx, ts.URL)
	defer func() { _ = ws3.Close(websocket.StatusNormalClosure, "") }()
	waitFetchCount(t, server, "r1", 1)
	waitFetchCount(t, server, "r2", 2)
	waitFetchCount(t, server, "r3", 2)

	server.To("r1").To("r2").Except("r3").Emit("room-union", "only-r1-r2")
	packet := readSocketEvent(t, ctx, ws1)
	if !strings.Contains(packet, `"room-union"`) || !strings.Contains(packet, `"only-r1-r2"`) {
		t.Fatalf("unexpected room packet %q", packet)
	}
	expectNoSocketEvent(t, ws2, 200*time.Millisecond)
	expectNoSocketEvent(t, ws3, 200*time.Millisecond)
}
