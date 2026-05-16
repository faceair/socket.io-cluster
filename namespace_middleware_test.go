package sio

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

func TestNamespaceMiddlewareChainStopsOnReject(t *testing.T) {
	server := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Port: "3000", Secret: "test-secret"})
	var first, second, third atomic.Int32
	workspace := server.Of("/workspace")
	workspace.Use(func(ServerSocket, *Handshake) any {
		first.Add(1)
		return nil
	})
	workspace.Use(func(ServerSocket, *Handshake) any {
		second.Add(1)
		return errors.New("workspace denied")
	})
	workspace.Use(func(ServerSocket, *Handshake) any {
		third.Add(1)
		return nil
	})
	ts := httptest.NewServer(server)
	defer ts.Close()
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	message := connectSocketClientExpectError(t, ctx, ts.URL, "/workspace", `{"token":"bad"}`)
	if message != "workspace denied" {
		t.Fatalf("connect_error message = %q, want workspace denied", message)
	}
	if first.Load() != 1 || second.Load() != 1 || third.Load() != 0 {
		t.Fatalf("middleware calls first=%d second=%d third=%d", first.Load(), second.Load(), third.Load())
	}
}

func TestDynamicNamespaceMiddlewareReceivesHandshakeAuth(t *testing.T) {
	server := mustNewServer(t, &ServerConfig{AcceptAnyNamespace: true, Port: "3000", Secret: "test-secret"})
	var dynamicAuthSeen atomic.Bool
	server.OnNewNamespace(func(namespace *Namespace) {
		if namespace.Name() != "/dynamic" {
			return
		}
		namespace.Use(func(_ ServerSocket, handshake *Handshake) any {
			if string(handshake.Auth) == `{"token":"dynamic"}` {
				dynamicAuthSeen.Store(true)
				return nil
			}
			return errors.New("dynamic denied")
		})
	})
	ts := httptest.NewServer(server)
	defer ts.Close()
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws := connectSocketClientWithAuth(t, ctx, ts.URL, "/dynamic", `{"token":"dynamic"}`)
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()
	if !dynamicAuthSeen.Load() {
		t.Fatal("dynamic namespace middleware did not observe auth payload")
	}
}
