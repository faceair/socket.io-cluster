package siotest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	sio "github.com/faceair/socket.io-cluster"
	"github.com/faceair/socket.io-cluster/siotest"
)

func TestConnectAuthAndReadEvent(t *testing.T) {
	server := sio.NewServer(&sio.ServerConfig{Port: "3000", Secret: "test-secret"})
	if err := server.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	server.Use(func(_ sio.ServerSocket, handshake *sio.Handshake) any {
		var auth struct {
			Token       string `json:"token"`
			WorkspaceID string `json:"workspaceId"`
		}
		if err := json.Unmarshal(handshake.Auth, &auth); err != nil {
			return err
		}
		if auth.Token != "good-token" || auth.WorkspaceID != "workspace-1" {
			return errors.New("unauthorized")
		}
		return nil
	})
	server.OnConnection(func(socket sio.ServerSocket) { socket.Join("room-a") })
	ts := httptest.NewServer(server)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := siotest.Connect(ctx, ts.URL, siotest.ConnectOptions{Auth: map[string]string{"token": "good-token", "workspaceId": "workspace-1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close(ctx) }()

	server.To("room-a").Emit("hello", "world")
	event, err := client.ReadEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "hello" || len(event.Args) != 1 || string(event.Args[0]) != `"world"` {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestConnectError(t *testing.T) {
	server := sio.NewServer(&sio.ServerConfig{Port: "3000", Secret: "test-secret"})
	if err := server.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	server.Use(func(_ sio.ServerSocket, _ *sio.Handshake) any { return errors.New("unauthorized") })
	ts := httptest.NewServer(server)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := siotest.Connect(ctx, ts.URL, siotest.ConnectOptions{Auth: map[string]string{"token": "bad-token"}})
	if err == nil {
		t.Fatal("expected connect error")
	}
	var connectErr *siotest.ConnectError
	if !errors.As(err, &connectErr) {
		t.Fatalf("expected ConnectError, got %T %v", err, err)
	}
	if connectErr.Message != "unauthorized" {
		t.Fatalf("connect error message=%q", connectErr.Message)
	}
}
