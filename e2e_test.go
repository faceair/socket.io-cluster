package sio

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSocketIOClientE2E(t *testing.T) {
	if os.Getenv("SIO_JS_E2E") != "1" {
		t.Skip("set SIO_JS_E2E=1 to run JS socket.io-client e2e")
	}
	server := mustNewServer(t, &ServerConfig{
		AcceptAnyNamespace: true,
		Port:               "3000",
		Secret:             "test-secret",
		OnError:            func(err error) { t.Log(err) },
	})
	var authMu sync.Mutex
	authCounts := map[string]int{}
	server.Use(func(_ ServerSocket, handshake *Handshake) any {
		var auth struct {
			Token       string `json:"token"`
			WorkspaceID string `json:"workspaceId"`
		}
		if err := json.Unmarshal(handshake.Auth, &auth); err != nil {
			return err
		}
		switch auth.Token {
		case "good-token":
			if auth.WorkspaceID != "workspace-1" {
				return errors.New("unauthorized")
			}
		case "reconnect-token":
			if auth.WorkspaceID != "workspace-reconnect" {
				return errors.New("unauthorized")
			}
		default:
			return errors.New("unauthorized")
		}
		authMu.Lock()
		authCounts[auth.Token]++
		authMu.Unlock()
		return nil
	})
	server.OnConnection(func(socket ServerSocket) {
		socket.OnEvent("client-event", func(name string, ack func(string)) {
			ack("ack:" + name)
		})
		socket.OnEvent("transport", func(name string) { socket.Emit("server-event", "hello:"+name) })
		socket.OnEvent("server-binary", func() { socket.Emit("binary-event", Binary("from-server")) })
		socket.OnEvent("client-binary", func(data []byte, ack func(Binary)) {
			ack(Binary("ack-bin:" + string(data)))
		})
		socket.OnEvent("auth-count", func(token string, ack func(int)) {
			authMu.Lock()
			count := authCounts[token]
			authMu.Unlock()
			ack(count)
		})
		socket.OnEvent("force-transport-close", func() {
			if s, ok := socket.(*serverSocket); ok {
				time.AfterFunc(10*time.Millisecond, func() { s.conn.close(ReasonTransportClose) })
			}
		})
	})
	workspace := server.Of("/workspace")
	workspace.Use(func(_ ServerSocket, handshake *Handshake) any {
		var auth struct {
			Token       string `json:"token"`
			WorkspaceID string `json:"workspaceId"`
		}
		if err := json.Unmarshal(handshake.Auth, &auth); err != nil {
			return err
		}
		if auth.Token != "workspace-token" || auth.WorkspaceID != "workspace-1" {
			return errors.New("workspace unauthorized")
		}
		return nil
	})
	workspace.OnConnection(func(socket ServerSocket) {
		socket.OnEvent("namespace-event", func(value string, ack func(string)) { ack("namespace-ack:" + value) })
	})
	server.OnNewNamespace(func(namespace *Namespace) {
		if namespace.Name() != "/dynamic" {
			return
		}
		namespace.Use(func(_ ServerSocket, handshake *Handshake) any {
			var auth struct {
				Token       string `json:"token"`
				WorkspaceID string `json:"workspaceId"`
			}
			if err := json.Unmarshal(handshake.Auth, &auth); err != nil {
				return err
			}
			if auth.Token != "dynamic-token" || auth.WorkspaceID != "workspace-1" {
				return errors.New("dynamic unauthorized")
			}
			return nil
		})
	})
	ts := httptest.NewServer(server)
	defer ts.Close()
	defer func() { _ = server.Close() }()

	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "socketio-client.mjs")
	cmd.Dir = filepath.Join(root, "test/e2e")
	cmd.Env = append(os.Environ(), "SIO_E2E_URL="+ts.URL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node e2e failed: %v\n%s", err, out)
	}
}
