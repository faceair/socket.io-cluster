package sio

import "testing"

func mustNewServer(t testing.TB, config *ServerConfig) *Server {
	t.Helper()
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	return server
}
