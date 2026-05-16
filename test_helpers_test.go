package sio

import "testing"

func mustNewServer(t testing.TB, config *ServerConfig) *Server {
	t.Helper()
	if config == nil {
		config = &ServerConfig{Secret: "test-secret"}
	} else if config.Secret == "" {
		copy := *config
		copy.Secret = "test-secret"
		config = &copy
	}
	server := NewServer(config)
	if err := server.Run(); err != nil {
		_ = server.Close()
		t.Fatalf("Run failed: %v", err)
	}
	return server
}
