package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	sio "github.com/faceair/socket.io-cluster"
)

type debugServer struct {
	sio     *sio.Server
	podName string
}

func main() {
	port := getenv("PORT", "3000")
	server := sio.NewServer(&sio.ServerConfig{
		Port:               port,
		Secret:             getenv("SIO_CLUSTER_SECRET", "k8s-e2e-secret"),
		AcceptAnyNamespace: true,
		ServerConnectionStateRecovery: sio.ServerConnectionStateRecovery{
			Enabled:                  true,
			MaxDisconnectionDuration: 2 * time.Minute,
		},
		OnError: func(err error) { log.Println("socket.io error:", err) },
	})
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	d := &debugServer{sio: server, podName: getenv("POD_NAME", getenv("HOSTNAME", "unknown"))}
	server.OnConnection(func(socket sio.ServerSocket) {
		socket.Join("csr-room")
		socket.OnEvent("join", func(room string) { socket.Join(sio.Room(room)) })
		socket.OnEvent("leave", func(room string) { socket.Leave(sio.Room(room)) })
		socket.OnEvent("whoami", func(ack func(map[string]string)) {
			ack(map[string]string{"pod": d.podName, "id": string(socket.ID())})
		})
	})

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)
	mux.HandleFunc("/debug/info", d.info)
	mux.HandleFunc("/debug/broadcast", d.broadcast)
	mux.HandleFunc("/debug/room", d.room)
	mux.HandleFunc("/debug/ack", d.ack)

	addr := ":" + port
	log.Printf("socket.io k8s e2e server listening on %s pod=%s", addr, d.podName)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (d *debugServer) info(w http.ResponseWriter, _ *http.Request) {
	service := inferServiceName(os.Getenv("POD_NAME"))
	namespace := os.Getenv("POD_NAMESPACE")
	dnsName := service
	if service != "" && namespace != "" {
		dnsName = service + "." + namespace + ".svc"
	}
	ips, lookupErr := net.LookupHost(dnsName)
	lookupError := ""
	if lookupErr != nil {
		lookupError = lookupErr.Error()
	}
	writeJSON(w, map[string]any{
		"pod":         d.podName,
		"namespace":   namespace,
		"podIP":       os.Getenv("POD_IP"),
		"dnsName":     dnsName,
		"dnsIPs":      ips,
		"dnsError":    lookupError,
		"serviceName": service,
		"metrics":     d.sio.Metrics().Samples,
	})
}

func (d *debugServer) broadcast(w http.ResponseWriter, r *http.Request) {
	event, value := eventValue(r, "k8s-broadcast", "from-"+d.podName)
	d.sio.Emit(event, value)
	writeJSON(w, map[string]string{"status": "ok", "pod": d.podName})
}

func (d *debugServer) room(w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	if room == "" {
		http.Error(w, "missing room", http.StatusBadRequest)
		return
	}
	event, value := eventValue(r, "k8s-room", "from-"+d.podName)
	d.sio.To(sio.Room(room)).Emit(event, value)
	writeJSON(w, map[string]string{"status": "ok", "pod": d.podName, "room": room})
}

func (d *debugServer) ack(w http.ResponseWriter, r *http.Request) {
	event, value := eventValue(r, "k8s-ack", "from-"+d.podName)
	done := make(chan error, 1)
	d.sio.Timeout(5*time.Second).Emit(event, value, func(err error) { done <- err })
	select {
	case err := <-done:
		if err != nil {
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
			return
		}
		writeJSON(w, map[string]string{"status": "ok", "pod": d.podName})
	case <-time.After(6 * time.Second):
		http.Error(w, "ack endpoint timed out", http.StatusGatewayTimeout)
	}
}

func eventValue(r *http.Request, defaultEvent, defaultValue string) (string, string) {
	event := r.URL.Query().Get("event")
	if event == "" {
		event = defaultEvent
	}
	value := r.URL.Query().Get("value")
	if value == "" {
		value = defaultValue
	}
	return event, value
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Println("write json failed:", err)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func inferServiceName(podName string) string {
	parts := strings.Split(strings.TrimSpace(podName), "-")
	if len(parts) < 2 {
		return ""
	}
	last := parts[len(parts)-1]
	if allDigits(last) {
		return strings.Join(parts[:len(parts)-1], "-")
	}
	if len(parts) >= 3 && isHash(parts[len(parts)-2], 8, 12) && isHash(last, 5, 5) {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return ""
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func isHash(value string, minLen, maxLen int) bool {
	if len(value) < minLen || len(value) > maxLen {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}
