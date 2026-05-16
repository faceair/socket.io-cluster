package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

type metricSample struct {
	Name  string  `json:"Name"`
	Value float64 `json:"Value"`
}

type infoResponse struct {
	Pod     string         `json:"pod"`
	PodIP   string         `json:"podIP"`
	Metrics []metricSample `json:"metrics"`
}

type event struct {
	Name   string
	Values []string
	Offset string
}

type client struct {
	name   string
	base   string
	conn   *websocket.Conn
	pid    string
	events chan event
	mu     sync.Mutex
}

func main() {
	podA := flag.String("pod-a", "", "base URL of pod A port-forward, e.g. http://127.0.0.1:30001")
	podB := flag.String("pod-b", "", "base URL of pod B port-forward, e.g. http://127.0.0.1:30002")
	flag.Parse()
	if *podA == "" || *podB == "" {
		fatalf("--pod-a and --pod-b are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	infoA := waitPeerReady(ctx, *podA, "pod-a")
	infoB := waitPeerReady(ctx, *podB, "pod-b")
	printf("pods ready: A=%s(%s) B=%s(%s)", infoA.Pod, infoA.PodIP, infoB.Pod, infoB.PodIP)

	clientA := mustConnect(ctx, "client-a", *podA, "")
	defer clientA.close()
	clientB := mustConnect(ctx, "client-b", *podB, "")
	defer clientB.close()

	mustEventuallyHTTPEvent(ctx, *podA+"/debug/broadcast?event=k8s-broadcast&value=from-a", clientB, "k8s-broadcast", "from-a")
	printf("cross-pod broadcast ok")

	clientB.emit(`42["join","k8s-room"]`)
	time.Sleep(300 * time.Millisecond)
	mustEventuallyHTTPEvent(ctx, *podA+"/debug/room?room=k8s-room&event=k8s-room&value=room-from-a", clientB, "k8s-room", "room-from-a")
	printf("cross-pod room broadcast ok")

	mustEventuallyHTTP(ctx, *podA+"/debug/ack?event=k8s-ack&value=ack-from-a")
	printf("cross-pod broadcast ack ok")

	csr := mustConnect(ctx, "csr-owner", *podA, "")
	mustHTTP(ctx, *podA+"/debug/room?room=csr-room&event=k8s-csr&value=before")
	before := csr.waitEvent(ctx, "k8s-csr", "before")
	if before.Offset == "" {
		fatalf("csr before event did not include offset")
	}
	pid := csr.pid
	csr.close()
	time.Sleep(800 * time.Millisecond)
	mustHTTP(ctx, *podB+"/debug/room?room=csr-room&event=k8s-csr&value=missed")
	recovered := mustConnect(ctx, "csr-recovered", *podB, fmt.Sprintf(`{"pid":"%s","offset":"%s"}`, pid, before.Offset))
	defer recovered.close()
	recovered.waitEvent(ctx, "k8s-csr", "missed")
	printf("cross-node csr replay ok")

	printf("all k8s socket.io cluster checks passed")
}

func waitPeerReady(ctx context.Context, base, label string) infoResponse {
	var lastErr error
	for ctx.Err() == nil {
		info, err := getInfo(ctx, base)
		if err == nil && clusterPeers(info) >= 1 {
			return info
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("%s has %d cluster peers", label, clusterPeers(info))
		}
		time.Sleep(500 * time.Millisecond)
	}
	fatalf("%s did not become peer-ready: %v", label, lastErr)
	return infoResponse{}
}

func getInfo(ctx context.Context, base string) (infoResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/debug/info", nil)
	if err != nil {
		return infoResponse{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return infoResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return infoResponse{}, fmt.Errorf("GET %s/debug/info returned %s: %s", base, resp.Status, body)
	}
	var info infoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return infoResponse{}, err
	}
	return info, nil
}

func clusterPeers(info infoResponse) int {
	for _, sample := range info.Metrics {
		if sample.Name == "sio_cluster_peers" {
			return int(sample.Value)
		}
	}
	return 0
}

func mustConnect(ctx context.Context, name, base, auth string) *client {
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/socket.io/?EIO=4&transport=websocket"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		fatalf("%s connect failed: %v", name, err)
	}
	c := &client{name: name, base: base, conn: conn, events: make(chan event, 32)}
	c.handshake(ctx, auth)
	go c.readLoop()
	return c
}

func (c *client) handshake(ctx context.Context, auth string) {
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		fatalf("%s read open failed: %v", c.name, err)
	}
	if len(data) == 0 || data[0] != '0' {
		fatalf("%s bad open packet %q", c.name, data)
	}
	connect := "40"
	if auth != "" {
		connect += auth
	}
	c.emit(connect)
	for {
		_, data, err = c.conn.Read(ctx)
		if err != nil {
			fatalf("%s read connect failed: %v", c.name, err)
		}
		text := string(data)
		if text == "2" {
			c.emit("3")
			continue
		}
		if strings.HasPrefix(text, "40") {
			var payload struct {
				PID string `json:"pid"`
			}
			if len(text) > 2 {
				if err := json.Unmarshal([]byte(text[2:]), &payload); err != nil {
					fatalf("%s decode connect payload failed: %v packet=%q", c.name, err, text)
				}
			}
			c.pid = payload.PID
			printf("%s connected pid=%s", c.name, c.pid)
			return
		}
	}
}

func (c *client) readLoop() {
	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}
		text := string(data)
		switch {
		case text == "2":
			c.emit("3")
		case strings.HasPrefix(text, "42"):
			e, ackID, err := parseEventPacket(text)
			if err != nil {
				printf("%s ignored malformed event %q: %v", c.name, text, err)
				continue
			}
			if ackID != "" {
				c.emit("43" + ackID + `["ack-` + c.name + `"]`)
			}
			select {
			case c.events <- e:
			default:
				printf("%s event channel full, dropped %s", c.name, e.Name)
			}
		}
	}
}

func parseEventPacket(packet string) (event, string, error) {
	i := 2
	for i < len(packet) && packet[i] >= '0' && packet[i] <= '9' {
		i++
	}
	ackID := packet[2:i]
	if i >= len(packet) || packet[i] != '[' {
		return event{}, "", fmt.Errorf("missing JSON array")
	}
	var raw []json.RawMessage
	if err := json.Unmarshal([]byte(packet[i:]), &raw); err != nil {
		return event{}, "", err
	}
	if len(raw) == 0 {
		return event{}, ackID, fmt.Errorf("empty event array")
	}
	var name string
	if err := json.Unmarshal(raw[0], &name); err != nil {
		return event{}, ackID, err
	}
	e := event{Name: name}
	for _, item := range raw[1:] {
		var s string
		if err := json.Unmarshal(item, &s); err == nil {
			e.Values = append(e.Values, s)
			continue
		}
		e.Values = append(e.Values, string(item))
	}
	if len(e.Values) != 0 {
		last := e.Values[len(e.Values)-1]
		if _, err := fmt.Sscanf(last, "%d", new(uint64)); err == nil {
			e.Offset = last
		}
	}
	return e, ackID, nil
}

func (c *client) waitEvent(ctx context.Context, name, value string) event {
	for {
		select {
		case e := <-c.events:
			if e.Name != name {
				continue
			}
			if value == "" || contains(e.Values, value) {
				return e
			}
		case <-ctx.Done():
			fatalf("%s timed out waiting for event %s value %q", c.name, name, value)
		}
	}
}

func (c *client) waitEventWithin(ctx context.Context, name, value string, timeout time.Duration) (event, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case e := <-c.events:
			if e.Name != name {
				continue
			}
			if value == "" || contains(e.Values, value) {
				return e, true
			}
		case <-ctx.Done():
			return event{}, false
		}
	}
}

func (c *client) emit(packet string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageText, []byte(packet)); err != nil {
		fatalf("%s write %q failed: %v", c.name, packet, err)
	}
}

func (c *client) close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.Close(websocket.StatusNormalClosure, "transport close")
}

func mustHTTP(ctx context.Context, rawURL string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		fatalf("parse URL %q failed: %v", rawURL, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		fatalf("create request %q failed: %v", rawURL, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatalf("GET %s failed: %v", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fatalf("GET %s returned %s: %s", rawURL, resp.Status, body)
	}
}

func mustEventuallyHTTP(ctx context.Context, rawURL string) {
	var lastErr error
	for ctx.Err() == nil {
		lastErr = tryHTTP(ctx, rawURL)
		if lastErr == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	fatalf("GET %s did not succeed: %v", rawURL, lastErr)
}

func mustEventuallyHTTPEvent(ctx context.Context, rawURL string, c *client, name, value string) {
	var lastErr error
	for ctx.Err() == nil {
		lastErr = tryHTTP(ctx, rawURL)
		if lastErr == nil {
			if _, ok := c.waitEventWithin(ctx, name, value, time.Second); ok {
				return
			}
			lastErr = fmt.Errorf("event %s value %q was not delivered", name, value)
		}
		time.Sleep(500 * time.Millisecond)
	}
	fatalf("GET %s did not deliver %s value %q: %v", rawURL, name, value, lastErr)
}

func tryHTTP(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse URL %q failed: %w", rawURL, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("create request %q failed: %w", rawURL, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s failed: %w", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s returned %s: %s", rawURL, resp.Status, body)
	}
	return nil
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func printf(format string, args ...any) {
	fmt.Printf("[k8s-e2e] "+format+"\n", args...)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[k8s-e2e] "+format+"\n", args...)
	os.Exit(1)
}
