package siotest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"nhooyr.io/websocket"
)

const defaultPath = "/socket.io/"

type ConnectOptions struct {
	Path      string
	Namespace string
	Auth      any
	Header    http.Header
}

type ConnectError struct {
	Message string
	Payload json.RawMessage
}

func (e *ConnectError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "socket.io connect error"
}

type Event struct {
	Name string
	Args []json.RawMessage
}

type Client struct {
	conn      *websocket.Conn
	namespace string
	mu        sync.Mutex
}

func Connect(ctx context.Context, rawURL string, opts ConnectOptions) (*Client, error) {
	return Dial(ctx, rawURL, opts)
}

func Dial(ctx context.Context, rawURL string, opts ConnectOptions) (*Client, error) {
	wsURL, err := websocketURL(rawURL, opts.Path)
	if err != nil {
		return nil, err
	}
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: opts.Header})
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn, namespace: normalizeNamespace(opts.Namespace)}
	if err := client.expectOpen(ctx); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "open failed")
		return nil, err
	}
	packet, err := connectPacket(client.namespace, opts.Auth)
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "auth encode failed")
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageText, packet); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "connect write failed")
		return nil, err
	}
	if err := client.expectConnect(ctx); err != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "connect failed")
		return nil, err
	}
	return client, nil
}

func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close(websocket.StatusNormalClosure, "")
}

func (c *Client) Emit(ctx context.Context, event string, args ...any) error {
	values := make([]any, 0, len(args)+1)
	values = append(values, event)
	values = append(values, args...)
	payload, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("siotest: encode event %q failed: %w", event, err)
	}
	packet := make([]byte, 0, len(payload)+len(c.namespace)+4)
	packet = append(packet, '4', '2')
	packet = appendNamespace(packet, c.namespace)
	packet = append(packet, payload...)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.Write(ctx, websocket.MessageText, packet); err != nil {
		return fmt.Errorf("siotest: write event %q failed: %w", event, err)
	}
	return nil
}

func (c *Client) ReadEvent(ctx context.Context) (Event, error) {
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return Event{}, err
		}
		if string(data) == "2" {
			if err := c.conn.Write(ctx, websocket.MessageText, []byte("3")); err != nil {
				return Event{}, err
			}
			continue
		}
		if !strings.HasPrefix(string(data), "42") {
			continue
		}
		payload := socketPayloadAfterHeader(data[2:])
		var raw []json.RawMessage
		if err := json.Unmarshal(payload, &raw); err != nil {
			return Event{}, fmt.Errorf("siotest: decode event payload %q failed: %w", payload, err)
		}
		if len(raw) == 0 {
			return Event{}, fmt.Errorf("siotest: empty event payload %q", payload)
		}
		var name string
		if err := json.Unmarshal(raw[0], &name); err != nil {
			return Event{}, fmt.Errorf("siotest: event name decode failed: %w", err)
		}
		return Event{Name: name, Args: raw[1:]}, nil
	}
}

func (c *Client) expectOpen(ctx context.Context) error {
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		return err
	}
	if len(data) == 0 || data[0] != '0' {
		return fmt.Errorf("siotest: expected Engine.IO open packet, got %q", data)
	}
	return nil
}

func (c *Client) expectConnect(ctx context.Context) error {
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return err
		}
		s := string(data)
		switch {
		case s == "2":
			if err := c.conn.Write(ctx, websocket.MessageText, []byte("3")); err != nil {
				return err
			}
		case strings.HasPrefix(s, "40"):
			return nil
		case strings.HasPrefix(s, "44"):
			return parseConnectError(data[2:])
		}
	}
}

func websocketURL(rawURL, path string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("siotest: parse url %q failed: %w", rawURL, err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("siotest: unsupported url scheme %q", u.Scheme)
	}
	if path == "" {
		path = defaultPath
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	u.Path = path
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func connectPacket(namespace string, auth any) ([]byte, error) {
	packet := make([]byte, 0, len(namespace)+32)
	packet = append(packet, '4', '0')
	packet = appendNamespace(packet, namespace)
	if auth == nil {
		return packet, nil
	}
	payload, err := json.Marshal(auth)
	if err != nil {
		return nil, fmt.Errorf("siotest: encode auth failed: %w", err)
	}
	packet = append(packet, payload...)
	return packet, nil
}

func appendNamespace(dst []byte, namespace string) []byte {
	if namespace == "" || namespace == "/" {
		return dst
	}
	dst = append(dst, namespace...)
	dst = append(dst, ',')
	return dst
}

func normalizeNamespace(namespace string) string {
	if namespace == "" {
		return "/"
	}
	if namespace[0] != '/' {
		return "/" + namespace
	}
	return namespace
}

func socketPayloadAfterHeader(data []byte) []byte {
	if len(data) > 0 && data[0] == '/' {
		idx := strings.IndexByte(string(data), ',')
		if idx >= 0 {
			data = data[idx+1:]
		}
	}
	for len(data) > 0 && data[0] >= '0' && data[0] <= '9' {
		data = data[1:]
	}
	return data
}

func parseConnectError(data []byte) error {
	payload := socketPayloadAfterHeader(data)
	var parsed struct {
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return &ConnectError{Message: "socket.io connect error", Payload: append([]byte(nil), payload...)}
	}
	return &ConnectError{Message: parsed.Message, Payload: parsed.Data}
}

func AckPacket(id uint64, args ...any) ([]byte, error) {
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	packet := make([]byte, 0, len(payload)+24)
	packet = append(packet, '4', '3')
	packet = strconv.AppendUint(packet, id, 10)
	packet = append(packet, payload...)
	return packet, nil
}
