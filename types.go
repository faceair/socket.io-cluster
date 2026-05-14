package sio

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

const (
	SocketIOProtocolVersion = 5
	EngineIOProtocolVersion = 4
)

const (
	DefaultPath           = "/socket.io/"
	DefaultPingInterval   = 25 * time.Second
	DefaultPingTimeout    = 20 * time.Second
	DefaultConnectTimeout = 45 * time.Second
)

var ErrAckTimeout = errors.New("ack timeout")

type SocketID string
type Room string

// Binary marks an argument as a Socket.IO binary attachment. Plain []byte values
// are still treated as raw JSON when they already contain valid JSON.
type Binary []byte

type Reason string

const (
	ReasonTransportClose            Reason = "transport close"
	ReasonTransportError            Reason = "transport error"
	ReasonPingTimeout               Reason = "ping timeout"
	ReasonServerShuttingDown        Reason = "server shutting down"
	ReasonServerNamespaceDisconnect Reason = "server namespace disconnect"
	ReasonClientNamespaceDisconnect Reason = "client namespace disconnect"
)

type ServerAuthFunc func(w http.ResponseWriter, r *http.Request) bool

type ServerConfig struct {
	Path string
	// Port is the real HTTP listen port of this server, used to derive the
	// cluster AdvertiseURL. It is required unless Cluster.AdvertiseURL or an
	// equivalent port env var is set.
	Port                    string
	Authenticator           ServerAuthFunc
	PingInterval            time.Duration
	PingTimeout             time.Duration
	ConnectTimeout          time.Duration
	AcceptAnyNamespace      bool
	Cluster                 ClusterConfig
	ConnectionStateRecovery ConnectionStateRecoveryConfig
	OnError                 func(error)
}

type ClusterConfig struct {
	// AdvertiseURL is the full base URL that peers use to call this node. When
	// set, it must include scheme, host and port.
	AdvertiseURL string

	// NodeID is optional; empty uses POD_NAME, HOSTNAME, os.Hostname, then a
	// random node prefix.
	NodeID string
	// Peers and HeadlessDNS are optional discovery inputs. Empty means the node
	// still exposes cluster transport but has no remote peers until configured.
	Peers       []string
	HeadlessDNS []string

	RequestTimeout    time.Duration
	HeartbeatInterval time.Duration
	FanoutWorkers     int
}

type ConnectionStateRecoveryConfig struct {
	Enabled                    bool
	MaxDisconnectionDuration   time.Duration
	SkipMiddlewaresOnReconnect bool
}

type Handshake struct {
	Time    time.Time
	Auth    json.RawMessage
	Request *http.Request
}

type NspMiddlewareFunc func(socket ServerSocket, handshake *Handshake) any

type NamespaceConnectionFunc func(socket ServerSocket)
type ServerAnyConnectionFunc func(namespace string, socket ServerSocket)
type ServerNewNamespaceFunc func(namespace *Namespace)
type ServerSocketErrorFunc func(error)
type ServerSocketDisconnectingFunc func(Reason)
type ServerSocketDisconnectFunc func(Reason)

type Socket interface {
	ID() SocketID
	Connected() bool
	Recovered() bool
	Emit(eventName string, v ...any)
	Timeout(timeout time.Duration) Emitter
	OnEvent(eventName string, handler any)
	OnceEvent(eventName string, handler any)
	OffEvent(eventName string, handler ...any)
	OffAll()
}

type ServerSocket interface {
	Socket
	ServerSocketEvents
	Server() *Server
	Namespace() *Namespace
	Join(room ...Room)
	Leave(room Room)
	Rooms() []Room
	Use(f any)
	To(room ...Room) *BroadcastOperator
	In(room ...Room) *BroadcastOperator
	Except(room ...Room) *BroadcastOperator
	Local() *BroadcastOperator
	Broadcast() *BroadcastOperator
	Disconnect(close bool)
}

type ServerSocketEvents interface {
	OnError(f ServerSocketErrorFunc)
	OnceError(f ServerSocketErrorFunc)
	OffError(f ...ServerSocketErrorFunc)
	OnDisconnecting(f ServerSocketDisconnectingFunc)
	OnceDisconnecting(f ServerSocketDisconnectingFunc)
	OffDisconnecting(f ...ServerSocketDisconnectingFunc)
	OnDisconnect(f ServerSocketDisconnectFunc)
	OnceDisconnect(f ServerSocketDisconnectFunc)
	OffDisconnect(f ...ServerSocketDisconnectFunc)
}
