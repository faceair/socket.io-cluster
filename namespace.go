package sio

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Namespace struct {
	name    string
	server  *Server
	adapter *localAdapter

	mu          sync.RWMutex
	middlewares []NspMiddlewareFunc
	connections callbackStore[NamespaceConnectionFunc]
	handlers    *eventHandlers
}

func newNamespace(name string, server *Server) *Namespace {
	return &Namespace{name: name, server: server, adapter: newLocalAdapter(), handlers: newEventHandlers()}
}

func (n *Namespace) Name() string { return n.name }

func (n *Namespace) Use(f NspMiddlewareFunc) {
	n.mu.Lock()
	n.middlewares = append(n.middlewares, f)
	n.mu.Unlock()
}

func (n *Namespace) OnConnection(f NamespaceConnectionFunc)     { n.connections.add(f, false) }
func (n *Namespace) OnceConnection(f NamespaceConnectionFunc)   { n.connections.add(f, true) }
func (n *Namespace) OffConnection(f ...NamespaceConnectionFunc) { n.connections.remove(f...) }

func (n *Namespace) Emit(eventName string, v ...any)    { n.newBroadcastOperator().Emit(eventName, v...) }
func (n *Namespace) To(room ...Room) *BroadcastOperator { return n.newBroadcastOperator().To(room...) }
func (n *Namespace) In(room ...Room) *BroadcastOperator { return n.To(room...) }
func (n *Namespace) Except(room ...Room) *BroadcastOperator {
	return n.newBroadcastOperator().Except(room...)
}
func (n *Namespace) Local() *BroadcastOperator { return n.newBroadcastOperator().Local() }
func (n *Namespace) Timeout(timeout time.Duration) *BroadcastOperator {
	return n.newBroadcastOperator().Timeout(timeout)
}
func (n *Namespace) Sockets() []ServerSocket { return n.adapter.matchingSockets(broadcastOptions{}) }
func (n *Namespace) FetchSockets(room ...Room) []ServerSocket {
	return n.newBroadcastOperator().To(room...).FetchSockets()
}
func (n *Namespace) SocketsJoin(room ...Room)     { n.newBroadcastOperator().SocketsJoin(room...) }
func (n *Namespace) SocketsLeave(room ...Room)    { n.newBroadcastOperator().SocketsLeave(room...) }
func (n *Namespace) DisconnectSockets(close bool) { n.newBroadcastOperator().DisconnectSockets(close) }
func (n *Namespace) ServerSideEmit(eventName string, v ...any) {
	n.server.serverSideEmit(n.name, eventName, v...)
}
func (n *Namespace) OnServerSideEmit(eventName string, handler any) {
	if err := n.handlers.add(eventName, handler, false); err != nil {
		panic(err)
	}
}

func (n *Namespace) newBroadcastOperator() *BroadcastOperator {
	return &BroadcastOperator{namespace: n, opts: broadcastOptions{}}
}

func (n *Namespace) add(conn *engineConn, auth json.RawMessage, reqTime time.Time) (*serverSocket, error) {
	now := reqTime
	if now.IsZero() {
		now = time.Now()
	}
	var session *recoverySession
	var replay []recoveryPacket
	var recovered bool
	sessionAttached := false
	replayReleased := false
	defer func() {
		if recovered && !sessionAttached {
			session.release()
		}
		if !replayReleased {
			releaseReplayPackets(replay)
		}
	}()
	pid := ""
	if n.server.recovery != nil {
		var recovery recoveryAuth
		if len(auth) != 0 {
			_ = json.Unmarshal(auth, &recovery)
		}
		if recovery.PID != "" && recovery.Offset != "" {
			session, replay, recovered = n.server.recovery.recover(n.name, recovery.PID, recovery.Offset, now)
			if !recovered && n.server.cluster != nil {
				session, replay, recovered = n.server.cluster.recoverCSR(n.name, recovery.PID, recovery.Offset)
			}
		}
		if recovered {
			pid = session.pid
		} else {
			pid = string(n.server.ids.next())
		}
	}
	sid := n.server.ids.next()
	if recovered {
		sid = session.sid
	}
	socket := newServerSocket(n.server, conn, n, sid)
	socket.pid = pid
	socket.recovered = recovered
	handshake := &Handshake{Time: now, Auth: auth, Request: conn.request}
	n.mu.RLock()
	middlewares := append([]NspMiddlewareFunc(nil), n.middlewares...)
	n.mu.RUnlock()
	if !recovered || !n.server.recoverySkipMiddlewares {
		for _, mw := range middlewares {
			if v := mw(socket, handshake); v != nil {
				return nil, fmt.Errorf("sio: namespace %s middleware rejected connection: %v", n.name, v)
			}
		}
	}
	if recovered {
		socket.recoverySession = session
		sessionAttached = true
	}
	n.adapter.addSocket(socket)
	if recovered {
		n.adapter.addAllOwned(socket.id, socket, session.rooms)
		n.server.metrics.socketsRecovered.Add(1)
	}
	conn.addSocket(socket)
	socket.connected.Store(true)
	n.server.metrics.socketsConnected.Add(1)
	n.server.metrics.socketsActive.Add(1)
	socket.sendConnect()
	n.connections.forEach(func(f NamespaceConnectionFunc) { f(socket) })
	for _, packet := range replay {
		conn.sendSocketPayload(packet.packet, packet.attachmentViews())
		if packet.releaseAfterSend {
			packet.release()
		}
	}
	replayReleased = true
	return socket, nil
}

func (n *Namespace) remove(socket *serverSocket) {
	n.adapter.removeSocket(socket.id)
	n.server.metrics.socketsClosed.Add(1)
	n.server.metrics.socketsActive.Add(-1)
}
