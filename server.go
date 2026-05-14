package sio

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	emptyJSONArrayBytes = []byte("[]")
	forbiddenBodyBytes  = []byte(`{"code":4,"message":"Forbidden"}`)
)

type Server struct {
	path                    string
	acceptAnyNsp            bool
	authenticator           ServerAuthFunc
	pingInterval            time.Duration
	pingTimeout             time.Duration
	connectTimeout          time.Duration
	onError                 func(error)
	recoverySkipMiddlewares bool

	ids      *idGenerator
	lc       *lifecycle
	metrics  *metricsRecorder
	recovery *recoveryStore

	mu         sync.RWMutex
	namespaces map[string]*Namespace
	conns      map[string]*engineConn

	ackMu       sync.Mutex
	ackSeq      uint64
	broadcasts  map[uint64]*broadcastAckTracker
	cluster     *clusterNode
	anyConn     callbackStore[ServerAnyConnectionFunc]
	newNspHooks callbackStore[ServerNewNamespaceFunc]
}

func NewServer(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = new(ServerConfig)
	}
	path := config.Path
	if path == "" {
		path = DefaultPath
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	s := &Server{
		path:                    path,
		acceptAnyNsp:            config.AcceptAnyNamespace,
		authenticator:           config.Authenticator,
		pingInterval:            cmp.Or(config.PingInterval, DefaultPingInterval),
		pingTimeout:             cmp.Or(config.PingTimeout, DefaultPingTimeout),
		connectTimeout:          cmp.Or(config.ConnectTimeout, DefaultConnectTimeout),
		onError:                 config.OnError,
		recoverySkipMiddlewares: config.ConnectionStateRecovery.SkipMiddlewaresOnReconnect,
		ids:                     newIDGenerator(resolveNodeID(config.Cluster.NodeID)),
		lc:                      newLifecycle(context.Background()),
		metrics:                 newMetricsRecorder(),
		namespaces:              make(map[string]*Namespace),
		conns:                   make(map[string]*engineConn),
		broadcasts:              make(map[uint64]*broadcastAckTracker),
	}
	if config.ConnectionStateRecovery.Enabled {
		s.recovery = newRecoveryStore(cmp.Or(config.ConnectionStateRecovery.MaxDisconnectionDuration, 2*time.Minute))
	}
	s.namespaces["/"] = newNamespace("/", s)
	cluster, err := newClusterNode(s, config.Port, config.Cluster)
	if err != nil {
		return nil, err
	}
	s.cluster = cluster
	if s.recovery != nil {
		s.lc.start("recovery-sweeper", s.recoverySweepLoop)
	}
	s.lc.start("ack-sweeper", s.ackSweepLoop)
	return s, nil
}

func MustNewServer(config *ServerConfig) *Server {
	s, err := NewServer(config)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *Server) Of(namespace string) *Namespace {
	if namespace == "" || namespace[0] != '/' {
		namespace = "/" + namespace
	}
	s.mu.Lock()
	n := s.namespaces[namespace]
	created := false
	if n == nil {
		n = newNamespace(namespace, s)
		s.namespaces[namespace] = n
		created = true
	}
	s.mu.Unlock()
	if created && namespace != "/" {
		s.newNspHooks.forEach(func(f ServerNewNamespaceFunc) { f(n) })
	}
	return n
}

func (s *Server) Use(f NspMiddlewareFunc)                          { s.Of("/").Use(f) }
func (s *Server) OnConnection(f NamespaceConnectionFunc)           { s.Of("/").OnConnection(f) }
func (s *Server) OnceConnection(f NamespaceConnectionFunc)         { s.Of("/").OnceConnection(f) }
func (s *Server) OffConnection(f ...NamespaceConnectionFunc)       { s.Of("/").OffConnection(f...) }
func (s *Server) Emit(eventName string, v ...any)                  { s.Of("/").Emit(eventName, v...) }
func (s *Server) To(room ...Room) *BroadcastOperator               { return s.Of("/").To(room...) }
func (s *Server) In(room ...Room) *BroadcastOperator               { return s.To(room...) }
func (s *Server) Except(room ...Room) *BroadcastOperator           { return s.Of("/").Except(room...) }
func (s *Server) Local() *BroadcastOperator                        { return s.Of("/").Local() }
func (s *Server) Timeout(timeout time.Duration) *BroadcastOperator { return s.Of("/").Timeout(timeout) }
func (s *Server) Sockets() []ServerSocket                          { return s.Of("/").Sockets() }
func (s *Server) FetchSockets(room ...Room) []ServerSocket         { return s.Of("/").FetchSockets(room...) }
func (s *Server) SocketsJoin(room ...Room)                         { s.Of("/").SocketsJoin(room...) }
func (s *Server) SocketsLeave(room ...Room)                        { s.Of("/").SocketsLeave(room...) }
func (s *Server) DisconnectSockets(close bool)                     { s.Of("/").DisconnectSockets(close) }
func (s *Server) ServerSideEmit(eventName string, v ...any) {
	s.Of("/").ServerSideEmit(eventName, v...)
}
func (s *Server) OnServerSideEmit(eventName string, handler any) {
	s.Of("/").OnServerSideEmit(eventName, handler)
}
func (s *Server) OnAnyConnection(f ServerAnyConnectionFunc) { s.anyConn.add(f, false) }
func (s *Server) OnNewNamespace(f ServerNewNamespaceFunc)   { s.newNspHooks.add(f, false) }

func (s *Server) Run() error { return nil }

func (s *Server) Close() error {
	s.mu.RLock()
	conns := make([]*engineConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.RUnlock()
	for _, c := range conns {
		c.close(ReasonServerShuttingDown)
	}
	if s.cluster != nil {
		s.cluster.close()
	}
	return s.lc.stop(5 * time.Second)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, strings.TrimSuffix(s.path, "/")) {
		http.NotFound(w, r)
		return
	}
	if s.authenticator != nil && !s.authenticator(w, r) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(forbiddenBodyBytes)
		return
	}
	if s.cluster != nil && r.URL.Query().Get("transport") == "cluster" {
		s.cluster.serveHTTP(w, r)
		return
	}
	transport := r.URL.Query().Get("transport")
	switch transport {
	case "websocket":
		s.serveWebSocket(w, r)
	case "polling":
		s.servePolling(w, r)
	default:
		writeEngineError(w, 0, "Transport unknown")
	}
}

func (s *Server) getNamespace(name string) (*Namespace, bool) {
	if name == "" {
		name = "/"
	}
	s.mu.RLock()
	n := s.namespaces[name]
	s.mu.RUnlock()
	if n != nil {
		return n, true
	}
	if s.acceptAnyNsp {
		return s.Of(name), true
	}
	return nil, false
}

func (s *Server) addConn(c *engineConn) {
	s.mu.Lock()
	s.conns[c.sid] = c
	s.mu.Unlock()
	s.metrics.engineConnectionsOpened.Add(1)
	s.metrics.engineConnectionsActive.Add(1)
}

func (s *Server) getConn(sid string) (*engineConn, bool) {
	s.mu.RLock()
	c := s.conns[sid]
	s.mu.RUnlock()
	return c, c != nil
}

func (s *Server) removeConn(sid string) {
	s.mu.Lock()
	if _, ok := s.conns[sid]; ok {
		delete(s.conns, sid)
		s.metrics.engineConnectionsClosed.Add(1)
		s.metrics.engineConnectionsActive.Add(-1)
	}
	s.mu.Unlock()
}

func (s *Server) reportError(err error) {
	if err != nil && s.onError != nil {
		s.onError(err)
	}
}

func (s *Server) newBroadcastAck(handler *ackHandler, timeout time.Duration) *broadcastAckTracker {
	s.ackMu.Lock()
	s.ackSeq++
	id := uint64(1_000_000_000) + s.ackSeq
	tracker := &broadcastAckTracker{id: id, server: s, handler: handler, deadline: time.Now().Add(timeout)}
	if timeout == 0 {
		tracker.deadline = time.Time{}
	}
	s.broadcasts[id] = tracker
	s.metrics.acksRegistered.Add(1)
	s.ackMu.Unlock()
	return tracker
}

func (s *Server) getBroadcastAck(id uint64) (*broadcastAckTracker, bool) {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	t := s.broadcasts[id]
	return t, t != nil
}

func (s *Server) removeBroadcastAck(id uint64) {
	s.ackMu.Lock()
	delete(s.broadcasts, id)
	s.ackMu.Unlock()
}

func (s *Server) ackSweepLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.sweepAcks(now)
		}
	}
}

func (s *Server) recoverySweepLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.recovery.mu.Lock()
			s.recovery.pruneLocked(now)
			s.recovery.mu.Unlock()
		}
	}
}

func (s *Server) sweepAcks(now time.Time) {
	s.mu.RLock()
	for _, n := range s.namespaces {
		n.adapter.apply(broadcastOptions{}, func(socket *serverSocket) { socket.sweepAcks(now) })
	}
	s.mu.RUnlock()
	s.ackMu.Lock()
	for id, tracker := range s.broadcasts {
		if !tracker.deadline.IsZero() && !now.Before(tracker.deadline) {
			delete(s.broadcasts, id)
			s.metrics.acksTimedOut.Add(1)
			tracker.timeout()
		}
	}
	s.ackMu.Unlock()
}

type namespaceStat struct {
	namespace   string
	sockets     int
	rooms       int
	memberships int
}

func (s *Server) namespaceStats() []namespaceStat {
	s.mu.RLock()
	namespaces := make([]*Namespace, 0, len(s.namespaces))
	for _, n := range s.namespaces {
		namespaces = append(namespaces, n)
	}
	s.mu.RUnlock()
	out := make([]namespaceStat, 0, len(namespaces))
	for _, n := range namespaces {
		sockets, rooms, memberships := n.adapter.stats()
		out = append(out, namespaceStat{
			namespace:   n.name,
			sockets:     sockets,
			rooms:       rooms,
			memberships: memberships,
		})
	}
	return out
}

func (s *Server) serverSideEmit(namespace, eventName string, v ...any) {
	if s.cluster != nil {
		s.cluster.serverSideEmit(namespace, eventName, v...)
	}
}

func writeEngineError(w http.ResponseWriter, code int, message string) {
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(w, `{"code":%d,"message":%q}`, code, message)
}

type broadcastAckTarget interface {
	accept(JSONArrayView, [][]byte)
}

type broadcastAckTracker struct {
	id       uint64
	server   *Server
	handler  *ackHandler
	deadline time.Time
	mu       sync.Mutex
	expected int
	got      int
	done     bool
}

func (t *broadcastAckTracker) addExpected(n int) {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.expected += n
	done := t.expected > 0 && t.got >= t.expected
	if done {
		t.done = true
	}
	t.mu.Unlock()
	if done {
		t.server.removeBroadcastAck(t.id)
		t.server.metrics.acksResolved.Add(1)
		_ = t.handler.call(JSONArrayView{data: emptyJSONArrayBytes, pos: 1}, nil)
	}
}

func (t *broadcastAckTracker) accept(args JSONArrayView, _ [][]byte) {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.got++
	done := t.expected > 0 && t.got >= t.expected
	if done {
		t.done = true
	}
	t.mu.Unlock()
	if done {
		t.server.removeBroadcastAck(t.id)
		t.server.metrics.acksResolved.Add(1)
		_ = t.handler.call(JSONArrayView{data: emptyJSONArrayBytes, pos: 1}, nil)
	}
}

func (t *broadcastAckTracker) maybeFinish() {
	t.mu.Lock()
	done := !t.done && t.expected == 0
	if done {
		t.done = true
	}
	t.mu.Unlock()
	if done {
		t.server.removeBroadcastAck(t.id)
		t.server.metrics.acksResolved.Add(1)
		_ = t.handler.call(JSONArrayView{data: emptyJSONArrayBytes, pos: 1}, nil)
	}
}

func (t *broadcastAckTracker) timeout() {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.done = true
	t.mu.Unlock()
	t.handler.timeout()
}
