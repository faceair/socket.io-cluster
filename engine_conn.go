package sio

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"
)

type engineConn struct {
	server  *Server
	sid     string
	request *http.Request
	lc      *lifecycle

	wsMu sync.RWMutex
	ws   *websocket.Conn

	poll *pollQueue
	send chan *engineFrame

	mu            sync.RWMutex
	socketsByNsp  map[string]*serverSocket
	broadcastAcks map[uint64]broadcastAckTarget
	closed        atomic.Bool

	pendingBinary *pendingBinaryPacket
}

type pendingBinaryPacket struct {
	packet PacketView
	binary *byteBatch
}

var (
	engineProbePongPacket = []byte("3probe")
	engineOKPacket        = []byte("ok")
	enginePongPacket      = []byte("3")
	enginePingPacket      = []byte("2")
	pollingSeparator      = []byte{0x1e}
	pollingBase64Prefix   = []byte{'b'}
)

type pollQueue struct {
	mu     sync.Mutex
	ready  chan struct{}
	frames []*engineFrame
}

func newPollQueue() *pollQueue { return &pollQueue{ready: make(chan struct{}, 1)} }

func (q *pollQueue) add(f *engineFrame) {
	q.mu.Lock()
	q.frames = append(q.frames, f)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *pollQueue) take(timeout time.Duration) []*engineFrame {
	q.mu.Lock()
	if len(q.frames) > 0 {
		frames := q.frames
		q.frames = nil
		q.mu.Unlock()
		return frames
	}
	q.mu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-q.ready:
	case <-timer.C:
	}
	q.mu.Lock()
	frames := q.frames
	q.frames = nil
	q.mu.Unlock()
	return frames
}

func newEngineConn(server *Server, sid string, r *http.Request) *engineConn {
	return &engineConn{
		server:        server,
		sid:           sid,
		request:       r,
		lc:            newLifecycle(server.lc.context()),
		poll:          newPollQueue(),
		send:          make(chan *engineFrame, 256),
		socketsByNsp:  make(map[string]*serverSocket),
		broadcastAcks: make(map[uint64]broadcastAckTarget),
	}
}

func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		c := newEngineConn(s, string(s.ids.next()), r)
		s.addConn(c)
		c.acceptFreshWebSocket(w, r)
		return
	}
	c, ok := s.getConn(sid)
	if !ok {
		writeEngineError(w, 1, "Session ID unknown")
		return
	}
	c.acceptUpgradeWebSocket(w, r)
}

func (c *engineConn) acceptFreshWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		c.server.reportError(fmt.Errorf("engine.io: websocket accept for sid %s failed: %w", c.sid, err))
		c.server.removeConn(c.sid)
		return
	}
	c.ws = ws
	open := c.openPacket()
	if err := ws.Write(r.Context(), websocket.MessageText, open.B); err != nil {
		open.Release()
		c.server.reportError(fmt.Errorf("engine.io: websocket open write for sid %s failed: %w", c.sid, err))
		c.close(ReasonTransportError)
		return
	}
	open.Release()
	c.startWebSocketLoops()
}

func (c *engineConn) acceptUpgradeWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		c.server.reportError(fmt.Errorf("engine.io: websocket upgrade accept for sid %s failed: %w", c.sid, err))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	_, data, err := ws.Read(ctx)
	if err != nil || string(data) != "2probe" {
		_ = ws.Close(websocket.StatusProtocolError, "invalid probe")
		return
	}
	if err := ws.Write(ctx, websocket.MessageText, engineProbePongPacket); err != nil {
		_ = ws.Close(websocket.StatusInternalError, "probe write failed")
		return
	}
	_, data, err = ws.Read(ctx)
	if err != nil || string(data) != "5" {
		_ = ws.Close(websocket.StatusProtocolError, "upgrade packet expected")
		return
	}
	c.wsMu.Lock()
	c.ws = ws
	c.wsMu.Unlock()
	c.startWebSocketLoops()
}

func (c *engineConn) startWebSocketLoops() {
	c.lc.start("engine-read", c.readLoop)
	c.lc.start("engine-write", c.writeLoop)
}

func (s *Server) servePolling(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("sid")
	if sid == "" {
		if r.Method != http.MethodGet {
			writeEngineError(w, 2, "Bad handshake method")
			return
		}
		c := newEngineConn(s, string(s.ids.next()), r)
		s.addConn(c)
		open := c.openPacket()
		writePlain(w, open.B)
		open.Release()
		return
	}
	c, ok := s.getConn(sid)
	if !ok {
		writeEngineError(w, 1, "Session ID unknown")
		return
	}
	switch r.Method {
	case http.MethodGet:
		frames := c.poll.take(c.server.pingInterval + c.server.pingTimeout)
		writePollingFrames(w, frames)
	case http.MethodPost:
		body, err := readAllPooled(r.Body, r.ContentLength)
		if err != nil {
			writeEngineError(w, 3, "Bad request")
			return
		}
		if err := c.handlePayload(body.B); err != nil {
			body.Release()
			c.server.reportError(err)
			writeEngineError(w, 3, "Bad request")
			return
		}
		body.Release()
		writePlain(w, engineOKPacket)
	default:
		writeEngineError(w, 3, "Bad request")
	}
}

func (c *engineConn) openPacket() *pooledBytes {
	upgrades := []string{"websocket"}
	if c.request != nil && c.request.URL.Query().Get("transport") == "websocket" {
		upgrades = []string{}
	}
	out := acquireBytes(len(c.sid) + 128)
	out.AppendByte('0')
	out.AppendString(`{"sid":"`)
	appendJSONStringContentOwned(out, c.sid)
	out.AppendString(`","upgrades":[`)
	for i, upgrade := range upgrades {
		if i != 0 {
			out.AppendByte(',')
		}
		out.AppendQuote(upgrade)
	}
	out.AppendString(`],"pingInterval":`)
	out.AppendUint(uint64(c.server.pingInterval.Milliseconds()))
	out.AppendString(`,"pingTimeout":`)
	out.AppendUint(uint64(c.server.pingTimeout.Milliseconds()))
	out.AppendString(`,"maxPayload":1000000}`)
	return out
}

func (c *engineConn) readLoop(ctx context.Context) {
	for {
		c.wsMu.RLock()
		ws := c.ws
		c.wsMu.RUnlock()
		if ws == nil {
			return
		}
		typ, data, err := ws.Read(ctx)
		if err != nil {
			c.close(ReasonTransportClose)
			return
		}
		var handleErr error
		if typ == websocket.MessageBinary {
			handleErr = c.handleBinaryAttachment(data)
		} else {
			handleErr = c.handleEnginePacket(data)
		}
		if handleErr != nil {
			c.server.reportError(handleErr)
			c.close(ReasonTransportError)
			return
		}
	}
}

func (c *engineConn) writeLoop(ctx context.Context) {
	ping := time.NewTimer(c.server.pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-c.send:
			c.wsMu.RLock()
			ws := c.ws
			c.wsMu.RUnlock()
			if ws == nil {
				c.poll.add(f)
				continue
			}
			if err := ws.Write(ctx, f.typ, f.data); err != nil {
				releaseEngineFrame(f)
				c.close(ReasonTransportError)
				return
			}
			releaseEngineFrame(f)
		case <-ping.C:
			c.wsMu.RLock()
			ws := c.ws
			c.wsMu.RUnlock()
			if ws != nil {
				if err := ws.Write(ctx, websocket.MessageText, enginePingPacket); err != nil {
					c.close(ReasonPingTimeout)
					return
				}
			}
			ping.Reset(c.server.pingInterval)
		}
	}
}

func (c *engineConn) handlePayload(payload []byte) error {
	for len(payload) > 0 {
		idx := bytes.IndexByte(payload, 0x1e)
		part := payload
		if idx >= 0 {
			part = payload[:idx]
			payload = payload[idx+1:]
		} else {
			payload = nil
		}
		if len(part) == 0 {
			continue
		}
		if part[0] == 'b' {
			data, err := base64.StdEncoding.DecodeString(bytesToStringView(part[1:]))
			if err != nil {
				return fmt.Errorf("engine.io: polling base64 packet for sid %s decode failed: %w", c.sid, err)
			}
			if err := c.handleBinaryAttachment(data); err != nil {
				return err
			}
			continue
		}
		if err := c.handleEnginePacket(part); err != nil {
			return err
		}
	}
	return nil
}

func (c *engineConn) handleEnginePacket(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("engine.io: empty packet for sid %s", c.sid)
	}
	switch data[0] {
	case '2':
		c.enqueueRaw(enginePongPacket)
	case '3':
		return nil
	case '4':
		return c.handleSocketPacket(data[1:])
	case '1':
		c.close(ReasonTransportClose)
	}
	return nil
}

func (c *engineConn) handleSocketPacket(data []byte) error {
	p, err := ParsePacketView(data)
	if err != nil {
		c.server.metrics.parserErrors.Add(1)
		return fmt.Errorf("socket.io: parse packet for engine sid %s failed: %w", c.sid, err)
	}
	if int(p.Type) < len(c.server.metrics.packetsIn) {
		c.server.metrics.packetsIn[p.Type].Add(1)
	}
	c.server.metrics.packetBytesIn.Add(uint64(len(data)))
	if p.Attachments > 0 {
		c.mu.Lock()
		if c.pendingBinary != nil {
			c.mu.Unlock()
			return fmt.Errorf("socket.io: binary packet for engine sid %s arrived before previous packet completed", c.sid)
		}
		c.pendingBinary = &pendingBinaryPacket{packet: p, binary: acquireByteBatch(p.Attachments, 0)}
		c.mu.Unlock()
		return nil
	}
	return c.handleParsedSocketPacket(p)
}

func (c *engineConn) handleBinaryAttachment(data []byte) error {
	c.mu.Lock()
	pending := c.pendingBinary
	if pending == nil {
		c.mu.Unlock()
		return fmt.Errorf("socket.io: unexpected binary attachment for engine sid %s", c.sid)
	}
	pending.binary.AppendBytes(data)
	complete := len(pending.binary.Views()) == pending.packet.Attachments
	if !complete {
		c.metricsBinaryIn()
		c.mu.Unlock()
		return nil
	}
	p := pending.packet
	p.Binary = pending.binary.Views()
	binary := pending.binary
	c.pendingBinary = nil
	c.metricsBinaryIn()
	c.mu.Unlock()
	err := c.handleParsedSocketPacket(p)
	binary.Release()
	return err
}

func (c *engineConn) metricsBinaryIn() {
	c.server.metrics.binaryIn.Add(1)
}

func (c *engineConn) handleParsedSocketPacket(p PacketView) error {
	nsp := bytesToStringView(p.Namespace)
	if nsp == "" {
		nsp = "/"
	}
	if p.Type == PacketConnect {
		n, ok := c.server.getNamespace(nsp)
		if !ok {
			payload := acquireBytes(len(nsp) + 36)
			payload.B = appendPacketHeaderString(payload.B, PacketConnectError, 0, nsp, 0, false)
			payload.AppendString(`{"message":"Invalid namespace"}`)
			c.sendSocketPacket(payload.B)
			payload.Release()
			return nil
		}
		_, err := n.add(c, p.Payload, time.Now())
		return err
	}
	if p.Type == PacketAck || p.Type == PacketBinaryAck {
		if p.HasID {
			c.mu.RLock()
			tracker := c.broadcastAcks[p.ID]
			c.mu.RUnlock()
			if tracker != nil {
				tracker.accept(p.Args, p.Binary)
				return nil
			}
		}
	}
	c.mu.RLock()
	socket := c.socketsByNsp[nsp]
	c.mu.RUnlock()
	if socket == nil {
		return fmt.Errorf("socket.io: namespace %s is not connected for engine sid %s", nsp, c.sid)
	}
	socket.onPacket(p)
	return nil
}

func (c *engineConn) addSocket(socket *serverSocket) {
	c.mu.Lock()
	c.socketsByNsp[socket.namespace.name] = socket
	c.mu.Unlock()
}

func (c *engineConn) removeSocket(namespace string) {
	c.mu.Lock()
	delete(c.socketsByNsp, namespace)
	c.mu.Unlock()
}

func (c *engineConn) sendSocketPacket(packet []byte) {
	c.sendSocketPayload(packet, nil)
}

func (c *engineConn) sendSocketPayload(packet []byte, attachments [][]byte) {
	if len(packet) != 0 && packet[0] >= '0' && packet[0] <= '6' {
		typ := PacketType(packet[0] - '0')
		c.server.metrics.packetsOut[typ].Add(1)
	}
	c.server.metrics.packetBytesOut.Add(uint64(len(packet)))
	c.enqueueFrame(newEngineMessageFrame(packet))
	for _, attachment := range attachments {
		c.server.metrics.binaryOut.Add(1)
		c.enqueueFrame(newEngineBinaryFrame(attachment))
	}
}

func (c *engineConn) enqueueRaw(data []byte) { c.enqueueFrame(newEngineTextFrame(data)) }

func (c *engineConn) enqueueFrame(f *engineFrame) {
	if c.closed.Load() {
		releaseEngineFrame(f)
		return
	}
	c.wsMu.RLock()
	ws := c.ws
	c.wsMu.RUnlock()
	if ws == nil {
		c.poll.add(f)
		return
	}
	select {
	case c.send <- f:
	default:
		releaseEngineFrame(f)
		c.close(ReasonTransportError)
	}
}

func (c *engineConn) registerBroadcastAck(id uint64, tracker broadcastAckTarget) {
	c.mu.Lock()
	c.broadcastAcks[id] = tracker
	c.mu.Unlock()
}

func (c *engineConn) close(reason Reason) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.mu.RLock()
	sockets := make([]*serverSocket, 0, len(c.socketsByNsp))
	for _, s := range c.socketsByNsp {
		sockets = append(sockets, s)
	}
	c.mu.RUnlock()
	for _, socket := range sockets {
		socket.onClose(reason)
	}
	c.wsMu.Lock()
	if c.ws != nil {
		_ = c.ws.Close(websocket.StatusNormalClosure, string(reason))
	}
	c.wsMu.Unlock()
	c.mu.Lock()
	if c.pendingBinary != nil && c.pendingBinary.binary != nil {
		c.pendingBinary.binary.Release()
		c.pendingBinary = nil
	}
	c.mu.Unlock()
	_ = c.lc.stop(time.Second)
	c.server.removeConn(c.sid)
}

func writePlain(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writePollingFrames(w http.ResponseWriter, frames []*engineFrame) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	for i, f := range frames {
		if i != 0 {
			_, _ = w.Write(pollingSeparator)
		}
		if f.typ == websocket.MessageBinary {
			_, _ = w.Write(pollingBase64Prefix)
			encoder := base64.NewEncoder(base64.StdEncoding, w)
			_, _ = encoder.Write(f.data)
			_ = encoder.Close()
		} else {
			_, _ = w.Write(f.data)
		}
		releaseEngineFrame(f)
	}
}
