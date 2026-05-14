package sio

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
)

type serverSocket struct {
	id              SocketID
	pid             string
	server          *Server
	conn            *engineConn
	namespace       *Namespace
	connected       atomic.Bool
	recovered       bool
	recoverySession *recoverySession

	handlers *eventHandlers
	acks     *ackStore

	errorHandlers         callbackStore[ServerSocketErrorFunc]
	disconnectingHandlers callbackStore[ServerSocketDisconnectingFunc]
	disconnectHandlers    callbackStore[ServerSocketDisconnectFunc]

	matchSeen   uint64
	matchExcept uint64
}

func newServerSocket(server *Server, conn *engineConn, namespace *Namespace, id SocketID) *serverSocket {
	return &serverSocket{server: server, conn: conn, namespace: namespace, id: id, handlers: newEventHandlers(), acks: newAckStore()}
}

func (s *serverSocket) ID() SocketID          { return s.id }
func (s *serverSocket) Connected() bool       { return s.connected.Load() }
func (s *serverSocket) Recovered() bool       { return s.recovered }
func (s *serverSocket) Server() *Server       { return s.server }
func (s *serverSocket) Namespace() *Namespace { return s.namespace }

func (s *serverSocket) Join(room ...Room) { s.namespace.adapter.addAll(s.id, s, room) }
func (s *serverSocket) Leave(room Room)   { s.namespace.adapter.delete(s.id, room) }
func (s *serverSocket) Rooms() []Room     { return s.namespace.adapter.socketRooms(s.id) }
func (s *serverSocket) Use(f any)         {}
func (s *serverSocket) To(room ...Room) *BroadcastOperator {
	return s.newBroadcastOperator().To(room...)
}
func (s *serverSocket) In(room ...Room) *BroadcastOperator { return s.To(room...) }
func (s *serverSocket) Except(room ...Room) *BroadcastOperator {
	return s.newBroadcastOperator().Except(room...)
}
func (s *serverSocket) Local() *BroadcastOperator     { return s.newBroadcastOperator().Local() }
func (s *serverSocket) Broadcast() *BroadcastOperator { return s.newBroadcastOperator() }

func (s *serverSocket) newBroadcastOperator() *BroadcastOperator {
	return (&BroadcastOperator{namespace: s.namespace, opts: broadcastOptions{Except: []Room{Room(s.id)}}})
}

func (s *serverSocket) Emit(eventName string, v ...any) { s.emitWithOptions(eventName, 0, v...) }
func (s *serverSocket) Timeout(timeout time.Duration) Emitter {
	return Emitter{target: s, timeout: timeout}
}

func (s *serverSocket) emitWithOptions(eventName string, timeout time.Duration, v ...any) {
	if !s.Connected() {
		return
	}
	var id uint64
	hasID := false
	args := v
	if len(v) > 0 {
		last := v[len(v)-1]
		if last != nil && reflect.TypeOf(last).Kind() == reflect.Func {
			h, err := newAckHandler(last, timeout > 0)
			if err != nil {
				panic(err)
			}
			id = s.acks.next()
			hasID = true
			s.acks.add(id, h, timeout, time.Now())
			s.server.metrics.acksRegistered.Add(1)
			args = v[:len(v)-1]
		}
	}
	encoded, err := encodeAnyArgs(args)
	if err != nil {
		s.onError(fmt.Errorf("sio: socket %s emit %q encode failed: %w", s.id, eventName, err))
		return
	}
	defer encoded.Release()
	packet := acquireBytes(encodedSize(encoded) + len(eventName) + 32)
	packet.B = appendEventEncoded(packet.B, s.namespace.name, id, hasID, eventName, encoded)
	s.server.metrics.emitsTotal.Add(1)
	s.conn.sendSocketPayload(packet.B, encoded.BinaryViews())
	packet.Release()
}

func (s *serverSocket) sendConnect() {
	packet := acquireBytes(len(s.id) + len(s.pid) + 48)
	packet.B = appendConnectPacketString(packet.B, s.namespace.name, string(s.id), s.pid)
	s.conn.sendSocketPacket(packet.B)
	packet.Release()
}

func (s *serverSocket) sendAckEncoded(id uint64, encoded encodedArgs) {
	defer encoded.Release()
	packet := acquireBytes(encodedSize(encoded) + 24)
	packet.B = appendAckEncoded(packet.B, s.namespace.name, id, encoded)
	s.conn.sendSocketPayload(packet.B, encoded.BinaryViews())
	packet.Release()
}

func (s *serverSocket) onPacket(p PacketView) {
	switch p.Type {
	case PacketEvent, PacketBinaryEvent:
		if err := s.handlers.dispatch(s, p); err != nil {
			s.onError(err)
		}
	case PacketAck, PacketBinaryAck:
		if !p.HasID {
			s.onError(fmt.Errorf("sio: socket %s received ACK without id", s.id))
			return
		}
		h, ok := s.acks.take(p.ID)
		if !ok {
			return
		}
		s.server.metrics.acksResolved.Add(1)
		if err := h.call(p.Args, p.Binary); err != nil {
			s.onError(fmt.Errorf("sio: socket %s ack %d failed: %w", s.id, p.ID, err))
		}
	case PacketDisconnect:
		s.onClose(ReasonClientNamespaceDisconnect)
	}
}

func (s *serverSocket) Disconnect(closeConn bool) {
	if !s.Connected() {
		return
	}
	packet := acquireBytes(len(s.namespace.name) + 4)
	packet.B = appendDisconnectPacketString(packet.B, s.namespace.name)
	s.conn.sendSocketPacket(packet.B)
	packet.Release()
	s.onClose(ReasonServerNamespaceDisconnect)
	if closeConn {
		s.conn.close(ReasonServerNamespaceDisconnect)
	}
}

func (s *serverSocket) onClose(reason Reason) {
	if !s.connected.Swap(false) {
		return
	}
	s.disconnectingHandlers.forEach(func(f ServerSocketDisconnectingFunc) { f(reason) })
	if s.server.recovery != nil && s.pid != "" && isRecoverableDisconnect(reason) {
		s.server.recovery.save(s.namespace.name, s.pid, s.id, s.Rooms(), time.Now())
	}
	s.namespace.remove(s)
	s.conn.removeSocket(s.namespace.name)
	s.disconnectHandlers.forEach(func(f ServerSocketDisconnectFunc) { f(reason) })
	if s.recoverySession != nil {
		s.recoverySession.release()
		s.recoverySession = nil
	}
}

func (s *serverSocket) onError(err error) {
	s.errorHandlers.forEach(func(f ServerSocketErrorFunc) { f(err) })
	if s.server.onError != nil {
		s.server.onError(err)
	}
}

func isRecoverableDisconnect(reason Reason) bool {
	switch reason {
	case ReasonTransportClose, ReasonTransportError, ReasonPingTimeout:
		return true
	default:
		return false
	}
}

func (s *serverSocket) OnEvent(eventName string, handler any) {
	if err := s.handlers.add(eventName, handler, false); err != nil {
		panic(err)
	}
}
func (s *serverSocket) OnceEvent(eventName string, handler any) {
	if err := s.handlers.add(eventName, handler, true); err != nil {
		panic(err)
	}
}
func (s *serverSocket) OffEvent(eventName string, handler ...any) {
	s.handlers.off(eventName, handler...)
}
func (s *serverSocket) OffAll()                             { s.handlers.offAll() }
func (s *serverSocket) OnError(f ServerSocketErrorFunc)     { s.errorHandlers.add(f, false) }
func (s *serverSocket) OnceError(f ServerSocketErrorFunc)   { s.errorHandlers.add(f, true) }
func (s *serverSocket) OffError(f ...ServerSocketErrorFunc) { s.errorHandlers.remove(f...) }
func (s *serverSocket) OnDisconnecting(f ServerSocketDisconnectingFunc) {
	s.disconnectingHandlers.add(f, false)
}
func (s *serverSocket) OnceDisconnecting(f ServerSocketDisconnectingFunc) {
	s.disconnectingHandlers.add(f, true)
}
func (s *serverSocket) OffDisconnecting(f ...ServerSocketDisconnectingFunc) {
	s.disconnectingHandlers.remove(f...)
}
func (s *serverSocket) OnDisconnect(f ServerSocketDisconnectFunc) { s.disconnectHandlers.add(f, false) }
func (s *serverSocket) OnceDisconnect(f ServerSocketDisconnectFunc) {
	s.disconnectHandlers.add(f, true)
}
func (s *serverSocket) OffDisconnect(f ...ServerSocketDisconnectFunc) {
	s.disconnectHandlers.remove(f...)
}

func (s *serverSocket) sweepAcks(now time.Time) {
	for _, h := range s.acks.sweep(now) {
		s.server.metrics.acksTimedOut.Add(1)
		h.timeout()
	}
}

type encodedArgs struct {
	json   *byteBatch
	binary *byteBatch
}

func encodeAnyArgs(args []any) (encodedArgs, error) {
	if len(args) == 0 {
		return encodedArgs{}, nil
	}
	out := encodedArgs{
		json:   acquireByteBatch(len(args), estimateArgsBytes(args)),
		binary: acquireOptionalBinaryBatch(args),
	}
	for i, arg := range args {
		if b, ok := binaryArg(arg); ok {
			out.json.AppendPlaceholder(len(out.BinaryViews()))
			out.binary.AppendBytes(b)
			continue
		}
		if raw, ok := arg.([]byte); ok && json.Valid(raw) {
			out.json.AppendBytes(raw)
			continue
		}
		b, err := json.Marshal(arg)
		if err != nil {
			out.Release()
			return encodedArgs{}, fmt.Errorf("arg %d: %w", i, err)
		}
		out.json.AppendBytes(b)
	}
	return out, nil
}

func acquireOptionalBinaryBatch(args []any) *byteBatch {
	byteCap := estimateBinaryBytes(args)
	if byteCap == 0 && countBinaryArgs(args) == 0 {
		return nil
	}
	return acquireByteBatch(len(args), byteCap)
}

func binaryArg(arg any) ([]byte, bool) {
	switch v := arg.(type) {
	case Binary:
		return []byte(v), true
	case []byte:
		if json.Valid(v) {
			return nil, false
		}
		return v, true
	default:
		return nil, false
	}
}

func encodedSize(encoded encodedArgs) int {
	n := 0
	for _, b := range encoded.JSONViews() {
		n += len(b) + 1
	}
	return n
}

func appendEventEncoded(dst []byte, namespace string, id uint64, hasID bool, eventName string, encoded encodedArgs) []byte {
	if len(encoded.BinaryViews()) != 0 {
		return appendEventPacketString(dst, PacketBinaryEvent, len(encoded.BinaryViews()), namespace, id, hasID, eventName, encoded.JSONViews()...)
	}
	return appendEventPacketString(dst, PacketEvent, 0, namespace, id, hasID, eventName, encoded.JSONViews()...)
}

func appendAckEncoded(dst []byte, namespace string, id uint64, encoded encodedArgs) []byte {
	if len(encoded.BinaryViews()) != 0 {
		return appendAckPacketString(dst, PacketBinaryAck, len(encoded.BinaryViews()), namespace, id, encoded.JSONViews()...)
	}
	return appendAckPacketString(dst, PacketAck, 0, namespace, id, encoded.JSONViews()...)
}

func (e encodedArgs) JSONViews() [][]byte {
	if e.json == nil {
		return nil
	}
	return e.json.Views()
}

func (e encodedArgs) BinaryViews() [][]byte {
	if e.binary == nil {
		return nil
	}
	return e.binary.Views()
}

func (e encodedArgs) Release() {
	if e.json != nil {
		e.json.Release()
	}
	if e.binary != nil {
		e.binary.Release()
	}
}

func estimateArgsBytes(args []any) int {
	n := len(args) * 16
	for _, arg := range args {
		switch v := arg.(type) {
		case []byte:
			n += len(v)
		case Binary:
			n += 32
		case string:
			n += len(v) + 2
		}
	}
	return n
}

func estimateBinaryBytes(args []any) int {
	n := 0
	for _, arg := range args {
		if b, ok := binaryArg(arg); ok {
			n += len(b)
		}
	}
	return n
}

func countBinaryArgs(args []any) int {
	n := 0
	for _, arg := range args {
		if _, ok := binaryArg(arg); ok {
			n++
		}
	}
	return n
}
