package sio

import "time"

type remoteSocket struct {
	id        SocketID
	rooms     []Room
	namespace *Namespace
}

func (s *remoteSocket) ID() SocketID          { return s.id }
func (s *remoteSocket) Connected() bool       { return true }
func (s *remoteSocket) Recovered() bool       { return false }
func (s *remoteSocket) Server() *Server       { return s.namespace.server }
func (s *remoteSocket) Namespace() *Namespace { return s.namespace }
func (s *remoteSocket) Rooms() []Room         { return append([]Room(nil), s.rooms...) }
func (s *remoteSocket) Emit(eventName string, v ...any) {
	s.namespace.To(Room(s.id)).Emit(eventName, v...)
}
func (s *remoteSocket) Timeout(timeout time.Duration) Emitter {
	return Emitter{target: remoteEmitTarget{s: s, timeout: timeout}, timeout: timeout}
}
func (s *remoteSocket) Join(room ...Room)                                   { s.namespace.To(Room(s.id)).SocketsJoin(room...) }
func (s *remoteSocket) Leave(room Room)                                     { s.namespace.To(Room(s.id)).SocketsLeave(room) }
func (s *remoteSocket) Use(f any)                                           {}
func (s *remoteSocket) To(room ...Room) *BroadcastOperator                  { return s.namespace.To(room...) }
func (s *remoteSocket) In(room ...Room) *BroadcastOperator                  { return s.To(room...) }
func (s *remoteSocket) Except(room ...Room) *BroadcastOperator              { return s.namespace.Except(room...) }
func (s *remoteSocket) Local() *BroadcastOperator                           { return s.namespace.Local() }
func (s *remoteSocket) Broadcast() *BroadcastOperator                       { return s.namespace.Except(Room(s.id)) }
func (s *remoteSocket) Disconnect(close bool)                               { s.namespace.To(Room(s.id)).DisconnectSockets(close) }
func (s *remoteSocket) OnEvent(eventName string, handler any)               {}
func (s *remoteSocket) OnceEvent(eventName string, handler any)             {}
func (s *remoteSocket) OffEvent(eventName string, handler ...any)           {}
func (s *remoteSocket) OffAll()                                             {}
func (s *remoteSocket) OnError(f ServerSocketErrorFunc)                     {}
func (s *remoteSocket) OnceError(f ServerSocketErrorFunc)                   {}
func (s *remoteSocket) OffError(f ...ServerSocketErrorFunc)                 {}
func (s *remoteSocket) OnDisconnecting(f ServerSocketDisconnectingFunc)     {}
func (s *remoteSocket) OnceDisconnecting(f ServerSocketDisconnectingFunc)   {}
func (s *remoteSocket) OffDisconnecting(f ...ServerSocketDisconnectingFunc) {}
func (s *remoteSocket) OnDisconnect(f ServerSocketDisconnectFunc)           {}
func (s *remoteSocket) OnceDisconnect(f ServerSocketDisconnectFunc)         {}
func (s *remoteSocket) OffDisconnect(f ...ServerSocketDisconnectFunc)       {}

type remoteEmitTarget struct {
	s       *remoteSocket
	timeout time.Duration
}

func (t remoteEmitTarget) emitWithOptions(eventName string, timeout time.Duration, v ...any) {
	t.s.namespace.To(Room(t.s.id)).Timeout(timeout).Emit(eventName, v...)
}
