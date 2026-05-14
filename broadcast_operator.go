package sio

import (
	"fmt"
	"reflect"
	"time"
)

type BroadcastOperator struct {
	namespace *Namespace
	opts      broadcastOptions
	timeout   time.Duration
}

func (b *BroadcastOperator) To(room ...Room) *BroadcastOperator {
	n := *b
	n.opts.Rooms = append(append([]Room(nil), b.opts.Rooms...), room...)
	return &n
}
func (b *BroadcastOperator) In(room ...Room) *BroadcastOperator { return b.To(room...) }
func (b *BroadcastOperator) Except(room ...Room) *BroadcastOperator {
	n := *b
	n.opts.Except = append(append([]Room(nil), b.opts.Except...), room...)
	return &n
}
func (b *BroadcastOperator) Local() *BroadcastOperator {
	n := *b
	n.opts.Flags.Local = true
	return &n
}
func (b *BroadcastOperator) Timeout(timeout time.Duration) *BroadcastOperator {
	n := *b
	n.timeout = timeout
	return &n
}

func (b *BroadcastOperator) Emit(eventName string, v ...any) {
	args := v
	var ack any
	if len(args) > 0 {
		last := args[len(args)-1]
		if last != nil && reflect.TypeOf(last).Kind() == reflect.Func {
			ack = last
			args = args[:len(args)-1]
		}
	}
	if ack != nil {
		b.emitWithAck(eventName, ack, args...)
		return
	}
	offset := ""
	if b.namespace.server.recovery != nil {
		offset = b.namespace.server.recovery.nextOffset()
		withOffset := make([]any, 0, len(args)+1)
		withOffset = append(withOffset, args...)
		withOffset = append(withOffset, offset)
		args = withOffset
	}
	encoded, err := encodeAnyArgs(args)
	if err != nil {
		b.namespace.server.reportError(fmt.Errorf("sio: broadcast %s emit %q encode failed: %w", b.namespace.name, eventName, err))
		return
	}
	defer encoded.Release()
	packet := acquireBytes(encodedSize(encoded) + len(eventName) + 32)
	packet.B = appendEventEncoded(packet.B, b.namespace.name, 0, false, eventName, encoded)
	b.namespace.server.metrics.broadcastsTotal.Add(1)
	if offset != "" {
		b.namespace.server.recovery.log(b.namespace.name, b.opts, offset, packet.B, encoded.BinaryViews(), time.Now())
	}
	b.namespace.adapter.apply(b.opts, func(s *serverSocket) { s.conn.sendSocketPayload(packet.B, encoded.BinaryViews()) })
	if b.namespace.server.cluster != nil && !b.opts.Flags.Local {
		b.namespace.server.cluster.broadcast(b.namespace.name, b.opts, packet.B, encoded.BinaryViews())
	}
	packet.Release()
}

func (b *BroadcastOperator) emitWithAck(eventName string, ack any, args ...any) {
	h, err := newAckHandler(ack, true)
	if err != nil {
		panic(err)
	}
	encoded, err := encodeAnyArgs(args)
	if err != nil {
		b.namespace.server.reportError(fmt.Errorf("sio: broadcast %s ack emit %q encode failed: %w", b.namespace.name, eventName, err))
		return
	}
	defer encoded.Release()
	tracker := b.namespace.server.newBroadcastAck(h, b.timeout)
	packet := acquireBytes(encodedSize(encoded) + len(eventName) + 32)
	packet.B = appendEventEncoded(packet.B, b.namespace.name, tracker.id, true, eventName, encoded)
	b.namespace.server.metrics.broadcastsTotal.Add(1)
	local := b.namespace.adapter.apply(b.opts, func(s *serverSocket) {
		s.conn.registerBroadcastAck(tracker.id, tracker)
		s.conn.sendSocketPayload(packet.B, encoded.BinaryViews())
	})
	tracker.addExpected(local)
	if b.namespace.server.cluster != nil && !b.opts.Flags.Local {
		b.namespace.server.cluster.broadcastWithAck(b.namespace.name, b.opts, packet.B, encoded.BinaryViews(), tracker.id)
	}
	packet.Release()
	tracker.maybeFinish()
}

func (b *BroadcastOperator) FetchSockets() []ServerSocket {
	if b.namespace.server.cluster == nil || b.opts.Flags.Local {
		return b.namespace.adapter.matchingSockets(b.opts)
	}
	snapshots := b.namespace.server.cluster.fetchSockets(b.namespace.name, b.opts)
	out := make([]ServerSocket, 0, len(snapshots))
	local := b.namespace.adapter.matchingSockets(b.opts)
	localByID := make(map[SocketID]ServerSocket, len(local))
	for _, socket := range local {
		localByID[socket.ID()] = socket
	}
	for _, snapshot := range snapshots {
		if socket := localByID[snapshot.ID]; socket != nil {
			out = append(out, socket)
		} else {
			out = append(out, &remoteSocket{id: snapshot.ID, rooms: snapshot.Rooms, namespace: b.namespace})
		}
	}
	return out
}
func (b *BroadcastOperator) SocketsJoin(room ...Room) {
	b.namespace.adapter.apply(b.opts, func(s *serverSocket) { s.Join(room...) })
	if b.namespace.server.cluster != nil && !b.opts.Flags.Local {
		b.namespace.server.cluster.socketsJoin(b.namespace.name, b.opts, room)
	}
}
func (b *BroadcastOperator) SocketsLeave(room ...Room) {
	b.namespace.adapter.apply(b.opts, func(s *serverSocket) {
		for _, r := range room {
			s.Leave(r)
		}
	})
	if b.namespace.server.cluster != nil && !b.opts.Flags.Local {
		b.namespace.server.cluster.socketsLeave(b.namespace.name, b.opts, room)
	}
}
func (b *BroadcastOperator) DisconnectSockets(close bool) {
	b.namespace.adapter.apply(b.opts, func(s *serverSocket) { s.Disconnect(close) })
	if b.namespace.server.cluster != nil && !b.opts.Flags.Local {
		b.namespace.server.cluster.disconnectSockets(b.namespace.name, b.opts, close)
	}
}
