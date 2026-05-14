package sio

import (
	"sync"

	"nhooyr.io/websocket"
)

type engineFrame struct {
	typ   websocket.MessageType
	data  []byte
	owner *pooledBytes
}

var engineFramePool = sync.Pool{New: func() any { return &engineFrame{} }}

func newEngineMessageFrame(packet []byte) *engineFrame {
	f := engineFramePool.Get().(*engineFrame)
	need := len(packet) + 1
	f.owner = acquireBytes(need)
	f.data = f.owner.Resize(need)
	f.data[0] = '4'
	copy(f.data[1:], packet)
	f.typ = websocket.MessageText
	return f
}

func newEngineTextFrame(data []byte) *engineFrame {
	f := engineFramePool.Get().(*engineFrame)
	f.owner = acquireBytes(len(data))
	f.data = f.owner.Resize(len(data))
	copy(f.data, data)
	f.typ = websocket.MessageText
	return f
}

func newEngineBinaryFrame(data []byte) *engineFrame {
	f := engineFramePool.Get().(*engineFrame)
	f.owner = acquireBytes(len(data))
	f.data = f.owner.Resize(len(data))
	copy(f.data, data)
	f.typ = websocket.MessageBinary
	return f
}

func releaseEngineFrame(f *engineFrame) {
	if f == nil {
		return
	}
	if f.owner != nil {
		f.owner.Release()
	}
	f.typ = websocket.MessageText
	f.data = nil
	f.owner = nil
	engineFramePool.Put(f)
}
