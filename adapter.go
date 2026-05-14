package sio

import "sync"

type broadcastFlags struct {
	Local bool
}

type broadcastOptions struct {
	Rooms  []Room
	Except []Room
	Flags  broadcastFlags
}

type localAdapter struct {
	mu         sync.RWMutex
	rooms      map[Room]*roomEntry
	sids       map[SocketID]map[Room]*roomEntry
	socketByID map[SocketID]*serverSocket
	epoch      uint64
}

type roomEntry struct {
	name    Room
	owner   *pooledBytes
	members map[SocketID]*serverSocket
}

var adapterSocketSlicePool = sync.Pool{New: func() any {
	s := make([]*serverSocket, 0, 64)
	return &s
}}

func newLocalAdapter() *localAdapter {
	return &localAdapter{
		rooms:      make(map[Room]*roomEntry),
		sids:       make(map[SocketID]map[Room]*roomEntry),
		socketByID: make(map[SocketID]*serverSocket),
	}
}

func (a *localAdapter) addSocket(s *serverSocket) {
	a.mu.Lock()
	a.socketByID[s.id] = s
	a.mu.Unlock()
	a.addAll(s.id, s, []Room{Room(s.id)})
}

func (a *localAdapter) removeSocket(sid SocketID) {
	a.mu.Lock()
	entries := a.sids[sid]
	for _, entry := range entries {
		delete(entry.members, sid)
		if len(entry.members) == 0 {
			delete(a.rooms, entry.name)
			entry.release()
		}
	}
	delete(a.sids, sid)
	delete(a.socketByID, sid)
	a.mu.Unlock()
}

func (a *localAdapter) addAll(sid SocketID, socket *serverSocket, rooms []Room) {
	a.addAllWithRoomOwnership(sid, socket, rooms, false)
}

func (a *localAdapter) addAllOwned(sid SocketID, socket *serverSocket, rooms []Room) {
	a.addAllWithRoomOwnership(sid, socket, rooms, true)
}

func (a *localAdapter) addAllWithRoomOwnership(sid SocketID, socket *serverSocket, rooms []Room, ownNewRooms bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.sids[sid]; !ok {
		a.sids[sid] = make(map[Room]*roomEntry, len(rooms)+1)
	}
	for _, room := range rooms {
		entry := a.rooms[room]
		if entry == nil {
			entry = newRoomEntry(room, ownNewRooms)
			a.rooms[entry.name] = entry
		}
		a.sids[sid][entry.name] = entry
		entry.members[sid] = socket
	}
}

func (a *localAdapter) delete(sid SocketID, room Room) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := a.rooms[room]
	if entry == nil {
		return
	}
	if entries := a.sids[sid]; entries != nil {
		delete(entries, entry.name)
	}
	delete(entry.members, sid)
	if len(entry.members) == 0 {
		delete(a.rooms, entry.name)
		entry.release()
	}
}

func newRoomEntry(room Room, owned bool) *roomEntry {
	if !owned {
		return &roomEntry{name: room, members: make(map[SocketID]*serverSocket)}
	}
	owner := acquireBytes(len(room))
	name := appendOwnedRoom(owner, room)
	return &roomEntry{name: name, owner: owner, members: make(map[SocketID]*serverSocket)}
}

func appendOwnedRoom(owner *pooledBytes, room Room) Room {
	if room == "" {
		return ""
	}
	start := len(owner.B)
	owner.AppendRoom(room)
	return Room(bytesToStringView(owner.B[start:]))
}

func (e *roomEntry) release() {
	if e == nil {
		return
	}
	if e.owner != nil {
		e.owner.Release()
		e.owner = nil
	}
	e.name = ""
	for sid := range e.members {
		delete(e.members, sid)
	}
}

func (e *roomEntry) markExcept(token uint64) {
	for _, socket := range e.members {
		socket.matchExcept = token
	}
}

func (e *roomEntry) appendMatches(out []*serverSocket, token uint64) []*serverSocket {
	for _, socket := range e.members {
		if socket.matchSeen == token || socket.matchExcept == token {
			continue
		}
		socket.matchSeen = token
		out = append(out, socket)
	}
	return out
}

func (a *localAdapter) socketRooms(sid SocketID) []Room {
	a.mu.RLock()
	defer a.mu.RUnlock()
	entries := a.sids[sid]
	out := make([]Room, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.name)
	}
	return out
}

func (a *localAdapter) apply(opts broadcastOptions, fn func(*serverSocket)) int {
	bufp := adapterSocketSlicePool.Get().(*[]*serverSocket)
	matches := (*bufp)[:0]
	a.mu.Lock()
	a.epoch++
	token := a.epoch
	for _, room := range opts.Except {
		if entry := a.rooms[room]; entry != nil {
			entry.markExcept(token)
		}
	}
	if len(opts.Rooms) > 0 {
		for _, room := range opts.Rooms {
			if entry := a.rooms[room]; entry != nil {
				matches = entry.appendMatches(matches, token)
			}
		}
	} else {
		for _, socket := range a.socketByID {
			if socket.matchExcept == token {
				continue
			}
			matches = append(matches, socket)
		}
	}
	a.mu.Unlock()
	for _, socket := range matches {
		fn(socket)
	}
	count := len(matches)
	for i := range matches {
		matches[i] = nil
	}
	*bufp = matches[:0]
	adapterSocketSlicePool.Put(bufp)
	return count
}

func (a *localAdapter) matchingSockets(opts broadcastOptions) []ServerSocket {
	out := make([]ServerSocket, 0)
	a.apply(opts, func(s *serverSocket) { out = append(out, s) })
	return out
}

func (a *localAdapter) stats() (sockets, rooms, memberships int) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	sockets = len(a.socketByID)
	rooms = len(a.rooms)
	for _, entry := range a.rooms {
		memberships += len(entry.members)
	}
	return sockets, rooms, memberships
}
