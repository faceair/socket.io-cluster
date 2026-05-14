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
	rooms      map[Room]map[SocketID]*serverSocket
	sids       map[SocketID]map[Room]struct{}
	socketByID map[SocketID]*serverSocket
	epoch      uint64
}

var adapterSocketSlicePool = sync.Pool{New: func() any {
	s := make([]*serverSocket, 0, 64)
	return &s
}}

func newLocalAdapter() *localAdapter {
	return &localAdapter{
		rooms:      make(map[Room]map[SocketID]*serverSocket),
		sids:       make(map[SocketID]map[Room]struct{}),
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
	rooms := a.sids[sid]
	for room := range rooms {
		members := a.rooms[room]
		delete(members, sid)
		if len(members) == 0 {
			delete(a.rooms, room)
		}
	}
	delete(a.sids, sid)
	delete(a.socketByID, sid)
	a.mu.Unlock()
}

func (a *localAdapter) addAll(sid SocketID, socket *serverSocket, rooms []Room) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.sids[sid]; !ok {
		a.sids[sid] = make(map[Room]struct{}, len(rooms)+1)
	}
	for _, room := range rooms {
		a.sids[sid][room] = struct{}{}
		members := a.rooms[room]
		if members == nil {
			members = make(map[SocketID]*serverSocket)
			a.rooms[room] = members
		}
		members[sid] = socket
	}
}

func (a *localAdapter) delete(sid SocketID, room Room) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if rooms := a.sids[sid]; rooms != nil {
		delete(rooms, room)
	}
	if members := a.rooms[room]; members != nil {
		delete(members, sid)
		if len(members) == 0 {
			delete(a.rooms, room)
		}
	}
}

func (a *localAdapter) socketRooms(sid SocketID) []Room {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rooms := a.sids[sid]
	out := make([]Room, 0, len(rooms))
	for room := range rooms {
		out = append(out, room)
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
		for _, socket := range a.rooms[room] {
			socket.matchExcept = token
		}
	}
	if len(opts.Rooms) > 0 {
		for _, room := range opts.Rooms {
			for _, socket := range a.rooms[room] {
				if socket.matchSeen == token || socket.matchExcept == token {
					continue
				}
				socket.matchSeen = token
				matches = append(matches, socket)
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
	for _, members := range a.rooms {
		memberships += len(members)
	}
	return sockets, rooms, memberships
}
