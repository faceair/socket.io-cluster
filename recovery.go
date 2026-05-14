package sio

import (
	"strconv"
	"sync"
	"time"
)

type recoveryStore struct {
	mu       sync.Mutex
	maxAge   time.Duration
	next     uint64
	sessions map[string]*recoverySession
	packets  []recoveryPacket
}

type recoverySession struct {
	namespace string
	pid       string
	sid       SocketID
	rooms     []Room
	expiresAt time.Time
	owner     *pooledBytes
}

type recoveryPacket struct {
	namespace        string
	offset           string
	offsetSeq        uint64
	opts             broadcastOptions
	packet           []byte
	packetOwner      *pooledBytes
	attachments      *byteBatch
	views            *pooledByteViews
	releaseAfterSend bool
	createdAt        time.Time
}

type recoveryAuth struct {
	PID    string `json:"pid"`
	Offset string `json:"offset"`
}

func newRecoveryStore(maxAge time.Duration) *recoveryStore {
	return &recoveryStore{maxAge: maxAge, sessions: make(map[string]*recoverySession)}
}

func (s *recoveryStore) nextOffset() string {
	s.mu.Lock()
	now := uint64(time.Now().UnixNano())
	if now > s.next {
		s.next = now
	} else {
		s.next++
	}
	offset := strconv.FormatUint(s.next, 10)
	s.mu.Unlock()
	return offset
}

func (s *recoveryStore) log(namespace string, opts broadcastOptions, offset string, packet []byte, attachments [][]byte, now time.Time) {
	seq, err := strconv.ParseUint(offset, 10, 64)
	if err != nil {
		return
	}
	packetOwner := acquireBytes(len(packet))
	packetOwner.AppendBytes(packet)
	item := recoveryPacket{
		namespace: namespace,
		offset:    offset,
		offsetSeq: seq,
		opts: broadcastOptions{
			Rooms:  append([]Room(nil), opts.Rooms...),
			Except: append([]Room(nil), opts.Except...),
			Flags:  opts.Flags,
		},
		packet:      packetOwner.B,
		packetOwner: packetOwner,
		attachments: copyAttachmentsToBatch(attachments),
		createdAt:   now,
	}
	s.mu.Lock()
	s.packets = append(s.packets, item)
	s.pruneLocked(now)
	s.mu.Unlock()
}

func (s *recoveryStore) save(namespace, pid string, sid SocketID, rooms []Room, now time.Time) {
	if pid == "" {
		return
	}
	session := newRecoverySession(namespace, pid, sid, rooms, now.Add(s.maxAge))
	key := recoveryKey(namespace, pid)
	s.mu.Lock()
	if previous := s.sessions[key]; previous != nil {
		previous.release()
	}
	s.sessions[key] = session
	s.pruneLocked(now)
	s.mu.Unlock()
}

func (s *recoveryStore) recover(namespace, pid, offset string, now time.Time) (*recoverySession, []recoveryPacket, bool) {
	return s.recoverWithConsume(namespace, pid, offset, now, true)
}

func (s *recoveryStore) snapshot(namespace, pid, offset string, now time.Time) (*recoverySession, []recoveryPacket, bool) {
	return s.recoverWithConsume(namespace, pid, offset, now, false)
}

func (s *recoveryStore) deleteSession(namespace, pid string) {
	s.mu.Lock()
	key := recoveryKey(namespace, pid)
	if session := s.sessions[key]; session != nil {
		session.release()
		delete(s.sessions, key)
	}
	s.mu.Unlock()
}

func (s *recoveryStore) recoverWithConsume(namespace, pid, offset string, now time.Time, consume bool) (*recoverySession, []recoveryPacket, bool) {
	seq, err := strconv.ParseUint(offset, 10, 64)
	if pid == "" || err != nil {
		return nil, nil, false
	}
	key := recoveryKey(namespace, pid)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	session := s.sessions[key]
	if session == nil || now.After(session.expiresAt) {
		if session != nil {
			session.release()
			delete(s.sessions, key)
		}
		return nil, nil, false
	}
	if consume {
		delete(s.sessions, key)
	}
	packets := make([]recoveryPacket, 0)
	for _, packet := range s.packets {
		if packet.namespace != namespace || packet.offsetSeq <= seq {
			continue
		}
		if !recoveryPacketMatches(packet.opts, session.rooms) {
			continue
		}
		packets = append(packets, packet.cloneForReplay())
	}
	if consume {
		return session, packets, true
	}
	return session.clone(), packets, true
}

func (s *recoveryStore) pruneLocked(now time.Time) {
	cutoff := now.Add(-s.maxAge)
	keep := s.packets[:0]
	for _, packet := range s.packets {
		if packet.createdAt.After(cutoff) {
			keep = append(keep, packet)
		} else {
			packet.release()
		}
	}
	for i := len(keep); i < len(s.packets); i++ {
		s.packets[i] = recoveryPacket{}
	}
	s.packets = keep
	for key, session := range s.sessions {
		if now.After(session.expiresAt) {
			session.release()
			delete(s.sessions, key)
		}
	}
}

func (p *recoveryPacket) attachmentViews() [][]byte {
	if p == nil {
		return nil
	}
	if p.attachments != nil {
		return p.attachments.Views()
	}
	if p.views != nil {
		return p.views.V
	}
	return nil
}

func (p recoveryPacket) cloneForReplay() recoveryPacket {
	packetOwner := acquireBytes(len(p.packet))
	packetOwner.AppendBytes(p.packet)
	return recoveryPacket{
		namespace:        p.namespace,
		offset:           p.offset,
		offsetSeq:        p.offsetSeq,
		opts:             p.opts,
		packet:           packetOwner.B,
		packetOwner:      packetOwner,
		attachments:      copyAttachmentsToBatch(p.attachmentViews()),
		releaseAfterSend: true,
		createdAt:        p.createdAt,
	}
}

func (s *recoverySession) clone() *recoverySession {
	if s == nil {
		return nil
	}
	return newRecoverySession(s.namespace, s.pid, s.sid, s.rooms, s.expiresAt)
}

func (s *recoverySession) release() {
	if s == nil {
		return
	}
	if s.owner != nil {
		s.owner.Release()
	}
	s.owner = nil
	s.pid = ""
	s.sid = ""
	s.rooms = nil
}

func newRecoverySession(namespace, pid string, sid SocketID, rooms []Room, expiresAt time.Time) *recoverySession {
	total := len(pid) + len(sid)
	for _, room := range rooms {
		total += len(room)
	}
	owner := acquireBytes(total)
	ownedPID := appendOwnedString(owner, pid)
	ownedSID := appendOwnedSocketID(owner, sid)
	ownedRooms := make([]Room, 0, len(rooms))
	for _, room := range rooms {
		ownedRooms = append(ownedRooms, appendOwnedRoom(owner, room))
	}
	return &recoverySession{
		namespace: namespace,
		pid:       ownedPID,
		sid:       ownedSID,
		rooms:     ownedRooms,
		expiresAt: expiresAt,
		owner:     owner,
	}
}

func appendOwnedString(owner *pooledBytes, value string) string {
	if value == "" {
		return ""
	}
	start := len(owner.B)
	owner.AppendString(value)
	return bytesToStringView(owner.B[start:])
}

func appendOwnedSocketID(owner *pooledBytes, value SocketID) SocketID {
	if value == "" {
		return ""
	}
	start := len(owner.B)
	owner.AppendSocketID(value)
	return SocketID(bytesToStringView(owner.B[start:]))
}

func (p *recoveryPacket) release() {
	if p.attachments != nil {
		p.attachments.Release()
		p.attachments = nil
	}
	if p.views != nil {
		p.views.Release()
		p.views = nil
	}
	if p.packetOwner != nil {
		p.packetOwner.Release()
		p.packetOwner = nil
	}
	p.packet = nil
}

func recoveryPacketMatches(opts broadcastOptions, rooms []Room) bool {
	roomSet := make(map[Room]struct{}, len(rooms))
	for _, room := range rooms {
		roomSet[room] = struct{}{}
	}
	for _, room := range opts.Except {
		if _, ok := roomSet[room]; ok {
			return false
		}
	}
	if len(opts.Rooms) == 0 {
		return true
	}
	for _, room := range opts.Rooms {
		if _, ok := roomSet[room]; ok {
			return true
		}
	}
	return false
}

func recoveryKey(namespace, pid string) string { return namespace + "\x00" + pid }

func copyAttachmentsToBatch(in [][]byte) *byteBatch {
	if len(in) == 0 {
		return nil
	}
	batch := acquireByteBatch(len(in), totalByteLen(in))
	for _, attachment := range in {
		batch.AppendBytes(attachment)
	}
	return batch
}

func totalByteLen(in [][]byte) int {
	total := 0
	for _, b := range in {
		total += len(b)
	}
	return total
}
