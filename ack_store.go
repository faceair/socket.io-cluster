package sio

import (
	"sync"
	"time"
)

type ackStore struct {
	mu      sync.Mutex
	items   map[uint64]*ackEntry
	nextID  uint64
	nearest time.Time
}

type ackEntry struct {
	handler  *ackHandler
	deadline time.Time
}

func newAckStore() *ackStore {
	return &ackStore{items: make(map[uint64]*ackEntry)}
}

func (s *ackStore) next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	return id
}

func (s *ackStore) add(id uint64, h *ackHandler, timeout time.Duration, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := &ackEntry{handler: h}
	if timeout > 0 {
		entry.deadline = now.Add(timeout)
		if s.nearest.IsZero() || entry.deadline.Before(s.nearest) {
			s.nearest = entry.deadline
		}
	}
	s.items[id] = entry
}

func (s *ackStore) take(id uint64) (*ackHandler, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.items[id]
	if !ok {
		return nil, false
	}
	delete(s.items, id)
	return entry.handler, true
}

func (s *ackStore) sweep(now time.Time) (expired []*ackHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nearest = time.Time{}
	for id, entry := range s.items {
		if !entry.deadline.IsZero() && !now.Before(entry.deadline) {
			expired = append(expired, entry.handler)
			delete(s.items, id)
			continue
		}
		if !entry.deadline.IsZero() && (s.nearest.IsZero() || entry.deadline.Before(s.nearest)) {
			s.nearest = entry.deadline
		}
	}
	return expired
}
