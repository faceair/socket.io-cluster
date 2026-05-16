package sio

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLocalAdapterUnionExceptAndMutation(t *testing.T) {
	a := newLocalAdapter()
	s1 := &serverSocket{id: "s1"}
	s2 := &serverSocket{id: "s2"}
	s3 := &serverSocket{id: "s3"}
	a.addSocket(s1)
	a.addSocket(s2)
	a.addSocket(s3)
	a.addAll("s1", s1, []Room{"r1", "r2"})
	a.addAll("s2", s2, []Room{"r2"})
	a.addAll("s3", s3, []Room{"r3"})

	got := map[SocketID]int{}
	count := a.apply(broadcastOptions{Rooms: []Room{"r1", "r2"}, Except: []Room{"s2"}}, func(s *serverSocket) {
		got[s.id]++
	})
	if count != 1 || got["s1"] != 1 {
		t.Fatalf("unexpected match count=%d got=%v", count, got)
	}

	done := make(chan struct{})
	go func() {
		a.apply(broadcastOptions{Rooms: []Room{"r1"}}, func(s *serverSocket) {
			a.addAll(s.id, s, []Room{"joined-inside-callback"})
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("adapter apply deadlocked when callback mutated memberships")
	}
	rooms := a.socketRooms("s1")
	found := false
	for _, room := range rooms {
		if room == "joined-inside-callback" {
			found = true
		}
	}
	if !found {
		t.Fatalf("mutation from callback was not applied, rooms=%v", rooms)
	}
}

func TestLocalAdapterConcurrentJoinLeaveAndApply(t *testing.T) {
	a := newLocalAdapter()
	const socketCount = 128
	sockets := make([]*serverSocket, 0, socketCount)
	for i := 0; i < socketCount; i++ {
		s := &serverSocket{id: SocketID(fmt.Sprintf("s%d", i))}
		a.addSocket(s)
		sockets = append(sockets, s)
	}

	var wg sync.WaitGroup
	for i, socket := range sockets {
		wg.Add(1)
		go func(i int, socket *serverSocket) {
			defer wg.Done()
			room := Room(fmt.Sprintf("room-%d", i%8))
			for j := 0; j < 100; j++ {
				a.addAll(socket.id, socket, []Room{"hot", room})
				_ = a.apply(broadcastOptions{Rooms: []Room{"hot"}}, func(*serverSocket) {})
				a.delete(socket.id, "hot")
				a.delete(socket.id, room)
			}
		}(i, socket)
	}
	wg.Wait()

	socketsN, _, memberships := a.stats()
	if socketsN != socketCount {
		t.Fatalf("sockets = %d, want %d", socketsN, socketCount)
	}
	if memberships != socketCount {
		t.Fatalf("memberships = %d, want only default sid rooms %d", memberships, socketCount)
	}
}
