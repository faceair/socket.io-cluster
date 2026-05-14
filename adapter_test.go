package sio

import (
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
