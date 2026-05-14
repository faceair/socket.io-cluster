package sio

import (
	"reflect"
	"sync"
)

type callbackStore[T any] struct {
	mu   sync.Mutex
	on   []T
	once []T
}

func (s *callbackStore[T]) add(f T, once bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if once {
		s.once = append(s.once, f)
	} else {
		s.on = append(s.on, f)
	}
}

func (s *callbackStore[T]) remove(fs ...T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(fs) == 0 {
		s.on = nil
		s.once = nil
		return
	}
	for _, f := range fs {
		s.on = removeFuncValue(s.on, f)
		s.once = removeFuncValue(s.once, f)
	}
}

func (s *callbackStore[T]) forEach(fn func(T)) {
	s.mu.Lock()
	on := append([]T(nil), s.on...)
	once := append([]T(nil), s.once...)
	s.once = nil
	s.mu.Unlock()
	for _, f := range on {
		fn(f)
	}
	for _, f := range once {
		fn(f)
	}
}

func removeFuncValue[T any](in []T, v T) []T {
	needle := reflect.ValueOf(v)
	if !needle.IsValid() || needle.Kind() != reflect.Func {
		return in
	}
	ptr := needle.Pointer()
	out := in[:0]
	for _, item := range in {
		rv := reflect.ValueOf(item)
		if !rv.IsValid() || rv.Kind() != reflect.Func || rv.Pointer() != ptr {
			out = append(out, item)
		}
	}
	return out
}
