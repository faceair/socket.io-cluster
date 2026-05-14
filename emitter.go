package sio

import "time"

type Emitter struct {
	target  emitTarget
	timeout time.Duration
}

type emitTarget interface {
	emitWithOptions(eventName string, timeout time.Duration, v ...any)
}

func (e Emitter) Emit(eventName string, v ...any) {
	e.target.emitWithOptions(eventName, e.timeout, v...)
}

func (e Emitter) Timeout(timeout time.Duration) Emitter {
	e.timeout = timeout
	return e
}
