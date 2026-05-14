package sio

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type lifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newLifecycle(parent context.Context) *lifecycle {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &lifecycle{ctx: ctx, cancel: cancel}
}

func (l *lifecycle) context() context.Context { return l.ctx }

func (l *lifecycle) start(name string, run func(context.Context)) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		run(l.ctx)
	}()
}

func (l *lifecycle) stop(timeout time.Duration) error {
	l.cancel()
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	if timeout <= 0 {
		<-done
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("sio: lifecycle stop timed out after %s", timeout)
	}
}
