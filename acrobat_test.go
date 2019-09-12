package acrobat

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwingeng/slog"
)

type counter struct {
	n int32
}

func (this *counter) count(a []interface{}) {
	atomic.AddInt32(&this.n, int32(len(a)))
}

func TestAcrobat_BigDelay(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	acr := NewAcrobat("test", 10, time.Second*10, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	acr.Push(100)
	acr.Push(nil)
	if acr.QueueLen() != 2 {
		t.Fatalf("the length of acr.cmdQueue should be 2. len: %d", acr.QueueLen())
	}

	acr.Launch()
	defer func() {
		acr.ticker.Stop()
	}()

	time.Sleep(time.Millisecond * 20)
	if acr.QueueLen() != 2 {
		t.Fatalf("the length of acr.cmdQueue should not change. len: %d", acr.QueueLen())
	}

	acr.mu.Lock()
	for i := 0; i < 10; i++ {
		acr.dq.Enqueue(200)
	}
	acr.mu.Unlock()

	time.Sleep(time.Millisecond * 20)
	if acr.QueueLen() != 0 {
		t.Fatalf("the length of acr.cmdQueue should be 0. len: %d", acr.QueueLen())
	}
	if v := atomic.LoadInt32(&c.n); v != 12 {
		t.Fatalf("c.n should be 12. c.n: %d", v)
	}

	acr.mu.Lock()
	startTime1 := acr.startTime
	acr.mu.Unlock()
	if !startTime1.IsZero() {
		t.Fatal("startTime should be zero now")
	}

	acr.Push(300)
	acr.mu.Lock()
	startTime2 := acr.startTime
	acr.mu.Unlock()
	if startTime2.IsZero() {
		t.Fatal("startTime should NOT be zero now")
	}
}

func TestAcrobat_BigCapacity(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	acr := NewAcrobat("test", 1000, time.Millisecond*50, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	acr.Launch()
	defer func() {
		acr.ticker.Stop()
	}()

	for i := 0; i < 12; i++ {
		acr.Push(200)
	}
	if acr.QueueLen() != 12 {
		t.Fatalf("the length of acr.cmdQueue should be 12. len: %d", acr.QueueLen())
	}

	done := make(chan struct{})
	go func() {
		for range time.Tick(time.Millisecond * 5) {
			if atomic.LoadInt32(&c.n) > 0 {
				close(done)
				return
			}
		}
	}()

	select {
	case <-time.After(time.Millisecond * 80):
	case <-done:
	}
	if acr.QueueLen() != 0 {
		t.Fatalf("the length of acr.cmdQueue should be 0. len: %d", acr.QueueLen())
	}
	if v := atomic.LoadInt32(&c.n); v != 12 {
		t.Fatalf("c.n should be 12. c.n: %d", v)
	}
}

func TestAcrobat_NilFn(t *testing.T) {
	defer func() {
		_ = recover()
	}()
	NewAcrobat("test", 1, time.Second, nil)
	t.Fatal("no panic")
}
