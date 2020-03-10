package acrobat

import (
	"context"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
)

type counter struct {
	n int64
}

func (this *counter) count(arr []live.Data) {
	atomic.AddInt64(&this.n, int64(len(arr)))
}

func (this *counter) N() int64 {
	return atomic.LoadInt64(&this.n)
}

func TestAcrobat_BigDelay(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	acr := NewAcrobat("test", 10, time.Second*10, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	acr.Push(liveHelper.WrapInt(100))
	acr.Push(live.Nil)
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
		acr.dq.Enqueue(liveHelper.WrapInt(200))
	}
	acr.mu.Unlock()

	time.Sleep(time.Millisecond * 20)
	if acr.QueueLen() != 0 {
		t.Fatalf("the length of acr.cmdQueue should be 0. len: %d", acr.QueueLen())
	}
	if v := c.N(); v != 12 {
		t.Fatalf("c.n should be 12. c.n: %d", v)
	}

	acr.mu.Lock()
	startTime1 := acr.startTime
	acr.mu.Unlock()
	if !startTime1.IsZero() {
		t.Fatal("startTime should be zero now")
	}

	acr.Push(liveHelper.WrapInt(300))
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

	liveHelper := live.NewHelper(nil, nil)
	for i := 0; i < 12; i++ {
		acr.Push(liveHelper.WrapInt(200))
	}
	if acr.QueueLen() != 12 {
		t.Fatalf("the length of acr.cmdQueue should be 12. len: %d", acr.QueueLen())
	}

	done := make(chan struct{})
	go func() {
		for range time.Tick(time.Millisecond * 5) {
			if c.N() > 0 {
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
	if v := c.N(); v != 12 {
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

func TestAcrobat_Stress(t *testing.T) {
	var scav slog.Scavenger
	var c counter
	acr := NewAcrobat("test", 10, time.Second*10, c.count, WithLogger(&scav), WithBeatInterval(time.Millisecond))
	acr.Launch()
	defer func() {
		acr.ticker.Stop()
	}()

	liveHelper := live.NewHelper(nil, nil)
	const numGoroutines = 2000
	const maxNumJobs = 10000
	var total int64
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			n := rand.Intn(maxNumJobs)
			atomic.AddInt64(&total, int64(n))
			for j := 0; j < n; j++ {
				acr.Push(liveHelper.WrapInt(j))
			}
		}()
	}

	wg.Wait()
	time.Sleep(time.Millisecond * 20)
	if c.N() != atomic.LoadInt64(&total) {
		t.Fatal("c.N() != atomic.LoadInt64(&total)")
	}
	if c.N() != acr.NumProcessed() {
		t.Fatal("c.N() != acr.NumProcessed()")
	}
	if scav.Len() != 1 {
		t.Fatal("scav.Len() != 1")
	}
}

func TestAcrobat_PushUnique(t *testing.T) {
	var scav slog.Scavenger
	var c counter
	acr := NewAcrobat("test", 10, time.Millisecond, c.count, WithLogger(&scav), WithBeatInterval(time.Millisecond))

	liveHelper := live.NewHelper(nil, nil)
	acr.PushUnique(liveHelper.WrapInt(1), 100, false)
	if acr.QueueLen() != 1 {
		t.Fatal("acr.QueueLen() != 1")
	}
	acr.PushUnique(liveHelper.WrapInt(1), 100, false)
	if acr.QueueLen() != 1 {
		t.Fatal("acr.QueueLen() != 1")
	}
	acr.PushUnique(liveHelper.WrapInt(2), 200, false)
	if acr.QueueLen() != 2 {
		t.Fatal("acr.QueueLen() != 2")
	}
	acr.PushUnique(liveHelper.WrapInt(5), 200, false)
	if acr.QueueLen() != 2 {
		t.Fatal("acr.QueueLen() != 2")
	}
	if acr.dq.Peek(1).ToInt() != 2 {
		t.Fatal("acr.dq.Peek(1).ToInt() != 2")
	}
	acr.PushUnique(liveHelper.WrapInt(5), 200, true)
	if acr.QueueLen() != 2 {
		t.Fatal("acr.QueueLen() != 2")
	}
	if acr.dq.Peek(1).ToInt() != 5 {
		t.Fatal("acr.dq.Peek(1).ToInt() != 5")
	}

	acr.Push(liveHelper.WrapInt(1))
	acr.Push(liveHelper.WrapInt(2))
	if acr.QueueLen() != 4 {
		t.Fatal("acr.QueueLen() != 4")
	}

	acr.Launch()
	defer func() {
		acr.ticker.Stop()
	}()

	for c.N() == 0 {
		runtime.Gosched()
	}
	acr.mu.Lock()
	leN := len(acr.unique)
	acr.mu.Unlock()
	if leN != 0 {
		t.Fatal("leN != 0")
	}
}

func TestAcrobat_Shutdown1(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	acr := NewAcrobat("test", 10, time.Second*10, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	acr.Push(liveHelper.WrapInt(100))
	acr.Push(liveHelper.WrapInt(200))
	acr.Push(liveHelper.WrapInt(300))

	acr.Launch()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel1()
	if err := acr.Shutdown(ctx1); err != nil {
		t.Fatal(err)
	}
	if c.N() != 3 {
		t.Fatal("c.N() != 3")
	}
}

func TestAcrobat_Shutdown2(t *testing.T) {
	var log slog.DumbLogger
	sleep := func(arr []live.Data) {
		time.Sleep(time.Millisecond * 100)
	}
	acr := NewAcrobat("test", 10, time.Second*10, sleep, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	acr.Push(liveHelper.WrapInt(100))
	acr.Push(liveHelper.WrapInt(200))
	acr.Push(liveHelper.WrapInt(300))

	acr.Launch()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := acr.Shutdown(ctx1); err == nil || !os.IsTimeout(err) {
		t.Fatal("Shutdown should return a timeout error")
	}
}
