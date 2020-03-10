package tide

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

func TestTide_BigDelay(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	tide := NewTide("test", 10, time.Second*10, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	tide.Push(liveHelper.WrapInt(100))
	tide.Push(live.Nil)
	if tide.QueueLen() != 2 {
		t.Fatalf("the length of tide.cmdQueue should be 2. len: %d", tide.QueueLen())
	}

	tide.Launch()
	defer func() {
		tide.ticker.Stop()
	}()

	time.Sleep(time.Millisecond * 20)
	if tide.QueueLen() != 2 {
		t.Fatalf("the length of tide.cmdQueue should not change. len: %d", tide.QueueLen())
	}

	tide.mu.Lock()
	for i := 0; i < 10; i++ {
		tide.dq.Enqueue(liveHelper.WrapInt(200))
	}
	tide.mu.Unlock()

	time.Sleep(time.Millisecond * 20)
	if tide.QueueLen() != 0 {
		t.Fatalf("the length of tide.cmdQueue should be 0. len: %d", tide.QueueLen())
	}
	if v := c.N(); v != 12 {
		t.Fatalf("c.n should be 12. c.n: %d", v)
	}

	tide.mu.Lock()
	startTime1 := tide.startTime
	tide.mu.Unlock()
	if !startTime1.IsZero() {
		t.Fatal("startTime should be zero now")
	}

	tide.Push(liveHelper.WrapInt(300))
	tide.mu.Lock()
	startTime2 := tide.startTime
	tide.mu.Unlock()
	if startTime2.IsZero() {
		t.Fatal("startTime should NOT be zero now")
	}
}

func TestTide_BigCapacity(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	tide := NewTide("test", 1000, time.Millisecond*50, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	tide.Launch()
	defer func() {
		tide.ticker.Stop()
	}()

	liveHelper := live.NewHelper(nil, nil)
	for i := 0; i < 12; i++ {
		tide.Push(liveHelper.WrapInt(200))
	}
	if tide.QueueLen() != 12 {
		t.Fatalf("the length of tide.cmdQueue should be 12. len: %d", tide.QueueLen())
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
	if tide.QueueLen() != 0 {
		t.Fatalf("the length of tide.cmdQueue should be 0. len: %d", tide.QueueLen())
	}
	if v := c.N(); v != 12 {
		t.Fatalf("c.n should be 12. c.n: %d", v)
	}
}

func TestTide_NilFn(t *testing.T) {
	defer func() {
		_ = recover()
	}()
	NewTide("test", 1, time.Second, nil)
	t.Fatal("no panic")
}

func TestTide_Stress(t *testing.T) {
	var scav slog.Scavenger
	var c counter
	tide := NewTide("test", 10, time.Second*10, c.count, WithLogger(&scav), WithBeatInterval(time.Millisecond))
	tide.Launch()
	defer func() {
		tide.ticker.Stop()
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
				tide.Push(liveHelper.WrapInt(j))
			}
		}()
	}

	wg.Wait()
	time.Sleep(time.Millisecond * 20)
	if c.N() != atomic.LoadInt64(&total) {
		t.Fatal("c.N() != atomic.LoadInt64(&total)")
	}
	if c.N() != tide.NumProcessed() {
		t.Fatal("c.N() != tide.NumProcessed()")
	}
	if scav.Len() != 1 {
		t.Fatal("scav.Len() != 1")
	}
}

func TestTide_PushUnique(t *testing.T) {
	var scav slog.Scavenger
	var c counter
	tide := NewTide("test", 10, time.Millisecond, c.count, WithLogger(&scav), WithBeatInterval(time.Millisecond))

	liveHelper := live.NewHelper(nil, nil)
	tide.PushUnique(liveHelper.WrapInt(1), 100, false)
	if tide.QueueLen() != 1 {
		t.Fatal("tide.QueueLen() != 1")
	}
	tide.PushUnique(liveHelper.WrapInt(1), 100, false)
	if tide.QueueLen() != 1 {
		t.Fatal("tide.QueueLen() != 1")
	}
	tide.PushUnique(liveHelper.WrapInt(2), 200, false)
	if tide.QueueLen() != 2 {
		t.Fatal("tide.QueueLen() != 2")
	}
	tide.PushUnique(liveHelper.WrapInt(5), 200, false)
	if tide.QueueLen() != 2 {
		t.Fatal("tide.QueueLen() != 2")
	}
	if tide.dq.Peek(1).ToInt() != 2 {
		t.Fatal("tide.dq.Peek(1).ToInt() != 2")
	}
	tide.PushUnique(liveHelper.WrapInt(5), 200, true)
	if tide.QueueLen() != 2 {
		t.Fatal("tide.QueueLen() != 2")
	}
	if tide.dq.Peek(1).ToInt() != 5 {
		t.Fatal("tide.dq.Peek(1).ToInt() != 5")
	}

	tide.Push(liveHelper.WrapInt(1))
	tide.Push(liveHelper.WrapInt(2))
	if tide.QueueLen() != 4 {
		t.Fatal("tide.QueueLen() != 4")
	}

	tide.Launch()
	defer func() {
		tide.ticker.Stop()
	}()

	for c.N() == 0 {
		runtime.Gosched()
	}
	tide.mu.Lock()
	leN := len(tide.unique)
	tide.mu.Unlock()
	if leN != 0 {
		t.Fatal("leN != 0")
	}
}

func TestTide_Shutdown1(t *testing.T) {
	var log slog.DumbLogger
	var c counter
	tide := NewTide("test", 10, time.Second*10, c.count, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	tide.Push(liveHelper.WrapInt(100))
	tide.Push(liveHelper.WrapInt(200))
	tide.Push(liveHelper.WrapInt(300))

	tide.Launch()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel1()
	if err := tide.Shutdown(ctx1); err != nil {
		t.Fatal(err)
	}
	if c.N() != 3 {
		t.Fatal("c.N() != 3")
	}
}

func TestTide_Shutdown2(t *testing.T) {
	var log slog.DumbLogger
	sleep := func(arr []live.Data) {
		time.Sleep(time.Millisecond * 100)
	}
	tide := NewTide("test", 10, time.Second*10, sleep, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	tide.Push(liveHelper.WrapInt(100))
	tide.Push(liveHelper.WrapInt(200))
	tide.Push(liveHelper.WrapInt(300))

	tide.Launch()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := tide.Shutdown(ctx1); err == nil || !os.IsTimeout(err) {
		t.Fatal("Shutdown should return a timeout error")
	}
}
