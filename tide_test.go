package tide

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
)

type clock struct {
	sync.Mutex
	time.Time
}

func newClock() clock {
	return clock{Time: time.Now()}
}

func (this *clock) Add(d time.Duration) {
	this.Lock()
	this.Time = this.Time.Add(d)
	this.Unlock()
}

func (this *clock) Now() time.Time {
	this.Lock()
	now := this.Time
	this.Unlock()
	return now
}

func stepDown(tide *Tide, n int) error {
	t := time.After(time.Second)
	for {
		select {
		case v := <-tide.chStep:
			n -= v
			if n < 0 {
				return fmt.Errorf("n < 0. n: %d, v: %d", n, v)
			} else if n == 0 {
				return nil
			}
		case <-t:
			return errors.New("timeout")
		}
	}
}

type handler struct {
	n int64
}

func (this *handler) F(arr []live.Data) {
	atomic.AddInt64(&this.n, int64(len(arr)))
}

func (this *handler) N() int64 {
	return atomic.LoadInt64(&this.n)
}

func TestTide_BigDelay(t *testing.T) {
	var log slog.DumbLogger
	var h handler
	c := newClock()
	tide := NewTide("test", 10, time.Second*10, h.F, WithLogger(&log), WithManualDrive(c.Now), withStepByStep())
	go tide.Beat()
	if err := stepDown(tide, 0); err != nil {
		t.Fatal(err)
	}

	liveHelper := live.NewHelper(nil, nil)
	tide.Push(liveHelper.WrapInt(100))
	tide.Push(live.Nil)
	if tide.QueueLen() != 2 {
		t.Fatalf("the length of tide.cmdQueue should be 2. len: %d", tide.QueueLen())
	}

	c.Add(time.Millisecond * 20)
	go tide.Beat()
	if err := stepDown(tide, 0); err != nil {
		t.Fatal(err)
	}
	if tide.QueueLen() != 2 {
		t.Fatalf("the length of tide.cmdQueue should not change. len: %d", tide.QueueLen())
	}

	tide.mu.Lock()
	for i := 0; i < 10; i++ {
		tide.dq.Enqueue(liveHelper.WrapInt(200))
	}
	tide.mu.Unlock()

	c.Add(time.Millisecond * 20)
	go tide.Beat()
	if err := stepDown(tide, 12); err != nil {
		t.Fatal(err)
	}
	if tide.QueueLen() != 0 {
		t.Fatalf("the length of tide.cmdQueue should be 0. len: %d", tide.QueueLen())
	}
	if v := h.N(); v != 12 {
		t.Fatalf("h.n should be 12. h.n: %d", v)
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
	var h handler
	c := newClock()
	tide := NewTide("test", 1000, time.Millisecond*50, h.F, WithLogger(&log), WithManualDrive(c.Now), withStepByStep())

	liveHelper := live.NewHelper(nil, nil)
	for i := 0; i < 12; i++ {
		tide.Push(liveHelper.WrapInt(200))
	}
	if tide.QueueLen() != 12 {
		t.Fatalf("the length of tide.cmdQueue should be 12. len: %d", tide.QueueLen())
	}

	for i := 0; i < 10; i++ {
		var wanted int
		if i == 9 {
			wanted = 12
		}
		c.Add(time.Millisecond * 5)
		go tide.Beat()
		if err := stepDown(tide, wanted); err != nil {
			t.Fatal(err)
		}
	}
	if tide.QueueLen() != 0 {
		t.Fatalf("the length of tide.cmdQueue should be 0. len: %d", tide.QueueLen())
	}
	if v := h.N(); v != 12 {
		t.Fatalf("h.n should be 12. h.n: %d", v)
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
	var h handler
	c := newClock()
	tide := NewTide("test", 10, time.Second*10, h.F, WithLogger(&scav), WithManualDrive(c.Now), withStepByStep())

	liveHelper := live.NewHelper(nil, nil)
	const numGoroutines = 2000
	const maxNumJobs = 1000
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
	c.Add(time.Millisecond * 20)
	go tide.Beat()
	if err := stepDown(tide, int(atomic.LoadInt64(&total))); err != nil {
		t.Fatal(err)
	}
	if h.N() != atomic.LoadInt64(&total) {
		t.Fatal("h.N() != atomic.LoadInt64(&total)")
	}
	if h.N() != tide.NumProcessed() {
		t.Fatal("h.N() != tide.NumProcessed()")
	}
	if scav.Len() != 0 {
		t.Fatal("scav.Len() != 0")
	}
}

func TestTide_PushUnique(t *testing.T) {
	var scav slog.Scavenger
	var h handler
	c := newClock()
	tide := NewTide("test", 10, time.Millisecond, h.F, WithLogger(&scav), WithManualDrive(c.Now), withStepByStep())

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

	c.Add(time.Millisecond)
	go tide.Beat()
	if err := stepDown(tide, 4); err != nil {
		t.Fatal(err)
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
	var h handler
	tide := NewTide("test", 10, time.Second*10, h.F, WithLogger(&log), WithBeatInterval(time.Millisecond))
	liveHelper := live.NewHelper(nil, nil)
	tide.Push(liveHelper.WrapInt(100))
	tide.Push(liveHelper.WrapInt(200))
	tide.Push(liveHelper.WrapInt(300))

	tide.Launch()
	time.Sleep(time.Millisecond * 20)
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel1()
	if err := tide.Shutdown(ctx1); err != nil {
		t.Fatal(err)
	}
	if h.N() != 3 {
		t.Fatal("h.N() != 3")
	}
}

func TestTide_Shutdown2(t *testing.T) {
	var log slog.DumbLogger
	sleep := func(arr []live.Data) {
		time.Sleep(time.Millisecond * 100)
	}
	c := newClock()
	tide := NewTide("test", 10, time.Second*10, sleep, WithLogger(&log), WithManualDrive(c.Now), withStepByStep())
	liveHelper := live.NewHelper(nil, nil)
	tide.Push(liveHelper.WrapInt(100))
	tide.Push(liveHelper.WrapInt(200))
	tide.Push(liveHelper.WrapInt(300))

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := tide.Shutdown(ctx1); err == nil || !os.IsTimeout(err) {
		t.Fatal("Shutdown should return a timeout error")
	}
}
