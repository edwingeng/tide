package tide

import (
	"context"
	"errors"
	"fmt"
	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct {
	sync.Mutex
	time.Time
}

func newClock() clock {
	return clock{Time: time.Now()}
}

func (c *clock) Add(d time.Duration) {
	c.Lock()
	c.Time = c.Time.Add(d)
	c.Unlock()
}

func (c *clock) Now() time.Time {
	c.Lock()
	now := c.Time
	c.Unlock()
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

func (h *handler) Func(arr []live.Data) {
	atomic.AddInt64(&h.n, int64(len(arr)))
}

func (h *handler) N() int64 {
	return atomic.LoadInt64(&h.n)
}

func TestTide_BigDelay(t *testing.T) {
	var h handler
	c := newClock()
	logger := slog.NewDumbLogger()
	tide := NewTide("test", 10, time.Second*10, h.Func, WithLogger(logger),
		WithHandDriven(c.Now), withStepByStep())
	go tide.engineImpl(false)
	if err := stepDown(tide, 0); err != nil {
		t.Fatal(err)
	}

	tide.Push(live.WrapInt(100))
	tide.Push(live.Nil)
	if tide.Len() != 2 {
		t.Fatalf("the length should be 2. len: %d", tide.Len())
	}

	c.Add(time.Second * 2)
	go tide.engineImpl(false)
	if err := stepDown(tide, 0); err != nil {
		t.Fatal(err)
	}
	if tide.Len() != 2 {
		t.Fatalf("the length should not change. len: %d", tide.Len())
	}

	tide.mu.Lock()
	for i := 0; i < 10; i++ {
		tide.dq.Enqueue(live.WrapInt(200))
	}
	tide.mu.Unlock()

	c.Add(time.Second * 2)
	go tide.engineImpl(false)
	if err := stepDown(tide, 12); err != nil {
		t.Fatal(err)
	}
	if tide.Len() != 0 {
		t.Fatalf("the length should be 0. len: %d", tide.Len())
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

	tide.Push(live.WrapInt(300))
	tide.mu.Lock()
	startTime2 := tide.startTime
	tide.mu.Unlock()
	if startTime2.IsZero() {
		t.Fatal("startTime should NOT be zero now")
	}
}

func TestTide_BigCapacity(t *testing.T) {
	var h handler
	c := newClock()
	logger := slog.NewDumbLogger()
	tide := NewTide("test", 1000, time.Millisecond*50, h.Func, WithLogger(logger),
		WithHandDriven(c.Now), withStepByStep())

	for i := 0; i < 12; i++ {
		tide.Push(live.WrapInt(200))
	}
	if tide.Len() != 12 {
		t.Fatalf("the length should be 12. len: %d", tide.Len())
	}

	for i := 0; i < 10; i++ {
		var wanted int
		if i == 9 {
			wanted = 12
		}
		c.Add(time.Millisecond * 5)
		go tide.engineImpl(false)
		if err := stepDown(tide, wanted); err != nil {
			t.Fatal(err)
		}
	}
	if tide.Len() != 0 {
		t.Fatalf("the length should be 0. len: %d", tide.Len())
	}
	if v := h.N(); v != 12 {
		t.Fatalf("h.n should be 12. h.n: %d", v)
	}
}

func TestTide_Panic(t *testing.T) {
	func() {
		defer func() {
			_ = recover()
		}()
		NewTide("test", 1, time.Second, nil)
		t.Fatal("no panic")
	}()

	func() {
		defer func() {
			_ = recover()
		}()
		var h handler
		tide := NewTide("test", 1, time.Second, h.Func, WithHandDriven(time.Now))
		tide.Launch()
		t.Fatal("no panic")
	}()

	func() {
		fn := func(arr []live.Data) {
			panic("beta")
		}
		scav := slog.NewScavenger()
		tide := NewTide("test", 1, time.Second, fn, WithLogger(scav), WithHandDriven(time.Now))
		tide.Push(live.WrapInt(100))
		_ = tide.Flush(context.Background())
		if scav.Len() != 1 {
			t.Fatal(`scav.Len() != 1`)
		}
		if !scav.StringExists(" panic: ") {
			t.Fatal(`!scav.StringExists(" panic: ")`)
		}
	}()
}

func TestTide_Stress(t *testing.T) {
	var h handler
	c := newClock()
	scav := slog.NewScavenger()
	tide := NewTide("test", 10, time.Second*10, h.Func, WithLogger(scav),
		WithHandDriven(c.Now), withStepByStep())

	const numGoroutines = 1000
	const maxNumJobs = 1000
	var total int64
	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := rand.Intn(maxNumJobs)
			atomic.AddInt64(&total, int64(n))
			for j := 0; j < n; j++ {
				tide.Push(live.WrapInt(j))
				if j > 0 && j%100 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}

	wg.Wait()
	c.Add(time.Millisecond * 20)
	go tide.engineImpl(false)
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
	var h handler
	c := newClock()
	scav := slog.NewScavenger()
	tide := NewTide("test", 10, time.Millisecond, h.Func, WithLogger(scav),
		WithHandDriven(c.Now), withStepByStep())

	tide.PushUnique(live.WrapInt(1), 100, false)
	if tide.Len() != 1 {
		t.Fatal("tide.Len() != 1")
	}
	tide.PushUnique(live.WrapInt(1), 100, false)
	if tide.Len() != 1 {
		t.Fatal("tide.Len() != 1")
	}
	tide.PushUnique(live.WrapInt(2), 200, false)
	if tide.Len() != 2 {
		t.Fatal("tide.Len() != 2")
	}
	tide.PushUnique(live.WrapInt(5), 200, false)
	if tide.Len() != 2 {
		t.Fatal("tide.Len() != 2")
	}
	if tide.dq.Peek(1).Int() != 2 {
		t.Fatal("tide.dq.Peek(1).Int() != 2")
	}
	tide.PushUnique(live.WrapInt(5), 200, true)
	if tide.Len() != 2 {
		t.Fatal("tide.Len() != 2")
	}
	if tide.dq.Peek(1).Int() != 5 {
		t.Fatal("tide.dq.Peek(1).Int() != 5")
	}

	tide.Push(live.WrapInt(1))
	tide.Push(live.WrapInt(2))
	if tide.Len() != 4 {
		t.Fatal("tide.Len() != 4")
	}

	c.Add(time.Millisecond)
	go tide.engineImpl(false)
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
	var h handler
	scav := slog.NewScavenger()
	tide := NewTide("test", 10, time.Second*10, h.Func, WithLogger(scav), WithBeatInterval(time.Millisecond))
	tide.Push(live.WrapInt(100))
	tide.Push(live.WrapInt(200))
	tide.Push(live.WrapInt(300))

	tide.Launch()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel1()
	if err := tide.Shutdown(ctx1); err != nil {
		t.Fatal(err)
	}
	if h.N() != 3 {
		t.Fatal("h.N() != 3")
	}
	if !scav.StringExists("stopped") {
		t.Fatal(`!scav.StringExists("stopped")`)
	}
}

func TestTide_Shutdown2(t *testing.T) {
	var h handler
	c := newClock()
	logger := slog.NewDumbLogger()
	tide := NewTide("test", 10, time.Second*10, h.Func, WithLogger(logger),
		WithHandDriven(c.Now), withStepByStep())
	tide.Push(live.WrapInt(100))
	tide.Push(live.WrapInt(200))
	tide.Push(live.WrapInt(300))

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := tide.Shutdown(ctx1); err == nil || !os.IsTimeout(err) {
		t.Fatal("Shutdown should return a timeout error")
	}
}

func TestTide_Flush1(t *testing.T) {
	var h handler
	logger := slog.NewDumbLogger()
	tide := NewTide("test", 10, time.Second*10, h.Func, WithLogger(logger), WithBeatInterval(time.Millisecond))
	tide.Push(live.WrapInt(100))
	tide.Push(live.WrapInt(200))
	tide.Push(live.WrapInt(300))

	tide.Launch()
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel1()
	if err := tide.Flush(ctx1); err != nil {
		t.Fatal(err)
	}
	if h.N() != 3 {
		t.Fatal("h.N() != 3")
	}
}

func TestTide_Flush2(t *testing.T) {
	fn := func(arr []live.Data) {
		time.Sleep(time.Millisecond * 50)
	}
	logger := slog.NewDumbLogger()
	tide := NewTide("test", 10, time.Second*10, fn, WithLogger(logger), WithBeatInterval(time.Millisecond))
	tide.Push(live.WrapInt(100))
	tide.Launch()

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := tide.Flush(ctx1); err == nil {
		t.Fatal(`err == nil`)
	} else if !os.IsTimeout(err) {
		t.Fatal(`!os.IsTimeout(err)`)
	}
}

func TestTide_Flush3(t *testing.T) {
	var sleeping int64
	fn := func(arr []live.Data) {
		atomic.StoreInt64(&sleeping, 1)
		time.Sleep(time.Millisecond * 50)
	}
	logger := slog.NewDumbLogger()
	tide := NewTide("test", 10, time.Second*10, fn, WithLogger(logger), WithBeatInterval(time.Millisecond))
	tide.Push(live.WrapInt(100))
	tide.Launch()

	go func() {
		_ = tide.Flush(context.Background())
	}()

	for atomic.LoadInt64(&sleeping) == 0 {
		runtime.Gosched()
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := tide.Flush(ctx1); err == nil {
		t.Fatal(`err == nil`)
	} else if !os.IsTimeout(err) {
		t.Fatal(`!os.IsTimeout(err)`)
	}
}

func TestTide_Flush4(t *testing.T) {
	var h handler
	tide := NewTide("test", 10, time.Second*10, h.Func, WithHandDriven(time.Now))
	tide.Push(live.WrapInt(100))
	tide.Push(live.WrapInt(200))
	tide.Push(live.WrapInt(300))

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel1()
	if err := tide.Flush(ctx1); err != nil {
		t.Fatal(err)
	}
	if h.N() != 3 {
		t.Fatal("h.N() != 3")
	}
}

func TestTide_Flush5(t *testing.T) {
	fn := func(arr []live.Data) {
		time.Sleep(time.Millisecond * 50)
	}
	tide := NewTide("test", 10, time.Second*10, fn, WithHandDriven(time.Now))
	tide.Push(live.WrapInt(100))

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	if err := tide.Flush(ctx1); err == nil {
		t.Fatal(`err == nil`)
	} else if !os.IsTimeout(err) {
		t.Fatal(`!os.IsTimeout(err)`)
	}
}
