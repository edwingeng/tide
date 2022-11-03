package tide

import (
	"context"
	"fmt"
	"github.com/edwingeng/deque/v2"
	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type Func func(arr []live.Data)

type flushSignal struct {
	done chan struct{}
	quit bool
}

type Tide struct {
	slog.Logger
	name         string
	handDriven   bool
	fNow         func() time.Time
	beatInterval time.Duration
	maxLen       int
	maxDelay     time.Duration
	fn           Func

	stepByStep bool
	chStep     chan int

	tick    *time.Ticker
	chFlush chan flushSignal

	once         sync.Once
	numProcessed int64

	mu     sync.Mutex
	unique map[any]int
	dq     *deque.Deque[live.Data]

	startTime time.Time
}

func NewTide(name string, maxLen int, maxDelay time.Duration, fn Func, opts ...Option) (tide *Tide) {
	if fn == nil {
		panic("fn cannot be nil")
	}
	tide = &Tide{
		name:         name,
		fNow:         time.Now,
		Logger:       slog.NewDevelopmentConfig().MustBuild(),
		beatInterval: time.Millisecond * 100,
		maxLen:       maxLen,
		maxDelay:     maxDelay,
		fn:           fn,
		dq:           deque.NewDeque[live.Data](),
		unique:       make(map[any]int),
	}
	for _, opt := range opts {
		opt(tide)
	}
	return
}

func (x *Tide) Launch() {
	if x.handDriven {
		panic(fmt.Errorf("the tide %q is hand-driven", x.name))
	}
	x.once.Do(func() {
		x.tick = time.NewTicker(x.beatInterval)
		x.chFlush = make(chan flushSignal)
		go x.engine()
	})
}

func (x *Tide) engine() {
	x.Infof("<tide.%s> started", x.name)
	defer func() {
		x.Infof("<tide.%s> stopped", x.name)
	}()
	for {
		select {
		case <-x.tick.C:
			x.engineImpl(false)
		case signal := <-x.chFlush:
			x.engineImpl(true)
			close(signal.done)
			if signal.quit {
				return
			}
		}
	}
}

func (x *Tide) engineImpl(force bool) {
	var a []live.Data
	x.mu.Lock()
	n := x.dq.Len()
	t := x.startTime
	x.mu.Unlock()
	if n == 0 {
		goto exit
	}

	if n >= x.maxLen || x.fNow().Sub(t) >= x.maxDelay || force {
		x.mu.Lock()
		a = x.dq.DequeueMany(0)
		x.startTime = time.Time{}
		if len(x.unique) > 0 {
			for k := range x.unique {
				delete(x.unique, k)
			}
		}
		x.mu.Unlock()
		x.processJobs(a)
	}

	n = len(a)
exit:
	if x.stepByStep {
		select {
		case x.chStep <- n:
		case <-time.After(time.Second):
			x.Errorf("<tide.%s> no one is listening to the step-by-step channel", x.name)
		}
	}
}

func (x *Tide) processJobs(a []live.Data) {
	defer func() {
		if r := recover(); r != nil {
			x.Error(fmt.Sprintf("<tide.%s> panic: %+v\n%s", x.name, r, debug.Stack()))
		}
	}()
	n := int64(len(a))
	atomic.AddInt64(&x.numProcessed, n)
	x.fn(a)
}

func (x *Tide) Shutdown(ctx context.Context) error {
	return x.flushImpl(ctx, true)
}

func (x *Tide) Flush(ctx context.Context) error {
	return x.flushImpl(ctx, false)
}

func (x *Tide) flushImpl(ctx context.Context, quit bool) error {
	if x.handDriven {
		if _, ok := ctx.Deadline(); !ok {
			x.engineImpl(true)
			return nil
		}

		done := make(chan struct{})
		go func() {
			x.engineImpl(true)
			done <- struct{}{}
		}()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if quit {
		x.tick.Stop()
	}
	done := make(chan struct{})
	select {
	case x.chFlush <- flushSignal{done: done, quit: quit}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (x *Tide) Push(v live.Data) {
	x.mu.Lock()
	x.dq.Enqueue(v)
	if !x.startTime.IsZero() {
		x.mu.Unlock()
		return
	}
	x.startTime = x.fNow()
	x.mu.Unlock()
}

func (x *Tide) PushUnique(v live.Data, hint any, overwrite bool) {
	x.mu.Lock()
	if idx, ok := x.unique[hint]; ok {
		if overwrite {
			x.dq.Replace(idx, v)
		}
		x.mu.Unlock()
		return
	}

	x.dq.Enqueue(v)
	x.unique[hint] = x.dq.Len() - 1
	if !x.startTime.IsZero() {
		x.mu.Unlock()
		return
	}
	x.startTime = x.fNow()
	x.mu.Unlock()
}

func (x *Tide) Len() int {
	x.mu.Lock()
	n := x.dq.Len()
	x.mu.Unlock()
	return n
}

func (x *Tide) NumProcessed() int64 {
	return atomic.LoadInt64(&x.numProcessed)
}

type Option func(tide *Tide)

func WithLogger(log slog.Logger) Option {
	return func(tide *Tide) {
		tide.Logger = log
	}
}

func WithBeatInterval(interval time.Duration) Option {
	return func(tide *Tide) {
		tide.beatInterval = interval
	}
}

func WithHandDriven(fNow func() time.Time) Option {
	return func(tide *Tide) {
		tide.handDriven = true
		tide.fNow = fNow
	}
}

func withStepByStep() Option {
	return func(tide *Tide) {
		tide.stepByStep = true
		tide.chStep = make(chan int)
	}
}
