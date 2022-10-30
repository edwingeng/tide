package tide

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/live"
	"github.com/edwingeng/slog"
)

type Func func(arr []live.Data)

type flushSignal struct {
	done chan struct{}
	quit bool
}

type Tide struct {
	slog.Logger
	name           string
	bManualDriving bool
	fNow           func() time.Time
	beatInterval   time.Duration
	maxLen         int
	maxDelay       time.Duration
	fn             Func

	stepByStep bool
	chStep     chan int

	tick    *time.Ticker
	chFlush chan flushSignal

	once         sync.Once
	numProcessed int64

	mu     sync.Mutex
	unique map[interface{}]int
	dq     *Deque

	startTime time.Time
}

func NewTide(name string, maxLen int, maxDelay time.Duration, fn Func, opts ...Option) (tide *Tide) {
	if fn == nil {
		panic("fn cannot be nil")
	}
	tide = &Tide{
		name:         name,
		fNow:         time.Now,
		Logger:       slog.ConsoleLogger{},
		beatInterval: time.Millisecond * 100,
		maxLen:       maxLen,
		maxDelay:     maxDelay,
		fn:           fn,
		dq:           NewDeque(),
		unique:       make(map[interface{}]int),
	}
	for _, opt := range opts {
		opt(tide)
	}
	return
}

func (x *Tide) Launch() {
	if x.bManualDriving {
		panic(fmt.Errorf("<tide.%s> WithManualDriving'd Tide cannot be launched", x.name))
	}
	x.once.Do(func() {
		x.tick = time.NewTicker(x.beatInterval)
		x.chFlush = make(chan flushSignal, 16)
		go x.engine()
	})
}

func (x *Tide) engine() {
	x.Infof("<tide.%s> started", x.name)
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
	x.mu.Lock()
	n := x.dq.Len()
	t := x.startTime
	x.mu.Unlock()
	if n == 0 {
		goto exit
	}

	if n >= x.maxLen || !t.IsZero() && x.fNow().Sub(t) >= x.maxDelay || force {
		x.mu.Lock()
		a := x.dq.DequeueMany(0)
		x.startTime = time.Time{}
		if len(x.unique) > 0 {
			for k := range x.unique {
				delete(x.unique, k)
			}
		}
		x.mu.Unlock()
		if len(a) > 0 {
			x.process(a)
			n = len(a)
		}
	} else {
		n = 0
	}

exit:
	if x.stepByStep {
		select {
		case x.chStep <- n:
		case <-time.After(time.Second):
			x.Errorf("<tide.%s> no one is listening to the step-by-step channel", x.name)
		}
	}
}

func (x *Tide) process(a []live.Data) {
	defer func() {
		if r := recover(); r != nil {
			x.Error(fmt.Sprintf("<tide.%s> panic: %+v\n%s", x.name, r, debug.Stack()))
		}
	}()
	x.fn(a)
	atomic.AddInt64(&x.numProcessed, int64(len(a)))
}

func (x *Tide) Shutdown(ctx context.Context) error {
	return x.flushImpl(ctx, true)
}

func (x *Tide) Flush(ctx context.Context) error {
	return x.flushImpl(ctx, false)
}

func (x *Tide) flushImpl(ctx context.Context, quit bool) error {
	if x.bManualDriving {
		ch := make(chan struct{})
		go func() {
			x.engineImpl(true)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if quit {
		x.tick.Stop()
	}
	signal := flushSignal{done: make(chan struct{}), quit: quit}
	x.chFlush <- signal
	select {
	case <-signal.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (x *Tide) Push(v live.Data) {
	x.mu.Lock()
	switch x.dq.Empty() {
	case true:
		x.startTime = x.fNow()
	}
	x.dq.Enqueue(v)
	x.mu.Unlock()
}

func (x *Tide) PushUnique(v live.Data, hint interface{}, overwrite bool) {
	x.mu.Lock()
	if idx, ok := x.unique[hint]; ok {
		if overwrite {
			x.dq.Replace(idx, v)
		}
		x.mu.Unlock()
		return
	}
	switch x.dq.Empty() {
	case true:
		x.startTime = x.fNow()
	}
	x.dq.Enqueue(v)
	n := x.dq.Len()
	x.unique[hint] = n - 1
	x.mu.Unlock()
}

func (x *Tide) QueueLen() int {
	x.mu.Lock()
	n := x.dq.Len()
	x.mu.Unlock()
	return n
}

func (x *Tide) NumProcessed() int64 {
	return atomic.LoadInt64(&x.numProcessed)
}

func (x *Tide) Beat() {
	if x.bManualDriving {
		x.engineImpl(false)
	} else {
		panic(fmt.Errorf("<tide.%s> WithManualDriving is missing", x.name))
	}
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

func WithManualDriving(fNow func() time.Time) Option {
	return func(tide *Tide) {
		tide.bManualDriving = true
		tide.fNow = fNow
	}
}

func withStepByStep() Option {
	return func(tide *Tide) {
		tide.stepByStep = true
		tide.chStep = make(chan int)
	}
}
