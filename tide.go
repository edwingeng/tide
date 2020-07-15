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

func (this *Tide) Launch() {
	if this.bManualDriving {
		panic(fmt.Errorf("<tide.%s> WithManualDriving'd Tide cannot be launched", this.name))
	}
	this.once.Do(func() {
		this.tick = time.NewTicker(this.beatInterval)
		this.chFlush = make(chan flushSignal)
		go this.engine()
	})
}

func (this *Tide) engine() {
	this.Infof("<tide.%s> started", this.name)
	for {
		select {
		case <-this.tick.C:
			this.engineImpl(false)
		case signal := <-this.chFlush:
			this.engineImpl(true)
			close(signal.done)
			if signal.quit {
				return
			}
		}
	}
}

func (this *Tide) engineImpl(force bool) {
	this.mu.Lock()
	n := this.dq.Len()
	t := this.startTime
	this.mu.Unlock()
	if n == 0 {
		goto exit
	}

	if n >= this.maxLen || !t.IsZero() && this.fNow().Sub(t) >= this.maxDelay || force {
		this.mu.Lock()
		a := this.dq.DequeueMany(0)
		this.startTime = time.Time{}
		if len(this.unique) > 0 {
			for k := range this.unique {
				delete(this.unique, k)
			}
		}
		this.mu.Unlock()
		if len(a) > 0 {
			this.process(a)
			n = len(a)
		}
	} else {
		n = 0
	}

exit:
	if this.stepByStep {
		select {
		case this.chStep <- n:
		case <-time.After(time.Second):
			this.Errorf("<tide.%s> no one is listening to the step-by-step channel", this.name)
		}
	}
}

func (this *Tide) process(a []live.Data) {
	defer func() {
		if r := recover(); r != nil {
			this.Error(fmt.Sprintf("<tide.%s> panic: %+v\n%s", this.name, r, debug.Stack()))
		}
	}()
	this.fn(a)
	atomic.AddInt64(&this.numProcessed, int64(len(a)))
}

func (this *Tide) Shutdown(ctx context.Context) error {
	return this.flushImpl(ctx, true)
}

func (this *Tide) Flush(ctx context.Context) error {
	return this.flushImpl(ctx, false)
}

func (this *Tide) flushImpl(ctx context.Context, quit bool) error {
	if this.bManualDriving {
		ch := make(chan struct{})
		go func() {
			this.engineImpl(true)
			ch <- struct{}{}
		}()
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	this.tick.Stop()
	signal := flushSignal{done: make(chan struct{}), quit: quit}
	this.chFlush <- signal
	select {
	case <-signal.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (this *Tide) Push(v live.Data) {
	this.mu.Lock()
	switch this.dq.Empty() {
	case true:
		this.startTime = this.fNow()
	}
	this.dq.Enqueue(v)
	this.mu.Unlock()
}

func (this *Tide) PushUnique(v live.Data, hint interface{}, overwrite bool) {
	this.mu.Lock()
	if idx, ok := this.unique[hint]; ok {
		if overwrite {
			this.dq.Replace(idx, v)
		}
		this.mu.Unlock()
		return
	}
	switch this.dq.Empty() {
	case true:
		this.startTime = this.fNow()
	}
	this.dq.Enqueue(v)
	n := this.dq.Len()
	this.unique[hint] = n - 1
	this.mu.Unlock()
}

func (this *Tide) QueueLen() int {
	this.mu.Lock()
	n := this.dq.Len()
	this.mu.Unlock()
	return n
}

func (this *Tide) NumProcessed() int64 {
	return atomic.LoadInt64(&this.numProcessed)
}

func (this *Tide) Beat() {
	if this.bManualDriving {
		this.engineImpl(false)
	} else {
		panic(fmt.Errorf("<tide.%s> WithManualDriving is missing", this.name))
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
