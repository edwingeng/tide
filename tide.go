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

type Tide struct {
	slog.Logger
	name         string
	beatInterval time.Duration
	maxLen       int
	maxDelay     time.Duration
	fn           Func

	ticker *time.Ticker
	unique map[interface{}]int

	once         sync.Once
	numProcessed int64

	mu sync.Mutex
	dq *Deque

	startTime time.Time
	stop      chan chan struct{}
}

func NewTide(name string, maxLen int, maxDelay time.Duration, fn Func, opts ...Option) (tide *Tide) {
	if fn == nil {
		panic("fn cannot be nil")
	}
	tide = &Tide{
		name:         name,
		Logger:       slog.ConsoleLogger{},
		beatInterval: time.Millisecond * 100,
		maxLen:       maxLen,
		maxDelay:     maxDelay,
		fn:           fn,
		dq:           NewDeque(),
		unique:       make(map[interface{}]int),
		stop:         make(chan chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(tide)
	}
	return
}

func (this *Tide) Launch() {
	this.once.Do(func() {
		this.ticker = time.NewTicker(this.beatInterval)
		go this.engine()
	})
}

func (this *Tide) engine() {
	this.Infof("<tide.%s> started", this.name)
	for {
		select {
		case <-this.ticker.C:
			this.engineImpl(false)
		case ch := <-this.stop:
			this.engineImpl(true)
			close(ch)
			return
		}
	}
}

func (this *Tide) engineImpl(force bool) {
	this.mu.Lock()
	n := this.dq.Len()
	t := this.startTime
	this.mu.Unlock()
	if n == 0 {
		return
	}

	if n >= this.maxLen || !t.IsZero() && time.Now().Sub(t) >= this.maxDelay || force {
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
	this.ticker.Stop()
	ch := make(chan struct{})
	this.stop <- ch
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (this *Tide) Push(v live.Data) {
	this.mu.Lock()
	switch this.dq.Empty() {
	case true:
		this.startTime = time.Now()
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
		this.startTime = time.Now()
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
