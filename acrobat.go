package acrobat

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/deque"
	"github.com/edwingeng/slog"
)

type Func func(arr []interface{})

type Acrobat struct {
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
	dq deque.Deque

	startTime time.Time
	stop      chan chan struct{}
}

func NewAcrobat(name string, maxLen int, maxDelay time.Duration, fn Func, opts ...Option) (acrobat *Acrobat) {
	if fn == nil {
		panic("fn cannot be nil")
	}
	acrobat = &Acrobat{
		name:         name,
		Logger:       slog.ConsoleLogger{},
		beatInterval: time.Millisecond * 100,
		maxLen:       maxLen,
		maxDelay:     maxDelay,
		fn:           fn,
		dq:           deque.NewDeque(),
		unique:       make(map[interface{}]int),
		stop:         make(chan chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(acrobat)
	}
	return
}

func (this *Acrobat) Launch() {
	this.once.Do(func() {
		this.ticker = time.NewTicker(this.beatInterval)
		go this.engine()
	})
}

func (this *Acrobat) engine() {
	this.Infof("<acrobat.%s> started", this.name)
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

func (this *Acrobat) engineImpl(force bool) {
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
		if l := len(this.unique); l > 0 {
			switch {
			case l < 16:
				for k := range this.unique {
					delete(this.unique, k)
				}
			case l < 1024:
				this.unique = make(map[interface{}]int, l)
			default:
				this.unique = make(map[interface{}]int, 1024)
			}
		}
		this.mu.Unlock()
		if len(a) > 0 {
			this.process(a)
		}
	}
}

func (this *Acrobat) process(a []interface{}) {
	defer func() {
		if r := recover(); r != nil {
			this.Error(fmt.Sprintf("<acrobat.%s> panic: %+v\n%s", this.name, r, debug.Stack()))
		}
	}()
	this.fn(a)
	atomic.AddInt64(&this.numProcessed, int64(len(a)))
}

func (this *Acrobat) Shutdown(ctx context.Context) error {
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

func (this *Acrobat) Push(v interface{}) {
	this.mu.Lock()
	switch this.dq.Empty() {
	case true:
		this.startTime = time.Now()
	}
	this.dq.Enqueue(v)
	this.mu.Unlock()
}

func (this *Acrobat) PushUnique(v interface{}, hint interface{}, overwrite bool) {
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

func (this *Acrobat) QueueLen() int {
	this.mu.Lock()
	n := this.dq.Len()
	this.mu.Unlock()
	return n
}

func (this *Acrobat) NumProcessed() int64 {
	return atomic.LoadInt64(&this.numProcessed)
}

type Option func(acrobat *Acrobat)

func WithLogger(log slog.Logger) Option {
	return func(acrobat *Acrobat) {
		acrobat.Logger = log
	}
}

func WithBeatInterval(interval time.Duration) Option {
	return func(acrobat *Acrobat) {
		acrobat.beatInterval = interval
	}
}
