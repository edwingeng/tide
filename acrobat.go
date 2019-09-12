package acrobat

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwingeng/deque"
	"github.com/edwingeng/slog"
)

type Func func(a []interface{})

type Acrobat struct {
	slog.Logger
	name         string
	beatInterval time.Duration
	maxLen       int
	maxDelay     time.Duration
	fn           Func

	ticker *time.Ticker

	once         sync.Once
	numProcessed int64

	mu sync.Mutex
	dq deque.Deque

	startTime time.Time
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
	}
	for _, opt := range opts {
		opt(acrobat)
	}
	acrobat.ticker = time.NewTicker(acrobat.beatInterval)
	return
}

func (this *Acrobat) Launch() {
	this.once.Do(func() {
		go this.engine()
	})
}

func (this *Acrobat) engine() {
	this.Infof("<acrobat.%s> started", this.name)
	for range this.ticker.C {
		this.mu.Lock()
		n := this.dq.Len()
		t := this.startTime
		this.mu.Unlock()
		if n == 0 {
			continue
		}
		if n >= this.maxLen || (!t.IsZero() && time.Now().Sub(t) >= this.maxDelay) {
			this.mu.Lock()
			a := this.dq.DequeueMany(0)
			this.startTime = time.Time{}
			this.mu.Unlock()
			if len(a) > 0 {
				this.process(a)
			}
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

func (this *Acrobat) Push(v interface{}) {
	this.mu.Lock()
	switch this.dq.Empty() {
	case true:
		this.startTime = time.Now()
	}
	this.dq.Enqueue(v)
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
