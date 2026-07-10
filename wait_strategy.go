package lmax

import (
	"errors"
	"runtime"
	"sync"
	"time"
)

// ErrAlerted is returned from barrier waits when the disruptor is shutting
// down.
var ErrAlerted = errors.New("lmax: sequence barrier alerted")

// WaitStrategy decides how a consumer (or the barrier on its behalf) waits
// for a sequence to become available. Strategies trade CPU for latency.
type WaitStrategy interface {
	// WaitFor blocks until dependent() >= seq, returning the observed value,
	// or returns ErrAlerted once alerted() reports true.
	WaitFor(seq int64, dependent func() int64, alerted func() bool) (int64, error)
	// SignalAll wakes any blocked waiters. It is called by producers after
	// publishing and during shutdown; non-blocking strategies no-op.
	SignalAll()
}

// NonSignaling is implemented by strategies whose SignalAll is a no-op.
// Sequencers skip the per-publish SignalAll interface call for them — a
// measurable saving, since the dynamic call sits in the hottest function.
// Polling strategies (spin/yield/sleep) should implement it returning true;
// strategies that park waiters must not. Unknown strategies are treated
// conservatively as needing signals.
type NonSignaling interface {
	NoSignalNeeded() bool
}

func needsSignal(ws WaitStrategy) bool {
	if ns, ok := ws.(NonSignaling); ok {
		return !ns.NoSignalNeeded()
	}
	return true
}

// BusySpin spins as hard as possible. Lowest latency, burns a core per
// waiter, and never yields to the Go scheduler — only use it when
// GOMAXPROCS comfortably exceeds the number of spinning goroutines.
type BusySpin struct{}

func (BusySpin) WaitFor(seq int64, dependent func() int64, alerted func() bool) (int64, error) {
	for {
		if v := dependent(); v >= seq {
			return v, nil
		}
		if alerted() {
			return 0, ErrAlerted
		}
	}
}

func (BusySpin) SignalAll() {}

// Yielding spins briefly, then repeatedly yields the processor with
// runtime.Gosched. Low latency without starving other goroutines; the
// recommended default.
type Yielding struct{}

const yieldSpinTries = 100

func (Yielding) WaitFor(seq int64, dependent func() int64, alerted func() bool) (int64, error) {
	spins := yieldSpinTries
	for {
		if v := dependent(); v >= seq {
			return v, nil
		}
		if alerted() {
			return 0, ErrAlerted
		}
		if spins > 0 {
			spins--
		} else {
			runtime.Gosched()
		}
	}
}

func (Yielding) SignalAll() {}

// Sleeping spins, then yields, then sleeps between polls. Near-zero CPU when
// idle at the cost of wake-up latency around the sleep interval.
type Sleeping struct {
	// Interval between polls once the strategy has backed off to sleeping.
	// Zero means a 100µs default.
	Interval time.Duration
}

func (s Sleeping) WaitFor(seq int64, dependent func() int64, alerted func() bool) (int64, error) {
	interval := s.Interval
	if interval <= 0 {
		interval = 100 * time.Microsecond
	}
	spins := yieldSpinTries
	yields := yieldSpinTries
	for {
		if v := dependent(); v >= seq {
			return v, nil
		}
		if alerted() {
			return 0, ErrAlerted
		}
		switch {
		case spins > 0:
			spins--
		case yields > 0:
			yields--
			runtime.Gosched()
		default:
			time.Sleep(interval)
		}
	}
}

func (Sleeping) SignalAll() {}

// Blocking parks waiters on a condition variable and relies on producers
// signalling after each publish. Lowest CPU, highest latency. Safe for
// zero-value use, but must not be copied after first use.
type Blocking struct {
	mu   sync.Mutex
	cond *sync.Cond
	once sync.Once
}

func (b *Blocking) init() { b.cond = sync.NewCond(&b.mu) }

func (b *Blocking) WaitFor(seq int64, dependent func() int64, alerted func() bool) (int64, error) {
	b.once.Do(b.init)
	for {
		if alerted() {
			return 0, ErrAlerted
		}
		if v := dependent(); v >= seq {
			return v, nil
		}
		b.mu.Lock()
		for dependent() < seq && !alerted() {
			b.cond.Wait()
		}
		b.mu.Unlock()
	}
}

func (b *Blocking) SignalAll() {
	b.once.Do(b.init)
	b.mu.Lock()
	b.cond.Broadcast()
	b.mu.Unlock()
}

// NoSignalNeeded reports that BusySpin never blocks and needs no signal.
func (BusySpin) NoSignalNeeded() bool { return true }

// NoSignalNeeded reports that Yielding never blocks and needs no signal.
func (Yielding) NoSignalNeeded() bool { return true }

// NoSignalNeeded reports that Sleeping never blocks and needs no signal.
func (Sleeping) NoSignalNeeded() bool { return true }
