// Package lmax is an LMAX-Disruptor-style inter-goroutine messaging library:
// a pre-allocated ring buffer coordinated by padded sequence counters and
// memory barriers instead of locks. See docs/PLAN.md for the full design.
package lmax

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

type config struct {
	capacity int64
	multi    bool
	wait     WaitStrategy
}

// Option configures a Disruptor at construction time.
type Option func(*config)

// WithCapacity sets the ring capacity; it must be a power of two.
// The default is 1024.
func WithCapacity(n int64) Option { return func(c *config) { c.capacity = n } }

// WithSingleProducer selects the single-publisher fast path (the default).
// Publishing from more than one goroutine with this option is a data race.
func WithSingleProducer() Option { return func(c *config) { c.multi = false } }

// WithMultiProducer allows concurrent publishers at the cost of a CAS claim
// and per-slot publication tracking.
func WithMultiProducer() Option { return func(c *config) { c.multi = true } }

// WithWaitStrategy sets how consumers wait for events. The default is
// Yielding. A *Blocking strategy must be shared, so pass a pointer.
func WithWaitStrategy(ws WaitStrategy) Option { return func(c *config) { c.wait = ws } }

// Disruptor wires a ring buffer, a sequencer, and a DAG of event processors,
// and owns their lifecycle.
//
// Build the consumer graph with HandleWith/After before Start; publish with
// Next/Get/Publish (claim a slot, fill it in place, release it).
type Disruptor[T any] struct {
	ring *RingBuffer[T]
	seqr Sequencer
	wait WaitStrategy

	procs   []*EventProcessor[T]
	depSeqs map[*Sequence]bool // sequences used as a dependency of some group

	started atomic.Bool
	stopped atomic.Bool
	wg      sync.WaitGroup
}

// New creates a Disruptor for events of type T.
func New[T any](opts ...Option) (*Disruptor[T], error) {
	cfg := config{capacity: 1024, wait: Yielding{}}
	for _, o := range opts {
		o(&cfg)
	}
	if !isPow2(cfg.capacity) {
		return nil, fmt.Errorf("lmax: capacity must be a power of 2, got %d", cfg.capacity)
	}
	if cfg.wait == nil {
		return nil, errors.New("lmax: wait strategy must not be nil")
	}
	var seqr Sequencer
	if cfg.multi {
		seqr = NewMultiProducerSequencer(cfg.capacity, cfg.wait)
	} else {
		seqr = NewSingleProducerSequencer(cfg.capacity, cfg.wait)
	}
	return &Disruptor[T]{
		ring:    NewRingBuffer[T](cfg.capacity),
		seqr:    seqr,
		wait:    cfg.wait,
		depSeqs: make(map[*Sequence]bool),
	}, nil
}

// HandlerGroup is a set of consumers created together; use it as the
// dependency of a later stage via After.
type HandlerGroup[T any] struct {
	d    *Disruptor[T]
	seqs []*Sequence
}

// Then adds handlers that run after every handler in this group.
func (g *HandlerGroup[T]) Then(handlers ...EventHandler[T]) *HandlerGroup[T] {
	return g.d.handleWith(g.seqs, handlers)
}

// After starts building a stage that depends on all the given groups.
type After[T any] struct {
	d    *Disruptor[T]
	deps []*Sequence
}

// HandleWith adds root consumers, gated only on the producer cursor. Each
// handler runs in its own goroutine and sees every event.
func (d *Disruptor[T]) HandleWith(handlers ...EventHandler[T]) *HandlerGroup[T] {
	return d.handleWith(nil, handlers)
}

// After declares dependencies for the next stage:
// d.After(a, b).HandleWith(h) runs h only on events both a and b finished.
func (d *Disruptor[T]) After(groups ...*HandlerGroup[T]) *After[T] {
	if len(groups) == 0 {
		panic("lmax: After requires at least one group")
	}
	var deps []*Sequence
	for _, g := range groups {
		if g.d != d {
			panic("lmax: After called with a group from a different disruptor")
		}
		deps = append(deps, g.seqs...)
	}
	return &After[T]{d: d, deps: deps}
}

// HandleWith adds the stage's handlers, gated on the declared dependencies.
func (a *After[T]) HandleWith(handlers ...EventHandler[T]) *HandlerGroup[T] {
	return a.d.handleWith(a.deps, handlers)
}

func (d *Disruptor[T]) handleWith(deps []*Sequence, handlers []EventHandler[T]) *HandlerGroup[T] {
	if d.started.Load() {
		panic("lmax: cannot add handlers after Start")
	}
	if len(handlers) == 0 {
		panic("lmax: HandleWith requires at least one handler")
	}
	g := &HandlerGroup[T]{d: d}
	for _, h := range handlers {
		if h == nil {
			panic("lmax: nil EventHandler")
		}
		p := newEventProcessor(d.ring, newSequenceBarrier(d.seqr, d.wait, deps), h)
		d.procs = append(d.procs, p)
		g.seqs = append(g.seqs, p.Sequence())
	}
	for _, s := range deps {
		d.depSeqs[s] = true
	}
	return g
}

// Start validates the graph, gates the producer on the leaf consumers, and
// spawns one goroutine per handler.
func (d *Disruptor[T]) Start() error {
	if d.started.Swap(true) {
		return errors.New("lmax: Start called twice")
	}
	if len(d.procs) == 0 {
		return errors.New("lmax: no handlers registered")
	}
	// The producer only needs to gate on the leaves: every non-leaf is
	// bounded from above by its downstream consumers.
	var leaves []*Sequence
	for _, p := range d.procs {
		if !d.depSeqs[p.Sequence()] {
			leaves = append(leaves, p.Sequence())
		}
	}
	d.seqr.AddGating(leaves...)
	for _, p := range d.procs {
		d.wg.Add(1)
		go func(p *EventProcessor[T]) {
			defer d.wg.Done()
			p.run()
		}(p)
	}
	return nil
}

// Next claims one slot, spinning while the ring is full, and returns its
// sequence. Fill the slot via Get, then release it with Publish.
func (d *Disruptor[T]) Next() int64 { return d.seqr.Next(1) }

// NextN claims n slots and returns the highest sequence; the caller owns
// hi-n+1 .. hi and must PublishRange the same range.
func (d *Disruptor[T]) NextN(n int64) int64 { return d.seqr.Next(n) }

// TryNext claims one slot without blocking; ok is false if the ring is full.
func (d *Disruptor[T]) TryNext() (int64, bool) { return d.seqr.TryNext(1) }

// Get returns a pointer to the slot for seq, valid per the RingBuffer
// contract.
func (d *Disruptor[T]) Get(seq int64) *T { return d.ring.Get(seq) }

// Publish releases a single claimed slot to consumers.
func (d *Disruptor[T]) Publish(seq int64) { d.seqr.Publish(seq, seq) }

// PublishRange releases the claimed slots lo..hi to consumers.
func (d *Disruptor[T]) PublishRange(lo, hi int64) { d.seqr.Publish(lo, hi) }

// Cursor returns the highest published (single producer) or claimed
// (multi-producer) sequence.
func (d *Disruptor[T]) Cursor() int64 { return d.seqr.Cursor().Load() }

// Shutdown drains outstanding events and stops all processors. Callers must
// have stopped publishing first. It waits — bounded by ctx — until every
// consumer has processed everything published, then alerts the barriers and
// joins the goroutines.
func (d *Disruptor[T]) Shutdown(ctx context.Context) error {
	if !d.started.Load() {
		return errors.New("lmax: Shutdown before Start")
	}
	if d.stopped.Swap(true) {
		return errors.New("lmax: Shutdown called twice")
	}
	for !d.drained() {
		if err := ctx.Err(); err != nil {
			d.halt()
			return err
		}
		runtime.Gosched()
	}
	d.halt()
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Disruptor[T]) drained() bool {
	cursor := d.seqr.Cursor().Load()
	for _, p := range d.procs {
		if p.Sequence().Load() < cursor {
			return false
		}
	}
	return true
}

func (d *Disruptor[T]) halt() {
	for _, p := range d.procs {
		p.barrier.Alert()
	}
	d.wait.SignalAll()
}
