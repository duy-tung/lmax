package lmax

// EventHandler processes events as they become available. OnEvent receives a
// pointer into the ring: the slot may be reused by producers as soon as this
// consumer's sequence advances past seq, so handlers must not retain e.
// endOfBatch is true for the last event of the currently available batch,
// which lets flush-style handlers (e.g. journalers) amortize expensive
// operations across a batch.
type EventHandler[T any] interface {
	OnEvent(e *T, seq int64, endOfBatch bool)
}

// EventHandlerFunc adapts a plain function to the EventHandler interface.
type EventHandlerFunc[T any] func(e *T, seq int64, endOfBatch bool)

func (f EventHandlerFunc[T]) OnEvent(e *T, seq int64, endOfBatch bool) { f(e, seq, endOfBatch) }

// EventProcessor is one consumer: a goroutine running the canonical batch
// loop over the ring, gated by its barrier.
type EventProcessor[T any] struct {
	ring    *RingBuffer[T]
	barrier *SequenceBarrier
	handler EventHandler[T]
	seq     *Sequence
}

func newEventProcessor[T any](ring *RingBuffer[T], barrier *SequenceBarrier, handler EventHandler[T]) *EventProcessor[T] {
	return &EventProcessor[T]{ring: ring, barrier: barrier, handler: handler, seq: NewSequence()}
}

// Sequence exposes this consumer's progress, for gating and dependency edges.
func (p *EventProcessor[T]) Sequence() *Sequence { return p.seq }

// run consumes until the barrier is alerted. Progress is published once per
// batch, not per event — when a consumer falls behind it catches up with a
// single release store per batch instead of ping-ponging cache lines per
// element.
func (p *EventProcessor[T]) run() {
	next := p.seq.Load() + 1
	for {
		avail, err := p.barrier.WaitFor(next)
		if err != nil {
			return
		}
		for ; next <= avail; next++ {
			p.handler.OnEvent(p.ring.Get(next), next, next == avail)
		}
		p.seq.StoreRelease(avail)
	}
}
