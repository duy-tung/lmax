package lmax

import (
	"runtime"
	"sync/atomic"
)

// SequenceBarrier coordinates a consumer with the producer cursor and any
// upstream consumers it depends on. Dependency chains are what turn a set of
// independent consumers into a pipeline or DAG without extra queues.
type SequenceBarrier struct {
	seqr    Sequencer
	cursor  *Sequence
	deps    []*Sequence
	wait    WaitStrategy
	alerted atomic.Bool

	// Method values allocated once here so WaitFor stays allocation-free.
	depFn   func() int64
	alertFn func() bool
}

func newSequenceBarrier(seqr Sequencer, wait WaitStrategy, deps []*Sequence) *SequenceBarrier {
	b := &SequenceBarrier{seqr: seqr, cursor: seqr.Cursor(), deps: deps, wait: wait}
	if len(deps) == 0 {
		// Root consumers poll the cursor directly — no per-poll branch on
		// len(deps) and one less call level in the spin loop.
		b.depFn = b.cursor.Load
	} else {
		b.depFn = b.dependentMin
	}
	b.alertFn = b.alerted.Load
	return b
}

// dependentMin is the highest sequence this barrier's consumer may advance
// to: the slowest upstream consumer if there are dependencies, otherwise the
// producer cursor. (Upstream consumers can never exceed the cursor, so when
// deps exist the cursor need not be consulted.)
func (b *SequenceBarrier) dependentMin() int64 {
	if len(b.deps) == 0 {
		return b.cursor.Load()
	}
	return minSeq(b.deps, b.deps[0].Load())
}

// WaitFor blocks until sequence seq has been published (and processed by all
// upstream dependencies), returning the highest available sequence >= seq so
// the caller can consume a whole batch. It returns ErrAlerted on shutdown.
func (b *SequenceBarrier) WaitFor(seq int64) (int64, error) {
	for {
		if b.alerted.Load() {
			return 0, ErrAlerted
		}
		avail, err := b.wait.WaitFor(seq, b.depFn, b.alertFn)
		if err != nil {
			return 0, err
		}
		if avail < seq {
			continue
		}
		// With multiple producers the cursor tracks claims, not publishes;
		// trim the batch to the contiguous published prefix.
		if hi := b.seqr.HighestPublished(seq, avail); hi >= seq {
			return hi, nil
		}
		runtime.Gosched()
	}
}

// Alert puts the barrier into shutdown state, waking and failing any waiters.
func (b *SequenceBarrier) Alert() {
	b.alerted.Store(true)
	b.wait.SignalAll()
}
