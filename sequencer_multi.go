package lmax

import (
	"runtime"
	"sync/atomic"
)

// MultiProducerSequencer supports concurrent publishers. Claims are a CAS
// loop on the cursor (which therefore tracks the highest *claimed* sequence,
// not the highest published). Publication is recorded per slot in avail,
// which stores the round number seq >> shift for slot seq & mask — so
// consumers can detect the contiguous published prefix even when producers
// finish out of order.
type MultiProducerSequencer struct {
	cursor     *Sequence // highest claimed
	cachedGate *Sequence // shared cache of min gating sequence
	capacity   int64
	mask       int64
	shift      uint
	wait       WaitStrategy
	signal     bool // strategy parks waiters and needs SignalAll per publish
	gating     []*Sequence
	avail      []atomic.Int32
}

// NewMultiProducerSequencer creates a sequencer for a power-of-two capacity.
func NewMultiProducerSequencer(capacity int64, wait WaitStrategy) *MultiProducerSequencer {
	if !isPow2(capacity) {
		panic("lmax: sequencer capacity must be a power of 2")
	}
	s := &MultiProducerSequencer{
		cursor:     NewSequence(),
		cachedGate: NewSequence(),
		capacity:   capacity,
		mask:       capacity - 1,
		shift:      log2(capacity),
		wait:       wait,
		signal:     needsSignal(wait),
		avail:      make([]atomic.Int32, capacity),
	}
	for i := range s.avail {
		s.avail[i].Store(-1)
	}
	return s
}

func (s *MultiProducerSequencer) Cursor() *Sequence { return s.cursor }

func (s *MultiProducerSequencer) AddGating(seqs ...*Sequence) {
	s.gating = append(s.gating, seqs...)
}

func (s *MultiProducerSequencer) hasCapacity(n, current int64) bool {
	wrap := current + n - s.capacity
	cached := s.cachedGate.Load()
	if wrap > cached || cached > current {
		min := minSeq(s.gating, current)
		// Only store on change: an unconditional store from every producer
		// on this path keeps the cachedGate line in Modified state bouncing
		// between cores; skipping redundant stores lets it stay Shared.
		if min != cached {
			s.cachedGate.Store(min)
		}
		return wrap <= min
	}
	return true
}

func (s *MultiProducerSequencer) Next(n int64) int64 {
	if n < 1 {
		panic("lmax: Next requires n >= 1")
	}
	for {
		current := s.cursor.Load()
		if !s.hasCapacity(n, current) {
			runtime.Gosched()
			continue
		}
		if s.cursor.CompareAndSwap(current, current+n) {
			return current + n
		}
	}
}

func (s *MultiProducerSequencer) TryNext(n int64) (int64, bool) {
	if n < 1 {
		panic("lmax: TryNext requires n >= 1")
	}
	for {
		current := s.cursor.Load()
		if !s.hasCapacity(n, current) {
			return 0, false
		}
		if s.cursor.CompareAndSwap(current, current+n) {
			return current + n, true
		}
	}
}

func (s *MultiProducerSequencer) Publish(lo, hi int64) {
	for seq := lo; seq <= hi; seq++ {
		storeRelease32(&s.avail[seq&s.mask], int32(seq>>s.shift))
	}
	if s.signal {
		s.wait.SignalAll()
	}
}

func (s *MultiProducerSequencer) HighestPublished(lo, hi int64) int64 {
	for seq := lo; seq <= hi; seq++ {
		if s.avail[seq&s.mask].Load() != int32(seq>>s.shift) {
			return seq - 1
		}
	}
	return hi
}
