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
	avail      []availSlot
}

// availSlot pads each publication flag to its own cache line: without
// padding, 16 flags share a line and producers publishing neighboring
// sequences false-share it on every event (O5; ~10% at 3 producers, at the
// cost of 64B ring capacity in bookkeeping per slot).
type availSlot struct {
	v atomic.Int32
	_ [60]byte
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
		avail:      make([]availSlot, capacity),
	}
	for i := range s.avail {
		s.avail[i].v.Store(-1)
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
	if n < 1 || n > s.capacity {
		panic("lmax: Next requires 1 <= n <= capacity")
	}
	backoff := int32(1)
	for {
		current := s.cursor.Load()
		if !s.hasCapacity(n, current) {
			runtime.Gosched()
			continue
		}
		if s.cursor.CompareAndSwap(current, current+n) {
			return current + n
		}
		// Lost the CAS: back off before re-reading so the losers don't
		// keep hammering the cursor line while the winner publishes.
		procYield(backoff)
		if backoff < 16 {
			backoff <<= 1
		}
	}
}

func (s *MultiProducerSequencer) TryNext(n int64) (int64, bool) {
	if n < 1 || n > s.capacity {
		panic("lmax: TryNext requires 1 <= n <= capacity")
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
		storeRelease32(&s.avail[seq&s.mask].v, int32(seq>>s.shift))
	}
	if s.signal {
		s.wait.SignalAll()
	}
}

func (s *MultiProducerSequencer) HighestPublished(lo, hi int64) int64 {
	for seq := lo; seq <= hi; seq++ {
		if s.avail[seq&s.mask].v.Load() != int32(seq>>s.shift) {
			return seq - 1
		}
	}
	return hi
}
