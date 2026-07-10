package lmax

import "runtime"

// SingleProducerSequencer is the fast path for exactly one publishing
// goroutine. next and cachedGate are deliberately plain fields: with a single
// writer there is no contention, only visibility — and visibility is provided
// by the cursor store in Publish. In the common case Next touches zero shared
// variables thanks to the cached gate.
type SingleProducerSequencer struct {
	cursor   *Sequence
	capacity int64
	wait     WaitStrategy
	signal   bool // strategy parks waiters and needs SignalAll per publish
	gating   []*Sequence

	next       int64 // highest claimed; producer goroutine only
	cachedGate int64 // cached min of gating sequences; producer goroutine only
}

// NewSingleProducerSequencer creates a sequencer for a power-of-two capacity.
func NewSingleProducerSequencer(capacity int64, wait WaitStrategy) *SingleProducerSequencer {
	if !isPow2(capacity) {
		panic("lmax: sequencer capacity must be a power of 2")
	}
	return &SingleProducerSequencer{
		cursor:     NewSequence(),
		capacity:   capacity,
		wait:       wait,
		signal:     needsSignal(wait),
		next:       InitialSequence,
		cachedGate: InitialSequence,
	}
}

func (s *SingleProducerSequencer) Cursor() *Sequence { return s.cursor }

func (s *SingleProducerSequencer) AddGating(seqs ...*Sequence) {
	s.gating = append(s.gating, seqs...)
}

func (s *SingleProducerSequencer) Next(n int64) int64 {
	if n < 1 {
		panic("lmax: Next requires n >= 1")
	}
	next := s.next + n
	wrap := next - s.capacity
	if wrap > s.cachedGate {
		for {
			min := minSeq(s.gating, s.next)
			s.cachedGate = min
			if wrap <= min {
				break
			}
			runtime.Gosched()
		}
	}
	s.next = next
	return next
}

func (s *SingleProducerSequencer) TryNext(n int64) (int64, bool) {
	if n < 1 {
		panic("lmax: TryNext requires n >= 1")
	}
	next := s.next + n
	wrap := next - s.capacity
	if wrap > s.cachedGate {
		min := minSeq(s.gating, s.next)
		s.cachedGate = min
		if wrap > min {
			return 0, false
		}
	}
	s.next = next
	return next, true
}

// Publish releases slots up to hi with a single cursor store; the atomic
// store is the release edge that makes the slot writes visible to consumers.
func (s *SingleProducerSequencer) Publish(lo, hi int64) {
	s.cursor.Store(hi)
	if s.signal {
		s.wait.SignalAll()
	}
}

// HighestPublished is trivially hi: with one producer the cursor alone proves
// publication of everything at or below it.
func (s *SingleProducerSequencer) HighestPublished(lo, hi int64) int64 { return hi }
