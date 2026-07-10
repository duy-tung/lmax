package lmax

import "sync/atomic"

// InitialSequence is the value of every sequence before anything has been
// published or consumed.
const InitialSequence int64 = -1

// Sequence is a monotonically increasing counter padded to its own cache
// lines so that concurrently updated sequences never false-share. Exactly one
// goroutine writes a given Sequence; any number may read it.
//
// Always share a Sequence by pointer (obtained from NewSequence); copying the
// value would defeat nothing functionally but wastes the padding guarantees.
type Sequence struct {
	_   [CacheLinePad]byte
	val atomic.Int64
	_   [CacheLinePad - 8]byte
}

// NewSequence returns a Sequence initialized to InitialSequence.
func NewSequence() *Sequence {
	s := &Sequence{}
	s.val.Store(InitialSequence)
	return s
}

// Load atomically reads the sequence value.
func (s *Sequence) Load() int64 { return s.val.Load() }

// Store atomically writes the sequence value, making all writes that
// happened-before it visible to goroutines that observe the new value.
func (s *Sequence) Store(v int64) { s.val.Store(v) }

// StoreRelease writes the sequence value with release (rather than
// sequentially consistent) semantics: earlier writes by this goroutine are
// visible to any goroutine that observes the new value, but the store does
// not act as a full barrier. This is the publication fast path — see
// store_release_amd64.go.
func (s *Sequence) StoreRelease(v int64) { storeRelease64(&s.val, v) }

// CompareAndSwap atomically replaces old with new and reports success.
func (s *Sequence) CompareAndSwap(old, new int64) bool {
	return s.val.CompareAndSwap(old, new)
}
