package lmax

import "fmt"

// RingBuffer is pre-allocated storage for events, indexed by monotonically
// increasing sequences. Slot lookup is seq & mask, so the capacity must be a
// power of two.
//
// Get returns a pointer into the ring. The pointer is valid for a producer
// between claiming the sequence (Sequencer.Next) and publishing it, and for a
// consumer only until the consumer's sequence advances past it. Retaining the
// pointer beyond that window is a data race by contract.
type RingBuffer[T any] struct {
	entries []T
	mask    int64
}

// NewRingBuffer allocates a ring of the given power-of-two capacity.
func NewRingBuffer[T any](capacity int64) *RingBuffer[T] {
	if !isPow2(capacity) {
		panic(fmt.Sprintf("lmax: ring buffer capacity must be a power of 2, got %d", capacity))
	}
	return &RingBuffer[T]{entries: make([]T, capacity), mask: capacity - 1}
}

// Get returns a pointer to the slot for seq.
func (r *RingBuffer[T]) Get(seq int64) *T { return &r.entries[seq&r.mask] }

// Capacity returns the number of slots in the ring.
func (r *RingBuffer[T]) Capacity() int64 { return int64(len(r.entries)) }
