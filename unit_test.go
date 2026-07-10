package lmax

import (
	"testing"
	"unsafe"
)

func TestSequenceInitialValue(t *testing.T) {
	s := NewSequence()
	if got := s.Load(); got != InitialSequence {
		t.Fatalf("initial value = %d, want %d", got, InitialSequence)
	}
}

func TestSequenceStoreLoadCAS(t *testing.T) {
	s := NewSequence()
	s.Store(42)
	if got := s.Load(); got != 42 {
		t.Fatalf("Load = %d, want 42", got)
	}
	if s.CompareAndSwap(41, 43) {
		t.Fatal("CAS with wrong old value succeeded")
	}
	if !s.CompareAndSwap(42, 43) {
		t.Fatal("CAS with correct old value failed")
	}
	if got := s.Load(); got != 43 {
		t.Fatalf("Load after CAS = %d, want 43", got)
	}
}

func TestSequencePadding(t *testing.T) {
	want := uintptr(2 * CacheLinePad)
	if got := unsafe.Sizeof(Sequence{}); got != want {
		t.Fatalf("Sequence size = %d, want %d", got, want)
	}
}

func TestRingBufferRejectsNonPowerOfTwo(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for capacity 3")
		}
	}()
	NewRingBuffer[int](3)
}

func TestRingBufferWrapIndexing(t *testing.T) {
	r := NewRingBuffer[int64](4)
	for seq := int64(0); seq < 16; seq++ {
		*r.Get(seq) = seq
	}
	// After 4 full laps, slot i holds the last sequence mapping to it.
	for i := int64(0); i < 4; i++ {
		if got := *r.Get(i); got != 12+i {
			t.Errorf("slot %d = %d, want %d", i, got, 12+i)
		}
	}
	if r.Capacity() != 4 {
		t.Errorf("Capacity = %d, want 4", r.Capacity())
	}
}

func TestUtilHelpers(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		pow2 bool
	}{{1, true}, {2, true}, {1024, true}, {0, false}, {-4, false}, {3, false}, {6, false}} {
		if got := isPow2(tc.n); got != tc.pow2 {
			t.Errorf("isPow2(%d) = %v, want %v", tc.n, got, tc.pow2)
		}
	}
	for _, tc := range []struct{ n, l int64 }{{1, 0}, {2, 1}, {8, 3}, {1024, 10}} {
		if got := log2(tc.n); got != uint(tc.l) {
			t.Errorf("log2(%d) = %d, want %d", tc.n, got, tc.l)
		}
	}
}

func TestSingleProducerTryNextFull(t *testing.T) {
	s := NewSingleProducerSequencer(4, Yielding{})
	gate := NewSequence() // consumer stuck at -1
	s.AddGating(gate)

	hi, ok := s.TryNext(4)
	if !ok || hi != 3 {
		t.Fatalf("TryNext(4) = %d, %v; want 3, true", hi, ok)
	}
	s.Publish(0, 3)
	if _, ok := s.TryNext(1); ok {
		t.Fatal("TryNext succeeded on a full ring")
	}
	gate.Store(0) // consumer frees one slot
	hi, ok = s.TryNext(1)
	if !ok || hi != 4 {
		t.Fatalf("TryNext(1) after freeing = %d, %v; want 4, true", hi, ok)
	}
}

func TestMultiProducerTryNextFull(t *testing.T) {
	s := NewMultiProducerSequencer(4, Yielding{})
	gate := NewSequence()
	s.AddGating(gate)

	hi, ok := s.TryNext(4)
	if !ok || hi != 3 {
		t.Fatalf("TryNext(4) = %d, %v; want 3, true", hi, ok)
	}
	if _, ok := s.TryNext(1); ok {
		t.Fatal("TryNext succeeded on a full ring")
	}
	gate.Store(1)
	hi, ok = s.TryNext(2)
	if !ok || hi != 5 {
		t.Fatalf("TryNext(2) after freeing = %d, %v; want 5, true", hi, ok)
	}
}

func TestMultiProducerHighestPublishedGaps(t *testing.T) {
	s := NewMultiProducerSequencer(8, Yielding{})
	s.Publish(0, 0)
	s.Publish(2, 2) // gap at 1
	if got := s.HighestPublished(0, 2); got != 0 {
		t.Fatalf("HighestPublished(0,2) with gap = %d, want 0", got)
	}
	if got := s.HighestPublished(1, 2); got != 0 {
		t.Fatalf("HighestPublished(1,2) with gap = %d, want 0", got)
	}
	s.Publish(1, 1)
	if got := s.HighestPublished(0, 2); got != 2 {
		t.Fatalf("HighestPublished(0,2) contiguous = %d, want 2", got)
	}
	// Old rounds must not read as published for later laps.
	if got := s.HighestPublished(8, 10); got != 7 {
		t.Fatalf("HighestPublished(8,10) next lap = %d, want 7", got)
	}
}

func TestBarrierAlert(t *testing.T) {
	s := NewSingleProducerSequencer(8, Yielding{})
	b := newSequenceBarrier(s, Yielding{}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := b.WaitFor(0)
		done <- err
	}()
	b.Alert()
	if err := <-done; err != ErrAlerted {
		t.Fatalf("WaitFor after Alert = %v, want ErrAlerted", err)
	}
}
