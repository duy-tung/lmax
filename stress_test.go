package lmax

import (
	"context"
	"testing"
	"time"
)

// These stress tests exist primarily for the release-store fast path
// (store_release_amd64.s): the -race build intentionally swaps that path for
// sync/atomic, so the non-race run of this file is what actually exercises
// the MOV-based publication protocol at volume. Skipped under -short.

func TestStressSPSCReleaseStores(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	const n = 10_000_000
	d, err := New[testEvent](WithCapacity(1 << 10))
	if err != nil {
		t.Fatal(err)
	}
	h := newCountingHandler()
	d.HandleWith(h)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	checkHandler(t, "consumer", h, n)
}

func TestStressMPSCReleaseStores(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	const producers = 3
	const perProducer = 2_000_000
	d, err := New[testEvent](WithCapacity(1<<10), WithMultiProducer())
	if err != nil {
		t.Fatal(err)
	}
	var count, sum int64
	lastN := make([]int64, producers)
	for i := range lastN {
		lastN[i] = -1
	}
	ordered := true
	d.HandleWith(EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		count++
		sum += e.n
		if e.n != lastN[e.producer]+1 {
			ordered = false
		}
		lastN[e.producer] = e.n
	}))
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer func() { done <- struct{}{} }()
			for i := int64(0); i < perProducer; i++ {
				seq := d.Next()
				e := d.Get(seq)
				e.producer, e.n = p, i
				d.Publish(seq)
			}
		}(p)
	}
	for p := 0; p < producers; p++ {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if want := int64(producers * perProducer); count != want {
		t.Errorf("count = %d, want %d", count, want)
	}
	if want := int64(producers) * perProducer * (perProducer - 1) / 2; sum != want {
		t.Errorf("sum = %d, want %d", sum, want)
	}
	if !ordered {
		t.Error("per-producer ordering violated")
	}
}
