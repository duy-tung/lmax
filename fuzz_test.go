package lmax

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// FuzzDisruptorRoundTrip drives a two-stage pipeline with fuzzed capacity,
// event count, batch size, and producer mode, asserting no event is lost,
// duplicated, reordered, or seen before its upstream stage. The seed corpus
// runs on every plain `go test`; `go test -fuzz=FuzzDisruptorRoundTrip`
// explores further.
func FuzzDisruptorRoundTrip(f *testing.F) {
	f.Add(uint8(0), uint16(1), uint8(1), false)
	f.Add(uint8(1), uint16(3000), uint8(1), false) // capacity 2, heavy wrap
	f.Add(uint8(6), uint16(5000), uint8(7), false) // batched publish
	f.Add(uint8(4), uint16(4000), uint8(1), true)  // multi-producer
	f.Add(uint8(9), uint16(60000), uint8(64), false)
	f.Fuzz(func(t *testing.T, capExp uint8, n uint16, batch uint8, multi bool) {
		capacity := int64(1) << (capExp % 11) // 1..1024
		total := int64(n)%50_000 + 1
		batchSize := int64(batch)%32 + 1
		if batchSize > capacity {
			batchSize = capacity
		}

		opts := []Option{WithCapacity(capacity)}
		if multi {
			opts = append(opts, WithMultiProducer())
		}
		d, err := New[testEvent](opts...)
		if err != nil {
			t.Fatal(err)
		}
		stage1 := d.HandleWith(EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
			e.stage1 = e.v * 3
		}))
		var count, sum int64
		violations := 0
		ordered := true
		last := int64(-1)
		d.After(stage1).HandleWith(EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
			if e.stage1 != e.v*3 {
				violations++
			}
			if seq != last+1 {
				ordered = false
			}
			last = seq
			count++
			sum += e.v
		}))
		if err := d.Start(); err != nil {
			t.Fatal(err)
		}

		if multi {
			// Two producers split the total; per-producer values are still
			// globally summable.
			var wg sync.WaitGroup
			half := total / 2
			for p := int64(0); p < 2; p++ {
				lo, hi := p*half, (p+1)*half
				if p == 1 {
					hi = total
				}
				wg.Add(1)
				go func(lo, hi int64) {
					defer wg.Done()
					for i := lo; i < hi; i++ {
						seq := d.Next()
						d.Get(seq).v = i
						d.Publish(seq)
					}
				}(lo, hi)
			}
			wg.Wait()
		} else {
			for i := int64(0); i < total; i += batchSize {
				b := batchSize
				if total-i < b {
					b = total - i
				}
				base := i
				d.PublishBatch(b, func(j int64, e *testEvent) { e.v = base + j })
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := d.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if count != total {
			t.Errorf("count = %d, want %d (capacity=%d batch=%d multi=%v)", count, total, capacity, batchSize, multi)
		}
		if want := total * (total - 1) / 2; sum != want {
			t.Errorf("sum = %d, want %d", sum, want)
		}
		if violations != 0 {
			t.Errorf("dependency violations: %d", violations)
		}
		if !ordered {
			t.Error("out-of-order delivery")
		}
	})
}

// TestNoGoroutineLeakAfterShutdown asserts, without external dependencies,
// that repeated start/shutdown cycles do not accumulate goroutines.
func TestNoGoroutineLeakAfterShutdown(t *testing.T) {
	before := numGoroutinesSettled()
	for i := 0; i < 50; i++ {
		d, err := New[testEvent](WithCapacity(64))
		if err != nil {
			t.Fatal(err)
		}
		g := d.HandleWith(newCountingHandler(), newCountingHandler())
		d.After(g).HandleWith(newCountingHandler())
		if err := d.Start(); err != nil {
			t.Fatal(err)
		}
		publishSequential(d, 100)
		shutdown(t, d)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		after := numGoroutinesSettled()
		if after <= before+2 { // tolerance for runtime helpers
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines grew from %d to %d after 50 start/shutdown cycles", before, after)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// numGoroutinesSettled samples runtime.NumGoroutine after giving the
// scheduler a moment to retire exiting goroutines.
func numGoroutinesSettled() int {
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	return runtime.NumGoroutine()
}
