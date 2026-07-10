package lmax

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type testEvent struct {
	v        int64
	stage1   int64
	a, b     bool
	producer int
	n        int64
}

// countingHandler records everything a consumer sees. Its fields are written
// only by the processor goroutine; tests read them after Shutdown, which
// happens-after the goroutine exits.
type countingHandler struct {
	count   int64
	sum     int64
	lastSeq int64
	ordered bool
}

func newCountingHandler() *countingHandler {
	return &countingHandler{lastSeq: -1, ordered: true}
}

func (h *countingHandler) OnEvent(e *testEvent, seq int64, endOfBatch bool) {
	if seq != h.lastSeq+1 {
		h.ordered = false
	}
	h.lastSeq = seq
	h.count++
	h.sum += e.v
}

func shutdown(t *testing.T, d *Disruptor[testEvent]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func checkHandler(t *testing.T, name string, h *countingHandler, n int64) {
	t.Helper()
	if h.count != n {
		t.Errorf("%s: count = %d, want %d", name, h.count, n)
	}
	if want := n * (n - 1) / 2; h.sum != want {
		t.Errorf("%s: sum = %d, want %d", name, h.sum, want)
	}
	if !h.ordered {
		t.Errorf("%s: saw out-of-order or gapped sequences", name)
	}
}

func publishSequential(d *Disruptor[testEvent], n int64) {
	for i := int64(0); i < n; i++ {
		seq := d.Next()
		d.Get(seq).v = i
		d.Publish(seq)
	}
}

func TestIntegrationSPSC(t *testing.T) {
	const n = 1 << 20
	d, err := New[testEvent](WithCapacity(1024))
	if err != nil {
		t.Fatal(err)
	}
	h := newCountingHandler()
	d.HandleWith(h)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	checkHandler(t, "consumer", h, n)
}

func TestIntegrationTinyRingBackpressure(t *testing.T) {
	// Capacity 2 forces constant wrapping and producer gating on every claim.
	const n = 50_000
	d, err := New[testEvent](WithCapacity(2))
	if err != nil {
		t.Fatal(err)
	}
	h := newCountingHandler()
	d.HandleWith(h)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	checkHandler(t, "consumer", h, n)
}

func TestIntegrationBatchPublish(t *testing.T) {
	const n = 1 << 18
	const batch = 16
	d, err := New[testEvent](WithCapacity(1024))
	if err != nil {
		t.Fatal(err)
	}
	h := newCountingHandler()
	d.HandleWith(h)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < n; i += batch {
		hi := d.NextN(batch)
		lo := hi - batch + 1
		for seq := lo; seq <= hi; seq++ {
			d.Get(seq).v = i + (seq - lo)
		}
		d.PublishRange(lo, hi)
	}
	shutdown(t, d)
	checkHandler(t, "consumer", h, n)
}

func TestIntegrationFanOut(t *testing.T) {
	// One producer, three independent consumers: each must see every event.
	const n = 1 << 19
	d, err := New[testEvent](WithCapacity(1024))
	if err != nil {
		t.Fatal(err)
	}
	hs := []*countingHandler{newCountingHandler(), newCountingHandler(), newCountingHandler()}
	d.HandleWith(hs[0], hs[1], hs[2])
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	for i, h := range hs {
		checkHandler(t, fmt.Sprintf("consumer %d", i), h, n)
	}
}

func TestIntegrationPipeline(t *testing.T) {
	// Stage 1 transforms the event in place; stage 2 must observe the
	// transformation on every event — proving the dependency gating.
	const n = 1 << 19
	d, err := New[testEvent](WithCapacity(1024))
	if err != nil {
		t.Fatal(err)
	}
	stage1 := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		e.stage1 = e.v * 2
	})
	violations := 0
	h := newCountingHandler()
	stage2 := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		if e.stage1 != e.v*2 {
			violations++
		}
		h.OnEvent(e, seq, eob)
	})
	g1 := d.HandleWith(stage1)
	d.After(g1).HandleWith(stage2)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	checkHandler(t, "stage2", h, n)
	if violations != 0 {
		t.Errorf("stage2 ran before stage1 on %d events", violations)
	}
}

func TestIntegrationDiamond(t *testing.T) {
	// A and B in parallel, C after both (the LMAX journal ‖ replicate →
	// business-logic shape). C must see both stages' marks on every event.
	const n = 1 << 19
	d, err := New[testEvent](WithCapacity(1024))
	if err != nil {
		t.Fatal(err)
	}
	stageA := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) { e.a = true })
	stageB := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) { e.b = true })
	violations := 0
	h := newCountingHandler()
	stageC := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		if !e.a || !e.b {
			violations++
		}
		h.OnEvent(e, seq, eob)
	})
	ga := d.HandleWith(stageA)
	gb := d.HandleWith(stageB)
	d.After(ga, gb).HandleWith(stageC)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	checkHandler(t, "stageC", h, n)
	if violations != 0 {
		t.Errorf("C ran before A and B on %d events", violations)
	}
}

func TestIntegrationThenChain(t *testing.T) {
	const n = 1 << 16
	d, err := New[testEvent](WithCapacity(256))
	if err != nil {
		t.Fatal(err)
	}
	stage1 := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) { e.stage1 = e.v + 1 })
	h := newCountingHandler()
	violations := 0
	stage2 := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		if e.stage1 != e.v+1 {
			violations++
		}
		h.OnEvent(e, seq, eob)
	})
	d.HandleWith(stage1).Then(stage2)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	checkHandler(t, "then-stage", h, n)
	if violations != 0 {
		t.Errorf("Then stage ordering violated on %d events", violations)
	}
}

func TestIntegrationMultiProducer(t *testing.T) {
	// 4 producers, 1 consumer. The consumer must see every event exactly
	// once, and each producer's events in the order that producer sent them.
	const producers = 4
	const perProducer = 200_000
	d, err := New[testEvent](WithCapacity(1024), WithMultiProducer())
	if err != nil {
		t.Fatal(err)
	}
	var count, sum int64
	lastN := make([]int64, producers)
	for i := range lastN {
		lastN[i] = -1
	}
	perProducerOrdered := true
	d.HandleWith(EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		count++
		sum += e.n
		if e.n != lastN[e.producer]+1 {
			perProducerOrdered = false
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
	shutdown(t, d)
	if want := int64(producers * perProducer); count != want {
		t.Errorf("count = %d, want %d", count, want)
	}
	if want := int64(producers) * perProducer * (perProducer - 1) / 2; sum != want {
		t.Errorf("sum = %d, want %d", sum, want)
	}
	if !perProducerOrdered {
		t.Error("per-producer ordering violated")
	}
}

func TestIntegrationMultiProducerFanOutTinyRing(t *testing.T) {
	// Worst case for the availableBuffer protocol: heavy contention on an
	// 8-slot ring with two downstream consumers.
	const producers = 3
	const perProducer = 30_000
	d, err := New[testEvent](WithCapacity(8), WithMultiProducer())
	if err != nil {
		t.Fatal(err)
	}
	h1, h2 := newCountingHandler(), newCountingHandler()
	d.HandleWith(h1, h2)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	for p := 0; p < producers; p++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := int64(0); i < perProducer; i++ {
				seq := d.Next()
				d.Get(seq).v = 1
				d.Publish(seq)
			}
		}()
	}
	for p := 0; p < producers; p++ {
		<-done
	}
	shutdown(t, d)
	const want = producers * perProducer
	for name, h := range map[string]*countingHandler{"h1": h1, "h2": h2} {
		if h.count != want {
			t.Errorf("%s: count = %d, want %d", name, h.count, want)
		}
		if h.sum != want {
			t.Errorf("%s: sum = %d, want %d", name, h.sum, want)
		}
		if !h.ordered {
			t.Errorf("%s: ordering violated", name)
		}
	}
}

func TestIntegrationWaitStrategies(t *testing.T) {
	const n = 100_000
	for name, ws := range map[string]func() WaitStrategy{
		"BusySpin": func() WaitStrategy { return BusySpin{} },
		"Yielding": func() WaitStrategy { return Yielding{} },
		"Sleeping": func() WaitStrategy { return Sleeping{} },
		"Blocking": func() WaitStrategy { return &Blocking{} },
	} {
		t.Run(name, func(t *testing.T) {
			d, err := New[testEvent](WithCapacity(512), WithWaitStrategy(ws()))
			if err != nil {
				t.Fatal(err)
			}
			h := newCountingHandler()
			d.HandleWith(h)
			if err := d.Start(); err != nil {
				t.Fatal(err)
			}
			publishSequential(d, n)
			shutdown(t, d)
			checkHandler(t, name, h, n)
		})
	}
}

func TestIntegrationShutdownDrains(t *testing.T) {
	// Everything published before Shutdown must be processed, even with a
	// slow consumer that is well behind at shutdown time.
	const n = 10_000
	d, err := New[testEvent](WithCapacity(1 << 14))
	if err != nil {
		t.Fatal(err)
	}
	h := newCountingHandler()
	slow := EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		if seq%1000 == 0 {
			time.Sleep(time.Millisecond)
		}
		h.OnEvent(e, seq, eob)
	})
	d.HandleWith(slow)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	publishSequential(d, n)
	shutdown(t, d)
	checkHandler(t, "slow consumer", h, n)
}

func TestIntegrationShutdownTimeout(t *testing.T) {
	// A consumer that never finishes must make Shutdown return ctx error,
	// not hang.
	d, err := New[testEvent](WithCapacity(8))
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	d.HandleWith(EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {
		<-block
	}))
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	seq := d.Next()
	d.Publish(seq)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := d.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
	}
	close(block)
}

func TestLifecycleErrors(t *testing.T) {
	d, err := New[testEvent]()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(); err == nil {
		t.Error("Start with no handlers should fail")
	}

	if _, err := New[testEvent](WithCapacity(100)); err == nil {
		t.Error("New with non-power-of-2 capacity should fail")
	}

	d2, _ := New[testEvent]()
	d2.HandleWith(newCountingHandler())
	if err := d2.Start(); err != nil {
		t.Fatal(err)
	}
	if err := d2.Start(); err == nil {
		t.Error("second Start should fail")
	}
	shutdown(t, d2)
	if err := d2.Shutdown(context.Background()); err == nil {
		t.Error("second Shutdown should fail")
	}
}

func TestZeroAllocationHotPath(t *testing.T) {
	d, err := New[testEvent](WithCapacity(1 << 12))
	if err != nil {
		t.Fatal(err)
	}
	d.HandleWith(EventHandlerFunc[testEvent](func(e *testEvent, seq int64, eob bool) {}))
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(10_000, func() {
		seq := d.Next()
		d.Get(seq).v = 1
		d.Publish(seq)
	})
	shutdown(t, d)
	if allocs != 0 {
		t.Errorf("hot path allocated %.1f times per op, want 0", allocs)
	}
}
