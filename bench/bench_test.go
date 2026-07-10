// Package bench compares the disruptor against buffered channels for the
// same topologies. Run with: go test -bench=. -benchmem ./bench
//
// MPSC benchmarks use explicit producer goroutines (not RunParallel) so that
// producers + the consumer never exceed GOMAXPROCS — oversubscription
// otherwise pollutes the numbers with scheduler noise.
package bench

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	lmax "github.com/duy-tung/lmax"
)

const ringSize = 1 << 14

// Payload sizes: the by-value ring should widen its lead over channels as
// events grow, since channel send/recv copies twice (in and out).
type event8 struct{ v int64 }
type event64 struct {
	v   int64
	pad [56]byte
}
type event256 struct {
	v   int64
	pad [248]byte
}

func (e *event8) value() int64       { return e.v }
func (e *event8) setValue(v int64)   { e.v = v }
func (e *event64) value() int64      { return e.v }
func (e *event64) setValue(v int64)  { e.v = v }
func (e *event256) value() int64     { return e.v }
func (e *event256) setValue(v int64) { e.v = v }

type payloadPtr[T any] interface {
	*T
	value() int64
	setValue(int64)
}

func benchDisruptorSPSC[T any, PT payloadPtr[T]](b *testing.B) {
	d, err := lmax.New[T](lmax.WithCapacity(ringSize))
	if err != nil {
		b.Fatal(err)
	}
	var sum int64
	d.HandleWith(lmax.EventHandlerFunc[T](func(e *T, seq int64, eob bool) {
		sum += PT(e).value()
	}))
	if err := d.Start(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := d.Next()
		PT(d.Get(seq)).setValue(int64(i))
		d.Publish(seq)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	_ = sum
}

func benchChannelSPSC[T any, PT payloadPtr[T]](b *testing.B) {
	ch := make(chan T, ringSize)
	var sum int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range ch {
			sum += PT(&e).value()
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var e T
		PT(&e).setValue(int64(i))
		ch <- e
	}
	close(ch)
	wg.Wait()
	b.StopTimer()
	_ = sum
}

func BenchmarkDisruptorSPSC(b *testing.B) {
	b.Run("payload=8", benchDisruptorSPSC[event8])
	b.Run("payload=64", benchDisruptorSPSC[event64])
	b.Run("payload=256", benchDisruptorSPSC[event256])
}

func BenchmarkChannelSPSC(b *testing.B) {
	b.Run("payload=8", benchChannelSPSC[event8])
	b.Run("payload=64", benchChannelSPSC[event64])
	b.Run("payload=256", benchChannelSPSC[event256])
}

// Batch publication amortizes the per-event publish cost ("smart batching").
// "manual" uses NextN/Get/PublishRange inline (fastest — no indirect call);
// "api" uses the ergonomic PublishBatch, which pays one closure call per
// event.
func BenchmarkDisruptorSPSCBatch(b *testing.B) {
	run := func(name string, publish func(d *lmax.Disruptor[event8], i, n int64)) {
		for _, batch := range []int64{8, 64} {
			b.Run(name+"/batch="+itoa(batch), func(b *testing.B) {
				d, err := lmax.New[event8](lmax.WithCapacity(ringSize))
				if err != nil {
					b.Fatal(err)
				}
				var sum int64
				d.HandleWith(lmax.EventHandlerFunc[event8](func(e *event8, seq int64, eob bool) {
					sum += e.v
				}))
				if err := d.Start(); err != nil {
					b.Fatal(err)
				}
				b.ResetTimer()
				for i := int64(0); i < int64(b.N); i += batch {
					n := batch
					if int64(b.N)-i < n {
						n = int64(b.N) - i
					}
					publish(d, i, n)
				}
				if err := d.Shutdown(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_ = sum
			})
		}
	}
	run("manual", func(d *lmax.Disruptor[event8], i, n int64) {
		hi := d.NextN(n)
		lo := hi - n + 1
		for seq := lo; seq <= hi; seq++ {
			d.Get(seq).v = i + (seq - lo)
		}
		d.PublishRange(lo, hi)
	})
	run("api", func(d *lmax.Disruptor[event8], i, n int64) {
		d.PublishBatch(n, func(j int64, e *event8) { e.v = i + j })
	})
}

func benchDisruptorMPSC(b *testing.B, producers int) {
	d, err := lmax.New[event8](lmax.WithCapacity(ringSize), lmax.WithMultiProducer())
	if err != nil {
		b.Fatal(err)
	}
	var sum int64
	d.HandleWith(lmax.EventHandlerFunc[event8](func(e *event8, seq int64, eob bool) {
		sum += e.v
	}))
	if err := d.Start(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	per := b.N / producers
	for p := 0; p < producers; p++ {
		n := per
		if p == 0 {
			n += b.N % producers
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				seq := d.Next()
				d.Get(seq).v = 1
				d.Publish(seq)
			}
		}(n)
	}
	wg.Wait()
	if err := d.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	_ = sum
}

func benchChannelMPSC(b *testing.B, producers int) {
	ch := make(chan event8, ringSize)
	var sum int64
	var cwg sync.WaitGroup
	cwg.Add(1)
	go func() {
		defer cwg.Done()
		for e := range ch {
			sum += e.v
		}
	}()
	b.ResetTimer()
	var wg sync.WaitGroup
	per := b.N / producers
	for p := 0; p < producers; p++ {
		n := per
		if p == 0 {
			n += b.N % producers
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				ch <- event8{v: 1}
			}
		}(n)
	}
	wg.Wait()
	close(ch)
	cwg.Wait()
	b.StopTimer()
	_ = sum
}

func BenchmarkDisruptorMPSC(b *testing.B) {
	for _, p := range []int{1, 2, 3} {
		b.Run("producers="+itoa(int64(p)), func(b *testing.B) { benchDisruptorMPSC(b, p) })
	}
}

func BenchmarkChannelMPSC(b *testing.B) {
	for _, p := range []int{1, 2, 3} {
		b.Run("producers="+itoa(int64(p)), func(b *testing.B) { benchChannelMPSC(b, p) })
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestLatencySPSC reports one-way publish→consume latency percentiles on an
// otherwise idle ring, per wait strategy. Informational (logged with -v);
// the O7 spin-tuning gate compares these numbers before/after.
func TestLatencySPSC(t *testing.T) {
	const n = 50_000
	for name, ws := range map[string]func() lmax.WaitStrategy{
		"Yielding": func() lmax.WaitStrategy { return lmax.Yielding{} },
		"Blocking": func() lmax.WaitStrategy { return &lmax.Blocking{} },
	} {
		t.Run(name, func(t *testing.T) {
			type tsEvent struct{ sent int64 }
			d, err := lmax.New[tsEvent](lmax.WithCapacity(1024), lmax.WithWaitStrategy(ws()))
			if err != nil {
				t.Fatal(err)
			}
			lat := make([]int64, 0, n)
			d.HandleWith(lmax.EventHandlerFunc[tsEvent](func(e *tsEvent, seq int64, eob bool) {
				lat = append(lat, time.Now().UnixNano()-e.sent)
			}))
			if err := d.Start(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < n; i++ {
				seq := d.Next()
				d.Get(seq).sent = time.Now().UnixNano()
				d.Publish(seq)
				// Idle gap so we measure wake-up latency, not queueing.
				for j := 0; j < 200; j++ {
					_ = j
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := d.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if len(lat) != n {
				t.Fatalf("received %d latencies, want %d", len(lat), n)
			}
			sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
			t.Logf("%s one-way latency: p50=%dns p99=%dns p999=%dns max=%dns",
				name, lat[n/2], lat[n*99/100], lat[n*999/1000], lat[n-1])
		})
	}
}
