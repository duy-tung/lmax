// Package bench compares the disruptor against buffered channels for the
// same topologies. Run with: go test -bench=. -benchmem ./bench
package bench

import (
	"context"
	"sync"
	"testing"

	lmax "github.com/duy-tung/lmax"
)

type event struct{ v int64 }

const ringSize = 1 << 14

func BenchmarkDisruptorSPSC(b *testing.B) {
	d, err := lmax.New[event](lmax.WithCapacity(ringSize))
	if err != nil {
		b.Fatal(err)
	}
	var sum int64
	d.HandleWith(lmax.EventHandlerFunc[event](func(e *event, seq int64, eob bool) {
		sum += e.v
	}))
	if err := d.Start(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := d.Next()
		d.Get(seq).v = int64(i)
		d.Publish(seq)
	}
	if err := d.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	_ = sum
}

func BenchmarkChannelSPSC(b *testing.B) {
	ch := make(chan event, ringSize)
	var sum int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range ch {
			sum += e.v
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- event{v: int64(i)}
	}
	close(ch)
	wg.Wait()
	b.StopTimer()
	_ = sum
}

func BenchmarkDisruptorMPSC(b *testing.B) {
	d, err := lmax.New[event](lmax.WithCapacity(ringSize), lmax.WithMultiProducer())
	if err != nil {
		b.Fatal(err)
	}
	var sum int64
	d.HandleWith(lmax.EventHandlerFunc[event](func(e *event, seq int64, eob bool) {
		sum += e.v
	}))
	if err := d.Start(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			seq := d.Next()
			d.Get(seq).v = 1
			d.Publish(seq)
		}
	})
	if err := d.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	_ = sum
}

func BenchmarkChannelMPSC(b *testing.B) {
	ch := make(chan event, ringSize)
	var sum int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range ch {
			sum += e.v
		}
	}()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch <- event{v: 1}
		}
	})
	close(ch)
	wg.Wait()
	b.StopTimer()
	_ = sum
}
