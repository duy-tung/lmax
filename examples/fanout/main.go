// Fan-out: one producer, two independent consumers, each seeing every event.
package main

import (
	"context"
	"fmt"
	"log"

	lmax "github.com/duy-tung/lmax"
)

type tick struct {
	Symbol string
	Price  int64
}

func main() {
	d, err := lmax.New[tick](lmax.WithCapacity(1 << 10))
	if err != nil {
		log.Fatal(err)
	}

	var journaled, alerted int
	d.HandleWith(
		lmax.EventHandlerFunc[tick](func(e *tick, seq int64, endOfBatch bool) {
			journaled++ // e.g. append to a write-ahead log, flush on endOfBatch
		}),
		lmax.EventHandlerFunc[tick](func(e *tick, seq int64, endOfBatch bool) {
			if e.Price > 150 {
				alerted++
			}
		}),
	)
	if err := d.Start(); err != nil {
		log.Fatal(err)
	}

	for i := int64(0); i < 1000; i++ {
		seq := d.Next()
		e := d.Get(seq)
		e.Symbol, e.Price = "ACME", 100+i%100
		d.Publish(seq)
	}

	if err := d.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("journaled=%d alerted=%d\n", journaled, alerted)
}
