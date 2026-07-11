// Pipeline: stage 1 enriches each event in place; stage 2 runs only after
// stage 1 finished that event (the After dependency), so it can trust the
// enrichment — no extra queue between the stages.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	lmax "github.com/duy-tung/lmax"
)

type order struct {
	Raw    string
	Symbol string // filled by the parse stage
	Filled bool   // set by the execution stage
}

func main() {
	d, err := lmax.New[order](lmax.WithCapacity(1 << 10))
	if err != nil {
		log.Fatal(err)
	}

	parse := d.HandleWith(lmax.EventHandlerFunc[order](func(e *order, seq int64, eob bool) {
		e.Symbol, _, _ = strings.Cut(e.Raw, ":")
	}))

	var executed int
	d.After(parse).HandleWith(lmax.EventHandlerFunc[order](func(e *order, seq int64, eob bool) {
		if e.Symbol != "" { // safe: parse already ran for this sequence
			e.Filled = true
			executed++
		}
	}))

	if err := d.Start(); err != nil {
		log.Fatal(err)
	}

	// Publish a batch of raw orders in one claim/publish.
	d.PublishBatch(64, func(i int64, e *order) {
		*e = order{Raw: fmt.Sprintf("ACME:%d", i)}
	})

	if err := d.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("executed %d orders\n", executed)
}
