# lmax

An [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)-style
inter-goroutine messaging library for Go: a pre-allocated ring buffer
coordinated by cache-line-padded sequence counters and atomic
publish/consume protocols instead of locks.

Design and rationale: [docs/PLAN.md](docs/PLAN.md).

## Features

- Generic events (`New[T]`), stored by value in the ring — zero allocations
  on the publish/consume hot path (asserted in tests).
- Single-producer fast path (no atomics on claim) and multi-producer mode
  (CAS claim + per-slot publication tracking).
- Consumer topologies: fan-out (every consumer sees every event), pipelines,
  and dependency DAGs (`After`) — no extra queues between stages.
- Pluggable wait strategies: `BusySpin`, `Yielding` (default), `Sleeping`,
  `Blocking`.
- Batch consumption: consumers catch up with one atomic store per batch.

## Usage

```go
type Order struct {
	ID     int64
	Amount int64
}

d, err := lmax.New[Order](
	lmax.WithCapacity(1<<16),          // power of two
	lmax.WithWaitStrategy(lmax.Yielding{}),
)
if err != nil { ... }

// Consumer graph: journal and replicate in parallel, then business logic.
journal := d.HandleWith(journalHandler, replicateHandler)
d.After(journal).HandleWith(lmax.EventHandlerFunc[Order](
	func(e *Order, seq int64, endOfBatch bool) {
		process(e)
	}))

if err := d.Start(); err != nil { ... }

// Publish: claim a slot, fill it in place, release it.
seq := d.Next()
e := d.Get(seq)
e.ID, e.Amount = 1, 42
d.Publish(seq)

// Drain and stop.
if err := d.Shutdown(ctx); err != nil { ... }
```

For concurrent publishers add `lmax.WithMultiProducer()`.

**Contract:** the `*T` returned by `Get` (and passed to handlers) points into
the ring; do not retain it past `Publish` (producer) or past your handler
returning (consumer).

## Testing

```sh
go test -race ./...           # unit + integration (SPSC, fan-out, pipeline,
                              # diamond, multi-producer, shutdown, zero-alloc)
go test -bench=. -benchmem ./bench
```

Indicative numbers from a shared 4-vCPU cloud VM (Xeon @ 2.80GHz, Go 1.24,
median of 3 — run your own on real hardware; isolated cores change the
picture substantially). After the profile-driven optimization pass
([docs/OPTIMIZATION.md](docs/OPTIMIZATION.md)):

| Benchmark | Disruptor ns/op | Channel ns/op | Speedup |
|---|---:|---:|---:|
| SPSC, 8B events        |  36.4 | 143.0 | 3.9× |
| SPSC, 64B events       |  16.3 | 189.5 | 11.6× |
| SPSC, 256B events      |  17.1 | 334.8 | 19.6× |
| SPSC, batch=8 (manual) |   4.8 | 143.0 | 29.9× |
| MPSC, 1 producer       |  21.6 |  68.7 | 3.2× |
| MPSC, 2 producers      | 134.6 |  79.6 | 0.6× |
| MPSC, 3 producers      | 375.6 |  83.7 | 0.2× |

All zero allocs/op. Honest caveat: heavily contended multi-producer claims
on shared vCPUs remain slower than channels at 2-3 producers hammering
single events — batch publication (`PublishBatch`/`NextN`) is the designed
answer there. One-way latency (Yielding strategy, idle ring): p50 ≈ 0.6µs,
p99 ≈ 55µs on this VM.
