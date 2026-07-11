# LMAX Disruptor in Go — Implementation Plan & Specification

This document is the plan and technical specification for implementing an
LMAX-Disruptor-style inter-goroutine messaging library in Go. It is derived
from the "High-Performance Concurrency with LMAX Disruptor" tech-sharing deck
(Truong Hoang, 2021) and the original LMAX Disruptor design (Thompson,
Farley, Barker, Gee, Stewart — LMAX technical paper), adapted to Go's memory
model and runtime.

---

## 1. Background and Motivation

The deck's key points, which drive every design decision below:

1. **Concurrency is hard.** Concurrency is not just parallel execution — it is
   contention for shared resources. Correct concurrent code needs two things:
   *mutual exclusion* and *visibility of change*.
2. **Mechanical sympathy matters.** Understanding the hardware (CPU memory
   hierarchy, cache lines, cache coherence, false sharing) is what makes
   low-latency code possible.
3. **Locks are bad for hot paths.**
   - Coarse locks are safe but slow (the deck's benchmark: a single-threaded
     counter took ~175 ms; with a lock ~6.7 s; two threads with a lock ~22.7 s
     — locks made contended code *hundreds of times slower*).
   - Fine-grained locks reintroduce race conditions, inconsistent state, and
     deadlock (mutual exclusion + hold-and-wait + no preemption + circular
     wait).
   - The most costly operation in a concurrent system is a **contended write**.
4. **CAS beats locks.** Compare-And-Swap is an atomic machine instruction that
   avoids kernel arbitration and context switches.
5. **Queues have inherent problems.** A traditional bounded queue has
   head, tail, and size all contended by producers and consumers; the head and
   tail tend to share cache lines (false sharing), unbounded queues cause GC
   pressure, and lock-based queues serialize everything.

The LMAX Disruptor answers these with a **pre-allocated ring buffer**,
**sequence counters with single-writer ownership**, **memory barriers instead
of locks**, and **cache-line padding** — achieving millions of events/second
with mechanical sympathy. Our goal is the same architecture in idiomatic Go.

## 2. Goals

- G1: A generic (`Event any`) Disruptor library for passing events between
  goroutines with throughput significantly higher than buffered channels and
  `container/ring`+mutex approaches for the targeted topologies.
- G2: Zero allocations on the hot path (publish/consume) — events live in a
  pre-allocated ring and are *mutated in place*, never reallocated.
- G3: Support the canonical Disruptor topologies:
  - single producer → single consumer,
  - single producer → many independent consumers (broadcast, each sees every
    event),
  - pipeline / dependency graph (consumer B only sees events after consumer A),
  - multi-producer → consumers.
- G4: Pluggable wait strategies trading CPU for latency (busy-spin, yielding,
  sleeping, blocking).
- G5: Correct under `go test -race`, on amd64 and arm64.
- G6: Benchmarked, with reproducible comparisons against Go channels.

### Non-Goals

- Cross-process or network transport (in-process only).
- Replacing channels for general use — this is for hot paths with fixed
  topologies known at startup.
- Dynamic consumer registration after the disruptor has started.
- Java feature parity (no `WorkPool` work-stealing in v1, no batch event
  processor rewind, no async event translators).

## 3. Architecture Overview

```
                   ┌────────────────────────────────────────────┐
                   │                RingBuffer[T]               │
 Producer(s) ──▶   │  [e0][e1][e2][e3][e4][e5][e6][e7] (2^n)    │ ──▶ Consumer(s)
                   └────────────────────────────────────────────┘
      │                                                              │
      ▼                                                              ▼
  Sequencer                                                    Sequence(s)
  (cursor: last published seq)                        (last consumed seq per consumer)
      ▲                                                              │
      └──────────── gating: producer must not lap the ──────────────┘
                     slowest consumer's sequence
```

Core ideas, mapped from the deck:

| Problem (deck)                     | Disruptor answer                                            |
|------------------------------------|-------------------------------------------------------------|
| Contended writes are the enemy     | Every sequence counter has exactly **one writer**            |
| Locks → deadlock/perf collapse     | No locks anywhere on the hot path; CAS only where 2+ writers are unavoidable (multi-producer claim) |
| Visibility of change               | Release/acquire semantics via `sync/atomic` Load/Store       |
| False sharing kills throughput     | Pad every hot counter to its own cache line (128 B)          |
| Queue head/tail/size contention    | Ring buffer indexed by monotonically increasing `int64` sequences; index = `seq & (size-1)` |
| GC pressure from queue nodes       | Entries pre-allocated once; producers overwrite slots in place |

## 4. Package Layout

```
github.com/duy-tung/lmax
├── go.mod                       (module github.com/duy-tung/lmax, go 1.24)
├── docs/
│   └── PLAN.md                  (this document)
├── sequence.go                  Sequence: padded atomic int64
├── sequencer.go                 Sequencer interface
├── ringbuffer.go                RingBuffer[T]
├── sequencer_single.go          SingleProducerSequencer
├── sequencer_multi.go           MultiProducerSequencer
├── barrier.go                   SequenceBarrier (tracks cursor + dependent sequences)
├── wait_strategy.go             WaitStrategy interface + BusySpin/Yielding/Sleeping/Blocking
├── processor.go                 EventProcessor: the consumer loop (batching)
├── disruptor.go                 Disruptor[T]: builder/DSL, lifecycle (Start/Shutdown)
├── util.go                      ceilPow2, cache-line consts
├── *_test.go                    unit + race + stress tests
└── bench/
    └── bench_test.go            disruptor vs channel benchmarks
```

Single flat package `lmax` (plus `bench`) — the components are tightly coupled
and a flat package avoids exporting internals.

## 5. Detailed Component Specification

### 5.1 `Sequence` — padded atomic counter

The fundamental primitive. A monotonically increasing `int64` starting at
`-1` ("nothing published/consumed yet"). One goroutine writes it; any number
read it.

```go
const CacheLinePad = 128 // 2 cache lines: defeats adjacent-line prefetcher too

type Sequence struct {
    _   [CacheLinePad]byte
    val atomic.Int64
    _   [CacheLinePad - 8]byte
}

func NewSequence() *Sequence // initializes to -1
func (s *Sequence) Load() int64          // atomic acquire
func (s *Sequence) Store(v int64)        // atomic release
func (s *Sequence) CompareAndSwap(old, new int64) bool
```

Design notes:

- **Padding before and after** guarantees no other hot variable shares either
  adjacent cache line, regardless of where the struct lands in memory
  (deck slides 30–31: cache lines / false sharing).
- Go's `sync/atomic` operations are sequentially consistent — stronger than
  the Java Disruptor's lazySet/acquire-release, so correctness is preserved;
  we accept the (small) cost on the platforms we target. No `unsafe` memory
  ordering tricks in v1.
- `Sequence` values are allocated via `NewSequence()` and shared by pointer so
  padding is never copied away.

### 5.2 `RingBuffer[T]` — pre-allocated storage

```go
type RingBuffer[T any] struct {
    entries []T    // len == capacity, allocated once
    mask    int64  // capacity - 1
}

func NewRingBuffer[T any](capacity int64) *RingBuffer[T] // capacity MUST be a power of 2
func (r *RingBuffer[T]) Get(seq int64) *T { return &r.entries[seq&r.mask] }
```

- Capacity is a **power of two** so slot lookup is `seq & mask` (no division).
  The constructor panics on non-power-of-two.
- `Get` returns a *pointer into the ring*: producers fill the slot in place;
  consumers read it in place. No allocation, no copying of large events.
- Contract: a slot pointer is valid for the producer between `Next()` and
  `Publish(seq)`, and for a consumer only until it advances its sequence past
  `seq`. Holding a `*T` beyond that is a data race by contract (documented,
  and caught by the race detector in tests).
- Note: consecutive small events share cache lines *within the ring*. That is
  intentional and harmless for the single-writer-per-slot-at-a-time protocol
  (it is also how the Java Disruptor works); the *counters*, not the data, are
  the false-sharing hazard.

### 5.3 Sequencers — producer-side claim/publish protocol

The sequencer owns the **cursor** (highest published sequence) and enforces
the **gating rule**: a producer may claim sequence `n` only when
`n - capacity < min(consumer sequences)` — i.e. it never overwrites a slot a
consumer hasn't finished with (this replaces a queue's "is full" check).

```go
type Sequencer interface {
    Next(n int64) int64          // claim n slots, returns highest claimed; blocks (spins) when full
    TryNext(n int64) (int64, bool) // non-blocking variant
    Publish(lo, hi int64)        // make claimed slots visible to consumers
    Cursor() *Sequence
    // consumer coordination
    NewBarrier(deps ...*Sequence) *SequenceBarrier
    AddGating(seqs ...*Sequence)
    HighestPublished(lo, hi int64) int64
}
```

**SingleProducerSequencer** (the fast path; matches LMAX's own usage — their
Business Logic Processor is single-threaded):

- `next` and `cachedGate` are plain (non-atomic) fields — only one goroutine
  touches them. The deck's principle: *only contended writes need mutual
  exclusion; single-writer needs only visibility*.
- `Next(n)`: `next += n`; if `next - capacity >= cachedGate`, reload
  `cachedGate = min(gating sequences)` and spin (with the wait strategy's
  producer backoff) until there is room. Caching the gate means the common
  case reads **zero** shared variables.
- `Publish(lo, hi)`: single `cursor.Store(hi)` — the release write that makes
  the filled slots visible. Consumers' acquire-loads of the cursor observe the
  slot writes (happens-before via the atomic pair).
- `HighestPublished(lo, hi)` returns `hi` — with one producer the cursor alone
  is proof of publication.

**MultiProducerSequencer**:

- Claiming: `next` becomes an atomic counter; `Next(n)` is a CAS loop (or
  `Add` with post-check) — the deck's CAS section: contended, but no kernel
  arbitration.
- Publication cannot rely on the cursor alone (producer B may publish seq 7
  while producer A is still writing seq 6). We adopt the Java Disruptor's
  **availableBuffer**: an `[]atomic.Int32` of size `capacity`, where
  publishing seq `s` stores the *round number* `s >> log2(capacity)` at index
  `s & mask`. `HighestPublished(lo, hi)` scans forward until it finds an
  unpublished slot. This avoids the naive CAS-on-cursor spin that stalls all
  producers behind the slowest one.

### 5.4 `SequenceBarrier` — consumer-side gating

A consumer waits on a barrier composed of the **cursor** and the sequences of
any **upstream consumers** it depends on (this is how pipelines/DAGs are
built — dependency ordering *without* extra queues between stages):

```go
type SequenceBarrier struct {
    cursor *Sequence     // producer progress
    deps   []*Sequence   // upstream consumers (empty = depend on producer only)
    wait   WaitStrategy
    alerted atomic.Bool  // shutdown signal
}

// WaitFor blocks until sequence >= seq is available, returns the highest
// available (>= seq), enabling batch consumption.
func (b *SequenceBarrier) WaitFor(seq int64) (int64, error)
```

`WaitFor` returns `ErrAlerted` when the disruptor is shutting down.

### 5.5 `WaitStrategy` — the latency/CPU dial

```go
type WaitStrategy interface {
    // WaitFor spins/sleeps until dependent() >= seq or alerted.
    WaitFor(seq int64, dependent func() int64, alerted func() bool) (int64, error)
    // SignalAll wakes blocked waiters (no-op for non-blocking strategies).
    SignalAll()
}
```

| Strategy       | Mechanism                                                   | Use case                     |
|----------------|-------------------------------------------------------------|------------------------------|
| `BusySpin`     | tight loop on atomic load                                    | lowest latency, dedicated cores |
| `Yielding`     | spin N times, then `runtime.Gosched()`                       | low latency, shared cores (default) |
| `Sleeping`     | spin → `Gosched` → `time.Sleep(≈100µs)` backoff              | background consumers         |
| `Blocking`     | `sync.Cond`; producers call `SignalAll` after publish        | lowest CPU, latency-tolerant |

Go-specific: a spinning goroutine never yields to the scheduler on its own,
which can starve other goroutines on the same P — so unlike Java, **pure
busy-spin must be opt-in** and `Yielding` (with `Gosched`) is the default.
Docs will recommend `GOMAXPROCS >= producers + consumers + 1` for spinning
strategies.

### 5.6 `EventProcessor` — the consumer loop

One goroutine per consumer, running the canonical batch loop:

```go
type EventHandler[T any] interface {
    // OnEvent processes the event in slot *e. endOfBatch signals the last
    // event of the currently available batch (for flush-style handlers).
    OnEvent(e *T, seq int64, endOfBatch bool)
}

func (p *EventProcessor[T]) run() {
    next := p.seq.Load() + 1
    for {
        avail, err := p.barrier.WaitFor(next)
        if err != nil { return } // alerted → shutdown
        for ; next <= avail; next++ {
            p.handler.OnEvent(p.ring.Get(next), next, next == avail)
        }
        p.seq.Store(avail) // single release write per batch
    }
}
```

**Batching effect** (a core Disruptor throughput win): when a consumer falls
behind, `WaitFor` returns a whole range and the consumer catches up with one
atomic write per *batch*, not per event — the ring drains at memcpy-like
speed instead of ping-ponging cache lines per element.

Handler panics are not recovered in v1 (fail-fast, same as an unrecovered
panic in a goroutine); a `PanicHandler` hook is listed as a stretch goal.

### 5.7 `Disruptor[T]` — DSL, wiring, lifecycle

```go
d, err := lmax.New[MyEvent](
    lmax.WithCapacity(1<<16),
    lmax.WithSingleProducer(),            // or WithMultiProducer()
    lmax.WithWaitStrategy(lmax.Yielding{}),
)
journal := d.HandleWith(journalHandler)      // stage 1 (fan-out: pass several)
d.After(journal).HandleWith(bizHandler)      // stage 2: pipeline dependency
d.Start()                                     // spawns one goroutine per handler
defer d.Shutdown(ctx)                         // drain, alert barriers, join

// producer hot path — claim, write in place, publish:
seq := d.Next()
e := d.Get(seq)
e.Kind, e.Amount = OrderPlaced, 42
d.Publish(seq)
```

Wiring rules (validated in `Start`):

- `HandleWith(h...)` at the root creates processors gated on the cursor.
- `After(group).HandleWith(h...)` gates new processors on the *sequences* of
  the group — building the DAG (e.g. LMAX's journal ‖ replicate → business
  logic; Arcturus-style pipelines).
- The producer gates on the **leaf** consumers only (the min of the last
  stage bounds everything upstream).
- `Shutdown`: stop accepting publishes, wait until all consumer sequences
  reach the cursor (bounded by ctx), alert barriers, `SignalAll`, join
  goroutines.

### 5.8 Error handling & invariants

- Constructor errors: non-power-of-two capacity, zero handlers, `After` on an
  unknown group, `Start` called twice, publish after shutdown (panic — a
  programming error, like sending on a closed channel).
- Invariants asserted in tests: cursor ≥ every consumer seq; claimed but
  unpublished range ≤ capacity; each consumer sees every sequence exactly
  once, in order.

## 6. Go-Specific Design Decisions (vs. the Java original)

| Concern | Java Disruptor | This implementation |
|---|---|---|
| Execution unit | OS threads, often core-pinned | Goroutines; document `runtime.LockOSThread` + `GOMAXPROCS` guidance, no pinning API in v1 |
| Memory ordering | `lazySet`, acquire/release varhandles | `sync/atomic` (seq-cst) — simpler, correct; measured before any `unsafe` optimization |
| False sharing | `@Contended` / field padding | explicit 128-byte pad arrays around every hot counter |
| Generics | `EventFactory<T>` + erased generics | Go generics `[T any]`; ring is `[]T` (values, not pointers) so events are contiguous and GC-invisible |
| GC pressure | pre-allocated entries | same; `[]T` of structs → zero pointers if `T` has none, so GC scan cost ~0 |
| Blocking strategy | `ReentrantLock`+`Condition` | `sync.Cond` |
| Spin-wait | `Thread.onSpinWait()` | bounded spin + `runtime.Gosched()`; document scheduler-starvation caveat |

## 7. Milestones

Phased so every phase lands green (`go vet`, `go test -race`, benchmarks
compile) and independently reviewable:

- **M0 — Scaffolding.** `go.mod`, CI (GitHub Actions: vet, race tests,
  benchmarks on amd64), this spec. *(this PR)*
- **M1 — Core primitives.** `Sequence`, `RingBuffer[T]`, util; unit tests
  incl. a padding/`unsafe.Offsetof` layout test and wraparound tests.
- **M2 — SPSC path.** `SingleProducerSequencer`, `SequenceBarrier`,
  `Yielding` + `BusySpin` strategies, `EventProcessor`; race-detector stress
  test (10⁷ events, checksum + ordering assertions); first benchmark vs
  buffered channel.
- **M3 — Wait strategies + lifecycle.** `Sleeping`, `Blocking`,
  alert/shutdown protocol, `Disruptor[T]` DSL with fan-out (1P → N independent
  consumers).
- **M4 — Dependency graphs.** `After(...)` gating, pipeline & diamond
  topology tests (journal ‖ replicate → business-logic, per the LMAX case
  study).
- **M5 — Multi-producer.** `MultiProducerSequencer` with availableBuffer;
  N-producers stress tests; MPSC/MPMC benchmarks.
- **M6 — Hardening & docs.** Fuzz/long-run soak tests, `bench/` comparison
  table in README, examples (`examples/pipeline`, `examples/fanout`), API
  freeze, godoc pass.

Stretch (post-v1): panic handler hook, `TryPublishEvent` translator helpers,
work-pool (competing consumers), arm64 CI, optional `unsafe` relaxed atomics
behind a build tag if benchmarks justify it.

## 8. Testing Plan

1. **Unit tests** per component (sequence init/CAS, ring indexing & wrap at
   `int64` boundaries near overflow, gate math off-by-one at exactly
   `capacity` in flight).
2. **Race detector everywhere**: `go test -race ./...` is the merge gate; the
   in-place slot mutation contract is exactly what `-race` is good at
   catching.
3. **Stress/soak**: every topology (SPSC, fan-out, pipeline, diamond, MPSC)
   pushes ≥10⁷ events asserting (a) order per consumer, (b) no loss/dup via
   sum & count checks, (c) clean shutdown with no leaked goroutines
   (`goleak`).
4. **Determinism traps**: tiny ring (capacity 2, 4) tests to force constant
   wrapping and full-buffer gating; slow-consumer tests to exercise producer
   backpressure.
5. **Benchmarks** (`bench/`): disruptor vs `chan` (buffered, same capacity)
   for SPSC/MPSC/fan-out; report ops/sec, ns/op, allocs/op (must be 0),
   and p50/p99 latency via HDR-style histogram in a separate harness.
   Benchmarks pin `GOMAXPROCS` and document the machine, echoing the deck's
   benchmark-methodology emphasis.

## 9. Success Criteria

- Zero allocations per publish/consume (asserted by `testing.AllocsPerRun`).
- SPSC throughput ≥ 3× buffered channel on a modern 4+ core amd64 box
  (indicative target, not a hard gate; the honest gate is the published
  comparison table).
- All tests green under `-race`; no goroutine leaks after `Shutdown`.
- Public API documented; examples runnable via `go run`.

## 10. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Go scheduler starvation from spinning consumers | `Yielding` default, docs on GOMAXPROCS sizing, `Blocking` strategy for constrained environments |
| Seq-cst atomics leave performance on the table vs Java's lazySet | Benchmark first; only then consider build-tagged `unsafe` relaxed ops (stretch) |
| In-place slot access is an unsafe-by-contract API | Loud docs, race-detector tests, optional copying convenience API (`Consume(func(T))`) for safety-first users |
| Multi-producer availableBuffer complexity | Land it last (M5), after SPSC/graph paths are proven; port the Java algorithm faithfully with its tests |
| False-sharing pads bloat structs | Only `Sequence` and sequencer hot fields are padded; measured with layout tests |

---

## 11. Status vs. Plan (updated after implementation + review)

Implemented: M0–M5 in full, plus the profile-driven optimization pass
(docs/OPTIMIZATION.md) and a post-review hardening pass. Known deviations
from the text above, kept here so this document stops lying:

- **"No `unsafe` in v1" is superseded.** The O2 optimization introduced an
  amd64 assembly release-store fast path (with `unsafe.Pointer` casts),
  gated behind `!race` build tags; race builds and all other architectures
  use `sync/atomic` exactly as §6 describes. Rationale, disassembly
  evidence, and measurements live in docs/OPTIMIZATION.md.
- **Lifecycle enforcement (§5.8) is now implemented as specified**:
  claiming after Shutdown panics; `NextN`/`TryNext` validate
  `1 <= n <= capacity`; a failed `Start` (no handlers) leaves the instance
  usable so handlers can be added and `Start` retried.
- **`TryNextN` was not shipped** — `TryNext` (single slot), `NextN`, and
  `PublishBatch` cover the measured use cases; a claim-up-to-N API remains
  future work if a real consumer needs it.
- **CI exists** (.github/workflows/ci.yml): gofmt, vet, race suite
  (fallback store path), non-race suite + 10M-event stress (assembly store
  path), and a benchmark smoke run.
- **Examples exist**: examples/fanout, examples/pipeline.
- **M6 gaps closed post-review**: a native Go fuzz target
  (FuzzDisruptorRoundTrip: fuzzed capacity/count/batch/producer-mode with
  loss/dup/order/dependency invariants; seed corpus runs on every test),
  a dependency-free goroutine-leak regression test (50 start/shutdown
  cycles), arm64 CI (ubuntu-24.04-arm runner exercising the sync/atomic
  fallback on real ARM hardware), and WithPanicHandler (recovered value +
  sequence; the event is treated as handled — progress is one monotonic
  counter, so retry/park is impossible by construction).
- **Shutdown caveats are documented on the method**: a ctx error does not
  guarantee processor goroutines exited (alerts cannot interrupt a blocked
  handler), and in multi-producer mode a claimed-but-never-published slot
  stalls the drain until ctx expires.
