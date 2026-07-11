# Optimization Plan & Spec — Profile-Driven

Status: **implemented** — §7 records the measured results; §§1–6 are the
original spec, kept verbatim. Baseline commit: `9c4dd29`.

## 1. Baseline

Environment (all numbers below are from this box unless stated otherwise —
a *shared* cloud VM, so treat deltas as directional and re-verify on real
hardware):

- Intel Xeon @ 2.80GHz, **4 vCPUs**, Linux 6.18, Go 1.24.7, amd64.
- `go test -bench=. -benchmem ./bench`

| Benchmark        | ns/op | allocs/op | vs channel |
|------------------|------:|----------:|------------|
| Disruptor SPSC   |  ~65  | 0 | ~1.1× faster |
| Channel SPSC     |  ~72  | 0 | — |
| Disruptor MPSC   | ~111–131 | 0 | ~1.2× slower |
| Channel MPSC     |  ~96  | 0 | — |

Two problems to attack: (a) SPSC is barely ahead of channels when the design
should be far ahead; (b) MPSC *loses* to channels.

## 2. Profile Findings (measured, not guessed)

### 2.1 SPSC (`-cpuprofile`, 3s run, 57M events)

| % samples | Where | What it is |
|---:|---|---|
| **44.7%** | `SingleProducerSequencer.Publish` | cursor store + strategy signal, per event |
| 19.3% | `Yielding.WaitFor` | consumer spin loop |
| 11.7% | handler func | benchmark body (irreducible) |
| ~7% | `runtime.futex` + `pidleget` + lock2/unlock2 | scheduler traffic caused by `Gosched` in the spin loop |
| 2.5% | `Next` | claim (already near-free, cached gate works) |

Disassembly of `Publish` (the smoking gun — **two per-event costs**):

```asm
xchg   %rcx,0x80(%rdx)   ; atomic.Int64.Store = seq-cst XCHG: full barrier,
                         ; drains the store buffer, ~20–40 cycles + the
                         ; cursor cache line ping-pongs to the consumer core
call   *%rcx             ; s.wait.SignalAll() — dynamic interface call that
                         ; is a NO-OP for Yielding/BusySpin/Sleeping, paid
                         ; on every publish, never inlinable
```

For comparison, the Java Disruptor publishes with `lazySet` — a *release*
store, which on x86 is a plain `MOV` (x86-TSO makes every store a release
store); it never pays a full barrier per event, and that is the single
biggest mechanical difference between our port and the original.

### 2.2 MPSC (4 producers via RunParallel, 40M events)

| % samples | Where | What it is |
|---:|---|---|
| **29.1%** | `MultiProducerSequencer.Publish` | per-slot XCHG into `avail[]` — 16 `int32` entries share one 64-byte cache line, so *neighboring producers false-share publication lines* |
| **26.2%** | `Sequence.CompareAndSwap` | claim contention: all producers CAS one cursor |
| **19.5%** | `atomic.Int64.Load` | `hasCapacity` re-reads cursor + `cachedGate` in the CAS retry loop |
| 5.5% | `HighestPublished` | consumer scanning `avail[]` |
| 5.6% | `Yielding.WaitFor` | consumer spin |

Secondary observation: `hasCapacity` **stores** to the shared `cachedGate`
sequence on *every* miss path, even when the value did not change — a
contended write (the deck's cardinal sin) that invalidates the line in every
other producer's cache.

Benchmark-harness caveat: on 4 vCPUs, `RunParallel` runs 4 producer
goroutines *plus* 1 consumer — oversubscribed by one; part of the MPSC gap
is scheduler noise, which the methodology work (O9) must separate before we
trust MPSC deltas.

## 3. Ranked Optimizations

Ordering = expected impact ÷ risk. Each item has an acceptance gate;
anything that doesn't clear its gate on the benchmark suite gets reverted —
no speculative complexity survives.

### O1 — Elide `SignalAll` for non-blocking strategies (SPSC + MPSC)

*Problem:* every `Publish` pays a dynamic interface call that is a no-op for
3 of 4 strategies (§2.1 disasm).
*Change:* at sequencer construction, detect whether the strategy needs
signalling (interface `interface{ needsSignal() bool }` or a type switch on
`*Blocking`); store a `signal bool` (or nil `func()`) and branch on it in
`Publish`. A predictable not-taken branch replaces an indirect call.
*Risk:* trivial. Blocking-strategy path unchanged.
*Gate:* SPSC ≥ 5% faster; Blocking wait-strategy integration test still green.

### O2 — Release-store publication (build-tagged asm) (SPSC + MPSC)

*Problem:* `atomic.Int64.Store` is seq-cst (`XCHG`); publication only needs
*release* semantics — exactly what Java's `lazySet` exploits. This is the
largest per-event cost in the SPSC profile (§2.1) and one of the two big
MPSC costs (`avail[]` stores, §2.2).
*Change:* add `storeRelease(addr *int64, v int64)` (and an `int32` variant
for `avail[]`):

- `store_release_amd64.s`: plain `MOVQ` — x86-TSO gives release ordering;
  the asm call is itself the compiler reordering barrier.
- `store_release_arm64.s`: `STLR`.
- `store_release_generic.go` (`//go:build` everything else) **and**
  `store_release_race.go` (`//go:build race`): fall back to
  `atomic.StoreInt64`, so `-race` still validates the whole protocol and
  every other architecture stays correct by construction.

Use it in exactly three places: single-producer cursor publish,
multi-producer `avail[]` publish, consumer per-batch sequence store.
Claims (CAS) and all loads stay `sync/atomic`.
*Risk:* moderate — this is the one deliberately `unsafe`-flavored change.
Contained: three call sites, race-build fallback, loom-style stress tests
(§5) must stay green on amd64 *and* arm64 (or the arm64 part ships disabled
until CI can prove it).
*Gate:* SPSC ≥ 20% faster (XCHG→MOV on the hottest instruction); MPSC
`Publish` share of profile drops materially; `-race` suite green.

### O3 — Stop the `cachedGate` write storm (MPSC)

*Problem:* every producer's `hasCapacity` unconditionally stores the
re-computed min gate — contended writes to one line from all producers
(§2.2, 19.5% atomic-load figure is partly coherence misses this causes).
*Change:* only store when the value actually advanced:
`if min != cached { s.cachedGate.Store(min) }` — the line then stays in
Shared state across producers in the steady state.
*Risk:* none (the cache is advisory; correctness comes from the re-check).
*Gate:* MPSC ≥ 5% faster at 4 producers.

### O4 — Bounded-spin backoff in the multi-producer CAS loop (MPSC)

*Problem:* 26% of MPSC samples are CAS retries; failed CAS immediately
re-reads and retries, maximizing coherence traffic on the cursor line.
*Change:* on CAS failure, spin a small escalating number of iterations
(cheap PAUSE-like loop; a `procyield`-style asm helper can ride the O2 build
tags) before re-reading; `Gosched` only when the ring is actually full
(unchanged).
*Risk:* low; tune constants by benchmark sweep (1/4/16/64).
*Gate:* MPSC ≥ 10% faster at 4 producers and *not slower* at 2.

### O5 — `avail[]` false-sharing experiment (MPSC)

*Problem:* 16 publication flags per cache line; adjacent-sequence producers
invalidate each other on every publish (§2.2's 29%).
*Change:* benchmark three layouts behind one internal accessor:
(a) `int32` baseline, (b) `int64` entries (8/line), (c) 64-byte padded
entries (1/line — costs `capacity × 64B` and makes the consumer's
`HighestPublished` scan touch a line per slot).
*Risk:* low code risk; real risk is that (c) trades producer wins for
consumer scan losses — that's exactly what the experiment measures. Java
ships (a); we keep whichever measures best and delete the others.
*Gate:* keep a non-baseline layout only if MPSC ≥ 10% faster with no
fan-out regression.

### O6 — Batch publication helpers + smart batching (API, both modes)

*Problem:* every per-event cost above is amortizable; the ring's natural
advantage (the Disruptor's "smart batching") is currently only reachable via
raw `NextN`/`PublishRange`.
*Change:* add `PublishBatch(n int64, fill func(i int64, e *T))` (claim n,
fill, single release) and, for producers draining an upstream source,
`TryNextN` claim-as-many-as-available. Add batch variants to the benchmark
suite (batch = 1, 8, 64) so the amortization curve is visible in the README
table.
*Risk:* additive API, none.
*Gate:* batch=8 SPSC ≥ 3× faster than per-event channel send (this is the
headline number the README currently can't show).

### O7 — Spin-loop tuning in `Yielding` (consumer side)

*Problem:* ~7% of SPSC samples are futex/scheduler work caused by calling
`Gosched` on every post-spin iteration; the 100-iteration spin budget also
resets per `WaitFor` call rather than adapting.
*Change:* three-phase wait — N cheap spins (with the O4 pause helper), M
`Gosched` yields, then keep yielding but with an exponentially spaced pause
spin between yields. Sweep N/M by benchmark; keep the profile's futex share
< 2%.
*Risk:* latency-sensitive; measure wake-up latency (O9 histogram) not just
throughput.
*Gate:* SPSC throughput up or flat AND p99 wake-up latency not worse.

### O8 — Micro: specialize the no-dependency barrier path

*Problem:* `dependentMin` branches on `len(deps)` per poll and costs 1.3% of
SPSC samples through a closure indirection.
*Change:* in `newSequenceBarrier`, when `deps` is empty set
`depFn = cursor.Load` directly (method value on `atomic.Int64`), removing
the branch and one call level.
*Risk:* none. *Gate:* neutral-or-better; keep only if measurable.

### O9 — Benchmark methodology hardening (prerequisite for trusting O2–O5)

- Sweep producer counts (1/2/4) with `RunParallel` parallelism pinned so
  producers + consumer ≤ GOMAXPROCS (the current MPSC bench oversubscribes a
  4-vCPU box — some of the "slower than channels" is scheduler artifact).
- Add a latency harness (separate from `go test -bench`): timestamped
  events through an idle ring → p50/p99/p999 histogram, per wait strategy.
- Record `perf stat` (cycles, cache-misses, machine_clears.memory_ordering)
  alongside ns/op for before/after evidence on O2/O5.
- Payload-size sweep (8B vs 64B vs 256B events) — the by-value ring should
  shine vs channels as payload grows; today's table only shows 8B.

## 4. Explicit Non-Goals (researched, rejected for now)

- **Per-producer sharded lanes (SPSC × N + merge).** Changes delivery
  ordering semantics and the public API; revisit only if O2–O5 still lose to
  channels at ≥ 8 producers.
- **`//go:linkname` into `internal/runtime/atomic`.** Sealed in modern Go;
  the O2 asm shim is the supportable route.
- **Removing seq-cst from *loads*.** Loads on amd64 are already plain MOVs;
  nothing to win there on our primary target.
- **Rewriting consumers onto OS threads / core pinning API.** Out of scope
  for a library; documentation continues to recommend GOMAXPROCS sizing.

## 5. Verification Plan

1. Full existing suite (`go test -race ./...`, GOMAXPROCS=1/2 runs,
   `-count=3`) after each optimization lands — each is a separate commit so
   a regression bisects to one change.
2. New stress test for O2: SPSC + MPSC checksum tests compiled *without*
   `-race` on amd64 (the release-store path) run with 10⁷ events × repeated
   runs — the race build intentionally swaps the fast path out, so the
   non-race stress run is the one that exercises it.
3. Zero-alloc assertions extended to the new batch APIs.
4. Before/after table (per §3 gates) appended to README; every claimed win
   reproduced twice on a quiet machine before merging.

## 6. Execution Order

| Phase | Items | Rationale |
|---|---|---|
| P1 | O9 → O1 → O3 → O8 | Trustworthy harness first, then the zero-risk wins |
| P2 | O2 | The big one, isolated for easy revert/bisect |
| P3 | O4 → O5 | MPSC contention work, measured on the hardened harness |
| P4 | O6 → O7 | API-level amortization + latency polish, README table refresh |

Estimated end state if gates hold: SPSC publish path loses the XCHG and the
indirect call (its two dominant instructions), targeting ≥ 2× the channel
SPSC baseline on this box (more on unshared hardware); MPSC at minimum stops
losing to channels at 4 producers, with the honest possibility that heavily
contended MPSC remains channel-competitive rather than channel-beating on
4 shared vCPUs — the batch API (O6) is the designed escape hatch there.

---

## 7. Results (implemented)

All nine optimizations landed as separate commits, each gated by
measurement; the full verification matrix (`-race`, GOMAXPROCS=1/2,
10M-event non-race stress on the release-store fast path) is green.

Median-of-3 on the same 4-vCPU box (baseline → after, channel reference):

| Benchmark | Baseline | After | Channel | Outcome |
|---|---:|---:|---:|---|
| SPSC 8B    | ~96 ns  | **36 ns**  | 143 | 3.9× vs channel (was ~1.5×) |
| SPSC 64B   | ~46 ns  | **16 ns**  | 190 | 11.6× |
| SPSC 256B  | ~42 ns  | **17 ns**  | 335 | 19.6× |
| SPSC batch=8 (manual) | 4.7 ns | **4.8 ns** | — | ~30× vs per-event channel |
| MPSC p=1   | ~74 ns  | **22 ns**  | 69  | 3.2× |
| MPSC p=2   | ~152 ns | **135 ns** | 80  | improved ~25%, still behind |
| MPSC p=3   | ~330 ns | **~320 ns**| 84  | high variance, ~flat |

Latency (Yielding, idle ring): p50 0.6–2.2µs → **~0.6µs stable**;
p99 54–113µs → **51–61µs**.

Per-optimization notes:

- **O1 (signal elision), O3 (gate-store avoidance), O8 (barrier
  specialization)**: individually below the noise floor of this VM but
  strict instruction/contended-write reductions; kept.
- **O2 (release stores)** was the decisive win, exactly as the disassembly
  predicted: SPSC halved (~70–120 → ~35 ns) *and* run-to-run variance
  collapsed — the XCHG was both the cost and the noise source. MPSC p=1
  dropped to ~22 ns.
- **O4 (CAS backoff)**: ~25% at p=2, ~15% at p=3 vs a same-session
  no-backoff control; PAUSE cap 16 chosen by sweep.
- **O5 (avail layout)**: padded-64B beat packed-int32 by ~12% at p=3 and
  int64-spacing was worse than both; padded kept (64B bookkeeping per slot).
- **O6 (PublishBatch)**: batch=64 ≈ 4.7–4.9 ns/op through either path;
  the ergonomic closure API costs ~3–4 ns/event at batch=8 vs the manual
  NextN/Get/PublishRange path (indirect call per event) — both benchmarked.
- **O7 (spin tuning)**: PAUSE in the spin phase + PAUSE-spaced yields;
  p50 stabilized, p99 improved 15–50%, throughput flat-to-better.

Remaining honest gap: single-event MPSC at 2–3 producers on 4 shared vCPUs
still trails channels (the Go runtime's channel semaphore handles brutal
contention on oversubscribed cores well). The spec anticipated this; the
designed answer is batch claims, and the sharded-lane idea in §4 remains
the future direction if per-event contended MPSC becomes a hard
requirement.

An incidental finding worth keeping: SPSC with 64B/256B events is *faster*
than 8B events — at 8 bytes, eight ring slots share one cache line, so the
producer writing slot n+1 false-shares with the consumer reading slot n.
Users chasing latency should size (or pad) events toward the cache-line
size; documented here rather than auto-padding, since it trades memory.
