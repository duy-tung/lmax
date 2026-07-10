//go:build !amd64 || race

package lmax

import "sync/atomic"

// Portable/race-detector fallback for the release stores: sequentially
// consistent atomics are strictly stronger than release, so correctness is
// preserved everywhere; amd64 non-race builds use the MOV fast path in
// store_release_amd64.{go,s}. arm64 could use STLR the same way but ships on
// this fallback until CI can prove the assembly on real hardware.
func storeRelease64(a *atomic.Int64, v int64) { a.Store(v) }

func storeRelease32(a *atomic.Int32, v int32) { a.Store(v) }

// procYield backoff is a no-op on the fallback path (immediate retry —
// the pre-backoff behavior); only the amd64 fast path has PAUSE.
func procYield(n int32) {}
