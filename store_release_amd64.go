//go:build amd64 && !race

package lmax

import (
	"sync/atomic"
	"unsafe"
)

// Release-store fast path. Publication (cursor store, avail[] store,
// consumer sequence store) only needs release semantics, but sync/atomic
// stores are sequentially consistent — XCHG on amd64, a full barrier that
// drains the store buffer on every event. On x86-TSO every plain store
// already has release ordering, so a plain MOV issued from a non-inlinable
// assembly function (the call is the compiler reordering barrier) is
// sufficient and far cheaper. This mirrors the Java Disruptor's lazySet.
//
// The race build (and every non-amd64 arch) uses the sync/atomic fallback in
// store_release_fallback.go, so `go test -race` still validates the protocol
// with sanctioned synchronization edges.

// asmStoreRel64, asmStoreRel32, and procYield are implemented in
// store_release_amd64.s.
func asmStoreRel64(addr *int64, v int64)
func asmStoreRel32(addr *int32, v int32)

// procYield executes n PAUSE instructions — contention backoff that eases
// pressure on a contended cache line without a scheduler round-trip.
func procYield(n int32)

func storeRelease64(a *atomic.Int64, v int64) {
	asmStoreRel64((*int64)(unsafe.Pointer(a)), v)
}

func storeRelease32(a *atomic.Int32, v int32) {
	asmStoreRel32((*int32)(unsafe.Pointer(a)), v)
}
