//go:build amd64 && !race

#include "textflag.h"

// func asmStoreRel64(addr *int64, v int64)
// Plain MOV: x86-TSO gives every store release semantics; the function call
// boundary prevents the compiler from reordering it with earlier stores.
TEXT ·asmStoreRel64(SB), NOSPLIT, $0-16
	MOVQ	addr+0(FP), AX
	MOVQ	v+8(FP), CX
	MOVQ	CX, (AX)
	RET

// func asmStoreRel32(addr *int32, v int32)
TEXT ·asmStoreRel32(SB), NOSPLIT, $0-12
	MOVQ	addr+0(FP), AX
	MOVL	v+8(FP), CX
	MOVL	CX, (AX)
	RET
