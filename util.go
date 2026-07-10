package lmax

// CacheLinePad is the padding used around hot counters. 128 bytes covers two
// 64-byte cache lines, which also defeats the adjacent-line prefetcher on
// modern Intel parts.
const CacheLinePad = 128

func isPow2(n int64) bool { return n > 0 && n&(n-1) == 0 }

func log2(n int64) uint {
	var r uint
	for n > 1 {
		n >>= 1
		r++
	}
	return r
}

// minSeq returns the minimum value across seqs, starting from def.
func minSeq(seqs []*Sequence, def int64) int64 {
	min := def
	for _, s := range seqs {
		if v := s.Load(); v < min {
			min = v
		}
	}
	return min
}
