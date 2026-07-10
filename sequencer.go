package lmax

// Sequencer implements the producer-side claim/publish protocol over the ring
// and exposes publication state to consumer barriers.
type Sequencer interface {
	// Next claims n slots (n >= 1), spinning while the ring is full, and
	// returns the highest claimed sequence. Slots lo..hi with
	// lo = hi - n + 1 belong exclusively to the caller until published.
	Next(n int64) int64
	// TryNext is the non-blocking variant; ok is false when the ring lacks
	// capacity right now.
	TryNext(n int64) (hi int64, ok bool)
	// Publish makes the claimed slots lo..hi visible to consumers.
	Publish(lo, hi int64)
	// Cursor is the producer progress sequence barriers wait on.
	Cursor() *Sequence
	// AddGating registers the consumer sequences the producer must not lap.
	AddGating(seqs ...*Sequence)
	// HighestPublished returns the highest sequence in [lo, hi] such that
	// every sequence in [lo, result] has been published, or lo-1 if lo has
	// not been published.
	HighestPublished(lo, hi int64) int64
}
