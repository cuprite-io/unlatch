package unlatch

import (
	"sync/atomic"
)

const numStripes = 64 // 64 stripes balance memory and contention scaling

type paddedInt64 struct {
	val int64
	_   [56]byte // Padding to align struct to 64 bytes (prevent false sharing)
}

// stripeCounter implements a highly concurrent size counter.
// Contention is minimized by sharding updates across stripes using the key's hash.
type stripeCounter struct {
	stripes [numStripes]paddedInt64
}

// add increments or decrements the counter.
func (c *stripeCounter) add(hash uint64, delta int64) {
	idx := hash & (numStripes - 1)
	atomic.AddInt64(&c.stripes[idx].val, delta)
}

// sum calculates the approximate size by summing all stripes.
func (c *stripeCounter) sum() int64 {
	var total int64
	for i := 0; i < numStripes; i++ {
		total += atomic.LoadInt64(&c.stripes[i].val)
	}
	if total < 0 {
		return 0
	}
	return total
}

// reset clears all stripes to zero.
func (c *stripeCounter) reset() {
	for i := 0; i < numStripes; i++ {
		atomic.StoreInt64(&c.stripes[i].val, 0)
	}
}
