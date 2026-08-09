package unlatch

import (
	"sync/atomic"
	"unsafe"
)

// Slot and Bin constants
const (
	stateEmpty     = 0 // Slot is empty and reusable
	stateTryInsert = 1 // Slot is claimed for insertion but not yet valid/published
	stateValid     = 2 // Slot contains valid, readable key-value data
	stateUpdating  = 3 // Slot is being updated in-place (used to block readers)

	binNormal       = 0 // Bin is active and operating normally
	binInTransfer   = 1 // Bin is currently being migrated to the new table
	binDoneTransfer = 2 // Bin migration has finished; redirect to new table
)

// entry boxes key and value to ensure atomic pointer loads/stores, preventing data races and torn reads.
type entry[K comparable, V any] struct {
	key   K
	value V
}

// bin represents a primary hash bucket containing a 64-bit metadata header,
// an overflow pointer index, and inline entry pointers.
// Laid out to align with CPU cache lines (64 bytes).
type bin[K comparable, V any] struct {
	header  uint64            // [version:31 | signature:24 | lock:1 | status:2 | slotStates:6]
	linkIdx uint32            // Index of first overflow linkBucket in the links pool (0 means none)
	_       uint32            // Padding
	entries [3]unsafe.Pointer // Pointers to *entry[K, V]
	_       [24]byte          // Padding to make bin exactly 64 bytes
}

// linkBucket represents a chained bucket stored in the preallocated pool.
// It has the same capacity as a bin but operates headerless to minimize overhead.
type linkBucket[K comparable, V any] struct {
	entries [3]unsafe.Pointer // Pointers to *entry[K, V]
	nextIdx uint32            // Index of next overflow bucket in the pool (0 if none)
	states  uint32            // Low 6 bits: 2 bits per slot (Empty/TryInsert/Valid/Updating)
	_       [32]byte          // Padding to make linkBucket exactly 64 bytes
}

// Table contains the flat arrays of primary bins and overflow link buckets.
type Table[K comparable, V any] struct {
	bins         []bin[K, V]
	links        []linkBucket[K, V]
	linkNext     uint32 // Atomic index counter for allocating link buckets from links pool
	capacityMask uint64 // (Capacity - 1) for fast bitwise modulo indexing
	resizeIdx    int64  // Atomic stride counter for resizing progress
	resizeCount  int64  // Atomic bin migration counter for finalizing resize
}

// newTable creates and preallocates a Table with capacity bins.
func newTable[K comparable, V any](capacity int) *Table[K, V] {
	bins := make([]bin[K, V], capacity)
	// preallocate links pool (12.5% of bin count, minimum 16)
	linksSize := capacity / 8
	if linksSize < 16 {
		linksSize = 16
	}
	links := make([]linkBucket[K, V], linksSize)
	return &Table[K, V]{
		bins:         bins,
		links:        links,
		capacityMask: uint64(capacity - 1),
	}
}

// allocLink allocates a link bucket from the Table's pool using lock-free index increment.
// Returns the index of the allocated bucket, or 0 if the pool is exhausted.
func (t *Table[K, V]) allocLink() uint32 {
	idx := atomic.AddUint32(&t.linkNext, 1)
	if idx >= uint32(len(t.links)) {
		return 0 // Pool exhausted
	}
	return idx
}

// Header Bitwise Helper Functions

func getSlotState(header uint64, slot int) uint64 {
	return (header >> (slot * 2)) & 3
}

func setSlotState(header uint64, slot int, state uint64) uint64 {
	mask := uint64(3) << (slot * 2)
	return (header &^ mask) | (state << (slot * 2))
}

func getBinState(header uint64) uint64 {
	return (header >> 6) & 3
}

func setBinState(header uint64, state uint64) uint64 {
	return (header &^ (3 << 6)) | (state << 6)
}

func isLocked(header uint64) bool {
	return (header & (1 << 8)) != 0
}

func setLock(header uint64, locked bool) uint64 {
	if locked {
		return header | (1 << 8)
	}
	return header &^ (1 << 8)
}

func getTopHash(header uint64, slot int) uint8 {
	return uint8(header >> (9 + slot*8))
}

func setTopHash(header uint64, slot int, hash uint8) uint64 {
	mask := uint64(0xFF) << (9 + slot*8)
	return (header &^ mask) | (uint64(hash) << (9 + slot*8))
}

func getVersion(header uint64) uint32 {
	return uint32(header >> 33)
}

func incVersion(header uint64) uint64 {
	return header + (1 << 33)
}

// Link Bucket Helper Functions (simpler than primary bin because link buckets do not need concurrent resizing flags)

func getLinkSlotState(states uint32, slot int) uint32 {
	return (states >> (slot * 2)) & 3
}

func setLinkSlotState(states uint32, slot int, state uint32) uint32 {
	mask := uint32(3) << (slot * 2)
	return (states &^ mask) | (state << (slot * 2))
}
