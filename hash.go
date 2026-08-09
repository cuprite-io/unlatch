package unlatch

import (
	"fmt"
	"hash/maphash"
)

// Hasher is a function type that takes a comparable key and returns a 64-bit hash.
type Hasher[K comparable] func(K) uint64

// DefaultHasher returns a type-optimized Hasher for K.
// The type check is performed once at initialization time to avoid type-assertion overhead on the hot path.
func DefaultHasher[K comparable]() Hasher[K] {
	var zero K
	switch any(zero).(type) {
	case string:
		seed := maphash.MakeSeed()
		return func(k K) uint64 {
			return maphash.String(seed, any(k).(string))
		}
	case int64:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(int64)))
		}
	case uint64:
		return func(k K) uint64 {
			return hashUint64(any(k).(uint64))
		}
	case int:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(int)))
		}
	case uint:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(uint)))
		}
	case uintptr:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(uintptr)))
		}
	case int32:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(int32)))
		}
	case uint32:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(uint32)))
		}
	case int16:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(int16)))
		}
	case uint16:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(uint16)))
		}
	case int8:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(int8)))
		}
	case uint8:
		return func(k K) uint64 {
			return hashUint64(uint64(any(k).(uint8)))
		}
	default:
		// Fallback for custom comparable structs/interfaces.
		// Uses string serialization as a safe fallback seed hash.
		seed := maphash.MakeSeed()
		return func(k K) uint64 {
			s := fmt.Sprintf("%v", k)
			return maphash.Bytes(seed, []byte(s))
		}
	}
}

// hashUint64 is a highly performant 64-bit integer mixer (MurmurHash3 finalizer block).
func hashUint64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
