package unlatch

import (
	"sync/atomic"
	"unsafe"
)

// Map is a high-performance, lock-free concurrent hashmap using a 64-bit packed header.
type Map[K comparable, V any] struct {
	table            unsafe.Pointer // *Table[K, V] - Active hash table
	nextTable        unsafe.Pointer // *Table[K, V] - Temporary table during resize
	resizeInProgress uint32         // Atomic boolean (0 or 1) indicating if a resize is active
	hasher           Hasher[K]      // Hash function optimized for key type K
	size             *stripeCounter  // Striped size counter to avoid write contention
	maxLoadFactor    float64        // Max load factor threshold before triggering resize
}

type mapOptions struct {
	initialCapacity int
	loadFactor      float64
	hasher          any
}

// Option represents a configuration builder function for Map.
type Option interface {
	apply(*mapOptions)
}

type optionFunc func(*mapOptions)

func (f optionFunc) apply(o *mapOptions) {
	f(o)
}

// WithCapacity configures the initial size of the map.
// The capacity will be rounded up to the nearest power of 2.
func WithCapacity(cap int) Option {
	return optionFunc(func(o *mapOptions) {
		o.initialCapacity = cap
	})
}

// WithLoadFactor configures the load factor threshold before the map triggers an automatic resize.
// Default is 0.70 (70% occupancy).
func WithLoadFactor(lf float64) Option {
	return optionFunc(func(o *mapOptions) {
		o.loadFactor = lf
	})
}

// WithHasher configures a custom hash function for the key type.
func WithHasher[K comparable](h Hasher[K]) Option {
	return optionFunc(func(o *mapOptions) {
		o.hasher = h
	})
}

// New creates a new highly concurrent, lock-free Map with the provided configuration options.
func New[K comparable, V any](opts ...Option) *Map[K, V] {
	cfg := &mapOptions{
		initialCapacity: 32, // default starting capacity (must be power of 2)
		loadFactor:      0.70,
		hasher:          nil,
	}

	for _, opt := range opts {
		opt.apply(cfg)
	}

	var hasher Hasher[K]
	if cfg.hasher != nil {
		hasher = cfg.hasher.(Hasher[K])
	} else {
		hasher = DefaultHasher[K]()
	}

	// Ensure capacity is a power of 2
	cap := 1
	for cap < cfg.initialCapacity {
		cap <<= 1
	}

	tbl := newTable[K, V](cap)
	m := &Map[K, V]{
		table:         unsafe.Pointer(tbl),
		hasher:        hasher,
		size:          &stripeCounter{},
		maxLoadFactor: cfg.loadFactor,
	}
	return m
}

// activeTable returns the current active Table pointer atomically.
func (m *Map[K, V]) activeTable() *Table[K, V] {
	return (*Table[K, V])(atomic.LoadPointer(&m.table))
}

// Size returns the approximate number of elements in the map.
func (m *Map[K, V]) Size() int {
	return int(m.size.sum())
}

// Clear removes all elements from the map and resets the size counter.
func (m *Map[K, V]) Clear() {
	m.size.reset()
	tbl := m.activeTable()
	capacity := len(tbl.bins)
	newTbl := newTable[K, V](capacity)
	atomic.StorePointer(&m.table, unsafe.Pointer(newTbl))
}
