# unlatch

`unlatch` is a highly concurrent, lock-free hashmap for Go, designed to scale across multiple CPU cores without global locks. It offers concurrent safety while keeping read overhead low under thread contention.

## Key Features

- **Wait-Free Reads**: Optimistic versioned read paths execute zero memory writes, keeping read operations wait-free and scaling linearly with core counts.
- **Lock-Free Writes**: Employs a CAS-based 2-Phase Commit (2PC) write and insert protocol.
- **Cache-Line Optimized Buckets**: Bins and overflow buckets are aligned and padded to 64 bytes to minimize CPU L1 cache-line fetch overhead.
- **Cooperative Resizing**: Table migration is chunked into 64-bin strides, enabling concurrent writer threads to help migrate bins cooperatively during resizes. Resizing states are localized to prevent cross-phase race conditions.
- **Boxed Atomic Entries**: Keys and values are boxed in heap-allocated entries to enable atomic pointer swaps, preventing torn reads and satisfying Go's race detector.
- **Striped Count Counters**: Minimizes write contention when tracking map sizes by sharding size updates across multiple padded stripes.

## Installation

```bash
go get github.com/cuprite-io/unlatch
```

## Quick Start

```go
package main

import (
	"fmt"
	"github.com/cuprite-io/unlatch"
)

func main() {
	// Create a new map with default options
	m := unlatch.New[string, int]()

	// Put value
	m.Put("foo", 42)

	// Get value
	if val, ok := m.Get("foo"); ok {
		fmt.Printf("foo: %d\n", val) // Prints "foo: 42"
	}

	// Delete value
	m.Delete("foo")

	// Get size
	fmt.Printf("size: %d\n", m.Size()) // Prints "size: 0"
}
```

## Configuration Options

You can pass configuration options when creating a map using `unlatch.New`:

```go
// Create map with initial capacity of 1024 and 50% load factor threshold
m := unlatch.New[string, int](
	unlatch.WithCapacity(1024),
	unlatch.WithLoadFactor(0.50),
)
```

## Limitations

Before using `unlatch`, please note the following constraints:

1. **Heap Allocations on Insert/Update**: To guarantee atomic updates, prevent torn reads on multi-word types (like `string`), and remain fully compliant with Go's race detector, keys and values are boxed in heap-allocated `entry` structs. Consequently, every write (`Put`) or update allocates on the heap.
2. **Overflow Pool Exhaustion**: Overflow link buckets are allocated from a preallocated pool. Under highly pathological key collision patterns (e.g., hash collision attacks) or under-configured map capacities, the link pool can be exhausted. This will trigger a cooperative resize to redistribute keys even if the configured load factor threshold has not been met.
3. **Weakly Consistent Sizing**: Map size tracking utilizes a striped atomic counter to avoid CPU core contention on the hot path. The `Size()` method returns an approximate, weakly consistent count rather than a strictly synchronized real-time figure.
4. **Memory Footprint**: Primary bins and overflow buckets are padded to 64 bytes to align with CPU cache lines. While this prevents false sharing, it results in a larger memory overhead per slot compared to standard Go maps.

## License

Apache License 2.0. See the `LICENSE` file for details.
