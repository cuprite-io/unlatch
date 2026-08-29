# unlatch

<p align="center">
  <b>High-concurrency lock-free hashmap for Go.</b>
</p>

<p align="center">
  <img src="assets/mascot.png" alt="unlatch Mascot" width="300" />
</p>

<p align="center">
  <code>unlatch</code> is a highly concurrent, lock-free hashmap for Go, designed to scale across multiple CPU cores without global locks. It offers concurrent safety while keeping read overhead low under thread contention.
</p>

---

## ✨ Key Features

- **Wait-Free Reads**: Optimistic versioned read paths execute zero memory writes, keeping read operations wait-free and scaling linearly with core counts.
- **Lock-Free Writes**: Employs a CAS-based 2-Phase Commit (2PC) write and insert protocol.
- **Cache-Line Optimized Buckets**: Bins and overflow buckets are aligned and padded to 64 bytes to minimize CPU L1 cache-line fetch overhead.
- **Cooperative Resizing**: Table migration is chunked into 64-bin strides, enabling concurrent writer threads to help migrate bins cooperatively during resizes. Resizing states are localized to prevent cross-phase race conditions.
- **Boxed Atomic Entries**: Keys and values are boxed in heap-allocated entries to enable atomic pointer swaps, preventing torn reads and satisfying Go's race detector.
- **Striped Count Counters**: Minimizes write contention when tracking map sizes by sharding size updates across multiple padded stripes.

## 📊 Performance

Benchmarks run against standard concurrent Go map implementations (`sync.Map`, 64-way `ShardedMap` with `RWMutex`, and a standard `RWMutex` map) with 10,000 keys under parallel multi-core execution:

| Workload | Implementation | Latency (ns/op) | Latency (ms) | Memory (B/op) | Allocs/op |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Read-Heavy (100% Get)** | **unlatch** | **~21.27** | **0.000021 ms** | **0 B/op** | **0 allocs/op** |
| | `sync.Map` | ~51.92 | 0.000052 ms | 0 B/op | 0 allocs/op |
| | `ShardedMap (64 shards)` | ~57.56 | 0.000058 ms | 0 B/op | 0 allocs/op |
| | `MutexMap (RWMutex)` | ~126.10 | 0.000126 ms | 0 B/op | 0 allocs/op |
| **Mixed (50% Get / 50% Put)** | **unlatch** | **~60.57** | **0.000061 ms** | **12 B/op** | **0 allocs/op** |
| | `sync.Map` | ~112.10 | 0.000112 ms | 15 B/op | 0 allocs/op |
| | `ShardedMap (64 shards)` | ~109.50 | 0.000110 ms | 0 B/op | 0 allocs/op |
| | `MutexMap (RWMutex)` | ~106.80 | 0.000107 ms | 0 B/op | 0 allocs/op |
| **Write-Only (100% Put)** | **unlatch** | **~82.56** | **0.000083 ms** | **24 B/op** | **1 allocs/op** |
| | `sync.Map` | ~424.50 | 0.000425 ms | 32 B/op | 2 allocs/op |
| | `ShardedMap (64 shards)` | ~101.70 | 0.000102 ms | 0 B/op | 0 allocs/op |
| | `MutexMap (RWMutex)` | ~217.50 | 0.000218 ms | 0 B/op | 0 allocs/op |

## 📦 Installation

```bash
go get github.com/cuprite-io/unlatch
```

## 🚀 Quick Start

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

## ⚙️ Configuration Options

You can pass configuration options when creating a map using `unlatch.New`:

```go
// Create map with initial capacity of 1024 and 50% load factor threshold
m := unlatch.New[string, int](
	unlatch.WithCapacity(1024),
	unlatch.WithLoadFactor(0.50),
)
```

## ⚖️ Design Trade-offs & Limitations

While `unlatch` is optimized for high concurrency, low latency, and wait-free reads, its architecture makes specific deliberate engineering trade-offs:

1. **Heap Allocation on Writes (Boxed Atomic Entries)**:
   - **Trade-off**: To guarantee atomic updates, prevent torn reads on multi-word types (like `string`, `slice`, or interface types), and ensure full compliance with Go's race detector without heavyweight locks, keys and values are boxed in heap-allocated `entry` structs.
   - **Impact**: Read paths remain completely zero-allocation (`0 B/op`, `0 allocs/op`), while write paths (`Put`) allocate memory for the entry container.

2. **Memory Footprint vs. Cache-Line Alignment**:
   - **Trade-off**: Primary bins and overflow buckets are explicitly aligned and padded to 64 bytes to eliminate false sharing and ensure single-cache-line fetches during lookups.
   - **Impact**: Higher base memory consumption per slot compared to standard Go maps or tightly packed lock-based structures.

3. **Weakly Consistent Sizing (`Size()`)**:
   - **Trade-off**: Map size tracking utilizes a striped atomic counter array to prevent CPU core cache coherency bottlenecks on hot write paths.
   - **Impact**: `Size()` provides an approximate, weakly consistent count rather than a strictly synchronized, point-in-time snapshot.

4. **Link Pool Exhaustion & Cooperative Resizing**:
   - **Trade-off**: Overflow chaining utilizes a preallocated pool of link buckets to avoid unbounded heap fragmentation during hash collisions.
   - **Impact**: Under pathological key collision patterns (e.g. hash collision attacks) or severely under-provisioned initial capacities, link pool exhaustion will trigger a cooperative table resize to redistribute keys even before reaching the target load factor threshold.

5. **Cooperative Resize Overhead on Writers**:
   - **Trade-off**: Resizing is fully non-blocking and decentralized, chunked into 64-bin strides.
   - **Impact**: While readers remain completely unaffected and wait-free during migrations, concurrent writers temporarily assist in migrating bins, causing a brief latency spike for write operations during active expansion.

## 📄 License

Apache License 2.0. See the `LICENSE` file for details.

