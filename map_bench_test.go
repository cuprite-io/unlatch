package unlatch_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cuprite-io/unlatch"
)

// Sharded map implementation to serve as a benchmark comparison (similar to public sharded map libraries)
type shardedMap struct {
	shards [64]*shard
}

type shard struct {
	sync.RWMutex
	m map[string]int
}

func newShardedMap() *shardedMap {
	sm := &shardedMap{}
	for i := 0; i < 64; i++ {
		sm.shards[i] = &shard{m: make(map[string]int)}
	}
	return sm
}

func (sm *shardedMap) getShard(key string) *shard {
	// Simple hashing for shard selection
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash = (hash ^ uint32(key[i])) * 16777619
	}
	return sm.shards[hash%64]
}

func (sm *shardedMap) Get(key string) (int, bool) {
	s := sm.getShard(key)
	s.RLock()
	val, ok := s.m[key]
	s.RUnlock()
	return val, ok
}

func (sm *shardedMap) Put(key string, val int) {
	s := sm.getShard(key)
	s.Lock()
	s.m[key] = val
	s.Unlock()
}

// RWMutex wrapped native map
type mutexMap struct {
	sync.RWMutex
	m map[string]int
}

func newMutexMap() *mutexMap {
	return &mutexMap{m: make(map[string]int)}
}

func (mm *mutexMap) Get(key string) (int, bool) {
	mm.RLock()
	val, ok := mm.m[key]
	mm.RUnlock()
	return val, ok
}

func (mm *mutexMap) Put(key string, val int) {
	mm.Lock()
	mm.m[key] = val
	mm.Unlock()
}

// Global setups for benchmarks
const numKeys = 10000

func setupKeys() []string {
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

// --- 1. Read-Only (100% Get) Benchmarks ---

func BenchmarkReadHeavy_Unlatch(b *testing.B) {
	m := unlatch.New[string, int]()
	keys := setupKeys()
	for _, k := range keys {
		m.Put(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			_, _ = m.Get(k)
		}
	})
}

func BenchmarkReadHeavy_SyncMap(b *testing.B) {
	var m sync.Map
	keys := setupKeys()
	for _, k := range keys {
		m.Store(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			_, _ = m.Load(k)
		}
	})
}

func BenchmarkReadHeavy_ShardedMap(b *testing.B) {
	m := newShardedMap()
	keys := setupKeys()
	for _, k := range keys {
		m.Put(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			_, _ = m.Get(k)
		}
	})
}

func BenchmarkReadHeavy_MutexMap(b *testing.B) {
	m := newMutexMap()
	keys := setupKeys()
	for _, k := range keys {
		m.Put(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			_, _ = m.Get(k)
		}
	})
}

// --- 2. Write-Heavy (50% Get, 50% Put) Benchmarks ---

func BenchmarkMixedWrite_Unlatch(b *testing.B) {
	m := unlatch.New[string, int]()
	keys := setupKeys()
	for _, k := range keys {
		m.Put(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			if r.Float64() < 0.50 {
				m.Put(k, 100)
			} else {
				_, _ = m.Get(k)
			}
		}
	})
}

func BenchmarkMixedWrite_SyncMap(b *testing.B) {
	var m sync.Map
	keys := setupKeys()
	for _, k := range keys {
		m.Store(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			if r.Float64() < 0.50 {
				m.Store(k, 100)
			} else {
				_, _ = m.Load(k)
			}
		}
	})
}

func BenchmarkMixedWrite_ShardedMap(b *testing.B) {
	m := newShardedMap()
	keys := setupKeys()
	for _, k := range keys {
		m.Put(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			if r.Float64() < 0.50 {
				m.Put(k, 100)
			} else {
				_, _ = m.Get(k)
			}
		}
	})
}

func BenchmarkMixedWrite_MutexMap(b *testing.B) {
	m := newMutexMap()
	keys := setupKeys()
	for _, k := range keys {
		m.Put(k, 42)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			if r.Float64() < 0.50 {
				m.Put(k, 100)
			} else {
				_, _ = m.Get(k)
			}
		}
	})
}

// --- 3. Write-Only (100% Put) Benchmarks ---

func BenchmarkWriteOnly_Unlatch(b *testing.B) {
	m := unlatch.New[string, int]()
	keys := setupKeys()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			m.Put(k, 100)
		}
	})
}

func BenchmarkWriteOnly_SyncMap(b *testing.B) {
	var m sync.Map
	keys := setupKeys()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			m.Store(k, 100)
		}
	})
}

func BenchmarkWriteOnly_ShardedMap(b *testing.B) {
	m := newShardedMap()
	keys := setupKeys()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			m.Put(k, 100)
		}
	})
}

func BenchmarkWriteOnly_MutexMap(b *testing.B) {
	m := newMutexMap()
	keys := setupKeys()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(42))
		for pb.Next() {
			k := keys[r.Intn(numKeys)]
			m.Put(k, 100)
		}
	})
}
