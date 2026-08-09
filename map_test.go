package unlatch_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cuprite-io/unlatch"
)

func TestSequentialBasic(t *testing.T) {
	m := unlatch.New[string, int]()

	// 1. Assert initial state
	if val, ok := m.Get("a"); ok || val != 0 {
		t.Fatalf("Expected key 'a' to not exist, got val=%v, ok=%v", val, ok)
	}
	if size := m.Size(); size != 0 {
		t.Fatalf("Expected size 0, got %v", size)
	}

	// 2. Put elements
	m.Put("a", 1)
	m.Put("b", 2)
	m.Put("c", 3)

	if size := m.Size(); size != 3 {
		t.Fatalf("Expected size 3, got %v", size)
	}

	// 3. Get elements
	tests := []struct {
		key      string
		expected int
		found    bool
	}{
		{"a", 1, true},
		{"b", 2, true},
		{"c", 3, true},
		{"d", 0, false},
	}

	for _, tc := range tests {
		val, ok := m.Get(tc.key)
		if ok != tc.found || val != tc.expected {
			t.Errorf("Get(%q) = (%v, %v); expected (%v, %v)", tc.key, val, ok, tc.expected, tc.found)
		}
	}

	// 4. Update elements
	m.Put("a", 10)
	val, ok := m.Get("a")
	if !ok || val != 10 {
		t.Fatalf("Expected updated value 10 for 'a', got val=%v, ok=%v", val, ok)
	}

	// 5. Delete elements
	val, ok = m.Delete("b")
	if !ok || val != 2 {
		t.Fatalf("Expected deleted value 2 for 'b', got val=%v, ok=%v", val, ok)
	}
	if size := m.Size(); size != 2 {
		t.Fatalf("Expected size 2, got %v", size)
	}

	if val, ok = m.Get("b"); ok {
		t.Fatalf("Expected 'b' to be deleted, but found val=%v", val)
	}

	// Delete non-existent key
	val, ok = m.Delete("non-existent")
	if ok || val != 0 {
		t.Fatalf("Expected delete of non-existent key to fail, got val=%v, ok=%v", val, ok)
	}
}

func TestRange(t *testing.T) {
	m := unlatch.New[string, int]()
	elements := map[string]int{
		"k1": 100,
		"k2": 200,
		"k3": 300,
	}

	for k, v := range elements {
		m.Put(k, v)
	}

	seen := make(map[string]int)
	m.Range(func(key string, value int) bool {
		seen[key] = value
		return true
	})

	if len(seen) != len(elements) {
		t.Fatalf("Range saw %d elements, expected %d", len(seen), len(elements))
	}

	for k, v := range elements {
		if seen[k] != v {
			t.Errorf("Range value mismatch for %s: got %d, expected %d", k, seen[k], v)
		}
	}
}

func TestClear(t *testing.T) {
	m := unlatch.New[string, int]()
	m.Put("a", 1)
	m.Put("b", 2)

	m.Clear()
	if size := m.Size(); size != 0 {
		t.Fatalf("Expected size 0 after Clear(), got %d", size)
	}

	if val, ok := m.Get("a"); ok {
		t.Fatalf("Expected map to be empty after Clear(), got val=%d for 'a'", val)
	}
}

func TestConcurrentReadsWrites(t *testing.T) {
	m := unlatch.New[int, int](unlatch.WithCapacity(16))

	var wg sync.WaitGroup
	numWorkers := 16
	opsPerWorker := 1000

	// Concurrent Writers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := workerID*opsPerWorker + j
				m.Put(key, key*2)
			}
		}(i)
	}

	// Concurrent Readers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				// Get a key that is either not inserted yet, or inserted by workerID
				key := workerID*opsPerWorker + j
				val, ok := m.Get(key)
				if ok && val != key*2 {
					t.Errorf("Get(%d) = %d; expected %d", key, val, key*2)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedSize := numWorkers * opsPerWorker
	if size := m.Size(); size != expectedSize {
		t.Fatalf("Expected final size %d, got %d", expectedSize, size)
	}

	// Verify all elements are present
	for i := 0; i < numWorkers; i++ {
		for j := 0; j < opsPerWorker; j++ {
			key := i*opsPerWorker + j
			val, ok := m.Get(key)
			if !ok || val != key*2 {
				t.Errorf("Missing key %d or wrong value, got val=%v, ok=%v", key, val, ok)
			}
		}
	}
}

func TestConcurrentResizing(t *testing.T) {
	// Start with a small capacity to force multiple resizes during operations
	m := unlatch.New[string, int](unlatch.WithCapacity(8), unlatch.WithLoadFactor(0.50))

	numKeys := 5000

	// 1. Populate initial keys
	for i := 0; i < numKeys; i++ {
		m.Put(fmt.Sprintf("key-%d", i), i)
	}

	var wg sync.WaitGroup

	// Concurrent writers updating keys, causing concurrent updates during resize/read
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numKeys; i++ {
			// Update even keys
			if i%2 == 0 {
				m.Put(fmt.Sprintf("key-%d", i), i*10)
			}
		}
	}()

	// Concurrent readers doing lookups continuously
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%d", i)
			val, ok := m.Get(key)
			if ok {
				if i%2 == 0 {
					if val != i && val != i*10 {
						t.Errorf("Get(%s) = %d; expected %d or %d", key, val, i, i*10)
					}
				} else {
					if val != i {
						t.Errorf("Get(%s) = %d; expected %d", key, val, i)
					}
				}
			}
		}
	}()

	// Concurrent deleters removing keys concurrently (odd keys, so writers don't touch them)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numKeys; i++ {
			if i%2 != 0 && i%5 == 0 {
				m.Delete(fmt.Sprintf("key-%d", i))
			}
		}
	}()

	wg.Wait()

	// Verify that the remaining elements are still intact
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		val, ok := m.Get(key)
		if i%2 != 0 && i%5 == 0 {
			if ok {
				t.Errorf("Expected key %s to be deleted, but found val=%v", key, val)
			}
		} else if i%2 == 0 {
			if !ok || val != i*10 {
				t.Errorf("Expected key %s to exist with value %d, got val=%v, ok=%v", key, i*10, val, ok)
			}
		} else {
			if !ok || val != i {
				t.Errorf("Expected key %s to exist with value %d, got val=%v, ok=%v", key, i, val, ok)
			}
		}
	}
}
