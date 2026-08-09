package unlatch

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// checkResize checks if the load factor of the map has exceeded the threshold.
// If it has, it triggers a resize. Called lazily after successful inserts.
func (m *Map[K, V]) checkResize(tbl *Table[K, V]) {
	numBins := len(tbl.bins)
	// Each bin has 3 slots. Max occupancy = numBins * 3
	maxOccupancy := float64(numBins * 3)
	currentSize := float64(m.Size())
	if currentSize/maxOccupancy > m.maxLoadFactor {
		m.triggerResize(tbl)
	}
}

// triggerResize initiates the resizing process if no other thread has done so.
func (m *Map[K, V]) triggerResize(oldTbl *Table[K, V]) {
	if atomic.CompareAndSwapUint32(&m.resizeInProgress, 0, 1) {
		// We are the initiator!
		oldCap := len(oldTbl.bins)
		newCap := oldCap * 2
		newTbl := newTable[K, V](newCap)

		atomic.StorePointer(&m.nextTable, unsafe.Pointer(newTbl))

		m.migrateTable(oldTbl, newTbl)
	} else {
		// Help migrate if nextTable is ready
		nextPtr := atomic.LoadPointer(&m.nextTable)
		if nextPtr != nil {
			m.migrateTable(oldTbl, (*Table[K, V])(nextPtr))
		}
	}
}

// helpResize is called by writers when they encounter a migrating bin.
func (m *Map[K, V]) helpResize(oldTbl *Table[K, V], bIdx int) {
	nextPtr := atomic.LoadPointer(&m.nextTable)
	if nextPtr != nil {
		m.migrateTable(oldTbl, (*Table[K, V])(nextPtr))
	}
}

// migrateTable copies all bins from oldTbl to newTbl in strides of 64 bins.
func (m *Map[K, V]) migrateTable(oldTbl, newTbl *Table[K, V]) {
	oldCap := int64(len(oldTbl.bins))

	for {
		// Claim stride from oldTbl
		start := atomic.AddInt64(&oldTbl.resizeIdx, 64) - 64
		if start >= oldCap {
			// All strides claimed, wait for completion of the entire migration of oldTbl
			for atomic.LoadInt64(&oldTbl.resizeCount) < oldCap {
				runtime.Gosched()
			}
			return
		}

		end := start + 64
		if end > oldCap {
			end = oldCap
		}

		for i := start; i < end; i++ {
			m.migrateBin(oldTbl, newTbl, int(i))
		}
	}
}

// migrateBin migrates a single bin i from oldTbl to newTbl.
func (m *Map[K, V]) migrateBin(oldTbl, newTbl *Table[K, V], i int) {
	for {
		hdr := atomic.LoadUint64(&oldTbl.bins[i].header)
		state := getBinState(hdr)

		if state == binDoneTransfer {
			return // already migrated
		}
		if state == binInTransfer {
			// another thread is migrating, spin until it is done
			for getBinState(atomic.LoadUint64(&oldTbl.bins[i].header)) != binDoneTransfer {
				runtime.Gosched()
			}
			return
		}

		if isLocked(hdr) {
			runtime.Gosched()
			continue
		}

		// Wait for any pending writes to commit (TryInsert or Updating states)
		hasPending := false
		for s := 0; s < 3; s++ {
			st := getSlotState(hdr, s)
			if st == stateTryInsert || st == stateUpdating {
				hasPending = true
				break
			}
		}
		if hasPending {
			runtime.Gosched()
			continue
		}

		// Attempt to CAS bin state to binInTransfer
		newHdr := setBinState(hdr, binInTransfer)
		if atomic.CompareAndSwapUint64(&oldTbl.bins[i].header, hdr, newHdr) {
			// We own the migration of this bin!
			// 1. Copy valid elements from primary bin
			for s := 0; s < 3; s++ {
				if getSlotState(hdr, s) == stateValid {
					ptr := (*entry[K, V])(atomic.LoadPointer(&oldTbl.bins[i].entries[s]))
					if ptr != nil {
						key := ptr.key
						val := ptr.value
						hash := m.hasher(key)
						sig := uint8(hash >> 56)
						newTbl.insertMigrated(key, val, hash, sig)
					}
				}
			}

			// 2. Copy valid elements from overflow link chain
			linkIdx := atomic.LoadUint32(&oldTbl.bins[i].linkIdx)
			for linkIdx != 0 {
				link := &oldTbl.links[linkIdx]
				states := atomic.LoadUint32(&link.states)
				for s := 0; s < 3; s++ {
					if getLinkSlotState(states, s) == stateValid {
						ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[s]))
						if ptr != nil {
							key := ptr.key
							val := ptr.value
							hash := m.hasher(key)
							sig := uint8(hash >> 56)
							newTbl.insertMigrated(key, val, hash, sig)
						}
					}
				}
				linkIdx = atomic.LoadUint32(&link.nextIdx)
			}

			// Mark bin as DoneTransfer in old table
			for {
				h := atomic.LoadUint64(&oldTbl.bins[i].header)
				doneH := setBinState(h, binDoneTransfer)
				if atomic.CompareAndSwapUint64(&oldTbl.bins[i].header, h, doneH) {
					break
				}
			}

			// Increment global bin migration count on oldTbl
			oldCap := int64(len(oldTbl.bins))
			count := atomic.AddInt64(&oldTbl.resizeCount, 1)
			if count == oldCap {
				// We are the final thread to complete! Publish the new table.
				atomic.StorePointer(&m.table, unsafe.Pointer(newTbl))
				atomic.StorePointer(&m.nextTable, nil)
				atomic.StoreUint32(&m.resizeInProgress, 0)
			}
			return
		}
	}
}

// insertMigrated is a simplified, lock-free insert optimized for resizing destinations.
func (t *Table[K, V]) insertMigrated(key K, value V, hash uint64, sig uint8) {
	var attempts int
	for {
		attempts++
		bIdx := hash & t.capacityMask
		hdr := atomic.LoadUint64(&t.bins[bIdx].header)
		if isLocked(hdr) {
			backoff(attempts)
			continue
		}

		// Check primary slots
		inserted := false
		for i := 0; i < 3; i++ {
			if getSlotState(hdr, i) == stateEmpty {
				newHdr := setSlotState(hdr, i, stateTryInsert)
				newHdr = setTopHash(newHdr, i, sig)
				if atomic.CompareAndSwapUint64(&t.bins[bIdx].header, hdr, newHdr) {
					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&t.bins[bIdx].entries[i], unsafe.Pointer(newEnt))

					finalHdr := setSlotState(newHdr, i, stateValid)
					finalHdr = incVersion(finalHdr)
					if !atomic.CompareAndSwapUint64(&t.bins[bIdx].header, newHdr, finalHdr) {
						// If CAS fails (due to other slot updates), retry setting valid in loop
						for {
							currHdr := atomic.LoadUint64(&t.bins[bIdx].header)
							targetHdr := setSlotState(currHdr, i, stateValid)
							targetHdr = incVersion(targetHdr)
							if atomic.CompareAndSwapUint64(&t.bins[bIdx].header, currHdr, targetHdr) {
								break
							}
						}
					}
					return
				}
				inserted = true
				break
			}
		}
		if inserted {
			continue
		}

		// Try links
		if atomic.CompareAndSwapUint64(&t.bins[bIdx].header, hdr, setLock(hdr, true)) {
			linkIdx := atomic.LoadUint32(&t.bins[bIdx].linkIdx)
			lastIdx := uint32(0)
			currIdx := linkIdx
			for currIdx != 0 {
				link := &t.links[currIdx]
				states := atomic.LoadUint32(&link.states)
				for i := 0; i < 3; i++ {
					if getLinkSlotState(states, i) == stateEmpty {
						newStates := setLinkSlotState(states, i, stateTryInsert)
						atomic.StoreUint32(&link.states, newStates)

						newEnt := &entry[K, V]{key: key, value: value}
						atomic.StorePointer(&link.entries[i], unsafe.Pointer(newEnt))

						finalStates := setLinkSlotState(newStates, i, stateValid)
						atomic.StoreUint32(&link.states, finalStates)

						for {
							h := atomic.LoadUint64(&t.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&t.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						return
					}
				}
				lastIdx = currIdx
				currIdx = atomic.LoadUint32(&link.nextIdx)
			}

			// Allocate new link
			newLinkIdx := t.allocLink()
			if newLinkIdx == 0 {
				for {
					h := atomic.LoadUint64(&t.bins[bIdx].header)
					releasedH := setLock(h, false)
					if atomic.CompareAndSwapUint64(&t.bins[bIdx].header, h, releasedH) {
						break
					}
				}
				runtime.Gosched()
				continue
			}

			newLink := &t.links[newLinkIdx]
			newEnt := &entry[K, V]{key: key, value: value}
			newLink.entries[0] = unsafe.Pointer(newEnt)
			newLink.states = setLinkSlotState(0, 0, stateValid)

			if lastIdx == 0 {
				atomic.StoreUint32(&t.bins[bIdx].linkIdx, newLinkIdx)
			} else {
				atomic.StoreUint32(&t.links[lastIdx].nextIdx, newLinkIdx)
			}

			for {
				h := atomic.LoadUint64(&t.bins[bIdx].header)
				releasedH := setLock(h, false)
				releasedH = incVersion(releasedH)
				if atomic.CompareAndSwapUint64(&t.bins[bIdx].header, h, releasedH) {
					break
				}
			}
			return
		}
	}
}
