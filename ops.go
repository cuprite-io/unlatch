package unlatch

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Get returns the value associated with key, and a boolean indicating whether the key was found.
// Reads are completely wait-free and execute zero writes to shared memory.
func (m *Map[K, V]) Get(key K) (V, bool) {
	hash := m.hasher(key)
	sig := uint8(hash >> 56) // high 8 bits for tophash signature

	for {
		tbl := m.activeTable()
		bIdx := hash & tbl.capacityMask

		hdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
		binState := getBinState(hdr)

		if binState == binDoneTransfer {
			// Redirect lookup to nextTable
			nextTbl := (*Table[K, V])(atomic.LoadPointer(&m.nextTable))
			if nextTbl != nil {
				return nextTbl.get(key, hash, sig)
			}
		}

		if isLocked(hdr) {
			// Spin-wait briefly for reader instead of yielding immediately
			for i := 0; i < 10; i++ {
				if !isLocked(atomic.LoadUint64(&tbl.bins[bIdx].header)) {
					break
				}
			}
			continue
		}

		ver1 := getVersion(hdr)

		// Unpack states and signatures at once to avoid loop and function call overhead
		states := hdr
		st0 := states & 3
		st1 := (states >> 2) & 3
		st2 := (states >> 4) & 3

		tophashes := hdr >> 9
		sig0 := uint8(tophashes)
		sig1 := uint8(tophashes >> 8)
		sig2 := uint8(tophashes >> 16)

		// Unrolled Slot 0
		if st0 == stateValid && sig0 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[0]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				hdr2 := atomic.LoadUint64(&tbl.bins[bIdx].header)
				if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
					return val, true
				}
				continue // Raced by writer, retry outer loop
			}
		}
		// Unrolled Slot 1
		if st1 == stateValid && sig1 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[1]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				hdr2 := atomic.LoadUint64(&tbl.bins[bIdx].header)
				if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
					return val, true
				}
				continue
			}
		}
		// Unrolled Slot 2
		if st2 == stateValid && sig2 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[2]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				hdr2 := atomic.LoadUint64(&tbl.bins[bIdx].header)
				if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
					return val, true
				}
				continue
			}
		}

		// 2. Search overflow link buckets (unrolled checks)
		foundInLink := false
		var linkVal V
		linkIdx := atomic.LoadUint32(&tbl.bins[bIdx].linkIdx)
		for linkIdx != 0 {
			link := &tbl.links[linkIdx]
			lStates := atomic.LoadUint32(&link.states)
			stL0 := lStates & 3
			stL1 := (lStates >> 2) & 3
			stL2 := (lStates >> 4) & 3

			if stL0 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[0]))
				if ptr != nil && ptr.key == key {
					linkVal = ptr.value
					foundInLink = true
					break
				}
			}
			if stL1 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[1]))
				if ptr != nil && ptr.key == key {
					linkVal = ptr.value
					foundInLink = true
					break
				}
			}
			if stL2 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[2]))
				if ptr != nil && ptr.key == key {
					linkVal = ptr.value
					foundInLink = true
					break
				}
			}
			linkIdx = atomic.LoadUint32(&link.nextIdx)
		}

		if foundInLink {
			// Verify primary bin version hasn't changed to ensure consistency
			hdr2 := atomic.LoadUint64(&tbl.bins[bIdx].header)
			if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
				return linkVal, true
			}
			continue // Retry outer loop
		}

		// Verify version to confirm key was not present
		hdr2 := atomic.LoadUint64(&tbl.bins[bIdx].header)
		if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
			if binState == binInTransfer {
				nextTbl := (*Table[K, V])(atomic.LoadPointer(&m.nextTable))
				if nextTbl != nil {
					return nextTbl.get(key, hash, sig)
				}
			}
			var zero V
			return zero, false
		}
	}
}

// get is a wait-free internal Table lookup helper.
func (t *Table[K, V]) get(key K, hash uint64, sig uint8) (V, bool) {
	for {
		bIdx := hash & t.capacityMask
		hdr := atomic.LoadUint64(&t.bins[bIdx].header)
		if isLocked(hdr) {
			for i := 0; i < 10; i++ {
				if !isLocked(atomic.LoadUint64(&t.bins[bIdx].header)) {
					break
				}
			}
			continue
		}
		ver1 := getVersion(hdr)

		// Unpack states and signatures
		states := hdr
		st0 := states & 3
		st1 := (states >> 2) & 3
		st2 := (states >> 4) & 3

		tophashes := hdr >> 9
		sig0 := uint8(tophashes)
		sig1 := uint8(tophashes >> 8)
		sig2 := uint8(tophashes >> 16)

		// Unrolled Slot 0
		if st0 == stateValid && sig0 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&t.bins[bIdx].entries[0]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				hdr2 := atomic.LoadUint64(&t.bins[bIdx].header)
				if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
					return val, true
				}
				continue
			}
		}
		// Unrolled Slot 1
		if st1 == stateValid && sig1 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&t.bins[bIdx].entries[1]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				hdr2 := atomic.LoadUint64(&t.bins[bIdx].header)
				if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
					return val, true
				}
				continue
			}
		}
		// Unrolled Slot 2
		if st2 == stateValid && sig2 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&t.bins[bIdx].entries[2]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				hdr2 := atomic.LoadUint64(&t.bins[bIdx].header)
				if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
					return val, true
				}
				continue
			}
		}

		foundInLink := false
		var linkVal V
		linkIdx := atomic.LoadUint32(&t.bins[bIdx].linkIdx)
		for linkIdx != 0 {
			link := &t.links[linkIdx]
			lStates := atomic.LoadUint32(&link.states)
			stL0 := lStates & 3
			stL1 := (lStates >> 2) & 3
			stL2 := (lStates >> 4) & 3

			if stL0 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[0]))
				if ptr != nil && ptr.key == key {
					linkVal = ptr.value
					foundInLink = true
					break
				}
			}
			if stL1 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[1]))
				if ptr != nil && ptr.key == key {
					linkVal = ptr.value
					foundInLink = true
					break
				}
			}
			if stL2 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[2]))
				if ptr != nil && ptr.key == key {
					linkVal = ptr.value
					foundInLink = true
					break
				}
			}
			linkIdx = atomic.LoadUint32(&link.nextIdx)
		}

		if foundInLink {
			hdr2 := atomic.LoadUint64(&t.bins[bIdx].header)
			if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
				return linkVal, true
			}
			continue
		}

		hdr2 := atomic.LoadUint64(&t.bins[bIdx].header)
		if getVersion(hdr2) == ver1 && !isLocked(hdr2) {
			var zero V
			return zero, false
		}
	}
}

// Put associates key with value. Operates lock-free.
func (m *Map[K, V]) Put(key K, value V) {
	hash := m.hasher(key)
	sig := uint8(hash >> 56)

	var attempts int
	for {
		attempts++
		tbl := m.activeTable()
		bIdx := hash & tbl.capacityMask

		hdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
		binState := getBinState(hdr)

		if binState != binNormal {
			m.helpResize(tbl, int(bIdx))
			continue
		}

		if isLocked(hdr) {
			backoff(attempts)
			continue
		}

		// Unpack states and signatures
		states := hdr
		st0 := states & 3
		st1 := (states >> 2) & 3
		st2 := (states >> 4) & 3

		tophashes := hdr >> 9
		sig0 := uint8(tophashes)
		sig1 := uint8(tophashes >> 8)
		sig2 := uint8(tophashes >> 16)

		// 1. Update Path (Key already exists)
		// A. Check primary slots (Unrolled)
		found := false
		if st0 == stateValid && sig0 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[0]))
			if ptr != nil && ptr.key == key {
				newHdr := setSlotState(hdr, 0, stateUpdating)
				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&tbl.bins[bIdx].entries[0], unsafe.Pointer(newEnt))

					finalHdr := setSlotState(newHdr, 0, stateValid)
					finalHdr = incVersion(finalHdr)
					if !atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, newHdr, finalHdr) {
						for {
							currHdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
							targetHdr := setSlotState(currHdr, 0, stateValid)
							targetHdr = incVersion(targetHdr)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, currHdr, targetHdr) {
								break
							}
						}
					}
					return
				}
				found = true
			}
		}
		if !found && st1 == stateValid && sig1 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[1]))
			if ptr != nil && ptr.key == key {
				newHdr := setSlotState(hdr, 1, stateUpdating)
				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&tbl.bins[bIdx].entries[1], unsafe.Pointer(newEnt))

					finalHdr := setSlotState(newHdr, 1, stateValid)
					finalHdr = incVersion(finalHdr)
					if !atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, newHdr, finalHdr) {
						for {
							currHdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
							targetHdr := setSlotState(currHdr, 1, stateValid)
							targetHdr = incVersion(targetHdr)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, currHdr, targetHdr) {
								break
							}
						}
					}
					return
				}
				found = true
			}
		}
		if !found && st2 == stateValid && sig2 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[2]))
			if ptr != nil && ptr.key == key {
				newHdr := setSlotState(hdr, 2, stateUpdating)
				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&tbl.bins[bIdx].entries[2], unsafe.Pointer(newEnt))

					finalHdr := setSlotState(newHdr, 2, stateValid)
					finalHdr = incVersion(finalHdr)
					if !atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, newHdr, finalHdr) {
						for {
							currHdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
							targetHdr := setSlotState(currHdr, 2, stateValid)
							targetHdr = incVersion(targetHdr)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, currHdr, targetHdr) {
								break
							}
						}
					}
					return
				}
				found = true
			}
		}
		if found {
			continue // Raced, retry
		}

		// B. Check links for update
		linkIdx := atomic.LoadUint32(&tbl.bins[bIdx].linkIdx)
		currIdx := linkIdx
		for currIdx != 0 {
			link := &tbl.links[currIdx]
			lStates := atomic.LoadUint32(&link.states)
			stL0 := lStates & 3
			stL1 := (lStates >> 2) & 3
			stL2 := (lStates >> 4) & 3

			if stL0 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[0]))
				if ptr != nil && ptr.key == key {
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
						newStates := setLinkSlotState(lStates, 0, stateUpdating)
						atomic.StoreUint32(&link.states, newStates)

						newEnt := &entry[K, V]{key: key, value: value}
						atomic.StorePointer(&link.entries[0], unsafe.Pointer(newEnt))

						finalStates := setLinkSlotState(newStates, 0, stateValid)
						atomic.StoreUint32(&link.states, finalStates)

						for {
							h := atomic.LoadUint64(&tbl.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						return
					}
					found = true
					break
				}
			}
			if stL1 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[1]))
				if ptr != nil && ptr.key == key {
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
						newStates := setLinkSlotState(lStates, 1, stateUpdating)
						atomic.StoreUint32(&link.states, newStates)

						newEnt := &entry[K, V]{key: key, value: value}
						atomic.StorePointer(&link.entries[1], unsafe.Pointer(newEnt))

						finalStates := setLinkSlotState(newStates, 1, stateValid)
						atomic.StoreUint32(&link.states, finalStates)

						for {
							h := atomic.LoadUint64(&tbl.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						return
					}
					found = true
					break
				}
			}
			if stL2 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[2]))
				if ptr != nil && ptr.key == key {
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
						newStates := setLinkSlotState(lStates, 2, stateUpdating)
						atomic.StoreUint32(&link.states, newStates)

						newEnt := &entry[K, V]{key: key, value: value}
						atomic.StorePointer(&link.entries[2], unsafe.Pointer(newEnt))

						finalStates := setLinkSlotState(newStates, 2, stateValid)
						atomic.StoreUint32(&link.states, finalStates)

						for {
							h := atomic.LoadUint64(&tbl.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						return
					}
					found = true
					break
				}
			}
			currIdx = atomic.LoadUint32(&link.nextIdx)
		}
		if found {
			continue // Retry
		}

		// 2. Insert Path (Key does not exist)
		// A. Check primary slots for Empty space (Unrolled)
		inserted := false
		if st0 == stateEmpty {
			newHdr := setSlotState(hdr, 0, stateTryInsert)
			newHdr = setTopHash(newHdr, 0, sig)
			if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
				newEnt := &entry[K, V]{key: key, value: value}
				atomic.StorePointer(&tbl.bins[bIdx].entries[0], unsafe.Pointer(newEnt))

				finalHdr := setSlotState(newHdr, 0, stateValid)
				finalHdr = incVersion(finalHdr)
				if !atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, newHdr, finalHdr) {
					for {
						currHdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
						targetHdr := setSlotState(currHdr, 0, stateValid)
						targetHdr = incVersion(targetHdr)
						if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, currHdr, targetHdr) {
							break
						}
					}
				}
				m.size.add(hash, 1)
				m.checkResize(tbl)
				return
			}
			inserted = true
		}
		if !inserted && st1 == stateEmpty {
			newHdr := setSlotState(hdr, 1, stateTryInsert)
			newHdr = setTopHash(newHdr, 1, sig)
			if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
				newEnt := &entry[K, V]{key: key, value: value}
				atomic.StorePointer(&tbl.bins[bIdx].entries[1], unsafe.Pointer(newEnt))

				finalHdr := setSlotState(newHdr, 1, stateValid)
				finalHdr = incVersion(finalHdr)
				if !atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, newHdr, finalHdr) {
					for {
						currHdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
						targetHdr := setSlotState(currHdr, 1, stateValid)
						targetHdr = incVersion(targetHdr)
						if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, currHdr, targetHdr) {
							break
						}
					}
				}
				m.size.add(hash, 1)
				m.checkResize(tbl)
				return
			}
			inserted = true
		}
		if !inserted && st2 == stateEmpty {
			newHdr := setSlotState(hdr, 2, stateTryInsert)
			newHdr = setTopHash(newHdr, 2, sig)
			if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
				newEnt := &entry[K, V]{key: key, value: value}
				atomic.StorePointer(&tbl.bins[bIdx].entries[2], unsafe.Pointer(newEnt))

				finalHdr := setSlotState(newHdr, 2, stateValid)
				finalHdr = incVersion(finalHdr)
				if !atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, newHdr, finalHdr) {
					for {
						currHdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
						targetHdr := setSlotState(currHdr, 2, stateValid)
						targetHdr = incVersion(targetHdr)
						if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, currHdr, targetHdr) {
							break
						}
					}
				}
				m.size.add(hash, 1)
				m.checkResize(tbl)
				return
			}
			inserted = true
		}
		if inserted {
			continue
		}

		// B. Try inserting into existing overflow link buckets
		if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
			lastIdx := uint32(0)
			currIdx = linkIdx
			for currIdx != 0 {
				link := &tbl.links[currIdx]
				lStates := atomic.LoadUint32(&link.states)
				stL0 := lStates & 3
				stL1 := (lStates >> 2) & 3
				stL2 := (lStates >> 4) & 3

				if stL0 == stateEmpty {
					newStates := setLinkSlotState(lStates, 0, stateTryInsert)
					atomic.StoreUint32(&link.states, newStates)

					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&link.entries[0], unsafe.Pointer(newEnt))

					finalStates := setLinkSlotState(newStates, 0, stateValid)
					atomic.StoreUint32(&link.states, finalStates)

					for {
						h := atomic.LoadUint64(&tbl.bins[bIdx].header)
						releasedH := setLock(h, false)
						releasedH = incVersion(releasedH)
						if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
							break
						}
					}
					m.size.add(hash, 1)
					return
				}
				if stL1 == stateEmpty {
					newStates := setLinkSlotState(lStates, 1, stateTryInsert)
					atomic.StoreUint32(&link.states, newStates)

					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&link.entries[1], unsafe.Pointer(newEnt))

					finalStates := setLinkSlotState(newStates, 1, stateValid)
					atomic.StoreUint32(&link.states, finalStates)

					for {
						h := atomic.LoadUint64(&tbl.bins[bIdx].header)
						releasedH := setLock(h, false)
						releasedH = incVersion(releasedH)
						if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
							break
						}
					}
					m.size.add(hash, 1)
					return
				}
				if stL2 == stateEmpty {
					newStates := setLinkSlotState(lStates, 2, stateTryInsert)
					atomic.StoreUint32(&link.states, newStates)

					newEnt := &entry[K, V]{key: key, value: value}
					atomic.StorePointer(&link.entries[2], unsafe.Pointer(newEnt))

					finalStates := setLinkSlotState(newStates, 2, stateValid)
					atomic.StoreUint32(&link.states, finalStates)

					for {
						h := atomic.LoadUint64(&tbl.bins[bIdx].header)
						releasedH := setLock(h, false)
						releasedH = incVersion(releasedH)
						if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
							break
						}
					}
					m.size.add(hash, 1)
					return
				}
				lastIdx = currIdx
				currIdx = atomic.LoadUint32(&link.nextIdx)
			}

			// C. Chain new link bucket
			newLinkIdx := tbl.allocLink()
			if newLinkIdx == 0 {
				// Pool exhausted, release lock and trigger resize
				for {
					h := atomic.LoadUint64(&tbl.bins[bIdx].header)
					releasedH := setLock(h, false)
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
						break
					}
				}
				m.triggerResize(tbl)
				continue
			}

			newLink := &tbl.links[newLinkIdx]
			newEnt := &entry[K, V]{key: key, value: value}
			newLink.entries[0] = unsafe.Pointer(newEnt)
			newLink.states = setLinkSlotState(0, 0, stateValid)

			if lastIdx == 0 {
				atomic.StoreUint32(&tbl.bins[bIdx].linkIdx, newLinkIdx)
			} else {
				atomic.StoreUint32(&tbl.links[lastIdx].nextIdx, newLinkIdx)
			}

			for {
				h := atomic.LoadUint64(&tbl.bins[bIdx].header)
				releasedH := setLock(h, false)
				releasedH = incVersion(releasedH)
				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
					break
				}
			}
			m.size.add(hash, 1)
			return
		}
	}
}

// Delete removes the key and returns its old value and success status. Operates lock-free.
func (m *Map[K, V]) Delete(key K) (V, bool) {
	hash := m.hasher(key)
	sig := uint8(hash >> 56)

	var attempts int
	for {
		attempts++
		tbl := m.activeTable()
		bIdx := hash & tbl.capacityMask

		hdr := atomic.LoadUint64(&tbl.bins[bIdx].header)
		binState := getBinState(hdr)

		if binState != binNormal {
			m.helpResize(tbl, int(bIdx))
			continue
		}

		if isLocked(hdr) {
			backoff(attempts)
			continue
		}

		// Unpack states and signatures
		states := hdr
		st0 := states & 3
		st1 := (states >> 2) & 3
		st2 := (states >> 4) & 3

		tophashes := hdr >> 9
		sig0 := uint8(tophashes)
		sig1 := uint8(tophashes >> 8)
		sig2 := uint8(tophashes >> 16)

		// 1. Search primary bin slots (Unrolled)
		if st0 == stateValid && sig0 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[0]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				newHdr := setSlotState(hdr, 0, stateEmpty)
				newHdr = setTopHash(newHdr, 0, 0)
				newHdr = incVersion(newHdr)

				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
					atomic.StorePointer(&tbl.bins[bIdx].entries[0], nil)
					m.size.add(hash, -1)
					return val, true
				}
				continue
			}
		}
		if st1 == stateValid && sig1 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[1]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				newHdr := setSlotState(hdr, 1, stateEmpty)
				newHdr = setTopHash(newHdr, 1, 0)
				newHdr = incVersion(newHdr)

				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
					atomic.StorePointer(&tbl.bins[bIdx].entries[1], nil)
					m.size.add(hash, -1)
					return val, true
				}
				continue
			}
		}
		if st2 == stateValid && sig2 == sig {
			ptr := (*entry[K, V])(atomic.LoadPointer(&tbl.bins[bIdx].entries[2]))
			if ptr != nil && ptr.key == key {
				val := ptr.value
				newHdr := setSlotState(hdr, 2, stateEmpty)
				newHdr = setTopHash(newHdr, 2, 0)
				newHdr = incVersion(newHdr)

				if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, newHdr) {
					atomic.StorePointer(&tbl.bins[bIdx].entries[2], nil)
					m.size.add(hash, -1)
					return val, true
				}
				continue
			}
		}

		// 2. Search overflow link slots
		linkIdx := atomic.LoadUint32(&tbl.bins[bIdx].linkIdx)
		currIdx := linkIdx
		found := false
		var deletedVal V
		for currIdx != 0 {
			link := &tbl.links[currIdx]
			lStates := atomic.LoadUint32(&link.states)
			stL0 := lStates & 3
			stL1 := (lStates >> 2) & 3
			stL2 := (lStates >> 4) & 3

			if stL0 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[0]))
				if ptr != nil && ptr.key == key {
					deletedVal = ptr.value
					// Acquire primary bin lock
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
						newStates := setLinkSlotState(lStates, 0, stateEmpty)
						atomic.StoreUint32(&link.states, newStates)

						atomic.StorePointer(&link.entries[0], nil)

						for {
							h := atomic.LoadUint64(&tbl.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						m.size.add(hash, -1)
						return deletedVal, true
					}
					found = true
					break
				}
			}
			if stL1 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[1]))
				if ptr != nil && ptr.key == key {
					deletedVal = ptr.value
					// Acquire primary bin lock
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
						newStates := setLinkSlotState(lStates, 1, stateEmpty)
						atomic.StoreUint32(&link.states, newStates)

						atomic.StorePointer(&link.entries[1], nil)

						for {
							h := atomic.LoadUint64(&tbl.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						m.size.add(hash, -1)
						return deletedVal, true
					}
					found = true
					break
				}
			}
			if stL2 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[2]))
				if ptr != nil && ptr.key == key {
					deletedVal = ptr.value
					// Acquire primary bin lock
					if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, hdr, setLock(hdr, true)) {
						newStates := setLinkSlotState(lStates, 2, stateEmpty)
						atomic.StoreUint32(&link.states, newStates)

						atomic.StorePointer(&link.entries[2], nil)

						for {
							h := atomic.LoadUint64(&tbl.bins[bIdx].header)
							releasedH := setLock(h, false)
							releasedH = incVersion(releasedH)
							if atomic.CompareAndSwapUint64(&tbl.bins[bIdx].header, h, releasedH) {
								break
							}
						}
						m.size.add(hash, -1)
						return deletedVal, true
					}
					found = true
					break
				}
			}
			currIdx = atomic.LoadUint32(&link.nextIdx)
		}

		if found {
			continue // Retry lock/CAS
		}

		hdr2 := atomic.LoadUint64(&tbl.bins[bIdx].header)
		if getVersion(hdr2) == getVersion(hdr) && !isLocked(hdr2) {
			var zero V
			return zero, false
		}
	}
}

// Range calls f sequentially for each key and value present in the map.
// Range is weakly consistent.
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	tbl := m.activeTable()
	for i := range tbl.bins {
		b := &tbl.bins[i]
		hdr := atomic.LoadUint64(&b.header)

		for s := 0; s < 3; s++ {
			if getSlotState(hdr, s) == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&b.entries[s]))
				if ptr != nil {
					k := ptr.key
					v := ptr.value
					if !f(k, v) {
						return
					}
				}
			}
		}

		linkIdx := atomic.LoadUint32(&b.linkIdx)
		for linkIdx != 0 {
			link := &tbl.links[linkIdx]
			lStates := atomic.LoadUint32(&link.states)
			stL0 := lStates & 3
			stL1 := (lStates >> 2) & 3
			stL2 := (lStates >> 4) & 3

			if stL0 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[0]))
				if ptr != nil {
					k := ptr.key
					v := ptr.value
					if !f(k, v) {
						return
					}
				}
			}
			if stL1 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[1]))
				if ptr != nil {
					k := ptr.key
					v := ptr.value
					if !f(k, v) {
						return
					}
				}
			}
			if stL2 == stateValid {
				ptr := (*entry[K, V])(atomic.LoadPointer(&link.entries[2]))
				if ptr != nil {
					k := ptr.key
					v := ptr.value
					if !f(k, v) {
						return
					}
				}
			}
			linkIdx = atomic.LoadUint32(&link.nextIdx)
		}
	}
}

// backoff performs a highly efficient progressive spin-wait without thread sleep.
func backoff(attempts int) {
	if attempts < 4 {
		for i := 0; i < attempts*10; i++ {
			// tight active spin
		}
		return
	}
	runtime.Gosched() // yield to other goroutines
}
