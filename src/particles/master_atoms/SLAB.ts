import { ATOM_CLASS, ATOM_KEY, BC, GATE_ELECTRON, REGISTRY_PROTON, SEALED_ERROR_ELECTRON, SUBSCRIPTION_ELECTRON } from "../contract_factory/CONTROL_ROOM";


const pow2 = (n: N) => n > 0 && (n & (n - 1)) === 0;

export class SLAB_ATOM<X = U> {
    public key: ATOM_KEY;
  public registry: REGISTRY_PROTON | null = null;

	readonly capacity: number;
	
	readonly values: Float64Array;
	readonly versions: Uint32Array;
	readonly nextFree: Int32Array;
	readonly refCount: Int32Array;
	
	private freeHead = -1;
	private allocated = 0;
	
	readonly slab: Uint8Array;
	private slabTail = 0;
	
	private readonly slabOffsets: Int32Array;
	private readonly slabLengths: Int32Array;
	
	// Track which slots are currently in use (for debugging/validation)
	private readonly inUse: Uint8Array;
		constructor(key: ATOM_KEY, value: X, capacity: number, slabCapacity = 1 << 20) {
	  if (!pow2(capacity)) throw new Error("slotCapacity must be a power of two");
	      this.key = key;
    
	  this.capacity = capacity;
	  
	  this.values = new Float64Array(capacity);
	  this.versions = new Uint32Array(capacity);
	  this.nextFree = new Int32Array(capacity);
	  this.refCount = new Int32Array(capacity);
	  this.inUse = new Uint8Array(capacity);
	  
	  this.nextFree.fill(-1);
	  
	  // Slot 0 is reserved as the control slot (version counter)
	  this.allocated = 1;
	  this.refCount[0] = 1;
	  this.versions[0] = 0;
	  this.inUse[0] = 1;
	  
	  // Initialize slab-related arrays
	  this.slabOffsets = new Int32Array(capacity).fill(-1);
	  this.slabLengths = new Int32Array(capacity).fill(0);
	  
	  this.slab = new Uint8Array(slabCapacity);
	  this.slabTail = 0;
	  
	  // Build initial free list (skip slot 0)
	  // Link slots 1..capacity-1 in reverse order so we pop from the front
	  for (let i = capacity - 1; i >= 1; i--) {
		this.nextFree[i] = this.freeHead;
		this.freeHead = i;
	  }
	}	
	writeBinary(slot: N, data: U8, epoch: N): V {
		this.assertLive(slot);
		this.assertEpoch(epoch);
	
		const oldOffset = this.slabOffsets[slot];
		const oldLength = this.slabLengths[slot];
		const length = data.length;
	
		// Reuse the existing allocation when it is large enough.
		if (oldOffset >= 0 && length <= oldLength) {
		  this.slab.set(data, oldOffset);
	
		  if (length < oldLength) {
			this.freeBytes(
			  oldOffset + length,
			  oldLength - length
			);
		  }
	
		  this.slabOffsets[slot] = length === 0 ? 0 : oldOffset;
		  this.slabLengths[slot] = length;
		  this.values[slot] = 0;
		  this.versions[slot] = epoch;
		  return;
		}
	
		// Allocate first: exhaustion leaves the old payload intact.
		const offset = this.allocBytes(length);
	
		this.slab.set(data, offset);
	
		if (oldOffset >= 0 && oldLength > 0) {
		  this.freeBytes(oldOffset, oldLength);
		}
	
		this.slabOffsets[slot] = offset;
		this.slabLengths[slot] = length;
		this.values[slot] = 0;
		this.versions[slot] = epoch;
	  }  
	  readBinary(slot: N): U8 {
		const { offset, length } = this.ref(slot);
		return this.slab.subarray(offset, offset + length);
	  }  
	  private allocBytes(length: N): N {
		if (length === 0) return 0;
	
		// First-fit allocation from reclaimed ranges.
		for (let i = 0; i < this.freeRanges.length; i++) {
		  const range = this.freeRanges[i];
	
		  if (range.length < length) continue;
	
		  const offset = range.offset;
	
		  if (range.length === length) {
			this.freeRanges.splice(i, 1);
		  } else {
			range.offset += length;
			range.length -= length;
		  }
	
		  return offset;
		}
	
		if (length > this.slab.length - this.slabTail) {
		  throw new RangeError(
			"STORAGE_RING: no contiguous slab range large enough"
		  );
		}
	
		const offset = this.slabTail;
		this.slabTail += length;
		return offset;
	  }  
	  private freeBytes(offset: N, length: N): V {
		if (length === 0) return;
	
		const ranges = this.freeRanges;
		let index = 0;
	
		while (
		  index < ranges.length &&
		  ranges[index].offset < offset
		) {
		  index++;
		}
	
		ranges.splice(index, 0, { offset, length });
	
		// Merge with the preceding range.
		if (
		  index > 0 &&
		  ranges[index - 1].offset +
			ranges[index - 1].length === offset
		) {
		  ranges[index - 1].length += length;
		  ranges.splice(index, 1);
		  index--;
		}
	
		// Merge with the following range.
		if (
		  index + 1 < ranges.length &&
		  ranges[index].offset + ranges[index].length ===
			ranges[index + 1].offset
		) {
		  ranges[index].length += ranges[index + 1].length;
		  ranges.splice(index + 1, 1);
		}
	
		// Return a free suffix to the unused slab tail.
		const last = ranges[ranges.length - 1];
	
		if (
		  last !== undefined &&
		  last.offset + last.length === this.slabTail
		) {
		  this.slabTail = last.offset;
		  ranges.pop();
		}
	  }  
	  private assertLive(slot: N): V {
		if (
		  !Number.isInteger(slot) ||
		  slot <= 0 ||
		  slot >= this.allocated ||
		  this.refCount[slot] <= 0
		) {
		  throw new RangeError(
			`STORAGE_RING: invalid or released slot ${slot}`
		  );
		}
	  }  
	  private assertEpoch(epoch: N): V {
		if (
		  !Number.isInteger(epoch) ||
		  epoch < 0 ||
		  epoch > 0xffffffff
		) {
		  throw new RangeError(
			"STORAGE_RING: epoch must be a uint32"
		  );
		}
	  }
	tick(): number { return this.versions[0];	}	
	advance(): number {	  return ++this.versions[0];	}
		/**
	 * Allocate a new slot from the free list.
	 * Returns the slot index, or -1 if no slots are available.
	 */
	alloc(value = 0): number {
	  const slot = this.freeHead;
	  if (slot === -1) return -1; // No free slots
	  
	  // Remove from free list
	  this.freeHead = this.nextFree[slot];
	  this.nextFree[slot] = -1;
	  
	  // Initialize slot
	  this.values[slot] = value;
	  this.refCount[slot] = 1; // Initial reference
	  this.inUse[slot] = 1;
	  
	  // Clear slab allocation for this slot
	  this.slabOffsets[slot] = -1;
	  this.slabLengths[slot] = 0;
	  
	  this.allocated++;
	  
	  return slot;
	}
		/**
	 * Increment the reference count for a slot.
	 * Returns true if successful, false if the slot is not allocated.
	 */
	retain(slot: number): boolean {
	  if (slot < 0 || slot >= this.capacity) return false;
	  if (!this.inUse[slot]) return false;
	  
	  if (this.refCount[slot] === 0x7FFFFFFF) {
		throw new Error("Reference count overflow");
	  }
	  
	  this.refCount[slot]++;
	  return true;
	}
		/**
	 * Decrement the reference count for a slot.
	 * If the ref count reaches zero, the slot is freed.
	 * Returns true if the slot was freed, false otherwise.
	 */
	release(slot: number): boolean {
	  if (slot < 0 || slot >= this.capacity) return false;
	  if (!this.inUse[slot]) return false;
	  
	  if (this.refCount[slot] <= 0) {
		throw new Error("Reference count underflow");
	  }
	  
	  this.refCount[slot]--;
	  
	  if (this.refCount[slot] === 0) {
		// Free the slot
		this.inUse[slot] = 0;
		
		// Clear slab data
		this.slabOffsets[slot] = -1;
		this.slabLengths[slot] = 0;
		this.values[slot] = 0;
		
		// Add to free list
		this.nextFree[slot] = this.freeHead;
		this.freeHead = slot;
		
		this.allocated--;
		
		return true;
	  }
	  
	  return false;
	}
		/**
	 * Write a numeric value into a slot with epoch validation.
	 * Returns true if successful, false if the slot is invalid or epoch mismatch.
	 */
	write(slot: number, value: number, epoch: number): boolean {
	  if (slot < 0 || slot >= this.capacity) return false;
	  if (!this.inUse[slot]) return false;
	  if (this.refCount[slot] <= 0) return false;
	  
	  // Epoch validation: the slot's version must match the expected epoch
	  if (this.versions[slot] !== epoch) return false;
	  
	  this.values[slot] = value;
	  return true;
	}
		/**
	 * Read the numeric value from a slot.
	 * Returns NaN if the slot is invalid.
	 */
	read(slot: number): number {
	  if (slot < 0 || slot >= this.capacity) return NaN;
	  if (!this.inUse[slot]) return NaN;
	  
	  return this.values[slot];
	}
		/**
	 * Get the version number for a slot.
	 * Returns -1 if the slot is invalid.
	 */
	version(slot: number): number {
	  if (slot < 0 || slot >= this.capacity) return -1;
	  
	  return this.versions[slot];
	}
		/**
	 * Get the slab reference (offset and length) for a slot.
	 * Returns { offset: -1, length: 0 } if no slab data is associated.
	 */
	ref(slot: number): { offset: number; length: number } {
	  if (slot < 0 || slot >= this.capacity) {
		return { offset: -1, length: 0 };
	  }
	  
	  return {
		offset: this.slabOffsets[slot],
		length: this.slabLengths[slot],
	  };
	}
		// ============================================================
	// Additional methods for slab storage functionality
	// ============================================================
		/**
	 * Store variable-sized binary data in the slab for a given slot.
	 * Returns true if successful, false if insufficient slab space.
	 */
	storeSlab(slot: number, data: Uint8Array | ArrayBuffer): boolean {
	  if (slot < 0 || slot >= this.capacity) return false;
	  if (!this.inUse[slot]) return false;
	  
	  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
	  const len = bytes.length;
	  
	  if (len === 0) {
		// Zero-length: clear slab assignment
		this.slabOffsets[slot] = -1;
		this.slabLengths[slot] = 0;
		return true;
	  }
	  
	  // Simple bump allocator: check if we have room
	  if (this.slabTail + len > this.slab.length) {
		// Could implement compaction here, but for now just fail
		return false;
	  }
	  
	  const offset = this.slabTail;
	  this.slab.set(bytes, offset);
	  this.slabTail += len;
	  
	  this.slabOffsets[slot] = offset;
	  this.slabLengths[slot] = len;
	  
	  return true;
	}
		/**
	 * Read the variable-sized binary data associated with a slot.
	 * Returns a subarray view (no copy) or null if no data.
	 */
	loadSlab(slot: number): Uint8Array | null {
	  if (slot < 0 || slot >= this.capacity) return null;
	  if (!this.inUse[slot]) return null;
	  
	  const offset = this.slabOffsets[slot];
	  const length = this.slabLengths[slot];
	  
	  if (offset < 0 || length <= 0) return null;
	  
	  return this.slab.subarray(offset, offset + length);
	}
	private growthSlab(minAdditional: number): void {
		const newCapacity = Math.max(this.slab.length * 2, this.slabTail + minAdditional);
		const newSlab = new Uint8Array(newCapacity);
		newSlab.set(this.slab);
		this.slab = newSlab;
	}
		/**
	 * Check if the slab buffer needs compaction.
	 * Returns true if compaction would reclaim significant space.
	 */
	needsCompaction(): boolean {
	  if (this.allocated <= 1) return false; // Only control slot
	  
	  // Count live slab bytes
	  let liveBytes = 0;
	  for (let i = 1; i < this.capacity; i++) {
		if (this.inUse[i] && this.slabLengths[i] > 0) {
		  liveBytes += this.slabLengths[i];
		}
	  }
	  
	  // Compact if more than 50% of slab is dead space
	  const deadBytes = this.slabTail - liveBytes;
	  return deadBytes > this.slabTail * 0.5;
	}
		/**
	 * Compact the slab buffer, removing dead space.
	 * Invalidates any subarray views previously returned by loadSlab.
	 */
	compact(): void {
	  if (!this.needsCompaction()) return;
	  
	  const newSlab = new Uint8Array(this.slab.length);
	  let newTail = 0;
	  
	  for (let i = 1; i < this.capacity; i++) {
		if (!this.inUse[i]) continue;
		
		const length = this.slabLengths[i];
		if (length <= 0) continue;
		
		const oldOffset = this.slabOffsets[i];
		newSlab.set(
		  this.slab.subarray(oldOffset, oldOffset + length),
		  newTail
		);
		
		this.slabOffsets[i] = newTail;
		newTail += length;
	  }
	  
	  // Copy back into original slab buffer
	  this.slab.set(newSlab.subarray(0, newTail), 0);
	  this.slabTail = newTail;
	}
		/**
	 * Get statistics about the storage ring.
	 */
	stats(): {
	  capacity: number;
	  allocated: number;
	  freeSlots: number;
	  slabUsed: number;
	  slabCapacity: number;
	  slabUtilization: number;
	} {
	  return {
		capacity: this.capacity,
		allocated: this.allocated,
		freeSlots: this.capacity - this.allocated,
		slabUsed: this.slabTail,
		slabCapacity: this.slab.length,
		slabUtilization: this.slabTail / this.slab.length,
	  };
	}
		/**
	 * Validate internal consistency (for debugging).
	 * Returns an array of error strings, empty if consistent.
	 */
	validate(): string[] {
	  const errors: string[] = [];
	  
	  // Count free list
	  let freeCount = 0;
	  let current = this.freeHead;
	  const seen = new Set<number>();
	  
	  while (current !== -1) {
		if (current < 1 || current >= this.capacity) {
		  errors.push(`Free list contains invalid slot: ${current}`);
		  break;
		}
		if (seen.has(current)) {
		  errors.push(`Free list contains cycle at slot: ${current}`);
		  break;
		}
		seen.add(current);
		freeCount++;
		current = this.nextFree[current];
	  }
	  
	  if (freeCount !== this.capacity - this.allocated) {
		errors.push(
		  `Free list has ${freeCount} entries, expected ${this.capacity - this.allocated}`
		);
	  }
	  
	  // Validate slab offsets
	  for (let i = 1; i < this.capacity; i++) {
		if (!this.inUse[i]) continue;
		
		const offset = this.slabOffsets[i];
		const length = this.slabLengths[i];
		
		if (offset === -1 && length !== 0) {
		  errors.push(`Slot ${i}: offset -1 but length ${length}`);
		} else if (offset !== -1) {
		  if (offset < 0 || offset + length > this.slabTail) {
			errors.push(`Slot ${i}: slab range [${offset}, ${offset + length}) out of bounds (tail=${this.slabTail})`);
		  }
		}
	  }
	  
	  return errors;
	}
	get used(): N { return this.allocated - 1 - this.freeCount(); }
	get freeCount(): N { return this.freeHead === FREE_LIST_END ? 0 : this.walkFree(); }
	get slabUsed(): N { return this.liveBytes; }
	get slabFree(): N { return this.slab.length - this.liveBytes; }
	private walkFree(): N {
		let n = 0;
		let p = this.freeHead;
		while (p !== FREE_LIST_END) { n++; p = this.nextFree[p]; }
		return n;
	}
  }
