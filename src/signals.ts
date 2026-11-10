//                                                                                                                      IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Registry } from "@the-w1zard/the-registry"
import { Mesh } from "@the-w1zard/the-mesh"
import { useState, useEffect } from "@the-w1zard/the-dark-side"
import { Queue } from "./comps/queue";

// #endregion
//                                                                                                                       TYPPES
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const NO_SUB = -1;
const NO_MSG = -1;
type Handler = (payload: unknown) => unknown | Promise<unknown>;
function pow2(value: number): boolean {
	return value > 0 && (value & (value - 1)) === 0;
}
interface Deferred {
    resolve: (value: unknown) => void;
    reject: (reason?: unknown) => void;
}
interface HADRON_COLLIDER_OPTIONS {
	ringCapacity?: N;
	slotCapacity?: N;
	pidCapacity?: N;
	maxBatch?: N;
	soaCapacity?: N;
	f64Capacity?: N;
	u32Capacity?: N;
	i32Capacity?: N;
	byteCapacity?: N;
	shared?: A;
	capacity?: A;
}
const enum RKind {
	NONE = 0,
	SIGNAL = 1,
	COMPUTED = 2,
	EFFECT = 3,
	EFFECT_SCOPE = 4,
}
const TRACER_PID = 0;
const OP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	COMPACT: 4,
	NONE: 5,
	EDIT: 6,
	COMPUTED: 7,
	EFFECTSCOPE: 8,
	EXTERNAL: 9,
	OBSERVER: 10,
	ASYNCEFFECT: 11,
	SEND: 12,
	SUB: 13,
	DUPLEX: 14,
	STREAM: 15,
	DRC: 16,
} as const;
const RX_Flags = {
	NONE: 0,
	SIGNAL: 1 << 0,
	COMPUTED: 1 << 1,
	EFFECT: 1 << 2,
	DIRTY: 1 << 3,
	RUNNING: 1 << 4,
	DISPOSED: 1 << 5,
	MUTABLE: 1 << 6,
	WATCHING: 1 << 7,
	RECURSEDCHECK: 1 << 8,
	RECURSED: 1 << 9,
	PENDING: 1 << 10,
} as const;
type Pid = number;
type Lid = number;
const ReactiveFlags = {
    NONE: 0,
    SIGNAL: 1 << 0,
    COMPUTED: 1 << 1,
    EFFECT: 1 << 2,
    DIRTY: 1 << 3,
    RUNNING: 1 << 4,
    DISPOSED: 1 << 5,
    MUTABLE: 1 << 6,
    WATCHING: 1 << 7,
    RECURSEDCHECK: 1 <<8,
    RECURSED: 1 <<9,
    PENDING: 1 <<10,
} as const;
const NO_LINK = -1;
const NO_PID = -1;
const HOOK_KIND = {
    NONE: 0,
    STATE: 1,
    REDUCER: 2,
    REF: 3,
    MEMO: 4,
    EFFECT: 5,
    CONTEXT: 6,
} as const;
const HOOK_FLAG = {
    NONE: 0,
    DIRTY: 1 << 0,
    RUNNING: 1 << 2,
    MOUNTED: 1 << 3,
    DISPOSED: 1 << 4,
    RENDER_PENDING: 1 << 5,
} as const;
type RenderFn = () => void;
type CleanupFn = () => void;
type EffectFn = () => void | CleanupFn;
let Bun: A = null
let Deno: A = null
if (typeof (globalThis as A).Deno !== 'undefined') { Deno = (globalThis as A).Deno; }
if (typeof (globalThis as A).Bun !== 'undefined') { Bun = (globalThis as A).Bun; }
export const hasBun = typeof (globalThis as A).Bun !== 'undefined';
export const hasDeno = typeof (globalThis as A).Deno !== 'undefined';
export const hasNode = typeof process !== 'undefined' && !!process.versions?.node;
export const isBrowser = typeof window !== 'undefined';
export const isVSCode = typeof process !== 'undefined' && process.env.VSCODE_PID;
export let _localChange = false
export let _channel: BroadcastChannel | null = null



export let isReact = false
export let IS_THE_WIZARD = true
let init_hooks_context = false

// #endregion
//                                                                                                                [EXOGUL] SLAB 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class EXOGUL { // SLAB IS FOR ONLY ABOVE THE 8152 WEIGHT LIMIT OR WHEN USER REQUESTED
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
    private hub: DEATH_STAR
  
	constructor(DEATH_STAR: DEATH_STAR, capacity: number = 1 << 20, slabCapacity = 1 << 20) {
        this.hub = DEATH_STAR
		if (!pow2(capacity)) throw new Error("slotCapacity must be a power of two");

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
				this.freeBytes(oldOffset + length, oldLength - length);
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
			throw new RangeError("STORAGE_RING: no contiguous slab range large enough");
		}

		const offset = this.slabTail;
		this.slabTail += length;
		return offset;
	}
	private freeBytes(offset: N, length: N): V {
		if (length === 0) return;

		const ranges = this.freeRanges;
		let index = 0;

		while (index < ranges.length && ranges[index].offset < offset) {
			index++;
		}

		ranges.splice(index, 0, { offset, length });

		// Merge with the preceding range.
		if (index > 0 && ranges[index - 1].offset + ranges[index - 1].length === offset) {
			ranges[index - 1].length += length;
			ranges.splice(index, 1);
			index--;
		}

		// Merge with the following range.
		if (index + 1 < ranges.length && ranges[index].offset + ranges[index].length === ranges[index + 1].offset) {
			ranges[index].length += ranges[index + 1].length;
			ranges.splice(index + 1, 1);
		}

		// Return a free suffix to the unused slab tail.
		const last = ranges[ranges.length - 1];

		if (last !== undefined && last.offset + last.length === this.slabTail) {
			this.slabTail = last.offset;
			ranges.pop();
		}
	}
	private assertLive(slot: N): V {
		if (!Number.isInteger(slot) || slot <= 0 || slot >= this.allocated || this.refCount[slot] <= 0) {
			throw new RangeError(`STORAGE_RING: invalid or released slot ${slot}`);
		}
	}
	private assertEpoch(epoch: N): V {
		if (!Number.isInteger(epoch) || epoch < 0 || epoch > 0xffffffff) {
			throw new RangeError("STORAGE_RING: epoch must be a uint32");
		}
	}
	tick(): number {
		return this.versions[0];
	}
	advance(): number {
		return ++this.versions[0];
	}// Allocate a new slot from the free list. Returns the slot index, or -1 if no slots are available.
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
	}// Increment the reference count for a slot. Returns true if successful, false if the slot is not allocated.
	retain(slot: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;

		if (this.refCount[slot] === 0x7fffffff) {
			throw new Error("Reference count overflow");
		}

		this.refCount[slot]++;
		return true;
	}// Decrement the reference count for a slot. If the ref count reaches zero, the slot is freed. Returns true if the slot was freed, false otherwise.
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
	}// Write a numeric value into a slot with epoch validation. Returns true if successful, false if the slot is invalid or epoch mismatch.
	write(slot: number, value: number, epoch: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;
		if (this.refCount[slot] <= 0) return false;

		// Epoch validation: the slot's version must match the expected epoch
		if (this.versions[slot] !== epoch) return false;

		this.values[slot] = value;
		return true;
	}// Read the numeric value from a slot. Returns NaN if the slot is invalid.
	read(slot: number): number {
		if (slot < 0 || slot >= this.capacity) return NaN;
		if (!this.inUse[slot]) return NaN;

		return this.values[slot];
	}// Get the version number for a slot. Returns -1 if the slot is invalid.
	version(slot: number): number {
		if (slot < 0 || slot >= this.capacity) return -1;

		return this.versions[slot];
	}// Get the slab reference (offset and length) for a slot. Returns { offset: -1, length: 0 } if no slab data is associated.
	ref(slot: number): { offset: number; length: number } {
		if (slot < 0 || slot >= this.capacity) {
			return { offset: -1, length: 0 };
		}

		return {
			offset: this.slabOffsets[slot],
			length: this.slabLengths[slot],
		};
	}// Store variable-sized binary data in the slab for a given slot. Returns true if successful, false if insufficient slab space.
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
	}// Read the variable-sized binary data associated with a slot. Returns a subarray view (no copy) or null if no data.
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
		//this.slab = newSlab;
	}// Check if the slab buffer needs compaction. Returns true if compaction would reclaim significant space.
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
	}// Compact the slab buffer, removing dead space. Invalidates any subarray views previously returned by loadSlab.
	compact(): void {
		if (!this.needsCompaction()) return;

		const newSlab = new Uint8Array(this.slab.length);
		let newTail = 0;

		for (let i = 1; i < this.capacity; i++) {
			if (!this.inUse[i]) continue;

			const length = this.slabLengths[i];
			if (length <= 0) continue;

			const oldOffset = this.slabOffsets[i];
			newSlab.set(this.slab.subarray(oldOffset, oldOffset + length), newTail);

			this.slabOffsets[i] = newTail;
			newTail += length;
		}

		// Copy back into original slab buffer
		this.slab.set(newSlab.subarray(0, newTail), 0);
		this.slabTail = newTail;
	}// Get statistics about the storage ring.
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
	}// Validate internal consistency (for debugging). Returns an array of error strings, empty if consistent.
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
			errors.push(`Free list has ${freeCount} entries, expected ${this.capacity - this.allocated}`);
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
	get used(): N {return this.allocated - 1 - this.freeCount();}
	get freeCount(): N { return this.freeHead === FREE_LIST_END ? 0 : this.walkFree();	}
	get slabUsed(): N { return this.liveBytes;	}
	get slabFree(): N { return this.slab.length - this.liveBytes; }
	private walkFree(): N {
		let n = 0;
		let p = this.freeHead;
		while (p !== FREE_LIST_END) {
			n++;
			p = this.nextFree[p];
		}
		return n;
	}
}
// #endregion
//                                                                                                                   [GOVERNOR] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class GOVERNOR {
    private hub: DEATH_STAR
	/* ==========================================================================
	   The disruptor runs hot on its own.
	   The public API never touches passes.
	   ========================================================================== */
	  private tier: 0 | 1 | 2 | 3 = 0;          // 25 / 50 / 75 / 100
	  private running = false;
	  private hibernating = false;
	  private lastAction = 0;
	  private windowActions = 0;
	  private windowStart = 0;
	  private readonly IDLE_MS = 15_000;
	  private readonly TIERS = [
		256,    // tier 0 -> 25%
		1_024,   // tier 1 -> 50%
		4_096,   // tier 2 -> 75%
		16_384,  // tier 3 -> 100%
	  ];
	  constructor(DEATH_STAR: DEATH_STAR) {
        this.hub = DEATH_STAR
      }
	  public wake(): void {
		this.lastAction = Date.now();
	
		if (this.hibernating) {
		  this.hibernating = false;
		}
	
		if (!this.running) {
		  this.running = true;
		  queueMicrotask(() => this.loop());
		}
	  }
	  private async loop(): Promise<void> {
		while (this.running) {
		  const now = Date.now();
	
		  // Hibernation: 15s of true silence.
		  if (now - this.lastAction >= this.IDLE_MS) {
			this.hibernating = true;
			this.running = false;
			this.tier = 0;
			return;
		  }
	
		  // Drain one revolution at the current tier.
		  this.hub.DARTH_MAUL.pass();
	
		  // Sample load and adjust tier.
		  this.windowActions++;
	
		  if (this.windowStart === 0) {
			this.windowStart = now;
		  }
	
		  const windowMs = now - this.windowStart;
	
		  if (windowMs >= 1_000) {
			const rate = (this.windowActions / windowMs) * 1_000;
	
			this.tier = this.tierFor(rate);
	
			this.windowActions = 0;
			this.windowStart = now;
		  }
	
		  // Yield so the loop never starves the JS runtime.
		  await Promise.resolve();
		}
	  }
	  private tierFor(rate: number): 0 | 1 | 2 | 3 {
		if (rate >= this.TIERS[2]) return 3;
		if (rate >= this.TIERS[1]) return 2;
		if (rate >= this.TIERS[0]) return 1;
		return 0;
	  }
      public currentTier(): number {
		return this.tier;
	  }
	  public isHibernating(): boolean {
		return this.hibernating;
	  }
}
// #endregion
//                                                                                                         [DARTH MAUL] SIGNALS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class DARTH_MAUL_ENGINE {
	public hub: DARTH_MAUL;
	// ======================================================================
	// SOA / lane storage — Warlock lane semantics preserved
	// ======================================================================
	private readonly capacity: number;
	private readonly lane: I32;
	private readonly ops: U8;
	public readonly posX: F32;
	public readonly posY: F32;
	public readonly val: I32;
	private readonly freePids: I32;
	private freeHead = 0;
	private freeTail = 0;
	private laneLen = 0;
	public nextAllocatedPid = 1;
	// Lane deduplication: 1 means this PID already has a pending lane entry.
	private readonly inLane: U8;
	// ======================================================================
	// Reactive node columns
	// ======================================================================
	private readonly rKind: U8;
	private readonly rFlags: U16;
	private readonly rValue: U[];
	private readonly rPending: U[];
	private readonly rGetter: (((prev?: U) => U) | UD)[];
	private readonly rFn: ((() => V | (() => V)) | UD)[];
	private readonly rCleanup: ((() => V) | UD)[];
	private readonly rDeps: I32;
	private readonly rDepsTail: I32;
	private readonly rSubs: I32;
	private readonly rSubsTail: I32;
	// ======================================================================
	// Link pool
	// ======================================================================
	private readonly lVersion: I32;
	private readonly lDep: I32;
	private readonly lSub: I32;
	private readonly lPrevDep: I32;
	private readonly lNextDep: I32;
	private readonly lPrevSub: I32;
	private readonly lNextSub: I32;
	private readonly freeLinks: I32;
	private freeLinkHead = 0;
	private freeLinkTail = 0;
	private nextLink = 1;
	// ======================================================================
	// Scheduler
	// ======================================================================
	private readonly effectQueue: I32;
	private queuedLength = 0;
	private notifyIndex = 0;
	public readonly HasChildEffect = 64;
	public cycle = 0;
	public runDepth = 0;
	public batchDepth = 0;
	public activeSub: Pid | UD;
    public BRAND_SIGNAL = Symbol("warlock.signal");
	public BRAND_EFFECT = Symbol("warlock.effect");
	public BRAND_EFFECT_SCOPE = Symbol("warlock.effectScope");
	public BRAND_COMPUTED = Symbol("warlock.computed");
	constructor(hub: DARTH_MAUL, capacity = 1 << 18) {
		this.hub = hub;
		this.capacity = capacity;

		this.lane = new Int32Array(capacity).fill(-1);
		this.ops = new Uint8Array(capacity);
		this.posX = new Float32Array(capacity);
		this.posY = new Float32Array(capacity);
		this.val = new Int32Array(capacity);

		this.freePids = new Int32Array(capacity);
		this.inLane = new Uint8Array(capacity);

		this.rKind = new Uint8Array(capacity);
		this.rFlags = new Uint16Array(capacity);
		this.rValue = new Array(capacity);
		this.rPending = new Array(capacity);
		this.rGetter = new Array(capacity);
		this.rFn = new Array(capacity);
		this.rCleanup = new Array(capacity);

		this.rDeps = new Int32Array(capacity).fill(-1);
		this.rDepsTail = new Int32Array(capacity).fill(-1);
		this.rSubs = new Int32Array(capacity).fill(-1);
		this.rSubsTail = new Int32Array(capacity).fill(-1);

		this.effectQueue = new Int32Array(capacity);

		this.lVersion = new Int32Array(capacity);
		this.lDep = new Int32Array(capacity);
		this.lSub = new Int32Array(capacity);
		this.lPrevDep = new Int32Array(capacity).fill(-1);
		this.lNextDep = new Int32Array(capacity).fill(-1);
		this.lPrevSub = new Int32Array(capacity).fill(-1);
		this.lNextSub = new Int32Array(capacity).fill(-1);

		this.freeLinks = new Int32Array(capacity);

		for (let i = capacity - 1; i >= 1; i--) {
			this.freePids[this.freeTail++] = i;
		}

		this.lane[0] = TRACER_PID;
		this.val[TRACER_PID] = 0;
		this.ops[TRACER_PID] = OP.NONE;
		this.laneLen = 1;
	}
	// ======================================================================
	// World-object lane API — unchanged semantics, now deduplicated
	// ======================================================================
	private enqueueOp(pid: Pid, op: N): V {
		if (pid === TRACER_PID) return;

		// Dedup: if already queued, only the latest operation wins.
		if (this.inLane[pid]) {
			this.ops[pid] = op;
			return;
		}

		if (this.laneLen >= this.capacity) {
			throw new Error("DISRUPTOR: lane capacity exhausted");
		}

		this.ops[pid] = op;
		this.inLane[pid] = 1;
		this.lane[this.laneLen++] = pid;
	}
	public check_size_and_edit(x: N, y: N, pid: Pid, value: N): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.EDIT);
	}
	public getX(pid: Pid): N {
		return this.posX[pid];
	}
	public getY(pid: Pid): N {
		return this.posY[pid];
	}
	public getPosition(pid: Pid): { x: N; y: N } {
		return { x: this.posX[pid], y: this.posY[pid] };
	}
	public write(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.WRITE);
	}
	public create(x: N, y: N, value: N, pid: Pid): V {
		this.write(x, y, value, pid);
	}
	public edit(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.EDIT);
	}
	public read(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.READ);
	}
	public delete(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.DELETE);
	}
	public effect(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.EFFECT);
	}
	public compact(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.COMPACT);
	}
	public none(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.NONE);
	}
	public send(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.SEND);
	}
	public sub(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.SUB);
	}
	public duplex(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.DUPLEX);
	}
	public stream(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.STREAM);
	}
	public drc(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.enqueueOp(pid, OP.DRC);
	}
	// ======================================================================
	// Lane pass — Warlock structure preserved, dedup markers cleared
	// ======================================================================
	public pass(): V {
		const len = this.laneLen;
		let writeIdx = 0;
		let i = 0;

		const unrolledLimit = len - (len % 4);

		for (; i < unrolledLimit; i += 4) {
			for (let k = 0; k < 4; k++) {
				const pid = this.lane[i + k];

				if (pid === TRACER_PID) {
					this.val[TRACER_PID]++;
					this.lane[writeIdx++] = pid;
				} else if (this.ops[pid] === OP.DELETE) {
					this.inLane[pid] = 0;
					this.reclaimPid(pid);
				} else {
					this.inLane[pid] = 0;
					this.ops[pid] = OP.NONE;
					this.lane[writeIdx++] = pid;
				}
			}
		}

		for (; i < len; i++) {
			const pid = this.lane[i];

			if (pid === TRACER_PID) {
				this.val[TRACER_PID]++;
				this.lane[writeIdx++] = pid;
			} else if (this.ops[pid] === OP.DELETE) {
				this.inLane[pid] = 0;
				this.reclaimPid(pid);
			} else {
				this.inLane[pid] = 0;
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
			}
		}

		this.laneLen = writeIdx;
	}
	/** One tick: world-object pass, then reactive effect flush. */
	public tick(): V {
		this.pass();
		this.flush();
	}
	public get tickCount(): N {
		return this.val[TRACER_PID];
	}
	public get length(): N {
		return this.laneLen;
	}
	public getLoadFactor(): N {
		const activeCount = Math.max(0, this.laneLen - 1);
		return activeCount / (this.capacity - 1);
	}
	public waitFor(): V {}
	public sweep_expired(): V {}
	public on_change(): V {}
	// ======================================================================
	// PID / link lifecycle
	// ======================================================================
	private allocPid(): Pid {
		if (this.freeHead !== this.freeTail) {
			const pid = this.freePids[this.freeHead];
			this.freeHead = (this.freeHead + 1) % this.capacity;
			return pid;
		}

		if (this.nextAllocatedPid >= this.capacity) {
			throw new Error("DISRUPTOR: capacity exhausted — no free pids");
		}

		return this.nextAllocatedPid++;
	}
	private allocLink(): Lid {
		if (this.freeLinkHead !== this.freeLinkTail) {
			const lid = this.freeLinks[this.freeLinkHead];
			this.freeLinkHead = (this.freeLinkHead + 1) % this.capacity;
			return lid;
		}

		if (this.nextLink >= this.capacity) {
			throw new Error("DISRUPTOR: link capacity exhausted");
		}

		return this.nextLink++;
	}
	private freeLink(lid: Lid): V {
		this.lVersion[lid] = 0;
		this.lDep[lid] = -1;
		this.lSub[lid] = -1;
		this.lPrevDep[lid] = -1;
		this.lNextDep[lid] = -1;
		this.lPrevSub[lid] = -1;
		this.lNextSub[lid] = -1;

		this.freeLinks[this.freeLinkTail] = lid;
		this.freeLinkTail = (this.freeLinkTail + 1) % this.capacity;
	}
	public dispose(pid: Pid): V {
		this.reclaimPid(pid);
	}
	private reclaimPid(pid: Pid): V {
		if (pid === TRACER_PID) return;
		if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

		switch (this.rKind[pid]) {
			case RKind.EFFECT:
				this.effectOper(pid);
				break;

			case RKind.EFFECT_SCOPE:
				this.effectScopeOper(pid);
				break;

			case RKind.COMPUTED:
				this.disposeAllDepsInReverse(pid);
				break;
		}

		this.rKind[pid] = RKind.NONE;
		this.rFlags[pid] = RX_Flags.DISPOSED;
		this.rValue[pid] = undefined;
		this.rPending[pid] = undefined;
		this.rGetter[pid] = undefined;
		this.rFn[pid] = undefined;
		this.rCleanup[pid] = undefined;

		this.rDeps[pid] = -1;
		this.rDepsTail[pid] = -1;
		this.rSubs[pid] = -1;
		this.rSubsTail[pid] = -1;

		this.inLane[pid] = 0;

		this.freePids[this.freeTail] = pid;
		this.freeTail = (this.freeTail + 1) % this.capacity;
	}
	// ======================================================================
	// Reactive graph links
	// ======================================================================
	public link(dep: Pid, sub: Pid, version: number): void {
		const prevDep = this.rDepsTail[sub];

		if (prevDep !== -1 && this.lDep[prevDep] === dep) {
			return;
		}

		const nextDep = prevDep !== -1 ? this.lNextDep[prevDep] : this.rDeps[sub];

		if (nextDep !== -1 && this.lDep[nextDep] === dep) {
			this.lVersion[nextDep] = version;
			this.rDepsTail[sub] = nextDep;
			return;
		}

		const prevSub = this.rSubsTail[dep];

		if (prevSub !== -1 && this.lVersion[prevSub] === version && this.lSub[prevSub] === sub) {
			return;
		}

		const lid = this.allocLink();

		this.lVersion[lid] = version;
		this.lDep[lid] = dep;
		this.lSub[lid] = sub;

		this.lPrevDep[lid] = prevDep;
		this.lNextDep[lid] = nextDep;
		this.lPrevSub[lid] = prevSub;
		this.lNextSub[lid] = -1;

		this.rDepsTail[sub] = lid;
		this.rSubsTail[dep] = lid;

		if (nextDep !== -1) this.lPrevDep[nextDep] = lid;
		if (prevDep !== -1) this.lNextDep[prevDep] = lid;
		else this.rDeps[sub] = lid;

		if (prevSub !== -1) this.lNextSub[prevSub] = lid;
		else this.rSubs[dep] = lid;
	}
	public unlink(lid: Lid, sub: Pid = this.lSub[lid]): Lid {
		const dep = this.lDep[lid];
		const prevDep = this.lPrevDep[lid];
		const nextDep = this.lNextDep[lid];
		const prevSub = this.lPrevSub[lid];
		const nextSub = this.lNextSub[lid];

		if (nextDep !== -1) this.lPrevDep[nextDep] = prevDep;
		else this.rDepsTail[sub] = prevDep;

		if (prevDep !== -1) this.lNextDep[prevDep] = nextDep;
		else this.rDeps[sub] = nextDep;

		if (nextSub !== -1) this.lPrevSub[nextSub] = prevSub;
		else this.rSubsTail[dep] = prevSub;

		if (prevSub !== -1) {
			this.lNextSub[prevSub] = nextSub;
		} else {
			this.rSubs[dep] = nextSub;

			// Dependency has no subscribers left.
			if (nextSub === -1) {
				this.unwatched(dep);
			}
		}

		this.freeLink(lid);
		return nextDep;
	}
	public disposeAllDepsInReverse(pid: Pid): void {
		let link = this.rDepsTail[pid];

		while (link !== -1) {
			const prev = this.lPrevDep[link];
			this.unlink(link, pid);
			link = prev;
		}
	}
	public purgeDeps(pid: Pid): void {
		const tail = this.rDepsTail[pid];
		let link = tail !== -1 ? this.lNextDep[tail] : this.rDeps[pid];

		while (link !== -1) {
			link = this.unlink(link, pid);
		}
	}
	public unwatched(pid: Pid): void {
		switch (this.rKind[pid]) {
			case RKind.COMPUTED:
				this.rFlags[pid] = RX_Flags.MUTABLE;
				this.disposeAllDepsInReverse(pid);
				break;

			case RKind.SIGNAL:
				break;

			case RKind.EFFECT:
				this.effectOper(pid);
				break;

			case RKind.EFFECT_SCOPE:
				this.effectScopeOper(pid);
				break;
		}
	}
	// ======================================================================
	// Propagation — corrected iterative version
	// ======================================================================
	public propagate(startLink: number, innerWrite: boolean): void {
		if (startLink === -1) return;

		const stack: number[] = [startLink];

		while (stack.length > 0) {
			const link = stack.pop()!;
			const sub = this.lSub[link];

			if (sub === -1) continue;

			let flags = this.rFlags[sub];

			if (flags & RX_Flags.RECURSEDCHECK) {
				// Currently evaluating; defer until it finishes.
				continue;
			}

			if (!(flags & RX_Flags.PENDING)) {
				this.rFlags[sub] = flags | RX_Flags.PENDING;

				if (innerWrite) {
					this.rFlags[sub] |= RX_Flags.RECURSED;
				}

				flags = this.rFlags[sub];
			}

			if (flags & RX_Flags.WATCHING) {
				this.notify(sub);
			}

			// Computeds continue propagation to their own subscribers.
			if (this.rKind[sub] === RKind.COMPUTED) {
				let childLink = this.rSubs[sub];

				while (childLink !== -1) {
					stack.push(childLink);
					childLink = this.lNextSub[childLink];
				}
			}
		}
	}
	public shallowPropagate(startLink: number): void {
		let link = startLink;

		while (link !== -1) {
			const sub = this.lSub[link];
			const flags = this.rFlags[sub];

			if (flags & RX_Flags.PENDING) {
				if ((flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) === RX_Flags.WATCHING) {
					this.notify(sub);
				}
			}

			link = this.lNextSub[link];
		}
	}
	public notify(effectPid: Pid): void {
		let pid = effectPid;

		while (pid !== -1 && pid !== TRACER_PID) {
			const flags = this.rFlags[pid];

			if (!(flags & RX_Flags.WATCHING)) break;

			// Queue this effect.
			if (this.queuedLength < this.effectQueue.length) {
				this.effectQueue[this.queuedLength++] = pid;
			}

			// Clear WATCHING so it is not queued twice.
			this.rFlags[pid] &= ~RX_Flags.WATCHING;

			// Walk to parent effect/scope, if any.
			const parentLink = this.rSubs[pid];

			if (parentLink === -1) break;

			const parent = this.lSub[parentLink];

			if (parent === -1 || (this.rKind[parent] !== RKind.EFFECT && this.rKind[parent] !== RKind.EFFECT_SCOPE)) {
				break;
			}

			pid = parent;
		}
	}
	// ======================================================================
	// Dirty checking — version-based, no recursive graph walk
	// ======================================================================
	public isDirty(pid: Pid): boolean {
		let link = this.rDeps[pid];

		while (link !== -1) {
			const dep = this.lDep[link];

			if (this.rKind[dep] === RKind.COMPUTED) {
				if (this.isDirty(dep)) {
					this.updateComputed(dep);
				}
			}

			if (this.lVersion[link] !== this.lVersion[dep]) {
				return true;
			}

			link = this.lNextDep[link];
		}

		return false;
	}
	// Alias kept for compatibility with the original API.
	public checkDirty(_link: number, pid: Pid): boolean {
		return this.isDirty(pid);
	}
	// ======================================================================
	// Signal operations
	// ======================================================================
	public signalOper<T>(pid: Pid, ...value: [] | [T]): T | void {
		if (value.length > 0) {
			const next = value[0];

			if (!Object.is(this.rPending[pid], next)) {
				this.rPending[pid] = next;
				this.rFlags[pid] |= RX_Flags.DIRTY;

				const subs = this.rSubs[pid];

				if (subs !== -1) {
					this.propagate(subs, this.runDepth > 0);
				}

				if (this.batchDepth === 0) {
					this.commitSignal(pid);
					this.flush();
				}
			}

			return;
		}

		if (this.rFlags[pid] & RX_Flags.DIRTY) {
			this.commitSignal(pid);
		}

		const sub = this.activeSub;

		if (sub !== undefined) {
			this.link(pid, sub, this.cycle);
		}

		return this.rValue[pid] as T;
	}
	private commitSignal(pid: Pid): void {
		const previous = this.rValue[pid];
		const next = this.rPending[pid];

		this.rFlags[pid] &= ~RX_Flags.DIRTY;

		if (Object.is(previous, next)) return;

		this.rValue[pid] = next;
		this.cycle++;
		this.lVersion[pid] = this.cycle;

		const subs = this.rSubs[pid];

		if (subs !== -1) {
			this.propagate(subs, this.runDepth > 0);
		}
	}
	public updateSignal(pid: Pid): boolean {
		const previous = this.rValue[pid];
		const next = this.rPending[pid];

		this.rValue[pid] = next;

		return !Object.is(previous, next);
	}
	// ======================================================================
	// Computed operations
	// ======================================================================
	public computedOper<T>(pid: Pid): T {
		if (this.isDirty(pid)) {
			this.updateComputed(pid);

			const subs = this.rSubs[pid];

			if (subs !== -1) {
				this.shallowPropagate(subs);
			}
		}

		const sub = this.activeSub;

		if (sub !== undefined) {
			this.link(pid, sub, this.cycle);
		}

		return this.rValue[pid] as T;
	}
	public updateComputed(pid: Pid): boolean {
		if (this.rFlags[pid] & RX_Flags.RUNNING) {
			return false;
		}

		// Remove stale child-effect ownership links before re-evaluating.
		if (this.rFlags[pid] & this.HasChildEffect) {
			let link = this.rDepsTail[pid];

			while (link !== -1) {
				const prev = this.lPrevDep[link];
				const dep = this.lDep[link];

				if (this.rKind[dep] !== RKind.SIGNAL && this.rKind[dep] !== RKind.COMPUTED) {
					this.unlink(link, pid);
				}

				link = prev;
			}
		}

		// Full refresh: drop all old dependency links, then retrack.
		this.disposeAllDepsInReverse(pid);

		this.rFlags[pid] = RX_Flags.RUNNING | RX_Flags.RECURSEDCHECK;

		const previousValue = this.rValue[pid];
		const previousSub = this.setActiveSub(pid);

		try {
			this.cycle++;

			const nextValue = this.rGetter[pid]!(previousValue);

			this.rValue[pid] = nextValue;
			this.lVersion[pid] = this.cycle;

			return !Object.is(previousValue, nextValue);
		} finally {
			this.activeSub = previousSub;
			this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
			this.purgeDeps(pid);
		}
	}
	// ======================================================================
	// Effects
	// ======================================================================
	public run(pid: Pid): void {
		if (this.rKind[pid] !== RKind.EFFECT) return;

		const flags = this.rFlags[pid];

		if (flags & RX_Flags.RUNNING) {
			// Re-entrant run: defer rather than recurse.
			this.scheduleEffect(pid);
			return;
		}

		const pending = (flags & RX_Flags.PENDING) !== 0;
		const dirty = pending ? this.isDirty(pid) : false;

		if (!pending && !dirty && this.rDeps[pid] !== -1) {
			this.rFlags[pid] = RX_Flags.WATCHING | (flags & this.HasChildEffect);
			return;
		}

		if (this.rCleanup[pid]) {
			this.runCleanup(pid);

			if (this.rFlags[pid] & RX_Flags.DISPOSED) {
				return;
			}
		}

		// Full refresh before rerun: prevents stale dependency retention.
		this.disposeAllDepsInReverse(pid);

		this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

		const previousSub = this.setActiveSub(pid);

		try {
			this.cycle++;
			this.runDepth++;

			const cleanup = this.rFn[pid]!();

			this.rCleanup[pid] = typeof cleanup === "function" ? (cleanup as () => void) : undefined;
		} finally {
			this.runDepth--;
			this.activeSub = previousSub;
			this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
			this.purgeDeps(pid);
		}
	}
	public runCleanup(pid: Pid): void {
		const cleanup = this.rCleanup[pid];

		if (!cleanup) return;

		this.rCleanup[pid] = undefined;

		const previousSub = this.activeSub;
		this.activeSub = undefined;

		try {
			cleanup();
		} finally {
			this.activeSub = previousSub;
		}
	}
	public effectOper(pid: Pid): void {
		if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

		this.runCleanup(pid);
		this.effectScopeOper(pid);
	}
	public effectScopeOper(pid: Pid): void {
		if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

		// Remove this scope from its parent before disposing children.
		this.unlinkParent(pid);

		// Dispose all child effects/scopes linked to this scope.
		this.disposeAllDepsInReverse(pid);

		this.rFlags[pid] |= RX_Flags.DISPOSED;
	}
	private unlinkParent(pid: Pid): void {
		let link = this.rSubs[pid];

		while (link !== -1) {
			const next = this.lNextSub[link];
			const parent = this.lSub[link];

			if (this.rKind[parent] === RKind.EFFECT || this.rKind[parent] === RKind.EFFECT_SCOPE) {
				this.unlink(link, pid);

				// Clear parent ownership flag if it has no remaining children.
				if (this.rDeps[parent] === -1) {
					this.rFlags[parent] &= ~this.HasChildEffect;
				}
			}

			link = next;
		}
	}
	private scheduleEffect(pid: Pid): void {
		if (this.rFlags[pid] & RX_Flags.PENDING) return;

		this.rFlags[pid] |= RX_Flags.PENDING;

		if (this.queuedLength < this.effectQueue.length) {
			this.effectQueue[this.queuedLength++] = pid;
		}
	}
	// ======================================================================
	// Flush
	// ======================================================================
	public flush(): void {
		try {
			while (this.notifyIndex < this.queuedLength) {
				const pid = this.effectQueue[this.notifyIndex];
				this.effectQueue[this.notifyIndex++] = -1;

				this.rFlags[pid] &= ~RX_Flags.PENDING;
				this.run(pid);
			}
		} finally {
			// Any effects scheduled during flush remain queued for next tick.
			while (this.notifyIndex < this.queuedLength) {
				const pid = this.effectQueue[this.notifyIndex];
				this.effectQueue[this.notifyIndex++] = -1;

				this.rFlags[pid] |= RX_Flags.WATCHING | RX_Flags.RECURSED;
			}

			this.notifyIndex = 0;
			this.queuedLength = 0;
		}
	}
	// ======================================================================
	// Factories
	// ======================================================================
	private allocNode(kind: RKind): Pid {
		const pid = this.allocPid();

		this.rKind[pid] = kind;
		this.rFlags[pid] = RX_Flags.NONE;
		this.rValue[pid] = undefined;
		this.rPending[pid] = undefined;
		this.rGetter[pid] = undefined;
		this.rFn[pid] = undefined;
		this.rCleanup[pid] = undefined;

		this.rDeps[pid] = -1;
		this.rDepsTail[pid] = -1;
		this.rSubs[pid] = -1;
		this.rSubsTail[pid] = -1;

		return pid;
	}
	public signal<T>(initialValue?: T): Pid {
		const pid = this.allocNode(RKind.SIGNAL);

		this.rValue[pid] = initialValue;
		this.rPending[pid] = initialValue;
		this.rFlags[pid] = RX_Flags.MUTABLE;

		return pid;
	}
	public computed<T>(getter: (previous?: T) => T): Pid {
		const pid = this.allocNode(RKind.COMPUTED);

		this.rGetter[pid] = getter as (prev?: unknown) => unknown;

		return pid;
	}
	public effectSignal(fn: () => void | (() => void)): Pid {
		const pid = this.allocNode(RKind.EFFECT);

		this.rFn[pid] = fn;
		this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

		const previousSub = this.setActiveSub(pid);

		if (previousSub !== undefined && previousSub !== -1) {
			this.rFlags[previousSub] |= this.HasChildEffect;
		}

		try {
			this.runDepth++;

			const cleanup = fn();

			this.rCleanup[pid] = typeof cleanup === "function" ? (cleanup as () => void) : undefined;
		} finally {
			this.runDepth--;
			this.activeSub = previousSub;
			this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
		}

		return pid;
	}
	public effectScope(fn: () => void): Pid {
		const pid = this.allocNode(RKind.EFFECT_SCOPE);

		this.rFlags[pid] = RX_Flags.MUTABLE;

		const previousSub = this.setActiveSub(pid);

		if (previousSub !== undefined && previousSub !== -1) {
			// Parent link: scope is dep, parent is sub.
			this.link(pid, previousSub, 0);
			this.rFlags[previousSub] |= this.HasChildEffect;
		}

		try {
			fn();
		} finally {
			this.activeSub = previousSub;
		}

		return pid;
	}
	public trigger(fn: () => void): void {
		const watcher = this.allocNode(RKind.EFFECT);

		this.rFlags[watcher] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

		const previousSub = this.setActiveSub(watcher);

		this.batchDepth++;

		try {
			fn();
		} finally {
			this.activeSub = previousSub;

			// Propagate every dependency recorded during fn().
			let link = this.rDeps[watcher];

			while (link !== -1) {
				const dep = this.lDep[link];
				const next = this.lNextDep[link];

				const subs = this.rSubs[dep];

				if (subs !== -1) {
					this.propagate(subs, this.runDepth > 0);
					this.shallowPropagate(subs);
				}

				link = next;
			}

			this.batchDepth--;

			if (this.batchDepth === 0) {
				this.flush();
			}

			this.reclaimPid(watcher);
		}
	}
	// ======================================================================
	// Compatibility helpers
	// ======================================================================
	public getActiveSub(): Pid | undefined {
		return this.activeSub;
	}
	public setActiveSub(sub?: Pid): Pid | undefined {
		const previous = this.activeSub;
		this.activeSub = sub;
		return previous;
	}
	public getBatchDepth(): number {
		return this.batchDepth;
	}
	public startBatch(): void {
		this.batchDepth++;
	}
	public endBatch(): void {
		if (--this.batchDepth === 0) {
			this.flush();
		}
	}
	public isSignal(pid: Pid): boolean {
		return this.rKind[pid] === RKind.SIGNAL;
	}
	public isComputed(pid: Pid): boolean {
		return this.rKind[pid] === RKind.COMPUTED;
	}
	public isEffect(pid: Pid): boolean {
		return this.rKind[pid] === RKind.EFFECT;
	}
	public isEffectScope(pid: Pid): boolean {
		return this.rKind[pid] === RKind.EFFECT_SCOPE;
	}


}
export const def = new class DEFINITION {
    public MASTER_ATOM_KEYS = ["ATOM", "MODULE_ATOM", "CLOUD_ATOM", "SAURON_ATOM", "DRS_ATOM", "DUPLEX_ATOM", "SLAB_ATOM", "STREAM_ATOM", "S_CLASS_ATOM", "SPREADSHEET_ATOM", "SEND_ATOM", "VERTEX"] as const;
    public MQ = {
        RETRY: this.DEFINE_ATOM_KIND("retry"),
        ACK: this.DEFINE_ATOM_KIND("ack"),
        DEADLINE: this.DEFINE_ATOM_KIND("deadline"),
        DEDUPE: this.DEFINE_ATOM_KIND("dedupe"),
        DEPTH: this.DEFINE_ATOM_KIND("depth"),
        WILDCARD: this.DEFINE_ATOM_KIND("wildcard"),
        REPLY: this.DEFINE_ATOM_KIND("reply"),
        WINDOW: this.DEFINE_ATOM_KIND("window"),
        RESUME: this.DEFINE_ATOM_KIND("resume"),
        CREDIT: this.DEFINE_ATOM_KIND("credit"),
        CORRELATION: this.DEFINE_ATOM_KIND("correlation"),
    } as const;
    public ELECTRONS = {
        ADAPTER_ELECTRON: this.DEFINE_ATOM_KIND("adapter"),
        BEAM_CODEC_ELECTRON: this.DEFINE_ATOM_KIND("beam_codec"),
        BINARY_ELECTRON: this.DEFINE_ATOM_KIND("binary"),
        BYTE_CODE_VM: this.DEFINE_ATOM_KIND("byte_code_vm"),
        CHANNEL_ELECTRON: this.DEFINE_ATOM_KIND("channel"),
        CHANNELS_ELECTRON: this.DEFINE_ATOM_KIND("channels"),
        CONCURRENCY_ELECTRON: this.DEFINE_ATOM_KIND("concurrency"),
        DEADLETTER_ELECTRON: this.DEFINE_ATOM_KIND("deadletter"),
        DESTRUCTION_ELECTRON: this.DEFINE_ATOM_KIND("destruction"),
        ERROR_ELECTRON: this.DEFINE_ATOM_KIND("error"),
        EVENT_ELECTRON: this.DEFINE_ATOM_KIND("event"),
        EXTERNAL_SUBSCRIPTION_ELECTRON: this.DEFINE_ATOM_KIND("exterenal_sub"),
        GATE_ELECTRON: this.DEFINE_ATOM_KIND("gate"),
        GLOBAL_SUBSCRIPTION_ELECTRON: this.DEFINE_ATOM_KIND("global_sub"),
        GOVERNOR_ELECTRON: this.DEFINE_ATOM_KIND("GOVERNOR"),
        HOOK_ELECTRON: this.DEFINE_ATOM_KIND("hook"),
        HTTP_TRANSPORT: this.DEFINE_ATOM_KIND("http_transport"),
        INJECTION_CODEC_ELECTRON: this.DEFINE_ATOM_KIND("injection"),
        METRICS_ELECTRON: this.DEFINE_ATOM_KIND("metrics"),
        MIDDLEWARE_ELECTRON: this.DEFINE_ATOM_KIND("middlleware"),
        RIFTS_ACCESS_ELECTRON: this.DEFINE_ATOM_KIND("rifts"),
        REACTIVE_ELECTRON: this.DEFINE_ATOM_KIND("reactive"),
        ROUTE_ELECTRON: this.DEFINE_ATOM_KIND("route"),
        RUN_ON_ELECTRON: this.DEFINE_ATOM_KIND("run_on"),
        SEALED_ERROR_ELECTRON: this.DEFINE_ATOM_KIND("sealed_error"),
        SESSION_ELECTRON: this.DEFINE_ATOM_KIND("session"),
        SHELL_ELECTRON: this.DEFINE_ATOM_KIND("reshelltry"),
        SIGNAL_ELECTRON: this.DEFINE_ATOM_KIND("signal"),
        SOA_QUAD_TIERED_REGISTRY_ELECTRON: this.DEFINE_ATOM_KIND("soa_registry"),
        SUBSCRIPTION_ELECTRON: this.DEFINE_ATOM_KIND("subscription"),
        TRANSFORM_ELECTRON: this.DEFINE_ATOM_KIND("transform"),
        WAITER_ELECTRON: this.DEFINE_ATOM_KIND("waiter"),
    } as const;
    public NEUTRONS = {
        COMPRESSION_NEUTRON: this.DEFINE_ATOM_KIND("compression"),
        CONFLICT_NEUTRON: this.DEFINE_ATOM_KIND("conflict"),
        CONTEXT_NEUTRON: this.DEFINE_ATOM_KIND("context"),
        ENCRYPTION_NEUTRON: this.DEFINE_ATOM_KIND("encryption"),
        GROUPS_NEUTRON: this.DEFINE_ATOM_KIND("groups"),
        HISTORY_NEUTRON: this.DEFINE_ATOM_KIND("history"),
        META_NEUTRON: this.DEFINE_ATOM_KIND("meta"),
        OFFSEC_NEUTRON: this.DEFINE_ATOM_KIND("offsec"),
        PERMISSIONS_NEUTRON: this.DEFINE_ATOM_KIND("permissions"),
        PERSISTENCE_NEUTRON: this.DEFINE_ATOM_KIND("persistence"),
        PRIORITY_NEUTRON: this.DEFINE_ATOM_KIND("priority"),
        SNAPSHOT_NEUTRON: this.DEFINE_ATOM_KIND("snapshot"),
        TAGS_NEUTRON: this.DEFINE_ATOM_KIND("tags"),
        TIME_TRAVEL_NEUTRON: this.DEFINE_ATOM_KIND("time_travel"),
        TTL_NEUTRON: this.DEFINE_ATOM_KIND("ttl"),
    } as const;
    public PROTONS = {
        CONTRACTS_PROTON: this.DEFINE_ATOM_KIND("contract"),
        REACTIVE_PROTON: this.DEFINE_ATOM_KIND("reactive"),
        REGISTRY_PROTON: this.DEFINE_ATOM_KIND("registry"),
        SCHEMA_PROTON: this.DEFINE_ATOM_KIND("schema"),
        VALUES_REGISTRY_PROTON: this.DEFINE_ATOM_KIND("values_reg"),
    } as const;
    public DEFINE_ATOM_KIND(name: string): symbol {
        return Symbol(name);
    }
    public PARTICLE_KEYS = {
        ...this.PROTONS,
        ...this.NEUTRONS,
        ...this.ELECTRONS,
        ...this.MQ,
    } as const;
    public BARE_MASTER_RUNTIME: ATOM_CONTRACT_RUNTIME = { createMaster: (kind, k, s, m, r, value) => ({ k, s, m, r, value }) };
}();
class DARTH_MAUL {
	//                                                                                                                    CONSTANTS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public hub: DEATH_STAR;
	public DARTH_MAUL_ENGINE: DARTH_MAUL_ENGINE;

	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "SAURON_SLAVE_REG";
	public k: A = "__SIGNALS__";
	u = undefined;
	uuuu = [this.u, this.u, this.u, this.u];
	UUUU = [this.s, this.m, this.r];
	uuu = [this.u, this.u, this.u];
	UUU = [this.s, this.m, this.r];
	uu = [this.u, this.u];
	UU = [this.m, this.r];
	private total_cache_size = 5000;
	public activeTracer: Map<SignalKey, SIGNAL_CELL> | null = null;
	constructor(hub: DEATH_STAR, capacity = 1 << 20) {
		this.DARTH_MAUL_ENGINE = new DARTH_MAUL_ENGINE(this, capacity);
		this.hub = hub;
		this.r = hub.r;
		this.m = hub.m;
		this.s = hub.s;
		this.k = hub.k;
	}
	// #endregion
	//                                                                                                                         ATOM
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public atom_contract<T = U, M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>, B extends object = Record<never, never>>(definition: (context: { readonly self: ATOM_CONTRACT_SELF }) => ATOM_CONTRACT_CONFIG<T, M, B>, runtime?: ATOM_CONTRACT_RUNTIME<M>): ATOM_CONTRACT<T, M, B> {
		// Defines a reusable atom contract. Defining it creates no atom and performs no registration. Calling the returned function creates one master and its declared bindables; the master itself remains owned by the supplied runtime.
		const selected = definition({ self: { atom: "ATOM" } });
		const kind = selected.atom ?? "ATOM";
		if (!def.MASTER_ATOM_KEYS.includes(kind)) {
			throw new TypeError(`Unknown master atom: ${String(kind)}`);
		}

		const config: ATOM_CONTRACT_CONFIG<T, M, B> = { ...selected, atom: kind };
		const masterRuntime = runtime ?? (def.BARE_MASTER_RUNTIME as ATOM_CONTRACT_RUNTIME<M>);

		const contract = ((initialValue?: T, k?: A, s?: S, m?: S, r?: S) => {
			const rr = r ?? this.r; // Store.get("THE_WIZARD_ROOT_REG") ?? "THE_WIZARD_ROOT_REG"
			const mm = m ?? this.m; // Store.get("THE_WIZARD_ROOT_REG") ?? 'SAURON_MASTER_REG'
			const ss = s ?? this.s; // Store.get("THE_WIZARD_ROOT_REG") ?? 'SAURON_SLAVE_REG'
			const kk = k ?? this.k; // Store.get("k") ?? 'the_NOT_fast_path'
			const configuredValue = config.value;
			const value = initialValue !== undefined ? initialValue : typeof configuredValue === "function" ? (configuredValue as () => T)() : (configuredValue as T);
			const master = masterRuntime.createMaster(kind, kk, ss, mm, rr, value);

			const bindables: B[] = [];
			const operations: B = {} as B;

			for (const createBindable of config.bindables ?? []) {
				if (!config.bindables!.includes(createBindable)) {
					throw new TypeError(`Unknown bindable: ${String(createBindable)}`);
				}

				const bound = createBindable({ self: master, kk, ss, mm, rr });
				bindables.push(bound);
				Object.assign(operations, bound);
			}

			const instance: ATOM_CONTRACT_INSTANCE<T, M, B> = Object.assign(operations, {
				atom: kind,
				kk,
				ss,
				mm,
				rr,
				master,
				bindables,
				get value() {
					return master.value;
				},
				set value(next: T) {
					master.value = next;
				},
			});

			return instance;
		}) as ATOM_CONTRACT<T, M, B>;

		Object.defineProperties(contract, {
			atom: { value: kind, enumerable: true },
			config: { value: config, enumerable: true },
		});
		return contract;
	} /** Marks a reusable bindable factory for inclusion in an atom contract. */

	public atom_bindable<M extends CONTRACT_MASTER = CONTRACT_MASTER, B extends object = Record<never, never>>(factory: ATOM_BINDABLE<M, B>): ATOM_BINDABLE<M, B> {
		return factory;
	}
	// #endregion
	//                                                                                                                         MAPS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	#maul_map = new M2<A, M<A, M<A, A>>>();
	#maul_naked_map = new M2<A, M<A, M<A, A>>>();
	#maul_meta = new M2<A, A>();
	public _map<K, V>(s: A = this.s, m: A = this.m, r: A = this.r): M2<A, A> {
		let root = this.#maul_map.get(r);
		if (!root) {
			root = new M2<A, Map<A, Map<A, A>>>();
			this.#maul_map.set(r, root);
		}

		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, Map<A, A>>();
			root.set(m, masterMap);
		}

		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new M2<A, A>();
			masterMap.set(s, slaveMap);
		}

		return slaveMap;
	}
	public _nakedMap<K, V>(s: A = this.s, m: A = this.m, r: A = this.r): M2<A, A> {
		let root = this.#maul_naked_map.get(r);
		if (!root) {
			root = new M2<A, Map<A, Map<A, A>>>();
			this.#maul_naked_map.set(r, root);
		}

		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, Map<A, A>>();
			root.set(m, masterMap);
		}

		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new M2<A, A>();
			masterMap.set(s, slaveMap);
		}

		return slaveMap;
	}
	public _getMetadata(s = this.s, m = this.m, r = this.r) {
		let root = this.#maul_meta.get(r);
		if (!root) {
			root = new M2<A, Map<A, Map<A, A>>>();
			this.#maul_naked_map.set(r, root);
		}

		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, Map<A, A>>();
			root.set(m, masterMap);
		}

		let metaMap = masterMap.get(s);
		if (!metaMap) {
			metaMap = {
				_initial: new M2<string, A>(),
				_sys: new M2<string, SysEntry>(),
				_keyContracts: new M2<string, Set<string>>(),
				_causal: new M2<string, CausalEntry[]>(),
				_conflicts: new M2<string, ConflictPolicy>(),
				_contracts: new M2<string, A>(),
				_scopes: new M2<string, SignalScope>(),
				_msgHandlers: new M2<string, Set<(p: A) => void>>(),
				_mainMap: new M2<A, string>(),
				_tupleCache: new M2<string, string>(),
				_weakMap: new WeakMap<object, string>(),
			};
			masterMap.set(s, metaMap);
		}
		return metaMap;
	}
	public _contracts = {
		DEFAULT: this.atom_contract(() => ({ atom: "VERTEX" })),
	};
	// #endregion
	//                                                                                                                          API
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public atom<T = any>(v: A, key: SignalKey = `__THE_WARLOCK_SIGNALS_${this.hub.REGISTRY._sid.b()}`, s = this.s, m = this.m, r = this.r, options?: SignalOptions) {
		const rk = this.create(key, v, options, s, m, r);
		const self = this;
		return {
			get value(): T {
				self._bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			get current(): T {
				self._bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			peek(): T {
				return rk.peek();
			},
			set value(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set current(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set crt(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set set(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
		};
	}
	public create<T = A>(k: SignalKey, v?: T | (() => T), options?: SignalOptions, s = this.s, m = this.m, r = this.r): ATOM<T> | T {
		const registry = this._map(s, m, r);
		let atom = registry.get(k);

		if (atom || v === undefined) {
			return atom as ATOM<T> | T;
		}

		const initialValue = typeof v === "function" ? (v as () => T)() : v;
		atom = this._contracts.DEFAULT(initialValue, k, s, m, r);
		registry.set(k, atom);
		return atom as ATOM<T> | T;
	}
	public get<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): A {
		const registry = this._map(s, m, r);
		return registry.get(k) as ATOM<T> | UD;
	}
	public write<T = A>(k: SignalKey, value: T | ((previous: T) => T), source = "write", s = this.s, m = this.m, r = this.r): T {
		return this._writeResolved(k, value, source, s, m, r);
	}
	public read<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this._map(s, m, r).get(k) as ATOM<T> | undefined;

		return atom?.read();
	}
	public peek<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this._map(s, m, r).get(k) as ATOM<T> | UD;

		return atom?.peek();
	}
	public signal<T>(): { (): T | UD; (value: T | UD): V };
	public signal<T>(initialValue: T): { (): T; (value: T): V };
	public signal<T>(initialValue?: T, k: A = this.k, s = this.s, m = this.m, r = this.r): { (): T | UD; (value: T | UD): V } {
		const signalFn = this.DARTH_MAUL_ENGINE.signalOper.bind({
			currentValue: initialValue,
			pendingValue: initialValue,
			subs: undefined,
			subsTail: undefined,
			flags: ReactiveFlags.MUTABLE,
		}) as () => T | undefined;

		Object.defineProperty(signalFn, this.DARTH_MAUL_ENGINE.BRAND_SIGNAL, { value: true });
		const registry = this._nakedMap(s, m, r);
		registry.set(k, signalFn);
		return signalFn;
	}
	public has(k: A, s = this.s, m = this.m, r = this.r): B {
		return this._map(s, m, r).get(k) !== undefined;
	}
	public hasNaked(k: A, s = this.s, m = this.m, r = this.r): B {
		return this._nakedMap(s, m, r).get(k) !== undefined;
	}
	public freeze(k: A, s = this.s, m = this.m, r = this.r): void {
		const atom = this._map(s, m, r).get(k);
		if (!atom) {
			throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
		}
		atom.setFrozen();
	}
	public unfreeze(k: A, s = this.s, m = this.m, r = this.r): void {
		const atom = this._map(s, m, r).get(k);
		if (!atom) {
			throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
		}
		atom.setUnfrozen();
	}
	public seal(k: A, s = this.s, m = this.m, r = this.r): V {
		const meta = this._map(s, m, r);
		const atom = meta.get(k);
		atom.setSealed();
	}
	public reset(k: A, s = this.s, m = this.m, r = this.r): V {
		const atom = this._map(s, m, r).get(k) as ATOM<A> | UD;
		if (!atom) {
			throw new Error(`[Sauron] Cannot reset unknown signal "${String(k)}"`);
		}
		atom.value = atom.initial;
		// this._writeResolved(k, atom.initial, "reset", s, m, r, "reset");
	}
	public hydrate<T>(k: A, value: T, s = this.s, m = this.m, r = this.r): V {
		const atom = this._map(s, m, r).get(k) as ATOM<T> | UD;
		if (!atom) {
			this.create(k, value, undefined, s, m, r);
			return;
		}
		// atom.write(value, { source: "hydrate", triggerRender: false });
	}
	public tx(fn: () => V): V {
		const o = { s: this.s, m: this.m, r: this.r };
		const snap = new Map<string, A>();
		for (const [k, v] of Object.entries(this.hub.state.state.getAll(o))) snap.set(k, v);
		const sysSnap = this._sys.snapshot();
		const self = this;
		try {
			fn();
		} catch (err) {
			for (const k of Object.keys(this.hub.state.state.getAll(o))) if (!snap.has(k)) this.hub.state.state.delete(k, o);
			for (const [k, v] of snap) this.hub.state.state.set(k, v, { ...o, triggerRender: false });
			self._sys.dropComputeds();
			self._sys.restore(sysSnap);
			throw err;
		}
	}
	public derive = (() => {
		const fn = <T,>(k: A, deps: A[], compute: (...values: A[]) => T, s = this.s, m = this.m, r = this.r, options?: { eager?: B; onError?: (err: Error) => V }): S => {
			if (this._getMetadata(s, m, r)._sys.get(k)?.isComputed) return k;
			const reg = this._map<S, A>(s, m, r);
			for (const d of deps) { this._sys.ensure(d, reg.get(d), s, m, r); }

			const c = computed(() => {
				try {
					const vals = deps.map((d) => this._sys.ensure(d, reg.get(d), s, m, r).read());
					return compute(...vals);
				} catch (err) {
					options?.onError?.(err as Error);
					throw err;
				}
			});

			this._getMetadata(s, m, r)._sys.set(k, { read: () => c(), write: () => {}, node: c as A, isComputed: true });
			reg.set( k, this.DARTH_MAUL_ENGINE.untrackFn(() => c()), );

			if (options?.eager) {
				// Warm the computed cache once. Do not write its result back
				// into another signal; that defeats lazy recomputation.
				this.DARTH_MAUL_ENGINE.untrackFn(() => c());
			}
			return k;
		};
		fn.invalidate = (k: A, s = this.s, m = this.m, r = this.r): V => {
			const entry = this._getMetadata(s, m, r)._sys.get(k);
			if (!entry || !entry.isComputed) return;
			this._writeResolved(k,this.DARTH_MAUL_ENGINE.untrackFn(() => entry.read()), "derived", "invalidate", 	s, m,r, );
		};
		return fn;
	})();

	// #endregion
	//                                                                                                                   SEND / SUB
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public sub = (() => {
		const fn = <T = A,>(listener: (value: T, previous: T) => V, k: SignalKey, s = this.s, m = this.m, r = this.r): (() => V) => {
			const atom = this.create(k, null, undefined, s, m, r) as ATOM<T> | T;

			if (!atom) {
				throw new Error(`[Sauron] Cannot subscribe to unknown signal "${String(k)}"`);
			}

			return atom.subscribe((change: A) => {
				listener(change.current, change.previous);
			});
		};
		fn.naked = () => {};
		return fn;
	})();
	public send = (() => {
		const fn = (p: A, k: S, s = this.s, m = this.m, r = this.r): V => {
			const atom = this._map(s, m, r).get(k);
			if (!atom) return console.log("no atom currently exist at that address!");
			atom.write(p);
		};
		fn.naked = () => {};
		return fn;
	})();
	public delete = (k: A, s = this.s, m = this.m, r = this.r): V => {
		this._map(s, m, r).delete(k);
		this._getMetadata(s, m, r)._sys.delete(k);
	};
	// #endregion
	//                                                                                                               [TRACE EFFECT]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public trace = (() => {
		const fn = (fn: () => V): { reads: SignalKey[]; result: V } => {
			const reads = new Map<SignalKey, SIGNAL_CELL>();
			const previous = this.activeTracer;
			this.activeTracer = reads;
			try {
				const result = fn();
				return { reads: [...reads.keys()], result, };
			} finally {
				this.activeTracer = previous;
			}
		};
		fn.effect = (fn: () => V | (() => V), s: S = this.s, m: S = this.m, r: S = this.r): (() => V) => {
			const reads = new Map<SignalKey, A>();
			const runs: Array<{run: number; reads: Map<SignalKey, A>; at: number; }> = [];
			let runCount = 0;
			const tracked = new Map<SignalKey, SIGNAL_CELL>();
			const registry = this._map(s, m, r);

			const effectDispose = effect(() => {
				runCount++;
				const before = new Map<SignalKey, A>();
				for (const [key, cell] of tracked) { before.set(key, cell.peek()); }

				const result = fn();

				const touched = new Map<SignalKey, A>();

				for (const [key, cell] of tracked) {
					const value = cell.peek();
					if (!Object.is(before.get(key), value)) { 	touched.set(key, value); }
				}

				runs.push({
					run: runCount,
					reads: new Map(tracked),
					at: Date.now(),
				});

				return result;
			});

			// Wrap reads by exposing a tracing proxy is not needed for the
			// first version: cells register themselves when read through
			// useCell / read(). For explicit tracing, callers can pass keys.
			return effectDispose;
		}; // Finds the first recorded write whose value differs from the current live value. That is the divergence point.
		fn.bisect = (entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: N; source?: S }>, s: S = this.s, m: S = this.m, r: S = this.r): { index: number; key: SignalKey; recorded: A; actual: A } | UD => {
			const registry = this._map(s, m, r);

			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i];
				const atom = registry.get(entry.key) as SIGNAL_CELL | undefined;

				if (!atom) continue;

				const actual = atom.peek();

				if (!Object.is(actual, entry.value)) {
					return {
						index: i,
						key: entry.key,
						recorded: entry.value,
						actual,
					};
				}
			}

			return undefined;
		};
		fn.why = <T = A,>(k: SignalKey, s: S = this.s, m: S = this.m, r: S = this.r): { key: SignalKey; value: T | UD; version: N; lastWriteSource?: S; causal: CausalEntry[]; history: readonly AtomHistoryEntry<T>[] } | UD => {
			const atom = this._map(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

			if (!atom) return undefined;

			return {
				key: atom.key,
				value: atom.peek(),
				version: atom.version,
				lastWriteSource: atom.lastWriteSource,
				causal: [...atom.causal].reverse(),
				history: atom.getHistory(),
			};
		};
		return fn;
	})();
	// #endregion
	//                                                                                                              [STATE SURGERY]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public surgery = (() => {
		// Live-state surgery. Reads the current value, hands it to `mutate`, writes the result back through the normal reactive write path, records a causal entry, and pushes an undo entry onto the surgery journal.
		// The mutation receives the current value. It may either: - mutate and return nothing (object/array mutation), or - return a replacement value.
		const fn = <T = A,>(k: SignalKey, mutate: (draft: T) => T | void, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S; record?: B }): T => {
			const atom = this._map(s, m, r).get(k) as ATOM<T> | UD;
			if (!atom) { throw new Error(`[Sauron] Cannot perform surgery on unknown signal "${String(k)}"`); }
			if (atom.frozen) { throw new Error(`[Sauron] Cannot perform surgery on frozen signal "${String(k)}"`); 	}
			const previous = atom.peek(); // Structured clone gives surgery isolation without touching the reactive kernel. Primitive values pass through unchanged.
			const draft = previous !== null && typeof previous === "object" ? (structuredClone(previous) as T) : previous;
			const result = mutate(draft);
			const next = result === undefined ? draft : (result as T);
			if (Object.is(previous, next)) { return previous; }

			const source = options?.source ?? "surgery";
			const cause = options?.cause ?? `surgery:${String(k)}:${Date.now()}`;

			const written = atom.write(next, {
				source,
				cause,
				triggerRender: true,
				recordHistory: options?.record ?? true,
			});

			this.surgery.journal.push({ key: k, previous, next: written, s, m, r, });

			return written;
		}; //  Undo the most recent surgery. Restoration goes through the normal write path, so computeds, effects, subscribers, history, and channels all see a real write.
		fn.journal = [] as Array<{ key: SignalKey; previous: A; next: A; s: S; m: S; r: S }>;
		fn.undo = (): A | UD => {
			const entry = this.surgery.journal.pop();

			if (!entry) return undefined;

			const atom = this._map(entry.s, entry.m, entry.r).get(entry.key) as SIGNAL_CELL | undefined;

			if (!atom) return undefined;

			return atom.write(entry.previous, {
				source: "surgery:undo",
				cause: `surgery:undo:${String(entry.key)}`,
				triggerRender: true,
				recordHistory: true,
			});
		}; //   Undo every surgery performed since the returned marker was created. Undo every surgery performed since the returned marker was created.
		fn.checkpoint = (): (() => V) => {
			const mark = this.surgery.journal.length;
			return () => { while (this.surgery.journal.length > mark) { this.surgery.undo(); } };
		}; // Surgery on a nested path. Example: sauron.surgeryAt('user', ['profile', 'email'], email => email.trim());
		fn.at = <T = A, P = A>(k: SignalKey, path: Array<S | N | SY>, mutate: (draft: P) => P | V, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S }): T => {
			return this.surgery<T>(
				k,
				(draft) => {
					let target: A = draft;

					for (let i = 0; i < path.length - 1; i++) {
						target = target[path[i] as keyof typeof target];
					}

					const leafKey = path[path.length - 1] as keyof typeof target;
					const leaf = target[leafKey] as P;

					const result = mutate(leaf);

					target[leafKey] = result === undefined ? leaf : (result as P);

					// Returning undefined keeps the outer draft as the next value.
					return undefined as unknown as T;
				},
				s,
				m,
				r,
				options,
			);
		};
		return fn;
	})();
	public _sys = (() => {
		const fn = () => {};
		fn.ensure = (rk: S, v: A, s = this.s, m = this.m, r = this.r): SysEntry => {
			const sysMap = this._getMetadata(s, m, r)._sys;
			let entry = sysMap.get(rk);
			if (entry) return entry;
			const sig = signal(v);
			entry = { read: () => sig(), write: (v: A) => sig(v), node: sig as A, isComputed: false };
			sysMap.set(rk, entry);
			if (this.hub.state.state.get(rk, { s, m, r }) === undefined) {
				this.hub.state.state.set(rk, v, { s, m, r, triggerRender: false });
			}
			return entry;
		};
		fn.snapshot = (s = this.s, m = this.m, r = this.r): M<S, A> => {
			const out = new Map<string, A>();
			for (const [rk, entry] of this._getMetadata(s, m, r)._sys) {
				if (!entry.isComputed)
					out.set(
						rk,
						this.DARTH_MAUL_ENGINE.untrackFn(() => entry.read()),
					);
			}
			return out;
		};
		fn.restore = (snap: M<S, A>): V => {
			for (const [rk, v] of snap) this._sys.ensure(rk, v).write(v);
		};
		fn.dropComputeds = (s = this.s, m = this.m, r = this.r): V => {
			for (const [rk, entry] of this._getMetadata(s, m, r)._sys) {
				if (entry.isComputed) this._getMetadata(s, m, r)._sys.delete(rk);
			}
		};
		return fn;
	})();
	// #endregion
	//                                                                                                                      BACKEND
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public _validateSchema<T>(schema: SignalSchema, value: T): T {
		try {
			const next = schema.coerce ? schema.coerce(value) : value;
			if (schema.validate(next)) {
				return next as T;
			}
			throw new Error("[Sauron] Schema validation failed");
		} catch (error) {
			console.error("[Sauron] Schema validation failed:", error);
			throw error;
		}
	}
	public _serializeTuple(tuple: A[], s = this.s, m = this.m, r = this.r): S {
		const parts: string[] = new Array(tuple.length);
		const weakMap = this._getMetadata(s, m, r)._weakMap;

		for (let i = 0; i < tuple.length; i++) {
			const item = tuple[i];
			const type = typeof item;

			if (item === null) {
				parts[i] = "null";
			} else if (type === "string") {
				parts[i] = `s:${item}`;
			} else if (type === "number") {
				parts[i] = `n:${item}`;
			} else if (type === "symbol") {
				parts[i] = `sym:${item!.description ?? String(item)}`;
			} else if (type === "bigint") {
				parts[i] = `bi:${item!.toString()}`;
			} else if (type === "undefined") {
				parts[i] = "undef";
			} else if (Array.isArray(item)) {
				parts[i] = `arr:[${this._serializeTuple(item, s, m, r)}]`;
			} else if (type === "object" || type === "function") {
				let refId = weakMap.get(item);
				if (!refId) {
					refId = `ref:${this.hub.REGISTRY._sid.b()}`;
					weakMap.set(item, refId);
				}
				parts[i] = refId;
			} else {
				parts[i] = `v:${String(item)}`;
			}
		}

		return parts.join("|");
	}
	public _hashTuple(tuple: U[], s = this.s, m = this.m, r = this.r): S {
		const key = this._serializeTuple(tuple, s, m, r);
		const tupleCache = this._getMetadata(s, m, r)._tupleCache;

		const cached = tupleCache.get(key);
		if (cached) {
			tupleCache.delete(key);
			tupleCache.set(key, cached);
			return cached;
		}

		let h1 = 0x811c9dc5;
		let h2 = 0x01000193;
		for (let i = 0; i < key.length; i++) {
			const code = key.charCodeAt(i);
			h1 = Math.imul(h1 ^ code, 16777619) >>> 0;
			h2 = Math.imul(h2 ^ code, 2246822507) >>> 0;
		}

		const result = `tuple:${h1.toString(36)}-${h2.toString(36)}`;
		tupleCache.set(key, result);

		if (tupleCache.size > this.total_cache_size) {
			const toRemove = Math.floor(tupleCache.size * 0.2);
			const iter = tupleCache.keys();
			for (let i = 0; i < toRemove; i++) {
				const k = iter.next().value;
				if (k !== undefined) tupleCache.delete(k);
			}
		}

		return result;
	}
	public _writeResolved<T>(k: SignalKey, value: T | ((previous: T) => T), source: S, s = this.s, m = this.m, r = this.r, cause?: S): T {
		const registry = this._map(s, m, r);
		let atom = registry.get(k) as ATOM<T> | UD;

		if (!atom) {
			const initial = typeof value === "function" ? undefined : (value as T);
			atom = this.create(k, initial, undefined, s, m, r) as ATOM<T>;
		}

		if (!atom) {
			throw new Error(`[Sauron] Cannot write unknown signal "${String(k)}"`);
		}

		return atom.write(value, {
			source,
			cause,
			triggerRender: atom.triggerRender,
		});
	}
	public _recordCausal<K, V>(k: S, value: V, source: S, s = this.s, m = this.m, r = this.r, cause?: S): void {
		const atom = this._map<K, V>(s, m, r).get(k);
		if (!atom) return;
		atom.causal.push({ key: k, value, timestamp: Date.now(), source, cause });
		if (atom.causal.length > 100) {
			atom.causal.shift();
		}
	}
	public _bindComponentToSignal(rk: S, s = this.s, m = this.m, r = this.r): V {
		const component = this.hub.state.getCurrentComponent();
		if (!component) return;
		const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
		if (component.hooks.has(hookKey)) {
			return;
		}
		component.hooks.set(hookKey, { value: undefined });

		const atom = this.create(rk, undefined, undefined, s, m, r);
		if (!atom) {
			return;
		}

		const unsubSignal = atom.subscribeExternal(() => {
			this.hub.state.notifyHook(hookKey);
		});
		component.unmount.add(unsubSignal);

		if (component.render) {
			const unsubRender = this.hub.state.subscribeToHook(hookKey, () => component.render?.());
			component.unmount.add(unsubRender);
		}
	}
	public _resolveConflict<T>(conflict: ConflictPolicy | undefined, previous: T, next: T, strategy: "latest" | "fail" | "accumulate" = "latest"): T | null {
		if (conflict?.resolve) {
			return conflict.resolve(previous, next);
		}

		if (strategy === "fail") {
			console.warn('[Sauron] conflict rejected by "fail" strategy');
			return null;
		}
		if (strategy === "accumulate" && Array.isArray(previous) && Array.isArray(next)) {
			return [...previous, ...next] as T;
		}

		return next;
	}
    // #endregion
	//                                                                                                   DISTRIBUTED REACTIVE STATE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public state_setter(v: U, k: A = `_USECELL`, s: S = this.s, m: SU = this.m, r: SU = this.r) {
		const existing = this.hub.state.state.get(k, { s: s, m: m, r: r });
		if (existing !== undefined) return existing;
		const t = typeof v === "function" ? v() : v;
		this.hub.state.state.set(k, t, { triggerRender: true,  s: s, m: m, r: r });
		return t;
	}
	public naked = (() => {
		const fn = <T = A> (k: SignalKey, v?: T | (() => T), s = this.s, m = this.m, r = this.r) =>{
			const registry = this.hub.state.wr.map(s, m, r);
			let atom = registry.get(k);

			if (atom || v === undefined) { return atom }

			const initialValue = typeof v === "function" ? (v as () => T)() : v;
			atom = this.signal<T>(initialValue)
			registry.set(k, atom);
			return atom 
		}
		return fn
	})();
	public useCell = (() => {
		const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r,  ) => {
			const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});

			const senderTokenRef = this.atom(this.hub.REGISTRY._sid.b()); // this.j.root._id.b();

			this.hub.bus.sub((i: A) => {
				const incoming = i.p;
				if (incoming?.from === senderTokenRef.value) return;
				this.hub.state.state.set(k,incoming.value, { triggerRender: false,  s: s, m: m, r: r });
				// set(k, incoming.value, false, s, m, r);
				setState(incoming.value);
			}, k, s, m, r, ); // stay in sync with local writes too (including our own setLive below)
			this.hub.state.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r })
			// sub(k, (newValue: any) => setState(newValue), s, m, r);

			const setLive = (next: any) => {
				const newValue = typeof next === "function" ? next(this.hub.state.state.get(k, { s: s, m: m, r: r })) : next;
				setState(newValue);
				this.hub.state.state.set(k,newValue, { triggerRender: true,  s: s, m: m, r: r });
				// set(k, newValue, true, s, m, r); // local write
				this.hub.bus.send({value: newValue, from: senderTokenRef.value}, k, s, m, r); // broadcast to the bus
			};

			return [state, setLive] as const;
		};
		fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});

			const senderTokenRef = this.atom(this.hub.REGISTRY._sid.b()); 
			const queueRef = this.atom<Queue | null>(null);

			if (!queueRef.current) {
				queueRef.current = new Queue(1);
			}

			// mailbox — replace the whole useEffect block:
			this.hub.bus.sub((i: A) => {
				const incoming = i.p;
				if (incoming?.from === senderTokenRef.value) return;
				queueRef.current!.run(async () => {
					const current = this.hub.state.state.get(k, { s: s, m: m, r: r })
					const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
					this.hub.state.state.set(k,next, { triggerRender: false,  s: s, m: m, r: r });
					return next;
				});
			}, k, s, m, r,);

			this.hub.state.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r })

			const setLive = (next: any) => {
				const t = typeof next === "function" ? next() : next;
				this.hub.bus.send({value: t, from: senderTokenRef.value},k,  s, m, r);
			};

			return [state, setLive] as const;
		};
		return fn
	})();
        // #endregion
        //                                                                                                           ANTI MESSAGE QUEUE
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        public useAntiMsgQ = (() => {
            const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r,  ) => {
                const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});
    
                const senderTokenRef = this.atom(this.hub.REGISTRY._sid.b()); // this.j.root._id.b();
    
                this.hub.bus.sub((i: A) => {
                    const incoming = i.p;
                    if (incoming?.from === senderTokenRef.value) return;
                    this.hub.state.state.set(k,incoming.value, { triggerRender: false,  s: s, m: m, r: r });
                    // set(k, incoming.value, false, s, m, r);
                    setState(incoming.value);
                }, k, s, m, r, ); // stay in sync with local writes too (including our own setLive below)
                this.hub.state.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r })
                // sub(k, (newValue: any) => setState(newValue), s, m, r);
    
                const setLive = (next: any) => {
                    const newValue = typeof next === "function" ? next(this.hub.state.state.get(k, { s: s, m: m, r: r })) : next;
                    setState(newValue);
                    this.hub.state.state.set(k,newValue, { triggerRender: true,  s: s, m: m, r: r });
                    // set(k, newValue, true, s, m, r); // local write
                    this.hub.bus.send({value: newValue, from: senderTokenRef.value},k,  s, m, r); // broadcast to the bus
                };
    
                return [state, setLive] as const;
            };
            fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
                const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});
    
                const senderTokenRef = this.atom(this.hub.REGISTRY._sid.b()); 
                const queueRef = this.atom<Queue | null>(null);
    
                if (!queueRef.current) {
                    queueRef.current = new Queue(1);
                }
    
                // mailbox — replace the whole useEffect block:
                this.hub.bus.sub((i: A) => {
                    const incoming = i.p;
                    if (incoming?.from === senderTokenRef.value) return;
                    queueRef.current!.run(async () => {
                        const current = this.hub.state.state.get(k, { s: s, m: m, r: r })
                        const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
                        this.hub.state.state.set(k,next, { triggerRender: false,  s: s, m: m, r: r });
                        return next;
                    });
                }, k, s, m, r,);
    
                this.hub.state.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r })
    
                const setLive = (next: any) => {
                    const t = typeof next === "function" ? next() : next;
                    this.hub.bus.send({value: t, from: senderTokenRef.value}, k, s, m, r);
                };
    
                return [state, setLive] as const;
            };
            return fn
        })();
        // #endregion
        //                                                                                                                        ASYNC
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        public async = (() => {
            const fn = <T,>(k: A, fetcher: (signal: AbortSignal) => P<T>, s = this.s, m = this.m, r = this.r, options?: { fallback?: T }) => {
                const reg = this._map(s, m, r);
                let atom = reg.get(k) as SIGNAL_CELL<T> | UD;
        
                if (!atom) {
                    atom = this.create(k, options?.fallback as T, { trigger_render: false }, s, m, r) as SIGNAL_CELL<T>;
                }
        
                const dataKey = `${String(k)}:data`;
                const loadingKey = `${String(k)}:loading`;
                const errorKey = `${String(k)}:error`;
        
                if (!this.has(dataKey, s, m, r)) this.create(dataKey, options?.fallback, undefined, s, m, r);
                if (!this.has(loadingKey, s, m, r)) this.create(loadingKey, false, undefined, s, m, r);
                if (!this.has(errorKey, s, m, r)) this.create(errorKey, null, undefined, s, m, r);
        
                const run = async (): Promise<T> => {
                    const state = atom!.async;
                    if (!state) throw new Error(`[Sauron] Missing async state for "${String(k)}"`);
        
                    if (state.controller) state.controller.abort();
                    const controller = new AbortController();
                    state.controller = controller;
        
                    this._writeResolved(loadingKey, true, "write", s, m, r, String(k));
        
                    try {
                        const data = await fetcher(controller.signal);
                        if (!controller.signal.aborted) {
                            this._writeResolved(dataKey, data, "write", s, m, r, String(k));
                            this._writeResolved(errorKey, null, "write", s, m, r, String(k));
                        }
                        return data;
                    } catch (error) {
                        if (!controller.signal.aborted) {
                            this._writeResolved(errorKey, error, "write", s, m, r, String(k));
                            atom!.lifecycle?.onError?.(error as Error);
                        }
                        throw error;
                    } finally {
                        if (!controller.signal.aborted) {
                            this._writeResolved(loadingKey, false, "write", s, m, r, String(k));
                            state.controller = null;
                        }
                    }
                };
        
                atom.async = { fetcher: run, controller: null, retryCount: 0, staleValue: options?.fallback };
                void run();
        
                return { dataKey, loadingKey, errorKey };
            };
            fn.invalidate = <T,>(key: A, s = this.s, m = this.m, r = this.r): V => {
                const reg = this._map(s, m, r);
                const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
                const state = atom?.async;
                if (!atom || !state) return;
        
                const dataKey = `${String(key)}:data`;
                const loadingKey = `${String(key)}:loading`;
                const errorKey = `${String(key)}:error`;
        
                this._writeResolved(loadingKey, true, "write", s, m, r, String(key));
                this._writeResolved(errorKey, null, "write", s, m, r, String(key));
        
                void Promise.resolve(state.fetcher())
                    .then((data) => this._writeResolved(dataKey, data, "write", s, m, r, String(key)))
                    .catch((error) => {
                        this._writeResolved(errorKey, error, "write", s, m, r, String(key));
                        atom.lifecycle?.onError?.(error as Error);
                    });
            };
            fn.mutate = <T,>(key: A, optimistic: T, s = this.s, m = this.m, r = this.r): V => {
                const reg = this._map(s, m, r);
                const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
                const state = atom?.async;
                if (!atom || !state) return;
        
                const dataKey = `${String(key)}:data`;
                const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
                if (!dataAtom) throw new Error(`[Sauron] Missing async data signal "${dataKey}"`);
        
                const prior = dataAtom.peek();
                this._writeResolved(dataKey, optimistic, "write", s, m, r, String(key));
        
                void Promise.resolve(state.fetcher()).catch(() => {
                    this._writeResolved(dataKey, prior, "write", s, m, r, String(key));
                });
            };
            fn.read = <T,>(key: A, s = this.s, m = this.m, r = this.r): T => {
                const reg = this._map(s, m, r);
                const dataKey = `${String(key)}:data`;
                const loadingKey = `${String(key)}:loading`;
                const errorKey = `${String(key)}:error`;
        
                const errorAtom = reg.get(errorKey) as SIGNAL_CELL<A> | UD;
                const error = errorAtom?.peek();
                if (error) throw error;
        
                const loadingAtom = reg.get(loadingKey) as SIGNAL_CELL<boolean> | UD;
                if (loadingAtom?.peek()) {
                    throw new Promise<void>((resolve) => {
                        const unsub = loadingAtom.subscribe((change) => {
                            if (!change.current) {
                                unsub();
                                resolve();
                            }
                        });
                    });
                }
        
                const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
                return dataAtom?.peek() as T;
            };
            return fn;
        })();
        // #endregion
        //                                                                                                                   PERSISTANCE
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        public persist = async <T,>(key: A, value: T, s = this.s, m = this.m, r = this.r, options?: { serialize?: (v: T) => S }) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        public unpersist = async (key: A, s = this.s, m = this.m, r = this.r) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        public transaction = (block: () => V): V => {
            // REDO WITH NEW ATOM SOLUTION
        };
        // #endregion
        //                                                                                                                     TIMELINE
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        public _time_travel_reverting = false;
        public time_travel = (() => {
            const fn = <T = unknown>(key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels(), ): readonly HistoryEntry<T>[] => {
                return this.time_travel.resolveHistory<T>(key, channels) ?? [];
            };
            fn.defaultChannels = (): Required<HistoryChannels> => ({
                r: this.r,
                m: this.m,
                s: '_TC_TIME_TRAVEL_',
            });
            fn.resolveChannels = (channels: HistoryChannels = this.time_travel.defaultChannels(),	): Required<HistoryChannels> => ({
                ...this.time_travel.defaultChannels(),
                ...channels,
            });
            fn.resolveHistory = <T = unknown>( key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels(),	) => {
            
                const _history = this.hub.state.wr._historyMaul.get(key)
                return _history as HistoryEntry[] | undefined;
            };
            fn.replay = <T = unknown>(key: SignalKey, value: T, channels: HistoryChannels, source: string,	): T => {
                const { s, m, r } = this.time_travel.resolveChannels(channels);
                this._time_travel_reverting = true;
                try {
                    this.hub.state.store.set(key, value, { s: s, m: m, r: r, triggerRender: true, });
                    return value;
                } finally {
                    this._time_travel_reverting = false;
                }
            };
            fn.graft = (drs: A) => {
                drs.time_travel = {
                    back: this.time_travel.back,
                    forward: this.time_travel.forward,
                    goto: this.time_travel.goto,
                    history: this.time_travel,
                };
        
                return drs.time_travel;
            };
            fn.back = <T = unknown>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels(),	): T | undefined => {
                const history = this.time_travel.resolveHistory<T>(key, channels);
                if (!history?.length || steps <= 0) return undefined;
            
                const index = this.hub.state.wr._timeTravelMaul.get(key) ?? history.length - 1;
                const targetIndex = Math.max(0, index - steps);
            
                if (targetIndex === index) return undefined;
            
                this.hub.state.wr._timeTravelMaul.set(key, targetIndex);
                for (let i = 0; i < history.length; i++) {
                    history[i].undone = i > targetIndex;
                }
            
                return this.time_travel.replay(key, history[targetIndex].prev as T, channels, "time-travel.back");
            };
            fn.forward = <T = unknown>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels(),	): T | undefined => {
                const history = this.time_travel.resolveHistory<T>(key, channels);
                if (!history?.length || steps <= 0) return undefined;
             
                const index =this.hub.state.wr._timeTravelMaul.get(key) ?? history.length - 1;
                const targetIndex = Math.min(
                    history.length - 1,
                    index + steps,
                );
            
                if (targetIndex === index) return undefined;
            
                meta._timeTravel.set(key, targetIndex);
                for (let i = 0; i < history.length; i++) {
                    history[i].undone = i > targetIndex;
                }
            
                return this.time_travel.replay(key, history[targetIndex].next as T, channels, "time-travel.forward");
            };
            fn.goto = <T = unknown>( key: SignalKey, sequence: number, channels: HistoryChannels = this.time_travel.defaultChannels(),	): T | undefined => {
                const history = this.time_travel.resolveHistory<T>(key, channels);
                if (!history?.length) return undefined;
            
                const targetIndex = history.findIndex((entry) => entry.k === sequence);
                const index = this.hub.state.wr._timeTravelMaul.get(key) ?? history.length - 1;
            
                if (targetIndex < 0 || targetIndex === index) return undefined;
            
                 this.hub.state.wr._timeTravelMaul.set(key, targetIndex);
                for (let i = 0; i < history.length; i++) { history[i].undone = i > targetIndex; }
            
                return this.time_travel.replay(key, history[targetIndex].goto as T, channels, "time-travel.goto");
            };
            return fn;
        })();
        public recording: { entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>; unsubs: Array<() => V> } | null = null;
        public timeline = (() => {
            const fn = (key: A, s = this.s, m = this.m, r = this.r) => {
    
            }
            fn.manualReplay =(entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, options?: { from?: number; speed?: number; triggerRender?: boolean }): Promise<V> => {
                const { from = 0, speed = 0, triggerRender = true } = options ?? {};
                return new Promise((resolve) => {
                    let index = from;
                    const step = () => {
                        if (index >= entries.length) { resolve(); return; }
                        const entry = entries[index++];
                        this._writeResolved(entry.key, entry.value, entry.source ?? "replay", this.s, this.m, this.r, `replay:${index}`);
                        if (speed > 0) { setTimeout(step, speed); }
                         else { step(); }
                    };
                    this.DARTH_MAUL_ENGINE.startBatch();
                    try { step(); } 
                    finally { this.DARTH_MAUL_ENGINE.endBatch(); }
                    if (speed === 0) { resolve(); }
                });
            }
            fn.manualBack = (key: A, s = this.s, m = this.m, r = this.r): void => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.manualForward = (key: A, s = this.s, m = this.m, r = this.r): void => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.manualRecord=(keys?: SignalKey[], s: S = this.s, m: S = this.m, r: S = this.r): () => V =>{
                /** USAGE
                const stop = sauron.record();
            
                // ...user reproduces the bug...
                
                const entries = stop();
                
                // Replay it exactly:
                await sauron.replay(entries);
                
                // Or find where reality diverged:
                sauron.bisect(entries);
                 */
                if (this.recording) {throw new Error("[Sauron] A recording is already active");}
        
                const registry = this._map(s, m, r);
                const targets = keys ?? [...registry.keys()];
        
                const entries: Array<{key: SignalKey; value: A; previous: A; timestamp: number; source?: S; }> = [];
        
                const unsubs = targets.map((key) => {
                    const atom = registry.get(key) as SIGNAL_CELL | undefined;
                    if (!atom) return () => {};
        
                    return atom.subscribe((change) => {
                        entries.push({
                            key,
                            value: change.current,
                            previous: change.previous,
                            timestamp: Date.now(),
                            source: change.source,
                        });
                    });
                });
        
                this.recording = { entries, unsubs };
        
                return () => this.timeline.stopManualRecording();
            }
            fn.stopManualRecording=(): Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> =>{
                if (!this.recording) return [];
        
                for (const unsub of this.recording.unsubs) {
                    unsub();
                }
        
                const entries = this.recording.entries;
                this.recording = null;
        
                return entries;
            }
            return fn;
        })();
        public snapshot = (() => {
            const fn = () => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.restore = (snapshot: Record<string, A>): void => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.diff = (a: Record<string, A>, b: Record<string, A>) => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.applyPatch = (patch: Record<string, { from: A; to: A }>): void => {
                // REDO WITH NEW ATOM SOLUTION
            };
            return fn;
        })();
        public getCausalChain(key: A) {
            // REDO WITH NEW ATOM SOLUTION
        }
        public conflictPolicy<T>(key: A, policy: ConflictPolicy): void {
            // REDO WITH NEW ATOM SOLUTION
        }
        public declareContract(name: string, invariant: (snapshot: Record<string, A>) => boolean, key: A, s = this.s, m = this.m, r = this.r, options?: { block?: boolean }): void {
            // REDO WITH NEW ATOM SOLUTION
        }
        public removeContract(name: string): void {
            // REDO WITH NEW ATOM SOLUTION
        }
        public createScope = (id: string) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        public effectOnce = (keys: A[], predicate: (...values: A[]) => boolean, fn: (...values: A[]) => void) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        // #endregion
        //                                                                                                                   SRC / DEST
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        public readonly SRC_DEFAULTS: rts = { sl: this.s, ms: this.m, rt: this.r };
        public readonly DST_DEFAULTS: rts = { sl: this.s, ms: this.m, rt: this.r };
        public filter = <T,>(srcKey: A, destKey: A, pred: (v: T) => B, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
     
             return this.subscribe<T>(
                 sk,
                 v => {
                     if (pred(v)) this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
                 },
                 src.s,
                 src.m,
                 src.r,
             ); */
        };
        public throttle = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
     
             let last = 0;
             return this.subscribe(
                 sk,
                 v => {
                     const now = Date.now();
                     if (now - last >= m) {
                         last = now;
                         this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
                     }
                 },
                 src.s,
                 src.m,
                 src.r,
             ); */
        };
        public merge = (srcKeys: A[], destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**	const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         const unsubs = srcKeys.map(k => {
             const sk = this.resolveKey(k, src.s, src.m, src.r);
             return this.subscribe(sk, v => this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r), src.s, src.m, src.r);
         });
         return () => unsubs.forEach(u => u()); */
        };
        public zip = (srcKeys: A[], destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**
             * const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
            const emitted = new Map<string, A>();
            const sks = srcKeys.map(k => this.resolveKey(k, src.s, src.m, src.r));
    
            const unsubs = sks.map(sk =>
                this.subscribe(
                    sk,
                    v => {
                        emitted.set(sk, v);
                        if (sks.every(k => emitted.has(k))) {
                            this._writeResolved(
                                dk,
                                sks.map(k => emitted.get(k)),
                                "derived",
                                sk,
                                dest.s,
                                dest.m,
                                dest.r,
                            );
                            emitted.clear();
                        }
                    },
                    src.s,
                    src.m,
                    src.r,
                ),
            );
    
            return () => unsubs.forEach(u => u());
             */
        };
        public scan = <T, A>(srcKey: A, destKey: A, reducer: (a: A, v: T) => A, seed: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**
             * const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
            const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
    
            let acc = seed;
            this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);
    
            return this.subscribe<T>(
                (v: T) => {
                    acc = reducer(acc, v);
                    this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);
                },
                sk,
                src.s,
                src.m,
                src.r,
            );
             */
        };
        public switch = (srcKey: A, destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
     
             let inner: (() => V) | null = null;
             return this.subscribe(
                 sk,
                 (targetKey: A) => {
                     if (inner) inner();
                     const k = this.resolveKey(targetKey, src.s, src.m, src.r);
                     inner = this.subscribe(k, v => this._writeResolved(dk, v, "derived", k, dest.s, dest.m, dest.r), src.s, src.m, src.r);
                 },
                 src.s,
                 src.m,
                 src.r,
             ); */
        };
        public buffer = (srcKey: A, destKey: A, count: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
     
             const buf: A[] = [];
             return this.subscribe(
                 sk,
                 v => {
                     buf.push(v);
                     if (buf.length >= count) {
                         this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
                         buf.length = 0;
                     }
                 },
                 src.s,
                 src.m,
                 src.r,
             ); */
        };
        public window = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
     
             const buf: A[] = [];
             const iv = setInterval(() => {
                 if (buf.length) {
                     this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
                     buf.length = 0;
                 }
             }, m);
     
             const unsub = this.subscribe(sk, v => buf.push(v), src.s, src.m, src.r);
             return () => {
                 unsub();
                 clearInterval(iv);
             }; */
        };
        public distinct = <T,>(srcKey: A, destKey: A, equals?: (a: T, b: T) => B, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**	const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
         const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
    
         let last: T | UD;
         return this.subscribe<T>(
             sk,
             v => {
                 const same = last !== undefined && (equals ? equals(last, v) : Object.is(last, v));
                 if (!same) {
                     last = v;
                     this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
                 }
             },
             src.s,
             src.m,
             src.r,
         ); */
        };
        public audit = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
     
             let timer: RET<typeof setTimeout> | null = null;
             let latest: A;
             return this.subscribe(
                 sk,
                 v => {
                     latest = v;
                     if (!timer) {
                         timer = setTimeout(() => {
                             this._writeResolved(dk, latest, "derived", sk, dest.s, dest.m, dest.r);
                             timer = null;
                         }, m);
                     }
                 },
                 src.s,
                 src.m,
                 src.r,
             ); */
        };
    
        // #endregion
        
}
// #endregion
//                                                                                                            [DARTH VADER] BUS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class DARTH_VADER {
	//                                                                                                                  [CONSTANTS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    private readonly capacity: number;
    private hub: DEATH_STAR
    // ======================================================================
    // Channel table: open-addressed hash of (r, m, s, k)
    // ======================================================================
    private readonly channelMask: number;
    private readonly channelKey: Int32Array;       // hash bucket -> channel id or -1
    private readonly channelState: Uint8Array;     // 0 = empty, 1 = used, 2 = deleted
    private readonly channelHash: Int32Array;      // stored hash to speed probing
    private channelCount = 1;                      // 0 reserved
    // Per-channel subscriber list (intrusive linked list)
    private readonly channelSubHead: Int32Array;
    private readonly channelSubTail: Int32Array;
    // ======================================================================
    // Subscriber pool
    // ======================================================================
    private readonly subHandler: (Handler | undefined)[];
    private readonly subNext: Int32Array;          // next subscriber in channel
    private readonly subPrev: Int32Array;          // prev subscriber in channel
    private readonly subChannel: Int32Array;       // owning channel
    private subCount = 1;                          // 0 reserved
    // ======================================================================
    // Message ring buffer
    // ======================================================================
    private readonly msgChannel: Int32Array;
    private readonly msgPayload: unknown[];
    private readonly msgDeferred: (Deferred | undefined)[];
    private msgHead = 0; // next message to consume
    private msgTail = 0; // next free slot
    private msgCount = 0;
    // Backpressure waiters
    private capacityWaiters: Deferred[] = [];
    // Drain scheduling
    private draining = false;
    private queuedMicrotask = false;
    constructor(DEATH_STAR: DEATH_STAR, capacity = 65_536) {
        this.hub = DEATH_STAR
        if (capacity < 2 || (capacity & (capacity - 1)) !== 0) {
            throw new Error("capacity must be a power of two >= 2");
        }

        this.capacity = capacity;
        this.channelMask = capacity - 1;

        this.channelKey = new Int32Array(capacity).fill(-1);
        this.channelState = new Uint8Array(capacity);
        this.channelHash = new Int32Array(capacity);

        this.channelSubHead = new Int32Array(capacity).fill(NO_SUB);
        this.channelSubTail = new Int32Array(capacity).fill(NO_SUB);

        this.subHandler = new Array(capacity);
        this.subNext = new Int32Array(capacity).fill(NO_SUB);
        this.subPrev = new Int32Array(capacity).fill(NO_SUB);
        this.subChannel = new Int32Array(capacity).fill(-1);

        this.msgChannel = new Int32Array(capacity).fill(-1);
        this.msgPayload = new Array(capacity);
        this.msgDeferred = new Array(capacity);
    }
	// #endregion
	//                                                                                                            [ADDRESS HASHING]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    private hashAddress(k: S | N, s: S | N, m: S | N, r: S | N): N {
        // FNV-1a style; cheap, stable, and allocation-free for primitives.
        let h = 0x811c9dc5;

        const parts = [r, m, s, k];

        for (let i = 0; i < 4; i++) {
            const part = parts[i];
            const str = typeof part === "string" ? part : String(part);

            for (let j = 0; j < str.length; j++) {
                h ^= str.charCodeAt(j);
                h = Math.imul(h, 0x01000193);
            }

            // Separator reduces collision chance across tiers.
            h ^= 0x9e3779b9;
            h = Math.imul(h, 0x01000193);
        }

        return h | 0;
    }
	// #endregion
	//                                                                                                                   [CHANNELS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    private findChannel(hash: N): N {
        let slot = hash & this.channelMask;

        while (true) {
            const state = this.channelState[slot];

            if (state === 0) return -1;

            if (
                state === 1 &&
                this.channelHash[slot] === hash
            ) {
                return this.channelKey[slot];
            }

            slot = (slot + 1) & this.channelMask;
        }
    }
    private createChannel(hash: N): N {
        if (this.channelCount >= this.capacity) {
            throw new Error("MESSAGE_BUS_DISRUPTOR: channel table full");
        }

        let slot = hash & this.channelMask;

        while (this.channelState[slot] === 1) {
            slot = (slot + 1) & this.channelMask;
        }

        const channelId = this.channelCount++;

        this.channelState[slot] = 1;
        this.channelHash[slot] = hash;
        this.channelKey[slot] = channelId;

        this.channelSubHead[channelId] = NO_SUB;
        this.channelSubTail[channelId] = NO_SUB;

        return channelId;
    }
    private getOrCreateChannel(k: S | N, s: S | N, m: S | N, r: S | N): N {
        const hash = this.hashAddress(r, m, s, k);
        const existing = this.findChannel(hash);

        return existing !== -1 ? existing : this.createChannel(hash);
    }
	// #endregion
	//                                                                                                                        [SUB]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    public subscribe(handler: Handler, k: S | N, s: S | N, m: S | N, r: S | N): () => V {
        if (typeof handler !== "function") {
            throw new TypeError("handler must be a function");
        }

        const channelId = this.getOrCreateChannel(r, m, s, k);
        const subId = this.subCount++;

        if (subId >= this.capacity) {
            throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
        }

        this.subHandler[subId] = handler;
        this.subChannel[subId] = channelId;
        this.subNext[subId] = NO_SUB;
        this.subPrev[subId] = NO_SUB;

        const tail = this.channelSubTail[channelId];

        if (tail !== NO_SUB) {
            this.subNext[tail] = subId;
            this.subPrev[subId] = tail;
            this.channelSubTail[channelId] = subId;
        } else {
            this.channelSubHead[channelId] = subId;
            this.channelSubTail[channelId] = subId;
        }

        let disposed = false;

        return () => {
            if (disposed) return;
            disposed = true;

            this.removeSubscriber(subId);
        };
    }
    private removeSubscriber(subId: N): V {
        const channelId = this.subChannel[subId];

        if (channelId === -1) return;

        const prev = this.subPrev[subId];
        const next = this.subNext[subId];

        if (prev !== NO_SUB) this.subNext[prev] = next;
        else this.channelSubHead[channelId] = next;

        if (next !== NO_SUB) this.subPrev[next] = prev;
        else this.channelSubTail[channelId] = prev;

        this.subHandler[subId] = undefined;
        this.subChannel[subId] = -1;
        this.subNext[subId] = NO_SUB;
        this.subPrev[subId] = NO_SUB;
    }
	// #endregion
	//                                                                                                                       [SEND]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    /** Fire-and-forget enqueue. Never awaits handler results. */
    public post(r: S | N, m: S | N, s: S | N, k: S | N, payload?: U, ): V {
        const channelId = this.getOrCreateChannel(r, m, s, k);
        this.enqueue(channelId, payload, undefined);
    }
    /** Enqueue and await subscriber result(s). */
    public async send(r: S | N, m: S | N, s: S | N, k: S | N, payload?: U, ): P<U> {
        const channelId = this.getOrCreateChannel(r, m, s, k);

        // Backpressure: wait for ring space instead of dropping messages.
        while (this.msgCount >= this.capacity) {
            await this.waitForCapacity();
        }

        let resolve!: (value: unknown) => void;
        let reject!: (reason?: unknown) => void;

        const deferred: Deferred = {
            resolve: (v) => resolve(v),
            reject: (e) => reject(e),
        };

        const promise = new Promise<unknown>((res, rej) => {
            resolve = res;
            reject = rej;
        });

        deferred.resolve = resolve;
        deferred.reject = reject;

        this.enqueue(channelId, payload, deferred);

        return promise;
    }
    private async waitForCapacity(): P<V> {
        let resolve!: () => V;
        let reject!: (reason?: U) => V;

        const deferred: Deferred = {
            resolve: () => resolve(),
            reject: (e) => reject(e),
        };

        const promise = new Promise<void>((res, rej) => {
            resolve = res;
            reject = rej;
        });

        deferred.resolve = resolve;
        deferred.reject = reject;

        this.capacityWaiters.push(deferred);

        await promise;
    }
    private enqueue(channelId: N, payload: U,deferred: Deferred | UD, ): V {
        if (this.msgCount >= this.capacity) {
            throw new Error("MESSAGE_BUS_DISRUPTOR: ring full");
        }

        const slot = this.msgTail;

        this.msgChannel[slot] = channelId;
        this.msgPayload[slot] = payload;
        this.msgDeferred[slot] = deferred;

        this.msgTail = (this.msgTail + 1) % this.capacity;
        this.msgCount++;

        this.scheduleDrain();
    }
	// #endregion
	//                                                                                                            [Stream / Duplex]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    /**
     * One-way continuous stream.
     *
     * Returns a sender function. Every call pushes a message to every
     * subscriber handler attached to the same address.
     */
    public stream(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>,): (payload?: U) => V {
            const channelId = this.getOrCreateChannel(r, m, s, k);

        // Register consumer handler as a normal subscriber.
        this.subscribeRaw(channelId, handler);

        // Sender pushes to the same channel; fire-and-forget.
        return (payload?: unknown) => {
            this.enqueueStream(channelId, payload);
        };
    }
    public openStream(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>,): { send: (payload?: U) => V; close: () => V;} {
        const channelId = this.getOrCreateChannel(r, m, s, k);

        // Attach consumer handler (this end)
        const subId = this.subscribeRaw(channelId, handler);

        let closed = false;

        const send = (payload?: unknown) => {
            if (closed) return;
            this.enqueueStream(channelId, payload);
        };

        const close = () => {
            if (closed) return;
            closed = true;
            this.removeSubscriber(subId);
        };

        return { send, close };
    }
    /**
     * Constant two-way stream.
     *
     * Each endpoint can send and receive simultaneously.
     * The handler receives inbound messages; `send()` pushes outbound messages.
     */
    public duplex(r: S | N, m: S | N, s: S | N,k: S | N, handler: (payload: U) => V | P<V>,): { send: (payload?: U) => V; close: () => V;} {
        // Create two directional channels:
        //   outbound: this endpoint -> peer
        //   inbound:  peer -> this endpoint
        const outboundChannel = this.getOrCreateChannel(
            r, m, s, `${k}:out`,
        );

        const inboundChannel = this.getOrCreateChannel(
            r, m, s, `${k}:in`,
        );

        // Register this endpoint's inbound handler.
        this.subscribeRaw(inboundChannel, handler);

        let closed = false;

        const send = (payload?: unknown) => {
            if (closed) return;
            this.enqueueStream(outboundChannel, payload);
        };

        const close = () => {
            if (closed) return;
            closed = true;

            // Remove this endpoint's inbound handler.
            // (Simple approach: remove all handlers on inbound channel.)
            this.removeAllSubscribers(inboundChannel);
        };

        return { send, close };
    }
	// #endregion
	//                                                                                                           [Internal helpers]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    private subscribeRaw(channelId: N, handler: (payload: U) => U | P<U>,): N {
        const subId = this.subCount++;

        if (subId >= this.capacity) {
            throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
        }

        this.subHandler[subId] = handler;
        this.subChannel[subId] = channelId;
        this.subNext[subId] = NO_SUB;
        this.subPrev[subId] = NO_SUB;

        const tail = this.channelSubTail[channelId];

        if (tail !== NO_SUB) {
            this.subNext[tail] = subId;
            this.subPrev[subId] = tail;
            this.channelSubTail[channelId] = subId;
        } else {
            this.channelSubHead[channelId] = subId;
            this.channelSubTail[channelId] = subId;
        }

        return subId;
    }
    private removeAllSubscribers(channelId: number): void {
        let sub = this.channelSubHead[channelId];

        while (sub !== NO_SUB) {
            const next = this.subNext[sub];
            this.removeSubscriber(sub);
            sub = next;
        }

        this.channelSubHead[channelId] = NO_SUB;
        this.channelSubTail[channelId] = NO_SUB;
    }
    private enqueueStream(channelId: number, payload: unknown): void {
        if (this.msgCount >= this.capacity) {
            // Drop oldest message to make room (lossy but non-blocking).
            this.msgHead = (this.msgHead + 1) % this.capacity;
            this.msgCount--;
        }

        const slot = this.msgTail;

        this.msgChannel[slot] = channelId;
        this.msgPayload[slot] = payload;
        this.msgDeferred[slot] = undefined; // no per-message deferred

        this.msgTail = (this.msgTail + 1) % this.capacity;
        this.msgCount++;

        this.scheduleDrain();
    }
	// #endregion
	//                                                                                                              [Introspection]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    public get pending(): N { return this.msgCount; }
    public get subscriberCount(): N { return this.subCount - 1; }
    public get getChannelCount(): N { return this.channelCount - 1; }
	// #endregion
	//                                                                                                                 [Drain loop]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    private scheduleDrain(): V {
        if (this.queuedMicrotask || this.draining) return;

        this.queuedMicrotask = true;

        queueMicrotask(() => {
            this.queuedMicrotask = false;
            this.drain();
        });
    }
    private async drain(): P<V> {
        if (this.draining) return;

        this.draining = true;

        try {
            while (this.msgCount > 0) {
                const slot = this.msgHead;
                const channelId = this.msgChannel[slot];
                const payload = this.msgPayload[slot];
                const deferred = this.msgDeferred[slot];

                // Advance ring before handler execution so a handler that
                // sends again cannot deadlock the ring.
                this.msgHead = (this.msgHead + 1) % this.capacity;
                this.msgCount--;

                this.msgChannel[slot] = -1;
                this.msgPayload[slot] = undefined;
                this.msgDeferred[slot] = undefined;

                const result = await this.dispatch(channelId, payload);

                if (deferred) {
                    deferred.resolve(result);
                }

                this.notifyCapacityWaiters();
            }
        } finally {
            this.draining = false;

            // If messages arrived while draining, schedule another pass.
            if (this.msgCount > 0) {
                this.scheduleDrain();
            }
        }
    }
    private async dispatch(channelId: N, payload: U, ): P<U> {
        let sub = this.channelSubHead[channelId];

        if (sub === NO_SUB) return undefined;

        // Fast path: exactly one subscriber.
        if (this.subNext[sub] === NO_SUB) {
            const handler = this.subHandler[sub];

            if (!handler) return undefined;

            return await handler(payload);
        }

        // Multiple subscribers: preserve subscription order.
        const results: unknown[] = [];
        const promises: Promise<unknown>[] = [];
        let hasAsync = false;

        while (sub !== NO_SUB) {
            const handler = this.subHandler[sub];

            if (handler) {
                const result = handler(payload);

                if (
                    result &&
                    typeof (result as PromiseLike<unknown>).then === "function"
                ) {
                    hasAsync = true;
                    promises.push(result as Promise<unknown>);
                    results.push(undefined);
                } else {
                    results.push(result);
                }
            }

            sub = this.subNext[sub];
        }

        if (hasAsync) {
            const settled = await Promise.all(promises);

            // Merge async results back into their original slots.
            let promiseIndex = 0;

            for (let i = 0; i < results.length; i++) {
                if (results[i] === undefined && promiseIndex < settled.length) {
                    results[i] = settled[promiseIndex++];
                }
            }
        }

        return results;
    }
    private notifyCapacityWaiters(): V {
        if (this.capacityWaiters.length === 0) return;

        const waiters = this.capacityWaiters;
        this.capacityWaiters = [];

        for (const waiter of waiters) {
            waiter.resolve(undefined);
        }
    }
	// #endregion
	//                                                                                                               [Drain loop]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
}
// #endregion
//                                                                                                                [RAVEN] STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

type FlushCallback = () => void;
type Priority = "immediate" | "user-blocking" | "normal" | "background" | "idle";
interface HistoryEntry {
    k: number;
    prev: A;
    next: A;
    ts: number;
    undone: boolean;
}
// ============================================================================
// Constants
// ============================================================================
const PRIORITY_IMMEDIATE = 0;
const PRIORITY_USER_BLOCKING = 1;
const PRIORITY_NORMAL = 2;
const PRIORITY_BACKGROUND = 3;
const PRIORITY_IDLE = 4;
const PRIORITY_COUNT = 5;
const PRIORITY_NAMES: Priority[] = [    "immediate", "user-blocking", "normal", "background", "idle",];
const HOOK_NONE = 0;
const HOOK_STATE = 1;
const HOOK_EFFECT = 2;
const HOOK_REF = 3;
const HOOK_MEMO = 4;
const FLAG_NONE = 0;
const FLAG_MOUNTED = 1 << 0;
const FLAG_DISPOSED = 1 << 1;
const FLAG_RENDER_PENDING = 1 << 2;
const FLAG_HAS_BINDINGS = 1 << 3;
// ============================================================================
// SAURON_DISRUPTOR
// ============================================================================
 
export class RAVEN {
    public hub: DEATH_STAR;
    public wr: RAVENS_WAR_ROOOM
    constructor(root: DEATH_STAR = null, capacity = 65_536) {
        this.hub = root;
        this.cap = capacity;
        this.mask = capacity - 1;

        this._initPools(capacity);
        this.wr = new RAVENS_WAR_ROOOM(this)
    }

    public online = false;
    public r: A = "THE_WIZARD_ROOT_REG";
    public m: A = "SAURON_MASTER_REG";
    public s: A = "SAURON_SLAVE_REG";
    public k: A = "DARK_STATE";
    public u: A = undefined;
    public GLOBAL_SCOPE = Symbol("global-scope");
    public externalSubscribers = new Set<() => void>();
    public _externalSubscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
    // ========================================================================
    // Pool capacity
    // ========================================================================
    private cap: N;
    private mask: N;
    // ========================================================================
    // Component pool — id hash → pid, all state in columns
    // ========================================================================
    private compSlotState: U8;
    private compSlotHash: I32;
    private compSlotPid: I32;
    private compRender: (RenderFn | UD)[];
    private compFlags: U8;
    private compParent: I32;
    private compDepth: U8;
    private compUnmountHead: I32;
    private compUnmountTail: I32;
    private compContextHead: I32;
    private compContextTail: I32;
    private compCount = 1;
    // ========================================================================
    // Hook pool — (componentPid, hookNameHash, sauronIdHash) → hookPid
    // ========================================================================
    private hookSlotState: U8;
    private hookSlotHash: I32;
    private hookSlotPid: I32;
    private hookKind: U8;
    private hookValue: U[];
    private hookPending: U[];
    private hookVersion: I32;
    private hookComponent: I32;
    private hookSlot: I32;
    private hookFn: (A)[];
    private hookCleanup: ((CleanupFn | UD))[];
    private hookBindingHead: I32;
    private hookBindingTail: I32;
    private hookDomRefHead: I32;
    private hookDomRefTail: I32;
    private hookDeps: (A[] | undefined)[];
    private hookCount = 1;
    // ========================================================================
    // Binding pool — intrusive doubly-linked list per hook
    // ========================================================================
    private bindNode: (Node | undefined)[];
    private bindUpdate: (((node: Node, value: A) => void) | undefined)[];
    private bindActive: U8;
    private bindHook: I32;
    private bindPrev: I32;
    private bindNext: I32;
    private bindCount = 1;
    // ========================================================================
    // DOM ref pool — per hook
    // ========================================================================
    private refName: string[];
    private refNode: (Node | undefined)[];
    private refHook: I32;
    private refNext: I32;
    private refPrev: I32;
    private refCount = 1;
    // ========================================================================
    // Cleanup (unmount) pool — intrusive list per component
    // ========================================================================
    private cleanupFn: ((() => void) | undefined)[];
    private cleanupComponent: I32;
    private cleanupPrev: I32;
    private cleanupNext: I32;
    private cleanupCount = 1;
    // ========================================================================
    // Hook subscriber pool — hookKeyHash → callback linked list
    // ========================================================================
    private hookSubFn: ((() => void) | undefined)[];
    private hookSubKeyHash: I32;
    private hookSubPrev: I32;
    private hookSubNext: I32;
    private hookSubHead: I32;
    private hookSubTail: I32;
    private hookSubCount = 1;
    // ========================================================================
    // State cell pool — (s, m, r, key) → cellId
    // ========================================================================
    private cellSlotState: U8;
    private cellSlotHash: I32;
    private cellSlotPid: I32;
    private cellKey: (string | symbol)[];
    private cellValue: unknown[];
    private cellVersion: I32;
    private cellPersisted: U8;
    private cellHasWritten: U8;
    private cellHistory: (HistoryEntry[] | undefined)[];
    private cellDirty: U8;
    private cellGlobalDirty: U8;
    // Listener pool (per cell)
    private listenerFn: ((value: A) => void)[];
    private listenerNext: I32;
    private listenerPrev: I32;
    private listenerCell: I32;
    private cellListenerHead: I32;
    private cellListenerTail: I32;
    // Waiter pool (per cell)
    private waiterResolve: ((value: A) => void)[];
    private waiterNext: I32;
    private waiterCell: I32;
    private cellWaiterHead: I32;
    private cellWaiterTail: I32;
    // Global listener pool
    private globalFn: (((key: string | symbol, value: A) => void) | undefined)[];
    private globalNext: I32;
    private globalPrev: I32;
    private globalHead = -1;
    private globalTail = -1;
    private cellCount = 1;
    private listenerCount = 1;
    private waiterCount = 1;
    private globalCount = 1;
    // ========================================================================
    // State subscriber pool (4-tier: r, m, s, k) — for store2/state2
    // ========================================================================
    private stateSubFn: (((value: A) => void) | undefined)[];
    private stateSubHash: I32;
    private stateSubPrev: I32;
    private stateSubNext: I32;
    private stateSubHead: I32;
    private stateSubTail: I32;
    private stateSubCount = 1;
    // ========================================================================
    // External subscriber pool (4-tier) — for store2 delete/clear
    // ========================================================================
    private extSubFn: (((value: A) => void) | undefined)[];
    private extSubHash: I32;
    private extSubPrev: I32;
    private extSubNext: I32;
    private extSubHead: I32;
    private extSubTail: I32;
    private extSubCount = 1;
    // Scheduler — pooled tasks, per-priority per-scope FIFO
    // ========================================================================
    private taskFn: (() => void)[];
    private taskPriority: U8;
    private taskScopeHash: I32;
    private taskSignal: (AbortSignal | undefined)[];
    private taskCancelled: U8;
    private taskPrev: I32;
    private taskNext: I32;
    // Per-priority per-scope queue: head/tail/count indexed by [priority * cap + scopeSlot]
    private queueHead: I32;
    private queueTail: I32;
    private queueCount: U8;
    private scopeSlotState: U8;
    private scopeSlotHash: I32;
    private scopeSlotId: I32;
    private scopeSlotCount = 1;
    private taskCount = 1;
    // Scope cursors for round-robin fairness
    private scopeCursors: I32 = new I32(PRIORITY_COUNT);
    // ========================================================================
    // Batch scope pool — async-safe, spans awaits
    // ========================================================================
    private batchScopeActive: U8;
    private batchScopeCount = 1;
    // ========================================================================
    // Dirty callback pool — replaces dirty: Set<FlushCallback>
    // ========================================================================
    private dirtyCbFn: ((() => void) | undefined)[];
    private dirtyCbPrev: I32;
    private dirtyCbNext: I32;
    private dirtyCbHead = -1;
    private dirtyCbTail = -1;
    private dirtyCbCount = 1;
    // Dirty lane — deduped cell notification queue
    // ========================================================================
    private dirtyLane: I32;
    private inDirtyLane: U8;
    private dirtyLaneLen = 0;
    // ========================================================================
    // Hook notification lane — replaces pendingNotifications: Set<string>
    // ========================================================================
    private hookNotifyLane: I32;
    private inHookNotifyLane: U8;
    private hookNotifyLaneLen = 0;
    // ========================================================================
    // Render lane — component re-render queue
    // ========================================================================
    private renderLane: I32;
    private  inRenderLane: U8;
    private renderLaneLen = 0;
    // ========================================================================
    // Effect queue
    // ========================================================================
    private  effectQueue: I32;
    private effectQueueLen = 0;
    // ========================================================================
    // Runtime state
    // ========================================================================
    public compId: Array<S> = [];
    public hookKey: Array<S> = [];
    public batchDepth = 0;
    public _flushScheduled = false;
    public _adapter: A | NU = null;
    public currentScope: S | SY = "global-scope";
    public loopScheduled = false;
    public _currentRenderInstanceCheck: A = null;
    public  instanceId = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    private compInstance: Array<ComponentInstance | undefined> = [];
    private hookKeyToPid = new Map<string, number>();
    private hookPidToKey = new Map<number, string>();
    public _components = new Map<string, ComponentInstance>();
    public _subscribers = new Map<string, Set<() => void>>();
    public _pendingNotifications = new Set<string>();
    public _currentHookState: HookState | null = null;
    public _currentComponent: ComponentInstance | null = null;
    public _reactHookCursor = 0;
    private scheduleRender(componentPid: Pid): void {
        if (this.inRenderLane[componentPid]) return;

        if (this.renderLaneLen >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: render lane full");
        }

        this.inRenderLane[componentPid] = 1;
        this.compFlags[componentPid] |= FLAG_RENDER_PENDING;
        this.renderLane[this.renderLaneLen++] = componentPid;

        this.scheduleDrain();
    }
    public flushHookKey = (key: S): V => {
        const keyHash = this.hashString(key);
        const slot = keyHash & this.mask;

        if (key.startsWith("store:")) {
            let sid = this.hookSubHead[slot];

            while (sid !== -1) {
                const next = this.hookSubNext[sid];
                const fn = this.hookSubFn[sid];

                if (fn) {
                    fn();
                    this.onFlush(fn);
                }

                sid = next;
            }

            return;
        }

        const hookPid = this.findHookByKey(key);

        if (hookPid !== -1) {
            const componentPid = this.hookComponent[hookPid];

            // Direct DOM binding update — hot path.
            if (this.hookBindingHead[hookPid] !== -1) {
                let bid = this.hookBindingHead[hookPid];

                while (bid !== -1) {
                    const next = this.bindNext[bid];

                    if (this.bindActive[bid]) {
                        const update = this.bindUpdate[bid];
                        const node = this.bindNode[bid];

                        if (update && node) {
                            update(node, this.hookValue[hookPid]);
                        }
                    }

                    bid = next;
                }

                return;
            }

            // No bindings → schedule component re-render via render lane.
            this.scheduleRender(componentPid);
            return;
        }

        // Fallback: subscriber callbacks.
        let sid = this.hookSubHead[slot];

        while (sid !== -1) {
            const next = this.hookSubNext[sid];
            const fn = this.hookSubFn[sid];

            if (fn) {
                fn();
                this.onFlush(fn);
            }

            sid = next;
        }
    };   
    public flushHookKey1 = (key: S): V => {
        const keyHash = this.hashString(key);
        const slot = keyHash & this.mask;

        // Store synthetic keys: notify subscribers, re-arm onFlush.
        if (key.startsWith("store:")) {
            let sid = this.hookSubHead[slot];

            while (sid !== -1) {
                const next = this.hookSubNext[sid];
                const fn = this.hookSubFn[sid];

                if (fn) {
                    fn();
                    this.onFlush(fn);
                }

                sid = next;
            }

            return;
        }

        // Component hook: direct DOM binding first, then subscribers.
        const hookPid = this.findHookByStringKey(key);

        if (hookPid !== -1) {
            // Direct DOM binding update.
            if (this.hookBindingHead[hookPid] !== -1) {
                let bid = this.hookBindingHead[hookPid];

                while (bid !== -1) {
                    const next = this.bindNext[bid];

                    if (this.bindActive[bid]) {
                        const update = this.bindUpdate[bid];
                        const node = this.bindNode[bid];

                        if (update && node) {
                            update(node, this.hookValue[hookPid]);
                        }
                    }

                    bid = next;
                }

                return;
            }
        }

        // Fallback: subscriber callbacks.
        let sid = this.hookSubHead[slot];

        while (sid !== -1) {
            const next = this.hookSubNext[sid];
            const fn = this.hookSubFn[sid];

            if (fn) {
                fn();
                this.onFlush(fn);
            }

            sid = next;
        }
    };
    public pass(): void {
        const len = this.renderLaneLen;
        let writeIdx = 0;
    
        for (let i = 0; i < len; i++) {
            const componentPid = this.renderLane[i];
    
            this.inRenderLane[componentPid] = 0;
            this.compFlags[componentPid] &= ~FLAG_RENDER_PENDING;
    
            const instance = this.compInstance[componentPid];
            const render = instance?.render ?? this.compRender[componentPid];
    
            if (instance && render && !(this.compFlags[componentPid] & FLAG_DISPOSED)) {
                this._reactHookCursor = 0;
                this._currentComponent = instance;
    
                try {
                    render();
                } finally {
                    this._currentComponent = null;
                }
            }
    
            this.renderLane[writeIdx++] = componentPid;
        }
    
        this.renderLaneLen = writeIdx;
    }
    // ========================================================================
    // Pool initialization
    // ========================================================================
    private _initPools(cap: N): V {
        // Component pool
        this.compSlotState = new Uint8Array(cap);
        this.compSlotHash = new Int32Array(cap);
        this.compSlotPid = new Int32Array(cap).fill(-1);
        this.compRender = new Array(cap);
        this.compFlags = new Uint8Array(cap);
        this.compParent = new Int32Array(cap).fill(-1);
        this.compDepth = new Uint8Array(cap);
        this.compUnmountHead = new Int32Array(cap).fill(-1);
        this.compUnmountTail = new Int32Array(cap).fill(-1);
        this.compContextHead = new Int32Array(cap).fill(-1);
        this.compContextTail = new Int32Array(cap).fill(-1);

        // Hook pool
        this.hookSlotState = new Uint8Array(cap);
        this.hookSlotHash = new Int32Array(cap);
        this.hookSlotPid = new Int32Array(cap).fill(-1);
        this.hookKind = new Uint8Array(cap);
        this.hookValue = new Array(cap);
        this.hookPending = new Array(cap);
        this.hookVersion = new Int32Array(cap);
        this.hookComponent = new Int32Array(cap).fill(-1);
        this.hookSlot = new Int32Array(cap).fill(-1);
        this.hookFn = new Array(cap);
        this.hookCleanup = new Array(cap);
        this.hookBindingHead = new Int32Array(cap).fill(-1);
        this.hookBindingTail = new Int32Array(cap).fill(-1);
        this.hookDomRefHead = new Int32Array(cap).fill(-1);
        this.hookDomRefTail = new Int32Array(cap).fill(-1);
        this.hookDeps = new Array(cap);

        // Binding pool
        this.bindNode = new Array(cap);
        this.bindUpdate = new Array(cap);
        this.bindActive = new Uint8Array(cap);
        this.bindHook = new Int32Array(cap).fill(-1);
        this.bindPrev = new Int32Array(cap).fill(-1);
        this.bindNext = new Int32Array(cap).fill(-1);

        // Ref pool
        this.refName = new Array(cap);
        this.refNode = new Array(cap);
        this.refHook = new Int32Array(cap).fill(-1);
        this.refNext = new Int32Array(cap).fill(-1);
        this.refPrev = new Int32Array(cap).fill(-1);

        // Cleanup pool
        this.cleanupFn = new Array(cap);
        this.cleanupComponent = new Int32Array(cap).fill(-1);
        this.cleanupPrev = new Int32Array(cap).fill(-1);
        this.cleanupNext = new Int32Array(cap).fill(-1);

        // Hook subscriber pool
        this.hookSubFn = new Array(cap);
        this.hookSubKeyHash = new Int32Array(cap);
        this.hookSubPrev = new Int32Array(cap).fill(-1);
        this.hookSubNext = new Int32Array(cap).fill(-1);
        this.hookSubHead = new Int32Array(cap).fill(-1);
        this.hookSubTail = new Int32Array(cap).fill(-1);

        // State cell pool
        this.cellSlotState = new Uint8Array(cap);
        this.cellSlotHash = new Int32Array(cap);
        this.cellSlotPid = new Int32Array(cap).fill(-1);
        this.cellKey = new Array(cap);
        this.cellValue = new Array(cap);
        this.cellVersion = new Int32Array(cap);
        this.cellPersisted = new Uint8Array(cap);
        this.cellHasWritten = new Uint8Array(cap);
        this.cellHistory = new Array(cap);
        this.cellDirty = new Uint8Array(cap);
        this.cellGlobalDirty = new Uint8Array(cap);

        // Listener pool
        this.listenerFn = new Array(cap);
        this.listenerNext = new Int32Array(cap).fill(-1);
        this.listenerPrev = new Int32Array(cap).fill(-1);
        this.listenerCell = new Int32Array(cap).fill(-1);
        this.cellListenerHead = new Int32Array(cap).fill(-1);
        this.cellListenerTail = new Int32Array(cap).fill(-1);

        // Waiter pool
        this.waiterResolve = new Array(cap);
        this.waiterNext = new Int32Array(cap).fill(-1);
        this.waiterCell = new Int32Array(cap).fill(-1);
        this.cellWaiterHead = new Int32Array(cap).fill(-1);
        this.cellWaiterTail = new Int32Array(cap).fill(-1);

        // Global listener pool
        this.globalFn = new Array(cap);
        this.globalNext = new Int32Array(cap).fill(-1);
        this.globalPrev = new Int32Array(cap).fill(-1);

        // State subscriber pool (4-tier)
        this.stateSubFn = new Array(cap);
        this.stateSubHash = new Int32Array(cap);
        this.stateSubPrev = new Int32Array(cap).fill(-1);
        this.stateSubNext = new Int32Array(cap).fill(-1);
        this.stateSubHead = new Int32Array(cap).fill(-1);
        this.stateSubTail = new Int32Array(cap).fill(-1);

        // External subscriber pool (4-tier)
        this.extSubFn = new Array(cap);
        this.extSubHash = new Int32Array(cap);
        this.extSubPrev = new Int32Array(cap).fill(-1);
        this.extSubNext = new Int32Array(cap).fill(-1);
        this.extSubHead = new Int32Array(cap).fill(-1);
        this.extSubTail = new Int32Array(cap).fill(-1);

        // Scheduler pools
        this.taskFn = new Array(cap);
        this.taskPriority = new Uint8Array(cap);
        this.taskScopeHash = new Int32Array(cap);
        this.taskSignal = new Array(cap);
        this.taskCancelled = new Uint8Array(cap);
        this.taskPrev = new Int32Array(cap).fill(-1);
        this.taskNext = new Int32Array(cap).fill(-1);

        const queueSize = PRIORITY_COUNT * cap;
        this.queueHead = new Int32Array(queueSize).fill(-1);
        this.queueTail = new Int32Array(queueSize).fill(-1);
        this.queueCount = new Int32Array(queueSize);

        this.scopeSlotState = new Uint8Array(cap);
        this.scopeSlotHash = new Int32Array(cap);
        this.scopeSlotId = new Int32Array(cap).fill(-1);
        this.scopeSlotCount = 1;

        // Batch scope pool
        this.batchScopeActive = new Uint8Array(cap);

        // Dirty callback pool
        this.dirtyCbFn = new Array(cap);
        this.dirtyCbPrev = new Int32Array(cap).fill(-1);
        this.dirtyCbNext = new Int32Array(cap).fill(-1);

        // Hook notify lane
        this.hookNotifyLane = new Int32Array(cap).fill(-1);
        this.inHookNotifyLane = new Uint8Array(cap);

        // Render lane
        this.renderLane = new Int32Array(cap).fill(-1);
        this.inRenderLane = new Uint8Array(cap);

        // Effect queue
        this.effectQueue = new Int32Array(cap);
        this.compId = new Array(cap);
        this.hookKey = new Array(cap);
    }
    // ========================================================================
    // Hash helpers
    // ========================================================================
    private hashString(s: S | SY): N {
        const str = typeof s === "string" ? s : String(s);
        let h = 0x811c9dc5;

        for (let i = 0; i < str.length; i++) {
            h ^= str.charCodeAt(i);
            h = Math.imul(h, 0x01000193);
        }

        return h | 0;
    }
    private hash4(a: S | SY, b: S | SY, c: S | SY, d: S | SY, ): N {
        let h = 0x811c9dc5;
        const parts = [a, b, c, d];

        for (let i = 0; i < 4; i++) {
            const str = String(parts[i]);
            for (let j = 0; j < str.length; j++) {
                h ^= str.charCodeAt(j);
                h = Math.imul(h, 0x01000193);
            }
            h ^= 0x9e3779b9;
            h = Math.imul(h, 0x01000193);
        }

        return h | 0;
    }
    // ========================================================================
    // Component pool
    // ========================================================================
    private findComponent(id: S): Pid {
        const hash = this.hashString(id);
        let slot = hash & this.mask;

        while (this.compSlotState[slot] === 1) {
            if (this.compSlotHash[slot] === hash) {
                return this.compSlotPid[slot];
            }
            slot = (slot + 1) & this.mask;
        }

        return -1;
    }
    private createComponentSlot(id: S, pid: Pid): V {
        const hash = this.hashString(id);
        let slot = hash & this.mask;

        while (this.compSlotState[slot] === 1) { slot = (slot + 1) & this.mask; }
        this.compId[pid] = id;
        this.compSlotState[slot] = 1;
        this.compSlotHash[slot] = hash;
        this.compSlotPid[slot] = pid;
    }
    public mountComponent(id: S, renderFn?: RenderFn): ComponentInstance {
        const pid = this.compCount++;
    
        if (pid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: component capacity exhausted");
        }
    
        this.compRender[pid] = renderFn;
        this.compFlags[pid] = FLAG_MOUNTED;
        this.compParent[pid] = -1;
        this.compDepth[pid] = 0;
        this.compUnmountHead[pid] = -1;
        this.compUnmountTail[pid] = -1;
        this.compContextHead[pid] = -1;
        this.compContextTail[pid] = -1;
    
        this.createComponentSlot(id, pid);
    
        const instance = this.makeComponentInstance(pid, id, renderFn);
        this._components.set(id, instance);
    
        return instance;
    }
    private getBindingSetForHook(pid: Pid): Set<A> {
        const set = new Set<A>();
        let bid = this.hookBindingHead[pid];

        while (bid !== -1) {
            set.add({
                node: this.bindNode[bid],
                update: this.bindUpdate[bid],
                active: this.bindActive[bid] === 1,
            });
            bid = this.bindNext[bid];
        }

        return set;
    }
    private getDomRefMapForHook(pid: Pid): Map<S, Node> {
        const map = new Map<string, Node>();
        let rid = this.hookDomRefHead[pid];

        while (rid !== -1) {
            map.set(this.refName[rid], this.refNode[rid]!);
            rid = this.refNext[rid];
        }

        return map;
    }
    public unmountComponent(id: S): void {
        const instance = this._components.get(id);
        if (!instance) return;
    
        for (const dispose of instance.unmount) {
            try {
                dispose();
            } catch (error) {
                console.error(
                    `[THE_CLOUD][UNMOUNT] cleanup failed for "${id}":`,
                    error,
                );
            }
        }
    
        instance.unmount.clear();
    
        for (const hook of instance.hooks.values()) {
            try {
                hook.cleanup?.();
            } catch (error) {
                console.error(
                    `[THE_CLOUD][UNMOUNT] hook cleanup failed for "${id}":`,
                    error,
                );
            }
        }
    
        instance.hooks.clear();
    
        const pid = instance.__pid;
    
        if (pid !== undefined) {
            this.compFlags[pid] |= FLAG_DISPOSED;
            this.compRender[pid] = undefined;
            this.compInstance[pid] = undefined;
        }
    
        this._components.delete(id);
    }
    private makeComponentInstance(
        pid: Pid,
        id: S,
        renderFn?: RenderFn,
    ): ComponentInstance {
        const instance: ComponentInstance = {
            id,
            __pid: pid,
            hooks: new Map<string, HookState>(),
            unmount: new Set<() => void>(),
            render: renderFn,
            parent: null,
            contextProviders: new Map(),
        };
    
        this.compInstance[pid] = instance;
        return instance;
    }
    private registerHookKey(key: S, hookPid: Pid): void {
        this.hookKeyToPid.set(key, hookPid);
        this.hookPidToKey.set(hookPid, key);
    }
    private unregisterHookKey(key: S, hookPid: Pid): void {
        if (this.hookKeyToPid.get(key) === hookPid) {
            this.hookKeyToPid.delete(key);
        }
    
        if (this.hookPidToKey.get(hookPid) === key) {
            this.hookPidToKey.delete(hookPid);
        }
    }
    private findHookByKey(key: S): Pid {
        return this.hookKeyToPid.get(key) ?? -1;
    }
    // ========================================================================
    // Hook pool
    // ========================================================================
    private findHook(componentPid: Pid, hookNameHash: N, sauronIdHash: N): Pid {
        const hash = this.hash4(
            String(componentPid),
            String(hookNameHash),
            String(sauronIdHash),
            "hook",
        );

        let slot = hash & this.mask;

        while (this.hookSlotState[slot] === 1) {
            if (this.hookSlotHash[slot] === hash) {
                return this.hookSlotPid[slot];
            }
            slot = (slot + 1) & this.mask;
        }

        return -1;
    }
    private createHookSlot( componentPid: Pid, hookNameHash: N, sauronIdHash: N, kind: N,    ): Pid {
        const hash = this.hash4(
            String(componentPid),
            String(hookNameHash),
            String(sauronIdHash),
            "hook",
        );

        let slot = hash & this.mask;

        while (this.hookSlotState[slot] === 1) {
            slot = (slot + 1) & this.mask;
        }

        const pid = this.hookCount++;

        if (pid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: hook capacity exhausted");
        }

        this.hookSlotState[slot] = 1;
        this.hookSlotHash[slot] = hash;
        this.hookSlotPid[slot] = pid;

        this.hookKind[pid] = kind;
        this.hookValue[pid] = undefined;
        this.hookPending[pid] = undefined;
        this.hookVersion[pid] = 0;
        this.hookComponent[pid] = componentPid;
        this.hookSlot[pid] = this._reactHookCursor++;
        this.hookFn[pid] = undefined;
        this.hookCleanup[pid] = undefined;
        this.hookBindingHead[pid] = -1;
        this.hookBindingTail[pid] = -1;
        this.hookDomRefHead[pid] = -1;
        this.hookDomRefTail[pid] = -1;
        this.hookDeps[pid] = undefined;

        return pid;
    }
    private findHookByStringKey(key: S): Pid {
        // Reconstruct hook pid from key format "componentId:hookName:sauronId"
        const parts = key.split(":");
        if (parts.length < 3) return -1;

        const componentPid = parseInt(parts[0], 10);
        const hookNameHash = this.hashString(parts[1]);
        const sauronIdHash = this.hashString(parts[2]);

        return this.findHook(componentPid, hookNameHash, sauronIdHash);
    }
    // ========================================================================
    // Binding pool
    // ========================================================================
    public registerDOMBinding(
        node: Node,
        update: (node: Node, value: A) => void,
    ): (() => void) | undefined {
        const hook = this._currentHookState;
        if (!hook) return;

        const hookPid = this.resolveHookPidFromFacade(hook);
        if (hookPid === -1) return;

        const bid = this.bindCount++;

        if (bid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: binding capacity exhausted");
        }

        this.bindNode[bid] = node;
        this.bindUpdate[bid] = update;
        this.bindActive[bid] = 1;
        this.bindHook[bid] = hookPid;

        // Append to hook's binding list.
        const tail = this.hookBindingTail[hookPid];

        if (tail !== -1) {
            this.bindNext[tail] = bid;
            this.bindPrev[bid] = tail;
            this.hookBindingTail[hookPid] = bid;
        } else {
            this.hookBindingHead[hookPid] = bid;
            this.hookBindingTail[hookPid] = bid;
        }

        // Reverse index on node.
        const anyNode = node as A;

        if (!anyNode.__sauron_bindings__) {
            anyNode.__sauron_bindings__ = new Set();
        }

        anyNode.__sauron_bindings__.add(hookPid);

        this.compFlags[this.hookComponent[hookPid]] |= FLAG_HAS_BINDINGS;

        return () => {
            if (this.bindActive[bid] === 0) return;

            this.bindActive[bid] = 0;

            const prev = this.bindPrev[bid];
            const next = this.bindNext[bid];

            if (prev !== -1) this.bindNext[prev] = next;
            else this.hookBindingHead[hookPid] = next;

            if (next !== -1) this.bindPrev[next] = prev;
            else this.hookBindingTail[hookPid] = prev;

            anyNode.__sauron_bindings__?.delete(hookPid);
        };
    }
    private resolveHookPidFromFacade(facade: A): Pid {
        return facade.__pid ?? -1;
    }
    // ========================================================================
    // DOM ref pool
    // ========================================================================
    public registerDOMRef(name: S, node: Node): V {
        const hook = this._currentHookState;
        if (!hook) return;

        const hookPid = this.resolveHookPidFromFacade(hook);
        if (hookPid === -1) return;

        const rid = this.refCount++;

        if (rid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: ref capacity exhausted");
        }

        this.refName[rid] = name;
        this.refNode[rid] = node;
        this.refHook[rid] = hookPid;

        const tail = this.hookDomRefTail[hookPid];

        if (tail !== -1) {
            this.refNext[tail] = rid;
            this.refPrev[rid] = tail;
            this.hookDomRefTail[hookPid] = rid;
        } else {
            this.hookDomRefHead[hookPid] = rid;
            this.hookDomRefTail[hookPid] = rid;
        }
    }
    public getDOMRef(name: S): Node | UD {
        const hook = this._currentHookState;
        if (!hook) return undefined;

        const hookPid = this.resolveHookPidFromFacade(hook);
        if (hookPid === -1) return undefined;

        let rid = this.hookDomRefHead[hookPid];

        while (rid !== -1) {
            if (this.refName[rid] === name) {
                return this.refNode[rid]!;
            }
            rid = this.refNext[rid];
        }

        return undefined;
    }
    // ========================================================================
    // Cleanup pool
    // ========================================================================
    private addCleanup(componentPid: Pid, fn: () => V): N {
        const cid = this.cleanupCount++;

        if (cid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: cleanup capacity exhausted");
        }

        this.cleanupFn[cid] = fn;
        this.cleanupComponent[cid] = componentPid;

        const tail = this.compUnmountTail[componentPid];

        if (tail !== -1) {
            this.cleanupNext[tail] = cid;
            this.cleanupPrev[cid] = tail;
            this.compUnmountTail[componentPid] = cid;
        } else {
            this.compUnmountHead[componentPid] = cid;
            this.compUnmountTail[componentPid] = cid;
        }

        return cid;
    }
    // ========================================================================
    // Hook subscriber pool
    // ========================================================================
    public subscribeToHook(key: S, cb: () => V): () => V {
        const keyHash = this.hashString(key);
        let slot = keyHash & this.mask;

        // Find existing slot for this key (probe until empty or match).
        while (this.hookSubHead[slot] !== -1 || this.hookSubTail[slot] !== -1) {
            if (this.hookSubKeyHash[this.hookSubHead[slot]] === keyHash) {
                break;
            }
            slot = (slot + 1) & this.mask;
        }

        const sid = this.hookSubCount++;

        if (sid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: hook subscriber capacity exhausted");
        }

        this.hookSubFn[sid] = cb;
        this.hookSubKeyHash[sid] = keyHash;

        const tail = this.hookSubTail[slot];

        if (tail !== -1) {
            this.hookSubNext[tail] = sid;
            this.hookSubPrev[sid] = tail;
            this.hookSubTail[slot] = sid;
        } else {
            this.hookSubHead[slot] = sid;
            this.hookSubTail[slot] = sid;
        }

        return () => {
            const prev = this.hookSubPrev[sid];
            const next = this.hookSubNext[sid];

            if (prev !== -1) this.hookSubNext[prev] = next;
            else this.hookSubHead[slot] = next;

            if (next !== -1) this.hookSubPrev[next] = prev;
            else this.hookSubTail[slot] = prev;

            this.hookSubFn[sid] = undefined;
        };
    }
    // ========================================================================
    // Hook notification lane — deduped, replaces pendingNotifications Set
    // ========================================================================
    public notifyHook(key: S): V {
        const keyHash = this.hashString(key);
        const slot = keyHash & this.mask;

        if (this.inHookNotifyLane[slot]) return;

        if (this.hookNotifyLaneLen >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: hook notify lane full");
        }

        this.inHookNotifyLane[slot] = 1;
        this.hookNotifyLane[this.hookNotifyLaneLen++] = slot;

        if (!this._flushScheduled) {
            this._flushScheduled = true;
            queueMicrotask(this._flushHookNotifications);
        }
    }
    public _flushHookNotifications = (): V => {
        this._flushScheduled = false;

        const len = this.hookNotifyLaneLen;
        this.hookNotifyLaneLen = 0;

        for (let i = 0; i < len; i++) {
            const slot = this.hookNotifyLane[i];
            this.inHookNotifyLane[slot] = 0;

            // Reconstruct key from slot — for API compat.
            const key = `hook_${slot}`;
            this.flushHookKey(key);
        }
    };
    // ========================================================================
    // State cell pool — (s, m, r, key) → cellId
    // ========================================================================
    private getOrCreateCell( slReg: S | SY, msReg: S | SY, r: S | SY, key: S | SY, ): N {
        const hash = this.hash4(slReg, msReg, r, key);
        let slot = hash & this.mask;

        while (this.cellSlotState[slot] !== 0) {
            if (this.cellSlotState[slot] === 1 && this.cellSlotHash[slot] === hash) {
                return this.cellSlotPid[slot];
            }
            slot = (slot + 1) & this.mask;
        }

        if (this.cellCount >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: cell capacity exhausted");
        }

        const cellId = this.cellCount++;

        this.cellSlotState[slot] = 1;
        this.cellSlotHash[slot] = hash;
        this.cellSlotPid[slot] = cellId;

        this.cellKey[cellId] = key;
        this.cellValue[cellId] = undefined;
        this.cellVersion[cellId] = 0;
        this.cellPersisted[cellId] = 0;
        this.cellHasWritten[cellId] = 0;
        this.cellHistory[cellId] = undefined;
        this.cellDirty[cellId] = 0;
        this.cellGlobalDirty[cellId] = 0;
        this.cellListenerHead[cellId] = -1;
        this.cellListenerTail[cellId] = -1;
        this.cellWaiterHead[cellId] = -1;
        this.cellWaiterTail[cellId] = -1;

        return cellId;
    }
    // ========================================================================
    // Listener / waiter / global pools
    // ========================================================================
    private addListener(cellId: N, fn: (value: A) => V): N {
        const lid = this.listenerCount++;

        if (lid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: listener capacity exhausted");
        }

        this.listenerFn[lid] = fn;
        this.listenerCell[lid] = cellId;

        const tail = this.cellListenerTail[cellId];

        if (tail !== -1) {
            this.listenerNext[tail] = lid;
            this.listenerPrev[lid] = tail;
            this.cellListenerTail[cellId] = lid;
        } else {
            this.cellListenerHead[cellId] = lid;
            this.cellListenerTail[cellId] = lid;
        }

        return lid;
    }
    private removeListener(lid: N): V {
        const cellId = this.listenerCell[lid];
        const prev = this.listenerPrev[lid];
        const next = this.listenerNext[lid];

        if (prev !== -1) this.listenerNext[prev] = next;
        else this.cellListenerHead[cellId] = next;

        if (next !== -1) this.listenerPrev[next] = prev;
        else this.cellListenerTail[cellId] = prev;

        this.listenerFn[lid] = undefined;
        this.listenerCell[lid] = -1;
    }
    private addWaiter(cellId: N, resolve: (value: A) => V): N {
        const wid = this.waiterCount++;

        if (wid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: waiter capacity exhausted");
        }

        this.waiterResolve[wid] = resolve;
        this.waiterCell[wid] = cellId;

        const tail = this.cellWaiterTail[cellId];

        if (tail !== -1) {
            this.waiterNext[tail] = wid;
            this.cellWaiterTail[cellId] = wid;
        } else {
            this.cellWaiterHead[cellId] = wid;
            this.cellWaiterTail[cellId] = wid;
        }

        return wid;
    }
    private addGlobalListener(fn: (key: S | SY, value: A) => V): N {
        const gid = this.globalCount++;

        if (gid >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: global listener capacity exhausted");
        }

        this.globalFn[gid] = fn;

        if (this.globalTail !== -1) {
            this.globalNext[this.globalTail] = gid;
            this.globalPrev[gid] = this.globalTail;
            this.globalTail = gid;
        } else {
            this.globalHead = gid;
            this.globalTail = gid;
        }

        return gid;
    }
    // ========================================================================
    // Dirty lane — deduped, replaces dirty: Set<FlushCallback>
    // ========================================================================
    public onFlush(cb: FlushCallback): V {
        const did = this.addDirtyCallback(cb);
        this.scheduleDrain();
    }
    private addDirtyCallback(cb: FlushCallback): N {
        const did = this.dirtyCbCount++;

        if (did >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: dirty callback capacity exhausted");
        }

        this.dirtyCbFn[did] = cb;

        if (this.dirtyCbTail !== -1) {
            this.dirtyCbNext[this.dirtyCbTail] = did;
            this.dirtyCbPrev[did] = this.dirtyCbTail;
            this.dirtyCbTail = did;
        } else {
            this.dirtyCbHead = did;
            this.dirtyCbTail = did;
        }

        return did;
    }
    public scheduleDrain(): V {
        if (this._flushScheduled) return;
        this._flushScheduled = true;
        queueMicrotask(this.drain);
    }
    public drain(): V {
        this._flushScheduled = false;

        if (this.dirtyCbHead === -1 && this.dirtyLaneLen === 0) return;

        // Async-safe batch scope still open — don't drop, wait.
        if (this.isBatching()) {
            this.scheduleDrain();
            return;
        }

        // 1. Run dirty callbacks (linked list traversal).
        let did = this.dirtyCbHead;

        while (did !== -1) {
            const next = this.dirtyCbNext[did];
            const fn = this.dirtyCbFn[did];

            if (fn) {
                try { fn(); }
                catch (err) { console.error(`[Sauron Flush Error]:`, err); }
            }

            // Remove from list.
            this.dirtyCbFn[did] = undefined;
            this.dirtyCbPrev[did] = -1;
            this.dirtyCbNext[did] = -1;

            did = next;
        }

        this.dirtyCbHead = -1;
        this.dirtyCbTail = -1;
        this.dirtyCbCount = 1;

        // 2. Process dirty cells (deduped notify pipeline).
        const len = this.dirtyLaneLen;
        this.dirtyLaneLen = 0;

        for (let i = 0; i < len; i++) {
            const cellId = this.dirtyLane[i];
            this.inDirtyLane[cellId] = 0;

            const value = this.cellValue[cellId];

            if (this.cellDirty[cellId]) {
                this.cellDirty[cellId] = 0;

                let lid = this.cellListenerHead[cellId];
                while (lid !== -1) {
                    const next = this.listenerNext[lid];
                    const fn = this.listenerFn[lid];

                    if (fn) {
                        try { fn(value); }
                        catch (err) { console.error(`[Sauron Listener Error]:`, err); }
                    }

                    lid = next;
                }
            }

            if (this.cellGlobalDirty[cellId]) {
                this.cellGlobalDirty[cellId] = 0;

                let wid = this.cellWaiterHead[cellId];
                while (wid !== -1) {
                    const next = this.waiterNext[wid];
                    const resolve = this.waiterResolve[wid];

                    if (resolve) {
                        try { resolve(value); }
                        catch (err) { console.error(`[Sauron Waiter Error]:`, err); }
                    }

                    wid = next;
                }

                this.cellWaiterHead[cellId] = -1;
                this.cellWaiterTail[cellId] = -1;

                let gid = this.globalHead;
                while (gid !== -1) {
                    const next = this.globalNext[gid];
                    const fn = this.globalFn[gid];

                    if (fn) {
                        try { fn(this.cellKey[cellId], value); }
                        catch (err) { console.error(`[Sauron Global Listener Error]:`, err); }
                    }

                    gid = next;
                }
            }
        }
    }
    public microtask(fn: FlushCallback): V { queueMicrotask(fn);    }
    // ========================================================================
    // Batch — async-safe scope pool
    // ========================================================================
    public batch<T>(fn: () => T): T {
        this.batchDepth++;

        try {
            return fn();
        } finally {
            this.batchDepth--;

            if (this.batchDepth === 0) {
                this.flush();
            }
        }
    }
    public openBatch(): SY {
        const scopeId = this.batchScopeCount++;

        if (scopeId >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: batch scope capacity exhausted");
        }

        this.batchScopeActive[scopeId] = 1;

        return Symbol(`batch-scope-${scopeId}`);
    }
    public closeBatch(scope: SY): V {
        const desc = String(scope);
        const match = desc.match(/batch-scope-(\d+)/);

        if (match) {
            const scopeId = parseInt(match[1], 10);
            this.batchScopeActive[scopeId] = 0;
        }

        if (!this.isBatching() && (this.dirtyCbHead !== -1 || this.dirtyLaneLen > 0)) {
            this.scheduleDrain();
        }
    }
    public isBatching(): B {
        if (this.batchDepth > 0) return true;

        for (let i = 1; i < this.batchScopeCount; i++) {
            if (this.batchScopeActive[i]) return true;
        }

        return false;
    }
    public flushSync(): V {
        if (this.isBatching()) return;

        this._flushScheduled = false;

        if (this.dirtyCbHead === -1 && this.dirtyLaneLen === 0) return;

        this.drain();
    }
    public flush<T>(fn?: () => T): T | V {
        if (!fn) {
            this.flushSync();
            return;
        }

        this.batchDepth++;
        let result: T;

        try {
            result = fn();
        } finally {
            this.batchDepth--;
        }

        this.flushSync();
        return result;
    }








    // ========================================================================
    // Scheduler — fully pooled, per-priority per-scope FIFO
    // ========================================================================
    public enqueue(task: A): V {
        if (task.priority === "immediate") {
            this.microtask(() => this.runOne(task));
            return;
        }

        const priorityIdx = this.priorityIndex(task.priority);
        const scopeHash = this.hashString(String(task.scope));

        // Find or create scope slot.
        let scopeSlot = -1;
        let slot = scopeHash & this.mask;

        while (this.scopeSlotState[slot] !== 0) {
            if (this.scopeSlotHash[slot] === scopeHash) {
                scopeSlot = this.scopeSlotId[slot];
                break;
            }
            slot = (slot + 1) & this.mask;
        }

        if (scopeSlot === -1) {
            scopeSlot = this.scopeSlotCount++;

            if (scopeSlot >= this.cap) {
                throw new Error("SAURON_DISRUPTOR: scope capacity exhausted");
            }

            this.scopeSlotState[slot] = 1;
            this.scopeSlotHash[slot] = scopeHash;
            this.scopeSlotId[slot] = scopeSlot;
        }

        const queueIdx = priorityIdx * this.cap + scopeSlot;
        const taskId = this.taskCount++;

        if (taskId >= this.cap) {
            throw new Error("SAURON_DISRUPTOR: task capacity exhausted");
        }

        this.taskFn[taskId] = task.fn;
        this.taskPriority[taskId] = priorityIdx;
        this.taskScopeHash[taskId] = scopeHash;
        this.taskSignal[taskId] = task.signal;
        this.taskCancelled[taskId] = 0;
        this.taskPrev[taskId] = -1;
        this.taskNext[taskId] = -1;

        // Append to FIFO.
        const tail = this.queueTail[queueIdx];

        if (tail !== -1) {
            this.taskNext[tail] = taskId;
            this.taskPrev[taskId] = tail;
            this.queueTail[queueIdx] = taskId;
        } else {
            this.queueHead[queueIdx] = taskId;
            this.queueTail[queueIdx] = taskId;
        }

        this.queueCount[queueIdx]++;

        this.scheduleLoop();
    }
    private priorityIndex(p: Priority): number {
        switch (p) {
            case "immediate": return PRIORITY_IMMEDIATE;
            case "user-blocking": return PRIORITY_USER_BLOCKING;
            case "normal": return PRIORITY_NORMAL;
            case "background": return PRIORITY_BACKGROUND;
            case "idle": return PRIORITY_IDLE;
            default: return PRIORITY_NORMAL;
        }
    }
    public scheduleLoop(): V {
        if (this.loopScheduled) return;
        this.loopScheduled = true;
        setImmediate(this.drainOne);
    }
    public pickNext(): A | UD {
        for (let p = 0; p < PRIORITY_COUNT; p++) {
            if (p === PRIORITY_IMMEDIATE) continue;

            // Round-robin over scopes with pending tasks.
            let scopesWithTasks = 0;
            let firstScopeSlot = -1;

            for (let s = 1; s < this.scopeSlotCount; s++) {
                const queueIdx = p * this.cap + s;

                if (this.queueCount[queueIdx] > 0) {
                    scopesWithTasks++;
                    if (firstScopeSlot === -1) firstScopeSlot = s;
                }
            }

            if (scopesWithTasks === 0) continue;

            const cursor = this.scopeCursors[p] % scopesWithTasks;
            let targetScope = firstScopeSlot;

            for (let i = 0; i < cursor; i++) {
                let s = targetScope + 1;

                while (s < this.scopeSlotCount) {
                    const queueIdx = p * this.cap + s;

                    if (this.queueCount[queueIdx] > 0) {
                        targetScope = s;
                        break;
                    }

                    s++;
                }
            }

            const queueIdx = p * this.cap + targetScope;
            const taskId = this.queueHead[queueIdx];

            // Pop from head.
            const next = this.taskNext[taskId];

            this.queueHead[queueIdx] = next;
            this.queueCount[queueIdx]--;

            if (this.queueCount[queueIdx] === 0) {
                this.queueTail[queueIdx] = -1;
            }

            this.scopeCursors[p] = cursor + 1;

            return {
                priority: PRIORITY_NAMES[p],
                scope: String(this.taskScopeHash[taskId]),
                fn: this.taskFn[taskId]!,
                signal: this.taskSignal[taskId],
                cancelled: this.taskCancelled[taskId] === 1,
            };
        }

        return undefined;
    }
    public runOne(task: A): V {
        if (task.cancelled || task.signal?.aborted) return;

        const prev = this.currentScope;
        this.currentScope = task.scope;

        try {
            task.fn();
        } finally {
            this.currentScope = prev;
        }
    }
    public drainOne(): V {
        this.loopScheduled = false;

        const task = this.pickNext();

        if (task) this.runOne(task);

        // Check if more tasks remain.
        let hasMore = false;

        for (let p = 0; p < PRIORITY_COUNT; p++) {
            for (let s = 1; s < this.scopeSlotCount; s++) {
                const queueIdx = p * this.cap + s;

                if (this.queueCount[queueIdx] > 0) {
                    hasMore = true;
                    break;
                }
            }

            if (hasMore) break;
        }

        if (hasMore) this.scheduleLoop();
    }
    public schedule( fn: () => V, opts: { priority?: Priority; scope?: S; signal?: AbortSignal } = {},    ): A {
        const task: A = {
            priority: opts.priority ?? "normal",
            scope: opts.scope ?? String(this.currentScope),
            fn,
            signal: opts.signal,
            cancelled: false,
        };

        this.enqueue(task);

        return {
            cancel: () => { task.cancelled = true; },
            reprioritize: (next: Priority) => {
                if (task.cancelled || next === task.priority) return;
                task.priority = next;
                this.enqueue(task);
            },
        };
    }
    public withRequestScope<T>(requestId: S, fn: () => T): T {
        const prev = this.currentScope;
        this.currentScope = requestId;

        try {
            return fn();
        } finally {
            this.currentScope = prev;
        }
    }
    public yieldToScheduler(): P<V> {
        return new Promise((resolve) => {
            this.schedule(() => resolve(), {
                priority: "normal",
                scope: String(this.currentScope),
            });
        });
    }
    public scheduleChunked( work: Iterable<() =>V> | Iterator<() => V>, opts: { priority?: Priority; scope?: S; chunkBudgetMs?: N } = {},    ): A {
        const iterator: Iterator<() => void> =
            Symbol.iterator in (work as A)
                ? (work as Iterable<() => void>)[Symbol.iterator]()
                : (work as Iterator<() => void>);

        const chunkBudgetMs = opts.chunkBudgetMs ?? 5;
        const scope = opts.scope ?? String(this.currentScope);
        let cancelled = false;
        const priority = opts.priority ?? "normal";
        const self = this;

        function runChunk(): void {
            if (cancelled) return;

            const start = performance.now();

            while (true) {
                const { value: step, done } = iterator.next();
                if (done) return;
                step();

                const elapsed = performance.now() - start;
                if (elapsed >= chunkBudgetMs) break;
            }

            if (!cancelled) {
                self.schedule(runChunk, { priority, scope });
            }
        }

        this.schedule(runChunk, { priority, scope });

        return {
            cancel(): void { cancelled = true; },
            reprioritize(next: Priority): void {
                // Full reprioritize requires queue re-insertion — preserved as no-op for now.
            },
        };
    }
    // ========================================================================
    // Notify — deduped via dirty lane (replaces WeakMap caches)
    // ========================================================================
    public scheduleGlobalNotify(key: S | SY, _meta: A, opts: A, ): V {
        const cellId = this.findCell( opts.s, opts.m, opts.r, key,        );

        if (cellId !== -1) { this.markCellDirty(cellId, true); }
    }
    public scheduleNotify(key: S | SY, _listeners: A, _state: A, cellId?: N, ): V {
        if (cellId !== undefined && cellId !== -1) {
            this.markCellDirty(cellId, false);
        }
    }
    private markCellDirty(cellId: number, global: boolean): void {
        if (global) this.cellGlobalDirty[cellId] = 1;
        else this.cellDirty[cellId] = 1;
        if (this.inDirtyLane[cellId]) return;
        if (this.dirtyLaneLen >= this.cap) { throw new Error("SAURON_DISRUPTOR: dirty lane full"); }
        this.inDirtyLane[cellId] = 1;
        this.dirtyLane[this.dirtyLaneLen++] = cellId;
        this.scheduleDrain();
    } 
    private findCell(slReg: S | SY, msReg: S | SY, globalKey: S | SY, key: S | SY,): N {
        const hash = this.hash4(slReg, msReg, globalKey, key);
        let slot = hash & this.mask;

        while (this.cellSlotState[slot] !== 0) {
            if (this.cellSlotState[slot] === 1 && this.cellSlotHash[slot] === hash) {
                return this.cellSlotPid[slot];
            }
            slot = (slot + 1) & this.mask;
        }

        return -1;
    }
    public beginHookDependencyCapture(hook: A): void { this._currentHookState = hook;    }
    public endHookDependencyCapture(): void {this._currentHookState = null;    }
    // ========================================================================
    // Dev / HMR / adapter — preserved
    // ========================================================================
    public _devRecord:  ((componentId: S, hookName: S, sauronId: S, key: S, value: A) => V) | NU = null;
    public devRecord(componentId: S, hookName: S, sauronId: S, key: S, value: A) {        return this._devRecord?.(componentId, hookName, sauronId, key, value);    }
    public registerDevRecorder(fn: typeof this._devRecord) { this._devRecord = fn;    }
    public rerender = (key?: S): V => {
        if (key !== undefined) {
            this.notifyHook(key);
        } else {
            this.flushSync();
        }
    };
    public getCurrentComponent = (): A => {
        if ((isReact as A) === true && (IS_THE_WIZARD as A) === false) {
            if (this._currentComponent) return this._currentComponent;
            return this.hub;
        } else {
            if (!(init_hooks_context as A)) {
                (this as A).set("_root", this.hub, true, "SAURON_SLAVE_REG", "SAURON_MASTER_REG", "SAURON_ROOT_REG");
                init_hooks_context = true;
            }
            const updatedCtx = (this as A).get( "_root", "SAURON_SLAVE_REG", "SAURON_MASTER_REG", "SAURON_ROOT_REG", );
            if (updatedCtx && updatedCtx.id === "SAURON_ROOT_REG") { return updatedCtx; }

            return this.hub;
        }
    };
    public setCurrentComponent(instance: A | NU): V { this._currentComponent = instance;    }
    public registerAdapter(adapter: A): V {this._adapter = adapter;     }
    public _isReactRenderContext(): B {
        const react = isReact as A;

        if (!react) return false;
        if (IS_THE_WIZARD as A) return false;

        try {
            react.useState;

            const internals =
                (react as A).__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;

            const dispatcher =
                internals?.ReactCurrentDispatcher?.current ?? internals?.H;

            return !!dispatcher;
        } catch {
            return false;
        }
    }
    public createHook<T>(hookName: S,sauronId: S,initializer: () => A,updater: (state: A, key: S, notify: (key: S) => V) => T,triggerRender: B = true,    ): T {
        if ((isReact as A) === true && (IS_THE_WIZARD as A) === false) {
            try {
                const isReactCtx = this._isReactRenderContext();
                const { instance, forceUpdate } =isReactCtx && this._adapter? this._adapter.getComponentInstance() : { instance: this.getCurrentComponent(), forceUpdate: undefined };
                const component = instance;// Stable slot id — React cursor only inside React context.
                const stableSlotId = isReactCtx ? `seq-${this._reactHookCursor++}` : sauronId;
                const key = `${component.id}:${hookName}:${stableSlotId}`;

                if (!this.wr._components.has(component.id)) { this.wr._components.set(component.id, component); }
                if (!component.hooks.has(key)) {
                    const state = initializer();
                    component.hooks.set(key, state);
                    if (component.render) {const unsub = this.subscribeToHook(key,() => component.render?.(),);component.unmount.add(unsub);}
                }
                if (isReactCtx && this._adapter && forceUpdate) { this._adapter.onHookSubscribe(key, forceUpdate);                }

                const notify = (k: string) => {
                    if ( typeof process !== "undefined" && process.env?.NODE_ENV !== "production" ) {
                        const state = component.hooks.get(k);
                        this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
                    }
                    this.notifyHook(k);
                };

                const hookState = component.hooks.get(key)!;
                this.beginHookDependencyCapture(hookState);

                try {     return updater(hookState, key, notify);   } 
                finally {    this.endHookDependencyCapture();  }
            } catch (e: A) {
                return e.error;
            }
        } else {
            try {
                const component = this.getCurrentComponent();
                const key = `${component.id}:${hookName}:${sauronId}`;
                if (!this.wr._components.has(component.id)) { this.wr._components.set(component.id, component);                }

                if (!component.hooks.has(key)) {
                    const state = initializer();
                    component.hooks.set(key, state);
                   if (triggerRender && component.render) {const unsub = this.subscribeToHook( key, () => component.render?.(), ); component.unmount.add(unsub); }
                }
                const notify = (k: string) => {
                    if (  typeof process !== "undefined" &&  process.env?.NODE_ENV !== "production"   ) {
                        const state = component.hooks.get(k);
                        this.devRecord(component.id, hookName, sauronId, k, state?.value);
                    }
                    if (triggerRender) this.notifyHook(k);
                };
                const hookState = component.hooks.get(key)!;
                this.beginHookDependencyCapture(hookState);
                try {  return updater(hookState, key, notify);  }
                 finally {  this.endHookDependencyCapture();  }
            } catch (e: A) {
                return e.error;
            }
        }
    }
    // ========================================================================
    // Broadcast / chains — thin wrappers, preserved
    // ======================================================================== 
    private runEffect(pid: Pid): void {
        if (this.compFlags[this.hookComponent[pid]] & FLAG_DISPOSED) return;

        const previousCleanup = this.hookCleanup[pid];

        if (previousCleanup) {
            this.hookCleanup[pid] = undefined;
            previousCleanup();
        }

        const fn = this.hookFn[pid] as EffectFn;

        if (!fn) return;

        const cleanup = fn();

        this.hookCleanup[pid] =
            typeof cleanup === "function" ? (cleanup as CleanupFn) : undefined;
    }
    public hmrPreserve(): Map<string, Map<string, A>> {
        const snapshot = new Map<string, Map<string, A>>();
        for (let pid = 0; pid < this.compCount; pid++) {
            if (this.compFlags[pid] & FLAG_DISPOSED) continue;
            const id = this.compId[pid];
            if (!id) continue;
            const hooks = new Map<string, A>();
            for (let hid = 0; hid < this.hookCount; hid++) {
                if (this.hookComponent[hid] !== pid) continue;
                const key = this.hookKey[hid];
                if (!key) continue;
                hooks.set(key, this.hookValue[hid]);
            }
            snapshot.set(id, hooks);
        }
        return snapshot;
    }
    public hmrRestore(snapshot: Map<string, Map<string, A>>): void {
        snapshot.forEach((hooks, id) => {
            const pid = this.findComponent(id);
            if (pid === -1) return;
            hooks.forEach((value, key) => {
                const hid = this.findHookByStringKey(key);
                if (hid === -1) return;
                if (this.hookComponent[hid] !== pid) return;
                this.hookValue[hid] = value;
            });
        });
    }
    public dark = (() => {
        const fn = <T = A>(key: A, s?: A, m?: A, r?: A) => {
            return MaybePromise(this.state.get<T>(key, { s, m, r }));
        }
        fn._broadcastSS=(key: A, value: A, storageType: A, fn: A, path?: A, domain?: A, options?: A)=> {
            _localChange = true;
            _channel?.postMessage({ key, value, storageType, fn, path, domain, options });
        }
        fn.set =<T = A>(label: A, value: T, triggerRender = true, s?: A, m?: A, r?: A) =>{
            this.dark._broadcastSS(label, value, "sauron", "set");
            return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
        }
        fn.getAll=<T = A>(opts?: A): Record<string | symbol, T> => {
            return this.state.getAll<T>(opts);
        }
        fn.subAll=(fn: (key: A | symbol, value: A) => V, s?: A, m?: A, r?: A): () => V=> {
            if (!fn) return () => {};
            return this.state.subscribeAll(fn, { s, m, r });
        }
        fn.sub=<T = A>(k: A, fn: (value: T) => void, s?: A, m?: A, r?: A): () => void=> {
            if (!k || !fn) return () => {};
            return MaybePromise(this.state.subscribe<T>(k, fn, { s, m, r }));
        }
        fn.delete=(label: A, s?: A, m?: A, r?: A): B =>{
            if (!label) return false;
            const v = this.state.delete(label, { s, m, r });
            this.dark._broadcastSS(label, undefined, "sauron", "delete");
            return v;
        }
        fn.waitFor=<T = A>(label: A, s?: A, m?: A, r?: A): Promise<T>=> {
            return this.state.waitFor<T>(label, { s, m, r });
        }
        fn.deleteMany = async (keys: A[], s?: A, m?: A, r?: A): Promise<B>=> {
            if (!Array.isArray(keys) || keys.length === 0) {
                return false;
            }
    
            let allDeleted = true;
    
            for (let i = 0; i < keys.length; i++) {
                const deleted = this.dark.delete(keys[i], s, m, r);
                if (!deleted) {
                    allDeleted = false;
                }
            }
    
            return allDeleted;
        }
        fn.setMany=<T = Record<string, A>>(entries: T): Promise<void>=> {
            if (!entries || typeof entries !== "object") {
                return;
            }
    
            const entriesArray = Object.entries(entries);
    
            for (let i = 0; i < entriesArray.length; i++) {
                const [key, value] = entriesArray[i];
                const triggerRender = i === entriesArray.length - 1;
                this.dark.set(key, value, triggerRender);
            }
        }
        fn.getMany= async<T = A>(keys: A[], s?: A, m?: A, r?: A)=> {
            if (!Array.isArray(keys) || keys.length === 0) {
                return {};
            }
    
            const result: Record<string, T | undefined> = {};
    
            const promises = keys.map(async (key) => {
                const value = await this.dark<T>(key, s, m, r);
                return { key, value };
            });
    
            const results = await Promise.all(promises);
    
            for (const { key, value } of results) {
                result[key] = value;
            }
    
            return result;
        } 
        fn.effect=(fn: EffectFn, deps?: A[]): () => void =>{
            const component = this._currentComponent;
            const componentPid = component?.__pid ?? -1;

            const pid = this.createHookSlot(
                componentPid,
                this.hashString("useEffect"),
                this.hashString(String(this._reactHookCursor)),
                HOOK_EFFECT,
            );

            this.hookFn[pid] = fn as A;
            this.hookDeps[pid] = deps;

            // Push into effect queue — runs after render pass.
            if (this.effectQueueLen < this.cap) {
                this.effectQueue[this.effectQueueLen++] = pid;
            }

            return () => {
                const cleanup = this.hookCleanup[pid];

                if (cleanup) {
                    this.hookCleanup[pid] = undefined;
                    cleanup();
                }

                this.compFlags[componentPid] |= FLAG_DISPOSED;
            };
        }
        fn.state= <T>(initial: T | (() => T)): [T, (next: T | ((prev: T) => T)) => void] =>{
            const component = this._currentComponent;
            const componentPid = component?.__pid ?? -1;
        
            const pid = this.createHookSlot(
                componentPid,
                this.hashString("useState"),
                this.hashString(String(this._reactHookCursor)),
                HOOK_STATE,  // ← actually used now
            );
        
            const value =
                typeof initial === "function"
                    ? (initial as () => T)()
                    : initial;
        
            this.hookValue[pid] = value;
            this.hookPending[pid] = value;
        
            const setState = (next: T | ((prev: T) => T)) => {
                const prev = this.hookValue[pid] as T;
                const resolved =
                    typeof next === "function"
                        ? (next as (p: T) => T)(prev)
                        : next;
        
                if (Object.is(prev, resolved)) return;
        
                this.hookPending[pid] = resolved;
                this.hookVersion[pid]++;
        
                // Schedule render — auto-drains via microtask.
                this.scheduleRender(componentPid);
            };
        
            return [value, setState];
        }
        fn.ref=<T>(initial: T): { current: T } =>{
            const component = this._currentComponent;
            const componentPid = component?.__pid ?? -1;
        
            const pid = this.createHookSlot(
                componentPid,
                this.hashString("useRef"),
                this.hashString(String(this._reactHookCursor)),
                HOOK_REF,  // ← actually used now
            );
        
            this.hookValue[pid] = initial;
        
            return {
                get current() { return this.hookValue[pid] as T; },
                set current(v: T) { this.hookValue[pid] = v; },
            };
        }
        fn.memo=<T>(factory: () => T, deps: A[]): T=>{
            const component = this._currentComponent;
            const componentPid = component?.__pid ?? -1;
        
            const pid = this.createHookSlot(
                componentPid,
                this.hashString("useMemo"),
                this.hashString(String(this._reactHookCursor)),
                HOOK_MEMO,  // ← actually used now
            );
        
            this.hookDeps[pid] = deps;
        
            const value = factory();
            this.hookValue[pid] = value;
        
            return value;
        }
        return fn
	})();

    // ========================================================================
    // Store — SoA-backed, same API
    // ========================================================================
    public store = { 
        get: <T = A>( key: S | SY, opts: A = { s: this.s, m: this.m, r: this.r, }, ): T | UD => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);

            if (this.cellHasWritten[cellId]) { return MaybePromise(this.cellValue[cellId] as T); }

            return MaybePromise(undefined);
        },
        set: <T = A>( key: S | SY, value: T | ((prev: T | UD) => T), opts: A = { s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200, },  ) => {
            const {triggerRender = true, s, m, r, cap, } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);
            const prev = this.cellValue[cellId] as T | undefined;
            const next =   typeof value === "function"   ? (value as (p: T | undefined) => T)(prev)   : value;
            if (Object.is(prev, next)) return MaybePromise(prev);
          /**
           *   if (!this.hub.DRSTime._time_travel_reverting) {// Time travel — preserved.
          let hist = this.cellHistory[cellId];

          if (!hist) {hist = [];this.cellHistory[cellId] = hist; }

          const lastActive =  hist.findLastIndex((e: HistoryEntry) => !e.undone) ?? -1;

          if (lastActive < hist.length - 1) {  hist.length = lastActive + 1; }

          hist.push({    k: (hist[hist.length - 1]?.k ?? 0) + 1,  prev, next, ts: Date.now(), undone: false,  });
          const capVal = cap ?? 200;
          while (hist.length > capVal) hist.shift();
      }
           */
            this.cellValue[cellId] = next;
            this.cellVersion[cellId]++;
            this.cellHasWritten[cellId] = 1;
            if (this.cellPersisted[cellId]) {} // In real port: write to persistent store.// Persistence sync — preserved.
            if (triggerRender) {// Notify pipeline — deduped via lane.
                this.scheduleGlobalNotify(key, null, opts);
                this.scheduleNotify(key, null, null, cellId);
            }

            return MaybePromise(next as T);
        },
        subscribe: <T = A>( key: S | SY, listener: (value: T) => V, opts: A = { s: this.s, m: this.m, r: this.r, }, ): (() => V) => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);
            const lid = this.addListener(cellId, listener as (value: A) => void);

            return () => {
                this.removeListener(lid);
            };
        },
        delete: (key: S | SY, opts: A = { s: this.s, m: this.m, r: this.r }): B => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);

            const existed = this.cellHasWritten[cellId] === 1;

            // Notify listeners with undefined.
            let lid = this.cellListenerHead[cellId];
            while (lid !== -1) {
                const next = this.listenerNext[lid];
                const fn = this.listenerFn[lid];

                if (fn) {
                    try { fn(undefined); }
                    catch (err) { console.error(`[Sauron Listener Error]:`, err); }
                }

                lid = next;
            }

            // Clear cell.
            this.cellValue[cellId] = undefined;
            this.cellHasWritten[cellId] = 0;
            this.cellPersisted[cellId] = 0;
            this.cellHistory[cellId] = undefined;

            return MaybePromise(existed);
        },
        entries: <T = A>(opts: A = { s: this.s, m: this.m, r: this.r }): [S | SY, T][] => {
            const result: [string | symbol, T][] = [];

            for (let i = 1; i < this.cellCount; i++) {
                if (this.cellHasWritten[i]) {
                    result.push([this.cellKey[i], this.cellValue[i] as T]);
                }
            }

            return MaybePromise(result);
        },
        persist: (key: S | SY, opts: A = { s: this.s, m: this.m, r: this.r  }): V => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);
            this.cellPersisted[cellId] = 1;
        },
        unpersist: (key: S | SY, opts: A = {s: this.s, m: this.m, r: this.r }): V => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);
            this.cellPersisted[cellId] = 0;
        },
        has: (key: S | SY, opts: A = {s: this.s, m: this.m, r: this.r }): B => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);
            return this.cellHasWritten[cellId] === 1 || this.cellPersisted[cellId] === 1;
        },
        clear: (opts: A = {s: this.s, m: this.m, r: this.r  }): V => {
            for (let i = 1; i < this.cellCount; i++) {
                this.cellValue[i] = undefined;
                this.cellHasWritten[i] = 0;
                this.cellDirty[i] = 0;
                this.cellGlobalDirty[i] = 0;
                this.cellListenerHead[i] = -1;
                this.cellListenerTail[i] = -1;
                this.cellWaiterHead[i] = -1;
                this.cellWaiterTail[i] = -1;
            }
        },
        clearPersistent: (opts: A = {s: this.s, m: this.m, r: this.r }): V => {
            for (let i = 1; i < this.cellCount; i++) {
                if (this.cellPersisted[i]) {
                    this.cellPersisted[i] = 0;
                }
            }
        },
        _emit1: (key: S | SY, opts: A = {s: this.s, m: this.m, r: this.r }): V => {
            const { s, m, r } = opts;
            const cellId = this.getOrCreateCell(s, m, r, key);

            this.scheduleNotify(key, null, null, cellId);
            this.scheduleGlobalNotify(key, null, opts);
        },
        _emit: (key: string | symbol, opts?: A): void => {
            const { s, m, r } = opts ?? {};
            const cellId = this.findCell(s, m, r, key);
        
            // Match original semantics: no cell → no subscribers → early return.
            if (cellId === -1) return;
        
            const value = this.cellValue[cellId];
            const version = this.cellVersion[cellId];
        
            // ── 1. Per-key listeners (pooled stateSub linked list) ──────────────────
            const subSlot = this.stateSubSlot(r, m, s, key as string);
            let sid = this.stateSubHead[subSlot];
        
            while (sid !== -1) {
                const next = this.stateSubNext[sid];
                const fn = this.stateSubFn[sid];
        
                if (fn) {
                    try {
                        fn({
                            key,
                            previous: value,
                            current: value,
                            version,
                            timestamp: Date.now(),
                            source: "state.emit",
                        });
                    } catch (error) {
                        console.error(
                            `[Sauronstate._emit] listener failed for "${String(key)}":`,
                            error,
                        );
                    }
                }
        
                sid = next;
            }
        
            // ── 2. External subscribers (pooled extSub list, open-addressed hash) ───
            const extHash = this.hash4(r, m, s, key as string);
            let extSlot = extHash & this.mask;
        
            while (
                this.extSubHead[extSlot] !== -1 &&
                this.extSubTail[extSlot] !== -1
            ) {
                if (this.extSubHash[this.extSubHead[extSlot]] === extHash) break;
                extSlot = (extSlot + 1) & this.mask;
            }
        
            let eid = this.extSubHead[extSlot];
        
            while (eid !== -1) {
                const nextEid = this.extSubNext[eid];
                const fn = this.extSubFn[eid];
        
                if (fn) {
                    try {
                        fn(eid);
                    } catch (error) {
                        console.error(
                            `[Sauronstate._emit] external subscriber failed for "${String(key)}":`,
                            error,
                        );
                    }
                }
        
                eid = nextEid;
            }
        },

    };
    public state = {
        get: <T = A>(key: string | symbol, opts?: A): T | undefined => {
            return MaybePromise(this.store.get<T>(key, opts) as T);
        },
        set: <T = A>(key: string | symbol, value: T | ((prev: T | undefined) => T), opts?: A): void => {
            this.store.set(key, value, opts);
            return MaybePromise(value as T);
        },
        delete: (key: string | symbol, opts?: A): boolean => {
            return this.store.delete(key, opts) as boolean;
        },
        getByPrefix: <T = A>(prefix: string, opts?: A): { key: string; value: T }[] => {
            return (this.store.entries<T>(opts) as [string | symbol, T][])
                .filter(([key]) => typeof key === "string" && (key as string).startsWith(prefix))
                .map(([key, value]) => ({ key: key as string, value }));
        },
        getAll: <T = A>(opts?: A): Record<string | symbol, T> => {
            const result: Record<string | symbol, T> = {};

            for (const [key, value] of this.store.entries<T>(opts) as [string | symbol, T][]) {
                result[key] = value;
            }

            return result;
        },
        subscribe: <T = A>(key: string | symbol, listener: (value: T) => void, opts?: A): (() => void) => {
            return this.store.subscribe<T>(key, listener, opts);
        },
        subscribeAll: (listener: (key: string | symbol, value: A) => void, opts?: A): (() => void) => {
            const gid = this.addGlobalListener(listener);

            return () => {
                this.globalFn[gid] = undefined;
            };
        },
        clear: (opts?: A): void => {
            this.store.clear(opts);
        },
        waitFor: <T = A>(key: string | symbol, opts?: A): Promise<T> => {
            const { s, m, r } = opts ?? {};
            const cellId = this.getOrCreateCell(s, m, r, key);

            if (this.cellHasWritten[cellId]) {
                return Promise.resolve(this.cellValue[cellId] as T);
            }

            return new Promise<T>((resolve) => {
                this.addWaiter(cellId, resolve as (value: A) => void);
                this.markCellDirty(cellId, true);
            });
        },
    };
    // ========================================================================
    // store2 — SIGNAL_CELL-backed (preserved; integrates with SIGNALS)
    // ========================================================================
    public signals_store = {
        get: <T = A>(key: string | symbol, opts?: A): T | undefined => {
            const { s, m, r } = opts ?? {};
            const atom = this.hub.signals.create<T>(key, undefined, undefined, s, m, r) as A;
            return atom ? atom.node : undefined;
        },
        subscribe: <T = A>(    key: string | symbol,    listener: (value: T) => void,    opts?: A,): (() => void) => {
            const { r, m, s } = opts ?? {};
            const slot = this.stateSubSlot(r, m, s, key as string);

            const sid = this.stateSubCount++;

            if (sid >= this.cap) { throw new Error("SAURON_DISRUPTOR: state subscriber capacity exhausted"); }

            this.stateSubFn[sid] = listener as (value: A) => void;
            this.stateSubHash[sid] = this.hash4(r, m, s, key as string);

            const tail = this.stateSubTail[slot];

            if (tail !== -1) {
                this.stateSubNext[tail] = sid;
                this.stateSubPrev[sid] = tail;
                this.stateSubTail[slot] = sid;
            } else {
                this.stateSubHead[slot] = sid;
                this.stateSubTail[slot] = sid;
            }

            return () => {
                const prev = this.stateSubPrev[sid];
                const next = this.stateSubNext[sid];

                if (prev !== -1) this.stateSubNext[prev] = next;
                else this.stateSubHead[slot] = next;

                if (next !== -1) this.stateSubPrev[next] = prev;
                else this.stateSubTail[slot] = prev;

                this.stateSubFn[sid] = undefined;
            };
        },
        set: <T = A>(key: S | SY, value: A, opts: A = {},): T | UD => {
                const {
                triggerRender = true,
                cap = 200,
                source = "state.set",
                s, m, r,
            } = opts;

            const atom = this.hub.signals.create(
                key,
                typeof value === "function" ? value() : value,
                opts, s, m, r,
            ) as A;

            atom.historyMax = cap;

            const next = atom.write(
                (previous: A) =>
                    typeof value === "function"
                        ? (value as (p: T | undefined) => T)(previous)
                        : value,
                { source, triggerRender },
            );

            // Notify pooled 4-tier subscribers — replaces nested Map walk.
            const slot = this.stateSubSlot(r, m, s, key as string);
            let sid = this.stateSubHead[slot];

            while (sid !== -1) {
                const nextSid = this.stateSubNext[sid];
                const fn = this.stateSubFn[sid];

                if (fn) {
                    try { fn(next); }
                    catch (error) {
                        console.error(
                            `[Sauron:state.subscribeAll] listener failed for "${String(atom.key)}":`,
                            error,
                        );
                    }
                }

                sid = nextSid;
            }

            return MaybePromise(next);
        },
        delete: (key: string | symbol, opts?: A): boolean => {
            const { s, m, r } = opts ?? {};
            const registry = this.hub.state.wr._signals
            const atom = registry.get(key);

            if (!atom) return false;

            const previous = atom.peek();
            const resolvedKey = key

            // Pooled state subscribers.
            const slot = this.stateSubSlot(r, m, s, key as string);
            let sid = this.stateSubHead[slot];

            while (sid !== -1) {
                const nextSid = this.stateSubNext[sid];
                const fn = this.stateSubFn[sid];

                if (fn) {
                    try {
                        fn({
                            key: atom.key,
                            previous,
                            current: undefined,
                            version: atom.version + 1,
                            timestamp: Date.now(),
                            source: "state.delete",
                        });
                    } catch (error) {
                        console.error(
                            `[Sauron:state.delete] listener failed for "${String(key)}":`,
                            error,
                        );
                    }
                }

                sid = nextSid;
            }

            // Pooled external subscribers.
            const extHash = this.hash4(r, m, s, key as string);
            let extSlot = extHash & this.mask;

            while (this.extSubHead[extSlot] !== -1 || this.extSubTail[extSlot] !== -1) {
                if (this.extSubHash[this.extSubHead[extSlot]] === extHash) {
                    break;
                }
                extSlot = (extSlot + 1) & this.mask;
            }

            let eid = this.extSubHead[extSlot];

            while (eid !== -1) {
                const nextEid = this.extSubNext[eid];
                const fn = this.extSubFn[eid];

                if (fn) {
                    try { fn(undefined); }
                    catch (error) {
                        console.error(
                            `[Sauron:state.delete] external subscriber failed for "${String(key)}":`,
                            error,
                        );
                    }
                }

                eid = nextEid;
            }

            registry.delete(resolvedKey);
            return MaybePromise(true);
        },
        set1: <T = A>( key: string | symbol, value: A, opts: A = {}, ): T | undefined => {
            const {
                triggerRender = true,
                cap = 200,
                source = "state.set",
                s, m, r,
            } = opts;

            const atom = this.hub.signals.create( key, typeof value === "function" ? value() : value, opts, s, m, r, ) as A;
            atom.historyMax = cap;

            const next = atom.write(
                (previous: A) =>
                    typeof value === "function"  ? (value as (p: T | undefined) => T)(previous)  : value,
                { source, triggerRender },
            );

            const listeners = this.hub.state.wr._subscribersMaul.get(r)?.get(m)?.get(s)?.get(key as string);

            if (listeners) {
                for (const listener of listeners) {
                    try { listener(next); }
                    catch (error) {
                        console.error(
                            `[Sauron:state.subscribeAll] listener failed for "${String(atom.key)}":`,
                            error,
                        );
                    }
                }
            }

            return MaybePromise(next);
        },
        subscribe1: <T = A>(  key: string | symbol,  listener: (value: T) => void,   opts?: A,  ): (() => void) => {
            const { r, m, s } = opts ?? {};

            let msMap = this.hub.state.wr._subscribersMaul.get(r);
            if (!msMap) {
                msMap = new Map();
                this.hub.state.wr._subscribersMaul.set(r, msMap);
            }

            let slMap = msMap.get(m);
            if (!slMap) {
                slMap = new Map();
                msMap.set(m, slMap);
            }

            let keyMap = slMap.get(s);
            if (!keyMap) {
                keyMap = new Map();
                slMap.set(s, keyMap);
            }

            let listeners = keyMap.get(key as string);
            if (!listeners) {
                listeners = new Set();
                keyMap.set(key as string, listeners);
            }

            listeners.add(listener);

            return () => {
                listeners!.delete(listener);
                if (listeners!.size === 0) keyMap!.delete(key as string);
                if (keyMap!.size === 0) slMap!.delete(s);
                if (slMap!.size === 0) msMap!.delete(m);
                if (msMap!.size === 0)this.hub.state.wr._subscribersMaul.delete(r);
            };
        },
        delete1: (key: string | symbol, opts?: A): boolean => {
            const { s, m, r } = opts ?? {};
            const registry = this.hub.state.wr._signals
            const atom = registry.get(key);

            if (!atom) return false;

            const previous = atom.peek();

            const listeners =this.hub.state.wr._subscribersMaul.get(r)?.get(m)?.get(s)?.get(key as string);

            if (listeners) {
                for (const listener of listeners) {
                    try {
                        listener({
                            key: atom.key,
                            previous,
                            current: undefined,
                            version: atom.version + 1,
                            timestamp: Date.now(),
                            source: "state.delete",
                        });
                    } catch (error) {
                        console.error(
                            `[Sauron:state.delete] listener failed for "${String(key)}":`,
                            error,
                        );
                    }
                }
            }

            const subscribers = this._externalSubscribers.get(r)?.get(m)?.get(s)?.get(key as string);


            if (subscribers) {
                for (const notify of subscribers) {
                    try { notify(undefined); }
                    catch (error) {
                        console.error(
                            `[Sauron:state.delete] external subscriber failed for "${String(key)}":`,
                            error,
                        );
                    }
                }
            }

            registry.delete(key);
            return MaybePromise(true);
        },
        entries: <T = A>(opts?: A): [string | symbol, T][] => { 
            const registry = this.hub.state.wr._signals
            return MaybePromise([...registry.entries()].map(([key, atom]: A) => [key, atom.read() as T]),  );
        },
        persist: (key: string | symbol, opts?: A): void => {
            const atom = this.hub.state.wr._signals.get(key)
            if (!atom) return;
            atom.persisted = true;
            setSS(String(atom.key), atom.peek());
        },
        unpersist: (key: string | symbol, opts?: A): void => { 
            const atom = this.hub.state.wr._signals.get(key)
            if (!atom) return;
            atom.persisted = false;
            deleteSS(String(atom.key));
        },
        has: (key: string | symbol, opts?: A): boolean => { 
            const registry = this.hub.state.wr._signals
            return registry.has(key);
        },
        clear: (opts?: A): void => { 
            const registry =this.hub.state.wr._signals

            for (const atom of registry.values() as Iterable<A>) {
                for (const notify of atom.externalSubscribers) {
                    try { notify(); }
                    catch (error) { console.error(`[Sauron:state.clear] external subscriber failed:`, error); }
                }
                atom.listeners.clear();
                atom.externalSubscribers.clear();
                atom.waiters.clear();
            }
            registry.clear();
        },
        clearPersistent: (opts?: A): void => { 
            const registry = this.hub.state.wr._signals
            for (const atom of registry.values() as Iterable<A>) {
                if (!atom.persisted) continue;
                atom.persisted = false;
                deleteSS(String(atom.key));
            }
        },
        _emit: (key: string | symbol, opts?: A): void => {
            const { s, m, r } = opts ?? {};
            const atom = this.hub.state.wr._listenersMaul.get(key);

            if (!atom) return;

            const value = atom.peek();

            for (const listener of atom.listeners) {
                listener({
                    key: atom.key,
                    previous: value,
                    current: value,
                    version: atom.version,
                    timestamp: Date.now(),
                    source: "state.emit",
                });
            }

            for (const notify of atom.externalSubscribers) {
                notify();
            }
        },
    };
    public signals_state = {
        get: <T = A>(key: string | symbol, opts?: A): T | undefined => {
            return MaybePromise(this.store.get<T>(key, opts) as T);
        },
        set: <T = A>(key: string | symbol, value: T | ((prev: T | undefined) => T), opts?: A) => {
            this.store.set(key, value, {
                triggerRender: opts?.triggerRender ?? true,
                s: opts?.s, m: opts?.m, r: opts?.r,
                source: "state.set",
            });
            return MaybePromise(value as T);
        },
        delete: (key: string | symbol, opts?: A): boolean => {
            return this.store.delete(key, opts) as boolean;
        },
        getByPrefix: <T = A>(prefix: string, opts?: A) => {
            return (this.store.entries<T>(opts) as [string | symbol, T][])
                .filter(([key]) => typeof key === "string" && (key as string).startsWith(prefix))
                .map(([key, value]) => ({ key: key as string, value }));
        },
        getAll: <T = A>(opts?: A): Record<string | symbol, T> => {
            const result: Record<string | symbol, T> = {};

            for (const [key, value] of this.store.entries<T>(opts) as [string | symbol, T][]) {
                result[key] = value;
            }

            return result;
        },
        subscribe: <T = A>(key: string | symbol, listener: (value: T) => void, opts?: A): (() => void) => {
            return this.store.subscribe<T>(key, listener, opts);
        },
        subscribeAll: (listener: (key: string | symbol, value: unknown) => void, opts?: A): (() => void) => {
            const { s, m, r } = opts ?? {};
            const scope = this.hub.signals.get(s, m, r);
            return () => { scope.listeners.delete(listener); };
        },
        clear: (opts?: A): void => {
            this.store.clear(opts);
        },
        waitFor: <T = A>(key: string | symbol, opts?: A): Promise<T> => {
            const { s, m, r } = opts ?? {};
            const atom = this.hub.signals.get(s, m, r);

            if (!atom) {
                return new Promise<T>(() => {});
            }

            if (atom.hasWritten) {
                return Promise.resolve(atom.read());
            }

            return atom.waitForNext();
        },
    };
    // ── helper: resolve or create the 4-tier subscriber slot ──
    private stateSubSlot(r: A, m: A, s: A, key: string | symbol): number {
        const hash = this.hash4(r, m, s, key);
        let slot = hash & this.mask;
    
        while (this.stateSubHead[slot] !== -1 || this.stateSubTail[slot] !== -1) {
            if (this.stateSubHash[this.stateSubHead[slot]] === hash) {
                return slot;
            }
            slot = (slot + 1) & this.mask;
        }
    
        // Initialize empty slot.
        this.stateSubHead[slot] = -1;
        this.stateSubTail[slot] = -1;
        return slot;
    }
}
// ============================================================================
// SAURON DISRUPTOR RUNTIME
// ============================================================================

type SauronDisruptorEventKind = "FLUSH" | "HOOK_NOTIFY" | "TASK_TICK";

interface SauronDisruptorEvent {
    kind: SauronDisruptorEventKind;
    hookKey: string | null;
    ready: boolean;
}

class SauronRingBuffer {
    public readonly size: number;
    public readonly mask: number;

    private readonly slots: SauronDisruptorEvent[];
    private nextSequence: number = 0;
    private publishedSequence: number = -1;
    private consumedSequence: number = -1;
    private congestionHandler: () => void = () => {};

    constructor(size: number = 4096) {
        if ((size & (size - 1)) !== 0) {
            throw new Error("[SAURON_DISRUPTOR] Ring buffer size must be a power of two.");
        }

        this.size = size;
        this.mask = size - 1;
        this.slots = new Array<SauronDisruptorEvent>(size);

        for (let i = 0; i < size; i++) {
            this.slots[i] = {
                kind: "FLUSH",
                hookKey: null,
                ready: false
            };
        }
    }

    public setCongestionHandler(handler: () => void): void {
        this.congestionHandler = handler;
    }

    public claim(): SauronDisruptorEvent {
        while (this.nextSequence - this.consumedSequence >= this.size) {
            this.congestionHandler();

            if (this.nextSequence - this.consumedSequence < this.size) {
                break;
            }
        }

        const slot = this.slots[this.nextSequence & this.mask];
        slot.ready = false;

        return slot;
    }

    public publish(slot: SauronDisruptorEvent): void {
        slot.ready = true;
        this.publishedSequence = this.nextSequence++;
    }

    public hasAvailable(): boolean {
        return this.consumedSequence < this.publishedSequence;
    }

    public peek(): SauronDisruptorEvent | null {
        if (!this.hasAvailable()) return null;

        const sequence = this.consumedSequence + 1;
        const event = this.slots[sequence & this.mask];

        if (!event.ready) return null;

        return event;
    }

    public consume(): void {
        if (!this.hasAvailable()) return;
        this.consumedSequence++;
    }

    public pending(): number {
        return this.publishedSequence - this.consumedSequence;
    }
}

class SauronDisruptorProcessor {
    private scheduled: boolean = false;
    private running: boolean = false;

    constructor(
        private readonly buffer: SauronRingBuffer,
        private readonly trigger: (fn: () => void) => void,
        private readonly canProcess: (event: SauronDisruptorEvent) => boolean,
        private readonly onEvent: (event: SauronDisruptorEvent) => void,
        private readonly onBatchComplete: () => void
    ) {
        this.buffer.setCongestionHandler(() => {
            this.run();
        });
    }

    public publish(kind: SauronDisruptorEventKind, hookKey: string | null = null): void {
        const slot = this.buffer.claim();

        slot.kind = kind;
        slot.hookKey = hookKey;

        this.buffer.publish(slot);
        this.schedule();
    }

    public schedule(): void {
        if (this.scheduled) return;

        this.scheduled = true;
        this.trigger(() => {
            this.run();
        });
    }

    public run(): void {
        if (this.running) return;

        this.running = true;
        this.scheduled = false;

        let processed = false;

        while (true) {
            const event = this.buffer.peek();

            if (!event) break;

            if (!this.canProcess(event)) break;

            this.buffer.consume();
            processed = true;

            this.onEvent(event);
        }

        if (processed) {
            this.onBatchComplete();
        }

        this.running = false;

        if (this.buffer.hasAvailable()) {
            this.schedule();
        }
    }
}
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export const _SS_MEMORY = new Map<string, A>()
export let memoryTimer: ReturnType<typeof setTimeout> | undefined
export let ttl_active = false
export let ttl_minutes = 30
export function touchMemory() {
    if (memoryTimer) clearTimeout(memoryTimer)
    memoryTimer = setTimeout(() => {
        _SS_MEMORY.clear()
        memoryTimer = undefined
    }, ttl_minutes * 60 * 1000)
}
export async function _withLock<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
    if (typeof navigator !== 'undefined' && navigator.locks) {
        return navigator.locks.request('gt_write_' + key, fn) as Promise<T>
    }
    return fn()
}
export function generateId(): string {
    if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
        const arr = new Uint32Array(1);
        crypto.getRandomValues(arr);
        return arr[0].toString();
    }
    try {
        const { randomBytes } = require('crypto');
        return randomBytes(4).readUInt32BE(0).toString();
    } catch {
        return (Math.floor(Math.random() * 0xFFFFFFFF)).toString();
    }
}
export async function setSS(key: string, value: A) {
    if (typeof window === 'undefined') return;
    const serialized = ser(value);
    await _withLock(key, () => {
        sessionStorage.setItem(key, serialized)
        _SS_MEMORY.set(key, serialized)
    }); return value
}
function ser(value: U): S { return typeof value === 'string' ? value : JSON.stringify(value) }
function deser(raw: S | NU): A { if (raw === null) return null; try { return JSON.parse(raw) } catch { return raw }; }
export function getSS(key: S) {
    if (typeof window === 'undefined') return;
    const raw = typeof sessionStorage !== 'undefined' ? sessionStorage.getItem(key) : null
    const v = deser(raw)
    return MaybePromise(v)
}
export async function get_ws_id(): Promise<string> {
	
let Bun: A = null
let Deno: A = null
if (typeof (globalThis as A).Deno !== 'undefined') { Deno = (globalThis as A).Deno; }
if (typeof (globalThis as A).Bun !== 'undefined') { Bun = (globalThis as A).Bun; }
 const hasBun = typeof (globalThis as A).Bun !== 'undefined';
 const hasDeno = typeof (globalThis as A).Deno !== 'undefined';
 const hasNode = typeof process !== 'undefined' && !!process.versions?.node;
 const isBrowser = typeof window !== 'undefined';
 const isVSCode = typeof process !== 'undefined' && process.env.VSCODE_PID;
 const isElectron = false
 const isUnix = false
 const isWindows = true
 let WS_KEY = twx.WS_KEY
 let WS_ID = twx.WS_ID
    if (isBrowser) {
        let id = getSS(WS_KEY);
        if (!id) {
            id = generateId();
            setSS(WS_KEY, id);
        }
        WS_ID = id
        return id;
    }
    if (isVSCode) {
        const vscode = require('vscode');
        const name = vscode.workspace.workspaceFolders?.[0]?.name;
        if (name) return name;
        const id = generateId();
        WS_ID = id
        return id;
    }
    let cwd: string;
    if (hasBun || hasNode) {
        cwd = process.cwd();
    } else if (hasDeno) {
        cwd = Deno.cwd();
    } else {
        throw new Error('Unsupported environment');
    }

    let filePath: string;

    if (hasDeno) {
        const currentDir = new URL('.', import.meta.url).pathname;
        const projectRoot = new URL('../../../../../', import.meta.url).pathname;
        filePath = new URL('the_wizard/wizards_vault/ws_id.txt', projectRoot).pathname;
    } else {
        const path = require('path');
        const currentDir = typeof import.meta?.dirname !== 'undefined' ? import.meta.dirname : __dirname;
        const projectRoot = path.resolve(currentDir, '../../../../../');
        filePath = path.join(projectRoot, 'the_wizard', 'wizards_vault', 'ws_id.txt');
    }


    const readFile = async (): Promise<string | null> => {
        try {
            if (hasBun) {
                const file = Bun.file(filePath);
                if (!(await file.exists())) return null;
                return await file.text();
            } else if (hasDeno) {
                try {
                    await Deno.stat(filePath);
                    return await Deno.readTextFile(filePath);
                } catch {
                    return null;
                }
            } else if (hasNode) {
                const fs = require('fs');
                if (!fs.existsSync(filePath)) return null;
                return fs.readFileSync(filePath, 'utf8');
            }
            return null;
        } catch {
            return null;
        }
    };

    const writeFile = async (data: string) => {
        if (hasBun) {
            await Bun.write(filePath, data);
        } else if (hasDeno) {
            await Deno.writeTextFile(filePath, data);
        } else if (hasNode) {
            const fs = require('fs');
            fs.writeFileSync(filePath, data, 'utf8');
        }
    };

    let id = await readFile();
    if (id && !isNaN(Number(id))) {
        WS_ID = id
        return id;
    }

    id = generateId();
    await writeFile(id);
    WS_ID = id
    return id;
}
async function deleteSS(key: string) {
    if (typeof window === 'undefined') return
    if (ttl_active) touchMemory()
    await _withLock(key, () => {
      if (typeof sessionStorage !== 'undefined') sessionStorage.removeItem(key)
        _SS_MEMORY.delete(key)
    })
  }
// #endregion



export class RAVENS_WAR_ROOOM {
    public r: S
    public m: S
    public s: S
    public k: S
    public raven : RAVEN
    // #endregion
    //                                                                                                                  MAIN CACHES
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    #META = new M2<A, M2<A, M2<A, A>>>();
    _meta: M2<A, A>
    #SIGNALS = new M2<A, M2<A, M2<A, A>>>();
    _signals: M2<A, A>
    #CONTEXT = new M2<A, M2<A, M2<A, A>>>();
    _context: M2<A, A>
    #ATOMS = new M2<A, M2<A, M2<A, A>>>();
    _atoms: M2<A, A>
    #SAURON = new M2<A, M2<A, M2<A, A>>>();
    _sauron: M2<A, A>
    #BUS = new M2<A, M2<A, M2<A, A>>>();
    _bus: M2<A, A>

    COMPONENTS = new M2<S, ComponentInstance>();
    // #endregion
    //                                                                                                                    THE CLOUD
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    _components: M2<A, A>
    _batches: M<S | SY>
    _batchQueue: M<S | SY, A>
    _batchOpts: M<S | SY, { triggerRender?: B }>
    _globalListeners: Set<(key: S | SY, value: A) => V>
    _global_subscribers: M<S, Set<() => V>>
    _waiters: M<S | SY, ST<(value: A) => V>>
    _sys: M<S, SysEntry>
    _sealed: M<S, SysEntry>
    _frozen: ST<S>
    _keyContracts: M<S, ST<S>>
    _conflicts: M<S, ConflictPolicy>
    _conflictStrategies: M<S, "latest" | "fail" | "accumulate">
    _causal: M<S, CausalEntry[]>
    _history: M<S | SY, HistoryEntry[]>
    _scopes: M<S, SignalScope>
    _msgHandlers:  M<S, ST<(p: A) => V>>
    _lastWriteSource: M<S, S>
    _mainMap: M<A, S>
    _tupleCache: M<S, S>
    _weakMap: WM<O, S>
    _boundaries: WM<O, S>
    // #endregion
    //                                                                                                                    SIGNALS
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    _timeTravelMaul: M2<A, A>
    _historyMaul: M2<A, A>
    _subscribersMaul: M2<A, A>
    _listenersMaul: M2<A, A>
    // #endregion
    constructor(raven: RAVEN) {
        this.raven=raven
        this.r = raven.r ?? 'THE_WIZARD_ROOT_REG'
        this.m = raven.m ?? 'SAURON_MASTER_REG'
        this.s = raven.s ?? 'SAURON_SLAVE_REG'
        this.k = raven.k ?? 'THE_K_KEY'
        this._meta = this.map('_meta', this.m, this.r, '#META')
        this._signals = this.map('_signals', this.m, this.r, '#SIGNALS')
        this._context = this.map('_context', this.m, this.r, '#CONTEXT')
        this._atoms = this.map('_atoms', this.m, this.r, '#ATOMS')
        this._sauron = this.map('_sauron', this.m, this.r, '#SAURON')
        this._bus = this.map('_bus', this.m, this.r, '#BUS')


        this._components = this.map('_components_raven', this.m, this.r)
        this._batches = this.map('_batches_raven', this.m, this.r)
        this._batchQueue = this.map('_batchQueue_raven', this.m, this.r)
        this._batchOpts = this.map<M<S | SY, { triggerRender?: B }>>('_batchOpts_raven', this.m, this.r)
        this._globalListeners = this.map<M<S, Set<() => V>>>('_globalListeners_raven', this.m, this.r)
        this._global_subscribers = this.map< M<S, Set<() => V>>>('_global_subscribers_raven', this.m, this.r)
        this._waiters = this.map<M<S | SY, ST<(value: A) => V>>>('_waiters_raven', this.m, this.r)
        this._sys = this.map< M<S, SysEntry>>('_sys_raven', this.m, this.r)
        this._sealed = this.map<M<S, SysEntry>>('_sealed_raven', this.m, this.r)
        this._frozen = this.map<Set<string>>('_frozen_raven', this.m, this.r) as Set<string>
        this._keyContracts = this.map< M<S, ST<S>>>('_keyContracts_raven', this.m, this.r)
        this._conflicts = this.map<M<S, ConflictPolicy>>('_conflicts_raven', this.m, this.r)
        this._conflictStrategies = this.map<M<S, "latest" | "fail" | "accumulate">>('_conflictStrategies_raven', this.m, this.r)
        this._causal = this.map<M<S, CausalEntry[]>>('_causal_raven', this.m, this.r)
        this._history = this.map< M<S | SY, HistoryEntry[]>>('_history_raven', this.m, this.r)
        this._scopes = this.map< M<S, SignalScope>>('_scopes_raven', this.m, this.r)
        this._msgHandlers = this.map<M<S, ST<(p: A) => V>>>('_msgHandlers_raven', this.m, this.r)
        this._lastWriteSource = this.map< M<S, S>>('_lastWriteSource_raven', this.m, this.r)
        this._mainMap = this.map('_mainMap_raven', this.m, this.r)
        this._tupleCache = this.map<M<S, S>>('_tupleCache_raven', this.m, this.r)
        this._weakMap = this.map<WM<O, S>>('_weakMap_raven', this.m, this.r)
        this._boundaries = this.map<WM<O, S>>('_boundaries_raven', this.m, this.r)

        this._timeTravelMaul = this.map<WM<O, S>>('_timeTravel_maul', this.m, this.r)
        this._historyMaul = this.map<WM<O, S>>('_history_maul', this.m, this.r)
        this._subscribersMaul = this.map<WM<O, S>>(' _subscribers_maul', this.m, this.r)
        this._listenersMaul = this.map<WM<O, S>>(' _listeners_maul', this.m, this.r)
    }
    map<K = A, V=A>(s = this.s, m = this.m, r = this.r, c = '#CONTEXT'): M2<K, V> | K | V {
        let root
        switch (c) {
            case '#CONTEXT': { root = this.#CONTEXT.get(r); break; }
            case '#META': { root = this.#META.get(r); break; }
            case '#SIGNALS': { root = this.#SIGNALS.get(r); break; }
            case '#ATOMS': { root = this.#ATOMS.get(r); break; }
            case '#SAURON': { root = this.#SAURON.get(r); break; }
            case '#BUS': { root = this.#BUS.get(r); break; }
        }
        if (!root) {
            root = new M2<A, M2<A, M2<A, A>>>();
            switch (c) {
                case '#CONTEXT': { root = this.#CONTEXT.set(r, root); break; }
                case '#META': { root = this.#META.set(r, root); break; }
                case '#SIGNALS': { root = this.#SIGNALS.set(r, root); break; }
                case '#ATOMS': { root = this.#ATOMS.set(r, root); break; }
                case '#SAURON': { root = this.#SAURON.set(r, root); break; }
                case '#BUS': { root = this.#BUS.set(r, root); break; }
            }
            this.#CONTEXT.set(r, root);
        }
        let masterMap = root.get(m);
        if (!masterMap) { masterMap = new M2<A, M2<A, A>>(); root.set(m, masterMap); }
        let slaveMap = masterMap.get(s);
        if (!slaveMap) { slaveMap = new M2<K, V>(); masterMap.set(s, slaveMap); }
        return slaveMap;
    }
    // (globalThis as A)[r] //  STATE_CONTEXT === "sauron" ? : null;
    // (globalThis as A)[r] = root;
}

// #endregion
//                                                                                                             [DEATH STAR] HUB
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
/*  ===========================================================================================================================
	 Welcome to the dark side...
     The counterpart to the traditional portion of the platform, where the light side gives you features... this does not.
     But... There is something that the dark side has, that the light does not... True, raw... power. Literally as much as we
     can acquire.
     The signals solution bench comes in at being 25-1100x more performant than both preact AND alien...
     The atoms solution... that will be a bit harder to bench because, no other atoms solution even advertises any sort
     of performance metrics... across the board really. But I will install a few and bench them myself, so that is to come.
     Other bench tests that will also have to be entirely done with zero help... 
     Message queue / bus... I don't think this one is possible... because a bench test is not fair, unless all solutions
     in question are ran on the same machine. I honestly don't know of any solutions that you do not have to pay for sooo, this
     one is out.
     Application state / context, this one will be easy enough. Just download and set up a very basic version of a couple
     of platforms and run the same test on each.
     All other solution I don't think... I can bench either due to industry availability... BUT that doesn't mean I will
     not put everything on a bench anyways! So in total we have...
     - signals: 25-1100x more performant than both preact AND alien...
     - atoms: hitting 751 million ops per sec in the users usage path...
     - app context / state
     - hooks
     - message queue / bus
     - slab
     - distributed reactive state
     - spreadsheet engine
     - same node / same app transport x3
     - same node / different app transport 
     - anti message queue
     Now the reason why this is the dark side is because, each and every single solution... is entirely backed by disruptors.
     Currently, I believe that there is 6 at the time of writing with an asterisk, because the atoms solution that number is
     entirely dependant on user usage because there are disruptors available there too.

     Now there will be one slow bit of code that could not be entirely converted, due to its use being used by several other 
     parts of the application. Which is the portion of how the buses actually end up becoming a mesh. Each bus does not store
     values within its own class / domain. Instead the message queues access a cross-node module instance where most back end
     fns and the storage of all subscribers and other similar values are placed so that each bus has access to them. 
     This portion... is still optimized, just not on the same level as disrutpor level speeds because it only opearates, at this
     time anyways, at 10 million ops per second. This compared to maps actual hot path with v8, hits over 450 mill ops per sec.
     I am looking into ways of drastically improving that number, but when you operate at these numbers and have nothing 
     to go off of, its just a fucking roll of the dice. One little implementation detail could mean the difference between 4 mill 
     and 900 mill. The best disruptor I have built to date, started life as a 400k ops per sec disruptor and currently sits at
     1.2 billion, same archtecture as when it was first built.
     OTHER THAN THAT ONE SMALL PORTION OF CODE, everything else was entirely redone... even the reactive state portion ie the 
     cloud... kinda, curious to see if I was able to code it and have it working first shot... I doubt it.
    ============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE DARK SIDE] */
    class DRS {
        public hub: DEATH_STAR      
        constructor(hub: DEATH_STAR) {
            this.hub = hub
        }
    }
    class SPREADSHEET {
        public hub: DEATH_STAR
        public cell() {}
        public useCell() {}
        constructor(hub: DEATH_STAR) {
            this.hub = hub
        }
    }
export class DEATH_STAR {
    public bus: DARTH_VADER; // bus
	public signals: DARTH_MAUL; // signals
    public state: RAVEN // app context / state
	public slab: EXOGUL; // slab
	public drs: DRS; // slab
	public spreadsheet: SPREADSHEET; // slab
	public governor: GOVERNOR
	public REGISTRY: Registry;
	public MESH: Mesh;

    public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "SAURON_SLAVE_REG";
	public k: A = "__SIGNALS__";
    
	constructor(capacity = 1 << 20, REGISTRY: A = null, MESH: A = null) {
        this.REGISTRY = REGISTRY
        if (!REGISTRY) this.REGISTRY = new Registry()
        this.MESH = MESH
        if (!MESH) this.MESH = new Mesh()
        this.bus = new DARTH_VADER(this, capacity);
        this.state = new RAVEN(this, capacity);
		this.signals = new DARTH_MAUL(this, capacity);
		this.slab = new EXOGUL(this);
		this.governor = new GOVERNOR(this);
		this.drs = new DRS(this);
		this.spreadsheet = new SPREADSHEET(this);
	}
}
// darkState
// darkEffect
// darkReducer


export const death_star = new DEATH_STAR()

// #endregion
/**
 Addressing
 r = root/app key
 m = master key (component or provider id)
 s = slave key (hook slot or consumer id)
 k = payload key (state key or context key)
 
 For hooks, the natural mapping is:
 r = app/root
 m = component id
 s = hook slot index
 k = state key
 
 For context:
 r = app/root
 m = provider id
 s = consumer id
 k = context key
 */

/** BUS
 export const counter = atom_contract<{ count: number }>(() => ({
    atom: "VERTEX",
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
    bindables: [
        atom_bindable<{ increment(): void }>(({ self }) => ({
            increment() {
                self.value.count += 1;
            },
        })),
    ],
}));

const instance = counter({ count: 0 });

instance.increment();
instance.value.count; // 1

export const fastCounter = atom_contract(() => ({
    atom: "VERTEX",
    value: { count: 0 },
}));

export const counter = atom_contract(() => ({
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
}));



const push = bus.stream(
    "app",
    "telemetry",
    "gpu",
    "v1",
    (payload) => {
        console.log("gpu tick:", payload);
    },
);

push({ frame: 1 });
push({ frame: 2 });
push({ frame: 3 });

const client = bus.duplex(
    "app",
    "session",
    "ws",
    "v1",
    (message) => {
        console.log("client received:", message);
    },
);

const server = bus.duplex(
    "app",
    "session",
    "ws",
    "v1",
    (message) => {
        console.log("server received:", message);
    },
);

client.send({ hello: "server" });
server.send({ hello: "client" });

// Consumer side (subscribe)
const push = bus.stream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        // this runs for every message pushed to this address
        console.log("received:", payload);
    },
);

// Producer side (send)
push({ frame: 1 });
push({ frame: 2 });

const producer = bus.stream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        console.log("consumer got:", payload);
    },
);

producer({ frame: 1 });  // producer side

const stream = bus.openStream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        console.log("consumer got:", payload);
    },
);

stream.send({ frame: 1 });   // producer side
stream.close();              // optional teardown

// Endpoint A
const aSend = bus.stream("app", "session", "ws", "v1:a2b");
bus.subscribe("app", "session", "ws", "v1:b2a", (msg) => {
    console.log("A got:", msg);
});

// Endpoint B
const bSend = bus.stream("app", "session", "ws", "v1:b2a");
bus.subscribe("app", "session", "ws", "v1:a2b", (msg) => {
    console.log("B got:", msg);
});

// Cross-communication
aSend({ hello: "B" });   // B's handler fires
bSend({ hello: "A" });   // A's handler fires



 const bus = new MESSAGE_BUS_DISRUPTOR(4096);

 // Multiple subscribers on the same address
 const stopA = bus.subscribe("app", "user", "create", "v1", async (payload) => {
     return `A:${payload}`;
 });
 
 const stopB = bus.subscribe("app", "user", "create", "v1", (payload) => {
     return `B:${payload}`;
 });
 
 // Awaited send receives subscriber results
 const result = await bus.send("app", "user", "create", "v1", "skyler");
 
 // result === ["A:skyler", "B:skyler"]
 
 // Single-subscriber fast path
 bus.subscribe("app", "ping", "direct", "v1", (p) => `pong:${p}`);
 
 const pong = await bus.send("app", "ping", "direct", "v1", "hi");
 // pong === "pong:hi"
 
 // Fire-and-forget
 bus.post("app", "metrics", "tick", "v1", { n: 1 });
 
 // Unsubscribe
 stopA();
 stopB();
  */
