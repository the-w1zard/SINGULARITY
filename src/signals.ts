//                                                                                                                      IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Registry } from "@the-w1zard/the-registry";
// import { Mesh } from "@the-w1zard/the-mesh"
// import { darkState, useEffect } from "@the-w1zard/the-dark-side"
import { Queue } from "./comps/queue";

// #endregion
//                                                                                                                       TYPPES
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const NO_SUB = -1;
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
type RenderFn = () => void;
type CleanupFn = () => void;
type EffectFn = () => void | CleanupFn;
let Bun: A = null;
let Deno: A = null;
if (typeof (globalThis as A).Deno !== "undefined") { Deno = (globalThis as A).Deno;}
if (typeof (globalThis as A).Bun !== "undefined") {	Bun = (globalThis as A).Bun;}
export const hasBun = typeof (globalThis as A).Bun !== "undefined";
export const hasDeno = typeof (globalThis as A).Deno !== "undefined";
export const hasNode = typeof process !== "undefined" && !!process.versions?.node;
export const isBrowser = typeof window !== "undefined";
export const isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
export let _localChange = false;
export let _channel: BroadcastChannel | null = null;

export let isReact = false;
export let IS_THE_WIZARD = true;
let init_hooks_context = false;

// #endregion
//                                                                                                                [EXOGUL] SLAB
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const FREE_LIST_END = -1;
class EXOGUL {
	// SLAB IS FOR ONLY ABOVE THE 8152 WEIGHT LIMIT OR WHEN USER REQUESTED
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
	private freeRanges: { offset: number; length: number }[] = [];
	private hub: DEATH_STAR;

	constructor(DEATH_STAR: DEATH_STAR, capacity: number = 1 << 20, slabCapacity = 1 << 20) {
		this.hub = DEATH_STAR;
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
	} // Allocate a new slot from the free list. Returns the slot index, or -1 if no slots are available.
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
	} // Increment the reference count for a slot. Returns true if successful, false if the slot is not allocated.
	retain(slot: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;

		if (this.refCount[slot] === 0x7fffffff) {
			throw new Error("Reference count overflow");
		}

		this.refCount[slot]++;
		return true;
	} // Decrement the reference count for a slot. If the ref count reaches zero, the slot is freed. Returns true if the slot was freed, false otherwise.
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
	} // Write a numeric value into a slot with epoch validation. Returns true if successful, false if the slot is invalid or epoch mismatch.
	write(slot: number, value: number, epoch: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;
		if (this.refCount[slot] <= 0) return false;

		// Epoch validation: the slot's version must match the expected epoch
		if (this.versions[slot] !== epoch) return false;

		this.values[slot] = value;
		return true;
	} // Read the numeric value from a slot. Returns NaN if the slot is invalid.
	read(slot: number): number {
		if (slot < 0 || slot >= this.capacity) return NaN;
		if (!this.inUse[slot]) return NaN;

		return this.values[slot];
	} // Get the version number for a slot. Returns -1 if the slot is invalid.
	version(slot: number): number {
		if (slot < 0 || slot >= this.capacity) return -1;

		return this.versions[slot];
	} // Get the slab reference (offset and length) for a slot. Returns { offset: -1, length: 0 } if no slab data is associated.
	ref(slot: number): { offset: number; length: number } {
		if (slot < 0 || slot >= this.capacity) {
			return { offset: -1, length: 0 };
		}

		return {
			offset: this.slabOffsets[slot],
			length: this.slabLengths[slot],
		};
	} // Store variable-sized binary data in the slab for a given slot. Returns true if successful, false if insufficient slab space.
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
	} // Read the variable-sized binary data associated with a slot. Returns a subarray view (no copy) or null if no data.
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
	} // Check if the slab buffer needs compaction. Returns true if compaction would reclaim significant space.
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
	} // Compact the slab buffer, removing dead space. Invalidates any subarray views previously returned by loadSlab.
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
	} // Get statistics about the storage ring.
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
	} // Validate internal consistency (for debugging). Returns an array of error strings, empty if consistent.
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
	private get liveBytes(): number {
		let total = 0;

		for (let i = 1; i < this.capacity; i++) {
			if (this.inUse[i] && this.slabLengths[i] > 0) {
				total += this.slabLengths[i];
			}
		}

		return total;
	}
	get used(): N {
		return this.allocated - 1 - this.freeCount();
	}
	freeCount(): N {
		return this.freeHead === FREE_LIST_END ? 0 : this.walkFree();
	}
	get slabUsed(): N {
		return this.liveBytes;
	}
	get slabFree(): N {
		return this.slab.length - this.liveBytes;
	}
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
	private hub: DEATH_STAR;
	/* ==========================================================================
	   The disruptor runs hot on its own.
	   The public API never touches passes.
	   ========================================================================== */
	private tier: 0 | 1 | 2 | 3 = 0; // 25 / 50 / 75 / 100
	private running = false;
	private hibernating = false;
	private lastAction = 0;
	private windowActions = 0;
	private windowStart = 0;
	private readonly IDLE_MS = 15_000;
	private readonly TIERS = [
		256, // tier 0 -> 25%
		1_024, // tier 1 -> 50%
		4_096, // tier 2 -> 75%
		16_384, // tier 3 -> 100%
	];
	constructor(DEATH_STAR: DEATH_STAR) {
		this.hub = DEATH_STAR;
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
			this.hub.signals.DARTH_MAUL_ENGINE.pass();

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
    //                                                                                                                    CONSTANTS
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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
	public untrack = <T,>(fn: () => T): T => {
		const prev = this.setActiveSub(undefined);
		try {
			return fn();
		} finally {
			this.setActiveSub(prev);
		}
	};
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

class DARTH_MAUL {
	//                                                                                                                    CONSTANTS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public hub: DEATH_STAR;
	public DARTH_MAUL_ENGINE: DARTH_MAUL_ENGINE;

	public r: S = "this.r";
	public m: S = "this.m";
	public s: S = "this.s";
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
	//                                                                                                                         MAPS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	#maul_meta = new M2<A, A>();
	private _getMetadata(s = this.s, m = this.m, r = this.r) {
		let root = this.#maul_meta.get(r);
		if (!root) {
			root = new M2<A, Map<A, Map<A, A>>>();
			this.#maul_meta.set(r, root);
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
		DEFAULT: ATOM_CONTRACT(() => ({ atom: "VERTEX" })),
	};
	// #endregion
	//                                                                                                                          API
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public atom<T = any>(v: A, key: SignalKey = `__THE_WARLOCK_SIGNALS_${this.hub.REGISTRY._sid()}`, s = this.s, m = this.m, r = this.r, options?: SignalOptions) {
		const rk = this.create(key, v, options, s, m, r);
		const self = this;
		return {
			get value(): T {
				self._.bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			get current(): T {
				self._.bindComponentToSignal(rk, s, m, r);
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
		const registry = this.hub.wr._signals;
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
		const registry = this.hub.wr._signals;
		return registry.get(k) as ATOM<T> | UD;
	}
	public write<T = A>(k: SignalKey, value: T | ((previous: T) => T), source = "write", s = this.s, m = this.m, r = this.r): T {
		return this._.writeResolved(k, value, source, s, m, r);
	}
	public read<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this.hub.wr._signals.get(k) as ATOM<T> | undefined;

		return atom?.read();
	}
	public peek<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this.hub.wr._signals.get(k) as ATOM<T> | UD;

		return atom?.peek();
	}
	public signal<T>(initialValue?: T): { (): T | UD; (value: T): V } {
        const eng = this.DARTH_MAUL_ENGINE;
        const pid = eng.signal<T>(initialValue);          
        const fn = ((...args: [] | [T]) => eng.signalOper<T>(pid, ...args)) as A;
        fn.pid = pid;
        Object.defineProperty(fn, eng.BRAND_SIGNAL, { value: true });
        return fn;                                        
    }
	public has(k: A, s = this.s, m = this.m, r = this.r): B {
		return this.hub.wr._signals.get(k) !== undefined;
	}
	public freeze(k: A, s = this.s, m = this.m, r = this.r): void {
		const atom = this.hub.wr._signals.get(k);
		if (!atom) {
			throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
		}
		atom.setFrozen();
	}
	public unfreeze(k: A, s = this.s, m = this.m, r = this.r): void {
		const atom = this.hub.wr._signals.get(k);
		if (!atom) {
			throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
		}
		atom.setUnfrozen();
	}
	public seal(k: A, s = this.s, m = this.m, r = this.r): V {
		const meta = this.hub.wr._signals;
		const atom = meta.get(k);
		atom.setSealed();
	}
	public reset(k: A, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.wr._signals.get(k) as ATOM<A> | UD;
		if (!atom) {
			throw new Error(`[Sauron] Cannot reset unknown signal "${String(k)}"`);
		}
		atom.value = atom.initial;
		// this._writeResolved(k, atom.initial, "reset", s, m, r, "reset");
	}
	public hydrate<T>(k: A, value: T, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.wr._signals.get(k) as ATOM<T> | UD;
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
			const reg = this.hub.wr._signals;
			for (const d of deps) {
				this._sys.ensure(d, reg.get(d), s, m, r);
			}

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
			reg.set(
				k,
				this.DARTH_MAUL_ENGINE.untrack(() => c()),
			);

			if (options?.eager) {
				// Warm the computed cache once. Do not write its result back
				// into another signal; that defeats lazy recomputation.
				this.DARTH_MAUL_ENGINE.untrack(() => c());
			}
			return k;
		};
		fn.invalidate = (k: A, s = this.s, m = this.m, r = this.r): V => {
			const entry = this._getMetadata(s, m, r)._sys.get(k);
			if (!entry || !entry.isComputed) return;
			this._.writeResolved(
				k,
				this.DARTH_MAUL_ENGINE.untrack(() => entry.read()),
				"derived",
				"invalidate",
				s,
				m,
				r,
			);
		};
		return fn;
	})();
	// #endregion
	//                                                                                                                   SEND / SUB
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    #defBucket?: M2<A, A>;
    #bufs = new WM<O, A[]>(); // open batch buffers, touched only inside batches
    private bucket(s: S, m: S, r: S): M2<A, A> {
        if (s === this.s && m === this.m && r === this.r) {
            return (this.#defBucket ??= this.hub.wr.map(s, m, r, "#NAKED_SIGNALS") as M2<A, A>);
        }
        return this.hub.wr.map(s, m, r, "#NAKED_SIGNALS") as M2<A, A>;
    }
    private channel(k: A, s: S, m: S, r: S, create: B): A {
        const b = this.bucket(s, m, r);
        let sig = b.get(k);
        if (!sig && create) {
            sig = this.signal<A>(undefined);
            b.set(k, sig);
        }
        return sig;
    }
	public sub = (() => {
        const fn = <T = A,>(listener: (value: T, previous: T) => A, k: A, s = this.s, m = this.m, r = this.r) => {
            const eng = this.DARTH_MAUL_ENGINE;
            const sig = this.channel(k, s, m, r, true);
            let prev: T | UD;
            let first = true;
            let live = true;
    
            const pid = eng.effectSignal(() => {
                const env = sig();
                if (first) { first = false; return; }
                if (!env) return;
                eng.untrack(() => {
                    if (env.b !== undefined) {
                        // batched delivery: replies can't make it back, ignore return values
                        if (this.#bufs.get(sig) === env.b) this.#bufs.delete(sig);
                        for (const p of env.b) {
                            try { listener(p, prev as T); } catch (e) { console.error("[bus]", e); }
                            prev = p;
                        }
                    } else {
                        try {
                            const v = listener(env.p, prev as T);
                            if (v !== undefined) (env.r ??= []).push(v); // array only allocated if someone replies
                        } catch (e) { console.error("[bus]", e); }
                        prev = env.p;
                    }
                });
                // effect body returns nothing on purpose: a returned function would be treated as cleanup
            });
    
            return () => { if (live) { live = false; eng.dispose(pid); } };
        };
        fn.naked = (k: A, s = this.s, m = this.m, r = this.r) => this.channel(k, s, m, r, true);
        return fn;
    })();    
    public send = (() => {
        const dispatch = (p: A, k: S, s: S, m: S, r: S): A[] | UD => {
            const sig = this.channel(k, s, m, r, false);
            if (!sig) return;
    
            if (this.DARTH_MAUL_ENGINE.batchDepth > 0) {
                const buf = this.#bufs.get(sig);
                if (buf) { buf.push(p); return; }
                const b = [p];
                this.#bufs.set(sig, b);
                sig({ b });
                return; // flush is deferred, so there are no replies yet
            }
    
            const env: A = { p, r: undefined };
            sig(env);       // synchronous commit + flush: listeners have already run
            return env.r;
        };
        const fn = (p: A, k: S, s = this.s, m = this.m, r = this.r): A => dispatch(p, k, s, m, r)?.[0];
        fn.all = (p: A, k: S, s = this.s, m = this.m, r = this.r): A[] => dispatch(p, k, s, m, r) ?? [];
        return fn;
    })();
	public delete = (k: A, s = this.s, m = this.m, r = this.r): V => {
		this.hub.wr._signals.delete(k);
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
				return { reads: [...reads.keys()], result };
			} finally {
				this.activeTracer = previous;
			}
		};
		fn.effect = (fn: () => V | (() => V), s: S = this.s, m: S = this.m, r: S = this.r) => {
			const reads = new Map<SignalKey, A>();
			const runs: Array<{ run: number; reads: Map<SignalKey, A>; at: number }> = [];
			let runCount = 0;
			const tracked = new Map<SignalKey, SIGNAL_CELL>();
			const registry = this.hub.wr._signals;

			const effectDispose = effect(() => {
				runCount++;
				const before = new Map<SignalKey, A>();
				for (const [key, cell] of tracked) {
					before.set(key, cell.peek());
				}

				const result = fn();

				const touched = new Map<SignalKey, A>();

				for (const [key, cell] of tracked) {
					const value = cell.peek();
					if (!Object.is(before.get(key), value)) {
						touched.set(key, value);
					}
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
			const registry = this.hub.wr._signals;

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
			const atom = this.hub.wr._signals.get(k) as SIGNAL_CELL<T> | undefined;

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
			const atom = this.hub.wr._signals.get(k) as ATOM<T> | UD;
			if (!atom) {
				throw new Error(`[Sauron] Cannot perform surgery on unknown signal "${String(k)}"`);
			}
			if (atom.frozen) {
				throw new Error(`[Sauron] Cannot perform surgery on frozen signal "${String(k)}"`);
			}
			const previous = atom.peek(); // Structured clone gives surgery isolation without touching the reactive kernel. Primitive values pass through unchanged.
			const draft = previous !== null && typeof previous === "object" ? (structuredClone(previous) as T) : previous;
			const result = mutate(draft);
			const next = result === undefined ? draft : (result as T);
			if (Object.is(previous, next)) {
				return previous;
			}

			const source = options?.source ?? "surgery";
			const cause = options?.cause ?? `surgery:${String(k)}:${Date.now()}`;

			const written = atom.write(next);

			this.surgery.journal.push({ key: k, previous, next: written, s, m, r });

			return written;
		}; //  Undo the most recent surgery. Restoration goes through the normal write path, so computeds, effects, subscribers, history, and channels all see a real write.
		fn.journal = [] as Array<{ key: SignalKey; previous: A; next: A; s: S; m: S; r: S }>;
		fn.undo = (): A | UD => {
			const entry = this.surgery.journal.pop();

			if (!entry) return undefined;

			const atom = this.hub.wr._signals.get(entry.key) as SIGNAL_CELL | undefined;

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
			return () => {
				while (this.surgery.journal.length > mark) {
					this.surgery.undo();
				}
			};
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
						this.DARTH_MAUL_ENGINE.untrack(() => entry.read()),
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
	public _ = (() => {
		const fn = () => {};
		fn.validateSchema = <T,>(schema: SignalSchema, value: T): T => {
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
		};
		fn.serializeTuple = (tuple: A[], s = this.s, m = this.m, r = this.r): S => {
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
					parts[i] = `arr:[${this._.serializeTuple(item, s, m, r)}]`;
				} else if (type === "object" || type === "function") {
					let refId = weakMap.get(item);
					if (!refId) {
						refId = `ref:${this.hub.REGISTRY._sid()}`;
						weakMap.set(item, refId);
					}
					parts[i] = refId;
				} else {
					parts[i] = `v:${String(item)}`;
				}
			}

			return parts.join("|");
		};
		fn.hashTuple = (tuple: U[], s = this.s, m = this.m, r = this.r): S => {
			const key = this._.serializeTuple(tuple, s, m, r);
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
		};
		fn.writeResolved = <T,>(k: SignalKey, value: T | ((previous: T) => T), source: S, s = this.s, m = this.m, r = this.r, cause?: S): T => {
			const registry = this.hub.wr._signals;
			let atom = registry.get(k) as ATOM<T> | UD;

			if (!atom) {
				const initial = typeof value === "function" ? undefined : (value as T);
				atom = this.create(k, initial, undefined, s, m, r) as ATOM<T>;
			}

			if (!atom) {
				throw new Error(`[Sauron] Cannot write unknown signal "${String(k)}"`);
			}

			return atom.write(value);
		};
		fn.recordCausal = <K, V>(k: S, value: V, source: S, s = this.s, m = this.m, r = this.r, cause?: S): void => {
			const atom = this.hub.wr._signals.get(k);
			if (!atom) return;
			atom.causal.push({ key: k, value, timestamp: Date.now(), source, cause });
			if (atom.causal.length > 100) {
				atom.causal.shift();
			}
		};
		fn.bindComponentToSignal = (rk: S, s = this.s, m = this.m, r = this.r): V => {
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
		};
		fn.resolveConflict = <T,>(conflict: ConflictPolicy | undefined, previous: T, next: T, strategy: "latest" | "fail" | "accumulate" = "latest"): T | null => {
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
		};
		return fn;
	})();
	// #endregion

	//                                                                                                                        ASYNC
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public async = (() => {
		const fn = <T,>(k: A, fetcher: (signal: AbortSignal) => P<T>, s = this.s, m = this.m, r = this.r, options?: { fallback?: T }) => {
			const reg = this.hub.wr._signals;
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

				this._.writeResolved(loadingKey, true, "write", s, m, r, String(k));

				try {
					const data = await fetcher(controller.signal);
					if (!controller.signal.aborted) {
						this._.writeResolved(dataKey, data, "write", s, m, r, String(k));
						this._.writeResolved(errorKey, null, "write", s, m, r, String(k));
					}
					return data;
				} catch (error) {
					if (!controller.signal.aborted) {
						this._.writeResolved(errorKey, error, "write", s, m, r, String(k));
						atom!.lifecycle?.onError?.(error as Error);
					}
					throw error;
				} finally {
					if (!controller.signal.aborted) {
						this._.writeResolved(loadingKey, false, "write", s, m, r, String(k));
						state.controller = null;
					}
				}
			};

			atom.async = { fetcher: run, controller: null, retryCount: 0, staleValue: options?.fallback };
			void run();

			return { dataKey, loadingKey, errorKey };
		};
		fn.invalidate = <T,>(key: A, s = this.s, m = this.m, r = this.r): V => {
			const reg = this.hub.wr._signals;
			const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
			const state = atom?.async;
			if (!atom || !state) return;

			const dataKey = `${String(key)}:data`;
			const loadingKey = `${String(key)}:loading`;
			const errorKey = `${String(key)}:error`;

			this._.writeResolved(loadingKey, true, "write", s, m, r, String(key));
			this._.writeResolved(errorKey, null, "write", s, m, r, String(key));

			void Promise.resolve(state.fetcher())
				.then((data) => this._.writeResolved(dataKey, data, "write", s, m, r, String(key)))
				.catch((error) => {
					this._.writeResolved(errorKey, error, "write", s, m, r, String(key));
					atom.lifecycle?.onError?.(error as Error);
				});
		};
		fn.mutate = <T,>(key: A, optimistic: T, s = this.s, m = this.m, r = this.r): V => {
			const reg = this.hub.wr._signals;
			const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
			const state = atom?.async;
			if (!atom || !state) return;

			const dataKey = `${String(key)}:data`;
			const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
			if (!dataAtom) throw new Error(`[Sauron] Missing async data signal "${dataKey}"`);

			const prior = dataAtom.peek();
			this._.writeResolved(dataKey, optimistic, "write", s, m, r, String(key));

			void Promise.resolve(state.fetcher()).catch(() => {
				this._.writeResolved(dataKey, prior, "write", s, m, r, String(key));
			});
		};
		fn.read = <T,>(key: A, s = this.s, m = this.m, r = this.r): T => {
			const reg = this.hub.wr._signals;
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
		const fn = <T = unknown,>(key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels()): readonly HistoryEntry[] => {
			return this.time_travel.resolveHistory<T>(key, channels) ?? [];
		};
		fn.defaultChannels = (): Required<HistoryChannels> => ({
			r: this.r,
			m: this.m,
			s: "_TC_TIME_TRAVEL_",
		});
		fn.resolveChannels = (channels: HistoryChannels = this.time_travel.defaultChannels()): Required<HistoryChannels> => ({
			...this.time_travel.defaultChannels(),
			...channels,
		});
		fn.resolveHistory = <T = unknown,>(key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels()) => {
			const _history = this.hub.wr._historyMaul.get(key);
			return _history as HistoryEntry[] | undefined;
		};
		fn.replay = <T = unknown,>(key: SignalKey, value: T, channels: HistoryChannels, source: string): T => {
			const { s, m, r } = this.time_travel.resolveChannels(channels);
			this._time_travel_reverting = true;
			try {
				this.hub.state.store.set(key, value, { s: s, m: m, r: r, triggerRender: true });
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
		fn.back = <T = unknown,>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels()): T | undefined => {
			const history = this.time_travel.resolveHistory<T>(key, channels);
			if (!history?.length || steps <= 0) return undefined;

			const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
			const targetIndex = Math.max(0, index - steps);

			if (targetIndex === index) return undefined;

			this.hub.wr._timeTravelMaul.set(key, targetIndex);
			for (let i = 0; i < history.length; i++) {
				history[i].undone = i > targetIndex;
			}

			return this.time_travel.replay(key, history[targetIndex].prev as T, channels, "time-travel.back");
		};
		fn.forward = <T = unknown,>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels()): T | undefined => {
			const history = this.time_travel.resolveHistory<T>(key, channels);
			if (!history?.length || steps <= 0) return undefined;

			const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
			const targetIndex = Math.min(history.length - 1, index + steps);

			if (targetIndex === index) return undefined;

			this.hub.wr._timeTravelMaul.set(key, targetIndex);
			for (let i = 0; i < history.length; i++) {
				history[i].undone = i > targetIndex;
			}

			return this.time_travel.replay(key, history[targetIndex].next as T, channels, "time-travel.forward");
		};
		fn.goto = <T = unknown,>(key: SignalKey, sequence: number, channels: HistoryChannels = this.time_travel.defaultChannels()): T | undefined => {
			const history = this.time_travel.resolveHistory<T>(key, channels);
			if (!history?.length) return undefined;

			const targetIndex = history.findIndex((entry) => entry.k === sequence);
			const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;

			if (targetIndex < 0 || targetIndex === index) return undefined;

			this.hub.wr._timeTravelMaul.set(key, targetIndex);
			for (let i = 0; i < history.length; i++) {
				history[i].undone = i > targetIndex;
			}

			return this.time_travel.replay(key, history[targetIndex].goto as T, channels, "time-travel.goto");
		};
		return fn;
	})();
	public recording: { entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>; unsubs: Array<() => V> } | null = null;
	public timeline = (() => {
		const fn = (key: A, s = this.s, m = this.m, r = this.r) => {};
		fn.manualReplay = (entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, options?: { from?: number; speed?: number; triggerRender?: boolean }): Promise<V> => {
			const { from = 0, speed = 0, triggerRender = true } = options ?? {};
			return new Promise((resolve) => {
				let index = from;
				const step = () => {
					if (index >= entries.length) {
						resolve();
						return;
					}
					const entry = entries[index++];
					this._.writeResolved(entry.key, entry.value, entry.source ?? "replay", this.s, this.m, this.r, `replay:${index}`);
					if (speed > 0) {
						setTimeout(step, speed);
					} else {
						step();
					}
				};
				this.DARTH_MAUL_ENGINE.startBatch();
				try {
					step();
				} finally {
					this.DARTH_MAUL_ENGINE.endBatch();
				}
				if (speed === 0) {
					resolve();
				}
			});
		};
		fn.manualBack = (key: A, s = this.s, m = this.m, r = this.r): void => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.manualForward = (key: A, s = this.s, m = this.m, r = this.r): void => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.manualRecord = (keys?: SignalKey[], s: S = this.s, m: S = this.m, r: S = this.r): (() => V) => {
			/** USAGE
                const stop = sauron.record();
            
                // ...user reproduces the bug...
                
                const entries = stop();
                
                // Replay it exactly:
                await sauron.replay(entries);
                
                // Or find where reality diverged:
                sauron.bisect(entries);
                 */
			if (this.recording) {
				throw new Error("[Sauron] A recording is already active");
			}

			const registry = this.hub.wr._signals;
			const targets = keys ?? [...registry.keys()];

			const entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> = [];

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
		};
		fn.stopManualRecording = (): Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> => {
			if (!this.recording) return [];

			for (const unsub of this.recording.unsubs) {
				unsub();
			}

			const entries = this.recording.entries;
			this.recording = null;

			return entries;
		};
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
const queued: (EffectNode | UD)[] = [];
export interface ReactiveNode {
	deps?: Link;
	depsTail?: Link;
	subs?: Link;
	subsTail?: Link;
	flags: ReactiveFlags;
}
export interface Link {
	version: number;
	dep: ReactiveNode;
	sub: ReactiveNode;
	prevSub: Link | undefined;
	nextSub: Link | undefined;
	prevDep: Link | undefined;
	nextDep: Link | undefined;
}
interface Stack<T> { value: T;	prev: Stack<T> | undefined;}
export const enum ReactiveFlags {
	None = 0,
	Mutable = 1,
	Watching = 2,
	RecursedCheck = 4,
	Recursed = 8,
	Dirty = 16,
	Pending = 32,
}
export function createReactiveSystem({ update, notify, unwatched,}: { update(sub: ReactiveNode): B;	notify(sub: ReactiveNode): V; unwatched(sub: ReactiveNode): V;}) {
	return { link, unlink, propagate, checkDirty, shallowPropagate,	};
	function link(dep: ReactiveNode, sub: ReactiveNode, version: number): void {
		const prevDep = sub.depsTail;
		if (prevDep !== undefined && prevDep.dep === dep) { return; }
		const nextDep = prevDep !== undefined ? prevDep.nextDep : sub.deps;
		if (nextDep !== undefined && nextDep.dep === dep) {
			nextDep.version = version;
			sub.depsTail = nextDep;
			return;
		}
		const prevSub = dep.subsTail;
		if (prevSub !== undefined && prevSub.version === version && prevSub.sub === sub) { return; }
		const newLink
			= sub.depsTail
			= dep.subsTail
			= {
				version,
				dep,
				sub,
				prevDep,
				nextDep,
				prevSub,
				nextSub: undefined,
			};
		if (nextDep !== undefined) { nextDep.prevDep = newLink; }
		if (prevDep !== undefined) { prevDep.nextDep = newLink; }
        else { sub.deps = newLink; }
		if (prevSub !== undefined) { prevSub.nextSub = newLink; }
        else { dep.subs = newLink; }
	}
	function unlink(link: Link, sub = link.sub): Link | undefined {
		const { dep, prevDep, nextDep, nextSub, prevSub } = link;
		if (nextDep !== undefined) {
			nextDep.prevDep = prevDep;
		} else {
			sub.depsTail = prevDep;
		}
		if (prevDep !== undefined) {
			prevDep.nextDep = nextDep;
		} else {
			sub.deps = nextDep;
		}
		if (nextSub !== undefined) {
			nextSub.prevSub = prevSub;
		} else {
			dep.subsTail = prevSub;
		}
		if (prevSub !== undefined) {
			prevSub.nextSub = nextSub;
		} else if ((dep.subs = nextSub) === undefined) {
			unwatched(dep);
		}
		return nextDep;
	}
	function propagate(link: Link, innerWrite: boolean): void {
		let next = link.nextSub;
		let stack: Stack<Link | undefined> | undefined;

		top: do {
			const sub = link.sub;
			let flags = sub.flags;

			if (!(flags & (ReactiveFlags.RecursedCheck | ReactiveFlags.Recursed | ReactiveFlags.Dirty | ReactiveFlags.Pending))) {
				sub.flags = flags | ReactiveFlags.Pending;
				if (innerWrite) {
					sub.flags |= ReactiveFlags.Recursed;
				}
			} else if (!(flags & (ReactiveFlags.RecursedCheck | ReactiveFlags.Recursed))) {
				flags = ReactiveFlags.None;
			} else if (!(flags & ReactiveFlags.RecursedCheck)) {
				sub.flags = (flags & ~ReactiveFlags.Recursed) | ReactiveFlags.Pending;
			} else if (!(flags & (ReactiveFlags.Dirty | ReactiveFlags.Pending)) && isValidLink(link, sub)) {
				sub.flags = flags | (ReactiveFlags.Recursed | ReactiveFlags.Pending);
				flags &= ReactiveFlags.Mutable;
			} else {
				flags = ReactiveFlags.None;
			}

			if (flags & ReactiveFlags.Watching) { notify(sub); }

			if (flags & ReactiveFlags.Mutable) {
				const subSubs = sub.subs;
				if (subSubs !== undefined) {
					const nextSub = (link = subSubs).nextSub;
					if (nextSub !== undefined) {
						stack = { value: next, prev: stack };
						next = nextSub;
					}
					continue;
				}
			}

			if ((link = next!) !== undefined) {
				next = link.nextSub;
				continue;
			}

			while (stack !== undefined) {
				link = stack.value!;
				stack = stack.prev;
				if (link !== undefined) {
					next = link.nextSub;
					continue top;
				}
			}

			break;
		} while (true);
	}
	function checkDirty(link: Link, sub: ReactiveNode): boolean {
		let stack: Stack<Link> | undefined;
		let checkDepth = 0;
		let dirty = false;

		top: do {
			const dep = link.dep;
			const flags = dep.flags;

			if (sub.flags & ReactiveFlags.Dirty) {
				dirty = true;
			} else if ((flags & (ReactiveFlags.Mutable | ReactiveFlags.Dirty)) === (ReactiveFlags.Mutable | ReactiveFlags.Dirty)) {
				const subs = dep.subs!;
				if (update(dep)) {
					if (subs.nextSub !== undefined) { shallowPropagate(subs); }
					dirty = true;
				}
			} else if ((flags & (ReactiveFlags.Mutable | ReactiveFlags.Pending)) === (ReactiveFlags.Mutable | ReactiveFlags.Pending)) {
				stack = { value: link, prev: stack };
				link = dep.deps!;
				sub = dep;
				++checkDepth;
				continue;
			}

			if (!dirty) {
				const nextDep = link.nextDep;
				if (nextDep !== undefined) { link = nextDep; continue; }
			}

			while (checkDepth--) {
				link = stack!.value;
				stack = stack!.prev;
				if (dirty) {
					const subs = sub.subs!;
					if (update(sub)) {
						if (subs.nextSub !== undefined) {
							shallowPropagate(subs);
						}
						sub = link.sub;
						continue;
					}
					dirty = false;
				} else {
					sub.flags &= ~ReactiveFlags.Pending;
				}
				sub = link.sub;
				const nextDep = link.nextDep;
				if (nextDep !== undefined) {
					link = nextDep;
					continue top;
				}
			}

			return dirty && !!sub.flags;
		} while (true);
	}
	function shallowPropagate(link: Link): void {
		do {
			const sub = link.sub;
			const flags = sub.flags;
			if ((flags & (ReactiveFlags.Pending | ReactiveFlags.Dirty)) === ReactiveFlags.Pending) {
				sub.flags = flags | ReactiveFlags.Dirty;
				if ((flags & (ReactiveFlags.Watching | ReactiveFlags.RecursedCheck)) === ReactiveFlags.Watching) {
					notify(sub);
				}
			}
		} while ((link = link.nextSub!) !== undefined);
	}
	function isValidLink(checkLink: Link, sub: ReactiveNode): boolean {
		let link = sub.depsTail;
		while (link !== undefined) {
			if (link === checkLink) {
				return true;
			}
			link = link.prevDep;
		}
		return false;
	}
}
interface EffectScopeNode extends ReactiveNode {}
interface EffectNode extends ReactiveNode {	fn(): (() => V) | V; cleanup?: () => V;}
interface ComputedNode<T = any> extends ReactiveNode {	value: T | UD;	getter: (previousValue?: T) => T;} 
interface SignalNode<T = any> extends ReactiveNode { currentValue: T; pendingValue: T;}
const HasChildEffect = 64;
let cycle = 0;
let runDepth = 0;
let batchDepth = 0;
let notifyIndex = 0;
let queuedLength = 0;
let activeSub: ReactiveNode | UD;
const {	link, unlink, propagate, checkDirty, shallowPropagate,} = createReactiveSystem({	
    update(node: SignalNode | ComputedNode | EffectScopeNode): boolean {
		if ('getter' in node) { return updateComputed(node); }
		if ('currentValue' in node) { return updateSignal(node); }
		node.flags = ReactiveFlags.Mutable;
		return true;
	},
	notify(effect: EffectNode) {
		let insertIndex = queuedLength;
		let firstInsertedIndex = insertIndex;
		do {
			queued[insertIndex++] = effect;
			effect.flags &= ~ReactiveFlags.Watching;
			effect = effect.subs?.sub as EffectNode;
			if (effect === undefined || !(effect.flags & ReactiveFlags.Watching)) { break; }
		} while (true);

		queuedLength = insertIndex;

		while (firstInsertedIndex < --insertIndex) {
			const left = queued[firstInsertedIndex];
			queued[firstInsertedIndex++] = queued[insertIndex];
			queued[insertIndex] = left;
		}
	},
	unwatched(node: SignalNode | ComputedNode | EffectNode | EffectScopeNode) {
		if ('getter' in node) {
			if (node.depsTail !== undefined) {
				node.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
				disposeAllDepsInReverse(node);
			}
		}
		else if ('currentValue' in node) { }// Nothing to do for signals, they are always mutable and never dirty until pendingValue changes
		else if ('fn' in node) { effectOper.call(node); }
		else { effectScopeOper.call(node); }
	},
});
export function getActiveSub(): ReactiveNode | UD {	return activeSub;}
export function setActiveSub(sub?: ReactiveNode) {
	const prevSub = activeSub;
	activeSub = sub;
	return prevSub;
}
export function getBatchDepth(): N { return batchDepth;}
export function startBatch() { ++batchDepth;}
export function endBatch() { if (!--batchDepth) { flush(); }}
export function isSignal(fn: () => V): B {	return fn.name === 'bound ' + signalOper.name;}
export function isComputed(fn: () => V): B { return fn.name === 'bound ' + computedOper.name;}
export function isEffect(fn: () => V): B {	return fn.name === 'bound ' + effectOper.name;}
export function isEffectScope(fn: () => V): B {	return fn.name === 'bound ' + effectScopeOper.name;}
export function signal<T>(): { (): T | UD;	(value: T | UD): V;};
export function signal<T>(initialValue: T): { (): T; (value: T): V;};
export function signal<T>(initialValue?: T): { (): T | UD;	(value: T | UD): V;} {
	return signalOper.bind({
		currentValue: initialValue,
		pendingValue: initialValue,
		subs: undefined,
		subsTail: undefined,
		flags: ReactiveFlags.Mutable,
	}) as () => T | undefined;
}
export function computed<T>(getter: (previousValue?: T) => T): () => T {
	return computedOper.bind({
		value: undefined,
		subs: undefined,
		subsTail: undefined,
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.None,
		getter: getter as (previousValue?: unknown) => unknown,
	}) as () => T;
}
export function effect(fn: () => V | (() => V)): () => V {
	const e: EffectNode = {
		fn,
		cleanup: undefined,
		subs: undefined,
		subsTail: undefined,
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.Watching | ReactiveFlags.RecursedCheck,
	};
	const prevSub = setActiveSub(e);
	if (prevSub !== undefined) {
		link(e, prevSub, 0);
		prevSub.flags |= HasChildEffect;
	}
	try {
		++runDepth;
		const result = e.fn();
		e.cleanup = typeof result === "function" ? result : undefined;
	} finally {
		--runDepth;
		activeSub = prevSub;
		e.flags &= ~ReactiveFlags.RecursedCheck;
	}
	return effectOper.bind(e);
}
export function effectScope(fn: () => V): () => V {
	const e: EffectScopeNode = {
		deps: undefined,
		depsTail: undefined,
		subs: undefined,
		subsTail: undefined,
		flags: ReactiveFlags.Mutable,
	};
	const prevSub = setActiveSub(e);
	if (prevSub !== undefined) {
		link(e, prevSub, 0);
		prevSub.flags |= HasChildEffect;
	}
	try {
		fn();
	} finally {
		activeSub = prevSub;
	}
	return effectScopeOper.bind(e);
}
export function trigger(fn: () => V) {
	const sub: ReactiveNode = {
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.Watching | ReactiveFlags.RecursedCheck,
	};
	const prevSub = setActiveSub(sub);
	++batchDepth;
	try {
		fn();
	} finally {
		activeSub = prevSub;
		sub.flags = ReactiveFlags.None;
		let link = sub.deps;
		while (link !== undefined) {
			const dep = link.dep;
			link = unlink(link, sub);
			const subs = dep.subs;
			if (subs !== undefined) {
				propagate(subs, !!runDepth);
				shallowPropagate(subs);
			}
		}
		if (!--batchDepth) {
			flush();
		}
	}
}
function updateComputed(c: ComputedNode): B {
	if (c.flags & HasChildEffect) {
		let link = c.depsTail;
		while (link !== undefined) {
			const prev = link.prevDep;
			const dep = link.dep;
			if (!('getter' in dep) && !('currentValue' in dep)) {
				unlink(link, c);
			}
			link = prev;
		}
	}
	c.depsTail = undefined;
	c.flags = ReactiveFlags.Mutable | ReactiveFlags.RecursedCheck;
	const prevSub = setActiveSub(c);
	try {
		++cycle;
		const oldValue = c.value;
		return oldValue !== (c.value = c.getter(oldValue));
	} finally {
		activeSub = prevSub;
		c.flags &= ~ReactiveFlags.RecursedCheck;
		purgeDeps(c);
	}
}
function updateSignal(s: SignalNode): B {	
    s.flags = ReactiveFlags.Mutable;
    const previous = s.currentValue;
    const next = s.pendingValue;
    s.currentValue = next;
    return !Object.is(previous, next);
}
function run(e: EffectNode): V {
	const flags = e.flags;
	if (
		flags & ReactiveFlags.Dirty
		|| (
			flags & ReactiveFlags.Pending
			&& checkDirty(e.deps!, e)
		)
	) {
		if (flags & HasChildEffect) {
			let link = e.depsTail;
			while (link !== undefined) {
				const prev = link.prevDep;
				const dep = link.dep;
				if (!('getter' in dep) && !('currentValue' in dep)) {
					unlink(link, e);
				}
				link = prev;
			}
		}
		if (e.cleanup) {
			runCleanup(e);
			if (!e.flags) {
				return;
			}
		}
		e.depsTail = undefined;
		e.flags = ReactiveFlags.Watching | ReactiveFlags.RecursedCheck;
		const prevSub = setActiveSub(e);
		try {
			++cycle;
			++runDepth;
			const result = e.fn();
			e.cleanup = typeof result === "function" ? result : undefined;
		} finally {
			--runDepth;
			activeSub = prevSub;
			e.flags &= ~ReactiveFlags.RecursedCheck;
			purgeDeps(e);
		}
	} else if (e.deps !== undefined) {
		e.flags = ReactiveFlags.Watching | (flags & HasChildEffect);
	}
}
function flush(): V {
	try {
		while (notifyIndex < queuedLength) {
			const effect = queued[notifyIndex]!;
			queued[notifyIndex++] = undefined;
			run(effect);
		}
	} finally {
		while (notifyIndex < queuedLength) {
			const effect = queued[notifyIndex]!;
			queued[notifyIndex++] = undefined;
			effect.flags |= ReactiveFlags.Watching | ReactiveFlags.Recursed;
		}
		notifyIndex = 0;
		queuedLength = 0;
	}
}
function computedOper<T>(this: ComputedNode<T>): T {
	const flags = this.flags;
	if ( flags & ReactiveFlags.Dirty || ( flags & ReactiveFlags.Pending && ( checkDirty(this.deps!, this) || (this.flags = flags & ~ReactiveFlags.Pending, false) ) ) ) {
		if (updateComputed(this)) {
			const subs = this.subs;
			if (subs !== undefined) { shallowPropagate(subs); }
		}
	} else if (!flags) {
		this.flags = ReactiveFlags.Mutable | ReactiveFlags.RecursedCheck;
		const prevSub = setActiveSub(this);
		try {
			this.value = this.getter();
		} finally {
			activeSub = prevSub;
			this.flags &= ~ReactiveFlags.RecursedCheck;
		}
	}
	const sub = activeSub;
	if (sub !== undefined) {
		link(this, sub, cycle);
	}
	return this.value!;
}
function signalOper<T>(this: SignalNode<T>, ...value: [T]): T | V {
	if (value.length) {
		if (this.pendingValue !== (this.pendingValue = value[0])) {
			this.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
			const subs = this.subs;
			if (subs !== undefined) {
				propagate(subs, !!runDepth);
				if (!batchDepth) {
					flush();
				}
			}
		}
	} else {
		if (value.length) {
            const next = value[0];

            if (!Object.is(this.pendingValue, next)) {
                this.pendingValue = next;
                this.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
                const subs = this.subs;
                if (subs !== undefined) {
                    propagate(subs, !!runDepth);
                    if (!batchDepth) {
                        flush();
                    }
                }
            }
        } else {
            if (this.flags & ReactiveFlags.Dirty) {
                if (updateSignal(this)) {
                    const subs = this.subs;
                    if (subs !== undefined) {
                        shallowPropagate(subs);
                    }
                }
            }
            const sub = activeSub;
            if (sub !== undefined) {
                link(this, sub, cycle);
            }
            return this.currentValue;
        }
	}
}
function runCleanup(e: EffectNode): V {
	const cleanup = e.cleanup!;
	e.cleanup = undefined;
	const prevSub = activeSub;
	activeSub = undefined;
	try {
		cleanup();
	} finally {
		activeSub = prevSub;
	}
}
function effectOper(this: EffectNode): V {
	effectScopeOper.call(this);
	if (this.cleanup) {
		runCleanup(this);
	}
}
function effectScopeOper(this: EffectScopeNode): V {
	this.flags = ReactiveFlags.None;
	disposeAllDepsInReverse(this);
	const sub = this.subs;
	if (sub !== undefined) {
		unlink(sub);
	}
}
function disposeAllDepsInReverse(sub: ReactiveNode): V {
	let link = sub.depsTail;
	while (link !== undefined) {
		const prev = link.prevDep;
		unlink(link, sub);
		link = prev;
	}
}
function purgeDeps(sub: ReactiveNode) {
	const depsTail = sub.depsTail;
	let dep = depsTail !== undefined ? depsTail.nextDep : sub.deps;
	while (dep !== undefined) {
		dep = unlink(dep, sub);
	}
}
export interface BusMessage<P = A> {
	id: string;          // unique message id
	cid?: string;        // correlation id: request id being answered
	r: S;
	m: S;
	s: S;
	k: S;
	p: P;                // payload
	from?: S;            // sender identity, useful for echo suppression
	replyTo?: string;    // reply channel key
	ts: number;
}
export interface BusHandler<P = A, R = A> {
	(msg: BusMessage<P>): R | Promise<R> | void;
}
type Scope = { r: S; m: S; s: S; k: S };
type HandlerSet = Set<BusHandler>;
class BEAR {
    public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public hub: DEATH_STAR
    #SIGNAL_CELLS = new M2<A, M2<A, M2<A, A>>>();
    _signal_cells: M2<A, A>;

    _signal_listeners: M2<A, ST<(value: A) => void>>;
    _signal_global_listeners: M2<A, ST<(key: A, value: A) => void>>;
    _signal_waiters: M2<A, ST<(value: A) => void>>;

    _signal_persistent: M2<A, A>;
    _signal_persistent_keys: M2<A, A>;
    constructor(raven: DEATH_STAR) {
        this.r = raven.r ?? "THE_WIZARD_ROOT_REG";
		this.m = raven.m ?? "SAURON_MASTER_REG";

		this.s = raven.s ?? "SAURON_SLAVE_REG";
		this.k = raven.k ?? "THE_K_KEY";
		this.hub = raven
        this._signal_cells = this.map("_signal_cells_raven", this.m, this.r, "#SIGNAL_CELLS");

        this._signal_listeners = this.map("_signal_listeners_raven", this.m, this.r);
        this._signal_global_listeners = this.map("_signal_global_listeners_raven", this.m, this.r);
        this._signal_waiters = this.map("_signal_waiters_raven", this.m, this.r);

        this._signal_persistent_keys = this.map("_signal_persistent_keys_raven", this.m, this.r);
        this._signal_persistent = this.map("_signal_persistent_raven", this.m, this.r);
		this.selfId = `bus_${this.hub.REGISTRY._sid()}`
    }
    public get_root(c: S, r: S) {
		let root;
		switch (c) {
			case "#SIGNAL_CELLS": { root = this.#SIGNAL_CELLS.get(r); break; }
			case "globalThis": { (globalThis as A)[r]; break; }
		}
		if (!root) {
			root = new M2<A, M2<A, M2<A, A>>>();
			switch (c) {
				case "#SIGNAL_CELLS": { root = this.#SIGNAL_CELLS.set(r, root); break; }
				case "globalThis": { (globalThis as A)[r] = root; break; }
			}
		}
		return root;
	}
	public map<K = A, V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): M2<K, V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) { masterMap = new M2<A, M2<A, A>>(); root.set(m, masterMap); }
		let slaveMap = masterMap.get(s);
		if (!slaveMap) { slaveMap = new M2<K, V>(); masterMap.set(s, slaveMap); }
		return slaveMap;
	}
	private get<T = A>( key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },	): { (): T | UD; (value: T | UD): V } {
		const { s, m, r } = opts;
	
		let cell = this._signal_cells.get(key);
	
		if (!cell) {
			cell = signal<T>(undefined);
			this._signal_cells.set(key, cell);
		}
	
		return cell as { (): T | UD; (value: T | UD): V };
	}
	private read<T = A>( cell: { (): T | UD; (value: T | UD): V }, ): T | UD {
		const prevSub = getActiveSub();
	
		// Do not create a dependency while performing an administrative read.
		setActiveSub(undefined);
	
		try {
			return cell() as T | UD;
		} finally {
			setActiveSub(prevSub);
		}
	}
	private write<T = A>( cell: { (): T | UD; (value: T | UD): V }, value: T | UD,	): void {
		cell(value as T);
	}
	private notifyKeyListeners( key: A, value: A | UD, opts: ScopeOptions,	): void {
		const listeners = this._signal_listeners.get(key);
	
		if (!listeners) return;
	
		for (const listener of listeners) {
			try {
				listener(value);
			} catch (err) {
				console.error(`[Sauron Signal Listener Error]:`, err);
			}
		}
	}
	public scheduleGlobalNotify( key: A, value: A | UD, opts: ScopeOptions,	): void {
		const globalListeners = this._signal_global_listeners.get(
			"_signal_global_listeners_raven",
		);
	
		if (globalListeners) {
			for (const listener of globalListeners) {
				try {
					listener(key, value);
				} catch (err) {
					console.error(`[Sauron Signal Global Listener Error]:`, err);
				}
			}
		}
	
		const waiters = this._signal_waiters.get(key);
	
		if (waiters) {
			const resolved = Array.from(waiters);
			this._signal_waiters.delete(key);
	
			for (const resolve of resolved) {
				try {
					resolve(value);
				} catch (err) {
					console.error(`[Sauron Signal Waiter Error]:`, err);
				}
			}
		}
	}
	private notifyAll( key: A, value: A | UD, opts: ScopeOptions,	): void {
		this.notifyKeyListeners(key, value, opts);
		this.scheduleGlobalNotify(key, value, opts);
	}
	public atom<T = any>(v: A, k: SignalKey = `__THE_WARLOCK_SIGNALS_${this.hub.REGISTRY._sid()}`, s = this.s, m = this.m, r = this.r, ) {
		return {
			get value(): T { return this.signal(k); },
			get current(): T { return this.signal(k); },
			peek(): T { return this.signal(k); },
			set value(v: T) { this.signal(k, v); },
			set current(v: T) { this.signal(k, v); },
			set crt(v: T) { this.signal(k, v); },
			set set(v: T) { this.signal(k, v); },
		};
	}
	public store = {
		get: <T = A,>(
			key: A,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): T | UD => {
			const cell = this.get<T>(key, opts);
			const value = this.read<T>(cell);
	
			if (value !== undefined) {
				return MaybePromise(value);
			}
	
			const persistent = this._signal_persistent;
			const persistKeys = this._signal_persistent_keys;
	
			if (persistKeys.has(key) && persistent.has(key)) {
				const hydrated = persistent.get(key) as T;
				this.write(cell, hydrated);
				return MaybePromise(hydrated);
			}
	
			return MaybePromise(undefined);
		},
		set: <T = A,>(
			key: A,
			value: T | ((prev: T | UD) => T),
			opts: { triggerRender?: B; cap?: N } & ScopeOptions =
				{ s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200 },
		): T | UD => {
			const { triggerRender = true, s, m, r, cap } = opts;
			const cell = this.get<T>(key, opts);
		
			const prev = this.read<T>(cell);
			const next =
				typeof value === "function"
					? (value as (prev: T | UD) => T)(prev)
					: value;
		
			if (Object.is(prev, next)) {
				return MaybePromise(prev as T);
			}
		
			this.write(cell, next);
		
			if (this._signal_persistent_keys.has(key)) {
				this._signal_persistent.set(key, next);
			}
		
			this.notifyKeyListeners(key, next, { s, m, r });
		
			if (triggerRender) {
				this.scheduleGlobalNotify(key, next, { s, m, r });
			}
		
			return MaybePromise(next as T);
		},
		subscribe: <T = A,>(
			key: A,
			listener: (value: T) => void,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): (() => void) => {
			const cell = this.get<T>(key, opts);
			const listeners = this._signal_listeners;
	
			if (!listeners.has(key)) {
				listeners.set(key, new Set());
			}
	
			listeners.get(key)!.add(listener as (value: A) => void);
	
			return () => {
				const current = listeners.get(key);
	
				if (!current) return;
	
				current.delete(listener as (value: A) => void);
	
				if (current.size === 0) {
					listeners.delete(key);
				}
			};
		},
		delete: (key: A,opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },): B => {
			const { s, m, r } = opts;
			const cells = this._signal_cells;
			const cell = cells.get(key);
		
			const existed = cells.has(key);
		
			if (cell) {
				this.write(cell, undefined);
			}
		
			cells.delete(key);
		
			if (this._signal_persistent_keys.has(key)) {
				this._signal_persistent_keys.delete(key);
				this._signal_persistent.delete(key);
			}
		
			this.notifyKeyListeners(key, undefined, { s, m, r });
		
			if (existed) {
				this.scheduleGlobalNotify(key, undefined, { s, m, r });
			}
		
			this._signal_listeners.delete(key);
		
			return MaybePromise(existed);
		},
		entries: <T = A,>(
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): [A, T][] => {
			const cells = this._signal_cells;
			const result: [A, T][] = [];
	
			for (const [key, cell] of cells.entries()) {
				const value = this.read<T>(cell as any);
	
				if (value !== undefined) {
					result.push([key, value]);
				}
			}
	
			return MaybePromise(result);
		},	
		persist: (
			key: A,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): void => {
			const cell = this.get(key, opts);
			const value = this.read(cell);
	
			this._signal_persistent_keys.set(key, true);
	
			if (value !== undefined) {
				this._signal_persistent.set(key, value);
			}
		},	
		unpersist: (
			key: A,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): void => {
			this._signal_persistent_keys.delete(key);
			this._signal_persistent.delete(key);
		},	
		has: (
			key: A,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): B => {
			const cell = this._signal_cells.get(key);
	
			if (!cell) {
				return MaybePromise(false);
			}
	
			const value = this.read(cell as any);
	
			return MaybePromise(value !== undefined);
		},	
		clear: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }, ): void => {
			const cells = this._signal_cells;
		
			for (const [key, cell] of Array.from(cells.entries())) {
				this.write(cell as any, undefined);
				this.notifyKeyListeners(key, undefined, opts);
				this.scheduleGlobalNotify(key, undefined, opts);
			}
		
			cells.clear();
			this._signal_listeners.clear();
		},
		clearPersistent: (
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): void => {
			this._signal_persistent.clear();
			this._signal_persistent_keys.clear();
		},
		_emit: (key: A, s = this.s, m = this.m, r = this.r, ): void => {
			const opts = { s, m, r };
			const cell = this._signal_cells.get(key);
		
			if (!cell) return;
		
			const value = this.read(cell as any);
		
			this.notifyAll(key, value, opts);
		},
	};
	public state: SauronState = {
		get: <T = A,>(
			key: string | symbol,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): T | UD => {
			return MaybePromise(this.store.get<T>(key, opts) as T);
		},	
		set: <T = A,>(
			key: string | symbol,
			value: T | ((prev: T | UD) => T),
			opts: { triggerRender?: B } & ScopeOptions =
				{ s: this.s, m: this.m, r: this.r },
		): void => {
			const { s, m, r, triggerRender = true } = opts;
	
			this.store.set(key, value, {
				s,
				m,
				r,
				triggerRender,
			});
	
			return MaybePromise(value as T);
		},	
		delete: (key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }, ): B => {
			return this.store.delete(key, opts);
		},
		getByPrefix: <T = A,>(
			prefix: string,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): { key: string; value: T }[] => {
			return this.store
				.entries<T>(opts)
				.filter(([key]) => typeof key === "string" && key.startsWith(prefix))
				.map(([key, value]) => ({ key: key as string, value }));
		},
		getAll: <T = A,>(
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): Record<string | symbol, T> => {
			const result: Record<string | symbol, T> = {};
	
			for (const [key, value] of this.store.entries<T>(opts)) {
				result[key] = value;
			}
	
			return result;
		},
		subscribe: <T = A,>(
			key: string | symbol,
			listener: (value: T) => void,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): (() => void) => {
			return this.store.subscribe<T>(key, listener, opts);
		},
		subscribeAll: (
			listener: (key: string | symbol, value: A) => void,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): (() => void) => {
			const { s, m, r } = opts;
			const listeners = this._signal_global_listeners;
	
			if (!listeners.has("_signal_global_listeners_raven")) {
				listeners.set("_signal_global_listeners_raven", new Set());
			}
	
			const set = listeners.get("_signal_global_listeners_raven")!;
			set.add(listener as any);
	
			return () => {
				set.delete(listener as any);
	
				if (set.size === 0) {
					listeners.delete("_signal_global_listeners_raven");
				}
			};
		},
		clear: (
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): void => {
			const { s, m, r } = opts;
	
			this.store.clear(opts);
	
			this._signal_waiters.clear();
	
			const globalListeners = this._signal_global_listeners.get(
				"_signal_global_listeners_raven",
			);
	
			globalListeners?.clear();
		},
		waitFor: <T = A,>(
			key: string | symbol,
			opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
		): Promise<T> => {
			const existing = this.state.get<T>(key, opts);
	
			if (existing !== undefined) {
				return Promise.resolve(existing);
			}
	
			return new Promise<T>((resolve) => {
				const waiters = this._signal_waiters;
	
				if (!waiters.has(key)) {
					waiters.set(key, new Set());
				}
	
				waiters.get(key)!.add(resolve as A);
			});
		},
	};
	public cell = (() => {
		const fn = <T = A>(key: A, s?: S, m?: S, r?: S) => {
			return MaybePromise(this.state.get<T>(key, { s, m, r }));
		};
		fn.getAll= <T = A>(opts?: ScopeOptions): Record<S | SY, T> =>{
			return this.state.getAll<T>(opts);
		}
		fn.set=<T = A>(label: A, value: T, triggerRender = true, s?: S, m?: S, r?: S) =>{
			this.cell._broadcastSS(label, value, "sauron", "set");
			return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
		}
		fn._broadcastSS=(key: A, value: A, storageType: S, fn: S, path?: S, domain?: S, options?: A) =>{
			_localChange = true;
			_channel?.postMessage({ key, value, storageType, fn, path, domain, options });
		}
		fn.subAll=(fn: (key: S | symbol, value: A) => V, s?: S, m?: S, r?: S): () => V =>{
			if (!fn) return () => {};
			return this.state.subscribeAll(fn, { s, m, r });
		}
		fn.sub=<T = A>(k: A, fn: (value: T) => void, s?: S, m?: S, r?: S): () => void =>{
			if (!k || !fn) return () => {};
			return MaybePromise(this.state.subscribe<T>(k, fn, { s, m, r }));
		}
		fn.delete=(label: A, s?: S, m?: S, r?: S): B =>{
			if (!label) return false;
			const v = this.state.delete(label, { s, m, r });
			this.cell._broadcastSS(label, undefined, "sauron", "delete");
			return v;
		}
		fn.waitFor=<T = A>(label: S, s?: S, m?: S, r?: S): P<T> =>{
			return this.state.waitFor<T>(label, { s, m, r });
		}
		fn.deleteMany= async(keys: A[], s?: S, m?: S, r?: S): P<B> =>{
			if (!Array.isArray(keys) || keys.length === 0) {
				return false;
			}
			let allDeleted = true;
	
			// Batch all deletes without triggering renders until the end
			for (let i = 0; i < keys.length; i++) {
				const deleted = this.cell.delete(keys[i], s, m, r);
				if (!deleted) {
					allDeleted = false;
				}
			}
	
			return allDeleted;
		}
		fn.setMany=async<T = Record<S, A>>(entries: T): Promise<void>=> {
			if (!entries || typeof entries !== "object") {
				return;
			}
			// Batch all sets without triggering renders until the end
			const entriesArray = Object.entries(entries);
			for (let i = 0; i < entriesArray.length; i++) {
				const [key, value] = entriesArray[i];
				// Only trigger render on the last item to avoid multiple renders
				const triggerRender = i === entriesArray.length - 1;
				this.cell.set(key, value, triggerRender);
			}
		}
		fn.getMany=async<T = A>(keys: S[], s?: S, m?: S, r?: S) =>{
			if (!Array.isArray(keys) || keys.length === 0) {
				return {};
			}
	
			const result: Record<S, T | undefined> = {};
	
			const promises = keys.map(async (key) => {
				const value = await this.cell<T>(key, s, m, r);
				return { key, value };
			});
	
			const results = await Promise.all(promises);
	
			for (const { key, value } of results) {
				result[key] = value;
			}
	
			return result;
		}
		return fn;
	})();
	//                                                                                                   DISTRIBUTED REACTIVE STATE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public state_setter(v: U, k: A = `_USECELL`, s: S = this.s, m: SU = this.m, r: SU = this.r) {
		const existing = this.state.get(k, { s: s, m: m, r: r });
		if (existing !== undefined) return existing;
		const t = typeof v === "function" ? v() : v;
		this.state.set(k, t, { triggerRender: true, s: s, m: m, r: r });
		return t;
	}
	public signal = (() => {
		const fn = <T = A,>(k: SignalKey, v?: T | (() => T)) => {
			const registry = this._signal_cells
			let atom = registry.get(k);

			if (atom && v === undefined) { return atom; }
			if (atom && v) { 
                const t = UF(v)
                atom = registry.set(k, t)
                return atom;
             }

			const initialValue = UF(v)
			atom = signal<T>(initialValue);
			registry.set(k, atom);
			return atom;
		};
		return fn;
	})();
	public useCell = (() => {
		const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = darkState(() => { return this.state_setter(v, k, s, m, r); });

			const senderTokenRef = this.atom(this.hub.REGISTRY._sid());

			this.hub.bus.sub( (i: A) => {
					const incoming = i.p;
					if (incoming?.from === senderTokenRef.value) return;
					this.state.set(k, incoming.value, { triggerRender: false, s: s, m: m, r: r });
					// set(k, incoming.value, false, s, m, r);
					setState(incoming.value);
			 }, k, s, m, 	r, ); // stay in sync with local writes too (including our own setLive below)
			this.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r });
			// sub(k, (newValue: any) => setState(newValue), s, m, r);

			const setLive = (next: any) => {
				const newValue = typeof next === "function" ? next(this.state.get(k, { s: s, m: m, r: r })) : next;
				setState(newValue);
				this.state.set(k, newValue, { triggerRender: true, s: s, m: m, r: r });
				// set(k, newValue, true, s, m, r); // local write
				this.hub.bus.send({ value: newValue, from: senderTokenRef.value }, k, s, m, r); // broadcast to the bus
			};

			return [state, setLive] as const;
		};
		fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = darkState(() => { return this.state_setter(v, k, s, m, r); });

			const senderTokenRef = this.atom(this.hub.REGISTRY._sid());
			const queueRef = this.atom<Queue | null>(null);

			if (!queueRef.current) {
				queueRef.current = new Queue(1);
			}

			// mailbox — replace the whole useEffect block:
			this.hub.bus.sub( (i: A) => {
			    const incoming = i.p;
			    if (incoming?.from === senderTokenRef.value) return;
			    queueRef.current!.run(async () => {
						const current = this.state.get(k, { s: s, m: m, r: r });
						const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
						this.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
						return next;
			    });
			}, k, s, m, r, );

			this.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r });

			const setLive = (next: any) => {
				const t = typeof next === "function" ? next() : next;
				this.hub.bus.send({ value: t, from: senderTokenRef.value }, k, s, m, r);
			};

			return [state, setLive] as const;
		};
		return fn;
	})();
	// #endregion
	//                                                                                                           ANTI MESSAGE QUEUE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public useAntiMsgQ = (() => {
		const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = darkState(() => { return this.state_setter(v, k, s, m, r); });

			const senderTokenRef = this.atom(this.hub.REGISTRY._sid()); // this.j.root._id.b();

			this.hub.bus.sub( (i: A) => {
					const incoming = i.p;
					if (incoming?.from === senderTokenRef.value) return;
					this.state.set(k, incoming.value, { triggerRender: false, s: s, m: m, r: r });
					// set(k, incoming.value, false, s, m, r);
					setState(incoming.value);
			}, k, s, m, r); // stay in sync with local writes too (including our own setLive below)
			this.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r });
			// sub(k, (newValue: any) => setState(newValue), s, m, r);

			const setLive = (next: any) => {
				const newValue = typeof next === "function" ? next(this.state.get(k, { s: s, m: m, r: r })) : next;
				setState(newValue);
				this.state.set(k, newValue, { triggerRender: true, s: s, m: m, r: r });
				// set(k, newValue, true, s, m, r); // local write
				this.hub.bus.send({ value: newValue, from: senderTokenRef.value }, k, s, m, r); // broadcast to the bus
			};

			return [state, setLive] as const;
		};
		fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = darkState(() => {
				return this.state_setter(v, k, s, m, r);
			});

			const senderTokenRef = this.atom(this.hub.REGISTRY._sid());
			const queueRef = this.atom<Queue | null>(null);

			if (!queueRef.current) {
				queueRef.current = new Queue(1);
			}

			// mailbox — replace the whole useEffect block:
			this.hub.bus.sub(
				(i: A) => {
					const incoming = i.p;
					if (incoming?.from === senderTokenRef.value) return;
					queueRef.current!.run(async () => {
						const current = this.state.get(k, { s: s, m: m, r: r });
						const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
						this.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
						return next;
					});
				},
				k,
				s,
				m,
				r,
			);

			this.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r });

			const setLive = (next: any) => {
				const t = typeof next === "function" ? next() : next;
				this.hub.bus.send({ value: t, from: senderTokenRef.value }, k, s, m, r);
			};

			return [state, setLive] as const;
		};
		return fn;
	})();
	// #endregion
	private channels = new M<S, HandlerSet>();
	private pending = new M<S, { resolve: (v: A) => V; reject: (e: A) => void; timer?: A }>();
  
	private seq = 0;
	private selfId: S 
    private defaultTimeout = 5000
	private channelKey({ r, m, s, k }: Scope): S { return `${r}::${m}::${s}::${k}`; }
	private id(): string { return `${this.selfId}_${Date.now().toString(36)}_${(this.seq++).toString(36)}`;	}
	public sub<P = A, R = A>( handler: BusHandler<P, R>, k: S, s: S, m: S, r: S, ): () => void {
		const key = this.channelKey({ r, m, s, k });
	
		let set = this.channels.get(key);
		if (!set) {
		  set = new Set();
		  this.channels.set(key, set);
		}
	
		set.add(handler as BusHandler);
	
		return () => {
		  const current = this.channels.get(key);
		  if (!current) return;
	
		  current.delete(handler as BusHandler);
	
		  if (current.size === 0) this.channels.delete(key);
		};
	}
	public send<Z = A, R = A>( payload: Z, k: S, s: S, m: S, r: S, opts: { timeout?: N; from?: S } = {}, ): P<R> {
		const cid = this.id();
		const timeout = opts.timeout ?? this.defaultTimeout;
	
		return new P<R>((resolve, reject) => {
		  const timer = setTimeout(() => {
			this.pending.delete(cid);
			reject(new Error(`[mini-bus] request timed out: ${k} (${cid})`));
		  }, timeout);
	
		  this.pending.set(cid, { resolve, reject, timer });
	
		  try {
			this.dispatch({ id: cid, cid, r, m, s, k, p: payload, from: opts.from ?? this.selfId, replyTo: `${r}::${m}::${s}::${k}::reply`, ts: Date.now(), });
		  } catch (err) {
			clearTimeout(timer);
			this.pending.delete(cid);
			reject(err);
		  }
		});
	}
	public reply<R = A>( request: BusMessage, value: R | Promise<R>, ): void {
		if (!request.cid) return;
	
		const pending = this.pending.get(request.cid);
		if (!pending) return;
	
		this.pending.delete(request.cid);
	
		if (pending.timer) clearTimeout(pending.timer);
	
		Promise.resolve(value).then(pending.resolve, pending.reject);
	  }
	private dispatch(msg: BusMessage): void {
		const handlers = this.channels.get(this.channelKey(msg));
		if (!handlers) return;
	
		for (const handler of Array.from(handlers)) {
		  try {
			const result = handler(msg);
	
			// Auto-reply when the handler returns a value and the sender expects one.
			if (msg.cid && result !== undefined) {
			  this.reply(msg, result);
			}
		  } catch (err) {
			if (msg.cid) this.reply(msg, Promise.reject(err));
			else console.error("[mini-bus] handler error:", err);
		  }
		}
	  }
}
// #endregion
//                                                                                                            [DARTH VADER] BUS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class DARTH_VADER {
	//                                                                                                                  [CONSTANTS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	private capacity: number;
	private hub: DEATH_STAR;
	// ======================================================================
	// Channel table: open-addressed hash of (r, m, s, k)
	// ======================================================================
	private channelMask: number;
	private channelKey: Int32Array; // hash bucket -> channel id or -1
	private channelState: Uint8Array; // 0 = empty, 1 = used, 2 = deleted
	private channelHash: Int32Array; // stored hash to speed probing
	private channelCount = 1; // 0 reserved
	// Per-channel subscriber list (intrusive linked list)
	private channelSubHead: Int32Array;
	private channelSubTail: Int32Array;
	// ======================================================================
	// Subscriber pool
	// ======================================================================
	private subHandler: (Handler | undefined)[];
	private subNext: Int32Array; // next subscriber in channel
	private subPrev: Int32Array; // prev subscriber in channel
	private subChannel: Int32Array; // owning channel
	private subCount = 1; // 0 reserved
	// ======================================================================
	// Message ring buffer
	// ======================================================================
	private msgChannel: Int32Array;
	private msgPayload: unknown[];
	private msgDeferred: (Deferred | undefined)[];
	private msgHead = 0; // next message to consume
	private msgTail = 0; // next free slot
	private msgCount = 0;
	// Backpressure waiters
	private capacityWaiters: Deferred[] = [];
	// Drain scheduling
	private draining = false;
	private queuedMicrotask = false;
	constructor(DEATH_STAR: DEATH_STAR, capacity = 65_536) {
		this.hub = DEATH_STAR;
		this.r = DEATH_STAR.r;
		this.m = DEATH_STAR.m;
		this.s = DEATH_STAR.s;
		this.k = DEATH_STAR.k;
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
	private hashAddress(k: S | N, s: S | N, m: S | N, r: S | N): N {// FNV-1a style; cheap, stable, and allocation-free for primitives.
		
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
    private subFreeList: N[] = [];
    private allocateSubscriber(): N {
        const reused = this.subFreeList.pop();

        if (reused !== undefined) {
            return reused;
        }

        const subId = this.allocateSubscriber();

        if (subId >= this.capacity) {
            throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
        }

        return subId;
    }
    private releaseSubscriber(subId: N): V {
        this.subHandler[subId] = undefined;
        this.subChannel[subId] = -1;
        this.subNext[subId] = NO_SUB;
        this.subPrev[subId] = NO_SUB;

        this.subFreeList.push(subId);
    }
	// #endregion
	//                                                                                                                   [CHANNELS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	private findChannel(hash: N): N {
		let slot = hash & this.channelMask;

		while (true) {
			const state = this.channelState[slot];

			if (state === 0) return -1;

			if (state === 1 && this.channelHash[slot] === hash) {
				return this.channelKey[slot];
			}

			slot = (slot + 1) & this.channelMask;
		}
	}
	private createChannel(hash: N): N {
		if (this.channelCount >= this.capacity) {
			this.grow();
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
	public sub(handler: Handler, k: S | N, s: S | N, m: S | N, r: S | N): () => V {
		if (typeof handler !== "function") { throw new TypeError("handler must be a function"); }
        if (this.subCount >= this.capacity) { this.grow();  }
        
		const channelId = this.getOrCreateChannel(r, m, s, k);
		const subId = this.allocateSubscriber();

		if (subId >= this.capacity) { throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted"); }

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
        this.releaseSubscriber(subId);
	}
	// #endregion
	//                                                                                                                       [SEND]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public post(payload: U, k: A, s: S | N, m: S | N, r: S | N): V {/** Fire-and-forget enqueue. Never awaits handler results. */
		const channelId = this.getOrCreateChannel(r, m, s, k);
		this.enqueue(channelId, payload, undefined);
	}
    public send = (() => {
		const fn =  <T = A> (payload: U, k: A, s: S | N, m: S | N, r: S | N) => {
            const channelId = this.getOrCreateChannel(r, m, s, k);
            while (this.msgCount >= this.capacity) { this.grow(); }
            let resolve!: (value: U) => V;
            let reject!: (reason?: U) => V;
            const deferred: Deferred = { resolve: (v) => resolve(v), reject: (e) => reject(e), };
            deferred.resolve = resolve;
            deferred.reject = reject;
    
            let v: A = this.enqueue(channelId, payload, deferred);
            if (v !== undefined && !(v instanceof Promise)) {
				return MaybePromise<T>(v);
			} else {
                const promise = new Promise<U>((res, rej) => { resolve = res; reject = rej; });
                return MaybePromise<T>(promise);
			}
        }
        fn.switch = <T = A> (p: U, k: A, s: S = this.s, m: S = this.m, r: S = this.r,  adapter = "ws") => {
            const msg: SIG_BUS_Message = { id: this.hub.REGISTRY._sid(), r: r, m: m, s: s, k: k, p: { p }, ts: Date.now(), _ack: false };

			let v: A = this.DISPATCH(msg, adapter);
			if (v !== undefined && !(v instanceof Promise)) {
				return MaybePromise<T>(v);
			} else {
				const timeoutMs = 5000;
				return MaybePromise(
					(() => {
						return new Promise<T>((resolve, reject) => {
							let timer: RET<typeof setTimeout> | UD;
							if (v) {
								if (timer) clearTimeout(timer);
								resolve(v as T);
								return;
							}
							if (timeoutMs) {
								timer = setTimeout(() => { reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`)); }, timeoutMs);
							}
						});
					})(),
				);
			}
        }
        fn.original_send = async (payload: U, k: A, s: S | N, m: S | N, r: S | N): P<U> => {
            const channelId = this.getOrCreateChannel(r, m, s, k);

            while (this.msgCount >= this.capacity) { this.grow(); }
    
            let resolve!: (value: U) => V;
            let reject!: (reason?: U) => V;
    
            const deferred: Deferred = { resolve: (v) => resolve(v), reject: (e) => reject(e), };
    
            const promise = new Promise<unknown>((res, rej) => { resolve = res; reject = rej; });
    
            deferred.resolve = resolve;
            deferred.reject = reject;
    
            this.enqueue(channelId, payload, deferred);
    
            return promise;
        }
	return fn;
	})();
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
	private enqueue(channelId: N, payload: U, deferred: Deferred | UD): V {
		if (this.msgCount >= this.capacity) { this.grow();	}

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
	public stream(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>): (payload?: U) => V {// One-way continuous stream. Returns a sender function. Every call pushes a message to every subscriber handler attached to the same address.
		const channelId = this.getOrCreateChannel(r, m, s, k);

		// Register consumer handler as a normal subscriber.
		this.subscribeRaw(channelId, handler);

		// Sender pushes to the same channel; fire-and-forget.
		return (payload?: unknown) => {
			this.enqueueStream(channelId, payload);
		};
	}
	public openStream(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>): { send: (payload?: U) => V; close: () => V } {
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
 	public duplex(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>): { send: (payload?: U) => V; close: () => V } {// Constant two-way stream. Each endpoint can send and receive simultaneously. The handler receives inbound messages; `send()` pushes outbound messages.
		// Create two directional channels:
		//   outbound: this endpoint -> peer
		//   inbound:  peer -> this endpoint
		const outboundChannel = this.getOrCreateChannel(r, m, s, `${k}:out`);

		const inboundChannel = this.getOrCreateChannel(r, m, s, `${k}:in`);

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
	private subscribeRaw(channelId: N, handler: (payload: U) => U | P<U>): N {
        if (this.subCount >= this.capacity) {  this.grow();  }
		const subId = this.allocateSubscriber();
		if (subId >= this.capacity) { throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted"); }
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
	public get pending(): N {
		return this.msgCount;
	}
	public get subscriberCount(): N {
		return this.subCount - 1;
	}
	public get getChannelCount(): N {
		return this.channelCount - 1;
	}
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
				if (deferred) { deferred.resolve(result); }
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
	private async dispatch(channelId: N, payload: U): P<U> {
		let sub = this.channelSubHead[channelId];
		if (sub === NO_SUB) return undefined;
		if (this.subNext[sub] === NO_SUB) {// Fast path: exactly one subscriber.
			const handler = this.subHandler[sub];
			if (!handler) return undefined;
			return await handler(payload);
		}
		const results: unknown[] = [];// Multiple subscribers: preserve subscription order.
		const promises: Promise<unknown>[] = [];
		let hasAsync = false;
		while (sub !== NO_SUB) {
			const handler = this.subHandler[sub];
			if (handler) {
				const result = handler(payload);
				if (result && typeof (result as PromiseLike<unknown>).then === "function") {
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
			let promiseIndex = 0;// Merge async results back into their original slots.
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
		for (const waiter of waiters) { waiter.resolve(undefined); }
	}
    private grow(): void {
        const oldCapacity = this.capacity;
        const newCapacity = oldCapacity * 2;
        const newMask = newCapacity - 1;
    
        const channelKey = new Int32Array(newCapacity).fill(-1);
        const channelState = new Uint8Array(newCapacity);
        const channelHash = new Int32Array(newCapacity);
    
        const channelSubHead = new Int32Array(newCapacity).fill(NO_SUB);
        const channelSubTail = new Int32Array(newCapacity).fill(NO_SUB);
    
        // Rehash existing channels directly from the old table.
        for (let oldSlot = 0; oldSlot < oldCapacity; oldSlot++) {
            if (this.channelState[oldSlot] !== 1) continue;
    
            const hash = this.channelHash[oldSlot];
            const channelId = this.channelKey[oldSlot];
    
            let newSlot = hash & newMask;
    
            while (channelState[newSlot] === 1) {
                newSlot = (newSlot + 1) & newMask;
            }
    
            channelState[newSlot] = 1;
            channelHash[newSlot] = hash;
            channelKey[newSlot] = channelId;
    
            channelSubHead[channelId] = this.channelSubHead[channelId];
            channelSubTail[channelId] = this.channelSubTail[channelId];
        }
    
        const subHandler = new Array<Handler | undefined>(newCapacity);
        const subNext = new Int32Array(newCapacity).fill(NO_SUB);
        const subPrev = new Int32Array(newCapacity).fill(NO_SUB);
        const subChannel = new Int32Array(newCapacity).fill(-1);
    
        for (let subId = 0; subId < this.subCount; subId++) {
            subHandler[subId] = this.subHandler[subId];
            subNext[subId] = this.subNext[subId];
            subPrev[subId] = this.subPrev[subId];
            subChannel[subId] = this.subChannel[subId];
        }
    
        const msgChannel = new Int32Array(newCapacity).fill(-1);
        const msgPayload: unknown[] = new Array(newCapacity);
        const msgDeferred: (Deferred | undefined)[] = new Array(newCapacity);
    
        // Copy the ring in FIFO order, preserving wraparound.
        for (let i = 0; i < this.msgCount; i++) {
            const oldSlot = (this.msgHead + i) % oldCapacity;
    
            msgChannel[i] = this.msgChannel[oldSlot];
            msgPayload[i] = this.msgPayload[oldSlot];
            msgDeferred[i] = this.msgDeferred[oldSlot];
        }
    
        this.channelKey = channelKey;
        this.channelState = channelState;
        this.channelHash = channelHash;
    
        this.channelSubHead = channelSubHead;
        this.channelSubTail = channelSubTail;
    
        this.subHandler = subHandler;
        this.subNext = subNext;
        this.subPrev = subPrev;
        this.subChannel = subChannel;
    
        this.msgChannel = msgChannel;
        this.msgPayload = msgPayload;
        this.msgDeferred = msgDeferred;
    
        this.channelMask = newMask;
        this.capacity = newCapacity;
    
        this.msgHead = 0;
        this.msgTail = this.msgCount;
    }
	// #endregion
	//                                                                                                                 [Drain loop]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
}
// #endregion
// #endregion
//                                                                                                                [RAVEN] STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export let _root: ComponentInstance = {
	id: "THE_WIZARD_ROOT_REG",
	hooks: new Map(),
	unmount: new Set(),
	contextProviders: new Map(),
};
const queues: Record<Priority, Map<symbol | string, ScheduledTask[]>> = {
	immediate: new Map(),
	"user-blocking": new Map(),
	normal: new Map(),
	background: new Map(),
	idle: new Map(),
};
type Slot<P> = { sequence: number; key: unknown; payload: P | undefined; tombstone: boolean };
const nextPow2 = (n: number) => 1 << Math.ceil(Math.log2(Math.max(2, n)));
class Disruptor<P> {
	private readonly mask: number;
	private readonly buffer: Slot<P>[];
	public cursor = -1;

	constructor(public readonly capacity: number) {
		const size = nextPow2(capacity);
		this.mask = size - 1;
		this.buffer = Array.from({ length: size }, () => ({ sequence: -1, key: undefined, payload: undefined, tombstone: false }));
	}

	/** The slot that WILL be claimed next — peek before you commit to it. */
	peekNext(): Slot<P> {
		return this.buffer[(this.cursor + 1) & this.mask];
	}

	publish(key: unknown, payload: P | undefined, tombstone = false): number {
		const seq = this.cursor + 1;
		const slot = this.buffer[seq & this.mask];
		slot.sequence = seq;
		slot.key = key;
		slot.payload = payload;
		slot.tombstone = tombstone;
		this.cursor = seq;
		return seq;
	}

	read(seq: number): Slot<P> | undefined {
		if (seq < 0 || seq > this.cursor) return undefined;
		const slot = this.buffer[seq & this.mask];
		return slot.sequence === seq ? slot : undefined;
	}
}
function assertEncodable(value: unknown): void {
	if (typeof value === "function") {
		throw new TypeError("[RAVEN] state values must be plain data — functions can't be stored here.");
	}
}
function encode<T>(value: T): T {
	assertEncodable(value);
	if (value === null || typeof value !== "object") return value;
	return (typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value))) as T;
}
function decode<T>(value: T): T {
	if (value === null || typeof value !== "object") return value;
	return (typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value))) as T;
}
class MDisruptor<K = any, V = any> {
	private ring: Disruptor<V>;
	private index = new Map<K, number>();

	constructor(initialCapacity = 256) {
		this.ring = new Disruptor<V>(initialCapacity);
	}

	get size() {
		return this.index.size;
	}

	get(key: K): V | undefined {
		const seq = this.index.get(key);
		if (seq === undefined) return undefined;
		const slot = this.ring.read(seq);
		if (!slot || slot.tombstone) return undefined;
		return decode(slot.payload as V);
	}

	has(key: K): boolean {
		const seq = this.index.get(key);
		if (seq === undefined) return false;
		const slot = this.ring.read(seq);
		return !!slot && !slot.tombstone;
	}

	set(key: K, value: V): this {
		this.growIfNeeded();
		// Evict-and-carry: whatever currently sits in the slot we're about
		// to claim gets relocated forward first if it's still a live key,
		// so a full ring never loses data on wraparound.
		while (true) {
			const occupant = this.ring.peekNext();
			const stillLive = occupant.sequence >= 0 && !occupant.tombstone && this.index.get(occupant.key as K) === occupant.sequence;
			if (!stillLive) break;
			const movedSeq = this.ring.publish(occupant.key, occupant.payload, false);
			this.index.set(occupant.key as K, movedSeq);
		}
		const seq = this.ring.publish(key, encode(value), false);
		this.index.set(key, seq);
		return this;
	}

	delete(key: K): boolean {
		// No need to touch the ring at all — removing the index entry is
		// enough; the slot is reclaimed lazily the next time it's evicted.
		return this.index.delete(key);
	}

	clear(): void {
		this.index.clear();
	}

	entries(): [K, V][] {
		const out: [K, V][] = [];
		for (const key of this.index.keys()) {
			const v = this.get(key);
			if (v !== undefined) out.push([key, v]);
		}
		return out;
	}

	private growIfNeeded(): void {
		if (this.index.size / this.ring.capacity < 0.75) return;
		const grown = new Disruptor<V>(this.ring.capacity * 2);
		for (const [key] of this.index) {
			const v = this.get(key); // decode current value
			if (v === undefined) continue;
			const seq = grown.publish(key, encode(v), false);
			this.index.set(key, seq);
		}
		this.ring = grown;
	}
}
class Lane<V> {
	private ring: Disruptor<V>;
	private writes = 0;
	constructor(public readonly capacity: number) {
		this.ring = new Disruptor<V>(capacity);
	}
	get isFull(): boolean {
		return this.writes >= this.capacity;
	}
	get pressure(): number {
		return this.writes / this.capacity;
	}
	write(key: unknown, value: V): number {
		this.writes++;
		return this.ring.publish(key, value);
	}
	readAt(seq: number): V | undefined {
		const slot = this.ring.read(seq);
		return slot && !slot.tombstone ? slot.payload : undefined;
	}
	/** Day 0 gets physically rewritten here — we don't tombstone the old
	 *  ring, we just stop referencing it. The old Disruptor object (and
	 *  everything it held) is garbage the moment nothing points at it. */
	reset(): void {
		this.writes = 0;
		this.ring = new Disruptor<V>(this.capacity);
	}
}
interface IndexEntry {
	lane: number;
	seq: number;
}
class MTracer<K = any, V = any> {
	private lanes: Lane<V>[];
	private index = new Map<K, IndexEntry>();
	private dial = 0;

	/** The tracer particle itself — ticks once per completed lap, forever.
	 *  Nobody reads Date.now() anywhere in this class. */
	public revolution = 0;

	constructor(
		private readonly laneCapacity = 8192,
		private readonly dialSize = 7, // the "7 day" window — your dial
	) {
		this.lanes = Array.from({ length: dialSize }, () => new Lane<V>(laneCapacity));
	}

	private get activeLane(): Lane<V> {
		return this.lanes[this.dial];
	}

	private advanceDialIfFull(): void {
		if (!this.activeLane.isFull) return;
		this.revolution++;
		this.dial = (this.dial + 1) % this.dialSize;
		// We've swept back around to the lane that was "day 0". Reset it —
		// this is the rewrite — then drop any index entries still pointing
		// at it so get() can't hand back memory that no longer belongs to them.
		this.lanes[this.dial].reset();
		for (const [key, entry] of this.index) {
			if (entry.lane === this.dial) this.index.delete(key);
		}
	}

	set(key: K, value: V): void {
		this.advanceDialIfFull();
		const seq = this.activeLane.write(key, encode(value));
		this.index.set(key, { lane: this.dial, seq });
	}

	get(key: K): V | undefined {
		const entry = this.index.get(key);
		if (!entry) return undefined;
		const payload = this.lanes[entry.lane].readAt(entry.seq);
		if (payload === undefined) {
			this.index.delete(key);
			return undefined;
		}
		return decode(payload);
	}

	has(key: K): boolean {
		return this.index.has(key) && this.get(key) !== undefined;
	}
	delete(key: K): boolean {
		return this.index.delete(key);
	}
	clear(): void {
		this.index.clear();
		this.lanes.forEach((l) => l.reset());
		this.dial = 0;
	}
	entries(): [K, V][] {
		const out: [K, V][] = [];
		for (const key of this.index.keys()) {
			const v = this.get(key);
			if (v !== undefined) out.push([key, v]);
		}
		return out;
	}

	/** Free backpressure: how close the active lane is to ticking the dial
	 *  over. A producer driving this toward 1 while nobody's reading is
	 *  exactly "queue depth" — except no timer instrumented it. */
	get pressure(): number {
		return this.activeLane.pressure;
	}
}
class STracer<V = any> {
	private lanes: Lane<true>[];
	private index = new Map<V, IndexEntry>(); // member -> location, SameValueZero like native Set
	private dial = 0;
	public revolution = 0;

	constructor(
		private readonly laneCapacity = 8192,
		private readonly dialSize = 7,
	) {
		this.lanes = Array.from({ length: dialSize }, () => new Lane<true>(laneCapacity));
	}

	private get activeLane(): Lane<true> {
		return this.lanes[this.dial];
	}

	private advanceDialIfFull(): void {
		if (!this.activeLane.isFull) return;
		this.revolution++;
		this.dial = (this.dial + 1) % this.dialSize;
		this.lanes[this.dial].reset();
		for (const [member, entry] of this.index) if (entry.lane === this.dial) this.index.delete(member);
	}

	add(value: V): this {
		if (this.index.has(value)) return this; // Set semantics: no dupes, no wasted slot
		this.advanceDialIfFull();
		const seq = this.activeLane.write(value, true); // no encode() — member is never cloned
		this.index.set(value, { lane: this.dial, seq });
		return this;
	}

	has(value: V): boolean {
		const entry = this.index.get(value);
		return !!entry && this.lanes[entry.lane].readAt(entry.seq) !== undefined;
	}

	delete(value: V): boolean {
		return this.index.delete(value);
	}
	clear(): void {
		this.index.clear();
		this.lanes.forEach((l) => l.reset());
		this.dial = 0;
	}
	get size(): number {
		return this.index.size;
	}

	forEach(cb: (value: V) => void): void {
		for (const member of this.index.keys()) if (this.has(member)) cb(member);
	}

	[Symbol.iterator](): Iterator<V> {
		const live: V[] = [];
		for (const member of this.index.keys()) if (this.has(member)) live.push(member);
		return live[Symbol.iterator]();
	}

	get pressure(): number {
		return this.activeLane.pressure;
	}
}
class OTracer<V = any> {
	private lanes: Lane<V>[];
	private index = new Map<string, IndexEntry>();
	private dial = 0;
	public revolution = 0;
	private static readonly BLOCKED = new Set(["__proto__", "constructor", "prototype"]);

	constructor(
		private readonly laneCapacity = 256,
		private readonly dialSize = 7,
		private readonly skipEncode = false,
	) {
		this.lanes = Array.from({ length: dialSize }, () => new Lane<V>(laneCapacity));

		const handler: ProxyHandler<any> = {
			get: (_t, prop) => {
				if (typeof prop === "symbol") return (this as any)[prop];
				if (OTracer.BLOCKED.has(prop)) return undefined;
				return this.readKey(prop);
			},
			set: (_t, prop, value) => {
				if (typeof prop === "symbol") return false;
				if (OTracer.BLOCKED.has(prop)) {
					throw new TypeError(`[RAVEN] "${prop}" is not a writable key on this store.`);
				}
				this.writeKey(prop, value);
				return true;
			},
			has: (_t, prop) => {
				if (typeof prop === "symbol") return false;
				return this.readKey(prop as string) !== undefined;
			},
			deleteProperty: (_t, prop) => {
				if (typeof prop === "symbol") return false;
				return this.index.delete(prop as string);
			},
			ownKeys: () => [...this.index.keys()].filter((k) => this.readKey(k) !== undefined),
			getOwnPropertyDescriptor: (_t, prop) => {
				if (typeof prop === "symbol") return undefined;
				const value = this.readKey(prop as string);
				if (value === undefined) return undefined;
				return { value, writable: true, enumerable: true, configurable: true };
			},
		};

		// eslint-disable-next-line no-constructor-return
		return new Proxy(Object.create(null), handler);
	}

	private get activeLane(): Lane<V> {
		return this.lanes[this.dial];
	}

	private advanceDialIfFull(): void {
		if (!this.activeLane.isFull) return;
		this.revolution++;
		this.dial = (this.dial + 1) % this.dialSize;
		this.lanes[this.dial].reset();
		for (const [key, entry] of this.index) if (entry.lane === this.dial) this.index.delete(key);
	}

	private readKey(key: string): V | undefined {
		const entry = this.index.get(key);
		if (!entry) return undefined;
		const payload = this.lanes[entry.lane].readAt(entry.seq);
		if (payload === undefined) {
			this.index.delete(key);
			return undefined;
		}
		return this.skipEncode ? payload : decode(payload);
	}

	private writeKey(key: string, value: V): void {
		this.advanceDialIfFull();
		const payload = this.skipEncode ? value : encode(value);
		const seq = this.activeLane.write(key, payload);
		this.index.set(key, { lane: this.dial, seq });
	}
}
class RAVEN {
	public hub: DEATH_STAR;
	public t: A = {};

	constructor(hub: DEATH_STAR = null) {
		this.hub = hub;
		this.r = hub.r;
		this.m = hub.m;
		this.s = hub.s;
		this.k = hub.k;
	}

	public online = false;
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public u = undefined;
	public uuuu = [this.u, this.u, this.u, this.u];
	public UUUU = [this.s, this.m, this.r];
	public uuu = [this.u, this.u, this.u];
	public UUU = [this.s, this.m, this.r];
	public uu = [this.u, this.u];
	public UU = [this.m, this.r];

	public batchDepth = 0;
	public openScopes = new Set<BatchScope>();
	public dirty = new Set<FlushCallback>();
	public _flushScheduled = false;
	public _adapter: RuntimeAdapter | null = null;
	public _currentHookState: HookState | null = null;
	public _currentComponent: ComponentInstance | null = null;
	public PRIORITY_ORDER: Priority[] = ["immediate", "user-blocking", "normal", "background", "idle"];
	public GLOBAL_SCOPE = Symbol("global-scope");
	public scopeCursors: Record<Priority, number> = { immediate: 0, "user-blocking": 0, normal: 0, background: 0, idle: 0 };
	public currentScope: symbol | string = this.GLOBAL_SCOPE;
	public loopScheduled = false;
	public _subscribers = new Map<string, Set<() => void>>();
	public _externalSubscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
	public _components = new Map<string, ComponentInstance>();
	public _pendingNotifications = new Set<string>();
	public externalSubscribers = new Set<() => void>();
	public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;

	public onFlush(cb: FlushCallback): void {
		this.dirty.add(cb);
		this.scheduleDrain();
	}
	public scheduleDrain(): void {
		if (this._flushScheduled) return;
		this._flushScheduled = true;
		queueMicrotask(this.drain);
	}
	public drain(): void {
		this._flushScheduled = false;
		if (this.dirty.size === 0) return;

		// An explicit openBatch() scope is still open (deliberately spanning an await) —
		// don't drop the pending callbacks, just wait for closeBatch() to let them through.
		if (this.openScopes.size > 0) {
			this.scheduleDrain();
			return;
		}

		const callbacks = this.dirty;
		this.dirty = new Set();
		for (const cb of callbacks) cb();
	}
	public microtask(fn: FlushCallback): void {
		queueMicrotask(fn);
	}
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
	public openBatch(): BatchScope {
		const scope: BatchScope = Symbol("batch-scope");
		this.openScopes.add(scope);
		return scope;
	}
	public closeBatch(scope: BatchScope): void {
		this.openScopes.delete(scope);
		if (this.openScopes.size === 0 && this.dirty.size > 0) {
			this.scheduleDrain();
		}
	}
	public isBatching(): boolean {
		return this.batchDepth > 0 || this.openScopes.size > 0;
	}
	public flushSync(): void {
		if (this.openScopes.size > 0) return;
		this._flushScheduled = false;
		if (this.dirty.size === 0) return;
		const callbacks = this.dirty;
		this.dirty = new Set();
		for (const cb of callbacks) cb();
	}
	public flush<T>(fn?: () => T): T | void {
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
	public enqueue(task: ScheduledTask): void {
		if (task.priority === "immediate") {
			this.microtask(() => this.runOne(task));
			return;
		}
		const tier = queues[task.priority];
		if (!tier.has(task.scope)) tier.set(task.scope, []);
		tier.get(task.scope)!.push(task);
		this.scheduleLoop();
	}
	public scheduleLoop(): void {
		if (this.loopScheduled) return;
		this.loopScheduled = true;
		setImmediate(this.drainOne);
	}
	public pickNext(): ScheduledTask | undefined {
		for (const p of this.PRIORITY_ORDER) {
			if (p === "immediate") continue;
			const tier = queues[p];
			if (tier.size === 0) continue;
			const scopes = Array.from(tier.keys());
			const idx = this.scopeCursors[p] % scopes.length;
			const scope = scopes[idx];
			const list = tier.get(scope)!;
			const task = list.shift()!;
			if (list.length === 0) tier.delete(scope);
			this.scopeCursors[p] = idx + 1;
			return MaybePromise(task);
		}
		return undefined;
	}
	public runOne(task: ScheduledTask): void {
		if (task.cancelled || task.signal?.aborted) return;
		const prev = this.currentScope;
		this.currentScope = task.scope;
		try {
			task.fn();
		} finally {
			this.currentScope = prev;
		}
	}
	public drainOne(): void {
		this.loopScheduled = false;
		const task = this.pickNext();
		if (task) this.runOne(task);
		if (Object.values(queues).some((m) => m.size > 0)) this.scheduleLoop();
	}
	public schedule(fn: () => void, opts: { priority?: Priority; scope?: string; signal?: AbortSignal } = {}): Task {
		const task: ScheduledTask = { priority: opts.priority ?? "normal", scope: opts.scope ?? this.currentScope, fn, signal: opts.signal, cancelled: false };
		// signals.chains().sc.addEventListener('abort', () => { task.cancelled = true })
		this.enqueue(task);
		const self = this;
		return {
			cancel(): void {
				task.cancelled = true;
			},
			reprioritize(next: Priority): void {
				if (task.cancelled || next === task.priority) return;
				const old = queues[task.priority].get(task.scope);
				if (old) {
					const i = old.indexOf(task);
					if (i !== -1) old.splice(i, 1);
					if (old.length === 0) queues[task.priority].delete(task.scope);
				}
				task.priority = next;
				self.enqueue(task);
			},
		};
	}
	public withRequestScope<T>(requestId: string, fn: () => T): T {
		const prev = this.currentScope;
		this.currentScope = requestId;
		try {
			return fn();
		} finally {
			this.currentScope = prev;
		}
	}
	public yieldToScheduler(): Promise<void> {
		return new Promise((resolve) => {
			this.schedule(() => resolve(), { priority: "normal", scope: this.currentScope as string });
		});
	}
	public scheduleChunked(work: Iterable<() => void> | Iterator<() => void>, opts: { priority?: Priority; scope?: string; chunkBudgetMs?: number } = {}): Task {
		const iterator: Iterator<() => void> = Symbol.iterator in (work as A) ? (work as Iterable<() => void>)[Symbol.iterator]() : (work as Iterator<() => void>);
		const chunkBudgetMs = opts.chunkBudgetMs ?? 5;
		const scope = opts.scope ?? (this.currentScope as string);
		let cancelled = false;
		let priority = opts.priority ?? "normal";
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
			if (!cancelled) self.schedule(runChunk, { priority, scope });
		}

		this.schedule(runChunk, { priority, scope });
		return {
			cancel(): void {
				cancelled = true;
			},
			reprioritize(next: Priority): void {
				priority = next;
			},
		};
	}
	// SAURON
	_globalNotifyCache = new WeakMap<A, Map<A, () => void>>();
	_notifyCache = new WeakMap<Map<A, Set<(value: A) => void>>, Map<A, () => void>>();
	public scheduleGlobalNotify(key: string | symbol, meta: A, opts: ScopeOptions): void {
		let cache = this._globalNotifyCache.get(meta);
		if (!cache) {
			cache = new Map();
			this._globalNotifyCache.set(meta, cache);
		}
		let fn = cache.get(key);
		if (!fn) {
			fn = () => {
				cache!.delete(key);
				const finalVal = this.store.get(key, opts);
				const waiters = meta._waiters.get(key);
				if (waiters) {
					for (const resolveFn of waiters) {
						try {
							resolveFn(finalVal);
						} catch (err) {
							console.error(`[Sauron Waiter Error]:`, err);
						}
					}
					meta._waiters.delete(key);
				}
				for (const listener of meta._globalListeners) {
					try {
						listener(key, finalVal);
					} catch (err) {
						console.error(`[Sauron Global Listener Error]:`, err);
					}
				}
			};
			cache.set(key, fn);
		}
		this.onFlush(fn);
	}
	public scheduleNotify(key: A, _listeners: Map<A, Set<(value: A) => void>>, _state: A): void {
		let cache = this._notifyCache.get(_listeners);
		if (!cache) {
			cache = new Map();
			this._notifyCache.set(_listeners, cache);
		}
		let fn = cache.get(key);
		if (!fn) {
			fn = () => {
				cache!.delete(key);
				const val = _state.get(key);
				_listeners.get(key)?.forEach((l) => l(val));
			};
			cache.set(key, fn);
		}
		this.onFlush(fn);
	}
	public store = {
		get: <T = A,>(key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): T | undefined => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;

			if (_state.has(key)) return MaybePromise(_state.get(key));
			if (_persistKeys.has(key) && _persistent.has(key)) {
				const v = _persistent.get(key);
				_state.set(key, v);
				return MaybePromise(v);
			}
			return undefined;
		},
		set: <T = A,>(key: A, value: T | ((prev: T | undefined) => T), opts: { triggerRender?: boolean; cap?: number } & ScopeOptions = { s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200 }) => {
			const { triggerRender = true, s, m, r, cap } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			const _listeners = this.hub.wr._listeners;
			const prev = _state.get(key);
			const next = typeof value === "function" ? (value as (prev: T | undefined) => T)(prev) : value;
			if (Object.is(prev, next)) return;
			// TIME TRAVEL

			_state.set(key, next);
			if (_persistKeys.has(key)) _persistent.set(key, next);
			if (triggerRender) this.scheduleGlobalNotify(key, this.hub.wr, opts);
			if (triggerRender) this.scheduleNotify(key, _listeners, _state);
			return MaybePromise(next as T);
		},
		subscribe: <T = A,>(key: A, listener: (value: T) => void, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): (() => void) => {
			const { s, m, r } = opts;
			const _listeners = this.hub.wr._listeners;

			if (!_listeners.has(key)) _listeners.set(key, new Set());
			_listeners.get(key)!.add(listener);
			return () => {
				_listeners.get(key)?.delete(listener);
				if (_listeners.get(key)?.size === 0) _listeners.delete(key);
			};
		},
		delete: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): boolean => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			const _listeners = this.hub.wr._listeners;

			const existed = _state.delete(key);
			if (_persistKeys.has(key)) {
				_persistKeys.delete(key);
				_persistent.delete(key);
			}
			_listeners.get(key)?.forEach((l) => l(undefined));
			_listeners.delete(key);
			return MaybePromise(existed);
		},
		entries: <T = A,>(opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): [A, T][] => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			return MaybePromise(Array.from(_state.entries()));
		},
		persist: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _persistKeys = this.hub.wr._persistent_keys;
			_persistKeys.set(key, true);
		},
		unpersist: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			_persistKeys.delete(key);
			_persistent.delete(key);
		},
		has: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): boolean => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;

			return _state.has(key) || (_persistKeys.has(key) && _persistent.has(key));
		},
		clear: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _listeners = this.hub.wr._listeners;
			_state.clear();
			_listeners.clear();
		},
		clearPersistent: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			_persistent.clear();
			_persistKeys.clear();
		},
		_emit: (key: string | symbol, s = this.s, m = this.m, r = this.r): void => {
			const opts = { s, m, r };
			const _state = this.hub.wr._gt;
			const _listeners = this.hub.wr._listeners;
			const _waiters = this.hub.wr;
			this.scheduleNotify(key, _listeners, _state);
			this.scheduleGlobalNotify(key, _waiters, opts);
		},
		// subscribe, delete, entries, persist, unpersist, has, clear,
		// clearPersistent, _emit — all unchanged
	};
	public state: SauronState = {
		get: <T = A,>(key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): T | undefined => {
			return MaybePromise(this.store.get<T>(key, opts) as T);
		},
		set: <T = A,>(key: string | symbol, value: T | ((prev: T | undefined) => T), opts: { triggerRender?: boolean } & ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r, triggerRender = true } = opts;
			this.store.set(key, value, { triggerRender, s, m, r });
			return MaybePromise(value as T);
		},
		delete: (key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): boolean => {
			const { s, m, r } = opts;
			const meta = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
			const deleted = this.store.delete(key, opts);
			if (deleted) {
				for (const listener of meta._globalListeners) {
					try {
						listener(key, undefined);
					} catch (err) {
						console.error(`[Sauron Global Listener Error]:`, err);
					}
				}
			}
			return deleted;
		},
		getByPrefix: <T = A,>(prefix: string, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): { key: string; value: T }[] => {
			return this.store
				.entries<T>(opts)
				.filter(([key]) => typeof key === "string" && key.startsWith(prefix))
				.map(([key, value]) => ({ key: key as string, value }));
		},
		getAll: <T = A,>(opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): Record<string | symbol, T> => {
			const result: Record<string | symbol, T> = {};
			for (const [key, value] of this.store.entries<T>(opts)) {
				result[key] = value;
			}
			return result;
		},
		subscribe: <T = A,>(key: string | symbol, listener: (value: T) => void, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): (() => void) => {
			return this.store.subscribe<T>(key, listener, opts);
		},
		subscribeAll: (listener: (key: string | symbol, value: A) => void, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): (() => void) => {
			const { s, m, r } = opts;
			const meta = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
			meta._globalListeners.add(listener);
			return () => {
				meta._globalListeners.delete(listener);
			};
		},
		clear: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const meta = this.hub.wr;
			this.store.clear(opts);
			meta._waiters.clear();
			const gl = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
			gl.clear();
		},
		waitFor: <T = A,>(key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): Promise<T> => {
			const { s, m, r } = opts;
			const meta = this.hub.wr;

			// If it already exists, resolve immediately
			const existing = this.state.get<T>(key, opts);
			if (existing !== undefined) return Promise.resolve(existing);

			return new Promise<T>((resolve) => {
				if (!meta._waiters.has(key)) meta._waiters.set(key, new Set());
				meta._waiters.get(key)!.add(resolve as A);
			});
		},
	};
	public _devRecord: ((componentId: string, hookName: string, sauronId: string, key: string, value: A) => void) | null = null;
	public devRecord(componentId: string, hookName: string, sauronId: string, key: string, value: A) {
		return this._devRecord?.(componentId, hookName, sauronId, key, value);
	}
	public registerDevRecorder(fn: typeof this._devRecord) {
		this._devRecord = fn;
	}
	public rerender = (key?: string): void => {
		if (key !== undefined) {
			//   const hs = graphHookStates.get(key);
			// if (hs) notifyHook(`${GRAPH_BRIDGE_COMPONENT_ID}:${key}`);
			this.notifyHook(key);
		} else {
			this.flushSync();
		}
	};
	public flushHookKey = (key: string) => {
		const [componentId] = key.split(":");
		/**
		 * Store/global synthetic hook keys do not map to a component hook state.
		 * They simply notify subscribers.
		 */
		if (componentId === "store") {
			this._subscribers.get(key)?.forEach((callback: A) => {
				if (typeof callback !== "function") return;
				callback();
				this.onFlush(callback);
			});
			return;
		}

		const component = this._components.get(componentId) ?? _root;
		const hook = component?.hooks.get(key);

		/**
		 * Prefer direct DOM binding updates where bindings exist.
		 */
		if (hook?.bindings && hook.bindings.size > 0) {
			for (const binding of hook.bindings) {
				if (!binding.active) continue;

				binding.update(binding.node, hook.value);
			}

			return;
		}
		this._subscribers.get(key)?.forEach((callback: A) => {
			if (typeof callback !== "function") return;

			callback();
			this.onFlush(callback);
		});
	};
	public _flushHookNotifications = () => {
		this._flushScheduled = false;
		const keys = Array.from(this._pendingNotifications);
		this._pendingNotifications.clear();

		for (const key of keys) {
			this.flushHookKey(key);
		}
	};
	public getCurrentComponent = (): ComponentInstance => {
		if (isReact === true && IS_THE_WIZARD === false) {
			if (this._currentComponent) return this._currentComponent;
			return _root;
		} else {
			if (!init_hooks_context) {
				this.set("_root", _root, true, this.s, this.m, this.r);
				init_hooks_context = true;
			}
			const updated_ctx = this.get("_root", this.s, this.m, this.r) as ComponentInstance;
			if (updated_ctx && updated_ctx.id === this.r) return updated_ctx;
			else return _root;
		}
	};
	public getCurrentComponentCell = (): ComponentInstance => {
		if (isReact === true && IS_THE_WIZARD === false) {
			if (this._currentComponent) return this._currentComponent;
			return _root;
		} else {
			if (!init_hooks_context) {
				this.hub.bear.cell.set("_root", _root, true, this.s, this.m, this.r);
				init_hooks_context = true;
			}
			const updated_ctx = this.hub.bear.cell("_root", this.s, this.m, this.r) as ComponentInstance;
			if (updated_ctx && updated_ctx.id === this.r) return updated_ctx;
			else return _root;
		}
	};
	public unmountComponent(id: string) {
		const instance = this._components.get(id);

		if (!instance) return;

		for (const dispose of instance.unmount) {
			try {
				dispose();
			} catch (error) {
				console.error(`[THE_CLOUD][UNMOUNT] cleanup failed for "${id}":`, error);
			}
		}

		instance.unmount.clear();

		for (const hook of instance.hooks.values()) {
			try {
				hook.cleanup?.();
			} catch (error) {
				console.error(`[THE_CLOUD][UNMOUNT] hook cleanup failed for "${id}":`, error);
			}
		}

		instance.hooks.clear();
		this._components.delete(id);
	}
	public mountComponent(id: string, renderFn?: () => void): ComponentInstance {
		const instance: ComponentInstance = {
			id,
			hooks: new Map(),
			unmount: new Set(),
			render: renderFn,
		};
		this._components.set(id, instance);
		return instance;
	}
	public hmrPreserve(): Map<string, Map<string, A>> {
		const snapshot = new Map<string, Map<string, A>>();
		this._components.forEach((instance, id) => {
			const hooks = new Map<string, A>();
			instance.hooks.forEach((state, key) => {
				hooks.set(key, state.value);
			});
			snapshot.set(id, hooks);
		});
		return snapshot;
	}
	public hmrRestore(snapshot: Map<string, Map<string, A>>) {
		snapshot.forEach((hooks, id) => {
			const instance = this._components.get(id);
			if (!instance) return;
			hooks.forEach((value, key) => {
				const state = instance.hooks.get(key);
				if (state) state.value = value;
			});
		});
	}
	public subscribeToHook(key: string, cb: () => void): () => void {
		if (!this._subscribers.has(key)) this._subscribers.set(key, new Set());
		this._subscribers.get(key)!.add(cb);
		return () => {
			this._subscribers.get(key)?.delete(cb);
			if (this._subscribers.get(key)?.size === 0) this._subscribers.delete(key);
		};
	}
	public notifyHook(key: string) {
		this._pendingNotifications.add(key);
		if (!this._flushScheduled) {
			this._flushScheduled = true;
			queueMicrotask(this._flushHookNotifications);
		}
	}
	public endHookDependencyCapture() {
		this._currentHookState = null;
	}
	public beginHookDependencyCapture(hook: HookState) {
		this._currentHookState = hook;
	}
	public registerDOMBinding(node: Node, update: (node: Node, value: A) => void) {
		const hook = this._currentHookState;
		if (!hook) return;
		const binding = { node, update, active: true };
		if (!hook.bindings) {
			hook.bindings = new Set();
		}
		hook.bindings.add(binding);
		const anyNode = node as A;
		if (!anyNode.__sauron_bindings__) {
			anyNode.__sauron_bindings__ = new Set();
		}

		anyNode.__sauron_bindings__.add(hook);
		return () => {
			if (!binding.active) return;
			binding.active = false;
			hook.bindings?.delete(binding);
			anyNode.__sauron_bindings__?.delete(hook);
		};
	}
	public setCurrentComponent(instance: ComponentInstance | null) {
		this._currentComponent = instance;
	}
	public registerDOMRef(name: string, node: Node) {
		if (!this._currentHookState) return;
		if (!this._currentHookState.domRefs) this._currentHookState.domRefs = new Map();
		this._currentHookState.domRefs.set(name, node);
	}
	public getDOMRef(name: string): Node | undefined {
		if (!this._currentHookState || !this._currentHookState.domRefs) return undefined;
		return this._currentHookState.domRefs.get(name);
	}
	public registerAdapter(adapter: RuntimeAdapter) {
		this._adapter = adapter;
	}
	public _reactHookCursor = 0;
	public _currentRenderInstanceCheck: A = null;
	public _isReactRenderContext(): boolean {
		const _react = isReact as A;
		if (!_react) return false;
		if (IS_THE_WIZARD) return false;
		try {
			_react.darkState;
			// If darkState is callable without throwing, we're in a React render context
			// We probe by checking if the dispatcher is active via a known internal
			const internals = (_react as A).__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;
			const dispatcher = internals?.ReactCurrentDispatcher?.current ?? internals?.H;
			return !!dispatcher;
		} catch (e) {
			// log(`[THE_CLOUD][_IS_REACT_RENDER_CONTEXT]`, { error: e, }, t.error)
			return false;
		}
	}
	public createHook<T>(hookName: string, sauronId: string, initializer: () => HookState, updater: (state: HookState, key: string, notify: (key: string) => void) => T, triggerRender: boolean = true): T {
		if (isReact === true && IS_THE_WIZARD === false) {
			try {
				const isReact = this._isReactRenderContext();
				const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponent(), forceUpdate: undefined };

				const component = instance;

				// ─── THE TARGETED FIX ───────────────────────────────────────────────────────
				// Use sequential indexing when in React so it matches React's rendering slots.
				// Otherwise, use Sauron's default compilation ID string.
				const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
				const key = `${component.id}:${hookName}:${stableSlotId}`;
				// ────────────────────────────────────────────────────────────────────────────
				if (!this._components.has(component.id)) {
					this._components.set(component.id, component);
				}

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);
					if (component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				if (isReact && this._adapter && forceUpdate) { this._adapter.onHookSubscribe(key, forceUpdate); }

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
					}
					this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); 	} 
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				//  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		} else {
			try {
				const component = this.getCurrentComponent();
				const key = `${component.id}:${hookName}:${sauronId}`;

				if (!this._components.has(component.id)) { this._components.set(component.id, component); }

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);

					if (triggerRender && component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, sauronId, k, state?.value);
					}
					if (triggerRender) this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); }
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				// log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		}
	}
	public createHookCell<T>(hookName: string, sauronId: string, initializer: () => HookState, updater: (state: HookState, key: string, notify: (key: string) => void) => T, triggerRender: boolean = true): T {
		if (isReact === true && IS_THE_WIZARD === false) {
			try {
				const isReact = this._isReactRenderContext();
				const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponentCell(), forceUpdate: undefined };

				const component = instance;

				// ─── THE TARGETED FIX ───────────────────────────────────────────────────────
				// Use sequential indexing when in React so it matches React's rendering slots.
				// Otherwise, use Sauron's default compilation ID string.
				const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
				const key = `${component.id}:${hookName}:${stableSlotId}`;
				// ────────────────────────────────────────────────────────────────────────────
				if (!this._components.has(component.id)) {
					this._components.set(component.id, component);
				}

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);
					if (component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				if (isReact && this._adapter && forceUpdate) { this._adapter.onHookSubscribe(key, forceUpdate); }

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
					}
					this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); 	} 
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				//  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		} else {
			try {
				const component = this.getCurrentComponentCell();
				const key = `${component.id}:${hookName}:${sauronId}`;

				if (!this._components.has(component.id)) { this._components.set(component.id, component); }

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);

					if (triggerRender && component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, sauronId, k, state?.value);
					}
					if (triggerRender) this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); }
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				// log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		}
	}
	public _broadcastSS(key: A, value: A, storageType: S, fn: S, path?: S, domain?: S, options?: A) {
		_localChange = true;
		_channel?.postMessage({ key, value, storageType, fn, path, domain, options });
	}
	public getDark<T = A>(key: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.get<T>(key, { s, m, r }));
	}
	public setDark<T = A>(label: S, value: T, triggerRender = true, s?: S, m?: S, r?: S) {
		this._broadcastSS(label, value, "sauron", "set");
		return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
	}
	public getAllDark<T = A>(opts?: ScopeOptions): Record<S | symbol, T> {
		return this.state.getAll<T>(opts);
	}
	public subAllDark(fn: (key: S | symbol, value: A) => V, s?: S, m?: S, r?: S): () => V {
		if (!fn) return () => {};
		return this.state.subscribeAll(fn, { s, m, r });
	}
	public subDark<T = A>(k: A, fn: (value: T) => void, s?: S, m?: S, r?: S): () => void {
		if (!k || !fn) return () => {};
		return MaybePromise(this.state.subscribe<T>(k, fn, { s, m, r }));
	}
	public deleteDark(label: A, s?: S, m?: S, r?: S): B {
		if (!label) return false;
		const v = this.state.delete(label, { s, m, r });
		this._broadcastSS(label, undefined, "sauron", "delete");
		return v;
	}
	public waitForDark<T = A>(label: S, s?: S, m?: S, r?: S): P<T> {
		return this.state.waitFor<T>(label, { s, m, r });
	}
	public async deleteManyDark(keys: A[], s?: S, m?: S, r?: S): P<B> {
		if (!Array.isArray(keys) || keys.length === 0) {
			return false;
		}
		let allDeleted = true;

		// Batch all deletes without triggering renders until the end
		for (let i = 0; i < keys.length; i++) {
			const deleted = this.delSauron(keys[i], s, m, r);
			if (!deleted) {
				allDeleted = false;
			}
		}

		return allDeleted;
	}
	public async setManyDark<T = Record<S, A>>(entries: T): Promise<void> {
		if (!entries || typeof entries !== "object") {
			return;
		}
		// Batch all sets without triggering renders until the end
		const entriesArray = Object.entries(entries);
		for (let i = 0; i < entriesArray.length; i++) {
			const [key, value] = entriesArray[i];
			// Only trigger render on the last item to avoid multiple renders
			const triggerRender = i === entriesArray.length - 1;
			this.set(key, value, triggerRender);
		}
	}
	public async getManyDark<T = A>(keys: S[], s?: S, m?: S, r?: S) {
		if (!Array.isArray(keys) || keys.length === 0) {
			return {};
		}

		const result: Record<S, T | undefined> = {};

		const promises = keys.map(async (key) => {
			const value = await this.get<T>(key, s, m, r);
			return { key, value };
		});

		const results = await Promise.all(promises);

		for (const { key, value } of results) {
			result[key] = value;
		}

		return result;
	}

	public getAll<T = A>(opts?: ScopeOptions): Record<S | SY, T> {
		return this.state.getAll<T>(opts);
	}
	public get<T = A>(key: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.get<T>(key, { s, m, r }));
	}
	public getReChain<T = A>(key: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.get<T>(key, { s, m, r }));
	}
	public setChain<T = A>(label: A, value: T, triggerRender = true, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
	}
	public set<T = A>(label: A, value: T, triggerRender = true, s?: S, m?: S, r?: S) {
		this._broadcastSS(label, value, "sauron", "set");
		return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
	}
	public subChain<T = A>(label: S, fn: (value: T) => V, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.subscribe<T>(label, fn, { s, m, r }));
	}
	public sub<T = A>(label: A, fn: (value: T) => V, s?: S, m?: S, r?: S): () => V {
		if (!label || !fn) return () => {};
		return MaybePromise(this.state.subscribe<T>(label, fn, { s, m, r }));
	}
	public subAll(fn: (key: A, value: A) => void, s?: S, m?: S, r?: S): () => V {
		if (!fn) return () => {};
		return this.state.subscribeAll(fn, { s, m, r });
	}
	public delSauron(label: S, s?: S, m?: S, r?: S): B {
		if (!label) return false;
		const v = this.state.delete(label, { s, m, r });
		this._broadcastSS(label, undefined, "sauron", "delete");
		return v;
	}
	public deleteChain(label: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.delete(label, { s, m, r }));
	}
	public waitFor<T = A>(label: S, s?: S, m?: S, r?: S): P<T> {
		return this.state.waitFor<T>(label, { s, m, r });
	}

	public __subscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
}
// #endregion
//                                                                                                                         [ss] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const _SS_MEMORY = new Map<string, A>();
export let memoryTimer: ReturnType<typeof setTimeout> | undefined;
export let ttl_active = false;
export let ttl_minutes = 30;
export function touchMemory() {
	if (memoryTimer) clearTimeout(memoryTimer);
	memoryTimer = setTimeout(
		() => {
			_SS_MEMORY.clear();
			memoryTimer = undefined;
		},
		ttl_minutes * 60 * 1000,
	);
}
export async function _withLock<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
	if (typeof navigator !== "undefined" && navigator.locks) {
		return navigator.locks.request("gt_write_" + key, fn) as Promise<T>;
	}
	return fn();
}
export function generateId(): string {
	if (typeof crypto !== "undefined" && crypto.getRandomValues) {
		const arr = new Uint32Array(1);
		crypto.getRandomValues(arr);
		return arr[0].toString();
	}
	try {
		const { randomBytes } = require("crypto");
		return randomBytes(4).readUInt32BE(0).toString();
	} catch {
		return Math.floor(Math.random() * 0xffffffff).toString();
	}
}
export async function setSS(key: string, value: A) {
	if (typeof window === "undefined") return;
	const serialized = ser(value);
	await _withLock(key, () => {
		sessionStorage.setItem(key, serialized);
		_SS_MEMORY.set(key, serialized);
	});
	return value;
}
function ser(value: U): S {
	return typeof value === "string" ? value : JSON.stringify(value);
}
function deser(raw: S | NU): A {
	if (raw === null) return null;
	try {
		return JSON.parse(raw);
	} catch {
		return raw;
	}
}
export function getSS(key: S) {
	if (typeof window === "undefined") return;
	const raw = typeof sessionStorage !== "undefined" ? sessionStorage.getItem(key) : null;
	const v = deser(raw);
	return MaybePromise(v);
}
export async function get_ws_id(): Promise<string> {
	let Bun: A = null;
	let Deno: A = null;
	if (typeof (globalThis as A).Deno !== "undefined") {
		Deno = (globalThis as A).Deno;
	}
	if (typeof (globalThis as A).Bun !== "undefined") {
		Bun = (globalThis as A).Bun;
	}
	const hasBun = typeof (globalThis as A).Bun !== "undefined";
	const hasDeno = typeof (globalThis as A).Deno !== "undefined";
	const hasNode = typeof process !== "undefined" && !!process.versions?.node;
	const isBrowser = typeof window !== "undefined";
	const isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
	const isElectron = false;
	const isUnix = false;
	const isWindows = true;
	let WS_KEY = twx.WS_KEY;
	let WS_ID = twx.WS_ID;
	if (isBrowser) {
		let id = getSS(WS_KEY);
		if (!id) {
			id = generateId();
			setSS(WS_KEY, id);
		}
		WS_ID = id;
		return id;
	}
	if (isVSCode) {
		const vscode = require("vscode");
		const name = vscode.workspace.workspaceFolders?.[0]?.name;
		if (name) return name;
		const id = generateId();
		WS_ID = id;
		return id;
	}
	let cwd: string;
	if (hasBun || hasNode) {
		cwd = process.cwd();
	} else if (hasDeno) {
		cwd = Deno.cwd();
	} else {
		throw new Error("Unsupported environment");
	}

	let filePath: string;

	if (hasDeno) {
		const currentDir = new URL(".", import.meta.url).pathname;
		const projectRoot = new URL("../../../../../", import.meta.url).pathname;
		filePath = new URL("the_wizard/wizards_vault/ws_id.txt", projectRoot).pathname;
	} else {
		const path = require("path");
		const currentDir = typeof import.meta?.dirname !== "undefined" ? import.meta.dirname : __dirname;
		const projectRoot = path.resolve(currentDir, "../../../../../");
		filePath = path.join(projectRoot, "the_wizard", "wizards_vault", "ws_id.txt");
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
				const fs = require("fs");
				if (!fs.existsSync(filePath)) return null;
				return fs.readFileSync(filePath, "utf8");
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
			const fs = require("fs");
			fs.writeFileSync(filePath, data, "utf8");
		}
	};

	let id = await readFile();
	if (id && !isNaN(Number(id))) {
		WS_ID = id;
		return id;
	}

	id = generateId();
	await writeFile(id);
	WS_ID = id;
	return id;
}
async function deleteSS(key: string) {
	if (typeof window === "undefined") return;
	if (ttl_active) touchMemory();
	await _withLock(key, () => {
		if (typeof sessionStorage !== "undefined") sessionStorage.removeItem(key);
		_SS_MEMORY.delete(key);
	});
}
// #endregion
//                                                                                                                   [WAR ROOM] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const rootRegistry = new M2<A, M2<A, M2<A, A>>>();

export class WAR_ROOOM {
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	private raven: DEATH_STAR;
	//                                                                                                                  MAIN CACHES
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	#META = new M2<A, M2<A, M2<A, A>>>();
	_meta: MTracer<A, A>;
	#SIGNALS = new M2<A, M2<A, M2<A, A>>>();
	_signals: M2<A, A>;
	#NAKED_SIGNALS = new M2<A, M2<A, M2<A, A>>>();
	_naked_signals: M2<A, A>;
	#CONTEXT = new M2<A, M2<A, M2<A, A>>>();
	_context: MTracer<A, A>;
	#ATOMS = new M2<A, M2<A, M2<A, A>>>();
	_atoms: MTracer<A, A>;
	#SAURON = new M2<A, M2<A, M2<A, A>>>();
	_sauron: MTracer<A, A>;
	#BUS = new M2<A, M2<A, M2<A, A>>>();
	_bus: MTracer<A, A>;
	_gt: MTracer<A, A>;

	COMPONENTS = new M2<S, ComponentInstance>();
	// #endregion
	//                                                                                                                    THE CLOUD
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	_components: MTracer<A, A>;
	_batches: MTracer<S | SY>;
	_batchQueue: MTracer<S | SY, A>;
	_batchOpts: M<S | SY, { triggerRender?: B }>;
	_sys: MTracer<S, SysEntry>;
	_sealed: MTracer<S, SysEntry>;
	_keyContracts: M<S, ST<S>>;
	_conflicts: MTracer<S, ConflictPolicy>;
	_conflictStrategies: MTracer<S, "latest" | "fail" | "accumulate">;
	_causal: M<S, CausalEntry[]>;
	_history: M<S | SY, HistoryEntry[]>;
	_scopes: MTracer<S, SignalScope>;
	_msgHandlers: M<S, ST<(p: A) => V>>;
	_lastWriteSource: M<S, S>;
	_mainMap: MTracer<A, S>;
	_tupleCache: MTracer<S, S>;
	_weakMap: MTracer<O, S>;
	_boundaries: MTracer<O, S>;
	_persistent: MTracer<A, A>;
	_persistent_keys: MTracer<A, A>;

	_global_subscribers: M2<S, ST<() => V>>;
	_waiters: M2<S | SY, ST<(value: A) => V>>;
	_listeners: M2<A, ST<(value: A) => void>>;
	// #endregion
	//                                                                                                                    SIGNALS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	_timeTravelMaul: MTracer<A, A>;
	_historyMaul: MTracer<A, A>;

	_subscribersMaul: M2<A, A>;
	_listenersMaul: M2<A, A>;
	// #endregion
	constructor(raven: DEATH_STAR) {
		this.raven = raven;
		this.r = raven.r ?? "THE_WIZARD_ROOT_REG";
		this.m = raven.m ?? "SAURON_MASTER_REG";

		this.s = raven.s ?? "SAURON_SLAVE_REG";
		this.k = raven.k ?? "THE_K_KEY";

		this._meta = this.tracer("_meta", this.m, this.r, "#META");
		this._signals = this.map("_signals", this.m, this.r, "#SIGNALS");
		this._naked_signals = this.map("_naked_signals", this.m, this.r, "#NAKED_SIGNALS");
		this._context = this.tracer("_context", this.m, this.r, "#CONTEXT");
		this._atoms = this.tracer("_atoms", this.m, this.r, "#ATOMS");
		this._sauron = this.tracer("_sauron", this.m, this.r, "#SAURON");
		this._bus = this.tracer("_bus", this.m, this.r, "#BUS");
		this._gt = this.tracer("_gt", this.m, this.r);

		this._listeners = this.map("_listeners_raven", this.m, this.r);
		this._global_subscribers = this.map<M<S, ST<() => V>>>("_global_subscribers_raven", this.m, this.r);
		this._waiters = this.map<M<S | SY, ST<(value: A) => V>>>("_waiters_raven", this.m, this.r);

		this._persistent_keys = this.tracer("_persistent_keys_raven", this.m, this.r);
		this._persistent = this.tracer("_persistent_raven", this.m, this.r);
		this._components = this.tracer("_components_raven", this.m, this.r);
		this._batches = this.tracer("_batches_raven", this.m, this.r);
		this._batchQueue = this.tracer("_batchQueue_raven", this.m, this.r);
		this._batchOpts = this.map<M<S | SY, { triggerRender?: B }>>("_batchOpts_raven", this.m, this.r);
		this._sys = this.tracer<MTracer<S, SysEntry>>("_sys_raven", this.m, this.r);
		this._sealed = this.tracer<MTracer<S, SysEntry>>("_sealed_raven", this.m, this.r);
		this._keyContracts = this.map<M<S, ST<S>>>("_keyContracts_raven", this.m, this.r);
		this._conflicts = this.tracer<MTracer<S, ConflictPolicy>>("_conflicts_raven", this.m, this.r);
		this._conflictStrategies = this.tracer<MTracer<S, "latest" | "fail" | "accumulate">>("_conflictStrategies_raven", this.m, this.r);
		this._causal = this.map<M<S, CausalEntry[]>>("_causal_raven", this.m, this.r);
		this._history = this.map<M<S | SY, HistoryEntry[]>>("_history_raven", this.m, this.r);
		this._scopes = this.tracer<MTracer<S, SignalScope>>("_scopes_raven", this.m, this.r);
		this._msgHandlers = this.map<M<S, ST<(p: A) => V>>>("_msgHandlers_raven", this.m, this.r);
		this._lastWriteSource = this.map<M<S, S>>("_lastWriteSource_raven", this.m, this.r);
		this._mainMap = this.tracer("_mainMap_raven", this.m, this.r);
		this._tupleCache = this.tracer<MTracer<S, S>>("_tupleCache_raven", this.m, this.r);
		this._weakMap = this.tracer<MTracer<O, S>>("_weakMap_raven", this.m, this.r);
		this._boundaries = this.tracer<MTracer<O, S>>("_boundaries_raven", this.m, this.r);

		this._timeTravelMaul = this.tracer<MTracer<O, S>>("_timeTravel_maul", this.m, this.r, "#SIGNALS");
		this._historyMaul = this.tracer<MTracer<O, S>>("_history_maul", this.m, this.r, "#SIGNALS");

		this._subscribersMaul = this.map<M2<O, S>>(" _subscribers_maul", this.m, this.r, "#SIGNALS");
		this._listenersMaul = this.map<M2<O, S>>(" _listeners_maul", this.m, this.r, "#SIGNALS");
	}
	get_root(c: S, r: S) {
		let root;
		switch (c) {
			case "#CONTEXT": { root = this.#CONTEXT.get(r); break; }
			case "#META": { root = this.#META.get(r); break; }
			case "#SIGNALS": { root = this.#SIGNALS.get(r); break; }
			case "#ATOMS": { root = this.#ATOMS.get(r); break; }
			case "#SAURON": { root = this.#SAURON.get(r); break; }
			case "#BUS": { root = this.#BUS.get(r); break; }
			case "#NAKED_SIGNALS": { root = this.#NAKED_SIGNALS.get(r); break; }
			case "gt": { root = rootRegistry.get(r); break; }
			case "globalThis": { (globalThis as A)[r]; break; }
		}
		if (!root) {
			root = new M2<A, M2<A, M2<A, A>>>();
			switch (c) {
				case "#CONTEXT": { root = this.#CONTEXT.set(r, root); break; }
				case "#META": { root = this.#META.set(r, root); break; }
				case "#SIGNALS": { root = this.#SIGNALS.set(r, root); break; }
				case "#ATOMS": { root = this.#ATOMS.set(r, root); break; }
				case "#SAURON": { root = this.#SAURON.set(r, root); break; }
				case "#NAKED_SIGNALS": { root = this.#NAKED_SIGNALS.set(r, root); break; }
				case "#BUS": { root = this.#BUS.set(r, root); break; }
				case "gt": { rootRegistry.set(r, root); break; }
				case "globalThis": { (globalThis as A)[r] = root; break; }
			}
		}
		return root;
	}
	map<K = A, V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): M2<K, V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new M2<K, V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	tracer<K = A, V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): MTracer<K, V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new MTracer<K, V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	set<V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): STracer<V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new STracer<V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	obj<V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): OTracer<V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new OTracer<V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	
}
// (globalThis as A)[r] //  STATE_CONTEXT === "sauron" ? : null;
	// (globalThis as A)[r] = root;
// #endregion
//                                                                                                             [DEATH STAR] HUB
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
/*  ===========================================================================================================================
	 Welcome to the dark side...
     The counterpart to the traditional portion of the platform, where the light side gives you features... this does not.
     But... There is something that the dark side has, that the light does not... True, raw... power. Literally as much as we
     can acquire.
     The signals solution bench comes in at being 40-1100x more performant than both preact AND alien...
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
     - signals: 40-1100x more performant than both preact AND alien...
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

     This portion is still optimized, just not at disruptor speed — it operates at 105M ops/sec, which is the same rate my M2 hits. 
     The rest of the mesh paths run at 400-600M ops/sec, above that line.
     I am looking into ways of drastically improving that number, but when you operate at these numbers and have nothing 
     to go off of, its just a fucking roll of the dice. One little implementation detail could mean the difference between 4 mill 
     and 900 mill. The best disruptor I have built to date, started life as a 400k ops per sec disruptor and currently sits at
     1.2 billion(2.1 bill on a friends newer machine), same archtecture as when it was first built.
     OTHER THAN THAT ONE SMALL PORTION OF CODE, everything else was entirely redone... even the reactive state portion ie the 
     cloud... kinda, curious to see if I was able to code it and have it working first shot... I doubt it.
    ============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE DARK SIDE] */
class DRS {
    public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public hub: DEATH_STAR;
	constructor(hub: DEATH_STAR) {
		this.hub = hub;
	}
    public state_setter(v: U, k: A = `_USECELL`, s: S = this.s, m: SU = this.m, r: SU = this.r) {
		const existing = this.hub.bear.state.get(k, { s: s, m: m, r: r });
		if (existing !== undefined) return existing;
		const t = UF(v)
		this.hub.bear.state.set(k, t, { triggerRender: false, s: s, m: m, r: r });
		return t;
	}
	public useCell = (() => {
		const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = darkState(() => { return this.state_setter(v, k, s, m, r); });

			const senderTokenRef = this.hub.bear.atom(this.hub.REGISTRY._sid());

			this.hub.bear.sub( (i: A) => {
					const incoming = i.p;
					if (incoming?.from === senderTokenRef.value) return;
					this.hub.bear.state.set(k, incoming.value, { triggerRender: false, s: s, m: m, r: r });
					// set(k, incoming.value, false, s, m, r);
					setState(incoming.value);
			 }, k, s, m, 	r, ); // stay in sync with local writes too (including our own setLive below)
			this.hub.bear.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r });
			// sub(k, (newValue: any) => setState(newValue), s, m, r);

			const setLive = (next: any) => {
				const newValue = typeof next === "function" ? next(this.hub.bear.state.get(k, { s: s, m: m, r: r })) : next;
				setState(newValue);
				this.hub.bear.state.set(k, newValue, { triggerRender: true, s: s, m: m, r: r });
				// set(k, newValue, true, s, m, r); // local write
				this.hub.bear.send({ value: newValue, from: senderTokenRef.value }, k, s, m, r); // broadcast to the bus
			};

			return [state, setLive] as const;
		};
		fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = darkState(() => { return this.state_setter(v, k, s, m, r); });

			const senderTokenRef = this.hub.bear.atom(this.hub.REGISTRY._sid());
			const queueRef = this.hub.bear.atom<Queue | null>(null);

			if (!queueRef.current) {
				queueRef.current = new Queue(1);
			}

			// mailbox — replace the whole useEffect block:
			this.hub.bear.sub( (i: A) => {
			    const incoming = i.p;
			    if (incoming?.from === senderTokenRef.value) return;
			    queueRef.current!.run(async () => {
						const current = this.hub.bear.state.get(k, { s: s, m: m, r: r });
						const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
						this.hub.bear.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
						return next;
			    });
			}, k, s, m, r, );

			this.hub.bear.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, m: m, r: r });

			const setLive = (next: any) => {
				const t = typeof next === "function" ? next() : next;
				this.hub.bear.send({ value: t, from: senderTokenRef.value }, k, s, m, r);
			};

			return [state, setLive] as const;
		};
		return fn;
	})();
}
class SPREADSHEET {
    public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public hub: DEATH_STAR;
	public cell() {}
	public useCell() {}
	constructor(hub: DEATH_STAR) {
		this.hub = hub;
	}
}
export class DEATH_STAR {
	public bus: DARTH_VADER; // bus
	public signals: DARTH_MAUL; // signals
	public state: RAVEN; // app context / state
	public slab: EXOGUL; // slab
	public drs: DRS; // slab
	public spreadsheet: SPREADSHEET; // slab
	public governor: GOVERNOR;
	public REGISTRY: Registry;
	public wr: WAR_ROOOM;
	public bear: BEAR
	//public MESH: Mesh;

	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "SAURON_SLAVE_REG";
	public k: A = "__SIGNALS__";

	constructor(capacity = 1 << 20, REGISTRY: A = null, MESH: A = null) {
		this.REGISTRY = REGISTRY;
		if (!REGISTRY) this.REGISTRY = new Registry();
		this.wr = new WAR_ROOOM(this);
		// this.MESH = MESH
		//if (!MESH) this.MESH = new Mesh()
		this.bus = new DARTH_VADER(this, capacity);
		this.state = new RAVEN(this);
		this.signals = new DARTH_MAUL(this, capacity);
		this.slab = new EXOGUL(this);
		this.governor = new GOVERNOR(this);
		this.drs = new DRS(this);
		this.spreadsheet = new SPREADSHEET(this);
		this.bear = new BEAR(this)
	}
}
export const death_star = new DEATH_STAR();
// #endregion
//                                                                                                                    USE STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkState<T = any, S = T>(initialValue?: T | (() => T), options: StateOptions<T, S> = {}, __sauron_id__ = _sid()) {
	return death_star.state.createHook( "darkState", __sauron_id__, () => ({
			value: {
				current: typeof initialValue === "function" ? (initialValue as () => T)() : initialValue,
				derived: undefined as A,
			},
		}),
		(state, key, notify) => {
			const s = state.value;

			const equals: (a: S, b: S) => boolean = options.equals ?? ((a, b) => a === b);
			const selector = options.selector ?? null;
			const derive = options.derive ?? null;

			const getValue = (): S => (selector ? selector(s.current) : (s.current as unknown as S));

			const setValue = (v: T | ((prev: T) => T)) => {
				const next = typeof v === "function" ? (v as (prev: T) => T)(s.current) : v;
				const prevSelected = getValue();
				const nextSelected = selector ? selector(next) : (next as unknown as S);
				if (!equals(prevSelected, nextSelected)) {
					s.current = next;
					if (derive) {s.derived = derive(next);}
					notify(key);
				}
				return next;
			};

			if (derive && s.derived === undefined) { s.derived = derive(s.current); }
			if (derive) { return [getValue(), setValue, s.derived] as const; }

			return [getValue(), setValue] as const;
		},
	);
}
// #endregion
//                                                                                                                USE CALL BACK
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkCallback<T extends (...args: A[]) => A>(callback: T, deps?: A[], __sauron_id__ = _sid()): T {
	return death_star.state.createHook(
		"darkCallback",
		__sauron_id__,
		(): CallbackHookState => ({ value: undefined, callback, deps: deps ?? [] }),
		(s: HookState) => {
			const state = s as CallbackHookState;
			const hasChanged = !deps || deps.length !== state.deps.length || deps.some((d, i) => d !== state.deps[i]);

			if (hasChanged) {
				state.callback = callback;
				state.deps = deps ?? [];
			}

			return state.callback as T;
		},
	);
}
// #endregion
//                                                                                                                   USE EFFECT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkEffect(effect: (signal?: AbortSignal) => void | (() => void), deps?: A[], __sauron_id__ = _sid()): void {
	death_star.state.createHook(
		"darkEffect",
		__sauron_id__,
		(): HookState => {
			const controller = new AbortController();
			const cleanup = effect(controller.signal);
			return {
				value: undefined,
				deps: deps ?? [],
				cleanup: () => {
					controller.abort();
					if (typeof cleanup === "function") cleanup();
				},
			};
		},
		(state) => {
			if (deps === undefined || !_depsEqual(state.deps ?? [], deps)) {
				state.cleanup?.();
				const controller = new AbortController();
				const cleanup = effect(controller.signal);
				state.deps = deps ?? [];
				state.cleanup = () => {
					controller.abort();
					if (typeof cleanup === "function") cleanup();
				};
			}
			return undefined;
		},
	);
}
function _depsEqual(a: A[], b: A[]): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (!Object.is(a[i], b[i])) return false;
	}
	return true;
}
// #endregion
//                                                                                                                 USE INTERVAL
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkInterval(options: { timer?: number; autoStart?: boolean; execute: () => void }, __sauron_id__ = _sid()) {
	const autoStart = options.autoStart ? Boolean(options.autoStart) !== false : true;
	const callback = options.execute ? options.execute : console.log(`interval triggered`);
	const delay = options.timer ? options.timer : 30000;
	return death_star.state.createHook(
		"darkInterval",
		__sauron_id__,
		(): HookState => ({
			value: {
				callback,
				delay,
				running: autoStart,
				handle: null as A,
				attached: false,
				cleanup: null as null | (() => void),
			},
		}),

		(hook, key) => {
			const s = hook.value;

			const start = () => {
				if (s.running) return;
				s.running = true;
				death_star.state.notifyHook(key);

				if (typeof window !== "undefined" && s.delay !== null) {
					s.handle = setInterval(() => {
						s.callback();
					}, s.delay);
				}
			};

			const stop = () => {
				if (!s.running) return;
				s.running = false;

				if (s.handle) {
					clearInterval(s.handle);
					s.handle = null;
				}

				death_star.state.notifyHook(key);
			};

			if (!s.attached && typeof window !== "undefined") {
				s.attached = true;

				if (s.running && s.delay !== null) {
					s.handle = setInterval(() => {
						s.callback();
					}, s.delay);
				}

				s.cleanup = () => {
					if (s.handle) clearInterval(s.handle);
				};
			}

			const isRunning = () => s.running;

			return [isRunning(), start, stop];
		},
	);
}
// #endregion
//                                                                                                           USE DEFERRED VALUE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkDeferredValue<T>(value: T, __sauron_id__ = _sid()): T {
	return death_star.state.createHook(
		"darkDeferredValue",
		__sauron_id__,
		(): HookState => ({ value }),
		(state, key, notify) => {
			if (!Object.is(state.value, value)) {
				queueMicrotask(() => {
					state.value = value;
					notify(key);
				});
			}
			return state.value as T;
		},
	);
}
// #endregion
//                                                                                                        USE IMPERATIVE HANDLE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkImperativeHandle<T>(ref: { current: T | null }, init: () => T, deps?: A[], __sauron_id__ = _sid()): void {
	return death_star.state.createHook(
		"darkImperativeHandle",
		__sauron_id__,
		(): HookState => {
			if (ref) ref.current = init();
			return { value: undefined, deps: deps ?? [] };
		},
		(state) => {
			if (deps === undefined || !_depsEqual(state.deps ?? [], deps)) {
				if (ref) ref.current = init();
				state.deps = deps ?? [];
			}
			return undefined;
		},
	);
}
// #endregion
//                                                                                                                     USE MEMO
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkMemo<T>(factory: () => T, deps: A[], __sauron_id__ = _sid()): T {
	return death_star.state.createHook(
		"darkMemo",
		__sauron_id__,
		(): HookState => ({ value: factory(), deps }),
		(state) => {
			if (!_depsEqual(state.deps ?? [], deps)) {
				state.value = factory();
				state.deps = deps;
			}
			return state.value as T;
		},
	);
}

// #endregion
// #endregion
//                                                                                                                 ATOM HELPERS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function patch<T extends object>(self: { value: T }, partial: Partial<T>): T {
	self.value = { ...self.value, ...partial };
	return self.value;
}
export function update<T>(self: { value: T }, fn: (current: T) => T): T {
	self.value = fn(self.value);
	return self.value;
}
export function createHistory<T>(limit = 50) {
	const past: T[] = [];
	const future: T[] = [];
	return {
		push(value: T) { past.push(value); if (past.length > limit) past.shift(); future.length = 0; },
		undo(current: T): T | undefined { if (!past.length) return undefined; future.push(current); return past.pop(); },
		redo(current: T): T | undefined { if (!future.length) return undefined; past.push(current); return future.pop(); },
		get canUndo() { return past.length > 0; },
		get canRedo() { return future.length > 0; },
	};
}
const def = new (class DEFINITION {
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
})();
function createLiveSelf(instanceId: S) {
	const liveTarget: R<A> = {};
	const selfProxy = new PX(liveTarget, {
		get(target, key: S) {
			return target[key];
		},
		set(target, key: S, value: A) {
			// reuse the existing Sauron store — same primitive `live`/`useLive` already build on
			target[key] = value;
			death_star.bear.cell.set(`${instanceId}:${key}`, value);
			//  setPrev(sV, `${instanceId}:${key}`, value)
			return true;
		},
	});
	return { selfProxy, liveTarget };
}
function mergeInto<T extends object>(target: T, source: object): T {
	// defineProperties, not assign — this is what preserves get/set accessors
	// as live accessors instead of flattening them into snapshots (the bug from last time)
	Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
	return target;
} 
function resolveFactory(raw: AnyFactory): AnyFactory {
	if (typeof raw === "function") return raw;
	return () => raw;
}
type StubHit = { category: S; key: S; prop: S };
const STUB_HITS: StubHit[] = [];
const NINJA_STUB_BRAND = "__is_atom_stub__";
function isStub(value: A): B {
	try {
		return !!value && (typeof value === "object" || typeof value === "function") && value[NINJA_STUB_BRAND] === true;
	} catch {
		return false;
	}
}
function makeStub(trace?: { category: S; key: S }): A {
		return new Proxy(() => makeStub(trace), {
		get: (_t, prop: S | SY) => {
			if (prop === NINJA_STUB_BRAND) return true;
			if (prop === Symbol.toPrimitive || prop === "toString" || prop === "valueOf") return () => "";
			if (trace && typeof prop === "string") {
				STUB_HITS.push({ ...trace, prop });
			}
			return makeStub(trace);
		},
		apply: () => {
			if (trace) STUB_HITS.push({ ...trace, prop: "()" });
			return makeStub(trace);
		},
	});
}
function NexusMirror(excludeName: S) {
	return new Proxy(
		{},
		{
			get: (_t: A, name: S) => {
				if (name === excludeName) return makeStub({ category: excludeName, key: name });
				return new Proxy([], {
					get: (_s: A, prop: S | SY) => {
						let arr = death_star.bear.cell(name);
						if (!arr) return makeStub({ category: name, key: String(prop) });
						if (prop in arr) return (arr as any)[prop];
						return makeStub({ category: name, key: String(prop) });
					},
				});
			},
		},
	);
}
function buildCtx(refs: A, selfProxy: A): SelfType {
	let builtins = {
		self: selfProxy,
	};
	return new Proxy(builtins, {
		get: (target, key: S) => (key in target ? (target as A)[key] : (refs as A)[key]),
	});
}
function withTypoGuard<T extends Record<S, A>>(result: T, label: S, allowedExtraKeys: S[] = ["freyr"]): T {
	return new Proxy(result, {
		get(target, prop, receiver) {
			if (typeof prop === "symbol" || prop in target || allowedExtraKeys.includes(prop as string)) {
				return Reflect.get(target, prop, receiver);
			}

			const validKeys = Object.keys(target).join(", ");

			throw new Error(`\n\x1b[31m[${label} Typo Error]\x1b[0m Property \x1b[33m"${String(prop)}"\x1b[0m does not exist on this ${label} definition.\n` + `  -> \x1b[32mDid you make a spelling mistake?\x1b[0m\n` + `  -> \x1b[35mAvailable definitions:\x1b[0m [ ${validKeys} ]\n` + `  -> \x1b[36mDefined at:\x1b[0m ${location}\n`);
		},
	});
}
function createResolvingSelf() {
	const draft: Record<string, any> = {};
	const self = new Proxy(draft, {
		get(target, key: string) {
			return key in target ? target[key] : makeStub({ category: "self", key });
		},
	});
	return { self, draft };
}
function resolvePool<R extends Record<S, A>>(factory: (self: A) => R, passes: N = 3): R {
	const { self, draft } = createResolvingSelf();
	let out: R;
	for (let i = 0; i < passes; i++) {
		out = factory(self);
		mergeInto(draft, out); // descriptor-based — accessors survive, same helper as before
	}
	return draft as R; // only the FINAL pass's result ever gets exposed/committed
}
function createMaster(kind, kk, ss, mm, rr, initialValue) {
	if (!def.MASTER_ATOM_KEYS.includes(kind)) throw new TypeError(`Unknown master atom: ${kind}`);
	const kindFactory = resolveFactory(death_star.bear.cell(kind));
	if (!kindFactory) throw new TypeError(`No behavior registered for master atom: ${kind}`);

	const [value, setLive] = death_star.bear.useCell.mailbox(initialValue, kk, ss, mm, rr); // once — real subscribe

	return { value, setLive, kindFactory }; // handed to the pool builder, not yet merged
}
export function ATOM_BINDABLE<T extends object>(factory: (ctx: { self: any }) => T): any {
	const build = (instance: any): T => {
		const self = new Proxy({}, {
			get: (_t, key) => instance[key as string],
			set: (_t, key, value) => { instance[key as string] = value; return true; },
			has: (_t, key) => key in instance,
		});
		return factory({ self });
	};
	Object.defineProperty(build, "__build", { value: build, enumerable: false });
	return build;
}
export function ATOM_CONTRACT<C extends AnyObject | Factory, M extends CONTRACT_MASTER<T>, B extends object = Record<S, never>, T = A>(arg1: S | C, arg2?: C) {
	const instanceId = _sid();
	const categoryName = typeof arg1 === "string" ? arg1 : `ATOM_CONTRACT${instanceId}`;
	const rawConfig = (typeof arg1 === "string" ? arg2! : arg1) as AnyType;
	if (rawConfig === undefined || rawConfig === null) throw new Error(`Self is required, define self in your props at ${location}`);
	const factory = resolveFactory(rawConfig as Factory);
	const refs = NexusMirror(categoryName);
	const { selfProxy, liveTarget } = createLiveSelf(instanceId);
	const pass = factory(buildCtx(refs, selfProxy));
	const rr = pass.r ?? death_star.r;
	const mm = pass.m ?? death_star.m;
	const ss = pass.s ?? death_star.s;
	const kk = pass.k ?? death_star.k;
	Object.assign(liveTarget, pass);
	const kind = pass.atom ?? "ATOM";
	const contract = ((initialValue?: T) => {
		const { value, setLive, kindFactory } = createMaster(kind, kk, ss, mm, rr, initialValue);
	
		const pool = resolvePool((self) => {
			mergeInto(self, {
				get value() { return value; },
				set value(next) { setLive(next); },
			});
			mergeInto(self, kindFactory(buildCtx(NexusMirror(kind), self)));
			mergeInto(self, { atom: kind, k: kk, s: ss, m: mm, r: rr });
	
			for (const [bindKey, ref] of Object.entries(pass.bindables ?? {}) as [string, ATOM_BINDABLE][]) {
				mergeInto(self, { [bindKey]: ref({ self, k: kk, s: ss, m: mm, r: rr }) });
			}
			return self;
		}, 3);
	
		return pool as ATOM_CONTRACT_INSTANCE<T, M, B>;
	}) as ATOM_CONTRACT<T, M, B>;
	Object.assign(contract, liveTarget);
	death_star.bear.cell.set(categoryName, contract);
	return withTypoGuard(contract, categoryName) //as unknown as MirrorImageFactory<C>;
}

// #endregion
const liveContract = ATOM_CONTRACT(() => ({
	atom: "VERTEX",
	value: { count: 0 },
	bindables: {
		counter: ATOM_BINDABLE(({ self }) => ({
			increment() {
				self.value = { count: self.value.count + 1 };
				return self.value.count;
			},
		})),
	},
}));

const a = liveContract();
console.log("before:", a.read());        // { count: 0 }
a.counter.increment();
console.log("after, same read fn:", a.read()); // must show { count: 1 } WITHOUT calling read() being redefined/rebuilt
export class RAVENS_WAR_ROOOM2 {
    public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
    public raven : RAVEN

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
    _gt: M2<A, A>

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
    _persistent: M2<A, A>
    _persistent_keys: M2<A, A>
    _listeners: M2<A, Set<(value: A) => void>>
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
        this._gt = this.map('_gt', this.m, this.r)


        this._listeners = this.map('_listeners_raven', this.m, this.r)
        this._persistent_keys = this.map('_persistent_keys_raven', this.m, this.r)
        this._persistent = this.map('_persistent_raven', this.m, this.r)
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
    map<K = A, V=A>(s = this.s, m = this.m, r = this.r, c = 'gt'): M2<K, V> | K | V {
        let root
        switch (c) {
            case '#CONTEXT': { root = this.#CONTEXT.get(r); break; }
            case '#META': { root = this.#META.get(r); break; }
            case '#SIGNALS': { root = this.#SIGNALS.get(r); break; }
            case '#ATOMS': { root = this.#ATOMS.get(r); break; }
            case '#SAURON': { root = this.#SAURON.get(r); break; }
            case '#BUS': { root = this.#BUS.get(r); break; }
            case 'gt': { root = rootRegistry.get(r); break }
            case 'globalThis': { (globalThis as A)[r]; break; }
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
                case 'gt': {rootRegistry.set(r, root); break; }
                case 'globalThis': { (globalThis as A)[r] = root; break; }
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
