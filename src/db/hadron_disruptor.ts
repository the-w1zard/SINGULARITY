//                                                                                               IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
import { scheduler } from "node:timers/promises";
import { ssr } from "../../saurons_eye";
export let THE_WIZARD_ROOT_REG = '__THE_WIZARD_REG__'
export let SAURON_MASTER_REG = '__SAURON__'
export let SAURON_SLAVE_REG = '__STATE__'
// #endregion
//                                                                                                 TYPES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export const TRACER_PID = 0;
export const OP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	COMPACT: 4,
	NONE: 5,
    SEND: 6,
    SUB: 7,
    DUPLEX: 8,
    STREAM:9,
    DRC: 10,
} as const;
const REC = 8;
const F_OP = 0;
const F_PID = 1;
const F_SLOT = 2;
const F_A = 3;
const F_B = 4;
const F_C = 5;
const F_EXT = 7;

export const DEFAULT_SCHEDULE = [
  OP.READ,
  OP.WRITE,
  OP.READ,
  OP.WRITE,
  OP.DELETE,
  OP.EFFECT,
  OP.SEND,
  OP.SUB,
  OP.DUPLEX,
  OP.STREAM,
  OP.DRC,
  OP.COMPACT,
] as const;

const pow2 = (n: N) => n > 0 && (n & (n - 1)) === 0;

export const PAYLOAD = {
    SCALAR: 0,
    BINARY: 1,
} as const;

export type HC_HANDLER = (p: unknown) => unknown | Promise<unknown>;
export type PID = number;
export type SLOT = number;
export type OpKind = (typeof OP)[keyof typeof OP];
export type WaitStrategy = "spin" | "yield" | "park";
export interface HADRON_OPTIONS {
  capacity?: N;
  pidCapacity?: N;
  f64Capacity?: N;
  u32Capacity?: N;
  i32Capacity?: N;
  byteCapacity?: N;
  maxBatch?: N;
  wait?: WaitStrategy;
  shared?: B;
  schedule?: readonly N[];
}
export interface LANE_STATS {
  published: N;
  consumed: N;
  batches: N;
  maxBatch: N;
  fullStalls: N;
  claimed: N;
}
export interface HADRON_COLLIDER_OPTIONS {
  ringCapacity?: number;
  slotCapacity?: number;
  pidCapacity?: number;
  maxBatch?: number;
  soaCapacity?: number;
}
//                                    RECORD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
/* --------------------------------------------------------------------------
   Record layout — 8 x Int32 = 32 bytes.

   0 opcode
   1 pid
   2 slot
   3 a — primary payload / f64 arena offset / u32 arena offset
   4 b — secondary payload / length / count
   5 c — tertiary payload / flags
   6 ext
   7 reserved 
   -------------------------------------------------------------------------- */

// #endregion
// #endregion
//                                                                                                 CODEC 
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
interface ScopeComponents {	r: N; m: N;	s: N;}
const FLYWEIGHT_SCOPE: ScopeComponents = { r: 0, m: 0, s: 0 };
class INJECTION_CODEC2 {
	/* =======================================================================================================================
	  Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested Map trees with direct PID-indexed arrays.
	========================================================================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [INJECTION_CODEC] */
	public readonly maxPids: N;
	public readonly listeners: Array<ListenerFn | UD>;

	public readonly pidToHash: U32;
	public readonly hashToPidMap = new Map<N, N>();
	public nextAllocatedPid = 1;

	public readonly channelMap: I32;
	public readonly subChannelMap: I32;
	public readonly tripChannelMap: I32;

	public readonly dirtyPids: I32;
	public dirtyCount = 0;
	public _flushScheduled = false;
	public freeList: N[] = [];
	constructor(maxPids = 8192) {
		this.maxPids = maxPids;
		this.listeners = new Array(maxPids);
		this.dirtyPids = new Int32Array(maxPids);

		this.pidToHash = new Uint32Array(maxPids);
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
	}
	public getScopeHashForPid(pid: N): N { return this.pidToHash[pid]; }
	public getExistingPid(scopeHash: N): N {
		console.log("getExistingPid");
		return this.hashToPidMap.get(scopeHash) ?? -1;
	}
	public hashComponent(val: A, fallback: S): N {
		const str = String(val ?? fallback);
		let hash = 0x811c9dc5;
		for (let i = 0; i < str.length; i++) {
			hash ^= str.charCodeAt(i);
			hash = Math.imul(hash, 0x01000193) >>> 0;
		}
		return hash;
	}
	public getScopeHash(key: S | SY, sl_reg?: A, ms_reg?: A, globalKey?: A): N {
		const ch = this.hashComponent(globalKey, THE_WIZARD_ROOT_REG);
		const sub = this.hashComponent(ms_reg, SAURON_MASTER_REG);
		const trip = this.hashComponent(sl_reg, SAURON_SLAVE_REG);
		const k = this.hashComponent(key, "KEY");
		// Combine hash channels deterministically
		return (ch ^ sub ^ trip ^ k) >>> 0;
	}
	public parseScopeKey(pid: N): ScopeComponents {
		FLYWEIGHT_SCOPE.r = this.channelMap[pid];
		FLYWEIGHT_SCOPE.m = this.subChannelMap[pid];
		FLYWEIGHT_SCOPE.s = this.tripChannelMap[pid];
		return FLYWEIGHT_SCOPE;
	}
	public allocateScopePid(scopeHash: N, key: A, sl_reg?: A, ms_reg?: A, globalKey?: A): N {
		let pid = this.hashToPidMap.get(scopeHash);
		if (pid !== undefined) return pid;

		pid = this.freeList.pop() ?? this.nextAllocatedPid++;

		if (pid >= this.maxPids) throw new Error("Hadron Notification Hub: PID capacity exceeded");

		this.hashToPidMap.set(scopeHash, pid);
		this.pidToHash[pid] = scopeHash;

		// Populate SoA lanes for zero-alloc reverse lookups
		this.channelMap[pid] = this.hashComponent(globalKey, THE_WIZARD_ROOT_REG);
		this.subChannelMap[pid] = this.hashComponent(ms_reg, SAURON_MASTER_REG);
		this.tripChannelMap[pid] = this.hashComponent(sl_reg, SAURON_SLAVE_REG);

		return pid;
	}
	public markDirty(pid: N): V {
		this.dirtyPids[this.dirtyCount++] = pid;
	}
	public subscribe(pid: N, fn: ListenerFn): () => V {
		this.listeners[pid] = fn;
		return () => {
			this.listeners[pid] = undefined;
		};
	}
	public flushNotifications(): V {
		this._flushScheduled = false;
		for (let i = 0; i < this.dirtyCount; i++) {
			const pid = this.dirtyPids[i];
			const fn = this.listeners[pid];
			if (fn !== undefined) {
				fn(pid);
			}
		}
		this.dirtyCount = 0; // Reset dirty stack for next batch
	}
	public notifyHook(key: S) {
		// 1. Map the string key to its Hadron PID
		const scopeHash = this.getScopeHash(key);
		const pid = this.allocateScopePid(scopeHash, key); // 2. Mark PID dirty in the Hadron Notification ring buffer
		this.markDirty(pid); // 3. Schedule microtask flush if not already scheduled (Your original guard!)
		if (!this._flushScheduled) {
			this._flushScheduled = true;
			queueMicrotask(() => {
				// Flushes all dirty PIDs accumulated across microtasks
				this._flushScheduled = false;
				this.flushNotifications();
			});
		}
	}
	public notifyPid(pid: N, value: N): V {
		const fn = this.listeners[pid];

		if (fn !== undefined) {
			fn(pid, value);
		}
	}
	public releasePid(pid: N): V {
		this.hashToPidMap.delete(this.pidToHash[pid]);
		this.pidToHash[pid] = 0;
		this.freeList.push(pid);
	}
} 

export class HighPerformanceSoACodec {
    public readonly maxPids: number;
    
    // SoA Memory Layout (Typed Arrays for Cache Locality)
    public readonly channelMap: Int32Array;
    public readonly subChannelMap: Int32Array;
    public readonly tripChannelMap: Int32Array;
    public readonly keyMap: Int32Array;
    
    public readonly listeners: Array<((pid: number, val?: number) => void) | undefined>;
    public nextPid = 1;

    constructor(maxPids = 8192) {
        this.maxPids = maxPids;
        this.channelMap = new Int32Array(maxPids);
        this.subChannelMap = new Int32Array(maxPids);
        this.tripChannelMap = new Int32Array(maxPids);
        this.keyMap = new Int32Array(maxPids);
        this.listeners = new Array(maxPids);
    }

    // Hot path lookup or allocation: O(1) inline integer assignment
    public allocatePid(rId: number, mId: number, sId: number, kId: number): number {
        // In a true LMAX disruptor ring or fixed socket registry, 
        // PIDs can be pre-mapped or statically routed at startup.
        const pid = this.nextPid++;
        if (pid >= this.maxPids) throw new Error("PID capacity exceeded");

        this.channelMap[pid] = rId;
        this.subChannelMap[pid] = mId;
        this.tripChannelMap[pid] = sId;
        this.keyMap[pid] = kId;

        return pid;
    }
}
export class CONVERT_CHANNELS_AND_PIDS {
	/**
	const registry = new UnifiedRegistry(8192);
	const disruptor = new DisruptorSoA(8192);
	
	// 1. Register a Single String
	const pidSingle = registry.resolveSingle("user:logged_in");
	
	// 2. Register a Multi-Channel address
	const pidMulti = registry.resolveMulti("THE_WIZARD", "SAURON_MASTER", "SLAVE_01", "STATE_MUTATION");
	
	// 3. Publish both raw PIDs into the Disruptor (Hot path - 0 allocations)
	disruptor.publish(pidSingle, 1, 0);
	disruptor.publish(pidMulti, 99.5, 1);
	
	// 4. Consume and decode depending on what kind of PID it is
	while (disruptor.poll((pid, value, flag) => {
		// Check type lane or call decode based on your routing context
		if (registry.typeMap[pid] === 0) {
			const key = registry.decodeSingle(pid);
			console.log(`[SINGLE] Key: "${key}" | Value: ${value}`);
		} else {
			const scope = registry.decodeMulti(pid);
			console.log(`[MULTI] Channels: ${scope.r} > ${scope.m} > ${scope.s} > ${scope.k} | Value: ${value}`);
		}
	})) {
		// Drains ring buffer
	}
	 */
    private stringToId = new Map<string, number>();
    private idToString: string[] = [];
    private addressToPidMap = new Map<string, number>();
    public nextPid = 1;
    // SoA Memory Lanes (Indexed by PID)
    public readonly typeMap: Uint8Array;       // 0 = Single String, 1 = Multi-Channel
    public readonly channelMap: Int32Array;    // r channel
    public readonly subChannelMap: Int32Array; // m channel
    public readonly tripChannelMap: Int32Array;// s channel
    public readonly keyMap: Int32Array;        // k channel / single key string ID
    // Zero-alloc flyweight for multi-channel decoding
    public readonly multiFlyweight = { r: "", m: "", s: "", k: "" };
    constructor(maxPids = 65536) {
        this.typeMap = new Uint8Array(maxPids);
        this.channelMap = new Int32Array(maxPids);
        this.subChannelMap = new Int32Array(maxPids);
        this.tripChannelMap = new Int32Array(maxPids);
        this.keyMap = new Int32Array(maxPids);
    }
    private intern(val: string): number {
        let id = this.stringToId.get(val);
        if (id === undefined) {
            id = this.idToString.length;
            this.stringToId.set(val, id);
            this.idToString[id] = val;
        }
        return id;
    }// Input Style A: Register a SINGLE string key -> returns a PID.
    public resolveSingle(key: string): number {
        let pid = this.addressToPidMap.get(key);
        if (pid === undefined) {
            pid = this.nextPid++;
            this.addressToPidMap.set(key, pid);

            this.typeMap[pid] = 0; // Mark as Single String
            this.keyMap[pid] = this.intern(key);
        }
        return pid;
    }// Input Style B: Register 3-4 channel strings -> returns a PID.
    public resolveMulti(r: string, m: string, s: string, k = ""): number {
        const addressKey = `${r}:${m}:${s}:${k}`;
        let pid = this.addressToPidMap.get(addressKey);
        if (pid === undefined) {
            pid = this.nextPid++;
            this.addressToPidMap.set(addressKey, pid);

            this.typeMap[pid] = 1; // Mark as Multi-Channel
            this.channelMap[pid] = this.intern(r);
            this.subChannelMap[pid] = this.intern(m);
            this.tripChannelMap[pid] = this.intern(s);
            this.keyMap[pid] = this.intern(k);
        }
        return pid;
    }// Decode PID back into a Single String (if registered that way).
    public decodeSingle(pid: number): string {
        return this.idToString[this.keyMap[pid]] ?? "";
    }// Decode PID back into 3-4 Channels (zero-alloc flyweight mutation).
    public decodeMulti(pid: number): typeof this.multiFlyweight {
        this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
        this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
        this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
        this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
        return this.multiFlyweight;
    }
}
class INJECTION_CODEC {
/*  =============================================================================
	 Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested 
	 Map trees with direct PID-indexed arrays.
	================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [INJECTION_CODEC] */
	private ids = new Map<S, N>();
	private names: S[] = [];
	private pids = new Map<S, N>();          // "a:b:c:d" → pid
	private addresses = new Map<N, [N, N, N, N]>(); // pid → [a,b,c,d]
	private nextId = 0;
	// ── hot path (was INJECTION_CODEC) — SoA, per-pid, high volume ──
	public readonly maxPids: N;
	public readonly listeners: Array<ListenerFn | UD>;
	public readonly pidToHash: U32;          // keep? see note
	public nextAllocatedPid = 1;             // 0 = TRACER_PID
	public readonly channelMap: I32;
	public readonly subChannelMap: I32;
	public readonly tripChannelMap: I32;
	public readonly dirtyPids: I32;
	public dirtyCount = 0;
	public _flushScheduled = false;
	constructor(maxPids = 8192) {
		this.maxPids = maxPids;
		this.listeners = new Array(maxPids);
		this.dirtyPids = new Int32Array(maxPids);
		this.pidToHash = new Uint32Array(maxPids);
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
	}
	private intern(value: S): N {
		let id = this.ids.get(value);
		if (id === undefined) {
			id = this.nextId++;
			this.ids.set(value, id);
			this.names[id] = value;
		}
		return id;
	}
	/** Exact identity: encode address → pid (interned, no collisions). */
	public encode(pid_key: S, sl: S, ms: S, rt: S): N {
		const a = this.intern(rt);
		const b = this.intern(ms);
		const c = this.intern(sl);
		const d = this.intern(pid_key);
		const addressKey = `${a}:${b}:${c}:${d}`;

		let pid = this.pids.get(addressKey);
		if (pid === undefined) {
			pid = this.nextAllocatedPid++;
			if (pid >= this.maxPids) throw new Error("INJECTION_CODEC: PID capacity exceeded");
			this.pids.set(addressKey, pid);
			this.addresses.set(pid, [a, b, c, d]);
			// populate SoA lanes from the interned ids (no re-hash)
			this.channelMap[pid] = a;
			this.subChannelMap[pid] = b;
			this.tripChannelMap[pid] = c;
		}
		return pid;
	}
	/** Exact decode: pid → address. */
	public decode(pid: N): { key: S; sl: S; ms: S; rt: S } {
		const addr = this.addresses.get(pid);
		if (!addr) throw new Error(`INJECTION_CODEC: unknown pid ${pid}`);
		const [a, b, c, d] = addr;
		return { r: this.names[a], m: this.names[b], s: this.names[c], k: this.names[d] };
	}
	public parseScopeKey(pid: N): ScopeComponents {
		FLYWEIGHT_SCOPE.r = this.channelMap[pid];
		FLYWEIGHT_SCOPE.m = this.subChannelMap[pid];
		FLYWEIGHT_SCOPE.s = this.tripChannelMap[pid];
		return FLYWEIGHT_SCOPE;
	}
	public markDirty(pid: N): V { this.dirtyPids[this.dirtyCount++] = pid; }
	public subscribe(pid: N, fn: ListenerFn): () => V {
		this.listeners[pid] = fn;
		return () => { this.listeners[pid] = undefined; };
	}
	public flushNotifications(): V {
		this._flushScheduled = false;
		for (let i = 0; i < this.dirtyCount; i++) {
			const pid = this.dirtyPids[i];
			const fn = this.listeners[pid];
			if (fn !== undefined) fn(pid);
		}
		this.dirtyCount = 0;
	}
	public notifyHook(key: S): V {
		const pid = this.encode(key, "", "", "");
		this.markDirty(pid);
		if (!this._flushScheduled) {
			this._flushScheduled = true;
			queueMicrotask(() => { this._flushScheduled = false; this.flushNotifications(); });
		}
	}
	public notifyPid(pid: N, value: N): V {
		const fn = this.listeners[pid];
		if (fn !== undefined) fn(pid, value);
	}
	public releasePid(pid: N): V {
		const addr = this.addresses.get(pid);
		if (addr) {
			this.pids.delete(`${addr[0]}:${addr[1]}:${addr[2]}:${addr[3]}`);
			this.addresses.delete(pid);
		}
		this.pidToHash[pid] = 0;
	}
}
const TAG = {
	UNDEFINED: 0,
	NULL: 1,
	FALSE: 2,
	TRUE: 3,
	INT32: 4,
	FLOAT64: 5,
	STRING: 6,
	ARRAY: 7,
	OBJECT: 8,
	UINT8ARRAY: 9,
	REF: 10,
} as const;
export class BEAM_CODEC {
	/* =============================================================================
     Encodes ordinary JavaScript values into a collider-owned byte slab.
	 ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
	private bytes: U8;
	private view: DataView;
	private tail = 0;

	private strings = new Map<S, N>();
	private stringBytes: U8[] = [];
	private refs = new Map<object, N>();
	private nextRef = 0;

	constructor(initialCapacity = 1 << 16) {
		this.bytes = new Uint8Array(initialCapacity);
		this.view = new DataView(this.bytes.buffer);
	}
	encode(value: U): U8 {
		this.tail = 0;
		this.strings.clear();
		this.stringBytes.length = 0;
		this.refs.clear();
		this.nextRef = 0;

		this.writeValue(value);

		return this.bytes.subarray(0, this.tail);
	}
	/* HC_BINARY_CODEC */
	decode(binary: U8): U {
		const savedBytes = this.bytes;
		const savedView = this.view;
		const savedTail = this.tail;

		try {
			this.bytes = binary;
			this.view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
			this.tail = binary.length;

			this.refs.clear();
			this.nextRef = 0;

			const cursor = { at: 0 };

			return this.readValue(cursor);
		} finally {
			// Restore the encode-side buffer so the next encode()
			// never writes into the disruptor pool.
			this.bytes = savedBytes;
			this.view = savedView;
			this.tail = savedTail;
		}
	}
	private need(count: N): V {
		if (this.tail + count <= this.bytes.length) return;

		let size = this.bytes.length;

		while (size < this.tail + count) size *= 2;

		const next = new Uint8Array(size);
		next.set(this.bytes);

		this.bytes = next;
		this.view = new DataView(next.buffer);
	}
	private u8(value: N): V {
		this.need(1);
		this.bytes[this.tail++] = value;
	}
	private i32(value: N): V {
		this.need(4);
		this.view.setInt32(this.tail, value, true);
		this.tail += 4;
	}
	private f64(value: N): V {
		this.need(8);
		this.view.setFloat64(this.tail, value, true);
		this.tail += 8;
	}
	private writeValue(value: U): V {
		if (value === undefined) return this.u8(TAG.UNDEFINED);
		if (value === null) return this.u8(TAG.NULL);
		if (value === false) return this.u8(TAG.FALSE);
		if (value === true) return this.u8(TAG.TRUE);
		const type = typeof value;
		if (type === "number") {
			const n = value as N;
			if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
				this.u8(TAG.INT32);
				return this.i32(n);
			}

			this.u8(TAG.FLOAT64);
			return this.f64(n);
		}

		if (type === "string") {
			return this.writeString(value as S);
		}

		if (Array.isArray(value)) {
			return this.writeArray(value);
		}

		if (value instanceof Uint8Array) {
			return this.writeBytes(value);
		}

		if (type === "object") {
			return this.writeObject(value as object);
		}

		throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
	}
	private writeString(value: S): V {
		const encoded = new TextEncoder().encode(value);

		this.u8(TAG.STRING);
		this.i32(encoded.length);
		this.need(encoded.length);
		this.bytes.set(encoded, this.tail);
		this.tail += encoded.length;
	}
	private writeArray(value: U[]): V {
		const ref = this.beginRef(value);

		this.u8(TAG.ARRAY);
		this.i32(value.length);

		for (let i = 0; i < value.length; i++) {
			this.writeValue(value[i]);
		}

		this.endRef(ref);
	}
	private writeBytes(value: U8): V {
		this.u8(TAG.UINT8ARRAY);
		this.i32(value.length);
		this.need(value.length);
		this.bytes.set(value, this.tail);
		this.tail += value.length;
	}
	private writeObject(value: object): V {
		const ref = this.beginRef(value);
		const keys = Object.keys(value);

		this.u8(TAG.OBJECT);
		this.i32(keys.length);

		for (const key of keys) {
			this.writeString(key);
			this.writeValue((value as any)[key]);
		}

		this.endRef(ref);
	}
	private beginRef(value: object): N {
		const existing = this.refs.get(value);

		if (existing !== undefined) {
			this.u8(TAG.REF);
			this.i32(existing);
			return -1;
		}

		const ref = this.nextRef++;
		this.refs.set(value, ref);

		return ref;
	}
	private endRef(ref: N): V {
		// Reserved for future back-reference patching.
		void ref;
	}
	private readU8(cursor: { at: N }): N {
		return this.bytes[cursor.at++];
	}
	private readI32(cursor: { at: N }): N {
		const value = this.view.getInt32(cursor.at, true);
		cursor.at += 4;

		return value;
	}
	private readF64(cursor: { at: N }): N {
		const value = this.view.getFloat64(cursor.at, true);
		cursor.at += 8;

		return value;
	}
	private readValue(cursor: { at: N }): U {
		const tag = this.readU8(cursor);

		switch (tag) {
			case TAG.UNDEFINED:
				return undefined;

			case TAG.NULL:
				return null;

			case TAG.FALSE:
				return false;

			case TAG.TRUE:
				return true;

			case TAG.INT32:
				return this.readI32(cursor);

			case TAG.FLOAT64:
				return this.readF64(cursor);

			case TAG.STRING:
				return this.readString(cursor);

			case TAG.ARRAY:
				return this.readArray(cursor);

			case TAG.OBJECT:
				return this.readObject(cursor);

			case TAG.UINT8ARRAY:
				return this.readBytes(cursor);

			default:
				throw new Error(`HC_BINARY_CODEC: unknown tag ${tag}`);
		}
	}
	private readString(cursor: { at: N }): S {
		const length = this.readI32(cursor);
		const bytes = this.bytes.subarray(cursor.at, cursor.at + length);

		cursor.at += length;

		return new TextDecoder().decode(bytes);
	}
	private readArray(cursor: { at: N }): U[] {
		const length = this.readI32(cursor);
		const out: U[] = new Array(length);

		for (let i = 0; i < length; i++) {
			out[i] = this.readValue(cursor);
		}

		return out;
	}
	private readObject(cursor: { at: N }): Record<S, U> {
		const length = this.readI32(cursor);
		const out: Record<S, U> = {};

		for (let i = 0; i < length; i++) {
			const key = this.readString(cursor);
			out[key] = this.readValue(cursor);
		}

		return out;
	}
	private readBytes(cursor: { at: N }): Uint8Array {
		const length = this.readI32(cursor);
		const out = this.bytes.slice(cursor.at, cursor.at + length);

		cursor.at += length;

		return out;
	}
}
// #endregion
//                                                                                     HADRON_SLOT_STORE
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

class STORAGE_RING {
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
		constructor(capacity: number, slabCapacity = 1 << 20) {
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

// #endregion
//                                                                                                    VM
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export const BC = {
	HALT: 0,

	LOAD_EVENT_FIELD: 1,
	LOAD_F64: 2,
	STORE_F64: 3,

	ADD_F64: 4,
	SUB_F64: 5,
	MUL_F64: 6,
	DIV_F64: 7,

	LOAD_IMMEDIATE_F64: 8,

	MARK_DIRTY: 9,
	EMIT: 10,
	EMIT_F64: 11,

	JUMP: 12,
	JUMP_IF_ZERO: 13,
	JUMP_IF_NOT_ZERO: 14,
} as const;
export class RF_CAVITY_VM {
	private pc = 0;
	private acc = 0;

	constructor(
		private readonly d: HADRON_DISRUPTOR,
		private readonly values: F64,
		private readonly event: F64,
	) {}

	run(code: I32, pid: N, slot: N): V {
		this.pc = 0;
		this.acc = 0;

		for (;;) {
			const op = code[this.pc++];

			switch (op) {
				case BC.HALT:
					return;

				case BC.LOAD_EVENT_FIELD: {
					const field = code[this.pc++];
					this.acc = this.event[field];
					break;
				}

				case BC.STORE_F64: {
					const target = code[this.pc++];
					this.values[target] = this.acc;
					break;
				}

				case BC.EMIT: {
					const lane = code[this.pc++];
					this.d.emitRaw(lane, pid, slot, this.acc | 0);
					break;
				}

				case BC.ADD_F64: {
					const operand = code[this.pc++];
					this.acc += this.values[operand];
					break;
				}

				case BC.JUMP: {
					this.pc = code[this.pc++];
					break;
				}

				case BC.JUMP_IF_ZERO: {
					const target = code[this.pc++];

					if (this.acc === 0) {
						this.pc = target;
					}

					break;
				}

				default:
					throw new Error(`BYTECODE_VM: unknown opcode ${op} at pc=${this.pc - 1}`);
			}
		}
	}
}
// #endregion
//                                                                                         SOA_DISRUPTOR 
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class BOOSTER_DISRUPTOR {
/*  =============================================================================

	=============================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [POWER_OF_TWO] */
   public readonly capacity: number;
   public readonly mask: number;
   // ── Structure of Arrays (SoA) Event Lanes ──────────────────────────────
   public readonly pids: Int32Array;
   public readonly values: Float64Array;
   public readonly flags: Uint8Array;
   // ── Cursors ────────────────────────────────────────────────────────────
   public writeCursor = 0;
   public readCursor = 0;

   constructor(capacity = 65536) {// Enforce power-of-two for bitwise masking
	   if ((capacity & (capacity - 1)) !== 0) {throw new Error("Disruptor capacity must be a power of two.");}
	   this.capacity = capacity;
	   this.mask = capacity - 1;
	   this.pids = new Int32Array(capacity);// Pre-allocate contiguous typed memory blocks (Zero GC churn)
	   this.values = new Float64Array(capacity);
	   this.flags = new Uint8Array(capacity);
   }// Publishes an event directly into the next ring slot. O(1) time complexity, zero heap allocation, single CPU instruction cycle.
   public send(pid: number, value: number, flag = 0): boolean {// Optional backpressure check: Prevent writer from overlapping reader
	   if (this.writeCursor - this.readCursor >= this.capacity) {return false;}// Ring buffer full
	   const index = this.writeCursor & this.mask;
	   this.pids[index] = pid;// Write directly to parallel SoA typed array lanes
	   this.values[index] = value;
	   this.flags[index] = flag;
	   this.writeCursor++;
	   return true;
   }// Drains and processes the ring buffer in batches using a zero-allocation loop.
   public sub(handler: (pid: number, value: number, flag: number) => void): number {
	   const targetWrite = this.writeCursor;
	   let currentRead = this.readCursor;
	   if (currentRead === targetWrite) {return 0;}// Nothing to consume 
	   let processed = 0;
	   while (currentRead < targetWrite) {
		   const index = currentRead & this.mask;// Execute handler on raw lane data
		   handler(this.pids[index],this.values[index],this.flags[index]);
		   currentRead++;
		   processed++;
	   }
	   this.readCursor = currentRead;
	   return processed;
   }// Resets cursors and ring lanes instantly without reallocating memory.
   public reset(): void {
	   this.writeCursor = 0;
	   this.readCursor = 0;
	   this.pids.fill(0);
	   this.values.fill(0);
	   this.flags.fill(0);
   }
	constructor() {}
	// reg fns
	public write() {}
	public read() {}
	public subscribe() {}
	public edit() {}
	public delete() {}
	public reclaim_pid() {}
	public pass() {}
	public length() {}
	public load_factor() {}
	public sweep_expired() {}
	public compact() {}
	public stream() {}
	public duplex() {}
	public on_change() {}
}
// #endregion
//                                                                                    PARTICLE_DISRUPTOR 
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class LINAC_LANE {
	/*  =====================================================================================================
		 --- DisruptorSoA.editDirect ---
		 Peak (Best Batch): 902.06 M ops/sec
		 Average:           701.57 M ops/sec
		======================================================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC]  */
		// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
		//                                                                                                               SOA
		// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
		private readonly capacity: N;
		private readonly lane: I32;
		private readonly ops: U8;
		public readonly posX: F32;
		public readonly posY: F32;
		public readonly val: I32;
		private readonly freePids: I32;
		private freeHead = 0;
		private freeTail = 0;
		private laneLen = 0;
		private ids = new Map<S, N>();
		private names: S[] = [];
		private pids = new Map<S, N>();           // "a:b:c:d" → pid
		private addresses = new Map<N, [N, N, N, N]>(); // pid → [a,b,c,d]
		private nextId = 0;
		public readonly maxPids: N;
		public readonly listeners: Array<ListenerFn | UD>;
		public readonly pidToHash: U32;         
		public nextAllocatedPid = 1;             // 0 = TRACER_PID
		public readonly channelMap: I32;
		public readonly subChannelMap: I32;
		public readonly tripChannelMap: I32;
		public readonly dirtyPids: I32;
	
		constructor(capacity?: number) {
			this.capacity = capacity !== undefined ? capacity : 1024;
	
			this.lane = new Int32Array(this.capacity).fill(-1);
			this.ops = new Uint8Array(this.capacity);
			this.posX = new Float32Array(this.capacity);
			this.posY = new Float32Array(this.capacity);
			this.val = new Int32Array(this.capacity);
			this.freePids = new Int32Array(this.capacity);
	
			for (let i = 1; i < this.capacity; i++) { 
				this.freePids[this.freeTail++] = i; 
			}
	
			this.lane[0] = TRACER_PID;
			this.val[TRACER_PID] = 0;
			this.ops[TRACER_PID] = OP.NONE;
			this.laneLen = 1;
		}
	
		public check_size_and_edit(x: N, y: N, pid: Pid, value: N) {
			this.posX[pid] = x; 
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.EDIT;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		} 
	
		public getX(pid: N): number { return this.posX[pid]; }
		public getY(pid: N): number { return this.posY[pid]; }  
		public getPosition(pid: N): { x: number; y: number } {
			return { x: this.posX[pid], y: this.posY[pid] };
		}
	
		// --- Wired Operations (Minimum Required Set) ---
	
		public write(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.WRITE;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public create(x: N, y: N, value: N, pid: Pid): V {
			this.write(x, y, value, pid);
		}
	
		public edit(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.EDIT;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public read(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.READ;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public delete(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.DELETE;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public effect(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.EFFECT;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public compact(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.COMPACT;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public none(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.NONE;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public send(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.SEND;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public sub(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.SUB;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public duplex(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.DUPLEX;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public stream(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.STREAM;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		public drc(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.ops[pid] = OP.DRC;
			if (this.laneLen < this.capacity) {
				this.lane[this.laneLen++] = pid;
			}
		}
	
		// --- Pass Loop ---
	
		public pass(): V {
			const len = this.laneLen;
			let writeIdx = 0;
			let i = 0;
			const unrolledLimit = len - (len % 4);
		   
			for (; i < unrolledLimit; i += 4) {
			  let pid = this.lane[i];
			  if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
			  else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
			  else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
		   
			  pid = this.lane[i + 1];
			  if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
			  else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
			  else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
		   
			  pid = this.lane[i + 2];
			  if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
			  else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
			  else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
		   
			  pid = this.lane[i + 3];
			  if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
			  else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
			  else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
			}
		   
			for (; i < len; i++) {
			  const pid = this.lane[i];
			  if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
			  else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
			  else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
			}
		   
			this.laneLen = writeIdx;
		} 
	
		public get tick(): N { return this.val[TRACER_PID]; }
		public get length(): N { return this.laneLen; }
		
		public getLoadFactor(): N {
			const activeCount = Math.max(0, this.laneLen - 1);
			return activeCount / (this.capacity - 1);
		}   
	
		public waitFor() {}
		public sweep_expired() {}
		public on_change() {}
		private reclaimPid(pid: N) {}
	}
class LINAC_DISRUPTOR {
	public k: ATOM_KEY = '1';
	constructor(capacity?: number) {
		this.capacity = capacity !== undefined ? capacity : 1024;

	
	}
}
// #endregion
//                                                                                    GOVERNOR REGULATOR
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class CRYO_REGULATOR {}
class BEAM_CONTROLLER {
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
  constructor(private readonly hub: DISRUPTOR_HUB) {}
  wake(): void {
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
      this.hub.collider.pass();

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
  currentTier(): number {
    return this.tier;
  }
  isHibernating(): boolean {
    return this.hibernating;
  }
}
// #endregion
//                                                                                         DISRUPTOR_HUB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class HADRON_PARTICLE_DISRUPTOR {
	//                                                                                             CONSTANTS
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public collider: HADRON_COLLIDER;
	public slabs: HADRON_SLOT_STORE;
	public pid_codec: PID_CODEC;
	public disruptor: HADRON_DISRUPTOR;
	public governor: DISRUPTOR_GOVERNOR;
	public binary_codec = new HC_BINARY_CODEC();
	public address_codec = new HC_ADDRESS_CODEC();
	public MAX_PENDING = 4096;
	public sentTimestamps;
	public pendingResolvers;
	public cursor = 0;
	public readonly counter: U32;
	public _byteid() {
		return ++this.counter[0];
	}
	private static readonly MAX_INLINE = 4096;
	private static readonly DEFAULT_TTL_MS = 86_400_000;

	constructor(options: HADRON_COLLIDER_OPTIONS = {}) {
		const slotCapacity = options.slotCapacity ?? 1 << 20;
		const ringCapacity = options.ringCapacity ?? 1 << 18;
		const pidCapacity = options.pidCapacity ?? 1 << 20;

		this.counter = new Uint32Array(1); // Allocates a single 4-byte chunk in memory
		this.sentTimestamps = new Float64Array(this.MAX_PENDING);
		this.pendingResolvers = new Array(this.MAX_PENDING);

		this.governor = new DISRUPTOR_GOVERNOR(this);
		this.pid_codec = new PID_CODEC(options.pidCapacity ?? 1 << 20);
		this.slabs = new HADRON_SLOT_STORE(slotCapacity);
		this.disruptor = new HADRON_DISRUPTOR(this, {
			capacity: ringCapacity,
			pidCapacity,
			maxBatch: options.maxBatch,
		});
		this.collider = new HADRON_COLLIDER(this, this.disruptor, options);
		this.fulfil_this();
	}
	// #endregion
	//                                                                                           SCALER PATH
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public set_kernel(d: HADRON_DISRUPTOR, ring: I32, from: N, to: N, epoch: N, values: F64, versions: U32): V {
	
	}
	public get_kernel(d: HADRON_DISRUPTOR, ring: I32, from: N, to: N, epoch: N, dirty: U32, subscribers: I32): N {
	
	}
	public DELETE_KERNEL(d: HADRON_DISRUPTOR, ring: I32, from: N, to: N, epoch: N, nextFree: I32, refCount: I32, freeHead: I32): V {
		
	}
	public COMPACT_KERNEL(values: F64, versions: U32, nextFree: I32, freeHead: I32, slotCount: N): N {
		let write = 0;
		const freeSet = nextFree;

		// Mark free slots with -1 during the free-list pass.
		for (let at = freeHead[0]; at !== -1; at = freeSet[at]) {
			freeSet[at] = -1;
		}

		for (let read = 0; read < slotCount; read++) {
			if (freeSet[read] === -1) continue;

			if (write !== read) {
				values[write] = values[read];
				versions[write] = versions[read];
			}

			write++;
		}

		// Rebuild free tail.
		for (let at = write; at < slotCount; at++) {
			freeSet[at] = at + 1 < slotCount ? at + 1 : -1;
		}

		freeHead[0] = write < slotCount ? write : -1;

		return slotCount - write;
	}

	// #endregion
	//                                                                      900 DISRUPTOR OR LINAC_DISRUPTOR
	//                                                                     900 DISRUPTOR OR LINAC_DISRUPTOR
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public get= (() => {
		const fn = (k: A, s: S, m: S, r: S) => {}
		fn.slab = (k: A, s: S, m: S, r: S) => {}
		fn.kernel = (k: A, s: S, m: S, r: S) => {
			const mask = d.mask;
			let invalidated = 0;
	
			for (let seq = from; seq < to; seq++) {
				const base = (seq & mask) << 3;
	
				const pid = ring[base + 1];
				const slot = ring[base + 2];
	
				const word = pid >>> 5;
				const bit = 1 << (pid & 31);
	
				if ((dirty[word] & bit) !== 0) {
					dirty[word] &= ~bit;
					subscribers[slot] = epoch;
					invalidated++;
				}
			}
	
			return invalidated;
		}
		fn.s_class = (k: A, s: S, m: S, r: S) => {}
		fn.module = (k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public set= (() => {
		const fn = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.slab = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.kernel = (p: U,k: A, s: S, m: S, r: S) => {
			const mask = d.mask;

			for (let seq = from; seq < to; seq++) {
				const base = (seq & mask) << 3;
	
				const slot = ring[base + 2];
				const operand = ring[base + 3];
	
				values[slot] = d.f64At(operand);
				versions[slot] = epoch;
			}
		}
		fn.s_class = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.module = (p: U,k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public edit= (() => {
		const fn = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.slab = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.kernel = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.s_class = (p: U,k: A, s: S, m: S, r: S) => {}
		fn.module = (p: U,k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public has= (() => {
		const fn = (k: A, s: S, m: S, r: S) => {}
		fn.slab = (k: A, s: S, m: S, r: S) => {}
		fn.kernel = (k: A, s: S, m: S, r: S) => {}
		fn.s_class = (k: A, s: S, m: S, r: S) => {}
		fn.module = (k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public getMany= (() => {
		const fn = (particles: A[]) => {}
		fn.slab = (particles: A[]) => {}
		fn.kernel = (particles: A[]) => {}
		return fn
    })();
	public setMany= (() => {
		const fn = (particles: A[]) => {}
		fn.slab = (particles: A[]) => {}
		fn.kernel = (particles: A[]) => {}
		return fn
    })();
	public delete= (() => {
		const fn = (k: A, s: S, m: S, r: S) => {}
		fn.slab = (k: A, s: S, m: S, r: S) => {}
		fn.kernel = (k: A, s: S, m: S, r: S) => {
			const mask = d.mask;
		let head = freeHead[0];

		for (let seq = from; seq < to; seq++) {
			const base = (seq & mask) << 3;

			const slot = ring[base + 2];

			if (--refCount[slot] <= 0) {
				nextFree[slot] = head;
				head = slot;
			}
		}

		freeHead[0] = head;
		}
		fn.s_class = (k: A, s: S, m: S, r: S) => {}
		fn.module = (k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public deleteMany= (() => {
		const fn = (particles: A[]) => {}
		fn.slab = (particles: A[]) => {}
		fn.kernel = (particles: A[]) => {}
		return fn
    })();
	public waitFor= (() => {
		const fn = (k: A, s: S, m: S, r: S) => {
		}
		fn.slab = (k: A, s: S, m: S, r: S) => {}
		fn.kernel = (k: A, s: S, m: S, r: S) => {}
		fn.s_class = (k: A, s: S, m: S, r: S) => {}
		fn.module = (k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public subscribe= (() => {
		const fn = (k: A, s: S, m: S, r: S) => {}
		fn.slab = (k: A, s: S, m: S, r: S) => {}
		fn.kernel = (k: A, s: S, m: S, r: S) => {}
		fn.s_class = (k: A, s: S, m: S, r: S) => {}
		fn.module = (k: A, s: S, m: S, r: S) => {}
		return fn
    })();
	public _emit(p: U, k: A, s: S, m: S, r: S): V { }
	// #endregion
	//                                                                                     BOOSTER_DISRUPTOR
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public send= (() => {
		const fn = (p: U, k: A, s: S, m: S, r: S, ttl?: N) => {}
		fn.slab = (p: U, k: A, s: S, m: S, r: S, ttl?: N) => {}
		
		return fn
    })();
	public sub= (() => {
		const fn = (handler: (pid: N, value: N) => void, k: A, s: S, m: S, r: S) => {}
		fn.slab = (handler: (pid: N, value: N) => void, k: A, s: S, m: S, r: S) => {}
		
		return fn
    })();
	// #endregion
	//                                    
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	private resolveValue(p: U): U {
		return typeof p === "function" ? (p as () => U)() : p;
	}
	private pidOf(k: A, s: S, m: S, r: S): PID {
		const pid = this.existing(k, s, m, r);
		return pid >= 0 ? pid : this.alloc(k, s, m, r);
	}
	public sweepExpired(): N {
		let released = 0;
		const now = Date.now();

		for (let pid = 0; pid < this.collider.capacity; pid++) {
			const expiresAt = this.collider.expiresAt[pid];

			if (expiresAt !== 0 && now > expiresAt) {
				this.collider.expiresAt[pid] = 0;
				this.collider.delete(pid); // existing function
				released++;
			}
		}

		return released;
	}
	public address(pid: PID) {
		return this.address_codec.decode(pid);
	}
	public awaitReply(msgId: S, timeout: N | UD): P<U> {
		return new Promise((resolve) => {
			const timer = timeout
				? setTimeout(() => {
						this.pending.delete(msgId);
						resolve({ success: false, timeout: true });
					}, timeout)
				: undefined;

			this.pending.set(msgId, { resolve, timer });
		});
	}
	public resolveReply(originalMsgId: S, value: U): B {
		const tx = this.pending.get(originalMsgId);
		if (!tx) return false;
		if (tx.timer) clearTimeout(tx.timer);
		this.pending.delete(originalMsgId);
		tx.resolve(value);
		return true;
	}
	private readonly seen = new Set<string>();
	public dispatch(msg: SIG_BUS_Message): void {
		// ── REPLY ──────────────────────────────────────────────
		if (msg.k && msg.k.startsWith(":reply:")) {
			const originalMsgId = msg.k.replace(":reply:", "");
			if (this.resolveReply(originalMsgId, msg.p)) {
				return;
			}
		}

		// ── DEDUPE / TTL ───────────────────────────────────────
		if (this.seen.has(msg.id)) return;
		if (msg && msg.ts)
			if (msg.ttl && Date.now() - msg.ts > msg.ttl) {
				return;
			}
		if (this.seen.size > 10000) this.seen.clear();
		this.seen.add(msg.id);

		// ── DELIVER ────────────────────────────────────────────
		const handlers = ssr.se.handlers.matching(msg.k, msg.s, msg.m, msg.r);
		if (handlers.length === 0) {
			this.resolveReply(msg.id, { success: false });
			return;
		}

		Promise.all(handlers.map((handler) => Promise.resolve(handler(msg.p)).catch(() => ({ success: false })))).then((results) => {
			const value = results.length === 1 ? results[0] : results;

			if (msg.transport === "soa") {
				this.sendReply_soa(msg, value);
				return;
			}
			if (msg.transport === "particle") {
				this.sendReply_particle(msg, value);
				return;
			}
		});
	}
	private sendReply_soa(original: SIG_BUS_Message, value: U): V {
		const replyMsg: SIG_BUS_Message = {
			id: `${original.id}:reply`,
			r: original.r,
			m: original.m,
			s: original.s,
			k: `:reply:${original.k}`,
			p: value,
			ts: Date.now(),
			_ack: false,
			binary: false,
			transport: "soa",
		};

		const frame = this.binary_codec.encode(replyMsg);
		const ref = this.slabs.allocBytes(frame);

		const pid = this.existing(original.k, original.s, original.m, original.r);

		const target = pid >= 0 ? pid : this.alloc(original.k, original.s, original.m, original.r);

		this.collider.admit_soa(ref.offset, ref.length, 0, target);
		this.collider.ops[target] = OP.SEND;
	}
	private sendReply_particle(original: SIG_BUS_Message, value: U): V {
		const replyMsg: SIG_BUS_Message = {
			id: `${original.id}:reply`,
			r: original.r,
			m: original.m,
			s: original.s,
			k: `:reply:${original.k}`,
			p: value,
			ts: Date.now(),
			_ack: false,
			binary: false,
			transport: "particle",
		};

		const frame = this.binary_codec.encode(replyMsg);
		const ref = this.slabs.allocBytes(frame);

		this.disruptor.emitRaw(OP.SEND, -1, -1, ref.offset, ref.length);
	}
	private fulfil_this() {
		this.disruptor.fulfil_this(this);
		this.collider.fulfil_this(this, this.disruptor);
	}
	public hash(key: A, s: S, m: S, r: S) {
		return this.pid_codec.getScopeHash(key, s, m, r);
	}
	public alloc(key: A, s: S, m: S, r: S): N {
	}
	public existing(key: A, s: S, m: S, r: S): N {
		console.log("existing");

		return this.pid_codec.getExistingPid(this.hash(key, s, m, r));
	}
	// #endregion
}
export const hub = new DISRUPTOR_HUB()
// #endregion
//                                                                                          MaybePromise
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════


function MaybePromise<R = any>(v: R): MaybePromise {
    return v !== undefined ? v as R : Promise.resolve(v) as P<R>
}
// #endregion














