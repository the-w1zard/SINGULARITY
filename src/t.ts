
import { performance } from "node:perf_hooks";



 

	// ---- interned registry IDs (assigned once at init) ----
	const ROOT_STRIDE = 8; // max distinct roots
	const MASTER_STRIDE = 64; // max distinct masters
	const SLAVE_STRIDE = 256; // max distinct slaves
	const TOTAL_SLOTS = ROOT_STRIDE * MASTER_STRIDE * SLAVE_STRIDE;
	
	const internRoot = new Map<string, number>();
	const internMaster = new Map<string, number>();
	const internSlave = new Map<string, number>();
	
	let rootCursor = 0
	let	masterCursor = 0
	let	slaveCursor = 0
	
	const idFor = (m: Map<string, number>, s: string, cur: () => number): number => {
		let id = m.get(s);
		if (id === undefined) {
			id = cur();
			m.set(s, id);
		}
		return id;
	};
	
	// ---- the flat registry ----
	// leaf type is parameterized: M2BI for numeric/V128 payloads, Map for arbitrary
	type Leaf<K, V> = A;
	const REGISTRY: AR<Leaf<A, A> | UD> = new Array(TOTAL_SLOTS);
	type LeafFactory = (rootId: N, masterId: N, slaveId: N) => Leaf<A, A>; // optional per-root factory: default = M2BI with sensible cap/stride
	let leafFactory: LeafFactory = () => new M2B(4096, 1);
	function configureLeafFactory(f: LeafFactory): V {
		leafFactory = f;
	}
	function setRootKey(root: S): N {
		return idFor(internRoot, root, () => rootCursor++);
	}
	function getNestedMap3<K = N, V = N>(s: S, m: S, r: S): Leaf<K, V> {
		// hot path — same call shape as getNestedMap(s, m, r)
		const rid = idFor(internRoot, r, () => rootCursor++);
		const mid = idFor(internMaster, m, () => masterCursor++);
		const sid = idFor(internSlave, s, () => slaveCursor++);
	
		const idx = (rid * MASTER_STRIDE + mid) * SLAVE_STRIDE + sid;
	
		let leaf = REGISTRY[idx];
		if (leaf === undefined) {
			leaf = leafFactory(rid, mid, sid);
			REGISTRY[idx] = leaf;
		}
		return leaf as Leaf<K, V>;
	}
	
	type Key = number | string | symbol | object;
	type Simd = Float64Array | Float32Array | number[];

	export class M2Bin<K = number, V = number> {
		#vals: Float64Array;
		#pidToSlot: Int32Array;
		#slotToPid: Int32Array;
		#size = 0;
		#odd: Map<Key, V> | null = null;
	
		constructor(capacity = 1024) {
			const cap = Math.max(1, capacity | 0);
			this.#pidToSlot = new Int32Array(cap).fill(-1);
			this.#slotToPid = new Int32Array(cap).fill(-1);
			this.#vals = new Float64Array(cap);
		}
	
		get size(): number {
			return this.#size + (this.#odd === null ? 0 : this.#odd.size);
		}
		get capacity(): number {
			return this.#vals.length;
		}
		/** live backing buffer: floating-point value of slot i is buffer[i] */
		get buffer(): Float64Array {
			return this.#vals;
		}
	
		// ---- Map API ----------------------------------------------------------
		get(k: K): V | undefined {
			const p = this.#pidToSlot;
			if (typeof k === "number" && k >= 0 && k < p.length) {
				const i = p[k];
				return (i < 0 ? undefined : this.#vals[i]) as V | undefined;
			}
			return this.#odd === null ? undefined : this.#odd.get(k as Key);
		}
	
		has(k: K): boolean {
			if (typeof k === "number" && k >= 0) return k < this.#pidToSlot.length && this.#pidToSlot[k] !== -1;
			return this.#odd !== null && this.#odd.has(k as Key);
		}
	
		set(k: K, v: V): this {
			if (typeof v === "number" && typeof k === "number" && k >= 0) {
				this.#vals[this.#open(k)] = v;
				return this;
			}
			(this.#odd ??= new Map()).set(k as Key, v);
			return this;
		}
	
		delete(k: K): boolean {
			if (typeof k === "number" && k >= 0) {
				if (k >= this.#pidToSlot.length) return false;
				const i = this.#pidToSlot[k];
				if (i < 0) return false;
				const last = --this.#size;
				const lastPid = this.#slotToPid[last];
				if (i !== last) {
					this.#vals[i] = this.#vals[last];
					this.#slotToPid[i] = lastPid;
					this.#pidToSlot[lastPid] = i;
				}
				this.#pidToSlot[k] = -1;
				this.#slotToPid[last] = -1;
				return true;
			}
			return this.#odd !== null && this.#odd.delete(k as Key);
		}
	
		clear(): void {
			this.#pidToSlot.fill(-1);
			this.#slotToPid.fill(-1);
			this.#size = 0;
			this.#odd?.clear();
		}
	
		*keys(): MapIterator<K> {
			for (let i = 0; i < this.#size; i++) yield this.#slotToPid[i] as K;
			if (this.#odd !== null) yield* this.#odd.keys() as MapIterator<K>;
		}
		*values(): MapIterator<V> {
			for (let i = 0; i < this.#size; i++) yield this.#vals[i] as V;
			if (this.#odd !== null) yield* this.#odd.values();
		}
		*entries(): MapIterator<[K, V]> {
			for (let i = 0; i < this.#size; i++) yield [this.#slotToPid[i] as K, this.#vals[i] as V];
			if (this.#odd !== null) yield* this.#odd.entries() as MapIterator<[K, V]>;
		}
		[Symbol.iterator](): MapIterator<[K, V]> {
			return this.entries();
		}
	
		forEach(fn: (v: V, k: K, m: Map<K, V>) => void, thisArg?: unknown): void {
			for (let i = 0; i < this.#size; i++) fn.call(thisArg, this.#vals[i] as V, this.#slotToPid[i] as K, this);
			this.#odd?.forEach((v, k) => fn.call(thisArg, v, k as K, this));
		}
	
		// ---- binary lane: no Map ceremony, no boxing ---------------------------
		/** read by pid */
		g(pid: number): V | undefined {
			const i = this.#pidToSlot[pid];
			return (i < 0 ? undefined : this.#vals[i]) as V | undefined;
		}
		/** write by pid */
		s(v: number, pid: number): this {
			this.#vals[this.#open(pid)] = v;
			return this;
		}
		/** read with fallback — the accumulator primitive */
		getOr(pid: number, dflt = 0): number {
			const i = this.#pidToSlot[pid];
			return i < 0 ? dflt : this.#vals[i];
		}
		/** pid += delta, returns the new value */
		add(pid: number, delta: number): number {
			const i = this.#open(pid);
			return (this.#vals[i] += delta);
		}
		in(pid: number): boolean {
			return this.#pidToSlot[pid] !== -1;
		}
		d(pid: number): boolean {
			return this.delete(pid as unknown as K);
		}
		/** walk (pid, slot) without allocating an iterator */
		forEachRaw(fn: (pid: number, slot: number) => void): void {
			for (let i = 0; i < this.#size; i++) fn(this.#slotToPid[i], i);
		}
		/** values for a list of pids, NaN where absent */
		gather(pids: ArrayLike<number>, out = new Float64Array(pids.length)): Float64Array {
			for (let i = 0; i < pids.length; i++) {
				const s = this.#pidToSlot[pids[i]];
				out[i] = s < 0 ? NaN : this.#vals[s];
			}
			return out;
		}
	
		#open(pid: number): number {
			let p = this.#pidToSlot;
			if (pid >= p.length) {
				this.#grow(pid);
				p = this.#pidToSlot;
			}
			let i = p[pid];
			if (i < 0) {
				if (this.#size === p.length) {
					this.#grow(this.#size);
					p = this.#pidToSlot;
				}
				i = this.#size++;
				p[pid] = i;
				this.#slotToPid[i] = pid;
			}
			return i;
		}
		#grow(need: number): void {
			let cap = this.#pidToSlot.length;
			while (cap <= need) cap *= 2;
			const p2s = new Int32Array(cap).fill(-1);
			p2s.set(this.#pidToSlot);
			this.#pidToSlot = p2s;
			const s2p = new Int32Array(cap).fill(-1);
			s2p.set(this.#slotToPid);
			this.#slotToPid = s2p;
			const v = new Float64Array(cap);
			v.set(this.#vals);
			this.#vals = v;
		}
	}
	export interface M2Bin<K, V> extends M<K, V> {}
	Object.setPrototypeOf(M2Bin.prototype, Map.prototype);
	export class M2Dense<V = number> {
		#vals: F64;
		#live: U8;
		#size = 0;
		#odd: M<Key, V> | null = null;
	
		constructor(capacity = 1024) {
			const cap = Math.max(1, capacity | 0);
			this.#vals = new Float64Array(cap);
			this.#live = new Uint8Array(cap);
		}
	
		get size(): N {
			return this.#size + (this.#odd === null ? 0 : this.#odd.size);
		}
		get capacity(): N {
			return this.#vals.length;
		}
		/** live backing buffer: value of pid p is buffer[p], valid where live[p] === 1 */
		get buffer(): F64 {
			return this.#vals;
		}
		get live(): U8 {
			return this.#live;
		}
	
		// ---- Map API ----------------------------------------------------------
		get(k: number): V | UD {
			const l = this.#live;
			if (typeof k === "number" && k >= 0 && k < l.length && l[k] === 1) return this.#vals[k] as V;
			if (typeof k !== "number" || k < 0) return this.#odd === null ? undefined : this.#odd.get(k as Key);
			return undefined;
		}
		has(k: number): boolean {
			const l = this.#live;
			if (typeof k === "number" && k >= 0 && k < l.length) return l[k] === 1;
			return this.#odd !== null && this.#odd.has(k as Key);
		}
		set(k: number, v: V): this {
			if (typeof v === "number" && typeof k === "number" && k >= 0) {
				if (k >= this.#live.length) this.#grow(k);
				if (this.#live[k] === 0) {
					this.#live[k] = 1;
					this.#size++;
				}
				this.#vals[k] = v;
				return this;
			}
			(this.#odd ??= new Map()).set(k as Key, v);
			return this;
		}
		delete(k: number): B {
			const l = this.#live;
			if (typeof k === "number" && k >= 0) {
				if (k >= l.length || l[k] === 0) return false;
				l[k] = 0;
				this.#size--;
				return true;
			}
			return this.#odd !== null && this.#odd.delete(k as Key);
		}
		clear() {
			this.#live.fill(0);
			this.#size = 0;
			this.#odd?.clear();
		}
		*keys(): MapIterator<N> {
			const l = this.#live,
				n = l.length;
			for (let k = 0; k < n; k++) if (l[k] === 1) yield k;
			if (this.#odd !== null) yield* this.#odd.keys() as MapIterator<N>;
		}
		*values(): MapIterator<V> {
			const l = this.#live,
				n = this.#size;
			if (this.#odd?.size) {
				const odd = [...this.#odd.values()];
				let seen = 0;
				for (let k = 0; k < l.length && seen < n; k++)
					if (l[k] === 1) {
						seen++;
						yield this.#vals[k] as V;
					}
				yield* odd;
				return;
			}
			let seen = 0;
			for (let k = 0; k < l.length && seen < n; k++)
				if (l[k] === 1) {
					seen++;
					yield this.#vals[k] as V;
				}
		}
		*entries(): MapIterator<[N, V]> {
			const l = this.#live;
			for (let k = 0; k < l.length; k++) if (l[k] === 1) yield [k, this.#vals[k] as V];
			if (this.#odd !== null) yield* this.#odd.entries() as MapIterator<[N, V]>;
		}
		[Symbol.iterator](): MapIterator<[N, V]> {
			return this.entries();
		}
		forEach(fn: (v: V, k: N, m: M<N, V>) => V, thisArg?: U) {
			const l = this.#live;
			for (let k = 0; k < l.length; k++) if (l[k] === 1) fn.call(thisArg, this.#vals[k] as V, k, this as U as Map<N, V>);
			this.#odd?.forEach((v, k) => fn.call(thisArg, v, k as N, this as U as Map<N, V>));
		}
	
		// ---- binary lane -------------------------------------------------------
		/** read by pid — no typeof guard: pids are numbers */
		g(pid: N): V | UD {
			return this.#live[pid] === 1 ? (this.#vals[pid] as V) : undefined;
		}
		/** write by pid */
		s(v: N, pid: N): this {
			if (pid >= this.#live.length) this.#grow(pid);
			if (this.#live[pid] === 0) {
				this.#live[pid] = 1;
				this.#size++;
			}
			this.#vals[pid] = v;
			return this;
		}
		getOr(pid: N, dflt = 0): N {
			return this.#live[pid] === 1 ? this.#vals[pid] : dflt;
		}
		/** pid += delta, returns the new value */
		add(pid: N, delta: N): N {
			if (pid >= this.#live.length) this.#grow(pid);
			if (this.#live[pid] === 0) {
				this.#live[pid] = 1;
				this.#size++;
			}
			return (this.#vals[pid] += delta);
		}
		in(pid: N): B {
			return this.#live[pid] === 1;
		}
		/** walk live pids without allocating an iterator */
		forEachRaw(fn: (pid: N, value: N) => V) {
			const l = this.#live;
			for (let k = 0; k < l.length; k++) if (l[k] === 1) fn(k, this.#vals[k]);
		}
		/** values for a list of pids, NaN where absent */
		gather(pids: ArrayLike<N>, out = new Float64Array(pids.length)): Float64Array {
			for (let i = 0; i < pids.length; i++) {
				const p = pids[i];
				out[i] = this.#live[p] === 1 ? this.#vals[p] : NaN;
			}
			return out;
		}
		#grow(need: N) {
			let cap = this.#live.length;
			while (cap <= need) cap *= 2;
			const v = new Float64Array(cap);
			v.set(this.#vals);
			this.#vals = v;
			const l = new Uint8Array(cap);
			l.set(this.#live);
			this.#live = l;
		}
	}
	// export interface M2Dense<K, V>  {}
	export class M2Rows<K = N> {
		#vals: Float64Array;
		#pidToSlot: Int32Array;
		#slotToPid: Int32Array;
		#size = 0;
		readonly stride: N;
	
		constructor(capacity = 1024, stride = 3) {
			this.stride = Math.max(1, stride | 0);
			const cap = Math.max(1, capacity | 0);
			this.#pidToSlot = new Int32Array(cap).fill(-1);
			this.#slotToPid = new Int32Array(cap).fill(-1);
			this.#vals = new Float64Array(cap * this.stride);
		}
	
		get size(): N {
			return this.#size;
		}
		get capacity(): N {
			return this.#pidToSlot.length;
		}
		get buffer(): Float64Array {
			return this.#vals;
		}
	
		get(pid: N): Float64Array | UD {
			const i = this.#pidToSlot[pid] as N;
			if (i < 0) return undefined;
			const o = i * this.stride;
			return this.#vals.subarray(o, o + this.stride);
		}
		has(pid: N): boolean {
			const i = this.#pidToSlot[pid];
			return i !== undefined && i !== -1;
		}
		set(pid: N, v: Float64Array | ArrayLike<N>): this {
			this.write(pid, v);
			return this;
		}
		delete(pid: N): boolean {
			const i = this.#pidToSlot[pid] as N;
			if (i === undefined || i < 0) return false;
			const st = this.stride;
			const last = --this.#size;
			const lastPid = this.#slotToPid[last];
			if (i !== last) {
				this.#vals.copyWithin(i * st, last * st, last * st + st);
				this.#slotToPid[i] = lastPid;
				this.#pidToSlot[lastPid] = i;
			}
			this.#pidToSlot[pid] = -1;
			this.#slotToPid[last] = -1;
			return true;
		}
		clear(): void {
			this.#pidToSlot.fill(-1);
			this.#slotToPid.fill(-1);
			this.#size = 0;
		}
	
		/** copy a row in, no validation per component */
		write(pid: N, v: Float64Array | ArrayLike<N>): V {
			const o = this.#open(pid) * this.stride;
			this.#vals.set(v as ArrayLike<N>, o);
		}
		/** single component: row[c] = v */
		put(pid: N, c: N, v: N): V {
			this.#vals[this.#open(pid) * this.stride + c] = v;
		}
		/** single component: row[c] */
		at(pid: N, c: N): N {
			return this.#vals[this.#pidToSlot[pid] * this.stride + c];
		}
		/** pid -> slot, or -1 */
		slotOf(pid: N): N {
			const i = this.#pidToSlot[pid];
			return i === undefined ? -1 : i;
		}
		/** zero-copy row view you can write through */
		row(pid: N): F64 | UD {
			return this.get(pid);
		}
		/** view over the live prefix of the whole table — feed straight to a shader/worker */
		live(): F64 {
			return this.#vals.subarray(0, this.#size * this.stride);
		}
		*keys(): MapIterator<K> {
			for (let i = 0; i < this.#size; i++) yield this.#slotToPid[i] as K;
		}
		*values(): MapIterator<F64> {
			const st = this.stride;
			for (let i = 0; i < this.#size; i++) yield this.#vals.subarray(i * st, i * st + st);
		}
		*entries(): MapIterator<[K, F64]> {
			const st = this.stride;
			for (let i = 0; i < this.#size; i++) yield [this.#slotToPid[i] as K, this.#vals.subarray(i * st, i * st + st)];
		}
		[Symbol.iterator](): MapIterator<[K, F64]> {
			return this.entries();
		}
		forEach(fn: (v: F64, k: K, m: this) => V, thisArg?: U): V {
            for (const [k, v] of this.entries()) fn.call(thisArg, v, k, this);
        }
		forEachRaw(fn: (pid: N, slot: N) => V): V {
			for (let i = 0; i < this.#size; i++) fn(this.#slotToPid[i], i);
		}
	
		#open(pid: N): N {
			let p = this.#pidToSlot;
			if (pid >= p.length) {
				this.#grow(pid);
				p = this.#pidToSlot;
			}
			let i = p[pid];
			if (i < 0) {
				if (this.#size === p.length) {
					this.#grow(this.#size);
					p = this.#pidToSlot;
				}
				i = this.#size++;
				p[pid] = i;
				this.#slotToPid[i] = pid;
			}
			return i;
		}
		#grow(need: N): V {
			let cap = this.#pidToSlot.length;
			while (cap <= need) cap *= 2;
			const p2s = new Int32Array(cap).fill(-1);
			p2s.set(this.#pidToSlot);
			this.#pidToSlot = p2s;
			const s2p = new Int32Array(cap).fill(-1);
			s2p.set(this.#slotToPid);
			this.#slotToPid = s2p;
			const v = new Float64Array(cap * this.stride);
			v.set(this.#vals);
			this.#vals = v;
		}
	}
	export interface M2Rows<K>  {}
	Object.setPrototypeOf(M2Rows.prototype, Map.prototype);
	
	export const SAURON_SLAVE_REG = "SAURON_SLAVE_REG" as const;
	export const SAURON_MASTER_REG = "SAURON_MASTER_REG" as const;
	export const THE_WIZARD_ROOT_REG = "THE_WIZARD_ROOT_REG" as const;
	const LEAF_CAP = 1024;/** last resolve, so the hot call costs three compares and no Map lookup */
	let memoRoot: U = null;
	let memoMaster: U = null;
	let memoServant: U = null;//let memoLeaf: U = null;
	export type NestedLeaf<Stride extends N = 1> = Stride extends 1 ? M2Bin<N, N> : M2Rows<N>;

	export function getNestedMap<K = N, V = N>(s: S = SAURON_SLAVE_REG, m: S = SAURON_MASTER_REG, r: S = THE_WIZARD_ROOT_REG): M2Bin<K, V> {
		const g = globalThis as U as Record<S, U>;
		let root = g[r] as Map<S, Map<S, M2Bin<K, V>>> | UD;
		if (!root) {
			root = new Map();
			g[r] = root;
		}
	
		if (root === memoRoot && m === memoMaster && s === memoServant) return memoLeaf as M2Bin<K, V>;
	
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new Map();
			root.set(m, masterMap);
		}
		let leaf = masterMap.get(s);
		if (!leaf) {
			leaf = new M2Bin<K, V>(LEAF_CAP);
			masterMap.set(s, leaf);
		}
	
		memoRoot = root;
		memoMaster = m;
		memoServant = s;
		memoLeaf = leaf;
		return leaf;
	}
	/** Same registry, strided rows. Stride is fixed per (r, m, s). */
	export function getNestedRows(s: string = SAURON_SLAVE_REG, m: string = SAURON_MASTER_REG, r: string = THE_WIZARD_ROOT_REG, stride = 3): M2Rows<number> {
		const g = globalThis as unknown as Record<string, unknown>;
		let root = g[r] as Map<string, Map<string, M2Rows<number>>> | undefined;
		if (!root) {
			root = new Map();
			g[r] = root;
		}
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new Map();
			root.set(m, masterMap);
		}
		let leaf = masterMap.get(s);
		if (!leaf) {
			leaf = new M2Rows<number>(LEAF_CAP, stride);
			masterMap.set(s, leaf);
		} else if (leaf.stride !== stride) {
			throw new Error(`getNestedRows: stride mismatch for ${r}/${m}/${s}: have ${leaf.stride}, asked ${stride}`);
		}
		return leaf;
	}
	/** drop the memo — call after replacing globalThis[r] wholesale */ 
	// ---- 1. Interned Registry Constants & Dense Integer IDs ----
	export const SAURON_SLAVE_REGN = 0;
	export const SAURON_MASTER_REGN = 0;
	export const THE_WIZARD_ROOT_REGN = 0;
	
	// Maximum bounds for the flat binary tensor grid (Root x Master x Slave)
	//const ROOT_STRIDE = 8; // Max 8 roots
	//const MASTER_STRIDE = 64; // Max 64 masters per root
	//const SLAVE_STRIDE = 256; // Max 256 slaves per master
	//const TOTAL_SLOTS = ROOT_STRIDE * MASTER_STRIDE * SLAVE_STRIDE; // 131,072 total fixed slots
	
	// ---- 2. Pre-allocated Contiguous Binary Store Array ----
	// Holds M2Bin or M2Rows instances directly with zero Map lookups on the hot path
	const BINARY_REGISTRY: AR<M2Bin<N, N> | M2Rows<N> | UD> = new Array(TOTAL_SLOTS);
	// Hot-path cache registers (single-slot cache for sub-nanosecond repeated calls)
	let memoRid = -1;
	let memoMid = -1;
	let memoSid = -1;
	let memoLeaf: U = null;
	// ---- 3. True-Binary Hot-Path Resolution Function ----
	export function getNestedMapBinary(sid: N = Number(SAURON_SLAVE_REG), mid: N = Number(SAURON_MASTER_REG), rid: N = Number(THE_WIZARD_ROOT_REG), capacity: N = 1024): M2Bin<N, N> {
		// 1. Hot path cache check: single branch, zero hash, zero allocation
		if (rid === memoRid && mid === memoMid && sid === memoSid) {
			return memoLeaf as M2Bin<N, N>;
		}
	
		// 2. Compute flat binary index via bitshifts / direct multiplication
		// Index formula: (rid * MASTER_STRIDE + mid) * SLAVE_STRIDE + sid
		const idx = rid * 16384 + mid * 256 + sid; // Optimized stride multipliers
	
		let leaf = BINARY_REGISTRY[idx];
		if (leaf === undefined) {
			leaf = new M2Bin<N, N>(capacity);
			BINARY_REGISTRY[idx] = leaf;
		}
	
		// Update memo registers
		memoRid = rid;
		memoMid = mid;
		memoSid = sid;
		memoLeaf = leaf;
	
		return leaf as M2Bin<N, N>;
	}
	// ---- 4. Strided Variant for Multi-Component Buffers (Positions, Velocities) ----
	export function getNestedRowsBinary(sid: N = Number(SAURON_SLAVE_REG), mid: N = Number(SAURON_MASTER_REG), rid: N = Number(THE_WIZARD_ROOT_REG), stride: N = 3, capacity: N = 1024): M2Rows<N> {
		const idx = rid * 16384 + mid * 256 + sid;
	
		let leaf = BINARY_REGISTRY[idx];
		if (leaf === undefined) {
			leaf = new M2Rows<N>(capacity, stride);
			BINARY_REGISTRY[idx] = leaf;
		} else if ((leaf as M2Rows<N>).stride !== stride) {
			throw new Error(`getNestedRowsBinary: stride mismatch at index ${idx}: have ${(leaf as M2Rows<N>).stride}, asked ${stride}`);
		}
	
		return leaf as M2Rows<N>;
	}
	// Reset cache helper





export function resetBinaryCache(): void {
		memoRid = -1;
		memoMid = -1;
		memoSid = -1;
		memoLeaf = null;
}
class CONVERT_CHANNELS_AND_PIDS {
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
type BYTE_MAP<K = A, V = A> = {
	pidToSlot: I32;
	set(k: K, v: V): V;
	get(k: K): V | undefined;
	has(k: K): B;
	delete(k: K): B;
	readonly size: N;
};
// const MODULE_CACHE = new BYTEOP_MAP<A, BYTEOP_MAP<A, BYTEOP_MAP<A, A>>>();

const FLYWEIGHT_SCOPE: ScopeComponents = { r: 0, m: 0, s: 0, rt: 1, ms: 1, sl: 1 };

class TRIPLE_TIERED_MAP2 {
	/*  =============================================================================
     Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested 
     Map trees with direct PID-indexed arrays.
    ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [INJECTION_CODEC] */
	private ids = new M2<S, N>();
	private names: S[] = [];
	private pids = new M2<S, N>(); // "a:b:c:d" → pid
	private addresses = new M2<N, [N, N, N, N]>(); // pid → [a,b,c,d]
	private nextId = 0;
	// ── hot path (was INJECTION_CODEC) — SoA, per-pid, high volume ──
	public readonly maxPids: N;
	public readonly listeners: Array<ListenerFn | UD>;
	public readonly pidToHash: U32; // keep? see note
	public nextAllocatedPid = 1; // 0 = TRACER_PID
	public readonly channelMap: I32;
	public readonly subChannelMap: I32;
	public readonly tripChannelMap: I32;
	constructor(maxPids = 8192) {
		this.maxPids = maxPids;
		this.listeners = new Array(maxPids);
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
	}/** Exact identity: encode address → pid (interned, no collisions). */
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
	}/** Exact decode: pid → address. */
	public decode(pid: N): { key: S; sl: S; ms: S; rt: S } {
		const addr = this.addresses.get(pid);
		if (!addr) throw new Error(`INJECTION_CODEC: unknown pid ${pid}`);
		const [a, b, c, d] = addr;
		return { rt: this.names[a], ms: this.names[b], sl: this.names[c], key: this.names[d] };
	}
	public parseScopeKey(pid: N): ScopeComponents {
		FLYWEIGHT_SCOPE.r = this.channelMap[pid];
		FLYWEIGHT_SCOPE.m = this.subChannelMap[pid];
		FLYWEIGHT_SCOPE.s = this.tripChannelMap[pid];
		return FLYWEIGHT_SCOPE;
	}
}
class BYTEOP_MAP<K = A, V = A> {
/*  ==========================================================================================================================
     Gives us what we need to replace javscripts map, allowing us to build the triped tiered nested map
    ============================================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BYTEOP_MAP] */
    readonly pidToSlot: I32;
    private keyToId = new Map<K, N>();
    private keys: (K | undefined)[];
    private values: (V | undefined)[];
    private freeSlots: N[] = [];
    private nextId = 0;
    readonly maxParticles: N; 
    constructor(maxParticles: N = 65536) {
        this.maxParticles = maxParticles; 
        this.pidToSlot = new Int32Array(maxParticles).fill(-1);
        this.keys = new Array(maxParticles);
        this.values = new Array(maxParticles);
    }

    set(k: K, v: V): V {
        let id = this.keyToId.get(k);
        if (id === undefined) {
            id = this.nextId < this.maxParticles
                ? this.nextId++
                : this.freeSlots.pop() ?? -1;
            if (id === -1) throw new RangeError("BYTE_MAP capacity exceeded");
            this.keyToId.set(k, id);
            this.keys[id] = k;
            this.pidToSlot[id] = id;
        }
        this.values[this.pidToSlot[id]] = v;
        return v;
    }

    get(k: K): V | undefined {
        const id = this.keyToId.get(k);
        return id === undefined ? undefined : this.values[this.pidToSlot[id]];
    }

    has(k: K): B {
        return this.keyToId.has(k);
    }

    delete(k: K): B {
        const id = this.keyToId.get(k);
        if (id === undefined) return false;
        this.keyToId.delete(k);
        this.keys[id] = undefined;
        this.values[this.pidToSlot[id]] = undefined;
        this.pidToSlot[id] = -1;
        this.freeSlots.push(id);
        return true;
    }

    get size(): N {
        return this.keyToId.size;
    }
}
class BYTE_MAP21 {
        readonly pidToSlot: I32;
        constructor(maxParticles: N= 65_536) { this.pidToSlot = new Int32Array(maxParticles).fill(-1);	}
        set(pid: N, slot: N): V { this.pidToSlot[pid] = slot;	}
        get(pid: N): N { return this.pidToSlot[pid];	}
        delete(pid: N): V { this.pidToSlot[pid] = -1;	}
}
type NestedRoot = M<S, M<S, M2Dense<N>>>;

// const LEAF_CAP = 1024;

// let memoRoot: NestedRoot | UD;
// let memoMaster: S | UD;
let memoSlave: S | UD;
// let memoLeaf: M2Dense<number> | UD;

export function getNestedMap6(	s: S = SAURON_SLAVE_REG,	m: S = SAURON_MASTER_REG,	r: S = THE_WIZARD_ROOT_REG,): M2Dense<N> {
	const g = globalThis as U as Record<S, U>;
	let root = g[r] as NestedRoot | UD;

	// Avoid nested Map lookups for the most recently requested leaf.
	if (root && root === memoRoot && m === memoMaster && s === memoSlave) {
		return memoLeaf! as A;
	}

	if (!root) {
		root = new Map();
		g[r] = root;
	}

	let masterMap = root.get(m);
	if (!masterMap) {
		masterMap = new Map();
		root.set(m, masterMap);
	}

	let leaf = masterMap.get(s);
	if (!leaf) {
		leaf = new M2Dense<N>(LEAF_CAP);
		masterMap.set(s, leaf);
	}

	memoRoot = root;
	memoMaster = m;
	memoSlave = s;
	memoLeaf = leaf;

	return leaf;
}

export function resetNestedMapCache(): V {
	memoRoot = undefined;
	memoMaster = undefined;
	memoSlave = undefined;
	memoLeaf = undefined;
}

export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);

type F64 = Float64Array;
type V128<T = A> = N | F64 | T;   // a "value" is either scalar (stride=1) or a stride-length Float64 view
type Derived = {
	deps: AR<N>;
	fn: (...vals: AR<V128>) => V128;
	dirty?: B;
	has?: B;
	value?: V128 | A;
};
export type M2BEvent = {
	op?: "set" | "upd" | "del" | "dirty";
	pid: N;
	v?: V128;
	prev?: V128;
	has?: B;
	batch?: B;
};
type Sub = (e: M2BEvent) => V;
const sameValue128 = (a: V128 | UD, b: V128 | UD): B => {
	if (a === b) return true;
	if (a instanceof Float64Array && b instanceof Float64Array) {
		if (a.length !== b.length) return false;
		for (let i = 0; i < a.length; i++) if (!Object.is(a[i], b[i])) return false;
		return true;
	}
	return Object.is(a, b);
};
export class M2<H = A, T = A> extends Map<H, T> implements M2<H, T> {
	// public g = this.get;
	// public h = this.has;
	// public d = this.delete;
	// public c = this.clear
	public f = this.forEach;//                                                     [FOR EACH]

	#subs?: Set<Sub>;
	#hist?: AR<M2Event<H, T>>;
	#histLimit = 0;
	#strict = false;
	#iterating = 0;
	#batching = 0;
	#pending?: M2<H, M2Event<H, T>>;
	#derived?: M2<H, Derived>;
	#dependents?: M2<H, Set<H>>;
	#emit(e: M2Event<H, T>): V {
		if (this.#batching > 0) {
			// coalesce: last write per key wins, keep prev from first event
			const pending = this.#pending!.get(e.k);
			this.#pending!.set(
				e.k,
				pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true },
			);
			return;
		}
	
		this.#record(e);
		this.#subs?.forEach((sub) => {
			try { sub(e); } catch (err: A) { process.emitWarning(err); }
		});
	}
	#record(e: M2Event<H, T>): V {
		if (!this.#hist) return;

		this.#hist.push(e);

		if (this.#hist.length > this.#histLimit) {
			this.#hist.splice(0, this.#hist.length - this.#histLimit);
		}
	}
	#invalidate(k: H): V {
		const deps = this.#dependents?.get(k);
		if (!deps) return;

		for (const dk of deps) {
			const d = this.#derived?.get(dk);
			if (!d || d.dirty) continue;

			d.dirty = true;
			this.#emit({ op: "dirty", k: dk, has: Boolean(d.has ?? false) } as A);
		}
	}
	#computeDerived<ZZ = A>(k: H, d: Derived): T {
		const vals = d.deps.map((dep:A) => this.g(dep) as T);
		const next = d.fn(...vals as A);

		const had = d.has;
		const prev = d.value as A

		d.value = next;
		d.has = true;
		d.dirty = false;

		if (!had || !sameValue(prev, next)) {
			this.#emit({
				op: had ? "upd" : "set",
				k,
				v: next,
				prev: prev as A,
				has: true, pid: 0
			});
		}

		return next;
	}
	#assertNotIterating(op: S): V {
		if (this.#strict && this.#iterating > 0) {
			throw new Error(`M2.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
		}
	}
	public [Symbol.iterator](): MapIterator<[H, T]> {return this.entries();	}
	public on(fn: Sub): () => V {//                                                [ON]
		(this.#subs ??= new Set()).add(fn);
		return () => this.off(fn);
	}
	public off(fn: Sub): V {//                                                    [OFF]
		this.#subs?.delete(fn);
	} 
	public batch(fn: () => V): this {//                                               [BATCH]
		this.#batching++;
		this.#pending ??= new M2<H, M2Event<H, T>>();
	
		try {
			fn();
		} finally {
			this.#batching--;
	
			if (this.#batching === 0) {
				const events = [...this.#pending.values()];
				this.#pending = undefined;
	
				for (const e of events) {
					this.#record(e);
					this.#subs?.forEach(sub => sub(e));
				}
			}
		}
	
		return this;
	}
	public s(v: UF<T>, k: H): this {//                                                  [SET]
		this.#assertNotIterating("s");

		const next = rv(v);
		const has = this.h(k);
		const prev = has ? this.g(k) : undefined;

		super.set(k, next);

		this.#invalidate(k);
		this.#emit({
			op: "set",
			k,
			v: next,
			prev: prev as T,
			has, pid: 0
		});

		return this;
	}
	public mutate(k: H, fn: (old: T, has: true) => T): T | UD {//                    [MUTATE]
		this.#assertNotIterating("mutate");
	
		if (!this.h(k)) return undefined;
	
		const old = this.g(k) as T;
		const next = fn(old, true);
	
		this.s(next, k);
		return next;
	}
	public d(k: H): B {//                                                            [DELETE]
		this.#assertNotIterating("delete");

		if (!this.h(k)) return false;

		const prev = this.g(k) as T;
		const out = super.delete(k);

		this.#invalidate(k);
		this.#emit({
			op: "del",
			k,
			prev,
			has: false,
              pid: 0
		});

		return out;
	}
	public c(): V {//                                                                 [CLEAR]
		
		this.#assertNotIterating("clear");

		const entries = [...this.entries()];
		super.clear();

		for (const [k, prev] of entries) {
			this.#invalidate(k);
			this.#emit({ op: "del", k, prev, has: false, pid: 0 });
		}
	}
	public dv(k: H, deps: A, fn: (...vals: AR<T>) => T): this {//              [DERRIVED]
		
		(this.#derived ??= new M2<H, Derived>())
    .set(k, {
        deps,
        fn,
        dirty: true,
        has: false,
    });

		for (const dep of deps) {
			(this.#dependents ??= new M2<H, Set<H>>())
    .getOrM(() => new Set(), dep)
    .add(k);
		}

		return this;
	}
	public dirty<T>(...keys: AR<H>): this {//                                            [DIRTY]
		
		for (const k of keys) {
			const d = this.#derived?.get(k);

			if (d && !d.dirty) {
				d.dirty = true;
				this.#emit({ op: "dirty", k, has: Boolean(d.has ?? false), pid: 0, prev: (k as any).prev }) as T;
			}
		}

		return this;
	}
	public g(k: H): T | UD {//                                                         [GET]
		
		const d = this.#derived?.get(k);

		if (d) {
			if (d.dirty || !d.has) return this.#computeDerived(k, d);
			return d.value;
		}

		return super.get(k);
	}
	public h(k: H): B {//                                                              [HAS]
		
		const d = this.#derived?.get(k);

		if (d) {
			if (d.dirty || !d.has) this.#computeDerived(k, d);
			return true;
		}

		return super.has(k);
	}
	public $debug(limit: N = 256): this {//                                          [$DEBUG]
		this.#hist ??= [];
		this.#histLimit = limit;
		return this;
	}
	public $strict(): this {//                                                      [$STRICT]
		this.#strict = true;
		return this;
	}
	public $history(): AR<M2Event<H, T>> {//                                       [$HISTORY]
		return this.#hist ? [...this.#hist] : [];
	}
	public $why(k: H): M2Event<H, T> | UD {//                                          [$WHY]
		if (!this.#hist) return undefined;

		for (let i = this.#hist.length - 1; i >= 0; i--) {
			if (Object.is(this.#hist[i].k, k)) return this.#hist[i];
		}

		return undefined;
	}
	public $diff(other: M2<H, T>) {//                                                [$DIFF]
		const add: AR<[H, T]> = [];
		const del: AR<H> = [];
		const chg: AR<[H, T, T]> = [];

		for (const [k, v] of this) {
			if (!other.has(k)) {
				add.push([k, v]);
			} else if (!Object.is(other.get(k), v)) {
				chg.push([k, v, other.get(k) as T]);
			}
		}

		for (const k of other.keys()) {
			if (!this.has(k)) del.push(k);
		}

		return { add, del, chg };
	}
	public *entries(): MapIterator<[H, T]> {//                                     [*ENTRIES]
		this.#iterating++;

		try {
			for (const entry of super.entries()) yield entry;
		} finally {
			this.#iterating--;
		}
	}
	public *keys(): MapIterator<H> {//                                                [*KEYS]
		this.#iterating++;

		try {
			for (const k of super.keys()) yield k;
		} finally {
			this.#iterating--;
		}
	}
	public *values(): MapIterator<T> {//                                            [*VALUES]
		this.#iterating++;

		try {
			for (const v of super.values()) yield v;
		} finally {
			this.#iterating--;
		}
	}
	public swap(value: UF<T>, key: H): T | UD {//                                      [SWAP]
		const next = rv(value);
		const previous = this.has(key) ? this.get(key) : next;
		this.s(next, key);
		return previous as T | UD;
	}
	public take(k: H): T | UD {//                                                      [TAKE]
		const value = this.g(k);
		this.d(k);
		return value;
	}
	public peek(k: H): T | false {//                                                   [PEEK]
		return this.h(k) ? (this.g(k) as T) : false;
	}
	public getOrI(value:UF<T>, k: H): T {//                                          [GETORI]
		if (this.h(k)) return this.g(k) as T;
		const next = rv(value);
		this.s(next, k);
		return next;
	}
	public getOrM(make: () => T, k: H): T {//                                        [GETORM]
		if (this.h(k)) return this.g(k) as T;
		const value = make();
		this.s(value, k);
		return value;
	}
	public update(k: H, fn: (old: T, has: B) => T, initial?: UF<T>): T {//           [UPDATE]
		const has = this.h(k);
	
		const old = has
			? (this.g(k) as T)
			: initial === undefined
				? undefined as T
				: rv(initial);
	
		const next = rv(fn(old, has));
	
		this.s(next, k);
		return next;
	}
	public setIfAbsent(value: UF<T>, k: H): B {//                               [SETIFABSENT]
		if (this.h(k)) return false;
		const next = rv(value);
		this.s(next, k);
		return true;
	}
	public setIfPresent(value: UF<T>, k: H): B {//                             [SETIFPRESENT]
		if (!this.h(k)) return false;
		const next = rv(value);
		this.s(next, k);
		return true;
	}
	public replace(value: UF<T>, k: H): B {//                                       [REPLACE]
		const next = rv(value);
		return this.setIfPresent(next, k);
	}
	public inc(key: H, by: N = 1): N {//                                                [INC]
		return this.update(key, (old) => (((old ?? 0) as N) + by) as unknown as T, 0 as unknown as T) as N;
	}
	public touch(k: H): B {//                                                     [SWTOUCHAP]
		return this.setIfAbsent(undefined as unknown as T, k);
	}
	public mapValues<G>(fn: (v: T, k: H) => G): M2<H, G> {//                      [MAPVALUES]
		const out = new M2<H, G>();
		for (const [k, v] of this) out.s(fn(v, k), k);
		return out;
	}
	public mapKeys<G>(fn: (v: T, k: H) => G): M2<G, T> {//                          [MAPKEYS]
		const out = new M2<G, T>();
		for (const [k, v] of this) out.s(v, fn(v, k));
		return out;
	}
	public filterKV(pred: (v: T, k: H) => B): this {//                             [FILTERKV]
		for (const [k, v] of this) {
			if (!pred(v, k)) this.d(k);
		}
		return this;
	}
	public filterKeys(pred: (v: T, k: H) => B): this {//                         [FILTERKEYS]
		return this.filterKV(pred);
	}
	public partition(pred: (v: T, k: H) => B): [M2<H, T>, M2<H, T>] {//           [PARTITION]
		const yes = new M2<H, T>();
		const no = new M2<H, T>();

		for (const [k, v] of this) {
			(pred(v, k) ? yes : no).s(v, k);
		}

		return [yes, no];
	}
	public find(pred: (v: T, k: H) => B): [H, T] | UD {//                              [FIND]
		for (const [k, v] of this) {
			if (pred(v, k)) return [k, v];
		}
		return undefined;
	}
	public findKey(pred: (v: T, k: H) => B): H | UD {//                             [FINDKEY]
		for (const [k, v] of this) {
			if (pred(v, k)) return k;
		}
		return undefined;
	}
	public findValue(pred: (v: T, k: H) => B): T | UD {//                         [FINDVALUE]
		for (const [k, v] of this) {
			if (pred(v, k)) return v;
		}
		return undefined;
	}
	public some(pred: (v: T, k: H) => B): B {//                                        [SOME]
		for (const [k, v] of this) {
			if (pred(v, k)) return true;
		}
		return false;
	}
	public every(pred: (v: T, k: H) => B): B {//                                      [EVERY]
		for (const [k, v] of this) {
			if (!pred(v, k)) return false;
		}
		return true;
	}
	public reduce<G>(fn: (acc: G, v: T, k: H) => G, initial: G): G {//               [REDUCE]
		let acc = initial;
		for (const [k, v] of this) acc = fn(acc, v, k);
		return acc;
	}
	public forEachKV(fn: (v: T, k: H, map: this) => void): void {//                 [FOREACH]
		for (const [k, v] of this) fn(v, k, this);
	}
	public invert(): M2<T, H> {//                                                    [INVERT]
		const out = new M2<T, H>();
		for (const [k, v] of this) out.s(k, v);
		return out;
	}
	public unique<G>(fn?: (v: T, k: H) => G): M2<G, T> {//                           [UNIQUE]
		const out = new M2<G, T>();
		for (const [k, v] of this) {
			const key = (fn ? fn(v, k) : v) as unknown as G;
			if (!out.h(key)) out.s(v, key);
		}
		return out;
	}
	public pick(keys: Iterable<H>): M2<H, T> {//                                       [PICK]
		const out = new M2<H, T>();
		for (const k of keys) {
			if (this.h(k)) out.s(this.g(k) as T, k);
		}
		return out;
	}
	public omit(keys: Iterable<H>): M2<H, T> {//                                       [EMIT]
		const omitted = new Set(keys);
		const out = new M2<H, T>();
		for (const [k, v] of this) {
			if (!omitted.has(k)) out.s(v, k);
		}
		return out;
	}
	public merge(other: Iterable<readonly [H, T]>): this {//                          [MERGE]
		for (const [k, v] of other){
		const n = rv(v);
			 this.s(n, k);
			}
		return this;
	}
	public clearAndSet(entries: Iterable<readonly [H, T]>): this {//            [CLEARANDSET]
		return this.batch(() => {
			this.clear();

			for (const [k, v] of entries) {
		const n = rv(v);
				this.s(n, k);
			}
		});
	}
	public toObject(): OB<T> {//                                                  [TOPOBJECT]
		const out = {} as OB<T>;
		for (const [k, v] of this) out[k as S] = v;
		return out;
	}
	public toValues(): A2I<T> { return A2.from(this.values()); }//                 [TOVALUES]
	public toKeys(): A2I<H> { return A2.from(this.keys()); }//                       [TOKEYS]
	public toArray(): A2I<[H, T]> { return A2.from(this.entries()); }//             [TOARRAY]
	public group<G>(fn: (v: T, k: H) => G): M2<G, A2<T>> {//                          [GROUP]
		const out = new M2<G, A2<T>>();
		for (const [k, v] of this) out.getOrM(() => new A2<T>(), fn(v, k)).push(v);
		return out;
	}
	public clone(): this {//                                                          [CLONE]
		return new M2(this) as this;
	}
	public equals(other: M2<H, T>): B {//                                           [EQUALS]
		if (other === this) return true;
		if (this.size !== other.size) return false;

		for (const [k, v] of this) {
			if (!other.has(k) || !sameValue(other.get(k), v)) return false;
		}

		return true;
	}
	static fromObject<X extends Record<S, A>>(obj: X): M2<S, X[keyof X]> {//      [FROMOBBJ]
		return new M2(Object.entries(obj) as AR<[S, X[keyof X]]>);
	}
	static fromEntries<H, T>(entries: I<readonly [H, T]>): M2<H, T> {//        [FROMENTIRES]
		return new M2(entries);
	}
	static from<H, T>(map: RM<H, T>): M2<H, T> {//                                    [FROM]
		return new M2(map);
	}
}
export class M2B {
	readonly cap: N;
	readonly stride: N;

	readonly pidToSlot: I32;
	readonly slotToPid: I32;
	readonly vals: F64;

	#size = 0;
	#subs?: Set<Sub>;
	#hist?: AR<M2BEvent>;
	#histLimit = 0;
	#strict = false;
	#iterating = 0;
	#batching = 0;
	#pending?: Map<N, M2BEvent>;
	#derived?: Map<N, Derived>;
	#dependents?: Map<N, Set<N>>;

	constructor(maxParticles: N= 65_536, stride: N = 1, seed?: I<readonly [N, V128]>) {
		this.cap = maxParticles;
		this.stride = stride;
		this.pidToSlot = new Int32Array(maxParticles).fill(-1);
		this.slotToPid = new Int32Array(maxParticles).fill(-1);
		this.vals = new Float64Array(maxParticles * stride);
		if (seed) for (const [pid, v] of seed) this.s(v, pid);
	}

	get size(): N { return this.#size; }

	// ---------------------------------------------------------------- internals
	#emit(e: M2BEvent): V {
		if (this.#batching > 0) {
			this.#pending ??= new Map();
			const pending = this.#pending.get(e.pid);
			this.#pending.set(e.pid, pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true });
			return;
		}
		this.#record(e);
		this.#subs?.forEach((sub) => { try { sub(e); } catch (err: A) { process.emitWarning(err); } });
	}
	#record(e: M2BEvent): V {
		if (!this.#hist) return;
		this.#hist.push(e);
		if (this.#hist.length > this.#histLimit) this.#hist.splice(0, this.#hist.length - this.#histLimit);
	}
	#invalidate(pid: N): V {
		const deps = this.#dependents?.get(pid);
		if (!deps) return;
		for (const dk of deps) {
			const d = this.#derived?.get(dk);
			if (!d || d.dirty) continue;
			d.dirty = true;
			this.#emit({ op: "dirty", pid: dk, has: d.has });
		}
	}
	#computeDerived(pid: N, d: Derived): V128 {
		const vals = d.deps.map((dep) => this.g(dep) as V128);
		const next = d.fn(...vals);
		const had = d.has;
		const prev = d.value;
		d.value = next; d.has = true; d.dirty = false;
		if (!had || !sameValue128(prev, next)) {
			this.#emit({ op: had ? "upd" : "set", pid, v: next, prev, has: true });
		}
		return next;
	}
	#assertNotIterating(op: S): V {
		if (this.#strict && this.#iterating > 0) {
			throw new Error(`M2B.${op}() during iteration. Snapshot keys first, or disable $strict().`);
		}
	}
	#readSlot(slot: N): V128 {
		return this.stride === 1 ? this.vals[slot] : this.vals.subarray(slot * this.stride, slot * this.stride + this.stride);
	}
	#writeSlot(slot: N, v: V128): V {
		if (this.stride === 1) this.vals[slot] = v as N;
		else this.vals.set(v as F64, slot * this.stride);
	}
	#fresh(): M2B { return new M2B(this.cap, this.stride); } //                     [sibling-store factory]

	// ---------------------------------------------------------------- subs / batch
	public on(fn: Sub): () => V { (this.#subs ??= new Set()).add(fn); return () => this.off(fn); } //      [ON]
	public off(fn: Sub): V { this.#subs?.delete(fn); } //                                                  [OFF]
	public batch(fn: () => V): this { //                                                                 [BATCH]
		this.#batching++;
		this.#pending ??= new Map();
		try { fn(); }
		finally {
			this.#batching--;
			if (this.#batching === 0) {
				const events = [...this.#pending.values()];
				this.#pending = undefined;
				for (const e of events) { this.#record(e); this.#subs?.forEach(sub => sub(e)); }
			}
		}
		return this;
	}

	// ---------------------------------------------------------------- core CRUD
	public s(v: V128, pid: N): this { //                                                                    [SET]
		this.#assertNotIterating("s");
		let slot = this.pidToSlot[pid];
		const has = slot !== -1;
		const prev = has ? this.#readSlot(slot) : undefined;
		if (!has) {
			slot = this.#size;
			this.pidToSlot[pid] = slot;
			this.slotToPid[slot] = pid;
			this.#size++;
		}
		this.#writeSlot(slot, v);
		this.#invalidate(pid);
		this.#emit({ op: has ? "upd" : "set", pid, v, prev, has });
		return this;
	}
	public mutate(pid: N, fn: (old: V128, has: true) => V128): V128 | UD { //                            [MUTATE]
		this.#assertNotIterating("mutate");
		if (!this.h(pid)) return undefined;
		const next = fn(this.g(pid) as V128, true);
		this.s(next, pid);
		return next;
	}
	public d(pid: N): B { //                                                                             [DELETE]
		this.#assertNotIterating("delete");
		const slot = this.pidToSlot[pid];
		if (slot === -1) return false;
		const prev = this.#readSlot(slot);
		const lastSlot = this.#size - 1;
		const lastPid = this.slotToPid[lastSlot];
		if (slot !== lastSlot) {
			this.#writeSlot(slot, this.#readSlot(lastSlot));
			this.slotToPid[slot] = lastPid;
			this.pidToSlot[lastPid] = slot;
		}
		this.pidToSlot[pid] = -1;
		this.slotToPid[lastSlot] = -1;
		this.#size--;
		this.#invalidate(pid);
		this.#emit({ op: "del", pid, prev, has: false });
		return true;
	}
	public c(): V { //                                                                                     [CLEAR]
		this.#assertNotIterating("clear");
		for (let i = this.#size - 1; i >= 0; i--) {
			const pid = this.slotToPid[i];
			const prev = this.#readSlot(i);
			this.pidToSlot[pid] = -1;
			this.slotToPid[i] = -1;
			this.#invalidate(pid);
			this.#emit({ op: "del", pid, prev, has: false });
		}
		this.#size = 0;
	}
	public dv(pid: N, deps: AR<N>, fn: (...vals: AR<V128>) => V128): this { //                          [DERIVED]
		(this.#derived ??= new Map()).set(pid, { deps, fn, dirty: true, has: false });
		for (const dep of deps) {
			const set = (this.#dependents ??= new Map());
			if (!set.has(dep)) set.set(dep, new Set());
			set.get(dep)!.add(pid);
		}
		return this;
	}
	public dirty(...pids: AR<N>): this { //                                                                [DIRTY]
		for (const pid of pids) {
			const d = this.#derived?.get(pid);
			if (d && !d.dirty) { d.dirty = true; this.#emit({ op: "dirty", pid, has: d.has }); }
		}
		return this;
	}
	public g(pid: N): V128 | UD { //                                                                         [GET]
		const d = this.#derived?.get(pid);
		if (d) return d.dirty || !d.has ? this.#computeDerived(pid, d) : d.value!;
		const slot = this.pidToSlot[pid];
		return slot === -1 ? undefined : this.#readSlot(slot);
	}
	public h(pid: N): B { //                                                                                [HAS]
		const d = this.#derived?.get(pid);
		if (d) { if (d.dirty || !d.has) this.#computeDerived(pid, d); return true; }
		return this.pidToSlot[pid] !== -1;
	}

	// ---------------------------------------------------------------- debug
	public $debug(limit: N = 256): this { this.#hist ??= []; this.#histLimit = limit; return this; } //  [$DEBUG]
	public $strict(): this { this.#strict = true; return this; } //                                     [$STRICT]
	public $history(): AR<M2BEvent> { return this.#hist ? [...this.#hist] : []; } //                    [$HISTORY]
	public $why(pid: N): M2BEvent | UD { //                                                                [$WHY]
		if (!this.#hist) return undefined;
		for (let i = this.#hist.length - 1; i >= 0; i--) if (this.#hist[i].pid === pid) return this.#hist[i];
		return undefined;
	}
	public $diff(other: M2B) { //                                                                        [$DIFF]
		const add: AR<[N, V128]> = [];
		const del: AR<N> = [];
		const chg: AR<[N, V128, V128]> = [];
		for (const [pid, v] of this) {
			if (!other.has(pid)) add.push([pid, v]);
			else if (!sameValue128(other.get(pid), v)) chg.push([pid, v, other.get(pid)!]);
		}
		for (const pid of other.keys()) if (!this.has(pid)) del.push(pid);
		return { add, del, chg };
	}

	// ---------------------------------------------------------------- iteration (dense slots, no hashing)
	public *entries(): Generator<[N, V128]> { //                                                       [*ENTRIES]
		this.#iterating++;
		try { for (let i = 0; i < this.#size; i++) yield [this.slotToPid[i], this.#readSlot(i)]; }
		finally { this.#iterating--; }
	}
	public *keys(): Generator<N> { //                                                                      [*KEYS]
		this.#iterating++;
		try { for (let i = 0; i < this.#size; i++) yield this.slotToPid[i]; }
		finally { this.#iterating--; }
	}
	public *values(): Generator<V128> { //                                                              [*VALUES]
		this.#iterating++;
		try { for (let i = 0; i < this.#size; i++) yield this.#readSlot(i); }
		finally { this.#iterating--; }
	}
	public [Symbol.iterator](): Generator<[N, V128]> { return this.entries(); }

	// ---------------------------------------------------------------- alias / has/has-not helpers
	public has = this.h; //                                                                             [has alias]
	public get = this.g; //                                                                             [get alias]
	public delete = this.d; //                                                                        [delete alias]
	public clear = this.c; //                                                                           [clear alias]
	public f = this.forEach.bind(this); //                                                            [FOR EACH]
	public forEach(fn: (v: V128, pid: N, map: this) => V): V { for (const [pid, v] of this) fn(v, pid, this); }

	// ---------------------------------------------------------------- sugar
	public swap(value: V128, pid: N): V128 | UD { //                                                       [SWAP]
		const previous = this.has(pid) ? this.get(pid) : value;
		this.s(value, pid);
		return previous;
	}
	public take(pid: N): V128 | UD { const value = this.g(pid); this.d(pid); return value; } //             [TAKE]
	public peek(pid: N): V128 | false { return this.h(pid) ? (this.g(pid) as V128) : false; } //            [PEEK]
	public getOrI(value: V128, pid: N): V128 { //                                                        [GETORI]
		if (this.h(pid)) return this.g(pid) as V128;
		this.s(value, pid);
		return value;
	}
	public getOrM(make: () => V128, pid: N): V128 { //                                                   [GETORM]
		if (this.h(pid)) return this.g(pid) as V128;
		const value = make();
		this.s(value, pid);
		return value;
	}
	public update(pid: N, fn: (old: V128, has: B) => V128, initial?: V128): V128 { //                    [UPDATE]
		const has = this.h(pid);
		const old = has ? (this.g(pid) as V128) : (initial === undefined ? (0 as V128) : initial);
		const next = fn(old, has);
		this.s(next, pid);
		return next;
	}
	public setIfAbsent(value: V128, pid: N): B { //                                                [SETIFABSENT]
		if (this.h(pid)) return false;
		this.s(value, pid);
		return true;
	}
	public setIfPresent(value: V128, pid: N): B { //                                              [SETIFPRESENT]
		if (!this.h(pid)) return false;
		this.s(value, pid);
		return true;
	}
	public replace(value: V128, pid: N): B { return this.setIfPresent(value, pid); } //                 [REPLACE]
	public inc(pid: N, by: N = 1): N { //                                                                   [INC]
		return this.update(pid, (old) => (((old as N) ?? 0) + by) as V128, 0) as N;
	}
	public touch(pid: N): B { return this.setIfAbsent(0 as V128, pid); } //                            [SWTOUCHAP]

	// ---------------------------------------------------------------- transforms (produce new M2B, same cap/stride)
	public mapValues(fn: (v: V128, pid: N) => V128): M2B { //                                         [MAPVALUES]
		const out = this.#fresh();
		for (const [pid, v] of this) out.s(fn(v, pid), pid);
		return out;
	}
	public mapKeys(fn: (v: V128, pid: N) => N): M2B { //                                                [MAPKEYS]
		const out = this.#fresh(); // target pids must still fit within cap
		for (const [pid, v] of this) out.s(v, fn(v, pid));
		return out;
	}
	public filterKV(pred: (v: V128, pid: N) => B): this { //                                           [FILTERKV]
		for (const pid of [...this.keys()]) if (!pred(this.g(pid) as V128, pid)) this.d(pid);
		return this;
	}
	public filterKeys(pred: (v: V128, pid: N) => B): this { return this.filterKV(pred); } //          [FILTERKEYS]
	public partition(pred: (v: V128, pid: N) => B): [M2B, M2B] { //                                   [PARTITION]
		const yes = this.#fresh(), no = this.#fresh();
		for (const [pid, v] of this) (pred(v, pid) ? yes : no).s(v, pid);
		return [yes, no];
	}
	public find(pred: (v: V128, pid: N) => B): [N, V128] | UD { //                                          [FIND]
		for (const [pid, v] of this) if (pred(v, pid)) return [pid, v];
		return undefined;
	}
	public findKey(pred: (v: V128, pid: N) => B): N | UD { //                                           [FINDKEY]
		for (const [pid, v] of this) if (pred(v, pid)) return pid;
		return undefined;
	}
	public findValue(pred: (v: V128, pid: N) => B): V128 | UD { //                                    [FINDVALUE]
		for (const [pid, v] of this) if (pred(v, pid)) return v;
		return undefined;
	}
	public some(pred: (v: V128, pid: N) => B): B { //                                                       [SOME]
		for (const [pid, v] of this) if (pred(v, pid)) return true;
		return false;
	}
	public every(pred: (v: V128, pid: N) => B): B { //                                                     [EVERY]
		for (const [pid, v] of this) if (!pred(v, pid)) return false;
		return true;
	}
	public reduce<G>(fn: (acc: G, v: V128, pid: N) => G, initial: G): G { //                              [REDUCE]
		let acc = initial;
		for (const [pid, v] of this) acc = fn(acc, v, pid);
		return acc;
	}
	public forEachKV(fn: (v: V128, pid: N, map: this) => V): V { for (const [pid, v] of this) fn(v, pid, this); } // [FOREACH]
	public invert(): M2<V128, N> { //                                                                     [INVERT]
		const out = new Map<S, N>(); // values aren't valid pids/Int32 slots in general -> plain Map, not M2B
		for (const [pid, v] of this) out.set(this.stride === 1 ? String(v) : Array.from(v as F64).join(","), pid);
		return out as A;
	}
	public unique(fn?: (v: V128, pid: N) => N): M2B { //                                                   [UNIQUE]
		const out = this.#fresh();
		const seen = new Set<S>();
		for (const [pid, v] of this) {
			const keyFor = fn ? fn(v, pid) : pid;
			const dedupeKey = this.stride === 1 ? String(v) : Array.from(v as F64).join(",");
			if (!seen.has(dedupeKey)) { seen.add(dedupeKey); out.s(v, keyFor); }
		}
		return out;
	}
	public pick(pids: Iterable<N>): M2B { //                                                                [PICK]
		const out = this.#fresh();
		for (const pid of pids) if (this.h(pid)) out.s(this.g(pid) as V128, pid);
		return out;
	}
	public omit(pids: Iterable<N>): M2B { //                                                                [OMIT]
		const omitted = new Set(pids);
		const out = this.#fresh();
		for (const [pid, v] of this) if (!omitted.has(pid)) out.s(v, pid);
		return out;
	}
	public merge(other: Iterable<readonly [N, V128]>): this { //                                           [MERGE]
		for (const [pid, v] of other) this.s(v, pid);
		return this;
	}
	public clearAndSet(entries: Iterable<readonly [N, V128]>): this { //                            [CLEARANDSET]
		return this.batch(() => { this.clear(); for (const [pid, v] of entries) this.s(v, pid); });
	}
	public toObject(): OB<V128> { //                                                                  [TOPOBJECT]
		const out = {} as OB<V128>;
		for (const [pid, v] of this) out[pid as unknown as S] = v;
		return out;
	}
	public toValues(): AR<V128> { return [...this.values()]; } //                                       [TOVALUES]
	public toKeys(): AR<N> { return [...this.keys()]; } //                                                [TOKEYS]
	public toArray(): AR<[N, V128]> { return [...this.entries()]; } //                                   [TOARRAY]
	public group<G>(fn: (v: V128, pid: N) => G): Map<G, AR<V128>> { //                                     [GROUP]
		const out = new Map<G, AR<V128>>();
		for (const [pid, v] of this) {
			const k = fn(v, pid);
			if (!out.has(k)) out.set(k, []);
			out.get(k)!.push(v);
		}
		return out;
	}
	public clone(): this { //                                                                              [CLONE]
		return new M2B(this.cap, this.stride, this.entries()) as this;
	}
	public equals(other: M2B): B { //                                                                     [EQUALS]
		if (other === this) return true;
		if (this.size !== other.size) return false;
		for (const [pid, v] of this) if (!other.has(pid) || !sameValue128(other.get(pid), v)) return false;
		return true;
	}

	// ---------------------------------------------------------------- statics
	static fromObject(obj: Record<N, V128>, maxParticles: N, stride: N = 1): M2B { //                  [FROMOBBJ]
		return new M2B(maxParticles, stride, Object.entries(obj).map(([k, v]) => [Number(k), v] as [N, V128]));
	}
	static fromEntries(entries: I<readonly [N, V128]>, maxParticles: N, stride: N = 1): M2B { //    [FROMENTIRES]
		return new M2B(maxParticles, stride, entries);
	}
	static from(map: RM<N, V128>, maxParticles: N, stride: N = 1): M2B { //                                [FROM]
		return new M2B(maxParticles, stride, map as A);
	}
}

function benchmarkStore(name: string, store: any, iterations: number = 10_000_000) {
    // Warmup
    if (name === 'M2B' || name ==='M2') {
        for (let i = 0; i < 1000; i++) {
            store.s(i * 1.5, i % 4096);
            store.g(i % 4096);
        }
    } else {
        for (let i = 0; i < 1000; i++) {
            store.set(i * 1.5, i % 4096);
            store.get(i % 4096);
        }
    }
   
    if (typeof store.clear === "function") store.clear();

    const durationMs = 10000; // Exactly 10 seconds target or fixed loop
    let ops = 0;
    
    const start = performance.now();
    const endTarget = start + durationMs;

    // Run for 10 seconds straight doing set and get over and over
    while (performance.now() < endTarget) {
        if (name === 'M2B' || name ==='M2') {
            for (let pid = 0; pid < 1024; pid++) {
                store.s(pid * 3.14, pid);
                store.g(pid);
                ops += 2; // one set, one get
            }
        } else {
            for (let pid = 0; pid < 1024; pid++) {
                store.set(pid * 3.14, pid);
                store.get(pid);
                ops += 2; // one set, one get
            }
        }
    }

    const elapsed = (performance.now() - start) / 1000;
    const opsPerSec = Math.round(ops / elapsed);

    console.log(`[${name}] ${ops.toLocaleString()} ops in ${elapsed.toFixed(2)}s -> ${(opsPerSec).toLocaleString()} ops/sec`);
    return opsPerSec;
}

export function runBenchmarks() {
    console.log("--- Starting 10-Second Binary Store Benchmarks (Set & Get) ---");
    
    const BMB3 = new BYTEOP_MAP(1024)
    benchmarkStore("BYTEOP_MAP LG vs)", BMB3);
    const m = new Map()
    benchmarkStore("Map", m);
    const m6 = getNestedMap6('1','1','1')
    benchmarkStore("getNestedMap6 vs)", m6);
    const m2b = new M2B(65_536)
    benchmarkStore("M2B", m2b);
    const m2 = new M2()
    benchmarkStore("M2", m2);
    const byteM = new BYTE_MAP21()
    benchmarkStore("BYTE_MAP small vs)", byteM);
    // 2. M2Bin (Map subclass with flat Int32/Float64 lanes)
    const m2bin = new M2Bin(16384);
    benchmarkStore("M2Bin (Hybrid Map + Int32/F64)", m2bin);
    // 3. M2Dense (Direct pid-indexed F64 array with live byte markers)
    const m2dense = new M2Dense(16384);
    benchmarkStore("M2Dense (Dense Array + Live Byte Lane)", m2dense);
    // 4. M2Rows (Strided multi-component row store)
    const m2rows = new M2Rows(16384, 1);
    benchmarkStore("M2Rows (Strided Row Buffer, stride=1)", m2rows);
    console.log("--- Benchmarks Complete ---");
}

// Uncomment to run directly if executed as a script
runBenchmarks();
