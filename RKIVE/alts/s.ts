import { performance } from "node:perf_hooks";
// ============================================================================
// 1. Original 3-Tier Nested JS Map
// ============================================================================

const LEAF_CAP = 1024;
class DummyLeaf {
    constructor(cap = LEAF_CAP) {}
    s(v: number, pid: number) {}
    g(pid: number) { return pid; }
}
function getOriginalNestedMap(s = SAURON_SLAVE_REG, m = SAURON_MASTER_REG, r = THE_WIZARD_ROOT_REG) {
    let root = (globalThis as any)[r];
    if (!root) {
        root = new Map<string, Map<string, DummyLeaf>>();
        (globalThis as any)[r] = root;
    }
    let masterMap = root.get(m);
    if (!masterMap) {
        masterMap = new Map<string, DummyLeaf>();
        root.set(m, masterMap);
    }
    let slaveMap = masterMap.get(s);
    if (!slaveMap) {
        slaveMap = new DummyLeaf(LEAF_CAP);
        masterMap.set(s, slaveMap);
    }
    return slaveMap;
}
export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
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
export class M2BORIGINAL {
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
export class M2B {
    readonly cap: N;
    readonly stride: N;

    readonly pidToSlot: I32;
    readonly slotToPid: I32;
    readonly vals: Float64Array;

    readonly #objects: unknown[];   // non-numeric values (leaves, etc.)
    readonly #present: Uint8Array;  // 1 if pid has any value (numeric or object)
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

    constructor(maxParticles: N = 65_536, stride: N = 1, seed?: I<readonly [N, V128]>) {
        this.cap = maxParticles;
        this.stride = stride;
        this.pidToSlot = new Int32Array(maxParticles).fill(-1);
        this.slotToPid = new Int32Array(maxParticles).fill(-1);
        this.vals = new Float64Array(maxParticles * stride);
        this.#objects = new Array(maxParticles);
        this.#present = new Uint8Array(maxParticles);
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
        if (!this.#present[this.slotToPid[slot]]) return undefined as any;
        // if stride === 1 and the slot holds a scalar we stored in `vals`, return scalar
        // but if the slot holds an object, return the object
        const pid = this.slotToPid[slot];
        const obj = this.#objects[pid];
        if (obj !== undefined) return obj as any;
        return this.stride === 1 ? this.vals[slot] : this.vals.subarray(slot * this.stride, slot * this.stride + this.stride);
    }
    #writeSlot(slot: N, v: V128): V {
        const pid = this.slotToPid[slot];
        if (typeof v === "number" && this.stride === 1) {
            this.vals[slot] = v;
            this.#objects[pid] = undefined;
        } else if (v instanceof Float64Array) {
            this.vals.set(v, slot * this.stride);
            this.#objects[pid] = undefined;
        } else {
            // non-numeric object (M2B, Map, etc.) — store in object lane
            this.#objects[pid] = v;
        }
    }

    // ---------------------------------------------------------------- subs / batch
    public on(fn: Sub): () => V { (this.#subs ??= new Set()).add(fn); return () => this.off(fn); }
    public off(fn: Sub): V { this.#subs?.delete(fn); }
    public batch(fn: () => V): this {
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
    public s(v: V128, pid: N): this {
        this.#assertNotIterating("s");
        let slot = this.pidToSlot[pid];
        const has = slot !== -1;
        const prev = has ? this.#readSlot(slot) : undefined;
        if (!has) {
            slot = this.#size;
            this.pidToSlot[pid] = slot;
            this.slotToPid[slot] = pid;
            this.#size++;
            this.#present[pid] = 1;
        }
        this.#writeSlot(slot, v);
        this.#invalidate(pid);
        this.#emit({ op: has ? "upd" : "set", pid, v, prev, has });
        return this;
    }
    public mutate(pid: N, fn: (old: V128, has: true) => V128): V128 | UD {
        this.#assertNotIterating("mutate");
        if (!this.h(pid)) return undefined;
        const next = fn(this.g(pid) as V128, true);
        this.s(next, pid);
        return next;
    }
    public d(pid: N): B {
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
        this.#objects[pid] = undefined;
        this.#present[pid] = 0;
        this.#size--;
        this.#invalidate(pid);
        this.#emit({ op: "del", pid, prev, has: false });
        return true;
    }
    public c(): V {
        this.#assertNotIterating("clear");
        for (let i = this.#size - 1; i >= 0; i--) {
            const pid = this.slotToPid[i];
            const prev = this.#readSlot(i);
            this.pidToSlot[pid] = -1;
            this.slotToPid[i] = -1;
            this.#objects[pid] = undefined;
            this.#present[pid] = 0;
            this.#invalidate(pid);
            this.#emit({ op: "del", pid, prev, has: false });
        }
        this.#size = 0;
    }
    public dv(pid: N, deps: AR<N>, fn: (...vals: AR<V128>) => V128): this {
        (this.#derived ??= new Map()).set(pid, { deps, fn, dirty: true, has: false });
        for (const dep of deps) {
            const set = (this.#dependents ??= new Map());
            if (!set.has(dep)) set.set(dep, new Set());
            set.get(dep)!.add(pid);
        }
        return this;
    }
    public dirty(...pids: AR<N>): this {
        for (const pid of pids) {
            const d = this.#derived?.get(pid);
            if (d && !d.dirty) { d.dirty = true; this.#emit({ op: "dirty", pid, has: d.has }); }
        }
        return this;
    }
    public g(pid: N): V128 | UD {
        // router path: no #derived check, straight array/object read
        if (!this.#present[pid]) return undefined;
        const obj = this.#objects[pid];
        if (obj !== undefined) return obj as any;
        const slot = this.pidToSlot[pid];
        if (slot === -1) return undefined;
        return this.stride === 1 ? this.vals[slot] : this.vals.subarray(slot * this.stride, slot * this.stride + this.stride);
    }
    public h(pid: N): B {
        return this.#present[pid] === 1;
    }

    // ---------------------------------------------------------------- debug
    public $debug(limit: N = 256): this { this.#hist ??= []; this.#histLimit = limit; return this; }
    public $strict(): this { this.#strict = true; return this; }
    public $history(): AR<M2BEvent> { return this.#hist ? [...this.#hist] : []; }
    public $why(pid: N): M2BEvent | UD {
        if (!this.#hist) return undefined;
        for (let i = this.#hist.length - 1; i >= 0; i--) if (this.#hist[i].pid === pid) return this.#hist[i];
        return undefined;
    }
    public $diff(other: M2B) {
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

    // ---------------------------------------------------------------- iteration
    public *entries(): Generator<[N, V128]> {
        this.#iterating++;
        try { for (let i = 0; i < this.#size; i++) yield [this.slotToPid[i], this.#readSlot(i)]; }
        finally { this.#iterating--; }
    }
    public *keys(): Generator<N> {
        this.#iterating++;
        try { for (let i = 0; i < this.#size; i++) yield this.slotToPid[i]; }
        finally { this.#iterating--; }
    }
    public *values(): Generator<V128> {
        this.#iterating++;
        try { for (let i = 0; i < this.#size; i++) yield this.#readSlot(i); }
        finally { this.#iterating--; }
    }
    public [Symbol.iterator](): Generator<[N, V128]> { return this.entries(); }

    // ---------------------------------------------------------------- aliases
    public has = this.h;
    public get = this.g;
    public delete = this.d;
    public clear = this.c;
    public f = this.forEach.bind(this);
    public forEach(fn: (v: V128, pid: N, map: this) => V): V { for (const [pid, v] of this) fn(v, pid, this); }

    // ---------------------------------------------------------------- sugar
    public swap(value: V128, pid: N): V128 | UD {
        const previous = this.has(pid) ? this.get(pid) : value;
        this.s(value, pid);
        return previous;
    }
    public take(pid: N): V128 | UD { const value = this.g(pid); this.d(pid); return value; }
    public peek(pid: N): V128 | false { return this.h(pid) ? (this.g(pid) as V128) : false; }
    public getOrI(value: V128, pid: N): V128 {
        if (this.h(pid)) return this.g(pid) as V128;
        this.s(value, pid);
        return value;
    }
    public getOrM(make: () => V128, pid: N): V128 {
        if (this.h(pid)) return this.g(pid) as V128;
        const value = make();
        this.s(value, pid);
        return value;
    }
    public update(pid: N, fn: (old: V128, has: B) => V128, initial?: V128): V128 {
        const has = this.h(pid);
        const old = has ? (this.g(pid) as V128) : (initial === undefined ? (0 as V128) : initial);
        const next = fn(old, has);
        this.s(next, pid);
        return next;
    }
    public setIfAbsent(value: V128, pid: N): B {
        if (this.h(pid)) return false;
        this.s(value, pid);
        return true;
    }
    public setIfPresent(value: V128, pid: N): B {
        if (!this.h(pid)) return false;
        this.s(value, pid);
        return true;
    }
    public replace(value: V128, pid: N): B { return this.setIfPresent(value, pid); }
    public inc(pid: N, by: N = 1): N {
        return this.update(pid, (old) => (((old as N) ?? 0) + by) as V128, 0) as N;
    }
    public touch(pid: N): B { return this.setIfAbsent(0 as V128, pid); }
}
const THE_WIZARD_ROOT_REG = 1;
const SAURON_MASTER_REG = 1;
const SAURON_SLAVE_REG = 1;
const m2b_cache = new M2B(16)

function getNestedM2B(s = SAURON_SLAVE_REG, m = SAURON_MASTER_REG, r = THE_WIZARD_ROOT_REG) {
    let root = m2b_cache.g(r);
    if (!root) {
        root = new M2B(16);
        m2b_cache.s(r, root)
    }
    let masterMap = root.g(m);
    if (!masterMap) {
        masterMap = new M2B(16);
        root.s(m, masterMap);
    }
    let slaveMap = masterMap.g(s);
    if (!slaveMap) {
        slaveMap = new M2B(16);
        masterMap.s(s, slaveMap);
    }
    return slaveMap;
}







// ============================================================================
// 2. Object-based Class Nested Map (Pre-allocated 3D flat Array)
// ============================================================================
const MAX  = 65_536
const REG = new M2<N, A>();
const k3 = (r: N, m:N, s: N) => (Number(r) << 20) ^ (Number(m) << 10) ^ Number(s); // or bigint if ids exceed bit budget
function getNestedMap2(s = 1, m =1, r =1) {
    const key = k3(r, m, s);

    let store = REG.get(key);

    if (!store) {
        store = new M2B(MAX);
        REG.set(key, store);
    }

    return store;
}
class M2BNestedClass {
    readonly #slaves: any[];
    readonly #rootStride: number;
    readonly #masterStride: number;
    constructor(maxSlave = 256, maxMaster = 32, maxRoot = 8) {
        this.#rootStride = maxMaster * maxSlave;
        this.#masterStride = maxSlave;
        this.#slaves = new Array(maxRoot * maxMaster * maxSlave);
    }
    getNestedMap(s = 0, m = 0, r = 0) {
        const i = r * this.#rootStride + m * this.#masterStride + s;
        let store = this.#slaves[i];
        if (store === undefined) {
            store = new DummyLeaf(LEAF_CAP);
            this.#slaves[i] = store;
        }
        return store;
    }
    has(s: N, m: N, r: N): B {  return this.#slaves[this.#index(s, m, r)] !== undefined;  }
    delete(s: N, m: N, r: N): B {
        const i = this.#index(s, m, r);
        if (this.#slaves[i] === undefined) return false;
        this.#slaves[i] = undefined;
        return true;
    }
    clear(): V { this.#slaves.fill(undefined);  }
    #index(s: N, m: N, r: N): N {  return r * this.#rootStride + m * this.#masterStride + s; }
    // getNetedMap = (s: N = SAURON_SLAVE_REG, m: N = SAURON_MASTER_REG, r: N = THE_WIZARD_ROOT_REG): M2B | UD => { return m2bNested.getNestedMap(s, m, r);    };
}
const nestedClassInstance = new M2BNestedClass(256, 64, 8);
// ============================================================================
// 3. Interned Linear ID Flat Registry (String Interning)
// ============================================================================
const ROOT_STRIDE = 8;
const MASTER_STRIDE = 64;
const SLAVE_STRIDE = 256;
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
type Leaf<K, V> = A;
const REGISTRY: any[] = new Array(TOTAL_SLOTS);
type LeafFactory = (rootId: N, masterId: N, slaveId: N) => Leaf<A, A>; // optional per-root factory: default = M2B with sensible cap/stride
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
function getNestedMapInterned(s = "comp_space", m = "workspace", r = "sauron") {
    const rid = idFor(internRoot, r, () => rootCursor++);
    const mid = idFor(internMaster, m, () => masterCursor++);
    const sid = idFor(internSlave, s, () => slaveCursor++);
    const idx = (rid * MASTER_STRIDE + mid) * SLAVE_STRIDE + sid;
    let leaf = REGISTRY[idx];
    if (leaf === undefined) {
        leaf = new DummyLeaf(LEAF_CAP);
        REGISTRY[idx] = leaf;
    }
    return leaf;
}
const BINARY_REGISTRY: any[] = new Array(TOTAL_SLOTS);
let memoRid = -1, memoMid = -1, memoSid = -1, memoLeaf: any = null;
function getNestedMapBinary(sid = 0, mid = 0, rid = 0) {
    if (rid === memoRid && mid === memoMid && sid === memoSid) {
        return memoLeaf;
    }
    const idx = (rid * 16384) + (mid * 256) + sid;
    let leaf = BINARY_REGISTRY[idx];
    if (leaf === undefined) {
        leaf = new DummyLeaf(LEAF_CAP);
        BINARY_REGISTRY[idx] = leaf;
    }
    memoRid = rid; memoMid = mid; memoSid = sid; memoLeaf = leaf;
    return leaf;
}
// ============================================================================
// Runner (10 Seconds Each: Set & Get via Router)
// ============================================================================
function runRouterBenchmark1(name: string, routerFn: () => any) {
    const durationMs = 10000;
    let ops = 0;
    const start = performance.now();
    const endTarget = start + durationMs;

    while (performance.now() < endTarget) {
        for (let i = 0; i < 1024; i++) {
            const store = routerFn();
            store.s(i * 1.5, i);
            store.g(i);
            ops += 2;
        }
    }

    const elapsed = (performance.now() - start) / 1000;
    const opsPerSec = Math.round(ops / elapsed);
    console.log(`[${name}] ${ops.toLocaleString()} ops in ${elapsed.toFixed(2)}s -> ${opsPerSec.toLocaleString()} ops/sec`);
}
function runRouterBenchmark(name: string, routerFn: (s: number, m: number, r: number) => any) {
    const durationMs = 10000;
    let ops = 0;
    let sink = 0;
    const start = performance.now();
    const endTarget = start + durationMs;
    const sMax = 256, mMax = 64, rMax = 8;
    let counter = 0;

    while (performance.now() < endTarget) {
        for (let i = 0; i < 1024; i++) {
            // vary the key so no folding/hoisting is possible
            const s = counter & 255;          // 0..255
            const m = (counter >> 8) & 63;    // 0..63
            const r = (counter >> 14) & 7;    // 0..7
            counter++;

            const store = routerFn(s, m, r);
            store.s(i * 1.5, i);
            sink += store.g(i);               // observe the read
            ops += 2;
        }
    }
    if (sink === Number.NEGATIVE_INFINITY) console.log("impossible");

    const elapsed = (performance.now() - start) / 1000;
    console.log(`[${name}] ${ops.toLocaleString()} ops -> ${Math.round(ops / elapsed).toLocaleString()} ops/sec`);
}
export function runAllBenchmarks() {
    console.log("=== Starting 10-Second Nested Router Benchmarks ===");
    // Warmup
    for (let i = 0; i < 100; i++) {
        getNestedM2B()
    }

    runRouterBenchmark("6. MINE", (s, m, r) => getNestedM2B(3,2,1));
   
 
   
    // getNestedMapBinary and getNestedMap3 must accept (s,m,r) params — check their signatures
    
    console.log("=== Benchmarks Complete ===");
}

runAllBenchmarks();
/**
 runRouterBenchmark("1. 3D FLAT (M2BNestedClass)", (s,m,r) => nestedClassInstance.getNestedMap(s, m, r));
 runRouterBenchmark("2. True Binary + Single-Slot Memo", (s, m, r) => getNestedMapBinary(s, m, r));

 runRouterBenchmark("3. NOT NESTED, CHS CONVERTED TO BIG INT", (s, m, r) => getNestedMap2(s, m, r));
 runRouterBenchmark("4. ORIGINAL RECIPE", (s,m,r) => getOriginalNestedMap(
    s === 0 ? "comp_space" : `slave_${s}`, 
    m === 0 ? "workspace"  : `master_${m}`, 
    r === 0 ? "sauron"     : `root_${r}`
));

runRouterBenchmark("6. HOT PATH IDX", (s, m, r) => getNestedMap3(
    s === 0 ? "comp_space" : `slave_${s}`, 
    m === 0 ? "workspace"  : `master_${m}`, 
    r === 0 ? "sauron"     : `root_${r}`
));

runRouterBenchmark("5. INTERNED", (s,m,r) => getNestedMapInterned(
    s === 0 ? "comp_space" : `slave_${s}`, 
    m === 0 ? "workspace"  : `master_${m}`, 
    r === 0 ? "sauron"     : `root_${r}`
));
 */
