// bus-bench.ts
// FIGHT=... bun --expose-gc bus-bench.ts

// import { SERVER_KVDB } from "./db/server/db";
import { darkState, root } from "./signals";


const CAPACITY = 1 << 18;
const DURATION = 12;
const REPORT_MS = 12;
const BATCH_SIZE = 100_000; // 500_000
const TOTAL_BATCHES = 100; // 500
const RUN_BARE_METAL_CPU_TEST: Boolean = true;

const pid = 1;
let seq = 0;
const v = `${++seq}`;

type fighter =
| "RAVEN"
| "db"
| "bare"
| "SKYWALKER"
| "SITH"
| "PALPATINE"
| "SIDIUS"
| "JEDI"
| "alien"
| "preact"
| "alien2"
| "preact2"
| "alien3"
|"SIGNAL_ENGINE"
|"REACTIVE_SYSTEM"

const FIGHT: fighter = (process.env.FIGHT as fighter) ?? "bare";


type SIG = { r: S; m: S; s: S; k: S; p: U };
type H = (p: U, msg?: SIG) => number;
type BUS_CHANNEL = { handler?: H };

const r = "THE_WIZARD_ROOT_REG";
const m = "SAURON_MASTER_REG";
const s = "SAURON_SLAVE_REG";
const k = "__SAURON__";

const handler = (p) => p.p + 1;
const handle2 = (p) => p.p + 1;
const handleFlat = (p) => p.p + 1;

// ─────────────────────────────────────────────────────────────
// BENCH
// ─────────────────────────────────────────────────────────────

const dur = 1000 * DURATION;
const report = 1000 * REPORT_MS;

function BARE_METAL_CPU(name: string, write: () => void): void {
  const batchTimes = new Float64Array(TOTAL_BATCHES);
  for (let i = 0; i < 2_000_000; i++) write();

  const globalStart = process.hrtime.bigint();
  for (let b = 0; b < TOTAL_BATCHES; b++) {
    const batchStart = process.hrtime.bigint();
    for (let i = 0; i < BATCH_SIZE; i++) write();
    batchTimes[b] = Number(process.hrtime.bigint() - batchStart);
  }
  const globalEnd = process.hrtime.bigint();

  const totalOps = BATCH_SIZE * TOTAL_BATCHES;
  let bestOpsPerSec = 0;
  for (let b = 0; b < TOTAL_BATCHES; b++) {
    const ops = BATCH_SIZE / (batchTimes[b] / 1e9);
    if (ops > bestOpsPerSec) bestOpsPerSec = ops;
  }
  const avg = totalOps / (Number(globalEnd - globalStart) / 1e9);

  console.log(`------------ ${name} ---------------`);
  console.log(`Peak (Best Batch):                      ${(bestOpsPerSec / 1e6).toFixed(2)} M ops/sec`);
  console.log(`Average:                                ${(avg / 1e6).toFixed(2)} M ops/sec`);
  console.log(`---------------------------------------------------------`);
}3

function BENCH(name: string, write: () => void): void {
  let windowOps = 0, totalOps = 0, best = 0, elapsedTotal = 0;
  let start = process.hrtime.bigint();
  let windowStart = start;
  let cpuStart = process.cpuUsage();

  console.log(`\n${name}`);
  console.log("sec | M writes/s | best M/s | avg M/s | CPU %");

  while (elapsedTotal < dur) {
    for (let i = 0; i < 100_000; i++) write();
    windowOps += 100_000; totalOps += 100_000;
    const now = process.hrtime.bigint();
    const elapsedUs = Number(now - windowStart) / 1_000;
    elapsedTotal = Number(now - start) / 1e6;
    if (elapsedUs >= report * 1_000) {
      const mp = windowOps / elapsedUs;
      const avg = totalOps / (Number(now - start) / 1e9) / 1e6;
      const cpuNow = process.cpuUsage();
      const cpuUs = cpuNow.user - cpuStart.user + cpuNow.system - cpuStart.system;
      best = Math.max(best, mp);
      console.log(
        String(Math.floor(elapsedTotal / 1_000)).padStart(3), "|",
        mp.toFixed(2).padStart(10), "|",
        best.toFixed(2).padStart(8), "|",
        avg.toFixed(7).padStart(7), "|",
        ((cpuUs / elapsedUs) * 100).toFixed(1).padStart(5)
      );
      windowOps = 0; windowStart = now; cpuStart = cpuNow;
    }
  }
}

const bench = RUN_BARE_METAL_CPU_TEST === false ? BENCH : BARE_METAL_CPU;
export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);


export class M2<H = A, T = A> extends Map<H, T> implements M2I<H, T> {
	// public g = this.get;
	// public h = this.has;
	// public d = this.delete;
	// public c = this.clear
	public f = this.forEach;//                                                     [FOR EACH]

	#subs?: Set<Sub<H, T>>;
	#hist?: AR<M2Event<H, T>>;
	#histLimit = 0;
	#strict = false;
	#iterating = 0;
	#batching = 0;
	#pending?: M2<H, M2Event<H, T>>;
	#derived?: M2<H, Derived<H, T>>;
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
			this.#emit({ op: "dirty", k: dk, has: d.has });
		}
	}
	#computeDerived(k: H, d: Derived<H, T>): T {
		const vals = d.deps.map((dep) => this.g(dep) as T);
		const next = d.fn(...vals);

		const had = d.has;
		const prev = d.value;

		d.value = next;
		d.has = true;
		d.dirty = false;

		if (!had || !sameValue(prev, next)) {
			this.#emit({
				op: had ? "upd" : "set",
				k,
				v: next,
				prev: prev as T,
				has: true,
			});
		}

		return next;
	}
	#assertNotIterating(op: S): V {
		if (this.#strict && this.#iterating > 0) {
			throw new Error(`M2I.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
		}
	}
	public [Symbol.iterator](): MapIterator<[H, T]> {return this.entries();	}
	public on(fn: Sub<H, T>): () => V {//                                                [ON]
		(this.#subs ??= new Set()).add(fn);
		return () => this.off(fn);
	}
	public off(fn: Sub<H, T>): V {//                                                    [OFF]
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
			has,
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
		});

		return out;
	}
	public c(): V {//                                                                 [CLEAR]
		
		this.#assertNotIterating("clear");

		const entries = [...this.entries()];
		super.clear();

		for (const [k, prev] of entries) {
			this.#invalidate(k);
			this.#emit({ op: "del", k, prev, has: false });
		}
	}
	public dv(k: H, deps: AR<H>, fn: (...vals: AR<T>) => T): this {//              [DERRIVED]
		
		(this.#derived ??= new M2<H, Derived<H, T>>())
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
	public dirty(...keys: AR<H>): this {//                                            [DIRTY]
		
		for (const k of keys) {
			const d = this.#derived?.get(k);

			if (d && !d.dirty) {
				d.dirty = true;
				this.#emit({ op: "dirty", k, has: d.has });
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
	public mapValues<G>(fn: (v: T, k: H) => G): M2I<H, G> {//                      [MAPVALUES]
		const out = new M2<H, G>();
		for (const [k, v] of this) out.s(fn(v, k), k);
		return out;
	}
	public mapKeys<G>(fn: (v: T, k: H) => G): M2I<G, T> {//                          [MAPKEYS]
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
	public partition(pred: (v: T, k: H) => B): [M2I<H, T>, M2I<H, T>] {//           [PARTITION]
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
	public invert(): M2I<T, H> {//                                                    [INVERT]
		const out = new M2<T, H>();
		for (const [k, v] of this) out.s(k, v);
		return out;
	}
	public unique<G>(fn?: (v: T, k: H) => G): M2I<G, T> {//                           [UNIQUE]
		const out = new M2<G, T>();
		for (const [k, v] of this) {
			const key = (fn ? fn(v, k) : v) as unknown as G;
			if (!out.h(key)) out.s(v, key);
		}
		return out;
	}
	public pick(keys: Iterable<H>): M2I<H, T> {//                                       [PICK]
		const out = new M2<H, T>();
		for (const k of keys) {
			if (this.h(k)) out.s(this.g(k) as T, k);
		}
		return out;
	}
	public omit(keys: Iterable<H>): M2I<H, T> {//                                       [EMIT]
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
	public group<G>(fn: (v: T, k: H) => G): M2I<G, A2<T>> {//                          [GROUP]
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
export class O2<K extends PropertyKey = S, T = A> {
    #data: Record<K, T> = Object.create(null);

    public f = this.forEachKV;//                                                    [FOR EACH]

    #subs?: Set<Sub<K, T>>;
    #hist?: M2Event<K, T>[];
    #histLimit = 0;
    #strict = false;
    #iterating = 0;
    #batching = 0;
    #pending?: Record<K, M2Event<K, T>>;
    #derived?: Record<K, Derived<K, T>>;
    #dependents?: Record<K, Set<K>>;

    constructor(entries?: I<readonly [K, T]>) {//                                   [CONSTRUCT]
        if (!entries) return;
        for (const [k, v] of entries) this.#data[k] = v;
    }

    #rawHas(k: K): B { return Object.prototype.hasOwnProperty.call(this.#data, k); }
    #rawGet(k: K): T | UD { return this.#data[k]; }

    #emit(e: M2Event<K, T>): V {
        if (this.#batching > 0) {
            const pending = this.#pending![e.k];
            this.#pending![e.k] = pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true };
            return;
        }

        this.#record(e);
        this.#subs?.forEach((sub) => {
            try { sub(e); } catch (err: A) { process.emitWarning(err); }
        });
    }
    #record(e: M2Event<K, T>): V {
        if (!this.#hist) return;

        this.#hist.push(e);

        if (this.#hist.length > this.#histLimit) {
            this.#hist.splice(0, this.#hist.length - this.#histLimit);
        }
    }
    #invalidate(k: K): V {
        const deps = this.#dependents?.[k];
        if (!deps) return;

        for (const dk of deps) {
            const d = this.#derived?.[dk];
            if (!d || d.dirty) continue;

            d.dirty = true;
            this.#emit({ op: "dirty", k: dk, has: d.has });
        }
    }
    #computeDerived(k: K, d: Derived<K, T>): T {
        const vals = d.deps.map((dep) => this.g(dep) as T);
        const next = d.fn(...vals);

        const had = d.has;
        const prev = d.value;

        d.value = next;
        d.has = true;
        d.dirty = false;

        if (!had || !sameValue(prev, next)) {
            this.#emit({ op: had ? "upd" : "set", k, v: next, prev: prev as T, has: true });
        }

        return next;
    }
    #assertNotIterating(op: S): V {
        if (this.#strict && this.#iterating > 0) {
            throw new Error(`O2.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
        }
    }
    // IN ORDER TO MATCH MAPS CALL API FOR INSTANT AND EASY CONVERSIONS
    public get(k: K): T | UD { 
        const d = this.#derived?.[k];
        if (d) { if (d.dirty || !d.has) return this.#computeDerived(k, d); return d.value; }
        return this.#rawGet(k);
     }
    public set(k: K, v: UF<T>): this { 
        this.#assertNotIterating("s");
        const next = rv(v);
        const has = this.h(k);
        const prev = has ? this.g(k) : undefined;
        this.#data[k] = next;
        this.#invalidate(k);
        this.#emit({ op: "set", k, v: next, prev: prev as T, has });
        return this;
     }   // note: flipped to match Map's (key, value)
    public has(k: K): B { 
        const d = this.#derived?.[k];
        if (d) { if (d.dirty || !d.has) this.#computeDerived(k, d); return true; }
        return this.#rawHas(k);
     }
    public delete(k: K): B { 
        this.#assertNotIterating("delete");
        if (!this.h(k)) return false;
        const prev = this.g(k) as T;
        delete this.#data[k];
        this.#invalidate(k);
        this.#emit({ op: "del", k, prev, has: false });
        return true;
     }
    public clear(): V { 
        this.#assertNotIterating("clear");

        const entries = this.toArray();
        this.#data = Object.create(null);//              one object swap, no per-key delete

        for (const [k, prev] of entries) {
            this.#invalidate(k);
            this.#emit({ op: "del", k, prev, has: false });
        }
     }
    public forEach(fn: (k: K, v: T, map: this) => void): void { return this.forEachKV(fn); }
    public get length(): N { return this.size; }
    static groupBy<T, K extends PropertyKey>(items: I<T>, fn: (item: T, index: N) => K): O2<K, A2<T>> {
        const out = new O2<K, A2<T>>();
        let i = 0;
        for (const item of items) out.getOrM(fn(item, i++), () => new A2<T>(),).push(item);
        return out;
    }
    public get size(): N {//                                                             [SIZE]
        return Reflect.ownKeys(this.#data).length as N;
    }
    public [Symbol.iterator](): Generator<[K, T]> { return this.entries(); }
    public on(fn: Sub<K, T>): () => V {//                                                  [ON]
        (this.#subs ??= new Set()).add(fn);
        return () => this.off(fn);
    }
    public off(fn: Sub<K, T>): V {//                                                      [OFF]
        this.#subs?.delete(fn);
    }
    public batch(fn: () => V): this {//                                                 [BATCH]
        this.#batching++;
        this.#pending ??= Object.create(null);

        try {
            fn();
        } finally {
            this.#batching--;

            if (this.#batching === 0) {
                const pending = this.#pending!;
                this.#pending = undefined;

                for (const k of Reflect.ownKeys(pending) as K[]) {
                    const e = pending[k];
                    this.#record(e);
                    this.#subs?.forEach((sub) => {
                        try { sub(e); } catch (err: A) { process.emitWarning(err); }
                    });
                }
            }

        }

        return this;
    }
    public s(k: K, v: UF<T>, ): this {//                                                    [SET]
        this.#assertNotIterating("s");

        const next = rv(v);
        const has = this.h(k);
        const prev = has ? this.g(k) : undefined;

        this.#data[k] = next;

        this.#invalidate(k);
        this.#emit({ op: "set", k, v: next, prev: prev as T, has });

        return this;
    }
    public mutate(k: K, fn: (old: T, has: true) => T): T | UD {//                      [MUTATE]
        this.#assertNotIterating("mutate");

        if (!this.h(k)) return undefined;

        const old = this.g(k) as T;
        const next = fn(old, true);

        this.s(k, next);
        return next;
    }
    public d(k: K): B {//                                                              [DELETE]
        this.#assertNotIterating("delete");
        if (!this.h(k)) return false;
        const prev = this.g(k) as T;
        delete this.#data[k];
        this.#invalidate(k);
        this.#emit({ op: "del", k, prev, has: false });
        return true;
    }
    public c(): V {//                                                                   [CLEAR]
        this.#assertNotIterating("clear");

        const entries = this.toArray();
        this.#data = Object.create(null);//              one object swap, no per-key delete

        for (const [k, prev] of entries) {
            this.#invalidate(k);
            this.#emit({ op: "del", k, prev, has: false });
        }
    }
    public dv(k: K, deps: AR<K>, fn: (...vals: AR<T>) => T): this {//                [DERRIVED]
        (this.#derived ??= Object.create(null))[k] = { deps, fn, dirty: true, has: false };

        for (const dep of deps) {
            const dependents = (this.#dependents ??= Object.create(null));
            (dependents[dep] ??= new Set<K>()).add(k);
        }

        return this;
    }
    public dirty(...keys: AR<K>): this {//                                              [DIRTY]
        for (const k of keys) {
            const d = this.#derived?.[k];

            if (d && !d.dirty) {
                d.dirty = true;
                this.#emit({ op: "dirty", k, has: d.has });
            }
        }

        return this;
    }
    public g(k: K): T | UD {//                                                           [GET]
        const d = this.#derived?.[k];
        if (d) { if (d.dirty || !d.has) return this.#computeDerived(k, d); return d.value; }
        return this.#rawGet(k);
    }
    public h(k: K): B {//                                                                [HAS]
        const d = this.#derived?.[k];
        if (d) { if (d.dirty || !d.has) this.#computeDerived(k, d); return true; }
        return this.#rawHas(k);
    }
    public $debug(limit: N = 256): this {//                                            [$DEBUG]
        this.#hist ??= [];
        this.#histLimit = limit;
        return this;
    }
    public $strict(): this {//                                                        [$STRICT]
        this.#strict = true;
        return this;
    }
    public $history(): M2Event<K, T>[] {//                                          [$HISTORY]
        return this.#hist ? [...this.#hist] : [];
    }
    public $why(k: K): M2Event<K, T> | UD {//                                            [$WHY]
        if (!this.#hist) return undefined;

        for (let i = this.#hist.length - 1; i >= 0; i--) {
            if (Object.is(this.#hist[i].k, k)) return this.#hist[i];
        }

        return undefined;
    }
    public $diff(other: O2<K, T>) {//                                                  [$DIFF]
        const add: AR<[K, T]> = [];
        const del: AR<K> = [];
        const chg: AR<[K, T, T]> = [];

        for (const [k, v] of this) {
            if (!other.h(k)) {
                add.push([k, v]);
            } else if (!Object.is(other.g(k), v)) {
                chg.push([k, v, other.g(k) as T]);
            }
        }

        for (const k of other.keys()) {
            if (!this.h(k)) del.push(k);
        }

        return { add, del, chg };
    }
    public *entries(): Generator<[K, T]> {//                                       [*ENTRIES]
        this.#iterating++;

        try {
            for (const k of Reflect.ownKeys(this.#data) as K[]) yield [k, this.#data[k]];
        } finally {
            this.#iterating--;
        }
    }
    public *keys(): Generator<K> {//                                                  [*KEYS]
        this.#iterating++;

        try {
            for (const k of Reflect.ownKeys(this.#data) as K[]) yield k;
        } finally {
            this.#iterating--;
        }
    }
    public *values(): Generator<T> {//                                              [*VALUES]
        this.#iterating++;

        try {
            for (const k of Reflect.ownKeys(this.#data) as K[]) yield this.#data[k];
        } finally {
            this.#iterating--;
        }
    }
    public swap(value: UF<T>, key: K): T | UD {//                                       [SWAP]
        const next = rv(value);
        const previous = this.h(key) ? this.g(key) : next;//       h()/g() kept derived-aware
        this.s(key, next);
        return previous as T | UD;
    }
    public take(k: K): T | UD {//                                                       [TAKE]
        const value = this.g(k);
        this.d(k);
        return value;
    }
    public peek(k: K): T | false {//                                                    [PEEK]
        return this.h(k) ? (this.g(k) as T) : false;
    }
    public getOrI( k: K, value: UF<T>): T {//                                          [GETORI]
        if (this.h(k)) return this.g(k) as T;
        const next = rv(value);
        this.s(k, next)
        return next;
    }
    public getOrM(k: K, make: () => T): T {//                                        [GETORM]
        if (this.h(k)) return this.g(k) as T;
        const value = make();
        this.s(k, value)
        return value;
    }
    public update(k: K, fn: (old: T, has: B) => T, initial?: UF<T>): T {//            [UPDATE]
        const has = this.h(k);

        const old = has
            ? (this.g(k) as T)
            : initial === undefined
                ? undefined as T
                : rv(initial);

        const next = rv(fn(old, has));

        this.s(k, next)
        return next;
    }
    public setIfAbsent(k: K, value: UF<T>, ): B {//                               [SETIFABSENT]
        if (this.h(k)) return false;
        const next = rv(value);
        this.s(k, next)
        return true;
    }
    public setIfPresent(k: K, value: UF<T> ): B {//                             [SETIFPRESENT]
        if (!this.h(k)) return false;
        const next = rv(value);
        this.s(k, next)
        return true;
    }
    public replace(k: K, value: UF<T> ): B {//                                        [REPLACE]
        const next = rv(value);
        return this.setIfPresent(k, next);
    }
    public inc(key: K, by: N = 1): N {//                                                 [INC]
        return this.update(key, (old) => (((old ?? 0) as N) + by) as unknown as T, 0 as unknown as T) as N;
    }
    public touch(k: K): B {//                                                      [SWTOUCHAP]
        return this.setIfAbsent(k, undefined as unknown as T);
    }
    public mapValues<G>(fn: ( k: K, v: T,) => G): O2<K, G> {//                        [MAPVALUES]
        const out = new O2<K, G>();
        for (const [k, v] of this) out.s(k, fn(k, v));
        return out;
    }
    public mapKeys<G extends PropertyKey>(fn: (k: K, v: T ) => G): O2<G, T> {//        [MAPKEYS]
        const out = new O2<G, T>();
        for (const [k, v] of this) out.s(fn(k, v), v);
        return out;
    }
    public filterKV(pred: (k: K, v: T,) => B): this {//                              [FILTERKV]
        for (const [k, v] of this) {
            if (!pred(k, v)) this.d(k);
        }
        return this;
    }
    public filterKeys(pred: (k: K, v: T ) => B): this {//                          [FILTERKEYS]
        return this.filterKV(pred);
    }
    public partition(pred: (k: K, v: T ) => B): [O2<K, T>, O2<K, T>] {//            [PARTITION]
        const yes = new O2<K, T>();
        const no = new O2<K, T>();

        for (const [k, v] of this) {
            (pred(k, v) ? yes : no).s(k, v);
        }

        return [yes, no];
    }
    public find(pred: (k: K, v: T ) => B): [K, T] | UD {//                               [FIND]
        for (const [k, v] of this) {
            if (pred(k, v)) return [k, v];
        }
        return undefined;
    }
    public findKey(pred: (k: K, v: T ) => B): K | UD {//                              [FINDKEY]
        for (const [k, v] of this) {
            if (pred(k, v)) return k;
        }
        return undefined;
    }
    public findValue(pred: (k: K, v: T ) => B): T | UD {//                          [FINDVALUE]
        for (const [k, v] of this) {
            if (pred(k, v)) return v;
        }
        return undefined;
    }
    public some(pred: (k: K, v: T ) => B): B {//                                         [SOME]
        for (const [k, v] of this) {
            if (pred(k, v)) return true;
        }
        return false;
    }
    public every(pred: (k: K, v: T ) => B): B {//                                       [EVERY]
        for (const [k, v] of this) {
            if (!pred(k, v)) return false;
        }
        return true;
    }
    public reduce<G>(fn: (acc: G,k: K,  v: T ) => G, initial: G): G {//                [REDUCE]
        let acc = initial;
        for (const [k, v] of this) acc = fn(acc, k, v);
        return acc;
    }
    public forEachKV(fn: (k: K, v: T , map: this) => void): void {//                 [FOREACH]
        for (const [k, v] of this) fn(k, v, this);
    }
    public invert(): O2<A, K> {//               values become keys — must be string|number|symbol
        const out = new O2<A, K>();
        for (const [k, v] of this) out.s(k, v as A);
        return out;
    }
    public unique<G extends PropertyKey>(fn?: (k: K, v: T ) => G): O2<G, T> {//          [UNIQUE]
        const out = new O2<G, T>();
        for (const [k, v] of this) {
            const key = (fn ? fn(k, v) : v) as unknown as G;
            if (!out.h(key)) out.s(key, v);
        }
        return out;
    }
    public pick(keys: Iterable<K>): O2<K, T> {//                                          [PICK]
        const out = new O2<K, T>();
        for (const k of keys) {
            if (this.h(k)) out.s(k, this.g(k) as T);
        }
        return out;
    }
    public omit(keys: Iterable<K>): O2<K, T> {//                                          [OMIT]
        const omitted = new Set(keys);
        const out = new O2<K, T>();
        for (const [k, v] of this) {
            if (!omitted.has(k)) out.s(k, v);
        }
        return out;
    }
    public merge(other: Iterable<readonly [K, T]>): this {//                           [MERGE]
        for (const [k, v] of other) {
            const n = rv(v);
            this.s(k, n);
        }
        return this;
    }
    public clearAndSet(entries: Iterable<readonly [K, T]>): this {//             [CLEARANDSET]
        return this.batch(() => {
            this.c();//                         (M2's version calls the native Map.clear() here
            for (const [k, v] of entries) {    //  — silent, no del events; c() is almost
                const n = rv(v);                //  certainly what was intended, so I used it)
                this.s(k, n);
            }
        });
    }
    public toObject(): OB<T> {//                                                   [TOOBJECT]
        const out = {} as OB<T>;
        for (const [k, v] of this) out[k as S] = v;
        return out;
    }
    public toObjectFast(): OB<T> {//          bulk native copy, skips the iterator entirely
        return { ...(this.#data as A) };
    }
    public snapshot(): OB<T> { return this.toObjectFast(); }//      handy name for hook/selector use
    public toJSON(): OB<T> { return this.toObjectFast(); }//     JSON.stringify(store) just works
    public select<G>(fn: (data: Readonly<Record<K, T>>) => G): G {//                   [SELECT]
        return fn(this.#data);
    }
    public patch(partial: Readonly<Partial<Record<K, T>>>): this {//                    [PATCH]
        return this.batch(() => {
            for (const k of Reflect.ownKeys(partial) as K[]) this.s(k, (partial as A)[k] as T);
        });
    }
    public toValues(): A2I<T> { return A2.from(this.values()); }//                 [TOVALUES]
    public toKeys(): A2I<K> { return A2.from(this.keys()); }//                       [TOKEYS]
    public toArray(): A2I<[K, T]> { return A2.from(this.entries()); }//             [TOARRAY]
    public group<G extends PropertyKey>(fn: (v: T, k: K) => G): O2<G, A2<T>> {//       [GROUP]
        const out = new O2<G, A2<T>>();
        for (const [k, v] of this) out.getOrM(fn(v, k), () => new A2<T>()).push(v);
        return out;
    }
    public clone(): this {//                                                          [CLONE]
        return new O2<K, T>(this) as this;
    }
    public equals(other: O2<K, T>): B {//                                            [EQUALS]
        if (other === this) return true;
        if (this.size !== other.size) return false;

        for (const [k, v] of this) {
            if (!other.h(k) || !sameValue(other.g(k), v)) return false;
        }

        return true;
    }
    static fromObject<X extends OB<A>>(obj: X): O2<S, X[keyof X]> {//              [FROMOBBJ]
        const out = new O2<S, X[keyof X]>();
        out.#data = { ...obj } as Record<S, X[keyof X]>;//            direct copy, no per-key emits
        return out;
    }
    static fromEntries<K extends PropertyKey, T>(entries: I<readonly [K, T]>): O2<K, T> {//  [FROMENTIRES]
        return new O2(entries);
    }
    static from<K extends PropertyKey, T>(source: O2<K, T> | Readonly<Record<K, T>>): O2<K, T> {// [FROM]
        const out = new O2<K, T>();
        out.#data = source instanceof O2 ? { ...source.#data } : { ...source };
        return out;
    }
}
/**
 * if (FIGHT === "db") {
const db = new SERVER_KVDB(
  "__SAURON__",
  "SAURON_SLAVE_REG",
  "SAURON_MASTER_REG",
  "THE_WIZARD_ROOT_REG",
  {
    directory: "./db/server/bench-data",
    durability: "group",
    walBatchBytes: 1024 * 1024,
    walBatchRecords: 8192,
    walFlushIntervalMs: 1,
    walHighWaterBytes: 64 * 1024 * 1024,
    checkpointIntervalMs: 10_000,
  },
);
await db.set(`keyy`, '1');

bench("RAVEN.store.get", () => {
  db.hot(`keyy`);
});
} 
 */
  
if (FIGHT === 'bare') {
//const hot_cache: Record<string, string> = Object.create(null);
const JSObj = Object.create(null) as Record<string, string>;
JSObj['keyy'] = "1";
bench("JSObj['keyy'];", () => { JSObj['keyy']; });
const O2Test = new O2<string, string>();
O2Test['keyy'] = '1'
bench("O2Test['keyy']", () => { O2Test['keyy'] });



O2Test.s("1", "keyy");
bench("O2.get(k)  [real #data]", () => { O2Test.get('keyy'); });

const d = new O2<string, string>();
d.s("2", "a");
d.dv("b", ["a"], (a) => (a as string) + "!");
d.get("b");
bench("O2.get(derived clean)", () => { d.get('b'); });

const dd = new O2<string, string>();
dd.s("2", "a");
dd.dv("b", ["a"], (a) => (a as string) + "!");
bench("O2.get(derived dirty)", () => { dd.dirty('b'); dd.get('b'); });

bench("O2.get(miss)", () => { O2Test.get('nope'); });

bench("O2.h(k) hit", () => { O2Test.h('keyy'); });
bench("O2.h(k) miss", () => { O2Test.h('nope'); });

bench("O2.s(v,k) overwrite", () => { O2Test.s("1", "keyy"); });
bench("O2.s(v,k) new+del churn", () => {
  const k = Math.random().toString(36).slice(2, 10);
  O2Test.s("1", k); O2Test.d(k);
});

bench("O2.size", () => { O2Test.size; });
}


if (FIGHT === "RAVEN") {
    /* ------------------------------------------------------------------ *
     * STORE
     * ------------------------------------------------------------------ */

    const storeKey = "bench-store";
    let storeValue = 0;

    root.state.store.set(storeKey, 1, { triggerRender: false });

    bench("RAVEN.store.get", () => {
        root.state.store.get(storeKey);
    });

    bench("RAVEN.store.has", () => {
        root.state.store.has(storeKey);
    });

    bench("RAVEN.store.set", () => {
        root.state.store.set(storeKey, ++storeValue, { triggerRender: false });
    });

    bench("RAVEN.store.set.functional", () => {
        root.state.store.set(storeKey, (prev: number | undefined) => (prev ?? 0) + 1, {
            triggerRender: false,
        });
    });

    bench("RAVEN.store.get.set", () => {
        root.state.store.get(storeKey);
        root.state.store.set(storeKey, ++storeValue, { triggerRender: false });
    });

    bench("RAVEN.store.entries", () => {
        root.state.store.entries();
    });

    bench("RAVEN.store.subscribe", () => {
        const dispose = root.state.store.subscribe(storeKey, () => {});
        dispose();
    });

    /* ------------------------------------------------------------------ *
     * STORE PERSISTENCE
     * ------------------------------------------------------------------ */

    const persistKey = "bench-persist";
    root.state.store.set(persistKey, 1, { triggerRender: false });
    root.state.store.persist(persistKey);

    bench("RAVEN.store.persist.get", () => {
        root.state.store.get(persistKey);
    });

    bench("RAVEN.store.persist.has", () => {
        root.state.store.has(persistKey);
    });

    bench("RAVEN.store.persist.set", () => {
        root.state.store.set(persistKey, ++storeValue, { triggerRender: false });
    });

    root.state.store.unpersist(persistKey);

    /* ------------------------------------------------------------------ *
     * STATE
     * ------------------------------------------------------------------ */

    const stateKey = "bench-state";
    let stateValue = 0;

    root.state.state.set(stateKey, 1, { triggerRender: false });

    bench("RAVEN.state.get", () => {
        root.state.state.get(stateKey);
    });

    bench("RAVEN.state.set", () => {
        root.state.state.set(stateKey, ++stateValue, { triggerRender: false });
    });

    bench("RAVEN.state.get.set", () => {
        root.state.state.get(stateKey);
        root.state.state.set(stateKey, ++stateValue, { triggerRender: false });
    });

    bench("RAVEN.state.getAll", () => {
        root.state.state.getAll();
    });

    bench("RAVEN.state.getByPrefix", () => {
        root.state.state.getByPrefix("bench-");
    });

    bench("RAVEN.state.subscribe", () => {
        const dispose = root.state.state.subscribe(stateKey, () => {});
        dispose();
    });

    /* ------------------------------------------------------------------ *
     * STATE NOTIFICATION
     * ------------------------------------------------------------------ */

    const notifyKey = "bench-state-notify";
    let notifyValue = 0;
    let notifyCount = 0;

    root.state.state.set(notifyKey, 0, { triggerRender: false });

    const disposeStateNotify = root.state.state.subscribe(notifyKey, () => {
        notifyCount++;
    });

    bench("RAVEN.state.notify", () => {
        root.state.state.set(notifyKey, ++notifyValue);
        root.state.flushSync();
    });

    disposeStateNotify();

    /* ------------------------------------------------------------------ *
     * HOOK: darkState
     * ------------------------------------------------------------------ */

    const [darkValue, setDarkValue] = darkState(0, {}, "bench-darkState");

    bench("RAVEN.darkState.get", () => {
        darkState(0, {}, "bench-darkState");
    });

    bench("RAVEN.darkState.set", () => {
        setDarkValue(++storeValue);
    });

    bench("RAVEN.darkState.get.set", () => {
        darkState(0, {}, "bench-darkState");
        setDarkValue(++storeValue);
    });

    /* ------------------------------------------------------------------ *
     * HOOK: darkState SELECTOR
     * ------------------------------------------------------------------ */

    const [selectedValue, setSelectedValue] = darkState(
        { count: 0, other: 0 },
        {
            selector: (v) => v.count,
        },
        "bench-darkState-selector",
    );

    bench("RAVEN.darkState.selector.get", () => {
        darkState(
            { count: 0, other: 0 },
            {
                selector: (v) => v.count,
            },
            "bench-darkState-selector",
        );
    });

    bench("RAVEN.darkState.selector.set", () => {
        setSelectedValue((prev) => ({
            ...prev,
            count: prev.count + 1,
        }));
    });

    /* ------------------------------------------------------------------ *
     * HOOK: darkState DERIVED
     * ------------------------------------------------------------------ */

    const [derivedValue, setDerivedValue, derived] = darkState(
        1,
        {
            derive: (v) => v * 2,
        },
        "bench-darkState-derived",
    );

    bench("RAVEN.darkState.derived.get", () => {
        darkState(
            1,
            {
                derive: (v) => v * 2,
            },
            "bench-darkState-derived",
        );
    });

    bench("RAVEN.darkState.derived.set", () => {
        setDerivedValue(++storeValue);
    });

    /* ------------------------------------------------------------------ *
     * KEEP FIXTURES LIVE
     * ------------------------------------------------------------------ */

    void notifyCount;
    void darkValue;
    void selectedValue;
    void derivedValue;
    void derived;
}



