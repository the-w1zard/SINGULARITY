//                                                                                                                     IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Worker } from "node:worker_threads";
import { mkdir, open, readFile, rm, stat} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { encodeRecordChain, decodeRecordChain, encodeFreePage} from "./pages";
import { decodeWalRecords, encodeWalRecord} from "./wal";
import { decodeManifest, encodeManifest} from "./manifest";
import { encodeUtf8, decodeUtf8} from "./binary";
import { isWorkerError, NO_PAGE, OP_DELETE, OP_PUT, PAGE_SIZE, PendingBatch, PendingWorkerRequest, StoredEntry, WorkerRequestInput, type Durability, type FastKVOptions, type PageWrite, type WorkerRequest, type WorkerResponse} from "./types";

const workerPath = fileURLToPath( new URL("./storage-worker.ts", import.meta.url));
// #endregion
//                                                                                                                        DB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type MaybePromise<R = any> = R;
export function MaybePromise(v) {
  return v !== undefined ? v : Promise.resolve(v);
}
export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
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
	public forEach(fn: (v: T, k: K, map: this) => void): void { return this.forEachKV(fn); }
	public get length(): N { return this.size; }
	static groupBy<T, K extends PropertyKey>(items: I<T>, fn: (item: T, index: N) => K): O2<K, A2<T>> {
		const out = new O2<K, A2<T>>();
		let i = 0;
		for (const item of items) out.getOrM(() => new A2<T>(), fn(item, i++)).push(item);
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
	public s(v: UF<T>, k: K): this {//                                                    [SET]
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

		this.s(next, k);
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
		this.s(next, key);
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
	public getOrI(value: UF<T>, k: K): T {//                                          [GETORI]
		if (this.h(k)) return this.g(k) as T;
		const next = rv(value);
		this.s(next, k);
		return next;
	}
	public getOrM(make: () => T, k: K): T {//                                        [GETORM]
		if (this.h(k)) return this.g(k) as T;
		const value = make();
		this.s(value, k);
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

		this.s(next, k);
		return next;
	}
	public setIfAbsent(value: UF<T>, k: K): B {//                               [SETIFABSENT]
		if (this.h(k)) return false;
		const next = rv(value);
		this.s(next, k);
		return true;
	}
	public setIfPresent(value: UF<T>, k: K): B {//                             [SETIFPRESENT]
		if (!this.h(k)) return false;
		const next = rv(value);
		this.s(next, k);
		return true;
	}
	public replace(value: UF<T>, k: K): B {//                                        [REPLACE]
		const next = rv(value);
		return this.setIfPresent(next, k);
	}
	public inc(key: K, by: N = 1): N {//                                                 [INC]
		return this.update(key, (old) => (((old ?? 0) as N) + by) as unknown as T, 0 as unknown as T) as N;
	}
	public touch(k: K): B {//                                                      [SWTOUCHAP]
		return this.setIfAbsent(undefined as unknown as T, k);
	}
	public mapValues<G>(fn: (v: T, k: K) => G): O2<K, G> {//                        [MAPVALUES]
		const out = new O2<K, G>();
		for (const [k, v] of this) out.s(fn(v, k), k);
		return out;
	}
	public mapKeys<G extends PropertyKey>(fn: (v: T, k: K) => G): O2<G, T> {//        [MAPKEYS]
		const out = new O2<G, T>();
		for (const [k, v] of this) out.s(v, fn(v, k));
		return out;
	}
	public filterKV(pred: (v: T, k: K) => B): this {//                              [FILTERKV]
		for (const [k, v] of this) {
			if (!pred(v, k)) this.d(k);
		}
		return this;
	}
	public filterKeys(pred: (v: T, k: K) => B): this {//                          [FILTERKEYS]
		return this.filterKV(pred);
	}
	public partition(pred: (v: T, k: K) => B): [O2<K, T>, O2<K, T>] {//            [PARTITION]
		const yes = new O2<K, T>();
		const no = new O2<K, T>();

		for (const [k, v] of this) {
			(pred(v, k) ? yes : no).s(v, k);
		}

		return [yes, no];
	}
	public find(pred: (v: T, k: K) => B): [K, T] | UD {//                               [FIND]
		for (const [k, v] of this) {
			if (pred(v, k)) return [k, v];
		}
		return undefined;
	}
	public findKey(pred: (v: T, k: K) => B): K | UD {//                              [FINDKEY]
		for (const [k, v] of this) {
			if (pred(v, k)) return k;
		}
		return undefined;
	}
	public findValue(pred: (v: T, k: K) => B): T | UD {//                          [FINDVALUE]
		for (const [k, v] of this) {
			if (pred(v, k)) return v;
		}
		return undefined;
	}
	public some(pred: (v: T, k: K) => B): B {//                                         [SOME]
		for (const [k, v] of this) {
			if (pred(v, k)) return true;
		}
		return false;
	}
	public every(pred: (v: T, k: K) => B): B {//                                       [EVERY]
		for (const [k, v] of this) {
			if (!pred(v, k)) return false;
		}
		return true;
	}
	public reduce<G>(fn: (acc: G, v: T, k: K) => G, initial: G): G {//                [REDUCE]
		let acc = initial;
		for (const [k, v] of this) acc = fn(acc, v, k);
		return acc;
	}
	public forEachKV(fn: (v: T, k: K, map: this) => void): void {//                 [FOREACH]
		for (const [k, v] of this) fn(v, k, this);
	}
	public invert(): O2<A, K> {//               values become keys — must be string|number|symbol
		const out = new O2<A, K>();
		for (const [k, v] of this) out.s(k, v as A);
		return out;
	}
	public unique<G extends PropertyKey>(fn?: (v: T, k: K) => G): O2<G, T> {//          [UNIQUE]
		const out = new O2<G, T>();
		for (const [k, v] of this) {
			const key = (fn ? fn(v, k) : v) as unknown as G;
			if (!out.h(key)) out.s(v, key);
		}
		return out;
	}
	public pick(keys: Iterable<K>): O2<K, T> {//                                          [PICK]
		const out = new O2<K, T>();
		for (const k of keys) {
			if (this.h(k)) out.s(this.g(k) as T, k);
		}
		return out;
	}
	public omit(keys: Iterable<K>): O2<K, T> {//                                          [OMIT]
		const omitted = new Set(keys);
		const out = new O2<K, T>();
		for (const [k, v] of this) {
			if (!omitted.has(k)) out.s(v, k);
		}
		return out;
	}
	public merge(other: Iterable<readonly [K, T]>): this {//                           [MERGE]
		for (const [k, v] of other) {
			const n = rv(v);
			this.s(n, k);
		}
		return this;
	}
	public clearAndSet(entries: Iterable<readonly [K, T]>): this {//             [CLEARANDSET]
		return this.batch(() => {
			this.c();//                         (M2's version calls the native Map.clear() here
			for (const [k, v] of entries) {    //  — silent, no del events; c() is almost
				const n = rv(v);                //  certainly what was intended, so I used it)
				this.s(n, k);
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
			for (const k of Reflect.ownKeys(partial) as K[]) this.s((partial as A)[k] as T, k);
		});
	}
	public toValues(): A2I<T> { return A2.from(this.values()); }//                 [TOVALUES]
	public toKeys(): A2I<K> { return A2.from(this.keys()); }//                       [TOKEYS]
	public toArray(): A2I<[K, T]> { return A2.from(this.entries()); }//             [TOARRAY]
	public group<G extends PropertyKey>(fn: (v: T, k: K) => G): O2<G, A2<T>> {//       [GROUP]
		const out = new O2<G, A2<T>>();
		for (const [k, v] of this) out.getOrM(() => new A2<T>(), fn(v, k)).push(v);
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
export class A2<T = A> extends Array<T> implements A2<T> {
  constructor(...items: T[]) {
      super(...items);
      Object.setPrototypeOf(this, new.target.prototype);
  }

  static get [Symbol.species](): ArrayConstructor {
      return Array;
  }

  static from<T>(arrayLike: ArrayLike<T> | Iterable<T>): A2<T> {
      return new A2<T>(...Array.from(arrayLike));
  }

  static of<T>(...items: T[]): A2<T> {
      return new A2<T>(...items);
  }

  first(): T | UD {
      return this[0];
  }

  last(): T | UD {
      return this[this.length - 1];
  }

  isEmpty(): B {
      return this.length === 0;
  }

  isNotEmpty(): B {
      return this.length !== 0;
  }

  clear(): this {
      this.length = 0;
      return this;
  }

  clone(): A2<T> {
      return new A2<T>(...this);
  }

  unique(): A2<T> {
      return new A2<T>(...new Set(this));
  }

  compact(): A2<NonNullable<T>> {
      return new A2<NonNullable<T>>(
          ...this.filter((value): value is NonNullable<T> => value != null)
      );
  }

  take(count: N): A2<T> {
      return new A2<T>(...this.slice(0, Math.max(0, count)));
  }

  takeLast(count: N): A2<T> {
      return new A2<T>(...this.slice(Math.max(0, this.length - Math.max(0, count))));
  }

  drop(count: N): A2<T> {
      return new A2<T>(...this.slice(Math.max(0, count)));
  }

  dropLast(count: N): A2<T> {
      return new A2<T>(...this.slice(0, Math.max(0, this.length - Math.max(0, count))));
  }

  chunk(size: N): A2<A2<T>> {
      if (!Number.isInteger(size) || size < 1) {
          throw new RangeError("Chunk size must be a positive integer.");
      }

      const result = new A2<A2<T>>();

      for (let i = 0; i < this.length; i += size) {
          result.push(new A2<T>(...this.slice(i, i + size)));
      }

      return result;
  }

  flatten<X>(depth: N = Infinity): A2<X> {
      return new A2<X>(...this.flat(depth) as X[]);
  }

  flatMap<U>(callback: (value: T, index: N, array: this) => U | readonly U[]): A2<U> {
      return new A2<U>(
          ...this.flatMap((value, index) => callback(value, index as N, this))
      );
  }

  distinctBy<K>(callback: (value: T, index: N) => K): A2<T> {
      const seen = new Set<K>();
      const result = new A2<T>();

      this.forEach((value, index) => {
          const key = callback(value, index as N);

          if (!seen.has(key)) {
              seen.add(key);
              result.push(value);
          }
      });

      return result;
  }

  groupBy<K extends PropertyKey>(callback: (value: T, index: N) => K): Map<K, A2<T>> {
      const result = new Map<K, A2<T>>();

      this.forEach((value, index) => {
          const key = callback(value, index as N);
          let group = result.get(key);

          if (!group) {
              group = new A2<T>();
              result.set(key, group);
          }

          group.push(value);
      });

      return result;
  }

  partition(callback: (value: T, index: N) => B): [A2<T>, A2<T>] {
      const yes = new A2<T>();
      const no = new A2<T>();

      this.forEach((value, index) => {
          (callback(value, index as N) ? yes : no).push(value);
      });

      return [yes, no];
  }

  insert(index: N, ...items: T[]): this {
      this.splice(index, 0, ...items);
      return this;
  }

  removeAt(index: N): T | UD {
      const normalized = index < 0 ? this.length + index : index;

      if (normalized < 0 || normalized >= this.length) {
          return undefined;
      }

      return this.splice(normalized, 1)[0];
  }

  remove(value: T): B {
      const index = this.indexOf(value);

      if (index === -1) {
          return false;
      }

      this.splice(index, 1);
      return true;
  }

  removeAll(value: T): N {
      let removed = 0;

      for (let i = this.length - 1; i >= 0; i--) {
          if (Object.is(this[i], value)) {
              this.splice(i, 1);
              removed++;
          }
      }

      return removed as N;
  }

  replaceAt(index: N, value: T): T | UD {
      const normalized = index < 0 ? this.length + index : index;

      if (normalized < 0 || normalized >= this.length) {
          return undefined;
      }

      const previous = this[normalized];
      this[normalized] = value;
      return previous;
  }

  move(from: N, to: N): this {
      const source = from < 0 ? this.length + from : from;
      const target = to < 0 ? this.length + to : to;

      if (
          source < 0 || source >= this.length ||
          target < 0 || target >= this.length ||
          source === target
      ) {
          return this;
      }

      const [item] = this.splice(source, 1);
      this.splice(target, 0, item);
      return this;
  }

  swap(first: N, second: N): this {
      const a = first < 0 ? this.length + first : first;
      const b = second < 0 ? this.length + second : second;

      if (
          a < 0 || a >= this.length ||
          b < 0 || b >= this.length
      ) {
          return this;
      }

      [this[a], this[b]] = [this[b], this[a]];
      return this;
  }

  sample(): T | UD {
      if (this.length === 0) {
          return undefined;
      }

      return this[Math.floor(Math.random() * this.length)];
  }

  shuffle(): this {
      for (let i = this.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [this[i], this[j]] = [this[j], this[i]];
      }

      return this;
  }

  countBy(callback: (value: T, index: N) => B): N {
      let count = 0;

      this.forEach((value, index) => {
          if (callback(value, index as N)) count++;
      });

      return count as N;
  }

  sum(callback?: (value: T, index: N) => N): N {
      let total = 0;

      this.forEach((value, index) => {
          total += callback ? callback(value, index as N) : Number(value);
      });

      return total as N;
  }

  average(callback?: (value: T, index: N) => N): N {
      return (this.length === 0 ? NaN : this.sum(callback) / this.length) as N;
  }

  min(callback?: (value: T, index: N) => N): T | UD {
      if (this.length === 0) return undefined;

      return this.reduce((best, value, index) => {
          const current = callback ? callback(value, index as N) : Number(value);
          const bestValue = callback ? callback(best, this.indexOf(best) as N) : Number(best);

          return current < bestValue ? value : best;
      });
  }

  max(callback?: (value: T, index: N) => N): T | UD {
      if (this.length === 0) return undefined;

      return this.reduce((best, value, index) => {
          const current = callback ? callback(value, index as N) : Number(value);
          const bestValue = callback ? callback(best, this.indexOf(best) as N) : Number(best);

          return current > bestValue ? value : best;
      });
  }

  equals(array: readonly T[]): B {
      return this.length === array.length &&
          this.every((value, index) => Object.is(value, array[index]));
  }

  includesAll(values: readonly T[]): B {
      return values.every(value => this.includes(value));
  }

  intersects(values: readonly T[]): B {
      return values.some(value => this.includes(value));
  }
}

export class SERVER_KVDB {
  //                                                                                                                   CONSTANTS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public r: S = "THE_WIZARD_ROOT_REG";
  public m: S = "SAURON_MASTER_REG";
  public s: S = "SAURON_SLAVE_REG";
  public k: S = "__SAURON__";
  #hot_cache: Record<string, unknown> = Object.create(null);
  ///#hot_cache = new O2<S, A>();
  #waiters = new O2<S, A>();
  
  #entries = new O2<S, StoredEntry>();
  readonly #options: Required<FastKVOptions>;
  #directory!: S;
  #dataPath!: S;
  #walPath!: S;
  #manifestPath!: S;
  #worker?: Worker;
  #pending = new O2<S, PendingWorkerRequest>();
  #freePages: N[] = [];
  #dirtyPages = new O2<N, Uint8Array>();
  #pageCount = 0;
  #nextSequence = 1n;
  #checkpointSequence = 0n;
  #walSize = 0;
  #batch?: PendingBatch;
  #batchTimer?: NodeJS.Timeout;
  #writeQueue: P<U> = Promise.resolve();
  #closed = false;
  private _onError: (err: U) => void

  constructor(k: S = "__SAURON__", s: S = "SAURON_SLAVE_REG", m: S = "SAURON_MASTER_REG", r: S = "THE_WIZARD_ROOT_REG", options: Partial<FastKVOptions> = {}) {
    this.k = k
    this.s = s
    this.m = m
    this.r = r
    this.#options = {
      directory: options.directory ?? "./data/fastkv",
      durability: options.durability ?? "group",
      walBatchBytes: options.walBatchBytes ?? 256 * 1024,
      walBatchRecords: options.walBatchRecords ?? 4096,
      walFlushIntervalMs: options.walFlushIntervalMs ?? 1,
      walHighWaterBytes: options.walHighWaterBytes ?? 16 * 1024 * 1024,
      checkpointIntervalMs: options.checkpointIntervalMs ?? 5_000
    };
  }
  // #endregion
  //                                                                                                                         API
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public get = (() => {
    const z = <T = A>(k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {  
      const cached = this.#hot_cache[k];
      if (cached !== undefined) return MaybePromise(cached as T)
      else {
        const entry = this.#entries.get(k);
        if (!entry) return undefined;
        return MaybePromise(this.#workerRequest({
          kind: "readPages",
          pageNumbers: entry.pageNumbers
        }).then((response) => {
          if (response.kind !== "readPagesOk") {
            throw new Error(response.kind === "error" ? response.error : "Unexpected worker response" );
          }
          const decoded = this.#decodeRecordChainFromPages(response.pages,entry.pageNumbers);
          if (!decoded) {return undefined;}
          this.#hot_cache[k] = decoded.value;
          return decoded.value as T;
        }))
      }    
    }
    z.many = async (keys: S[]): P<V> => {
      await Promise.all(Object.entries(keys).map(([k]) => this.get(k)))
    }
    z.hot = <T = A>(k: S,) => {  
     return this.#hot_cache[k]
    }
    return z;
  })();
  public hot<T = A>(k: S,) {  
    return this.#hot_cache[k]
   }
  public set = (() => {
    type ZOptions = { durable: boolean; silent?: boolean; origin?: string };
    const z = async (k: S, value: A, s: S = this.s, m: S = this.m, r: S = this.r, options: ZOptions = { durable: true } ): P<V> => {
          this.#hot_cache[k] =value;
        // this.#hot_cache.set(k, value);

      const wk = `__waiters_${k}`
      const ws = this.#waiters.get(wk) as Set<(value: unknown) => void> | undefined
      if (ws) { for (const resolve of ws) resolve(value); this.#waiters.delete(wk); }
      this._emit(k, value)

      const valueBytes = typeof value === "string" ? encodeUtf8(value) : value;
      const keyBytes = encodeUtf8(k);
      return this.#enqueueWrite(async () => {
        if (this.#closed) { throw new Error("FastKV is closed"); }
        this.#applyPut(k, keyBytes, valueBytes);
        const record = encodeWalRecord({
          sequence: this.#nextSequence++,
          operation: OP_PUT,
          key: keyBytes,
          value: valueBytes
        });
        const durable = this.#resolveDurability(options.durable);
        const flushPromise = this.#appendToBatch( record, durable );
        if ( options.durable === true || this.#options.durability === "strict"  ) {  await flushPromise;  }
      });
    }
    z.many = async (entries: Record<S, A>): P<V> => {
      await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)))
    }
    return z;
  })();
  public delete = (() => {
    const z = async (key: S, options: { durable?: B } = {} ): P<B>=> {
      const keyBytes = encodeUtf8(key);
      this._emit(key, undefined)
      return this.#enqueueWrite(async () => {
        if (this.#closed) {throw new Error("FastKV is closed");}
        if (!this.#entries.has(key)) {return false;}
        this.#applyDelete(key);
        const record = encodeWalRecord({
          sequence: this.#nextSequence++,
          operation: OP_DELETE,
          key: keyBytes,
          value: new Uint8Array(0)
        });
        const durable = this.#resolveDurability(options.durable);
        const flushPromise = this.#appendToBatch(record,durable);
        if ( options.durable === true || this.#options.durability === "strict"  ) {await flushPromise;}
        return true;
      });
    } 
    z.many = async (keys: S[]): P<V> => {await Promise.all(keys.map((k) => this.delete(k))) }
    return z;
  })();
  public update = (() => {
    const z = async <T = A>(key: S, updater: (prev: T | UD) => T) => {
      const prev = await this.get<T>(key)
      const next = updater(prev)
      await this.set(key, next)
      return next
  }
    z.many = () => {}
    return z;
  })();
  public has(key: S): B { return this.#entries.has(key); }
  
  public send = (() => {
    const z = () => {}
    z.many = () => {}
    return z;
  })();
  // #endregion
  //                                                                                                                         SUB
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public subscribe<T = A>(key: S, cb: Subscriber<T>): Teardown {
    let set = this.#waiters.get(key)
    if (!set) { set = new Set(); this.#waiters.set(key, set) }
    set.add(cb)

    const cached = this.#hot_cache[key]
    if (cached) { try { cb(cached as T) } catch (e) { this._onError(e) } }

    return () => {
        const s = this.#waiters.get(key)
        if (!s) return
        s.delete(cb)
        if (s.size === 0) this.#waiters.delete(key)
    }
  }/** Selector-based subscription — only fires when the selected slice changes. */
  public select<T = A, S = A>(key: A, selector: Selector<T | UD, S>, cb: Subscriber<S>, chain?: CHAIN): Teardown {
        let last: S | undefined
        let first = true
        return this.subscribe<T>(key, (v) => {
            const next = selector(v)
            if (first || !Object.is(last, next)) {
                first = false
                last = next
                cb(next)
            }
        })
  }
  private _emit(key: S, value: A): V {
        const set = this.#waiters.get(key)
        if (!set) return
        for (const cb of set) {
            try { cb(value) }
            catch (e) { this._onError(e) }
        }
  }
  private _peek(key: S): A {
    const v = this.#hot_cache[key]
    return v === undefined ? undefined : v
  }
  public waitFor<T>(key: S): Promise<T> {  
    const existing = this._peek(key)
    if (existing !== undefined) return Promise.resolve(existing)

    return new Promise<T>((resolve) => {
        const waiterKey = `__waiters_${key}`
        let waiters = this.#waiters.get(waiterKey) as Set<(value: T) => void> | undefined

        if (!waiters) {
            waiters = new Set<(value: T) => void>()
            this.#waiters.set(waiterKey, waiters)
        }

        waiters.add(resolve)
    })
  }
  // #endregion
  //                                                                                                                  WORKER FNS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async open(): P<V> {
    if (this.#worker) { throw new Error("FastKV is already open"); }

    this.#directory = path.resolve(this.#options.directory);
    this.#dataPath = path.join(this.#directory, "data.paged");
    this.#walPath = path.join(this.#directory, "wal.log");
    this.#manifestPath = path.join(this.#directory, "manifest.bin");

    await mkdir(this.#directory, { recursive: true });

    const dataFile = await open(this.#dataPath, "a+");
    await dataFile.close();

    const walFile = await open(this.#walPath, "a+");
    await walFile.close();

    await this.#recover();
    await this.#startWorker();

    setInterval(() => {
      void this.#checkpointIfRequired();
    }, this.#options.checkpointIntervalMs).unref();
  }
  public async flush(): P<V> {
    await this.#enqueueWrite(async () => {
      await this.#flushCurrentBatch();
    });
  }
  public async checkpoint():P<V> {
    await this.#enqueueWrite(async () => {
      await this.#checkpoint();
    });
  }
  public async compact(): P<V> {
    await this.#enqueueWrite(async () => {
      await this.#flushCurrentBatch();
      const compactPath = `${this.#dataPath}.compact`;
      await this.#workerRequest({ kind: "compactBegin", compactPath });
      const livePages: PageWrite[] = [];
      let nextPage = 0;
      // const oldEntries = new O2(this.#entries);
      // const oldValues = new O2(this.#hot_cache);
      const oldEntries = Object.assign(Object.create(null), this.#entries);
      const oldValues = Object.assign(Object.create(null), this.#hot_cache);
      this.#entries.clear();
      // this.#hot_cache.clear();
      this.#hot_cache = Object.create(null);
      this.#freePages = [];
      this.#dirtyPages.clear();
      this.#pageCount = 0;
      for (const [key, entry] of oldEntries) {
        const value = oldValues.get(key);
        if (!value) continue;
       // this.#hot_cache.set(key, value);
       this.#hot_cache[key] = value;
        const keyBytes = encodeUtf8(key);
        const chain = encodeRecordChain(
          keyBytes,
          value,
          () => nextPage++
        );

        for (const page of chain.pages) {
          livePages.push({
            pageNumber: chain.pageNumbers[0]!,
            buffer: page
          });
        }

        this.#entries.set(key, {
          pageNumber: chain.pageNumbers[0]!,
          pageNumbers: chain.pageNumbers
        });

      }
      this.#pageCount = nextPage;
      const manifest = encodeManifest({checkpointSequence: this.#nextSequence, pageCount: this.#pageCount });
      await this.#workerRequest({ kind: "compactWrite", pages: livePages });
      await this.#workerRequest({
        kind: "compactCommit",
        dataPath: this.#dataPath,
        compactPath,
        manifest,
        walPath: this.#walPath
      });
      this.#checkpointSequence = this.#nextSequence;
      this.#walSize = 0;
    });
  }
  public async close(): P<V> {
    if (this.#closed) return;

    this.#closed = true;

    await this.#enqueueWrite(async () => {
      await this.#checkpoint();
    });

    await this.#workerRequest({
      kind: "close"
    });

    await this.#worker?.terminate();
    this.#worker = undefined;
  }
  get stats() {
    return {
      keys: this.#entries.size,
      pages: this.#pageCount,
      freePages: this.#freePages.length,
      dirtyPages: this.#dirtyPages.size,
      walBytes: this.#walSize,
      checkpointSequence: this.#checkpointSequence,
      nextSequence: this.#nextSequence,
      durability: this.#options.durability
    };
  }
  #resolveDurability(
    requested: boolean | undefined
  ): boolean {
    if (requested !== undefined) {
      return requested;
    }
  
    return (
      this.#options.durability === "group" ||
      this.#options.durability === "strict"
    );
  }
  #applyPut( key: string, keyBytes: Uint8Array, valueBytes: Uint8Array, replay = false ): void {
    const previous = this.#entries.get(key);

    const chain = encodeRecordChain(
      keyBytes,
      valueBytes,
      () => {
        const reused = this.#freePages.pop();

        if (reused !== undefined) {
          return reused;
        }

        return this.#pageCount++;
      }
    );

    if (previous) {
      for (const pageNumber of previous.pageNumbers) {
        this.#dirtyPages.set(
          pageNumber,
          encodeFreePage()
        );

        this.#freePages.push(pageNumber);
      }
    }

    for (let index = 0; index < chain.pages.length; index++) {
      this.#dirtyPages.set(
        chain.pageNumbers[index]!,
        chain.pages[index]!
      );
    }

    this.#entries.set(key, {
      pageNumber: chain.pageNumbers[0]!,
      pageNumbers: chain.pageNumbers
    });

   // this.#hot_cache.set(key, valueBytes);

    if (replay) {
      this.#nextSequence = this.#nextSequence;
    }
  }
  #applyDelete(key: string, replay = false ): void {
    const previous = this.#entries.get(key);

    if (!previous) return;

    for (const pageNumber of previous.pageNumbers) {
      this.#dirtyPages.set(
        pageNumber,
        encodeFreePage()
      );

      this.#freePages.push(pageNumber);
    }

    this.#entries.delete(key);
    //this.#hot_cache.delete(key);
    delete this.#hot_cache[key];
    if (replay) {
      this.#nextSequence = this.#nextSequence;
    }
  }
  #enqueueWrite<T>(
    operation: () => Promise<T>
  ): Promise<T> {
    const result = this.#writeQueue.then(operation, operation);

    this.#writeQueue = result.catch(() => undefined);

    return result;
  }
  #appendToBatch( record: Uint8Array, durable: boolean  ): Promise<void> {
    if (!this.#batch) {
      this.#batch = {
        batchId: crypto.randomUUID(),
        records: [],
        byteLength: 0,
        durable: false,
        waiters: []
      };
    }
  
    const batch = this.#batch;
  
    batch.records.push(record);
    batch.byteLength += record.length;
    batch.durable ||= durable;
  
    return new Promise<void>((resolve) => {
      batch.waiters.push(resolve);
  
      if (
        batch.byteLength >= this.#options.walBatchBytes ||
        batch.records.length >= this.#options.walBatchRecords
      ) {
        void this.#flushCurrentBatch();
        return;
      }
  
      this.#batchTimer ??= setTimeout(() => {
        this.#batchTimer = undefined;
        void this.#flushCurrentBatch();
      }, this.#options.walFlushIntervalMs);
    });
  }
  #workerRequest(request: WorkerRequestInput ): Promise<WorkerResponse> {
    if (!this.#worker) {return Promise.reject( new Error("FastKV is not open") );    }

    const id = crypto.randomUUID();

    return new Promise<WorkerResponse>((resolve, reject) => {
      this.#pending.set(id, {
        resolve,
        reject
      });

      this.#worker!.postMessage({
        ...request,
        id
      } satisfies WorkerRequest);
    });
  }
  // pages.ts
  #decodeRecordChainFromPages(
    pages: Uint8Array[],
    pageNumbers: number[]
  ): { key: Uint8Array; value: Uint8Array } | undefined {
    const pageByNumber = new Map<number, Uint8Array>();

    for (let index = 0; index < pageNumbers.length; index++) {
      const pageNumber = pageNumbers[index]!;
      const page = pages[index];

      if (page) {
        pageByNumber.set(pageNumber, page);
      }
    }

    const firstPageNumber = pageNumbers[0];

    if (firstPageNumber === undefined) {
      return undefined;
    }

    return decodeRecordChain(
      (pageNumber) => pageByNumber.get(pageNumber),
      firstPageNumber
    );
  }
  async #recover(): Promise<void> {
    const manifestBytes = await readFile(this.#manifestPath, { flag: "r" } ).catch(() => undefined);
    const manifest = manifestBytes ? decodeManifest(manifestBytes) : undefined;
    this.#checkpointSequence =  manifest?.checkpointSequence ?? 0n;
    const dataFile = await open(this.#dataPath, "r");
    try {
      const fileSize = Number((await dataFile.stat()).size);
      const pageCount = Math.floor(fileSize / PAGE_SIZE);
      const pageCache = new Map<number, Uint8Array>();
      for (let pageNumber = 0; pageNumber < pageCount; pageNumber++) {
        const page = new Uint8Array(PAGE_SIZE);
        const buffer = Buffer.from(
          page.buffer,
          page.byteOffset,
          page.byteLength
        );
        const result = await dataFile.read(
          buffer,
          0,
          PAGE_SIZE,
          pageNumber * PAGE_SIZE
        );
        if (result.bytesRead !== PAGE_SIZE) break;
        pageCache.set(pageNumber, page);
      }
      const readPage = (pageNumber: N ): U8 | UD => pageCache.get(pageNumber);
      for (const pageNumber of pageCache.keys()) {
        const page = pageCache.get(pageNumber)!;
        const view = new DataView(
          page.buffer,
          page.byteOffset,
          page.byteLength
        );
        const state = view.getUint32(0, true);
        if (state === 0) { this.#freePages.push(pageNumber); continue; }
        if (state !== 1) continue;
        const decoded = decodeRecordChain( readPage, pageNumber );
        if (!decoded) continue;
        const key = decodeUtf8(decoded.key);
        const pageNumbers: number[] = [];
        let cursor: number | undefined = pageNumber;
        while (cursor !== undefined && cursor !== NO_PAGE) {
          pageNumbers.push(cursor);
          const currentPage = pageCache.get(cursor)!;
          const currentView = new DataView(
            currentPage.buffer,
            currentPage.byteOffset,
            currentPage.byteLength
          );
          const nextPage = currentView.getUint32(4, true);
          cursor = nextPage === NO_PAGE ? undefined : nextPage;
        }

        this.#entries.set(key, {
          pageNumber,
          pageNumbers
        });

        //this.#hot_cache.set(key, decoded.value);
        this.#hot_cache[key] = decoded.value;
      }

      this.#pageCount = pageCount;
    } finally {
      await dataFile.close();
    }

    const walBytes = await readFile(this.#walPath)
      .catch(() => new Uint8Array(0));

    const decoded = decodeWalRecords(
      walBytes,
      this.#checkpointSequence
    );

    for (const record of decoded.records) {
      const key = decodeUtf8(record.key);

      if (record.operation === OP_PUT) {
        this.#applyPut(
          key,
          record.key,
          record.value,
          true
        );
      }

      else if (record.operation === OP_DELETE) {
        this.#applyDelete(key, true);
      }

      this.#nextSequence = record.sequence + 1n;
    }

    this.#walSize = decoded.validBytes;
  }
  async #flushCurrentBatch(): Promise<void> {
    const batch = this.#batch;
    if (!batch || batch.records.length === 0) { return; }
    this.#batch = undefined;
    if (this.#batchTimer) { clearTimeout(this.#batchTimer);  this.#batchTimer = undefined;  }
    let totalLength = 0;
    for (const record of batch.records) { totalLength += record.length;  }
    const bytes = new Uint8Array(totalLength);
    let offset = 0;
    for (const record of batch.records) {
      bytes.set(record, offset);
      offset += record.length;
    }
    try {
      const response = await this.#workerRequest({
        kind: "appendWal",
        batchId: batch.batchId,
        bytes,
        durable: batch.durable
      });
      if (isWorkerError(response)) {throw new Error(`WAL append failed: ${response.error}`); }
      if (response.kind !== "walOk") {throw new Error(`Unexpected WAL response: ${response.kind}`);}
      this.#walSize = response.walSize;
      for (const resolve of batch.waiters) {  resolve(); }
    } catch (error) {
      for (const waiter of batch.waiters) {
        // Resolve rather than reject: durability waiters must not hang.
        waiter();
      }
      throw error;
    }
  }
  async #checkpointIfRequired(): Promise<void> {
    if (this.#walSize >= this.#options.walHighWaterBytes) {
      await this.checkpoint();
    }
  }
  async #checkpoint(): Promise<void> {
    if (!this.#worker) {
      throw new Error("FastKV is not open");
    }

    await this.#flushCurrentBatch();

    const pages: PageWrite[] = [];

    for (const [pageNumber, buffer] of this.#dirtyPages) {
      pages.push({
        pageNumber,
        buffer: buffer.slice()
      });
    }

    const manifest = encodeManifest({
      checkpointSequence: this.#nextSequence - 1n,
      pageCount: this.#pageCount
    });

    await this.#workerRequest({
      kind: "checkpoint",
      pages,
      manifest
    });

    this.#dirtyPages.clear();
    this.#checkpointSequence = this.#nextSequence - 1n;
    this.#walSize = 0;
  }
  async #startWorker(): Promise<void> {
    this.#worker = new Worker(workerPath);
    this.#worker.on("message", (response: WorkerResponse) => {
      const pending = this.#pending.get(response.id);
      if (!pending) return;
      this.#pending.delete(response.id);
      if (isWorkerError(response)) {
        pending.reject(new Error(response.error));
        return;
      }
      pending.resolve(response);
    });
    this.#worker.on("error", (error) => {
      for (const pending of this.#pending.values()) { pending.reject(error); }
      this.#pending.clear();
    });
    await this.#workerRequest({
      kind: "init",
      dataPath: this.#dataPath,
      walPath: this.#walPath
    });
  }
  
  // #endregion
}

// #endregion


/**
 I am building a durable file-backed key-value database in TypeScript/Node.
 i wanted it so that the db uses the exact same 4 tiered scope channel format that every other aspect of the platform uses...
 r = root key or db name in this case
 m = master key, but this is used for workspace id  so each workspace would have a seperate  db tables specifically for that workspace only
 s = comp or category key, this would be the finaly folder within the db file/folider io
 
 k = key
 
 now if the user doesnt touch any of the keys other than k and s, and has worspaces toggled on, then the user autoamtilcy has a workspace aware db...  at the same time... it allows the user with surgical precision and ease to very quickly change tables, giving the user an amazing dx to organize data....
 Architecture:
 - Worker thread: the actual storage engine.
 - Worker owns: channel index, WAL, page allocation, dirty pages, free pages,
   recovery, checkpointing, compaction, and all file I/O.
 
 Key identity:
 - Every record is identified by the platform’s four-tier channel:
   r = root/database
   m = master/workspace
   s = category/table
   k = key
 - k/s/m/r are first-class parts of the stored record identity.
 - Do NOT flatten them into an “internal key” string.
 - Public methods take k plus optional s, m, r, with instance defaults:
   get(k, s = this.s, m = this.m, r = this.r)
   set(k, value, s = this.s, m = this.m, r = this.r)
   delete(k, s = this.s, m = this.m, r = this.r)
   has(k, s = this.s, m = this.m, r = this.r)
 
 Durability:
 - Every write is durable. Period.
 - There is no durable option, no durability mode, and no options object
   exposed on set/delete.
 - set() resolves only after its WAL record has been flushed/synced.
 - WAL batching/group commit is internal only.
 
 Read path:
 - get() checks hot cache first.
 - On hit: return value synchronously.
 - On miss: send channel identity to worker.
 - Worker looks up the channel in its index, reads/decodes the value,
   and returns the decoded value.
 - Main thread caches the decoded value.
 - There is no page-read API exposed to the main thread.
 
 Worker responses:
 - Use explicit response kinds.
 - Do not use a generic ok response with optional fields for different ops.
 - Example:
   readOk / notFound / writeOk / deleteOk / error
 
 Important:
 - Do not add abstractions I did not ask for.
 - Do not invent internal keys, durability options, or extra caches.
 - Keep the API minimal: k, value, s, m, r
 */
