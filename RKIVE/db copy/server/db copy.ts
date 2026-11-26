//                                                                                                                     IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Worker } from "node:worker_threads";
import { mkdir, open, readFile, rm, stat} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { encodeUtf8, decodeUtf8, writeBigUint64, writeUint32, readUint32, readBigUint64} from "./binary";
import { isWorkerError, MANIFEST_VERSION, NO_PAGE, OP_DELETE, OP_PUT, PAGE_FREE, PAGE_HEADER_SIZE, PAGE_PAYLOAD_CAPACITY, PAGE_RECORD_CONTINUATION, PAGE_RECORD_HEAD, PAGE_SIZE, PendingBatch, PendingWorkerRequest, StoredEntry, WAL_VERSION, WorkerRequestInput, type Durability, type FastKVOptions, type PageWrite, type WorkerRequest, type WorkerResponse} from "./types";

const workerPath = fileURLToPath( new URL("./storage-worker.ts", import.meta.url));
// #endregion
//                                                                                                                       TYPES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type MaybePromise<R = any> = R;
function MaybePromise(v) { return v !== undefined ? v : Promise.resolve(v);}
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
type AvroWriteFn = (w: FastAvroWriter, v: A) => V;
type AvroReadFn = (r: FastAvroReader) => A;
type AVROField = { name: S; type: AVROSchema };
type AVROValue = any;
type AVROSchema = S | AVROSchema[] | {
    type?: S | AVROSchema[];
    name?: S;
    namespace?: S;
    fields?: AVROField[];
    items?: AVROSchema;
    values?: AVROSchema;
    symbols?: AR<S>;
    size?: N;
};
export class FastAvroWriter {
  buf: Buffer = Buffer.alloc(256);
  pos: N = 0;

  reset(): V { this.pos = 0; }

  private ensure(length: N): V {
    if (this.pos + length <= this.buf.length) return;
    let size = this.buf.length;
    while (size < this.pos + length) size *= 2;
    const next = Buffer.alloc(size);
    this.buf.copy(next, 0, 0, this.pos);
    this.buf = next;
  }

  byte(v: N): V { this.ensure(1); this.buf[this.pos++] = v; }
  bytes(v: Uint8Array): V { this.ensure(v.length); this.buf.set(v, this.pos); this.pos += v.length; }

  long(value: N): V {
    this.ensure(10);
    if (Math.abs(value) <= SAFE_ZIGZAG_LIMIT) {
      let n = value >= 0 ? value * 2 : (-value * 2 - 1);
      while (n >= 128) { this.buf[this.pos++] = (n % 128) | 0x80; n = Math.floor(n / 128); }
      this.buf[this.pos++] = n;
      return;
    }
    let n = BigInt(value);
    n = n >= 0n ? n << 1n : ((-n) << 1n) - 1n;
    while (n >= 0x80n) { this.buf[this.pos++] = Number(n & 0x7fn) | 0x80; n >>= 7n; }
    this.buf[this.pos++] = Number(n);
  }
  float(value: N): V { this.ensure(4); this.buf.writeFloatLE(value, this.pos); this.pos += 4; }
  double(value: N): V { this.ensure(8); this.buf.writeDoubleLE(value, this.pos); this.pos += 8; }

  string(v: S): V {
    const len = Buffer.byteLength(v, "utf8");
    this.long(len);
    this.ensure(len);
    this.buf.write(v, this.pos, "utf8");
    this.pos += len;
  }

  finish(): Buffer { return Buffer.from(this.buf.subarray(0, this.pos)); }
}
export class FastAvroReader {
  pos: N = 0;
  constructor(private readonly buf: Buffer) {}

  byte(): N { return this.buf[this.pos++]; }
  bytes(length: N): Buffer { const out = Buffer.from(this.buf.subarray(this.pos, this.pos + length)); this.pos += length; return out; }

  long(): N {
    let result = 0, shift = 0, b: N;
    do {
      b = this.buf[this.pos++];
      result += (b & 0x7f) * Math.pow(2, shift);
      shift += 7;
    } while (b & 0x80);
    return result % 2 === 0 ? result / 2 : -(result + 1) / 2;
  }
  float(): N { const n = this.buf.readFloatLE(this.pos); this.pos += 4; return n; }
  double(): N { const n = this.buf.readDoubleLE(this.pos); this.pos += 8; return n; }
  string(): S { const len = this.long(); const s = this.buf.toString("utf8", this.pos, this.pos + len); this.pos += len; return s; }
  done(): B { return this.pos === this.buf.length; }
}
const AVRO_ERROR_SCHEMA: AVROSchema = {
  type: "record",
  name: "AVROError",
  fields: [
    { name: "type", type: "string" },
    { name: "error", type: "string" },
    { name: "id", type: "string" },
  ],
};
export class BINARY_AVRO_CODEGEN {
  private readonly names: OB<AVROSchema> = Object.create(null);

  private readonly writeRoot: AvroWriteFn;
  private readonly readRoot: AvroReadFn;
  private readonly writeError: AvroWriteFn;
  private readonly readError: AvroReadFn;
  private readonly writer = new FastAvroWriter();
  private readonly errorSchema = AVRO_ERROR_SCHEMA;

  constructor(schema: AVROSchema) {
    if (Array.isArray(schema)) throw new Error("Avro: root schema cannot be a union");

    this.collect(schema);
    this.collect(this.errorSchema);

    // Everything below runs exactly ONCE, here, at construction — the generated
    // source text is the whole point: no schema-walking left at encode/decode time.
    const writeBody = this.genWrite(schema, "value").join("\n");
    const readBody = `return ${this.genRead(schema)};`;
    this.writeRoot = new Function("w", "value", writeBody) as A as AvroWriteFn;
    this.readRoot = new Function("r", readBody) as A as AvroReadFn;

    const errWriteBody = this.genWrite(this.errorSchema, "value").join("\n");
    const errReadBody = `return ${this.genRead(this.errorSchema)};`;
    this.writeError = new Function("w", "value", errWriteBody) as A as AvroWriteFn;
    this.readError = new Function("r", errReadBody) as A as AvroReadFn;
  }

  // ---- compile-time schema bookkeeping (never runs after construction) ----

  private collect(schema: AVROSchema): V {
    if (Array.isArray(schema)) { for (const b of schema) this.collect(b); return; }
    if (typeof schema === "string") return;

    if (schema.name) this.names[schema.name] = schema;

    if (Array.isArray(schema.type)) this.collect(schema.type);
    else if (typeof schema.type === "object") this.collect(schema.type);
    else if (typeof schema.type === "string" && ![
      "null", "boolean", "int", "long", "float", "double",
      "bytes", "string", "record", "array", "map", "enum", "fixed",
    ].includes(schema.type)) {
      if (!this.names[schema.type]) throw new Error(`Avro: unknown named type ${schema.type}`);
    }

    if (schema.fields) for (const field of schema.fields) this.collect(field.type);
    if (schema.items) this.collect(schema.items);
    if (schema.values) this.collect(schema.values);
  }

  private resolve(schema: AVROSchema): AVROSchema {
    if (typeof schema === "string" && this.names[schema]) return this.names[schema];
    return schema;
  }

  private kindOf(schema: AVROSchema): S {
    schema = this.resolve(schema);
    if (typeof schema === "string") return schema;
    if (Array.isArray(schema)) return "union";
    return typeof schema.type === "string" ? schema.type : this.kindOf(schema.type);
  }

  // ---- codegen: produces an inline boolean-expression STRING testing a union branch ----

  private genMatch(schema: AVROSchema, expr: S): S {
    schema = this.resolve(schema);
  
    if (typeof schema === "string") {
      switch (schema) {
        case "null": return `(${expr} === null || ${expr} === undefined)`;
        case "boolean": return `typeof ${expr} === "boolean"`;
        case "int": return `typeof ${expr} === "number" && Number.isInteger(${expr})`;
        case "long": case "float": case "double": return `typeof ${expr} === "number"`;
        case "string": return `typeof ${expr} === "string"`;
        case "bytes": return `${expr} instanceof Uint8Array`;
        default: return "false";
      }
    }
  
    if (Array.isArray(schema)) {return schema.map(s => this.genMatch(s, expr)).join(" || ");}
  
    if (typeof schema.type !== "string") return this.genMatch(schema.type, expr);
  
    switch (schema.type) {
      case "fixed": return `${expr} instanceof Uint8Array && ${expr}.length === ${(schema as A).size}`;
      case "enum": return `${JSON.stringify((schema as A).symbols)}.includes(${expr})`;
      case "array": return `Array.isArray(${expr})`;
      case "map": case "record": return `${expr} !== null && typeof ${expr} === "object" && !Array.isArray(${expr})`;
      default: return this.genMatch(schema.type, expr);
    }
  }

  // ---- codegen: produces an array of JS STATEMENT strings that write `expr` into `w` ----

  private genWrite(schema: AVROSchema, expr: S): AR<S> {
    schema = this.resolve(schema);

    if (Array.isArray(schema)) {
      if (schema.length === 2) {
        const nullIdx = schema.findIndex((s) => this.kindOf(s) === "null");
        if (nullIdx !== -1) {
          const otherIdx = nullIdx === 0 ? 1 : 0;
          // common case: an optional field — special-cased to skip the generic
          // branch-scan entirely, since this covers most unions you'll ever write.
          return [`if (${expr} === null || ${expr} === undefined) { w.long(${nullIdx}); } else { w.long(${otherIdx}); ${this.genWrite(schema[otherIdx], expr).join(" ")} }`];
        }
      }
      const branches = schema.map((s, i) => `if (${this.genMatch(s, expr)}) { w.long(${i}); ${this.genWrite(s, expr).join(" ")} }`);
      return [branches.join(" else ") + " else { throw new Error('Avro: no matching union branch'); }"];
    }

    if (typeof schema === "string") {
      switch (schema) {
        case "null": return [];
        case "boolean": return [`w.byte(${expr} ? 1 : 0);`];
        case "int": case "long": return [`w.long(${expr});`];
        case "float": return [`w.float(${expr});`];
        case "double": return [`w.double(${expr});`];
        case "string": return [`w.string(${expr});`];
        case "bytes": return [`w.long(${expr}.length); w.bytes(${expr});`];
        default: throw new Error(`Avro: unknown type ${schema}`);
      }
    }
    if (typeof schema.type !== "string") return this.genWrite(schema.type, expr);

    switch (schema.type) {
      case "null": return [];
      case "boolean": return [`w.byte(${expr} ? 1 : 0);`];
      case "int": case "long": return [`w.long(${expr});`];
      case "float": return [`w.float(${expr});`];
      case "double": return [`w.double(${expr});`];
      case "string": return [`w.string(${expr});`];
      case "bytes": return [`w.long(${expr}.length); w.bytes(${expr});`];
      case "fixed": {
        const size = (schema as A).size;
        return [`if (!(${expr} instanceof Uint8Array) || ${expr}.length !== ${size}) throw new Error("Avro: invalid fixed value"); w.bytes(${expr});`];
      }
      case "enum": {
        const symbols = JSON.stringify((schema as A).symbols);
        return [`{ const i = ${symbols}.indexOf(${expr}); if (i < 0) throw new Error("Avro: invalid enum symbol " + ${expr}); w.long(i); }`];
      }
      case "record": {
        const lines: S[] = [];
        for (const f of schema.fields as A[]) lines.push(...this.genWrite(f.type, `${expr}[${JSON.stringify(f.name)}]`));
        return lines;
      }
      case "array": {
        const inner = this.genWrite((schema as A).items, "arr[i]").join(" ");
        return [`{ const arr = ${expr}; if (arr.length) { w.long(arr.length); for (let i = 0; i < arr.length; i++) { ${inner} } } w.long(0); }`];
      }
      case "map": {
        const inner = this.genWrite((schema as A).values, "obj[k]").join(" ");
        return [`{ const obj = ${expr}; const keys = Object.keys(obj); if (keys.length) { w.long(keys.length); for (let i = 0; i < keys.length; i++) { const k = keys[i]; w.string(k); ${inner} } } w.long(0); }`];
      }
      default: throw new Error(`Avro: unsupported schema type ${schema.type}`);
    }
  }

  // ---- codegen: produces a single JS EXPRESSION string that reads one value from `r` ----

  private genRead(schema: AVROSchema): S {
    schema = this.resolve(schema);

    if (Array.isArray(schema)) {
      const cases = schema.map((s, i) => `case ${i}: return ${this.genRead(s)};`).join(" ");
      return `(function(){ const i = r.long(); switch(i){ ${cases} default: throw new Error("Avro: invalid union index"); } })()`;
    }
    if (typeof schema === "string") {
      switch (schema) {
        case "null": return "null";
        case "boolean": return "(r.byte() === 1)";
        case "int": case "long": return "r.long()";
        case "float": return "r.float()";
        case "double": return "r.double()";
        case "string": return "r.string()";
        case "bytes": return "r.bytes(r.long())";
        default: throw new Error(`Avro: unknown type ${schema}`);
      }
    }
    if (typeof schema.type !== "string") return this.genRead(schema.type);

    switch (schema.type) {
      case "null": return "null";
      case "boolean": return "(r.byte() === 1)";
      case "int": case "long": return "r.long()";
      case "float": return "r.float()";
      case "double": return "r.double()";
      case "string": return "r.string()";
      case "bytes": return "r.bytes(r.long())";
      case "fixed": return `r.bytes(${(schema as A).size})`;
      case "enum": return `(${JSON.stringify((schema as A).symbols)})[r.long()]`;
      case "record": {
        const assigns = (schema.fields as A[]).map((f) => `o[${JSON.stringify(f.name)}] = ${this.genRead(f.type)};`).join(" ");
        return `(function(){ const o = {}; ${assigns} return o; })()`;
      }
      case "array": return `(function(){ const out = []; let c; while ((c = r.long()) !== 0) { for (let i = 0; i < c; i++) out.push(${this.genRead((schema as A).items)}); } return out; })()`;
      case "map": return `(function(){ const out = {}; let c; while ((c = r.long()) !== 0) { for (let i = 0; i < c; i++) { const k = r.string(); out[k] = ${this.genRead((schema as A).values)}; } } return out; })()`;
      default: throw new Error(`Avro: unsupported schema type ${schema.type}`);
    }
  }

  // ---- public API: identical contract to BINARY_AVRO_FAST / the original BINARY_AVRO ----

  public encode_msg(msg: AVROValue): Buffer {
    if (msg instanceof Uint8Array) return msg as A;
    try {
      this.writer.reset();
      this.writer.long(0);
      this.writeRoot(this.writer, msg);
      return this.writer.finish();
    } catch (err) {
      console.log("[AVRO_PIPE] Failed to encode outbound packet:", err);
      this.writer.reset();
      this.writer.long(1);
      this.writeError(this.writer, {
        type: "error",
        error: err instanceof Error ? err.message : "Encoding failed",
        id: String((msg as A)?.id ?? "unknown"),
      });
      return this.writer.finish();
    }
  }

  public decode_msg(raw: A): AVROValue {
    if (!(raw instanceof Uint8Array)) {
      console.error("[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
      return null;
    }
    try {
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength);
      const r = new FastAvroReader(buf);
      const branch = r.long();
      if (branch !== 0 && branch !== 1) throw new Error("Avro: invalid envelope branch");
      const value = branch === 0 ? this.readRoot(r) : this.readError(r);
      if (!r.done()) throw new Error("Avro: trailing bytes");
      return value;
    } catch (err) {
      console.error("[AVRO_PIPE] Failed to decode inbound packet:", err);
      return null;
    }
  }
}
const SAFE_ZIGZAG_LIMIT = Number.MAX_SAFE_INTEGER / 2;
const SIG_BUS_SCHEMA: AVROSchema = {
  type: "record",
  name: "SIGBusMessage",
  fields: [
    { name: "id", type: "string" },
    { name: "r", type: "string" },
    { name: "m", type: "string" },
    { name: "s", type: "string" },
    { name: "k", type: "string" },
    { name: "p", type: "string" },
    { name: "ts", type: "long" },
    { name: "_ack", type: "boolean" },
    { name: "replyTo", type: ["null", "string"] },
    { name: "stream", type: ["null", "boolean"] },
    { name: "end", type: ["null", "boolean"] },
    { name: "ttl", type: ["null", "long"] },
    { name: "error", type: ["null", { type: "record", name: "ErrorInfo", fields: [
    { name: "message", type: "string" },
    { name: "code", type: ["null", "string"] },
    ] }] },
  ],
};
// #endregion
//                                                                                                                    MANIFEST
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const MANIFEST_MAGIC = 0x464b5631; // FKV1
const MANIFEST_SIZE = 24;
export interface Manifest {
  checkpointSequence: bigint;
  pageCount: number;
}
export function encodeManifest(  manifest: Manifest): Uint8Array {
  const bytes = new Uint8Array(MANIFEST_SIZE);
  const view = new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength
  );

  view.setUint32(0, MANIFEST_MAGIC, true);
  view.setUint32(4, MANIFEST_VERSION, true);
  writeBigUint64(bytes, 8, manifest.checkpointSequence);
  writeUint32(bytes, 16, manifest.pageCount);
  writeUint32(bytes, 20, crc32(bytes, 0, 20));

  return bytes;
}
export function decodeManifest(  bytes: Uint8Array): Manifest | undefined {
  if (bytes.length < MANIFEST_SIZE) return undefined;

  const view = new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength
  );

  if (view.getUint32(0, true) !== MANIFEST_MAGIC) {
    return undefined;
  }

  if (view.getUint32(4, true) !== MANIFEST_VERSION) {
    return undefined;
  }

  const storedChecksum = readUint32(bytes, 20);

  if (storedChecksum !== crc32(bytes, 0, 20)) {
    return undefined;
  }

  return {
    checkpointSequence: readBigUint64(bytes, 8),
    pageCount: readUint32(bytes, 16)
  };
}
// #endregion
//                                                                                                                         WAL
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface WalRecord {
  sequence: bigint;
  operation: number;
  key: Uint8Array;
  value: Uint8Array;
}
export function encodeWalRecord(  record: WalRecord): Uint8Array {
  const body = new Uint8Array(
    1 +
    1 +
    8 +
    4 +
    4 +
    record.key.length +
    record.value.length
  );

  const view = new DataView(
    body.buffer,
    body.byteOffset,
    body.byteLength
  );

  let offset = 0;

  view.setUint8(offset, WAL_VERSION);
  offset += 1;

  view.setUint8(offset, record.operation);
  offset += 1;

  view.setBigUint64(offset, record.sequence, true);
  offset += 8;

  view.setUint32(offset, record.key.length, true);
  offset += 4;

  view.setUint32(offset, record.value.length, true);
  offset += 4;

  body.set(record.key, offset);
  offset += record.key.length;

  body.set(record.value, offset);

  const frame = new Uint8Array(
    4 + body.length + 4
  );

  const frameView = new DataView(
    frame.buffer,
    frame.byteOffset,
    frame.byteLength
  );

  frameView.setUint32(0, body.length, true);
  frame.set(body, 4);

  frameView.setUint32(
    4 + body.length,
    crc32(frame, 4, 4 + body.length),
    true
  );

  return frame;
}
export interface DecodedWalResult {
  records: WalRecord[];
  validBytes: number;
}
export function decodeWalRecords(
  bytes: Uint8Array,
  minimumSequence: bigint
): DecodedWalResult {
  const records: WalRecord[] = [];
  let offset = 0;

  while (offset + 4 <= bytes.length) {
    const bodyLength = readUint32(bytes, offset);

    const frameEnd =
      offset + 4 + bodyLength + 4;

    if (
      bodyLength < 18 ||
      frameEnd > bytes.length
    ) {
      break;
    }

    const storedChecksum = readUint32(
      bytes,
      offset + 4 + bodyLength
    );

    const actualChecksum = crc32(
      bytes,
      offset + 4,
      offset + 4 + bodyLength
    );

    if (storedChecksum !== actualChecksum) {
      break;
    }

    const body = bytes.subarray(
      offset + 4,
      offset + 4 + bodyLength
    );

    const bodyView = new DataView(
      body.buffer,
      body.byteOffset,
      body.byteLength
    );

    const version = bodyView.getUint8(0);

    if (version !== WAL_VERSION) {
      break;
    }

    const operation = bodyView.getUint8(1);
    const sequence = bodyView.getBigUint64(2, true);
    const keyLength = bodyView.getUint32(10, true);
    const valueLength = bodyView.getUint32(14, true);

    if (sequence <= minimumSequence) {
      offset = frameEnd;
      continue;
    }

    const keyStart = 18;
    const valueStart = keyStart + keyLength;
    const valueEnd = valueStart + valueLength;

    if (valueEnd > body.length) {
      break;
    }

    records.push({
      sequence,
      operation,
      key: body.subarray(keyStart, valueStart),
      value: body.subarray(valueStart, valueEnd)
    });

    offset = frameEnd;
  }

  return {
    records,
    validBytes: offset
  };
}
// #endregion
//                                                                                                                         CRC
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
  let value = i;

  for (let bit = 0; bit < 8; bit++) {
    value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }

  CRC_TABLE[i] = value >>> 0;
}
export function crc32(bytes: Uint8Array, start = 0, end = bytes.length): number {
  let crc = 0xffffffff;

  for (let i = start; i < end; i++) {
    crc = CRC_TABLE[(crc ^ bytes[i]!) & 0xff]! ^ (crc >>> 8);
  }

  return (crc ^ 0xffffffff) >>> 0;
}
// #endregion
//                                                                                                                       PAGES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface RecordChain {
  pageNumbers: number[];
  pages: Uint8Array[];
}
export function encodeRecordChain(    key: Uint8Array,    value: Uint8Array,    allocatePage: () => number  ): RecordChain {
  const payload = new Uint8Array(
    12 + key.length + value.length
  );

  const payloadView = new DataView(
    payload.buffer,
    payload.byteOffset,
    payload.byteLength
  );

  payloadView.setUint32(0, key.length, true);
  payloadView.setUint32(4, value.length, true);
  payloadView.setUint32(
    8,
    crc32(value),
    true
  );

  payload.set(key, 12);
  payload.set(value, 12 + key.length);

  const pageCount = Math.max(
    1,
    Math.ceil(payload.length / PAGE_PAYLOAD_CAPACITY)
  );

  const pageNumbers: number[] = [];
  const pages: Uint8Array[] = [];

  for (let index = 0; index < pageCount; index++) {
    const pageNumber = allocatePage();
    const page = new Uint8Array(PAGE_SIZE);
    const view = new DataView(
      page.buffer,
      page.byteOffset,
      page.byteLength
    );

    const payloadStart = index * PAGE_PAYLOAD_CAPACITY;
    const payloadEnd = Math.min(
      payload.length,
      payloadStart + PAGE_PAYLOAD_CAPACITY
    );

    const used = payloadEnd - payloadStart;
    const nextPage =
      index === pageCount - 1
        ? NO_PAGE
        : NO_PAGE; // patched after next page is allocated

    pageNumbers.push(pageNumber);
    pages.push(page);

    view.setUint32(
      0,
      index === 0 ? PAGE_RECORD_HEAD : PAGE_RECORD_CONTINUATION,
      true
    );

    view.setUint32(4, nextPage, true);
    view.setUint32(8, used, true);
    view.setUint32(
      12,
      crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used),
      true
    );

    page.set(
      payload.subarray(payloadStart, payloadEnd),
      PAGE_HEADER_SIZE
    );
  }

  for (let index = 0; index < pageCount - 1; index++) {
    const page = pages[index]!;
    const view = new DataView(
      page.buffer,
      page.byteOffset,
      page.byteLength
    );

    view.setUint32(4, pageNumbers[index + 1]!, true);
    view.setUint32(
      12,
      crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + view.getUint32(8, true)),
      true
    );
  }

  return {
    pageNumbers,
    pages
  };
}
export interface DecodedRecord {
  key: Uint8Array;
  value: Uint8Array;
}
export function decodeRecordChain(    readPage: (pageNumber: number) => Uint8Array | undefined,    headPageNumber: number  ): DecodedRecord | undefined {
  const chunks: Uint8Array[] = [];
  let pageNumber: number | undefined = headPageNumber;
  let isHead = true;

  while (
    pageNumber !== undefined &&
    pageNumber !== NO_PAGE
  ) {
    const page = readPage(pageNumber);

    if (!page) return undefined;

    const view = new DataView(
      page.buffer,
      page.byteOffset,
      page.byteLength
    );

    const state = view.getUint32(0, true);
    const nextPage = view.getUint32(4, true);
    const used = view.getUint32(8, true);
    const checksum = view.getUint32(12, true);

    const expectedState = isHead
      ? PAGE_RECORD_HEAD
      : PAGE_RECORD_CONTINUATION;

    if (
      state !== expectedState ||
      used > PAGE_PAYLOAD_CAPACITY ||
      checksum !== crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)
    ) {
      return undefined;
    }

    chunks.push(
      page.subarray(PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)
    );

    pageNumber = nextPage === NO_PAGE ? undefined : nextPage;
    isHead = false;
  }

  if (chunks.length === 0) return undefined;

  const payloadLength = chunks.reduce(
    (total, chunk) => total + chunk.length,
    0
  );

  const payload = new Uint8Array(payloadLength);
  let offset = 0;

  for (const chunk of chunks) {
    payload.set(chunk, offset);
    offset += chunk.length;
  }

  if (payload.length < 12) return undefined;

  const payloadView = new DataView(
    payload.buffer,
    payload.byteOffset,
    payload.byteLength
  );

  const keyLength = payloadView.getUint32(0, true);
  const valueLength = payloadView.getUint32(4, true);
  const valueChecksum = payloadView.getUint32(8, true);

  const key = payload.subarray(12, 12 + keyLength);
  const value = payload.subarray(
    12 + keyLength,
    12 + keyLength + valueLength
  );

  if (value.length !== valueLength) return undefined;
  if (crc32(value) !== valueChecksum) return undefined;

  return {
    key,
    value
  };
}
export function encodeFreePage(): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  const view = new DataView(
    page.buffer,
    page.byteOffset,
    page.byteLength
  );

  view.setUint32(0, PAGE_FREE, true);
  return page;
}
// #endregion
//                                                                                                                          DB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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

    const walBytes = await readFile(this.#walPath).catch(() => new Uint8Array(0));

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
 I am building a durable file-backed key-value database in TypeScript/Node, AKA  A WAL DB WHERE THE USER PATH DOES NOT TOUCH ANY OF THE I/O PORTION OF THE DB FUNCTIONALITY, ALL OF THAT IS PASSED TO THE WORKER, AS YOU WILL SEE IN THE CODE ITS ALREADY CURRENTLY IMPLEMENTED IN THE READ AND WRITE PATHS... DO NOT FUCKING CHANGE THIS ASPECT BECAUSE THIS IS CURRENTLY THE FASTEST DB SOLUTION I COULD TEST, THE ONLY THING THAT CAME CLOSE TO IN PERFORMANCE WAS A DB THAT UTILIZED A SERVER FARM
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

 ENCODER
 - we need to switch over to BINARY_AVRO_CODEGEN as it is much faster as it encodes at 150+ encodes a second

 IMPORTANT ITEMS TO FIX
 - CURRENTLY THE SOLUTION ENCODES AND DECODES... A REDICULOUS AMOUNT OF TIMES, THIS IS NOT NEEDED... USING THE AVRO SOLUTION THAT IS PROVIDED, THERE IS ZERO NEED OR REQUIREMENT TO ENCODE ANY MORE THAN ONE TIME AND DECODE ONE TIME
 
 Important:
 - Do not add abstractions I did not ask for.
 - Do not invent internal keys, durability options, or extra caches.
 - Keep the API minimal: k, value, s, m, r
 */
