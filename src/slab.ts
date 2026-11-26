// SLAB.TS
import { slab, SlabServices } from './slab.types';
export const sameValue = (a: A, b: A): B =>	Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);

export function expose<T extends object>(
  instance: T,
  name: keyof SlabServices & string,
): void {
  (slab as any)[name] = new Proxy(instance, {
    get(target, key, receiver) {
      const value = Reflect.get(target, key, target);

      if (typeof value === 'function') {
        return value.bind(target);
      }

      return value;
    },

    set(target, key, value) {
      Reflect.set(target, key, value, target);
      return true;
    },

    has(target, key) {
      return Reflect.has(target, key);
    },
  });
}

export class O2<K extends PropertyKey = S, T = A> {
  data: Record<K, T> = Object.create(null);

  public f = this.forEachKV;//                                                    [FOR EACH]

  subs?: Set<Sub<K, T>>;
  hist?: M2Event<K, T>[];
  histLimit = 0;
  strict = false;
  iterating = 0;
  batching = 0;
  pending?: Record<K, M2Event<K, T>>;
  derived?: Record<K, Derived<K, T>>;
  dependents?: Record<K, Set<K>>;

  constructor(entries?: I<readonly [K, T]>) {//                                   [CONSTRUCT]
    if (!entries) return;
    for (const [k, v] of entries) this.data[k] = v;
  }

  #rawHas(k: K): B { return Object.prototype.hasOwnProperty.call(this.data, k); }
  #rawGet(k: K): T | UD { return this.data[k]; }

  #emit(e: M2Event<K, T>): V {
    if (this.batching > 0) {
      const pending = this.pending![e.k];
      this.pending![e.k] = pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true };
      return;
    }

    this.#record(e);
    this.subs?.forEach((sub) => {
      try { sub(e); } catch (err: A) { process.emitWarning(err); }
    });
  }
  #record(e: M2Event<K, T>): V {
    if (!this.hist) return;

    this.hist.push(e);

    if (this.hist.length > this.histLimit) {
      this.hist.splice(0, this.hist.length - this.histLimit);
    }
  }
  #invalidate(k: K): V {
    const deps = this.dependents?.[k];
    if (!deps) return;

    for (const dk of deps) {
      const d = this.derived?.[dk];
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
    if (this.strict && this.iterating > 0) {
      throw new Error(`O2.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
    }
  }
  // IN ORDER TO MATCH MAPS CALL API FOR INSTANT AND EASY CONVERSIONS
  public get(k: K): T | UD { 
    const d = this.derived?.[k];
    if (d) { if (d.dirty || !d.has) return this.#computeDerived(k, d); return d.value; }
    return this.#rawGet(k);
   }
  public set(k: K, v: UF<T>): this { 
    this.#assertNotIterating("s");
    const next = rv(v);
    const has = this.h(k);
    const prev = has ? this.g(k) : undefined;
    this.data[k] = next;
    this.#invalidate(k);
    this.#emit({ op: "set", k, v: next, prev: prev as T, has });
    return this;
   }   // note: flipped to match Map's (key, value)
  public has(k: K): B { 
    const d = this.derived?.[k];
    if (d) { if (d.dirty || !d.has) this.#computeDerived(k, d); return true; }
    return this.#rawHas(k);
   }
  public delete(k: K): B { 
    this.#assertNotIterating("delete");
    if (!this.h(k)) return false;
    const prev = this.g(k) as T;
    delete this.data[k];
    this.#invalidate(k);
    this.#emit({ op: "del", k, prev, has: false });
    return true;
   }
  public clear(): V { 
    this.#assertNotIterating("clear");

    const entries = this.toArray();
    this.data = Object.create(null);//              one object swap, no per-key delete

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
    return Reflect.ownKeys(this.data).length as N;
  }
  public [Symbol.iterator](): Generator<[K, T]> { return this.entries(); }
  public on(fn: Sub<K, T>): () => V {//                                                  [ON]
    (this.subs ??= new Set()).add(fn);
    return () => this.off(fn);
  }
  public off(fn: Sub<K, T>): V {//                                                      [OFF]
    this.subs?.delete(fn);
  }
  public batch(fn: () => V): this {//                                                 [BATCH]
    this.batching++;
    this.pending ??= Object.create(null);

    try {
      fn();
    } finally {
      this.batching--;

      if (this.batching === 0) {
        const pending = this.pending!;
        this.pending = undefined;

        for (const k of Reflect.ownKeys(pending) as K[]) {
          const e = pending[k];
          this.#record(e);
          this.subs?.forEach((sub) => {
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

    this.data[k] = next;

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
    delete this.data[k];
    this.#invalidate(k);
    this.#emit({ op: "del", k, prev, has: false });
    return true;
  }
  public c(): V {//                                                                   [CLEAR]
    this.#assertNotIterating("clear");

    const entries = this.toArray();
    this.data = Object.create(null);//              one object swap, no per-key delete

    for (const [k, prev] of entries) {
      this.#invalidate(k);
      this.#emit({ op: "del", k, prev, has: false });
    }
  }
  public dv(k: K, deps: AR<K>, fn: (...vals: AR<T>) => T): this {//                [DERRIVED]
    (this.derived ??= Object.create(null))[k] = { deps, fn, dirty: true, has: false };

    for (const dep of deps) {
      const dependents = (this.dependents ??= Object.create(null));
      (dependents[dep] ??= new Set<K>()).add(k);
    }

    return this;
  }
  public dirty(...keys: AR<K>): this {//                                              [DIRTY]
    for (const k of keys) {
      const d = this.derived?.[k];

      if (d && !d.dirty) {
        d.dirty = true;
        this.#emit({ op: "dirty", k, has: d.has });
      }
    }

    return this;
  }
  public g(k: K): T | UD {//                                                           [GET]
    const d = this.derived?.[k];
    if (d) { if (d.dirty || !d.has) return this.#computeDerived(k, d); return d.value; }
    return this.#rawGet(k);
  }
  public h(k: K): B {//                                                                [HAS]
    const d = this.derived?.[k];
    if (d) { if (d.dirty || !d.has) this.#computeDerived(k, d); return true; }
    return this.#rawHas(k);
  }
  public $debug(limit: N = 256): this {//                                            [$DEBUG]
    this.hist ??= [];
    this.histLimit = limit;
    return this;
  }
  public $strict(): this {//                                                        [$STRICT]
    this.strict = true;
    return this;
  }
  public $history(): M2Event<K, T>[] {//                                          [$HISTORY]
    return this.hist ? [...this.hist] : [];
  }
  public $why(k: K): M2Event<K, T> | UD {//                                            [$WHY]
    if (!this.hist) return undefined;

    for (let i = this.hist.length - 1; i >= 0; i--) {
      if (Object.is(this.hist[i].k, k)) return this.hist[i];
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
    this.iterating++;

    try {
      for (const k of Reflect.ownKeys(this.data) as K[]) yield [k, this.data[k]];
    } finally {
      this.iterating--;
    }
  }
  public *keys(): Generator<K> {//                                                  [*KEYS]
    this.iterating++;

    try {
      for (const k of Reflect.ownKeys(this.data) as K[]) yield k;
    } finally {
      this.iterating--;
    }
  }
  public *values(): Generator<T> {//                                              [*VALUES]
    this.iterating++;

    try {
      for (const k of Reflect.ownKeys(this.data) as K[]) yield this.data[k];
    } finally {
      this.iterating--;
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
    return { ...(this.data as A) };
  }
  public snapshot(): OB<T> { return this.toObjectFast(); }//      handy name for hook/selector use
  public toJSON(): OB<T> { return this.toObjectFast(); }//     JSON.stringify(store) just works
  public select<G>(fn: (data: Readonly<Record<K, T>>) => G): G {//                   [SELECT]
    return fn(this.data);
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
    out.data = { ...obj } as Record<S, X[keyof X]>;//            direct copy, no per-key emits
    return out;
  }
  static fromEntries<K extends PropertyKey, T>(entries: I<readonly [K, T]>): O2<K, T> {//  [FROMENTIRES]
    return new O2(entries);
  }
  static from<K extends PropertyKey, T>(source: O2<K, T> | Readonly<Record<K, T>>): O2<K, T> {// [FROM]
    const out = new O2<K, T>();
    out.data = source instanceof O2 ? { ...source.data } : { ...source };
    return out;
  }
}
