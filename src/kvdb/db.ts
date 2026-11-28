//                                                                                                                     IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Worker } from "node:worker_threads";
import { mkdir, open, readFile, rm, stat} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { encodeUtf8, decodeUtf8 } from "./binary";
import { isWorkerError, NO_PAGE, OP_DELETE, OP_PUT, PAGE_SIZE, PendingBatch, PendingWorkerRequest, StoredEntry, WorkerRequestInput, type FastKVOptions, type PageWrite, type WorkerRequest, type WorkerResponse} from "./types";
import { decodeManifest, decodeRecordChain, decodeWalRecords, encodeFreePage, encodeManifest, encodeRecordChain, encodeWalRecord } from "./wal";

const workerPath = fileURLToPath( new URL("./storage-worker.ts", import.meta.url));
// #endregion
//                                                                                                                       TYPES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type MaybePromise<R = any> = R;
function MaybePromise(v) { return v !== undefined ? v : Promise.resolve(v);}
export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
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
// #endregion
//                                                                                                                          DB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class WAL_KVDB {
  //                                                                                                                   CONSTANTS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public r: S = "THE_WIZARD_ROOT_REG";
  public m: S = "SAURON_MASTER_REG";
  public s: S = "SAURON_SLAVE_REG";
  public k: S = "__SAURON__";
  public hot_cache: Record<string, unknown> = Object.create(null);
  ///hot_cache = new O2<S, A>();
  public waiters: Record<S, ST<(value: U) => V>> = Object.create(null);
  public entries: R<S, StoredEntry> = Object.create(null) ;
  public pending: R<S, PendingWorkerRequest> = Object.create(null);
  public dirtyPages: R<N, U8> = Object.create(null);
  readonly #options: Required<FastKVOptions>;
  #locks: R<S, LockEntry> = Object.create(null);  
  #ttlMap: R<S, N> = Object.create(null);
  #patternWaiters: Array<{pattern: S; cb: (key: S, value: A | UD) => V;}> = [];
  #directory!: S;
  #dataPath!: S;
  #walPath!: S;
  #manifestPath!: S;
  #worker?: Worker;
  #freePages: N[] = [];
  #pageCount = 0;
  #nextSequence = 1n;
  #checkpointSequence = 0n;
  #walSize = 0;
  #batch?: PendingBatch;
  #batchTimer?: NodeJS.Timeout;
  #writeQueue: P<U> = P.resolve();
  #closed = false;
  #_onError: (err: U) => V
  #ttl_active = false
  #ttl_minutes = 10

  public middlewares: NginMiddleware[] = []
  public _localChange = false
  public _channel?: BroadcastChannel
  protected memoryTimer?: RET<typeof setTimeout>
  protected touchMemory() {
    if (this.memoryTimer) clearTimeout(this.memoryTimer)
    this.memoryTimer = setTimeout(() => {
      //this.memory.clear()
      this.memoryTimer = undefined
    }, this.#ttl_minutes * 60 * 1000)
  }

  private async runBeforeGet(key: S) { for (const m of this.middlewares) { if (m.beforeGet) await m.beforeGet(key) }; }
  private async runAfterGet(key: S, value: U) { for (const m of this.middlewares) { if (m.afterGet) await m.afterGet(key, value) }; }
  private async runBeforeSet(key: S, value: U) { for (const m of this.middlewares) { if (m.beforeSet) await m.beforeSet(key, value) }; }
  private async runAfterSet(key: S, value: U) { for (const m of this.middlewares) { if (m.afterSet) await m.afterSet(key, value); } }
  private async runBeforeDelete(key: S) { for (const m of this.middlewares) { if (m.beforeDelete) await m.beforeDelete(key); } }
  private async runAfterDelete(key: S) { for (const m of this.middlewares) { if (m.afterDelete) await m.afterDelete(key); } }
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
    if (typeof BroadcastChannel !== 'undefined') {
      this._channel = new BroadcastChannel('DARKSIDE_STORAGE_SYNC')
      this._channel.onmessage = (e) => {
        if (this._localChange) { this._localChange = false; return; }

        const { key, value, storageType, fn } = e.data
        if (storageType !== 'mem') return

        if (fn === 'delete') {
          delete this.hot_cache[key];
          this.#_emit(key, undefined)
        } else {
          this.hot_cache[key] = value;
          this.#_emit(key, value)
        }
      }
    }
  }
  public _broadcast(key: S, value: S, storageType: S, fn: S, path?: S, domain?: S, options?: A) {
    this._localChange = true
    this._channel?.postMessage({ key, value, storageType, fn, path, domain, options })
  }
  private _withLock<T>(key: S, fn: () => T) {
    if (typeof navigator !== 'undefined' && navigator.locks) {
      return navigator.locks.request('mem_write_' + key, fn)
    }
    return fn()
  }
  // #endregion
  //                                                                                                                         API
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public get = (() => {
    const z = <T = A>(k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {  
      const cached = this.hot_cache[k];
      if (cached !== undefined) return MaybePromise(cached as T)
      else {
        const entry = this.entries[k]
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
          this.hot_cache[k] = decoded.value;
          return decoded.value as T;
        }))
      }    
    }
    z.many = async (keys: S[]): P<V> => {
      await Promise.all(Object.entries(keys).map(([k]) => this.get(k)))
    }
    z.hot = <T = A>(k: S,) => {  
     return this.hot_cache[k]
    }
    return z;
  })();
  public set = (() => {
    type ZOptions = { durable?: boolean; silent?: boolean };
    const z = (k: S, value: A, s: S = this.s, m: S = this.m, r: S = this.r, options: ZOptions = {} ): V => {
        this.hot_cache[k] = value;

        this.#resolveWaiters(k, value);
        this.#_emit(k, value);

        void this.#enqueueWrite(async () => {
            const valueBytes = typeof value === 'string'
                ? encodeUtf8(value)
                : value;

            const keyBytes = encodeUtf8(k);

            if (this.#closed) return;

            this.#applyPut(k, keyBytes, valueBytes);

            const record = encodeWalRecord({
                sequence: this.#nextSequence++,
                operation: OP_PUT,
                key: keyBytes,
                value: valueBytes
            });

            const durable = this.#resolveDurability(options.durable);
            const flushPromise = this.#appendToBatch(record, durable);

            if (
                options.durable === true ||
                this.#options.durability === 'strict'
            ) {
                await flushPromise;
            }
        });
    };
    z.many = (entries: Record<S, A>): V => {for (const k in entries) { z(k, entries[k]!); } };
    return z;
  })();
  public delete = (() => {
    const z =(key: S, options: { durable?: B } = {}): V=> {
      delete this.hot_cache[key];

      this.#resolveWaiters(key, undefined);
      this.#_emit(key, undefined);

      void this.#enqueueWrite(async () => {
          const keyBytes = encodeUtf8(key);

          if (this.#closed) return;
          if (!(key in this.entries)) return;

          this.#applyDelete(key);

          const record = encodeWalRecord({
              sequence: this.#nextSequence++,
              operation: OP_DELETE,
              key: keyBytes,
              value: new Uint8Array(0)
          });

          const durable = this.#resolveDurability(options.durable);
          const flushPromise = this.#appendToBatch(record, durable);

          if (
              options.durable === true ||
              this.#options.durability === 'strict'
          ) {
              await flushPromise;
          }
      });
    };
    z.many = (keys: S[]): V => { for (const k of keys) this.delete(k);  };
    return z;
  })();
	public update = (() => {
		const z = <T = A>(key: S, updater: A) => {
      const prev = this.hot_cache[key];

      if (prev !== undefined) {
          const next = typeof updater === "function" ? updater(prev) : updater; 
          this.hot_cache[key] = next;

          this.#resolveWaiters(key, next);
          this.#_emit(key, next);

          void this.#enqueueWrite(async () => {
              const valueBytes = typeof next === 'string' ? encodeUtf8(next) : next as Uint8Array;

              const keyBytes = encodeUtf8(key);

              if (this.#closed) return;

              this.#applyPut(key, keyBytes, valueBytes);

              const record = encodeWalRecord({
                  sequence: this.#nextSequence++,
                  operation: OP_PUT,
                  key: keyBytes,
                  value: valueBytes
              });

              const flushPromise = this.#appendToBatch(record, true);

              if (this.#options.durability === 'strict') {
                  await flushPromise;
              }
          });

          return MaybePromise(next as T);
      }

      return MaybePromise(
          this.get<T>(key).then((value) => {
              const next = updater(value);

              this.set(key, next as A);

              return next;
          })
      );
    };
		z.many = (entries: Record<S, A>): V => { for (const [k, v] of Object.entries(entries)) this.update(k, v);  };
		return z;
	})();
  public has(key: S): B { return Object.hasOwn(this.entries, key);}
  public hot<T = A>(k: S,) { return this.hot_cache[k]  }
  public hotMemory<T = A>(k: S,) { return this.hot_cache[k] }
  public hotSetMemory<T = A>(key: S, value: U): void {
    this.hot_cache[key] = value;

    this.#resolveWaiters(key, value as A);
    this.#_emit(key, value as A);
}
  public getMemory(key: S) {
    if (this.#ttl_active) this.touchMemory()
    if (!key) return null
    this.runBeforeGet(key)
    const v = this.hot_cache[key] ?? null
    this.runAfterGet(key, v)
    return v
  }
  public async getManyMemory(keys: S[]): P<A> {
    if (this.#ttl_active) this.touchMemory()
    const out: Record<S, U> = {}
    for (const k of keys) {
      const v = await this.getMemory(k)
      if (v !== null && v !== undefined) out[k] = v
    }
    return out
  }
  public setMemory(key: S, value: A) {
    if (this.#ttl_active) this.touchMemory(); if (!key) return;
    const wk = `__waiters_${key}`
    const ws = this.waiters[wk] as Set<(value: U) => V> | UD
    if (ws) { for (const resolve of ws) resolve(value); delete this.waiters[wk];  }
    this.runBeforeSet(key, value)
    this._withLock(key, () => { this.hot_cache[key] = value; })
    this.runAfterSet(key, value)
    this._broadcast(key, value, 'mem', 'set')
    this.#_emit(key, value)
    return value
  }
  public updateMemory<T = A>(key: S, updater: A ) {
    if (this.#ttl_active) this.touchMemory(); if (!key) return; if (!this.has(key)) return
    const wk = `__waiters_${key}`
    const ws = this.waiters[wk] as Set<(value: U) => V> | UD
    const prev = this.hot_cache[wk] as T; 
    const next = typeof updater === "function" ? updater(prev) : updater; 

    if (ws) { for (const resolve of ws) resolve(next);   delete this.waiters[wk];  }
    this.runBeforeSet(key, next)
    this._withLock(key, () => { this.hot_cache[key] = next; })
    this.runAfterSet(key, next)
    this._broadcast(key, next, 'mem', 'set')
    this.#_emit(key, next)
    return next
  }
  public async setManyMemory(entries: Record<S, A>): P<V> {
    if (this.#ttl_active) this.touchMemory()
    for (const [k, v] of Object.entries(entries)) { await this.setMemory(k, v); }
  }
  public deleteMemory(key: S) {
    if (this.#ttl_active) this.touchMemory()
    this.runBeforeDelete(key)
    this._withLock(key, () => { delete this.hot_cache[key]; })
    this.runAfterDelete(key)
  }
  public deleteManyMemory(keys: S[]) {
    if (this.#ttl_active) this.touchMemory()
    for (const k of keys) this.deleteMemory(k);
  }
  public subMemory<T>(key: S, cb: (value: T) => void): () => void {
    let set = this.waiters[key] as
        | Set<(value: unknown) => void>
        | undefined;

    if (!set) {
        set = new Set();
        this.waiters[key] = set as never;
    }

    set.add(cb as (value: unknown) => void);

    const cached = this.hot_cache[key];

    if (cached !== undefined) {
        try {
            cb(cached as T);
        } catch (error) {
            this.#_onError(error);
        }
    }

    return () => {
        const current = this.waiters[key] as
            | Set<(value: unknown) => void>
            | undefined;

        if (!current) return;

        current.delete(cb as (value: unknown) => void);

        if (current.size === 0) {
            delete this.waiters[key];
        }
    };
}

  public send = (() => {
    const z = () => {}
    z.many = () => {}
    return z;
  })();
  // #endregion
  //                                                                                                                         SUB
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  #_emit(key: S, value: A | UD): V {
    const set = this.waiters[key];

    if (set) {
        for (const cb of set) {
            try {
                cb(value);
            } catch (error) {
                this.#_onError(error);
            }
        }
    }

    for (const watcher of this.#patternWaiters) {
        if (this.#matchesPattern(key, watcher.pattern)) {
            try {
                watcher.cb(key, value);
            } catch (error) {
                this.#_onError(error);
            }
        }
    }
  }
  #_peek(key: S): A {
    const v = this.hot_cache[key]
    return v === undefined ? undefined : v
  }
  #resolveWaiters(key: S, value: A | UD): V {
    const waiterKey = `__waiters_${key}`;
    const set = this.waiters[waiterKey] as | Set<(value: A | UD) => V> | UD;
    if (!set) return;
    delete this.waiters[waiterKey];
    for (const resolve of set) {resolve(value);}
  }
  #matchesPattern(key: S, pattern: S): B {
    if (pattern === '*') return true;

    if (pattern.endsWith('*')) {
        return key.startsWith(pattern.slice(0, -1));
    }

    return key === pattern;
  }
  public subscribe<T = A>(key: S, cb: Subscriber<T>): Teardown {
    let set = this.waiters[key];

    if (!set) {
        set = new Set();
        this.waiters[key] = set;
    }

    set.add(cb as (value: unknown) => void);

    const cached = this.hot_cache[key];

    if (cached !== undefined) {
        try {
            cb(cached as T);
        } catch (error) {
            this.#_onError(error);
        }
    }

    return () => {
        const current = this.waiters[key];

        if (!current) return;

        current.delete(cb as (value: unknown) => void);

        if (current.size === 0) {
            delete this.waiters[key];
        }
    };
  }
  public select<T = A, Selected = A>(key: S, selector: Selector<T | undefined, Selected>, cb: Subscriber<Selected>): Teardown {
      let last: Selected | undefined;
      let first = true;

      return this.subscribe<T>(key, (value) => {
          const next = selector(value);

          if (first || !Object.is(last, next)) {
              first = false;
              last = next;
              cb(next);
          }
      });
  }
  public waitFor<T = A>(key: S): P<T> {
      const existing = this.hot_cache[key];
      if (existing !== undefined) { return Promise.resolve(existing as T); }
      return new Promise<T>((resolve) => {
          const waiterKey = `__waiters_${key}`;
          let set = this.waiters[waiterKey] as | Set<(value: T) => void> | undefined;
          if (!set) {
              set = new Set();
              this.waiters[waiterKey] = set as never;
          }

          set.add(resolve);
      });
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
      // const oldEntries = new O2(this.entries);
      // const oldValues = new O2(this.hot_cache);
      const oldEntries = Object.assign(Object.create(null), this.entries);
      const oldValues = Object.assign(Object.create(null), this.hot_cache);
      this.entries = Object.create(null);
      // this.hot_cache.clear();
      this.hot_cache = Object.create(null);
      this.#freePages = [];
      this.dirtyPages = Object.create(null);
      this.#pageCount = 0;
      for (const [key, entry] of oldEntries) {
        const value = oldValues.get(key);
        if (!value) continue;
       // this.hot_cache.set(key, value);
       this.hot_cache[key] = value;
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

        this.entries[key] = {
          pageNumber: chain.pageNumbers[0]!,
          pageNumbers: chain.pageNumbers
      };

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
      keys: this.entries.size,
      pages: this.#pageCount,
      freePages: this.#freePages.length,
      dirtyPages: Object.keys(this.dirtyPages).length,
      walBytes: this.#walSize,
      checkpointSequence: this.#checkpointSequence,
      nextSequence: this.#nextSequence,
      durability: this.#options.durability
    };
  }
  #resolveDurability(requested: B | UD  ): B {
    if (requested !== undefined) { return requested; }
  
    return (
      this.#options.durability === "group" ||
      this.#options.durability === "strict"
    );
  }
  #applyPut( key: string, keyBytes: Uint8Array, valueBytes: Uint8Array, replay = false ): void {
    const previous = this.entries[key]

    const chain = encodeRecordChain(
      keyBytes,
      valueBytes,
      () => {
        const reused = this.#freePages.pop();

        if (reused !== undefined) { return reused; }

        return this.#pageCount++;
      }
    );

    if (previous) {
      for (const pageNumber of previous.pageNumbers) {
        this.dirtyPages[pageNumber] = encodeFreePage();

        this.#freePages.push(pageNumber);
      }
    }

    for (let index = 0; index < chain.pages.length; index++) {this.dirtyPages[chain.pageNumbers[index]!] = chain.pages[index]!;  }
  
  this.entries[key] = { pageNumber: chain.pageNumbers[0]!, pageNumbers: chain.pageNumbers  };
   // this.hot_cache.set(key, valueBytes);
    if (replay) {this.#nextSequence = this.#nextSequence; }
  }
  #applyDelete(key: string, replay = false ): void {
    const previous = this.entries[key]
    if (!previous) return;
    for (const pageNumber of previous.pageNumbers) {
      this.dirtyPages[pageNumber] = encodeFreePage();
      this.#freePages.push(pageNumber);
    }

    delete this.entries[key];
    //this.hot_cache.delete(key);
    delete this.hot_cache[key];
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
  #appendToBatch( record: Uint8Array, durable: B  ): P<V> {
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
      this.pending[id] = {
        resolve,
        reject
    };

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

        this.entries[key] = {
          pageNumber,
          pageNumbers
      };

        //this.hot_cache.set(key, decoded.value);
        this.hot_cache[key] = decoded.value;
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

    for (const pageNumber in this.dirtyPages) {
      pages.push({
          pageNumber: Number(pageNumber),
          buffer: this.dirtyPages[pageNumber]!.slice()
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

    this.dirtyPages = Object.create(null);
    this.#checkpointSequence = this.#nextSequence - 1n;
    this.#walSize = 0;
  }
  async #startWorker(): Promise<void> {
    this.#worker = new Worker(workerPath);
    this.#worker.on("message", (response: WorkerResponse) => {
      const pending = this.pending[response.id]
      if (!pending) return;
      delete this.pending[response.id];
      if (isWorkerError(response)) {
        pending.reject(new Error(response.error));
        return;
      }
      pending.resolve(response);
    });
    this.#worker.on("error", (error) => {
      for (const id in this.pending) {
        this.pending[id]!.reject(error);
    }
      this.pending = Object.create(null);
    });
    await this.#workerRequest({
      kind: "init",
      dataPath: this.#dataPath,
      walPath: this.#walPath
    });
  }
  // #endregion
  //                                                                                                                       WATCH
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public watchMany(keys: S[], handler: (key: S, value: A | undefined) => void): { values: Record<S, A | undefined>; teardown: Teardown;} {
    const values: Record<S, A | undefined> = Object.create(null);
    const teardowns: Teardown[] = [];

    for (const key of keys) {
        values[key] = this.hot_cache[key];

        teardowns.push(
            this.subscribe(key, (value) => {
                handler(key, value);
            })
        );

        if (values[key] === undefined) {
            void this.get(key);
        }
    }

    return {
        values,
        teardown: () => {
            for (const teardown of teardowns) {
                if (typeof teardown === 'function') {
                    teardown();
                }
            }
        }
    };
  }
  public watchPattern(pattern: S, handler: (key: S, value: A | UD) => V): Teardown {
      const watcher = {
          pattern,
          cb: handler
      };

      this.#patternWaiters.push(watcher);

      return () => {
          const index = this.#patternWaiters.indexOf(watcher);

          if (index !== -1) {
              this.#patternWaiters.splice(index, 1);
          }
      };
  }
  public watchPrefix(prefix: S, handler: (key: S, value: A | UD) => V): Teardown {
      return this.watchPattern(`${prefix}*`, handler);
  }
  public watchAll(handler: (key: S, value: A | UD) => V): Teardown {
      return this.watchPattern('*', handler);
  }
  // #endregion
  //                                                                                                              SET IF CHANGED
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async setIfChanged(key: S, value: A): Promise<boolean> {
      const current = await this.get(key);

      if (this.#valueEquals(current as U, value as U)) {
          return false;
      }

      this.set(key, value);

      return true;
  }
  public expire(key: S, ms: number): void {
      const expiresAt = Date.now() + ms;

      this.#ttlMap[key] = expiresAt;

      setTimeout(() => {
          const expires = this.#ttlMap[key];

          if (expires !== undefined && expires <= Date.now()) {
              this.delete(key);
              delete this.#ttlMap[key];
          }
      }, ms + 5);
  }
  public expireAt(key: S, timestamp: number): void {
      const ms = Math.max(0, timestamp - Date.now());

      this.expire(key, ms);
  }
  // #endregion
  //                                                                                                                     PERSIST
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public persist(key: S): void {
    delete this.#ttlMap[key];
  }
  public touch(key: S, ms?: number): void {
      const expiresAt = this.#ttlMap[key];

      if (expiresAt === undefined) return;

      const remaining = expiresAt - Date.now();

      this.expire(key, ms ?? remaining);
  }
  // #endregion
  //                                                                                                                        LOCK
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async lock(key: S, ttlMs = 10_000): Promise<string | null> {
      const now = Date.now();
      const existing = this.#locks[key];

      if (existing && existing.expiresAt > now) {
          return null;
      }

      const owner = `${key}:${now}:${Math.random().toString(36).slice(2)}`;

      this.#locks[key] = {
          owner,
          expiresAt: now + ttlMs
      };

      setTimeout(() => {
          const current = this.#locks[key];

          if (
              current &&
              current.owner === owner &&
              current.expiresAt <= Date.now()
          ) {
              delete this.#locks[key];
          }
      }, ttlMs + 5);

      return owner;
  }
  public unlock(key: S, owner: S): boolean {
      const current = this.#locks[key];

      if (!current || current.owner !== owner) return false;

      delete this.#locks[key];

      return true;
  }
  public renewLock(key: S, owner: S, ttlMs = 10_000): boolean {
      const current = this.#locks[key];

      if (!current || current.owner !== owner) return false;

      current.expiresAt = Date.now() + ttlMs;

      return true;
  }
  public lockApi = {
      acquire: (key: S, ttlMs?: number) => this.lock(key, ttlMs),
      release: (key: S, owner: S) => this.unlock(key, owner),
      renew: (key: S, owner: S, ttlMs?: number) =>
          this.renewLock(key, owner, ttlMs)
  };
  // #endregion
  //                                                                                                                TRANSACTIONS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  #txDepth = 0;
  public transaction<T = U>(fn: (db: WAL_KVDB) => Promise<T> | T  ): Promise<T> | T {
      return fn(this);
  }
  // #endregion
  //                                                                                                                       BATCH
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public batch(entries: Array<{ key: S; value: A }>): V {
      for (const entry of entries) {
          this.set(entry.key, entry.value);
      }
  }
  // #endregion
  //                                                                                                                        LIVE
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public live<T extends Record<S, unknown>>(key: S, initial: T = Object.create(null) as T): T {
      let current: T = { ...initial };

      const teardown = this.subscribe<T>(key, (value) => {
          if (value && typeof value === 'object') {
              current = {
                  ...current,
                  ...(value as Partial<T>)
              };
          }
      });

      const persist = (): void => {
          this.set(key, current as A);
      };

      const proxy = new Proxy(current, {
          set: (target, property, value) => {
              target[property as keyof T] = value;
              current = { ...target };
              persist();
              return true;
          },
          get: (target, property) => {
              return target[property as keyof T];
          }
      }) as T;

      const existing = this.hot_cache[key];

      if (existing && typeof existing === 'object') {
          Object.assign(proxy, existing);
      } else {
          persist();
      }

      return proxy;
  }
  public useLive() { }
  // #endregion
  //                                                                                                             COMPUTED VALUES
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public computed<T = A>( key: S, deps: S[], computeFn: (...values: A[]) => T): Teardown {
    const update = (): void => {
        Promise.all(deps.map((dep) => this.get(dep)))
            .then((values) => {
                const result = computeFn(...(values as A[]));

                this.set(key, result as A);
            })
            .catch((error) => {
                this.#_onError(error);
            });
    };

    const teardowns: Teardown[] = deps.map((dep) =>
        this.subscribe(dep, () => {
            update();
        })
    );

    update();

    return () => {
        for (const teardown of teardowns) {
            if (typeof teardown === 'function') {
                teardown();
            }
        }
    };
  }
  // #endregion
  //                                                                                                                    SNAPSHOT
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public snapshot() {
      return {
          memory: { ...this.hot_cache },
          persistence: { ...this.hot_cache },
      };
  }
  // #endregion
  //                                                                                                                     RESTORE
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public restore(snapshot: Snapshot): void {
      this.hot_cache = Object.create(null);

      Object.assign(this.hot_cache, snapshot.memory);
  }
  // #endregion
  //                                                                                                                      EXPORT
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async export(prefix?: S): Promise<Record<S, A>> {
    const out: Record<S, A> = Object.create(null);

    for (const key of Object.keys(this.entries)) {
        if (prefix && !key.startsWith(prefix)) continue;

        out[key] = (await this.get(key)) as A;
    }

    return out;
  }
  // #endregion
  //                                                                                                                      IMPORT
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async import(data: Record<S, A>,strategy: 'overwrite' | 'skip' | 'merge' = 'overwrite'): Promise<void> {
    for (const key in data) {
        const incoming = data[key]!;
        const existing = this.hot_cache[key];

        if (strategy === 'skip' && existing !== undefined) continue;

        if (
            strategy === 'merge' &&
            existing &&
            typeof existing === 'object' &&
            incoming &&
            typeof incoming === 'object'
        ) {
            this.set(key, {
                ...(existing as A),
                ...(incoming as A)
            });
        } else {
            this.set(key, incoming);
        }
    }
  }
  // #endregion
  //                                                                                                                  MIDDLEWARE
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public use(mw:NginMiddleware): V { this.middlewares.push(mw) }
  // #endregion
  //                                                                                                                    INDEXING
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  #indexes: Record<S, {config: IndexConfig; data: Map<S, Set<S>>;}> = Object.create(null);
  #indexKey(indexKey: S, value: U): S {
    if (typeof value === 'string') {
        return `${indexKey}:${value}`;
    }

    if (value instanceof Uint8Array) {
        let hex = '';

        for (const byte of value) {
            hex += byte.toString(16).padStart(2, '0');
        }

        return `${indexKey}:${hex}`;
    }

    return `${indexKey}:`;
  }
  #getFieldValue(value: U, field: S): U {
    if (typeof value === 'string') {
        return value;
    }

    if (value instanceof Uint8Array) {
        return value;
    }

    return undefined as U;
  }
  public index = {
      create: async (name: S, config: IndexConfig): Promise<void> => {
          const data = new Map<S, Set<S>>();

          for (const key of Object.keys(this.entries)) {
              const value = await this.get(key);

              if (value === undefined) continue;

              for (const indexKey of config.keys) {
                  const fieldValue = this.#getFieldValue(value as U, indexKey);
                  const indexEntry = this.#indexKey(indexKey, fieldValue);

                  let keys = data.get(indexEntry);

                  if (!keys) {
                      keys = new Set();
                      data.set(indexEntry, keys);
                  }

                  keys.add(key);
              }
          }

          this.#indexes[name] = {
              config,
              data
          };
      },

      query: async (
          name: S,
          filter: IndexFilter
      ): Promise<Array<{ key: S; value: U }>> => {
          const index = this.#indexes[name];

          if (!index) return [];

          let resultKeys: Set<S> | null = null;

          for (const field in filter) {
              const indexEntry = this.#indexKey(field, filter[field] as U);
              const keys = index.data.get(indexEntry) ?? new Set<S>();

              if (resultKeys === null) {
                  resultKeys = new Set(keys);
              } else {
                  const next = new Set<S>();

                  for (const key of resultKeys) {
                      if (keys.has(key)) next.add(key);
                  }

                  resultKeys = next;
              }
          }

          const out: Array<{ key: S; value: U }> = [];

          if (!resultKeys) return out;

          for (const key of resultKeys) {
              out.push({
                  key,
                  value: (await this.get(key)) as U
              });
          }

          return out;
      }
  };
  // #endregion
  //                                                                                                                       QUERY
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async query(expr: QueryExpr): Promise<Array<{ key: S; value: A }>> {
    const out: Array<{ key: S; value: A }> = [];

    for (const key of Object.keys(this.entries)) {
        const value = await this.get(key);

        if (this.#matchesQuery(value, expr)) {
            out.push({
                key,
                value: value as A
            });
        }
    }

    return out;
  }
  #matchesQuery(value: U, expr: QueryExpr): boolean {
      if (!expr) return true;

      if (expr.type === 'eq') {
          return this.#valueEquals(value, expr.value);
      }

      if (expr.type === 'and') {
          return expr.exprs.every((child) =>
              this.#matchesQuery(value, child)
          );
      }

      if (expr.type === 'or') {
          return expr.exprs.some((child) =>
              this.#matchesQuery(value, child)
          );
      }

      return false;
  }
  #valueEquals(a: U, b: U): boolean {
      if (a === b) return true;

      if (a instanceof Uint8Array && b instanceof Uint8Array) {
          if (a.length !== b.length) return false;

          for (let i = 0; i < a.length; i++) {
              if (a[i] !== b[i]) return false;
          }

          return true;
      }

      return false;
  }
  public getMemoryEntries(): Array<{ key: S; value: U }> {
    const out: Array<{ key: S; value: U }> = [];

    for (const key in this.hot_cache) {
        out.push({
            key,
            value: this.hot_cache[key] as U
        });
    }

    return out;
  }
  #aux?: WAL_KVDB;
  public async openFile(filePath: S): Promise<void> {
    if (this.#aux) return;

    this.#aux = new WAL_KVDB(filePath);

    await this.#aux.open();
  }
  public async closeFile(): Promise<void> {
      if (!this.#aux) return;
  
      await this.#aux.close();
      this.#aux = undefined;
  }
  public getTables(): S[] {
      return [this.k];
  }
  public async getSchema() {
      const fields = new Map<S, Set<S>>();

      for (const key of Object.keys(this.entries)) {
          const value = await this.get(key);

          if (!value || typeof value !== 'object') continue;

          for (const field in value as Record<S, unknown>) {
              const type = typeof (value as Record<S, unknown>)[field];

              if (!fields.has(field)) fields.set(field, new Set());

              fields.get(field)!.add(type);
          }
      }

      const schema: Record<S, S[]> = Object.create(null);

      for (const [field, types] of fields) {
          schema[field] = Array.from(types);
      }

      return schema;
  }
  public async queryTable(): Promise<Array<{ key: S; value: U }>> {
      const out: Array<{ key: S; value: U }> = [];

      for (const key of Object.keys(this.entries)) {
          out.push({
              key,
              value: (await this.get(key)) as U
          });
      }

      return out;
  }
  public async updateCell(key: S, col: S, value: U): Promise<void> {
      const current = (await this.get(key)) as Record<S, U> | undefined;
      const row = current && typeof current === 'object'
          ? { ...current }
          : Object.create(null);

      row[col] = value;

      this.set(key, row as A);
  }
  public insertRow(key: S, cols: S[], values: U[]): void {
      const row: Record<S, U> = Object.create(null);

      for (let i = 0; i < cols.length; i++) {
          row[cols[i]!] = values[i] as U;
      }

      this.set(key, row as A);
  }
  public deleteRow(key: S): void {
      this.delete(key);
  }
  public async addColumn(col: S, value: U = null as U): Promise<void> {
      for (const key of Object.keys(this.entries)) {
          const current = (await this.get(key)) as Record<S, U> | undefined;

          if (!current || typeof current !== 'object') continue;
          if (col in current) continue;

          this.set(key, {
              ...current,
              [col]: value
          } as A);
      }
  }
  // #endregion
}
// #endregion
