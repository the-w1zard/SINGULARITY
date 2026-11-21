//                                                                                                                     IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Worker } from "node:worker_threads";
import { mkdir, open, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { encodeRecordChain, decodeRecordChain, encodeFreePage } from "./pages";
import { decodeWalRecords, encodeWalRecord } from "./wal";
import { decodeManifest, encodeManifest } from "./manifest";
import { encodeUtf8, decodeUtf8 } from "./binary";
import { isWorkerError, NO_PAGE, OP_DELETE, OP_PUT, PAGE_SIZE } from "./types";
const workerPath = fileURLToPath(new URL("./storage-worker.ts", import.meta.url));
export function MaybePromise(v) {
    return v !== undefined ? v : Promise.resolve(v);
}
export const rv = (v) => typeof v === "function" ? v() : v;
export const sameValue = (a, b) => Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
export class O2 {
    #data = Object.create(null);
    f = this.forEachKV; //                                                    [FOR EACH]
    #subs;
    #hist;
    #histLimit = 0;
    #strict = false;
    #iterating = 0;
    #batching = 0;
    #pending;
    #derived;
    #dependents;
    constructor(entries) {
        if (!entries)
            return;
        for (const [k, v] of entries)
            this.#data[k] = v;
    }
    #rawHas(k) { return Object.prototype.hasOwnProperty.call(this.#data, k); }
    #rawGet(k) { return this.#data[k]; }
    #emit(e) {
        if (this.#batching > 0) {
            const pending = this.#pending[e.k];
            this.#pending[e.k] = pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true };
            return;
        }
        this.#record(e);
        this.#subs?.forEach((sub) => {
            try {
                sub(e);
            }
            catch (err) {
                process.emitWarning(err);
            }
        });
    }
    #record(e) {
        if (!this.#hist)
            return;
        this.#hist.push(e);
        if (this.#hist.length > this.#histLimit) {
            this.#hist.splice(0, this.#hist.length - this.#histLimit);
        }
    }
    #invalidate(k) {
        const deps = this.#dependents?.[k];
        if (!deps)
            return;
        for (const dk of deps) {
            const d = this.#derived?.[dk];
            if (!d || d.dirty)
                continue;
            d.dirty = true;
            this.#emit({ op: "dirty", k: dk, has: d.has });
        }
    }
    #computeDerived(k, d) {
        const vals = d.deps.map((dep) => this.g(dep));
        const next = d.fn(...vals);
        const had = d.has;
        const prev = d.value;
        d.value = next;
        d.has = true;
        d.dirty = false;
        if (!had || !sameValue(prev, next)) {
            this.#emit({ op: had ? "upd" : "set", k, v: next, prev: prev, has: true });
        }
        return next;
    }
    #assertNotIterating(op) {
        if (this.#strict && this.#iterating > 0) {
            throw new Error(`O2.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
        }
    }
    // IN ORDER TO MATCH MAPS CALL API FOR INSTANT AND EASY CONVERSIONS
    get(k) {
        const d = this.#derived?.[k];
        if (d) {
            if (d.dirty || !d.has)
                return this.#computeDerived(k, d);
            return d.value;
        }
        return this.#rawGet(k);
    }
    set(k, v) {
        this.#assertNotIterating("s");
        const next = rv(v);
        const has = this.h(k);
        const prev = has ? this.g(k) : undefined;
        this.#data[k] = next;
        this.#invalidate(k);
        this.#emit({ op: "set", k, v: next, prev: prev, has });
        return this;
    } // note: flipped to match Map's (key, value)
    has(k) {
        const d = this.#derived?.[k];
        if (d) {
            if (d.dirty || !d.has)
                this.#computeDerived(k, d);
            return true;
        }
        return this.#rawHas(k);
    }
    delete(k) {
        this.#assertNotIterating("delete");
        if (!this.h(k))
            return false;
        const prev = this.g(k);
        delete this.#data[k];
        this.#invalidate(k);
        this.#emit({ op: "del", k, prev, has: false });
        return true;
    }
    clear() {
        this.#assertNotIterating("clear");
        const entries = this.toArray();
        this.#data = Object.create(null); //              one object swap, no per-key delete
        for (const [k, prev] of entries) {
            this.#invalidate(k);
            this.#emit({ op: "del", k, prev, has: false });
        }
    }
    forEach(fn) { return this.forEachKV(fn); }
    get length() { return this.size; }
    static groupBy(items, fn) {
        const out = new O2();
        let i = 0;
        for (const item of items)
            out.getOrM(() => new A2(), fn(item, i++)).push(item);
        return out;
    }
    get size() {
        return Reflect.ownKeys(this.#data).length;
    }
    [Symbol.iterator]() { return this.entries(); }
    on(fn) {
        (this.#subs ??= new Set()).add(fn);
        return () => this.off(fn);
    }
    off(fn) {
        this.#subs?.delete(fn);
    }
    batch(fn) {
        this.#batching++;
        this.#pending ??= Object.create(null);
        try {
            fn();
        }
        finally {
            this.#batching--;
            if (this.#batching === 0) {
                const pending = this.#pending;
                this.#pending = undefined;
                for (const k of Reflect.ownKeys(pending)) {
                    const e = pending[k];
                    this.#record(e);
                    this.#subs?.forEach((sub) => {
                        try {
                            sub(e);
                        }
                        catch (err) {
                            process.emitWarning(err);
                        }
                    });
                }
            }
        }
        return this;
    }
    s(v, k) {
        this.#assertNotIterating("s");
        const next = rv(v);
        const has = this.h(k);
        const prev = has ? this.g(k) : undefined;
        this.#data[k] = next;
        this.#invalidate(k);
        this.#emit({ op: "set", k, v: next, prev: prev, has });
        return this;
    }
    mutate(k, fn) {
        this.#assertNotIterating("mutate");
        if (!this.h(k))
            return undefined;
        const old = this.g(k);
        const next = fn(old, true);
        this.s(next, k);
        return next;
    }
    d(k) {
        this.#assertNotIterating("delete");
        if (!this.h(k))
            return false;
        const prev = this.g(k);
        delete this.#data[k];
        this.#invalidate(k);
        this.#emit({ op: "del", k, prev, has: false });
        return true;
    }
    c() {
        this.#assertNotIterating("clear");
        const entries = this.toArray();
        this.#data = Object.create(null); //              one object swap, no per-key delete
        for (const [k, prev] of entries) {
            this.#invalidate(k);
            this.#emit({ op: "del", k, prev, has: false });
        }
    }
    dv(k, deps, fn) {
        (this.#derived ??= Object.create(null))[k] = { deps, fn, dirty: true, has: false };
        for (const dep of deps) {
            const dependents = (this.#dependents ??= Object.create(null));
            (dependents[dep] ??= new Set()).add(k);
        }
        return this;
    }
    dirty(...keys) {
        for (const k of keys) {
            const d = this.#derived?.[k];
            if (d && !d.dirty) {
                d.dirty = true;
                this.#emit({ op: "dirty", k, has: d.has });
            }
        }
        return this;
    }
    g(k) {
        const d = this.#derived?.[k];
        if (d) {
            if (d.dirty || !d.has)
                return this.#computeDerived(k, d);
            return d.value;
        }
        return this.#rawGet(k);
    }
    h(k) {
        const d = this.#derived?.[k];
        if (d) {
            if (d.dirty || !d.has)
                this.#computeDerived(k, d);
            return true;
        }
        return this.#rawHas(k);
    }
    $debug(limit = 256) {
        this.#hist ??= [];
        this.#histLimit = limit;
        return this;
    }
    $strict() {
        this.#strict = true;
        return this;
    }
    $history() {
        return this.#hist ? [...this.#hist] : [];
    }
    $why(k) {
        if (!this.#hist)
            return undefined;
        for (let i = this.#hist.length - 1; i >= 0; i--) {
            if (Object.is(this.#hist[i].k, k))
                return this.#hist[i];
        }
        return undefined;
    }
    $diff(other) {
        const add = [];
        const del = [];
        const chg = [];
        for (const [k, v] of this) {
            if (!other.h(k)) {
                add.push([k, v]);
            }
            else if (!Object.is(other.g(k), v)) {
                chg.push([k, v, other.g(k)]);
            }
        }
        for (const k of other.keys()) {
            if (!this.h(k))
                del.push(k);
        }
        return { add, del, chg };
    }
    *entries() {
        this.#iterating++;
        try {
            for (const k of Reflect.ownKeys(this.#data))
                yield [k, this.#data[k]];
        }
        finally {
            this.#iterating--;
        }
    }
    *keys() {
        this.#iterating++;
        try {
            for (const k of Reflect.ownKeys(this.#data))
                yield k;
        }
        finally {
            this.#iterating--;
        }
    }
    *values() {
        this.#iterating++;
        try {
            for (const k of Reflect.ownKeys(this.#data))
                yield this.#data[k];
        }
        finally {
            this.#iterating--;
        }
    }
    swap(value, key) {
        const next = rv(value);
        const previous = this.h(key) ? this.g(key) : next; //       h()/g() kept derived-aware
        this.s(next, key);
        return previous;
    }
    take(k) {
        const value = this.g(k);
        this.d(k);
        return value;
    }
    peek(k) {
        return this.h(k) ? this.g(k) : false;
    }
    getOrI(value, k) {
        if (this.h(k))
            return this.g(k);
        const next = rv(value);
        this.s(next, k);
        return next;
    }
    getOrM(make, k) {
        if (this.h(k))
            return this.g(k);
        const value = make();
        this.s(value, k);
        return value;
    }
    update(k, fn, initial) {
        const has = this.h(k);
        const old = has
            ? this.g(k)
            : initial === undefined
                ? undefined
                : rv(initial);
        const next = rv(fn(old, has));
        this.s(next, k);
        return next;
    }
    setIfAbsent(value, k) {
        if (this.h(k))
            return false;
        const next = rv(value);
        this.s(next, k);
        return true;
    }
    setIfPresent(value, k) {
        if (!this.h(k))
            return false;
        const next = rv(value);
        this.s(next, k);
        return true;
    }
    replace(value, k) {
        const next = rv(value);
        return this.setIfPresent(next, k);
    }
    inc(key, by = 1) {
        return this.update(key, (old) => ((old ?? 0) + by), 0);
    }
    touch(k) {
        return this.setIfAbsent(undefined, k);
    }
    mapValues(fn) {
        const out = new O2();
        for (const [k, v] of this)
            out.s(fn(v, k), k);
        return out;
    }
    mapKeys(fn) {
        const out = new O2();
        for (const [k, v] of this)
            out.s(v, fn(v, k));
        return out;
    }
    filterKV(pred) {
        for (const [k, v] of this) {
            if (!pred(v, k))
                this.d(k);
        }
        return this;
    }
    filterKeys(pred) {
        return this.filterKV(pred);
    }
    partition(pred) {
        const yes = new O2();
        const no = new O2();
        for (const [k, v] of this) {
            (pred(v, k) ? yes : no).s(v, k);
        }
        return [yes, no];
    }
    find(pred) {
        for (const [k, v] of this) {
            if (pred(v, k))
                return [k, v];
        }
        return undefined;
    }
    findKey(pred) {
        for (const [k, v] of this) {
            if (pred(v, k))
                return k;
        }
        return undefined;
    }
    findValue(pred) {
        for (const [k, v] of this) {
            if (pred(v, k))
                return v;
        }
        return undefined;
    }
    some(pred) {
        for (const [k, v] of this) {
            if (pred(v, k))
                return true;
        }
        return false;
    }
    every(pred) {
        for (const [k, v] of this) {
            if (!pred(v, k))
                return false;
        }
        return true;
    }
    reduce(fn, initial) {
        let acc = initial;
        for (const [k, v] of this)
            acc = fn(acc, v, k);
        return acc;
    }
    forEachKV(fn) {
        for (const [k, v] of this)
            fn(v, k, this);
    }
    invert() {
        const out = new O2();
        for (const [k, v] of this)
            out.s(k, v);
        return out;
    }
    unique(fn) {
        const out = new O2();
        for (const [k, v] of this) {
            const key = (fn ? fn(v, k) : v);
            if (!out.h(key))
                out.s(v, key);
        }
        return out;
    }
    pick(keys) {
        const out = new O2();
        for (const k of keys) {
            if (this.h(k))
                out.s(this.g(k), k);
        }
        return out;
    }
    omit(keys) {
        const omitted = new Set(keys);
        const out = new O2();
        for (const [k, v] of this) {
            if (!omitted.has(k))
                out.s(v, k);
        }
        return out;
    }
    merge(other) {
        for (const [k, v] of other) {
            const n = rv(v);
            this.s(n, k);
        }
        return this;
    }
    clearAndSet(entries) {
        return this.batch(() => {
            this.c(); //                         (M2's version calls the native Map.clear() here
            for (const [k, v] of entries) { //  — silent, no del events; c() is almost
                const n = rv(v); //  certainly what was intended, so I used it)
                this.s(n, k);
            }
        });
    }
    toObject() {
        const out = {};
        for (const [k, v] of this)
            out[k] = v;
        return out;
    }
    toObjectFast() {
        return { ...this.#data };
    }
    snapshot() { return this.toObjectFast(); } //      handy name for hook/selector use
    toJSON() { return this.toObjectFast(); } //     JSON.stringify(store) just works
    select(fn) {
        return fn(this.#data);
    }
    patch(partial) {
        return this.batch(() => {
            for (const k of Reflect.ownKeys(partial))
                this.s(partial[k], k);
        });
    }
    toValues() { return A2.from(this.values()); } //                 [TOVALUES]
    toKeys() { return A2.from(this.keys()); } //                       [TOKEYS]
    toArray() { return A2.from(this.entries()); } //             [TOARRAY]
    group(fn) {
        const out = new O2();
        for (const [k, v] of this)
            out.getOrM(() => new A2(), fn(v, k)).push(v);
        return out;
    }
    clone() {
        return new O2(this);
    }
    equals(other) {
        if (other === this)
            return true;
        if (this.size !== other.size)
            return false;
        for (const [k, v] of this) {
            if (!other.h(k) || !sameValue(other.g(k), v))
                return false;
        }
        return true;
    }
    static fromObject(obj) {
        const out = new O2();
        out.#data = { ...obj }; //            direct copy, no per-key emits
        return out;
    }
    static fromEntries(entries) {
        return new O2(entries);
    }
    static from(source) {
        const out = new O2();
        out.#data = source instanceof O2 ? { ...source.#data } : { ...source };
        return out;
    }
}
export class A2 extends Array {
    constructor(...items) {
        super(...items);
        Object.setPrototypeOf(this, new.target.prototype);
    }
    static get [Symbol.species]() {
        return Array;
    }
    static from(arrayLike) {
        return new A2(...Array.from(arrayLike));
    }
    static of(...items) {
        return new A2(...items);
    }
    first() {
        return this[0];
    }
    last() {
        return this[this.length - 1];
    }
    isEmpty() {
        return this.length === 0;
    }
    isNotEmpty() {
        return this.length !== 0;
    }
    clear() {
        this.length = 0;
        return this;
    }
    clone() {
        return new A2(...this);
    }
    unique() {
        return new A2(...new Set(this));
    }
    compact() {
        return new A2(...this.filter((value) => value != null));
    }
    take(count) {
        return new A2(...this.slice(0, Math.max(0, count)));
    }
    takeLast(count) {
        return new A2(...this.slice(Math.max(0, this.length - Math.max(0, count))));
    }
    drop(count) {
        return new A2(...this.slice(Math.max(0, count)));
    }
    dropLast(count) {
        return new A2(...this.slice(0, Math.max(0, this.length - Math.max(0, count))));
    }
    chunk(size) {
        if (!Number.isInteger(size) || size < 1) {
            throw new RangeError("Chunk size must be a positive integer.");
        }
        const result = new A2();
        for (let i = 0; i < this.length; i += size) {
            result.push(new A2(...this.slice(i, i + size)));
        }
        return result;
    }
    flatten(depth = Infinity) {
        return new A2(...this.flat(depth));
    }
    flatMap(callback) {
        return new A2(...this.flatMap((value, index) => callback(value, index, this)));
    }
    distinctBy(callback) {
        const seen = new Set();
        const result = new A2();
        this.forEach((value, index) => {
            const key = callback(value, index);
            if (!seen.has(key)) {
                seen.add(key);
                result.push(value);
            }
        });
        return result;
    }
    groupBy(callback) {
        const result = new Map();
        this.forEach((value, index) => {
            const key = callback(value, index);
            let group = result.get(key);
            if (!group) {
                group = new A2();
                result.set(key, group);
            }
            group.push(value);
        });
        return result;
    }
    partition(callback) {
        const yes = new A2();
        const no = new A2();
        this.forEach((value, index) => {
            (callback(value, index) ? yes : no).push(value);
        });
        return [yes, no];
    }
    insert(index, ...items) {
        this.splice(index, 0, ...items);
        return this;
    }
    removeAt(index) {
        const normalized = index < 0 ? this.length + index : index;
        if (normalized < 0 || normalized >= this.length) {
            return undefined;
        }
        return this.splice(normalized, 1)[0];
    }
    remove(value) {
        const index = this.indexOf(value);
        if (index === -1) {
            return false;
        }
        this.splice(index, 1);
        return true;
    }
    removeAll(value) {
        let removed = 0;
        for (let i = this.length - 1; i >= 0; i--) {
            if (Object.is(this[i], value)) {
                this.splice(i, 1);
                removed++;
            }
        }
        return removed;
    }
    replaceAt(index, value) {
        const normalized = index < 0 ? this.length + index : index;
        if (normalized < 0 || normalized >= this.length) {
            return undefined;
        }
        const previous = this[normalized];
        this[normalized] = value;
        return previous;
    }
    move(from, to) {
        const source = from < 0 ? this.length + from : from;
        const target = to < 0 ? this.length + to : to;
        if (source < 0 || source >= this.length ||
            target < 0 || target >= this.length ||
            source === target) {
            return this;
        }
        const [item] = this.splice(source, 1);
        this.splice(target, 0, item);
        return this;
    }
    swap(first, second) {
        const a = first < 0 ? this.length + first : first;
        const b = second < 0 ? this.length + second : second;
        if (a < 0 || a >= this.length ||
            b < 0 || b >= this.length) {
            return this;
        }
        [this[a], this[b]] = [this[b], this[a]];
        return this;
    }
    sample() {
        if (this.length === 0) {
            return undefined;
        }
        return this[Math.floor(Math.random() * this.length)];
    }
    shuffle() {
        for (let i = this.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [this[i], this[j]] = [this[j], this[i]];
        }
        return this;
    }
    countBy(callback) {
        let count = 0;
        this.forEach((value, index) => {
            if (callback(value, index))
                count++;
        });
        return count;
    }
    sum(callback) {
        let total = 0;
        this.forEach((value, index) => {
            total += callback ? callback(value, index) : Number(value);
        });
        return total;
    }
    average(callback) {
        return (this.length === 0 ? NaN : this.sum(callback) / this.length);
    }
    min(callback) {
        if (this.length === 0)
            return undefined;
        return this.reduce((best, value, index) => {
            const current = callback ? callback(value, index) : Number(value);
            const bestValue = callback ? callback(best, this.indexOf(best)) : Number(best);
            return current < bestValue ? value : best;
        });
    }
    max(callback) {
        if (this.length === 0)
            return undefined;
        return this.reduce((best, value, index) => {
            const current = callback ? callback(value, index) : Number(value);
            const bestValue = callback ? callback(best, this.indexOf(best)) : Number(best);
            return current > bestValue ? value : best;
        });
    }
    equals(array) {
        return this.length === array.length &&
            this.every((value, index) => Object.is(value, array[index]));
    }
    includesAll(values) {
        return values.every(value => this.includes(value));
    }
    intersects(values) {
        return values.some(value => this.includes(value));
    }
}
export class SERVER_KVDB {
    //                                                                                                                   CONSTANTS
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    #hot_cache = Object.create(null);
    ///#hot_cache = new O2<S, A>();
    #waiters = new O2();
    #entries = new O2();
    #options;
    #directory;
    #dataPath;
    #walPath;
    #manifestPath;
    #worker;
    #pending = new O2();
    #freePages = [];
    #dirtyPages = new O2();
    #pageCount = 0;
    #nextSequence = 1n;
    #checkpointSequence = 0n;
    #walSize = 0;
    #batch;
    #batchTimer;
    #writeQueue = Promise.resolve();
    #closed = false;
    _onError;
    constructor(k = "__SAURON__", s = "SAURON_SLAVE_REG", m = "SAURON_MASTER_REG", r = "THE_WIZARD_ROOT_REG", options = {}) {
        this.k = k;
        this.s = s;
        this.m = m;
        this.r = r;
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
    get = (() => {
        const z = (k, s = this.s, m = this.m, r = this.r) => {
            const cached = this.#hot_cache[k];
            if (cached !== undefined)
                return MaybePromise(cached);
            else {
                const entry = this.#entries.get(k);
                if (!entry)
                    return undefined;
                return MaybePromise(this.#workerRequest({
                    kind: "readPages",
                    pageNumbers: entry.pageNumbers
                }).then((response) => {
                    if (response.kind !== "readPagesOk") {
                        throw new Error(response.kind === "error" ? response.error : "Unexpected worker response");
                    }
                    const decoded = this.#decodeRecordChainFromPages(response.pages, entry.pageNumbers);
                    if (!decoded) {
                        return undefined;
                    }
                    this.#hot_cache[k] = decoded.value;
                    return decoded.value;
                }));
            }
        };
        z.many = async (keys) => {
            await Promise.all(Object.entries(keys).map(([k]) => this.get(k)));
        };
        z.hot = (k) => {
            return this.#hot_cache[k];
        };
        return z;
    })();
    hot(k) {
        return this.#hot_cache[k];
    }
    set = (() => {
        const z = async (k, value, s = this.s, m = this.m, r = this.r, options = { durable: true }) => {
            this.#hot_cache[k] = value;
            // this.#hot_cache.set(k, value);
            const wk = `__waiters_${k}`;
            const ws = this.#waiters.get(wk);
            if (ws) {
                for (const resolve of ws)
                    resolve(value);
                this.#waiters.delete(wk);
            }
            this._emit(k, value);
            const valueBytes = typeof value === "string" ? encodeUtf8(value) : value;
            const keyBytes = encodeUtf8(k);
            return this.#enqueueWrite(async () => {
                if (this.#closed) {
                    throw new Error("FastKV is closed");
                }
                this.#applyPut(k, keyBytes, valueBytes);
                const record = encodeWalRecord({
                    sequence: this.#nextSequence++,
                    operation: OP_PUT,
                    key: keyBytes,
                    value: valueBytes
                });
                const durable = this.#resolveDurability(options.durable);
                const flushPromise = this.#appendToBatch(record, durable);
                if (options.durable === true || this.#options.durability === "strict") {
                    await flushPromise;
                }
            });
        };
        z.many = async (entries) => {
            await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)));
        };
        return z;
    })();
    delete = (() => {
        const z = async (key, options = {}) => {
            const keyBytes = encodeUtf8(key);
            this._emit(key, undefined);
            return this.#enqueueWrite(async () => {
                if (this.#closed) {
                    throw new Error("FastKV is closed");
                }
                if (!this.#entries.has(key)) {
                    return false;
                }
                this.#applyDelete(key);
                const record = encodeWalRecord({
                    sequence: this.#nextSequence++,
                    operation: OP_DELETE,
                    key: keyBytes,
                    value: new Uint8Array(0)
                });
                const durable = this.#resolveDurability(options.durable);
                const flushPromise = this.#appendToBatch(record, durable);
                if (options.durable === true || this.#options.durability === "strict") {
                    await flushPromise;
                }
                return true;
            });
        };
        z.many = async (keys) => { await Promise.all(keys.map((k) => this.delete(k))); };
        return z;
    })();
    update = (() => {
        const z = async (key, updater) => {
            const prev = await this.get(key);
            const next = updater(prev);
            await this.set(key, next);
            return next;
        };
        z.many = () => { };
        return z;
    })();
    has(key) { return this.#entries.has(key); }
    send = (() => {
        const z = () => { };
        z.many = () => { };
        return z;
    })();
    // #endregion
    //                                                                                                                         SUB
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    subscribe(key, cb) {
        let set = this.#waiters.get(key);
        if (!set) {
            set = new Set();
            this.#waiters.set(key, set);
        }
        set.add(cb);
        const cached = this.#hot_cache[key];
        if (cached) {
            try {
                cb(cached);
            }
            catch (e) {
                this._onError(e);
            }
        }
        return () => {
            const s = this.#waiters.get(key);
            if (!s)
                return;
            s.delete(cb);
            if (s.size === 0)
                this.#waiters.delete(key);
        };
    } /** Selector-based subscription — only fires when the selected slice changes. */
    select(key, selector, cb, chain) {
        let last;
        let first = true;
        return this.subscribe(key, (v) => {
            const next = selector(v);
            if (first || !Object.is(last, next)) {
                first = false;
                last = next;
                cb(next);
            }
        });
    }
    _emit(key, value) {
        const set = this.#waiters.get(key);
        if (!set)
            return;
        for (const cb of set) {
            try {
                cb(value);
            }
            catch (e) {
                this._onError(e);
            }
        }
    }
    _peek(key) {
        const v = this.#hot_cache[key];
        return v === undefined ? undefined : v;
    }
    waitFor(key) {
        const existing = this._peek(key);
        if (existing !== undefined)
            return Promise.resolve(existing);
        return new Promise((resolve) => {
            const waiterKey = `__waiters_${key}`;
            let waiters = this.#waiters.get(waiterKey);
            if (!waiters) {
                waiters = new Set();
                this.#waiters.set(waiterKey, waiters);
            }
            waiters.add(resolve);
        });
    }
    // #endregion
    //                                                                                                                  WORKER FNS
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    async open() {
        if (this.#worker) {
            throw new Error("FastKV is already open");
        }
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
    async flush() {
        await this.#enqueueWrite(async () => {
            await this.#flushCurrentBatch();
        });
    }
    async checkpoint() {
        await this.#enqueueWrite(async () => {
            await this.#checkpoint();
        });
    }
    async compact() {
        await this.#enqueueWrite(async () => {
            await this.#flushCurrentBatch();
            const compactPath = `${this.#dataPath}.compact`;
            await this.#workerRequest({ kind: "compactBegin", compactPath });
            const livePages = [];
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
                if (!value)
                    continue;
                // this.#hot_cache.set(key, value);
                this.#hot_cache[key] = value;
                const keyBytes = encodeUtf8(key);
                const chain = encodeRecordChain(keyBytes, value, () => nextPage++);
                for (const page of chain.pages) {
                    livePages.push({
                        pageNumber: chain.pageNumbers[0],
                        buffer: page
                    });
                }
                this.#entries.set(key, {
                    pageNumber: chain.pageNumbers[0],
                    pageNumbers: chain.pageNumbers
                });
            }
            this.#pageCount = nextPage;
            const manifest = encodeManifest({ checkpointSequence: this.#nextSequence, pageCount: this.#pageCount });
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
    async close() {
        if (this.#closed)
            return;
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
    #resolveDurability(requested) {
        if (requested !== undefined) {
            return requested;
        }
        return (this.#options.durability === "group" ||
            this.#options.durability === "strict");
    }
    #applyPut(key, keyBytes, valueBytes, replay = false) {
        const previous = this.#entries.get(key);
        const chain = encodeRecordChain(keyBytes, valueBytes, () => {
            const reused = this.#freePages.pop();
            if (reused !== undefined) {
                return reused;
            }
            return this.#pageCount++;
        });
        if (previous) {
            for (const pageNumber of previous.pageNumbers) {
                this.#dirtyPages.set(pageNumber, encodeFreePage());
                this.#freePages.push(pageNumber);
            }
        }
        for (let index = 0; index < chain.pages.length; index++) {
            this.#dirtyPages.set(chain.pageNumbers[index], chain.pages[index]);
        }
        this.#entries.set(key, {
            pageNumber: chain.pageNumbers[0],
            pageNumbers: chain.pageNumbers
        });
        // this.#hot_cache.set(key, valueBytes);
        if (replay) {
            this.#nextSequence = this.#nextSequence;
        }
    }
    #applyDelete(key, replay = false) {
        const previous = this.#entries.get(key);
        if (!previous)
            return;
        for (const pageNumber of previous.pageNumbers) {
            this.#dirtyPages.set(pageNumber, encodeFreePage());
            this.#freePages.push(pageNumber);
        }
        this.#entries.delete(key);
        //this.#hot_cache.delete(key);
        delete this.#hot_cache[key];
        if (replay) {
            this.#nextSequence = this.#nextSequence;
        }
    }
    #enqueueWrite(operation) {
        const result = this.#writeQueue.then(operation, operation);
        this.#writeQueue = result.catch(() => undefined);
        return result;
    }
    #appendToBatch(record, durable) {
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
        return new Promise((resolve) => {
            batch.waiters.push(resolve);
            if (batch.byteLength >= this.#options.walBatchBytes ||
                batch.records.length >= this.#options.walBatchRecords) {
                void this.#flushCurrentBatch();
                return;
            }
            this.#batchTimer ??= setTimeout(() => {
                this.#batchTimer = undefined;
                void this.#flushCurrentBatch();
            }, this.#options.walFlushIntervalMs);
        });
    }
    #workerRequest(request) {
        if (!this.#worker) {
            return Promise.reject(new Error("FastKV is not open"));
        }
        const id = crypto.randomUUID();
        return new Promise((resolve, reject) => {
            this.#pending.set(id, {
                resolve,
                reject
            });
            this.#worker.postMessage({
                ...request,
                id
            });
        });
    }
    // pages.ts
    #decodeRecordChainFromPages(pages, pageNumbers) {
        const pageByNumber = new Map();
        for (let index = 0; index < pageNumbers.length; index++) {
            const pageNumber = pageNumbers[index];
            const page = pages[index];
            if (page) {
                pageByNumber.set(pageNumber, page);
            }
        }
        const firstPageNumber = pageNumbers[0];
        if (firstPageNumber === undefined) {
            return undefined;
        }
        return decodeRecordChain((pageNumber) => pageByNumber.get(pageNumber), firstPageNumber);
    }
    async #recover() {
        const manifestBytes = await readFile(this.#manifestPath, { flag: "r" }).catch(() => undefined);
        const manifest = manifestBytes ? decodeManifest(manifestBytes) : undefined;
        this.#checkpointSequence = manifest?.checkpointSequence ?? 0n;
        const dataFile = await open(this.#dataPath, "r");
        try {
            const fileSize = Number((await dataFile.stat()).size);
            const pageCount = Math.floor(fileSize / PAGE_SIZE);
            const pageCache = new Map();
            for (let pageNumber = 0; pageNumber < pageCount; pageNumber++) {
                const page = new Uint8Array(PAGE_SIZE);
                const buffer = Buffer.from(page.buffer, page.byteOffset, page.byteLength);
                const result = await dataFile.read(buffer, 0, PAGE_SIZE, pageNumber * PAGE_SIZE);
                if (result.bytesRead !== PAGE_SIZE)
                    break;
                pageCache.set(pageNumber, page);
            }
            const readPage = (pageNumber) => pageCache.get(pageNumber);
            for (const pageNumber of pageCache.keys()) {
                const page = pageCache.get(pageNumber);
                const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
                const state = view.getUint32(0, true);
                if (state === 0) {
                    this.#freePages.push(pageNumber);
                    continue;
                }
                if (state !== 1)
                    continue;
                const decoded = decodeRecordChain(readPage, pageNumber);
                if (!decoded)
                    continue;
                const key = decodeUtf8(decoded.key);
                const pageNumbers = [];
                let cursor = pageNumber;
                while (cursor !== undefined && cursor !== NO_PAGE) {
                    pageNumbers.push(cursor);
                    const currentPage = pageCache.get(cursor);
                    const currentView = new DataView(currentPage.buffer, currentPage.byteOffset, currentPage.byteLength);
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
        }
        finally {
            await dataFile.close();
        }
        const walBytes = await readFile(this.#walPath)
            .catch(() => new Uint8Array(0));
        const decoded = decodeWalRecords(walBytes, this.#checkpointSequence);
        for (const record of decoded.records) {
            const key = decodeUtf8(record.key);
            if (record.operation === OP_PUT) {
                this.#applyPut(key, record.key, record.value, true);
            }
            else if (record.operation === OP_DELETE) {
                this.#applyDelete(key, true);
            }
            this.#nextSequence = record.sequence + 1n;
        }
        this.#walSize = decoded.validBytes;
    }
    async #flushCurrentBatch() {
        const batch = this.#batch;
        if (!batch || batch.records.length === 0) {
            return;
        }
        this.#batch = undefined;
        if (this.#batchTimer) {
            clearTimeout(this.#batchTimer);
            this.#batchTimer = undefined;
        }
        let totalLength = 0;
        for (const record of batch.records) {
            totalLength += record.length;
        }
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
            if (isWorkerError(response)) {
                throw new Error(`WAL append failed: ${response.error}`);
            }
            if (response.kind !== "walOk") {
                throw new Error(`Unexpected WAL response: ${response.kind}`);
            }
            this.#walSize = response.walSize;
            for (const resolve of batch.waiters) {
                resolve();
            }
        }
        catch (error) {
            for (const waiter of batch.waiters) {
                // Resolve rather than reject: durability waiters must not hang.
                waiter();
            }
            throw error;
        }
    }
    async #checkpointIfRequired() {
        if (this.#walSize >= this.#options.walHighWaterBytes) {
            await this.checkpoint();
        }
    }
    async #checkpoint() {
        if (!this.#worker) {
            throw new Error("FastKV is not open");
        }
        await this.#flushCurrentBatch();
        const pages = [];
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
    async #startWorker() {
        this.#worker = new Worker(workerPath);
        this.#worker.on("message", (response) => {
            const pending = this.#pending.get(response.id);
            if (!pending)
                return;
            this.#pending.delete(response.id);
            if (isWorkerError(response)) {
                pending.reject(new Error(response.error));
                return;
            }
            pending.resolve(response);
        });
        this.#worker.on("error", (error) => {
            for (const pending of this.#pending.values()) {
                pending.reject(error);
            }
            this.#pending.clear();
        });
        await this.#workerRequest({
            kind: "init",
            dataPath: this.#dataPath,
            walPath: this.#walPath
        });
    }
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
