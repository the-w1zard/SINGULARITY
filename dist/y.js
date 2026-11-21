// bus-bench.ts
// FIGHT=... bun --expose-gc bus-bench.ts
// import { SERVER_KVDB } from "./db/server/db";
import { darkState, root } from "./signals";
const CAPACITY = 1 << 18;
const DURATION = 12;
const REPORT_MS = 12;
const BATCH_SIZE = 100_000; // 500_000
const TOTAL_BATCHES = 100; // 500
const RUN_BARE_METAL_CPU_TEST = true;
const pid = 1;
let seq = 0;
const v = `${++seq}`;
const FIGHT = process.env.FIGHT ?? "bare";
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
function BARE_METAL_CPU(name, write) {
    const batchTimes = new Float64Array(TOTAL_BATCHES);
    for (let i = 0; i < 2_000_000; i++)
        write();
    const globalStart = process.hrtime.bigint();
    for (let b = 0; b < TOTAL_BATCHES; b++) {
        const batchStart = process.hrtime.bigint();
        for (let i = 0; i < BATCH_SIZE; i++)
            write();
        batchTimes[b] = Number(process.hrtime.bigint() - batchStart);
    }
    const globalEnd = process.hrtime.bigint();
    const totalOps = BATCH_SIZE * TOTAL_BATCHES;
    let bestOpsPerSec = 0;
    for (let b = 0; b < TOTAL_BATCHES; b++) {
        const ops = BATCH_SIZE / (batchTimes[b] / 1e9);
        if (ops > bestOpsPerSec)
            bestOpsPerSec = ops;
    }
    const avg = totalOps / (Number(globalEnd - globalStart) / 1e9);
    console.log(`------------ ${name} ---------------`);
    console.log(`Peak (Best Batch):                      ${(bestOpsPerSec / 1e6).toFixed(2)} M ops/sec`);
    console.log(`Average:                                ${(avg / 1e6).toFixed(2)} M ops/sec`);
    console.log(`---------------------------------------------------------`);
}
3;
function BENCH(name, write) {
    let windowOps = 0, totalOps = 0, best = 0, elapsedTotal = 0;
    let start = process.hrtime.bigint();
    let windowStart = start;
    let cpuStart = process.cpuUsage();
    console.log(`\n${name}`);
    console.log("sec | M writes/s | best M/s | avg M/s | CPU %");
    while (elapsedTotal < dur) {
        for (let i = 0; i < 100_000; i++)
            write();
        windowOps += 100_000;
        totalOps += 100_000;
        const now = process.hrtime.bigint();
        const elapsedUs = Number(now - windowStart) / 1_000;
        elapsedTotal = Number(now - start) / 1e6;
        if (elapsedUs >= report * 1_000) {
            const mp = windowOps / elapsedUs;
            const avg = totalOps / (Number(now - start) / 1e9) / 1e6;
            const cpuNow = process.cpuUsage();
            const cpuUs = cpuNow.user - cpuStart.user + cpuNow.system - cpuStart.system;
            best = Math.max(best, mp);
            console.log(String(Math.floor(elapsedTotal / 1_000)).padStart(3), "|", mp.toFixed(2).padStart(10), "|", best.toFixed(2).padStart(8), "|", avg.toFixed(7).padStart(7), "|", ((cpuUs / elapsedUs) * 100).toFixed(1).padStart(5));
            windowOps = 0;
            windowStart = now;
            cpuStart = cpuNow;
        }
    }
}
const bench = RUN_BARE_METAL_CPU_TEST === false ? BENCH : BARE_METAL_CPU;
export const rv = (v) => typeof v === "function" ? v() : v;
export const sameValue = (a, b) => Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
export class M2 extends Map {
    // public g = this.get;
    // public h = this.has;
    // public d = this.delete;
    // public c = this.clear
    f = this.forEach; //                                                     [FOR EACH]
    #subs;
    #hist;
    #histLimit = 0;
    #strict = false;
    #iterating = 0;
    #batching = 0;
    #pending;
    #derived;
    #dependents;
    #emit(e) {
        if (this.#batching > 0) {
            // coalesce: last write per key wins, keep prev from first event
            const pending = this.#pending.get(e.k);
            this.#pending.set(e.k, pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true });
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
        const deps = this.#dependents?.get(k);
        if (!deps)
            return;
        for (const dk of deps) {
            const d = this.#derived?.get(dk);
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
            this.#emit({
                op: had ? "upd" : "set",
                k,
                v: next,
                prev: prev,
                has: true,
            });
        }
        return next;
    }
    #assertNotIterating(op) {
        if (this.#strict && this.#iterating > 0) {
            throw new Error(`M2I.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
        }
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
        this.#pending ??= new M2();
        try {
            fn();
        }
        finally {
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
    s(v, k) {
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
            prev: prev,
            has,
        });
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
    c() {
        this.#assertNotIterating("clear");
        const entries = [...this.entries()];
        super.clear();
        for (const [k, prev] of entries) {
            this.#invalidate(k);
            this.#emit({ op: "del", k, prev, has: false });
        }
    }
    dv(k, deps, fn) {
        (this.#derived ??= new M2())
            .set(k, {
            deps,
            fn,
            dirty: true,
            has: false,
        });
        for (const dep of deps) {
            (this.#dependents ??= new M2())
                .getOrM(() => new Set(), dep)
                .add(k);
        }
        return this;
    }
    dirty(...keys) {
        for (const k of keys) {
            const d = this.#derived?.get(k);
            if (d && !d.dirty) {
                d.dirty = true;
                this.#emit({ op: "dirty", k, has: d.has });
            }
        }
        return this;
    }
    g(k) {
        const d = this.#derived?.get(k);
        if (d) {
            if (d.dirty || !d.has)
                return this.#computeDerived(k, d);
            return d.value;
        }
        return super.get(k);
    }
    h(k) {
        const d = this.#derived?.get(k);
        if (d) {
            if (d.dirty || !d.has)
                this.#computeDerived(k, d);
            return true;
        }
        return super.has(k);
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
            if (!other.has(k)) {
                add.push([k, v]);
            }
            else if (!Object.is(other.get(k), v)) {
                chg.push([k, v, other.get(k)]);
            }
        }
        for (const k of other.keys()) {
            if (!this.has(k))
                del.push(k);
        }
        return { add, del, chg };
    }
    *entries() {
        this.#iterating++;
        try {
            for (const entry of super.entries())
                yield entry;
        }
        finally {
            this.#iterating--;
        }
    }
    *keys() {
        this.#iterating++;
        try {
            for (const k of super.keys())
                yield k;
        }
        finally {
            this.#iterating--;
        }
    }
    *values() {
        this.#iterating++;
        try {
            for (const v of super.values())
                yield v;
        }
        finally {
            this.#iterating--;
        }
    }
    swap(value, key) {
        const next = rv(value);
        const previous = this.has(key) ? this.get(key) : next;
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
        const out = new M2();
        for (const [k, v] of this)
            out.s(fn(v, k), k);
        return out;
    }
    mapKeys(fn) {
        const out = new M2();
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
        const yes = new M2();
        const no = new M2();
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
        const out = new M2();
        for (const [k, v] of this)
            out.s(k, v);
        return out;
    }
    unique(fn) {
        const out = new M2();
        for (const [k, v] of this) {
            const key = (fn ? fn(v, k) : v);
            if (!out.h(key))
                out.s(v, key);
        }
        return out;
    }
    pick(keys) {
        const out = new M2();
        for (const k of keys) {
            if (this.h(k))
                out.s(this.g(k), k);
        }
        return out;
    }
    omit(keys) {
        const omitted = new Set(keys);
        const out = new M2();
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
            this.clear();
            for (const [k, v] of entries) {
                const n = rv(v);
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
    toValues() { return A2.from(this.values()); } //                 [TOVALUES]
    toKeys() { return A2.from(this.keys()); } //                       [TOKEYS]
    toArray() { return A2.from(this.entries()); } //             [TOARRAY]
    group(fn) {
        const out = new M2();
        for (const [k, v] of this)
            out.getOrM(() => new A2(), fn(v, k)).push(v);
        return out;
    }
    clone() {
        return new M2(this);
    }
    equals(other) {
        if (other === this)
            return true;
        if (this.size !== other.size)
            return false;
        for (const [k, v] of this) {
            if (!other.has(k) || !sameValue(other.get(k), v))
                return false;
        }
        return true;
    }
    static fromObject(obj) {
        return new M2(Object.entries(obj));
    }
    static fromEntries(entries) {
        return new M2(entries);
    }
    static from(map) {
        return new M2(map);
    }
}
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
            out.getOrM(fn(item, i++), () => new A2()).push(item);
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
    s(k, v) {
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
        this.s(k, next);
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
        this.s(key, next);
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
    getOrI(k, value) {
        if (this.h(k))
            return this.g(k);
        const next = rv(value);
        this.s(k, next);
        return next;
    }
    getOrM(k, make) {
        if (this.h(k))
            return this.g(k);
        const value = make();
        this.s(k, value);
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
        this.s(k, next);
        return next;
    }
    setIfAbsent(k, value) {
        if (this.h(k))
            return false;
        const next = rv(value);
        this.s(k, next);
        return true;
    }
    setIfPresent(k, value) {
        if (!this.h(k))
            return false;
        const next = rv(value);
        this.s(k, next);
        return true;
    }
    replace(k, value) {
        const next = rv(value);
        return this.setIfPresent(k, next);
    }
    inc(key, by = 1) {
        return this.update(key, (old) => ((old ?? 0) + by), 0);
    }
    touch(k) {
        return this.setIfAbsent(k, undefined);
    }
    mapValues(fn) {
        const out = new O2();
        for (const [k, v] of this)
            out.s(k, fn(k, v));
        return out;
    }
    mapKeys(fn) {
        const out = new O2();
        for (const [k, v] of this)
            out.s(fn(k, v), v);
        return out;
    }
    filterKV(pred) {
        for (const [k, v] of this) {
            if (!pred(k, v))
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
            (pred(k, v) ? yes : no).s(k, v);
        }
        return [yes, no];
    }
    find(pred) {
        for (const [k, v] of this) {
            if (pred(k, v))
                return [k, v];
        }
        return undefined;
    }
    findKey(pred) {
        for (const [k, v] of this) {
            if (pred(k, v))
                return k;
        }
        return undefined;
    }
    findValue(pred) {
        for (const [k, v] of this) {
            if (pred(k, v))
                return v;
        }
        return undefined;
    }
    some(pred) {
        for (const [k, v] of this) {
            if (pred(k, v))
                return true;
        }
        return false;
    }
    every(pred) {
        for (const [k, v] of this) {
            if (!pred(k, v))
                return false;
        }
        return true;
    }
    reduce(fn, initial) {
        let acc = initial;
        for (const [k, v] of this)
            acc = fn(acc, k, v);
        return acc;
    }
    forEachKV(fn) {
        for (const [k, v] of this)
            fn(k, v, this);
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
            const key = (fn ? fn(k, v) : v);
            if (!out.h(key))
                out.s(key, v);
        }
        return out;
    }
    pick(keys) {
        const out = new O2();
        for (const k of keys) {
            if (this.h(k))
                out.s(k, this.g(k));
        }
        return out;
    }
    omit(keys) {
        const omitted = new Set(keys);
        const out = new O2();
        for (const [k, v] of this) {
            if (!omitted.has(k))
                out.s(k, v);
        }
        return out;
    }
    merge(other) {
        for (const [k, v] of other) {
            const n = rv(v);
            this.s(k, n);
        }
        return this;
    }
    clearAndSet(entries) {
        return this.batch(() => {
            this.c(); //                         (M2's version calls the native Map.clear() here
            for (const [k, v] of entries) { //  — silent, no del events; c() is almost
                const n = rv(v); //  certainly what was intended, so I used it)
                this.s(k, n);
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
                this.s(k, partial[k]);
        });
    }
    toValues() { return A2.from(this.values()); } //                 [TOVALUES]
    toKeys() { return A2.from(this.keys()); } //                       [TOKEYS]
    toArray() { return A2.from(this.entries()); } //             [TOARRAY]
    group(fn) {
        const out = new O2();
        for (const [k, v] of this)
            out.getOrM(fn(v, k), () => new A2()).push(v);
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
    const JSObj = Object.create(null);
    JSObj['keyy'] = "1";
    bench("JSObj['keyy'];", () => { JSObj['keyy']; });
    const O2Test = new O2();
    O2Test['keyy'] = '1';
    bench("O2Test['keyy']", () => { O2Test['keyy']; });
    O2Test.s("1", "keyy");
    bench("O2.get(k)  [real #data]", () => { O2Test.get('keyy'); });
    const d = new O2();
    d.s("2", "a");
    d.dv("b", ["a"], (a) => a + "!");
    d.get("b");
    bench("O2.get(derived clean)", () => { d.get('b'); });
    const dd = new O2();
    dd.s("2", "a");
    dd.dv("b", ["a"], (a) => a + "!");
    bench("O2.get(derived dirty)", () => { dd.dirty('b'); dd.get('b'); });
    bench("O2.get(miss)", () => { O2Test.get('nope'); });
    bench("O2.h(k) hit", () => { O2Test.h('keyy'); });
    bench("O2.h(k) miss", () => { O2Test.h('nope'); });
    bench("O2.s(v,k) overwrite", () => { O2Test.s("1", "keyy"); });
    bench("O2.s(v,k) new+del churn", () => {
        const k = Math.random().toString(36).slice(2, 10);
        O2Test.s("1", k);
        O2Test.d(k);
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
        root.state.store.set(storeKey, (prev) => (prev ?? 0) + 1, {
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
        const dispose = root.state.store.subscribe(storeKey, () => { });
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
        const dispose = root.state.state.subscribe(stateKey, () => { });
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
    const [selectedValue, setSelectedValue] = darkState({ count: 0, other: 0 }, {
        selector: (v) => v.count,
    }, "bench-darkState-selector");
    bench("RAVEN.darkState.selector.get", () => {
        darkState({ count: 0, other: 0 }, {
            selector: (v) => v.count,
        }, "bench-darkState-selector");
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
    const [derivedValue, setDerivedValue, derived] = darkState(1, {
        derive: (v) => v * 2,
    }, "bench-darkState-derived");
    bench("RAVEN.darkState.derived.get", () => {
        darkState(1, {
            derive: (v) => v * 2,
        }, "bench-darkState-derived");
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
