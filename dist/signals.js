/*  ===========================================================================================================================
     Welcome to the dark side...
     The counterpart to the traditional portion of the platform, where the light side gives you features... this does not.
     But... There is something that the dark side has, that the light does not... True, raw... power. Literally as much as we
     can acquire.
     The signals solution bench comes in at being 40-1100x more performant than both preact AND alien...
     The atoms solution... that will be a bit harder to bench because, no other atoms solution even advertises any sort
     of performance metrics... across the board really. But I will install a few and bench them myself, so that is to come.
     Other bench tests that will also have to be entirely done with zero help...
     Message queue / bus... I don't think this one is possible... because a bench test is not fair, unless all solutions
     in question are ran on the same machine. I honestly don't know of any solutions that you do not have to pay for sooo, this
     one is out.
     Application state / context, this one will be easy enough. Just download and set up a very basic version of a couple
     of platforms and run the same test on each.
     All other solution I don't think... I can bench either due to industry availability... BUT that doesn't mean I will
     not put everything on a bench anyways! So in total we have...
     - signals: 40-1100x more performant than both preact AND alien...
     - atoms: hitting 751 million ops per sec in the users usage path...
     - app context / state
     - hooks
     - message queue / bus
     - slab
     - distributed reactive state
     - spreadsheet engine
     - same node / same app transport x3
     - same node / different app transport
     - anti message queue
     Now the reason why this is the dark side is because, each and every single solution... is entirely backed by disruptors.
     Currently, I believe that there is 6 at the time of writing with an asterisk, because the atoms solution that number is
     entirely dependant on user usage because there are disruptors available there too.

     Now there will be one slow bit of code that could not be entirely converted, due to its use being used by several other
     parts of the application. Which is the portion of how the buses actually end up becoming a mesh. Each bus does not store
     values within its own class / domain. Instead the message queues access a cross-node module instance where most back end
     fns and the storage of all subscribers and other similar values are placed so that each bus has access to them.

     This portion is still optimized, just not at disruptor speed — it operates at 105M ops/sec, which is the same rate my M2 hits.
     The rest of the mesh paths run at 400-600M ops/sec, above that line.
     I am looking into ways of drastically improving that number, but when you operate at these numbers and have nothing
     to go off of, its just a fucking roll of the dice. One little implementation detail could mean the difference between 4 mill
     and 900 mill. The best disruptor I have built to date, started life as a 400k ops per sec disruptor and currently sits at
     1.2 billion(2.1 bill on a friends newer machine), same archtecture as when it was first built.
     OTHER THAN THAT ONE SMALL PORTION OF CODE, everything else was entirely redone... even the reactive state portion ie the
     cloud... kinda, curious to see if I was able to code it and have it working first shot... I doubt it.
    ============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE DARK SIDE] */
//                                                                                                                      IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import "@the-w1zard/alias_types";
import "@the-w1zard/types";
import "@the-w1zard/dx";
import { Registry } from "@the-w1zard/the-registry";
// import { Mesh } from "@the-w1zard/the-mesh"
// import { darkState, useEffect } from "@the-w1zard/the-dark-side"
import { Queue } from "./comps/queue";
//import { M2 } from "#dx";
import { parseFormula, isFormula, normalizeCellRef, parseCellRef, createSpreadsheetGraph, cellKey, colToIndex, indexToCol, qualifyAst, clearGraphCellInPlace, writeGraphCellInPlace, isVolatileAst, recalculateDirty, boxContains, collectDependencies, } from './comps/cell';
import { BUS_CRYPTO } from "./comps/DISRUPTOR_CRYPTO";
import { BINARY_CODEC } from "./comps/binary";
import { MCR_Combo } from "@the-w1zard/dx/twx";
//import { M2 } from "@the-w1zard/dx";
// #endregion
//                                                                                                                        M2 O2
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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
export function makeGateAccess() {
    return {
        gateOf(cell) {
            return cell?.read();
        },
        isSealed(cell) {
            return cell?.read()?.sealed ?? false;
        },
        isFrozen(cell) {
            return cell?.read()?.frozen ?? false;
        },
    };
}
const NO_SUB = -1;
function pow2(value) {
    return value > 0 && (value & (value - 1)) === 0;
}
const TRACER_PID = 0;
export const OP = {
    READ: 0,
    WRITE: 1,
    DELETE: 2,
    EFFECT: 3,
    COMPACT: 4,
    NONE: 5,
    EDIT: 6,
    COMPUTED: 7,
    EFFECTSCOPE: 8,
    EXTERNAL: 9,
    OBSERVER: 10,
    ASYNCEFFECT: 11,
    SEND: 12,
    SUB: 13,
    DUPLEX: 14,
    STREAM: 15,
    DRC: 16,
};
const RX_Flags = {
    NONE: 0,
    SIGNAL: 1 << 0,
    COMPUTED: 1 << 1,
    EFFECT: 1 << 2,
    DIRTY: 1 << 3,
    RUNNING: 1 << 4,
    DISPOSED: 1 << 5,
    MUTABLE: 1 << 6,
    WATCHING: 1 << 7,
    RECURSEDCHECK: 1 << 8,
    RECURSED: 1 << 9,
    PENDING: 1 << 10,
};
let Bun = null;
let Deno = null;
if (typeof globalThis.Deno !== "undefined") {
    Deno = globalThis.Deno;
}
if (typeof globalThis.Bun !== "undefined") {
    Bun = globalThis.Bun;
}
export const hasBun = typeof globalThis.Bun !== "undefined";
export const hasDeno = typeof globalThis.Deno !== "undefined";
export const hasNode = typeof process !== "undefined" && !!process.versions?.node;
export const isBrowser = typeof window !== "undefined";
export const isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
export let _localChange = false;
export let _channel = null;
export let isReact = false;
export let IS_THE_WIZARD = true;
let init_hooks_context = false;
const FREE_LIST_END = -1;
export const END = Symbol("rift:end");
const REC = 8;
const F_OP = 0;
const F_PID = 1;
const F_SLOT = 2;
const F_A = 3;
const F_B = 4;
const F_C = 5;
const F_EPOCH = 6;
const F_EXT = 7;
const DEFAULT_SCHEDULE = [
    OP.READ,
    OP.WRITE,
    OP.READ,
    OP.WRITE,
    OP.DELETE,
    OP.EFFECT,
    OP.COMPACT,
];
const pow22 = (n) => n > 0 && (n & (n - 1)) === 0;
export const OP2 = {
    READ: 0,
    WRITE: 1,
    DELETE: 2,
    EFFECT: 3,
    COMPACT: 4,
};
export const rv = (v) => typeof v === "function" ? v() : v;
export const sameValue = (a, b) => Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
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
};
// #endregion
//                                                                                                                   [GOVERNOR]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class GOVERNOR {
    hub;
    /* ==========================================================================
       The disruptor runs hot on its own.
       The public API never touches passes.
       ========================================================================== */
    tier = 0; // 25 / 50 / 75 / 100
    running = false;
    hibernating = false;
    lastAction = 0;
    windowActions = 0;
    windowStart = 0;
    IDLE_MS = 15_000;
    TIERS = [
        256, // tier 0 -> 25%
        1_024, // tier 1 -> 50%
        4_096, // tier 2 -> 75%
        16_384, // tier 3 -> 100%
    ];
    constructor(DEATH_STAR) {
        this.hub = DEATH_STAR;
    }
    wake() {
        this.lastAction = Date.now();
        if (this.hibernating) {
            this.hibernating = false;
        }
        if (!this.running) {
            this.running = true;
            queueMicrotask(() => this.loop());
        }
    }
    async loop() {
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
            this.hub.signals.signals_raptor_ngin.pass();
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
    tierFor(rate) {
        if (rate >= this.TIERS[2])
            return 3;
        if (rate >= this.TIERS[1])
            return 2;
        if (rate >= this.TIERS[0])
            return 1;
        return 0;
    }
    currentTier() {
        return this.tier;
    }
    isHibernating() {
        return this.hibernating;
    }
}
// #endregion
//                                                                                                         [DARTH MAUL] SIGNALS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const queued = [];
const HasChildEffect = 64;
let cycle = 0;
let runDepth = 0;
let batchDepth = 0;
let notifyIndex = 0;
let queuedLength = 0;
let activeSub;
let _n = 0;
const _rxid = () => `${_signals_root.id}:rx${++_n}`;
;
let _seq = 0;
const seqId = () => `seq${++_seq}`;
const str = (v) => (v == null ? '' : String(v));
let _signals_root = {
    id: "THE_WIZARD_SIGNALS_ROOT",
    hooks: new O2(),
    unmount: new Set(),
    contextProviders: new O2(),
};
const rx = new O2().$debug(128); // ONE store. id -> binding, and the binding holds the value.
rx.on((e) => {
    if (e.op !== 'set' && e.op !== 'upd')
        return;
    e.v.write(e.v.v);
});
const get = (id) => rx.g(id).v;
const set = (id, v) => rx.mutate(id, (b) => { b.v = v; return b; });
const { link, unlink, propagate, checkDirty, shallowPropagate, } = createReactiveSystem({
    update(node) {
        if ('getter' in node) {
            return updateComputed(node);
        }
        if ('currentValue' in node) {
            return updateSignal(node);
        }
        node.flags = 1;
        return true;
    },
    notify(effect) {
        let insertIndex = queuedLength;
        let firstInsertedIndex = insertIndex;
        do {
            queued[insertIndex++] = effect;
            effect.flags &= ~2;
            effect = effect.subs?.sub;
            if (effect === undefined || !(effect.flags & 2)) {
                break;
            }
        } while (true);
        queuedLength = insertIndex;
        while (firstInsertedIndex < --insertIndex) {
            const left = queued[firstInsertedIndex];
            queued[firstInsertedIndex++] = queued[insertIndex];
            queued[insertIndex] = left;
        }
    },
    unwatched(node) {
        if ('getter' in node) {
            if (node.depsTail !== undefined) {
                node.flags = 1 | 16;
                disposeAllDepsInReverse(node);
            }
        }
        else if ('currentValue' in node) {
        }
        else if ('fn' in node) {
            effectOper.call(node);
        }
        else {
            effectScopeOper.call(node);
        }
    },
});
function updateComputed(c) {
    if (c.flags & HasChildEffect) {
        let link = c.depsTail;
        while (link !== undefined) {
            const prev = link.prevDep;
            const dep = link.dep;
            if (!('getter' in dep) && !('currentValue' in dep)) {
                unlink(link, c);
            }
            link = prev;
        }
    }
    c.depsTail = undefined;
    c.flags = 1 | 4;
    const prevSub = setActiveSub(c);
    try {
        ++cycle;
        const oldValue = c.value;
        return oldValue !== (c.value = c.getter(oldValue));
    }
    finally {
        activeSub = prevSub;
        c.flags &= ~4;
        purgeDeps(c);
    }
}
function updateSignal(s) { s.flags = 1; return s.currentValue !== (s.currentValue = s.pendingValue); }
function run(e) {
    const flags = e.flags;
    if (flags & 16
        || (flags & 32
            && checkDirty(e.deps, e))) {
        if (flags & HasChildEffect) {
            let link = e.depsTail;
            while (link !== undefined) {
                const prev = link.prevDep;
                const dep = link.dep;
                if (!('getter' in dep) && !('currentValue' in dep)) {
                    unlink(link, e);
                }
                link = prev;
            }
        }
        if (e.cleanup) {
            runCleanup(e);
            if (!e.flags) {
                return;
            }
        }
        e.depsTail = undefined;
        e.flags = 2 | 4;
        const prevSub = setActiveSub(e);
        try {
            ++cycle;
            ++runDepth;
            e.cleanup = e.fn();
        }
        finally {
            --runDepth;
            activeSub = prevSub;
            e.flags &= ~4;
            purgeDeps(e);
        }
    }
    else if (e.deps !== undefined) {
        e.flags = 2 | (flags & HasChildEffect);
    }
}
function flush() {
    try {
        while (notifyIndex < queuedLength) {
            const effect = queued[notifyIndex];
            queued[notifyIndex++] = undefined;
            run(effect);
        }
    }
    finally {
        while (notifyIndex < queuedLength) {
            const effect = queued[notifyIndex];
            queued[notifyIndex++] = undefined;
            effect.flags |= 2 | 8;
        }
        notifyIndex = 0;
        queuedLength = 0;
    }
}
function computedOper() {
    const flags = this.flags;
    if (flags & 16
        || (flags & 32
            && (checkDirty(this.deps, this)
                || (this.flags = flags & ~32, false)))) {
        if (updateComputed(this)) {
            const subs = this.subs;
            if (subs !== undefined) {
                shallowPropagate(subs);
            }
        }
    }
    else if (!flags) {
        this.flags = 1 | 4;
        const prevSub = setActiveSub(this);
        try {
            this.value = this.getter();
        }
        finally {
            activeSub = prevSub;
            this.flags &= ~4;
        }
    }
    const sub = activeSub;
    if (sub !== undefined) {
        link(this, sub, cycle);
    }
    return this.value;
}
function signalOper(...value) {
    if (value.length) {
        if (this.pendingValue !== (this.pendingValue = value[0])) {
            this.flags = 1 | 16;
            const subs = this.subs;
            if (subs !== undefined) {
                propagate(subs, !!runDepth);
                if (!batchDepth) {
                    flush();
                }
            }
        }
    }
    else {
        if (this.flags & 16) {
            if (updateSignal(this)) {
                const subs = this.subs;
                if (subs !== undefined) {
                    shallowPropagate(subs);
                }
            }
        }
        const sub = activeSub;
        if (sub !== undefined) {
            link(this, sub, cycle);
        }
        return this.currentValue;
    }
}
function runCleanup(e) {
    const cleanup = e.cleanup;
    e.cleanup = undefined;
    const prevSub = activeSub;
    activeSub = undefined;
    try {
        cleanup();
    }
    finally {
        activeSub = prevSub;
    }
}
function effectOper() {
    effectScopeOper.call(this);
    if (this.cleanup) {
        runCleanup(this);
    }
}
function effectScopeOper() {
    this.flags = 0;
    disposeAllDepsInReverse(this);
    const sub = this.subs;
    if (sub !== undefined) {
        unlink(sub);
    }
}
function disposeAllDepsInReverse(sub) {
    let link = sub.depsTail;
    while (link !== undefined) {
        const prev = link.prevDep;
        unlink(link, sub);
        link = prev;
    }
}
function purgeDeps(sub) {
    const depsTail = sub.depsTail;
    let dep = depsTail !== undefined ? depsTail.nextDep : sub.deps;
    while (dep !== undefined) {
        dep = unlink(dep, sub);
    }
}
function makeRx(id, b) {
    const fn = (...args) => {
        if (args.length === 0)
            return b.v;
        set(id, args[0]);
    };
    const r = fn;
    Object.defineProperties(r, { id: { value: id }, node: { value: b.node }, el: { value: b.el } });
    b.write(b.v);
    rx.s(b, id);
    r.attach = (parent) => { for (const n of b.owned)
        parent.appendChild(n); return r; };
    r.dispose = () => {
        rx.d(id);
        _signals_root.unmount.delete(r.dispose); // idempotent under a second call
        for (const n of b.owned)
            n.parentNode?.removeChild(n);
    };
    _signals_root.unmount.add(r.dispose);
    return r;
}
export function getActiveSub() { return activeSub; }
export function setActiveSub(sub) { const prevSub = activeSub; activeSub = sub; return prevSub; }
export function getBatchDepth() { return batchDepth; }
export function startBatch() { ++batchDepth; }
export function endBatch() { if (!--batchDepth) {
    flush();
} }
export function isSignal(fn) { return fn.name === 'bound ' + signalOper.name; }
export function isComputed(fn) { return fn.name === 'bound ' + computedOper.name; }
export function isEffect(fn) { return fn.name === 'bound ' + effectOper.name; }
export function isEffectScope(fn) { return fn.name === 'bound ' + effectScopeOper.name; }
export function computed(getter) {
    return computedOper.bind({
        value: undefined,
        subs: undefined,
        subsTail: undefined,
        deps: undefined,
        depsTail: undefined,
        flags: 0,
        getter: getter,
    });
}
export function effect(fn) {
    const e = {
        fn,
        cleanup: undefined,
        subs: undefined,
        subsTail: undefined,
        deps: undefined,
        depsTail: undefined,
        flags: 2 | 4,
    };
    const prevSub = setActiveSub(e);
    if (prevSub !== undefined) {
        link(e, prevSub, 0);
        prevSub.flags |= HasChildEffect;
    }
    try {
        ++runDepth;
        e.cleanup = e.fn();
    }
    finally {
        --runDepth;
        activeSub = prevSub;
        e.flags &= ~4;
    }
    return effectOper.bind(e);
}
export function effectScope(fn) {
    const e = {
        deps: undefined,
        depsTail: undefined,
        subs: undefined,
        subsTail: undefined,
        flags: 1,
    };
    const prevSub = setActiveSub(e);
    if (prevSub !== undefined) {
        link(e, prevSub, 0);
        prevSub.flags |= HasChildEffect;
    }
    try {
        fn();
    }
    finally {
        activeSub = prevSub;
    }
    return effectScopeOper.bind(e);
}
export function trigger(fn) {
    const sub = {
        deps: undefined,
        depsTail: undefined,
        flags: 2,
    };
    const prevSub = setActiveSub(sub);
    try {
        fn();
    }
    finally {
        activeSub = prevSub;
        sub.flags = 0;
        let link = sub.deps;
        while (link !== undefined) {
            const dep = link.dep;
            link = unlink(link, sub);
            const subs = dep.subs;
            if (subs !== undefined) {
                propagate(subs, !!runDepth);
                shallowPropagate(subs);
            }
        }
        if (!batchDepth) {
            flush();
        }
    }
}
export function createReactiveSystem({ update, notify, unwatched, }) {
    return {
        link,
        unlink,
        propagate,
        checkDirty,
        shallowPropagate,
    };
    function link(dep, sub, version) {
        const prevDep = sub.depsTail;
        if (prevDep !== undefined && prevDep.dep === dep) {
            return;
        }
        const nextDep = prevDep !== undefined ? prevDep.nextDep : sub.deps;
        if (nextDep !== undefined && nextDep.dep === dep) {
            nextDep.version = version;
            sub.depsTail = nextDep;
            return;
        }
        const prevSub = dep.subsTail;
        if (prevSub !== undefined && prevSub.version === version && prevSub.sub === sub) {
            return;
        }
        const newLink = sub.depsTail
            = dep.subsTail
                = {
                    version,
                    dep,
                    sub,
                    prevDep,
                    nextDep,
                    prevSub,
                    nextSub: undefined,
                };
        if (nextDep !== undefined) {
            nextDep.prevDep = newLink;
        }
        if (prevDep !== undefined) {
            prevDep.nextDep = newLink;
        }
        else {
            sub.deps = newLink;
        }
        if (prevSub !== undefined) {
            prevSub.nextSub = newLink;
        }
        else {
            dep.subs = newLink;
        }
    }
    function unlink(link, sub = link.sub) {
        const { dep, prevDep, nextDep, nextSub, prevSub } = link;
        if (nextDep !== undefined) {
            nextDep.prevDep = prevDep;
        }
        else {
            sub.depsTail = prevDep;
        }
        if (prevDep !== undefined) {
            prevDep.nextDep = nextDep;
        }
        else {
            sub.deps = nextDep;
        }
        if (nextSub !== undefined) {
            nextSub.prevSub = prevSub;
        }
        else {
            dep.subsTail = prevSub;
        }
        if (prevSub !== undefined) {
            prevSub.nextSub = nextSub;
        }
        else if ((dep.subs = nextSub) === undefined) {
            unwatched(dep);
        }
        return nextDep;
    }
    function propagate(link, innerWrite) {
        let next = link.nextSub;
        let stack;
        top: do {
            const sub = link.sub;
            let flags = sub.flags;
            if (!(flags & (4 | 8 | 16 | 32))) {
                sub.flags = flags | 32;
                if (innerWrite) {
                    sub.flags |= 8;
                }
            }
            else if (!(flags & (4 | 8))) {
                flags = 0;
            }
            else if (!(flags & 4)) {
                sub.flags = (flags & ~8) | 32;
            }
            else if (!(flags & (16 | 32)) && isValidLink(link, sub)) {
                sub.flags = flags | (8 | 32);
                flags &= 1;
            }
            else {
                flags = 0;
            }
            if (flags & 2) {
                notify(sub);
            }
            if (flags & 1) {
                const subSubs = sub.subs;
                if (subSubs !== undefined) {
                    const nextSub = (link = subSubs).nextSub;
                    if (nextSub !== undefined) {
                        stack = { value: next, prev: stack };
                        next = nextSub;
                    }
                    continue;
                }
            }
            if ((link = next) !== undefined) {
                next = link.nextSub;
                continue;
            }
            while (stack !== undefined) {
                link = stack.value;
                stack = stack.prev;
                if (link !== undefined) {
                    next = link.nextSub;
                    continue top;
                }
            }
            break;
        } while (true);
    }
    function checkDirty(link, sub) {
        let stack;
        let checkDepth = 0;
        let dirty = false;
        top: do {
            const dep = link.dep;
            const flags = dep.flags;
            if (sub.flags & 16) {
                dirty = true;
            }
            else if ((flags & (1 | 16)) === (1 | 16)) {
                const subs = dep.subs;
                if (update(dep)) {
                    if (subs.nextSub !== undefined) {
                        shallowPropagate(subs);
                    }
                    dirty = true;
                }
            }
            else if ((flags & (1 | 32)) === (1 | 32)) {
                stack = { value: link, prev: stack };
                link = dep.deps;
                sub = dep;
                ++checkDepth;
                continue;
            }
            if (!dirty) {
                const nextDep = link.nextDep;
                if (nextDep !== undefined) {
                    link = nextDep;
                    continue;
                }
            }
            while (checkDepth--) {
                link = stack.value;
                stack = stack.prev;
                if (dirty) {
                    const subs = sub.subs;
                    if (update(sub)) {
                        if (subs.nextSub !== undefined) {
                            shallowPropagate(subs);
                        }
                        sub = link.sub;
                        continue;
                    }
                    dirty = false;
                }
                else {
                    sub.flags &= ~32;
                }
                sub = link.sub;
                const nextDep = link.nextDep;
                if (nextDep !== undefined) {
                    link = nextDep;
                    continue top;
                }
            }
            return dirty && !!sub.flags;
        } while (true);
    }
    function shallowPropagate(link) {
        do {
            const sub = link.sub;
            const flags = sub.flags;
            if ((flags & (32 | 16)) === 32) {
                sub.flags = flags | 16;
                if ((flags & (2 | 4)) === 2) {
                    notify(sub);
                }
            }
        } while ((link = link.nextSub) !== undefined);
    }
    function isValidLink(checkLink, sub) {
        let link = sub.depsTail;
        while (link !== undefined) {
            if (link === checkLink) {
                return true;
            }
            link = link.prevDep;
        }
        return false;
    }
}
export function signal(initialValue) {
    return signalOper.bind({
        currentValue: initialValue,
        pendingValue: initialValue,
        subs: undefined,
        subsTail: undefined,
        flags: 1,
    });
}
export function signalRx(initialValue, injectDom = true, __sauron_id__ = _rxid()) {
    const el = injectDom ? document.createElement('span') : null;
    el?.setAttribute('data-sid', __sauron_id__);
    return makeRx(__sauron_id__, {
        v: initialValue, node: el, el, owned: el ? [el] : [],
        write(v) { if (el)
            el.textContent = str(v); },
    });
}
export function signalRxTxt(initialValue, node, injectDom = true, __sauron_id__ = _rxid()) {
    let text = node ?? null;
    const owned = [];
    if (!text && injectDom)
        owned.push(document.createComment(__sauron_id__), (text = document.createTextNode('')));
    return makeRx(__sauron_id__, {
        v: initialValue, node: text, el: text?.parentElement ?? null, owned,
        write(v) { if (text)
            text.data = str(v); },
    });
}
export function signalRxAtr(initialValue, node, attributeName = 'value', injectDom = true, __sauron_id__ = _rxid()) {
    if (node && injectDom && !node.hasAttribute('data-sid'))
        node.setAttribute('data-sid', __sauron_id__);
    const live = node instanceof HTMLInputElement && attributeName === 'value' ? node : null;
    return makeRx(__sauron_id__, {
        v: initialValue, node, el: node ?? null, owned: [],
        write(v) {
            if (!node)
                return;
            if (v == null || v === false)
                node.removeAttribute(attributeName);
            else
                node.setAttribute(attributeName, v === true ? '' : str(v));
            if (live && live.value !== str(v) && document.activeElement !== live)
                live.value = str(v);
        },
    });
}
export function scanRoot(root = document, componentId = _signals_root.id) {
    const out = Object.create(null);
    for (const el of root.querySelectorAll('[data-sid]')) {
        out[el.getAttribute('data-sid')] = el.getAttribute('value') ?? el.textContent;
    }
    const w = document.createTreeWalker(root, NodeFilter.SHOW_COMMENT);
    while (w.nextNode()) {
        const c = w.currentNode;
        if (c.data.startsWith('rx'))
            out[c.data] = c.nextSibling?.textContent ?? null;
    }
    return out;
}
// #endregion
//                                                                                                [DARTH MAUL] DISRPTOR SIGNALS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class SIGNALS {
    root;
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    hub;
    signals;
    selfId;
    #SIGNAL_CELLS = Object.create(null);
    _signal_cells;
    #SIGNAL_LISTENERS = Object.create(null);
    _signal_listeners;
    #SIGNAL_GLOBAL_LISTENERS = Object.create(null);
    _signal_global_listeners;
    #SIGNAL_WAITERS = Object.create(null);
    _signal_waiters;
    #SIGNAL_PERSISTENT = Object.create(null);
    _signal_persistent;
    #SIGNAL_PERSISTENT_KEYS = Object.create(null);
    _signal_persistent_keys;
    get_(c = "signal_cells") {
        let _;
        switch (c) {
            case "signal_cells": {
                _ = this._signal_cells;
                break;
            }
            case "signal_listeners": {
                _ = this._signal_listeners;
                break;
            }
            case "signal_global_listeners": {
                _ = this._signal_global_listeners;
                break;
            }
            case "signal_waiters": {
                _ = this._signal_waiters;
                break;
            }
            case "signal_persistent": {
                _ = this._signal_persistent;
                break;
            }
            case "signal_persistent_keys": {
                _ = this._signal_persistent_keys;
                break;
            }
            default: {
                break;
            }
        }
        return _;
    }
    getHash(c = "signal_cells", r = this.r) {
        let root;
        switch (c) {
            case "signal_cells": {
                root = this.#SIGNAL_CELLS[r];
                break;
            }
            case "signal_listeners": {
                root = this.#SIGNAL_LISTENERS[r];
                break;
            }
            case "signal_global_listeners": {
                root = this.#SIGNAL_GLOBAL_LISTENERS[r];
                break;
            }
            case "signal_waiters": {
                root = this.#SIGNAL_WAITERS[r];
                break;
            }
            case "signal_persistent": {
                root = this.#SIGNAL_PERSISTENT[r];
                break;
            }
            case "signal_persistent_keys": {
                root = this.#SIGNAL_PERSISTENT_KEYS[r];
                break;
            }
            default: {
                break;
            }
        }
        return root;
    }
    get_root(c = "signal_cells", r) {
        let root;
        switch (c) {
            case "signal_cells": {
                root = this.#SIGNAL_CELLS[r];
                break;
            }
            case "signal_listeners": {
                root = this.#SIGNAL_LISTENERS[r];
                break;
            }
            case "signal_global_listeners": {
                root = this.#SIGNAL_GLOBAL_LISTENERS[r];
                break;
            }
            case "signal_waiters": {
                root = this.#SIGNAL_WAITERS[r];
                break;
            }
            case "signal_persistent": {
                root = this.#SIGNAL_PERSISTENT[r];
                break;
            }
            case "signal_persistent_keys": {
                root = this.#SIGNAL_PERSISTENT_KEYS[r];
                break;
            }
        }
        if (!root) {
            root = Object.create(null);
            switch (c) {
                case "signal_cells": {
                    this.#SIGNAL_CELLS[r] = root;
                    break;
                }
                case "signal_listeners": {
                    this.#SIGNAL_LISTENERS[r] = root;
                    break;
                }
                case "signal_global_listeners": {
                    this.#SIGNAL_GLOBAL_LISTENERS[r] = root;
                    break;
                }
                case "signal_waiters": {
                    this.#SIGNAL_WAITERS[r] = root;
                    break;
                }
                case "signal_persistent": {
                    this.#SIGNAL_PERSISTENT[r] = root;
                    break;
                }
                case "signal_persistent_keys": {
                    this.#SIGNAL_PERSISTENT_KEYS[r] = root;
                    break;
                }
            }
        }
        return root;
    }
    getS(s = this.s, m = this.m, r = this.r, c = "signal_cells") {
        const rL = this.get_root(c, r);
        const mL = (rL[m] ??= Object.create(null));
        const sL = (mL[s] ??= Object.create(null));
        return sL;
    }
    getM(m = this.m, r = this.r, c = "signal_cells") {
        const rL = this.get_root(c, r);
        const mL = (rL[m] ??= Object.create(null));
        return mL;
    }
    getR(r = this.r, c = "signal_cells") {
        const rL = this.get_root(c, r);
        return rL;
    }
    get = (() => {
        const fn = (k, s = this.s, m = this.m, r = this.r, c = "signal_cells") => {
            const rL = this.get_root(c, r);
            const mL = (rL[m] ??= Object.create(null));
            const registry = (mL[s] ??= Object.create(null));
            const z = registry[k];
            if (!z)
                return;
            else
                return z;
        };
        return fn;
    })();
    getHot(k) {
        return this._signal_cells[k];
    }
    read = (() => {
        const fn = (k, s = this.s, m = this.m, r = this.r, c = "signal_cells") => {
            const rL = this.get_root(c, r);
            const mL = (rL[m] ??= Object.create(null));
            const registry = (mL[s] ??= Object.create(null));
            const z = registry[k];
            if (!z)
                return;
            else
                return z;
        };
        return fn;
    })();
    readHot(k) {
        return this._signal_cells[k];
    }
    write = (() => {
        const fn = (v, k, s = this.s, m = this.m, r = this.r, c = "signal_cells") => {
            const rL = this.get_root(c, r);
            const mL = (rL[m] ??= Object.create(null));
            const registry = (mL[s] ??= Object.create(null));
            const t = UF(v);
            const atom = signal(t);
            registry[k] = atom;
            return atom;
        };
        return fn;
    })();
    setHot(v, k) {
        const registry = this._signal_cells;
        const t = UF(v);
        const atom = signal(t);
        registry[k] = atom;
        return atom;
    }
    notifyKeyListeners(key, value, opts) {
        const listeners = this._signal_listeners[key];
        if (!listeners)
            return;
        for (const listener of listeners) {
            try {
                listener(value);
            }
            catch (err) {
                console.error(`[Sauron Signal Listener Error]:`, err);
            }
        }
    }
    scheduleGlobalNotify(key, value, opts) {
        const globalListeners = this._signal_global_listeners["_signal_global_listeners_raven"];
        if (globalListeners) {
            for (const listener of globalListeners) {
                try {
                    listener(key, value);
                }
                catch (err) {
                    console.error(`[Sauron Signal Global Listener Error]:`, err);
                }
            }
        }
        const waiters = this._signal_waiters[key];
        if (waiters) {
            const resolved = Array.from(waiters);
            delete this._signal_waiters[key];
            for (const resolve of resolved) {
                try {
                    resolve(value);
                }
                catch (err) {
                    console.error(`[Sauron Signal Waiter Error]:`, err);
                }
            }
        }
    }
    notifyAll(key, value, opts) {
        this.notifyKeyListeners(key, value, opts);
        this.scheduleGlobalNotify(key, value, opts);
    }
    atom = (() => {
        const fn = (v, k = `__THE_WARLOCK_SIGNALS_${this.hub.reg._sid()}`, s = this.s, m = this.m, r = this.r) => {
            return {
                get value() { return this.signal(undefined, k, s, m, r); },
                get current() { return this.signal(undefined, k, s, m, r); },
                peek() { return this.signal(undefined, k, s, m, r); },
                set value(v) { this.signal(v, k, s, m, r); },
                set current(v) { this.signal(v, k, s, m, r); },
                set crt(v) { this.signal(v, k, s, m, r); },
                set set(v) { this.signal(v, k, s, m, r); },
            };
        };
        fn.hot = (v, k = `__THE_WARLOCK_SIGNALS_${this.hub.reg._sid()}`, s = this.s, m = this.m, r = this.r) => {
            return {
                get value() { return this.signal.hot(k); },
                get current() { return this.signal.hot(k); },
                peek() { return this.signal.hot(k); },
                set value(v) { this.signal.hot(k, v); },
                set current(v) { this.signal.hot(k, v); },
                set crt(v) { this.signal.hot(k, v); },
                set set(v) { this.signal.hot(k, v); },
            };
        };
        return fn;
    })();
    signal = (() => {
        const fn = (v, k, s = this.s, m = this.m, r = this.r, c = "signal_cells") => {
            const rL = this.get_root(c, r);
            const mL = (rL[m] ??= Object.create(null));
            const registry = (mL[s] ??= Object.create(null));
            let atom = registry[k];
            if (atom && v === undefined) {
                return atom;
            }
            if (atom && v !== undefined) {
                const t = UF(v);
                registry[k] = t;
                return t;
            }
            const initialValue = UF(v);
            atom = signal(initialValue);
            registry[k] = atom;
            return atom;
        };
        fn.hot = (k, v) => {
            const registry = this._signal_cells;
            let atom = registry[k];
            if (atom && v === undefined) {
                return atom;
            }
            if (atom && v !== undefined) {
                const t = UF(v);
                registry[k] = t;
                return t;
            }
            const initialValue = UF(v);
            atom = signal(initialValue);
            registry[k] = atom;
            return atom;
        };
        return fn;
    })();
    store = {
        get: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const value = this.get(key, s, m, r);
            if (value !== undefined) {
                return MaybePromise(value);
            }
            const persistent = this._signal_persistent;
            const persistKeys = this._signal_persistent_keys;
            if (key in persistKeys && key in persistent) {
                const hydrated = persistent[key];
                this.write(hydrated, key, s, m, r);
                return MaybePromise(hydrated);
            }
            return MaybePromise(undefined);
        },
        set: (key, value, opts = { s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200 }) => {
            const { triggerRender = true, s, m, r } = opts;
            const prev = this.get(key, s, m, r);
            const next = typeof value === "function" ? value(prev) : value;
            if (Object.is(prev, next)) {
                return MaybePromise(prev);
            }
            this.write(next, key, s, m, r);
            if (key in this._signal_persistent_keys) {
                this._signal_persistent[key] = next;
            }
            this.notifyKeyListeners(key, next, { s, m, r });
            if (triggerRender) {
                this.scheduleGlobalNotify(key, next, { s, m, r });
            }
            return MaybePromise(next);
        },
        subscribe: (key, listener, opts = { s: this.s, m: this.m, r: this.r }) => {
            const listeners = this._signal_listeners;
            if (!(key in listeners)) {
                listeners[key] = new Set();
            }
            listeners[key].add(listener);
            return () => {
                const current = listeners[key];
                if (!current)
                    return;
                current.delete(listener);
                if (current.size === 0)
                    delete listeners[key];
            };
        },
        delete: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const cells = this._signal_cells;
            const existed = key in cells;
            if (existed)
                delete cells[key];
            if (key in this._signal_persistent_keys) {
                delete this._signal_persistent_keys[key];
                delete this._signal_persistent[key];
            }
            this.notifyKeyListeners(key, undefined, { s, m, r });
            if (existed)
                this.scheduleGlobalNotify(key, undefined, { s, m, r });
            delete this._signal_listeners[key];
            return MaybePromise(existed);
        },
        entries: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const cells = this._signal_cells;
            const result = [];
            for (const key of Object.keys(cells)) {
                const value = cells[key];
                if (value !== undefined)
                    result.push([key, value]);
            }
            return MaybePromise(result);
        },
        persist: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const value = this.get(key, s, m, r);
            this._signal_persistent_keys[key] = true;
            if (value !== undefined)
                this._signal_persistent[key] = value;
        },
        unpersist: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            delete this._signal_persistent_keys[key];
            delete this._signal_persistent[key];
        },
        has: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const value = this.get(key, s, m, r);
            return MaybePromise(value !== undefined);
        },
        clear: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const cells = this._signal_cells;
            for (const key of Object.keys(cells)) {
                delete cells[key];
                this.notifyKeyListeners(key, undefined, opts);
                this.scheduleGlobalNotify(key, undefined, opts);
            }
            for (const key of Object.keys(this._signal_listeners)) {
                delete this._signal_listeners[key];
            }
        },
        clearPersistent: (opts = { s: this.s, m: this.m, r: this.r }) => {
            for (const k of Object.keys(this._signal_persistent))
                delete this._signal_persistent[k];
            for (const k of Object.keys(this._signal_persistent_keys))
                delete this._signal_persistent_keys[k];
        },
        _emit: (key, s = this.s, m = this.m, r = this.r) => {
            const opts = { s, m, r };
            const value = this.get(key, s, m, r);
            if (value === undefined)
                return;
            this.notifyAll(key, value, opts);
        },
    };
    state = {
        get: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            return this.store.get(key, opts);
        },
        set: (key, value, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r, triggerRender = true } = opts;
            this.store.set(key, value, { s, m, r, triggerRender });
        },
        delete: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            return this.store.delete(key, opts);
        },
        getByPrefix: (prefix, opts = { s: this.s, m: this.m, r: this.r }) => {
            return this.store.entries(opts)
                .filter(([key]) => typeof key === "string" && key.startsWith(prefix))
                .map(([key, value]) => ({ key: key, value }));
        },
        getAll: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const result = {};
            for (const [key, value] of this.store.entries(opts))
                result[key] = value;
            return result;
        },
        subscribe: (key, listener, opts = { s: this.s, m: this.m, r: this.r }) => {
            return this.store.subscribe(key, listener, opts);
        },
        subscribeAll: (listener, opts = { s: this.s, m: this.m, r: this.r }) => {
            const listeners = this._signal_global_listeners;
            const K = "_signal_global_listeners_raven";
            if (!(K in listeners))
                listeners[K] = new Set();
            const set = listeners[K];
            set.add(listener);
            return () => {
                set.delete(listener);
                if (set.size === 0)
                    delete listeners[K];
            };
        },
        clear: (opts = { s: this.s, m: this.m, r: this.r }) => {
            this.store.clear(opts);
            for (const key of Object.keys(this._signal_waiters)) {
                delete this._signal_waiters[key];
            }
            const globalListeners = this._signal_global_listeners["_signal_global_listeners_raven"];
            globalListeners?.clear();
        },
        waitFor: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const existing = this.state.get(key, opts);
            if (existing !== undefined)
                return Promise.resolve(existing);
            return new Promise((resolve) => {
                const waiters = this._signal_waiters;
                const k = key;
                if (!(k in waiters))
                    waiters[k] = new Set();
                waiters[k].add(resolve);
            });
        },
    };
    cell = (() => {
        const fn = (key, s, m, r) => {
            return MaybePromise(this.state.get(key, { s, m, r }));
        };
        fn.getAll = (opts) => {
            return this.state.getAll(opts);
        };
        fn.set = (label, value, triggerRender = true, s, m, r) => {
            this.cell._broadcastSS(label, value, "sauron", "set");
            return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
        };
        fn._broadcastSS = (key, value, storageType, fn, path, domain, options) => {
            _localChange = true;
            _channel?.postMessage({ key, value, storageType, fn, path, domain, options });
        };
        fn.subAll = (fn, s, m, r) => {
            if (!fn)
                return () => { };
            return this.state.subscribeAll(fn, { s, m, r });
        };
        fn.sub = (k, fn, s, m, r) => {
            if (!k || !fn)
                return () => { };
            return MaybePromise(this.state.subscribe(k, fn, { s, m, r }));
        };
        fn.delete = (label, s, m, r) => {
            if (!label)
                return false;
            const v = this.state.delete(label, { s, m, r });
            this.cell._broadcastSS(label, undefined, "sauron", "delete");
            return v;
        };
        fn.waitFor = (label, s, m, r) => {
            return this.state.waitFor(label, { s, m, r });
        };
        fn.deleteMany = async (keys, s, m, r) => {
            if (!Array.isArray(keys) || keys.length === 0) {
                return false;
            }
            let allDeleted = true;
            // Batch all deletes without triggering renders until the end
            for (let i = 0; i < keys.length; i++) {
                const deleted = this.cell.delete(keys[i], s, m, r);
                if (!deleted) {
                    allDeleted = false;
                }
            }
            return allDeleted;
        };
        fn.setMany = async (entries) => {
            if (!entries || typeof entries !== "object") {
                return;
            }
            // Batch all sets without triggering renders until the end
            const entriesArray = Object.entries(entries);
            for (let i = 0; i < entriesArray.length; i++) {
                const [key, value] = entriesArray[i];
                // Only trigger render on the last item to avoid multiple renders
                const triggerRender = i === entriesArray.length - 1;
                this.cell.set(key, value, triggerRender);
            }
        };
        fn.getMany = async (keys, s, m, r) => {
            if (!Array.isArray(keys) || keys.length === 0) {
                return {};
            }
            const result = {};
            const promises = keys.map(async (key) => {
                const value = await this.cell(key, s, m, r);
                return { key, value };
            });
            const results = await Promise.all(promises);
            for (const { key, value } of results) {
                result[key] = value;
            }
            return result;
        };
        return fn;
    })();
    state_setter(v, k = `_USECELL`, s = this.s, m = this.m, r = this.r) {
        const existing = this.state.get(k, { s: s, m: m, r: r });
        if (existing !== undefined)
            return existing;
        const t = typeof v === "function" ? v() : v;
        this.state.set(k, t, { triggerRender: true, s: s, m: m, r: r });
        return t;
    }
    ATOM = (() => {
        function mergeInto(target, source) {
            Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
            return target;
        }
        function resolveFactory(raw) {
            return typeof raw === "function" ? raw : () => raw;
        }
        const NINJA_STUB_BRAND = "__is_atom_stub__";
        function makeStub(trace) {
            return new Proxy(() => makeStub(trace), {
                get: (_t, prop) => (prop === NINJA_STUB_BRAND ? true : makeStub(trace)),
                apply: () => makeStub(trace),
            });
        }
        function withTypoGuard(result, label) {
            return new Proxy(result, {
                get(target, prop, receiver) {
                    if (typeof prop === "symbol" || prop in target)
                        return Reflect.get(target, prop, receiver);
                    throw new Error(`\n[${label} Typo Error] Property "${String(prop)}" does not exist on this ${label} definition.\n  -> Available definitions: [ ${Object.keys(target).join(", ")} ]\n`);
                },
            });
        }
        // ============================================================================
        //  ONE CELL PER MASTER INSTANCE — the platform primitives, nothing else.
        //      const { selfProxy, liveTarget } = createLiveSelf(instanceId)
        //      Object.assign(liveTarget, pass)
        //      const [o, o$] = root.DISRUPTOR.useCell.obj(liveTarget, label, category)
        //      o$.merge(result)
        //  liveTarget IS the cell value: the mailbox seeds the store with it and
        //  setLive sends that same object, so self and the cell are one object with
        //  one write path. Namespaces are one level: liveTarget.<name>.
        // ============================================================================
        function createLiveSelf(instanceId) {
            const liveTarget = {};
            const selfProxy = new Proxy(liveTarget, {
                get: (t, k) => t[k],
                set: (t, k, v) => { t[k] = v; this.hub.root.state.state.set(`${instanceId}:${k}`, v); return true; },
            });
            return { selfProxy, liveTarget };
        }
        function nsSelf(liveTarget, name, publish, useCell) {
            const rec = () => (liveTarget[name] ??= {});
            return new Proxy({}, {
                get(_t, key) {
                    if (typeof key !== "string" || key === "then")
                        return undefined;
                    if (key === "useCell")
                        return useCell;
                    if (key in rec())
                        return rec()[key];
                    if (key in liveTarget) {
                        if (liveTarget[key]?._isNamespace)
                            throw new Error(`[isolation] "${name}" cannot reach namespace "${key}"`);
                        return liveTarget[key];
                    }
                    return undefined;
                },
                set(_t, key, value) {
                    if (typeof key !== "string")
                        return false;
                    rec()[key] = value;
                    publish();
                    return true;
                },
                has(_t, key) { return typeof key === "string" && (key in rec() || key in liveTarget); },
            });
        }
        // The atom's useCell: [own record, merge into own record].
        function cellHandle(record, publish) {
            const merge = (patch) => {
                const p = typeof patch === "function" ? patch(record()) : patch;
                if (p && typeof p === "object")
                    Object.assign(record(), p);
                publish();
            };
            const h = [record(), merge];
            h.get = (key) => (key === undefined ? record() : record()[key]);
            h.set = (key, value) => {
                if (typeof key === "string")
                    merge({ [key]: value });
                else if (key && typeof key === "object")
                    merge(key);
                return h;
            };
            return h;
        }
        function requiredBindables() {
            return { gate: SIGNALS["signals_raptor"]["ATOMS"]["GATE"], subscription: SIGNALS["signals_raptor"]["ATOMS"]["SUBSCRIPTION"], context: SIGNALS["signals_raptor"]["ATOMS"]["CONTEXT"] };
        }
        function withoutKeys(obj, keys) {
            const desc = Object.getOwnPropertyDescriptors(obj);
            for (const k of keys)
                delete desc[k];
            return desc;
        }
        function createMaster(kind, config) {
            const kindFactory = resolveFactory(this.signals.signals.state.get(kind));
            if (!kindFactory)
                throw new TypeError(`Unknown master atom: ${kind}`);
            const instanceId = `${kind}:${this.root.reg._sid()}`;
            const { selfProxy, liveTarget } = createLiveSelf(instanceId);
            const names = new Set();
            const refreshers = {};
            let o$;
            const publish = () => {
                if (!o$)
                    return; // still seeding: the first merge publishes the whole record
                for (const f of Object.values(refreshers))
                    f();
                o$.merge({});
                for (const n of names)
                    liveTarget[n]._version = liveTarget._version;
            };
            const bind = (name, factory, cfg) => {
                if (name in liveTarget && !names.has(name))
                    throw new Error(`[COLLISION] bound atom "${name}" collides with master field "${name}"`);
                names.add(name);
                liveTarget[name] = Object.assign(liveTarget[name] ?? {}, { _isNamespace: true });
                const useCell = () => cellHandle(() => liveTarget[name], publish);
                const self = nsSelf(liveTarget, name, publish, useCell);
                const ctx = { self, useCell, channel: (l) => this.signals.signals.state.subscribe(instanceId, l), master: selfProxy, config: cfg, k: root.k, s: root.s, m: root.m, r: root.r };
                const out = factory(ctx);
                Object.defineProperties(liveTarget[name], withoutKeys(out, ["bindables", "refresh"]));
                if (typeof out.refresh === "function")
                    refreshers[name] = out.refresh;
            };
            const view = (name) => {
                const rec = liveTarget[name];
                return new Proxy(rec, {
                    get: (t, k) => (typeof t[k] === "function" ? t[k].bind(rec) : t[k]),
                    set: (t, k, v) => { t[k] = v; publish(); return true; },
                });
            };
            const masterCtx = (cfg) => {
                const ctx = { self: selfProxy, useCell: () => cellHandle(() => liveTarget, publish), channel: (l) => this.root.signals.signals.state.subscribe(instanceId, l), config: cfg, master: selfProxy, k: root.k, s: root.s, m: root.m, r: root.r };
                for (const name of names)
                    ctx[name] = view(name);
                return ctx;
            };
            // seed: config lives at the root, same level as the namespaces
            for (const [key, v] of Object.entries(config)) {
                if (key === "_version")
                    throw new Error(`[COLLISION] "_version" is reserved by the cell`);
                liveTarget[key] = v;
            }
            // open the cell on the seeded object — this IS the resource from here on
            const [cell, handle] = this.root.bus.drs.useCell.obj(liveTarget, instanceId, kind);
            o$ = handle;
            // 1. required bound atoms first — the master reads gate.sealed on read()
            for (const [name, factory] of Object.entries(requiredBindables()))
                bind(name, factory, config);
            // 2. probe for the master's own bindables list, then build those too
            const probe = kindFactory(masterCtx(config));
            for (const [name, factory] of Object.entries(probe.bindables ?? {}))
                bind(name, resolveFactory(factory), config);
            // 3. re-run the factory now that every namespace exists (live views)
            const built = kindFactory(masterCtx(config));
            // 4. the master's own fields into the root
            const masterFields = withoutKeys(built, ["bindables", "value"]);
            for (const key of Object.keys(masterFields)) {
                if (key === "_version")
                    throw new Error(`[COLLISION] "_version" is reserved by the cell`);
                if (names.has(key))
                    throw new Error(`[COLLISION] master field "${key}" collides with bound atom "${key}"`);
            }
            Object.defineProperties(liveTarget, masterFields);
            // 5. the public surface: flat master ops + one handle per namespace
            const api = { atom: kind };
            for (const name of names)
                api[name] = view(name);
            for (const fn of ["read", "write", "subscribe", "waitFor"])
                if (fn in liveTarget)
                    api[fn] = liveTarget[fn];
            for (const key of Object.keys(masterFields)) {
                if (key in api)
                    continue;
                if (typeof liveTarget[key] === "function") {
                    api[key] = liveTarget[key];
                    continue;
                }
                Object.defineProperty(api, key, { enumerable: true, get: () => liveTarget[key] });
            }
            api.version = () => cell._version;
            publish(); // build is one publish: every namespace refreshes together
            return withTypoGuard(api, instanceId.split(":")[1]);
        }
        const z = () => { };
        z.MASTER = (name, arg) => {
            const f = resolveFactory(arg);
            if (this.root && this.root.state)
                this.root.state.state.set(name, f);
            return f;
        };
        z.BIND = (name, arg) => { const f = resolveFactory(arg); if (this.root && this.root.state)
            this.root.state.state.set(name, f); return f; };
        z.BINDABLE = (factory) => { return factory; };
        z.CONTRACT = (arg1, arg2) => {
            let num = 0;
            const id = this.root && this.root.reg ? `${this.root.reg._sid()}` : `${++num}`;
            const categoryName = typeof arg1 === "string" ? arg1 : `ATOM_CONTRACT${id}`;
            const rawConfig = typeof arg1 === "string" ? arg2 : arg1;
            if (rawConfig === undefined || rawConfig === null)
                throw new Error("Self is required");
            const factory = resolveFactory(rawConfig);
            const pass = factory({});
            const kind = pass.atom ?? "ATOM";
            // atomContract(7) and atomContract({ value: 7 }) both mean the same thing
            const contract = ((config = {}) => {
                const normalized = config && typeof config === "object" ? config : { value: config };
                return createMaster(kind, normalized);
            });
            Object.assign(contract, pass);
            if (this.root && this.root.signals && this.root.signals.signals && this.root.signals.signals.state)
                this.root.signals.signals.state.set(categoryName, contract);
            return contract;
        };
        return z;
    })();
    MASTERS = (() => {
        const z = () => { };
        z.SAURON = this.ATOM.MASTER("SAURON", ({ self, master, config }) => ({}));
        z.MSG_Q = this.ATOM.MASTER("MSG_Q", ({ self, master, config }) => ({}));
        z.SLAB = this.ATOM.MASTER("SLAB", ({ self, master, config }) => ({}));
        z.DRS = this.ATOM.MASTER("DRS", ({ self, master, config }) => ({}));
        z.SUB = this.ATOM.MASTER("SUB", ({ self, master, config }) => ({}));
        z.STREAM = this.ATOM.MASTER("STREAM", ({ self, master, config }) => ({}));
        z.SPREADSHEET = this.ATOM.MASTER("SPREADSHEET", ({ self, master, config }) => ({}));
        z.SEND = this.ATOM.MASTER("SEND", ({ self, master, config }) => ({}));
        z.CLOUD = this.ATOM.MASTER("CLOUD", ({ self, master, config }) => ({}));
        z.DUPLEX = this.ATOM.MASTER("DUPLEX", ({ self, master, config }) => ({}));
        z.MODULE = this.ATOM.MASTER("MODULE", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            pipeline: 'A',
            send(message, signal) { },
            dispatch(hook, ctx, terminal) { },
            deliver(ctx) { },
        }));
        z.MODULE_STORAGE = this.ATOM.MASTER("MODULE_STORAGE_PROTON", ({ self, master, config }) => ({}));
        z.S_CLASS = this.ATOM.MASTER("S_CLASS", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            pipeline: 'A',
            send(message, signal) { },
            dispatch(hook, ctx, terminal) { },
            deliver(ctx) { },
        }));
        z.S_CLASS_STORAGE = this.ATOM.MASTER("S_CLASS_STORAGE_PROTON", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            pipeline: 'A',
            send(message, signal) { },
            dispatch(hook, ctx, terminal) { },
            deliver(ctx) { },
        }));
        z.ATOM = this.ATOM.MASTER('ATOM', ({ self, config, gate, subscription, SEALED_ERROR, conflict }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            value: config.value,
            read() {
                const r = self.value;
                if (r !== null) {
                    if (gate?.sealed || gate?.frozen)
                        throw new SEALED_ERROR.log(self.k);
                }
                return self.value;
            },
            write(next) {
                if (gate?.sealed || gate?.frozen)
                    return false;
                const current = self.value;
                if (Object.is(current, next))
                    return false;
                const decision = conflict.check({
                    current,
                    incoming: next,
                    previous: self.previous ?? current,
                });
                if (!decision.accepted) {
                    return false;
                }
                const resolved = decision.value;
                if (Object.is(current, resolved))
                    return false;
                self.useCell("__sauron_useCell_3f71936c__").set("value", resolved);
                return true;
            },
            subscribe(listener) { return subscription.subscribe(listener); },
            waitFor() { },
        }));
        z.VERTEX = this.ATOM.MASTER("VERTEX", ({ self, config, gate, subscription, SEALED_ERROR }) => ({
            value: config.value,
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            pid: 1,
            slabRegistry: new Map(),
            ...(() => {
                const capacity = (() => {
                    const need = (value) => Array.isArray(value) ? value.length + 1 :
                        value instanceof Map || value instanceof Set ? value.size + 1 : 2;
                    const size = need(self.value);
                    return size > 8192 ? 64 :
                        size <= 64 ? 64 :
                            size <= 128 ? 128 :
                                size <= 256 ? 256 :
                                    size <= 1024 ? 1024 : 8192;
                })();
                const lane = new Int32Array(config.capacity ?? capacity).fill(-1);
                const ops = new Uint8Array(config.capacity ?? capacity);
                const posX = new Float32Array(config.capacity ?? capacity);
                const posY = new Float32Array(config.capacity ?? capacity);
                const val = new Int32Array(config.capacity ?? capacity);
                const freePids = new Int32Array(config.capacity ?? capacity);
                let freeHead = 0;
                let freeTail = 0;
                let laneLen = 0;
                let nextId = 0;
                const ids = new Map();
                const names = [];
                const pids = new Map();
                const addresses = new Map();
                const listener = [];
                const pidToHash = new Uint32Array(config.capacity ?? capacity);
                const channelMap = new Int32Array(config.capacity ?? capacity);
                const subChannelMap = new Int32Array(config.capacity ?? capacity);
                const tripChannelMap = new Int32Array(config.capacity ?? capacity);
                const dirtyPids = new Int32Array(config.capacity ?? capacity);
                for (let i = 1; i < lane.length; i++) {
                    freePids[freeTail++] = i;
                }
                lane[0] = TRACER_PID;
                val[TRACER_PID] = 0;
                ops[TRACER_PID] = OP.NONE;
                laneLen = 1;
                return {
                    capacity: lane.length,
                    lane,
                    ops,
                    posX,
                    posY,
                    val,
                    freePids,
                    freeHead,
                    freeTail,
                    laneLen,
                    ids,
                    names,
                    pids,
                    addresses,
                    nextId,
                    listener,
                    pidToHash,
                    nextAllocatedPid: 1,
                    channelMap,
                    subChannelMap,
                    tripChannelMap,
                    dirtyPids,
                };
            })(),
            need(value) { return Array.isArray(value) ? value.length + 1 : value instanceof Map || value instanceof Set ? value.size + 1 : 2; },
            BINARY_CODEC: new BINARY_CODEC(),
            ...(() => {
                const codec = new BINARY_CODEC();
                return {
                    codec,
                    encodeToSlab(pid, value) {
                        if (typeof value === "number") {
                            self.slabRegistry.delete(pid);
                            return value;
                        }
                        const bytes = codec.encode_msg(value);
                        self.slabRegistry.set(pid, bytes);
                        return bytes.byteLength;
                    },
                    clearSlab(pid) {
                        self.slabRegistry.delete(pid);
                    },
                    hasSlab(pid) {
                        return self.slabRegistry.has(pid);
                    },
                    readSlabValue(pid) {
                        const bytes = self.slabRegistry.get(pid);
                        return bytes ? codec.decode_msg(bytes) : null;
                    },
                };
            })(),
            ...(() => {
                const codec = self.BINARY_CODEC;
                const x = self.posX;
                const y = self.posY;
                const pid = self.pid;
                const initialize = () => {
                    const value = self.value;
                    const v = codec.encode_msg(value);
                    const position = self.getPosition(pid);
                    if (self.need(value) > 8192) {
                        self.encodeToSlab(pid, v);
                        x[pid] = position.x;
                        y[pid] = position.y;
                        self.ops[pid] = OP.WRITE;
                        self.lane[self.laneLen++] = pid;
                    }
                    else {
                        const vb = codec.encode_msg(v);
                        self.edit(position.x, position.y, vb, pid);
                    }
                };
                return { initialize };
            })(),
            check_size_and_edit(value) {
                const v = self.BINARY_CODEC.encode_msg(value);
                const { x, y } = self.getPosition(self.pid);
                if (self.need(value) > 8192) {
                    self.encodeToSlab(self.pid, v);
                    self.posX[self.pid] = x;
                    self.posY[self.pid] = y;
                    self.ops[self.pid] = OP.EDIT;
                    self.lane[self.laneLen++] = self.pid;
                }
                else {
                    self.create(x, y, v, self.pid);
                }
            },
            getX(pid) { return self.posX[pid]; },
            getY(pid) { return self.posY[pid]; },
            getPosition(pid) { return { x: self.posX[pid], y: self.posY[pid] }; },
            create(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.WRITE; self.lane[self.laneLen++] = pid; },
            edit(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.EDIT; },
            subscribeValue(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SUB; },
            readValue(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.READ; },
            effect(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.EFFECT; },
            delete(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DELETE; },
            compact(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.COMPACT; },
            none(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.NONE; },
            send(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SEND; },
            sub(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SUB; },
            duplex(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DUPLEX; },
            stream(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.STREAM; },
            drc(x, y, v, pid) { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DRC; },
            reclaimPid(pid) { self.posX[pid] = 0; self.posY[pid] = 0; self.val[pid] = 0; self.ops[pid] = OP.NONE; self.freePids[self.freeTail] = pid; self.freeTail = (self.freeTail + 1) % self.capacity; },
            pass() {
                const len = self.laneLen;
                let writeIdx = 0;
                let i = 0;
                const unrolledLimit = len - (len % 4);
                for (; i < unrolledLimit; i += 4) {
                    let pid = self.lane[i];
                    if (pid === TRACER_PID) {
                        self.val[TRACER_PID]++;
                        self.lane[writeIdx++] = pid;
                    }
                    else if (self.ops[pid] === OP.DELETE) {
                        self.reclaimPid(pid);
                    }
                    else {
                        self.ops[pid] = OP.NONE;
                        self.lane[writeIdx++] = pid;
                    }
                    pid = self.lane[i + 1];
                    if (pid === TRACER_PID) {
                        self.val[TRACER_PID]++;
                        self.lane[writeIdx++] = pid;
                    }
                    else if (self.ops[pid] === OP.DELETE) {
                        self.reclaimPid(pid);
                    }
                    else {
                        self.ops[pid] = OP.NONE;
                        self.lane[writeIdx++] = pid;
                    }
                    pid = self.lane[i + 2];
                    if (pid === TRACER_PID) {
                        self.val[TRACER_PID]++;
                        self.lane[writeIdx++] = pid;
                    }
                    else if (self.ops[pid] === OP.DELETE) {
                        self.reclaimPid(pid);
                    }
                    else {
                        self.ops[pid] = OP.NONE;
                        self.lane[writeIdx++] = pid;
                    }
                    pid = self.lane[i + 3];
                    if (pid === TRACER_PID) {
                        self.val[TRACER_PID]++;
                        self.lane[writeIdx++] = pid;
                    }
                    else if (self.ops[pid] === OP.DELETE) {
                        self.reclaimPid(pid);
                    }
                    else {
                        self.ops[pid] = OP.NONE;
                        self.lane[writeIdx++] = pid;
                    }
                }
                for (; i < len; i++) {
                    const pid = self.lane[i];
                    if (pid === TRACER_PID) {
                        self.val[TRACER_PID]++;
                        self.lane[writeIdx++] = pid;
                    }
                    else if (self.ops[pid] === OP.DELETE) {
                        self.reclaimPid(pid);
                    }
                    else {
                        self.ops[pid] = OP.NONE;
                        self.lane[writeIdx++] = pid;
                    }
                }
                self.laneLen = writeIdx;
            },
            get tick() { return self.val[TRACER_PID]; },
            get length() { return self.laneLen; },
            getLoadFactor() {
                const activeCount = Math.max(0, self.laneLen - 1);
                return activeCount / (self.capacity - 1);
            },
            read() {
                const r = self.registry;
                if (r !== null) {
                    if (gate !== undefined && gate.sealed)
                        throw new SEALED_ERROR.log(self.key);
                }
                return self.value;
            },
            write(next) {
                const r = self.registry;
                if (r === null) {
                    self.value = next;
                    return true;
                }
                if (gate !== undefined && (gate.sealed || gate.frozen))
                    return false;
                const previous = self.value;
                if (Object.is(previous, next))
                    return false;
                self.value = next;
                if (subscription !== undefined)
                    subscription.notify({ key: self.key, previous, current: next });
                return true;
            },
            subscribe(listener) { return subscription.subscribe(listener); },
            waitFor() { },
        }));
        return z;
    })();
    ATOMS = (() => {
        const z = () => { };
        z.LIFECYCLE = this.ATOM.BIND("ADAPTER", ({ self, master, config }) => ({}));
        z.ASYNC = this.ATOM.BIND("ADAPTER", ({ self, master, config }) => ({}));
        z.ADAPTER = this.ATOM.BIND("ADAPTER", ({ self, master, config }) => ({}));
        z.AW_SET = this.ATOM.BIND("AW_SET", ({ self, master, config }) => ({}));
        z.CASUAL_CURSOR = this.ATOM.BIND("CASUAL_CURSOR", ({ self, master, config }) => ({}));
        z.DELTA_STORE = this.ATOM.BIND("DELTA_STORE", ({ self, master, config }) => ({}));
        z.G_SET = this.ATOM.BIND("G_SET", ({ self, master, config }) => ({}));
        z.VERSION_VECTOR = this.ATOM.BIND("VERSION_VECTOR", ({ self, master, config }) => ({}));
        z.CIRCUIT_BREAKER = this.ATOM.BIND("CIRCUIT_BREAKER", ({ self, master, config }) => ({}));
        z.CQRS = this.ATOM.BIND("CQRS", ({ self, master, config }) => ({}));
        z.RAFT = this.ATOM.BIND("RAFT", ({ self, master, config }) => ({}));
        z.YJS = this.ATOM.BIND("YJS", ({ self, master, config }) => ({}));
        z.DIFFPATCH = this.ATOM.BIND("DIFFPATCH", ({ self, master, config }) => ({}));
        z.HYBRID_LOGICAL_CLOCK = this.ATOM.BIND("HYBRID_LOGICAL_CLOCK", ({ self, master, config }) => ({}));
        z.LWW = this.ATOM.BIND("LWW", ({ self, master, config }) => ({}));
        z.LWW_ELEMENTAL = this.ATOM.BIND("LWW_ELEMENTAL", ({ self, master, config }) => ({}));
        z.MV = this.ATOM.BIND("MV", ({ self, master, config }) => ({}));
        z.OPTIMISTIC = this.ATOM.BIND("OPTIMISTIC", ({ self, master, config }) => ({}));
        z.OR = this.ATOM.BIND("OR", ({ self, master, config }) => ({}));
        z.PN = this.ATOM.BIND("PN", ({ self, master, config }) => ({}));
        z.REVERSE_HEARTBEAT_MGR = this.ATOM.BIND("REVERSE_HEARTBEAT_MGR", ({ self, master, config }) => ({}));
        z.SEMAPHORE = this.ATOM.BIND("SEMAPHORE", ({ self, master, config }) => ({}));
        z.TOMBSTONE_COMPACTOR = this.ATOM.BIND("TOMBSTONE_COMPACTOR", ({ self, master, config }) => ({}));
        z.YIMIR = this.ATOM.BIND("YIMIR", ({ self, master, config }) => ({}));
        z.HYBRID_LOGICAL_CLOCK_CRDT = this.ATOM.BIND("HYBRID_LOGICAL_CLOCK_CRDT", ({ self, master, config }) => ({}));
        z.SOA_QUAD_TIERED_REGISTRY = this.ATOM.BIND("SOA_QUAD_TIERED_REGISTRY", ({ self, master, config }) => ({}));
        z.BINARY = this.ATOM.BIND("BINARY", ({ self, master, config }) => ({}));
        z.SIGNAL = this.ATOM.BIND("SIGNAL", ({ self, master, config }) => ({}));
        z.GOVERNOR = this.ATOM.BIND("GOVERNOR", ({ self, master, config }) => ({}));
        z.BYTE_CODE_VM = this.ATOM.BIND("BYTE_CODE_VM", ({ self, master, config }) => ({}));
        z.CHANNEL = this.ATOM.BIND("CHANNEL", ({ self, master, config }) => ({}));
        z.CHANNELS = this.ATOM.BIND("CHANNELS", ({ self, master, config }) => ({}));
        z.MIDDLEWARE = this.ATOM.BIND("MIDDLEWARE", ({ self, master, config }) => ({}));
        z.BEAM_CODEC = this.ATOM.BIND("BEAM_CODEC", ({ self, master, config }) => ({}));
        z.GOVERNER = this.ATOM.BIND("GOVERNER", ({ self, master, config }) => ({}));
        z.INJECTION_CODEC = this.ATOM.BIND("INJECTION_CODEC", ({ self, master, config }) => ({}));
        z.RUN_ON = this.ATOM.BIND("RUN_ON", ({ self, master, config }) => ({}));
        z.SEALED_ERROR = this.ATOM.BIND("SEALED_ERROR", ({ self, master, config }) => ({}));
        z.SHELL = this.ATOM.BIND("SHELL", ({ self, master, config }) => ({}));
        z.TRANSFORM = this.ATOM.BIND("TRANSFORM", ({ self, master, config }) => ({}));
        z.CONFLICT = this.ATOM.BIND("CONFLICT", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            conflictStrategy: (config.conflictStrategy ?? "latest"),
            conflict: config.conflict,
            resolve(current, incoming) {
                switch (self.conflictStrategy) {
                    case "fail":
                        throw new Error("SOUL_NEUTRON conflict: concurrent values cannot be merged");
                    case "accumulate":
                        if (typeof current === "number" && typeof incoming === "number")
                            return (current + incoming);
                        if (Array.isArray(current) && Array.isArray(incoming))
                            return [...current, ...incoming];
                        if (typeof current === "string" && typeof incoming === "string")
                            return (current + incoming);
                        throw new TypeError("SOUL_NEUTRON accumulate conflict requires matching number, string, or array values");
                    case "latest":
                    default:
                        return incoming;
                }
            },
        }));
        z.CONTEXT = this.ATOM.BIND("CONTEXT", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            store: new Map(),
            set(key, value) {
                self.store.set(key, value);
                // if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('context_set', undefined, undefined, undefined, { key });
            },
            get(key) {
                return self.store.get(key);
            },
            /** Read with a fallback — context lookups always expect a default. */
            getOr(key, fallback) {
                return self.store.has(key) ? self.store.get(key) : fallback;
            },
            has(key) {
                return self.store.has(key);
            },
            delete(key) {
                return self.store.delete(key);
            },
            keys() {
                return [...self.store.keys()];
            },
            /** Full snapshot, for hand-off / persistence / diagnostics. */
            snapshot() {
                return Object.fromEntries(self.store);
            },
            /** Restore from a snapshot. Replaces the store wholesale. */
            restore(snap) {
                self.store = new Map(Object.entries(snap));
            },
            /** Merge a partial snapshot over the current store (existing keys win unless overwrite). */
            merge(snap, overwrite = false) {
                for (const [k, v] of Object.entries(snap)) {
                    if (!overwrite && self.store.has(k))
                        continue;
                    self.store.set(k, v);
                }
            },
            /** Scoped read: return a sub-object of the store whose keys start with the prefix. */
            scoped(prefix) {
                const out = {};
                for (const [k, v] of self.store) {
                    if (k.startsWith(prefix))
                        out[k] = v;
                }
                return out;
            },
            purge() {
                self.store.clear();
            },
        }));
        z.ENCRYPTION = this.ATOM.BIND("ENCRYPTION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            encryptionConfigs: [],
            getEncryptionConfigs() {
                return [...self.encryptionConfigs];
            },
            configureEncryption(config) {
                self.encryptionConfigs.push(config);
            },
        }));
        z.GROUPS = this.ATOM.BIND("GROUPS", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            groups: new Map(),
            createGroup(name, channels = [], options) {
                self.groups.set(name, {
                    channels: new Set(channels),
                    enabled: options?.enabled ?? true,
                    middleware: options?.middleware ? [options.middleware] : [],
                });
            },
            deleteGroup(name) {
                self.groups.delete(name);
            },
            enableGroup(name) {
                const g = self.groups.get(name);
                if (!g)
                    return;
                g.enabled = true;
            },
            disableGroup(name) {
                const g = self.groups.get(name);
                if (!g)
                    return;
                g.enabled = false;
            },
            addToGroup(name, rt) {
                const g = self.groups.get(name);
                if (!g)
                    return;
                g.channels.add(rt);
            },
            removeFromGroup(name, rt) {
                const g = self.groups.get(name);
                if (!g)
                    return;
                g.channels.delete(rt);
            },
            async groupBroadcast(name, p, sl = self.sl, ms = self.ms, rt = self.rt, ttl) {
                const g = self.groups.get(name);
                if (!g || !g.enabled)
                    return;
                await Promise.all([...g.channels].map(ch => self.broadcast(p, ch, sl, ms, rt, ttl)));
            },
            groupUse(name, fn) {
                const g = self.groups.get(name);
                if (!g)
                    return;
                g.middleware.push(fn);
            },
            getAllGroups() {
                const out = {};
                for (const [name, g] of self.groups) {
                    out[name] = {
                        channels: [...g.channels],
                        enabled: g.enabled,
                        middlewareCount: g.middleware.length,
                    };
                }
                return out;
            },
        }));
        z.HISTORY = this.ATOM.BIND("HISTORY", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            history: new Map(),
            defaultLimit: 128,
            limits: new Map(),
            /** Append one message. Returns the entry so callers can thread it onward. */
            record(rt, msg) {
                let list = self.history.get(rt);
                if (!list) {
                    list = [];
                    self.history.set(rt, list);
                }
                list.push(msg);
                const limit = self.limits.get(rt) ?? self.defaultLimit;
                if (list.length > limit)
                    list.splice(0, list.length - limit);
                return msg;
            },
            setChannelLimit(rt, limit) {
                self.limits.set(rt, limit);
                const list = self.history.get(rt);
                if (list && list.length > limit) {
                    list.splice(0, list.length - limit);
                }
            },
            getChannelLimit(rt) {
                return self.limits.get(rt) ?? self.defaultLimit;
            },
            getRecentHistory(rt, limit = 3) {
                return (self.history.get(rt) ?? []).slice(-limit);
            },
            getChannelHistory(rt) {
                return [...(self.history.get(rt) ?? [])];
            },
            getAllHistoryChannels() {
                return [...self.history.keys()];
            },
            getChannelDepth(rt) {
                return (self.history.get(rt) ?? []).length;
            },
            getHistorySizes() {
                const out = {};
                for (const [ch, list] of self.history)
                    out[ch] = list.length;
                return out;
            },
            clearChannel(rt) {
                self.history.delete(rt);
            },
            purge() {
                self.history.clear();
                self.limits.clear();
            },
        }));
        z.META = this.ATOM.BIND("META", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            // ── your blob, organized ───────────────────────────────────────────────────────────────────
            _batches: new Set(),
            _batchQueue: new Map(),
            _batchOpts: new Map(),
            _globalListeners: new Set(),
            _global_subscribers: new Map(),
            _sys: new Map(),
            _sealed: new Set(),
            _frozen: new Set(),
            _keyContracts: new Map(),
            _conflicts: new Map(),
            _conflictStrategies: new Map(),
            _causal: new Map(),
            _scopes: new Map(),
            _msgHandlers: new Map(),
            _lastWriteSource: new Map(),
            _mainMap: new Map(),
            _tupleCache: new Map(),
            _weakMap: new WeakMap(),
            _boundaries: new WeakMap(),
            // ── sys entries ────────────────────────────────────────────────────────────────────────────
            /** Register or update a system entry for this cell. */
            setSys(name, entry) {
                const prev = self._sys.get(name);
                const next = {
                    name,
                    value: entry.value ?? prev?.value,
                    ts: Date.now(),
                    revision: (prev?.revision ?? 0) + 1,
                    source: entry.source ?? "meta",
                };
                self._sys.set(name, next);
                return next;
            },
            getSys(name) {
                return self._sys.get(name) ?? null;
            },
            deleteSys(name) {
                return self._sys.delete(name);
            },
            listSys() {
                return [...self._sys.keys()];
            },
            // ── causal log ─────────────────────────────────────────────────────────────────────────────
            /** Append a causal entry. Keep it cheap — callers pass what they know. */
            recordCausal(key, cause, value) {
                const list = self._causal.get(key) ?? [];
                list.push({ cause, value, ts: Date.now() });
                // hard cap so long-lived cells don't balloon
                if (list.length > 256)
                    list.splice(0, list.length - 256);
                self._causal.set(key, list);
            },
            getCausal(key) {
                return self._causal.get(key) ?? [];
            },
            clearCausal(key) {
                if (key)
                    self._causal.delete(key);
                else
                    self._causal.clear();
            },
            // ── last write provenance ──────────────────────────────────────────────────────────────────
            noteWrite(key, source) {
                self._lastWriteSource.set(key, source);
            },
            whoWrote(key) {
                return self._lastWriteSource.get(key) ?? null;
            },
            // ── msg handlers ───────────────────────────────────────────────────────────────────────────
            addMsgHandler(key, fn) {
                const set = self._msgHandlers.get(key) ?? new Set();
                set.add(fn);
                self._msgHandlers.set(key, set);
                return () => set.delete(fn);
            },
            emitMsg(key, p) {
                for (const fn of self._msgHandlers.get(key) ?? []) {
                    try {
                        fn(p);
                    }
                    catch (e) {
                        console.error(`[meta-shard] msg handler failed for "${key}":`, e);
                    }
                }
            },
            // ── global listeners ───────────────────────────────────────────────────────────────────────
            onGlobal(fn) {
                self._globalListeners.add(fn);
                return () => self._globalListeners.delete(fn);
            },
            emitGlobal(key, value) {
                for (const fn of self._globalListeners) {
                    try {
                        fn(key, value);
                    }
                    catch (e) {
                        console.error("[meta-shard] global listener failed:", e);
                    }
                }
            },
            // ── inspect / purge ────────────────────────────────────────────────────────────────────────
            inspect() {
                return {
                    sys: Object.fromEntries(self._sys),
                    causalKeys: [...self._causal.keys()],
                    sealed: [...self._sealed],
                    frozen: [...self._frozen],
                    lastWriteSource: Object.fromEntries(self._lastWriteSource),
                    msgHandlerKeys: [...self._msgHandlers.keys()],
                    globalListenerCount: self._globalListeners.size,
                };
            },
            purge() {
                for (const map of [
                    self._batchQueue,
                    self._batchOpts,
                    self._global_subscribers,
                    self._sys,
                    self._keyContracts,
                    self._conflicts,
                    self._conflictStrategies,
                    self._causal,
                    self._scopes,
                    self._msgHandlers,
                    self._lastWriteSource,
                    self._mainMap,
                    self._tupleCache,
                ]) {
                    map.clear();
                }
                self._batches.clear();
                self._sealed.clear();
                self._frozen.clear();
                self._globalListeners.clear();
                self._weakMap = new WeakMap();
                self._boundaries = new WeakMap();
            },
        }));
        z.OFFSEC = this.ATOM.BIND("OFFSEC", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            secrets: new Set(),
            audit: [],
            maxAudit: (config.maxAudit ?? 500),
            // ── secret marking ─────────────────────────────────────────────────────────────────────────
            /** Mark a value (token, key, credential) as secret. Reference equality by default. */
            markSecret(value) {
                self.secrets.add(value);
            },
            isSecret(value) {
                return self.secrets.has(value);
            },
            clearSecret(value) {
                return self.secrets.delete(value);
            },
            /**
             * Deep-scan a message for marked secrets; replace with '[REDACTED]' before the message
             * leaves the boundary. Mutates nothing — returns a copy when a hit occurs.
             */
            scrub(payload) {
                let redacted = 0;
                const seen = new Set();
                const walk = (v) => {
                    if (self.secrets.has(v)) {
                        redacted++;
                        return "[REDACTED]";
                    }
                    if (v === null || typeof v !== "object")
                        return v;
                    if (seen.has(v))
                        return "[CYCLIC]";
                    seen.add(v);
                    if (Array.isArray(v))
                        return v.map(walk);
                    const out = {};
                    for (const [k, val] of Object.entries(v)) {
                        out[k] = walk(val);
                    }
                    return out;
                };
                const clean = walk(payload);
                if (redacted) {
                    self.audit.push({
                        at: Date.now(),
                        what: `scrubbed ${redacted} secret(s)`,
                    });
                    if (self.audit.length > self.maxAudit)
                        self.audit.shift();
                }
                return { clean, redacted };
            },
            listAudit() {
                return [...self.audit];
            },
            purge() {
                self.secrets.clear();
                self.audit.length = 0;
            },
        }));
        z.PERMISSIONS = this.ATOM.BIND("PERMISSIONS", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            permissions: new Map(),
            setPermissions(rt, perms) {
                self.permissions.set(rt, perms);
            },
            getPermissions(rt) {
                return self.permissions.get(rt);
            },
            removePermissions(rt) {
                return self.permissions.delete(rt);
            },
            hasPermissions(rt) {
                return self.permissions.has(rt);
            },
            getAllPermissions() {
                return Object.fromEntries(self.permissions);
            },
            listProtectedChannels() {
                return [...self.permissions.keys()];
            },
            /** Wildcard-aware lookup: exact wins, else longest matching prefix before `*`. */
            resolvePermissions(rt) {
                if (self.permissions.has(rt))
                    return self.permissions.get(rt);
                let best;
                let bestLen = -1;
                for (const [pattern, perms] of self.permissions) {
                    const star = pattern.indexOf("*");
                    if (star === -1)
                        continue;
                    if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) {
                        best = perms;
                        bestLen = star;
                    }
                }
                return best;
            },
            /** Check one capability. No permission set → allowed (open channel). */
            check(rt, action, role) {
                const perms = self.resolvePermissions(rt);
                if (!perms)
                    return true;
                if (role && perms.roles && perms.roles.length && !perms.roles.includes(role)) {
                    return false;
                }
                if (action === "read")
                    return perms.read;
                if (action === "write")
                    return perms.write;
                return perms[action] ?? perms.write;
            },
            purge() {
                self.permissions.clear();
            },
        }));
        z.PERSISTENCE = this.ATOM.BIND("PERSISTENCE", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            durableOffsets: new Map(),
            persisted: config.persisted,
            durableHistory: new Map(),
            getDurableHistorySizes() {
                const out = {};
                for (const [ch, hist] of self.durableHistory) {
                    out[ch] = hist.length;
                }
                return out;
            },
            async replay(options, handler) {
                const history = self.durableHistory.get(options.rt) ?? [];
                let slice = history;
                if (options.fromOffset !== undefined) {
                    slice = slice.slice(options.fromOffset);
                }
                if (options.toOffset !== undefined) {
                    slice = slice.slice(0, options.toOffset);
                }
                if (options.fromTimestamp !== undefined) {
                    slice = slice.filter(m => m.ts !== undefined && m.ts >= options.fromTimestamp);
                }
                if (options.toTimestamp !== undefined) {
                    slice = slice.filter(m => m.ts !== undefined && m.ts <= options.toTimestamp);
                }
                for (const m of slice) {
                    handler(m.p);
                }
            },
            subscribeDurable(handler, options, k, sl = self.sl, ms = self.ms, rt = self.rt) {
                const key = `${rt}:${options.subscriberId}`;
                const offset = self.durableOffsets.get(key) ?? options.fromOffset ?? 0;
                const history = self.durableHistory.get(rt) ?? [];
                history.slice(offset).forEach(m => handler(m.p));
                return self.sub((p) => {
                    const h = self.durableHistory.get(rt) ?? [];
                    const nextOffset = h.length;
                    self.durableOffsets.set(key, nextOffset);
                    handler(p);
                }, k, sl, ms, rt);
            },
            getAllCheckpoints() {
                return Object.fromEntries(self.durableOffsets);
            },
            async setCheckpoint(subscriberId, offset, k, sl, ms, rt) {
                const key = `${rt}:${subscriberId}`;
                self.durableOffsets.set(key, offset);
                if (self.DEVTOOLS_ACTIVE) {
                    self.emitDevtoolsEvent("checkpoint_set", rt, ms || undefined, undefined, { subscriberId, offset });
                }
            },
            async getCheckpoint(subscriberId, k, sl, ms, rt) {
                const key = `${rt}:${ms}:${subscriberId}`;
                return self.durableOffsets.has(key)
                    ? self.durableOffsets.get(key)
                    : null;
            },
        }));
        z.PRIORITY = this.ATOM.BIND("PRIORITY", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            queues: new Map(),
            queueDefaults: new Map(),
            defaultPriority: "normal",
            seq: 0,
            setDefaultPriority(priority) {
                self.defaultPriority = priority;
            },
            getDefaultPriority() {
                return self.defaultPriority;
            },
            setQueueDefault(queue, priority) {
                self.queueDefaults.set(queue, priority);
            },
            getQueueDefault(queue) {
                return self.queueDefaults.get(queue) ?? self.defaultPriority;
            },
            priorityWeight(p) {
                return p === "high" ? 3 : p === "low" ? 1 : 2;
            },
            enqueue(queue, item, priority) {
                const p = priority ?? self.getQueueDefault(queue);
                let q = self.queues.get(queue);
                if (!q) {
                    q = [];
                    self.queues.set(queue, q);
                }
                const entry = {
                    priority: p,
                    weight: self.priorityWeight(p),
                    seq: self.seq++,
                    item,
                    enqueuedAt: Date.now(),
                };
                q.push(entry);
                if (self.DEVTOOLS_ACTIVE) {
                    self.emitDevtoolsEvent("priority_enqueued", undefined, undefined, queue, {
                        priority: p,
                        depth: q.length,
                    });
                }
                return entry.seq;
            },
            /** Highest weight wins; FIFO within the same weight (low seq = older = first). */
            dequeue(queue) {
                const q = self.queues.get(queue);
                if (!q || !q.length)
                    return undefined;
                let bestIdx = 0;
                for (let i = 1; i < q.length; i++) {
                    const a = q[i], b = q[bestIdx];
                    if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) {
                        bestIdx = i;
                    }
                }
                const [entry] = q.splice(bestIdx, 1);
                if (self.DEVTOOLS_ACTIVE) {
                    self.emitDevtoolsEvent("priority_dequeued", undefined, undefined, queue, {
                        priority: entry.priority,
                        remaining: q.length,
                    });
                }
                return entry.item;
            },
            peek(queue) {
                const q = self.queues.get(queue);
                if (!q || !q.length)
                    return undefined;
                let bestIdx = 0;
                for (let i = 1; i < q.length; i++) {
                    const a = q[i], b = q[bestIdx];
                    if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) {
                        bestIdx = i;
                    }
                }
                return q[bestIdx].item;
            },
            getDepth(queue) {
                return (self.queues.get(queue) ?? []).length;
            },
            listQueues() {
                return [...self.queues.keys()];
            },
            getDepths() {
                const out = {};
                for (const [q, list] of self.queues) {
                    out[q] = list.length;
                }
                return out;
            },
            clearQueue(queue) {
                self.queues.delete(queue);
            },
            purge() {
                self.queues.clear();
                self.queueDefaults.clear();
            },
        }));
        z.SNAPSHOT = this.ATOM.BIND("SNAPSHOT", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            version: 0,
            history: [],
            historyMax: (config.historyMax ?? 128),
            undoStack: [],
            redoStack: [],
            waiters: new Set(),
            /** Capture the current state. Call this from ATOM.write() after a successful write. */
            capture(value, label) {
                self.version++;
                const entry = {
                    version: self.version,
                    value,
                    ts: Date.now(),
                    label,
                };
                self.history.push(entry);
                if (self.history.length > self.historyMax) {
                    self.history.splice(0, self.history.length - self.historyMax);
                }
                self.redoStack.length = 0;
                for (const resolve of self.waiters) {
                    resolve(value);
                }
                self.waiters.clear();
                return entry;
            },
            /** Latest snapshot, or undefined if none. */
            latest() {
                return self.history[self.history.length - 1];
            },
            /** Fetch a snapshot by exact version number. */
            byVersion(v) {
                return self.history.find(e => e.version === v);
            },
            /** Fetch the last N snapshots, oldest first. */
            recent(n = 3) {
                return self.history.slice(-n);
            },
            /** All snapshots since a timestamp. */
            since(ts) {
                return self.history.filter(e => e.ts >= ts);
            },
            count() {
                return self.history.length;
            },
            /** Wait for the next capture (first one if none exist yet). */
            waitForNext(timeoutMs = 0) {
                const latest = self.latest();
                if (latest)
                    return Promise.resolve(latest.value);
                return new Promise((resolve, reject) => {
                    const waiter = (v) => resolve(v);
                    self.waiters.add(waiter);
                    if (timeoutMs > 0) {
                        setTimeout(() => {
                            if (self.waiters.delete(waiter)) {
                                reject(new Error("ATOM snapshot wait timed out"));
                            }
                        }, timeoutMs);
                    }
                });
            },
            /** Move an entry from history into the undo stack (used by TIME_TRAVEL). */
            pushUndo(entry) {
                self.undoStack.push(entry);
                if (self.undoStack.length > self.historyMax) {
                    self.undoStack.splice(0, self.undoStack.length - self.historyMax);
                }
            },
            /** Move an entry from history into the redo stack (used by TIME_TRAVEL). */
            pushRedo(entry) {
                self.redoStack.push(entry);
                if (self.redoStack.length > self.historyMax) {
                    self.redoStack.splice(0, self.redoStack.length - self.historyMax);
                }
            },
            purge() {
                self.history.length = 0;
                self.undoStack.length = 0;
                self.redoStack.length = 0;
                self.waiters.clear();
                self.version = 0;
            },
        }));
        z.TAGS = this.ATOM.BIND("TAGS", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            tagSubs: new Map(),
            tagsByChannel: new Map(),
            // ── channel → tags ─────────────────────────────────────────────────────────────────────────
            /** Declare the tags a channel carries. Replaces any previous tag set for that channel. */
            tagChannel(channel, tags) {
                self.tagsByChannel.set(channel, new Set(tags));
            },
            untagChannel(channel) {
                return self.tagsByChannel.delete(channel);
            },
            getTagsForChannel(channel) {
                return [...(self.tagsByChannel.get(channel) ?? [])];
            },
            /** All channels carrying at least one of the given tags. */
            channelsByTags(tags) {
                const want = new Set(tags);
                const out = [];
                for (const [ch, chTags] of self.tagsByChannel) {
                    for (const t of chTags) {
                        if (want.has(t)) {
                            out.push(ch);
                            break;
                        }
                    }
                }
                return out;
            },
            // ── subscriber side ─────────────────────────────────────────────────────────────────────────
            subscribeByTag(tag, handler) {
                const set = self.tagSubs.get(tag) ?? new Set();
                set.add(handler);
                self.tagSubs.set(tag, set);
                return () => set.delete(handler);
            },
            unsubscribeByTag(tag, handler) {
                return self.tagSubs.get(tag)?.delete(handler) ?? false;
            },
            listTags() {
                return [...self.tagSubs.keys()];
            },
            // ── publish side ────────────────────────────────────────────────────────────────────────────
            /** Fan a tagged message out to every handler subscribed to a tag the channel carries. */
            publishByTag(msg) {
                const tags = self.tagsByChannel.get(msg.r);
                if (!tags || tags.size === 0)
                    return 0;
                let delivered = 0;
                for (const tag of tags) {
                    const subs = self.tagSubs.get(tag);
                    if (!subs)
                        continue;
                    for (const handler of [...subs]) {
                        try {
                            handler(msg);
                            delivered++;
                        }
                        catch (e) {
                            console.error(`[tags-NEUTRON] tag handler failed for "${tag}":`, e);
                        }
                    }
                }
                return delivered;
            },
            getTagsByChannel() {
                const out = {};
                for (const [ch, tags] of self.tagsByChannel) {
                    out[ch] = [...tags];
                }
                return out;
            },
            purge() {
                self.tagSubs.clear();
                self.tagsByChannel.clear();
            },
        }));
        z.TIME_TRAVEL = this.ATOM.BIND("TIME_TRAVEL", ({ self, master, config, SNAPSHOT }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            snapshots: null,
            version: 0,
            historyIndex: -1,
            undoStack: [],
            initial: undefined,
            _time_travel_reverting: false,
            autoTimers: new Set(),
            /** Bind to the snapshot shard of the same cell. Call once after both shards are bound. */
            attach(snapshots) {
                self.snapshots = snapshots;
                self.version = snapshots.version;
                self.historyIndex = snapshots.history.length - 1;
                self.initial = snapshots.history[0]?.value;
            },
            /** Write a value back to the cell and record it as a proper version (fires subs like a normal write). */
            replay: (entry) => {
                self._time_travel_reverting = true;
                try {
                    self._writeThrough?.(entry.value, entry.label ? `time-travel:${entry.label}` : "time-travel");
                }
                finally {
                    self._time_travel_reverting = false;
                }
            },
            /** Injected by ATOM when the shard is bound — how restores become real writes. */
            _writeThrough: undefined,
            _current() {
                return self.snapshots?.history[self.historyIndex];
            },
            back() {
                if (!self.snapshots || self.historyIndex <= 0)
                    return false;
                self.historyIndex--;
                const entry = self._current();
                if (!entry)
                    return false;
                // The entry we stepped away from goes on redo.
                const departed = self.snapshots.history[self.historyIndex + 1];
                if (departed)
                    self.snapshots.pushRedo(departed);
                self.replay(entry);
                return true;
            },
            forward() {
                if (!self.snapshots)
                    return false;
                const nextEntry = self.snapshots.history[self.historyIndex + 1];
                if (!nextEntry)
                    return false;
                self.historyIndex++;
                self.replay(nextEntry);
                return true;
            },
            goto(version) {
                if (!self.snapshots)
                    return false;
                const target = self.snapshots.byVersion(version);
                if (!target)
                    return false;
                const idx = self.snapshots.history.indexOf(target);
                if (idx < 0)
                    return false;
                const departed = self._current();
                if (departed && idx < self.historyIndex) {
                    self.snapshots.pushRedo(departed);
                }
                self.historyIndex = idx;
                self.replay(target);
                return true;
            },
            auto_back(time = 2500) {
                const t = setInterval(() => {
                    if (!self.back())
                        clearInterval(t);
                }, time);
                self.autoTimers.add(t);
            },
            auto_forward(time = 2500) {
                const t = setInterval(() => {
                    if (!self.forward())
                        clearInterval(t);
                }, time);
                self.autoTimers.add(t);
            },
            /** Graft a foreign snapshot sequence onto this timeline (branch merge). */
            graft(entries) {
                if (!self.snapshots)
                    return 0;
                let grafted = 0;
                for (const e of entries) {
                    self.snapshots.history.push({
                        version: ++self.snapshots.version,
                        value: e.value,
                        ts: e.ts ?? Date.now(),
                        label: e.label ? `grafted:${e.label}` : "grafted",
                    });
                    grafted++;
                }
                self.historyIndex = self.snapshots.history.length - 1;
                return grafted;
            },
            /** Stop all auto walkers. */
            stopAuto() {
                for (const t of self.autoTimers)
                    clearInterval(t);
                self.autoTimers.clear();
            },
            /** True while replay() is writing — ATOM.write() checks this to avoid re-capturing. */
            get isReverting() {
                return self._time_travel_reverting;
            },
            purge() {
                self.stopAuto();
                self.snapshots = null;
                self.undoStack.length = 0;
                self.historyIndex = -1;
            },
        }));
        z.TTL = this.ATOM.BIND("TTL", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            entries: new Map(),
            onExpire: [],
            defaultTtlMs: (config.defaultTtlMs ?? 0),
            // ── write ──────────────────────────────────────────────────────────────────────────────────
            set(key, data, ttlMs) {
                const ttl = ttlMs ?? self.defaultTtlMs;
                if (!ttl || ttl <= 0) {
                    throw new Error("[ttl-shard] no ttl provided and no defaultTtlMs configured");
                }
                const entry = {
                    expiresAt: Date.now() + ttl,
                    data,
                };
                self.entries.set(key, entry);
                return entry;
            },
            touch(key, ttlMs) {
                const e = self.entries.get(key);
                if (!e)
                    return false;
                e.expiresAt = Date.now() + (ttlMs ?? self.defaultTtlMs);
                return true;
            },
            // ── read ───────────────────────────────────────────────────────────────────────────────────
            /** Returns undefined if missing OR expired (and removes it lazily). */
            get(key) {
                const e = self.entries.get(key);
                if (!e)
                    return undefined;
                if (Date.now() >= e.expiresAt) {
                    self.entries.delete(key);
                    return undefined;
                }
                return e.data;
            },
            has(key) {
                const e = self.entries.get(key);
                return !!e && Date.now() < e.expiresAt;
            },
            peek(key) {
                return self.entries.get(key)?.data;
            },
            timeToLiveMs(key) {
                const e = self.entries.get(key);
                if (!e)
                    return null;
                return Math.max(0, e.expiresAt - Date.now());
            },
            // ── delete ─────────────────────────────────────────────────────────────────────────────────
            delete(key) {
                return self.entries.delete(key);
            },
            // ── sweep ──────────────────────────────────────────────────────────────────────────────────
            /** Removes expired entries, fires onExpire hooks. Returns count expired. */
            sweep() {
                const now = Date.now();
                let n = 0;
                for (const [key, e] of [...self.entries]) {
                    if (now >= e.expiresAt) {
                        self.entries.delete(key);
                        n++;
                        for (const fn of self.onExpire) {
                            try {
                                fn(key, e.data);
                            }
                            catch (err) {
                                console.error(`[ttl-shard] onExpire failed for "${key}":`, err);
                            }
                        }
                    }
                }
                return n;
            },
            /** Sweep on a cadence. Owns its own timer — call stopSweeper() to cancel. */
            _sweeper: null,
            startSweeper(intervalMs = 30_000) {
                if (self._sweeper)
                    return;
                self._sweeper = setInterval(() => self.sweep(), intervalMs);
            },
            stopSweeper() {
                if (self._sweeper) {
                    clearInterval(self._sweeper);
                    self._sweeper = null;
                }
            },
            get size() {
                return self.entries.size;
            },
            listKeys() {
                return [...self.entries.keys()];
            },
            purge() {
                self.stopSweeper();
                self.entries.clear();
                self.onExpire.length = 0;
            },
        }));
        z.REACTIVE = this.ATOM.BIND("REACTIVE", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            records: new Map(),
            listeners: new Map(),
            get(key) {
                return self.records.get(key);
            },
            set(key, record) {
                self.records.set(key, record);
                const subs = self.listeners.get(key);
                if (subs) {
                    for (const l of [...subs]) {
                        try {
                            l(record);
                        }
                        catch {
                            // one bad listener never kills the rest
                        }
                    }
                }
            },
            sub(key, fn) {
                let subs = self.listeners.get(key);
                if (!subs) {
                    subs = new Set();
                    self.listeners.set(key, subs);
                }
                subs.add(fn);
                return () => {
                    const s = self.listeners.get(key);
                    if (!s)
                        return;
                    s.delete(fn);
                    if (!s.size)
                        self.listeners.delete(key);
                };
            },
            delete(key) {
                self.records.delete(key);
                self.listeners.delete(key);
            },
            clear() {
                self.records.clear();
                self.listeners.clear();
            },
            get size() {
                return self.records.size;
            },
        }));
        z.SCHEMA = this.ATOM.BIND("SCHEMA", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            _schemas: new Map(),
            setChannelSchema(schema, rt) {
                self._schemas.set(rt, schema);
            },
            getChannelSchema(rt) {
                return self._schemas.get(rt);
            },
            removeChannelSchema(rt) {
                const had = self._schemas.delete(rt);
                return had;
            },
            hasChannelSchema(rt) {
                return self._schemas.has(rt);
            },
            listChannelSchemas() {
                return [...self._schemas.keys()];
            },
            getAllSchemas() {
                return Object.fromEntries(self._schemas);
            },
            /** Wildcard-aware lookup: exact channel wins, else longest matching prefix before `*`. */
            resolveSchema(rt) {
                if (self._schemas.has(rt))
                    return self._schemas.get(rt);
                let best;
                let bestLen = -1;
                for (const [pattern, schema] of self._schemas) {
                    const star = pattern.indexOf("*");
                    if (star === -1)
                        continue;
                    if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) {
                        best = schema;
                        bestLen = star;
                    }
                }
                return best;
            },
            /**
             * Validate a message against its channel schema. No schema → pass (open channel).
             * Returns { ok, errors } — never throws.
             */
            checkSchema(rt, msg) {
                const schema = self.resolveSchema(rt);
                if (!schema)
                    return { ok: true, errors: [] };
                const errors = [];
                const payload = (msg.p ?? {});
                for (const [field, rule] of Object.entries(schema.fields ?? {})) {
                    const v = payload[field];
                    if (v === undefined || v === null) {
                        if (rule.required)
                            errors.push(`missing required field "${field}"`);
                        continue;
                    }
                    if (rule.type !== "any") {
                        const actual = typeof v;
                        if (rule.type === "number" && actual !== "number") {
                            errors.push(`field "${field}" expected number, got ${actual}`);
                        }
                        else if (rule.type === "string" && actual !== "string") {
                            errors.push(`field "${field}" expected string, got ${actual}`);
                        }
                        else if (rule.type === "boolean" && actual !== "boolean") {
                            errors.push(`field "${field}" expected boolean, got ${actual}`);
                        }
                    }
                }
                if (schema.validate) {
                    try {
                        const r = schema.validate(msg);
                        if (r !== true && r !== undefined) {
                            errors.push(typeof r === "string"
                                ? r
                                : `validator rejected message on "${rt}"`);
                        }
                    }
                    catch (e) {
                        errors.push(`validator threw: ${e}`);
                    }
                }
                return { ok: errors.length === 0, errors };
            },
            purge() {
                self._schemas.clear();
            },
        }));
        z.VALUES_REGISTRY = this.ATOM.BIND("VALUES_REGISTRY", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            instanceId: `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`,
            hasWritten: false,
            lastWriteSource: undefined,
            lastWriteAt: undefined,
            writeCount: 0,
            recordWrite(source = "write") {
                self.hasWritten = true;
                self.lastWriteSource = source;
                self.lastWriteAt = Date.now();
                self.writeCount++;
            },
            reset() {
                self.hasWritten = false;
                self.lastWriteSource = undefined;
                self.lastWriteAt = undefined;
                self.writeCount = 0;
            },
        }));
        z.CONCURRENCY = this.ATOM.BIND("CONCURRENCY", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            current: Math.min(config.maxConcurrency ?? 64, Math.max(config.minConcurrency ?? 1, config.initialConcurrency ?? Math.max(4, Math.floor((config.maxConcurrency ?? 64) / 4)))),
            min: (config.minConcurrency ?? 1),
            max: (config.maxConcurrency ?? 64),
            holders: new Set(),
            waiters: [],
            peak: 0,
            get active() {
                return self.holders.size;
            },
            get concurrency() {
                return self.current;
            },
            get peakConcurrency() {
                return self.peak;
            },
            tryAcquire() {
                if (self.holders.size >= self.current)
                    return undefined;
                const h = Symbol("governor-slot");
                self.holders.add(h);
                if (self.holders.size > self.peak)
                    self.peak = self.holders.size;
                return h;
            },
            async acquire() {
                const h = self.tryAcquire();
                if (h)
                    return h;
                return new Promise(resolve => self.waiters.push(() => resolve(self.tryAcquire())));
            },
            release(h) {
                self.holders.delete(h);
                const next = self.waiters.shift();
                if (next)
                    next();
            },
            /** AIMD. Call from telemetry. */
            evaluateResistance(signal) {
                const target = signal.targetLatencyMs ?? 250;
                if (signal.error || signal.latency > target * 1.5) {
                    self.multiplicativeDecrease();
                }
                else if (signal.latency < target * 0.7) {
                    self.additiveIncrease();
                }
            },
            additiveIncrease() {
                if (self.current < self.max)
                    self.resize(self.current + 1);
            },
            multiplicativeDecrease() {
                if (self.current > self.min) {
                    self.resize(Math.max(self.min, Math.floor(self.current * 0.5)));
                }
            },
            resize(n) {
                self.current = Math.max(self.min, Math.min(self.max, Math.floor(n)));
                if (self.current > self.peak)
                    self.peak = self.current;
                // Wake waiters if capacity grew.
                while (self.holders.size < self.current && self.waiters.length) {
                    self.waiters.shift()?.();
                }
            },
        }));
        z.CONTRACTS = this.ATOM.BIND("CONTRACTS", ({ self, master, config }) => ({
            useCell: (() => {
                const fn = () => { };
                return fn;
            })(),
            r: master.r,
            m: master.m,
            s: master.s,
            k: config.k,
            contracts: new Map(),
            _keyContracts: new Map(),
            causal: [],
            violations: [],
            violationMax: 64,
            _throwOnBlock: true,
            add: (contract) => {
                self.contracts.set(contract.name, contract);
                return () => self.contracts.delete(contract.name);
            },
            remove: (name) => {
                return self.contracts.delete(name);
            },
            get: (name) => {
                return self.contracts.get(name);
            },
            list: () => {
                return [...self.contracts.values()];
            },
            bindKeyContracts: (keyContracts) => {
                self._keyContracts = keyContracts;
            },
            _record: (v, blocking) => {
                self.violations.push(v);
                if (self.violations.length > self.violationMax)
                    self.violations.splice(0, self.violations.length - self.violationMax);
                if (blocking)
                    throw new Error(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
                console.warn(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
                return false;
            },
            check: (value, phase, snapshot) => {
                for (const c of self.contracts.values()) {
                    if (phase === 'pre' && c.precondition) {
                        try {
                            if (!c.precondition(value))
                                return self._record({ contract: c.name, phase, value, message: 'precondition returned false', ts: Date.now() }, !!c.block && self._throwOnBlock);
                        }
                        catch (e) {
                            return self._record({ contract: c.name, phase, value, message: `precondition threw: ${e}`, ts: Date.now() }, true);
                        }
                    }
                    if (phase === 'post' && c.postcondition) {
                        try {
                            if (!c.postcondition(value))
                                return self._record({ contract: c.name, phase, value, message: 'postcondition returned false', ts: Date.now() }, !!c.block && self._throwOnBlock);
                        }
                        catch (e) {
                            return self._record({ contract: c.name, phase, value, message: `postcondition threw: ${e}`, ts: Date.now() }, true);
                        }
                    }
                    if (phase === 'invariant' && c.invariant && snapshot) {
                        try {
                            if (!c.invariant(snapshot))
                                return self._record({ contract: c.name, phase, value, message: 'invariant returned false', ts: Date.now() }, !!c.block && self._throwOnBlock);
                        }
                        catch (e) {
                            return self._record({ contract: c.name, phase, value, message: `invariant threw: ${e}`, ts: Date.now() }, true);
                        }
                    }
                }
                return true;
            },
            checkKey: (key, value, phase) => {
                for (const name of self._keyContracts.get(key) ?? []) {
                    const c = self.contracts.get(name);
                    if (!c)
                        continue;
                    const check = phase === 'pre' ? c.precondition : c.postcondition;
                    if (!check)
                        continue;
                    try {
                        if (!check(value))
                            return self._record({ contract: name, phase, value, message: `key contract failed for "${key}"`, ts: Date.now() }, !!c.block && self._throwOnBlock);
                    }
                    catch (e) {
                        return self._record({ contract: name, phase, value, message: `key contract threw: ${e}`, ts: Date.now() }, true);
                    }
                }
                return true;
            },
            canWrite: (value) => { return self.check(value, 'pre'); },
            didWrite: (value) => { return self.check(value, 'post'); },
            listViolations: () => { return [...self.violations]; },
            clearViolations: () => { self.violations.length = 0; },
            purge: () => { self.contracts.clear(); self.violations.length = 0; self._keyContracts.clear(); }
        }));
        z.COMPRESSION = this.ATOM.BIND("COMPRESSION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            compressionConfigs: [],
            getCompressionConfigs() {
                return [...self.compressionConfigs];
            },
            configureCompression(config) {
                self.compressionConfigs.push(config);
            },
        }));
        z.DEADLETTER = this.ATOM.BIND("DEADLETTER", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            root_cell_key: config.root_cell_key,
            deadLetters: [],
            maxDeadLetters: (config.maxDeadLetters ?? 1000),
            wire: config.wire,
            /** Park a failed message. Oldest evicted when over max. */
            record(msg, reason) {
                self.deadLetters.push({ ...msg, _ack: true });
                if (self.deadLetters.length > self.maxDeadLetters) {
                    self.deadLetters.shift();
                }
            },
            async listDeadLetters(k, sl, ms, rt) {
                if (!rt && !ms && !sl && !k)
                    return [...self.deadLetters];
                return self.deadLetters.filter(m => (!rt || m.rt === rt) &&
                    (!ms || m.ms === ms) &&
                    (!sl || m.sl === sl) &&
                    (!k || m.k === k));
            },
            async requeueDeadLetter(id, ttl) {
                const idx = self.deadLetters.findIndex(m => m.id === id);
                if (idx === -1)
                    return;
                const msg = self.deadLetters.splice(idx, 1)[0];
                msg.ttl = ttl ?? msg.ttl;
                await self.wire?.enqueue(msg);
            },
            purgeDeadLetter(id) {
                const idx = self.deadLetters.findIndex(m => m.id === id);
                if (idx === -1)
                    return false;
                self.deadLetters.splice(idx, 1);
                return true;
            },
            count() {
                return self.deadLetters.length;
            },
            purge() {
                self.deadLetters.length = 0;
            },
        }));
        z.DESTRUCTION = this.ATOM.BIND("DESTRUCTION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            beforeDestruct: [],
            afterDestruct: [],
            records: [],
            destroyed: false,
            blocked: false,
            // ── hooks ──────────────────────────────────────────────────────────────────────────────────
            onBefore(fn) {
                self.beforeDestruct.push(fn);
                return () => {
                    self.beforeDestruct = self.beforeDestruct.filter(f => f !== fn);
                };
            },
            onAfter(fn) {
                self.afterDestruct.push(fn);
                return () => {
                    self.afterDestruct = self.afterDestruct.filter(f => f !== fn);
                };
            },
            /** Veto a pending destruction (e.g. in a beforeDestruct hook, or upfront). */
            block() {
                self.blocked = true;
            },
            unblock() {
                self.blocked = false;
            },
            // ── the kill switch ────────────────────────────────────────────────────────────────────────
            /**
             * Runs the full destruction sequence.
             * Returns the record on success, null when blocked or already destroyed.
             * `reviver` is a factory the record keeps so the cell can be rebuilt later.
             */
            destroy(cause = "unknown", reviver) {
                if (self.destroyed)
                    return null;
                if (self.blocked)
                    return null;
                const record = {
                    key: self.root_cell,
                    cause,
                    at: Date.now(),
                    reviver,
                };
                for (const fn of self.beforeDestruct) {
                    try {
                        fn(self.root_cell);
                    }
                    catch (e) {
                        console.error("[destruction-shard] beforeDestruct hook failed:", e);
                    }
                }
                self.destroyed = true;
                self.records.push(record);
                for (const fn of self.afterDestruct) {
                    try {
                        fn(record);
                    }
                    catch (e) {
                        console.error("[destruction-shard] afterDestruct hook failed:", e);
                    }
                }
                return record;
            },
            resurrect() {
                if (!self.destroyed)
                    return null;
                const last = self.records[self.records.length - 1];
                self.destroyed = false;
                self.blocked = false;
                return last?.reviver?.() ?? null;
            },
            isDestroyed() {
                return self.destroyed;
            },
            clearRecords() {
                self.records.length = 0;
            },
            purge() {
                self.beforeDestruct.length = 0;
                self.afterDestruct.length = 0;
                self.records.length = 0;
            },
        }));
        class ORC_SHOUT {
            c = {
                emerald: { 10: "#002c22", 9: "#004f3b", 8: "#016630", 7: "#007a55", 6: "#009966", 5: "#00bc7d" },
                blue: { 10: "", 9: "", 8: "", 7: "", 6: "", 5: "" },
            };
            t = {
                deep: {
                    bg: "#020817",
                    txt: "#f8fafc",
                    size: 12,
                    title_size: 14,
                    title_txt: "#94a3b8",
                    payload_key: "#a4ed2c",
                    payload_value: "#f8fafc",
                    font: "monospace",
                    border: "2px solid #101727",
                    borderRadius: "6px",
                    padding: "8px 16px",
                    svg: "",
                },
                vintage: {
                    bg: "#001B29",
                    txt: "#e9e1ba",
                    size: 12,
                    title_size: 14,
                    title_txt: "#ada785",
                    payload_key: "#29c3a0",
                    payload_value: "#fafafa",
                    font: "monospace",
                    border: "2px solid #112932",
                    borderRadius: "6px",
                    padding: "8px 16px",
                    svg: "",
                },
                error: {
                    bg: "#09090B",
                    txt: "#e7000b",
                    size: 12,
                    title_size: 14,
                    title_txt: "#9f0712",
                    payload_key: "#a4ed2c",
                    payload_value: "#f8fafc",
                    font: "monospace",
                    border: "2px solid #e7000b",
                    borderRadius: "6px",
                    padding: "8px 16px",
                    svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e7000b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
                },
                warn: {
                    bg: "#09090B",
                    txt: "#e17100",
                    size: 12,
                    title_size: 14,
                    title_txt: "#973c00",
                    payload_key: "#a4ed2c",
                    payload_value: "#f8fafc",
                    font: "monospace",
                    border: "2px solid #e17100",
                    borderRadius: "6px",
                    padding: "8px 16px",
                    svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e17100" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`,
                },
                success: {
                    bg: "#09090B",
                    txt: "#009966",
                    size: 12,
                    title_size: 14,
                    title_txt: "#016630",
                    payload_key: "#a4ed2c",
                    payload_value: "#f8fafc",
                    font: "monospace",
                    border: "2px solid #009966",
                    borderRadius: "6px",
                    padding: "8px 16px",
                    svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#009966" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.801 10A10 10 0 1 1 17 3.335"/><path d="m9 11 3 3L22 4"/></svg>`,
                },
                info: {
                    bg: "#09090B",
                    txt: "#155dfc",
                    size: 12,
                    title_size: 14,
                    title_txt: "#193cb8",
                    payload_key: "#a4ed2c",
                    payload_value: "#f8fafc",
                    font: "monospace",
                    border: "2px solid #155dfc",
                    borderRadius: "6px",
                    padding: "8px 16px",
                    svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#155dfc" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
                },
            };
            log(msg, p, theme = this.t.deep) {
                const isArray = Array.isArray(msg);
                const title = isArray ? msg[0] : msg;
                const paragraphs = isArray ? msg.slice(1) : [];
                const containerStyle = `
					background: ${theme.bg};
					${theme.border ? `border: ${theme.border};` : ""}
					${theme.borderRadius ? `border-radius: ${theme.borderRadius};` : ""}
					${theme.padding ? `padding: ${theme.padding};` : ""}
					font-family: ${theme.font};
					display: block;
					margin: 4px 0;
				`;
                const titleWithIcon = theme?.svg ? `${theme?.svg} ${title}` : title;
                const titleStyle = `
					color: ${theme.title_txt};
					font-size: ${theme.title_size}px;
					font-weight: bold;
					font-family: ${theme.font};
					display: flex;
					align-items: center;
					gap: 8px;
				`;
                const paragraphStyle = `
					color: ${theme.txt};
					font-size: ${theme.size}px;
					font-family: ${theme.font};
					margin-top: 4px;
				`;
                const keyStyle = `
					color: ${theme.payload_key};
					font-weight: bold;
					font-family: ${theme.font};
				`;
                const valueStyle = `
					color: ${theme.payload_value};
					font-family: ${theme.font};
				`;
                let payloadStr = "";
                if (p && typeof p === "object") {
                    const entries = Object.entries(p);
                    payloadStr = entries.map(([key, value]) => `%c${key}: %c${JSON.stringify(value, null, 2)}`).join("\n");
                }
                const logParts = [`%c${titleWithIcon}`, ...paragraphs.map((p) => `%c${p}`), payloadStr ? `\n${payloadStr}` : ""].filter(Boolean).join("\n");
                const styles = [containerStyle + titleStyle, ...paragraphs.map(() => paragraphStyle), ...(payloadStr ? [keyStyle, valueStyle] : [])];
                console.log(logParts, ...styles);
            }
        }
        const os = new ORC_SHOUT();
        z.ERROR = this.ATOM.BIND("ERROR", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            /** History lives here regardless of emission — history is cheap, display is gated. */
            error_history: [],
            HISTORY_CAP: 512,
            // ── runtime-editable controls ─────────────────────────────────────────────────────────────
            /** Master kill-switch. false → nothing emits, history still accrues. */
            ENABLED: true,
            /** Emission only in dev mode unless this is explicitly flipped on. */
            ENABLE_IN_PRODUCTION: false,
            /**
             * Surgical control. null = every module emits.
             * Record<string, boolean> = only listed modules emit, with true/false overriding.
             */
            SURGICAL_LOGS: (config.surgical ?? null),
            /** Theme passed through to the LOGGER. Default = the error theme. */
            theme: (os.t),
            /** "TERMINAL" | "FILE" — pipes through the LOGGER's configured sink. */
            PIPE_TO_FILE_OR_TERMINAL: "TERMINAL",
            /** Set at wiring time by the platform; falls back to plain console when absent. */
            logger: os.log,
            // ── the single emission point ─────────────────────────────────────────────────────────────
            /** Every error-producing call site in every shard funnels through here. */
            emit(module, code, message, detail) {
                const entry = { module, code, message, detail, ts: Date.now() };
                self.error_history.push(entry);
                if (self.error_history.length > self.HISTORY_CAP) {
                    self.error_history.splice(0, self.error_history.length - self.HISTORY_CAP);
                }
                if (self.shouldEmit(module)) {
                    const title = `[${String(self.root_cell_key)}] ${module} · ${code}`;
                    if (self.logger) {
                        self.logger.theme = self.theme;
                        self.logger.drop_log([title, message], detail);
                    }
                    else if (self.shouldEmit(module)) {
                        console.error(`${title}\n  ${message}`, detail ?? "");
                    }
                }
                return entry;
            },
            /** Surgical gate: master switch → dev/production → per-module toggle. */
            shouldEmit(module) {
                if (!self.ENABLED)
                    return false;
                if (self.ENABLE_IN_PRODUCTION === false && root.state.get("DEV_MODE") !== true)
                    return false;
                if (self.SURGICAL_LOGS === null)
                    return true;
                // exact match wins, then longest-prefix match (section-level control)
                if (module in self.SURGICAL_LOGS)
                    return self.SURGICAL_LOGS[module];
                let best = null;
                for (const prefix of Object.keys(self.SURGICAL_LOGS)) {
                    if (module.startsWith(prefix + ".") && (best === null || prefix.length > best.length)) {
                        best = prefix;
                    }
                }
                return best !== null ? self.SURGICAL_LOGS[best] : false;
            },
            // ── convenience emitters used by the call sites below ────────────────────────────────────
            sealed(detail) {
                return self.emit("GATE", "GATE_SEALED", "CELL IS SEALED — CHANGE SEALED TO FALSE TO RESUME WRITES", detail);
            },
            frozen(detail) {
                return self.emit("GATE", "GATE_FROZEN", "CELL IS FROZEN — VIEWABLE BUT NOT EDITABLE", detail);
            },
            asleep(detail) {
                return self.emit("GATE", "GATE_ASLEEP", "CELL IS ASLEEP — WILL NOT BE AVAILABLE UNTIL ITS CONFIGURED WAKE TIME", detail);
            },
            shardNotFound(name, detail) {
                return self.emit("REGISTRY", "SHARD_NOT_FOUND", `NO SHARD LABELED "${name}" RELATIVE TO THIS CELL — REVIEW YOUR CONTRACT`, detail);
            },
            contractViolation(contract, detail) {
                return self.emit("CONTRACTS", "CONTRACT_VIOLATION", `CONTRACT VIOLATION: ${contract}`, detail);
            },
            schemaViolation(topic, detail) {
                return self.emit("SCHEMA", "SCHEMA_VIOLATION", `SCHEMA VIOLATION ON TOPIC "${topic}"`, detail);
            },
            permissionDenied(ch, detail) {
                return self.emit("PERMISSIONS", "PERMISSION_DENIED", `PERMISSION DENIED ON CHANNEL "${ch}"`, detail);
            },
            /** Flip an entire section or a single module. */
            setSection(path, on) {
                (self.SURGICAL_LOGS ??= {})[path] = on;
            },
            clearSurgical() {
                self.SURGICAL_LOGS = null;
            },
            // ── diagnostics ───────────────────────────────────────────────────────────────────────────
            recent(limit = 20) {
                return self.error_history.slice(-limit);
            },
            byModule(module) {
                return self.error_history.filter(e => e.module === module);
            },
            byCode(code) {
                return self.error_history.filter(e => e.code === code);
            },
            purge() {
                self.error_history.length = 0;
            },
        }));
        z.EVENT = this.ATOM.BIND("EVENT", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            events: new Map(),
            wildcard: new Set(),
            on(event, fn) {
                const set = self.events.get(event) ?? new Set();
                set.add(fn);
                self.events.set(event, set);
                return () => set.delete(fn);
            },
            once(event, fn) {
                const off = self.on(event, (p) => {
                    off();
                    fn(p);
                });
                return off;
            },
            onAny(fn) {
                self.wildcard.add(fn);
                return () => self.wildcard.delete(fn);
            },
            off(event, fn) {
                if (!fn)
                    self.events.delete(event);
                else
                    self.events.get(event)?.delete(fn);
            },
            emit(event, payload) {
                let delivered = 0;
                for (const fn of self.wildcard) {
                    try {
                        fn(event, payload);
                        delivered++;
                    }
                    catch (e) {
                        console.error(`[event-shard] wildcard listener failed for "${event}":`, e);
                    }
                }
                for (const fn of [...(self.events.get(event) ?? [])]) {
                    try {
                        fn(payload);
                        delivered++;
                    }
                    catch (e) {
                        console.error(`[event-shard] listener failed for "${event}":`, e);
                    }
                }
                return delivered;
            },
            async emitAsync(event, payload) {
                let delivered = 0;
                for (const fn of self.wildcard) {
                    try {
                        await fn(event, payload);
                        delivered++;
                    }
                    catch (e) {
                        console.error(`[event-shard] wildcard listener failed for "${event}":`, e);
                    }
                }
                for (const fn of [...(self.events.get(event) ?? [])]) {
                    try {
                        await fn(payload);
                        delivered++;
                    }
                    catch (e) {
                        console.error(`[event-shard] listener failed for "${event}":`, e);
                    }
                }
                return delivered;
            },
            listenerCount(event) {
                if (event)
                    return self.events.get(event)?.size ?? 0;
                let n = self.wildcard.size;
                for (const set of self.events.values()) {
                    n += set.size;
                }
                return n;
            },
            eventNames() {
                return [...self.events.keys()];
            },
            purge() {
                self.events.clear();
                self.wildcard.clear();
            },
        }));
        z.EXTERNAL_SUBSCRIPTION = this.ATOM.BIND("EXTERNAL_SUBSCRIPTION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            externalSubscribers: new Set(),
            subscribe(listener) {
                self.externalSubscribers.add(listener);
                return () => {
                    self.externalSubscribers.delete(listener);
                };
            },
            notify(change) {
                for (const listener of self.externalSubscribers) {
                    try {
                        listener(change);
                    }
                    catch (error) {
                        console.error(`[soul-shard] listener failed for "${String(change.key)}":`, error);
                    }
                }
            },
            get size() {
                return self.externalSubscribers.size;
            },
            purge() {
                self.externalSubscribers.clear();
            },
        }));
        z.GATE = this.ATOM.BIND("GATE", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            sealed: (config.sealed ?? false),
            frozen: (config.frozen ?? false),
            sleep: config.sleep,
            get asleep() {
                return self.sleep !== undefined && Date.now() < self.sleep;
            },
            seal() {
                self.sealed = true;
            },
            unseal() {
                self.sealed = false;
            },
            freeze() {
                self.frozen = true;
            },
            unfreeze() {
                self.frozen = false;
            },
            sleepUntil(timestamp) {
                self.sleep = timestamp;
            },
            wake() {
                self.sleep = undefined;
            },
            canRead() {
                return !self.asleep;
            },
            canWrite() {
                return !self.sealed && !self.frozen && !self.asleep;
            },
        }));
        z.GLOBAL_SUBSCRIPTION = this.ATOM.BIND("GLOBAL_SUBSCRIPTION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            globalListeners: new Set(),
            subscribe(listener) {
                self.globalListeners.add(listener);
                return () => {
                    self.globalListeners.delete(listener);
                };
            },
            notify(change) {
                for (const listener of self.globalListeners) {
                    try {
                        listener(change);
                    }
                    catch (error) {
                        console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
                    }
                }
            },
            get size() {
                return self.globalListeners.size;
            },
            purge() {
                self.globalListeners.clear();
            },
        }));
        z.HOOK = this.ATOM.BIND("HOOK", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            hooks: new Map(),
            factories: new Map(),
            /** Register a hook factory. The factory runs once, lazily, on first read. */
            register(name, factory) {
                self.factories.set(name, factory);
                // if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('hook_registered', undefined, undefined, undefined, { name });
            },
            /** Read the hook's value, computing it on first access. */
            use(name) {
                const existing = self.hooks.get(name);
                if (existing)
                    return existing.value;
                const factory = self.factories.get(name);
                if (!factory)
                    throw new Error(`[hook-ELECTRON] no hook registered as "${name}"`);
                const value = factory();
                self.hooks.set(name, { value, registeredAt: Date.now() });
                // if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('hook_computed', undefined, undefined, undefined, { name });
                return value;
            },
            isComputed(name) {
                return self.hooks.has(name);
            },
            /** Drop the memoized value; the next use() re-runs the factory. Returns whether an entry existed. */
            reset(name) {
                const entry = self.hooks.get(name);
                if (!entry)
                    return false;
                self.hooks.delete(name);
                try {
                    entry.teardown?.();
                }
                catch (e) {
                    console.error(`[hook-ELECTRON] teardown failed for "${name}":`, e);
                }
                return true;
            },
            /** Drop the factory and the value. The hook is gone until re-registered. */
            unregister(name) {
                self.reset(name);
                self.factories.delete(name);
            },
            list() {
                return [...self.factories.keys()];
            },
            purge() {
                for (const entry of self.hooks.values()) {
                    try {
                        entry.teardown?.();
                    }
                    catch (e) {
                        console.error("[hook-ELECTRON] teardown failed during purge:", e);
                    }
                }
                self.hooks.clear();
                self.factories.clear();
            },
        }));
        z.HTTP_TRANSPORT = this.ATOM.BIND("HTTP_TRANSPORT", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            token: config.token,
            timeoutMs: (config.timeoutMs ?? 10_000),
            backoffMs: (config.backoffMs ?? 100),
            maxBackoffMs: (config.maxBackoffMs ?? 5_000),
            maxAttempts: (config.maxAttempts ?? 3),
            codec: BINARY_CODEC,
            onError: (config.onError ?? (() => { })),
            /** Inbound dispatcher — set at attach(). Receives decoded envelopes. */
            dispatch: null,
            // ── OUTBOUND: send an envelope to another IP ─────────────────────────────
            async send(endpoint, env) {
                const envelope = {
                    ts: Date.now(),
                    id: seqId(),
                    ...env,
                };
                const body = self.codec.encode_msg(envelope);
                let attempt = 0;
                let lastErr = null;
                while (attempt < self.maxAttempts) {
                    attempt++;
                    try {
                        const res = await fetch(endpoint, {
                            method: "POST",
                            headers: {
                                "content-type": "application/octet-stream",
                                authorization: `Bearer ${self.token}`,
                            },
                            body: body,
                            signal: AbortSignal.timeout(self.timeoutMs),
                        });
                        if (!res.ok) {
                            lastErr = new Error(`[http-transport] endpoint ${endpoint} responded ${res.status}`);
                            // 4xx = caller's fault, retrying will not help. Stop now.
                            if (res.status >= 400 && res.status < 500)
                                break;
                            // 5xx / network: retry with backoff
                        }
                        else {
                            const raw = new Uint8Array(await res.arrayBuffer());
                            const decoded = self.codec.decode_msg(raw);
                            if (decoded === null) {
                                lastErr = new Error(`[http-transport] endpoint ${endpoint} returned undecodable ack`);
                            }
                            else {
                                return decoded;
                            }
                        }
                    }
                    catch (err) {
                        lastErr = err;
                        // network-level failure: retry with backoff
                    }
                    if (attempt < self.maxAttempts) {
                        await new Promise(r => setTimeout(r, Math.min(self.backoffMs * 2 ** (attempt - 1), self.maxBackoffMs)));
                    }
                }
                self.onError(lastErr);
                return {
                    ok: false,
                    error: lastErr instanceof Error ? lastErr.message : String(lastErr),
                };
            },
            // ── INBOUND: accept POSTs from the wire ──────────────────────────────────
            /**
             * Wire this into any runtime's http server. `handler` must:
             *   • match on the path (transport does not route — the host decides)
             *   • read the full request body into a Uint8Array
             *   • call this.receive(bytes) and send back the returned ack frame
             * Node: http.createServer((req, res) => { if (transport.matches(req)) { ... res.end(ack) } })
             * Bun: Bun.serve({ fetch: (req) => transport.matchesBun(req) ? new Response(ack) : undefined })
             * Deno: same shape as Bun via req.arrayBuffer()
             */
            attach(dispatch) {
                self.dispatch = dispatch;
            },
            /** Path/content-type matcher for host servers. */
            matches(pathname, method) {
                return method === "POST" && pathname === "/bus";
            },
            /**
             * The one method a host server calls: bytes in → ack bytes out.
             * Auth is checked BEFORE dispatch. Dispatch happens inside try/catch —
             * a throwing handler becomes an error frame, never a crash.
             */
            async receive(bytes, authHeader) {
                const ack = (ok, result, error) => self.codec.encode_msg({ ok, result, error });
                if (authHeader !== `Bearer ${self.token}`) {
                    return ack(false, undefined, "unauthorized");
                }
                const env = self.codec.decode_msg(bytes);
                if (env === null || typeof env !== "object") {
                    return ack(false, undefined, "undecodable envelope");
                }
                const e = env;
                if (typeof e.r !== "string" ||
                    typeof e.m !== "string" ||
                    typeof e.s !== "string" ||
                    typeof e.k !== "string") {
                    return ack(false, undefined, "malformed envelope: rt/ms/sl/key required");
                }
                if (!self.dispatch) {
                    return ack(false, undefined, "no dispatcher attached");
                }
                try {
                    const result = await self.dispatch(env);
                    return ack(true, result);
                }
                catch (err) {
                    return ack(false, undefined, err instanceof Error ? err.message : String(err));
                }
            },
        }));
        z.GLOBAL_SUBSCRIPTION = this.ATOM.BIND("GLOBAL_SUBSCRIPTION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            globalListeners: new Set(),
            subscribe(listener) {
                self.globalListeners.add(listener);
                return () => {
                    self.globalListeners.delete(listener);
                };
            },
            notify(change) {
                for (const listener of self.globalListeners) {
                    try {
                        listener(change);
                    }
                    catch (error) {
                        console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
                    }
                }
            },
            get size() {
                return self.globalListeners.size;
            },
            purge() {
                self.globalListeners.clear();
            },
        }));
        z.METRICS = this.ATOM.BIND("METRICS", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            adapters: {
                metrics: new Map(),
            },
            seen: new Set(),
            getSeenCount() {
                return self.seen.size;
            },
            getMessageCount(rt) {
                return self.messageCount.get(rt) ?? 0;
            },
            messageCount: new Map(),
            getAdapterMetrics() {
                const now = Date.now();
                const out = [];
                for (const m of self.adapterMetrics.values()) {
                    const windowSec = Math.max(1, (now - m.lastWindow) / 1000);
                    const throughput = m.lastCount / windowSec;
                    const failureRate = m.total === 0 ? 0 : m.failures / m.total;
                    const latencyMs = m.total === 0 ? 0 : m.totalLatency / m.total;
                    out.push({
                        id: m.id,
                        latencyMs,
                        failureRate,
                        throughputPerSec: throughput,
                        healthy: !m.disabled && failureRate < 0.5,
                        disabled: m.disabled,
                    });
                    m.lastWindow = now;
                    m.lastCount = 0;
                }
                return out;
            },
            _queueMetrics: {
                totalEnqueued: 0,
                totalDropped: 0,
                totalProcessed: 0,
                peakSize: 0,
                currentSize: 0,
            },
        }));
        z.SUBSCRIPTION = this.ATOM.BIND("SUBSCRIPTION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            listeners: new Set(),
            subscribe(listener) {
                self.listeners.add(listener);
                return () => {
                    self.listeners.delete(listener);
                };
            },
            notify(change) {
                for (const listener of self.listeners) {
                    try {
                        listener(change);
                    }
                    catch (error) {
                        console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
                    }
                }
            },
            get size() {
                return self.listeners.size;
            },
            purge() {
                self.listeners.clear();
            },
        }));
        z.WAITER = this.ATOM.BIND("WAITER", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            waiters: new Set(),
            wait(timeout) {
                return new Promise((resolve, reject) => {
                    const waiter = {
                        resolve: resolve,
                        reject,
                    };
                    self.waiters.add(waiter);
                    if (timeout !== undefined && timeout > 0) {
                        const timer = setTimeout(() => {
                            if (self.waiters.delete(waiter)) {
                                reject(new Error("SOUL_ELECTRON wait timed out"));
                            }
                        }, timeout);
                        const originalResolve = waiter.resolve;
                        waiter.resolve = value => {
                            clearTimeout(timer);
                            originalResolve(value);
                        };
                        const originalReject = waiter.reject;
                        waiter.reject = reason => {
                            clearTimeout(timer);
                            originalReject(reason);
                        };
                    }
                });
            },
            resolve(value) {
                const waiters = [...self.waiters];
                self.waiters.clear();
                for (const waiter of waiters) {
                    waiter.resolve(value);
                }
                return waiters.length;
            },
            reject(reason) {
                const waiters = [...self.waiters];
                self.waiters.clear();
                for (const waiter of waiters) {
                    waiter.reject(reason);
                }
                return waiters.length;
            },
            purge() {
                self.reject(new Error("SOUL_ELECTRON waiter ELECTRON destroyed"));
            },
        }));
        z.SESSION = this.ATOM.BIND("SESSION", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            _cse_token: (config.cse_token ?? null),
            _sessionId: null,
            _sessionToken: null,
            _session_started_at: null,
            _session_expires_at: null,
            _auth_vized: false,
            _auth_initialized: false,
            _sessionTtlMs: (config.sessionTtlMs ?? 1000 * 60 * 60 * 12),
            // ── crypto ─────────────────────────────────────────────────────────────────────────────────
            computeHmacSha256(secret, data) {
                if (BUS_CRYPTO) {
                    const key = BUS_CRYPTO.to_utf8_bytes(secret);
                    const mac = BUS_CRYPTO.hash_hmac(key, data);
                    return BUS_CRYPTO.to_hex(BUS_CRYPTO.from_base64(mac));
                }
                return "";
            },
            // ── lifecycle ──────────────────────────────────────────────────────────────────────────────
            /**
             * Stand the session up. Reads CSE_TOKEN from .env when no token was injected.
             * No token anywhere? The session still runs, but UNAUTHORIZED (matches the bus's stance).
             */
            async initAuth() {
                if (self._auth_initialized)
                    return;
                if (!self._cse_token && typeof process !== "undefined") {
                    try {
                        //   if (dotenv) dotenv.config();
                    }
                    catch {
                        // env loader optional
                    }
                    const sk = (typeof process !== "undefined" && process.env?.CSE_TOKEN) || null;
                    if (sk && sk.length >= 5)
                        self._cse_token = sk;
                }
                self._newSession();
                self._auth_initialized = true;
            },
            _newSession() {
                const now = Date.now();
                if (self._cse_token && BUS_CRYPTO) {
                    self._sessionId = BUS_CRYPTO.to_hex(BUS_CRYPTO.random_bytes(16));
                    self._sessionToken = self.computeHmacSha256(self._cse_token, self._sessionId);
                }
                else {
                    // degraded mode: still unique, just not cryptographically bound
                    self._sessionId = `sess_${now}_${Math.random().toString(36).slice(2)}`;
                    self._sessionToken = null;
                }
                self._session_started_at = now;
                self._session_expires_at = now + self._sessionTtlMs;
                self._auth_vized = !!self._sessionToken;
            },
            /** Rotate the session id + token. Anything wrapped before is invalid after this. */
            renew() {
                self._newSession();
            },
            /** Force-expire. secureUnwrap() will reject everything until renew() or initAuth(). */
            expire() {
                self._session_expires_at = Date.now() - 1;
                self._auth_vized = false;
            },
            isValid() {
                return self._session_expires_at !== null && Date.now() < self._session_expires_at;
            },
            get expired() {
                return !self.isValid();
            },
            get isSecure() {
                return self._cse_token !== null && self._sessionToken !== null;
            },
            // ── wrap / unwrap ──────────────────────────────────────────────────────────────────────────
            /** Wrap a payload with the cell's session identity. */
            secureWrap(p) {
                return {
                    sessionId: self._sessionId,
                    token: self._sessionToken,
                    ts: Date.now(),
                    p,
                };
            },
            /** Unwrap + verify. Tampered, stale, or foreign session → null. */
            async secureUnwrap(msg) {
                if (!msg?.sessionId || !msg?.token)
                    return null;
                if (msg.sessionId !== self._sessionId)
                    return null; // not our session
                if (!self.isValid())
                    return null; // our session but expired
                const expected = await self.computeHmacSha256(self._cse_token ?? "", msg.sessionId);
                if (self._sessionToken && msg.token !== expected)
                    return null;
                return msg.p;
            },
            purge() {
                self._sessionId = null;
                self._sessionToken = null;
                self._session_started_at = null;
                self._session_expires_at = null;
                self._auth_vized = false;
                self._auth_initialized = false;
            },
        }));
        z.ROUTE = this.ATOM.BIND("ROUTE", ({ self, master, config }) => ({
            k: config.k ?? "__SIGNALS__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            routeRules: [],
            toRegex(pattern) {
                return new RegExp("^" +
                    pattern
                        .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
                        .replace(/\*\*/g, ".*")
                        .replace(/\*/g, "[^.]+") +
                    "$");
            },
            addRouteRule(rule) {
                self.routeRules.push(rule);
                if (self.DEVTOOLS_ACTIVE) {
                    self.emitDevtoolsEvent("route_added", undefined, undefined, undefined, {
                        name: rule.name,
                    });
                }
            },
            removeRouteRule(name) {
                self.routeRules = self.routeRules.filter(r => r.name !== name);
                if (self.DEVTOOLS_ACTIVE) {
                    self.emitDevtoolsEvent("route_removed", undefined, undefined, undefined, { name });
                }
            },
            listRouteRules() {
                return [...self.routeRules];
            },
            /** First matching enabled rule wins. Returns the rewritten target(s); adapter defaults to same_boundary. */
            resolve(rt, ms, sl, k) {
                for (const rule of self.routeRules) {
                    if (rule.enabled === false)
                        continue;
                    if (!self.toRegex(rule.match).test(rt))
                        continue;
                    const target = {
                        adapter: rule.adapter ?? "same_boundary",
                        rt: rule.target?.rt ?? rt,
                        ms: rule.target?.ms ?? ms,
                        sl: rule.target?.sl ?? sl,
                        k: rule.target?.k ?? k,
                        // priority: rule.priority,
                    };
                    if (self.DEVTOOLS_ACTIVE) {
                        self.emitDevtoolsEvent("route_resolved", rt, ms, undefined, {
                            rule: rule.name,
                            adapter: target.adapter,
                        });
                    }
                    return [target];
                }
                // no rule: the identity target — same address, same boundary (PATCH 4's fallback)
                return [{ adapter: "same_boundary", rt, ms, sl, k }];
            },
            purge() {
                self.routeRules.length = 0;
            },
        }));
        z.RIFTS_ACCESS = this.ATOM.BIND("RIFTS_ACCESS", ({ self, master, config }) => ({
            k: config.k ?? "__MODULE_ATOM__",
            s: config.s ?? "SAURON_SLAVE_REG",
            m: config.m ?? "SAURON_MASTER_REG",
            r: config.r ?? "THE_WIZARD_ROOT_REG",
            initial: config.options?.module_type === "class_fn_pool" || !config.options?.module_type ? { ...config.initial } : config.initial,
            module: config.options?.module_type === "class_fn_pool" || !config.options?.module_type ? { ...config.initial } : config.initial,
            module_type: config.options?.module_type ?? "class_fn_pool",
            version: 0,
            sealed: false,
            frozen: (config.options?.module_type ?? "class_fn_pool") === "class_fn_pool" ? false : config.options?.frozen ?? true,
            hasWritten: false,
            persisted: false,
            listeners: new Set(),
            waiters: new Set(),
            undoStack: undefined,
            conflict: config.options?.conflict,
            historyIndex: 0,
            history: config.options?.history ? [{ k: 0, value: config.initial, timestamp: Date.now(), source: "initial", }] : undefined,
            historyMax: config.options?.history ? Math.max(1, config.options.history.max ?? 50) : 50,
            lifecycle: config.options?.lifecycle,
            async: undefined,
            metrics: undefined,
            conflictStrategy: config.options?.conflictStrategy,
            schema: config.options?.schema,
            contracts: new Map(),
            causal: [],
            lastWriteSource: undefined,
            batches: new Set(),
            batchQueue: new Map(),
            batchOpts: new Map(),
            triggerRender: true,
            boundary: "same-app",
            read() { return self.module; },
            peek() { return self.module; },
            write(incoming, options = {}) {
                if (self.frozen)
                    return self.peek();
                const prev = self.module;
                const resolved = incoming;
                const next = self.module_type === "class_fn_pool" ? resolved ? { ...prev, ...resolved } : prev : resolved;
                self.module = next;
                self.hasWritten = true;
                self.version += 1;
                self.lastWriteSource = options.source ?? "write";
                self.assertContracts(next, prev);
                for (const l of self.listeners) {
                    try {
                        l(next);
                    }
                    catch (e) { }
                }
                root.bus.send(next, self.k, self.s, self.m, self.r);
                for (const w of self.waiters) {
                    try {
                        w(next);
                    }
                    catch (e) { }
                }
                if (options.recordHistory !== false) {
                    self.recordHistory(prev, next, self.lastWriteSource);
                }
                self.recordCausal(next, self.lastWriteSource, options.cause);
                self.waiters.clear();
                self.notifyListeners({
                    key: self.key,
                    previous: prev,
                    current: next,
                    version: self.version,
                    timestamp: Date.now(),
                    source: self.lastWriteSource,
                });
                return next;
            },
            register(name, val) { return self.write((prev) => ({ ...prev, [name]: val })); },
            unregister(name) {
                return self.write((prev) => {
                    const { [name]: _gone, ...rest } = prev;
                    return rest;
                });
            },
            slot(name) { return self.module[name]; },
            on(listener) { self.listeners.add(listener); listener(self.module); },
            waitFor() { return new Promise(resolve => { self.waiters.add(resolve); }); },
            async waitForSlot(name, timeoutMs = 0) {
                const existing = self.module[name];
                if (existing !== undefined)
                    return existing;
                if (!timeoutMs) {
                    const next = await self.waitFor();
                    return next[name];
                }
                return Promise.race([
                    self.waitFor().then(next => next[name]),
                    new Promise((_, reject) => setTimeout(() => reject(new Error(`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}ms`)), timeoutMs)),
                ]);
            },
            inspect() { return { ...self.module }; },
            setSchema(schema) { self.schema = schema; return self; },
            setLifecycle(lifecycle) { self.lifecycle = lifecycle; return self; },
            setConflict(conflict, strategy) {
                self.conflict = conflict;
                self.conflictStrategy = strategy;
                return self;
            },
            setFrozen() { self.frozen = true; },
            setUnfrozen() { self.frozen = false; },
            setSealed() { self.sealed = true; },
            setUnsealed() { self.sealed = false; },
            setBoundary() { },
            validate(next) {
                if (!self.schema)
                    return next;
                return this._.validateSchema(self.schema, next);
            },
            assertContracts(next, previous) {
                for (const contract of self.contracts.values()) {
                    let valid = false;
                    try {
                        valid = contract.postcondition(next, previous);
                    }
                    catch (error) {
                        console.error(`[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`, error);
                    }
                    if (valid)
                        continue;
                    const message = `[Sauron] contract "${contract.name}" violated ` +
                        `on write to "${String(self.key)}"`;
                    if (contract.block) {
                        throw new Error(message);
                    }
                    console.error(message);
                }
            },
            recordHistory(previous, next, source) {
                if (!self.history)
                    return;
                self.history.splice(self.historyIndex + 1);
                const last = self.history[self.history.length - 1];
                self.history.push({
                    k: (last?.k ?? -1) + 1,
                    prev: previous,
                    value: next,
                    next,
                    timestamp: Date.now(),
                    source,
                    undone: false,
                });
                while (self.history.length > self.historyMax) {
                    self.history.shift();
                }
                self.historyIndex = self.history.length - 1;
            },
            getHistory() {
                return self.history ?? [];
            },
            timeTravel(steps = 1) {
                if (!self.history?.length || steps <= 0)
                    return undefined;
                const idx = Math.max(0, Math.min(self.history.length - 1, self.historyIndex + steps));
                if (idx === self.historyIndex)
                    return undefined;
                self.historyIndex = idx;
                return self.replayHistory("time-travel");
            },
            back(steps = 1) {
                return self.timeTravel(-Math.abs(steps));
            },
            forward(steps = 1) {
                return self.timeTravel(Math.abs(steps));
            },
            goto(sequence) {
                if (!self.history?.length)
                    return undefined;
                const idx = self.history.findIndex(entry => entry.k === sequence);
                if (idx < 0 || idx === self.historyIndex)
                    return undefined;
                self.historyIndex = idx;
                return self.replayHistory("time-travel.goto");
            },
            replayHistory(source) {
                const entry = self.history[self.historyIndex];
                return self.write(entry.value, {
                    source,
                    cause: "time-travel",
                    triggerRender: true,
                    recordHistory: false,
                });
            },
            recordCausal(value, source, cause) {
                self.causal.push({
                    value,
                    source,
                    cause,
                    timestamp: Date.now(),
                });
                if (self.causal.length > 100) {
                    self.causal.shift();
                }
            },
            recordMetrics() {
                if (!self.metrics)
                    return;
                self.metrics.writeCount += 1;
                self.metrics.lastWrite = Date.now();
            },
            notifyListeners(change) {
                const listeners = self._listeners.get(self.key);
                if (!listeners)
                    return;
                for (const listener of listeners) {
                    try {
                        listener(change);
                    }
                    catch (error) {
                        console.error(`[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`, error);
                    }
                }
            },
            resolveWaiters(value) {
                const waiting = self._waiters.get(self.key);
                if (!waiting)
                    return;
                self._waiters.delete(self.key);
                for (const resolve of waiting) {
                    try {
                        resolve(value);
                    }
                    catch (error) {
                        console.error(`[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`, error);
                    }
                }
            },
            notifyExternalSubscribers() {
                const subscribers = self._externalSubscribers.get(self.key);
                if (!subscribers)
                    return;
                for (const callback of subscribers) {
                    try {
                        callback();
                    }
                    catch (error) {
                        console.error(`[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`, error);
                    }
                }
            },
        }));
        return z;
    })();
    contracts = (() => {
        const z = () => { };
        z.DEFAULT = this.ATOM.CONTRACT(() => ({ atom: "VERTEX" }));
        z.vertexContract = this.ATOM.CONTRACT(() => ({
            atom: "VERTEX",
            value: { count: 0 },
            bindables: { conflict: this.ATOMS.CONFLICT, ttl: this.ATOMS.TTL },
        }));
        z.atomMasterContract = this.ATOM.CONTRACT(() => ({
            atom: "ATOM",
            value: 0,
            bindables: { ttl: this.ATOMS.TTL },
        }));
        z.moduleContract = this.ATOM.CONTRACT(() => ({
            atom: "MODULE",
            value: [],
            bindables: { conflict: this.ATOMS.CONFLICT },
        }));
        return z;
    })();
    atoms_def = new (class DEFINITION {
        MASTER_ATOM_KEYS = ["ATOM", "MODULE", "CLOUD", "SAURON", "DRS", "MSG_Q", "SLAB", "S_CLASS", "SPREADSHEET", "VERTEX"];
        MQ = {
            RETRY: this.DEFINE_ATOM_KIND("retry"),
            ACK: this.DEFINE_ATOM_KIND("ack"),
            DEADLINE: this.DEFINE_ATOM_KIND("deadline"),
            DEDUPE: this.DEFINE_ATOM_KIND("dedupe"),
            DEPTH: this.DEFINE_ATOM_KIND("depth"),
            WILDCARD: this.DEFINE_ATOM_KIND("wildcard"),
            REPLY: this.DEFINE_ATOM_KIND("reply"),
            WINDOW: this.DEFINE_ATOM_KIND("window"),
            RESUME: this.DEFINE_ATOM_KIND("resume"),
            CREDIT: this.DEFINE_ATOM_KIND("credit"),
            CORRELATION: this.DEFINE_ATOM_KIND("correlation"),
            SEND: this.DEFINE_ATOM_KIND("send"),
            SUB: this.DEFINE_ATOM_KIND("send"),
            STREAM: this.DEFINE_ATOM_KIND("send"),
            HANDLER: this.DEFINE_ATOM_KIND("send"),
            DUPLEX: this.DEFINE_ATOM_KIND("send"),
            DRS: this.DEFINE_ATOM_KIND("send"),
            ANTIMSGQUEUE: this.DEFINE_ATOM_KIND("send"),
        };
        ELECTRONS = {
            AW_SET: this.DEFINE_ATOM_KIND("adapter"),
            CASUAL_CURSOR: this.DEFINE_ATOM_KIND("adapter"),
            DELTA_STORE: this.DEFINE_ATOM_KIND("adapter"),
            G_SET: this.DEFINE_ATOM_KIND("adapter"),
            VERSION_VECTOR: this.DEFINE_ATOM_KIND("adapter"),
            CIRCUIT_BREAKER: this.DEFINE_ATOM_KIND("adapter"),
            CQRS: this.DEFINE_ATOM_KIND("adapter"),
            RAFT: this.DEFINE_ATOM_KIND("adapter"),
            YJS: this.DEFINE_ATOM_KIND("adapter"),
            DIFFPATCH: this.DEFINE_ATOM_KIND("adapter"),
            HYBRID_LOGICAL_CLOCK: this.DEFINE_ATOM_KIND("adapter"),
            LWW: this.DEFINE_ATOM_KIND("adapter"),
            LWW_ELEMENTAL: this.DEFINE_ATOM_KIND("adapter"),
            MV: this.DEFINE_ATOM_KIND("adapter"),
            OPTIMISTIC: this.DEFINE_ATOM_KIND("adapter"),
            OR: this.DEFINE_ATOM_KIND("adapter"),
            PN: this.DEFINE_ATOM_KIND("adapter"),
            REVERSE_HEARTBEAT_MGR: this.DEFINE_ATOM_KIND("adapter"),
            SEMAPHORE: this.DEFINE_ATOM_KIND("adapter"),
            TOMBSTONE_COMPACTOR: this.DEFINE_ATOM_KIND("adapter"),
            YIMIR: this.DEFINE_ATOM_KIND("adapter"),
            HYBRID_LOGICAL_CLOCK_CRDT: this.DEFINE_ATOM_KIND("adapter"),
            ADAPTER: this.DEFINE_ATOM_KIND("adapter"),
            BEAM_CODEC: this.DEFINE_ATOM_KIND("beam_codec"),
            BINARY: this.DEFINE_ATOM_KIND("binary"),
            BYTE_CODE_VM: this.DEFINE_ATOM_KIND("byte_code_vm"),
            CHANNEL: this.DEFINE_ATOM_KIND("channel"),
            CHANNELS: this.DEFINE_ATOM_KIND("channels"),
            CONCURRENCY: this.DEFINE_ATOM_KIND("concurrency"),
            DEADLETTER: this.DEFINE_ATOM_KIND("deadletter"),
            DESTRUCTION: this.DEFINE_ATOM_KIND("destruction"),
            ERROR: this.DEFINE_ATOM_KIND("error"),
            EVENT: this.DEFINE_ATOM_KIND("event"),
            EXTERNAL_SUBSCRIPTION: this.DEFINE_ATOM_KIND("exterenal_sub"),
            GATE: this.DEFINE_ATOM_KIND("gate"),
            GLOBAL_SUBSCRIPTION: this.DEFINE_ATOM_KIND("global_sub"),
            GOVERNOR: this.DEFINE_ATOM_KIND("GOVERNOR"),
            HOOK: this.DEFINE_ATOM_KIND("hook"),
            HTTP_TRANSPORT: this.DEFINE_ATOM_KIND("http_transport"),
            INJECTION_CODEC: this.DEFINE_ATOM_KIND("injection"),
            METRICS: this.DEFINE_ATOM_KIND("metrics"),
            MIDDLEWARE: this.DEFINE_ATOM_KIND("middlleware"),
            RIFTS_ACCESS: this.DEFINE_ATOM_KIND("rifts"),
            REACTIVE: this.DEFINE_ATOM_KIND("reactive"),
            ROUTE: this.DEFINE_ATOM_KIND("route"),
            RUN_ON: this.DEFINE_ATOM_KIND("run_on"),
            SEALED_ERROR: this.DEFINE_ATOM_KIND("sealed_error"),
            SESSION: this.DEFINE_ATOM_KIND("session"),
            SHELL: this.DEFINE_ATOM_KIND("reshelltry"),
            SIGNAL: this.DEFINE_ATOM_KIND("signal"),
            SOA_QUAD_TIERED_REGISTRY: this.DEFINE_ATOM_KIND("soa_registry"),
            SUBSCRIPTION: this.DEFINE_ATOM_KIND("subscription"),
            TRANSFORM: this.DEFINE_ATOM_KIND("transform"),
            WAITER: this.DEFINE_ATOM_KIND("waiter"),
            CONTRACTS: this.DEFINE_ATOM_KIND("contract"),
            SCHEMA: this.DEFINE_ATOM_KIND("schema"),
            VALUES_REGISTRY: this.DEFINE_ATOM_KIND("values_reg"),
            COMPRESSION: this.DEFINE_ATOM_KIND("compression"),
            CONFLICT: this.DEFINE_ATOM_KIND("conflict"),
            CONTEXT: this.DEFINE_ATOM_KIND("context"),
            ENCRYPTION: this.DEFINE_ATOM_KIND("encryption"),
            GROUPS: this.DEFINE_ATOM_KIND("groups"),
            HISTORY: this.DEFINE_ATOM_KIND("history"),
            META: this.DEFINE_ATOM_KIND("meta"),
            OFFSEC: this.DEFINE_ATOM_KIND("offsec"),
            PERMISSIONS: this.DEFINE_ATOM_KIND("permissions"),
            PERSISTENCE: this.DEFINE_ATOM_KIND("persistence"),
            PRIORITY: this.DEFINE_ATOM_KIND("priority"),
            SNAPSHOT: this.DEFINE_ATOM_KIND("snapshot"),
            TAGS: this.DEFINE_ATOM_KIND("tags"),
            TIME_TRAVEL: this.DEFINE_ATOM_KIND("time_travel"),
            TTL: this.DEFINE_ATOM_KIND("ttl"),
        };
        DEFINE_ATOM_KIND(name) {
            return Symbol(name);
        }
        PARTICLE_KEYS = {
            ...this.ELECTRONS,
            ...this.MQ,
        };
    })();
    signals_raptor_ngin = new (class DARTH_MAUL_ENGINE {
        signals_raptor;
        root;
        signals;
        //                                                                                                                    CONSTANTS
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        capacity;
        lane;
        ops;
        posX;
        posY;
        val;
        freePids;
        freeHead = 0;
        freeTail = 0;
        laneLen = 0;
        nextAllocatedPid = 1;
        // Lane deduplication: 1 means this PID already has a pending lane entry.
        inLane;
        // ======================================================================
        // Reactive node columns
        // ======================================================================
        rKind;
        rFlags;
        rValue;
        rPending;
        rGetter;
        rFn;
        rCleanup;
        rDeps;
        rDepsTail;
        rSubs;
        rSubsTail;
        // ======================================================================
        // Link pool
        // ======================================================================
        lVersion;
        lDep;
        lSub;
        lPrevDep;
        lNextDep;
        lPrevSub;
        lNextSub;
        freeLinks;
        freeLinkHead = 0;
        freeLinkTail = 0;
        nextLink = 1;
        // ======================================================================
        // Scheduler
        // ======================================================================
        effectQueue;
        queuedLength = 0;
        notifyIndex = 0;
        HasChildEffect = 64;
        cycle = 0;
        runDepth = 0;
        batchDepth = 0;
        activeSub;
        BRAND_SIGNAL = Symbol("warlock.signal");
        BRAND_EFFECT = Symbol("warlock.effect");
        BRAND_EFFECT_SCOPE = Symbol("warlock.effectScope");
        BRAND_COMPUTED = Symbol("warlock.computed");
        // #endregion
        constructor(s, capacity = 1 << 18) {
            this.root = s.root;
            this.signals_raptor = s.signals_raptor;
            this.signals = s;
            this.capacity = capacity;
            this.lane = new Int32Array(capacity).fill(-1);
            this.ops = new Uint8Array(capacity);
            this.posX = new Float32Array(capacity);
            this.posY = new Float32Array(capacity);
            this.val = new Int32Array(capacity);
            this.freePids = new Int32Array(capacity);
            this.inLane = new Uint8Array(capacity);
            this.rKind = new Uint8Array(capacity);
            this.rFlags = new Uint16Array(capacity);
            this.rValue = new Array(capacity);
            this.rPending = new Array(capacity);
            this.rGetter = new Array(capacity);
            this.rFn = new Array(capacity);
            this.rCleanup = new Array(capacity);
            this.rDeps = new Int32Array(capacity).fill(-1);
            this.rDepsTail = new Int32Array(capacity).fill(-1);
            this.rSubs = new Int32Array(capacity).fill(-1);
            this.rSubsTail = new Int32Array(capacity).fill(-1);
            this.effectQueue = new Int32Array(capacity);
            this.lVersion = new Int32Array(capacity);
            this.lDep = new Int32Array(capacity);
            this.lSub = new Int32Array(capacity);
            this.lPrevDep = new Int32Array(capacity).fill(-1);
            this.lNextDep = new Int32Array(capacity).fill(-1);
            this.lPrevSub = new Int32Array(capacity).fill(-1);
            this.lNextSub = new Int32Array(capacity).fill(-1);
            this.freeLinks = new Int32Array(capacity);
            for (let i = capacity - 1; i >= 1; i--) {
                this.freePids[this.freeTail++] = i;
            }
            this.lane[0] = TRACER_PID;
            this.val[TRACER_PID] = 0;
            this.ops[TRACER_PID] = OP.NONE;
            this.laneLen = 1;
        }
        // ======================================================================
        // World-object lane API — unchanged semantics, now deduplicated
        // ======================================================================
        enqueueOp(pid, op) {
            if (pid === TRACER_PID)
                return;
            // Dedup: if already queued, only the latest operation wins.
            if (this.inLane[pid]) {
                this.ops[pid] = op;
                return;
            }
            if (this.laneLen >= this.capacity) {
                throw new Error("DISRUPTOR: lane capacity exhausted");
            }
            this.ops[pid] = op;
            this.inLane[pid] = 1;
            this.lane[this.laneLen++] = pid;
        }
        check_size_and_edit(x, y, pid, value) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.EDIT);
        }
        getX(pid) {
            return this.posX[pid];
        }
        getY(pid) {
            return this.posY[pid];
        }
        getPosition(pid) {
            return { x: this.posX[pid], y: this.posY[pid] };
        }
        write(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.WRITE);
        }
        create(x, y, value, pid) {
            this.write(x, y, value, pid);
        }
        edit(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.EDIT);
        }
        read(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.READ);
        }
        delete(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.DELETE);
        }
        effect(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.EFFECT);
        }
        compact(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.COMPACT);
        }
        none(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.NONE);
        }
        send(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.SEND);
        }
        sub(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.SUB);
        }
        duplex(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.DUPLEX);
        }
        stream(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.STREAM);
        }
        drc(x, y, value, pid) {
            this.posX[pid] = x;
            this.posY[pid] = y;
            this.val[pid] = value;
            this.enqueueOp(pid, OP.DRC);
        }
        // ======================================================================
        // Lane pass — Warlock structure preserved, dedup markers cleared
        // ======================================================================
        pass() {
            const len = this.laneLen;
            let writeIdx = 0;
            let i = 0;
            const unrolledLimit = len - (len % 4);
            for (; i < unrolledLimit; i += 4) {
                for (let k = 0; k < 4; k++) {
                    const pid = this.lane[i + k];
                    if (pid === TRACER_PID) {
                        this.val[TRACER_PID]++;
                        this.lane[writeIdx++] = pid;
                    }
                    else if (this.ops[pid] === OP.DELETE) {
                        this.inLane[pid] = 0;
                        this.reclaimPid(pid);
                    }
                    else {
                        this.inLane[pid] = 0;
                        this.ops[pid] = OP.NONE;
                        this.lane[writeIdx++] = pid;
                    }
                }
            }
            for (; i < len; i++) {
                const pid = this.lane[i];
                if (pid === TRACER_PID) {
                    this.val[TRACER_PID]++;
                    this.lane[writeIdx++] = pid;
                }
                else if (this.ops[pid] === OP.DELETE) {
                    this.inLane[pid] = 0;
                    this.reclaimPid(pid);
                }
                else {
                    this.inLane[pid] = 0;
                    this.ops[pid] = OP.NONE;
                    this.lane[writeIdx++] = pid;
                }
            }
            this.laneLen = writeIdx;
        }
        /** One tick: world-object pass, then reactive effect flush. */
        tick() {
            this.pass();
            this.flush();
        }
        get tickCount() { return this.val[TRACER_PID]; }
        get length() { return this.laneLen; }
        getLoadFactor() {
            const activeCount = Math.max(0, this.laneLen - 1);
            return activeCount / (this.capacity - 1);
        }
        waitFor() { }
        sweep_expired() { }
        on_change() { }
        // ======================================================================
        // PID / link lifecycle
        // ======================================================================
        allocPid() {
            if (this.freeHead !== this.freeTail) {
                const pid = this.freePids[this.freeHead];
                this.freeHead = (this.freeHead + 1) % this.capacity;
                return pid;
            }
            if (this.nextAllocatedPid >= this.capacity) {
                throw new Error("DISRUPTOR: capacity exhausted — no free pids");
            }
            return this.nextAllocatedPid++;
        }
        allocLink() {
            if (this.freeLinkHead !== this.freeLinkTail) {
                const lid = this.freeLinks[this.freeLinkHead];
                this.freeLinkHead = (this.freeLinkHead + 1) % this.capacity;
                return lid;
            }
            if (this.nextLink >= this.capacity) {
                throw new Error("DISRUPTOR: link capacity exhausted");
            }
            return this.nextLink++;
        }
        freeLink(lid) {
            this.lVersion[lid] = 0;
            this.lDep[lid] = -1;
            this.lSub[lid] = -1;
            this.lPrevDep[lid] = -1;
            this.lNextDep[lid] = -1;
            this.lPrevSub[lid] = -1;
            this.lNextSub[lid] = -1;
            this.freeLinks[this.freeLinkTail] = lid;
            this.freeLinkTail = (this.freeLinkTail + 1) % this.capacity;
        }
        dispose(pid) { this.reclaimPid(pid); }
        reclaimPid(pid) {
            if (pid === TRACER_PID)
                return;
            if (this.rFlags[pid] & RX_Flags.DISPOSED)
                return;
            switch (this.rKind[pid]) {
                case 3 /* RKind.EFFECT */:
                    this.effectOper(pid);
                    break;
                case 4 /* RKind.EFFECT_SCOPE */:
                    this.effectScopeOper(pid);
                    break;
                case 2 /* RKind.COMPUTED */:
                    this.disposeAllDepsInReverse(pid);
                    break;
            }
            this.rKind[pid] = 0 /* RKind.NONE */;
            this.rFlags[pid] = RX_Flags.DISPOSED;
            this.rValue[pid] = undefined;
            this.rPending[pid] = undefined;
            this.rGetter[pid] = undefined;
            this.rFn[pid] = undefined;
            this.rCleanup[pid] = undefined;
            this.rDeps[pid] = -1;
            this.rDepsTail[pid] = -1;
            this.rSubs[pid] = -1;
            this.rSubsTail[pid] = -1;
            this.inLane[pid] = 0;
            this.freePids[this.freeTail] = pid;
            this.freeTail = (this.freeTail + 1) % this.capacity;
        }
        // ======================================================================
        // Reactive graph links
        // ======================================================================
        link(dep, sub, version) {
            const prevDep = this.rDepsTail[sub];
            if (prevDep !== -1 && this.lDep[prevDep] === dep) {
                return;
            }
            const nextDep = prevDep !== -1 ? this.lNextDep[prevDep] : this.rDeps[sub];
            if (nextDep !== -1 && this.lDep[nextDep] === dep) {
                this.lVersion[nextDep] = version;
                this.rDepsTail[sub] = nextDep;
                return;
            }
            const prevSub = this.rSubsTail[dep];
            if (prevSub !== -1 && this.lVersion[prevSub] === version && this.lSub[prevSub] === sub) {
                return;
            }
            const lid = this.allocLink();
            this.lVersion[lid] = version;
            this.lDep[lid] = dep;
            this.lSub[lid] = sub;
            this.lPrevDep[lid] = prevDep;
            this.lNextDep[lid] = nextDep;
            this.lPrevSub[lid] = prevSub;
            this.lNextSub[lid] = -1;
            this.rDepsTail[sub] = lid;
            this.rSubsTail[dep] = lid;
            if (nextDep !== -1)
                this.lPrevDep[nextDep] = lid;
            if (prevDep !== -1)
                this.lNextDep[prevDep] = lid;
            else
                this.rDeps[sub] = lid;
            if (prevSub !== -1)
                this.lNextSub[prevSub] = lid;
            else
                this.rSubs[dep] = lid;
        }
        unlink(lid, sub = this.lSub[lid]) {
            const dep = this.lDep[lid];
            const prevDep = this.lPrevDep[lid];
            const nextDep = this.lNextDep[lid];
            const prevSub = this.lPrevSub[lid];
            const nextSub = this.lNextSub[lid];
            if (nextDep !== -1)
                this.lPrevDep[nextDep] = prevDep;
            else
                this.rDepsTail[sub] = prevDep;
            if (prevDep !== -1)
                this.lNextDep[prevDep] = nextDep;
            else
                this.rDeps[sub] = nextDep;
            if (nextSub !== -1)
                this.lPrevSub[nextSub] = prevSub;
            else
                this.rSubsTail[dep] = prevSub;
            if (prevSub !== -1) {
                this.lNextSub[prevSub] = nextSub;
            }
            else {
                this.rSubs[dep] = nextSub;
                // Dependency has no subscribers left.
                if (nextSub === -1) {
                    this.unwatched(dep);
                }
            }
            this.freeLink(lid);
            return nextDep;
        }
        disposeAllDepsInReverse(pid) {
            let link = this.rDepsTail[pid];
            while (link !== -1) {
                const prev = this.lPrevDep[link];
                this.unlink(link, pid);
                link = prev;
            }
        }
        purgeDeps(pid) {
            const tail = this.rDepsTail[pid];
            let link = tail !== -1 ? this.lNextDep[tail] : this.rDeps[pid];
            while (link !== -1) {
                link = this.unlink(link, pid);
            }
        }
        unwatched(pid) {
            switch (this.rKind[pid]) {
                case 2 /* RKind.COMPUTED */:
                    this.rFlags[pid] = RX_Flags.MUTABLE;
                    this.disposeAllDepsInReverse(pid);
                    break;
                case 1 /* RKind.SIGNAL */:
                    break;
                case 3 /* RKind.EFFECT */:
                    this.effectOper(pid);
                    break;
                case 4 /* RKind.EFFECT_SCOPE */:
                    this.effectScopeOper(pid);
                    break;
            }
        }
        // ======================================================================
        // Propagation — corrected iterative version
        // ======================================================================
        propagate(startLink, innerWrite) {
            if (startLink === -1)
                return;
            const stack = [startLink];
            while (stack.length > 0) {
                const link = stack.pop();
                const sub = this.lSub[link];
                if (sub === -1)
                    continue;
                let flags = this.rFlags[sub];
                if (flags & RX_Flags.RECURSEDCHECK) {
                    // Currently evaluating; defer until it finishes.
                    continue;
                }
                if (!(flags & RX_Flags.PENDING)) {
                    this.rFlags[sub] = flags | RX_Flags.PENDING;
                    if (innerWrite) {
                        this.rFlags[sub] |= RX_Flags.RECURSED;
                    }
                    flags = this.rFlags[sub];
                }
                if (flags & RX_Flags.WATCHING) {
                    this.notify(sub);
                }
                // Computeds continue propagation to their own subscribers.
                if (this.rKind[sub] === 2 /* RKind.COMPUTED */) {
                    let childLink = this.rSubs[sub];
                    while (childLink !== -1) {
                        stack.push(childLink);
                        childLink = this.lNextSub[childLink];
                    }
                }
            }
        }
        shallowPropagate(startLink) {
            let link = startLink;
            while (link !== -1) {
                const sub = this.lSub[link];
                const flags = this.rFlags[sub];
                if (flags & RX_Flags.PENDING) {
                    if ((flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) === RX_Flags.WATCHING) {
                        this.notify(sub);
                    }
                }
                link = this.lNextSub[link];
            }
        }
        notify(effectPid) {
            let pid = effectPid;
            while (pid !== -1 && pid !== TRACER_PID) {
                const flags = this.rFlags[pid];
                if (!(flags & RX_Flags.WATCHING))
                    break;
                // Queue this effect.
                if (this.queuedLength < this.effectQueue.length) {
                    this.effectQueue[this.queuedLength++] = pid;
                }
                // Clear WATCHING so it is not queued twice.
                this.rFlags[pid] &= ~RX_Flags.WATCHING;
                // Walk to parent effect/scope, if any.
                const parentLink = this.rSubs[pid];
                if (parentLink === -1)
                    break;
                const parent = this.lSub[parentLink];
                if (parent === -1 || (this.rKind[parent] !== 3 /* RKind.EFFECT */ && this.rKind[parent] !== 4 /* RKind.EFFECT_SCOPE */)) {
                    break;
                }
                pid = parent;
            }
        }
        // ======================================================================
        // Dirty checking — version-based, no recursive graph walk
        // ======================================================================
        isDirty(pid) {
            let link = this.rDeps[pid];
            while (link !== -1) {
                const dep = this.lDep[link];
                if (this.rKind[dep] === 2 /* RKind.COMPUTED */) {
                    if (this.isDirty(dep)) {
                        this.updateComputed(dep);
                    }
                }
                if (this.lVersion[link] !== this.lVersion[dep]) {
                    return true;
                }
                link = this.lNextDep[link];
            }
            return false;
        }
        // Alias kept for compatibility with the original API.
        checkDirty(_link, pid) {
            return this.isDirty(pid);
        }
        // ======================================================================
        // Signal operations
        // ======================================================================
        signalOper(pid, ...value) {
            if (value.length > 0) {
                const next = value[0];
                if (!Object.is(this.rPending[pid], next)) {
                    this.rPending[pid] = next;
                    this.rFlags[pid] |= RX_Flags.DIRTY;
                    const subs = this.rSubs[pid];
                    if (subs !== -1) {
                        this.propagate(subs, this.runDepth > 0);
                    }
                    if (this.batchDepth === 0) {
                        this.commitSignal(pid);
                        this.flush();
                    }
                }
                return;
            }
            if (this.rFlags[pid] & RX_Flags.DIRTY) {
                this.commitSignal(pid);
            }
            const sub = this.activeSub;
            if (sub !== undefined) {
                this.link(pid, sub, this.cycle);
            }
            return this.rValue[pid];
        }
        commitSignal(pid) {
            const previous = this.rValue[pid];
            const next = this.rPending[pid];
            this.rFlags[pid] &= ~RX_Flags.DIRTY;
            if (Object.is(previous, next))
                return;
            this.rValue[pid] = next;
            this.cycle++;
            this.lVersion[pid] = this.cycle;
            const subs = this.rSubs[pid];
            if (subs !== -1) {
                this.propagate(subs, this.runDepth > 0);
            }
        }
        updateSignal(pid) {
            const previous = this.rValue[pid];
            const next = this.rPending[pid];
            this.rValue[pid] = next;
            return !Object.is(previous, next);
        }
        // ======================================================================
        // Computed operations
        // ======================================================================
        computedOper(pid) {
            if (this.isDirty(pid)) {
                this.updateComputed(pid);
                const subs = this.rSubs[pid];
                if (subs !== -1) {
                    this.shallowPropagate(subs);
                }
            }
            const sub = this.activeSub;
            if (sub !== undefined) {
                this.link(pid, sub, this.cycle);
            }
            return this.rValue[pid];
        }
        updateComputed(pid) {
            if (this.rFlags[pid] & RX_Flags.RUNNING) {
                return false;
            }
            // Remove stale child-effect ownership links before re-evaluating.
            if (this.rFlags[pid] & this.HasChildEffect) {
                let link = this.rDepsTail[pid];
                while (link !== -1) {
                    const prev = this.lPrevDep[link];
                    const dep = this.lDep[link];
                    if (this.rKind[dep] !== 1 /* RKind.SIGNAL */ && this.rKind[dep] !== 2 /* RKind.COMPUTED */) {
                        this.unlink(link, pid);
                    }
                    link = prev;
                }
            }
            // Full refresh: drop all old dependency links, then retrack.
            this.disposeAllDepsInReverse(pid);
            this.rFlags[pid] = RX_Flags.RUNNING | RX_Flags.RECURSEDCHECK;
            const previousValue = this.rValue[pid];
            const previousSub = this.setActiveSub(pid);
            try {
                this.cycle++;
                const nextValue = this.rGetter[pid](previousValue);
                this.rValue[pid] = nextValue;
                this.lVersion[pid] = this.cycle;
                return !Object.is(previousValue, nextValue);
            }
            finally {
                this.activeSub = previousSub;
                this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
                this.purgeDeps(pid);
            }
        }
        // ======================================================================
        // Effects
        // ======================================================================
        run(pid) {
            if (this.rKind[pid] !== 3 /* RKind.EFFECT */)
                return;
            const flags = this.rFlags[pid];
            if (flags & RX_Flags.RUNNING) {
                // Re-entrant run: defer rather than recurse.
                this.scheduleEffect(pid);
                return;
            }
            const pending = (flags & RX_Flags.PENDING) !== 0;
            const dirty = pending ? this.isDirty(pid) : false;
            if (!pending && !dirty && this.rDeps[pid] !== -1) {
                this.rFlags[pid] = RX_Flags.WATCHING | (flags & this.HasChildEffect);
                return;
            }
            if (this.rCleanup[pid]) {
                this.runCleanup(pid);
                if (this.rFlags[pid] & RX_Flags.DISPOSED) {
                    return;
                }
            }
            // Full refresh before rerun: prevents stale dependency retention.
            this.disposeAllDepsInReverse(pid);
            this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
            const previousSub = this.setActiveSub(pid);
            try {
                this.cycle++;
                this.runDepth++;
                const cleanup = this.rFn[pid]();
                this.rCleanup[pid] = typeof cleanup === "function" ? cleanup : undefined;
            }
            finally {
                this.runDepth--;
                this.activeSub = previousSub;
                this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
                this.purgeDeps(pid);
            }
        }
        runCleanup(pid) {
            const cleanup = this.rCleanup[pid];
            if (!cleanup)
                return;
            this.rCleanup[pid] = undefined;
            const previousSub = this.activeSub;
            this.activeSub = undefined;
            try {
                cleanup();
            }
            finally {
                this.activeSub = previousSub;
            }
        }
        effectOper(pid) {
            if (this.rFlags[pid] & RX_Flags.DISPOSED)
                return;
            this.runCleanup(pid);
            this.effectScopeOper(pid);
        }
        effectScopeOper(pid) {
            if (this.rFlags[pid] & RX_Flags.DISPOSED)
                return;
            // Remove this scope from its parent before disposing children.
            this.unlinkParent(pid);
            // Dispose all child effects/scopes linked to this scope.
            this.disposeAllDepsInReverse(pid);
            this.rFlags[pid] |= RX_Flags.DISPOSED;
        }
        unlinkParent(pid) {
            let link = this.rSubs[pid];
            while (link !== -1) {
                const next = this.lNextSub[link];
                const parent = this.lSub[link];
                if (this.rKind[parent] === 3 /* RKind.EFFECT */ || this.rKind[parent] === 4 /* RKind.EFFECT_SCOPE */) {
                    this.unlink(link, pid);
                    // Clear parent ownership flag if it has no remaining children.
                    if (this.rDeps[parent] === -1) {
                        this.rFlags[parent] &= ~this.HasChildEffect;
                    }
                }
                link = next;
            }
        }
        scheduleEffect(pid) {
            if (this.rFlags[pid] & RX_Flags.PENDING)
                return;
            this.rFlags[pid] |= RX_Flags.PENDING;
            if (this.queuedLength < this.effectQueue.length) {
                this.effectQueue[this.queuedLength++] = pid;
            }
        }
        // ======================================================================
        // Flush
        // ======================================================================
        flush() {
            try {
                while (this.notifyIndex < this.queuedLength) {
                    const pid = this.effectQueue[this.notifyIndex];
                    this.effectQueue[this.notifyIndex++] = -1;
                    this.rFlags[pid] &= ~RX_Flags.PENDING;
                    this.run(pid);
                }
            }
            finally {
                // Any effects scheduled during flush remain queued for next tick.
                while (this.notifyIndex < this.queuedLength) {
                    const pid = this.effectQueue[this.notifyIndex];
                    this.effectQueue[this.notifyIndex++] = -1;
                    this.rFlags[pid] |= RX_Flags.WATCHING | RX_Flags.RECURSED;
                }
                this.notifyIndex = 0;
                this.queuedLength = 0;
            }
        }
        untrack = (fn) => {
            const prev = this.setActiveSub(undefined);
            try {
                return fn();
            }
            finally {
                this.setActiveSub(prev);
            }
        };
        // ======================================================================
        // Factories
        // ======================================================================
        allocNode(kind) {
            const pid = this.allocPid();
            this.rKind[pid] = kind;
            this.rFlags[pid] = RX_Flags.NONE;
            this.rValue[pid] = undefined;
            this.rPending[pid] = undefined;
            this.rGetter[pid] = undefined;
            this.rFn[pid] = undefined;
            this.rCleanup[pid] = undefined;
            this.rDeps[pid] = -1;
            this.rDepsTail[pid] = -1;
            this.rSubs[pid] = -1;
            this.rSubsTail[pid] = -1;
            return pid;
        }
        signal(initialValue) {
            const pid = this.allocNode(1 /* RKind.SIGNAL */);
            this.rValue[pid] = initialValue;
            this.rPending[pid] = initialValue;
            this.rFlags[pid] = RX_Flags.MUTABLE;
            return pid;
        }
        computed(getter) {
            const pid = this.allocNode(2 /* RKind.COMPUTED */);
            this.rGetter[pid] = getter;
            return pid;
        }
        effectSignal(fn) {
            const pid = this.allocNode(3 /* RKind.EFFECT */);
            this.rFn[pid] = fn;
            this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
            const previousSub = this.setActiveSub(pid);
            if (previousSub !== undefined && previousSub !== -1) {
                this.rFlags[previousSub] |= this.HasChildEffect;
            }
            try {
                this.runDepth++;
                const cleanup = fn();
                this.rCleanup[pid] = typeof cleanup === "function" ? cleanup : undefined;
            }
            finally {
                this.runDepth--;
                this.activeSub = previousSub;
                this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
            }
            return pid;
        }
        effectScope(fn) {
            const pid = this.allocNode(4 /* RKind.EFFECT_SCOPE */);
            this.rFlags[pid] = RX_Flags.MUTABLE;
            const previousSub = this.setActiveSub(pid);
            if (previousSub !== undefined && previousSub !== -1) {
                // Parent link: scope is dep, parent is sub.
                this.link(pid, previousSub, 0);
                this.rFlags[previousSub] |= this.HasChildEffect;
            }
            try {
                fn();
            }
            finally {
                this.activeSub = previousSub;
            }
            return pid;
        }
        trigger(fn) {
            const watcher = this.allocNode(3 /* RKind.EFFECT */);
            this.rFlags[watcher] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
            const previousSub = this.setActiveSub(watcher);
            this.batchDepth++;
            try {
                fn();
            }
            finally {
                this.activeSub = previousSub;
                // Propagate every dependency recorded during fn().
                let link = this.rDeps[watcher];
                while (link !== -1) {
                    const dep = this.lDep[link];
                    const next = this.lNextDep[link];
                    const subs = this.rSubs[dep];
                    if (subs !== -1) {
                        this.propagate(subs, this.runDepth > 0);
                        this.shallowPropagate(subs);
                    }
                    link = next;
                }
                this.batchDepth--;
                if (this.batchDepth === 0) {
                    this.flush();
                }
                this.reclaimPid(watcher);
            }
        }
        // ======================================================================
        // Compatibility helpers
        // ======================================================================
        getActiveSub() {
            return this.activeSub;
        }
        setActiveSub(sub) {
            const previous = this.activeSub;
            this.activeSub = sub;
            return previous;
        }
        getBatchDepth() {
            return this.batchDepth;
        }
        startBatch() {
            this.batchDepth++;
        }
        endBatch() {
            if (--this.batchDepth === 0) {
                this.flush();
            }
        }
        isSignal(pid) {
            return this.rKind[pid] === 1 /* RKind.SIGNAL */;
        }
        isComputed(pid) {
            return this.rKind[pid] === 2 /* RKind.COMPUTED */;
        }
        isEffect(pid) {
            return this.rKind[pid] === 3 /* RKind.EFFECT */;
        }
        isEffectScope(pid) {
            return this.rKind[pid] === 4 /* RKind.EFFECT_SCOPE */;
        }
    })(this);
    signals_raptor = new (class DARTH_MAUL {
        //                                                                                                                    CONSTANTS
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        root;
        hub;
        signals;
        state;
        DARTH_MAUL_ENGINE;
        r = "this.r";
        m = "this.m";
        s = "this.s";
        k = "__SIGNALS__";
        u = undefined;
        uuuu = [this.u, this.u, this.u, this.u];
        UUUU = [this.s, this.m, this.r];
        uuu = [this.u, this.u, this.u];
        UUU = [this.s, this.m, this.r];
        uu = [this.u, this.u];
        UU = [this.m, this.r];
        total_cache_size = 5000;
        activeTracer = null;
        // capacity = 1 << 20
        // #endregion
        //                                                                                                                         MAPS
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        #maul_meta = new M2();
        _getMetadata(s = this.s, m = this.m, r = this.r) {
            let root = this.#maul_meta.get(r);
            if (!root) {
                root = new M2();
                this.#maul_meta.set(r, root);
            }
            let masterMap = root.get(m);
            if (!masterMap) {
                masterMap = new M2();
                root.set(m, masterMap);
            }
            let metaMap = masterMap.get(s);
            if (!metaMap) {
                metaMap = {
                    _initial: new M2(),
                    _sys: new M2(),
                    _keyContracts: new M2(),
                    _causal: new M2(),
                    _conflicts: new M2(),
                    _contracts: new M2(),
                    _scopes: new M2(),
                    _msgHandlers: new M2(),
                    _mainMap: new M2(),
                    _tupleCache: new M2(),
                    _weakMap: new WeakMap(),
                };
                masterMap.set(s, metaMap);
            }
            return metaMap;
        }
        // #endregion
        //                                                                                                                        ATOMS
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        // #endregion
        //                                                                                                                          API
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        atom(v, key = `__THE_WARLOCK_SIGNALS_${this.root.reg._sid()}`, s = this.s, m = this.m, r = this.r, options) {
            const rk = this.create(key, v, options, s, m, r);
            const self = this;
            return {
                get value() {
                    self._.bindComponentToSignal(rk, s, m, r);
                    return self.read(rk, s, m, r);
                },
                get current() {
                    self._.bindComponentToSignal(rk, s, m, r);
                    return self.read(rk, s, m, r);
                },
                peek() {
                    return rk.peek();
                },
                set value(v) {
                    self.write(rk, v, "write", s, m, r);
                },
                set current(v) {
                    self.write(rk, v, "write", s, m, r);
                },
                set crt(v) {
                    self.write(rk, v, "write", s, m, r);
                },
                set set(v) {
                    self.write(rk, v, "write", s, m, r);
                },
            };
        }
        create(k, v, options, s = this.s, m = this.m, r = this.r) {
            const registry = this.hub.wr._signals;
            let atom = registry.get(k);
            if (atom && v === undefined) {
                return atom;
            }
            if (atom && v !== undefined) {
                const vv = typeof v === "function" ? v() : v;
                atom.write(vv);
                return atom;
            }
            const initialValue = typeof v === "function" ? v() : v;
            atom = this.signals.contracts.DEFAULT(initialValue, k, s, m, r);
            registry.set(k, atom);
            return atom;
        }
        get(k, s = this.s, m = this.m, r = this.r) {
            const registry = this.hub.wr._signals;
            return registry.get(k);
        }
        write(k, value, source = "write", s = this.s, m = this.m, r = this.r) {
            return this._.writeResolved(k, value, source, s, m, r);
        }
        read(k, s = this.s, m = this.m, r = this.r) {
            const atom = this.hub.wr._signals.get(k);
            return atom?.read();
        }
        peek(k, s = this.s, m = this.m, r = this.r) {
            const atom = this.hub.wr._signals.get(k);
            return atom?.peek();
        }
        signal(initialValue) {
            const eng = this.DARTH_MAUL_ENGINE;
            const pid = eng.signal(initialValue);
            const fn = ((...args) => eng.signalOper(pid, ...args));
            fn.pid = pid;
            Object.defineProperty(fn, eng.BRAND_SIGNAL, { value: true });
            return fn;
        }
        has(k, s = this.s, m = this.m, r = this.r) { return this.hub.wr._signals.get(k) !== undefined; }
        freeze(k, s = this.s, m = this.m, r = this.r) {
            const atom = this.hub.wr._signals.get(k);
            if (!atom) {
                throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
            }
            atom.setFrozen();
        }
        unfreeze(k, s = this.s, m = this.m, r = this.r) {
            const atom = this.hub.wr._signals.get(k);
            if (!atom) {
                throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
            }
            atom.setUnfrozen();
        }
        seal(k, s = this.s, m = this.m, r = this.r) {
            const meta = this.hub.wr._signals;
            const atom = meta.get(k);
            atom.setSealed();
        }
        reset(k, s = this.s, m = this.m, r = this.r) {
            const atom = this.hub.wr._signals.get(k);
            if (!atom) {
                throw new Error(`[Sauron] Cannot reset unknown signal "${String(k)}"`);
            }
            atom.value = atom.initial;
            // this._writeResolved(k, atom.initial, "reset", s, m, r, "reset");
        }
        hydrate(k, value, s = this.s, m = this.m, r = this.r) {
            let atom = this.hub.wr._signals.get(k);
            if (!atom) {
                this.create(k, value, undefined, s, m, r);
                return;
            }
            // atom.write(value, { source: "hydrate", triggerRender: false });
        }
        tx(fn) {
            const o = { s: this.s, m: this.m, r: this.r };
            const snap = new Map();
            for (const [k, v] of Object.entries(this.hub.state.state.getAll(o)))
                snap.set(k, v);
            const sysSnap = this._sys.snapshot();
            const self = this;
            try {
                fn();
            }
            catch (err) {
                for (const k of Object.keys(this.hub.state.state.getAll(o)))
                    if (!snap.has(k))
                        this.hub.state.state.delete(k, o);
                for (const [k, v] of snap)
                    this.hub.state.state.set(k, v, { ...o, triggerRender: false });
                self._sys.dropComputeds();
                self._sys.restore(sysSnap);
                throw err;
            }
        }
        derive = (() => {
            const fn = (k, deps, compute, s = this.s, m = this.m, r = this.r, options) => {
                if (this._getMetadata(s, m, r)._sys.get(k)?.isComputed)
                    return k;
                const reg = this.hub.wr._signals;
                for (const d of deps) {
                    this._sys.ensure(d, reg.get(d), s, m, r);
                }
                const c = computed(() => {
                    try {
                        const vals = deps.map((d) => this._sys.ensure(d, reg.get(d), s, m, r).read());
                        return compute(...vals);
                    }
                    catch (err) {
                        options?.onError?.(err);
                        throw err;
                    }
                });
                this._getMetadata(s, m, r)._sys.set(k, { read: () => c(), write: () => { }, node: c, isComputed: true });
                reg.set(k, this.DARTH_MAUL_ENGINE.untrack(() => c()));
                if (options?.eager) {
                    // Warm the computed cache once. Do not write its result back
                    // into another signal; that defeats lazy recomputation.
                    this.DARTH_MAUL_ENGINE.untrack(() => c());
                }
                return k;
            };
            fn.invalidate = (k, s = this.s, m = this.m, r = this.r) => {
                const entry = this._getMetadata(s, m, r)._sys.get(k);
                if (!entry || !entry.isComputed)
                    return;
                this._.writeResolved(k, this.DARTH_MAUL_ENGINE.untrack(() => entry.read()), "derived", "invalidate", s, m, r);
            };
            return fn;
        })();
        // #endregion
        //                                                                                                                   SEND / SUB
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        #defBucket;
        #bufs = new WeakMap(); // open batch buffers, touched only inside batches
        bucket(s, m, r) {
            if (s === this.s && m === this.m && r === this.r) {
                return (this.#defBucket ??= this.hub.wr.map(s, m, r, "#NAKED_SIGNALS"));
            }
            return this.hub.wr.map(s, m, r, "#NAKED_SIGNALS");
        }
        channel(k, s, m, r, create) {
            const b = this.bucket(s, m, r);
            let sig = b.get(k);
            if (!sig && create) {
                sig = this.signal(undefined);
                b.set(k, sig);
            }
            return sig;
        }
        sub = (() => {
            const fn = (listener, k, s = this.s, m = this.m, r = this.r) => {
                const eng = this.DARTH_MAUL_ENGINE;
                const sig = this.channel(k, s, m, r, true);
                let prev;
                let first = true;
                let live = true;
                const pid = eng.effectSignal(() => {
                    const env = sig();
                    if (first) {
                        first = false;
                        return;
                    }
                    if (!env)
                        return;
                    eng.untrack(() => {
                        if (env.b !== undefined) {
                            // batched delivery: replies can't make it back, ignore return values
                            if (this.#bufs.get(sig) === env.b)
                                this.#bufs.delete(sig);
                            for (const p of env.b) {
                                try {
                                    listener(p, prev);
                                }
                                catch (e) {
                                    console.error("[bus]", e);
                                }
                                prev = p;
                            }
                        }
                        else {
                            try {
                                const v = listener(env.p, prev);
                                if (v !== undefined)
                                    (env.r ??= []).push(v); // array only allocated if someone replies
                            }
                            catch (e) {
                                console.error("[bus]", e);
                            }
                            prev = env.p;
                        }
                    });
                    // effect body returns nothing on purpose: a returned function would be treated as cleanup
                });
                return () => { if (live) {
                    live = false;
                    eng.dispose(pid);
                } };
            };
            fn.naked = (k, s = this.s, m = this.m, r = this.r) => this.channel(k, s, m, r, true);
            return fn;
        })();
        send = (() => {
            const dispatch = (p, k, s, m, r) => {
                const sig = this.channel(k, s, m, r, false);
                if (!sig)
                    return;
                if (this.DARTH_MAUL_ENGINE.batchDepth > 0) {
                    const buf = this.#bufs.get(sig);
                    if (buf) {
                        buf.push(p);
                        return;
                    }
                    const b = [p];
                    this.#bufs.set(sig, b);
                    sig({ b });
                    return; // flush is deferred, so there are no replies yet
                }
                const env = { p, r: undefined };
                sig(env); // synchronous commit + flush: listeners have already run
                return env.r;
            };
            const fn = (p, k, s = this.s, m = this.m, r = this.r) => dispatch(p, k, s, m, r)?.[0];
            fn.all = (p, k, s = this.s, m = this.m, r = this.r) => dispatch(p, k, s, m, r) ?? [];
            return fn;
        })();
        delete = (k, s = this.s, m = this.m, r = this.r) => {
            this.hub.wr._signals.delete(k);
            this._getMetadata(s, m, r)._sys.delete(k);
        };
        // #endregion
        //                                                                                                               [TRACE EFFECT]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        trace = (() => {
            const fn = (fn) => {
                const reads = new Map();
                const previous = this.activeTracer;
                this.activeTracer = reads;
                try {
                    const result = fn();
                    return { reads: [...reads.keys()], result };
                }
                finally {
                    this.activeTracer = previous;
                }
            };
            fn.effect = (fn, s = this.s, m = this.m, r = this.r) => {
                const reads = new Map();
                const runs = [];
                let runCount = 0;
                const tracked = new Map();
                const registry = this.hub.wr._signals;
                const effectDispose = effect(() => {
                    runCount++;
                    const before = new Map();
                    for (const [key, cell] of tracked) {
                        before.set(key, cell.peek());
                    }
                    const result = fn();
                    const touched = new Map();
                    for (const [key, cell] of tracked) {
                        const value = cell.peek();
                        if (!Object.is(before.get(key), value)) {
                            touched.set(key, value);
                        }
                    }
                    runs.push({
                        run: runCount,
                        reads: new Map(tracked),
                        at: Date.now(),
                    });
                    return result;
                });
                // Wrap reads by exposing a tracing proxy is not needed for the
                // first version: cells register themselves when read through
                // useCell / read(). For explicit tracing, callers can pass keys.
                return effectDispose;
            }; // Finds the first recorded write whose value differs from the current live value. That is the divergence point.
            fn.bisect = (entries, s = this.s, m = this.m, r = this.r) => {
                const registry = this.hub.wr._signals;
                for (let i = 0; i < entries.length; i++) {
                    const entry = entries[i];
                    const atom = registry.get(entry.key);
                    if (!atom)
                        continue;
                    const actual = atom.peek();
                    if (!Object.is(actual, entry.value)) {
                        return {
                            index: i,
                            key: entry.key,
                            recorded: entry.value,
                            actual,
                        };
                    }
                }
                return undefined;
            };
            fn.why = (k, s = this.s, m = this.m, r = this.r) => {
                const atom = this.hub.wr._signals.get(k);
                if (!atom)
                    return undefined;
                return {
                    key: atom.key,
                    value: atom.peek(),
                    version: atom.version,
                    lastWriteSource: atom.lastWriteSource,
                    causal: [...atom.causal].reverse(),
                    history: atom.getHistory(),
                };
            };
            return fn;
        })();
        // #endregion
        //                                                                                                              [STATE SURGERY]
        // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        surgery = (() => {
            // Live-state surgery. Reads the current value, hands it to `mutate`, writes the result back through the normal reactive write path, records a causal entry, and pushes an undo entry onto the surgery journal.
            // The mutation receives the current value. It may either: - mutate and return nothing (object/array mutation), or - return a replacement value.
            const fn = (k, mutate, s = this.s, m = this.m, r = this.r, options) => {
                const atom = this.hub.wr._signals.get(k);
                if (!atom) {
                    throw new Error(`[Sauron] Cannot perform surgery on unknown signal "${String(k)}"`);
                }
                if (atom.frozen) {
                    throw new Error(`[Sauron] Cannot perform surgery on frozen signal "${String(k)}"`);
                }
                const previous = atom.peek(); // Structured clone gives surgery isolation without touching the reactive kernel. Primitive values pass through unchanged.
                const draft = previous !== null && typeof previous === "object" ? structuredClone(previous) : previous;
                const result = mutate(draft);
                const next = result === undefined ? draft : result;
                if (Object.is(previous, next)) {
                    return previous;
                }
                const source = options?.source ?? "surgery";
                const cause = options?.cause ?? `surgery:${String(k)}:${Date.now()}`;
                const written = atom.write(next);
                this.surgery.journal.push({ key: k, previous, next: written, s, m, r });
                return written;
            }; //  Undo the most recent surgery. Restoration goes through the normal write path, so computeds, effects, subscribers, history, and channels all see a real write.
            fn.journal = [];
            fn.undo = () => {
                const entry = this.surgery.journal.pop();
                if (!entry)
                    return undefined;
                const atom = this.hub.wr._signals.get(entry.key);
                if (!atom)
                    return undefined;
                return atom.write(entry.previous, {
                    source: "surgery:undo",
                    cause: `surgery:undo:${String(entry.key)}`,
                    triggerRender: true,
                    recordHistory: true,
                });
            }; //   Undo every surgery performed since the returned marker was created. Undo every surgery performed since the returned marker was created.
            fn.checkpoint = () => {
                const mark = this.surgery.journal.length;
                return () => {
                    while (this.surgery.journal.length > mark) {
                        this.surgery.undo();
                    }
                };
            }; // Surgery on a nested path. Example: sauron.surgeryAt('user', ['profile', 'email'], email => email.trim());
            fn.at = (k, path, mutate, s = this.s, m = this.m, r = this.r, options) => {
                return this.surgery(k, (draft) => {
                    let target = draft;
                    for (let i = 0; i < path.length - 1; i++) {
                        target = target[path[i]];
                    }
                    const leafKey = path[path.length - 1];
                    const leaf = target[leafKey];
                    const result = mutate(leaf);
                    target[leafKey] = result === undefined ? leaf : result;
                    // Returning undefined keeps the outer draft as the next value.
                    return undefined;
                }, s, m, r, options);
            };
            return fn;
        })();
        _sys = (() => {
            const fn = () => { };
            fn.ensure = (rk, v, s = this.s, m = this.m, r = this.r) => {
                const sysMap = this._getMetadata(s, m, r)._sys;
                let entry = sysMap.get(rk);
                if (entry)
                    return entry;
                const sig = signal(v);
                entry = { read: () => sig(), write: (v) => sig(v), node: sig, isComputed: false };
                sysMap.set(rk, entry);
                if (this.hub.state.state.get(rk, { s, m, r }) === undefined) {
                    this.hub.state.state.set(rk, v, { s, m, r, triggerRender: false });
                }
                return entry;
            };
            fn.snapshot = (s = this.s, m = this.m, r = this.r) => {
                const out = new Map();
                for (const [rk, entry] of this._getMetadata(s, m, r)._sys) {
                    if (!entry.isComputed)
                        out.set(rk, this.DARTH_MAUL_ENGINE.untrack(() => entry.read()));
                }
                return out;
            };
            fn.restore = (snap) => {
                for (const [rk, v] of snap)
                    this._sys.ensure(rk, v).write(v);
            };
            fn.dropComputeds = (s = this.s, m = this.m, r = this.r) => {
                for (const [rk, entry] of this._getMetadata(s, m, r)._sys) {
                    if (entry.isComputed)
                        this._getMetadata(s, m, r)._sys.delete(rk);
                }
            };
            return fn;
        })();
        // #endregion
        //                                                                                                                      BACKEND
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        _ = (() => {
            const fn = () => { };
            fn.validateSchema = (schema, value) => {
                try {
                    const next = schema.coerce ? schema.coerce(value) : value;
                    if (schema.validate(next)) {
                        return next;
                    }
                    throw new Error("[Sauron] Schema validation failed");
                }
                catch (error) {
                    console.error("[Sauron] Schema validation failed:", error);
                    throw error;
                }
            };
            fn.serializeTuple = (tuple, s = this.s, m = this.m, r = this.r) => {
                const parts = new Array(tuple.length);
                const weakMap = this._getMetadata(s, m, r)._weakMap;
                for (let i = 0; i < tuple.length; i++) {
                    const item = tuple[i];
                    const type = typeof item;
                    if (item === null) {
                        parts[i] = "null";
                    }
                    else if (type === "string") {
                        parts[i] = `s:${item}`;
                    }
                    else if (type === "number") {
                        parts[i] = `n:${item}`;
                    }
                    else if (type === "symbol") {
                        parts[i] = `sym:${item.description ?? String(item)}`;
                    }
                    else if (type === "bigint") {
                        parts[i] = `bi:${item.toString()}`;
                    }
                    else if (type === "undefined") {
                        parts[i] = "undef";
                    }
                    else if (Array.isArray(item)) {
                        parts[i] = `arr:[${this._.serializeTuple(item, s, m, r)}]`;
                    }
                    else if (type === "object" || type === "function") {
                        let refId = weakMap.get(item);
                        if (!refId) {
                            refId = `ref:${this.hub.reg._sid()}`;
                            weakMap.set(item, refId);
                        }
                        parts[i] = refId;
                    }
                    else {
                        parts[i] = `v:${String(item)}`;
                    }
                }
                return parts.join("|");
            };
            fn.hashTuple = (tuple, s = this.s, m = this.m, r = this.r) => {
                const key = this._.serializeTuple(tuple, s, m, r);
                const tupleCache = this._getMetadata(s, m, r)._tupleCache;
                const cached = tupleCache.get(key);
                if (cached) {
                    tupleCache.delete(key);
                    tupleCache.set(key, cached);
                    return cached;
                }
                let h1 = 0x811c9dc5;
                let h2 = 0x01000193;
                for (let i = 0; i < key.length; i++) {
                    const code = key.charCodeAt(i);
                    h1 = Math.imul(h1 ^ code, 16777619) >>> 0;
                    h2 = Math.imul(h2 ^ code, 2246822507) >>> 0;
                }
                const result = `tuple:${h1.toString(36)}-${h2.toString(36)}`;
                tupleCache.set(key, result);
                if (tupleCache.size > this.total_cache_size) {
                    const toRemove = Math.floor(tupleCache.size * 0.2);
                    const iter = tupleCache.keys();
                    for (let i = 0; i < toRemove; i++) {
                        const k = iter.next().value;
                        if (k !== undefined)
                            tupleCache.delete(k);
                    }
                }
                return result;
            };
            fn.writeResolved = (k, value, source, s = this.s, m = this.m, r = this.r, cause) => {
                const registry = this.hub.wr._signals;
                let atom = registry.get(k);
                if (!atom) {
                    const initial = typeof value === "function" ? undefined : value;
                    atom = this.create(k, initial, undefined, s, m, r);
                }
                if (!atom) {
                    throw new Error(`[Sauron] Cannot write unknown signal "${String(k)}"`);
                }
                return atom.write(value);
            };
            fn.recordCausal = (k, value, source, s = this.s, m = this.m, r = this.r, cause) => {
                const atom = this.hub.wr._signals.get(k);
                if (!atom)
                    return;
                atom.causal.push({ key: k, value, timestamp: Date.now(), source, cause });
                if (atom.causal.length > 100) {
                    atom.causal.shift();
                }
            };
            fn.bindComponentToSignal = (rk, s = this.s, m = this.m, r = this.r) => {
                const component = this.root.state.getCurrentComponent();
                if (!component)
                    return;
                const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
                if (component.hooks.has(hookKey))
                    return;
                const atom = this.create(rk, undefined, undefined, s, m, r);
                if (!atom)
                    return;
                const hook = { value: atom.read(), bindings: new Set() };
                component.hooks.set(hookKey, hook);
                const unsubSignal = atom.subscribeExternal(() => { hook.value = atom.read(); this.root.state.notifyHook(hookKey); });
                component.unmount.add(unsubSignal);
                if (component.render) {
                    const unsubRender = this.root.state.subscribeToHook(hookKey, () => component.render?.());
                    component.unmount.add(unsubRender);
                }
            };
            fn.bindComponentToSignal2 = (rk, s = this.s, m = this.m, r = this.r) => {
                const component = this.hub.state.getCurrentComponent();
                if (!component)
                    return;
                const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
                if (component.hooks.has(hookKey)) {
                    return;
                }
                component.hooks.set(hookKey, { value: undefined });
                const atom = this.create(rk, undefined, undefined, s, m, r);
                if (!atom) {
                    return;
                }
                const unsubSignal = atom.subscribeExternal(() => {
                    this.hub.state.notifyHook(hookKey);
                });
                component.unmount.add(unsubSignal);
                if (component.render) {
                    const unsubRender = this.hub.state.subscribeToHook(hookKey, () => component.render?.());
                    component.unmount.add(unsubRender);
                }
            };
            fn.resolveConflict = (conflict, previous, next, strategy = "latest") => {
                if (conflict?.resolve) {
                    return conflict.resolve(previous, next);
                }
                if (strategy === "fail") {
                    console.warn('[Sauron] conflict rejected by "fail" strategy');
                    return null;
                }
                if (strategy === "accumulate" && Array.isArray(previous) && Array.isArray(next)) {
                    return [...previous, ...next];
                }
                return next;
            };
            return fn;
        })();
        // #endregion
        //                                                                                                                        ASYNC
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        async = (() => {
            const fn = (k, fetcher, s = this.s, m = this.m, r = this.r, options) => {
                const reg = this.hub.wr._signals;
                let atom = reg.get(k);
                if (!atom) {
                    atom = this.create(k, options?.fallback, { triggerRender: false }, s, m, r);
                }
                const dataKey = `${String(k)}:data`;
                const loadingKey = `${String(k)}:loading`;
                const errorKey = `${String(k)}:error`;
                if (!this.has(dataKey, s, m, r))
                    this.create(dataKey, options?.fallback, undefined, s, m, r);
                if (!this.has(loadingKey, s, m, r))
                    this.create(loadingKey, false, undefined, s, m, r);
                if (!this.has(errorKey, s, m, r))
                    this.create(errorKey, null, undefined, s, m, r);
                const run = async () => {
                    const state = atom.async;
                    if (!state)
                        throw new Error(`[Sauron] Missing async state for "${String(k)}"`);
                    if (state.controller)
                        state.controller.abort();
                    const controller = new AbortController();
                    state.controller = controller;
                    this._.writeResolved(loadingKey, true, "write", s, m, r, String(k));
                    try {
                        const data = await fetcher(controller.signal);
                        if (!controller.signal.aborted) {
                            this._.writeResolved(dataKey, data, "write", s, m, r, String(k));
                            this._.writeResolved(errorKey, null, "write", s, m, r, String(k));
                        }
                        return data;
                    }
                    catch (error) {
                        if (!controller.signal.aborted) {
                            this._.writeResolved(errorKey, error, "write", s, m, r, String(k));
                            atom.lifecycle?.onError?.(error);
                        }
                        throw error;
                    }
                    finally {
                        if (!controller.signal.aborted) {
                            this._.writeResolved(loadingKey, false, "write", s, m, r, String(k));
                            state.controller = null;
                        }
                    }
                };
                atom.async = { fetcher: run, controller: null, retryCount: 0, staleValue: options?.fallback };
                void run();
                return { dataKey, loadingKey, errorKey };
            };
            fn.invalidate = (key, s = this.s, m = this.m, r = this.r) => {
                const reg = this.hub.wr._signals;
                const atom = reg.get(key);
                const state = atom?.async;
                if (!atom || !state)
                    return;
                const dataKey = `${String(key)}:data`;
                const loadingKey = `${String(key)}:loading`;
                const errorKey = `${String(key)}:error`;
                this._.writeResolved(loadingKey, true, "write", s, m, r, String(key));
                this._.writeResolved(errorKey, null, "write", s, m, r, String(key));
                void Promise.resolve(state.fetcher())
                    .then((data) => this._.writeResolved(dataKey, data, "write", s, m, r, String(key)))
                    .catch((error) => {
                    this._.writeResolved(errorKey, error, "write", s, m, r, String(key));
                    atom.lifecycle?.onError?.(error);
                });
            };
            fn.mutate = (key, optimistic, s = this.s, m = this.m, r = this.r) => {
                const reg = this.hub.wr._signals;
                const atom = reg.get(key);
                const state = atom?.async;
                if (!atom || !state)
                    return;
                const dataKey = `${String(key)}:data`;
                const dataAtom = reg.get(dataKey);
                if (!dataAtom)
                    throw new Error(`[Sauron] Missing async data signal "${dataKey}"`);
                const prior = dataAtom.peek();
                this._.writeResolved(dataKey, optimistic, "write", s, m, r, String(key));
                void Promise.resolve(state.fetcher()).catch(() => {
                    this._.writeResolved(dataKey, prior, "write", s, m, r, String(key));
                });
            };
            fn.read = (key, s = this.s, m = this.m, r = this.r) => {
                const reg = this.hub.wr._signals;
                const dataKey = `${String(key)}:data`;
                const loadingKey = `${String(key)}:loading`;
                const errorKey = `${String(key)}:error`;
                const errorAtom = reg.get(errorKey);
                const error = errorAtom?.peek();
                if (error)
                    throw error;
                const loadingAtom = reg.get(loadingKey);
                if (loadingAtom?.peek()) {
                    throw new Promise((resolve) => {
                        const unsub = loadingAtom.subscribe((change) => {
                            if (!change.current) {
                                unsub();
                                resolve();
                            }
                        });
                    });
                }
                const dataAtom = reg.get(dataKey);
                return dataAtom?.peek();
            };
            return fn;
        })();
        // #endregion
        //                                                                                                                   PERSISTANCE
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        persist = async (key, value, s = this.s, m = this.m, r = this.r, options) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        unpersist = async (key, s = this.s, m = this.m, r = this.r) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        transaction = (block) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        // #endregion
        //                                                                                                                     TIMELINE
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        _time_travel_reverting = false;
        time_travel = (() => {
            const fn = (key, channels = this.time_travel.defaultChannels()) => {
                return this.time_travel.resolveHistory(key, channels) ?? [];
            };
            fn.defaultChannels = () => ({
                r: this.r,
                m: this.m,
                s: "_TC_TIME_TRAVEL_",
            });
            fn.resolveChannels = (channels = this.time_travel.defaultChannels()) => ({
                ...this.time_travel.defaultChannels(),
                ...channels,
            });
            fn.resolveHistory = (key, channels = this.time_travel.defaultChannels()) => {
                const _history = this.hub.wr._historyMaul.get(key);
                return _history;
            };
            fn.replay = (key, value, channels, source) => {
                const { s, m, r } = this.time_travel.resolveChannels(channels);
                this._time_travel_reverting = true;
                try {
                    this.hub.state.store.set(key, value, { s: s, m: m, r: r, triggerRender: true });
                    return value;
                }
                finally {
                    this._time_travel_reverting = false;
                }
            };
            fn.graft = (drs) => {
                drs.time_travel = {
                    back: this.time_travel.back,
                    forward: this.time_travel.forward,
                    goto: this.time_travel.goto,
                    history: this.time_travel,
                };
                return drs.time_travel;
            };
            fn.back = (key, steps = 1, channels = this.time_travel.defaultChannels()) => {
                const history = this.time_travel.resolveHistory(key, channels);
                if (!history?.length || steps <= 0)
                    return undefined;
                const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
                const targetIndex = Math.max(0, index - steps);
                if (targetIndex === index)
                    return undefined;
                this.hub.wr._timeTravelMaul.set(key, targetIndex);
                for (let i = 0; i < history.length; i++) {
                    history[i].undone = i > targetIndex;
                }
                return this.time_travel.replay(key, history[targetIndex].prev, channels, "time-travel.back");
            };
            fn.forward = (key, steps = 1, channels = this.time_travel.defaultChannels()) => {
                const history = this.time_travel.resolveHistory(key, channels);
                if (!history?.length || steps <= 0)
                    return undefined;
                const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
                const targetIndex = Math.min(history.length - 1, index + steps);
                if (targetIndex === index)
                    return undefined;
                this.hub.wr._timeTravelMaul.set(key, targetIndex);
                for (let i = 0; i < history.length; i++) {
                    history[i].undone = i > targetIndex;
                }
                return this.time_travel.replay(key, history[targetIndex].next, channels, "time-travel.forward");
            };
            fn.goto = (key, sequence, channels = this.time_travel.defaultChannels()) => {
                const history = this.time_travel.resolveHistory(key, channels);
                if (!history?.length)
                    return undefined;
                const targetIndex = history.findIndex((entry) => entry.k === sequence);
                const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
                if (targetIndex < 0 || targetIndex === index)
                    return undefined;
                this.hub.wr._timeTravelMaul.set(key, targetIndex);
                for (let i = 0; i < history.length; i++) {
                    history[i].undone = i > targetIndex;
                }
                return this.time_travel.replay(key, history[targetIndex].goto, channels, "time-travel.goto");
            };
            return fn;
        })();
        recording = null;
        timeline = (() => {
            const fn = (key, s = this.s, m = this.m, r = this.r) => { };
            fn.manualReplay = (entries, options) => {
                const { from = 0, speed = 0, triggerRender = true } = options ?? {};
                return new Promise((resolve) => {
                    let index = from;
                    const step = () => {
                        if (index >= entries.length) {
                            resolve();
                            return;
                        }
                        const entry = entries[index++];
                        this._.writeResolved(entry.key, entry.value, entry.source ?? "replay", this.s, this.m, this.r, `replay:${index}`);
                        if (speed > 0) {
                            setTimeout(step, speed);
                        }
                        else {
                            step();
                        }
                    };
                    this.DARTH_MAUL_ENGINE.startBatch();
                    try {
                        step();
                    }
                    finally {
                        this.DARTH_MAUL_ENGINE.endBatch();
                    }
                    if (speed === 0) {
                        resolve();
                    }
                });
            };
            fn.manualBack = (key, s = this.s, m = this.m, r = this.r) => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.manualForward = (key, s = this.s, m = this.m, r = this.r) => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.manualRecord = (keys, s = this.s, m = this.m, r = this.r) => {
                /** USAGE
                    const stop = sauron.record();
                
                    // ...user reproduces the bug...
                    
                    const entries = stop();
                    
                    // Replay it exactly:
                    await sauron.replay(entries);
                    
                    // Or find where reality diverged:
                    sauron.bisect(entries);
                     */
                if (this.recording) {
                    throw new Error("[Sauron] A recording is already active");
                }
                const registry = this.hub.wr._signals;
                const targets = keys ?? [...registry.keys()];
                const entries = [];
                const unsubs = targets.map((key) => {
                    const atom = registry.get(key);
                    if (!atom)
                        return () => { };
                    return atom.subscribe((change) => {
                        entries.push({
                            key,
                            value: change.current,
                            previous: change.previous,
                            timestamp: Date.now(),
                            source: change.source,
                        });
                    });
                });
                this.recording = { entries, unsubs };
                return () => this.timeline.stopManualRecording();
            };
            fn.stopManualRecording = () => {
                if (!this.recording)
                    return [];
                for (const unsub of this.recording.unsubs) {
                    unsub();
                }
                const entries = this.recording.entries;
                this.recording = null;
                return entries;
            };
            return fn;
        })();
        snapshot = (() => {
            const fn = () => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.restore = (snapshot) => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.diff = (a, b) => {
                // REDO WITH NEW ATOM SOLUTION
            };
            fn.applyPatch = (patch) => {
                // REDO WITH NEW ATOM SOLUTION
            };
            return fn;
        })();
        getCausalChain(key) {
            // REDO WITH NEW ATOM SOLUTION
        }
        conflictPolicy(key, policy) {
            // REDO WITH NEW ATOM SOLUTION
        }
        declareContract(name, invariant, key, s = this.s, m = this.m, r = this.r, options) {
            // REDO WITH NEW ATOM SOLUTION
        }
        removeContract(name) {
            // REDO WITH NEW ATOM SOLUTION
        }
        createScope = (id) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        effectOnce = (keys, predicate, fn) => {
            // REDO WITH NEW ATOM SOLUTION
        };
        // #endregion
        //                                                                                                                   SRC / DEST
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        SRC_DEFAULTS = { sl: this.s, ms: this.m, rt: this.r };
        DST_DEFAULTS = { sl: this.s, ms: this.m, rt: this.r };
        filter = (srcKey, destKey, pred, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         
                 return this.subscribe<T>(
                     sk,
                     v => {
                         if (pred(v)) this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
                     },
                     src.s,
                     src.m,
                     src.r,
                 ); */
        };
        throttle = (srcKey, destKey, m, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         
                 let last = 0;
                 return this.subscribe(
                     sk,
                     v => {
                         const now = Date.now();
                         if (now - last >= m) {
                             last = now;
                             this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
                         }
                     },
                     src.s,
                     src.m,
                     src.r,
                 ); */
        };
        merge = (srcKeys, destKey, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**	const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
             const unsubs = srcKeys.map(k => {
                 const sk = this.resolveKey(k, src.s, src.m, src.r);
                 return this.subscribe(sk, v => this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r), src.s, src.m, src.r);
             });
             return () => unsubs.forEach(u => u()); */
        };
        zip = (srcKeys, destKey, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**
                 * const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
                const emitted = new Map<string, A>();
                const sks = srcKeys.map(k => this.resolveKey(k, src.s, src.m, src.r));
        
                const unsubs = sks.map(sk =>
                    this.subscribe(
                        sk,
                        v => {
                            emitted.set(sk, v);
                            if (sks.every(k => emitted.has(k))) {
                                this._writeResolved(
                                    dk,
                                    sks.map(k => emitted.get(k)),
                                    "derived",
                                    sk,
                                    dest.s,
                                    dest.m,
                                    dest.r,
                                );
                                emitted.clear();
                            }
                        },
                        src.s,
                        src.m,
                        src.r,
                    ),
                );
        
                return () => unsubs.forEach(u => u());
                 */
        };
        scan = (srcKey, destKey, reducer, seed, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**
                 * const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
        
                let acc = seed;
                this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);
        
                return this.subscribe<T>(
                    (v: T) => {
                        acc = reducer(acc, v);
                        this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);
                    },
                    sk,
                    src.s,
                    src.m,
                    src.r,
                );
                 */
        };
        switch = (srcKey, destKey, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         
                 let inner: (() => V) | null = null;
                 return this.subscribe(
                     sk,
                     (targetKey: A) => {
                         if (inner) inner();
                         const k = this.resolveKey(targetKey, src.s, src.m, src.r);
                         inner = this.subscribe(k, v => this._writeResolved(dk, v, "derived", k, dest.s, dest.m, dest.r), src.s, src.m, src.r);
                     },
                     src.s,
                     src.m,
                     src.r,
                 ); */
        };
        buffer = (srcKey, destKey, count, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         
                 const buf: A[] = [];
                 return this.subscribe(
                     sk,
                     v => {
                         buf.push(v);
                         if (buf.length >= count) {
                             this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
                             buf.length = 0;
                         }
                     },
                     src.s,
                     src.m,
                     src.r,
                 ); */
        };
        window = (srcKey, destKey, m, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         
                 const buf: A[] = [];
                 const iv = setInterval(() => {
                     if (buf.length) {
                         this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
                         buf.length = 0;
                     }
                 }, m);
         
                 const unsub = this.subscribe(sk, v => buf.push(v), src.s, src.m, src.r);
                 return () => {
                     unsub();
                     clearInterval(iv);
                 }; */
        };
        distinct = (srcKey, destKey, equals, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**	const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
             const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
        
             let last: T | UD;
             return this.subscribe<T>(
                 sk,
                 v => {
                     const same = last !== undefined && (equals ? equals(last, v) : Object.is(last, v));
                     if (!same) {
                         last = v;
                         this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
                     }
                 },
                 src.s,
                 src.m,
                 src.r,
             ); */
        };
        audit = (srcKey, destKey, m, src = this.SRC_DEFAULTS, dest = this.DST_DEFAULTS) => {
            /**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
                 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
         
                 let timer: RET<typeof setTimeout> | null = null;
                 let latest: A;
                 return this.subscribe(
                     sk,
                     v => {
                         latest = v;
                         if (!timer) {
                             timer = setTimeout(() => {
                                 this._writeResolved(dk, latest, "derived", sk, dest.s, dest.m, dest.r);
                                 timer = null;
                             }, m);
                         }
                     },
                     src.s,
                     src.m,
                     src.r,
                 ); */
        };
    })();
    r2x = new (class R2X {
        hub;
        signals;
        constructor(s) {
            this.hub = s.root;
            this.signals = s;
        }
    })(this);
    R21 = class R2X {
        hub;
        signals;
        constructor(s) {
            this.hub = s.root;
            this.signals = s;
        }
    };
}
// #endregion
//                                                                                                                         AVRO
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class FastAvroWriter {
    buf = Buffer.alloc(256);
    pos = 0;
    reset() { this.pos = 0; }
    ensure(length) {
        if (this.pos + length <= this.buf.length)
            return;
        let size = this.buf.length;
        while (size < this.pos + length)
            size *= 2;
        const next = Buffer.alloc(size);
        this.buf.copy(next, 0, 0, this.pos);
        this.buf = next;
    }
    byte(v) { this.ensure(1); this.buf[this.pos++] = v; }
    bytes(v) { this.ensure(v.length); this.buf.set(v, this.pos); this.pos += v.length; }
    long(value) {
        this.ensure(10);
        if (Math.abs(value) <= SAFE_ZIGZAG_LIMIT) {
            let n = value >= 0 ? value * 2 : (-value * 2 - 1);
            while (n >= 128) {
                this.buf[this.pos++] = (n % 128) | 0x80;
                n = Math.floor(n / 128);
            }
            this.buf[this.pos++] = n;
            return;
        }
        let n = BigInt(value);
        n = n >= 0n ? n << 1n : ((-n) << 1n) - 1n;
        while (n >= 0x80n) {
            this.buf[this.pos++] = Number(n & 0x7fn) | 0x80;
            n >>= 7n;
        }
        this.buf[this.pos++] = Number(n);
    }
    float(value) { this.ensure(4); this.buf.writeFloatLE(value, this.pos); this.pos += 4; }
    double(value) { this.ensure(8); this.buf.writeDoubleLE(value, this.pos); this.pos += 8; }
    string(v) {
        const len = Buffer.byteLength(v, "utf8");
        this.long(len);
        this.ensure(len);
        this.buf.write(v, this.pos, "utf8");
        this.pos += len;
    }
    finish() { return Buffer.from(this.buf.subarray(0, this.pos)); }
}
export class FastAvroReader {
    buf;
    pos = 0;
    constructor(buf) {
        this.buf = buf;
    }
    byte() { return this.buf[this.pos++]; }
    bytes(length) { const out = Buffer.from(this.buf.subarray(this.pos, this.pos + length)); this.pos += length; return out; }
    long() {
        let result = 0, shift = 0, b;
        do {
            b = this.buf[this.pos++];
            result += (b & 0x7f) * Math.pow(2, shift);
            shift += 7;
        } while (b & 0x80);
        return result % 2 === 0 ? result / 2 : -(result + 1) / 2;
    }
    float() { const n = this.buf.readFloatLE(this.pos); this.pos += 4; return n; }
    double() { const n = this.buf.readDoubleLE(this.pos); this.pos += 8; return n; }
    string() { const len = this.long(); const s = this.buf.toString("utf8", this.pos, this.pos + len); this.pos += len; return s; }
    done() { return this.pos === this.buf.length; }
}
const AVRO_ERROR_SCHEMA = {
    type: "record",
    name: "AVROError",
    fields: [
        { name: "type", type: "string" },
        { name: "error", type: "string" },
        { name: "id", type: "string" },
    ],
};
export class BINARY_AVRO_CODEGEN {
    names = Object.create(null);
    writeRoot;
    readRoot;
    writeError;
    readError;
    writer = new FastAvroWriter();
    errorSchema = AVRO_ERROR_SCHEMA;
    constructor(schema) {
        if (Array.isArray(schema))
            throw new Error("Avro: root schema cannot be a union");
        this.collect(schema);
        this.collect(this.errorSchema);
        // Everything below runs exactly ONCE, here, at construction — the generated
        // source text is the whole point: no schema-walking left at encode/decode time.
        const writeBody = this.genWrite(schema, "value").join("\n");
        const readBody = `return ${this.genRead(schema)};`;
        this.writeRoot = new Function("w", "value", writeBody);
        this.readRoot = new Function("r", readBody);
        const errWriteBody = this.genWrite(this.errorSchema, "value").join("\n");
        const errReadBody = `return ${this.genRead(this.errorSchema)};`;
        this.writeError = new Function("w", "value", errWriteBody);
        this.readError = new Function("r", errReadBody);
    }
    // ---- compile-time schema bookkeeping (never runs after construction) ----
    collect(schema) {
        if (Array.isArray(schema)) {
            for (const b of schema)
                this.collect(b);
            return;
        }
        if (typeof schema === "string")
            return;
        if (schema.name)
            this.names[schema.name] = schema;
        if (Array.isArray(schema.type))
            this.collect(schema.type);
        else if (typeof schema.type === "object")
            this.collect(schema.type);
        else if (typeof schema.type === "string" && ![
            "null", "boolean", "int", "long", "float", "double",
            "bytes", "string", "record", "array", "map", "enum", "fixed",
        ].includes(schema.type)) {
            if (!this.names[schema.type])
                throw new Error(`Avro: unknown named type ${schema.type}`);
        }
        if (schema.fields)
            for (const field of schema.fields)
                this.collect(field.type);
        if (schema.items)
            this.collect(schema.items);
        if (schema.values)
            this.collect(schema.values);
    }
    resolve(schema) {
        if (typeof schema === "string" && this.names[schema])
            return this.names[schema];
        return schema;
    }
    kindOf(schema) {
        schema = this.resolve(schema);
        if (typeof schema === "string")
            return schema;
        if (Array.isArray(schema))
            return "union";
        return typeof schema.type === "string" ? schema.type : this.kindOf(schema.type);
    }
    // ---- codegen: produces an inline boolean-expression STRING testing a union branch ----
    genMatch(schema, expr) {
        schema = this.resolve(schema);
        if (typeof schema === "string") {
            switch (schema) {
                case "null": return `(${expr} === null || ${expr} === undefined)`;
                case "boolean": return `typeof ${expr} === "boolean"`;
                case "int": return `typeof ${expr} === "number" && Number.isInteger(${expr})`;
                case "long":
                case "float":
                case "double": return `typeof ${expr} === "number"`;
                case "string": return `typeof ${expr} === "string"`;
                case "bytes": return `${expr} instanceof Uint8Array`;
                default: return "false";
            }
        }
        if (Array.isArray(schema)) {
            return schema.map(s => this.genMatch(s, expr)).join(" || ");
        }
        if (typeof schema.type !== "string")
            return this.genMatch(schema.type, expr);
        switch (schema.type) {
            case "fixed": return `${expr} instanceof Uint8Array && ${expr}.length === ${schema.size}`;
            case "enum": return `${JSON.stringify(schema.symbols)}.includes(${expr})`;
            case "array": return `Array.isArray(${expr})`;
            case "map":
            case "record": return `${expr} !== null && typeof ${expr} === "object" && !Array.isArray(${expr})`;
            default: return this.genMatch(schema.type, expr);
        }
    }
    // ---- codegen: produces an array of JS STATEMENT strings that write `expr` into `w` ----
    genWrite(schema, expr) {
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
                case "int":
                case "long": return [`w.long(${expr});`];
                case "float": return [`w.float(${expr});`];
                case "double": return [`w.double(${expr});`];
                case "string": return [`w.string(${expr});`];
                case "bytes": return [`w.long(${expr}.length); w.bytes(${expr});`];
                default: throw new Error(`Avro: unknown type ${schema}`);
            }
        }
        if (typeof schema.type !== "string")
            return this.genWrite(schema.type, expr);
        switch (schema.type) {
            case "null": return [];
            case "boolean": return [`w.byte(${expr} ? 1 : 0);`];
            case "int":
            case "long": return [`w.long(${expr});`];
            case "float": return [`w.float(${expr});`];
            case "double": return [`w.double(${expr});`];
            case "string": return [`w.string(${expr});`];
            case "bytes": return [`w.long(${expr}.length); w.bytes(${expr});`];
            case "fixed": {
                const size = schema.size;
                return [`if (!(${expr} instanceof Uint8Array) || ${expr}.length !== ${size}) throw new Error("Avro: invalid fixed value"); w.bytes(${expr});`];
            }
            case "enum": {
                const symbols = JSON.stringify(schema.symbols);
                return [`{ const i = ${symbols}.indexOf(${expr}); if (i < 0) throw new Error("Avro: invalid enum symbol " + ${expr}); w.long(i); }`];
            }
            case "record": {
                const lines = [];
                for (const f of schema.fields)
                    lines.push(...this.genWrite(f.type, `${expr}[${JSON.stringify(f.name)}]`));
                return lines;
            }
            case "array": {
                const inner = this.genWrite(schema.items, "arr[i]").join(" ");
                return [`{ const arr = ${expr}; if (arr.length) { w.long(arr.length); for (let i = 0; i < arr.length; i++) { ${inner} } } w.long(0); }`];
            }
            case "map": {
                const inner = this.genWrite(schema.values, "obj[k]").join(" ");
                return [`{ const obj = ${expr}; const keys = Object.keys(obj); if (keys.length) { w.long(keys.length); for (let i = 0; i < keys.length; i++) { const k = keys[i]; w.string(k); ${inner} } } w.long(0); }`];
            }
            default: throw new Error(`Avro: unsupported schema type ${schema.type}`);
        }
    }
    // ---- codegen: produces a single JS EXPRESSION string that reads one value from `r` ----
    genRead(schema) {
        schema = this.resolve(schema);
        if (Array.isArray(schema)) {
            const cases = schema.map((s, i) => `case ${i}: return ${this.genRead(s)};`).join(" ");
            return `(function(){ const i = r.long(); switch(i){ ${cases} default: throw new Error("Avro: invalid union index"); } })()`;
        }
        if (typeof schema === "string") {
            switch (schema) {
                case "null": return "null";
                case "boolean": return "(r.byte() === 1)";
                case "int":
                case "long": return "r.long()";
                case "float": return "r.float()";
                case "double": return "r.double()";
                case "string": return "r.string()";
                case "bytes": return "r.bytes(r.long())";
                default: throw new Error(`Avro: unknown type ${schema}`);
            }
        }
        if (typeof schema.type !== "string")
            return this.genRead(schema.type);
        switch (schema.type) {
            case "null": return "null";
            case "boolean": return "(r.byte() === 1)";
            case "int":
            case "long": return "r.long()";
            case "float": return "r.float()";
            case "double": return "r.double()";
            case "string": return "r.string()";
            case "bytes": return "r.bytes(r.long())";
            case "fixed": return `r.bytes(${schema.size})`;
            case "enum": return `(${JSON.stringify(schema.symbols)})[r.long()]`;
            case "record": {
                const assigns = schema.fields.map((f) => `o[${JSON.stringify(f.name)}] = ${this.genRead(f.type)};`).join(" ");
                return `(function(){ const o = {}; ${assigns} return o; })()`;
            }
            case "array": return `(function(){ const out = []; let c; while ((c = r.long()) !== 0) { for (let i = 0; i < c; i++) out.push(${this.genRead(schema.items)}); } return out; })()`;
            case "map": return `(function(){ const out = {}; let c; while ((c = r.long()) !== 0) { for (let i = 0; i < c; i++) { const k = r.string(); out[k] = ${this.genRead(schema.values)}; } } return out; })()`;
            default: throw new Error(`Avro: unsupported schema type ${schema.type}`);
        }
    }
    // ---- public API: identical contract to BINARY_AVRO_FAST / the original BINARY_AVRO ----
    encode_msg(msg) {
        if (msg instanceof Uint8Array)
            return msg;
        try {
            this.writer.reset();
            this.writer.long(0);
            this.writeRoot(this.writer, msg);
            return this.writer.finish();
        }
        catch (err) {
            console.log("[AVRO_PIPE] Failed to encode outbound packet:", err);
            this.writer.reset();
            this.writer.long(1);
            this.writeError(this.writer, {
                type: "error",
                error: err instanceof Error ? err.message : "Encoding failed",
                id: String(msg?.id ?? "unknown"),
            });
            return this.writer.finish();
        }
    }
    decode_msg(raw) {
        if (!(raw instanceof Uint8Array)) {
            console.error("[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
            return null;
        }
        try {
            const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength);
            const r = new FastAvroReader(buf);
            const branch = r.long();
            if (branch !== 0 && branch !== 1)
                throw new Error("Avro: invalid envelope branch");
            const value = branch === 0 ? this.readRoot(r) : this.readError(r);
            if (!r.done())
                throw new Error("Avro: trailing bytes");
            return value;
        }
        catch (err) {
            console.error("[AVRO_PIPE] Failed to decode inbound packet:", err);
            return null;
        }
    }
}
const SAFE_ZIGZAG_LIMIT = Number.MAX_SAFE_INTEGER / 2;
const THE_MESH_CACHE = new O2();
const SIG_BUS_SCHEMA = {
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
//                                                                                                                    PID TABLE 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class PidTable {
    #toPid = Object.create(null);
    #toKey = [];
    intern(key) {
        const existing = this.#toPid[key];
        if (existing !== undefined)
            return existing;
        const pid = this.#toKey.length;
        this.#toPid[key] = pid;
        this.#toKey.push(key);
        return pid;
    }
    keyOf(pid) { return this.#toKey[pid]; }
}
// #endregion
//                                                                                                                   MESH STORE 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class THE_MESH_STORE {
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    cache;
    DEVTOOLS_ACTIVE = false;
    THE_MESH_CACHE = new O2();
    _the_mesh_cache = new O2();
    _handlers = new O2();
    #subRoot = new O2();
    #metaSubRoot = new O2();
    _meta = new O2();
    #signalsSubRoot = new O2();
    _signals = new O2();
    #nakedSignalsSubRoot = new O2();
    _naked_signals = new O2();
    #contextSubRoot = new O2();
    _context = new O2();
    #atomsSubRoot = new O2();
    _atoms = new O2();
    #sauronSubRoot = new O2();
    _sauron = new O2();
    constructor(hub, cache = "mesh") {
        this.r = hub.r;
        this.m = hub.m;
        this.s = hub.s;
        this.k = hub.k;
        this.cache = cache;
        this.DEVTOOLS_ACTIVE = hub.DEVTOOLS_ACTIVE;
        this._handlers = this.getS('_handlers', this.m, this.r);
        const rL = (globalThis[this.r] ??= new O2());
        const mL = (rL[this.m] ??= new O2());
        this._handlers = mL['_handlers'] ??= new O2(); // this._globalThis
        this._meta = this.getS('_meta', this.m, this.r);
        this._signals = this.getS('_signals', this.m, this.r);
        this._naked_signals = this.getS('_naked_signals', this.m, this.r);
        this._context = this.getS('_context', this.m, this.r);
        this._atoms = this.getS('_atoms', this.m, this.r);
        this._sauron = this.getS('_sauron', this.m, this.r);
        this._the_mesh_cache = this.getS('_the_mesh_cache', this.m, this.r);
    }
    getS(s = this.s, m = this.m, r = this.r, c = "mesh") {
        const cache = this.cache === 'globalThis' || c === 'globalThis' ? globalThis : this.THE_MESH_CACHE;
        const rL = (cache[r] ??= new O2());
        const mL = (rL[m] ??= new O2());
        const sL = (mL[s] ??= new O2());
        return sL;
    }
    getM(m = this.m, r = this.r, c = "mesh") {
        const cache = this.cache === 'globalThis' || c === 'globalThis' ? globalThis : this.THE_MESH_CACHE;
        const rL = (cache[r] ??= new O2());
        const mL = (rL[m] ??= new O2());
        return mL;
    }
    getR(r = this.r, c = "mesh") {
        const cache = this.cache === 'globalThis' || c === 'globalThis' ? globalThis : this.THE_MESH_CACHE;
        const rL = (cache[r] ??= new O2());
        return rL;
    }
    handlers = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_handlers', m = this.m, r = this.r, c = "mesh") => {
                if (this.cache === 'globalThis' || c === 'globalThis') {
                    return globalThis[r]?.[m]?.[s]?.[k];
                }
                else {
                    return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
                }
            };
            fn.hotGT = (k) => { return this._handlers[k]; };
            fn.hotMesh = (k) => { return globalThis[this.s]?.[k]; };
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_handlers', m = this.m, r = this.r, c = "mesh") => {
                const cache = this.cache === 'globalThis' || c === 'globalThis' ? globalThis : this.THE_MESH_CACHE;
                const rL = (cache[r] ??= new O2());
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.handlers.emit(k, s, m, r, next);
                return this;
            };
            fn.hotGT = (v, k, s = '_handlers', m = this.m, r = this.r) => {
                const sL = this._handlers[k];
                const next = rv(v);
                sL[k] = next;
                this.handlers.emit(k, s, m, r, next);
                return next;
            };
            fn.hotMesh = (v, k, s = '_handlers', m = this.m, r = this.r) => {
                const sL = this._handlers[k];
                const next = rv(v);
                sL[k] = next;
                this.handlers.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_handlers', m = this.m, r = this.r, c = "mesh") => {
                return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
                if (this.cache === 'globalThis' || c === 'globalThis') {
                    return k in globalThis[r]?.[m]?.[s];
                }
                else {
                    return k in this.THE_MESH_CACHE[r]?.[m]?.[s];
                }
            };
            fn.hot = (k) => { return k in this._handlers; };
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_handlers', m = this.m, r = this.r, c = "mesh") => {
                const cache = this.cache === 'globalThis' || c === 'globalThis' ? globalThis : this.THE_MESH_CACHE;
                const rL = (cache[r] ??= new O2());
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.handlers.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hotMesh = (k, s = '_handlers', m = this.m, r = this.r, c = "mesh") => {
                if (!(k in this._handlers))
                    return false;
                delete this._handlers[k];
                this.handlers.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hotGT = (v, k, s = '_handlers', m = this.m, r = this.r) => {
                const sL = globalThis[s];
                const next = rv(v);
                sL[k] = next;
                this.handlers.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_handlers', m = this.m, r = this.r) => {
            const rL = (this.#subRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_handlers', m = this.m, r = this.r) => {
            this.#subRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_atoms', m = this.m, r = this.r) => {
            const set = this.#subRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    meta = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_meta', m = this.m, r = this.r) => { return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]; };
            fn.hot = (k) => this._meta[k];
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_meta', m = this.m, r = this.r) => {
                const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.meta.emit(k, s, m, r, next);
                return this;
            };
            fn.hot = (v, k, s = '_meta', m = this.m, r = this.r) => {
                const sL = this._meta;
                const next = rv(v);
                sL[k] = next;
                this.meta.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_meta', m = this.m, r = this.r) => { return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s]; };
            fn.hot = (k) => k in this._meta;
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_meta', m = this.m, r = this.r) => {
                const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.meta.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hot = (k, s = '_meta', m = this.m, r = this.r) => {
                if (!(k in this._meta))
                    return false;
                delete this._meta[k];
                this.meta.emit(k, s, m, r, undefined);
                return true;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_meta', m = this.m, r = this.r) => {
            const rL = (this.#metaSubRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_meta', m = this.m, r = this.r) => {
            this.#metaSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_atoms', m = this.m, r = this.r) => {
            const set = this.#metaSubRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    signals = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_signals', m = this.m, r = this.r) => {
                return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
            };
            fn.hot = (k) => this._signals[k];
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_signals', m = this.m, r = this.r) => {
                const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.signals.emit(k, s, m, r, next);
                return this;
            };
            fn.hot = (v, k, s = '_signals', m = this.m, r = this.r) => {
                const sL = this._signals;
                const next = rv(v);
                sL[k] = next;
                this.signals.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_signals', m = this.m, r = this.r) => {
                return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
            };
            fn.hot = (k) => k in this._signals;
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_signals', m = this.m, r = this.r) => {
                const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.signals.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hot = (k, s = '_signals', m = this.m, r = this.r) => {
                if (!(k in this._signals))
                    return false;
                delete this._signals[k];
                this.signals.emit(k, s, m, r, undefined);
                return true;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_signals', m = this.m, r = this.r) => {
            const rL = (this.#signalsSubRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_signals', m = this.m, r = this.r) => {
            this.#signalsSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_signals', m = this.m, r = this.r) => {
            const set = this.#signalsSubRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    naked_signals = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_naked_signals', m = this.m, r = this.r) => {
                return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
            };
            fn.hot = (k) => this._naked_signals[k];
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_naked_signals', m = this.m, r = this.r) => {
                const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.naked_signals.emit(k, s, m, r, next);
                return this;
            };
            fn.hot = (v, k, s = '_naked_signals', m = this.m, r = this.r) => {
                const sL = this._naked_signals;
                const next = rv(v);
                sL[k] = next;
                this.naked_signals.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_naked_signals', m = this.m, r = this.r) => {
                return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
            };
            fn.hot = (k) => k in this._naked_signals;
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_naked_signals', m = this.m, r = this.r) => {
                const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.naked_signals.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hot = (k, s = '_naked_signals', m = this.m, r = this.r) => {
                if (!(k in this._naked_signals))
                    return false;
                delete this._naked_signals[k];
                this.naked_signals.emit(k, s, m, r, undefined);
                return true;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_naked_signals', m = this.m, r = this.r) => {
            const rL = (this.#nakedSignalsSubRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_naked_signals', m = this.m, r = this.r) => {
            this.#nakedSignalsSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_atoms', m = this.m, r = this.r) => {
            const set = this.#nakedSignalsSubRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    context = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_context', m = this.m, r = this.r) => { return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]; };
            fn.hot = (k) => this._context[k];
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_context', m = this.m, r = this.r) => {
                const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.context.emit(k, s, m, r, next);
                return this;
            };
            fn.hot = (v, k, s = '_context', m = this.m, r = this.r) => {
                const sL = this._context;
                const next = rv(v);
                sL[k] = next;
                this.context.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_context', m = this.m, r = this.r) => {
                return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
            };
            fn.hot = (k) => k in this._context;
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_context', m = this.m, r = this.r) => {
                const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.context.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hot = (k, s = '_context', m = this.m, r = this.r) => {
                if (!(k in this._context))
                    return false;
                delete this._context[k];
                this.context.emit(k, s, m, r, undefined);
                return true;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_context', m = this.m, r = this.r) => {
            const rL = (this.#contextSubRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_context', m = this.m, r = this.r) => {
            this.#contextSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_atoms', m = this.m, r = this.r) => {
            const set = this.#contextSubRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    atoms = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_atoms', m = this.m, r = this.r) => { return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]; };
            fn.hot = (k) => this._atoms[k];
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_atoms', m = this.m, r = this.r) => {
                const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.atoms.emit(k, s, m, r, next);
                return this;
            };
            fn.hot = (v, k, s = '_atoms', m = this.m, r = this.r) => {
                const sL = this._atoms;
                const next = rv(v);
                sL[k] = next;
                this.atoms.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_atoms', m = this.m, r = this.r) => {
                return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
            };
            fn.hot = (k) => k in this._atoms;
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_atoms', m = this.m, r = this.r) => {
                const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.atoms.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hot = (k, s = '_atoms', m = this.m, r = this.r) => {
                if (!(k in this._atoms))
                    return false;
                delete this._atoms[k];
                this.atoms.emit(k, s, m, r, undefined);
                return true;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_atoms', m = this.m, r = this.r) => {
            const rL = (this.#atomsSubRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_atoms', m = this.m, r = this.r) => {
            this.#atomsSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_atoms', m = this.m, r = this.r) => {
            const set = this.#atomsSubRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    sauron = (() => {
        const fn = () => { };
        fn.get = (() => {
            const fn = (k, s = '_sauron', m = this.m, r = this.r) => {
                return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
            };
            fn.hot = (k) => this._sauron[k];
            return fn;
        })();
        fn.set = (() => {
            const fn = (v, k, s = '_sauron', m = this.m, r = this.r) => {
                const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const sL = (mL[s] ??= Object.create(null));
                const next = rv(v);
                sL[k] = next;
                this.sauron.emit(k, s, m, r, next);
                return this;
            };
            fn.hot = (v, k, s = '_sauron', m = this.m, r = this.r) => {
                const sL = this._sauron;
                const next = rv(v);
                sL[k] = next;
                this.sauron.emit(k, s, m, r, next);
                return next;
            };
            return fn;
        })();
        fn.has = (() => {
            const fn = (k, s = '_sauron', m = this.m, r = this.r) => {
                return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
            };
            fn.hot = (k) => k in this._sauron;
            return fn;
        })();
        fn.delete = (() => {
            const fn = (k, s = '_sauron', m = this.m, r = this.r) => {
                const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
                if (!sL || !(k in sL))
                    return false;
                delete sL[k];
                this.sauron.emit(k, s, m, r, undefined);
                return true;
            };
            fn.hot = (k, s = '_sauron', m = this.m, r = this.r) => {
                if (!(k in this._sauron))
                    return false;
                delete this._sauron[k];
                this.sauron.emit(k, s, m, r, undefined);
                return true;
            };
            return fn;
        })();
        fn.on = (fn, k, s = '_sauron', m = this.m, r = this.r) => {
            const rL = (this.#sauronSubRoot[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            const set = (sL[k] ??= new Set());
            set.add(fn);
            return () => {
                set.delete(fn);
                if (!set.size)
                    delete sL[k];
            };
        };
        fn.off = (fn, k, s = '_sauron', m = this.m, r = this.r) => {
            this.#sauronSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
        };
        fn.emit = (v, k, s = '_sauron', m = this.m, r = this.r) => {
            const set = this.#sauronSubRoot[r]?.[m]?.[s]?.[k];
            if (!set)
                return;
            for (const fn of set)
                fn(v);
        };
        return fn;
    })();
    u = undefined;
    uuuu = [this.u, this.u, this.u, this.u];
    UUUU = () => {
        return [this.s, this.m, this.r];
    };
    uuu = [this.u, this.u, this.u];
    UUU = () => {
        return [this.s, this.m, this.r];
    };
    uu = [this.u, this.u];
    UU = () => {
        return [this.m, this.r];
    };
    //                                                                                 MSG Q
    // #region ═════════════════════════════════════════════════════════════════════════════════════
    _cse_token;
    _init_sessionToken;
    _sessionToken;
    _sessionId;
    online = false;
    _auth_initialized = false;
    _http = null;
    _cross_boundary = null;
    _same_boundary = null;
    adapters = new O2();
    streams = new O2();
    QUEUE_KEY = 'staff:sends:offline:queue';
    pending = new O2();
    messageCount = new O2();
    seen = new Set();
    throttling = false;
    history = new O2();
    groups;
    transforms;
    middleware = [];
    globalMiddleware = {
        beforeSend: [],
        afterSend: [],
        beforeReceive: [],
        afterReceive: [],
        onError: [],
    };
    async invokeHandlers(msg) {
        const root = this.THE_MESH_CACHE;
        const rL = (root[msg.r] ??= Object.create(null));
        const mL = (rL[msg.m] ??= Object.create(null));
        const channel = mL[msg.s] ??= Object.create(null);
        const set = channel[msg.k]?.handler;
        if (!set || !set.size)
            return { success: false };
        const results = await Promise.all([...set].map((h) => Promise.resolve(h(msg.p)).catch(() => ({ success: false }))));
        if (results.length === 1)
            return results[0];
        const merged = {};
        const collisions = [];
        for (const result of results) {
            if (result === undefined || result === null)
                continue;
            if (typeof result !== "object") {
                merged["value"] = result;
                continue;
            }
            for (const [key, value] of Object.entries(result)) {
                if (key in merged)
                    collisions.push({ key, r: msg.r });
                merged[key] = value;
            }
        }
        if (collisions.length > 0)
            merged._cse_collisions = collisions;
        return Object.keys(merged).length === 0 ? { success: true } : merged;
    }
    schemas = new O2();
    adapter = null;
    IS_WIN = process.platform === "win32";
    PIPE_PATH = this.IS_WIN ? "\\\\.\\pipe\\sig-bus-perf" : "/tmp/sig-bus-perf.sock";
    waiters = new O2();
    adapterMetrics = new O2();
    backpressure = {};
    wildcardSubscriptions = [];
    deliveryHandlers = new Set();
    deadLetters = [];
    durableOffsets = new O2();
    durableHistory = new O2();
    permissions = new O2();
    routeRules = [];
    encryptionConfigs = [];
    compressionConfigs = [];
    tagSubs = new O2();
    tagsByChannel = new O2();
    queue = [];
    eventNames = [];
    listenerCount = 0;
    priorityWeight = {
        high: 3,
        normal: 2,
        low: 1,
    };
    defaultPriority = "normal";
    serialExecutor_ = class {
        chain = Promise.resolve();
        enqueue(fn) {
            const run = this.chain.then(fn);
            this.chain = run.then(() => undefined, () => undefined);
            return run;
        }
        get idle() {
            return this.chain;
        }
    };
    serial_() {
        return new this.serialExecutor_();
    }
    handlers_ = {
        getExact: (k, s = this.s, m = this.m, r = this.r) => {
            const handlers = this.getS(s, m, r).get(k);
            return handlers;
        },
        matching: (k, s = this.s, m = this.m, r = this.r) => {
            // 1. handlers whose CHANNEL matches
            const byChannel = new Set();
            for (const [ch, set] of this.getR(r)) {
                if (this.handlers_.matchesPattern(ch, r))
                    for (const h of set)
                        byChannel.add(h);
            }
            // 2. handlers whose CHANNEL and SUB match
            const bySub = new Set();
            for (const [sub, chMap] of this.getM(m, r)) {
                if (!this.handlers_.matchesPattern(sub, m))
                    continue;
                for (const [ch, set] of chMap) {
                    if (!this.handlers_.matchesPattern(ch, r))
                        continue;
                    for (const h of set)
                        bySub.add(h);
                }
            }
            // 3. handlers whose SUB and TRIP match
            const byTrip = new Set();
            for (const [trip, subMap] of this.getS(s, m, r)) {
                if (!this.handlers_.matchesPattern(trip, s))
                    continue;
                for (const [sub, set] of subMap) {
                    if (!this.handlers_.matchesPattern(sub, m))
                        continue;
                    for (const h of set)
                        byTrip.add(h);
                }
            }
            // 4. handlers whose TRIP and QUAD match
            const byQuad = new Set();
            for (const [quad, tripMap] of this.getS(s, m, r)?.[k]) {
                if (!this.handlers_.matchesPattern(quad, k))
                    continue;
                for (const [trip, set] of tripMap) {
                    if (!this.handlers_.matchesPattern(trip, s))
                        continue;
                    for (const h of set)
                        byQuad.add(h);
                }
            }
            // A handler matches only if it is in ALL four candidate sets.
            const out = [];
            for (const h of byChannel) {
                if (bySub.has(h) && byTrip.has(h) && byQuad.has(h))
                    out.push(h);
            }
            return out;
        },
        /**
         * 		list: (k?: string, s?: string, m?: string, r?: string): { r: string; count: number }[] => {
        if (r) {
            const handlers = this._map<string, Set<A>>(s ?? this.s, m ?? this.m, r);
            const set = handlers.get(k ?? this.k);
            return [{ r, count: set?.size ?? 0 }];
        }
        // no r given — enumerate via the accessor across the current scope
        const out: { r: string; count: number }[] = [];
        for (const [ch] of this.asmap<string, Set<A>>(this.s, this.m, this.r)) {
            const set = this._map<string, Set<A>>(this.s, this.m, ch).get(this.k);
            out.push({ r: ch, count: set?.size ?? 0 });
        }
        return out;
        },
        */
        invoke: async (msg) => {
            const handlers = this.handlers_.matching(msg.k ? msg.k : "[NO_ADP_FOUND]", msg.s, msg.m, msg.r);
            if (handlers.length === 0)
                return { success: false };
            console.log(handlers, "emitter.invokeHandlers.handlers");
            // transforms (incoming)
            //  msg = await this.applyTransforms(msg, 'incoming')
            const results = await Promise.all(handlers.map((h) => Promise.resolve(h(msg.p)).catch(() => ({ success: false }))));
            console.log(results, "emitter.invokeHandlers.results");
            if (results.length === 1)
                return results[0];
            const merged = {};
            const collisions = [];
            for (const result of results) {
                if (result === undefined || result === null)
                    continue;
                if (typeof result !== "object") {
                    merged["value"] = result;
                    continue;
                }
                const r = result;
                for (const [key, value] of Object.entries(r)) {
                    if (key in merged)
                        collisions.push({ key, r: msg.r });
                    merged[key] = value;
                }
            }
            if (collisions.length > 0) {
                merged._cse_collisions = collisions;
            }
            console.log(merged, "emitter.invokeHandlers.merged");
            return Object.keys(merged).length === 0 ? { success: true } : merged;
        },
        matchesPattern: (pattern, r) => {
            if (pattern === "*")
                return true;
            if (pattern === r)
                return true;
            const regex = new RegExp("^" + pattern.replace(/\*/g, "[^:]+(?::[^:]+)*") + "$");
            return regex.test(r);
        },
    };
    schemas_ = {
        getAll: () => {
            return O2.fromEntries(this.schemas);
        },
    };
    groups_ = {
        create: (name, channels = [], options) => {
            this.groups.set(name, {
                channels: new Set(channels),
                enabled: options?.enabled ?? true,
                middleware: options?.middleware ? [options.middleware] : [],
            });
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[HOOK_SYSTEM][SIGNALS][GROUPS][CREATE]`, { msg: "group_created", name }, this.log.t.error);
        },
        delete: (name) => {
            this.groups.delete(name);
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[HOOK_SYSTEM][SIGNALS][GROUPS][DELETE]`, { msg: "group_deleted", name }, this.log.t.error);
        },
        enable: (name) => {
            const g = this.groups.get(name);
            if (!g)
                return;
            g.enabled = true;
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[HOOK_SYSTEM][SIGNALS][GROUPS][ENABLE]`, { msg: "group_enabled", name }, this.log.t.error);
        },
        disable: (name) => {
            const g = this.groups.get(name);
            if (!g)
                return;
            g.enabled = false;
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[HOOK_SYSTEM][SIGNALS][GROUPS][DISABLE]`, { msg: "group_disabled", name }, this.log.t.error);
        },
        addTo: (name, r) => {
            const g = this.groups.get(name);
            if (!g)
                return;
            g.channels.add(r);
        },
        removeFrom: (name, r) => {
            const g = this.groups.get(name);
            if (!g)
                return;
            g.channels.delete(r);
        },
        use: (name, fn) => {
            const g = this.groups.get(name);
            if (!g)
                return;
            g.middleware.push(fn);
        },
        getAll: () => {
            const out = {};
            for (const [name, g] of this.groups) {
                out[name] = { channels: [...g.channels], enabled: g.enabled, middlewareCount: g.middleware.length };
            }
            return out;
        },
    };
    transforms_ = {
        add: (channelPattern, transform) => {
            const list = this.transforms.get(channelPattern) ?? [];
            list.push(transform);
            this.transforms.set(channelPattern, list);
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[✘][HOOK_SYSTEM][CHECK_DYNAMIC]`, {
                    channelPattern,
                    msg: "transform_added",
                }, this.log.t.error); //this.emitDevtoolsEvent('transform_added', channelPattern)
        },
        getAll: () => {
            return Object.fromEntries(this.transforms);
        },
        apply: async (msg, dir) => {
            for (const [pattern, list] of this.transforms.entries()) {
                if (!this.handlers_.matchesPattern(pattern, msg.r))
                    continue;
                for (const t of list) {
                    const fn = dir === "incoming" ? t.incoming : t.outgoing;
                    if (!fn)
                        continue;
                    msg = await Promise.resolve(fn(msg));
                }
            }
            return msg;
        },
        remove: (channelPattern) => {
            this.transforms.delete(channelPattern);
        },
    };
    encryption_ = {
        getConfigs: () => {
            return [...this.encryptionConfigs];
        },
        configure: (config) => {
            this.encryptionConfigs.push(config);
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[SIGNALS][ENCRYPTION][CONFIGURE]`, { msg: "encryption_configured", config }, this.log.t.deep);
        },
        unwrap: async (msg) => {
            if (!msg?.sessionId || !msg?.token)
                return null;
            const expected = this.encryption_.computeHmacSha256(this._cse_token, msg.sessionId);
            if (msg.token !== expected)
                return null;
            return msg.p;
        },
        wrap: (p) => {
            return {
                sessionId: this._sessionId,
                token: this._sessionToken,
                p,
            };
        },
        init: async () => {
            // no dotenv import — read the env directly
            const sk = (typeof process !== "undefined" && process.env?.ENC_TOKEN) || undefined;
            if (sk && sk.length >= 5) {
                // seed the session once, then derive a stable token from it
                this._cse_token = sk;
                this._sessionId = this.crypto.to_hex(this.crypto.random_bytes(16));
                // computeHmacSha256 is sync-hex on Node; if only WebCrypto is available
                // the async path fills _hmacCache and the sync read picks it up next tick.
                this._sessionToken = this.encryption_.computeHmacSha256(this._cse_token, this._sessionId);
                this._init_sessionToken = !!this._sessionToken;
            }
            else {
                if (this.DEVTOOLS_ACTIVE)
                    this.log.log(`[SIGNALS][ENCRYPTION][CONFIGURE]`, { msg: "[!ENC_TOKEN - SIGNALS IS STILL AVAILABLE, IF YOU WOULD LIKE TO ADD A SECURITY LAYER THEN ADD ENC_TOKEN TO .ENV]" }, this.log.t.deep);
                this._auth_initialized = false;
            }
        },
        computeHmacSha256: (secret, data) => {
            if (!this.crypto.ready())
                return "";
            // try the sync path first (Node); catch the "no Node crypto" throw
            let syncResult = "";
            try {
                syncResult = this.crypto.hash_hmac_hex(secret, data);
            }
            catch {
                syncResult = "";
            }
            if (syncResult)
                return syncResult;
            // WebCrypto-only: fire async, cache, return "" this tick.
            // cache key must match whatever the sync path will look up next tick.
            const cacheKey = this.crypto.to_hex(this.crypto.to_utf8_bytes(secret)) + ":" + this.crypto.to_hex(this.crypto.to_utf8_bytes(data));
            this.crypto.hash_hmac_hex_async(secret, data).then((hex) => {
                this.crypto._hmacCache.set(cacheKey, hex);
            });
            return "";
        },
    };
    crypto = new (class DISRUPTOR_CRYPTO {
        ser(value) {
            if (value instanceof Map) {
                return JSON.stringify({ __type: "Map", value: Array.from(value.entries()).map(([key, val]) => [this.ser(key), this.ser(val)]) });
            }
            if (value instanceof Set) {
                return JSON.stringify({ __type: "Set", value: Array.from(value.values()).map((value) => this.ser(value)) });
            }
            return typeof value === "string" ? value : JSON.stringify(value);
        }
        deser(raw) {
            if (raw === null)
                return null;
            try {
                const parsed = JSON.parse(raw);
                if (parsed?.__type === "Map") {
                    return new Map(parsed.value.map(([key, value]) => [this.deser(key), this.deser(value)]));
                }
                if (parsed?.__type === "Set") {
                    return new Set(parsed.value.map((value) => this.deser(value)));
                }
                return parsed;
            }
            catch {
                return raw;
            }
        }
        hasNode = typeof process !== "undefined" && !!process.versions?.node;
        _buf(bytes) {
            const out = new Uint8Array(new ArrayBuffer(bytes.byteLength));
            out.set(bytes);
            return out;
        }
        _hasSubtle = typeof globalThis.crypto !== "undefined" && typeof globalThis.crypto.subtle !== "undefined" && typeof globalThis.crypto.getRandomValues === "function";
        _nodeCrypto = null;
        _getNodeCrypto() {
            if (this._nodeCrypto)
                return this._nodeCrypto;
            if (!this.hasNode)
                return null;
            try {
                this._nodeCrypto = require("node:crypto");
            }
            catch {
                this._nodeCrypto = null;
            }
            return this._nodeCrypto;
        }
        constructor() { }
        // ── random bytes ────────────────────────────────────────────────────────
        random_bytes(size = 32) {
            if (this._hasSubtle)
                return globalThis.crypto.getRandomValues(new Uint8Array(size));
            const nc = this._getNodeCrypto();
            if (nc)
                return new Uint8Array(nc.randomBytes(size));
            // last resort — NOT cryptographically secure, but the bus will refuse
            // to init auth without real randomness, so this never protects anything.
            const out = new Uint8Array(size);
            for (let i = 0; i < size; i++)
                out[i] = (Math.random() * 256) | 0;
            return out;
        }
        // ── HMAC-SHA256 ─────────────────────────────────────────────────────────
        // Returned as hex so it matches the existing computeHmacSha256 contract.
        // Async version — used when WebCrypto is the only option.
        async hash_hmac_hex_async(secret, data) {
            const keyBytes = typeof secret === "string" ? this.to_utf8_bytes(secret) : secret;
            const dataBytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
            if (this._hasSubtle) {
                const key = await globalThis.crypto.subtle.importKey("raw", this._buf(keyBytes), // ← wrap
                { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
                const sig = await globalThis.crypto.subtle.sign("HMAC", key, this._buf(dataBytes));
                return this.to_hex(new Uint8Array(sig));
            }
            const nc = this._getNodeCrypto();
            if (nc) {
                const h = nc.createHmac("sha256", Buffer.from(keyBytes));
                h.update(Buffer.from(dataBytes));
                return h.digest("hex");
            }
            return "";
        }
        // Cache used by the sync path when only WebCrypto is available.
        // Populated by hash_hmac_hex_async's callers, read by hash_hmac_hex.
        _hmacCache = new Map();
        // ── SHA-256 ─────────────────────────────────────────────────────────────
        async hash_sha256_hex(data) {
            const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
            if (this._hasSubtle) {
                const digest = await globalThis.crypto.subtle.digest("SHA-256", this._buf(bytes));
                return this.to_hex(new Uint8Array(digest));
            }
            const nc = this._getNodeCrypto();
            if (nc) {
                const h = nc.createHash("sha256");
                h.update(Buffer.from(bytes));
                return h.digest("hex");
            }
            return "";
        }
        // ── HKDF (derive a session key from a shared secret) ────────────────────
        async derive_hkdf(ikm, salt, info, length = 32) {
            if (this._hasSubtle) {
                const key = await globalThis.crypto.subtle.importKey("raw", this._buf(ikm), // ← wrap
                "HKDF", false, ["deriveBits"]);
                const bits = await globalThis.crypto.subtle.deriveBits({
                    name: "HKDF",
                    hash: "SHA-256",
                    salt: this._buf(salt), // ← wrap
                    info: this._buf(info), // ← wrap
                }, key, length * 8);
                return new Uint8Array(bits);
            }
            const nc = this._getNodeCrypto();
            if (nc) {
                return new Uint8Array(nc.hkdfSync("sha256", Buffer.from(ikm), Buffer.from(salt), Buffer.from(info), length));
            }
            return new Uint8Array(length);
        }
        // ── AES-GCM (the bus's symmetric p encryption) ────────────────────
        async encrypt_aes_gcm(key, iv, data) {
            if (this._hasSubtle) {
                const k = await globalThis.crypto.subtle.importKey("raw", this._buf(key), // ← wrap
                "AES-GCM", false, ["encrypt"]);
                const ct = await globalThis.crypto.subtle.encrypt({ name: "AES-GCM", iv: this._buf(iv) }, // ← wrap
                k, this._buf(data));
                return new Uint8Array(ct);
            }
            const nc = this._getNodeCrypto();
            if (nc) {
                const cipher = nc.createCipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
                const ct = Buffer.concat([cipher.update(Buffer.from(data)), cipher.final()]);
                const tag = cipher.getAuthTag();
                // append tag, matching WebCrypto's output format
                return new Uint8Array(Buffer.concat([ct, tag]));
            }
            return data;
        }
        async decrypt_aes_gcm(key, iv, ct) {
            if (this._hasSubtle) {
                const k = await globalThis.crypto.subtle.importKey("raw", this._buf(key), // ← wrap
                "AES-GCM", false, ["decrypt"]);
                const pt = await globalThis.crypto.subtle.decrypt({ name: "AES-GCM", iv: this._buf(iv) }, // ← wrap
                k, this._buf(ct));
                return new Uint8Array(pt);
            }
            const nc = this._getNodeCrypto();
            if (nc) {
                // split off the 16-byte tag from the end
                const tag = ct.slice(ct.length - 16);
                const body = ct.slice(0, ct.length - 16);
                const decipher = nc.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
                decipher.setAuthTag(Buffer.from(tag));
                const pt = Buffer.concat([decipher.update(Buffer.from(body)), decipher.final()]);
                return new Uint8Array(pt);
            }
            return ct;
        }
        // ── Encoding ────────────────────────────────────────────────────────────
        to_utf8_bytes(str) {
            if (typeof TextEncoder !== "undefined")
                return new TextEncoder().encode(str);
            const nc = this._getNodeCrypto();
            if (nc)
                return new Uint8Array(Buffer.from(str, "utf8"));
            // last resort
            const out = new Uint8Array(str.length);
            for (let i = 0; i < str.length; i++)
                out[i] = str.charCodeAt(i) & 0xff;
            return out;
        }
        to_hex(data) {
            if (typeof Buffer !== "undefined")
                return Buffer.from(data).toString("hex");
            let out = "";
            for (let i = 0; i < data.length; i++)
                out += data[i].toString(16).padStart(2, "0");
            return out;
        }
        from_hex(hex) {
            if (typeof Buffer !== "undefined")
                return new Uint8Array(Buffer.from(hex, "hex"));
            const out = new Uint8Array(hex.length / 2);
            for (let i = 0; i < hex.length; i += 2)
                out[i / 2] = parseInt(hex.slice(i, i + 2), 16);
            return out;
        }
        to_base64(data) {
            if (typeof Buffer !== "undefined")
                return Buffer.from(data).toString("base64");
            let bin = "";
            for (let i = 0; i < data.length; i++)
                bin += String.fromCharCode(data[i]);
            return btoa(bin);
        }
        from_base64(str) {
            if (typeof Buffer !== "undefined")
                return new Uint8Array(Buffer.from(str, "base64"));
            const bin = atob(str);
            const out = new Uint8Array(bin.length);
            for (let i = 0; i < bin.length; i++)
                out[i] = bin.charCodeAt(i);
            return out;
        }
        // ── Constants ───────────────────────────────────────────────────────────
        ready() {
            return this._hasSubtle || this._getNodeCrypto() !== null;
        }
        secure_equal(a, b) {
            let diff = a.length ^ b.length;
            const len = Math.max(a.length, b.length);
            for (let i = 0; i < len; i++)
                diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
            return diff === 0;
        }
        hash_hmac_hex(secret, data) {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("Synchronous HMAC requires Node crypto");
            const key = typeof secret === "string" ? this.to_utf8_bytes(secret) : secret;
            const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
            return nc.createHmac("sha256", Buffer.from(key)).update(Buffer.from(bytes)).digest("hex");
        }
        hash_hmac(secret, data) {
            return this.hash_hmac_hex(secret, data);
        }
        hash_sha256(data) {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("Synchronous hashing requires Node crypto");
            const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
            return nc.createHash("sha256").update(Buffer.from(bytes)).digest("hex");
        }
        derive_key(password, salt, iterations = 310_000, length = 32) {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("Synchronous PBKDF2 requires Node crypto");
            return new Uint8Array(nc.pbkdf2Sync(Buffer.from(this.to_utf8_bytes(password)), Buffer.from(salt), iterations, length, "sha256"));
        }
        symmetric_encrypt(p, key) {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("Synchronous encryption requires Node crypto");
            if (key.length !== 32)
                throw new Error("AES-256-GCM requires a 32-byte key");
            const nonce = nc.randomBytes(12);
            const cipher = nc.createCipheriv("aes-256-gcm", Buffer.from(key), nonce);
            const ciphertext = Buffer.concat([cipher.update(p, "utf8"), cipher.final(), cipher.getAuthTag()]);
            return {
                ciphertext: this.to_base64(new Uint8Array(ciphertext)),
                nonce: this.to_base64(new Uint8Array(nonce)),
            };
        }
        symmetric_decrypt(p, key) {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("Synchronous decryption requires Node crypto");
            if (key.length !== 32)
                throw new Error("AES-256-GCM requires a 32-byte key");
            const ciphertext = this.from_base64(p.ciphertext);
            const nonce = this.from_base64(p.nonce);
            if (nonce.length !== 12 || ciphertext.length < 16)
                throw new Error("Invalid AES-GCM payload");
            const body = ciphertext.slice(0, -16);
            const tag = ciphertext.slice(-16);
            const decipher = nc.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(nonce));
            decipher.setAuthTag(Buffer.from(tag));
            return Buffer.concat([decipher.update(Buffer.from(body)), decipher.final()]).toString("utf8");
        }
        generate_key_pair(_type) {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("Synchronous key generation requires Node crypto");
            const pair = nc.generateKeyPairSync("ed25519");
            return {
                publicKey: new Uint8Array(pair.publicKey.export({ type: "spki", format: "der" })),
                secretKey: new Uint8Array(pair.privateKey.export({ type: "pkcs8", format: "der" })),
            };
        }
        pqc_generate_KEM_keys() {
            const nc = this._getNodeCrypto();
            if (!nc)
                throw new Error("PQC key generation requires a supported Node crypto provider");
            const pair = nc.generateKeyPairSync("ml-kem-768");
            return {
                publicKey: new Uint8Array(pair.publicKey.export({ type: "spki", format: "der" })),
                secretKey: new Uint8Array(pair.privateKey.export({ type: "pkcs8", format: "der" })),
                algorithm: "ML-KEM-768",
            };
        }
    })();
    compression_ = {
        getConfigs: () => {
            return [...this.compressionConfigs];
        },
        configure: (config) => {
            this.compressionConfigs.push(config);
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[SIGNALS][COMPRESSION][CONFIGURE]`, { msg: "compression_configured", config }, this.log.t.deep);
        },
    };
    checkpoints_ = {
        getAll: () => {
            return O2.fromEntries(this.durableOffsets);
        },
    };
    wildcards_ = {
        getHandlers: (rt, ms) => {
            const handlers = [];
            const fullTopic = `${rt}.${ms}`;
            for (const { pattern, handler } of this.wildcardSubscriptions) {
                if (this.BACK_END.matchesPattern(fullTopic, pattern)) {
                    handlers.push(handler);
                }
            }
            return handlers;
        },
        matches: (topic, pattern) => {
            const topicParts = topic.split(".");
            const patternParts = pattern.split(".");
            // If lengths don't match, return false
            if (topicParts.length !== patternParts.length)
                return false;
            // Check each part
            for (let i = 0; i < topicParts.length; i++) {
                if (patternParts[i] === "*")
                    continue; // Wildcard matches anything
                if (topicParts[i] !== patternParts[i])
                    return false;
            }
            return true;
        },
    };
    durable_ = {
        getHistorySizes: () => {
            const out = {};
            for (const [ch, hist] of this.durableHistory)
                out[ch] = hist.length;
            return out;
        },
    };
    tags_ = {
        sub: (tag, handler) => {
            const set = this.tagSubs.get(tag) ?? new Set();
            set.add(handler);
            this.tagSubs.set(tag, set);
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[SIGNALS][TAGS][SUB]`, { msg: "tag_subscribed", tag }, this.log.t.deep);
            return () => set.delete(handler);
        },
        list: () => {
            return [...this.tagSubs.keys()];
        },
        listChannels: (tag) => {
            const out = [];
            for (const [ch, tags] of this.tagsByChannel.entries()) {
                if (tags.has(tag))
                    out.push(ch);
            }
            return out;
        },
    };
    routes_ = {
        addRule: (rule) => {
            this.routeRules.push(rule);
            if (this.DEVTOOLS_ACTIVE)
                this.log.log(`[SIGNALS][ROUTES][ADD_RULE]`, { msg: "route_added", rule }, this.log.t.deep);
        },
        removeRule: (name) => {
            this.routeRules.filter((r) => r.name !== name);
        },
        listRules: () => {
            return [...this.routeRules];
        },
    };
    msgs_ = {
        getCount: (r) => {
            return this.messageCount.get(r) ?? 0;
        },
    };
    middleware_ = {
        list: () => {
            return [this.middleware.length];
        },
    };
    channels_ = {
        // list: (): string[] => { return [...this._channels(this.s, this.m, this.r).keys()]; },
        setSchema: (schema, r) => {
            this.schemas.set(r, schema);
        },
        setPermissions: (perms, r) => {
            this.permissions.set(r, perms);
        },
        getSchema: (r) => {
            return this.schemas.get(r) ?? null;
        },
        getPermissions: (r) => {
            return this.permissions
                .get(r) ?? null;
        },
    };
    defaultPriority_ = {
        set: (priority) => {
            this.defaultPriority = priority;
        },
        get: () => {
            return this.defaultPriority;
        },
    };
    BACK_END = (() => {
        const fn = () => { };
        fn.run = {
            beforeSend: async (msg) => {
                for (const fn of this.globalMiddleware.beforeSend) {
                    const r = await fn(msg);
                    if (r === false)
                        return false;
                }
                return true;
            },
            afterSend: async (msg, result) => {
                for (const fn of this.globalMiddleware.afterSend) {
                    await fn(msg, result);
                }
            },
            beforeReceive: async (msg) => {
                for (const fn of this.globalMiddleware.beforeReceive) {
                    const r = await fn(msg);
                    if (r === false)
                        return false;
                }
                return true;
            },
            afterReceive: async (msg, result) => {
                for (const fn of this.globalMiddleware.afterReceive) {
                    await fn(msg, result);
                }
            },
            onError: async (err, msg) => {
                for (const fn of this.globalMiddleware.onError) {
                    await fn(err, msg);
                }
                //  log(`[THE_WIZARD][STAFF][SEND_VIA_ADAPTER]`, {   msg: msg,  transport: `[${msg ? msg.boundary : 'undefined'}]`,  }, t.deep)//this.drop.t.error
            },
            middleware: (msg) => {
                for (const fn of this.middleware) {
                    if (fn(msg) === false)
                        return false;
                }
                // group middleware
                for (const [, g] of this.groups) {
                    if (!g.enabled)
                        continue;
                    if (![...g.channels].some((ch) => this.handlers_.matchesPattern(ch, msg.r)))
                        continue;
                    for (const fn of g.middleware) {
                        if (fn(msg) === false)
                            return false;
                    }
                }
                return true;
            },
        };
        fn.clean_string = (r) => {
            return r.replace(/[\[\]]/g, "");
        };
        fn.getActiveStreams = () => {
            return [...this.streams.keys()];
        };
        fn.matchesPattern = (pattern, r) => {
            if (pattern === "*")
                return true;
            if (pattern === r)
                return true;
            const regex = new RegExp("^" + pattern.replace(/\*/g, "[^:]+(?::[^:]+)*") + "$");
            return regex.test(r);
        };
        fn.SHOULD_BACKPRESSURE = () => {
            if (!this.backpressure.maxQueueSize)
                return false;
            if (this.queue.length < this.backpressure.maxQueueSize)
                return false;
            if (this.throttling)
                return true;
            this.throttling = true;
            setTimeout(() => {
                this.throttling = false;
                this.DISPATCH.drain().catch(console.error);
            }, this.backpressure.throttleMs ?? 100);
            return true;
        };
        fn.getSubTopology = (s = this.s, m = this.m, r = this.r) => {
            const out = [];
            const channels = this.getR(r);
            for (const [ch, set] of channels) {
                const subMap = this.getM(m, r);
                out.push({
                    r: ch,
                    handlerCount: set.size,
                    sub_channels: [...subMap.entries()].map(([sc, hs]) => ({
                        subChannel: sc,
                        handlerCount: hs.size
                    }))
                });
            }
            return out;
        };
        return fn;
    })();
    binary = new BINARY_AVRO_CODEGEN(SIG_BUS_SCHEMA);
    DISPATCH = (() => {
        const codecRegistry = new Map();
        function getCodec(schemaName, schema) {
            let codec = codecRegistry.get(schemaName);
            if (!codec) {
                codec = new BINARY_AVRO_CODEGEN(schema); // only runs once per distinct schema, ever
                codecRegistry.set(schemaName, codec);
            }
            return codec;
        }
        const fn = async (raw, adapter) => {
            let msg = raw;
            //                                    DECODE DE-BINARY
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            // Decode first — mirrors encode running last on the send side.
            if (msg instanceof Uint8Array || (typeof Buffer !== "undefined" && Buffer.isBuffer(msg))) {
                msg = this.binary.decode_msg(msg);
                if (!msg)
                    return { success: false, decodeFailed: true };
            }
            // #endregion
            //                                    SECURE UNWRAP
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            if (this._auth_initialized) {
                msg = await this.encryption.unwrap(msg);
                if (!msg)
                    return { success: false, unauthorized: true };
            }
            // #endregion
            //                                    CHANNEL MAPPING
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            msg.r = msg.r ?? this.r;
            msg.m = msg.m ?? this.m;
            msg.s = msg.s ?? this.s;
            msg.k = msg.k;
            console.log(msg, "emitter.dispatch.msg");
            // #endregion
            //                                    TRACE METRICS
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            if (this.DEVTOOLS_ACTIVE)
                this.metrics.traceStage(msg.id, msg.r, "dispatch"); // _metrics
            // #endregion
            //                                    REPLY
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            if (msg.r && msg.r.startsWith(":reply:")) {
                const originalMsgId = msg.r.replace(":reply:", "");
                const pendingTx = this.pending.get(originalMsgId);
                if (pendingTx) {
                    clearTimeout(pendingTx.timer);
                    this.pending.delete(originalMsgId);
                    pendingTx.resolve(msg.p);
                    return { success: true, isReply: true };
                }
            }
            // #endregion
            //                                    TTL
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            if (this.seen.has(msg.id))
                return { success: true };
            if (msg.ttl && Date.now() - msg.ts > msg.ttl) {
                if (this.DEVTOOLS_ACTIVE)
                    this.log.log(`[SIGNALS][DISPATCH][TTL]`, { msg: "ttl_expired", p: msg }, this.log.t.deep);
                return { success: false, dropped: true };
            }
            if (this.seen.size > 10000)
                this.seen.clear();
            this.seen.add(msg.id);
            // #endregion
            //                                    MSG HISTORY
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            const h = this.history.get(msg.r) ?? [];
            h.push(msg.p);
            if (h.length > 3)
                h.splice(0, h.length - 3);
            this.history.set(msg.r, h);
            // #endregion
            //                                    MSG COUNT
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            const count = (this.messageCount.get(msg.r) ?? 0) + 1;
            this.messageCount.set(msg.r, count);
            // #endregion
            //                                    IF RUNNING NO MIDDLEWARE
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            if (!this.BACK_END.run.middleware(msg)) {
                if (this.DEVTOOLS_ACTIVE)
                    this.log.log(`[SIGNALS][DISPATCH][NO_MIDDLEWARE]`, { msg: "blocked", p: msg }, this.log.t.deep);
                return { success: false, dropped: true };
            }
            // #endregion
            //                                    ROUTING RULES
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            //   let targets = [...this.adapters].filter(a => a !== msg._adapter)
            for (const rule of this.routeRules) {
                if (!rule.match(msg))
                    continue;
                if (this.DEVTOOLS_ACTIVE) {
                    // log(`[SIGNALS][DISPATCH][ROUTING]`, { msg: 'route_matched', p: msg  }, t.deep)
                    this.metrics.traceStage(msg.id, msg.r, "route", { rule: rule.name }); // _metrics
                }
                //     if (rule.targetAdapters) { targets = targets.filter(a => rule.targetAdapters!.includes(a.id ?? '')) }
                if (rule.targetAdapters) {
                    // if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('route_rejected', msg.r, msg.m || undefined, msg.id, { rule: rule.name })
                    if (!rule.targetAdapters.includes(msg.r))
                        return { success: false, routed: false };
                }
            }
            // #endregion
            //                                    APPLY TRANSFORMS
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            msg = await this.transforms.apply(msg, "outgoing");
            if (this.DEVTOOLS_ACTIVE) {
                //  this.emitDevtoolsEvent('transform_applied', msg.r, msg.m || undefined, msg.id, { direction: 'outgoing' });
                this.metrics.traceStage(msg.id, msg.r, "transform"); // _metrics
            }
            /**  const localListeners = this.meta._subscribers(raw.k, raw.s, raw.m, raw.r)
              if (localListeners && localListeners.size > 0) {
                  queueMicrotask(() => {
                      for (const listener of localListeners) {
                          listener(msg);
                      }
                  });
              } */
            // #endregion
            //                                    SEND
            // #region ═════════════════════════════════════════════════════════════════════════════════════
            Promise.resolve().then(() => {
                switch (adapter) {
                    case "http":
                        this.DISPATCH.sendMessage(msg, this.adp.http);
                        break;
                    case "cross_boundary":
                        this.DISPATCH.sendMessage(msg, this.adp.cross_boundary);
                        break;
                    case "same_boundary":
                        this.DISPATCH.sendMessage(msg, this.adp.same_boundary);
                        break;
                    case "atom_adapter":
                        this.DISPATCH.sendMessage(msg, this.adp.atom);
                        break;
                    case "ws":
                        this.DISPATCH.sendMessage(msg, this.adp.ws);
                        break;
                    case "reliableSocket":
                        this.DISPATCH.sendMessage(msg, this.adp.reliableSocket);
                        break;
                    case "workerThreadAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.workerThread);
                        break;
                    case "webTransportDatagramsAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.webTransportDatagrams);
                        break;
                    case "webTransportClientAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.webTransportClient);
                        break;
                    case "webRtcDataChannelHostAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.webRtcDataChannelHost);
                        break;
                    case "webRtcDataChannelClientAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.webRtcDataChannelClient);
                        break;
                    case "dataChannelAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.dataChannel);
                        break;
                    case "startWebTransportServer":
                        this.DISPATCH.sendMessage(msg, this.adp.startWebTransportServer);
                        break;
                    case "webTransportAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.webTransport);
                        break;
                    case "webRTCAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.webRTC);
                        break;
                    case "sseAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.sse);
                        break;
                    case "sharedArrayBufferAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.sharedArrayBuffer);
                        break;
                    case "sharedWorkerAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.sharedWorker);
                        break;
                    case "serviceWorkerAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.serviceWorker);
                        break;
                    case "postMessageAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.postMessage);
                        break;
                    case "startNamedPipeBroker":
                        this.DISPATCH.sendMessage(msg, this.adp.startNamedPipeBroker);
                        break;
                    case "startMQTTBroker":
                        this.DISPATCH.sendMessage(msg, this.adp.startMQTTBroker);
                        break;
                    case "mqttAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.mqtt);
                        break;
                    case "messageChannelAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.messageChannel);
                        break;
                    case "messageChannel":
                        this.DISPATCH.sendMessage(msg, this.adp.messageChannel);
                        break;
                    case "http2ClientAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.http2Client);
                        break;
                    case "http2ServerAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.http2Server);
                        break;
                    case "httpAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.http);
                        break;
                    case "electronIPCMainAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.electronIPCMain);
                        break;
                    case "electronIPCRendererAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.electronIPCRenderer);
                        break;
                    case "nativeMessagingHostAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.nativeMessagingHost);
                        break;
                    case "nativeMessagingBrowserAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.nativeMessagingBrowser);
                        break;
                    case "childProcessAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.childProcess);
                        break;
                    case "broadcastChannelAdapter2":
                        this.DISPATCH.sendMessage(msg, this.adp.broadcastChannel2);
                        break;
                    case "broadcastChannelAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.broadcastChannel);
                        break;
                    case "broadcastChannelAdapter":
                        this.DISPATCH.sendMessage(msg, this.adp.broadcastChannel);
                        break;
                    default:
                        console.log("http adapter");
                        this.DISPATCH.sendMessage(msg, this.adp.broadcastChannel);
                    // this.sendMessage(msg, a.http);
                    // this.sendMessage(msg, a.same_boundary);
                }
            });
            // #endregion
            const result = await this.invokeHandlers(msg);
            console.log(result, "emitter.dispatch.result");
            return result;
        };
        fn.hot_dispatch = () => {
        };
        fn.sendMessage = (msg, adapter, boundary) => {
            // Prepare p - JSON serialize, strip functions
            let finalizedPayload = { ...msg };
            if (finalizedPayload.p && typeof finalizedPayload.p === "object" && boundary === "cross-node") {
                finalizedPayload.p = JSON.parse(JSON.stringify(finalizedPayload.p, (key, value) => {
                    if (typeof value === "function")
                        return undefined;
                    return value;
                }));
            }
            // Send through adapter
            if (this._auth_initialized) {
                const secured = this.encryption.wrap(finalizedPayload);
                adapter.send(secured);
            }
            else {
                adapter.send(finalizedPayload);
            }
        };
        fn.drain = async () => {
            const startTime = Date.now();
            const msgs = [...this.queue];
            this.queue = [];
            // await this.root.JEDI.IDB_KV.delete(this.QUEUE_KEY);
            // Process each message through flood gates (round robin)
            for (const msg of msgs) {
                if (msg.ttl && msg.ts !== undefined && Date.now() - msg.ts > msg.ttl)
                    continue;
                await this.DISPATCH(msg);
            }
            this.metrics.recordQueue("drain", Date.now() - startTime); // _metrics
        };
        fn.enqueue = async (msg) => {
            if (this.DEVTOOLS_ACTIVE) {
                this.log.log(`[THE_WIZARD][SIGNALS][ENQUEUE]`, { msg: msg }, this.log.t.deep); //this.drop.t.error
                // this.emitDevtoolsEvent('enqueue', msg.r, msg.m || undefined, msg.id, { priority: msg.priority, ttl: msg.ttl });
                this.metrics.recordFeature("floodGate"); // _metrics
                this.metrics.traceStage(msg.id, String(msg.r), "enqueue", { priority: msg.priority, ttl: msg.ttl }); // _metrics
                this.metrics.tracePayloadSize(msg.id, JSON.stringify(msg.p ?? "").length); // _metrics
            }
            // Normal queue processing continues here...
            if (this.DEVTOOLS_ACTIVE) {
                this.metrics.recordQueue("enqueue");
            }
            this.queue.push(msg);
            this.queue.sort((a, b) => this.priorityWeight[b.priority] - this.priorityWeight[a.priority]);
            if (this.backpressure.maxQueueSize && this.queue.length > this.backpressure.maxQueueSize) {
                const dropped = this.queue.pop();
                if (dropped) {
                    this.deadLetters.push(dropped);
                    if (this.DEVTOOLS_ACTIVE)
                        this.metrics.recordQueue("drop"); // _metrics
                }
            }
            const PAYLOAD_SIZE_CAP = 524288;
            const persistable = this.queue.map((m) => {
                const size = JSON.stringify(m.p).length;
                return size > PAYLOAD_SIZE_CAP ? { ...m, p: null } : m;
            });
            // wait this.root.JEDI.IDB_KV?.set(this.QUEUE_KEY, persistable);
            if (this.DEVTOOLS_ACTIVE && this.queue.some((m) => m.id === msg.id)) {
                this.metrics.finishTrace(msg.id, "queued");
            } // _metrics
        };
        return fn;
    })();
    encryption = {
        wrap: (f) => {
            return f;
        },
        unwrap: (f) => {
            return f;
        },
    };
    adp = (() => {
        const z = () => { };
        z.ws = undefined;
        z.cross_boundary = undefined;
        z.same_boundary = undefined;
        z.atom = undefined;
        z.reliableSocket = undefined;
        z.workerThread = undefined;
        z.webTransportDatagrams = undefined;
        z.webTransportClient = undefined;
        z.webRtcDataChannelHost = undefined;
        z.webRtcDataChannelClient = undefined;
        z.dataChannel = undefined;
        z.startWebTransportServer = undefined;
        z.webTransport = undefined;
        z.webRTC = undefined;
        z.sse = undefined;
        z.sharedArrayBuffer = undefined;
        z.sharedWorker = undefined;
        z.serviceWorker = undefined;
        z.postMessage = undefined;
        z.startNamedPipeBroker = undefined;
        z.startMQTTBroker = undefined;
        z.mqtt = undefined;
        z.messageChannel = undefined;
        z.messageChannel = undefined;
        z.http2Client = undefined;
        z.http2Server = undefined;
        z.http = undefined;
        z.electronIPCMain = undefined;
        z.electronIPCRenderer = undefined;
        z.nativeMessagingHost = undefined;
        z.nativeMessagingBrowser = undefined;
        z.childProcess = undefined;
        z.broadcastChannel2 = undefined;
        z.broadcastChannel = undefined;
        z.broadcastChannel = undefined;
        return z;
    })();
    async receive(msg) {
        if (this.seen.has(msg.id))
            return;
        if (msg.ttl && msg.ts !== undefined && Date.now() - msg.ts > msg.ttl)
            return;
        if (this.seen.size > 10000)
            this.seen.clear();
        this.seen.add(msg.id);
        if (!(await this.BACK_END.run.beforeReceive(msg)))
            return;
        if (!this.BACK_END.run.middleware(msg))
            return;
        // schema validation
        const schema = this.schemas.get(msg.r);
        if (schema) {
            const ok = schema.validate(msg.p);
            if (!ok) {
                this.deadLetters.push(msg);
                await this.BACK_END.run.onError(new Error(`schema validation failed for ${msg.r}`), msg);
                return;
            }
            if (schema.coerce)
                msg.p = schema.coerce(msg.p);
        }
        const result = await this.handlers_.invoke(msg);
        return result;
    }
    log = new (class ORC_SHOUT {
        /**
         * ✔ ✘
           drop.log(`[THE_WIZARD][STAFF][SEND_VIA_ADAPTER]`, {
            messageId: msg.id,
            rt: msg.rt,
            ms: msg.ms,
            sl: msg.sl,
            k: msg.k,
            isBinaryTarget: msg.binary,
            isSecuredTarget: this.staff._auth_initialized,
            payloadPreview: msg.p
           }, t.deep)//this.drop.t.error
         *
         *  try {// check to see if in dynamic imports cache
         *     return dynamic_importer().has(module)
         *   } catch (e: A) {log(`[✘][HOOK_SYSTEM][CHECK_DYNAMIC]`, { error: e }, t.error) }
         *
            log(`[✘][HOOK_SYSTEM][CHECK_DYNAMIC]`, { error: e }, t.error)
            log(`[SIGNALS][ENCRPYTION][CONFIGURE]`, { msg: 'encryption_configured', config }, t.deep)
         */
        c = {
            emerald: {
                10: "#002c22",
                9: "#004f3b",
                8: "#016630",
                7: "#007a55",
                6: "#009966",
                5: "#00bc7d",
            },
            blue: {
                10: "",
                9: "",
                8: "",
                7: "",
                6: "",
                5: "",
            },
        };
        t = {
            deep: {
                bg: "#020817",
                txt: "#f8fafc",
                size: 12,
                title_size: 14,
                title_txt: "#94a3b8",
                payload_key: "#a4ed2c",
                payload_value: "#f8fafc",
                font: "monospace",
                border: "2px solid #101727",
                borderRadius: "6px",
                padding: "8px 16px",
                svg: "",
            },
            vintage: {
                bg: "#001B29",
                txt: "#e9e1ba",
                size: 12,
                title_size: 14,
                title_txt: "#ada785",
                payload_key: "#29c3a0",
                payload_value: "#fafafa",
                font: "monospace",
                border: "2px solid #112932",
                borderRadius: "6px",
                padding: "8px 16px",
                svg: "",
            },
            error: {
                bg: "#09090B",
                txt: "#e7000b",
                size: 12,
                title_size: 14,
                title_txt: "#9f0712",
                payload_key: "#a4ed2c",
                payload_value: "#f8fafc",
                font: "monospace",
                border: "2px solid #e7000b",
                borderRadius: "6px",
                padding: "8px 16px",
                svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e7000b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
            },
            warn: {
                bg: "#09090B",
                txt: "#e17100",
                size: 12,
                title_size: 14,
                title_txt: "#973c00",
                payload_key: "#a4ed2c",
                payload_value: "#f8fafc",
                font: "monospace",
                border: "2px solid #e17100",
                borderRadius: "6px",
                padding: "8px 16px",
                svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e17100" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`,
            },
            success: {
                bg: "#09090B",
                txt: "#009966",
                size: 12,
                title_size: 14,
                title_txt: "#016630",
                payload_key: "#a4ed2c",
                payload_value: "#f8fafc",
                font: "monospace",
                border: "2px solid #009966",
                borderRadius: "6px",
                padding: "8px 16px",
                svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#009966" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.801 10A10 10 0 1 1 17 3.335"/><path d="m9 11 3 3L22 4"/></svg>`,
            },
            info: {
                bg: "#09090B",
                txt: "#155dfc",
                size: 12,
                title_size: 14,
                title_txt: "#193cb8",
                payload_key: "#a4ed2c",
                payload_value: "#f8fafc",
                font: "monospace",
                border: "2px solid #155dfc",
                borderRadius: "6px",
                padding: "8px 16px",
                svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#155dfc" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
            },
        };
        log(msg, p, theme = this.t.deep) {
            const isArray = Array.isArray(msg);
            const title = isArray ? msg[0] : msg;
            const paragraphs = isArray ? msg.slice(1) : [];
            // Build container style (background + border + padding)
            const containerStyle = `
				  background: ${theme.bg};
				  ${theme.border ? `border: ${theme.border};` : ""}
				  ${theme.borderRadius ? `border-radius: ${theme.borderRadius};` : ""}
				  ${theme.padding ? `padding: ${theme.padding};` : ""}
				  font-family: ${theme.font};
				  display: block;
				  margin: 4px 0;
			  `;
            // Build title style (with SVG if present)
            const titleWithIcon = theme?.svg ? `${theme?.svg} ${title}` : title;
            const titleStyle = `
				  color: ${theme.title_txt};
				  font-size: ${theme.title_size}px;
				  font-weight: bold;
				  font-family: ${theme.font};
				  display: flex;
				  align-items: center;
				  gap: 8px;
			  `;
            // Build paragraph style
            const paragraphStyle = `
				  color: ${theme.txt};
				  font-size: ${theme.size}px;
				  font-family: ${theme.font};
				  margin-top: 4px;
			  `;
            // Build p key/value styles
            const keyStyle = `
				  color: ${theme.payload_key};
				  font-weight: bold;
				  font-family: ${theme.font};
			  `;
            const valueStyle = `
				  color: ${theme.payload_value};
				  font-family: ${theme.font};
			  `;
            // Format p as key: value pairs
            let payloadStr = "";
            if (p && typeof p === "object") {
                const entries = Object.entries(p);
                payloadStr = entries.map(([key, value]) => `%c${key}: %c${JSON.stringify(value, null, 2)}`).join("\n");
            }
            // Build the final log
            const logParts = [`%c${titleWithIcon}`, ...paragraphs.map((p) => `%c${p}`), payloadStr ? `\n${payloadStr}` : ""].filter(Boolean).join("\n");
            const styles = [
                containerStyle + titleStyle, // Container + title combined
                ...paragraphs.map(() => paragraphStyle),
                ...(payloadStr ? [keyStyle, valueStyle] : []),
            ];
            console.log(logParts, ...styles);
            // const dev_tools = get('DEVTOOLS_ACTIVE')
            // if (dev_tools === true) console.log(logParts, ...styles);
        }
    })();
    metrics = new (class METRICS_PORNSTAR {
        // #region ███ CONSTANTS ██████████████████████████████████████
        _startTime = Date.now();
        _nodeId = `node_${Math.random().toString(36).slice(2, 8)}`;
        _snapshots = [];
        _maxSnapshots = 200;
        // Queue Metrics
        _queueMetrics = {
            totalEnqueued: 0,
            totalDropped: 0,
            totalProcessed: 0,
            peakSize: 0,
            currentSize: 0,
            drainCount: 0,
            totalDrainTime: 0,
            deadLetterSize: 0,
            avgQueueDepth: 0,
            depthSamples: 0
        };
        // Flood Gate Metrics - THE GOOD STUFF
        _floodGateMetrics = {
            gate1: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
            gate2: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
            gate3: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
            gate4: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
            totalHits: 0,
            totalErrors: 0,
            totalLatency: 0,
            latencyCount: 0,
            bypassed: 0,
            queued: 0,
            lastWindowStart: Date.now(),
            windowHits: 0
        };
        // Lane Metrics - SPREAD 'EM WIDE
        _laneMetrics = {
            normal: { processed: 0, errors: 0, totalLatency: 0, count: 0, lastWindowStart: Date.now(), windowCount: 0 },
            signal: { processed: 0, errors: 0, totalLatency: 0, count: 0, lastWindowStart: Date.now(), windowCount: 0 },
            binary: { processed: 0, errors: 0, totalLatency: 0, count: 0, lastWindowStart: Date.now(), windowCount: 0 },
            signal_save_value: { processed: 0, errors: 0, totalLatency: 0, count: 0, lastWindowStart: Date.now(), windowCount: 0 }
        };
        // Adapter Metrics - ALL THE ADAPTERS
        _adapterMetrics = new Map();
        // Backpressure Metrics
        _backpressureMetrics = {
            throttleCount: 0,
            lastThrottle: 0,
            totalBackpressureEvents: 0,
            currentThrottleDuration: 0
        };
        // Channel Metrics
        _channelMetrics = new Map();
        // Feature Usage Metrics
        _featureMetrics = {
            beacon: 0,
            radar: 0,
            vessel: 0,
            dock: 0,
            convoy: 0,
            sonar: 0,
            heartbeat: 0,
            watchdog: 0,
            floodGate: 0,
            pipeline: 0,
            tide: 0,
            mesh: 0,
            crdt: 0,
            synapse: 0,
            transaction: 0,
            circuitBreaker: 0,
            rateLimit: 0,
            plinko: 0,
            gateMaster: 0,
            saga: 0,
            ecs: 0,
            reactor: 0
        };
        _eventLoopLag = 0;
        _lagMeasurements = [];
        traces = new Map();
        MAX_TRACES = 2000; // ring buffer — oldest evicted first
        traceOrder = [];
        // #endregion
        // #region ███ CONSTRUCTOR ██████████████████████████████████████
        constructor() {
            // Measure event loop lag periodically
            setInterval(() => {
                const start = Date.now();
                Promise.resolve().then(() => {
                    this._eventLoopLag = Date.now() - start;
                    this._lagMeasurements.push(this._eventLoopLag);
                    if (this._lagMeasurements.length > 100) {
                        this._lagMeasurements.shift();
                    }
                });
            }, 1000);
        }
        // #endregion
        // #region ███ RECORD METHODS ██████████████████████████████████████
        recordQueue(action, duration) {
            const now = Date.now();
            switch (action) {
                case 'enqueue':
                    this._queueMetrics.totalEnqueued++;
                    this._queueMetrics.currentSize++;
                    if (this._queueMetrics.currentSize > this._queueMetrics.peakSize) {
                        this._queueMetrics.peakSize = this._queueMetrics.currentSize;
                    }
                    break;
                case 'dequeue':
                case 'process':
                    this._queueMetrics.currentSize = Math.max(0, this._queueMetrics.currentSize - 1);
                    if (action === 'process') {
                        this._queueMetrics.totalProcessed++;
                    }
                    break;
                case 'drop':
                    this._queueMetrics.totalDropped++;
                    this._queueMetrics.currentSize = Math.max(0, this._queueMetrics.currentSize - 1);
                    break;
                case 'drain':
                    this._queueMetrics.drainCount++;
                    if (duration) {
                        this._queueMetrics.totalDrainTime += duration;
                    }
                    break;
            }
            // Track average queue depth
            this._queueMetrics.depthSamples++;
            this._queueMetrics.avgQueueDepth =
                (this._queueMetrics.avgQueueDepth * (this._queueMetrics.depthSamples - 1) + this._queueMetrics.currentSize) /
                    this._queueMetrics.depthSamples;
        }
        recordFloodGate(gateNumber, latency, error) {
            const gateKey = `gate${gateNumber}`;
            const gate = this._floodGateMetrics[gateKey];
            gate.hits++;
            gate.lastHit = Date.now();
            gate.totalLatency += latency;
            gate.count++;
            if (error) {
                gate.errors++;
                this._floodGateMetrics.totalErrors++;
            }
            this._floodGateMetrics.totalHits++;
            this._floodGateMetrics.totalLatency += latency;
            this._floodGateMetrics.latencyCount++;
            this._floodGateMetrics.windowHits++;
            // Reset window every second
            const now = Date.now();
            if (now - this._floodGateMetrics.lastWindowStart > 1000) {
                this._floodGateMetrics.lastWindowStart = now;
                this._floodGateMetrics.windowHits = 0;
            }
        }
        recordLane(lane, latency, error) {
            const metrics = this._laneMetrics[lane];
            metrics.processed++;
            metrics.totalLatency += latency;
            metrics.count++;
            metrics.windowCount++;
            if (error)
                metrics.errors++;
            // Reset window every second
            const now = Date.now();
            if (now - metrics.lastWindowStart > 1000) {
                metrics.lastWindowStart = now;
                metrics.windowCount = 0;
            }
        }
        recordAdapter(id, latency, isFloodGate = false, error) {
            let metrics = this._adapterMetrics.get(id);
            if (!metrics) {
                metrics = {
                    total: 0,
                    failures: 0,
                    totalLatency: 0,
                    count: 0,
                    lastHit: 0,
                    floodGateProcessed: 0,
                    lastWindowStart: Date.now(),
                    windowCount: 0
                };
                this._adapterMetrics.set(id, metrics);
            }
            metrics.total++;
            metrics.totalLatency += latency;
            metrics.count++;
            metrics.lastHit = Date.now();
            metrics.windowCount++;
            if (isFloodGate)
                metrics.floodGateProcessed++;
            if (error)
                metrics.failures++;
        }
        recordBackpressure(throttled, duration) {
            if (throttled) {
                this._backpressureMetrics.throttleCount++;
                this._backpressureMetrics.lastThrottle = Date.now();
                this._backpressureMetrics.totalBackpressureEvents++;
                if (duration) {
                    this._backpressureMetrics.currentThrottleDuration = duration;
                }
            }
        }
        recordChannel(rt, handlerCount) {
            let metrics = this._channelMetrics.get(rt);
            if (!metrics) {
                metrics = { messages: 0, lastMessage: 0, handlers: 0 };
                this._channelMetrics.set(rt, metrics);
            }
            metrics.messages++;
            metrics.lastMessage = Date.now();
            if (handlerCount !== undefined) {
                metrics.handlers = handlerCount;
            }
        }
        recordFeature(feature) {
            this._featureMetrics[feature]++;
        }
        // #endregion
        // #region ███ SNAPSHOT █████████████████████████████████████████████
        snapshot(queueSize, maxQueueSize, openGates, throttled, adapters, channels) {
            const now = Date.now();
            const windowSeconds = 1;
            // Calculate throughputs
            const floodGateThroughput = this._floodGateMetrics.windowHits / windowSeconds;
            const laneThroughput = {
                normal: this._laneMetrics.normal.windowCount / windowSeconds,
                signal: this._laneMetrics.signal.windowCount / windowSeconds,
                binary: this._laneMetrics.binary.windowCount / windowSeconds,
                signal_save_value: this._laneMetrics.signal_save_value.windowCount / windowSeconds
            };
            // Calculate adapter metrics
            const adapterMetricsArray = [];
            for (const [id, metrics] of this._adapterMetrics) {
                const avgLatency = metrics.count > 0 ? metrics.totalLatency / metrics.count : 0;
                const errorRate = metrics.total > 0 ? metrics.failures / metrics.total : 0;
                const throughput = metrics.windowCount / windowSeconds;
                adapterMetricsArray.push({
                    id,
                    total: metrics.total,
                    failures: metrics.failures,
                    avgLatency,
                    throughput,
                    errorRate,
                    healthy: errorRate < 0.5 && metrics.total > 0,
                    floodGateProcessed: metrics.floodGateProcessed
                });
            }
            // Sort adapters by total
            adapterMetricsArray.sort((a, b) => b.total - a.total);
            // Get top channels
            const topChannels = Array.from(this._channelMetrics.entries())
                .map(([name, metrics]) => ({ name, count: metrics.messages }))
                .sort((a, b) => b.count - a.count)
                .slice(0, 10);
            const activeHandlers = Array.from(channels.values()).reduce((sum, set) => sum + set.size, 0);
            const snapshot = {
                timestamp: now,
                nodeId: this._nodeId,
                uptime: now - this._startTime,
                queue: {
                    currentSize: queueSize,
                    maxSize: maxQueueSize,
                    fillPercent: maxQueueSize > 0 ? (queueSize / maxQueueSize) * 100 : 0,
                    peakSize: this._queueMetrics.peakSize,
                    totalEnqueued: this._queueMetrics.totalEnqueued,
                    totalDropped: this._queueMetrics.totalDropped,
                    totalProcessed: this._queueMetrics.totalProcessed,
                    drainCount: this._queueMetrics.drainCount,
                    avgDrainTime: this._queueMetrics.drainCount > 0 ?
                        this._queueMetrics.totalDrainTime / this._queueMetrics.drainCount : 0,
                    deadLetterSize: this._queueMetrics.deadLetterSize
                },
                floodGates: {
                    openGates,
                    gate1: {
                        hits: this._floodGateMetrics.gate1.hits,
                        errors: this._floodGateMetrics.gate1.errors,
                        lastHit: this._floodGateMetrics.gate1.lastHit,
                        avgLatency: this._floodGateMetrics.gate1.count > 0 ?
                            this._floodGateMetrics.gate1.totalLatency / this._floodGateMetrics.gate1.count : 0
                    },
                    gate2: {
                        hits: this._floodGateMetrics.gate2.hits,
                        errors: this._floodGateMetrics.gate2.errors,
                        lastHit: this._floodGateMetrics.gate2.lastHit,
                        avgLatency: this._floodGateMetrics.gate2.count > 0 ?
                            this._floodGateMetrics.gate2.totalLatency / this._floodGateMetrics.gate2.count : 0
                    },
                    gate3: {
                        hits: this._floodGateMetrics.gate3.hits,
                        errors: this._floodGateMetrics.gate3.errors,
                        lastHit: this._floodGateMetrics.gate3.lastHit,
                        avgLatency: this._floodGateMetrics.gate3.count > 0 ?
                            this._floodGateMetrics.gate3.totalLatency / this._floodGateMetrics.gate3.count : 0
                    },
                    gate4: {
                        hits: this._floodGateMetrics.gate4.hits,
                        errors: this._floodGateMetrics.gate4.errors,
                        lastHit: this._floodGateMetrics.gate4.lastHit,
                        avgLatency: this._floodGateMetrics.gate4.count > 0 ?
                            this._floodGateMetrics.gate4.totalLatency / this._floodGateMetrics.gate4.count : 0
                    },
                    totalHits: this._floodGateMetrics.totalHits,
                    totalErrors: this._floodGateMetrics.totalErrors,
                    bypassRate: this._floodGateMetrics.totalHits + this._floodGateMetrics.queued > 0 ?
                        (this._floodGateMetrics.totalHits / (this._floodGateMetrics.totalHits + this._floodGateMetrics.queued)) * 100 : 0,
                    avgLatency: this._floodGateMetrics.latencyCount > 0 ?
                        this._floodGateMetrics.totalLatency / this._floodGateMetrics.latencyCount : 0,
                    throughputPerSecond: floodGateThroughput
                },
                lanes: {
                    normal: {
                        processed: this._laneMetrics.normal.processed,
                        avgLatency: this._laneMetrics.normal.count > 0 ?
                            this._laneMetrics.normal.totalLatency / this._laneMetrics.normal.count : 0,
                        errors: this._laneMetrics.normal.errors,
                        throughput: laneThroughput.normal
                    },
                    signal: {
                        processed: this._laneMetrics.signal.processed,
                        avgLatency: this._laneMetrics.signal.count > 0 ?
                            this._laneMetrics.signal.totalLatency / this._laneMetrics.signal.count : 0,
                        errors: this._laneMetrics.signal.errors,
                        throughput: laneThroughput.signal
                    },
                    binary: {
                        processed: this._laneMetrics.binary.processed,
                        avgLatency: this._laneMetrics.binary.count > 0 ?
                            this._laneMetrics.binary.totalLatency / this._laneMetrics.binary.count : 0,
                        errors: this._laneMetrics.binary.errors,
                        throughput: laneThroughput.binary
                    },
                    signal_save_value: {
                        processed: this._laneMetrics.signal_save_value.processed,
                        avgLatency: this._laneMetrics.signal_save_value.count > 0 ?
                            this._laneMetrics.signal_save_value.totalLatency / this._laneMetrics.signal_save_value.count : 0,
                        errors: this._laneMetrics.signal_save_value.errors,
                        throughput: laneThroughput.signal_save_value
                    }
                },
                adapters: adapterMetricsArray.slice(0, 20),
                backpressure: {
                    throttled,
                    throttleCount: this._backpressureMetrics.throttleCount,
                    lastThrottle: this._backpressureMetrics.lastThrottle,
                    avgQueueDepth: this._queueMetrics.avgQueueDepth,
                    maxQueueDepth: this._queueMetrics.peakSize,
                    totalBackpressureEvents: this._backpressureMetrics.totalBackpressureEvents
                },
                channels: {
                    total: channels.size,
                    activeSubs: activeHandlers,
                    totalMessages: Array.from(this._channelMetrics.values()).reduce((sum, m) => sum + m.messages, 0),
                    topChannels
                },
                system: {
                    memoryUsage: process.memoryUsage(),
                    cpuUsage: process.cpuUsage(),
                    eventLoopLag: this._eventLoopLag,
                    activeHandlers,
                    pendingRequests: this._queueMetrics.currentSize
                },
                features: { ...this._featureMetrics }
            };
            // Store snapshot
            this._snapshots.push(snapshot);
            if (this._snapshots.length > this._maxSnapshots) {
                this._snapshots.shift();
            }
            return snapshot;
        }
        // #endregion
        // #region ███ QUERY METHODS ████████████████████████████████████████
        getHistory(limit) {
            return limit ? this._snapshots.slice(-limit) : this._snapshots;
        }
        getLatest() {
            return this._snapshots[this._snapshots.length - 1] || null;
        }
        getMetricsSummary() {
            const latest = this.getLatest();
            if (!latest)
                return null;
            return {
                totalMessages: latest.queue.totalProcessed + latest.queue.totalDropped,
                totalErrors: latest.floodGates.totalErrors + latest.lanes.normal.errors + latest.lanes.signal.errors + latest.lanes.binary.errors + latest.lanes.signal_save_value.errors,
                avgLatency: latest.floodGates.avgLatency,
                throughput: latest.floodGates.throughputPerSecond,
                queuePressure: latest.queue.fillPercent,
                activeAdapters: latest.adapters.length,
                features: latest.features
            };
        }
        // #endregion
        // #region ███ STREAM DATA FOR DASHBOARD ███████████████████████████
        getStreamData() {
            const latest = this.getLatest();
            if (!latest)
                return null;
            return {
                ...latest,
                adapters: latest.adapters,
                history: this._snapshots.slice(-30).map(s => ({
                    timestamp: s.timestamp,
                    queueSize: s.queue.currentSize,
                    openGates: s.floodGates.openGates,
                    throughput: s.floodGates.throughputPerSecond,
                    avgLatency: s.floodGates.avgLatency,
                    errors: s.floodGates.totalErrors
                }))
            };
        }
        // #endregion
        // #region ███ RESET █████████████████████████████████████████████████
        reset() {
            this._startTime = Date.now();
            this._snapshots = [];
            this._queueMetrics = {
                totalEnqueued: 0,
                totalDropped: 0,
                totalProcessed: 0,
                peakSize: 0,
                currentSize: 0,
                drainCount: 0,
                totalDrainTime: 0,
                deadLetterSize: 0,
                avgQueueDepth: 0,
                depthSamples: 0
            };
            this._floodGateMetrics = {
                gate1: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
                gate2: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
                gate3: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
                gate4: { hits: 0, errors: 0, lastHit: 0, totalLatency: 0, count: 0 },
                totalHits: 0,
                totalErrors: 0,
                totalLatency: 0,
                latencyCount: 0,
                bypassed: 0,
                queued: 0,
                lastWindowStart: Date.now(),
                windowHits: 0
            };
            this._adapterMetrics.clear();
            this._channelMetrics.clear();
            Object.keys(this._featureMetrics).forEach(key => {
                this._featureMetrics[key] = 0;
            });
        }
        // #endregion
        // #region ███ PER MSG TRACING █████████████████████████████████████████████████
        getOrCreateTrace(id, rt, ms) {
            let t = this.traces.get(id);
            if (!t) {
                t = { id, rt, ms, createdAt: Date.now(), stages: [] };
                this.traces.set(id, t);
                this.traceOrder.push(id);
                if (this.traceOrder.length > this.MAX_TRACES) {
                    const evict = this.traceOrder.shift();
                    if (evict)
                        this.traces.delete(evict);
                }
            }
            return t;
        }
        // Call at the start of any stage a message passes through.
        traceStage(id, rt, stage, detail, ms) {
            const t = this.getOrCreateTrace(id, rt, ms);
            const prev = t.stages[t.stages.length - 1];
            const now = Date.now();
            if (prev && prev.durationMs === undefined) {
                prev.durationMs = now - prev.ts; // close out the previous open stage
            }
            t.stages.push({ stage, ts: now, detail });
        }
        traceLane(id, lane) {
            const t = this.traces.get(id);
            if (t)
                t.lane = lane;
        }
        traceGate(id, gate) {
            const t = this.traces.get(id);
            if (t)
                t.gate = gate;
        }
        traceAdapter(id, adapterId) {
            const t = this.traces.get(id);
            if (t)
                t.adapterId = adapterId;
        }
        tracePayloadSize(id, bytes) {
            const t = this.traces.get(id);
            if (t)
                t.payloadSize = bytes;
        }
        // Call when a message reaches a terminal state.
        finishTrace(id, outcome) {
            const t = this.traces.get(id);
            if (!t)
                return;
            const now = Date.now();
            const last = t.stages[t.stages.length - 1];
            if (last && last.durationMs === undefined)
                last.durationMs = now - last.ts;
            t.finishedAt = now;
            t.totalMs = now - t.createdAt;
            t.outcome = outcome;
        }
        // #endregion
        // #region ███ query surface for consumers █████████████████████████████████████████████████
        getMessageTrace(id) {
            return this.traces.get(id) ?? null;
        }
        getRecentTraces(limit = 100) {
            return this.traceOrder.slice(-limit).map(id => this.traces.get(id)).filter(Boolean);
        }
        getTracesByChannel(rt, limit = 100) {
            const out = [];
            for (let i = this.traceOrder.length - 1; i >= 0 && out.length < limit; i--) {
                const t = this.traces.get(this.traceOrder[i]);
                if (t && t.rt === rt)
                    out.push(t);
            }
            return out;
        }
        getTracesByAdapter(adapterId, limit = 100) {
            const out = [];
            for (let i = this.traceOrder.length - 1; i >= 0 && out.length < limit; i--) {
                const t = this.traces.get(this.traceOrder[i]);
                if (t && t.adapterId === adapterId)
                    out.push(t);
            }
            return out;
        }
        getTracesByOutcome(outcome, limit = 100) {
            const out = [];
            for (let i = this.traceOrder.length - 1; i >= 0 && out.length < limit; i--) {
                const t = this.traces.get(this.traceOrder[i]);
                if (t && t.outcome === outcome)
                    out.push(t);
            }
            return out;
        }
    })();
    //public channel = new O2<S, Set<CSEHandler>>()
    //public sub_channel = new O2<S, O2<S, Set<CSEHandler>>>()
    //public trip_channel = new O2<S, O2<S, Set<CSEHandler>>>()
    //public quad_channel = new O2<S, O2<S, Set<CSEHandler>>>()
    contracts = new O2();
    traceCounter = 0;
    relayCounter = 0;
    closeHandlers = new Set();
    counters = {
        dataSent: 0,
        dataAcked: 0,
        dataRetransmitted: 0,
        nackSent: 0,
        nackReceived: 0,
        overflow: 0,
        dlq: 0,
        dropped: 0,
        errors: 0,
        streamsActive: 0,
        streamsCompleted: 0,
        relaySent: 0,
    };
    h = {
        //HELPERS
        WIRE_VERSION: 1,
        parseEnvelope: (raw) => {
            if (!raw || typeof raw !== "object")
                return null;
            const e = raw;
            if (e.v !== this.h.WIRE_VERSION || typeof e.sid !== "string" || typeof e.seq !== "number")
                return null;
            return e;
        },
        normCh: (v) => {
            return v.replace(/[\[\]]/g, "").trim();
        },
        fnv1a: (input) => {
            // FNV-1a 32-bit — isomorphic, deterministic, no crypto dependency.
            let h = 0x811c9dc5;
            for (let i = 0; i < input.length; i++) {
                h ^= input.charCodeAt(i);
                h = Math.imul(h, 0x01000193);
            }
            return (h >>> 0).toString(36);
        },
        streamId: (quad, trip, sub, ch, name = "stream") => {
            const base = [this.h.normCh(quad), this.h.normCh(trip), this.h.normCh(sub), this.h.normCh(ch)].join("|");
            const slug = name.replace(/[^a-zA-Z0-9_-]+/g, "-").slice(0, 20) || "s";
            return `sl_${this.h.fnv1a(`${base}|${name}`)}_${slug}`;
        },
        newEpoch: () => {
            return `${Date.now().toString(36)}-${this.h.fnv1a(Math.random().toString(36).slice(2))}`;
        },
        newTrace: () => {
            this.traceCounter = (this.traceCounter + 1) >>> 0;
            return `tr-${Date.now().toString(36)}-${this.traceCounter.toString(36)}-${this.h.fnv1a(Math.random().toString(36).slice(2))}`;
        },
        toBase64: (bytes) => {
            if (typeof Buffer !== "undefined")
                return Buffer.from(bytes).toString("base64");
            let s = "";
            for (let i = 0; i < bytes.length; i++)
                s += String.fromCharCode(bytes[i]);
            return btoa(s);
        },
        fromBase64: (b64) => {
            if (typeof Buffer !== "undefined")
                return new Uint8Array(Buffer.from(b64, "base64"));
            const bin = atob(b64);
            const out = new Uint8Array(bin.length);
            for (let i = 0; i < bin.length; i++)
                out[i] = bin.charCodeAt(i);
            return out;
        },
        encodePayload: (data, binary) => {
            if (!binary)
                return { enc: "json", data };
            const bytes = data instanceof Uint8Array ? data : new TextEncoder().encode(String(data));
            return { enc: "base64", data: this.h.toBase64(bytes) };
        },
        decodePayload: (env, binary) => {
            if (binary && env.enc === "base64" && typeof env.data === "string")
                return this.h.fromBase64(env.data);
            return env.data;
        },
        sleep: (m) => new Promise((r) => setTimeout(r, m)),
        isFunc: (v) => {
            return typeof v === "function";
        },
        trackActive: (delta) => {
            this.counters.streamsActive = Math.max(0, this.counters.streamsActive + delta);
        },
        normalizeKey: (rawKey) => {
            return rawKey.trim().toLowerCase();
        },
    };
    // #endregion
    //                                                                               SIGNALS
    // #region ═════════════════════════════════════════════════════════════════════════════════════
    mainMap = new O2();
    weakMap = new WeakMap();
    tupleCache = new O2();
    listeners = new O2();
    conflicts = new O2();
    causal = new O2();
    // #endregion
    //                                                                              REACTIVE
    // #region ═════════════════════════════════════════════════════════════════════════════════════
    components = new O2();
    pendingNotifications = new Set();
    loopScheduled = false;
    currentComponent = null;
    currentHookState = null;
    batchDepth = 0;
    openScopes = new Set();
    dirty = new Set();
    PRIORITY_ORDER = ["immediate", "user-blocking", "normal", "background", "idle"];
    GLOBAL_SCOPE = Symbol("THE_WIZARD_ROOT_REG");
    scopeCursors = { immediate: 0, "user-blocking": 0, normal: 0, background: 0, idle: 0 };
    currentScope = this.GLOBAL_SCOPE;
    flushScheduled = false;
}
// #endregion
//                                                               [SITH] [PALPATINE] [DARTH VADER] [GALEN MAREK] [RULE OF 2] BUS 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class SITH {
    root;
    mesh_store;
    state;
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    capacity;
    pidCapacity;
    schedule;
    shared;
    maxBatch;
    wait;
    f64Capacity = 1 << 18;
    u32Capacity = 1 << 18;
    i32Capacity = 1 << 18;
    byteCapacity = 1 << 20;
    DEVTOOLS_ACTIVE = false;
    constructor() { }
    send = (() => {
        //const msg: SIG_BUS_Message = { id: this.hub.reg._sid(), r: r, m: m, s: s, k: k,  p: p, ts: Date.now(), _ack: false };
        const fn = (p, k, s = '_globalThis', m = this.m, r = this.r, adapter = "ws") => {
            let v = this.mesh_store.DISPATCH({ id: this.root.reg._sid(), r: r, m: m, s: s, k: k, p: p, _ack: false }, adapter);
            if (v !== undefined && !(v instanceof Promise)) {
                return MaybePromise(v);
            }
            else {
                const tMs = 5000;
                return MaybePromise((() => {
                    return new P((resolve, reject) => {
                        let timer;
                        if (v) {
                            if (timer)
                                clearTimeout(timer);
                            resolve(v);
                            return;
                        }
                        if (tMs) {
                            timer = setTimeout(() => { reject(new Error(`wait() timed out after ${tMs}ms`)); }, tMs);
                        }
                    });
                })());
            }
        };
        fn.gt = (() => {
            const fn = (p, k, s = '_globalThis', m = this.m, r = this.r, c = 'mesh') => {
                const root = c === 'globalThis' ? globalThis : this.mesh_store.THE_MESH_CACHE;
                const rL = (root[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const channel = mL[s] ??= Object.create(null);
                const handler = channel[k]?.handler;
                if (!handler)
                    return undefined;
                const v = handler(p, { r, m, s, k, p: p });
                if (v !== undefined && !(v instanceof Promise)) {
                    return MaybePromise(v);
                }
                else {
                    const tMs = 5000;
                    return MaybePromise((() => {
                        return new P((resolve, reject) => {
                            let timer;
                            if (v) {
                                if (timer)
                                    clearTimeout(timer);
                                resolve(v);
                                return;
                            }
                            if (tMs) {
                                timer = setTimeout(() => { reject(new Error(`wait() timed out after ${tMs}ms`)); }, tMs);
                            }
                        });
                    })());
                }
            };
            fn.hot = (() => {
                const fn = (p, k, s = '_globalThis', m = this.m, r = this.r, c = 'mesh') => {
                    const handler = this.mesh_store[k]._handlers;
                    if (!handler)
                        return undefined;
                    const v = handler(p, { r, m, s, k, p: p });
                    if (v !== undefined && !(v instanceof Promise)) {
                        return MaybePromise(v);
                    }
                    else {
                        const tMs = 5000;
                        return MaybePromise((() => {
                            return new P((resolve, reject) => {
                                let timer;
                                if (v) {
                                    if (timer)
                                        clearTimeout(timer);
                                    resolve(v);
                                    return;
                                }
                                if (tMs) {
                                    timer = setTimeout(() => { reject(new Error(`wait() timed out after ${tMs}ms`)); }, tMs);
                                }
                            });
                        })());
                    }
                };
                return fn;
            })();
            return fn;
        })();
        fn.adapter = (() => {
            const fn = () => { };
            fn.hot = (() => {
                const fn = () => { };
                return fn;
            })();
            return fn;
        })();
        fn.stream = (p, k, s = '_globalThis', m = this.m, r = this.r, c = 'mesh') => {
            const root = c === 'globalThis' ? globalThis : this.mesh_store.THE_MESH_CACHE;
            const rL = (root[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const _listeners = mL[s] ??= Object.create(null);
            const handler = _listeners[k]?.handler;
            if (!handler)
                return undefined;
            const msg = { r, m, s, k, p: p, _ack: false };
            const result = handler(p, msg);
            if (handler) {
                for (const listener of handler)
                    listener(msg);
            }
            return result;
        };
        fn.nested = (() => {
            const fn = (p, k, s = '_globalThis', m = this.m, r = this.r, c = 'mesh') => {
                const root = c === 'globalThis' ? globalThis : this.mesh_store.THE_MESH_CACHE;
                const rL = (root[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const _listeners = mL[s] ??= Object.create(null);
                const set = _listeners[k]?.handler;
                if (!set)
                    return;
                for (const fn of set) {
                    fn(p);
                }
            };
            fn.hot = (() => {
                const fn = (p, k, s = '_globalThis') => {
                    const set = this.mesh_store._handlers[k]?.handler;
                    if (!set)
                        return;
                    for (const fn of set) {
                        fn(p);
                    }
                };
                return fn;
            })();
            return fn;
        })();
        return fn;
    })();
    sub = (() => {
        const fn = (handler, k, s = '_globalThis', m = this.m, r = this.r, c = "mesh") => {
            // CHANNEL LEVEL
            const channelSet = (this.mesh_store.getR(r, c)[r] ??= new Set());
            channelSet.add(handler);
            // SUB_CHANNEL LEVEL
            const subMap = this.mesh_store.getM(m, r, c);
            const subSet = (subMap[r] ??= new Set());
            subSet.add(handler);
            // TRIP_CHANNEL LEVEL
            const tripMap = this.mesh_store.getS(s, m, r, c);
            const tripSet = (tripMap[m] ??= new Set());
            tripSet.add(handler);
            // QUAD_CHANNEL LEVEL
            const quadMap = (tripMap[k] ??= Object.create(null));
            const quadSet = (quadMap[s] ??= new Set());
            quadSet.add(handler);
            return () => {
                // QUAD_CHANNEL
                quadSet.delete(handler);
                if (!quadSet.size)
                    delete quadMap[s];
                if (!Object.keys(quadMap).length)
                    delete tripMap[k];
                // TRIP_CHANNEL
                tripSet.delete(handler);
                if (!tripSet.size)
                    delete tripMap[m];
                // SUB_CHANNEL
                subSet.delete(handler);
                if (!subSet.size)
                    delete subMap[r];
                // CHANNEL
                channelSet.delete(handler);
                if (!channelSet.size)
                    delete this.mesh_store.getR(r, c)[r];
                // Remove empty hierarchy nodes
                if (!Object.keys(subMap).length) {
                    delete this.mesh_store.getR(c, r)[m];
                }
                if (!Object.keys(tripMap).length) {
                    delete this.mesh_store.getR(c, r)[m];
                }
            };
        };
        fn.hot = (handler, k) => {
            const root = this.mesh_store._handlers;
            let handlers = root[k];
            if (!handlers) {
                handlers = new Set();
                root[k] = handlers;
            }
            handlers.add(handler);
            return () => { handlers.delete(handler); if (handlers.size === 0) {
                delete root[k];
            } };
        };
        fn.gt = (() => {
            const fn = (handler, k, s = '_globalThis', m = this.m, r = this.r, c = 'mesh') => {
                const root = c === 'globalThis' ? globalThis : this.mesh_store.THE_MESH_CACHE;
                const rL = (root[r] ??= new O2());
                const mL = (rL[m] ??= new O2());
                const sl = mL[s] ??= new O2();
                const channel = (sl[k] ??= Object.create(null));
                channel.handler = handler;
                return () => {
                    if (channel.handler === handler) {
                        channel.handler = undefined;
                    }
                };
            };
            fn.hot = (() => {
                const fn = (handler, k, s = '_globalThis') => {
                    const channel = this.mesh_store._handlers[k]?.handler;
                    channel.handler = handler;
                    return () => {
                        if (channel.handler === handler) {
                            channel.handler = undefined;
                        }
                    };
                };
                return fn;
            })();
            return fn;
        })();
        fn.adapter = (() => {
            const fn = (handler, k, s = '_globalThis', m = this.m, r = this.r, c = "mesh") => { };
            fn.hot = (() => {
                const fn = () => { };
                return fn;
            })();
            return fn;
        })();
        fn.nested = (handler, k, s = '_globalThis', m = this.m, r = this.r, c = 'mesh') => {
            if (r === this.r && m === this.m && s === this.s) {
                const root = this.mesh_store._handlers[k]?.handler;
                const _listeners = root[k];
                if (!_listeners.has(k)) {
                    _listeners.set(k, new Set());
                }
                _listeners[k].add(handler);
                return () => {
                    const set = _listeners.get(k);
                    if (set) {
                        set.delete(handler);
                        if (set.size === 0)
                            _listeners.delete(k);
                    }
                };
            }
            else {
                const root = c === 'globalThis' ? globalThis : this.mesh_store.THE_MESH_CACHE;
                const rL = (root[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                let _listeners = mL[s] ??= Object.create(null);
                _listeners = _listeners[k];
                if (!_listeners.has(k)) {
                    _listeners.set(k, new Set());
                }
                _listeners.get(k).add(handler);
                return () => {
                    const set = _listeners.get(k);
                    if (set) {
                        set.delete(handler);
                        if (set.size === 0)
                            _listeners.delete(k);
                    }
                };
            }
        };
        fn.nested3 = (p, k, s = '_globalThis', m = this.m, r = this.r) => {
            if (r === this.r && m === this.m && s === this.s) {
            }
            else {
            }
        };
        return fn;
    })();
    async *streamGen(p, k, s = '_globalThis', m = this.m, r = this.r, opts = {}) {
        const replyTo = this.root.reg._sid();
        const queue = [];
        let closed = false;
        let failure;
        let wake;
        const handlers = this.mesh_store.getS(s, m, r).get(k);
        if (!handlers || !handlers.size) {
            throw new Error(`[rift] no subscriber for k="${k}"`);
        }
        const reply = (value) => {
            if (value === END) {
                closed = true;
            }
            else if (value instanceof Error) {
                failure = value;
            }
            else {
                queue.push(value);
            }
            wake?.();
            wake = undefined;
        };
        for (const handler of handlers) {
            handler({ id: this.root.reg._sid(), r: r ?? this.r, m: m ?? this.m, s: s ?? this.s, k, p: { p }, ts: Date.now(), _ack: false, replyTo, stream: true }, reply);
        }
        while (true) {
            if (queue.length) {
                yield queue.shift();
                continue;
            }
            if (failure)
                throw failure;
            if (closed)
                return;
            await new P((resolveWake) => { wake = resolveWake; });
        }
    }
    stream = (() => {
        function stream(handlerOrSlave, k, s = this.s, m = this.m, r = this.r) {
            const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
            const global = globalThis;
            const channel = (this.channels[k] ??= {});
            if (!channel)
                throw new Error("Channel does not exist");
            const listeners = (channel.push ??= new Set());
            if (handler) {
                listeners.add(handler);
                return () => { listeners.delete(handler); if (!listeners.size)
                    channel.push = undefined; };
            }
            const queue = [];
            const waiters = [];
            let offset = 0;
            let closed = false;
            const listener = (msg) => {
                if (waiters.length) {
                    waiters.shift().resolve({ value: msg, done: false });
                }
                else {
                    queue.push(msg);
                }
            };
            listeners.add(listener);
            const close = () => {
                if (closed)
                    return;
                closed = true;
                listeners.delete(listener);
                if (!listeners.size)
                    channel.push = undefined;
                for (const waiter of waiters) {
                    waiter.resolve({ value: undefined, done: true });
                }
                waiters.length = 0;
                queue.length = 0;
                offset = 0;
            };
            const iterator = {
                next: () => {
                    if (offset < queue.length) {
                        const value = queue[offset++];
                        if (offset === queue.length) {
                            queue.length = 0;
                            offset = 0;
                        }
                        return Promise.resolve({ value, done: false });
                    }
                    if (closed) {
                        return Promise.resolve({ value: undefined, done: true });
                    }
                    return new Promise((resolve) => waiters.push({ resolve }));
                },
                return: async () => { close(); return { value: undefined, done: true }; },
                [Symbol.asyncIterator]() { return iterator; },
            };
            return iterator;
        }
        const fn = () => stream;
        fn.request = (p, onChunk, k, s = this.s, m = this.m, r = this.r) => {
            const streamId = this.root.reg._sid(); // `[${r}][${m}][${s}][${k}][${_sid()}]`.replace(/[.\[\]]/g, match => (match === "." ? "_" : ""));
            const result = new Promise((resolve, reject) => {
                let closed = false;
                let closeSubscription;
                const finish = (error) => {
                    if (closed)
                        return;
                    closed = true;
                    closeSubscription?.();
                    if (error) {
                        reject(error instanceof Error ? error : new Error(String(error)));
                    }
                    else {
                        resolve();
                    }
                };
                closeSubscription = this.sub((data) => {
                    if (closed)
                        return;
                    switch (data.type) {
                        case "chunk":
                            onChunk(data.data);
                            break;
                        case "done":
                            finish();
                            break;
                        case "error":
                            finish(data.error);
                            break;
                    }
                }, k, s, m, r);
                try {
                    this.send({ streamId, ...p }, k, s, m, r);
                }
                catch (error) {
                    finish(error);
                }
            });
            return result;
        };
        fn.native = (k, s = this.s, m = this.m, r = this.r) => {
            return new WritableStream({ write: (chunk) => { this.send(chunk, k, s, m, r); }, });
        };
        fn.openSync = (k, s, m, r = this.r, opts) => {
            // Like syncStream, but for continuous/unbounded emission — no known chunk count up front. Adds a bounded in-flight window so a fast sender backs off instead of flooding a slow receiver.
            const cleanChannel = r.replace(/[\[\]]/g, "");
            const streamId = this.root.reg._sid(); //`${cleanChannel}-${m}-${Math.random().toString(36).slice(2)}`.replace(/\./g, "-");
            const windowSize = opts?.windowSize ?? 32;
            const unacked = new Map();
            let nextSeq = 0;
            let acknowledgedUpTo = -1;
            const sendQueue = [];
            let draining = false;
            const closeAckSub = this.sub((ackData) => {
                if (typeof ackData?.seq !== "number")
                    return;
                unacked.delete(ackData.seq);
                if (ackData.seq > acknowledgedUpTo)
                    acknowledgedUpTo = ackData.seq;
                drain(); // an ack frees a window slot — try sending more
            }, k, `sync.${streamId}:ack`, m, r);
            const closeRetransmitSub = this.sub(async (reqData) => {
                const missing = unacked.get(reqData?.seq);
                if (missing) {
                    missing.retries++;
                    await this.send.gt.hot({ streamId, seq: reqData.seq, data: missing.p, isRetry: true }, k, s, m, r);
                }
            }, k, `sync.${streamId}:retransmit`, m, r);
            const drain = async () => {
                if (draining)
                    return;
                draining = true;
                while (sendQueue.length > 0 && unacked.size < windowSize) {
                    const p = sendQueue.shift();
                    const seq = nextSeq++;
                    unacked.set(seq, { p, retries: 0 });
                    await this.send({ streamId, seq, data: p, isRetry: false }, k, s, m, r);
                }
                draining = false;
            };
            return {
                push: (p) => { sendQueue.push(p); drain(); },
                pendingCount: () => unacked.size + sendQueue.length,
                close: () => { closeAckSub(); closeRetransmitSub(); },
            };
        };
        fn.sync = (k, s = this.s, m = this.m, r = this.r, opts = {}) => {
            const windowSize = opts.windowSize ?? 8;
            const retransmitMs = opts.retransmitMs ?? 5000;
            const inFlight = new Map();
            let nextSeq = 0;
            let closed = false;
            self;
            const unsub = this.sub((data) => {
                const d = data;
                if (!d || typeof d.seq !== "number")
                    return;
                const o = inFlight.get(d.seq);
                if (o && !o.acked) {
                    o.acked = true;
                    if (o.timer)
                        clearTimeout(o.timer);
                    inFlight.delete(d.seq);
                }
            }, `${k}:syncAck`, s, m, r);
            const unsubData = this.sub((data) => {
                const d = data;
                if (d && d.sync === true && typeof d.seq === "number") {
                    this.send({ seq: d.seq }, `${k}:syncAck`, s, m, r);
                }
            }, k, s, m, r);
            const push = (p) => {
                if (closed)
                    throw new Error(`syncStream ${r}: push after close`);
                if (inFlight.size >= windowSize) {
                    throw new Error(`syncStream ${r}: window full (${windowSize})`);
                }
                const o = { seq: nextSeq++, p, acked: false, sentAt: Date.now() };
                inFlight.set(o.seq, o);
                const send = () => this.send({ sync: true, seq: o.seq, p: o.p }, k, s, m, r);
                send();
                const arm = () => { o.timer = setTimeout(() => { if (!o.acked) {
                    send();
                    arm();
                } }, retransmitMs); };
                arm();
            };
            const handle = {
                push,
                pendingCount: () => inFlight.size,
                close: () => {
                    closed = true;
                    for (const o of inFlight.values())
                        if (o.timer)
                            clearTimeout(o.timer);
                    unsub();
                    unsubData();
                },
            };
            return handle;
        };
        fn.hot = (key, fn, s = this.s, m = this.m, r = this.r) => {
            const channel = this.mesh_store[key];
            const streamLink = { fn, prev: channel.streamTail, next: undefined, };
            if (channel.streamTail !== undefined) {
                channel.streamTail.next = streamLink;
            }
            else {
                channel.streamHead = streamLink;
            }
            channel.streamTail = streamLink;
            return () => {
                const previous = streamLink.prev;
                const next = streamLink.next;
                if (next !== undefined)
                    next.prev = previous;
                else
                    channel.streamTail = previous;
                if (previous !== undefined)
                    previous.next = next;
                else
                    channel.streamHead = next;
            };
        };
        return fn;
    })();
    handler = (() => {
        const fn = (handler, k = this.k, s = '_globalThis', m = this.m, r = this.r, binary = false) => {
            return this.sub(async (inboundReq) => {
                if (!inboundReq || !inboundReq.streamId)
                    return;
                const streamId = inboundReq.streamId;
                let isCancelled = false;
                let closeControlSubscription = null;
                try { // 1. Await the control stream cancellation listener safely
                    closeControlSubscription = this.sub(() => { isCancelled = true; }, `stream.${streamId}:cancel`, s, m, r);
                    const result = await handler(inboundReq);
                    let data = result;
                    if (result && typeof result === "object" && "ok" in result && "data" in result) {
                        data = result.data;
                    }
                    if (typeof data === "function") {
                        data = data();
                    }
                    if (data && typeof data[Symbol.asyncIterator] === "function") { // Core Async Generator Stream Loop
                        for await (const chunk of data) {
                            if (isCancelled) {
                                if (typeof data.return === "function") {
                                    await data.return();
                                }
                                break;
                            }
                            await this.send({ type: "chunk", data: chunk }, k, `stream.${streamId}`, m, r);
                        }
                        if (!isCancelled)
                            await this.send({ type: "done" }, k, `stream.${streamId}`, m, r);
                        if (closeControlSubscription)
                            closeControlSubscription();
                        return;
                    } // Sync Generator Stream Loop
                    if (data && typeof data[Symbol.iterator] === "function" && !Array.isArray(data)) {
                        for (const chunk of data) {
                            if (isCancelled)
                                break;
                            await this.send({ type: "chunk", data: chunk }, k, `stream.${streamId}`, m, r);
                        }
                        if (!isCancelled)
                            await this.send({ type: "done" }, k, `stream.${streamId}`, m, r);
                        if (closeControlSubscription)
                            closeControlSubscription();
                        return;
                    } // Array Handling Stream Loop
                    if (Array.isArray(data)) {
                        for (const chunk of data) {
                            if (isCancelled)
                                break;
                            await this.send({ type: "chunk", data: chunk }, k, `stream.${streamId}`, m, r);
                        }
                        if (!isCancelled)
                            await this.send({ type: "done" }, k, `stream.${streamId}`, m, r);
                        if (closeControlSubscription)
                            closeControlSubscription();
                        return;
                    } // Fallback Single Value Response
                    if (!isCancelled) {
                        await this.send({ type: "chunk", data: data }, k, `stream.${streamId}`, m, r);
                        await this.send({ type: "done" }, k, `stream.${streamId}`, m, r);
                    }
                    if (closeControlSubscription)
                        closeControlSubscription();
                }
                catch (err) {
                    if (closeControlSubscription)
                        closeControlSubscription();
                    //SE.deadLetters.push({id: streamId, r: r || this.r, p: inboundReq, ts: Date.now(), error: err.message});
                    this.send({ type: "error", error: err.message }, k, `stream.${streamId}`, m, r);
                }
            }, k, s, m, r);
        };
        fn.native = (k, s = '_globalThis', m = this.m, r = this.r) => {
            let unsubscribe = null;
            return new ReadableStream({
                start: (controller) => { unsubscribe = this.sub((data) => { try {
                    controller.enqueue(data);
                }
                catch { } }, k, s, m, r); },
                cancel: () => { if (unsubscribe) {
                    unsubscribe();
                    unsubscribe = null;
                } },
            });
        };
        fn.openSync = (handler, k, s = '_globalThis', m = this.m, r = this.r) => {
            const expectedSeqMap = new Map();
            const bufferMap = new Map();
            return this.sub(async (inbound) => {
                if (!inbound?.streamId)
                    return;
                const { streamId, seq, data } = inbound;
                if (!expectedSeqMap.has(streamId)) {
                    expectedSeqMap.set(streamId, 0);
                    bufferMap.set(streamId, new Map());
                }
                const expectedSeq = expectedSeqMap.get(streamId);
                const buffer = bufferMap.get(streamId);
                if (seq === expectedSeq) {
                    await handler(data, seq);
                    expectedSeqMap.set(streamId, expectedSeq + 1);
                    await this.send({ seq }, k, `sync.${streamId}:ack`, m, r);
                    let next = expectedSeq + 1;
                    while (buffer.has(next)) {
                        await handler(buffer.get(next), next);
                        buffer.delete(next);
                        next++;
                        expectedSeqMap.set(streamId, next);
                        await this.send({ seq: next - 1 }, k, `sync.${streamId}:ack`, m, r);
                    }
                }
                else if (seq > expectedSeq) {
                    buffer.set(seq, data);
                    for (let missing = expectedSeq; missing < seq; missing++) {
                        if (!buffer.has(missing)) {
                            await this.send({ seq: missing }, k, `sync.${streamId}:retransmit`, m, r);
                        }
                    }
                }
                else {
                    await this.send({ seq }, k, `sync.${streamId}:ack`, m, r); // duplicate — re-ack, no reprocess
                }
            }, k, s, m, r);
        };
        fn.sync = (k, s = '_globalThis', m = this.m, r = this.r) => {
            const seqs = new Map();
            const waiters = new Set();
            const completes = new Set();
            let nextDeliver = 0;
            let done = false;
            const deliver = () => {
                while (seqs.has(nextDeliver)) {
                    const p = seqs.get(nextDeliver);
                    seqs.delete(nextDeliver);
                    for (const cb of [...waiters]) {
                        try {
                            cb(p, nextDeliver);
                        }
                        catch {
                            /* Consumable errors isolated */
                        }
                    }
                    nextDeliver++;
                }
            };
            const unsub = this.sub((data) => {
                const d = data;
                if (!d || d.sync !== true)
                    return;
                if (d.complete === true) {
                    done = true;
                    for (const cb of [...completes]) {
                        try {
                            cb();
                        }
                        catch {
                            /* ignore */
                        }
                    }
                    return;
                }
                if (typeof d.seq !== "number")
                    return;
                if (d.seq >= nextDeliver && !seqs.has(d.seq)) {
                    seqs.set(d.seq, d.p);
                    deliver();
                }
            }, k, s, m, r);
            return {
                onOrdered(cb) {
                    waiters.add(cb);
                },
                onComplete(cb) {
                    completes.add(cb);
                },
                close() {
                    unsub();
                    waiters.clear();
                    completes.clear();
                },
            };
        };
        return fn;
    })();
    duplex = (() => {
        const fn = async (k, s = '_globalThis', m = this.m, r = this.r) => {
            const hub = { k, s, m, r };
            return new this.duplexPlus(this, this, { k: k, s: s, m: m, r: r });
        };
        fn.stream = (readConfig, writeConfig) => {
            return {
                readable: this.handler.native(readConfig.k, readConfig.sl, readConfig.ms, readConfig.rt),
                writable: this.stream.native(writeConfig.k, writeConfig.sl, writeConfig.ms, writeConfig.rt),
            };
        };
        fn.sx = (k, s, m, r, opts) => {
            const windowSize = opts?.windowSize ?? 32;
            const up = this.stream.openSync(k, `${s}:up`, m, r, { windowSize });
            const downHandlers = new Set();
            const errorHandlers = new Set();
            const closeHandlers = new Set();
            let upClosed = false;
            let downClosed = false;
            let fatallyErrored = false;
            let lastSeqSeen = -1;
            const down = this.handler.openSync((chunk, seq) => {
                if (fatallyErrored)
                    return;
                if (lastSeqSeen !== -1 && seq !== lastSeqSeen + 1) {
                    reportError({ dir: "down", message: `sequence gap: expected ${lastSeqSeen + 1}, got ${seq}`, fatal: false });
                }
                lastSeqSeen = seq;
                downHandlers.forEach(h => h(chunk, seq));
            }, k, `${s}:down`, m, r);
            // remote-reported errors arrive here — this is what makes error propagation BILATERAL,
            // not just local: the other side explicitly tells us when something broke on their end
            const errorSub = this.sub((err) => {
                errorHandlers.forEach(h => h(err));
                if (err.fatal) {
                    fatallyErrored = true;
                    teardown();
                }
            }, k, `${s}:error`, m, r);
            // remote-reported half-close — lets THIS side know the peer will send no more, so onData
            // consumers can know to expect a natural end rather than silence-as-ambiguity
            const remoteCloseSub = this.sub((dir) => {
                if (dir === "up")
                    downClosed = true; // peer's up = our down, from our perspective
                closeHandlers.forEach(h => h());
            }, k, `${s}:remote-close`, m, r);
            const reportError = (err) => {
                errorHandlers.forEach(h => h(err));
                this.send(err, k, `${s}:error`, m, r); // propagate to the OTHER side too — bilateral, not local-only
                if (err.fatal) {
                    fatallyErrored = true;
                    teardown();
                }
            };
            let tornDown = false;
            const teardown = () => {
                if (tornDown)
                    return;
                tornDown = true;
                up.close();
                down();
                errorSub();
                remoteCloseSub();
                downHandlers.clear();
                errorHandlers.clear();
                closeHandlers.clear();
            };
            return {
                send: (p) => {
                    if (upClosed) {
                        reportError({ dir: "up", message: "send after half-close", fatal: false, retryable: false, code: "HALF_CLOSED" });
                        return;
                    }
                    if (fatallyErrored)
                        return;
                    try {
                        up.push(p);
                    }
                    catch (err) {
                        reportError({ dir: "up", message: err.message ?? String(err), fatal: false, retryable: true, code: "SEND_FAILED" });
                    }
                },
                onData: (handler) => {
                    downHandlers.add(handler);
                    return () => downHandlers.delete(handler);
                },
                onError: (handler) => {
                    errorHandlers.add(handler);
                    return () => errorHandlers.delete(handler);
                },
                onRemoteClose: (handler) => {
                    closeHandlers.add(handler);
                    return () => closeHandlers.delete(handler);
                },
                /** Half-close: stop sending, but keep receiving. Peer is notified so they know THIS side is done sending. */
                closeUp: () => {
                    if (upClosed)
                        return;
                    up.close();
                    upClosed = true;
                    this.send("up", k, `${s}:remote-close`, m, r);
                },
                /** Full close of both directions, notifies peer, tears down liveness tracking. */
                close: () => {
                    if (!upClosed)
                        this.send("up", k, `${s}:remote-close`, m, r);
                    upClosed = true;
                    downClosed = true;
                    teardown();
                },
                isFullyClosed: () => upClosed && downClosed,
                isFatallyErrored: () => fatallyErrored,
            };
        };
        fn.async = (k = this.k, s = this.s, m = this.m, r = this.r, opts) => {
            const windowSize = opts?.windowSize ?? 32;
            const up = this.stream.sync(k, `${s}:up`, m, r, { windowSize });
            const downHandlers = new Set();
            const errorHandlers = new Set();
            const closeHandlers = new Set();
            let upClosed = false;
            let downClosed = false;
            let fatallyErrored = false;
            let tornDown = false;
            let lastSeqSeen = -1;
            let downUnsub;
            let errorUnsub;
            let remoteCloseUnsub;
            const down = () => {
                const stream = this.handler.sync(k, `${s}:down`, m, r); // Accept p as unknown, then cast to TDown
                stream.onOrdered((p, seq) => {
                    if (fatallyErrored)
                        return;
                    const chunk = p;
                    if (lastSeqSeen !== -1 && seq !== lastSeqSeen + 1) {
                        reportError({ dir: "down", message: `sequence gap: expected ${lastSeqSeen + 1}, got ${seq}`, fatal: false });
                    }
                    lastSeqSeen = seq;
                    downHandlers.forEach(h => h(chunk, seq));
                });
                downUnsub = () => stream.close();
            };
            const errorSub = () => {
                errorUnsub = this.sub((err) => {
                    errorHandlers.forEach(h => h(err));
                    if (err.fatal) {
                        fatallyErrored = true;
                        teardown();
                    }
                }, k, `${m}:error`, m, r);
            };
            const remoteCloseSub = () => {
                remoteCloseUnsub = this.sub((dir) => {
                    if (dir === "up")
                        downClosed = true;
                    closeHandlers.forEach(h => h());
                }, k, `${s}:remote-close`, m, r);
            };
            const reportError = (err) => {
                errorHandlers.forEach(h => h(err));
                this.send(err, k, `${s}:error`, m, r);
                if (err.fatal) {
                    fatallyErrored = true;
                    teardown();
                }
            };
            const teardown = () => {
                if (tornDown)
                    return;
                tornDown = true;
                up.close();
                downUnsub?.();
                errorUnsub?.();
                remoteCloseUnsub?.();
                downHandlers.clear();
                errorHandlers.clear();
                closeHandlers.clear();
            };
            down();
            errorSub();
            remoteCloseSub();
            const dx = {
                send: (p) => {
                    if (upClosed) {
                        reportError({ dir: "up", message: "send after half-close", fatal: false, retryable: false, code: "HALF_CLOSED" });
                        return;
                    }
                    if (fatallyErrored)
                        return;
                    try {
                        up.push(p);
                    }
                    catch (err) {
                        reportError({ dir: "up", message: err.message ?? String(err), fatal: false, retryable: true, code: "SEND_FAILED" });
                    }
                },
                onData: (handler) => {
                    downHandlers.add(handler);
                    return () => downHandlers.delete(handler);
                },
                onError: (handler) => {
                    errorHandlers.add(handler);
                    return () => errorHandlers.delete(handler);
                },
                onRemoteClose: (handler) => {
                    closeHandlers.add(handler);
                    return () => closeHandlers.delete(handler);
                },
                closeUp: () => {
                    if (upClosed)
                        return;
                    up.close();
                    upClosed = true;
                    this.send("up", k, `${s}:remote-close`, m, r);
                },
                close: () => {
                    if (!upClosed) {
                        this.send("up", k, `${s}:remote-close`, m, r);
                    }
                    upClosed = true;
                    downClosed = true;
                    teardown();
                },
                isFullyClosed: () => upClosed && downClosed,
                isFatallyErrored: () => fatallyErrored,
            };
            return dx;
        };
        fn.open = (opts = { highWaterMark: 32 }, k, s = this.s, m = this.m, r = this.r) => {
            const streamId = this.root.reg._sid();
            const highWaterMark = opts?.highWaterMark ?? 32;
            const queue = [];
            const handlers = new Set();
            const drainHandlers = new Set();
            const closeHandlers = new Set();
            const errorHandlers = new Set();
            let writable = true;
            let readable = true;
            let draining = false;
            const write = async (chunk) => {
                if (!writable) {
                    throw new Error("Stream is not writable");
                }
                queue.push(chunk);
                if (queue.length >= highWaterMark) {
                    await new Promise(resolve => {
                        const check = () => {
                            if (queue.length < highWaterMark) {
                                drainHandlers.delete(check);
                                resolve();
                            }
                        };
                        drainHandlers.add(check);
                        check();
                    });
                }
                await drain();
            };
            const drain = async () => {
                if (draining)
                    return;
                draining = true;
                try {
                    while (queue.length > 0 && writable) {
                        const chunk = queue.shift();
                        await this.send({ streamId, data: chunk }, k, s, m, r);
                        drainHandlers.forEach(handler => handler());
                    }
                }
                finally {
                    draining = false;
                }
            };
            const unsubscribe = this.sub(async (packet) => {
                if (packet?.streamId !== streamId)
                    return;
                if (packet.type === "data") {
                    handlers.forEach(handler => { handler(packet.data); });
                }
                if (packet.type === "close") {
                    readable = false;
                    closeHandlers.forEach(handler => handler());
                }
                if (packet.type === "error") {
                    errorHandlers.forEach(handler => { handler(packet.error); });
                }
            }, k, s, m, r);
            return {
                write,
                onData: (handler) => {
                    handlers.add(handler);
                    return () => { handlers.delete(handler); };
                },
                onDrain: (handler) => {
                    drainHandlers.add(handler);
                    return () => { drainHandlers.delete(handler); };
                },
                onClose: (handler) => {
                    closeHandlers.add(handler);
                    return () => { closeHandlers.delete(handler); };
                },
                onError: (handler) => {
                    errorHandlers.add(handler);
                    return () => { errorHandlers.delete(handler); };
                },
                close: async () => {
                    if (!writable && !readable)
                        return;
                    writable = false;
                    await this.send({ streamId, type: "close" }, k, s, m, r);
                    unsubscribe();
                    closeHandlers.forEach(handler => handler());
                },
                abort: async (reason) => {
                    writable = false;
                    readable = false;
                    await this.send({ streamId, type: "error", error: reason }, k, s, m, r);
                    unsubscribe();
                    errorHandlers.forEach(handler => handler(reason));
                },
                writable: () => writable,
                readable: () => readable,
                buffered: () => queue.length,
            };
        };
        return fn;
    })();
    d_handler = (() => {
        const fn = () => { };
        fn.sx = (k, s, m, r, opts) => {
            const windowSize = opts?.windowSize ?? 32;
            const down = this.stream.openSync(k, `${s}:down`, m, r, { windowSize });
            const upHandlers = new Set();
            const errorHandlers = new Set();
            const closeHandlers = new Set();
            let downClosed = false;
            let upClosed = false;
            let fatallyErrored = false;
            let lastSeqSeen = -1;
            const up = this.handler.openSync((chunk, seq) => {
                if (fatallyErrored)
                    return;
                if (lastSeqSeen !== -1 && seq !== lastSeqSeen + 1) {
                    reportError({ dir: "down", message: `sequence gap: expected ${lastSeqSeen + 1}, got ${seq}`, fatal: false });
                }
                lastSeqSeen = seq;
                upHandlers.forEach(h => h(chunk, seq));
            }, k, `${s}:up`, m, r);
            const errorSub = this.sub((err) => {
                errorHandlers.forEach(h => h(err));
                if (err.fatal) {
                    fatallyErrored = true;
                    teardown();
                }
            }, k, `${s}:error`, m, r);
            const remoteCloseSub = this.sub((dir) => {
                if (dir === "up")
                    upClosed = true; // peer's up = our up, from THIS side's receiving perspective
                closeHandlers.forEach(h => h());
            }, k, `${s}:remote-close`, m, r);
            const reportError = (err) => {
                errorHandlers.forEach(h => h(err));
                this.send(err, k, `${s}:error`, m, r);
                if (err.fatal) {
                    fatallyErrored = true;
                    teardown();
                }
            };
            let tornDown = false;
            const teardown = () => {
                if (tornDown)
                    return;
                tornDown = true;
                down.close();
                up();
                errorSub();
                remoteCloseSub();
                upHandlers.clear();
                errorHandlers.clear();
                closeHandlers.clear();
            };
            return {
                send: (p) => {
                    if (downClosed) {
                        reportError({ dir: "down", message: "send after half-close", fatal: false, retryable: false, code: "HALF_CLOSED" });
                        return;
                    }
                    if (fatallyErrored)
                        return;
                    try {
                        down.push(p);
                    }
                    catch (err) {
                        reportError({ dir: "down", message: err.message ?? String(err), fatal: false, retryable: true, code: "SEND_FAILED" });
                    }
                },
                onData: (handler) => {
                    upHandlers.add(handler);
                    return () => upHandlers.delete(handler);
                },
                onError: (handler) => {
                    errorHandlers.add(handler);
                    return () => errorHandlers.delete(handler);
                },
                onRemoteClose: (handler) => {
                    closeHandlers.add(handler);
                    return () => closeHandlers.delete(handler);
                },
                closeDown: () => {
                    if (downClosed)
                        return;
                    down.close();
                    downClosed = true;
                    this.send("down", k, `${s}:remote-close`, m, r);
                },
                close: () => {
                    if (!downClosed)
                        this.send("down", k, `${s}:remote-close`, m, r);
                    downClosed = true;
                    upClosed = true;
                    teardown();
                },
                isFullyClosed: () => downClosed && upClosed,
                isFatallyErrored: () => fatallyErrored,
            };
        };
        return fn;
    })();
    duplexPlus = class duplex {
        r = "THE_WIZARD_ROOT_REG";
        m = "SAURON_MASTER_REG";
        s = "SAURON_SLAVE_REG";
        k = "__SAURON__";
        hub;
        abus;
        bbus;
        constructor(abus, bbus, hub = { k: "__SAURON__", s: "SAURON_SLAVE_REG", m: "SAURON_MASTER_REG", r: "THE_WIZARD_ROOT_REG", }) {
            this.r = hub.r;
            this.m = hub.m;
            this.s = hub.s;
            this.k = hub.k;
            this.abus = abus; //new MAREK(SITH);
            this.bbus = bbus; //new this.MAREK(SITH);
        }
        stream = (() => {
            const fn = (k, s = this.s, m = this.m, r = this.r) => {
                return {
                    send: (p, sendOpts) => this.abus.stream(k, s, m, r),
                    sub: (handler) => this.abus.handler(k, s, m, r),
                };
            };
            return fn;
        })();
        handler = (() => {
            const fn = (k, s = this.s, m = this.m, r = this.r) => {
                return {
                    send: (p, sendOpts) => this.abus.stream(k, s, m, r),
                    sub: (handler) => this.abus.handler(k, s, m, r),
                };
            };
            return fn;
        })();
    };
    g_bus = new (class G_BUS {
        /**
        Several peak runs followed by usage paths
        Peak:                                       1_228.50 M ops/sec
        Peak:                                       3_103.41 M ops/sec
        Peak:                                       2_370.05 M ops/sec
        Peak:                                       2_325.32 M ops/sec
        These runs were all

        FELKOR2.send
        sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
        12    |   1146.44      |   1146.44    |   1146.44   |    100.0
        
        FELKOR2.stream
        sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
        12    |   1319.94      |   1319.94    |   1319.94   |    100.0
        PS F:\mq\src> bun run aa.ts
        
        FELKOR2.iterator.publish
        sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
        12    |     540.61     |   540.61     |  540.61     |    100.3
        PS F:\mq\src> bun run aa.ts
        
        FELKOR2.duplex
        sec | M writes/s | best M/s | avg M/s | CPU %
        12 |     898.32 |   898.32 |  898.32 | 100.0
        */
        s;
        m;
        r;
        k;
        channels;
        hot_path_test;
        constructor() { }
        // const msg: SIG_BUS_Message1 = { r, m, s, k, p: { p }, ts: Date.now(), _ack: false, };
        // const result = handler(p, msg);
        send(p, k, s = this.s, m = this.m, r = this.r) {
            const channel = this.channels[k];
            const handler = channel?.handler;
            if (!handler)
                return undefined;
            return handler(p, { r, m, s, k, p: p });
        }
        hot_send(p, k, path) {
            return this.channels[k]?.handler({ p: p });
        }
        sendStream(p, k, s = this.s, m = this.m, r = this.r) {
            const channel = this.channels[k];
            const handler = channel?.handler;
            if (!handler)
                return undefined;
            const msg = { r, m, s, k, p: p, _ack: false };
            const result = handler(p, msg);
            const listeners = channel.push;
            if (listeners) {
                for (const listener of listeners)
                    listener(msg);
            }
            return result;
        }
        sub(handler, k, s = this.s, m = this.m, r = this.r) {
            const channel = (this.channels[k] ??= {});
            channel.handler = handler;
            return () => {
                if (channel.handler === handler) {
                    channel.handler = undefined;
                }
            };
        }
        stream(handlerOrSlave, k, s = this.s, m = this.m, r = this.r) {
            const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
            const global = globalThis;
            const channel = (this.channels[k] ??= {});
            if (!channel)
                throw new Error("Channel does not exist");
            const listeners = (channel.push ??= new Set());
            if (handler) {
                listeners.add(handler);
                return () => {
                    listeners.delete(handler);
                    if (!listeners.size)
                        channel.push = undefined;
                };
            }
            const queue = [];
            const waiters = [];
            let offset = 0;
            let closed = false;
            const listener = (msg) => {
                if (waiters.length) {
                    waiters.shift().resolve({ value: msg, done: false });
                }
                else {
                    queue.push(msg);
                }
            };
            listeners.add(listener);
            const close = () => {
                if (closed)
                    return;
                closed = true;
                listeners.delete(listener);
                if (!listeners.size)
                    channel.push = undefined;
                for (const waiter of waiters) {
                    waiter.resolve({ value: undefined, done: true });
                }
                waiters.length = 0;
                queue.length = 0;
                offset = 0;
            };
            const iterator = {
                next: () => {
                    if (offset < queue.length) {
                        const value = queue[offset++];
                        if (offset === queue.length) {
                            queue.length = 0;
                            offset = 0;
                        }
                        return Promise.resolve({ value, done: false });
                    }
                    if (closed) {
                        return Promise.resolve({ value: undefined, done: true });
                    }
                    return new Promise((resolve) => waiters.push({ resolve }));
                },
                return: async () => {
                    close();
                    return { value: undefined, done: true };
                },
                [Symbol.asyncIterator]() {
                    return iterator;
                },
            };
            return iterator;
        }
        duplex(k, s = this.s, m = this.m, r = this.r) {
            return {
                send: (p) => this.sendStream(p, k, s, m, r),
                sub: (handler) => this.sub(handler, k, s, m, r),
                stream: (handler) => (handler ? this.stream(handler, k, s, m, r) : this.stream(k, s, m, r)),
            };
        }
    })();
    drs = new (class DRS {
        r = "THE_WIZARD_ROOT_REG";
        m = "SAURON_MASTER_REG";
        s = "SAURON_SLAVE_REG";
        k = "__SAURON__";
        root;
        hub;
        state;
        bus;
        constructor() { }
        state_setter(v, k = `_USECELL`, s = this.s, m = this.m, r = this.r) {
            const existing = this.hub.root.state.state.get(k, { s: s, m: m, r: r });
            if (existing !== undefined)
                return existing;
            const t = UF(v);
            this.hub.root.state.state.set(k, t, { triggerRender: false, s: s, m: m, r: r });
            return t;
        }
        cell = (() => {
            const fn = (k, s = this.s, m = this.m, r = this.r, v) => {
                const [value, setValue] = darkState(() => { return this.state_setter(v, k, s, m, r); });
                this.bus.sub((i) => {
                    const incoming = i.p;
                    this.hub.root.state.set(k, incoming, false, s, m, r);
                }, k, s, m, r);
                this.hub.root.state.sub(k, (newValue) => setValue(newValue), s, m, r);
                return value; // still no setter — this one was never meant to write
            };
            fn.send = async (p, k, s = this.s, m = this.m, r = this.r) => {
                const msgId = this.hub.root.reg._sid();
                const msg = { id: msgId, r: r, m: m, s: s, k: k, p: { p }, _ack: false };
                await this.root.mesh.DISPATCH({ id: msgId, r: r, m: m, s: s, k: k, p: { p }, _ack: false });
                const rr = this.cell(`${k}${this.root.RTRN_TO_SDR}`, s, m, r);
                await this.root.mesh.BACK_END.run.afterSend(msg, rr);
                return rr;
                //if (!(await this.BACK_END.run.beforeSend(msg))) return { success: false, blocked: true }
                // ── TRANSPORT SELECTION — most specific live boundary at this address ──
                // const boundary = this.boundaries.select(msg)
                // ── 'same-app' (or no subs) — normal path, unchanged ──
                /**  if (!this.online || this.BACK_END.SHOULD_BACKPRESSURE()) {
                    await this.DISPATCH.enqueue(msg)
                    log(`[SIGNALS][ENCRPYTION][CONFIGURE]`, { type: 'sendsub', r: r, m: m, s: s, k: k, id: msgId, p: msg, ttl: ttl, _ack: 'UNKNOWN', _adapter: 'UNKNOWN', priority: this.defaultPriority, tags: 'UNKNOWN', streamId: 'UNKNOWN', token: 'UNKNOWN', binary: binary, error: 'NONE', adapter_id: 'UNKNOWN', detail: '[!this.online || this.shouldBackpressure()]' }, t.deep)
                    return { success: false, queued: true }
                } */
            };
            fn.cell = (k, s = this.s, m = this.m, r = this.r, type = "cell", chain, v) => {
                const cellKey = `${k}:${this.root.RTRN_TO_SDR}`;
                const [value, setValue] = darkState(() => { return this.state_setter(v, k, s, m, r); });
                this.bus.sub((i) => {
                    const incoming = i.p;
                    setDark(k, incoming, false, s, m, r);
                }, k, s, m, r);
                subDark((newValue) => setValue(newValue), k, s, m, r);
                let rr;
                if (type === "waitFor") {
                    this.bus.send({ cmd: "waitFor", k, s, m, r }, k, s, m, r);
                    this.bus.sub((m) => setValue(m.p), cellKey, s, m, r);
                }
                else if (type === "sub") {
                    this.bus.send({ cmd: "subscribe", k, s, m, r }, k, s, m, r);
                    this.bus.sub((m) => setValue(m.p), cellKey, s, m, r);
                }
                else if (type === "cell") {
                    r = this.cell(cellKey, s, m, r);
                }
                darkEffect(() => {
                    if (rr)
                        setValue(rr);
                }, [rr]);
                return MCR_Combo(value, chain); // still no setter — this one was never meant to write
            };
            fn.crdt = (k = this.k, s = this.s, m = this.m, r = this.r, chain, v) => {
                const [value, setValue] = darkState(() => { return this.state_setter(v, k, s, m, r); });
                this.bus.sub((i) => {
                    const incoming = i.p;
                    setDark(k, incoming, true, s, m, r);
                }, k, s, m, r);
                subDark((newValue) => setValue(newValue), k, s, m, r);
                return MCR_Combo(value, chain); // still no setter — this one was never meant to write
            };
            return fn;
        })();
        useCell = (() => {
            const fn = (v, k = `_USECELL`, s = this.s, m = this.m, r = this.r, type = "none", options = {}, __sauron_id__ = this.hub.root.reg._sid()) => {
                const cellKey = `${k}:${this.root.RTRN_TO_SDR}`;
                const [state, setState] = darkState(() => { return this.state_setter(v, k, s, m, r); });
                const senderTokenRef = this.root.signals.signals.atom(this.root.reg._sid()); // this.root._id.b();
                if (type === "none") {
                    this.bus.sub((i) => {
                        const incoming = i.p;
                        if (incoming?.from === senderTokenRef.value)
                            return;
                        this.root.state.set(k, incoming.value, false, s, m, r);
                        setState(incoming.value);
                    }, k, s, m, r); // stay in sync with local writes too (including our own setLive below)
                    sub((newValue) => setState(newValue), k, s, m, r);
                }
                const setLive = (next) => {
                    const newValue = typeof next === "function" ? next(getDark(k, s, m, r)) : next;
                    setState(newValue);
                    setDark(k, newValue, true, s, m, r); // local write
                    this.bus.send({ value: newValue, from: senderTokenRef.value }, k, s, m, r); // broadcast to the bus
                };
                let rr;
                if (type === "waitFor") {
                    this.bus.send({ cmd: "waitFor", k: cellKey, s, m, r }, k, s, m, r);
                    this.bus.sub((m) => setLive(m), cellKey, s, m, r);
                }
                else if (type === "sub") {
                    this.bus.send({ cmd: "subscribe", k: cellKey, s, m, r }, k, s, m, r);
                    this.bus.sub((m) => setLive(m), cellKey, s, m, r);
                }
                else if (type === "cell") {
                    rr = this.cell(cellKey, s, m, r);
                    darkEffect(() => {
                        if (rr !== undefined) {
                            setDark(k, rr, false, s, m, r);
                            setState(rr);
                        }
                    }, [rr]);
                }
                return [state, setLive];
            };
            fn.obj = (v = {}, k = `_USECELL_OBJ`, s = this.s, m = this.m, r = this.r) => {
                const [state, setLive] = fn(v, k, s, m, r);
                const value = state && typeof state === "object" && !Array.isArray(state) ? state : v;
                const actions = {
                    get(key) { return value[key]; },
                    set(key, next) {
                        setLive((previous) => {
                            const current = previous && typeof previous === "object" && !Array.isArray(previous) ? previous : v;
                            if (Object.is(current[key], next)) {
                                return current;
                            }
                            return { ...current, [key]: next };
                        });
                    },
                    delete(key) {
                        setLive((previous) => {
                            const current = previous && typeof previous === "object" && !Array.isArray(previous) ? previous : v;
                            if (!(key in current))
                                return current;
                            const next = { ...current };
                            delete next[key];
                            return next;
                        });
                    },
                    merge(next) {
                        setLive((previous) => {
                            const current = previous && typeof previous === "object" && !Array.isArray(previous) ? previous : v;
                            return { ...current, ...next };
                        });
                    },
                    patch(key, updater) {
                        setLive((previous) => {
                            const current = previous && typeof previous === "object" && !Array.isArray(previous) ? previous : v;
                            return { ...current, [key]: updater(current[key]) };
                        });
                    },
                    pick(...keys) {
                        const next = {};
                        for (const key of keys) {
                            if (key in value) {
                                next[key] = value[key];
                            }
                        }
                        return next;
                    },
                    reset() { setLive(() => v); },
                };
                return [value, actions];
            };
            fn.mailbox = (v, k = `_USECELL_MAILBOX`, s = this.s, m = this.m, r = this.r, options = {}, __sauron_id__ = this.root.reg._sid()) => {
                return root.state.createHookCell("useCell.mailbox", __sauron_id__, () => ({
                    value: {
                        current: this.state_setter(v, k, s, m, r),
                        derived: undefined,
                    },
                }), (state, key, notify) => {
                    const st = state.value;
                    const senderToken = this.root.signals.signals.atom(this.root.reg._sid());
                    const queueRef = this.root.signals.signals.atom(null);
                    if (!queueRef.current) {
                        queueRef.current = new Queue(1);
                    }
                    const equals = options.equals ?? ((a, b) => a === b);
                    const selector = options.selector ?? null;
                    const derive = options.derive ?? null;
                    const getValue = () => (selector ? selector(st.current) : st.current);
                    const setValue = (next) => {
                        const prevSelected = getValue();
                        const nextSelected = selector ? selector(next) : next;
                        if (!equals(prevSelected, nextSelected)) {
                            st.current = next;
                            if (derive) {
                                st.derived = derive(next);
                            }
                            notify(key);
                        }
                        return next;
                    };
                    this.bus.sub((i) => {
                        const incoming = i.p;
                        if (incoming?.from === senderToken.value)
                            return;
                        queueRef.current.run(async () => {
                            const current = this.hub.root.state.state.get(k, { s: s, m: m, r: r });
                            const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
                            this.hub.root.state.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
                            return next;
                        });
                    }, k, s, m, r);
                    this.hub.root.state.state.subscribe(k, (newValue) => setValue(newValue), { s: s, m: m, r: r });
                    const setLive = (next) => {
                        const t = typeof next === "function" ? next() : next;
                        this.bus.send({ value: t, from: senderToken.value }, k, s, m, r);
                    };
                    if (derive && st.derived === undefined) {
                        st.derived = derive(st.current);
                    }
                    if (derive) {
                        return [getValue(), setLive, st.derived];
                    }
                    return [getValue(), setLive];
                });
            };
            return fn;
        })();
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        useAntiMsgQ = (() => {
            const fn = (v, k = `_USECELL`, s = this.s, m = this.m, r = this.r, options = {}, __sauron_id__ = this.root.reg._sid()) => {
                return root.state.createHookCell("useAntiMsgQ", __sauron_id__, () => ({
                    value: {
                        current: this.state_setter(v, k, s, m, r),
                        derived: undefined,
                    },
                }), (state, key, notify) => {
                    const st = state.value;
                    const senderToken = this.root.signals.signals.atom(this.root.reg._sid());
                    const equals = options.equals ?? ((a, b) => a === b);
                    const selector = options.selector ?? null;
                    const derive = options.derive ?? null;
                    const getValue = () => (selector ? selector(st.current) : st.current);
                    const setValue = (next) => {
                        const prevSelected = getValue();
                        const nextSelected = selector ? selector(next) : next;
                        if (!equals(prevSelected, nextSelected)) {
                            st.current = next;
                            if (derive) {
                                st.derived = derive(next);
                            }
                            notify(key);
                        }
                        return next;
                    };
                    this.bus.sub((i) => {
                        const incoming = i.p;
                        if (incoming?.from === senderToken.value)
                            return;
                        setDark(k, incoming.value, false, s, m, r);
                        setValue(incoming.value);
                    }, k, s, m, r);
                    subDark((newValue) => setValue(newValue), k, s, m, r);
                    const setLive = (next) => {
                        const newValue = typeof next === "function" ? next(getDark(k, s, m, r)) : next;
                        setValue(newValue);
                        setDark(k, newValue, true, s, m, r);
                        this.bus.send({ value: newValue, from: senderToken.value }, k, s, m, r);
                    };
                    if (derive && st.derived === undefined) {
                        st.derived = derive(st.current);
                    }
                    if (derive) {
                        return [getValue(), setLive, st.derived];
                    }
                    return [getValue(), setLive];
                });
            };
            fn.mailbox = (v, k = `_USECELL_MAILBOX`, s = this.s, m = this.m, r = this.r, options = {}, __sauron_id__ = this.hub.root.reg._sid()) => {
                const senderToken = this.hub.root.signals.signals.atom(this.hub.root.reg._sid());
                const queueRef = this.hub.root.signals.signals.atom(null);
                return root.state.createHookCell("useAntiMsgQ.mailbox", __sauron_id__, () => ({
                    value: {
                        current: this.state_setter(v, k, s, m, r),
                        derived: undefined,
                    },
                }), (state, key, notify) => {
                    const st = state.value;
                    if (!queueRef.current) {
                        queueRef.current = new Queue(1);
                    }
                    const equals = options.equals ?? ((a, b) => a === b);
                    const selector = options.selector ?? null;
                    const derive = options.derive ?? null;
                    const getValue = () => (selector ? selector(st.current) : st.current);
                    const setValue = (next) => {
                        const prevSelected = getValue();
                        const nextSelected = selector ? selector(next) : next;
                        if (!equals(prevSelected, nextSelected)) {
                            st.current = next;
                            if (derive) {
                                st.derived = derive(next);
                            }
                            notify(key);
                        }
                        return next;
                    };
                    this.bus.sub((i) => {
                        const incoming = i.p;
                        if (incoming?.from === senderToken.value)
                            return;
                        queueRef.current.run(async () => {
                            const current = this.hub.root.state.state.get(k, { s: s, m: m, r: r });
                            const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
                            this.hub.root.state.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
                            return next;
                        });
                    }, k, s, m, r);
                    this.hub.root.state.state.subscribe(k, (newValue) => setValue(newValue), { s: s, m: m, r: r });
                    const setLive = (next) => {
                        const t = typeof next === "function" ? next() : next;
                        this.hub.root.bus.send({ value: t, from: senderToken.value }, k, s, m, r);
                    };
                    if (derive && st.derived === undefined) {
                        st.derived = derive(st.current);
                    }
                    if (derive) {
                        return [getValue(), setLive, st.derived];
                    }
                    return [getValue(), setLive];
                });
            };
            return fn;
        })();
    })();
    spreadsheet = new (class SPREADSHEET_NGIN {
        r = "THE_WIZARD_ROOT_REG";
        m = "SAURON_MASTER_REG";
        s = "SAURON_SLAVE_REG";
        k = "__SAURON__";
        root;
        hub;
        //                                                                                                                  SPREADSHEET
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        spreadsheet = (() => {
            const fn = (sheet, cell, s = this.s, m = this.m, r = this.r, chain) => MCR_Combo(this.hub.drs.cell(this.spreadsheet.envKey(sheet, cell), s, m, r)?.computed ?? 0, chain);
            fn.raw = (sheet, cell, s = this.s, m = this.m, r = this.r) => this.hub.drs.cell(this.spreadsheet.envKey(sheet, cell), s, m, r)?.raw;
            fn.range = (sheet, ref, s = this.s, m = this.m, r = this.r) => this.spreadsheet.rangeKeys(ref, sheet).map(row => row.map(k => this.hub.drs.cell(this.spreadsheet.envKeyOf(k), s, m, r)?.computed ?? 0));
            // HELPERS
            fn.HISTORY_CAP = 200;
            fn.envKey = (sheet, cell) => `${sheet}::${normalizeCellRef(cell)}`;
            fn.rangeKeys = (ref, sheet) => {
                const [a, b = a] = ref.split(":"); // "A1:B3" -> "A1", "B3" (single cell works too)
                const s = parseCellRef(a, sheet), e = parseCellRef(b, sheet);
                const [c1, c2] = [colToIndex(s.col), colToIndex(e.col)].sort((x, y) => x - y);
                const [r1, r2] = [s.row, e.row].sort((x, y) => x - y);
                const rows = [];
                for (let r = r1; r <= r2; r++) {
                    const row = [];
                    for (let c = c1; c <= c2; c++)
                        row.push(cellKey(indexToCol(c), r, s.sheet));
                    rows.push(row);
                }
                return rows;
            };
            // "sheet!B3" (graph key) -> "sheet::B3" (the state key where the envelope lives)
            fn.envKeyOf = (graphKey) => {
                const { sheet, col, row } = parseCellRef(graphKey);
                return `${sheet}::${col}${row}`;
            };
            // state key for "who reads this cell": "sheet::B3#dependents"
            fn.dk = (graphKey) => `${this.spreadsheet.envKeyOf(graphKey)}#dependents`;
            fn.vk = (sheet) => `${sheet}::#volatile`;
            fn.rk = (sheet) => `${sheet}::#ranges`; // [{ t: "sheet!B3", box }] range formulas reading this sheet
            fn.xk = (sheet) => `${sheet}::#extent`; // { r: lastUsedRow }
            // get() with the MaybePromise quirk removed: a missing key returns undefined, not a Promise
            fn.peek = (k, s, m, r) => {
                const v = getDark(k, s, m, r);
                return v instanceof Promise ? undefined : v;
            };
            // what a cell currently shows: computed if it's a formula, else its raw value
            fn.visible = (g, key) => g.computed.get(key) ?? g.cells.get(key)?.raw;
            // prepend "=" if missing
            fn.spreadsheetFormula = (expression) => (expression.startsWith("=") ? expression : `=${expression}`);
            fn.load = (g, seen, key, s, m, r) => {
                if (seen.has(key))
                    return false;
                seen.add(key);
                const env = this.spreadsheet.peek(this.spreadsheet.envKeyOf(key), s, m, r);
                if (env && env.raw !== undefined) {
                    const cell = { raw: env.raw };
                    if (isFormula(env.raw)) {
                        try {
                            cell.ast = qualifyAst(parseFormula(env.raw), parseCellRef(key).sheet);
                            cell.volatile = isVolatileAst(cell.ast);
                        }
                        catch { }
                        cell.computed = env.computed;
                        g.computed.set(key, env.computed);
                    }
                    g.cells.set(key, cell);
                    if (env.deps?.length)
                        g.deps.set(key, new Set(env.deps));
                    if (env.ranges?.length)
                        g.ranges.set(key, env.ranges);
                }
                const ds = this.spreadsheet.peek(this.spreadsheet.dk(key), s, m, r);
                if (ds?.length)
                    g.dependents.set(key, new Set(ds));
                return true;
            };
            fn.syncRangeIndex = (that, key, oldBoxes, newBoxes, s, m, r) => {
                for (const sh of new Set([...oldBoxes, ...newBoxes].map(b => b.sheet ?? ""))) {
                    const idx = (this.spreadsheet.peek(this.spreadsheet.rk(sh), s, m, r) ?? []).filter((e) => e.t !== key);
                    for (const box of newBoxes)
                        if ((box.sheet ?? "") === sh)
                            idx.push({ t: key, box });
                    that.writeCell(this.spreadsheet.rk(sh), idx, s, m, r);
                }
            };
            fn.loadSlice = (g, seen, roots, newDeps, s, m, r) => {
                const idx = new Map();
                const rangeDependents = (key) => {
                    const sheet = parseCellRef(key).sheet ?? "";
                    if (!idx.has(sheet))
                        idx.set(sheet, this.spreadsheet.peek(this.spreadsheet.rk(sheet), s, m, r) ?? []);
                    return idx
                        .get(sheet)
                        .filter(e => boxContains(e.box, key))
                        .map(e => e.t);
                };
                for (const r of roots)
                    this.spreadsheet.load(g, seen, r, s, m, r);
                for (const r of roots)
                    for (const d of g.deps.get(r) ?? [])
                        this.spreadsheet.load(g, seen, d, s, m, r); // old inputs
                for (const d of newDeps)
                    this.spreadsheet.load(g, seen, d, s, m, r); // new inputs
                const queued = new Set(roots); // separate from `seen`: a cell loaded as an input must still be queued as a dependent
                const queue = [...roots];
                for (let i = 0; i < queue.length; i++) {
                    const cur = queue[i];
                    for (const dep of new Set([...(g.dependents.get(cur) ?? []), ...rangeDependents(cur)])) {
                        this.spreadsheet.load(g, seen, dep, s, m, r);
                        if (queued.has(dep))
                            continue;
                        queued.add(dep);
                        queue.push(dep);
                        for (const input of g.deps.get(dep) ?? [])
                            this.spreadsheet.load(g, seen, input, s, m, r);
                    }
                }
            };
            fn.tick = (that, sheet, s, m, r) => {
                const vols = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
                if (!vols.length)
                    return;
                const { g, seen } = this.spreadsheet.openGraph(s, m, r);
                this.spreadsheet.loadSlice(g, seen, vols, [], s, m, r);
                this.spreadsheet.persist(that, g, sheet, recalculateDirty(g, vols, []), [], false, s, m, r);
            };
            fn.commit = (that, sheet, ref, raw, s, m, r) => {
                const key = normalizeCellRef(ref, sheet);
                let newDeps = new Set();
                if (isFormula(raw)) {
                    try {
                        newDeps = collectDependencies(qualifyAst(parseFormula(raw), sheet));
                    }
                    catch { }
                }
                const vols = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
                const { g, seen } = this.spreadsheet.openGraph(s, m, r);
                this.spreadsheet.loadSlice(g, seen, [key, ...vols], newDeps, s, m, r);
                const oldDeps = new Set(g.deps.get(key) ?? []);
                const changed = raw === undefined ? clearGraphCellInPlace(g, ref, sheet) : writeGraphCellInPlace(g, ref, raw, sheet);
                for (const k of changed) {
                    const env = { raw: g.cells.get(k)?.raw, computed: this.spreadsheet.visible(g, k) ?? 0, deps: [...(g.deps.get(k) ?? [])], t: Date.now() };
                    that.writeCell(this.spreadsheet.envKeyOf(k), env, s, m, r);
                    const h = this.spreadsheet.peek(`${this.spreadsheet.envKeyOf(k)}#h`, s, m, r) ?? [];
                    that.writeCell(`${this.spreadsheet.envKeyOf(k)}#h`, [...h, { raw: env.raw, computed: env.computed, t: env.t }].slice(-this.spreadsheet.HISTORY_CAP), s, m, r);
                }
                this.syncNumeric(g, changed);
                for (const d of new Set([...oldDeps, ...(g.deps.get(key) ?? [])])) {
                    that.writeCell(this.spreadsheet.dk(d), [...(g.dependents.get(d) ?? [])], s, m, r);
                }
            };
            fn.persist = (that, g, sheet, changed, depKeys, history, s, m, r) => {
                const rows = new Map();
                for (const k of changed) {
                    const cell = g.cells.get(k);
                    const env = { raw: cell?.raw, computed: this.spreadsheet.visible(g, k) ?? 0, deps: [...(g.deps.get(k) ?? [])], ranges: g.ranges.get(k) ?? [], volatile: !!cell?.volatile, t: Date.now() };
                    that.writeCell(this.spreadsheet.envKeyOf(k), env, s, m, r);
                    if (history && !env.volatile) {
                        const h = this.spreadsheet.peek(`${this.spreadsheet.envKeyOf(k)}#h`, s, m, r) ?? [];
                        that.writeCell(`${this.spreadsheet.envKeyOf(k)}#h`, [...h, { raw: env.raw, computed: env.computed, t: env.t }].slice(-this.spreadsheet.HISTORY_CAP), s, m, r);
                    }
                    if (cell) {
                        const p = parseCellRef(k), sh = p.sheet ?? "";
                        rows.set(sh, Math.max(rows.get(sh) ?? 0, p.row));
                    }
                }
                this.syncNumeric(g, changed);
                for (const d of depKeys)
                    that.writeCell(this.spreadsheet.dk(d), [...(g.dependents.get(d) ?? [])], s, m, r);
                const rt = r;
                for (const [sh, r] of rows) { // used-row extent per sheet (grow only)
                    if (r > (this.spreadsheet.peek(this.spreadsheet.xk(sh), s, m, rt)?.r ?? 0))
                        that.writeCell(this.spreadsheet.xk(sh), { r }, s, m, rt);
                }
                const before = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
                const vols = new Set(before);
                for (const k of changed)
                    g.cells.get(k)?.volatile ? vols.add(k) : vols.delete(k);
                if (vols.size !== before.length || before.some(k => !vols.has(k)))
                    that.writeCell(this.spreadsheet.vk(sheet), [...vols], s, m, r);
            };
            fn.openGraph = (s, m, r) => {
                const g = createSpreadsheetGraph(), seen = new Set();
                g.fetch = k => {
                    this.spreadsheet.load(g, seen, k, s, m, r);
                    return g.cells.get(k);
                }; // INDIRECT / OFFSET / range members
                g.extent = sheet => this.spreadsheet.peek(this.spreadsheet.xk(sheet ?? ""), s, m, r)?.r ?? 0;
                return { g, seen };
            };
            return fn;
        })();
        // #endregion
        //                                                                                                              USE SPREADSHEET
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        useSpreadsheet = (() => {
            const commit = (that, sheet, ref, raw, s, m, r) => {
                const key = normalizeCellRef(ref, sheet);
                let newDeps = new Set();
                if (isFormula(raw)) {
                    try {
                        newDeps = collectDependencies(qualifyAst(parseFormula(raw), sheet));
                    }
                    catch { }
                }
                const vols = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
                const { g, seen } = this.spreadsheet.openGraph(s, m, r);
                this.spreadsheet.loadSlice(g, seen, [key, ...vols], newDeps, s, m, r);
                const oldDeps = new Set(g.deps.get(key) ?? []);
                const changed = raw === undefined ? clearGraphCellInPlace(g, ref, sheet) : writeGraphCellInPlace(g, ref, raw, sheet);
                for (const k of changed) {
                    const env = { raw: g.cells.get(k)?.raw, computed: this.spreadsheet.visible(g, k) ?? 0, deps: [...(g.deps.get(k) ?? [])], t: Date.now() };
                    that.writeCell(this.spreadsheet.envKeyOf(k), env, s, m, r);
                    const h = this.spreadsheet.peek(`${this.spreadsheet.envKeyOf(k)}#h`, s, m, r) ?? [];
                    that.writeCell(`${this.spreadsheet.envKeyOf(k)}#h`, [...h, { raw: env.raw, computed: env.computed, t: env.t }].slice(-this.spreadsheet.HISTORY_CAP), s, m, r);
                }
                this.syncNumeric(g, changed);
                for (const d of new Set([...oldDeps, ...(g.deps.get(key) ?? [])])) {
                    that.writeCell(this.spreadsheet.dk(d), [...(g.dependents.get(d) ?? [])], s, m, r);
                }
            };
            const fn = (sheet, cell, v, s = this.s, m = this.m, r = this.r, type = "none", chain) => {
                const seed = v === undefined ? undefined : (() => {
                    commit(this, sheet, cell, typeof v === "function" ? v() : v, s, m, r);
                    return this.spreadsheet.peek(this.spreadsheet.envKey(sheet, cell), s, m, r);
                });
                const [getter, setter] = this.hub.drs.useCell(this.spreadsheet.envKey(sheet, cell), seed, s, m, r, type);
                const read = (ref) => this.spreadsheet.peek(this.spreadsheet.envKey(sheet, ref), s, m, r);
                const write = (ref, raw) => commit(this, sheet, ref, raw, s, m, r);
                return [getter, setter];
            };
            fn.grid = (sheet, cell, v, s = this.s, m = this.m, r = this.r, type = "none", chain) => {
                const seed = v === undefined ? undefined : (() => {
                    commit(this, sheet, cell, typeof v === "function" ? v() : v, s, m, r);
                    return this.spreadsheet.peek(this.spreadsheet.envKey(sheet, cell), s, m, r);
                });
                const [getter, setter] = this.hub.drs.useCell(seed, this.spreadsheet.envKey(sheet, cell), s, m, r, type);
                const read = (ref) => this.spreadsheet.peek(this.spreadsheet.envKey(sheet, ref), s, m, r);
                const write = (ref, raw) => commit(this, sheet, ref, raw, s, m, r);
                const rt = r;
                // opts: the bag of tricks
                const opts = {
                    sheet,
                    cell,
                    envelope: getter, //env, // { raw, computed, deps, t }
                    raw: getter, //env?.raw, // this cell's raw: "=SUM(B1:B2)" or literal
                    deps: getter, //env?.deps ?? [],
                    dependents: this.spreadsheet.peek(this.spreadsheet.dk(normalizeCellRef(cell, sheet)), s, m, r) ?? [],
                    get: (ref) => read(ref)?.computed, // any cell, computed
                    getRaw: (ref) => read(ref)?.raw,
                    set: write, // opts.set("B1", 500)
                    formula: (ref, expr) => write(ref, this.spreadsheet.spreadsheetFormula(expr)),
                    clear: (ref) => write(ref, undefined),
                    setMany: (edits) => {
                        for (const [r, raw] of edits)
                            write(r, raw);
                    },
                    fill: (anchor, block) => {
                        const a = parseCellRef(anchor, sheet);
                        block.forEach((row, ri) => row.forEach((raw, ci) => write(`${indexToCol(colToIndex(a.col) + ci)}${a.row + ri}`, raw)));
                    },
                    range: (ref) => this.spreadsheet.rangeKeys(ref, sheet).map(r => r.map(k => this.spreadsheet.peek(this.spreadsheet.envKeyOf(k), s, m, rt)?.computed ?? 0)),
                    rawRange: (ref) => this.spreadsheet.rangeKeys(ref, sheet).map(r => r.map(k => this.spreadsheet.peek(this.spreadsheet.envKeyOf(k), s, m, rt)?.raw)),
                    history: (ref = cell) => this.spreadsheet.peek(`${this.spreadsheet.envKey(sheet, ref)}#h`, s, m, r) ?? [],
                };
                return [getter, setter, opts];
            };
            return fn;
        })();
        // #endregion 
        //                                                                                                                  [CONSTANTS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        compile(ast) {
            const code = [];
            const consts = [];
            const ranges = [];
            let ok = true;
            const rangePids = (node) => {
                if (node.type === "CellRef")
                    return [this.pids.intern(cellKey(node.col, node.row, node.sheet))];
                if (node.type !== "Range")
                    return undefined;
                const c1 = colToIndex(node.start.col), c2 = colToIndex(node.end.col);
                const r1 = node.start.row, r2 = node.end.row;
                const out = [];
                for (let r = Math.min(r1, r2); r <= Math.max(r1, r2); r++)
                    for (let c = Math.min(c1, c2); c <= Math.max(c1, c2); c++)
                        out.push(this.pids.intern(cellKey(indexToCol(c), r, node.sheet)));
                return out;
            };
            const AGG = {
                SUM: 6 /* VM_OP.SUM_RANGE */, AVERAGE: 8 /* VM_OP.AVG_RANGE */,
                MIN: 9 /* VM_OP.MIN_RANGE */, MAX: 10 /* VM_OP.MAX_RANGE */, COUNT: 11 /* VM_OP.COUNT_RANGE */,
            };
            const emit = (node) => {
                if (!ok)
                    return;
                switch (node.type) {
                    case "Number":
                        code.push(0 /* VM_OP.PUSH_CONST */, consts.length);
                        consts.push(node.value);
                        return;
                    case "CellRef":
                        code.push(1 /* VM_OP.PUSH_CELL */, this.pids.intern(cellKey(node.col, node.row, node.sheet)));
                        return;
                    case "BinOp": {
                        const op = { "+": 2 /* VM_OP.ADD */, "-": 3 /* VM_OP.SUB */, "*": 4 /* VM_OP.MUL */, "/": 5 /* VM_OP.DIV */ }[node.op];
                        if (op === undefined) {
                            ok = false;
                            return;
                        } // comparisons, &, ^ — outside this VM
                        emit(node.left);
                        emit(node.right);
                        code.push(op, 0);
                        return;
                    }
                    case "FunctionCall": {
                        const opcode = node.args.length === 1 && AGG[node.name];
                        const pidList = opcode && rangePids(node.args[0]);
                        if (!opcode || !pidList) {
                            ok = false;
                            return;
                        } // VLOOKUP, text fns, SUMIF, lambdas, etc.
                        code.push(opcode, ranges.length);
                        ranges.push(Int32Array.from(pidList));
                        return;
                    }
                    default: ok = false;
                }
            };
            emit(ast);
            if (!ok)
                return undefined;
            code.push(7 /* VM_OP.RET */, 0);
            return { code: Int32Array.from(code), consts: Float64Array.from(consts), ranges };
        }
        capacity;
        mask;
        pidCapacity;
        schedule;
        shared;
        rings = [];
        CTRL = 16;
        ctrl;
        available = [];
        clock;
        phaseIndex;
        running;
        pidToSlot;
        dirty;
        f64;
        u32;
        i32;
        bytes;
        f64Tail = 0;
        u32Tail = 0;
        i32Tail = 0;
        byteTail = 0;
        maxBatch;
        wait;
        handlers = [];
        constructor() { }
        // #endregion
        //                                                                                                                   [HANDLERS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        sub(op, handler) {
            this.handlers[op] = handler;
            return this;
        }
        /*======================================================================
            MULTI-PRODUCER CLAIM / PUBLISH
            Claim the next sequence for a lane. This is a true multi-producer sequencer: - CAS reserves a unique sequence. - The availability buffer marks that exact sequence published. - Consumers never rely on a single global “highest published” value.
            ====================================================================== */
        ctrlAt(lane, field) { return lane * this.CTRL + field; }
        claim(lane) {
            const next = this.ctrlAt(lane, 0);
            const stall = this.ctrlAt(lane, 4);
            const claimed = this.ctrlAt(lane, 5);
            let spin = 0;
            for (;;) {
                const seq = Atomics.load(this.ctrl, next);
                const wrap = seq - this.capacity;
                const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
                if (gate > wrap) {
                    if (Atomics.compareExchange(this.ctrl, next, seq, seq + 1) === seq) {
                        Atomics.add(this.ctrl, claimed, 1);
                        return seq;
                    }
                    continue;
                }
                Atomics.add(this.ctrl, stall, 1);
                if (this.wait === "spin") {
                    spin++;
                    continue;
                }
                if (this.wait === "yield") {
                    this.yieldNow();
                    continue;
                }
                Atomics.wait(this.ctrl, next, seq, 1);
                spin = 0;
            }
        }
        // Publish a claimed record. The record must be fully written BEFORE this call. The availability buffer is the publication barrier.
        publish(lane, seq) {
            const avail = this.available[lane];
            Atomics.store(avail, seq & this.mask, seq + 1);
            const pub = this.ctrlAt(lane, 6);
            Atomics.add(this.ctrl, pub, 1);
            const waiting = this.ctrlAt(lane, 0);
            Atomics.notify(this.ctrl, waiting);
        }
        // One-shot enqueue. Returns: >= 0 — published sequence  -1  — nonblocking mode and lane was full
        emit(op, pid, slot, a = 0, b = 0, c = 0, ext = 0, block = true) {
            const lane = this.laneFor(op);
            const ring = this.rings[lane];
            if (!block && this.isFull(lane))
                return -1;
            const seq = this.claim(lane);
            const base = (seq & this.mask) * REC;
            const epoch = Atomics.load(this.clock, 0);
            ring[base + F_OP] = op;
            ring[base + F_PID] = pid;
            ring[base + F_SLOT] = slot;
            ring[base + F_A] = a;
            ring[base + F_B] = b;
            ring[base + F_C] = c;
            ring[base + F_EPOCH] = epoch;
            ring[base + F_EXT] = ext;
            this.publish(lane, seq);
            return seq;
        }
        // #endregion
        //                                                                                                         [PID / SLOT / DIRTY] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        bind(pid, slot) { Atomics.store(this.pidToSlot, pid, slot); }
        unbind(pid) { Atomics.store(this.pidToSlot, pid, -1); }
        slot(pid) { return Atomics.load(this.pidToSlot, pid); }
        dirtyMark(pid) { this.dirty[pid >>> 5] |= 1 << (pid & 31); }
        dirtyPeek(pid) { return (this.dirty[pid >>> 5] & (1 << (pid & 31))) !== 0; }
        dirtyTake(pid) {
            const w = pid >>> 5;
            const bit = 1 << (pid & 31);
            const word = this.dirty[w];
            if ((word & bit) === 0)
                return false;
            this.dirty[w] = word & ~bit;
            return true;
        }
        pids = new PidTable();
        ensureNumericSlot(pid) {
            const existing = this.slot(pid);
            if (existing >= 0)
                return existing;
            const s = this.allocF64Slot();
            this.bind(pid, s);
            return s;
        }
        releaseNumericSlot(pid) {
            const s = this.slot(pid);
            if (s < 0)
                return;
            this.unbind(pid);
            this.freeF64Slot(s);
        }
        syncNumeric(g, changed) {
            for (const k of changed) {
                const computed = this.spreadsheet.visible(g, k);
                const pid = this.pids.intern(k);
                if (typeof computed === "number" && Number.isFinite(computed)) {
                    this.storeComputed(this.ensureNumericSlot(pid), computed);
                }
                else {
                    this.releaseNumericSlot(pid);
                }
            }
        }
        // #endregion
        //                                                                                                                 [SOA ARENAS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        f64Free = [];
        f64Bits; // init in constructor, right after this.f64 is created:
        scratchF = new Float64Array(1);
        scratchI = new BigInt64Array(this.scratchF.buffer);
        allocF64Slot() {
            if (this.f64Free.length)
                return this.f64Free.pop();
            const at = this.f64Tail++;
            if (at >= this.f64.length)
                throw new Error("f64 arena exhausted — raise f64Capacity");
            return at;
        }
        freeF64Slot(at) { this.f64Free.push(at); }
        storeComputed(slot, value) { this.f64[slot] = value; }
        loadComputed(slot) { return this.f64[slot]; }
        allocF64(value) {
            const at = this.f64Tail++;
            this.f64[at] = value;
            return at;
        }
        allocU32(value) {
            const at = this.u32Tail++;
            this.u32[at] = value;
            return at;
        }
        allocI32(value) {
            const at = this.i32Tail++;
            this.i32[at] = value;
            return at;
        }
        allocBytes(data) {
            const at = this.byteTail;
            this.bytes.set(data, at);
            this.byteTail += data.length;
            return at;
        }
        f64At(offset) { return this.f64[offset]; }
        u32At(offset) { return this.u32[offset]; }
        i32At(offset) { return this.i32[offset]; }
        bytesAt(offset, length) { return this.bytes.subarray(offset, offset + length); }
        exportBuffers() {
            return {
                rings: this.rings.map(r => r.buffer), ctrl: this.ctrl.buffer,
                available: this.available.map(a => a.buffer), clock: this.clock.buffer,
                phaseIndex: this.phaseIndex.buffer, running: this.running.buffer,
                pidToSlot: this.pidToSlot.buffer, dirty: this.dirty.buffer,
                f64: this.f64.buffer, u32: this.u32.buffer, i32: this.i32.buffer, bytes: this.bytes.buffer,
                capacity: this.capacity, pidCapacity: this.pidCapacity, schedule: this.schedule, maxBatch: this.maxBatch,
            };
        }
        static fromBuffers(buf) {
            const self = Object.create(SITH.prototype); //        bypass the allocating constructor entirely
            self.capacity = buf.capacity;
            self.mask = buf.capacity - 1;
            self.pidCapacity = buf.pidCapacity;
            self.schedule = buf.schedule;
            self.maxBatch = buf.maxBatch;
            self.wait = "yield";
            self.shared = true;
            self.rings = buf.rings.map((b) => new Int32Array(b));
            self.ctrl = new Int32Array(buf.ctrl);
            self.available = buf.available.map((b) => new Int32Array(b));
            self.clock = new Int32Array(buf.clock);
            self.phaseIndex = new Int32Array(buf.phaseIndex);
            self.running = new Int32Array(buf.running);
            self.pidToSlot = new Int32Array(buf.pidToSlot);
            self.dirty = new Uint32Array(buf.dirty);
            self.f64 = new Float64Array(buf.f64);
            self.f64Bits = new BigInt64Array(buf.f64);
            self.u32 = new Uint32Array(buf.u32);
            self.i32 = new Int32Array(buf.i32);
            self.bytes = new Uint8Array(buf.bytes);
            self.f64Tail = 0;
            self.f64Free = [];
            self.scratchF = new Float64Array(1);
            self.scratchI = new BigInt64Array(self.scratchF.buffer);
            self.handlers = [];
            return self;
        }
        exportArena() {
            return { f64: this.f64.buffer, pidToSlot: this.pidToSlot.buffer, levelCounter: this.levelCounter.buffer };
        }
        // #endregion
        //                                                                                                           [CONSUMER BARRIER] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        /*======================================================================
            Return the highest contiguous available sequence for a lane. This scans only the availability window, then caches the result. It avoids the classic problem of a slow producer blocking visibility of later producers.
            ====================================================================== */
        availableThrough(lane) {
            const ctrlBase = this.ctrlAt(lane, 0);
            const gateField = this.ctrlAt(lane, 1);
            const cacheField = this.ctrlAt(lane, 2);
            const next = Atomics.load(this.ctrl, ctrlBase);
            let gate = Atomics.load(this.ctrl, gateField);
            let cached = Atomics.load(this.ctrl, cacheField);
            if (cached >= gate) {
                const avail = this.available[lane];
                const cap = this.capacity;
                let scan = cached;
                const limit = Math.min(next, cached + this.maxBatch);
                while (scan < limit) {
                    const flag = Atomics.load(avail, scan & (cap - 1));
                    if (flag <= scan)
                        break;
                    scan++;
                }
                cached = scan;
                Atomics.store(this.ctrl, cacheField, cached);
            }
            return cached;
        }
        // #endregion
        //                                                                                                     [PHASE-LOCKED EXECUTION] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        /*======================================================================
        Drain exactly one scheduled phase. A phase is homogeneous: READ only, WRITE only, DELETE only, etc.
        ====================================================================== */
        drainPhase(limit = this.maxBatch) {
            const schedule = this.schedule;
            const lane = schedule[Atomics.load(this.phaseIndex, 0)];
            const handler = this.handlers[lane];
            if (!handler) {
                this.nextPhase();
                return 0;
            }
            const ring = this.rings[lane];
            const gateField = this.ctrlAt(lane, 1);
            const available = this.availableThrough(lane);
            const head = Atomics.load(this.ctrl, gateField);
            const end = Math.min(available, head + limit);
            if (end <= head) {
                this.nextPhase();
                return 0;
            }
            const epoch = Atomics.load(this.clock, 0);
            // Batch boundary is handed to the handler.
            // The handler can specialize contiguous execution.
            handler(ring, head, end, epoch, this);
            Atomics.store(this.ctrl, gateField, end);
            const consumedField = this.ctrlAt(lane, 7);
            const batchField = this.ctrlAt(lane, 8);
            const maxBatchField = this.ctrlAt(lane, 9);
            const count = end - head;
            Atomics.add(this.ctrl, consumedField, count);
            Atomics.add(this.ctrl, batchField, 1);
            const prevMax = Atomics.load(this.ctrl, maxBatchField);
            if (count > prevMax)
                Atomics.store(this.ctrl, maxBatchField, count);
            this.nextPhase();
            return count;
        }
        revolution(limit = this.maxBatch) {
            let total = 0;
            for (let i = 0; i < this.schedule.length; i++) {
                total += this.drainPhase(limit);
            }
            Atomics.add(this.clock, 0, 1);
            return total;
        }
        drainAll(limit = this.maxBatch) {
            let total = 0;
            for (;;) {
                const before = total;
                total += this.revolution(limit);
                if (total === before)
                    break;
            }
            return total;
        }
        levelCounter = new Int32Array(new SharedArrayBuffer(4));
        workers = [];
        ownerOf(pid) { return pid % this.workers.length; }
        registerWorker(post) { this.workers.push({ post }); }
        sendProgram(pid, program) {
            this.workers[this.ownerOf(pid)].post({ type: "register", pid, program });
        }
        dispatchLevel(readyPids) {
            const byWorker = new Map();
            for (const pid of readyPids) {
                const w = this.ownerOf(pid);
                (byWorker.get(w) ?? byWorker.set(w, []).get(w)).push(pid);
            }
            Atomics.store(this.levelCounter, 0, readyPids.length);
            for (const [w, pids] of byWorker) {
                const buf = Int32Array.from(pids);
                this.workers[w].post({ type: "computeLevel", pids: buf }, [buf.buffer]);
            }
        }
        recalcParallel(levels) {
            for (const level of levels) {
                this.dispatchLevel(level);
                while (Atomics.load(this.levelCounter, 0) > 0) {
                    Atomics.wait(this.levelCounter, 0, Atomics.load(this.levelCounter, 0));
                }
            }
        }
        // #endregion
        //                                                                                                            [CLOCK / CONTROL] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        tick() { return Atomics.load(this.clock, 0); }
        currentPhase() { return this.schedule[Atomics.load(this.phaseIndex, 0)]; }
        start() { Atomics.store(this.running, 0, 1); }
        stop() {
            Atomics.store(this.running, 0, 0);
            Atomics.notify(this.running, 0);
        }
        isRunning() { return Atomics.load(this.running, 0) === 1; }
        // #endregion
        //                                                                                                                      [STATS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        stats(lane) {
            const b = lane * this.CTRL;
            return {
                claimed: Atomics.load(this.ctrl, b + 5),
                published: Atomics.load(this.ctrl, b + 6),
                consumed: Atomics.load(this.ctrl, b + 7),
                batches: Atomics.load(this.ctrl, b + 8),
                maxBatch: Atomics.load(this.ctrl, b + 9),
                fullStalls: Atomics.load(this.ctrl, b + 4),
            };
        }
        pending(lane) {
            const published = Atomics.load(this.ctrl, this.ctrlAt(lane, 6));
            const consumed = Atomics.load(this.ctrl, this.ctrlAt(lane, 7));
            return published - consumed;
        }
        // #endregion
        //                                                                                                                   [INTERNAL] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        laneFor(op) { return op; }
        isFull(lane) {
            const next = Atomics.load(this.ctrl, this.ctrlAt(lane, 0));
            const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
            return next - gate >= this.capacity;
        }
        nextPhase() {
            const next = (Atomics.load(this.phaseIndex, 0) + 1) % this.schedule.length;
            Atomics.store(this.phaseIndex, 0, next);
        }
        yieldNow() {
            if (typeof scheduler !== "undefined" && "yield" in scheduler) {
                scheduler.yield();
            }
            else if (typeof setImmediate === "function") {
                setImmediate(() => { });
            }
            else {
                setTimeout(() => { }, 0);
            }
        }
    })();
    batching_raptor = new (class PALPATINE {
        hub;
        root;
        /*
        Per-lane shared control block layout:
    
        0 nextClaim
        1 gatingSequence  — consumer progress
        2 cachedAvailableSequence
        3 publishedHighest
        4 fullStalls
        5 claimed
        6 published
        7 consumed
        8 batches
        9 maxBatch
       */
        //                                                                                                                  [CONSTANTS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        capacity;
        mask;
        pidCapacity;
        schedule;
        shared;
        rings = [];
        CTRL = 16;
        ctrl;
        available = [];
        clock;
        phaseIndex;
        running;
        pidToSlot;
        dirty;
        f64;
        u32;
        i32;
        bytes;
        f64Tail = 0;
        u32Tail = 0;
        i32Tail = 0;
        byteTail = 0;
        maxBatch;
        wait;
        handlers = [];
        constructor(SITH) {
            const capacity = SITH.capacity ?? 1 << 18;
            const pidCapacity = SITH.pidCapacity ?? 1 << 20;
            if (!pow2(capacity))
                throw new Error("capacity must be a power of two");
            if (!pow2(pidCapacity))
                throw new Error("pidCapacity must be a power of two");
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.pidCapacity = pidCapacity;
            this.schedule = SITH.schedule ?? DEFAULT_SCHEDULE;
            this.shared = SITH.shared ?? typeof SharedArrayBuffer !== "undefined";
            this.maxBatch = SITH.maxBatch ?? 8192;
            this.wait = SITH.wait ?? "yield";
            const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;
            const laneCount = this.schedule.length;
            this.rings = Array.from({ length: laneCount }, () => new Int32Array(new AB(capacity * REC * 4)));
            this.ctrl = new Int32Array(new AB(laneCount * this.CTRL * 4));
            // Availability buffer uses one Int32 per ring slot.
            this.available = Array.from({ length: laneCount }, () => new Int32Array(new AB(capacity * 4)));
            this.clock = new Int32Array(new AB(4));
            this.phaseIndex = new Int32Array(new AB(4));
            this.running = new Int32Array(new AB(4));
            this.pidToSlot = new Int32Array(new AB(pidCapacity * 4));
            this.pidToSlot.fill(-1);
            this.dirty = new Uint32Array(new AB((pidCapacity >>> 5) * 4));
            this.f64 = new Float64Array(new AB((SITH.f64Capacity ?? 1 << 18) * 8));
            this.u32 = new Uint32Array(new AB((SITH.u32Capacity ?? 1 << 18) * 4));
            this.i32 = new Int32Array(new AB((SITH.i32Capacity ?? 1 << 18) * 4));
            this.bytes = new Uint8Array(new AB(SITH.byteCapacity ?? 1 << 20));
        }
        // #endregion
        //                                                                                                                   [HANDLERS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        sub(op, handler) {
            this.handlers[op] = handler;
            return this;
        }
        /*======================================================================
           MULTI-PRODUCER CLAIM / PUBLISH
           Claim the next sequence for a lane. This is a true multi-producer sequencer: - CAS reserves a unique sequence. - The availability buffer marks that exact sequence published. - Consumers never rely on a single global “highest published” value.
          ====================================================================== */
        ctrlAt(lane, field) { return lane * this.CTRL + field; }
        claim(lane) {
            const next = this.ctrlAt(lane, 0);
            const stall = this.ctrlAt(lane, 4);
            const claimed = this.ctrlAt(lane, 5);
            let spin = 0;
            for (;;) {
                const seq = Atomics.load(this.ctrl, next);
                const wrap = seq - this.capacity;
                const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
                if (gate > wrap) {
                    if (Atomics.compareExchange(this.ctrl, next, seq, seq + 1) === seq) {
                        Atomics.add(this.ctrl, claimed, 1);
                        return seq;
                    }
                    continue;
                }
                Atomics.add(this.ctrl, stall, 1);
                if (this.wait === "spin") {
                    spin++;
                    continue;
                }
                if (this.wait === "yield") {
                    this.yieldNow();
                    continue;
                }
                Atomics.wait(this.ctrl, next, seq, 1);
                spin = 0;
            }
        }
        // Publish a claimed record. The record must be fully written BEFORE this call. The availability buffer is the publication barrier.
        publish(lane, seq) {
            const avail = this.available[lane];
            Atomics.store(avail, seq & this.mask, seq + 1);
            const pub = this.ctrlAt(lane, 6);
            Atomics.add(this.ctrl, pub, 1);
            const waiting = this.ctrlAt(lane, 0);
            Atomics.notify(this.ctrl, waiting);
        }
        // One-shot enqueue. Returns: >= 0 — published sequence  -1  — nonblocking mode and lane was full
        emit(op, pid, slot, a = 0, b = 0, c = 0, ext = 0, block = true) {
            const lane = this.laneFor(op);
            const ring = this.rings[lane];
            if (!block && this.isFull(lane))
                return -1;
            const seq = this.claim(lane);
            const base = (seq & this.mask) * REC;
            const epoch = Atomics.load(this.clock, 0);
            ring[base + F_OP] = op;
            ring[base + F_PID] = pid;
            ring[base + F_SLOT] = slot;
            ring[base + F_A] = a;
            ring[base + F_B] = b;
            ring[base + F_C] = c;
            ring[base + F_EPOCH] = epoch;
            ring[base + F_EXT] = ext;
            this.publish(lane, seq);
            return seq;
        }
        // #endregion
        //                                                                                                         [PID / SLOT / DIRTY] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        bind(pid, slot) { this.pidToSlot[pid] = slot; }
        unbind(pid) { this.pidToSlot[pid] = -1; }
        slot(pid) { return this.pidToSlot[pid]; }
        dirtyMark(pid) { this.dirty[pid >>> 5] |= 1 << (pid & 31); }
        dirtyPeek(pid) { return (this.dirty[pid >>> 5] & (1 << (pid & 31))) !== 0; }
        dirtyTake(pid) {
            const w = pid >>> 5;
            const bit = 1 << (pid & 31);
            const word = this.dirty[w];
            if ((word & bit) === 0)
                return false;
            this.dirty[w] = word & ~bit;
            return true;
        }
        // #endregion
        //                                                                                                                 [SOA ARENAS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        allocF64(value) {
            const at = this.f64Tail++;
            this.f64[at] = value;
            return at;
        }
        allocU32(value) {
            const at = this.u32Tail++;
            this.u32[at] = value;
            return at;
        }
        allocI32(value) {
            const at = this.i32Tail++;
            this.i32[at] = value;
            return at;
        }
        allocBytes(data) {
            const at = this.byteTail;
            this.bytes.set(data, at);
            this.byteTail += data.length;
            return at;
        }
        f64At(offset) { return this.f64[offset]; }
        u32At(offset) { return this.u32[offset]; }
        i32At(offset) { return this.i32[offset]; }
        bytesAt(offset, length) { return this.bytes.subarray(offset, offset + length); }
        // #endregion
        //                                                                                                           [CONSUMER BARRIER] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        /*======================================================================
           
           Return the highest contiguous available sequence for a lane. This scans only the availability window, then caches the result. It avoids the classic problem of a slow producer blocking visibility of later producers.
          ====================================================================== */
        availableThrough(lane) {
            const ctrlBase = this.ctrlAt(lane, 0);
            const gateField = this.ctrlAt(lane, 1);
            const cacheField = this.ctrlAt(lane, 2);
            const next = Atomics.load(this.ctrl, ctrlBase);
            let gate = Atomics.load(this.ctrl, gateField);
            let cached = Atomics.load(this.ctrl, cacheField);
            if (cached >= gate) {
                const avail = this.available[lane];
                const cap = this.capacity;
                let scan = cached;
                const limit = Math.min(next, cached + this.maxBatch);
                while (scan < limit) {
                    const flag = Atomics.load(avail, scan & (cap - 1));
                    if (flag <= scan)
                        break;
                    scan++;
                }
                cached = scan;
                Atomics.store(this.ctrl, cacheField, cached);
            }
            return cached;
        }
        // #endregion
        //                                                                                                     [PHASE-LOCKED EXECUTION] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        /*======================================================================
           Drain exactly one scheduled phase. A phase is homogeneous: READ only, WRITE only, DELETE only, etc.
           ====================================================================== */
        drainPhase(limit = this.maxBatch) {
            const schedule = this.schedule;
            const lane = schedule[Atomics.load(this.phaseIndex, 0)];
            const handler = this.handlers[lane];
            if (!handler) {
                this.nextPhase();
                return 0;
            }
            const ring = this.rings[lane];
            const gateField = this.ctrlAt(lane, 1);
            const available = this.availableThrough(lane);
            const head = Atomics.load(this.ctrl, gateField);
            const end = Math.min(available, head + limit);
            if (end <= head) {
                this.nextPhase();
                return 0;
            }
            const epoch = Atomics.load(this.clock, 0);
            // Batch boundary is handed to the handler.
            // The handler can specialize contiguous execution.
            handler(ring, head, end, epoch, this);
            Atomics.store(this.ctrl, gateField, end);
            const consumedField = this.ctrlAt(lane, 7);
            const batchField = this.ctrlAt(lane, 8);
            const maxBatchField = this.ctrlAt(lane, 9);
            const count = end - head;
            Atomics.add(this.ctrl, consumedField, count);
            Atomics.add(this.ctrl, batchField, 1);
            const prevMax = Atomics.load(this.ctrl, maxBatchField);
            if (count > prevMax)
                Atomics.store(this.ctrl, maxBatchField, count);
            this.nextPhase();
            return count;
        } // Run one full revolution. The tracer particle advances only after every scheduled phase runs.
        revolution(limit = this.maxBatch) {
            let total = 0;
            for (let i = 0; i < this.schedule.length; i++) {
                total += this.drainPhase(limit);
            }
            Atomics.add(this.clock, 0, 1);
            return total;
        } // Run until all lanes are drained. Useful for shutdown, transaction boundaries, and tests.
        drainAll(limit = this.maxBatch) {
            let total = 0;
            for (;;) {
                const before = total;
                total += this.revolution(limit);
                if (total === before)
                    break;
            }
            return total;
        }
        // #endregion
        //                                                                                                            [CLOCK / CONTROL] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        tick() { return Atomics.load(this.clock, 0); }
        currentPhase() { return this.schedule[Atomics.load(this.phaseIndex, 0)]; }
        start() { Atomics.store(this.running, 0, 1); }
        stop() {
            Atomics.store(this.running, 0, 0);
            Atomics.notify(this.running, 0);
        }
        isRunning() { return Atomics.load(this.running, 0) === 1; }
        // #endregion
        //                                                                                                                      [STATS] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        stats(lane) {
            const b = lane * this.CTRL;
            return {
                claimed: Atomics.load(this.ctrl, b + 5),
                published: Atomics.load(this.ctrl, b + 6),
                consumed: Atomics.load(this.ctrl, b + 7),
                batches: Atomics.load(this.ctrl, b + 8),
                maxBatch: Atomics.load(this.ctrl, b + 9),
                fullStalls: Atomics.load(this.ctrl, b + 4),
            };
        }
        pending(lane) {
            const published = Atomics.load(this.ctrl, this.ctrlAt(lane, 6));
            const consumed = Atomics.load(this.ctrl, this.ctrlAt(lane, 7));
            return published - consumed;
        }
        // #endregion
        //                                                                                                                   [INTERNAL] 
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        laneFor(op) { return op; }
        isFull(lane) {
            const next = Atomics.load(this.ctrl, this.ctrlAt(lane, 0));
            const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
            return next - gate >= this.capacity;
        }
        nextPhase() {
            const next = (Atomics.load(this.phaseIndex, 0) + 1) % this.schedule.length;
            Atomics.store(this.phaseIndex, 0, next);
        }
        yieldNow() {
            if (typeof scheduler !== "undefined" && "yield" in scheduler) {
                scheduler.yield();
            }
            else if (typeof setImmediate === "function") {
                setImmediate(() => { });
            }
            else {
                setTimeout(() => { }, 0);
            }
        }
    })(this);
    stream_raptor = new (class VADER {
        //                                                                                                                  [CONSTANTS]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        r = "THE_WIZARD_ROOT_REG";
        m = "SAURON_MASTER_REG";
        s = "SAURON_SLAVE_REG";
        k = "__SAURON__";
        capacity;
        hub;
        root;
        // ======================================================================
        // Channel table: open-addressed hash of (r, m, s, k)
        // ======================================================================
        channelMask;
        channelKey; // hash bucket -> channel id or -1
        channelState; // 0 = empty, 1 = used, 2 = deleted
        channelHash; // stored hash to speed probing
        channelCount = 1; // 0 reserved
        // Per-channel subscriber list (intrusive linked list)
        channelSubHead;
        channelSubTail;
        // ======================================================================
        // Subscriber pool
        // ======================================================================
        subHandler;
        subNext; // next subscriber in channel
        subPrev; // prev subscriber in channel
        subChannel; // owning channel
        subCount = 1; // 0 reserved
        // ======================================================================
        // Message ring buffer
        // ======================================================================
        msgChannel;
        msgPayload;
        msgDeferred;
        msgHead = 0; // next message to consume
        msgTail = 0; // next free slot
        msgCount = 0;
        // Backpressure waiters
        capacityWaiters = [];
        // Drain scheduling
        draining = false;
        queuedMicrotask = false;
        constructor(SITH) {
            if (SITH.capacity < 2 || (SITH.capacity & (SITH.capacity - 1)) !== 0) {
                throw new Error("capacity must be a power of two >= 2");
            }
            this.capacity = SITH.capacity;
            this.channelMask = SITH.capacity - 1;
            this.hub = SITH;
            this.root = SITH.root;
            this.r = SITH.r;
            this.m = SITH.m;
            this.s = SITH.s;
            this.k = SITH.k;
            this.channelKey = new Int32Array(SITH.capacity).fill(-1);
            this.channelState = new Uint8Array(SITH.capacity);
            this.channelHash = new Int32Array(SITH.capacity);
            this.channelSubHead = new Int32Array(SITH.capacity).fill(NO_SUB);
            this.channelSubTail = new Int32Array(SITH.capacity).fill(NO_SUB);
            this.subHandler = new Array(SITH.capacity);
            this.subNext = new Int32Array(SITH.capacity).fill(NO_SUB);
            this.subPrev = new Int32Array(SITH.capacity).fill(NO_SUB);
            this.subChannel = new Int32Array(SITH.capacity).fill(-1);
            this.msgChannel = new Int32Array(SITH.capacity).fill(-1);
            this.msgPayload = new Array(SITH.capacity);
            this.msgDeferred = new Array(SITH.capacity);
        }
        // #endregion
        //                                                                                                            [ADDRESS HASHING]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        hashAddress(k, s, m, r) {
            let h = 0x811c9dc5;
            const parts = [r, m, s, k];
            for (let i = 0; i < 4; i++) {
                const part = parts[i];
                const str = typeof part === "string" ? part : String(part);
                for (let j = 0; j < str.length; j++) {
                    h ^= str.charCodeAt(j);
                    h = Math.imul(h, 0x01000193);
                }
                // Separator reduces collision chance across tiers.
                h ^= 0x9e3779b9;
                h = Math.imul(h, 0x01000193);
            }
            return h | 0;
        }
        subFreeList = [];
        allocateSubscriber() {
            const reused = this.subFreeList.pop();
            if (reused !== undefined) {
                return reused;
            }
            const subId = this.allocateSubscriber();
            if (subId >= this.capacity) {
                throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
            }
            return subId;
        }
        releaseSubscriber(subId) {
            this.subHandler[subId] = undefined;
            this.subChannel[subId] = -1;
            this.subNext[subId] = NO_SUB;
            this.subPrev[subId] = NO_SUB;
            this.subFreeList.push(subId);
        }
        // #endregion
        //                                                                                                                   [CHANNELS]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        findChannel(hash) {
            let slot = hash & this.channelMask;
            while (true) {
                const state = this.channelState[slot];
                if (state === 0)
                    return -1;
                if (state === 1 && this.channelHash[slot] === hash) {
                    return this.channelKey[slot];
                }
                slot = (slot + 1) & this.channelMask;
            }
        }
        createChannel(hash) {
            if (this.channelCount >= this.capacity) {
                this.grow();
            }
            let slot = hash & this.channelMask;
            while (this.channelState[slot] === 1) {
                slot = (slot + 1) & this.channelMask;
            }
            const channelId = this.channelCount++;
            this.channelState[slot] = 1;
            this.channelHash[slot] = hash;
            this.channelKey[slot] = channelId;
            this.channelSubHead[channelId] = NO_SUB;
            this.channelSubTail[channelId] = NO_SUB;
            return channelId;
        }
        getOrCreateChannel(k, s, m, r) {
            const hash = this.hashAddress(r, m, s, k);
            const existing = this.findChannel(hash);
            return existing !== -1 ? existing : this.createChannel(hash);
        }
        // #endregion
        //                                                                                                                        [SUB]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        sub(handler, k, s, m, r) {
            if (typeof handler !== "function") {
                throw new TypeError("handler must be a function");
            }
            if (this.subCount >= this.capacity) {
                this.grow();
            }
            const channelId = this.getOrCreateChannel(r, m, s, k);
            const subId = this.allocateSubscriber();
            if (subId >= this.capacity) {
                throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
            }
            this.subHandler[subId] = handler;
            this.subChannel[subId] = channelId;
            this.subNext[subId] = NO_SUB;
            this.subPrev[subId] = NO_SUB;
            const tail = this.channelSubTail[channelId];
            if (tail !== NO_SUB) {
                this.subNext[tail] = subId;
                this.subPrev[subId] = tail;
                this.channelSubTail[channelId] = subId;
            }
            else {
                this.channelSubHead[channelId] = subId;
                this.channelSubTail[channelId] = subId;
            }
            let disposed = false;
            return () => {
                if (disposed)
                    return;
                disposed = true;
                this.removeSubscriber(subId);
            };
        }
        removeSubscriber(subId) {
            const channelId = this.subChannel[subId];
            if (channelId === -1)
                return;
            const prev = this.subPrev[subId];
            const next = this.subNext[subId];
            if (prev !== NO_SUB)
                this.subNext[prev] = next;
            else
                this.channelSubHead[channelId] = next;
            if (next !== NO_SUB)
                this.subPrev[next] = prev;
            else
                this.channelSubTail[channelId] = prev;
            this.subHandler[subId] = undefined;
            this.subChannel[subId] = -1;
            this.subNext[subId] = NO_SUB;
            this.subPrev[subId] = NO_SUB;
            this.releaseSubscriber(subId);
        }
        // #endregion
        //                                                                                                                       [SEND]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        post(payload, k, s, m, r) {
            const channelId = this.getOrCreateChannel(r, m, s, k);
            this.enqueue(channelId, payload, undefined);
        }
        send = (() => {
            const fn = (payload, k, s, m, r) => {
                const channelId = this.getOrCreateChannel(r, m, s, k);
                while (this.msgCount >= this.capacity) {
                    this.grow();
                }
                let resolve;
                let reject;
                let settled = false;
                let rejected = false;
                let result;
                let promiseResolve;
                let promiseReject;
                const deferred = {
                    resolve: (v) => {
                        if (settled)
                            return;
                        settled = true;
                        result = v;
                        promiseResolve?.(v);
                    },
                    reject: (e) => {
                        if (settled)
                            return;
                        settled = true;
                        rejected = true;
                        result = e;
                        promiseReject?.(e);
                    },
                };
                let v = this.enqueue(channelId, payload, deferred);
                if (v !== undefined && !(v instanceof Promise)) {
                    return MaybePromise(v);
                }
                else {
                    const promise = new Promise((res, rej) => {
                        promiseResolve = res;
                        promiseReject = rej;
                        if (settled) {
                            if (rejected)
                                rej(result);
                            else
                                res(result);
                        }
                    });
                    return MaybePromise(promise);
                }
            };
            fn.switch = (p, k, s = this.s, m = this.m, r = this.r, adapter = "ws") => {
                const msg = { id: this.hub.root.reg._sid(), r: r, m: m, s: s, k: k, p: { p }, ts: Date.now(), _ack: false };
                let v = this.root.mesh.DISPATCH(msg, adapter);
                if (v !== undefined && !(v instanceof Promise)) {
                    return MaybePromise(v);
                }
                else {
                    const timeoutMs = 5000;
                    return MaybePromise((() => {
                        return new Promise((resolve, reject) => {
                            let timer;
                            if (v) {
                                if (timer)
                                    clearTimeout(timer);
                                resolve(v);
                                return;
                            }
                            if (timeoutMs) {
                                timer = setTimeout(() => { reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`)); }, timeoutMs);
                            }
                        });
                    })());
                }
            };
            fn.original_send = async (payload, k, s, m, r) => {
                const channelId = this.getOrCreateChannel(r, m, s, k);
                while (this.msgCount >= this.capacity) {
                    this.grow();
                }
                let resolve;
                let reject;
                const deferred = { resolve: (v) => resolve(v), reject: (e) => reject(e), };
                const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
                deferred.resolve = resolve;
                deferred.reject = reject;
                this.enqueue(channelId, payload, deferred);
                return promise;
            };
            return fn;
        })();
        async waitForCapacity() {
            let resolve;
            let reject;
            const deferred = {
                resolve: () => resolve(),
                reject: (e) => reject(e),
            };
            const promise = new Promise((res, rej) => {
                resolve = res;
                reject = rej;
            });
            deferred.resolve = resolve;
            deferred.reject = reject;
            this.capacityWaiters.push(deferred);
            await promise;
        }
        enqueue(channelId, payload, deferred) {
            if (this.msgCount >= this.capacity) {
                this.grow();
            }
            const slot = this.msgTail;
            this.msgChannel[slot] = channelId;
            this.msgPayload[slot] = payload;
            this.msgDeferred[slot] = deferred;
            this.msgTail = (this.msgTail + 1) % this.capacity;
            this.msgCount++;
            this.scheduleDrain();
        }
        // #endregion
        //                                                                                                            [Stream / Duplex]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        stream(r, m, s, k, handler) {
            const channelId = this.getOrCreateChannel(r, m, s, k);
            // Register consumer handler as a normal subscriber.
            this.subscribeRaw(channelId, handler);
            // Sender pushes to the same channel; fire-and-forget.
            return (payload) => {
                this.enqueueStream(channelId, payload);
            };
        }
        openStream(r, m, s, k, handler) {
            const channelId = this.getOrCreateChannel(r, m, s, k);
            // Attach consumer handler (this end)
            const subId = this.subscribeRaw(channelId, handler);
            let closed = false;
            const send = (payload) => {
                if (closed)
                    return;
                this.enqueueStream(channelId, payload);
            };
            const close = () => {
                if (closed)
                    return;
                closed = true;
                this.removeSubscriber(subId);
            };
            return { send, close };
        }
        duplex(r, m, s, k, handler) {
            // Create two directional channels:
            //   outbound: this endpoint -> peer
            //   inbound:  peer -> this endpoint
            const outboundChannel = this.getOrCreateChannel(r, m, s, `${k}:out`);
            const inboundChannel = this.getOrCreateChannel(r, m, s, `${k}:in`);
            // Register this endpoint's inbound handler.
            this.subscribeRaw(inboundChannel, handler);
            let closed = false;
            const send = (payload) => {
                if (closed)
                    return;
                this.enqueueStream(outboundChannel, payload);
            };
            const close = () => {
                if (closed)
                    return;
                closed = true;
                // Remove this endpoint's inbound handler.
                // (Simple approach: remove all handlers on inbound channel.)
                this.removeAllSubscribers(inboundChannel);
            };
            return { send, close };
        }
        // #endregion
        //                                                                                                           [Internal helpers]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        subscribeRaw(channelId, handler) {
            if (this.subCount >= this.capacity) {
                this.grow();
            }
            const subId = this.allocateSubscriber();
            if (subId >= this.capacity) {
                throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
            }
            this.subHandler[subId] = handler;
            this.subChannel[subId] = channelId;
            this.subNext[subId] = NO_SUB;
            this.subPrev[subId] = NO_SUB;
            const tail = this.channelSubTail[channelId];
            if (tail !== NO_SUB) {
                this.subNext[tail] = subId;
                this.subPrev[subId] = tail;
                this.channelSubTail[channelId] = subId;
            }
            else {
                this.channelSubHead[channelId] = subId;
                this.channelSubTail[channelId] = subId;
            }
            return subId;
        }
        removeAllSubscribers(channelId) {
            let sub = this.channelSubHead[channelId];
            while (sub !== NO_SUB) {
                const next = this.subNext[sub];
                this.removeSubscriber(sub);
                sub = next;
            }
            this.channelSubHead[channelId] = NO_SUB;
            this.channelSubTail[channelId] = NO_SUB;
        }
        enqueueStream(channelId, payload) {
            if (this.msgCount >= this.capacity) {
                // Drop oldest message to make room (lossy but non-blocking).
                this.msgHead = (this.msgHead + 1) % this.capacity;
                this.msgCount--;
            }
            const slot = this.msgTail;
            this.msgChannel[slot] = channelId;
            this.msgPayload[slot] = payload;
            this.msgDeferred[slot] = undefined; // no per-message deferred
            this.msgTail = (this.msgTail + 1) % this.capacity;
            this.msgCount++;
            this.scheduleDrain();
        }
        // #endregion
        //                                                                                                              [Introspection]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        get pending() {
            return this.msgCount;
        }
        get subscriberCount() {
            return this.subCount - 1;
        }
        get getChannelCount() {
            return this.channelCount - 1;
        }
        // #endregion
        //                                                                                                                 [Drain loop]
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        scheduleDrain() {
            if (this.queuedMicrotask || this.draining)
                return;
            this.queuedMicrotask = true;
            queueMicrotask(() => {
                this.queuedMicrotask = false;
                this.drain();
            });
        }
        async drain() {
            if (this.draining)
                return;
            this.draining = true;
            try {
                while (this.msgCount > 0) {
                    const slot = this.msgHead;
                    const channelId = this.msgChannel[slot];
                    const payload = this.msgPayload[slot];
                    const deferred = this.msgDeferred[slot];
                    // Advance ring before handler execution so a handler that
                    // sends again cannot deadlock the ring.
                    this.msgHead = (this.msgHead + 1) % this.capacity;
                    this.msgCount--;
                    this.msgChannel[slot] = -1;
                    this.msgPayload[slot] = undefined;
                    this.msgDeferred[slot] = undefined;
                    const result = await this.dispatch(channelId, payload);
                    if (deferred) {
                        deferred.resolve(result);
                    }
                    this.notifyCapacityWaiters();
                }
            }
            finally {
                this.draining = false;
                // If messages arrived while draining, schedule another pass.
                if (this.msgCount > 0) {
                    this.scheduleDrain();
                }
            }
        }
        async dispatch(channelId, payload) {
            let sub = this.channelSubHead[channelId];
            if (sub === NO_SUB)
                return undefined;
            if (this.subNext[sub] === NO_SUB) { // Fast path: exactly one subscriber.
                const handler = this.subHandler[sub];
                if (!handler)
                    return undefined;
                return await handler(payload);
            }
            const results = []; // Multiple subscribers: preserve subscription order.
            const promises = [];
            let hasAsync = false;
            while (sub !== NO_SUB) {
                const handler = this.subHandler[sub];
                if (handler) {
                    const result = handler(payload);
                    if (result && typeof result.then === "function") {
                        hasAsync = true;
                        promises.push(result);
                        results.push(undefined);
                    }
                    else {
                        results.push(result);
                    }
                }
                sub = this.subNext[sub];
            }
            if (hasAsync) {
                const settled = await Promise.all(promises);
                let promiseIndex = 0; // Merge async results back into their original slots.
                for (let i = 0; i < results.length; i++) {
                    if (results[i] === undefined && promiseIndex < settled.length) {
                        results[i] = settled[promiseIndex++];
                    }
                }
            }
            return results;
        }
        notifyCapacityWaiters() {
            if (this.capacityWaiters.length === 0)
                return;
            const waiters = this.capacityWaiters;
            this.capacityWaiters = [];
            for (const waiter of waiters) {
                waiter.resolve(undefined);
            }
        }
        grow() {
            const oldCapacity = this.capacity;
            const newCapacity = oldCapacity * 2;
            const newMask = newCapacity - 1;
            const channelKey = new Int32Array(newCapacity).fill(-1);
            const channelState = new Uint8Array(newCapacity);
            const channelHash = new Int32Array(newCapacity);
            const channelSubHead = new Int32Array(newCapacity).fill(NO_SUB);
            const channelSubTail = new Int32Array(newCapacity).fill(NO_SUB);
            // Rehash existing channels directly from the old table.
            for (let oldSlot = 0; oldSlot < oldCapacity; oldSlot++) {
                if (this.channelState[oldSlot] !== 1)
                    continue;
                const hash = this.channelHash[oldSlot];
                const channelId = this.channelKey[oldSlot];
                let newSlot = hash & newMask;
                while (channelState[newSlot] === 1) {
                    newSlot = (newSlot + 1) & newMask;
                }
                channelState[newSlot] = 1;
                channelHash[newSlot] = hash;
                channelKey[newSlot] = channelId;
                channelSubHead[channelId] = this.channelSubHead[channelId];
                channelSubTail[channelId] = this.channelSubTail[channelId];
            }
            const subHandler = new Array(newCapacity);
            const subNext = new Int32Array(newCapacity).fill(NO_SUB);
            const subPrev = new Int32Array(newCapacity).fill(NO_SUB);
            const subChannel = new Int32Array(newCapacity).fill(-1);
            for (let subId = 0; subId < this.subCount; subId++) {
                subHandler[subId] = this.subHandler[subId];
                subNext[subId] = this.subNext[subId];
                subPrev[subId] = this.subPrev[subId];
                subChannel[subId] = this.subChannel[subId];
            }
            const msgChannel = new Int32Array(newCapacity).fill(-1);
            const msgPayload = new Array(newCapacity);
            const msgDeferred = new Array(newCapacity);
            // Copy the ring in FIFO order, preserving wraparound.
            for (let i = 0; i < this.msgCount; i++) {
                const oldSlot = (this.msgHead + i) % oldCapacity;
                msgChannel[i] = this.msgChannel[oldSlot];
                msgPayload[i] = this.msgPayload[oldSlot];
                msgDeferred[i] = this.msgDeferred[oldSlot];
            }
            this.channelKey = channelKey;
            this.channelState = channelState;
            this.channelHash = channelHash;
            this.channelSubHead = channelSubHead;
            this.channelSubTail = channelSubTail;
            this.subHandler = subHandler;
            this.subNext = subNext;
            this.subPrev = subPrev;
            this.subChannel = subChannel;
            this.msgChannel = msgChannel;
            this.msgPayload = msgPayload;
            this.msgDeferred = msgDeferred;
            this.channelMask = newMask;
            this.capacity = newCapacity;
            this.msgHead = 0;
            this.msgTail = this.msgCount;
        }
    })(this);
    ro2 = new (class RULE_OF_2 {
        r = "THE_WIZARD_ROOT_REG";
        m = "SAURON_MASTER_REG";
        s = "SAURON_SLAVE_REG";
        k = "__SAURON__";
        capacity;
        mask;
        // ── Structure of Arrays (SoA) Event Lanes ──────────────────────────────
        pids;
        values;
        flags;
        // ── Cursors ────────────────────────────────────────────────────────────
        writeCursor = 0;
        readCursor = 0;
        root;
        hub;
        constructor(hub) {
            this.pids = new Int32Array(hub.capacity);
            this.values = new Float64Array(hub.capacity);
            this.flags = new Uint8Array(hub.capacity);
        } // Publishes an event directly into the next ring slot. O(1) time complexity, zero heap allocation, single CPU instruction cycle.
        publish(pid, value, flag = 0) {
            // Optional backpressure check: Prevent writer from overlapping reader
            if (this.writeCursor - this.readCursor >= this.capacity) {
                return false;
            } // Ring buffer full
            const index = this.writeCursor & this.mask;
            this.pids[index] = pid; // Write directly to parallel SoA typed array lanes
            this.values[index] = value;
            this.flags[index] = flag;
            this.writeCursor++;
            return true;
        } // Drains and processes the ring buffer in batches using a zero-allocation loop.
        handler(handler) {
            const targetWrite = this.writeCursor;
            let currentRead = this.readCursor;
            if (currentRead === targetWrite) {
                return 0;
            } // Nothing to consume
            let processed = 0;
            while (currentRead < targetWrite) {
                const index = currentRead & this.mask; // Execute handler on raw lane data
                handler(this.pids[index], this.values[index], this.flags[index]);
                currentRead++;
                processed++;
            }
            this.readCursor = currentRead;
            return processed;
        } // Resets cursors and ring lanes instantly without reallocating memory.
        reset() {
            this.writeCursor = 0;
            this.readCursor = 0;
            this.pids.fill(0);
            this.values.fill(0);
            this.flags.fill(0);
        }
    })(this);
    exogul = new (class EXOGUL {
        capacity;
        values;
        versions;
        nextFree;
        refCount;
        freeHead = -1;
        allocated = 0;
        slab;
        slabTail = 0;
        slabOffsets;
        slabLengths;
        // Track which slots are currently in use (for debugging/validation)
        inUse;
        freeRanges = [];
        hub;
        constructor(capacity = 1 << 20, slabCapacity = 1 << 20) {
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
        writeBinary(slot, data, epoch) {
            this.assertLive(slot);
            this.assertEpoch(epoch);
            const oldOffset = this.slabOffsets[slot];
            const oldLength = this.slabLengths[slot];
            const length = data.length;
            // Reuse the existing allocation when it is large enough.
            if (oldOffset >= 0 && length <= oldLength) {
                this.slab.set(data, oldOffset);
                if (length < oldLength) {
                    this.freeBytes(oldOffset + length, oldLength - length);
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
        readBinary(slot) {
            const { offset, length } = this.ref(slot);
            return this.slab.subarray(offset, offset + length);
        }
        allocBytes(length) {
            if (length === 0)
                return 0;
            // First-fit allocation from reclaimed ranges.
            for (let i = 0; i < this.freeRanges.length; i++) {
                const range = this.freeRanges[i];
                if (range.length < length)
                    continue;
                const offset = range.offset;
                if (range.length === length) {
                    this.freeRanges.splice(i, 1);
                }
                else {
                    range.offset += length;
                    range.length -= length;
                }
                return offset;
            }
            if (length > this.slab.length - this.slabTail) {
                throw new RangeError("STORAGE_RING: no contiguous slab range large enough");
            }
            const offset = this.slabTail;
            this.slabTail += length;
            return offset;
        }
        freeBytes(offset, length) {
            if (length === 0)
                return;
            const ranges = this.freeRanges;
            let index = 0;
            while (index < ranges.length && ranges[index].offset < offset) {
                index++;
            }
            ranges.splice(index, 0, { offset, length });
            // Merge with the preceding range.
            if (index > 0 && ranges[index - 1].offset + ranges[index - 1].length === offset) {
                ranges[index - 1].length += length;
                ranges.splice(index, 1);
                index--;
            }
            // Merge with the following range.
            if (index + 1 < ranges.length && ranges[index].offset + ranges[index].length === ranges[index + 1].offset) {
                ranges[index].length += ranges[index + 1].length;
                ranges.splice(index + 1, 1);
            }
            // Return a free suffix to the unused slab tail.
            const last = ranges[ranges.length - 1];
            if (last !== undefined && last.offset + last.length === this.slabTail) {
                this.slabTail = last.offset;
                ranges.pop();
            }
        }
        assertLive(slot) {
            if (!Number.isInteger(slot) || slot <= 0 || slot >= this.allocated || this.refCount[slot] <= 0) {
                throw new RangeError(`STORAGE_RING: invalid or released slot ${slot}`);
            }
        }
        assertEpoch(epoch) {
            if (!Number.isInteger(epoch) || epoch < 0 || epoch > 0xffffffff) {
                throw new RangeError("STORAGE_RING: epoch must be a uint32");
            }
        }
        tick() {
            return this.versions[0];
        }
        advance() {
            return ++this.versions[0];
        } // Allocate a new slot from the free list. Returns the slot index, or -1 if no slots are available.
        alloc(value = 0) {
            const slot = this.freeHead;
            if (slot === -1)
                return -1; // No free slots
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
        } // Increment the reference count for a slot. Returns true if successful, false if the slot is not allocated.
        retain(slot) {
            if (slot < 0 || slot >= this.capacity)
                return false;
            if (!this.inUse[slot])
                return false;
            if (this.refCount[slot] === 0x7fffffff) {
                throw new Error("Reference count overflow");
            }
            this.refCount[slot]++;
            return true;
        } // Decrement the reference count for a slot. If the ref count reaches zero, the slot is freed. Returns true if the slot was freed, false otherwise.
        release(slot) {
            if (slot < 0 || slot >= this.capacity)
                return false;
            if (!this.inUse[slot])
                return false;
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
        } // Write a numeric value into a slot with epoch validation. Returns true if successful, false if the slot is invalid or epoch mismatch.
        write(slot, value, epoch) {
            if (slot < 0 || slot >= this.capacity)
                return false;
            if (!this.inUse[slot])
                return false;
            if (this.refCount[slot] <= 0)
                return false;
            // Epoch validation: the slot's version must match the expected epoch
            if (this.versions[slot] !== epoch)
                return false;
            this.values[slot] = value;
            return true;
        } // Read the numeric value from a slot. Returns NaN if the slot is invalid.
        read(slot) {
            if (slot < 0 || slot >= this.capacity)
                return NaN;
            if (!this.inUse[slot])
                return NaN;
            return this.values[slot];
        } // Get the version number for a slot. Returns -1 if the slot is invalid.
        version(slot) {
            if (slot < 0 || slot >= this.capacity)
                return -1;
            return this.versions[slot];
        } // Get the slab reference (offset and length) for a slot. Returns { offset: -1, length: 0 } if no slab data is associated.
        ref(slot) {
            if (slot < 0 || slot >= this.capacity) {
                return { offset: -1, length: 0 };
            }
            return {
                offset: this.slabOffsets[slot],
                length: this.slabLengths[slot],
            };
        } // Store variable-sized binary data in the slab for a given slot. Returns true if successful, false if insufficient slab space.
        storeSlab(slot, data) {
            if (slot < 0 || slot >= this.capacity)
                return false;
            if (!this.inUse[slot])
                return false;
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
        } // Read the variable-sized binary data associated with a slot. Returns a subarray view (no copy) or null if no data.
        loadSlab(slot) {
            if (slot < 0 || slot >= this.capacity)
                return null;
            if (!this.inUse[slot])
                return null;
            const offset = this.slabOffsets[slot];
            const length = this.slabLengths[slot];
            if (offset < 0 || length <= 0)
                return null;
            return this.slab.subarray(offset, offset + length);
        }
        growthSlab(minAdditional) {
            const newCapacity = Math.max(this.slab.length * 2, this.slabTail + minAdditional);
            const newSlab = new Uint8Array(newCapacity);
            newSlab.set(this.slab);
            //this.slab = newSlab;
        } // Check if the slab buffer needs compaction. Returns true if compaction would reclaim significant space.
        needsCompaction() {
            if (this.allocated <= 1)
                return false; // Only control slot
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
        } // Compact the slab buffer, removing dead space. Invalidates any subarray views previously returned by loadSlab.
        compact() {
            if (!this.needsCompaction())
                return;
            const newSlab = new Uint8Array(this.slab.length);
            let newTail = 0;
            for (let i = 1; i < this.capacity; i++) {
                if (!this.inUse[i])
                    continue;
                const length = this.slabLengths[i];
                if (length <= 0)
                    continue;
                const oldOffset = this.slabOffsets[i];
                newSlab.set(this.slab.subarray(oldOffset, oldOffset + length), newTail);
                this.slabOffsets[i] = newTail;
                newTail += length;
            }
            // Copy back into original slab buffer
            this.slab.set(newSlab.subarray(0, newTail), 0);
            this.slabTail = newTail;
        } // Get statistics about the storage ring.
        stats() {
            return {
                capacity: this.capacity,
                allocated: this.allocated,
                freeSlots: this.capacity - this.allocated,
                slabUsed: this.slabTail,
                slabCapacity: this.slab.length,
                slabUtilization: this.slabTail / this.slab.length,
            };
        } // Validate internal consistency (for debugging). Returns an array of error strings, empty if consistent.
        validate() {
            const errors = [];
            // Count free list
            let freeCount = 0;
            let current = this.freeHead;
            const seen = new Set();
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
                errors.push(`Free list has ${freeCount} entries, expected ${this.capacity - this.allocated}`);
            }
            // Validate slab offsets
            for (let i = 1; i < this.capacity; i++) {
                if (!this.inUse[i])
                    continue;
                const offset = this.slabOffsets[i];
                const length = this.slabLengths[i];
                if (offset === -1 && length !== 0) {
                    errors.push(`Slot ${i}: offset -1 but length ${length}`);
                }
                else if (offset !== -1) {
                    if (offset < 0 || offset + length > this.slabTail) {
                        errors.push(`Slot ${i}: slab range [${offset}, ${offset + length}) out of bounds (tail=${this.slabTail})`);
                    }
                }
            }
            return errors;
        }
        get liveBytes() {
            let total = 0;
            for (let i = 1; i < this.capacity; i++) {
                if (this.inUse[i] && this.slabLengths[i] > 0) {
                    total += this.slabLengths[i];
                }
            }
            return total;
        }
        get used() {
            return this.allocated - 1 - this.freeCount();
        }
        freeCount() {
            return this.freeHead === FREE_LIST_END ? 0 : this.walkFree();
        }
        get slabUsed() {
            return this.liveBytes;
        }
        get slabFree() {
            return this.slab.length - this.liveBytes;
        }
        walkFree() {
            let n = 0;
            let p = this.freeHead;
            while (p !== FREE_LIST_END) {
                n++;
                p = this.nextFree[p];
            }
            return n;
        }
    })();
    codecs = (() => {
        //                                                                                                CODECS
        // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
        const z = () => { };
        z.beam = new (class BEAM_CODEC {
            /*  =============================================================================
                 Encodes ordinary JavaScript values into a collider-owned byte slab.
                ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
            hub;
            bytes;
            view;
            tail = 0;
            strings = new Map();
            stringBytes = [];
            refs = new Map();
            nextRef = 0;
            constructor(hub, initialCapacity = 1 << 16) {
                this.hub = hub;
                this.bytes = new Uint8Array(initialCapacity);
                this.view = new DataView(this.bytes.buffer);
            }
            /* HC_BINARY_CODEC */
            encode(value) {
                this.tail = 0;
                this.strings.clear();
                this.stringBytes.length = 0;
                this.refs.clear();
                this.nextRef = 0;
                this.writeValue(value);
                return this.bytes.subarray(0, this.tail);
            }
            decode(binary) {
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
                }
                finally {
                    // Restore the encode-side buffer so the next encode()
                    // never writes into the disruptor pool.
                    this.bytes = savedBytes;
                    this.view = savedView;
                    this.tail = savedTail;
                }
            }
            need(count) {
                if (this.tail + count <= this.bytes.length)
                    return;
                let size = this.bytes.length;
                while (size < this.tail + count)
                    size *= 2;
                const next = new Uint8Array(size);
                next.set(this.bytes);
                this.bytes = next;
                this.view = new DataView(next.buffer);
            }
            u8(value) {
                this.need(1);
                this.bytes[this.tail++] = value;
            }
            i32(value) {
                this.need(4);
                this.view.setInt32(this.tail, value, true);
                this.tail += 4;
            }
            f64(value) {
                this.need(8);
                this.view.setFloat64(this.tail, value, true);
                this.tail += 8;
            }
            writeValue(value) {
                if (value === undefined)
                    return this.u8(TAG.UNDEFINED);
                if (value === null)
                    return this.u8(TAG.NULL);
                if (value === false)
                    return this.u8(TAG.FALSE);
                if (value === true)
                    return this.u8(TAG.TRUE);
                const type = typeof value;
                if (type === "number") {
                    const n = value;
                    if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
                        this.u8(TAG.INT32);
                        return this.i32(n);
                    }
                    this.u8(TAG.FLOAT64);
                    return this.f64(n);
                }
                if (type === "string") {
                    return this.writeString(value);
                }
                if (Array.isArray(value)) {
                    return this.writeArray(value);
                }
                if (value instanceof Uint8Array) {
                    return this.writeBytes(value);
                }
                if (type === "object") {
                    return this.writeObject(value);
                }
                throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
            }
            writeString(value) {
                const encoded = new TextEncoder().encode(value);
                this.u8(TAG.STRING);
                this.i32(encoded.length);
                this.need(encoded.length);
                this.bytes.set(encoded, this.tail);
                this.tail += encoded.length;
            }
            writeArray(value) {
                const ref = this.beginRef(value);
                this.u8(TAG.ARRAY);
                this.i32(value.length);
                for (let i = 0; i < value.length; i++) {
                    this.writeValue(value[i]);
                }
                this.endRef(ref);
            }
            writeBytes(value) {
                this.u8(TAG.UINT8ARRAY);
                this.i32(value.length);
                this.need(value.length);
                this.bytes.set(value, this.tail);
                this.tail += value.length;
            }
            writeObject(value) {
                const ref = this.beginRef(value);
                const keys = Object.keys(value);
                this.u8(TAG.OBJECT);
                this.i32(keys.length);
                for (const key of keys) {
                    this.writeString(key);
                    this.writeValue(value[key]);
                }
                this.endRef(ref);
            }
            beginRef(value) {
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
            endRef(ref) { void ref; } // Reserved for future back-reference patching.
            readU8(cursor) { return this.bytes[cursor.at++]; }
            readI32(cursor) {
                const value = this.view.getInt32(cursor.at, true);
                cursor.at += 4;
                return value;
            }
            readF64(cursor) {
                const value = this.view.getFloat64(cursor.at, true);
                cursor.at += 8;
                return value;
            }
            readValue(cursor) {
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
            readString(cursor) {
                const length = this.readI32(cursor);
                const bytes = this.bytes.subarray(cursor.at, cursor.at + length);
                cursor.at += length;
                return new TextDecoder().decode(bytes);
            }
            readArray(cursor) {
                const length = this.readI32(cursor);
                const out = new Array(length);
                for (let i = 0; i < length; i++) {
                    out[i] = this.readValue(cursor);
                }
                return out;
            }
            readObject(cursor) {
                const length = this.readI32(cursor);
                const out = {};
                for (let i = 0; i < length; i++) {
                    const key = this.readString(cursor);
                    out[key] = this.readValue(cursor);
                }
                return out;
            }
            readBytes(cursor) {
                const length = this.readI32(cursor);
                const out = this.bytes.slice(cursor.at, cursor.at + length);
                cursor.at += length;
                return out;
            }
        })(this);
        z.cbor = new (class BINARY_CBOR {
            encodeJSONEnvelope(envelope) { return new TextEncoder().encode(JSON.stringify(envelope)); }
            decodeJSONEnvelope(bytes) { return JSON.parse(new TextDecoder().decode(bytes)); }
            encoder = new TextEncoder();
            decoder = new TextDecoder();
            ensureAvailable(buf, pos, length) { if (pos < 0 || length < 0 || pos + length > buf.length)
                throw new Error("CBOR: truncated buffer"); }
            readUint(buf, pos) {
                this.ensureAvailable(buf, pos, 1);
                const info = buf[pos] & 0x1f;
                if (info <= 23)
                    return [info, pos + 1];
                if (info === 24) {
                    this.ensureAvailable(buf, pos + 1, 1);
                    return [buf[pos + 1], pos + 2];
                }
                if (info === 25) {
                    this.ensureAvailable(buf, pos + 1, 2);
                    return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(0, false), pos + 3];
                }
                if (info === 26) {
                    this.ensureAvailable(buf, pos + 1, 4);
                    return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(0, false), pos + 5];
                }
                if (info === 27) {
                    this.ensureAvailable(buf, pos + 1, 8);
                    const value = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8).getBigUint64(0, false);
                    if (value > BigInt(Number.MAX_SAFE_INTEGER))
                        throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER");
                    return [Number(value), pos + 9];
                }
                ;
                throw new Error(`CBOR: unsupported additional info ${info}`);
            }
            encodeUint64(majorShifted, n) {
                if (n <= 23n)
                    return new Uint8Array([majorShifted | Number(n)]);
                if (n <= 0xffn) {
                    return new Uint8Array([majorShifted | 24, Number(n)]);
                }
                if (n <= 0xffffn) {
                    const b = new Uint8Array(3);
                    b[0] = majorShifted | 25;
                    new DataView(b.buffer).setUint16(1, Number(n), false);
                    return b;
                }
                if (n <= 0xffffffffn) {
                    const b = new Uint8Array(5);
                    b[0] = majorShifted | 26;
                    new DataView(b.buffer).setUint32(1, Number(n), false);
                    return b;
                }
                const b = new Uint8Array(9);
                b[0] = majorShifted | 27;
                new DataView(b.buffer).setBigUint64(1, n, false);
                return b;
            }
            encode(value) {
                const parts = [];
                this.encodeValue(value, parts);
                const total = parts.reduce((n, p) => n + p.length, 0);
                const out = new Uint8Array(total);
                let offset = 0;
                for (const p of parts) {
                    out.set(p, offset);
                    offset += p.length;
                }
                return out;
            }
            encodeValue(v, out) {
                if (v === null) {
                    out.push(new Uint8Array([0xf6]));
                    return;
                }
                if (v === undefined) {
                    out.push(new Uint8Array([0xf7]));
                    return;
                }
                if (v === true) {
                    out.push(new Uint8Array([0xf5]));
                    return;
                }
                if (v === false) {
                    out.push(new Uint8Array([0xf4]));
                    return;
                }
                if (typeof v === 'number') {
                    if (Number.isSafeInteger(v)) {
                        if (v >= 0)
                            out.push(this.encodeUint64(0x00, BigInt(v)));
                        else
                            out.push(this.encodeUint64(0x20, BigInt(-1 - v)));
                    }
                    else {
                        const b = new Uint8Array(9);
                        b[0] = 0xfb;
                        new DataView(b.buffer).setFloat64(1, v, false);
                        out.push(b);
                    }
                    return;
                }
                if (typeof v === 'string') {
                    const encoded = this.encoder.encode(v);
                    out.push(this.encodeUint(0x60, encoded.length));
                    out.push(encoded);
                    return;
                }
                if (v instanceof Uint8Array) {
                    out.push(this.encodeUint(0x40, v.length));
                    out.push(v);
                    return;
                }
                if (Array.isArray(v)) {
                    out.push(this.encodeUint(0x80, v.length));
                    for (const item of v) {
                        this.encodeValue(item, out);
                    }
                    return;
                }
                if (typeof v === 'object') {
                    const proto = Object.getPrototypeOf(v);
                    if (proto !== Object.prototype && proto !== null)
                        throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`);
                    const keys = Object.keys(v);
                    out.push(this.encodeUint(0xa0, keys.length));
                    for (const key of keys) {
                        this.encodeValue(key, out);
                        this.encodeValue(v[key], out);
                    }
                    return;
                }
                ;
                throw new Error(`CBOR: unsupported value type ${typeof v}`);
            }
            encodeUint(majorShifted, n) {
                if (n <= 23)
                    return new Uint8Array([majorShifted | n]);
                if (n <= 0xff)
                    return new Uint8Array([majorShifted | 24, n]);
                if (n <= 0xffff) {
                    const b = new Uint8Array(3);
                    b[0] = majorShifted | 25;
                    new DataView(b.buffer).setUint16(1, n, false);
                    return b;
                }
                ;
                const b = new Uint8Array(5);
                b[0] = majorShifted | 26;
                new DataView(b.buffer).setUint32(1, n, false);
                return b;
            }
            decode(buf) {
                const [value, end] = this.decodeValue(buf, 0);
                if (end !== buf.length) {
                    throw new Error("CBOR: trailing bytes");
                }
                ;
                return value;
            }
            decodeValue(buf, pos) {
                this.ensureAvailable(buf, pos, 1);
                const first = buf[pos];
                const major = first >> 5;
                const info = first & 0x1f;
                if (major === 7) {
                    if (info === 20)
                        return [false, pos + 1];
                    if (info === 21)
                        return [true, pos + 1];
                    if (info === 22)
                        return [null, pos + 1];
                    if (info === 23)
                        return [undefined, pos + 1];
                    if (info === 27) {
                        this.ensureAvailable(buf, pos + 1, 8);
                        const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8);
                        return [dv.getFloat64(0, false), pos + 9];
                    }
                }
                const [n, nextPos] = this.readUint(buf, pos);
                if (major === 0)
                    return [n, nextPos];
                if (major === 1)
                    return [-1 - n, nextPos];
                if (major === 2) {
                    this.ensureAvailable(buf, nextPos, n);
                    const bytes = buf.slice(nextPos, nextPos + n);
                    return [bytes, nextPos + n];
                }
                if (major === 3) {
                    this.ensureAvailable(buf, nextPos, n);
                    const bytes = buf.slice(nextPos, nextPos + n);
                    return [this.decoder.decode(bytes), nextPos + n];
                }
                if (major === 4) {
                    const arr = [];
                    let p = nextPos;
                    for (let i = 0; i < n; i++) {
                        const [item, np] = this.decodeValue(buf, p);
                        arr.push(item);
                        p = np;
                    }
                    return [arr, p];
                }
                if (major === 5) {
                    const obj = {};
                    let p = nextPos;
                    for (let i = 0; i < n; i++) {
                        const [k, kp] = this.decodeValue(buf, p);
                        const [v, vp] = this.decodeValue(buf, kp);
                        obj[k] = v;
                        p = vp;
                    }
                    return [obj, p];
                }
                ;
                throw new Error(`CBOR: unsupported major type ${major}`);
            }
            encode_msg(msg) {
                if (msg instanceof Uint8Array)
                    return msg; // If it's already an encoded Uint8Array, don't re-encode it
                try {
                    return this.encode(msg);
                }
                catch (err) {
                    console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err);
                    return this.encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" });
                }
            }
            decode_msg(raw) {
                if (!(raw instanceof Uint8Array) && !(typeof Buffer !== 'undefined' && Buffer.isBuffer(raw))) {
                    console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
                    return null;
                }
                try {
                    const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array(raw.buffer, raw.byteOffset, raw.length);
                    return this.decode(uint8);
                }
                catch (err) {
                    console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
                    return null;
                }
            }
        })();
        z.serializer = (() => {
            const SKIP = Symbol('ser:skip');
            const DEFAULT_CONFIG = {
                date: true, bigint: true, regexp: true, error: true, specialNumber: true,
                undefined: true, typedArray: true, url: true, map: true, set: true,
                array: true, object: true, string: true, number: true, boolean: true,
            };
            const TYPED_ARRAY_NAMES = new Set([
                'Int8Array', 'Uint8Array', 'Uint8ClampedArray', 'Int16Array', 'Uint16Array',
                'Int32Array', 'Uint32Array', 'Float32Array', 'Float64Array', 'BigInt64Array', 'BigUint64Array',
            ]);
            const z = () => { };
            z.encoder = class SERI {
                config;
                customEncoder;
                constructor(config = {}, customEncoder) {
                    this.config = { ...DEFAULT_CONFIG, ...config };
                    this.customEncoder = customEncoder;
                }
                encodeJSONEnvelope(envelope) {
                    return new TextEncoder().encode(JSON.stringify(envelope));
                }
                ser(value) {
                    return JSON.stringify(this._encode(value));
                }
                _encode(value) {
                    if (this.customEncoder) {
                        const custom = this.customEncoder(value, v => this._encode(v));
                        //if (custom !== SKIP) return { __type: custom.type, value: custom.value };
                    }
                    if (value === null)
                        return null;
                    if (value === undefined) {
                        return this.config.undefined ? { __type: 'Undefined' } : undefined;
                    }
                    const t = typeof value;
                    if (t === 'bigint') {
                        return this.config.bigint
                            ? { __type: 'BigInt', value: value.toString() }
                            : value.toString();
                    }
                    if (t === 'number') {
                        if (!Number.isFinite(value)) {
                            return this.config.specialNumber
                                ? { __type: 'SpecialNumber', value: Number.isNaN(value) ? 'NaN' : value > 0 ? 'Infinity' : '-Infinity' }
                                : null;
                        }
                        return this.config.number ? { __type: 'Primitive', value } : value;
                    }
                    if (t === 'string')
                        return this.config.string ? { __type: 'Primitive', value } : value;
                    if (t === 'boolean')
                        return this.config.boolean ? { __type: 'Primitive', value } : value;
                    if (value instanceof Date) {
                        return this.config.date ? { __type: 'Date', value: value.toISOString() } : value.toISOString();
                    }
                    if (value instanceof RegExp) {
                        return this.config.regexp
                            ? { __type: 'RegExp', value: { source: value.source, flags: value.flags } }
                            : value.toString();
                    }
                    if (value instanceof Error) {
                        return this.config.error
                            ? { __type: 'Error', value: { name: value.name, message: value.message, stack: value.stack } }
                            : value.message;
                    }
                    if (value instanceof URL) {
                        return this.config.url ? { __type: 'URL', value: value.href } : value.href;
                    }
                    if (ArrayBuffer.isView(value) && TYPED_ARRAY_NAMES.has(value.constructor.name)) {
                        if (!this.config.typedArray)
                            return undefined; // no lossless native fallback
                        const view = value;
                        const bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
                        let binary = '';
                        for (let i = 0; i < bytes.length; i++)
                            binary += String.fromCharCode(bytes[i]);
                        return { __type: value.constructor.name, value: btoa(binary) };
                    }
                    if (value instanceof Map) {
                        const entries = Array.from(value.entries()).map(([k, v]) => [this._encode(k), this._encode(v)]);
                        return this.config.map ? { __type: 'Map', value: entries } : Object.fromEntries(entries);
                    }
                    if (value instanceof Set) {
                        const items = Array.from(value.values()).map(v => this._encode(v));
                        return this.config.set ? { __type: 'Set', value: items } : items;
                    }
                    if (Array.isArray(value)) {
                        const items = value.map(v => this._encode(v));
                        return this.config.array ? { __type: 'Array', value: items } : items;
                    }
                    if (t === 'object') {
                        const obj = {};
                        for (const [k, v] of Object.entries(value))
                            obj[k] = this._encode(v);
                        return this.config.object ? { __type: 'Object', value: obj } : obj;
                    }
                    return value; // functions/symbols — unsupported, same as native JSON
                }
            };
            z.decoder = class DESERI {
                config;
                customDecoder;
                constructor(config = {}, customDecoder) {
                    this.config = { ...DEFAULT_CONFIG, ...config };
                    this.customDecoder = customDecoder;
                }
                decodeJSONEnvelope(bytes) {
                    return JSON.parse(new TextDecoder().decode(bytes));
                }
                deser(raw) {
                    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
                    return this._decode(parsed);
                }
                _decode(parsed) {
                    if (parsed === null || parsed === undefined)
                        return parsed;
                    if (typeof parsed === 'object' && '__type' in parsed) {
                        const { __type: type, value } = parsed;
                        if (this.customDecoder) {
                            const custom = this.customDecoder(type, value, v => this._decode(v));
                            if (custom !== SKIP)
                                return custom;
                        }
                        switch (type) {
                            case 'Undefined': return undefined;
                            case 'BigInt': return BigInt(value);
                            case 'SpecialNumber':
                                return value === 'NaN' ? NaN : value === 'Infinity' ? Infinity : -Infinity;
                            case 'Date': return new Date(value);
                            case 'RegExp': {
                                const v = value;
                                return new RegExp(v.source, v.flags);
                            }
                            case 'Error': {
                                const v = value;
                                const e = new Error(v.message);
                                e.name = v.name;
                                e.stack = v.stack;
                                return e;
                            }
                            case 'URL': return new URL(value);
                            case 'Map': return new Map(value.map(([k, v]) => [this._decode(k), this._decode(v)]));
                            case 'Set': return new Set(value.map(v => this._decode(v)));
                            case 'Array': return value.map(v => this._decode(v));
                            case 'Object': {
                                const obj = {};
                                for (const [k, v] of Object.entries(value))
                                    obj[k] = this._decode(v);
                                return obj;
                            }
                            case 'Primitive': return value;
                            default: {
                                if (TYPED_ARRAY_NAMES.has(type)) {
                                    const binary = atob(value);
                                    const bytes = new Uint8Array(binary.length);
                                    for (let i = 0; i < binary.length; i++)
                                        bytes[i] = binary.charCodeAt(i);
                                    const Ctor = globalThis[type];
                                    return new Ctor(bytes.buffer);
                                }
                                return parsed; // unrecognized tag — hand back untouched
                            }
                        }
                    }
                    if (Array.isArray(parsed))
                        return parsed.map(v => this._decode(v));
                    if (typeof parsed === 'object') {
                        const obj = {};
                        for (const [k, v] of Object.entries(parsed))
                            obj[k] = this._decode(v);
                        return obj;
                    }
                    return parsed;
                }
            };
            z.enc = null;
            z.dec = null;
            return z;
        })();
        // #endregion
        //                                                                                                PIDS
        // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
        z.soa = new (class SOA_CODEC {
            // HIGH_PERFORMANCE_SOA_CODEC
            maxPids;
            hub;
            // SoA Memory Layout (Typed Arrays for Cache Locality)
            channelMap;
            subChannelMap;
            tripChannelMap;
            keyMap;
            listeners;
            nextPid = 1;
            constructor(hub, maxPids = 8192) {
                this.maxPids = maxPids;
                this.hub = hub;
                this.channelMap = new Int32Array(maxPids);
                this.subChannelMap = new Int32Array(maxPids);
                this.tripChannelMap = new Int32Array(maxPids);
                this.keyMap = new Int32Array(maxPids);
                this.listeners = new Array(maxPids);
            } // Hot path lookup or allocation: O(1) inline integer assignment
            allocatePid(rId, mId, sId, kId) {
                // In a true LMAX disruptor ring or fixed socket registry,
                // PIDs can be pre-mapped or statically routed at startup.
                const pid = this.nextPid++;
                if (pid >= this.maxPids)
                    throw new Error("PID capacity exceeded");
                this.channelMap[pid] = rId;
                this.subChannelMap[pid] = mId;
                this.tripChannelMap[pid] = sId;
                this.keyMap[pid] = kId;
                return pid;
            }
            /**
             * Encodes a columnar batch (e.g., a spreadsheet column or database table chunk)
             * directly into a parallel SoA byte slab.
             */
            encodeBatch(ids, values) {
                const headerLength = 8; // 4 bytes for count, 4 bytes for flags
                const idBytes = ids.byteLength;
                const valBytes = values.byteLength;
                const totalSize = headerLength + idBytes + valBytes;
                const slab = new Uint8Array(totalSize);
                const view = new DataView(slab.buffer);
                // Write header metadata
                view.setInt32(0, ids.length, true);
                view.setInt32(4, 0, true); // reserved flags
                // Copy raw typed array memory directly into the slab (Zero allocation overhead)
                slab.set(new Uint8Array(ids.buffer, ids.byteOffset, idBytes), headerLength);
                slab.set(new Uint8Array(values.buffer, values.byteOffset, valBytes), headerLength + idBytes);
                return slab;
            }
            /**
             * Decodes / Views the binary slab directly as SoA arenas
             * without creating individual JavaScript objects.
             */
            decodeBatchView(slab) {
                const view = new DataView(slab.buffer, slab.byteOffset, slab.byteLength);
                const count = view.getInt32(0, true);
                const headerLength = 8;
                const idByteLen = count * 4; // Int32 = 4 bytes
                const valByteLen = count * 8; // Float64 = 8 bytes
                const ids = new Int32Array(slab.buffer, slab.byteOffset + headerLength, count);
                const values = new Float64Array(slab.buffer, slab.byteOffset + headerLength + idByteLen, count);
                return { ids, values };
            }
        })(this);
        const FLYWEIGHT_SCOPE = { r: 0, m: 0, s: 0, k: 0 };
        z.pids = new (class INJECTION_CODEC {
            /*  ==========================================================================================================================
             Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested Map trees with direct PID-indexed arrays.
            ========================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [INJECTION_CODEC] */
            ids = new Map();
            names = [];
            pids = new Map(); // "a:b:c:d" → pid
            addresses = new Map(); // pid → [a,b,c,d]
            nextId = 0;
            maxPids;
            listeners;
            nextAllocatedPid = 1; // 0 = TRACER_PID
            channelMap;
            subChannelMap;
            tripChannelMap;
            dirtyPids;
            dirtyCount = 0;
            _flushScheduled = false;
            constructor(maxPids = 8192) {
                this.maxPids = maxPids;
                this.listeners = new Array(maxPids);
                this.dirtyPids = new Int32Array(maxPids);
                this.channelMap = new Int32Array(maxPids);
                this.subChannelMap = new Int32Array(maxPids);
                this.tripChannelMap = new Int32Array(maxPids);
            }
            intern(value) {
                let id = this.ids.get(value);
                if (id === undefined) {
                    id = this.nextId++;
                    this.ids.set(value, id);
                    this.names[id] = value;
                }
                return id;
            } /** Exact identity: encode address → pid (interned, no collisions). */
            encode(pid_key, sl, ms, rt) {
                const a = this.intern(rt);
                const b = this.intern(ms);
                const c = this.intern(sl);
                const d = this.intern(pid_key);
                const addressKey = `${a}:${b}:${c}:${d}`;
                let pid = this.pids.get(addressKey);
                if (pid === undefined) {
                    pid = this.nextAllocatedPid++;
                    if (pid >= this.maxPids)
                        throw new Error("INJECTION_CODEC: PID capacity exceeded");
                    this.pids.set(addressKey, pid);
                    this.addresses.set(pid, [a, b, c, d]);
                    // populate SoA lanes from the interned ids (no re-hash)
                    this.channelMap[pid] = a;
                    this.subChannelMap[pid] = b;
                    this.tripChannelMap[pid] = c;
                }
                return pid;
            } /** Exact decode: pid → address. */
            decode(pid) {
                const addr = this.addresses.get(pid);
                if (!addr)
                    throw new Error(`INJECTION_CODEC: unknown pid ${pid}`);
                const [a, b, c, d] = addr;
                return { rt: this.names[a], ms: this.names[b], sl: this.names[c], key: this.names[d] };
            }
            parseScopeKey(pid) {
                FLYWEIGHT_SCOPE.r = this.channelMap[pid];
                FLYWEIGHT_SCOPE.m = this.subChannelMap[pid];
                FLYWEIGHT_SCOPE.s = this.tripChannelMap[pid];
                return FLYWEIGHT_SCOPE;
            }
            markDirty(pid) {
                this.dirtyPids[this.dirtyCount++] = pid;
            }
            subscribe(pid, fn) {
                this.listeners[pid] = fn;
                return () => {
                    this.listeners[pid] = undefined;
                };
            }
            flushNotifications() {
                this._flushScheduled = false;
                for (let i = 0; i < this.dirtyCount; i++) {
                    const pid = this.dirtyPids[i];
                    const fn = this.listeners[pid];
                    if (fn !== undefined)
                        fn(pid);
                }
                this.dirtyCount = 0;
            }
            notifyHook(key) {
                const pid = this.encode(key, "", "", "");
                this.markDirty(pid);
                if (!this._flushScheduled) {
                    this._flushScheduled = true;
                    queueMicrotask(() => {
                        this._flushScheduled = false;
                        this.flushNotifications();
                    });
                }
            }
            notifyPid(pid, value) {
                const fn = this.listeners[pid];
                if (fn !== undefined)
                    fn(pid, value);
            }
        })();
        z.byte_nested_maps = new (class BYTE_NESTED_MAPS {
            /*  ==========================================================================================================================
             maps single strings or multi-channel routing paths to compact numeric PIDs using typed arrays for high-performance,
             zero-allocation messaging. It interns strings to enable ultra-fast encoding and decoding on performance-critical
             execution paths.
            ========================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BYTE_ATOM_REG] */
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
            stringToId = new Map();
            idToString = [];
            addressToPidMap = new Map();
            nextPid = 1;
            // SoA Memory Lanes (Indexed by PID)
            typeMap; // 0 = Single String, 1 = Multi-Channel
            channelMap; // r channel
            subChannelMap; // m channel
            tripChannelMap; // s channel
            keyMap; // k channel / single key string ID
            // Zero-alloc flyweight for multi-channel decoding
            multiFlyweight = { r: "", m: "", s: "", k: "" };
            constructor(maxPids = 65536) {
                this.typeMap = new Uint8Array(maxPids);
                this.channelMap = new Int32Array(maxPids);
                this.subChannelMap = new Int32Array(maxPids);
                this.tripChannelMap = new Int32Array(maxPids);
                this.keyMap = new Int32Array(maxPids);
            }
            intern(val) {
                let id = this.stringToId.get(val);
                if (id === undefined) {
                    id = this.idToString.length;
                    this.stringToId.set(val, id);
                    this.idToString[id] = val;
                }
                return id;
            } // Input Style A: Register a SINGLE string key -> returns a PID.
            resolveSingle(key) {
                let pid = this.addressToPidMap.get(key);
                if (pid === undefined) {
                    pid = this.nextPid++;
                    this.addressToPidMap.set(key, pid);
                    this.typeMap[pid] = 0; // Mark as Single String
                    this.keyMap[pid] = this.intern(key);
                }
                return pid;
            } // Input Style B: Register 3-4 channel strings -> returns a PID.
            resolveMulti(r, m, s, k = "") {
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
            } // Decode PID back into a Single String (if registered that way).
            decodeSingle(pid) {
                return this.idToString[this.keyMap[pid]] ?? "";
            } // Decode PID back into 3-4 Channels (zero-alloc flyweight mutation).
            decodeMulti(pid) {
                this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
                this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
                this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
                this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
                return this.multiFlyweight;
            }
        })();
        z.example = new (class EXAMPLE {
        })();
        // #endregion
        //                                                                                                 TOYS
        // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
        z.byte_map = new (class BYTE_MAP {
            /*  ==========================================================================================================================
             provides O(1) particle-ID-to-slot lookups using a typed array, with -1 indicating an unmapped particle. It supports
             constant-time assignment, retrieval, and deletion without searching a registry.
            ======================================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DIRECT_SLOT_INDEX] */
            pidToSlot;
            constructor(maxParticles = 1024) {
                this.pidToSlot = new Int32Array(maxParticles).fill(-1);
            }
            set(pid, slot) {
                this.pidToSlot[pid] = slot;
            }
            get(pid) {
                return this.pidToSlot[pid];
            }
            delete(pid) {
                this.pidToSlot[pid] = -1;
            }
        })();
        z.byte_clock = new (class BYTE_CODE_CLOCK {
            /*  ==========================================================================================================================
             maintains a lightweight 8-bit sequence counter, incrementing an epoch whenever the sequence wraps around.
             hasCompletedCycle() uses signed 8-bit arithmetic to detect whether the sequence has advanced since a starting point.
            ========================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BYTE_CODE_CLOCK] */
            register = new Uint8Array(4);
            tick() {
                const seq = (this.register[3] + 1) & 0xff;
                this.register[3] = seq;
                if (seq === 0)
                    this.register[1] = (this.register[1] + 1) & 0xff;
            }
            hasCompletedCycle(startSeq, currentSeq) { return ((currentSeq - startSeq) << 24) >> 24 > 0; }
            get seq() { return this.register[3]; }
            get epoch() { return this.register[1]; }
        })();
        z.concurrent_batching = class CONCURRENT_BYTE_BATCH {
            tiers;
            collider;
            /*  ==========================================================================================================================
             - Tier ordering: original runs numeric tiers sequentially; remake runs all tiers concurrently.
             - Concurrency: original enforces a per-tier concurrency limit; remake's Promise.all can run the entire batch simultaneously, ignoring tier.concurrency.
             - Staging: original holds tasks until execute() releases a tier; remake queues particle IDs until drain().
             - Task lifecycle: original supports retries, timeouts, error handling, and collected results; remake lacks those mechanisms.	======================================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DIRECT_SLOT_INDEX] */
            queues = new Map();
            constructor(tiers, collider) {
                this.tiers = tiers;
                this.collider = collider;
                for (const t of tiers)
                    this.queues.set(t.name, { pids: [], resolvers: [] });
            }
            tier(name) {
                return this.tiers.find((t) => t.name === name);
            }
            submit(pid, tierName, work) {
                let q = this.queues.get(tierName);
                if (!q) {
                    // implicit tier: any tier name referenced before registration gets
                    // created on demand with sensible defaults (concurrency 4, batch 8)
                    const tier = { name: tierName, concurrency: 4, batch: 8, run: (p) => p.payload };
                    this.tiers.push(tier);
                    q = { pids: [], resolvers: [] };
                    this.queues.set(tierName, q);
                }
                return new Promise((resolve) => {
                    q.pids.push(pid);
                    q.resolvers.push(resolve);
                    if (work)
                        this.collider.markTierWork(pid, tierName, work);
                });
            }
            async drain() {
                // tiers run concurrently; within a tier, batches run concurrently
                await Promise.all(this.tiers.map(async (tier) => {
                    const q = this.queues.get(tier.name);
                    while (q.pids.length > 0) {
                        const batch = q.pids.splice(0, Math.min(tier.batch, q.pids.length));
                        const resolvers = q.resolvers.splice(0, batch.length);
                        await Promise.all(batch.map(async (pid, i) => {
                            const particle = this.collider.particleAt(pid);
                            if (particle)
                                await tier.run(particle);
                            resolvers[i]?.(particle?.payload);
                            this.collider.clearTierWork(pid);
                        }));
                        if (tier.concurrency === 1)
                            continue; // preserve order for serial tiers
                    }
                }));
            }
            get pending() {
                let n = 0;
                for (const q of this.queues.values())
                    n += q.pids.length;
                return n;
            }
        };
        z.queued_tiered_concurrent_batches = class QUEUED_TIERED_CONCURRENT_BYTE_BATCHES {
            tiers;
            collider;
            /*  ==========================================================================================================================
             groups particle work into named tiers, processing batches concurrently across tiers and resolving each submission when
             its particle is processed. Each tier supports configurable batch sizes and concurrency settings, with implicit tier
             creation for unregistered names.
            ================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [QUEUED_TIERED_CONCURRENT_BYTE_BATCHES] */
            queues = new Map();
            constructor(tiers, collider) {
                this.tiers = tiers;
                this.collider = collider;
                for (const t of tiers)
                    this.queues.set(t.name, { pids: [], resolvers: [] });
            }
            tier(name) {
                return this.tiers.find((t) => t.name === name);
            }
            submit(pid, tierName, work) {
                let q = this.queues.get(tierName);
                if (!q) {
                    this.tiers.push({ name: tierName, concurrency: 4, batch: 8, run: (p) => p.payload });
                    q = { pids: [], resolvers: [] };
                    this.queues.set(tierName, q);
                }
                return new Promise((resolve) => {
                    q.pids.push(pid);
                    q.resolvers.push(resolve);
                    if (work)
                        this.collider.markTierWork(pid, tierName, work);
                });
            }
            async drain() {
                for (let t = 0; t < this.tiers.length; t++) {
                    const tier = this.tiers[t];
                    const q = this.queues.get(tier.name);
                    const concurrency = Math.max(1, Math.floor(tier.concurrency) || 1);
                    const batchSize = Math.max(1, Math.floor(tier.batch) || 1);
                    while (q.pids.length > 0) {
                        const batch = q.pids.splice(0, batchSize);
                        const resolvers = q.resolvers.splice(0, batch.length);
                        let next = 0;
                        await Promise.all(Array.from({ length: Math.min(concurrency, batch.length) }, async () => {
                            while (next < batch.length) {
                                const i = next++;
                                const pid = batch[i];
                                const particle = this.collider.particleAt(pid);
                                try {
                                    if (particle)
                                        await tier.run(particle);
                                    resolvers[i]?.(particle?.payload);
                                }
                                catch {
                                    resolvers[i]?.(undefined);
                                }
                                finally {
                                    this.collider.clearTierWork(pid);
                                }
                            }
                        }));
                    }
                }
            }
            get pending() {
                let n = 0;
                for (const q of this.queues.values())
                    n += q.pids.length;
                return n;
            }
        };
        // #endregion
        //                                                                                            ENCRYPTION
        // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
        return z;
        // #endregion
    })();
}
// #endregion
// #endregion
//                                                                                                                RAPTORS RAVEN 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export let _root = {
    id: "THE_WIZARD_ROOT_REG",
    hooks: new Map(),
    unmount: new Set(),
    contextProviders: new Map(),
};
const queues = {
    immediate: new Map(),
    "user-blocking": new Map(),
    normal: new Map(),
    background: new Map(),
    idle: new Map(),
};
const nextPow2 = (n) => 1 << Math.ceil(Math.log2(Math.max(2, n)));
class Disruptor {
    capacity;
    mask;
    buffer;
    cursor = -1;
    constructor(capacity) {
        this.capacity = capacity;
        const size = nextPow2(capacity);
        this.mask = size - 1;
        this.buffer = Array.from({ length: size }, () => ({ sequence: -1, key: undefined, payload: undefined, tombstone: false }));
    }
    /** The slot that WILL be claimed next — peek before you commit to it. */
    peekNext() {
        return this.buffer[(this.cursor + 1) & this.mask];
    }
    publish(key, payload, tombstone = false) {
        const seq = this.cursor + 1;
        const slot = this.buffer[seq & this.mask];
        slot.sequence = seq;
        slot.key = key;
        slot.payload = payload;
        slot.tombstone = tombstone;
        this.cursor = seq;
        return seq;
    }
    read(seq) {
        if (seq < 0 || seq > this.cursor)
            return undefined;
        const slot = this.buffer[seq & this.mask];
        return slot.sequence === seq ? slot : undefined;
    }
}
function assertEncodable(value) {
    if (typeof value === "function") {
        throw new TypeError("[RAVEN] state values must be plain data — functions can't be stored here.");
    }
}
function encode(value) {
    assertEncodable(value);
    if (value === null || typeof value !== "object")
        return value;
    return (typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value)));
}
function decode(value) {
    if (value === null || typeof value !== "object")
        return value;
    return (typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value)));
}
class Lane {
    capacity;
    ring;
    writes = 0;
    constructor(capacity) {
        this.capacity = capacity;
        this.ring = new Disruptor(capacity);
    }
    get isFull() {
        return this.writes >= this.capacity;
    }
    get pressure() {
        return this.writes / this.capacity;
    }
    write(key, value) {
        this.writes++;
        return this.ring.publish(key, value);
    }
    readAt(seq) {
        const slot = this.ring.read(seq);
        return slot && !slot.tombstone ? slot.payload : undefined;
    }
    /** Day 0 gets physically rewritten here — we don't tombstone the old
     *  ring, we just stop referencing it. The old Disruptor object (and
     *  everything it held) is garbage the moment nothing points at it. */
    reset() {
        this.writes = 0;
        this.ring = new Disruptor(this.capacity);
    }
}
class MTracer {
    laneCapacity;
    dialSize;
    lanes;
    index = new Map();
    dial = 0;
    /** The tracer particle itself — ticks once per completed lap, forever.
     *  Nobody reads Date.now() anywhere in this class. */
    revolution = 0;
    constructor(laneCapacity = 8192, dialSize = 7) {
        this.laneCapacity = laneCapacity;
        this.dialSize = dialSize;
        this.lanes = Array.from({ length: dialSize }, () => new Lane(laneCapacity));
    }
    get activeLane() {
        return this.lanes[this.dial];
    }
    advanceDialIfFull() {
        if (!this.activeLane.isFull)
            return;
        this.revolution++;
        this.dial = (this.dial + 1) % this.dialSize;
        // We've swept back around to the lane that was "day 0". Reset it —
        // this is the rewrite — then drop any index entries still pointing
        // at it so get() can't hand back memory that no longer belongs to them.
        this.lanes[this.dial].reset();
        for (const [key, entry] of this.index) {
            if (entry.lane === this.dial)
                this.index.delete(key);
        }
    }
    set(key, value) {
        this.advanceDialIfFull();
        const seq = this.activeLane.write(key, encode(value));
        this.index.set(key, { lane: this.dial, seq });
    }
    get(key) {
        const entry = this.index.get(key);
        if (!entry)
            return undefined;
        const payload = this.lanes[entry.lane].readAt(entry.seq);
        if (payload === undefined) {
            this.index.delete(key);
            return undefined;
        }
        return decode(payload);
    }
    has(key) {
        return this.index.has(key) && this.get(key) !== undefined;
    }
    delete(key) {
        return this.index.delete(key);
    }
    clear() {
        this.index.clear();
        this.lanes.forEach((l) => l.reset());
        this.dial = 0;
    }
    entries() {
        const out = [];
        for (const key of this.index.keys()) {
            const v = this.get(key);
            if (v !== undefined)
                out.push([key, v]);
        }
        return out;
    }
    /** Free backpressure: how close the active lane is to ticking the dial
     *  over. A producer driving this toward 1 while nobody's reading is
     *  exactly "queue depth" — except no timer instrumented it. */
    get pressure() {
        return this.activeLane.pressure;
    }
}
class STracer {
    laneCapacity;
    dialSize;
    lanes;
    index = new Map(); // member -> location, SameValueZero like native Set
    dial = 0;
    revolution = 0;
    constructor(laneCapacity = 8192, dialSize = 7) {
        this.laneCapacity = laneCapacity;
        this.dialSize = dialSize;
        this.lanes = Array.from({ length: dialSize }, () => new Lane(laneCapacity));
    }
    get activeLane() {
        return this.lanes[this.dial];
    }
    advanceDialIfFull() {
        if (!this.activeLane.isFull)
            return;
        this.revolution++;
        this.dial = (this.dial + 1) % this.dialSize;
        this.lanes[this.dial].reset();
        for (const [member, entry] of this.index)
            if (entry.lane === this.dial)
                this.index.delete(member);
    }
    add(value) {
        if (this.index.has(value))
            return this; // Set semantics: no dupes, no wasted slot
        this.advanceDialIfFull();
        const seq = this.activeLane.write(value, true); // no encode() — member is never cloned
        this.index.set(value, { lane: this.dial, seq });
        return this;
    }
    has(value) {
        const entry = this.index.get(value);
        return !!entry && this.lanes[entry.lane].readAt(entry.seq) !== undefined;
    }
    delete(value) {
        return this.index.delete(value);
    }
    clear() {
        this.index.clear();
        this.lanes.forEach((l) => l.reset());
        this.dial = 0;
    }
    get size() {
        return this.index.size;
    }
    forEach(cb) {
        for (const member of this.index.keys())
            if (this.has(member))
                cb(member);
    }
    [Symbol.iterator]() {
        const live = [];
        for (const member of this.index.keys())
            if (this.has(member))
                live.push(member);
        return live[Symbol.iterator]();
    }
    get pressure() {
        return this.activeLane.pressure;
    }
}
class OTracer {
    laneCapacity;
    dialSize;
    skipEncode;
    lanes;
    index = new Map();
    dial = 0;
    revolution = 0;
    static BLOCKED = new Set(["__proto__", "constructor", "prototype"]);
    constructor(laneCapacity = 256, dialSize = 7, skipEncode = false) {
        this.laneCapacity = laneCapacity;
        this.dialSize = dialSize;
        this.skipEncode = skipEncode;
        this.lanes = Array.from({ length: dialSize }, () => new Lane(laneCapacity));
        const handler = {
            get: (_t, prop) => {
                if (typeof prop === "symbol")
                    return this[prop];
                if (OTracer.BLOCKED.has(prop))
                    return undefined;
                return this.readKey(prop);
            },
            set: (_t, prop, value) => {
                if (typeof prop === "symbol")
                    return false;
                if (OTracer.BLOCKED.has(prop)) {
                    throw new TypeError(`[RAVEN] "${prop}" is not a writable key on this store.`);
                }
                this.writeKey(prop, value);
                return true;
            },
            has: (_t, prop) => {
                if (typeof prop === "symbol")
                    return false;
                return this.readKey(prop) !== undefined;
            },
            deleteProperty: (_t, prop) => {
                if (typeof prop === "symbol")
                    return false;
                return this.index.delete(prop);
            },
            ownKeys: () => [...this.index.keys()].filter((k) => this.readKey(k) !== undefined),
            getOwnPropertyDescriptor: (_t, prop) => {
                if (typeof prop === "symbol")
                    return undefined;
                const value = this.readKey(prop);
                if (value === undefined)
                    return undefined;
                return { value, writable: true, enumerable: true, configurable: true };
            },
        };
        // eslint-disable-next-line no-constructor-return
        return new Proxy(Object.create(null), handler);
    }
    get activeLane() {
        return this.lanes[this.dial];
    }
    advanceDialIfFull() {
        if (!this.activeLane.isFull)
            return;
        this.revolution++;
        this.dial = (this.dial + 1) % this.dialSize;
        this.lanes[this.dial].reset();
        for (const [key, entry] of this.index)
            if (entry.lane === this.dial)
                this.index.delete(key);
    }
    readKey(key) {
        const entry = this.index.get(key);
        if (!entry)
            return undefined;
        const payload = this.lanes[entry.lane].readAt(entry.seq);
        if (payload === undefined) {
            this.index.delete(key);
            return undefined;
        }
        return this.skipEncode ? payload : decode(payload);
    }
    writeKey(key, value) {
        this.advanceDialIfFull();
        const payload = this.skipEncode ? value : encode(value);
        const seq = this.activeLane.write(key, payload);
        this.index.set(key, { lane: this.dial, seq });
    }
}
// #endregion
//                                                                                                                [RAVEN] STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class RAVEN {
    hub;
    t = {};
    constructor(hub = null) {
        this.hub = hub;
        this.r = hub.r;
        this.m = hub.m;
        this.s = hub.s;
        this.k = hub.k;
    }
    online = false;
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    u = undefined;
    uuuu = [this.u, this.u, this.u, this.u];
    UUUU = [this.s, this.m, this.r];
    uuu = [this.u, this.u, this.u];
    UUU = [this.s, this.m, this.r];
    uu = [this.u, this.u];
    UU = [this.m, this.r];
    batchDepth = 0;
    openScopes = new Set();
    dirty = new Set();
    _flushScheduled = false;
    _adapter = null;
    _currentHookState = null;
    _currentComponent = null;
    PRIORITY_ORDER = ["immediate", "user-blocking", "normal", "background", "idle"];
    GLOBAL_SCOPE = Symbol("global-scope");
    scopeCursors = { immediate: 0, "user-blocking": 0, normal: 0, background: 0, idle: 0 };
    currentScope = this.GLOBAL_SCOPE;
    loopScheduled = false;
    _subscribers = new Map();
    _externalSubscribers = new Map();
    _components = new Map();
    _pendingNotifications = new Set();
    externalSubscribers = new Set();
    instanceId = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    drain() {
        this._flushScheduled = false;
        if (this.dirty.size === 0)
            return;
        // An explicit openBatch() scope is still open (deliberately spanning an await) —
        // don't drop the pending callbacks, just wait for closeBatch() to let them through.
        if (this.openScopes.size > 0) {
            this.scheduleDrain();
            return;
        }
        const callbacks = this.dirty;
        this.dirty = new Set();
        for (const cb of callbacks)
            cb();
    }
    microtask(fn) { queueMicrotask(fn); }
    batch(fn) {
        this.batchDepth++;
        try {
            return fn();
        }
        finally {
            this.batchDepth--;
            if (this.batchDepth === 0) {
                this.flush();
            }
        }
    }
    openBatch() {
        const scope = Symbol("batch-scope");
        this.openScopes.add(scope);
        return scope;
    }
    closeBatch(scope) {
        this.openScopes.delete(scope);
        if (this.openScopes.size === 0 && this.dirty.size > 0) {
            this.scheduleDrain();
        }
    }
    isBatching() { return this.batchDepth > 0 || this.openScopes.size > 0; }
    flushSync() {
        if (this.openScopes.size > 0)
            return;
        this._flushScheduled = false;
        if (this.dirty.size === 0)
            return;
        const callbacks = this.dirty;
        this.dirty = new Set();
        for (const cb of callbacks)
            cb();
    }
    flush(fn) {
        if (!fn) {
            this.flushSync();
            return;
        }
        this.batchDepth++;
        let result;
        try {
            result = fn();
        }
        finally {
            this.batchDepth--;
        }
        this.flushSync();
        return result;
    }
    enqueue(task) {
        if (task.priority === "immediate") {
            this.microtask(() => this.runOne(task));
            return;
        }
        const tier = queues[task.priority];
        if (!tier.has(task.scope))
            tier.set(task.scope, []);
        tier.get(task.scope).push(task);
        this.scheduleLoop();
    }
    scheduleLoop() {
        if (this.loopScheduled)
            return;
        this.loopScheduled = true;
        setImmediate(this.drainOne);
    }
    pickNext() {
        for (const p of this.PRIORITY_ORDER) {
            if (p === "immediate")
                continue;
            const tier = queues[p];
            if (tier.size === 0)
                continue;
            const scopes = Array.from(tier.keys());
            const idx = this.scopeCursors[p] % scopes.length;
            const scope = scopes[idx];
            const list = tier.get(scope);
            const task = list.shift();
            if (list.length === 0)
                tier.delete(scope);
            this.scopeCursors[p] = idx + 1;
            return MaybePromise(task);
        }
        return undefined;
    }
    runOne(task) {
        if (task.cancelled || task.signal?.aborted)
            return;
        const prev = this.currentScope;
        this.currentScope = task.scope;
        try {
            task.fn();
        }
        finally {
            this.currentScope = prev;
        }
    }
    drainOne() {
        this.loopScheduled = false;
        const task = this.pickNext();
        if (task)
            this.runOne(task);
        if (Object.values(queues).some((m) => m.size > 0))
            this.scheduleLoop();
    }
    schedule(fn, opts = {}) {
        const task = { priority: opts.priority ?? "normal", scope: opts.scope ?? this.currentScope, fn, signal: opts.signal, cancelled: false };
        // signals.chains().sc.addEventListener('abort', () => { task.cancelled = true })
        this.enqueue(task);
        const self = this;
        return {
            cancel() { task.cancelled = true; },
            reprioritize(next) {
                if (task.cancelled || next === task.priority)
                    return;
                const old = queues[task.priority].get(task.scope);
                if (old) {
                    const i = old.indexOf(task);
                    if (i !== -1)
                        old.splice(i, 1);
                    if (old.length === 0)
                        queues[task.priority].delete(task.scope);
                }
                task.priority = next;
                self.enqueue(task);
            },
        };
    }
    withRequestScope(requestId, fn) {
        const prev = this.currentScope;
        this.currentScope = requestId;
        try {
            return fn();
        }
        finally {
            this.currentScope = prev;
        }
    }
    yieldToScheduler() {
        return new Promise((resolve) => { this.schedule(() => resolve(), { priority: "normal", scope: this.currentScope }); });
    }
    scheduleChunked(work, opts = {}) {
        const iterator = Symbol.iterator in work ? work[Symbol.iterator]() : work;
        const chunkBudgetMs = opts.chunkBudgetMs ?? 5;
        const scope = opts.scope ?? this.currentScope;
        let cancelled = false;
        let priority = opts.priority ?? "normal";
        const self = this;
        function runChunk() {
            if (cancelled)
                return;
            const start = performance.now();
            while (true) {
                const { value: step, done } = iterator.next();
                if (done)
                    return;
                step();
                const elapsed = performance.now() - start;
                if (elapsed >= chunkBudgetMs)
                    break;
            }
            if (!cancelled)
                self.schedule(runChunk, { priority, scope });
        }
        this.schedule(runChunk, { priority, scope });
        return {
            cancel() {
                cancelled = true;
            },
            reprioritize(next) {
                priority = next;
            },
        };
    }
    // SAURON
    _globalNotifyCache = new WeakMap();
    _notifyCache = new WeakMap();
    scheduleGlobalNotify(key, meta, opts) {
        let cache = this._globalNotifyCache.get(meta);
        if (!cache) {
            cache = new Map();
            this._globalNotifyCache.set(meta, cache);
        }
        let fn = cache.get(key);
        if (!fn) {
            fn = () => {
                cache.delete(key);
                const finalVal = this.store.get(key, opts);
                const waiters = meta._waiters.get(key);
                if (waiters) {
                    for (const resolveFn of waiters) {
                        try {
                            resolveFn(finalVal);
                        }
                        catch (err) {
                            console.error(`[Sauron Waiter Error]:`, err);
                        }
                    }
                    meta._waiters.delete(key);
                }
                for (const listener of meta._globalListeners) {
                    try {
                        listener(key, finalVal);
                    }
                    catch (err) {
                        console.error(`[Sauron Global Listener Error]:`, err);
                    }
                }
            };
            cache.set(key, fn);
        }
        this.onFlush(fn);
    }
    scheduleNotify(key, _listeners, _state) {
        let cache = this._notifyCache.get(_listeners);
        if (!cache) {
            cache = new Map();
            this._notifyCache.set(_listeners, cache);
        }
        let fn = cache.get(key);
        if (!fn) {
            fn = () => {
                cache.delete(key);
                const val = _state.get(key);
                _listeners.get(key)?.forEach((l) => l(val));
            };
            cache.set(key, fn);
        }
        this.onFlush(fn);
    }
    store = {
        get: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _state = this.hub.wr._gt;
            const _persistent = this.hub.wr._persistent;
            const _persistKeys = this.hub.wr._persistent_keys;
            if (_state.has(key))
                return MaybePromise(_state.get(key));
            if (_persistKeys.has(key) && _persistent.has(key)) {
                const v = _persistent.get(key);
                _state.set(key, v);
                return MaybePromise(v);
            }
            return undefined;
        },
        set: (key, value, opts = { s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200 }) => {
            const { triggerRender = true, s, m, r, cap } = opts;
            const _state = this.hub.wr._gt;
            const _persistent = this.hub.wr._persistent;
            const _persistKeys = this.hub.wr._persistent_keys;
            const _listeners = this.hub.wr._listeners;
            const prev = _state.get(key);
            const next = typeof value === "function" ? value(prev) : value;
            if (Object.is(prev, next))
                return;
            // TIME TRAVEL
            _state.set(key, next);
            if (_persistKeys.has(key))
                _persistent.set(key, next);
            if (triggerRender)
                this.scheduleGlobalNotify(key, this.hub.wr, opts);
            if (triggerRender)
                this.scheduleNotify(key, _listeners, _state);
            return MaybePromise(next);
        },
        subscribe: (key, listener, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _listeners = this.hub.wr._listeners;
            if (!_listeners.has(key))
                _listeners.set(key, new Set());
            _listeners.get(key).add(listener);
            return () => {
                _listeners.get(key)?.delete(listener);
                if (_listeners.get(key)?.size === 0)
                    _listeners.delete(key);
            };
        },
        delete: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _state = this.hub.wr._gt;
            const _persistent = this.hub.wr._persistent;
            const _persistKeys = this.hub.wr._persistent_keys;
            const _listeners = this.hub.wr._listeners;
            const existed = _state.delete(key);
            if (_persistKeys.has(key)) {
                _persistKeys.delete(key);
                _persistent.delete(key);
            }
            _listeners.get(key)?.forEach((l) => l(undefined));
            _listeners.delete(key);
            return MaybePromise(existed);
        },
        entries: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _state = this.hub.wr._gt;
            return MaybePromise(Array.from(_state.entries()));
        },
        persist: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _persistKeys = this.hub.wr._persistent_keys;
            _persistKeys.set(key, true);
        },
        unpersist: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _persistent = this.hub.wr._persistent;
            const _persistKeys = this.hub.wr._persistent_keys;
            _persistKeys.delete(key);
            _persistent.delete(key);
        },
        has: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _state = this.hub.wr._gt;
            const _persistent = this.hub.wr._persistent;
            const _persistKeys = this.hub.wr._persistent_keys;
            return _state.has(key) || (_persistKeys.has(key) && _persistent.has(key));
        },
        clear: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _state = this.hub.wr._gt;
            const _listeners = this.hub.wr._listeners;
            _state.clear();
            _listeners.clear();
        },
        clearPersistent: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const _persistent = this.hub.wr._persistent;
            const _persistKeys = this.hub.wr._persistent_keys;
            _persistent.clear();
            _persistKeys.clear();
        },
        _emit: (key, s = this.s, m = this.m, r = this.r) => {
            const opts = { s, m, r };
            const _state = this.hub.wr._gt;
            const _listeners = this.hub.wr._listeners;
            const _waiters = this.hub.wr;
            this.scheduleNotify(key, _listeners, _state);
            this.scheduleGlobalNotify(key, _waiters, opts);
        },
        // subscribe, delete, entries, persist, unpersist, has, clear,
        // clearPersistent, _emit — all unchanged
    };
    state = {
        get: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            return MaybePromise(this.store.get(key, opts));
        },
        set: (key, value, opts = { s: this.s, m: this.m, r: this.r, triggerRender: true }) => {
            const { s, m, r, triggerRender = true } = opts;
            this.store.set(key, value, { triggerRender, s, m, r });
            return MaybePromise(value);
        },
        delete: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const meta = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
            const deleted = this.store.delete(key, opts);
            if (deleted) {
                for (const listener of meta._globalListeners) {
                    try {
                        listener(key, undefined);
                    }
                    catch (err) {
                        console.error(`[Sauron Global Listener Error]:`, err);
                    }
                }
            }
            return deleted;
        },
        getByPrefix: (prefix, opts = { s: this.s, m: this.m, r: this.r }) => {
            return this.store
                .entries(opts)
                .filter(([key]) => typeof key === "string" && key.startsWith(prefix))
                .map(([key, value]) => ({ key: key, value }));
        },
        getAll: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const result = {};
            for (const [key, value] of this.store.entries(opts)) {
                result[key] = value;
            }
            return result;
        },
        subscribe: (key, listener, opts = { s: this.s, m: this.m, r: this.r }) => {
            return this.store.subscribe(key, listener, opts);
        },
        subscribeAll: (listener, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const meta = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
            meta._globalListeners.add(listener);
            return () => {
                meta._globalListeners.delete(listener);
            };
        },
        clear: (opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const meta = this.hub.wr;
            this.store.clear(opts);
            meta._waiters.clear();
            const gl = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
            gl.clear();
        },
        waitFor: (key, opts = { s: this.s, m: this.m, r: this.r }) => {
            const { s, m, r } = opts;
            const meta = this.hub.wr;
            // If it already exists, resolve immediately
            const existing = this.state.get(key, opts);
            if (existing !== undefined)
                return Promise.resolve(existing);
            return new Promise((resolve) => {
                if (!meta._waiters.has(key))
                    meta._waiters.set(key, new Set());
                meta._waiters.get(key).add(resolve);
            });
        },
    };
    _devRecord = null;
    devRecord(componentId, hookName, sauronId, key, value) {
        return this._devRecord?.(componentId, hookName, sauronId, key, value);
    }
    registerDevRecorder(fn) { this._devRecord = fn; }
    rerender = (key) => {
        if (key !== undefined) {
            //   const hs = graphHookStates.get(key);
            // if (hs) notifyHook(`${GRAPH_BRIDGE_COMPONENT_ID}:${key}`);
            this.notifyHook(key);
        }
        else {
            this.flushSync();
        }
    };
    getCurrentComponent = () => {
        if (isReact === true && IS_THE_WIZARD === false) {
            if (this._currentComponent)
                return this._currentComponent;
            return _root;
        }
        else {
            if (!init_hooks_context) {
                this.set("_root", _root, true, this.s, this.m, this.r);
                init_hooks_context = true;
            }
            const updated_ctx = this.get("_root", this.s, this.m, this.r);
            if (updated_ctx && updated_ctx.id === this.r)
                return updated_ctx;
            else
                return _root;
        }
    };
    getCurrentComponentCell = () => {
        if (isReact === true && IS_THE_WIZARD === false) {
            if (this._currentComponent)
                return this._currentComponent;
            return _root;
        }
        else {
            if (!init_hooks_context) {
                this.set("_root", _root, true, this.s, this.m, this.r);
                init_hooks_context = true;
            }
            const updated_ctx = this.get("_root", this.s, this.m, this.r);
            if (updated_ctx && updated_ctx.id === this.r)
                return updated_ctx;
            else
                return _root;
        }
    };
    unmountComponent(id) {
        const instance = this._components.get(id);
        if (!instance)
            return;
        for (const dispose of instance.unmount) {
            try {
                dispose();
            }
            catch (error) {
                console.error(`[THE_CLOUD][UNMOUNT] cleanup failed for "${id}":`, error);
            }
        }
        instance.unmount.clear();
        for (const hook of instance.hooks.values()) {
            try {
                hook.cleanup?.();
            }
            catch (error) {
                console.error(`[THE_CLOUD][UNMOUNT] hook cleanup failed for "${id}":`, error);
            }
        }
        instance.hooks.clear();
        this._components.delete(id);
    }
    mountComponent(id, renderFn) {
        const instance = { id, hooks: new Map(), unmount: new Set(), render: renderFn, };
        this._components.set(id, instance);
        return instance;
    }
    hmrPreserve() {
        const snapshot = new Map();
        this._components.forEach((instance, id) => {
            const hooks = new Map();
            instance.hooks.forEach((state, key) => { hooks.set(key, state.value); });
            snapshot.set(id, hooks);
        });
        return snapshot;
    }
    hmrRestore(snapshot) {
        snapshot.forEach((hooks, id) => {
            const instance = this._components.get(id);
            if (!instance)
                return;
            hooks.forEach((value, key) => {
                const state = instance.hooks.get(key);
                if (state)
                    state.value = value;
            });
        });
    }
    subscribeToHook(key, cb) {
        if (!this._subscribers.has(key))
            this._subscribers.set(key, new Set());
        this._subscribers.get(key).add(cb);
        return () => {
            this._subscribers.get(key)?.delete(cb);
            if (this._subscribers.get(key)?.size === 0)
                this._subscribers.delete(key);
        };
    }
    notifyHook(key) {
        this._pendingNotifications.add(key);
        if (!this._flushScheduled) {
            this._flushScheduled = true;
            queueMicrotask(this._flushHookNotifications);
        }
    }
    scheduleDrain() {
        if (this._flushScheduled)
            return;
        this._flushScheduled = true;
        queueMicrotask(this.drain);
    }
    _flushHookNotifications = () => {
        this._flushScheduled = false;
        const keys = Array.from(this._pendingNotifications);
        this._pendingNotifications.clear();
        for (const key of keys) {
            this.flushHookKey(key);
        }
    };
    onFlush(cb) { this.dirty.add(cb); this.scheduleDrain(); }
    endHookDependencyCapture() { this._currentHookState = null; }
    beginHookDependencyCapture(hook) { this._currentHookState = hook; }
    flushHookKey = (key) => {
        const [componentId] = key.split(":");
        /**
         * Store/global synthetic hook keys do not map to a component hook state.
         * They simply notify subscribers.
         */
        if (componentId === "store") {
            this._subscribers.get(key)?.forEach((callback) => {
                if (typeof callback !== "function")
                    return;
                callback();
                this.onFlush(callback);
            });
            return;
        }
        const component = this._components.get(componentId) ?? _root;
        const hook = component?.hooks.get(key);
        /**
         * Prefer direct DOM binding updates where bindings exist.
         */
        if (hook?.bindings && hook.bindings.size > 0) {
            for (const binding of hook.bindings) {
                if (!binding.active)
                    continue;
                binding.update(binding.node, hook.value);
            }
            return;
        }
        this._subscribers.get(key)?.forEach((callback) => {
            if (typeof callback !== "function")
                return;
            callback();
            this.onFlush(callback);
        });
    };
    registerDOMBinding(node, update) {
        const hook = this._currentHookState;
        if (!hook)
            return;
        const binding = { node, update, active: true };
        if (!hook.bindings) {
            hook.bindings = new Set();
        }
        hook.bindings.add(binding);
        const anyNode = node;
        if (!anyNode.__sauron_bindings__) {
            anyNode.__sauron_bindings__ = new Set();
        }
        anyNode.__sauron_bindings__.add(hook);
        return () => {
            if (!binding.active)
                return;
            binding.active = false;
            hook.bindings?.delete(binding);
            anyNode.__sauron_bindings__?.delete(hook);
        };
    }
    setCurrentComponent(instance) { this._currentComponent = instance; }
    registerDOMRef(name, node) {
        if (!this._currentHookState)
            return;
        if (!this._currentHookState.domRefs)
            this._currentHookState.domRefs = new Map();
        this._currentHookState.domRefs.set(name, node);
    }
    getDOMRef(name) {
        if (!this._currentHookState || !this._currentHookState.domRefs)
            return undefined;
        return this._currentHookState.domRefs.get(name);
    }
    registerAdapter(adapter) { this._adapter = adapter; }
    _reactHookCursor = 0;
    _currentRenderInstanceCheck = null;
    _isReactRenderContext() {
        const _react = isReact;
        if (!_react)
            return false;
        if (IS_THE_WIZARD)
            return false;
        try {
            _react.darkState;
            // If darkState is callable without throwing, we're in a React render context
            // We probe by checking if the dispatcher is active via a known internal
            const internals = _react.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;
            const dispatcher = internals?.ReactCurrentDispatcher?.current ?? internals?.H;
            return !!dispatcher;
        }
        catch (e) {
            // log(`[THE_CLOUD][_IS_REACT_RENDER_CONTEXT]`, { error: e, }, t.error)
            return false;
        }
    }
    createHook(hookName, sauronId, initializer, updater, triggerRender = true) {
        if (isReact === true && IS_THE_WIZARD === false) {
            try {
                const isReact = this._isReactRenderContext();
                const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponent(), forceUpdate: undefined };
                const component = instance;
                // ─── THE TARGETED FIX ───────────────────────────────────────────────────────
                // Use sequential indexing when in React so it matches React's rendering slots.
                // Otherwise, use Sauron's default compilation ID string.
                const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
                const key = `${component.id}:${hookName}:${stableSlotId}`;
                // ────────────────────────────────────────────────────────────────────────────
                if (!this._components.has(component.id)) {
                    this._components.set(component.id, component);
                }
                if (!component.hooks.has(key)) {
                    const state = initializer();
                    component.hooks.set(key, state);
                    if (component.render) {
                        const unsub = this.subscribeToHook(key, () => component.render?.());
                        component.unmount.add(unsub);
                    }
                }
                if (isReact && this._adapter && forceUpdate) {
                    this._adapter.onHookSubscribe(key, forceUpdate);
                }
                const notify = (k) => {
                    if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
                        const state = component.hooks.get(k);
                        this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
                    }
                    this.notifyHook(k);
                };
                const hookState = component.hooks.get(key);
                this.beginHookDependencyCapture(hookState);
                try {
                    return updater(hookState, key, notify);
                }
                finally {
                    this.endHookDependencyCapture();
                }
            }
            catch (e) {
                //  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
                return e.error;
            }
        }
        else {
            try {
                const component = this.getCurrentComponent();
                const key = `${component.id}:${hookName}:${sauronId}`;
                if (!this._components.has(component.id)) {
                    this._components.set(component.id, component);
                }
                if (!component.hooks.has(key)) {
                    const state = initializer();
                    component.hooks.set(key, state);
                    if (triggerRender && component.render) {
                        const unsub = this.subscribeToHook(key, () => component.render?.());
                        component.unmount.add(unsub);
                    }
                }
                const notify = (k) => {
                    if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
                        const state = component.hooks.get(k);
                        this.devRecord(component.id, hookName, sauronId, k, state?.value);
                    }
                    if (triggerRender)
                        this.notifyHook(k);
                };
                const hookState = component.hooks.get(key);
                this.beginHookDependencyCapture(hookState);
                try {
                    return updater(hookState, key, notify);
                }
                finally {
                    this.endHookDependencyCapture();
                }
            }
            catch (e) {
                // log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
                return e.error;
            }
        }
    }
    createHookCell(hookName, sauronId, initializer, updater, triggerRender = true) {
        if (isReact === true && IS_THE_WIZARD === false) {
            try {
                const isReact = this._isReactRenderContext();
                const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponentCell(), forceUpdate: undefined };
                const component = instance;
                // ─── THE TARGETED FIX ───────────────────────────────────────────────────────
                // Use sequential indexing when in React so it matches React's rendering slots.
                // Otherwise, use Sauron's default compilation ID string.
                const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
                const key = `${component.id}:${hookName}:${stableSlotId}`;
                // ────────────────────────────────────────────────────────────────────────────
                if (!this._components.has(component.id)) {
                    this._components.set(component.id, component);
                }
                if (!component.hooks.has(key)) {
                    const state = initializer();
                    component.hooks.set(key, state);
                    if (component.render) {
                        const unsub = this.subscribeToHook(key, () => component.render?.());
                        component.unmount.add(unsub);
                    }
                }
                if (isReact && this._adapter && forceUpdate) {
                    this._adapter.onHookSubscribe(key, forceUpdate);
                }
                const notify = (k) => {
                    if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
                        const state = component.hooks.get(k);
                        this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
                    }
                    this.notifyHook(k);
                };
                const hookState = component.hooks.get(key);
                this.beginHookDependencyCapture(hookState);
                try {
                    return updater(hookState, key, notify);
                }
                finally {
                    this.endHookDependencyCapture();
                }
            }
            catch (e) {
                //  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
                return e.error;
            }
        }
        else {
            try {
                const component = this.getCurrentComponentCell();
                const key = `${component.id}:${hookName}:${sauronId}`;
                if (!this._components.has(component.id)) {
                    this._components.set(component.id, component);
                }
                if (!component.hooks.has(key)) {
                    const state = initializer();
                    component.hooks.set(key, state);
                    if (triggerRender && component.render) {
                        const unsub = this.subscribeToHook(key, () => component.render?.());
                        component.unmount.add(unsub);
                    }
                }
                const notify = (k) => {
                    if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
                        const state = component.hooks.get(k);
                        this.devRecord(component.id, hookName, sauronId, k, state?.value);
                    }
                    if (triggerRender)
                        this.notifyHook(k);
                };
                const hookState = component.hooks.get(key);
                this.beginHookDependencyCapture(hookState);
                try {
                    return updater(hookState, key, notify);
                }
                finally {
                    this.endHookDependencyCapture();
                }
            }
            catch (e) {
                // log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
                return e.error;
            }
        }
    }
    _broadcastSS(key, value, storageType, fn, path, domain, options) {
        _localChange = true;
        _channel?.postMessage({ key, value, storageType, fn, path, domain, options });
    }
    getDark(key, s, m, r) {
        return MaybePromise(this.state.get(key, { s, m, r }));
    }
    setDark(label, value, triggerRender = true, s, m, r) {
        this._broadcastSS(label, value, "sauron", "set");
        return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
    }
    getAllDark(opts) {
        return this.state.getAll(opts);
    }
    subAllDark(fn, s, m, r) {
        if (!fn)
            return () => { };
        return this.state.subscribeAll(fn, { s, m, r });
    }
    subDark(k, fn, s, m, r) {
        if (!k || !fn)
            return () => { };
        return MaybePromise(this.state.subscribe(k, fn, { s, m, r }));
    }
    deleteDark(label, s, m, r) {
        if (!label)
            return false;
        const v = this.state.delete(label, { s, m, r });
        this._broadcastSS(label, undefined, "sauron", "delete");
        return v;
    }
    waitForDark(label, s, m, r) {
        return this.state.waitFor(label, { s, m, r });
    }
    async deleteManyDark(keys, s, m, r) {
        if (!Array.isArray(keys) || keys.length === 0) {
            return false;
        }
        let allDeleted = true;
        // Batch all deletes without triggering renders until the end
        for (let i = 0; i < keys.length; i++) {
            const deleted = this.delSauron(keys[i], s, m, r);
            if (!deleted) {
                allDeleted = false;
            }
        }
        return allDeleted;
    }
    async setManyDark(entries) {
        if (!entries || typeof entries !== "object") {
            return;
        }
        // Batch all sets without triggering renders until the end
        const entriesArray = Object.entries(entries);
        for (let i = 0; i < entriesArray.length; i++) {
            const [key, value] = entriesArray[i];
            // Only trigger render on the last item to avoid multiple renders
            const triggerRender = i === entriesArray.length - 1;
            this.set(key, value, triggerRender);
        }
    }
    async getManyDark(keys, s, m, r) {
        if (!Array.isArray(keys) || keys.length === 0) {
            return {};
        }
        const result = {};
        const promises = keys.map(async (key) => {
            const value = await this.get(key, s, m, r);
            return { key, value };
        });
        const results = await Promise.all(promises);
        for (const { key, value } of results) {
            result[key] = value;
        }
        return result;
    }
    getAll(opts) {
        return this.state.getAll(opts);
    }
    get(key, s, m, r) {
        return MaybePromise(this.state.get(key, { s, m, r }));
    }
    getReChain(key, s, m, r) {
        return MaybePromise(this.state.get(key, { s, m, r }));
    }
    setChain(label, value, triggerRender = true, s, m, r) {
        return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
    }
    set(label, value, triggerRender = true, s, m, r) {
        this._broadcastSS(label, value, "sauron", "set");
        return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
    }
    subChain(label, fn, s, m, r) {
        return MaybePromise(this.state.subscribe(label, fn, { s, m, r }));
    }
    sub(label, fn, s, m, r) {
        if (!label || !fn)
            return () => { };
        return MaybePromise(this.state.subscribe(label, fn, { s, m, r }));
    }
    subAll(fn, s, m, r) {
        if (!fn)
            return () => { };
        return this.state.subscribeAll(fn, { s, m, r });
    }
    delSauron(label, s, m, r) {
        if (!label)
            return false;
        const v = this.state.delete(label, { s, m, r });
        this._broadcastSS(label, undefined, "sauron", "delete");
        return v;
    }
    deleteChain(label, s, m, r) {
        return MaybePromise(this.state.delete(label, { s, m, r }));
    }
    waitFor(label, s, m, r) {
        return this.state.waitFor(label, { s, m, r });
    }
    __subscribers = new Map();
}
// #endregion
//                                                                                                                         [ss] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const _SS_MEMORY = new Map();
export let memoryTimer;
export let ttl_active = false;
export let ttl_minutes = 30;
export function touchMemory() {
    if (memoryTimer)
        clearTimeout(memoryTimer);
    memoryTimer = setTimeout(() => {
        _SS_MEMORY.clear();
        memoryTimer = undefined;
    }, ttl_minutes * 60 * 1000);
}
export async function _withLock(key, fn) {
    if (typeof navigator !== "undefined" && navigator.locks) {
        return navigator.locks.request("gt_write_" + key, fn);
    }
    return fn();
}
export function generateId() {
    if (typeof crypto !== "undefined" && crypto.getRandomValues) {
        const arr = new Uint32Array(1);
        crypto.getRandomValues(arr);
        return arr[0].toString();
    }
    try {
        const { randomBytes } = require("crypto");
        return randomBytes(4).readUInt32BE(0).toString();
    }
    catch {
        return Math.floor(Math.random() * 0xffffffff).toString();
    }
}
export async function setSS(key, value) {
    if (typeof window === "undefined")
        return;
    const serialized = ser(value);
    await _withLock(key, () => {
        sessionStorage.setItem(key, serialized);
        _SS_MEMORY.set(key, serialized);
    });
    return value;
}
function ser(value) {
    return typeof value === "string" ? value : JSON.stringify(value);
}
function deser(raw) {
    if (raw === null)
        return null;
    try {
        return JSON.parse(raw);
    }
    catch {
        return raw;
    }
}
export function getSS(key) {
    if (typeof window === "undefined")
        return;
    const raw = typeof sessionStorage !== "undefined" ? sessionStorage.getItem(key) : null;
    const v = deser(raw);
    return MaybePromise(v);
}
export async function get_ws_id() {
    let Bun = null;
    let Deno = null;
    if (typeof globalThis.Deno !== "undefined") {
        Deno = globalThis.Deno;
    }
    if (typeof globalThis.Bun !== "undefined") {
        Bun = globalThis.Bun;
    }
    const hasBun = typeof globalThis.Bun !== "undefined";
    const hasDeno = typeof globalThis.Deno !== "undefined";
    const hasNode = typeof process !== "undefined" && !!process.versions?.node;
    const isBrowser = typeof window !== "undefined";
    const isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
    const isElectron = false;
    const isUnix = false;
    const isWindows = true;
    let WS_KEY = root.reg.get('WS_KEY');
    let WS_ID = root.reg.get('WS_ID');
    if (isBrowser) {
        let id = getSS(WS_KEY);
        if (!id) {
            id = generateId();
            setSS(WS_KEY, id);
        }
        WS_ID = id;
        return id;
    }
    if (isVSCode) {
        const vscode = require("vscode");
        const name = vscode.workspace.workspaceFolders?.[0]?.name;
        if (name)
            return name;
        const id = generateId();
        WS_ID = id;
        return id;
    }
    let cwd;
    if (hasBun || hasNode) {
        cwd = process.cwd();
    }
    else if (hasDeno) {
        cwd = Deno.cwd();
    }
    else {
        throw new Error("Unsupported environment");
    }
    let filePath;
    if (hasDeno) {
        const currentDir = new URL(".", import.meta.url).pathname;
        const projectRoot = new URL("../../../../../", import.meta.url).pathname;
        filePath = new URL("the_wizard/wizards_vault/ws_id.txt", projectRoot).pathname;
    }
    else {
        const path = require("path");
        const currentDir = typeof import.meta?.dirname !== "undefined" ? import.meta.dirname : __dirname;
        const projectRoot = path.resolve(currentDir, "../../../../../");
        filePath = path.join(projectRoot, "the_wizard", "wizards_vault", "ws_id.txt");
    }
    const readFile = async () => {
        try {
            if (hasBun) {
                const file = Bun.file(filePath);
                if (!(await file.exists()))
                    return null;
                return await file.text();
            }
            else if (hasDeno) {
                try {
                    await Deno.stat(filePath);
                    return await Deno.readTextFile(filePath);
                }
                catch {
                    return null;
                }
            }
            else if (hasNode) {
                const fs = require("fs");
                if (!fs.existsSync(filePath))
                    return null;
                return fs.readFileSync(filePath, "utf8");
            }
            return null;
        }
        catch {
            return null;
        }
    };
    const writeFile = async (data) => {
        if (hasBun) {
            await Bun.write(filePath, data);
        }
        else if (hasDeno) {
            await Deno.writeTextFile(filePath, data);
        }
        else if (hasNode) {
            const fs = require("fs");
            fs.writeFileSync(filePath, data, "utf8");
        }
    };
    let id = await readFile();
    if (id && !isNaN(Number(id))) {
        WS_ID = id;
        return id;
    }
    id = generateId();
    await writeFile(id);
    WS_ID = id;
    return id;
}
async function deleteSS(key) {
    if (typeof window === "undefined")
        return;
    if (ttl_active)
        touchMemory();
    await _withLock(key, () => {
        if (typeof sessionStorage !== "undefined")
            sessionStorage.removeItem(key);
        _SS_MEMORY.delete(key);
    });
}
// #endregion
//                                                                                                                   [WAR ROOM] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const rootRegistry = new M2();
export class WAR_ROOOM {
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    raven;
    //                                                                                                                  MAIN CACHES
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    #META = new M2();
    _meta;
    #SIGNALS = new M2();
    _signals;
    #NAKED_SIGNALS = new M2();
    _naked_signals;
    #CONTEXT = new M2();
    _context;
    #ATOMS = new M2();
    _atoms;
    #SAURON = new M2();
    _sauron;
    #BUS = new M2();
    _bus;
    _gt;
    COMPONENTS = new M2();
    // #endregion
    //                                                                                                                    THE CLOUD
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    _components;
    _batches;
    _batchQueue;
    _batchOpts;
    _sys;
    _sealed;
    _keyContracts;
    _conflicts;
    _conflictStrategies;
    _causal;
    _history;
    _scopes;
    _msgHandlers;
    _lastWriteSource;
    _mainMap;
    _tupleCache;
    _weakMap;
    _boundaries;
    _persistent;
    _persistent_keys;
    _global_subscribers;
    _waiters;
    _listeners;
    // #endregion
    //                                                                                                                    SIGNALS
    // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    _timeTravelMaul;
    _historyMaul;
    _subscribersMaul;
    _listenersMaul;
    // #endregion
    constructor(raven) {
    }
    get_root(c, r) {
        let root;
        switch (c) {
            case "#CONTEXT": {
                root = this.#CONTEXT.get(r);
                break;
            }
            case "#META": {
                root = this.#META.get(r);
                break;
            }
            case "#SIGNALS": {
                root = this.#SIGNALS.get(r);
                break;
            }
            case "#ATOMS": {
                root = this.#ATOMS.get(r);
                break;
            }
            case "#SAURON": {
                root = this.#SAURON.get(r);
                break;
            }
            case "#BUS": {
                root = this.#BUS.get(r);
                break;
            }
            case "#NAKED_SIGNALS": {
                root = this.#NAKED_SIGNALS.get(r);
                break;
            }
            case "gt": {
                root = rootRegistry.get(r);
                break;
            }
            case "globalThis": {
                globalThis[r];
                break;
            }
        }
        if (!root) {
            root = new M2();
            switch (c) {
                case "#CONTEXT": {
                    root = this.#CONTEXT.set(r, root);
                    break;
                }
                case "#META": {
                    root = this.#META.set(r, root);
                    break;
                }
                case "#SIGNALS": {
                    root = this.#SIGNALS.set(r, root);
                    break;
                }
                case "#ATOMS": {
                    root = this.#ATOMS.set(r, root);
                    break;
                }
                case "#SAURON": {
                    root = this.#SAURON.set(r, root);
                    break;
                }
                case "#NAKED_SIGNALS": {
                    root = this.#NAKED_SIGNALS.set(r, root);
                    break;
                }
                case "#BUS": {
                    root = this.#BUS.set(r, root);
                    break;
                }
                case "gt": {
                    rootRegistry.set(r, root);
                    break;
                }
                case "globalThis": {
                    globalThis[r] = root;
                    break;
                }
            }
        }
        return root;
    }
    map(s = this.s, m = this.m, r = this.r, c = "gt") {
        const root = this.get_root(c, r);
        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new M2();
            root.set(m, masterMap);
        }
        let slaveMap = masterMap.get(s);
        if (!slaveMap) {
            slaveMap = new M2();
            masterMap.set(s, slaveMap);
        }
        return slaveMap;
    }
    tracer(s = this.s, m = this.m, r = this.r, c = "gt") {
        const root = this.get_root(c, r);
        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new M2();
            root.set(m, masterMap);
        }
        let slaveMap = masterMap.get(s);
        if (!slaveMap) {
            slaveMap = new MTracer();
            masterMap.set(s, slaveMap);
        }
        return slaveMap;
    }
    set(s = this.s, m = this.m, r = this.r, c = "gt") {
        const root = this.get_root(c, r);
        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new M2();
            root.set(m, masterMap);
        }
        let slaveMap = masterMap.get(s);
        if (!slaveMap) {
            slaveMap = new STracer();
            masterMap.set(s, slaveMap);
        }
        return slaveMap;
    }
    obj(s = this.s, m = this.m, r = this.r, c = "gt") {
        const root = this.get_root(c, r);
        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new M2();
            root.set(m, masterMap);
        }
        let slaveMap = masterMap.get(s);
        if (!slaveMap) {
            slaveMap = new OTracer();
            masterMap.set(s, slaveMap);
        }
        return slaveMap;
    }
}
// (globalThis as A)[r] //  STATE_CONTEXT === "sauron" ? : null;
// (globalThis as A)[r] = root;
// #endregion
//                                                                                                             [DEATH STAR] HUB
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class DEATH_STAR {
    mesh;
    state; // app context / state
    bus; // bus
    signals; // signals
    governor;
    reg;
    wr;
    //public MESH: Mesh;
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SIGNALS__";
    RTRN_TO_SDR = 'RTRN_TO_SDR';
    DEVTOOLS_ACTIVE = false;
    capacity = 1 << 20;
    pidCapacity = 1024;
    schedule = DEFAULT_SCHEDULE;
    shared = typeof SharedArrayBuffer !== "undefined";
    maxBatch = 8192;
    wait = "yield";
    f64Capacity = 1 << 18;
    u32Capacity = 1 << 18;
    i32Capacity = 1 << 18;
    byteCapacity = 1 << 20;
    CTRL = 16;
    f64 = new Float64Array(0);
    constructor(REGISTRY = null, MESH = null) {
        (async () => {
            const serial = executor();
            await serial.enqueue(() => (this.state = new RAVEN(this)));
            await serial.enqueue(() => {
                this.reg = REGISTRY;
                if (!REGISTRY)
                    this.reg = new Registry();
            });
            await serial.enqueue(() => (this.wr = new WAR_ROOOM(this)));
            await serial.enqueue(() => {
                if (MESH)
                    this.mesh = MESH;
                else if (!MESH)
                    this.mesh = new THE_MESH_STORE(this);
            });
            await serial.enqueue(() => (this.governor = new GOVERNOR(this)));
            await serial.enqueue(() => (this.bus = new SITH()));
            await serial.enqueue(() => (this.signals = new SIGNALS()));
            await serial.enqueue(() => (this.init()));
            await serial.exec();
        })();
    }
    update_mesh(MESH) {
        this.mesh = MESH;
        this.bus.mesh_store = MESH;
    }
    init() {
        //                                                                                                              signals.signals
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.signals.root = this;
        this.signals.r = this.r;
        this.signals.m = this.m;
        this.signals.s = this.s;
        this.signals.k = this.k;
        this.signals.hub = this;
        this.signals.signals = this.signals;
        this.signals._signal_cells = this.signals.signals.getS(this.s, this.m, this.r, "#SIGNAL_CELLS");
        this.signals._signal_listeners = this.signals.signals.getS(this.s, this.m, this.r, "#SIGNAL_LISTENERS");
        this.signals._signal_global_listeners = this.signals.signals.getS(this.s, this.m, this.r, "#SIGNAL_GLOBAL_LISTENERS");
        this.signals._signal_waiters = this.signals.signals.getS(this.s, this.m, this.r, "#SIGNAL_WAITERS");
        this.signals._signal_persistent = this.signals.signals.getS(this.s, this.m, this.r, "#SIGNAL_PERSISTENT");
        this.signals._signal_persistent_keys = this.signals.signals.getS(this.s, this.m, this.r, "#SIGNAL_PERSISTENT_KEYS");
        this.signals.selfId = `SIGNALS_${this.reg._sid()}`;
        // #endregion
        //                                                                                                       signals.signals_raptor
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.signals.signals_raptor.root = this;
        this.signals.signals_raptor.hub = this;
        this.state = this.state;
        this.signals.signals_raptor.r = this.r;
        this.signals.signals_raptor.m = this.m;
        this.signals.signals_raptor.s = this.s;
        this.signals.signals_raptor.k = this.k;
        this.signals.signals_raptor.signals = this.signals;
        this.signals.signals_raptor.DARTH_MAUL_ENGINE = this.signals.signals_raptor_ngin;
        // #endregion
        //                                                                                                                          bus
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.root = this;
        this.bus.shared = this.shared;
        this.bus.state = this.state.state;
        this.bus.wait = this.wait;
        this.bus.maxBatch = this.maxBatch;
        this.bus.capacity = this.capacity;
        this.bus.schedule = this.schedule;
        this.bus.r = this.r;
        this.bus.m = this.m;
        this.bus.s = this.s;
        this.bus.k = this.k;
        this.bus.DEVTOOLS_ACTIVE = this.DEVTOOLS_ACTIVE;
        // #endregion
        //                                                                                                          bus.batching_raptor
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.batching_raptor.hub = this.bus;
        this.bus.batching_raptor.root = this;
        // #endregion
        //                                                                                                            bus.stream_raptor
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.stream_raptor.capacity = this.capacity;
        this.bus.stream_raptor.channelMask = this.capacity - 1;
        this.bus.stream_raptor.hub = this.bus;
        this.bus.stream_raptor.root = this;
        this.bus.stream_raptor.r = this.r;
        this.bus.stream_raptor.m = this.m;
        this.bus.stream_raptor.s = this.s;
        this.bus.stream_raptor.k = this.k;
        // #endregion
        //                                                                                                                      bus.ro2
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.ro2.root = this;
        this.bus.ro2.hub = this.bus;
        this.bus.ro2.r = this.r;
        this.bus.ro2.m = this.m;
        this.bus.ro2.s = this.s;
        this.bus.ro2.k = this.k;
        if ((this.capacity & (this.capacity - 1)) !== 0) {
            throw new Error("Disruptor capacity must be a power of two.");
        }
        this.bus.ro2.capacity = this.capacity;
        this.bus.ro2.mask = this.capacity - 1;
        // #endregion
        //                                                                                                                   bus.exogul
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.exogul.hub = this.bus;
        if (!pow2(this.capacity))
            throw new Error("slotCapacity must be a power of two");
        // #endregion
        //                                                                                                             SIGNALS.signals
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.g_bus.s = this.s;
        this.bus.g_bus.m = this.m;
        this.bus.g_bus.r = this.r;
        this.bus.g_bus.k = this.k;
        const root = globalThis;
        const rL = (root[this.r] ??= Object.create(null));
        const mL = (rL[this.m] ??= Object.create(null));
        this.bus.g_bus.channels = mL[this.s] ??= Object.create(null);
        // #endregion
        //                                                                                                             SIGNALS.signals
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.wr.raven = this;
        this.wr.r = this.r ?? "THE_WIZARD_ROOT_REG";
        this.wr.m = this.m ?? "SAURON_MASTER_REG";
        this.wr.s = this.s ?? "SAURON_SLAVE_REG";
        this.wr.k = this.k ?? "THE_K_KEY";
        this.wr._meta = this.wr.tracer("_meta", this.m, this.r, "#META");
        this.wr._signals = this.wr.map("_signals", this.m, this.r, "#SIGNALS");
        this.wr._naked_signals = this.wr.map("_naked_signals", this.m, this.r, "#NAKED_SIGNALS");
        this.wr._context = this.wr.tracer("_context", this.m, this.r, "#CONTEXT");
        this.wr._atoms = this.wr.tracer("_atoms", this.m, this.r, "#ATOMS");
        this.wr._sauron = this.wr.tracer("_sauron", this.m, this.r, "#SAURON");
        this.wr._bus = this.wr.tracer("_bus", this.m, this.r, "#BUS");
        this.wr._gt = this.wr.tracer("_gt", this.m, this.r);
        this.wr._listeners = this.wr.map("_listeners_raven", this.m, this.r);
        this.wr._global_subscribers = this.wr.map("_global_subscribers_raven", this.m, this.r);
        this.wr._waiters = this.wr.map("_waiters_raven", this.m, this.r);
        this.wr._persistent_keys = this.wr.tracer("_persistent_keys_raven", this.m, this.r);
        this.wr._persistent = this.wr.tracer("_persistent_raven", this.m, this.r);
        this.wr._components = this.wr.tracer("_components_raven", this.m, this.r);
        this.wr._batches = this.wr.tracer("_batches_raven", this.m, this.r);
        this.wr._batchQueue = this.wr.tracer("_batchQueue_raven", this.m, this.r);
        this.wr._batchOpts = this.wr.map("_batchOpts_raven", this.m, this.r);
        this.wr._sys = this.wr.tracer("_sys_raven", this.m, this.r);
        this.wr._sealed = this.wr.tracer("_sealed_raven", this.m, this.r);
        this.wr._keyContracts = this.wr.map("_keyContracts_raven", this.m, this.r);
        this.wr._conflicts = this.wr.tracer("_conflicts_raven", this.m, this.r);
        this.wr._conflictStrategies = this.wr.tracer("_conflictStrategies_raven", this.m, this.r);
        this.wr._causal = this.wr.map("_causal_raven", this.m, this.r);
        this.wr._history = this.wr.map("_history_raven", this.m, this.r);
        this.wr._scopes = this.wr.tracer("_scopes_raven", this.m, this.r);
        this.wr._msgHandlers = this.wr.map("_msgHandlers_raven", this.m, this.r);
        this.wr._lastWriteSource = this.wr.map("_lastWriteSource_raven", this.m, this.r);
        this.wr._mainMap = this.wr.tracer("_mainMap_raven", this.m, this.r);
        this.wr._tupleCache = this.wr.tracer("_tupleCache_raven", this.m, this.r);
        this.wr._weakMap = this.wr.tracer("_weakMap_raven", this.m, this.r);
        this.wr._boundaries = this.wr.tracer("_boundaries_raven", this.m, this.r);
        this.wr._timeTravelMaul = this.wr.tracer("_timeTravel_maul", this.m, this.r, "#SIGNALS");
        this.wr._historyMaul = this.wr.tracer("_history_maul", this.m, this.r, "#SIGNALS");
        this.wr._subscribersMaul = this.wr.map(" _subscribers_maul", this.m, this.r, "#SIGNALS");
        this.wr._listenersMaul = this.wr.map(" _listeners_maul", this.m, this.r, "#SIGNALS");
        // #endregion
        //                                                                                                             SIGNALS.signals
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.drs.root = this;
        this.bus.drs.hub = this.bus;
        this.bus.drs.r = this.r;
        this.bus.drs.m = this.m;
        this.bus.drs.s = this.s;
        this.bus.drs.k = this.k;
        this.bus.drs.state = this.state.state;
        this.bus.drs.bus = this.bus;
        // #endregion
        //                                                                                                             SIGNALS.signals
        // #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
        this.bus.spreadsheet.hub = this.bus;
        this.bus.spreadsheet.root = this;
        this.bus.spreadsheet.r = this.r;
        this.bus.spreadsheet.m = this.m;
        this.bus.spreadsheet.s = this.s;
        this.bus.spreadsheet.k = this.k;
        const capacity = this.capacity ?? 1 << 18;
        const pidCapacity = this.pidCapacity ?? 1 << 20;
        if (!pow2(capacity))
            throw new Error("capacity must be a power of two");
        if (!pow2(pidCapacity))
            throw new Error("pidCapacity must be a power of two");
        this.bus.spreadsheet.capacity = capacity;
        this.bus.spreadsheet.mask = capacity - 1;
        this.bus.spreadsheet.pidCapacity = pidCapacity;
        this.bus.spreadsheet.schedule = this.schedule ?? DEFAULT_SCHEDULE;
        this.bus.spreadsheet.shared = this.shared ?? typeof SharedArrayBuffer !== "undefined";
        this.bus.spreadsheet.maxBatch = this.maxBatch ?? 8192;
        this.bus.spreadsheet.wait = this.wait ?? "yield";
        const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;
        const laneCount = this.schedule.length;
        this.bus.spreadsheet.rings = Array.from({ length: laneCount }, () => new Int32Array(new AB(capacity * REC * 4)));
        this.bus.spreadsheet.ctrl = new Int32Array(new AB(laneCount * this.CTRL * 4));
        // Availability buffer uses one Int32 per ring slot.
        this.bus.spreadsheet.available = Array.from({ length: laneCount }, () => new Int32Array(new AB(capacity * 4)));
        this.bus.spreadsheet.clock = new Int32Array(new AB(4));
        this.bus.spreadsheet.phaseIndex = new Int32Array(new AB(4));
        this.bus.spreadsheet.running = new Int32Array(new AB(4));
        this.bus.spreadsheet.pidToSlot = new Int32Array(new AB(pidCapacity * 4));
        this.bus.spreadsheet.pidToSlot.fill(-1);
        this.bus.spreadsheet.dirty = new Uint32Array(new AB((pidCapacity >>> 5) * 4));
        this.bus.spreadsheet.f64 = new Float64Array(new AB((this.f64Capacity ?? 1 << 18) * 8));
        this.bus.spreadsheet.u32 = new Uint32Array(new AB((this.u32Capacity ?? 1 << 18) * 4));
        this.bus.spreadsheet.i32 = new Int32Array(new AB((this.i32Capacity ?? 1 << 18) * 4));
        this.bus.spreadsheet.bytes = new Uint8Array(new AB(this.byteCapacity ?? 1 << 20));
        this.bus.spreadsheet.f64Bits = new BigInt64Array(this.f64.buffer);
        this.bus.spreadsheet.f64 = new Float64Array(new AB((this.f64Capacity ?? 1 << 18) * 8));
        this.bus.codecs.serializer.enc = new (this.bus.codecs.serializer.encoder)({});
        this.bus.codecs.serializer.dec = new (this.bus.codecs.serializer.decoder)({});
        // #endregion
    }
    update_reg(REGISTRY) {
        this.reg = REGISTRY;
    }
}
// send               p: U, k: A, s: S = '_globalThis', m: S = this.m, r: S = this.r, adapter = "ws"
// send.gt.hot        p: U, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c:S='mesh'
// sub                handler: A, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c: c_type = "mesh"
// sub.hot            handler: A, k: S
// __sauron_id__ = this.root.reg._sid()
export const root = new DEATH_STAR();
const mesh = new THE_MESH_STORE(root);
root.update_mesh(mesh);
class _serialExecutor {
    chain = Promise.resolve();
    enqueue(fn) {
        const run = this.chain.then(fn);
        this.chain = run.then(() => undefined, () => undefined);
        return run;
    }
    exec() {
        return this.chain;
    }
}
function executor() {
    return new _serialExecutor();
}
function getDark(key, s, m, r) {
    return MaybePromise(root.state.state.get(key, { s, m, r }));
}
function setDark(label, value, triggerRender = true, s, m, r) {
    root.state._broadcastSS(label, value, "sauron", "set");
    return MaybePromise(root.state.state.set(label, value, { triggerRender, s, m, r }));
}
function subDark(fn, k, s, m, r) {
    if (!k || !fn)
        return () => { };
    return MaybePromise(root.state.state.subscribe(k, fn, { s, m, r }));
}
// #endregion
//                                                                                                                    USE STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkState(initialValue, options = {}, __sauron_id__ = this.hub.reg._sid()) {
    return root.state.createHookCell("darkState", __sauron_id__, () => ({
        value: {
            current: typeof initialValue === "function" ? initialValue() : initialValue,
            derived: undefined,
        },
    }), (state, key, notify) => {
        const s = state.value;
        const equals = options.equals ?? ((a, b) => a === b);
        const selector = options.selector ?? null;
        const derive = options.derive ?? null;
        const getValue = () => (selector ? selector(s.current) : s.current);
        const setValue = (v) => {
            const next = typeof v === "function" ? v(s.current) : v;
            const prevSelected = getValue();
            const nextSelected = selector ? selector(next) : next;
            if (!equals(prevSelected, nextSelected)) {
                s.current = next;
                if (derive) {
                    s.derived = derive(next);
                }
                notify(key);
            }
            return next;
        };
        if (derive && s.derived === undefined) {
            s.derived = derive(s.current);
        }
        if (derive) {
            return [getValue(), setValue, s.derived];
        }
        return [getValue(), setValue];
    });
}
globalThis.darkState = darkState;
// #endregion
//                                                                                                                USE CALL BACK
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkCallback(callback, deps, __sauron_id__ = this.hub.reg._sid()) {
    return root.state.createHookCell("darkCallback", __sauron_id__, () => ({ value: undefined, callback, deps: deps ?? [] }), (s) => {
        const state = s;
        const hasChanged = !deps || deps.length !== state.deps.length || deps.some((d, i) => d !== state.deps[i]);
        if (hasChanged) {
            state.callback = callback;
            state.deps = deps ?? [];
        }
        return state.callback;
    });
}
globalThis.darkCallback = darkCallback;
// #endregion
//                                                                                                                   USE EFFECT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkEffect(effect, deps, __sauron_id__ = this.hub.reg._sid()) {
    root.state.createHookCell("darkEffect", __sauron_id__, () => {
        const controller = new AbortController();
        const cleanup = effect(controller.signal);
        return {
            value: undefined,
            deps: deps ?? [],
            cleanup: () => {
                controller.abort();
                if (typeof cleanup === "function")
                    cleanup();
            },
        };
    }, (state) => {
        if (deps === undefined || !_depsEqual(state.deps ?? [], deps)) {
            state.cleanup?.();
            const controller = new AbortController();
            const cleanup = effect(controller.signal);
            state.deps = deps ?? [];
            state.cleanup = () => {
                controller.abort();
                if (typeof cleanup === "function")
                    cleanup();
            };
        }
        return undefined;
    });
}
function _depsEqual(a, b) {
    if (a.length !== b.length)
        return false;
    for (let i = 0; i < a.length; i++) {
        if (!Object.is(a[i], b[i]))
            return false;
    }
    return true;
}
globalThis.darkEffect = darkEffect;
// #endregion
//                                                                                                                 USE INTERVAL
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkInterval(options, __sauron_id__ = this.hub.reg._sid()) {
    const autoStart = options.autoStart ? Boolean(options.autoStart) !== false : true;
    const callback = options.execute ? options.execute : console.log(`interval triggered`);
    const delay = options.timer ? options.timer : 30000;
    return root.state.createHook("darkInterval", __sauron_id__, () => ({
        value: {
            callback,
            delay,
            running: autoStart,
            handle: null,
            attached: false,
            cleanup: null,
        },
    }), (hook, key) => {
        const s = hook.value;
        const start = () => {
            if (s.running)
                return;
            s.running = true;
            root.state.notifyHook(key);
            if (typeof window !== "undefined" && s.delay !== null) {
                s.handle = setInterval(() => {
                    s.callback();
                }, s.delay);
            }
        };
        const stop = () => {
            if (!s.running)
                return;
            s.running = false;
            if (s.handle) {
                clearInterval(s.handle);
                s.handle = null;
            }
            root.state.notifyHook(key);
        };
        if (!s.attached && typeof window !== "undefined") {
            s.attached = true;
            if (s.running && s.delay !== null) {
                s.handle = setInterval(() => {
                    s.callback();
                }, s.delay);
            }
            s.cleanup = () => {
                if (s.handle)
                    clearInterval(s.handle);
            };
        }
        const isRunning = () => s.running;
        return [isRunning(), start, stop];
    });
}
// #endregion
//                                                                                                           USE DEFERRED VALUE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkDeferredValue(value, __sauron_id__ = this.hub.reg._sid()) {
    return root.state.createHook("darkDeferredValue", __sauron_id__, () => ({ value }), (state, key, notify) => {
        if (!Object.is(state.value, value)) {
            queueMicrotask(() => {
                state.value = value;
                notify(key);
            });
        }
        return state.value;
    });
}
// #endregion
//                                                                                                        USE IMPERATIVE HANDLE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkImperativeHandle(ref, init, deps, __sauron_id__ = this.hub.reg._sid()) {
    return root.state.createHook("darkImperativeHandle", __sauron_id__, () => {
        if (ref)
            ref.current = init();
        return { value: undefined, deps: deps ?? [] };
    }, (state) => {
        if (deps === undefined || !_depsEqual(state.deps ?? [], deps)) {
            if (ref)
                ref.current = init();
            state.deps = deps ?? [];
        }
        return undefined;
    });
}
// #endregion
//                                                                                                                     USE MEMO
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkMemo(factory, deps, __sauron_id__ = this.hub.reg._sid()) {
    return root.state.createHook("darkMemo", __sauron_id__, () => ({ value: factory(), deps }), (state) => {
        if (!_depsEqual(state.deps ?? [], deps)) {
            state.value = factory();
            state.deps = deps;
        }
        return state.value;
    });
}
// #endregion
// #endregion
/**
 Addressing
 r = root/app key
 m = master key (component or provider id)
 s = slave key (hook slot or consumer id)
 k = payload key (state key or context key)
 
 For hooks, the natural mapping is:
 r = app/root
 m = component id
 s = hook slot index
 k = state key
 
 For context:
 r = app/root
 m = provider id
 s = consumer id
 k = context key
 */
/** BUS
 export const counter = atom_contract<{ count: number }>(() => ({
    atom: "VERTEX",
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
    bindables: [
        atom_bindable<{ increment(): void }>(({ self }) => ({
            increment() {
                self.value.count += 1;
            },
        })),
    ],
}));

const instance = counter({ count: 0 });

instance.increment();
instance.value.count; // 1

export const fastCounter = atom_contract(() => ({
    atom: "VERTEX",
    value: { count: 0 },
}));

export const counter = atom_contract(() => ({
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
}));



const push = bus.stream(
    "app",
    "telemetry",
    "gpu",
    "v1",
    (payload) => {
        console.log("gpu tick:", payload);
    },
);

push({ frame: 1 });
push({ frame: 2 });
push({ frame: 3 });

const client = bus.duplex(
    "app",
    "session",
    "ws",
    "v1",
    (message) => {
        console.log("client received:", message);
    },
);

const server = bus.duplex(
    "app",
    "session",
    "ws",
    "v1",
    (message) => {
        console.log("server received:", message);
    },
);

client.send({ hello: "server" });
server.send({ hello: "client" });

// Consumer side (subscribe)
const push = bus.stream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        // this runs for every message pushed to this address
        console.log("received:", payload);
    },
);

// Producer side (send)
push({ frame: 1 });
push({ frame: 2 });

const producer = bus.stream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        console.log("consumer got:", payload);
    },
);

producer({ frame: 1 });  // producer side

const stream = bus.openStream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        console.log("consumer got:", payload);
    },
);

stream.send({ frame: 1 });   // producer side
stream.close();              // optional teardown

// Endpoint A
const aSend = bus.stream("app", "session", "ws", "v1:a2b");
bus.subscribe("app", "session", "ws", "v1:b2a", (msg) => {
    console.log("A got:", msg);
});

// Endpoint B
const bSend = bus.stream("app", "session", "ws", "v1:b2a");
bus.subscribe("app", "session", "ws", "v1:a2b", (msg) => {
    console.log("B got:", msg);
});

// Cross-communication
aSend({ hello: "B" });   // B's handler fires
bSend({ hello: "A" });   // A's handler fires



 const bus = new MESSAGE_BUS_DISRUPTOR(4096);

 // Multiple subscribers on the same address
 const stopA = bus.subscribe("app", "user", "create", "v1", async (payload) => {
     return `A:${payload}`;
 });
 
 const stopB = bus.subscribe("app", "user", "create", "v1", (payload) => {
     return `B:${payload}`;
 });
 
 // Awaited send receives subscriber results
 const result = await bus.send("app", "user", "create", "v1", "skyler");
 
 // result === ["A:skyler", "B:skyler"]
 
 // Single-subscriber fast path
 bus.subscribe("app", "ping", "direct", "v1", (p) => `pong:${p}`);
 
 const pong = await bus.send("app", "ping", "direct", "v1", "hi");
 // pong === "pong:hi"
 
 // Fire-and-forget
 bus.post("app", "metrics", "tick", "v1", { n: 1 });
 
 // Unsubscribe
 stopA();
 stopB();
  */
