// SLAB.TS
import { slab } from './slab.types';
export function expose(instance, name) {
    slab[name] = new Proxy(instance, {
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
export class O2 {
    data = Object.create(null);
    f = this.forEachKV; //                                                    [FOR EACH]
    subs;
    hist;
    histLimit = 0;
    strict = false;
    iterating = 0;
    batching = 0;
    pending;
    derived;
    dependents;
    constructor(entries) {
        if (!entries)
            return;
        for (const [k, v] of entries)
            this.data[k] = v;
    }
    #rawHas(k) { return Object.prototype.hasOwnProperty.call(this.data, k); }
    #rawGet(k) { return this.data[k]; }
    #emit(e) {
        if (this.batching > 0) {
            const pending = this.pending[e.k];
            this.pending[e.k] = pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true };
            return;
        }
        this.#record(e);
        this.subs?.forEach((sub) => {
            try {
                sub(e);
            }
            catch (err) {
                process.emitWarning(err);
            }
        });
    }
    #record(e) {
        if (!this.hist)
            return;
        this.hist.push(e);
        if (this.hist.length > this.histLimit) {
            this.hist.splice(0, this.hist.length - this.histLimit);
        }
    }
    #invalidate(k) {
        const deps = this.dependents?.[k];
        if (!deps)
            return;
        for (const dk of deps) {
            const d = this.derived?.[dk];
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
        if (this.strict && this.iterating > 0) {
            throw new Error(`O2.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
        }
    }
    // IN ORDER TO MATCH MAPS CALL API FOR INSTANT AND EASY CONVERSIONS
    get(k) {
        const d = this.derived?.[k];
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
        this.data[k] = next;
        this.#invalidate(k);
        this.#emit({ op: "set", k, v: next, prev: prev, has });
        return this;
    } // note: flipped to match Map's (key, value)
    has(k) {
        const d = this.derived?.[k];
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
        delete this.data[k];
        this.#invalidate(k);
        this.#emit({ op: "del", k, prev, has: false });
        return true;
    }
    clear() {
        this.#assertNotIterating("clear");
        const entries = this.toArray();
        this.data = Object.create(null); //              one object swap, no per-key delete
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
        return Reflect.ownKeys(this.data).length;
    }
    [Symbol.iterator]() { return this.entries(); }
    on(fn) {
        (this.subs ??= new Set()).add(fn);
        return () => this.off(fn);
    }
    off(fn) {
        this.subs?.delete(fn);
    }
    batch(fn) {
        this.batching++;
        this.pending ??= Object.create(null);
        try {
            fn();
        }
        finally {
            this.batching--;
            if (this.batching === 0) {
                const pending = this.pending;
                this.pending = undefined;
                for (const k of Reflect.ownKeys(pending)) {
                    const e = pending[k];
                    this.#record(e);
                    this.subs?.forEach((sub) => {
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
        this.data[k] = next;
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
        delete this.data[k];
        this.#invalidate(k);
        this.#emit({ op: "del", k, prev, has: false });
        return true;
    }
    c() {
        this.#assertNotIterating("clear");
        const entries = this.toArray();
        this.data = Object.create(null); //              one object swap, no per-key delete
        for (const [k, prev] of entries) {
            this.#invalidate(k);
            this.#emit({ op: "del", k, prev, has: false });
        }
    }
    dv(k, deps, fn) {
        (this.derived ??= Object.create(null))[k] = { deps, fn, dirty: true, has: false };
        for (const dep of deps) {
            const dependents = (this.dependents ??= Object.create(null));
            (dependents[dep] ??= new Set()).add(k);
        }
        return this;
    }
    dirty(...keys) {
        for (const k of keys) {
            const d = this.derived?.[k];
            if (d && !d.dirty) {
                d.dirty = true;
                this.#emit({ op: "dirty", k, has: d.has });
            }
        }
        return this;
    }
    g(k) {
        const d = this.derived?.[k];
        if (d) {
            if (d.dirty || !d.has)
                return this.#computeDerived(k, d);
            return d.value;
        }
        return this.#rawGet(k);
    }
    h(k) {
        const d = this.derived?.[k];
        if (d) {
            if (d.dirty || !d.has)
                this.#computeDerived(k, d);
            return true;
        }
        return this.#rawHas(k);
    }
    $debug(limit = 256) {
        this.hist ??= [];
        this.histLimit = limit;
        return this;
    }
    $strict() {
        this.strict = true;
        return this;
    }
    $history() {
        return this.hist ? [...this.hist] : [];
    }
    $why(k) {
        if (!this.hist)
            return undefined;
        for (let i = this.hist.length - 1; i >= 0; i--) {
            if (Object.is(this.hist[i].k, k))
                return this.hist[i];
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
        this.iterating++;
        try {
            for (const k of Reflect.ownKeys(this.data))
                yield [k, this.data[k]];
        }
        finally {
            this.iterating--;
        }
    }
    *keys() {
        this.iterating++;
        try {
            for (const k of Reflect.ownKeys(this.data))
                yield k;
        }
        finally {
            this.iterating--;
        }
    }
    *values() {
        this.iterating++;
        try {
            for (const k of Reflect.ownKeys(this.data))
                yield this.data[k];
        }
        finally {
            this.iterating--;
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
        return { ...this.data };
    }
    snapshot() { return this.toObjectFast(); } //      handy name for hook/selector use
    toJSON() { return this.toObjectFast(); } //     JSON.stringify(store) just works
    select(fn) {
        return fn(this.data);
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
        out.data = { ...obj }; //            direct copy, no per-key emits
        return out;
    }
    static fromEntries(entries) {
        return new O2(entries);
    }
    static from(source) {
        const out = new O2();
        out.data = source instanceof O2 ? { ...source.data } : { ...source };
        return out;
    }
}
