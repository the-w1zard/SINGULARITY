
/**
 const disruptor = new THREE_LANE_DISRUPTOR<A>();

 // Direct access; no getNestedMap.
 
 const prev = disruptor.getState(sl_reg, ms_reg, globalKey, key);
 
 disruptor.setState(sl_reg, ms_reg, globalKey, key, next);
 
 const persistent = disruptor.getPersistent(
     sl_reg, ms_reg, globalKey, key
 );
 
 const persist = disruptor.getPersistKeys(
     sl_reg, ms_reg, globalKey, key
 );
 
 const listeners = disruptor.getListeners(
     sl_reg, ms_reg, globalKey, key
 );
 */

type DisruptorAddress = U;
export type DisruptorHandler = (pid: number, value: number, flag: number) => void;

export class DARTH_VADER<U = any, A = any> {
    private capacity: number;
    private mask: number;
    private size = 0;
    private tombstones = 0;

    private states: Uint8Array;
    private hashes: Uint32Array;
    private a: any[];
    private b: any[];
    private c: any[];
    private k: any[];
    private values: any[];

    private readonly depth: 1 | 2 | 3 | 4;
    private readonly hashValue: (v: any) => number;

    constructor(  depth: 1 | 2 | 3 | 4, capacity = 65536, hashValue: (v: any) => number = (v) => (typeof v === 'string' ? v.length : Number(v) || 0) ) {
        this.depth = depth;
        this.hashValue = hashValue;
        this.capacity = 1;
        while (this.capacity < capacity) this.capacity <<= 1;
        this.mask = this.capacity - 1;

        this.states = new Uint8Array(this.capacity);
        this.hashes = new Uint32Array(this.capacity);
        this.a = new Array(this.capacity);
        this.b = new Array(this.capacity);
        this.c = new Array(this.capacity);
        this.k = new Array(this.capacity);
        this.values = new Array(this.capacity);
    }

    private hash(key: A, s: A, m: A, r: A): number {
        let h = 2166136261;
        const mix = (v: any) => {
            h = Math.imul(h ^ this.hashValue(v), 16777619);
        };

        if (this.depth >= 2) mix(r);
        if (this.depth >= 3) mix(m);
        if (this.depth >= 4) mix(s);
        mix(key);

        return h >>> 0;
    }

    private matches(i: number, key: A, s: A, m: A, r: A): boolean {
        if (this.depth >= 2 && !Object.is(this.a[i], r)) return false;
        if (this.depth >= 3 && !Object.is(this.b[i], m)) return false;
        if (this.depth >= 4 && !Object.is(this.c[i], s)) return false;

        if (this.depth === 1) return Object.is(this.a[i], key);
        if (this.depth === 2) return Object.is(this.b[i], key);
        if (this.depth === 3) return Object.is(this.c[i], key);

        return Object.is(this.k[i], key);
    }

    private locate(key: A, s: A, m: A, r: A, insert: boolean): number {
        const hash = this.hash(key, s, m, r);
        let index = hash & this.mask;
        let tombstone = -1;

        for (let n = 0; n < this.capacity; n++) {
            const state = this.states[index];
            if (state === 0) {
                if (!insert) return -1;
                return tombstone >= 0 ? tombstone : index;
            }
            if (state === 2) {
                if (tombstone < 0) tombstone = index;
            } else if (this.hashes[index] === hash && this.matches(index, key, s, m, r)) {
                return -(index + 2);
            }
            index = (index + 1) & this.mask;
        }
        return insert ? tombstone : -1;
    }

    private grow(): void {
        const oldCapacity = this.capacity;
        const oldStates = this.states;
        const oldHashes = this.hashes;
        const oldA = this.a;
        const oldB = this.b;
        const oldC = this.c;
        const oldK = this.k;
        const oldValues = this.values;

        this.capacity <<= 1;
        this.mask = this.capacity - 1;

        this.states = new Uint8Array(this.capacity);
        this.hashes = new Uint32Array(this.capacity);
        this.a = new Array(this.capacity);
        this.b = new Array(this.capacity);
        this.c = new Array(this.capacity);
        this.k = new Array(this.capacity);
        this.values = new Array(this.capacity);

        this.size = 0;
        this.tombstones = 0;

        for (let i = 0; i < oldCapacity; i++) {
            if (oldStates[i] !== 1) continue;

            const index = this.locate(
                oldA[i],
                this.depth >= 3 ? oldB[i] : undefined,
                this.depth >= 4 ? oldC[i] : undefined,
                this.depth === 2 ? oldB[i] : this.depth === 3 ? oldC[i] : oldK[i],
                true
            );

            this.writeInternal(
                index,
                oldHashes[i],
                oldValues[i],
                oldA[i],
                oldB[i],
                oldC[i],
                oldK[i]
            );
        }
    }

    private writeInternal(i: number, hash: number, value: any, key: A, s: A, m: A, r: A): void {
        if (this.states[i] === 2) this.tombstones--;

        this.states[i] = 1;
        this.hashes[i] = hash;

        this.a[i] = this.depth === 1 ? key : r;
        this.b[i] = this.depth === 2 ? key : m;
        this.c[i] = this.depth === 3 ? key : s;
        this.k[i] = this.depth === 4 ? key : undefined;

        this.values[i] = value;
        this.size++;
    }

    public get<T = U>(key: A, s: A, m: A, r: A): T | undefined {
        const result = this.locate(key, s, m, r, false);
        if (result === -1) return undefined;
        return this.values[-result - 2] as T;
    }

    public has(key: A, s: A, m: A, r: A): boolean {
        return this.locate(key, s, m, r, false) !== -1;
    }

    public set<T>(value: T, key: A, s: A, m: A, r: A): T {
        let index = this.locate(key, s, m, r, false);

        if (index !== -1) {
            this.values[-index - 2] = value;
            return value;
        }

        if ((this.size + this.tombstones + 1) * 10 >= this.capacity * 7) {
            this.grow();
        }

        index = this.locate(key, s, m, r, true);
        this.writeInternal(
            index,
            this.hash(key, s, m, r),
            value,
            key,
            s,
            m,
            r
        );

        return value;
    }

    public delete(key: A, s: A, m: A, r: A): boolean {
        const result = this.locate(key, s, m, r, false);
        if (result === -1) return false;

        const i = -result - 2;
        this.states[i] = 2;
        this.values[i] = undefined;
        this.a[i] = undefined;
        this.b[i] = undefined;
        this.c[i] = undefined;
        this.k[i] = undefined;

        this.size--;
        this.tombstones++;

        return true;
    }

    public get length(): number {
        return this.size;
    }
}
export class SlaveLane<T = any> {
    public readonly values: Array<T>;
    public readonly slots: Int32Array;

    private readonly keyIds = new Map<any, number>();
    private nextId = 0;

    constructor(capacity = 65536) {
        this.values = new Array<T>(capacity);
        this.slots = new Int32Array(capacity).fill(-1);
    }

    // Fast path: key is already a dense integer slot.
    public setById(value: T, id: number): T {
        this.values[id] = value;
        return value;
    }

    public getById<Z = T>(id: number): Z | T | undefined {
        return this.values[id] as Z | T | undefined;
    }

    public deleteById(id: number): boolean {
        if (this.values[id] === undefined) return false;

        this.values[id] = undefined as T;
        return true;
    }

    public hasById(id: number): boolean {
        return this.values[id] !== undefined;
    }

    // Cold-ish path: arbitrary key -> stable integer slot.
    public id(key: any): number {
        let id = this.keyIds.get(key);

        if (id === undefined) {
            id = this.nextId++;
            this.keyIds.set(key, id);
        }

        return id;
    }

    public set(value: any, key: any): any {
        return this.setById(value, this.id(key));
    }

    public get<T2 = T>(key: any): T2 | T| undefined {
        return this.getById<T2>(this.id(key));
    }

    public has(key: any): boolean {
        return this.hasById(this.id(key));
    }

    public delete(key: any): boolean {
        return this.deleteById(this.id(key));
    }
}
export class THREE_LANE_DISRUPTOR<T = any> {
    public R = 'THE_WIZARD_ROOT_REG'
    public M = 'SAURON_MASTER_REG'
    public S = 'SAURON_SLAVE_REG'
    public K = 'THE_DARK_SIDE'
    public capacity: N = 65536
    // 1 << 16
    constructor(root: A, capacity: N= 65536) {
        this.capacity = capacity
        this.R = root.r
        this.M = root.m
        this.S = root.s
        this.K = root.k
    }
    private readonly OBJECT_IDS = new WeakMap<O, N>();
    private nextObjectId = 1;
    private hashText(value: S):N {
        let h = 2166136261;
        for (let i = 0; i < value.length; i++) { h = Math.imul(h ^ value.charCodeAt(i), 16777619); }
        return h >>> 0;
    }
    private hashValue = (v: U): N => {
        if (v === null) return 0x42108421;
        if (v === undefined) return 0x12345678;
        const t = typeof v;
        if (t === "number") {// Fast float/int mix without stringifying
            let h = 2166136261;
            h = Math.imul(h ^ (v as number | 0), 16777619);
            return h >>> 0;
        }
        if (t === "boolean") { return v ? 0x31337001 : 0x31337002; }
        if (t === "string") { return this.hashText(v as string); }
        if (t === "object" || t === "function") {
            const obj = v as object;
            let id = this.OBJECT_IDS.get(obj);
            if (id === undefined) { id = this.nextObjectId++; this.OBJECT_IDS.set(obj, id); }
            return Math.imul(id, 0x9e3779b1) >>> 0;
        }
        return this.hashText(String(v));
    };
    private readonly R_LANE = new DARTH_VADER(2, this.capacity, this.hashValue)
    private readonly M_LANE = new DARTH_VADER(3, this.capacity, this.hashValue)
    private readonly S_LANE = new DARTH_VADER(4, this.capacity, this.hashValue);
    private readonly SS_LANE = new DARTH_VADER<SlaveLane>(  4,  this.capacity, this.hashValue  );
    private static readonly SLAVE_LANE = Symbol("SLAVE_LANE");

    public sLane(s: A = this.S, m: A = this.M, r: A = this.R): SlaveLane {
            const existing = this.SS_LANE.get<SlaveLane>(THREE_LANE_DISRUPTOR.SLAVE_LANE, s, m, r );

        if (existing !== undefined) {        return existing;    }
        const lane = new SlaveLane(this.capacity);

        this.SS_LANE.set(lane,THREE_LANE_DISRUPTOR.SLAVE_LANE, s, m, r );

        return lane;
    }
    public hot_Path = this.sLane( "SAURON", this.M, this.R );
    public r = (() => {
		const fn =<T = A>( key: A, r: A = this.R): T | UD => {return this.R_LANE.get<T>(r, undefined, undefined, key); }
        fn.set= <T>(value: T, key: A, r: A = this.R): T =>{ return this.R_LANE.set(r, undefined, undefined, key, value);  }
        fn.has=(key: A, r: A = this.R): B => {  return this.R_LANE.has(r, undefined, undefined, key);  }
        fn.delete=(key: A, r: A = this.R): B =>{  return this.R_LANE.delete(r, undefined, undefined, key);  }
	return fn;
	})();
    public m = (() => {
		const fn = <T = A>( key: A, m: A = this.M, r: A = this.R ): T | UD => { return this.M_LANE.get<T>(r, m, undefined, key);}
        fn.set=<T>(value: T, key: A,  m: A = this.M, r: A = this.R ): T=> {return this.M_LANE.set(r, m, undefined, key, value); }
        fn.has=( key: A,  m: A = this.M, r: A = this.R): B =>{ return this.M_LANE.has(r, m, undefined, key); }
        fn.delete=( key: A,  m: A = this.M, r: A = this.R): B=> { return this.M_LANE.delete(r, m, undefined, key); }
	return fn;
	})();
    public s = (() => {
		const fn = <T = A>(key: A, s: A = this.S, m: A = this.M, r: A = this.R): T | UD =>{ return this.S_LANE.get<T>(key, s, m, r); }
        fn.set=<T>(value: T, key: A, s: A = this.S, m: A = this.M, r: A = this.R ): T=> { return this.S_LANE.set(value, key, s, m, r); }
        fn.has=(key: A, s: A = this.S, m: A = this.M, r: A = this.R): B =>{ return this.S_LANE.has(key, s, m, r); }
        fn.delete=(key: A, s: A = this.S, m: A = this.M, r: A = this.R): B=> { return this.S_LANE.delete(key, s, m, r); }
        return fn;
	})();
    // , s: A = 'SAURON', m: A = this.M, r: A = this.R
    public state = (() => {
        // Pre-resolve the exact target map/lane for the default 'SAURON', m, r context once during initialization
       /** const targetMap = this.SS_LANE.get<T>('SAURON', this.M, this.R);
        const fn = <T = A>(key: A): T | UD => { return targetMap.get(key); };

        fn.set = <T>(value: T, key: A): T => {
            targetMap.set(key, value);
            return value;
        }; */
        const fn =<T = A>(key: A, s: A = 'THE_CLOUD', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r); }
        fn.set=<T>(value: T, key: A, s: A = 'THE_CLOUD', m: A = this.M, r: A = this.R  ): T=> { return this.s.set(value, key, s, m, r); }
        return fn;

        return fn;
    })();
    public store = (() => {
		const fn =<T = A>(key: A, s: A = 'THE_CLOUD', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r); }
        fn.set=<T>(value: T, key: A, s: A = 'THE_CLOUD', m: A = this.M, r: A = this.R  ): T=> { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    public persistent = (() => {
		const fn =<T = A>(key: S, s: S = '_PERSISTENT', m: S = this.M, r: S = this.R ): T | UD=> { return this.s<T>(key, s, m, r); }
        fn.set=<T>( value: T, key: A,s: S = '_PERSISTENT', m: A = this.M, r: A = this.R ): T=> { return this.s.set(value, key, s, m, r); }
        fn.keys = (() => {
            const fn =(key: A, s: A = '_PERSIST_KEYS', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r)  }
            fn.set=(value: T, key: A, s: A = '_PERSIST_KEYS', m: A = this.M, r: A = this.R  ): T =>{ return this.s.set(value, key, s, m, r); }
            return fn;
        })();
        return fn;
	})();
    public listeners = (() => {
		const fn =<T = Set<(value: A) => V>>( key: A, s: A = '_LISTENERS', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
        fn.set=<T>(value: T, key: A, s: A = '_LISTENERS', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    public _globalNotifyCache = (() => {
		const fn =<T = Set<(value: A) => V>>( key: A, s: A = '_globalNotifyCache', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
        fn.set=<T>(value: T, key: A, s: A = '_globalNotifyCache', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    public _notifyCache = (() => {
		const fn =<T = Set<(value: A) => V>>( key: A, s: A = '_notifyCache', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
        fn.set=<T>(value: T, key: A, s: A = '_notifyCache', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    public _components = (() => {
		const fn =<T = Set<(value: A) => V>>( key: A, s: A = '_components', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
        fn.set=<T>(value: T, key: A, s: A = '_components', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    public _externalSubscribers = (() => {
		const fn =<T = Set<(value: A) => V>>( key: A, s: A = '_externalSubscribers', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
        fn.set=<T>(value: T, key: A, s: A = '_externalSubscribers', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    public _subscribers = (() => {
		const fn =<T = Set<(value: A) => V>>( key: A, s: A = '_subscribers', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
        fn.set=<T>(value: T, key: A, s: A = '_subscribers', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
        return fn;
	})();
    
    public signals = (() => {
		const fn =() => {}
        fn._map = (() => {
            const fn= <T = Set<(value: A) => V>>( key: A, s: A = 'MAUL', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
            fn.set=<T>(value: T, key: A, s: A = 'MAUL', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
            return fn;
        })();
        fn._nakedMap = (() => {
            const fn =<T = Set<(value: A) => V>>( key: A, s: A = 'NAKED_MAUL', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
            fn.set=<T>(value: T, key: A, s: A = 'NAKED_MAUL', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
            return fn;
        })();
        fn._getMetadata = (() => {
            const fn =<T = Set<(value: A) => V>>( key: A, s: A = 'META_MAUL', m: A = this.M, r: A = this.R): T | UD =>{ return this.s<T>(key, s, m, r);}
            fn.set=<T>(value: T, key: A, s: A = 'META_MAUL', m: A = this.M, r: A = this.R): T => { return this.s.set(value, key, s, m, r); }
            return fn;
        })();
        return fn;
	})();

}

/**
 * #region ═════════ THREE_LANE_DISRUPTOR BENCHMARK ═════════
 * Bun benchmark suite comparing THREE_LANE_DISRUPTOR vs nested Map
 * #region ═════════════════════════════════════════════════
 */
import { performance } from "node:perf_hooks";

// [Paste your DARTH_VADER and THREE_LANE_DISRUPTOR classes right here in the file]

const ITERATIONS = 1_000_000;

console.log("=== REAL-WORLD EXACT KEY/VALUE HOT-PATH BENCHMARK ===");
console.log(`Operations per test: ${ITERATIONS.toLocaleString()}\n`);

// Real-world configuration values directly from your environment model
const r = "THE_WIZARD_ROOT_99";
const m = "SAURON_MASTER_CORE_01";
const s = "SAURON_SLAVE_ENGINE_01";
const k = "signal_node_target_key";
const value = {
    active: true,
    state: "RUNNING",
    payload: "data_packet_alpha_omega_99",
    timestamp: Date.now(),
};

const disruptor = new THREE_LANE_DISRUPTOR({ r, m, s, k,});

// 2. Pure Map Engine utilizing your exact _simple_map method

 

import { A2, B2B, D2, ER2, I2, M2B, M2, N2, O2, P2, RE2, SET2, SET2B, SOA_CODEC, S2, U82, USP2, WM2, WS2, RES2, R2, R2I, M2BIN, M2D, M2R } from "#dx";


class SimpleMapEngine {
    public MODULE_CACHE = new M2<any, any>();
    public r = "THE_WIZARD_ROOT_99";
    public m = "SAURON_MASTER_CORE_01";
    public s = "SAURON_SLAVE_ENGINE_01";
    public hot_Path = this._simple_map('SAURON', this.m, this.r)
    public hot_m2b = new M2B();
    public hot_m2bin = new M2BIN<A, A>();
    public hot_m2d= new M2D<A, A>();
    public hot_m2r= new M2R();
    public hot_m2= new M2<A, A>();
    public hot_map= new M2<A, A>();
    public _simple_map(s: any = this.s, m: any = this.m, r: any = this.r): M2<any, any> {
        let root = this.MODULE_CACHE.get(r);  
        if (!root) {
            root = new M2<any, M2<any, M2<any, any>>>();
            this.MODULE_CACHE.set(r, root);   
        }

        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new M2<any, M2<any, any>>();
            root.set(m, masterMap);
        }

        let slaveMap = masterMap.get(s);
        if (!slaveMap) {
            slaveMap = new M2<any, any>();
            masterMap.set(s, slaveMap);
        }

        return slaveMap;
    }
    
}

const sme = new SimpleMapEngine();


// --- WARM UP ---
disruptor.state.set(value, k);
disruptor.store.set(value, k);
disruptor.persistent.set(value, k);

// --- BENCHMARK: EXACT-KEY HOT-PATH SETS (.state.set) ---
console.log("--- EXACT-KEY HOT-PATH SETS (.state.set) ---");

const t1Start = performance.now();
for (let i = 0; i < ITERATIONS; i++) {    disruptor.state.set(value, k);}
const disruptorSetTime = performance.now() - t1Start;
console.log(`Disruptor .state.set(): ${disruptorSetTime.toFixed(2)}ms`);
console.log(`Throughput: ${(ITERATIONS / (disruptorSetTime / 1000)).toLocaleString()} ops/sec\n`);

// --- BENCHMARK: EXACT-KEY HOT-PATH GETS (.state) ---
console.log("--- EXACT-KEY HOT-PATH GETS (.state) ---");

const t2Start = performance.now();
let hits = 0;
for (let i = 0; i < ITERATIONS; i++) { if (disruptor.state(k) !== undefined) { hits++; }}
const disruptorGetTime = performance.now() - t2Start;
console.log(`Disruptor .state():     ${disruptorGetTime.toFixed(2)}ms`);
console.log(`Throughput: ${(ITERATIONS / (disruptorGetTime / 1000)).toLocaleString()} ops/sec`);
console.log(`Valid Hits: ${hits.toLocaleString()}`);

// --- BENCHMARK: PERSISTENT HOT-PATH GETS (.persistent) ---
console.log("\n--- PERSISTENT HOT-PATH GETS (.persistent) ---");

const t3Start = performance.now();
let pHits = 0;
for (let i = 0; i < ITERATIONS; i++) { if (disruptor.persistent(k) !== undefined) { pHits++; }}
const persistentGetTime = performance.now() - t3Start;
console.log(`Disruptor .persistent(): ${persistentGetTime.toFixed(2)}ms`);
console.log(`Throughput: ${(ITERATIONS / (persistentGetTime / 1000)).toLocaleString()} ops/sec`);
console.log(`Valid Hits: ${pHits.toLocaleString()}`);

// --- SET BENCHMARK ---
console.log("--- SET PERFORMANCE (1M writes) ---");
const t1 = performance.now();
for (let i = 0; i < ITERATIONS; i++) {    disruptor.state.set(value, k);}
const disruptorSet = performance.now() - t1;
console.log(`Disruptor .state.set(): ${disruptorSet.toFixed(2)}ms (${(ITERATIONS / (disruptorSet / 1000)).toLocaleString()} ops/sec)`);

const t2 = performance.now();
for (let i = 0; i < ITERATIONS; i++) {    const mm = sme._simple_map("SAURON", m, r);   mm.set(k, value);}
const mapSet = performance.now() - t2;
console.log(`_simple_map().set():    ${mapSet.toFixed(2)}ms (${(ITERATIONS / (mapSet / 1000)).toLocaleString()} ops/sec)`);
console.log(`Speedup multiplier: ${(mapSet / disruptorSet).toFixed(2)}x\n`);

const t33 = performance.now();
for (let i = 0; i < ITERATIONS; i++) { sme.hot_Path.set("SAURON_key", 1); const mm = sme.hot_Path.get("SAURON_key");}
const mapSet2 = performance.now() - t33;
console.log(`sme.hot_Path.get("SAURON_key")   ${mapSet2.toFixed(2)}ms (${(ITERATIONS / (mapSet2 / 1000)).toLocaleString()} ops/sec)`);
console.log(`Speedup multiplier: ${(mapSet2 / disruptorSet).toFixed(2)}x\n`);

const t44 = performance.now();
for (let i = 0; i < ITERATIONS; i++) {     sme.hot_Path.set("SAURON_key", 1)}
const mapSet24 = performance.now() - t44;
console.log(`sme.hot_Path.set("SAURON_key", 1)    ${mapSet24.toFixed(2)}ms (${(ITERATIONS / (mapSet24 / 1000)).toLocaleString()} ops/sec)`);
console.log(`Speedup multiplier: ${(mapSet24 / disruptorSet).toFixed(2)}x\n`);

const t55 = performance.now();
for (let i = 0; i < ITERATIONS; i++) {    disruptor.hot_Path.set(value, k);    disruptor.hot_Path.get(k);}
const mapSet245 = performance.now() - t55;
console.log(`sme.hot_Path.set("SAURON_key", 1)    ${mapSet245.toFixed(2)}ms (${(ITERATIONS / (mapSet245 / 1000)).toLocaleString()} ops/sec)`);
console.log(`Speedup multiplier: ${(mapSet245 / disruptorSet).toFixed(2)}x\n`);

// --- GET BENCHMARK ---
console.log("--- GET PERFORMANCE (1M reads) ---");
const t3 = performance.now();
let dHits = 0;
for (let i = 0; i < ITERATIONS; i++) {    if (disruptor.state(k) !== undefined) dHits++;}
const disruptorGet = performance.now() - t3;
console.log(`Disruptor .state():     ${disruptorGet.toFixed(2)}ms (${(ITERATIONS / (disruptorGet / 1000)).toLocaleString()} ops/sec)`);

const t4 = performance.now();
let mHits = 0;
for (let i = 0; i < ITERATIONS; i++) { const mm  = sme._simple_map("SAURON", m, r) ;if (mm.get(k) !== undefined) mHits++;}
const mapGet = performance.now() - t4;
console.log(`_simple_map().get():    ${mapGet.toFixed(2)}ms (${(ITERATIONS / (mapGet / 1000)).toLocaleString()} ops/sec)`);
console.log(`Speedup multiplier: ${(mapGet / disruptorGet).toFixed(2)}x`);


// ============================================================
// M2 MAP FAMILY BENCHMARK
// ============================================================

console.log("\n--- M2 MAP FAMILY BENCHMARK ---");

// Seed all maps before measuring reads.
sme.hot_map.set(k, value);
sme.hot_m2b.s(1, 1);
sme.hot_m2bin.set(1, 1);
sme.hot_m2d.set(1, value);
sme.hot_m2r.set(1, []);
sme.hot_m2.set(k, value);

// ------------------------------------------------------------
// M2B
// ------------------------------------------------------------
class _serialExecutor {
    private chain: P<V> = Promise.resolve();
    enqueue<T>(fn: () => P<T>): P<T> {
        const run = this.chain.then(fn);
        this.chain = run.then(() => undefined, () => undefined);
        return run;
    }
    get run(): Promise<V> { return this.chain; }
}
export function executor() { return new _serialExecutor(); }

const ex = executor();
let m2bHits = 0;
    let start = performance.now();
    let m2bMixed
    let m2bGet
    let m2bSet 
ex.enqueue(async () => {
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2b.s(1, 1);
    }
      m2bSet = performance.now() - start;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        if (sme.hot_m2b.get(1) !== undefined) m2bHits++;
    }
      m2bGet = performance.now() - start;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2b.s(1, 1);
        sme.hot_m2b.get(1);
    }
      m2bMixed = performance.now() - start;
  });
  let m2binHits
  let m2binSet
  let m2binGet
  let m2binMixed
  ex.enqueue(async () => {
          m2binHits = 0;
          const pids = new Int32Array(ITERATIONS);
          const values = new Array(ITERATIONS);
          
          // pre-populate: dense pid per iteration, object value
          for (let i = 0; i < ITERATIONS; i++) {
              pids[i] = i;
              values[i] = { active: true, state: "RUNNING", payload: "x", timestamp: i };
          }
          
          
    start = performance.now();
    
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2bin.set(pids[i], values[i]);
    }
      m2binSet = performance.now() - start;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        if (sme.hot_m2bin.get(pids[i]) !== undefined) m2binHits++;
    }
      m2binGet = performance.now() - start;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2bin.set(pids[i], values[i]);
        sme.hot_m2bin.get(pids[i]);
    }
      m2binMixed = performance.now() - start;
  });
  let m2dSet 
  let m2dGet
  let m2dHits
  let m2dMixed 
  ex.enqueue(async () => {
          m2dHits = 0;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2d.set(ITERATIONS, value);
    }
      m2dSet = performance.now() - start;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        if (sme.hot_m2d.get(ITERATIONS) !== undefined) m2dHits++;
    }
      m2dGet = performance.now() - start;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2d.set(ITERATIONS, value);
        sme.hot_m2d.get(ITERATIONS);
    }

    m2dMixed = performance.now() - start;
  });
    let m2rMixed
    let m2rHits
    let m2rSet
    let m2rGet
  ex.enqueue(async () => {
      m2rHits = 0;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2r.set(1, []);
    }
      m2rSet = performance.now() - start;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        if (sme.hot_m2r.get(1) !== undefined) m2rHits++;
    }
      m2rGet = performance.now() - start;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2r.set(1, []);
        sme.hot_m2r.get(1);
    }
      m2rMixed = performance.now() - start;
    
  });
  let m2Mixed
  let m2Hits
  let m2Set
  let m2Get 
  ex.enqueue(async () => {
    
      m2Hits = 0;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2.set(k, value);
    }
      m2Set = performance.now() - start;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        if (sme.hot_m2.get(k) !== undefined) m2Hits++;
    }
      m2Get = performance.now() - start;
    
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_m2.set(k, value);
        sme.hot_m2.get(k);
    }
      m2Mixed = performance.now() - start;
  });
  let MAPGet
  let MAPMixed
  let MAPSet
  let MAPHits
  ex.enqueue(async () => {
    
    MAPHits = 0;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {sme.hot_map.set(k, value); }
    MAPSet = performance.now() - start;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        if (sme.hot_map.get(k) !== undefined) MAPHits++;
    }
    MAPGet = performance.now() - start;

    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        sme.hot_map.set(k, value);
        sme.hot_map.get(k);
    }
    MAPMixed = performance.now() - start;
  });
  



// ------------------------------------------------------------
// RESULTS
// ------------------------------------------------------------
ex.enqueue(async () => {
    const reportMap = (
        name: string,
        set: number,
        get: number,
        mixed: number,
        hits: number
    ) => {
        console.log(`\n${name}`);
        console.log(`SET:   ${set.toFixed(2)}ms | ${(ITERATIONS / (set / 1000)).toLocaleString()} ops/sec`);
        console.log(`GET:   ${get.toFixed(2)}ms | ${(ITERATIONS / (get / 1000)).toLocaleString()} ops/sec`);
        console.log(`MIXED: ${mixed.toFixed(2)}ms | ${(ITERATIONS * 2 / (mixed / 1000)).toLocaleString()} ops/sec`);
        console.log(`HITS:  ${hits.toLocaleString()}`);
    };
    
    console.log("\n========== M2 RESULTS ==========");
    
    reportMap("M2B", m2bSet, m2bGet, m2bMixed, m2bHits);
    reportMap("M2BIN", m2binSet, m2binGet, m2binMixed, m2binHits);
    reportMap("M2D", m2dSet, m2dGet, m2dMixed, m2dHits);
    reportMap("M2R", m2rSet, m2rGet, m2rMixed, m2rHits);
    reportMap("M2", m2Set, m2Get, m2Mixed, m2Hits);
    reportMap("MAP", MAPSet, MAPGet, MAPMixed, MAPHits);
    
    
    
    
    console.log("\n========== END M2 RESULTS ==========");
    
    
});


ex.run









