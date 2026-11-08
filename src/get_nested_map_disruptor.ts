
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

type DisruptorAddress = unknown;

class DisruptorLane {
    private capacity: number;
    private size = 0;
    private tombstones = 0;

    private states: Uint8Array;
    private hashes: Uint32Array;

    private a: DisruptorAddress[];
    private b: DisruptorAddress[];
    private c: DisruptorAddress[];
    private k: DisruptorAddress[];
    private values: unknown[];

    constructor(
        private readonly depth: 2 | 3 | 4,
        capacity = 1024,
        private readonly hashValue: (v: unknown) => number
    ) {
        this.capacity = 1;
        while (this.capacity < capacity) this.capacity <<= 1;

        this.states = new Uint8Array(this.capacity);
        this.hashes = new Uint32Array(this.capacity);

        this.a = new Array(this.capacity);
        this.b = new Array(this.capacity);
        this.c = new Array(this.capacity);
        this.k = new Array(this.capacity);
        this.values = new Array(this.capacity);
    }

    private hash(r: unknown, m: unknown, s: unknown, key: unknown): number {
        let h = 2166136261;

        const mix = (v: unknown) => {
            h = Math.imul(h ^ this.hashValue(v), 16777619);
        };

        mix(r);
        if (this.depth >= 3) mix(m);
        if (this.depth >= 4) mix(s);
        mix(key);

        return h >>> 0;
    }

    private matches(
        i: number,
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown
    ): boolean {
        if (!Object.is(this.a[i], r)) return false;
        if (this.depth >= 3 && !Object.is(this.b[i], m)) return false;
        if (this.depth >= 4 && !Object.is(this.c[i], s)) return false;

        if (this.depth === 2) return Object.is(this.b[i], key);
        if (this.depth === 3) return Object.is(this.c[i], key);
        return Object.is(this.k[i], key);
    }

    // Existing slot: -(index + 2)
    // Empty slot: index
    // Missing slot without insertion: -1

    private locate(
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown,
        insert: boolean
    ): number {
        const hash = this.hash(r, m, s, key);
        const mask = this.capacity - 1;

        let index = hash & mask;
        let tombstone = -1;

        for (let n = 0; n < this.capacity; n++) {
            const state = this.states[index];

            if (state === 0) {
                if (!insert) return -1;
                return tombstone >= 0 ? tombstone : index;
            }

            if (state === 2) {
                if (tombstone < 0) tombstone = index;
            } else if (
                this.hashes[index] === hash &&
                this.matches(index, r, m, s, key)
            ) {
                return -(index + 2);
            }

            index = (index + 1) & mask;
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
                this.depth === 2 ? oldB[i] :
                    this.depth === 3 ? oldC[i] : oldK[i],
                true
            );

            this.write(
                index,
                oldHashes[i],
                oldA[i],
                oldB[i],
                oldC[i],
                oldK[i],
                oldValues[i]
            );
        }
    }

    private write(
        i: number,
        hash: number,
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown,
        value: unknown
    ): void {
        if (this.states[i] === 2) this.tombstones--;

        this.states[i] = 1;
        this.hashes[i] = hash;

        this.a[i] = r;
        this.b[i] = this.depth === 2 ? key : m;
        this.c[i] = this.depth === 3 ? key : s;
        this.k[i] = this.depth === 4 ? key : undefined;

        this.values[i] = value;
        this.size++;
    }

    public get<T = unknown>(
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown
    ): T | undefined {
        const result = this.locate(r, m, s, key, false);
        if (result === -1) return undefined;

        return this.values[-result - 2] as T;
    }

    public has(
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown
    ): boolean {
        return this.locate(r, m, s, key, false) !== -1;
    }

    public set<T>(
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown,
        value: T
    ): T {
        let index = this.locate(r, m, s, key, false);

        if (index !== -1) {
            this.values[-index - 2] = value;
            return value;
        }

        if ((this.size + this.tombstones + 1) * 10 >= this.capacity * 7) {
            this.grow();
        }

        index = this.locate(r, m, s, key, true);

        this.write(
            index,
            this.hash(r, m, s, key),
            r, m, s, key, value
        );

        return value;
    }

    public delete(
        r: unknown,
        m: unknown,
        s: unknown,
        key: unknown
    ): boolean {
        const result = this.locate(r, m, s, key, false);
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

export class THREE_LANE_DISRUPTOR<A = any> {
    private readonly OBJECT_IDS = new WeakMap<object, number>();
    private nextObjectId = 1;

    private hashText(value: string): number {
        let h = 2166136261;

        for (let i = 0; i < value.length; i++) {
            h = Math.imul(h ^ value.charCodeAt(i), 16777619);
        }

        return h >>> 0;
    }

    private hashValue = (v: unknown): number => {
        if (v === null) return 0x42108421;
        if (v === undefined) return 0x12345678;

        if (typeof v === "object" || typeof v === "function") {
            const obj = v as object;

            let id = this.OBJECT_IDS.get(obj);

            if (id === undefined) {
                id = this.nextObjectId++;
                this.OBJECT_IDS.set(obj, id);
            }

            return Math.imul(id, 0x9e3779b1) >>> 0;
        }

        return this.hashText(`${typeof v}:${String(v)}`);
    };

    private readonly R_LANE = new DisruptorLane(2, 1024, this.hashValue);
    private readonly M_LANE = new DisruptorLane(3, 1024, this.hashValue);
    private readonly S_LANE = new DisruptorLane(4, 1024, this.hashValue);

    // R: (r, key)

    public getR<T = A>(r: A, key: A): T | undefined {
        return this.R_LANE.get<T>(r, undefined, undefined, key);
    }

    public setR<T>(r: A, key: A, value: T): T {
        return this.R_LANE.set(r, undefined, undefined, key, value);
    }

    public hasR(r: A, key: A): boolean {
        return this.R_LANE.has(r, undefined, undefined, key);
    }

    public deleteR(r: A, key: A): boolean {
        return this.R_LANE.delete(r, undefined, undefined, key);
    }

    // M: (r, m, key)

    public getM<T = A>(r: A, m: A, key: A): T | undefined {
        return this.M_LANE.get<T>(r, m, undefined, key);
    }

    public setM<T>(r: A, m: A, key: A, value: T): T {
        return this.M_LANE.set(r, m, undefined, key, value);
    }

    public hasM(r: A, m: A, key: A): boolean {
        return this.M_LANE.has(r, m, undefined, key);
    }

    public deleteM(r: A, m: A, key: A): boolean {
        return this.M_LANE.delete(r, m, undefined, key);
    }

    // S: (r, m, s, key)

    public getS<T = A>(r: A, m: A, s: A, key: A): T | undefined {
        return this.S_LANE.get<T>(r, m, s, key);
    }

    public setS<T>(r: A, m: A, s: A, key: A, value: T): T {
        return this.S_LANE.set(r, m, s, key, value);
    }

    public hasS(r: A, m: A, s: A, key: A): boolean {
        return this.S_LANE.has(r, m, s, key);
    }

    public deleteS(r: A, m: A, s: A, key: A): boolean {
        return this.S_LANE.delete(r, m, s, key);
    }

    // Specialized S-lane namespace accessors.
    // Argument order preserves the original (s, m, r) convention.

    public getState<T = A>(s: A, m: A, r: A, key: A): T | undefined {
        return this.getS<T>(r, m, s, key);
    }

    public setState<T>(s: A, m: A, r: A, key: A, value: T): T {
        return this.setS(r, m, s, key, value);
    }

    public getPersistent<T = A>(s: A, m: A, r: A, key: A): T | undefined {
        return this.getS<T>(r, m, `${String(s)}_PERSISTENT` as A, key);
    }

    public setPersistent<T>(s: A, m: A, r: A, key: A, value: T): T {
        return this.setS(r, m, `${String(s)}_PERSISTENT` as A, key, value);
    }

    public getPersistKeys(s: A, m: A, r: A, key: A): boolean {
        return this.getS<boolean>(r, m, `${String(s)}_PERSIST_KEYS` as A, key) ?? false;
    }

    public setPersistKeys(s: A, m: A, r: A, key: A, value: boolean): boolean {
        return this.setS(r, m, `${String(s)}_PERSIST_KEYS` as A, key, value);
    }

    public getListeners<T = Set<(value: A) => void>>(
        s: A, m: A, r: A, key: A
    ): T | undefined {
        return this.getS<T>(r, m, `${String(s)}_LISTENERS` as A, key);
    }

    public setListeners<T>(
        s: A, m: A, r: A, key: A, value: T
    ): T {
        return this.setS(r, m, `${String(s)}_LISTENERS` as A, key, value);
    }
}
