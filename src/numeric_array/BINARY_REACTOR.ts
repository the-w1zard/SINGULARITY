type KEY = string | number | symbol;
type PAYLOAD = unknown;
type EXECUTOR = () => void;
type DISPOSE = () => void;
const enum CELL {
    EMPTY,
    ACTIVE
}
const enum EXEC {
    EMPTY,
    EFFECT,
    COMPUTED
}
const EMPTY = 0;
const TOMBSTONE = 0xFFFFFFFF;
export interface CHANNEL {
    r: KEY;
    m: KEY;
    s: KEY;
    k: KEY;
}
export interface ATOM<T = unknown> extends CHANNEL {
    readonly id: number;
    readonly version: number;
    get(): T;
    peek(): T;
    set(p: T): T;
    update(fn: (v: T) => T): T;
    subscribe(fn: (previous: T, next: T) => void): DISPOSE;
    dispose(): void;
}
// #region
export class BINARY_REACTOR {
    // --------------------------------------------------
    // KEY ARENA
    // --------------------------------------------------
    private keyIds = new Map<KEY, number>();
    private keyValues: (KEY | undefined)[] = [];

    private keyRefs = new Uint32Array(256);

    private keyNextFree = new Uint32Array(256);



    private keyCount = 0;

    private keyFreeHead = 0;



    private retainKey(key: KEY): number {

        const existing = this.keyIds.get(key);



        if (existing !== undefined) {

            this.keyRefs[existing]++;

            return existing;

        }



        let id: number;



        if (this.keyFreeHead) {

            id = this.keyFreeHead;

            this.keyFreeHead = this.keyNextFree[id];

        } else {

            id = ++this.keyCount;



            if (id >= this.keyRefs.length) {

                this.keyRefs = this.grow(this.keyRefs);

                this.keyNextFree = this.grow(this.keyNextFree);

            }

        }



        this.keyValues[id] = key;

        this.keyRefs[id] = 1;

        this.keyNextFree[id] = 0;

        this.keyIds.set(key, id);



        return id;

    }



    private findKey(key: KEY): number {

        return this.keyIds.get(key) ?? 0;

    }



    private releaseKey(id: number): void {

        if (!id || !this.keyRefs[id]) return;



        if (--this.keyRefs[id]) return;



        const key = this.keyValues[id];



        if (key !== undefined) {

            this.keyIds.delete(key);

        }



        this.keyValues[id] = undefined;



        this.keyNextFree[id] = this.keyFreeHead;

        this.keyFreeHead = id;

    }



    // --------------------------------------------------

    // VALUE ARENA

    // --------------------------------------------------



    private values: PAYLOAD[] = [undefined];

    private valueRefs = new Uint32Array(256);

    private valueNextFree = new Uint32Array(256);



    private valueCount = 0;

    private valueFreeHead = 0;



    private encode(p: PAYLOAD): number {

        let id: number;



        if (this.valueFreeHead) {

            id = this.valueFreeHead;

            this.valueFreeHead = this.valueNextFree[id];

        } else {

            id = ++this.valueCount;



            if (id >= this.valueRefs.length) {

                this.valueRefs = this.grow(this.valueRefs);

                this.valueNextFree = this.grow(this.valueNextFree);

            }

        }



        this.values[id] = p;

        this.valueRefs[id] = 1;

        this.valueNextFree[id] = 0;



        return id;

    }



    private decode(id: number): PAYLOAD {

        return this.values[id];

    }



    private releaseValue(id: number): void {

        if (!id || !this.valueRefs[id]) return;



        if (--this.valueRefs[id]) return;



        this.values[id] = undefined;



        this.valueNextFree[id] = this.valueFreeHead;

        this.valueFreeHead = id;

    }



    // --------------------------------------------------

    // CELL ARENA

    // --------------------------------------------------



    private cellState = new Uint8Array(256);

    private cellGeneration = new Uint32Array(256);

    private cellNextFree = new Uint32Array(256);



    private cellValue = new Uint32Array(256);

    private cellVersion = new Float64Array(256);



    private cellR = new Uint32Array(256);

    private cellM = new Uint32Array(256);

    private cellS = new Uint32Array(256);

    private cellK = new Uint32Array(256);



    private cellSubscribers = new Uint32Array(256);

    private cellComputed = new Uint32Array(256);

    private cellDirtyQueued = new Uint8Array(256);



    private cellCount = 0;

    private cellFreeHead = 0;



    // --------------------------------------------------

    // ADDRESS TABLE

    // 0 = empty

    // 0xFFFFFFFF = tombstone

    // --------------------------------------------------



    private addressTable = new Uint32Array(512);

    private addressCount = 0;

    private addressTombstones = 0;



    private hashAddress(

        r: number,

        m: number,

        s: number,

        k: number

    ): number {

        let h = 2166136261;



        h = Math.imul(h ^ r, 16777619);

        h = Math.imul(h ^ m, 16777619);

        h = Math.imul(h ^ s, 16777619);

        h = Math.imul(h ^ k, 16777619);



        return h >>> 0;

    }



    private addressSlot(

        r: number,

        m: number,

        s: number,

        k: number

    ): number {

        const mask = this.addressTable.length - 1;



        let index = this.hashAddress(r, m, s, k) & mask;

        let tombstone = -1;



        while (true) {

            const id = this.addressTable[index];



            if (id === EMPTY) {

                return tombstone === -1 ? index : tombstone;

            }



            if (id === TOMBSTONE) {

                if (tombstone === -1) tombstone = index;

            } else if (

                this.cellR[id] === r &&

                this.cellM[id] === m &&

                this.cellS[id] === s &&

                this.cellK[id] === k

            ) {

                return index;

            }



            index = (index + 1) & mask;

        }

    }



    private findCell(

        r: KEY,

        m: KEY,

        s: KEY,

        k: KEY

    ): number {

        const ri = this.findKey(r);

        const mi = this.findKey(m);

        const si = this.findKey(s);

        const ki = this.findKey(k);



        if (!ri || !mi || !si || !ki) return 0;



        const mask = this.addressTable.length - 1;



        let index = this.hashAddress(ri, mi, si, ki) & mask;



        while (true) {

            const id = this.addressTable[index];



            if (id === EMPTY) return 0;



            if (

                id !== TOMBSTONE &&

                this.cellR[id] === ri &&

                this.cellM[id] === mi &&

                this.cellS[id] === si &&

                this.cellK[id] === ki

            ) {

                return id;

            }



            index = (index + 1) & mask;

        }

    }



    private insertAddress(id: number): void {

        if (

            (this.addressCount + this.addressTombstones + 1) * 4 >=

            this.addressTable.length * 3

        ) {

            this.rehashAddresses();

        }



        const index = this.addressSlot(

            this.cellR[id],

            this.cellM[id],

            this.cellS[id],

            this.cellK[id]

        );



        if (this.addressTable[index] === TOMBSTONE) {

            this.addressTombstones--;

        }



        this.addressTable[index] = id;

        this.addressCount++;

    }



    private removeAddress(id: number): void {

        const index = this.addressSlot(

            this.cellR[id],

            this.cellM[id],

            this.cellS[id],

            this.cellK[id]

        );



        if (this.addressTable[index] !== id) return;



        this.addressTable[index] = TOMBSTONE;

        this.addressCount--;

        this.addressTombstones++;

    }



    private rehashAddresses(): void {

        const length = this.addressTable.length;



        this.addressTable = new Uint32Array(length * 2);

        this.addressCount = 0;

        this.addressTombstones = 0;



        for (let id = 1; id <= this.cellCount; id++) {

            if (this.cellState[id] === CELL.ACTIVE) {

                this.insertAddress(id);

            }

        }

    }



    // --------------------------------------------------

    // EXECUTION ARENA

    // --------------------------------------------------



    private execState = new Uint8Array(256);

    private execGeneration = new Uint32Array(256);

    private execNextFree = new Uint32Array(256);



    private execDirty = new Uint8Array(256);

    private execQueued = new Uint8Array(256);

    private execRunning = new Uint8Array(256);

    private execHasRun = new Uint8Array(256);



    private execOutput = new Uint32Array(256);

    private execDependencies = new Uint32Array(256);



    private execFn: (EXECUTOR | undefined)[] = [];



    private execCount = 0;

    private execFreeHead = 0;



    // --------------------------------------------------

    // SUBSCRIBER ARENA

    // --------------------------------------------------



    private edgeActive = new Uint8Array(256);



    private edgeCell = new Uint32Array(256);

    private edgeExec = new Uint32Array(256);



    private edgeNextCell = new Uint32Array(256);

    private edgePrevCell = new Uint32Array(256);



    private edgeNextExec = new Uint32Array(256);

    private edgePrevExec = new Uint32Array(256);



    private edgeNextFree = new Uint32Array(256);



    private edgeSeenVersion = new Float64Array(256);

    private edgeRunStamp = new Float64Array(256);



    private edgeCount = 0;

    private edgeFreeHead = 0;



    // --------------------------------------------------

    // QUEUES

    // --------------------------------------------------



    private executionQueue = new Uint32Array(256);

    private executionQueueGeneration = new Uint32Array(256);



    private executionHead = 0;

    private executionTail = 0;



    private dirtyQueue = new Uint32Array(256);

    private dirtyHead = 0;

    private dirtyTail = 0;



    // --------------------------------------------------

    // SCHEDULER STATE

    // --------------------------------------------------



    private epoch = 0;

    private currentExecution = 0;

    private currentStamp = 0;



    private batchDepth = 0;

    private flushing = false;



    // --------------------------------------------------

    // BOUNDARY LISTENERS

    // --------------------------------------------------



    private listeners: ((

        (previous: PAYLOAD, next: PAYLOAD) => void

    )[] | undefined)[] = [];



    // --------------------------------------------------

    // ARRAY GROWTH

    // --------------------------------------------------



    private grow<T extends Uint8Array | Uint32Array | Float64Array>(

        old: T

    ): T {

        const Constructor = old.constructor as {

            new(length: number): T

        };



        const next = new Constructor(old.length * 2);

        next.set(old);



        return next;

    }



    private growCells(): void {

        this.cellState = this.grow(this.cellState);

        this.cellGeneration = this.grow(this.cellGeneration);

        this.cellNextFree = this.grow(this.cellNextFree);



        this.cellValue = this.grow(this.cellValue);

        this.cellVersion = this.grow(this.cellVersion);



        this.cellR = this.grow(this.cellR);

        this.cellM = this.grow(this.cellM);

        this.cellS = this.grow(this.cellS);

        this.cellK = this.grow(this.cellK);



        this.cellSubscribers = this.grow(this.cellSubscribers);

        this.cellComputed = this.grow(this.cellComputed);

        this.cellDirtyQueued = this.grow(this.cellDirtyQueued);

    }



    private growExecutions(): void {

        this.execState = this.grow(this.execState);

        this.execGeneration = this.grow(this.execGeneration);

        this.execNextFree = this.grow(this.execNextFree);



        this.execDirty = this.grow(this.execDirty);

        this.execQueued = this.grow(this.execQueued);

        this.execRunning = this.grow(this.execRunning);

        this.execHasRun = this.grow(this.execHasRun);



        this.execOutput = this.grow(this.execOutput);

        this.execDependencies = this.grow(this.execDependencies);

    }



    private growEdges(): void {

        this.edgeActive = this.grow(this.edgeActive);



        this.edgeCell = this.grow(this.edgeCell);

        this.edgeExec = this.grow(this.edgeExec);



        this.edgeNextCell = this.grow(this.edgeNextCell);

        this.edgePrevCell = this.grow(this.edgePrevCell);



        this.edgeNextExec = this.grow(this.edgeNextExec);

        this.edgePrevExec = this.grow(this.edgePrevExec);



        this.edgeNextFree = this.grow(this.edgeNextFree);



        this.edgeSeenVersion = this.grow(this.edgeSeenVersion);

        this.edgeRunStamp = this.grow(this.edgeRunStamp);

    }



    // --------------------------------------------------

    // CELL ALLOCATION

    // --------------------------------------------------



    private allocateCell(

        r: KEY,

        m: KEY,

        s: KEY,

        k: KEY,

        p: PAYLOAD

    ): number {

        let id: number;



        if (this.cellFreeHead) {

            id = this.cellFreeHead;

            this.cellFreeHead = this.cellNextFree[id];

        } else {

            id = ++this.cellCount;



            if (id >= this.cellState.length) {

                this.growCells();

            }

        }



        this.cellState[id] = CELL.ACTIVE;



        if (!this.cellGeneration[id]) {

            this.cellGeneration[id] = 1;

        }



        this.cellR[id] = this.retainKey(r);

        this.cellM[id] = this.retainKey(m);

        this.cellS[id] = this.retainKey(s);

        this.cellK[id] = this.retainKey(k);



        this.cellValue[id] = this.encode(p);

        this.cellVersion[id] = 0;



        this.cellSubscribers[id] = 0;

        this.cellComputed[id] = 0;

        this.cellDirtyQueued[id] = 0;



        this.cellNextFree[id] = 0;

        this.listeners[id] = [];



        this.insertAddress(id);



        return id;

    }



    private assertCell(id: number, generation: number): void {

        if (

            this.cellState[id] !== CELL.ACTIVE ||

            this.cellGeneration[id] !== generation

        ) {

            throw new Error("Attempted to access a disposed atom.");

        }

    }



    // --------------------------------------------------

    // EXECUTION ALLOCATION

    // --------------------------------------------------



    private allocateExecution(

        kind: EXEC,

        fn: EXECUTOR

    ): number {

        let id: number;



        if (this.execFreeHead) {

            id = this.execFreeHead;

            this.execFreeHead = this.execNextFree[id];

        } else {

            id = ++this.execCount;



            if (id >= this.execState.length) {

                this.growExecutions();

            }

        }



        this.execGeneration[id]++;

        if (!this.execGeneration[id]) this.execGeneration[id] = 1;



        this.execState[id] = kind;

        this.execDirty[id] = 1;

        this.execQueued[id] = 0;

        this.execRunning[id] = 0;

        this.execHasRun[id] = 0;



        this.execOutput[id] = 0;

        this.execDependencies[id] = 0;

        this.execNextFree[id] = 0;



        this.execFn[id] = fn;



        return id;

    }



    private releaseExecution(id: number): void {

        if (this.execState[id] === EXEC.EMPTY) return;



        this.cleanupDependencies(id);



        const output = this.execOutput[id];



        if (output && this.cellComputed[output] === id) {

            this.cellComputed[output] = 0;

        }



        this.execState[id] = EXEC.EMPTY;

        this.execDirty[id] = 0;

        this.execQueued[id] = 0;

        this.execRunning[id] = 0;

        this.execHasRun[id] = 0;



        this.execOutput[id] = 0;

        this.execFn[id] = undefined;



        this.execGeneration[id]++;



        this.execNextFree[id] = this.execFreeHead;

        this.execFreeHead = id;

    }



    // --------------------------------------------------

    // SUBSCRIBER ALLOCATION

    // --------------------------------------------------



    private allocateEdge(

        cell: number,

        execution: number,

        stamp: number

    ): number {

        let edge: number;



        if (this.edgeFreeHead) {

            edge = this.edgeFreeHead;

            this.edgeFreeHead = this.edgeNextFree[edge];

        } else {

            edge = ++this.edgeCount;



            if (edge >= this.edgeActive.length) {

                this.growEdges();

            }

        }



        this.edgeActive[edge] = 1;

        this.edgeCell[edge] = cell;

        this.edgeExec[edge] = execution;



        this.edgeSeenVersion[edge] = this.cellVersion[cell];

        this.edgeRunStamp[edge] = stamp;



        const cellHead = this.cellSubscribers[cell];



        this.edgePrevCell[edge] = 0;

        this.edgeNextCell[edge] = cellHead;



        if (cellHead) {

            this.edgePrevCell[cellHead] = edge;

        }



        this.cellSubscribers[cell] = edge;



        const execHead = this.execDependencies[execution];



        this.edgePrevExec[edge] = 0;

        this.edgeNextExec[edge] = execHead;



        if (execHead) {

            this.edgePrevExec[execHead] = edge;

        }



        this.execDependencies[execution] = edge;

        this.edgeNextFree[edge] = 0;



        return edge;

    }



    private removeEdge(edge: number): void {

        if (!this.edgeActive[edge]) return;



        const cell = this.edgeCell[edge];

        const execution = this.edgeExec[edge];



        const previousCell = this.edgePrevCell[edge];

        const nextCell = this.edgeNextCell[edge];



        if (previousCell) {

            this.edgeNextCell[previousCell] = nextCell;

        } else {

            this.cellSubscribers[cell] = nextCell;

        }



        if (nextCell) {

            this.edgePrevCell[nextCell] = previousCell;

        }



        const previousExec = this.edgePrevExec[edge];

        const nextExec = this.edgeNextExec[edge];



        if (previousExec) {

            this.edgeNextExec[previousExec] = nextExec;

        } else {

            this.execDependencies[execution] = nextExec;

        }



        if (nextExec) {

            this.edgePrevExec[nextExec] = previousExec;

        }



        this.edgeActive[edge] = 0;



        this.edgeCell[edge] = 0;

        this.edgeExec[edge] = 0;



        this.edgeNextCell[edge] = 0;

        this.edgePrevCell[edge] = 0;

        this.edgeNextExec[edge] = 0;

        this.edgePrevExec[edge] = 0;



        this.edgeNextFree[edge] = this.edgeFreeHead;

        this.edgeFreeHead = edge;

    }



    private cleanupDependencies(execution: number): void {

        let edge = this.execDependencies[execution];



        while (edge) {

            const next = this.edgeNextExec[edge];

            this.removeEdge(edge);

            edge = next;

        }



        this.execDependencies[execution] = 0;

    }



    private track(cell: number): void {

        const execution = this.currentExecution;



        if (!execution) return;



        const stamp = this.currentStamp;



        let edge = this.execDependencies[execution];



        while (edge) {

            if (this.edgeCell[edge] === cell) {

                if (this.edgeRunStamp[edge] !== stamp) {
                    this.edgeRunStamp[edge] = stamp;
                    this.edgeSeenVersion[edge] = this.cellVersion[cell];
                }

                return;
            }

            edge = this.edgeNextExec[edge];

        }

        this.allocateEdge(cell, execution, stamp);

    }



    private commitDependencies(

        execution: number,

        stamp: number

    ): void {

        let edge = this.execDependencies[execution];



        while (edge) {

            const next = this.edgeNextExec[edge];



            if (this.edgeRunStamp[edge] !== stamp) {

                this.removeEdge(edge);

            }



            edge = next;

        }

    }



    private rollbackDependencies(

        execution: number,

        stamp: number

    ): void {

        let edge = this.execDependencies[execution];



        while (edge) {

            const next = this.edgeNextExec[edge];



            if (this.edgeRunStamp[edge] === stamp) {

                this.removeEdge(edge);

            }



            edge = next;

        }

    }



    // --------------------------------------------------

    // EPOCHS

    // --------------------------------------------------



    private nextEpoch(): number {

        this.epoch++;



        if (this.epoch >= Number.MAX_SAFE_INTEGER) {

            this.epoch = 1;

            this.edgeRunStamp.fill(0);

        }



        return this.epoch;

    }



    // --------------------------------------------------

    // QUEUES

    // --------------------------------------------------



    private enqueueExecution(id: number): void {

        if (

            !id ||

            this.execState[id] === EXEC.EMPTY ||

            this.execQueued[id]

        ) return;



        if (this.executionTail >= this.executionQueue.length) {

            if (this.executionHead) {

                const count = this.executionTail - this.executionHead;



                this.executionQueue.copyWithin(

                    0,

                    this.executionHead,

                    this.executionTail

                );



                this.executionQueueGeneration.copyWithin(

                    0,

                    this.executionHead,

                    this.executionTail

                );



                this.executionHead = 0;

                this.executionTail = count;

            } else {

                this.executionQueue = this.grow(this.executionQueue);

                this.executionQueueGeneration = this.grow(

                    this.executionQueueGeneration

                );

            }

        }



        this.execQueued[id] = 1;



        this.executionQueue[this.executionTail] = id;

        this.executionQueueGeneration[this.executionTail] =

            this.execGeneration[id];



        this.executionTail++;

    }



    private enqueueDirtyCell(id: number): void {

        if (this.cellDirtyQueued[id]) return;



        if (this.dirtyTail >= this.dirtyQueue.length) {

            if (this.dirtyHead) {

                const count = this.dirtyTail - this.dirtyHead;



                this.dirtyQueue.copyWithin(

                    0,

                    this.dirtyHead,

                    this.dirtyTail

                );



                this.dirtyHead = 0;

                this.dirtyTail = count;

            } else {

                this.dirtyQueue = this.grow(this.dirtyQueue);

            }

        }



        this.cellDirtyQueued[id] = 1;

        this.dirtyQueue[this.dirtyTail++] = id;

    }



    // --------------------------------------------------

    // INVALIDATION

    // --------------------------------------------------



    private invalidate(cell: number): void {

        this.enqueueDirtyCell(cell);



        if (!this.batchDepth && !this.currentExecution && !this.flushing) {

            this.flush();

        }

    }



    private propagate(): void {

        while (this.dirtyHead < this.dirtyTail) {

            const cell = this.dirtyQueue[this.dirtyHead++];



            this.cellDirtyQueued[cell] = 0;



            if (this.cellState[cell] !== CELL.ACTIVE) continue;

            const computation = this.cellComputed[cell];
            const invalidated = computation !== 0 && this.execDirty[computation] !== 0;

            let edge = this.cellSubscribers[cell];



            while (edge) {

                const next = this.edgeNextCell[edge];



                if (

                    this.edgeActive[edge] &&

                    (invalidated || this.edgeSeenVersion[edge] !== this.cellVersion[cell])

                ) {

                    const execution = this.edgeExec[edge];

                    const kind = this.execState[execution];



                    if (kind === EXEC.COMPUTED) {

                        if (!this.execDirty[execution]) {

                            this.execDirty[execution] = 1;



                            const output = this.execOutput[execution];



                            if (output) {

                                this.enqueueDirtyCell(output);

                            }

                        }

                    } else if (kind === EXEC.EFFECT) {

                        this.execDirty[execution] = 1;

                        this.enqueueExecution(execution);

                    }

                }



                edge = next;

            }

        }



        this.dirtyHead = 0;

        this.dirtyTail = 0;

    }



    // --------------------------------------------------

    // COMPUTED VALIDATION

    // --------------------------------------------------



    private ensureComputed(cell: number): void {

        const execution = this.cellComputed[cell];



        if (!execution) return;



        if (this.execRunning[execution]) {

            throw new Error("Computed dependency cycle detected.");

        }

        if (!this.execDirty[execution] && this.execHasRun[execution]) return;

        this.runExecution(execution);

    }



    private dependenciesChanged(execution: number): boolean {

        let edge = this.execDependencies[execution];



        while (edge) {

            const cell = this.edgeCell[edge];



            if (this.cellState[cell] !== CELL.ACTIVE) {

                return true;

            }



            this.ensureComputed(cell);



            if (

                this.edgeSeenVersion[edge] !== this.cellVersion[cell]

            ) {

                return true;

            }



            edge = this.edgeNextExec[edge];

        }



        return false;

    }



    // --------------------------------------------------

    // EXECUTION

    // --------------------------------------------------



    private runExecution(execution: number): void {

        const kind = this.execState[execution];



        if (kind === EXEC.EMPTY) return;



        if (this.execRunning[execution]) {

            throw new Error("Reactive execution cycle detected.");

        }



        if (kind === EXEC.EFFECT && !this.execDirty[execution]) {

            return;

        }



        const generation = this.execGeneration[execution];



        // Mark running before pulling dependencies.

        // This catches indirect computed cycles.



        this.execRunning[execution] = 1;



        let changed: boolean;



        try {

            changed = this.dependenciesChanged(execution);

        } catch (error) {

            this.execRunning[execution] = 0;

            throw error;

        }



        if (

            this.execHasRun[execution] &&

            !changed

        ) {

            this.execDirty[execution] = 0;

            this.execRunning[execution] = 0;

            return;

        }



        const previousExecution = this.currentExecution;

        const previousStamp = this.currentStamp;



        const stamp = this.nextEpoch();



        this.currentExecution = execution;

        this.currentStamp = stamp;



        this.execDirty[execution] = 0;



        let succeeded = false;



        try {

            const fn = this.execFn[execution];



            if (fn) fn();



            succeeded = true;

        } finally {

            this.currentExecution = previousExecution;

            this.currentStamp = previousStamp;



            if (

                this.execState[execution] === kind &&

                this.execGeneration[execution] === generation

            ) {

                if (succeeded) {

                    this.commitDependencies(execution, stamp);

                    this.execHasRun[execution] = 1;

                    this.execDirty[execution] = 0;

                } else {

                    this.rollbackDependencies(execution, stamp);

                    this.execDirty[execution] = 1;

                }



                this.execRunning[execution] = 0;

            }

        }

    }



    // --------------------------------------------------

    // SCHEDULER

    // --------------------------------------------------



    private flush(): void {

        if (this.flushing || this.batchDepth || this.currentExecution) {

            return;

        }



        this.flushing = true;



        let firstError: unknown;

        let hasError = false;



        try {

            while (

                this.dirtyHead < this.dirtyTail ||

                this.executionHead < this.executionTail

            ) {

                this.propagate();



                if (this.executionHead >= this.executionTail) {

                    continue;

                }



                const index = this.executionHead++;



                const execution = this.executionQueue[index];

                const generation = this.executionQueueGeneration[index];



                if (

                    this.execGeneration[execution] !== generation ||

                    this.execState[execution] === EXEC.EMPTY

                ) {

                    continue;

                }



                this.execQueued[execution] = 0;



                try {

                    this.runExecution(execution);

                } catch (error) {

                    if (!hasError) {

                        firstError = error;

                        hasError = true;

                    }

                }

            }

        } finally {

            this.executionHead = 0;

            this.executionTail = 0;

            this.flushing = false;

        }



        if (hasError) throw firstError;

    }



    // --------------------------------------------------

    // CELL READ / WRITE

    // --------------------------------------------------



    private readCell(

        id: number,

        generation: number,

        track = true

    ): PAYLOAD {

        this.assertCell(id, generation);

        if (!this.currentExecution && this.dirtyHead < this.dirtyTail) {
            this.propagate();
        }

        this.ensureComputed(id);



        if (!this.currentExecution && !this.flushing && !this.batchDepth) {

            this.flush();

        }



        this.assertCell(id, generation);



        if (track) this.track(id);



        return this.decode(this.cellValue[id]);

    }



    private writeCell(

        id: number,

        generation: number,

        p: PAYLOAD,

        writable = true,

        notify = true

    ): PAYLOAD {

        this.assertCell(id, generation);



        if (!writable) {

            throw new Error("Computed atoms are read-only.");

        }



        const previousHandle = this.cellValue[id];

        const previous = this.decode(previousHandle);



        if (Object.is(previous, p)) return previous;



        const nextHandle = this.encode(p);



        this.cellValue[id] = nextHandle;

        this.cellVersion[id]++;



        this.releaseValue(previousHandle);



        if (notify) this.enqueueDirtyCell(id);



        let listenerError: unknown;

        let hasListenerError = false;



        const listeners = this.listeners[id];



        if (listeners) {

            for (let i = 0; i < listeners.length; i++) {

                try {

                    listeners[i](previous, p);

                } catch (error) {

                    if (!hasListenerError) {

                        listenerError = error;

                        hasListenerError = true;

                    }

                }

            }

        }



        if (!this.batchDepth && !this.currentExecution && !this.flushing) {

            try {

                this.flush();

            } catch (error) {

                if (!hasListenerError) throw error;

            }

        }



        if (hasListenerError) throw listenerError;



        return p;

    }



    // --------------------------------------------------

    // ATOM HANDLE

    // --------------------------------------------------



    private makeAtom<T>(

        id: number,

        generation: number,

        r: KEY,

        m: KEY,

        s: KEY,

        k: KEY,

        writable: boolean

    ): ATOM<T> {

        const reactor = this;



        return {

            id,

            r, m, s, k,



            get version() {

                reactor.assertCell(id, generation);

                return reactor.cellVersion[id];

            },



            get(): T {

                return reactor.readCell(id, generation) as T;

            },



            peek(): T {

                return reactor.readCell(id, generation, false) as T;

            },



            set(p: T): T {

                return reactor.writeCell(

                    id,

                    generation,

                    p,

                    writable

                ) as T;

            },



            update(fn: (v: T) => T): T {

                reactor.assertCell(id, generation);



                if (!writable) {

                    throw new Error("Computed atoms are read-only.");

                }



                const previous = reactor.readCell(

                    id,

                    generation,

                    false

                ) as T;



                return reactor.writeCell(

                    id,

                    generation,

                    fn(previous),

                    true

                ) as T;

            },



            subscribe(fn): DISPOSE {

                reactor.assertCell(id, generation);



                const listeners = reactor.listeners[id]!;

                listeners.push(fn as (previous: PAYLOAD, next: PAYLOAD) => void);



                let active = true;



                return () => {

                    if (!active) return;

                    active = false;



                    const index = listeners.indexOf(fn as (previous: PAYLOAD, next: PAYLOAD) => void);



                    if (index !== -1) {

                        listeners.splice(index, 1);

                    }

                };

            },



            dispose(): void {

                reactor.disposeCell(id, generation);

            }

        };

    }



    // --------------------------------------------------

    // PUBLIC ATOM

    // --------------------------------------------------



    atom<T>(

        p: T,

        k: KEY,

        s: KEY = "\_\_SLAVE\_\_",

        m: KEY = "\_\_MASTER\_\_",

        r: KEY = "\_\_ROOT\_\_"

    ): ATOM<T> {

        let id = this.findCell(r, m, s, k);



        if (!id) {

            id = this.allocateCell(r, m, s, k, p);

        }



        return this.makeAtom<T>(

            id,

            this.cellGeneration[id],

            r, m, s, k,

            this.cellComputed[id] === 0

        );

    }



    // --------------------------------------------------

    // PUBLIC EFFECT

    // --------------------------------------------------



    effect(fn: EXECUTOR): DISPOSE {

        const execution = this.allocateExecution(EXEC.EFFECT, fn);

        const generation = this.execGeneration[execution];



        try {

            this.runExecution(execution);

            this.flush();

        } catch (error) {

            if (

                this.execState[execution] !== EXEC.EMPTY &&

                this.execGeneration[execution] === generation

            ) {

                this.releaseExecution(execution);

            }



            throw error;

        }



        let active = true;



        return () => {

            if (!active) return;

            active = false;



            if (

                this.execState[execution] !== EXEC.EMPTY &&

                this.execGeneration[execution] === generation

            ) {

                this.releaseExecution(execution);

            }

        };

    }



    // --------------------------------------------------

    // PUBLIC COMPUTED

    // --------------------------------------------------



    computed<T>(

        fn: () => T,

        k: KEY,

        s: KEY = "\_\_SLAVE\_\_",

        m: KEY = "\_\_MASTER\_\_",

        r: KEY = "\_\_ROOT\_\_"

    ): ATOM<T> {

        if (this.findCell(r, m, s, k)) {

            throw new Error("Computed address already exists.");

        }



        const output = this.allocateCell(

            r, m, s, k, undefined

        );



        const execution = this.allocateExecution(

            EXEC.COMPUTED,

            () => {

                const value = fn();



                this.writeCell(

                    output,

                    this.cellGeneration[output],

                    value,

                    true,

                    false

                );

            }

        );



        this.execOutput[execution] = output;

        this.cellComputed[output] = execution;



        return this.makeAtom<T>(

            output,

            this.cellGeneration[output],

            r, m, s, k,

            false

        );

    }



    // --------------------------------------------------

    // PUBLIC BATCH

    // --------------------------------------------------



    batch<T>(fn: () => T): T {

        this.batchDepth++;



        let result!: T;

        let operationError: unknown;

        let hasOperationError = false;



        try {

            result = fn();

        } catch (error) {

            operationError = error;

            hasOperationError = true;

        } finally {

            this.batchDepth--;



            if (!this.batchDepth) {

                try {

                    this.flush();

                } catch (error) {

                    if (!hasOperationError) {

                        operationError = error;

                        hasOperationError = true;

                    }

                }

            }

        }



        if (hasOperationError) throw operationError;



        return result;

    }



    // --------------------------------------------------

    // PUBLIC DISPOSAL

    // --------------------------------------------------



    disposeCell(id: number, generation: number): void {

        this.assertCell(id, generation);



        let edge = this.cellSubscribers[id];



        while (edge) {

            const next = this.edgeNextCell[edge];

            const execution = this.edgeExec[edge];

            const kind = this.execState[execution];



            if (kind === EXEC.COMPUTED) {

                if (!this.execDirty[execution]) {

                    this.execDirty[execution] = 1;



                    const output = this.execOutput[execution];



                    if (output && this.cellState[output] === CELL.ACTIVE) {

                        this.enqueueDirtyCell(output);

                    }

                }

            } else if (kind === EXEC.EFFECT) {

                this.execDirty[execution] = 1;

                this.enqueueExecution(execution);

            }



            this.removeEdge(edge);

            edge = next;

        }



        const computation = this.cellComputed[id];



        if (computation) {

            this.releaseExecution(computation);

        }



        this.removeAddress(id);



        this.releaseValue(this.cellValue[id]);



        this.releaseKey(this.cellR[id]);

        this.releaseKey(this.cellM[id]);

        this.releaseKey(this.cellS[id]);

        this.releaseKey(this.cellK[id]);



        this.cellValue[id] = 0;

        this.cellVersion[id] = 0;



        this.cellR[id] = 0;

        this.cellM[id] = 0;

        this.cellS[id] = 0;

        this.cellK[id] = 0;



        this.cellSubscribers[id] = 0;

        this.cellComputed[id] = 0;

        this.cellDirtyQueued[id] = 0;



        this.listeners[id] = undefined;



        this.cellState[id] = CELL.EMPTY;



        this.cellGeneration[id]++;

        if (!this.cellGeneration[id]) this.cellGeneration[id] = 1;



        this.cellNextFree[id] = this.cellFreeHead;

        this.cellFreeHead = id;



        if (!this.batchDepth && !this.currentExecution && !this.flushing) {

            this.flush();

        }

    }

}



export const BINARY = new BINARY_REACTOR();



// #endregion








