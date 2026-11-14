
type KEY = string | number | symbol;
type PAYLOAD = unknown;
type EXECUTOR = () => void;
type DISPOSE = () => void;

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
    subscribe(fn: (p: T, v: T) => void): DISPOSE;
}

const enum CELL {
    EMPTY,
    ACTIVE
}

const enum EXEC {
    EMPTY,
    EFFECT,
    COMPUTED
}

export class BINARY_REACTOR {
    private state = new Uint8Array(256);
    private flags = new Uint8Array(256);
    private version = new Float64Array(256);
    private values = new Float64Array(256);
    private subscriberOffset = new Uint32Array(256);
    private subscriberCount = new Uint32Array(256);
    private computedExec = new Uint32Array(256);
    private trackStamp = new Uint32Array(256);

    private execState = new Uint8Array(256);
    private execDirty = new Uint8Array(256);
    private execQueued = new Uint8Array(256);
    private execRunning = new Uint8Array(256);
    private execDeps = new Uint32Array(256);
    private execFreeNext = new Uint32Array(256);
    private execFn: EXECUTOR[] = [];
    private execCount = 0;
    private execFreeHead = 0;

    private edgeCell = new Uint32Array(256);
    private edgeExec = new Uint32Array(256);
    private edgeNextCell = new Uint32Array(256);
    private edgePrevCell = new Uint32Array(256);
    private edgeNextExec = new Uint32Array(256);
    private edgePrevExec = new Uint32Array(256);
    private edgeSeenVersion = new Float64Array(256);
    private edgeActive = new Uint8Array(256);
    private edgeCount = 0;
    private edgeFreeHead = 0;

    private queue = new Uint32Array(256);
    private queueHead = 0;
    private queueTail = 0;

    private dirtyCells = new Uint32Array(256);
    private dirtyHead = 0;
    private dirtyTail = 0;

    private cellCount = 0;
    private batchDepth = 0;
    private flushing = false;

    private currentExecution = 0;
    private currentStamp = 0;

    private objectValues: PAYLOAD[] = [undefined];

    private keysR: KEY[] = [];
    private keysM: KEY[] = [];
    private keysS: KEY[] = [];
    private keysK: KEY[] = [];

    private addressTable = new Uint32Array(512);
    private listeners: ((p: unknown, v: unknown) => void)[][] = [];

    private grow<T extends Uint8Array | Uint32Array | Int32Array | Float64Array>(
        old: T
    ): T {
        const next = new (old.constructor as {
            new(length: number): T
        })(old.length * 2);

        next.set(old);
        return next;
    }
    private growCells(): void {
        this.state = this.grow(this.state);
        this.flags = this.grow(this.flags);
        this.version = this.grow(this.version);
        this.values = this.grow(this.values);
        this.subscriberOffset = this.grow(this.subscriberOffset);
        this.subscriberCount = this.grow(this.subscriberCount);
        this.computedExec = this.grow(this.computedExec);
        this.trackStamp = this.grow(this.trackStamp);
    }
    private growExecutions(): void {
        this.execState = this.grow(this.execState);
        this.execDirty = this.grow(this.execDirty);
        this.execQueued = this.grow(this.execQueued);
        this.execRunning = this.grow(this.execRunning);
        this.execDeps = this.grow(this.execDeps);
        this.execFreeNext = this.grow(this.execFreeNext);
    }
    private growEdges(): void {
        this.edgeCell = this.grow(this.edgeCell);
        this.edgeExec = this.grow(this.edgeExec);
        this.edgeNextCell = this.grow(this.edgeNextCell);
        this.edgePrevCell = this.grow(this.edgePrevCell);
        this.edgeNextExec = this.grow(this.edgeNextExec);
        this.edgePrevExec = this.grow(this.edgePrevExec);
        this.edgeSeenVersion = this.grow(this.edgeSeenVersion);
        this.edgeActive = this.grow(this.edgeActive);
    }
    private growQueue(): void {
        this.queue = this.grow(this.queue);
    }
    private growDirtyCells(): void {
        this.dirtyCells = this.grow(this.dirtyCells);
    }
    // --------------------------------------------------
    // VALUE HANDLES
    // --------------------------------------------------
    private encode(p: PAYLOAD): number {
        const id = this.objectValues.length;
        this.objectValues.push(p);
        return id;
    }
    private decode(id: number): PAYLOAD {
        return this.objectValues[id];
    }
    // --------------------------------------------------
    // FOUR-TIER ADDRESSING
    // --------------------------------------------------
    private hash(k: KEY): number {
        const str = typeof k === "symbol"
            ? String(k)
            : `${typeof k}:${k}`;

        let h = 2166136261;

        for (let i = 0; i < str.length; i++) {
            h = Math.imul(h ^ str.charCodeAt(i), 16777619);
        }

        return h >>> 0;
    }
    private hashAddress(
        r: KEY,
        m: KEY,
        s: KEY,
        k: KEY
    ): number {
        let h = this.hash(r);
        h = Math.imul(h ^ this.hash(m), 16777619);
        h = Math.imul(h ^ this.hash(s), 16777619);
        h = Math.imul(h ^ this.hash(k), 16777619);
        return h >>> 0;
    }
    private same(
        id: number,
        r: KEY,
        m: KEY,
        s: KEY,
        k: KEY
    ): boolean {
        return Object.is(this.keysR[id], r)
            && Object.is(this.keysM[id], m)
            && Object.is(this.keysS[id], s)
            && Object.is(this.keysK[id], k);
    }
    private findSlot(
        r: KEY,
        m: KEY,
        s: KEY,
        k: KEY
    ): number {
        const mask = this.addressTable.length - 1;

        let index = this.hashAddress(r, m, s, k) & mask;

        while (this.addressTable[index]) {
            if (this.same(this.addressTable[index], r, m, s, k)) {
                return index;
            }

            index = (index + 1) & mask;
        }

        return index;
    }
    private findCell(
        r: KEY,
        m: KEY,
        s: KEY,
        k: KEY
    ): number {
        return this.addressTable[
            this.findSlot(r, m, s, k)
        ];
    }
    private rehash(): void {
        const old = this.addressTable;

        this.addressTable = new Uint32Array(old.length * 2);

        for (let id = 1; id <= this.cellCount; id++) {
            if (this.state[id] !== CELL.ACTIVE) continue;

            const index = this.findSlot(
                this.keysR[id],
                this.keysM[id],
                this.keysS[id],
                this.keysK[id]
            );

            this.addressTable[index] = id;
        }
    }
    private allocateCell(
        r: KEY,
        m: KEY,
        s: KEY,
        k: KEY,
        p: PAYLOAD
    ): number {
        if (this.cellCount + 1 >= this.state.length) {
            this.growCells();
        }

        const id = ++this.cellCount;

        this.state[id] = CELL.ACTIVE;
        this.values[id] = this.encode(p);

        this.keysR[id] = r;
        this.keysM[id] = m;
        this.keysS[id] = s;
        this.keysK[id] = k;

        if ((this.cellCount + 1) * 4 >= this.addressTable.length * 3) {
            this.rehash();
        }

        const index = this.findSlot(r, m, s, k);

        this.addressTable[index] = id;

        return id;
    }
    // --------------------------------------------------
    // EXECUTION ALLOCATION
    // --------------------------------------------------
    private allocateExecution( kind: EXEC, fn: EXECUTOR ): number {
        let id: number;

        if (this.execFreeHead) {
            id = this.execFreeHead;
            this.execFreeHead = this.execFreeNext[id];
        } else {
            if (this.execCount + 1 >= this.execState.length) {
                this.growExecutions();
            }

            id = ++this.execCount;
        }

        this.execState[id] = kind;
        this.execFn[id] = fn;
        this.execDirty[id] = 1;
        this.execQueued[id] = 0;
        this.execRunning[id] = 0;
        this.execDeps[id] = 0;

        return id;
    }
    private releaseExecution(id: number): void {
        this.cleanup(id);

        this.execState[id] = EXEC.EMPTY;
        this.execDirty[id] = 0;
        this.execQueued[id] = 0;
        this.execRunning[id] = 0;
        this.execFn[id] = undefined as unknown as EXECUTOR;

        this.execFreeNext[id] = this.execFreeHead;
        this.execFreeHead = id;
    }
    // --------------------------------------------------
    // NUMERIC SUBSCRIBER MEMORY
    // --------------------------------------------------

    private allocateEdge( cell: number, execution: number ): number {
        let edge: number;

        if (this.edgeFreeHead) {
            edge = this.edgeFreeHead;
            this.edgeFreeHead = this.edgeNextExec[edge];
        } else {
            if (this.edgeCount + 1 >= this.edgeActive.length) {
                this.growEdges();
            }

            edge = ++this.edgeCount;
        }

        this.edgeActive[edge] = 1;
        this.edgeCell[edge] = cell;
        this.edgeExec[edge] = execution;
        this.edgeSeenVersion[edge] = this.version[cell];

        // Link into cell subscribers.
        const previousCellHead = this.subscriberOffset[cell];

        this.edgePrevCell[edge] = 0;
        this.edgeNextCell[edge] = previousCellHead;

        if (previousCellHead) {
            this.edgePrevCell[previousCellHead] = edge;
        }

        this.subscriberOffset[cell] = edge;
        this.subscriberCount[cell]++;

        // Link into execution dependencies.
        const previousExecHead = this.execDeps[execution];

        this.edgePrevExec[edge] = 0;
        this.edgeNextExec[edge] = previousExecHead;

        if (previousExecHead) {
            this.edgePrevExec[previousExecHead] = edge;
        }

        this.execDeps[execution] = edge;

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
            this.subscriberOffset[cell] = nextCell;
        }

        if (nextCell) {
            this.edgePrevCell[nextCell] = previousCell;
        }

        this.subscriberCount[cell]--;

        const previousExec = this.edgePrevExec[edge];
        const nextExec = this.edgeNextExec[edge];

        if (previousExec) {
            this.edgeNextExec[previousExec] = nextExec;
        } else {
            this.execDeps[execution] = nextExec;
        }

        if (nextExec) {
            this.edgePrevExec[nextExec] = previousExec;
        }

        this.edgeActive[edge] = 0;

        this.edgeNextExec[edge] = this.edgeFreeHead;
        this.edgeFreeHead = edge;
    }
    private cleanup(execution: number): void {
        let edge = this.execDeps[execution];

        while (edge) {
            const next = this.edgeNextExec[edge];
            this.removeEdge(edge);
            edge = next;
        }

        this.execDeps[execution] = 0;
    }
    // --------------------------------------------------
    // DEPENDENCY TRACKING
    // --------------------------------------------------
    private nextStamp(): number {
        this.currentStamp++;

        if (this.currentStamp >= 0xFFFFFFFF) {
            this.trackStamp.fill(0);
            this.currentStamp = 1;
        }

        return this.currentStamp;
    }
    private track(cell: number): void {
        const execution = this.currentExecution;

        if (!execution) return;

        if (this.trackStamp[cell] === this.currentStamp) {
            return;
        }

        this.trackStamp[cell] = this.currentStamp;

        this.allocateEdge(cell, execution);
    }
    // --------------------------------------------------
    // SCHEDULER
    // --------------------------------------------------
    private enqueue(execution: number): void {
        if (
            !execution ||
            this.execState[execution] === EXEC.EMPTY ||
            this.execQueued[execution]
        ) return;

        if (this.queueTail >= this.queue.length) {
            if (this.queueHead) {
                this.queue.copyWithin(
                    0,
                    this.queueHead,
                    this.queueTail
                );

                this.queueTail -= this.queueHead;
                this.queueHead = 0;
            } else {
                this.growQueue();
            }
        }

        this.execQueued[execution] = 1;
        this.queue[this.queueTail++] = execution;
    }
    private enqueueDirtyCell(cell: number): void {
        if (this.dirtyTail >= this.dirtyCells.length) {
            if (this.dirtyHead) {
                this.dirtyCells.copyWithin(
                    0,
                    this.dirtyHead,
                    this.dirtyTail
                );

                this.dirtyTail -= this.dirtyHead;
                this.dirtyHead = 0;
            } else {
                this.growDirtyCells();
            }
        }

        this.dirtyCells[this.dirtyTail++] = cell;
    }
    private invalidate(cell: number): void {
        this.enqueueDirtyCell(cell);

        while (this.dirtyHead < this.dirtyTail) {
            const current = this.dirtyCells[this.dirtyHead++];

            let edge = this.subscriberOffset[current];

            while (edge) {
                const next = this.edgeNextCell[edge];

                if (
                    this.edgeActive[edge] &&
                    this.edgeSeenVersion[edge] !== this.version[current]
                ) {
                    const execution = this.edgeExec[edge];

                    if (this.execState[execution] === EXEC.COMPUTED) {
                        if (!this.execDirty[execution]) {
                            this.execDirty[execution] = 1;

                            enqueueOutput:
                            {
                                const output = this.execOutput[execution];
                                if (output) this.enqueueDirtyCell(output);
                            }
                        }
                    } else {
                        this.enqueue(execution);
                    }
                }

                edge = next;
            }
        }

        this.dirtyHead = 0;
        this.dirtyTail = 0;
    }
    private flush(): void {
        if (
            this.flushing ||
            this.batchDepth ||
            this.currentExecution
        ) return;

        this.flushing = true;

        try {
            while (this.queueHead < this.queueTail) {
                const execution = this.queue[this.queueHead++];

                this.execQueued[execution] = 0;

                if (
                    this.execState[execution] !== EXEC.EMPTY &&
                    this.execDirty[execution]
                ) {
                    this.runExecution(execution);
                }
            }
        } finally {
            this.queueHead = 0;
            this.queueTail = 0;
            this.flushing = false;
        }
    }
    // --------------------------------------------------
    // EXECUTION
    // --------------------------------------------------
    private runExecution(execution: number): void {
        if (
            this.execState[execution] === EXEC.EMPTY ||
            this.execRunning[execution]
        ) return;

        this.cleanup(execution);

        const previousExecution = this.currentExecution;
        const previousStamp = this.currentStamp;

        this.currentExecution = execution;
        this.currentStamp = this.nextStamp();

        this.execRunning[execution] = 1;
        this.execDirty[execution] = 0;

        try {
            this.execFn[execution]();
        } catch (error) {
            this.execDirty[execution] = 1;
            throw error;
        } finally {
            this.execRunning[execution] = 0;
            this.currentExecution = previousExecution;
            this.currentStamp = previousStamp;

            if (!this.currentExecution) {
                this.flush();
            }
        }
    }
    private ensureComputed(cell: number): void {
        const execution = this.computedExec[cell];

        if (
            execution &&
            this.execDirty[execution] &&
            !this.execRunning[execution]
        ) {
            this.runExecution(execution);
        }
    }
    // --------------------------------------------------
    // CELL WRITES
    // --------------------------------------------------
    private writeCell(cell: number, p: PAYLOAD): PAYLOAD {
        const previous = this.decode(this.values[cell]);

        if (Object.is(previous, p)) return previous;

        this.values[cell] = this.encode(p);
        this.version[cell]++;

        this.invalidate(cell);

        const listeners = this.listeners[cell];

        if (listeners) {
            for (let i = 0; i < listeners.length; i++) {
                listeners[i](previous, p);
            }
        }

        return p;
    }
    // --------------------------------------------------
    // PUBLIC API
    // --------------------------------------------------
    atom<T>( p: T, k: KEY, s: KEY = "__SLAVE__", m: KEY = "__MASTER__", r: KEY = "__ROOT__"    ): ATOM<T> {
        let id = this.findCell(r, m, s, k);

        if (!id) {
            id = this.allocateCell(r, m, s, k, p);
        }

        const cell = id;
        const reactor = this;

        return {
            r, m, s, k,
            id: cell,

            get version() {
                return reactor.version[cell];
            },

            get(): T {
                reactor.ensureComputed(cell);
                reactor.track(cell);

                return reactor.decode(
                    reactor.values[cell]
                ) as T;
            },

            peek(): T {
                reactor.ensureComputed(cell);

                return reactor.decode(
                    reactor.values[cell]
                ) as T;
            },

            set(value: T): T {
                return reactor.writeCell(cell, value) as T;
            },

            update(fn): T {
                return reactor.writeCell(
                    cell,
                    fn(reactor.decode(reactor.values[cell]) as T)
                ) as T;
            },

            subscribe(fn): DISPOSE {
                const list = reactor.listeners[cell]
                    ?? (reactor.listeners[cell] = []);

                list.push(fn);

                return () => {
                    const index = list.indexOf(fn);

                    if (index !== -1) {
                        list.splice(index, 1);
                    }
                };
            }
        };
    }
    effect(fn: EXECUTOR): DISPOSE {
        const execution = this.allocateExecution(
            EXEC.EFFECT,
            fn
        );

        this.runExecution(execution);

        return () => this.releaseExecution(execution);
    }
    computed<T>( fn: () => T, k: KEY, s: KEY = "__SLAVE__", m: KEY = "__MASTER__", r: KEY = "__ROOT__"    ): ATOM<T> {
        const output = this.atom<T>(
            undefined as T,
            k, s, m, r
        );

        if (this.computedExec[output.id]) {
            throw new Error("A computed cell already exists at this address.");
        }

        const execution = this.allocateExecution(
            EXEC.COMPUTED,
            () => {
                this.writeCell(output.id, fn());
            }
        );

        this.execOutput[execution] = output.id;
        this.computedExec[output.id] = execution;

        return output;
    }
    batch<T>(fn: () => T): T {
        this.batchDepth++;

        try {
            return fn();
        } finally {
            this.batchDepth--;

            if (!this.batchDepth) {
                this.flush();
            }
        }
    }
    private execOutput = new Uint32Array(256);
}
 
export const BINARY = new BINARY_REACTOR();
