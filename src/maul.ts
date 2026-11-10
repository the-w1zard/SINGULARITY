const enum RKind {
    NONE = 0,
    SIGNAL = 1,
    COMPUTED = 2,
    EFFECT = 3,
    EFFECT_SCOPE = 4,
}

const TRACER_PID = 0;

const OP = {
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
} as const;

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
} as const;

type Pid = number;
type Lid = number;

export class DARTH_MAUL {
    // ======================================================================
    // SOA / lane storage — Warlock lane semantics preserved
    // ======================================================================
    private readonly capacity: number;
    private readonly lane: Int32Array;
    private readonly ops: Uint8Array;
    public readonly posX: Float32Array;
    public readonly posY: Float32Array;
    public readonly val: Int32Array;

    private readonly freePids: Int32Array;
    private freeHead = 0;
    private freeTail = 0;
    private laneLen = 0;
    public nextAllocatedPid = 1;

    // Lane deduplication: 1 means this PID already has a pending lane entry.
    private readonly inLane: Uint8Array;

    // ======================================================================
    // Reactive node columns
    // ======================================================================
    private readonly rKind: Uint8Array;
    private readonly rFlags: Uint16Array;
    private readonly rValue: unknown[];
    private readonly rPending: unknown[];
    private readonly rGetter: (((prev?: unknown) => unknown) | undefined)[];
    private readonly rFn: (((() => void | (() => void)) | undefined))[];
    private readonly rCleanup: (((() => void) | undefined))[];

    private readonly rDeps: Int32Array;
    private readonly rDepsTail: Int32Array;
    private readonly rSubs: Int32Array;
    private readonly rSubsTail: Int32Array;

    // ======================================================================
    // Link pool
    // ======================================================================
    private readonly lVersion: Int32Array;
    private readonly lDep: Int32Array;
    private readonly lSub: Int32Array;
    private readonly lPrevDep: Int32Array;
    private readonly lNextDep: Int32Array;
    private readonly lPrevSub: Int32Array;
    private readonly lNextSub: Int32Array;

    private readonly freeLinks: Int32Array;
    private freeLinkHead = 0;
    private freeLinkTail = 0;
    private nextLink = 1;

    // ======================================================================
    // Scheduler
    // ======================================================================
    private readonly effectQueue: Int32Array;
    private queuedLength = 0;
    private notifyIndex = 0;

    public readonly HasChildEffect = 64;

    public cycle = 0;
    public runDepth = 0;
    public batchDepth = 0;
    public activeSub: Pid | undefined;

    constructor(capacity = 65_536) {
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

    private enqueueOp(pid: Pid, op: number): void {
        if (pid === TRACER_PID) return;

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

    public check_size_and_edit(x: number, y: number, pid: Pid, value: number): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.EDIT);
    }

    public getX(pid: Pid): number {
        return this.posX[pid];
    }

    public getY(pid: Pid): number {
        return this.posY[pid];
    }

    public getPosition(pid: Pid): { x: number; y: number } {
        return { x: this.posX[pid], y: this.posY[pid] };
    }

    public write(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.WRITE);
    }

    public create(x: number, y: number, value: number, pid: Pid): void {
        this.write(x, y, value, pid);
    }

    public edit(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.EDIT);
    }

    public read(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.READ);
    }

    public delete(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.DELETE);
    }

    public effect(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.EFFECT);
    }

    public compact(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.COMPACT);
    }

    public none(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.NONE);
    }

    public send(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.SEND);
    }

    public sub(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.SUB);
    }

    public duplex(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.DUPLEX);
    }

    public stream(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.STREAM);
    }

    public drc(x: number, y: number, value: number, pid: Pid): void {
        this.posX[pid] = x;
        this.posY[pid] = y;
        this.val[pid] = value;
        this.enqueueOp(pid, OP.DRC);
    }

    // ======================================================================
    // Lane pass — Warlock structure preserved, dedup markers cleared
    // ======================================================================

    public pass(): void {
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
                } else if (this.ops[pid] === OP.DELETE) {
                    this.inLane[pid] = 0;
                    this.reclaimPid(pid);
                } else {
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
            } else if (this.ops[pid] === OP.DELETE) {
                this.inLane[pid] = 0;
                this.reclaimPid(pid);
            } else {
                this.inLane[pid] = 0;
                this.ops[pid] = OP.NONE;
                this.lane[writeIdx++] = pid;
            }
        }

        this.laneLen = writeIdx;
    }

    /** One tick: world-object pass, then reactive effect flush. */
    public tick(): void {
        this.pass();
        this.flush();
    }

    public get tickCount(): number {
        return this.val[TRACER_PID];
    }

    public get length(): number {
        return this.laneLen;
    }

    public getLoadFactor(): number {
        const activeCount = Math.max(0, this.laneLen - 1);
        return activeCount / (this.capacity - 1);
    }

    public waitFor(): void {}
    public sweep_expired(): void {}
    public on_change(): void {}

    // ======================================================================
    // PID / link lifecycle
    // ======================================================================

    private allocPid(): Pid {
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

    private allocLink(): Lid {
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

    private freeLink(lid: Lid): void {
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

    public dispose(pid: Pid): void {
        this.reclaimPid(pid);
    }

    private reclaimPid(pid: Pid): void {
        if (pid === TRACER_PID) return;
        if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

        switch (this.rKind[pid]) {
            case RKind.EFFECT:
                this.effectOper(pid);
                break;

            case RKind.EFFECT_SCOPE:
                this.effectScopeOper(pid);
                break;

            case RKind.COMPUTED:
                this.disposeAllDepsInReverse(pid);
                break;
        }

        this.rKind[pid] = RKind.NONE;
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

    public link(dep: Pid, sub: Pid, version: number): void {
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

        if (
            prevSub !== -1 &&
            this.lVersion[prevSub] === version &&
            this.lSub[prevSub] === sub
        ) {
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

        if (nextDep !== -1) this.lPrevDep[nextDep] = lid;
        if (prevDep !== -1) this.lNextDep[prevDep] = lid;
        else this.rDeps[sub] = lid;

        if (prevSub !== -1) this.lNextSub[prevSub] = lid;
        else this.rSubs[dep] = lid;
    }

    public unlink(lid: Lid, sub: Pid = this.lSub[lid]): Lid {
        const dep = this.lDep[lid];
        const prevDep = this.lPrevDep[lid];
        const nextDep = this.lNextDep[lid];
        const prevSub = this.lPrevSub[lid];
        const nextSub = this.lNextSub[lid];

        if (nextDep !== -1) this.lPrevDep[nextDep] = prevDep;
        else this.rDepsTail[sub] = prevDep;

        if (prevDep !== -1) this.lNextDep[prevDep] = nextDep;
        else this.rDeps[sub] = nextDep;

        if (nextSub !== -1) this.lPrevSub[nextSub] = prevSub;
        else this.rSubsTail[dep] = prevSub;

        if (prevSub !== -1) {
            this.lNextSub[prevSub] = nextSub;
        } else {
            this.rSubs[dep] = nextSub;

            // Dependency has no subscribers left.
            if (nextSub === -1) {
                this.unwatched(dep);
            }
        }

        this.freeLink(lid);
        return nextDep;
    }

    public disposeAllDepsInReverse(pid: Pid): void {
        let link = this.rDepsTail[pid];

        while (link !== -1) {
            const prev = this.lPrevDep[link];
            this.unlink(link, pid);
            link = prev;
        }
    }

    public purgeDeps(pid: Pid): void {
        const tail = this.rDepsTail[pid];
        let link = tail !== -1 ? this.lNextDep[tail] : this.rDeps[pid];

        while (link !== -1) {
            link = this.unlink(link, pid);
        }
    }

    public unwatched(pid: Pid): void {
        switch (this.rKind[pid]) {
            case RKind.COMPUTED:
                this.rFlags[pid] = RX_Flags.MUTABLE;
                this.disposeAllDepsInReverse(pid);
                break;

            case RKind.SIGNAL:
                break;

            case RKind.EFFECT:
                this.effectOper(pid);
                break;

            case RKind.EFFECT_SCOPE:
                this.effectScopeOper(pid);
                break;
        }
    }

    // ======================================================================
    // Propagation — corrected iterative version
    // ======================================================================

    public propagate(startLink: number, innerWrite: boolean): void {
        if (startLink === -1) return;

        const stack: number[] = [startLink];

        while (stack.length > 0) {
            const link = stack.pop()!;
            const sub = this.lSub[link];

            if (sub === -1) continue;

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
            if (this.rKind[sub] === RKind.COMPUTED) {
                let childLink = this.rSubs[sub];

                while (childLink !== -1) {
                    stack.push(childLink);
                    childLink = this.lNextSub[childLink];
                }
            }
        }
    }

    public shallowPropagate(startLink: number): void {
        let link = startLink;

        while (link !== -1) {
            const sub = this.lSub[link];
            const flags = this.rFlags[sub];

            if (flags & RX_Flags.PENDING) {
                if (
                    (flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) ===
                    RX_Flags.WATCHING
                ) {
                    this.notify(sub);
                }
            }

            link = this.lNextSub[link];
        }
    }

    public notify(effectPid: Pid): void {
        let pid = effectPid;

        while (pid !== -1 && pid !== TRACER_PID) {
            const flags = this.rFlags[pid];

            if (!(flags & RX_Flags.WATCHING)) break;

            // Queue this effect.
            if (this.queuedLength < this.effectQueue.length) {
                this.effectQueue[this.queuedLength++] = pid;
            }

            // Clear WATCHING so it is not queued twice.
            this.rFlags[pid] &= ~RX_Flags.WATCHING;

            // Walk to parent effect/scope, if any.
            const parentLink = this.rSubs[pid];

            if (parentLink === -1) break;

            const parent = this.lSub[parentLink];

            if (
                parent === -1 ||
                (this.rKind[parent] !== RKind.EFFECT &&
                    this.rKind[parent] !== RKind.EFFECT_SCOPE)
            ) {
                break;
            }

            pid = parent;
        }
    }

    // ======================================================================
    // Dirty checking — version-based, no recursive graph walk
    // ======================================================================

    public isDirty(pid: Pid): boolean {
        let link = this.rDeps[pid];

        while (link !== -1) {
            const dep = this.lDep[link];

            if (this.rKind[dep] === RKind.COMPUTED) {
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
    public checkDirty(_link: number, pid: Pid): boolean {
        return this.isDirty(pid);
    }

    // ======================================================================
    // Signal operations
    // ======================================================================

    public signalOper<T>(pid: Pid, ...value: [] | [T]): T | void {
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

        return this.rValue[pid] as T;
    }

    private commitSignal(pid: Pid): void {
        const previous = this.rValue[pid];
        const next = this.rPending[pid];

        this.rFlags[pid] &= ~RX_Flags.DIRTY;

        if (Object.is(previous, next)) return;

        this.rValue[pid] = next;
        this.cycle++;
        this.lVersion[pid] = this.cycle;

        const subs = this.rSubs[pid];

        if (subs !== -1) {
            this.propagate(subs, this.runDepth > 0);
        }
    }

    public updateSignal(pid: Pid): boolean {
        const previous = this.rValue[pid];
        const next = this.rPending[pid];

        this.rValue[pid] = next;

        return !Object.is(previous, next);
    }

    // ======================================================================
    // Computed operations
    // ======================================================================

    public computedOper<T>(pid: Pid): T {
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

        return this.rValue[pid] as T;
    }

    public updateComputed(pid: Pid): boolean {
        if (this.rFlags[pid] & RX_Flags.RUNNING) {
            return false;
        }

        // Remove stale child-effect ownership links before re-evaluating.
        if (this.rFlags[pid] & this.HasChildEffect) {
            let link = this.rDepsTail[pid];

            while (link !== -1) {
                const prev = this.lPrevDep[link];
                const dep = this.lDep[link];

                if (
                    this.rKind[dep] !== RKind.SIGNAL &&
                    this.rKind[dep] !== RKind.COMPUTED
                ) {
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

            const nextValue = this.rGetter[pid]!(previousValue);

            this.rValue[pid] = nextValue;
            this.lVersion[pid] = this.cycle;

            return !Object.is(previousValue, nextValue);
        } finally {
            this.activeSub = previousSub;
            this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
            this.purgeDeps(pid);
        }
    }

    // ======================================================================
    // Effects
    // ======================================================================

    public run(pid: Pid): void {
        if (this.rKind[pid] !== RKind.EFFECT) return;

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

            const cleanup = this.rFn[pid]!();

            this.rCleanup[pid] =
                typeof cleanup === "function" ? (cleanup as () => void) : undefined;
        } finally {
            this.runDepth--;
            this.activeSub = previousSub;
            this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
            this.purgeDeps(pid);
        }
    }

    public runCleanup(pid: Pid): void {
        const cleanup = this.rCleanup[pid];

        if (!cleanup) return;

        this.rCleanup[pid] = undefined;

        const previousSub = this.activeSub;
        this.activeSub = undefined;

        try {
            cleanup();
        } finally {
            this.activeSub = previousSub;
        }
    }

    public effectOper(pid: Pid): void {
        if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

        this.runCleanup(pid);
        this.effectScopeOper(pid);
    }

    public effectScopeOper(pid: Pid): void {
        if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

        // Remove this scope from its parent before disposing children.
        this.unlinkParent(pid);

        // Dispose all child effects/scopes linked to this scope.
        this.disposeAllDepsInReverse(pid);

        this.rFlags[pid] |= RX_Flags.DISPOSED;
    }

    private unlinkParent(pid: Pid): void {
        let link = this.rSubs[pid];

        while (link !== -1) {
            const next = this.lNextSub[link];
            const parent = this.lSub[link];

            if (
                this.rKind[parent] === RKind.EFFECT ||
                this.rKind[parent] === RKind.EFFECT_SCOPE
            ) {
                this.unlink(link, pid);

                // Clear parent ownership flag if it has no remaining children.
                if (this.rDeps[parent] === -1) {
                    this.rFlags[parent] &= ~this.HasChildEffect;
                }
            }

            link = next;
        }
    }

    private scheduleEffect(pid: Pid): void {
        if (this.rFlags[pid] & RX_Flags.PENDING) return;

        this.rFlags[pid] |= RX_Flags.PENDING;

        if (this.queuedLength < this.effectQueue.length) {
            this.effectQueue[this.queuedLength++] = pid;
        }
    }

    // ======================================================================
    // Flush
    // ======================================================================

    public flush(): void {
        try {
            while (this.notifyIndex < this.queuedLength) {
                const pid = this.effectQueue[this.notifyIndex];
                this.effectQueue[this.notifyIndex++] = -1;

                this.rFlags[pid] &= ~RX_Flags.PENDING;
                this.run(pid);
            }
        } finally {
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

    // ======================================================================
    // Factories
    // ======================================================================

    private allocNode(kind: RKind): Pid {
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

    public signal<T>(initialValue?: T): Pid {
        const pid = this.allocNode(RKind.SIGNAL);

        this.rValue[pid] = initialValue;
        this.rPending[pid] = initialValue;
        this.rFlags[pid] = RX_Flags.MUTABLE;

        return pid;
    }

    public computed<T>(getter: (previous?: T) => T): Pid {
        const pid = this.allocNode(RKind.COMPUTED);

        this.rGetter[pid] = getter as (prev?: unknown) => unknown;

        return pid;
    }

    public effectSignal(fn: () => void | (() => void)): Pid {
        const pid = this.allocNode(RKind.EFFECT);

        this.rFn[pid] = fn;
        this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

        const previousSub = this.setActiveSub(pid);

        if (previousSub !== undefined && previousSub !== -1) {
            this.rFlags[previousSub] |= this.HasChildEffect;
        }

        try {
            this.runDepth++;

            const cleanup = fn();

            this.rCleanup[pid] =
                typeof cleanup === "function" ? (cleanup as () => void) : undefined;
        } finally {
            this.runDepth--;
            this.activeSub = previousSub;
            this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
        }

        return pid;
    }

    public effectScope(fn: () => void): Pid {
        const pid = this.allocNode(RKind.EFFECT_SCOPE);

        this.rFlags[pid] = RX_Flags.MUTABLE;

        const previousSub = this.setActiveSub(pid);

        if (previousSub !== undefined && previousSub !== -1) {
            // Parent link: scope is dep, parent is sub.
            this.link(pid, previousSub, 0);
            this.rFlags[previousSub] |= this.HasChildEffect;
        }

        try {
            fn();
        } finally {
            this.activeSub = previousSub;
        }

        return pid;
    }

    public trigger(fn: () => void): void {
        const watcher = this.allocNode(RKind.EFFECT);

        this.rFlags[watcher] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

        const previousSub = this.setActiveSub(watcher);

        this.batchDepth++;

        try {
            fn();
        } finally {
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

    public getActiveSub(): Pid | undefined {
        return this.activeSub;
    }

    public setActiveSub(sub?: Pid): Pid | undefined {
        const previous = this.activeSub;
        this.activeSub = sub;
        return previous;
    }

    public getBatchDepth(): number {
        return this.batchDepth;
    }

    public startBatch(): void {
        this.batchDepth++;
    }

    public endBatch(): void {
        if (--this.batchDepth === 0) {
            this.flush();
        }
    }

    public isSignal(pid: Pid): boolean {
        return this.rKind[pid] === RKind.SIGNAL;
    }

    public isComputed(pid: Pid): boolean {
        return this.rKind[pid] === RKind.COMPUTED;
    }

    public isEffect(pid: Pid): boolean {
        return this.rKind[pid] === RKind.EFFECT;
    }

    public isEffectScope(pid: Pid): boolean {
        return this.rKind[pid] === RKind.EFFECT_SCOPE;
    }
}
