
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
	COMPUTED:7,
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
    RECURSEDCHECK: 1 <<8,
    RECURSED: 1 <<9,
    PENDING: 1 <<10,
} as const;
type Lid = number;

export class SIGNALS_DISRUPTOR {
	// #region ═══════════════════════════════════════════════════════════════════════════════════════ SOA
	private readonly capacity: N;
	private readonly lane: I32;
	private readonly ops: U8;
	public readonly posX: F32;
	public readonly posY: F32;
	public readonly val: I32;
	private readonly freePids: I32;
	private freeHead = 0;
	private freeTail = 0;
	private laneLen = 0;
	public nextAllocatedPid = 1;             // 0 = TRACER_PID
    // #endregion
	// #region ═══════════════════════════════════════════════════════════════════════ REACTIVE_COLUMNS
	private readonly rKind: U8;               // RKind per pid
	private readonly rFlags: Uint16Array;     // RX_Flags per pid
	private readonly rValue: unknown[];       // currentValue (signal) / value (computed)
	private readonly rPending: unknown[];     // pendingValue (signal only)
	private readonly rGetter: (((prev?: unknown) => unknown) | undefined)[];
	private readonly rFn: ((() => void | (() => void)) | undefined)[];
	private readonly rCleanup: ((() => void) | undefined)[];
	// private readonly rDeps: (RX_Link | undefined)[];
    private readonly rDeps: Int32Array;
    private readonly rDepsTail: Int32Array;
	//private readonly rDepsTail: (RX_Link | undefined)[];
	//private readonly rSubs: (RX_Link | undefined)[];
	//private readonly rSubsTail: (RX_Link | undefined)[];
    private readonly rSubs: Int32Array;
    private readonly rSubsTail: Int32Array;
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
	// Dedicated effect-flush queue — kept separate from `lane` so a full lane
	// can never silently drop a pending effect (see notes above).
	private readonly effectQueue: I32;
	private queuedLength = 0;
	private notifyIndex = 0;
	public readonly HasChildEffect = 64;
	public cycle = 0;
	public runDepth = 0;
	public batchDepth = 0;
	public activeSub: Pid | undefined;
	constructor(capacity: number = 65_536) {
		this.capacity = capacity  
        // 1024;
        // 65_536
        // 131_072

		this.lane = new Int32Array(this.capacity).fill(-1);
		this.ops = new Uint8Array(this.capacity);
		this.posX = new Float32Array(this.capacity);
		this.posY = new Float32Array(this.capacity);
		this.val = new Int32Array(this.capacity);
		this.freePids = new Int32Array(this.capacity);

		this.rKind = new Uint8Array(this.capacity);
		this.rFlags = new Uint16Array(this.capacity);
		this.rValue = new Array(this.capacity);
		this.rPending = new Array(this.capacity);
		this.rGetter = new Array(this.capacity);
		this.rFn = new Array(this.capacity);
		this.rCleanup = new Array(this.capacity);
		//this.rDeps = new Array(this.capacity);
		//this.rDepsTail = new Array(this.capacity);
		//this.rSubs = new Array(this.capacity);
		//this.rSubsTail = new Array(this.capacity);
        this.rDeps = new Int32Array(this.capacity).fill(-1);
        this.rDepsTail = new Int32Array(this.capacity).fill(-1);

        this.rSubs = new Int32Array(this.capacity).fill(-1);
        this.rSubsTail = new Int32Array(this.capacity).fill(-1);
		this.effectQueue = new Int32Array(this.capacity);
        this.lVersion = new Int32Array(capacity);
        this.lDep      = new Int32Array(capacity);
        this.lSub      = new Int32Array(capacity);

        this.lPrevDep  = new Int32Array(capacity).fill(-1);
        this.lNextDep  = new Int32Array(capacity).fill(-1);

        this.lPrevSub  = new Int32Array(capacity).fill(-1);
        this.lNextSub  = new Int32Array(capacity).fill(-1);
        this.freeLinks = new Int32Array(this.capacity);
		for (let i = 1; i < this.capacity; i++) {
			this.freePids[this.freeTail++] = i;
		}

		this.lane[0] = TRACER_PID;
		this.val[TRACER_PID] = 0;
		this.ops[TRACER_PID] = OP.NONE;
		this.laneLen = 1;
	}
	public check_size_and_edit(x: N, y: N, pid: Pid, value: N) {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.EDIT;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	}
	public getX(pid: N): number { return this.posX[pid]; }
	public getY(pid: N): number { return this.posY[pid]; }
	public getPosition(pid: N): { x: number; y: number } {
		return { x: this.posX[pid], y: this.posY[pid] };
	}
	// --- Wired Operations (Minimum Required Set) — unchanged ---
	public write(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.WRITE;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	}
	public create(x: N, y: N, value: N, pid: Pid): V { this.write(x, y, value, pid); }
	public edit(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.EDIT;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public read(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.READ;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public delete(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.DELETE;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public effect(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.EFFECT;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public compact(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.COMPACT;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public none(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.NONE;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public send(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.SEND;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public sub(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.SUB;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public duplex(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.DUPLEX;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public stream(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.STREAM;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	public drc(x: N, y: N, value: N, pid: Pid): V {
		this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = value;
		this.ops[pid] = OP.DRC;
		if (this.laneLen < this.capacity) this.lane[this.laneLen++] = pid;
	}
	// --- Pass Loop (bug fixed: branch 1 now assigns, matching branches 2-4) ---
	public pass(): V {
		const len = this.laneLen;
		let writeIdx = 0;
		let i = 0;
		const unrolledLimit = len - (len % 4);

		for (; i < unrolledLimit; i += 4) {
			for (let k = 0; k < 4; k++) {
				const pid = this.lane[i + k];
				if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
				else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
				else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
			}
		}

		for (; i < len; i++) {
			const pid = this.lane[i];
			if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
			else if (this.ops[pid] === OP.DELETE) { this.reclaimPid(pid); }
			else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
		}

		this.laneLen = writeIdx;
	}
	/** Runs `pass()` and `flush()` together — one tick for both world-objects and the reactive graph. */
	public tick(): void {
		this.pass();
		this.flush();
	}
	public get tickCount(): N { return this.val[TRACER_PID]; }
	public get length(): N { return this.laneLen; }
	public getLoadFactor(): N {
		const activeCount = Math.max(0, this.laneLen - 1);
		return activeCount / (this.capacity - 1);
	}
	public waitFor() {}
	public sweep_expired() {}
	public on_change() {}
    // #endregion
	// #region ═══════════════════════════════════════════════════════════════════════ PID_LIFECYCLE
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
            const id = this.freeLinks[this.freeLinkHead];
            this.freeLinkHead = (this.freeLinkHead + 1) % this.capacity;
            return id;
        }
    
        return this.nextLink++;
    }
	/** Real now. Tears down reactive state (if any) before returning the pid to the pool. */
	private reclaimPid(pid: Pid): void {
		switch (this.rKind[pid]) {
			case RKind.EFFECT: this.effectOper(pid); break;
			case RKind.EFFECT_SCOPE: this.effectScopeOper(pid); break;
			case RKind.COMPUTED: this.disposeAllDepsInReverse(pid); break;
			// SIGNAL / NONE: nothing to unwind
		}

		this.rKind[pid] = RKind.NONE;
		this.rFlags[pid] = RX_Flags.NONE;
		this.rValue[pid] = undefined;
		this.rPending[pid] = undefined;
		this.rGetter[pid] = undefined;
		this.rFn[pid] = undefined;
		this.rCleanup[pid] = undefined;
		//this.rDeps[pid] = undefined;
		//this.rDepsTail[pid] = undefined;
		//this.rSubs[pid] = undefined;
		//this.rSubsTail[pid] = undefined;
        this.rDeps[pid] = -1;
        this.rDepsTail[pid] = -1;

        this.rSubs[pid] = -1;
        this.rSubsTail[pid] = -1;

		this.freePids[this.freeTail] = pid;
		this.freeTail = (this.freeTail + 1) % this.capacity;
	}
	/** Public disposer — call this instead of invoking an "effectFn" closure. */
	public dispose(pid: Pid): void {
		this.reclaimPid(pid);
	}
    // #endregion
	// #region ═══════════════════════════════════════════════════════════════════════ REACTIVE_ENGINE
	public getActiveSub(): Pid | undefined { return this.activeSub; }
	public setActiveSub(sub?: Pid) {
		const prevSub = this.activeSub;
		this.activeSub = sub;
		return prevSub;
	}
	public getBatchDepth(): number { return this.batchDepth; }
	public startBatch() { ++this.batchDepth; }
	public endBatch() { if (!--this.batchDepth) this.flush(); }
	public update(pid: Pid): boolean {
		switch (this.rKind[pid]) {
			case RKind.COMPUTED: return this.updateComputed(pid);
			case RKind.SIGNAL: return this.updateSignal(pid);
			default:
				this.rFlags[pid] = RX_Flags.MUTABLE;
				return true;
		}
	}
    public notify(effectPid: Pid): void {
		let insertIndex = this.queuedLength;
		let first = insertIndex;
		let pid: Pid = effectPid;

		do {
			this.effectQueue[insertIndex++] = pid;
			this.rFlags[pid] &= ~RX_Flags.WATCHING;
			const headLink = this.rSubs[pid];
			pid = headLink !== -1 ? this.lSub[headLink] : (TRACER_PID as Pid);
			if (pid === TRACER_PID || !(this.rFlags[pid] & RX_Flags.WATCHING)) break;
		} while (true);

		this.queuedLength = insertIndex;

		while (first < --insertIndex) {
			const left = this.effectQueue[first];
			this.effectQueue[first++] = this.effectQueue[insertIndex];
			this.effectQueue[insertIndex] = left;
		}
	}
	public unwatched(pid: Pid): void {
		switch (this.rKind[pid]) {
			case RKind.COMPUTED:
				if (this.rDepsTail[pid] !== undefined) {
					this.rFlags[pid] = RX_Flags.MUTABLE;
					this.disposeAllDepsInReverse(pid);
				}
				break;
			case RKind.SIGNAL:
				break; // always mutable, never dirty until pendingValue changes
			case RKind.EFFECT:
				this.effectOper(pid);
				break;
			case RKind.EFFECT_SCOPE:
				this.effectScopeOper(pid);
				break;
		}
	}
    public lin1(dep: Pid, sub: Pid, version: number): void {
        const prevDep = this.rDepsTail[sub];
        if (  prevDep !== RX_Flags.NONE && this.lDep[prevDep] === dep ) {  return; }
        const nextDep = prevDep !== RX_Flags.NONE ? this.lNextDep[prevDep] : this.rDeps[sub];
        if (  nextDep !== RX_Flags.NONE &&  this.lDep[nextDep] === dep  ) {
            this.lVersion[nextDep] = version;
            this.rDepsTail[sub] = nextDep;
            return;
        }
        const prevSub = this.rSubsTail[dep];
        if (  prevSub !== RX_Flags.NONE && this.lVersion[prevSub] === version &&  this.lSub[prevSub] === sub  ) {
            return;
        }
        const lid = this.allocLink();
        this.lVersion[lid] = version;
        this.lDep[lid] = dep;
        this.lSub[lid] = sub;
        this.lPrevDep[lid] = prevDep;
        this.lNextDep[lid] = nextDep;
        this.lPrevSub[lid] = prevSub;
        this.lNextSub[lid] = RX_Flags.NONE;
        this.rDepsTail[sub] = lid;
        this.rSubsTail[dep] = lid;
        if (nextDep !== RX_Flags.NONE) {
            this.lPrevDep[nextDep] = lid;
        }
        if (prevDep !== RX_Flags.NONE) { this.lNextDep[prevDep] = lid; }
        else {  this.rDeps[sub] = lid; }
        if (prevSub !== RX_Flags.NONE) {  this.lNextSub[prevSub] = lid;  } 
        else { this.rSubs[dep] = lid;  }
    }
	public link(dep: Pid, sub: Pid, version: number): void {
		const prevDep = this.rDepsTail[sub];
		if (prevDep !== -1 && this.lDep[prevDep] === dep) { return; }
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
		if (nextDep !== -1) {
			this.lPrevDep[nextDep] = lid;
		}
		if (prevDep !== -1) { this.lNextDep[prevDep] = lid; }
		else { this.rDeps[sub] = lid; }
		if (prevSub !== -1) { this.lNextSub[prevSub] = lid; }
		else { this.rSubs[dep] = lid; }
	}
    public unlink1(lid: Lid, sub: Pid = this.lSub[lid] ): Lid {
        const dep = this.lDep[lid];
        const prevDep = this.lPrevDep[lid];
        const nextDep = this.lNextDep[lid];
        const prevSub = this.lPrevSub[lid];
        const nextSub = this.lNextSub[lid];
        if (nextDep !== RX_Flags.NONE) this.lPrevDep[nextDep] = prevDep;
        else this.rDepsTail[sub] = prevDep;
        if (prevDep !== RX_Flags.NONE) this.lNextDep[prevDep] = nextDep;
        else this.rDeps[sub] = nextDep;
        if (nextSub !== RX_Flags.NONE) this.lPrevSub[nextSub] = prevSub;
        else this.rSubsTail[dep] = prevSub;
        if (prevSub !== RX_Flags.NONE) { this.lNextSub[prevSub] = nextSub; } 
        else {
            this.rSubs[dep] = nextSub;
            if (nextSub === RX_Flags.NONE) { this.unwatched(dep);  }
        }
        this.freeLink(lid);
        return nextDep;
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
		if (prevSub !== -1) { this.lNextSub[prevSub] = nextSub; }
		else {
			this.rSubs[dep] = nextSub;
			if (nextSub === -1) { this.unwatched(dep); }
		}
		this.freeLink(lid);
		return nextDep;
	}
    private freeLink(lid: Lid): void {
        this.freeLinks[this.freeLinkTail] = lid;
        this.freeLinkTail =
            (this.freeLinkTail + 1) % this.capacity;
    }
	public propagate(link: number, innerWrite: boolean): void {
        let next = this.lNextSub[link];
        let stack: Stack<number | -1> | undefined;
        top: do {
            const sub = this.lSub[link];
            let flags = this.rFlags[sub];
    
            if (!(flags & (RX_Flags.RECURSEDCHECK | RX_Flags.RECURSED | RX_Flags.PENDING))) {
                this.rFlags[sub] = flags | RX_Flags.PENDING;
                if (innerWrite) this.rFlags[sub] |= RX_Flags.RECURSED;
            } else if (!(flags & (RX_Flags.RECURSEDCHECK | RX_Flags.RECURSED))) {
                flags = RX_Flags.NONE;
            } else if (!(flags & RX_Flags.RECURSEDCHECK)) {
                this.rFlags[sub] = (flags & ~RX_Flags.RECURSED) | RX_Flags.PENDING;
            } else if (!(flags & (RX_Flags.PENDING || RX_Flags.PENDING)) && this.isValidLink(link, sub)) {
                this.rFlags[sub] = flags | (RX_Flags.RECURSED | RX_Flags.PENDING);
                flags &= RX_Flags.MUTABLE;
            } else {
                flags = RX_Flags.NONE;
            }
    
            if (flags & RX_Flags.WATCHING) this.notify(sub);
    
            if (flags & RX_Flags.MUTABLE) {
                const subSubs = this.rSubs[sub];
                if (subSubs !== -1) {
                    link = subSubs;
                    const nextSub = this.lNextSub[link];
                    if (nextSub !== -1) {
                        stack = { value: next, prev: stack };
                        next = nextSub;
                    }
                    continue;
                }
            }
    
            if (link !== -1 && next !== -1) {
                link = next;
                next = this.lNextSub[link];
                continue;
            }
    
            while (stack !== undefined) {
                link = stack.value;
                stack = stack.prev;
                if (link !== -1) {
                    next = this.lNextSub[link];
                    continue top;
                }
            }
    
            break;
        } while (true);
    }    
    public checkDirty(link: number, sub: Pid): boolean {
		let stack: Stack<number> | undefined;
		let checkDepth = 0;
		let dirty = false;
		top: do {
			const dep = this.lDep[link];
			const flags = this.rFlags[dep];

			if (this.rFlags[sub]) {
				dirty = true;
			} else if ((flags & RX_Flags.MUTABLE) === RX_Flags.MUTABLE) {
				const subs = this.rSubs[dep];
				if (this.update(dep)) {
					if (subs !== -1) this.shallowPropagate(subs);
					dirty = true;
				}
			} else if ((flags & (RX_Flags.MUTABLE | RX_Flags.PENDING)) === (RX_Flags.MUTABLE | RX_Flags.PENDING)) {
				stack = { value: link, prev: stack };
				link = this.rDeps[dep];
				sub = dep;
				++checkDepth;
				continue;
			}

			if (!dirty) {
				const nextDep = this.lNextDep[link];
				if (nextDep !== -1) { link = nextDep; continue; }
			}

			while (checkDepth--) {
				link = stack!.value;
				stack = stack!.prev;
				if (dirty) {
					const subs = this.rSubs[sub];
					if (this.update(sub)) {
						if (subs !== -1) this.shallowPropagate(subs);
						sub = this.lSub[link];
						continue;
					}
					dirty = false;
				} else {
					this.rFlags[sub] &= ~RX_Flags.PENDING;
				}
				sub = this.lSub[link];
				const nextDep = this.lNextDep[link];
				if (nextDep !== -1) { link = nextDep; continue top; }
			}

			return dirty && !!this.rFlags[sub];
		} while (true);
	}
    public shallowPropagate1(link: number): void {
		do {
			const sub = this.lSub[link];
			const flags = this.rFlags[sub];
			if ((flags & RX_Flags.PENDING) === RX_Flags.PENDING) {
				this.rFlags[sub] = flags;
				if ((flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) === RX_Flags.WATCHING) {
					this.notify(sub);
				}
			}
		} while ((link = this.lNextSub[link]) !== -1);
	}
	public shallowPropagate(link: number): void {
		if (link === -1) return;
		do {
			const sub = this.lSub[link];
			const flags = this.rFlags[sub];
			if ((flags & RX_Flags.PENDING) === RX_Flags.PENDING) {
				this.rFlags[sub] = flags;
				if ((flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) === RX_Flags.WATCHING) {
					this.notify(sub);
				}
			}
		} while ((link = this.lNextSub[link]) !== -1);
	}
	public isValidLink(checkLink: number, sub: Pid): boolean {
		let link = this.rDepsTail[sub];
		while (link !== -1) {
			if (link === checkLink) return true;
			link = this.lPrevDep[link];
		}
		return false;
	}
	public isSignal(pid: Pid): boolean { return this.rKind[pid] === RKind.SIGNAL; }
	public isComputed(pid: Pid): boolean { return this.rKind[pid] === RKind.COMPUTED; }
	public isEffect(pid: Pid): boolean { return this.rKind[pid] === RKind.EFFECT; }
	public isEffectScope(pid: Pid): boolean { return this.rKind[pid] === RKind.EFFECT_SCOPE; }
	public updateComputed(pid: Pid): boolean {
		if (this.rFlags[pid] & this.HasChildEffect) {
			let link = this.rDepsTail[pid];
			while (link !== -1) {
				const prev = this.lPrevDep[link];
				const dep = this.lDep[link];
				if (this.rKind[dep] !== RKind.SIGNAL && this.rKind[dep] !== RKind.COMPUTED) {
					this.unlink(link, pid);
				}
				link = prev;
			}
		}
		this.rDepsTail[pid] = -1;
		this.rFlags[pid] = RX_Flags.MUTABLE | RX_Flags.RECURSEDCHECK;
		const prevSub = this.setActiveSub(pid);
		try {
			++this.cycle;
			const oldValue = this.rValue[pid];
			const nextValue = this.rGetter[pid]!(oldValue);
			this.rValue[pid] = nextValue;
			return !Object.is(oldValue, nextValue);
		} finally {
			this.activeSub = prevSub;
			this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
			this.purgeDeps(pid);
		}
	}
	public updateSignal(pid: Pid): boolean {
		this.rFlags[pid] = RX_Flags.MUTABLE;
		const previous = this.rValue[pid];
		const next = this.rPending[pid];
		this.rValue[pid] = next;
		return !Object.is(previous, next);
	}
    public run1(pid: Pid): void {
		const flags = this.rFlags[pid];
		if (flags || (flags & RX_Flags.PENDING && this.checkDirty(this.rDeps[pid], pid))) {
			if (flags & this.HasChildEffect) {
				let link = this.rDepsTail[pid];
				while (link !== -1) {
					const prev = this.lPrevDep[link];
					const dep = this.lDep[link];
					if (this.rKind[dep] !== RKind.SIGNAL && this.rKind[dep] !== RKind.COMPUTED) {
						this.unlink(link, pid);
					}
					link = prev;
				}
			}
			if (this.rCleanup[pid]) {
				this.runCleanup(pid);
				if (!this.rFlags[pid]) return;
			}
			this.rDepsTail[pid] = -1;
			this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
			const prevSub = this.setActiveSub(pid);
			try {
				++this.cycle;
				++this.runDepth;
				const cleanup = this.rFn[pid]!();
				this.rCleanup[pid] = typeof cleanup === 'function' ? cleanup : undefined;
			} finally {
				--this.runDepth;
				this.activeSub = prevSub;
				this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
				this.purgeDeps(pid);
			}
		} else if (this.rDeps[pid] !== -1) {
			this.rFlags[pid] = RX_Flags.WATCHING | (flags & this.HasChildEffect);
		}
	}
    public run(pid: Pid): void {
		const flags = this.rFlags[pid];
		if (flags & RX_Flags.PENDING ? this.checkDirty(this.rDeps[pid], pid) : flags) {
			if (flags & this.HasChildEffect) {
				let link = this.rDepsTail[pid];
				while (link !== -1) {
					const prev = this.lPrevDep[link];
					const dep = this.lDep[link];
					if (this.rKind[dep] !== RKind.SIGNAL && this.rKind[dep] !== RKind.COMPUTED) {
						this.unlink(link, pid);
					}
					link = prev;
				}
			}
			if (this.rCleanup[pid]) {
				this.runCleanup(pid);
				if (!this.rFlags[pid]) return;
			}
			this.rDepsTail[pid] = -1;
			this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
			const prevSub = this.setActiveSub(pid);
			try {
				++this.cycle;
				++this.runDepth;
				const cleanup = this.rFn[pid]!();
				this.rCleanup[pid] = typeof cleanup === 'function' ? cleanup : undefined;
			} finally {
				--this.runDepth;
				this.activeSub = prevSub;
				this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
				this.purgeDeps(pid);
			}
		} else if (this.rDeps[pid] !== -1) {
			this.rFlags[pid] = RX_Flags.WATCHING | (flags & this.HasChildEffect);
		}
	}
	public flush(): void {
		try {
			while (this.notifyIndex < this.queuedLength) {
				const pid = this.effectQueue[this.notifyIndex];
				this.effectQueue[this.notifyIndex++] = -1;
				this.run(pid);
			}
		} finally {
			while (this.notifyIndex < this.queuedLength) {
				const pid = this.effectQueue[this.notifyIndex];
				this.effectQueue[this.notifyIndex++] = -1;
				this.rFlags[pid] |= RX_Flags.WATCHING | RX_Flags.RECURSED;
			}
			this.notifyIndex = 0;
			this.queuedLength = 0;
		}
	}
	public computedOper<T = unknown>(pid: Pid): T {
		const flags = this.rFlags[pid];
		if (flags & flags & RX_Flags.PENDING && (this.checkDirty(this.rDeps[pid]!, pid) || ((this.rFlags[pid] = flags & ~RX_Flags.PENDING), false))) {
			if (this.updateComputed(pid)) {
				const subs = this.rSubs[pid];
				if (subs !== undefined) this.shallowPropagate(subs);
			}
		} else if (!flags) {
			this.rFlags[pid] = RX_Flags.PENDING | RX_Flags.RECURSEDCHECK;
			const prevSub = this.setActiveSub(pid);
			try {
				this.rValue[pid] = this.rGetter[pid]!();
			} finally {
				this.activeSub = prevSub;
				this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
			}
		}
		const sub = this.activeSub;
		if (sub !== undefined) this.link(pid, sub, this.cycle);
		return this.rValue[pid] as T;
	}
	// NOTE: deduped — the original had an identical branch nested inside its own
	// else-branch (copy/paste leftover). Same behavior, half the code.
	public signalOper<T = unknown>(pid: Pid, ...value: [T] | []): T | void {
		if (value.length) {
			const next = value[0];
			if (!Object.is(this.rPending[pid], next)) {
				this.rPending[pid] = next;
				this.rFlags[pid] = RX_Flags.MUTABLE | RX_Flags.DIRTY;
				const subs = this.rSubs[pid];
				if (subs !== undefined) {
					this.propagate(subs, !!this.runDepth);
					if (!this.batchDepth) this.flush();
				}
			}
			return;
		}

		if (this.rFlags[pid] & RX_Flags.DIRTY) {
			if (this.updateSignal(pid)) {
				const subs = this.rSubs[pid];
				if (subs !== undefined) this.shallowPropagate(subs);
			}
		}
		const sub = this.activeSub;
		if (sub !== undefined) this.link(pid, sub, this.cycle);
		return this.rValue[pid] as T;
	}
	public runCleanup(pid: Pid): void {
		const cleanup = this.rCleanup[pid];
		this.rCleanup[pid] = undefined;
		const prevSub = this.activeSub;
		this.activeSub = undefined;
		try {
			cleanup?.();
		} finally {
			this.activeSub = prevSub;
		}
	}
	public effectOper(pid: Pid): void {
		this.effectScopeOper(pid);
		if (this.rCleanup[pid]) this.runCleanup(pid);
	}
	public effectScopeOper(pid: Pid): void {
		this.rFlags[pid] = RX_Flags.NONE;
		this.disposeAllDepsInReverse(pid);
		const sub = this.rSubs[pid];
		if (sub !== undefined) this.unlink(sub);
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
		const depsTail = this.rDepsTail[pid];
		let dep = depsTail !== -1 ? this.lNextDep[depsTail] : this.rDeps[pid];
		while (dep !== -1) {
			dep = this.unlink(dep, pid);
		}
	}
    // #endregion
	// #region ═══════════════════════════════════════════════════════════════════════ FACTORIES
	private allocNode(kind: RKind): Pid {
		const pid = this.allocPid();
		this.rKind[pid] = kind;
		this.rFlags[pid] = RX_Flags.NONE;
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
		this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
		this.rFn[pid] = fn;

        const prevSub = this.setActiveSub(pid);
		if (prevSub !== undefined && prevSub !== -1) {
			this.rFlags[prevSub] |= this.HasChildEffect;
		}
		try {
			++this.runDepth;
			const cleanup = fn();
			this.rCleanup[pid] = typeof cleanup === 'function' ? cleanup : undefined;
		} finally {
			--this.runDepth;
			this.activeSub = prevSub;
			this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
		}
		return pid;
	}
	public effectScope(fn: () => void): Pid {
		const pid = this.allocNode(RKind.EFFECT_SCOPE);
		this.rFlags[pid] = RX_Flags.MUTABLE;

		const prevSub = this.setActiveSub(pid);
		if (prevSub !== undefined) {
			this.link(pid, prevSub, 0);
			this.rFlags[prevSub] |= this.HasChildEffect;
		}
		try {
			fn();
		} finally {
			this.activeSub = prevSub;
		}
		return pid;
	}
    public trigger(fn: () => void): void {
		const pid = this.allocNode(RKind.EFFECT); // transient watcher, same shape as an effect
		this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
		const prevSub = this.setActiveSub(pid);
		++this.batchDepth;
		try {
			fn();
		} finally {
			this.activeSub = prevSub;
			this.rFlags[pid] = RX_Flags.NONE;
			let link = this.rDeps[pid];
			while (link !== -1) {
				const dep = this.lDep[link];
				link = this.unlink(link, pid);
				const subs = this.rSubs[dep];
				if (subs !== -1) {
					this.propagate(subs, !!this.runDepth);
					this.shallowPropagate(subs);
				}
			}
			if (!--this.batchDepth) this.flush();
			this.reclaimPid(pid); // not retained anywhere
		}
	}
    // #endregion
}


// benchmark.ts
import { performance } from 'node:perf_hooks';
// (Assuming SIGNALS_DISRUPTOR, RKind, OP, RX_Flags, etc. are imported or in scope)
function runBenchmarks() {
    const CAPACITY = 65_536;
    const disruptor = new SIGNALS_DISRUPTOR(CAPACITY);

    console.log(`=== SIGNALS_DISRUPTOR BENCHMARK (Capacity: ${CAPACITY}) ===\n`);

    // -------------------------------------------------------------------------
    // 1. STRESS TEST: Signal Creation Churn
    // -------------------------------------------------------------------------
    const ITERATIONS = 50_000;
    let start = performance.now();
    
    const pids: number[] = [];
    for (let i = 0; i < ITERATIONS; i++) {
        pids.push(disruptor.signal(i));
    }
    const createTime = performance.now() - start;
    console.log(`[Alloc] Created ${ITERATIONS} signals: ${createTime.toFixed(2)}ms`);

    // -------------------------------------------------------------------------
    // 2. STRESS TEST: Write & Propagation Throughput
    // -------------------------------------------------------------------------
    // Build a chain or fan-out of computed/effects to test propagation cost
    const rootSignal = disruptor.signal(0);
    const computedNodes: number[] = [];
    
    let lastPid = rootSignal;
    for (let i = 0; i < 1_000; i++) {
        const c = disruptor.computed(() => disruptor.signalOper<number>(lastPid)! + 1);
        computedNodes.push(c);
        lastPid = c;
    }

    start = performance.now();
    const WRITE_CYCLES = 10_000;
    for (let w = 0; w < WRITE_CYCLES; w++) {
        disruptor.signalOper(rootSignal, w);
    }
    const writeTime = performance.now() - start;
    console.log(`[Throughput] ${WRITE_CYCLES} writes across a 1,000-node computed chain: ${writeTime.toFixed(2)}ms`);
    console.log(`             Average time per write-cascade: ${(writeTime / WRITE_CYCLES).toFixed(4)}ms`);

    // -------------------------------------------------------------------------
    // 3. STRESS TEST: Unified Tick Loop (`pass()` + `flush()`)
    // -------------------------------------------------------------------------
    for (let i = 0; i < 1_000; i++) {
        disruptor.write(i, i, i * 10, pids[i]);
    }

    start = performance.now();
    const TICK_CYCLES = 50_000;
    for (let t = 0; t < TICK_CYCLES; t++) {
        disruptor.tick();
    }
    const tickTime = performance.now() - start;
    console.log(`[Tick Loop] ${TICK_CYCLES} full engine ticks (lane pass + flush): ${tickTime.toFixed(2)}ms`);
    console.log(`            Ticks per second: ${((TICK_CYCLES / tickTime) * 1000).toLocaleString()} tps`);

    // -------------------------------------------------------------------------
    // 4. STRESS TEST: PID Reclaim & Memory Recycling
    // -------------------------------------------------------------------------
    start = performance.now();
    for (let i = 0; i < ITERATIONS; i++) {
        disruptor.dispose(pids[i]);
    }
    const disposeTime = performance.now() - start;
    console.log(`[Reclaim] Disposed/reclaimed ${ITERATIONS} PIDs: ${disposeTime.toFixed(2)}ms`);

    console.log(`\nFinal Engine Load Factor: ${(disruptor.getLoadFactor() * 100).toFixed(2)}%`);
}

runBenchmarks();
