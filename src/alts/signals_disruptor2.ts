// warlock-disruptor-signals.ts
export const TRACER_PID = 0;

export const OP = {
  NONE: 0,
  WRITE: 1,
  DELETE: 2,
  EFFECT: 3,
    EDIT: 6,
    DISPOSE_SCOPE: 4,
} as const;

export const KIND = {
  FREE: 0,
  SIGNAL: 1,
  COMPUTED: 2,
  EFFECT: 3,
  SCOPE: 4,
} as const;

export const FLAG = {
  NONE: 0,
  WATCHING: 1,
  RUNNING: 2,
  DIRTY: 4,
  PENDING: 8,
  DISPOSED: 16,
  HAS_CHILD_EFFECT: 32,
} as const;

type Teardown = void | (() => void);
type Getter<T> = (previous?: T) => T;

export class WarlockDisruptor {
  private readonly capacity: number;

  // Existing spatial/entity lane
  private readonly lane: Int32Array;
  private readonly ops: Uint8Array;
  public readonly posX: Float32Array;
  public readonly posY: Float32Array;
  public readonly val: Int32Array;

  // PID free list
  private readonly freePids: Int32Array;
  private freeHead = 0;
  private freeTail = 0;
  private laneLen = 0;
  public nextAllocatedPid = 1;

  // Signal-graph node storage
  private readonly kind: Uint8Array;
  private readonly flags: Uint8Array;
  private readonly value: Array<any>;
  private readonly pendingValue: Array<any>;
  private readonly version: Float64Array;
  private readonly getter: Array<Getter<any> | undefined>;
  private readonly cleanup: Array<Teardown>;

  // Dependency/subscriber links, stored as a doubly linked graph
  private readonly linkDep: Int32Array;
  private readonly linkSub: Int32Array;
  private readonly linkPrevDep: Int32Array;
  private readonly linkNextDep: Int32Array;
  private readonly linkPrevSub: Int32Array;
  private readonly linkNextSub: Int32Array;
  private readonly linkVersion: Float64Array;
  private readonly linkFree: Int32Array;
  private linkFreeTop = 0;

  private readonly depsHead: Int32Array;
  private readonly depsTail: Int32Array;
  private readonly subsHead: Int32Array;
  private readonly subsTail: Int32Array;

  // Scheduler
  private readonly notifyQueue: Int32Array;
  private notifyHead = 0;
  private notifyTail = 0;

  private activeSub = -1;
  private readonly activeStack: number[] = [];
  private cycle = 0;
  private batchDepth = 0;
  private runDepth = 0;

  constructor(capacity = 1024) {
    this.capacity = capacity;

    this.lane = new Int32Array(capacity).fill(-1);
    this.ops = new Uint8Array(capacity);
    this.posX = new Float32Array(capacity);
    this.posY = new Float32Array(capacity);
    this.val = new Int32Array(capacity);

    this.freePids = new Int32Array(capacity);
    for (let i = capacity - 1; i >= 1; i--) {
      this.freePids[this.freeTail++] = i;
    }

    this.lane[0] = TRACER_PID;
    this.val[TRACER_PID] = 0;
    this.ops[TRACER_PID] = OP.NONE;
    this.laneLen = 1;

    const n = capacity;

    this.kind = new Uint8Array(n);
    this.flags = new Uint8Array(n);
    this.value = new Array(n);
    this.pendingValue = new Array(n);
    this.version = new Float64Array(n);
    this.getter = new Array(n);
    this.cleanup = new Array(n);

    this.linkDep = new Int32Array(n * 4);
    this.linkSub = new Int32Array(n * 4);
    this.linkPrevDep = new Int32Array(n * 4);
    this.linkNextDep = new Int32Array(n * 4);
    this.linkPrevSub = new Int32Array(n * 4);
    this.linkNextSub = new Int32Array(n * 4);
    this.linkVersion = new Float64Array(n * 4);
    this.linkFree = new Int32Array(n * 4);

    for (let i = (n * 4) - 1; i >= 0; i--) {
      this.linkFree[this.linkFreeTop++] = i;
    }

    this.depsHead = new Int32Array(n).fill(-1);
    this.depsTail = new Int32Array(n).fill(-1);
    this.subsHead = new Int32Array(n).fill(-1);
    this.subsTail = new Int32Array(n).fill(-1);

    this.notifyQueue = new Int32Array(n);
  }

  // ==========================================================================
  // PID lifecycle
  // ==========================================================================

  private allocPid(kind: number): number {
    if (this.freeHead === this.freeTail) {
      throw new Error("DISRUPTOR: signal capacity exhausted");
    }

    const pid = this.freePids[this.freeHead++];

    this.kind[pid] = kind;
    this.flags[pid] = FLAG.NONE;
    this.value[pid] = undefined;
    this.pendingValue[pid] = undefined;
    this.version[pid] = 0;
    this.getter[pid] = undefined;
    this.cleanup[pid] = undefined;

    this.depsHead[pid] = -1;
    this.depsTail[pid] = -1;
    this.subsHead[pid] = -1;
    this.subsTail[pid] = -1;

    return pid;
  }

  private reclaimPid(pid: number): void {
    if (pid === TRACER_PID || this.kind[pid] === KIND.FREE) return;

    this.disposeNode(pid);

    this.kind[pid] = KIND.FREE;
    this.flags[pid] = FLAG.NONE;
    this.value[pid] = undefined;
    this.pendingValue[pid] = undefined;
    this.getter[pid] = undefined;
    this.cleanup[pid] = undefined;

    this.freePids[--this.freeHead] = pid;
  }

  // ==========================================================================
  // Link graph
  // ==========================================================================

  private allocLink(): number {
    if (this.linkFreeTop === 0) {
      throw new Error("DISRUPTOR: link capacity exhausted");
    }
    return this.linkFree[--this.linkFreeTop];
  }

  private freeLink(link: number): void {
    this.linkDep[link] = -1;
    this.linkSub[link] = -1;
    this.linkPrevDep[link] = -1;
    this.linkNextDep[link] = -1;
    this.linkPrevSub[link] = -1;
    this.linkNextSub[link] = -1;
    this.linkVersion[link] = 0;
    this.linkFree[this.linkFreeTop++] = link;
  }

  private link(dep: number, sub: number): void {
    // Already linked during this tracking cycle?
    let link = this.depsTail[sub];

    while (link !== -1) {
      if (this.linkDep[link] === dep) {
        this.linkVersion[link] = this.cycle;
        return;
      }
      link = this.linkPrevDep[link];
    }

    const newLink = this.allocLink();

    const prevDep = this.depsTail[sub];
    const prevSub = this.subsTail[dep];

    this.linkDep[newLink] = dep;
    this.linkSub[newLink] = sub;
    this.linkVersion[newLink] = this.cycle;

    this.linkPrevDep[newLink] = prevDep;
    this.linkNextDep[newLink] = -1;
    this.linkPrevSub[newLink] = prevSub;
    this.linkNextSub[newLink] = -1;

    if (prevDep !== -1) this.linkNextDep[prevDep] = newLink;
    else this.depsHead[sub] = newLink;

    if (prevSub !== -1) this.linkNextSub[prevSub] = newLink;
    else this.subsHead[dep] = newLink;

    this.depsTail[sub] = newLink;
    this.subsTail[dep] = newLink;
  }

  private unlink(link: number): void {
    const dep = this.linkDep[link];
    const sub = this.linkSub[link];

    const prevDep = this.linkPrevDep[link];
    const nextDep = this.linkNextDep[link];
    const prevSub = this.linkPrevSub[link];
    const nextSub = this.linkNextSub[link];

    if (prevDep !== -1) this.linkNextDep[prevDep] = nextDep;
    else this.depsHead[sub] = nextDep;

    if (nextDep !== -1) this.linkPrevDep[nextDep] = prevDep;
    else this.depsTail[sub] = prevDep;

    if (prevSub !== -1) this.linkNextSub[prevSub] = nextSub;
    else this.subsHead[dep] = nextSub;

    if (nextSub !== -1) this.linkPrevSub[nextSub] = prevSub;
    else this.subsTail[dep] = prevSub;

    this.freeLink(link);
  }

  private unlinkAllDeps(sub: number): void {
    let link = this.depsTail[sub];

    while (link !== -1) {
      const prev = this.linkPrevDep[link];
      this.unlink(link);
      link = prev;
    }
  }

  private unlinkAllSubs(dep: number): void {
    let link = this.subsTail[dep];

    while (link !== -1) {
      const prev = this.linkPrevSub[link];
      this.unlink(link);
      link = prev;
    }
  }

  // ==========================================================================
  // Public signal API
  // ==========================================================================

  public signal<T>(initialValue: T): {
    (): T;
    (value: T): void;
  } {
    const pid = this.allocPid(KIND.SIGNAL);
    this.value[pid] = initialValue;
    this.pendingValue[pid] = initialValue;

    const read = () => this.readSignal<T>(pid);
    const write = (next: T) => this.writeSignal(pid, next);

    const fn = Object.assign(read, { write }) as (() => T) & {
      write: (value: T) => void;
    };

    return fn as any;
  }

  public computed<T>(getter: Getter<T>): () => T {
    const pid = this.allocPid(KIND.COMPUTED);
    this.getter[pid] = getter;

    return () => this.readComputed<T>(pid);
  }

  public effect(fn: () => Teardown): () => void {
    const pid = this.allocPid(KIND.EFFECT);
    this.getter[pid] = fn as Getter<any>;

    this.runEffect(pid);

    return () => this.disposeNode(pid);
  }

  public effectScope(fn: () => void): () => void {
    const pid = this.allocPid(KIND.SCOPE);

    this.activeStack.push(this.activeSub);
    this.activeSub = pid;

    try {
      fn();
    } finally {
      this.activeSub = this.activeStack.pop() ?? -1;
    }

    return () => this.disposeNode(pid);
  }

  public untrack<T>(fn: () => T): T {
    const prev = this.activeSub;
    this.activeSub = -1;

    try {
      return fn();
    } finally {
      this.activeSub = prev;
    }
  }

  public batch<T>(fn: () => T): T {
    this.batchDepth++;

    try {
      return fn();
    } finally {
      if (--this.batchDepth === 0) this.pass();
    }
  }

  // ==========================================================================
  // Reads
  // ==========================================================================

  private readSignal<T>(pid: number): T {
    if (this.activeSub !== -1) {
      this.link(pid, this.activeSub);
    }

    return this.value[pid] as T;
  }

  private readComputed<T>(pid: number): T {
    if (this.activeSub !== -1) {
      this.link(pid, this.activeSub);
    }

    if (this.isComputedDirty(pid)) {
      this.runComputed(pid);
    }

    return this.value[pid] as T;
  }

  private isComputedDirty(pid: number): boolean {
    if (this.flags[pid] & FLAG.DIRTY) return true;

    let link = this.depsHead[pid];

    while (link !== -1) {
      const dep = this.linkDep[link];

      if (this.kind[dep] === KIND.COMPUTED && this.isComputedDirty(dep)) {
        this.runComputed(dep);
      }

      if (this.version[dep] !== this.linkVersion[link]) {
        return true;
      }

      link = this.linkNextDep[link];
    }

    return false;
  }

  private runComputed(pid: number): void {
    const previous = this.value[pid];

    this.unlinkAllDeps(pid);
    this.flags[pid] = FLAG.RUNNING;

    const prevSub = this.activeSub;
    this.activeSub = pid;
    this.cycle++;

    try {
      this.value[pid] = this.getter[pid]!(previous);
    } finally {
      this.activeSub = prevSub;
      this.flags[pid] = FLAG.NONE;
      this.version[pid] = this.cycle;
      this.purgeStaleDeps(pid);
    }
  }

  private purgeStaleDeps(pid: number): void {
    const tail = this.depsTail[pid];
    let link = tail === -1 ? this.depsHead[pid] : this.linkNextDep[tail];

    while (link !== -1) {
      const next = this.linkNextDep[link];
      this.unlink(link);
      link = next;
    }
  }

  // ==========================================================================
  // Writes and lane scheduling
  // ==========================================================================

  private writeSignal(pid: number, next: unknown): void {
    if (Object.is(this.pendingValue[pid], next)) return;

    this.pendingValue[pid] = next;
    this.ops[pid] = OP.WRITE;

    this.enqueue(pid);

    if (this.batchDepth === 0) {
      this.pass();
    }
  }

  private enqueue(pid: number): void {
    if (this.laneLen >= this.capacity) {
      throw new Error("DISRUPTOR: lane full");
    }

    this.lane[this.laneLen++] = pid;
  }

  public pass(): void {
    const len = this.laneLen;
    let writeIndex = 0;

    for (let i = 0; i < len; i++) {
      const pid = this.lane[i];
      const op = this.ops[pid];

      if (pid === TRACER_PID) {
        this.val[TRACER_PID]++;
        this.lane[writeIndex++] = pid;
        continue;
      }

      if (op === OP.WRITE) {
        this.commitWrite(pid);
        this.lane[writeIndex++] = pid;
        this.ops[pid] = OP.NONE;
      } else if (op === OP.DELETE) {
        this.reclaimPid(pid);
      } else if (op === OP.EFFECT) {
        this.runEffect(pid);
        this.lane[writeIndex++] = pid;
        this.ops[pid] = OP.NONE;
      } else if (op === OP.DISPOSE_SCOPE) {
        this.reclaimPid(pid);
      }
    }

    this.laneLen = writeIndex;
    this.flushEffects();
  }

  private commitWrite(pid: number): void {
    const previous = this.value[pid];
    const next = this.pendingValue[pid];

    if (Object.is(previous, next)) return;

    this.value[pid] = next;
    this.cycle++;
    this.version[pid] = this.cycle;

    this.propagateFrom(pid);
  }

  private propagateFrom(dep: number): void {
    let link = this.subsHead[dep];

    while (link !== -1) {
      const sub = this.linkSub[link];
      const subKind = this.kind[sub];

      if (subKind === KIND.EFFECT) {
        this.scheduleEffect(sub);
      } else if (subKind === KIND.COMPUTED) {
        this.flags[sub] |= FLAG.DIRTY;
        this.propagateFrom(sub);
      }

      link = this.linkNextSub[link];
    }
  }

  private scheduleEffect(pid: number): void {
    if (this.flags[pid] & FLAG.PENDING) return;

    this.flags[pid] |= FLAG.PENDING;
    this.ops[pid] = OP.EFFECT;

    if (this.notifyTail < this.notifyQueue.length) {
      this.notifyQueue[this.notifyTail++] = pid;
    }
  }

  private flushEffects(): void {
    while (this.notifyHead < this.notifyTail) {
      const pid = this.notifyQueue[this.notifyHead++];
      this.flags[pid] &= ~FLAG.PENDING;
      this.runEffect(pid);
    }

    this.notifyHead = 0;
    this.notifyTail = 0;
  }

  // ==========================================================================
  // Effects and disposal
  // ==========================================================================

  private runEffect(pid: number): void {
    if (this.kind[pid] !== KIND.EFFECT) return;

    const previousCleanup = this.cleanup[pid];

    if (previousCleanup) {
      this.cleanup[pid] = undefined;
      this.activeStack.push(this.activeSub);
      this.activeSub = -1;

      try {
        previousCleanup();
      } finally {
        this.activeSub = this.activeStack.pop() ?? -1;
      }
    }

    this.unlinkAllDeps(pid);

    const previousSub = this.activeSub;
    this.activeSub = pid;
    this.flags[pid] = FLAG.RUNNING | FLAG.WATCHING;
    this.cycle++;
    this.runDepth++;

    try {
      this.cleanup[pid] = this.getter[pid]!();
    } finally {
      this.runDepth--;
      this.activeSub = previousSub;
      this.flags[pid] &= ~FLAG.RUNNING;
      this.purgeStaleDeps(pid);
    }
  }

  private disposeNode(pid: number): void {
    const kind = this.kind[pid];

    if (kind === KIND.EFFECT || kind === KIND.SCOPE) {
      const cleanup = this.cleanup[pid];

      if (cleanup) {
        this.cleanup[pid] = undefined;

        const previousSub = this.activeSub;
        this.activeSub = -1;

        try {
          cleanup();
        } finally {
          this.activeSub = previousSub;
        }
      }
    }

    this.unlinkAllDeps(pid);
    this.unlinkAllSubs(pid);

    // Tell parent effect/scope that this child is gone.
    let parentLink = this.subsHead[pid];

    if (parentLink !== -1) {
      const parent = this.linkSub[parentLink];
      this.flags[parent] &= ~FLAG.HAS_CHILD_EFFECT;
    }
  }

  // ==========================================================================
  // Existing disruptor surface
  // ==========================================================================

  public check_size_and_edit(x: number, y: number, pid: number, value: number): void {
    this.posX[pid] = x;
    this.posY[pid] = y;
    this.val[pid] = value;
    this.ops[pid] = OP.EDIT as unknown as number;

    if (this.laneLen < this.capacity) {
      this.lane[this.laneLen++] = pid;
    }
  }

  public getX(pid: number): number {
    return this.posX[pid];
  }

  public getY(pid: number): number {
    return this.posY[pid];
  }

  public getPosition(pid: number): { x: number; y: number } {
    return { x: this.posX[pid], y: this.posY[pid] };
  }

  public get tick(): number {
    return this.val[TRACER_PID];
  }

  public get length(): number {
    return this.laneLen;
  }

  public getLoadFactor(): number {
    return Math.max(0, this.laneLen - 1) / (this.capacity - 1);
  }
}
