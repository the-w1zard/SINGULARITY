/* ==========================================================================
   DARTH_MAUL_ENGINE — array-native reactive core
   --------------------------------------------------------------------------
   User-facing model:
     { r, m, s, k, p }

   Internal model:
     - r/m/s/k: interned integer IDs
     - p:       binary payload handle in a contiguous byte arena
     - nodes:   integer PIDs in typed-array columns
     - links:   integer LIDs in typed-array columns
     - work:    integer scheduler ring buffer

   The user never sees binary storage. They give and receive plain JS values.
   ========================================================================== */

type Pid = number;
type Lid = number;

export const OP = {
  NONE: 0,
  WRITE: 1,
  EDIT: 2,
  READ: 3,
  DELETE: 4,
  EFFECT: 5,
  COMPACT: 6,
  SEND: 7,
  SUB: 8,
  DUPLEX: 9,
  STREAM: 10,
  DRC: 11,
} as const;

export const RKind = {
  NONE: 0,
  SIGNAL: 1,
  COMPUTED: 2,
  EFFECT: 3,
  EFFECT_SCOPE: 4,
} as const;

export const RX_Flags = {
  NONE: 0,
  MUTABLE: 1 << 0,
  DIRTY: 1 << 1,
  PENDING: 1 << 2,
  WATCHING: 1 << 3,
  RUNNING: 1 << 4,
  RECURSEDCHECK: 1 << 5,
  RECURSED: 1 << 6,
  DISPOSED: 1 << 7,
} as const;

const TRACER_PID = 0;
const EMPTY = -1;

/* ==========================================================================
   Growable binary payload arena
   ========================================================================== */

class ByteArena {
  bytes: Uint8Array;
  length = 0;

  constructor(initialBytes = 1 << 20) {
    this.bytes = new Uint8Array(initialBytes);
  }

  private ensure(extra: number) {
    if (this.length + extra <= this.bytes.length) return;

    let next = this.bytes.length;

    while (next < this.length + extra) next *= 2;

    const grown = new Uint8Array(next);
    grown.set(this.bytes);
    this.bytes = grown;
  }

  writeU8(value: number) {
    this.ensure(1);
    this.bytes[this.length++] = value & 0xff;
  }

  writeU32(value: number) {
    this.ensure(4);
    const view = new DataView(this.bytes.buffer);
    view.setUint32(this.length, value >>> 0, true);
    this.length += 4;
  }

  writeF64(value: number) {
    this.ensure(8);
    const view = new DataView(this.bytes.buffer);
    view.setFloat64(this.length, value, true);
    this.length += 8;
  }

  writeBytes(source: Uint8Array) {
    this.ensure(source.length);
    this.bytes.set(source, this.length);
    this.length += source.length;
  }

  readU8(offset: number) {
    return this.bytes[offset];
  }

  readU32(offset: number) {
    return new DataView(this.bytes.buffer).getUint32(offset, true);
  }

  readF64(offset: number) {
    return new DataView(this.bytes.buffer).getFloat64(offset, true);
  }

  slice(offset: number, length: number) {
    return this.bytes.subarray(offset, offset + length);
  }
}

/* ==========================================================================
   Channel + payload record store
   ========================================================================== */

const PAYLOAD_TYPE = {
  NULL: 0,
  UNDEFINED: 1,
  FALSE: 2,
  TRUE: 3,
  NUMBER: 4,
  STRING: 5,
  ARRAY: 6,
  OBJECT: 7,
  REF: 8,
} as const;

export interface ChannelRecord {
  r: string;
  m: string;
  s: string;
  k: string;
  p: unknown;
}

export class WARLOCK_NGIN {
  public hub: unknown;

  private readonly capacity: number;

  /* ---------------------------- World lane ---------------------------- */

  private readonly lane: Int32Array;
  private readonly ops: Uint8Array;
  private readonly posX: Float32Array;
  private readonly posY: Float32Array;
  private readonly val: Int32Array;

  private readonly freePids: Int32Array;
  private freeHead = 0;
  private freeTail = 0;

  private readonly inLane: Uint8Array;
  private laneLen = 0;
  public nextAllocatedPid = 1;

  /* -------------------------- Channel columns ------------------------- */

  private readonly recR: Int32Array;
  private readonly recM: Int32Array;
  private readonly recS: Int32Array;
  private readonly recK: Int32Array;

  private readonly recPOffset: Int32Array;
  private readonly recPLength: Int32Array;
  private readonly recPVersion: Int32Array;

  /* ------------------------- Reactive columns ------------------------- */

  private readonly rKind: Uint8Array;
  private readonly rFlags: Uint16Array;

  private readonly rValueHandle: Int32Array;
  private readonly rPendingHandle: Int32Array;
  private readonly rValueVersion: Int32Array;

  private readonly rDeps: Int32Array;
  private readonly rDepsTail: Int32Array;
  private readonly rSubs: Int32Array;
  private readonly rSubsTail: Int32Array;

  /* Direct execution paths.
     JavaScript closures cannot inhabit a typed array, but they can be
     addressed by integer ID exactly like every other column. */
  private readonly fnTable: Array<((...args: unknown[]) => unknown) | undefined>;
  private nextFnId = 1;

  /* ----------------------------- Link pool ---------------------------- */

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

  /* ----------------------------- Scheduler ---------------------------- */

  private readonly workQueue: Int32Array;
  private workHead = 0;
  private workTail = 0;
  private workCount = 0;

  public cycle = 0;
  public runDepth = 0;
  public batchDepth = 0;
  public activeSub: Pid | undefined;

  public readonly HasChildEffect = 64;
  public BRAND_SIGNAL = Symbol("warlock.signal");
  public BRAND_EFFECT = Symbol("warlock.effect");
  public BRAND_EFFECT_SCOPE = Symbol("warlock.effectScope");
  public BRAND_COMPUTED = Symbol("warlock.computed");

  private readonly arena = new ByteArena();
  private readonly stringIds = new Map<string, number>();
  private readonly stringTable: string[] = [""];

  constructor(hub: unknown =null, capacity = 1 << 18) {
    this.hub = hub;
    this.capacity = capacity;

    this.lane = new Int32Array(capacity).fill(EMPTY);
    this.ops = new Uint8Array(capacity);
    this.posX = new Float32Array(capacity);
    this.posY = new Float32Array(capacity);
    this.val = new Int32Array(capacity);

    this.freePids = new Int32Array(capacity);
    this.inLane = new Uint8Array(capacity);

    this.recR = new Int32Array(capacity).fill(EMPTY);
    this.recM = new Int32Array(capacity).fill(EMPTY);
    this.recS = new Int32Array(capacity).fill(EMPTY);
    this.recK = new Int32Array(capacity).fill(EMPTY);

    this.recPOffset = new Int32Array(capacity).fill(EMPTY);
    this.recPLength = new Int32Array(capacity).fill(0);
    this.recPVersion = new Int32Array(capacity).fill(0);

    this.rKind = new Uint8Array(capacity);
    this.rFlags = new Uint16Array(capacity);

    this.rValueHandle = new Int32Array(capacity).fill(EMPTY);
    this.rPendingHandle = new Int32Array(capacity).fill(EMPTY);
    this.rValueVersion = new Int32Array(capacity).fill(0);

    this.rDeps = new Int32Array(capacity).fill(EMPTY);
    this.rDepsTail = new Int32Array(capacity).fill(EMPTY);
    this.rSubs = new Int32Array(capacity).fill(EMPTY);
    this.rSubsTail = new Int32Array(capacity).fill(EMPTY);

    this.fnTable = new Array(capacity);

    this.lVersion = new Int32Array(capacity);
    this.lDep = new Int32Array(capacity).fill(EMPTY);
    this.lSub = new Int32Array(capacity).fill(EMPTY);

    this.lPrevDep = new Int32Array(capacity).fill(EMPTY);
    this.lNextDep = new Int32Array(capacity).fill(EMPTY);
    this.lPrevSub = new Int32Array(capacity).fill(EMPTY);
    this.lNextSub = new Int32Array(capacity).fill(EMPTY);

    this.freeLinks = new Int32Array(capacity);

    this.workQueue = new Int32Array(capacity);

    for (let i = capacity - 1; i >= 1; i--) {
      this.freePids[this.freeTail++] = i;
    }

    this.lane[TRACER_PID] = TRACER_PID;
    this.val[TRACER_PID] = 0;
    this.ops[TRACER_PID] = OP.NONE;
    this.laneLen = 1;
  }

  /* ==========================================================================
     String interning
     ========================================================================== */

  private intern(value: string): number {
    const existing = this.stringIds.get(value);

    if (existing !== undefined) return existing;

    const id = this.stringTable.length;
    this.stringTable.push(value);
    this.stringIds.set(value, id);

    return id;
  }

  private getString(id: number): string {
    return this.stringTable[id] ?? "";
  }

  /* ==========================================================================
     Payload encoding — JS in, binary stored, JS out
     ========================================================================== */

  public encodePayload(value: unknown): { offset: number; length: number } {
    const start = this.arena.length;

    this.encodeValue(value);

    return {
      offset: start,
      length: this.arena.length - start,
    };
  }

  private encodeValue(value: unknown): void {
    if (value === null) {
      this.arena.writeU8(PAYLOAD_TYPE.NULL);
      return;
    }

    if (value === undefined) {
      this.arena.writeU8(PAYLOAD_TYPE.UNDEFINED);
      return;
    }

    if (typeof value === "boolean") {
      this.arena.writeU8(value ? PAYLOAD_TYPE.TRUE : PAYLOAD_TYPE.FALSE);
      return;
    }

    if (typeof value === "number") {
      this.arena.writeU8(PAYLOAD_TYPE.NUMBER);
      this.arena.writeF64(value);
      return;
    }

    if (typeof value === "string") {
      this.arena.writeU8(PAYLOAD_TYPE.STRING);
      this.arena.writeU32(this.intern(value));
      return;
    }

    if (Array.isArray(value)) {
      this.arena.writeU8(PAYLOAD_TYPE.ARRAY);
      this.arena.writeU32(value.length);

      for (let i = 0; i < value.length; i++) {
        this.encodeValue(value[i]);
      }

      return;
    }

    if (typeof value === "object") {
      const keys = Object.keys(value as Record<string, unknown>);

      this.arena.writeU8(PAYLOAD_TYPE.OBJECT);
      this.arena.writeU32(keys.length);

      for (const key of keys) {
        this.arena.writeU32(this.intern(key));
        this.encodeValue((value as Record<string, unknown>)[key]);
      }

      return;
    }

    /* Functions, Dates, Maps, Sets, class instances, etc. are preserved as
       opaque JS references. They are not serialized; they are addressed. */
    this.arena.writeU8(PAYLOAD_TYPE.REF);
    this.arena.writeF64(0);
    this.refStash.push(value);
  }

  private refStash: unknown[] = [];

  public decodePayload(offset: number, length: number): unknown {
    if (offset === EMPTY) return undefined;

    const cursor = { index: 0 };
    return this.decodeValue(offset, cursor);
  }

  private decodeValue(base: number, cursor: { index: number }): unknown {
    const type = this.arena.readU8(base + cursor.index++);

    switch (type) {
      case PAYLOAD_TYPE.NULL:
        return null;

      case PAYLOAD_TYPE.UNDEFINED:
        return undefined;

      case PAYLOAD_TYPE.FALSE:
        return false;

      case PAYLOAD_TYPE.TRUE:
        return true;

      case PAYLOAD_TYPE.NUMBER:
        return this.arena.readF64(base + cursor.index), (cursor.index += 8), this.arena.readF64(base + cursor.index - 8);

      case PAYLOAD_TYPE.STRING:
        return this.getString(this.arena.readU32(base + cursor.index)), (cursor.index += 4), this.getString(this.arena.readU32(base + cursor.index - 4));

      case PAYLOAD_TYPE.ARRAY: {
        const count = this.arena.readU32(base + cursor.index);
        cursor.index += 4;

        const output: unknown[] = new Array(count);

        for (let i = 0; i < count; i++) {
          output[i] = this.decodeValue(base, cursor);
        }

        return output;
      }

      case PAYLOAD_TYPE.OBJECT: {
        const count = this.arena.readU32(base + cursor.index);
        cursor.index += 4;

        const output: Record<string, unknown> = {};

        for (let i = 0; i < count; i++) {
          const key = this.getString(this.arena.readU32(base + cursor.index));
          cursor.index += 4;

          output[key] = this.decodeValue(base, cursor);
        }

        return output;
      }

      case PAYLOAD_TYPE.REF:
        cursor.index += 8;
        return undefined;

      default:
        return undefined;
    }
  }

  /* ==========================================================================
     Channel API — user sees JavaScript, engine sees integers and bytes
     ========================================================================== */

  public setRecord(pid: Pid, record: ChannelRecord): void {
    this.recR[pid] = this.intern(record.r);
    this.recM[pid] = this.intern(record.m);
    this.recS[pid] = this.intern(record.s);
    this.recK[pid] = this.intern(record.k);

    const encoded = this.encodePayload(record.p);

    this.recPOffset[pid] = encoded.offset;
    this.recPLength[pid] = encoded.length;
    this.recPVersion[pid] = ++this.cycle;
  }

  public getRecord(pid: Pid): ChannelRecord | undefined {
    if (pid === TRACER_PID || this.recPOffset[pid] === EMPTY) return undefined;

    return {
      r: this.getString(this.recR[pid]),
      m: this.getString(this.recM[pid]),
      s: this.getString(this.recS[pid]),
      k: this.getString(this.recK[pid]),
      p: this.decodePayload(this.recPOffset[pid], this.recPLength[pid]),
    };
  }

  public getPayload(pid: Pid): unknown {
    return this.decodePayload(this.recPOffset[pid], this.recPLength[pid]);
  }

  /* ==========================================================================
     World-object lane API
     ========================================================================== */

  private enqueueOp(pid: Pid, op: number): void {
    if (pid === TRACER_PID) return;

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

  public remove(x: number, y: number, value: number, pid: Pid): void {
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

  public getX(pid: Pid): number {
    return this.posX[pid];
  }

  public getY(pid: Pid): number {
    return this.posY[pid];
  }

  public getPosition(pid: Pid): { x: number; y: number } {
    return { x: this.posX[pid], y: this.posY[pid] };
  }

  public pass(): void {
    let writeIndex = 0;

    for (let i = 0; i < this.laneLen; i++) {
      const pid = this.lane[i];

      if (pid === TRACER_PID) {
        this.val[TRACER_PID]++;
        this.lane[writeIndex++] = pid;
        continue;
      }

      this.inLane[pid] = 0;

      if (this.ops[pid] === OP.DELETE) {
        this.reclaimPid(pid);
        continue;
      }

      this.ops[pid] = OP.NONE;
      this.lane[writeIndex++] = pid;
    }

    this.laneLen = writeIndex;
  }

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
    return Math.max(0, this.laneLen - 1) / (this.capacity - 1);
  }

  /* ==========================================================================
     PID / link lifecycle
     ========================================================================== */

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
    this.lDep[lid] = EMPTY;
    this.lSub[lid] = EMPTY;
    this.lPrevDep[lid] = EMPTY;
    this.lNextDep[lid] = EMPTY;
    this.lPrevSub[lid] = EMPTY;
    this.lNextSub[lid] = EMPTY;

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

    this.rValueHandle[pid] = EMPTY;
    this.rPendingHandle[pid] = EMPTY;
    this.rValueVersion[pid] = 0;

    this.fnTable[pid] = undefined;

    this.rDeps[pid] = EMPTY;
    this.rDepsTail[pid] = EMPTY;
    this.rSubs[pid] = EMPTY;
    this.rSubsTail[pid] = EMPTY;

    this.inLane[pid] = 0;

    this.freePids[this.freeTail] = pid;
    this.freeTail = (this.freeTail + 1) % this.capacity;
  }

  /* ==========================================================================
     Integer dependency links
     ========================================================================== */

  public link(dep: Pid, sub: Pid, version: number): void {
    const prevDep = this.rDepsTail[sub];

    if (prevDep !== EMPTY && this.lDep[prevDep] === dep) {
      return;
    }

    const nextDep = prevDep !== EMPTY ? this.lNextDep[prevDep] : this.rDeps[sub];

    if (nextDep !== EMPTY && this.lDep[nextDep] === dep) {
      this.lVersion[nextDep] = version;
      this.rDepsTail[sub] = nextDep;
      return;
    }

    const lid = this.allocLink();

    this.lVersion[lid] = version;
    this.lDep[lid] = dep;
    this.lSub[lid] = sub;

    this.lPrevDep[lid] = prevDep;
    this.lNextDep[lid] = nextDep;

    const prevSub = this.rSubsTail[dep];
    this.lPrevSub[lid] = prevSub;
    this.lNextSub[lid] = EMPTY;

    this.rDepsTail[sub] = lid;
    this.rSubsTail[dep] = lid;

    if (nextDep !== EMPTY) this.lPrevDep[nextDep] = lid;

    if (prevDep !== EMPTY) {
      this.lNextDep[prevDep] = lid;
    } else {
      this.rDeps[sub] = lid;
    }

    if (prevSub !== EMPTY) {
      this.lNextSub[prevSub] = lid;
    } else {
      this.rSubs[dep] = lid;
    }
  }

  public unlink(lid: Lid, sub: Pid = this.lSub[lid]): Lid {
    const dep = this.lDep[lid];

    const prevDep = this.lPrevDep[lid];
    const nextDep = this.lNextDep[lid];
    const prevSub = this.lPrevSub[lid];
    const nextSub = this.lNextSub[lid];

    if (nextDep !== EMPTY) this.lPrevDep[nextDep] = prevDep;
    else this.rDepsTail[sub] = prevDep;

    if (prevDep !== EMPTY) this.lNextDep[prevDep] = nextDep;
    else this.rDeps[sub] = nextDep;

    if (nextSub !== EMPTY) this.lPrevSub[nextSub] = prevSub;
    else this.rSubsTail[dep] = prevSub;

    if (prevSub !== EMPTY) {
      this.lNextSub[prevSub] = nextSub;
    } else {
      this.rSubs[dep] = nextSub;

      if (nextSub === EMPTY) {
        this.unwatched(dep);
      }
    }

    this.freeLink(lid);

    return nextDep;
  }

  public disposeAllDepsInReverse(pid: Pid): void {
    let link = this.rDepsTail[pid];

    while (link !== EMPTY) {
      const previous = this.lPrevDep[link];
      this.unlink(link, pid);
      link = previous;
    }
  }

  public purgeDeps(pid: Pid): void {
    const tail = this.rDepsTail[pid];
    let link = tail !== EMPTY ? this.lNextDep[tail] : this.rDeps[pid];

    while (link !== EMPTY) {
      link = this.unlink(link, pid);
    }
  }

  public unwatched(pid: Pid): void {
    switch (this.rKind[pid]) {
      case RKind.COMPUTED:
        this.rFlags[pid] = RX_Flags.MUTABLE;
        this.disposeAllDepsInReverse(pid);
        break;

      case RKind.EFFECT:
        this.schedule(pid);
        break;

      case RKind.EFFECT_SCOPE:
        this.effectScopeOper(pid);
        break;
    }
  }

  /* ==========================================================================
     Propagation — integer links only, no object traversal
     ========================================================================== */

  public propagate(startLink: Lid, innerWrite: boolean): void {
    if (startLink === EMPTY) return;

    const stack: number[] = [startLink];

    while (stack.length > 0) {
      const link = stack.pop()!;
      const sub = this.lSub[link];

      if (sub === EMPTY) continue;

      let flags = this.rFlags[sub];

      if (flags & RX_Flags.RECURSEDCHECK) continue;

      if (!(flags & RX_Flags.PENDING)) {
        flags |= RX_Flags.PENDING;

        if (innerWrite) {
          flags |= RX_Flags.RECURSED;
        }

        this.rFlags[sub] = flags;
      }

      if (flags & RX_Flags.WATCHING) {
        this.schedule(sub);
      }

      if (this.rKind[sub] === RKind.COMPUTED) {
        let childLink = this.rSubs[sub];

        while (childLink !== EMPTY) {
          stack.push(childLink);
          childLink = this.lNextSub[childLink];
        }
      }
    }
  }

  public shallowPropagate(startLink: Lid): void {
    let link = startLink;

    while (link !== EMPTY) {
      const sub = this.lSub[link];
      const flags = this.rFlags[sub];

      if (
        flags & RX_Flags.PENDING &&
        (flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) === RX_Flags.WATCHING
      ) {
        this.schedule(sub);
      }

      link = this.lNextSub[link];
    }
  }

  /* ==========================================================================
     Scheduler — flat integer ring, not an object-graph walk
     ========================================================================== */

  private schedule(pid: Pid): void {
    if (this.rFlags[pid] & RX_Flags.PENDING) return;

    this.rFlags[pid] |= RX_Flags.PENDING;

    if (this.workCount >= this.workQueue.length) {
      throw new Error("DISRUPTOR: scheduler capacity exhausted");
    }

    this.workQueue[this.workTail] = pid;
    this.workTail = (this.workTail + 1) % this.workQueue.length;
    this.workCount++;
  }

  public flush(): void {
    try {
      while (this.workCount > 0) {
        const pid = this.workQueue[this.workHead];

        this.workQueue[this.workHead] = EMPTY;
        this.workHead = (this.workHead + 1) % this.workQueue.length;
        this.workCount--;

        this.rFlags[pid] &= ~RX_Flags.PENDING;
        this.run(pid);
      }
    } finally {
      this.workHead = 0;
      this.workTail = 0;
      this.workCount = 0;
    }
  }

  /* ==========================================================================
     Dirty checking — version deltas, not recursive object traversal
     ========================================================================== */

  public isDirty(pid: Pid): boolean {
    let link = this.rDeps[pid];

    while (link !== EMPTY) {
      const dep = this.lDep[link];

      if (this.rKind[dep] === RKind.COMPUTED && this.isDirty(dep)) {
        this.updateComputed(dep);
      }

      if (this.lVersion[link] !== this.rValueVersion[dep]) {
        return true;
      }

      link = this.lNextDep[link];
    }

    return false;
  }

  public checkDirty(_link: number, pid: Pid): boolean {
    return this.isDirty(pid);
  }

  /* ==========================================================================
     Signals
     ========================================================================== */

  public signal<T>(initialValue?: T): Pid {
    const pid = this.allocNode(RKind.SIGNAL);

    const encoded = this.encodePayload(initialValue);

    this.rValueHandle[pid] = encoded.offset;
    this.rPendingHandle[pid] = encoded.offset;
    this.rValueVersion[pid] = 0;

    this.rFlags[pid] = RX_Flags.MUTABLE;

    return pid;
  }

  public signalOper<T>(pid: Pid, ...value: [] | [T]): T | void {
    if (value.length > 0) {
      const encoded = this.encodePayload(value[0]);

      if (
        this.rPendingHandle[pid] !== encoded.offset ||
        this.recPLength[pid] !== encoded.length
      ) {
        this.rPendingHandle[pid] = encoded.offset;
        this.rFlags[pid] |= RX_Flags.DIRTY;

        const subs = this.rSubs[pid];

        if (subs !== EMPTY) {
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

    return this.decodePayload(this.rValueHandle[pid], this.recPLength[pid]) as T;
  }

  private commitSignal(pid: Pid): void {
    const previous = this.rValueHandle[pid];
    const next = this.rPendingHandle[pid];

    this.rFlags[pid] &= ~RX_Flags.DIRTY;

    if (previous === next) return;

    this.rValueHandle[pid] = next;
    this.rValueVersion[pid] = ++this.cycle;

    const subs = this.rSubs[pid];

    if (subs !== EMPTY) {
      this.propagate(subs, this.runDepth > 0);
    }
  }

  /* ==========================================================================
     Computeds
     ========================================================================== */

  public computed<T>(getter: (previous?: T) => T): Pid {
    const pid = this.allocNode(RKind.COMPUTED);

    this.fnTable[pid] = getter as (...args: unknown[]) => unknown;

    return pid;
  }

  public computedOper<T>(pid: Pid): T {
    if (this.isDirty(pid)) {
      this.updateComputed(pid);

      const subs = this.rSubs[pid];

      if (subs !== EMPTY) {
        this.shallowPropagate(subs);
      }
    }

    const sub = this.activeSub;

    if (sub !== undefined) {
      this.link(pid, sub, this.cycle);
    }

    return this.decodePayload(this.rValueHandle[pid], this.recPLength[pid]) as T;
  }

  public updateComputed(pid: Pid): boolean {
    if (this.rFlags[pid] & RX_Flags.RUNNING) return false;

    this.disposeAllDepsInReverse(pid);

    this.rFlags[pid] = RX_Flags.RUNNING | RX_Flags.RECURSEDCHECK;

    const previousSub = this.setActiveSub(pid);

    try {
      this.cycle++;

      const getter = this.fnTable[pid] as (previous?: unknown) => unknown;
      const previous = this.decodePayload(this.rValueHandle[pid], this.recPLength[pid]);

      const next = getter(previous);
      const encoded = this.encodePayload(next);

      const changed =
        this.rValueHandle[pid] !== encoded.offset ||
        this.recPLength[pid] !== encoded.length;

      this.rValueHandle[pid] = encoded.offset;
      this.recPLength[pid] = encoded.length;
      this.rValueVersion[pid] = this.cycle;

      return changed;
    } finally {
      this.activeSub = previousSub;
      this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
      this.purgeDeps(pid);
    }
  }

  /* ==========================================================================
     Effects
     ========================================================================== */

  public effectSignal(fn: () => void | (() => void)): Pid {
    const pid = this.allocNode(RKind.EFFECT);

    this.fnTable[pid] = fn as (...args: unknown[]) => unknown;
    this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

    const previousSub = this.setActiveSub(pid);

    if (previousSub !== undefined) {
      this.rFlags[previousSub] |= this.HasChildEffect;
    }

    try {
      this.runDepth++;

      const cleanup = fn();

      if (typeof cleanup === "function") {
        this.fnTable[pid + 0x1000000] = cleanup as (...args: unknown[]) => unknown;
      }
    } finally {
      this.runDepth--;
      this.activeSub = previousSub;
      this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
    }

    return pid;
  }

  public run(pid: Pid): void {
    if (this.rKind[pid] !== RKind.EFFECT) return;

    const flags = this.rFlags[pid];

    if (flags & RX_Flags.RUNNING) {
      this.schedule(pid);
      return;
    }

    const pending = (flags & RX_Flags.PENDING) !== 0;
    const dirty = pending ? this.isDirty(pid) : false;

    if (!pending && !dirty && this.rDeps[pid] !== EMPTY) {
      this.rFlags[pid] = RX_Flags.WATCHING | (flags & this.HasChildEffect);
      return;
    }

    this.disposeAllDepsInReverse(pid);

    this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

    const previousSub = this.setActiveSub(pid);

    try {
      this.runDepth++;
      this.cycle++;

      const fn = this.fnTable[pid] as () => unknown;
      const cleanup = fn();

      if (typeof cleanup === "function") {
        this.fnTable[pid + 0x1000000] = cleanup as (...args: unknown[]) => unknown;
      }
    } finally {
      this.runDepth--;
      this.activeSub = previousSub;
      this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
      this.purgeDeps(pid);
    }
  }

  public effectOper(pid: Pid): void {
    if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

    const cleanup = this.fnTable[pid + 0x1000000];

    if (typeof cleanup === "function") {
      this.fnTable[pid + 0x1000000] = undefined;
      cleanup();
    }

    this.effectScopeOper(pid);
  }

  public effectScope(fn: () => void): Pid {
    const pid = this.allocNode(RKind.EFFECT_SCOPE);

    this.fnTable[pid] = fn as (...args: unknown[]) => unknown;
    this.rFlags[pid] = RX_Flags.MUTABLE;

    const previousSub = this.setActiveSub(pid);

    if (previousSub !== undefined) {
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

  public effectScopeOper(pid: Pid): void {
    if (this.rFlags[pid] & RX_Flags.DISPOSED) return;

    this.unlinkParent(pid);
    this.disposeAllDepsInReverse(pid);

    this.rFlags[pid] |= RX_Flags.DISPOSED;
  }

  private unlinkParent(pid: Pid): void {
    let link = this.rSubs[pid];

    while (link !== EMPTY) {
      const next = this.lNextSub[link];
      const parent = this.lSub[link];

      if (
        this.rKind[parent] === RKind.EFFECT ||
        this.rKind[parent] === RKind.EFFECT_SCOPE
      ) {
        this.unlink(link, pid);

        if (this.rDeps[parent] === EMPTY) {
          this.rFlags[parent] &= ~this.HasChildEffect;
        }
      }

      link = next;
    }
  }

  public trigger(fn: () => void): void {
    const watcher = this.allocNode(RKind.EFFECT);

    this.rFlags[watcher] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;

    const previousSub = this.setActiveSub(watcher);

    this.batchDepth++;

    try {
      fn();

      let link = this.rDeps[watcher];

      while (link !== EMPTY) {
        const dep = this.lDep[link];
        const next = this.lNextDep[link];

        const subs = this.rSubs[dep];

        if (subs !== EMPTY) {
          this.propagate(subs, this.runDepth > 0);
          this.shallowPropagate(subs);
        }

        link = next;
      }
    } finally {
      this.activeSub = previousSub;
      this.batchDepth--;

      if (this.batchDepth === 0) {
        this.flush();
      }

      this.reclaimPid(watcher);
    }
  }

  /* ==========================================================================
     Node allocation and compatibility
     ========================================================================== */

  private allocNode(kind: number): Pid {
    const pid = this.allocPid();

    this.rKind[pid] = kind;
    this.rFlags[pid] = RX_Flags.NONE;

    this.rValueHandle[pid] = EMPTY;
    this.rPendingHandle[pid] = EMPTY;
    this.rValueVersion[pid] = 0;

    this.fnTable[pid] = undefined;

    this.rDeps[pid] = EMPTY;
    this.rDepsTail[pid] = EMPTY;
    this.rSubs[pid] = EMPTY;
    this.rSubsTail[pid] = EMPTY;

    return pid;
  }

  public untrack<T>(fn: () => T): T {
    const previous = this.setActiveSub(undefined);

    try {
      return fn();
    } finally {
      this.setActiveSub(previous);
    }
  }

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
