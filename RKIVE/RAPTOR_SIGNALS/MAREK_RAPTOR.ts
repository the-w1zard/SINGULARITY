/* eslint-disable @typescript-eslint/no-explicit-any */

export const TRACER_PID = 0;

export const OP = {
  READ: 0,
  WRITE: 1,
  DELETE: 2,
  EFFECT: 3,
  COMPACT: 4,
  NONE: 5,
  EDIT: 6,
  PUT: 7,
  SEND: 11,
  SUB: 12,
  DUPLEX: 13,
  STREAM: 14,
  DRC: 15,
  TRANSFORM: 16,
} as const;

const IN_LANE = 0x80;
const OP_MASK = 0x7f;

const STATE = {
  FREE: 0,
  CHANNEL: 1,
  SUBSCRIBER: 2,
  STREAM_SUBSCRIBER: 3,
  DUPLEX: 4,
  EFFECT: 5,
} as const;

const PAYLOAD = {
  NULL: 0,
  NUMBER: 1,
  STRING: 2,
  BOOLEAN_FALSE: 3,
  BOOLEAN_TRUE: 4,
  JSON: 5,
} as const;

export type ChannelAddress = {
  r: string;
  m?: string;
  s?: string;
  k?: string;
};

export type SubContext = {
  reply(value: any): void;
  emit(value: any): void;
  channel: ChannelAddress;
};

export type SubHandler = (payload: any, ctx: SubContext) => any | void | Promise<any>;
export type StreamHandler = (payload: any, ctx: SubContext) => any | void | Promise<any>;
export type DuplexHandler = (payload: any, ctx: SubContext) => any | void | Promise<any>;

type PendingReply = {
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
};

type N = number;
type V = void;

export class MAREK_RAPTOR {
  public readonly capacity: N;

  private readonly state: Uint8Array;
  private readonly ops: Uint8Array;
  private readonly version: Int32Array;

  private readonly keyR: Int32Array;
  private readonly keyM: Int32Array;
  private readonly keyS: Int32Array;
  private readonly keyK: Int32Array;

  private readonly payloadOffset: Int32Array;
  private readonly payloadLength: Int32Array;

  private readonly subscriberHead: Int32Array;
  private readonly subscriberNext: Int32Array;
  private readonly subscriberTarget: Int32Array;

  private readonly dependencyOffset: Int32Array;
  private readonly dependencyCount: Int32Array;
  private readonly dependencyVersion: Int32Array;

  private readonly handlerId: Int32Array;

  private readonly lane: Int32Array;
  private readonly freePids: Int32Array;

  private  payloadHeap: Uint8Array;
  private payloadTail = 0;

  private laneLength = 0;
  private freeTail = 0;
  private nextPid = 1;
  private epoch = 1;

  private readonly interned = new Map<string, N>();
  private readonly channelSlots = new Map<string, N>();
  private readonly handlers: Array<SubHandler | StreamHandler | DuplexHandler> = [];
  private readonly pending = new Map<N, PendingReply>();
  private pendingQueue: N[] = [];
  private nextPendingId = 1;

  private trackingDependencies: N[] | null = null;

  constructor(
    capacity = 1 << 20,
    payloadBytes = 1 << 24,
  ) {
    this.capacity = capacity;

    this.state = new Uint8Array(capacity);
    this.ops = new Uint8Array(capacity);
    this.version = new Int32Array(capacity);

    this.keyR = new Int32Array(capacity).fill(-1);
    this.keyM = new Int32Array(capacity).fill(-1);
    this.keyS = new Int32Array(capacity).fill(-1);
    this.keyK = new Int32Array(capacity).fill(-1);

    this.payloadOffset = new Int32Array(capacity).fill(-1);
    this.payloadLength = new Int32Array(capacity);

    this.subscriberHead = new Int32Array(capacity).fill(-1);
    this.subscriberNext = new Int32Array(capacity).fill(-1);
    this.subscriberTarget = new Int32Array(capacity).fill(-1);

    this.dependencyOffset = new Int32Array(capacity).fill(-1);
    this.dependencyCount = new Int32Array(capacity);
    this.dependencyVersion = new Int32Array(capacity);

    this.handlerId = new Int32Array(capacity).fill(-1);

    this.lane = new Int32Array(capacity).fill(-1);
    this.freePids = new Int32Array(capacity);

    this.payloadHeap = new Uint8Array(payloadBytes);

    this.lane[0] = TRACER_PID;
    this.ops[TRACER_PID] = OP.NONE | IN_LANE;
    this.laneLength = 1;
  }

  /* ------------------------------------------------------------------ *
   * Public surface: ordinary JavaScript in, ordinary JavaScript out.
   * ------------------------------------------------------------------ */

  public send(
    channel: ChannelAddress,
    payload: any,
    options: { reply?: boolean } = {},
  ): Promise<any> | V {
    const slot = this.resolveChannel(channel);
    const encoded = this.encodePayload(payload);

    this.payloadOffset[slot] = encoded.offset;
    this.payloadLength[slot] = encoded.length;
    this.version[slot] = ++this.epoch;

    const wantsReply = options.reply === true;

    if (wantsReply) {
      const pendingId = this.nextPendingId++;
      const promise = new Promise<any>((resolve, reject) => {
        this.pending.set(pendingId, { resolve, reject });
      });

      this.pendingQueue.push(pendingId);
      this.admit(slot, OP.SEND);
      this.pass();

      return promise;
    }

    this.admit(slot, OP.SEND);
    this.pass();

    return undefined;
  }

  public sub(channel: ChannelAddress, handler: SubHandler): () => V {
    return this.addSubscriber(channel, handler, STATE.SUBSCRIBER, OP.SUB);
  }

  public stream(channel: ChannelAddress, handler: StreamHandler): () => V {
    return this.addSubscriber(channel, handler, STATE.STREAM_SUBSCRIBER, OP.STREAM);
  }

  public duplex(
    channel: ChannelAddress,
    handler: DuplexHandler,
  ): {
    send: (payload: any) => Promise<any>;
    emit: (payload: any) => V;
    close: () => V;
  } {
    const channelSlot = this.resolveChannel(channel);
    const handlerSlot = this.allocateHandlerSlot(channel, handler, STATE.DUPLEX, OP.DUPLEX);

    this.linkSubscriber(channelSlot, handlerSlot);

    return {
      send: (payload: any) => this.send(channel, payload, { reply: true }) as Promise<any>,
      emit: (payload: any) => {
        this.emitStream(channel, payload);
      },
      close: () => {
        this.unlinkSubscriber(channelSlot, handlerSlot);
        this.releaseSlot(handlerSlot);
      },
    };
  }

  public emitStream(channel: ChannelAddress, payload: any): V {
    const slot = this.resolveChannel(channel);
    const encoded = this.encodePayload(payload);

    this.payloadOffset[slot] = encoded.offset;
    this.payloadLength[slot] = encoded.length;
    this.version[slot] = ++this.epoch;

    this.admit(slot, OP.STREAM);
    this.pass();
  }

  public set(channel: ChannelAddress, payload: any): V {
    const slot = this.resolveChannel(channel);
    const encoded = this.encodePayload(payload);

    this.payloadOffset[slot] = encoded.offset;
    this.payloadLength[slot] = encoded.length;
    this.version[slot] = ++this.epoch;

    this.admit(slot, OP.WRITE);
    this.pass();
  }

  public get(channel: ChannelAddress): any {
    const slot = this.resolveChannel(channel);

    if (this.trackingDependencies) {
      this.trackingDependencies.push(slot);
    }

    return this.decodePayload(this.payloadOffset[slot], this.payloadLength[slot]);
  }

  public effect(fn: () => V | Promise<V>): () => V {
    const effectSlot = this.allocateSlot();
    this.state[effectSlot] = STATE.EFFECT;
    this.ops[effectSlot] = OP.EFFECT;

    const run = (): V => {
      const previous = this.trackingDependencies;
      const dependencies: N[] = [];

      this.trackingDependencies = dependencies;

      try {
        fn();
      } finally {
        this.trackingDependencies = previous;
      }

      this.storeDependencies(effectSlot, dependencies);
    };

    this.handlers[effectSlot] = run as unknown as SubHandler;
    run();

    return () => {
      this.releaseSlot(effectSlot);
    };
  }

  public stats() {
    return {
      capacity: this.capacity,
      liveSlots: this.nextPid - 1 - this.freeTail,
      laneLength: this.laneLength,
      payloadBytesUsed: this.payloadTail,
      pendingReplies: this.pending.size,
    };
  }

  /* ------------------------------------------------------------------ *
   * Channel resolution: r/m/s/k becomes one integer slot.
   * ------------------------------------------------------------------ */

  private channelKey(channel: ChannelAddress): string {
    return [
      channel.r ?? "",
      channel.m ?? "",
      channel.s ?? "",
      channel.k ?? "",
    ].join("|");
  }

  private resolveChannel(channel: ChannelAddress): N {
    const composite = this.channelKey(channel);

    const existing = this.channelSlots.get(composite);
    if (existing !== undefined) return existing;

    const slot = this.allocateSlot();

    this.state[slot] = STATE.CHANNEL;
    this.ops[slot] = OP.NONE;
    this.keyR[slot] = this.intern(channel.r ?? "");
    this.keyM[slot] = this.intern(channel.m ?? "");
    this.keyS[slot] = this.intern(channel.s ?? "");
    this.keyK[slot] = this.intern(channel.k ?? "");

    this.channelSlots.set(composite, slot);

    return slot;
  }

  private intern(value: string): N {
    const existing = this.interned.get(value);
    if (existing !== undefined) return existing;

    const id = this.interned.size;
    this.interned.set(value, id);

    return id;
  }

  /* ------------------------------------------------------------------ *
   * Payload encoding: JS payload becomes bytes only at the boundary.
   * ------------------------------------------------------------------ */

  private ensurePayloadCapacity(length: N): V {
    if (this.payloadTail + length <= this.payloadHeap.length) return;

    const nextLength = Math.max(
      this.payloadHeap.length * 2,
      this.payloadTail + length,
    );

    const next = new Uint8Array(nextLength);
    next.set(this.payloadHeap.subarray(0, this.payloadTail));
    this.payloadHeap = next;
  }

  private encodePayload(value: any): { offset: N; length: N } {
    const heap = this.payloadHeap;

    if (value === null || value === undefined) {
      this.ensurePayloadCapacity(1);
      const offset = this.payloadTail;
      heap[offset] = PAYLOAD.NULL;
      this.payloadTail += 1;

      return { offset, length: 1 };
    }

    if (typeof value === "number" && Number.isFinite(value)) {
      this.ensurePayloadCapacity(9);
      const offset = this.payloadTail;

      heap[offset] = PAYLOAD.NUMBER;
      new Float64Array(heap.buffer, offset + 1, 1)[0] = value;
      this.payloadTail += 9;

      return { offset, length: 9 };
    }

    if (typeof value === "boolean") {
      this.ensurePayloadCapacity(1);
      const offset = this.payloadTail;

      heap[offset] = value ? PAYLOAD.BOOLEAN_TRUE : PAYLOAD.BOOLEAN_FALSE;
      this.payloadTail += 1;

      return { offset, length: 1 };
    }

    if (typeof value === "string") {
      const bytes = new TextEncoder().encode(value);
      this.ensurePayloadCapacity(bytes.length + 5);

      const offset = this.payloadTail;
      const view = new DataView(heap.buffer);

      heap[offset] = PAYLOAD.STRING;
      view.setUint32(offset + 1, bytes.length, true);
      heap.set(bytes, offset + 5);
      this.payloadTail += bytes.length + 5;

      return { offset, length: bytes.length + 5 };
    }

    const json = JSON.stringify(value);
    const bytes = new TextEncoder().encode(json);

    this.ensurePayloadCapacity(bytes.length + 5);

    const offset = this.payloadTail;
    const view = new DataView(heap.buffer);

    heap[offset] = PAYLOAD.JSON;
    view.setUint32(offset + 1, bytes.length, true);
    heap.set(bytes, offset + 5);
    this.payloadTail += bytes.length + 5;

    return { offset, length: bytes.length + 5 };
  }

  private decodePayload(offset: N, length: N): any {
    if (offset < 0 || length === 0) return undefined;

    const heap = this.payloadHeap;
    const tag = heap[offset];

    if (tag === PAYLOAD.NULL) return null;

    if (tag === PAYLOAD.NUMBER) {
      return new Float64Array(heap.buffer, offset + 1, 1)[0];
    }

    if (tag === PAYLOAD.BOOLEAN_FALSE) return false;
    if (tag === PAYLOAD.BOOLEAN_TRUE) return true;

    const view = new DataView(heap.buffer);
    const byteLength = view.getUint32(offset + 1, true);
    const bytes = heap.subarray(offset + 5, offset + 5 + byteLength);
    const text = new TextDecoder().decode(bytes);

    if (tag === PAYLOAD.STRING) return text;
    if (tag === PAYLOAD.JSON) return JSON.parse(text);

    return undefined;
  }

  /* ------------------------------------------------------------------ *
   * Subscriber links: integer-linked lists, not object graphs.
   * ------------------------------------------------------------------ */

  private addSubscriber(
    channel: ChannelAddress,
    handler: SubHandler | StreamHandler,
    state: N,
    op: N,
  ): () => V {
    const channelSlot = this.resolveChannel(channel);
    const handlerSlot = this.allocateHandlerSlot(channel, handler, state, op);

    this.linkSubscriber(channelSlot, handlerSlot);

    return () => {
      this.unlinkSubscriber(channelSlot, handlerSlot);
      this.releaseSlot(handlerSlot);
    };
  }

  private allocateHandlerSlot(
    channel: ChannelAddress,
    handler: SubHandler | StreamHandler | DuplexHandler,
    state: N,
    op: N,
  ): N {
    const slot = this.allocateSlot();

    this.state[slot] = state;
    this.ops[slot] = op;
    this.handlerId[slot] = this.handlers.length;

    this.handlers.push(handler);

    return slot;
  }

  private linkSubscriber(channelSlot: N, handlerSlot: N): V {
    const entry = this.allocateSlot();

    this.state[entry] = STATE.FREE;
    this.subscriberTarget[entry] = handlerSlot;
    this.subscriberNext[entry] = this.subscriberHead[channelSlot];
    this.subscriberHead[channelSlot] = entry;
  }

  private unlinkSubscriber(channelSlot: N, handlerSlot: N): V {
    let previous = -1;
    let entry = this.subscriberHead[channelSlot];

    while (entry !== -1) {
      const next = this.subscriberNext[entry];

      if (this.subscriberTarget[entry] === handlerSlot) {
        if (previous === -1) {
          this.subscriberHead[channelSlot] = next;
        } else {
          this.subscriberNext[previous] = next;
        }

        this.releaseSlot(entry);
        return;
      }

      previous = entry;
      entry = next;
    }
  }

  private notifySubscribers(channelSlot: N, op: N): V {
    let entry = this.subscriberHead[channelSlot];

    while (entry !== -1) {
      const handlerSlot = this.subscriberTarget[entry];

      if (handlerSlot !== -1) {
        this.admit(handlerSlot, op);
      }

      entry = this.subscriberNext[entry];
    }
  }

  /* ------------------------------------------------------------------ *
   * Dependencies: version-stamped pull validation.
   * ------------------------------------------------------------------ */

  private storeDependencies(effectSlot: N, dependencies: N[]): V {
    const start = this.payloadTail;

    this.dependencyOffset[effectSlot] = start;
    this.dependencyCount[effectSlot] = dependencies.length;

    for (let i = 0; i < dependencies.length; i++) {
      const dependency = dependencies[i];

      this.ensurePayloadCapacity(4);
      new Int32Array(this.payloadHeap.buffer, this.payloadTail, 1)[0] = dependency;
      this.payloadTail += 4;
    }

    this.refreshDependencyVersions(effectSlot, dependencies, start);
  }

  private refreshDependencyVersions(effectSlot: N, dependencies: N[], start: N): V {
    const view = new Int32Array(this.payloadHeap.buffer);

    for (let i = 0; i < dependencies.length; i++) {
      view[start + i] = this.version[dependencies[i]];
    }

    this.dependencyVersion[effectSlot] = this.epoch;
  }

  private dependenciesChanged(effectSlot: N): boolean {
    const offset = this.dependencyOffset[effectSlot];
    const count = this.dependencyCount[effectSlot];

    if (offset < 0 || count === 0) return false;

    const view = new Int32Array(this.payloadHeap.buffer);

    for (let i = 0; i < count; i++) {
      const dependency = view[offset + i];

      if (this.version[dependency] !== view[offset + i]) {
        return true;
      }
    }

    return false;
  }

  /* ------------------------------------------------------------------ *
   * Slot allocator.
   * ------------------------------------------------------------------ */

  private allocateSlot(): N {
    if (this.freeTail > 0) {
      const reused = this.freePids[--this.freeTail];
      this.resetSlot(reused);

      return reused;
    }

    if (this.nextPid >= this.capacity) {
      throw new Error("ReactiveMemory: slot capacity exhausted");
    }

    const slot = this.nextPid++;
    this.resetSlot(slot);

    return slot;
  }

  private resetSlot(slot: N): V {
    this.state[slot] = STATE.FREE;
    this.ops[slot] = OP.NONE;
    this.version[slot] = 0;

    this.keyR[slot] = -1;
    this.keyM[slot] = -1;
    this.keyS[slot] = -1;
    this.keyK[slot] = -1;

    this.payloadOffset[slot] = -1;
    this.payloadLength[slot] = 0;

    this.subscriberHead[slot] = -1;
    this.subscriberNext[slot] = -1;
    this.subscriberTarget[slot] = -1;

    this.dependencyOffset[slot] = -1;
    this.dependencyCount[slot] = 0;

    this.handlerId[slot] = -1;
  }

  private releaseSlot(slot: N): V {
    if (slot === TRACER_PID) return;

    this.handlers[this.handlerId[slot]] = undefined as any;
    this.resetSlot(slot);

    this.freePids[this.freeTail++] = slot;
  }

  /* ------------------------------------------------------------------ *
   * Dirty lane: the scheduler.
   * ------------------------------------------------------------------ */

  private admit(pid: N, op: N): V {
    if ((this.ops[pid] & IN_LANE) !== 0) {
      this.ops[pid] = op | IN_LANE;
      return;
    }

    if (this.laneLength >= this.capacity) {
      throw new Error("ReactiveMemory: dirty lane overflow");
    }

    this.lane[this.laneLength++] = pid;
    this.ops[pid] = op | IN_LANE;
  }

  public pass(): V {
    let readIndex = 0;
    let writeIndex = 0;

    while (readIndex < this.laneLength) {
      const pid = this.lane[readIndex++];
      const op = this.ops[pid] & OP_MASK;

      this.ops[pid] = OP.NONE | IN_LANE;

      if (pid === TRACER_PID) {
        this.lane[writeIndex++] = pid;
        continue;
      }

      const keep = this.execute(pid, op);

      if (keep) {
        this.lane[writeIndex++] = pid;
      } else {
        this.ops[pid] = OP.NONE;
      }
    }

    this.laneLength = writeIndex;
  }

  private execute(pid: N, op: N): boolean {
    switch (op) {
      case OP.WRITE:
      case OP.EDIT:
      case OP.PUT:
        this.notifySubscribers(pid, OP.SUB);
        return true;

      case OP.SEND:
        return this.executeSend(pid);

      case OP.SUB:
        return this.executeSubscriber(pid, false);

      case OP.STREAM:
        return this.executeSubscriber(pid, true);

      case OP.DUPLEX:
        return this.executeSubscriber(pid, false);

      case OP.EFFECT:
        return this.executeEffect(pid);

      case OP.DELETE:
        this.releaseSlot(pid);
        return false;

      default:
        return true;
    }
  }

  private executeSend(channelSlot: N): boolean {
    const pendingId = this.pendingQueue.shift();

    this.notifySubscribers(channelSlot, OP.SUB);

    if (pendingId === undefined) return true;

    const record = this.pending.get(pendingId);

    if (!record) return true;

    // If no subscriber replies synchronously, leave the promise pending.
    // A sub handler receives ctx.reply() and resolves it.
    return true;
  }

  private executeSubscriber(handlerSlot: N, isStream: boolean): boolean {
    const handler = this.handlers[this.handlerId[handlerSlot]];

    if (!handler) return false;

    // A subscriber is linked to exactly one channel in this implementation.
    // Find that channel by scanning reverse links only when needed.
    const channelSlot = this.findOwnerChannel(handlerSlot);

    if (channelSlot === -1) return false;

    const payload = this.decodePayload(
      this.payloadOffset[channelSlot],
      this.payloadLength[channelSlot],
    );

    const pendingId = isStream ? undefined : this.pendingQueue.shift();

    const ctx: SubContext = {
      channel: {
        r: this.readString(this.keyR[channelSlot]),
        m: this.readString(this.keyM[channelSlot]),
        s: this.readString(this.keyS[channelSlot]),
        k: this.readString(this.keyK[channelSlot]),
      },
      reply: (value: any) => {
        if (pendingId === undefined) return;

        const record = this.pending.get(pendingId);
        if (!record) return;

        this.pending.delete(pendingId);
        record.resolve(value);
      },
      emit: (value: any) => {
        this.emitStream(ctx.channel, value);
      },
    };

    try {
      const result = handler(payload, ctx);

      if (
        pendingId !== undefined &&
        result !== undefined &&
        result !== null &&
        typeof (result as any).then === "function"
      ) {
        (result as Promise<any>).then(
          (value) => ctx.reply(value),
          (error) => {
            const record = this.pending.get(pendingId);
            if (record) {
              this.pending.delete(pendingId);
              record.reject(error);
            }
          },
        );
      } else if (pendingId !== undefined && result !== undefined) {
        ctx.reply(result);
      }
    } catch (error) {
      if (pendingId !== undefined) {
        const record = this.pending.get(pendingId);

        if (record) {
          this.pending.delete(pendingId);
          record.reject(error);
        }
      }
    }

    return true;
  }

  private executeEffect(effectSlot: N): boolean {
    const handler = this.handlers[this.handlerId[effectSlot]] as unknown as
      | (() => V | Promise<V>)
      | undefined;

    if (!handler) return false;

    if (!this.dependenciesChanged(effectSlot)) return true;

    handler();

    return true;
  }

  private findOwnerChannel(handlerSlot: N): N {
    for (let slot = 1; slot < this.nextPid; slot++) {
      if (this.state[slot] !== STATE.CHANNEL) continue;

      let entry = this.subscriberHead[slot];

      while (entry !== -1) {
        if (this.subscriberTarget[entry] === handlerSlot) return slot;
        entry = this.subscriberNext[entry];
      }
    }

    return -1;
  }

  private readString(id: N): string {
    for (const [value, internedId] of this.interned) {
      if (internedId === id) return value;
    }

    return "";
  }
}
