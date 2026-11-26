// @ts-ignore -- native N-API addon; runtime resolves the .node binary
const walSharedMemoryModule = await require('../wal_shared_memory.node');

const native =  (walSharedMemoryModule as any).default ?? walSharedMemoryModule;

export type BoundaryType = 'SAME_APP' | 'SAME_NODE' | 'CROSS_NODE';
export type ContainerKind = 'WORKER' | 'PROCESS';
export interface HeaderViews {
  headerInt32: Int32Array;
  payloadUint8: Uint8Array;
  capacity: N;
}
export interface DecodedFrame {
  topicId: N;
  topicName: S | undefined;
  p: U8;
}
export interface RingPairDescriptor {
  aToB_fast: SharedArrayBuffer;
  bToA_fast: SharedArrayBuffer;
  aToB_overflow: SharedArrayBuffer;
  bToA_overflow: SharedArrayBuffer;
}
export interface NativeRingPairDescriptor {
  name: S;
  requestBytes: N;
  responseBytes: N;
  overflowBytes: N;
  maxPayloadBytes: N;
}
export interface NativeSharedMemoryBinding {
  create(name: S, requestBytes: N, responseBytes: N, overflowBytes: N, maxPayloadBytes: N): NativeRingPairDescriptor;
  attach(descriptor: NativeRingPairDescriptor): RingPair;
}
export interface PeerRegistryEntry {
  addressSpaceId: S;
  processId: S;
  boundaryType: BoundaryType;
  container?: ContainerKind;
  k: S;
  s: S;
  m: S;
  r: S;
  capacityBytes: N;
  ring?: RingPairDescriptor;
  native?: NativeRingPairDescriptor;
}
export class BinaryEncoder {
  private static topicToId: Record<S, N> = Object.create(null);
  private static idToTopic: Record<N, S> = Object.create(null);

  public static hashTopic(topic: S): N {
    let hash = 0x811c9dc5;
    for (let i = 0; i < topic.length; i++) {
      hash ^= topic.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash;
  }

  public static getTopicId(topic: S): N {
    const existingId = this.topicToId[topic];
    if (existingId !== undefined) return existingId;

    const topicId = this.hashTopic(topic);
    const collisionTopic = this.idToTopic[topicId];

    if (collisionTopic !== undefined && collisionTopic !== topic) {
      throw new Error(`Topic hash collision between "${topic}" and "${collisionTopic}" (ID: ${topicId})`);
    }

    this.topicToId[topic] = topicId;
    this.idToTopic[topicId] = topic;
    return topicId;
  }

  public static getTopicName(topicId: N): S | undefined {
    return this.idToTopic[topicId];
  }

  public static registerTopics(topics: AR<S>): void {
    for (const topic of topics) this.getTopicId(topic);
  }

  public static resetRegistry(): void {
    this.topicToId = Object.create(null);
    this.idToTopic = Object.create(null);
  }
}
const FLYWEIGHT_FRAME: DecodedFrame = {
  topicId: 0,
  topicName: undefined,
  p: new Uint8Array(0)
};
export class BinaryFrameSerializer {
  private static textEncoder = new TextEncoder();
  private static headerBuffer = new ArrayBuffer(4);
  private static headerView = new DataView(BinaryFrameSerializer.headerBuffer);
  private static headerBytes = new Uint8Array(BinaryFrameSerializer.headerBuffer);

  public static encodeFrame(topic: S, p: U8 | S | Record<S, U>): U8 {
    const topicId = BinaryEncoder.getTopicId(topic);
    const payloadBytes = p instanceof Uint8Array
      ? p
      : this.textEncoder.encode(typeof p === 'string' ? p : JSON.stringify(p));

    const frame = new Uint8Array(4 + payloadBytes.byteLength);
    this.headerView.setUint32(0, topicId, true);
    frame.set(this.headerBytes, 0);
    frame.set(payloadBytes, 4);
    return frame;
  }

  public static decodeFrame(frameBytes: U8): DecodedFrame {
    if (frameBytes.byteLength < 4) throw new Error('Invalid frame: buffer smaller than minimum header size (4 bytes).');

    this.headerBytes[0] = frameBytes[0];
    this.headerBytes[1] = frameBytes[1];
    this.headerBytes[2] = frameBytes[2];
    this.headerBytes[3] = frameBytes[3];

    FLYWEIGHT_FRAME.topicId = this.headerView.getUint32(0, true);
    FLYWEIGHT_FRAME.topicName = BinaryEncoder.getTopicName(FLYWEIGHT_FRAME.topicId);
    FLYWEIGHT_FRAME.p = frameBytes.subarray(4);
    return FLYWEIGHT_FRAME;
  }
}
export const HEADER_SIZE_BYTES = 64;
export const HEADER_IDX_WRITE_HEAD = 0;
export const HEADER_IDX_READ_HEAD = 1;
export const HEADER_IDX_MUTEX = 2;
export const HEADER_IDX_CAPACITY = 3;
export const MUTEX_UNLOCKED = 0;
export const MUTEX_LOCKED = 1;
export const SENTINEL_WRAP_MARKER = 0xFFFFFFFF;
export function createSharedDepotBuffer(capacityBytes: N): SharedArrayBuffer {
  const alignedCapacity = Math.ceil(capacityBytes / 64) * 64;
  const sab = new SharedArrayBuffer(HEADER_SIZE_BYTES + alignedCapacity);
  const header = new Int32Array(sab, 0, 4);

  Atomics.store(header, HEADER_IDX_WRITE_HEAD, 0);
  Atomics.store(header, HEADER_IDX_READ_HEAD, 0);
  Atomics.store(header, HEADER_IDX_MUTEX, MUTEX_UNLOCKED);
  Atomics.store(header, HEADER_IDX_CAPACITY, alignedCapacity);
  return sab;
}
export function mapDepotBuffer(sab: SharedArrayBuffer): HeaderViews {
  const headerInt32 = new Int32Array(sab, 0, 4);
  const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
  return {
    headerInt32,
    payloadUint8: new Uint8Array(sab, HEADER_SIZE_BYTES, capacity),
    capacity
  };
}
export function validateHeader(sab: SharedArrayBuffer): B {
  if (sab.byteLength < HEADER_SIZE_BYTES) return false;
  const header = new Int32Array(sab, 0, 4);
  return sab.byteLength === HEADER_SIZE_BYTES + Atomics.load(header, HEADER_IDX_CAPACITY);
}
export function calculateFrameFootprint(payloadSizeBytes: N): N {
  return Math.ceil((4 + payloadSizeBytes) / 4) * 4;
}
export class RING_BUFFER {
  private headerInt32: Int32Array;
  private payloadUint8: Uint8Array;
  private payloadDataView: DataView;
  public readonly capacity: N;

  constructor(sab: SharedArrayBuffer) {
    const views = mapDepotBuffer(sab);
    this.headerInt32 = views.headerInt32;
    this.payloadUint8 = views.payloadUint8;
    this.capacity = views.capacity;
    this.payloadDataView = new DataView(sab, HEADER_SIZE_BYTES, this.capacity);
  }

  private acquireLock(): void {
    while (Atomics.compareExchange(this.headerInt32, HEADER_IDX_MUTEX, MUTEX_UNLOCKED, MUTEX_LOCKED) !== MUTEX_UNLOCKED) {}
  }

  private releaseLock(): void {
    Atomics.store(this.headerInt32, HEADER_IDX_MUTEX, MUTEX_UNLOCKED);
  }

  public write(p: U8): B {
    const footprint = calculateFrameFootprint(p.byteLength);
    if (footprint >= this.capacity) return false;

    this.acquireLock();
    try {
      const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
      const readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
      let target = writeHead;

      if (readHead === writeHead) {
        Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, 0);
        target = 0;
      } else if (writeHead > readHead) {
        const tail = this.capacity - writeHead;

        if (footprint <= tail) {
          if (footprint === tail && readHead === 0) return false;
        } else {
          if (footprint >= readHead) return false;
          this.payloadDataView.setUint32(writeHead, SENTINEL_WRAP_MARKER, true);
          target = 0;
        }
      } else {
        if (footprint >= readHead - writeHead) return false;
      }

      this.payloadDataView.setUint32(target, p.byteLength, true);
      this.payloadUint8.set(p, target + 4);
      Atomics.store(this.headerInt32, HEADER_IDX_WRITE_HEAD, (target + footprint) % this.capacity);
      Atomics.notify(this.headerInt32, HEADER_IDX_WRITE_HEAD, 1);
      return true;
    } finally {
      this.releaseLock();
    }
  }

  public read(): U8 | null {
    this.acquireLock();
    try {
      let readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
      const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
      if (readHead === writeHead) return null;

      let frameHeader = this.payloadDataView.getUint32(readHead, true);

      if (frameHeader === SENTINEL_WRAP_MARKER) {
        readHead = 0;
        Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, 0);
        if (readHead === writeHead) return null;
        frameHeader = this.payloadDataView.getUint32(readHead, true);
      }

      const payloadLength = frameHeader;
      const footprint = calculateFrameFootprint(payloadLength);
      const p = this.payloadUint8.subarray(readHead + 4, readHead + 4 + payloadLength);

      Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, (readHead + footprint) % this.capacity);
      return p;
    } finally {
      this.releaseLock();
    }
  }

  public waitForDataAsync(timeoutMs: N = 1000): P<'ok' | 'not-equal' | 'timed-out'> {
    const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
    const readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
    if (writeHead !== readHead) return Promise.resolve('ok');

    const result = (Atomics as A).waitAsync(this.headerInt32, HEADER_IDX_WRITE_HEAD, writeHead, timeoutMs);
    return result.async ? result.value : Promise.resolve(result.value);
  }
}
const DISRUPTOR_HEADER_SIZE = 64;
const H_WRITE = 0;
const H_READ = 1;
const H_CAPACITY = 2;
const H_SLOT_BYTES = 3;
const EMPTY_PAYLOAD = new Uint8Array(0);
export function createSharedDisruptorBuffer(capacity: N, maxPayloadBytes: N): SharedArrayBuffer {
  if ((capacity & (capacity - 1)) !== 0) throw new Error('Disruptor capacity must be a power of two.');

  const slotPayloadBytes = Math.ceil(maxPayloadBytes / 8) * 8;
  const total = DISRUPTOR_HEADER_SIZE + capacity * 4 + capacity * 8 + capacity * 4 + capacity + capacity * slotPayloadBytes;
  const sab = new SharedArrayBuffer(total);
  const header = new Int32Array(sab, 0, 4);

  Atomics.store(header, H_WRITE, 0);
  Atomics.store(header, H_READ, 0);
  Atomics.store(header, H_CAPACITY, capacity);
  Atomics.store(header, H_SLOT_BYTES, slotPayloadBytes);
  return sab;
}
export function validateDisruptorHeader(sab: SharedArrayBuffer): B {
  if (sab.byteLength < DISRUPTOR_HEADER_SIZE) return false;

  const header = new Int32Array(sab, 0, 4);
  const capacity = Atomics.load(header, H_CAPACITY);
  const slotPayloadBytes = Atomics.load(header, H_SLOT_BYTES);

  if (capacity <= 0 || (capacity & (capacity - 1)) !== 0) return false;
  return sab.byteLength === DISRUPTOR_HEADER_SIZE + capacity * 4 + capacity * 8 + capacity * 4 + capacity + capacity * slotPayloadBytes;
}
export class POWER_OF_TWO {
  public readonly capacity: N;
  public readonly mask: N;
  public readonly slotPayloadBytes: N;
  public readonly pids: Int32Array;
  public readonly values: Float64Array;
  public readonly lengths: Int32Array;
  public readonly flags: Uint8Array;
  public readonly payload: Uint8Array;
  private readonly header: Int32Array;

  constructor(sab: SharedArrayBuffer) {
    this.header = new Int32Array(sab, 0, 4);
    const capacity = Atomics.load(this.header, H_CAPACITY);

    if ((capacity & (capacity - 1)) !== 0) throw new Error('Disruptor capacity must be a power of two.');

    this.capacity = capacity;
    this.mask = capacity - 1;
    this.slotPayloadBytes = Atomics.load(this.header, H_SLOT_BYTES);

    let offset = DISRUPTOR_HEADER_SIZE;
    this.pids = new Int32Array(sab, offset, capacity); offset += capacity * 4;
    this.values = new Float64Array(sab, offset, capacity); offset += capacity * 8;
    this.lengths = new Int32Array(sab, offset, capacity); offset += capacity * 4;
    this.flags = new Uint8Array(sab, offset, capacity); offset += capacity;
    this.payload = new Uint8Array(sab, offset, capacity * this.slotPayloadBytes);
  }

  public publish(pid: N, value: N, flag: N = 0, payload?: U8): B {
    const payloadLength = payload?.byteLength ?? 0;
    if (payloadLength > this.slotPayloadBytes) return false;

    const write = Atomics.load(this.header, H_WRITE);
    const read = Atomics.load(this.header, H_READ);
    if (write - read >= this.capacity) return false;

    const index = write & this.mask;
    this.pids[index] = pid;
    this.values[index] = value;
    this.flags[index] = flag;
    this.lengths[index] = payloadLength;
    if (payloadLength > 0) this.payload.set(payload!, index * this.slotPayloadBytes);

    Atomics.store(this.header, H_WRITE, write + 1);
    Atomics.notify(this.header, H_WRITE);
    return true;
  }

  public consume(handler: (pid: N, value: N, flag: N, payload: U8) => void): N {
    const targetWrite = Atomics.load(this.header, H_WRITE);
    let read = Atomics.load(this.header, H_READ);
    if (read === targetWrite) return 0;

    let processed = 0;
    while (read < targetWrite) {
      const index = read & this.mask;
      const length = this.lengths[index];
      const base = index * this.slotPayloadBytes;
      const payload = length > 0 ? this.payload.subarray(base, base + length) : EMPTY_PAYLOAD;

      handler(this.pids[index], this.values[index], this.flags[index], payload);
      read++;
      processed++;
    }

    Atomics.store(this.header, H_READ, read);
    return processed;
  }

  public waitForDataAsync(timeoutMs: N = 1000): P<'ok' | 'not-equal' | 'timed-out'> {
    const write = Atomics.load(this.header, H_WRITE);
    const read = Atomics.load(this.header, H_READ);
    if (write !== read) return Promise.resolve('ok');

    const result = (Atomics as A).waitAsync(this.header, H_WRITE, write, timeoutMs);
    return result.async ? result.value : Promise.resolve(result.value);
  }

  public reset(): void {
    Atomics.store(this.header, H_WRITE, 0);
    Atomics.store(this.header, H_READ, 0);
  }
}
function encodeTopicIdFrame(topicId: N, payload: U8): U8 {
  const frame = new Uint8Array(4 + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, topicId, true);
  frame.set(payload, 4);
  return frame;
}
function decodeTopicIdFrame(frame: U8): { topicId: N; payload: U8 } {
  const topicId = new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, true);
  return { topicId, payload: frame.subarray(4) };
}
export class HybridChannel {
  constructor(
    private readonly fast: POWER_OF_TWO,
    private readonly overflow: RING_BUFFER
  ) {}

  public publish(topicId: N, payload: U8, value: N = 0, flag: N = 0): B {
    if (payload.byteLength <= this.fast.slotPayloadBytes && this.fast.publish(topicId, value, flag, payload)) return true;
    return this.overflow.write(encodeTopicIdFrame(topicId, payload));
  }

  public consume(handler: (topicId: N, payload: U8, value: N, flag: N) => void): N {
    let processed = this.fast.consume((pid, value, flag, payload) => handler(pid, payload, value, flag));

    let frame: U8 | null;
    while ((frame = this.overflow.read()) !== null) {
      const { topicId, payload } = decodeTopicIdFrame(frame);
      handler(topicId, payload, 0, 0);
      processed++;
    }

    return processed;
  }
  public async drainLoop(
    handler: (topicId: N, payload: U8, value: N, flag: N) => void,
    signal?: AbortSignal
  ): Promise<void> {
    while (!signal?.aborted) {
      const processed = this.consume(handler);
      if (processed === 0) {
        await Promise.race([
          this.fast.waitForDataAsync(1000),
          this.overflow.waitForDataAsync(1000)
        ]);
      }
    }
  }
}
export class RingPair {
  private constructor(
    public readonly tx: HybridChannel,
    public readonly rx: HybridChannel,
    public readonly descriptor: RingPairDescriptor
  ) {}

  static create(capacity: N, maxPayloadBytes: N, overflowCapacityBytes: N): RingPair {
    const descriptor: RingPairDescriptor = {
      aToB_fast: createSharedDisruptorBuffer(capacity, maxPayloadBytes),
      bToA_fast: createSharedDisruptorBuffer(capacity, maxPayloadBytes),
      aToB_overflow: createSharedDepotBuffer(overflowCapacityBytes),
      bToA_overflow: createSharedDepotBuffer(overflowCapacityBytes)
    };

    return new RingPair(
      new HybridChannel(new POWER_OF_TWO(descriptor.aToB_fast), new RING_BUFFER(descriptor.aToB_overflow)),
      new HybridChannel(new POWER_OF_TWO(descriptor.bToA_fast), new RING_BUFFER(descriptor.bToA_overflow)),
      descriptor
    );
  }

  static attach(descriptor: RingPairDescriptor): RingPair {
    if (
      !validateDisruptorHeader(descriptor.aToB_fast) ||
      !validateDisruptorHeader(descriptor.bToA_fast) ||
      !validateHeader(descriptor.aToB_overflow) ||
      !validateHeader(descriptor.bToA_overflow)
    ) {
      throw new Error('[RingPair] invalid depot header');
    }

    return new RingPair(
      new HybridChannel(new POWER_OF_TWO(descriptor.bToA_fast), new RING_BUFFER(descriptor.bToA_overflow)),
      new HybridChannel(new POWER_OF_TWO(descriptor.aToB_fast), new RING_BUFFER(descriptor.aToB_overflow)),
      descriptor
    );
  }
}

export const NATIVE_SHARED_MEMORY_BRIDGE = {
  create: (
    name: S,
    requestBytes: N,
    responseBytes: N,
    overflowBytes: N,
    maxPayloadBytes: N
  ): NativeRingPairDescriptor => {
    if (!native) throw new Error('[BMRF] native shared-memory binding unavailable');
    return native.create(name, requestBytes, responseBytes, overflowBytes, maxPayloadBytes);
  },

  attach: (descriptor: NativeRingPairDescriptor): RingPair => {
    if (!native) throw new Error('[BMRF] native shared-memory binding unavailable');

    const handle = native.attach(descriptor);

    const requestChannel = {
      publish: (topicId: N, payload: U8, value: N = 0, flag: N = 0): B =>
        native.publishRequest(handle, encodeTopicIdFrame(topicId, payload)),

      consume: (handler: (topicId: N, payload: U8, value: N, flag: N) => void): N => {
        const frame = native.readRequest(handle);
        if (!frame) return 0;

        const decoded = decodeTopicIdFrame(frame.payload);
        handler(decoded.topicId, decoded.payload, 0, 0);
        native.commitRequestRead(handle, frame.nextRead);
        return 1;
      },

      waitForDataAsync: (timeoutMs: N = 1000): P<B> =>
        Promise.resolve(native.waitForRequest(handle, timeoutMs))
    };

    const responseChannel = {
      publish: (topicId: N, payload: U8, value: N = 0, flag: N = 0): B =>
        native.publishResponse(handle, encodeTopicIdFrame(topicId, payload)),

      consume: (handler: (topicId: N, payload: U8, value: N, flag: N) => void): N => {
        const frame = native.readResponse(handle);
        if (!frame) return 0;

        const decoded = decodeTopicIdFrame(frame.payload);
        handler(decoded.topicId, decoded.payload, 0, 0);
        native.commitResponseRead(handle, frame.nextRead);
        return 1;
      },

      waitForDataAsync: (timeoutMs: N = 1000): P<B> =>
        Promise.resolve(native.waitForResponse(handle, timeoutMs))
    };

    return new RingPair(
      requestChannel as A as HybridChannel,
      responseChannel as A as HybridChannel,
      {
        aToB_fast: new SharedArrayBuffer(0),
        bToA_fast: new SharedArrayBuffer(0),
        aToB_overflow: new SharedArrayBuffer(0),
        bToA_overflow: new SharedArrayBuffer(0)
      }
    );
  }
};
const localDepots: Record<S, RingPairDescriptor> = Object.create(null);
function localProcessId(): S {
  return typeof process !== 'undefined' && process.pid != null ? `${process.pid}` : 'worker';
}
function detectContainer(): ContainerKind {
  const g = globalThis as A;
  if (typeof g.importScripts === 'function' && typeof g.window === 'undefined') return 'WORKER';
  if (g.__IS_WORKER_THREAD__ === true) return 'WORKER';
  return 'PROCESS';
}
type BusSend = (msg: A, k: S, s: S, m: S, r: S) => void;
type BusSub = (handler: A, k: S, s: S, m: S, r: S) => void;
export class ControlPlaneBootstrapper {
  private peerRegistry: Record<S, PeerRegistryEntry> = Object.create(null);

  constructor(
    private localAddressSpaceId: S,
    private k: S,
    private s: S,
    private m: S,
    private r: S,
    private onPromote: (peer: PeerRegistryEntry, pair: RingPair) => void,
    private send: BusSend,
    private sub: BusSub
  ) {
    this.sub((msg: A) => {
      if (msg.type !== 'BMRF_DEPOT_REGISTER') return;

      const peer = msg.p as PeerRegistryEntry;
      if (peer.addressSpaceId === this.localAddressSpaceId) return;

      this.peerRegistry[peer.addressSpaceId] = peer;
      this.promoteRouteToDataPlane(peer);
    }, this.k, this.s, this.m, this.r);
  }

  public announceDepot(
    capacity: N,
    maxPayloadBytes: N,
    overflowCapacityBytes: N
  ): RingPair {
    const pair = RingPair.create(capacity, maxPayloadBytes, overflowCapacityBytes);
    const sameProcess = detectContainer() === 'WORKER';

    const peer: PeerRegistryEntry = {
      addressSpaceId: this.localAddressSpaceId,
      processId: localProcessId(),
      boundaryType: 'SAME_NODE',
      container: detectContainer(),
      k: this.k,
      s: this.s,
      m: this.m,
      r: this.r,
      capacityBytes: overflowCapacityBytes,
      ring: sameProcess ? pair.descriptor : undefined,
      native: sameProcess
        ? undefined
        : NATIVE_SHARED_MEMORY_BRIDGE.create(
          `WAL_KVDB_${this.localAddressSpaceId}`,
          capacity,
          capacity,
          overflowCapacityBytes,
          maxPayloadBytes
        )
    };

    localDepots[this.localAddressSpaceId] = pair.descriptor;
    this.send({ type: 'BMRF_DEPOT_REGISTER', p: peer }, this.k, this.s, this.m, this.r);
    return pair;
  }

  private promoteRouteToDataPlane(peer: PeerRegistryEntry): void {
    if (peer.boundaryType === 'CROSS_NODE') return;

    try {
      const pair = peer.processId === localProcessId() && peer.ring
        ? RingPair.attach(peer.ring)
        : NATIVE_SHARED_MEMORY_BRIDGE.attach(peer.native!);

      this.onPromote(peer, pair);
    } catch (error) {
      console.error('[BMRF] promotion failed; staying on baseline transport', error);
    }
  }
}
