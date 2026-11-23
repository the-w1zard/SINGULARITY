// ============================================================================
// SAME NODE, DIFF APP TRANSPORT LAYER - BASED OFF OF THE POWER OF 2 DISRUPTOR
// REPLACING THE BASIC RING BUFFER THAT WAS HERE BEFORE IT.
// ============================================================================
const FLYWEIGHT_FRAME: DecodedFrame = {
  topicId: 0,
  topicName: undefined,
  p: new Uint8Array(0)
};
export class BinaryEncoder {
  //  32-bit FNV-1a Non-Cryptographic String Hash.
  private static topicToIdMap: Map<string, number> = new Map();
  private static idToTopicMap: Map<number, string> = new Map();
  public static hashTopic(topic: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < topic.length; i++) {
      hash ^= topic.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash;
  }
  public static getTopicId(topic: string): number {
    const existingId = this.topicToIdMap.get(topic);
    if (existingId !== undefined) { return existingId; }

    const topicId = this.hashTopic(topic);
    const collisionTopic = this.idToTopicMap.get(topicId);
    if (collisionTopic !== undefined && collisionTopic !== topic) {
      throw new Error(`Topic hash collision detected between "${topic}" and "${collisionTopic}" (ID: ${topicId})`);
    }

    this.topicToIdMap.set(topic, topicId);
    this.idToTopicMap.set(topicId, topic);
    return topicId;
  }
  public static getTopicName(topicId: number): string | undefined {
    return this.idToTopicMap.get(topicId);
  }
  public static registerTopics(topics: string[]): void {
    for (const topic of topics) {
      this.getTopicId(topic);
    }
  }
  public static resetRegistry(): void {
    this.topicToIdMap.clear();
    this.idToTopicMap.clear();
  }
}
export class BinaryFrameSerializer {
  private static textEncoder = new TextEncoder();// Reusable scratch buffer & DataView for header manipulations (Zero GC)
  private static headerBuffer = new ArrayBuffer(4);
  private static headerView = new DataView(BinaryFrameSerializer.headerBuffer);
  private static headerBytes = new Uint8Array(BinaryFrameSerializer.headerBuffer);
  public static encodeFrame(topic: S, p: U8 | S | Record<S, U>  ): U8 {// Serializes a topic and p into a binary frame.
    const topicId = BinaryEncoder.getTopicId(topic);
    let payloadBytes: Uint8Array;
    if (p instanceof Uint8Array) {
      payloadBytes = p;
    } else if (typeof p === "string") {
      payloadBytes = this.textEncoder.encode(p);
    } else {
      payloadBytes = this.textEncoder.encode(JSON.stringify(p));
    }

    const frame = new Uint8Array(4 + payloadBytes.byteLength);

    // 1. Write Uint32 Topic ID (Little-Endian) via byte copies (Zero DataView allocation)
    this.headerView.setUint32(0, topicId, true);
    frame.set(this.headerBytes, 0);

    // 2. Copy Payload Bytes
    frame.set(payloadBytes, 4);

    return frame;
  }

  /**
   * Zero-GC binary frame decode.
   * Mutates and returns a shared FLYWEIGHT_FRAME object.
   */
  public static decodeFrame(frameBytes: Uint8Array): DecodedFrame {
    if (frameBytes.byteLength < 4) {
      throw new Error("Invalid frame: buffer smaller than minimum header size (4 bytes).");
    }

    // Direct byte copy into static DataView to read Uint32 safely without memory allocation
    this.headerBytes[0] = frameBytes[0];
    this.headerBytes[1] = frameBytes[1];
    this.headerBytes[2] = frameBytes[2];
    this.headerBytes[3] = frameBytes[3];

    const topicId = this.headerView.getUint32(0, true);

    FLYWEIGHT_FRAME.topicId = topicId;
    FLYWEIGHT_FRAME.topicName = BinaryEncoder.getTopicName(topicId);
    FLYWEIGHT_FRAME.p = frameBytes.subarray(4); // Zero-copy view

    return FLYWEIGHT_FRAME;
  }
}
export interface HeaderViews {
  headerInt32: Int32Array; // Views first 16 bytes as Int32 slots
  payloadUint8: Uint8Array; // Views from byte 64 to end of buffer
  capacity: number;
}
export interface ScopeComponents {
  r: number;
  m: number;
  s: number;
}
export interface DecodedFrame {
  topicId: number;
  topicName: string | undefined;
  p: Uint8Array;
}
export interface PeerRegistryEntry {
  addressSpaceId: string
  processId: string
  boundaryType: BoundaryType
  container?: ContainerKind
  k: string
  s: string
  m: string
  r: string
  capacityBytes: number
  ring?: RingPairDescriptor
}
export interface RingPairDescriptor {
  aToB: SharedArrayBuffer
  bToA: SharedArrayBuffer
}
export interface RingPairDescriptor {
  aToB: SharedArrayBuffer
  bToA: SharedArrayBuffer
}
export interface PeerRegistryEntry {
  addressSpaceId: string
  processId: string
  boundaryType: BoundaryType
  container?: ContainerKind
  k: string
  s: string
  m: string
  r: string
  capacityBytes: number
  ring?: RingPairDescriptor
}
export type BoundaryType = 'SAME_APP' | 'SAME_NODE' | 'CROSS_NODE';
export type ContainerKind = 'WORKER' | 'PROCESS';
export interface RingPairDescriptor {
  aToB_fast: SharedArrayBuffer;
  bToA_fast: SharedArrayBuffer;
  aToB_overflow: SharedArrayBuffer;
  bToA_overflow: SharedArrayBuffer;
}
export interface PeerRegistryEntry {
  addressSpaceId: string;
  processId: string;
  boundaryType: BoundaryType;
  container?: ContainerKind;
  k: string; s: string; m: string; r: string;
  capacityBytes: number;
  ring?: RingPairDescriptor;
}
export interface HeaderViews {
  headerInt32: Int32Array;
  payloadUint8: Uint8Array;
  capacity: number;
}
export const HEADER_SIZE_BYTES = 64;
export const HEADER_IDX_WRITE_HEAD = 0;
export const HEADER_IDX_READ_HEAD = 1;
export const HEADER_IDX_MUTEX = 2;
export const HEADER_IDX_CAPACITY = 3;
export const MUTEX_UNLOCKED = 0;
export const MUTEX_LOCKED = 1;
export const SENTINEL_WRAP_MARKER = 0xFFFFFFFF;

export function createSharedDepotBuffer(capacityBytes: number): SharedArrayBuffer {
  const alignedCapacity = Math.ceil(capacityBytes / 64) * 64;
  const totalSizeBytes = HEADER_SIZE_BYTES + alignedCapacity;
  const sab = new SharedArrayBuffer(totalSizeBytes);
  const headerView = new Int32Array(sab, 0, 4);
  Atomics.store(headerView, HEADER_IDX_WRITE_HEAD, 0);
  Atomics.store(headerView, HEADER_IDX_READ_HEAD, 0);
  Atomics.store(headerView, HEADER_IDX_MUTEX, MUTEX_UNLOCKED);
  Atomics.store(headerView, HEADER_IDX_CAPACITY, alignedCapacity);
  return sab;
}
export function mapDepotBuffer(sab: SharedArrayBuffer): HeaderViews {
  const headerInt32 = new Int32Array(sab, 0, 4);
  const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
  const payloadUint8 = new Uint8Array(sab, HEADER_SIZE_BYTES, capacity);
  return { headerInt32, payloadUint8, capacity };
}
export function validateHeader(sab: SharedArrayBuffer): boolean {
  if (sab.byteLength < HEADER_SIZE_BYTES) return false;
  const headerInt32 = new Int32Array(sab, 0, 4);
  const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
  return sab.byteLength === HEADER_SIZE_BYTES + capacity;
}
export function calculateFrameFootprint(payloadSizeBytes: number): number {
  return Math.ceil((4 + payloadSizeBytes) / 4) * 4;
}// ---- RING_BUFFER — your existing overflow-lane class, unchanged ----
export class RING_BUFFER {
  private headerInt32: Int32Array;
  private payloadUint8: Uint8Array;
  private payloadDataView: DataView;
  public readonly capacity: number;

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
  private releaseLock(): void { Atomics.store(this.headerInt32, HEADER_IDX_MUTEX, MUTEX_UNLOCKED); }

  public write(p: Uint8Array): boolean {
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

  public read(): Uint8Array | null {
    this.acquireLock();
    try {
      let readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
      const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
      if (readHead === writeHead) return null;
      let frameHeaderVal = this.payloadDataView.getUint32(readHead, true);
      if (frameHeaderVal === SENTINEL_WRAP_MARKER) {
        readHead = 0;
        Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, 0);
        if (readHead === writeHead) return null;
        frameHeaderVal = this.payloadDataView.getUint32(readHead, true);
      }
      const payloadLength = frameHeaderVal;
      const footprint = calculateFrameFootprint(payloadLength);
      const p = this.payloadUint8.slice(readHead + 4, readHead + 4 + payloadLength);
      const nextReadHead = (readHead + footprint) % this.capacity;
      Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, nextReadHead);
      return p;
    } finally {
      this.releaseLock();
    }
  }

  public waitForDataAsync(timeoutMs = 1000): Promise<'ok' | 'not-equal' | 'timed-out'> {
    const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
    const readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
    if (writeHead !== readHead) return Promise.resolve('ok');
    const res = (Atomics as any).waitAsync(this.headerInt32, HEADER_IDX_WRITE_HEAD, writeHead, timeoutMs);
    return res.async ? res.value : Promise.resolve(res.value);
  }
}// ---- POWER_OF_TWO — now SAB-backed, fixed-width payload lane ----
const DISRUPTOR_HEADER_SIZE = 64;
const H_WRITE = 0, H_READ = 1, H_CAPACITY = 2, H_SLOT_BYTES = 3;
const EMPTY_PAYLOAD = new Uint8Array(0);
export function createSharedDisruptorBuffer(capacity: number, maxPayloadBytes: number): SharedArrayBuffer {
  if ((capacity & (capacity - 1)) !== 0) throw new Error("Disruptor capacity must be a power of two.");
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
export function validateDisruptorHeader(sab: SharedArrayBuffer): boolean {
  if (sab.byteLength < DISRUPTOR_HEADER_SIZE) return false;
  const header = new Int32Array(sab, 0, 4);
  const capacity = Atomics.load(header, H_CAPACITY);
  const slotPayloadBytes = Atomics.load(header, H_SLOT_BYTES);
  if (capacity <= 0 || (capacity & (capacity - 1)) !== 0) return false;
  const expected = DISRUPTOR_HEADER_SIZE + capacity * 4 + capacity * 8 + capacity * 4 + capacity + capacity * slotPayloadBytes;
  return sab.byteLength === expected;
}
export class POWER_OF_TWO {
  public readonly capacity: number;
  public readonly mask: number;
  public readonly slotPayloadBytes: number;
  public readonly pids: Int32Array;
  public readonly values: Float64Array;
  public readonly lengths: Int32Array;
  public readonly flags: Uint8Array;
  public readonly payload: Uint8Array;
  private readonly header: Int32Array;

  constructor(sab: SharedArrayBuffer) {
    this.header = new Int32Array(sab, 0, 4);
    const capacity = Atomics.load(this.header, H_CAPACITY);
    if ((capacity & (capacity - 1)) !== 0) throw new Error("Disruptor capacity must be a power of two.");
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

  public publish(pid: number, value: number, flag = 0, payload?: Uint8Array): boolean {
    const payloadLen = payload?.byteLength ?? 0;
    if (payloadLen > this.slotPayloadBytes) return false;
    const w = Atomics.load(this.header, H_WRITE);
    const r = Atomics.load(this.header, H_READ);
    if (w - r >= this.capacity) return false;
    const index = w & this.mask;
    this.pids[index] = pid;
    this.values[index] = value;
    this.flags[index] = flag;
    this.lengths[index] = payloadLen;
    if (payloadLen > 0) this.payload.set(payload!, index * this.slotPayloadBytes);
    Atomics.store(this.header, H_WRITE, w + 1);
    Atomics.notify(this.header, H_WRITE);
    return true;
  }

  public consume(handler: (pid: number, value: number, flag: number, payload: Uint8Array) => void): number {
    const targetWrite = Atomics.load(this.header, H_WRITE);
    let currentRead = Atomics.load(this.header, H_READ);
    if (currentRead === targetWrite) return 0;
    let processed = 0;
    while (currentRead < targetWrite) {
      const index = currentRead & this.mask;
      const len = this.lengths[index];
      const base = index * this.slotPayloadBytes;
      const payloadView = len > 0 ? this.payload.subarray(base, base + len) : EMPTY_PAYLOAD;
      handler(this.pids[index], this.values[index], this.flags[index], payloadView);
      currentRead++;
      processed++;
    }
    Atomics.store(this.header, H_READ, currentRead);
    return processed;
  }

  public waitForDataAsync(timeoutMs = 1000): Promise<'ok' | 'not-equal' | 'timed-out'> {
    const w = Atomics.load(this.header, H_WRITE);
    const r = Atomics.load(this.header, H_READ);
    if (w !== r) return Promise.resolve('ok');
    const res = (Atomics as any).waitAsync(this.header, H_WRITE, w, timeoutMs);
    return res.async ? res.value : Promise.resolve(res.value);
  }

  public reset(): void {
    Atomics.store(this.header, H_WRITE, 0);
    Atomics.store(this.header, H_READ, 0);
  }
}// ---- HybridChannel: fast lane + overflow lane, one API ----
function encodeTopicIdFrame(topicId: number, payload: Uint8Array): Uint8Array {
  const frame = new Uint8Array(4 + payload.byteLength);
  new DataView(frame.buffer).setUint32(0, topicId, true);
  frame.set(payload, 4);
  return frame;
}
function decodeTopicIdFrame(frame: Uint8Array): { topicId: number; payload: Uint8Array } {
  const topicId = new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, true);
  return { topicId, payload: frame.subarray(4) };
}
export class HybridChannel {
  private fastDrops = 0;
  private overflowDrops = 0;

  constructor(private readonly fast: POWER_OF_TWO, private readonly overflow: RING_BUFFER) {}

  public publish(topicId: number, payload: Uint8Array, value = 0, flag = 0): boolean {
    if (payload.byteLength <= this.fast.slotPayloadBytes) {
      const ok = this.fast.publish(topicId, value, flag, payload);
      if (!ok) this.fastDrops++;
      return ok;
    }
    const ok = this.overflow.write(encodeTopicIdFrame(topicId, payload));
    if (!ok) this.overflowDrops++;
    return ok;
  }

  public consume(handler: (topicId: number, payload: Uint8Array, value: number, flag: number) => void): number {
    let processed = this.fast.consume((pid, value, flag, payload) => handler(pid, payload, value, flag));
    let frame: Uint8Array | null;
    while ((frame = this.overflow.read()) !== null) {
      const { topicId, payload } = decodeTopicIdFrame(frame);
      handler(topicId, payload, 0, 0);
      processed++;
    }
    return processed;
  }

  public async drainLoop(
    handler: (topicId: number, payload: Uint8Array, value: number, flag: number) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    while (!signal?.aborted) {
      const processed = this.consume(handler);
      if (processed === 0) {
        await Promise.race([this.fast.waitForDataAsync(1000), this.overflow.waitForDataAsync(1000)]);
      }
    }
  }

  public get dropStats() {
    return { fastDrops: this.fastDrops, overflowDrops: this.overflowDrops };
  }
}// ---- RingPair / ControlPlaneBootstrapper ----

const g = globalThis as any;
function localProcessId(): string {
  return typeof process !== 'undefined' && (process as any).pid != null ? `${(process as any).pid}` : 'worker';
}
function detectContainer(): ContainerKind {
  if (typeof g.importScripts === 'function' && typeof g.window === 'undefined') return 'WORKER';
  if (g.__IS_WORKER_THREAD__ === true) return 'WORKER';
  return 'PROCESS';
}
export class RingPair {
  private constructor(
    public readonly tx: HybridChannel,
    public readonly rx: HybridChannel,
    public readonly descriptor: RingPairDescriptor,
  ) {}

  static create(capacity: number, maxPayloadBytes: number, overflowCapacityBytes: number): RingPair {
    const descriptor: RingPairDescriptor = {
      aToB_fast: createSharedDisruptorBuffer(capacity, maxPayloadBytes),
      bToA_fast: createSharedDisruptorBuffer(capacity, maxPayloadBytes),
      aToB_overflow: createSharedDepotBuffer(overflowCapacityBytes),
      bToA_overflow: createSharedDepotBuffer(overflowCapacityBytes),
    };
    const tx = new HybridChannel(new POWER_OF_TWO(descriptor.aToB_fast), new RING_BUFFER(descriptor.aToB_overflow));
    const rx = new HybridChannel(new POWER_OF_TWO(descriptor.bToA_fast), new RING_BUFFER(descriptor.bToA_overflow));
    return new RingPair(tx, rx, descriptor);
  }

  static attach(descriptor: RingPairDescriptor): RingPair {
    if (
      !validateDisruptorHeader(descriptor.aToB_fast) || !validateDisruptorHeader(descriptor.bToA_fast) ||
      !validateHeader(descriptor.aToB_overflow) || !validateHeader(descriptor.bToA_overflow)
    ) {
      throw new Error('[RingPair] invalid depot header');
    }
    const tx = new HybridChannel(new POWER_OF_TWO(descriptor.bToA_fast), new RING_BUFFER(descriptor.bToA_overflow));
    const rx = new HybridChannel(new POWER_OF_TWO(descriptor.aToB_fast), new RING_BUFFER(descriptor.aToB_overflow));
    return new RingPair(tx, rx, descriptor);
  }
}
export class ControlPlaneBootstrapper {
  private peerRegistryMap = new Map<string, PeerRegistryEntry>();

  constructor(
    private localAddressSpaceId: string,
    private k: string, private s: string, private m: string, private r: string,
    private onPromote: (peer: PeerRegistryEntry, pair: RingPair) => void,
  ) {
    sub((msg: { type: string; p: PeerRegistryEntry }) => {
      if (msg.type !== 'BMRF_DEPOT_REGISTER') return;
      const peer = msg.p;
      if (peer.addressSpaceId === this.localAddressSpaceId) return;
      this.peerRegistryMap.set(peer.addressSpaceId, peer);
      this.promoteRouteToDataPlane(peer);
    }, this.k, this.s, this.m, this.r);
  }

  public announceDepot(capacity: number, maxPayloadBytes: number, overflowCapacityBytes: number): RingPair {
    const pair = RingPair.create(capacity, maxPayloadBytes, overflowCapacityBytes);
    const payload: PeerRegistryEntry = {
      addressSpaceId: this.localAddressSpaceId,
      processId: localProcessId(),
      boundaryType: 'SAME_NODE',
      container: detectContainer(),
      k: this.k, s: this.s, m: this.m, r: this.r,
      capacityBytes: overflowCapacityBytes,
      ring: pair.descriptor,
    };
    send({ type: 'BMRF_DEPOT_REGISTER', p: payload }, this.k, this.s, this.m, this.r).send();
    return pair;
  }

  private promoteRouteToDataPlane(peer: PeerRegistryEntry): void {
    if (!peer.ring) return;
    if (peer.boundaryType === 'CROSS_NODE') return;
    if (peer.processId !== localProcessId()) return;
    try {
      const pair = RingPair.attach(peer.ring);
      this.onPromote(peer, pair);
      console.log(`[BMRF] Route to AddressSpace ${peer.addressSpaceId} PROMOTED to Layer 2 Data Plane.`);
    } catch (e) {
      console.error('[BMRF] promotion failed, staying on baseline transport', e);
    }
  }
}
