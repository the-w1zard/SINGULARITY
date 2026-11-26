// ============================================================================
// SAME NODE, DIFF APP TRANSPORT LAYER - BASED OFF OF THE POWER OF 2 DISRUPTOR
// REPLACING THE BASIC RING BUFFER THAT WAS HERE BEFORE IT.
// ============================================================================
const FLYWEIGHT_FRAME = {
    topicId: 0,
    topicName: undefined,
    p: new Uint8Array(0)
};
export class BinaryEncoder {
    static topicToId = Object.create(null);
    static idToTopic = Object.create(null);
    static hashTopic(topic) {
        let hash = 0x811c9dc5;
        for (let i = 0; i < topic.length; i++) {
            hash ^= topic.charCodeAt(i);
            hash = Math.imul(hash, 0x01000193) >>> 0;
        }
        return hash;
    }
    static getTopicId(topic) {
        const existingId = this.topicToId[topic];
        if (existingId !== undefined)
            return existingId;
        const topicId = this.hashTopic(topic);
        const collisionTopic = this.idToTopic[topicId];
        if (collisionTopic !== undefined && collisionTopic !== topic) {
            throw new Error(`Topic hash collision between "${topic}" and "${collisionTopic}" (ID: ${topicId})`);
        }
        this.topicToId[topic] = topicId;
        this.idToTopic[topicId] = topic;
        return topicId;
    }
    static getTopicName(topicId) {
        return this.idToTopic[topicId];
    }
    static registerTopics(topics) {
        for (const topic of topics)
            this.getTopicId(topic);
    }
    static resetRegistry() {
        this.topicToId = Object.create(null);
        this.idToTopic = Object.create(null);
    }
}
export class BinaryFrameSerializer {
    static textEncoder = new TextEncoder(); // Reusable scratch buffer & DataView for header manipulations (Zero GC)
    static headerBuffer = new ArrayBuffer(4);
    static headerView = new DataView(BinaryFrameSerializer.headerBuffer);
    static headerBytes = new Uint8Array(BinaryFrameSerializer.headerBuffer);
    static encodeFrame(topic, p) {
        const topicId = BinaryEncoder.getTopicId(topic);
        let payloadBytes;
        if (p instanceof Uint8Array) {
            payloadBytes = p;
        }
        else if (typeof p === "string") {
            payloadBytes = this.textEncoder.encode(p);
        }
        else {
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
    static decodeFrame(frameBytes) {
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
export const HEADER_SIZE_BYTES = 64;
export const HEADER_IDX_WRITE_HEAD = 0;
export const HEADER_IDX_READ_HEAD = 1;
export const HEADER_IDX_MUTEX = 2;
export const HEADER_IDX_CAPACITY = 3;
export const MUTEX_UNLOCKED = 0;
export const MUTEX_LOCKED = 1;
export const SENTINEL_WRAP_MARKER = 0xFFFFFFFF;
export function createSharedDepotBuffer(capacityBytes) {
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
export function mapDepotBuffer(sab) {
    const headerInt32 = new Int32Array(sab, 0, 4);
    const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
    const payloadUint8 = new Uint8Array(sab, HEADER_SIZE_BYTES, capacity);
    return { headerInt32, payloadUint8, capacity };
}
export function validateHeader(sab) {
    if (sab.byteLength < HEADER_SIZE_BYTES)
        return false;
    const headerInt32 = new Int32Array(sab, 0, 4);
    const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
    return sab.byteLength === HEADER_SIZE_BYTES + capacity;
}
export function calculateFrameFootprint(payloadSizeBytes) {
    return Math.ceil((4 + payloadSizeBytes) / 4) * 4;
} // ---- RING_BUFFER — your existing overflow-lane class, unchanged ----
export class RING_BUFFER {
    headerInt32;
    payloadUint8;
    payloadDataView;
    capacity;
    constructor(sab) {
        const views = mapDepotBuffer(sab);
        this.headerInt32 = views.headerInt32;
        this.payloadUint8 = views.payloadUint8;
        this.capacity = views.capacity;
        this.payloadDataView = new DataView(sab, HEADER_SIZE_BYTES, this.capacity);
    }
    acquireLock() {
        while (Atomics.compareExchange(this.headerInt32, HEADER_IDX_MUTEX, MUTEX_UNLOCKED, MUTEX_LOCKED) !== MUTEX_UNLOCKED) { }
    }
    releaseLock() { Atomics.store(this.headerInt32, HEADER_IDX_MUTEX, MUTEX_UNLOCKED); }
    write(p) {
        const footprint = calculateFrameFootprint(p.byteLength);
        if (footprint >= this.capacity)
            return false;
        this.acquireLock();
        try {
            const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
            const readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
            let target = writeHead;
            if (readHead === writeHead) {
                Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, 0);
                target = 0;
            }
            else if (writeHead > readHead) {
                const tail = this.capacity - writeHead;
                if (footprint <= tail) {
                    if (footprint === tail && readHead === 0)
                        return false;
                }
                else {
                    if (footprint >= readHead)
                        return false;
                    this.payloadDataView.setUint32(writeHead, SENTINEL_WRAP_MARKER, true);
                    target = 0;
                }
            }
            else {
                if (footprint >= readHead - writeHead)
                    return false;
            }
            this.payloadDataView.setUint32(target, p.byteLength, true);
            this.payloadUint8.set(p, target + 4);
            Atomics.store(this.headerInt32, HEADER_IDX_WRITE_HEAD, (target + footprint) % this.capacity);
            Atomics.notify(this.headerInt32, HEADER_IDX_WRITE_HEAD, 1);
            return true;
        }
        finally {
            this.releaseLock();
        }
    }
    read() {
        this.acquireLock();
        try {
            let readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
            const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
            if (readHead === writeHead)
                return null;
            let frameHeaderVal = this.payloadDataView.getUint32(readHead, true);
            if (frameHeaderVal === SENTINEL_WRAP_MARKER) {
                readHead = 0;
                Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, 0);
                if (readHead === writeHead)
                    return null;
                frameHeaderVal = this.payloadDataView.getUint32(readHead, true);
            }
            const payloadLength = frameHeaderVal;
            const footprint = calculateFrameFootprint(payloadLength);
            const p = this.payloadUint8.slice(readHead + 4, readHead + 4 + payloadLength);
            const nextReadHead = (readHead + footprint) % this.capacity;
            Atomics.store(this.headerInt32, HEADER_IDX_READ_HEAD, nextReadHead);
            return p;
        }
        finally {
            this.releaseLock();
        }
    }
    waitForDataAsync(timeoutMs = 1000) {
        const writeHead = Atomics.load(this.headerInt32, HEADER_IDX_WRITE_HEAD);
        const readHead = Atomics.load(this.headerInt32, HEADER_IDX_READ_HEAD);
        if (writeHead !== readHead)
            return Promise.resolve('ok');
        const res = Atomics.waitAsync(this.headerInt32, HEADER_IDX_WRITE_HEAD, writeHead, timeoutMs);
        return res.async ? res.value : Promise.resolve(res.value);
    }
} // ---- POWER_OF_TWO — now SAB-backed, fixed-width payload lane ----
const DISRUPTOR_HEADER_SIZE = 64;
const H_WRITE = 0, H_READ = 1, H_CAPACITY = 2, H_SLOT_BYTES = 3;
const EMPTY_PAYLOAD = new Uint8Array(0);
export function createSharedDisruptorBuffer(capacity, maxPayloadBytes) {
    if ((capacity & (capacity - 1)) !== 0)
        throw new Error("Disruptor capacity must be a power of two.");
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
export function validateDisruptorHeader(sab) {
    if (sab.byteLength < DISRUPTOR_HEADER_SIZE)
        return false;
    const header = new Int32Array(sab, 0, 4);
    const capacity = Atomics.load(header, H_CAPACITY);
    const slotPayloadBytes = Atomics.load(header, H_SLOT_BYTES);
    if (capacity <= 0 || (capacity & (capacity - 1)) !== 0)
        return false;
    const expected = DISRUPTOR_HEADER_SIZE + capacity * 4 + capacity * 8 + capacity * 4 + capacity + capacity * slotPayloadBytes;
    return sab.byteLength === expected;
}
export class POWER_OF_TWO {
    capacity;
    mask;
    slotPayloadBytes;
    pids;
    values;
    lengths;
    flags;
    payload;
    header;
    constructor(sab) {
        this.header = new Int32Array(sab, 0, 4);
        const capacity = Atomics.load(this.header, H_CAPACITY);
        if ((capacity & (capacity - 1)) !== 0)
            throw new Error("Disruptor capacity must be a power of two.");
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.slotPayloadBytes = Atomics.load(this.header, H_SLOT_BYTES);
        let offset = DISRUPTOR_HEADER_SIZE;
        this.pids = new Int32Array(sab, offset, capacity);
        offset += capacity * 4;
        this.values = new Float64Array(sab, offset, capacity);
        offset += capacity * 8;
        this.lengths = new Int32Array(sab, offset, capacity);
        offset += capacity * 4;
        this.flags = new Uint8Array(sab, offset, capacity);
        offset += capacity;
        this.payload = new Uint8Array(sab, offset, capacity * this.slotPayloadBytes);
    }
    publish(pid, value, flag = 0, payload) {
        const payloadLen = payload?.byteLength ?? 0;
        if (payloadLen > this.slotPayloadBytes)
            return false;
        const w = Atomics.load(this.header, H_WRITE);
        const r = Atomics.load(this.header, H_READ);
        if (w - r >= this.capacity)
            return false;
        const index = w & this.mask;
        this.pids[index] = pid;
        this.values[index] = value;
        this.flags[index] = flag;
        this.lengths[index] = payloadLen;
        if (payloadLen > 0)
            this.payload.set(payload, index * this.slotPayloadBytes);
        Atomics.store(this.header, H_WRITE, w + 1);
        Atomics.notify(this.header, H_WRITE);
        return true;
    }
    consume(handler) {
        const targetWrite = Atomics.load(this.header, H_WRITE);
        let currentRead = Atomics.load(this.header, H_READ);
        if (currentRead === targetWrite)
            return 0;
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
    waitForDataAsync(timeoutMs = 1000) {
        const w = Atomics.load(this.header, H_WRITE);
        const r = Atomics.load(this.header, H_READ);
        if (w !== r)
            return Promise.resolve('ok');
        const res = Atomics.waitAsync(this.header, H_WRITE, w, timeoutMs);
        return res.async ? res.value : Promise.resolve(res.value);
    }
    reset() {
        Atomics.store(this.header, H_WRITE, 0);
        Atomics.store(this.header, H_READ, 0);
    }
} // ---- HybridChannel: fast lane + overflow lane, one API ----
function encodeTopicIdFrame(topicId, payload) {
    const frame = new Uint8Array(4 + payload.byteLength);
    new DataView(frame.buffer).setUint32(0, topicId, true);
    frame.set(payload, 4);
    return frame;
}
function decodeTopicIdFrame(frame) {
    const topicId = new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, true);
    return { topicId, payload: frame.subarray(4) };
}
export class HybridChannel {
    fast;
    overflow;
    fastDrops = 0;
    overflowDrops = 0;
    constructor(fast, overflow) {
        this.fast = fast;
        this.overflow = overflow;
    }
    publish(topicId, payload, value = 0, flag = 0) {
        if (payload.byteLength <= this.fast.slotPayloadBytes) {
            const ok = this.fast.publish(topicId, value, flag, payload);
            if (!ok)
                this.fastDrops++;
            return ok;
        }
        const ok = this.overflow.write(encodeTopicIdFrame(topicId, payload));
        if (!ok)
            this.overflowDrops++;
        return ok;
    }
    consume(handler) {
        let processed = this.fast.consume((pid, value, flag, payload) => handler(pid, payload, value, flag));
        let frame;
        while ((frame = this.overflow.read()) !== null) {
            const { topicId, payload } = decodeTopicIdFrame(frame);
            handler(topicId, payload, 0, 0);
            processed++;
        }
        return processed;
    }
    async drainLoop(handler, signal) {
        while (!signal?.aborted) {
            const processed = this.consume(handler);
            if (processed === 0) {
                await Promise.race([this.fast.waitForDataAsync(1000), this.overflow.waitForDataAsync(1000)]);
            }
        }
    }
    get dropStats() {
        return { fastDrops: this.fastDrops, overflowDrops: this.overflowDrops };
    }
} // ---- RingPair / ControlPlaneBootstrapper ----
const g = globalThis;
function localProcessId() {
    return typeof process !== 'undefined' && process.pid != null ? `${process.pid}` : 'worker';
}
function detectContainer() {
    if (typeof g.importScripts === 'function' && typeof g.window === 'undefined')
        return 'WORKER';
    if (g.__IS_WORKER_THREAD__ === true)
        return 'WORKER';
    return 'PROCESS';
}
export class RingPair {
    tx;
    rx;
    descriptor;
    constructor(tx, rx, descriptor) {
        this.tx = tx;
        this.rx = rx;
        this.descriptor = descriptor;
    }
    static create(capacity, maxPayloadBytes, overflowCapacityBytes) {
        const descriptor = {
            aToB_fast: createSharedDisruptorBuffer(capacity, maxPayloadBytes),
            bToA_fast: createSharedDisruptorBuffer(capacity, maxPayloadBytes),
            aToB_overflow: createSharedDepotBuffer(overflowCapacityBytes),
            bToA_overflow: createSharedDepotBuffer(overflowCapacityBytes),
        };
        const tx = new HybridChannel(new POWER_OF_TWO(descriptor.aToB_fast), new RING_BUFFER(descriptor.aToB_overflow));
        const rx = new HybridChannel(new POWER_OF_TWO(descriptor.bToA_fast), new RING_BUFFER(descriptor.bToA_overflow));
        return new RingPair(tx, rx, descriptor);
    }
    static attach(descriptor) {
        if (!validateDisruptorHeader(descriptor.aToB_fast) || !validateDisruptorHeader(descriptor.bToA_fast) ||
            !validateHeader(descriptor.aToB_overflow) || !validateHeader(descriptor.bToA_overflow)) {
            throw new Error('[RingPair] invalid depot header');
        }
        const tx = new HybridChannel(new POWER_OF_TWO(descriptor.bToA_fast), new RING_BUFFER(descriptor.bToA_overflow));
        const rx = new HybridChannel(new POWER_OF_TWO(descriptor.aToB_fast), new RING_BUFFER(descriptor.aToB_overflow));
        return new RingPair(tx, rx, descriptor);
    }
}
const localDepots = Object.create(null);
export class ControlPlaneBootstrapper {
    localAddressSpaceId;
    k;
    s;
    m;
    r;
    onPromote;
    send;
    sub;
    peerRegistry = Object.create(null);
    constructor(localAddressSpaceId, k, s, m, r, onPromote, send, sub) {
        this.localAddressSpaceId = localAddressSpaceId;
        this.k = k;
        this.s = s;
        this.m = m;
        this.r = r;
        this.onPromote = onPromote;
        this.send = send;
        this.sub = sub;
        this.sub((msg) => {
            if (msg.type !== 'BMRF_DEPOT_REGISTER')
                return;
            const peer = msg.p;
            if (peer.addressSpaceId === this.localAddressSpaceId)
                return;
            this.peerRegistry[peer.addressSpaceId] = peer;
            const ring = localDepots[peer.addressSpaceId];
            if (ring) {
                this.promoteRouteToDataPlane({ ...peer, ring });
            }
        }, this.k, this.s, this.m, this.r);
    }
    announceDepot(capacity, maxPayloadBytes, overflowCapacityBytes) {
        const pair = RingPair.create(capacity, maxPayloadBytes, overflowCapacityBytes);
        localDepots[this.localAddressSpaceId] = pair.descriptor;
        this.send({
            type: 'BMRF_DEPOT_REGISTER',
            p: {
                addressSpaceId: this.localAddressSpaceId,
                processId: localProcessId(),
                boundaryType: 'SAME_NODE',
                container: detectContainer(),
                k: this.k, s: this.s, m: this.m, r: this.r,
                capacityBytes: overflowCapacityBytes,
            },
        }, this.k, this.s, this.m, this.r);
        return pair;
    }
    promoteRouteToDataPlane(peer) {
        if (!peer.ring)
            return;
        if (peer.boundaryType === 'CROSS_NODE')
            return;
        if (peer.processId !== localProcessId())
            return;
        try {
            const pair = RingPair.attach(peer.ring);
            this.onPromote(peer, pair);
            console.log(`[BMRF] Route to AddressSpace ${peer.addressSpaceId} PROMOTED to Layer 2 Data Plane.`);
        }
        catch (e) {
            console.error('[BMRF] promotion failed, staying on baseline transport', e);
        }
    }
}
