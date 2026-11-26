export const HEADER_SIZE_BYTES = 64; // Int32 Array Indices (4 bytes per element)
export const HEADER_IDX_WRITE_HEAD = 0; // Byte offset 0
export const HEADER_IDX_READ_HEAD = 1; // Byte offset 4
export const HEADER_IDX_MUTEX = 2; // Byte offset 8
export const HEADER_IDX_CAPACITY = 3; // Byte offset 12
export const MUTEX_UNLOCKED = 0; // Mutex States
export const MUTEX_LOCKED = 1;
export const SENTINEL_WRAP_MARKER = 0xFFFFFFFF; // 0xFFFFFFFF (Uint32 max) written to length prefix slot when p wraps to offset 0
export function createSharedDepotBuffer(capacityBytes) {
    // Enforce 64-byte alignment on capacity
    const alignedCapacity = Math.ceil(capacityBytes / 64) * 64;
    const totalSizeBytes = HEADER_SIZE_BYTES + alignedCapacity;
    const sab = new SharedArrayBuffer(totalSizeBytes);
    const headerView = new Int32Array(sab, 0, 4);
    // Initialize Header State
    Atomics.store(headerView, HEADER_IDX_WRITE_HEAD, 0);
    Atomics.store(headerView, HEADER_IDX_READ_HEAD, 0);
    Atomics.store(headerView, HEADER_IDX_MUTEX, MUTEX_UNLOCKED);
    Atomics.store(headerView, HEADER_IDX_CAPACITY, alignedCapacity);
    return sab;
}
export function mapDepotBuffer(sab) {
    // Map control slots (first 16 bytes containing 4 x Int32 values)
    const headerInt32 = new Int32Array(sab, 0, 4);
    // Read capacity directly from header slot
    const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
    // Map p slice strictly starting at byte 64
    const payloadUint8 = new Uint8Array(sab, HEADER_SIZE_BYTES, capacity);
    return {
        headerInt32,
        payloadUint8,
        capacity
    };
}
export function validateHeader(sab) {
    if (sab.byteLength < HEADER_SIZE_BYTES)
        return false;
    const headerInt32 = new Int32Array(sab, 0, 4);
    const capacity = Atomics.load(headerInt32, HEADER_IDX_CAPACITY);
    // Buffer length must equal 64 bytes header + registered capacity
    return sab.byteLength === HEADER_SIZE_BYTES + capacity;
}
export function calculateFrameFootprint(payloadSizeBytes) {
    const rawEnvelopeBytes = 4 + payloadSizeBytes;
    return Math.ceil(rawEnvelopeBytes / 4) * 4;
}
const FLYWEIGHT_FRAME = {
    topicId: 0,
    topicName: undefined,
    p: new Uint8Array(0)
};
export class BinaryEncoder {
    static topicToIdMap = new Map();
    static idToTopicMap = new Map();
    static hashTopic(topic) {
        let hash = 0x811c9dc5;
        for (let i = 0; i < topic.length; i++) {
            hash ^= topic.charCodeAt(i);
            hash = Math.imul(hash, 0x01000193) >>> 0;
        }
        return hash;
    }
    static getTopicId(topic) {
        const existingId = this.topicToIdMap.get(topic);
        if (existingId !== undefined) {
            return existingId;
        }
        const topicId = this.hashTopic(topic);
        const collisionTopic = this.idToTopicMap.get(topicId);
        if (collisionTopic !== undefined && collisionTopic !== topic) {
            throw new Error(`Topic hash collision detected between "${topic}" and "${collisionTopic}" (ID: ${topicId})`);
        }
        this.topicToIdMap.set(topic, topicId);
        this.idToTopicMap.set(topicId, topic);
        return topicId;
    }
    static getTopicName(topicId) {
        return this.idToTopicMap.get(topicId);
    }
    static registerTopics(topics) {
        for (const topic of topics) {
            this.getTopicId(topic);
        }
    }
    static resetRegistry() {
        this.topicToIdMap.clear();
        this.idToTopicMap.clear();
    }
}
export class BinaryFrameSerializer {
    static textEncoder = new TextEncoder();
    // Reusable scratch buffer & DataView for header manipulations (Zero GC)
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
