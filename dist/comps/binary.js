const TAG = {
    UNDEFINED: 0,
    NULL: 1,
    FALSE: 2,
    TRUE: 3,
    INT32: 4,
    FLOAT64: 5,
    STRING: 6,
    ARRAY: 7,
    OBJECT: 8,
    UINT8ARRAY: 9,
    REF: 10,
};
const encoder = new TextEncoder();
const decoder = new TextDecoder();
export class BINARY_CODEC {
    encodeJSONEnvelope(envelope) {
        return new TextEncoder().encode(JSON.stringify(envelope));
    }
    decodeJSONEnvelope(bytes) {
        return JSON.parse(new TextDecoder().decode(bytes));
    }
    /* =============================================================================
     Encodes ordinary JavaScript values into a collider-owned byte slab.
     ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
    bytes;
    view;
    tail = 0;
    strings = new Map();
    stringBytes = [];
    refs = new Map();
    nextRef = 0;
    encode_js(value) {
        this.tail = 0;
        this.strings.clear();
        this.stringBytes.length = 0;
        this.refs.clear();
        this.nextRef = 0;
        this.writeValue(value);
        return this.bytes.subarray(0, this.tail);
    }
    /* HC_BINARY_CODEC */
    decode_js(binary) {
        const savedBytes = this.bytes;
        const savedView = this.view;
        const savedTail = this.tail;
        try {
            this.bytes = binary;
            this.view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
            this.tail = binary.length;
            this.refs.clear();
            this.nextRef = 0;
            const cursor = { at: 0 };
            return this.readValue(cursor);
        }
        finally {
            // Restore the encode-side buffer so the next encode()
            // never writes into the disruptor pool.
            this.bytes = savedBytes;
            this.view = savedView;
            this.tail = savedTail;
        }
    }
    need(count) {
        if (this.tail + count <= this.bytes.length)
            return;
        let size = this.bytes.length;
        while (size < this.tail + count)
            size *= 2;
        const next = new Uint8Array(size);
        next.set(this.bytes);
        this.bytes = next;
        this.view = new DataView(next.buffer);
    }
    u8(value) {
        this.need(1);
        this.bytes[this.tail++] = value;
    }
    i32(value) {
        this.need(4);
        this.view.setInt32(this.tail, value, true);
        this.tail += 4;
    }
    f64(value) {
        this.need(8);
        this.view.setFloat64(this.tail, value, true);
        this.tail += 8;
    }
    writeValue(value) {
        if (value === undefined)
            return this.u8(TAG.UNDEFINED);
        if (value === null)
            return this.u8(TAG.NULL);
        if (value === false)
            return this.u8(TAG.FALSE);
        if (value === true)
            return this.u8(TAG.TRUE);
        const type = typeof value;
        if (type === "number") {
            const n = value;
            if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
                this.u8(TAG.INT32);
                return this.i32(n);
            }
            this.u8(TAG.FLOAT64);
            return this.f64(n);
        }
        if (type === "string") {
            return this.writeString(value);
        }
        if (Array.isArray(value)) {
            return this.writeArray(value);
        }
        if (value instanceof Uint8Array) {
            return this.writeBytes(value);
        }
        if (type === "object") {
            return this.writeObject(value);
        }
        throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
    }
    writeString(value) {
        const encoded = new TextEncoder().encode(value);
        this.u8(TAG.STRING);
        this.i32(encoded.length);
        this.need(encoded.length);
        this.bytes.set(encoded, this.tail);
        this.tail += encoded.length;
    }
    writeArray(value) {
        const ref = this.beginRef(value);
        this.u8(TAG.ARRAY);
        this.i32(value.length);
        for (let i = 0; i < value.length; i++) {
            this.writeValue(value[i]);
        }
        this.endRef(ref);
    }
    writeBytes(value) {
        this.u8(TAG.UINT8ARRAY);
        this.i32(value.length);
        this.need(value.length);
        this.bytes.set(value, this.tail);
        this.tail += value.length;
    }
    writeObject(value) {
        const ref = this.beginRef(value);
        const keys = Object.keys(value);
        this.u8(TAG.OBJECT);
        this.i32(keys.length);
        for (const key of keys) {
            this.writeString(key);
            this.writeValue(value[key]);
        }
        this.endRef(ref);
    }
    beginRef(value) {
        const existing = this.refs.get(value);
        if (existing !== undefined) {
            this.u8(TAG.REF);
            this.i32(existing);
            return -1;
        }
        const ref = this.nextRef++;
        this.refs.set(value, ref);
        return ref;
    }
    endRef(ref) {
        // Reserved for future back-reference patching.
        void ref;
    }
    readU8(cursor) {
        return this.bytes[cursor.at++];
    }
    readI32(cursor) {
        const value = this.view.getInt32(cursor.at, true);
        cursor.at += 4;
        return value;
    }
    readF64(cursor) {
        const value = this.view.getFloat64(cursor.at, true);
        cursor.at += 8;
        return value;
    }
    readValue(cursor) {
        const tag = this.readU8(cursor);
        switch (tag) {
            case TAG.UNDEFINED:
                return undefined;
            case TAG.NULL:
                return null;
            case TAG.FALSE:
                return false;
            case TAG.TRUE:
                return true;
            case TAG.INT32:
                return this.readI32(cursor);
            case TAG.FLOAT64:
                return this.readF64(cursor);
            case TAG.STRING:
                return this.readString(cursor);
            case TAG.ARRAY:
                return this.readArray(cursor);
            case TAG.OBJECT:
                return this.readObject(cursor);
            case TAG.UINT8ARRAY:
                return this.readBytes(cursor);
            default:
                throw new Error(`HC_BINARY_CODEC: unknown tag ${tag}`);
        }
    }
    readString(cursor) {
        const length = this.readI32(cursor);
        const bytes = this.bytes.subarray(cursor.at, cursor.at + length);
        cursor.at += length;
        return new TextDecoder().decode(bytes);
    }
    readArray(cursor) {
        const length = this.readI32(cursor);
        const out = new Array(length);
        for (let i = 0; i < length; i++) {
            out[i] = this.readValue(cursor);
        }
        return out;
    }
    readObject(cursor) {
        const length = this.readI32(cursor);
        const out = {};
        for (let i = 0; i < length; i++) {
            const key = this.readString(cursor);
            out[key] = this.readValue(cursor);
        }
        return out;
    }
    readBytes(cursor) {
        const length = this.readI32(cursor);
        const out = this.bytes.slice(cursor.at, cursor.at + length);
        cursor.at += length;
        return out;
    }
    /* =============================================================================
     BINARY ENC, WITH JSON ABOVE
     ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
    ensureAvailable(buf, pos, length) {
        if (pos < 0 || length < 0 || pos + length > buf.length)
            throw new Error("CBOR: truncated buffer");
    }
    readUint(buf, pos) {
        this.ensureAvailable(buf, pos, 1);
        const info = buf[pos] & 0x1f;
        if (info <= 23)
            return [info, pos + 1];
        if (info === 24) {
            this.ensureAvailable(buf, pos + 1, 1);
            return [buf[pos + 1], pos + 2];
        }
        if (info === 25) {
            this.ensureAvailable(buf, pos + 1, 2);
            return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(0, false), pos + 3];
        }
        if (info === 26) {
            this.ensureAvailable(buf, pos + 1, 4);
            return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(0, false), pos + 5];
        }
        if (info === 27) {
            this.ensureAvailable(buf, pos + 1, 8);
            const value = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8).getBigUint64(0, false);
            if (value > BigInt(Number.MAX_SAFE_INTEGER))
                throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER");
            return [Number(value), pos + 9];
        }
        throw new Error(`CBOR: unsupported additional info ${info}`);
    }
    encodeUint64(majorShifted, n) {
        if (n <= 23n)
            return new Uint8Array([majorShifted | Number(n)]);
        if (n <= 0xffn) {
            return new Uint8Array([majorShifted | 24, Number(n)]);
        }
        if (n <= 0xffffn) {
            const b = new Uint8Array(3);
            b[0] = majorShifted | 25;
            new DataView(b.buffer).setUint16(1, Number(n), false);
            return b;
        }
        if (n <= 0xffffffffn) {
            const b = new Uint8Array(5);
            b[0] = majorShifted | 26;
            new DataView(b.buffer).setUint32(1, Number(n), false);
            return b;
        }
        const b = new Uint8Array(9);
        b[0] = majorShifted | 27;
        new DataView(b.buffer).setBigUint64(1, n, false);
        return b;
    }
    encode(value) {
        const parts = [];
        this.encodeValue(value, parts);
        const total = parts.reduce((n, p) => n + p.length, 0);
        const out = new Uint8Array(total);
        let offset = 0;
        for (const p of parts) {
            out.set(p, offset);
            offset += p.length;
        }
        return out;
    }
    encodeValue(v, out) {
        if (v === null) {
            out.push(new Uint8Array([0xf6]));
            return;
        }
        if (v === undefined) {
            out.push(new Uint8Array([0xf7]));
            return;
        }
        if (v === true) {
            out.push(new Uint8Array([0xf5]));
            return;
        }
        if (v === false) {
            out.push(new Uint8Array([0xf4]));
            return;
        }
        if (typeof v === "number") {
            if (Number.isSafeInteger(v)) {
                if (v >= 0)
                    out.push(this.encodeUint64(0x00, BigInt(v)));
                else
                    out.push(this.encodeUint64(0x20, BigInt(-1 - v)));
            }
            else {
                const b = new Uint8Array(9);
                b[0] = 0xfb;
                new DataView(b.buffer).setFloat64(1, v, false);
                out.push(b);
            }
            return;
        }
        if (typeof v === "string") {
            const encoded = encoder.encode(v);
            out.push(this.encodeUint(0x60, encoded.length));
            out.push(encoded);
            return;
        }
        if (v instanceof Uint8Array) {
            out.push(this.encodeUint(0x40, v.length));
            out.push(v);
            return;
        }
        if (Array.isArray(v)) {
            out.push(this.encodeUint(0x80, v.length));
            for (const item of v) {
                this.encodeValue(item, out);
            }
            return;
        }
        if (typeof v === "object") {
            const proto = Object.getPrototypeOf(v);
            if (proto !== Object.prototype && proto !== null)
                throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`);
            const keys = Object.keys(v);
            out.push(this.encodeUint(0xa0, keys.length));
            for (const key of keys) {
                this.encodeValue(key, out);
                this.encodeValue(v[key], out);
            }
            return;
        }
        throw new Error(`CBOR: unsupported value type ${typeof v}`);
    }
    encodeUint(majorShifted, n) {
        if (n <= 23)
            return new Uint8Array([majorShifted | n]);
        if (n <= 0xff)
            return new Uint8Array([majorShifted | 24, n]);
        if (n <= 0xffff) {
            const b = new Uint8Array(3);
            b[0] = majorShifted | 25;
            new DataView(b.buffer).setUint16(1, n, false);
            return b;
        }
        const b = new Uint8Array(5);
        b[0] = majorShifted | 26;
        new DataView(b.buffer).setUint32(1, n, false);
        return b;
    }
    decode(buf) {
        const [value, end] = this.decodeValue(buf, 0);
        if (end !== buf.length) {
            throw new Error("CBOR: trailing bytes");
        }
        return value;
    }
    decodeValue(buf, pos) {
        this.ensureAvailable(buf, pos, 1);
        const first = buf[pos];
        const major = first >> 5;
        const info = first & 0x1f;
        if (major === 7) {
            if (info === 20)
                return [false, pos + 1];
            if (info === 21)
                return [true, pos + 1];
            if (info === 22)
                return [null, pos + 1];
            if (info === 23)
                return [undefined, pos + 1];
            if (info === 27) {
                this.ensureAvailable(buf, pos + 1, 8);
                const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8);
                return [dv.getFloat64(0, false), pos + 9];
            }
        }
        const [n, nextPos] = this.readUint(buf, pos);
        if (major === 0)
            return [n, nextPos];
        if (major === 1)
            return [-1 - n, nextPos];
        if (major === 2) {
            this.ensureAvailable(buf, nextPos, n);
            const bytes = buf.slice(nextPos, nextPos + n);
            return [bytes, nextPos + n];
        }
        if (major === 3) {
            this.ensureAvailable(buf, nextPos, n);
            const bytes = buf.slice(nextPos, nextPos + n);
            return [decoder.decode(bytes), nextPos + n];
        }
        if (major === 4) {
            const arr = [];
            let p = nextPos;
            for (let i = 0; i < n; i++) {
                const [item, np] = this.decodeValue(buf, p);
                arr.push(item);
                p = np;
            }
            return [arr, p];
        }
        if (major === 5) {
            const obj = {};
            let p = nextPos;
            for (let i = 0; i < n; i++) {
                const [k, kp] = this.decodeValue(buf, p);
                const [v, vp] = this.decodeValue(buf, kp);
                obj[k] = v;
                p = vp;
            }
            return [obj, p];
        }
        throw new Error(`CBOR: unsupported major type ${major}`);
    }
    encode_msg(msg) {
        // Instead of leaking a raw object, encode an explicit fallback/error frame
        if (msg instanceof Uint8Array)
            return msg; // If it's already an encoded Uint8Array, don't re-encode it
        try {
            return this.encode(msg);
        }
        catch (err) {
            console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err);
            return this.encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" });
        }
    }
    decode_msg(raw) {
        // Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
        if (!(raw instanceof Uint8Array) && !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))) {
            console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
            return null;
        }
        try {
            const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array(raw.buffer, raw.byteOffset, raw.length);
            return this.decode(uint8);
        }
        catch (err) {
            console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
            return null;
        }
    }
    /*=============================================================================
     BINARY FRAME SERIOALIZER, HEADER VAILDATION, CALCULATE FRAME FOOT PRINT
    ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
    HEADER_SIZE_BYTES = 64; // Int32 Array Indices (4 bytes per element)
    HEADER_IDX_WRITE_HEAD = 0; // Byte offset 0
    HEADER_IDX_READ_HEAD = 1; // Byte offset 4
    HEADER_IDX_MUTEX = 2; // Byte offset 8
    HEADER_IDX_CAPACITY = 3; // Byte offset 12
    MUTEX_UNLOCKED = 0; // Mutex States
    MUTEX_LOCKED = 1;
    SENTINEL_WRAP_MARKER = 0xffffffff; // 0xFFFFFFFF (Uint32 max) written to length prefix slot when p wraps to offset 0
    static topicToIdMap = new Map();
    static idToTopicMap = new Map();
    static headerBuffer = new ArrayBuffer(4); // Reusable scratch buffer & DataView for header manipulations (Zero GC)
    static headerView = new DataView(BINARY_CODEC.headerBuffer);
    static headerBytes = new Uint8Array(BINARY_CODEC.headerBuffer);
    FLYWEIGHT_FRAME() {
        return {
            topicId: 0,
            topicName: undefined,
            p: new Uint8Array(0),
        };
    }
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
    static encodeFrame(topic, p) {
        const topicId = BINARY_CODEC.getTopicId(topic);
        let payloadBytes;
        if (p instanceof Uint8Array) {
            payloadBytes = p;
        }
        else if (typeof p === "string") {
            payloadBytes = encoder.encode(p);
        }
        else {
            payloadBytes = encoder.encode(JSON.stringify(p));
        }
        const frame = new Uint8Array(4 + payloadBytes.byteLength);
        // 1. Write Uint32 Topic ID (Little-Endian) via byte copies (Zero DataView allocation)
        this.headerView.setUint32(0, topicId, true);
        frame.set(this.headerBytes, 0);
        // 2. Copy Payload Bytes
        frame.set(payloadBytes, 4);
        return frame;
    }
    decodeFrame(frameBytes) {
        if (frameBytes.byteLength < 4) {
            throw new Error("Invalid frame: buffer smaller than minimum header size (4 bytes).");
        }
        // Direct byte copy into static DataView to read Uint32 safely without memory allocation
        BINARY_CODEC.headerBytes[0] = frameBytes[0];
        BINARY_CODEC.headerBytes[1] = frameBytes[1];
        BINARY_CODEC.headerBytes[2] = frameBytes[2];
        BINARY_CODEC.headerBytes[3] = frameBytes[3];
        const topicId = BINARY_CODEC.headerView.getUint32(0, true);
        this.FLYWEIGHT_FRAME().topicId = topicId;
        this.FLYWEIGHT_FRAME().topicName = BINARY_CODEC.getTopicName(topicId);
        this.FLYWEIGHT_FRAME().p = frameBytes.subarray(4); // Zero-copy view
        return this.FLYWEIGHT_FRAME();
    }
    createSharedDepotBuffer(capacityBytes) {
        // Enforce 64-byte alignment on capacity
        const alignedCapacity = Math.ceil(capacityBytes / 64) * 64;
        const totalSizeBytes = this.HEADER_SIZE_BYTES + alignedCapacity;
        const sab = new SharedArrayBuffer(totalSizeBytes);
        const headerView = new Int32Array(sab, 0, 4);
        // Initialize Header State
        Atomics.store(headerView, this.HEADER_IDX_WRITE_HEAD, 0);
        Atomics.store(headerView, this.HEADER_IDX_READ_HEAD, 0);
        Atomics.store(headerView, this.HEADER_IDX_MUTEX, this.MUTEX_UNLOCKED);
        Atomics.store(headerView, this.HEADER_IDX_CAPACITY, alignedCapacity);
        return sab;
    }
    mapDepotBuffer(sab) {
        // Map control slots (first 16 bytes containing 4 x Int32 values)
        const headerInt32 = new Int32Array(sab, 0, 4);
        // Read capacity directly from header slot
        const capacity = Atomics.load(headerInt32, this.HEADER_IDX_CAPACITY);
        // Map p slice strictly starting at byte 64
        const payloadUint8 = new Uint8Array(sab, this.HEADER_SIZE_BYTES, capacity);
        return {
            headerInt32,
            payloadUint8,
            capacity,
        };
    }
    validateHeader(sab) {
        if (sab.byteLength < this.HEADER_SIZE_BYTES)
            return false;
        const headerInt32 = new Int32Array(sab, 0, 4);
        const capacity = Atomics.load(headerInt32, this.HEADER_IDX_CAPACITY);
        // Buffer length must equal 64 bytes header + registered capacity
        return sab.byteLength === this.HEADER_SIZE_BYTES + capacity;
    }
    calculateFrameFootprint(payloadSizeBytes) {
        const rawEnvelopeBytes = 4 + payloadSizeBytes;
        return Math.ceil(rawEnvelopeBytes / 4) * 4;
    }
}
