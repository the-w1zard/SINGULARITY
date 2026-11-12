// electrons/binary.ts
export type CBORValue = undefined | null | boolean | number | string | Uint8Array | CBORValue[] | { [k: string]: CBORValue };
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
} as const;
const encoder = new TextEncoder();
const decoder = new TextDecoder();
export class BINARY_CODEC {

	encodeJSONEnvelope(envelope: any): Uint8Array {
		return new TextEncoder().encode(JSON.stringify(envelope));
	}
	decodeJSONEnvelope(bytes: Uint8Array): any {
		return JSON.parse(new TextDecoder().decode(bytes));
	}
	 
        /* =============================================================================
         Encodes ordinary JavaScript values into a collider-owned byte slab.
         ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
        private bytes: U8;
        private view: DataView;
        private tail = 0;
    
        private strings = new Map<S, N>();
        private stringBytes: U8[] = [];
        private refs = new Map<object, N>();
        private nextRef = 0;
        encode_js(value: U): U8 {
            this.tail = 0;
            this.strings.clear();
            this.stringBytes.length = 0;
            this.refs.clear();
            this.nextRef = 0;
    
            this.writeValue(value);
    
            return this.bytes.subarray(0, this.tail);
        }
        /* HC_BINARY_CODEC */
        decode_js(binary: U8): U {
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
            } finally {
                // Restore the encode-side buffer so the next encode()
                // never writes into the disruptor pool.
                this.bytes = savedBytes;
                this.view = savedView;
                this.tail = savedTail;
            }
        }
        private need(count: N): V {
            if (this.tail + count <= this.bytes.length) return;
    
            let size = this.bytes.length;
    
            while (size < this.tail + count) size *= 2;
    
            const next = new Uint8Array(size);
            next.set(this.bytes);
    
            this.bytes = next;
            this.view = new DataView(next.buffer);
        }
        private u8(value: N): V {
            this.need(1);
            this.bytes[this.tail++] = value;
        }
        private i32(value: N): V {
            this.need(4);
            this.view.setInt32(this.tail, value, true);
            this.tail += 4;
        }
        private f64(value: N): V {
            this.need(8);
            this.view.setFloat64(this.tail, value, true);
            this.tail += 8;
        }
        private writeValue(value: U): V {
            if (value === undefined) return this.u8(TAG.UNDEFINED);
            if (value === null) return this.u8(TAG.NULL);
            if (value === false) return this.u8(TAG.FALSE);
            if (value === true) return this.u8(TAG.TRUE);
            const type = typeof value;
            if (type === "number") {
                const n = value as N;
                if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
                    this.u8(TAG.INT32);
                    return this.i32(n);
                }
    
                this.u8(TAG.FLOAT64);
                return this.f64(n);
            }
    
            if (type === "string") {
                return this.writeString(value as S);
            }
    
            if (Array.isArray(value)) {
                return this.writeArray(value);
            }
    
            if (value instanceof Uint8Array) {
                return this.writeBytes(value);
            }
    
            if (type === "object") {
                return this.writeObject(value as object);
            }
    
            throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
        }
        private writeString(value: S): V {
            const encoded = new TextEncoder().encode(value);
    
            this.u8(TAG.STRING);
            this.i32(encoded.length);
            this.need(encoded.length);
            this.bytes.set(encoded, this.tail);
            this.tail += encoded.length;
        }
        private writeArray(value: U[]): V {
            const ref = this.beginRef(value);
    
            this.u8(TAG.ARRAY);
            this.i32(value.length);
    
            for (let i = 0; i < value.length; i++) {
                this.writeValue(value[i]);
            }
    
            this.endRef(ref);
        }
        private writeBytes(value: U8): V {
            this.u8(TAG.UINT8ARRAY);
            this.i32(value.length);
            this.need(value.length);
            this.bytes.set(value, this.tail);
            this.tail += value.length;
        }
        private writeObject(value: object): V {
            const ref = this.beginRef(value);
            const keys = Object.keys(value);
    
            this.u8(TAG.OBJECT);
            this.i32(keys.length);
    
            for (const key of keys) {
                this.writeString(key);
                this.writeValue((value as any)[key]);
            }
    
            this.endRef(ref);
        }
        private beginRef(value: object): N {
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
        private endRef(ref: N): V {
            // Reserved for future back-reference patching.
            void ref;
        }
        private readU8(cursor: { at: N }): N {
            return this.bytes[cursor.at++];
        }
        private readI32(cursor: { at: N }): N {
            const value = this.view.getInt32(cursor.at, true);
            cursor.at += 4;
    
            return value;
        }
        private readF64(cursor: { at: N }): N {
            const value = this.view.getFloat64(cursor.at, true);
            cursor.at += 8;
    
            return value;
        }
        private readValue(cursor: { at: N }): U {
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
        private readString(cursor: { at: N }): S {
            const length = this.readI32(cursor);
            const bytes = this.bytes.subarray(cursor.at, cursor.at + length);
    
            cursor.at += length;
    
            return new TextDecoder().decode(bytes);
        }
        private readArray(cursor: { at: N }): U[] {
            const length = this.readI32(cursor);
            const out: U[] = new Array(length);
    
            for (let i = 0; i < length; i++) {
                out[i] = this.readValue(cursor);
            }
    
            return out;
        }
        private readObject(cursor: { at: N }): Record<S, U> {
            const length = this.readI32(cursor);
            const out: Record<S, U> = {};
    
            for (let i = 0; i < length; i++) {
                const key = this.readString(cursor);
                out[key] = this.readValue(cursor);
            }
    
            return out;
        }
        private readBytes(cursor: { at: N }): Uint8Array {
            const length = this.readI32(cursor);
            const out = this.bytes.slice(cursor.at, cursor.at + length);
    
            cursor.at += length;
    
            return out;
        }
	/* =============================================================================
     BINARY ENC, WITH JSON ABOVE
     ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
	private ensureAvailable(buf: Uint8Array, pos: number, length: number): void {
		if (pos < 0 || length < 0 || pos + length > buf.length) throw new Error("CBOR: truncated buffer");
	}
	private readUint(buf: Uint8Array, pos: number): [number, number] {
		this.ensureAvailable(buf, pos, 1);
		const info = buf[pos] & 0x1f;
		if (info <= 23) return [info, pos + 1];
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
			if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER");
			return [Number(value), pos + 9];
		}
		throw new Error(`CBOR: unsupported additional info ${info}`);
	}
	private encodeUint64(majorShifted: number, n: bigint): Uint8Array {
		if (n <= 23n) return new Uint8Array([majorShifted | Number(n)]);
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
	private encode(value: CBORValue): Uint8Array {
		const parts: Uint8Array[] = [];
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
	private encodeValue(v: CBORValue, out: Uint8Array[]): void {
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
				if (v >= 0) out.push(this.encodeUint64(0x00, BigInt(v)));
				else out.push(this.encodeUint64(0x20, BigInt(-1 - v)));
			} else {
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
			if (proto !== Object.prototype && proto !== null) throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`);
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
	private encodeUint(majorShifted: number, n: number): Uint8Array {
		if (n <= 23) return new Uint8Array([majorShifted | n]);
		if (n <= 0xff) return new Uint8Array([majorShifted | 24, n]);
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
	private decode(buf: Uint8Array): CBORValue {
		const [value, end] = this.decodeValue(buf, 0);
		if (end !== buf.length) {
			throw new Error("CBOR: trailing bytes");
		}
		return value;
	}
	private decodeValue(buf: Uint8Array, pos: number): [CBORValue, number] {
		this.ensureAvailable(buf, pos, 1);
		const first = buf[pos];
		const major = first >> 5;
		const info = first & 0x1f;

		if (major === 7) {
			if (info === 20) return [false, pos + 1];
			if (info === 21) return [true, pos + 1];
			if (info === 22) return [null, pos + 1];
			if (info === 23) return [undefined, pos + 1];
			if (info === 27) {
				this.ensureAvailable(buf, pos + 1, 8);
				const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8);
				return [dv.getFloat64(0, false), pos + 9];
			}
		}
		const [n, nextPos] = this.readUint(buf, pos);
		if (major === 0) return [n, nextPos];
		if (major === 1) return [-1 - n, nextPos];
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
			const arr: CBORValue[] = [];
			let p = nextPos;
			for (let i = 0; i < n; i++) {
				const [item, np] = this.decodeValue(buf, p);
				arr.push(item);
				p = np;
			}
			return [arr, p];
		}
		if (major === 5) {
			const obj: Record<string, CBORValue> = {};
			let p = nextPos;
			for (let i = 0; i < n; i++) {
				const [k, kp] = this.decodeValue(buf, p);
				const [v, vp] = this.decodeValue(buf, kp);
				obj[k as string] = v;
				p = vp;
			}
			return [obj, p];
		}
		throw new Error(`CBOR: unsupported major type ${major}`);
	}
	public encode_msg(msg: any): Uint8Array {
		// Instead of leaking a raw object, encode an explicit fallback/error frame
		if (msg instanceof Uint8Array) return msg; // If it's already an encoded Uint8Array, don't re-encode it
		try {
			return this.encode(msg);
		} catch (err) {
			console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err);
			return this.encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" });
		}
	}
	public decode_msg(raw: any): any {
		// Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
		if (!(raw instanceof Uint8Array) && !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))) {
			console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
			return null;
		}
		try {
			const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array((raw as any).buffer, (raw as any).byteOffset, (raw as any).length);
			return this.decode(uint8);
		} catch (err) {
			console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
			return null;
		}
	}
	/*=============================================================================
     BINARY FRAME SERIOALIZER, HEADER VAILDATION, CALCULATE FRAME FOOT PRINT
    ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
	public HEADER_SIZE_BYTES = 64; // Int32 Array Indices (4 bytes per element)
	public HEADER_IDX_WRITE_HEAD = 0; // Byte offset 0
	public HEADER_IDX_READ_HEAD = 1; // Byte offset 4
	public HEADER_IDX_MUTEX = 2; // Byte offset 8
	public HEADER_IDX_CAPACITY = 3; // Byte offset 12
	public MUTEX_UNLOCKED = 0; // Mutex States
	public MUTEX_LOCKED = 1;
	public SENTINEL_WRAP_MARKER = 0xffffffff; // 0xFFFFFFFF (Uint32 max) written to length prefix slot when p wraps to offset 0
	private static topicToIdMap: Map<string, number> = new Map();
	private static idToTopicMap: Map<number, string> = new Map();
	private static headerBuffer = new ArrayBuffer(4); // Reusable scratch buffer & DataView for header manipulations (Zero GC)
	private static headerView = new DataView(BINARY_CODEC.headerBuffer);
	private static headerBytes = new Uint8Array(BINARY_CODEC.headerBuffer);

	public FLYWEIGHT_FRAME(): DecodedFrame{
		return {
			topicId: 0,
			topicName: undefined,
			p: new Uint8Array(0),
		}
	}
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

	public static encodeFrame(topic: string, p: Uint8Array | string | Record<string, unknown>): Uint8Array {
		const topicId = BINARY_CODEC.getTopicId(topic);

		let payloadBytes: Uint8Array;
		if (p instanceof Uint8Array) {
			payloadBytes = p;
		} else if (typeof p === "string") {
			payloadBytes = encoder.encode(p);
		} else {
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
	public   decodeFrame(frameBytes: Uint8Array): DecodedFrame {
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
	public createSharedDepotBuffer(capacityBytes: number): SharedArrayBuffer {
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
	public mapDepotBuffer(sab: SharedArrayBuffer): HeaderViews {
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
	public validateHeader(sab: SharedArrayBuffer): boolean {
		if (sab.byteLength < this.HEADER_SIZE_BYTES) return false;

		const headerInt32 = new Int32Array(sab, 0, 4);
		const capacity = Atomics.load(headerInt32, this.HEADER_IDX_CAPACITY);

		// Buffer length must equal 64 bytes header + registered capacity
		return sab.byteLength === this.HEADER_SIZE_BYTES + capacity;
	}
	public calculateFrameFootprint(payloadSizeBytes: number): number {
		const rawEnvelopeBytes = 4 + payloadSizeBytes;
		return Math.ceil(rawEnvelopeBytes / 4) * 4;
	}
}
