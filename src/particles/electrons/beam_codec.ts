// electrons/beam_codec.ts

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

export class BEAM_CODEC_ELECTRON {
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

    constructor(initialCapacity = 1 << 16) {
        this.bytes = new Uint8Array(initialCapacity);
        this.view = new DataView(this.bytes.buffer);
    }
    encode(value: U): U8 {
        this.tail = 0;
        this.strings.clear();
        this.stringBytes.length = 0;
        this.refs.clear();
        this.nextRef = 0;

        this.writeValue(value);

        return this.bytes.subarray(0, this.tail);
    }
    /* HC_BINARY_CODEC */
    decode(binary: U8): U {
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
}
