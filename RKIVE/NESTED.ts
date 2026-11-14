
const OP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	COMPACT: 4,
	NONE: 5,
    EDIT: 6,
	COMPUTED:7,
	EFFECTSCOPE: 8,
	EXTERNAL: 9,
	OBSERVER: 10,
	ASYNCEFFECT: 11,
	SEND: 12,
	SUB: 13,
	DUPLEX: 14,
	STREAM: 15,
	DRC: 16,
} as const;
const HOTS = {
    SAURON: 0,
    SIGNALS: 1,
    THE_CLOUD: 2,
    BUS: 3,
    HOOKS: 4,
    ATOMS: 5,
    DRC: 16,
} as const;
type AVROField = { name: S; type: AVROSchema };
type AVROSchema = S | AVROSchema[] | {
		type: S | AVROSchema[];
		name?: S;
		namespace?: S;
		fields?: AVROField[];
		items?: AVROSchema;
		values?: AVROSchema;
		symbols?: AR<S>;
		size?: N;
};
type AVROValue = any;
class AVROWriter {
		private buf = new Uint8Array(256);
		private pos = 0;
		private ensure=(length: N): V =>{
			if (this.pos + length <= this.buf.length) return;
			let size = this.buf.length;
			while (size < this.pos + length) size *= 2;
			const next = new Uint8Array(size);
			next.set(this.buf);
			this.buf = next;
		}
		get offset(): number {
			return this.pos;
		}
		byte =(v: N): V =>{
			this.ensure(1);
			this.buf[this.pos++] = v;
		}
		bytes=(v: Uint8Array): V=> {
			this.ensure(v.length);
			this.buf.set(v, this.pos);
			this.pos += v.length;
		}
		long=(value: number | bigint): V =>{
			let n = BigInt(value);
			if (n < -(1n << 63n) || n > (1n << 63n) - 1n) throw new Error("Avro: long out of range");
			n = n >= 0n ? n << 1n : ((-n) << 1n) - 1n;
			while (n >= 0x80n) {
				this.byte(Number(n & 0x7fn) | 0x80);
				n >>= 7n;
			}
			this.byte(Number(n));
		}
		int=(value: N): V=> {
			if (!Number.isInteger(value) || value < -2147483648 || value > 2147483647)
				throw new Error("Avro: int out of range");
			this.long(value);
		}
		float=(value: N): V=> {
			this.ensure(4);
			new DataView(this.buf.buffer).setFloat32(this.pos, value, true);
			this.pos += 4;
		}
		double=(value: N): V =>{
			this.ensure(8);
			new DataView(this.buf.buffer).setFloat64(this.pos, value, true);
			this.pos += 8;
		}
		finish = (): Uint8Array =>{
			return this.buf.slice(0, this.pos);
		}
}
class AVROReader {
		private pos = 0;
		private readonly view: DataView;

		constructor(private readonly buf: Uint8Array) {
			this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
		}

		private ensure(length: N): V {
			if (length < 0 || this.pos + length > this.buf.length)
				throw new Error("Avro: truncated buffer");
		}

		byte(): N {
			this.ensure(1);
			return this.buf[this.pos++];
		}

		bytes(length: N): Uint8Array {
			this.ensure(length);
			const out = this.buf.slice(this.pos, this.pos + length);
			this.pos += length;
			return out;
		}

		long(): number | bigint {
			let n = 0n;
			let shift = 0n;

			for (let i = 0; i < 10; i++) {
				const b = this.byte();
				n |= BigInt(b & 0x7f) << shift;
				if ((b & 0x80) === 0) {
					const value = (n & 1n) === 0n ? n >> 1n : -((n >> 1n) + 1n);
					return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
						? Number(value)
						: value;
				}
				shift += 7n;
			}

			throw new Error("Avro: invalid long");
		}

		int(): N {
			const n = this.long();
			if (typeof n === "bigint" || n < -2147483648 || n > 2147483647)
				throw new Error("Avro: invalid int");
			return n;
		}

		float(): N {
			this.ensure(4);
			const n = this.view.getFloat32(this.pos, true);
			this.pos += 4;
			return n;
		}

		double(): N {
			this.ensure(8);
			const n = this.view.getFloat64(this.pos, true);
			this.pos += 8;
			return n;
		}

		done(): B { return this.pos === this.buf.length; }
}
const AVRO_ERROR_SCHEMA: AVROSchema = {
		type: "record",
		name: "AVROError",
		fields: [
			{ name: "type", type: "string" },
			{ name: "error", type: "string" },
			{ name: "id", type: "string" },
		],
};
class BINARY_AVRO {
		private readonly encoder = new TextEncoder();
		private readonly decoder = new TextDecoder();
		private readonly schema: AVROSchema;
		private readonly names: OB<AVROSchema> = Object.create(null);
		private readonly errorSchema = AVRO_ERROR_SCHEMA;
		private readonly wireSchema: AVROSchema[];
		constructor(schema: AVROSchema) {
			if (Array.isArray(schema)) throw new Error("Avro: root schema cannot be a union");
			this.schema = schema;
			this.wireSchema = [schema, this.errorSchema];
			this.#collect(schema);
			this.#collect(this.errorSchema);
		}
		 #collect(schema: AVROSchema): V {
			if (Array.isArray(schema)) {
				for (const branch of schema) this.#collect(branch);
				return;
			}
			if (typeof schema === "string") return;

			if (schema.name) this.names[schema.name] = schema;

			if (Array.isArray(schema.type)) this.#collect(schema.type);
			else if (typeof schema.type === "object") this.#collect(schema.type);
			else if (typeof schema.type === "string" && ![
				"null", "boolean", "int", "long", "float", "double",
				"bytes", "string", "record", "array", "map", "enum", "fixed",
			].includes(schema.type)) {
				if (!this.names[schema.type]) throw new Error(`Avro: unknown named type ${schema.type}`);
			}

			if (schema.fields) for (const field of schema.fields) this.#collect(field.type);
			if (schema.items) this.#collect(schema.items);
			if (schema.values) this.#collect(schema.values);
		}
		 #resolve(schema: AVROSchema): AVROSchema {
			if (typeof schema === "string" && this.names[schema]) return this.names[schema];
			return schema;
		}
		 #kind(schema: AVROSchema): S {
			schema = this.#resolve(schema);
			if (typeof schema === "string") return schema;
			if (Array.isArray(schema)) return "union";
			return typeof schema.type === "string" ? schema.type : this.#kind(schema.type);
		}
		 #matches(schema: AVROSchema, value: AVROValue): B {
			schema = this.#resolve(schema);
			const kind = this.#kind(schema);

			switch (kind) {
				case "null": return value === null || value === undefined;
				case "boolean": return typeof value === "boolean";
				case "int": return typeof value === "number" && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647;
				case "long": return typeof value === "bigint" || (typeof value === "number" && Number.isSafeInteger(value));
				case "float":
				case "double": return typeof value === "number";
				case "string": return typeof value === "string";
				case "bytes":
				case "fixed": return value instanceof Uint8Array && (kind !== "fixed" || value.length === (schema as any).size);
				case "array": return Array.isArray(value);
				case "map": return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array);
				case "record": return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array);
				case "enum": return typeof value === "string" && (schema as any).symbols.includes(value);
				default: return false;
			}
		}
		 #write(w: AVROWriter, schema: AVROSchema, value: AVROValue): V {
			schema = this.#resolve(schema);

			if (Array.isArray(schema)) {
				const index = schema.findIndex(branch => this.#matches(branch, value));
				if (index === -1) throw new Error("Avro: no matching union branch");
				w.long(index);
				this.#write(w, schema[index], value);
				return;
			}

			if (typeof schema === "string") {
				switch (schema) {
					case "null": return;
					case "boolean": w.byte(value ? 1 : 0); return;
					case "int": w.int(value); return;
					case "long": w.long(value); return;
					case "float": w.float(value); return;
					case "double": w.double(value); return;
					case "string": {
						const bytes = this.encoder.encode(value);
						w.long(bytes.length);
						w.bytes(bytes);
						return;
					}
					case "bytes": {
						w.long(value.length);
						w.bytes(value);
						return;
					}
					default: throw new Error(`Avro: unknown type ${schema}`);
				}
			}

			if (typeof schema.type !== "string") {
				this.#write(w, schema.type, value);
				return;
			}

			switch (schema.type) {
				case "null": return;
				case "boolean":
					w.byte(value ? 1 : 0);
					return;
				case "int":
					w.int(value);
					return;
				case "long":
					w.long(value);
					return;
				case "float":
					w.float(value);
					return;
				case "double":
					w.double(value);
					return;
				case "string": {
					const bytes = this.encoder.encode(value);
					w.long(bytes.length);
					w.bytes(bytes);
					return;
				}
				case "bytes":
					w.long(value.length);
					w.bytes(value);
					return;
				case "fixed":
					if (!(value instanceof Uint8Array) || value.length !== schema.size)
						throw new Error("Avro: invalid fixed value");
					w.bytes(value);
					return;
				case "enum": {
					const index = schema.symbols!.indexOf(value);
					if (index < 0) throw new Error(`Avro: invalid enum symbol ${value}`);
					w.long(index);
					return;
				}
				case "record":
					for (const field of schema.fields!) {
						if (value[field.name] === undefined && !Array.isArray(field.type))
							throw new Error(`Avro: missing field ${field.name}`);
						this.#write(w, field.type, value[field.name]);
					}
					return;
				case "array":
					if (value.length) {
						w.long(value.length);
						for (const item of value) this.#write(w, schema.items!, item);
					}
					w.long(0);
					return;
				case "map": {
					const keys = Object.keys(value);
					if (keys.length) {
						w.long(keys.length);
						for (const key of keys) {
							this.#write(w, "string", key);
							this.#write(w, schema.values!, value[key]);
						}
					}
					w.long(0);
					return;
				}
				default:
					throw new Error(`Avro: unsupported schema type ${schema.type}`);
			}
		}
		 #read(r: AVROReader, schema: AVROSchema): AVROValue {
			schema = this.#resolve(schema);

			if (Array.isArray(schema)) {
				const index = r.long();
				if (typeof index !== "number" || index < 0 || index >= schema.length)
					throw new Error("Avro: invalid union index");
				return this.#read(r, schema[index]);
			}

			if (typeof schema === "string") {
				switch (schema) {
					case "null": return null;
					case "boolean": {
						const b = r.byte();
						if (b > 1) throw new Error("Avro: invalid boolean");
						return b === 1;
					}
					case "int": return r.int();
					case "long": return r.long();
					case "float": return r.float();
					case "double": return r.double();
					case "string": return this.decoder.decode(r.bytes(Number(r.long())));
					case "bytes": return r.bytes(Number(r.long()));
					default: throw new Error(`Avro: unknown type ${schema}`);
				}
			}

			if (typeof schema.type !== "string") return this.#read(r, schema.type);

			switch (schema.type) {
				case "null": return null;
				case "boolean": {
					const b = r.byte();
					if (b > 1) throw new Error("Avro: invalid boolean");
					return b === 1;
				}
				case "int": return r.int();
				case "long": return r.long();
				case "float": return r.float();
				case "double": return r.double();
				case "string": return this.decoder.decode(r.bytes(Number(r.long())));
				case "bytes": return r.bytes(Number(r.long()));
				case "fixed": return r.bytes(schema.size!);
				case "enum": {
					const index = r.long();
					if (typeof index !== "number" || index < 0 || index >= schema.symbols!.length)
						throw new Error("Avro: invalid enum index");
					return schema.symbols![index];
				}
				case "record": {
					const out: OB<AVROValue> = {};
					for (const field of schema.fields!) out[field.name] = this.#read(r, field.type);
					return out;
				}
				case "array": {
					const out: AVROValue[] = [];
					let count: number | bigint;
					while ((count = r.long()) !== 0) {
						if (typeof count === "bigint") throw new Error("Avro: array block too large");
						if (count < 0) {
							count = -count;
							const blockSize = r.long();
							if (typeof blockSize !== "number" || blockSize < 0) throw new Error("Avro: invalid array block");
						}
						for (let i = 0; i < count; i++) out.push(this.#read(r, schema.items!));
					}
					return out;
				}
				case "map": {
					const out: OB<AVROValue> = {};
					let count: number | bigint;
					while ((count = r.long()) !== 0) {
						if (typeof count === "bigint") throw new Error("Avro: map block too large");
						if (count < 0) {
							count = -count;
							const blockSize = r.long();
							if (typeof blockSize !== "number" || blockSize < 0) throw new Error("Avro: invalid map block");
						}
						for (let i = 0; i < count; i++) {
							const key = this.#read(r, "string");
							out[key] = this.#read(r, schema.values!);
						}
					}
					return out;
				}
				default:
					throw new Error(`Avro: unsupported schema type ${schema.type}`);
			}
		}
		 #encode(value: AVROValue, branch: N): Uint8Array {
			const w = new AVROWriter();
			w.long(branch);
			this.#write(w, this.wireSchema[branch], value);
			return w.finish();
		}
		 #decode(bytes: Uint8Array): AVROValue {
			const r = new AVROReader(bytes);
			const branch = r.long();
			if (typeof branch !== "number" || branch < 0 || branch >= this.wireSchema.length)
				throw new Error("Avro: invalid envelope branch");
			const value = this.#read(r, this.wireSchema[branch]);
			if (!r.done()) throw new Error("Avro: trailing bytes");
			return value;
		}
		private get recordFields(): AVROField[] {
			const schema = this.#resolve(this.schema);
		
			if (typeof schema === "string" || Array.isArray(schema) || schema.type !== "record")
				throw new Error("Avro: indexed encoding requires a root record");
		
			return schema.fields!;
		}
		public encode_msg(msg: AVROValue): Uint8Array {
			if (msg instanceof Uint8Array) return msg;
			try {
				return this.#encode(msg, 0);
			} catch (err) {
				console.log("[AVRO_PIPE] Failed to encode outbound packet:", err);
				return this.#encode({
					type: "error",
					error: err instanceof Error ? err.message : "Encoding failed",
					id: String(msg?.id ?? "unknown"),
				}, 1);
			}
		}
		public decode_msg(raw: any): AVROValue {
            if (
                !(raw instanceof Uint8Array) &&
                !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))
            ) {
                console.error(
                    "[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream."
                );
        
                return null as any;
            }
        
            try {
                const source: any = raw;
        
                const bytes = source instanceof Uint8Array
                    ? source
                    : new Uint8Array(
                        source.buffer,
                        source.byteOffset,
                        source.byteLength
                    );
        
                return this.#decode(bytes);
            } catch (err) {
                console.error(
                    "[AVRO_PIPE] Failed to decode inbound packet:",
                    err
                );
        
                return null as any;
            }
        }
		private readonly INDEX_MAGIC = 0x41564958; // AVIX
		public encode_indexed(msg: AVROValue): Uint8Array {
			if (this.#kind(this.schema) !== "record")
				throw new Error("Avro: indexed encoding requires a root record");

			const fields = this.recordFields;
			const w = new AVROWriter();

			w.long(0);

			const offsets = new Uint32Array(fields.length * 2);

			for (let i = 0; i < fields.length; i++) {
				const field = fields[i];

				offsets[i * 2] = w.offset;
				this.#write(w, field.type, msg[field.name]);
				offsets[i * 2 + 1] = w.offset;
			}

			const body = w.finish();
			const headerSize = 8 + offsets.length * 4;
			const output = new Uint8Array(headerSize + body.length);
			const view = new DataView(output.buffer);

			view.setUint32(0, this.INDEX_MAGIC, true);
			view.setUint32(4, fields.length, true);

			for (let i = 0; i < offsets.length; i++)
				view.setUint32(8 + i * 4, offsets[i] + headerSize, true);

			output.set(body, headerSize);

			return output;
		}
		public decode_indexed_field(raw: Uint8Array, name: string): AVROValue {
			if (this.#kind(this.schema) !== "record")
				throw new Error("Avro: indexed decoding requires a root record");

			const fields = this.recordFields;
			const index = fields.findIndex(field => field.name === name);

			if (index < 0)
				throw new Error(`Avro: unknown field ${name}`);

			if (raw.length < 8)
				throw new Error("Avro: truncated indexed buffer");

			const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

			if (view.getUint32(0, true) !== this.INDEX_MAGIC)
				throw new Error("Avro: invalid indexed buffer");

			if (view.getUint32(4, true) !== fields.length)
				throw new Error("Avro: indexed schema mismatch");

			const headerSize = 8 + fields.length * 8;

			if (raw.length < headerSize)
				throw new Error("Avro: truncated index");

			const start = view.getUint32(8 + index * 8, true);
			const end = view.getUint32(12 + index * 8, true);

			if (start < headerSize || end < start || end > raw.length)
				throw new Error("Avro: invalid field offsets");

			const bytes = raw.subarray(start, end);
			const reader = new AVROReader(bytes);
			const value = this.#read(reader, fields[index].type);

			if (!reader.done())
				throw new Error("Avro: field length mismatch");

			return value;
		}
		public decode_indexed_msg(raw: Uint8Array): AVROValue {
			if (raw.length < 8)
				throw new Error("Avro: truncated indexed buffer");

			const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

			if (view.getUint32(0, true) !== this.INDEX_MAGIC)
				throw new Error("Avro: invalid indexed buffer");

			const count = view.getUint32(4, true);
			const headerSize = 8 + count * 8;

			if (count !== this.recordFields?.length || raw.length < headerSize)
				throw new Error("Avro: indexed schema mismatch");

			return this.#decode(raw.subarray(headerSize));
		}
}
class AVRO_RING {
    public readonly capacity: number;
    public readonly mask: number;

    // The actual disruptor storage: encoded Avro payloads.
    public  bytes: Uint8Array;

    // Ring descriptor lanes.
    public readonly offsets: Int32Array;
    public readonly lengths: Int32Array;
    public readonly ops: Uint8Array;

    // Ring cursors.
    public writeCursor = 0;
    public readCursor = 0;

    // Key -> ring sequence.
    public readonly keyRings = new Map<any, number>();

    private nextSequence = 0;

    constructor(capacity = 65536) {
        if ((capacity & (capacity - 1)) !== 0) {
            throw new Error("AVRO_RING capacity must be a power of two.");
        }

        this.capacity = capacity;
        this.mask = capacity - 1;

        this.bytes = new Uint8Array(capacity * 16);

        this.offsets = new Int32Array(capacity);
        this.lengths = new Int32Array(capacity);
        this.ops = new Uint8Array(capacity);
    }

    /* ------------------------------------------------------------------
       Publish encoded payload into the ring.
       ------------------------------------------------------------------ */

    public publish(
        key: unknown,
        payload: Uint8Array
    ): number {
        const sequence = this.nextSequence++;
        const ringIndex = sequence & this.mask;

        // Ensure the byte ring can hold this payload.
        this.ensureBytes(payload.length);

        const offset = this.writeCursor;

        this.bytes.set(payload, offset);

        this.offsets[ringIndex] = offset;
        this.lengths[ringIndex] = payload.length;
        this.ops[ringIndex] = OP.WRITE;

        this.writeCursor = (offset + payload.length) % this.bytes.length;

        // Remember which ring sequence holds this key.
        this.keyRings.set(key, sequence);

        return sequence;
    }

    /* ------------------------------------------------------------------
       Read a published payload by key.
       ------------------------------------------------------------------ */

    public read(key: unknown): Uint8Array | undefined {
        const sequence = this.keyRings.get(key);

        if (sequence === undefined) {
            return undefined;
        }

        const ringIndex = sequence & this.mask;

        if (this.ops[ringIndex] === OP.DELETE) {
            return undefined;
        }

        const offset = this.offsets[ringIndex];
        const length = this.lengths[ringIndex];

        return this.bytes.subarray(offset, offset + length);
    }

    /* ------------------------------------------------------------------
       Consume in ring order.
       ------------------------------------------------------------------ */

    public consume(
        handler: (key: unknown, payload: Uint8Array) => void
    ): number {
        const available = this.nextSequence - this.readCursor;

        if (available <= 0) {
            return 0;
        }

        let processed = 0;

        while (this.readCursor < this.nextSequence) {
            const sequence = this.readCursor;
            const ringIndex = sequence & this.mask;

            if (this.ops[ringIndex] !== OP.DELETE) {
                const offset = this.offsets[ringIndex];
                const length = this.lengths[ringIndex];

                const payload = this.bytes.subarray(offset, offset + length);

                // Find the key for this sequence.
                for (const [key, seq] of this.keyRings) {
                    if (seq === sequence) {
                        handler(key, payload);
                        break;
                    }
                }
            }

            this.readCursor++;
            processed++;
        }

        return processed;
    }

    /* ------------------------------------------------------------------
       Delete
       ------------------------------------------------------------------ */

    public remove(key: unknown): boolean {
        const sequence = this.keyRings.get(key);

        if (sequence === undefined) {
            return false;
        }

        const ringIndex = sequence & this.mask;

        if (this.ops[ringIndex] === OP.DELETE) {
            return false;
        }

        this.ops[ringIndex] = OP.DELETE;

        return true;
    }

    /* ------------------------------------------------------------------
       Byte ring growth
       ------------------------------------------------------------------ */

    private ensureBytes(required: number): void {
        if (this.writeCursor + required <= this.bytes.length) {
            return;
        }

        let size = this.bytes.length;

        while (size < this.writeCursor + required) {
            size <<= 1;
        }

        const next = new Uint8Array(size);
        next.set(this.bytes);

        this.bytes = next;
    }
}
class AVRO_DISRUPTOR {
    private readonly rings: Array<AVRO_RING>;
    private readonly avro: BINARY_AVRO;

    constructor(
        avro: BINARY_AVRO,
        laneCount = 32,
        capacity = 65536
    ) {
        this.avro = avro;

        this.rings = new Array<AVRO_RING>(laneCount);

        for (let i = 0; i < laneCount; i++) {
            this.rings[i] = new AVRO_RING(capacity);
        }
    }

    /* ------------------------------------------------------------------
       Write path
       ------------------------------------------------------------------ */

    public set(
        hots: number,
        key: unknown,
        value: AVROValue
    ): void {
        const ring = this.rings[hots];

        const encoded = this.avro.encode_indexed(value);

        ring.publish(key, encoded);
    }

    /* ------------------------------------------------------------------
       Read path
       ------------------------------------------------------------------ */

    public get(
        hots: number,
        key: unknown
    ): AVROValue | undefined {
        const ring = this.rings[hots];

        const encoded = ring.read(key);

        if (encoded === undefined) {
            return undefined;
        }

        return this.avro.decode_indexed_msg(encoded);
    }

    public has(hots: number, key: unknown): boolean {
        const ring = this.rings[hots];

        return ring.read(key) !== undefined;
    }

    public delete(hots: number, key: unknown): boolean {
        const ring = this.rings[hots];

        return ring.remove(key);
    }

    public ring(hots: number): AVRO_RING {
        return this.rings[hots];
    }
}
class SimpleMapEngine {
    public MODULE_CACHE = new Map<any, any>();

    public r = "THE_WIZARD_ROOT_99";
    public m = "SAURON_MASTER_CORE_01";
    public s = "SAURON_SLAVE_ENGINE_01";

    public _simple_map( s: any = this.s, m: any = this.m, r: any = this.r ): M2<any, any> {
        let root = this.MODULE_CACHE.get(r);

        if (!root) {
            root = new M2<any, M2<any, M2<any, any>>>();
            this.MODULE_CACHE.set(r, root);
        }

        let masterMap = root.get(m);

        if (!masterMap) {
            masterMap = new M2<any, M2<any, any>>();
            root.set(m, masterMap);
        }

        let slaveMap = masterMap.get(s);

        if (!slaveMap) {
            slaveMap = new M2<any, unknown>();
            masterMap.set(s, slaveMap);
        }

        return slaveMap;
    }
}

class RAW_RING {
    public readonly capacity: number;
    public readonly mask: number;

    public readonly pids: Int32Array;
    public readonly values: Float64Array;
    public readonly flags: Uint8Array;

    public writeCursor = 0;
    public readCursor = 0;

    constructor(capacity = 65536) {
        if ((capacity & (capacity - 1)) !== 0) {
            throw new Error(
                "Raw ring capacity must be a power of two."
            );
        }

        this.capacity = capacity;
        this.mask = capacity - 1;

        this.pids = new Int32Array(capacity);
        this.values = new Float64Array(capacity);
        this.flags = new Uint8Array(capacity);
    }

    public publish(
        pid: number,
        value: number,
        flag = 0
    ): boolean {
        if (
            this.writeCursor - this.readCursor >=
            this.capacity
        ) {
            return false;
        }

        const index = this.writeCursor & this.mask;

        this.pids[index] = pid;
        this.values[index] = value;
        this.flags[index] = flag;

        this.writeCursor++;

        return true;
    }
}



const ITERATIONS = 1_000_000;
const PAYLOAD_SCHEMA: AVROSchema = {
    type: "record",
    name: "SignalPayload",
    fields: [
        {
            name: "active",
            type: "boolean",
        },
        {
            name: "state",
            type: "string",
        },
        {
            name: "payload",
            type: "string",
        },
        {
            name: "timestamp",
            type: "long",
        },
    ],
};


import { performance } from "node:perf_hooks";

const avro = new BINARY_AVRO(PAYLOAD_SCHEMA);

const disruptor = new AVRO_DISRUPTOR(avro);
const mapEngine = new SimpleMapEngine();
const rawRing = new RAW_RING(65536);

const key = "signal_node_target_key";

const value = {
    active: true,
    state: "RUNNING",
    payload: "data_packet_alpha_omega_99",
    timestamp: Date.now(),
};

// Warm up / register.
disruptor.set(HOTS.SAURON, key, value);

const hotMap = mapEngine._simple_map(
    "SAURON",
    mapEngine.m,
    mapEngine.r
);

hotMap.set(key, value);

const encodedPayload = avro.encode_indexed(value);

const hotRing = disruptor.ring(HOTS.SAURON);

function report(
    name: string,
    elapsed: number,
    hits = ITERATIONS
): void {
    const opsPerSec = ITERATIONS / (elapsed / 1000);

    console.log(
        `${name.padEnd(38)} ` +
        `${elapsed.toFixed(2).padStart(10)} ms   ` +
        `${Math.round(opsPerSec).toLocaleString().padStart(15)} ops/sec   ` +
        `hits: ${hits.toLocaleString()}`
    );
}

console.log("=== AVRO DISRUPTOR BENCHMARK ===");
console.log(`Operations per test: ${ITERATIONS.toLocaleString()}\n`);

/* --------------------------------------------------------------------------
   1. Full JS boundary write
   -------------------------------------------------------------------------- */

{
    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        disruptor.set(
            HOTS.SAURON,
            key,
            value
        );
    }

    const elapsed = performance.now() - start;

    report(
        "disruptor.set(): JS + Avro + ring",
        elapsed
    );
}

/* --------------------------------------------------------------------------
   2. Full JS boundary read
   -------------------------------------------------------------------------- */

{
    let hits = 0;

    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        const result = disruptor.get(
            HOTS.SAURON,
            key
        );

        if (result !== undefined) {
            hits++;
        }
    }

    const elapsed = performance.now() - start;

    report(
        "disruptor.get(): ring + Avro decode",
        elapsed,
        hits
    );
}

/* --------------------------------------------------------------------------
   3. Pre-encoded disruptor write
   -------------------------------------------------------------------------- */

{
    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        hotRing.publish(
            key,
            encodedPayload
        );
    }

    const elapsed = performance.now() - start;

    report(
        "ring.publish(): pre-encoded payload",
        elapsed
    );
}

/* --------------------------------------------------------------------------
   4. Pre-encoded ring read
   -------------------------------------------------------------------------- */

{
    let hits = 0;

    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        const encoded = hotRing.read(key);

        if (encoded !== undefined) {
            hits++;
        }
    }

    const elapsed = performance.now() - start;

    report(
        "ring.read(): pre-encoded payload",
        elapsed,
        hits
    );
}

/* --------------------------------------------------------------------------
   5. Nested Map write
   -------------------------------------------------------------------------- */

{
    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        hotMap.set(key, value);
    }

    const elapsed = performance.now() - start;

    report(
        "_simple_map().set(): nested Map",
        elapsed
    );
}

/* --------------------------------------------------------------------------
   6. Nested Map read
   -------------------------------------------------------------------------- */

{
    let hits = 0;

    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        const result = hotMap.get(key);

        if (result !== undefined) {
            hits++;
        }
    }

    const elapsed = performance.now() - start;

    report(
        "_simple_map().get(): nested Map",
        elapsed,
        hits
    );
}

/* --------------------------------------------------------------------------
   7. Raw ring publish ceiling
   -------------------------------------------------------------------------- */

{
    const start = performance.now();

    for (let i = 0; i < ITERATIONS; i++) {
        rawRing.publish(
            1,
            123.456,
            OP.WRITE
        );
    }

    const elapsed = performance.now() - start;

    report(
        "raw ring publish(): ceiling",
        elapsed
    );
}
