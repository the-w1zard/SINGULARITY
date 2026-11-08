//                                                                                                                         IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import Store from "@the-w1zard/store"
import { BYTE_ID } from "./comps/byte_id"
import { Queue, TIERED_QUEUE } from "./comps/queue"
import { useReducer, useEffect, useState, } from '#sauron'
import { getCurrentComponent, notifyHook, subscribeToHook } from '#the-cloud'

const __sid = new BYTE_ID()
export function _sidi(): S {return __sid.b()}
// #endregion 
//                                                                                                                      REACTIVITY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
interface ReactiveNode {
	deps?: Link;
	depsTail?: Link;
	subs?: Link;
	subsTail?: Link;
	flags: ReactiveFlags;
}

interface Stack<T> {
	value: T;
	prev: Stack<T> | undefined;
}
interface EffectScopeNode extends ReactiveNode {}
interface EffectNode extends ReactiveNode {
	fn(): (() => void) | void;
	cleanup: (() => void) | void;
}
interface ComputedNode<T = any> extends ReactiveNode {
	value: T | undefined;
	getter: (previousValue?: T) => T;
}
interface SignalNode<T = any> extends ReactiveNode {
	currentValue: T;
	pendingValue: T;
}
const queued: (EffectNode | undefined)[] = [];
const TRACER_PID = 0;
const SLAB_THRESHOLD = 8192;
const DEFAULT_SOA_CAPACITY = 1 << 16;
const DEFAULT_SLAB_CAPACITY = 1 << 20;
const DEFAULT_MAX_PIDS = 1 << 20;
type PID = number;
type SLOT = number;
interface HADRON_COLLIDER_OPTIONS {
    soaCapacity?: number;
    slabCapacity?: number;
    maxPids?: number;
    initialByteCapacity?: number;
}
interface ScopeOptions {
    s: string;
    ms_reg: string;
    r: string;
}
interface Link {
    version: number;
    dep: ReactiveNode;
    sub: ReactiveNode;
    prevSub?: Link;
    nextSub?: Link;
    prevDep?: Link;
    nextDep?: Link;
}
interface EffectNode extends ReactiveNode {
    fn: () => void | (() => void);
    cleanup: void | (() => void);
}
interface ComputedNode<T = any> extends ReactiveNode {
    value: T | undefined;
    getter: (previous?: T) => T;
    dirty: boolean;
}
interface SignalNode<T = any> extends ReactiveNode {
    currentValue: T;
    pendingValue: T;
}
const REC = 8;
const F_OP = 0;
const F_PID = 1;
const F_SLOT = 2;
const F_A = 3;
const F_B = 4;
const F_C = 5;
const F_EXT = 7;
const DOMAIN = {
	STATE: 0,
	CONTEXT: 1,
	SIGNALS: 2,
	ATOMS: 3,
	SEND: 4,
	SUB: 5,
	DUPLEX: 6,
	STREAM: 7,
	DRS: 8,
	SPREADSHEET: 9,
	DB: 10,
  } as const;
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
const busOP = {
	SEND: 12,
	SUB: 13,
	DUPLEX: 14,
	STREAM: 15,
} as const;
const spreadsheetOP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	NONE: 4,
    EDIT: 5,
	COMPUTED:6,
} as const;
interface SithMetadataEntry {
    _globalListeners: Set<(key: S | SY, value: A) => V>;
    _waiters: M<S | SY, Set<(value: A) => V>>;
    _history: M<S | SY, A[]>;
    _scopes: M<N, M<A, A>>;
    _listeners: M<N, M<A, Set<(value: A) => V>>>;
    _persistent: M<N, M<A, A>>;
    _persistKeys: M<N, M<A>>;
	_sealed:Set<string>
	_frozen: Set<string>
	_conflictStrategies: Map<string, "latest" | "fail" | "accumulate">
	_conflicts:Map<string, A>
    _batches: Set<S | SY>;
    _batchQueue: M<S | SY, A>;
    _batchOpts: M<S | SY, { triggerRender?: B }>;
    _global_subscribers: M<S, Set<() => V>>;
    _keyContracts: M<S, Set<S>>;
    _sys: M<S, A>;
    _causal: M<S, A[]>;
    _msgHandlers: M<S, Set<(p: A) => V>>;
    _lastWriteSource: M<S, S>;
    _tupleCache: M<S, S>;
    _mainMap: M<A, S>;
    _weakMap: WeakMap<O, S>;
    _boundaries: WeakMap<O, S>;
}
const DEFAULT_SCHEDULE = [OP.READ, OP.WRITE, OP.READ, OP.WRITE, OP.DELETE, OP.EFFECT, OP.SEND, OP.SUB,  OP.DRC, OP.COMPACT] as const;
const PAYLOAD = {
	SCALAR: 0,
	BINARY: 1,
} as const;
type HC_HANDLER = (p: U) => U | P<U>;
type OpKind = (typeof OP)[keyof typeof OP];
type WaitStrategy = "spin" | "yield" | "park";
interface HADRON_OPTIONS {
	capacity?: N;
	pidCapacity?: N;
	f64Capacity?: N;
	u32Capacity?: N;
	i32Capacity?: N;
	byteCapacity?: N;
	maxBatch?: N;
	wait?: WaitStrategy;
	shared?: B;
	schedule?: readonly N[];
}
type ReactiveFlags = number;
interface ReactiveNode {
    deps?: Link;
    depsTail?: Link;
    subs?: Link;
    subsTail?: Link;
    flags: ReactiveFlags;
}
const ReactiveFlags = {
    NONE: 0,
    SIGNAL: 1 << 0,
    COMPUTED: 1 << 1,
    EFFECT: 1 << 2,
    DIRTY: 1 << 3,
    RUNNING: 1 << 4,
    DISPOSED: 1 << 5,
    MUTABLE: 1 << 6,
    WATCHING: 1 << 7,
    RECURSEDCHECK: 1 <<8,
    RECURSED: 1 <<9,
    PENDING: 1 <<10,
} as const;
interface HADRON_COLLIDER_OPTIONS {
	ringCapacity?: N;
	slotCapacity?: N;
	pidCapacity?: N;
	maxBatch?: N;
	soaCapacity?: N;
	f64Capacity?:N
u32Capacity?:N
i32Capacity?:N
byteCapacity?:N
shared?:A
}
type DisruptorHandler = (pid: N, value: N, flag: N) => V;
const MAX_PID = 0xffffff;
interface LANE_STATS {
	published: N;
	consumed: N;
	batches: N;
	maxBatch: N;
	fullStalls: N;
	claimed: N;
}
interface ScopeComponents {
	r: N;
	m: N;
	s: N;
}
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
const queues: Record<Priority, Map<symbol | string, ScheduledTask[]>> = {
    immediate: new Map(), 'user-blocking': new Map(), normal: new Map(), background: new Map(), idle: new Map()
}
function pow2(value: number): boolean {
    return value > 0 && (value & (value - 1)) === 0;
}
// #endregion
//                                                                                                                    [SIGNALS_HUB]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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
			if (!(raw instanceof Uint8Array) && !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))) {
				console.error("[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
				return null;
			}
			try {
				const bytes = raw instanceof Uint8Array
					? raw
					: new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
				return this.#decode(bytes);
			} catch (err) {
				console.error("[AVRO_PIPE] Failed to decode inbound packet:", err);
				return null;
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
type Key = number | string | symbol | object;
export class M2Dense<V = number> {
		#vals: F64;
		#live: U8;
		#size = 0;
		#odd: M<Key, V> | null = null;
	
		constructor(capacity = 1024) {
			const cap = Math.max(1, capacity | 0);
			this.#vals = new Float64Array(cap);
			this.#live = new Uint8Array(cap);
		}
	
		get size(): N {
			return this.#size + (this.#odd === null ? 0 : this.#odd.size);
		}
		get capacity(): N {
			return this.#vals.length;
		}
		/** live backing buffer: value of pid p is buffer[p], valid where live[p] === 1 */
		get buffer(): F64 {
			return this.#vals;
		}
		get live(): U8 {
			return this.#live;
		}
	
		// ---- Map API ----------------------------------------------------------
		get(k: number): V | UD {
			const l = this.#live;
			if (typeof k === "number" && k >= 0 && k < l.length && l[k] === 1) return this.#vals[k] as V;
			if (typeof k !== "number" || k < 0) return this.#odd === null ? undefined : this.#odd.get(k as Key);
			return undefined;
		}
		has(k: number): boolean {
			const l = this.#live;
			if (typeof k === "number" && k >= 0 && k < l.length) return l[k] === 1;
			return this.#odd !== null && this.#odd.has(k as Key);
		}
		set(k: number, v: V): this {
			if (typeof v === "number" && typeof k === "number" && k >= 0) {
				if (k >= this.#live.length) this.#grow(k);
				if (this.#live[k] === 0) {
					this.#live[k] = 1;
					this.#size++;
				}
				this.#vals[k] = v;
				return this;
			}
			(this.#odd ??= new Map()).set(k as Key, v);
			return this;
		}
		delete(k: number): B {
			const l = this.#live;
			if (typeof k === "number" && k >= 0) {
				if (k >= l.length || l[k] === 0) return false;
				l[k] = 0;
				this.#size--;
				return true;
			}
			return this.#odd !== null && this.#odd.delete(k as Key);
		}
		clear() {
			this.#live.fill(0);
			this.#size = 0;
			this.#odd?.clear();
		}
		*keys(): MapIterator<N> {
			const l = this.#live,
				n = l.length;
			for (let k = 0; k < n; k++) if (l[k] === 1) yield k;
			if (this.#odd !== null) yield* this.#odd.keys() as MapIterator<N>;
		}
		*values(): MapIterator<V> {
			const l = this.#live,
				n = this.#size;
			if (this.#odd?.size) {
				const odd = [...this.#odd.values()];
				let seen = 0;
				for (let k = 0; k < l.length && seen < n; k++)
					if (l[k] === 1) {
						seen++;
						yield this.#vals[k] as V;
					}
				yield* odd;
				return;
			}
			let seen = 0;
			for (let k = 0; k < l.length && seen < n; k++)
				if (l[k] === 1) {
					seen++;
					yield this.#vals[k] as V;
				}
		}
		*entries(): MapIterator<[N, V]> {
			const l = this.#live;
			for (let k = 0; k < l.length; k++) if (l[k] === 1) yield [k, this.#vals[k] as V];
			if (this.#odd !== null) yield* this.#odd.entries() as MapIterator<[N, V]>;
		}
		[Symbol.iterator](): MapIterator<[N, V]> {
			return this.entries();
		}
		forEach(fn: (v: V, k: N, m: M<N, V>) => V, thisArg?: U) {
			const l = this.#live;
			for (let k = 0; k < l.length; k++) if (l[k] === 1) fn.call(thisArg, this.#vals[k] as V, k, this as U as Map<N, V>);
			this.#odd?.forEach((v, k) => fn.call(thisArg, v, k as N, this as U as Map<N, V>));
		}
	
		// ---- binary lane -------------------------------------------------------
		/** read by pid — no typeof guard: pids are numbers */
		g(pid: N): V | UD {
			return this.#live[pid] === 1 ? (this.#vals[pid] as V) : undefined;
		}
		/** write by pid */
		s(v: N, pid: N): this {
			if (pid >= this.#live.length) this.#grow(pid);
			if (this.#live[pid] === 0) {
				this.#live[pid] = 1;
				this.#size++;
			}
			this.#vals[pid] = v;
			return this;
		}
		getOr(pid: N, dflt = 0): N {
			return this.#live[pid] === 1 ? this.#vals[pid] : dflt;
		}
		/** pid += delta, returns the new value */
		add(pid: N, delta: N): N {
			if (pid >= this.#live.length) this.#grow(pid);
			if (this.#live[pid] === 0) {
				this.#live[pid] = 1;
				this.#size++;
			}
			return (this.#vals[pid] += delta);
		}
		in(pid: N): B {
			return this.#live[pid] === 1;
		}
		/** walk live pids without allocating an iterator */
		forEachRaw(fn: (pid: N, value: N) => V) {
			const l = this.#live;
			for (let k = 0; k < l.length; k++) if (l[k] === 1) fn(k, this.#vals[k]);
		}
		/** values for a list of pids, NaN where absent */
		gather(pids: ArrayLike<N>, out = new Float64Array(pids.length)): Float64Array {
			for (let i = 0; i < pids.length; i++) {
				const p = pids[i];
				out[i] = this.#live[p] === 1 ? this.#vals[p] : NaN;
			}
			return out;
		}
		#grow(need: N) {
			let cap = this.#live.length;
			while (cap <= need) cap *= 2;
			const v = new Float64Array(cap);
			v.set(this.#vals);
			this.#vals = v;
			const l = new Uint8Array(cap);
			l.set(this.#live);
			this.#live = l;
		}
}
type NestedRoot = Map<S, Map<S, M2Dense<number>>>;
// #endregion
//                                                                                                     [PROJECT_PLANET_KILLER]  HUB     
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class PROJECT_PLANET_KILLER {// YESSSS... THIS IS THE NOD THE PLATFORM IS MAKING WITH ITS NAME...
	// (globalThis as A).ENC_ATOM = ENC_ATOM;
	// (globalThis as A).AVRO = AVROI;
	public r 
	public m 
	public s 
	public root: SIGNALS
	public STATE_CONTEXT = 'sauron'
	#reg = new M2();
	#context = new M2();
	#meta = new M2(); 
	constructor(root:SIGNALS) {
		this.root = root
		this.r = Store.get('THE_WIZARD_ROOT_REG') ?? 'THE_WIZARD_ROOT_REG'
		this.m = Store.get('SAURON_MASTER_REG') ?? 'SAURON_MASTER_REG'
		this.s = Store.get('SAURON_SLAVE_REG') ?? 'SAURON_SLAVE_REG'
	}
	public _globalNotifyCache = new WeakMap<A, Map<A, () => V>>();
	public _notifyCache = new WeakMap<Map<A, Set<(value: A) => V>>, Map<A, () => V>>();
	public dirty = new Set<FlushCallback>();
	public _flushScheduled = false;
	public openScopes = new Set<BatchScope>();
	public LEAF_CAP = 65_536;
	public memoRoot: NestedRoot | UD;
	public memoMaster: S | UD;
	public memoSlave: S | UD;
	public memoLeaf: M2Dense<N> | UD;
	public HOT_LEAF_COUNT = 2;
	public PID_CAP = 1 << 18;
	public values = new Float64Array(this.HOT_LEAF_COUNT *this. PID_CAP);
	public present = new Uint8Array(this.HOT_LEAF_COUNT * this.PID_CAP);
	public CLOUD_LEAF = 0;
	public ATOMS_LEAF = 1;
	public DRS_LEAF = 2;
	public CACHE_LEAF = 3;
	public ATOMS_OFFSET = this.ATOMS_LEAF * this.PID_CAP;
	public CLOUD_OFFSET = this.CLOUD_LEAF * this.PID_CAP;
	public DRS_OFFSET = this.DRS_LEAF * this.PID_CAP;
	public CACHE_OFFSET = this.CACHE_LEAF * this.PID_CAP;


	public schemas = {
		BUS_ATOM: new BINARY_AVRO({// FIXED LAYOUT ADDRESS HEADER BINARY SCHEMA OBJ
				type: "record",
				name: "ENC_ATOM",
				fields: [
					{ name: "r", type: ["int", "bytes"] },
					{ name: "m", type: ["int", "bytes"] },
					{ name: "s", type: ["int", "bytes"] },
					{ name: "k", type: ["int", "bytes"] },
					{ name: "payload", type: ["int", "bytes"] },
				],
		}),
		QUEUE_ITEM: new BINARY_AVRO({// FIXED LAYOUT ADDRESS HEADER BINARY SCHEMA OBJ
			type: "record",
			name: "QUEUE_ITEM",
			fields: [
				{ name: "id", type: ["int", "bytes"] },
				{ name: "r", type: ["int", "bytes"] },
				{ name: "m", type: ["int", "bytes"] },
				{ name: "s", type: ["int", "bytes"] },
				{ name: "k", type: ["int", "bytes"] },
				{ name: "payload", type: ["int", "bytes"] },
			],
	    }),
		ENC_ATOM: new BINARY_AVRO({// FIXED LAYOUT ADDRESS HEADER BINARY SCHEMA OBJ
			/** THIS GIVES US THE FLEXIILITY WE NEED SUCH AS:
			 * - VARIABLE LENGTH PAYLAOD
			 * - THE ABILITY TO GO, WHERE WE WANT, WHEN WE WANT TO GO AND NOT HAVE TO WORRY ABOUT GOING BACK TO THE PLACE YOU ENCOED YOUR PAYLOAD JUST TO TRADE YOUR PID BACK IN FOR SAID PAYLOAD
			 * - MUCH MORE EASIER API TO DEAL WITH
			 * - FASTER ENCODING PROCESS
			 * - THE SOLUTION IS CODED IN A WAY WHERE WE HAVE TEED OURSELVES UP TO ALLOW THE CREATION OF RESUSBALE SCHEMA FNS THAT WE CAN REACH FOR WHENEVER WE ARE ENCODING OR DECODING ANYTHING
			 * - IF A USER DOES COME ACROSS IT, IT DOESNT HAVE THAT BINARY COMPLEXITY ATTACHED TO IT THROUGH AND THROUGH, AS IT IS EASY TO LOOK AT, UNDERSTAND AND EVEN PICK IT UP TO USE WITHOUT MUCH GUIDANCE, IF ANY
			 * - ALLOWS US TO JUST... DECODE A SINGLE VALUE WITHIN THE SCHEMA TO CONSUME IT, WHILE AT THE SAME TIME NOT WALKING THE ENTIRE OBJ EACH TIME
			 
			
			const channel = avro.decode_indexed_field(encoded, "k");
			
			const payload = avro.decode_indexed_field(encoded, "value");
			
			const encoded = avro.encode_indexed({
				r: "root",
				m: "master",
				s: "slave",
				k: "channel",
				value: payload
			});
			*/
				type: "record",
				name: "ENC_ATOM",
				fields: [
					{ name: "r", type: ["int", "bytes"] },
					{ name: "m", type: ["int", "bytes"] },
					{ name: "s", type: ["int", "bytes"] },
					{ name: "k", type: ["int", "bytes"] },
					{ name: "payload", type: ["int", "bytes"] },
				],
		}),
		AVROI :new BINARY_AVRO({
				type: "record",
				name: "SITHMessage",
				fields: [
					{ name: "pid", type: "int" },
					{ name: "op", type: "int" },
					{ name: "value", type: ["null", "double"] },
					{ name: "payload", type: ["null", "bytes"] },
				],
		})
	}
	public getNestedMap(s: S = this.s, m: S = this.m, r: S = this.r): M2Dense<N> {
		const g = globalThis as unknown as Record<string, unknown>;
		let root = g[r] as NestedRoot | undefined;

		// Avoid nested Map lookups for the most recently requested leaf.
		if (root && root === this.memoRoot && m === this.memoMaster && s === this.memoSlave) {
			return this.memoLeaf!;
		}

		if (!root) {
			root = new Map();
			g[r] = root;
		}

		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new Map();
			root.set(m, masterMap);
		}

		let leaf = masterMap.get(s);
		if (!leaf) {
			leaf = new M2Dense<number>(this.LEAF_CAP);
			masterMap.set(s, leaf);
		}

		this.memoRoot = root;
		this.memoMaster = m;
		this.memoSlave = s;
		this.memoLeaf = leaf;

		return leaf;
	}
	public atom = (() => {// GET
		const fn = (pid: N): N | UD  =>{
			const i = this.CLOUD_OFFSET + pid;
			return this.present[i] ? this.values[i] : undefined;
		}
		fn.set=(pid: N, value: N): V =>{
			const i = this.CLOUD_OFFSET + pid;
			this.values[i] = value;
			this.present[i] = 1;
		}
	return fn
	})();
	public sauron = (() => {// GET
		const fn = () => {}
		fn.set=(pid: N, value: N): V =>{
			const i = this.CLOUD_OFFSET + pid;
			this.values[i] = value;
			this.present[i] = 1;
		}
	return fn
	})();
	public drs = (() => {// GET
		const fn =() =>{}
		return fn

	})();
	public db_cache = (() => {// GET
		const fn =() =>{}

	return fn
	})();


	public _channels(s: A = this.root.s, m: A = this.root.m, r: A = this.root.r) {
       let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
        //let root = (globalThis as A)[r];
        if (!root) {
            root = new Map<A, Map<A, Map<A, A>>>();
            (globalThis as A)[r] = root;
        }

        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new Map<A, Map<A, A>>();
            root.set(m, masterMap);
        }

        let metaMap = masterMap.get(`${s}_CHANNELS`);
        if (!metaMap) {
            metaMap = {
                channels: new Map<string, Set<A>>(),
                sub_channels: new Map<string, Map<string, Set<A>>>(),
                trip_channels: new Map<string, Map<string, Set<A>>>(),
                quad_channels: new Map<string, Map<string, Set<A>>>(),
            };
            masterMap.set(`${s}_CHANNELS`, metaMap);
        }
        return metaMap;
    }
	public _listeners(s = this.root.s, m = this.root.m, r = this.root.r) {
		return this._channels(s, m, r).channels as Map<string, Set<A>>;
	}
	public getMetadata(s = this.root.s, m = this.root.m, r = this.root.r) {
		let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
		if (!root) {
			root = new Map<A, Map<A, Map<A, A>>>();
			(globalThis as A)[r] = root;
		}

		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new Map<A, Map<A, A>>();
			root.set(m, masterMap);
		}

		let metaMap = masterMap.get(`${s}_SIGNALS`);
		if (!metaMap) {
			metaMap = {
				_initial: new Map<string, A>(),
				_sys: new Map<string, SysEntry>(),
				_keyContracts: new Map<string, Set<string>>(),
				_causal: new Map<string, CausalEntry[]>(),
				_conflicts: new Map<string, ConflictPolicy>(),      
				_contracts: new Map<string, A>(),      
				_scopes: new Map<string, SignalScope>(),
				_msgHandlers: new Map<string, Set<(p: A) => void>>(),
				_mainMap: new Map<A, string>(),
				_tupleCache: new Map<string, string>(),
				_weakMap: new WeakMap<object, string>(),
			};
			masterMap.set(`${s}_SIGNALS`, metaMap);
		}
		return metaMap;
	}
	public MODULE_CACHE(s = this.root.s, m = this.root.m, r = this.root.r) {
		const meta = this.getMetadata(s, m, r);
		let module_cache = meta.get(this.root.mc);
		if (!module_cache) {
			module_cache = new Map<A, Map<A, Map<A, A>>>();
			this.root.create(this.root.mc, module_cache, undefined, s, m, r);
		}
		return module_cache;
	}

	public store = {
		get: <T = A,>(key: A, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): T | undefined => {
			const { s, ms_reg, r } = opts;
			const _state = this.state.getMap<A, A>(s, ms_reg, r);
			const _persistent = this.state.getMap<A, A>(`${s}_PERSISTENT`, ms_reg, r);
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);

			// recordDependency(key)
			if (_state.has(key)) return MaybePromise(_state.get(key));
			if (_persistKeys.has(key) && _persistent.has(key)) {
				const v = _persistent.get(key);
				_state.set(key, v);
				return MaybePromise(v);
			}
			return undefined;
		},
		set: <T = A,>(key: A, value: T | ((prev: T | undefined) => T), opts: { triggerRender?: boolean; cap?: number } & ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r, triggerRender: true, cap: 200 }) => {
			const { triggerRender = true, s, ms_reg, r, cap } = opts;
			const _state = this.state.getMap<A, A>(s, ms_reg, r);
			const _persistent = this.state.getMap<A, A>(`${s}_PERSISTENT`, ms_reg, r);
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);
			const _listeners = this.state.getMap<A, Set<(value: A) => void>>(`${s}_LISTENERS`, ms_reg, r);
			const prev = _state.get(key);
			const next = typeof value === "function" ? (value as (prev: T | undefined) => T)(prev) : value;
			if (Object.is(prev, next)) return;
			// TIME TRAVEL

			if (!this.root._time_travel_reverting) {
				const meta = this.state.getMetadata(s, ms_reg, r);
				let hist = meta._history.get(key);
				if (!hist) {
					hist = [];
					meta._history.set(key, hist);
				}
				const lastActive = hist.findLastIndex((e: HistoryEntry) => !e.undone) ?? -1;
				if (lastActive < hist.length - 1) hist.length = lastActive + 1; // truncate redo tail
				hist.push({ k: (hist[hist.length - 1]?.k ?? 0) + 1, prev, next, ts: Date.now(), undone: false });
				const cap_val = cap ?? 200;
				while (hist.length > cap_val) hist.shift();
			}
			_state.set(key, next);
			if (_persistKeys.has(key)) _persistent.set(key, next);
			if (triggerRender) this.state.scheduleGlobalNotify(key, this.state.getMetadata(s, ms_reg, r), opts);
			if (triggerRender) this.state.scheduleNotify(key, _listeners, _state);
			return MaybePromise(next as T);
		},
		subscribe: <T = A,>(key: A, listener: (value: T) => void, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): (() => void) => {
			const { s, ms_reg, r } = opts;
			const _listeners = this.state.getMap<A, Set<(value: A) => void>>(`${s}_LISTENERS`, ms_reg, r);

			if (!_listeners.has(key)) _listeners.set(key, new Set());
			_listeners.get(key)!.add(listener);
			return () => {
				_listeners.get(key)?.delete(listener);
				if (_listeners.get(key)?.size === 0) _listeners.delete(key);
			};
		},
		delete: (key: A, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): boolean => {
			const { s, ms_reg, r } = opts;
			const _state = this.state.getMap<A, A>(s, ms_reg, r);
			const _persistent = this.state.getMap<A, A>(`${s}_PERSISTENT`, ms_reg, r);
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);
			const _listeners = this.state.getMap<A, Set<(value: A) => void>>(`${s}_LISTENERS`, ms_reg, r);

			const existed = _state.delete(key);
			if (_persistKeys.has(key)) {
				_persistKeys.delete(key);
				_persistent.delete(key);
			}
			_listeners.get(key)?.forEach((l) => l(undefined));
			_listeners.delete(key);
			return MaybePromise(existed);
		},
		entries: <T = A,>(opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): [A, T][] => {
			const { s, ms_reg, r } = opts;
			const _state = this.state.getMap<A, A>(s, ms_reg, r);
			return MaybePromise(Array.from(_state.entries()));
		},
		persist: (key: A, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): void => {
			const { s, ms_reg, r } = opts;
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);
_persistKeys.set(key, true);
		},
		unpersist: (key: A, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): void => {
			const { s, ms_reg, r } = opts;
			const _persistent = this.state.getMap<A, A>(`${s}_PERSISTENT`, ms_reg, r);
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);
			_persistKeys.delete(key);
			_persistent.delete(key);
		},
		has: (key: A, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): boolean => {
			const { s, ms_reg, r } = opts;
			const _state = this.state.getMap<A, A>(s, ms_reg, r);
			const _persistent = this.state.getMap<A, A>(`${s}_PERSISTENT`, ms_reg, r);
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);

			return _state.has(key) || (_persistKeys.has(key) && _persistent.has(key));
		},
		clear: (opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): void => {
			const { s, ms_reg, r } = opts;
			const _state = this.state.getMap<A, A>(s, ms_reg, r);
			const _listeners = this.state.getMap<A, Set<(value: A) => void>>(`${s}_LISTENERS`, ms_reg, r);
			_state.clear();
			_listeners.clear();
		},
		clearPersistent: (opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): void => {
			const { s, ms_reg, r } = opts;
			const _persistent = this.state.getMap<A, A>(`${s}_PERSISTENT`, ms_reg, r);
			const _persistKeys = this.state.getMap<A, boolean>(`${s}_PERSIST_KEYS`, ms_reg, r);
			_persistent.clear();
			_persistKeys.clear();
		},
		_emit: (key: string | symbol, s = this.s, ms_reg = this.m, r = this.r): void => {
			const opts = { s, ms_reg, r };
			const _state = this.state.getMap(s, ms_reg, r);
			const _listeners = this.state.getMap<A, Set<(value: A) => void>>(`${s}_LISTENERS`, ms_reg, r);
			const meta = this.state.getMetadata(s, ms_reg, r);
			this.state.scheduleNotify(key, _listeners, _state);
			this.state.scheduleGlobalNotify(key, meta, opts);
		},
	};
	public state = {
		getMap: <K, V>(slave: A = this.s, master: A = this.m, rk: A = this.r): M2<K, V> => {
			let root = this.#context.get(rk); // (globalThis as any)[rk]  //  ; (globalThis as any)[rk] = root;
			if (!root) {
				root = new M2<A, M2I<A, M2I<A, A>>>();
				this.#context.set(rk, root);
			}
	
			let masterMap = root.get(master);
			if (!masterMap) {
				masterMap = new M2<A, M2I<A, A>>();
				root.set(master, masterMap);
			}
	
			let slaveMap = masterMap.get(slave);
			if (!slaveMap) {
				slaveMap = new M2<K, V>();
				masterMap.set(slave, slaveMap);
			}
			return slaveMap;
		},
		getMetadata:(s = this.s, m = this.m, r = this.r) => {
			let root = this.#meta.get(r); //  (globalThis as A)[r]; // STATE_CONTEXT === "sauron" ?  : null;
			if (!root) {
				root = new M2<A, M2I<A, M2I<A, A>>>();
				this.#meta.set(r, root);
			}
	
			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new M2<A, M2I<A, A>>();
				root.set(m, masterMap);
			}
	
			let metaMap = masterMap.get(s);
			if (!metaMap) {
				metaMap = {
					_batches: new Set<S | SY>(),
					_batchQueue: new Map<S | SY, A>(),
					_batchOpts: new Map<S | SY, { triggerRender?: B }>(),
					_history: new Map<S | SY, A[]>(),
					_timeTravel: new Map<S | SY, N>(),
					_globalListeners: new Set<(key: S | SY, value: A) => V>(),
					_global_subscribers: new Map<S, Set<() => V>>(),
					_waiters: new Map<S | SY, Set<(value: A) => V>>(),
					_sys: new Map<S, A>(),
					_sealed: new Set<S>(),
					_frozen: new Set<S>(),
					_keyContracts: new Map<S, Set<S>>(),
					_conflicts: new Map<S, A>(),
					_conflictStrategies: new Map<S, "latest" | "fail" | "accumulate">(),
					_causal: new Map<S, A[]>(),
					_scopes: new Map<S, A>(),
					_msgHandlers: new Map<S, Set<(p: A) => V>>(),
					_lastWriteSource: new Map<S, S>(),
					_mainMap: new Map<A, S>(),
					_tupleCache: new Map<S, S>(),
					_weakMap: new WeakMap<O, S>(),
					_boundaries: new WeakMap<O, S>(),
				};
				masterMap.set(s, metaMap);
			}
			return metaMap;
		},
		get: <T = A,>(key: string | symbol, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): T | undefined => {
			return MaybePromise(this.store.get<T>(key, opts) as T);
		},
		set: <T = A,>(key: string | symbol, value: T | ((prev: T | undefined) => T), opts: { triggerRender?: boolean } & ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): void => {
			const { s, ms_reg, r, triggerRender = true } = opts;
			this.store.set(key, value, { triggerRender, s, ms_reg, r });
			return MaybePromise(value as T);
		},
		delete: (key: string | symbol, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): boolean => {
			const { s, ms_reg, r } = opts;
			const meta = this.state.getMetadata(s, ms_reg, r);
			const deleted = this.store.delete(key, opts);
			if (deleted) {
				for (const listener of meta._globalListeners) {
					try {
						listener(key, undefined);
					} catch (err) {
						console.error(`[Sauron Global Listener Error]:`, err);
					}
				}
			}
			return deleted;
		},
		getByPrefix: <T = A,>(prefix: string, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): { key: string; value: T }[] => {
			return this.store
				.entries<T>(opts)
				.filter(([key]) => typeof key === "string" && key.startsWith(prefix))
				.map(([key, value]) => ({ key: key as string, value }));
		},
		getAll: <T = A,>(opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): Record<string | symbol, T> => {
			const result: Record<string | symbol, T> = {};
			for (const [key, value] of this.store.entries<T>(opts)) {
				result[key] = value;
			}
			return result;
		},
		subscribe: <T = A,>(key: string | symbol, listener: (value: T) => void, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): (() => void) => {
			return this.store.subscribe<T>(key, listener, opts);
		},
		subscribeAll: (listener: (key: string | symbol, value: A) => void, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): (() => void) => {
			const { s, ms_reg, r } = opts;
			const meta = this.state.getMetadata(s, ms_reg, r);
			meta._globalListeners.add(listener);
			return () => {
				meta._globalListeners.delete(listener);
			};
		},
		clear: (opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): void => {
			const { s, ms_reg, r } = opts;
			const meta = this.state.getMetadata(s, ms_reg, r);
			this.store.clear(opts);
			meta._waiters.clear();
			meta._globalListeners.clear();
		},
		waitFor: <T = A,>(key: string | symbol, opts: ScopeOptions = { s: this.s, ms_reg: this.m, r: this.r }): Promise<T> => {
			const { s, ms_reg, r } = opts;
			const meta = this.state.getMetadata(s, ms_reg, r);

			// If it already exists, resolve immediately
			const existing = this.state.get<T>(key, opts);
			if (existing !== undefined) return Promise.resolve(existing);

			return new Promise<T>((resolve) => {
				if (!meta._waiters.has(key)) meta._waiters.set(key, new Set());
				meta._waiters.get(key)!.add(resolve as A);
			});
		},
		scheduleNotify: (key: A, _listeners: Map<A, Set<(value: A) => V>>, _state: Map<A, A>): V =>{
			let cache = this._notifyCache.get(_listeners);
			if (!cache) {
				cache = new Map();
				this._notifyCache.set(_listeners, cache);
			}
			let fn = cache.get(key);
			if (!fn) {
				fn = () => {
					cache!.delete(key);
					const val = _state.get(key);
					_listeners.get(key)?.forEach((l) => l(val));
				};
				cache.set(key, fn);
			}
			this.state.onFlush(fn);
		},
		scheduleGlobalNotify :(key: S | SY, meta: A, opts: A): V => {
			let cache = this._globalNotifyCache.get(meta);
			if (!cache) {
				cache = new Map();
				this._globalNotifyCache.set(meta, cache);
			}
			let fn = cache.get(key);
			if (!fn) {
				fn = () => {
					cache!.delete(key);
					const finalVal = this.state.get(key, opts);
					const waiters = meta._waiters.get(key);
					if (waiters) {
						for (const resolveFn of waiters) {
							try {
								resolveFn(finalVal);
							} catch (err) {
								console.error(`[Sauron Waiter Error]:`, err);
							}
						}
						meta._waiters.delete(key);
					}
					for (const listener of meta._globalListeners) {
						try {
							listener(key, finalVal);
						} catch (err) {
							console.error(`[Sauron Global Listener Error]:`, err);
						}
					}
				};
				cache.set(key, fn);
			}
			this.state.onFlush(fn);
		},
		onFlush: (cb: FlushCallback): void =>{
			this.dirty.add(cb);
			this.state.scheduleDrain();
		},
		scheduleDrain: (): void =>{
			if (this._flushScheduled) return;
			this._flushScheduled = true;
			queueMicrotask(this.state.drain);
		},
		drain: (): void =>{
			this._flushScheduled = false;
			if (this.dirty.size === 0) return;
	
			// An explicit openBatch() scope is still open (deliberately spanning an await) —
			// don't drop the pending callbacks, just wait for closeBatch() to let them through.
			if (this.openScopes.size > 0) {
				this.state.scheduleDrain();
				return;
			}
	
			const callbacks = this.dirty;
			this.dirty = new Set();
			for (const cb of callbacks) cb();
		}
	};
	/**	public mesh_reg = (() => {
	 const mt = <K, V>(s: S = this.root.s, m: S= this.root.m, r: S = this.root.r): Map<A, A> => {
		 let root = (globalThis as A)[r] // this.STATE_CONTEXT === "sauron" ?  : null; // this.#reg.get(r); // 
		 if (!root) {
			 root = new Map<A, Map<A, Map<A, A>>>();
			 (globalThis as A)[r] = root;// this.#reg.set(r, root)
		 }

		 let masterMap = root.get(m);
		 if (!masterMap) {
			 masterMap = new Map<A, Map<A, A>>();
			 root.set(m, masterMap);
		 }

		 let slaveMap = masterMap.get(`${s}_SIGNALS_`);
		 if (!slaveMap) {
			 slaveMap = new Map<K, V>();
			 masterMap.set(`${s}_SIGNALS_`, slaveMap);
		 }
		 return slaveMap as Map<A, A>;
	 }; // get channels / root channels
	 mt.gks = (r = this.root.r) => {
		 // 1. Root Lookup via Module Cache (Fast path)
		 let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
		 if (!root) {
			 root = new Map<A, Map<A, Map<A, A>>>();
			 (globalThis as A)[r] = root;
		 }
		 return root;
	 }; // get sub_channel / master channels
	 mt.mss = (m = this.root.m, r = this.root.r) => {
		 let root = this.atom_reg.gks(r);
		 let masterMap = root.get(m);
		 if (!masterMap) {
			 masterMap = new Map<A, Map<A, A>>();
			 root.set(m, masterMap);
		 }
		 return masterMap;
	 }; // get value
	 mt.v = <T,>(k: A, t = this.root.s, s = this.root.m, c = this.root.r) => {
		 const reg = this.atom_reg(t, s, c);
		 return reg.get(k) as SIGNAL_CELL<T>;
	 }; // get meta value
	 mt.GET_META = (meta_key: string, s = this.root.s, m = this.root.m, r = this.root.r) => {
		 const atom_reg = this.atom_reg(s, m, r);
		 let meta_cache = atom_reg.get(meta_key);
		 if (!meta_cache) {
			 meta_cache = new Map<A, A>();
			 this.root.create(meta_key, meta_cache, undefined, s, m, r);
		 }
		 return meta_cache;
	 };
	 mt.MODULE_CACHE = <T = A,>(s = this.root.s, m = this.root.m, r = this.root.r): T => {
		 return this.atom_reg.GET_META(this.root.mc, s, m, r) as T;
	 };
	 return mt;
 })(); */
}
// #endregion
//                                          [VADER]  RX SCHEDULER / SIGNALS BACKEND / CODECS / SLAB / STATE/CONTEXT HOT PATH NGIN / 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class VADER {
	//                                                              [DARTH VADER] THE TRUE POWER OF THE REACTIVE SIDE [COMBINED RX]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public root: SIGNALS;
	get hub(): PROJECT_PLANET_KILLER {
		return this.root.hub;
	}
	get disruptor(): GALEN_MAREK {
		return this.root.bus;
	}

	public FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325n;
	public FNV_PRIME_64 = 0x100000001b3n;
	public U64_MAX = (1n << 64n) - 1n;
	public U64_MOD = 1n << 64n;
	public _localChange = false;
	public _channel: BroadcastChannel | null = null;
	public activeScope: EffectScopeNode | undefined;
	public defaultErrorHandler: EffectErrorHandler | null = null;
	public effects = new Set<SyncJob>();
	public externals = new Set<SyncJob>();
	public observers = new Set<SyncJob>();
	public flushQueued = false;
	public flushing = false;
	public isReact = false;
	public computations = new Set<SyncJob>();
	public init_hooks_context = false;
	public queued: (EffectNode | undefined)[] = [];
	// Sets dedupe jobs: one computation/effect/render callback runs once per flush, even if multiple atoms invalidate it.
	public asyncEffects = new Map<string, AsyncEffectJob>();
	readonly shared: B;

	constructor(root: SIGNALS, options: HADRON_COLLIDER_OPTIONS = {}, initialCapacity = 1 << 16, maxPids = 65536) {
		this.root = root;
		const ringCapacity = options.ringCapacity ?? 1 << 18;
		const slotCapacity = options.slotCapacity ?? 1 << 20;
		const pidCapacity = options.pidCapacity ?? 1 << 20;

		this.pidToSlot = new Int32Array(pidCapacity).fill(-1);
		this.slotToPid = new Int32Array(slotCapacity).fill(-1);

		this.intakeCapacity = ringCapacity;

		this.intakePid = new Int32Array(ringCapacity);
		this.intakeSlot = new Int32Array(ringCapacity);
		this.intakeValueOffset = new Int32Array(ringCapacity);
		this.intakeOp = new Int32Array(ringCapacity);
		this.intakeValueLength = new Int32Array(ringCapacity);
		this.install();

		//                                                       SOA ENGINE
		// ══════════════════════════════════════════════════════════════════════════════════════════════════════
		const soa_capacity = options.soaCapacity ?? 1 << 14;
		this.capacity = soa_capacity;
		this.lane = new Int32Array(soa_capacity).fill(-1);
		this.ops = new Uint8Array(soa_capacity);
		this.posX = new Float32Array(soa_capacity);
		this.posY = new Float32Array(soa_capacity);
		this.val = new Int32Array(soa_capacity);
		this.freePids = new Int32Array(soa_capacity);

		this.freeHead = 0;
		this.freeTail = 0;
		for (let i = soa_capacity - 1; i >= 1; i--) {
			this.freePids[this.freeTail] = i;
			this.freeTail = (this.freeTail + 1) % soa_capacity;
		}

		this.maxBatch = options.maxBatch ?? 8192;
		this.shared = options.shared ?? typeof SharedArrayBuffer !== "undefined";

		this.lane[0] = TRACER_PID;
		this.val[TRACER_PID] = 0;
		this.ops[TRACER_PID] = OP.NONE;
		this.laneLen = 1;
		this.storage = new Uint8Array(soa_capacity);
		this.expiresAt = new Float64Array(soa_capacity);

		if (!pow2(slotCapacity)) throw new Error("slotCapacity must be a power of two");

		this.capacity = slotCapacity;
		this.slotCapacity = slotCapacity;

		this.values = new Float64Array(slotCapacity);
		this.versions = new Uint32Array(slotCapacity);
		this.nextFree = new Int32Array(slotCapacity);
		this.refCount = new Int32Array(slotCapacity);

		this.nextFree.fill(-1);

		this.allocated = 1;
		this.refCount[0] = 1;
		this.versions[0] = 0;

		this.slabOffsets = new Int32Array(slotCapacity);
		this.slabLengths = new Int32Array(slotCapacity);
		this.slab = new Uint8Array(slotCapacity);

		this.typeMap = new Uint8Array(maxPids);
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
		this.keyMap = new Int32Array(maxPids);

		this.bytes = new Uint8Array(initialCapacity);
		this.view = new DataView(this.bytes.buffer);

		const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;

		this.f64 = new Float64Array(new AB((options.f64Capacity ?? 1 << 18) * 8));
		this.u32 = new Uint32Array(new AB((options.u32Capacity ?? 1 << 18) * 4));
		this.i32 = new Int32Array(new AB((options.i32Capacity ?? 1 << 18) * 4));
		this.bytes = new Uint8Array(new AB(options.byteCapacity ?? 1 << 20));
	}
	public batch<T>(fn: () => T): T {
		this.batchDepth += 1;
		try {
			return fn();
		} finally {
			this.batchDepth -= 1;

			if (this.batchDepth === 0) {
				this.requestFlush();
			}
		}
	}
	public enqueueComputation(job: SyncJob) {
		this.computations.add(job);
		this.requestFlush();
	}
	public enqueueEffect(job: SyncJob) {
		this.effects.add(job);
		this.requestFlush();
	}
	public enqueueExternal(job: SyncJob) {
		this.externals.add(job);
		this.requestFlush();
	}
	public enqueueObserver(job: SyncJob) {
		this.observers.add(job);
		this.requestFlush();
	}
	public enqueueAsyncEffect(job: AsyncEffectJob) {
		this.asyncEffects.set(job.id, job);
		this.requestFlush();
	}
	private requestFlush() {
		if (this.batchDepth > 0 || this.flushQueued || this.flushing) {
			return;
		}

		this.flushQueued = true;

		queueMicrotask(() => {
			this.flushQueued = false;
			void this.rx_flush();
		});
	}
	private take<T>(set: Set<T>) {
		const jobs = [...set];
		set.clear();
		return jobs;
	}
	private runSyncJobs(jobs: SyncJob[], phase: string) {
		for (const job of jobs) {
			try {
				job();
			} catch (error) {
				console.error(`[ReactiveScheduler:${phase}] job failed:`, error);
			}
		}
	}
	private async rx_flush() {
		if (this.flushing || this.batchDepth > 0) return;

		this.flushing = true;

		try {
			// A computed job may write another atom and schedule more computed jobs. Drain until no computed work remains so derived state is stable first.
			while (this.computations.size > 0) {
				this.runSyncJobs(this.take(this.computations), "computation");
			}
			this.runSyncJobs(this.take(this.effects), "effect");
			await this.flushAsyncEffects();
			this.runSyncJobs(this.take(this.externals), "external");
			this.runSyncJobs(this.take(this.observers), "observer");
		} finally {
			this.flushing = false;
			if (
				// If work arrived while flushing, schedule the next batch.
				this.computations.size > 0
				|| this.effects.size > 0
				|| this.externals.size > 0
				|| this.observers.size > 0
				|| this.asyncEffects.size > 0
			) {
				this.requestFlush();
			}
		}
	}
	private async flushAsyncEffects() {
		if (this.asyncEffects.size === 0) return;

		const queued = [...this.asyncEffects.values()];
		this.asyncEffects.clear();

		const queue = new TIERED_QUEUE({
			defaultConcurrent: 20,
			onTierError: (tier, error) => {
				console.error(`[ReactiveScheduler] async effect tier ${tier} failed:`, error);
			},
		});

		for (const job of queued) {
			void queue.ADD(
				job.priority,
				job.id,
				async () => {
					await job.run();
				},
				{
					retry: job.retry,
					timeout: job.timeout,
				},
			);
		}

		await queue.execute();
	}
	public assertSamplingRate = (rate: number): void => {
		if (!Number.isFinite(rate) || rate < 0 || rate > 1) {
			throw new RangeError(`sampling rate must be a finite number from 0 to 1; got ${rate}`);
		}
	};
	public utf8Bytes = (value: string): Uint8Array => {
		return new TextEncoder().encode(value);
	};
	public fnv1a64 = (value: string): bigint => {
		let hash = this.FNV_OFFSET_BASIS_64;

		for (const byte of this.utf8Bytes(value)) {
			hash ^= BigInt(byte);
			hash = (hash * this.FNV_PRIME_64) & this.U64_MAX;
		}

		return hash;
	};
	public samplingThreshold = (rate: number): bigint => {
		if (rate === 0) return 0n;
		if (rate === 1) return this.U64_MOD;

		// Decimal rates are converted deterministically through their string form.
		// For stricter cross-language protocol guarantees, accept an integer
		// numerator/denominator instead of a JS floating-point value.
		const [whole, fraction = ""] = String(rate).split(".");
		const denominator = 10n ** BigInt(fraction.length);
		const numerator = BigInt(`${whole}${fraction}`);

		return (this.U64_MOD * numerator) / denominator;
	};
	public untrackFn = <T,>(fn: () => T): T => {
		const prev = this.setActiveSub(undefined);
		try {
			return fn();
		} finally {
			this.setActiveSub(prev);
		}
	};
	public untrack = <T,>(fn: () => T): T => {
		return this.untrackFn(fn);
	};
	public runTeardown = (t: Teardown): void => {
		if (t == null) return;
		if (typeof t === "function") {
			t();
			return;
		}
		if (Array.isArray(t)) {
			for (const x of t) this.runTeardown(x);
			return;
		}
		const anyT = t as A;
		if (typeof anyT.unsubscribe === "function") {
			anyT.unsubscribe();
			return;
		}
		if (typeof anyT[Symbol.dispose] === "function") {
			anyT[Symbol.dispose]();
			return;
		}
		throw new TypeError("Invalid teardown: " + t);
	};
	public setEffectErrorHandler = (fn: EffectErrorHandler | null): void => {
		this.defaultErrorHandler = fn;
	};
	public sysEffect = (fn: () => void | (() => void)): ReturnType<typeof effect> => {
		return effect(fn);
	};
	public shouldSample = (key: string, config: SamplingConfig, namespace = ""): boolean => {
		this.assertSamplingRate(config.rate);

		if (config.rate === 0) return false;
		if (config.rate === 1) return true;

		const version = config.version ?? 1;
		const algorithm = config.algorithm ?? "fnv1a64";

		const input = [`v=${version}`, `alg=${algorithm}`, `seed=${String(config.seed)}`, `namespace=${namespace}`, `key=${key}`].join("\u001f");

		return this.fnv1a64(input) < this.samplingThreshold(config.rate);
	};
	// #endregion
	//                                                                                                                 [SIGNALS RX]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public BRAND_SIGNAL = Symbol("warlock.signal");
	public BRAND_EFFECT = Symbol("warlock.effect");
	public BRAND_EFFECT_SCOPE = Symbol("warlock.effectScope");
	public BRAND_COMPUTED = Symbol("warlock.computed");
	public HasChildEffect = 64;
	public cycle = 0;
	public runDepth = 0;
	public batchDepth = 0;
	public notifyIndex = 0;
	public queuedLength = 0;
	private readonly maxBatch: N;
	public activeSub: ReactiveNode | undefined;
	public getActiveSub(): ReactiveNode | undefined {
		return this.activeSub;
	}
	public setActiveSub(sub?: ReactiveNode) {
		const prevSub = this.activeSub;
		this.activeSub = sub;
		return prevSub;
	}
	public getBatchDepth(): number {
		return this.batchDepth;
	}
	public startBatch() {
		++this.batchDepth;
	}
	public endBatch() {
		if (!--this.batchDepth) {
			this.flush();
		}
	}
	public update(node: SignalNode | ComputedNode | EffectScopeNode): boolean {
		if ("getter" in node) {
			return this.updateComputed(node);
		}
		if ("currentValue" in node) {
			return this.updateSignal(node);
		}
		node.flags = ReactiveFlags.MUTABLE;
		return true;
	}
	public notify(effect: EffectNode) {
		let insertIndex = this.queuedLength;
		let firstInsertedIndex = insertIndex;

		do {
			queued[insertIndex++] = effect;
			effect.flags &= ~ReactiveFlags.WATCHING;
			effect = effect.subs?.sub as EffectNode;
			if (effect === undefined || !(effect.flags & ReactiveFlags.WATCHING)) {
				break;
			}
		} while (true);

		this.queuedLength = insertIndex;

		while (firstInsertedIndex < --insertIndex) {
			const left = queued[firstInsertedIndex];
			queued[firstInsertedIndex++] = queued[insertIndex];
			queued[insertIndex] = left;
		}
	}
	public unwatched(node: SignalNode | ComputedNode | EffectNode | EffectScopeNode) {
		if ("getter" in node) {
			if (node.depsTail !== undefined) {
				node.flags = ReactiveFlags.MUTABLE;
				this.disposeAllDepsInReverse(node);
			}
		} else if ("currentValue" in node) {
			// Nothing to do for signals, they are always mutable and never dirty until pendingValue changes
		} else if ("fn" in node) {
			this.effectOper.call(node, node);
		} else {
			this.effectScopeOper.call(node, node);
		}
	}
	public link(dep: ReactiveNode, sub: ReactiveNode, version: number): void {
		const prevDep = sub.depsTail;
		if (prevDep !== undefined && prevDep.dep === dep) {
			return;
		}
		const nextDep = prevDep !== undefined ? prevDep.nextDep : sub.deps;
		if (nextDep !== undefined && nextDep.dep === dep) {
			nextDep.version = version;
			sub.depsTail = nextDep;
			return;
		}
		const prevSub = dep.subsTail;
		if (prevSub !== undefined && prevSub.version === version && prevSub.sub === sub) {
			return;
		}
		const newLink =
			(sub.depsTail =
			dep.subsTail =
				{
					version,
					dep,
					sub,
					prevDep,
					nextDep,
					prevSub,
					nextSub: undefined,
				});
		if (nextDep !== undefined) {
			nextDep.prevDep = newLink;
		}
		if (prevDep !== undefined) {
			prevDep.nextDep = newLink;
		} else {
			sub.deps = newLink;
		}
		if (prevSub !== undefined) {
			prevSub.nextSub = newLink;
		} else {
			dep.subs = newLink;
		}
	}
	public unlink(link: Link, sub = this.link.sub): Link | undefined {
		const { dep, prevDep, nextDep, nextSub, prevSub } = link;
		if (nextDep !== undefined) {
			nextDep.prevDep = prevDep;
		} else {
			sub.depsTail = prevDep;
		}
		if (prevDep !== undefined) {
			prevDep.nextDep = nextDep;
		} else {
			sub.deps = nextDep;
		}
		if (nextSub !== undefined) {
			nextSub.prevSub = prevSub;
		} else {
			dep.subsTail = prevSub;
		}
		if (prevSub !== undefined) {
			prevSub.nextSub = nextSub;
		} else if ((dep.subs = nextSub) === undefined) {
			this.unwatched(dep);
		}
		return nextDep;
	}
	public propagate(link: Link, innerWrite: boolean): void {
		let next = link.nextSub;
		let stack: Stack<Link | undefined> | undefined;

		top: do {
			const sub = link.sub;
			let flags = sub.flags;

			if (!(flags & (ReactiveFlags.RECURSEDCHECK | ReactiveFlags.RECURSED | ReactiveFlags.PENDING))) {
				sub.flags = flags | ReactiveFlags.PENDING;
				if (innerWrite) {
					sub.flags |= ReactiveFlags.RECURSED;
				}
			} else if (!(flags & (ReactiveFlags.RECURSEDCHECK | ReactiveFlags.RECURSED))) {
				flags = ReactiveFlags.NONE;
			} else if (!(flags & ReactiveFlags.RECURSEDCHECK)) {
				sub.flags = (flags & ~ReactiveFlags.RECURSED) | ReactiveFlags.PENDING;
			} else if (!(flags & (ReactiveFlags.PENDING || ReactiveFlags.PENDING)) && this.isValidLink(link, sub)) {
				sub.flags = flags | (ReactiveFlags.RECURSED | ReactiveFlags.PENDING);
				flags &= ReactiveFlags.MUTABLE;
			} else {
				flags = ReactiveFlags.NONE;
			}

			if (flags & ReactiveFlags.WATCHING) {
				this.notify(sub);
			}

			if (flags & ReactiveFlags.MUTABLE) {
				const subSubs = sub.subs;
				if (subSubs !== undefined) {
					const nextSub = (link = subSubs).nextSub;
					if (nextSub !== undefined) {
						stack = { value: next, prev: stack };
						next = nextSub;
					}
					continue;
				}
			}

			if ((link = next!) !== undefined) {
				next = link.nextSub;
				continue;
			}

			while (stack !== undefined) {
				link = stack.value!;
				stack = stack.prev;
				if (link !== undefined) {
					next = link.nextSub;
					continue top;
				}
			}

			break;
		} while (true);
	}
	public checkDirty(link: Link, sub: ReactiveNode): boolean {
		let stack: Stack<Link> | undefined;
		let checkDepth = 0;
		let dirty = false;

		top: do {
			const dep = link.dep;
			const flags = dep.flags;

			if (sub.flags) {
				dirty = true;
			} else if ((flags & ReactiveFlags.MUTABLE) === ReactiveFlags.MUTABLE) {
				const subs = dep.subs!;
				if (this.update(dep)) {
					if (subs.nextSub !== undefined) {
						this.shallowPropagate(subs);
					}
					dirty = true;
				}
			} else if ((flags & (ReactiveFlags.MUTABLE | ReactiveFlags.PENDING)) === (ReactiveFlags.MUTABLE | ReactiveFlags.PENDING)) {
				stack = { value: link, prev: stack };
				link = dep.deps!;
				sub = dep;
				++checkDepth;
				continue;
			}

			if (!dirty) {
				const nextDep = link.nextDep;
				if (nextDep !== undefined) {
					link = nextDep;
					continue;
				}
			}

			while (checkDepth--) {
				link = stack!.value;
				stack = stack!.prev;
				if (dirty) {
					const subs = sub.subs!;
					if (this.update(sub)) {
						if (subs.nextSub !== undefined) {
							this.shallowPropagate(subs);
						}
						sub = link.sub;
						continue;
					}
					dirty = false;
				} else {
					sub.flags &= ~ReactiveFlags.PENDING;
				}
				sub = link.sub;
				const nextDep = link.nextDep;
				if (nextDep !== undefined) {
					link = nextDep;
					continue top;
				}
			}

			return dirty && !!sub.flags;
		} while (true);
	}
	public shallowPropagate(link: Link): void {
		do {
			const sub = link.sub;
			const flags = sub.flags;
			if ((flags & ReactiveFlags.PENDING) === ReactiveFlags.PENDING) {
				sub.flags = flags;
				if ((flags & (ReactiveFlags.WATCHING | ReactiveFlags.RECURSEDCHECK)) === ReactiveFlags.WATCHING) {
					this.notify(sub);
				}
			}
		} while ((link = link.nextSub!) !== undefined);
	}
	public isValidLink(checkLink: Link, sub: ReactiveNode): boolean {
		let link = sub.depsTail;
		while (link !== undefined) {
			if (link === checkLink) {
				return true;
			}
			link = link.prevDep;
		}
		return false;
	}
	public isSignal(fn: unknown): fn is Signal<any> {
		return typeof fn === "function" && this.BRAND_SIGNAL in fn;
	}
	public isComputed(fn: unknown) {
		return typeof fn === "function" && this.BRAND_COMPUTED in fn;
	} // : fn is Computed<any>
	public isEffect(fn: unknown) {
		return typeof fn === "function" && this.BRAND_EFFECT in fn;
	} // : fn is Effect
	public isEffectScope(fn: unknown) {
		return typeof fn === "function" && this.BRAND_EFFECT_SCOPE in fn;
	} // : fn is EffectScope
	public updateComputed(c: ComputedNode): boolean {
		if (c.flags & this.HasChildEffect) {
			let link = c.depsTail;
			while (link !== undefined) {
				const prev = link.prevDep;
				const dep = link.dep;
				if (!("getter" in dep) && !("currentValue" in dep)) {
					this.unlink(link, c);
				}
				link = prev;
			}
		}
		c.depsTail = undefined;
		c.flags = ReactiveFlags.MUTABLE | ReactiveFlags.RECURSEDCHECK;
		const prevSub = this.setActiveSub(c);
		try {
			++this.cycle;
			const oldValue = c.value;
			const nextValue = c.getter(oldValue);
			c.value = nextValue;
			return !Object.is(oldValue, nextValue);
		} finally {
			this.activeSub = prevSub;
			c.flags &= ~ReactiveFlags.RECURSEDCHECK;
			this.purgeDeps(c);
		}
	}
	public updateSignal(s: SignalNode): boolean {
		s.flags = ReactiveFlags.MUTABLE;
		const previous = s.currentValue;
		const next = s.pendingValue;
		s.currentValue = next;
		return !Object.is(previous, next);
	}
	public run(e: EffectNode): void {
		const flags = e.flags;
		if (flags || (flags & ReactiveFlags.PENDING && this.checkDirty(e.deps!, e))) {
			if (flags & this.HasChildEffect) {
				let link = e.depsTail;
				while (link !== undefined) {
					const prev = link.prevDep;
					const dep = link.dep;
					if (!("getter" in dep) && !("currentValue" in dep)) {
						this.unlink(link, e);
					}
					link = prev;
				}
			}
			if (e.cleanup) {
				this.runCleanup(e);
				if (!e.flags) {
					return;
				}
			}
			e.depsTail = undefined;
			e.flags = ReactiveFlags.WATCHING | ReactiveFlags.RECURSEDCHECK;
			const prevSub = this.setActiveSub(e);
			try {
				++this.cycle;
				++this.runDepth;
				e.cleanup = e.fn();
			} /** catch (err:A) {
				const handler = e.onError ?? this.defaultErrorHandler;
				if (handler) handler(err, e);
				else queueMicrotask(() => {throw err;});
			} */ finally {
				--this.runDepth;
				this.activeSub = prevSub;
				e.flags &= ~ReactiveFlags.RECURSEDCHECK;
				this.purgeDeps(e);
			}
		} else if (e.deps !== undefined) {
			e.flags = ReactiveFlags.WATCHING | (flags & this.HasChildEffect);
		}
	}
	public flush(): void {
		try {
			while (this.notifyIndex < this.queuedLength) {
				const effect = queued[this.notifyIndex]!;
				queued[this.notifyIndex++] = undefined;
				this.run(effect);
			}
		} finally {
			while (this.notifyIndex < this.queuedLength) {
				const effect = queued[this.notifyIndex]!;
				queued[this.notifyIndex++] = undefined;
				effect.flags |= ReactiveFlags.WATCHING | ReactiveFlags.RECURSED;
			}
			this.notifyIndex = 0;
			this.queuedLength = 0;
		}
	}
	public computedOper<T>(that: ComputedNode<T>): T {
		const flags = that.flags;
		if (flags & flags & ReactiveFlags.PENDING && (this.checkDirty(that.deps!, that) || ((that.flags = flags & ~ReactiveFlags.PENDING), false))) {
			if (this.updateComputed(that)) {
				const subs = that.subs;
				if (subs !== undefined) {
					this.shallowPropagate(subs);
				}
			}
		} else if (!flags) {
			that.flags = ReactiveFlags.PENDING | ReactiveFlags.RECURSEDCHECK;
			const prevSub = this.setActiveSub(that);
			try {
				that.value = that.getter();
			} finally {
				this.activeSub = prevSub;
				that.flags &= ~ReactiveFlags.RECURSEDCHECK;
			}
		}
		const sub = this.activeSub;
		if (sub !== undefined) {
			this.link(that, sub, this.cycle);
		}
		return that.value!;
	}
	public signalOper<T>(that: SignalNode<T>, ...value: [T]): T | void {
		if (value.length) {
			const next = value[0];

			if (!Object.is(that.pendingValue, next)) {
				that.pendingValue = next;
				that.flags = ReactiveFlags.MUTABLE | ReactiveFlags.DIRTY;

				const subs = that.subs;

				if (subs !== undefined) {
					this.propagate(subs, !!this.runDepth);

					if (!this.batchDepth) {
						this.flush();
					}
				}
			}
		} else {
			if (value.length) {
				const next = value[0];

				if (!Object.is(that.pendingValue, next)) {
					that.pendingValue = next;
					that.flags = ReactiveFlags.MUTABLE | ReactiveFlags.DIRTY;
					const subs = that.subs;
					if (subs !== undefined) {
						this.propagate(subs, !!this.runDepth);
						if (!this.batchDepth) {
							this.flush();
						}
					}
				}
			} else {
				if (that.flags & ReactiveFlags.DIRTY) {
					if (this.updateSignal(that)) {
						const subs = that.subs;
						if (subs !== undefined) {
							this.shallowPropagate(subs);
						}
					}
				}
				const sub = this.activeSub;
				if (sub !== undefined) {
					this.link(that, sub, this.cycle);
				}
				return that.currentValue;
			}
		}
	}
	public runCleanup(e: EffectNode): void {
		const cleanup = e.cleanup!;
		e.cleanup = undefined;
		const prevSub = this.activeSub;
		this.activeSub = undefined;
		try {
			cleanup();
		} finally {
			this.activeSub = prevSub;
		}
	}
	public effectOper(that: EffectNode): void {
		this.effectScopeOper.call(that, that);
		if (that.cleanup) {
			this.runCleanup(that);
		}
	}
	public effectScopeOper(that: EffectScopeNode): void {
		that.flags = ReactiveFlags.NONE;
		this.disposeAllDepsInReverse(that);
		const sub = that.subs;
		if (sub !== undefined) {
			this.unlink(sub);
		}
	}
	public disposeAllDepsInReverse(sub: ReactiveNode): void {
		let link = sub.depsTail;
		while (link !== undefined) {
			const prev = link.prevDep;
			this.unlink(link, sub);
			link = prev;
		}
	}
	public purgeDeps(sub: ReactiveNode) {
		const depsTail = sub.depsTail;
		let dep = depsTail !== undefined ? depsTail.nextDep : sub.deps;
		while (dep !== undefined) {
			dep = this.unlink(dep, sub);
		}
	}
	// #endregion
	//                                                                                                           [DARTH VADER] SLAB
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public pidToSlot: I32;
	private readonly slotToPid: I32;

	private readonly intakePid: I32;
	private readonly intakeSlot: I32;
	private readonly intakeValueOffset: I32;
	private readonly intakeOp: I32;
	private readonly intakeValueLength: Int32Array;
	private intakeHead = 0;
	private intakeTail = 0;
	private intakeCapacity: N;

	cursor = 0;
	public storage: U8; // 0 = DISRUPTOR, 1 = SLAB
	public expiresAt: F64; // 0 = no expiry

	readonly versions: U32;
	readonly refCount: I32;
	readonly values: F64;
	private readonly slabOffsets: I32;
	private readonly slabLengths: I32;
	readonly nextFree: I32;
	private slabTail = 0;
	private allocated = 0;
	private freeHead = -1;
	private readonly f64: F64;
	private readonly u32: U32;
	private readonly i32: I32;
	private readonly slab: U8;

	private f64Tail = 0;
	private u32Tail = 0;
	private i32Tail = 0;
	private byteTail = 0;
	bind(pid: N, slot: N): V {
		this.pidToSlot[pid] = slot;
	}
	unbind(pid: N): V {
		this.pidToSlot[pid] = -1;
	}
	slot(pid: N): N {
		return this.pidToSlot[pid];
	}
	allocF64(value: N): N {
		const at = this.f64Tail++;
		this.f64[at] = value;
		return at;
	}
	allocU32(value: N): N {
		const at = this.u32Tail++;
		this.u32[at] = value;
		return at;
	}
	allocI32(value: N): N {
		const at = this.i32Tail++;
		this.i32[at] = value;
		return at;
	}
	allocBytes(data: U8): N {
		const at = this.byteTail;
		this.bytes.set(data, at);
		this.byteTail += data.length;
		return at;
	}
	f64At(offset: N): N {
		return this.f64[offset];
	}
	u32At(offset: N): N {
		return this.u32[offset];
	}
	i32At(offset: N): N {
		return this.i32[offset];
	}
	bytesAt(offset: N, length: N): U8 {
		return this.bytes.subarray(offset, offset + length);
	}
	public slab_storage = (() => {
		const fn = () => {};
		fn.admit = (offset: N, length: N, pid: Pid): V => {
			this.root.governor.wake();
			this.posX[pid] = offset;
			this.posY[pid] = length;
			this.val[pid] = 0;
			this.ops[pid] = OP.WRITE;
			this.storage[pid] = 1; // SLAB
			const slot = this.laneLen++;
			this.lane[slot] = pid;
		};
		fn.settle = (): N => {
			// Drain every queued cycle until the collider is empty

			let total = 0;

			for (;;) {
				const before = total;
				total += this.slab_storage.pass();
				if (total === before) break;
			}

			return total;
		};
		fn.advance = (): N => {
			return ++this.versions[0];
		};
		fn.version = (slot: N): N => {
			return this.versions[slot];
		};
		fn.retain = (slot: N): V => {
			this.refCount[slot]++;
		};
		fn.release = (slot: N): V => {
			const count = --this.refCount[slot];

			if (count > 0) return;

			this.values[slot] = 0;
			this.versions[slot] = 0;
			this.slabOffsets[slot] = 0;
			this.slabLengths[slot] = 0;
			this.nextFree[slot] = this.freeHead;
			this.freeHead = slot;
		};
		fn.alloc = (value = 0): N => {
			let slot = this.freeHead;
			if (slot >= 0) {
				this.freeHead = this.nextFree[slot];
				this.nextFree[slot] = -1;
			} else {
				if (this.allocated >= this.capacity) throw new Error("HADRON_SLOT_STORE: out of slots");
				slot = this.allocated++;
			}

			this.values[slot] = value;
			this.refCount[slot] = 1;
			// <<< FIX: clear slab state on reuse, so a recycled slot can't leak old bytes
			this.slabOffsets[slot] = 0;
			this.slabLengths[slot] = 0;
			return slot;
		};
		fn.pass = (maxBatch = this.maxBatch): N => {
			this.admitIntake();

			const tracer = this.slab.tick();

			const total = this.disruptor.revolution(maxBatch, tracer);
			this.pass();
			this.slab.advance();

			return total;
		};
		fn.tick = (): N => {
			return this.slab.tick();
		};
		fn.value = (pid: PID): N => {
			// Current committed value for a PID.

			const slot = this.pidToSlot[pid];
			return slot < 0 ? 0 : this.slab.read(slot);
		};
		fn.delete = (pid: PID): V => {
			// Queue a particle deletion

			const slot = this.pidToSlot[pid];
			if (slot < 0) return;

			this.enqueueIntake(OP.DELETE, pid, slot, -1);
		};
		fn.read = (pid: PID): V => {
			// Queue a particle read / invalidation check.

			const slot = this.pidToSlot[pid];
			if (slot < 0) return;

			this.enqueueIntake(OP.READ, pid, slot, -1);
		};
		fn.write = (pid: PID, value: N): V => {
			// Queue a particle write. Nothing is applied until the WRITE pass.

			const slot = this.pidToSlot[pid];

			if (slot < 0) {
				this.slab.spawn(pid, value);
				return;
			}

			const valueOffset = this.allocF64(value);

			this.enqueueIntake(OP.WRITE, pid, slot, valueOffset);
		};
		fn.readBinary = (pid: PID): U8 | NU => {
			const slot = this.pidToSlot[pid];

			if (slot < 0) return null;

			return this.slab.readBinary(slot);
		};
		fn.writeBinary = (pid: PID, binary: Uint8Array): void => {
			const slot = this.pidToSlot[pid];

			if (slot < 0) {
				this.slab.spawnBinary(pid, binary);
				return;
			}

			this.slab.writeBinary(slot, binary);

			const ref = this.slab.binaryRef(slot);

			this.enqueueIntake(OP.WRITE, pid, slot, ref.offset, ref.length);
		};
		fn.allocBinary = (binary: Uint8Array, epoch: N): number => {
			const slot = this.slab.alloc(0);
			this.slab.writeBinary(slot, binary, epoch);
			return slot;
		};
		fn.spawnBinary = (pid: PID, binary: U8): SLOT => {
			const slot = this.slab.allocBinary(binary);

			this.pidToSlot[pid] = slot;
			this.slotToPid[slot] = pid;
			this.disruptor.bind(pid, slot);

			return slot;
		};
		fn.spawn = (pid: PID, initialValue = 0): SLOT => {
			// Allocate a resident particle slot and bind it to a PID.

			if (this.pidToSlot[pid] !== -1) {
				return this.pidToSlot[pid];
			}

			const slot = this.slab.alloc(initialValue);

			this.pidToSlot[pid] = slot;
			this.slotToPid[slot] = pid;
			this.disruptor.bind(pid, slot);

			return slot;
		};
		fn.binaryRef = (slot: N): { offset: N; length: N } => {
			return { offset: this.slabOffsets[slot], length: this.slabLengths[slot] };
		};
		fn.allocBytes = (data: Uint8Array): { offset: number; length: number } => {
			// <<< FIX: bounds check here too
			if (this.slabTail + data.length > this.slab.length) {
				throw new Error("HADRON_SLOT_STORE: slab overflow");
			}

			const offset = this.slabTail;
			this.slab.set(data, offset);
			this.slabTail += data.length;
			return { offset, length: data.length };
		};
		fn.readBytes = (offset: number, length: number): Uint8Array => {
			return this.slab.subarray(offset, offset + length);
		};
		return fn;
	})();
	// #endregion
	//                                                                                                         [DARTH VADER] ENGINE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public capacity: N;
	public slotCapacity: N;
	private readonly lane: I32;
	public ops: U8;
	public readonly posX: F32;
	public readonly posY: F32;
	public readonly val: I32;
	private readonly freePids: I32;
	private freeTail = 0;
	private laneLen = 0;
	public getX(pid: N): number { return this.posX[pid]; }
	public getY(pid: N): number { return this.posY[pid]; }  
	public getPosition(pid: N): { x: number; y: number } {
		return { x: this.posX[pid], y: this.posY[pid] };
	}
	public check_size_and_edit(x: N, y: N, pid: Pid, value: N) {
		this.posX[pid] = x; 
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.EDIT;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	} 
	public admit(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.WRITE;
		this.storage[pid] = 0; // DISRUPTOR
		const slot = this.laneLen++;
		this.lane[slot] = pid;
	}
	public edit(pid: Pid, x: N, y: N, value: N): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.EDIT;
		
	}
	public delete(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.DELETE;
	}
	public get(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.READ;
	}
	public set(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.WRITE;
		this.storage[pid] = 0; // DISRUPTOR
		const slot = this.laneLen++;
		this.lane[slot] = pid;
	}
	public effect(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.EFFECT;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	}
	public compact(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.COMPACT;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	}
	public none(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.NONE;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	}
	public drc(x: N, y: N, value: N, pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.DRC;
		if (this.laneLen < this.capacity) {
			this.lane[this.laneLen++] = pid;
		}
	}
	public getAll() {}
	public getMany() {}
	public subAll() {}
	public waitFor() {}
	public pass1(): V {
		const len = this.laneLen;
		let writeIdx = 0;
		let i = 0;
		const unrolledLimit = len - (len % 4);

		for (; i < unrolledLimit; i += 4) {
			let pid = this.lane[i];
			if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; } 
			else if (this.ops[pid] === OP.DELETE) { this.reclaim_pid(pid); } 
			else if (this.ops[pid] === OP.SEND) { this.deliver(pid); this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; } 
			else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }

			pid = this.lane[i + 1];
			if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; } 
			else if (this.ops[pid] === OP.DELETE) { this.reclaim_pid(pid); } 
			else if (this.ops[pid] === OP.SEND) { this.deliver(pid); this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; } 
			else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }

			pid = this.lane[i + 2];
			if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; } 
			else if (this.ops[pid] === OP.DELETE) { this.reclaim_pid(pid); } 
			else if (this.ops[pid] === OP.SEND) { this.deliver(pid); this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
			else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }

			pid = this.lane[i + 3];
			if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; } 
			else if (this.ops[pid] === OP.DELETE) { this.reclaim_pid(pid); }
			else if (this.ops[pid] === OP.SEND) { this.deliver(pid); this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; } 
			else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
		}

		for (; i < len; i++) {
			const pid = this.lane[i];
			if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; } 
			else if (this.ops[pid] === OP.DELETE) { this.reclaim_pid(pid); } 
			else if (this.ops[pid] === OP.SEND) { this.deliver(pid); this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; } 
			else { this.ops[pid] = OP.NONE; this.lane[writeIdx++] = pid; }
		}

		this.laneLen = writeIdx;
	}
	// ═══════════════════════════════════════════════════════════════════════════
	// FULLY WIRED DISPATCHER (NO STUBS, CALLING REAL RX_S METHODS)
	// ═══════════════════════════════════════════════════════════════════════════

	public pass(): V {
		const len = this.laneLen;
		let writeIdx = 0;
		let i = 0;
		const unrolledLimit = len - (len % 4);

		for (; i < unrolledLimit; i += 4) {
			writeIdx = this._processReactivePid(this.lane[i], writeIdx);
			writeIdx = this._processReactivePid(this.lane[i + 1], writeIdx);
			writeIdx = this._processReactivePid(this.lane[i + 2], writeIdx);
			writeIdx = this._processReactivePid(this.lane[i + 3], writeIdx);
		}

		for (; i < len; i++) {
			writeIdx = this._processReactivePid(this.lane[i], writeIdx);
		}

		this.laneLen = writeIdx;
	}

	private _processReactivePid(pid: Pid, writeIdx: N): N {
		if (pid === TRACER_PID) {
			this.val[TRACER_PID]++;
			this.lane[writeIdx++] = pid;
			return writeIdx;
		}

		const op = this.ops[pid];

		switch (op) {
			case OP.READ:
			case OP.NONE:
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;

			case OP.WRITE:
			case OP.EDIT: {
				// Push SoA spatial update into your reactive registry/signal nodes if stored per-pid
				// If you store node references in an array mapped by pid (e.g. this.nodeRegistry[pid]):
				const node = this.nodeRegistry?.[pid];
				if (node && "pendingValue" in node) {
					node.pendingValue = this.val[pid];
					node.flags = ReactiveFlags.MUTABLE | ReactiveFlags.DIRTY;
					if (node.subs !== undefined) {
						this.propagate(node.subs, !!this.runDepth);
						if (!this.getBatchDepth()) {
							this.flush();
						}
					}
				}

				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.DELETE: {
				const node = this.nodeRegistry?.[pid];
				if (node) {
					this.disposeAllDepsInReverse(node);
					if (node.subs !== undefined) {
						this.unlink(node.subs);
					}
					this.nodeRegistry[pid] = undefined;
				}
				this.reclaim_pid(pid);
				break;
			}

			case OP.EFFECT: {
				// Actually execute or run the effect node tied to this pid
				const effectNode = this.effectRegistry?.[pid];
				if (effectNode) {
					this.run(effectNode);
				} else {
					// Fallback to flushing the active queue if managed globally
					this.flush();
				}

				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.COMPUTED: {
				// Force actual re-evaluation of the computed node tied to this pid
				const computedNode = this.computedRegistry?.[pid];
				if (computedNode) {
					this.updateComputed(computedNode);
					const subs = computedNode.subs;
					if (subs !== undefined) {
						this.shallowPropagate(subs);
					}
				}

				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.EFFECTSCOPE: {
				const scopeNode = this.scopeRegistry?.[pid];
				if (scopeNode) {
					this.effectScopeOper(scopeNode);
				}
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.EXTERNAL:
			case OP.OBSERVER: {
				if (!this.getBatchDepth()) {
					this.flush();
				}
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.ASYNCEFFECT: {
				queueMicrotask(() => {
					if (this.ops[pid] !== OP.DELETE) {
						const effectNode = this.effectRegistry?.[pid];
						if (effectNode) {
							this.run(effectNode);
						} else {
							this.flush();
						}
					}
				});
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.SEND:
				this.deliver(pid);
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;

			case OP.STREAM:
			case OP.DUPLEX: {
				const val = this.val[pid];
				this.storage[pid] = val;
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
			}

			case OP.DRC: {
				if (this.val[pid] <= 0) {
					this.reclaim_pid(pid);
				} else {
					this.ops[pid] = OP.NONE;
					this.lane[writeIdx++] = pid;
				}
				break;
			}

			default:
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
				break;
		}

		return writeIdx;
	}
	private reclaim_pid(pid: Pid): V {
		this.root.governor.wake();
		this.posX[pid] = 0;
		this.posY[pid] = 0;
		this.val[pid] = 0;
		this.ops[pid] = OP.NONE;
		this.freePids[this.freeTail] = pid;
		this.freeTail = (this.freeTail + 1) % this.capacity;
	}
	get length(): N {
		return this.laneLen;
	}
	public getLoadFactor(): N {
		const activeCount = Math.max(0, this.laneLen - 1);
		const maxCapacity = this.capacity - 1;
		return activeCount / maxCapacity;
	}
	public sweepExpired(now: N): N {
		let released = 0;

		for (let pid = 0; pid < this.capacity; pid++) {
			const expiresAt = this.expiresAt[pid];

			if (expiresAt !== 0 && now > expiresAt) {
				this.expiresAt[pid] = 0;
				this.delete(pid);
				released++;
			}
		}

		return released;
	}
	public _broadcast(key: A, value: A, storageType: S, fn: S, path?: S, domain?: S, options?: A) {
		this._localChange = true;
		this._channel?.postMessage({ key, value, storageType, fn, path, domain, options });
	}
	private enqueueIntake(op: N, pid: PID, slot: SLOT, valueOffset: N, valueLength = 0): V {
		const next = (this.intakeTail + 1) % this.intakeCapacity;

		if (next === this.intakeHead) {
			throw new Error("HADRON_COLLIDER: intake full");
		}

		this.intakeOp[this.intakeTail] = op;
		this.intakePid[this.intakeTail] = pid;
		this.intakeSlot[this.intakeTail] = slot;
		this.intakeValueOffset[this.intakeTail] = valueOffset;
		this.intakeValueLength[this.intakeTail] = valueLength;

		this.intakeTail = next;
	}
	private admitIntake(): V {
		while (this.intakeHead !== this.intakeTail) {
			const op = this.intakeOp[this.intakeHead];
			const pid = this.intakePid[this.intakeHead];
			const slot = this.intakeSlot[this.intakeHead];
			const valueOffset = this.intakeValueOffset[this.intakeHead];
			const valueLength = this.intakeValueLength[this.intakeHead];

			this.disruptor.emitRaw(op, pid, slot, valueOffset, valueLength);

			this.intakeHead = (this.intakeHead + 1) % this.intakeCapacity;
		}
	}
	// #endregion
	//                                                                                  [DARTH VADER] TRANSPORT FNS FOR SOA ADAPTER
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public send(p: U, k: A, s: S, m: S, r: S, ttl?: N): P<U> {
		const msgId = this.hub._byteid();
		const msg: SIG_BUS_Message = { id: msgId, r, m, s, k, p: { p }, ttl, ts: Date.now(), _ack: false, binary: false, transport: "soa" };
		// The bus owns reply correlation, exactly like your current send().
		const reply = this.hub.awaitReply(String(msgId), ttl);
		const pid = this.hub.existing(k, s, m, r);
		const target = pid >= 0 ? pid : this.hub.alloc(k, s, m, r);
		const frame = this.hub.binary_codec.encode(msg);
		const ref = this.slab.allocBytes(frame);
		// Reusing the SOA particle fields for the frame location:
		// posX = slab offset
		// posY = frame length
		this.admit(ref.offset, ref.length, 0, target);
		// The SOA lane is the transport.
		// This is the in-flight message, not a cache.
		this.ops[target] = OP.SEND;
		return reply;
	}
	public sub(handler: (pid: number, value: number) => void, k: A, s: S, m: S, r: S): () => V {
		const pid = this.hub.alloc(k, s, m, r);
		return this.hub.pid_codec.subscribe(pid, handler);
	}
	private deliver(pid: Pid): V {
		// posX = slab offset, posY = frame length
		const offset = this.posX[pid];
		const length = this.posY[pid];

		const frame = this.slab.readBytes(offset, length);

		const msg = this.hub.binary_codec.decode(frame);

		// Right now: notify the subscribed PID.
		// Later: this is also where bus.dispatch(msg) plugs in.
		if (!msg) return;
		this.hub.pid_codec.notifyPid(pid, this.val[pid]);
		void this.hub.dispatch(msg);
	}
	// #endregion
	//                                                                                              [DARTH VADER] INSTALL OPS LANES
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public install = (() => {
		const fn = (): V => {
			this.install.writePass();
			this.install.readPass();
			this.install.deletePass();
			this.install.sendPass();
			this.install.subPass();
			this.install.compactPass();
			this.install.EFFECTPass();
			this.install.NONEPass();
			this.install.DUPLEXPass();
			this.install.STREAMPass();
			this.install.DRCPass();
		};
		fn.EFFECTPass = () => {};
		fn.NONEPass = () => {};
		fn.DUPLEXPass = () => {};
		fn.STREAMPass = () => {};
		fn.DRCPass = () => {};
		fn.subPass = (): V => {
			this.root.sub(OP.SUB, (ring, from, to, tracer, d) => {
				for (let seq = from; seq < to; seq++) {
					// Subscription registration / removal already happened
					// synchronously through this.bus.sub().
					// This lane exists to keep SUB lifecycle ordered
					// with the rest of the disruptor stream.
				}
			});
		};
		fn.sendPass = (): void => {
			this.disruptor.on(OP.SEND, (ring, from, to, tracer, d) => {
				for (let seq = from; seq < to; seq++) {
					const base = (seq & d.mask) * REC;

					const frameOffset = ring[base + F_A];
					const frameLength = ring[base + F_B];

					const frame = this.slab_storage.readBytes(frameOffset, frameLength);

					const msg = this.hub.binary_codec.decode(frame) as SIG_BUS_Message;

					if (!msg) continue;

					void this.hub.dispatch(msg);
				}
			});
		};
		fn.compactPass = (): V => {
			this.disruptor.on(OP.COMPACT, (_ring, _from, _to, _epoch, _d) => {
				// Reserved for bulk free-list repair, page reclamation,
				// or collider-level defragmentation.
			});
		};
		fn.deletePass = (): V => {
			this.disruptor.on(OP.DELETE, (ring, from, to, _epoch, d) => {
				for (let seq = from; seq < to; seq++) {
					const base = (seq & d.mask) * 8;

					const pid = ring[base + 1];
					const slot = ring[base + 2];

					if (slot < 0) continue;

					this.slab_storage.release(slot);

					this.pidToSlot[pid] = -1;
					this.slotToPid[slot] = -1;
					d.unbind(pid);

					this.reclaim_pid(pid); // <<< clears lane/ops/posX/posY/val, returns pid to freePids
				}
			});
		};
		fn.readPass = (): V => {
			this.disruptor.on(OP.READ, (ring, from, to, epoch, d) => {
				for (let seq = from; seq < to; seq++) {
					const base = (seq & d.mask) * 8;

					const pid = ring[base + 1];
					const slot = ring[base + 2];

					if (slot < 0) continue;

					const changed = this.slab_storage.version(slot) === epoch;
				}
			});
		};
		fn.writePass = (): V => {
			this.disruptor.on(OP.WRITE, (ring, from, to, epoch, d) => {
				for (let seq = from; seq < to; seq++) {
					const base = (seq & d.mask) * 8;

					const slot = ring[base + 2];
					const length = ring[base + 4]; // F_B — valueLength

					if (length > 0) {
						// Binary record: bytes already resident (disruptor pool or slab).
						// Commit the version tick only — do NOT re-write bytes.
						this.slab_storage.write(slot, 0, epoch);
					} else {
						const valueOffset = ring[base + 3];
						this.slab_storage.write(slot, d.f64At(valueOffset), epoch);
					}
				}
			});
		};
		return fn;
	})();
	// #endregion
	//                                                                                                                       CONSTS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "__SITH_SAURON__";
	public k: A = "__BINARY__";
	private stringToId = new Map<string, number>();
	private idToString: string[] = [];
	private addressToPidMap = new Map<string, number>();
	public nextPid = 1; // SoA Memory Lanes (Indexed by PID)
	public readonly typeMap: Uint8Array; // 0 = Single String, 1 = Multi-Channel
	public readonly channelMap: Int32Array; // r channel
	public readonly subChannelMap: Int32Array; // m channel
	public readonly tripChannelMap: Int32Array; // s channel
	public readonly keyMap: Int32Array; // k channel / single key string ID
	public readonly multiFlyweight = { r: "", m: "", s: "", k: "" }; // Zero-alloc flyweight for multi-channel decoding
	private metadataRegistry: SithMetadataEntry[] = [];
	public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
	public store = (() => {
		const HEADER_SIZE = 32;
		const fn = () => {};
		fn.encode = (r: string, m: string, s: string, k: string, payload: Uint8Array): Uint8Array => {
			const enc = new TextEncoder();
			const rBytes = enc.encode(r);
			const mBytes = enc.encode(m);
			const sBytes = enc.encode(s);
			const kBytes = enc.encode(k);

			const rOffset = HEADER_SIZE;
			const mOffset = rOffset + rBytes.length;
			const sOffset = mOffset + mBytes.length;
			const kOffset = sOffset + sBytes.length;
			const payloadOffset = kOffset + kBytes.length;
			const totalSize = payloadOffset + payload.length;

			const buffer = new Uint8Array(totalSize);
			const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

			view.setUint32(0, 0x41544f4d, true); // Magic "ATOM"
			view.setUint32(4, rOffset, true);
			view.setUint32(8, mOffset, true);
			view.setUint32(12, sOffset, true);
			view.setUint32(16, kOffset, true);
			view.setUint32(20, payloadOffset, true);

			buffer.set(rBytes, rOffset);
			buffer.set(mBytes, mOffset);
			buffer.set(sBytes, sOffset);
			buffer.set(kBytes, kOffset);
			buffer.set(payload, payloadOffset);

			return buffer;
		};
		fn.decode = (atomBuffer: Uint8Array) => {
			const view = new DataView(atomBuffer.buffer, atomBuffer.byteOffset, atomBuffer.byteLength);
			const dec = new TextDecoder();

			const rOff = view.getUint32(4, true);
			const mOff = view.getUint32(8, true);
			const sOff = view.getUint32(12, true);
			const kOff = view.getUint32(16, true);
			const pOff = view.getUint32(20, true);

			return {
				r: dec.decode(atomBuffer.subarray(rOff, mOff)),
				m: dec.decode(atomBuffer.subarray(mOff, sOff)),
				s: dec.decode(atomBuffer.subarray(sOff, kOff)),
				k: dec.decode(atomBuffer.subarray(kOff, pOff)),
				payload: atomBuffer.subarray(pOff),
			};
		}; // Individual slot decoders (O(1) direct address jump per channel)
		fn.decodeR = (atomBuffer: Uint8Array): string => {
			const view = new DataView(atomBuffer.buffer, atomBuffer.byteOffset, atomBuffer.byteLength);
			return new TextDecoder().decode(atomBuffer.subarray(view.getUint32(4, true), view.getUint32(8, true)));
		};
		fn.decodeM = (atomBuffer: Uint8Array): string => {
			const view = new DataView(atomBuffer.buffer, atomBuffer.byteOffset, atomBuffer.byteLength);
			return new TextDecoder().decode(atomBuffer.subarray(view.getUint32(8, true), view.getUint32(12, true)));
		};
		fn.decodeS = (atomBuffer: Uint8Array): string => {
			const view = new DataView(atomBuffer.buffer, atomBuffer.byteOffset, atomBuffer.byteLength);
			return new TextDecoder().decode(atomBuffer.subarray(view.getUint32(12, true), view.getUint32(16, true)));
		};
		fn.decodeK = (atomBuffer: Uint8Array): string => {
			const view = new DataView(atomBuffer.buffer, atomBuffer.byteOffset, atomBuffer.byteLength);
			return new TextDecoder().decode(atomBuffer.subarray(view.getUint32(16, true), view.getUint32(20, true)));
		};
		fn.decodePayload = (atomBuffer: Uint8Array): Uint8Array => {
			const view = new DataView(atomBuffer.buffer, atomBuffer.byteOffset, atomBuffer.byteLength);
			return atomBuffer.subarray(view.getUint32(20, true));
		};
		fn.decodeSingle = fn.decodeK; // alias for existing reference
		/**
			 * fn.decodeMulti=(pid: number): typeof this.multiFlyweight =>{// Decode PID back into 3-4 Channeqw22 (zero-alloc flyweight mutation).
			this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
			this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
			this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
			this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
			return this.multiFlyweight;
		}
		fn.decodeSingle=(pid: number): string=> {return this.idToString[this.keyMap[pid]] ?? "";}
		fn.intern=(val: string): number=> {
			let id = this.stringToId.get(val);
			if (id === undefined) {
				id = this.idToString.length;
				this.stringToId.set(val, id);
				this.idToString[id] = val;
			}
			return id;
		}
			 */
		/**
		 * PURE BINARY REPLACEMENT FOR getMetadata:
		 * Resolves the 4-channel scope via CVRT_CHS_AND_PIDS to get a numeric pid,
		 * then indexes directly into a flat array with zero map traversal overhead.
		 */
		fn.getMetadata = (pid: number): SithMetadataEntry => {
			let meta = this.metadataRegistry[pid];
			if (!meta) {
				meta = {
					_listeners: new Map<N, M<A, Set<(value: A) => V>>>(),
					_persistent: new Map<N, M<A, A>>(),
					_persistKeys: new Map<N, M<A>>(),
					_batches: new Set<string | symbol>(),
					_batchQueue: new Map<string | symbol, A>(),
					_batchOpts: new Map<string | symbol, { triggerRender?: boolean }>(),
					_globalListeners: new Set<(key: string | symbol, value: A) => void>(),
					_global_subscribers: new Map<string, Set<() => void>>(),
					_waiters: new Map<string | symbol, Set<(value: A) => void>>(),
					_sys: new Map<string, A>(),
					_sealed: new Set<string>(),
					_frozen: new Set<string>(),
					_keyContracts: new Map<string, Set<string>>(),
					_conflicts: new Map<string, A>(),
					_conflictStrategies: new Map<string, "latest" | "fail" | "accumulate">(),
					_causal: new Map<string, A[]>(),
					_history: new Map<string | symbol, A[]>(),
					_scopes: new Map<number, M<any, any>>(),
					_msgHandlers: new Map<string, Set<(p: A) => void>>(),
					_lastWriteSource: new Map<string, string>(),
					_mainMap: new Map<A, string>(),
					_tupleCache: new Map<string, string>(),
					_weakMap: new WeakMap<object, string>(),
					_boundaries: new WeakMap<object, string>(),
				};
				this.metadataRegistry[pid] = meta;
			}
			return meta;
		}; // Input Style A: Register a SINGLE string key -> returns a PID.
		fn.getSingleMap = (key: string): number => {
			let pid = this.addressToPidMap.get(key);
			if (pid === undefined) {
				pid = this.nextPid++;
				this.addressToPidMap.set(key, pid);

				this.typeMap[pid] = 0; // Mark as Single String
				this.keyMap[pid] = this.store.intern(key);
			}
			return pid;
		}; // Input Style B: Register 3-4 channel strings -> returns a PID.
		// Decode PID back into a Single String (if registered that way).
		fn.getNestedMap = (r: string, m: string, s: string, k = ""): number => {
			const addressKey = `${r}:${m}:${s}:${k}`;
			let pid = this.addressToPidMap.get(addressKey);
			if (pid === undefined) {
				pid = this.nextPid++;
				this.addressToPidMap.set(addressKey, pid);

				this.typeMap[pid] = 1; // Mark as Multi-Channel
				this.channelMap[pid] = this.store.intern(r);
				this.subChannelMap[pid] = this.store.intern(m);
				this.tripChannelMap[pid] = this.store.intern(s);
				this.keyMap[pid] = this.store.intern(k);
			}
			return pid;
		};
		return fn;
	})();
	// #endregion
	//                                                                                                     COLLIDER OUTPUT BOUNDARY
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	// This is where completed particle cycles leave the collider. It must NOT be a reactive system, transport, or bus. The bus/reactive layer consumes these events.
	protected onParticleChanged(pid: PID, slot: SLOT, epoch: N): V {
		// Intentionally minimal.
		// Your bus / reactive process / post office subscribes here.
	}
	// #endregion
	//                                                                                                         [DARTH VADER] CODECS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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
	private topicToIdMap: Map<string, number> = new Map();
	private idToTopicMap: Map<number, string> = new Map();
	private headerBuffer = new ArrayBuffer(4); // Reusable scratch buffer & DataView for header manipulations (Zero GC)
	private headerView = new DataView(this.headerBuffer);
	private headerBytes = new Uint8Array(this.headerBuffer);
	private bytes: U8;
	public js_codec = (() => {
		let bytes: U8;
		let view: DataView;
		let tail = 0;
		let strings = new Map<S, N>();
		let stringBytes: U8[] = [];
		let refs = new Map<object, N>();
		let nextRef = 0;
		const fn = () => {};
		function need(count: N): V {
			if (tail + count <= bytes.length) return;

			let size = bytes.length;

			while (size < tail + count) size *= 2;

			const next = new Uint8Array(size);
			next.set(bytes);

			bytes = next;
			view = new DataView(next.buffer);
		}
		function u8(value: N): V {
			need(1);
			bytes[tail++] = value;
		}
		function i32(value: N): V {
			need(4);
			view.setInt32(tail, value, true);
			tail += 4;
		}
		function f64(value: N): V {
			need(8);
			view.setFloat64(tail, value, true);
			tail += 8;
		}
		function writeValue(value: U): V {
			if (value === undefined) return u8(TAG.UNDEFINED);
			if (value === null) return u8(TAG.NULL);
			if (value === false) return u8(TAG.FALSE);
			if (value === true) return u8(TAG.TRUE);
			const type = typeof value;
			if (type === "number") {
				const n = value as N;
				if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
					u8(TAG.INT32);
					return i32(n);
				}

				u8(TAG.FLOAT64);
				return f64(n);
			}

			if (type === "string") {
				return writeString(value as S);
			}

			if (Array.isArray(value)) {
				return writeArray(value);
			}

			if (value instanceof Uint8Array) {
				return writeBytes(value);
			}

			if (type === "object") {
				return writeObject(value as object);
			}

			throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
		}
		function writeString(value: S): V {
			const encoded = new TextEncoder().encode(value);

			u8(TAG.STRING);
			i32(encoded.length);
			need(encoded.length);
			bytes.set(encoded, tail);
			tail += encoded.length;
		}
		function writeArray(value: U[]): V {
			const ref = beginRef(value);

			u8(TAG.ARRAY);
			i32(value.length);

			for (let i = 0; i < value.length; i++) {
				writeValue(value[i]);
			}

			endRef(ref);
		}
		function writeBytes(value: U8): V {
			u8(TAG.UINT8ARRAY);
			i32(value.length);
			need(value.length);
			bytes.set(value, tail);
			tail += value.length;
		}
		function writeObject(value: object): V {
			const ref = beginRef(value);
			const keys = Object.keys(value);

			u8(TAG.OBJECT);
			i32(keys.length);

			for (const key of keys) {
				writeString(key);
				writeValue((value as any)[key]);
			}

			endRef(ref);
		}
		function beginRef(value: object): N {
			const existing = refs.get(value);

			if (existing !== undefined) {
				u8(TAG.REF);
				i32(existing);
				return -1;
			}

			const ref = nextRef++;
			refs.set(value, ref);

			return ref;
		}
		function endRef(ref: N): V {
			void ref;
		} // Reserved for future back-reference patching.
		function readU8(cursor: { at: N }): N {
			return bytes[cursor.at++];
		}
		function readI32(cursor: { at: N }): N {
			const value = view.getInt32(cursor.at, true);
			cursor.at += 4;

			return value;
		}
		function readF64(cursor: { at: N }): N {
			const value = view.getFloat64(cursor.at, true);
			cursor.at += 8;

			return value;
		}
		function readValue(cursor: { at: N }): U {
			const tag = readU8(cursor);

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
					return readI32(cursor);

				case TAG.FLOAT64:
					return readF64(cursor);

				case TAG.STRING:
					return readString(cursor);

				case TAG.ARRAY:
					return readArray(cursor);

				case TAG.OBJECT:
					return readObject(cursor);

				case TAG.UINT8ARRAY:
					return readBytes(cursor);

				default:
					throw new Error(`HC_BINARY_CODEC: unknown tag ${tag}`);
			}
		}
		function readString(cursor: { at: N }): S {
			const length = readI32(cursor);
			bytes = bytes.subarray(cursor.at, cursor.at + length);

			cursor.at += length;

			return new TextDecoder().decode(bytes);
		}
		function readArray(cursor: { at: N }): U[] {
			const length = readI32(cursor);
			const out: U[] = new Array(length);

			for (let i = 0; i < length; i++) {
				out[i] = readValue(cursor);
			}

			return out;
		}
		function readObject(cursor: { at: N }): Record<S, U> {
			const length = readI32(cursor);
			const out: Record<S, U> = {};

			for (let i = 0; i < length; i++) {
				const key = readString(cursor);
				out[key] = readValue(cursor);
			}

			return out;
		}
		function readBytes(cursor: { at: N }): Uint8Array {
			const length = readI32(cursor);
			const out = bytes.slice(cursor.at, cursor.at + length);

			cursor.at += length;

			return out;
		}
		fn.encode = (value: U): U8 => {
			tail = 0;
			strings.clear();
			stringBytes.length = 0;
			refs.clear();
			nextRef = 0;

			writeValue(value);

			return bytes.subarray(0, tail);
		};
		fn.decode = (binary: U8): U => {
			const savedBytes = bytes;
			const savedView = view;
			const savedTail = tail;

			try {
				bytes = binary;
				view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
				tail = binary.length;

				refs.clear();
				nextRef = 0;

				const cursor = { at: 0 };

				return readValue(cursor);
			} finally {
				// Restore the encode-side buffer so the next encode()
				// never writes into the disruptor pool.
				bytes = savedBytes;
				view = savedView;
				tail = savedTail;
			}
		};
		return fn;
	})();
	public cbor_codec = (() => {
		const self = this;
		const fn = () => {};
		function ensureAvailable(buf: Uint8Array, pos: number, length: number): void {
			if (pos < 0 || length < 0 || pos + length > buf.length) throw new Error("CBOR: truncated buffer");
		}
		function readUint(buf: Uint8Array, pos: number): [number, number] {
			ensureAvailable(buf, pos, 1);
			const info = buf[pos] & 0x1f;
			if (info <= 23) return [info, pos + 1];
			if (info === 24) {
				ensureAvailable(buf, pos + 1, 1);
				return [buf[pos + 1], pos + 2];
			}
			if (info === 25) {
				ensureAvailable(buf, pos + 1, 2);
				return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(0, false), pos + 3];
			}
			if (info === 26) {
				ensureAvailable(buf, pos + 1, 4);
				return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(0, false), pos + 5];
			}
			if (info === 27) {
				ensureAvailable(buf, pos + 1, 8);
				const value = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8).getBigUint64(0, false);
				if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER");
				return [Number(value), pos + 9];
			}
			throw new Error(`CBOR: unsupported additional info ${info}`);
		}
		function encodeUint64(majorShifted: number, n: bigint): Uint8Array {
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
		function encode(value: CBORValue): Uint8Array {
			const parts: Uint8Array[] = [];
			encodeValue(value, parts);
			const total = parts.reduce((n, p) => n + p.length, 0);
			const out = new Uint8Array(total);
			let offset = 0;
			for (const p of parts) {
				out.set(p, offset);
				offset += p.length;
			}
			return out;
		}
		function encodeValue(v: CBORValue, out: Uint8Array[]): void {
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
					if (v >= 0) out.push(encodeUint64(0x00, BigInt(v)));
					else out.push(encodeUint64(0x20, BigInt(-1 - v)));
				} else {
					const b = new Uint8Array(9);
					b[0] = 0xfb;
					new DataView(b.buffer).setFloat64(1, v, false);
					out.push(b);
				}
				return;
			}

			if (typeof v === "string") {
				const encoded = self.js_codec.encode(v);
				out.push(encodeUint(0x60, encoded.length));
				out.push(encoded);
				return;
			}
			if (v instanceof Uint8Array) {
				out.push(encodeUint(0x40, v.length));
				out.push(v);
				return;
			}
			if (Array.isArray(v)) {
				out.push(encodeUint(0x80, v.length));
				for (const item of v) {
					encodeValue(item, out);
				}
				return;
			}
			if (typeof v === "object") {
				const proto = Object.getPrototypeOf(v);
				if (proto !== Object.prototype && proto !== null) throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`);
				const keys = Object.keys(v);
				out.push(encodeUint(0xa0, keys.length));
				for (const key of keys) {
					encodeValue(key, out);
					encodeValue(v[key], out);
				}
				return;
			}
			throw new Error(`CBOR: unsupported value type ${typeof v}`);
		}
		function encodeUint(majorShifted: number, n: number): Uint8Array {
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
		function decode(buf: Uint8Array): CBORValue {
			const [value, end] = decodeValue(buf, 0);
			if (end !== buf.length) {
				throw new Error("CBOR: trailing bytes");
			}
			return value;
		}
		type CBORValue = U | null | B | N | S | Uint8Array | CBORValue[] | { [k: S]: CBORValue };
		function decodeValue(buf: Uint8Array, pos: number): [CBORValue, number] {
			ensureAvailable(buf, pos, 1);
			const first = buf[pos];
			const major = first >> 5;
			const info = first & 0x1f;

			if (major === 7) {
				if (info === 20) return [false, pos + 1];
				if (info === 21) return [true, pos + 1];
				if (info === 22) return [null, pos + 1];
				if (info === 23) return [undefined, pos + 1];
				if (info === 27) {
					ensureAvailable(buf, pos + 1, 8);
					const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8);
					return [dv.getFloat64(0, false), pos + 9];
				}
			}
			const [n, nextPos] = readUint(buf, pos);
			if (major === 0) return [n, nextPos];
			if (major === 1) return [-1 - n, nextPos];
			if (major === 2) {
				ensureAvailable(buf, nextPos, n);
				const bytes = buf.slice(nextPos, nextPos + n);
				return [bytes, nextPos + n];
			}
			if (major === 3) {
				ensureAvailable(buf, nextPos, n);
				const bytes = buf.slice(nextPos, nextPos + n);
				return [self.js_codec.decode(bytes), nextPos + n];
			}
			if (major === 4) {
				const arr: CBORValue[] = [];
				let p = nextPos;
				for (let i = 0; i < n; i++) {
					const [item, np] = decodeValue(buf, p);
					arr.push(item);
					p = np;
				}
				return [arr, p];
			}
			if (major === 5) {
				const obj: Record<string, CBORValue> = {};
				let p = nextPos;
				for (let i = 0; i < n; i++) {
					const [k, kp] = decodeValue(buf, p);
					const [v, vp] = decodeValue(buf, kp);
					obj[k as string] = v;
					p = vp;
				}
				return [obj, p];
			}
			throw new Error(`CBOR: unsupported major type ${major}`);
		}
		fn.encode_msg = (msg: any): Uint8Array => {
			// Instead of leaking a raw object, encode an explicit fallback/error frame
			if (msg instanceof Uint8Array) return msg; // If it's already an encoded Uint8Array, don't re-encode it
			try {
				return encode(msg);
			} catch (err) {
				console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err);
				return encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" });
			}
		};
		fn.decode_msg = (raw: any): any => {
			// Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
			if (!(raw instanceof Uint8Array) && !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))) {
				console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
				return null;
			}
			try {
				const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array((raw as any).buffer, (raw as any).byteOffset, (raw as any).length);
				return decode(uint8);
			} catch (err) {
				console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
				return null;
			}
		};
		return fn;
	})();
	public serializer = (() => {
		const fn = () => {};
		fn.FLYWEIGHT_FRAME = {
			topicId: 0,
			topicName: undefined,
			p: new Uint8Array(0),
		} as DecodedFrame;
		fn.hashTopic = (topic: string): number => {
			let hash = 0x811c9dc5;
			for (let i = 0; i < topic.length; i++) {
				hash ^= topic.charCodeAt(i);
				hash = Math.imul(hash, 0x01000193) >>> 0;
			}
			return hash;
		};
		fn.getTopicId = (topic: string): number => {
			const existingId = this.topicToIdMap.get(topic);
			if (existingId !== undefined) {
				return existingId;
			}

			const topicId = this.serializer.hashTopic(topic);
			const collisionTopic = this.idToTopicMap.get(topicId);
			if (collisionTopic !== undefined && collisionTopic !== topic) {
				throw new Error(`Topic hash collision detected between "${topic}" and "${collisionTopic}" (ID: ${topicId})`);
			}

			this.topicToIdMap.set(topic, topicId);
			this.idToTopicMap.set(topicId, topic);
			return topicId;
		};
		fn.getTopicName = (topicId: number): string | undefined => {
			return this.idToTopicMap.get(topicId);
		};
		fn.registerTopics = (topics: string[]): void => {
			for (const topic of topics) {
				this.serializer.getTopicId(topic);
			}
		};
		fn.resetRegistry = (): void => {
			this.topicToIdMap.clear();
			this.idToTopicMap.clear();
		};
		fn.encodeFrame = (topic: string, p: Uint8Array | string | Record<string, unknown>): Uint8Array => {
			const topicId = this.serializer.getTopicId(topic);

			let payloadBytes: Uint8Array;
			if (p instanceof Uint8Array) {
				payloadBytes = p;
			} else if (typeof p === "string") {
				payloadBytes = this.js_codec.encode(p);
			} else {
				payloadBytes = this.js_codec.encode(JSON.stringify(p));
			}

			const frame = new Uint8Array(4 + payloadBytes.byteLength);

			// 1. Write Uint32 Topic ID (Little-Endian) via byte copies (Zero DataView allocation)
			this.headerView.setUint32(0, topicId, true);
			frame.set(this.headerBytes, 0);

			// 2. Copy Payload Bytes
			frame.set(payloadBytes, 4);

			return frame;
		};
		fn.decodeFrame = (frameBytes: Uint8Array): DecodedFrame => {
			if (frameBytes.byteLength < 4) {
				throw new Error("Invalid frame: buffer smaller than minimum header size (4 bytes).");
			}

			// Direct byte copy into static DataView to read Uint32 safely without memory allocation
			this.headerBytes[0] = frameBytes[0];
			this.headerBytes[1] = frameBytes[1];
			this.headerBytes[2] = frameBytes[2];
			this.headerBytes[3] = frameBytes[3];

			const topicId = this.headerView.getUint32(0, true);

			this.serializer.FLYWEIGHT_FRAME.topicId = topicId;
			this.serializer.FLYWEIGHT_FRAME.topicName = this.serializer.getTopicName(topicId);
			this.serializer.FLYWEIGHT_FRAME.p = frameBytes.subarray(4); // Zero-copy view

			return this.serializer.FLYWEIGHT_FRAME;
		};
		fn.createSharedDepotBuffer = (capacityBytes: number): SharedArrayBuffer => {
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
		};
		fn.mapDepotBuffer = (sab: SharedArrayBuffer): HeaderViews => {
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
		};
		fn.validateHeader = (sab: SharedArrayBuffer): boolean => {
			if (sab.byteLength < this.HEADER_SIZE_BYTES) return false;

			const headerInt32 = new Int32Array(sab, 0, 4);
			const capacity = Atomics.load(headerInt32, this.HEADER_IDX_CAPACITY);

			// Buffer length must equal 64 bytes header + registered capacity
			return sab.byteLength === this.HEADER_SIZE_BYTES + capacity;
		};
		fn.calculateFrameFootprint = (payloadSizeBytes: number): number => {
			const rawEnvelopeBytes = 4 + payloadSizeBytes;
			return Math.ceil(rawEnvelopeBytes / 4) * 4;
		};

		return fn;
	})();
	// #endregion
}
// #endregion
//                                                                                                               [GALEN_MAREK]  BUS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class GALEN_MAREK {
	// #region INTERNALS
	public root: SIGNALS;
	readonly capacity: N;
	readonly mask: N;
	readonly pidCapacity: N;
	readonly schedule: readonly N[];
	readonly shared: B;
	private readonly rings: I32[] = [];
	/*
	  Per-lane shared control block layout:
  
	  0 nextClaim
	  1 gatingSequence  — consumer progress
	  2 cachedAvailableSequence
	  3 publishedHighest
	  4 fullStalls
	  5 claimed
	  6 published
	  7 consumed
	  8 batches
	  9 maxBatch
	 */
	private static readonly CTRL = 16;
	private readonly ctrl: I32;
	private readonly available: I32[] = [];
	private readonly clock: I32;
	private readonly phaseIndex: I32;
	private readonly running: I32;
	private readonly pidToSlot: I32;
	private readonly dirty: U32;
	private readonly f64: F64;
	private readonly u32: U32;
	private readonly i32: I32;
	private readonly bytes: U8;
	private f64Tail = 0;
	private u32Tail = 0;
	private i32Tail = 0;
	private byteTail = 0;
	private readonly maxBatch: N;
	private readonly wait: WaitStrategy;
	private handlers: AR< | ((ring: I32, from: N, to: N, epoch: N, d: GALEN_MAREK ) => V) | NU > = [];
	constructor(root: SIGNALS, options: HADRON_OPTIONS = {}) {
		this.root = root
	  	const capacity = options.capacity ?? 1 << 18;
		const pidCapacity = options.pidCapacity ?? 1 << 20;
	
		if (!pow2(capacity)) throw new Error("capacity must be a power of two");
		if (!pow2(pidCapacity)) throw new Error("pidCapacity must be a power of two");
	
		this.capacity = capacity;
		this.mask = capacity - 1;
		this.pidCapacity = pidCapacity;
	
		this.schedule = options.schedule ?? DEFAULT_SCHEDULE;
		this.shared = options.shared ?? typeof SharedArrayBuffer !== "undefined";
	
		this.maxBatch = options.maxBatch ?? 8192;
		this.wait = options.wait ?? "yield";
	
		const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;
	
		const laneCount = this.schedule.length;
	
		this.rings = Array.from(
			{ length: laneCount },
			() => new Int32Array(new AB(capacity * REC * 4))
		);
	
		this.ctrl = new Int32Array(new AB(laneCount * GALEN_MAREK.CTRL * 4));
	
		// Availability buffer uses one Int32 per ring slot.
		this.available = Array.from(
			{ length: laneCount },
			() => new Int32Array(new AB(capacity * 4))
		);
	
		this.clock = new Int32Array(new AB(4));
		this.phaseIndex = new Int32Array(new AB(4));
		this.running = new Int32Array(new AB(4));
	
		this.pidToSlot = new Int32Array(new AB(pidCapacity * 4));
		this.pidToSlot.fill(-1);
	
		this.dirty = new Uint32Array(new AB((pidCapacity >>> 5) * 4));
	
		this.f64 = new Float64Array(new AB((options.f64Capacity ?? 1 << 18) * 8));
		this.u32 = new Uint32Array(new AB((options.u32Capacity ?? 1 << 18) * 4));
		this.i32 = new Int32Array(new AB((options.i32Capacity ?? 1 << 18) * 4));
		this.bytes = new Uint8Array(new AB(options.byteCapacity ?? 1 << 20));
		this.r = root.r
		this.m = root.m
		this.s = root.s
	}
/*  ======================================================================
	 HANDLERS
	====================================================================== */
	public sub( op: OpKind, handler: ( ring: I32, from: N, to: N, epoch: N, d: GALEN_MAREK ) => V  ): this {
	  this.handlers[op] = handler;
	  return this;
	}
/*  ======================================================================
	 MULTI-PRODUCER CLAIM / PUBLISH
	 Claim the next sequence for a lane. This is a true multi-producer sequencer: - CAS reserves a unique sequence. - The availability buffer marks that exact sequence published. - Consumers never rely on a single global “highest published” value.
	====================================================================== */
	private ctrlAt(lane: N, field: number): number { return lane * GALEN_MAREK.CTRL + field;  }
	public claim(lane: number): number {
	  const next = this.ctrlAt(lane, 0);
	  const stall = this.ctrlAt(lane, 4);
	  const claimed = this.ctrlAt(lane, 5);
  
	  let spin = 0;
  
	  for (;;) {
		const seq = Atomics.load(this.ctrl, next);
		const wrap = seq - this.capacity;
		const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
  
		if (gate > wrap) {
		  if (Atomics.compareExchange(this.ctrl, next, seq, seq + 1) === seq) {
			Atomics.add(this.ctrl, claimed, 1);
			return seq;
		  }
		  continue;
		}
  
		Atomics.add(this.ctrl, stall, 1);
  
		if (this.wait === "spin") {
		  spin++;
		  continue;
		}
  
		if (this.wait === "yield") {
		  this.yieldNow();
		  continue;
		}
  
		Atomics.wait(this.ctrl, next, seq, 1);
		spin = 0;
	  }
	}// Publish a claimed record. The record must be fully written BEFORE this call. The availability buffer is the publication barrier.
	public publish(lane: number, seq: number): void {
	  const avail = this.available[lane];
	  Atomics.store(avail, seq & this.mask, seq + 1);
  
	  const pub = this.ctrlAt(lane, 6);
	  Atomics.add(this.ctrl, pub, 1);
  
	  const waiting = this.ctrlAt(lane, 0);
	  Atomics.notify(this.ctrl, waiting);
	}// One-shot enqueue. Returns: >= 0 — published sequence  -1  — nonblocking mode and lane was full
	public emit( op: OpKind, pid: N, slot: N, a = 0, b = 0, c = 0, ext = 0, block = true  ): N {
	  const lane = this.laneFor(op);
	  const ring = this.rings[lane];
  
	  if (!block && this.isFull(lane)) return -1;
  
	  const seq = this.claim(lane);
	  const base = (seq & this.mask) * REC;
	  const epoch = Atomics.load(this.clock, 0);
  
	  ring[base + F_OP] = op;
	  ring[base + F_PID] = pid;
	  ring[base + F_SLOT] = slot;
	  ring[base + F_A] = a;
	  ring[base + F_B] = b;
	  ring[base + F_C] = c;
	  ring[base + F_EXT] = ext;
  
	  this.publish(lane, seq);
	  return seq;
	}
/*  ======================================================================
	 PID / SLOT / DIRTY
	====================================================================== */
	public bind(pid: N, slot: N): V { this.pidToSlot[pid] = slot;  }
	public unbind(pid: N): V { this.pidToSlot[pid] = -1;  }
	public slot(pid: N): N { return this.pidToSlot[pid];  }
	public dirtyMark(pid: N): V { this.dirty[pid >>> 5] |= 1 << (pid & 31);  }
	public dirtyPeek(pid: N): B { return (this.dirty[pid >>> 5] & (1 << (pid & 31))) !== 0;  }
	public dirtyTake(pid: N): B {
	  const w = pid >>> 5;
	  const bit = 1 << (pid & 31);
	  const word = this.dirty[w];
  
	  if ((word & bit) === 0) return false;
  
	  this.dirty[w] = word & ~bit;
	  return true;
	}
/*  ======================================================================
	 SOA ARENAS
	====================================================================== */
	public allocF64(value: N): N {
	  const at = this.f64Tail++;
	  this.f64[at] = value;
	  return at;
	}
	public allocU32(value: N): N {
	  const at = this.u32Tail++;
	  this.u32[at] = value;
	  return at;
	}
	public allocI32(value: N): N {
	  const at = this.i32Tail++;
	  this.i32[at] = value;
	  return at;
	}
	public allocBytes(data: U8): N {
	  const at = this.byteTail;
	  this.bytes.set(data, at);
	  this.byteTail += data.length;
	  return at;
	}
	public f64At(offset: N): N {    return this.f64[offset];  }
	public u32At(offset: N): N {    return this.u32[offset];  }
	public i32At(offset: N): N {    return this.i32[offset];  }
	public bytesAt(offset: N, length: N): U8 {    return this.bytes.subarray(offset, offset + length);  }
/*  ======================================================================
	 CONSUMER BARRIER
	 Return the highest contiguous available sequence for a lane. This scans only the availability window, then caches the result. It avoids the classic problem of a slow producer blocking visibility of later producers.
	====================================================================== */
	private availableThrough(lane: number): number {
	  const ctrlBase = this.ctrlAt(lane, 0);
	  const gateField = this.ctrlAt(lane, 1);
	  const cacheField = this.ctrlAt(lane, 2);
  
	  const next = Atomics.load(this.ctrl, ctrlBase);
	  let gate = Atomics.load(this.ctrl, gateField);
	  let cached = Atomics.load(this.ctrl, cacheField);
  
	  if (cached >= gate) {
		const avail = this.available[lane];
		const cap = this.capacity;
  
		let scan = cached;
		const limit = Math.min(next, cached + this.maxBatch);
  
		while (scan < limit) {
		  const flag = Atomics.load(avail, scan & (cap - 1));
		  if (flag <= scan) break;
		  scan++;
		}
  
		cached = scan;
		Atomics.store(this.ctrl, cacheField, cached);
	  }
  
	  return cached;
	}/*======================================================================
	   PHASE-LOCKED EXECUTION
	   Drain exactly one scheduled phase. A phase is homogeneous: READ only, WRITE only, DELETE only, etc.
	   ====================================================================== */
	public drainPhase(limit = this.maxBatch): number {
	  const schedule = this.schedule;
	  const lane = schedule[Atomics.load(this.phaseIndex, 0)];
	  const handler = this.handlers[lane];
  
	  if (!handler) {
		this.nextPhase();
		return 0;
	  }
  
	  const ring = this.rings[lane];
	  const gateField = this.ctrlAt(lane, 1);
  
	  const available = this.availableThrough(lane);
	  const head = Atomics.load(this.ctrl, gateField);
  
	  const end = Math.min(available, head + limit);
  
	  if (end <= head) {
		this.nextPhase();
		return 0;
	  }
  
	  const epoch = Atomics.load(this.clock, 0);
  
	  // Batch boundary is handed to the handler.
	  // The handler can specialize contiguous execution.
	  handler(ring, head, end, epoch, this);
  
	  Atomics.store(this.ctrl, gateField, end);
  
	  const consumedField = this.ctrlAt(lane, 7);
	  const batchField = this.ctrlAt(lane, 8);
	  const maxBatchField = this.ctrlAt(lane, 9);
  
	  const count = end - head;
  
	  Atomics.add(this.ctrl, consumedField, count);
	  Atomics.add(this.ctrl, batchField, 1);
  
	  const prevMax = Atomics.load(this.ctrl, maxBatchField);
	  if (count > prevMax) Atomics.store(this.ctrl, maxBatchField, count);
  
	  this.nextPhase();
	  return count;
	}// Run one full revolution. The tracer particle advances only after every scheduled phase runs.
	public revolution(limit = this.maxBatch): number {
	  let total = 0;
  
	  for (let i = 0; i < this.schedule.length; i++) {
		total += this.drainPhase(limit);
	  }
  
	  Atomics.add(this.clock, 0, 1);
	  return total;
	}// Run until all lanes are drained. Useful for shutdown, transaction boundaries, and tests.
	public drainAll(limit = this.maxBatch): number {
	  let total = 0;
  
	  for (;;) {
		const before = total;
		total += this.revolution(limit);
		if (total === before) break;
	  }
  
	  return total;
	}
/*  ======================================================================
	 CLOCK / CONTROL
	====================================================================== */
	public tick(): number { return Atomics.load(this.clock, 0);  }
	public currentPhase(): OpKind { return this.schedule[Atomics.load(this.phaseIndex, 0)];  }
	public start(): void {    Atomics.store(this.running, 0, 1);  }
	public stop(): void {
	  Atomics.store(this.running, 0, 0);
	  Atomics.notify(this.running, 0);
	}
	public isRunning(): boolean {    return Atomics.load(this.running, 0) === 1;  }
/*  ======================================================================
	 STATS
	====================================================================== */
	public stats(lane: number): LANE_STATS {
	  const b = lane * GALEN_MAREK.CTRL;
  
	  return {
		claimed: Atomics.load(this.ctrl, b + 5),
		published: Atomics.load(this.ctrl, b + 6),
		consumed: Atomics.load(this.ctrl, b + 7),
		batches: Atomics.load(this.ctrl, b + 8),
		maxBatch: Atomics.load(this.ctrl, b + 9),
		fullStalls: Atomics.load(this.ctrl, b + 4),
	  };
	}
	public pending(lane: number): number {
	  const published = Atomics.load(this.ctrl, this.ctrlAt(lane, 6));
	  const consumed = Atomics.load(this.ctrl, this.ctrlAt(lane, 7));
	  return published - consumed;
	}
/*  ======================================================================
	 INTERNAL
	====================================================================== */
	private laneFor(op: OpKind): number {    return op;  }
	private isFull(lane: number): boolean {
	  const next = Atomics.load(this.ctrl, this.ctrlAt(lane, 0));
	  const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
	  return next - gate >= this.capacity;
	}
	private nextPhase(): void {
	  const next = (Atomics.load(this.phaseIndex, 0) + 1) % this.schedule.length;
	  Atomics.store(this.phaseIndex, 0, next);
	}
	private yieldNow(): void {
	  if (typeof scheduler !== "undefined" && "yield" in scheduler) {
		scheduler.yield();
	  } else if (typeof setImmediate === "function") {
		setImmediate(() => {});
	  } else {
		setTimeout(() => {}, 0);
	  }
	}
	// #endregion
	// #region BUS FNS
	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "_TC_SIGNALS_";
	public k: A = "__SIGNALS__";
	public send: DISRUPTOR_BUS_SEND = (() => {
		const fn = <T = A,> (p: U, k: A, s:S = this.s, m:S = this.m, r:S = this.r, ttl?: N, chain?: CHAIN,  adapter = "disruptor") => {
			const msg: SIG_BUS_Message = { id: this.root._id._byteid(), r: r, m: m, s: s, k: k, p: p, ts: Date.now(), _ack: false };

			let v: A =  this.DISPATCH(msg, adapter);
			if (v !== undefined && !(v instanceof Promise)) {
				return MCR_Combo<T>(v, chain);
			} else {
				const timeoutMs = 5000;
				return MaybePromise(
					(() => {
						return new Promise<T>((resolve, reject) => {
							let timer: RET<typeof setTimeout> | UD;
							if (v) {
								if (timer) clearTimeout(timer);
								resolve(v as T);
								return;
							}
							if (timeoutMs) {
								timer = setTimeout(() => {
									reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`));
								}, timeoutMs);
							}
						});
					})(),
				);
			}
		};
		fn.broadcast = async (p: U, k: S, s = this.s, m = this.m, r = this.r, ttl?: N, binary = false, adapter = "http"): P<V> => {

			const msg: SIG_BUS_Message = { id: this.root._id.b(), r: r, m: m, s: s, k: k, p: {p}, ttl, ts: Date.now(), _ack: false };
			const a = SSR._adapters();
			await Promise.all(
				[...a].map(a => {
					try {
						return this.DISPATCH(msg, adapter);
					} catch {
						return Promise.resolve();
					}
				}),
			);
		};
		fn.broadcastState = (state: A, k: S, s: S, m: S, r: S, chain?: "chain" | "rechain") => {
			const v = this.send(state, k, s, m, r).catch((err: any) => {
				console.error("Failed to broadcast state:", err);
			});
			return MCR_Combo(v, chain);
		};
		type Wildcard = "*";
		type W = "*";
		const isWildcard = (value: S | UD): value is Wildcard => value === "*";
		fn.emit = (k: S | "*", p: U, s = this.s, m = this.m, r = this.r): V => {
			const handlers = new Set<CSEHandler>();
			const ch = this.root.ssr._channels(s, m, r).channels;
			const sch = this.root.ssr._channels(s, m, r).sub_channels;
			const tch = this.root.ssr._channels(s, m, r).trip_channels;
			const qch = this.root.ssr._channels(s, m, r).quad_channels;

			const add = (set?: Set<CSEHandler>) => {
				if (set) for (const handler of set) handlers.add(handler);
			};

			const walkChannels = (channel: S | "*") => {
				if (isWildcard(channel)) {
					for (const set of ch.values()) add(set);
				} else {
					add(ch.get(channel));
				}
			};

			const walkSubChannels = (sub: S | "*", channel: S | "*") => {
				if (isWildcard(sub)) {
					for (const [ch, set] of sch.values()) {
						if (isWildcard(channel)) {
							for (const handlersAtChannel of set.values()) add(handlersAtChannel);
						} else {
							add(set.get(channel));
						}
					}
				} else {
					const byChannel = sch.get(sub);
					if (!byChannel) return;

					if (isWildcard(channel)) {
						for (const set of byChannel.values()) add(set);
					} else {
						add(byChannel.get(channel));
					}
				}
			};

			const walkTrips = (trip: S | "*", sub: S | "*", channel: S | "*") => {
				if (isWildcard(trip)) {
					for (const bySub of tch.values()) {
						if (isWildcard(sub)) {
							for (const byChannel of bySub.values()) {
								if (isWildcard(channel)) {
									for (const set of byChannel.values()) add(set);
								} else {
									add(byChannel.get(channel));
								}
							}
						} else {
							const byChannel = bySub.get(sub);
							if (!byChannel) continue;

							if (isWildcard(channel)) {
								for (const set of byChannel.values()) add(set);
							} else {
								add(byChannel.get(channel));
							}
						}
					}
				} else {
					const bySub = tch.get(trip);
					if (!bySub) return;

					if (isWildcard(sub)) {
						for (const byChannel of bySub.values()) {
							if (isWildcard(channel)) {
								for (const set of byChannel.values()) add(set);
							} else {
								add(byChannel.get(channel));
							}
						}
					} else {
						const byChannel = bySub.get(sub);
						if (!byChannel) return;

						if (isWildcard(channel)) {
							for (const set of byChannel.values()) add(set);
						} else {
							add(byChannel.get(channel));
						}
					}
				}
			};

			const walkQuads = () => {
				if (isWildcard(k)) {
					for (const byTrip of qch.values()) {
						if (isWildcard(s)) {
							for (const bySub of byTrip.values()) {
								if (isWildcard(m)) {
									for (const byChannel of bySub.values()) {
										if (isWildcard(r)) {
											for (const set of byChannel.values()) add(set);
										} else {
											add(byChannel.get(r));
										}
									}
								} else {
									const byChannel = bySub.get(m);
									if (!byChannel) continue;
									if (isWildcard(r)) {
										for (const set of byChannel.values()) add(set);
									} else {
										add(byChannel.get(r));
									}
								}
							}
						} else {
							const bySub = byTrip.get(s);
							if (!bySub) continue;
							if (isWildcard(m)) {
								for (const byChannel of bySub.values()) {
									if (isWildcard(r)) {
										for (const set of byChannel.values()) add(set);
									} else {
										add(byChannel.get(r));
									}
								}
							} else {
								const byChannel = bySub.get(m);
								if (!byChannel) continue;
								if (isWildcard(r)) {
									for (const set of byChannel.values()) add(set);
								} else {
									add(byChannel.get(r));
								}
							}
						}
					}
				} else {
					const byTrip = qch.get(k);
					if (!byTrip) return;
					// Same descendant walk as above, starting at byTrip.
				}
			};

			// Use the index at the broadest requested scope.
			if (isWildcard(k)) {
				walkQuads();
			} else if (isWildcard(s)) {
				// trip_channels is keyed by s, then m, then r.
				walkTrips(s, m, r);
			} else if (isWildcard(m)) {
				// sub_channels is keyed by m, then r.
				walkSubChannels(m, r);
			} else if (isWildcard(r)) {
				// channels is keyed by r.
				walkChannels(r);
			} else {
				// Exact path: any one index can retrieve it.
				add(qch.get(k)?.get(s)?.get(m)?.get(r));
			}

			for (const handler of handlers) handler(p);
		};
		return fn;
	})();
	public DISPATCH = (() => {
		const fn = async (raw: A, adapter?: string) => {
			let msg: A = raw;
			//                                    DECODE DE-BINARY
			// #region ═════════════════════════════════════════════════════════════════════════════════════
			// Decode first — mirrors encode running last on the send side.
			if (msg instanceof Uint8Array || (typeof Buffer !== "undefined" && Buffer.isBuffer(msg))) {
				msg = this.t.decode_msg(msg);
				if (!msg) return {success: false, decodeFailed: true};
			}
			// #endregion
			//                                    SECURE UNWRAP
			// #region ═════════════════════════════════════════════════════════════════════════════════════
			if (this.root.se._auth_initialized) {
				msg = await this.root.se.encryption.unwrap(msg);
				if (!msg) return {success: false, unauthorized: true};
			}
			// #endregion
			//                                    CHANNEL MAPPING
			// #region ═════════════════════════════════════════════════════════════════════════════════════
			msg.r = msg.r ?? this.r;
			msg.m = msg.m ?? this.m;
			msg.s = msg.s ?? this.s;
			msg.k = msg.k;
			console.log(msg, "emitter.dispatch.msg");
			// #endregion
			//                                    REPLY
			// #region ═════════════════════════════════════════════════════════════════════════════════════
			if (msg.r && msg.r.startsWith(":reply:")) {
				const originalMsgId = msg.r.replace(":reply:", "");
				const pendingTx = this.root.se.pending.get(originalMsgId);
				if (pendingTx) {
					clearTimeout(pendingTx.timer);
					this.root.se.pending.delete(originalMsgId);
					pendingTx.resolve(msg.p);
					return {success: true, isReply: true};
				}
			}
			// #endregion
			//                                    ROUTING RULES
			// #region ═════════════════════════════════════════════════════════════════════════════════════
			//   let targets = [...this.adapters].filter(a => a !== msg._adapter)
			for (const rule of SE._routeRules()) {
				if (!rule.match(msg)) continue;
				//     if (rule.targetAdapters) { targets = targets.filter(a => rule.targetAdapters!.includes(a.id ?? '')) }
				if (rule.targetrts) {
					// if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('route_rejected', msg.r, msg.m || undefined, msg.id, { rule: rule.name })
					if (!rule.targetrts.includes(msg.r)) return {success: false, routed: false};
				}
			}
	
			// #endregion
			//                                    SEND
			// #region ═════════════════════════════════════════════════════════════════════════════════════
			Promise.resolve().then(() => {
				const a = this._adapters();
				switch (adapter) {
					case "http":
						this.DISPATCH.sendMessage(msg, a.http);
						break;
					case "cross_boundary":
						this.DISPATCH.sendMessage(msg, a.cross_boundary);
						break;
					case "same_boundary":
						this.DISPATCH.sendMessage(msg, a.same_boundary);
						break;
					case "atom_adapter":
						this.DISPATCH.sendMessage(msg, a.atom_adapter);
						break;
					case "ws":
						this.DISPATCH.sendMessage(msg, a.ws);
						break;

					case "reliableSocket":
						this.DISPATCH.sendMessage(msg, a.reliableSocket);
						break;
					case "workerThreadAdapter":
						this.DISPATCH.sendMessage(msg, a.workerThreadAdapter);
						break;
					case "webTransportDatagramsAdapter":
						this.DISPATCH.sendMessage(msg, a.webTransportDatagramsAdapter);
						break;
					case "webTransportClientAdapter":
						this.DISPATCH.sendMessage(msg, a.webTransportClientAdapter);
						break;
					case "webRtcDataChannelHostAdapter":
						this.DISPATCH.sendMessage(msg, a.webRtcDataChannelHostAdapter);
						break;
					case "webRtcDataChannelClientAdapter":
						this.DISPATCH.sendMessage(msg, a.webRtcDataChannelClientAdapter);
						break;
					case "dataChannelAdapter":
						this.DISPATCH.sendMessage(msg, a.dataChannelAdapter);
						break;
					case "startWebTransportServer":
						this.DISPATCH.sendMessage(msg, a.startWebTransportServer);
						break;
					case "webTransportAdapter":
						this.DISPATCH.sendMessage(msg, a.webTransportAdapter);
						break;
					case "webRTCAdapter":
						this.DISPATCH.sendMessage(msg, a.webRTCAdapter);
						break;
					case "sseAdapter":
						this.DISPATCH.sendMessage(msg, a.sseAdapter);
						break;
					case "sharedArrayBufferAdapter":
						this.DISPATCH.sendMessage(msg, a.sharedArrayBufferAdapter);
						break;
					case "sharedWorkerAdapter":
						this.DISPATCH.sendMessage(msg, a.sharedWorkerAdapter);
						break;
					case "serviceWorkerAdapter":
						this.DISPATCH.sendMessage(msg, a.serviceWorkerAdapter);
						break;
					case "postMessageAdapter":
						this.DISPATCH.sendMessage(msg, a.postMessageAdapter);
						break;
					case "startNamedPipeBroker":
						this.DISPATCH.sendMessage(msg, a.startNamedPipeBroker);
						break;
					case "startMQTTBroker":
						this.DISPATCH.sendMessage(msg, a.startMQTTBroker);
						break;
					case "mqttAdapter":
						this.DISPATCH.sendMessage(msg, a.mqttAdapter);
						break;
					case "messageChannelAdapter":
						this.DISPATCH.sendMessage(msg, a.messageChannelAdapter);
						break;
					case "messageChannel":
						this.DISPATCH.sendMessage(msg, a.messageChannel);
						break;
					case "http2ClientAdapter":
						this.DISPATCH.sendMessage(msg, a.http2ClientAdapter);
						break;
					case "http2ServerAdapter":
						this.DISPATCH.sendMessage(msg, a.http2ServerAdapter);
						break;
					case "httpAdapter":
						this.DISPATCH.sendMessage(msg, a.httpAdapter);
						break;
					case "electronIPCMainAdapter":
						this.DISPATCH.sendMessage(msg, a.electronIPCMainAdapter);
						break;
					case "electronIPCRendererAdapter":
						this.DISPATCH.sendMessage(msg, a.electronIPCRendererAdapter);
						break;
					case "nativeMessagingHostAdapter":
						this.DISPATCH.sendMessage(msg, a.nativeMessagingHostAdapter);
						break;
					case "nativeMessagingBrowserAdapter":
						this.DISPATCH.sendMessage(msg, a.nativeMessagingBrowserAdapter);
						break;
					case "childProcessAdapter":
						this.DISPATCH.sendMessage(msg, a.childProcessAdapter);
						break;
					case "broadcastChannelAdapter2":
						this.DISPATCH.sendMessage(msg, a.broadcastChannelAdapter2);
						break;
					case "broadcastChannelAdapter":
						this.DISPATCH.sendMessage(msg, a.broadcastChannelAdapter);
						break;
					case "broadcastChannelAdapter":
						this.DISPATCH.sendMessage(msg, a.broadcastChannelAdapter);
						break;

					default:
						console.log("http adapter");
						this.DISPATCH.sendMessage(msg, a.broadcastChannelAdapter);
					// this.sendMessage(msg, a.http);
					// this.sendMessage(msg, a.same_boundary);
				}
			});
			// #endregion
			const result = await this.invokeHandlers(msg);
			console.log(result, "emitter.dispatch.result");
			return result;
		};
		fn.drain = async (): P<V> => {
			const startTime = Date.now();
			const msgs = [...this.root.se._queue()];
			this.root.se._queue().set([]);
			await this.root.JEDI.IDB_KV.delete(this.root.se.QUEUE_KEY);
	
			// Process each message through flood gates (round robin)
			for (const msg of msgs) {
				if (msg.ttl && msg.ts !== undefined && Date.now() - msg.ts > msg.ttl) continue;
				await this.DISPATCH(msg);
			}
	
			this.t.recordQueue("drain", Date.now() - startTime); // _metrics
		};
		fn.enqueue = async (msg: SIG_BUS_Message): P<V> => {
			if (this.root.DEVTOOLS_ACTIVE) {
				log(`[THE_WIZARD][SIGNALS][ENQUEUE]`, {msg: msg}, t.deep); //this.drop.t.error
				// this.emitDevtoolsEvent('enqueue', msg.r, msg.m || undefined, msg.id, {          priority: msg.priority,          ttl: msg.ttl        });
				this.t.recordFeature("floodGate"); // _metrics
				this.t.traceStage(msg.id, String(msg.r), "enqueue", {priority: msg.priority, ttl: msg.ttl}); // _metrics
				this.t.tracePayloadSize(msg.id, JSON.stringify(msg.p ?? "").length); // _metrics
			}
	
			// Normal queue processing continues here...
			if (this.root.DEVTOOLS_ACTIVE) {
				this.t.recordQueue("enqueue");
			}
	
			this.root.se._queue().push(msg);
			this.root.se._queue().sort((a: A, b: A) => this.root.se.priorityWeight(b.priority) - this.root.se.priorityWeight(a.priority));
	
			if (this.root.se._backpressure().maxQueueSize && this.root.se._queue().length > this.root.se._backpressure().maxQueueSize) {
				const dropped = this.root.se._queue().pop();
				if (dropped) {
					this.root.se._deadLetters().push(dropped);
					if (this.root.DEVTOOLS_ACTIVE) this.t.recordQueue("drop"); // _metrics
				}
			}
	
			const PAYLOAD_SIZE_CAP = 524288;
			const persistable = this.root.se._queue().map((m: A) => {
				const size = JSON.stringify(m.p).length;
				return size > PAYLOAD_SIZE_CAP ? {...m, p: null} : m;
			});
			await this.root.JEDI.IDB_KV?.set(this.root.se.QUEUE_KEY, persistable);
	
			if (this.root.DEVTOOLS_ACTIVE && this.root.se._queue().some((m: A) => m.id === msg.id)) {
				this.t.finishTrace(msg.id, "queued");
			} // _metrics
		};
		fn.sendMessage = (msg: SIG_BUS_Message, adapter: SIG_BUS_Adapter, boundary?: "same-app" | "same-node" | "cross-node") => {
			// Prepare p - JSON serialize, strip functions
			let finalizedPayload = {...msg};
			if (finalizedPayload.p && typeof finalizedPayload.p === "object" && boundary === "cross-node") {
				finalizedPayload.p = JSON.parse(
					JSON.stringify(finalizedPayload.p, (key, value) => {
						if (typeof value === "function") return undefined;
						return value;
					}),
				);
			}
	
			// Send through adapter
			if (this.root._auth_initialized) {
				const secured = this.root.se.encryption.wrap(finalizedPayload);
				adapter.send(secured as A);
			} else {
				adapter.send(finalizedPayload);
			}
		};
		return fn;
	})();
	// #endregion
}
// #endregion
//                                                                                               [POWER_OF_TWO]  SPREADSHEET ENGINE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class POWER_OF_TWO {
	public root: SIGNALS
	public readonly capacity: number;
	public readonly mask: number;
	// ── Structure of Arrays (SoA) Event Lanes ──────────────────────────────
	public readonly pids: Int32Array;
	public readonly values: Float64Array;
	public readonly flags: Uint8Array;
	// ── Cursors ────────────────────────────────────────────────────────────
	public writeCursor = 0;
	public readCursor = 0;
	constructor(root: SIGNALS, capacity = 65536) {// Enforce power-of-two for bitwise masking
		if ((capacity & (capacity - 1)) !== 0) { throw new Error("Disruptor capacity must be a power of two."); }
		this.root = root
		this.capacity = capacity;
		this.mask = capacity - 1;
		this.pids = new Int32Array(capacity); // Pre-allocate contiguous typed memory blocks (Zero GC churn)
		this.values = new Float64Array(capacity);
		this.flags = new Uint8Array(capacity);
	} // Publishes an event directly into the next ring slot. O(1) time complexity, zero heap allocation, single CPU instruction cycle.
	public publish(pid: number, value: number, flag = 0): boolean {
		// Optional backpressure check: Prevent writer from overlapping reader
		if (this.writeCursor - this.readCursor >= this.capacity) {
			return false;
		} // Ring buffer full
		const index = this.writeCursor & this.mask;
		this.pids[index] = pid; // Write directly to parallel SoA typed array lanes
		this.values[index] = value;
		this.flags[index] = flag;
		this.writeCursor++;
		return true;
	} // Drains and processes the ring buffer in batches using a zero-allocation loop.
	public consume(handler: (pid: number, value: number, flag: number) => void): number {
		const targetWrite = this.writeCursor;
		let currentRead = this.readCursor;
		if (currentRead === targetWrite) {
			return 0;
		} // Nothing to consume
		let processed = 0;
		while (currentRead < targetWrite) {
			const index = currentRead & this.mask; // Execute handler on raw lane data
			handler(this.pids[index], this.values[index], this.flags[index]);
			currentRead++;
			processed++;
		}
		this.readCursor = currentRead;
		return processed;
	} // Resets cursors and ring lanes instantly without reallocating memory.
	public reset(): void {
		this.writeCursor = 0;
		this.readCursor = 0;
		this.pids.fill(0);
		this.values.fill(0);
		this.flags.fill(0);
	}
}
// #endregion
//                                                                                                                         GOVERNER
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class GOVERNOR {
	/* ==========================================================================
	   The disruptor runs hot on its own.
	   The public API never touches passes.
	   ========================================================================== */
	  private tier: 0 | 1 | 2 | 3 = 0;          // 25 / 50 / 75 / 100
	  private running = false;
	  private hibernating = false;
	
	  private lastAction = 0;
	  private windowActions = 0;
	  private windowStart = 0;
	
	  private readonly IDLE_MS = 15_000;
	  private readonly TIERS = [
		256,    // tier 0 -> 25%
		1_024,   // tier 1 -> 50%
		4_096,   // tier 2 -> 75%
		16_384,  // tier 3 -> 100%
	  ];
	  constructor(private readonly root: SIGNALS) {}
	  wake(): void {
		this.lastAction = Date.now();
	
		if (this.hibernating) {
		  this.hibernating = false;
		}
	
		if (!this.running) {
		  this.running = true;
		  queueMicrotask(() => this.loop());
		}
	  }
	  private async loop(): Promise<void> {
		while (this.running) {
		  const now = Date.now();
	
		  // Hibernation: 15s of true silence.
		  if (now - this.lastAction >= this.IDLE_MS) {
			this.hibernating = true;
			this.running = false;
			this.tier = 0;
			return;
		  }
	
		  // Drain one revolution at the current tier.
		  this.root.RX_S.pass();
	
		  // Sample load and adjust tier.
		  this.windowActions++;
	
		  if (this.windowStart === 0) {
			this.windowStart = now;
		  }
	
		  const windowMs = now - this.windowStart;
	
		  if (windowMs >= 1_000) {
			const rate = (this.windowActions / windowMs) * 1_000;
	
			this.tier = this.tierFor(rate);
	
			this.windowActions = 0;
			this.windowStart = now;
		  }
	
		  // Yield so the loop never starves the JS runtime.
		  await Promise.resolve();
		}
	  }
	  private tierFor(rate: number): 0 | 1 | 2 | 3 {
		if (rate >= this.TIERS[2]) return 3;
		if (rate >= this.TIERS[1]) return 2;
		if (rate >= this.TIERS[0]) return 1;
		return 0;
	  }
	  currentTier(): number {
		return this.tier;
	  }
	  isHibernating(): boolean {
		return this.hibernating;
	  }
}
// #endregion 
//                                                                                                                        [SIGNALS]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class SIGNALS {
	//                                                                                                                  [CONSTANTS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public RX_S: VADER;
	public hub: PROJECT_PLANET_KILLER;
	public bus: GALEN_MAREK;
	public ss_engine: POWER_OF_TWO;
	public governor: GOVERNOR
	public _id: BYTE_ID = __sid
	public RTRN_TO_SDR = "_RETURN_TO_SENDER";
	public twm = "__THE_WARLOCK_MODULE_";
	public mc = `__THE_WARLOCK_MODULE_CACHE__`;
	public subcribers = "_SUBSCRIBERS";
	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "_TC_SIGNALS_";
	public k: A = "__SIGNALS__";
	public activeTracer: Map<SignalKey, SIGNAL_CELL> | null = null;
	constructor() {
		this.s = Store.get("SAURON_SLAVE_REG") ?? "SAURON_SLAVE_REG";
		this.m = Store.get("SAURON_MASTER_REG") ?? "SAURON_MASTER_REG";
		this.r = Store.get("THE_WIZARD_ROOT_REG") ?? "THE_WIZARD_ROOT_REG";
		this.hub = new PROJECT_PLANET_KILLER(this);
		this.RX_S = new VADER(this);
		this.ss_engine = new POWER_OF_TWO(this);
		this.bus = new GALEN_MAREK(this);
		this.governor = new GOVERNOR(this)
	}
	public sg(k: A) {
		return `${k}`;
	}
	u = undefined;
	uuuu = [this.u, this.u, this.u, this.u];
	UUUU = [this.s, this.m, this.r];
	uuu = [this.u, this.u, this.u];
	UUU = [this.s, this.m, this.r];
	uu = [this.u, this.u];
	UU = [this.m, this.r];
	private total_cache_size = 5000;
	// #endregion
	//                                                                                                                       [API]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public atom<T = any>(v: A, key: SignalKey = `__THE_WARLOCK_SIGNALS_${__sid.b()}`, s = this.s, m = this.m, r = this.r, options?: SignalOptions) {
		const rk = this.create(key, v, options, s, m, r);
		const self = this;
		return {
			get value(): T {
				self._bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			set value(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			peek(): T {
				return rk.peek();
			},
			get current(): T {
				self._bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			set current(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set crt(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set set(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
		};
	}
	public create<T = A>(k: SignalKey, v?: T | (() => T), options?: SignalOptions, s = this.s, m = this.m, r = this.r): SIGNAL_CELL<T> | T {
		const registry = this.hub.getNestedMap(s, m, r);
		let atom = registry.get(k);

		if (atom || v === undefined) {
			return atom as SIGNAL_CELL<T> | T;
		}

		const initialValue = typeof v === "function" ? (v as () => T)() : v;
		atom = new SIGNAL_CELL(k, initialValue, s, m, r, options);
		registry.set(k, atom);
		return atom as SIGNAL_CELL<T> | T;
	}
	public get<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): A {
		const registry = this.hub.atom_reg(s, m, r);
		return registry.get(k) as SIGNAL_CELL<T> | UD;
	}
	public write<T = A>(key: SignalKey, value: T | ((previous: T) => T), source = "write", s = this.s, m = this.m, r = this.r): T {
		return this._writeResolved(key, value, source, s, m, r);
	}
	public read<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		return atom?.read();
	}
	public peek<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		return atom?.peek();
	}
	signal<T>(): { (): T | undefined; (value: T | undefined): void;	};
	signal<T>(initialValue: T): { (): T; (value: T): void;	};
	signal<T>(initialValue?: T): { (): T | undefined; (value: T | undefined): void;	} {
		const signalFn = this.RX_S.signalOper.bind({
			currentValue: initialValue,
			pendingValue: initialValue,
			subs: undefined,
			subsTail: undefined,
			flags: ReactiveFlags.MUTABLE,
		}) as () => T | undefined;
	
		Object.defineProperty(signalFn, this.RX_S.BRAND_SIGNAL, { value: true })
		return signalFn
	}
	public computed<T>(getter: (previousValue?: T) => T): () => T {
		const v = this.RX_S.computedOper.bind({
			value: undefined,
			subs: undefined,
			subsTail: undefined,
			deps: undefined,
			depsTail: undefined,
			flags: ReactiveFlags.NONE,
			getter: getter as (previousValue?: unknown) => unknown,
		}) as () => T;
		Object.defineProperty(v, this.RX_S.BRAND_COMPUTED, { value: true });
		return v
	}
	public effect(fn: () => void | (() => void),  options?: { onError?: EffectErrorHandler },): () => void {
		const e: EffectNode = {
			fn,
			cleanup: undefined,
			subs: undefined,
			subsTail: undefined,
			deps: undefined,
			depsTail: undefined,
			flags: ReactiveFlags.WATCHING | ReactiveFlags.RECURSEDCHECK,
		};
		const prevSub = this.RX_S.setActiveSub(e);
		if (prevSub !== undefined) {
			this.RX_S.link(e, prevSub, 0);
			prevSub.flags |= this.RX_S.HasChildEffect;
		}
		try {
			++this.RX_S.runDepth;
			e.cleanup = e.fn();
		} finally {
			--this.RX_S.runDepth;
			this.RX_S.activeSub = prevSub;
			e.flags &= ~ReactiveFlags.RECURSEDCHECK;
	
		}
	
		const effectFn = this.RX_S.effectOper.bind(e)// as Effect;
	
		Object.defineProperty(effectFn, this.RX_S.BRAND_EFFECT, { value: true });
	
	
		return this.RX_S.effectOper.bind(e);
	}
	public effectScope(fn: () => void): () => void {
		const e: EffectScopeNode = {
			deps: undefined,
			depsTail: undefined,
			subs: undefined,
			subsTail: undefined,
			flags: ReactiveFlags.MUTABLE,
		};
		const prevSub = this.RX_S.setActiveSub(e);
		if (prevSub !== undefined) {
			this.RX_S.link(e, prevSub, 0);
			prevSub.flags |= this.RX_S.HasChildEffect;
		}
		try {
			fn();
		} finally {
			this.RX_S.activeSub = prevSub;
		}
		const scopeFn = this.RX_S.effectScopeOper.bind(e) //as EffectScope;
		Object.defineProperty(scopeFn, this.RX_S.BRAND_EFFECT_SCOPE, { value: true });
		return this.RX_S.effectScopeOper.bind(e);
	}
	public trigger(fn: () => void) {
		const sub: ReactiveNode = {
			deps: undefined,
			depsTail: undefined,
			flags: ReactiveFlags.WATCHING | ReactiveFlags.RECURSEDCHECK,
		};
		const prevSub = this.RX_S.setActiveSub(sub);
		++this.RX_S.batchDepth;
		try {
			fn();
		} finally {
			this.RX_S.activeSub = prevSub;
			sub.flags = ReactiveFlags.NONE;
			let link = sub.deps;
			while (link !== undefined) {
				const dep = link.dep;
				link = this.RX_S.unlink(link, sub);
				const subs = dep.subs;
				if (subs !== undefined) {
					this.RX_S.propagate(subs, !!this.RX_S.runDepth);
					this.RX_S.shallowPropagate(subs);
				}
			}
			if (!--this.RX_S.batchDepth) {
				this.RX_S.flush();
			}
		}
	}
	public runTeardown(t: Teardown): V {
		if (t == null) return;
		if (typeof t === "function") {
			t();
			return;
		}
		if (Array.isArray(t)) {
			for (const x of t) this.runTeardown(x);
			return;
		}
		const anyT = t as A;
		if (typeof anyT.unsubscribe === "function") {
			anyT.unsubscribe();
			return;
		}
		if (typeof anyT[Symbol.dispose] === "function") {
			anyT[Symbol.dispose]();
			return;
		}
		throw new TypeError("Invalid teardown: " + t);
	}
	public reportError(e: U): V {
		const g = globalThis as A;
		//log(`[✘][HOOK_SYSTEM][SIGNALS][REPORT_ERROR]`, { error: e }, t.error)
		if (typeof g.reportError === "function") {
			g.reportError(e);
			return;
		}
		queueMicrotask(() => {
			throw e;
		});
	}
	public registerSchema<T = A>(k: A, schema: SignalSchema, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		if (!atom) {
			throw new Error(`[Sauron] Cannot register schema for unknown signal "${String(k)}"`);
		}

		atom.setSchema(schema);
	}
	public has(k: A, s = this.s, m = this.m, r = this.r): B {
		return this.hub.atom_reg(s, m, r).get(k) !== undefined;
	}
	public freeze(k: A, s = this.s, m = this.m, r = this.r): void {
		const atom = this.hub.atom_reg(s, m, r).get(k);

		if (!atom) {
			throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
		}

		atom.setFrozen();
	}
	public seal(k: A, s = this.s, m = this.m, r = this.r): V {
		const meta = this.hub.atom_reg(s, m, r);
		const atom = meta.get(k);
		atom.setSealed();
	}
	public reset(k: A, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<A> | undefined;

		if (!atom) {
			throw new Error(`[Sauron] Cannot reset unknown signal "${String(k)}"`);
		}

		this._writeResolved(k, atom.initial, "reset", s, m, r, "reset");
	}
	public hydrate<T>(k: A, value: T, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		if (!atom) {
			this.create(k, value, undefined, s, m, r);
			return;
		}

		atom.write(value, { source: "hydrate", triggerRender: false });
	}
	public tx(fn: () => V): V {
		const o = {s: this.s, ms_reg: this.m,r: this.r }
		const snap = new Map<string, A>();
		for (const [k, v] of Object.entries(this.hub.state.getAll(o))) snap.set(k, v);
		const sysSnap = this._sys.snapshot();
		const self = this;
		try {
			fn();
		} catch (err) {
			for (const k of Object.keys(this.hub.state.getAll(o))) if (!snap.has(k)) this.hub.state.delete(k, o);
			for (const [k, v] of snap) this.hub.state.set(k, v, {...o, triggerRender: false});
			self._sys.dropComputeds();
			self._sys.restore(sysSnap);
			throw err;
		}
	}
	public derive = (() => {
		const fn = <T>(k: A, deps: A[], compute: (...values: A[]) => T, s = this.s, m = this.m, r = this.r, options?: { eager?: B; onError?: (err: Error) => V }): S => {
			if (this.hub.getMetadata(s, m, r)._sys.get(k)?.isComputed) return k;
	
			const reg = this.hub.atom_reg<string, A>(s, m, r);
	
			for (const d of deps) {
				this._sysEnsure(d, reg.get(d), s, m, r);
			}
	
			const c = computed(() => {
				try {
					const vals = deps.map((d) => this._sysEnsure(d, reg.get(d), s, m, r).read());
					return compute(...vals);
				} catch (err) {
					options?.onError?.(err as Error);
					throw err;
				}
			});
	
			this.hub.getMetadata(s, m, r)._sys.set(k, { read: () => c(), write: () => {}, node: c as A, isComputed: true });
			reg.set(
				k,
				this.RX_S.untrackFn(() => c()),
			);
	
			if (options?.eager) {
				// Warm the computed cache once. Do not write its result back
				// into another signal; that defeats lazy recomputation.
				this.RX_S.untrackFn(() => c());
			}
			return k;
		}
		fn.invalidate=(k: A, s = this.s, m = this.m, r = this.r): V =>{
			const entry = this.hub.getMetadata(s, m, r)._sys.get(k);
			if (!entry || !entry.isComputed) return;
			this._writeResolved(
				k,
				this.RX_S.untrackFn(() => entry.read()),
				"derived",
				"invalidate",
				s,
				m,
				r,
			);
		}
		return fn
	})();
	// #endregion
	//                                                                                                               [TRACE EFFECT]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public trace = (() => {
		const fn  =(fn: () => V): { reads: SignalKey[]; result: V } =>{
			const reads = new Map<SignalKey, SIGNAL_CELL>();
			const previous = this.activeTracer;
	
			this.activeTracer = reads;
	
			try {
				const result = fn();
	
				return {
					reads: [...reads.keys()],
					result,
				};
			} finally {
				this.activeTracer = previous;
			}
		}
		fn.effect=(fn: () => void | (() => void), s: S = this.s, m: S = this.m, r: S = this.r): () => V =>{
			const reads = new Map<SignalKey, A>();
			const runs: Array<{
				run: number;
				reads: Map<SignalKey, A>;
				at: number;
			}> = [];
	
			let runCount = 0;
	
			const tracked = new Map<SignalKey, SIGNAL_CELL>();
			const registry = this.hub.atom_reg(s, m, r);
	
			const effectDispose = effect(() => {
				runCount++;
	
				const before = new Map<SignalKey, A>();
	
				for (const [key, cell] of tracked) {
					before.set(key, cell.peek());
				}
	
				const result = fn();
	
				const touched = new Map<SignalKey, A>();
	
				for (const [key, cell] of tracked) {
					const value = cell.peek();
	
					if (!Object.is(before.get(key), value)) {
						touched.set(key, value);
					}
				}
	
				runs.push({
					run: runCount,
					reads: new Map(tracked),
					at: Date.now(),
				});
	
				return result;
			});
	
			// Wrap reads by exposing a tracing proxy is not needed for the
			// first version: cells register themselves when read through
			// useCell / read(). For explicit tracing, callers can pass keys.
			return effectDispose;
		}// Finds the first recorded write whose value differs from the current live value. That is the divergence point.
		fn.bisect=(entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, s: S = this.s, m: S = this.m, r: S = this.r): { index: number; key: SignalKey; recorded: A; actual: A } | UD =>{
			const registry = this.hub.atom_reg(s, m, r);
	
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i];
				const atom = registry.get(entry.key) as SIGNAL_CELL | undefined;
	
				if (!atom) continue;
	
				const actual = atom.peek();
	
				if (!Object.is(actual, entry.value)) {
					return {
						index: i,
						key: entry.key,
						recorded: entry.value,
						actual,
					};
				}
			}
	
			return undefined;
		}
		fn.why= <T = A>(k: SignalKey, s: S = this.s, m: S = this.m, r: S = this.r): { key: SignalKey; value: T | UD; version: number; lastWriteSource?: S; causal: CausalEntry[];  history: readonly AtomHistoryEntry<T>[] } | UD =>{
			const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;
	
			if (!atom) return undefined;
	
			return {
				key: atom.key,
				value: atom.peek(),
				version: atom.version,
				lastWriteSource: atom.lastWriteSource,
				causal: [...atom.causal].reverse(),
				history: atom.getHistory(),
			};
		}
		return fn;
	})();
	// #endregion
	//                                                                                                              [STATE SURGERY]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public surgery = (() => {
		// Live-state surgery. Reads the current value, hands it to `mutate`, writes the result back through the normal reactive write path, records a causal entry, and pushes an undo entry onto the surgery journal.
	    // The mutation receives the current value. It may either: - mutate and return nothing (object/array mutation), or - return a replacement value.
		const fn = <T = A>(k: SignalKey, mutate: (draft: T) => T | void, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S; record?: B }): T=> {
			const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;
	
			if (!atom) {
				throw new Error(`[Sauron] Cannot perform surgery on unknown signal "${String(k)}"`);
			}
	
			if (atom.frozen) {
				throw new Error(`[Sauron] Cannot perform surgery on frozen signal "${String(k)}"`);
			}
	
			const previous = atom.peek();
	
			// Structured clone gives surgery isolation without touching the
			// reactive kernel. Primitive values pass through unchanged.
			const draft = previous !== null && typeof previous === "object" ? (structuredClone(previous) as T) : previous;
	
			const result = mutate(draft);
	
			const next = result === undefined ? draft : (result as T);
	
			if (Object.is(previous, next)) {
				return previous;
			}
	
			const source = options?.source ?? "surgery";
			const cause = options?.cause ?? `surgery:${String(k)}:${Date.now()}`;
	
			const written = atom.write(next, {
				source,
				cause,
				triggerRender: true,
				recordHistory: options?.record ?? true,
			});
	
			this.surgery.journal.push({
				key: k,
				previous,
				next: written,
				s,
				m,
				r,
			});
	
			return written;
		}//  Undo the most recent surgery. Restoration goes through the normal write path, so computeds, effects, subscribers, history, and channels all see a real write.
		fn.journal = [] as Array<{ key: SignalKey; previous: A; next: A; s: S; m: S; r: S }>;
		fn.undo = (): A | UD =>{
			const entry = this.surgery.journal.pop();
	
			if (!entry) return undefined;
	
			const atom = this.hub.atom_reg(entry.s, entry.m, entry.r).get(entry.key) as SIGNAL_CELL | undefined;
	
			if (!atom) return undefined;
	
			return atom.write(entry.previous, {
				source: "surgery:undo",
				cause: `surgery:undo:${String(entry.key)}`,
				triggerRender: true,
				recordHistory: true,
			});
		}//   Undo every surgery performed since the returned marker was created. Undo every surgery performed since the returned marker was created.
		fn.checkpoint=(): () => V =>{
			const mark = this.surgery.journal.length;
	
			return () => {
				while (this.surgery.journal.length > mark) {
					this.surgery.undo();
				}
			};
		}// Surgery on a nested path. Example: sauron.surgeryAt('user', ['profile', 'email'], email => email.trim());
		fn.at=<T = A, P = A>(k: SignalKey, path: Array<string | number | symbol>, mutate: (draft: P) => P | void, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S }): T =>{
			return this.surgery<T>(
				k,
				(draft) => {
					let target: A = draft;
	
					for (let i = 0; i < path.length - 1; i++) {
						target = target[path[i] as keyof typeof target];
					}
	
					const leafKey = path[path.length - 1] as keyof typeof target;
					const leaf = target[leafKey] as P;
	
					const result = mutate(leaf);
	
					target[leafKey] = result === undefined ? leaf : (result as P);
	
					// Returning undefined keeps the outer draft as the next value.
					return undefined as unknown as T;
				},
				s,
				m,
				r,
				options,
			);
		}
		return fn;
	})();
	public _sys = (() => {
		const fn = () => {}
		fn.ensure=(rk: S, v: A, s = this.s, m = this.m, r = this.r): SysEntry =>{
			const sysMap = this.hub.getMetadata(s, m, r)._sys;
			let entry = sysMap.get(rk);
			if (entry) return entry;
			const sig = signal(v);
			entry = { read: () => sig(), write: (v: A) => sig(v), node: sig as A, isComputed: false };
			sysMap.set(rk, entry);
			if (Store.state.get(rk, { s, m, r }) === undefined) {
				Store.state.set(rk, v, { s, m, r, triggerRender: false });
			}
			return entry;
		}
		fn.snapshot=(s = this.s, m = this.m, r = this.r): M<S, A>=> {
			const out = new Map<string, A>();
			for (const [rk, entry] of this.hub.getMetadata(s, m, r)._sys) {
				if (!entry.isComputed)
					out.set(
						rk,
						this.RX_S.untrackFn(() => entry.read()),
					);
			}
			return out;
		}
		fn.restore=(snap: M<S, A>): V=> {
			for (const [rk, v] of snap) this._sysEnsure(rk, v).write(v);
		}
		fn.dropComputeds=(s = this.s, m = this.m, r = this.r): V=> {
			for (const [rk, entry] of this.hub.getMetadata(s, m, r)._sys) {
				if (entry.isComputed) this.hub.getMetadata(s, m, r)._sys.delete(rk);
			}
		}
		return fn;
	})();
	// #endregion
	//                                                                                                                      BACKEND
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public _validateSchema<T>(schema: SignalSchema, value: T): T {
		try {
			const next = schema.coerce ? schema.coerce(value) : value;

			if (schema.validate(next)) {
				return next as T;
			}

			throw new Error("[Sauron] Schema validation failed");
		} catch (error) {
			console.error("[Sauron] Schema validation failed:", error);
			throw error;
		}
	}
	public _serializeTuple(tuple: A[], s = this.s, m = this.m, r = this.r): S {
		const parts: string[] = new Array(tuple.length);
		const weakMap = this.hub.getMetadata(s, m, r)._weakMap;

		for (let i = 0; i < tuple.length; i++) {
			const item = tuple[i];
			const type = typeof item;

			if (item === null) {
				parts[i] = "null";
			} else if (type === "string") {
				parts[i] = `s:${item}`;
			} else if (type === "number") {
				parts[i] = `n:${item}`;
			} else if (type === "symbol") {
				parts[i] = `sym:${item!.description ?? String(item)}`;
			} else if (type === "bigint") {
				parts[i] = `bi:${item!.toString()}`;
			} else if (type === "undefined") {
				parts[i] = "undef";
			} else if (Array.isArray(item)) {
				parts[i] = `arr:[${this._serializeTuple(item, s, m, r)}]`;
			} else if (type === "object" || type === "function") {
				let refId = weakMap.get(item);
				if (!refId) {
					refId = `ref:${__sid.b()}`;
					weakMap.set(item, refId);
				}
				parts[i] = refId;
			} else {
				parts[i] = `v:${String(item)}`;
			}
		}

		return parts.join("|");
	}
	public _hashTuple(tuple: U[], s = this.s, m = this.m, r = this.r): S {
		const key = this._serializeTuple(tuple, s, m, r);
		const tupleCache = this.hub.getMetadata(s, m, r)._tupleCache;

		const cached = tupleCache.get(key);
		if (cached) {
			tupleCache.delete(key);
			tupleCache.set(key, cached);
			return cached;
		}

		let h1 = 0x811c9dc5;
		let h2 = 0x01000193;
		for (let i = 0; i < key.length; i++) {
			const code = key.charCodeAt(i);
			h1 = Math.imul(h1 ^ code, 16777619) >>> 0;
			h2 = Math.imul(h2 ^ code, 2246822507) >>> 0;
		}

		const result = `tuple:${h1.toString(36)}-${h2.toString(36)}`;
		tupleCache.set(key, result);

		if (tupleCache.size > this.total_cache_size) {
			const toRemove = Math.floor(tupleCache.size * 0.2);
			const iter = tupleCache.keys();
			for (let i = 0; i < toRemove; i++) {
				const k = iter.next().value;
				if (k !== undefined) tupleCache.delete(k);
			}
		}

		return result;
	}
	public _writeResolved<T>(k: SignalKey, value: T | ((previous: T) => T), source: S, s = this.s, m = this.m, r = this.r, cause?: S): T {
		const registry = this.hub.atom_reg(s, m, r);
		let atom = registry.get(k) as SIGNAL_CELL<T> | undefined;

		if (!atom) {
			const initial = typeof value === "function" ? undefined : (value as T);

			atom = this.create(k, initial, undefined, s, m, r) as SIGNAL_CELL<T>;
		}

		if (!atom) {
			throw new Error(`[Sauron] Cannot write unknown signal "${String(k)}"`);
		}

		return atom.write(value, {
			source,
			cause,
			triggerRender: atom.triggerRender,
		});
	}
	public _recordCausal<K, V>(k: S, value: V, source: S, s = this.s, m = this.m, r = this.r, cause?: S): void {
		const atom = this.hub.atom_reg<K, V>(s, m, r).get(k);

		if (!atom) return;

		atom.causal.push({
			key: k,
			value,
			timestamp: Date.now(),
			source,
			cause,
		});

		if (atom.causal.length > 100) {
			atom.causal.shift();
		}
	}
	public _bindComponentToSignal(rk: S, s = this.s, m = this.m, r = this.r): V {
		const component = getCurrentComponent();
		if (!component) return;
		const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
		if (component.hooks.has(hookKey)) {
			return;
		}
		component.hooks.set(hookKey, { value: undefined });

		const atom = this.create(rk, undefined, undefined, s, m, r);
		if (!atom) {
			return;
		}

		const unsubSignal = atom.subscribeExternal(() => {
			notifyHook(hookKey);
		});
		component.unmount.add(unsubSignal);

		if (component.render) {
			const unsubRender = subscribeToHook(hookKey, () => component.render?.());
			component.unmount.add(unsubRender);
		}
	}
	public _resolveConflict<T>(conflict: ConflictPolicy | undefined, previous: T, next: T, strategy: "latest" | "fail" | "accumulate" = "latest"): T | null {
		if (conflict?.resolve) {
			return conflict.resolve(previous, next);
		}

		if (strategy === "fail") {
			console.warn('[Sauron] conflict rejected by "fail" strategy');
			return null;
		}

		if (strategy === "accumulate" && Array.isArray(previous) && Array.isArray(next)) {
			return [...previous, ...next] as T;
		}

		return next;
	}
	private _sysEnsure(rk: S, v: A, s = this.s, m = this.m, r = this.r): SysEntry {
		const sysMap = this.hub.getMetadata(s, m, r)._sys;
		let entry = sysMap.get(rk);
		if (entry) return entry;
		const sig = signal(v);
		entry = { read: () => sig(), write: (v: A) => sig(v), node: sig as A, isComputed: false };
		sysMap.set(rk, entry);
		if (Store.state.get(rk, { s, m, r }) === undefined) {
			Store.state.set(rk, v, { s, m, r, triggerRender: false });
		}
		return entry;
	}
	 
	// #endregion
	//                                                                                                                   SEND / SUB
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public sub<T = A>(listener: (value: T, previous: T) => V,k: SignalKey,  s = this.s, m = this.m, r = this.r): () => V {
		const atom = this.create(k, null, undefined, s, m, r) as SIGNAL_CELL;

		if (!atom) {
			throw new Error(`[Sauron] Cannot subscribe to unknown signal "${String(k)}"`);
		}

		return atom.subscribe((change: A) => {
			listener(change.current, change.previous);
		});
	}
	public send = (k: S, p: A, s = this.s, m = this.m, r = this.r): V => {
		const atom = this.hub.atom_reg(s, m, r).get(k);
		atom.write(p);
	};
	public delete = (k: A, s = this.s, m = this.m, r = this.r): V => {
		this.hub.atom_reg(s, m, r).delete(k);
		this.hub.getMetadata(s, m, r)._sys.delete(k);
	};
	// #endregion
	//                                                                                                   DISTRIBUTED REACTIVE STATE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public state_setter(v: any, k: any = `_USECELL`, s: S = this.s, m: SU = this.m, r: SU = this.r) {
		const existing = this.hub.state.get(k, { s: s, ms_reg: m, r: r });
		if (existing !== undefined) return existing;
		const t = typeof v === "function" ? v() : v;
		this.hub.state.set(k, t, { triggerRender: true,  s: s, ms_reg: m, r: r });
		return t;
	}
	public naked = (() => {
		const fn = <T = A> (k: SignalKey, v?: T | (() => T), s = this.s, m = this.m, r = this.r) =>{
			const registry = this.hub.state.getMap(s, m, r);
			let atom = registry.get(k);

			if (atom || v === undefined) { return atom }

			const initialValue = typeof v === "function" ? (v as () => T)() : v;
			atom = this.signal<T>(initialValue)
			registry.set(k, atom);
			return atom 
		}
		return fn
	})();
	public useCell = (() => {
		const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r,  ) => {
			const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});

			const senderTokenRef = this.atom(_sidi()); // this.j.root._id.b();

			this.sub((i: A) => {
				const incoming = i.p;
				if (incoming?.from === senderTokenRef.value) return;
				this.hub.state.set(k,incoming.value, { triggerRender: false,  s: s, ms_reg: m, r: r });
				// set(k, incoming.value, false, s, m, r);
				setState(incoming.value);
			}, k, s, m, r, ); // stay in sync with local writes too (including our own setLive below)
			this.hub.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, ms_reg: m, r: r })
			// sub(k, (newValue: any) => setState(newValue), s, m, r);

			const setLive = (next: any) => {
				const newValue = typeof next === "function" ? next(this.hub.state.get(k, { s: s, ms_reg: m, r: r })) : next;
				setState(newValue);
				this.hub.state.set(k,newValue, { triggerRender: true,  s: s, ms_reg: m, r: r });
				// set(k, newValue, true, s, m, r); // local write
				this.send(k,{value: newValue, from: senderTokenRef.value},  s, m, r); // broadcast to the bus
			};

			return [state, setLive] as const;
		};
		fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});

			const senderTokenRef = this.atom(_sidi()); 
			const queueRef = this.atom<Queue | null>(null);

			if (!queueRef.current) {
				queueRef.current = new Queue(1);
			}

			// mailbox — replace the whole useEffect block:
			this.sub((i: A) => {
				const incoming = i.p;
				if (incoming?.from === senderTokenRef.value) return;
				queueRef.current!.run(async () => {
					const current = this.hub.state.get(k, { s: s, ms_reg: m, r: r })
					const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
					this.hub.state.set(k,next, { triggerRender: false,  s: s, ms_reg: m, r: r });
					return next;
				});
			}, k, s, m, r,);

			this.hub.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, ms_reg: m, r: r })

			const setLive = (next: any) => {
				const t = typeof next === "function" ? next() : next;
				this.send(k,{value: t, from: senderTokenRef.value},  s, m, r);
			};

			return [state, setLive] as const;
		};
		return fn
	})();
	// #endregion
	//                                                                                                           ANTI MESSAGE QUEUE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

	public useAntiMsgQ = (() => {
		const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r,  ) => {
			const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});

			const senderTokenRef = this.atom(_sidi()); // this.j.root._id.b();

			this.sub((i: A) => {
				const incoming = i.p;
				if (incoming?.from === senderTokenRef.value) return;
				this.hub.state.set(k,incoming.value, { triggerRender: false,  s: s, ms_reg: m, r: r });
				// set(k, incoming.value, false, s, m, r);
				setState(incoming.value);
			}, k, s, m, r, ); // stay in sync with local writes too (including our own setLive below)
			this.hub.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, ms_reg: m, r: r })
			// sub(k, (newValue: any) => setState(newValue), s, m, r);

			const setLive = (next: any) => {
				const newValue = typeof next === "function" ? next(this.hub.state.get(k, { s: s, ms_reg: m, r: r })) : next;
				setState(newValue);
				this.hub.state.set(k,newValue, { triggerRender: true,  s: s, ms_reg: m, r: r });
				// set(k, newValue, true, s, m, r); // local write
				this.send(k,{value: newValue, from: senderTokenRef.value},  s, m, r); // broadcast to the bus
			};

			return [state, setLive] as const;
		};
		fn.mailbox = (v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r) => {
			const [state, setState] = useState(() => {return this.state_setter(v, k, s, m, r);});

			const senderTokenRef = this.atom(_sidi()); 
			const queueRef = this.atom<Queue | null>(null);

			if (!queueRef.current) {
				queueRef.current = new Queue(1);
			}

			// mailbox — replace the whole useEffect block:
			this.sub((i: A) => {
				const incoming = i.p;
				if (incoming?.from === senderTokenRef.value) return;
				queueRef.current!.run(async () => {
					const current = this.hub.state.get(k, { s: s, ms_reg: m, r: r })
					const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
					this.hub.state.set(k,next, { triggerRender: false,  s: s, ms_reg: m, r: r });
					return next;
				});
			}, k, s, m, r,);

			this.hub.state.subscribe(k, (newValue: any) => setState(newValue), { s: s, ms_reg: m, r: r })

			const setLive = (next: any) => {
				const t = typeof next === "function" ? next() : next;
				this.send(k,{value: t, from: senderTokenRef.value},  s, m, r);
			};

			return [state, setLive] as const;
		};
		return fn
	})();
	// #endregion
	//                                                                                                                        ASYNC
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public async = (() => {
		const fn = <T,>(k: A, fetcher: (signal: AbortSignal) => P<T>, s = this.s, m = this.m, r = this.r, options?: { fallback?: T }) => {
			const reg = this.hub.atom_reg(s, m, r);
			let atom = reg.get(k) as SIGNAL_CELL<T> | UD;
	
			if (!atom) {
				atom = this.create(k, options?.fallback as T, { trigger_render: false }, s, m, r) as SIGNAL_CELL<T>;
			}
	
			const dataKey = `${String(k)}:data`;
			const loadingKey = `${String(k)}:loading`;
			const errorKey = `${String(k)}:error`;
	
			if (!this.has(dataKey, s, m, r)) this.create(dataKey, options?.fallback, undefined, s, m, r);
			if (!this.has(loadingKey, s, m, r)) this.create(loadingKey, false, undefined, s, m, r);
			if (!this.has(errorKey, s, m, r)) this.create(errorKey, null, undefined, s, m, r);
	
			const run = async (): Promise<T> => {
				const state = atom!.async;
				if (!state) throw new Error(`[Sauron] Missing async state for "${String(k)}"`);
	
				if (state.controller) state.controller.abort();
				const controller = new AbortController();
				state.controller = controller;
	
				this._writeResolved(loadingKey, true, "write", s, m, r, String(k));
	
				try {
					const data = await fetcher(controller.signal);
					if (!controller.signal.aborted) {
						this._writeResolved(dataKey, data, "write", s, m, r, String(k));
						this._writeResolved(errorKey, null, "write", s, m, r, String(k));
					}
					return data;
				} catch (error) {
					if (!controller.signal.aborted) {
						this._writeResolved(errorKey, error, "write", s, m, r, String(k));
						atom!.lifecycle?.onError?.(error as Error);
					}
					throw error;
				} finally {
					if (!controller.signal.aborted) {
						this._writeResolved(loadingKey, false, "write", s, m, r, String(k));
						state.controller = null;
					}
				}
			};
	
			atom.async = { fetcher: run, controller: null, retryCount: 0, staleValue: options?.fallback };
			void run();
	
			return { dataKey, loadingKey, errorKey };
		};
		fn.invalidate = <T,>(key: A, s = this.s, m = this.m, r = this.r): V => {
			const reg = this.hub.atom_reg(s, m, r);
			const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
			const state = atom?.async;
			if (!atom || !state) return;
	
			const dataKey = `${String(key)}:data`;
			const loadingKey = `${String(key)}:loading`;
			const errorKey = `${String(key)}:error`;
	
			this._writeResolved(loadingKey, true, "write", s, m, r, String(key));
			this._writeResolved(errorKey, null, "write", s, m, r, String(key));
	
			void Promise.resolve(state.fetcher())
				.then((data) => this._writeResolved(dataKey, data, "write", s, m, r, String(key)))
				.catch((error) => {
					this._writeResolved(errorKey, error, "write", s, m, r, String(key));
					atom.lifecycle?.onError?.(error as Error);
				});
		};
		fn.mutate = <T,>(key: A, optimistic: T, s = this.s, m = this.m, r = this.r): V => {
			const reg = this.hub.atom_reg(s, m, r);
			const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
			const state = atom?.async;
			if (!atom || !state) return;
	
			const dataKey = `${String(key)}:data`;
			const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
			if (!dataAtom) throw new Error(`[Sauron] Missing async data signal "${dataKey}"`);
	
			const prior = dataAtom.peek();
			this._writeResolved(dataKey, optimistic, "write", s, m, r, String(key));
	
			void Promise.resolve(state.fetcher()).catch(() => {
				this._writeResolved(dataKey, prior, "write", s, m, r, String(key));
			});
		};
		fn.read = <T,>(key: A, s = this.s, m = this.m, r = this.r): T => {
			const reg = this.hub.atom_reg(s, m, r);
			const dataKey = `${String(key)}:data`;
			const loadingKey = `${String(key)}:loading`;
			const errorKey = `${String(key)}:error`;
	
			const errorAtom = reg.get(errorKey) as SIGNAL_CELL<A> | UD;
			const error = errorAtom?.peek();
			if (error) throw error;
	
			const loadingAtom = reg.get(loadingKey) as SIGNAL_CELL<boolean> | UD;
			if (loadingAtom?.peek()) {
				throw new Promise<void>((resolve) => {
					const unsub = loadingAtom.subscribe((change) => {
						if (!change.current) {
							unsub();
							resolve();
						}
					});
				});
			}
	
			const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
			return dataAtom?.peek() as T;
		};
		return fn;
	})();
	// #endregion
	//                                                                                                                   PERSISTANCE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public persist = async <T,>(key: A, value: T, s = this.s, m = this.m, r = this.r, options?: { serialize?: (v: T) => S }) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public unpersist = async (key: A, s = this.s, m = this.m, r = this.r) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public transaction = (block: () => V): V => {
		// REDO WITH NEW ATOM SOLUTION
	};
	// #endregion
	//                                                                                                                     TIMELINE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public _time_travel_reverting = false;
	public time_travel = (() => {
		const fn = <T = unknown>(key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels(), ): readonly HistoryEntry<T>[] => {
			return this.time_travel.resolveHistory<T>(key, channels) ?? [];
		};
		fn.defaultChannels = (): Required<HistoryChannels> => ({
			r: this.r,
			m: this.m,
			s: '_TC_TIME_TRAVEL_',
		});
		fn.resolveChannels = (channels: HistoryChannels = this.time_travel.defaultChannels(),	): Required<HistoryChannels> => ({
			...this.time_travel.defaultChannels(),
			...channels,
		});
		fn.resolveHistory = <T = unknown>( key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels(),	) => {
			const { s, m, r } = this.time_travel.resolveChannels(channels);
			const meta = this.hub.state.getMetadata(s, m, r);
			return meta._history.get(key) as HistoryEntry<T>[] | undefined;
		};
		fn.replay = <T = unknown>(key: SignalKey, value: T, channels: HistoryChannels, source: string,	): T => {
			const { s, m, r } = this.time_travel.resolveChannels(channels);
			this._time_travel_reverting = true;
			try {
				this.hub.store.set(key, value, {
					s: s,
					ms_reg: m,
					r: r,
					triggerRender: true,
				});
				return value;
			} finally {
				this._time_travel_reverting = false;
			}
		};
		fn.graft = (drs: A) => {
			drs.time_travel = {
				back: this.time_travel.back,
				forward: this.time_travel.forward,
				goto: this.time_travel.goto,
				history: this.time_travel,
			};
	
			return drs.time_travel;
		};
		fn.back = <T = unknown>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels(),	): T | undefined => {
			const history = this.time_travel.resolveHistory<T>(key, channels);
			if (!history?.length || steps <= 0) return undefined;
		
			const { s, m, r } = this.time_travel.resolveChannels(channels);
			const meta = this.hub.state.getMetadata(s, m, r);
			const index = meta._timeTravel.get(key) ?? history.length - 1;
			const targetIndex = Math.max(0, index - steps);
		
			if (targetIndex === index) return undefined;
		
			meta._timeTravel.set(key, targetIndex);
			for (let i = 0; i < history.length; i++) {
				history[i].undone = i > targetIndex;
			}
		
			return this.time_travel.replay(key, history[targetIndex].prev as T, channels, "time-travel.back");
		};
		fn.forward = <T = unknown>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels(),	): T | undefined => {
			const history = this.time_travel.resolveHistory<T>(key, channels);
			if (!history?.length || steps <= 0) return undefined;
		
			const { s, m, r } = this.time_travel.resolveChannels(channels);
			const meta = this.hub.state.getMetadata(s, m, r);
			const index = meta._timeTravel.get(key) ?? history.length - 1;
			const targetIndex = Math.min(
				history.length - 1,
				index + steps,
			);
		
			if (targetIndex === index) return undefined;
		
			meta._timeTravel.set(key, targetIndex);
			for (let i = 0; i < history.length; i++) {
				history[i].undone = i > targetIndex;
			}
		
			return this.time_travel.replay(key, history[targetIndex].next as T, channels, "time-travel.forward");
		};
		fn.goto = <T = unknown>( key: SignalKey, sequence: number, channels: HistoryChannels = this.time_travel.defaultChannels(),	): T | undefined => {
			const history = this.time_travel.resolveHistory<T>(key, channels);
			if (!history?.length) return undefined;
		
			const { s, m, r } = this.time_travel.resolveChannels(channels);
			const meta = this.hub.state.getMetadata(s, m, r);
			const targetIndex = history.findIndex((entry) => entry.k === sequence);
			const index = meta._timeTravel.get(key) ?? history.length - 1;
		
			if (targetIndex < 0 || targetIndex === index) return undefined;
		
			meta._timeTravel.set(key, targetIndex);
			for (let i = 0; i < history.length; i++) { history[i].undone = i > targetIndex; }
		
			return this.time_travel.replay(key, history[targetIndex].goto as T, channels, "time-travel.goto");
		};
		return fn;
	})();
	public recording: { entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>; unsubs: Array<() => V> } | null = null;
	public timeline = (() => {
		const fn = (key: A, s = this.s, m = this.m, r = this.r) => {

		}
		fn.manualReplay =(entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, options?: { from?: number; speed?: number; triggerRender?: boolean }): Promise<V> => {
			const { from = 0, speed = 0, triggerRender = true } = options ?? {};
			return new Promise((resolve) => {
				let index = from;
				const step = () => {
					if (index >= entries.length) { resolve(); return; }
					const entry = entries[index++];
					this._writeResolved(entry.key, entry.value, entry.source ?? "replay", this.s, this.m, this.r, `replay:${index}`);
					if (speed > 0) { setTimeout(step, speed); }
					 else { step(); }
				};
				this.RX_S.startBatch();
				try { step(); } 
				finally { this.RX_S.endBatch(); }
				if (speed === 0) { resolve(); }
			});
		}
		fn.manualBack = (key: A, s = this.s, m = this.m, r = this.r): void => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.manualForward = (key: A, s = this.s, m = this.m, r = this.r): void => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.manualRecord=(keys?: SignalKey[], s: S = this.s, m: S = this.m, r: S = this.r): () => V =>{
			/** USAGE
			const stop = sauron.record();
		
			// ...user reproduces the bug...
			
			const entries = stop();
			
			// Replay it exactly:
			await sauron.replay(entries);
			
			// Or find where reality diverged:
			sauron.bisect(entries);
			 */
			if (this.recording) {throw new Error("[Sauron] A recording is already active");}
	
			const registry = this.hub.atom_reg(s, m, r);
			const targets = keys ?? [...registry.keys()];
	
			const entries: Array<{key: SignalKey; value: A; previous: A; timestamp: number; source?: S; }> = [];
	
			const unsubs = targets.map((key) => {
				const atom = registry.get(key) as SIGNAL_CELL | undefined;
				if (!atom) return () => {};
	
				return atom.subscribe((change) => {
					entries.push({
						key,
						value: change.current,
						previous: change.previous,
						timestamp: Date.now(),
						source: change.source,
					});
				});
			});
	
			this.recording = { entries, unsubs };
	
			return () => this.timeline.stopManualRecording();
		}
		fn.stopManualRecording=(): Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> =>{
			if (!this.recording) return [];
	
			for (const unsub of this.recording.unsubs) {
				unsub();
			}
	
			const entries = this.recording.entries;
			this.recording = null;
	
			return entries;
		}
		return fn;
	})();
	public snapshot = (() => {
		const fn = () => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.restore = (snapshot: Record<string, A>): void => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.diff = (a: Record<string, A>, b: Record<string, A>) => {
			// REDO WITH NEW ATOM SOLUTION
		};
		fn.applyPatch = (patch: Record<string, { from: A; to: A }>): void => {
			// REDO WITH NEW ATOM SOLUTION
		};
		return fn;
	})();
	public getCausalChain(key: A) {
		// REDO WITH NEW ATOM SOLUTION
	}
	public conflictPolicy<T>(key: A, policy: ConflictPolicy): void {
		// REDO WITH NEW ATOM SOLUTION
	}
	public declareContract(name: string, invariant: (snapshot: Record<string, A>) => boolean, key: A, s = this.s, m = this.m, r = this.r, options?: { block?: boolean }): void {
		// REDO WITH NEW ATOM SOLUTION
	}
	public removeContract(name: string): void {
		// REDO WITH NEW ATOM SOLUTION
	}
	public createScope = (id: string) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public effectOnce = (keys: A[], predicate: (...values: A[]) => boolean, fn: (...values: A[]) => void) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	// #endregion
	//                                                                                                                   SRC / DEST
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public readonly SRC_DEFAULTS: rts = { sl: this.s, ms: this.m, rt: this.r };
	public readonly DST_DEFAULTS: rts = { sl: this.s, ms: this.m, rt: this.r };
	public filter = <T,>(srcKey: A, destKey: A, pred: (v: T) => B, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 return this.subscribe<T>(
			 sk,
			 v => {
				 if (pred(v)) this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public throttle = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 let last = 0;
		 return this.subscribe(
			 sk,
			 v => {
				 const now = Date.now();
				 if (now - last >= m) {
					 last = now;
					 this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
				 }
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public merge = (srcKeys: A[], destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**	const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
	 const unsubs = srcKeys.map(k => {
		 const sk = this.resolveKey(k, src.s, src.m, src.r);
		 return this.subscribe(sk, v => this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r), src.s, src.m, src.r);
	 });
	 return () => unsubs.forEach(u => u()); */
	};
	public zip = (srcKeys: A[], destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**
		 * const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
		const emitted = new Map<string, A>();
		const sks = srcKeys.map(k => this.resolveKey(k, src.s, src.m, src.r));

		const unsubs = sks.map(sk =>
			this.subscribe(
				sk,
				v => {
					emitted.set(sk, v);
					if (sks.every(k => emitted.has(k))) {
						this._writeResolved(
							dk,
							sks.map(k => emitted.get(k)),
							"derived",
							sk,
							dest.s,
							dest.m,
							dest.r,
						);
						emitted.clear();
					}
				},
				src.s,
				src.m,
				src.r,
			),
		);

		return () => unsubs.forEach(u => u());
		 */
	};
	public scan = <T, A>(srcKey: A, destKey: A, reducer: (a: A, v: T) => A, seed: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**
		 * const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);

		let acc = seed;
		this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);

		return this.subscribe<T>(
			(v: T) => {
				acc = reducer(acc, v);
				this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);
			},
			sk,
			src.s,
			src.m,
			src.r,
		);
		 */
	};
	public switch = (srcKey: A, destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 let inner: (() => V) | null = null;
		 return this.subscribe(
			 sk,
			 (targetKey: A) => {
				 if (inner) inner();
				 const k = this.resolveKey(targetKey, src.s, src.m, src.r);
				 inner = this.subscribe(k, v => this._writeResolved(dk, v, "derived", k, dest.s, dest.m, dest.r), src.s, src.m, src.r);
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public buffer = (srcKey: A, destKey: A, count: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 const buf: A[] = [];
		 return this.subscribe(
			 sk,
			 v => {
				 buf.push(v);
				 if (buf.length >= count) {
					 this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
					 buf.length = 0;
				 }
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public window = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 const buf: A[] = [];
		 const iv = setInterval(() => {
			 if (buf.length) {
				 this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
				 buf.length = 0;
			 }
		 }, m);
 
		 const unsub = this.subscribe(sk, v => buf.push(v), src.s, src.m, src.r);
		 return () => {
			 unsub();
			 clearInterval(iv);
		 }; */
	};
	public distinct = <T,>(srcKey: A, destKey: A, equals?: (a: T, b: T) => B, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**	const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
	 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);

	 let last: T | UD;
	 return this.subscribe<T>(
		 sk,
		 v => {
			 const same = last !== undefined && (equals ? equals(last, v) : Object.is(last, v));
			 if (!same) {
				 last = v;
				 this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
			 }
		 },
		 src.s,
		 src.m,
		 src.r,
	 ); */
	};
	public audit = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 let timer: RET<typeof setTimeout> | null = null;
		 let latest: A;
		 return this.subscribe(
			 sk,
			 v => {
				 latest = v;
				 if (!timer) {
					 timer = setTimeout(() => {
						 this._writeResolved(dk, latest, "derived", sk, dest.s, dest.m, dest.r);
						 timer = null;
					 }, m);
				 }
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};

	// #endregion
	//                                                                                                                    CONSTANTS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public _sauronIdCounter = 0;
	public _auth_initialized = false;
	public readonly QUEUE_KEY = "staff:sends:offline:queue";
	public deadLetters: SIG_BUS_Message[] = [];
	public _schemas = new Map<S, SIG_BUS_Schema>();
	public defaultPriority: A = "normal"; // CSEPriority
	public middleware: ((msg: SIG_BUS_Message) => B | V)[] = [];
	public groups = new Map<S, { channels: Set<S>; enabled: B; middleware: ((msg: SIG_BUS_Message) => B | V)[] }>();
	public queue: SIG_BUS_Message[] = [];
	public seen = new Set<S>();
	public globalMiddleware: { beforeSend: ((msg: SIG_BUS_Message) => B | V | P<B | V>)[]; afterSend: ((msg: SIG_BUS_Message, result: U) => V | P<V>)[]; beforeReceive: ((msg: SIG_BUS_Message) => B | V | P<B | V>)[]; afterReceive: ((msg: SIG_BUS_Message, result: U) => V | P<V>)[]; onError: ((err: Error, msg?: SIG_BUS_Message) => V | P<V>)[] } = { beforeSend: [], afterSend: [], beforeReceive: [], afterReceive: [], onError: [] };
	// #endregion
}
export const signals = new SIGNALS()
// #endregion 
//                                                                                                                       SIGNAL_CELL
// #region ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
let ACTIVE_SIGNALS: SIGNALS | null = null;

class SIGNAL_CELL<T = any> {
	public node: SignalNode<T>;
	public version = 0;
	public lastWriteSource: S | undefined = undefined;
	public causal: CausalEntry[] = [];
	public history: AtomHistoryEntry<T>[] = [];
	public sealed = false;
	public frozen = false;
	public schema: SignalSchema | undefined = undefined;
	public triggerRender: B;
	public async: { fetcher: () => P<T>; controller: AbortController | null; retryCount: N; staleValue: T | undefined } | null = null;
	public lifecycle: { onError?: (error: Error) => V; onSettled?: (value: T) => V } = {};
	private listeners = new Set<(change: { key: SignalKey; value: T; current: T; previous: T; version: N; source?: S }) => void>();
	private historyLimit = 100;

	constructor(
		public readonly key: SignalKey,
		public readonly initial: T,
		public readonly s: S,
		public readonly m: S,
		public readonly r: S,
		options?: SignalOptions,
	) {
		this.triggerRender = options?.trigger_render !== false;
		this.node = {
			currentValue: initial,
			pendingValue: initial,
			subs: undefined,
			subsTail: undefined,
			flags: ReactiveFlags.Mutable,
		} as SignalNode<T>;
	}

	private get rx(): RX_SCHEDULER | null { return ACTIVE_SIGNALS?.RX_S ?? null; }
	private get currentValue(): T { return (this.node as A).currentValue as T; }
	private setCurrentValue(value: T): V { (this.node as A).currentValue = value; }

	/** Tracked read. Registers the caller (effect/computed) as a subscriber. */
	public read(): T {
		const rx = this.rx;
		if (!rx) return this.currentValue;
		const value = rx.signalOper(this.node as SignalNode<T>) as T;
		return value === undefined ? this.currentValue : value;
	}

	/** Untracked read. Never links, never notifies. */
	public peek(): T {
		const pending = (this.node as A).pendingValue as T | undefined;
		return pending === undefined ? this.currentValue : pending;
	}

	public write(value: T | ((previous: T) => T), options?: { source?: S; cause?: S; triggerRender?: B; recordHistory?: B }): T {
		if (this.sealed) throw new Error(`[Sauron] Signal "${String(this.key)}" is sealed`);
		if (this.frozen) throw new Error(`[Sauron] Signal "${String(this.key)}" is frozen`);

		const previous = this.peek();
		const next = typeof value === "function" ? (value as (previous: T) => T)(previous) : value;
		if (Object.is(previous, next)) return previous;

		const rx = this.rx;
		if (rx) {
			rx.signalOper(this.node as SignalNode<T>, next);
			this.setCurrentValue(next);
		} else {
			(this.node as A).pendingValue = next;
			this.setCurrentValue(next);
		}

		this.version++;
		this.lastWriteSource = options?.source;

		if (options?.recordHistory !== false) {
			this.history.push({ value: next, previous, timestamp: Date.now(), source: options?.source, cause: options?.cause } as AtomHistoryEntry<T>);
			if (this.history.length > this.historyLimit) this.history.shift();
		}

		for (const listener of [...this.listeners]) {
			try {
				listener({ key: this.key, value: next, current: next, previous, version: this.version, source: options?.source });
			} catch (error) {
				console.error(`[Sauron] listener failed for "${String(this.key)}":`, error);
			}
		}

		return next;
	}

	public subscribe(fn: (change: { key: SignalKey; value: T; current: T; previous: T; version: N; source?: S }) => void): () => void {
		this.listeners.add(fn);
		return () => { this.listeners.delete(fn); };
	}

	public subscribeExternal(fn: (change: { key: SignalKey; value: T; current: T; previous: T; version: N; source?: S }) => void): () => void {
		return this.subscribe(fn);
	}

	public getHistory(): readonly AtomHistoryEntry<T>[] { return this.history; }

	public setSchema(schema: SignalSchema): V { this.schema = schema; }
	public setSealed(): V { this.sealed = true; }
	public setFrozen(): V { this.frozen = true; }

	reseed(value: T): V {
		(this.node as A).currentValue = value;
		(this.node as A).pendingValue = value;
	}
}
const def = new class DEFINITION {
	public MASTER_ATOM_KEYS = ["ATOM", "MODULE_ATOM", "CLOUD_ATOM", "SAURON_ATOM", "DRS_ATOM", "DUPLEX_ATOM", "SLAB_ATOM", "STREAM_ATOM", "S_CLASS_ATOM", "SPREADSHEET_ATOM", "SEND_ATOM", "VERTEX"] as const;
	public MQ = {
		RETRY: this.DEFINE_ATOM_KIND("retry"),
		ACK: this.DEFINE_ATOM_KIND("ack"),
		DEADLINE: this.DEFINE_ATOM_KIND("deadline"),
		DEDUPE: this.DEFINE_ATOM_KIND("dedupe"),
		DEPTH: this.DEFINE_ATOM_KIND("depth"),
		WILDCARD: this.DEFINE_ATOM_KIND("wildcard"),
		REPLY: this.DEFINE_ATOM_KIND("reply"),
		WINDOW: this.DEFINE_ATOM_KIND("window"),
		RESUME: this.DEFINE_ATOM_KIND("resume"),
		CREDIT: this.DEFINE_ATOM_KIND("credit"),
		CORRELATION: this.DEFINE_ATOM_KIND("correlation"),
	} as const;
	public ELECTRONS = {
		ADAPTER_ELECTRON: this.DEFINE_ATOM_KIND("adapter"),
		BEAM_CODEC_ELECTRON: this.DEFINE_ATOM_KIND("beam_codec"),
		BINARY_ELECTRON: this.DEFINE_ATOM_KIND("binary"),
		BYTE_CODE_VM: this.DEFINE_ATOM_KIND("byte_code_vm"),
		CHANNEL_ELECTRON: this.DEFINE_ATOM_KIND("channel"),
		CHANNELS_ELECTRON: this.DEFINE_ATOM_KIND("channels"),
		CONCURRENCY_ELECTRON: this.DEFINE_ATOM_KIND("concurrency"),
		DEADLETTER_ELECTRON: this.DEFINE_ATOM_KIND("deadletter"),
		DESTRUCTION_ELECTRON: this.DEFINE_ATOM_KIND("destruction"),
		ERROR_ELECTRON: this.DEFINE_ATOM_KIND("error"),
		EVENT_ELECTRON: this.DEFINE_ATOM_KIND("event"),
		EXTERNAL_SUBSCRIPTION_ELECTRON: this.DEFINE_ATOM_KIND("exterenal_sub"),
		GATE_ELECTRON: this.DEFINE_ATOM_KIND("gate"),
		GLOBAL_SUBSCRIPTION_ELECTRON: this.DEFINE_ATOM_KIND("global_sub"),
		GOVERNOR_ELECTRON: this.DEFINE_ATOM_KIND("GOVERNOR"),
		HOOK_ELECTRON: this.DEFINE_ATOM_KIND("hook"),
		HTTP_TRANSPORT: this.DEFINE_ATOM_KIND("http_transport"),
		INJECTION_CODEC_ELECTRON: this.DEFINE_ATOM_KIND("injection"),
		METRICS_ELECTRON: this.DEFINE_ATOM_KIND("metrics"),
		MIDDLEWARE_ELECTRON: this.DEFINE_ATOM_KIND("middlleware"),
		RIFTS_ACCESS_ELECTRON: this.DEFINE_ATOM_KIND("rifts"),
		REACTIVE_ELECTRON: this.DEFINE_ATOM_KIND("reactive"),
		ROUTE_ELECTRON: this.DEFINE_ATOM_KIND("route"),
		RUN_ON_ELECTRON: this.DEFINE_ATOM_KIND("run_on"),
		SEALED_ERROR_ELECTRON: this.DEFINE_ATOM_KIND("sealed_error"),
		SESSION_ELECTRON: this.DEFINE_ATOM_KIND("session"),
		SHELL_ELECTRON: this.DEFINE_ATOM_KIND("reshelltry"),
		SIGNAL_ELECTRON: this.DEFINE_ATOM_KIND("signal"),
		SOA_QUAD_TIERED_REGISTRY_ELECTRON: this.DEFINE_ATOM_KIND("soa_registry"),
		SUBSCRIPTION_ELECTRON: this.DEFINE_ATOM_KIND("subscription"),
		TRANSFORM_ELECTRON: this.DEFINE_ATOM_KIND("transform"),
		WAITER_ELECTRON: this.DEFINE_ATOM_KIND("waiter"),
	} as const;
	public NEUTRONS = {
		COMPRESSION_NEUTRON: this.DEFINE_ATOM_KIND("compression"),
		CONFLICT_NEUTRON: this.DEFINE_ATOM_KIND("conflict"),
		CONTEXT_NEUTRON: this.DEFINE_ATOM_KIND("context"),
		ENCRYPTION_NEUTRON: this.DEFINE_ATOM_KIND("encryption"),
		GROUPS_NEUTRON: this.DEFINE_ATOM_KIND("groups"),
		HISTORY_NEUTRON: this.DEFINE_ATOM_KIND("history"),
		META_NEUTRON: this.DEFINE_ATOM_KIND("meta"),
		OFFSEC_NEUTRON: this.DEFINE_ATOM_KIND("offsec"),
		PERMISSIONS_NEUTRON: this.DEFINE_ATOM_KIND("permissions"),
		PERSISTENCE_NEUTRON: this.DEFINE_ATOM_KIND("persistence"),
		PRIORITY_NEUTRON: this.DEFINE_ATOM_KIND("priority"),
		SNAPSHOT_NEUTRON: this.DEFINE_ATOM_KIND("snapshot"),
		TAGS_NEUTRON: this.DEFINE_ATOM_KIND("tags"),
		TIME_TRAVEL_NEUTRON: this.DEFINE_ATOM_KIND("time_travel"),
		TTL_NEUTRON: this.DEFINE_ATOM_KIND("ttl"),
	} as const;
	public PROTONS = {
		CONTRACTS_PROTON: this.DEFINE_ATOM_KIND("contract"),
		REACTIVE_PROTON: this.DEFINE_ATOM_KIND("reactive"),
		REGISTRY_PROTON: this.DEFINE_ATOM_KIND("registry"),
		SCHEMA_PROTON: this.DEFINE_ATOM_KIND("schema"),
		VALUES_REGISTRY_PROTON: this.DEFINE_ATOM_KIND("values_reg"),
	} as const;
	public DEFINE_ATOM_KIND(name: string): symbol {
		return Symbol(name);
	}
	public PARTICLE_KEYS = {
		...this.PROTONS,
		...this.NEUTRONS,
		...this.ELECTRONS,
		...this.MQ,
	} as const;
	public BARE_MASTER_RUNTIME: ATOM_CONTRACT_RUNTIME = { createMaster: (kind, k, s, m, r, value) => ({ k, s, m, r, value }) };
}();

export function atom_contract<T = U, M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>,B extends object = Record<never, never>>(definition: (context: { readonly self: ATOM_CONTRACT_SELF }) => ATOM_CONTRACT_CONFIG<T, M, B>, runtime?: ATOM_CONTRACT_RUNTIME<M>): ATOM_CONTRACT<T, M, B> {// Defines a reusable atom contract. Defining it creates no atom and performs no registration. Calling the returned function creates one master and its declared bindables; the master itself remains owned by the supplied runtime.
	const selected = definition({ self: { atom: "ATOM" } });
	const kind = selected.atom ?? "ATOM";
	if (!def.MASTER_ATOM_KEYS.includes(kind)) { throw new TypeError(`Unknown master atom: ${String(kind)}`); }

	const config: ATOM_CONTRACT_CONFIG<T, M, B> = {...selected,atom: kind };
	const masterRuntime = runtime ?? (def.BARE_MASTER_RUNTIME as ATOM_CONTRACT_RUNTIME<M>);

	const contract = ((initialValue?: T, k?: A, s?: S, m?: S, r?: S) => {
		const rr = r ?? Store.get("THE_WIZARD_ROOT_REG") ?? "THE_WIZARD_ROOT_REG"
		const mm = m ?? Store.get("THE_WIZARD_ROOT_REG") ?? 'SAURON_MASTER_REG'
		const ss = s ?? Store.get("THE_WIZARD_ROOT_REG") ?? 'SAURON_SLAVE_REG'
		const kk = k ?? Store.get("k") ?? 'the_NOT_fast_path'
		const configuredValue = config.value;
		const value = initialValue !== undefined ? initialValue : typeof configuredValue === "function" ? (configuredValue as () => T)() : configuredValue as T;
		const master = masterRuntime.createMaster(kind, kk, ss, mm, rr, value);

		const bindables: B[] = [];
        const operations: B = {} as B;
		
		for (const createBindable of config.bindables ?? []) {
			if (!config.bindables!.includes(createBindable)) {
				throw new TypeError(`Unknown bindable: ${String(createBindable)}`);
			  }

            const bound = createBindable({self: master, kk, ss, mm, rr, });
            bindables.push(bound);
            Object.assign(operations, bound);
        }

		const instance: ATOM_CONTRACT_INSTANCE<T, M, B> = Object.assign(operations, {
            atom: kind, kk, ss, mm, rr, master, bindables,
            get value() {return master.value;},
            set value(next: T) { master.value = next; },
        });

		return instance;
	}) as ATOM_CONTRACT<T, M, B>;

	Object.defineProperties(contract, {
		atom: { value: kind, enumerable: true },
		config: { value: config, enumerable: true },
	});
	return contract;
}/** Marks a reusable bindable factory for inclusion in an atom contract. */

export function atom_bindable<M extends CONTRACT_MASTER = CONTRACT_MASTER, B extends object = Record<never, never>>(factory: ATOM_BINDABLE<M, B>): ATOM_BINDABLE<M, B> {return factory;}

/**
 export const counter = atom_contract<{ count: number }>(() => ({
    atom: "VERTEX",
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
    bindables: [
        atom_bindable<{ increment(): void }>(({ self }) => ({
            increment() {
                self.value.count += 1;
            },
        })),
    ],
}));

const instance = counter({ count: 0 });

instance.increment();
instance.value.count; // 1

export const fastCounter = atom_contract(() => ({
    atom: "VERTEX",
    value: { count: 0 },
}));

export const counter = atom_contract(() => ({
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
}));


*/
// #endregion
//                                                                                                                         [SIGNALS]
// #region ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function gAtom<T>(k: SignalKey, s? :S, m ?:S, r?:S,) {
	const ss = s ?? Store.get('SAURON_SLAVE_REG')
	const mm = m ?? Store.get('SAURON_MASTER_REG')
	const rr = r ?? Store.get('SAURON_SLAVE_REG')
    const registry = signals.hub.atom_reg(ss, mm, rr,);
    return registry.get(k) as SIGNAL_CELL<T> | undefined;
}
export function getAtom<T>(k: SignalKey, v?: T | (() => T), options?: SignalOptions, s? :S, m ?:S, r?:S,): SIGNAL_CELL<T> | undefined {
	const ss = s ?? Store.get('SAURON_SLAVE_REG')
	const mm = m ?? Store.get('SAURON_MASTER_REG')
	const rr = r ?? Store.get('SAURON_SLAVE_REG')
    const registry = signals.hub.atom_reg(ss, mm, rr,);
    let atom = registry.get(k) as SIGNAL_CELL<T> | undefined;

    if (atom || v === undefined) { return atom; }

    const initialValue = typeof v === "function" ? (v as () => T)() : v;
    atom = new SIGNAL_CELL(k, initialValue,ss!,mm!,rr!, options,);
    registry.set(k, atom);
    return atom;
}
export const createAtom = getAtom
export function subAtom<T = A>(label: string, fn: (value: T) => void, s?: A, m?: A, r?: A) { }   // return emitter.sub(fn, label, s, m, r)
export function editAtom() {

}
export function hasAtom() {

}
export function deleteAtom() {

}
// #endregion 
/*  ==========================================================================================================================
	 The Anti Message queue...
	 I guess you can call it a new message protocol, but... an anti message protocol.
	 Comparitively its concept is... just ass backwards when placed beside, or in this instance, inside a message queue itself.
	 The reason is because... there are no subscriberes, in the traditional sense. There is no... send event, where the end
	 point gets pinged or dinged with a message that comes flying in. Theres none of that, and even though... in order to use 
	 this new message protocol, a new primitive wasn't actually required. As it uses another primitive on the bus, but then 
	 now one would learn of this... EXTREMELY ussful, but anarchist msg protocol. 
	 The easiest way to explain it is...
	 Think of a big corpo giant, like the one elliot alderson was trying to take down... For the life of me I can't remember the
	 name... E Corp. Remember the scene where they tapped the FBIs conference call... It's basically that... a conference call.
	 No one actually actively subscribes to it in this case. How it works is you simply place useAntiMsgQ in each place that 
	 needs to partake in with the conference call. And that's its. 
	 For my personal use case, we'll look at the atoms for example and how this one fn allowed me to delete my entire architecture
	 and basically... not deal with any of the same problems everyone else does with thier signals and atoms solutions.
	 The contract, master atom, and each of the bindable atoms... simply just get a couple lines of code.
	 The master is the only one that can actually edit the data on the conference call, where as all the bindables, can only merge 
	 data, or add onto what is already there. 
	 This gives the user a nice place to track that atoms history of ops. 
	 It also gives the few atoms that need a context... an actual context while keeping everyone... stupid, basically.
	 Whenever something needs to happen between the master and one of its atoms, it updates the conference call, and it ripples 
	 out to each of the bound atoms, once it reaches them, each atom will check to see if they are effected by this event.
	 If not they just continue fucking the dog... if they are effected they do the thing they are triggered in regards to,
	 and return a success msg, or not depending on what it is exactly that they are doing.
	 And that is it basically... no one signs up for anything... no heavy ceremony... and it truly allows me to fucking YEEEEET
	 every atom u bind to each, in whatever direction I feel like at the time, and every single master atom u build... 
 	 I'm not fucking joking... Those lil fuckers get YEETED!
   ========================================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [USE ANTI MSG Q] */
/* =================================================================================================================================
	 sauron.surgery('cart:items', (items) => {
	 	items.push({ id: 'test', price: 10 });
	 });
	 
	 sauron.surgeryAt('user', ['profile', 'email'], (email) => email.trim());
	 
	 const restore = sauron.surgeryCheckpoint();
	 	
	 sauron.surgery('cart:total', () => 0);
	 sauron.surgery('cart:items', (items) => []);
	 
	 restore(); // both surgeries undone, graph settles once per undo
    ==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DRS] */
/*  =================================================================================================================================
	 all atom solutoin.... evne tho so many are so drastically differnt they all end up dealing with the sma eissues... like mine for exmaple.. as aweosme as it is right now... ok...

	 i forgot ihavent even told u what it actually
	 its the fastest... most complext atom solution tha twll be built for a veryt long time.... this is done because of a for carefully selected and designed decisions that were not rushed...
	 the fast path.... is the deafult apth... thats the only way you can get bvoth fast and confugrally complex....
	 but this is where it starts to veer from normtal aom solution.... already.. there is 10 master atoms that a user can choose form, the deiciosn comes down to what ur storing exactly... the fast path actually performs beqtter.... running hot... like fucking literlly.. because the fast path instead of a simple signals ... class that holds a value... each atom that get screated via
	 const value = atom(3)
	 THATS LITERLLY.. IT..... but anyways.... each atom, has its own unqieu disruptor as its storage medium... where in the users usage path.... i was getting 750 mill ops/sec.... after that i pushed it even further but its not the user path anymore but i got i tup to 950 and then i hit 900 on the other disruptor that i have....
	 master atoms only contain 2 values
	 just fucking 2... that is it....
	 the keys, and the value of the atom itself... thats the fast path... there are other amaster atoms where thats just no feasable.... whihc is whgy they ainty the fastest... now this is where we could get by... because this is where everything and all atom soltiunos deal with this.. but there are 100 configurable atoms that can be bound to a master atom... now.... typcially... that means you now have to have  afactroy... you have to have some way of working with mthe so thats noather palce for cod eper atom to live... so we are now sitting at two... the atoms itself 3 and any helpre function so r component required for said atoms functionality so four at a very minumiin.. ... how the fuck do u make this... so its not a giant fucking bag of dog shit.... becasuse with 4+ plus places... it becomes brittel with many break points that can occur since every spit u place code...one more place for it to fail...huge fucking msess... then u got toi store these fucking thhings... master atoms and its bound atom..... whihc ppl usinly reach for a graph.... but thats for the poors... because its slow and gay.....

	 my genius so so fucking colassaly large.... so dense... and just... never stops vibrating due its raw unpotinatoinl power.... it generates gravity... it trruly does....

	 how about no?.... to all of the above... the code for each atom..... lives ina single spot ... insxide its atom, fullstop... no where else... no joke...no regirsty atoms... to mark which master atom.... has what... and then storing those fucking things... which brings me ot my next point..... no storage.. or orginazastion .... of literllty.. AND I FUCKING MEAN THIS... of any kind... so no maps... no nested maps... no helper look up functions.... none of that shit... we are essetnailly.... going to create it for the user... and aiming off in arandom directino each and ervery single time..... fucking yeet it as ahard as u can and hopefully pegs some poors in the head or something....

	 im serious.....no storage.... no tracking of any kind... ok?
	 esesntially that leave sus with...all we reall yhave to build is the contract builder... which its the helpre fucniton for the users to create contracts....so they can create a reuablefucntion to call repeadlty to create custom atoms...

	 thats it..... and before you ask theres something missing here.... like the entire fucking solution... no... thats tthe best part... its not missing... i just dont need it .....

	 trust me.... I'm a fucking wizard....

	 taking, and i awil buidl first class primtive for this, but taking my distrbruted reactive state solution.... and essentially... turning it inside out....for elegantly... each atom and amster atom... only requires 2 lines of code.. when a atom is bound to a master atom, it only needs the master atoms address... so that it can palce itself on the same distrbuted state graph or whatever... channel who the fuck cares... thus.... turning a setter and gettr hook... into basically.... what is a 100 person conference calll... seriously... the master atom whenerv eri it wants somenthing done... it send out a trigger over the state... each atom gets triggered and checks it to see if they are apart of that rounds triggered event... if they are they then go do their thing, return a valuye by mergigin it into the state with their own id so the maste rcan be like ok... fuck  face atom 89 did a succesful exetnion of whatever... obviously not everything needs a rteturn vvalue but if it does... then once the master sees it then only the amster can remove values from the state... and as a default the state of each atom  is bacailly going to be like....a viable registry for th euser to look at, giving the user a hsitoryy of all action sdodnewith that atom.....so we basilly... alt delted... 80% of the ops that would other wixe need to be required to do eeach and every single time someone touch an atom in any capacity.... and at the same time... it completly drops... A SHIT TON of issues that plague every single atoms solution... becuase we simply... jsut do not have the archtecutre thre to give us the problems in the frist place.. LOL... so before... my atroms solution already outperfromaned everyone by massive margin... this just shits on aevreyone becasue once ur in disruptor areas of perfmance... u need to treat every signel op... like its the literlly....last drop of water u will ever have... and this sessentiallyliterlly deleted 80% of the required aerchtecvture that everyopne deals with.... 

	 the distrubted state and this is where we get our dick impant... becasuye right now our dick sare huge... but tthey are about to get even bigger...
	 the distruibted state... at first i was gonng to sue signals as a transport protocol ... but a custom one so it didnt create an atom like it does now with the current transport adapter... but then i though..... what do we usually do when we want to gofucking all ball sout... and jkust fucking hammer it... lets take our fasteest disruptor.... and that is what the signals soltuiion will run off in place of the typcial sinngals mediunm... IN ADDITOIN TO THAT..... if we look at everyhting right now... take a step back .... really look at it... like fuck man... if this were a race... everyone would be you ... know 5 miles into the race... meanwhile.. we are looking back at them..... from fucking pluto....CAN WE TAKE THIS FUCKING FARTHER THOUGH? like really.... FUCK YAAAA.....the entiresoltuioin.... every single map, every singlt set... every single boolean... gone... fucking hgone... ok/?.... avor encoder....cbor as a back up... meanwhile... everything is converted...  to byte op code... no json encoders or anything.....


	 no ... the master does not know... who is bound to it... the only palce where there is a record of that is the contrac titself.... the master atom doesnt evne have a way to check...  and the contract is also how th euser aactiastes functionality... so just calling th econtract via a straight call.. creates a an atom whatever it is that they defined.. and then nested within it... are the functions that deal with the bound atoms.... so really... the master atom couldnt not be any fucking dumber... which is hoenstly... a  great thing to be frank... like actually...

	 noj the contrac t is not the registry... u create amaster atom... adn then say u need to bind 6 other atoms to it... yuou create them and simply just pass the masters addres to it.... and then fucking yeet it... in whatever direciotn u  want

	 this ended up being so fucking elegant .... like u must admit... my fucking genius... is like it truly generates gravity........

	 dude... llol... that is such a n easy fucing thing to do... if u fuck that up... im seriously never fucking using u again... like i literly deltedd 90% of the soluitioun.... more then that really.... 
	 so i already have the disruptro backend... 95% built in preprartion for this day.... the master atoms... bovioiusly wil need some ediitng.... i just simply need to change a couple of things to hook the signals soltuioiiun to th edisrupotr.... i already have the drs primtimves... that i m going to copy over... and then adjust for the new system... becasue thre is going to be 2 paths.... for the distrubted state... and this gets even wilder.. ones disruptro... and ones actual signals... so super fast.....BUT.... the state is going to be able to be shared.... among node modules... so if we have 4  entirely differnt nod e modules... typcailly a pain in the fucking ass to cordinate anything between them.. can now all hook up to the same stateand signals solutoin basically...  and ill builkd amini solution that connects on the eother end... the db aspect... becasue dude this ssoltuion is fucking wild...is virtuall ydone... also waiting for this day.... its actually dbs going into it LOL.....AND THEN ... the its final resting place.... is going to be beside JEDI, which is one of the buses in platformas bus mesh... where it si goiing to be called SITH... because the disruptor is named the power of two.. and it fits that its going after raw power... and as it takes tha tplace... it will also hgave  a bus... that is the coutner part to jedi... jedi is extremly feature rich... obivosuly slwoer because o fit.... sith.. is going to mirrror the api... but with zero fucking features..... AND the distribted spreadsheet engine.... will move OFF OF jedi... to come over to live on sith... because the distribted spreadsheet engine deals with mostly..... numbers... and who fiends over numbers ?.... disruptors.... so the disruptro wil 100% pwoer the spreadsheet engine.... AND.....it will also provide the mesh.... the disruptor transport adapter... that it has been waiting for.,..... so 3 dbs... a signals solution..and atoms solutoin... a spread sheet engine... tranport aeddapters... OH.... so this itnhg is not only get=ting a disruptor per master atom... but each op..... that needs to happen... is getting its OWN disrupotor ND its own QUEUE.... SO instead of doing 15 ops passes bakc to back to back... thats reall yinnefecint... instead its going to be a single pass....15 wide... so while there is not asingle disrupotr in any other fucking platform or atoms oslution.. and if we inlcude the same node, differenty app trapsnort adapter... this will have at a minimin at all times.. 17 disrtuptors... not inclduing any that get created via a fast pass atom......

	 oh and the platforms reactivity / epoch clock... is also getting converted over... man see i told u my fucking genius....... gravity.... heavy geavity.... so the the epoch clock... deleted... instead there wil be a tracer particle... that is going to be placed with the data on the disrutptor.... there is no deinfed constatn.... in relation to the clocks counter... because.... that counter... actually travel swith the particle round iteself since it can hold its own value.... so each revolution it ++s one.... now this dletes all ops costs  and A SHIT TON ... of problems where a wall clock or epohch suusaly has to deal with.... we also get free ..... what sth ewor dim looking for.... essentially if the node experoiiecnes choking or perofmrance dip or all the resources basically get used up....because the tracer particle is trableing WITH THE DATA.... the clock itself.. also slows down... so its never out of sync.... now this also unlcoks a redicklous amoount of other free features... ie... there will also be a 7 day roating state.. where on the 7th day it writes over tiself... the only issue is... at 750 million oops per second..... how th efuck do u define a day? u cant... which.... actually ends up being a massive plus... if its a hotter app... it jsutsimply has a faste r7 dayt like roration... and ther e will be a dial to increase the amounts or decrease... but data...or state.. enver goes stale... whenerve rthe disruptor goes to sleep... the tracer.... ALSO goes to sleep... so the when the clock wakes up... everyithng around it is literlly... as it lerft it l.....s o nothing is stale... and it comes iwth a shit ton o fother abenefits tooo

	 dont worry aobut th disurptor....  i just dleted and toook care of 99% of the soluition... ur fucking job.... ok?.... ur ONE FUCKING JOB.....
	 is the contract....ok?
	 u dont have to worry about fucking where to stroe shit... as thats alrleady taken care of...
	 
	 here is the shape that it needs to end up in
	 
	 export const counter = atom_contract(({ self }: any) => ({
	 ttl: { ms: 60_000 },
	 history: true,
	 snapshot: true,
	 }));
	 
	 SO IM EVEN TAKING THE THOUGHT PROCEESS OF THAT PART AWAY FROM U...  so AGAIN
	 
	 U HAVE ONE FUCKING JOB... ONE FUCKING JOB... DO NOT WORRY ABOUT THE DFISURPOTORS DO NOT FUCKING WORRY ABOUTR THE DISTRUBRTED STATE.... IT IS SO FAR FROM U .... U MIGHT AS WELL BE IN A DIFFEREN TUNIVERSE.... OK?
	 ok... u have to fucking create it.... so that we can add the functionality of the bindable atoms... onto it.... id ont see that here... i dont see much of fucking anything really... THERE ARE 12 MASTER ATOMS.... u didnt cod ein a place... fo rth euser to fucking choose one... and if i can recyucle.... previous attmteps and re use the bidnable atoms.... i already have 80+ of them.... 
U HAV EONE FUCKING JOB.....DUDE... STOP... FUCKIING AROUND.....
	==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DRS] */
	// User-facing trigger-render: force a flush for one key (or everything pending) without A state change. Zero-arg = same as flushSync for pending notifications.
	/**export function rerender(key?: string): void {
	 if (key !== undefined) {
		 //   const hs = graphHookStates.get(key);
		 // if (hs) notifyHook(`${GRAPH_BRIDGE_COMPONENT_ID}:${key}`);
		 notifyHook(key);
	 } else {
		 flushSync();
	 }
	} */


	