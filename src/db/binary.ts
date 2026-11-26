//                                                                                                                       BIANRY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function encodeUtf8(value: string): Uint8Array {
	return new TextEncoder().encode(value);
}
export function decodeUtf8(bytes: Uint8Array): string {
	return new TextDecoder().decode(bytes);
}
export function concatBytes(parts: Uint8Array[]): Uint8Array {
	let length = 0;

	for (const part of parts) {
		length += part.length;
	}

	const output = new Uint8Array(length);
	let offset = 0;

	for (const part of parts) {
		output.set(part, offset);
		offset += part.length;
	}

	return output;
}
export function readUint32(bytes: Uint8Array, offset: number): number {
	return (bytes[offset]! | (bytes[offset + 1]! << 8) | (bytes[offset + 2]! << 16) | (bytes[offset + 3]! << 24)) >>> 0;
}
export function writeUint32(bytes: Uint8Array, offset: number, value: number): void {
	bytes[offset] = value & 0xff;
	bytes[offset + 1] = (value >>> 8) & 0xff;
	bytes[offset + 2] = (value >>> 16) & 0xff;
	bytes[offset + 3] = (value >>> 24) & 0xff;
}
export function readBigUint64(bytes: Uint8Array, offset: number): bigint {
	let value = 0n;

	for (let i = 7; i >= 0; i--) {
		value = (value << 8n) | BigInt(bytes[offset + i]!);
	}

	return value;
}
export function writeBigUint64(bytes: Uint8Array, offset: number, value: bigint): void {
	for (let i = 0; i < 8; i++) {
		bytes[offset + i] = Number((value >> BigInt(i * 8)) & 0xffn);
	}
}
// #endregion
//                                                                                                                          CRC
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
	let value = i;

	for (let bit = 0; bit < 8; bit++) {
		value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
	}

	CRC_TABLE[i] = value >>> 0;
}
export function crc32(bytes: Uint8Array, start = 0, end = bytes.length): number {
	let crc = 0xffffffff;

	for (let i = start; i < end; i++) {
		crc = CRC_TABLE[(crc ^ bytes[i]!) & 0xff]! ^ (crc >>> 8);
	}

	return (crc ^ 0xffffffff) >>> 0;
}
// #endregion
//                                                                                                                         AVRO
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type AVROValue = any;
export type AvroWriteFn = (w: FastAvroWriter, v: A) => V;
export type AvroReadFn = (r: FastAvroReader) => A;
export type AVROField = { name: S; type: AVROSchema };
export type AVROSchema = S | AVROSchema[] | {
    type?: S | AVROSchema[];
    name?: S;
    namespace?: S;
    fields?: AVROField[];
    items?: AVROSchema;
    values?: AVROSchema;
    symbols?: AR<S>;
    size?: N;
};
export class FastAvroWriter {
  buf: Buffer = Buffer.alloc(256);
  pos: N = 0;

  reset(): V { this.pos = 0; }

  private ensure(length: N): V {
    if (this.pos + length <= this.buf.length) return;
    let size = this.buf.length;
    while (size < this.pos + length) size *= 2;
    const next = Buffer.alloc(size);
    this.buf.copy(next, 0, 0, this.pos);
    this.buf = next;
  }

  byte(v: N): V { this.ensure(1); this.buf[this.pos++] = v; }
  bytes(v: Uint8Array): V { this.ensure(v.length); this.buf.set(v, this.pos); this.pos += v.length; }

  long(value: N): V {
    this.ensure(10);
    if (Math.abs(value) <= SAFE_ZIGZAG_LIMIT) {
      let n = value >= 0 ? value * 2 : (-value * 2 - 1);
      while (n >= 128) { this.buf[this.pos++] = (n % 128) | 0x80; n = Math.floor(n / 128); }
      this.buf[this.pos++] = n;
      return;
    }
    let n = BigInt(value);
    n = n >= 0n ? n << 1n : ((-n) << 1n) - 1n;
    while (n >= 0x80n) { this.buf[this.pos++] = Number(n & 0x7fn) | 0x80; n >>= 7n; }
    this.buf[this.pos++] = Number(n);
  }
  float(value: N): V { this.ensure(4); this.buf.writeFloatLE(value, this.pos); this.pos += 4; }
  double(value: N): V { this.ensure(8); this.buf.writeDoubleLE(value, this.pos); this.pos += 8; }

  string(v: S): V {
    const len = Buffer.byteLength(v, "utf8");
    this.long(len);
    this.ensure(len);
    this.buf.write(v, this.pos, "utf8");
    this.pos += len;
  }

  finish(): Buffer { return Buffer.from(this.buf.subarray(0, this.pos)); }
}
export class FastAvroReader {
  pos: N = 0;
  constructor(private readonly buf: Buffer) {}

  byte(): N { return this.buf[this.pos++]; }
  bytes(length: N): Buffer { const out = Buffer.from(this.buf.subarray(this.pos, this.pos + length)); this.pos += length; return out; }

  long(): N {
    let result = 0, shift = 0, b: N;
    do {
      b = this.buf[this.pos++];
      result += (b & 0x7f) * Math.pow(2, shift);
      shift += 7;
    } while (b & 0x80);
    return result % 2 === 0 ? result / 2 : -(result + 1) / 2;
  }
  float(): N { const n = this.buf.readFloatLE(this.pos); this.pos += 4; return n; }
  double(): N { const n = this.buf.readDoubleLE(this.pos); this.pos += 8; return n; }
  string(): S { const len = this.long(); const s = this.buf.toString("utf8", this.pos, this.pos + len); this.pos += len; return s; }
  done(): B { return this.pos === this.buf.length; }
}
export class BINARY_AVRO_CODEGEN {
  private readonly names: OB<AVROSchema> = Object.create(null);
  private readonly writeRoot: AvroWriteFn;
  private readonly readRoot: AvroReadFn;
  private readonly writeError: AvroWriteFn;
  private readonly readError: AvroReadFn;
  private readonly writer = new FastAvroWriter();
  private readonly errorSchema = AVRO_ERROR_SCHEMA;
  constructor(schema: AVROSchema) {
    if (Array.isArray(schema)) throw new Error("Avro: root schema cannot be a union");

    this.collect(schema);
    this.collect(this.errorSchema);

    // Everything below runs exactly ONCE, here, at construction — the generated
    // source text is the whole point: no schema-walking left at encode/decode time.
    const writeBody = this.genWrite(schema, "value").join("\n");
    const readBody = `return ${this.genRead(schema)};`;
    this.writeRoot = new Function("w", "value", writeBody) as A as AvroWriteFn;
    this.readRoot = new Function("r", readBody) as A as AvroReadFn;

    const errWriteBody = this.genWrite(this.errorSchema, "value").join("\n");
    const errReadBody = `return ${this.genRead(this.errorSchema)};`;
    this.writeError = new Function("w", "value", errWriteBody) as A as AvroWriteFn;
    this.readError = new Function("r", errReadBody) as A as AvroReadFn;
  }
  // ---- compile-time schema bookkeeping (never runs after construction) ----
  private collect(schema: AVROSchema): V {
    if (Array.isArray(schema)) { for (const b of schema) this.collect(b); return; }
    if (typeof schema === "string") return;

    if (schema.name) this.names[schema.name] = schema;

    if (Array.isArray(schema.type)) this.collect(schema.type);
    else if (typeof schema.type === "object") this.collect(schema.type);
    else if (typeof schema.type === "string" && ![
      "null", "boolean", "int", "long", "float", "double",
      "bytes", "string", "record", "array", "map", "enum", "fixed",
    ].includes(schema.type)) {
      if (!this.names[schema.type]) throw new Error(`Avro: unknown named type ${schema.type}`);
    }

    if (schema.fields) for (const field of schema.fields) this.collect(field.type);
    if (schema.items) this.collect(schema.items);
    if (schema.values) this.collect(schema.values);
  }
  private resolve(schema: AVROSchema): AVROSchema {
    if (typeof schema === "string" && this.names[schema]) return this.names[schema];
    return schema;
  }
  private kindOf(schema: AVROSchema): S {
    schema = this.resolve(schema);
    if (typeof schema === "string") return schema;
    if (Array.isArray(schema)) return "union";
    return typeof schema.type === "string" ? schema.type : this.kindOf(schema.type);
  }
  // ---- codegen: produces an inline boolean-expression STRING testing a union branch ----
  private genMatch(schema: AVROSchema, expr: S): S {
    schema = this.resolve(schema);
  
    if (typeof schema === "string") {
      switch (schema) {
        case "null": return `(${expr} === null || ${expr} === undefined)`;
        case "boolean": return `typeof ${expr} === "boolean"`;
        case "int": return `typeof ${expr} === "number" && Number.isInteger(${expr})`;
        case "long": case "float": case "double": return `typeof ${expr} === "number"`;
        case "string": return `typeof ${expr} === "string"`;
        case "bytes": return `${expr} instanceof Uint8Array`;
        default: return "false";
      }
    }
  
    if (Array.isArray(schema)) {return schema.map(s => this.genMatch(s, expr)).join(" || ");}
  
    if (typeof schema.type !== "string") return this.genMatch(schema.type, expr);
  
    switch (schema.type) {
      case "fixed": return `${expr} instanceof Uint8Array && ${expr}.length === ${(schema as A).size}`;
      case "enum": return `${JSON.stringify((schema as A).symbols)}.includes(${expr})`;
      case "array": return `Array.isArray(${expr})`;
      case "map": case "record": return `${expr} !== null && typeof ${expr} === "object" && !Array.isArray(${expr})`;
      default: return this.genMatch(schema.type, expr);
    }
  }
  // ---- codegen: produces an array of JS STATEMENT strings that write `expr` into `w` ----
  private genWrite(schema: AVROSchema, expr: S): AR<S> {
    schema = this.resolve(schema);

    if (Array.isArray(schema)) {
      if (schema.length === 2) {
        const nullIdx = schema.findIndex((s) => this.kindOf(s) === "null");
        if (nullIdx !== -1) {
          const otherIdx = nullIdx === 0 ? 1 : 0;
          // common case: an optional field — special-cased to skip the generic
          // branch-scan entirely, since this covers most unions you'll ever write.
          return [`if (${expr} === null || ${expr} === undefined) { w.long(${nullIdx}); } else { w.long(${otherIdx}); ${this.genWrite(schema[otherIdx], expr).join(" ")} }`];
        }
      }
      const branches = schema.map((s, i) => `if (${this.genMatch(s, expr)}) { w.long(${i}); ${this.genWrite(s, expr).join(" ")} }`);
      return [branches.join(" else ") + " else { throw new Error('Avro: no matching union branch'); }"];
    }

    if (typeof schema === "string") {
      switch (schema) {
        case "null": return [];
        case "boolean": return [`w.byte(${expr} ? 1 : 0);`];
        case "int": case "long": return [`w.long(${expr});`];
        case "float": return [`w.float(${expr});`];
        case "double": return [`w.double(${expr});`];
        case "string": return [`w.string(${expr});`];
        case "bytes": return [`w.long(${expr}.length); w.bytes(${expr});`];
        default: throw new Error(`Avro: unknown type ${schema}`);
      }
    }
    if (typeof schema.type !== "string") return this.genWrite(schema.type, expr);

    switch (schema.type) {
      case "null": return [];
      case "boolean": return [`w.byte(${expr} ? 1 : 0);`];
      case "int": case "long": return [`w.long(${expr});`];
      case "float": return [`w.float(${expr});`];
      case "double": return [`w.double(${expr});`];
      case "string": return [`w.string(${expr});`];
      case "bytes": return [`w.long(${expr}.length); w.bytes(${expr});`];
      case "fixed": {
        const size = (schema as A).size;
        return [`if (!(${expr} instanceof Uint8Array) || ${expr}.length !== ${size}) throw new Error("Avro: invalid fixed value"); w.bytes(${expr});`];
      }
      case "enum": {
        const symbols = JSON.stringify((schema as A).symbols);
        return [`{ const i = ${symbols}.indexOf(${expr}); if (i < 0) throw new Error("Avro: invalid enum symbol " + ${expr}); w.long(i); }`];
      }
      case "record": {
        const lines: S[] = [];
        for (const f of schema.fields as A[]) lines.push(...this.genWrite(f.type, `${expr}[${JSON.stringify(f.name)}]`));
        return lines;
      }
      case "array": {
        const inner = this.genWrite((schema as A).items, "arr[i]").join(" ");
        return [`{ const arr = ${expr}; if (arr.length) { w.long(arr.length); for (let i = 0; i < arr.length; i++) { ${inner} } } w.long(0); }`];
      }
      case "map": {
        const inner = this.genWrite((schema as A).values, "obj[k]").join(" ");
        return [`{ const obj = ${expr}; const keys = Object.keys(obj); if (keys.length) { w.long(keys.length); for (let i = 0; i < keys.length; i++) { const k = keys[i]; w.string(k); ${inner} } } w.long(0); }`];
      }
      default: throw new Error(`Avro: unsupported schema type ${schema.type}`);
    }
  }
  // ---- codegen: produces a single JS EXPRESSION string that reads one value from `r` ----
  private genRead(schema: AVROSchema): S {
    schema = this.resolve(schema);

    if (Array.isArray(schema)) {
      const cases = schema.map((s, i) => `case ${i}: return ${this.genRead(s)};`).join(" ");
      return `(function(){ const i = r.long(); switch(i){ ${cases} default: throw new Error("Avro: invalid union index"); } })()`;
    }
    if (typeof schema === "string") {
      switch (schema) {
        case "null": return "null";
        case "boolean": return "(r.byte() === 1)";
        case "int": case "long": return "r.long()";
        case "float": return "r.float()";
        case "double": return "r.double()";
        case "string": return "r.string()";
        case "bytes": return "r.bytes(r.long())";
        default: throw new Error(`Avro: unknown type ${schema}`);
      }
    }
    if (typeof schema.type !== "string") return this.genRead(schema.type);

    switch (schema.type) {
      case "null": return "null";
      case "boolean": return "(r.byte() === 1)";
      case "int": case "long": return "r.long()";
      case "float": return "r.float()";
      case "double": return "r.double()";
      case "string": return "r.string()";
      case "bytes": return "r.bytes(r.long())";
      case "fixed": return `r.bytes(${(schema as A).size})`;
      case "enum": return `(${JSON.stringify((schema as A).symbols)})[r.long()]`;
      case "record": {
        const assigns = (schema.fields as A[]).map((f) => `o[${JSON.stringify(f.name)}] = ${this.genRead(f.type)};`).join(" ");
        return `(function(){ const o = {}; ${assigns} return o; })()`;
      }
      case "array": return `(function(){ const out = []; let c; while ((c = r.long()) !== 0) { for (let i = 0; i < c; i++) out.push(${this.genRead((schema as A).items)}); } return out; })()`;
      case "map": return `(function(){ const out = {}; let c; while ((c = r.long()) !== 0) { for (let i = 0; i < c; i++) { const k = r.string(); out[k] = ${this.genRead((schema as A).values)}; } } return out; })()`;
      default: throw new Error(`Avro: unsupported schema type ${schema.type}`);
    }
  }
  // ---- public API: identical contract to BINARY_AVRO_FAST / the original BINARY_AVRO ----
  public encode_msg(msg: AVROValue): Buffer {
    if (msg instanceof Uint8Array) return msg as A;
    try {
      this.writer.reset();
      this.writer.long(0);
      this.writeRoot(this.writer, msg);
      return this.writer.finish();
    } catch (err) {
      console.log("[AVRO_PIPE] Failed to encode outbound packet:", err);
      this.writer.reset();
      this.writer.long(1);
      this.writeError(this.writer, {
        type: "error",
        error: err instanceof Error ? err.message : "Encoding failed",
        id: String((msg as A)?.id ?? "unknown"),
      });
      return this.writer.finish();
    }
  }

  public decode_msg(raw: A): AVROValue {
    /**if (!(raw instanceof ArrayBuffer)) return
     if (!(raw instanceof Uint8Array)) {
       console.error("[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
       return null;
     } */
    try {
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength) ;
      const r = new FastAvroReader(buf);
      const branch = r.long();
      if (branch !== 0 && branch !== 1) throw new Error("Avro: invalid envelope branch");
      const value = branch === 0 ? this.readRoot(r) : this.readError(r);
      if (!r.done()) throw new Error("Avro: trailing bytes");
      return value;
    } catch (err) {
      console.error("[AVRO_PIPE] Failed to decode inbound packet:", err);
      return null;
    }
  }
}
export const SAFE_ZIGZAG_LIMIT = Number.MAX_SAFE_INTEGER / 2;
export const SIG_BUS_SCHEMA: AVROSchema = {
  type: "record",
  name: "SIGBusMessage",
  fields: [
    { name: "v", type: "int" },          // protocol/schema version
    { name: "kind", type: "int" },       // 0 request, 1 response, 2 event, 3 control
    { name: "id", type: "string" },
    { name: "r", type: "string" },
    { name: "m", type: "string" },
    { name: "s", type: "string" },
    { name: "k", type: "string" },
    { name: "p", type: "bytes" },        // payload, optionally Avro/MessagePack encoded
    { name: "ts", type: "long" },
    { name: "ack", type: "boolean" },
    { name: "replyTo", type: ["null", "string"] },
    { name: "stream", type: ["null", "boolean"] },
    { name: "end", type: ["null", "boolean"] },
    { name: "ttl", type: ["null", "long"] },
  ]
};
export const WAL_KVDB_SCHEMA: AVROSchema = {
  type: "record",
  name: "SIGBusMessage",
  fields: [
    { name: "op", type: "int" },
    { name: "id", type: "string" },
    { name: "r", type: "string" },
    { name: "m", type: "string" },
    { name: "s", type: "string" },
    { name: "k", type: "string" },
    { name: "p", type: "bytes" },       
    { name: "replyTo", type:"string" },
    { name: "error", type: "string" }
  ]
};
export const KV_PAYLOAD_SCHEMA: AVROSchema = {
  type: "record",
  name: "KVPayload",
  fields: [
    { name: "key", type: "string" },
    { name: "value", type: "bytes" },
    { name: "ok", type: "boolean" }
  ]
};
export const ERROR_INFO_SCHEMA: AVROSchema = {
  type: "record",
  name: "ErrorInfo",
  fields: [
    { name: "message", type: "string" },
    { name: "code", type: "int" }
  ]
};
export const AVRO_ERROR_SCHEMA: AVROSchema = {
  type: "record",
  name: "AVROError",
  fields: [
    { name: "type", type: "string" },
    { name: "error", type: "string" },
    { name: "id", type: "string" },
  ],
};
const codecRegistry = new Map<S, BINARY_AVRO_CODEGEN>();

function getCodec(schemaName: S, schema: AVROSchema): BINARY_AVRO_CODEGEN {
  let codec = codecRegistry.get(schemaName);
  if (!codec) {
    codec = new BINARY_AVRO_CODEGEN(schema); // only runs once per distinct schema, ever
    codecRegistry.set(schemaName, codec);
  }
  return codec;
}
export const ENVELOPE_CODEC = getCodec('SIGBusMessage', WAL_KVDB_SCHEMA)
export const PAYLOAD_CODEC = getCodec('KVPayload', KV_PAYLOAD_SCHEMA)
// #endregion


