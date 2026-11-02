// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
//
// Design pillars:
//   1. Structured clone in/out — no JSON round-trip. Map/Set/Date/Blob native.
//   2. Transaction batching — all ops in a tick share one transaction.
//   3. SWR memory cache + in-flight dedup + write coalescing.
//   4. Cross-tab sync via BroadcastChannel + HLC (no LWW-by-luck).
//   5. Pub/sub that returns Teardown and bridges to onFlush.
//   6. Explicit versioning — defineMigration(v, upgrade) once.
//   7. Cursor-as-async-iterator — `for await (const [k,v] of db.entries())`.
//   8. Zero deps. Browser + Bun + Node (via fake-indexeddb).
// ─────────────────────────────────────────────────────────────────────────────

import { HYBRID_LOGICAL_CLOCK } from '../../../../the-cloud/resolvers/hybrid_logical_clock';
import { MCR_Combo } from '../../saurons_eye'
import { node_module_rifts } from '../../../../the-cloud/solutions/node_module_rifts';

let Bun: A = null
if (typeof (globalThis as A).Bun !== 'undefined') { Bun = (globalThis as A).Bun; }
let Deno: A = null
if (typeof (globalThis as A).Deno !== 'undefined') { Deno = (globalThis as A).Deno; }
const isBrowser = typeof window !== 'undefined';

export type Teardown = | V | NU | (() => V) | { unsubscribe(): V } | { [Symbol.dispose](): V } | Teardown[]

export function runTeardown(t: Teardown): V {
  if (t == null) return
  if (typeof t === 'function') return void t()
  if (Array.isArray(t)) { for (const x of t) runTeardown(x); return }
  const a = t as A
  if (typeof a.unsubscribe === 'function') return void a.unsubscribe()
  if (typeof a[Symbol.dispose] === 'function') return void a[Symbol.dispose]()
}

export function reportError(e: U): V {
  const g = globalThis as A
  if (typeof g.reportError === 'function') return void g.reportError(e)
  queueMicrotask(() => { throw e })
} 

export class BINARY {
    public storage_path: A
    private indexMap = new Map<S, A>();
  private memoryCache = new Map<S, { value: A; expiry: N }>();
  private defaultMemoryTtlMs: N;
  Bun: A = null;
  Deno: A = null;
  hasBun
  hasDeno 
  hasNode 
  isBrowser = typeof window !== "undefined";
  isVSCode
  isElectron 
  isUnix 
  isWindows 

    constructor(storage_path = './') { 
        this.storage_path = storage_path;
        this.defaultMemoryTtlMs = 30
        if (typeof (globalThis as A).Deno !== "undefined") {
            this.Deno = (globalThis as A).Deno;
        }
        if (typeof (globalThis as A).Bun !== "undefined") {
            this.Bun = (globalThis as A).Bun;
        }
        this.hasBun = typeof (globalThis as A).Bun !== "undefined";
        this.hasDeno = typeof (globalThis as A).Deno !== "undefined";
        this.hasNode = typeof process !== "undefined" && !!process.versions?.node;
        this.isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
        this.isElectron = false;
        this.isUnix = false;
        this.isWindows = true;
     }
  //                                    FILE WRITES
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

  public init_kv_ngin(namespaceDir: S, memoryTtlMs = 15 * 60 * 1000) {
    const path = node_module_rifts.import('path')
    this.storage_path = path.resolve(namespaceDir);
    this.defaultMemoryTtlMs = memoryTtlMs;
    this.kv_ngin.initializeStorage();
  }
  public kv_ngin = {
    initializeStorage: (): V => {
      const fs = node_module_rifts.import('fs')
      if (!fs.existsSync(this.storage_path)) {
        fs.mkdirSync(this.storage_path, { recursive: true });
      }
      this.kv_ngin.rebuildIndexFromDisk();
    },
    rebuildIndexFromDisk: (): V => {
      const fs = node_module_rifts.import('fs')
      const path = node_module_rifts.import('path')
      const entries = fs.readdirSync(this.storage_path, { withFileTypes: true });
      const now = Date.now();

      for (const entry of entries) {
        if (entry.isFile() && entry.name.endsWith('.bin')) {
          const key = path.basename(entry.name, '.bin');
          const fullPath = path.join(this.storage_path, entry.name);
          const stats = fs.statSync(fullPath);

          this.indexMap.set(key, {
            updatedAt: stats.mtimeMs,
            isBinaryFile: true,
            filePath: fullPath
          });
        }
      }
    },
    set: async <T>(key: S, value: T, ttlMs?: N): P<V> => {
      if (this.hasBun) {
        const fs = node_module_rifts.import('fs')
        const path = node_module_rifts.import('path')
        const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, '_');
        const filePath = path.join(this.storage_path, `${safeKey}.bin`);
        const tempPath = `${filePath}.tmp`;
        let p: U8;
        if (value instanceof Uint8Array) p = value;
        else if (value instanceof ArrayBuffer) p = new Uint8Array(value);
        else p = this.encode_msg(value);// Atomic write via temp file rename to prevent corruption
        await Bun.write(tempPath, p);
        if (fs.existsSync(filePath)) { fs.unlinkSync(filePath); }
        Bun.renameSync(tempPath, filePath)
        const stats = fs.statSync(filePath);
        this.indexMap.set(key, { updatedAt: stats.mtimeMs, isBinaryFile: true, filePath });// Set memory cache hot layer with TTL
        const expiry = (now: any) => now + (ttlMs || this.defaultMemoryTtlMs);
        this.memoryCache.set(key, { value, expiry: expiry(Date.now()) });
      } else if (this.hasDeno) {
        const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, '_');
        const filePath = `${this.storage_path}/${safeKey}.bin`;
        const tempPath = `${filePath}.tmp`;
        const dir = filePath.substring(0, filePath.lastIndexOf('/'));
        let p: U8;
        if (value instanceof Uint8Array) p = value;
        else if (value instanceof ArrayBuffer) p = new Uint8Array(value);
        else p = this.encode_msg(value);// Atomic write via temp file rename to prevent corruption
        await Deno.mkdir(dir, { recursive: true });
        await Deno.writeFile(tempPath, p);
        try {  await Deno.remove(filePath);   } 
        catch (error) { if (!(error instanceof Deno.errors.NotFound)) { throw error; } }
        Deno.renameSync(tempPath, filePath);
        const stats = Deno.statSync(filePath);
        this.indexMap.set(key, { updatedAt: stats.mtime?.getTime() ?? Date.now(), isBinaryFile: true, filePath });// Set memory cache hot layer with TTL
        const expiry = Date.now() + (ttlMs || this.defaultMemoryTtlMs);
        this.memoryCache.set(key, { value, expiry });
      } else {
        const fs = node_module_rifts.import('fs')
        const path = node_module_rifts.import('path')
        const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, '_');
        const filePath = path.join(this.storage_path, `${safeKey}.bin`);
        const tempPath = `${filePath}.tmp`;
        let p: U8;
        if (value instanceof Uint8Array) p = value;
        else if (value instanceof ArrayBuffer) p = new Uint8Array(value);
        else p = this.encode_msg(value);// Atomic write via temp file rename to prevent corruption         
        fs.writeFileSync(tempPath, p);
        if (fs.existsSync(filePath)) { fs.unlinkSync(filePath); }
        fs.renameSync(tempPath, filePath);
        const stats = fs.statSync(filePath);
        this.indexMap.set(key, { updatedAt: stats.mtimeMs, isBinaryFile: true, filePath });// Set memory cache hot layer with TTL
        const expiry = (now: A) => now + (ttlMs || this.defaultMemoryTtlMs);
        this.memoryCache.set(key, { value, expiry: expiry(Date.now()) });
      }
    },
    delete: async (key: S): P<B> => {
      if (this.hasBun) {
        const indexEntry = this.indexMap.get(key);
        if (indexEntry?.filePath) { const file = Bun.file(indexEntry.filePath); if (await file.exists()) { await file.delete(); } }
        this.indexMap.delete(key);
        this.memoryCache.delete(key);
        return true;
      } else if (this.hasDeno) {
        const indexEntry = this.indexMap.get(key);
        if (indexEntry?.filePath) { try { await Deno.remove(indexEntry.filePath); } catch { } }
        this.indexMap.delete(key);
        this.memoryCache.delete(key);
        return true;
      } else {
        const fs = node_module_rifts.import('fs')
        const indexEntry = this.indexMap.get(key);
        if (indexEntry?.filePath && fs.existsSync(indexEntry.filePath)) { fs.unlinkSync(indexEntry.filePath); }
        this.indexMap.delete(key);
        this.memoryCache.delete(key);
        return true;
      }
    },
    get: async<T>(key: S): P<T | NU> => {
      if (this.hasBun) {
        const fs = node_module_rifts.import('fs');
        const now = Date.now();
        const cached = this.memoryCache.get(key);
        if (cached) {
          if (cached.expiry > now) { return cached.value as T; }
          this.memoryCache.delete(key);
        }
        const indexEntry = this.indexMap.get(key);
        if (!indexEntry || !indexEntry.filePath) { return null; }

        // Check if file exists using Bun's file API
        const file = Bun.file(indexEntry.filePath);
        const exists = await file.exists();
        if (!exists) { return null; }

        const rawBuffer = await file.arrayBuffer();
        if (rawBuffer.byteLength === 0) return null;

        try {
          const decoded = this.decode_msg(new Uint8Array(rawBuffer));
          this.memoryCache.set(key, { value: decoded, expiry: now + this.defaultMemoryTtlMs });
          return decoded as T;
        } catch {
          return new Uint8Array(rawBuffer) as U as T;
        }
      } else if (this.hasDeno) {
        const now = Date.now();
        const cached = this.memoryCache.get(key);
        if (cached) {
          if (cached.expiry > now) { return cached.value as T; }
          this.memoryCache.delete(key);
        }
        const indexEntry = this.indexMap.get(key);
        if (!indexEntry || !indexEntry.filePath) { return null; }

        try {
          const fileInfo = await Deno.stat(indexEntry.filePath);
          if (!fileInfo.isFile) { return null; }
        } catch {
          return null;
        }

        const rawBuffer = await Deno.readFile(indexEntry.filePath);
        if (rawBuffer.length === 0) return null;

        try {
          const decoded = this.decode_msg(rawBuffer);
          this.memoryCache.set(key, { value: decoded, expiry: now + this.defaultMemoryTtlMs });
          return decoded as T;
        } catch {
          return rawBuffer as U as T;
        }
      } else {
        const fs = node_module_rifts.import('fs')
        const now = Date.now();
        const cached = this.memoryCache.get(key);
        if (cached) {
          if (cached.expiry > now) { return cached.value as T; }
          this.memoryCache.delete(key);
        }
        const indexEntry = this.indexMap.get(key);
        if (!indexEntry || !indexEntry.filePath || !fs.existsSync(indexEntry.filePath)) { return null; }
        const rawBuffer = fs.readFileSync(indexEntry.filePath);// If it's a raw binary array/buffer storage mode, return the Uint8Array directly
        if (rawBuffer.length === 0) return null;
        try {
          const decoded = this.decode_msg(rawBuffer);
          this.memoryCache.set(key, { value: decoded, expiry: now + this.defaultMemoryTtlMs });
          return decoded as T;
        } catch {// Fallback for raw byte buffers if CBOR decode fails
          return rawBuffer as U as T;
        }
      }
    },
    getRunningSize: (): N => { return this.indexMap.size; },
  }
  // #endregion
  //                                    ENCODERS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
    private readonly encoder = new TextEncoder()
    private readonly decoder = new TextDecoder()
    private ensureAvailable(buf: Uint8Array, pos: number, length: number): void { if (pos < 0 || length < 0 || pos + length > buf.length) throw new Error("CBOR: truncated buffer") }
    private readUint(buf: Uint8Array, pos: number): [number, number] {
      this.ensureAvailable(buf, pos, 1)
      const info = buf[pos] & 0x1f
      if (info <= 23) return [info, pos + 1]
      if (info === 24) { this.ensureAvailable(buf, pos + 1, 1); return [buf[pos + 1], pos + 2] }
      if (info === 25) { this.ensureAvailable(buf, pos + 1, 2); return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(0, false), pos + 3]; }
      if (info === 26) { this.ensureAvailable(buf, pos + 1, 4); return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(0, false), pos + 5]; }
      if (info === 27) {
        this.ensureAvailable(buf, pos + 1, 8)
        const value = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8).getBigUint64(0, false)
        if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER")
        return [Number(value), pos + 9]
      }; throw new Error(`CBOR: unsupported additional info ${info}`)
    }
    private encodeUint64(majorShifted: number, n: bigint): Uint8Array {
      if (n <= 23n) return new Uint8Array([majorShifted | Number(n)])
      if (n <= 0xffn) { return new Uint8Array([majorShifted | 24, Number(n)]) }
      if (n <= 0xffffn) {
        const b = new Uint8Array(3)
        b[0] = majorShifted | 25
        new DataView(b.buffer).setUint16(1, Number(n), false)
        return b
      }
      if (n <= 0xffffffffn) {
        const b = new Uint8Array(5)
        b[0] = majorShifted | 26
        new DataView(b.buffer).setUint32(1, Number(n), false)
        return b
      }
      const b = new Uint8Array(9)
      b[0] = majorShifted | 27
      new DataView(b.buffer).setBigUint64(1, n, false)
      return b
    }
    private encode(value: CBORValue): Uint8Array {
      const parts: Uint8Array[] = []
      this.encodeValue(value, parts)
      const total = parts.reduce((n, p) => n + p.length, 0)
      const out = new Uint8Array(total)
      let offset = 0
      for (const p of parts) { out.set(p, offset); offset += p.length }
      return out
    }
    private encodeValue(v: CBORValue, out: Uint8Array[]): void {
      if (v === null) { out.push(new Uint8Array([0xf6])); return; }
      if (v === undefined) { out.push(new Uint8Array([0xf7])); return; }
      if (v === true) { out.push(new Uint8Array([0xf5])); return }
      if (v === false) { out.push(new Uint8Array([0xf4])); return }
  
      if (typeof v === 'number') {
        if (Number.isSafeInteger(v)) {
          if (v >= 0) out.push(this.encodeUint64(0x00, BigInt(v)))
          else out.push(this.encodeUint64(0x20, BigInt(-1 - v)))
        } else {
          const b = new Uint8Array(9)
          b[0] = 0xfb
          new DataView(b.buffer).setFloat64(1, v, false)
          out.push(b)
        }
        return
      }
  
      if (typeof v === 'string') { const encoded = this.encoder.encode(v); out.push(this.encodeUint(0x60, encoded.length)); out.push(encoded); return; }
      if (v instanceof Uint8Array) { out.push(this.encodeUint(0x40, v.length)); out.push(v); return; }
      if (Array.isArray(v)) {
        out.push(this.encodeUint(0x80, v.length));
        for (const item of v) { this.encodeValue(item, out); }
        return;
      }
      if (typeof v === 'object') {
        const proto = Object.getPrototypeOf(v)
        if (proto !== Object.prototype && proto !== null) throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`)
        const keys = Object.keys(v)
        out.push(this.encodeUint(0xa0, keys.length))
        for (const key of keys) { this.encodeValue(key, out); this.encodeValue(v[key], out); }
        return
      }; throw new Error(`CBOR: unsupported value type ${typeof v}`)
    }
    private encodeUint(majorShifted: number, n: number): Uint8Array {
      if (n <= 23) return new Uint8Array([majorShifted | n]);
      if (n <= 0xff) return new Uint8Array([majorShifted | 24, n]);
      if (n <= 0xffff) { const b = new Uint8Array(3); b[0] = majorShifted | 25; new DataView(b.buffer).setUint16(1, n, false); return b };
      const b = new Uint8Array(5); b[0] = majorShifted | 26; new DataView(b.buffer).setUint32(1, n, false); return b;
    }
    private decode(buf: Uint8Array): CBORValue {
      const [value, end] = this.decodeValue(buf, 0);
      if (end !== buf.length) { throw new Error("CBOR: trailing bytes") };
      return value;
    }
    private decodeValue(buf: Uint8Array, pos: number): [CBORValue, number] {
      this.ensureAvailable(buf, pos, 1)
      const first = buf[pos]
      const major = first >> 5
      const info = first & 0x1f
  
      if (major === 7) {
        if (info === 20) return [false, pos + 1]
        if (info === 21) return [true, pos + 1]
        if (info === 22) return [null, pos + 1]
        if (info === 23) return [undefined, pos + 1]
        if (info === 27) { this.ensureAvailable(buf, pos + 1, 8); const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8); return [dv.getFloat64(0, false), pos + 9]; }
      }
      const [n, nextPos] = this.readUint(buf, pos)
      if (major === 0) return [n, nextPos]
      if (major === 1) return [-1 - n, nextPos]
      if (major === 2) { this.ensureAvailable(buf, nextPos, n); const bytes = buf.slice(nextPos, nextPos + n); return [bytes, nextPos + n]; }
      if (major === 3) { this.ensureAvailable(buf, nextPos, n); const bytes = buf.slice(nextPos, nextPos + n); return [this.decoder.decode(bytes), nextPos + n]; }
      if (major === 4) {
        const arr: CBORValue[] = []
        let p = nextPos
        for (let i = 0; i < n; i++) { const [item, np] = this.decodeValue(buf, p); arr.push(item); p = np; }
        return [arr, p]
      }
      if (major === 5) {
        const obj: Record<string, CBORValue> = {}
        let p = nextPos
        for (let i = 0; i < n; i++) { const [k, kp] = this.decodeValue(buf, p); const [v, vp] = this.decodeValue(buf, kp); obj[k as string] = v; p = vp; }
        return [obj, p]
      }; throw new Error(`CBOR: unsupported major type ${major}`)
    }
    public encode_msg(msg: any): Uint8Array {// Instead of leaking a raw object, encode an explicit fallback/error frame
      if (msg instanceof Uint8Array) return msg;// If it's already an encoded Uint8Array, don't re-encode it
      try { return this.encode(msg); }
      catch (err) { console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err); return this.encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" }); }
    }
    public decode_msg(raw: any): any {// Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
      if (!(raw instanceof Uint8Array) && !(typeof Buffer !== 'undefined' && Buffer.isBuffer(raw))) { console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream."); return null; }
      try {
        const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array((raw as any).buffer, (raw as any).byteOffset, (raw as any).length); return this.decode(uint8);
      } catch (err) {
        console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
        return null;
      }
    }
  // #endregion
  //                                    SERIALIZER
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
  HEADER_SIZE_BYTES = 64;// Int32 Array Indices (4 bytes per element)
   HEADER_IDX_WRITE_HEAD = 0; // Byte offset 0
  HEADER_IDX_READ_HEAD = 1; // Byte offset 4
  HEADER_IDX_MUTEX = 2; // Byte offset 8
  HEADER_IDX_CAPACITY = 3; // Byte offset 12
  MUTEX_UNLOCKED = 0;// Mutex States
  MUTEX_LOCKED = 1;
  SENTINEL_WRAP_MARKER = 0xFFFFFFFF;// 0xFFFFFFFF (Uint32 max) written to length prefix slot when p wraps to offset 0
  
  public FLYWEIGHT_FRAME: DecodedFrame = { topicId: 0, topicName: undefined, p: new Uint8Array(0) };
    
  createSharedDepotBuffer(capacityBytes: number): SharedArrayBuffer {
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
  mapDepotBuffer(sab: SharedArrayBuffer): HeaderViews {
      // Map control slots (first 16 bytes containing 4 x Int32 values)
      const headerInt32 = new Int32Array(sab, 0, 4);
  
      // Read capacity directly from header slot
      const capacity = Atomics.load(headerInt32, this.HEADER_IDX_CAPACITY);
  
      // Map p slice strictly starting at byte 64
      const payloadUint8 = new Uint8Array(sab, this.HEADER_SIZE_BYTES, capacity);
  
      return {
          headerInt32,
          payloadUint8,
          capacity
      };
  }
  validateHeader(sab: SharedArrayBuffer): boolean {
      if (sab.byteLength < this.HEADER_SIZE_BYTES) return false;
  
      const headerInt32 = new Int32Array(sab, 0, 4);
      const capacity = Atomics.load(headerInt32, this.HEADER_IDX_CAPACITY);
  
      // Buffer length must equal 64 bytes header + registered capacity
      return sab.byteLength === this.HEADER_SIZE_BYTES + capacity;
  }
  calculateFrameFootprint(payloadSizeBytes: number): number {
      const rawEnvelopeBytes = 4 + payloadSizeBytes;
      return Math.ceil(rawEnvelopeBytes / 4) * 4;
  }
  
  
  
      private   topicToIdMap: Map<string, number> = new Map();
      private   idToTopicMap: Map<number, string> = new Map();
      public   hashTopic(topic: string): number {
          let hash = 0x811c9dc5;
          for (let i = 0; i < topic.length; i++) {
              hash ^= topic.charCodeAt(i);
              hash = Math.imul(hash, 0x01000193) >>> 0;
          }
          return hash;
      }
      public   getTopicId(topic: string): number {
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
      public   getTopicName(topicId: number): string | undefined {
          return this.idToTopicMap.get(topicId);
      }
      public   registerTopics(topics: string[]): void {
          for (const topic of topics) {
              this.getTopicId(topic);
          }
      }
      public   resetRegistry(): void {
          this.topicToIdMap.clear();
          this.idToTopicMap.clear();
      }
  
  
  
  
  
  private   textEncoder = new TextEncoder();
  
      // Reusable scratch buffer & DataView for header manipulations (Zero GC)
      private   headerBuffer = new ArrayBuffer(4);
      private   headerView = new DataView(this.headerBuffer);
      private   headerBytes = new Uint8Array(this.headerBuffer);
  
      public   encodeFrame(        topic: string,        p: Uint8Array | string | Record<string, unknown>    ): Uint8Array {
          const topicId = this.getTopicId(topic);
  
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
      public   decodeFrame(frameBytes: Uint8Array): DecodedFrame {
          if (frameBytes.byteLength < 4) {
              throw new Error("Invalid frame: buffer smaller than minimum header size (4 bytes).");
          }
  
          // Direct byte copy into static DataView to read Uint32 safely without memory allocation
          this.headerBytes[0] = frameBytes[0];
          this.headerBytes[1] = frameBytes[1];
          this.headerBytes[2] = frameBytes[2];
          this.headerBytes[3] = frameBytes[3];
  
          const topicId = this.headerView.getUint32(0, true);
  
          this.FLYWEIGHT_FRAME.topicId = topicId;
          this.FLYWEIGHT_FRAME.topicName = this.getTopicName(topicId);
          this.FLYWEIGHT_FRAME.p = frameBytes.subarray(4); // Zero-copy view
  
          return this.FLYWEIGHT_FRAME;
      }
  // #endregion 
  }

export class INDEXED_DB {
    readonly dbName: S
    readonly storeName: S
    private _version: N
    private _migrations = new Map<N, Migration>()
    private _nodeId: S
    private _hlc: HYBRID_LOGICAL_CLOCK
    private _onFlush: (fn: () => V) => V
    private _onError: (err: U) => void
    // ── connection ──────────────────────────────────────────────────────────
    private _db: IDBDatabase | NU = null
    private _opening: P<IDBDatabase> | NU = null
    // ── memory cache (SWR) ──────────────────────────────────────────────────
    private memory = new Map<S, EntryRecord>()
    private inflight = new Map<S, P<A>>()
    private ttlMs: N
    private memoryTimer?: ReturnType<typeof setTimeout>
    // ── transaction batching ────────────────────────────────────────────────
    private pending: PendingOp[] = []
    private flushScheduled = false
    private _closed = false
    // ── subscribers ─────────────────────────────────────────────────────────
    private subscribers = new Map<S, Set<Subscriber<A>>>()
    // ── cross-tab ───────────────────────────────────────────────────────────
    private _channel?: BroadcastChannel
    private _localChange = false
    constructor(opts: IndexedDbOptions = {}) {
        this.dbName = opts.dbName ?? 'the_wizard'
        this.storeName = opts.storeName ?? 'the_wizards_tardis'
        this._version = opts.version ?? 1
        this._nodeId = opts.nodeId ?? Math.random().toString(36).slice(2, 10)
        this.ttlMs = opts.ttlMs ?? 30 * 60 * 1000
        this._onFlush = opts.onFlush ?? ((fn) => queueMicrotask(fn))
        this._onError = opts.onError ?? reportError
        if (typeof BroadcastChannel !== 'undefined') {
            this._channel = new BroadcastChannel(`sauron_idb_sync:${this.dbName}:${this.storeName}`)
            this._channel.onmessage = (e) => this._onRemote(e.data)
        }
           
        this._hlc = new HYBRID_LOGICAL_CLOCK(this._nodeId)
    }
    // ───────────────────────────────────────────────────────────────────────
    // Migrations — call before first open. Each version gets one upgrader.
    // ───────────────────────────────────────────────────────────────────────
    defineMigration(version: N, fn: Migration): this {
        this._migrations.set(version, fn)
        if (version > this._version) this._version = version
        return this
    }
    // ───────────────────────────────────────────────────────────────────────
    // Open — idempotent, single in-flight promise, runs all pending migrations
    // ───────────────────────────────────────────────────────────────────────
    private open(): P<IDBDatabase> {
        if (this._db) return Promise.resolve(this._db)
        if (this._opening) return this._opening
        if (typeof indexedDB === 'undefined') {
            return Promise.reject(new Error('[INDEXED_DB] indexedDB is not defined in this environment'))
        }

        this._opening = new Promise<IDBDatabase>((resolve, reject) => {
            const req = indexedDB.open(this.dbName, this._version)

            req.onupgradeneeded = (ev) => {
                const db = req.result
                const oldVersion = (ev as IDBVersionChangeEvent).oldVersion

                if (!db.objectStoreNames.contains(this.storeName)) {
                    db.createObjectStore(this.storeName)
                }

                // run migrations from oldVersion+1 .. this._version
                for (let v = oldVersion + 1; v <= this._version; v++) {
                    const fn = this._migrations.get(v)
                    if (fn) {
                        try { fn(db, oldVersion) }
                        catch (err) { this._onError(err); reject(err); return }
                    }
                }
            }

            req.onsuccess = () => {
                this._db = req.result
                this._opening = null
                this._db.onclose = () => { this._db = null }
                this._db.onversionchange = () => { this._db?.close(); this._db = null }
                resolve(this._db)
            }

            req.onerror = () => { this._opening = null; reject(req.error) }
            req.onblocked = () => { /* another tab holds an older version; wait */ }
        })

        return this._opening
    }
    // ───────────────────────────────────────────────────────────────────────
    // Transaction batching
    //
    // All ops queued within a microtask get executed in a single IDB
    // transaction. This is the biggest perf win vs. the old per-op tx().
    //
    // We serialize ALL ops through one readwrite transaction. IDB will
    // upgrade readonly ops to readwrite anyway when mixed, and the cost of
    // a readwrite tx over readonly is tiny compared to the setup cost.
    // ───────────────────────────────────────────────────────────────────────
    private scheduleFlush(): V {
        if (this.flushScheduled) return
        this.flushScheduled = true
        this._onFlush(() => this.flush())
    }
    private async flush(): P<V> {
        this.flushScheduled = false
        if (this.pending.length === 0 || this._closed) return
        const ops = this.pending
        this.pending = []

        let db: IDBDatabase
        try { db = await this.open() }
        catch (err) {
            for (const op of ops) ('reject' in op && op.reject(err))
            return
        }

        let tx: IDBTransaction
        try { tx = db.transaction(this.storeName, 'readwrite') }
        catch (err) {
            for (const op of ops) ('reject' in op && op.reject(err))
            return
        }

        const store = tx.objectStore(this.storeName)

        // Wire up all requests inside one tx. IDB keeps the tx alive as long
        // as there's at least one pending request, so scheduling them all
        // before returning is safe.
        for (const op of ops) {
            try {
                switch (op.kind) {
                    case 'get': {
                        const r = store.get(op.key)
                        r.onsuccess = () => {
                            const rec = r.result as EntryRecord | undefined
                            if (rec) { this.memory.set(op.key, rec); op.resolve(rec.value) }
                            else op.resolve(undefined)
                        }
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                    case 'put': {
                        const rec: EntryRecord = {
                            value: op.value,
                            hlc: this._hlc.tick(),
                            nodeId: this._nodeId,
                            ts: Date.now(),
                        }
                        const r = store.put(rec, op.key)
                        r.onsuccess = () => {
                            this.memory.set(op.key, rec)
                            this._broadcastPut(op.key, rec)
                            this._emit(op.key, rec.value)
                            op.resolve()
                        }
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                    case 'del': {
                        const r = store.delete(op.key)
                        r.onsuccess = () => {
                            this.memory.delete(op.key)
                            this._broadcastDel(op.key)
                            this._emit(op.key, undefined)
                            op.resolve()
                        }
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                    case 'has': {
                        const r = store.getKey(op.key)
                        r.onsuccess = () => op.resolve(r.result !== undefined)
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                    case 'keys': {
                        const r = store.getAllKeys()
                        r.onsuccess = () => op.resolve(r.result as string[])
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                    case 'clear': {
                        const r = store.clear()
                        r.onsuccess = () => {
                            this.memory.clear()
                            this._broadcastClear()
                            op.resolve()
                        }
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                    case 'cursor': {
                        const out: [string, A][] = []
                        const r = store.openCursor(op.range)
                        r.onsuccess = () => {
                            const c = r.result
                            if (c) {
                                const rec = c.value as EntryRecord
                                out.push([c.key as string, rec.value])
                                c.continue()
                            } else {
                                op.resolve(out)
                            }
                        }
                        r.onerror = () => op.reject(r.error)
                        break
                    }
                }
            } catch (err) {
                'reject' in op && op.reject(err)
            }
        }
    }
    // ───────────────────────────────────────────────────────────────────────
    // Core CRUD — all batched through the pending queue
    // ───────────────────────────────────────────────────────────────────────
    public get<T = A>(key: A, chain?: CHAIN) {
        if (!key) return undefined
        const cached = this.memory.get(key)
        if (cached) return MCR_Combo(cached.value as T, chain)
        return MCR_Combo(this._enqueueGet<T>(key), chain)
    }
    public getOrThrow<T = A>(key: A, chain?: CHAIN) {
        const v = this.get<T>(key)
        if (v === undefined) throw new Error(`[INDEXED_DB] key not found: ${key}`)
        return MCR_Combo(v, chain)
    }
    public has(key: A, chain?: CHAIN) {
        if (!key) return false
        if (this.memory.has(key)) return MCR_Combo(true, chain)
        return MCR_Combo(this._enqueueHas(key), chain)
    }
    public set<T = A>(key: A, value: T, chain?: CHAIN) {
        if (!key) return
        // write-through memory so subsequent gets in the same tick see it
        const rec: EntryRecord<T> = {
            value,
            hlc: this._hlc.tick(),
            nodeId: this._nodeId,
            ts: Date.now(),
        }
        this.memory.set(key, rec)
        return MCR_Combo(this._enqueuePut(key, value), chain)
    }
    public async setMany(entries: Record<S, A>): P<V> {
        await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)))
    }
    public async update<T = A>(key: A, updater: (prev: T | UD) => T, chain?: CHAIN) {
        const prev = await this.get<T>(key)
        const next = updater(prev)
        await this.set(key, next)
        return MCR_Combo(next, chain)
    }
    public delete(key: A, chain?: CHAIN) {
        if (!key) return
        this.memory.delete(key)
        return MCR_Combo(this._enqueueDel(key), chain)
    }
    public async deleteMany(keys: A[]): P<V> {
        await Promise.all(keys.map((k) => this.delete(k)))
    }
    public async clear(chain?: CHAIN) {
        this.memory.clear()
        return MCR_Combo(this._enqueueClear(), chain)
    }
    public async keys(chain?: CHAIN) {
        return MCR_Combo(this._enqueueKeys(), chain)
    }
    // ───────────────────────────────────────────────────────────────────────
    // In-flight dedup: same key requested twice in a tick → one IDB get
    // ───────────────────────────────────────────────────────────────────────
    private _enqueueGet<T>(key: A) {
        const existing = this.inflight.get(key)
        if (existing) return existing as P<T | UD>

        const p = new Promise<T | UD>((resolve, reject) => {
            this.pending.push({ kind: 'get', key, resolve: resolve as A, reject })
            this.scheduleFlush()
        }).finally(() => { this.inflight.delete(key) })

        this.inflight.set(key, p)
        return p
    }
    private _enqueuePut(key: A, value: A): P<V> {
        return new Promise<void>((resolve, reject) => {
            this.pending.push({ kind: 'put', key, value, resolve, reject })
            this.scheduleFlush()
        })
    }
    private _enqueueDel(key: A):  P<V> {
        return new Promise<void>((resolve, reject) => {
            this.pending.push({ kind: 'del', key, resolve, reject })
            this.scheduleFlush()
        })
    }
    private _enqueueHas(key: A): P<B> {
        return new Promise<boolean>((resolve, reject) => {
            this.pending.push({ kind: 'has', key, resolve, reject })
            this.scheduleFlush()
        })
    }
    private _enqueueKeys(): P<S[]> {
        return new Promise<string[]>((resolve, reject) => {
            this.pending.push({ kind: 'keys', resolve, reject })
            this.scheduleFlush()
        })
    }
    private _enqueueClear(): P<V> {
        return new Promise<V>((resolve, reject) => {
            this.pending.push({ kind: 'clear', resolve, reject })
            this.scheduleFlush()
        })
    }
    private _enqueueCursor(range?: IDBKeyRange): P<[S, A][]> {
        return new Promise<[S, A][]>((resolve, reject) => {
            this.pending.push({ kind: 'cursor', range, resolve, reject })
            this.scheduleFlush()
        })
    }
    // ───────────────────────────────────────────────────────────────────────
    // Pub/sub
    // ───────────────────────────────────────────────────────────────────────
    subscribe<T = A>(key: A, cb: Subscriber<T>, chain?: CHAIN): Teardown {
        let set = this.subscribers.get(key)
        if (!set) { set = new Set(); this.subscribers.set(key, set) }
        set.add(cb)

        const cached = this.memory.get(key)
        if (cached) { try { cb(cached.value as T) } catch (e) { this._onError(e) } }

        return () => {
            const s = this.subscribers.get(key)
            if (!s) return
            s.delete(cb)
            if (s.size === 0) this.subscribers.delete(key)
        }
    }
    /** Selector-based subscription — only fires when the selected slice changes. */
    select<T = A, S = A>(key: A, selector: Selector<T | UD, S>, cb: Subscriber<S>, chain?: CHAIN): Teardown {
        let last: S | undefined
        let first = true
        return this.subscribe<T>(key, (v) => {
            const next = selector(v)
            if (first || !Object.is(last, next)) {
                first = false
                last = next
                cb(next)
            }
        })
    }
    private _emit(key: S, value: A): V {
        const set = this.subscribers.get(key)
        if (!set) return
        for (const cb of set) {
            try { cb(value) }
            catch (e) { this._onError(e) }
        }
    }
    // ───────────────────────────────────────────────────────────────────────
    // waitFor — with waiter cleanup (no leak)
    // ───────────────────────────────────────────────────────────────────────
    waitFor<T = A>(key: S, opts: { timeoutMs?: N } = {}): P<T> {
        const existing = this.memory.get(key)
        if (existing) return Promise.resolve(existing.value as T)

        return new Promise<T>((resolve, reject) => {
            let timer: RET<typeof setTimeout> | UD
            const unsub = this.subscribe<T>(key, (v) => {
                if (v === undefined) return
                runTeardown(unsub)
                if (timer) clearTimeout(timer)
                resolve(v)
            })
            if (opts.timeoutMs && opts.timeoutMs > 0) {
                timer = setTimeout(() => {
                    runTeardown(unsub)
                    reject(new Error(`[INDEXED_DB] waitFor(${key}) timed out after ${opts.timeoutMs}ms`))
                }, opts.timeoutMs)
            }
        })
    }
    // ───────────────────────────────────────────────────────────────────────
    // Cursor as async iterator — `for await (const [k, v] of db.entries())`
    // ───────────────────────────────────────────────────────────────────────
    entries(range?: IDBKeyRange): AsyncIterableIterator<[string, A]> {
        let buf: [S, A][] | NU = null
        let idx = 0
        const self = this
        return {
            [Symbol.asyncIterator]() { return this },
            async next() {
                if (!buf) { buf = await self._enqueueCursor(range) }
                if (idx >= buf.length) return { done: true, value: undefined as A }
                return { done: false, value: buf[idx++] }
            },
        }
    }
    // ───────────────────────────────────────────────────────────────────────
    // Cross-tab sync via BroadcastChannel + HLC
    // ───────────────────────────────────────────────────────────────────────
    private _broadcastPut(key: S, rec: EntryRecord): V {
        this._localChange = true
        this._channel?.postMessage({ type: 'put', key, rec })
    }
    private _broadcastDel(key: S): V {
        this._localChange = true
        this._channel?.postMessage({ type: 'del', key })
    }
    private _broadcastClear(): V {
        this._localChange = true
        this._channel?.postMessage({ type: 'clear' })
    }
    private _onRemote(msg: A): V {
        if (!msg || typeof msg !== 'object') return
        switch (msg.type) {
            case 'put': {
                const remote = msg.rec as EntryRecord
                if (!remote?.hlc) return
                this._hlc.update(remote.hlc)
                const local = this.memory.get(msg.key)
                if (local && this._hlc.compare(local.hlc, remote.hlc) >= 0) return // local wins
                this.memory.set(msg.key, remote)
                this._emit(msg.key, remote.value)
                break
            }
            case 'del': {
                this.memory.delete(msg.key)
                this._emit(msg.key, undefined)
                break
            }
            case 'clear': {
                this.memory.clear()
                // don't re-emit every key; consumers should listen for clear
                this._emit('__clear__', undefined)
                break
            }
        }
    }
    // ───────────────────────────────────────────────────────────────────────
    // Conflict policy — optional, mirrors your ConflictPolicy shape
    // ───────────────────────────────────────────────────────────────────────
    async merge<T = A>(key: S, incoming: T, policy: ConflictResolver<T>): P<V> {
        const local = await this.get<T>(key)
        if (local === undefined) return this.set(key, incoming)
        const localRec = this.memory.get(key)!
        const remoteHlc = this._hlc.tick()
        const resolved = policy(local, incoming, {
            key,
            localHlc: localRec.hlc,
            remoteHlc,
            nodeId: this._nodeId,
        })
        return this.set(key, resolved)
    }
    // ───────────────────────────────────────────────────────────────────────
    // Memory cache control
    // ───────────────────────────────────────────────────────────────────────
    touchMemory(): V {
        if (this.memoryTimer) clearTimeout(this.memoryTimer)
        this.memoryTimer = setTimeout(() => {
            this.memory.clear()
            this.memoryTimer = undefined
        }, this.ttlMs)
    }
    /** Force memory to rehydrate from IDB on next read. */
    invalidateMemory(key?: S): V {
        if (key) this.memory.delete(key)
        else this.memory.clear()
    }
    private _withLock<T>(key: S, fn: () => T) {
        if (typeof navigator !== 'undefined' && navigator.locks) {
          return navigator.locks.request('mem_write_' + key, fn)
        }
        return fn()
      }
    // ───────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ───────────────────────────────────────────────────────────────────────
    async close(): P<V> {
        this._closed = true
        if (this.memoryTimer) clearTimeout(this.memoryTimer)
        this._channel?.close()
        if (this._db) { this._db.close(); this._db = null }
    }
    isOpen(): B { return !!this._db }
    async info(): P<{ isOpen: B; dbName: S; storeName: S; version: N; count: N }> {
        if (!this._db) await this.open()
        const tx = this._db!.transaction(this.storeName, 'readonly')
        const count = await new Promise<N>((resolve, reject) => {
            const r = tx.objectStore(this.storeName).count()
            r.onsuccess = () => resolve(r.result)
            r.onerror = () => reject(r.error)
        })
        return {
            isOpen: !!this._db,
            dbName: this.dbName,
            storeName: this.storeName,
            version: this._version,
            count,
        }
    }
}

