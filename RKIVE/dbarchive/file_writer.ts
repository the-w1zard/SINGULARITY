let Bun: A = null
if (typeof (globalThis as A).Bun !== 'undefined') { Bun = (globalThis as A).Bun; }
let Deno: A = null
if (typeof (globalThis as A).Deno !== 'undefined') { Deno = (globalThis as A).Deno; }

export class BINARY {
    public storage_path: A;
    private indexMap = new Map<S, A>();
    private memoryCache = new Map<S, { value: A; expiry: N }>();
    private defaultMemoryTtlMs: N;
    Bun: A = null;
    Deno: A = null;
    hasBun;
    hasDeno;
    hasNode;
    isBrowser = typeof window !== "undefined";
    isVSCode;
    isElectron;
    isUnix;
    isWindows;
    #path
    #fs
  
    constructor(storage_path = "./") {
      this.storage_path = storage_path;
      this.defaultMemoryTtlMs = 30;
      if (typeof (globalThis as A).Deno !== "undefined") { this.Deno = (globalThis as A).Deno; }
      if (typeof (globalThis as A).Bun !== "undefined") { this.Bun = (globalThis as A).Bun; }
      this.hasBun = typeof (globalThis as A).Bun !== "undefined";
      this.hasDeno = typeof (globalThis as A).Deno !== "undefined";
      this.hasNode = typeof process !== "undefined" && !!process.versions?.node;
      this.isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
      this.isElectron = false;
      this.isUnix = false;
      this.isWindows = true;
      if (!this.hasDeno) {
        this.#path = require("path");
        this.#fs = require("fs");
      }
     
    }
    //                                    FILE WRITES
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
  
    public init_kv_ngin(namespaceDir: S, memoryTtlMs = 15 * 60 * 1000) { 
      this.storage_path = this.#path.resolve(namespaceDir);
      this.defaultMemoryTtlMs = memoryTtlMs;
      this.kv_ngin.initializeStorage();
    }
    public kv_ngin = {
      initializeStorage: (): V => { 
        if (!this.#fs.existsSync(this.storage_path)) {this.#fs.mkdirSync(this.storage_path, { recursive: true });}
        this.kv_ngin.rebuildIndexFromDisk();
      },
      rebuildIndexFromDisk: (): V => { 
        const entries = this.#fs.readdirSync(this.storage_path, {
          withFileTypes: true,
        });
        const now = Date.now();
  
        for (const entry of entries) {
          if (entry.isFile() && entry.name.endsWith(".bin")) {
            const key = this.#path.basename(entry.name, ".bin");
            const fullPath = this.#path.join(this.storage_path, entry.name);
            const stats = this.#fs.statSync(fullPath);
  
            this.indexMap.set(key, {
              updatedAt: stats.mtimeMs,
              isBinaryFile: true,
              filePath: fullPath,
            });
          }
        }
      },
      set: async <T,>(key: S, value: T, ttlMs?: N): P<V> => {
        if (this.hasBun) { 
          const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, "_");
          const filePath = this.#path.join(this.storage_path, `${safeKey}.bin`);
          const tempPath = `${filePath}.tmp`;
          let p: U8;
          if (value instanceof Uint8Array) p = value;
          else if (value instanceof ArrayBuffer) p = new Uint8Array(value);
          else p = this.encode_msg(value); // Atomic write via temp file rename to prevent corruption
          await Bun.write(tempPath, p);
          if (this.#fs) { this.#fs.unlinkSync(filePath); }
          Bun.renameSync(tempPath, filePath);
          const stats = this.#fs.statSync(filePath);
          this.indexMap.set(key, {
            updatedAt: stats.mtimeMs,
            isBinaryFile: true,
            filePath,
          }); // Set memory cache hot layer with TTL
          const expiry = (now: any) => now + (ttlMs || this.defaultMemoryTtlMs);
          this.memoryCache.set(key, { value, expiry: expiry(Date.now()) });
        } else if (this.hasDeno) {
          const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, "_");
          const filePath = `${this.storage_path}/${safeKey}.bin`;
          const tempPath = `${filePath}.tmp`;
          const dir = filePath.substring(0, filePath.lastIndexOf("/"));
          let p: U8;
          if (value instanceof Uint8Array) p = value;
          else if (value instanceof ArrayBuffer) p = new Uint8Array(value);
          else p = this.encode_msg(value); // Atomic write via temp file rename to prevent corruption
          await Deno.mkdir(dir, { recursive: true });
          await Deno.writeFile(tempPath, p);
          try { await Deno.remove(filePath); } 
          catch (error) { if (!(error instanceof Deno.errors.NotFound)) { throw error; } }
          Deno.renameSync(tempPath, filePath);
          const stats = Deno.statSync(filePath);
          this.indexMap.set(key, {
            updatedAt: stats.mtime?.getTime() ?? Date.now(),
            isBinaryFile: true,
            filePath,
          }); // Set memory cache hot layer with TTL
          const expiry = Date.now() + (ttlMs || this.defaultMemoryTtlMs);
          this.memoryCache.set(key, { value, expiry });
        } else {
          const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, "_");
          const filePath = this.#path.join(this.storage_path, `${safeKey}.bin`);
          const tempPath = `${filePath}.tmp`;
          let p: U8;
          if (value instanceof Uint8Array) p = value;
          else if (value instanceof ArrayBuffer) p = new Uint8Array(value);
          else p = this.encode_msg(value); // Atomic write via temp file rename to prevent corruption
          this.#fs.writeFileSync(tempPath, p);
          if (this.#fs.existsSync(filePath)) { this.#fs.unlinkSync(filePath); }
          this.#fs.renameSync(tempPath, filePath);
          const stats = this.#fs.statSync(filePath);
          this.indexMap.set(key, {
            updatedAt: stats.mtimeMs,
            isBinaryFile: true,
            filePath,
          }); // Set memory cache hot layer with TTL
          const expiry = (now: A) => now + (ttlMs || this.defaultMemoryTtlMs);
          this.memoryCache.set(key, { value, expiry: expiry(Date.now()) });
        }
      },
      delete: async (key: S): P<B> => {
        if (this.hasBun) {
          const indexEntry = this.indexMap.get(key);
          if (indexEntry?.filePath) {
            const file = Bun.file(indexEntry.filePath);
            if (await file.exists()) {await file.delete();}
          }
          this.indexMap.delete(key);
          this.memoryCache.delete(key);
          return true;
        } else if (this.hasDeno) {
          const indexEntry = this.indexMap.get(key);
          if (indexEntry?.filePath) {
            try { await Deno.remove(indexEntry.filePath);} 
            catch {}
          }
          this.indexMap.delete(key);
          this.memoryCache.delete(key);
          return true;
        } else {
          const indexEntry = this.indexMap.get(key);
          if (indexEntry?.filePath && this.#fs.existsSync(indexEntry.filePath)) { this.#fs.unlinkSync(indexEntry.filePath); }
          this.indexMap.delete(key);
          this.memoryCache.delete(key);
          return true;
        }
      },
      get: async <T,>(key: S): P<T | NU> => {
        if (this.hasBun) {
          const now = Date.now();
          const cached = this.memoryCache.get(key);
          if (cached) {
            if (cached.expiry > now) {return cached.value as T;}
            this.memoryCache.delete(key);
          }
          const indexEntry = this.indexMap.get(key);
          if (!indexEntry || !indexEntry.filePath) {return null;}
          const file = Bun.file(indexEntry.filePath); // Check if file exists using Bun's file API
          const exists = await file.exists();
          if (!exists) {return null;}
          const rawBuffer = await file.arrayBuffer();
          if (rawBuffer.byteLength === 0) return null;
          try {
            const decoded = this.decode_msg(new Uint8Array(rawBuffer));
            this.memoryCache.set(key, {
              value: decoded,
              expiry: now + this.defaultMemoryTtlMs,
            });
            return decoded as T;
          } catch {
            return new Uint8Array(rawBuffer) as U as T;
          }
        } else if (this.hasDeno) {
          const now = Date.now();
          const cached = this.memoryCache.get(key);
          if (cached) {
            if (cached.expiry > now) {return cached.value as T;}
            this.memoryCache.delete(key);
          }
          const indexEntry = this.indexMap.get(key);
          if (!indexEntry || !indexEntry.filePath) {return null;}
          try {
            const fileInfo = await Deno.stat(indexEntry.filePath);
            if (!fileInfo.isFile) {return null;}
          } catch {
            return null;
          }
          const rawBuffer = await Deno.readFile(indexEntry.filePath);
          if (rawBuffer.length === 0) return null;
          try {
            const decoded = this.decode_msg(rawBuffer);
            this.memoryCache.set(key, {
              value: decoded,
              expiry: now + this.defaultMemoryTtlMs,
            });
            return decoded as T;
          } catch {
            return rawBuffer as U as T;
          }
        } else {
          const now = Date.now();
          const cached = this.memoryCache.get(key);
          if (cached) {
            if (cached.expiry > now) {return cached.value as T;}
            this.memoryCache.delete(key);
          }
          const indexEntry = this.indexMap.get(key);
          if (!indexEntry ||!indexEntry.filePath ||!this.#fs.existsSync(indexEntry.filePath)) {return null;}
          const rawBuffer = this.#fs.readFileSync(indexEntry.filePath); // If it's a raw binary array/buffer storage mode, return the Uint8Array directly
          if (rawBuffer.length === 0) return null;
          try {
            const decoded = this.decode_msg(rawBuffer);
            this.memoryCache.set(key, {
              value: decoded,
              expiry: now + this.defaultMemoryTtlMs,
            });
            return decoded as T;
          } catch {
            // Fallback for raw byte buffers if CBOR decode fails
            return rawBuffer as U as T;
          }
        }
      },
      getRunningSize: (): N => {return this.indexMap.size;},
    };
    // #endregion
    //                                    ENCODERS
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
    private readonly encoder = new TextEncoder();
    private readonly decoder = new TextDecoder();
    private ensureAvailable(buf: Uint8Array, pos: number, length: number): void {
      if (pos < 0 || length < 0 || pos + length > buf.length)
        throw new Error("CBOR: truncated buffer");
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
        return [
          new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(
            0,
            false,
          ),
          pos + 3,
        ];
      }
      if (info === 26) {
        this.ensureAvailable(buf, pos + 1, 4);
        return [
          new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(
            0,
            false,
          ),
          pos + 5,
        ];
      }
      if (info === 27) {
        this.ensureAvailable(buf, pos + 1, 8);
        const value = new DataView(
          buf.buffer,
          buf.byteOffset + pos + 1,
          8,
        ).getBigUint64(0, false);
        if (value > BigInt(Number.MAX_SAFE_INTEGER))
          throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER");
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
        const encoded = this.encoder.encode(v);
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
          throw new Error(
            `CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`,
          );
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
        return [this.decoder.decode(bytes), nextPos + n];
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
        console.log(
          "[BINARY_PIPE] Failed to encode outbound packet strictly:",
          err,
        );
        return this.encode({
          type: "error",
          error: "Encoding failed",
          id: msg?.id || "unknown",
        });
      }
    }
    public decode_msg(raw: any): any {
      // Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
      if (
        !(raw instanceof Uint8Array) &&
        !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))
      ) {
        console.error(
          "[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream.",
        );
        return null;
      }
      try {
        const uint8 =
          raw instanceof Uint8Array
            ? raw
            : new Uint8Array(
                (raw as any).buffer,
                (raw as any).byteOffset,
                (raw as any).length,
              );
        return this.decode(uint8);
      } catch (err) {
        console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
        return null;
      }
    }
    // #endregion
    //                                    SERIALIZER
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
    HEADER_SIZE_BYTES = 64; // Int32 Array Indices (4 bytes per element)
    HEADER_IDX_WRITE_HEAD = 0; // Byte offset 0
    HEADER_IDX_READ_HEAD = 1; // Byte offset 4
    HEADER_IDX_MUTEX = 2; // Byte offset 8
    HEADER_IDX_CAPACITY = 3; // Byte offset 12
    MUTEX_UNLOCKED = 0; // Mutex States
    MUTEX_LOCKED = 1;
    SENTINEL_WRAP_MARKER = 0xffffffff; // 0xFFFFFFFF (Uint32 max) written to length prefix slot when p wraps to offset 0
    public FLYWEIGHT_FRAME: DecodedFrame = {
      topicId: 0,
      topicName: undefined,
      p: new Uint8Array(0),
    };
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
        capacity,
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
    private topicToIdMap: Map<string, number> = new Map();
    private idToTopicMap: Map<number, string> = new Map();
    public hashTopic(topic: string): number {
      let hash = 0x811c9dc5;
      for (let i = 0; i < topic.length; i++) {
        hash ^= topic.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193) >>> 0;
      }
      return hash;
    }
    public getTopicId(topic: string): number {
      const existingId = this.topicToIdMap.get(topic);
      if (existingId !== undefined) {
        return existingId;
      }
  
      const topicId = this.hashTopic(topic);
      const collisionTopic = this.idToTopicMap.get(topicId);
      if (collisionTopic !== undefined && collisionTopic !== topic) {
        throw new Error(
          `Topic hash collision detected between "${topic}" and "${collisionTopic}" (ID: ${topicId})`,
        );
      }
  
      this.topicToIdMap.set(topic, topicId);
      this.idToTopicMap.set(topicId, topic);
      return topicId;
    }
    public getTopicName(topicId: number): string | undefined {
      return this.idToTopicMap.get(topicId);
    }
    public registerTopics(topics: string[]): void {
      for (const topic of topics) {
        this.getTopicId(topic);
      }
    }
    public resetRegistry(): void {
      this.topicToIdMap.clear();
      this.idToTopicMap.clear();
    }
    // Reusable scratch buffer & DataView for header manipulations (Zero GC)
    private headerBuffer = new ArrayBuffer(4);
    private headerView = new DataView(this.headerBuffer);
    private headerBytes = new Uint8Array(this.headerBuffer);
  
    public encodeFrame(
      topic: string,
      p: Uint8Array | string | Record<string, unknown>,
    ): Uint8Array {
      const topicId = this.getTopicId(topic);
  
      let payloadBytes: Uint8Array;
      if (p instanceof Uint8Array) {
        payloadBytes = p;
      } else if (typeof p === "string") {
        payloadBytes = this.encoder.encode(p);
      } else {
        payloadBytes = this.encoder.encode(JSON.stringify(p));
      }
  
      const frame = new Uint8Array(4 + payloadBytes.byteLength);
  
      // 1. Write Uint32 Topic ID (Little-Endian) via byte copies (Zero DataView allocation)
      this.headerView.setUint32(0, topicId, true);
      frame.set(this.headerBytes, 0);
  
      // 2. Copy Payload Bytes
      frame.set(payloadBytes, 4);
  
      return frame;
    }
    public decodeFrame(frameBytes: Uint8Array): DecodedFrame {
      if (frameBytes.byteLength < 4) {
        throw new Error(
          "Invalid frame: buffer smaller than minimum header size (4 bytes).",
        );
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
