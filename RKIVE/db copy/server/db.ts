//                                                                                                                     IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Worker } from "node:worker_threads";
import { mkdir, open, readFile, rm, stat} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { encodeUtf8, decodeUtf8, writeBigUint64, writeUint32, readUint32, readBigUint64} from "./binary";
import { isWorkerError, MANIFEST_VERSION, NO_PAGE, OP_DELETE, OP_PUT, PAGE_FREE, PAGE_HEADER_SIZE, PAGE_PAYLOAD_CAPACITY, PAGE_RECORD_CONTINUATION, PAGE_RECORD_HEAD, PAGE_SIZE, PendingBatch, PendingWorkerRequest, StoredEntry, WAL_VERSION, WorkerRequestInput, type Durability, type FastKVOptions, type PageWrite, type WorkerRequest, type WorkerResponse} from "./types";

const workerPath = fileURLToPath( new URL("./storage-worker.ts", import.meta.url));
// #endregion
//                                                                                                                       TYPES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type MaybePromise<R = any> = R;
function MaybePromise(v) { return v !== undefined ? v : Promise.resolve(v);}
export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
export class A2<T = A> extends Array<T> implements A2<T> {
  constructor(...items: T[]) {
      super(...items);
      Object.setPrototypeOf(this, new.target.prototype);
  }

  static get [Symbol.species](): ArrayConstructor {
      return Array;
  }

  static from<T>(arrayLike: ArrayLike<T> | Iterable<T>): A2<T> {
      return new A2<T>(...Array.from(arrayLike));
  }

  static of<T>(...items: T[]): A2<T> {
      return new A2<T>(...items);
  }

  first(): T | UD {
      return this[0];
  }

  last(): T | UD {
      return this[this.length - 1];
  }

  isEmpty(): B {
      return this.length === 0;
  }

  isNotEmpty(): B {
      return this.length !== 0;
  }

  clear(): this {
      this.length = 0;
      return this;
  }

  clone(): A2<T> {
      return new A2<T>(...this);
  }

  unique(): A2<T> {
      return new A2<T>(...new Set(this));
  }

  compact(): A2<NonNullable<T>> {
      return new A2<NonNullable<T>>(
          ...this.filter((value): value is NonNullable<T> => value != null)
      );
  }

  take(count: N): A2<T> {
      return new A2<T>(...this.slice(0, Math.max(0, count)));
  }

  takeLast(count: N): A2<T> {
      return new A2<T>(...this.slice(Math.max(0, this.length - Math.max(0, count))));
  }

  drop(count: N): A2<T> {
      return new A2<T>(...this.slice(Math.max(0, count)));
  }

  dropLast(count: N): A2<T> {
      return new A2<T>(...this.slice(0, Math.max(0, this.length - Math.max(0, count))));
  }

  chunk(size: N): A2<A2<T>> {
      if (!Number.isInteger(size) || size < 1) {
          throw new RangeError("Chunk size must be a positive integer.");
      }

      const result = new A2<A2<T>>();

      for (let i = 0; i < this.length; i += size) {
          result.push(new A2<T>(...this.slice(i, i + size)));
      }

      return result;
  }

  flatten<X>(depth: N = Infinity): A2<X> {
      return new A2<X>(...this.flat(depth) as X[]);
  }

  flatMap<U>(callback: (value: T, index: N, array: this) => U | readonly U[]): A2<U> {
      return new A2<U>(
          ...this.flatMap((value, index) => callback(value, index as N, this))
      );
  }

  distinctBy<K>(callback: (value: T, index: N) => K): A2<T> {
      const seen = new Set<K>();
      const result = new A2<T>();

      this.forEach((value, index) => {
          const key = callback(value, index as N);

          if (!seen.has(key)) {
              seen.add(key);
              result.push(value);
          }
      });

      return result;
  }

  groupBy<K extends PropertyKey>(callback: (value: T, index: N) => K): Map<K, A2<T>> {
      const result = new Map<K, A2<T>>();

      this.forEach((value, index) => {
          const key = callback(value, index as N);
          let group = result.get(key);

          if (!group) {
              group = new A2<T>();
              result.set(key, group);
          }

          group.push(value);
      });

      return result;
  }

  partition(callback: (value: T, index: N) => B): [A2<T>, A2<T>] {
      const yes = new A2<T>();
      const no = new A2<T>();

      this.forEach((value, index) => {
          (callback(value, index as N) ? yes : no).push(value);
      });

      return [yes, no];
  }

  insert(index: N, ...items: T[]): this {
      this.splice(index, 0, ...items);
      return this;
  }

  removeAt(index: N): T | UD {
      const normalized = index < 0 ? this.length + index : index;

      if (normalized < 0 || normalized >= this.length) {
          return undefined;
      }

      return this.splice(normalized, 1)[0];
  }

  remove(value: T): B {
      const index = this.indexOf(value);

      if (index === -1) {
          return false;
      }

      this.splice(index, 1);
      return true;
  }

  removeAll(value: T): N {
      let removed = 0;

      for (let i = this.length - 1; i >= 0; i--) {
          if (Object.is(this[i], value)) {
              this.splice(i, 1);
              removed++;
          }
      }

      return removed as N;
  }

  replaceAt(index: N, value: T): T | UD {
      const normalized = index < 0 ? this.length + index : index;

      if (normalized < 0 || normalized >= this.length) {
          return undefined;
      }

      const previous = this[normalized];
      this[normalized] = value;
      return previous;
  }

  move(from: N, to: N): this {
      const source = from < 0 ? this.length + from : from;
      const target = to < 0 ? this.length + to : to;

      if (
          source < 0 || source >= this.length ||
          target < 0 || target >= this.length ||
          source === target
      ) {
          return this;
      }

      const [item] = this.splice(source, 1);
      this.splice(target, 0, item);
      return this;
  }

  swap(first: N, second: N): this {
      const a = first < 0 ? this.length + first : first;
      const b = second < 0 ? this.length + second : second;

      if (
          a < 0 || a >= this.length ||
          b < 0 || b >= this.length
      ) {
          return this;
      }

      [this[a], this[b]] = [this[b], this[a]];
      return this;
  }

  sample(): T | UD {
      if (this.length === 0) {
          return undefined;
      }

      return this[Math.floor(Math.random() * this.length)];
  }

  shuffle(): this {
      for (let i = this.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [this[i], this[j]] = [this[j], this[i]];
      }

      return this;
  }

  countBy(callback: (value: T, index: N) => B): N {
      let count = 0;

      this.forEach((value, index) => {
          if (callback(value, index as N)) count++;
      });

      return count as N;
  }

  sum(callback?: (value: T, index: N) => N): N {
      let total = 0;

      this.forEach((value, index) => {
          total += callback ? callback(value, index as N) : Number(value);
      });

      return total as N;
  }

  average(callback?: (value: T, index: N) => N): N {
      return (this.length === 0 ? NaN : this.sum(callback) / this.length) as N;
  }

  min(callback?: (value: T, index: N) => N): T | UD {
      if (this.length === 0) return undefined;

      return this.reduce((best, value, index) => {
          const current = callback ? callback(value, index as N) : Number(value);
          const bestValue = callback ? callback(best, this.indexOf(best) as N) : Number(best);

          return current < bestValue ? value : best;
      });
  }

  max(callback?: (value: T, index: N) => N): T | UD {
      if (this.length === 0) return undefined;

      return this.reduce((best, value, index) => {
          const current = callback ? callback(value, index as N) : Number(value);
          const bestValue = callback ? callback(best, this.indexOf(best) as N) : Number(best);

          return current > bestValue ? value : best;
      });
  }

  equals(array: readonly T[]): B {
      return this.length === array.length &&
          this.every((value, index) => Object.is(value, array[index]));
  }

  includesAll(values: readonly T[]): B {
      return values.every(value => this.includes(value));
  }

  intersects(values: readonly T[]): B {
      return values.some(value => this.includes(value));
  }
}
// #endregion
//                                                                                                                    MANIFEST
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const MANIFEST_MAGIC = 0x464b5631; // FKV1
const MANIFEST_SIZE = 24;
export interface Manifest {
  checkpointSequence: bigint;
  pageCount: number;
}
export function encodeManifest(  manifest: Manifest): Uint8Array {
  const bytes = new Uint8Array(MANIFEST_SIZE);
  const view = new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength
  );

  view.setUint32(0, MANIFEST_MAGIC, true);
  view.setUint32(4, MANIFEST_VERSION, true);
  writeBigUint64(bytes, 8, manifest.checkpointSequence);
  writeUint32(bytes, 16, manifest.pageCount);
  writeUint32(bytes, 20, crc32(bytes, 0, 20));

  return bytes;
}
export function decodeManifest(  bytes: Uint8Array): Manifest | undefined {
  if (bytes.length < MANIFEST_SIZE) return undefined;

  const view = new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength
  );

  if (view.getUint32(0, true) !== MANIFEST_MAGIC) {
    return undefined;
  }

  if (view.getUint32(4, true) !== MANIFEST_VERSION) {
    return undefined;
  }

  const storedChecksum = readUint32(bytes, 20);

  if (storedChecksum !== crc32(bytes, 0, 20)) {
    return undefined;
  }

  return {
    checkpointSequence: readBigUint64(bytes, 8),
    pageCount: readUint32(bytes, 16)
  };
}
// #endregion
//                                                                                                                         WAL
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface WalRecord {
  sequence: bigint;
  operation: number;
  key: Uint8Array;
  value: Uint8Array;
}
export function encodeWalRecord(
  record: WalRecord
): Uint8Array {
  // Single allocation: the frame is written in place, no body staging buffer.
  const bodyLength =
    1 +
    1 +
    8 +
    4 +
    4 +
    record.key.length +
    record.value.length;

  const frame = new Uint8Array(
    4 + bodyLength + 4
  );

  const view = new DataView(
    frame.buffer,
    frame.byteOffset,
    frame.byteLength
  );

  view.setUint32(0, bodyLength, true);

  let offset = 4;

  view.setUint8(offset, WAL_VERSION);
  offset += 1;

  view.setUint8(offset, record.operation);
  offset += 1;

  view.setBigUint64(offset, record.sequence, true);
  offset += 8;

  view.setUint32(offset, record.key.length, true);
  offset += 4;

  view.setUint32(offset, record.value.length, true);
  offset += 4;

  frame.set(record.key, offset);
  offset += record.key.length;

  frame.set(record.value, offset);

  view.setUint32(
    4 + bodyLength,
    crc32(frame, 4, 4 + bodyLength),
    true
  );

  return frame;
}
export interface DecodedWalResult {
  records: WalRecord[];
  validBytes: number;
}
export function decodeWalRecords(
  bytes: Uint8Array,
  minimumSequence: bigint
): DecodedWalResult {
  const records: WalRecord[] = [];
  let offset = 0;

  while (offset + 4 <= bytes.length) {
    const bodyLength = readUint32(bytes, offset);

    const frameEnd =
      offset + 4 + bodyLength + 4;

    if (
      bodyLength < 18 ||
      frameEnd > bytes.length
    ) {
      break;
    }

    const storedChecksum = readUint32(
      bytes,
      offset + 4 + bodyLength
    );

    const actualChecksum = crc32(
      bytes,
      offset + 4,
      offset + 4 + bodyLength
    );

    if (storedChecksum !== actualChecksum) {
      break;
    }

    const body = bytes.subarray(
      offset + 4,
      offset + 4 + bodyLength
    );

    const bodyView = new DataView(
      body.buffer,
      body.byteOffset,
      body.byteLength
    );

    const version = bodyView.getUint8(0);

    if (version !== WAL_VERSION) {
      break;
    }

    const operation = bodyView.getUint8(1);
    const sequence = bodyView.getBigUint64(2, true);
    const keyLength = bodyView.getUint32(10, true);
    const valueLength = bodyView.getUint32(14, true);

    if (sequence <= minimumSequence) {
      offset = frameEnd;
      continue;
    }

    const keyStart = 18;
    const valueStart = keyStart + keyLength;
    const valueEnd = valueStart + valueLength;

    if (valueEnd > body.length) {
      break;
    }

    records.push({
      sequence,
      operation,
      key: body.subarray(keyStart, valueStart),
      value: body.subarray(valueStart, valueEnd)
    });

    offset = frameEnd;
  }

  return {
    records,
    validBytes: offset
  };
}


// #endregion
//                                                                                                                         CRC
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
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
//                                                                                                                       PAGES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  export interface RecordChain {
    pageNumbers: number[];
    pages: Uint8Array[];
  }
  export function encodeRecordChain(    key: Uint8Array,    value: Uint8Array,    allocatePage: () => number  ): RecordChain {
    // No payload staging buffer: key/value bytes are written straight into the
    // destination pages. payload layout is [12-byte header][key][value].
    const keyLength = key.length;
    const valueLength = value.length;
    const payloadLength = 12 + keyLength + valueLength;
  
    const pageCount = Math.max(
      1,
      Math.ceil(payloadLength / PAGE_PAYLOAD_CAPACITY)
    );
  
    const pageNumbers: number[] = [];
    const pages: Uint8Array[] = [];
    const valueChecksum = crc32(value);
    const keyEndInPayload = 12 + keyLength;
  
    for (let index = 0; index < pageCount; index++) {
      const pageNumber = allocatePage();
      const page = new Uint8Array(PAGE_SIZE);
      const view = new DataView(
        page.buffer,
        page.byteOffset,
        page.byteLength
      );
  
      const payloadStart = index * PAGE_PAYLOAD_CAPACITY;
      const payloadEnd = Math.min(
        payloadLength,
        payloadStart + PAGE_PAYLOAD_CAPACITY
      );
  
      const used = payloadEnd - payloadStart;
  
      pageNumbers.push(pageNumber);
      pages.push(page);
  
      view.setUint32(
        0,
        index === 0 ? PAGE_RECORD_HEAD : PAGE_RECORD_CONTINUATION,
        true
      );
  
      view.setUint32(4, NO_PAGE, true); // next, patched below once known
      view.setUint32(8, used, true);
  
      let dataOffset = PAGE_HEADER_SIZE;
      let dataStart = payloadStart;
  
      if (index === 0) {
        view.setUint32(dataOffset, keyLength, true);
        view.setUint32(dataOffset + 4, valueLength, true);
        view.setUint32(dataOffset + 8, valueChecksum, true);
        dataOffset += 12;
        dataStart = 12;
      }
  
      if (dataStart < keyEndInPayload) {
        const copyEnd = Math.min(payloadEnd, keyEndInPayload);
        page.set(
          key.subarray(dataStart - 12, copyEnd - 12),
          dataOffset
        );
        dataOffset += copyEnd - dataStart;
        dataStart = copyEnd;
      }
  
      if (dataStart < payloadEnd) {
        page.set(
          value.subarray(dataStart - keyEndInPayload, payloadEnd - keyEndInPayload),
          dataOffset
        );
      }
  
      view.setUint32(
        12,
        crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used),
        true
      );
    }
  
    // Patching the next pointer does not affect the checksum: crc covers
    // [PAGE_HEADER_SIZE, ...), never the 16-byte header.
    for (let index = 0; index < pageCount - 1; index++) {
      const page = pages[index]!;
      new DataView(
        page.buffer,
        page.byteOffset,
        page.byteLength
      ).setUint32(4, pageNumbers[index + 1]!, true);
    }
  
    return {
      pageNumbers,
      pages
    };
  }
  export interface DecodedRecord {
    key: Uint8Array;
    value: Uint8Array;
  }
  export function decodeRecordChain(    readPage: (pageNumber: number) => Uint8Array | undefined,    headPageNumber: number  ): DecodedRecord | undefined {
    const chunks: Uint8Array[] = [];
    let pageNumber: number | undefined = headPageNumber;
    let isHead = true;
  
    while (
      pageNumber !== undefined &&
      pageNumber !== NO_PAGE
    ) {
      const page = readPage(pageNumber);
  
      if (!page) return undefined;
  
      const view = new DataView(
        page.buffer,
        page.byteOffset,
        page.byteLength
      );
  
      const state = view.getUint32(0, true);
      const nextPage = view.getUint32(4, true);
      const used = view.getUint32(8, true);
      const checksum = view.getUint32(12, true);
  
      const expectedState = isHead
        ? PAGE_RECORD_HEAD
        : PAGE_RECORD_CONTINUATION;
  
      if (
        state !== expectedState ||
        used > PAGE_PAYLOAD_CAPACITY ||
        checksum !== crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)
      ) {
        return undefined;
      }
  
      chunks.push(
        page.subarray(PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)
      );
  
      pageNumber = nextPage === NO_PAGE ? undefined : nextPage;
      isHead = false;
    }
  
    if (chunks.length === 0) return undefined;
  
    const payloadLength = chunks.reduce(
      (total, chunk) => total + chunk.length,
      0
    );
  
    const payload = new Uint8Array(payloadLength);
    let offset = 0;
  
    for (const chunk of chunks) {
      payload.set(chunk, offset);
      offset += chunk.length;
    }
  
    if (payload.length < 12) return undefined;
  
    const payloadView = new DataView(
      payload.buffer,
      payload.byteOffset,
      payload.byteLength
    );
  
    const keyLength = payloadView.getUint32(0, true);
    const valueLength = payloadView.getUint32(4, true);
    const valueChecksum = payloadView.getUint32(8, true);
  
    const key = payload.subarray(12, 12 + keyLength);
    const value = payload.subarray(
      12 + keyLength,
      12 + keyLength + valueLength
    );
  
    if (value.length !== valueLength) return undefined;
    if (crc32(value) !== valueChecksum) return undefined;
  
    return {
      key,
      value
    };
  }
  // A free page is a constant: one shared, never-mutated buffer for every
  // freed page instead of a fresh zeroed PAGE_SIZE allocation per page.
  const FREE_PAGE: Uint8Array = (() => {
    const page = new Uint8Array(PAGE_SIZE);
    new DataView(
      page.buffer,
      page.byteOffset,
      page.byteLength
    ).setUint32(0, PAGE_FREE, true);
    return page;
  })();
  export function encodeFreePage(): Uint8Array {
    return FREE_PAGE;
  }

// #endregion
//                                                                                                                          DB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const KV_RPC_OP = {
  GET: 1,
  SET: 2,
  DELETE: 3,
  UPDATE: 4,
  SUBSCRIBE: 5,
  UNSUBSCRIBE: 6,
  SNAPSHOT: 7,
  CHANGE: 8,
  ERROR: 9
} as const;

export type KV_RPC_OP_VALUE = typeof KV_RPC_OP[keyof typeof KV_RPC_OP];
export class WAL_KVDB {
  //                                                                                                                   CONSTANTS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public r: S = "THE_WIZARD_ROOT_REG";
  public m: S = "SAURON_MASTER_REG";
  public s: S = "SAURON_SLAVE_REG";
  public k: S = "__SAURON__";
  hot_cache: Record<string, unknown> = Object.create(null);
  ///hot_cache = new O2<S, A>();
  waiters: Record<S, A> = Object.create(null);
  entries: Record<S, StoredEntry> = Object.create(null) ;
  pending: Record<S, PendingWorkerRequest> = Object.create(null);
  dirtyPages: Record<N, Uint8Array> = Object.create(null);
  readonly #options: Required<FastKVOptions>;
  #directory!: S;
  #dataPath!: S;
  #walPath!: S;
  #manifestPath!: S;
  #worker?: Worker;
  #freePages: N[] = [];
  #pageCount = 0;
  #nextSequence = 1n;
  #checkpointSequence = 0n;
  #walSize = 0;
  #batch?: PendingBatch;
  #batchTimer?: NodeJS.Timeout;
  #writeQueue: P<U> = Promise.resolve();
  #closed = false;
  private _onError: (err: U) => void

  constructor(k: S = "__SAURON__", s: S = "SAURON_SLAVE_REG", m: S = "SAURON_MASTER_REG", r: S = "THE_WIZARD_ROOT_REG", options: Partial<FastKVOptions> = {}) {
    this.k = k
    this.s = s
    this.m = m
    this.r = r
    this.#options = {
      directory: options.directory ?? "./data/fastkv",
      durability: options.durability ?? "group",
      walBatchBytes: options.walBatchBytes ?? 256 * 1024,
      walBatchRecords: options.walBatchRecords ?? 4096,
      walFlushIntervalMs: options.walFlushIntervalMs ?? 1,
      walHighWaterBytes: options.walHighWaterBytes ?? 16 * 1024 * 1024,
      checkpointIntervalMs: options.checkpointIntervalMs ?? 5_000
    };
  }
  // #endregion
  //                                                                                                                         API
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public get = (() => {
    const z = <T = A>(k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {  
      const cached = this.hot_cache[k];
      if (cached !== undefined) return MaybePromise(cached as T)
      else {
        const entry = this.entries[k]
        if (!entry) return undefined;
        return MaybePromise(this.#workerRequest({
          kind: "readPages",
          pageNumbers: entry.pageNumbers
        }).then((response) => {
          if (response.kind !== "readPagesOk") {
            throw new Error(response.kind === "error" ? response.error : "Unexpected worker response" );
          }
          const decoded = this.#decodeRecordChainFromPages(response.pages,entry.pageNumbers);
          if (!decoded) {return undefined;}
          this.hot_cache[k] = decoded.value;
          return decoded.value as T;
        }))
      }    
    }
    z.many = async (keys: S[]): P<V> => {
      await Promise.all(Object.entries(keys).map(([k]) => this.get(k)))
    }
    z.hot = <T = A>(k: S,) => {  
     return this.hot_cache[k]
    }
    return z;
  })();
  public hot<T = A>(k: S,) {  
    return this.hot_cache[k]
  }
  public set = (() => {
    type ZOptions = { durable: boolean; silent?: boolean; origin?: string };
    const z = async (k: S, value: A, s: S = this.s, m: S = this.m, r: S = this.r, options: ZOptions = { durable: true } ): P<V> => {
          this.hot_cache[k] =value;
        // this.hot_cache.set(k, value);

      if (this.waiters.size !== 0) {
        const wk = `__waiters_${k}`
        const ws = this.waiters.get(wk) as Set<(value: unknown) => void> | undefined
        if (ws) { for (const resolve of ws) resolve(value); this.waiters.delete(wk); }
      }
      this._emit(k, value)

      const valueBytes = typeof value === "string" ? encodeUtf8(value) : value;
      const keyBytes = encodeUtf8(k);
      return this.#enqueueWrite(async () => {
        if (this.#closed) { throw new Error("FastKV is closed"); }
        this.#applyPut(k, keyBytes, valueBytes);
        const record = encodeWalRecord({
          sequence: this.#nextSequence++,
          operation: OP_PUT,
          key: keyBytes,
          value: valueBytes
        });
        const durable = this.#resolveDurability(options.durable);
        const flushPromise = this.#appendToBatch( record, durable );
        if ( options.durable === true || this.#options.durability === "strict"  ) {  await flushPromise;  }
      });
    }
    z.many = async (entries: Record<S, A>): P<V> => {
      await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)))
    }
    return z;
  })();
  public delete = (() => {
    const z = async (key: S, options: { durable?: B } = {} ): P<B>=> {
      const keyBytes = encodeUtf8(key);
      this._emit(key, undefined)
      return this.#enqueueWrite(async () => {
        if (this.#closed) {throw new Error("FastKV is closed");}
        if (!(key in this.entries)) { return false; }
        this.#applyDelete(key);
        const record = encodeWalRecord({
          sequence: this.#nextSequence++,
          operation: OP_DELETE,
          key: keyBytes,
          value: new Uint8Array(0)
        });
        const durable = this.#resolveDurability(options.durable);
        const flushPromise = this.#appendToBatch(record,durable);
        if ( options.durable === true || this.#options.durability === "strict"  ) {await flushPromise;}
        return true;
      });
    } 
    z.many = async (keys: S[]): P<V> => {await Promise.all(keys.map((k) => this.delete(k))) }
    return z;
  })();
  public update = (() => {
    const z = async <T = A>(key: S, updater: (prev: T | UD) => T) => {
      const prev = await this.get<T>(key)
      const next = updater(prev)
      await this.set(key, next)
      return next
  }
    z.many = () => {}
    return z;
  })();
  public has(key: S): B { return Object.hasOwn(this.entries, key);}
  public send = (() => {
    const z = () => {}
    z.many = () => {}
    return z;
  })();
  // #endregion
  //                                                                                                                         SUB
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public subscribe<T = A>(key: S, cb: Subscriber<T>): Teardown {
    let set = this.waiters.get(key)
    if (!set) { set = new Set(); this.waiters.set(key, set) }
    set.add(cb)

    const cached = this.hot_cache[key]
    if (cached) { try { cb(cached as T) } catch (e) { this._onError(e) } }

    return () => {
        const s = this.waiters.get(key)
        if (!s) return
        s.delete(cb)
        if (s.size === 0) this.waiters.delete(key)
    }
  }/** Selector-based subscription — only fires when the selected slice changes. */
  public select<T = A, S = A>(key: A, selector: Selector<T | UD, S>, cb: Subscriber<S>, chain?: CHAIN): Teardown {
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
        const set = this.waiters.get(key)
        if (!set) return
        for (const cb of set) {
            try { cb(value) }
            catch (e) { this._onError(e) }
        }
  }
  private _peek(key: S): A {
    const v = this.hot_cache[key]
    return v === undefined ? undefined : v
  }
  public waitFor<T>(key: S): Promise<T> {  
    const existing = this._peek(key)
    if (existing !== undefined) return Promise.resolve(existing)

    return new Promise<T>((resolve) => {
        const waiterKey = `__waiters_${key}`
        let waiters = this.waiters.get(waiterKey) as Set<(value: T) => void> | undefined

        if (!waiters) {
            waiters = new Set<(value: T) => void>()
            this.waiters.set(waiterKey, waiters)
        }

        waiters.add(resolve)
    })
  }
  // #endregion
  //                                                                                                                  WORKER FNS
  // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
  public async open(): P<V> {
    if (this.#worker) { throw new Error("FastKV is already open"); }

    this.#directory = path.resolve(this.#options.directory);
    this.#dataPath = path.join(this.#directory, "data.paged");
    this.#walPath = path.join(this.#directory, "wal.log");
    this.#manifestPath = path.join(this.#directory, "manifest.bin");

    await mkdir(this.#directory, { recursive: true });

    const dataFile = await open(this.#dataPath, "a+");
    await dataFile.close();

    const walFile = await open(this.#walPath, "a+");
    await walFile.close();

    await this.#recover();
    await this.#startWorker();

    setInterval(() => {
      void this.#checkpointIfRequired();
    }, this.#options.checkpointIntervalMs).unref();
  }
  public async flush(): P<V> {
    await this.#enqueueWrite(async () => {
      await this.#flushCurrentBatch();
    });
  }
  public async checkpoint():P<V> {
    await this.#enqueueWrite(async () => {
      await this.#checkpoint();
    });
  }
  public async compact(): P<V> {
    await this.#enqueueWrite(async () => {
      await this.#flushCurrentBatch();
      const compactPath = `${this.#dataPath}.compact`;
      await this.#workerRequest({ kind: "compactBegin", compactPath });
      const livePages: PageWrite[] = [];
      let nextPage = 0;
      // const oldEntries = new O2(this.entries);
      // const oldValues = new O2(this.hot_cache);
      const oldEntries = Object.assign(Object.create(null), this.entries);
      const oldValues = Object.assign(Object.create(null), this.hot_cache);
      this.entries = Object.create(null);
      // this.hot_cache.clear();
      this.hot_cache = Object.create(null);
      this.#freePages = [];
      this.dirtyPages = Object.create(null);
      this.#pageCount = 0;
      for (const [key, entry] of oldEntries) {
        const value = oldValues.get(key);
        if (!value) continue;
       // this.hot_cache.set(key, value);
       this.hot_cache[key] = value;
        const keyBytes = encodeUtf8(key);
        const chain = encodeRecordChain(
          keyBytes,
          value,
          () => nextPage++
        );

        for (const page of chain.pages) {
          livePages.push({
            pageNumber: chain.pageNumbers[0]!,
            buffer: page
          });
        }

        this.entries[key] = {
          pageNumber: chain.pageNumbers[0]!,
          pageNumbers: chain.pageNumbers
      };

      }
      this.#pageCount = nextPage;
      const manifest = encodeManifest({checkpointSequence: this.#nextSequence, pageCount: this.#pageCount });
      await this.#workerRequest({ kind: "compactWrite", pages: livePages });
      await this.#workerRequest({
        kind: "compactCommit",
        dataPath: this.#dataPath,
        compactPath,
        manifest,
        walPath: this.#walPath
      });
      this.#checkpointSequence = this.#nextSequence;
      this.#walSize = 0;
    });
  }
  public async close(): P<V> {
    if (this.#closed) return;

    this.#closed = true;

    await this.#enqueueWrite(async () => {
      await this.#checkpoint();
    });

    await this.#workerRequest({
      kind: "close"
    });

    await this.#worker?.terminate();
    this.#worker = undefined;
  }
  get stats() {
    return {
      keys: this.entries.size,
      pages: this.#pageCount,
      freePages: this.#freePages.length,
      dirtyPages: Object.keys(this.dirtyPages).length,
      walBytes: this.#walSize,
      checkpointSequence: this.#checkpointSequence,
      nextSequence: this.#nextSequence,
      durability: this.#options.durability
    };
  }
  #resolveDurability(
    requested: boolean | undefined
  ): boolean {
    if (requested !== undefined) {
      return requested;
    }
  
    return (
      this.#options.durability === "group" ||
      this.#options.durability === "strict"
    );
  }
  #applyPut( key: string, keyBytes: Uint8Array, valueBytes: Uint8Array, replay = false ): void {
    const previous = this.entries[key]

    const chain = encodeRecordChain(
      keyBytes,
      valueBytes,
      () => {
        const reused = this.#freePages.pop();

        if (reused !== undefined) {
          return reused;
        }

        return this.#pageCount++;
      }
    );

    if (previous) {
      for (const pageNumber of previous.pageNumbers) {
        this.dirtyPages[pageNumber] = encodeFreePage();

        this.#freePages.push(pageNumber);
      }
    }

    for (let index = 0; index < chain.pages.length; index++) {
      this.dirtyPages[chain.pageNumbers[index]!] = chain.pages[index]!;
  }
  
  this.entries[key] = {
      pageNumber: chain.pageNumbers[0]!,
      pageNumbers: chain.pageNumbers
  };

   // this.hot_cache.set(key, valueBytes);

    if (replay) {
      this.#nextSequence = this.#nextSequence;
    }
  }
  #applyDelete(key: string, replay = false ): void {
    const previous = this.entries[key]

    if (!previous) return;

    for (const pageNumber of previous.pageNumbers) {
      this.dirtyPages[pageNumber] = encodeFreePage();

      this.#freePages.push(pageNumber);
    }

    delete this.entries[key];
    //this.hot_cache.delete(key);
    delete this.hot_cache[key];
    if (replay) {
      this.#nextSequence = this.#nextSequence;
    }
  }
  #enqueueWrite<T>(
    operation: () => Promise<T>
  ): Promise<T> {
    const result = this.#writeQueue.then(operation, operation);

    this.#writeQueue = result.catch(() => undefined);

    return result;
  }
  #appendToBatch( record: Uint8Array, durable: boolean  ): Promise<void> {
    if (!this.#batch) {
      this.#batch = {
        batchId: crypto.randomUUID(),
        records: [],
        byteLength: 0,
        durable: false,
        waiters: []
      };
    }
  
    const batch = this.#batch;
  
    batch.records.push(record);
    batch.byteLength += record.length;
    batch.durable ||= durable;
  
    return new Promise<void>((resolve) => {
      batch.waiters.push(resolve);
  
      if (
        batch.byteLength >= this.#options.walBatchBytes ||
        batch.records.length >= this.#options.walBatchRecords
      ) {
        void this.#flushCurrentBatch();
        return;
      }
  
      this.#batchTimer ??= setTimeout(() => {
        this.#batchTimer = undefined;
        void this.#flushCurrentBatch();
      }, this.#options.walFlushIntervalMs);
    });
  }
  #workerRequest(request: WorkerRequestInput ): Promise<WorkerResponse> {
    if (!this.#worker) {return Promise.reject( new Error("FastKV is not open") );    }

    const id = crypto.randomUUID();

    return new Promise<WorkerResponse>((resolve, reject) => {
      this.pending[id] = {
        resolve,
        reject
    };

      this.#worker!.postMessage({
        ...request,
        id
      } satisfies WorkerRequest);
    });
  }
  // pages.ts
  #decodeRecordChainFromPages(
    pages: Uint8Array[],
    pageNumbers: number[]
  ): { key: Uint8Array; value: Uint8Array } | undefined {
    const pageByNumber = new Map<number, Uint8Array>();

    for (let index = 0; index < pageNumbers.length; index++) {
      const pageNumber = pageNumbers[index]!;
      const page = pages[index];

      if (page) {
        pageByNumber.set(pageNumber, page);
      }
    }

    const firstPageNumber = pageNumbers[0];

    if (firstPageNumber === undefined) {
      return undefined;
    }

    return decodeRecordChain(
      (pageNumber) => pageByNumber.get(pageNumber),
      firstPageNumber
    );
  }
  async #recover(): Promise<void> {
    const manifestBytes = await readFile(this.#manifestPath, { flag: "r" } ).catch(() => undefined);
    const manifest = manifestBytes ? decodeManifest(manifestBytes) : undefined;
    this.#checkpointSequence =  manifest?.checkpointSequence ?? 0n;
    const dataFile = await open(this.#dataPath, "r");
    try {
      const fileSize = Number((await dataFile.stat()).size);
      const pageCount = Math.floor(fileSize / PAGE_SIZE);
      const pageCache = new Map<number, Uint8Array>();
      for (let pageNumber = 0; pageNumber < pageCount; pageNumber++) {
        const page = new Uint8Array(PAGE_SIZE);
        const buffer = Buffer.from(
          page.buffer,
          page.byteOffset,
          page.byteLength
        );
        const result = await dataFile.read(
          buffer,
          0,
          PAGE_SIZE,
          pageNumber * PAGE_SIZE
        );
        if (result.bytesRead !== PAGE_SIZE) break;
        pageCache.set(pageNumber, page);
      }
      const readPage = (pageNumber: N ): U8 | UD => pageCache.get(pageNumber);
      for (const pageNumber of pageCache.keys()) {
        const page = pageCache.get(pageNumber)!;
        const view = new DataView(
          page.buffer,
          page.byteOffset,
          page.byteLength
        );
        const state = view.getUint32(0, true);
        if (state === 0) { this.#freePages.push(pageNumber); continue; }
        if (state !== 1) continue;
        const decoded = decodeRecordChain( readPage, pageNumber );
        if (!decoded) continue;
        const key = decodeUtf8(decoded.key);
        const pageNumbers: number[] = [];
        let cursor: number | undefined = pageNumber;
        while (cursor !== undefined && cursor !== NO_PAGE) {
          pageNumbers.push(cursor);
          const currentPage = pageCache.get(cursor)!;
          const currentView = new DataView(
            currentPage.buffer,
            currentPage.byteOffset,
            currentPage.byteLength
          );
          const nextPage = currentView.getUint32(4, true);
          cursor = nextPage === NO_PAGE ? undefined : nextPage;
        }

        this.entries[key] = {
          pageNumber,
          pageNumbers
      };

        //this.hot_cache.set(key, decoded.value);
        this.hot_cache[key] = decoded.value;
      }

      this.#pageCount = pageCount;
    } finally {
      await dataFile.close();
    }

    const walBytes = await readFile(this.#walPath)
      .catch(() => new Uint8Array(0));

    const decoded = decodeWalRecords(
      walBytes,
      this.#checkpointSequence
    );

    for (const record of decoded.records) {
      const key = decodeUtf8(record.key);

      if (record.operation === OP_PUT) {
        this.#applyPut(
          key,
          record.key,
          record.value,
          true
        );
      }

      else if (record.operation === OP_DELETE) {
        this.#applyDelete(key, true);
      }

      this.#nextSequence = record.sequence + 1n;
    }

    this.#walSize = decoded.validBytes;
  }
  async #flushCurrentBatch(): Promise<void> {
    const batch = this.#batch;
    if (!batch || batch.records.length === 0) { return; }
    this.#batch = undefined;
    if (this.#batchTimer) { clearTimeout(this.#batchTimer);  this.#batchTimer = undefined;  }
    let totalLength = 0;
    for (const record of batch.records) { totalLength += record.length;  }
    const bytes = new Uint8Array(totalLength);
    let offset = 0;
    for (const record of batch.records) {
      bytes.set(record, offset);
      offset += record.length;
    }
    try {
      const response = await this.#workerRequest({
        kind: "appendWal",
        batchId: batch.batchId,
        bytes,
        durable: batch.durable
      });
      if (isWorkerError(response)) {throw new Error(`WAL append failed: ${response.error}`); }
      if (response.kind !== "walOk") {throw new Error(`Unexpected WAL response: ${response.kind}`);}
      this.#walSize = response.walSize;
      for (const resolve of batch.waiters) {  resolve(); }
    } catch (error) {
      for (const waiter of batch.waiters) {
        // Resolve rather than reject: durability waiters must not hang.
        waiter();
      }
      throw error;
    }
  }
  async #checkpointIfRequired(): Promise<void> {
    if (this.#walSize >= this.#options.walHighWaterBytes) {
      await this.checkpoint();
    }
  }
  async #checkpoint(): Promise<void> {
    if (!this.#worker) {
      throw new Error("FastKV is not open");
    }

    await this.#flushCurrentBatch();

    const pages: PageWrite[] = [];

    for (const pageNumber in this.dirtyPages) {
      pages.push({
          pageNumber: Number(pageNumber),
          buffer: this.dirtyPages[pageNumber]!.slice()
      });
  }

    const manifest = encodeManifest({
      checkpointSequence: this.#nextSequence - 1n,
      pageCount: this.#pageCount
    });

    await this.#workerRequest({
      kind: "checkpoint",
      pages,
      manifest
    });

    this.dirtyPages = Object.create(null);
    this.#checkpointSequence = this.#nextSequence - 1n;
    this.#walSize = 0;
  }
  async #startWorker(): Promise<void> {
    this.#worker = new Worker(workerPath);
    this.#worker.on("message", (response: WorkerResponse) => {
      const pending = this.pending[response.id]
      if (!pending) return;
      delete this.pending[response.id];
      if (isWorkerError(response)) {
        pending.reject(new Error(response.error));
        return;
      }
      pending.resolve(response);
    });
    this.#worker.on("error", (error) => {
      for (const id in this.pending) {
        this.pending[id]!.reject(error);
    }
      this.pending = Object.create(null);
    });
    await this.#workerRequest({
      kind: "init",
      dataPath: this.#dataPath,
      walPath: this.#walPath
    });
  }
  
  // #endregion
}

// #endregion
