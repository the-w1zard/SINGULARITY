//                                                                                                                     IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Worker } from "node:worker_threads";
import { mkdir, open, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { encodeUtf8, decodeUtf8 } from "./binary";
import { isWorkerError, NO_PAGE, OP_DELETE, OP_PUT, PAGE_SIZE } from "./types";
import { decodeManifest, decodeRecordChain, decodeWalRecords, encodeFreePage, encodeManifest, encodeRecordChain, encodeWalRecord } from "./wal";
const workerPath = fileURLToPath(new URL("./storage-worker.ts", import.meta.url));
function MaybePromise(v) { return v !== undefined ? v : Promise.resolve(v); }
export const rv = (v) => typeof v === "function" ? v() : v;
export const sameValue = (a, b) => Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
export class A2 extends Array {
    constructor(...items) {
        super(...items);
        Object.setPrototypeOf(this, new.target.prototype);
    }
    static get [Symbol.species]() {
        return Array;
    }
    static from(arrayLike) {
        return new A2(...Array.from(arrayLike));
    }
    static of(...items) {
        return new A2(...items);
    }
    first() {
        return this[0];
    }
    last() {
        return this[this.length - 1];
    }
    isEmpty() {
        return this.length === 0;
    }
    isNotEmpty() {
        return this.length !== 0;
    }
    clear() {
        this.length = 0;
        return this;
    }
    clone() {
        return new A2(...this);
    }
    unique() {
        return new A2(...new Set(this));
    }
    compact() {
        return new A2(...this.filter((value) => value != null));
    }
    take(count) {
        return new A2(...this.slice(0, Math.max(0, count)));
    }
    takeLast(count) {
        return new A2(...this.slice(Math.max(0, this.length - Math.max(0, count))));
    }
    drop(count) {
        return new A2(...this.slice(Math.max(0, count)));
    }
    dropLast(count) {
        return new A2(...this.slice(0, Math.max(0, this.length - Math.max(0, count))));
    }
    chunk(size) {
        if (!Number.isInteger(size) || size < 1) {
            throw new RangeError("Chunk size must be a positive integer.");
        }
        const result = new A2();
        for (let i = 0; i < this.length; i += size) {
            result.push(new A2(...this.slice(i, i + size)));
        }
        return result;
    }
    flatten(depth = Infinity) {
        return new A2(...this.flat(depth));
    }
    flatMap(callback) {
        return new A2(...this.flatMap((value, index) => callback(value, index, this)));
    }
    distinctBy(callback) {
        const seen = new Set();
        const result = new A2();
        this.forEach((value, index) => {
            const key = callback(value, index);
            if (!seen.has(key)) {
                seen.add(key);
                result.push(value);
            }
        });
        return result;
    }
    groupBy(callback) {
        const result = new Map();
        this.forEach((value, index) => {
            const key = callback(value, index);
            let group = result.get(key);
            if (!group) {
                group = new A2();
                result.set(key, group);
            }
            group.push(value);
        });
        return result;
    }
    partition(callback) {
        const yes = new A2();
        const no = new A2();
        this.forEach((value, index) => {
            (callback(value, index) ? yes : no).push(value);
        });
        return [yes, no];
    }
    insert(index, ...items) {
        this.splice(index, 0, ...items);
        return this;
    }
    removeAt(index) {
        const normalized = index < 0 ? this.length + index : index;
        if (normalized < 0 || normalized >= this.length) {
            return undefined;
        }
        return this.splice(normalized, 1)[0];
    }
    remove(value) {
        const index = this.indexOf(value);
        if (index === -1) {
            return false;
        }
        this.splice(index, 1);
        return true;
    }
    removeAll(value) {
        let removed = 0;
        for (let i = this.length - 1; i >= 0; i--) {
            if (Object.is(this[i], value)) {
                this.splice(i, 1);
                removed++;
            }
        }
        return removed;
    }
    replaceAt(index, value) {
        const normalized = index < 0 ? this.length + index : index;
        if (normalized < 0 || normalized >= this.length) {
            return undefined;
        }
        const previous = this[normalized];
        this[normalized] = value;
        return previous;
    }
    move(from, to) {
        const source = from < 0 ? this.length + from : from;
        const target = to < 0 ? this.length + to : to;
        if (source < 0 || source >= this.length ||
            target < 0 || target >= this.length ||
            source === target) {
            return this;
        }
        const [item] = this.splice(source, 1);
        this.splice(target, 0, item);
        return this;
    }
    swap(first, second) {
        const a = first < 0 ? this.length + first : first;
        const b = second < 0 ? this.length + second : second;
        if (a < 0 || a >= this.length ||
            b < 0 || b >= this.length) {
            return this;
        }
        [this[a], this[b]] = [this[b], this[a]];
        return this;
    }
    sample() {
        if (this.length === 0) {
            return undefined;
        }
        return this[Math.floor(Math.random() * this.length)];
    }
    shuffle() {
        for (let i = this.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [this[i], this[j]] = [this[j], this[i]];
        }
        return this;
    }
    countBy(callback) {
        let count = 0;
        this.forEach((value, index) => {
            if (callback(value, index))
                count++;
        });
        return count;
    }
    sum(callback) {
        let total = 0;
        this.forEach((value, index) => {
            total += callback ? callback(value, index) : Number(value);
        });
        return total;
    }
    average(callback) {
        return (this.length === 0 ? NaN : this.sum(callback) / this.length);
    }
    min(callback) {
        if (this.length === 0)
            return undefined;
        return this.reduce((best, value, index) => {
            const current = callback ? callback(value, index) : Number(value);
            const bestValue = callback ? callback(best, this.indexOf(best)) : Number(best);
            return current < bestValue ? value : best;
        });
    }
    max(callback) {
        if (this.length === 0)
            return undefined;
        return this.reduce((best, value, index) => {
            const current = callback ? callback(value, index) : Number(value);
            const bestValue = callback ? callback(best, this.indexOf(best)) : Number(best);
            return current > bestValue ? value : best;
        });
    }
    equals(array) {
        return this.length === array.length &&
            this.every((value, index) => Object.is(value, array[index]));
    }
    includesAll(values) {
        return values.every(value => this.includes(value));
    }
    intersects(values) {
        return values.some(value => this.includes(value));
    }
}
// #endregion
//                                                                                                                          DB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class WAL_KVDB {
    //                                                                                                                   CONSTANTS
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    r = "THE_WIZARD_ROOT_REG";
    m = "SAURON_MASTER_REG";
    s = "SAURON_SLAVE_REG";
    k = "__SAURON__";
    hot_cache = Object.create(null);
    ///hot_cache = new O2<S, A>();
    waiters = Object.create(null);
    entries = Object.create(null);
    pending = Object.create(null);
    dirtyPages = Object.create(null);
    #options;
    #directory;
    #dataPath;
    #walPath;
    #manifestPath;
    #worker;
    #freePages = [];
    #pageCount = 0;
    #nextSequence = 1n;
    #checkpointSequence = 0n;
    #walSize = 0;
    #batch;
    #batchTimer;
    #writeQueue = Promise.resolve();
    #closed = false;
    _onError;
    constructor(k = "__SAURON__", s = "SAURON_SLAVE_REG", m = "SAURON_MASTER_REG", r = "THE_WIZARD_ROOT_REG", options = {}) {
        this.k = k;
        this.s = s;
        this.m = m;
        this.r = r;
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
    get = (() => {
        const z = (k, s = this.s, m = this.m, r = this.r) => {
            const cached = this.hot_cache[k];
            if (cached !== undefined)
                return MaybePromise(cached);
            else {
                const entry = this.entries[k];
                if (!entry)
                    return undefined;
                return MaybePromise(this.#workerRequest({
                    kind: "readPages",
                    pageNumbers: entry.pageNumbers
                }).then((response) => {
                    if (response.kind !== "readPagesOk") {
                        throw new Error(response.kind === "error" ? response.error : "Unexpected worker response");
                    }
                    const decoded = this.#decodeRecordChainFromPages(response.pages, entry.pageNumbers);
                    if (!decoded) {
                        return undefined;
                    }
                    this.hot_cache[k] = decoded.value;
                    return decoded.value;
                }));
            }
        };
        z.many = async (keys) => {
            await Promise.all(Object.entries(keys).map(([k]) => this.get(k)));
        };
        z.hot = (k) => {
            return this.hot_cache[k];
        };
        return z;
    })();
    hot(k) {
        return this.hot_cache[k];
    }
    set = (() => {
        const z = async (k, value, s = this.s, m = this.m, r = this.r, options = { durable: true }) => {
            this.hot_cache[k] = value;
            // this.hot_cache.set(k, value);
            if (this.waiters.size !== 0) {
                const wk = `__waiters_${k}`;
                const ws = this.waiters.get(wk);
                if (ws) {
                    for (const resolve of ws)
                        resolve(value);
                    this.waiters.delete(wk);
                }
            }
            this._emit(k, value);
            const valueBytes = typeof value === "string" ? encodeUtf8(value) : value;
            const keyBytes = encodeUtf8(k);
            return this.#enqueueWrite(async () => {
                if (this.#closed) {
                    throw new Error("FastKV is closed");
                }
                this.#applyPut(k, keyBytes, valueBytes);
                const record = encodeWalRecord({
                    sequence: this.#nextSequence++,
                    operation: OP_PUT,
                    key: keyBytes,
                    value: valueBytes
                });
                const durable = this.#resolveDurability(options.durable);
                const flushPromise = this.#appendToBatch(record, durable);
                if (options.durable === true || this.#options.durability === "strict") {
                    await flushPromise;
                }
            });
        };
        z.many = async (entries) => {
            await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)));
        };
        return z;
    })();
    delete = (() => {
        const z = async (key, options = {}) => {
            const keyBytes = encodeUtf8(key);
            this._emit(key, undefined);
            return this.#enqueueWrite(async () => {
                if (this.#closed) {
                    throw new Error("FastKV is closed");
                }
                if (!(key in this.entries)) {
                    return false;
                }
                this.#applyDelete(key);
                const record = encodeWalRecord({
                    sequence: this.#nextSequence++,
                    operation: OP_DELETE,
                    key: keyBytes,
                    value: new Uint8Array(0)
                });
                const durable = this.#resolveDurability(options.durable);
                const flushPromise = this.#appendToBatch(record, durable);
                if (options.durable === true || this.#options.durability === "strict") {
                    await flushPromise;
                }
                return true;
            });
        };
        z.many = async (keys) => { await Promise.all(keys.map((k) => this.delete(k))); };
        return z;
    })();
    update = (() => {
        const z = async (key, updater) => {
            const prev = await this.get(key);
            const next = updater(prev);
            await this.set(key, next);
            return next;
        };
        z.many = () => { };
        return z;
    })();
    has(key) { return Object.hasOwn(this.entries, key); }
    send = (() => {
        const z = () => { };
        z.many = () => { };
        return z;
    })();
    // #endregion
    //                                                                                                                         SUB
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    subscribe(key, cb) {
        let set = this.waiters.get(key);
        if (!set) {
            set = new Set();
            this.waiters.set(key, set);
        }
        set.add(cb);
        const cached = this.hot_cache[key];
        if (cached) {
            try {
                cb(cached);
            }
            catch (e) {
                this._onError(e);
            }
        }
        return () => {
            const s = this.waiters.get(key);
            if (!s)
                return;
            s.delete(cb);
            if (s.size === 0)
                this.waiters.delete(key);
        };
    } /** Selector-based subscription — only fires when the selected slice changes. */
    select(key, selector, cb, chain) {
        let last;
        let first = true;
        return this.subscribe(key, (v) => {
            const next = selector(v);
            if (first || !Object.is(last, next)) {
                first = false;
                last = next;
                cb(next);
            }
        });
    }
    _emit(key, value) {
        const set = this.waiters.get(key);
        if (!set)
            return;
        for (const cb of set) {
            try {
                cb(value);
            }
            catch (e) {
                this._onError(e);
            }
        }
    }
    _peek(key) {
        const v = this.hot_cache[key];
        return v === undefined ? undefined : v;
    }
    waitFor(key) {
        const existing = this._peek(key);
        if (existing !== undefined)
            return Promise.resolve(existing);
        return new Promise((resolve) => {
            const waiterKey = `__waiters_${key}`;
            let waiters = this.waiters.get(waiterKey);
            if (!waiters) {
                waiters = new Set();
                this.waiters.set(waiterKey, waiters);
            }
            waiters.add(resolve);
        });
    }
    // #endregion
    //                                                                                                                  WORKER FNS
    // #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    async open() {
        if (this.#worker) {
            throw new Error("FastKV is already open");
        }
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
    async flush() {
        await this.#enqueueWrite(async () => {
            await this.#flushCurrentBatch();
        });
    }
    async checkpoint() {
        await this.#enqueueWrite(async () => {
            await this.#checkpoint();
        });
    }
    async compact() {
        await this.#enqueueWrite(async () => {
            await this.#flushCurrentBatch();
            const compactPath = `${this.#dataPath}.compact`;
            await this.#workerRequest({ kind: "compactBegin", compactPath });
            const livePages = [];
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
                if (!value)
                    continue;
                // this.hot_cache.set(key, value);
                this.hot_cache[key] = value;
                const keyBytes = encodeUtf8(key);
                const chain = encodeRecordChain(keyBytes, value, () => nextPage++);
                for (const page of chain.pages) {
                    livePages.push({
                        pageNumber: chain.pageNumbers[0],
                        buffer: page
                    });
                }
                this.entries[key] = {
                    pageNumber: chain.pageNumbers[0],
                    pageNumbers: chain.pageNumbers
                };
            }
            this.#pageCount = nextPage;
            const manifest = encodeManifest({ checkpointSequence: this.#nextSequence, pageCount: this.#pageCount });
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
    async close() {
        if (this.#closed)
            return;
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
    #resolveDurability(requested) {
        if (requested !== undefined) {
            return requested;
        }
        return (this.#options.durability === "group" ||
            this.#options.durability === "strict");
    }
    #applyPut(key, keyBytes, valueBytes, replay = false) {
        const previous = this.entries[key];
        const chain = encodeRecordChain(keyBytes, valueBytes, () => {
            const reused = this.#freePages.pop();
            if (reused !== undefined) {
                return reused;
            }
            return this.#pageCount++;
        });
        if (previous) {
            for (const pageNumber of previous.pageNumbers) {
                this.dirtyPages[pageNumber] = encodeFreePage();
                this.#freePages.push(pageNumber);
            }
        }
        for (let index = 0; index < chain.pages.length; index++) {
            this.dirtyPages[chain.pageNumbers[index]] = chain.pages[index];
        }
        this.entries[key] = {
            pageNumber: chain.pageNumbers[0],
            pageNumbers: chain.pageNumbers
        };
        // this.hot_cache.set(key, valueBytes);
        if (replay) {
            this.#nextSequence = this.#nextSequence;
        }
    }
    #applyDelete(key, replay = false) {
        const previous = this.entries[key];
        if (!previous)
            return;
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
    #enqueueWrite(operation) {
        const result = this.#writeQueue.then(operation, operation);
        this.#writeQueue = result.catch(() => undefined);
        return result;
    }
    #appendToBatch(record, durable) {
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
        return new Promise((resolve) => {
            batch.waiters.push(resolve);
            if (batch.byteLength >= this.#options.walBatchBytes ||
                batch.records.length >= this.#options.walBatchRecords) {
                void this.#flushCurrentBatch();
                return;
            }
            this.#batchTimer ??= setTimeout(() => {
                this.#batchTimer = undefined;
                void this.#flushCurrentBatch();
            }, this.#options.walFlushIntervalMs);
        });
    }
    #workerRequest(request) {
        if (!this.#worker) {
            return Promise.reject(new Error("FastKV is not open"));
        }
        const id = crypto.randomUUID();
        return new Promise((resolve, reject) => {
            this.pending[id] = {
                resolve,
                reject
            };
            this.#worker.postMessage({
                ...request,
                id
            });
        });
    }
    // pages.ts
    #decodeRecordChainFromPages(pages, pageNumbers) {
        const pageByNumber = new Map();
        for (let index = 0; index < pageNumbers.length; index++) {
            const pageNumber = pageNumbers[index];
            const page = pages[index];
            if (page) {
                pageByNumber.set(pageNumber, page);
            }
        }
        const firstPageNumber = pageNumbers[0];
        if (firstPageNumber === undefined) {
            return undefined;
        }
        return decodeRecordChain((pageNumber) => pageByNumber.get(pageNumber), firstPageNumber);
    }
    async #recover() {
        const manifestBytes = await readFile(this.#manifestPath, { flag: "r" }).catch(() => undefined);
        const manifest = manifestBytes ? decodeManifest(manifestBytes) : undefined;
        this.#checkpointSequence = manifest?.checkpointSequence ?? 0n;
        const dataFile = await open(this.#dataPath, "r");
        try {
            const fileSize = Number((await dataFile.stat()).size);
            const pageCount = Math.floor(fileSize / PAGE_SIZE);
            const pageCache = new Map();
            for (let pageNumber = 0; pageNumber < pageCount; pageNumber++) {
                const page = new Uint8Array(PAGE_SIZE);
                const buffer = Buffer.from(page.buffer, page.byteOffset, page.byteLength);
                const result = await dataFile.read(buffer, 0, PAGE_SIZE, pageNumber * PAGE_SIZE);
                if (result.bytesRead !== PAGE_SIZE)
                    break;
                pageCache.set(pageNumber, page);
            }
            const readPage = (pageNumber) => pageCache.get(pageNumber);
            for (const pageNumber of pageCache.keys()) {
                const page = pageCache.get(pageNumber);
                const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
                const state = view.getUint32(0, true);
                if (state === 0) {
                    this.#freePages.push(pageNumber);
                    continue;
                }
                if (state !== 1)
                    continue;
                const decoded = decodeRecordChain(readPage, pageNumber);
                if (!decoded)
                    continue;
                const key = decodeUtf8(decoded.key);
                const pageNumbers = [];
                let cursor = pageNumber;
                while (cursor !== undefined && cursor !== NO_PAGE) {
                    pageNumbers.push(cursor);
                    const currentPage = pageCache.get(cursor);
                    const currentView = new DataView(currentPage.buffer, currentPage.byteOffset, currentPage.byteLength);
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
        }
        finally {
            await dataFile.close();
        }
        const walBytes = await readFile(this.#walPath)
            .catch(() => new Uint8Array(0));
        const decoded = decodeWalRecords(walBytes, this.#checkpointSequence);
        for (const record of decoded.records) {
            const key = decodeUtf8(record.key);
            if (record.operation === OP_PUT) {
                this.#applyPut(key, record.key, record.value, true);
            }
            else if (record.operation === OP_DELETE) {
                this.#applyDelete(key, true);
            }
            this.#nextSequence = record.sequence + 1n;
        }
        this.#walSize = decoded.validBytes;
    }
    async #flushCurrentBatch() {
        const batch = this.#batch;
        if (!batch || batch.records.length === 0) {
            return;
        }
        this.#batch = undefined;
        if (this.#batchTimer) {
            clearTimeout(this.#batchTimer);
            this.#batchTimer = undefined;
        }
        let totalLength = 0;
        for (const record of batch.records) {
            totalLength += record.length;
        }
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
            if (isWorkerError(response)) {
                throw new Error(`WAL append failed: ${response.error}`);
            }
            if (response.kind !== "walOk") {
                throw new Error(`Unexpected WAL response: ${response.kind}`);
            }
            this.#walSize = response.walSize;
            for (const resolve of batch.waiters) {
                resolve();
            }
        }
        catch (error) {
            for (const waiter of batch.waiters) {
                // Resolve rather than reject: durability waiters must not hang.
                waiter();
            }
            throw error;
        }
    }
    async #checkpointIfRequired() {
        if (this.#walSize >= this.#options.walHighWaterBytes) {
            await this.checkpoint();
        }
    }
    async #checkpoint() {
        if (!this.#worker) {
            throw new Error("FastKV is not open");
        }
        await this.#flushCurrentBatch();
        const pages = [];
        for (const pageNumber in this.dirtyPages) {
            pages.push({
                pageNumber: Number(pageNumber),
                buffer: this.dirtyPages[pageNumber].slice()
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
    async #startWorker() {
        this.#worker = new Worker(workerPath);
        this.#worker.on("message", (response) => {
            const pending = this.pending[response.id];
            if (!pending)
                return;
            delete this.pending[response.id];
            if (isWorkerError(response)) {
                pending.reject(new Error(response.error));
                return;
            }
            pending.resolve(response);
        });
        this.#worker.on("error", (error) => {
            for (const id in this.pending) {
                this.pending[id].reject(error);
            }
            this.pending = Object.create(null);
        });
        await this.#workerRequest({
            kind: "init",
            dataPath: this.#dataPath,
            walPath: this.#walPath
        });
    }
}
/**
    const watchHandlers = new Map<S, Map<S, (value: A) => V>>();
        
case "watch": {
  const watchKey = k!;
  const handler = (val: any) => {
    stream.send({
      key: watchKey,
      value: val !== undefined ? JSON.stringify(val) : null,
      event: "watch",
    });
  };

  if (!watchHandlers.has("watch")) {
    watchHandlers.set("watch", new Map());
  }
  watchHandlers.get("watch")!.set(watchKey, handler);

  await db.watch(watchKey, handler);
  result = { subscribed: true, key: watchKey };
  break;
}
case "unwatch": {
  const watchKey = k!;
  const handlers = watchHandlers.get("watch");
  if (handlers && handlers.has(watchKey)) {
    handlers.delete(watchKey);
    result = { unsubscribed: true, key: watchKey };
  } else {
    result = { unsubscribed: false, error: "No such watch" };
  }
  break;
} */
// #endregion
