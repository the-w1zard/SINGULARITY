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
import { HYBRID_LOGICAL_CLOCK_CRDT } from '#hlc';
const isBrowser = typeof window !== 'undefined';
export function runTeardown(t) {
    if (t == null)
        return;
    if (typeof t === 'function')
        return void t();
    if (Array.isArray(t)) {
        for (const x of t)
            runTeardown(x);
        return;
    }
    const a = t;
    if (typeof a.unsubscribe === 'function')
        return void a.unsubscribe();
    if (typeof a[Symbol.dispose] === 'function')
        return void a[Symbol.dispose]();
}
export function reportError(e) {
    const g = globalThis;
    if (typeof g.reportError === 'function')
        return void g.reportError(e);
    queueMicrotask(() => { throw e; });
}
export class INDEXED_DB {
    dbName;
    storeName;
    _version;
    _migrations = new Map();
    _nodeId;
    _hlc;
    _onFlush;
    _onError;
    // ── connection ──────────────────────────────────────────────────────────
    _db = null;
    _opening = null;
    // ── memory cache (SWR) ──────────────────────────────────────────────────
    memory = new O2();
    inflight = new O2();
    ttlMs;
    memoryTimer;
    // ── transaction batching ────────────────────────────────────────────────
    pending = [];
    flushScheduled = false;
    _closed = false;
    // ── subscribers ─────────────────────────────────────────────────────────
    subscribers = new Map();
    // ── cross-tab ───────────────────────────────────────────────────────────
    _channel;
    _localChange = false;
    constructor(opts = {}) {
        this.dbName = opts.dbName ?? 'the_wizard';
        this.storeName = opts.storeName ?? 'the_wizards_tardis';
        this._version = opts.version ?? 1;
        this._nodeId = opts.nodeId ?? Math.random().toString(36).slice(2, 10);
        this.ttlMs = opts.ttlMs ?? 30 * 60 * 1000;
        this._onFlush = opts.onFlush ?? ((fn) => queueMicrotask(fn));
        this._onError = opts.onError ?? reportError;
        if (typeof BroadcastChannel !== 'undefined') {
            this._channel = new BroadcastChannel(`sauron_idb_sync:${this.dbName}:${this.storeName}`);
            this._channel.onmessage = (e) => this._onRemote(e.data);
        }
        this._hlc = new HYBRID_LOGICAL_CLOCK_CRDT(this._nodeId);
    }
    // ───────────────────────────────────────────────────────────────────────
    // Migrations — call before first open. Each version gets one upgrader.
    // ───────────────────────────────────────────────────────────────────────
    defineMigration(version, fn) {
        this._migrations.set(version, fn);
        if (version > this._version)
            this._version = version;
        return this;
    }
    // ───────────────────────────────────────────────────────────────────────
    // Open — idempotent, single in-flight promise, runs all pending migrations
    // ───────────────────────────────────────────────────────────────────────
    open() {
        if (this._db)
            return Promise.resolve(this._db);
        if (this._opening)
            return this._opening;
        if (typeof indexedDB === 'undefined') {
            return Promise.reject(new Error('[INDEXED_DB] indexedDB is not defined in this environment'));
        }
        this._opening = new Promise((resolve, reject) => {
            const req = indexedDB.open(this.dbName, this._version);
            req.onupgradeneeded = (ev) => {
                const db = req.result;
                const oldVersion = ev.oldVersion;
                if (!db.objectStoreNames.contains(this.storeName)) {
                    db.createObjectStore(this.storeName);
                }
                // run migrations from oldVersion+1 .. this._version
                for (let v = oldVersion + 1; v <= this._version; v++) {
                    const fn = this._migrations.get(v);
                    if (fn) {
                        try {
                            fn(db, oldVersion);
                        }
                        catch (err) {
                            this._onError(err);
                            reject(err);
                            return;
                        }
                    }
                }
            };
            req.onsuccess = () => {
                this._db = req.result;
                this._opening = null;
                this._db.onclose = () => { this._db = null; };
                this._db.onversionchange = () => { this._db?.close(); this._db = null; };
                resolve(this._db);
            };
            req.onerror = () => { this._opening = null; reject(req.error); };
            req.onblocked = () => { };
        });
        return this._opening;
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
    scheduleFlush() {
        if (this.flushScheduled)
            return;
        this.flushScheduled = true;
        this._onFlush(() => this.flush());
    }
    async flush() {
        this.flushScheduled = false;
        if (this.pending.length === 0 || this._closed)
            return;
        const ops = this.pending;
        this.pending = [];
        let db;
        try {
            db = await this.open();
        }
        catch (err) {
            for (const op of ops)
                ('reject' in op && op.reject(err));
            return;
        }
        let tx;
        try {
            tx = db.transaction(this.storeName, 'readwrite');
        }
        catch (err) {
            for (const op of ops)
                ('reject' in op && op.reject(err));
            return;
        }
        const store = tx.objectStore(this.storeName);
        // Wire up all requests inside one tx. IDB keeps the tx alive as long
        // as there's at least one pending request, so scheduling them all
        // before returning is safe.
        for (const op of ops) {
            try {
                switch (op.kind) {
                    case 'get': {
                        const r = store.get(op.key);
                        r.onsuccess = () => {
                            const rec = r.result;
                            if (rec) {
                                this.memory.set(op.key, rec);
                                op.resolve(rec.value);
                            }
                            else
                                op.resolve(undefined);
                        };
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                    case 'put': {
                        const rec = {
                            value: op.value,
                            hlc: this._hlc.tick(),
                            nodeId: this._nodeId,
                            ts: Date.now(),
                        };
                        const r = store.put(rec, op.key);
                        r.onsuccess = () => {
                            this.memory.set(op.key, rec);
                            this._broadcastPut(op.key, rec);
                            this._emit(op.key, rec.value);
                            op.resolve();
                        };
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                    case 'del': {
                        const r = store.delete(op.key);
                        r.onsuccess = () => {
                            this.memory.delete(op.key);
                            this._broadcastDel(op.key);
                            this._emit(op.key, undefined);
                            op.resolve();
                        };
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                    case 'has': {
                        const r = store.getKey(op.key);
                        r.onsuccess = () => op.resolve(r.result !== undefined);
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                    case 'keys': {
                        const r = store.getAllKeys();
                        r.onsuccess = () => op.resolve(r.result);
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                    case 'clear': {
                        const r = store.clear();
                        r.onsuccess = () => {
                            this.memory.clear();
                            this._broadcastClear();
                            op.resolve();
                        };
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                    case 'cursor': {
                        const out = [];
                        const r = store.openCursor(op.range);
                        r.onsuccess = () => {
                            const c = r.result;
                            if (c) {
                                const rec = c.value;
                                out.push([c.key, rec.value]);
                                c.continue();
                            }
                            else {
                                op.resolve(out);
                            }
                        };
                        r.onerror = () => op.reject(r.error);
                        break;
                    }
                }
            }
            catch (err) {
                'reject' in op && op.reject(err);
            }
        }
    }
    // ───────────────────────────────────────────────────────────────────────
    // Core CRUD — all batched through the pending queue
    // ───────────────────────────────────────────────────────────────────────
    get(key) {
        const cached = this.memory.get(key);
        if (cached) {
            return MaybePromise(cached.value);
        }
        else {
            return MaybePromise(this._enqueueGet(key));
        }
    }
    getOrThrow(key) {
        const v = this.get(key);
        if (v === undefined)
            throw new Error(`[INDEXED_DB] key not found: ${key}`);
        else
            return MaybePromise(v);
    }
    has(key) {
        if (this.memory.has(key))
            return MaybePromise(true);
        else
            return MaybePromise(this._enqueueHas(key));
    }
    set(key, value) {
        // write-through memory so subsequent gets in the same tick see it
        const rec = {
            value,
            hlc: this._hlc.tick(),
            nodeId: this._nodeId,
            ts: Date.now(),
        };
        this.memory.set(key, rec);
        return MaybePromise(this._enqueuePut(key, value));
    }
    async setMany(entries) {
        await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)));
    }
    async update(key, updater) {
        const prev = await this.get(key);
        const next = updater(prev);
        await this.set(key, next);
        return MaybePromise(next);
    }
    delete(key) {
        this.memory.delete(key);
        return MaybePromise(this._enqueueDel(key));
    }
    async deleteMany(keys) {
        await Promise.all(keys.map((k) => this.delete(k)));
    }
    async clear(chain) {
        this.memory.clear();
        return MaybePromise(this._enqueueClear());
    }
    async keys(chain) {
        return MaybePromise(this._enqueueKeys());
    }
    // ───────────────────────────────────────────────────────────────────────
    // In-flight dedup: same key requested twice in a tick → one IDB get
    // ───────────────────────────────────────────────────────────────────────
    _enqueueGet(key) {
        const existing = this.inflight.get(key);
        if (existing)
            return existing;
        const p = new Promise((resolve, reject) => {
            this.pending.push({ kind: 'get', key, resolve: resolve, reject });
            this.scheduleFlush();
        }).finally(() => { this.inflight.delete(key); });
        this.inflight.set(key, p);
        return p;
    }
    _enqueuePut(key, value) {
        return new Promise((resolve, reject) => {
            this.pending.push({ kind: 'put', key, value, resolve, reject });
            this.scheduleFlush();
        });
    }
    _enqueueDel(key) {
        return new Promise((resolve, reject) => {
            this.pending.push({ kind: 'del', key, resolve, reject });
            this.scheduleFlush();
        });
    }
    _enqueueHas(key) {
        return new Promise((resolve, reject) => {
            this.pending.push({ kind: 'has', key, resolve, reject });
            this.scheduleFlush();
        });
    }
    _enqueueKeys() {
        return new Promise((resolve, reject) => {
            this.pending.push({ kind: 'keys', resolve, reject });
            this.scheduleFlush();
        });
    }
    _enqueueClear() {
        return new Promise((resolve, reject) => {
            this.pending.push({ kind: 'clear', resolve, reject });
            this.scheduleFlush();
        });
    }
    _enqueueCursor(range) {
        return new Promise((resolve, reject) => {
            this.pending.push({ kind: 'cursor', range, resolve, reject });
            this.scheduleFlush();
        });
    }
    // ───────────────────────────────────────────────────────────────────────
    // Pub/sub
    // ───────────────────────────────────────────────────────────────────────
    subscribe(key, cb, chain) {
        let set = this.subscribers.get(key);
        if (!set) {
            set = new Set();
            this.subscribers.set(key, set);
        }
        set.add(cb);
        const cached = this.memory.get(key);
        if (cached) {
            try {
                cb(cached.value);
            }
            catch (e) {
                this._onError(e);
            }
        }
        return () => {
            const s = this.subscribers.get(key);
            if (!s)
                return;
            s.delete(cb);
            if (s.size === 0)
                this.subscribers.delete(key);
        };
    }
    /** Selector-based subscription — only fires when the selected slice changes. */
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
        const set = this.subscribers.get(key);
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
    // ───────────────────────────────────────────────────────────────────────
    // waitFor — with waiter cleanup (no leak)
    // ───────────────────────────────────────────────────────────────────────
    waitFor(key, opts = {}) {
        const existing = this.memory.get(key);
        if (existing)
            return Promise.resolve(existing.value);
        return new Promise((resolve, reject) => {
            let timer;
            const unsub = this.subscribe(key, (v) => {
                if (v === undefined)
                    return;
                runTeardown(unsub);
                if (timer)
                    clearTimeout(timer);
                resolve(v);
            });
            if (opts.timeoutMs && opts.timeoutMs > 0) {
                timer = setTimeout(() => {
                    runTeardown(unsub);
                    reject(new Error(`[INDEXED_DB] waitFor(${key}) timed out after ${opts.timeoutMs}ms`));
                }, opts.timeoutMs);
            }
        });
    }
    // ───────────────────────────────────────────────────────────────────────
    // Cursor as async iterator — `for await (const [k, v] of db.entries())`
    // ───────────────────────────────────────────────────────────────────────
    entries(range) {
        let buf = null;
        let idx = 0;
        const self = this;
        return {
            [Symbol.asyncIterator]() { return this; },
            async next() {
                if (!buf) {
                    buf = await self._enqueueCursor(range);
                }
                if (idx >= buf.length)
                    return { done: true, value: undefined };
                return { done: false, value: buf[idx++] };
            },
        };
    }
    // ───────────────────────────────────────────────────────────────────────
    // Cross-tab sync via BroadcastChannel + HLC
    // ───────────────────────────────────────────────────────────────────────
    _broadcastPut(key, rec) {
        this._localChange = true;
        this._channel?.postMessage({ type: 'put', key, rec });
    }
    _broadcastDel(key) {
        this._localChange = true;
        this._channel?.postMessage({ type: 'del', key });
    }
    _broadcastClear() {
        this._localChange = true;
        this._channel?.postMessage({ type: 'clear' });
    }
    _onRemote(msg) {
        if (!msg || typeof msg !== 'object')
            return;
        switch (msg.type) {
            case 'put': {
                const remote = msg.rec;
                if (!remote?.hlc)
                    return;
                this._hlc.update(remote.hlc);
                const local = this.memory.get(msg.key);
                if (local && this._hlc.compare(local.hlc, remote.hlc) >= 0)
                    return; // local wins
                this.memory.set(msg.key, remote);
                this._emit(msg.key, remote.value);
                break;
            }
            case 'del': {
                this.memory.delete(msg.key);
                this._emit(msg.key, undefined);
                break;
            }
            case 'clear': {
                this.memory.clear();
                // don't re-emit every key; consumers should listen for clear
                this._emit('__clear__', undefined);
                break;
            }
        }
    }
    // ───────────────────────────────────────────────────────────────────────
    // Conflict policy — optional, mirrors your ConflictPolicy shape
    // ───────────────────────────────────────────────────────────────────────
    async merge(key, incoming, policy) {
        const local = await this.get(key);
        if (local === undefined)
            return this.set(key, incoming);
        const localRec = this.memory.get(key);
        const remoteHlc = this._hlc.tick();
        const resolved = policy(local, incoming, {
            key,
            localHlc: localRec.hlc,
            remoteHlc,
            nodeId: this._nodeId,
        });
        return this.set(key, resolved);
    }
    // ───────────────────────────────────────────────────────────────────────
    // Memory cache control
    // ───────────────────────────────────────────────────────────────────────
    touchMemory() {
        if (this.memoryTimer)
            clearTimeout(this.memoryTimer);
        this.memoryTimer = setTimeout(() => {
            this.memory.clear();
            this.memoryTimer = undefined;
        }, this.ttlMs);
    }
    /** Force memory to rehydrate from IDB on next read. */
    invalidateMemory(key) {
        if (key)
            this.memory.delete(key);
        else
            this.memory.clear();
    }
    _withLock(key, fn) {
        if (typeof navigator !== 'undefined' && navigator.locks) {
            return navigator.locks.request('mem_write_' + key, fn);
        }
        return fn();
    }
    // ───────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ───────────────────────────────────────────────────────────────────────
    async close() {
        this._closed = true;
        if (this.memoryTimer)
            clearTimeout(this.memoryTimer);
        this._channel?.close();
        if (this._db) {
            this._db.close();
            this._db = null;
        }
    }
    isOpen() { return !!this._db; }
    async info() {
        if (!this._db)
            await this.open();
        const tx = this._db.transaction(this.storeName, 'readonly');
        const count = await new Promise((resolve, reject) => {
            const r = tx.objectStore(this.storeName).count();
            r.onsuccess = () => resolve(r.result);
            r.onerror = () => reject(r.error);
        });
        return {
            isOpen: !!this._db,
            dbName: this.dbName,
            storeName: this.storeName,
            version: this._version,
            count,
        };
    }
}
