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
 

export class INDEXED_DB {
    readonly dbName: S
    readonly storeName: S
    private _version: N
    private _migrations = new Map<N, Migration>()
    private _nodeId: S
    private _hlc: HYBRID_LOGICAL_CLOCK_CRDT
    private _onFlush: (fn: () => V) => V
    private _onError: (err: U) => void
    // ── connection ──────────────────────────────────────────────────────────
    private _db: IDBDatabase | NU = null
    private _opening: P<IDBDatabase> | NU = null
    // ── memory cache (SWR) ──────────────────────────────────────────────────
    private memory = new O2<S, EntryRecord>()
    private inflight = new O2<S, P<A>>()
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
           
        this._hlc = new HYBRID_LOGICAL_CLOCK_CRDT(this._nodeId)
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
    public get<T = A>(key: A) {
        const cached = this.memory.get(key)
        if (cached) { return MaybePromise(cached.value as T) }
        else { return MaybePromise(this._enqueueGet<T>(key)) }
    }
    public getOrThrow<T = A>(key: A) {
        const v = this.get<T>(key)
        if (v === undefined) throw new Error(`[INDEXED_DB] key not found: ${key}`)
        else return MaybePromise(v)
    }
    public has(key: A) {
        if (this.memory.has(key)) return MaybePromise(true)
        else return MaybePromise(this._enqueueHas(key))
    }
    public set<T = A>(key: A, value: T) {
        // write-through memory so subsequent gets in the same tick see it
        const rec: EntryRecord<T> = {
            value,
            hlc: this._hlc.tick(),
            nodeId: this._nodeId,
            ts: Date.now(),
        }
        this.memory.set(key, rec)
        return MaybePromise(this._enqueuePut(key, value))
    }
    public async setMany(entries: Record<S, A>): P<V> {
        await Promise.all(Object.entries(entries).map(([k, v]) => this.set(k, v)))
    }
    public async update<T = A>(key: A, updater: (prev: T | UD) => T) {
        const prev = await this.get<T>(key)
        const next = updater(prev)
        await this.set(key, next)
        return MaybePromise(next)
    }
    public delete(key: A) {
        this.memory.delete(key)
        return MaybePromise(this._enqueueDel(key))
    }
    public async deleteMany(keys: A[]): P<V> {
        await Promise.all(keys.map((k) => this.delete(k)))
    }
    public async clear(chain?: CHAIN) {
        this.memory.clear()
        return MaybePromise(this._enqueueClear())
    }
    public async keys(chain?: CHAIN) {
        return MaybePromise(this._enqueueKeys())
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

