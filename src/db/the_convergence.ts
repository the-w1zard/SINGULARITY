//  ══════════════════════════════════════════════════════════════════════════════
//  ND                              THE PUBLIC API
//  One API for everything. The disruptor is ALWAYS the memory layer.
//  The only fork is persistence: INDEXED_DB in the client, BINARY kvdb on the
//  server — decided once, at boot, never per call.
//
//  Flow:
//    set → hub.set_soa (always, hot) + persist (fire-and-forget) → hot reads
//          come off the disruptor inside the TTL; a cold read falls through to
//          the store and rehydrates the disruptor with the remaining TTL.
//
//  Env detection: client = `indexedDB` + `window` present.
//  Everything else (Bun / Deno / node) is the server path.
//  ══════════════════════════════════════════════════════════════════════════════

import { hub } from './hadron_disruptor'
import { INDEXED_DB, BINARY } from './binary_kvdb'



export class THE_PUBLIC_DB {
    readonly dbName: S
    readonly storeName: S
    private _version: N
    private _migrations = new Map<N, Migration>()

    // BOTH backends, alive at the same time, built ONCE from constructor config
    private _idb: INDEXED_DB | NU = null
    private _bin: BINARY | NU = null
    private hub: typeof hub

    private _hlc: HYBRID_LOGICAL_CLOCK

    private _onError: (err: U) => void
    private _onFlush: (fn: () => V) => V
    private opts: DBOptions
    private ttlMs: N
    private ns: NS
    public prefix: S | NU
    private onError: NonNullable<DBOptions['onError']>
    private _opening: P<IDBDatabase> | NU = null
    private inflight = new Map<S, P<A>>()
    private memoryTimer?: ReturnType<typeof setTimeout>
    private pending: PendingOp[] = []
    private flushScheduled = false
    private _closed = false
    private subscribers = new Map<S, Set<Subscriber<A>>>()
    private _channel?: BroadcastChannel
    private _localChange = false
    private _nodeId: S


    // spec state
    private middlewares: NginMiddleware[] = []
    private ttlMap = new Map<S, N>()
    private locks = new Map<S, LockEntry>()
    private indexes = new Map<S, { config: IndexConfig; data: MP<S, Set<S>> }>()
    /** local mirror of hot keys so enumerate-style calls (export / query / watchPattern) work */
    private memoryKeys = new Set<S>()

    constructor(hub_: typeof hub, opts: DBOptions = {}) {
        this.hub = hub_
        this.dbName = opts.dbName ?? 'the_wizard'
        this.storeName = opts.storeName ?? 'the_wizards_tardis'
        this._version = opts.version ?? 1
        this._nodeId = opts.nodeId ?? Math.random().toString(36).slice(2, 10)
        this._onFlush = opts.onFlush ?? ((fn) => queueMicrotask(fn))
        this._onError = opts.onError ?? reportError
        this.opts = opts   
        
        this.ttlMs = opts.ttlMs ?? 15 * 60 * 1000
        this.ns = opts.namespace ?? DEFAULT_NS
        this.prefix = opts.prefix ?? null
        this.onError = opts.onError ?? ((err, op, key) =>
        console.error(`[THE_PUBLIC_DB] ${op} failed for ${key}:`, err))
    }

    // ── PER-CALL ENV DETECTION + ADAPTERS ──────────────────────────────────
    private env(): Env {
        return (typeof indexedDB !== 'undefined' && typeof document !== 'undefined') ? 'client' : 'server'
    }
    /** the CALL's adapter. both exist; this call only ever touches one. */
    private adapter(call: Env = this.env()): INDEXED_DB | BINARY {
        if (call === 'client') {
            if (!this._idb) {
                this._idb = new INDEXED_DB({
                    dbName: this.opts.client?.dbName ?? 'the_wizard',
                    storeName: this.opts.client?.storeName ?? 'the_wizards_tardis',
                    nodeId: this.opts.client?.nodeId,
                })
            }
            return this._idb
        }
        if (!this._bin) {
            this._bin = new BINARY()
            this._bin.init_kv_ngin(this.opts.server?.dir ?? './hadron_kv', this.opts.server?.memoryTtlMs ?? this.ttlMs)
        }
        return this._bin
    }
    public k(key: S): S { return this.prefix ? `${this.prefix}:${key}` : key }
    // ── middleware runner ──────────────────────────────────────────────────
    private async runBeforeGet(key: S) { for (const m of this.middlewares) { if (m.beforeGet) await m.beforeGet(key) } }
    private async runAfterGet(key: S, value: U) { for (const m of this.middlewares) { if (m.afterGet) await m.afterGet(key, value) } }
    private async runBeforeSet(key: S, value: U) { for (const m of this.middlewares) { if (m.beforeSet) await m.beforeSet(key, value) } }
    private async runAfterSet(key: S, value: U) { for (const m of this.middlewares) { if (m.afterSet) await m.afterSet(key, value) } }
    private async runBeforeDelete(key: S) { for (const m of this.middlewares) { if (m.beforeDelete) await m.beforeDelete(key) } }
    private async runAfterDelete(key: S) { for (const m of this.middlewares) { if (m.afterDelete) await m.afterDelete(key) } }
    public use(mw: NginMiddleware): V { this.middlewares.push(mw) }

    // ═══════════════════════════ MEMORY (disruptor) ═══════════════════════
    /** memory layer = the disruptor, always. soa namespaces (key, s, m, r). */
    private soa(key: S) { const { r, m, s } = this.ns; return { k: this.k(key), s, m, r } }

    public getMemory<T = A>(key: S, chain?: CHAIN): A | NU {
        const { k, s, m, r } = this.soa(key)
        void this.runBeforeGet(k)
        const v = this.hub.get_soa(k, s, m, r) ?? null
        void this.runAfterGet(k, v)
        return chain ? (v as A) : v
    }

    public async getManyMemory(keys: S[], chain?: CHAIN): P<A> {
        const out: Record<S, U> = {}
        for (const key of keys) {
            const v = this.getMemory(key)
            if (v !== null && v !== undefined) out[key] = v
        }
        return chain ? (out as A) : out
    }

    public setMemory(key: S, value: A, chain?: CHAIN): A {
        const { k, s, m, r } = this.soa(key)
        void this.runBeforeSet(k, value)
        this.hub.set_soa(value, k, s, m, r)
        this.memoryKeys.add(k)
        void this.runAfterSet(k, value)
        return chain ? (value as A) : value
    }

    public updateMemory(key: S, value: A, chain?: CHAIN): A | V {
        if (!this.hasMemory(key)) return
        return this.setMemory(key, value, chain)
    }

    public async setManyMemory(entries: Record<S, A>): P<V> {
        for (const [key, v] of Object.entries(entries)) this.setMemory(key, v)
    }

    public deleteMemory(key: S): V {
        const { k, s, m, r } = this.soa(key)
        void this.runBeforeDelete(k)
        this.hub.delete_soa(k, s, m, r)
        this.memoryKeys.delete(k)
        void this.runAfterDelete(k)
    }

    public deleteManyMemory(keys: S[]): V { for (const k of keys) this.deleteMemory(k) }

    public subMemory<T>(key: S, cb: (value: T) => V): () => V {
        const { k, s, m, r } = this.soa(key)
        const current = this.hub.get_soa(k, s, m, r)
        if (current !== undefined) cb(current as T)
        return this.hub.subscribe_soa((_pid: N, value: A) => cb(value as T), k, s, m, r)
    }

    public hasMemory(key: S): B {
        const { k, s, m, r } = this.soa(key)
        return this.hub.has_soa(k, s, m, r) || this.memoryKeys.has(k)
    }

    public getMemoryEntries(): { key: S; value: U }[] {
        const out: { key: S; value: U }[] = []
        for (const k of this.memoryKeys) {
            const { s, m, r } = this.ns
            const v = this.hub.get_soa(k, s, m, r)
            if (v !== undefined) out.push({ key: k, value: v })
        }
        return out
    }

    // ═══════════════════════════ GET ══════════════════════════════════════
    /**
     * get → hot off the disruptor; cold falls through to the CALL's adapter and
     * rehydrates the disruptor. binary=true reads the raw binary file path record.
     */
    public async get<T = A>(key: S, binary = false, chain?: CHAIN): P<A> {
        const k = this.k(key)
        await this.runBeforeGet(k)

        const hot = this.getMemory(k)
        if (hot !== null && hot !== undefined) {
            await this.runAfterGet(k, hot)
            return hot
        }

        const env = this.env()
        let v: U = null
        if (binary) {
            const raw = await this.adapterGet(env, k)
            const path = raw as S
            v = await this.readBinaryFile(path, env)
        } else {
            v = await this.adapterGet(env, k)
        }
        await this.runAfterGet(k, v)
        if (v !== null && v !== undefined) {
            this.setMemory(k, v as A)
            this.memoryKeys.add(k)
        }
        return chain ? v : v
    }

    public async getOrThrow<T = A>(key: S, binary = false, chain?: CHAIN): P<T> {
        const v = await this.get<T>(key, binary, chain)
        if (v === null || v === undefined) throw new Error(`[THE_PUBLIC_DB] key not found: ${this.k(key)}`)
        return v
    }

    public async getMany(keys: S[], binary = false): P<A> {
        const out: Record<S, U> = {}
        for (const k of keys) {
            const v = await this.get(k, binary)
            if (v !== null && v !== undefined) out[k] = v
        }
        return out
    }

    // ═══════════════════════════ SET ══════════════════════════════════════
    /**
     * set → disruptor ALWAYS takes the value (hot), then the CALL's adapter
     * persists it. Errors never break the set — they land in onError.
     */
    public async set<T = A>(key: S, value: T, binary = false, chain?: CHAIN): P<V> {
        const k = this.k(key)
        await this.runBeforeSet(k, value)

        // disruptor takes it FIRST — always, both envs
        this.setMemory(k, value as A)

        const env = this.env()
        try {
            if (binary) {
                const path = `${(this.adapter(env) as BINARY).storage_path}/${k}`
                await this.writeBinaryFile(path, value as U, env)
                await this.adapterSet(env, k, path)
            } else {
                await this.adapterSet(env, k, value)
            }
        } catch (err) {
            this.onError(err, 'put', k)
        }

        await this.runAfterSet(k, value)
        return value
    }

    public async setMany(entries: Record<S, A>, binary = false): P<V> {
        for (const [k, v] of Object.entries(entries)) await this.set(k, v as A, binary)
    }

    public async update<T = A>(key: S, updater: (prev: T | UD) => T, chain?: CHAIN): P<T | V> {
        const prev = await this.get<T>(key)
        const next = updater(prev as T | UD)
        await this.set(key, next)
        return chain ? next : next
    }

    // ═══════════════════════════ DELETE ═══════════════════════════════════
    public async delete(key: S, binary = false): P<V> {
        const k = this.k(key)
        await this.runBeforeDelete(k)
        const env = this.env()
        try {
            if (binary) {
                const raw = await this.adapterGet(env, k)
                const path = raw as S
                await this.deleteBinaryFile(path, env)
            }
            await this.adapterDelete(env, k)
        } catch (err) {
            this.onError(err, 'del', k)
        }
        this.deleteMemory(k)
        await this.runAfterDelete(k)
    }

    public async deleteMany(keys: S[], binary = false): P<V> { for (const k of keys) await this.delete(k, binary) }

    public async has(key: S, chain?: CHAIN): P<B> {
        const k = this.k(key)
        if (this.hasMemory(k)) return true
        const env = this.env()
        const v = await this.adapterGet(env, k)
        return chain ? (v !== null && v !== undefined) : (v !== null && v !== undefined)
    }

    // ── adapter primitives (per-call env) ──────────────────────────────────
    private adapterGet(env: Env, key: S): P<U> {
        const a = this.adapter(env)
        return env === 'client' ? (a as INDEXED_DB).get<U>(key) : (a as BINARY).kv_ngin.get<U>(key)
    }
    private adapterSet(env: Env, key: S, value: U): P<V> {
        const a = this.adapter(env)
        return env === 'client'
            ? (a as INDEXED_DB).set(key, value)
            : (a as BINARY).kv_ngin.set(key, value, this.ttlMs)
    }
    private adapterDelete(env: Env, key: S): P<V> {
        const a = this.adapter(env)
        return env === 'client' ? (a as INDEXED_DB).delete(key) : (a as BINARY).kv_ngin.delete(key)
    }

    /** enumerate the CALL's adapter keys. client: IDB keys(). server: scan the .bin dir. */
    private async adapterKeys(env: Env): P<S[]> {
        const a = this.adapter(env)
        if (env === 'client') return (a as INDEXED_DB).keys()
        const fs = await this.fsModule()
        if (!fs) return []
        try {
            return (fs.readdirSync(this._bin!.storage_path) as S[])
                .filter((f: S) => f.endsWith('.bin'))
                .map((f: S) => f.slice(0, -4))
        } catch { return [] }
    }

    private async adapterClear(env: Env): P<V> {
        const a = this.adapter(env)
        if (env === 'client') return (a as INDEXED_DB).clear()
        const keys = await this.adapterKeys(env)
        for (const k of keys) await (a as BINARY).kv_ngin.delete(k)
    }

    // ── binary file helpers (server path) ──────────────────────────────────
    private async writeBinaryFile(path: S, value: U, env: Env): P<V> {
        if (env !== 'server') throw new Error('[THE_PUBLIC_DB] binary file write is server-only')
        const fs = await this.fsModule()
        const encoded = this._bin!.encode_msg(value as A)
        fs.writeFileSync(path, Buffer.from(encoded))
    }
    private async readBinaryFile(path: S, env: Env): P<U> {
        if (env !== 'server') return null
        const fs = await this.fsModule()
        try {
            const bytes = fs.readFileSync(path)
            return this._bin!.decode_msg(new Uint8Array(bytes))
        } catch { return null }
    }
    private async deleteBinaryFile(path: S, env: Env): P<V> {
        if (env !== 'server') return
        const fs = await this.fsModule()
        try { fs.unlinkSync(path) } catch { }
    }
    private fsModule(): P<A> {
        return new Promise((resolve) => {
            // node/Bun/Deno all resolve fs; browser never reaches here (binary=true is server-only)
            import('fs' as S).then(resolve).catch(() => resolve(null))
        })
    }

    // ═══════════════════════ WATCH ════════════════════════════════════════
    /** subscriptions live on the disruptor — one path, client or server. */
    public subscribe<T = A>(key: S, listener: (value: T) => V): Teardown {
        return this.subMemory<T>(key, listener)
    }

    public async watchMany(keys: S[], handler: (value: A) => V): P<Teardown> {
        const uns: Teardown[] = keys.map((k) => this.subscribe(k, handler))
        return () => { for (const un of uns) runTeardown(un) }
    }

    public async watchPattern(pattern: S, handler: (value: A) => V): P<Teardown> {
        // glob-ish: '*' matches any chars
        const rx = new RegExp('^' + pattern.replace(/[.*+?^${}()|[\]\\]/g, (c) => (c === '*' ? '.*' : '\\' + c)) + '$')
        const env = this.env()
        const keys = (await this.adapterKeys(env)).filter((k) => rx.test(k))
        const uns: Teardown[] = keys.map((k) => this.subscribe(k, handler))
        void (async () => {
            for (const k of keys) {
                const v = await this.get(k)
                if (v !== null) handler(v)
            }
        })()
        return () => { for (const un of uns) runTeardown(un) }
    }

    public async watchPrefix(prefix: S, handler: (value: A) => V): P<Teardown> { return this.watchPattern(`${prefix}*`, handler) }
    public async watchAll(handler: (value: A) => V): P<Teardown> { return this.watchPattern('*', handler) }

    /** resolves when a set lands (or the value is already hot). */
    public waitFor<T = A>(key: S, chain?: CHAIN): P<T> {
        const k = this.k(key)
        const hot = this.hub.get_soa(k, this.ns.s, this.ns.m, this.ns.r)
        if (hot !== undefined) return P.resolve(hot as T)
        return new Promise<T>((resolve) => {
            this.subscribe<T>(k, (v) => resolve(v))
        })
    }

    // ═══════════════════════ SET IF CHANGED ═══════════════════════════════
    public async setIfChanged(key: S, value: A): P<B> {
        const current = await this.get(key)
        if (this.ser(current) === this.ser(value)) return false
        await this.set(key, value)
        return true
    }

    // ═══════════════════════ TTL ══════════════════════════════════════════
    public async expire(key: S, ms: N): P<V> {
        const k = this.k(key)
        const expiresAt = Date.now() + ms
        this.ttlMap.set(k, expiresAt)
        setTimeout(async () => {
            const ts = this.ttlMap.get(k)
            if (ts && ts <= Date.now()) {
                await this.delete(k)
                this.ttlMap.delete(k)
            }
        }, ms + 5)
    }
    public async expireAt(key: S, timestamp: N): P<V> {
        const ms = Math.max(0, timestamp - Date.now())
        await this.expire(key, ms)
    }
    public async persist(key: S): P<V> { this.ttlMap.delete(this.k(key)) }
    public async touch(key: S, ms?: N): P<V> {
        const k = this.k(key)
        if (!this.ttlMap.has(k)) return
        const expiresAt = this.ttlMap.get(k)!
        await this.expire(k, ms ?? expiresAt - Date.now())
    }

    // ═══════════════════════ LOCKS ════════════════════════════════════════
    public async lock(key: S, ttlMs = 10000): P<S | NU> {
        const k = this.k(key)
        const now = Date.now()
        const existing = this.locks.get(k)
        if (existing && existing.expiresAt > now) return null
        const owner = `${k}:${now}:${Math.random().toString(36).slice(2)}`
        this.locks.set(k, { owner, expiresAt: now + ttlMs })
        setTimeout(() => {
            const l = this.locks.get(k)
            if (l && l.owner === owner && l.expiresAt <= Date.now()) this.locks.delete(k)
        }, ttlMs + 5)
        return owner
    }
    public async unlock(key: S, owner: S): P<B> {
        const k = this.k(key)
        const l = this.locks.get(k)
        if (!l || l.owner !== owner) return false
        this.locks.delete(k)
        return true
    }
    public async renewLock(key: S, owner: S, ttlMs = 10000): P<B> {
        const k = this.k(key)
        const l = this.locks.get(k)
        if (!l || l.owner !== owner) return false
        l.expiresAt = Date.now() + ttlMs
        this.locks.set(k, l)
        return true
    }
    public lockApi = {
        acquire: (key: S, ttlMs?: N) => this.lock(key, ttlMs),
        release: (key: S, owner: S) => this.unlock(key, owner),
        renew: (key: S, owner: S, ttlMs?: N) => this.renewLock(key, owner, ttlMs),
    }

    // ═══════════════════════ TRANSACTION / BATCH ══════════════════════════
    public async transaction(fn: (tx: THE_PUBLIC_DB) => P<V> | V): P<V> { await fn(this) }
    public async batch(entries: { key: S; value: U }[]): P<V> {
        for (const { key, value } of entries) await this.set(key, value as A)
    }

    // ═══════════════════════ LIVE ═════════════════════════════════════════
    public live<T extends Record<S, U>>(key: S, initial: T = {} as T): T {
        const k = this.k(key)
        let current: T = { ...initial }
        this.subscribe<A>(k, (incoming) => {
            if (incoming && typeof incoming === 'object') current = { ...current, ...(incoming as Partial<T>) }
        })
        const persist = () => this.set(key, current as A)

        const proxy = new Proxy(current, {
            set: (target, prop, value) => {
                target[prop as keyof T] = value
                current = { ...target }
                persist().catch(console.error)
                return true
            },
            get: (target, prop) => target[prop as keyof T],
        }) as T

        void (async () => {
            const existing = await this.get(key)
            if (existing && typeof existing === 'object') Object.assign(proxy, existing)
            else await this.set(key, proxy as A)
        })().catch(console.error)
        return proxy
    }
    public useLive() { }

    // ═══════════════════════ COMPUTED ═════════════════════════════════════
    public computed<T = A>(key: S, deps: S[], computeFn: (...values: A[]) => T): V {
        const update = async () => {
            const values = await Promise.all(deps.map((d) => this.get(d)))
            const result = computeFn(...(values as A[]))
            await this.set(key, result as A)
        }
        deps.forEach((d) => {
            this.subscribe(this.k(d), () => { update().catch(console.error) })
        })
        update().catch(console.error)
    }

    // ═══════════════════════ SCOPES ═══════════════════════════════════════
    public scope(prefix: S): THE_PUBLIC_DB {
        const p = this.prefix ? `${this.prefix}:${prefix}` : prefix
        return new THE_PUBLIC_DB(this.hub, { ...this.opts, prefix: p, namespace: this.ns })
    }
    public scoped(workspaceName: S): THE_PUBLIC_DB { return this.scope(`workspace:${workspaceName}`) }

    // ═══════════════════════ SNAPSHOT / RESTORE ═══════════════════════════
    public async snapshot(): P<Snapshot> {
        const memory: Record<S, U> = {}
        for (const { key, value } of this.getMemoryEntries()) memory[key] = value
        const env = this.env()
        const persistent: Record<S, U> = {}
        const keys = await this.adapterKeys(env)
        for (const k of keys) persistent[k] = await this.adapterGet(env, k)
        return { memory, persistent }
    }

    public async restore(snapshot: Snapshot): P<V> {
        this.memoryKeys.clear()
        const env = this.env()
        await this.adapterClear(env)
        for (const [k, v] of Object.entries(snapshot.persistent)) {
            if (v !== null && v !== undefined) await this.adapterSet(env, k, v)
        }
        for (const [k, v] of Object.entries(snapshot.memory)) {
            this.setMemory(k, v as A)
            this.memoryKeys.add(k)
        }
    }

    // ═══════════════════════ EXPORT / IMPORT ══════════════════════════════
    public async export(prefix?: S): P<Record<S, U>> {
        const out: Record<S, U> = {}
        const keys = await this.adapterKeys(this.env())
        for (const k of keys) {
            if (prefix && !k.startsWith(prefix)) continue
            const v = await this.get(k)
            out[k] = v
        }
        return out
    }

    public async import(data: Record<S, U>, strategy: ExportStrategy = 'merge'): P<V> {
        for (const [k, v] of Object.entries(data)) {
            const existing = await this.get(k)
            if (strategy === 'skip' && existing !== null && existing !== undefined) continue
            if (strategy === 'merge' && existing && typeof existing === 'object' && typeof v === 'object') {
                await this.set(k, { ...(existing as A), ...(v as A) })
            } else {
                await this.set(k, v as A)
            }
        }
    }

    // ═══════════════════════ INDEXING ═════════════════════════════════════
    public index = {
        create: async (name: S, config: IndexConfig): P<V> => {
            const data = new Map<S, Set<S>>()
            const keys = await this.adapterKeys(this.env())
            for (const k of keys) {
                const v = await this.get(k)
                if (!v || typeof v !== 'object') continue
                for (const idxKey of config.keys) {
                    const val = (v as A)[idxKey]
                    const sk = `${idxKey}:${this.ser(val)}`
                    if (!data.has(sk)) data.set(sk, new Set())
                    data.get(sk)!.add(k)
                }
            }
            this.indexes.set(name, { config, data })
        },
        query: async (name: S, filter: IndexFilter): P<Record<S, U>[]> => {
            const idx = this.indexes.get(name)
            if (!idx) return []
            let resultKeys: Set<S> | NU = null
            for (const [k, v] of Object.entries(filter)) {
                const sk = `${k}:${this.ser(v)}`
                const set = idx.data.get(sk) ?? new Set<S>()
                if (resultKeys === null) resultKeys = new Set(set)
                else {
                    const next = new Set<S>()
                    for (const key of resultKeys) if (set.has(key)) next.add(key)
                    resultKeys = next
                }
            }
            const out: Record<S, U>[] = []
            if (!resultKeys) return out
            for (const k of resultKeys) out.push({ key: k, value: await this.get(k) })
            return out
        },
    }

    // ═══════════════════════ QUERY ════════════════════════════════════════
    public async query(expr: QueryExpr): P<Record<S, U>[]> {
        const keys = await this.adapterKeys(this.env())
        const out: Record<S, U>[] = []
        for (const k of keys) {
            const v = await this.get(k)
            if (this.matchesQuery(v, expr)) out.push({ key: k, value: v })
        }
        return out
    }
    public matchesQuery(v: A, expr: QueryExpr): B {
        if (!expr) return true
        if (expr.type === 'eq') return !!(v && typeof v === 'object' && this.ser((v as A)[expr.key]) === this.ser(expr.value))
        if (expr.type === 'and') return expr.exprs.every((e) => this.matchesQuery(v, e))
        if (expr.type === 'or') return expr.exprs.some((e) => this.matchesQuery(v, e))
        return false
    }

    // ═══════════════════════ SER / DESER ══════════════════════════════════
    public ser(value: U): S {
        if (value instanceof Map) return JSON.stringify({ __type: 'Map', value: Array.from(value.entries()).map(([key, val]) => [this.ser(key), this.ser(val)]) })
        if (value instanceof Set) return JSON.stringify({ __type: 'Set', value: Array.from(value.values()).map((v) => this.ser(v)) })
        return typeof value === 'string' ? value : JSON.stringify(value)
    }
    public deser(raw: S | NU): A {
        if (raw === null) return null
        try {
            const parsed = JSON.parse(raw)
            if (parsed?.__type === 'Map') return new Map(parsed.value.map(([key, value]: [s, s]) => [this.deser(key), this.deser(value)]))
            if (parsed?.__type === 'Set') return new Set(parsed.value.map((value: s) => this.deser(value)))
            return parsed
        } catch { return raw }
    }

    // ═══════════════════════ ATOMIC ═══════════════════════════════════════
    public atomic = {
        compareAndSwap: async <T = A>(key: S, expected: T | NU, next: T): P<B> => {
            const current = await this.get(key)
            if (this.ser(current) !== this.ser(expected)) return false
            await this.set(key, next as A)
            return true
        },
        mutate: async <T = A>(key: S, fn: (current: T | NU) => T): P<T> => {
            const current = await this.get<T>(key)
            const next = fn(current as T | NU)
            await this.set(key, next as A)
            return next
        },
        increment: async (key: S, by: N = 1): P<N> => {
            const current = await this.get(key)
            const n = typeof current === 'number' ? current : parseFloat(String(current ?? '0')) || 0
            const next = n + by
            await this.set(key, next as A)
            return next
        },
        append: async (key: S, value: A): P<A> => {
            const current = await this.get(key)
            if (Array.isArray(current)) { const next = [...current, value]; await this.set(key, next as A); return next }
            if (typeof current === 'string') { const next = current + String(value); await this.set(key, next as A); return next }
            const next = [current, value]
            await this.set(key, next as A)
            return next
        },
    }

    // ═══════════════════════ KEY LIST / META ══════════════════════════════
    public async keys(): P<S[]> { return this.adapterKeys(this.env()) }
    public get environment(): Env { return this.env() }
    public backendName(call: Env = this.env()): S { return call === 'client' ? 'indexed_db' : 'binary_kvdb' }
    public isOpen(): B {
        const env = this.env()
        return env === 'client' ? (this._idb as INDEXED_DB)?.isOpen() ?? false : !!this._bin
    }
    public async close(): P<V> {
        const env = this.env()
        if (env === 'client') (this._idb as INDEXED_DB | NU)?.close?.()
        // server: kv files are written synchronously per-op — nothing to flush
    }
}
 



export type DBOptions = {
    /** client-side IDB config — built lazily, only when a client call arrives */
    client?: { dbName?: S; storeName?: S ; nodeId?: S }
    /** server-side kvdb config — built lazily, only when a server call arrives */
    server?: { dir?: S; memoryTtlMs?: N; }
    /** disruptor hot value lifetime */
    ttlMs?: N
    namespace?: NS
    prefix?: S | NU
    /** persist errors never break a set() — they land here (default: console.error) */
    onError?: (err: U, op: 'put' | 'del', key: S) => V
}
export type NS = { r: S; m: S; s: S }   // rt / ms / sl namespace tuple

const DEFAULT_NS: NS = { r: 'root', m: 'default', s: 'default' }
const DEFAULT_TTL_MS = 15 * 60 * 1000

const IS_CLIENT: B =    typeof indexedDB !== 'undefined' &&    typeof window !== 'undefined' &&    typeof document !== 'undefined'
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

export const DB = new THE_PUBLIC_DB(hub, {
    client: { dbName: 'the_wizard', storeName: 'the_wizards_tardis' },
    server: { dir: './hadron_kv' },
})
