/* SKYWALKER.ts
 * Object-backed, graph-free reactive store.
 * - No dependency graph.
 * - No Map/Set in store hot paths.
 * - Direct key -> listener / DOM binding notification.
 * - Microtask-batched flush; sync flush available.
 * - Cross-tab sync through an injectable transport (BroadcastChannel by default).
 */

'use strict';

type S = string;
type Any = any;
type Fn = (...args: Any[]) => Any;

export type ChangeOp = 'set' | 'del' | 'clear';
export interface ChangeEvent {
    op: ChangeOp;
    key: S;
    value: Any;
    previous: Any;
    remote?: boolean;
}

export type Unsubscribe = () => void;
export type Listener<T = Any> = (value: T, previous: Any, event: ChangeEvent) => void;
export type GlobalListener = (event: ChangeEvent) => void;

export interface Signal<T = Any> {
    (): T;
    (value: T): void;
    readonly key: S;
    get value(): T;
    get current(): T;
    get crt(): T;
    set value(v: T);
    set current(v: T);
    set crt(v: T);
    set set(v: T);
    peek(): T;
    has(): boolean;
    delete(): boolean;
    sub(fn: Listener<T>): Unsubscribe;
}

export interface Atom<T = Any> {
    readonly key: S;
    get value(): T;
    get current(): T;
    get crt(): T;
    set value(v: T);
    set current(v: T);
    set crt(v: T);
    set set(v: T);
    peek(): T;
    has(): boolean;
    delete(): boolean;
    sub(fn: Listener<T>): Unsubscribe;
}

export interface DOMBinding<T = Any> {
    node: Node;
    write(value: T, node: Node): void;
}

export interface BroadcastMessage {
    op: ChangeOp;
    key: S;
    value: Any;
    previous: Any;
    origin: S;
}

export interface SauronSignalsOptions {
    /** Cross-tab channel name. Omit to disable automatic cross-tab sync. */
    broadcastChannel?: S | false;
    /** Custom transport; receives outbound messages and returns an inbound sink. */
    transport?: {
        send(msg: BroadcastMessage): void;
        receive(fn: (msg: BroadcastMessage) => void): Unsubscribe;
    };
    /** Storage adapter for persist()/unpersist(). Defaults to in-memory only. */
    storage?: {
        get(key: S): Any;
        set(key: S, value: Any): void;
        delete(key: S): void;
        clear(): void;
    };
}

const NOOP: Fn = () => {};
const hasOwn = Object.prototype.hasOwnProperty;

/** Stable, allocation-cheap id generator. */
let UID = 0;
const uid = (prefix: S): S => prefix + (++UID).toString(36);

function safe<T>(fn: () => T, fallback?: T): T {
    try {
        return fn();
    } catch (error) {
        if (typeof console !== 'undefined') console.error('[SAURON_SIGNALS]', error);
        return fallback as T;
    }
}

export class SKYWALKER {
    /* ------------------------------------------------------------------ *
     * Object-only state
     * ------------------------------------------------------------------ */

    /** Primary values. Prototype-less object: fastest property access. */
    protected _store: Record<S, Any> = Object.create(null);

    /** key -> subscriptionId -> listener */
    protected _listeners: Record<S, Record<S, Listener>> = Object.create(null);

    /** subscriptionId -> global listener */
    protected _globalListeners: Record<S, GlobalListener> = Object.create(null);

    /** key -> waiterId -> resolver */
    protected _waiters: Record<S, Record<S, (value: Any) => void>> = Object.create(null);

    /** key -> bindingId -> DOM binding */
    protected _bindings: Record<S, Record<S, DOMBinding>> = Object.create(null);

    /** Explicit persistence registry. */
    protected _persistKeys: Record<S, true> = Object.create(null);

    /** Persistent mirror. */
    protected _persistent: Record<S, Any> = Object.create(null);

    /** Pending notification keys for the current microtask flush. */
    protected _pending: Record<S, true> = Object.create(null);
    protected _pendingCount = 0;
    protected _flushScheduled = false;

    /** Batch depth. */
    protected _batchDepth = 0;

    /** Open async batch scopes. */
    protected _openScopes = 0;

    protected _channel: BroadcastChannel | null = null;
    protected _transport: SauronSignalsOptions['transport'];
    protected _storage: NonNullable<SauronSignalsOptions['storage']>;
    protected _origin = uid('origin:');

    /* ------------------------------------------------------------------ *
     * Constructor-created hot paths
     * ------------------------------------------------------------------ */

    /** Direct store read. No lookup indirection beyond one property access. */
    public hotGet: <T = Any>(key: S) => T | undefined;

    /** Direct store write + notify. */
    public hotSet: <T = Any>(key: S, value: T) => T;

    /** Direct listener registration. */
    public hotSub: <T = Any>(key: S, listener: Listener<T>) => Unsubscribe;

    /** Direct stream send. */
    public hotSend: <T = Any, P = Any>(payload: P, key: S) => T | undefined;

    constructor(public readonly name: S = 'SAURON_SIGNALS', options: SauronSignalsOptions = {}) {
        this._storage = options.storage ?? {
            get: () => undefined,
            set: NOOP,
            delete: NOOP,
            clear: NOOP,
        };

        this.hotGet = (key) => this._store[key];

        this.hotSet = (key, value) => {
            const previous = this._store[key];
            if (Object.is(previous, value)) return value;

            this._store[key] = value;

            if (this._persistKeys[key]) {
                this._persistent[key] = value;
                this._storage.set(key, value);
            }

            this._emit(key, value, previous, 'set', false);
            return value;
        };

        this.hotSub = (key, listener) => {
            const id = uid('s:');
            const bucket = (this._listeners[key] ??= Object.create(null));
            bucket[id] = listener;
            return () => {
                const current = this._listeners[key];
                if (!current || current[id] !== listener) return;
                delete current[id];
                if (this._isEmpty(current)) delete this._listeners[key];
            };
        };

        this.hotSend = (payload, key) => {
            const handler = this._channels[key]?.handler;
            return handler ? handler(payload, {
                r: 'THE_WIZARD_ROOT_REG',
                m: 'SAURON_MASTER_REG',
                s: this.name,
                k: key,
                p: payload,
            }) : undefined;
        };

        this._transport = options.transport ?? this._createBroadcastTransport(options.broadcastChannel);
        this._transport?.receive((message) => this._receiveRemote(message));
    }

    /* ------------------------------------------------------------------ *
     * Core reads / writes
     * ------------------------------------------------------------------ */

    public get<T = Any>(key: S): T | undefined {
        return this._store[key] as T | undefined;
    }

    public getMany<T = Any>(keys: readonly S[]): Record<S, T | undefined> {
        const out: Record<S, T | undefined> = Object.create(null);
        const store = this._store;
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            out[key] = store[key];
        }
        return out;
    }

    public set<T = Any>(key: S, value: T | ((previous: Any) => T)): T {
        const previous = this._store[key];
        const next = typeof value === 'function'
            ? (value as (previous: Any) => T)(previous)
            : value;

        return this.hotSet(key, next);
    }

    public setMany(entries: Record<S, Any>): Record<S, Any> {
        const out: Record<S, Any> = Object.create(null);
        for (const key in entries) {
            if (!hasOwn.call(entries, key)) continue;
            out[key] = this.set(key, entries[key]);
        }
        return out;
    }

    public has(key: S): boolean {
        return hasOwn.call(this._store, key);
    }

    public peek<T = Any>(key: S): T | undefined {
        return this._store[key] as T | undefined;
    }

    public read<T = Any>(key: S): T | undefined {
        return this._store[key] as T | undefined;
    }

    public write<T = Any>(key: S, value: T): T {
        return this.set(key, value);
    }

    public create<T = Any>(key: S, value: T): T {
        if (this.has(key)) return this._store[key] as T;
        return this.set(key, value);
    }

    public swap<T = Any>(key: S, value: T | ((previous: Any) => T)): T | undefined {
        const previous = this._store[key];
        this.set(key, value);
        return previous as T | undefined;
    }

    /** In-place edit. Avoids an extra read when callers already hold the value. */
    public edit<T = Any>(key: S, value: T | ((previous: Any) => T)): T {
        return this.set(key, value);
    }

    public delete(key: S): boolean {
        if (!hasOwn.call(this._store, key)) return false;

        const previous = this._store[key];
        delete this._store[key];

        if (this._persistKeys[key]) {
            delete this._persistKeys[key];
            delete this._persistent[key];
            this._storage.delete(key);
        }

        this._emit(key, undefined, previous, 'del', false);
        return true;
    }

    public deleteMany(keys: readonly S[]): Record<S, boolean> {
        const out: Record<S, boolean> = Object.create(null);
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            out[key] = this.delete(key);
        }
        return out;
    }

    public clear(): void {
        const keys = Object.keys(this._store);

        this._store = Object.create(null);
        this._persistKeys = Object.create(null);
        this._persistent = Object.create(null);
        this._storage.clear();

        for (let i = 0; i < keys.length; i++) {
            this._emit(keys[i], undefined, undefined, 'clear', false);
        }

        this._waiters = Object.create(null);
    }

    public entries<T = Any>(): [S, T][] {
        const store = this._store;
        const keys = Object.keys(store);
        const out: [S, T][] = [];

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            out.push([key, store[key] as T]);
        }

        return out;
    }

    public getAll<T = Any>(): Record<S, T> {
        return { ...this._store } as Record<S, T>;
    }

    /**
     * Prefix lookup scans owned keys. This avoids a trie/graph and remains
     * predictable; add a domain-specific bucket index only if a prefix family
     * becomes a measured hot path.
     */
    public getByPrefix<T = Any>(prefix: S): Record<S, T> {
        const out: Record<S, T> = Object.create(null);
        const store = this._store;
        const keys = Object.keys(store);

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            if (key.startsWith(prefix)) out[key] = store[key] as T;
        }

        return out;
    }

    /* ------------------------------------------------------------------ *
     * Subscriptions
     * ------------------------------------------------------------------ */

    public sub<T = Any>(key: S, listener: Listener<T>): Unsubscribe {
        return this.hotSub(key, listener);
    }

    public subMany<T = Any>(keys: readonly S[], listener: Listener<T>): Unsubscribe {
        const offs: Unsubscribe[] = [];

        for (let i = 0; i < keys.length; i++) {
            offs.push(this.sub(keys[i], listener));
        }

        return () => {
            for (let i = 0; i < offs.length; i++) offs[i]();
        };
    }

    public subAll(listener: GlobalListener): Unsubscribe {
        const id = uid('g:');
        this._globalListeners[id] = listener;

        return () => {
            if (this._globalListeners[id] === listener) delete this._globalListeners[id];
        };
    }

    public waitFor<T = Any>(key: S): Promise<T> {
        const existing = this._store[key];
        if (existing !== undefined) return Promise.resolve(existing as T);

        return new Promise<T>((resolve) => {
            const id = uid('w:');
            const bucket = (this._waiters[key] ??= Object.create(null));
            bucket[id] = resolve as (value: Any) => void;
        });
    }

    /** Manual notification. Useful after external mutation or hydration. */
    public _emit(
        key: S,
        value: Any,
        previous: Any,
        op: ChangeOp = 'set',
        remote = false,
    ): void {
        const event: ChangeEvent = { op, key, value, previous, remote };

        const listeners = this._listeners[key];
        if (listeners) {
            for (const id in listeners) {
                safe(() => listeners[id](value, previous, event));
            }
        }

        const globalListeners = this._globalListeners;
        for (const id in globalListeners) {
            safe(() => globalListeners[id](event));
        }

        const bindings = this._bindings[key];
        if (bindings) {
            for (const id in bindings) {
                const binding = bindings[id];
                if (binding) safe(() => binding.write(value, binding.node));
            }
        }

        const waiters = this._waiters[key];
        if (waiters) {
            for (const id in waiters) {
                const resolve = waiters[id];
                delete waiters[id];
                safe(() => resolve(value));
            }
            delete this._waiters[key];
        }
    }

    /* ------------------------------------------------------------------ *
     * Batching
     * ------------------------------------------------------------------ */

    public batch<T>(fn: () => T): T {
        this._batchDepth++;

        try {
            return fn();
        } finally {
            this._batchDepth--;
            if (this._batchDepth === 0 && this._openScopes === 0) this.flushSync();
        }
    }

    public openBatch(): symbol {
        this._openScopes++;
        return Symbol('sauron-batch');
    }

    public closeBatch(scope: symbol): void {
        this._openScopes--;
        if (this._openScopes <= 0) {
            this._openScopes = 0;
            if (this._pendingCount > 0) this._scheduleFlush();
        }
    }

    public isBatching(): boolean {
        return this._batchDepth > 0 || this._openScopes > 0;
    }

    public flush(): void {
        this.flushSync();
    }

    public flushSync(): void {
        if (this._openScopes > 0) return;

        this._flushScheduled = false;

        if (this._pendingCount === 0) return;

        const pending = this._pending;
        this._pending = Object.create(null);
        this._pendingCount = 0;

        for (const key in pending) {
            const value = this._store[key];
            const previous = value;

            this._emit(key, value, previous, 'set', false);
        }
    }

    protected _scheduleFlush(): void {
        if (this._flushScheduled) return;
        this._flushScheduled = true;
        queueMicrotask(() => this.flushSync());
    }

    /* ------------------------------------------------------------------ *
     * DOM injection
     * ------------------------------------------------------------------ */

    public dom<T = Any>(
        key: S,
        node: Node,
        write?: (value: T, node: Node) => void,
    ): Unsubscribe {
        const id = uid('b:');
        const binding: DOMBinding<T> = {
            node,
            write: write ?? ((value, target) => {
                (target as HTMLElement).textContent = value == null ? '' : String(value);
            }),
        };

        const bucket = (this._bindings[key] ??= Object.create(null));
        bucket[id] = binding;

        // Initial synchronous paint.
        safe(() => binding.write(this._store[key] as T, node));

        return () => {
            const current = this._bindings[key];
            if (!current || current[id] !== binding) return;
            delete current[id];
            if (this._isEmpty(current)) delete this._bindings[key];
        };
    }

    public domText<T = Any>(key: S, node: Text): Unsubscribe {
        return this.dom<T>(key, node, (value, target) => {
            (target as Text).data = value == null ? '' : String(value);
        });
    }

    public domAttr<T = Any>(
        key: S,
        element: Element,
        attribute: S,
    ): Unsubscribe {
        return this.dom<T>(key, element, (value, target) => {
            const el = target as Element;

            if (value == null || value === false) {
                el.removeAttribute(attribute);
                return;
            }

            el.setAttribute(attribute, value === true ? '' : String(value));
        });
    }

    /* ------------------------------------------------------------------ *
     * Persistence
     * ------------------------------------------------------------------ */

    public persist(key: S): void {
        this._persistKeys[key] = true;

        const value = this._store[key];
        if (value !== undefined) {
            this._persistent[key] = value;
            this._storage.set(key, value);
        }
    }

    public unpersist(key: S): void {
        delete this._persistKeys[key];
        delete this._persistent[key];
        this._storage.delete(key);
    }

    /* ------------------------------------------------------------------ *
     * Signals / atoms
     * ------------------------------------------------------------------ */

    public signal<T = Any>(key: S, initialValue?: T): Signal<T> {
        if (initialValue !== undefined && !this.has(key)) this.set(key, initialValue);

        const self = this;

        const fn = ((next?: T) => {
            if (arguments.length === 0) return self.get<T>(key) as T;
            self.set(key, next as T);
        }) as Signal<T>;

        Object.defineProperty(fn, 'key', { value: key, enumerable: true });

        Object.defineProperty(fn, 'value', {
            get: () => self.get<T>(key) as T,
            set: (value: T) => self.set(key, value),
            enumerable: true,
        });

        Object.defineProperty(fn, 'current', {
            get: fn.__lookupGetter__('value'),
            set: fn.__lookupSetter__('value'),
            enumerable: true,
        });

        Object.defineProperty(fn, 'crt', {
            get: fn.__lookupGetter__('value'),
            set: fn.__lookupSetter__('value'),
            enumerable: true,
        });

        Object.defineProperty(fn, 'set', {
            get: fn.__lookupGetter__('value'),
            set: fn.__lookupSetter__('value'),
            enumerable: true,
        });

        fn.peek = () => self.peek<T>(key) as T;
        fn.has = () => self.has(key);
        fn.delete = () => self.delete(key);
        fn.sub = (listener: Listener<T>) => self.sub(key, listener);

        return fn;
    }

    public atom<T = Any>(key: S, initialValue?: T): Atom<T> {
        this.signal(key, initialValue);

        const self = this;

        const atom: Atom<T> = {
            key,

            get value() {
                return self.get<T>(key) as T;
            },

            set value(value: T) {
                self.set(key, value);
            },

            get current() {
                return self.get<T>(key) as T;
            },

            set current(value: T) {
                self.set(key, value);
            },

            get crt() {
                return self.get<T>(key) as T;
            },

            set crt(value: T) {
                self.set(key, value);
            },

            get set() {
                return self.get<T>(key) as T;
            },

            set set(value: T) {
                self.set(key, value);
            },

            peek: () => self.peek<T>(key) as T,
            has: () => self.has(key),
            delete: () => self.delete(key),
            sub: (listener: Listener<T>) => self.sub(key, listener),
        };

        return atom;
    }

    /* ------------------------------------------------------------------ *
     * Bus integration
     * ------------------------------------------------------------------ */

    protected _channels: Record<S, { handler?: Fn; push?: Record<S, Fn> }> = Object.create(null);

    public send<T = Any, P = Any>(payload: P, key: S): T | undefined {
        return this.hotSend<T, P>(payload, key);
    }

    public subHandler<P = Any, T = Any>(key: S, handler: (payload: P, message: Any) => T | Promise<T>): Unsubscribe {
        const channel = (this._channels[key] ??= Object.create(null));
        channel.handler = handler as Fn;

        return () => {
            if (channel.handler === handler) delete channel.handler;
        };
    }

    public stream(key: S, handler: (message: Any) => void): Unsubscribe {
        const channel = (this._channels[key] ??= Object.create(null));
        const id = uid('p:');
        const push = (channel.push ??= Object.create(null));
        push[id] = handler;

        return () => {
            const current = this._channels[key]?.push;
            if (!current || current[id] !== handler) return;
            delete current[id];
            if (this._isEmpty(current)) delete this._channels[key].push;
        };
    }

    public duplex(key: S) {
        const self = this;

        return {
            send<T = Any, P = Any>(payload: P): T | undefined {
                return self.send<T, P>(payload, key);
            },
            sub<P = Any, T = Any>(handler: (payload: P, message: Any) => T | Promise<T>): Unsubscribe {
                return self.subHandler<P, T>(key, handler);
            },
            stream(handler: (message: Any) => void): Unsubscribe {
                return self.stream(key, handler);
            },
        };
    }

    /* ------------------------------------------------------------------ *
     * Cross-tab sync
     * ------------------------------------------------------------------ */

    public _broadcast(key: S, value: Any, previous: Any, op: ChangeOp): void {
        if (!this._transport) return;

        const message: BroadcastMessage = {
            op,
            key,
            value,
            previous,
            origin: this._origin,
        };

        safe(() => this._transport!.send(message));
    }

    protected _receiveRemote(message: BroadcastMessage): void {
        if (!message || message.origin === this._origin) return;

        const { op, key, value, previous } = message;

        if (op === 'del' || op === 'clear') {
            if (hasOwn.call(this._store, key)) {
                delete this._store[key];
                this._emit(key, undefined, previous, op, true);
            }
            return;
        }

        const current = this._store[key];
        if (Object.is(current, value)) return;

        this._store[key] = value;
        this._emit(key, value, previous, 'set', true);
    }

    protected _createBroadcastTransport(
        channelName: S | false | undefined,
    ): SauronSignalsOptions['transport'] {
        if (channelName === false) return undefined;

        const name = channelName ?? `sauron-signals:${this.name}`;
        const channel = typeof BroadcastChannel !== 'undefined' ? new BroadcastChannel(name) : null;

        if (!channel) return undefined;

        this._channel = channel;

        return {
            send: (message) => channel.postMessage(message),
            receive: (fn) => {
                const listener = (event: MessageEvent<BroadcastMessage>) => fn(event.data);
                channel.addEventListener('message', listener);
                return () => channel.removeEventListener('message', listener);
            },
        };
    }

    protected _isEmpty(record: Record<S, Any>): boolean {
        for (const key in record) return false;
        return true;
    }

    /** Explicit teardown. */
    public dispose(): void {
        this._channel?.close();
        this._channel = null;
        this.clear();
        this._globalListeners = Object.create(null);
        this._bindings = Object.create(null);
    }
}
