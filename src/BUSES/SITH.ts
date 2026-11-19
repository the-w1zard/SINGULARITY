/**
 * SITH.ts — keyed signal store. Zero dependencies, framework-agnostic, no graph.
 *
 * Design (why it's fast):
 *  - No dependency graph, no auto-tracking, no computed topology. A write touches exactly
 *    one slot and walks that slot's listener array. O(listeners of that key), nothing else.
 *  - Each key owns a monomorphic `Slot` (value + listeners + flags). `signal()` / `atom()`
 *    hold the slot directly, so hot reads/writes skip the Map lookup entirely.
 *  - Listener arrays are copy-on-write: emit iterates the array with no snapshot/clone,
 *    and subscribing/unsubscribing (rare) pays the copy instead of every write.
 *  - Equality check is inlined (=== plus NaN), no Object.is call.
 *  - Global / prefix listeners cost one `length` check per write when unused.
 *  - Batching dedupes per slot: N writes to one key in a batch => 1 notification.
 *  - Persistence and cross-tab messages are coalesced / opt-in per key and never touch
 *    the hot path unless the key asked for them.
 *
 * Semantics worth knowing:
 *  - Notifications are synchronous and depth-first (except inside batch()).
 *  - A listener unsubscribed during an in-flight emit still receives that emit.
 *  - A listener that throws never breaks other listeners; the error goes to `onError`
 *    (default: rethrown in a microtask so it hits the global error handler).
 *  - `undefined` is "no value" for waitFor's default predicate and for persistence.
 *  - Objects are compared by reference. Mutating in place? Use edit() / touch().
 */

export type Unsub = () => void;
export type Listener<T = any> = (value: T, prev: T | undefined, key: string) => void;
export type KeyListener = (key: string, value: any, prev: any) => void;
/** Minimal Storage shape (localStorage / sessionStorage / any sync adapter). */
export interface KV {
	getItem(k: string): string | null;
	setItem(k: string, v: string): void;
	removeItem(k: string): void;
	readonly length?: number;
	key?(i: number): string | null;
}
export type BodyMsg =
	| { o: 's'; k: string; v: any }
	| { o: 'd'; k: string }
	| { o: 'm'; kv: [string, any][] }
	| { o: 'x'; ks: string[] }
	| { o: 'c' };
export type Msg = BodyMsg & { i: string };
/** Plug in your own cross-tab transport here. */
export interface Transport {
	post(msg: Msg): void;
	listen(cb: (msg: Msg) => void): void;
	close(): void;
}
export interface StoreOptions {
	/** Storage for persist(). Default: localStorage when available. `null` disables. */
	storage?: KV | null;
	/** Prefix for persisted keys in storage. Default 'sig:' */
	persistPrefix?: string;
	serialize?: (v: any) => string;
	deserialize?: (s: string) => any;
	/** BroadcastChannel name, a custom Transport, or `false` to disable. Default 'signals'. */
	channel?: string | Transport | false;
	/** Return false to keep a key tab-local. */
	broadcastFilter?: (key: string) => boolean;
	onError?: (e: unknown) => void;
}
const EMPTY: any[] = Object.freeze([]) as any;
const FORCE: unique symbol = Symbol('force');
const defined = (v: unknown): boolean => v !== undefined;
/** One per key. Keep the shape stable so V8 keeps every access monomorphic. */
export class Slot {
	v: any = undefined;
	s: Listener[] = EMPTY;
	has = false;
	p = false; // persisted
	pin = 0; // held by signal()/atom() handles
	readonly k: string;
	constructor(k: string) {
		this.k = k;
	}
}
export class SITH<M extends Record<string, any> = Record<string, any>> {
	private m: Record<string, Slot> = Object.create(null);
	private ga: KeyListener[] = EMPTY;
	private pa: { p: string; f: KeyListener }[] = EMPTY;
	private depth = 0;
	private q: Record<string, any> = Object.create(null);
	private dirty: Slot[] = [];
	private pq = false;
	private cnt = 0;
	private readonly err: (e: unknown) => void;
	private readonly pp: string;
	private readonly ser: (v: any) => string;
	private readonly de: (s: string) => any;
	private readonly st: KV | null;
	private readonly bf: ((k: string) => boolean) | null;
	private readonly id: string;
	private tx: Transport | null;

	constructor(o: StoreOptions = {}) {
		this.err =
			o.onError ??
			((x: unknown) => {
				queueMicrotask(() => {
					throw x;
				});
			});
		this.pp = o.persistPrefix ?? 'sig:';
		this.ser = o.serialize ?? ((v: any) => JSON.stringify(v));
		this.de = o.deserialize ?? ((s: string) => JSON.parse(s));
		this.st = o.storage === undefined ? defaultStorage() : o.storage;
		this.bf = o.broadcastFilter ?? null;
		this.id = Math.random().toString(36).slice(2) + Date.now().toString(36);
		const ch = o.channel === undefined ? 'signals' : o.channel;
		this.tx = ch === false ? null : typeof ch === 'string' ? bcTransport(ch) : ch;
		if (this.tx) this.tx.listen((msg) => this._receive(msg));
	}

	// ───────────────────────────── reads ─────────────────────────────

	get<K extends keyof M & string>(k: K): M[K] | undefined {
		const e = this.m[k];
		return e === undefined ? undefined : e.v;
	}
	getOr<K extends keyof M & string>(k: K, fallback: M[K]): M[K] {
		const e = this.m[k];
		return e !== undefined && e.has ? e.v : fallback;
	}
	peek<K extends keyof M & string>(k: K): M[K] | undefined {
		const e = this.m[k];
		return e === undefined ? undefined : e.v;
	}
	read<K extends keyof M & string>(k: K): M[K] | undefined {
		const e = this.m[k];
		return e === undefined ? undefined : e.v;
	}
	hot<K extends keyof M & string>(k: K, v?: M[K]): M[K] | undefined {
		if (arguments.length > 1) {
			this._w(this._ent(k), v, true);
			return v;
		}
		const e = this.m[k];
		return e === undefined ? undefined : e.v;
	}
	getMany<K extends keyof M & string>(keys: readonly K[]): (M[K] | undefined)[] {
		const out = new Array(keys.length);
		for (let i = 0; i < keys.length; i++) {
			const e = this.m[keys[i]];
			out[i] = e === undefined ? undefined : e.v;
		}
		return out;
	}
	pick<K extends keyof M & string>(keys: readonly K[]): Partial<Pick<M, K>> {
		const out: any = Object.create(null);
		for (let i = 0; i < keys.length; i++) {
			const k = keys[i];
			const e = this.m[k];
			if (e !== undefined && e.has) out[k] = e.v;
		}
		return out;
	}
	has(k: string): boolean {
		const e = this.m[k];
		return e !== undefined && e.has;
	}
	get size(): number {
		return this.cnt;
	}
	keys(): string[] {
		const out: string[] = [];
		for (const k in this.m) {
			const e = this.m[k];
			if (e.has) out.push(k);
		}
		return out;
	}
	values(): any[] {
		const out: any[] = [];
		for (const k in this.m) {
			const e = this.m[k];
			if (e.has) out.push(e.v);
		}
		return out;
	}
	entries(): [string, any][] {
		const out: [string, any][] = [];
		for (const k in this.m) {
			const e = this.m[k];
			if (e.has) out.push([k, e.v]);
		}
		return out;
	}
	getAll(): Partial<M> {
		const out: any = Object.create(null);
		for (const k in this.m) {
			const e = this.m[k];
			if (e.has) out[k] = e.v;
		}
		return out;
	}
	getByPrefix(prefix: string, strip = false): Record<string, any> {
		const out: any = Object.create(null);
		const n = prefix.length;
		for (const k in this.m) {
			const e = this.m[k];
			if (e.has && k.startsWith(prefix)) out[strip ? k.slice(n) : k] = e.v;
		}
		return out;
	}
	keysByPrefix(prefix: string): string[] {
		const out: string[] = [];
		for (const k in this.m) {
			const e = this.m[k];
			if (e.has && k.startsWith(prefix)) out.push(k);
		}
		return out;
	}
	// ───────────────────────────── writes ─────────────────────────────
	set<K extends keyof M & string>(k: K, v: M[K]): boolean {
		let e = this.m[k];
		if (e === undefined) e = this._mk(k);
		return this._w(e, v, true);
	}
	write<K extends keyof M & string>(k: K, v: M[K]): boolean {
		return this.set(k, v);
	}
	setLocal<K extends keyof M & string>(k: K, v: M[K]): boolean {
		let e = this.m[k];
		if (e === undefined) e = this._mk(k);
		return this._w(e, v, false);
	}
	setMany(o: Partial<M>): number {
		const kv: [string, any][] = [];
		let n = 0;
		this.batch(() => {
			const ks = Object.keys(o);
			for (let i = 0; i < ks.length; i++) {
				const k = ks[i];
				if (this._sm(k, (o as any)[k], kv)) n++;
			}
		});
		if (kv.length !== 0 && this.tx !== null) this._post({ o: 'm', kv });
		return n;
	}
	create<K extends keyof M & string>(k: K, init: M[K] | (() => M[K])): M[K] {
		let e = this.m[k];
		if (e !== undefined && e.has) return e.v;
		if (e === undefined) e = this._mk(k);
		this._w(e, typeof init === 'function' ? (init as () => M[K])() : init, true);
		return e.v;
	}
	update<K extends keyof M & string>(k: K, fn: (prev: M[K] | undefined) => M[K]): M[K] {
		let e = this.m[k];
		if (e === undefined) e = this._mk(k);
		const next = fn(e.v);
		this._w(e, next, true);
		return next;
	}
	swap<K extends keyof M & string>(k: K, v: M[K]): M[K] | undefined {
		let e = this.m[k];
		if (e === undefined) e = this._mk(k);
		const prev = e.v;
		this._w(e, v, true);
		return prev;
	}
	edit<K extends keyof M & string>(k: K, fn: (v: M[K]) => M[K] | void): boolean {
		const e = this.m[k];
		if (e === undefined || !e.has) return false;
		const r = fn(e.v);
		if (r !== undefined && r !== e.v) this._w(e, r, true);
		else this._touch(e);
		return true;
	}
	touch(k: string): boolean {
		const e = this.m[k];
		if (e === undefined || !e.has) return false;
		this._touch(e);
		return true;
	}
	_emit(k: string): void {
		const e = this.m[k];
		if (e !== undefined) this._n(e, FORCE);
	}
	delete(k: string): boolean {
		const e = this.m[k];
		if (e === undefined || !e.has) return false;
		this._d(e, true);
		return true;
	}
	deleteMany(keys: readonly string[]): number {
		const ks: string[] = [];
		this.batch(() => {
			for (let i = 0; i < keys.length; i++) {
				const e = this.m[keys[i]];
				if (e !== undefined && e.has) {
					this._d(e, false);
					if (this._ok(e.k)) ks.push(e.k);
				}
			}
		});
		if (ks.length !== 0 && this.tx !== null) this._post({ o: 'x', ks });
		return ks.length;
	}
	deleteByPrefix(prefix: string): number {
		return this.deleteMany(this.keysByPrefix(prefix));
	}
	clear(): void { this._clear(true);	}
	// ───────────────────────────── batching ─────────────────────────────
	batch<R>(fn: () => R): R {
		this.depth++;
		try {
			return fn();
		} finally {
			if (--this.depth === 0) this._flushQ();
		}
	}

	// ───────────────────────────── subscriptions ─────────────────────────────
	sub<K extends keyof M & string>(k: K, fn: Listener<M[K]>, immediate = false): Unsub {
		const e = this._ent(k);
		const n = e.s.slice();
		n.push(fn);
		e.s = n;
		if (immediate && e.has) {
			try {
				fn(e.v, undefined, k);
			} catch (x) {
				this.err(x);
			}
		}
		let live = true;
		return () => {
			if (!live) return;
			live = false;
			const s = e.s;
			const i = s.indexOf(fn);
			if (i < 0) return;
			if (s.length === 1) e.s = EMPTY;
			else {
				const c = s.slice();
				c.splice(i, 1);
				e.s = c;
			}
			if (!e.has) this._gc(e);
		};
	}
	once<K extends keyof M & string>(k: K, fn: Listener<M[K]>): Unsub {
		const off: Unsub = this.sub(k, (v, p, key) => {
			off();
			fn(v, p, key);
		});
		return off;
	}
	subAll(fn: KeyListener): Unsub {
		const n = this.ga.slice();
		n.push(fn);
		this.ga = n;
		return () => {
			const i = this.ga.indexOf(fn);
			if (i < 0) return;
			if (this.ga.length === 1) this.ga = EMPTY;
			else {
				const c = this.ga.slice();
				c.splice(i, 1);
				this.ga = c;
			}
		};
	}
	subPrefix(prefix: string, fn: KeyListener): Unsub {
		const rec = { p: prefix, f: fn };
		const n = this.pa.slice();
		n.push(rec);
		this.pa = n;
		return () => {
			const i = this.pa.indexOf(rec);
			if (i < 0) return;
			if (this.pa.length === 1) this.pa = EMPTY;
			else {
				const c = this.pa.slice();
				c.splice(i, 1);
				this.pa = c;
			}
		};
	}
	subMany(keys: readonly (keyof M & string)[], fn: KeyListener): Unsub;
	subMany(map: { [K in keyof M & string]?: Listener<M[K]> }): Unsub;
	subMany(a: any, fn?: KeyListener): Unsub {
		const offs: Unsub[] = [];
		if (Array.isArray(a)) {
			for (let i = 0; i < a.length; i++) {
				offs.push(this.sub(a[i], (v, p, k) => (fn as KeyListener)(k, v, p)));
			}
		} else {
			const ks = Object.keys(a);
			for (let i = 0; i < ks.length; i++) offs.push(this.sub(ks[i] as any, a[ks[i]]));
		}
		return () => {
			for (let i = 0; i < offs.length; i++) offs[i]();
		};
	}
	waitFor<K extends keyof M & string>(
		k: K,
		pred: (v: M[K]) => boolean = defined,
		o?: { timeout?: number; signal?: AbortSignal },
	): Promise<M[K]> {
		const cur = this.m[k];
		if (cur !== undefined && cur.has && pred(cur.v)) return Promise.resolve(cur.v);
		return new Promise<M[K]>((res, rej) => {
			let off: Unsub | undefined;
			let t: ReturnType<typeof setTimeout> | undefined;
			const sig = o?.signal;
			const done = () => {
				if (off) off();
				if (t !== undefined) clearTimeout(t);
				if (sig) sig.removeEventListener('abort', ab);
			};
			const ab = () => {
				done();
				rej((sig as any).reason ?? new Error(`waitFor("${k}") aborted`));
			};
			off = this.sub(k, (v) => {
				if (pred(v)) {
					done();
					res(v);
				}
			});
			if (o?.timeout !== undefined) {
				const ms = o.timeout;
				t = setTimeout(() => {
					done();
					rej(new Error(`waitFor("${k}") timed out after ${ms}ms`));
				}, ms);
			}
			if (sig) {
				if (sig.aborted) ab();
				else sig.addEventListener('abort', ab, { once: true });
			}
		});
	}
	derive<K extends keyof M & string>(
		out: K,
		sources: readonly (keyof M & string)[],
		fn: (...v: any[]) => M[K],
	): Unsub {
		const run = () => {
			const args = new Array(sources.length);
			for (let i = 0; i < sources.length; i++) {
				const e = this.m[sources[i]];
				args[i] = e === undefined ? undefined : e.v;
			}
			this.set(out, fn(...args));
		};
		const offs = sources.map((s) => this.sub(s, run));
		run();
		return () => offs.forEach((o) => o());
	}
	// ───────────────────────────── handles ─────────────────────────────
	signal<K extends keyof M & string>(k: K, init?: M[K]): Sig<M[K]> {
		const e = this._pinSlot(k, init);
		return new Sig<M[K]>(this, e);
	}
	atom<K extends keyof M & string>(k: K, init?: M[K]): Atom<M[K]> {
		const e = this._pinSlot(k, init);
		return new Atom<M[K]>(this, e);
	}

	// ───────────────────────────── persistence ─────────────────────────────

	persist<K extends keyof M & string>(k: K, init?: M[K]): M[K] | undefined {
		const e = this._ent(k);
		const st = this.st;
		if (st !== null) {
			let raw: string | null = null;
			try {
				raw = st.getItem(this.pp + k);
			} catch (x) {
				this.err(x);
			}
			if (raw !== null) {
				let ok = false;
				let val: any;
				try {
					val = this.de(raw);
					ok = true;
				} catch (x) {
					this.err(x);
				}
				if (ok) {
					e.p = false;
					this._w(e, val, false);
					e.p = true;
					return e.v;
				}
			}
		}
		e.p = true;
		if (!e.has && init !== undefined) this._w(e, init, true);
		else if (e.has) this._dirty(e);
		return e.v;
	}
	persistMany(keys: readonly (keyof M & string)[]): void {
		for (let i = 0; i < keys.length; i++) this.persist(keys[i]);
	}
	unpersist(k: string, keepStorage = false): void {
		const e = this.m[k];
		if (e !== undefined) {
			e.p = false;
			const i = this.dirty.indexOf(e);
			if (i !== -1) this.dirty.splice(i, 1);
		}
		if (!keepStorage && this.st !== null) {
			try {
				this.st.removeItem(this.pp + k);
			} catch (x) {
				this.err(x);
			}
		}
		if (e !== undefined && !e.has) this._gc(e);
	}
	unpersistMany(keys: readonly string[], keepStorage = false): void {
		for (let i = 0; i < keys.length; i++) this.unpersist(keys[i], keepStorage);
	}
	isPersisted(k: string): boolean {
		const e = this.m[k];
		return e !== undefined && e.p;
	}
	hydrate(): number {
		const st = this.st;
		if (st === null || typeof st.key !== 'function' || st.length === undefined) return 0;
		const names: string[] = [];
		for (let i = 0; i < st.length; i++) {
			const full = st.key(i);
			if (full !== null && full.startsWith(this.pp)) names.push(full.slice(this.pp.length));
		}
		this.batch(() => {
			for (let i = 0; i < names.length; i++) this.persist(names[i] as any);
		});
		return names.length;
	}
	flush(): void {
		this.pq = false;
		const st = this.st;
		const dirty = this.dirty;
		this.dirty = [];

		if (st !== null) {
			for (let i = 0; i < dirty.length; i++) {
				const e = dirty[i];
				try {
					if (e.has && e.v !== undefined) st.setItem(this.pp + e.k, this.ser(e.v));
					else st.removeItem(this.pp + e.k);
				} catch (x) {
					this.err(x);
				}
			}
		}
	}

	// ───────────────────────────── cross-tab ─────────────────────────────

	_broadcast(msg: BodyMsg): void {
		this._post(msg);
	}
	_receive(msg: Msg): void {
		if (msg.i === this.id) return;
		switch (msg.o) {
			case 's': {
				this._w(this._ent(msg.k), msg.v, false);
				break;
			}
			case 'd': {
				const e = this.m[msg.k];
				if (e !== undefined && e.has) this._d(e, false);
				break;
			}
			case 'm': {
				const kv = msg.kv;
				this.batch(() => {
					for (let i = 0; i < kv.length; i++) this._w(this._ent(kv[i][0]), kv[i][1], false);
				});
				break;
			}
			case 'x': {
				const ks = msg.ks;
				this.batch(() => {
					for (let i = 0; i < ks.length; i++) {
						const e = this.m[ks[i]];
						if (e !== undefined && e.has) this._d(e, false);
					}
				});
				break;
			}
			case 'c':
				this._clear(false);
				break;
		}
	}
	dispose(): void {
		if (this.tx) this.tx.close();
		this.tx = null;
		this.ga = EMPTY;
		this.pa = EMPTY;
		for (const k in this.m) this.m[k].s = EMPTY;
		this.flush();
	}

	// ───────────────────────────── internals ─────────────────────────────

	_w(e: Slot, v: any, bc: boolean): boolean {
		const prev = e.v;
		if (e.has) {
			if (v === prev || (v !== v && prev !== prev)) return false;
		} else {
			e.has = true;
			this.cnt++;
		}
		e.v = v;
		if (e.p) this._dirty(e);
		if (bc && this.tx !== null && this._ok(e.k)) this._post({ o: 's', k: e.k, v });
		if (this.depth !== 0) {
			if (!Object.prototype.hasOwnProperty.call(this.q, e.k)) this.q[e.k] = prev;
		} else {
			this._fire(e, prev);
		}
		return true;
	}

	private _d(e: Slot, bc: boolean): void {
		const prev = e.v;
		e.v = undefined;
		e.has = false;
		this.cnt--;
		if (e.p) this._dirty(e);
		if (bc && this.tx !== null && this._ok(e.k)) this._post({ o: 'd', k: e.k });
		if (this.depth !== 0) {
			if (!Object.prototype.hasOwnProperty.call(this.q, e.k)) this.q[e.k] = prev;
		} else {
			this._fire(e, prev);
			this._gc(e);
		}
	}

	private _touch(e: Slot): void {
		if (e.p) this._dirty(e);
		if (this.tx !== null && this._ok(e.k)) this._post({ o: 's', k: e.k, v: e.v });
		this._n(e, FORCE);
	}

	private _n(e: Slot, prev: any): void {
		if (this.depth !== 0) {
			if (!Object.prototype.hasOwnProperty.call(this.q, e.k)) this.q[e.k] = prev;
		} else {
			this._fire(e, prev === FORCE ? e.v : prev);
		}
	}

	private _fire(e: Slot, prev: any): void {
		const v = e.v;
		const k = e.k;
		const s = e.s;
		for (let i = 0; i < s.length; i++) {
			try {
				s[i](v, prev, k);
			} catch (x) {
				this.err(x);
			}
		}
		const g = this.ga;
		for (let i = 0; i < g.length; i++) {
			try {
				g[i](k, v, prev);
			} catch (x) {
				this.err(x);
			}
		}
		const p = this.pa;
		if (p.length !== 0) {
			for (let i = 0; i < p.length; i++) {
				if (k.startsWith(p[i].p)) {
					try {
						p[i].f(k, v, prev);
					} catch (x) {
						this.err(x);
					}
				}
			}
		}
	}

	private _flushQ(): void {
		const q = this.q;
		const ks = Object.keys(q);
		if (ks.length === 0) return;

		this.q = Object.create(null);

		for (let i = 0; i < ks.length; i++) {
			const k = ks[i];
			const e = this.m[k];
			const prev = q[k];
			if (e === undefined) continue;

			if (prev === FORCE) this._fire(e, e.v);
			else if (!(e.v === prev || (e.v !== e.v && prev !== prev))) this._fire(e, prev);

			if (!e.has) this._gc(e);
		}
	}

	private _clear(bc: boolean): void {
		this.batch(() => {
			for (const k in this.m) {
				const e = this.m[k];
				if (e.has) this._d(e, false);
			}
		});
		if (bc && this.tx !== null) this._post({ o: 'c' });
	}

	private _sm(k: string, v: any, kv: [string, any][]): boolean {
		let e = this.m[k];
		if (e === undefined) e = this._mk(k);
		if (this._w(e, v, false)) {
			if (this._ok(k)) kv.push([k, v]);
			return true;
		}
		return false;
	}

	private _mk(k: string): Slot {
		const e = new Slot(k);
		this.m[k] = e;
		return e;
	}
	private _ent(k: string): Slot {
		let e = this.m[k];
		if (e === undefined) e = this._mk(k);
		return e;
	}
	private _pinSlot(k: string, init: any): Slot {
		const e = this._ent(k);
		e.pin++;
		if (init !== undefined && !e.has) this._w(e, init, true);
		return e;
	}
	_gc(e: Slot): void {
		if (!e.has && e.s === EMPTY && e.pin === 0 && !e.p && this.depth === 0 && this.m[e.k] === e) {
			delete this.m[e.k];
		}
	}
	_unpin(e: Slot): void {
		if (e.pin > 0) e.pin--;
		if (!e.has) this._gc(e);
	}

	private _dirty(e: Slot): void {
		if (this.dirty.indexOf(e) === -1) this.dirty.push(e);
		if (!this.pq) {
			this.pq = true;
			queueMicrotask(() => {
				if (this.pq) this.flush();
			});
		}
	}
	private _ok(k: string): boolean {
		return this.bf === null || this.bf(k);
	}
	private _post(m: BodyMsg): void {
		const tx = this.tx;
		if (tx === null) return;
		try {
			tx.post({ ...m, i: this.id } as Msg);
		} catch (x) {
			this.err(x);
		}
	}
}
// ───────────────────────────── handles ─────────────────────────────
/** Functional handle bound directly to a slot. */
export class Sig<T> {
	private readonly _s: SITH<any>;
	private readonly _e: Slot;
	constructor(s: SITH<any>, e: Slot) {
		this._s = s;
		this._e = e;
	}
	get key(): string {
		return this._e.k;
	}
	get(): T {
		return this._e.v;
	}
	peek(): T {
		return this._e.v;
	}
	set(v: T): boolean {
		return this._s._w(this._e, v, true);
	}
	update(fn: (prev: T) => T): T {
		const n = fn(this._e.v);
		this._s._w(this._e, n, true);
		return n;
	}
	get value(): T {
		return this._e.v;
	}
	set value(v: T) {
		this._s._w(this._e, v, true);
	}
	has(): boolean {
		return this._e.has;
	}
	sub(fn: Listener<T>, immediate = false): Unsub {
		return this._s.sub(this._e.k, fn, immediate);
	}
	delete(): boolean {
		return this._s.delete(this._e.k);
	}
	/** Release the slot pin so the store can garbage-collect an unused key. */
	release(): void {
		this._s._unpin(this._e);
	}
}
/** useRef-shaped handle. */
export class Atom<T> {
	private readonly _s: SITH<any>;
	private readonly _e: Slot;
	constructor(s: SITH<any>, e: Slot) {
		this._s = s;
		this._e = e;
	}
	get key(): string {
		return this._e.k;
	}
	get value(): T {
		return this._e.v;
	}
	set value(v: T) {
		this._s._w(this._e, v, true);
	}
	get current(): T {
		return this._e.v;
	}
	set current(v: T) {
		this._s._w(this._e, v, true);
	}
	get crt(): T {
		return this._e.v;
	}
	set crt(v: T) {
		this._s._w(this._e, v, true);
	}
	get set(): T {
		return this._e.v;
	}
	set set(v: T) {
		this._s._w(this._e, v, true);
	}
	peek(): T {
		return this._e.v;
	}
	get(): T {
		return this._e.v;
	}
	update(fn: (prev: T) => T): T {
		const n = fn(this._e.v);
		this._s._w(this._e, n, true);
		return n;
	}
	sub(fn: Listener<T>, immediate = false): Unsub {
		return this._s.sub(this._e.k, fn, immediate);
	}
	release(): void {
		this._s._unpin(this._e);
	}
}
// ───────────────────────────── environment helpers ─────────────────────────────
function defaultStorage(): KV | null {
	try {
		const ls = (globalThis as any).localStorage as KV | undefined;
		return ls ?? null;
	} catch {
		return null; // sandboxed iframes / blocked storage throw on access
	}
}
function bcTransport(name: string): Transport | null {
	const BC = (globalThis as any).BroadcastChannel as typeof BroadcastChannel | undefined;
	if (!BC) return null;
	const c = new BC(name);
	return {
		post: (m) => c.postMessage(m),
		listen: (cb) => {
			c.onmessage = (ev: MessageEvent) => cb(ev.data as Msg);
		},
		close: () => c.close(),
	};
}
// ───────────────────────────── exports ─────────────────────────────
export function createStore<M extends Record<string, any> = Record<string, any>>(o?: StoreOptions): SITH<M> {
	return new SITH<M>(o);
}
/** Shared default instance. */
export const signals = new SITH();
export default signals;
