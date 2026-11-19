// YODA.ts
'use strict';
/**
creating a reactive signals solution that writes to the dom where we do not use a disruptor, we do not use encoding... we only use obj since we do not use maps and arrays are just not realistic for real world usage.

Instead, we turn to this bus to the spine of everything. Creating the hot paths within the constructor, making it so that we match... literally almost to the second decimal point the same performance as a direct import.

So with that knowledge, we have an oppurtunity to build a rediculously fast system, that comprises of almost no machinery when compared to the others...

other rules that we need to follow:
no graphs of any kind, they are slow
exposed api includes at a minimum, this does not mean this is EVERYTHING and thats it... no, this needs to be a real solution I am just providing these as a must have:
get
getMany
set
setMany
delete
deleteMany
sub
sub all
subMany
swap / edit
wait for
get by prefix
get all
clear
entries
persist
unpresist
has
create
write
read
peek
_emit for waiters and such
_broadcast ( auto cross tab sync, i alreqady have this if u dont know how )
signal ( creates a basic signal that at a min holds the value or maybe we don't do this depending on the type of path we go down in terms of propgating the changes and notifiying lsiteners and such )
atom ( where it has useref like surface area of functionality available for example return {
						get value(): T { return this.signal.hot(k); },
						get current(): T { return this.signal.hot(k); },
						peek(): T { return this.signal.hot(k); },
						set value(v: T) { this.signal.hot(k, v); },
						set current(v: T) { this.signal.hot(k, v); },
						set crt(v: T) { this.signal.hot(k, v); },
						set set(v: T) { this.signal.hot(k, v); },
					};)

tips and tricks: I have 2 of my own solutions, where they are pretty trick... making it so that whenever someone looks at them, they always ask... wheres the rest of it. One being a complete applications reactive state/context solution, that is signals just not... as pretty as other solutions, since there is basically nothing to it. This solution has dom inject and allows me to create hooks

as you will notice the bus is already in perfect shape for you to build out the solution, as the send and sub have cold and hot paths already along with a nubmer of other things already in place for you...

obviously a hot path for stream, handler and duplex need to also be created...

---
IMPLEMENTATION NOTES (read me first)

1. ARCHITECTURE: the store is NOT a second parallel notification system.
   get/set/has/delete/peek/swap/edit all live on top of a single `O2`
   instance (`_store`) for the data + batching + derived-value machinery,
   but its change events are bridged straight into `_channels[k].push`
   (the exact same Set `stream()` already uses) via one `#ensureDispatcher`
   hookup installed once in the constructor. So "subscribing to a value"
   IS "subscribing to that key's channel" -- there's no second listener
   registry duplicating what the bus already does. That's the "bus is the
   spine of everything" part taken literally.

2. BUG FOUND & FIXED: in the pasted draft, `sub.hot` stored `channel.handler`
   as a `Set` ("channel.handler ??= new Set()`, `.add`/`.delete`), but
   `send.hot` calls it directly as a function (`this._channels[k]?.handler(p)`).
   Register via `sub.hot` then call `send.hot` and you call a Set like a
   function -> runtime crash. Fixed below: `sub.hot` now matches `sub`'s
   cold-path semantics (single replaceable handler), just skipping the
   getS() resolution -- same shape as send.hot, not a Set.

3. DESIGN CALLS I MADE where the spec names a method but not its exact
   contract (flagging these explicitly rather than quietly guessing):
     - `write` / `read`   -> raw, non-reactive escape hatch straight into
       a getS() slot (any s/m/r/cache target). No O2, no events, no
       persistence. For interop / config, not application state.
     - `create`           -> set-if-absent, returns the resulting value
       (`O2.getOrI`).
     - `swap` vs `edit`   -> `swap` replaces and returns the OLD value
       (already on O2). `edit` is the functional form: `edit(k, fn)`
       where `fn(old, has) -> next` (already on O2 as `update`).
     - cross-key subscribe (the "sub" item in the store list, as distinct
       from the bus's existing `sub`) is just `stream(handler, k)`, which
       already existed and is multi-listener. I didn't add a second,
       colliding `sub` for the store -- `subAll`/`subMany` are the only
       genuinely new subscribe surfaces, and they're built on `stream`.
     - `persist`/`_broadcast` use `localStorage` + `BroadcastChannel` as a
       default, swappable storage adapter. You said you already have your
       own cross-tab mechanism -- `_broadcast` is a single method, replace
       its body with a call into that and everything upstream (set/delete/
       persist) keeps working unchanged, since they only ever call
       `this._broadcast(e)`.

4. PERF SCOPE: full hot/cold duality (bypassing getS entirely) is done for
   the single-key ops (get/set/has/delete/peek/signal), mirroring send/sub.
   Bulk ops (getMany/setMany/deleteMany/getByPrefix/getAll/clear/entries/
   swap/edit/create) operate on `this._store` directly -- there's no
   separate "cold, addressable to a different m/r" variant of those, since
   that's not where the per-op overhead matters and it keeps the surface
   honest instead of faking symmetry nobody will hit in a profiler.

5. Verified with a real run in this sandbox (ts-node), not claimed from
   memory -- see the test output in my reply. I did not fabricate
   ops/sec numbers; a sandbox container isn't your bare-metal Bun box, so
   I only confirmed correctness, not comparative throughput.
---
 */

type BUS_HANDLER<P = U, T = U> = (p: P, msg?: SIG_BUS_Message1) => T | Promise<T>; // msg optional: send.hot skips building it, same as a direct function call
interface SIG_BUS_Message1 {
	id?: string;
	r?: S;
	m?: S;
	s: S;
	k: S;
	p?: any;
	ts?: number;
	_ack?: boolean;
	_reply?: boolean;
	_rid?: string;
	ackTo?: string;
}
interface BUS_CHANNEL {
	handler?: BUS_HANDLER<any, any>;
	push?: Set<(msg: SIG_BUS_Message1) => void>;
}
type c_type = "mesh" | 'globalThis'

/** sentinel for signal.hot's optional 2nd arg -- lets `undefined` still be a settable value */
const NONE: A = Symbol('signals.none');

type Waiter<T = A> = { resolve: (v: T) => V; predicate?: (v: T, has: B) => B };
type PersistCfg = { storage?: { getItem(k: S): S | null; setItem(k: S, v: S): void; removeItem(k: S): void }; prefix: S };

export class YODA<Z extends string = string, X extends string = string> {
	/**
	Several peak runs followed by usage paths
	Peak:                                       1_228.50 M ops/sec
	Peak:                                       3_103.41 M ops/sec
	Peak:                                       2_370.05 M ops/sec
	Peak:                                       2_325.32 M ops/sec
	Peak:                                       3_399.44 M ops/sec

	These runs were all


	FELKOR2.send
	sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
	12    |   1146.44      |   1146.44    |   1146.44   |    100.0

	FELKOR2.stream
	sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
	12    |   1319.94      |   1319.94    |   1319.94   |    100.0
	PS F:\mq\src> bun run aa.ts

	FELKOR2.iterator.publish
	sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
	12    |     540.61     |   540.61     |  540.61     |    100.3
	PS F:\mq\src> bun run aa.ts

	FELKOR2.duplex
	sec | M writes/s | best M/s | avg M/s | CPU %
	12 |     898.32 |   898.32 |  898.32 | 100.0
	 */
	public s: S;
	public m: S;
	public r: S;
	public k: S;
	public cache: c_type

	THE_CACHE: R<S, R<S, R<S, R<S, A>>>> = Object.create(null);
	_channels: R<S, BUS_CHANNEL>;
	_store: O2<S, A>;
	_waiters: R<S, Set<Waiter>>;
	_listeners: R<S, A>; // reserved, not used by the additions below
	_global_listeners: Set<(e: M2Event<S, A>) => V>;
	_persistKeys: Set<S>;
	_persistent: R<S, PersistCfg>;
	_bc?: { postMessage(m: A): V; onmessage: ((ev: { data: A }) => V) | null };
	#dispatcherInstalled = false;

	constructor(k: A = "__SAURON__", s: S = "SAURON_SLAVE_REG" as S, m: S = "SAURON_MASTER_REG" as S, r: S = "THE_WIZARD_ROOT_REG" as S, cache: c_type = "mesh") {
		this.s = s;
		this.m = m;
		this.r = r;
		this.k = k;
		this.cache = cache
		this._channels = this.getS('_channels', this.m, this.r);
		this._store = this.#slot('_store', () => new O2<S, A>());
		this._waiters = this.getS('_waiters', this.m, this.r);
		this._listeners = this.getS('_listeners', this.m, this.r);
		this._global_listeners = this.#slot('_global_listeners', () => new Set());
		this._persistKeys = this.#slot('_persistKeys', () => new Set());
		this._persistent = this.getS('_persistent', this.m, this.r);

		this.#ensureDispatcher();
		this.#listenBroadcast();

		// rebind the two bus methods into instance properties so each can
		// carry a `.hot` sibling without touching their existing bodies.
		this.stream = Object.assign(this.stream.bind(this), {
			hot: (handler: (msg: SIG_BUS_Message1) => V, k: S): (() => V) => {
				const channel = (this._channels[k] ??= {});
				const listeners = (channel.push ??= new Set());
				listeners.add(handler);
				return () => { listeners.delete(handler); if (!listeners.size) channel.push = undefined; };
			},
		}) as A;
		this.duplex = Object.assign(this.duplex.bind(this), {
			hot: (k: S) => ({
				send: <T = U, P = U>(p: P) => this.sendStream<T>(p, k, this.s, this.m, this.r),
				sub: <P = U, T = U>(handler: BUS_HANDLER<P, T>) => (this.sub as A).hot(handler, k),
				stream: (handler?: (msg: SIG_BUS_Message1) => V) => (this.stream as A).hot(handler, k),
			}),
		}) as A;
	}

	// ---- existing mesh slot helpers (unchanged) ----------------------------
	public getS(s: S = this.s, m: S = this.m, r: S = this.r, c: c_type = "mesh") {
		const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_CACHE;
		const rL = (cache[r] ??= Object.create(null));
		const mL = (rL[m] ??= Object.create(null));
		const sL = (mL[s] ??= Object.create(null));
		return sL;
	}
	public getM(m: S = this.m, r: S = this.r, c = "mesh") {
		const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_CACHE;
		const rL = (cache[r] ??= Object.create(null));
		const mL = (rL[m] ??= Object.create(null));
		return mL;
	}
	public getR(r: S = this.r, c = "mesh") {
		const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_CACHE;
		const rL = (cache[r] ??= Object.create(null));
		return rL;
	}
	/** like getS/getM but caches a *class instance* (e.g. the O2 store) instead of a plain object slot */
	#slot<T>(name: S, factory: () => T, m: S = this.m, r: S = this.r, c: c_type = this.cache): T {
		const mLevel = this.getM(m, r, c);
		return (mLevel[name] ??= factory()) as T;
	}

	// ---- bus (unchanged except for the sub.hot fix noted above) -----------
	public send = (() => {
		const fn = <T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD => {
			const channels = this.getS('_channels', this.m, this.r);
			const channel = channels[k];
			const handler = channel?.handler;
			if (!handler) return undefined;
			return handler(p, { r, m, s, k, p: p });
		};
		fn.hot = <T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD => {// SAME OPS COST AS A DIRECT IMPORT
			return this._channels[k]?.handler?.(p) as A;
		};
		return fn;
	})();
	public sub = (() => {
		const fn = <P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V => {
			const channels = this.getS('_channels', this.m, this.r);
			const channel = (channels[k] ??= {});
			channel.handler = handler;
			return () => {
				if (channel.handler === handler) {
					channel.handler = undefined;
				}
			};
		};
		fn.hot = <P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S): () => V => {// FIXED: single handler, matches send.hot's expectations (was a Set, incompatible with send.hot calling it directly)
			const channel = (this._channels[k] ??= {});
			channel.handler = handler;
			return () => { if (channel.handler === handler) channel.handler = undefined; };
		};
		return fn;
	})();

	public sendStream<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
		const channel = this._channels[k];
		const handler = channel?.handler;
		const msg: SIG_BUS_Message1 = { r, m, s, k, p: p, _ack: false };
		// FIXED: used to `return undefined` here when no single handler was registered,
		// which skipped the push-listener fan-out below entirely -- so a channel with
		// only stream()/duplex().stream() subscribers (no sub()) never got notified.
		const result = handler ? handler(p, msg) : undefined;
		const listeners = channel?.push;
		if (listeners) {
			for (const listener of listeners) listener(msg);
		}
		return result as T | P<T> | UD;
	}
	public stream(handler: (msg: SIG_BUS_Message1) => V, k: S, s: S, m?: S, r?: S): () => V;
	public stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<SIG_BUS_Message1>;
	public stream(handlerOrSlave: ((msg: SIG_BUS_Message1) => V) | S, k: S, s: S = this.s, m: S = this.m, r: S = this.r): (() => V) | AsyncIterableIterator<SIG_BUS_Message1> {
		const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
		const global = globalThis as any;
		const channel = (this._channels[k] ??= {});

		if (!channel) throw new Error("Channel does not exist");

		const listeners = (channel.push ??= new Set());

		if (handler) {
			listeners.add(handler);

			return () => {
				listeners.delete(handler);
				if (!listeners.size) channel.push = undefined;
			};
		}

		const queue: SIG_BUS_Message1[] = [];
		const waiters: Array<{ resolve: (value: IteratorResult<SIG_BUS_Message1>) => void }> = [];

		let offset = 0;
		let closed = false;

		const listener = (msg: SIG_BUS_Message1) => {
			if (waiters.length) {
				waiters.shift()!.resolve({ value: msg, done: false });
			} else {
				queue.push(msg);
			}
		};

		listeners.add(listener);

		const close = () => {
			if (closed) return;
			closed = true;

			listeners.delete(listener);
			if (!listeners.size) channel.push = undefined;

			for (const waiter of waiters) {
				waiter.resolve({ value: undefined, done: true });
			}

			waiters.length = 0;
			queue.length = 0;
			offset = 0;
		};

		const iterator: AsyncIterableIterator<SIG_BUS_Message1> = {
			next: (): Promise<IteratorResult<SIG_BUS_Message1>> => {
				if (offset < queue.length) {
					const value = queue[offset++];

					if (offset === queue.length) {
						queue.length = 0;
						offset = 0;
					}

					return Promise.resolve({ value, done: false });
				}

				if (closed) {
					return Promise.resolve({ value: undefined, done: true });
				}

				return new Promise((resolve) => waiters.push({ resolve }));
			},

			return: async () => {
				close();
				return { value: undefined, done: true };
			},

			[Symbol.asyncIterator](): AsyncIterableIterator<SIG_BUS_Message1> {
				return iterator;
			},
		};

		return iterator;
	}
	public duplex(k: S, s: S = this.s, m: S = this.m, r: S = this.r) {
		return {
			send: <T = U, P = U>(p: P) => this.sendStream<T>(p, k, s, m, r),
			sub: <P = U, T = U>(handler: BUS_HANDLER<P, T>) => this.sub<P, T>(handler, k, s, m, r),
			stream: (handler?: (msg: SIG_BUS_Message1) => V) => (handler ? this.stream(handler, k, s, m, r) : this.stream(k, s, m, r)),
		};
	}

	// ======================================================================
	// store API (new)
	// ======================================================================

	#ensureDispatcher(): V {
		if (this.#dispatcherInstalled) return;
		this.#dispatcherInstalled = true;
		this._store.on((e: M2Event<S, A>) => this._emit(e));
	}

	/** central fan-out: bus push-listeners for the key, subAll listeners, waiters, persistence, cross-tab. exposed per spec ("_emit for waiters and such"). */
	public _emit(e: M2Event<S, A>): V {
		const listeners = this._channels[e.k as S]?.push;
		if (listeners) for (const fn of listeners) { try { (fn as A)(e); } catch (err: A) { /* one bad subscriber shouldn't take down the rest */ } }

		if (this._global_listeners.size) for (const fn of this._global_listeners) { try { fn(e); } catch (err: A) { } }

		const waiters = this._waiters[e.k as S];
		if (waiters?.size) {
			for (const w of [...waiters]) {
				if (!w.predicate || w.predicate(e.v, e.has)) { waiters.delete(w); w.resolve(e.v); }
			}
		}

		if (this._persistKeys.has(e.k as S)) this.#writeThrough(e.k as S, e.v, e.op === "del"); // note: e.has means "existed BEFORE this event" (O2's convention), not "should persist" -- op is the correct signal here

		this._broadcast(e);
	}

	/** cross-tab sync. you said you already have one -- swap this body for it; set/delete/persist only ever call `this._broadcast(e)`, nothing else needs to change. */
	public _broadcast(e: M2Event<S, A>): V {
		if (typeof BroadcastChannel === "undefined") return;
		(this._bc ??= new BroadcastChannel(`${String(this.k)}:${this.m}:${this.r}`) as A).postMessage(e);
	}
	#listenBroadcast(): V {
		if (typeof BroadcastChannel === "undefined") return;
		const bc = (this._bc ??= new BroadcastChannel(`${String(this.k)}:${this.m}:${this.r}`) as A);
		bc.onmessage = (ev: { data: A }) => {
			const e = ev.data as M2Event<S, A>;
			// BroadcastChannel never delivers to its own sender, so this can't echo-loop locally.
			if (e.op === "del") this._store.d(e.k);
			else this._store.s(e.v, e.k);
		};
	}
	#writeThrough(k: S, v: A, isDelete: B): V {
		const cfg = this._persistent[k];
		if (!cfg?.storage) return;
		try {
			if (isDelete) cfg.storage.removeItem(cfg.prefix + k);
			else cfg.storage.setItem(cfg.prefix + k, JSON.stringify(v));
		} catch (err: A) { /* quota / circular-structure errors are swallowed here; surface via subAll if you need to react to them */ }
	}

	// -- single key, cold+hot (mirrors send/sub) -----------------------------
	public get = (() => {
		const fn = <T = U>(k: S, m: S = this.m, r: S = this.r): T | UD =>
			this.#slot<O2<S, A>>('_store', () => new O2(), m, r).g(k) as T | UD;
		fn.hot = <T = U>(k: S): T | UD => this._store.g(k) as T | UD; // SAME OPS COST AS A DIRECT IMPORT
		return fn;
	})();

	public set = (() => {
		const fn = <T = U>(v: UF<T>, k: S, m: S = this.m, r: S = this.r): T => {
			const next = rv(v);
			this.#slot<O2<S, A>>('_store', () => new O2(), m, r).s(next, k);
			return next;
		};
		fn.hot = <T = U>(v: UF<T>, k: S): T => {
			const next = rv(v);
			this._store.s(next, k); // O2.s() emits -> dispatcher -> _emit, so this already fans out/persists/broadcasts
			return next;
		};
		return fn;
	})();

	public has = (() => {
		const fn = (k: S, m: S = this.m, r: S = this.r): B => this.#slot<O2<S, A>>('_store', () => new O2(), m, r).h(k);
		fn.hot = (k: S): B => this._store.h(k);
		return fn;
	})();

	public delete = (() => {
		const fn = (k: S, m: S = this.m, r: S = this.r): B => this.#slot<O2<S, A>>('_store', () => new O2(), m, r).d(k);
		fn.hot = (k: S): B => this._store.d(k);
		return fn;
	})();

	public peek = (() => {
		const fn = <T = U>(k: S, m: S = this.m, r: S = this.r): T | false => this.#slot<O2<S, A>>('_store', () => new O2(), m, r).peek(k) as T | false;
		fn.hot = <T = U>(k: S): T | false => this._store.peek(k) as T | false;
		return fn;
	})();

	/** replaces k's value, returns the PREVIOUS value */
	public swap = <T = U>(v: UF<T>, k: S): T | UD => this._store.swap(v, k) as T | UD;
	/** functional update: edit(k, old => next) */
	public edit = <T = U>(k: S, fn: (old: T, has: B) => T): T => this._store.update(k, fn as A) as T;

	public create = <T = U>(k: S, initial: UF<T>): T => this._store.getOrI(initial as A, k) as T;

	// -- bulk (operate on this._store; no cross-mesh cold path, see notes) --
	public getMany = <T = U>(keys: I<S>): OB<T | UD> => {
		const out: OB<T | UD> = Object.create(null);
		for (const k of keys) out[k] = this._store.g(k) as T | UD;
		return out;
	};
	public setMany = (entries: I<readonly [S, A]> | OB<A>): V => {
		this._store.batch(() => {
			const iter: I<readonly [S, A]> = typeof (entries as A)[Symbol.iterator] === "function"
				? (entries as I<readonly [S, A]>)
				: (Object.entries(entries as OB<A>) as A);
			for (const [k, v] of iter) this._store.s(rv(v), k);
		});
	};
	public deleteMany = (keys: I<S>): N => {
		let count = 0;
		this._store.batch(() => { for (const k of keys) if (this._store.d(k)) count++; });
		return count;
	};
	public getByPrefix = <T = U>(prefix: S): OB<T> => {
		const out: OB<T> = Object.create(null);
		for (const [k, v] of this._store) if ((k as S).startsWith(prefix)) out[k as S] = v as T;
		return out;
	};
	public getAll = <T = U>(): OB<T> => this._store.toObject() as OB<T>;
	public entries = (): A2<[S, A]> => this._store.toArray();
	public clear = (): V => this._store.c();
	/** raw, non-reactive escape hatch: writes/reads a plain getS() slot directly, bypassing O2 and every event. For interop/config, not app state. */
	public write = (k: S, v: A, s: S = this.s, m: S = this.m, r: S = this.r, c: c_type = this.cache): V => {
		this.getS(s, m, r, c)[k] = v;
	};
	public read = <T = A>(k: S, s: S = this.s, m: S = this.m, r: S = this.r, c: c_type = this.cache): T => {
		return this.getS(s, m, r, c)[k];
	};
	/** subscribe to every store change, any key */
	public subAll = (handler: (e: M2Event<S, A>) => V): (() => V) => {
		this._global_listeners.add(handler);
		return () => { this._global_listeners.delete(handler); };
	};
	/** one handler across several keys (built on the bus's own multi-listener `stream`) */
	public subMany = (keys: I<S>, handler: (e: M2Event<S, A>) => V): (() => V) => {
		const unsubs = [...keys].map((k) => (this.stream as A).hot(handler, k) as () => V);
		return () => { for (const u of unsubs) u(); };
	};
	/** resolves once key k exists (and, if given, satisfies predicate); resolves immediately if already true */
	public waitFor = <T = U>(k: S, predicate?: (v: T, has: B) => B): P<T> => {
		if (this._store.h(k) && (!predicate || predicate(this._store.g(k) as T, true))) {
			return Promise.resolve(this._store.g(k) as T);
		}
		return new Promise<T>((resolve) => {
			const set = (this._waiters[k] ??= new Set()) as Set<Waiter<T>>;
			set.add({ resolve: resolve as A, predicate: predicate as A });
		});
	};
	/** mirrors Object.defineProperty semantics - true registration + optional storage adapter + hydrate-on-call */
	public persist = (k: S, opts: { storage?: PersistCfg["storage"]; prefix?: S } = {}): (() => V) => {
		const storage = opts.storage ?? (typeof localStorage !== "undefined" ? (localStorage as A) : undefined);
		const prefix = opts.prefix ?? `${String(this.k)}:`;
		this._persistKeys.add(k);
		this._persistent[k] = { storage, prefix };

		if (storage && !this._store.h(k)) {
			try {
				const raw = storage.getItem(prefix + k);
				if (raw !== null) this._store.s(JSON.parse(raw), k);
			} catch (err: A) { /* corrupt/foreign entry under this key -- ignored, not thrown */ }
		}

		return () => this.unpersist(k);
	};
	public unpersist = (k: S, opts: { clearStorage?: B } = {}): V => {
		const cfg = this._persistent[k];
		this._persistKeys.delete(k);
		delete this._persistent[k];
		if (opts.clearStorage && cfg?.storage) {
			try { cfg.storage.removeItem(cfg.prefix + k); } catch (err: A) { }
		}
	};
	/** basic signal: .hot(k) reads, .hot(k, v) writes -- same shape as send/sub's cold/hot split */
	public signal = (() => {
		const fn = <T = U>(k: S): T | UD => this._store.g(k) as T | UD;
		fn.hot = <T = U>(k: S, v: T | typeof NONE = NONE): T | UD => {
			if (v === NONE) return this._store.g(k) as T | UD;
			this._store.s(v as T, k);
			return v as T;
		};
		return fn;
	})();

	/** useRef-shaped handle over a signal */
	public atom<T = U>(k: S) {
		const self = this;
		return {
			get value(): T { return self.signal.hot<T>(k) as T; },
			get current(): T { return self.signal.hot<T>(k) as T; },
			peek(): T { return self._store.peek(k) as T; },
			set value(v: T) { self.signal.hot<T>(k, v); },
			set current(v: T) { self.signal.hot<T>(k, v); },
			set crt(v: T) { self.signal.hot<T>(k, v); },
			set set(v: T) { self.signal.hot<T>(k, v); },
		};
	}
}




export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B => Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);

class O2<K extends PropertyKey = S, T = A> {
	#data: Record<K, T> = Object.create(null);

	public f = this.forEachKV;//                                                    [FOR EACH]

	#subs?: Set<Sub<K, T>>;
	#hist?: M2Event<K, T>[];
	#histLimit = 0;
	#strict = false;
	#iterating = 0;
	#batching = 0;
	#pending?: Record<K, M2Event<K, T>>;
	#derived?: Record<K, Derived<K, T>>;
	#dependents?: Record<K, Set<K>>;

	constructor(entries?: I<readonly [K, T]>) {//                                   [CONSTRUCT]
		if (!entries) return;
		for (const [k, v] of entries) this.#data[k] = v;
	}

	#rawHas(k: K): B { return Object.prototype.hasOwnProperty.call(this.#data, k); }
	#rawGet(k: K): T | UD { return this.#data[k]; }

	#emit(e: M2Event<K, T>): V {
		if (this.#batching > 0) {
			const pending = this.#pending![e.k];
			this.#pending![e.k] = pending ? { ...e, prev: pending.prev, batch: true } : { ...e, batch: true };
			return;
		}

		this.#record(e);
		this.#subs?.forEach((sub) => {
			try { sub(e); } catch (err: A) { process.emitWarning(err); }
		});
	}
	#record(e: M2Event<K, T>): V {
		if (!this.#hist) return;

		this.#hist.push(e);

		if (this.#hist.length > this.#histLimit) {
			this.#hist.splice(0, this.#hist.length - this.#histLimit);
		}
	}
	#invalidate(k: K): V {
		const deps = this.#dependents?.[k];
		if (!deps) return;

		for (const dk of deps) {
			const d = this.#derived?.[dk];
			if (!d || d.dirty) continue;

			d.dirty = true;
			this.#emit({ op: "dirty", k: dk, has: d.has });
		}
	}
	#computeDerived(k: K, d: Derived<K, T>): T {
		const vals = d.deps.map((dep) => this.g(dep) as T);
		const next = d.fn(...vals);

		const had = d.has;
		const prev = d.value;

		d.value = next;
		d.has = true;
		d.dirty = false;

		if (!had || !sameValue(prev, next)) {
			this.#emit({ op: had ? "upd" : "set", k, v: next, prev: prev as T, has: true });
		}

		return next;
	}
	#assertNotIterating(op: S): V {
		if (this.#strict && this.#iterating > 0) {
			throw new Error(`O2I.${op}() during iteration. Use filterKV(), collect keys first, or disable $strict().`);
		}
	}

	public get size(): N {//                                                             [SIZE]
		return Reflect.ownKeys(this.#data).length as N;
	}
	public [Symbol.iterator](): Generator<[K, T]> { return this.entries(); }
	public on(fn: Sub<K, T>): () => V {//                                                  [ON]
		(this.#subs ??= new Set()).add(fn);
		return () => this.off(fn);
	}
	public off(fn: Sub<K, T>): V {//                                                      [OFF]
		this.#subs?.delete(fn);
	}
	public batch(fn: () => V): this {//                                                 [BATCH]
		this.#batching++;
		this.#pending ??= Object.create(null);

		try {
			fn();
		} finally {
			this.#batching--;

			if (this.#batching === 0) {
				const pending = this.#pending!;
				this.#pending = undefined;

				for (const k of Reflect.ownKeys(pending) as K[]) {
					const e = pending[k];
					this.#record(e);
					this.#subs?.forEach((sub) => {
						try { sub(e); } catch (err: A) { process.emitWarning(err); }
					});
				}
			}

		}

		return this;
	}
	public s(v: UF<T>, k: K): this {//                                                    [SET]
		this.#assertNotIterating("s");

		const next = rv(v);
		const has = this.h(k);
		const prev = has ? this.g(k) : undefined;

		this.#data[k] = next;

		this.#invalidate(k);
		this.#emit({ op: "set", k, v: next, prev: prev as T, has });

		return this;
	}
	public mutate(k: K, fn: (old: T, has: true) => T): T | UD {//                      [MUTATE]
		this.#assertNotIterating("mutate");

		if (!this.h(k)) return undefined;

		const old = this.g(k) as T;
		const next = fn(old, true);

		this.s(next, k);
		return next;
	}
	public d(k: K): B {//                                                              [DELETE]
		this.#assertNotIterating("delete");

		if (!this.h(k)) return false;

		const prev = this.g(k) as T;
		delete this.#data[k];

		this.#invalidate(k);
		this.#emit({ op: "del", k, prev, has: false });

		return true;
	}
	public c(): V {//                                                                   [CLEAR]
		this.#assertNotIterating("clear");

		const entries = this.toArray();
		this.#data = Object.create(null);//              one object swap, no per-key delete

		for (const [k, prev] of entries) {
			this.#invalidate(k);
			this.#emit({ op: "del", k, prev, has: false });
		}
	}
	public dv(k: K, deps: AR<K>, fn: (...vals: AR<T>) => T): this {//                [DERRIVED]
		(this.#derived ??= Object.create(null))[k] = { deps, fn, dirty: true, has: false };

		for (const dep of deps) {
			const dependents = (this.#dependents ??= Object.create(null));
			(dependents[dep] ??= new Set<K>()).add(k);
		}

		return this;
	}
	public dirty(...keys: AR<K>): this {//                                              [DIRTY]
		for (const k of keys) {
			const d = this.#derived?.[k];

			if (d && !d.dirty) {
				d.dirty = true;
				this.#emit({ op: "dirty", k, has: d.has });
			}
		}

		return this;
	}
	public g(k: K): T | UD {//                                                           [GET]
		const d = this.#derived?.[k];

		if (d) {
			if (d.dirty || !d.has) return this.#computeDerived(k, d);
			return d.value;
		}

		return this.#rawGet(k);
	}
	public h(k: K): B {//                                                                [HAS]
		const d = this.#derived?.[k];

		if (d) {
			if (d.dirty || !d.has) this.#computeDerived(k, d);
			return true;
		}

		return this.#rawHas(k);
	}
	public $debug(limit: N = 256): this {//                                            [$DEBUG]
		this.#hist ??= [];
		this.#histLimit = limit;
		return this;
	}
	public $strict(): this {//                                                        [$STRICT]
		this.#strict = true;
		return this;
	}
	public $history(): M2Event<K, T>[] {//                                          [$HISTORY]
		return this.#hist ? [...this.#hist] : [];
	}
	public $why(k: K): M2Event<K, T> | UD {//                                            [$WHY]
		if (!this.#hist) return undefined;

		for (let i = this.#hist.length - 1; i >= 0; i--) {
			if (Object.is(this.#hist[i].k, k)) return this.#hist[i];
		}

		return undefined;
	}
	public $diff(other: O2I<K, T>) {//                                                  [$DIFF]
		const add: AR<[K, T]> = [];
		const del: AR<K> = [];
		const chg: AR<[K, T, T]> = [];

		for (const [k, v] of this) {
			if (!other.h(k)) {
				add.push([k, v]);
			} else if (!Object.is(other.g(k), v)) {
				chg.push([k, v, other.g(k) as T]);
			}
		}

		for (const k of other.keys()) {
			if (!this.h(k)) del.push(k);
		}

		return { add, del, chg };
	}
	public *entries(): Generator<[K, T]> {//                                       [*ENTRIES]
		this.#iterating++;

		try {
			for (const k of Reflect.ownKeys(this.#data) as K[]) yield [k, this.#data[k]];
		} finally {
			this.#iterating--;
		}
	}
	public *keys(): Generator<K> {//                                                  [*KEYS]
		this.#iterating++;

		try {
			for (const k of Reflect.ownKeys(this.#data) as K[]) yield k;
		} finally {
			this.#iterating--;
		}
	}
	public *values(): Generator<T> {//                                              [*VALUES]
		this.#iterating++;

		try {
			for (const k of Reflect.ownKeys(this.#data) as K[]) yield this.#data[k];
		} finally {
			this.#iterating--;
		}
	}
	public swap(value: UF<T>, key: K): T | UD {//                                       [SWAP]
		const next = rv(value);
		const previous = this.h(key) ? this.g(key) : next;//       h()/g() kept derived-aware
		this.s(next, key);
		return previous as T | UD;
	}
	public take(k: K): T | UD {//                                                       [TAKE]
		const value = this.g(k);
		this.d(k);
		return value;
	}
	public peek(k: K): T | false {//                                                    [PEEK]
		return this.h(k) ? (this.g(k) as T) : false;
	}
	public getOrI(value: UF<T>, k: K): T {//                                          [GETORI]
		if (this.h(k)) return this.g(k) as T;
		const next = rv(value);
		this.s(next, k);
		return next;
	}
	public getOrM(make: () => T, k: K): T {//                                        [GETORM]
		if (this.h(k)) return this.g(k) as T;
		const value = make();
		this.s(value, k);
		return value;
	}
	public update(k: K, fn: (old: T, has: B) => T, initial?: UF<T>): T {//            [UPDATE]
		const has = this.h(k);

		const old = has
			? (this.g(k) as T)
			: initial === undefined
				? undefined as T
				: rv(initial);

		const next = rv(fn(old, has));

		this.s(next, k);
		return next;
	}
	public setIfAbsent(value: UF<T>, k: K): B {//                               [SETIFABSENT]
		if (this.h(k)) return false;
		const next = rv(value);
		this.s(next, k);
		return true;
	}
	public setIfPresent(value: UF<T>, k: K): B {//                             [SETIFPRESENT]
		if (!this.h(k)) return false;
		const next = rv(value);
		this.s(next, k);
		return true;
	}
	public replace(value: UF<T>, k: K): B {//                                        [REPLACE]
		const next = rv(value);
		return this.setIfPresent(next, k);
	}
	public inc(key: K, by: N = 1): N {//                                                 [INC]
		return this.update(key, (old) => (((old ?? 0) as N) + by) as unknown as T, 0 as unknown as T) as N;
	}
	public touch(k: K): B {//                                                      [SWTOUCHAP]
		return this.setIfAbsent(undefined as unknown as T, k);
	}
	public mapValues<G>(fn: (v: T, k: K) => G): O2I<K, G> {//                        [MAPVALUES]
		const out = new O2I<K, G>();
		for (const [k, v] of this) out.s(fn(v, k), k);
		return out;
	}
	public mapKeys<G extends PropertyKey>(fn: (v: T, k: K) => G): O2I<G, T> {//        [MAPKEYS]
		const out = new O2I<G, T>();
		for (const [k, v] of this) out.s(v, fn(v, k));
		return out;
	}
	public filterKV(pred: (v: T, k: K) => B): this {//                              [FILTERKV]
		for (const [k, v] of this) {
			if (!pred(v, k)) this.d(k);
		}
		return this;
	}
	public filterKeys(pred: (v: T, k: K) => B): this {//                          [FILTERKEYS]
		return this.filterKV(pred);
	}
	public partition(pred: (v: T, k: K) => B): [O2I<K, T>, O2I<K, T>] {//            [PARTITION]
		const yes = new O2I<K, T>();
		const no = new O2I<K, T>();

		for (const [k, v] of this) {
			(pred(v, k) ? yes : no).s(v, k);
		}

		return [yes, no];
	}
	public find(pred: (v: T, k: K) => B): [K, T] | UD {//                               [FIND]
		for (const [k, v] of this) {
			if (pred(v, k)) return [k, v];
		}
		return undefined;
	}
	public findKey(pred: (v: T, k: K) => B): K | UD {//                              [FINDKEY]
		for (const [k, v] of this) {
			if (pred(v, k)) return k;
		}
		return undefined;
	}
	public findValue(pred: (v: T, k: K) => B): T | UD {//                          [FINDVALUE]
		for (const [k, v] of this) {
			if (pred(v, k)) return v;
		}
		return undefined;
	}
	public some(pred: (v: T, k: K) => B): B {//                                         [SOME]
		for (const [k, v] of this) {
			if (pred(v, k)) return true;
		}
		return false;
	}
	public every(pred: (v: T, k: K) => B): B {//                                       [EVERY]
		for (const [k, v] of this) {
			if (!pred(v, k)) return false;
		}
		return true;
	}
	public reduce<G>(fn: (acc: G, v: T, k: K) => G, initial: G): G {//                [REDUCE]
		let acc = initial;
		for (const [k, v] of this) acc = fn(acc, v, k);
		return acc;
	}
	public forEachKV(fn: (v: T, k: K, map: this) => void): void {//                 [FOREACH]
		for (const [k, v] of this) fn(v, k, this);
	}
	public invert(): O2I<A, K> {//               values become keys — must be string|number|symbol
		const out = new O2I<A, K>();
		for (const [k, v] of this) out.s(k, v as A);
		return out;
	}
	public unique<G extends PropertyKey>(fn?: (v: T, k: K) => G): O2I<G, T> {//          [UNIQUE]
		const out = new O2I<G, T>();
		for (const [k, v] of this) {
			const key = (fn ? fn(v, k) : v) as unknown as G;
			if (!out.h(key)) out.s(v, key);
		}
		return out;
	}
	public pick(keys: Iterable<K>): O2I<K, T> {//                                          [PICK]
		const out = new O2I<K, T>();
		for (const k of keys) {
			if (this.h(k)) out.s(this.g(k) as T, k);
		}
		return out;
	}
	public omit(keys: Iterable<K>): O2I<K, T> {//                                          [OMIT]
		const omitted = new Set(keys);
		const out = new O2I<K, T>();
		for (const [k, v] of this) {
			if (!omitted.has(k)) out.s(v, k);
		}
		return out;
	}
	public merge(other: Iterable<readonly [K, T]>): this {//                           [MERGE]
		for (const [k, v] of other) {
			const n = rv(v);
			this.s(n, k);
		}
		return this;
	}
	public clearAndSet(entries: Iterable<readonly [K, T]>): this {//             [CLEARANDSET]
		return this.batch(() => {
			this.c();//                         (M2's version calls the native Map.clear() here
			for (const [k, v] of entries) {    //  — silent, no del events; c() is almost
				const n = rv(v);                //  certainly what was intended, so I used it)
				this.s(n, k);
			}
		});
	}
	public toObject(): OB<T> {//                                                   [TOOBJECT]
		const out = {} as OB<T>;
		for (const [k, v] of this) out[k as S] = v;
		return out;
	}
	public toObjectFast(): OB<T> {//          bulk native copy, skips the iterator entirely
		return { ...(this.#data as A) };
	}
	public snapshot(): OB<T> { return this.toObjectFast(); }//      handy name for hook/selector use
	public toJSON(): OB<T> { return this.toObjectFast(); }//     JSON.stringify(store) just works
	public select<G>(fn: (data: Readonly<Record<K, T>>) => G): G {//                   [SELECT]
		return fn(this.#data);
	}
	public patch(partial: Readonly<Partial<Record<K, T>>>): this {//                    [PATCH]
		return this.batch(() => {
			for (const k of Reflect.ownKeys(partial) as K[]) this.s((partial as A)[k] as T, k);
		});
	}
	public toValues(): A2<T> { return A2.from(this.values()); }//                 [TOVALUES]
	public toKeys(): A2<K> { return A2.from(this.keys()); }//                       [TOKEYS]
	public toArray(): A2<[K, T]> { return A2.from(this.entries()); }//             [TOARRAY]
	public group<G extends PropertyKey>(fn: (v: T, k: K) => G): O2I<G, A2<T>> {//       [GROUP]
		const out = new O2I<G, A2<T>>();
		for (const [k, v] of this) out.getOrM(() => new A2<T>(), fn(v, k)).push(v);
		return out;
	}
	public clone(): this {//                                                          [CLONE]
		return new O2I<K, T>(this) as this;
	}
	public equals(other: O2I<K, T>): B {//                                            [EQUALS]
		if (other === this) return true;
		if (this.size !== other.size) return false;

		for (const [k, v] of this) {
			if (!other.h(k) || !sameValue(other.g(k), v)) return false;
		}

		return true;
	}
	static fromObject<X extends OB<A>>(obj: X): O2I<S, X[keyof X]> {//              [FROMOBBJ]
		const out = new O2I<S, X[keyof X]>();
		out.#data = { ...obj } as Record<S, X[keyof X]>;//            direct copy, no per-key emits
		return out;
	}
	static fromEntries<K extends PropertyKey, T>(entries: I<readonly [K, T]>): O2I<K, T> {//  [FROMENTIRES]
		return new O2I(entries);
	}
	static from<K extends PropertyKey, T>(source: O2I<K, T> | Readonly<Record<K, T>>): O2I<K, T> {// [FROM]
		const out = new O2I<K, T>();
		out.#data = source instanceof O2I ? { ...source.#data } : { ...source };
		return out;
	}
}
type O2I<K extends PropertyKey = S, T = A> = O2<K, T>;
const O2I = O2; // so `new O2I(...)` calls used inside the class body resolve at runtime


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

