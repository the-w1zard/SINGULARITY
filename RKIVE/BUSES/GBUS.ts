'use strict';
/**
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

this is a compeition... your response will benched in comparison to others... let the best one win! If you want the other soutions to review YOU NEED TO STOP AND ASK FOR THEM RIGHT NOW BUT REMEMBER... THIS IS GOING INTO A REAL WORLD APPLICATION, NOT SOME TOY PROJECT, NOT SOME YOUTUBERS WEEKEND VIDEO PROJECT... A REAL PLATFORM
 */


type BUS_HANDLER<P = U, T = U> = (p: P, msg: SIG_BUS_Message1) => T | Promise<T>;
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
export class SIGNALS<Z extends string = string, X extends string = string> {
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
	_channels: R<S, A>;
	_store: R<S, A>;
	_waiters: R<S, A>;
	_listeners: R<S, A>;
	_global_listeners: R<S, A>;
	_persistKeys: R<S, A>;
	_persistent: R<S, A>;
	constructor(k: A =  "__SAURON__",s: S = "SAURON_SLAVE_REG" as S, m: S = "SAURON_MASTER_REG" as S, r: S = "THE_WIZARD_ROOT_REG" as S, cache: c_type = "mesh") {
		this.s = s;
		this.m = m;
		this.r = r;
		this.k = k;
		this.cache = cache
		this._channels = this.getS('_channels', this.m, this.r);
		this._store = this.getS('_store', this.m, this.r);
		this._waiters = this.getS('_waiters', this.m, this.r);
		this._listeners = this.getS('_listeners', this.m, this.r);
		this._global_listeners = this.getS('_global_listeners', this.m, this.r);
		this._persistKeys = this.getS('_persistKeys', this.m, this.r);
		this._persistent = this.getS('_persistent', this.m, this.r);
	}
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
	public send = (() => {
		const fn =<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD  => {
			const channels = this.getS('_channels', this.m, this.r);
			const channel = channels[k];
			const handler = channel?.handler;
			if (!handler) return undefined;
			return handler(p, { r, m, s, k, p: p });
		};
		fn.hot =<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD => {// SAME OPS COST AS A DIRECT IMPORT
			return this._channels[k]?.handler(p);
		};
		return fn;
	})();
	public sub = (() => {
		const fn =<P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V  => {
			const channels = this.getS('_channels', this.m, this.r);
			const channel = (channels[k] ??= {});
			channel.handler = handler;
			return () => {
				if (channel.handler === handler) {
					channel.handler = undefined;
				}
			};
		};
		fn.hot =<P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V => {// SAME OPS COST AS A DIRECT IMPORT
			const channel = (this._channels[k] ??= { handler: new Set() });
			channel.handler.add(handler);
			return () => { channel.handler.delete(handler); };
		};
		return fn;
	})();

	public sendStream<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
		const channel = this._channels[k];
		const handler = channel?.handler;
		if (!handler) return undefined;
		const msg: SIG_BUS_Message1 = { r, m, s, k, p: p, _ack: false };
		const result = handler(p, msg);
		const listeners = channel.push;
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
}
export const rv = <T>(v: UF<T>): T => typeof v === "function" ? (v as () => T)() : v;
export const sameValue = (a: A, b: A): B =>    Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);

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
	public toValues(): A2I<T> { return A2.from(this.values()); }//                 [TOVALUES]
	public toKeys(): A2I<K> { return A2.from(this.keys()); }//                       [TOKEYS]
	public toArray(): A2I<[K, T]> { return A2.from(this.entries()); }//             [TOARRAY]
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




/**
 * // ---------------------------------------------------------------- child: one mode
const { spawnSync } = require('child_process');

const DURATION = Number(process.env.DURATION || 12);
const pid = 1, v = 1;
function child(mode) {
  const bus = new SIGNALS();
  const acc = new Int32Array(4);
  const keep = new Array(1024).fill(null); let kn = 0;
  let write;

  if (mode === 'trivial') {
    bus.sub((p) => p, 'k');
    write = () => { bus.send(v, 'k'); };
  } else if (mode === 'reads') {
    bus.sub((p, msg) => { acc[0] = (acc[0] + p + msg.k.length + msg.s.length) | 0; return p; }, 'k');
    write = () => { bus.send(v, 'k'); };
  } else if (mode === 'escape') {
    bus.sub((p, msg) => { keep[kn++ & 1023] = msg; return p; }, 'k');
    write = () => { bus.send(v, 'k'); };
  } else if (mode === 'multi8') {
    const hs = [(p) => p + 1, (p) => p + 2, (p) => p * 3, (p) => p - 4, (p) => p ^ 5, (p) => p | 6, (p) => p & 7, (p) => p + 8];
    const keys = hs.map((_, i) => 'k' + i);
    hs.forEach((h, i) => bus.sub(h, keys[i]));
    let i = 0;
    write = () => { bus.send(v, keys[i]); i = (i + 1) & 7; };
  } else if (mode === 'roundtrip') {
    // sub replies with a value derived from the message; caller consumes the reply
    bus.sub((p, msg) => p + msg.k.length, 'k');
    write = () => { acc[2] = (acc[2] + bus.send(v, 'k')) | 0; };
  } else if (mode === 'replyobj') {
    // sub returns a NEW message object; caller reads a field off the reply
    bus.sub((p, msg) => ({ id: msg.k, p: p + 1, ok: true }), 'k');
    write = () => { acc[2] = (acc[2] + bus.send(v, 'k').p) | 0; };
  } else if (mode === 'stream0') {
    bus.sub((p) => p, 'k');
    write = () => { bus.sendStream(v, 'k'); };
  } else if (mode === 'stream1') {
    bus.sub((p) => p, 'k');
    bus.stream((msg) => { acc[1] = (acc[1] + msg.p) | 0; }, 'k');
    write = () => { bus.sendStream(v, 'k'); };
  } else throw new Error('unknown mode ' + mode);

  const ns = (ms) => BigInt(Math.round(ms)) * 1000000n;
  const warmEnd = process.hrtime.bigint() + ns(0);
  while (process.hrtime.bigint() < warmEnd) for (let i = 0; i < 100000; i++) write();

  const wins = new Float64Array(100000); let nw = 0;
  const t0 = process.hrtime.bigint(), end = t0 + ns(DURATION * 1000);
  let wStart = t0, wOps = 0, total = 0, now = t0;
  do {
    for (let i = 0; i < 100000; i++) write();
    wOps += 100000; total += 100000;
    now = process.hrtime.bigint();
    if (now - wStart >= 1000000000n) { wins[nw++] = wOps / (Number(now - wStart) / 1e9); wOps = 0; wStart = now; }
  } while (now < end);
  const avg = total / (Number(now - t0) / 1e9);
  let peak = 0; for (let i = 0; i < nw; i++) if (wins[i] > peak) peak = wins[i];
  if (nw === 0) peak = avg;
  console.log(JSON.stringify({ mode, peak, avg, check: acc[0] + acc[1] + acc[2] + kn }));
}

// ---------------------------------------------------------------- parent
function parent() {
  const rt = typeof Bun !== 'undefined' ? 'bun ' + Bun.version : 'node ' + process.version;
  console.log(`${rt} | ${DURATION}s per run (+1s warmup) | each op = one send() that runs a handler\n`);
  console.log('mode'.padEnd(10), 'peak M/s'.padStart(10), 'avg M/s'.padStart(10));
  for (const mode of ['trivial', 'roundtrip', 'replyobj', 'reads', 'escape', 'multi8', 'stream0', 'stream1']) {
    const out = spawnSync(process.execPath, [__filename, mode], { env: process.env, encoding: 'utf8' });
    let j;
    try { j = JSON.parse(out.stdout.trim().split('\n').pop()); } catch (e) { console.log(mode, 'FAILED', out.stderr); continue; }
    console.log(mode.padEnd(10), (j.peak / 1e6).toFixed(0).padStart(10), (j.avg / 1e6).toFixed(0).padStart(10));
  }
}

if (process.argv.length >= 3) child(process.argv[2]); else parent();
 */
