type NGIN_KEY = string;
type NGIN_VALUE = unknown;
type NGIN_DISPOSE = () => void;
type NGIN_HANDLER<T = NGIN_VALUE> = (value: T, previous: T | undefined, key: NGIN_KEY) => void;
type NGIN_EVENT<T = NGIN_VALUE> = {
	op: "set" | "delete" | "clear";
	k: NGIN_KEY;
	v?: T;
	prev?: T;
	ts: number;
};
type NGIN_ATOM<T> = {
	readonly value: T;
	readonly current: T;
	readonly crt: T;
	set value(v: T);
	set current(v: T);
	set crt(v: T);
	set set(v: T);
	get(): T;
	peek(): T;
	update(fn: (value: T) => T): T;
	sub(fn: NGIN_HANDLER<T>): NGIN_DISPOSE;
	dispose(): void;
};
type NGIN_SIGNAL<T> = {
	readonly value: T;
	readonly current: T;
	peek(): T;
	set(value: T): T;
	update(fn: (value: T) => T): T;
	sub(fn: NGIN_HANDLER<T>): NGIN_DISPOSE;
};
type NGIN_OPTIONS<T> = {
	initial?: T;
	equals?: (a: T, b: T) => boolean;
	persist?: boolean;
};
type NGIN_STORAGE = {
	getItem(key: string): string | null;
	setItem(key: string, value: string): void;
	removeItem(key: string): void;
};
export class JEDI<T = NGIN_VALUE> {
	public readonly bus: SIGNALS;
	public readonly s: string;
	public readonly m: string;
	public readonly r: string;
	public readonly k: string;

	public readonly _store: Record<string, T>;
	public readonly _listeners: Record<string, NGIN_DISPOSE>;
	public readonly _channels: Record<string, boolean>;
	public readonly _waiters: Record<string, Set<(value: T) => void>>;
	public readonly _persistKeys: Record<string, true>;
	public readonly _persistent: Record<string, true>;

	private readonly _equals: (a: T, b: T) => boolean;
	private readonly _storage?: NGIN_STORAGE;
	private readonly _prefix: string;
	private _all?: NGIN_DISPOSE;
	private _broadcast?: (event: NGIN_EVENT<T>) => void;

	public readonly get: {
		<K extends string>(key: K): T | undefined;
		hot<K extends string>(key: K): T | undefined;
	};

	public readonly getMany: (keys: Iterable<string>) => Record<string, T>;
	public readonly set: (key: string, value: T | ((previous: T | undefined) => T)) => T;
	public readonly setMany: (values: Readonly<Record<string, T>>) => this;
	public readonly delete: (key: string) => boolean;
	public readonly deleteMany: (keys: Iterable<string>) => number;
	public readonly has: (key: string) => boolean;
	public readonly read: (key: string) => T | undefined;
	public readonly write: (key: string, value: T) => T;
	public readonly peek: (key: string) => T | undefined;
	public readonly swap: (key: string, value: T | ((previous: T | undefined) => T)) => T | undefined;
	public readonly edit: (key: string, fn: (previous: T | undefined) => T) => T;
	public readonly create: (key: string, initial: T | (() => T)) => T;
	public readonly clear: () => void;
	public readonly entries: () => IterableIterator<[string, T]>;
	public readonly getAll: () => Record<string, T>;
	public readonly getByPrefix: (prefix: string) => Record<string, T>;

	public readonly sub: {
		(key: string, handler: NGIN_HANDLER<T>): NGIN_DISPOSE;
		(handler: (event: NGIN_EVENT<T>) => void): NGIN_DISPOSE;
		hot(key: string, handler: NGIN_HANDLER<T>): NGIN_DISPOSE;
	};

	public readonly subMany: (keys: Iterable<string>, handler: NGIN_HANDLER<T>) => NGIN_DISPOSE;
	public readonly subAll: (handler: (event: NGIN_EVENT<T>) => void) => NGIN_DISPOSE;
	public readonly waitFor: (key: string, predicate?: (value: T) => boolean) => Promise<T>;
	public readonly persist: (key: string) => this;
	public readonly unpersist: (key: string) => this;

	public readonly signal: <V = T>(key: string, options?: NGIN_OPTIONS<V>) => NGIN_SIGNAL<V>;
	public readonly atom: <V = T>(key: string, options?: NGIN_OPTIONS<V>) => NGIN_ATOM<V>;

	constructor(
		bus: SIGNALS,
		options: {
			s?: string;
			m?: string;
			r?: string;
			k?: string;
			prefix?: string;
			storage?: NGIN_STORAGE;
			broadcast?: (event: NGIN_EVENT<T>) => void;
			equals?: (a: T, b: T) => boolean;
		} = {},
	) {
		this.bus = bus;
		this.s = options.s ?? bus.s;
		this.m = options.m ?? bus.m;
		this.r = options.r ?? bus.r;
		this.k = options.k ?? bus.k;
		this._prefix = options.prefix ?? "";
		this._storage = options.storage;
		this._broadcast = options.broadcast;
		this._equals = options.equals ?? Object.is;

		this._store = bus.getS("_store", this.m, this.r) as Record<string, T>;
		this._listeners = bus.getS("_listeners", this.m, this.r) as Record<string, NGIN_DISPOSE>;
		this._channels = bus.getS("_channels", this.m, this.r) as Record<string, boolean>;
		this._waiters = bus.getS("_waiters", this.m, this.r) as Record<string, Set<(value: T) => void>>;
		this._persistKeys = bus.getS("_persistKeys", this.m, this.r) as Record<string, true>;
		this._persistent = bus.getS("_persistent", this.m, this.r) as Record<string, true>;

		const get = (key: string): T | undefined => this._store[this._key(key)];
		const set = (key: string, value: T | ((previous: T | undefined) => T)): T => {
			const k = this._key(key);
			const previous = this._store[k];
			const next = typeof value === "function"
				? (value as (previous: T | undefined) => T)(previous)
				: value;

			if (this._equals(previous as T, next) && Object.prototype.hasOwnProperty.call(this._store, k)) {
				return next;
			}

			this._store[k] = next;
			this._emit(k, "set", next, previous);
			return next;
		};

		this.get = Object.assign(get, { hot: get });
		this.getMany = (keys) => {
			const result = Object.create(null) as Record<string, T>;
			for (const key of keys) {
				const k = this._key(key);
				if (Object.prototype.hasOwnProperty.call(this._store, k)) result[key] = this._store[k];
			}
			return result;
		};

		this.set = set;
		this.setMany = (values) => {
			for (const key of Object.keys(values)) set(key, values[key]);
			return this;
		};

		this.delete = (key) => {
			const k = this._key(key);
			if (!Object.prototype.hasOwnProperty.call(this._store, k)) return false;
			const previous = this._store[k];
			delete this._store[k];
			this._emit(k, "delete", undefined, previous);
			return true;
		};

		this.deleteMany = (keys) => {
			let count = 0;
			for (const key of keys) if (this.delete(key)) count++;
			return count;
		};

		this.has = (key) => Object.prototype.hasOwnProperty.call(this._store, this._key(key));
		this.read = get;
		this.write = (key, value) => set(key, value);
		this.peek = get;

		this.swap = (key, value) => {
			const previous = get(key);
			set(key, value);
			return previous;
		};

		this.edit = (key, fn) => set(key, fn(get(key)));
		this.create = (key, initial) => {
			if (this.has(key)) return get(key) as T;
			return set(key, typeof initial === "function" ? (initial as () => T)() : initial);
		};

		this.clear = () => {
			for (const key of Object.keys(this._store)) this.delete(this._externalKey(key));
		};

		this.entries = () => {
			const entries = Object.create(null) as Record<string, T>;
			for (const key of Object.keys(this._store)) {
				if (key.startsWith(this._prefix)) entries[this._externalKey(key)] = this._store[key];
			}
			return Object.entries(entries) as IterableIterator<[string, T]>;
		};

		this.getAll = () => {
			const result = Object.create(null) as Record<string, T>;
			for (const key of Object.keys(this._store)) {
				if (key.startsWith(this._prefix)) result[this._externalKey(key)] = this._store[key];
			}
			return result;
		};

		this.getByPrefix = (prefix) => {
			const result = Object.create(null) as Record<string, T>;
			for (const key of Object.keys(this._store)) {
				const external = this._externalKey(key);
				if (external.startsWith(prefix)) result[external] = this._store[key];
			}
			return result;
		};

		const subscribe = (key: string, handler: NGIN_HANDLER<T>): NGIN_DISPOSE => {
			const channel = this._channel(key);
			const dispose = this.bus.stream((msg) => {
				const event = msg.p as NGIN_EVENT<T>;
				if (event.op === "set") handler(event.v as T, event.prev, key);
				else handler(undefined as T, event.prev, key);
			}, channel, this.s, this.m, this.r);

			this._listeners[channel] = dispose;
			return () => {
				dispose();
				delete this._listeners[channel];
			};
		};

		this.sub = Object.assign(
			((keyOrHandler: string | ((event: NGIN_EVENT<T>) => void), handler?: NGIN_HANDLER<T>) => {
				if (typeof keyOrHandler === "function") return this.subAll(keyOrHandler);
				return subscribe(keyOrHandler, handler!);
			}) as {
				(key: string, handler: NGIN_HANDLER<T>): NGIN_DISPOSE;
				(handler: (event: NGIN_EVENT<T>) => void): NGIN_DISPOSE;
				hot(key: string, handler: NGIN_HANDLER<T>): NGIN_DISPOSE;
			},
			{ hot: subscribe },
		);

		this.subMany = (keys, handler) => {
			const disposers: Record<string, NGIN_DISPOSE> = Object.create(null);
			for (const key of keys) disposers[key] = subscribe(key, handler);
			return () => {
				for (const key of Object.keys(disposers)) disposers[key]();
			};
		};

		this.subAll = (handler) => {
			const channel = this._channel("*");
			const dispose = this.bus.stream((msg) => handler(msg.p as NGIN_EVENT<T>), channel, this.s, this.m, this.r);
			return dispose;
		};

		this.waitFor = (key, predicate = () => true) => {
			const current = get(key);
			if (current !== undefined && predicate(current)) return Promise.resolve(current);

			return new Promise<T>((resolve) => {
				const dispose = subscribe(key, (value) => {
					if (!predicate(value)) return;
					dispose();
					resolve(value);
				});
			});
		};

		this.persist = (key) => {
			const k = this._key(key);
			this._persistKeys[k] = true;
			this._persistent[k] = true;

			if (this._storage) {
				const stored = this._storage.getItem(k);
				if (stored !== null) {
					try {
						const value = JSON.parse(stored) as T;
						const previous = this._store[k];
						this._store[k] = value;
						this._emit(k, "set", value, previous);
					} catch {
						// Invalid persisted data is left untouched.
					}
				} else if (Object.prototype.hasOwnProperty.call(this._store, k)) {
					this._storage.setItem(k, JSON.stringify(this._store[k]));
				}
			}

			return this;
		};

		this.unpersist = (key) => {
			const k = this._key(key);
			delete this._persistKeys[k];
			delete this._persistent[k];
			this._storage?.removeItem(k);
			return this;
		};

		this.signal = <V = T>(key: string, signalOptions: NGIN_OPTIONS<V> = {}) => {
			if (signalOptions.initial !== undefined && !this.has(key)) {
				this.set(key, signalOptions.initial as unknown as T);
			}

			const equals = signalOptions.equals ?? Object.is;
			const signal: NGIN_SIGNAL<V> = {
				get value() { return this.get(); },
				get current() { return this.get(); },
				get: () => this.get(key) as V,
				peek: () => this.peek(key) as V,
				set: (value) => {
					const previous = this.get(key) as V;
					if (equals(previous, value)) return value;
					this.set(key, value as unknown as T);
					return value;
				},
				update: (fn) => signal.set(fn(signal.get())),
				sub: (fn) => this.sub(key, fn as NGIN_HANDLER<T>),
			};
			return signal;
		};

		this.atom = <V = T>(key: string, atomOptions: NGIN_OPTIONS<V> = {}) => {
			const signal = this.signal<V>(key, atomOptions);
			let disposed = false;

			const atom = {
				get value() { return signal.get(); },
				get current() { return signal.get(); },
				get crt() { return signal.get(); },
				set value(value: V) { signal.set(value); },
				set current(value: V) { signal.set(value); },
				set crt(value: V) { signal.set(value); },
				set set(value: V) { signal.set(value); },
				get: () => signal.get(),
				peek: () => signal.peek(),
				update: (fn: (value: V) => V) => signal.update(fn),
				sub: (fn: NGIN_HANDLER<V>) => signal.sub(fn),
				dispose: () => { disposed = true; },
			} as NGIN_ATOM<V>;

			if (atomOptions.persist) this.persist(key);
			if (disposed) return atom;
			return atom;
		};
	}

	private _key(key: string): string {
		return this._prefix + key;
	}

	private _externalKey(key: string): string {
		return key.slice(this._prefix.length);
	}

	private _channel(key: string): string {
		return this.k + ":" + this._key(key);
	}

	private _emit(key: string, op: NGIN_EVENT<T>["op"], value?: T, previous?: T): void {
		const event: NGIN_EVENT<T> = { op, k: key, v: value, prev: previous, ts: Date.now() };
		const channel = this._channel(key);

		if (!this._channels[channel]) {
			this.bus.sub((_payload, _msg) => undefined, channel, this.s, this.m, this.r);
			this._channels[channel] = true;
		}

		this.bus.sendStream(event, channel, this.s, this.m, this.r);
		this.bus.sendStream(event, this._channel("*"), this.s, this.m, this.r);

		const waiters = this._waiters[key];
		if (waiters && value !== undefined) {
			for (const resolve of waiters) resolve(value);
			delete this._waiters[key];
		}

		if (this._persistKeys[key] && this._storage) {
			if (op === "delete") this._storage.removeItem(key);
			else this._storage.setItem(key, JSON.stringify(value));
		}

		this._broadcast?.(event);
	}

	public bindDOM<V>(
		key: string,
		target: Node,
		render: (value: V, target: Node) => void = ((value, node) => {
			if (node instanceof Text) node.textContent = String(value ?? "");
			else if (node instanceof HTMLElement) node.textContent = String(value ?? "");
		}) as (value: V, target: Node) => void,
	): NGIN_DISPOSE {
		render(this.get(key) as V, target);
		return this.sub(key, (value) => render(value as V, target));
	}

	public bindText(key: string, target: Text): NGIN_DISPOSE {
		return this.bindDOM(key, target, (value, node) => {
			(node as Text).textContent = String(value ?? "");
		});
	}

	public bindAttribute(key: string, target: Element, attribute: string): NGIN_DISPOSE {
		const render = (value: T | undefined) => {
			if (value === null || value === undefined || value === false) target.removeAttribute(attribute);
			else target.setAttribute(attribute, value === true ? "" : String(value));
		};

		render(this.get(key));
		return this.sub(key, render);
	}
}
