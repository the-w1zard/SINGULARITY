type SIGNAL_KEY = string | number | symbol;
type SIGNAL_VALUE = unknown;
type SIGNAL_DISPOSE = () => void;
type SIGNAL_LISTENER<T = SIGNAL_VALUE> = (value: T, previous: T) => void;
interface SIGNAL_SUB<T = SIGNAL_VALUE> {
	fn: SIGNAL_LISTENER<T>;
	active: boolean;
}
interface SIGNAL_CELL<T = SIGNAL_VALUE> {
	value: T;
	version: number;
	subs: SIGNAL_SUB<T>[];
}
interface SIGNAL_EFFECT {
	fn: () => void;
	deps: SIGNAL_CELL[];
	active: boolean;
	running: boolean;
}
type SIGNAL_LEVEL<T> = Record<PropertyKey, T>;
const SIGNAL_EMPTY = Object.create(null) as SIGNAL_LEVEL<never>;
export class PALPATINE<R extends SIGNAL_KEY = string, M extends SIGNAL_KEY = string, S extends SIGNAL_KEY = string, K extends SIGNAL_KEY = string> {
	private readonly root: SIGNAL_LEVEL<SIGNAL_LEVEL<SIGNAL_LEVEL<SIGNAL_LEVEL<SIGNAL_CELL>>>> = Object.create(null);
	private activeEffect: SIGNAL_EFFECT | undefined;

	constructor(
		public readonly r: R,
		public readonly m: M,
		public readonly s: S,
		public readonly k: K,
	) {}

	private cell<T>(r: R, m: M, s: S, k: K, initial?: T): SIGNAL_CELL<T> {
		const root = this.root;
		let rNode = root[r] as SIGNAL_LEVEL<SIGNAL_LEVEL<SIGNAL_LEVEL<SIGNAL_CELL>>> | undefined;

		if (rNode === undefined) root[r] = rNode = Object.create(null);
		let mNode = rNode[m] as SIGNAL_LEVEL<SIGNAL_LEVEL<SIGNAL_CELL>> | undefined;

		if (mNode === undefined) rNode[m] = mNode = Object.create(null);
		let sNode = mNode[s] as SIGNAL_LEVEL<SIGNAL_CELL> | undefined;

		if (sNode === undefined) mNode[s] = sNode = Object.create(null);
		let cell = sNode[k] as SIGNAL_CELL<T> | undefined;

		if (cell === undefined) {
			cell = { value: initial as T, version: 0, subs: [] };
			sNode[k] = cell as SIGNAL_CELL;
		}

		return cell;
	}

	private track(cell: SIGNAL_CELL): void {
		const effect = this.activeEffect;
		if (effect === undefined || !effect.active || effect.running) return;

		const deps = effect.deps;
		for (let i = 0, n = deps.length; i < n; i++) {
			if (deps[i] === cell) return;
		}

		deps.push(cell);
		cell.subs.push({
			fn: effect.fn as SIGNAL_LISTENER,
			active: true,
		});
	}

	private notify<T>(cell: SIGNAL_CELL<T>, value: T, previous: T): void {
		const subs = cell.subs;
		for (let i = 0, n = subs.length; i < n; i++) {
			const sub = subs[i];
			if (sub.active) sub.fn(value, previous);
		}
	}

	get<T = SIGNAL_VALUE>(k: K, r: R = this.r, m: M = this.m, s: S = this.s): T {
		const cell = this.cell<T>(r, m, s, k);
		this.track(cell);
		return cell.value;
	}

	peek<T = SIGNAL_VALUE>(k: K, r: R = this.r, m: M = this.m, s: S = this.s): T {
		return this.cell<T>(r, m, s, k).value;
	}

	has(k: K, r: R = this.r, m: M = this.m, s: S = this.s): boolean {
		const rNode = this.root[r];
		if (rNode === undefined) return false;
		const mNode = rNode[m];
		if (mNode === undefined) return false;
		const sNode = mNode[s];
		return sNode !== undefined && sNode[k] !== undefined;
	}

	create<T>(k: K, initial: T, r: R = this.r, m: M = this.m, s: S = this.s): T {
		const rNode = this.root[r] ?? (this.root[r] = Object.create(null));
		const mNode = rNode[m] ?? (rNode[m] = Object.create(null));
		const sNode = mNode[s] ?? (mNode[s] = Object.create(null));

		if (sNode[k] === undefined) {
			sNode[k] = { value: initial, version: 0, subs: [] };
		}

		return (sNode[k] as SIGNAL_CELL<T>).value;
	}

	set<T>(k: K, value: T, r: R = this.r, m: M = this.m, s: S = this.s): T {
		const cell = this.cell<T>(r, m, s, k);
		const previous = cell.value;

		if (Object.is(previous, value)) return value;

		cell.value = value;
		cell.version++;
		this.notify(cell, value, previous);
		return value;
	}

	write<T>(k: K, value: T, r: R = this.r, m: M = this.m, s: S = this.s): T {
		return this.set(k, value, r, m, s);
	}

	swap<T>(k: K, update: (value: T) => T, r: R = this.r, m: M = this.m, s: S = this.s): T {
		const cell = this.cell<T>(r, m, s, k);
		const previous = cell.value;
		const value = update(previous);

		if (Object.is(previous, value)) return value;

		cell.value = value;
		cell.version++;
		this.notify(cell, value, previous);
		return value;
	}

	edit<T>(k: K, update: (value: T) => T, r: R = this.r, m: M = this.m, s: S = this.s): T {
		return this.swap(k, update, r, m, s);
	}

	delete(k: K, r: R = this.r, m: M = this.m, s: S = this.s): boolean {
		const rNode = this.root[r];
		if (rNode === undefined) return false;
		const mNode = rNode[m];
		if (mNode === undefined) return false;
		const sNode = mNode[s];
		if (sNode === undefined) return false;

		const cell = sNode[k];
		if (cell === undefined) return false;

		const previous = cell.value;
		delete sNode[k];
		cell.version++;
		this.notify(cell, undefined, previous);
		return true;
	}

	sub<T = SIGNAL_VALUE>(
		k: K,
		fn: SIGNAL_LISTENER<T>,
		r: R = this.r,
		m: M = this.m,
		s: S = this.s,
	): SIGNAL_DISPOSE {
		const cell = this.cell<T>(r, m, s, k);
		const sub: SIGNAL_SUB<T> = { fn, active: true };
		cell.subs.push(sub);

		return () => {
			if (!sub.active) return;
			sub.active = false;
		};
	}

	subMany<T = SIGNAL_VALUE>(
		keys: readonly K[],
		fn: (key: K, value: T, previous: T) => void,
		r: R = this.r,
		m: M = this.m,
		s: S = this.s,
	): SIGNAL_DISPOSE {
		const disposers = new Array<SIGNAL_DISPOSE>(keys.length);

		for (let i = 0; i < keys.length; i++) {
			const key = keys[i];
			disposers[i] = this.sub<T>(
				key,
				(value, previous) => fn(key, value, previous),
				r,
				m,
				s,
			);
		}

		return () => {
			for (let i = 0; i < disposers.length; i++) disposers[i]();
		};
	}

	subAll(
		fn: (key: K, value: SIGNAL_VALUE, previous: SIGNAL_VALUE) => void,
		r: R = this.r,
		m: M = this.m,
		s: S = this.s,
	): SIGNAL_DISPOSE {
		const rNode = this.root[r];
		const mNode = rNode?.[m];
		const sNode = mNode?.[s];
		if (sNode === undefined) return () => {};

		const disposers: SIGNAL_DISPOSE[] = [];
		for (const key in sNode) {
			const cell = sNode[key] as SIGNAL_CELL;
			const sub: SIGNAL_SUB = {
				fn: (value, previous) => fn(key as K, value, previous),
				active: true,
			};
			cell.subs.push(sub);
			disposers.push(() => {
				sub.active = false;
			});
		}

		return () => {
			for (let i = 0; i < disposers.length; i++) disposers[i]();
		};
	}

	effect(fn: () => void): SIGNAL_DISPOSE {
		const effect: SIGNAL_EFFECT = {
			fn,
			deps: [],
			active: true,
			running: false,
		};

		const run = (): void => {
			if (!effect.active || effect.running) return;

			effect.running = true;

			const deps = effect.deps;
			for (let i = 0; i < deps.length; i++) {
				const subs = deps[i].subs;
				for (let j = 0; j < subs.length; j++) {
					if (subs[j].fn === run) subs[j].active = false;
				}
			}
			deps.length = 0;

			const previous = this.activeEffect;
			this.activeEffect = effect;

			try {
				fn();
			} finally {
				this.activeEffect = previous;
				effect.running = false;
			}
		};

		effect.fn = run;
		run();

		return () => {
			if (!effect.active) return;
			effect.active = false;

			const deps = effect.deps;
			for (let i = 0; i < deps.length; i++) {
				const subs = deps[i].subs;
				for (let j = 0; j < subs.length; j++) {
					if (subs[j].fn === run) subs[j].active = false;
				}
			}
			deps.length = 0;
		};
	}

	signal<T>(k: K, initial: T, r: R = this.r, m: M = this.m, s: S = this.s) {
		this.create(k, initial, r, m, s);

		const store = this;
		return {
			get value(): T {
				return store.get<T>(k, r, m, s);
			},
			set value(value: T) {
				store.set(k, value, r, m, s);
			},
			get current(): T {
				return store.get<T>(k, r, m, s);
			},
			set current(value: T) {
				store.set(k, value, r, m, s);
			},
			get peek(): T {
				return store.peek<T>(k, r, m, s);
			},
			set: (value: T): T => store.set(k, value, r, m, s),
			update: (fn: (value: T) => T): T => store.swap(k, fn, r, m, s),
			sub: (fn: SIGNAL_LISTENER<T>): SIGNAL_DISPOSE => store.sub(k, fn, r, m, s),
		};
	}

	atom<T>(k: K, initial: T, r: R = this.r, m: M = this.m, s: S = this.s) {
		return this.signal(k, initial, r, m, s);
	}

	getMany<T = SIGNAL_VALUE>(keys: readonly K[], r: R = this.r, m: M = this.m, s: S = this.s): T[] {
		const result = new Array<T>(keys.length);
		for (let i = 0; i < keys.length; i++) {
			result[i] = this.get<T>(keys[i], r, m, s);
		}
		return result;
	}

	setMany<T>(keys: readonly K[], values: readonly T[], r: R = this.r, m: M = this.m, s: S = this.s): void {
		for (let i = 0, n = keys.length; i < n; i++) {
			this.set(keys[i], values[i], r, m, s);
		}
	}

	clear(r: R = this.r, m: M = this.m, s: S = this.s): void {
		const rNode = this.root[r];
		const mNode = rNode?.[m];
		const sNode = mNode?.[s];
		if (sNode === undefined) return;

		for (const key in sNode) {
			const cell = sNode[key];
			const previous = cell.value;
			delete sNode[key];
			cell.version++;
			this.notify(cell, undefined, previous);
		}
	}

	*entries<T = SIGNAL_VALUE>(r: R = this.r, m: M = this.m, s: S = this.s): IterableIterator<[K, T]> {
		const rNode = this.root[r];
		const mNode = rNode?.[m];
		const sNode = mNode?.[s];
		if (sNode === undefined) return;

		for (const key in sNode) {
			yield [key as K, (sNode[key] as SIGNAL_CELL<T>).value];
		}
	}
}
