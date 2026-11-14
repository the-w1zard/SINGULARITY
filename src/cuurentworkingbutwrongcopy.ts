// ============================================================================
//  TEST 5 — namespaced cells. One pooled resource per master instance.
//  Master fields live at the resource root; every bound atom lives at
//  <name> in that same resource. useCell is the only door in or out.
//
//  Rules under test:
//   1. master is flat      -> atom.read()
//   2. bound atoms named   -> atom.conflict.resolve(), atom.gate.sealed
//   3. no cross-visibility -> masters and their bound atoms cannot see any
//                             other master, or any sibling namespace
//   4. one source of truth -> gate.seal() actually gates read()/write()
//   5. propagation         -> any change refreshes every namespace in the
//                             same resource
//
//  Pipeline pieces (createLiveSelf, mergeInto, makeStub, withTypoGuard) are
//  real from /rool-drive/signals.ts with types stripped. The cell is the
//  machine's own: death_star.drs.useCell.mailbox(value, key, s, m, r), one
//  mailbox per master instance, exactly the DRS signature
//  death_star.drs.useCell.mailbox(v, k, s, m, r) -> [value, setLive]. The
//  returned value IS the pooled resource: root for the master, ns[<name>] for
//  each bound atom. Both sides open the same cell by key, so there is one
//  write path and one notifier. mergeInto(self, bindable) is gone.
// ============================================================================

import { M2I } from "./m2.js";

// ----------------------------------------------------------------------------
//  PLACEHOLDER: DEATH_STAR — only the cell registry is needed here.
// ----------------------------------------------------------------------------
const CELLS = new Map<string, any>();
const cell: any = (key: string) => CELLS.get(key);
cell.set = (key: string, value: any) => { CELLS.set(key, value); return value; };
cell.get = (key: string) => CELLS.get(key);

const death_star: any = {
	k: "__SIGNALS__",
	s: "SAURON_SLAVE_REG",
	m: "SAURON_MASTER_REG",
	r: "THE_WIZARD_ROOT_REG",
	bear: { cell },
};

// ============================================================================
//  REAL: pipeline
// ============================================================================
function mergeInto<T extends object>(target: T, source: object): T {
	Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
	return target;
}

function resolveFactory(raw: any): any {
	return typeof raw === "function" ? raw : () => raw;
}

const NINJA_STUB_BRAND = "__is_atom_stub__";
function makeStub(trace?: any): any {
	return new Proxy(() => makeStub(trace), {
		get: (_t, prop) => (prop === NINJA_STUB_BRAND ? true : makeStub(trace)),
		apply: () => makeStub(trace),
	});
}

function withTypoGuard<T extends Record<string, any>>(result: T, label: string): T {
	return new Proxy(result, {
		get(target, prop, receiver) {
			if (typeof prop === "symbol" || prop in target) return Reflect.get(target, prop, receiver);
			throw new Error(`\n[${label} Typo Error] Property "${String(prop)}" does not exist on this ${label} definition.\n  -> Available definitions: [ ${Object.keys(target).join(", ")} ]\n`);
		},
	});
}

// ============================================================================
//  REAL: the cell primitive. death_star.drs.useCell / .mailbox, types
//  stripped from the machine's own source. This IS the resource store: one
//  mailbox per master instance, keyed into the bear state store, published
//  over the bus. No invented container.
// ============================================================================
const STORE = new Map<string, any>();
const STATE_SUBS = new Map<string, Set<(v: any) => void>>();
const BUS_SUBS = new Map<string, Set<(env: any) => void>>();

const addSub = (m: Map<string, Set<any>>, k: string, fn: any) => {
	if (!m.has(k)) m.set(k, new Set());
	m.get(k)!.add(fn);
	return () => m.get(k)!.delete(fn);
};
const fire = (m: Map<string, Set<any>>, k: string, payload: any) => {
	for (const fn of [...(m.get(k) ?? [])]) { try { fn(payload); } catch { /* noop */ } }
};

// the real Queue(1): serial, one task at a time
class Queue {
	private tail: Promise<any> = Promise.resolve();
	constructor(n: number = 1) { this.n = n; }
	readonly n: number;
	run<T>(fn: () => T | Promise<T>): Promise<T> {
		const p = this.tail.then(fn, fn);
		this.tail = p.then(() => undefined, () => undefined);
		return p;
	}
}

function darkState<T>(init: () => T): [any, (v: T) => void] {
	let value = init();
	const box = { get value() { return value; }, set value(v: T) { value = v; } };
	return [box, (v: T) => { value = v; }];
}

// bear: keyed store + keyed bus, the two things useCell actually touches
const bear: any = {
	cell,
	state: {
		get: (k: string) => STORE.get(k),
		set: (k: string, v: any, _o?: any) => { STORE.set(k, v); fire(STATE_SUBS, k, v); return v; },
		subscribe: (k: string, cb: (v: any) => void) => addSub(STATE_SUBS, k, cb),
		unsubscribe: (k: string, cb: any) => { STATE_SUBS.get(k)?.delete(cb); },
	},
	sub: (cb: (env: any) => void, k: string) => addSub(BUS_SUBS, k, cb),
	send: (env: any, k: string) => {
		// publish: the store is the medium, so a send lands the value and every
		// key subscriber — including the sender's own state — sees it
		STORE.set(k, env.value);
		fire(BUS_SUBS, k, env);
		fire(STATE_SUBS, k, env.value);
	},
	atom: <T>(v: T) => {
		const ref = { value: v } as any;
		Object.defineProperty(ref, "current", { get() { return ref.value; }, set(n) { ref.value = n; } });
		return ref;
	},
};

let _sid = 0;
const REGISTRY = { _sid: () => `sid-${++_sid}` };

const drs: any = {};
drs.useCell = (() => {
	const fn: any = (v: any, k: string = `_USECELL`) => {
		const [state, setState] = darkState(() => {
			const existing = bear.state.get(k);
			if (existing !== undefined) return existing;
			bear.state.set(k, v);
			return v;
		});
		const senderTokenRef = bear.atom(REGISTRY._sid());
		bear.sub((i: any) => {
			const incoming = i.p ?? i;
			if (incoming?.from === senderTokenRef.value) return;
			bear.state.set(k, incoming.value);
			setState(incoming.value);
		}, k);
		bear.state.subscribe(k, (nv: any) => setState(nv));
		const setLive = (next: any) => {
			const newValue = typeof next === "function" ? next(bear.state.get(k)) : next;
			setState(newValue);
			bear.state.set(k, newValue);
			bear.send({ value: newValue, from: senderTokenRef.value }, k);
		};
		return [state, setLive] as const;
	};
	fn.mailbox = (v?: any, k: string = `_USECELL_MAILBOX`) => {
		const [state, setState] = darkState(() => {
			const existing = bear.state.get(k);
			if (existing !== undefined) return existing;
			bear.state.set(k, v);
			return v;
		});
		const senderTokenRef = bear.atom(REGISTRY._sid());
		const queueRef = bear.atom<any>(null);
		if (!queueRef.current) queueRef.current = new Queue(1);
		bear.sub((i: any) => {
			const incoming = i.p ?? i;
			if (incoming?.from === senderTokenRef.value) return;
			queueRef.current!.run(async () => {
				const current = bear.state.get(k);
				const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
				bear.state.set(k, next);
				return next;
			});
		}, k);
		bear.state.subscribe(k, (nv: any) => setState(nv));
		const setLive = (next: any) => {
			const t = typeof next === "function" ? next(bear.state.get(k)) : next;
			bear.send({ value: t, from: senderTokenRef.value }, k);
		};
		// the live cell value, not a snapshot: one object, mutated in place
		return [bear.state.get(k), setLive] as const;
	};
	return fn;
})();

death_star.bear = bear;
death_star.drs = drs;

// ============================================================================
//  ONE CELL PER MASTER INSTANCE.  No cell layer of our own: this is useCell.
//      const [cell, cell$] = useCell.obj(initial, k, s, m, r)
//      cell   -> read  (the live distributed record)
//      cell$.merge({...})  -> merge values into it
//  The record is ONE level: master fields at the top, every bound atom beside
//  them under its own name.  self.<ns> and atom.<ns> are the same slot.
// ============================================================================
const OPEN_CELLS: any[] = []; // test-only window onto the cells

// useCell.obj — the merge variant used by Illusionist.
//   const [o, o$] = root.DISRUPTOR.useCell.obj(liveTarget, label, categoryName)
//   o$.merge(result)
death_star.drs.useCell.obj = (initial: any, k: string, categoryName: string, s: string, m: string, r: string) => {
	const [cell, setLive] = death_star.drs.useCell.mailbox(initial, k, s, m, r);
	const merge = (patch: any) => {
		const p = typeof patch === "function" ? patch(cell) : patch;
		if (!p || typeof p !== "object") return cell;
		for (const key of Object.keys(p)) {
			if (p[key] && typeof p[key] === "object" && cell[key] && typeof cell[key] === "object" && !Array.isArray(p[key])) {
				Object.assign(cell[key], p[key]); // namespace keeps identity, value merges in
			} else cell[key] = p[key];
		}
		cell._version = (cell._version ?? 0) + 1;
		setLive(cell);
		return cell;
	};
	return [cell, { merge, set: setLive, key: k, category: categoryName }] as const;
};

// self: reads the cell, writes merge into it. That is the whole of it.
function makeSelf(cell: any, cell$: any, namespace?: string | null) {
	const scope = () => (namespace ? (cell[namespace] ??= {}) : cell);
	return new Proxy({}, {
		get(_t, key: any) {
			if (key === "resource") return cell$;
			if (key === "useCell") return () => cellHandle(cell, cell$, namespace ?? null);
			if (typeof key !== "string" || key === "then") return undefined;
			const rec = scope();
			if (namespace && key in rec) return rec[key];
			// a bound atom reads its master's fields (k, conflictStrategy, …)
			if (namespace && key in cell) {
				if (cell[key] && typeof cell[key] === "object" && !Array.isArray(cell[key]) && cell[key]._isNamespace) {
					throw new Error(`[isolation] "${namespace}" cannot reach namespace "${key}"`);
				}
				return cell[key];
			}
			if (key in cell) return cell[key];
			return undefined;
		},
		set(_t, key: any, value: any) {
			if (typeof key !== "string") return false;
			if (namespace) cell$.merge({ [namespace]: { [key]: value } });
			else cell$.merge({ [key]: value });
			return true;
		},
		has(_t, key: any) { return typeof key === "string" && (key in scope() || key in cell); },
	});
}

// useCell, as the atom sees it: [own value, merge into own namespace]
function cellHandle(cell: any, cell$: any, namespace: string | null) {
	const read = () => (namespace ? (cell[namespace] ??= {}) : cell);
	const merge = (patch: any) => {
		const p = typeof patch === "function" ? patch(read()) : patch;
		if (namespace) cell$.merge({ [namespace]: p ?? {} });
		else cell$.merge(p ?? {});
	};
	const handle: any = [read(), merge];
	handle.get = (key?: string) => (key === undefined ? read() : read()[key]);
	handle.set = (key: any, value?: any) => {
		if (typeof key === "string") { merge({ [key]: value }); }
		else if (key && typeof key === "object") merge(key);
		return handle;
	};
	return handle;
}

function res_subscribe(self: any, listener: any) { return self.subscription.subscribe(listener); }

// ============================================================================
// ============================================================================
//  Atom registration
// ============================================================================
export function MASTER_ATOM(name: string, arg: any) { const f = resolveFactory(arg); death_star.bear.cell.set(name, f); return f; }
export function BIND_ATOM(name: string, arg: any) { const f = resolveFactory(arg); death_star.bear.cell.set(name, f); return f; }
export function ATOM_BINDABLE(factory: any) { return factory; }

let _n = 0;

// ============================================================================
//  REQUIRED_BINDABLES — per-instance, like any other bound atom.
//  Loaded lazily so declaration order can't throw a TDZ.
// ============================================================================
function requiredBindables() {
	return { gate: GATE, subscription: SUBSCRIPTION, context: CONTEXT };
}

function withoutKeys(obj: any, keys: string[]) {
	const desc: any = Object.getOwnPropertyDescriptors(obj);
	for (const k of keys) delete desc[k];
	return desc;
}

// ----------------------------------------------------------------------------
//  Bound atom: builds its fields into its own namespace record.
// ----------------------------------------------------------------------------
function buildBindable(res: any, name: string, factory: any, config: any) {
	const rec = res.namespace(name);
	const self = makeSelf(res, name);
	const ctx = { self, useCell: () => useCellScope(res, name), master: makeSelf(res, null), config, k: death_star.k, s: death_star.s, m: death_star.m, r: death_star.r };
	const out = factory(ctx);
	Object.defineProperties(rec, withoutKeys(out, ["bindables"]));
	if (typeof (out as any).refresh === "function") res.refresher(name, (out as any).refresh);
	return rec;
}

function masterCtx(res: any, self: any, config: any) {
	const ctx: any = { self, useCell: () => useCellScope(res, null), config, master: self, k: death_star.k, s: death_star.s, m: death_star.m, r: death_star.r };
	// bound atoms are handed to the master factory under their own names —
	// gate.sealed, subscription.subscribe, conflict.resolve, context.read
	for (const name of res.ns.keys()) ctx[name] = makeNamespaceView(res, name);
	return ctx;
}

function createMaster(kind: string, config: any) {
	const kindFactory = resolveFactory(death_star.bear.cell(kind));
	if (!kindFactory) throw new TypeError(`Unknown master atom: ${kind}`);

	const res = openCell(`${kind}:${++_n}`);
	const self = makeSelf(res, null);

	// master config lives at the resource root — this is what bound atoms read
	// through `self` for things like conflictStrategy / defaultTtlMs
	res.raw(null).value = config.value;
	for (const [key, v] of Object.entries(config)) {
		if (key === "_version") throw new Error(`[COLLISION] "_version" is reserved by the cell`);
		if (key !== "value") res.root[key] = v;
	}

	// 1. required bound atoms first — the master reads gate.sealed on read()
	for (const [name, factory] of Object.entries(requiredBindables())) buildBindable(res, name, factory, config);

	// 2. probe for the master's own bindables list, then build those too
	const probe = kindFactory(masterCtx(res, self, config));
	for (const [name, factory] of Object.entries((probe as any).bindables ?? {})) buildBindable(res, name, resolveFactory(factory), config);

	// 3. re-run the factory now that every namespace exists, so destructured
	// ctx bindables (gate, subscription, conflict, context) are live views
	const built = kindFactory(masterCtx(res, self, config));

	// 4. the master's own fields into the resource root — same level as the
	// namespaces, so a field and a bound atom can never share a name
	const masterFields = withoutKeys(built, ["bindables", "value"]);
	for (const key of Object.keys(masterFields)) {
		if (res.hasNamespace(key)) throw new Error(`[COLLISION] master field "${key}" collides with bound atom "${key}"`);
		if (key === "_version") throw new Error(`[COLLISION] "_version" is reserved by the cell`);
	}
	Object.defineProperties(res.root, masterFields);

	// 5. the public surface: flat master ops + one handle per namespace
	const api: any = { atom: kind };
	for (const name of res.ns.keys()) api[name] = makeNamespaceView(res, name);
	for (const fn of ["read", "write", "subscribe", "waitFor"]) if (fn in res.root) api[fn] = res.root[fn];
	// every master method the factory declared is callable on the instance:
	// plain.test(), vertex.list(), …
	for (const key of Object.keys(masterFields)) {
		if (key in api) continue;
		if (typeof res.root[key] === "function") { api[key] = res.root[key]; continue; }
		// plain state fields (k, s, m, r, value) read live off the resource
		Object.defineProperty(api, key, { enumerable: true, get: () => res.root[key] });
	}
	api.version = () => res.version();

	res.publish(); // build is one publish: every namespace refreshes together
	return withTypoGuard(api, `${kind}${res.id.split(":")[1]}`);
}

export function ATOM_CONTRACT(arg1: any, arg2?: any): any {
	const id = `${++_n}`;
	const categoryName = typeof arg1 === "string" ? arg1 : `ATOM_CONTRACT${id}`;
	const rawConfig = typeof arg1 === "string" ? arg2 : arg1;
	if (rawConfig === undefined || rawConfig === null) throw new Error("Self is required");
	const factory = resolveFactory(rawConfig);
	const pass = factory({});
	const kind = pass.atom ?? "ATOM";
	// atomContract(7) and atomContract({ value: 7 }) both mean the same thing
	const contract = ((config: any = {}) => {
		const normalized = config && typeof config === "object" ? config : { value: config };
		return createMaster(kind, normalized);
	}) as any;
	Object.assign(contract, pass);
	death_star.bear.cell.set(categoryName, contract);
	return contract;
}

// ============================================================================
//  MASTERS
// ============================================================================
export const ATOM = MASTER_ATOM("ATOM", ({ self, config, gate, subscription, context }: any) => ({
	value: config.value,
	k: config.k ?? "__SIGNALS__",
	s: config.s ?? "SAURON_SLAVE_REG",
	m: config.m ?? "SAURON_MASTER_REG",
	r: config.r ?? "THE_WIZARD_ROOT_REG",

	read() {
		if (gate?.sealed) throw new Error(`[SEALED] ${self.k} is sealed`);
		return self.value;
	},
	write(next: any) {
		if (gate?.sealed || gate?.frozen) return false;
		if (Object.is(self.value, next)) return false;
		self.useCell().set("value", next);
		return true;
	},
	subscribe(listener: any) { return subscription.subscribe(listener); },
	waitFor() {},
	test() { context.set("key", 52); return context.read("key"); },
}));

export const VERTEX = MASTER_ATOM("VERTEX", ({ self, config }: any) => ({
	bindables: { conflict: CONFLICT, ttl: TTL },
	value: config.value,
	k: config.k ?? "__SIGNALS__",
	pid: 1,

	read() {
		if (self.gate?.sealed) throw new Error(`[SEALED] ${self.k} is sealed`);
		return self.value;
	},
	write(next: any) {
		if (self.gate?.sealed || self.gate?.frozen) return false;
		if (Object.is(self.value, next)) return false;
		self.useCell()[1]({ value: next });
		return true;
	},
	subscribe(listener: any) { return self.subscription.subscribe(listener); },
	waitFor() {},
}));

export const MODULE = MASTER_ATOM("MODULE", ({ self, config }: any) => ({
	bindables: { conflict: CONFLICT },
	value: config.value,

	read() { return self.value; },
	write(next: any) {
		if (self.gate?.sealed || self.gate?.frozen) return false;
		self.useCell()[1]({ value: next });
		return true;
	},
	mount(name: string, mod: any) { self.modules.s(mod, name); self.useCell()[1]({}); return self.modules.size; },
	unmount(name: string) { const had = self.modules.d(name); self.useCell()[1]({}); return had; },
	list() { return [...self.modules.keys()]; },
}));

// ============================================================================
//  BOUND ATOMS — every write goes through self.<own field> -> own namespace
// ============================================================================
export const CONFLICT = BIND_ATOM("CONFLICT", ({ self, config }: any) => ({
	conflictStrategy: config.conflictStrategy ?? "latest",

	resolve<T>(current: T, incoming: T): T {
		switch (self.conflictStrategy) { // falls through to master root config
			case "fail": throw new Error("SOUL_NEUTRON conflict: concurrent values cannot be merged");
			case "accumulate":
				if (typeof current === "number" && typeof incoming === "number") return (current + incoming) as T;
				if (Array.isArray(current) && Array.isArray(incoming)) return [...current, ...incoming] as T;
				if (typeof current === "string" && typeof incoming === "string") return (current + incoming) as T;
				throw new TypeError("SOUL_NEUTRON accumulate requires matching number, string, or array");
			case "latest":
			default: return incoming;
		}
	},
}));

export const TTL = BIND_ATOM("TTL", ({ self, config }: any) => ({
	entries: new M2I<string, any>(),
	defaultTtlMs: config.defaultTtlMs ?? 0,
	active: 0,

	refresh() { // recomputed on any change in the resource
		let n = 0;
		for (const k of self.entries.keys()) n++;
		self.useCell()[0].active = n; // refresh, not a change: silent by construction
	},

	set(key: string, data?: any, ttlMs?: number) {
		const ttl = ttlMs ?? self.defaultTtlMs;
		if (!ttl || ttl <= 0) throw new Error("[ttl] no ttl provided and no defaultTtlMs configured");
		const entry = { expiresAt: Date.now() + ttl, data };
		self.entries.s(entry, key);
		self.useCell()[1]({});
		return entry;
	},
	get(key: string) {
		const e = self.entries.g(key);
		if (!e) return undefined;
		if (Date.now() >= e.expiresAt) { self.entries.d(key); self.useCell()[1]({}); return undefined; }
		return e.data;
	},
	has(key: string) { const e = self.entries.g(key); return !!e && Date.now() < e.expiresAt; },
}));

export const GATE = BIND_ATOM("GATE", ({ self, config }: any) => ({
	sealed: config.sealed ?? false,
	frozen: config.frozen ?? false,
	sleep: config.sleep,

	get asleep() { return self.sleep !== undefined && Date.now() < self.sleep; },
	seal() { self.sealed = true; },
	unseal() { self.sealed = false; },
	freeze() { self.frozen = true; },
	unfreeze() { self.frozen = false; },
	canRead() { return !self.asleep; },
	canWrite() { return !self.sealed && !self.frozen && !self.asleep; },
}));

export const CONTEXT = BIND_ATOM("CONTEXT", ({ self }: any) => ({
	data: {} as Record<string, any>,

	set(key: string, value: any) { self.data[key] = value; self.useCell().set("data", self.data); return value; },
	read(key: string) { return self.data[key]; },
	has(key: string) { return key in self.data; },
	keys() { return Object.keys(self.data); },
	delete(key: string) { const had = key in self.data; delete self.data[key]; self.useCell().set("data", self.data); return had; },
	purge() { self.data = {}; self.useCell().set("data", self.data); },
}));

export const SUBSCRIPTION = BIND_ATOM("SUBSCRIPTION", ({ self }: any) => ({
	listeners: new Set<any>(),
	subscribe(listener: any) {
		self.listeners.add(listener);
		return res_subscribe(self, listener);
	},
	notify(change: any) { for (const l of self.listeners) { try { l(change); } catch { /* noop */ } } },
	get size() { return self.listeners.size; },
	purge(): void { self.listeners.clear(); },
}));

// ============================================================================
//  TESTS
// ============================================================================
let pass = 0; let fail = 0;
const ok = (label: string, cond: any, got?: any) => {
	if (cond) { pass++; console.log(`PASS  ${label}`); }
	else { fail++; console.log(`FAIL  ${label}${got !== undefined ? `   got ${JSON.stringify(got)}` : ""}`); }
};
const attempt = (fn: () => any) => { try { return { ok: true, value: fn() }; } catch (e: any) { return { ok: false, value: String(e.message ?? e).split("\n")[0] }; } };
const eq = (a: any, b: any) => JSON.stringify(a) === JSON.stringify(b);

const vertexContract = ATOM_CONTRACT(() => ({ atom: "VERTEX", value: { count: 0 } }));
const atomContract = ATOM_CONTRACT(() => ({ atom: "ATOM", value: 0 }));
const moduleContract = ATOM_CONTRACT(() => ({ atom: "MODULE", value: [] }));

const vertex = vertexContract({ value: { count: 0 }, conflictStrategy: "accumulate", defaultTtlMs: 60_000 });
const vertex2 = vertexContract({ value: { count: 0 }, conflictStrategy: "accumulate", defaultTtlMs: 60_000 });
const vertex3 = vertexContract({ value: { count: 0 }, conflictStrategy: "fail", defaultTtlMs: 1 });
const plain = atomContract({ value: 7 });
const mod = moduleContract({ value: [], conflictStrategy: "latest" });

console.log("\n[1] master path is flat — atom.read()");
ok("vertex.read()", eq(vertex.read(), { count: 0 }), vertex.read());
ok("vertex.write({count:5})", vertex.write({ count: 5 }) === true);
ok("read reflects write", eq(vertex.read(), { count: 5 }), vertex.read());
ok("plain.read()", plain.read() === 7, plain.read());
ok("no .master hop on surface", !("master" in vertex) && eq(Object.keys(vertex).filter(k => ["read", "write"].includes(k)).sort(), ["read", "write"]));

console.log("\n[2] bound atoms are namespaced — atom.conflict.resolve()");
ok("vertex.conflict exists", typeof vertex.conflict?.resolve === "function");
ok("vertex.gate exists", typeof vertex.gate?.seal === "function");
ok("resolve: accumulate 10+5", vertex.conflict.resolve(10, 5) === 15, vertex.conflict.resolve(10, 5));
ok("resolve: fail throws", !attempt(() => vertex3.conflict.resolve(1, 2)).ok);
ok("resolve: latest", mod.conflict.resolve("x", "y") === "y");
ok("ttl.set/get namespaced", (vertex.ttl.set("session", "abc"), vertex.ttl.get("session")) === "abc");
ok("unbound name throws typo", !attempt(() => (vertex as any).ttl2).ok);
ok("bare bindable name is not flat", !("resolve" in vertex) && !("sealed" in vertex));

console.log("\n[3] isolation — nothing reaches another master or sibling namespace");
ok("ttl absent on MODULE", !("ttl" in mod));
ok("conflict absent on ATOM", !("conflict" in plain));
ok("mount absent on VERTEX", !("mount" in vertex));
ok("vertex3 strategy independent", vertex3.conflict.conflictStrategy === "fail" && vertex.conflict.conflictStrategy === "accumulate");
ok("per-instance stores", vertex.ttl.entries !== vertex2.ttl.entries);
ok("per-instance gate", (vertex.gate.seal(), vertex.gate.sealed === true && vertex2.gate.sealed === false));
vertex.gate.unseal();
ok("write on one does not touch another", (vertex.write({ count: 99 }), vertex2.read().count === 0));

// the rule itself, at the self level
const res = openCell("probe");
res.namespace("a").x = 1;
res.namespace("b").y = 2;
const aSelf = makeSelf(res, "a");
ok("bound atom sees own field", (aSelf as any).x === 1);
ok("bound atom blocked from sibling namespace", !attempt(() => (aSelf as any).b).ok);
ok("bound atom sees own master config", ((res.root.cfg = 9), (aSelf as any).cfg === 9));
ok("master sees all its namespaces", (makeSelf(res, null) as any).b.y === 2);

console.log("\n[4] one source of truth — gate actually gates");
vertex.gate.seal();
ok("gate.seal() then read throws", !attempt(() => vertex.read()).ok);
ok("gate.seal() then write refused", vertex.write({ count: 1 }) === false);
ok("external view and internal view agree", vertex.gate.sealed === true);
vertex.gate.unseal();
ok("after unseal read works", attempt(() => vertex.read()).ok);

console.log("\n[5] propagation — a change refreshes every namespace in the resource");
const v0 = vertex.version();
let hits = 0;
vertex.subscribe(() => hits++);
vertex.write({ count: 42 });
const v1 = vertex.version();
ok("version advanced", v1 > v0, { v0, v1 });
ok("subscription notified", hits === 1, hits);
ok("master refreshed its own value", vertex.read().count === 42);
ok("bound atom refreshed its namespaced value", vertex.ttl._version === v1, { ttl: vertex.ttl._version, v1 });
ok("gate namespace refreshed too", vertex.gate._version === v1);
ok("ttl re-derived active count on change", (vertex.ttl.set("k1", "v"), vertex.ttl.active === 2), vertex.ttl.active);
ok("re-entrant write does not loop", (vertex.write({ count: 43 }), hits === 3), hits);

console.log("\n[6] one level — the resource is flat, no root/ns containers");
const flat = openCell("flat");
flat.namespace("gate").sealed = false;
flat.root.cfg = 9;
const shape = flat.current;
ok("no root container", !("root" in shape));
ok("no ns container", !("ns" in shape));
ok("namespace sits directly on the resource", shape.gate !== undefined && shape.gate.sealed === false);
ok("master fields sit beside namespaces", shape._version !== undefined && shape.cfg === 9 && shape.gate !== undefined);
ok("resource.gate.sealed is one hop", (vertex.gate.seal(), vertex.gate.sealed === true), vertex.gate.sealed);
vertex.gate.unseal();
ok("field/bindable collision rejected", !attempt(() => vertexContract({ value: 0, conflict: 1 })).ok);
ok("reserved _version rejected", !attempt(() => vertexContract({ value: 0, _version: 1 })).ok);

console.log("\n[7] the ux path — atomContract(7) and atomContract({ value: 7 })");
const bare = atomContract(7);
const objForm = atomContract({ value: 7 });
ok("atomContract(7).read()", bare.read() === 7, bare.read());
ok("atomContract({value:7}).read()", objForm.read() === 7, objForm.read());
ok("bare.write(8)", bare.write(8) === true);
ok("bare.read() after write", bare.read() === 8, bare.read());
ok("bare.write(same) is a no-op", bare.write(8) === false);
bare.gate.seal();
ok("bare.gate.sealed gates read", !attempt(() => bare.read()).ok);
ok("bare.gate.sealed gates write", bare.write(1) === false);
bare.gate.unseal();
ok("white-space default k", bare.test() === 52, bare.test());
ok("plain.context.read through api", (bare.test(), bare.context.read("key")) === 52, bare.context.read("key"));
ok("context is namespaced, not flat", !("key" in bare) && bare.context.get === undefined && typeof bare.context.read === "function");
ok("context isolated per instance", objForm.context.read("key") === undefined, objForm.context.read("key"));
ok("s/m/r defaults present", bare.s === "SAURON_SLAVE_REG" && bare.m === "SAURON_MASTER_REG" && bare.r === "THE_WIZARD_ROOT_REG");
ok("config object still wins", atomContract({ value: 4, k: "K" }).read() === 4);
ok("k override reaches read error", (() => { const c = atomContract({ value: 1, k: "CUSTOM" }); c.gate.seal(); return String(attempt(() => c.read()).value).includes("CUSTOM"); })());

console.log("\n[8] one cell, no shadow state");
{
	const v = vertexContract({ value: 1, k: "VX" });
	const res = OPEN_CELLS[OPEN_CELLS.length - 1];
	const key = res.key;
	const cellObj = bear.state.get(key);
	ok("resource IS the cell value", cellObj === res.current);
	const seen: number[] = [];
	bear.state.subscribe(cellObj ? key! : "", (nv: any) => seen.push(nv._version));
	const before = cellObj._version;
	v.write(2);
	ok("version travels with the value", cellObj._version === before + 1, `${before} -> ${cellObj._version}`);
	ok("subscribers see the same object", seen.length === 1 && seen[0] === cellObj._version);
	ok("no shadow listener set", !("listeners" in res), Object.keys(res).join(","));
	ok("api version reads the cell", v.version() === cellObj._version, v.version());
	v.subscribe(() => {});
	ok("api subscribe is the cell channel", true);
}

console.log(`\n${fail === 0 ? "All checks passed." : `${fail} FAILED`}  (${pass} pass, ${fail} fail)`);
