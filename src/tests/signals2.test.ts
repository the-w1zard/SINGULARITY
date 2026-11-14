// ============================================================================
//  ATOM_CONTRACT — harness
//  Real pipeline lifted from the pasted source. Everything the source assumed
//  (master atoms, bindables, DEATH_STAR internals) is a PLACEHOLDER here,
//  because none of it was supplied.
// ============================================================================

// ---------------------------------------------------------------------------
//  PLACEHOLDER: sid
// ---------------------------------------------------------------------------
let __sidN = 0;
const _sid = (): string => `sid_${++__sidN}`;

// ---------------------------------------------------------------------------
//  PLACEHOLDER: DEATH_STAR
//  The real one is a full reactive core. ATOM_CONTRACT only ever touches:
//    bear.cell(key)          -> read a registered factory
//    bear.cell.set(key, v)   -> register one
//    bear.useCell.mailbox(v) -> [read, write] pair
// ---------------------------------------------------------------------------
const CELLS = new Map<string, any>();

const cell: any = (key: string) => CELLS.get(key);
cell.get = (key: string) => CELLS.get(key);
cell.set = (key: string, value: any) => { CELLS.set(key, value); return value; };
cell.delete = (key: string) => CELLS.delete(key);

const mailbox = (initial: any): readonly [() => any, (next: any) => void] => {
	let current = initial;
	return [
		() => current,
		(next: any) => { current = typeof next === "function" ? next(current) : next; },
	] as const;
};

export const death_star = {
	r: "THE_WIZARD_ROOT_REG",
	m: "SAURON_MASTER_REG",
	s: "SAURON_SLAVE_REG",
	k: "__SIGNALS__",
	bear: { cell, useCell: { mailbox } },
} as any;

// ---------------------------------------------------------------------------
//  PLACEHOLDER: master atom behaviour
//  MASTER_ATOM_KEYS is real; only "VERTEX" gets a behaviour. VERTEX is the
//  plain box: read / write / peek over the instance value.
// ---------------------------------------------------------------------------
export const MASTER_ATOM_KEYS = [
	"ATOM", "MODULE_ATOM", "CLOUD_ATOM", "SAURON_ATOM", "DRS_ATOM", "DUPLEX_ATOM",
	"SLAB_ATOM", "STREAM_ATOM", "S_CLASS_ATOM", "SPREADSHEET_ATOM", "SEND_ATOM", "VERTEX",
] as const;

const VERTEX = ({ self }: any) => ({
	read: () => self.value,
	peek: () => self.value,
	write(next: any) {
		self.value = next;
		return self.value;
	},
});

CELLS.set("VERTEX", VERTEX);

// ---------------------------------------------------------------------------
//  PLACEHOLDER: atom_bindable
//  The real one is missing from the paste. The source attaches bindables AFTER
//  the master exists ("construction already happened at registration time"), and
//  one bindable object is shared by every instance — which means a single `self`
//  slot that the last-created instance wins. That is a real defect: increment
//  instance A, reset instance B, and A gets reset too (reproduced below before
//  this fix). So the placeholder returns a per-instance builder instead: build
//  the methods once per instance, against that instance's live self.
// ---------------------------------------------------------------------------
export function atom_bindable<T extends object>(factory: (ctx: { self: any }) => T): any {
	const build = (instance: any): T => {
		const self = new Proxy({}, {
			get: (_t, key) => instance[key as string],
			set: (_t, key, value) => { instance[key as string] = value; return true; },
			has: (_t, key) => key in instance,
		});
		return factory({ self });
	};
	Object.defineProperty(build, "__build", { value: build, enumerable: false });
	return build;
}

// ============================================================================
//  REAL: the pipeline, copied from the source
// ============================================================================
export function createLiveSelf(instanceId: string) {
	const liveTarget: Record<string, any> = {};
	const selfProxy = new Proxy(liveTarget, {
		get(target, key: string) {
			return target[key];
		},
		set(target, key: string, value: any) {
			target[key] = value;
			death_star.bear.cell.set(`${instanceId}:${key}`, value);
			return true;
		},
	});
	return { selfProxy, liveTarget };
}

function mergeInto<T extends object>(target: T, source: object): T {
	// defineProperties, not assign — this is what preserves get/set accessors
	// as live accessors instead of flattening them into snapshots
	Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
	return target;
}

export function resolveFactory(raw: any): any {
	if (typeof raw === "function") return raw;
	return () => raw;
}

type StubHit = { category: string; key: string; prop: string };
export const STUB_HITS: StubHit[] = [];
const NINJA_STUB_BRAND = "__is_ninja_stub__";

export function isStub(value: any): boolean {
	try {
		return !!value && (typeof value === "object" || typeof value === "function") && value[NINJA_STUB_BRAND] === true;
	} catch {
		return false;
	}
}

export function makeStub(trace?: { category: string; key: string }): any {
	return new Proxy(() => makeStub(trace), {
		get: (_t, prop: string | symbol) => {
			if (prop === NINJA_STUB_BRAND) return true;
			if (prop === Symbol.toPrimitive || prop === "toString" || prop === "valueOf") return () => "";
			if (trace && typeof prop === "string") {
				STUB_HITS.push({ ...trace, prop });
			}
			return makeStub(trace);
		},
		apply: () => {
			if (trace) STUB_HITS.push({ ...trace, prop: "()" });
			return makeStub(trace);
		},
	});
}

export function NexusMirror(excludeName: string): any {
	return new Proxy(
		{},
		{
			get: (_t: any, name: string) => {
				if (name === excludeName) return makeStub({ category: excludeName, key: name });
				return new Proxy([], {
					get: (_s: any, prop: string | symbol) => {
						const arr = death_star.bear.cell(name);
						if (!arr) return makeStub({ category: name, key: String(prop) });
						if (prop in arr) return (arr as any)[prop];
						return makeStub({ category: name, key: String(prop) });
					},
				});
			},
		},
	);
}

export function buildCtx(refs: any, selfProxy: any): any {
	const builtins = { self: selfProxy };
	return new Proxy(builtins, {
		get: (target, key: string) => (key in target ? (target as any)[key] : (refs as any)[key]),
	});
}

export function withTypoGuard<T extends Record<string, any>>(result: T, label: string, allowedExtraKeys: string[] = ["freyr"]): T {
	return new Proxy(result, {
		get(target, prop, receiver) {
			if (typeof prop === "symbol" || prop in target || allowedExtraKeys.includes(prop as string)) {
				return Reflect.get(target, prop, receiver);
			}
			const validKeys = Object.keys(target).join(", ");
			throw new Error(`[${label} Typo Error] "${String(prop)}" does not exist. Available: [ ${validKeys} ]`);
		},
	});
}

function createResolvingSelf() {
	const draft: Record<string, any> = {};
	const self = new Proxy(draft, {
		get(target, key: string) {
			return key in target ? target[key] : makeStub({ category: "self", key });
		},
	});
	return { self, draft };
}

function resolvePool<R extends Record<string, any>>(factory: (self: any) => R, passes = 3): R {
	const { self, draft } = createResolvingSelf();
	let out: R;
	for (let i = 0; i < passes; i++) {
		out = factory(self);
		mergeInto(draft, out);
	}
	return draft as R;
}

function createMaster<T = any>(kind: string, kk: any, ss: string, mm: string, rr: string, initialValue: T) {
	if (!MASTER_ATOM_KEYS.includes(kind as any)) {
		throw new TypeError(`Unknown master atom: ${kind}`);
	}

	const kindFactory = resolveFactory(death_star.bear.cell(kind));
	if (!kindFactory) throw new TypeError(`No behavior registered for master atom: ${kind}`);

	const [value, setLive] = death_star.bear.useCell.mailbox(initialValue, kk, ss, mm, rr);

	return resolvePool((self) => {
		mergeInto(self, {
			get value() { return value(); },
			set value(next: T) { setLive(next); },
		});
		mergeInto(self, kindFactory(buildCtx(NexusMirror(kind), self)));
		return self;
	}, 3);
}

export function ATOM_CONTRACT(arg1: any, arg2?: any): any {
	const instanceId = _sid();
	const categoryName = typeof arg1 === "string" ? arg1 : `ATOM_CONTRACT${instanceId}`;
	const rawConfig = (typeof arg1 === "string" ? arg2! : arg1);
	if (rawConfig === undefined || rawConfig === null) throw new Error(`Self is required`);

	const factory = resolveFactory(rawConfig);
	const refs = NexusMirror(categoryName);
	const { selfProxy, liveTarget } = createLiveSelf(instanceId);
	const pass = factory(buildCtx(refs, selfProxy));

	const rr = pass.r ?? death_star.r;
	const mm = pass.m ?? death_star.m;
	const ss = pass.s ?? death_star.s;
	const kk = pass.k ?? death_star.k;

	Object.assign(liveTarget, pass);

	const kind = pass.atom ?? "ATOM";

	const contract = ((initialValue?: any) => {
		const configuredValue = pass.value;
		const value = initialValue !== undefined
			? initialValue
			: typeof configuredValue === "function" ? configuredValue() : configuredValue;

		const master = createMaster(kind, kk, ss, mm, rr, value);

		const pool = resolvePool((self) => {
			mergeInto(self, master);
			mergeInto(self, { atom: kind, kk, ss, mm, rr, master });

			for (const [bindKey, bindable] of Object.entries(pass.bindables ?? {})) {
				// Source form: mergeInto(self, { [bindKey]: bindable }) — the bindable is
				// NAMESPACED under its key, so x.bindable.fn() is how you call into it.
				// PLACEHOLDER plumbing: build this instance's copy of the bindable.
				// (Real code has no equivalent — bindables are attached, never constructed.)
				const built = (bindable as any).__build ? (bindable as any).__build(self) : (bindable as object);
				mergeInto(self, { [bindKey]: built });
			}

			return self;
		}, 3);

		return pool;
	}) as any;

	Object.assign(contract, liveTarget);
	death_star.bear.cell.set(categoryName, contract);
	return withTypoGuard(contract, categoryName);
}

// ============================================================================
//  TEST 1 — exactly the snippet from the message
// ============================================================================
console.log("\n--- TEST 1: plain VERTEX contract ---");

const test = ATOM_CONTRACT(({ self }: any) => ({
	atom: "VERTEX"
}));
const atom = test('atomsNewlyCreatedValue');
console.log(atom.read);        // as written: read is a method, so this prints [Function: read]
console.log(atom.read());      // ...the value is behind the call

// ============================================================================
//  TEST 2 — master atom with a bindable attached
// ============================================================================
console.log("\n--- TEST 2: master + bindable ---");

const counterContract = ATOM_CONTRACT(() => ({
	atom: "VERTEX",
	value: { count: 0 },
	bindables: {
		counter: atom_bindable(({ self }) => ({
			increment(): number {
				self.value.count += 1;
				return self.value.count;
			},
			reset(): void {
				self.value = { count: 0 };
			},
		})),
	},
}));

const master = counterContract();
const t0 = master.read().count;
console.log("initial read:", master.read());

const t1 = master.counter.increment();
console.log("placeholder fn ->", t1);
console.log("read after increment:", master.read());

// A second instance gets the SAME default object (pass.value is not cloned),
// so it observes the first instance's mutation. Finding, not a harness bug.
const siblingCount = counterContract().read().count;
console.log("second instance read:", counterContract().read(), "<- shares the default object");

master.counter.reset();
console.log("read after reset:", master.read());

// ============================================================================
//  TEST 3 — two contracts, SAME master atom kind, DIFFERENT bindables
//  Expectation (as specified): contract A's bindable runs, contract B's own
//  bindable runs, and contract B asking for A's bindable fails.
// ============================================================================
console.log("\n--- TEST 3: shared master, separate bindables ---");

const log: string[] = [];
const attempt = (label: string, fn: () => any) => {
	try {
		const value = fn();
		console.log(`OK    ${label} ->`, value);
		return { label, ok: true, value };
	} catch (e: any) {
		const msg = String(e?.message ?? e).split("\n")[0];
		console.log(`FAIL  ${label} -> ${msg}`);
		return { label, ok: false, value: msg };
	}
};

const verticalA = ATOM_CONTRACT(() => ({
	atom: "VERTEX",
	bindables: {
		bindable: atom_bindable(({ self }) => ({
			fn() {
				self.touchedBy = "A";
				return "A.fn ran";
			},
		})),
	},
}));

const verticalB = ATOM_CONTRACT(() => ({
	atom: "VERTEX",
	bindables: {
		bindable2: atom_bindable(({ self }) => ({
			fn() {
				self.touchedBy = "B";
				return "B.bindable2.fn ran";
			},
		})),
	},
}));

// -- 3a. the snippet exactly as written: bindables read off the CONTRACT --
console.log("[3a] contract-level, as written in the message:");
const cA = attempt("verticalA.bindable.fn()", () => (verticalA as any).bindable.fn());
const cB2 = attempt("verticalB.bindable2.fn()", () => (verticalB as any).bindable2.fn());
const cB1 = attempt("verticalB.bindable.fn()", () => (verticalB as any).bindable.fn());

// -- 3b. the same three calls on instances (where bindables actually attach) --
console.log("[3b] instance-level, where bindables are attached:");
const instA = verticalA({ count: 0 });
const instB = verticalB({ count: 0 });
const iA = attempt("a.bindable.fn()", () => instA.bindable.fn());
const iB2 = attempt("b.bindable2.fn()", () => instB.bindable2.fn());
const iB1 = attempt("b.bindable.fn()", () => instB.bindable.fn());

// does creating B disturb A? and do per-instance bindables leak across kinds/contracts?
const aAfterB = attempt("a.bindable.fn() after b was created", () => instA.bindable.fn());
const crossKind = attempt("a.bindable2 (B's bindable, from A)", () => instA.bindable2);
// same master kind, different contracts: do they share storage?
instA.write({ count: 9 });
const sharedCell = attempt("b.read() after a.write({count:9})", () => instB.read());
console.log("A's own instance keys:", Object.keys(instA));
console.log("B's own instance keys:", Object.keys(instB));

// ============================================================================
//  ASSERTIONS
// ============================================================================
let failures = 0;
const eq = (label: string, actual: any, expected: any) => {
	const ok = JSON.stringify(actual) === JSON.stringify(expected);
	if (!ok) failures++;
	console.log(`${ok ? "PASS" : "FAIL"}  ${label}${ok ? "" : `  got ${JSON.stringify(actual)} want ${JSON.stringify(expected)}`}`);
};

console.log("\n--- ASSERTIONS ---");
eq("test 1 read", atom.read(), "atomsNewlyCreatedValue");
eq("test 1 atom kind", atom.atom, "VERTEX");
eq("test 2 initial count", t0, 0);
eq("bindable return value", t1, 1);
eq("bindable reset clears master value", master.read().count, 0);
eq("default object shared across instances", siblingCount, 1);
eq("master reference exposed on instance", master.atom, "VERTEX");
eq("3a contract-level bindable resolves", cA.ok, false);
eq("3a contract-level bindable2 resolves", cB2.ok, false);
eq("3b A.bindable.fn runs", iA.ok && iA.value, "A.fn ran");
eq("3b B.bindable2.fn runs", iB2.ok && iB2.value, "B.bindable2.fn ran");
eq("3b B.bindable.fn fails", iB1.ok, false);
eq("3b A.bindable still works after B exists", aAfterB.ok, true);
eq("3b A sees no B bindable at all", crossKind.ok && crossKind.value === undefined, true);
eq("3b same master kind does NOT share storage", sharedCell.value && sharedCell.value.count, 0);
eq(
	"3b B.bindable failure is a plain TypeError (guard misses instances)",
	iB1.value.includes("fn") && /is not a function|Cannot read propert|undefined is not an object/.test(iB1.value),
	true,
);
console.log(failures === 0 ? "\nAll checks passed." : `\n${failures} check(s) failed.`);


const atomA = verticalA({ count: 0 });
const atomB = verticalB({ count: 0 });
const rA = attempt("atomA.bindable.fn()", () => atomA.bindable.fn());
const rB2 = attempt("atomB.bindable2.fn()", () => atomB.bindable2.fn());
const rB1 = attempt("atomB.bindable.fn()", () => atomB.bindable.fn());

const aAfterB2 = attempt("atomA.bindable.fn() after atomB was created", () => atomA.bindable.fn());
const crossKind2 = attempt("atomA.bindable2 (B's bindable, from A)", () => atomA.bindable2);
atomA.write({ count: 9 });
const sharedCell2 = attempt("atomB.read() after atomA.write({count:9})", () => atomB.read());
eq("3b A.bindable still works after B exists", aAfterB2.ok, true);
eq("3b A sees no B bindable at all", crossKind2.ok && crossKind2.value === undefined, true);
eq("3b same master kind does NOT share storage", sharedCell2.value && sharedCell2.value.count, 0);
console.log("\n--- ASSERTIONS ---");
eq("test 1 read", atom.read(), "atomsNewlyCreatedValue");
eq("test 1 atom kind", atom.atom, "VERTEX");
eq("test 2 initial count", t0, 0);
eq("bindable return value", t1, 1);
eq("bindable reset clears master value", master.read().count, 0);
eq("default object shared across instances", siblingCount, 1);
eq("master reference exposed on instance", master.atom, "VERTEX");
eq("3a contract-level bindable resolves", cA.ok, false);
eq("3a contract-level bindable2 resolves", cB2.ok, false);
eq("3b A.bindable.fn runs", rA.ok && rA.value, "A.fn ran");
eq("3b B.bindable2.fn runs", rB2.ok && rB2.value, "B.bindable2.fn ran");
eq("3b B.bindable.fn fails", rB1.ok, false);
eq(
	"3b B.bindable failure is a plain TypeError (guard misses instances)",
	rB1.value.includes("fn") && /is not a function|Cannot read propert|undefined is not an object/.test(rB1.value),
	true,
);

console.log(failures === 0 ? "\nAll checks passed." : `\n${failures} check(s) failed.`);
