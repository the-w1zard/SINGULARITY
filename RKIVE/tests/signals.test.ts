import { ATOM_CONTRACT } from "../signals";
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
eq("3b B.bindable failure is a plain TypeError (guard misses instances)", /Cannot read properties of undefined/.test(String(iB1.value)), true);
eq("3b A.bindable still works after B exists", aAfterB.ok, true);
eq("3b A sees no B bindable at all", crossKind.ok && crossKind.value === undefined, true);
eq("3b same master kind does NOT share storage", sharedCell.value && sharedCell.value.count, 0);

console.log(failures === 0 ? "\nAll checks passed." : `\n${failures} check(s) failed.`);
