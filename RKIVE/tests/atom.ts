//                                                                                                                      EXAMPLE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
/**
i NEED YOU TO CONVERT SOME CLASSES/FNS OVER TO BECOME BIND ATOM FOR ME, EACH BIND ATOM IS REQUIRED TO HAVE THE K S M AND R FIELDS IN EACH ONE BEYOND THAT, ITS JUST WHATERVER ELSE NEEDS TO BE CONVERTED
const defaults = []
export const EXAMPLE = BIND_ATOM("CONTRACTS", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
    name: "CONTRACTS", // Standard properties
    useCell: (() => { // IIFE
        const fn = () => {};
        return fn;
    })(),
    Contracts: class Contracts {
        add(contract: SOUL_CONTRACT) {
            self.contracts.set(contract.name, contract);
        }

        remove(name: S) {
            return self.contracts.delete(name);
        }
    },
    factories: {
        createContract: (name: S, precondition: (value: A) => B) => ({
            name,
            precondition,
        }),
    
        createViolation: (
            contract: S,
            message: S,
        ): CONTRACT_VIOLATION => ({
            contract,
            phase: "pre",
            message,
            ts: Date.now(),
            value: undefined,
        }),
    },
    initialized: (() => {
        self.contracts = new Map<S, SOUL_CONTRACT>();
    
        return true;
    })(),
    contracts: new Map<S, SOUL_CONTRACT>(),
    contractProxy: new Proxy(self.contracts, {
        get(target, key, receiver) {
            if (key === "count") return target.size;
    
            return Reflect.get(target, key, receiver);
        },
    
        set(target, key, value) {
            return Reflect.set(target, key, value);
        },
    }),
    // Class expression instantiated immediately
    manager: new (class {
        clear() {
            self.contracts.clear();
        }
    
        get size() {
            return self.contracts.size;
        }
    })(),
    _record: (() => {
        let count = 0;
    
        return (v: CONTRACT_VIOLATION, blocking: B): B => {
            count++;
    
            self.violations.push(v);
    
            if (blocking) throw new Error(v.message);
    
            return false;
        };
    })(),	
    get recordCount() {
        return recordCount;
    },
    ContractManager: class ContractManager {
        add(contract: SOUL_CONTRACT) {
            self.contracts.set(contract.name, contract);
        }
    
        remove(name: S) {
            return self.contracts.delete(name);
        }
    },
    ready: Promise.resolve().then(() => {
        return self.contracts.size;
    }),
    validateAsync: async (value: A) => {
        return self.check(value, "pre");
    },
    internalKey: Symbol("CONTRACTS_INTERNAL"),

    [Symbol.toStringTag]: "CONTRACTS",
    
    [Symbol.iterator]: function* () {
        yield* self.contracts.values();
    },
    CONTRACTS, // Shorthand properties
    // [key]: value, 
    [Symbol.iterator]: function* () {},// Computed property names
    add(contract: typeof CONTRACTS) {},// Method shorthand
    get size() { return this.contracts.size;  },// Getters
    set blocking(v: B) { this._throwOnBlock = v;  },// Setters
    async validate() {},// Async methods
    *entries() {},// Generator methods

    // Spread and composition
    ...defaults,
    ...(() => ({
        initialized: true,
    }))(),

    // Conditional properties
    ...(config.debug && {
        debug: true,
    }),
    ...(config.strict ? {
        strict: true,
        violationMax: 128,
    } : {}),

    // Nested objects
    options: {
        blocking: true,
        maxViolations: 64,
    },
    handlers: {
        add(contract) {},
        remove(name) {},
    },

    // Dynamic initialization
    contracts2: new Map<S, SOUL_CONTRACT>(),

    initialized2: Date.now(),

    id: Symbol("CONTRACTS"),

    [Symbol.apply('thisArg')]: "CONTRACTS",

    // Private closure state through getters
    get violationCount() {
        return this.violations.length;
    },
    get isEmpty() {
        return this.contracts.size === 0;
    },

}))
 */


// #endregion
const def = new (class DEFINITION {
    public MASTER_ATOM_KEYS = ["ATOM", "MODULE", "CLOUD", "SAURON", "DRS", "MSG_Q", "SLAB",  "S_CLASS", "SPREADSHEET",  "VERTEX"] as const;
    public MQ = {
        RETRY: this.DEFINE_ATOM_KIND("retry"),
        ACK: this.DEFINE_ATOM_KIND("ack"),
        DEADLINE: this.DEFINE_ATOM_KIND("deadline"),
        DEDUPE: this.DEFINE_ATOM_KIND("dedupe"),
        DEPTH: this.DEFINE_ATOM_KIND("depth"),
        WILDCARD: this.DEFINE_ATOM_KIND("wildcard"),
        REPLY: this.DEFINE_ATOM_KIND("reply"),
        WINDOW: this.DEFINE_ATOM_KIND("window"),
        RESUME: this.DEFINE_ATOM_KIND("resume"),
        CREDIT: this.DEFINE_ATOM_KIND("credit"),
        CORRELATION: this.DEFINE_ATOM_KIND("correlation"),
        SEND: this.DEFINE_ATOM_KIND("send"),
        SUB: this.DEFINE_ATOM_KIND("send"),
        STREAM: this.DEFINE_ATOM_KIND("send"),
        HANDLER: this.DEFINE_ATOM_KIND("send"),
        DUPLEX: this.DEFINE_ATOM_KIND("send"),
        DRS: this.DEFINE_ATOM_KIND("send"),
        ANTIMSGQUEUE: this.DEFINE_ATOM_KIND("send"),
    } as const;
    public ELECTRONS = {
        AW_SET: this.DEFINE_ATOM_KIND("adapter"),
        CASUAL_CURSOR: this.DEFINE_ATOM_KIND("adapter"),
        DELTA_STORE: this.DEFINE_ATOM_KIND("adapter"),
        G_SET: this.DEFINE_ATOM_KIND("adapter"),
        VERSION_VECTOR: this.DEFINE_ATOM_KIND("adapter"),
        CIRCUIT_BREAKER: this.DEFINE_ATOM_KIND("adapter"),
        CQRS: this.DEFINE_ATOM_KIND("adapter"),
        RAFT: this.DEFINE_ATOM_KIND("adapter"),
        YJS: this.DEFINE_ATOM_KIND("adapter"),
        DIFFPATCH: this.DEFINE_ATOM_KIND("adapter"),
        HYBRID_LOGICAL_CLOCK: this.DEFINE_ATOM_KIND("adapter"),
        LWW: this.DEFINE_ATOM_KIND("adapter"),
        LWW_ELEMENTAL: this.DEFINE_ATOM_KIND("adapter"),
        MV: this.DEFINE_ATOM_KIND("adapter"),
        OPTIMISTIC: this.DEFINE_ATOM_KIND("adapter"),
        OR: this.DEFINE_ATOM_KIND("adapter"),
        PN: this.DEFINE_ATOM_KIND("adapter"),
        REVERSE_HEARTBEAT_MGR: this.DEFINE_ATOM_KIND("adapter"),
        SEMAPHORE: this.DEFINE_ATOM_KIND("adapter"),
        TOMBSTONE_COMPACTOR: this.DEFINE_ATOM_KIND("adapter"),
        YIMIR: this.DEFINE_ATOM_KIND("adapter"),
        HYBRID_LOGICAL_CLOCK_CRDT: this.DEFINE_ATOM_KIND("adapter"),
        ADAPTER: this.DEFINE_ATOM_KIND("adapter"),
        BEAM_CODEC: this.DEFINE_ATOM_KIND("beam_codec"),
        BINARY: this.DEFINE_ATOM_KIND("binary"),
        BYTE_CODE_VM: this.DEFINE_ATOM_KIND("byte_code_vm"),
        CHANNEL: this.DEFINE_ATOM_KIND("channel"),
        CHANNELS: this.DEFINE_ATOM_KIND("channels"),
        CONCURRENCY: this.DEFINE_ATOM_KIND("concurrency"),
        DEADLETTER: this.DEFINE_ATOM_KIND("deadletter"),
        DESTRUCTION: this.DEFINE_ATOM_KIND("destruction"),
        ERROR: this.DEFINE_ATOM_KIND("error"),
        EVENT: this.DEFINE_ATOM_KIND("event"),
        EXTERNAL_SUBSCRIPTION: this.DEFINE_ATOM_KIND("exterenal_sub"),
        GATE: this.DEFINE_ATOM_KIND("gate"),
        GLOBAL_SUBSCRIPTION: this.DEFINE_ATOM_KIND("global_sub"),
        GOVERNOR: this.DEFINE_ATOM_KIND("GOVERNOR"),
        HOOK: this.DEFINE_ATOM_KIND("hook"),
        HTTP_TRANSPORT: this.DEFINE_ATOM_KIND("http_transport"),
        INJECTION_CODEC: this.DEFINE_ATOM_KIND("injection"),
        METRICS: this.DEFINE_ATOM_KIND("metrics"),
        MIDDLEWARE: this.DEFINE_ATOM_KIND("middlleware"),
        RIFTS_ACCESS: this.DEFINE_ATOM_KIND("rifts"),
        REACTIVE: this.DEFINE_ATOM_KIND("reactive"),
        ROUTE: this.DEFINE_ATOM_KIND("route"),
        RUN_ON: this.DEFINE_ATOM_KIND("run_on"),
        SEALED_ERROR: this.DEFINE_ATOM_KIND("sealed_error"),
        SESSION: this.DEFINE_ATOM_KIND("session"),
        SHELL: this.DEFINE_ATOM_KIND("reshelltry"),
        SIGNAL: this.DEFINE_ATOM_KIND("signal"),
        SOA_QUAD_TIERED_REGISTRY: this.DEFINE_ATOM_KIND("soa_registry"),
        SUBSCRIPTION: this.DEFINE_ATOM_KIND("subscription"),
        TRANSFORM: this.DEFINE_ATOM_KIND("transform"),
        WAITER: this.DEFINE_ATOM_KIND("waiter"),
        CONTRACTS: this.DEFINE_ATOM_KIND("contract"),
        SCHEMA: this.DEFINE_ATOM_KIND("schema"),
        VALUES_REGISTRY: this.DEFINE_ATOM_KIND("values_reg"),
        COMPRESSION: this.DEFINE_ATOM_KIND("compression"),
        CONFLICT: this.DEFINE_ATOM_KIND("conflict"),
        CONTEXT: this.DEFINE_ATOM_KIND("context"),
        ENCRYPTION: this.DEFINE_ATOM_KIND("encryption"),
        GROUPS: this.DEFINE_ATOM_KIND("groups"),
        HISTORY: this.DEFINE_ATOM_KIND("history"),
        META: this.DEFINE_ATOM_KIND("meta"),
        OFFSEC: this.DEFINE_ATOM_KIND("offsec"),
        PERMISSIONS: this.DEFINE_ATOM_KIND("permissions"),
        PERSISTENCE: this.DEFINE_ATOM_KIND("persistence"),
        PRIORITY: this.DEFINE_ATOM_KIND("priority"),
        SNAPSHOT: this.DEFINE_ATOM_KIND("snapshot"),
        TAGS: this.DEFINE_ATOM_KIND("tags"),
        TIME_TRAVEL: this.DEFINE_ATOM_KIND("time_travel"),
        TTL: this.DEFINE_ATOM_KIND("ttl"),
    } as const;
    public DEFINE_ATOM_KIND(name: string): symbol {
        return Symbol(name);
    }
    public PARTICLE_KEYS = {
        ...this.ELECTRONS,
        ...this.MQ,
    } as const;
})();
function isStub(value: A): B {
	try {
		return !!value && (typeof value === "object" || typeof value === "function") && value[NINJA_STUB_BRAND] === true;
	} catch {
		return false;
	}
}
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
function withoutKeys(obj: any, keys: string[]) {
	const desc: any = Object.getOwnPropertyDescriptors(obj);
	for (const k of keys) delete desc[k];
	return desc;
}
function mergeInto3<T extends object>(target: T, source: object): T {
	Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
	return target;
}
function resolveFactory(raw: any): any {
	return typeof raw === "function" ? raw : () => raw;
}
const NINJA_STUB_BRAND3 = "__is_atom_stub__";
function makeStub3(trace?: any): any {
	return new Proxy(() => makeStub(trace), {
		get: (_t, prop) => (prop === NINJA_STUB_BRAND ? true : makeStub(trace)),
		apply: () => makeStub(trace),
	});
}
function withTypoGuard3<T extends Record<string, any>>(result: T, label: string): T {
	return new Proxy(result, {
		get(target, prop, receiver) {
			if (typeof prop === "symbol" || prop in target) return Reflect.get(target, prop, receiver);
			throw new Error(`\n[${label} Typo Error] Property "${String(prop)}" does not exist on this ${label} definition.\n  -> Available definitions: [ ${Object.keys(target).join(", ")} ]\n`);
		},
	});
}
// #endregion
//                                                                                                                      MOVED OVER FROM LATEST CONVERSION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
function requiredBindables() {	return { gate: GATE, subscription: SUBSCRIPTION, context: CONTEXT };}
export function patch<T extends object>(self: { value: T }, partial: Partial<T>): T {
    self.value = { ...self.value, ...partial };
    return self.value;
}
export function update<T>(self: { value: T }, fn: (current: T) => T): T {
    self.value = fn(self.value);
    return self.value;
}
export function createHistory<T>(limit = 50) {
    const past: T[] = [];
    const future: T[] = [];
    return {
        push(value: T) { past.push(value); if (past.length > limit) past.shift(); future.length = 0; },
        undo(current: T): T | undefined { if (!past.length) return undefined; future.push(current); return past.pop(); },
        redo(current: T): T | undefined { if (!future.length) return undefined; past.push(current); return future.pop(); },
        get canUndo() { return past.length > 0; },
        get canRedo() { return future.length > 0; },
    };
}
function createLiveSelf(instanceId: S) {
    const liveTarget: R<A> = {};
    const selfProxy = new PX(liveTarget, {
        get(target, key: S) {
            return target[key];
        },
        set(target, key: S, value: A) {
            // reuse the existing Sauron store — same primitive `live`/`useLive` already build on
            target[key] = value;
            death_star.bear.cell.set(`${instanceId}:${key}`, value);
            //  setPrev(sV, `${instanceId}:${key}`, value)
            return true;
        },
    });
    return { selfProxy, liveTarget };
}
function mergeInto<T extends object>(target: T, source: object): T {
    // defineProperties, not assign — this is what preserves get/set accessors
    // as live accessors instead of flattening them into snapshots (the bug from last time)
    Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
    return target;
} 
function resolveFactory(raw: any): any {
    return typeof raw === "function" ? raw : () => raw;
}
function resolveAtomFactory<C extends AtomObject>(raw: C | AtomConfigFactory<C>): AtomConfigFactory<C> {
    return typeof raw === "function" ? raw : () => raw;
}
type StubHit = { category: S; key: S; prop: S };
const STUB_HITS: StubHit[] = [];
const NINJA_STUB_BRAND = "__is_atom_stub__";
function makeStub(trace?: { category: S; key: S }): A {
        return new Proxy(() => makeStub(trace), {
        get: (_t, prop: S | SY) => {
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
function NexusMirror(excludeName: S) {
    return new Proxy(
        {},
        {
            get: (_t: A, name: S) => {
                if (name === excludeName) return makeStub({ category: excludeName, key: name });
                return new Proxy([], {
                    get: (_s: A, prop: S | SY) => {
                        let arr = death_star.bear.cell(name);
                        if (!arr) return makeStub({ category: name, key: String(prop) });
                        if (prop in arr) return (arr as any)[prop];
                        return makeStub({ category: name, key: String(prop) });
                    },
                });
            },
        },
    );
}
export function buildCtx(refs: A, selfProxy: A, extras: A = {}): SelfType {
    const builtins = { self: selfProxy, ...extras };
    return new Proxy(builtins, { get: (target, key: S) => (key in target ? (target as A)[key] : (refs as A)[key]) });
}
function withTypoGuard<T extends Record<S, A>>(result: T, label: S, allowedExtraKeys: S[] = ["freyr"]): T {
    return new Proxy(result, {
        get(target, prop, receiver) {
            if (typeof prop === "symbol" || prop in target || allowedExtraKeys.includes(prop as string)) { return Reflect.get(target, prop, receiver); }
            const validKeys = Object.keys(target).join(", ");
            throw new Error(`\n\x1b[31m[${label} Typo Error]\x1b[0m Property \x1b[33m"${String(prop)}"\x1b[0m does not exist on this ${label} definition.\n` + `  -> \x1b[32mDid you make a spelling mistake?\x1b[0m\n` + `  -> \x1b[35mAvailable definitions:\x1b[0m [ ${validKeys} ]\n`);
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
function resolvePool<R extends Record<S, A>>(factory: (self: A) => R, passes: N = 3): R {
    const { self, draft } = createResolvingSelf();
    let out: R;
    for (let i = 0; i < passes; i++) {
        out = factory(self);
        mergeInto(draft, out); // descriptor-based — accessors survive, same helper as before
    }
    return draft as R; // only the FINAL pass's result ever gets exposed/committed
}
export function ATOM_BINDABLE<T extends object>(factory: (ctx: { self: any }) => T): any {
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
export function MASTER_ATOM<C extends AnyObject | Factory>(name: S, arg1: C) {
    const factory = resolveFactory(arg1 as Factory);
    death_star.bear.cell.set(name, factory);
    return factory as C; // the export IS the factory — re-invoked fresh per atom, never a pre-built singleton
}
export function BIND_ATOM<Name extends string, C extends AtomObject>(name: Name, arg1: C | AtomConfigFactory<C>): AtomConfigFactory<C> {
    const factory = resolveAtomFactory(arg1);
    death_star.bear.cell.set(name, factory);
    return factory;
}
const REQUIRED_BINDABLES: Record<string, AtomConfigFactory> = {
    gate: GATE,
    subscription: SUBSCRIPTION,
};
function buildPoolSelf(address: A, s: S, m: S, r: S, initialValue: U) {
    const [value, setLive] = death_star.drs.useCell.mailbox(initialValue, address, s, m, r);
    const target: A = { k: address, s, m, r };
    Object.defineProperty(target, "value", {
        get() { return value; },
        set(next) { setLive(next); },
        enumerable: true,
    });
    return target;
}
function createIsolatedSelf(initial = Object.create(null)) {
    const state = new Map();
  
    const self = new Proxy(Object.create(null), {
      get(_target, key) {
        if (typeof key === "symbol") return undefined;
  
        if (state.has(key)) return state.get(key);
  
        // During early passes, return a resolvable stub rather than undefined.
        return makeResolvableStub(key);
      },
  
      set(_target, key, value) {
        if (typeof key === "symbol") return false;
        state.set(key, value);
        return true;
      },
  
      has(_target, key) {
        return typeof key === "string" && state.has(key);
      },
    });
  
    return { self, state };
}
function createMaster1(kind: S, config: A) {
    const bindables = new Map<string, any>();
    const kindFactory = resolveAtomFactory(death_star.bear.cell(kind));
    if (!kindFactory) throw new TypeError(`Unknown master atom: ${kind}`);

    const k = config.k ?? death_star.k;
    const s = config.s ?? death_star.s;
    const m = config.m ?? death_star.m;
    const r = config.r ?? death_star.r;

    const self = buildPoolSelf(k, s, m, r, config.value);

    let pass: A = {};
    for (let i = 0; i < 3; i++) {
        pass = kindFactory({ self, config });
        mergeInto(self, pass);
    }

    const allBindables = { ...REQUIRED_BINDABLES, ...(pass.bindables ?? {}) };

    for (const [bindKey, bindFactory] of Object.entries(allBindables) as [string, AtomConfigFactory][]) {
        const bk = `${k}:${bindKey}`;
        const bself = buildPoolSelf(bk, s, m, r, config[bindKey]?.value);

        let bpass: A = {};
        for (let j = 0; j < 3; j++) {
            bpass = bindFactory({ self: bself, master: self, config });
            mergeInto(bself, bpass);
        }
    }

    return self;
}
function resolveFactoryPasses(factory, ctx, passes = 3) {
    const { self, state } = createIsolatedSelf();
  
    for (let pass = 0; pass < passes; pass++) {
      const result = factory(ctx);
  
      for (const [key, value] of Object.entries(result)) {
        // Later passes overwrite earlier values, including functions that
        // closed over a stub during pass 1.
        state.set(key, value);
      }
    }
  
    return { self, state };
}
function createMasterFacade(masterSelf, allowed = ["read", "write", "subscribe"]) {
    return new Proxy(Object.create(null), {
      get(_target, key) {
        if (typeof key !== "string") return undefined;
  
        if (!allowed.includes(key)) {
          throw new Error(
            `[MASTER_ACCESS_DENIED] Bindable cannot access master.${key}`
          );
        }
  
        const value = masterSelf[key];
        return typeof value === "function" ? value.bind(masterSelf) : value;
      },
  
      set() {
        throw new Error("[MASTER_IMMUTABLE] Bindables cannot mutate their master");
      },
    });
}
function createCellBackedSelf(instanceId, initialValue, cell) {
    const [value, setValue] = cell(initialValue, instanceId);
  
    const self = new Proxy(Object.create(null), {
      get(_target, key) {
        if (key === "value") return value;
        return undefined;
      },
  
      set(_target, key, next) {
        if (key !== "value") return false;
        setValue(next);
        return true;
      },
    });
  
    return self;
}
function createMaster(kind: string, config: any) {
    const kindFactory = resolveAtomFactory(
      death_star.bear.cell(kind)
    );
  
    if (!kindFactory) {
      throw new TypeError(`Unknown master atom: ${kind}`);
    }
  
    const k = config.k ?? death_star.k;
    const s = config.s ?? death_star.s;
    const m = config.m ?? death_star.m;
    const r = config.r ?? death_star.r;
  
    const universe = new Map<string, any>();
  
    const masterSelf = buildPoolSelf(k, s, m, r, config.value);
  
    const masterPass = resolvePool(
      (self) => kindFactory({ self, config }),
      3
    );
  
    mergeInto(masterSelf, masterPass);
  
    universe.set(kind, masterSelf);
  
    const bindableFactories = {
      ...REQUIRED_BINDABLES,
      ...(masterPass.bindables ?? {}),
    };
  
    for (const [name, factory] of Object.entries(bindableFactories)) {
      const bindableAddress = `${k}:${name}`;
  
      const bindableSelf = buildPoolSelf(
        bindableAddress,
        s,
        m,
        r,
        config[name]?.value
      );
  
      const bindablePass = resolvePool(
        (self) =>
          factory({
            self,
            master: masterSelf,
            config,
          }),
        3
      );
  
      mergeInto(bindableSelf, bindablePass);
  
      universe.set(name, bindableSelf);
    }
  
    return {
      ...masterSelf,
      ...Object.fromEntries(universe.entries()),
    };
}
export function ATOM_CONTRACT<const C extends AtomObject | AtomConfigFactory>(
    arg1: string | C,
    arg2?: C,
): AtomMirror<C> {
    const instanceId = _sid();
    const categoryName = typeof arg1 === "string" ? arg1 : `ATOM_CONTRACT${instanceId}`;
    const rawConfig = (typeof arg1 === "string" ? arg2! : arg1) ;
    if (rawConfig === undefined || rawConfig === null) throw new Error(`Self is required, define self in your props at ${location}`);
    const factory = resolveFactory<C>(rawConfig);
    const refs = NexusMirror(categoryName);
    const { selfProxy, liveTarget } = createLiveSelf(instanceId);
    const pass = factory(buildCtx(refs, selfProxy));
    const rr = pass.r ?? death_star.r;
    const mm = pass.m ?? death_star.m;
    const ss = pass.s ?? death_star.s;
    const kk = pass.k ?? death_star.k;
    Object.assign(liveTarget, pass);
    const kind = pass.atom ?? "ATOM";
    const contract = ((config: A = {}) => {
        const { master, boundAtoms } = createMaster(kind, config);
        const atomObj = { atom: kind, ...master, ...boundAtoms }; // top-level surface stays flat for the USER (atom.gate, atom.read), but internally master's own fns never saw boundAtoms as part of ITS self
        return withTypoGuard(atomObj, categoryName);
    }) as ATOM_CONTRACT<C>;
    Object.assign(contract, liveTarget);
    death_star.bear.cell.set(categoryName, contract);
    return withTypoGuard(contract, categoryName) as unknown as AtomMirror<C>  //as ATOM_KIND_REGISTRY;
}

// #endregion
//                                                                                                                      MOVED OVER FROM LATEST CONVERSION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// #endregion
//                                                                                                                      MOVED OVER FROM LATEST CONVERSION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// #endregion
//                                                                                                                      MOVED OVER FROM LATEST CONVERSION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// #endregion
//                                                                                                                      MOVED OVER FROM LATEST CONVERSION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════















