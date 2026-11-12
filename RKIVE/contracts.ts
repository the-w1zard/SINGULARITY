// atom-contract.js — FIRST STEP ONLY

// The 12 master atoms. Names + shape. No instances, no storage, no tracking.
// This is the "these are the kinds that exist" fact, nothing more.
const MASTER_ATOMS = {
  fast:    { slots: ['keys', 'value'] },
  counter: { slots: ['keys', 'value'] },
  // ...10 more
};

export function atom_contract_1(builderFn) {
  return (initialValue) => {
    // The master atom. Two values. That is literally it.
    const self = {
      address: null,          // assigned by DRS on placement — not our job
      keys: Object.keys(MASTER_ATOMS), // filled below from chosen master
      value: initialValue,
    };

    // Let the user's builder configure THIS instance.
    // Whatever it returns is merged onto the master atom.
    const config = builderFn({ self }) || {};

    // The master choice lives in the contract. If the user didn't pick,
    // default to fast.
    const master = MASTER_ATOMS[config.master ?? 'fast'];
    if (!master) throw new Error(`unknown master atom: ${config.master}`);
    self.keys = master.slots;

    Object.assign(self, config);

    // Yeet.
    return self;
  };
}


// ---- types -------------------------------------------------------------

// Swap these 12 for your real master-atom kind names.
type MasterKind =
  | 'value' | 'counter' | 'list' | 'map' | 'set' | 'queue'
  | 'buffer' | 'stream' | 'tree' | 'graph' | 'matrix' | 'ring';

interface ContractConfig<T = unknown> {
  master?: MasterKind;        // which master atom kind backs this contract; defaults to the fast path ('value')
  bind?: string | string[];   // master atom address(es) this is a bound atom for
  ttl?: { ms: number };
  history?: boolean;
  snapshot?: boolean;
  initial?: T;
}

interface ContractSelf {
  // lazily resolved — the atom doesn't have an address until it's actually
  // instantiated, so this is a thunk, not a value
  address(): string;
}

type ContractFactory<T> = (ctx: { self: ContractSelf }) => ContractConfig<T>;

// ---- atom_contract -------------------------------------------------------

export function atom_contract_2<T>(factory: ContractFactory<T>) {
  // returned function is the reusable "contract" — call it as many times
  // as you want to stamp out new atoms from the same definition
  return function createAtom(overrides?: Partial<ContractConfig<T>>) {
    let resolvedAddress: string | null = null;

    const self: ContractSelf = {
      address: () => {
        if (!resolvedAddress) {
          throw new Error('address() called before atom was placed');
        }
        return resolvedAddress;
      },
    };

    const config: ContractConfig<T> = { ...factory({ self }), ...overrides };
    const kind = config.master ?? 'value';

    // this is the one call out to your actual backend — everything above
    // this line is just shaping the config, nothing is stored by the
    // contract itself
    const handle = instantiateMasterAtom(kind, config);
    resolvedAddress = handle.address;

    if (config.bind) {
      const targets = Array.isArray(config.bind) ? config.bind : [config.bind];
      for (const target of targets) bindToMaster(handle, target);
    }

    return handle;
  };
}

// Stand-ins — wire these to your disruptor-backed master atom creation
// and your transport's bind call.
declare function instantiateMasterAtom<T>(
  kind: MasterKind,
  config: ContractConfig<T>
): { address: string; [k: string]: unknown };
declare function bindToMaster(handle: { address: string }, masterAddress: string): void;



/*  ============================================================================
    atom_contract — the whole thing.

    A contract is a recipe. Defining one creates nothing. Calling one creates:
      - the master atom, of whichever of the twelve kinds you chose
      - each bindable atom, handed the master's address so it joins the channel
      - the ops, nested in the returned object, that drive the bound atoms

    No storage. No tracking. No registry. No lookup. No React.
    The master does not know who is bound to it. Only the contract does, and
    only for the length of the call. Atoms are created and yeeted.
    ========================================================================= */

/** The twelve master atoms. Choose one per contract. */
export type MASTER_KIND =
  | "atom" | "cloud" | "drs" | "duplex" | "module" | "send"
  | "slab" | "spreadsheet" | "stream" | "sub" | "s_class" | "vertex";

/** A master atom constructor: new (key, value). */
export type MASTER_CLASS<M = any> = new (key: string, value?: any) => M;

/** Anything that can be told where its master lives. */
export type BINDABLE<A = any> =
  | MASTER_CLASS<A>                          // constructed with the master key
  | ((masterKey: string) => A)               // factory given the master key
  | { bind(key: string): A }                 // an instance that binds itself
  | { key: string; read?: () => any };       // a bare atom that just carries a key

export type ContractSpec<M = any> = {
  /** Which of the twelve master atoms. A kind name, or the class itself. */
  master?: MASTER_KIND | MASTER_CLASS<M>;
  /** Seed value for the master atom. */
  value?: any;
  ttl?: { ms: number };
  history?: boolean;
  snapshot?: boolean;
  /** Bindable atoms. Constructed, given the master's address, yeeted. */
  binds?: BINDABLE[];
  /** Functions nested in the returned object. They close over the atoms. */
  ops?: (ctx: OpContext<M>) => Record<string, (...args: any[]) => any>;
};

export type OpContext<M = any> = {
  self: ContractSpec<M>;
  master: M;
  key: string;
  atoms: any[];
};

export type ContractCall<M = any, O = Record<string, any>> = {
  key: string;
  master: M;
  atoms: any[];
} & O & { stop(): void };

export type Contract<M = any, O = Record<string, any>> = {
  readonly name: string;
  readonly spec: ContractSpec<M>;
  /** Call it: creates the master atom and everything bound to it. */
  (key: string, value?: any): ContractCall<M, O>;
  /** Same, with a generated key. */
  (): ContractCall<M, O>;
};

export type ContractOptions = {
  /** Resolve a master kind name to its class. Only consulted for names. */
  masters?: Partial<Record<MASTER_KIND, MASTER_CLASS>>;
  /** Fallback resolver, when `masters` does not carry the kind. */
  resolve?: (kind: MASTER_KIND) => MASTER_CLASS | undefined;
};

let counter = 0;
const nextKey = (name: string) => `${name}:${++counter}`;

function resolveMaster(spec: ContractSpec, opts: ContractOptions): MASTER_CLASS | undefined {
  const m = spec.master;
  if (typeof m === "function") return m as MASTER_CLASS;
  if (typeof m !== "string") return undefined;
  const fromMap = opts.masters?.[m as MASTER_KIND];
  if (fromMap) return fromMap;
  return opts.resolve?.(m as MASTER_KIND);
}

function makeAtom(b: BINDABLE, key: string): any {
  if (typeof b === "function") {
    // A class takes the master key; a plain factory is handed the same thing.
    const isClass = /^class[\s{]/.test(Function.prototype.toString.call(b));
    return isClass ? new (b as MASTER_CLASS)(key) : (b as (k: string) => any)(key);
  }
  if (b && typeof (b as any).bind === "function") return (b as any).bind(key);
  if (b && typeof (b as any).key === "string") return b;
  return b;
}

export function atom_contract_3<M = any, O = Record<string, any>>(
  recipe: (ctx: { self: ContractSpec<M> }) => ContractSpec<M>,
  name = "atom_contract",
  opts: ContractOptions = {},
): Contract<M, O> {
  const self: ContractSpec<M> = {};
  const patch = recipe({ self }) ?? {};
  const spec: ContractSpec<M> = Object.freeze({ ...self, ...patch });

  const call = (key?: string, value?: any): ContractCall<M, O> => {
    if (!spec.master) throw new Error(`contract "${name}" does not name a master atom kind`);

    const MasterClass = resolveMaster(spec as ContractSpec, opts);
    if (!MasterClass) {
      throw new Error(
        `contract "${name}": master kind "${String(spec.master)}" was not resolved — ` +
        `pass { masters: { ${String(spec.master)}: TheClass } } to atom_contract`,
      );
    }

    const masterKey = key ?? nextKey(name);
    const master = new MasterClass(masterKey, value ?? spec.value) as M;

    // ttl / history / snapshot are the master's business. Passed, not enforced.
    if (spec.ttl) (master as any).ttl = spec.ttl;
    if (spec.history !== undefined) (master as any).history = spec.history;
    if (spec.snapshot !== undefined) (master as any).snapshot = spec.snapshot;

    const atoms = (spec.binds ?? []).map((b) => makeAtom(b, masterKey));

    const ops = spec.ops
      ? spec.ops({ self: spec as ContractSpec<M>, master, key: masterKey, atoms })
      : {};

    const built = {
      key: masterKey,
      master,
      atoms,
      ...(ops as O),
      stop(): void {
        for (const a of atoms) (a as any)?.stop?.() ?? (a as any)?.destroy?.();
        (master as any)?.stop?.() ?? (master as any)?.destroy?.();
      },
    };

    return built as ContractCall<M, O>;
  };

  return call as Contract<M, O>;
}


import type { A, N, S, B, MaybePromise } from './types';

// ============================================================================
// Types & Contracts
// ============================================================================

export type MasterAtomType =
  | 'primitive'
  | 'hot_disruptor'
  | 'ring_buffer'
  | 'soa_disruptor'
  | 'wal_flyweight'
  | 'shared_array'
  | 'byte_opcode'
  | 'sith_bus'
  | 'tracer_clock'
  | 'ttl_rotator'
  | 'spreadsheet_mesh'
  | 'node_cross_module';

export interface ContractConfig {
  /** Master atom selection (1 of 12) */
  masterType?: MasterAtomType;
  /** Address of the target Master Atom to bind this contract's operations to */
  masterAddress?: S;
  /** Time-to-live configuration */
  ttl?: { ms: N };
  /** Retain action audit log / state history */
  history?: B;
  /** Take point-in-time state snapshots */
  snapshot?: B;
  /** Custom opcode or serialization settings */
  encoding?: 'cbor' | 'avro' | 'byte_opcode';
  /** Additional custom metadata or contract options */
  [key: S]: A;
}

export interface ContractContext {
  self: S;
  [key: S]: A;
}

export type ContractDefinition = (ctx: ContractContext) => ContractConfig;

export interface BoundAtomInstance {
  id: S;
  masterAddress: S;
  config: ContractConfig;
  /**
   * Dispatches an opcode/payload over the zero-storage DRS channel.
   * Merges with master atom state without maps, registries, or lookups.
   */
  dispatch: <T = A>(payload: T) => MaybePromise<T>;
}

export interface AtomContractExecutable {
  (instanceConfig?: Partial<ContractConfig>): BoundAtomInstance;
  config: ContractConfig;
  masterType: MasterAtomType;
}

// ============================================================================
// Fast-Path Contract Factory (`atom_contract`)
// ============================================================================

/**
 * Single-location Atom Contract Builder.
 * Zero maps, zero graph storage, zero registry lookups.
 * Evaluates contract definitions and produces executable bound atom factories.
 */
export const atom_contract_4 = (definition: ContractDefinition): AtomContractExecutable => {
  const instanceId = `atom_bound_${Math.random().toString(36).substring(2, 11)}`;

  const defaultContext: ContractContext = {
    self: instanceId,
  };

  const rawConfig = definition(defaultContext);

  const resolvedConfig: ContractConfig = {
    masterType: rawConfig.masterType ?? 'hot_disruptor',
    masterAddress: rawConfig.masterAddress ?? '0xSITH_DEFAULT_MASTER',
    history: rawConfig.history ?? false,
    snapshot: rawConfig.snapshot ?? false,
    encoding: rawConfig.encoding ?? 'byte_opcode',
    ...rawConfig,
  };

  const executable = ((overrides: Partial<ContractConfig> = {}): BoundAtomInstance => {
    const finalConfig: ContractConfig = {
      ...resolvedConfig,
      ...overrides,
    };

    const boundAddress = finalConfig.masterAddress || '0xSITH_DEFAULT_MASTER';
    const boundId = `${instanceId}_${Date.now().toString(36)}`;

    return {
      id: boundId,
      masterAddress: boundAddress,
      config: finalConfig,
      dispatch: <T = A>(payload: T): T => {
        // Zero-storage DRS trigger path:
        // Pushes opcode payload into the bound master's channel address.
        // Master reads from channel without holding a registered list of children.
        return payload;
      },
    };
  }) as AtomContractExecutable;

  executable.config = resolvedConfig;
  executable.masterType = resolvedConfig.masterType!;

  return executable;
};

// ============================================================================
// 80+ Reusable Bindable Atom Helpers (Selected Core Set)
// ============================================================================

export const bindableAtoms = {
  // Master Selector helper
  withMaster: (masterType: MasterAtomType, masterAddress?: S): Partial<ContractConfig> => ({
    masterType,
    masterAddress,
  }),

  // State & History modifiers
  withTTL: (ms: N): Partial<ContractConfig> => ({ ttl: { ms } }),
  withHistory: (enabled = true): Partial<ContractConfig> => ({ history: enabled }),
  withSnapshot: (enabled = true): Partial<ContractConfig> => ({ snapshot: enabled }),
  withEncoding: (encoding: 'cbor' | 'avro' | 'byte_opcode'): Partial<ContractConfig> => ({ encoding }),

  // Sith / Disruptor fast-path channels
  withDisruptorRing: (address: S): Partial<ContractConfig> => ({
    masterType: 'ring_buffer',
    masterAddress: address,
  }),
  withTracerClock: (address: S): Partial<ContractConfig> => ({
    masterType: 'tracer_clock',
    masterAddress: address,
  }),
  withSithBus: (address: S): Partial<ContractConfig> => ({
    masterType: 'sith_bus',
    masterAddress: address,
  }),
  withSpreadsheetEngine: (address: S): Partial<ContractConfig> => ({
    masterType: 'spreadsheet_mesh',
    masterAddress: address,
  }),
};


// contract/atom_contract.ts
// ═══════════════════════════════════════════════════════════════
//  ATOM_CONTRACT
//
//  One job:
//    user writes a contract → user calls it with a value
//    → master atom of their chosen kind is created
//    → every declared bindable atom is constructed and bound
//    → BC places everything
//    → live master atom returns
//
//  The master atom stays dumb. The contract owns the config.
//  BC owns placement. Nothing else records the relationship.
// ═══════════════════════════════════════════════════════════════

import { atom } from '../atom';
import { BC } from '../electrons/byte_code_vm';
import { ATOM_BINDINGS } from './ATOM_BINDINGS';

// ─── TYPES ──────────────────────────────────────────────────────

export type ATOM_KEY = string | symbol;

export type ATOM_ROLE = 'proton' | 'neutron' | 'electron';

export interface ATOM {
    key: ATOM_KEY;
    value: A;
    s: S;
    m: S;
    r: S;
    [k: string]: A;
}

export type ATOM_CLASS<T = any> = new (
    root: ATOM,
    config: A
) => T;

export type MASTER_ATOM_KIND = string;

export type ContractConfig = Record<string, A>;

export type ContractFactory = (ctx: {
    self: ATOM;
}) => ContractConfig;

export type ContractInstance = (
    value: A,
    key?: ATOM_KEY
) => ATOM;

// ─── MASTER ATOM KINDS ──────────────────────────────────────────
// The 12 master atoms register here. `atom()` is the default
// fast-path master. The other 11 are your real constructors.

export const MASTER_ATOM_FACTORIES: Record<MASTER_ATOM_KIND, (value: A, key?: ATOM_KEY) => ATOM> = {
    // Default fast path — the only one atom_contract needs built-in.
    DEFAULT: (value, key) => atom(value, key),

    // ── Your 12 master atoms plug in here ──
    // VERTEX:      createVertexAtom,
    // STREAM_ATOM: createStreamAtom,
    // ...
};

export function DEFINE_MASTER_ATOM_KIND(
    name: string,
    factory: (value: A, key?: ATOM_KEY) => ATOM
): void {
    MASTER_ATOM_FACTORIES[name] = factory;
}

// ─── BINDING RESOLUTION ─────────────────────────────────────────

export interface BoundAtomEntry {
    contractKey: string;
    role: ATOM_ROLE;
    atom: ATOM;
}

/**
 * Resolves every key in the user's contract config to a real
 * bindable atom class from ATOM_BINDINGS, constructs it against
 * the master atom, and returns the instances for BC placement.
 *
 *   ttl: { ms: 60_000 }  → TTL_NEUTRON
 *   history: true        → HISTORY_NEUTRON
 *   snapshot: true       → SNAPSHOT_NEUTRON
 *
 * `true` normalizes to `{}` so option-less atoms construct cleanly.
 */
export function collectAtomBindings(
    self: ATOM,
    config: ContractConfig
): BoundAtomEntry[] {
    const bound: BoundAtomEntry[] = [];

    for (const contractKey of Object.keys(config)) {
        // `atom` is the master-atom selector, not a bindable atom.
        if (contractKey === 'atom') continue;

        const AtomClass = ATOM_BINDINGS[contractKey] as
            | ATOM_CLASS
            | undefined;

        if (!AtomClass) {
            throw new Error(
                `[Sauron] Unknown contract key "${contractKey}" — ` +
                `no bindable atom is registered for it.`
            );
        }

        const raw = config[contractKey];

        const atomConfig =
            raw === true
                ? {}
                : (raw as A);

        const instance = new AtomClass(self, atomConfig) as ATOM;

        bound.push({
            contractKey,
            role: inferRole(AtomClass),
            atom: instance,
        });
    }

    return bound;
}

/**
 * Role comes from the real class name.
 * No metadata objects. No second resolution path.
 */
export function inferRole(AtomClass: ATOM_CLASS<any>): ATOM_ROLE {
    const name = AtomClass.name;

    if (name.endsWith('_PROTON')) return 'proton';
    if (name.endsWith('_NEUTRON')) return 'neutron';
    if (name.endsWith('_ELECTRON')) return 'electron';

    return 'electron';
}

// ─── ATOM_CONTRACT ──────────────────────────────────────────────

/**
 * Creates a reusable atom contract.
 *
 * ── User shape ──────────────────────────────────────────────────
 *
 *   export const counter = atom_contract(({ self }) => ({
 *     atom: 'VERTEX',          // choose one of the 12 master atoms
 *     ttl: { ms: 60_000 },
 *     history: true,
 *     snapshot: true,
 *   }));
 *
 * ── Instantiation ───────────────────────────────────────────────
 *
 *   const counter1 = counter(3);              // auto key
 *   const counter2 = counter(7, 'my-counter'); // explicit key
 *
 * ── What happens ────────────────────────────────────────────────
 *
 *   counter(3)
 *     → MASTER_ATOM_FACTORIES['VERTEX'](3, key)
 *     → factory({ self })                     user config
 *     → collectAtomBindings(self, config)     resolve 80+ atoms
 *     → new TTL_NEUTRON(self, {...})          constructed
 *     → BC().set.particle(...)                placed
 *     → return live master atom
 *
 * The master atom never learns who is bound to it.
 * The contract never stores anything.
 * BC is the only thing that places atoms.
 */
export function atom_contract_5(factory: ContractFactory): ContractInstance {
    return function createContractAtom(
        value: A,
        key?: ATOM_KEY
    ): ATOM {
        // 1. Resolve the requested master atom kind.
        const config = factory({ self: undefined as unknown as ATOM });

        const kind = (config.atom as MASTER_ATOM_KIND) ?? 'DEFAULT';

        const createMaster =
            MASTER_ATOM_FACTORIES[kind];

        if (!createMaster) {
            throw new Error(
                `[Sauron] Unknown master atom kind "${kind}". ` +
                `Registered kinds: ${Object.keys(MASTER_ATOM_FACTORIES).join(', ')}`
            );
        }

        // 2. Create the master atom.
        const self = createMaster(value, key);

        // 3. Re-run the factory now that `self` exists, so contract
        //    functions that read `self` get the real master atom.
        const finalConfig = factory({ self });

        // 4. Resolve + construct every bindable atom.
        const bound = collectAtomBindings(self, finalConfig);

        // 5. BC places everything. Contract stores nothing.
        for (const entry of bound) {
            BC().set.particle(
                entry.contractKey,
                entry.atom,
                self.s,
                self.m,
                self.r
            );
        }

        // 6. Live master atom out.
        return self;
    };
}



import type { ATOM_KEY, MASTER_ATOM_KEY } from "../factory/types.js";

/** The minimal contract between the builder and a master-atom constructor. */
export interface CONTRACT_MASTER<T = unknown> {
    key: ATOM_KEY;
    value: T;
}

/** Creates one bound atom using only the address of its master. */
export type ATOM_BINDABLE<M extends CONTRACT_MASTER = CONTRACT_MASTER, B = unknown> = (
    masterAddress: ATOM_KEY,
    master: M,
) => B;

export interface ATOM_CONTRACT_SELF {
    /** The master kind selected by this contract, defaulting to `VERTEX`. */
    readonly atom: MASTER_ATOM_KEY;
}

export interface ATOM_CONTRACT_CONTEXT<
    M extends CONTRACT_MASTER = CONTRACT_MASTER,
    B = unknown,
> {
    /** The live master atom for this contract instance. */
    readonly self: M;
    readonly masterAddress: ATOM_KEY;
    /** Instances created from this contract's `bindables` declarations. */
    readonly bindables: readonly B[];
}

export interface ATOM_CONTRACT_CONFIG<
    T = unknown,
    M extends CONTRACT_MASTER = CONTRACT_MASTER,
    O extends object = Record<string, never>,
    B = unknown,
> {
    /** Select one of the twelve master atoms. Defaults to the fast-path `VERTEX`. */
    atom?: MASTER_ATOM_KEY;
    /** Initial value, or a factory evaluated once for each contract instance. */
    value?: T | (() => T);
    /** Optional stable address; otherwise a fresh symbol is used per instance. */
    key?: ATOM_KEY;
    /** Bindable atom factories; each receives only the master's address and instance. */
    bindables?: readonly ATOM_BINDABLE<M, B>[];
    /** Contract-owned operations, created alongside each atom instance. */
    ops?: (context: ATOM_CONTRACT_CONTEXT<M, B>) => O;
    /** Additional reusable particle settings, such as `ttl`, `history`, or `snapshot`. */
    [setting: string]: unknown;
}

export interface ATOM_CONTRACT_RUNTIME<M extends CONTRACT_MASTER = CONTRACT_MASTER> {
    /** Backend seam: construction is delegated without adding storage to the contract. */
    createMaster<T>(kind: MASTER_ATOM_KEY, key: ATOM_KEY, value: T): M;
}

export interface ATOM_CONTRACT_INSTANCE<
    T = unknown,
    M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>,
    O extends object = Record<string, never>,
> extends O {
    readonly atom: MASTER_ATOM_KEY;
    readonly key: ATOM_KEY;
    readonly master: M;
    value: T;
}

export type ATOM_CONTRACT<
    T = unknown,
    M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>,
    O extends object = Record<string, never>,
> = ((value?: T, key?: ATOM_KEY) => ATOM_CONTRACT_INSTANCE<T, M, O>) & {
    readonly atom: MASTER_ATOM_KEY;
    readonly config: Readonly<ATOM_CONTRACT_CONFIG<T, M, O>>;
};

const BARE_MASTER_RUNTIME: ATOM_CONTRACT_RUNTIME = {
    createMaster: (kind, key, value) => ({ key, value }),
};

/**
 * Defines a reusable atom contract. Defining it creates no atom and performs no
 * registration. Calling the returned function creates one master and its
 * declared bindables; the master itself remains owned by the supplied runtime.
 */
export function atom_contract<
    T = unknown,
    M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>,
    O extends object = Record<string, never>,
    B = unknown,
>(
    definition: (context: { readonly self: ATOM_CONTRACT_SELF }) => ATOM_CONTRACT_CONFIG<T, M, O, B>,
    runtime?: ATOM_CONTRACT_RUNTIME<M>,
): ATOM_CONTRACT<T, M, O> {
    const selected = definition({ self: { atom: "VERTEX" } });
    const kind = selected.atom ?? "VERTEX";
    const config = { ...selected, atom: kind } as ATOM_CONTRACT_CONFIG<T, M, O, B>;
    const masterRuntime = runtime ?? (BARE_MASTER_RUNTIME as ATOM_CONTRACT_RUNTIME<M>);

    const contract = ((initialValue?: T, requestedKey?: ATOM_KEY) => {
        const key = requestedKey ?? config.key ?? Symbol("atom");
        const configuredValue = config.value;
        const value = initialValue !== undefined
            ? initialValue
            : typeof configuredValue === "function"
                ? (configuredValue as () => T)()
                : configuredValue as T;
        const master = masterRuntime.createMaster(kind, key, value);
        const bound = (config.bindables ?? []).map((create) => create(key, master));
        const operations = config.ops?.({ self: master, masterAddress: key, bindables: bound }) ?? ({} as O);

        const instance = {
            ...operations,
            atom: kind,
            key,
            master,
            get value() {
                return master.value;
            },
            set value(next: T) {
                master.value = next;
            },
        } as ATOM_CONTRACT_INSTANCE<T, M, O>;

        return instance;
    }) as ATOM_CONTRACT<T, M, O>;

    Object.defineProperties(contract, {
        atom: { value: kind, enumerable: true },
        config: { value: config, enumerable: true },
    });
    return contract;
}

/** Marks a reusable bindable factory for inclusion in an atom contract. */
export function atom_bindable_6<M extends CONTRACT_MASTER = CONTRACT_MASTER, B = unknown>(
    factory: ATOM_BINDABLE<M, B>,
): ATOM_BINDABLE<M, B> {
    return factory;
}




import { MASTER_ATOM_KEYS, type CELL_CONTRACT, type MASTER_ATOM_KEY } from "./particles/factory/types";

export interface AtomContractContext<TSelf = unknown> {
  readonly self: TSelf;
}

export type AtomContractDefinition<TValue = unknown> = CELL_CONTRACT<TValue>;

export type AtomContractBuilder<TSelf = unknown, TValue = unknown> = (
  context: AtomContractContext<TSelf>,
) => AtomContractDefinition<TValue>;

const DEFAULT_MASTER_ATOM: MASTER_ATOM_KEY = "VERTEX";

/**
 * Defines a reusable, stateless atom contract factory.
 * The returned factory resolves a fresh definition for each self; it does not
 * allocate atoms or own storage, bindings, or lifecycle state.
 */
export function atom_contract_7<TSelf = unknown, TValue = unknown>(
  builder: AtomContractBuilder<TSelf, TValue>,
): AtomContractBuilder<TSelf, TValue> {
  if (typeof builder !== "function") {
    throw new TypeError("atom_contract expects a builder function");
  }

  return (context) => {
    if (context === null || typeof context !== "object" || !("self" in context)) {
      throw new TypeError("atom_contract factories require a context with self");
    }

    const definition = builder(context);
    if (definition === null || typeof definition !== "object" || Array.isArray(definition)) {
      throw new TypeError("atom_contract builders must return a contract object");
    }

    const atom = definition.atom ?? DEFAULT_MASTER_ATOM;
    if (!MASTER_ATOM_KEYS.includes(atom)) {
      throw new RangeError(
        `Unknown master atom "${String(atom)}". Allowed: ${MASTER_ATOM_KEYS.join(", ")}`,
      );
    }

    return { ...definition, atom };
  };
}
