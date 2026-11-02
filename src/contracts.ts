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
