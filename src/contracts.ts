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
export function atom_contract<TSelf = unknown, TValue = unknown>(
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