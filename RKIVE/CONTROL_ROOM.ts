
import {  getCallerLocation, withTypoGuard, resolveFactory ,buildCtx , zeroTwoPassCollection,  zeroNexusMirror,makeStub ,zeroArcaneRegistry,     } from "#HPR";
import type {  AllIncantationTypes, AnyFactory, AnyObject, Factory, IncantationsResult,    MirrorImageFactory, } from "#HPR";
import { BC } from "../factory/BEAM_CONTROLLER";
import { ELECTRON_DEFINITION, MASTER_ATOM_KEYS, MQ_DEFINITION, NEUTRONS_DEFINITION, PROTONS_DEFINITION, type ATOM_KEY, type CELL_CONTRACT, type CONSTITUENT_ROLE, type MASTER_ATOM_KEY } from "../factory/types";
import { ATOM } from "../master_atoms/ATOM";
import { CLOUD_ATOM } from "../master_atoms/CLOUD";
import { DRS_ATOM } from "../master_atoms/DRS";
import { DUPLEX_ATOM } from "../master_atoms/DUPLEX";
import { MODULE_ATOM } from "../master_atoms/MODULE";
import { S_CLASS_ATOM } from "../master_atoms/S_CLASS";
import { SEND_ATOM } from "../master_atoms/SEND";
import { SLAB_ATOM } from "../master_atoms/SLAB";
import { SPREADSHEET_ATOM } from "../master_atoms/SPREADSHEET";
import { STREAM_ATOM } from "../master_atoms/STREAM";
import { VERTEX } from "../master_atoms/VERTEX";
import { REGISTRY_PROTON } from "../protons/registry";

export function CONTROL_ROOM<C extends AnyObject | Factory>(arg1: string | C, arg2?: C) {
  // ── PASS 1: collect constants ──────────────────────────────────────────────
  const categoryName = typeof arg1 === 'string' ? arg1 : 'incantations_Lvl2';
  const location = getCallerLocation()
  const rawConfig = (typeof arg1 === 'string' ? arg2! : arg1) as AnyObject | Factory;
  if (rawConfig === undefined || rawConfig === null) throw new Error(`Self is required, define self in your props at ${location}`)
  console.log('Incantations_lvl2 called with:', {
    arg1,
    arg2,
    rawConfig,
    typeofArg1: typeof arg1,
    typeofRawConfig: typeof rawConfig
  });
  const factory: Factory = resolveFactory(rawConfig);
  const refs = zeroNexusMirror(categoryName);
  // ── STUB PROXY ENGINE ──────────────────────────────────────────────────────
 const stubProxy = new Proxy({}, { get: () => makeStub() });
   // const stubProxy = {};
  // ── PASS 1: COLLECT VECTOR CONSTANTS & SPECS ───────────────────────────────
  const pass1 = factory(buildCtx(refs, stubProxy));
  const collectedConstants: Record<string, any> = {}
  const collectedSpecs: Record<string, any> = {};

  for (const key in pass1) {
    if (pass1[key]?.constants) collectedConstants[key] = pass1[key].constants;
    if (pass1[key]?.specs) collectedSpecs[key] = pass1[key].specs;
  }

  // ── PASS 2: BIND REFLECTIVE RUNTIME RESOLUTION PROXIES ──────────────────────
  const { pass2 } = zeroTwoPassCollection(categoryName, factory);

  const result: any = {};
  for (const key in pass2) {
    let nodeDef = { 
      ...pass2[key],
      __isAtomContract: true,
      __atomContract: pass2[key],
     };
    // nodeDef = applyTypeGraph(nodeDef, key);
    result[key] = zeroRunNodePipeline(key, nodeDef, pass2, location, categoryName);
  }  type R = ReturnType<AnyFactory>; 
  
  zeroArcaneRegistry(categoryName, result)

  return withTypoGuard(result, location, categoryName) as {
    [K in keyof R]: R[K] extends {
      __S__?: infer S extends string;
      __V__?: infer V extends Record<string, Record<string, any>>;
      __T__?: infer T;
      __I__?: infer I;
      __E__?: infer E;
      __C__?: infer C;
      __SP__?: infer SP;
      __SO__?: infer SO;
      __CL__?: infer CL
    }
    ? IncantationsResult<S, V, T, I, C, SP, SO, CL, E, ReturnType<NonNullable<R[K]['global']>>>  & (ReturnType<NonNullable<R[K]['global']>> extends infer G ? G : {})
    : IncantationsResult<never, Record<string, Record<string, any>>>
  } & { incan_types: AllIncantationTypes }  as unknown as  MirrorImageFactory<C> // MirrorImage<C>;
}
const PARTICLE_KEYS = {
  ...PROTONS_DEFINITION,
  ...NEUTRONS_DEFINITION,
  ...ELECTRON_DEFINITION,
  ...MQ_DEFINITION,
} as const;

// Build once: lowercase contract tag ("ttl") -> class name ("TTL_NEUTRON").
// No second hand-written list — derived straight from the symbol descriptions
// that are already sitting in the four definition objects.
const PARTICLE_TAG_TO_CLASS_NAME: Record<string, keyof typeof PARTICLE_KEYS> = (() => {
  const index = {} as Record<string, keyof typeof PARTICLE_KEYS>;
  for (const className of Object.keys(PARTICLE_KEYS) as (keyof typeof PARTICLE_KEYS)[]) {
    const tag = PARTICLE_KEYS[className].description!; // "ttl", "history", etc.
    index[tag] = className;
  }
  return index;
})();

const PARTICLE_ROLE_INDEX: Record<string, CONSTITUENT_ROLE> = (() => {
  const index: Record<string, CONSTITUENT_ROLE> = {};
  for (const k of Object.keys(PROTONS_DEFINITION)) index[PROTONS_DEFINITION[k].description!] = "proton";
  for (const k of Object.keys(NEUTRONS_DEFINITION)) index[NEUTRONS_DEFINITION[k].description!] = "neutron";
  for (const k of Object.keys(ELECTRON_DEFINITION)) index[ELECTRON_DEFINITION[k].description!] = "electron";
  for (const k of Object.keys(MQ_DEFINITION)) index[MQ_DEFINITION[k].description!] = "neutron";
  return index;
})();
function inferRole(tag: string): CONSTITUENT_ROLE {
  return PARTICLE_ROLE_INDEX[tag] ?? "electron";
}
export function zeroAtomContractPipeline({
  key, nodeDef, siblings, location, categoryLabel,
}: { key: string; nodeDef: any; siblings: Record<string, any>; location: string; categoryLabel: string }): any {
  const contract: CELL_CONTRACT = nodeDef.__atomContract;
  const masterKind: MASTER_ATOM_KEY = contract.atom ?? "VERTEX";

  if (!(MASTER_ATOM_KEYS as readonly string[]).includes(masterKind)) {
    throw new Error(
      `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m Unknown master atom "${String(masterKind)}" on "${key}".\n` +
      `  -> Allowed: ${MASTER_ATOM_KEYS.join(", ")}\n  -> Defined at: ${location}\n`
    );
  }

  const value = resolveInitialValue(contract.value);
  const atomKey = contract.key ?? key;

  const existing = BC().ATOM_AT("root_cell", atomKey);
  if (existing) return existing;

  const master = createMasterAtom(masterKind, atomKey, value);

  if (contract.registry_address !== undefined) {
    const target = BC().ATOM_AT("root_cell", contract.registry_address);
    if (!target?.registry) {
      throw new Error(
        `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m registry_address "${String(contract.registry_address)}" on "${key}" does not point at an existing bound atom.\n` +
        `  -> Defined at: ${location}\n`
      );
    }
    master.registry = target.registry;
  }

  for (const tag of Object.keys(contract)) {
    if (RESERVED.has(tag) || DISPATCH_ONLY.has(tag)) continue;
    const spec = contract[tag];
    if (spec === undefined || spec === null || spec === false) continue;

    const className = PARTICLE_TAG_TO_CLASS_NAME[tag];
    if (!className) {
      throw new Error(
        `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m Unknown particle "${tag}" on master atom "${key}".\n` +
        `  -> Available: ${Object.keys(PARTICLE_TAG_TO_CLASS_NAME).join(", ")}\n  -> Defined at: ${location}\n`
      );
    }

    const Particle = (PARTICLE_CLASSES as any)[className];
    const shard = new Particle(atomKey, spec === true ? {} : spec);
    master.bind(shard, contract.registry_address !== undefined ? "shared" : "bespoke");
  }

  BC().ATOM_PUT("root_cell", atomKey, master);
  return master;
}
const MASTER_ATOM_CONSTRUCTORS: Record<  MASTER_ATOM_KEY,  any> = {
  ATOM,
  MODULE_ATOM,
  CLOUD_ATOM,
  SAURON_ATOM: ATOM,
  DRS_ATOM,
  DUPLEX_ATOM,
  SLAB_ATOM,
  STREAM_ATOM,
  S_CLASS_ATOM,
  SPREADSHEET_ATOM,
  SEND_ATOM,
  VERTEX,
};
// ── NODE PIPELINE ────────────────────────────────────────────────────
export function zeroRunNodePipeline(
  key: string,
  nodeDef: any,
  siblings: Record<string, any>,
  location: string,
  categoryLabel: string
): any {
  nodeDef.__location = location;
  nodeDef.__definitionName = key;

  if (nodeDef.__isAtomContract) {
    return zeroAtomContractPipeline({
      key,
      nodeDef,
      siblings,
      location,
      categoryLabel,
    });
  }

  return nodeDef;
}
// ── VALUE + MASTER RESOLUTION ────────────────────────────────────────
function resolveInitialValue(value: any): any {
  if (typeof value === "function") {
    return value();
  }

  return value;
}
function createMasterAtom(
  masterKind: MASTER_ATOM_KEY,
  atomKey: ATOM_KEY,
  value: any
): any {
  const MasterAtom =
    MASTER_ATOM_CONSTRUCTORS[masterKind];

  if (!MasterAtom) {
    throw new Error(
      `[control-room] No constructor registered for master atom "${masterKind}".`
    );
  }

  return new MasterAtom(atomKey, value);
}
// ── BINDING RESOLUTION ───────────────────────────────────────────────
function collectAtomBindings({
  contract,
  masterKey,
  label,
  categoryLabel,
  location,
}: {
  contract: CELL_CONTRACT;
  masterKey: ATOM_KEY;
  label: string;
  categoryLabel: string;
  location: string;
}): SLOT[] {
  const bindings: SLOT[] = [];

  for (const name of Object.keys(contract)) {
    if (RESERVED.has(name)) continue;

    const spec = contract[name];

    if (spec === undefined || spec === null || spec === false) {
      continue;
    }

    if (typeof spec === "function") {
      bindings.push({
        atom: spec(masterKey),
        owner: "bespoke",
        role: inferRole(name),
      });

      continue;
    }

    if (
      typeof spec === "object" &&
      "root_cell_key" in spec
    ) {
      bindings.push({
        atom: spec,
        owner: "shared",
        role: inferRole(name),
      });

      continue;
    }

    const Particle = PARTICLE_KEYS[name];

    if (!Particle) {
      throw new Error(
        `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m ` +
        `Unknown particle "${name}" on master atom "${label}".\n` +
        `  -> Available: ${Object.keys(PARTICLE_KEYS).join(", ")}\n` +
        `  -> Defined at: ${location}\n`
      );
    }

    bindings.push({
      atom: new Particle(
        masterKey,
        spec === true ? {} : spec
      ),
      owner: "bespoke",
      role: inferRole(name),
    });
  }

  return bindings;
}
// ── RESERVED CONTRACT FIELDS ────────────────────────────────────────
// Only 3 values live on a master atom contract. `atom` is the master-kind
// dispatch selector, not a stored value, but it still has to be skipped
// when walking the contract for particle bindings.
const RESERVED = new Set([
  "value",
  "key",
  "registry_address",
]);
const DISPATCH_ONLY = new Set(["atom"]); // excluded from bindings, not a contract value
// ── PARTICLE_KEYS ────────────────────────────────────────────────────
// No duplicated list. Merge the four symbol maps that already exist,
// and build a reverse index once so inferRole is O(1) instead of
// walking four objects per binding.

// ── REGISTRY ADDRESS ────────────────────────────────────────────────
// A particle's registry field name on REGISTRY_PROTON, e.g. "ttl" -> "_TTL_NEUTRON".
// Derived once from PARTICLE_KEYS so there's no second hand-maintained list.
function registryFieldFor(particleName: string): string {
  return `_${particleName.toUpperCase()}`;
}
function getOrCreateRegistry(atomKey: ATOM_KEY): REGISTRY_PROTON {
  const existing = BC().ATOM_AT_PARTICLE?.("_REGISTRY_PROTON_", atomKey)
    ?? BC().store.particle("_REGISTRY_PROTON_", "_ATOMS_", "_THE_WIZARD_")[atomKey as any];
  if (existing) return existing;

  const registry = new REGISTRY_PROTON(atomKey);
  BC().store.particle("_REGISTRY_PROTON_", "_ATOMS_", "_THE_WIZARD_")[atomKey as any] = registry;
  return registry;
}
// ── CONTRACT PIPELINE ────────────────────────────────────────────────

function stageAtomBindings({
  contract,
  registry,
  masterKey,
  label,
  categoryLabel,
  location,
}: {
  contract: CELL_CONTRACT;
  registry: REGISTRY_PROTON;
  masterKey: ATOM_KEY;
  label: string;
  categoryLabel: string;
  location: string;
}): void {
  // particles for THIS master atom live in their own metaMap slot,
  // keyed by masterKey — not the shared/global one.
  const slots = BC().store.particle(masterKey as S, "_ATOMS_", "_THE_WIZARD_");

  for (const name of Object.keys(contract)) {
    if (RESERVED.has(name) || DISPATCH_ONLY.has(name)) continue;

    const spec = contract[name];
    if (spec === undefined || spec === null || spec === false) continue;

    if (!(name in PARTICLE_KEYS)) {
      throw new Error(
        `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m ` +
        `Unknown particle "${name}" on master atom "${label}".\n` +
        `  -> Available: ${Object.keys(PARTICLE_KEYS).join(", ")}\n` +
        `  -> Defined at: ${location}\n`
      );
    }

    const field = registryFieldFor(name); // "ttl" -> "_TTL_NEUTRON"
    if (!(field in registry)) {
      throw new Error(
        `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m ` +
        `Particle "${name}" has no matching REGISTRY_PROTON field "${field}".\n` +
        `  -> Defined at: ${location}\n`
      );
    }
    if (!(field in slots)) {
      throw new Error(
        `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m ` +
        `Particle "${name}" has no matching storage slot "${field}" on this master atom.\n` +
        `  -> Defined at: ${location}\n`
      );
    }

    // No eager instantiation — stage the spec at its slot. First real
    // access builds it (via whatever lazy getter sits on the master atom).
    slots[field] = { __pending: true, spec: spec === true ? {} : spec, role: inferRole(name) };

    // Registry stores ONLY the address back to that slot — never the instance.
    (registry as any)[field] = masterKey;
  }
}
