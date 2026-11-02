ive alrady done almost that entire list... im litreally  just trying to gauge ur perforamnce today before giving u real work..... my signals/atom solution is already like 100x more performant..... more configurable and complex... while actaull ystill being easier.... ships with a lot more with it....becasuse i finally mananged to break away the foundaltional build blocks of my platform... and palce thme into their own sepearate modules... and rightr now i just pulled off my real signals solution... i was going to place it with the other reactive parts of my paltform... in the same module they are going in.. adn then i remember i was almost done my new atom solution.... and i want it pretty badly... so we need to finalzie.... exactly how the contracts work... basically... there are 5 master atoms a user can start with , all dependant on what it is that the value they are saving.... it defaults to the fastr/hot path wherneve r auser does not reach for a contracted atom...  but simple goes for const atom = signal()

or whatevre

the master atoms only have 3 values, 
value
key
registry address
a user cant creaste a another master atom witthout the contract singe it is what binds atoms together becasue there is not graph or god aatom
which makes it even more performant and more importantly effeiceint.... as all the atoms  that bind to it, sit in a regissrty till called or used.....
ther is obviousl ymore to it but for now we just need to finish of the contract aspect befreo moving on as its one of the most imopratn things that needs to be done
there are ovre 60+ atoms that can bind to a master atom... ok?

this time around im not going to give u so much freedom... since it seems to have a negative effect in the past we are going to start with copying over a other platform primtitive so that its the same shape for th euser as the other primtiives

zeroRunNodePipeline is where we can add all of our functionality concerning the contracts and what each does, configuration and everything.... if u need more information let me know and i can provide it 

i am going to give you the unfinished beam_controller and severael bindable atoms so you can atleast see them... because they are already real things...





import {  getCallerLocation, withTypoGuard, resolveFactory ,buildCtx , zeroTwoPassCollection,  zeroNexusMirror,makeStub ,zeroArcaneRegistry,     } from "#HPR";
import type {  AllIncantationTypes, AnyFactory, AnyObject, Factory, IncantationsResult,    MirrorImageFactory, } from "#HPR";
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



export type MASTER_ATOM_KEYS  = 
| "ATOM" // ATOMS, SIGNALS
| "MODULE_ATOM" // NODE MODULES, LOCALLY SOURCED MODULES, NAMED IMPORTS, CLASSES, FNS, ETC...
| "CLOUD_ATOM" // THE APPS CONTEXT / STATE
| "SAURON_ATOM" // REACTIVE HOOK CONTEXT
| "DRS_ATOM" // DISTRIBUTED REACTIVE STATE CONTEXT
| "DUPLEX_ATOM" //MESSAGE QUEUE DUPLEX
| "SLAB_ATOM" // USER OPT IN BINARY / BYTE STORAGE, IN ADDITION TO OVERFLOW FOR DISRUPTOR
| "STREAM_ATOM" // MQ, STREAM 
| "S_CLASS_ATOM" // THE S TIER OF CLASSES, THIS IS THAT SOLUTIONS STORAGE BACKEND
| "SPREADSHEET_ATOM" // MATH AND THINGS BRUH
| "SEND_ATOM" // MQ SEND
| "VERTEX" // THE DEFAULT ATOM, WHICH IS ALSO THE HOT AS FUCK PATH
export const MASTER_ATOM_KEYS = [
  "ATOM",
  "MODULE_ATOM",
  "CLOUD_ATOM",
  "SAURON_ATOM",
  "DRS_ATOM",
  "DUPLEX_ATOM",
  "SLAB_ATOM",
  "STREAM_ATOM",
  "S_CLASS_ATOM",
  "SPREADSHEET_ATOM",
  "SEND_ATOM",
  "VERTEX",
] as const;
export type MASTER_ATOM_KEY = (typeof MASTER_ATOM_KEYS)[number];
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
export const MQ_DEFINITION = {
  RETRY: DEFINE_ATOM_KIND("retry"),
  ACK: DEFINE_ATOM_KIND("ack"),
  DEADLINE: DEFINE_ATOM_KIND("deadline"),
  DEDUPE: DEFINE_ATOM_KIND("dedupe"),
  DEPTH: DEFINE_ATOM_KIND("depth"),
  WILDCARD: DEFINE_ATOM_KIND("wildcard"),
  REPLY: DEFINE_ATOM_KIND("reply"),
  WINDOW: DEFINE_ATOM_KIND("window"),
  RESUME: DEFINE_ATOM_KIND("resume"),
  CREDIT: DEFINE_ATOM_KIND("credit"),
  CORRELATION: DEFINE_ATOM_KIND("correlation")
} as const;
export const ELECTRON_DEFINITION = {
  ADAPTER_ELECTRON: DEFINE_ATOM_KIND("adapter"),
  BEAM_CODEC_ELECTRON: DEFINE_ATOM_KIND("beam_codec"),
  BINARY_ELECTRON: DEFINE_ATOM_KIND("binary"),
  BYTE_CODE_VM: DEFINE_ATOM_KIND("byte_code_vm"),
  CHANNEL_ELECTRON: DEFINE_ATOM_KIND("channel"),
  CHANNELS_ELECTRON: DEFINE_ATOM_KIND("channels"),
  CONCURRENCY_ELECTRON: DEFINE_ATOM_KIND("concurrency"),
  DEADLETTER_ELECTRON: DEFINE_ATOM_KIND("deadletter"),
  DESTRUCTION_ELECTRON: DEFINE_ATOM_KIND("destruction"),
  ERROR_ELECTRON: DEFINE_ATOM_KIND("error"),
  EVENT_ELECTRON: DEFINE_ATOM_KIND("event"),
  EXTERNAL_SUBSCRIPTION_ELECTRON: DEFINE_ATOM_KIND("exterenal_sub"),
  GATE_ELECTRON: DEFINE_ATOM_KIND("gate"),
  GLOBAL_SUBSCRIPTION_ELECTRON: DEFINE_ATOM_KIND("global_sub"),
  GOVERNER_ELECTRON: DEFINE_ATOM_KIND("governer"),
  HOOK_ELECTRON: DEFINE_ATOM_KIND("hook"),
  HTTP_TRANSPORT: DEFINE_ATOM_KIND("http_transport"),
  INJECTION_CODEC_ELECTRON: DEFINE_ATOM_KIND("injection"),
  METRICS_ELECTRON: DEFINE_ATOM_KIND("metrics"),
  MIDDLEWARE_ELECTRON: DEFINE_ATOM_KIND("middlleware"),
  RIFTS_ACCESS_ELECTRON: DEFINE_ATOM_KIND("rifts"),
  REACTIVE_ELECTRON: DEFINE_ATOM_KIND("reactive"),
  ROUTE_ELECTRON: DEFINE_ATOM_KIND("route"),
  RUN_ON_ELECTRON: DEFINE_ATOM_KIND("run_on"),
  SEALED_ERROR_ELECTRON: DEFINE_ATOM_KIND("sealed_error"),
  SESSION_ELECTRON: DEFINE_ATOM_KIND("session"),
  SHELL_ELECTRON: DEFINE_ATOM_KIND("reshelltry"),
  SIGNAL_ELECTRON: DEFINE_ATOM_KIND("signal"),
  SOA_QUAD_TIERED_REGISTRY_ELECTRON: DEFINE_ATOM_KIND("soa_registry"),
  SUBSCRIPTION_ELECTRON: DEFINE_ATOM_KIND("subscription"),
  TRANSFORM_ELECTRON: DEFINE_ATOM_KIND("transform"),
  WAITER_ELECTRON: DEFINE_ATOM_KIND("waiter"),
}as const;
export const NEUTRONS_DEFINITION = {
  COMPRESSION_NEUTRON: DEFINE_ATOM_KIND("compression"),
  CONFLICT_NEUTRON: DEFINE_ATOM_KIND("conflict"),
  CONTEXT_NEUTRON: DEFINE_ATOM_KIND("context"),
  ENCRYPTION_NEUTRON: DEFINE_ATOM_KIND("encryption"),
  GROUPS_NEUTRON: DEFINE_ATOM_KIND("groups"),
  HISTORY_NEUTRON: DEFINE_ATOM_KIND("history"),
  META_NEUTRON: DEFINE_ATOM_KIND("meta"),
  OFFSEC_NEUTRON: DEFINE_ATOM_KIND("offsec"),
  PERMISSIONS_NEUTRON: DEFINE_ATOM_KIND("permissions"),
  PERSISTENCE_NEUTRON: DEFINE_ATOM_KIND("persistence"),
  PRIORITY_NEUTRON: DEFINE_ATOM_KIND("priority"),
  SNAPSHOT_NEUTRON: DEFINE_ATOM_KIND("snapshot"),
  TAGS_NEUTRON: DEFINE_ATOM_KIND("tags"),
  TIME_TRAVEL_NEUTRON: DEFINE_ATOM_KIND("time_travel"),
  TTL_NEUTRON: DEFINE_ATOM_KIND("ttl"),
}as const;
export const PROTONS_DEFINITION = {
  CONTRACTS_PROTON: DEFINE_ATOM_KIND("contract"),
  REACTIVE_PROTON: DEFINE_ATOM_KIND("reactive"),
  REGISTRY_PROTON: DEFINE_ATOM_KIND("registry"),
  SCHEMA_PROTON: DEFINE_ATOM_KIND("schema"),
  VALUES_REGISTRY_PROTON: DEFINE_ATOM_KIND("values_reg"),
}as const;
export function DEFINE_ATOM_KIND(name: string): symbol {
	return Symbol(name);
}
export interface NODE {
  readonly key: ATOM_KEY;
  destroy(): void;
}
export type ATOM_TYPE = A;
export type ROOT = ATOM<A> | NU;
export type ATOM_CLASS<S extends object> = new (...args: A[]) => S;
export interface CHANGE<X> {
	key: ATOM_KEY;
	previous: X;
	current: X;
}
export type LISTENER<X> = (change: CHANGE<X>) => V;
export type UNSUBSCRIBE = () => V;
/** The role a constituent plays when bound to an atom.
 *  - proton:   identity-bearing, load-bearing. Changes what the atom *is*.
 *  - neutron:  tuning/policy/stabilizer. Same atom, different isotope.
 *  - electron: behavior/interaction. Split by exposure (core vs valence).
 */
export interface SLOT {
	atom: ATOM;
	owner: OWNERSHIP;
	/** Which constituent bucket this binding occupies. */
	role?: CONSTITUENT_ROLE;
	/** Only meaningful when role === "electron". Defaults to "valence". */
	shell?: ELECTRON_SHELL;
}
export const SUBSCRIPTIONS = DEFINE_ATOM_KIND("subscriptions");
export const GATE = DEFINE_ATOM_KIND("gate");
export type ATOM_KEY = string | number | symbol;
export type OWNERSHIP = "bespoke" | "shared";
export type CONSTITUENT_ROLE =
  | "proton"
  | "neutron"
  | "electron";
export type ELECTRON_SHELL = "core" | "valence";
export interface SLOT {
  atom: any;
  owner: OWNERSHIP;
  role?: CONSTITUENT_ROLE;
  shell?: ELECTRON_SHELL;
}
export interface CELL_CONTRACT<X> {
  value?: X | (() => X);
  key?: ATOM_KEY;
  docs?: any;
  atom?: MASTER_ATOM_KEY;
  [particle: string]: any;
}
export interface CONTROL_ROOM_CTX<X = any> {
  self: any;
  key: ATOM_KEY;
  label: string;
}
export type CONTROL_ROOM<X = any> = (  ctx: CONTROL_ROOM_CTX<X>) => CELL_CONTRACT<X>;
const RESERVED = new Set([
  "value",
  "key",
  "docs",
  "atom",
]);
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

export function zeroAtomContractPipeline({
  key,
  nodeDef,
  siblings,
  location,
  categoryLabel,
}: {
  key: string;
  nodeDef: any;
  siblings: Record<string, any>;
  location: string;
  categoryLabel: string;
}): any {
  const contract: CELL_CONTRACT = nodeDef.__atomContract;

  const masterKind: MASTER_ATOM_KEY =
    contract.atom ?? "VERTEX";

  if (
    !(MASTER_ATOM_KEYS as readonly string[]).includes(masterKind)
  ) {
    throw new Error(
      `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m ` +
      `Unknown master atom "${String(masterKind)}" on "${key}".\n` +
      `  -> Allowed: ${MASTER_ATOM_KEYS.join(", ")}\n` +
      `  -> Defined at: ${location}\n`
    );
  }

  const value = resolveInitialValue(contract.value);
  const atomKey = contract.key ?? key;

  const existing = BC().ATOM_AT("root_cell", atomKey);

  if (existing) {
    return existing;
  }

  const master = createMasterAtom(
    masterKind,
    atomKey,
    value
  );

  BC().ATOM_PUT("root_cell", atomKey, master);

  const bindings = collectAtomBindings({
    contract,
    masterKey: atomKey,
    label: key,
    categoryLabel,
    location,
  });

  for (const binding of bindings) {
    master.bind(binding.atom, binding.owner);
  }

  return master;
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
function inferRole(particleName: string): CONSTITUENT_ROLE {
  if (particleName in PROTONS_DEFINITION) {
    return "proton";
  }

  if (particleName in NEUTRONS_DEFINITION) {
    return "neutron";
  }

  if (particleName in ELECTRON_DEFINITION) {
    return "electron";
  }

  if (particleName in MQ_DEFINITION) {
    return "neutron";
  }

  return "electron";
}



// #endregion


import { useSXX } from './ssx';
import ZeroStore from './zero_store';
export function zeroArcaneRegistry(name, result) {
    const [s, setS] = useSXX(false); //useState(false)
    if (s === false) {
        for (const [key, values] of Object.entries(ZeroStore.getAll())) {
            const existing = ZeroStore.get(key);
            if (existing !== undefined)
                continue;
            for (const value of values) {
                ZeroStore.set(key, value);
            }
        }
        setS(true);
    }
}

export const STUB_HITS = [];
const NINJA_STUB_BRAND = '__is_ninja_stub__';
export function isStub(value) {
    try {
        return !!value && (typeof value === 'object' || typeof value === 'function') && value[NINJA_STUB_BRAND] === true;
    }
    catch {
        return false;
    }
}
export function makeStub(trace) {
    return new Proxy(() => makeStub(trace), {
        get: (_t, prop) => {
            if (prop === NINJA_STUB_BRAND)
                return true;
            if (prop === Symbol.toPrimitive || prop === 'toString' || prop === 'valueOf')
                return () => '';
            if (trace && typeof prop === 'string') {
                STUB_HITS.push({ ...trace, prop });
            }
            return makeStub(trace);
        },
        apply: () => { if (trace)
            STUB_HITS.push({ ...trace, prop: '()' }); return makeStub(trace); },
    });
}
export function flushStubReport() {
    if (!STUB_HITS.length)
        return;
    console.table(STUB_HITS);
    STUB_HITS.length = 0;
}
// #endregion
import { getCallerLocation } from "./get_caller_location";
import { makeStub } from "./stubs";
import ZeroStore from './zero_store';
export function zeroNexusMirror(excludeName) {
    return new Proxy({}, {
        get: (_t, name) => {
            if (name === excludeName)
                return makeStub({ category: excludeName, key: name, location: getCallerLocation() });
            return new Proxy([], {
                get: (_s, prop) => {
                    const arr = ZeroStore.get(name);
                    if (!arr)
                        return makeStub({ category: name, key: String(prop), location: getCallerLocation() });
                    if (prop in arr)
                        return arr[prop];
                    return makeStub({ category: name, key: String(prop), location: getCallerLocation() });
                }
            });
        }
    });
}
import { buildCtx } from "./build_ctx";
import { makeStub } from "./stubs.js";
import { zeroNexusMirror } from "./zero_nexus_mirror";
export function zeroTwoPassCollection(categoryName, factory) {
    const refs = zeroNexusMirror(categoryName);
    const stubProxy = new Proxy({}, { get: () => makeStub() });
    const pass1 = factory(buildCtx(refs, stubProxy));
    const collectedConstants = {};
    const collectedSpecs = {};
    for (const key in pass1) {
        if (pass1[key]?.constants)
            collectedConstants[key] = pass1[key].constants;
        if (pass1[key]?.specs)
            collectedSpecs[key] = pass1[key].specs;
    }
    const resolveProxy = new Proxy({}, {
        get: (_t, key) => new Proxy({}, {
            get: (_s, prop) => {
                if (prop === 'constants') {
                    const real = collectedConstants[key];
                    if (!real)
                        return makeStub();
                    return new Proxy(real, { get: (t, p) => (p in t ? t[p] : makeStub()) });
                }
                if (prop === 'specs') {
                    const real = collectedSpecs[key];
                    if (!real)
                        return makeStub();
                    return new Proxy(real, { get: (t, p) => (p in t ? t[p] : makeStub()) });
                }
                return makeStub();
            }
        })
    });
    const pass2 = factory(buildCtx(refs, resolveProxy));
    return { pass2, refs };
}

export function buildCtx(refs, selfProxy) {
    // const v = await maelstrom.RIFTS.get("JEDI").wait()
    let builtins = {
        self: selfProxy,
        s: null,
    };
    /**	DEFER_.run("JEDI", (JEDI) => {
     builtins = {
         self: selfProxy,
         s: JEDI,
     };
    }); */
    return new Proxy(builtins, {
        get: (target, key) => (key in target ? target[key] : refs[key]),
    });
}
export function initbuildCtx(refs, selfProxy) {
    // const v = await maelstrom.RIFTS.get("JEDI").wait()
    let builtins = {
        self: selfProxy,
    };
    return new Proxy(builtins, {
        get: (target, key) => (key in target ? target[key] : refs[key]),
    });
}


export function zeroRunNodePipeline(key, nodeDef, siblings, location, categoryLabel) {
    nodeDef.__location = location;
    nodeDef.__definitionName = key;
    // #endregion
    return nodeDef;
}

export function resolveFactory(raw) {
    if (typeof raw === 'function')
        return raw;
    return () => raw;
}
export function resolveVaultFactory(raw) {
    if (typeof raw === 'function')
        return raw;
    return () => raw;
}
export function resolveAnyFactory(raw) {
    if (typeof raw === 'function')
        return raw;
    return () => raw;
}
// #region ── TYPEO GUARD ────────────────────────────────────────────────────────────────── 
export function withTypoGuard(result, location, label, allowedExtraKeys = ['freyr']) {
    return new Proxy(result, {
        get(target, prop, receiver) {
            if (typeof prop === 'symbol' || prop in target || allowedExtraKeys.includes(prop)) {
                return Reflect.get(target, prop, receiver);
            }
            const validKeys = Object.keys(target).join(', ');
            throw new Error(`\n\x1b[31m[${label} Typo Error]\x1b[0m Property \x1b[33m"${String(prop)}"\x1b[0m does not exist on this ${label} definition.\n` +
                `  -> \x1b[32mDid you make a spelling mistake?\x1b[0m\n` +
                `  -> \x1b[35mAvailable definitions:\x1b[0m [ ${validKeys} ]\n` +
                `  -> \x1b[36mDefined at:\x1b[0m ${location}\n`);
        }
    });
}
// #endregion
export type ObjectType = Record<string, IncantationsConfig<any, any, any, any, any, any, any, any, any, any>>;
export type SelfType = { self: any;[category: string]: any };
export type Factory = (ctx: SelfType) => ObjectType


export type AnySelfType = { self: any;[key: string]: any };
export type AnyObject = Record<string, any>;
export type AnyFactory = (ctx: AnySelfType) => AnyObject;
export type AnyType = AnyObject | AnyFactory;


export type AO = Record<string, any>;
export type AS = { self: any;[key: string]: any };
export type AF = (ctx: AS) => AnyObject;
export type AOAF = AnyObject | AnyFactory;


export type incan_vars_obj = Record<string, any>;
export type incan_vars_ctx = { self: any;[category: string]: any };
export type incan_vars_vault_factory = (ctx: incan_vars_ctx) => incan_vars_obj | incan_vars_obj;

import type { ATOM_KEY } from "../contract/CONTROL_ROOM";
import type { ATOM } from "../master_atoms/ATOM";
//                                                                                       BEAM_CONTROLLER
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const MODULE_CACHE = new M2<A, M<A, M<A, A>>>();
export class BEAM_CONTROLLER {
	public r = "_THE_WIZARD_";
	public m = "_CELL_";
	public s = "_ATOM_";
	public registry: BYTE_ATOM_REG;
	public BINARY_CODEC: BINARY_CODEC;
	public INJECTION_CODEC: INJECTION_CODEC;
	public BYTE_REG_CAP: N;

	constructor(reg_cap: N = 8192, BYTE_REG_CAP = 65536) {
		this.registry = new BYTE_ATOM_REG(reg_cap);
		this.BINARY_CODEC = new BINARY_CODEC();
		this.INJECTION_CODEC = new INJECTION_CODEC();
		this.BYTE_REG_CAP = BYTE_REG_CAP;
	}
	// #endregion
	//                                                                                       BEAM_CONTROLLER
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public cache_type: "sauron" | "disruptor" | "cache" = "sauron";
	public store = (() => {
		const fn = <K, V>(s: S = this.s, m: S = this.m, r: S = this.r): M<K, V> => {
			let root;
			switch (this.cache_type) {
				case "disruptor": {
					break;
				}
				case "cache": {
					root = MODULE_CACHE.get(r);
					break;
				}
				case "sauron":
				default: {
					root = (globalThis as A)[r];
					break;
				}
			}
			if (!root) {
				root = new Map<A, Map<A, Map<A, A>>>();
				switch (this.cache_type) {
					case "disruptor": {
						break;
					}
					case "cache": {
						MODULE_CACHE.clear();
						MODULE_CACHE.set(r, root);
						break;
					}
					case "sauron":
					default: {
						(globalThis as A)[r] = root;
						break;
					}
				}
			}

			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new Map<A, Map<A, A>>();
				root.set(m, masterMap);
			}
			let slaveMap = masterMap.get(`${s}_ATOMS`);
			if (!slaveMap) {
				slaveMap = new Map<K, V>();
				masterMap.set(`${s}_ATOMS`, slaveMap);
			}
			return slaveMap;
		};
		fn.particle = (s: S = this.s, m: S = this.m, r: S = this.r) => {
			let root;
			switch (this.cache_type) {
				case "disruptor": {
					break;
				}
				case "cache": {
					root = MODULE_CACHE.get(r);
					break;
				}
				case "sauron":
				default: {
					root = (globalThis as A)[r];
					break;
				}
			}
			if (!root) {
				root = new Map<A, Map<A, Map<A, A>>>();
				switch (this.cache_type) {
					case "disruptor": {
						break;
					}
					case "cache": {
						MODULE_CACHE.clear();
						MODULE_CACHE.set(r, root);
						break;
					}
					case "sauron":
					default: {
						(globalThis as A)[r] = root;
						break;
					}
				}
			}

			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new Map<any, Map<any, any>>();
				root.set(m, masterMap);
			}

			let metaMap = masterMap.get(s);
			if (!metaMap) {
				metaMap = {
					// MASTER ATOMS INCLUDE:
					// ATOM / _SIGNAL: null,
					// CLOUD
					// DRC/DRS
					// DUPLEX
					// SEND: null,
					// STREAM: null,
					// SUB: null,
					// MODULE
					// S_CLASS
					// SLAB
					// VERTEX
					_REGISTRY_PROTON: null,
					_VALUES_REGISTRY_PROTON: null,
					_REACTIVE_PROTON: null,
					_GATE_PROTON: null,
					_SCHEMA_PROTON: null,
					_CONTRACTS_PROTON: null,

					_CONFLICT_NEUTRON: null,
					_COMPRESSION_NEUTRON: null,
					_CONTEXT_NEUTRON: null,
					_ENCRYPTION_NEUTRON: null,
					_GROUPS_NEUTRON: null,
					_HISTORY_NEUTRON: null,
					_PERMISSIONS_NEUTRON: null,
					_META_NEUTRON: null,
					_OFF_SEC_NEUTRON: null,
					_PERSISTENCE_NEUTRON: null,
					_TAGS_NEUTRON: null,
					_SNAPSHOT_NEUTRON: null,
					_PRIORITY_NEUTRON: null,
					_TIME_TRAVEL_NEUTRON: null,
					_TTL_NEUTRON: null,

					_BACKPRESSURE_ELECTRON: null,
					_BEAM_CODEC_ELECTRON: null,
					_BINARY_ELECTRON: null,
					_BYTE_CODE_VM_ELECTRON: null,
					_CHANNEL_ELECTRON: null,
					_CHANNELS_ELECTRON: null,
					_CONCURRENCY_ELECTRON: null,
					_DEADLETTER_ELECTRON: null,
					_DESTRUCTION_ELECTRON: null,

					_ERROR_ELECTRON: null,
					_EVENT_ELECTRON: null,
					_EXTERNAL_SUBSCRIPTION_ELECTRON: null,
					_GATE_ELECTRON: null,
					_GLOBAL_SUBSCRIPTION_ELECTRON: null,
					_HTTP_TRANSPO_ELECTRON: null,
					_INJECTION_CODEC_ELECTRON: null,
					_METRICS_ELECTRON: null,
					_MODULES_ELECTRON: null,
					_REACTIVE_ELECTRON: null,
					_ROUTE_ELECTRON: null,
					_SCOPE_REGISTRY_ELECTRON: null,
					_SEALED_ERROR_ELECTRON: null,

					_SESSION_ELECTRON: null,
					_SHELL_ELECTRON: null,
					_SUBSCRIPTION_ELECTRON: null,
					_WAITER_ELECTRON: null,

					_TRANSFORMS_ELECTRON: null,
					_MIDDLEWARE_ELECTRON: null,
					_ADAPTER_ELECTRON: null,
					_RUN_ON_ELECTRON: null,
				};
				masterMap.set(s, metaMap);
			}
			return metaMap;
		};
		
		return fn;
	})();
	public set = (() => {
		const fn = <T extends ATOM>(atom: T, s: S = this.s, m: S = this.m, r: S = this.r): T => {
			const atoms = this.store<ATOM_KEY, ATOM>(s, m, r);
			const current = atoms.get(atom.key);

			if (current !== undefined && current !== atom) {
				throw new Error(`master atom already registered: ${String(atom.key)}`);
			}

			atoms.set(atom.key, atom);
			return atom;
		};
		//const slots = this.store.particle(s, m, r);
		fn.particle = (k: ATOM_KEY, shard: ATOM, s: S = this.s, m: S = this.m, r: S = this.r): ATOM => {
			return this.store(s, m, r).set(atom.key, atom);
		};
		return fn;
	})();
	public at = (() => {
		const fn = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): ATOM | UD => {
			return this.store<ATOM_KEY, ATOM>(s, m, r).get(k);
		};
		fn.particle = <SHARD extends ATOM = ATOM>(k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): SHARD | UD => {
			return (this.store.particle(s, m, r)[this.store.slot_key(k)] as SHARD | null) ?? undefined;
		};
		return fn;
	})();
	public get = (() => {
		const fn = () => {};
		fn.particle = <SHARD extends ATOM = ATOM>(k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): SHARD | UD => {
			return this.at.particle<SHARD>(k, s, m, r);
		};
		return fn;
	})();
	public delete = (() => {
		const fn = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): B => {
			return this.store<ATOM_KEY, ATOM>(s, m, r).delete(k);
		};
		fn.particle = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): B => {
			const slots = this.store.particle(s, m, r);
			if (!(k in slots) || slots[k] === null) return false;
			slots[k] = null;
			return true;
		};
		return fn;
	})();
	public purge = (() => {
		const fn = () => {};
		fn.particle = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): N => {
			const slots = this.store.particle(s, m, r);
			let purged = 0;
			for (const key of Object.keys(slots)) {
				const shard = slots[key];
				if (shard === null) continue;
				(shard as { purge?(): V }).purge?.();
				slots[key] = null;
				purged++;
			}
			return purged;
		};
		return fn;
	})();
}

export type ArcheTypeCtx = { self: any;[category: string]: any };
// protons/reactive_store.ts
export interface RiftRecord<T = A> {
    opened: B;
    claimed: B;
    value?: T;
    createdAt: N;
    openedAt?: N;
}

type StoreListener<T = A> = (v: RiftRecord<T>) => void;

export class REACTIVE_PROTON {
    private records = new Map<S, RiftRecord<A>>();
    private listeners = new Map<S, Set<StoreListener<A>>>();

    get<T = A>(key: S): RiftRecord<T> | undefined { return this.records.get(key) as RiftRecord<T> | undefined; }
    set<T = A>(key: S, record: RiftRecord<T>): void {
        this.records.set(key, record as RiftRecord<A>);
        const subs = this.listeners.get(key);
        if (subs) for (const l of [...subs]) { try { l(record as RiftRecord<A>); } catch { /* one bad listener never kills the rest */ } }
    }
    sub<T = A>(key: S, fn: StoreListener<T>): () => V {
        let subs = this.listeners.get(key);
        if (!subs) { subs = new Set(); this.listeners.set(key, subs); }
        subs.add(fn as StoreListener<A>);
        return () => {
            const s = this.listeners.get(key);
            if (!s) return;
            s.delete(fn as StoreListener<A>);
            if (!s.size) this.listeners.delete(key);
        };
    }
    delete(key: S): void { this.records.delete(key); this.listeners.delete(key); }
    clear(): void { this.records.clear(); this.listeners.clear(); }
    get size(): N { return this.records.size; }
}

// protons/schema.ts
import type { ATOM_KEY } from "../contract/CONTROL_ROOM";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class SCHEMA_PROTON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	public _schemas = new Map<string, CSESchema>();

	public setChannelSchema(schema: CSESchema, rt: string): void {
		this._schemas.set(rt, schema);
	}
	public getChannelSchema(rt: string): CSESchema | undefined { return this._schemas.get(rt); }
	public removeChannelSchema(rt: string): B {
		const had = this._schemas.delete(rt);
		return had;
	}
	public hasChannelSchema(rt: string): B { return this._schemas.has(rt); }
	public listChannelSchemas(): string[] { return [...this._schemas.keys()]; }
	public getAllSchemas(): Record<string, CSESchema> { return Object.fromEntries(this._schemas); }

	/** Wildcard-aware lookup: exact channel wins, else longest matching prefix before `*`. */
	public resolveSchema(rt: string): CSESchema | undefined {
		if (this._schemas.has(rt)) return this._schemas.get(rt);
		let best: CSESchema | undefined;
		let bestLen = -1;
		for (const [pattern, schema] of this._schemas) {
			const star = pattern.indexOf('*');
			if (star === -1) continue;
			if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) { best = schema; bestLen = star; }
		}
		return best;
	}

	/**
	 * Validate a message against its channel schema. No schema → pass (open channel).
	 * Returns { ok, errors } — never throws.
	 */
	public checkSchema(rt: string, msg: CSEMessage): { ok: B; errors: S[] } {
		const schema = this.resolveSchema(rt);
		if (!schema) return { ok: true, errors: [] };
		const errors: S[] = [];
		const payload = (msg.p ?? {}) as Record<S, A>;
		for (const [field, rule] of Object.entries(schema.fields ?? {})) {
			const v = payload[field];
			if (v === undefined || v === null) {
				if (rule.required) errors.push(`missing required field "${field}"`);
				continue;
			}
			if (rule.type !== 'any') {
				const actual = typeof v;
				if (rule.type === 'number' && actual !== 'number') errors.push(`field "${field}" expected number, got ${actual}`);
				else if (rule.type === 'string' && actual !== 'string') errors.push(`field "${field}" expected string, got ${actual}`);
				else if (rule.type === 'boolean' && actual !== 'boolean') errors.push(`field "${field}" expected boolean, got ${actual}`);
			}
		}
		if (schema.validate) {
			try {
				const r = schema.validate(msg);
				if (r !== true && r !== undefined) errors.push(typeof r === 'string' ? r : `validator rejected message on "${rt}"`);
			} catch (e) { errors.push(`validator threw: ${e}`); }
		}
		return { ok: errors.length === 0, errors };
	}

	purge(): void { this._schemas.clear(); }
}
// #endregion


// protons/values_registry.ts                                     VALUES_REGISTRY_PROTON

import type { ATOM_KEY } from "../contract/CONTROL_ROOM";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class VALUES_REGISTRY_PROTON {
    public root_cell_key: ATOM_KEY;
    
    constructor(root_cell_key: ATOM_KEY, _config: Record<string, unknown> = {}) {
        this.root_cell_key = root_cell_key;
    }
    public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    public hasWritten = false;
    public lastWriteSource?: string;
    public lastWriteAt?: number;
    public writeCount = 0;
    public recordWrite(source = "write"): void {
        this.hasWritten = true;
        this.lastWriteSource = source;
        this.lastWriteAt = Date.now();
        this.writeCount++;
    }
    public reset(): void {
        this.hasWritten = false;
        this.lastWriteSource = undefined;
        this.lastWriteAt = undefined;
        this.writeCount = 0;
    }
}
// #endregion

// neutrons/history.ts
//                                   HISTORY_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class HISTORY_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	public history = new Map<string, CSEMessage[]>();
	public defaultLimit = 128;

	/** Append one message. Returns the entry so callers can thread it onward. */
	public record(rt: string, msg: CSEMessage): CSEMessage {
		let list = this.history.get(rt);
		if (!list) { list = []; this.history.set(rt, list); }
		list.push(msg);
		const limit = this.limits.get(rt) ?? this.defaultLimit;
		if (list.length > limit) list.splice(0, list.length - limit);
		return msg;
	}
	private limits = new Map<string, number>();

	public setChannelLimit(rt: string, limit: number): void {
		this.limits.set(rt, limit);
		const list = this.history.get(rt);
		if (list && list.length > limit) list.splice(0, list.length - limit);
	}
	public getChannelLimit(rt: string): number { return this.limits.get(rt) ?? this.defaultLimit; }

	public getRecentHistory(rt: string, limit = 3): CSEMessage[] { return (this.history.get(rt) ?? []).slice(-limit); }
	public getChannelHistory(rt: string): CSEMessage[] { return [...(this.history.get(rt) ?? [])]; }
	public getAllHistoryChannels(): string[] { return [...this.history.keys()]; }
	public getChannelDepth(rt: string): number { return (this.history.get(rt) ?? []).length; }
	public getHistorySizes(): Record<string, number> {
		const out: Record<string, number> = {};
		for (const [ch, list] of this.history) out[ch] = list.length;
		return out;
	}
	public clearChannel(rt: string): void { this.history.delete(rt); }

	purge(): void { this.history.clear(); this.limits.clear(); }
}
// #endregion
// neutrons/groups.ts

//  ND                                  GROUPS_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class GROUPS_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
    public groups = new Map<string, { channels: Set<string>; enabled: boolean; middleware: ((msg: CSEMessage) => boolean | void)[] }>()
	    public createGroup(name: string, channels: string[] = [], options?: GroupOptions): void {
        this.groups.set(name, {
            channels: new Set(channels),
            enabled: options?.enabled ?? true,
            middleware: options?.middleware ? [options.middleware] : []
        })

    }
    public deleteGroup(name: string): void {
        this.groups.delete(name);
    }
    public enableGroup(name: string): void {
        const g = this.groups.get(name);
        if (!g) return; g.enabled = true;
    }
    public disableGroup(name: string): void {
        const g = this.groups.get(name); if (!g) return; g.enabled = false;
    }


    public addToGroup(name: string, rt: string): void {
        const g = this.groups.get(name)
        if (!g) return
        g.channels.add(rt)
    }
    public removeFromGroup(name: string, rt: string): void {
        const g = this.groups.get(name)
        if (!g) return
        g.channels.delete(rt)
    }
    public async groupBroadcast(name: string, p: any, sl = this.sl, ms = this.ms, rt = this.rt, ttl?: number): Promise<void> {
        const g = this.groups.get(name)
        if (!g || !g.enabled) return
        await Promise.all([...g.channels].map(ch => this.broadcast(p, ch, sl, ms, rt, ttl)))
    }
    public groupUse(name: string, fn: (msg: CSEMessage) => boolean | void): void {
        const g = this.groups.get(name)
        if (!g) return
        g.middleware.push(fn)
    }
	 public getAllGroups(): Record<string, { channels: string[]; enabled: boolean; middlewareCount: number }> {
        const out: Record<string, { channels: string[]; enabled: boolean; middlewareCount: number }> = {}
        for (const [name, g] of this.groups) {
            out[name] = { channels: [...g.channels], enabled: g.enabled, middlewareCount: g.middleware.length }
        }
        return out
    }
}
// #endregion
// neutrons/permissions.ts
//                                    PERMISSIONS_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class PERMISSIONS_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	public permissions = new Map<string, ChannelPermissions>();

	public setPermissions(rt: string, perms: ChannelPermissions): void {
		this.permissions.set(rt, perms);
	}
	public getPermissions(rt: string): ChannelPermissions | undefined { return this.permissions.get(rt); }
	public removePermissions(rt: string): B { return this.permissions.delete(rt); }
	public hasPermissions(rt: string): B { return this.permissions.has(rt); }
	public getAllPermissions(): Record<string, ChannelPermissions> { return Object.fromEntries(this.permissions); }
	public listProtectedChannels(): string[] { return [...this.permissions.keys()]; }

	/** Wildcard-aware lookup: exact wins, else longest matching prefix before `*`. */
	public resolvePermissions(rt: string): ChannelPermissions | undefined {
		if (this.permissions.has(rt)) return this.permissions.get(rt);
		let best: ChannelPermissions | undefined;
		let bestLen = -1;
		for (const [pattern, perms] of this.permissions) {
			const star = pattern.indexOf('*');
			if (star === -1) continue;
			if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) { best = perms; bestLen = star; }
		}
		return best;
	}

	/** Check one capability. No permission set → allowed (open channel). */
	public check(rt: string, action: 'read' | 'write' | 'send' | 'subscribe' | 'admin', role?: S): B {
		const perms = this.resolvePermissions(rt);
		if (!perms) return true;
		if (role && perms.roles && perms.roles.length && !perms.roles.includes(role)) return false;
		if (action === 'read') return perms.read;
		if (action === 'write') return perms.write;
		return perms[action] ?? perms.write;
	}

	purge(): void { this.permissions.clear(); }
}
// #endregion
// neutrons/persistence.ts



//  ND                                  PERSISTENCE_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class PERSISTENCE_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
    public durableOffsets = new Map<string, number>() // key: `${rt}:${subscriberId}`
	public persisted?: B;
    public durableHistory = new Map<string, CSEMessage[]>()
	    public getDurableHistorySizes(): Record<string, number> {
        const out: Record<string, number> = {}
        for (const [ch, hist] of this.durableHistory) out[ch] = hist.length
        return out
    }

    public async replay(options: ReplayOptions, handler: CSEHandler): Promise<void> {
        const history = this.durableHistory.get(options.rt) ?? []
        let slice = history
        if (options.fromOffset !== undefined) slice = slice.slice(options.fromOffset)
        if (options.toOffset !== undefined) slice = slice.slice(0, options.toOffset)
        if (options.fromTimestamp !== undefined) { slice = slice.filter(m => m.ts !== undefined && m.ts >= options.fromTimestamp!); }

        if (options.toTimestamp !== undefined) { slice = slice.filter(m => m.ts !== undefined && m.ts <= options.toTimestamp!); }

        for (const m of slice) handler(m.p)
    }
  public subscribeDurable(handler: CSEHandler, options: DurableOptions, k: string, sl = this.sl, ms = this.ms, rt = this.rt): () => void {
        const key = `${rt}:${options.subscriberId}`
        const offset = this.durableOffsets.get(key) ?? options.fromOffset ?? 0
        const history = this.durableHistory.get(rt) ?? []
        history.slice(offset).forEach(m => handler(m.p))
        return this.sub((p: unknown) => {
            const h = this.durableHistory.get(rt) ?? []
            const nextOffset = h.length
            this.durableOffsets.set(key, nextOffset)
            handler(p)
        }, k, sl, ms, rt)
    }
	    public getAllCheckpoints(): Record<string, number> { return Object.fromEntries(this.durableOffsets) }
	
    public async setCheckpoint(subscriberId: string, offset: number, k: string, sl?: string, ms?: string, rt?: string,): Promise<void> {
        const key = `${rt}:${subscriberId}`
        this.durableOffsets.set(key, offset)
        if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('checkpoint_set', rt, ms || undefined, undefined, { subscriberId, offset })

    }
    public async getCheckpoint(subscriberId: string, k: string, sl?: string, ms?: string, rt?: string,): Promise<number | null> {
        const key = `${rt}:${ms}:${subscriberId}`
        return this.durableOffsets.has(key) ? this.durableOffsets.get(key)! : null
    }
}
// #endregion

