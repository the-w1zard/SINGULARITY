//                                                                                              [IMPORTS]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
import { BC } from "../atom_factory/BEAM_CONTROLLER";
import { ATOM } from "../atoms/ATOM";
import { MODULE_ATOM } from "../atoms/MODULE";
import { CLOUD_ATOM } from "../atoms/CLOUD";
import { DRS_ATOM } from "../atoms/DRS";
import { DUPLEX_ATOM } from "../atoms/DUPLEX";
import { SLAB_ATOM } from "../atoms/SLAB";
import { STREAM_ATOM } from "../atoms/STREAM";
import { S_CLASS_ATOM } from "../atoms/S_CLASS";
import { SPREADSHEET_ATOM } from "../atoms/SPREADSHEET";
import { SEND_ATOM } from "../atoms/SEND";
import { VERTEX } from "../atoms/VERTEX";
import { REGISTRY_PROTON } from "../protons/registry";
export { BC, ATOM, VERTEX,  SPREADSHEET_ATOM, SEND_ATOM, S_CLASS_ATOM, STREAM_ATOM, SLAB_ATOM, DUPLEX_ATOM, DRS_ATOM, CLOUD_ATOM, MODULE_ATOM,   }
export * from '../protons' 
export * from '../protons_mq' 
export * from '../electrons' 
export * from '../electrons_mq' 
export * from '../neutrons' 
export * from '../neutrons_mq' 
// #endregion
//                                                                                                           [CONTROL_ROOM]
/*==========================================================================================================================
   BEAM_CONTROLLER: How, what and where atoms end up in their final resting storage spot, giving the user
   several option, but will default to the fast path, the vertex master atom in combination 3 tiered map
   that can be found under store, as each vertex ships with its very own lmax disruptor.

   CONTROL_ROOM: This is what defines what happens to the master atoms, protons, electrons and neutrons at each stage reltating
   to that specific atoms life cycle. For example, what happens at creation, what goes where, what values get placed from where, and other things like history, snapshot and such... the manaager is what sets that fn into stone for the paltform, so when a value changes... this manager is here to enforce what it needs to do in realtion to its being the master atom, and its bound atoms. the control room first binds a registry atom to the master atom, where the regristry atom is where all the addresses of the bound atoms are palced

   DEFAULT ATOM: VERTEX
   DEFAULT STORAGE: sauron
   Each vertex comes with its own unique disruptor that rides on the coattails of v8s performance at full tilt. The users usage can actually reach 750 mill ops per second, while just outside of the users usage it was ablie to hit 900 mill ops/sec. I did not test the user path all that much at the time, but I imagine I can get the usage hot path to around 800-850. The default does not come bound with any other particle, while the usage is simply...

   ```tsx
   const v = atom(1)
   ```
   No other master atom can be created without atleast binding a single particle to it. The contact factory automates that requirement by automatically binding the regirstry_proton, because of the fact... if you are straying off the hot path... your obviously
   going to configure it, as there is no other reason to do so.

   There will actually be two contract factory builders... CONTROL_ROOM will help users build and configure a single atom along with its bound features.

   CONTROL_ROOM_LVL2 will allow users to create mutliple master atoms in a single defined primtive, in addition to configuring each of them.

   There is no graph...
   There is no god atom...

   Each atom is a reactive signal, using a heavily modified version of alien signals. Or to be 'more specificccc'... 
   A hot-rodded fine-grained-reactive signal w/ configurable storage options that pair elegantly with 60+ features, behaviours and fns
   that can be bound to its master atom. 
   Each part of the atom and everything that ends up being bounnd to it, is only bound by contract and nothing else...
   Making each there own thing for the entire lifec cycle. Each part is only called upon, used, viewed or executed only when
   that part is called out of the master atoms registry. Other wise, it stays dormant in storage.

   This is why its so fast.
   BUT WAIT... theres more...

   With all things binary, byte and so on that relates to dealing with its many aspects, I thought it would pair nicely... 
   like a fine wine and a steak... where an entire custom, from the top down so there will be no... mongo db found here. 
   The db will feature not one, but two disruptors that both hit 901 and 898 mill ops/sec. Both disruptors featuring an entire
   different build from one another. One will focus on the storage of memory and cache data, while the other will power the bus.
   It will have 2 different persistent options that RUN IN PARALLEL, DYNAMICALLY AND AUTOMATICALLY CHOSEN BY THE DB IN WHICH
   DB THE VALUE YOU PASSED WILL GET WRITTEN TO. This will make sense in a second...
   The 2 db's are:
   1. Indexed db, making it so that when we are client side we have a choice on where to save values based on our needs 
   and reqs. So we can store them on the disruptor, or if you need some user information saved client side that needs to persist...
   we got that too.
   2. The server side will a custom written binary file kvdb. Each value saved, will be saved to its own file where the filename 
   serve as the key. The index will be stored 100% in the disruptors memory, as the disruptor is also used by both dbs, providing
   a hot path for our data needs.

   Heres the party trick... theres no bridge between the two sides... which in effect, helps continue to push its performance metrics 
   into the straosphere since the binary solution that is used for the server side, is 100% custom written and leaned out of any 
   general or generic features that you would find litter the solutions of any one of the large providers.

   Both dbs... will actually share... the same api. 
   Which means, you only have one api to learn and you get 3 dbs and a bus. Each of which, featuring either 100% disruptor speeds or at th every minimum while in the hot paths of the server and client solutions.

   If you thought, well it CAN'T GET ANY BETTER THAN THIS, riggght?
   Already, the fastest... hottest atom / signals solution.
   While at the same time, not forfeiting the large surface area in width and depth on how we can configure out atoms...

   No other solution comes close. 
   At the time of writing, you can configure your master atom, along with its particles... in excess of 64 quinttilion different
   configurations.

   Master atoms include:
   - STREAM_ATOM
   - SEND_ATOM
   - DUPLEX_ATOM
   - DRS_ATOM
   - SLAB_ATOM
   - ATOM
   - CLOUD_ATOM
   - MODULE_ATOM
   - S_CLASS_ATOM
   - SPREADSHEET_ATOM
   - VERTEX_ATOM

   Which reminds me... spreadsheet atom runs off of a disruptor as well. As it is the perfect backend to run a spreadsheet 
   runtime on.
   But anyways, taking a stream master atom, and using the contract buidler... you can configure a very basic... or a very unique stream
   to fit your use case and reqs. Bolting features like... backpresure, ttl, and more... as if you were ordering a burger from 5 guys
   and you simply adding your toppings on.

   ```tsx
  export const custom_atom = CONTROL_ROOM("ATOM_STEAM", ({ self }: any) => ({
    atom: "STREAM_ATOM",
    transport,
  }));
  
  custom_atom.stream(() => ())

  custom_atom.handler((i: A) => {}, k, s, m, r); 
  
  export const m = CONTROL_ROOM("SEND_SUB", ({ self }: any) => ({
    send: {
      ttl: {},
      garaunteed_delivery: {},
    },
    sub: {
      ttl: {},
      garaunteed_delivery: {},
    },
  }));
  
  await m.send(v, k);
  m.send(v, k);
  
  m.sub((i: A) => {}, k, s, m, r);

  export const mq = CONTROL_ROOM_LVL2("mq", {
    jobs: () => ({
      atom: "SEND_ATOM",
  
      retry: { max: 3 },
      ack: true,
      deadline: { ms: 5_000 },
    }),
  
    events: () => ({
      atom: "STREAM_ATOM",
  
      window: { high: 256 },
      resume: true,
    }),
  
    duplex: () => ({
      atom: "DUPLEX_ATOM",
  
      correlation: true,
      credit: { high: 64 },
    }),
  });
  
  mq.jobs.send(value, key);
  mq.events.stream(() => {});
  mq.duplex.open();

  export const counter = CONTROL_ROOM("counter", () => ({
    value: 0,
  
    ttl: { ms: 60_000 },
    history: true,
  }));
  ```
  
	============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [CONTROL_ROOM] */
  // ── SHARD CONTRACT ──────────────────────────────────────────────
  // A shard can be:
  // 1. true                     → default particle config
  // 2. object                   → direct particle config
  // 3. function returning spec  → configured particle, Incantations-style
  // 4. function returning ATOM  → custom shard
  // 5. object with root_cell_key → shared shard
  
  export interface SHARD_CTX<X = U> {
    /** The live master atom this shard is binding to. */
    self: A;
  
    /** The master atom's resolved key. */
    key: ATOM_KEY;
  
    /** The contract key name, e.g. "ttl", "retry", "window". */
    name: S;
  }
  
  export interface SHARD_SPEC {
    /** Which particle class to instantiate. Must exist in PARTICLE_KEYS. */
    particle: S;
  
    /** Everything else is passed to the particle constructor as its config. */
    [config: S]: U;
  }
//  
// several option, but will default to the fast path, the vertex master atom in combination 3 tiered map
// 



const RESERVED = new Set(["value", "key", "docs"]);
let ANONYMOUS = 0;

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
// CONTROL_ROOM — single master atom contract builder
// CONTROL_ROOM_LVL2 — multiple master atoms in one defined primitive
// #endregion

 

export interface CONTROL_ROOM_CTX<X = U> {
  /** Live view of the cell being built. Empty during pass 1, the real cell during pass 2. */
  self: A;
  key: ATOM_KEY;
  label: S;
}

export interface CELL_CONTRACT<X = U> {
  /** The cell's initial value, or a function that produces it. */
  value?: X | (() => X);

  /** Defaults to the label. */
  key?: ATOM_KEY;

  /** Ignored by the forge. */
  docs?: U;

  /** Every other key names a shard. */
  [atom: S]: U;
}

export type CONTROL_ROOM<X = U> = (ctx: CONTROL_ROOM_CTX<X>) => CELL_CONTRACT<X>;

function make_self<X>(  get_built: () => ATOM<X> | NU,): A {
  return new Proxy({} as A, {
    get(_target, prop) {
      if (get_built() === null) return undefined;

      const v = (get_built() as A)[prop];

      return typeof v === "function" ? v.bind(get_built()) : v;
    },

    set(_target, prop, v) {
      const built = get_built();

      if (built === null) return false;

      return Reflect.set(built, prop, v);
    },
  });
}

function build_cell<X>(  label: S,  factory: CONTROL_ROOM<X>,): ATOM<X> {
  let built: ATOM<X> | NU = null;

  const self = make_self<X>(() => built);

  const ctx: CONTROL_ROOM_CTX<X> = {
    self,
    label,
    key: label,
  };

  // ── PASS 1: read the key. A key that already exists returns the existing cell. ──
  const pass1 = factory(ctx);

  const key: ATOM_KEY = pass1.key ?? label;

  const existing = BC().ATOM_AT<ATOM<X>>("root_cell", key);

  if (existing) return existing;

  // ── Build the bare cell and place it at its address. ──
  const initial =
    typeof pass1.value === "function"
      ? (pass1.value as () => X)()
      : (pass1.value as X);

  const cell = new ATOM<X>(key, initial);

  BC().ATOM_PUT("root_cell", key, cell);

  built = cell;
  ctx.key = key;

  // ── PASS 2: rerun so self-dependent fields resolve against the real cell,
  // then build and bind the shards. The value is not re-read. ──
  const contract = factory(ctx);

  const atoms: { atom: ATOM; owner: OWNERSHIP }[] = [];

  for (const name of Object.keys(contract)) {
    if (RESERVED.has(name)) continue;

    const v = contract[name];

    if (v === undefined || v === null || v === false) continue;

    if (typeof v === "function") {
      atoms.push({
        atom: (v as (root_cell_key: ATOM_KEY) => ATOM)(key),
        owner: "bespoke",
      });

      continue;
    }

    if (typeof v === "object" && "root_cell_key" in (v as object)) {
      atoms.push({
        atom: v as ATOM,
        owner: "shared",
      });

      continue;
    }

    if (name in PARTICLE_KEYS) {
      atoms.push({
        atom: new PARTICLE_KEYS[name](key, v === true ? {} : v),
        owner: "bespoke",
      });

      continue;
    }

    throw new Error(
      `[soul-forge] "${label}": unknown contract key "${name}". ` +
      `Shards: ${Object.keys(PARTICLE_KEYS).join(", ")}. ` +
      `For a custom shard, use a function: ${name}: (key) => new YOUR_SHARD(key)`,
    );
  }

  // The registry shard exists only when at least one shard was declared.
  // Binding places each shard at its own address.
  cell.registry =
    atoms.length > 0
      ? new REGISTRY_PROTON(key, { atoms })
      : null;

  return cell;
}

export function CONTROL_ROOM<X = U>(
  arg1: S | CONTROL_ROOM<X>,
  arg2?: CONTROL_ROOM<X>,
): ATOM<X> {
  const named = typeof arg1 === "string";

  const label = named ? arg1 : `atom_${++ANONYMOUS}`;

  const factory = named ? arg2 : arg1;

  if (typeof factory !== "function") {
    throw new Error("[soul-forge] CELL_FORGE needs a factory");
  }

  return build_cell<X>(label, factory);
}

// #endregion

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
// CONTROL_ROOM_LVL2 — multiple master atoms in one defined primitive
// #endregion

export interface CONTROL_ROOM_LVL2_CTX<X = U> {
  /** Live view of the cell being built. Empty during pass 1, the real cell during pass 2. */
  self: A;

  /** Final resolved key for this cell. */
  key: ATOM_KEY;

  /** Name of this cell inside the lvl2 primitive. */
  name: S;

  /** Name of the lvl2 primitive itself. */
  primitive: S;
}

export type CONTROL_ROOM_LVL2<X = U> = (
  ctx: CONTROL_ROOM_LVL2_CTX<X>,
) => CELL_CONTRACT<X>;

export interface CONTROL_ROOM_LVL2_CONTRACT {
  /** Each key becomes one master atom. */
  [name: S]: CONTROL_ROOM_LVL2<A>;
}

type CONTROL_ROOM_LVL2_RESULT<C extends CONTROL_ROOM_LVL2_CONTRACT> = {
  [K in keyof C]: ATOM<A>;
};

function build_lvl2_cell<X>(
  primitive: S,
  name: S,
  factory: CONTROL_ROOM_LVL2<X>,
): ATOM<X> {
  let built: ATOM<X> | NU = null;

  const self = make_self<X>(() => built);

  const ctx: CONTROL_ROOM_LVL2_CTX<X> = {
    self,
    name,
    primitive,
    key: name,
  };

  // ── PASS 1: read the key. A key that already exists returns the existing cell. ──
  const pass1 = factory(ctx);

  const key: ATOM_KEY = pass1.key ?? name;

  const existing = BC().ATOM_AT<ATOM<X>>("root_cell", key);

  if (existing) return existing;

  // ── Build the bare cell and place it at its address. ──
  const initial =
    typeof pass1.value === "function"
      ? (pass1.value as () => X)()
      : (pass1.value as X);

  const cell = new ATOM<X>(key, initial);

  BC().ATOM_PUT("root_cell", key, cell);

  built = cell;
  ctx.key = key;

  // ── PASS 2: rerun so self-dependent fields resolve against the real cell,
  // then build and bind the shards. The value is not re-read. ──
  const contract = factory(ctx);

  const atoms: { atom: ATOM; owner: OWNERSHIP }[] = [];

  for (const shard_name of Object.keys(contract)) {
    if (RESERVED.has(shard_name)) continue;

    const v = contract[shard_name];

    if (v === undefined || v === null || v === false) continue;

    if (typeof v === "function") {
      atoms.push({
        atom: (v as (root_cell_key: ATOM_KEY) => ATOM)(key),
        owner: "bespoke",
      });

      continue;
    }

    if (typeof v === "object" && "root_cell_key" in (v as object)) {
      atoms.push({
        atom: v as ATOM,
        owner: "shared",
      });

      continue;
    }

    if (shard_name in PARTICLE_KEYS) {
      atoms.push({
        atom: new PARTICLE_KEYS[shard_name](key, v === true ? {} : v),
        owner: "bespoke",
      });

      continue;
    }

    throw new Error(
      `[soul-forge] "${primitive}.${name}": unknown contract key "${shard_name}". ` +
      `Shards: ${Object.keys(PARTICLE_KEYS).join(", ")}. ` +
      `For a custom shard, use a function: ${shard_name}: (key) => new YOUR_SHARD(key)`,
    );
  }

  cell.registry =
    atoms.length > 0
      ? new REGISTRY_PROTON(key, { atoms })
      : null;

  return cell;
}

export function CONTROL_ROOM_LVL2<C extends CONTROL_ROOM_LVL2_CONTRACT>(
  arg1: S | C,
  arg2?: C,
): CONTROL_ROOM_LVL2_RESULT<C> {
  const named = typeof arg1 === "string";

  const primitive = named ? arg1 : "control_room_lvl2";

  const contract = named ? arg2 : arg1;

  if (!contract || typeof contract !== "object") {
    throw new Error("[soul-forge] CONTROL_ROOM_LVL2 needs a contract object");
  }

  const result: A = {};

  for (const name of Object.keys(contract)) {
    const factory = contract[name];

    if (typeof factory !== "function") {
      throw new Error(
        `[soul-forge] "${primitive}.${name}" needs a factory`,
      );
    }

    result[name] = build_lvl2_cell<A>(primitive, name, factory);
  }

  return result as CONTROL_ROOM_LVL2_RESULT<C>;
}

// #endregion
// #endregion
//                                                                                                [TYPES]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

// #endregion
