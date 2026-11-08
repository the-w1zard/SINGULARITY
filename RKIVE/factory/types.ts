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
export const SUBSCRIPTIONS = DEFINE_ATOM_KIND("subscriptions");
export const GATE = DEFINE_ATOM_KIND("gate");


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


export type MASTER_ATOM_KEY = (typeof MASTER_ATOM_KEYS)[number];


export interface NODE {
  readonly key: ATOM_KEY;
  destroy(): void;
}
export type ATOM_TYPE = A;
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
export interface CELL_CONTRACT<X = A> {
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
