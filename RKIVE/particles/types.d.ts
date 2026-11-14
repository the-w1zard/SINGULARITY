// import { ATOM } from './master_atoms/ATOM'
// import { MODULE } from './master_atoms/MODULE'
// import { CLOUD } from './master_atoms/CLOUD'
// import { SAURON } from './master_atoms/SAURON'
// import { DRS } from './master_atoms/DRS'
// import { MSG_Q } from './master_atoms/MSG_Q'
// import { SLAB } from './master_atoms/SLAB'
// import { S_CLASS } from './master_atoms/S_CLASS'
// import { SPREADSHEET } from './master_atoms/SPREADSHEET'
// import { VERTEX } from './master_atoms/VERTEX'
import {    ATOM, MODULE, CLOUD, SAURON, DRS, MSG_Q, SLAB, S_CLASS, SPREADSHEET, VERTEX,
    AW_SET, CASUAL_CURSOR, DELTA_STORE, G_SET, VERSION_VECTOR, CIRCUIT_BREAKER, CQRS, RAFT, YJS, DIFFPATCH, HYBRID_LOGICAL_CLOCK, LWW, LWW_ELEMENTAL, MV, OPTIMISTIC, OR, PN, REVERSE_HEARTBEAT_MGR, SEMAPHORE, TOMBSTONE_COMPACTOR, YIMIR, HYBRID_LOGICAL_CLOCK_CRDT, ADAPTER, BEAM_CODEC, BINARY, BYTE_CODE_VM, CHANNEL, CHANNELS, CONCURRENCY, DEADLETTER, DESTRUCTION, ERROR, EVENT, EXTERNAL_SUBSCRIPTION, GATE, GLOBAL_SUBSCRIPTION, GOVERNOR, HOOK, HTTP_TRANSPORT, INJECTION_CODEC, METRICS, MIDDLEWARE, RIFTS_ACCESS, REACTIVE, ROUTE, RUN_ON, SEALED_ERROR, SESSION, SHELL, SIGNAL, SOA_QUAD_TIERED_REGISTRY, SUBSCRIPTION, TRANSFORM, WAITER, CONTRACTS, SCHEMA, VALUES_REGISTRY, COMPRESSION, CONFLICT, CONTEXT, ENCRYPTION, GROUPS, HISTORY, META, OFFSEC, PERMISSIONS, PERSISTENCE, PRIORITY, SNAPSHOT, TAGS, TIME_TRAVEL, TTL,  } from '../signals'
export {} 

declare global {
    interface ATOM_KIND_REGISTRY {
       // GOVERNOR: typeof GOVERNOR;
        ATOM: typeof ATOM;
        MODULE: typeof MODULE;
        CLOUD: typeof CLOUD;
        SAURON: typeof SAURON;
        DRS: typeof DRS;
        MSG_Q: typeof MSG_Q;
        SLAB: typeof SLAB;
        S_CLASS: typeof S_CLASS;
        SPREADSHEET: typeof SPREADSHEET;
        VERTEX: typeof VERTEX;
        AW_SET: typeof AW_SET;
        CASUAL_CURSOR: typeof CASUAL_CURSOR;
        DELTA_STORE: typeof DELTA_STORE;
        G_SET: typeof G_SET;
        VERSION_VECTOR: typeof VERSION_VECTOR;
        CIRCUIT_BREAKER: typeof CIRCUIT_BREAKER;
        CQRS: typeof CQRS;
        RAFT: typeof RAFT;
        YJS: typeof YJS;
        DIFFPATCH: typeof DIFFPATCH;
        HYBRID_LOGICAL_CLOCK: typeof HYBRID_LOGICAL_CLOCK;
        LWW: typeof LWW;
        LWW_ELEMENTAL: typeof LWW_ELEMENTAL;
        MV: typeof MV;
        OPTIMISTIC: typeof OPTIMISTIC;
        OR: typeof OR;
        PN: typeof PN;
        REVERSE_HEARTBEAT_MGR: typeof REVERSE_HEARTBEAT_MGR;
        SEMAPHORE: typeof SEMAPHORE;
        TOMBSTONE_COMPACTOR: typeof TOMBSTONE_COMPACTOR;
        YIMIR: typeof YIMIR;
        HYBRID_LOGICAL_CLOCK_CRDT: typeof HYBRID_LOGICAL_CLOCK_CRDT;
        SOA_QUAD_TIERED_REGISTRY: typeof SOA_QUAD_TIERED_REGISTRY;
        BINARY: typeof BINARY;
        SIGNAL: typeof SIGNAL;
        ADAPTER: typeof ADAPTER;
        BEAM_CODEC: typeof BEAM_CODEC;
        BYTE_CODE_VM: typeof BYTE_CODE_VM;
        CHANNEL: typeof CHANNEL;
        CHANNELS: typeof CHANNELS;
        CONCURRENCY: typeof CONCURRENCY;
        DEADLETTER: typeof DEADLETTER;
        DESTRUCTION: typeof DESTRUCTION;
        ERROR: typeof ERROR;
        EVENT: typeof EVENT;
        EXTERNAL_SUBSCRIPTION: typeof EXTERNAL_SUBSCRIPTION;
        GATE: typeof GATE;
        GLOBAL_SUBSCRIPTION: typeof GLOBAL_SUBSCRIPTION;
        HOOK: typeof HOOK;
        HTTP_TRANSPORT: typeof HTTP_TRANSPORT;
        INJECTION_CODEC: typeof INJECTION_CODEC;
        METRICS: typeof METRICS;
        MIDDLEWARE: typeof MIDDLEWARE;
        RIFTS_ACCESS: typeof RIFTS_ACCESS;
        REACTIVE: typeof REACTIVE;
        ROUTE: typeof ROUTE;
        RUN_ON: typeof RUN_ON;
        SEALED_ERROR: typeof SEALED_ERROR;
        SESSION: typeof SESSION;
        SHELL: typeof SHELL;
        SUBSCRIPTION: typeof SUBSCRIPTION;
        TRANSFORM: typeof TRANSFORM;
        WAITER: typeof WAITER;
        CONTRACTS: typeof CONTRACTS;
        SCHEMA: typeof SCHEMA;
        VALUES_REGISTRY: typeof VALUES_REGISTRY;
        COMPRESSION: typeof COMPRESSION;
        CONFLICT: typeof CONFLICT;
        CONTEXT: typeof CONTEXT;
        ENCRYPTION: typeof ENCRYPTION;
        GROUPS: typeof GROUPS;
        HISTORY: typeof HISTORY;
        META: typeof META;
        OFFSEC: typeof OFFSEC;
        PERMISSIONS: typeof PERMISSIONS;
        PERSISTENCE: typeof PERSISTENCE;
        PRIORITY: typeof PRIORITY;
        SNAPSHOT: typeof SNAPSHOT;
        TAGS: typeof TAGS;
        TIME_TRAVEL: typeof TIME_TRAVEL;
        TTL: typeof TTL;
    }
}
