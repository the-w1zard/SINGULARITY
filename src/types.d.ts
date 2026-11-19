/// <reference types="@the-w1zard/dx" />

import {    ATOM, MODULE, CLOUD, SAURON, DRS, MSG_Q, SLAB, S_CLASS, SPREADSHEET, VERTEX,
    AW_SET, CASUAL_CURSOR, DELTA_STORE, G_SET, VERSION_VECTOR, CIRCUIT_BREAKER, CQRS, RAFT, YJS, DIFFPATCH, HYBRID_LOGICAL_CLOCK, LWW, LWW_ELEMENTAL, MV, OPTIMISTIC, OR, PN, REVERSE_HEARTBEAT_MGR, SEMAPHORE, TOMBSTONE_COMPACTOR, YIMIR, HYBRID_LOGICAL_CLOCK_CRDT, ADAPTER, BEAM_CODEC, BINARY, BYTE_CODE_VM, CHANNEL, CHANNELS, CONCURRENCY, DEADLETTER, DESTRUCTION, ERROR, EVENT, EXTERNAL_SUBSCRIPTION, GATE, GLOBAL_SUBSCRIPTION, GOVERNOR, HOOK, HTTP_TRANSPORT, INJECTION_CODEC, METRICS, MIDDLEWARE, RIFTS_ACCESS, REACTIVE, ROUTE, RUN_ON, SEALED_ERROR, SESSION, SHELL, SIGNAL, SOA_QUAD_TIERED_REGISTRY, SUBSCRIPTION, TRANSFORM, WAITER, CONTRACTS, SCHEMA, VALUES_REGISTRY, COMPRESSION, CONFLICT, CONTEXT, ENCRYPTION, GROUPS, HISTORY, META, OFFSEC, PERMISSIONS, PERSISTENCE, PRIORITY, SNAPSHOT, TAGS, TIME_TRAVEL, TTL,  } from './signals'

export {};
declare global {
	//                                                                                                                  [CONTRACTS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	interface CONTRACT_MASTER<T = U> {
		r: S;
		m: S;
		s: S;
		k: A;
		value: T;
		initial?: T;
		frozen?: boolean
		peek: <T = A>() => T | UD
		read: <T = A>() => T | UD
		write: <T = A>(value: T | ((previous: T) => T)) => T 
		triggerRender?: boolean
		subscribe?: (() => V)
		async?: A
		lifecycle?:A
	} 
	interface ATOM<T = U> extends CONTRACT_MASTER<T> {}
	/** The minimal contract between the builder and a master-atom constructor. */
	type ATOM_CONTRACT_INSTANCE<T = U, M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>, B extends object = Record<never, never>> = B & {
		readonly atom: MASTER_ATOM_KEY;
		readonly r?: S;
		readonly m?: S;
		readonly s?: S;
		readonly k?: A;
		readonly rr?: S;
		readonly mm?: S;
		readonly ss?: S;
		readonly kk?: A;
		readonly master: M;
		readonly bindables: readonly B[]; /** The bindables created for this instance; no master-side registry is added. */
		value: T;
	};
	type ATOM_CONTRACT<T = U, M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>, O extends object = Record<S, never>> = ( (
        value?: T,
        k?: ATOM_KEY,
        s?: S,
        m?: S,
        r?: S,
		...settings: U[]
    ) => ATOM_CONTRACT_INSTANCE<T, M, O>
) & {
    readonly atom: MASTER_ATOM_KEY;
    readonly config: Readonly<ATOM_CONTRACT_CONFIG<T, M, O>>;
};
type ATOM_BINDABLE<M extends CONTRACT_MASTER = CONTRACT_MASTER, B extends object = Record<never, never>> =
(ctx: { self: M & B; k: A; s: S; m: S; r: S }) => B;
interface ATOM_CONTRACT_CONFIG<T = U, M extends CONTRACT_MASTER = CONTRACT_MASTER, B extends object = Record<never, never>> {
	atom?: MASTER_ATOM_KEY; /** One of the twelve real master atoms. Defaults to `ATOM` for contracts. */
	value?: T | (() => T); /** Initial value, or a factory evaluated once for each contract instance. */
	r?: S;
	m?: S;
	s?: S;
	k?: A; /** Optional stable address; otherwise a fresh symbol is used per instance. */
	bindables?: Record<string, ATOM_BINDABLE<M, B>>;
	[setting: S]: U; /** Additional reusable particle settings, such as `ttl`, `history`, or `snapshot`. */
} 
	// #endregion
	//                                                                                                                       [CORE]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	interface ReactiveNode {
		deps?: Link;
		depsTail?: Link;
		subs?: Link;
		subsTail?: Link;
		flags: ReactiveFlags;
	}
	interface Link {
		version: number;
		dep: ReactiveNode;
		sub: ReactiveNode;
		prevSub: Link | undefined;
		nextSub: Link | undefined;
		prevDep: Link | undefined;
		nextDep: Link | undefined;
	}
	interface Stack<T> {
		value: T;
		prev: Stack<T> | undefined;
	}
	interface EffectScopeNode extends ReactiveNode {}
	interface EffectNode extends ReactiveNode {
		fn(): (() => void) | void;
		cleanup: (() => void) | void;
	}
	interface ComputedNode<T = any> extends ReactiveNode {
		value: T | undefined;
		getter: (previousValue?: T) => T;
	}
	interface SignalNode<T = any> extends ReactiveNode {
		currentValue: T;
		pendingValue: T;
	}
	// #endregion
 	//                                                                                                                       [ATOM]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
    interface ATOM_KIND_REGISTRY {
		// GOVERNOR: typeof GOVERNOR;
		 ATOM: SIGNALS["maul"]["ATOMS"]["ATOM"] ;
		 MODULE: SIGNALS["maul"]["ATOMS"]["MODULE"];
		 CLOUD: SIGNALS["maul"]["ATOMS"]["CLOUD"];
		 SAURON: SIGNALS["maul"]["ATOMS"]["SAURON"];
		 DRS: SIGNALS["maul"]["ATOMS"]["DRS"];
		 MSG_Q: SIGNALS["maul"]["ATOMS"]["MSG_Q"];
		 SLAB: SIGNALS["maul"]["ATOMS"]["SLAB"];
		 S_CLASS: SIGNALS["maul"]["ATOMS"]["S_CLASS"];
		 SPREADSHEET: SIGNALS["maul"]["ATOMS"]["SPREADSHEET"];
		 VERTEX: SIGNALS["maul"]["ATOMS"]["VERTEX"];
		 AW_SET: SIGNALS["maul"]["ATOMS"]["AW_SET"];
		 CASUAL_CURSOR: SIGNALS["maul"]["ATOMS"]["CASUAL_CURSOR"];
		 DELTA_STORE: SIGNALS["maul"]["ATOMS"]["DELTA_STORE"];
		 G_SET: SIGNALS["maul"]["ATOMS"]["G_SET"];
		 VERSION_VECTOR: SIGNALS["maul"]["ATOMS"]["VERSION_VECTOR"];
		 CIRCUIT_BREAKER: SIGNALS["maul"]["ATOMS"]["CIRCUIT_BREAKER"];
		 CQRS: SIGNALS["maul"]["ATOMS"]["CQRS"];
		 RAFT: SIGNALS["maul"]["ATOMS"]["RAFT"];
		 YJS: SIGNALS["maul"]["ATOMS"]["YJS"];
		 DIFFPATCH: SIGNALS["maul"]["ATOMS"]["DIFFPATCH"];
		 HYBRID_LOGICAL_CLOCK: SIGNALS["maul"]["ATOMS"]["HYBRID_LOGICAL_CLOCK"];
		 LWW: SIGNALS["maul"]["ATOMS"]["LWW"];
		 LWW_ELEMENTAL: SIGNALS["maul"]["ATOMS"]["LWW_ELEMENTAL"];
		 MV: SIGNALS["maul"]["ATOMS"]["MV"];
		 OPTIMISTIC: SIGNALS["maul"]["ATOMS"]["OPTIMISTIC"];
		 OR: SIGNALS["maul"]["ATOMS"]["OR"];
		 PN: SIGNALS["maul"]["ATOMS"]["PN"];
		 REVERSE_HEARTBEAT_MGR: SIGNALS["maul"]["ATOMS"]["REVERSE_HEARTBEAT_MGR"];
		 SEMAPHORE: SIGNALS["maul"]["ATOMS"]["SEMAPHORE"];
		 TOMBSTONE_COMPACTOR: SIGNALS["maul"]["ATOMS"]["TOMBSTONE_COMPACTOR"];
		 YIMIR: SIGNALS["maul"]["ATOMS"]["YIMIR"];
		 HYBRID_LOGICAL_CLOCK_CRDT: SIGNALS["maul"]["ATOMS"]["HYBRID_LOGICAL_CLOCK_CRDT"];
		 SOA_QUAD_TIERED_REGISTRY: SIGNALS["maul"]["ATOMS"]["SOA_QUAD_TIERED_REGISTRY"];
		 BINARY: SIGNALS["maul"]["ATOMS"]["BINARY"];
		 SIGNAL: SIGNALS["maul"]["ATOMS"]["SIGNAL"];
		 ADAPTER: SIGNALS["maul"]["ATOMS"]["ADAPTER"];
		 BEAM_CODEC: SIGNALS["maul"]["ATOMS"]["BEAM_CODEC"];
		 BYTE_CODE_VM: SIGNALS["maul"]["ATOMS"]["BYTE_CODE_VM"];
		 CHANNEL: SIGNALS["maul"]["ATOMS"]["CHANNEL"];
		 CHANNELS: SIGNALS["maul"]["ATOMS"]["CHANNELS"];
		 CONCURRENCY: SIGNALS["maul"]["ATOMS"]["CONCURRENCY"];
		 DEADLETTER: SIGNALS["maul"]["ATOMS"]["DEADLETTER"];
		 DESTRUCTION: SIGNALS["maul"]["ATOMS"]["DESTRUCTION"];
		 ERROR: SIGNALS["maul"]["ATOMS"]["ERROR"];
		 EVENT: SIGNALS["maul"]["ATOMS"]["EVENT"];
		 EXTERNAL_SUBSCRIPTION: SIGNALS["maul"]["ATOMS"]["EXTERNAL_SUBSCRIPTION"];
		 GATE: SIGNALS["maul"]["ATOMS"]["GATE"];
		 GLOBAL_SUBSCRIPTION: SIGNALS["maul"]["ATOMS"]["GLOBAL_SUBSCRIPTION"];
		 HOOK: SIGNALS["maul"]["ATOMS"]["HOOK"];
		 HTTP_TRANSPORT: SIGNALS["maul"]["ATOMS"]["HTTP_TRANSPORT"];
		 INJECTION_CODEC: SIGNALS["maul"]["ATOMS"]["INJECTION_CODEC"];
		 METRICS: SIGNALS["maul"]["ATOMS"]["METRICS"];
		 MIDDLEWARE: SIGNALS["maul"]["ATOMS"]["MIDDLEWARE"];
		 RIFTS_ACCESS: SIGNALS["maul"]["ATOMS"]["RIFTS_ACCESS"];
		 REACTIVE: SIGNALS["maul"]["ATOMS"]["REACTIVE"];
		 ROUTE: SIGNALS["maul"]["ATOMS"]["ROUTE"];
		 RUN_ON: SIGNALS["maul"]["ATOMS"]["RUN_ON"];
		 SEALED_ERROR: SIGNALS["maul"]["ATOMS"]["SEALED_ERROR"];
		 SESSION: SIGNALS["maul"]["ATOMS"]["SESSION"];
		 SHELL: SIGNALS["maul"]["ATOMS"]["SHELL"];
		 SUBSCRIPTION: SIGNALS["maul"]["ATOMS"]["SUBSCRIPTION"];
		 TRANSFORM: SIGNALS["maul"]["ATOMS"]["TRANSFORM"];
		 WAITER: SIGNALS["maul"]["ATOMS"]["WAITER"];
		 CONTRACTS: SIGNALS["maul"]["ATOMS"]["CONTRACTS"];
		 SCHEMA: SIGNALS["maul"]["ATOMS"]["SCHEMA"];
		 VALUES_REGISTRY: SIGNALS["maul"]["ATOMS"]["VALUES_REGISTRY"];
		 COMPRESSION: SIGNALS["maul"]["ATOMS"]["COMPRESSION"];
		 CONFLICT: SIGNALS["maul"]["ATOMS"]["CONFLICT"];
		 CONTEXT: SIGNALS["maul"]["ATOMS"]["CONTEXT"];
		 ENCRYPTION: SIGNALS["maul"]["ATOMS"]["ENCRYPTION"];
		 GROUPS: SIGNALS["maul"]["ATOMS"]["GROUPS"];
		 HISTORY: SIGNALS["maul"]["ATOMS"]["HISTORY"];
		 META: SIGNALS["maul"]["ATOMS"]["META"];
		 OFFSEC: SIGNALS["maul"]["ATOMS"]["OFFSEC"];
		 PERMISSIONS: SIGNALS["maul"]["ATOMS"]["PERMISSIONS"];
		 PERSISTENCE: SIGNALS["maul"]["ATOMS"]["PERSISTENCE"];
		 PRIORITY: SIGNALS["maul"]["ATOMS"]["PRIORITY"];
		 SNAPSHOT: SIGNALS["maul"]["ATOMS"]["SNAPSHOT"];
		 TAGS: SIGNALS["maul"]["ATOMS"]["TAGS"];
		 TIME_TRAVEL: SIGNALS["maul"]["ATOMS"]["TIME_TRAVEL"];
		 TTL: SIGNALS["maul"]["ATOMS"]["TTL"];
	 }

	 type AtomUnwrapFactory<C> = C extends (...args: any[]) => infer Output ? Output : C;
	 // What ATOM_CONTRACT's definition factory declares: { atom: "VERTEX", value?, bindables? }
	 type AtomConfigKind2<Node> = Node extends { atom?: infer K } ? K : "ATOM";
	 type AtomConfigBindables2<Node> = Node extends { bindables?: infer B } ? B : {};
	 type AtomConfigValue2<Node> = Node extends { value?: infer V | (() => infer V) } ? V : unknown;
	 // What a registered kind factory (MASTER_ATOM's own factory) declares, by convention —
	 // every field on its returned object besides `bindables`/`k`/`s`/`m`/`r`/`value` is a capability fn.
	 type AtomCapabilities<KindNode> = Omit<KindNode, "bindables" | "k" | "s" | "m" | "r" | "value">;
	 // What a single bindable factory declares, same convention.
	 type AtomBindableShape2<BindNode> = BindNode extends (...args: any[]) => infer Out ? Out : BindNode;
	 type AtomResult2<KindNode, Bindables extends Record<string, any>> = AtomCapabilities<KindNode> & { [K in keyof Bindables]: AtomBindableShape<ReturnType<Extract<Bindables[K], (...args: any[]) => any>>> } & { atom: string; value: AtomConfigValue<KindNode> };
	 //export type AtomMirror2<C, KindFactoryMap extends Record<string, any>> = AtomUnwrapFactory<C> extends infer Config ? AtomConfigKind<Config> extends infer K ? K extends keyof KindFactoryMap ? (config?: Record<string, any>) => AtomResult<ReturnType<KindFactoryMap[K]>, AtomConfigBindables<Config>> : (config?: Record<string, any>) => Record<string, any> : never : never;// kind not found in the map — fall back, don't lie
	 type AtomObject = Record<string, any>;
	 type AtomConfigFactory<C extends AtomObject = AtomObject> = (ctx: any) => C;
	 interface ATOM_KIND_REGISTRY {} 
	 
	 
	 
	 
	 type AtomConfigKind<Config> = Config extends { atom?: infer K } ? K : "ATOM";
	 type AtomConfigBindables<Config> = Config extends { bindables?: infer B } ? B : {};
	 type AtomConfigValue<Config> = Config extends { value?: infer V }
		? (V extends (...args: any[]) => infer R ? R : V)
		: unknown;
	 type AtomBindableShape<BindNode> = BindNode extends (...args: any[]) => infer Out ? Out : BindNode;
	 type AtomBindablesResult<Bindables> = { [K in keyof Bindables]: AtomBindableShape<Bindables[K]> };
	 
	 type AtomKindCapabilities<K> = K extends keyof ATOM_KIND_REGISTRY
		? Omit<AtomUnwrapFactory<ATOM_KIND_REGISTRY[K]>, "bindables">
		: AtomObject;
	 
	 type AtomResult<Config> =
		AtomKindCapabilities<AtomConfigKind<Config>> &
		AtomBindablesResult<AtomConfigBindables<Config>> &
		{ atom: AtomConfigKind<Config>; value: AtomConfigValue<Config> };
	 type AtomMirror<C> = (config?: AtomObject) => AtomResult<AtomUnwrapFactory<C>>;
	 
	// #endregion
	//                                                                                                                       [CORE]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

	// #endregion
	//                                                                                                                       [CORE]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

	// #endregion
	//                                                                                                                       [CORE]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

	// #endregion
	//                                                                                                                       [CORE]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
}
