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
	} /** The minimal contract between the builder and a master-atom constructor. */
	type ATOM_BINDABLE<M extends CONTRACT_MASTER = CONTRACT_MASTER, B extends object = Record<never, never>> = (context: ATOM_BINDABLE_CONTEXT<M>) => B; /** Creates one bound atom using only the address of its master. */
	interface ATOM_BINDABLE_CONTEXT<M extends CONTRACT_MASTER = CONTRACT_MASTER> {
		readonly self: M;
		r?: S;
		m?: S;
		s?: S;
		k?: A;
		rr?: S;
		mm?: S;
		ss?: S;
		kk?: A;
	}
	interface ATOM_CONTRACT_SELF {
		readonly atom: MASTER_ATOM_KEY;
	} /** The master kind selected by this contract, defaulting to `VERTEX`. */
	interface ATOM_CONTRACT_CONFIG<T = U, M extends CONTRACT_MASTER = CONTRACT_MASTER, B extends object = Record<never, never>> {
		atom?: MASTER_ATOM_KEY; /** One of the twelve real master atoms. Defaults to `ATOM` for contracts. */
		value?: T | (() => T); /** Initial value, or a factory evaluated once for each contract instance. */
		r: S;
		m: S;
		s: S;
		k: A; /** Optional stable address; otherwise a fresh symbol is used per instance. */
		bindables?: readonly ATOM_BINDABLE<M, B>[]; /** Bindable atom factories; each receives only the master's address and instance. */
		[setting: S]: U; /** Additional reusable particle settings, such as `ttl`, `history`, or `snapshot`. */
	} /** Backend seam: construction is delegated without adding storage to the contract. */
	interface ATOM_CONTRACT_RUNTIME<M extends CONTRACT_MASTER = CONTRACT_MASTER> {
		createMaster<T>(kind: MASTER_ATOM_KEY, r: S, m: S, s: S, k: A, value: T): M;
	}
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
	type ATOM_CONTRACT<T = U, M extends CONTRACT_MASTER<T> = CONTRACT_MASTER<T>, O extends object = Record<S, never>> = ((value?: T, key?: ATOM_KEY) => ATOM_CONTRACT_INSTANCE<T, M, O>) & { readonly atom: MASTER_ATOM_KEY; readonly config: Readonly<ATOM_CONTRACT_CONFIG<T, M, O>> };
	type MASTER_ATOM_KEYS =
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
		| "VERTEX"; // THE DEFAULT ATOM, WHICH IS ALSO THE HOT AS FUCK PATH
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

	// #endregion
	//                                                                                                                       [CORE]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
}
