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
