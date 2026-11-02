//                                                                                                                         IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import Store from "@the-w1zard/store"
import { BYTE_ID } from "./comps/byte_id"
import { TIERED_QUEUE } from "./comps/queue"
import { useReducer, useEffect} from '#sauron'
import { getCurrentComponent,notifyHook, 	subscribeToHook } from '#the-cloud'

const __sid = new BYTE_ID()
export function _sidi(): S {return __sid.b()}
// #endregion 
//                                                                                                                      REACTIVITY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface ReactiveNode {
	deps?: Link;
	depsTail?: Link;
	subs?: Link;
	subsTail?: Link;
	flags: ReactiveFlags;
}
export interface Link {
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
interface EffectScopeNode extends ReactiveNode {
}
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
const enum ReactiveFlags {
	None = 0,
	Mutable = 1,
	Watching = 2,
	RecursedCheck = 4,
	Recursed = 8,
	Dirty = 16,
	Pending = 32,
}
const BRAND_SIGNAL = Symbol('warlock.signal');
const BRAND_COMPUTED = Symbol('warlock.computed');
const BRAND_EFFECT = Symbol('warlock.effect');
const BRAND_EFFECT_SCOPE = Symbol('warlock.effectScope');
export function createReactiveSystem({	update,	notify,	unwatched,}: {	update(sub: ReactiveNode): boolean;	notify(sub: ReactiveNode): void;	unwatched(sub: ReactiveNode): void;}) {
	return {		link,		unlink,		propagate,		checkDirty,		shallowPropagate,	};
	function link(dep: ReactiveNode, sub: ReactiveNode, version: number): void {
		const prevDep = sub.depsTail;
		if (prevDep !== undefined && prevDep.dep === dep) {			return;		}
		const nextDep = prevDep !== undefined ? prevDep.nextDep : sub.deps;
		if (nextDep !== undefined && nextDep.dep === dep) {
			nextDep.version = version;
			sub.depsTail = nextDep;
			return;
		}
		const prevSub = dep.subsTail;
		if (prevSub !== undefined && prevSub.version === version && prevSub.sub === sub) {		return;		}
		const newLink
			= sub.depsTail
			= dep.subsTail
			= {
				version,
				dep,
				sub,
				prevDep,
				nextDep,
				prevSub,
				nextSub: undefined,
			};
		if (nextDep !== undefined) {
			nextDep.prevDep = newLink;
		}
		if (prevDep !== undefined) {
			prevDep.nextDep = newLink;
		} else {
			sub.deps = newLink;
		}
		if (prevSub !== undefined) {
			prevSub.nextSub = newLink;
		} else {
			dep.subs = newLink;
		}
	}
	function unlink(link: Link, sub = link.sub): Link | undefined {
		const { dep, prevDep, nextDep, nextSub, prevSub } = link;
		if (nextDep !== undefined) {
			nextDep.prevDep = prevDep;
		} else {
			sub.depsTail = prevDep;
		}
		if (prevDep !== undefined) {
			prevDep.nextDep = nextDep;
		} else {
			sub.deps = nextDep;
		}
		if (nextSub !== undefined) {
			nextSub.prevSub = prevSub;
		} else {
			dep.subsTail = prevSub;
		}
		if (prevSub !== undefined) {
			prevSub.nextSub = nextSub;
		} else if ((dep.subs = nextSub) === undefined) {
			unwatched(dep);
		}
		return nextDep;
	}
	function propagate(link: Link, innerWrite: boolean): void {
		let next = link.nextSub;
		let stack: Stack<Link | undefined> | undefined;

		top: do {
			const sub = link.sub;
			let flags = sub.flags;

			if (!(flags & (ReactiveFlags.RecursedCheck | ReactiveFlags.Recursed | ReactiveFlags.Dirty | ReactiveFlags.Pending))) {
				sub.flags = flags | ReactiveFlags.Pending;
				if (innerWrite) {
					sub.flags |= ReactiveFlags.Recursed;
				}
			} else if (!(flags & (ReactiveFlags.RecursedCheck | ReactiveFlags.Recursed))) {
				flags = ReactiveFlags.None;
			} else if (!(flags & ReactiveFlags.RecursedCheck)) {
				sub.flags = (flags & ~ReactiveFlags.Recursed) | ReactiveFlags.Pending;
			} else if (!(flags & (ReactiveFlags.Dirty | ReactiveFlags.Pending)) && isValidLink(link, sub)) {
				sub.flags = flags | (ReactiveFlags.Recursed | ReactiveFlags.Pending);
				flags &= ReactiveFlags.Mutable;
			} else {
				flags = ReactiveFlags.None;
			}

			if (flags & ReactiveFlags.Watching) {
				notify(sub);
			}

			if (flags & ReactiveFlags.Mutable) {
				const subSubs = sub.subs;
				if (subSubs !== undefined) {
					const nextSub = (link = subSubs).nextSub;
					if (nextSub !== undefined) {
						stack = { value: next, prev: stack };
						next = nextSub;
					}
					continue;
				}
			}

			if ((link = next!) !== undefined) {
				next = link.nextSub;
				continue;
			}

			while (stack !== undefined) {
				link = stack.value!;
				stack = stack.prev;
				if (link !== undefined) {
					next = link.nextSub;
					continue top;
				}
			}

			break;
		} while (true);
	}
	function checkDirty(link: Link, sub: ReactiveNode): boolean {
		let stack: Stack<Link> | undefined;
		let checkDepth = 0;
		let dirty = false;

		top: do {
			const dep = link.dep;
			const flags = dep.flags;

			if (sub.flags & ReactiveFlags.Dirty) {
				dirty = true;
			} else if ((flags & (ReactiveFlags.Mutable | ReactiveFlags.Dirty)) === (ReactiveFlags.Mutable | ReactiveFlags.Dirty)) {
				const subs = dep.subs!;
				if (update(dep)) {
					if (subs.nextSub !== undefined) {
						shallowPropagate(subs);
					}
					dirty = true;
				}
			} else if ((flags & (ReactiveFlags.Mutable | ReactiveFlags.Pending)) === (ReactiveFlags.Mutable | ReactiveFlags.Pending)) {
				stack = { value: link, prev: stack };
				link = dep.deps!;
				sub = dep;
				++checkDepth;
				continue;
			}

			if (!dirty) {
				const nextDep = link.nextDep;
				if (nextDep !== undefined) {
					link = nextDep;
					continue;
				}
			}

			while (checkDepth--) {
				link = stack!.value;
				stack = stack!.prev;
				if (dirty) {
					const subs = sub.subs!;
					if (update(sub)) {
						if (subs.nextSub !== undefined) {
							shallowPropagate(subs);
						}
						sub = link.sub;
						continue;
					}
					dirty = false;
				} else {
					sub.flags &= ~ReactiveFlags.Pending;
				}
				sub = link.sub;
				const nextDep = link.nextDep;
				if (nextDep !== undefined) {
					link = nextDep;
					continue top;
				}
			}

			return dirty && !!sub.flags;
		} while (true);
	}
	function shallowPropagate(link: Link): void {
		do {
			const sub = link.sub;
			const flags = sub.flags;
			if ((flags & (ReactiveFlags.Pending | ReactiveFlags.Dirty)) === ReactiveFlags.Pending) {
				sub.flags = flags | ReactiveFlags.Dirty;
				if ((flags & (ReactiveFlags.Watching | ReactiveFlags.RecursedCheck)) === ReactiveFlags.Watching) {
					notify(sub);
				}
			}
		} while ((link = link.nextSub!) !== undefined);
	}
	function isValidLink(checkLink: Link, sub: ReactiveNode): boolean {
		let link = sub.depsTail;
		while (link !== undefined) {
			if (link === checkLink) {
				return true;
			}
			link = link.prevDep;
		}
		return false;
	}
}
const HasChildEffect = 64;
let writeJournal: Array<() => void> | undefined;
let cycle = 0;
let runDepth = 0;
let batchDepth = 0;
let notifyIndex = 0;
let queuedLength = 0;
let activeSub: ReactiveNode | undefined;
const queued: (EffectNode | undefined)[] = [];
const {
	link,
	unlink,
	propagate,
	checkDirty,
	shallowPropagate,
} = createReactiveSystem({
	update(node: SignalNode | ComputedNode | EffectScopeNode): boolean {
		if ('getter' in node) {
			return updateComputed(node);
		}
		if ('currentValue' in node) {
			return updateSignal(node);
		}
		node.flags = ReactiveFlags.Mutable;
		return true;
	},
	notify(effect: EffectNode) {
		let insertIndex = queuedLength;
		let firstInsertedIndex = insertIndex;

		do {
			queued[insertIndex++] = effect;
			effect.flags &= ~ReactiveFlags.Watching;
			effect = effect.subs?.sub as EffectNode;
			if (effect === undefined || !(effect.flags & ReactiveFlags.Watching)) {
				break;
			}
		} while (true);

		queuedLength = insertIndex;

		while (firstInsertedIndex < --insertIndex) {
			const left = queued[firstInsertedIndex];
			queued[firstInsertedIndex++] = queued[insertIndex];
			queued[insertIndex] = left;
		}
	},
	unwatched(node: SignalNode | ComputedNode | EffectNode | EffectScopeNode) {
		if ('getter' in node) {
			if (node.depsTail !== undefined) {
				node.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
				disposeAllDepsInReverse(node);
			}
		}
		else if ('currentValue' in node) {
			// Nothing to do for signals, they are always mutable and never dirty until pendingValue changes
		}
		else if ('fn' in node) {
			effectOper.call(node);
		}
		else {
			effectScopeOper.call(node);
		}
	},
});
export function getActiveSub(): ReactiveNode | undefined {
	return activeSub;
}
export function setActiveSub(sub?: ReactiveNode) {
	const prevSub = activeSub;
	activeSub = sub;
	return prevSub;
}
export function getBatchDepth(): number {
	return batchDepth;
}
export function startBatch() {
	++batchDepth;
}
export function endBatch() {
	if (!--batchDepth) {
		flush();
	}
}
export function signal<T>(): {
	(): T | undefined;
	(value: T | undefined): void;
};
export function signal<T>(initialValue: T): {
	(): T;
	(value: T): void;
};
export function signal<T>(initialValue?: T): {
	(): T | undefined;
	(value: T | undefined): void;
} {
	const signalFn=  signalOper.bind({
		currentValue: initialValue,
		pendingValue: initialValue,
		subs: undefined,
		subsTail: undefined,
		flags: ReactiveFlags.Mutable,
	}) as () => T | undefined;

    Object.defineProperty(signalFn, BRAND_SIGNAL, { value: true })
    return signalFn

}
export function computed<T>(getter: (previousValue?: T) => T): () => T {
	const v = computedOper.bind({
		value: undefined,
		subs: undefined,
		subsTail: undefined,
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.None,
		getter: getter as (previousValue?: unknown) => unknown,
	}) as () => T;
    Object.defineProperty(v, BRAND_COMPUTED, { value: true });
    return v
}
export function effect(fn: () => void | (() => void)): () => void {
	const e: EffectNode = {
		fn,
		cleanup: undefined,
		subs: undefined,
		subsTail: undefined,
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.Watching | ReactiveFlags.RecursedCheck,
	};
	const prevSub = setActiveSub(e);
	if (prevSub !== undefined) {
		link(e, prevSub, 0);
		prevSub.flags |= HasChildEffect;
	}
	try {
		++runDepth;
		e.cleanup = e.fn();
	} finally {
		--runDepth;
		activeSub = prevSub;
		e.flags &= ~ReactiveFlags.RecursedCheck;

	}

    const effectFn = effectOper.bind(e)// as Effect;

    Object.defineProperty(effectFn, BRAND_EFFECT, { value: true });


	return effectOper.bind(e);
}
export function effectScope(fn: () => void): () => void {
	const e: EffectScopeNode = {
		deps: undefined,
		depsTail: undefined,
		subs: undefined,
		subsTail: undefined,
		flags: ReactiveFlags.Mutable,
	};
	const prevSub = setActiveSub(e);
	if (prevSub !== undefined) {
		link(e, prevSub, 0);
		prevSub.flags |= HasChildEffect;
	}
	try {
		fn();
	} finally {
		activeSub = prevSub;
	}
    const scopeFn = effectScopeOper.bind(e) //as EffectScope;
    Object.defineProperty(scopeFn, BRAND_EFFECT_SCOPE, { value: true });
	return effectScopeOper.bind(e);
}
export function trigger(fn: () => void) {
	const sub: ReactiveNode = {
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.Watching | ReactiveFlags.RecursedCheck,
	};
	const prevSub = setActiveSub(sub);
	++batchDepth;
	try {
		fn();
	} finally {
		activeSub = prevSub;
		sub.flags = ReactiveFlags.None;
		let link = sub.deps;
		while (link !== undefined) {
			const dep = link.dep;
			link = unlink(link, sub);
			const subs = dep.subs;
			if (subs !== undefined) {
				propagate(subs, !!runDepth);
				shallowPropagate(subs);
			}
		}
		if (!--batchDepth) {
			flush();
		}
	}
}
function updateComputed(c: ComputedNode): boolean {
	if (c.flags & HasChildEffect) {
		let link = c.depsTail;
		while (link !== undefined) {
			const prev = link.prevDep;
			const dep = link.dep;
			if (!('getter' in dep) && !('currentValue' in dep)) {
				unlink(link, c);
			}
			link = prev;
		}
	}
	c.depsTail = undefined;
	c.flags = ReactiveFlags.Mutable | ReactiveFlags.RecursedCheck;
	const prevSub = setActiveSub(c);
	try {
		++cycle;
		const oldValue = c.value;
const nextValue = c.getter(oldValue);
c.value = nextValue;
return !Object.is(oldValue, nextValue);
	} finally {
		activeSub = prevSub;
		c.flags &= ~ReactiveFlags.RecursedCheck;
		purgeDeps(c);
	}
}
function updateSignal(s: SignalNode): boolean {
	s.flags = ReactiveFlags.Mutable;

const previous = s.currentValue;
const next = s.pendingValue;
s.currentValue = next;
return !Object.is(previous, next);
}
function run(e: EffectNode): void {
	const flags = e.flags;
	if (
		flags & ReactiveFlags.Dirty
		|| (
			flags & ReactiveFlags.Pending
			&& checkDirty(e.deps!, e)
		)
	) {
		if (flags & HasChildEffect) {
			let link = e.depsTail;
			while (link !== undefined) {
				const prev = link.prevDep;
				const dep = link.dep;
				if (!('getter' in dep) && !('currentValue' in dep)) {
					unlink(link, e);
				}
				link = prev;
			}
		}
		if (e.cleanup) {
			runCleanup(e);
			if (!e.flags) {
				return;
			}
		}
		e.depsTail = undefined;
		e.flags = ReactiveFlags.Watching | ReactiveFlags.RecursedCheck;
		const prevSub = setActiveSub(e);
		try {
			++cycle;
			++runDepth;
			e.cleanup = e.fn();
		} finally {
			--runDepth;
			activeSub = prevSub;
			e.flags &= ~ReactiveFlags.RecursedCheck;
			purgeDeps(e);
		}
	} else if (e.deps !== undefined) {
		e.flags = ReactiveFlags.Watching | (flags & HasChildEffect);
	}
}
function flush(): void {
	try {
		while (notifyIndex < queuedLength) {
			const effect = queued[notifyIndex]!;
			queued[notifyIndex++] = undefined;
			run(effect);
		}
	} finally {
		while (notifyIndex < queuedLength) {
			const effect = queued[notifyIndex]!;
			queued[notifyIndex++] = undefined;
			effect.flags |= ReactiveFlags.Watching | ReactiveFlags.Recursed;
		}
		notifyIndex = 0;
		queuedLength = 0;
	}
}
function computedOper<T>(this: ComputedNode<T>): T {
	const flags = this.flags;
	if (
		flags & ReactiveFlags.Dirty
		|| (
			flags & ReactiveFlags.Pending
			&& (
				checkDirty(this.deps!, this)
				|| (this.flags = flags & ~ReactiveFlags.Pending, false)
			)
		)
	) {
		if (updateComputed(this)) {
			const subs = this.subs;
			if (subs !== undefined) {
				shallowPropagate(subs);
			}
		}
	} else if (!flags) {
		this.flags = ReactiveFlags.Mutable | ReactiveFlags.RecursedCheck;
		const prevSub = setActiveSub(this);
		try {
			this.value = this.getter();
		} finally {
			activeSub = prevSub;
			this.flags &= ~ReactiveFlags.RecursedCheck;
		}
	}
	const sub = activeSub;
	if (sub !== undefined) {
		link(this, sub, cycle);
	}
	return this.value!;
}
function signalOper<T>(this: SignalNode<T>, ...value: [T]): T | void {
    if (value.length) {
        const next = value[0];

        if (!Object.is(this.pendingValue, next)) {
            const previousPending = this.pendingValue;

            writeJournal?.push(() => {
                this.pendingValue = previousPending;
                this.flags = ReactiveFlags.Mutable;
            });

            this.pendingValue = next;
            this.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;

            const subs = this.subs;

            if (subs !== undefined) {
                propagate(subs, !!runDepth);

                if (!batchDepth) {
                    flush();
                }
            }
        }
    } else {
        if (value.length) {
            const next = value[0];
    
    if (!Object.is(this.pendingValue, next)) {
        this.pendingValue = next;
                this.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
                const subs = this.subs;
                if (subs !== undefined) {
                    propagate(subs, !!runDepth);
                    if (!batchDepth) {
                        flush();
                    }
                }
            }
        } else {
            if (this.flags & ReactiveFlags.Dirty) {
                if (updateSignal(this)) {
                    const subs = this.subs;
                    if (subs !== undefined) {
                        shallowPropagate(subs);
                    }
                }
            }
            const sub = activeSub;
            if (sub !== undefined) {
                link(this, sub, cycle);
            }
            return this.currentValue;
        }
    }
}
function runCleanup(e: EffectNode): void {
	const cleanup = e.cleanup!;
	e.cleanup = undefined;
	const prevSub = activeSub;
	activeSub = undefined;
	try {
		cleanup();
	} finally {
		activeSub = prevSub;
	}
}
function effectOper(this: EffectNode): void {
	effectScopeOper.call(this);
	if (this.cleanup) {
		runCleanup(this);
	}
}
function effectScopeOper(this: EffectScopeNode): void {
	this.flags = ReactiveFlags.None;
	disposeAllDepsInReverse(this);
	const sub = this.subs;
	if (sub !== undefined) {
		unlink(sub);
	}
}
function disposeAllDepsInReverse(sub: ReactiveNode): void {
	let link = sub.depsTail;
	while (link !== undefined) {
		const prev = link.prevDep;
		unlink(link, sub);
		link = prev;
	}
}
function purgeDeps(sub: ReactiveNode) {
	const depsTail = sub.depsTail;
	let dep = depsTail !== undefined ? depsTail.nextDep : sub.deps;
	while (dep !== undefined) {
		dep = unlink(dep, sub);
	}
}
export function isSignal(fn: unknown): fn is Signal<any> {
    return typeof fn === 'function' && BRAND_SIGNAL in fn;
}
function isComputed(fn: unknown) { // : fn is Computed<any>
    return typeof fn === 'function' && BRAND_COMPUTED in fn;
}
export function isEffect(fn: unknown) { // : fn is Effect
    return typeof fn === 'function' && BRAND_EFFECT in fn;
}
export function isEffectScope(fn: unknown){// : fn is EffectScope 
    return typeof fn === 'function' && BRAND_EFFECT_SCOPE in fn;
}

// #endregion 
//                                                                                                                    [SIGNALS_HUB]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class SIGNALS_HUB{ 
	public root: SIGNALS
	public STATE_CONTEXT = 'sauron'
	constructor(root:SIGNALS) {
		this.root = root
	}
	public atom_reg = (() => {
		const mt = <K, V>(s: S = this.root.s, m: S= this.root.m, r: S = this.root.r): Map<A, A> => {
			// 1. Root Lookup via Module Cache (Fast path)
			let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
			if (!root) {
				root = new Map<A, Map<A, Map<A, A>>>();
				(globalThis as A)[r] = root;
			}

			// 2. Master Map Lookup
			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new Map<A, Map<A, A>>();
				root.set(m, masterMap);
			}

			// 3. Slave Map Lookup
			let slaveMap = masterMap.get(`${s}_SIGNALS_`);
			if (!slaveMap) {
				slaveMap = new Map<K, V>();
				masterMap.set(`${s}_SIGNALS_`, slaveMap);
			}
			return slaveMap as Map<A, A>;
		}; // get channels / root channels
		mt.gks = (r = this.root.r) => {
			// 1. Root Lookup via Module Cache (Fast path)
			let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
			if (!root) {
				root = new Map<A, Map<A, Map<A, A>>>();
				(globalThis as A)[r] = root;
			}
			return root;
		}; // get sub_channel / master channels
		mt.mss = (m = this.root.m, r = this.root.r) => {
			let root = this.atom_reg.gks(r);
			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new Map<A, Map<A, A>>();
				root.set(m, masterMap);
			}
			return masterMap;
		}; // get value
		mt.v = <T,>(k: A, t = this.root.s, s = this.root.m, c = this.root.r) => {
			const reg = this.atom_reg(t, s, c);
			return reg.get(k) as SIGNAL_CELL<T>;
		}; // get meta value
		mt.GET_META = (meta_key: string, s = this.root.s, m = this.root.m, r = this.root.r) => {
			const atom_reg = this.atom_reg(s, m, r);
			let meta_cache = atom_reg.get(meta_key);
			if (!meta_cache) {
				meta_cache = new Map<A, A>();
				this.root.create(meta_key, meta_cache, undefined, s, m, r);
			}
			return meta_cache;
		};
		mt.MODULE_CACHE = <T = A,>(s = this.root.s, m = this.root.m, r = this.root.r): T => {
			return this.atom_reg.GET_META(this.root.mc, s, m, r) as T;
		};
		return mt;
	})();
	public _channels(s: A = this.root.s, m: A = this.root.m, r: A = this.root.r) {
       let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
        //let root = (globalThis as A)[r];
        if (!root) {
            root = new Map<A, Map<A, Map<A, A>>>();
            (globalThis as A)[r] = root;
        }

        let masterMap = root.get(m);
        if (!masterMap) {
            masterMap = new Map<A, Map<A, A>>();
            root.set(m, masterMap);
        }

        let metaMap = masterMap.get(`${s}_CHANNELS`);
        if (!metaMap) {
            metaMap = {
                channels: new Map<string, Set<A>>(),
                sub_channels: new Map<string, Map<string, Set<A>>>(),
                trip_channels: new Map<string, Map<string, Set<A>>>(),
                quad_channels: new Map<string, Map<string, Set<A>>>(),
            };
            masterMap.set(`${s}_CHANNELS`, metaMap);
        }
        return metaMap;
    }
	public _listeners(s = this.root.s, m = this.root.m, r = this.root.r) {
		return this._channels(s, m, r).channels as Map<string, Set<A>>;
	}
	public MODULE_CACHE(s = this.root.s, m = this.root.m, r = this.root.r) {
		const meta = this.getMetadata(s, m, r);
		let module_cache = meta.get(this.root.mc);
		if (!module_cache) {
			module_cache = new Map<A, Map<A, Map<A, A>>>();
			this.root.create(this.root.mc, module_cache, undefined, s, m, r);
		}
		return module_cache;
	}
	public getMetadata(s = this.root.s, m = this.root.m, r = this.root.r) {
		let root = this.STATE_CONTEXT === "sauron" ? (globalThis as A)[r] : null;
		if (!root) {
			root = new Map<A, Map<A, Map<A, A>>>();
			(globalThis as A)[r] = root;
		}

		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new Map<A, Map<A, A>>();
			root.set(m, masterMap);
		}

		let metaMap = masterMap.get(`${s}_SIGNALS`);
		if (!metaMap) {
			metaMap = {
				_initial: new Map<string, A>(),
				_sys: new Map<string, SysEntry>(),
				_keyContracts: new Map<string, Set<string>>(),
				_causal: new Map<string, CausalEntry[]>(),
				_conflicts: new Map<string, ConflictPolicy>(),      
				_contracts: new Map<string, A>(),      
				_scopes: new Map<string, SignalScope>(),
				_msgHandlers: new Map<string, Set<(p: A) => void>>(),
				_mainMap: new Map<A, string>(),
				_tupleCache: new Map<string, string>(),
				_weakMap: new WeakMap<object, string>(),
			};
			masterMap.set(`${s}_SIGNALS`, metaMap);
		}
		return metaMap;
	}
}
// #endregion
//                                                                                                                    [TIME TRAVEL]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class DRSTimeTravel {
	public _time_travel_reverting = false;
    public root
	constructor( root: A) {
        this.root = root
    }
	public FULFIL_THIS  (root:A) {
		this.root = root //this.root.SIGNALS;
	}
	private defaultChannels = (): Required<HistoryChannels> => ({
		r: this.root.r,
		m: this.root.m,
		s: '_TC_TIME_TRAVEL_',
	});
	private resolveChannels = (		channels: HistoryChannels = this.defaultChannels(),	): Required<HistoryChannels> => ({
		...this.defaultChannels(),
		...channels,
	});
	private resolveHistory = <T = unknown>(		key: SignalKey,		channels: HistoryChannels = this.defaultChannels(),	) => {
		const { s, m, r } = this.resolveChannels(channels);
		const meta = this.root.SAURON.getMetadata(s, m, r);
		return meta._history.get(key) as HistoryEntry<T>[] | undefined;
	};
	public history = <T = unknown>(		key: SignalKey,		channels: HistoryChannels = this.defaultChannels(),	): readonly HistoryEntry<T>[] => {
		return this.resolveHistory<T>(key, channels) ?? [];
	};
	private replay = <T = unknown>(
		key: SignalKey,
		value: T,
		channels: HistoryChannels,
		source: string,
	): T => {
		const { s, m, r } = this.resolveChannels(channels);

		this._time_travel_reverting = true;

		try {
			this.root.store.set(key, value, {
				sl_reg: s,
				ms_reg: m,
				globalKey: r,
				triggerRender: true,
			});

			return value;
		} finally {
			this._time_travel_reverting = false;
		}
	};
	public graft = (drs: A) => {
		drs.time_travel = {
			back: this.back,
			forward: this.forward,
			goto: this.goto,
			history: this.history,
		};

		return drs.time_travel;
	};
	public back = <T = unknown>(
		key: SignalKey,
		steps = 1,
		channels: HistoryChannels = this.defaultChannels(),
	): T | undefined => {
		const history = this.resolveHistory<T>(key, channels);
		if (!history?.length || steps <= 0) return undefined;
	
		const { s, m, r } = this.resolveChannels(channels);
		const meta = this.root.SAURON.getMetadata(s, m, r);
		const index = meta._timeTravel.get(key) ?? history.length - 1;
		const targetIndex = Math.max(0, index - steps);
	
		if (targetIndex === index) return undefined;
	
		meta._timeTravel.set(key, targetIndex);
		for (let i = 0; i < history.length; i++) {
			history[i].undone = i > targetIndex;
		}
	
		return this.replay(key, history[targetIndex].next as T, channels, "time-travel.back");
	};
	public forward = <T = unknown>(
		key: SignalKey,
		steps = 1,
		channels: HistoryChannels = this.defaultChannels(),
	): T | undefined => {
		const history = this.resolveHistory<T>(key, channels);
		if (!history?.length || steps <= 0) return undefined;
	
		const { s, m, r } = this.resolveChannels(channels);
		const meta = this.root.SAURON.getMetadata(s, m, r);
		const index = meta._timeTravel.get(key) ?? history.length - 1;
		const targetIndex = Math.min(
			history.length - 1,
			index + steps,
		);
	
		if (targetIndex === index) return undefined;
	
		meta._timeTravel.set(key, targetIndex);
		for (let i = 0; i < history.length; i++) {
			history[i].undone = i > targetIndex;
		}
	
		return this.replay(key, history[targetIndex].next as T, channels, "time-travel.forward");
	};
	public goto = <T = unknown>(
		key: SignalKey,
		sequence: number,
		channels: HistoryChannels = this.defaultChannels(),
	): T | undefined => {
		const history = this.resolveHistory<T>(key, channels);
		if (!history?.length) return undefined;
	
		const { s, m, r } = this.resolveChannels(channels);
		const meta = this.root.SAURON.getMetadata(s, m, r);
		const targetIndex = history.findIndex((entry) => entry.k === sequence);
		const index = meta._timeTravel.get(key) ?? history.length - 1;
	
		if (targetIndex < 0 || targetIndex === index) return undefined;
	
		meta._timeTravel.set(key, targetIndex);
		for (let i = 0; i < history.length; i++) {
			history[i].undone = i > targetIndex;
		}
	
		return this.replay(key, history[targetIndex].next as T, channels, "time-travel.goto");
	};
}
// #endregion 
//                                                                                                                         TIMELINE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface TimelineEntry<T> {
  id: string;
  scopeId: string | null;
  key: SignalKey;
  previous: T;
  next: T;
  source: string;
  cause?: string;
  timestamp: number;
  txId?: string;
}
export class Timeline {
  private entries: TimelineEntry<any>[] = [];
  private index = -1;
  private replaying = false;

  record<T>(entry: Omit<TimelineEntry<T>, 'id'>): void {
    if (this.replaying) return;

    this.entries.splice(this.index + 1);
    this.entries.push({ id: crypto.randomUUID(), ...entry });

    if (this.entries.length > 1000) {
      this.entries.shift();
    } else {
      this.index++;
    }
  }
  back(steps = 1): boolean {
    return this.goto(this.index - steps);
  }
  forward(steps = 1): boolean {
    return this.goto(this.index + steps);
  }
  goto(target: number): boolean {
    if (target < 0 || target >= this.entries.length) return false;
    if (target === this.index) return false;

    const direction = target > this.index ? 1 : -1;

    this.replaying = true;

    try {
      while (this.index !== target) {
        this.index += direction;

        const entry = this.entries[this.index];

        if (direction === 1) {
          entry.key.write?.(entry.next);
        } else {
          entry.key.write?.(entry.previous);
        }
      }

      return true;
    } finally {
      this.replaying = false;
    }
  }
  causalChain(key: SignalKey): TimelineEntry<any>[] {
    return this.entries.filter((entry) => entry.key === key);
  }
}
// #endregion
//                                                                                                                     RX SCHEDULER
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 class RX_SCHEDULER {
	public root: SIGNALS
	public FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325n;
	public FNV_PRIME_64 = 0x100000001b3n;
	public U64_MAX = (1n << 64n) - 1n;
	public U64_MOD = 1n << 64n;
	public _localChange = false;
	public activeScope: EffectScopeNode | undefined;
	public defaultErrorHandler: EffectErrorHandler | null = null;
	public effects = new Set<SyncJob>();
	public externals = new Set<SyncJob>();
	public observers = new Set<SyncJob>();
	public flushQueued = false;
	public flushing = false;
	public isReact = false;
	public computations = new Set<SyncJob>();
	public init_hooks_context = false;
	public notifyIndex = 0;
	public queued: (EffectNode | undefined)[] = [];
	// Sets dedupe jobs: one computation/effect/render callback runs once per flush, even if multiple atoms invalidate it.
	public asyncEffects = new Map<string, AsyncEffectJob>();
	public batch<T>(fn: () => T): T {
		batchDepth += 1;
		try {
			return fn();
		} finally {
			batchDepth -= 1;

			if (batchDepth === 0) {
				this.requestFlush();
			}
		}
	}
	public enqueueComputation(job: SyncJob) {
		this.computations.add(job);
		this.requestFlush();
	}
	public enqueueEffect(job: SyncJob) {
		this.effects.add(job);
		this.requestFlush();
	}
	public enqueueExternal(job: SyncJob) {
		this.externals.add(job);
		this.requestFlush();
	}
	public enqueueObserver(job: SyncJob) {
		this.observers.add(job);
		this.requestFlush();
	}
	public enqueueAsyncEffect(job: AsyncEffectJob) {
		this.asyncEffects.set(job.id, job);
		this.requestFlush();
	}
	private requestFlush() {
		if (batchDepth > 0 || this.flushQueued || this.flushing) {
			return;
		}

		this.flushQueued = true;

		queueMicrotask(() => {
			this.flushQueued = false;
			void this.rx_flush();
		});
	}
	private take<T>(set: Set<T>) {
		const jobs = [...set];
		set.clear();
		return jobs;
	}
	private runSyncJobs(jobs: SyncJob[], phase: string) {
		for (const job of jobs) {
			try {
				job();
			} catch (error) {
				console.error(`[ReactiveScheduler:${phase}] job failed:`, error);
			}
		}
	}
	private async rx_flush() {
		if (this.flushing || batchDepth > 0) return;

		this.flushing = true;

		try {
			// A computed job may write another atom and schedule more computed jobs. Drain until no computed work remains so derived state is stable first.
			while (this.computations.size > 0) {
				this.runSyncJobs(this.take(this.computations), "computation");
			}
			this.runSyncJobs(this.take(this.effects), "effect");
			await this.flushAsyncEffects();
			this.runSyncJobs(this.take(this.externals), "external");
			this.runSyncJobs(this.take(this.observers), "observer");
		} finally {
			this.flushing = false;
			if (
				// If work arrived while flushing, schedule the next batch.
				this.computations.size > 0
				|| this.effects.size > 0
				|| this.externals.size > 0
				|| this.observers.size > 0
				|| this.asyncEffects.size > 0
			) {
				this.requestFlush();
			}
		}
	}
	private async flushAsyncEffects() {
		if (this.asyncEffects.size === 0) return;

		const queued = [...this.asyncEffects.values()];
		this.asyncEffects.clear();

		const queue = new TIERED_QUEUE({
			defaultConcurrent: 20,
			onTierError: (tier, error) => {
				console.error(`[ReactiveScheduler] async effect tier ${tier} failed:`, error);
			},
		});

		for (const job of queued) {
			void queue.ADD(
				job.priority,
				job.id,
				async () => {
					await job.run();
				},
				{
					retry: job.retry,
					timeout: job.timeout,
				},
			);
		}

		await queue.execute();
	}
	public link
	public unlink
	public propagate
	public checkDirty
	public shallowPropagate
	constructor(root: SIGNALS) {
		this.root = root
		const self = this
		const { link, unlink, propagate, checkDirty, shallowPropagate } = createReactiveSystem({
			update(node: SignalNode | ComputedNode | EffectScopeNode): boolean {
				if ("getter" in node) {
					return self.updateComputed(node);
				}
				if ("currentValue" in node) {
					return self.updateSignal(node);
				}
				node.flags = ReactiveFlags.Mutable;
				return true;
			},
			notify(effect: EffectNode) {
				let insertIndex = queuedLength;
				let firstInsertedIndex = insertIndex;

				do {
					self.queued[insertIndex++] = effect;
					effect.flags &= ~ReactiveFlags.Watching;
					effect = effect.subs?.sub as EffectNode;
					if (effect === undefined || !(effect.flags & ReactiveFlags.Watching)) {
						break;
					}
				} while (true);

				queuedLength = insertIndex;

				while (firstInsertedIndex < --insertIndex) {
					const left = self.queued[firstInsertedIndex];
					self.queued[firstInsertedIndex++] = self.queued[insertIndex];
					self.queued[insertIndex] = left;
				}
			},
			unwatched(node: SignalNode | ComputedNode | EffectNode | EffectScopeNode) {
				if ("getter" in node) {
					if (node.depsTail !== undefined) {
						node.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
						disposeAllDepsInReverse(node);
					}
				} else if ("currentValue" in node) {
					// Nothing to do for signals, they are always mutable and never dirty until pendingValue changes
				} else if ("fn" in node) {
					effectOper.call(node);
				} else {
					effectScopeOper.call(node);
				}
			},
		});
		this.link = link
		this.unlink = unlink
		this.propagate = propagate
		this.checkDirty = checkDirty
		this.shallowPropagate = shallowPropagate
	}
	
	public flush = (): void => {
		try {
			while (this.notifyIndex < queuedLength) {
				const effect = this.queued[this.notifyIndex]!;
				this.queued[this.notifyIndex++] = undefined;
				this.run(effect);
			}
		} finally {
			while (this.notifyIndex < queuedLength) {
				const effect = this.queued[this.notifyIndex]!;
				this.queued[this.notifyIndex++] = undefined;
				effect.flags |= ReactiveFlags.Watching | ReactiveFlags.Recursed;
			}
			this.notifyIndex = 0;
			queuedLength = 0;
		}
	};
	public assertSamplingRate = (rate: number): void => {
		if (!Number.isFinite(rate) || rate < 0 || rate > 1) {
			throw new RangeError(`sampling rate must be a finite number from 0 to 1; got ${rate}`);
		}
	};
	public utf8Bytes = (value: string): Uint8Array => {
		return new TextEncoder().encode(value);
	};
	public fnv1a64 = (value: string): bigint => {
		let hash = this.FNV_OFFSET_BASIS_64;

		for (const byte of this.utf8Bytes(value)) {
			hash ^= BigInt(byte);
			hash = (hash * this.FNV_PRIME_64) & this.U64_MAX;
		}

		return hash;
	};
	public samplingThreshold = (rate: number): bigint => {
		if (rate === 0) return 0n;
		if (rate === 1) return this.U64_MOD;

		// Decimal rates are converted deterministically through their string form.
		// For stricter cross-language protocol guarantees, accept an integer
		// numerator/denominator instead of a JS floating-point value.
		const [whole, fraction = ""] = String(rate).split(".");
		const denominator = 10n ** BigInt(fraction.length);
		const numerator = BigInt(`${whole}${fraction}`);

		return (this.U64_MOD * numerator) / denominator;
	};
	public update = (node: SignalNode | ComputedNode | EffectScopeNode): boolean => {
		if ("getter" in node) {
			return this.updateComputed(node);
		}
		if ("currentValue" in node) {
			return this.updateSignal(node);
		}
		node.flags = ReactiveFlags.Mutable;
		return true;
	};
	public notify = (effect: EffectNode) => {
		let insertIndex = queuedLength;
		let firstInsertedIndex = insertIndex;

		do {
			this.queued[insertIndex++] = effect;
			effect.flags &= ~ReactiveFlags.Watching;
			effect = effect.subs?.sub as EffectNode;
			if (effect === undefined || !(effect.flags & ReactiveFlags.Watching)) {
				break;
			}
		} while (true);

		queuedLength = insertIndex;

		while (firstInsertedIndex < --insertIndex) {
			const left = this.queued[firstInsertedIndex];
			this.queued[firstInsertedIndex++] = this.queued[insertIndex];
			this.queued[insertIndex] = left;
		}
	};
	public unwatched = (node: SignalNode | ComputedNode | EffectNode | EffectScopeNode) => {
		if ("getter" in node) {
			if (node.depsTail !== undefined) {
				node.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
				disposeAllDepsInReverse(node);
			}
		} else if ("currentValue" in node) {
			// Nothing to do for signals, they are always mutable and never dirty until pendingValue changes
		} else if ("fn" in node) {
			effectOper.call(node);
		} else {
			effectScopeOper.call(node);
		}
	};
	public isValidLink = (checkLink: Link, sub: ReactiveNode): boolean => {
		let link = sub.depsTail;
		while (link !== undefined) {
			if (link === checkLink) {
				return true;
			}
			link = link.prevDep;
		}
		return false;
	};
	public setActiveSub = (sub?: ReactiveNode) => {
		const prevSub = activeSub;
		activeSub = sub;
		return prevSub;
	};
	public getActiveSub = (): ReactiveNode | undefined => {		return activeSub;	};
	public getBatchDepth = (): number => { return batchDepth;	};
	public startBatch = () => { ++batchDepth;	};
	public endBatch = () => { if (!--batchDepth) {			this.flush();		}	};
	public untrackFn = <T,>(fn: () => T): T => {
		const prev = this.setActiveSub(undefined);
		try {
			return fn();
		} finally {
			this.setActiveSub(prev);
		}
	};
	public untrack = <T,>(fn: () => T): T => {
		return this.untrackFn(fn);
	};
	public run = (e: EffectNode): void => {
		const flags = e.flags;
		if (flags & ReactiveFlags.Dirty || (flags & ReactiveFlags.Pending && this.checkDirty(e.deps!, e))) {
			if (flags & HasChildEffect) {
				let link = e.depsTail;
				while (link !== undefined) {
					const prev = link.prevDep;
					const dep = link.dep;
					if (!("getter" in dep) && !("currentValue" in dep)) {
						this.unlink(link, e);
					}
					link = prev;
				}
			}
			if (e.cleanup) {
				runCleanup(e);
				if (!e.flags) {
					return;
				}
			}
			e.depsTail = undefined;
			e.flags = ReactiveFlags.Watching | ReactiveFlags.RecursedCheck;
			const prevSub = this.setActiveSub(e);
			try {
				++cycle;
				++runDepth;
				e.cleanup = e.fn();
			} catch (err:A) {
				const handler = e.onError ?? this.defaultErrorHandler;
				if (handler) handler(err, e);
				else
					queueMicrotask(() => {
						throw err;
					});
			} finally {
				--runDepth;
				activeSub = prevSub;
				e.flags &= ~ReactiveFlags.RecursedCheck;
				purgeDeps(e);
			}
		} else if (e.deps !== undefined) {
			e.flags = ReactiveFlags.Watching | (flags & HasChildEffect);
		}
	};
	public updateSignal = (s: SignalNode): boolean => {
		s.flags = ReactiveFlags.Mutable;
		return s.currentValue !== (s.currentValue = s.pendingValue);
	};
	public updateComputed = (c: ComputedNode): boolean => {
		if (c.flags & HasChildEffect) {
			let link = c.depsTail;
			while (link !== undefined) {
				const prev = link.prevDep;
				const dep = link.dep;
				if (!("getter" in dep) && !("currentValue" in dep)) {
					this.unlink(link, c);
				}
				link = prev;
			}
		}
		c.depsTail = undefined;
		c.flags = ReactiveFlags.Mutable | ReactiveFlags.RecursedCheck;
		const prevSub = this.setActiveSub(c);
		try {
			++cycle;
			const oldValue = c.value;
			return oldValue !== (c.value = c.getter(oldValue));
		} finally {
			activeSub = prevSub;
			c.flags &= ~ReactiveFlags.RecursedCheck;
			purgeDeps(c);
		}
	};
	public trigger = (fn: () => void) => {
		const sub: ReactiveNode = {
			deps: undefined,
			depsTail: undefined,
			flags: ReactiveFlags.Watching | ReactiveFlags.RecursedCheck,
		};
		const prevSub = this.setActiveSub(sub);
		++batchDepth;
		try {
			fn();
		} finally {
			activeSub = prevSub;
			sub.flags = ReactiveFlags.None;
			let link = sub.deps;
			while (link !== undefined) {
				const dep = link.dep;
				link = this.unlink(link, sub);
				const subs = dep.subs;
				if (subs !== undefined) {
					this.propagate(subs, !!runDepth);
					this.shallowPropagate(subs);
				}
			}
			if (!--batchDepth) {
				this.flush();
			}
		}
	};

    public  runTeardown=(t: Teardown): void =>{
		if (t == null) return;
		if (typeof t === 'function') { t(); return; }
		if (Array.isArray(t)) { for (const x of t) this.runTeardown(x); return; }
		const anyT = t as A;
		if (typeof anyT.unsubscribe === 'function') { anyT.unsubscribe(); return; }
		if (typeof anyT[Symbol.dispose] === 'function') { anyT[Symbol.dispose](); return; }
		throw new TypeError('Invalid teardown: ' + t);
	}
	public setEffectErrorHandler = (fn: EffectErrorHandler | null): void => {
		this.defaultErrorHandler = fn;
	}
	public  sysEffect=(fn: () => void | (() => void)): ReturnType<typeof effect> =>{
		return effect(fn)
	}
	shouldSample = (key: string, config: SamplingConfig, namespace = ""): boolean => {
		this.assertSamplingRate(config.rate);

		if (config.rate === 0) return false;
		if (config.rate === 1) return true;

		const version = config.version ?? 1;
		const algorithm = config.algorithm ?? "fnv1a64";

		const input = [`v=${version}`, `alg=${algorithm}`, `seed=${String(config.seed)}`, `namespace=${namespace}`, `key=${key}`].join("\u001f");

		return this.fnv1a64(input) < this.samplingThreshold(config.rate);
	};

	
}
// User-facing trigger-render: force a flush for one key (or everything pending) without A state change. Zero-arg = same as flushSync for pending notifications.
/**export function rerender(key?: string): void {
 if (key !== undefined) {
	 //   const hs = graphHookStates.get(key);
	 // if (hs) notifyHook(`${GRAPH_BRIDGE_COMPONENT_ID}:${key}`);
	 notifyHook(key);
 } else {
	 flushSync();
 }
} */
// #endregion 
//                                                                                                                              TX
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type Undo = () => V;
const undoStack: Undo[] = [];
function journalSignalWrite<T>(
  node: SignalNode<T>,
  previousPending: T,
): void {
  undoStack.push(() => {
    node.pendingValue = previousPending;
    node.flags = ReactiveFlags.Mutable;
  });
}
let activeTx: Undo[] | null = null;
export function beginTx(): Undo[] {
  const journal: Undo[] = [];

  const previous = activeTx;
  activeTx = journal;

  return journal;
}
export function commitTx(): V {
  activeTx = null;
}
export function rollbackTx(journal: Undo[]): V {
  for (let i = journal.length - 1; i >= 0; i--) {
    journal[i]();
  }

  activeTx = null;
}
export function tx<T>(fn: () => T): T {
  const journal = beginTx();

  return signals.RX_S.batch(() => {
    try {
      const result = fn();
      commitTx();
      return result;
    } catch (error) {
      rollbackTx(journal);
      throw error;
    }
  });
}
// #endregion
//                                                                                                                          [SCOPE]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export type Disposer = () => V;
export interface Scope {
  id: string;
  parent: Scope | null;
  disposed: boolean;
  children: Set<Scope>;
  cleanups: Set<Disposer>;
  abort: AbortController;

  run<T>(fn: () => T): T;
  dispose(): void;
}
let activeScope: Scope | NU = null;
export function getScope(): Scope | NU {  return activeScope;}
export function createScope(  fn: (scope: Scope) => V | P<V>,  parent: Scope | NU = activeScope): Scope {
    const scope: Scope = {
        id: crypto.randomUUID(),
        parent,
        disposed: false,
        children: new Set(),
        cleanups: new Set(),
        abort: new AbortController(),

        run<T>(task: () => T): T {
        const previous = activeScope;
        activeScope = scope;

        try {
            return task();
        } finally {
            activeScope = previous;
        }
        },

        dispose() {
        if (scope.disposed) return;
        scope.disposed = true;

        scope.abort.abort();

        for (const child of [...scope.children]) {
            child.dispose();
        }

        for (const cleanup of [...scope.cleanups].reverse()) {
            try {
            cleanup();
            } catch (error) {
            reportError(error);
            }
        }

        scope.children.clear();
        scope.cleanups.clear();
        parent?.children.delete(scope);
        },
    };

    parent?.children.add(scope);

    const result = scope.run(() => fn(scope));

    // The critical part Solid makes users do manually:
    // preserve ownership across await.
    if (result instanceof Promise) {
        result.catch((error) => reportError(error));
    }

  return scope;
}
export function onScopeCleanup(disposer: Disposer): V {
  if (!activeScope) {
    throw new Error(
      '[Sauron] onScopeCleanup() called outside a scope'
    );
  }

  activeScope.cleanups.add(disposer);
}
export function withScope<T>(scope: Scope, fn: () => T): T {
  return scope.run(fn);
}/**const userScope = createScope(async (scope) => {
 const user = await fetchUser(scope.abort.signal); // owner survives await

 effect(() => {
   console.log(user.value);
 });

 onScopeCleanup(() => socket.close());
});

userScope.dispose(); // aborts fetch, disposes effects, closes socket */
export interface Resource<T> {
  data: Signal<T | undefined>;
  loading: Signal<boolean>;
  error: Signal<unknown | null>;
  reload(): Promise<void>;
  dispose(): void;
}
export function resource<T>(
  loader: (abort: AbortSignal, previous: T | undefined) => Promise<T>,
  options: {
    scope?: Scope;
    initial?: T;
    revalidate?: Signal<unknown>;
  } = {}
): Resource<T> {
  const scope = options.scope ?? createScope(() => {});
  const data = signal<T | undefined>(options.initial);
  const loading = signal(false);
  const error = signal<unknown | null>(null);

  let generation = 0;

  const load = async (): Promise<void> => {
    const current = ++generation;
    const controller = new AbortController();

    onScopeCleanup(() => controller.abort());

    loading(true);
    error(null);

    try {
      const value = await loader(controller.signal, data.peek());

      // Ignore stale completions.
      if (current !== generation || controller.signal.aborted) return;

      data(value);
    } catch (err) {
      if (current !== generation || controller.signal.aborted) return;

      error(err);
    } finally {
      if (current === generation && !controller.signal.aborted) {
        loading(false);
      }
    }
  };

  // Revalidate automatically when the trigger changes.
  effect(() => {
    options.revalidate?.();
    void load();
  });

  scope.cleanups.add(() => {
    generation++;
    loading(false);
  });

  return {
    data,
    loading,
    error,
    reload: load,
    dispose: () => scope.dispose(),
  };
}
export interface ReactiveAdapter {
	scope(): Scope;
	useValue<T>(signal: Signal<T>): T;
	onScopeCleanup(fn: Disposer): void;
}
// React
export function useSignal<T>(signal: SIGNAL_CELL<T>): T {
  const scope = useSauronScope();
  const [, force] = useReducer((n:A) => n + 1, 0);

  useEffect(() => {
    return signal.subscribe(() => force());
  }, [signal]);

  return signal.read();
}
// #endregion
//                                                                                                    [SIGNALS]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class SIGNALS {
	//                                                                                                                  [CONSTANTS]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public TIME_TRAVEL: DRSTimeTravel;
	public RX_S: RX_SCHEDULER;
	public hub: SIGNALS_HUB;

	public RTRN_TO_SDR = "_RETURN_TO_SENDER";
	public twm = "__THE_WARLOCK_MODULE_";
	public mc = `__THE_WARLOCK_MODULE_CACHE__`;
	public subcribers = "_SUBSCRIBERS";
	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "_TC_SIGNALS_";
	public k: A = "__SIGNALS__";

	public activeTracer: Map<SignalKey, SIGNAL_CELL> | null = null;

	constructor(root: A = null) {
		this.s = Store.get("SAURON_SLAVE_REG") ?? "_TC_ROOT_";
		this.m = Store.get("SAURON_MASTER_REG") ?? "TW_MASTER_REG";
		this.r = Store.get("THE_WIZARD_ROOT_REG") ?? "TW_ROOT_REG";
		this.hub = new SIGNALS_HUB(this);
		this.RX_S = new RX_SCHEDULER(root);
		this.TIME_TRAVEL = new DRSTimeTravel(this);
	}
	public sg(k: A) {
		return `${k}`;
	}
	u = undefined;
	uuuu = [this.u, this.u, this.u, this.u];
	UUUU = [this.s, this.m, this.r];
	uuu = [this.u, this.u, this.u];
	UUU = [this.s, this.m, this.r];
	uu = [this.u, this.u];
	UU = [this.m, this.r];

	private total_cache_size = 5000;

	// #endregion
	//                                                                                                                       [APII]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public atom<T = any>(v: A, key: SignalKey = `__THE_WARLOCK_SIGNALS_${__sid.b()}`, s = this.s, m = this.m, r = this.r, options?: SignalOptions) {
		const rk = this.create(key, v, options, s, m, r);
		const self = this;
		return {
			get value(): T {
				self._bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			set value(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			peek(): T {
				return rk.peek();
			},
			get current(): T {
				self._bindComponentToSignal(rk, s, m, r);
				return self.read<T>(rk, s, m, r) as T;
			},
			set current(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set crt(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
			set set(v: T) {
				self.write(rk, v, "write", s, m, r);
			},
		};
	}
	public create<T = A>(k: SignalKey, v?: T | (() => T), options?: SignalOptions, s = this.s, m = this.m, r = this.r): SIGNAL_CELL<T> | T {
		const registry = this.hub.atom_reg(s, m, r);
		let atom = registry.get(k);

		if (atom || v === undefined) {
			return atom as SIGNAL_CELL<T> | T;
		}

		const initialValue = typeof v === "function" ? (v as () => T)() : v;
		atom = new SIGNAL_CELL(k, initialValue, s, m, r, options);
		registry.set(k, atom);
		return atom as SIGNAL_CELL<T> | T;
	}
	public get<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): A {
		const registry = this.hub.atom_reg(s, m, r);
		return registry.get(k) as SIGNAL_CELL<T> | UD;
	}
	public write<T>(key: SignalKey, value: T | ((previous: T) => T), source = "write", s = this.s, m = this.m, r = this.r): T {
		return this._writeResolved(key, value, source, s, m, r);
	}
	public read<T>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		return atom?.read();
	}
	public peek<T>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		return atom?.peek();
	}
	public signal<T>(v?: T): {(): T | UD; (value: T | UD): V} {
		return signalOper.bind({value: v, currentValue: v, pendingValue: v, subs: undefined, subsTail: undefined, flags: ReactiveFlags.Mutable}) as () => T | undefined;
	}
	public tx<T>(fn: () => T): T {
		const undo: Array<() => void> = [];
		const previousJournal = writeJournal;
		const journal: Array<() => void> = undo;

		writeJournal = journal;

		return this.RX_S.startBatchScoped(() => {
			try {
				return fn();
			} catch (error) {
				for (let i = journal.length - 1; i >= 0; i--) {
					journal[i]();
				}

				// One flush, only if the rollback changed anything.
				this.RX_S.endBatchScoped();

				throw error;
			} finally {
				writeJournal = previousJournal;
			}
		});
	}
	public runTeardown(t: Teardown): V {
		if (t == null) return;
		if (typeof t === "function") {
			t();
			return;
		}
		if (Array.isArray(t)) {
			for (const x of t) this.runTeardown(x);
			return;
		}
		const anyT = t as A;
		if (typeof anyT.unsubscribe === "function") {
			anyT.unsubscribe();
			return;
		}
		if (typeof anyT[Symbol.dispose] === "function") {
			anyT[Symbol.dispose]();
			return;
		}
		throw new TypeError("Invalid teardown: " + t);
	}
	public reportError(e: U): V {
		const g = globalThis as A;
		//log(`[✘][HOOK_SYSTEM][SIGNALS][REPORT_ERROR]`, { error: e }, t.error)
		if (typeof g.reportError === "function") {
			g.reportError(e);
			return;
		}
		queueMicrotask(() => {
			throw e;
		});
	}
	public registerSchema<T>(k: A, schema: SignalSchema, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		if (!atom) {
			throw new Error(`[Sauron] Cannot register schema for unknown signal "${String(k)}"`);
		}

		atom.setSchema(schema);
	}
	public has(k: A, s = this.s, m = this.m, r = this.r): B {
		return this.hub.atom_reg(s, m, r).get(k) !== undefined;
	}
	public freeze(k: A, s = this.s, m = this.m, r = this.r): void {
		const atom = this.hub.atom_reg(s, m, r).get(k);

		if (!atom) {
			throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`);
		}

		atom.setFrozen();
	}
	public seal(k: A, s = this.s, m = this.m, r = this.r): V {
		const meta = this.hub.atom_reg(s, m, r);
		const atom = meta.get(k);
		atom.setSealed();
	}
	public reset(k: A, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<A> | undefined;

		if (!atom) {
			throw new Error(`[Sauron] Cannot reset unknown signal "${String(k)}"`);
		}

		this._writeResolved(k, atom.initial, "reset", s, m, r, "reset");
	}
	public hydrate<T>(k: A, value: T, s = this.s, m = this.m, r = this.r): V {
		const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;

		if (!atom) {
			this.create(k, value, undefined, s, m, r);
			return;
		}

		atom.write(value, { source: "hydrate", triggerRender: false });
	}
	public derive<T>(k: A, deps: A[], compute: (...values: A[]) => T, s = this.s, m = this.m, r = this.r, options?: { eager?: B; onError?: (err: Error) => V }): S {
		if (this.hub.getMetadata(s, m, r)._sys.get(k)?.isComputed) return k;

		const reg = this.hub.atom_reg<string, A>(s, m, r);

		for (const d of deps) {
			this._sysEnsure(d, reg.get(d), s, m, r);
		}

		const c = computed(() => {
			try {
				const vals = deps.map((d) => this._sysEnsure(d, reg.get(d), s, m, r).read());
				return compute(...vals);
			} catch (err) {
				options?.onError?.(err as Error);
				throw err;
			}
		});

		this.hub.getMetadata(s, m, r)._sys.set(k, { read: () => c(), write: () => {}, node: c as A, isComputed: true });
		reg.set(
			k,
			this.RX_S.untrackFn(() => c()),
		);

		if (options?.eager) {
			// Warm the computed cache once. Do not write its result back
			// into another signal; that defeats lazy recomputation.
			this.RX_S.untrackFn(() => c());
		}
		return k;
	}
	public invalidateDerived(k: A, s = this.s, m = this.m, r = this.r): V {
		const entry = this.hub.getMetadata(s, m, r)._sys.get(k);
		if (!entry || !entry.isComputed) return;
		this._writeResolved(
			k,
			this.RX_S.untrackFn(() => entry.read()),
			"derived",
			"invalidate",
			s,
			m,
			r,
		);
	}
	// #endregion
	//                                                                                                               [TRACE EFFECT]
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public trace = (() => {
		const fn  =(fn: () => V): { reads: SignalKey[]; result: V } =>{
			const reads = new Map<SignalKey, SIGNAL_CELL>();
			const previous = this.activeTracer;
	
			this.activeTracer = reads;
	
			try {
				const result = fn();
	
				return {
					reads: [...reads.keys()],
					result,
				};
			} finally {
				this.activeTracer = previous;
			}
		}
		fn.effect=(fn: () => void | (() => void), s: S = this.s, m: S = this.m, r: S = this.r): () => V =>{
			const reads = new Map<SignalKey, A>();
			const runs: Array<{
				run: number;
				reads: Map<SignalKey, A>;
				at: number;
			}> = [];
	
			let runCount = 0;
	
			const tracked = new Map<SignalKey, SIGNAL_CELL>();
			const registry = this.hub.atom_reg(s, m, r);
	
			const effectDispose = effect(() => {
				runCount++;
	
				const before = new Map<SignalKey, A>();
	
				for (const [key, cell] of tracked) {
					before.set(key, cell.peek());
				}
	
				const result = fn();
	
				const touched = new Map<SignalKey, A>();
	
				for (const [key, cell] of tracked) {
					const value = cell.peek();
	
					if (!Object.is(before.get(key), value)) {
						touched.set(key, value);
					}
				}
	
				runs.push({
					run: runCount,
					reads: new Map(tracked),
					at: Date.now(),
				});
	
				return result;
			});
	
			// Wrap reads by exposing a tracing proxy is not needed for the
			// first version: cells register themselves when read through
			// useCell / read(). For explicit tracing, callers can pass keys.
			return effectDispose;
		}// Finds the first recorded write whose value differs from the current live value. That is the divergence point.
		fn. bisect=(entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, s: S = this.s, m: S = this.m, r: S = this.r): { index: number; key: SignalKey; recorded: A; actual: A } | UD =>{
			const registry = this.hub.atom_reg(s, m, r);
	
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i];
				const atom = registry.get(entry.key) as SIGNAL_CELL | undefined;
	
				if (!atom) continue;
	
				const actual = atom.peek();
	
				if (!Object.is(actual, entry.value)) {
					return {
						index: i,
						key: entry.key,
						recorded: entry.value,
						actual,
					};
				}
			}
	
			return undefined;
		}
		fn.why= <T = A>(k: SignalKey, s: S = this.s, m: S = this.m, r: S = this.r): { key: SignalKey; value: T | UD; version: number; lastWriteSource?: S; causal: CausalEntry[];  history: readonly AtomHistoryEntry<T>[] } | UD =>{
			const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;
	
			if (!atom) return undefined;
	
			return {
				key: atom.key,
				value: atom.peek(),
				version: atom.version,
				lastWriteSource: atom.lastWriteSource,
				causal: [...atom.causal].reverse(),
				history: atom.getHistory(),
			};
		}
		return fn;
	})();
	// #endregion
	//                                                                                                              [STATE SURGERY]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public surgery = (() => {
		// Live-state surgery. Reads the current value, hands it to `mutate`, writes the result back through the normal reactive write path, records a causal entry, and pushes an undo entry onto the surgery journal.
	    // The mutation receives the current value. It may either: - mutate and return nothing (object/array mutation), or - return a replacement value.
		const fn = <T = A>(k: SignalKey, mutate: (draft: T) => T | void, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S; record?: B }): T=> {
			const atom = this.hub.atom_reg(s, m, r).get(k) as SIGNAL_CELL<T> | undefined;
	
			if (!atom) {
				throw new Error(`[Sauron] Cannot perform surgery on unknown signal "${String(k)}"`);
			}
	
			if (atom.frozen) {
				throw new Error(`[Sauron] Cannot perform surgery on frozen signal "${String(k)}"`);
			}
	
			const previous = atom.peek();
	
			// Structured clone gives surgery isolation without touching the
			// reactive kernel. Primitive values pass through unchanged.
			const draft = previous !== null && typeof previous === "object" ? (structuredClone(previous) as T) : previous;
	
			const result = mutate(draft);
	
			const next = result === undefined ? draft : (result as T);
	
			if (Object.is(previous, next)) {
				return previous;
			}
	
			const source = options?.source ?? "surgery";
			const cause = options?.cause ?? `surgery:${String(k)}:${Date.now()}`;
	
			const written = atom.write(next, {
				source,
				cause,
				triggerRender: true,
				recordHistory: options?.record ?? true,
			});
	
			this.surgery.journal.push({
				key: k,
				previous,
				next: written,
				s,
				m,
				r,
			});
	
			return written;
		}//  Undo the most recent surgery. Restoration goes through the normal write path, so computeds, effects, subscribers, history, and channels all see a real write.
		fn.journal = [] as Array<{ key: SignalKey; previous: A; next: A; s: S; m: S; r: S }>;
		fn.undo = (): A | UD =>{
			const entry = this.surgery.journal.pop();
	
			if (!entry) return undefined;
	
			const atom = this.hub.atom_reg(entry.s, entry.m, entry.r).get(entry.key) as SIGNAL_CELL | undefined;
	
			if (!atom) return undefined;
	
			return atom.write(entry.previous, {
				source: "surgery:undo",
				cause: `surgery:undo:${String(entry.key)}`,
				triggerRender: true,
				recordHistory: true,
			});
		}//   Undo every surgery performed since the returned marker was created. Undo every surgery performed since the returned marker was created.
		fn.checkpoint=(): () => V =>{
			const mark = this.surgery.journal.length;
	
			return () => {
				while (this.surgery.journal.length > mark) {
					this.surgery.undo();
				}
			};
		}// Surgery on a nested path. Example: sauron.surgeryAt('user', ['profile', 'email'], email => email.trim());
		fn.at=<T = A, P = A>(k: SignalKey, path: Array<string | number | symbol>, mutate: (draft: P) => P | void, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S }): T =>{
			return this.surgery<T>(
				k,
				(draft) => {
					let target: A = draft;
	
					for (let i = 0; i < path.length - 1; i++) {
						target = target[path[i] as keyof typeof target];
					}
	
					const leafKey = path[path.length - 1] as keyof typeof target;
					const leaf = target[leafKey] as P;
	
					const result = mutate(leaf);
	
					target[leafKey] = result === undefined ? leaf : (result as P);
	
					// Returning undefined keeps the outer draft as the next value.
					return undefined as unknown as T;
				},
				s,
				m,
				r,
				options,
			);
		}
		return fn;
	})();
	public _sys = (() => {
		const fn = () => {}
		fn.ensure=(rk: S, v: A, s = this.s, m = this.m, r = this.r): SysEntry =>{
			const sysMap = this.hub.getMetadata(s, m, r)._sys;
			let entry = sysMap.get(rk);
			if (entry) return entry;
			const sig = signal(v);
			entry = { read: () => sig(), write: (v: A) => sig(v), node: sig as A, isComputed: false };
			sysMap.set(rk, entry);
			if (Store.state.get(rk, { s, m, r }) === undefined) {
				Store.state.set(rk, v, { s, m, r, triggerRender: false });
			}
			return entry;
		}
		fn.snapshot=(s = this.s, m = this.m, r = this.r): M<S, A>=> {
			const out = new Map<string, A>();
			for (const [rk, entry] of this.hub.getMetadata(s, m, r)._sys) {
				if (!entry.isComputed)
					out.set(
						rk,
						this.RX_S.untrackFn(() => entry.read()),
					);
			}
			return out;
		}
		fn.restore=(snap: M<S, A>): V=> {
			for (const [rk, v] of snap) this._sysEnsure(rk, v).write(v);
		}
		fn.dropComputeds=(s = this.s, m = this.m, r = this.r): V=> {
			for (const [rk, entry] of this.hub.getMetadata(s, m, r)._sys) {
				if (entry.isComputed) this.hub.getMetadata(s, m, r)._sys.delete(rk);
			}
		}
		return fn;
	})();
	// #endregion
	//                                                                                                                      BACKEND
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public _validateSchema<T>(schema: SignalSchema, value: T): T {
		try {
			const next = schema.coerce ? schema.coerce(value) : value;

			if (schema.validate(next)) {
				return next as T;
			}

			throw new Error("[Sauron] Schema validation failed");
		} catch (error) {
			console.error("[Sauron] Schema validation failed:", error);
			throw error;
		}
	}
	public _serializeTuple(tuple: A[], s = this.s, m = this.m, r = this.r): S {
		const parts: string[] = new Array(tuple.length);
		const weakMap = this.hub.getMetadata(s, m, r)._weakMap;

		for (let i = 0; i < tuple.length; i++) {
			const item = tuple[i];
			const type = typeof item;

			if (item === null) {
				parts[i] = "null";
			} else if (type === "string") {
				parts[i] = `s:${item}`;
			} else if (type === "number") {
				parts[i] = `n:${item}`;
			} else if (type === "symbol") {
				parts[i] = `sym:${item!.description ?? String(item)}`;
			} else if (type === "bigint") {
				parts[i] = `bi:${item!.toString()}`;
			} else if (type === "undefined") {
				parts[i] = "undef";
			} else if (Array.isArray(item)) {
				parts[i] = `arr:[${this._serializeTuple(item, s, m, r)}]`;
			} else if (type === "object" || type === "function") {
				let refId = weakMap.get(item);
				if (!refId) {
					refId = `ref:${__sid.b()}`;
					weakMap.set(item, refId);
				}
				parts[i] = refId;
			} else {
				parts[i] = `v:${String(item)}`;
			}
		}

		return parts.join("|");
	}
	public _hashTuple(tuple: U[], s = this.s, m = this.m, r = this.r): S {
		const key = this._serializeTuple(tuple, s, m, r);
		const tupleCache = this.hub.getMetadata(s, m, r)._tupleCache;

		const cached = tupleCache.get(key);
		if (cached) {
			tupleCache.delete(key);
			tupleCache.set(key, cached);
			return cached;
		}

		let h1 = 0x811c9dc5;
		let h2 = 0x01000193;
		for (let i = 0; i < key.length; i++) {
			const code = key.charCodeAt(i);
			h1 = Math.imul(h1 ^ code, 16777619) >>> 0;
			h2 = Math.imul(h2 ^ code, 2246822507) >>> 0;
		}

		const result = `tuple:${h1.toString(36)}-${h2.toString(36)}`;
		tupleCache.set(key, result);

		if (tupleCache.size > this.total_cache_size) {
			const toRemove = Math.floor(tupleCache.size * 0.2);
			const iter = tupleCache.keys();
			for (let i = 0; i < toRemove; i++) {
				const k = iter.next().value;
				if (k !== undefined) tupleCache.delete(k);
			}
		}

		return result;
	}
	public _writeResolved<T>(k: SignalKey, value: T | ((previous: T) => T), source: S, s = this.s, m = this.m, r = this.r, cause?: S): T {
		const registry = this.hub.atom_reg(s, m, r);
		let atom = registry.get(k) as SIGNAL_CELL<T> | undefined;

		if (!atom) {
			const initial = typeof value === "function" ? undefined : (value as T);

			atom = this.create(k, initial, undefined, s, m, r) as SIGNAL_CELL<T>;
		}

		if (!atom) {
			throw new Error(`[Sauron] Cannot write unknown signal "${String(k)}"`);
		}

		return atom.write(value, {
			source,
			cause,
			triggerRender: atom.triggerRender,
		});
	}
	public _recordCausal<K, V>(k: S, value: V, source: S, s = this.s, m = this.m, r = this.r, cause?: S): void {
		const atom = this.hub.atom_reg<K, V>(s, m, r).get(k);

		if (!atom) return;

		atom.causal.push({
			key: k,
			value,
			timestamp: Date.now(),
			source,
			cause,
		});

		if (atom.causal.length > 100) {
			atom.causal.shift();
		}
	}
	public _bindComponentToSignal(rk: S, s = this.s, m = this.m, r = this.r): V {
		const component = getCurrentComponent();
		if (!component) return;
		const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
		if (component.hooks.has(hookKey)) {
			return;
		}
		component.hooks.set(hookKey, { value: undefined });

		const atom = this.create(rk, undefined, undefined, s, m, r);
		if (!atom) {
			return;
		}

		const unsubSignal = atom.subscribeExternal(() => {
			notifyHook(hookKey);
		});
		component.unmount.add(unsubSignal);

		if (component.render) {
			const unsubRender = subscribeToHook(hookKey, () => component.render?.());
			component.unmount.add(unsubRender);
		}
	}
	public _resolveConflict<T>(conflict: ConflictPolicy | undefined, previous: T, next: T, strategy: "latest" | "fail" | "accumulate" = "latest"): T | null {
		if (conflict?.resolve) {
			return conflict.resolve(previous, next);
		}

		if (strategy === "fail") {
			console.warn('[Sauron] conflict rejected by "fail" strategy');
			return null;
		}

		if (strategy === "accumulate" && Array.isArray(previous) && Array.isArray(next)) {
			return [...previous, ...next] as T;
		}

		return next;
	}
	private _sysEnsure(rk: S, v: A, s = this.s, m = this.m, r = this.r): SysEntry {
		const sysMap = this.hub.getMetadata(s, m, r)._sys;
		let entry = sysMap.get(rk);
		if (entry) return entry;
		const sig = signal(v);
		entry = { read: () => sig(), write: (v: A) => sig(v), node: sig as A, isComputed: false };
		sysMap.set(rk, entry);
		if (Store.state.get(rk, { s, m, r }) === undefined) {
			Store.state.set(rk, v, { s, m, r, triggerRender: false });
		}
		return entry;
	}
	private _sysRestore(snap: M<S, A>): V {
		for (const [rk, v] of snap) this._sysEnsure(rk, v).write(v);
	}
	private _sysDropComputeds(s = this.s, m = this.m, r = this.r): V {
		for (const [rk, entry] of this.hub.getMetadata(s, m, r)._sys) {
			if (entry.isComputed) this.hub.getMetadata(s, m, r)._sys.delete(rk);
		}
	}
	private _sysSnapshot(s = this.s, m = this.m, r = this.r): M<S, A> {
		const out = new Map<string, A>();
		for (const [rk, entry] of this.hub.getMetadata(s, m, r)._sys) {
			if (!entry.isComputed)
				out.set(
					rk,
					this.RX_S.untrackFn(() => entry.read()),
				);
		}
		return out;
	}
	// #endregion
	//                                                                                                                   SEND / SUB
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public sub<T = A>(k: SignalKey, listener: (value: T, previous: T) => V, s = this.s, m = this.m, r = this.r): () => V {
		const atom = this.create(k, null, undefined, s, m, r) as SIGNAL_CELL;

		if (!atom) {
			throw new Error(`[Sauron] Cannot subscribe to unknown signal "${String(k)}"`);
		}

		return atom.subscribe((change: A) => {
			listener(change.current, change.previous);
		});
	}
	public send = (k: S, p: A, s = this.s, m = this.m, r = this.r): V => {
		const atom = this.hub.atom_reg(s, m, r).get(k);
		atom.write(p);
	};
	public delete = (k: A, s = this.s, m = this.m, r = this.r): V => {
		this.hub.atom_reg(s, m, r).delete(k);
		this.hub.getMetadata(s, m, r)._sys.delete(k);
	};
	// #endregion
	//                                                                                                                        ASYNC
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public createAsync = <T,>(k: A, fetcher: (signal: AbortSignal) => P<T>, s = this.s, m = this.m, r = this.r, options?: { fallback?: T }) => {
		const reg = this.hub.atom_reg(s, m, r);
		let atom = reg.get(k) as SIGNAL_CELL<T> | UD;

		if (!atom) {
			atom = this.create(k, options?.fallback as T, { triggerRender: false }, s, m, r) as SIGNAL_CELL<T>;
		}

		const dataKey = `${String(k)}:data`;
		const loadingKey = `${String(k)}:loading`;
		const errorKey = `${String(k)}:error`;

		if (!this.has(dataKey, s, m, r)) this.create(dataKey, options?.fallback, undefined, s, m, r);
		if (!this.has(loadingKey, s, m, r)) this.create(loadingKey, false, undefined, s, m, r);
		if (!this.has(errorKey, s, m, r)) this.create(errorKey, null, undefined, s, m, r);

		const run = async (): Promise<T> => {
			const state = atom!.async;
			if (!state) throw new Error(`[Sauron] Missing async state for "${String(k)}"`);

			if (state.controller) state.controller.abort();
			const controller = new AbortController();
			state.controller = controller;

			this._writeResolved(loadingKey, true, "write", s, m, r, String(k));

			try {
				const data = await fetcher(controller.signal);
				if (!controller.signal.aborted) {
					this._writeResolved(dataKey, data, "write", s, m, r, String(k));
					this._writeResolved(errorKey, null, "write", s, m, r, String(k));
				}
				return data;
			} catch (error) {
				if (!controller.signal.aborted) {
					this._writeResolved(errorKey, error, "write", s, m, r, String(k));
					atom!.lifecycle?.onError?.(error as Error);
				}
				throw error;
			} finally {
				if (!controller.signal.aborted) {
					this._writeResolved(loadingKey, false, "write", s, m, r, String(k));
					state.controller = null;
				}
			}
		};

		atom.async = { fetcher: run, controller: null, retryCount: 0, staleValue: options?.fallback };
		void run();

		return { dataKey, loadingKey, errorKey };
	};
	public invalidateAsync = <T,>(key: A, s = this.s, m = this.m, r = this.r): V => {
		const reg = this.hub.atom_reg(s, m, r);
		const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
		const state = atom?.async;
		if (!atom || !state) return;

		const dataKey = `${String(key)}:data`;
		const loadingKey = `${String(key)}:loading`;
		const errorKey = `${String(key)}:error`;

		this._writeResolved(loadingKey, true, "write", s, m, r, String(key));
		this._writeResolved(errorKey, null, "write", s, m, r, String(key));

		void Promise.resolve(state.fetcher())
			.then((data) => this._writeResolved(dataKey, data, "write", s, m, r, String(key)))
			.catch((error) => {
				this._writeResolved(errorKey, error, "write", s, m, r, String(key));
				atom.lifecycle?.onError?.(error as Error);
			});
	};
	public mutateAsync = <T,>(key: A, optimistic: T, s = this.s, m = this.m, r = this.r): V => {
		const reg = this.hub.atom_reg(s, m, r);
		const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
		const state = atom?.async;
		if (!atom || !state) return;

		const dataKey = `${String(key)}:data`;
		const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
		if (!dataAtom) throw new Error(`[Sauron] Missing async data signal "${dataKey}"`);

		const prior = dataAtom.peek();
		this._writeResolved(dataKey, optimistic, "write", s, m, r, String(key));

		void Promise.resolve(state.fetcher()).catch(() => {
			this._writeResolved(dataKey, prior, "write", s, m, r, String(key));
		});
	};
	public readAsync = <T,>(key: A, s = this.s, m = this.m, r = this.r): T => {
		const reg = this.hub.atom_reg(s, m, r);
		const dataKey = `${String(key)}:data`;
		const loadingKey = `${String(key)}:loading`;
		const errorKey = `${String(key)}:error`;

		const errorAtom = reg.get(errorKey) as SIGNAL_CELL<A> | UD;
		const error = errorAtom?.peek();
		if (error) throw error;

		const loadingAtom = reg.get(loadingKey) as SIGNAL_CELL<boolean> | UD;
		if (loadingAtom?.peek()) {
			throw new Promise<void>((resolve) => {
				const unsub = loadingAtom.subscribe((change) => {
					if (!change.current) {
						unsub();
						resolve();
					}
				});
			});
		}

		const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
		return dataAtom?.peek() as T;
	};
	// #endregion
	//                                                                                                                   PERSISTANCE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public persist = async <T,>(key: A, value: T, s = this.s, m = this.m, r = this.r, options?: { serialize?: (v: T) => S }) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public unpersist = async (key: A, s = this.s, m = this.m, r = this.r) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public transaction = (block: () => V): V => {
		// REDO WITH NEW ATOM SOLUTION
	};
	// #endregion
	//                                                                                                                     TIMELINE
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public timeline = (() => {
		const fn = () => {}


		return fn;
	})();

	public snapshot = () => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public restore = (snapshot: Record<string, A>): void => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public diffSnapshots = (a: Record<string, A>, b: Record<string, A>) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public applyPatch = (patch: Record<string, { from: A; to: A }>): void => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public signalTimeline = (key: A, s = this.s, m = this.m, r = this.r) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public replay(entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, options?: { from?: number; speed?: number; triggerRender?: boolean }): Promise<V> {
		const { from = 0, speed = 0, triggerRender = true } = options ?? {};

		return new Promise((resolve) => {
			let index = from;

			const step = () => {
				if (index >= entries.length) {
					resolve();
					return;
				}

				const entry = entries[index++];

				this._writeResolved(entry.key, entry.value, entry.source ?? "replay", this.s, this.m, this.r, `replay:${index}`);

				if (speed > 0) {
					setTimeout(step, speed);
				} else {
					step();
				}
			};

			startBatch();

			try {
				step();
			} finally {
				endBatch();
			}

			if (speed === 0) {
				resolve();
			}
		});
	}
	public undo = (key: A, s = this.s, m = this.m, r = this.r): void => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public redo = (key: A, s = this.s, m = this.m, r = this.r): void => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public recording: { entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>; unsubs: Array<() => V> } | null = null;

	public record(keys?: SignalKey[], s: S = this.s, m: S = this.m, r: S = this.r): () => V {
		/** USAGE
		 * const stop = sauron.record();
	
		// ...user reproduces the bug...
		
		const entries = stop();
		
		// Replay it exactly:
		await sauron.replay(entries);
		
		// Or find where reality diverged:
		sauron.bisect(entries);
		 */
		if (this.recording) {			throw new Error("[Sauron] A recording is already active");		}

		const registry = this.hub.atom_reg(s, m, r);
		const targets = keys ?? [...registry.keys()];

		const entries: Array<{			key: SignalKey;			value: A;			previous: A;			timestamp: number;			source?: S;		}> = [];

		const unsubs = targets.map((key) => {
			const atom = registry.get(key) as SIGNAL_CELL | undefined;
			if (!atom) return () => {};

			return atom.subscribe((change) => {
				entries.push({
					key,
					value: change.current,
					previous: change.previous,
					timestamp: Date.now(),
					source: change.source,
				});
			});
		});

		this.recording = { entries, unsubs };

		return () => this.stopRecording();
	}
	public stopRecording(): Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> {
		if (!this.recording) return [];

		for (const unsub of this.recording.unsubs) {
			unsub();
		}

		const entries = this.recording.entries;
		this.recording = null;

		return entries;
	}
	
	




	public getCausalChain(key: A) {
		// REDO WITH NEW ATOM SOLUTION
	}
	public conflictPolicy<T>(key: A, policy: ConflictPolicy): void {
		// REDO WITH NEW ATOM SOLUTION
	}
	public declareContract(name: string, invariant: (snapshot: Record<string, A>) => boolean, key: A, s = this.s, m = this.m, r = this.r, options?: { block?: boolean }): void {
		// REDO WITH NEW ATOM SOLUTION
	}
	public removeContract(name: string): void {
		// REDO WITH NEW ATOM SOLUTION
	}
	public createScope = (id: string) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	public effectOnce = (keys: A[], predicate: (...values: A[]) => boolean, fn: (...values: A[]) => void) => {
		// REDO WITH NEW ATOM SOLUTION
	};
	// #endregion
	//                                                                                                                   SRC / DEST
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public readonly SRC_DEFAULTS: rts = { s: this.s, m: this.m, r: this.r };

	public readonly DST_DEFAULTS: rts = { s: this.s, m: this.m, r: this.r };
	public filter = <T,>(srcKey: A, destKey: A, pred: (v: T) => B, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 return this.subscribe<T>(
			 sk,
			 v => {
				 if (pred(v)) this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public throttle = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 let last = 0;
		 return this.subscribe(
			 sk,
			 v => {
				 const now = Date.now();
				 if (now - last >= m) {
					 last = now;
					 this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
				 }
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public merge = (srcKeys: A[], destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**	const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
	 const unsubs = srcKeys.map(k => {
		 const sk = this.resolveKey(k, src.s, src.m, src.r);
		 return this.subscribe(sk, v => this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r), src.s, src.m, src.r);
	 });
	 return () => unsubs.forEach(u => u()); */
	};
	public zip = (srcKeys: A[], destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**
		 * const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
		const emitted = new Map<string, A>();
		const sks = srcKeys.map(k => this.resolveKey(k, src.s, src.m, src.r));

		const unsubs = sks.map(sk =>
			this.subscribe(
				sk,
				v => {
					emitted.set(sk, v);
					if (sks.every(k => emitted.has(k))) {
						this._writeResolved(
							dk,
							sks.map(k => emitted.get(k)),
							"derived",
							sk,
							dest.s,
							dest.m,
							dest.r,
						);
						emitted.clear();
					}
				},
				src.s,
				src.m,
				src.r,
			),
		);

		return () => unsubs.forEach(u => u());
		 */
	};
	public scan = <T, A>(srcKey: A, destKey: A, reducer: (a: A, v: T) => A, seed: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**
		 * const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);

		let acc = seed;
		this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);

		return this.subscribe<T>(
			(v: T) => {
				acc = reducer(acc, v);
				this._writeResolved(dk, acc, "derived", sk, dest.s, dest.m, dest.r);
			},
			sk,
			src.s,
			src.m,
			src.r,
		);
		 */
	};
	public switch = (srcKey: A, destKey: A, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 let inner: (() => V) | null = null;
		 return this.subscribe(
			 sk,
			 (targetKey: A) => {
				 if (inner) inner();
				 const k = this.resolveKey(targetKey, src.s, src.m, src.r);
				 inner = this.subscribe(k, v => this._writeResolved(dk, v, "derived", k, dest.s, dest.m, dest.r), src.s, src.m, src.r);
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public buffer = (srcKey: A, destKey: A, count: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 const buf: A[] = [];
		 return this.subscribe(
			 sk,
			 v => {
				 buf.push(v);
				 if (buf.length >= count) {
					 this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
					 buf.length = 0;
				 }
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};
	public window = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 const buf: A[] = [];
		 const iv = setInterval(() => {
			 if (buf.length) {
				 this._writeResolved(dk, [...buf], "derived", sk, dest.s, dest.m, dest.r);
				 buf.length = 0;
			 }
		 }, m);
 
		 const unsub = this.subscribe(sk, v => buf.push(v), src.s, src.m, src.r);
		 return () => {
			 unsub();
			 clearInterval(iv);
		 }; */
	};
	public distinct = <T,>(srcKey: A, destKey: A, equals?: (a: T, b: T) => B, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**	const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
	 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);

	 let last: T | UD;
	 return this.subscribe<T>(
		 sk,
		 v => {
			 const same = last !== undefined && (equals ? equals(last, v) : Object.is(last, v));
			 if (!same) {
				 last = v;
				 this._writeResolved(dk, v, "derived", sk, dest.s, dest.m, dest.r);
			 }
		 },
		 src.s,
		 src.m,
		 src.r,
	 ); */
	};
	public audit = (srcKey: A, destKey: A, m: N, src: rts = this.SRC_DEFAULTS, dest: rts = this.DST_DEFAULTS) => {
		/**const sk = this.resolveKey(srcKey, src.s, src.m, src.r);
		 const dk = this.resolveKey(destKey, dest.s, dest.m, dest.r);
 
		 let timer: RET<typeof setTimeout> | null = null;
		 let latest: A;
		 return this.subscribe(
			 sk,
			 v => {
				 latest = v;
				 if (!timer) {
					 timer = setTimeout(() => {
						 this._writeResolved(dk, latest, "derived", sk, dest.s, dest.m, dest.r);
						 timer = null;
					 }, m);
				 }
			 },
			 src.s,
			 src.m,
			 src.r,
		 ); */
	};

	// #endregion
	//                                                                                                                    CONSTANTS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	public _sauronIdCounter = 0;
	public _auth_initialized = false;
	public readonly QUEUE_KEY = "staff:sends:offline:queue";
	public deadLetters: SIG_BUS_Message[] = [];
	public _schemas = new Map<S, SIG_BUS_Schema>();
	public defaultPriority: A = "normal"; // CSEPriority
	public middleware: ((msg: SIG_BUS_Message) => B | V)[] = [];
	public groups = new Map<S, { channels: Set<S>; enabled: B; middleware: ((msg: SIG_BUS_Message) => B | V)[] }>();
	public queue: SIG_BUS_Message[] = [];
	public seen = new Set<S>();
	public globalMiddleware: { beforeSend: ((msg: SIG_BUS_Message) => B | V | P<B | V>)[]; afterSend: ((msg: SIG_BUS_Message, result: U) => V | P<V>)[]; beforeReceive: ((msg: SIG_BUS_Message) => B | V | P<B | V>)[]; afterReceive: ((msg: SIG_BUS_Message, result: U) => V | P<V>)[]; onError: ((err: Error, msg?: SIG_BUS_Message) => V | P<V>)[] } = { beforeSend: [], afterSend: [], beforeReceive: [], afterReceive: [], onError: [] };
	// #endregion
}
export const signals = new SIGNALS()
// #endregion 
//                                                                                                                       SIGNAL_CELL
// #region ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class SIGNAL_CELL<T = A> {
	public initial: T;
	public key: SignalKey;
	public s: S
	public m: S
	public r: S;
	public node: Signal<T>;// The one canonical value source. signals owns value storage, dependency tracking, computed propagation, effects, and batching.

	public globalListeners = new Set<(key: SignalKey, s: S, m: S, r: S, value: U) => V>();
	public listeners = new Set<(change: Change<T>) => void>(); // Platform/manual subscriptions: state.subscribe(key, callback)
	public externalSubscribers = new Set<() => void>();// Component/hook/render bridge subscriptions: signal read in a component → notifyHook(hookKey)
	public waiters = new Set<(value: A) => void>();// Await-next-write subscriptions.

	public conflict?: ConflictPolicy;
	public conflictStrategy?: "latest" | "fail" | "accumulate";

	public version = 0;
	public undoStack?: HistoryEntry[];
	public historyIndex = 0;
	public history?: AtomHistoryEntry<T>[];
	public historyMax = 50;
	// Index of the currently materialized snapshot. `0` is always the initial snapshot when history is enabled.
	public persisted = false;
	public hasWritten = false;
	public lifecycle?: SignalLifecycle;
	public async?: AsyncSignalState<T>;
	public metrics?: Metrics;

	public sealed = false;
	public frozen = false;

	public schema?: SignalSchema;
	public contracts = new Map<string, AtomContract<T>>();
	public causal: CausalEntry[] = [];
	public lastWriteSource?: string;

	public batches = new Set<SignalKey>();
	public batchQueue = new Map<SignalKey, unknown>();
	public batchOpts = new Map<SignalKey, { triggerRender?: boolean }>();

	public triggerRender = true
	public source? = 'signals'

	public boundary: Boundary = 'same-app'

	constructor(key: SignalKey, initial: T, s:S, m:S, r:S,  options?: SignalOptions) {
		this.key = key;
		this.s = s;
		this.m = m;
		this.r = r;
		this.initial = initial;
		

		this.node = signals.signal(initial) as Signal<T>;
		if (options?.source)this.source = options?.source
		if (options?.schema) this.schema = options.schema;
		if (options?.lifecycle) this.lifecycle = options.lifecycle;
		if (options?.conflict) this.conflict = options.conflict;
		if (options?.conflictStrategy) this.conflictStrategy = options.conflictStrategy;
		if (options?.triggerRender !== undefined) { this.triggerRender = options.triggerRender; }

		if (options?.history) {
			this.historyMax = Math.max(1, options.history.max ?? 50);
			this.history = [{ k: 0, value: initial, timestamp: Date.now(), source: "initial", }];
			this.historyIndex = 0;
		}
	}
	public read(): T {
		if (signals.activeTracer) {
			signals.activeTracer.set(this.key, this);
		}
		return this.node();
	}
	public peek(): T { 
		return this.node.peek?.() ?? this.node();
	}
	public write(incoming: T | ((previous: T) => T), options: AtomWriteOptions = {}): T {
		if (this.frozen) return this.peek();

		const previous = this.peek();
		let next = typeof incoming === "function" ? (incoming as (previous: T) => T)(previous) : incoming;

		next = this.validate(next);

		if (!Object.is(previous, next) && this.conflict) {
			const resolved = signals._resolveConflict(this.conflict, previous, next, this.conflictStrategy,);
			if (resolved === null) return previous;
			next = resolved;
		}

		this.assertContracts(next, previous);
		if (Object.is(previous, next)) return previous;

		if (activeTx && !activeTx.has(this.key)) {
			root.SIGNALS.activeTx.set(this.key, {
				cell: this,
				previous,
			});
		}

		this.hasWritten = true;
		this.node(next);
		this.version += 1;
		this.lastWriteSource = options.source ?? "write";

		if (options.recordHistory !== false) { this.recordHistory(previous, next, this.lastWriteSource); }

		this.recordCausal(next, this.lastWriteSource, options.cause);
		this.recordMetrics();
		this.lifecycle?.onWrite?.(next, previous);

		this.resolveWaiters(next);

		if (options.triggerRender ?? this.triggerRender) {
			this.notifyExternalSubscribers();
			this.notifyListeners({ key: this.key, previous, current: next, version: this.version, timestamp: Date.now(), source: this.lastWriteSource, });
		}
		const timeline  = new Timeline()
        timeline.record({
            scopeId: getScope()?.id ?? null,
            key: this.key,
            previous,
            next,
            source: this.lastWriteSource!,
            cause: options.cause,
            timestamp: Date.now(),
            txId: activeTxId,
          });
         

		return next;
	}
	public setSchema(schema?: SignalSchema) {
		this.schema = schema;
		return this;
	}
	public setLifecycle(lifecycle?: SignalLifecycle<T>) {
		this.lifecycle = lifecycle;
		return this;
	}
	public setConflict(
		conflict?: ConflictPolicy,
		strategy?: "latest" | "fail" | "accumulate",
	) {
		this.conflict = conflict;
		this.conflictStrategy = strategy;
		return this;
	}
	public setFrozen(): void {
		this.frozen = true
	}
	public setUnfrozen(): void {
		this.frozen = false
	}
	public setSealed(): void {
		this.sealed = true
	}
	public setUnsealed(): void {
		this.sealed = false
	}
	public setBoundary(v: S) {
		this.boundary = v
	}
	private validate(next: T): T {
		if (!this.schema) {
			return next;
		}

		return signals._validateSchema(this.schema, next);
	}
	private assertContracts(next: T, previous: T) {
		for (const contract of this.contracts.values()) {
			let valid = false;

			try {
				valid = contract.postcondition(next, previous);
			} catch (error) {
				console.error(
					`[Sauron] contract "${contract.name}" threw for "${String(this.key)}":`,
					error,
				);
			}

			if (valid) continue;

			const message =
				`[Sauron] contract "${contract.name}" violated ` +
				`on write to "${String(this.key)}"`;

			if (contract.block) {
				throw new Error(message);
			}

			console.error(message);
		}
	}
	private recordHistory(previous: T, next: T, source?: string) {
		if (!this.history) return;

		this.history.splice(this.historyIndex + 1);

		const last = this.history[this.history.length - 1];
		this.history.push({
			k: (last?.k ?? -1) + 1,
			prev: previous,
			value: next, 
			next,
			timestamp: Date.now(),
			source,
			undone: false,
		});

		while (this.history.length > this.historyMax) {
			this.history.shift();
		}

		this.historyIndex = this.history.length - 1;
	}
	public getHistory(): readonly AtomHistoryEntry<T>[] { return this.history ?? []; }
	public timeTravel(steps = 1): T | undefined {
		if (!this.history?.length || steps <= 0) return undefined;
		const idx = Math.max(0, Math.min(this.history.length - 1, this.historyIndex + steps,));
		if (idx === this.historyIndex) return undefined;
		this.historyIndex = idx;
		return this.replayHistory("time-travel");
	}
	public back(steps = 1): T | undefined { return this.timeTravel(-Math.abs(steps)); }
	public forward(steps = 1): T | undefined { return this.timeTravel(Math.abs(steps)); }
	public goto(sequence: number): T | undefined {
		if (!this.history?.length) return undefined;
		const idx = this.history.findIndex(entry => entry.k === sequence);
		if (idx < 0 || idx === this.historyIndex) return undefined;
		this.historyIndex = idx;
		return this.replayHistory("time-travel.goto");
	}
	private replayHistory(source: string): T {
		const entry = this.history![this.historyIndex];
		return this.write(entry.value, {
			source,
			cause: "time-travel",
			triggerRender: true,
			recordHistory: false,
		});
	}
	private recordCausal(value: T, source?: string, cause?: string,) {
		this.causal.push({ value, source, cause, timestamp: Date.now(), } as CausalEntry);
		if (this.causal.length > 100) { this.causal.shift(); }
	}
	private recordMetrics() {
		if (!this.metrics) return;

		this.metrics.writeCount += 1;
		this.metrics.lastWrite = Date.now();
	}
	public subscribe(listener: (change: Change<T>) => void): Unsubscribe {
		this.listeners.add(listener);
		return () => { this.listeners.delete(listener); };
	}
	public subscribeExternal(callback: () => void): Unsubscribe {
		this.externalSubscribers.add(callback);
		return () => { this.externalSubscribers.delete(callback); };
	}
	public waitForNext(): Promise<T> {
		return new Promise<T>(resolve => { this.waiters.add(resolve); });
	}
	private notifyListeners(change: Change<T>) {
		for (const listener of this.listeners) {
			try { listener(change); }
			catch (error) { console.error(`[Sauron:SIGNAL_CELL:${String(this.key)}] listener failed:`, error); }
		}
	}
	private resolveWaiters(value: T) {
		if (this.waiters.size === 0) return;
		const waiting = this.waiters;
		this.waiters = new Set();
		for (const resolve of waiting) {
			try { resolve(value); }
			catch (error) { console.error(`[Sauron:SIGNAL_CELL:${String(this.key)}] waiter failed:`, error); }
		}
	}
	private notifyExternalSubscribers() {
		for (const callback of this.externalSubscribers) {
			try { callback(); }
			catch (error) { console.error(`[Sauron:SIGNAL_CELL:${String(this.key)}] external subscriber failed:`, error); }
		}
	}
}
// #endregion
//                                                                                                                         [SIGNALS]
// #region ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
/* =================================================================================================================================
sauron.surgery('cart:items', (items) => {
	items.push({ id: 'test', price: 10 });
});

sauron.surgeryAt('user', ['profile', 'email'], (email) => email.trim());

const restore = sauron.surgeryCheckpoint();
	
sauron.surgery('cart:total', () => 0);
sauron.surgery('cart:items', (items) => []);

restore(); // both surgeries undone, graph settles once per undo
==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DRS] */
export function gAtom<T>(k: SignalKey, s? :S, m ?:S, r?:S,) {
	const ss = s ?? Store.get('SAURON_SLAVE_REG')
	const mm = m ?? Store.get('SAURON_MASTER_REG')
	const rr = r ?? Store.get('SAURON_SLAVE_REG')
    const registry = signals.hub.atom_reg(ss, mm, rr,);
    return registry.get(k) as SIGNAL_CELL<T> | undefined;
}
export function getAtom<T>(k: SignalKey, v?: T | (() => T), options?: SignalOptions, s? :S, m ?:S, r?:S,): SIGNAL_CELL<T> | undefined {
	const ss = s ?? Store.get('SAURON_SLAVE_REG')
	const mm = m ?? Store.get('SAURON_MASTER_REG')
	const rr = r ?? Store.get('SAURON_SLAVE_REG')
    const registry = signals.hub.atom_reg(ss, mm, rr,);
    let atom = registry.get(k) as SIGNAL_CELL<T> | undefined;

    if (atom || v === undefined) { return atom; }

    const initialValue = typeof v === "function" ? (v as () => T)() : v;
    atom = new SIGNAL_CELL(k, initialValue,ss!,mm!,rr!, options,);
    registry.set(k, atom);
    return atom;
}
export const createAtom = getAtom
export function subAtom<T = A>(label: string, fn: (value: T) => void, s?: A, m?: A, r?: A) { }   // return emitter.sub(fn, label, s, m, r)
export function editAtom() {

}
export function hasAtom() {

}
export function deleteAtom() {

}
// #endregion 
/*  =================================================================================================================================
	 all atom solutoin.... evne tho so many are so drastically differnt they all end up dealing with the sma eissues... like mine for exmaple.. as aweosme as it is right now... ok...

	 i forgot ihavent even told u what it actually
	 its the fastest... most complext atom solution tha twll be built for a veryt long time.... this is done because of a for carefully selected and designed decisions that were not rushed...
	 the fast path.... is the deafult apth... thats the only way you can get bvoth fast and confugrally complex....
	 but this is where it starts to veer from normtal aom solution.... already.. there is 10 master atoms that a user can choose form, the deiciosn comes down to what ur storing exactly... the fast path actually performs beqtter.... running hot... like fucking literlly.. because the fast path instead of a simple signals ... class that holds a value... each atom that get screated via
	 const value = atom(3)
	 THATS LITERLLY.. IT..... but anyways.... each atom, has its own unqieu disruptor as its storage medium... where in the users usage path.... i was getting 750 mill ops/sec.... after that i pushed it even further but its not the user path anymore but i got i tup to 950 and then i hit 900 on the other disruptor that i have....
	 master atoms only contain 2 values
	 just fucking 2... that is it....
	 the keys, and the value of the atom itself... thats the fast path... there are other amaster atoms where thats just no feasable.... whihc is whgy they ainty the fastest... now this is where we could get by... because this is where everything and all atom soltiunos deal with this.. but there are 100 configurable atoms that can be bound to a master atom... now.... typcially... that means you now have to have  afactroy... you have to have some way of working with mthe so thats noather palce for cod eper atom to live... so we are now sitting at two... the atoms itself 3 and any helpre function so r component required for said atoms functionality so four at a very minumiin.. ... how the fuck do u make this... so its not a giant fucking bag of dog shit.... becasuse with 4+ plus places... it becomes brittel with many break points that can occur since every spit u place code...one more place for it to fail...huge fucking msess... then u got toi store these fucking thhings... master atoms and its bound atom..... whihc ppl usinly reach for a graph.... but thats for the poors... because its slow and gay.....

	 my genius so so fucking colassaly large.... so dense... and just... never stops vibrating due its raw unpotinatoinl power.... it generates gravity... it trruly does....

	 how about no?.... to all of the above... the code for each atom..... lives ina single spot ... insxide its atom, fullstop... no where else... no joke...no regirsty atoms... to mark which master atom.... has what... and then storing those fucking things... which brings me ot my next point..... no storage.. or orginazastion .... of literllty.. AND I FUCKING MEAN THIS... of any kind... so no maps... no nested maps... no helper look up functions.... none of that shit... we are essetnailly.... going to create it for the user... and aiming off in arandom directino each and ervery single time..... fucking yeet it as ahard as u can and hopefully pegs some poors in the head or something....

	 im serious.....no storage.... no tracking of any kind... ok?
	 esesntially that leave sus with...all we reall yhave to build is the contract builder... which its the helpre fucniton for the users to create contracts....so they can create a reuablefucntion to call repeadlty to create custom atoms...

	 thats it..... and before you ask theres something missing here.... like the entire fucking solution... no... thats tthe best part... its not missing... i just dont need it .....

	 trust me.... I'm a fucking wizard....

	 taking, and i awil buidl first class primtive for this, but taking my distrbruted reactive state solution.... and essentially... turning it inside out....for elegantly... each atom and amster atom... only requires 2 lines of code.. when a atom is bound to a master atom, it only needs the master atoms address... so that it can palce itself on the same distrbuted state graph or whatever... channel who the fuck cares... thus.... turning a setter and gettr hook... into basically.... what is a 100 person conference calll... seriously... the master atom whenerv eri it wants somenthing done... it send out a trigger over the state... each atom gets triggered and checks it to see if they are apart of that rounds triggered event... if they are they then go do their thing, return a valuye by mergigin it into the state with their own id so the maste rcan be like ok... fuck  face atom 89 did a succesful exetnion of whatever... obviously not everything needs a rteturn vvalue but if it does... then once the master sees it then only the amster can remove values from the state... and as a default the state of each atom  is bacailly going to be like....a viable registry for th euser to look at, giving the user a hsitoryy of all action sdodnewith that atom.....so we basilly... alt delted... 80% of the ops that would other wixe need to be required to do eeach and every single time someone touch an atom in any capacity.... and at the same time... it completly drops... A SHIT TON of issues that plague every single atoms solution... becuase we simply... jsut do not have the archtecutre thre to give us the problems in the frist place.. LOL... so before... my atroms solution already outperfromaned everyone by massive margin... this just shits on aevreyone becasue once ur in disruptor areas of perfmance... u need to treat every signel op... like its the literlly....last drop of water u will ever have... and this sessentiallyliterlly deleted 80% of the required aerchtecvture that everyopne deals with.... 

	 the distrubted state and this is where we get our dick impant... becasuye right now our dick sare huge... but tthey are about to get even bigger...
	 the distruibted state... at first i was gonng to sue signals as a transport protocol ... but a custom one so it didnt create an atom like it does now with the current transport adapter... but then i though..... what do we usually do when we want to gofucking all ball sout... and jkust fucking hammer it... lets take our fasteest disruptor.... and that is what the signals soltuiion will run off in place of the typcial sinngals mediunm... IN ADDITOIN TO THAT..... if we look at everyhting right now... take a step back .... really look at it... like fuck man... if this were a race... everyone would be you ... know 5 miles into the race... meanwhile.. we are looking back at them..... from fucking pluto....CAN WE TAKE THIS FUCKING FARTHER THOUGH? like really.... FUCK YAAAA.....the entiresoltuioin.... every single map, every singlt set... every single boolean... gone... fucking hgone... ok/?.... avor encoder....cbor as a back up... meanwhile... everything is converted...  to byte op code... no json encoders or anything.....


	 no ... the master does not know... who is bound to it... the only palce where there is a record of that is the contrac titself.... the master atom doesnt evne have a way to check...  and the contract is also how th euser aactiastes functionality... so just calling th econtract via a straight call.. creates a an atom whatever it is that they defined.. and then nested within it... are the functions that deal with the bound atoms.... so really... the master atom couldnt not be any fucking dumber... which is hoenstly... a  great thing to be frank... like actually...

	 noj the contrac t is not the registry... u create amaster atom... adn then say u need to bind 6 other atoms to it... yuou create them and simply just pass the masters addres to it.... and then fucking yeet it... in whatever direciotn u  want

	 this ended up being so fucking elegant .... like u must admit... my fucking genius... is like it truly generates gravity........

	 dude... llol... that is such a n easy fucing thing to do... if u fuck that up... im seriously never fucking using u again... like i literly deltedd 90% of the soluitioun.... more then that really.... 
	 so i already have the disruptro backend... 95% built in preprartion for this day.... the master atoms... bovioiusly wil need some ediitng.... i just simply need to change a couple of things to hook the signals soltuioiiun to th edisrupotr.... i already have the drs primtimves... that i m going to copy over... and then adjust for the new system... becasue thre is going to be 2 paths.... for the distrubted state... and this gets even wilder.. ones disruptro... and ones actual signals... so super fast.....BUT.... the state is going to be able to be shared.... among node modules... so if we have 4  entirely differnt nod e modules... typcailly a pain in the fucking ass to cordinate anything between them.. can now all hook up to the same stateand signals solutoin basically...  and ill builkd amini solution that connects on the eother end... the db aspect... becasue dude this ssoltuion is fucking wild...is virtuall ydone... also waiting for this day.... its actually dbs going into it LOL.....AND THEN ... the its final resting place.... is going to be beside JEDI, which is one of the buses in platformas bus mesh... where it si goiing to be called SITH... because the disruptor is named the power of two.. and it fits that its going after raw power... and as it takes tha tplace... it will also hgave  a bus... that is the coutner part to jedi... jedi is extremly feature rich... obivosuly slwoer because o fit.... sith.. is going to mirrror the api... but with zero fucking features..... AND the distribted spreadsheet engine.... will move OFF OF jedi... to come over to live on sith... because the distribted spreadsheet engine deals with mostly..... numbers... and who fiends over numbers ?.... disruptors.... so the disruptro wil 100% pwoer the spreadsheet engine.... AND.....it will also provide the mesh.... the disruptor transport adapter... that it has been waiting for.,..... so 3 dbs... a signals solution..and atoms solutoin... a spread sheet engine... tranport aeddapters... OH.... so this itnhg is not only get=ting a disruptor per master atom... but each op..... that needs to happen... is getting its OWN disrupotor ND its own QUEUE.... SO instead of doing 15 ops passes bakc to back to back... thats reall yinnefecint... instead its going to be a single pass....15 wide... so while there is not asingle disrupotr in any other fucking platform or atoms oslution.. and if we inlcude the same node, differenty app trapsnort adapter... this will have at a minimin at all times.. 17 disrtuptors... not inclduing any that get created via a fast pass atom......

	 oh and the platforms reactivity / epoch clock... is also getting converted over... man see i told u my fucking genius....... gravity.... heavy geavity.... so the the epoch clock... deleted... instead there wil be a tracer particle... that is going to be placed with the data on the disrutptor.... there is no deinfed constatn.... in relation to the clocks counter... because.... that counter... actually travel swith the particle round iteself since it can hold its own value.... so each revolution it ++s one.... now this dletes all ops costs  and A SHIT TON ... of problems where a wall clock or epohch suusaly has to deal with.... we also get free ..... what sth ewor dim looking for.... essentially if the node experoiiecnes choking or perofmrance dip or all the resources basically get used up....because the tracer particle is trableing WITH THE DATA.... the clock itself.. also slows down... so its never out of sync.... now this also unlcoks a redicklous amoount of other free features... ie... there will also be a 7 day roating state.. where on the 7th day it writes over tiself... the only issue is... at 750 million oops per second..... how th efuck do u define a day? u cant... which.... actually ends up being a massive plus... if its a hotter app... it jsutsimply has a faste r7 dayt like roration... and ther e will be a dial to increase the amounts or decrease... but data...or state.. enver goes stale... whenerve rthe disruptor goes to sleep... the tracer.... ALSO goes to sleep... so the when the clock wakes up... everyithng around it is literlly... as it lerft it l.....s o nothing is stale... and it comes iwth a shit ton o fother abenefits tooo

	 dont worry aobut th disurptor....  i just dleted and toook care of 99% of the soluition... ur fucking job.... ok?.... ur ONE FUCKING JOB.....
	 is the contract....ok?
	 u dont have to worry about fucking where to stroe shit... as thats alrleady taken care of...
	 
	 here is the shape that it needs to end up in
	 
	 export const counter = atom_contract(({ self }: any) => ({
	 ttl: { ms: 60_000 },
	 history: true,
	 snapshot: true,
	 }));
	 
	 SO IM EVEN TAKING THE THOUGHT PROCEESS OF THAT PART AWAY FROM U...  so AGAIN
	 
	 U HAVE ONE FUCKING JOB... ONE FUCKING JOB... DO NOT WORRY ABOUT THE DFISURPOTORS DO NOT FUCKING WORRY ABOUTR THE DISTRUBRTED STATE.... IT IS SO FAR FROM U .... U MIGHT AS WELL BE IN A DIFFEREN TUNIVERSE.... OK?
	 ok... u have to fucking create it.... so that we can add the functionality of the bindable atoms... onto it.... id ont see that here... i dont see much of fucking anything really... THERE ARE 12 MASTER ATOMS.... u didnt cod ein a place... fo rth euser to fucking choose one... and if i can recyucle.... previous attmteps and re use the bidnable atoms.... i already have 80+ of them.... 
U HAV EONE FUCKING JOB.....DUDE... STOP... FUCKIING AROUND.....
	==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DRS] */


