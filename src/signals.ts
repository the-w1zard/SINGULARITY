/*  ===========================================================================================================================
	 Welcome to the dark side...
     The counterpart to the traditional portion of the platform, where the light side gives you features... this does not.
     But... There is something that the dark side has, that the light does not... True, raw... power. Literally as much as we
     can acquire.
     The signals solution bench comes in at being 40-1100x more performant than both preact AND alien...
     The atoms solution... that will be a bit harder to bench because, no other atoms solution even advertises any sort
     of performance metrics... across the board really. But I will install a few and bench them myself, so that is to come.
     Other bench tests that will also have to be entirely done with zero help... 
     Message queue / bus... I don't think this one is possible... because a bench test is not fair, unless all solutions
     in question are ran on the same machine. I honestly don't know of any solutions that you do not have to pay for sooo, this
     one is out.
     Application state / context, this one will be easy enough. Just download and set up a very basic version of a couple
     of platforms and run the same test on each.
     All other solution I don't think... I can bench either due to industry availability... BUT that doesn't mean I will
     not put everything on a bench anyways! So in total we have...
     - signals: 40-1100x more performant than both preact AND alien...
     - atoms: hitting 751 million ops per sec in the users usage path...
     - app context / state
     - hooks
     - message queue / bus
     - slab
     - distributed reactive state
     - spreadsheet engine
     - same node / same app transport x3
     - same node / different app transport 
     - anti message queue
     Now the reason why this is the dark side is because, each and every single solution... is entirely backed by disruptors.
     Currently, I believe that there is 6 at the time of writing with an asterisk, because the atoms solution that number is
     entirely dependant on user usage because there are disruptors available there too.

     Now there will be one slow bit of code that could not be entirely converted, due to its use being used by several other 
     parts of the application. Which is the portion of how the buses actually end up becoming a mesh. Each bus does not store
     values within its own class / domain. Instead the message queues access a cross-node module instance where most back end
     fns and the storage of all subscribers and other similar values are placed so that each bus has access to them. 

     This portion is still optimized, just not at disruptor speed — it operates at 105M ops/sec, which is the same rate my M2 hits. 
     The rest of the mesh paths run at 400-600M ops/sec, above that line.
     I am looking into ways of drastically improving that number, but when you operate at these numbers and have nothing 
     to go off of, its just a fucking roll of the dice. One little implementation detail could mean the difference between 4 mill 
     and 900 mill. The best disruptor I have built to date, started life as a 400k ops per sec disruptor and currently sits at
     1.2 billion(2.1 bill on a friends newer machine), same archtecture as when it was first built.
     OTHER THAN THAT ONE SMALL PORTION OF CODE, everything else was entirely redone... even the reactive state portion ie the 
     cloud... kinda, curious to see if I was able to code it and have it working first shot... I doubt it.
    ============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE DARK SIDE] */
//                                                                                                                      IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { Registry } from "@the-w1zard/the-registry";
// import { Mesh } from "@the-w1zard/the-mesh"
// import { darkState, useEffect } from "@the-w1zard/the-dark-side"
import { queue, Queue } from "./comps/queue";
import { M2 } from "#dx";
import { parseFormula, removeDependencies, registerDependencies, evaluateSpreadsheetAst, writeGraphCell, collectAffected, recalculateCell, recalculateAffected, createEvalContext, isFormula, normalizeCellRef, parseCellRef, cloneGraph, createSpreadsheetGraph, round, gcd, combin, looseEqual, compare, approxMatch, wildcardMatch, matchCriteria, stddev, variance, correl, covariance, linreg, normalPDF, normalCDF, normalInv, dateSerial, fromSerial, networkDays, workday, dateDif, toRoman, formatText, evaluate, toNum, toStr, toBool, isErr, looseEq, cellKey, resolveCell, expandRange, flattenRange, colToIndex, indexToCol, parseRangeStr, tokenize, Parser, SpreadsheetMode, SpreadsheetChain, SpreadsheetRaw, SpreadsheetCell, SpreadsheetGraphState, SpreadsheetCellRef, qualifyAst, clearGraphCellInPlace, writeGraphCellInPlace, isVolatileAst, recalculateDirty, RangeBox, RangeEntry, boxContains, analyzeDependencies, collectDependencies, } from './comps/cell';
//import { M2 } from "@the-w1zard/dx";


// #endregion
//                                                                                                                       TYPPES
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface CustomCodecResult {
	type: string;
	value: unknown; // must already be JSON-safe / already encoded via encodeChild
  }
  export type CustomEncoder = (  value: unknown,  encodeChild: (v: unknown) => unknown) => CustomCodecResult | typeof SKIP;
  export type CustomDecoder = (  type: string,  value: unknown,  decodeChild: (v: unknown) => unknown) => unknown | typeof SKIP;
  export interface SerFormatConfig {
	date?: boolean;
	bigint?: boolean;
	regexp?: boolean;
	error?: boolean;
	specialNumber?: boolean; // NaN / Infinity / -Infinity
	undefined?: boolean;
	typedArray?: boolean;    // Uint8Array etc + ArrayBuffer
	url?: boolean;
	map?: boolean;
	set?: boolean;
	array?: boolean;
	object?: boolean;
	string?: boolean;
	number?: boolean;
	boolean?: boolean;
  }
export interface GATE_ACCESS {
    /** Reads the cell's GATE_SHARD if one is bound. Undefined = bare fast path = always open. */
    gateOf(cell: ATOM | null | UD): typeof GATE | UD;
    isSealed(cell: ATOM | null | UD): B;
    isFrozen(cell: ATOM | null | UD): B;
}
export function makeGateAccess(): GATE_ACCESS {
    return {
        gateOf(cell) {
            return cell?.read();
        },

        isSealed(cell) {
            return cell?.read()?.sealed ?? false;
        },

        isFrozen(cell) {
            return cell?.read()?.frozen ?? false;
        },
    };
}
export interface DEADLETTER_WIRE {
    enqueue(msg: CSEMessage): Promise<void> | void;
}
export interface DESTRUCTION_RECORD {
    key: A;
    cause: S;
    at: N;
    reviver?: () => A;
}
export interface BackpressureConfig {
    /** Max items queued before SHOULD_BACKPRESSURE() fires. 0/undefined = never. */
    maxQueueSize?: N;
    /** Cooldown between drain attempts once throttling starts. */
    throttleMs?: N;
    /** Coalescing window (ms) before a staged group flushes even if size not reached. */
    batchWaitMs?: N;
    /** Hard deadline (ms) from FIRST item staged — flushes the group no matter what. */
    batchMaxWaitMs?: N;
    /** Items per coalesced group. */
    batchSize?: N;
    /** Default per-tier concurrency if a tier registers without options. */
    defaultConcurrency?: N;
    /** Default retry / timeout for tier tasks. */
    retry?: N;
    timeout?: N;
}
type COALESCED_GROUP = {
    items: Array<{ msg: CSEMessage; resolve: (v: A) => void; reject: (e: A) => void }>;
    flushed: B;
};
export interface CONTRACT_PROTON {
	name: S;
	precondition?: (value: A) => boolean;
	postcondition?: (value: A) => boolean;
	invariant?: (snapshot: Record<S, A>) => boolean;
	block?: B;
}
export interface CONTRACT_VIOLATION {
	contract: S;
	phase: 'pre' | 'post' | 'invariant';
	value?: A;
	message: S;
	ts: N;
}
export interface RiftRecord<T = A> {
    opened: B;
    claimed: B;
    value?: T;
    createdAt: N;
    openedAt?: N;
}
type StoreListener<T = A> = (v: RiftRecord<T>) => void;
export interface TTL_ENTRY { expiresAt: N; data?: A; }

export interface OFFSEC_AUDIT_ENTRY {
    at: N;
    what: S;
    channel?: S;
}
export interface SNAPSHOT_ENTRY<W = A> {
    version: N;
    value: W;
    ts: N;
    label?: S;
}
export type S_CLASS_VALUE = Function; // anything non-serializable lives here
type MDV = S_CLASS_VALUE;

interface MODULE_SLOT {
    value: S_CLASS_VALUE;
    registeredAt: N;
    owner: OWNERSHIP;
    /** Optional hook fired right before the slot is replaced or removed. */
    onUnload?: (value: S_CLASS_VALUE) => V;
}
interface MODULE_OPTIONS {
	module_type?: 'class_fn_pool' | "fn" | 'comp' | 'node_module' ;
	schema?: SignalSchema;
	conflict?: ConflictPolicy;
	history?: { max: N };
	lifecycle?: SignalLifecycle;
	ttl?: N;
	broadcast?: B;
	conflictStrategy?: "latest" | "fail" | "accumulate";
	frozen?: B;
}
export type MODULE_VALUE = Function; // anything non-serializable lives here
interface MODULE_SLOT {
    value: MODULE_VALUE;
    registeredAt: number;
    owner: OWNERSHIP;
    /** Optional hook fired right before the slot is replaced or removed. */
    onUnload?: (value: MODULE_VALUE) => void;
}
type CBORValue = undefined | null | boolean | number | string | Uint8Array | CBORValue[] | { [k: string]: CBORValue }
type AVROField = { name: S; type: AVROSchema };
type AVROSchema = S | AVROSchema[] | {
		type: S | AVROSchema[];
		name?: S;
		namespace?: S;
		fields?: AVROField[];
		items?: AVROSchema;
		values?: AVROSchema;
		symbols?: AR<S>;
		size?: N;
};
type AVROValue = any;
type SIG_BUS_Message<P = A> = {
	id: S;
	r: S;
	m: S;
	s: S;
	k: S;
	p: { p: P };
	ts: N;
	_ack: B;
  
	/** Correlates an async reply back to the awaiting send() call. */
	replyTo?: S;
  
	/** Marks this message as part of a stream. */
	stream?: B;
  
	/** Marks the final stream frame. */
	end?: B;
  
	error?: {
	  message: S;
	  code?: S;
	};
  };
  type Handler = (payload: unknown) => unknown | Promise<unknown>;
  type Slot<P> = {
	sequence: number;
	key: unknown;
	payload: P | undefined;
	tombstone: boolean;
  };
  type hdr<P = A, R = A> = (
	msg: SIG_BUS_Message<P>,
	reply: (value: R | Promise<R>) => V,
  ) => A;
  type PendingEntry = {
	resolve: (value: A) => V;
	reject: (error: A) => V;
	timer?: A;
	stream?: B;
  };
  type Pid = number;
  type Lid = number;
  type RenderFn = () => void;
  type CleanupFn = () => void;
  type EffectFn = () => void | CleanupFn;
  type Scope = { r: S; m: S; s: S; k: S };
  type HandlerSet = Set<BusHandler>;
  interface Deferred {
	resolve: (value: unknown) => void;
	reject: (reason?: unknown) => void;
  }
  interface HADRON_COLLIDER_OPTIONS {
	ringCapacity?: N;
	slotCapacity?: N;
	pidCapacity?: N;
	maxBatch?: N;
	soaCapacity?: N;
	f64Capacity?: N;
	u32Capacity?: N;
	i32Capacity?: N;
	byteCapacity?: N;
	shared?: A;
	capacity?: A;
  }
  interface IndexEntry {
	lane: number;
	seq: number;
  }
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
  interface BusMessage<P = A> {
	id: string; // unique message id
	cid?: string; // correlation id: request id being answered
	r: S;
	m: S;
	s: S;
	k: S;
	p: P; // payload
	from?: S; // sender identity, useful for echo suppression
	replyTo?: string; // reply channel key
	ts: number;
  }
  interface BusHandler<P = A, R = A> {
	(msg: BusMessage<P>): R | Promise<R> | void;
  }
  const NO_SUB = -1;
  function pow2(value: number): boolean {
	return value > 0 && (value & (value - 1)) === 0;
  }
  const enum RKind {
	NONE = 0,
	SIGNAL = 1,
	COMPUTED = 2,
	EFFECT = 3,
	EFFECT_SCOPE = 4,
  }
  const TRACER_PID = 0;
  export const OP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	COMPACT: 4,
	NONE: 5,
	EDIT: 6,
	COMPUTED: 7,
	EFFECTSCOPE: 8,
	EXTERNAL: 9,
	OBSERVER: 10,
	ASYNCEFFECT: 11,
	SEND: 12,
	SUB: 13,
	DUPLEX: 14,
	STREAM: 15,
	DRC: 16,
  } as const;
  const RX_Flags = {
	NONE: 0,
	SIGNAL: 1 << 0,
	COMPUTED: 1 << 1,
	EFFECT: 1 << 2,
	DIRTY: 1 << 3,
	RUNNING: 1 << 4,
	DISPOSED: 1 << 5,
	MUTABLE: 1 << 6,
	WATCHING: 1 << 7,
	RECURSEDCHECK: 1 << 8,
	RECURSED: 1 << 9,
	PENDING: 1 << 10,
  } as const;
  let Bun: A = null;
  let Deno: A = null;
  if (typeof (globalThis as A).Deno !== "undefined") {
	Deno = (globalThis as A).Deno;
  }
  if (typeof (globalThis as A).Bun !== "undefined") {
	Bun = (globalThis as A).Bun;
  }
  export const hasBun = typeof (globalThis as A).Bun !== "undefined";
  export const hasDeno = typeof (globalThis as A).Deno !== "undefined";
  export const hasNode =
	typeof process !== "undefined" && !!process.versions?.node;
  export const isBrowser = typeof window !== "undefined";
  export const isVSCode =
	typeof process !== "undefined" && process.env.VSCODE_PID;
  export let _localChange = false;
  export let _channel: BroadcastChannel | null = null;
  export let isReact = false;
  export let IS_THE_WIZARD = true;
  let init_hooks_context = false;
  const FREE_LIST_END = -1;
  export const END = Symbol("rift:end");
  const REC = 8;
  const F_OP = 0;
  const F_PID = 1;
  const F_SLOT = 2;
  const F_A = 3;
  const F_B = 4;
  const F_C = 5;
  const F_EPOCH = 6;
  const F_EXT = 7;
  const DEFAULT_SCHEDULE = [
	OP.READ,
	OP.WRITE,
	OP.READ,
	OP.WRITE,
	OP.DELETE,
	OP.EFFECT,
	OP.COMPACT,
  ] as const;
  const pow22 = (n: number) => n > 0 && (n & (n - 1)) === 0;
  export const OP2 = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	COMPACT: 4,
  } as const;
  export type OpKind = (typeof OP)[keyof typeof OP];
  export type WaitStrategy = "spin" | "yield" | "park";
  export interface HADRON_OPTIONS {
	capacity?: number;
	pidCapacity?: number;
	f64Capacity?: number;
	u32Capacity?: number;
	i32Capacity?: number;
	byteCapacity?: number;
	maxBatch?: number;
	wait?: WaitStrategy;
	shared?: boolean;
	schedule?: readonly number[];
  }
  export interface LANE_STATS {
	published: number;
	consumed: number;
	batches: number;
	maxBatch: number;
	fullStalls: number;
	claimed: number;
  }
  export const rv = <T,>(v: UF<T>): T =>
	typeof v === "function" ? (v as () => T)() : v;
  export const sameValue = (a: A, b: A): B =>
	Object.is(a, b) || (a instanceof Date && b instanceof Date && +a === +b);
  type QuadSub<V> = (v: V | UD) => void;
  const enum VM_OP {
	PUSH_CONST = 0, // arg: index into consts[]
	PUSH_CELL = 1, // arg: pid — read from shared f64 arena via pidToSlot
	ADD = 2,
	SUB = 3,
	MUL = 4,
	DIV = 5,
	SUM_RANGE = 6, // args: startPid count — reads a contiguous slot run
	RET = 7,
  }
  interface Program {
	code: Int32Array; // [op, arg, op, arg, ...]
	consts: Float64Array;
  }
  const TAG = {
	UNDEFINED: 0,
	NULL: 1,
	FALSE: 2,
	TRUE: 3,
	INT32: 4,
	FLOAT64: 5,
	STRING: 6,
	ARRAY: 7,
	OBJECT: 8,
	UINT8ARRAY: 9,
	REF: 10,
} as const;
export interface RiftRecord<T = A> {
    opened: B;
    claimed: B;
    value?: T;
    createdAt: N;
    openedAt?: N;
}
type StoreListener<T = A> = (v: RiftRecord<T>) => void;
// #endregion
//                                                                                                                   [GOVERNOR]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
class GOVERNOR {
	private hub: DEATH_STAR;
	/* ==========================================================================
	   The disruptor runs hot on its own.
	   The public API never touches passes.
	   ========================================================================== */
	private tier: 0 | 1 | 2 | 3 = 0; // 25 / 50 / 75 / 100
	private running = false;
	private hibernating = false;
	private lastAction = 0;
	private windowActions = 0;
	private windowStart = 0;
	private readonly IDLE_MS = 15_000;
	private readonly TIERS = [
		256, // tier 0 -> 25%
		1_024, // tier 1 -> 50%
		4_096, // tier 2 -> 75%
		16_384, // tier 3 -> 100%
	];
	constructor(DEATH_STAR: DEATH_STAR) {
		this.hub = DEATH_STAR;
	}
	public wake(): void {
		this.lastAction = Date.now();

		if (this.hibernating) {
			this.hibernating = false;
		}

		if (!this.running) {
			this.running = true;
			queueMicrotask(() => this.loop());
		}
	}
	private async loop(): Promise<void> {
		while (this.running) {
			const now = Date.now();

			// Hibernation: 15s of true silence.
			if (now - this.lastAction >= this.IDLE_MS) {
				this.hibernating = true;
				this.running = false;
				this.tier = 0;
				return;
			}

			// Drain one revolution at the current tier.
			this.hub.signals.DARTH_MAUL_ENGINE.pass();

			// Sample load and adjust tier.
			this.windowActions++;

			if (this.windowStart === 0) {
				this.windowStart = now;
			}

			const windowMs = now - this.windowStart;

			if (windowMs >= 1_000) {
				const rate = (this.windowActions / windowMs) * 1_000;

				this.tier = this.tierFor(rate);

				this.windowActions = 0;
				this.windowStart = now;
			}

			// Yield so the loop never starves the JS runtime.
			await Promise.resolve();
		}
	}
	private tierFor(rate: number): 0 | 1 | 2 | 3 {
		if (rate >= this.TIERS[2]) return 3;
		if (rate >= this.TIERS[1]) return 2;
		if (rate >= this.TIERS[0]) return 1;
		return 0;
	}
	public currentTier(): number {
		return this.tier;
	}
	public isHibernating(): boolean {
		return this.hibernating;
	}
}
// #endregion
//                                                                                                [DARTH MAUL] DISRPTOR SIGNALS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const queued: (EffectNode | UD)[] = [];
export const enum ReactiveFlags {
	None = 0,
	Mutable = 1,
	Watching = 2,
	RecursedCheck = 4,
	Recursed = 8,
	Dirty = 16,
	Pending = 32,
}
const HasChildEffect = 64;
let cycle = 0;
let runDepth = 0;
let batchDepth = 0;
let notifyIndex = 0;
let queuedLength = 0;
let activeSub: ReactiveNode | UD;
class SIGNALS {
	public root: DEATH_STAR
	constructor(root: DEATH_STAR) { this.root = root }

	public r2x = new (class R2X {// EXAMPLE CLASS SHAPE
		public hub: DEATH_STAR
		public signals: SIGNALS
		constructor(s: SIGNALS) {
			this.hub = s.root
			this.signals = s
		}

		

	})(this);
	public R21 = class R2X {
        public hub: DEATH_STAR;
        public signals: SIGNALS;

        constructor(s: SIGNALS) {
            this.hub = s.root;
            this.signals = s;
        }
    };

	public rx = new (class RX {// REACTIVE ENGINE
		public hub: DEATH_STAR
		public signals: SIGNALS
		constructor(s: SIGNALS) { this.hub = s.root; this.signals = s; }
		
		private batchDepth = 0;
		private openScopes = new Set<BatchScope>();
		private dirty = new Set<FlushCallback>();
		private _flushScheduled = false;
		private _adapter: RuntimeAdapter | null = null;
		private _currentHookState: HookState | null = null;
		private _currentComponent: ComponentInstance | null = null;
		private PRIORITY_ORDER: Priority[] = ["immediate", "user-blocking", "normal", "background", "idle"];
		private GLOBAL_SCOPE = Symbol("global-scope");
		private scopeCursors: Record<Priority, number> = { immediate: 0, "user-blocking": 0, normal: 0, background: 0, idle: 0 };
		private currentScope: symbol | string = this.GLOBAL_SCOPE;
		private loopScheduled = false;
		private _subscribers = new Map<string, Set<() => void>>();
		private _components = new Map<string, ComponentInstance>();
		private _pendingNotifications = new Set<string>();
		private externalSubscribers = new Set<() => void>();
		private _externalSubscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
		isSignal(fn: () => V): B {	return fn.name === 'bound ' + signalOper.name;}
		isComputed(fn: () => V): B { return fn.name === 'bound ' + computedOper.name;}
		isEffect(fn: () => V): B {	return fn.name === 'bound ' + effectOper.name;}
		isEffectScope(fn: () => V): B {	return fn.name === 'bound ' + effectScopeOper.name;}
		public notifyHook(key: string) {
			this._pendingNotifications.add(key);
			if (!this._flushScheduled) {
				this._flushScheduled = true;
				queueMicrotask(this._flushHookNotifications);
			}
		}
		public subscribeToHook(key: string, cb: () => void): () => void {
			if (!this._subscribers.has(key)) this._subscribers.set(key, new Set());
			this._subscribers.get(key)!.add(cb);
			return () => {
				this._subscribers.get(key)?.delete(cb);
				if (this._subscribers.get(key)?.size === 0) this._subscribers.delete(key);
			};
		}
		public endHookDependencyCapture() { this._currentHookState = null;	}
		public beginHookDependencyCapture(hook: HookState) { this._currentHookState = hook;	}
		public registerDOMBinding(node: Node, update: (node: Node, value: A) => void) {
			const hook = this._currentHookState;
			if (!hook) return;
			const binding = { node, update, active: true };
			if (!hook.bindings) { hook.bindings = new Set(); }
			hook.bindings.add(binding);
			const anyNode = node as A;
			if (!anyNode.__sauron_bindings__) { anyNode.__sauron_bindings__ = new Set(); }
			anyNode.__sauron_bindings__.add(hook);
			return () => {
				if (!binding.active) return;
				binding.active = false;
				hook.bindings?.delete(binding);
				anyNode.__sauron_bindings__?.delete(hook);
			};
		}
		public setCurrentComponent(instance: ComponentInstance | null) { this._currentComponent = instance;	}
		public registerDOMRef(name: string, node: Node) {
			if (!this._currentHookState) return;
			if (!this._currentHookState.domRefs) this._currentHookState.domRefs = new Map();
			this._currentHookState.domRefs.set(name, node);
		}
		public getDOMRef(name: string): Node | undefined {
			if (!this._currentHookState || !this._currentHookState.domRefs) return undefined;
			return this._currentHookState.domRefs.get(name);
		}
		public getCurrentComponent = (): ComponentInstance => {
			if (isReact === true && IS_THE_WIZARD === false) {
				if (this._currentComponent) return this._currentComponent;
				return _root;
			} else {
				if (!init_hooks_context) {
					this.set("_root", _root, true, this.s, this.m, this.r);
					init_hooks_context = true;
				}
				const updated_ctx = this.get("_root", this.s, this.m, this.r) as ComponentInstance;
				if (updated_ctx && updated_ctx.id === this.r) return updated_ctx;
				else return _root;
			}
		};
		public unmountComponent(id: string) {
			const instance = this._components.get(id);
			if (!instance) return;
			for (const dispose of instance.unmount) {
				try { dispose(); } 
				catch (error) { console.error(`[THE_CLOUD][UNMOUNT] cleanup failed for "${id}":`, error); }
			}
			instance.unmount.clear();
			for (const hook of instance.hooks.values()) {
				try { hook.cleanup?.(); }
				catch (error) {  console.error(`[THE_CLOUD][UNMOUNT] hook cleanup failed for "${id}":`, error); }
			}
			instance.hooks.clear();
			this._components.delete(id);
		}
		public mountComponent(id: string, renderFn?: () => void): ComponentInstance {
			const instance: ComponentInstance = { id, hooks: new Map(), unmount: new Set(), render: renderFn, };
			this._components.set(id, instance);
			return instance;
		}
		public hmrPreserve(): Map<string, Map<string, A>> {
			const snapshot = new Map<string, Map<string, A>>();
			this._components.forEach((instance, id) => {
				const hooks = new Map<string, A>();
				instance.hooks.forEach((state, key) => { hooks.set(key, state.value); });
				snapshot.set(id, hooks);
			});
			return snapshot;
		}
		public hmrRestore(snapshot: Map<string, Map<string, A>>) {
			snapshot.forEach((hooks, id) => {
				const instance = this._components.get(id);
				if (!instance) return;
				hooks.forEach((value, key) => {
					const state = instance.hooks.get(key);
					if (state) state.value = value;
				});
			});
		}
		public _devRecord: ((componentId: string, hookName: string, sauronId: string, key: string, value: A) => void) | null = null;
		public devRecord(componentId: string, hookName: string, sauronId: string, key: string, value: A) {
			return this._devRecord?.(componentId, hookName, sauronId, key, value);
		}
		public registerDevRecorder(fn: typeof this._devRecord) { this._devRecord = fn;	}
		public rerender = (key?: string): void => {
			if (key !== undefined) {
				//   const hs = graphHookStates.get(key);
				// if (hs) notifyHook(`${GRAPH_BRIDGE_COMPONENT_ID}:${key}`);
				this.notifyHook(key);
			} else {
				this.flushSync();
			}
		};
		public flushHookKey = (key: string) => {
			const [componentId] = key.split(":");
			/**
			 * Store/global synthetic hook keys do not map to a component hook state.
			 * They simply notify subscribers.
			 */
			if (componentId === "store") {
				this._subscribers.get(key)?.forEach((callback: A) => {
					if (typeof callback !== "function") return;
					callback();
					this.onFlush(callback);
				});
				return;
			}
	
			const component = this._components.get(componentId) ?? _root;
			const hook = component?.hooks.get(key);
	
			/**
			 * Prefer direct DOM binding updates where bindings exist.
			 */
			if (hook?.bindings && hook.bindings.size > 0) {
				for (const binding of hook.bindings) {
					if (!binding.active) continue;
	
					binding.update(binding.node, hook.value);
				}
	
				return;
			}
			this._subscribers.get(key)?.forEach((callback: A) => {
				if (typeof callback !== "function") return;
	
				callback();
				this.onFlush(callback);
			});
		};
		public _flushHookNotifications = () => {
			this._flushScheduled = false;
			const keys = Array.from(this._pendingNotifications);
			this._pendingNotifications.clear();
	
			for (const key of keys) {
				this.flushHookKey(key);
			}
		};
		public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
		public onFlush(cb: FlushCallback): void { this.dirty.add(cb); this.scheduleDrain();	}
		public scheduleDrain(): void {
			if (this._flushScheduled) return;
			this._flushScheduled = true;
			queueMicrotask(this.drain);
		}
		public drain(): void {
			this._flushScheduled = false;
			if (this.dirty.size === 0) return;
	
			// An explicit openBatch() scope is still open (deliberately spanning an await) —
			// don't drop the pending callbacks, just wait for closeBatch() to let them through.
			if (this.openScopes.size > 0) {
				this.scheduleDrain();
				return;
			}
	
			const callbacks = this.dirty;
			this.dirty = new Set();
			for (const cb of callbacks) cb();
		}
		public microtask(fn: FlushCallback): void {	 queueMicrotask(fn); }
		public batch<T>(fn: () => T): T {
			this.batchDepth++;
			try {
				return fn();
			} finally {
				this.batchDepth--;
				if (this.batchDepth === 0) {
					this.flush();
				}
			}
		}
		public openBatch(): BatchScope {
			const scope: BatchScope = Symbol("batch-scope");
			this.openScopes.add(scope);
			return scope;
		}
		public closeBatch(scope: BatchScope): void {
			this.openScopes.delete(scope);
			if (this.openScopes.size === 0 && this.dirty.size > 0) { this.scheduleDrain(); }
		}
		public isBatching(): boolean { return this.batchDepth > 0 || this.openScopes.size > 0;	}
		public flushSync(): void {
			if (this.openScopes.size > 0) return;
			this._flushScheduled = false;
			if (this.dirty.size === 0) return;
			const callbacks = this.dirty;
			this.dirty = new Set();
			for (const cb of callbacks) cb();
		}
		public flush<T>(fn?: () => T): T | void {
			if (!fn) { this.flushSync(); return; }
			this.batchDepth++;
			let result: T;
			try { result = fn(); } 
			finally { this.batchDepth--; }
			this.flushSync();
			return result;
		}
		public enqueue(task: ScheduledTask): void {
			if (task.priority === "immediate") {
				this.microtask(() => this.runOne(task));
				return;
			}
			const tier = queues[task.priority];
			if (!tier.has(task.scope)) tier.set(task.scope, []);
			tier.get(task.scope)!.push(task);
			this.scheduleLoop();
		}
		public scheduleLoop(): void {
			if (this.loopScheduled) return;
			this.loopScheduled = true;
			setImmediate(this.drainOne);
		}
		public pickNext(): ScheduledTask | undefined {
			for (const p of this.PRIORITY_ORDER) {
				if (p === "immediate") continue;
				const tier = queues[p];
				if (tier.size === 0) continue;
				const scopes = Array.from(tier.keys());
				const idx = this.scopeCursors[p] % scopes.length;
				const scope = scopes[idx];
				const list = tier.get(scope)!;
				const task = list.shift()!;
				if (list.length === 0) tier.delete(scope);
				this.scopeCursors[p] = idx + 1;
				return MaybePromise(task);
			}
			return undefined;
		}
		public runOne(task: ScheduledTask): void {
			if (task.cancelled || task.signal?.aborted) return;
			const prev = this.currentScope;
			this.currentScope = task.scope;
			try { task.fn(); } 
			finally { this.currentScope = prev; }
		}
		public drainOne(): void {
			this.loopScheduled = false;
			const task = this.pickNext();
			if (task) this.runOne(task);
			if (Object.values(queues).some((m) => m.size > 0)) this.scheduleLoop();
		}
		public schedule(fn: () => void, opts: { priority?: Priority; scope?: string; signal?: AbortSignal } = {}): Task {
			const task: ScheduledTask = { priority: opts.priority ?? "normal", scope: opts.scope ?? this.currentScope, fn, signal: opts.signal, cancelled: false };
			// signals.chains().sc.addEventListener('abort', () => { task.cancelled = true })
			this.enqueue(task);
			const self = this;
			return {
				cancel(): void { task.cancelled = true; },
				reprioritize(next: Priority): void {
					if (task.cancelled || next === task.priority) return;
					const old = queues[task.priority].get(task.scope);
					if (old) {
						const i = old.indexOf(task);
						if (i !== -1) old.splice(i, 1);
						if (old.length === 0) queues[task.priority].delete(task.scope);
					}
					task.priority = next;
					self.enqueue(task);
				},
			};
		}
		public withRequestScope<T>(requestId: string, fn: () => T): T {
			const prev = this.currentScope;
			this.currentScope = requestId;
			try { return fn(); }
			 finally { this.currentScope = prev; }
		}
		public yieldToScheduler(): Promise<void> {
			return new Promise((resolve) => { this.schedule(() => resolve(), { priority: "normal", scope: this.currentScope as string });		});
		}
		public scheduleChunked(work: Iterable<() => void> | Iterator<() => void>, opts: { priority?: Priority; scope?: string; chunkBudgetMs?: number } = {}): Task {
			const iterator: Iterator<() => void> = Symbol.iterator in (work as A) ? (work as Iterable<() => void>)[Symbol.iterator]() : (work as Iterator<() => void>);
			const chunkBudgetMs = opts.chunkBudgetMs ?? 5;
			const scope = opts.scope ?? (this.currentScope as string);
			let cancelled = false;
			let priority = opts.priority ?? "normal";
			const self = this;
	
			function runChunk(): void {
				if (cancelled) return;
				const start = performance.now();
				while (true) {
					const { value: step, done } = iterator.next();
					if (done) return;
					step();
					const elapsed = performance.now() - start;
					if (elapsed >= chunkBudgetMs) break;
				}
				if (!cancelled) self.schedule(runChunk, { priority, scope });
			}
	
			this.schedule(runChunk, { priority, scope });
			return {
				cancel(): void {
					cancelled = true;
				},
				reprioritize(next: Priority): void {
					priority = next;
				},
			};
		}
		public _reactHookCursor = 0;
		public _currentRenderInstanceCheck: A = null;
		public _isReactRenderContext(): boolean {
			const _react = isReact as A;
			if (!_react) return false;
			if (IS_THE_WIZARD) return false;
			try {
				_react.darkState;
				// If darkState is callable without throwing, we're in a React render context
				// We probe by checking if the dispatcher is active via a known internal
				const internals = (_react as A).__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;
				const dispatcher = internals?.ReactCurrentDispatcher?.current ?? internals?.H;
				return !!dispatcher;
			} catch (e) {
				// log(`[THE_CLOUD][_IS_REACT_RENDER_CONTEXT]`, { error: e, }, t.error)
				return false;
			}
		}
		public createHook<T>(hookName: string, sauronId: string, initializer: () => HookState, updater: (state: HookState, key: string, notify: (key: string) => void) => T, triggerRender: boolean = true): T {
			if (isReact === true && IS_THE_WIZARD === false) {
				try {
					const isReact = this._isReactRenderContext();
					const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponent(), forceUpdate: undefined };
	
					const component = instance;
	
					// ─── THE TARGETED FIX ───────────────────────────────────────────────────────
					// Use sequential indexing when in React so it matches React's rendering slots.
					// Otherwise, use Sauron's default compilation ID string.
					const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
					const key = `${component.id}:${hookName}:${stableSlotId}`;
					// ────────────────────────────────────────────────────────────────────────────
					if (!this._components.has(component.id)) {
						this._components.set(component.id, component);
					}
	
					if (!component.hooks.has(key)) {
						const state = initializer();
						component.hooks.set(key, state);
						if (component.render) {
							const unsub = this.subscribeToHook(key, () => component.render?.());
							component.unmount.add(unsub);
						}
					}
	
					if (isReact && this._adapter && forceUpdate) { this._adapter.onHookSubscribe(key, forceUpdate); }
	
					const notify = (k: string) => {
						if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
							const state = component.hooks.get(k);
							this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
						}
						this.notifyHook(k);
					};
	
					const hookState = component.hooks.get(key)!;
					this.beginHookDependencyCapture(hookState);
					try { return updater(hookState, key, notify); 	} 
					finally { this.endHookDependencyCapture(); }
				} catch (e: A) {
					//  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
					return e.error;
				}
			} else {
				try {
					const component = this.getCurrentComponent();
					const key = `${component.id}:${hookName}:${sauronId}`;
	
					if (!this._components.has(component.id)) { this._components.set(component.id, component); }
	
					if (!component.hooks.has(key)) {
						const state = initializer();
						component.hooks.set(key, state);
	
						if (triggerRender && component.render) {
							const unsub = this.subscribeToHook(key, () => component.render?.());
							component.unmount.add(unsub);
						}
					}
	
					const notify = (k: string) => {
						if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
							const state = component.hooks.get(k);
							this.devRecord(component.id, hookName, sauronId, k, state?.value);
						}
						if (triggerRender) this.notifyHook(k);
					};
	
					const hookState = component.hooks.get(key)!;
					this.beginHookDependencyCapture(hookState);
					try { return updater(hookState, key, notify); }
					finally { this.endHookDependencyCapture(); }
				} catch (e: A) {
					// log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
					return e.error;
				}
			}
		}
	})(this);
	public dom_signals = new (class DOM_SIGNALS {// DIRECT DOM INJECTION, REACTIVE SIGNALS... ALMOST SOUNDS LIKE A FAST AND THE FURIOUS SPECCED ENGINE - NON-ATOMS SIGNALS
			public r: A = "THE_WIZARD_ROOT_REG";
			public m: A = "SAURON_MASTER_REG";
			public s: A = "SAURON_SLAVE_REG";
			public k: A = "__SAURON__";

			public hub: DEATH_STAR
			public signals: SIGNALS

			public selfId: S
			#SIGNAL_CELLS: Record<S, Record<S, Record<S, Record<S, A>>>> = Object.create(null);
			public _signal_cells: Record<S, A>
		
			public _signal_listeners: Record<S, ST<(value: A) => void>>;
			public _signal_global_listeners: Record<S, ST<(key: A, value: A) => void>>;
			public _signal_waiters: Record<S, ST<(value: A) => void>>;
		
			public _signal_persistent: Record<S, A>
			public _signal_persistent_keys: Record<S, A>
		
			public _signal_cells2: M2<A, A>;
			#SIGNAL_CELLS2 = new M2<A, M2<A, M2<A, A>>>();
			public _signal_listeners2: M2<A, ST<(value: A) => void>>;
			public _signal_waiters2: M2<A, ST<(value: A) => void>>;
			public _signal_global_listeners2: M2<A, ST<(key: A, value: A) => void>>;
			public _signal_persistent2: M2<A, A>;
			public _signal_persistent_keys2: M2<A, A>;
			constructor(s: SIGNALS) {
				this.r = s.root.r ?? "THE_WIZARD_ROOT_REG";
				this.m = s.root.m ?? "SAURON_MASTER_REG";
				this.s = s.root.s ?? "SAURON_SLAVE_REG";
				this.k = s.root.k ?? "THE_K_KEY";
				this.hub = s.root
				this.signals = s

				this._signal_cells = this.getS(this.s, this.m, this.r, "#SIGNAL_CELLS");
				this._signal_listeners = this.getS(this.s, this.m, this.r);
				this._signal_global_listeners = this.getS(this.s, this.m, this.r);
				this._signal_waiters = this.getS(this.s, this.m, this.r);
				this._signal_persistent_keys = this.getS(this.s, this.m, this.r);
				this._signal_persistent = this.getS(this.s, this.m, this.r);
				this.selfId = `bus_${this.hub.REGISTRY._sid()}`
			}
			public get_(c:S = "signal_cells") {
				let _
				switch (c) {
					case "signal_cells": { _ = this._signal_cells; break; }
					case "signal_listeners": { _ = this._signal_listeners; break; }
					case "signal_global_listeners": { _ = this._signal_global_listeners; break; }
					case "signal_waiters": { _ = this._signal_waiters; break; }
					case "signal_persistent": { _ = this._signal_persistent; break; }
					case "signal_persistent_keys": { _ = this._signal_persistent_keys; break; }
					default: {  break; }
				}
				return _
			}
			public getHash(c:S = "signal_cells", r:S = this.r) {
				let root;
				switch (c) {
					case "signal_cells": { root = this.#SIGNAL_CELLS[r]; break; }
					default: {  break; }
				}
				return root
			}
			public get_root(c: S = "signal_cells", r: S) {
				let root;
				switch (c) {
					case "signal_cells": { root = this.#SIGNAL_CELLS[r]; break; }
				}
				if (!root) {
					root = Object.create(null);
					switch (c) {
						case "signal_cells": { this.#SIGNAL_CELLS[r] = root; break; }
					}
				}
				return root;
			}
			public getS(s: S = this.s, m: S = this.m, r: S = this.r, c = "#handlers") {
				const rL = this.get_root(c, r);
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				return sL;
			}
			public getM(m: S = this.m, r: S = this.r, c = "#handlers") {
				const rL = this.get_root(c, r);
				const mL = (rL[m] ??= Object.create(null));
				return mL;
			}
			public getR(r: S = this.r, c = "#handlers") {
				const rL = this.get_root(c, r); 
				return rL;
			}
		   
			private get<T = A>( key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },	): { (): T | UD; (value: T | UD): V } {
				const { s, m, r } = opts;
				if (s === this.s && m === this.m && r === this.r ) {
		
				} else {
					
				}
				let cell = this._signal_cells.get(key);
			
				if (!cell) {
					cell = signal<T>(undefined);
					this._signal_cells.set(key, cell);
				}
			
				return cell as { (): T | UD; (value: T | UD): V };
			}
			private read<T = A>( cell: { (): T | UD; (value: T | UD): V }, ): T | UD {
				
				if (s === this.s && m === this.m && r === this.r ) {
		
				} else {
					
				}
				const prevSub = getActiveSub();
			
				// Do not create a dependency while performing an administrative read.
				setActiveSub(undefined);
			
				try {
					return cell() as T | UD;
				} finally {
					setActiveSub(prevSub);
				}
			}
			private write<T = A>( cell: { (): T | UD; (value: T | UD): V }, value: T | UD,	): void {
				
				if (s === this.s && m === this.m && r === this.r ) {
		
				} else {
					
				}
				cell(value as T);
			}
			private notifyKeyListeners( key: A, value: A | UD, opts: ScopeOptions,	): void {
				 
				const listeners = this._signal_listeners.get(key);
			
				if (!listeners) return;
			
				for (const listener of listeners) {
					try {
						listener(value);
					} catch (err) {
						console.error(`[Sauron Signal Listener Error]:`, err);
					}
				}
			}
			public scheduleGlobalNotify( key: A, value: A | UD, opts: ScopeOptions,	): void {
				const globalListeners = this._signal_global_listeners.get(
					"_signal_global_listeners_raven",
				);
			
				if (globalListeners) {
					for (const listener of globalListeners) {
						try {
							listener(key, value);
						} catch (err) {
							console.error(`[Sauron Signal Global Listener Error]:`, err);
						}
					}
				}
			
				const waiters = this._signal_waiters.get(key);
			
				if (waiters) {
					const resolved = Array.from(waiters);
					this._signal_waiters.delete(key);
			
					for (const resolve of resolved) {
						try {
							resolve(value);
						} catch (err) {
							console.error(`[Sauron Signal Waiter Error]:`, err);
						}
					}
				}
			}
			private notifyAll( key: A, value: A | UD, opts: ScopeOptions,	): void {
				this.notifyKeyListeners(key, value, opts);
				this.scheduleGlobalNotify(key, value, opts);
			}
			public atom<T = any>(v: A, k: SignalKey = `__THE_WARLOCK_SIGNALS_${this.hub.REGISTRY._sid()}`, s = this.s, m = this.m, r = this.r, ) {
				if (s === this.s && m === this.m && r === this.r ) {
		
				} else {
					
				}
				return {
					get value(): T { return this.signal(k); },
					get current(): T { return this.signal(k); },
					peek(): T { return this.signal(k); },
					set value(v: T) { this.signal(k, v); },
					set current(v: T) { this.signal(k, v); },
					set crt(v: T) { this.signal(k, v); },
					set set(v: T) { this.signal(k, v); },
				};
			}
			public signal = (() => {
				const fn = <T = A,>(k: SignalKey, v?: T | (() => T)) => {
					if (s === this.s && m === this.m && r === this.r ) {
			
					} else {
						
					}
					const registry = this._signal_cells
					let atom = registry.get(k);
		
					if (atom && v === undefined) { return atom; }
					if (atom && v) { 
						const t = UF(v)
						atom = registry.set(k, t)
						return atom;
					 }
		
					const initialValue = UF(v)
					atom = signal<T>(initialValue);
					registry.set(k, atom);
					return atom;
				};
				return fn;
			})();
			public store = {
				get: <T = A,>( key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }, ): T | UD => {
					const cell = this.get<T>(key, opts);
					const value = this.read<T>(cell);
			
					if (value !== undefined) {
						return MaybePromise(value);
					}
			
					const persistent = this._signal_persistent;
					const persistKeys = this._signal_persistent_keys;
			
					if (persistKeys.has(key) && persistent.has(key)) {
						const hydrated = persistent.get(key) as T;
						this.write(cell, hydrated);
						return MaybePromise(hydrated);
					}
			
					return MaybePromise(undefined);
				},
				set: <T = A,>( key: A, value: T | ((prev: T | UD) => T), opts: { triggerRender?: B; cap?: N } & ScopeOptions = { s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200 }, ): T | UD => {
					const { triggerRender = true, s, m, r, cap } = opts;
					const cell = this.get<T>(key, opts);
				
					const prev = this.read<T>(cell);
					const next =
						typeof value === "function"
							? (value as (prev: T | UD) => T)(prev)
							: value;
				
					if (Object.is(prev, next)) {
						return MaybePromise(prev as T);
					}
				
					this.write(cell, next);
				
					if (this._signal_persistent_keys.has(key)) {
						this._signal_persistent.set(key, next);
					}
				
					this.notifyKeyListeners(key, next, { s, m, r });
				
					if (triggerRender) {
						this.scheduleGlobalNotify(key, next, { s, m, r });
					}
				
					return MaybePromise(next as T);
				},
				subscribe: <T = A,>(
					key: A,
					listener: (value: T) => void,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): (() => void) => {
					const cell = this.get<T>(key, opts);
					const listeners = this._signal_listeners;
			
					if (!listeners.has(key)) {
						listeners.set(key, new Set());
					}
			
					listeners.get(key)!.add(listener as (value: A) => void);
			
					return () => {
						const current = listeners.get(key);
			
						if (!current) return;
			
						current.delete(listener as (value: A) => void);
			
						if (current.size === 0) {
							listeners.delete(key);
						}
					};
				},
				delete: (key: A,opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },): B => {
					const { s, m, r } = opts;
					const cells = this._signal_cells;
					const cell = cells.get(key);
				
					const existed = cells.has(key);
				
					if (cell) {
						this.write(cell, undefined);
					}
				
					cells.delete(key);
				
					if (this._signal_persistent_keys.has(key)) {
						this._signal_persistent_keys.delete(key);
						this._signal_persistent.delete(key);
					}
				
					this.notifyKeyListeners(key, undefined, { s, m, r });
				
					if (existed) {
						this.scheduleGlobalNotify(key, undefined, { s, m, r });
					}
				
					this._signal_listeners.delete(key);
				
					return MaybePromise(existed);
				},
				entries: <T = A,>(
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): [A, T][] => {
					const cells = this._signal_cells;
					const result: [A, T][] = [];
			
					for (const [key, cell] of cells.entries()) {
						const value = this.read<T>(cell as any);
			
						if (value !== undefined) {
							result.push([key, value]);
						}
					}
			
					return MaybePromise(result);
				},	
				persist: (
					key: A,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): void => {
					const cell = this.get(key, opts);
					const value = this.read(cell);
			
					this._signal_persistent_keys.set(key, true);
			
					if (value !== undefined) {
						this._signal_persistent.set(key, value);
					}
				},	
				unpersist: (
					key: A,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): void => {
					this._signal_persistent_keys.delete(key);
					this._signal_persistent.delete(key);
				},	
				has: (
					key: A,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): B => {
					const cell = this._signal_cells.get(key);
			
					if (!cell) {
						return MaybePromise(false);
					}
			
					const value = this.read(cell as any);
			
					return MaybePromise(value !== undefined);
				},	
				clear: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }, ): void => {
					const cells = this._signal_cells;
				
					for (const [key, cell] of Array.from(cells.entries())) {
						this.write(cell as any, undefined);
						this.notifyKeyListeners(key, undefined, opts);
						this.scheduleGlobalNotify(key, undefined, opts);
					}
				
					cells.clear();
					this._signal_listeners.clear();
				},
				clearPersistent: (
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): void => {
					this._signal_persistent.clear();
					this._signal_persistent_keys.clear();
				},
				_emit: (key: A, s = this.s, m = this.m, r = this.r, ): void => {
					const opts = { s, m, r };
					const cell = this._signal_cells.get(key);
				
					if (!cell) return;
				
					const value = this.read(cell as any);
				
					this.notifyAll(key, value, opts);
				},
			};
			public state: SauronState = {
				get: <T = A,>(
					key: string | symbol,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): T | UD => {
					return MaybePromise(this.store.get<T>(key, opts) as T);
				},	
				set: <T = A,>(
					key: string | symbol,
					value: T | ((prev: T | UD) => T),
					opts: { triggerRender?: B } & ScopeOptions =
						{ s: this.s, m: this.m, r: this.r },
				): void => {
					const { s, m, r, triggerRender = true } = opts;
			
					this.store.set(key, value, {
						s,
						m,
						r,
						triggerRender,
					});
			
					return MaybePromise(value as T);
				},	
				delete: (key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }, ): B => {
					return this.store.delete(key, opts);
				},
				getByPrefix: <T = A,>(
					prefix: string,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): { key: string; value: T }[] => {
					return this.store
						.entries<T>(opts)
						.filter(([key]) => typeof key === "string" && key.startsWith(prefix))
						.map(([key, value]) => ({ key: key as string, value }));
				},
				getAll: <T = A,>(
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): Record<string | symbol, T> => {
					const result: Record<string | symbol, T> = {};
			
					for (const [key, value] of this.store.entries<T>(opts)) {
						result[key] = value;
					}
			
					return result;
				},
				subscribe: <T = A,>(
					key: string | symbol,
					listener: (value: T) => void,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): (() => void) => {
					return this.store.subscribe<T>(key, listener, opts);
				},
				subscribeAll: (
					listener: (key: string | symbol, value: A) => void,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): (() => void) => {
					const { s, m, r } = opts;
					const listeners = this._signal_global_listeners;
			
					if (!listeners.has("_signal_global_listeners_raven")) {
						listeners.set("_signal_global_listeners_raven", new Set());
					}
			
					const set = listeners.get("_signal_global_listeners_raven")!;
					set.add(listener as any);
			
					return () => {
						set.delete(listener as any);
			
						if (set.size === 0) {
							listeners.delete("_signal_global_listeners_raven");
						}
					};
				},
				clear: (
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): void => {
					const { s, m, r } = opts;
			
					this.store.clear(opts);
			
					this._signal_waiters.clear();
			
					const globalListeners = this._signal_global_listeners.get(
						"_signal_global_listeners_raven",
					);
			
					globalListeners?.clear();
				},
				waitFor: <T = A,>(
					key: string | symbol,
					opts: ScopeOptions = { s: this.s, m: this.m, r: this.r },
				): Promise<T> => {
					const existing = this.state.get<T>(key, opts);
			
					if (existing !== undefined) {
						return Promise.resolve(existing);
					}
			
					return new Promise<T>((resolve) => {
						const waiters = this._signal_waiters;
			
						if (!waiters.has(key)) {
							waiters.set(key, new Set());
						}
			
						waiters.get(key)!.add(resolve as A);
					});
				},
			};
			public cell = (() => {
				const fn = <T = A>(key: A, s?: S, m?: S, r?: S) => {
					return MaybePromise(this.state.get<T>(key, { s, m, r }));
				};
				fn.getAll= <T = A>(opts?: ScopeOptions): Record<S | SY, T> =>{
					return this.state.getAll<T>(opts);
				}
				fn.set=<T = A>(label: A, value: T, triggerRender = true, s?: S, m?: S, r?: S) =>{
					this.cell._broadcastSS(label, value, "sauron", "set");
					return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
				}
				fn._broadcastSS=(key: A, value: A, storageType: S, fn: S, path?: S, domain?: S, options?: A) =>{
					_localChange = true;
					_channel?.postMessage({ key, value, storageType, fn, path, domain, options });
				}
				fn.subAll=(fn: (key: S | symbol, value: A) => V, s?: S, m?: S, r?: S): () => V =>{
					if (!fn) return () => {};
					return this.state.subscribeAll(fn, { s, m, r });
				}
				fn.sub=<T = A>(k: A, fn: (value: T) => void, s?: S, m?: S, r?: S): () => void =>{
					if (!k || !fn) return () => {};
					return MaybePromise(this.state.subscribe<T>(k, fn, { s, m, r }));
				}
				fn.delete=(label: A, s?: S, m?: S, r?: S): B =>{
					if (!label) return false;
					const v = this.state.delete(label, { s, m, r });
					this.cell._broadcastSS(label, undefined, "sauron", "delete");
					return v;
				}
				fn.waitFor=<T = A>(label: S, s?: S, m?: S, r?: S): P<T> =>{
					return this.state.waitFor<T>(label, { s, m, r });
				}
				fn.deleteMany= async(keys: A[], s?: S, m?: S, r?: S): P<B> =>{
					if (!Array.isArray(keys) || keys.length === 0) {
						return false;
					}
					let allDeleted = true;
			
					// Batch all deletes without triggering renders until the end
					for (let i = 0; i < keys.length; i++) {
						const deleted = this.cell.delete(keys[i], s, m, r);
						if (!deleted) {
							allDeleted = false;
						}
					}
			
					return allDeleted;
				}
				fn.setMany=async<T = Record<S, A>>(entries: T): Promise<void>=> {
					if (!entries || typeof entries !== "object") {
						return;
					}
					// Batch all sets without triggering renders until the end
					const entriesArray = Object.entries(entries);
					for (let i = 0; i < entriesArray.length; i++) {
						const [key, value] = entriesArray[i];
						// Only trigger render on the last item to avoid multiple renders
						const triggerRender = i === entriesArray.length - 1;
						this.cell.set(key, value, triggerRender);
					}
				}
				fn.getMany=async<T = A>(keys: S[], s?: S, m?: S, r?: S) =>{
					if (!Array.isArray(keys) || keys.length === 0) {
						return {};
					}
			
					const result: Record<S, T | undefined> = {};
			
					const promises = keys.map(async (key) => {
						const value = await this.cell<T>(key, s, m, r);
						return { key, value };
					});
			
					const results = await Promise.all(promises);
			
					for (const { key, value } of results) {
						result[key] = value;
					}
			
					return result;
				}
				return fn;
			})();
			public state_setter(v: U, k: A = `_USECELL`, s: S = this.s, m: SU = this.m, r: SU = this.r) {
				const existing = this.state.get(k, { s: s, m: m, r: r });
				if (existing !== undefined) return existing;
				const t = typeof v === "function" ? v() : v;
				this.state.set(k, t, { triggerRender: true, s: s, m: m, r: r });
				return t;
			} 
			/** public get_root2(c: S, r: S) {
			 let root;
			 switch (c) {
				 case "#SIGNAL_CELLS": { root = this.#SIGNAL_CELLS.get(r); break; }
				 case "globalThis": { (globalThis as A)[r]; break; }
			 }
			 if (!root) {
				 root = new M2<A, M2<A, M2<A, A>>>();
				 switch (c) {
					 case "#SIGNAL_CELLS": { root = this.#SIGNAL_CELLS.set(r, root); break; }
					 case "globalThis": { (globalThis as A)[r] = root; break; }
				 }
			 }
			 return root;
		 }
		 public map<K = A, V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): M2<K, V> | K | V {
			 const root = this.get_root(c, r);
			 let masterMap = root.get(m);
			 if (!masterMap) { masterMap = new M2<A, M2<A, A>>(); root.set(m, masterMap); }
			 let slaveMap = masterMap.get(s);
			 if (!slaveMap) { slaveMap = new M2<K, V>(); masterMap.set(s, slaveMap); }
			 return slaveMap;
		 } */
	})(this);
	public maul_ngin = new (class DARTH_MAUL_ENGINE {// DISRUPTOR SIGNALS ENGINE
		public maul: typeof this.signals.maul; 
		public root: DEATH_STAR
		public signals: SIGNALS
		//                                                                                                                    CONSTANTS
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private readonly capacity: number;
		private readonly lane: I32;
		private readonly ops: U8;
		public readonly posX: F32;
		public readonly posY: F32;
		public readonly val: I32;
		private readonly freePids: I32;
		private freeHead = 0;
		private freeTail = 0;
		private laneLen = 0;
		public nextAllocatedPid = 1;
		// Lane deduplication: 1 means this PID already has a pending lane entry.
		private readonly inLane: U8;
		// ======================================================================
		// Reactive node columns
		// ======================================================================
		private readonly rKind: U8;
		private readonly rFlags: U16;
		private readonly rValue: U[];
		private readonly rPending: U[];
		private readonly rGetter: (((prev?: U) => U) | UD)[];
		private readonly rFn: ((() => V | (() => V)) | UD)[];
		private readonly rCleanup: ((() => V) | UD)[];
		private readonly rDeps: I32;
		private readonly rDepsTail: I32;
		private readonly rSubs: I32;
		private readonly rSubsTail: I32;
		// ======================================================================
		// Link pool
		// ======================================================================
		private readonly lVersion: I32;
		private readonly lDep: I32;
		private readonly lSub: I32;
		private readonly lPrevDep: I32;
		private readonly lNextDep: I32;
		private readonly lPrevSub: I32;
		private readonly lNextSub: I32;
		private readonly freeLinks: I32;
		private freeLinkHead = 0;
		private freeLinkTail = 0;
		private nextLink = 1;
		// ======================================================================
		// Scheduler
		// ======================================================================
		private readonly effectQueue: I32;
		private queuedLength = 0;
		private notifyIndex = 0;
		public readonly HasChildEffect = 64;
		public cycle = 0;
		public runDepth = 0;
		public batchDepth = 0;
		public activeSub: Pid | UD;
		public BRAND_SIGNAL = Symbol("warlock.signal");
		public BRAND_EFFECT = Symbol("warlock.effect");
		public BRAND_EFFECT_SCOPE = Symbol("warlock.effectScope");
		public BRAND_COMPUTED = Symbol("warlock.computed");
		// #endregion
		constructor(s: SIGNALS, capacity = 1 << 18) {
			this.root = s.root;
			this.maul = s.maul;
			this.signals = s;
			this.capacity = capacity;
	
			this.lane = new Int32Array(capacity).fill(-1);
			this.ops = new Uint8Array(capacity);
			this.posX = new Float32Array(capacity);
			this.posY = new Float32Array(capacity);
			this.val = new Int32Array(capacity);
	
			this.freePids = new Int32Array(capacity);
			this.inLane = new Uint8Array(capacity);
	
			this.rKind = new Uint8Array(capacity);
			this.rFlags = new Uint16Array(capacity);
			this.rValue = new Array(capacity);
			this.rPending = new Array(capacity);
			this.rGetter = new Array(capacity);
			this.rFn = new Array(capacity);
			this.rCleanup = new Array(capacity);
	
			this.rDeps = new Int32Array(capacity).fill(-1);
			this.rDepsTail = new Int32Array(capacity).fill(-1);
			this.rSubs = new Int32Array(capacity).fill(-1);
			this.rSubsTail = new Int32Array(capacity).fill(-1);
	
			this.effectQueue = new Int32Array(capacity);
	
			this.lVersion = new Int32Array(capacity);
			this.lDep = new Int32Array(capacity);
			this.lSub = new Int32Array(capacity);
			this.lPrevDep = new Int32Array(capacity).fill(-1);
			this.lNextDep = new Int32Array(capacity).fill(-1);
			this.lPrevSub = new Int32Array(capacity).fill(-1);
			this.lNextSub = new Int32Array(capacity).fill(-1);
	
			this.freeLinks = new Int32Array(capacity);
	
			for (let i = capacity - 1; i >= 1; i--) {
				this.freePids[this.freeTail++] = i;
			}
	
			this.lane[0] = TRACER_PID;
			this.val[TRACER_PID] = 0;
			this.ops[TRACER_PID] = OP.NONE;
			this.laneLen = 1;
		}
		// ======================================================================
		// World-object lane API — unchanged semantics, now deduplicated
		// ======================================================================
		private enqueueOp(pid: Pid, op: N): V {
			if (pid === TRACER_PID) return;
	
			// Dedup: if already queued, only the latest operation wins.
			if (this.inLane[pid]) {
				this.ops[pid] = op;
				return;
			}
	
			if (this.laneLen >= this.capacity) {
				throw new Error("DISRUPTOR: lane capacity exhausted");
			}
	
			this.ops[pid] = op;
			this.inLane[pid] = 1;
			this.lane[this.laneLen++] = pid;
		}
		public check_size_and_edit(x: N, y: N, pid: Pid, value: N): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.EDIT);
		}
		public getX(pid: Pid): N {
			return this.posX[pid];
		}
		public getY(pid: Pid): N {
			return this.posY[pid];
		}
		public getPosition(pid: Pid): { x: N; y: N } {
			return { x: this.posX[pid], y: this.posY[pid] };
		}
		public write(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.WRITE);
		}
		public create(x: N, y: N, value: N, pid: Pid): V {
			this.write(x, y, value, pid);
		}
		public edit(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.EDIT);
		}
		public read(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.READ);
		}
		public delete(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.DELETE);
		}
		public effect(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.EFFECT);
		}
		public compact(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.COMPACT);
		}
		public none(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.NONE);
		}
		public send(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.SEND);
		}
		public sub(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.SUB);
		}
		public duplex(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.DUPLEX);
		}
		public stream(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.STREAM);
		}
		public drc(x: N, y: N, value: N, pid: Pid): V {
			this.posX[pid] = x;
			this.posY[pid] = y;
			this.val[pid] = value;
			this.enqueueOp(pid, OP.DRC);
		}
		// ======================================================================
		// Lane pass — Warlock structure preserved, dedup markers cleared
		// ======================================================================
		public pass(): V {
			const len = this.laneLen;
			let writeIdx = 0;
			let i = 0;
	
			const unrolledLimit = len - (len % 4);
	
			for (; i < unrolledLimit; i += 4) {
				for (let k = 0; k < 4; k++) {
					const pid = this.lane[i + k];
	
					if (pid === TRACER_PID) {
						this.val[TRACER_PID]++;
						this.lane[writeIdx++] = pid;
					} else if (this.ops[pid] === OP.DELETE) {
						this.inLane[pid] = 0;
						this.reclaimPid(pid);
					} else {
						this.inLane[pid] = 0;
						this.ops[pid] = OP.NONE;
						this.lane[writeIdx++] = pid;
					}
				}
			}
	
			for (; i < len; i++) {
				const pid = this.lane[i];
				if (pid === TRACER_PID) {
					this.val[TRACER_PID]++;
					this.lane[writeIdx++] = pid;
				} else if (this.ops[pid] === OP.DELETE) {
					this.inLane[pid] = 0;
					this.reclaimPid(pid);
				} else {
					this.inLane[pid] = 0;
					this.ops[pid] = OP.NONE;
					this.lane[writeIdx++] = pid;
				}
			}
			this.laneLen = writeIdx;
		}
		/** One tick: world-object pass, then reactive effect flush. */
		public tick(): V {
			this.pass();
			this.flush();
		}
		public get tickCount(): N { return this.val[TRACER_PID];	}
		public get length(): N { return this.laneLen;	}
		public getLoadFactor(): N {
			const activeCount = Math.max(0, this.laneLen - 1);
			return activeCount / (this.capacity - 1);
		}
		public waitFor(): V {}
		public sweep_expired(): V {}
		public on_change(): V {}
		// ======================================================================
		// PID / link lifecycle
		// ======================================================================
		private allocPid(): Pid {
			if (this.freeHead !== this.freeTail) {
				const pid = this.freePids[this.freeHead];
				this.freeHead = (this.freeHead + 1) % this.capacity;
				return pid;
			}
			if (this.nextAllocatedPid >= this.capacity) { throw new Error("DISRUPTOR: capacity exhausted — no free pids");		}
			return this.nextAllocatedPid++;
		}
		private allocLink(): Lid {
			if (this.freeLinkHead !== this.freeLinkTail) {
				const lid = this.freeLinks[this.freeLinkHead];
				this.freeLinkHead = (this.freeLinkHead + 1) % this.capacity;
				return lid;
			}
			if (this.nextLink >= this.capacity) { throw new Error("DISRUPTOR: link capacity exhausted"); }
			return this.nextLink++;
		}
		private freeLink(lid: Lid): V {
			this.lVersion[lid] = 0;
			this.lDep[lid] = -1;
			this.lSub[lid] = -1;
			this.lPrevDep[lid] = -1;
			this.lNextDep[lid] = -1;
			this.lPrevSub[lid] = -1;
			this.lNextSub[lid] = -1;
	
			this.freeLinks[this.freeLinkTail] = lid;
			this.freeLinkTail = (this.freeLinkTail + 1) % this.capacity;
		}
		public dispose(pid: Pid): V {	this.reclaimPid(pid);	}
		private reclaimPid(pid: Pid): V {
			if (pid === TRACER_PID) return;
			if (this.rFlags[pid] & RX_Flags.DISPOSED) return;
	
			switch (this.rKind[pid]) {
				case RKind.EFFECT:
					this.effectOper(pid);
					break;
	
				case RKind.EFFECT_SCOPE:
					this.effectScopeOper(pid);
					break;
	
				case RKind.COMPUTED:
					this.disposeAllDepsInReverse(pid);
					break;
			}
	
			this.rKind[pid] = RKind.NONE;
			this.rFlags[pid] = RX_Flags.DISPOSED;
			this.rValue[pid] = undefined;
			this.rPending[pid] = undefined;
			this.rGetter[pid] = undefined;
			this.rFn[pid] = undefined;
			this.rCleanup[pid] = undefined;
	
			this.rDeps[pid] = -1;
			this.rDepsTail[pid] = -1;
			this.rSubs[pid] = -1;
			this.rSubsTail[pid] = -1;
	
			this.inLane[pid] = 0;
	
			this.freePids[this.freeTail] = pid;
			this.freeTail = (this.freeTail + 1) % this.capacity;
		}
		// ======================================================================
		// Reactive graph links
		// ======================================================================
		public link(dep: Pid, sub: Pid, version: number): void {
			const prevDep = this.rDepsTail[sub];
	
			if (prevDep !== -1 && this.lDep[prevDep] === dep) {
				return;
			}
	
			const nextDep = prevDep !== -1 ? this.lNextDep[prevDep] : this.rDeps[sub];
	
			if (nextDep !== -1 && this.lDep[nextDep] === dep) {
				this.lVersion[nextDep] = version;
				this.rDepsTail[sub] = nextDep;
				return;
			}
	
			const prevSub = this.rSubsTail[dep];
	
			if (prevSub !== -1 && this.lVersion[prevSub] === version && this.lSub[prevSub] === sub) {
				return;
			}
	
			const lid = this.allocLink();
	
			this.lVersion[lid] = version;
			this.lDep[lid] = dep;
			this.lSub[lid] = sub;
	
			this.lPrevDep[lid] = prevDep;
			this.lNextDep[lid] = nextDep;
			this.lPrevSub[lid] = prevSub;
			this.lNextSub[lid] = -1;
	
			this.rDepsTail[sub] = lid;
			this.rSubsTail[dep] = lid;
	
			if (nextDep !== -1) this.lPrevDep[nextDep] = lid;
			if (prevDep !== -1) this.lNextDep[prevDep] = lid;
			else this.rDeps[sub] = lid;
	
			if (prevSub !== -1) this.lNextSub[prevSub] = lid;
			else this.rSubs[dep] = lid;
		}
		public unlink(lid: Lid, sub: Pid = this.lSub[lid]): Lid {
			const dep = this.lDep[lid];
			const prevDep = this.lPrevDep[lid];
			const nextDep = this.lNextDep[lid];
			const prevSub = this.lPrevSub[lid];
			const nextSub = this.lNextSub[lid];
	
			if (nextDep !== -1) this.lPrevDep[nextDep] = prevDep;
			else this.rDepsTail[sub] = prevDep;
	
			if (prevDep !== -1) this.lNextDep[prevDep] = nextDep;
			else this.rDeps[sub] = nextDep;
	
			if (nextSub !== -1) this.lPrevSub[nextSub] = prevSub;
			else this.rSubsTail[dep] = prevSub;
	
			if (prevSub !== -1) {
				this.lNextSub[prevSub] = nextSub;
			} else {
				this.rSubs[dep] = nextSub;
	
				// Dependency has no subscribers left.
				if (nextSub === -1) {
					this.unwatched(dep);
				}
			}
	
			this.freeLink(lid);
			return nextDep;
		}
		public disposeAllDepsInReverse(pid: Pid): void {
			let link = this.rDepsTail[pid];
	
			while (link !== -1) {
				const prev = this.lPrevDep[link];
				this.unlink(link, pid);
				link = prev;
			}
		}
		public purgeDeps(pid: Pid): void {
			const tail = this.rDepsTail[pid];
			let link = tail !== -1 ? this.lNextDep[tail] : this.rDeps[pid];
	
			while (link !== -1) {
				link = this.unlink(link, pid);
			}
		}
		public unwatched(pid: Pid): void {
			switch (this.rKind[pid]) {
				case RKind.COMPUTED:
					this.rFlags[pid] = RX_Flags.MUTABLE;
					this.disposeAllDepsInReverse(pid);
					break;
	
				case RKind.SIGNAL:
					break;
	
				case RKind.EFFECT:
					this.effectOper(pid);
					break;
	
				case RKind.EFFECT_SCOPE:
					this.effectScopeOper(pid);
					break;
			}
		}
		// ======================================================================
		// Propagation — corrected iterative version
		// ======================================================================
		public propagate(startLink: number, innerWrite: boolean): void {
			if (startLink === -1) return;
	
			const stack: number[] = [startLink];
	
			while (stack.length > 0) {
				const link = stack.pop()!;
				const sub = this.lSub[link];
	
				if (sub === -1) continue;
	
				let flags = this.rFlags[sub];
	
				if (flags & RX_Flags.RECURSEDCHECK) {
					// Currently evaluating; defer until it finishes.
					continue;
				}
	
				if (!(flags & RX_Flags.PENDING)) {
					this.rFlags[sub] = flags | RX_Flags.PENDING;
	
					if (innerWrite) {
						this.rFlags[sub] |= RX_Flags.RECURSED;
					}
	
					flags = this.rFlags[sub];
				}
	
				if (flags & RX_Flags.WATCHING) {
					this.notify(sub);
				}
	
				// Computeds continue propagation to their own subscribers.
				if (this.rKind[sub] === RKind.COMPUTED) {
					let childLink = this.rSubs[sub];
	
					while (childLink !== -1) {
						stack.push(childLink);
						childLink = this.lNextSub[childLink];
					}
				}
			}
		}
		public shallowPropagate(startLink: number): void {
			let link = startLink;
	
			while (link !== -1) {
				const sub = this.lSub[link];
				const flags = this.rFlags[sub];
	
				if (flags & RX_Flags.PENDING) {
					if ((flags & (RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK)) === RX_Flags.WATCHING) {
						this.notify(sub);
					}
				}
	
				link = this.lNextSub[link];
			}
		}
		public notify(effectPid: Pid): void {
			let pid = effectPid;
	
			while (pid !== -1 && pid !== TRACER_PID) {
				const flags = this.rFlags[pid];
	
				if (!(flags & RX_Flags.WATCHING)) break;
	
				// Queue this effect.
				if (this.queuedLength < this.effectQueue.length) {
					this.effectQueue[this.queuedLength++] = pid;
				}
	
				// Clear WATCHING so it is not queued twice.
				this.rFlags[pid] &= ~RX_Flags.WATCHING;
	
				// Walk to parent effect/scope, if any.
				const parentLink = this.rSubs[pid];
	
				if (parentLink === -1) break;
	
				const parent = this.lSub[parentLink];
	
				if (parent === -1 || (this.rKind[parent] !== RKind.EFFECT && this.rKind[parent] !== RKind.EFFECT_SCOPE)) {
					break;
				}
	
				pid = parent;
			}
		}
		// ======================================================================
		// Dirty checking — version-based, no recursive graph walk
		// ======================================================================
		public isDirty(pid: Pid): boolean {
			let link = this.rDeps[pid];
	
			while (link !== -1) {
				const dep = this.lDep[link];
	
				if (this.rKind[dep] === RKind.COMPUTED) {
					if (this.isDirty(dep)) {
						this.updateComputed(dep);
					}
				}
	
				if (this.lVersion[link] !== this.lVersion[dep]) {
					return true;
				}
	
				link = this.lNextDep[link];
			}
	
			return false;
		}
		// Alias kept for compatibility with the original API.
		public checkDirty(_link: number, pid: Pid): boolean {
			return this.isDirty(pid);
		}
		// ======================================================================
		// Signal operations
		// ======================================================================
		public signalOper<T>(pid: Pid, ...value: [] | [T]): T | void {
			if (value.length > 0) {
				const next = value[0];
	
				if (!Object.is(this.rPending[pid], next)) {
					this.rPending[pid] = next;
					this.rFlags[pid] |= RX_Flags.DIRTY;
	
					const subs = this.rSubs[pid];
	
					if (subs !== -1) {
						this.propagate(subs, this.runDepth > 0);
					}
	
					if (this.batchDepth === 0) {
						this.commitSignal(pid);
						this.flush();
					}
				}
	
				return;
			}
	
			if (this.rFlags[pid] & RX_Flags.DIRTY) {
				this.commitSignal(pid);
			}
	
			const sub = this.activeSub;
	
			if (sub !== undefined) {
				this.link(pid, sub, this.cycle);
			}
	
			return this.rValue[pid] as T;
		}
		private commitSignal(pid: Pid): void {
			const previous = this.rValue[pid];
			const next = this.rPending[pid];
	
			this.rFlags[pid] &= ~RX_Flags.DIRTY;
	
			if (Object.is(previous, next)) return;
	
			this.rValue[pid] = next;
			this.cycle++;
			this.lVersion[pid] = this.cycle;
	
			const subs = this.rSubs[pid];
	
			if (subs !== -1) {
				this.propagate(subs, this.runDepth > 0);
			}
		}
		public updateSignal(pid: Pid): boolean {
			const previous = this.rValue[pid];
			const next = this.rPending[pid];
	
			this.rValue[pid] = next;
	
			return !Object.is(previous, next);
		}
		// ======================================================================
		// Computed operations
		// ======================================================================
		public computedOper<T>(pid: Pid): T {
			if (this.isDirty(pid)) {
				this.updateComputed(pid);
	
				const subs = this.rSubs[pid];
	
				if (subs !== -1) {
					this.shallowPropagate(subs);
				}
			}
	
			const sub = this.activeSub;
	
			if (sub !== undefined) {
				this.link(pid, sub, this.cycle);
			}
	
			return this.rValue[pid] as T;
		}
		public updateComputed(pid: Pid): boolean {
			if (this.rFlags[pid] & RX_Flags.RUNNING) {
				return false;
			}
	
			// Remove stale child-effect ownership links before re-evaluating.
			if (this.rFlags[pid] & this.HasChildEffect) {
				let link = this.rDepsTail[pid];
	
				while (link !== -1) {
					const prev = this.lPrevDep[link];
					const dep = this.lDep[link];
	
					if (this.rKind[dep] !== RKind.SIGNAL && this.rKind[dep] !== RKind.COMPUTED) {
						this.unlink(link, pid);
					}
	
					link = prev;
				}
			}
	
			// Full refresh: drop all old dependency links, then retrack.
			this.disposeAllDepsInReverse(pid);
	
			this.rFlags[pid] = RX_Flags.RUNNING | RX_Flags.RECURSEDCHECK;
	
			const previousValue = this.rValue[pid];
			const previousSub = this.setActiveSub(pid);
	
			try {
				this.cycle++;
	
				const nextValue = this.rGetter[pid]!(previousValue);
	
				this.rValue[pid] = nextValue;
				this.lVersion[pid] = this.cycle;
	
				return !Object.is(previousValue, nextValue);
			} finally {
				this.activeSub = previousSub;
				this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
				this.purgeDeps(pid);
			}
		}
		// ======================================================================
		// Effects
		// ======================================================================
		public run(pid: Pid): void {
			if (this.rKind[pid] !== RKind.EFFECT) return;
	
			const flags = this.rFlags[pid];
	
			if (flags & RX_Flags.RUNNING) {
				// Re-entrant run: defer rather than recurse.
				this.scheduleEffect(pid);
				return;
			}
	
			const pending = (flags & RX_Flags.PENDING) !== 0;
			const dirty = pending ? this.isDirty(pid) : false;
	
			if (!pending && !dirty && this.rDeps[pid] !== -1) {
				this.rFlags[pid] = RX_Flags.WATCHING | (flags & this.HasChildEffect);
				return;
			}
	
			if (this.rCleanup[pid]) {
				this.runCleanup(pid);
	
				if (this.rFlags[pid] & RX_Flags.DISPOSED) {
					return;
				}
			}
	
			// Full refresh before rerun: prevents stale dependency retention.
			this.disposeAllDepsInReverse(pid);
	
			this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
	
			const previousSub = this.setActiveSub(pid);
	
			try {
				this.cycle++;
				this.runDepth++;
	
				const cleanup = this.rFn[pid]!();
	
				this.rCleanup[pid] = typeof cleanup === "function" ? (cleanup as () => void) : undefined;
			} finally {
				this.runDepth--;
				this.activeSub = previousSub;
				this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
				this.purgeDeps(pid);
			}
		}
		public runCleanup(pid: Pid): void {
			const cleanup = this.rCleanup[pid];
	
			if (!cleanup) return;
	
			this.rCleanup[pid] = undefined;
	
			const previousSub = this.activeSub;
			this.activeSub = undefined;
	
			try {
				cleanup();
			} finally {
				this.activeSub = previousSub;
			}
		}
		public effectOper(pid: Pid): void {
			if (this.rFlags[pid] & RX_Flags.DISPOSED) return;
	
			this.runCleanup(pid);
			this.effectScopeOper(pid);
		}
		public effectScopeOper(pid: Pid): void {
			if (this.rFlags[pid] & RX_Flags.DISPOSED) return;
	
			// Remove this scope from its parent before disposing children.
			this.unlinkParent(pid);
	
			// Dispose all child effects/scopes linked to this scope.
			this.disposeAllDepsInReverse(pid);
	
			this.rFlags[pid] |= RX_Flags.DISPOSED;
		}
		private unlinkParent(pid: Pid): void {
			let link = this.rSubs[pid];
	
			while (link !== -1) {
				const next = this.lNextSub[link];
				const parent = this.lSub[link];
	
				if (this.rKind[parent] === RKind.EFFECT || this.rKind[parent] === RKind.EFFECT_SCOPE) {
					this.unlink(link, pid);
	
					// Clear parent ownership flag if it has no remaining children.
					if (this.rDeps[parent] === -1) {
						this.rFlags[parent] &= ~this.HasChildEffect;
					}
				}
	
				link = next;
			}
		}
		private scheduleEffect(pid: Pid): void {
			if (this.rFlags[pid] & RX_Flags.PENDING) return;
	
			this.rFlags[pid] |= RX_Flags.PENDING;
	
			if (this.queuedLength < this.effectQueue.length) {
				this.effectQueue[this.queuedLength++] = pid;
			}
		}
		// ======================================================================
		// Flush
		// ======================================================================
		public flush(): void {
			try {
				while (this.notifyIndex < this.queuedLength) {
					const pid = this.effectQueue[this.notifyIndex];
					this.effectQueue[this.notifyIndex++] = -1;
	
					this.rFlags[pid] &= ~RX_Flags.PENDING;
					this.run(pid);
				}
			} finally {
				// Any effects scheduled during flush remain queued for next tick.
				while (this.notifyIndex < this.queuedLength) {
					const pid = this.effectQueue[this.notifyIndex];
					this.effectQueue[this.notifyIndex++] = -1;
	
					this.rFlags[pid] |= RX_Flags.WATCHING | RX_Flags.RECURSED;
				}
	
				this.notifyIndex = 0;
				this.queuedLength = 0;
			}
		}
		public untrack = <T,>(fn: () => T): T => {
			const prev = this.setActiveSub(undefined);
			try {
				return fn();
			} finally {
				this.setActiveSub(prev);
			}
		};
		// ======================================================================
		// Factories
		// ======================================================================
		private allocNode(kind: RKind): Pid {
			const pid = this.allocPid();
	
			this.rKind[pid] = kind;
			this.rFlags[pid] = RX_Flags.NONE;
			this.rValue[pid] = undefined;
			this.rPending[pid] = undefined;
			this.rGetter[pid] = undefined;
			this.rFn[pid] = undefined;
			this.rCleanup[pid] = undefined;
	
			this.rDeps[pid] = -1;
			this.rDepsTail[pid] = -1;
			this.rSubs[pid] = -1;
			this.rSubsTail[pid] = -1;
	
			return pid;
		}
		public signal<T>(initialValue?: T): Pid {
			const pid = this.allocNode(RKind.SIGNAL);
	
			this.rValue[pid] = initialValue;
			this.rPending[pid] = initialValue;
			this.rFlags[pid] = RX_Flags.MUTABLE;
	
			return pid;
		}
		public computed<T>(getter: (previous?: T) => T): Pid {
			const pid = this.allocNode(RKind.COMPUTED);
	
			this.rGetter[pid] = getter as (prev?: unknown) => unknown;
	
			return pid;
		}
		public effectSignal(fn: () => void | (() => void)): Pid {
			const pid = this.allocNode(RKind.EFFECT);
	
			this.rFn[pid] = fn;
			this.rFlags[pid] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
	
			const previousSub = this.setActiveSub(pid);
	
			if (previousSub !== undefined && previousSub !== -1) {
				this.rFlags[previousSub] |= this.HasChildEffect;
			}
	
			try {
				this.runDepth++;
	
				const cleanup = fn();
	
				this.rCleanup[pid] = typeof cleanup === "function" ? (cleanup as () => void) : undefined;
			} finally {
				this.runDepth--;
				this.activeSub = previousSub;
				this.rFlags[pid] &= ~RX_Flags.RECURSEDCHECK;
			}
	
			return pid;
		}
		public effectScope(fn: () => void): Pid {
			const pid = this.allocNode(RKind.EFFECT_SCOPE);
	
			this.rFlags[pid] = RX_Flags.MUTABLE;
	
			const previousSub = this.setActiveSub(pid);
	
			if (previousSub !== undefined && previousSub !== -1) {
				// Parent link: scope is dep, parent is sub.
				this.link(pid, previousSub, 0);
				this.rFlags[previousSub] |= this.HasChildEffect;
			}
	
			try {
				fn();
			} finally {
				this.activeSub = previousSub;
			}
	
			return pid;
		}
		public trigger(fn: () => void): void {
			const watcher = this.allocNode(RKind.EFFECT);
	
			this.rFlags[watcher] = RX_Flags.WATCHING | RX_Flags.RECURSEDCHECK;
	
			const previousSub = this.setActiveSub(watcher);
	
			this.batchDepth++;
	
			try {
				fn();
			} finally {
				this.activeSub = previousSub;
	
				// Propagate every dependency recorded during fn().
				let link = this.rDeps[watcher];
	
				while (link !== -1) {
					const dep = this.lDep[link];
					const next = this.lNextDep[link];
	
					const subs = this.rSubs[dep];
	
					if (subs !== -1) {
						this.propagate(subs, this.runDepth > 0);
						this.shallowPropagate(subs);
					}
	
					link = next;
				}
	
				this.batchDepth--;
	
				if (this.batchDepth === 0) {
					this.flush();
				}
	
				this.reclaimPid(watcher);
			}
		}
		// ======================================================================
		// Compatibility helpers
		// ======================================================================
		public getActiveSub(): Pid | undefined {
			return this.activeSub;
		}
		public setActiveSub(sub?: Pid): Pid | undefined {
			const previous = this.activeSub;
			this.activeSub = sub;
			return previous;
		}
		public getBatchDepth(): number {
			return this.batchDepth;
		}
		public startBatch(): void {
			this.batchDepth++;
		}
		public endBatch(): void {
			if (--this.batchDepth === 0) {
				this.flush();
			}
		}
		public isSignal(pid: Pid): boolean {
			return this.rKind[pid] === RKind.SIGNAL;
		}
		public isComputed(pid: Pid): boolean {
			return this.rKind[pid] === RKind.COMPUTED;
		}
		public isEffect(pid: Pid): boolean {
			return this.rKind[pid] === RKind.EFFECT;
		}
		public isEffectScope(pid: Pid): boolean {
			return this.rKind[pid] === RKind.EFFECT_SCOPE;
		}
	})(this);
	public maul = new (class DARTH_MAUL {// SIGNALS && ATOMS SOLUTION
		//                                                                                                                    CONSTANTS
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public root: DEATH_STAR;
		public signals: SIGNALS
		public DARTH_MAUL_ENGINE: typeof  this.signals.maul_ngin
	
		public r: S = "this.r";
		public m: S = "this.m";
		public s: S = "this.s";
		public k: A = "__SIGNALS__";
		u = undefined;
		uuuu = [this.u, this.u, this.u, this.u];
		UUUU = [this.s, this.m, this.r];
		uuu = [this.u, this.u, this.u];
		UUU = [this.s, this.m, this.r];
		uu = [this.u, this.u];
		UU = [this.m, this.r];
		private total_cache_size = 5000;
		public activeTracer: Map<SignalKey, SIGNAL_CELL> | null = null;
		constructor(s: 	SIGNALS, capacity = 1 << 20) {
			this.root = s.root;
			this.r = s.root.r;
			this.m = s.root.m;
			this.s = s.root.s;
			this.k = s.root.k;
			this.signals = s
			this.DARTH_MAUL_ENGINE = s.maul_ngin;
		}
		// #endregion
		//                                                                                                                         MAPS
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		#maul_meta = new M2<A, A>();
		private _getMetadata(s = this.s, m = this.m, r = this.r) {
			let root = this.#maul_meta.get(r);
			if (!root) {
				root = new M2<A, Map<A, Map<A, A>>>();
				this.#maul_meta.set(r, root);
			}
	
			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new M2<A, Map<A, A>>();
				root.set(m, masterMap);
			}
	
			let metaMap = masterMap.get(s);
			if (!metaMap) {
				metaMap = {
					_initial: new M2<string, A>(),
					_sys: new M2<string, SysEntry>(),
					_keyContracts: new M2<string, Set<string>>(),
					_causal: new M2<string, CausalEntry[]>(),
					_conflicts: new M2<string, ConflictPolicy>(),
					_contracts: new M2<string, A>(),
					_scopes: new M2<string, SignalScope>(),
					_msgHandlers: new M2<string, Set<(p: A) => void>>(),
					_mainMap: new M2<A, string>(),
					_tupleCache: new M2<string, string>(),
					_weakMap: new WeakMap<object, string>(),
				};
				masterMap.set(s, metaMap);
			}
			return metaMap;
		}
		public ATOM = (() => {
			function mergeInto<T extends object>(target: T, source: object): T {
				Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
				return target;
			}
			function resolveFactory(raw: any): any {
				return typeof raw === "function" ? raw : () => raw;
			}
			const NINJA_STUB_BRAND = "__is_atom_stub__";
			function makeStub(trace?: any): any {
				return new Proxy(() => makeStub(trace), {
					get: (_t, prop) => (prop === NINJA_STUB_BRAND ? true : makeStub(trace)),
					apply: () => makeStub(trace),
				});
			}
			function withTypoGuard<T extends Record<string, any>>(result: T, label: string): T {
				return new Proxy(result, {
					get(target, prop, receiver) {
						if (typeof prop === "symbol" || prop in target) return Reflect.get(target, prop, receiver);
						throw new Error(`\n[${label} Typo Error] Property "${String(prop)}" does not exist on this ${label} definition.\n  -> Available definitions: [ ${Object.keys(target).join(", ")} ]\n`);
					},
				});
			}
			// ============================================================================
			//  ONE CELL PER MASTER INSTANCE — the platform primitives, nothing else.
			//      const { selfProxy, liveTarget } = createLiveSelf(instanceId)
			//      Object.assign(liveTarget, pass)
			//      const [o, o$] = root.DISRUPTOR.useCell.obj(liveTarget, label, category)
			//      o$.merge(result)
			//  liveTarget IS the cell value: the mailbox seeds the store with it and
			//  setLive sends that same object, so self and the cell are one object with
			//  one write path. Namespaces are one level: liveTarget.<name>.
			// ============================================================================
			function createLiveSelf(instanceId: string) {
				const liveTarget: any = {};
				const selfProxy = new Proxy(liveTarget, {
					get: (t, k: any) => t[k],
					set: (t, k: any, v: any) => { t[k] = v; root.bear.cell.set(`${instanceId}:${k}`, v); return true; },
				});
				return { selfProxy, liveTarget };
			}
			const OPEN_CELLS: any[] = []; // test-only window onto the cells
			// useCell.obj — the merge variant Illusionist uses:
			//   const [o, o$] = root.DISRUPTOR.useCell.obj(liveTarget, prim_label, categoryName)
			//   o$.merge(result)
			this.root.bus.drs.useCell.obj = (liveTarget: any, label: string, category: string, s = root.s, m = root.m, r = root.r) => {
				const [cell, setLive] = root.drs.useCell.mailbox(liveTarget, label, s, m, r);
				const merge = (patch: any = {}) => {
					const p = typeof patch === "function" ? patch(cell) : patch;
					for (const k of Object.keys(p ?? {})) {
						const v = p[k];
						if (v && typeof v === "object" && !Array.isArray(v) && cell[k] && typeof cell[k] === "object" && !Array.isArray(cell[k])) Object.assign(cell[k], v);
						else cell[k] = v;
					}
					cell._version = (cell._version ?? 0) + 1;
					setLive(cell);
					return cell;
				};
				const o$ = { merge, set: setLive, key: label, category };
				OPEN_CELLS.push({ cell, ...o$ });
				return [cell, o$] as const;
			};
			// A bound atom's self: its own namespace, plus the master's root fields.
			// Nothing here stores state — it reads and writes liveTarget directly.
			function nsSelf(liveTarget: any, name: string, publish: () => void, useCell: () => any) {
				const rec = () => (liveTarget[name] ??= {});
				return new Proxy({}, {
					get(_t, key: any) {
						if (typeof key !== "string" || key === "then") return undefined;
						if (key === "useCell") return useCell;
						if (key in rec()) return rec()[key];
						if (key in liveTarget) {
							if (liveTarget[key]?._isNamespace) throw new Error(`[isolation] "${name}" cannot reach namespace "${key}"`);
							return liveTarget[key];
						}
						return undefined;
					},
					set(_t, key: any, value: any) {
						if (typeof key !== "string") return false;
						rec()[key] = value;
						publish();
						return true;
					},
					has(_t, key: any) { return typeof key === "string" && (key in rec() || key in liveTarget); },
				});
			}
			// The atom's useCell: [own record, merge into own record].
			function cellHandle(record: () => any, publish: () => void) {
				const merge = (patch: any) => {
					const p = typeof patch === "function" ? patch(record()) : patch;
					if (p && typeof p === "object") Object.assign(record(), p);
					publish();
				};
				const h: any = [record(), merge];
				h.get = (key?: string) => (key === undefined ? record() : record()[key]);
				h.set = (key: any, value?: any) => {
					if (typeof key === "string") merge({ [key]: value });
					else if (key && typeof key === "object") merge(key);
					return h;
				};
				return h;
			}
			function requiredBindables() {
				return { gate: GATE, subscription: SUBSCRIPTION, context: CONTEXT };
			}
			function withoutKeys(obj: any, keys: string[]) {
				const desc: any = Object.getOwnPropertyDescriptors(obj);
				for (const k of keys) delete desc[k];
				return desc;
			}
			function createMaster(kind: string, config: any) {
				const kindFactory = resolveFactory(root.bear.cell(kind));
				if (!kindFactory) throw new TypeError(`Unknown master atom: ${kind}`);
			
				const instanceId = `${kind}:${root.REGISTRY._sid}`;
				const { selfProxy, liveTarget } = createLiveSelf(instanceId);
				const names = new Set<string>();
				const refreshers: Record<string, () => void> = {};
				let o$: any;
			
				const publish = () => {
					if (!o$) return; // still seeding: the first merge publishes the whole record
					for (const f of Object.values(refreshers)) f();
					o$.merge({});
					for (const n of names) liveTarget[n]._version = liveTarget._version;
				};
			
				const bind = (name: string, factory: any, cfg: any) => {
					if (name in liveTarget && !names.has(name)) throw new Error(`[COLLISION] bound atom "${name}" collides with master field "${name}"`);
					names.add(name);
					liveTarget[name] = Object.assign(liveTarget[name] ?? {}, { _isNamespace: true });
					const useCell = () => cellHandle(() => liveTarget[name], publish);
					const self = nsSelf(liveTarget, name, publish, useCell);
					const ctx: any = { self, useCell, channel: (l: any) => root.bear.state.subscribe(instanceId, l), master: selfProxy, config: cfg, k: root.k, s: root.s, m: root.m, r: root.r };
					const out = factory(ctx);
					Object.defineProperties(liveTarget[name], withoutKeys(out, ["bindables", "refresh"]));
					if (typeof out.refresh === "function") refreshers[name] = out.refresh;
				};
			
				const view = (name: string) => {
					const rec = liveTarget[name];
					return new Proxy(rec, {
						get: (t, k: any) => (typeof t[k] === "function" ? t[k].bind(rec) : t[k]),
						set: (t, k: any, v: any) => { t[k] = v; publish(); return true; },
					});
				};
			
				const masterCtx = (cfg: any) => {
					const ctx: any = { self: selfProxy, useCell: () => cellHandle(() => liveTarget, publish), channel: (l: any) => root.bear.state.subscribe(instanceId, l), config: cfg, master: selfProxy, k: root.k, s: root.s, m: root.m, r: root.r };
					for (const name of names) ctx[name] = view(name);
					return ctx;
				};
			
				// seed: config lives at the root, same level as the namespaces
				for (const [key, v] of Object.entries(config)) {
					if (key === "_version") throw new Error(`[COLLISION] "_version" is reserved by the cell`);
					liveTarget[key] = v;
				}
			
				// open the cell on the seeded object — this IS the resource from here on
				const [cell, handle] = root.drs.useCell.obj(liveTarget, instanceId, kind);
				o$ = handle;
			
				// 1. required bound atoms first — the master reads gate.sealed on read()
				for (const [name, factory] of Object.entries(requiredBindables())) bind(name, factory, config);
			
				// 2. probe for the master's own bindables list, then build those too
				const probe = kindFactory(masterCtx(config));
				for (const [name, factory] of Object.entries((probe as any).bindables ?? {})) bind(name, resolveFactory(factory), config);
			
				// 3. re-run the factory now that every namespace exists (live views)
				const built = kindFactory(masterCtx(config));
			
				// 4. the master's own fields into the root
				const masterFields = withoutKeys(built, ["bindables", "value"]);
				for (const key of Object.keys(masterFields)) {
					if (key === "_version") throw new Error(`[COLLISION] "_version" is reserved by the cell`);
					if (names.has(key)) throw new Error(`[COLLISION] master field "${key}" collides with bound atom "${key}"`);
				}
				Object.defineProperties(liveTarget, masterFields);
			
				// 5. the public surface: flat master ops + one handle per namespace
				const api: any = { atom: kind };
				for (const name of names) api[name] = view(name);
				for (const fn of ["read", "write", "subscribe", "waitFor"]) if (fn in liveTarget) api[fn] = liveTarget[fn];
				for (const key of Object.keys(masterFields)) {
					if (key in api) continue;
					if (typeof liveTarget[key] === "function") { api[key] = liveTarget[key]; continue; }
					Object.defineProperty(api, key, { enumerable: true, get: () => liveTarget[key] });
				}
				api.version = () => cell._version;
			
				publish(); // build is one publish: every namespace refreshes together
				return withTypoGuard(api, instanceId.split(":")[1]);
			}
			const z = () => {}
			z.MASTER = (name: string, arg: any)=>{ const f = resolveFactory(arg); this.root.signals.bear.cell.set(name, f); return f; }
			z.BIND=(name: string, arg: any)=> { const f = resolveFactory(arg); this.root.signals.bear.cell.set(name, f); return f; }
			z.BINDABLE=(factory: any)=> { return factory; }
			z.CONTRACT=(arg1: any, arg2?: any): any =>{
				const id = `${root.REGISTRY._sid}`;
				const categoryName = typeof arg1 === "string" ? arg1 : `ATOM_CONTRACT${id}`;
				const rawConfig = typeof arg1 === "string" ? arg2 : arg1;
				if (rawConfig === undefined || rawConfig === null) throw new Error("Self is required");
				const factory = resolveFactory(rawConfig);
				const pass = factory({});
				const kind = pass.atom ?? "ATOM";
				// atomContract(7) and atomContract({ value: 7 }) both mean the same thing
				const contract = ((config: any = {}) => {
					const normalized = config && typeof config === "object" ? config : { value: config };
					return createMaster(kind, normalized);
				}) as any;
				Object.assign(contract, pass);
				root.bear.cell.set(categoryName, contract);
				return contract;
			}
			return z
		})(); 
		public MASTERS = (() => {
			const z = () => {}
			z.SAURON = this.ATOM.MASTER("SAURON", ({ self, master, config }: any) => ({}));
			z.MSG_Q = this.ATOM.MASTER("MSG_Q", ({ self, master, config }: any) => ({}));
			z.SLAB = this.ATOM.MASTER("SLAB_ATOM", <X = U>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
				capacity: config.capacity as number,
			
				values: new Float64Array(config.capacity),
				versions: new Uint32Array(config.capacity),
				nextFree: new Int32Array(config.capacity),
				refCount: new Int32Array(config.capacity),
			
				freeHead: -1,
				allocated: 0,
			
				slab: new Uint8Array(config.slabCapacity ?? (1 << 20)),
				slabTail: 0,
			
				slabOffsets: new Int32Array(config.capacity).fill(-1),
				slabLengths: new Int32Array(config.capacity).fill(0),
			
				inUse: new Uint8Array(config.capacity),
			
				initialized: (() => {
					const capacity = config.capacity as number;
			
					if (!pow2(capacity)) {
						throw new Error("slotCapacity must be a power of two");
					}
			
					self.nextFree.fill(-1);
			
					// Slot 0 is reserved as the control slot (version counter).
					self.allocated = 1;
					self.refCount[0] = 1;
					self.versions[0] = 0;
					self.inUse[0] = 1;
			
					// Build initial free list (skip slot 0).
					// Link slots 1..capacity-1 in reverse order so we pop from the front.
					for (let i = capacity - 1; i >= 1; i--) {
						self.nextFree[i] = self.freeHead;
						self.freeHead = i;
					}
			
					return true;
				})(),
			
				writeBinary(slot: N, data: U8, epoch: N): V {
					self.assertLive(slot);
					self.assertEpoch(epoch);
			
					const oldOffset = self.slabOffsets[slot];
					const oldLength = self.slabLengths[slot];
					const length = data.length;
			
					// Reuse the existing allocation when it is large enough.
					if (oldOffset >= 0 && length <= oldLength) {
						self.slab.set(data, oldOffset);
			
						if (length < oldLength) {
							self.freeBytes(oldOffset + length, oldLength - length);
						}
			
						self.slabOffsets[slot] = length === 0 ? 0 : oldOffset;
						self.slabLengths[slot] = length;
						self.values[slot] = 0;
						self.versions[slot] = epoch;
						return;
					}
			
					// Allocate first: exhaustion leaves the old payload intact.
					const offset = self.allocBytes(length);
			
					self.slab.set(data, offset);
			
					if (oldOffset >= 0 && oldLength > 0) {
						self.freeBytes(oldOffset, oldLength);
					}
			
					self.slabOffsets[slot] = offset;
					self.slabLengths[slot] = length;
					self.values[slot] = 0;
					self.versions[slot] = epoch;
				},
			
				readBinary(slot: N): U8 {
					const { offset, length } = self.ref(slot);
					return self.slab.subarray(offset, offset + length);
				},
			
				allocBytes(length: N): N {
					if (length === 0) return 0;
			
					// First-fit allocation from reclaimed ranges.
					for (let i = 0; i < self.freeRanges.length; i++) {
						const range = self.freeRanges[i];
			
						if (range.length < length) continue;
			
						const offset = range.offset;
			
						if (range.length === length) {
							self.freeRanges.splice(i, 1);
						} else {
							range.offset += length;
							range.length -= length;
						}
			
						return offset;
					}
			
					if (length > self.slab.length - self.slabTail) {
						throw new RangeError("STORAGE_RING: no contiguous slab range large enough");
					}
			
					const offset = self.slabTail;
					self.slabTail += length;
					return offset;
				},
			
				freeBytes(offset: N, length: N): V {
					if (length === 0) return;
			
					const ranges = self.freeRanges;
					let index = 0;
			
					while (index < ranges.length && ranges[index].offset < offset) {
						index++;
					}
			
					ranges.splice(index, 0, { offset, length });
			
					// Merge with the preceding range.
					if (
						index > 0 &&
						ranges[index - 1].offset + ranges[index - 1].length === offset
					) {
						ranges[index - 1].length += length;
						ranges.splice(index, 1);
						index--;
					}
			
					// Merge with the following range.
					if (
						index + 1 < ranges.length &&
						ranges[index].offset + ranges[index].length === ranges[index + 1].offset
					) {
						ranges[index].length += ranges[index + 1].length;
						ranges.splice(index + 1, 1);
					}
			
					// Return a free suffix to the unused slab tail.
					const last = ranges[ranges.length - 1];
			
					if (last !== undefined && last.offset + last.length === self.slabTail) {
						self.slabTail = last.offset;
						ranges.pop();
					}
				},
			
				assertLive(slot: N): V {
					if (
						!Number.isInteger(slot) ||
						slot <= 0 ||
						slot >= self.allocated ||
						self.refCount[slot] <= 0
					) {
						throw new RangeError(`STORAGE_RING: invalid or released slot ${slot}`);
					}
				},
			
				assertEpoch(epoch: N): V {
					if (
						!Number.isInteger(epoch) ||
						epoch < 0 ||
						epoch > 0xffffffff
					) {
						throw new RangeError("STORAGE_RING: epoch must be a uint32");
					}
				},
			
				tick(): number {
					return self.versions[0];
				},
			
				advance(): number {
					return ++self.versions[0];
				},
			
				alloc(value = 0): number {
					const slot = self.freeHead;
					if (slot === -1) return -1;
			
					// Remove from free list.
					self.freeHead = self.nextFree[slot];
					self.nextFree[slot] = -1;
			
					// Initialize slot.
					self.values[slot] = value;
					self.refCount[slot] = 1;
					self.inUse[slot] = 1;
			
					// Clear slab allocation for this slot.
					self.slabOffsets[slot] = -1;
					self.slabLengths[slot] = 0;
			
					self.allocated++;
			
					return slot;
				},
			
				retain(slot: number): boolean {
					if (slot < 0 || slot >= self.capacity) return false;
					if (!self.inUse[slot]) return false;
			
					if (self.refCount[slot] === 0x7FFFFFFF) {
						throw new Error("Reference count overflow");
					}
			
					self.refCount[slot]++;
					return true;
				},
			
				release(slot: number): boolean {
					if (slot < 0 || slot >= self.capacity) return false;
					if (!self.inUse[slot]) return false;
			
					if (self.refCount[slot] <= 0) {
						throw new Error("Reference count underflow");
					}
			
					self.refCount[slot]--;
			
					if (self.refCount[slot] === 0) {
						self.inUse[slot] = 0;
			
						// Clear slab data.
						self.slabOffsets[slot] = -1;
						self.slabLengths[slot] = 0;
						self.values[slot] = 0;
			
						// Add to free list.
						self.nextFree[slot] = self.freeHead;
						self.freeHead = slot;
			
						self.allocated--;
			
						return true;
					}
			
					return false;
				},
			
				write(slot: number, value: number, epoch: number): boolean {
					if (slot < 0 || slot >= self.capacity) return false;
					if (!self.inUse[slot]) return false;
					if (self.refCount[slot] <= 0) return false;
			
					// Epoch validation: the slot's version must match the expected epoch.
					if (self.versions[slot] !== epoch) return false;
			
					self.values[slot] = value;
					return true;
				},
			
				read(slot: number): number {
					if (slot < 0 || slot >= self.capacity) return NaN;
					if (!self.inUse[slot]) return NaN;
			
					return self.values[slot];
				},
			
				version(slot: number): number {
					if (slot < 0 || slot >= self.capacity) return -1;
					return self.versions[slot];
				},
			
				ref(slot: number): { offset: number; length: number } {
					if (slot < 0 || slot >= self.capacity) {
						return { offset: -1, length: 0 };
					}
			
					return {
						offset: self.slabOffsets[slot],
						length: self.slabLengths[slot],
					};
				},
			
				storeSlab(slot: number, data: Uint8Array | ArrayBuffer): boolean {
					if (slot < 0 || slot >= self.capacity) return false;
					if (!self.inUse[slot]) return false;
			
					const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
					const len = bytes.length;
			
					if (len === 0) {
						// Zero-length: clear slab assignment.
						self.slabOffsets[slot] = -1;
						self.slabLengths[slot] = 0;
						return true;
					}
			
					// Simple bump allocator: check if we have room.
					if (self.slabTail + len > self.slab.length) {
						// Could implement compaction here, but for now just fail.
						return false;
					}
			
					const offset = self.slabTail;
					self.slab.set(bytes, offset);
					self.slabTail += len;
			
					self.slabOffsets[slot] = offset;
					self.slabLengths[slot] = len;
			
					return true;
				},
			
				loadSlab(slot: number): Uint8Array | null {
					if (slot < 0 || slot >= self.capacity) return null;
					if (!self.inUse[slot]) return null;
			
					const offset = self.slabOffsets[slot];
					const length = self.slabLengths[slot];
			
					if (offset < 0 || length <= 0) return null;
			
					return self.slab.subarray(offset, offset + length);
				},
			
				growthSlab(minAdditional: number): void {
					const newCapacity = Math.max(self.slab.length * 2, self.slabTail + minAdditional);
					const newSlab = new Uint8Array(newCapacity);
					newSlab.set(self.slab);
					self.slab = newSlab;
				},
			
				needsCompaction(): boolean {
					if (self.allocated <= 1) return false;
			
					let liveBytes = 0;
					for (let i = 1; i < self.capacity; i++) {
						if (self.inUse[i] && self.slabLengths[i] > 0) {
							liveBytes += self.slabLengths[i];
						}
					}
			
					const deadBytes = self.slabTail - liveBytes;
					return deadBytes > self.slabTail * 0.5;
				},
			
				compact(): void {
					if (!self.needsCompaction()) return;
			
					const newSlab = new Uint8Array(self.slab.length);
					let newTail = 0;
			
					for (let i = 1; i < self.capacity; i++) {
						if (!self.inUse[i]) continue;
			
						const length = self.slabLengths[i];
						if (length <= 0) continue;
			
						const oldOffset = self.slabOffsets[i];
						newSlab.set(
							self.slab.subarray(oldOffset, oldOffset + length),
							newTail,
						);
			
						self.slabOffsets[i] = newTail;
						newTail += length;
					}
			
					// Copy back into original slab buffer.
					self.slab.set(newSlab.subarray(0, newTail), 0);
					self.slabTail = newTail;
				},
			
				stats(): {
					capacity: number;
					allocated: number;
					freeSlots: number;
					slabUsed: number;
					slabCapacity: number;
					slabUtilization: number;
				} {
					return {
						capacity: self.capacity,
						allocated: self.allocated,
						freeSlots: self.capacity - self.allocated,
						slabUsed: self.slabTail,
						slabCapacity: self.slab.length,
						slabUtilization: self.slabTail / self.slab.length,
					};
				},
			
				validate(): string[] {
					const errors: string[] = [];
			
					let freeCount = 0;
					let current = self.freeHead;
					const seen = new Set<number>();
			
					while (current !== -1) {
						if (current < 1 || current >= self.capacity) {
							errors.push(`Free list contains invalid slot: ${current}`);
							break;
						}
			
						if (seen.has(current)) {
							errors.push(`Free list contains cycle at slot: ${current}`);
							break;
						}
			
						seen.add(current);
						freeCount++;
						current = self.nextFree[current];
					}
			
					if (freeCount !== self.capacity - self.allocated) {
						errors.push(
							`Free list has ${freeCount} entries, expected ${self.capacity - self.allocated}`,
						);
					}
			
					for (let i = 1; i < self.capacity; i++) {
						if (!self.inUse[i]) continue;
			
						const offset = self.slabOffsets[i];
						const length = self.slabLengths[i];
			
						if (offset === -1 && length !== 0) {
							errors.push(`Slot ${i}: offset -1 but length ${length}`);
						} else if (offset !== -1) {
							if (offset < 0 || offset + length > self.slabTail) {
								errors.push(
									`Slot ${i}: slab range [${offset}, ${offset + length}) out of bounds (tail=${self.slabTail})`,
								);
							}
						}
					}
			
					return errors;
				},
			
				get used(): N {
					return self.allocated - 1 - self.freeCount();
				},
			
				get freeCount(): N {
					return self.freeHead === FREE_LIST_END ? 0 : self.walkFree();
				},
			
				get slabUsed(): N {
					return self.liveBytes;
				},
			
				get slabFree(): N {
					return self.slab.length - self.liveBytes;
				},
			
				walkFree(): N {
					let n = 0;
					let p = self.freeHead;
			
					while (p !== FREE_LIST_END) {
						n++;
						p = self.nextFree[p];
					}
			
					return n;
				},
			}));
			z.DRS = this.ATOM.MASTER("DRS_ATOM", <X = U>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				value: config.value as X,
			
			  
			
				destroy(): V {
					self.registry?.purgeAll();
					self.registry = null;
					BC().store.particle(self.s, self.m, self.r).delete(self.key)._REGISTRY_PROTON;
				},
			
				read(): X {
					const r = self.registry;
					if (r !== null) {
						const gate = r.get(GATE_ELECTRON);
						if (gate !== undefined && (gate as any).sealed) {
							throw new SEALED_ERROR_ELECTRON(self.key);
						}
					}
					return self.value;
				},
			
				write(next: X): B {
					const r = self.registry;
					if (r === null) {
						self.value = next;
						return true;
					}
			
					const gate = r.get(GATE_ELECTRON);
					if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
			
					const previous = self.value;
					if (Object.is(previous, next)) return false;
			
					self.value = next;
			
					const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
					if (subs !== undefined) {
						subs.notify({ key: self.key, previous, current: next });
					}
			
					return true;
				},
			
				subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
					const subs =
						(self.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
						self.bind(new SUBSCRIPTION_ELECTRON<X>(self));
			
					return subs.subscribe(listener);
				},
			
				waitFor() {},
			}));
			z.SUB = this.ATOM.MASTER("SUB_ATOM", <T = any>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				pipeline: undefined as PIPELINE<MQ_CTX<T>> | undefined,
			
				send(message: T, signal?: SIGNAL): B {
					const ctx: MQ_CTX<T> = {
						atom: self,
						message,
						signal,
						meta: undefined,
					};
			
					return self.dispatch("beforeSend", ctx, () => {
						self.deliver(ctx);
						return true;
					});
				},
			
				dispatch(hook: HOOK, ctx: MQ_CTX<T>, terminal: () => V): V {
					const p = self.pipeline ??= self.compile();
					return p.run(hook, ctx, terminal);
				},
			
				deliver(ctx: MQ_CTX<T>): V {
					// bare transport / queue / subscriber fanout
				},
			
				compile(): PIPELINE<MQ_CTX<T>> {
					const r = self.registry;
					const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];
			
					if (r !== null) {
						for (const shard of r.shards()) {
							if ("behaviours" in shard) {
								behaviours.push(...(shard as any).behaviours());
							}
						}
					}
			
					return compilePipeline(behaviours);
				},
			}));
			z.STREAM = this.ATOM.MASTER("STREAM_ATOM", <T = any>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				pipeline: undefined as PIPELINE<STREAM_CTX<T>> | undefined,
			
				send(message: T, signal?: SIGNAL): B {
					const ctx: STREAM_CTX<T> = {
						atom: self,
						message,
						signal,
						meta: undefined,
					};
			
					return self.dispatch("beforeSend", ctx, () => {
						self.deliver(ctx);
						return true;
					});
				},
			
				dispatch(hook: HOOK, ctx: STREAM_CTX<T>, terminal: () => V): V {
					const p = self.pipeline ??= self.compile();
					return p.run(hook, ctx, terminal);
				},
			
				deliver(ctx: STREAM_CTX<T>): V {
					// bare transport / queue / subscriber fanout
				},
			
				compile(): PIPELINE<STREAM_CTX<T>> {
					const r = self.registry;
					const behaviours: BEHAVIOUR<STREAM_CTX<T>>[] = [];
			
					if (r !== null) {
						for (const shard of r.shards()) {
							if ("behaviours" in shard) {
								behaviours.push(...(shard as any).behaviours());
							}
						}
					}
			
					return compilePipeline(behaviours);
				},
			}));
			z.SPREADSHEET = this.ATOM.MASTER("SPREADSHEET_ATOM", <X = U>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				key: config.key,
				value: config.value as X,
				registry: null as REGISTRY_PROTON | null,
			
				bind<S extends SPREADSHEET_ATOM>(atom: S, owner: OWNERSHIP = "bespoke"): S {
					if (self.registry === null) {
						self.registry = new REGISTRY_PROTON(self.key, { atoms: [{ atom, owner }] });
					} else {
						self.registry.bind(atom, owner);
					}
					return atom;
				},
			
				shard<S extends SPREADSHEET_ATOM>(ctor: ATOM_CLASS<S>): S | UD {
					return self.registry?.get(ctor);
				},
			
				unbind(ctor: ATOM_CLASS<any>): B {
					if (self.registry === null) return false;
			
					const removed = self.registry.unbind(ctor);
			
					// A registry with nothing in it is dropped, returning the cell to the bare fast path.
					if (self.registry.size === 0) {
						BC().store.particle(self.s, self.m, self.r).delete(self.key)._REGISTRY_PROTON;
						self.registry = null;
					}
			
					return removed;
				},
			
				destroy(): V {
					self.registry?.purgeAll();
					self.registry = null;
					BC().store.particle(self.s, self.m, self.r).delete(self.key)._REGISTRY_PROTON;
				},
			
				read(): X {
					const r = self.registry;
					if (r !== null) {
						const gate = r.get(GATE_ELECTRON);
						if (gate !== undefined && (gate as any).sealed) {
							throw new SEALED_ERROR_ELECTRON(self.key);
						}
					}
					return self.value;
				},
			
				write(next: X): B {
					const r = self.registry;
					if (r === null) {
						self.value = next;
						return true;
					}
			
					const gate = r.get(GATE_ELECTRON);
					if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
			
					const previous = self.value;
					if (Object.is(previous, next)) return false;
			
					self.value = next;
			
					const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
					if (subs !== undefined) {
						subs.notify({ key: self.key, previous, current: next });
					}
			
					return true;
				},
			
				subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
					const subs =
						(self.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
						self.bind(new SUBSCRIPTION_ELECTRON<X>(self));
			
					return subs.subscribe(listener);
				},
			
				waitFor() {},
			
				kind: undefined as SY,
				purge(): V {}, 
			}));
			z.SEND = this.ATOM.MASTER("SEND_ATOM", <T = any>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				pipeline: undefined as PIPELINE<MQ_CTX<T>> | undefined,
			
				send(message: T, signal?: SIGNAL): B {
					const ctx: MQ_CTX<T> = {
						atom: self,
						message,
						signal,
						meta: undefined,
					};
			
					return self.dispatch("beforeSend", ctx, () => {
						self.deliver(ctx);
						return true;
					});
				},
			
				dispatch(hook: HOOK, ctx: MQ_CTX<T>, terminal: () => V): V {
					const p = self.pipeline ??= self.compile();
					return p.run(hook, ctx, terminal);
				},
			
				deliver(ctx: MQ_CTX<T>): V {
					// bare transport / queue / subscriber fanout
				},
			
				compile(): PIPELINE<MQ_CTX<T>> {
					const r = self.registry;
					const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];
			
					if (r !== null) {
						for (const shard of r.shards()) {
							if ("behaviours" in shard) {
								behaviours.push(...(shard as any).behaviours());
							}
						}
					}
			
					return compilePipeline(behaviours);
				},
			}));
			z.CLOUD = this.ATOM.MASTER("CLOUD_ATOM", <X = U>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				value: config.value as X,
				registry: null as REGISTRY_PROTON | null,
			
				bind: <S extends ATOM>(shard: S, owner: OWNERSHIP = "bespoke"): S => {
					if (self.registry === null) {
						self.registry = new REGISTRY_PROTON(self.key, {
							shards: [{ shard, owner }],
						});
					} else {
						self.registry.bind(shard, owner);
					}
			
					return shard;
				},
			
				shard: <S extends ATOM>(ctor: ATOM_CLASS<S>): S | UD =>
					self.registry?.get(ctor),
			
				unbind: (ctor: SHARD_CLASS<any>): B => {
					if (self.registry === null) return false;
			
					const removed = self.registry.unbind(ctor);
			
					if (self.registry.size === 0) {
						BC().store.particle(self.s, self.m, self.r)
							.delete(self.k)._REGISTRY_PROTON;
			
						self.registry = null;
					}
			
					return removed;
				},
			
				destroy: (): void => {
					self.registry?.purgeAll();
					self.registry = null;
			
					BC().store.particle(self.s, self.m, self.r)
						.delete(self.k)._REGISTRY_PROTON;
				},
			
				read: (): X => {
					const registry = self.registry;
			
					if (registry !== null) {
						const gate = registry.get(GATE_ELECTRON);
			
						if (gate !== undefined && (gate as any).sealed) {
							throw new SEALED_ERROR_ELECTRON(self.k);
						}
					}
			
					return self.value;
				},
			
				write: (next: X): B => {
					const registry = self.registry;
			
					if (registry === null) {
						self.value = next;
						return true;
					}
			
					const gate = registry.get(GATE_ELECTRON);
			
					if (gate !== undefined && (gate.sealed || gate.frozen)) {
						return false;
					}
			
					const previous = self.value;
			
					if (Object.is(previous, next)) return false;
			
					self.value = next;
			
					const subs = registry.get(SUBSCRIPTION_ELECTRON) as
						SUBSCRIPTION_ELECTRON<X> | UD;
			
					if (subs !== undefined) {
						subs.notify({
							key: self.k,
							previous,
							current: next,
						});
					}
			
					return true;
				},
			
				subscribe: (listener: LISTENER<X>): UNSUBSCRIBE => {
					const subs =
						(self.shard(SUBSCRIPTION_ELECTRON) as
							SUBSCRIPTION_ELECTRON<X> | UD) ??
						self.bind(new SUBSCRIPTION_ELECTRON<X>(self));
			
					return subs.subscribe(listener);
				},
			
				waitFor: () => {},
			}));
			z.DUPLEX = this.ATOM.MASTER("DUPLEX", <T>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				pipeline: undefined as PIPELINE<MQ_CTX<T>> | undefined,
			
				send: (message: T, signal?: ATOM): B => {
					const ctx: MQ_CTX<T> = {
						atom: self,
						message,
						signal,
						meta: undefined,
					};
			
					return self.dispatch("beforeSend", ctx, () => {
						self.deliver(ctx);
						return true;
					});
				},
			
				dispatch: (
					hook: HOOK,
					ctx: MQ_CTX<T>,
					terminal: () => V,
				): V => {
					const p = self.pipeline ??= self.compile();
					return p.run(hook, ctx, terminal);
				},
			
				deliver: (ctx: MQ_CTX<T>): V => {
					// bare transport / queue / subscriber fanout
				},
			
				compile: (): PIPELINE<MQ_CTX<T>> => {
					const registry = self.registry;
					const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];
			
					if (registry !== null) {
						for (const shard of registry.shards()) {
							if ("behaviours" in shard) {
								behaviours.push(...(shard as any).behaviours());
							}
						}
					}
			
					return compilePipeline(behaviours);
				},
			}));
			z.MODULE = this.ATOM.MASTER("MODULE_ATOM", <T = any>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				initial: undefined as T,
				key: config.k ?? "__SIGNALS__",
				module: undefined as T,
				module_type: "class_fn_pool",
				version: 0,
				sealed: false,
				frozen: true,
				hasWritten: false,
				persisted: false,
			
				listeners: new Set<(change: Change<T>) => void>(),
				waiters: new Set<(value: T) => void>(),
				undoStack: undefined as HistoryEntry<T>[] | undefined,
			
				conflict: undefined as ConflictPolicy | undefined,
				historyIndex: 0,
				history: undefined as AtomHistoryEntry<T>[] | undefined,
				historyMax: 50,
				lifecycle: undefined as SignalLifecycle | undefined,
				async: undefined as AsyncSignalState<T> | undefined,
				metrics: undefined as Metrics | undefined,
				conflictStrategy: undefined as "latest" | "fail" | "accumulate" | undefined,
			
				schema: undefined as SignalSchema | undefined,
				contracts: new Map<string, AtomContract<T>>(),
				causal: [] as CausalEntry[],
				lastWriteSource: undefined as string | undefined,
			
				batches: new Set<SignalKey>(),
				batchQueue: new Map<SignalKey, unknown>(),
				batchOpts: new Map<SignalKey, { triggerRender?: boolean }>(),
			
				broadcast: true,
				triggerRender: true,
				boundary: "same-app" as Boundary,
			
				initialize: (
					initial: T,
					options: MODULE_OPTIONS = {
						module_type: "class_fn_pool",
						broadcast: true,
						frozen: true,
					},
				): void => {
					const ptype = options?.module_type
						? options.module_type
						: self.module_type;
					const shouldFreeze = options.frozen ?? self.frozen;
			
					switch (ptype) {
						case "fn":
						case "comp":
						case "node_module":
							self.frozen = shouldFreeze;
							self.module = initial;
							self.initial = initial;
							break;
			
						case "class_fn_pool":
						default:
							self.frozen = false;
							self.module = { ...initial };
							self.initial = { ...initial };
							break;
					}
			
					self.module_type = ptype;
			
					if (options?.broadcast) self.broadcast = options.broadcast;
					if (options?.module_type) self.module_type = options.module_type;
					if (options?.schema) self.schema = options.schema;
					if (options?.lifecycle) self.lifecycle = options.lifecycle;
					if (options?.conflict) self.conflict = options.conflict;
					if (options?.conflictStrategy) self.conflictStrategy = options.conflictStrategy;
			
					if (options?.history) {
						self.historyMax = Math.max(1, options.history.max ?? 50);
						self.history = [{
							k: 0,
							value: initial,
							timestamp: Date.now(),
							source: "initial",
						}];
						self.historyIndex = 0;
					}
				},
			
				read: (): T => self.module,
			
				peek: (): T => self.module,
			
				write: (
					incoming: any | ((prev: T) => T),
					options: AtomWriteOptions = {},
				): T => {
					if (self.frozen) return self.peek();
			
					const prev = self.module;
					const resolved = incoming;
			
					const next = self.module_type === "class_fn_pool"
						? resolved
							? { ...prev, ...resolved }
							: prev
						: resolved;
			
					self.module = next;
					self.hasWritten = true;
					self.version += 1;
					self.lastWriteSource = options.source ?? "write";
					self.assertContracts(next, prev);
			
					for (const l of self.listeners) {
						try {
							l(next);
						} catch (e) {}
					}
			
					root.bus.send(self.key, next, self.s, self.m, self.r);
			
					for (const w of self.waiters) {
						try {
							w(next);
						} catch (e) {}
					}
			
					if (options.recordHistory !== false) {
						self.recordHistory(prev, next, self.lastWriteSource);
					}
			
					self.recordCausal(next, self.lastWriteSource, options.cause);
					self.waiters.clear();
			
					self.notifyListeners({
						key: self.key,
						previous: prev,
						current: next,
						version: self.version,
						timestamp: Date.now(),
						source: self.lastWriteSource,
					});
			
					return next;
				},
			
				register: (name: string, val: any): T =>
					self.write((prev: A) => ({ ...prev, [name]: val })),
			
				unregister: (name: string): T =>
					self.write((prev: A) => {
						const { [name]: _gone, ...rest } = prev;
						return rest;
					}),
			
				slot: (name: string): any => self.module[name],
			
				on: (listener: RiftListener<any>): void => {
					self.listeners.add(listener);
					listener(self.module);
				},
			
				waitFor: (): Promise<T> =>
					new Promise((resolve) => {
						self.waiters.add(resolve);
					}),
			
				waitForSlot: async (name: string, timeoutMs = 0): Promise<any> => {
					const existing = self.module[name];
					if (existing !== undefined) return existing;
			
					if (!timeoutMs) {
						const next = await self.waitFor();
						return (next as Record<string, any>)[name];
					}
			
					return Promise.race([
						self.waitFor().then(
							(next) => (next as Record<string, any>)[name],
						),
						new Promise((_, reject) =>
							setTimeout(
								() => reject(
									new Error(
										`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}m`,
									),
								),
								timeoutMs,
							),
						),
					]);
				},
			
				inspect: (): T => ({ ...self.module }),
			
				setSchema: (schema?: SignalSchema) => {
					self.schema = schema;
					return self;
				},
			
				setLifecycle: (lifecycle?: SignalLifecycle<T>) => {
					self.lifecycle = lifecycle;
					return self;
				},
			
				setConflict: (
					conflict?: ConflictPolicy,
					strategy?: "latest" | "fail" | "accumulate",
				) => {
					self.conflict = conflict;
					self.conflictStrategy = strategy;
					return self;
				},
			
				setFrozen: (): void => {
					self.frozen = true;
				},
			
				setUnfrozen: (): void => {
					self.frozen = false;
				},
			
				setSealed: (): void => {
					self.sealed = true;
				},
			
				setUnsealed: (): void => {
					self.sealed = false;
				},
			
				setBoundary: (): void => {},
			
				validate: (next: T): T => {
					if (!self.schema) return next;
					return root.signals._.validateSchema(self.schema, next);
				},
			
				assertContracts: (next: T, previous: T): void => {
					for (const contract of self.contracts.values()) {
						let valid = false;
			
						try {
							valid = contract.postcondition(next, previous);
						} catch (error) {
							console.error(
								`[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`,
								error,
							);
						}
			
						if (valid) continue;
			
						const message =
							`[Sauron] contract "${contract.name}" violated ` +
							`on write to "${String(self.key)}"`;
			
						if (contract.block) {
							throw new Error(message);
						}
			
						console.error(message);
					}
				},
			
				recordHistory: (previous: T, next: T, source?: string): void => {
					if (!self.history) return;
			
					self.history.splice(self.historyIndex + 1);
			
					const last = self.history[self.history.length - 1];
			
					self.history.push({
						k: (last?.k ?? -1) + 1,
						prev: previous,
						value: next,
						next,
						timestamp: Date.now(),
						source,
						undone: false,
					});
			
					while (self.history.length > self.historyMax) {
						self.history.shift();
					}
			
					self.historyIndex = self.history.length - 1;
				},
			
				getHistory: (): readonly AtomHistoryEntry<T>[] =>
					self.history ?? [],
			
				timeTravel: (steps = 1): T | undefined => {
					if (!self.history?.length || steps <= 0) return undefined;
			
					const idx = Math.max(
						0,
						Math.min(self.history.length - 1, self.historyIndex + steps),
					);
			
					if (idx === self.historyIndex) return undefined;
			
					self.historyIndex = idx;
					return self.replayHistory("time-travel");
				},
			
				back: (steps = 1): T | undefined =>
					self.timeTravel(-Math.abs(steps)),
			
				forward: (steps = 1): T | undefined =>
					self.timeTravel(Math.abs(steps)),
			
				goto: (sequence: number): T | undefined => {
					if (!self.history?.length) return undefined;
			
					const idx = self.history.findIndex(
						(entry) => entry.k === sequence,
					);
			
					if (idx < 0 || idx === self.historyIndex) return undefined;
			
					self.historyIndex = idx;
					return self.replayHistory("time-travel.goto");
				},
			
				replayHistory: (source: string): T => {
					const entry = self.history![self.historyIndex];
			
					return self.write(entry.value as T, {
						source,
						cause: "time-travel",
						triggerRender: true,
						recordHistory: false,
					});
				},
			
				recordCausal: (
					value: T,
					source?: string,
					cause?: string,
				): void => {
					self.causal.push({
						value,
						source,
						cause,
						timestamp: Date.now(),
					} as CausalEntry);
			
					if (self.causal.length > 100) {
						self.causal.shift();
					}
				},
			
				recordMetrics: (): void => {
					if (!self.metrics) return;
			
					self.metrics.writeCount += 1;
					self.metrics.lastWrite = Date.now();
				},
				
				notifyListeners: (change: Change<T>): void => {
					const listeners = root.wr._listeners.get(self.key);
					if (!listeners) return;
			
					for (const listener of listeners) {
						try {
							listener(change);
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`,
								error,
							);
						}
					}
				},
			
				resolveWaiters: (value: T): void => {
					const waiting = root.wr._waiters.get(self.key);
					if (!waiting) return;
			
					root.wr._waiters.delete(self.key);
			
					for (const resolve of waiting) {
						try {
							resolve(value);
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`,
								error,
							);
						}
					}
				},
			
				notifyExternalSubscribers: (): void => {
					const subscribers = root.state._externalSubscribers.get(self.key);
					if (!subscribers) return;
			
					for (const callback of subscribers) {
						try {
							callback();
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`,
								error,
							);
						}
					}
				},
			}));
			z.MODULE_STORAGE = this.ATOM.MASTER(
				"MODULE_STORAGE_PROTON",
				({ self, master, config }: any) => ({
					k: config.k ?? "__SIGNALS__",
					s: config.s ?? "SAURON_SLAVE_REG",
					m: config.m ?? "SAURON_MASTER_REG",
					r: config.r ?? "THE_WIZARD_ROOT_REG",
			
					slots: new Map<S, MODULE_SLOT>(),
			
					addr: (
						k: S = self.k,
						s: S = self.s,
						c?: CACHE,
						m: S = self.m,
						r: S = self.r,
					): S =>
						c !== undefined
							? `[${r}][${m}][${c}][${s}][${k}]` as S
							: `[${r}][${m}][${s}][${k}]` as S,
			
					set: (
						value: MDV,
						k: S,
						s: S,
						c: S,
						m: S,
						r: S,
						options: {
							owner?: OWNERSHIP;
							onUnload?: (v: MDV) => V;
						} = {},
					): MDV => {
						const key = self.addr(k, s, c, m, r);
						const prev = self.slots.get(key);
			
						prev?.onUnload?.(prev.value);
			
						self.slots.set(key, {
							value,
							registeredAt: Date.now(),
							owner: options.owner ?? "bespoke",
							onUnload: options.onUnload,
						});
			
						return value;
					},
			
					get: (
						k: S,
						s: S,
						m: S,
						r: S,
					): MODULE_VALUE | undefined =>
						self.slots.get(self.addr(k, s, undefined, m, r))?.value,
			
					has: (
						k: S,
						s: S,
						m: S,
						r: S,
					): B =>
						self.slots.has(self.addr(k, s, undefined, m, r)),
			
					drop: (
						k: S,
						s: S,
						m: S,
						r: S,
						force: B = false,
					): B => {
						const key = self.addr(k, s, undefined, m, r);
						const slot = self.slots.get(key);
			
						if (!slot) return false;
						if (slot.owner === "shared" && !force) return false;
			
						slot.onUnload?.(slot.value);
						self.slots.delete(key);
			
						return true;
					},
			
					list: (
						s: S,
						m: S,
						r: S,
					): Array<{ k: S; owner: OWNERSHIP; registeredAt: N }> => {
						const prefix = `[${r}][${m}][${s}][`;
						const out: Array<{
							k: S;
							owner: OWNERSHIP;
							registeredAt: N;
						}> = [];
			
						for (const [key, slot] of self.slots) {
							if (key.startsWith(prefix)) {
								out.push({
									k: key.slice(0, -1).slice(prefix.length) as S,
									owner: slot.owner,
									registeredAt: slot.registeredAt,
								});
							}
						}
			
						return out;
					},
			
					inspect: (): Array<{ address: S; owner: OWNERSHIP; kind: S }> =>
						[...self.slots.entries()].map(([address, slot]) => ({
							address,
							owner: slot.owner,
							kind: typeof slot.value === "function"
								? (slot.value.name || "anonymous-fn") as S
								: "object" as S,
						})),
			
					purge: (): void => {
						for (const slot of self.slots.values()) {
							slot.onUnload?.(slot.value);
						}
						self.slots.clear();
					},
				}),
			);
			z.S_CLASS = this.ATOM.MASTER("S_CLASS_ATOM", <T = any>({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				initial: undefined as T,
				key: config.k ?? "__SIGNALS__",
				module: undefined as T,
				module_type: "class_fn_pool",
				version: 0,
				sealed: false,
				frozen: true,
				hasWritten: false,
				persisted: false,
			
				listeners: new Set<(change: Change<T>) => void>(),
				waiters: new Set<(value: T) => void>(),
				undoStack: undefined as HistoryEntry<T>[] | undefined,
			
				conflict: undefined as ConflictPolicy | undefined,
				historyIndex: 0,
				history: undefined as AtomHistoryEntry<T>[] | undefined,
				historyMax: 50,
				lifecycle: undefined as SignalLifecycle | undefined,
				async: undefined as AsyncSignalState<T> | undefined,
				metrics: undefined as Metrics | undefined,
				conflictStrategy: undefined as "latest" | "fail" | "accumulate" | undefined,
			
				schema: undefined as SignalSchema | undefined,
				contracts: new Map<string, AtomContract<T>>(),
				causal: [] as CausalEntry[],
				lastWriteSource: undefined as string | undefined,
			
				batches: new Set<SignalKey>(),
				batchQueue: new Map<SignalKey, unknown>(),
				batchOpts: new Map<SignalKey, { triggerRender?: boolean }>(),
			
				broadcast: true,
				triggerRender: true,
				boundary: "same-app" as Boundary,
			
				initialize: (
					initial: T,
					options: MODULE_OPTIONS = {
						module_type: "class_fn_pool",
						broadcast: true,
						frozen: true,
					},
				): void => {
					const ptype = options?.module_type
						? options.module_type
						: self.module_type;
					const shouldFreeze = options.frozen ?? self.frozen;
			
					switch (ptype) {
						case "fn":
						case "comp":
						case "node_module":
							self.frozen = shouldFreeze;
							self.module = initial;
							self.initial = initial;
							break;
			
						case "class_fn_pool":
						default:
							self.frozen = false;
							self.module = { ...initial };
							self.initial = { ...initial };
							break;
					}
			
					self.module_type = ptype;
			
					if (options?.broadcast) self.broadcast = options.broadcast;
					if (options?.module_type) self.module_type = options.module_type;
					if (options?.schema) self.schema = options.schema;
					if (options?.lifecycle) self.lifecycle = options.lifecycle;
					if (options?.conflict) self.conflict = options.conflict;
					if (options?.conflictStrategy) self.conflictStrategy = options.conflictStrategy;
			
					if (options?.history) {
						self.historyMax = Math.max(1, options.history.max ?? 50);
						self.history = [{
							k: 0,
							value: initial,
							timestamp: Date.now(),
							source: "initial",
						}];
						self.historyIndex = 0;
					}
				},
			
				read: (): T => self.module,
			
				peek: (): T => self.module,
			
				write: (
					incoming: any | ((prev: T) => T),
					options: AtomWriteOptions = {},
				): T => {
					if (self.frozen) return self.peek();
			
					const prev = self.module;
					const resolved = incoming;
			
					const next = self.module_type === "class_fn_pool"
						? resolved
							? { ...prev, ...resolved }
							: prev
						: resolved;
			
					self.module = next;
					self.hasWritten = true;
					self.version += 1;
					self.lastWriteSource = options.source ?? "write";
					self.assertContracts(next, prev);
			
					for (const l of self.listeners) {
						try {
							l(next);
						} catch (e) {}
					}
			
					root.bus.send(self.key, next, self.s, self.m, self.r);
			
					for (const w of self.waiters) {
						try {
							w(next);
						} catch (e) {}
					}
			
					if (options.recordHistory !== false) {
						self.recordHistory(prev, next, self.lastWriteSource);
					}
			
					self.recordCausal(next, self.lastWriteSource, options.cause);
					self.waiters.clear();
			
					self.notifyListeners({
						key: self.key,
						previous: prev,
						current: next,
						version: self.version,
						timestamp: Date.now(),
						source: self.lastWriteSource,
					});
			
					return next;
				},
			
				register: (name: string, val: any): T =>
					self.write((prev: A) => ({ ...prev, [name]: val })),
			
				unregister: (name: string): T =>
					self.write((prev: A) => {
						const { [name]: _gone, ...rest } = prev;
						return rest;
					}),
			
				slot: (name: string): any => self.module[name],
			
				on: (listener: RiftListener<any>): void => {
					self.listeners.add(listener);
					listener(self.module);
				},
			
				waitFor: (): Promise<T> =>
					new Promise((resolve) => {
						self.waiters.add(resolve);
					}),
			
				waitForSlot: async (name: string, timeoutMs = 0): Promise<any> => {
					const existing = self.module[name];
					if (existing !== undefined) return existing;
			
					if (!timeoutMs) {
						const next = await self.waitFor();
						return (next as Record<string, any>)[name];
					}
			
					return Promise.race([
						self.waitFor().then(
							(next) => (next as Record<string, any>)[name],
						),
						new Promise((_, reject) =>
							setTimeout(
								() => reject(
									new Error(
										`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}m`,
									),
								),
								timeoutMs,
							),
						),
					]);
				},
			
				inspect: (): T => ({ ...self.module }),
			
				setSchema: (schema?: SignalSchema) => {
					self.schema = schema;
					return self;
				},
			
				setLifecycle: (lifecycle?: SignalLifecycle<T>) => {
					self.lifecycle = lifecycle;
					return self;
				},
			
				setConflict: (
					conflict?: ConflictPolicy,
					strategy?: "latest" | "fail" | "accumulate",
				) => {
					self.conflict = conflict;
					self.conflictStrategy = strategy;
					return self;
				},
			
				setFrozen: (): void => {
					self.frozen = true;
				},
			
				setUnfrozen: (): void => {
					self.frozen = false;
				},
			
				setSealed: (): void => {
					self.sealed = true;
				},
			
				setUnsealed: (): void => {
					self.sealed = false;
				},
			
				setBoundary: (): void => {},
			
				validate: (next: T): T => {
					if (!self.schema) return next;
					return root.signals._.validateSchema(self.schema, next);
				},
			
				assertContracts: (next: T, previous: T): void => {
					for (const contract of self.contracts.values()) {
						let valid = false;
			
						try {
							valid = contract.postcondition(next, previous);
						} catch (error) {
							console.error(
								`[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`,
								error,
							);
						}
			
						if (valid) continue;
			
						const message =
							`[Sauron] contract "${contract.name}" violated ` +
							`on write to "${String(self.key)}"`;
			
						if (contract.block) {
							throw new Error(message);
						}
			
						console.error(message);
					}
				},
			
				recordHistory: (previous: T, next: T, source?: string): void => {
					if (!self.history) return;
			
					self.history.splice(self.historyIndex + 1);
			
					const last = self.history[self.history.length - 1];
			
					self.history.push({
						k: (last?.k ?? -1) + 1,
						prev: previous,
						value: next,
						next,
						timestamp: Date.now(),
						source,
						undone: false,
					});
			
					while (self.history.length > self.historyMax) {
						self.history.shift();
					}
			
					self.historyIndex = self.history.length - 1;
				},
			
				getHistory: (): readonly AtomHistoryEntry<T>[] =>
					self.history ?? [],
			
				timeTravel: (steps = 1): T | undefined => {
					if (!self.history?.length || steps <= 0) return undefined;
			
					const idx = Math.max(
						0,
						Math.min(self.history.length - 1, self.historyIndex + steps),
					);
			
					if (idx === self.historyIndex) return undefined;
			
					self.historyIndex = idx;
					return self.replayHistory("time-travel");
				},
			
				back: (steps = 1): T | undefined =>
					self.timeTravel(-Math.abs(steps)),
			
				forward: (steps = 1): T | undefined =>
					self.timeTravel(Math.abs(steps)),
			
				goto: (sequence: number): T | undefined => {
					if (!self.history?.length) return undefined;
			
					const idx = self.history.findIndex(
						(entry) => entry.k === sequence,
					);
			
					if (idx < 0 || idx === self.historyIndex) return undefined;
			
					self.historyIndex = idx;
					return self.replayHistory("time-travel.goto");
				},
			
				replayHistory: (source: string): T => {
					const entry = self.history![self.historyIndex];
			
					return self.write(entry.value as T, {
						source,
						cause: "time-travel",
						triggerRender: true,
						recordHistory: false,
					});
				},
			
				recordCausal: (
					value: T,
					source?: string,
					cause?: string,
				): void => {
					self.causal.push({
						value,
						source,
						cause,
						timestamp: Date.now(),
					} as CausalEntry);
			
					if (self.causal.length > 100) {
						self.causal.shift();
					}
				},
			
				recordMetrics: (): void => {
					if (!self.metrics) return;
			
					self.metrics.writeCount += 1;
					self.metrics.lastWrite = Date.now();
				},
			
				notifyListeners: (change: Change<T>): void => {
					const listeners = root.wr._listeners.get(self.key);
					if (!listeners) return;
			
					for (const listener of listeners) {
						try {
							listener(change);
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`,
								error,
							);
						}
					}
				},
			
				resolveWaiters: (value: T): void => {
					const waiting = root.wr._waiters.get(self.key);
					if (!waiting) return;
			
					root.wr._waiters.delete(self.key);
			
					for (const resolve of waiting) {
						try {
							resolve(value);
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`,
								error,
							);
						}
					}
				},
			
				notifyExternalSubscribers: (): void => {
					const subscribers = root.state._externalSubscribers.get(self.key);
					if (!subscribers) return;
			
					for (const callback of subscribers) {
						try {
							callback();
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`,
								error,
							);
						}
					}
				},
			}));
			z.S_CLASS_STORAGE = this.ATOM.MASTER(
				"S_CLASS_STORAGE_PROTON",
				({ self, master, config }: any) => ({
					k: config.k ?? "__SIGNALS__",
					s: config.s ?? "SAURON_SLAVE_REG",
					m: config.m ?? "SAURON_MASTER_REG",
					r: config.r ?? "THE_WIZARD_ROOT_REG",
			
					store: new Map<string, Record<string, any>>(),
					addr: (
						k: string,
						sl: string,
						ms: string,
						rt: string,
					): string => `[${rt}][${ms}][${sl}][${k}]`,
			
					set: (
						k: string,
						incoming: Record<string, any> | undefined,
						sl: string,
						ms: string,
						rt: string,
						bus: any,
					): Record<string, any> => {
						const key = self.addr(k, sl, ms, rt);
						const prev = self.store.get(key) ?? {};
						const next = incoming ? { ...prev, ...incoming } : prev;
			
						self.store.set(key, next);
						bus.send.emit(k, next, sl, ms, rt);
			
						return next;
					},
			
					get: (
						k: string,
						sl: string,
						ms: string,
						rt: string,
					): Record<string, any> | undefined =>
						self.store.get(self.addr(k, sl, ms, rt)),
			
					has: (
						k: string,
						sl: string,
						ms: string,
						rt: string,
					): boolean => self.store.has(self.addr(k, sl, ms, rt)),
			
					inspect: (): Array<{ address: string; value: Record<string, any> }> =>
						[...self.store.entries()].map(([address, value]) => ({
							address,
							value,
						})),
			
					clear: (): void => {
						self.store.clear();
					},
			
					get size(): number {
						return self.store.size;
					},
				}),
			);
			z.ATOM = this.ATOM.MASTER('ATOM',({ self, config, gate, subscription, SEALED_ERROR, conflict }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
				value: config.value as U,
				read() {
					const r = self.value;
					if (r !== null) { if (GATE !== undefined && gate.sealed) throw new SEALED_ERROR.log(self.k); }
					return self.value;
				},
				write1(next: U): B {
					const r = self.registry;
					if (r === null) { self.value = next; return true; }
					if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
					const previous = self.value;
					if (Object.is(previous, next)) return false;
					self.value = next;
					if (subscription !== undefined) subscription.notify({ key: self.key, previous, current: next });
					return true;
				},
				write(next) {
					if (gate?.sealed || gate?.frozen) return false;
					const current = self.value;
					if (Object.is(current, next)) return false;
				  
					const decision = conflict.check({
					  current,
					  incoming: next,
					  previous: self.previous ?? current,
					});
				  
					if (!decision.accepted) {
					  return false;
					}
				  
					const resolved = decision.value;
				  
					if (Object.is(current, resolved)) return false;
				  
					self.useCell().set("value", resolved);
				  
					return true;
				  },
				subscribe(listener: LISTENER<A>): UNSUBSCRIBE { return subscription.subscribe(listener); },
				waitFor() {},
			}));
			z.VERTEX = this.ATOM.MASTER("VERTEX", ({ self,config, gate, subscription, SEALED_ERROR }: any) => ({
				value: config.value as U,
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
				pid: 1,
				slabRegistry: new Map<number, Uint8Array>(),
				...(() => {
					const capacity = (() => {
						const need = (value: U): N =>
							Array.isArray(value) ? value.length + 1 :
							value instanceof Map || value instanceof Set ? value.size + 1 : 2;
			
						const size = need(self.value);
			
						return size > 8192 ? 64 :
							size <= 64 ? 64 :
							size <= 128 ? 128 :
							size <= 256 ? 256 :
							size <= 1024 ? 1024 : 8192;
					})();
			
					const lane = new Int32Array(config.capacity ?? capacity).fill(-1);
					const ops = new Uint8Array(config.capacity ?? capacity);
					const posX = new Float32Array(config.capacity ?? capacity);
					const posY = new Float32Array(config.capacity ?? capacity);
					const val = new Int32Array(config.capacity ?? capacity);
					const freePids = new Int32Array(config.capacity ?? capacity);
			
					let freeHead = 0;
					let freeTail = 0;
					let laneLen = 0;
					let nextId = 0;
			
					const ids = new Map<S, N>();
					const names: S[] = [];
					const pids = new Map<S, N>();
					const addresses = new Map<N, [N, N, N, N]>();
					const listener: Array<ListenerFn | UD> = [];
					const pidToHash = new Uint32Array(config.capacity ?? capacity);
					const channelMap = new Int32Array(config.capacity ?? capacity);
					const subChannelMap = new Int32Array(config.capacity ?? capacity);
					const tripChannelMap = new Int32Array(config.capacity ?? capacity);
					const dirtyPids = new Int32Array(config.capacity ?? capacity);
			
					for (let i = 1; i < lane.length; i++) {
						freePids[freeTail++] = i;
					}
			
					lane[0] = TRACER_PID;
					val[TRACER_PID] = 0;
					ops[TRACER_PID] = OP.NONE;
					laneLen = 1;
			
					return {
						capacity: lane.length,
						lane,
						ops,
						posX,
						posY,
						val,
						freePids,
						freeHead,
						freeTail,
						laneLen,
						ids,
						names,
						pids,
						addresses,
						nextId,
						listener,
						pidToHash,
						nextAllocatedPid: 1,
						channelMap,
						subChannelMap,
						tripChannelMap,
						dirtyPids,
					};
				})(),
				need(value: U): N {        return Array.isArray(value) ? value.length + 1 : value instanceof Map || value instanceof Set ? value.size + 1 : 2;    },
				BINARY_CODEC: new BINARY_CODEC(),
				...(() => {
					const codec = new BINARY_CODEC();
			
					return {
						codec,
						encodeToSlab(pid: N, value: A): N {
							if (typeof value === "number") {
								self.slabRegistry.delete(pid);
								return value;
							}
			
							const bytes = codec.encode_msg(value);
							self.slabRegistry.set(pid, bytes);
							return bytes.byteLength;
						},
			
						clearSlab(pid: N): void {
							self.slabRegistry.delete(pid);
						},
			
						hasSlab(pid: N): B {
							return self.slabRegistry.has(pid);
						},
			
						readSlabValue<T>(pid: N): T | NU {
							const bytes = self.slabRegistry.get(pid);
							return bytes ? codec.decode_msg(bytes) as T : null;
						},
					};
				})(),
				...(() => {
					const codec = self.BINARY_CODEC;
					const x = self.posX;
					const y = self.posY;
					const pid = self.pid;
			
					const initialize = () => {
						const value = self.value;
						const v = codec.encode_msg(value);
						const position = self.getPosition(pid);
			
						if (self.need(value) > 8192) {
							self.encodeToSlab(pid, v);
							x[pid] = position.x;
							y[pid] = position.y;
							self.ops[pid] = OP.WRITE;
							self.lane[self.laneLen++] = pid;
						} else {
							const vb = codec.encode_msg(v);
							self.edit(position.x, position.y, vb, pid);
						}
					};
			
					return { initialize };
				})(),
				check_size_and_edit(value: A): V {
					const v = self.BINARY_CODEC.encode_msg(value);
					const { x, y } = self.getPosition(self.pid);
			
					if (self.need(value) > 8192) {
						self.encodeToSlab(self.pid, v);
						self.posX[self.pid] = x;
						self.posY[self.pid] = y;
						self.ops[self.pid] = OP.EDIT;
						self.lane[self.laneLen++] = self.pid;
					} else {
						self.create(x, y, v, self.pid);
					}
				},
				getX(pid: N): N { return self.posX[pid]; },
				getY(pid: N): N { return self.posY[pid]; },
				getPosition(pid: N): { x: N; y: N } { return { x: self.posX[pid], y: self.posY[pid] }; },
				create(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.WRITE; self.lane[self.laneLen++] = pid; },
				edit(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.EDIT; },
				subscribeValue(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SUB; },
				readValue(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.READ; },
				effect(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.EFFECT; },
				delete(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DELETE; },
				compact(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.COMPACT; },
				none(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.NONE; },
				send(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SEND; },
				sub(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SUB; },
				duplex(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DUPLEX; },
				stream(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.STREAM; },
				drc(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DRC; },
				reclaimPid(pid: N): V { self.posX[pid] = 0; self.posY[pid] = 0; self.val[pid] = 0; self.ops[pid] = OP.NONE; self.freePids[self.freeTail] = pid; self.freeTail = (self.freeTail + 1) % self.capacity; },
				pass(): void {
					const len = self.laneLen;
					let writeIdx = 0;
					let i = 0;
					const unrolledLimit = len - (len % 4);
			
					for (; i < unrolledLimit; i += 4) {
						let pid = self.lane[i];
			
						if (pid === TRACER_PID) {
							self.val[TRACER_PID]++;
							self.lane[writeIdx++] = pid;
						} else if (self.ops[pid] === OP.DELETE) {
							self.reclaimPid(pid);
						} else {
							self.ops[pid] = OP.NONE;
							self.lane[writeIdx++] = pid;
						}
			
						pid = self.lane[i + 1];
			
						if (pid === TRACER_PID) {
							self.val[TRACER_PID]++;
							self.lane[writeIdx++] = pid;
						} else if (self.ops[pid] === OP.DELETE) {
							self.reclaimPid(pid);
						} else {
							self.ops[pid] = OP.NONE;
							self.lane[writeIdx++] = pid;
						}
			
						pid = self.lane[i + 2];
			
						if (pid === TRACER_PID) {
							self.val[TRACER_PID]++;
							self.lane[writeIdx++] = pid;
						} else if (self.ops[pid] === OP.DELETE) {
							self.reclaimPid(pid);
						} else {
							self.ops[pid] = OP.NONE;
							self.lane[writeIdx++] = pid;
						}
			
						pid = self.lane[i + 3];
			
						if (pid === TRACER_PID) {
							self.val[TRACER_PID]++;
							self.lane[writeIdx++] = pid;
						} else if (self.ops[pid] === OP.DELETE) {
							self.reclaimPid(pid);
						} else {
							self.ops[pid] = OP.NONE;
							self.lane[writeIdx++] = pid;
						}
					}
			
					for (; i < len; i++) {
						const pid = self.lane[i];
			
						if (pid === TRACER_PID) {
							self.val[TRACER_PID]++;
							self.lane[writeIdx++] = pid;
						} else if (self.ops[pid] === OP.DELETE) {
							self.reclaimPid(pid);
						} else {
							self.ops[pid] = OP.NONE;
							self.lane[writeIdx++] = pid;
						}
					}
			
					self.laneLen = writeIdx;
				},
				get tick(): N { return self.val[TRACER_PID]; },
				get length(): N { return self.laneLen; },
				getLoadFactor(): N {
					const activeCount = Math.max(0, self.laneLen - 1);
					return activeCount / (self.capacity - 1);
				},
				read() {
					const r = self.registry;
					if (r !== null) {  if (gate !== undefined && gate.sealed)  throw new SEALED_ERROR.log(self.key);  }
					return self.value;
				},
				write(next: A): B {
					const r = self.registry;
					if (r === null) { self.value = next; return true; }
			
					if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
					const previous = self.value;
					if (Object.is(previous, next)) return false;
					self.value = next;
			 
					if (subscription !== undefined)  subscription.notify({ key: self.key, previous, current: next });
					return true;
				},
				subscribe(listener: LISTENER<A>): UNSUBSCRIBE {return subscription.subscribe(listener); },
				waitFor() {},
			})); 
			return z
		})(); 
		public ATOMS = (() => {
			const z = () => {}
			z.ADAPTER = this.ATOM.BIND("ADAPTER", ({ self, master, config }: any) => ({}));
			z.AW_SET = this.ATOM.BIND("AW_SET", ({ self, master, config }: any) => ({}));
			z.CASUAL_CURSOR = this.ATOM.BIND("CASUAL_CURSOR", ({ self, master, config }: any) => ({}));
			z.DELTA_STORE = this.ATOM.BIND("DELTA_STORE", ({ self, master, config }: any) => ({}));
			z.G_SET = this.ATOM.BIND("G_SET", ({ self, master, config }: any) => ({}));
			z.VERSION_VECTOR = this.ATOM.BIND("VERSION_VECTOR", ({ self, master, config }: any) => ({}));
			z.CIRCUIT_BREAKER = this.ATOM.BIND("CIRCUIT_BREAKER", ({ self, master, config }: any) => ({}));
			z.CQRS = this.ATOM.BIND("CQRS", ({ self, master, config }: any) => ({}));
			z.RAFT = this.ATOM.BIND("RAFT", ({ self, master, config }: any) => ({}));
			z.YJS = this.ATOM.BIND("YJS", ({ self, master, config }: any) => ({}));
			z.DIFFPATCH = this.ATOM.BIND("DIFFPATCH", ({ self, master, config }: any) => ({}));
			z.HYBRID_LOGICAL_CLOCK = this.ATOM.BIND("HYBRID_LOGICAL_CLOCK", ({ self, master, config }: any) => ({}));
			z.LWW = this.ATOM.BIND("LWW", ({ self, master, config }: any) => ({}));
			z.LWW_ELEMENTAL = this.ATOM.BIND("LWW_ELEMENTAL", ({ self, master, config }: any) => ({}));
			z.MV = this.ATOM.BIND("MV", ({ self, master, config }: any) => ({}));
			z.OPTIMISTIC = this.ATOM.BIND("OPTIMISTIC", ({ self, master, config }: any) => ({}));
			z.OR = this.ATOM.BIND("OR", ({ self, master, config }: any) => ({}));
			z.PN = this.ATOM.BIND("PN", ({ self, master, config }: any) => ({}));
			z.REVERSE_HEARTBEAT_MGR = this.ATOM.BIND("REVERSE_HEARTBEAT_MGR", ({ self, master, config }: any) => ({}));
			z.SEMAPHORE = this.ATOM.BIND("SEMAPHORE", ({ self, master, config }: any) => ({}));
			z.TOMBSTONE_COMPACTOR = this.ATOM.BIND("TOMBSTONE_COMPACTOR", ({ self, master, config }: any) => ({}));
			z.YIMIR = this.ATOM.BIND("YIMIR", ({ self, master, config }: any) => ({}));
			z.HYBRID_LOGICAL_CLOCK_CRDT = this.ATOM.BIND("HYBRID_LOGICAL_CLOCK_CRDT", ({ self, master, config }: any) => ({}));
			z.SOA_QUAD_TIERED_REGISTRY = this.ATOM.BIND("SOA_QUAD_TIERED_REGISTRY", ({ self, master, config }: any) => ({}));
			z.BINARY = this.ATOM.BIND("BINARY", ({ self, master, config }: any) => ({}));
			z.SIGNAL = this.ATOM.BIND("SIGNAL", ({ self, master, config }: any) => ({}));
			z.GOVERNOR = this.ATOM.BIND("GOVERNOR", ({ self, master, config }: any) => ({}));
			z.BYTE_CODE_VM = this.ATOM.BIND("BYTE_CODE_VM", ({ self, master, config }: any) => ({}));
			z.CHANNEL = this.ATOM.BIND("CHANNEL", ({ self, master, config }: any) => ({}));
			z.CHANNELS = this.ATOM.BIND("CHANNELS", ({ self, master, config }: any) => ({}));
			z.MIDDLEWARE = this.ATOM.BIND("MIDDLEWARE", ({ self, master, config }: any) => ({}));
			z.BEAM_CODEC = this.ATOM.BIND("BEAM_CODEC", ({ self, master, config }: any) => ({}));
			z.GOVERNER = this.ATOM.BIND("GOVERNER", ({ self, master, config }: any) => ({}));
			z.INJECTION_CODEC = this.ATOM.BIND("INJECTION_CODEC", ({ self, master, config }: any) => ({}));
			z.RUN_ON = this.ATOM.BIND("RUN_ON", ({ self, master, config }: any) => ({}));
			z.SEALED_ERROR = this.ATOM.BIND("SEALED_ERROR", ({ self, master, config }: any) => ({}));
			z.SHELL = this.ATOM.BIND("SHELL", ({ self, master, config }: any) => ({}));
			z.TRANSFORM = this.ATOM.BIND("TRANSFORM", ({ self, master, config }: any) => ({}));
			z.CONFLICT = this.ATOM.BIND("CONFLICT", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				conflictStrategy: (config.conflictStrategy ?? "latest") as "latest" | "fail" | "accumulate",
				conflict: config.conflict,
			
				resolve<T>(current: T, incoming: T): T {
					switch (self.conflictStrategy) {
						case "fail":
							throw new Error("SOUL_NEUTRON conflict: concurrent values cannot be merged");
			
						case "accumulate":
							if (typeof current === "number" && typeof incoming === "number") return (current + incoming) as T;
							if (Array.isArray(current) && Array.isArray(incoming)) return [...current, ...incoming] as T;
							if (typeof current === "string" && typeof incoming === "string") return (current + incoming) as T;
							throw new TypeError("SOUL_NEUTRON accumulate conflict requires matching number, string, or array values");
			
						case "latest":
						default:
							return incoming;
					}
				},
			}));
			z.CONTEXT = this.ATOM.BIND("CONTEXT", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				store: new Map<S, A>(),
			
				set(key: S, value: A): void {
					self.store.set(key, value);
					// if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('context_set', undefined, undefined, undefined, { key });
				},
			
				get(key: S): A | undefined {
					return self.store.get(key);
				},
			
				/** Read with a fallback — context lookups always expect a default. */
				getOr(key: S, fallback: A): A {
					return self.store.has(key) ? self.store.get(key) as A : fallback;
				},
			
				has(key: S): B {
					return self.store.has(key);
				},
			
				delete(key: S): B {
					return self.store.delete(key);
				},
			
				keys(): S[] {
					return [...self.store.keys()];
				},
			
				/** Full snapshot, for hand-off / persistence / diagnostics. */
				snapshot(): Record<S, A> {
					return Object.fromEntries(self.store);
				},
			
				/** Restore from a snapshot. Replaces the store wholesale. */
				restore(snap: Record<S, A>): void {
					self.store = new Map(Object.entries(snap));
				},
			
				/** Merge a partial snapshot over the current store (existing keys win unless overwrite). */
				merge(snap: Record<S, A>, overwrite = false): void {
					for (const [k, v] of Object.entries(snap)) {
						if (!overwrite && self.store.has(k)) continue;
						self.store.set(k, v);
					}
				},
			
				/** Scoped read: return a sub-object of the store whose keys start with the prefix. */
				scoped(prefix: S): Record<S, A> {
					const out: Record<S, A> = {};
					for (const [k, v] of self.store) {
						if (k.startsWith(prefix)) out[k] = v;
					}
					return out;
				},
			
				purge(): void {
					self.store.clear();
				},
			}));
			z.ENCRYPTION = this.ATOM.BIND("ENCRYPTION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				encryptionConfigs: [] as EncryptionConfig[],
			
				getEncryptionConfigs(): EncryptionConfig[] {
					return [...self.encryptionConfigs];
				},
			
				configureEncryption(config: EncryptionConfig): void {
					self.encryptionConfigs.push(config);
				},
			}));
			z.GROUPS = this.ATOM.BIND("GROUPS", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				groups: new Map<string, {
					channels: Set<string>;
					enabled: boolean;
					middleware: ((msg: CSEMessage) => boolean | void)[];
				}>(),
			
				createGroup(name: string, channels: string[] = [], options?: GroupOptions): void {
					self.groups.set(name, {
						channels: new Set(channels),
						enabled: options?.enabled ?? true,
						middleware: options?.middleware ? [options.middleware] : [],
					});
				},
			
				deleteGroup(name: string): void {
					self.groups.delete(name);
				},
			
				enableGroup(name: string): void {
					const g = self.groups.get(name);
					if (!g) return;
					g.enabled = true;
				},
			
				disableGroup(name: string): void {
					const g = self.groups.get(name);
					if (!g) return;
					g.enabled = false;
				},
			
				addToGroup(name: string, rt: string): void {
					const g = self.groups.get(name);
					if (!g) return;
					g.channels.add(rt);
				},
			
				removeFromGroup(name: string, rt: string): void {
					const g = self.groups.get(name);
					if (!g) return;
					g.channels.delete(rt);
				},
			
				async groupBroadcast(
					name: string,
					p: any,
					sl = self.sl,
					ms = self.ms,
					rt = self.rt,
					ttl?: number,
				): Promise<void> {
					const g = self.groups.get(name);
					if (!g || !g.enabled) return;
			
					await Promise.all(
						[...g.channels].map(ch => self.broadcast(p, ch, sl, ms, rt, ttl)),
					);
				},
			
				groupUse(name: string, fn: (msg: CSEMessage) => boolean | void): void {
					const g = self.groups.get(name);
					if (!g) return;
					g.middleware.push(fn);
				},
			
				getAllGroups(): Record<string, {
					channels: string[];
					enabled: boolean;
					middlewareCount: number;
				}> {
					const out: Record<string, {
						channels: string[];
						enabled: boolean;
						middlewareCount: number;
					}> = {};
			
					for (const [name, g] of self.groups) {
						out[name] = {
							channels: [...g.channels],
							enabled: g.enabled,
							middlewareCount: g.middleware.length,
						};
					}
			
					return out;
				},
			}));
			z.HISTORY = this.ATOM.BIND("HISTORY", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				history: new Map<string, CSEMessage[]>(),
				defaultLimit: 128,
				limits: new Map<string, number>(),
			
				/** Append one message. Returns the entry so callers can thread it onward. */
				record(rt: string, msg: CSEMessage): CSEMessage {
					let list = self.history.get(rt);
					if (!list) {
						list = [];
						self.history.set(rt, list);
					}
			
					list.push(msg);
			
					const limit = self.limits.get(rt) ?? self.defaultLimit;
					if (list.length > limit) list.splice(0, list.length - limit);
			
					return msg;
				},
			
				setChannelLimit(rt: string, limit: number): void {
					self.limits.set(rt, limit);
			
					const list = self.history.get(rt);
					if (list && list.length > limit) {
						list.splice(0, list.length - limit);
					}
				},
			
				getChannelLimit(rt: string): number {
					return self.limits.get(rt) ?? self.defaultLimit;
				},
			
				getRecentHistory(rt: string, limit = 3): CSEMessage[] {
					return (self.history.get(rt) ?? []).slice(-limit);
				},
			
				getChannelHistory(rt: string): CSEMessage[] {
					return [...(self.history.get(rt) ?? [])];
				},
			
				getAllHistoryChannels(): string[] {
					return [...self.history.keys()];
				},
			
				getChannelDepth(rt: string): number {
					return (self.history.get(rt) ?? []).length;
				},
			
				getHistorySizes(): Record<string, number> {
					const out: Record<string, number> = {};
					for (const [ch, list] of self.history) out[ch] = list.length;
					return out;
				},
			
				clearChannel(rt: string): void {
					self.history.delete(rt);
				},
			
				purge(): void {
					self.history.clear();
					self.limits.clear();
				},
			}));
			z.META = this.ATOM.BIND("META", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				// ── your blob, organized ───────────────────────────────────────────────────────────────────
				_batches: new Set<string | symbol>(),
				_batchQueue: new Map<string | symbol, A>(),
				_batchOpts: new Map<string | symbol, { triggerRender?: B }>(),
				_globalListeners: new Set<(key: S | symbol, value: A) => void>(),
				_global_subscribers: new Map<S, Set<() => void>>(),
				_sys: new Map<S, SysEntry>(),
				_sealed: new Set<S>(),
				_frozen: new Set<S>(),
				_keyContracts: new Map<S, Set<S>>(),
				_conflicts: new Map<S, ConflictPolicy>(),
				_conflictStrategies: new Map<S, "latest" | "fail" | "accumulate">(),
				_causal: new Map<S, CausalEntry[]>(),
				_scopes: new Map<S, SignalScope>(),
				_msgHandlers: new Map<S, Set<(p: A) => void>>(),
				_lastWriteSource: new Map<S, S>(),
				_mainMap: new Map<A, S>(),
				_tupleCache: new Map<S, S>(),
				_weakMap: new WeakMap<object, S>(),
				_boundaries: new WeakMap<object, S>(),
			
				// ── sys entries ────────────────────────────────────────────────────────────────────────────
				/** Register or update a system entry for this cell. */
				setSys(name: S, entry: Partial<SysEntry> & { value?: A, source?:A }): SysEntry {
					const prev = self._sys.get(name);
					const next: SysEntry = {
						name,
						value: entry.value ?? prev?.value,
						ts: Date.now(),
						revision: (prev?.revision ?? 0) + 1,
						source: entry.source ?? "meta",
					} as A;
					self._sys.set(name, next);
					return next;
				},
			
				getSys(name: S): SysEntry | null {
					return self._sys.get(name) ?? null;
				},
			
				deleteSys(name: S): B {
					return self._sys.delete(name);
				},
			
				listSys(): S[] {
					return [...self._sys.keys()];
				},
			
				// ── causal log ─────────────────────────────────────────────────────────────────────────────
				/** Append a causal entry. Keep it cheap — callers pass what they know. */
				recordCausal(key: S, cause: S, value?: A): void {
					const list = self._causal.get(key) ?? [];
					list.push({ cause, value, ts: Date.now() });
			
					// hard cap so long-lived cells don't balloon
					if (list.length > 256) list.splice(0, list.length - 256);
			
					self._causal.set(key, list);
				},
			
				getCausal(key: S): CausalEntry[] {
					return self._causal.get(key) ?? [];
				},
			
				clearCausal(key?: S): void {
					if (key) self._causal.delete(key);
					else self._causal.clear();
				},
			
				// ── last write provenance ──────────────────────────────────────────────────────────────────
				noteWrite(key: S, source: S): void {
					self._lastWriteSource.set(key, source);
				},
			
				whoWrote(key: S): S | null {
					return self._lastWriteSource.get(key) ?? null;
				},
			
				// ── msg handlers ───────────────────────────────────────────────────────────────────────────
				addMsgHandler(key: S, fn: (p: A) => void): () => void {
					const set = self._msgHandlers.get(key) ?? new Set();
					set.add(fn);
					self._msgHandlers.set(key, set);
					return () => set.delete(fn);
				},
			
				emitMsg(key: S, p: A): void {
					for (const fn of self._msgHandlers.get(key) ?? []) {
						try {
							fn(p);
						} catch (e) {
							console.error(`[meta-shard] msg handler failed for "${key}":`, e);
						}
					}
				},
			
				// ── global listeners ───────────────────────────────────────────────────────────────────────
				onGlobal(fn: (key: S | symbol, value: A) => void): () => void {
					self._globalListeners.add(fn);
					return () => self._globalListeners.delete(fn);
				},
			
				emitGlobal(key: S | symbol, value: A): void {
					for (const fn of self._globalListeners) {
						try {
							fn(key, value);
						} catch (e) {
							console.error("[meta-shard] global listener failed:", e);
						}
					}
				},
			
				// ── inspect / purge ────────────────────────────────────────────────────────────────────────
				inspect(): Record<S, A> {
					return {
						sys: Object.fromEntries(self._sys),
						causalKeys: [...self._causal.keys()],
						sealed: [...self._sealed],
						frozen: [...self._frozen],
						lastWriteSource: Object.fromEntries(self._lastWriteSource),
						msgHandlerKeys: [...self._msgHandlers.keys()],
						globalListenerCount: self._globalListeners.size,
					};
				},
			
				purge(): void {
					for (const map of [
						self._batchQueue,
						self._batchOpts,
						self._global_subscribers,
						self._sys,
						self._keyContracts,
						self._conflicts,
						self._conflictStrategies,
						self._causal,
						self._scopes,
						self._msgHandlers,
						self._lastWriteSource,
						self._mainMap,
						self._tupleCache,
					] as Map<A, A>[]) {
						map.clear();
					}
			
					self._batches.clear();
					self._sealed.clear();
					self._frozen.clear();
					self._globalListeners.clear();
					self._weakMap = new WeakMap();
					self._boundaries = new WeakMap();
				},
			}));
			z.OFFSEC = this.ATOM.BIND("OFFSEC", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				secrets: new Set<A>(),
				audit: [] as OFFSEC_AUDIT_ENTRY[],
				maxAudit: (config.maxAudit ?? 500) as N,
			
				// ── secret marking ─────────────────────────────────────────────────────────────────────────
				/** Mark a value (token, key, credential) as secret. Reference equality by default. */
				markSecret(value: A): void {
					self.secrets.add(value);
				},
			
				isSecret(value: A): B {
					return self.secrets.has(value);
				},
			
				clearSecret(value: A): B {
					return self.secrets.delete(value);
				},
			
				/**
				 * Deep-scan a message for marked secrets; replace with '[REDACTED]' before the message
				 * leaves the boundary. Mutates nothing — returns a copy when a hit occurs.
				 */
				scrub<T>(payload: T): { clean: T; redacted: N } {
					let redacted = 0;
					const seen = new Set<object>();
			
					const walk = (v: unknown): unknown => {
						if (self.secrets.has(v as A)) {
							redacted++;
							return "[REDACTED]";
						}
			
						if (v === null || typeof v !== "object") return v;
						if (seen.has(v as object)) return "[CYCLIC]";
			
						seen.add(v as object);
			
						if (Array.isArray(v)) return v.map(walk);
			
						const out: Record<S, unknown> = {};
						for (const [k, val] of Object.entries(v as Record<S, unknown>)) {
							out[k] = walk(val);
						}
			
						return out;
					};
			
					const clean = walk(payload) as T;
			
					if (redacted) {
						self.audit.push({
							at: Date.now(),
							what: `scrubbed ${redacted} secret(s)`,
						});
			
						if (self.audit.length > self.maxAudit) self.audit.shift();
					}
			
					return { clean, redacted };
				},
			
				listAudit(): OFFSEC_AUDIT_ENTRY[] {
					return [...self.audit];
				},
			
				purge(): void {
					self.secrets.clear();
					self.audit.length = 0;
				},
			}));
			z.PERMISSIONS = this.ATOM.BIND("PERMISSIONS", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				permissions: new Map<string, ChannelPermissions>(),
			
				setPermissions(rt: string, perms: ChannelPermissions): void {
					self.permissions.set(rt, perms);
				},
			
				getPermissions(rt: string): ChannelPermissions | undefined {
					return self.permissions.get(rt);
				},
			
				removePermissions(rt: string): B {
					return self.permissions.delete(rt);
				},
			
				hasPermissions(rt: string): B {
					return self.permissions.has(rt);
				},
			
				getAllPermissions(): Record<string, ChannelPermissions> {
					return Object.fromEntries(self.permissions);
				},
			
				listProtectedChannels(): string[] {
					return [...self.permissions.keys()];
				},
			
				/** Wildcard-aware lookup: exact wins, else longest matching prefix before `*`. */
				resolvePermissions(rt: string): ChannelPermissions | undefined {
					if (self.permissions.has(rt)) return self.permissions.get(rt);
			
					let best: ChannelPermissions | undefined;
					let bestLen = -1;
			
					for (const [pattern, perms] of self.permissions) {
						const star = pattern.indexOf("*");
						if (star === -1) continue;
			
						if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) {
							best = perms;
							bestLen = star;
						}
					}
			
					return best;
				},
			
				/** Check one capability. No permission set → allowed (open channel). */
				check(
					rt: string,
					action: "read" | "write" | "send" | "subscribe" | "admin",
					role?: S,
				): B {
					const perms = self.resolvePermissions(rt);
					if (!perms) return true;
			
					if (role && perms.roles && perms.roles.length && !perms.roles.includes(role)) {
						return false;
					}
			
					if (action === "read") return perms.read;
					if (action === "write") return perms.write;
			
					return perms[action] ?? perms.write;
				},
			
				purge(): void {
					self.permissions.clear();
				},
			}));
			z.PERSISTENCE = this.ATOM.BIND("PERSISTENCE", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				durableOffsets: new Map<string, number>(),
				persisted: config.persisted as B | undefined,
				durableHistory: new Map<string, CSEMessage[]>(),
			
				getDurableHistorySizes(): Record<string, number> {
					const out: Record<string, number> = {};
			
					for (const [ch, hist] of self.durableHistory) {
						out[ch] = hist.length;
					}
			
					return out;
				},
			
				async replay(options: ReplayOptions, handler: CSEHandler): Promise<void> {
					const history = self.durableHistory.get(options.rt) ?? [];
					let slice = history;
			
					if (options.fromOffset !== undefined) {
						slice = slice.slice(options.fromOffset);
					}
			
					if (options.toOffset !== undefined) {
						slice = slice.slice(0, options.toOffset);
					}
			
					if (options.fromTimestamp !== undefined) {
						slice = slice.filter(m => m.ts !== undefined && m.ts >= options.fromTimestamp!);
					}
			
					if (options.toTimestamp !== undefined) {
						slice = slice.filter(m => m.ts !== undefined && m.ts <= options.toTimestamp!);
					}
			
					for (const m of slice) {
						handler(m.p);
					}
				},
			
				subscribeDurable(
					handler: CSEHandler,
					options: DurableOptions,
					k: string,
					sl = self.sl,
					ms = self.ms,
					rt = self.rt,
				): () => void {
					const key = `${rt}:${options.subscriberId}`;
					const offset = self.durableOffsets.get(key) ?? options.fromOffset ?? 0;
					const history = self.durableHistory.get(rt) ?? [];
			
					history.slice(offset).forEach(m => handler(m.p));
			
					return self.sub(
						(p: unknown) => {
							const h = self.durableHistory.get(rt) ?? [];
							const nextOffset = h.length;
			
							self.durableOffsets.set(key, nextOffset);
							handler(p);
						},
						k,
						sl,
						ms,
						rt,
					);
				},
			
				getAllCheckpoints(): Record<string, number> {
					return Object.fromEntries(self.durableOffsets);
				},
			
				async setCheckpoint(
					subscriberId: string,
					offset: number,
					k: string,
					sl?: string,
					ms?: string,
					rt?: string,
				): Promise<void> {
					const key = `${rt}:${subscriberId}`;
			
					self.durableOffsets.set(key, offset);
			
					if (self.DEVTOOLS_ACTIVE) {
						self.emitDevtoolsEvent(
							"checkpoint_set",
							rt,
							ms || undefined,
							undefined,
							{ subscriberId, offset },
						);
					}
				},
			
				async getCheckpoint(
					subscriberId: string,
					k: string,
					sl?: string,
					ms?: string,
					rt?: string,
				): Promise<number | null> {
					const key = `${rt}:${ms}:${subscriberId}`;
			
					return self.durableOffsets.has(key)
						? self.durableOffsets.get(key)!
						: null;
				},
			}));
			z.PRIORITY = this.ATOM.BIND("PRIORITY", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				queues: new Map<string, Array<{
					priority: SIG_BUS_Priority;
					weight: N;
					seq: N;
					item: A;
					enqueuedAt: N;
				}>>(),
			
				queueDefaults: new Map<string, SIG_BUS_Priority>(),
				defaultPriority: "normal" as SIG_BUS_Priority,
				seq: 0,
			
				setDefaultPriority(priority: SIG_BUS_Priority): void {
					self.defaultPriority = priority;
				},
			
				getDefaultPriority(): SIG_BUS_Priority {
					return self.defaultPriority;
				},
			
				setQueueDefault(queue: string, priority: SIG_BUS_Priority): void {
					self.queueDefaults.set(queue, priority);
				},
			
				getQueueDefault(queue: string): SIG_BUS_Priority {
					return self.queueDefaults.get(queue) ?? self.defaultPriority;
				},
			
				priorityWeight(p?: SIG_BUS_Priority): number {
					return p === "high" ? 3 : p === "low" ? 1 : 2;
				},
			
				enqueue(queue: string, item: A, priority?: SIG_BUS_Priority): N {
					const p = priority ?? self.getQueueDefault(queue);
					let q = self.queues.get(queue);
			
					if (!q) {
						q = [];
						self.queues.set(queue, q);
					}
			
					const entry = {
						priority: p,
						weight: self.priorityWeight(p),
						seq: self.seq++,
						item,
						enqueuedAt: Date.now(),
					};
			
					q.push(entry);
			
					if (self.DEVTOOLS_ACTIVE) {
						self.emitDevtoolsEvent("priority_enqueued", undefined, undefined, queue, {
							priority: p,
							depth: q.length,
						});
					}
			
					return entry.seq;
				},
			
				/** Highest weight wins; FIFO within the same weight (low seq = older = first). */
				dequeue(queue: string): A | undefined {
					const q = self.queues.get(queue);
					if (!q || !q.length) return undefined;
			
					let bestIdx = 0;
			
					for (let i = 1; i < q.length; i++) {
						const a = q[i], b = q[bestIdx];
			
						if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) {
							bestIdx = i;
						}
					}
			
					const [entry] = q.splice(bestIdx, 1);
			
					if (self.DEVTOOLS_ACTIVE) {
						self.emitDevtoolsEvent("priority_dequeued", undefined, undefined, queue, {
							priority: entry.priority,
							remaining: q.length,
						});
					}
			
					return entry.item;
				},
			
				peek(queue: string): A | undefined {
					const q = self.queues.get(queue);
					if (!q || !q.length) return undefined;
			
					let bestIdx = 0;
			
					for (let i = 1; i < q.length; i++) {
						const a = q[i], b = q[bestIdx];
			
						if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) {
							bestIdx = i;
						}
					}
			
					return q[bestIdx].item;
				},
			
				getDepth(queue: string): number {
					return (self.queues.get(queue) ?? []).length;
				},
			
				listQueues(): string[] {
					return [...self.queues.keys()];
				},
			
				getDepths(): Record<string, number> {
					const out: Record<string, number> = {};
			
					for (const [q, list] of self.queues) {
						out[q] = list.length;
					}
			
					return out;
				},
			
				clearQueue(queue: string): void {
					self.queues.delete(queue);
				},
			
				purge(): void {
					self.queues.clear();
					self.queueDefaults.clear();
				},
			}));
			z.SNAPSHOT = this.ATOM.BIND("SNAPSHOT", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				version: 0,
				history: [] as SNAPSHOT_ENTRY<W>[],
				historyMax: (config.historyMax ?? 128) as N,
				undoStack: [] as SNAPSHOT_ENTRY<W>[],
				redoStack: [] as SNAPSHOT_ENTRY<W>[],
				waiters: new Set<(value: W) => void>(),
			
				/** Capture the current state. Call this from ATOM.write() after a successful write. */
				capture(value: W, label?: S): SNAPSHOT_ENTRY<W> {
					self.version++;
			
					const entry: SNAPSHOT_ENTRY<W> = {
						version: self.version,
						value,
						ts: Date.now(),
						label,
					};
			
					self.history.push(entry);
			
					if (self.history.length > self.historyMax) {
						self.history.splice(0, self.history.length - self.historyMax);
					}
			
					self.redoStack.length = 0;
			
					for (const resolve of self.waiters) {
						resolve(value);
					}
			
					self.waiters.clear();
			
					return entry;
				},
			
				/** Latest snapshot, or undefined if none. */
				latest(): SNAPSHOT_ENTRY<W> | undefined {
					return self.history[self.history.length - 1];
				},
			
				/** Fetch a snapshot by exact version number. */
				byVersion(v: N): SNAPSHOT_ENTRY<W> | undefined {
					return self.history.find(e => e.version === v);
				},
			
				/** Fetch the last N snapshots, oldest first. */
				recent(n = 3): SNAPSHOT_ENTRY<W>[] {
					return self.history.slice(-n);
				},
			
				/** All snapshots since a timestamp. */
				since(ts: N): SNAPSHOT_ENTRY<W>[] {
					return self.history.filter(e => e.ts >= ts);
				},
			
				count(): N {
					return self.history.length;
				},
			
				/** Wait for the next capture (first one if none exist yet). */
				waitForNext(timeoutMs = 0): Promise<W> {
					const latest = self.latest();
			
					if (latest) return Promise.resolve(latest.value);
			
					return new Promise<W>((resolve, reject) => {
						const waiter = (v: W) => resolve(v);
			
						self.waiters.add(waiter);
			
						if (timeoutMs > 0) {
							setTimeout(() => {
								if (self.waiters.delete(waiter)) {
									reject(new Error("ATOM snapshot wait timed out"));
								}
							}, timeoutMs);
						}
					});
				},
			
				/** Move an entry from history into the undo stack (used by TIME_TRAVEL). */
				pushUndo(entry: SNAPSHOT_ENTRY<W>): void {
					self.undoStack.push(entry);
			
					if (self.undoStack.length > self.historyMax) {
						self.undoStack.splice(0, self.undoStack.length - self.historyMax);
					}
				},
			
				/** Move an entry from history into the redo stack (used by TIME_TRAVEL). */
				pushRedo(entry: SNAPSHOT_ENTRY<W>): void {
					self.redoStack.push(entry);
			
					if (self.redoStack.length > self.historyMax) {
						self.redoStack.splice(0, self.redoStack.length - self.historyMax);
					}
				},
			
				purge(): void {
					self.history.length = 0;
					self.undoStack.length = 0;
					self.redoStack.length = 0;
					self.waiters.clear();
					self.version = 0;
				},
			}));
			z.TAGS = this.ATOM.BIND("TAGS", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				tagSubs: new Map<string, Set<CSEHandler>>(),
				tagsByChannel: new Map<string, Set<string>>(),
			
				// ── channel → tags ─────────────────────────────────────────────────────────────────────────
				/** Declare the tags a channel carries. Replaces any previous tag set for that channel. */
				tagChannel(channel: S, tags: S[]): void {
					self.tagsByChannel.set(channel, new Set(tags));
				},
			
				untagChannel(channel: S): B {
					return self.tagsByChannel.delete(channel);
				},
			
				getTagsForChannel(channel: S): S[] {
					return [...(self.tagsByChannel.get(channel) ?? [])];
				},
			
				/** All channels carrying at least one of the given tags. */
				channelsByTags(tags: S[]): S[] {
					const want = new Set(tags);
					const out: S[] = [];
			
					for (const [ch, chTags] of self.tagsByChannel) {
						for (const t of chTags) {
							if (want.has(t)) {
								out.push(ch);
								break;
							}
						}
					}
			
					return out;
				},
			
				// ── subscriber side ─────────────────────────────────────────────────────────────────────────
				subscribeByTag(tag: string, handler: CSEHandler): () => void {
					const set = self.tagSubs.get(tag) ?? new Set<CSEHandler>();
					set.add(handler);
					self.tagSubs.set(tag, set);
			
					return () => set.delete(handler);
				},
			
				unsubscribeByTag(tag: string, handler: CSEHandler): B {
					return self.tagSubs.get(tag)?.delete(handler) ?? false;
				},
			
				listTags(): string[] {
					return [...self.tagSubs.keys()];
				},
			
				// ── publish side ────────────────────────────────────────────────────────────────────────────
				/** Fan a tagged message out to every handler subscribed to a tag the channel carries. */
				publishByTag(msg: CSEMessage): N {
					const tags = self.tagsByChannel.get(msg.r);
					if (!tags || tags.size === 0) return 0;
			
					let delivered = 0;
			
					for (const tag of tags) {
						const subs = self.tagSubs.get(tag);
						if (!subs) continue;
			
						for (const handler of [...subs]) {
							try {
								handler(msg);
								delivered++;
							} catch (e) {
								console.error(`[tags-NEUTRON] tag handler failed for "${tag}":`, e);
							}
						}
					}
			
					return delivered;
				},
			
				getTagsByChannel(): Record<string, string[]> {
					const out: Record<string, string[]> = {};
			
					for (const [ch, tags] of self.tagsByChannel) {
						out[ch] = [...tags];
					}
			
					return out;
				},
			
				purge(): void {
					self.tagSubs.clear();
					self.tagsByChannel.clear();
				},
			}));
			z.TIME_TRAVEL = this.ATOM.BIND("TIME_TRAVEL", ({ self, master, config, SNAPSHOT }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				snapshots: null as typeof SNAPSHOT | null,
				version: 0,
				historyIndex: -1,
				undoStack: [] as SNAPSHOT_ENTRY<W>[],
				initial: undefined as W | undefined,
				_time_travel_reverting: false,
				autoTimers: new Set<ReturnType<typeof setInterval>>(),
			
				/** Bind to the snapshot shard of the same cell. Call once after both shards are bound. */
				attach(snapshots: typeof SNAPSHOT): void {
					self.snapshots = snapshots;
					self.version = snapshots.version;
					self.historyIndex = snapshots.history.length - 1;
					self.initial = snapshots.history[0]?.value;
				},
			
				/** Write a value back to the cell and record it as a proper version (fires subs like a normal write). */
				replay: (entry: SNAPSHOT_ENTRY<W>): void => {
					self._time_travel_reverting = true;
			
					try {
						self._writeThrough?.(
							entry.value,
							entry.label ? `time-travel:${entry.label}` : "time-travel",
						);
					} finally {
						self._time_travel_reverting = false;
					}
				},
			
				/** Injected by ATOM when the shard is bound — how restores become real writes. */
				_writeThrough: undefined as ((value: W, cause?: S) => void) | undefined,
			
				_current(): SNAPSHOT_ENTRY<W> | undefined {
					return self.snapshots?.history[self.historyIndex];
				},
			
				back(): B {
					if (!self.snapshots || self.historyIndex <= 0) return false;
			
					self.historyIndex--;
			
					const entry = self._current();
					if (!entry) return false;
			
					// The entry we stepped away from goes on redo.
					const departed = self.snapshots.history[self.historyIndex + 1];
					if (departed) self.snapshots.pushRedo(departed);
			
					self.replay(entry);
					return true;
				},
			
				forward(): B {
					if (!self.snapshots) return false;
			
					const nextEntry = self.snapshots.history[self.historyIndex + 1];
					if (!nextEntry) return false;
			
					self.historyIndex++;
					self.replay(nextEntry);
					return true;
				},
			
				goto(version: N): B {
					if (!self.snapshots) return false;
			
					const target = self.snapshots.byVersion(version);
					if (!target) return false;
			
					const idx = self.snapshots.history.indexOf(target);
					if (idx < 0) return false;
			
					const departed = self._current();
					if (departed && idx < self.historyIndex) {
						self.snapshots.pushRedo(departed);
					}
			
					self.historyIndex = idx;
					self.replay(target);
					return true;
				},
			
				auto_back(time = 2500): void {
					const t = setInterval(() => {
						if (!self.back()) clearInterval(t);
					}, time);
			
					self.autoTimers.add(t);
				},
			
				auto_forward(time = 2500): void {
					const t = setInterval(() => {
						if (!self.forward()) clearInterval(t);
					}, time);
			
					self.autoTimers.add(t);
				},
			
				/** Graft a foreign snapshot sequence onto this timeline (branch merge). */
				graft(entries: SNAPSHOT_ENTRY<W>[]): N {
					if (!self.snapshots) return 0;
			
					let grafted = 0;
			
					for (const e of entries) {
						self.snapshots.history.push({
							version: ++self.snapshots.version,
							value: e.value,
							ts: e.ts ?? Date.now(),
							label: e.label ? `grafted:${e.label}` : "grafted",
						});
			
						grafted++;
					}
			
					self.historyIndex = self.snapshots.history.length - 1;
					return grafted;
				},
			
				/** Stop all auto walkers. */
				stopAuto(): void {
					for (const t of self.autoTimers) clearInterval(t);
					self.autoTimers.clear();
				},
			
				/** True while replay() is writing — ATOM.write() checks this to avoid re-capturing. */
				get isReverting(): B {
					return self._time_travel_reverting;
				},
			
				purge(): void {
					self.stopAuto();
					self.snapshots = null;
					self.undoStack.length = 0;
					self.historyIndex = -1;
				},
			}));
			z.TTL = this.ATOM.BIND("TTL", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				entries: new Map<S, TTL_ENTRY>(),
				onExpire: [] as ((key: S, data?: A) => void)[],
				defaultTtlMs: (config.defaultTtlMs ?? 0) as N,
			
				// ── write ──────────────────────────────────────────────────────────────────────────────────
				set(key: S, data?: A, ttlMs?: N): TTL_ENTRY {
					const ttl = ttlMs ?? self.defaultTtlMs;
			
					if (!ttl || ttl <= 0) {
						throw new Error("[ttl-shard] no ttl provided and no defaultTtlMs configured");
					}
			
					const entry: TTL_ENTRY = {
						expiresAt: Date.now() + ttl,
						data,
					};
			
					self.entries.set(key, entry);
					return entry;
				},
			
				touch(key: S, ttlMs?: N): B {
					const e = self.entries.get(key);
					if (!e) return false;
			
					e.expiresAt = Date.now() + (ttlMs ?? self.defaultTtlMs);
					return true;
				},
			
				// ── read ───────────────────────────────────────────────────────────────────────────────────
				/** Returns undefined if missing OR expired (and removes it lazily). */
				get(key: S): A | undefined {
					const e = self.entries.get(key);
					if (!e) return undefined;
			
					if (Date.now() >= e.expiresAt) {
						self.entries.delete(key);
						return undefined;
					}
			
					return e.data;
				},
			
				has(key: S): B {
					const e = self.entries.get(key);
					return !!e && Date.now() < e.expiresAt;
				},
			
				peek(key: S): A | undefined {
					return self.entries.get(key)?.data;
				},
			
				timeToLiveMs(key: S): N | null {
					const e = self.entries.get(key);
					if (!e) return null;
			
					return Math.max(0, e.expiresAt - Date.now());
				},
			
				// ── delete ─────────────────────────────────────────────────────────────────────────────────
				delete(key: S): B {
					return self.entries.delete(key);
				},
			
				// ── sweep ──────────────────────────────────────────────────────────────────────────────────
				/** Removes expired entries, fires onExpire hooks. Returns count expired. */
				sweep(): number {
					const now = Date.now();
					let n = 0;
			
					for (const [key, e] of [...self.entries]) {
						if (now >= e.expiresAt) {
							self.entries.delete(key);
							n++;
			
							for (const fn of self.onExpire) {
								try {
									fn(key, e.data);
								} catch (err) {
									console.error(`[ttl-shard] onExpire failed for "${key}":`, err);
								}
							}
						}
					}
			
					return n;
				},
			
				/** Sweep on a cadence. Owns its own timer — call stopSweeper() to cancel. */
				_sweeper: null as ReturnType<typeof setInterval> | null,
			
				startSweeper(intervalMs = 30_000): void {
					if (self._sweeper) return;
					self._sweeper = setInterval(() => self.sweep(), intervalMs);
				},
			
				stopSweeper(): void {
					if (self._sweeper) {
						clearInterval(self._sweeper);
						self._sweeper = null;
					}
				},
			
				get size(): number {
					return self.entries.size;
				},
			
				listKeys(): S[] {
					return [...self.entries.keys()];
				},
			
				purge(): void {
					self.stopSweeper();
					self.entries.clear();
					self.onExpire.length = 0;
				},
			}));
			z.REACTIVE = this.ATOM.BIND("REACTIVE", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				records: new Map<S, RiftRecord<A>>(),
				listeners: new Map<S, Set<StoreListener<A>>>(),
			
				get<T = A>(key: S): RiftRecord<T> | undefined {
					return self.records.get(key) as RiftRecord<T> | undefined;
				},
			
				set<T = A>(key: S, record: RiftRecord<T>): void {
					self.records.set(key, record as RiftRecord<A>);
			
					const subs = self.listeners.get(key);
			
					if (subs) {
						for (const l of [...subs]) {
							try {
								l(record as RiftRecord<A>);
							} catch {
								// one bad listener never kills the rest
							}
						}
					}
				},
			
				sub<T = A>(key: S, fn: StoreListener<T>): () => void {
					let subs = self.listeners.get(key);
			
					if (!subs) {
						subs = new Set();
						self.listeners.set(key, subs);
					}
			
					subs.add(fn as StoreListener<A>);
			
					return () => {
						const s = self.listeners.get(key);
						if (!s) return;
			
						s.delete(fn as StoreListener<A>);
			
						if (!s.size) self.listeners.delete(key);
					};
				},
			
				delete(key: S): void {
					self.records.delete(key);
					self.listeners.delete(key);
				},
			
				clear(): void {
					self.records.clear();
					self.listeners.clear();
				},
			
				get size(): N {
					return self.records.size;
				},
			}));
			z.SCHEMA = this.ATOM.BIND("SCHEMA", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				_schemas: new Map<string, CSESchema>(),
			
				setChannelSchema(schema: CSESchema, rt: string): void {
					self._schemas.set(rt, schema);
				},
			
				getChannelSchema(rt: string): CSESchema | undefined {
					return self._schemas.get(rt);
				},
			
				removeChannelSchema(rt: string): B {
					const had = self._schemas.delete(rt);
					return had;
				},
			
				hasChannelSchema(rt: string): B {
					return self._schemas.has(rt);
				},
			
				listChannelSchemas(): string[] {
					return [...self._schemas.keys()];
				},
			
				getAllSchemas(): Record<string, CSESchema> {
					return Object.fromEntries(self._schemas);
				},
			
				/** Wildcard-aware lookup: exact channel wins, else longest matching prefix before `*`. */
				resolveSchema(rt: string): CSESchema | undefined {
					if (self._schemas.has(rt)) return self._schemas.get(rt);
			
					let best: CSESchema | undefined;
					let bestLen = -1;
			
					for (const [pattern, schema] of self._schemas) {
						const star = pattern.indexOf("*");
						if (star === -1) continue;
			
						if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) {
							best = schema;
							bestLen = star;
						}
					}
			
					return best;
				},
			
				/**
				 * Validate a message against its channel schema. No schema → pass (open channel).
				 * Returns { ok, errors } — never throws.
				 */
				checkSchema(rt: S, msg: CSEMessage): { ok: B; errors: S[] } {
					const schema = self.resolveSchema(rt) as A;
					if (!schema) return { ok: true, errors: [] };
			
					const errors: S[] = [];
					const payload = (msg.p ?? {}) as Record<S, A>;
			
					for (const [field, rule] of Object.entries(schema.fields ?? {}) as A) {
						const v = payload[field];
			
						if (v === undefined || v === null) {
							if (rule.required) errors.push(`missing required field "${field}"`);
							continue;
						}
			
						if (rule.type !== "any") {
							const actual = typeof v;
			
							if (rule.type === "number" && actual !== "number") {
								errors.push(`field "${field}" expected number, got ${actual}`);
							} else if (rule.type === "string" && actual !== "string") {
								errors.push(`field "${field}" expected string, got ${actual}`);
							} else if (rule.type === "boolean" && actual !== "boolean") {
								errors.push(`field "${field}" expected boolean, got ${actual}`);
							}
						}
					}
			
					if (schema.validate) {
						try {
							const r = schema.validate(msg);
			
							if (r !== true && r !== undefined) {
								errors.push(
									typeof r === "string"
										? r
										: `validator rejected message on "${rt}"`,
								);
							}
						} catch (e) {
							errors.push(`validator threw: ${e}`);
						}
					}
			
					return { ok: errors.length === 0, errors };
				},
			
				purge(): void {
					self._schemas.clear();
				},
			}));
			z.VALUES_REGISTRY = this.ATOM.BIND("VALUES_REGISTRY", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				instanceId: `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`,
				hasWritten: false,
				lastWriteSource: undefined as string | undefined,
				lastWriteAt: undefined as number | undefined,
				writeCount: 0,
			
				recordWrite(source = "write"): void {
					self.hasWritten = true;
					self.lastWriteSource = source;
					self.lastWriteAt = Date.now();
					self.writeCount++;
				},
			
				reset(): void {
					self.hasWritten = false;
					self.lastWriteSource = undefined;
					self.lastWriteAt = undefined;
					self.writeCount = 0;
				},
			}));
			z.CONCURRENCY = this.ATOM.BIND("CONCURRENCY", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				current: Math.min(
					config.maxConcurrency ?? 64,
					Math.max(
						config.minConcurrency ?? 1,
						config.initialConcurrency ?? Math.max(4, Math.floor((config.maxConcurrency ?? 64) / 4)),
					),
				) as N,
			
				min: (config.minConcurrency ?? 1) as N,
				max: (config.maxConcurrency ?? 64) as N,
				holders: new Set<symbol>(),
				waiters: [] as Array<() => void>,
				peak: 0 as N,
			
				get active(): N {
					return self.holders.size;
				},
			
				get concurrency(): N {
					return self.current;
				},
			
				get peakConcurrency(): N {
					return self.peak;
				},
			
				tryAcquire(): symbol | undefined {
					if (self.holders.size >= self.current) return undefined;
			
					const h = Symbol("governor-slot");
					self.holders.add(h);
			
					if (self.holders.size > self.peak) self.peak = self.holders.size;
			
					return h;
				},
			
				async acquire(): Promise<symbol> {
					const h = self.tryAcquire();
					if (h) return h;
			
					return new Promise<symbol>(resolve =>
						self.waiters.push(() => resolve(self.tryAcquire()!)),
					);
				},
			
				release(h: symbol): void {
					self.holders.delete(h);
			
					const next = self.waiters.shift();
					if (next) next();
				},
			
				/** AIMD. Call from telemetry. */
				evaluateResistance(signal: { latency: N; error: B; targetLatencyMs?: N }): void {
					const target = signal.targetLatencyMs ?? 250;
			
					if (signal.error || signal.latency > target * 1.5) {
						self.multiplicativeDecrease();
					} else if (signal.latency < target * 0.7) {
						self.additiveIncrease();
					}
				},
			
				additiveIncrease(): void {
					if (self.current < self.max) self.resize(self.current + 1);
				},
			
				multiplicativeDecrease(): void {
					if (self.current > self.min) {
						self.resize(Math.max(self.min, Math.floor(self.current * 0.5)));
					}
				},
			
				resize(n: N): void {
					self.current = Math.max(self.min, Math.min(self.max, Math.floor(n)));
			
					if (self.current > self.peak) self.peak = self.current;
			
					// Wake waiters if capacity grew.
					while (self.holders.size < self.current && self.waiters.length) {
						self.waiters.shift()?.();
					}
				},
			}));
			z.CONTRACTS = this.ATOM.BIND("CONTRACTS", ({ self, master, config }: any) => ({
				useCell: (() => {
					const fn = () => {};
					return fn;
				})(),
				r: master.r,
				m: master.m,
				s: master.s,
				k: config.k,
				contracts: new Map<S, A>(),
				_keyContracts: new Map() as Map<S, Set<S>>,
				causal: [] as CONTRACT_VIOLATION[],
				violations: [] as CONTRACT_VIOLATION[],
				violationMax: 64,
				_throwOnBlock: true as B,
				add: (contract: A): (() => void) => {
					self.contracts.set(contract.name, contract);
					return () => self.contracts.delete(contract.name);
				},
				remove: (name: S): B => {
					return self.contracts.delete(name);
				},
				get: (name: S): A | undefined => {
					return self.contracts.get(name);
				},
				list: (): A[] => {
					return [...self.contracts.values()];
				},
				bindKeyContracts: (keyContracts: Map<S, Set<S>>): void => {
					self._keyContracts = keyContracts;
				},
				_record: (v: CONTRACT_VIOLATION, blocking: B): B => {
					self.violations.push(v);
					if (self.violations.length > self.violationMax) self.violations.splice(0, self.violations.length - self.violationMax);
					if (blocking) throw new Error(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
					console.warn(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
					return false;
				},
				check: (value: A, phase: 'pre' | 'post'| 'invariant', snapshot?: Record<S, A>): B =>{
					for (const c of self.contracts.values()) {
						if (phase === 'pre' && c.precondition) {
							try { if (!c.precondition(value)) return self._record({ contract: c.name, phase, value, message: 'precondition returned false', ts: Date.now() }, !!c.block && self._throwOnBlock); }
							catch (e) { return self._record({ contract: c.name, phase, value, message: `precondition threw: ${e}`, ts: Date.now() }, true); }
						}
						if (phase === 'post' && c.postcondition) {
							try { if (!c.postcondition(value)) return self._record({ contract: c.name, phase, value, message: 'postcondition returned false', ts: Date.now() }, !!c.block && self._throwOnBlock); }
							catch (e) { return self._record({ contract: c.name, phase, value, message: `postcondition threw: ${e}`, ts: Date.now() }, true); }
						}
						if (phase === 'invariant' && c.invariant && snapshot) {
							try { if (!c.invariant(snapshot)) return self._record({ contract: c.name, phase, value, message: 'invariant returned false', ts: Date.now() }, !!c.block && self._throwOnBlock); }
							catch (e) { return self._record({ contract: c.name, phase, value, message: `invariant threw: ${e}`, ts: Date.now() }, true); }
						}
					}
					return true;
				},
				checkKey:(key: S, value: A, phase: 'pre' | 'post'): B =>{
					for (const name of self._keyContracts.get(key) ?? []) {
						const c = self.contracts.get(name);
						if (!c) continue;
						const check = phase === 'pre' ? c.precondition : c.postcondition;
						if (!check) continue;
						try {
							if (!check(value)) return self._record({ contract: name, phase, value, message: `key contract failed for "${key}"`, ts: Date.now() }, !!c.block && self._throwOnBlock);
						} catch (e) {
							return self._record({ contract: name, phase, value, message: `key contract threw: ${e}`, ts: Date.now() }, true);
						}
					}
					return true;
				},
				canWrite: (value: A): B=> { return self.check(value, 'pre'); },
				didWrite:(value: A): B=> { return self.check(value, 'post'); },
				listViolations:(): CONTRACT_VIOLATION[]=> { return [...self.violations]; },
				clearViolations:(): void=> { self.violations.length = 0; },
				purge:(): void=> { self.contracts.clear(); self.violations.length = 0; self._keyContracts.clear(); }
			}))
			z.COMPRESSION = this.ATOM.BIND("COMPRESSION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
				compressionConfigs: [] as CompressionConfig[],
			
				getCompressionConfigs(): CompressionConfig[] {
					return [...self.compressionConfigs];
				},
			
				configureCompression(config: CompressionConfig): void {
					self.compressionConfigs.push(config);
				},
			}));
			z.DEADLETTER = this.ATOM.BIND("DEADLETTER", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				root_cell_key: config.root_cell_key as A,
				deadLetters: [] as CSEMessage[],
				maxDeadLetters: (config.maxDeadLetters ?? 1000) as N,
				wire: config.wire as DEADLETTER_WIRE | undefined,
			
				/** Park a failed message. Oldest evicted when over max. */
				record(msg: CSEMessage, reason?: S): void {
					self.deadLetters.push({ ...msg, _ack: true } as CSEMessage);
			
					if (self.deadLetters.length > self.maxDeadLetters) {
						self.deadLetters.shift();
					}
				},
			
				async listDeadLetters(
					k?: S,
					sl?: S,
					ms?: S,
					rt?: S,
				): Promise<CSEMessage[]> {
					if (!rt && !ms && !sl && !k) return [...self.deadLetters];
			
					return self.deadLetters.filter(
						m =>
							(!rt || m.rt === rt) &&
							(!ms || m.ms === ms) &&
							(!sl || m.sl === sl) &&
							(!k || m.k === k),
					);
				},
			
				async requeueDeadLetter(id: S, ttl?: N): Promise<void> {
					const idx = self.deadLetters.findIndex(m => m.id === id);
					if (idx === -1) return;
			
					const msg = self.deadLetters.splice(idx, 1)[0];
					msg.ttl = ttl ?? msg.ttl;
			
					await self.wire?.enqueue(msg);
				},
			
				purgeDeadLetter(id: S): B {
					const idx = self.deadLetters.findIndex(m => m.id === id);
					if (idx === -1) return false;
			
					self.deadLetters.splice(idx, 1);
					return true;
				},
			
				count(): N {
					return self.deadLetters.length;
				},
			
				purge(): void {
					self.deadLetters.length = 0;
				},
			}));
			z.DESTRUCTION = this.ATOM.BIND("DESTRUCTION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				beforeDestruct: [] as ((key: A) => void)[],
				afterDestruct: [] as ((record: DESTRUCTION_RECORD) => void)[],
				records: [] as DESTRUCTION_RECORD[],
				destroyed: false as B,
				blocked: false as B,
			
				// ── hooks ──────────────────────────────────────────────────────────────────────────────────
				onBefore(fn: (key: A) => void): () => void {
					self.beforeDestruct.push(fn);
			
					return () => {
						self.beforeDestruct = self.beforeDestruct.filter(f => f !== fn);
					};
				},
			
				onAfter(fn: (record: DESTRUCTION_RECORD) => void): () => void {
					self.afterDestruct.push(fn);
			
					return () => {
						self.afterDestruct = self.afterDestruct.filter(f => f !== fn);
					};
				},
			
				/** Veto a pending destruction (e.g. in a beforeDestruct hook, or upfront). */
				block(): void {
					self.blocked = true;
				},
			
				unblock(): void {
					self.blocked = false;
				},
			
				// ── the kill switch ────────────────────────────────────────────────────────────────────────
				/**
				 * Runs the full destruction sequence.
				 * Returns the record on success, null when blocked or already destroyed.
				 * `reviver` is a factory the record keeps so the cell can be rebuilt later.
				 */
				destroy(cause = "unknown", reviver?: () => A): DESTRUCTION_RECORD | null {
					if (self.destroyed) return null;
					if (self.blocked) return null;
			
					const record: DESTRUCTION_RECORD = {
						key: self.root_cell,
						cause,
						at: Date.now(),
						reviver,
					};
			
					for (const fn of self.beforeDestruct) {
						try {
							fn(self.root_cell);
						} catch (e) {
							console.error("[destruction-shard] beforeDestruct hook failed:", e);
						}
					}
			
					self.destroyed = true;
					self.records.push(record);
			
					for (const fn of self.afterDestruct) {
						try {
							fn(record);
						} catch (e) {
							console.error("[destruction-shard] afterDestruct hook failed:", e);
						}
					}
			
					return record;
				},
			
				resurrect(): A | null {
					if (!self.destroyed) return null;
			
					const last = self.records[self.records.length - 1];
					self.destroyed = false;
					self.blocked = false;
			
					return last?.reviver?.() ?? null;
				},
			
				isDestroyed(): B {
					return self.destroyed;
				},
			
				clearRecords(): void {
					self.records.length = 0;
				},
			
				purge(): void {
					self.beforeDestruct.length = 0;
					self.afterDestruct.length = 0;
					self.records.length = 0;
				},
			}));
			type MODULE_PATH = S;
			interface ERROR_EMISSION {
				module: MODULE_PATH;
				code: S;
				message: S;
				detail?: A;
				ts: N;
			}
			class ORC_SHOUT {
				c = {
					emerald: { 10: "#002c22", 9: "#004f3b", 8: "#016630", 7: "#007a55", 6: "#009966", 5: "#00bc7d" },
					blue: { 10: "", 9: "", 8: "", 7: "", 6: "", 5: "" },
				};
				t = {
					deep: {
						bg: "#020817",
						txt: "#f8fafc",
						size: 12,
						title_size: 14,
						title_txt: "#94a3b8",
						payload_key: "#a4ed2c",
						payload_value: "#f8fafc",
						font: "monospace",
						border: "2px solid #101727",
						borderRadius: "6px",
						padding: "8px 16px",
						svg: "",
					},
					vintage: {
						bg: "#001B29",
						txt: "#e9e1ba",
						size: 12,
						title_size: 14,
						title_txt: "#ada785",
						payload_key: "#29c3a0",
						payload_value: "#fafafa",
						font: "monospace",
						border: "2px solid #112932",
						borderRadius: "6px",
						padding: "8px 16px",
						svg: "",
					},
					error: {
						bg: "#09090B",
						txt: "#e7000b",
						size: 12,
						title_size: 14,
						title_txt: "#9f0712",
						payload_key: "#a4ed2c",
						payload_value: "#f8fafc",
						font: "monospace",
						border: "2px solid #e7000b",
						borderRadius: "6px",
						padding: "8px 16px",
						svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e7000b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
					},
					warn: {
						bg: "#09090B",
						txt: "#e17100",
						size: 12,
						title_size: 14,
						title_txt: "#973c00",
						payload_key: "#a4ed2c",
						payload_value: "#f8fafc",
						font: "monospace",
						border: "2px solid #e17100",
						borderRadius: "6px",
						padding: "8px 16px",
						svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e17100" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`,
					},
					success: {
						bg: "#09090B",
						txt: "#009966",
						size: 12,
						title_size: 14,
						title_txt: "#016630",
						payload_key: "#a4ed2c",
						payload_value: "#f8fafc",
						font: "monospace",
						border: "2px solid #009966",
						borderRadius: "6px",
						padding: "8px 16px",
						svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#009966" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.801 10A10 10 0 1 1 17 3.335"/><path d="m9 11 3 3L22 4"/></svg>`,
					},
					info: {
						bg: "#09090B",
						txt: "#155dfc",
						size: 12,
						title_size: 14,
						title_txt: "#193cb8",
						payload_key: "#a4ed2c",
						payload_value: "#f8fafc",
						font: "monospace",
						border: "2px solid #155dfc",
						borderRadius: "6px",
						padding: "8px 16px",
						svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#155dfc" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
					},
				};
				log(msg: string | string[], p?: any, theme = this.t.deep) {
					const isArray = Array.isArray(msg);
					const title = isArray ? msg[0] : msg;
					const paragraphs = isArray ? msg.slice(1) : [];

					const containerStyle = `
						background: ${theme.bg};
						${theme.border ? `border: ${theme.border};` : ""}
						${theme.borderRadius ? `border-radius: ${theme.borderRadius};` : ""}
						${theme.padding ? `padding: ${theme.padding};` : ""}
						font-family: ${theme.font};
						display: block;
						margin: 4px 0;
					`;

					const titleWithIcon = theme?.svg ? `${theme?.svg} ${title}` : title;
					const titleStyle = `
						color: ${theme.title_txt};
						font-size: ${theme.title_size}px;
						font-weight: bold;
						font-family: ${theme.font};
						display: flex;
						align-items: center;
						gap: 8px;
					`;

					const paragraphStyle = `
						color: ${theme.txt};
						font-size: ${theme.size}px;
						font-family: ${theme.font};
						margin-top: 4px;
					`;

					const keyStyle = `
						color: ${theme.payload_key};
						font-weight: bold;
						font-family: ${theme.font};
					`;
					const valueStyle = `
						color: ${theme.payload_value};
						font-family: ${theme.font};
					`;

					let payloadStr = "";
					if (p && typeof p === "object") {
						const entries = Object.entries(p);
						payloadStr = entries.map(([key, value]) => `%c${key}: %c${JSON.stringify(value, null, 2)}`).join("\n");
					}

					const logParts = [`%c${titleWithIcon}`, ...paragraphs.map((p) => `%c${p}`), payloadStr ? `\n${payloadStr}` : ""].filter(Boolean).join("\n");

					const styles = [containerStyle + titleStyle, ...paragraphs.map(() => paragraphStyle), ...(payloadStr ? [keyStyle, valueStyle] : [])];

					console.log(logParts, ...styles);
				}
			}
			const os = new ORC_SHOUT()
			z.ERROR = this.ATOM.BIND("ERROR", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				/** History lives here regardless of emission — history is cheap, display is gated. */
				error_history: [] as ERROR_EMISSION[],
				HISTORY_CAP: 512,
			
				// ── runtime-editable controls ─────────────────────────────────────────────────────────────
				/** Master kill-switch. false → nothing emits, history still accrues. */
				ENABLED: true as B,
			
				/** Emission only in dev mode unless this is explicitly flipped on. */
				ENABLE_IN_PRODUCTION: false as B,
			
				/**
				 * Surgical control. null = every module emits.
				 * Record<string, boolean> = only listed modules emit, with true/false overriding.
				 */
				SURGICAL_LOGS: (config.surgical ?? null) as Record<MODULE_PATH, B> | null,
			
				/** Theme passed through to the LOGGER. Default = the error theme. */
				theme: (os.t),
			
				/** "TERMINAL" | "FILE" — pipes through the LOGGER's configured sink. */
				PIPE_TO_FILE_OR_TERMINAL: "TERMINAL" as "TERMINAL" | "FILE",
			
				/** Set at wiring time by the platform; falls back to plain console when absent. */
				logger: os.log,
			
				// ── the single emission point ─────────────────────────────────────────────────────────────
				/** Every error-producing call site in every shard funnels through here. */
				emit(module: MODULE_PATH, code: S, message: S, detail?: A): ERROR_EMISSION {
					const entry: ERROR_EMISSION = { module, code, message, detail, ts: Date.now() };
			
					self.error_history.push(entry);
			
					if (self.error_history.length > self.HISTORY_CAP) {
						self.error_history.splice(0, self.error_history.length - self.HISTORY_CAP);
					}
			
					if (self.shouldEmit(module)) {
						const title = `[${String(self.root_cell_key)}] ${module} · ${code}`;
			
						if (self.logger) {
							self.logger.theme = self.theme;
							self.logger.drop_log([title, message], detail);
						} else if (self.shouldEmit(module)) {
							console.error(`${title}\n  ${message}`, detail ?? "");
						}
					}
			
					return entry;
				},
			
				/** Surgical gate: master switch → dev/production → per-module toggle. */
				shouldEmit(module: MODULE_PATH): B {
					if (!self.ENABLED) return false;
					if (self.ENABLE_IN_PRODUCTION === false && root.state.get("DEV_MODE") !== true) return false;
					if (self.SURGICAL_LOGS === null) return true;
			
					// exact match wins, then longest-prefix match (section-level control)
					if (module in self.SURGICAL_LOGS) return self.SURGICAL_LOGS[module];
			
					let best: S | null = null;
			
					for (const prefix of Object.keys(self.SURGICAL_LOGS)) {
						if (module.startsWith(prefix + ".") && (best === null || prefix.length > best.length)) {
							best = prefix;
						}
					}
			
					return best !== null ? self.SURGICAL_LOGS[best] : false;
				},
			
				// ── convenience emitters used by the call sites below ────────────────────────────────────
				sealed(detail?: A) {
					return self.emit("GATE", "GATE_SEALED", "CELL IS SEALED — CHANGE SEALED TO FALSE TO RESUME WRITES", detail);
				},
			
				frozen(detail?: A) {
					return self.emit("GATE", "GATE_FROZEN", "CELL IS FROZEN — VIEWABLE BUT NOT EDITABLE", detail);
				},
			
				asleep(detail?: A) {
					return self.emit("GATE", "GATE_ASLEEP", "CELL IS ASLEEP — WILL NOT BE AVAILABLE UNTIL ITS CONFIGURED WAKE TIME", detail);
				},
			
				shardNotFound(name: S, detail?: A) {
					return self.emit("REGISTRY", "SHARD_NOT_FOUND", `NO SHARD LABELED "${name}" RELATIVE TO THIS CELL — REVIEW YOUR CONTRACT`, detail);
				},
			
				contractViolation(contract: S, detail?: A) {
					return self.emit("CONTRACTS", "CONTRACT_VIOLATION", `CONTRACT VIOLATION: ${contract}`, detail);
				},
			
				schemaViolation(topic: S, detail?: A) {
					return self.emit("SCHEMA", "SCHEMA_VIOLATION", `SCHEMA VIOLATION ON TOPIC "${topic}"`, detail);
				},
			
				permissionDenied(ch: S, detail?: A) {
					return self.emit("PERMISSIONS", "PERMISSION_DENIED", `PERMISSION DENIED ON CHANNEL "${ch}"`, detail);
				},
			
				/** Flip an entire section or a single module. */
				setSection(path: MODULE_PATH, on: B): void {
					(self.SURGICAL_LOGS ??= {})[path] = on;
				},
			
				clearSurgical(): void {
					self.SURGICAL_LOGS = null;
				},
			
				// ── diagnostics ───────────────────────────────────────────────────────────────────────────
				recent(limit = 20): ERROR_EMISSION[] {
					return self.error_history.slice(-limit);
				},
			
				byModule(module: MODULE_PATH): ERROR_EMISSION[] {
					return self.error_history.filter(e => e.module === module);
				},
			
				byCode(code: S): ERROR_EMISSION[] {
					return self.error_history.filter(e => e.code === code);
				},
			
				purge(): void {
					self.error_history.length = 0;
				},
			}));
			type EVENT_LISTENER = (payload?: A) => void;
			z.EVENT = this.ATOM.BIND("EVENT", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				events: new Map<S, Set<EVENT_LISTENER>>(),
				wildcard: new Set<(event: S, payload?: A) => void>(),
			
				on(event: S, fn: EVENT_LISTENER): () => void {
					const set = self.events.get(event) ?? new Set<EVENT_LISTENER>();
					set.add(fn);
					self.events.set(event, set);
			
					return () => set.delete(fn);
				},
			
				once(event: S, fn: EVENT_LISTENER): () => void {
					const off = self.on(event, (p: A) => {
						off();
						fn(p);
					});
			
					return off;
				},
			
				onAny(fn: (event: S, payload?: A) => void): () => void {
					self.wildcard.add(fn);
					return () => self.wildcard.delete(fn);
				},
			
				off(event: S, fn?: EVENT_LISTENER): void {
					if (!fn) self.events.delete(event);
					else self.events.get(event)?.delete(fn);
				},
			
				emit(event: S, payload?: A): number {
					let delivered = 0;
			
					for (const fn of self.wildcard) {
						try {
							fn(event, payload);
							delivered++;
						} catch (e) {
							console.error(`[event-shard] wildcard listener failed for "${event}":`, e);
						}
					}
			
					for (const fn of [...(self.events.get(event) ?? [])]) {
						try {
							fn(payload);
							delivered++;
						} catch (e) {
							console.error(`[event-shard] listener failed for "${event}":`, e);
						}
					}
			
					return delivered;
				},
			
				async emitAsync(event: S, payload?: A): Promise<number> {
					let delivered = 0;
			
					for (const fn of self.wildcard) {
						try {
							await fn(event, payload);
							delivered++;
						} catch (e) {
							console.error(`[event-shard] wildcard listener failed for "${event}":`, e);
						}
					}
			
					for (const fn of [...(self.events.get(event) ?? [])]) {
						try {
							await fn(payload);
							delivered++;
						} catch (e) {
							console.error(`[event-shard] listener failed for "${event}":`, e);
						}
					}
			
					return delivered;
				},
			
				listenerCount(event?: S): number {
					if (event) return self.events.get(event)?.size ?? 0;
			
					let n = self.wildcard.size;
			
					for (const set of self.events.values()) {
						n += set.size;
					}
			
					return n;
				},
			
				eventNames(): S[] {
					return [...self.events.keys()];
				},
			
				purge(): void {
					self.events.clear();
					self.wildcard.clear();
				},
			}));
			z.EXTERNAL_SUBSCRIPTION = this.ATOM.BIND("EXTERNAL_SUBSCRIPTION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				externalSubscribers: new Set<LISTENER<W>>(),
			
				subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
					self.externalSubscribers.add(listener);
			
					return () => {
						self.externalSubscribers.delete(listener);
					};
				},
			
				notify(change: CHANGE<W>): void {
					for (const listener of self.externalSubscribers) {
						try {
							listener(change);
						} catch (error) {
							console.error(`[soul-shard] listener failed for "${String(change.key)}":`, error);
						}
					}
				},
			
				get size(): number {
					return self.externalSubscribers.size;
				},
			
				purge(): void {
					self.externalSubscribers.clear();
				},
			}));
			z.GATE = this.ATOM.BIND("GATE", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				sealed: (config.sealed ?? false) as B,
				frozen: (config.frozen ?? false) as B,
				sleep: config.sleep as N | undefined,
			
				get asleep(): B {
					return self.sleep !== undefined && Date.now() < self.sleep;
				},
			
				seal(): V {
					self.sealed = true;
				},
			
				unseal(): V {
					self.sealed = false;
				},
			
				freeze(): V {
					self.frozen = true;
				},
			
				unfreeze(): V {
					self.frozen = false;
				},
			
				sleepUntil(timestamp: N): V {
					self.sleep = timestamp;
				},
			
				wake(): V {
					self.sleep = undefined;
				},
			
				canRead(): B {
					return !self.asleep;
				},
			
				canWrite(): B {
					return !self.sealed && !self.frozen && !self.asleep;
				},
			}));
			z.GLOBAL_SUBSCRIPTION = this.ATOM.BIND("GLOBAL_SUBSCRIPTION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				globalListeners: new Set<LISTENER<W>>(),
			
				subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
					self.globalListeners.add(listener);
			
					return () => {
						self.globalListeners.delete(listener);
					};
				},
			
				notify(change: CHANGE<W>): void {
					for (const listener of self.globalListeners) {
						try {
							listener(change);
						} catch (error) {
							console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
						}
					}
				},
			
				get size(): number {
					return self.globalListeners.size;
				},
			
				purge(): void {
					self.globalListeners.clear();
				},
			}));
			z.HOOK = this.ATOM.BIND("HOOK", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				hooks: new Map<S, HOOK_ENTRY>(),
				factories: new Map<S, () => A>(),
			
				/** Register a hook factory. The factory runs once, lazily, on first read. */
				register(name: S, factory: () => A): void {
					self.factories.set(name, factory);
					// if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('hook_registered', undefined, undefined, undefined, { name });
				},
			
				/** Read the hook's value, computing it on first access. */
				use(name: S): A {
					const existing = self.hooks.get(name);
					if (existing) return existing.value;
			
					const factory = self.factories.get(name);
					if (!factory) throw new Error(`[hook-ELECTRON] no hook registered as "${name}"`);
			
					const value = factory();
					self.hooks.set(name, { value, registeredAt: Date.now() });
			
					// if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('hook_computed', undefined, undefined, undefined, { name });
			
					return value;
				},
			
				isComputed(name: S): B {
					return self.hooks.has(name);
				},
			
				/** Drop the memoized value; the next use() re-runs the factory. Returns whether an entry existed. */
				reset(name: S): B {
					const entry = self.hooks.get(name);
					if (!entry) return false;
			
					self.hooks.delete(name);
			
					try {
						entry.teardown?.();
					} catch (e) {
						console.error(`[hook-ELECTRON] teardown failed for "${name}":`, e);
					}
			
					return true;
				},
			
				/** Drop the factory and the value. The hook is gone until re-registered. */
				unregister(name: S): void {
					self.reset(name);
					self.factories.delete(name);
				},
			
				list(): S[] {
					return [...self.factories.keys()];
				},
			
				purge(): void {
					for (const entry of self.hooks.values()) {
						try {
							entry.teardown?.();
						} catch (e) {
							console.error("[hook-ELECTRON] teardown failed during purge:", e);
						}
					}
			
					self.hooks.clear();
					self.factories.clear();
				},
			}));
			z.HTTP_TRANSPORT = this.ATOM.BIND("HTTP_TRANSPORT", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				token: config.token as string,
				timeoutMs: (config.timeoutMs ?? 10_000) as number,
				backoffMs: (config.backoffMs ?? 100) as number,
				maxBackoffMs: (config.maxBackoffMs ?? 5_000) as number,
				maxAttempts: (config.maxAttempts ?? 3) as number,
				codec: BINARY_CODEC as typeof BINARY_CODEC,
				onError: (config.onError ?? (() => {})) as (err: unknown) => void,
			
				/** Inbound dispatcher — set at attach(). Receives decoded envelopes. */
				dispatch: null as ((env: HttpEnvelope) => Promise<unknown> | unknown) | null,
			
				// ── OUTBOUND: send an envelope to another IP ─────────────────────────────
				async send(
					endpoint: string,
					env: HttpEnvelope,
				): Promise<{ ok: boolean; result?: unknown; error?: string }> {
					const envelope: Required<Pick<HttpEnvelope, "ts" | "id">> & HttpEnvelope = {
						ts: Date.now(),
						id: seqId(),
						...env,
					};
			
					const body = self.codec.encode_msg(envelope);
			
					let attempt = 0;
					let lastErr: unknown = null;
			
					while (attempt < self.maxAttempts) {
						attempt++;
			
						try {
							const res = await fetch(endpoint, {
								method: "POST",
								headers: {
									"content-type": "application/octet-stream",
									authorization: `Bearer ${self.token}`,
								},
								body: body as unknown as BodyInit,
								signal: AbortSignal.timeout(self.timeoutMs),
							});
			
							if (!res.ok) {
								lastErr = new Error(`[http-transport] endpoint ${endpoint} responded ${res.status}`);
			
								// 4xx = caller's fault, retrying will not help. Stop now.
								if (res.status >= 400 && res.status < 500) break;
								// 5xx / network: retry with backoff
							} else {
								const raw = new Uint8Array(await res.arrayBuffer());
								const decoded = self.codec.decode_msg(raw);
			
								if (decoded === null) {
									lastErr = new Error(`[http-transport] endpoint ${endpoint} returned undecodable ack`);
								} else {
									return decoded as { ok: boolean; result?: unknown; error?: string };
								}
							}
						} catch (err) {
							lastErr = err;
							// network-level failure: retry with backoff
						}
			
						if (attempt < self.maxAttempts) {
							await new Promise(r =>
								setTimeout(
									r,
									Math.min(self.backoffMs * 2 ** (attempt - 1), self.maxBackoffMs),
								),
							);
						}
					}
			
					self.onError(lastErr);
			
					return {
						ok: false,
						error: lastErr instanceof Error ? lastErr.message : String(lastErr),
					};
				},
			
				// ── INBOUND: accept POSTs from the wire ──────────────────────────────────
				/**
				 * Wire this into any runtime's http server. `handler` must:
				 *   • match on the path (transport does not route — the host decides)
				 *   • read the full request body into a Uint8Array
				 *   • call this.receive(bytes) and send back the returned ack frame
				 * Node: http.createServer((req, res) => { if (transport.matches(req)) { ... res.end(ack) } })
				 * Bun: Bun.serve({ fetch: (req) => transport.matchesBun(req) ? new Response(ack) : undefined })
				 * Deno: same shape as Bun via req.arrayBuffer()
				 */
				attach(dispatch: (env: HttpEnvelope) => Promise<unknown> | unknown): void {
					self.dispatch = dispatch;
				},
			
				/** Path/content-type matcher for host servers. */
				matches(pathname: string, method: string): boolean {
					return method === "POST" && pathname === "/bus";
				},
			
				/**
				 * The one method a host server calls: bytes in → ack bytes out.
				 * Auth is checked BEFORE dispatch. Dispatch happens inside try/catch —
				 * a throwing handler becomes an error frame, never a crash.
				 */
				async receive(bytes: Uint8Array, authHeader: string | null): Promise<Uint8Array> {
					const ack = (ok: boolean, result?: unknown, error?: string): Uint8Array =>
						self.codec.encode_msg({ ok, result, error });
			
					if (authHeader !== `Bearer ${self.token}`) {
						return ack(false, undefined, "unauthorized");
					}
			
					const env = self.codec.decode_msg(bytes);
			
					if (env === null || typeof env !== "object") {
						return ack(false, undefined, "undecodable envelope");
					}
			
					const e = env as Partial<HttpEnvelope>;
			
					if (
						typeof e.rt !== "string" ||
						typeof e.ms !== "string" ||
						typeof e.sl !== "string" ||
						typeof e.key !== "string"
					) {
						return ack(false, undefined, "malformed envelope: rt/ms/sl/key required");
					}
			
					if (!self.dispatch) {
						return ack(false, undefined, "no dispatcher attached");
					}
			
					try {
						const result = await self.dispatch(env as HttpEnvelope);
						return ack(true, result);
					} catch (err) {
						return ack(
							false,
							undefined,
							err instanceof Error ? err.message : String(err),
						);
					}
				},
			}));
			z.GLOBAL_SUBSCRIPTION = this.ATOM.BIND("GLOBAL_SUBSCRIPTION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				globalListeners: new Set<LISTENER<W>>(),
			
				subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
					self.globalListeners.add(listener);
			
					return () => {
						self.globalListeners.delete(listener);
					};
				},
			
				notify(change: CHANGE<W>): void {
					for (const listener of self.globalListeners) {
						try {
							listener(change);
						} catch (error) {
							console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
						}
					}
				},
			
				get size(): number {
					return self.globalListeners.size;
				},
			
				purge(): void {
					self.globalListeners.clear();
				},
			}));
			z.METRICS = this.ATOM.BIND("METRICS", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
			
				adapters: {
					metrics: new Map<string, {
						id: string;
						total: number;
						failures: number;
						totalLatency: number;
						disabled: boolean;
						lastWindow: number;
						lastCount: number;
					}>(),
				},
			
				seen: new Set<string>(),
			
				getSeenCount(): number {
					return self.seen.size;
				},
			
				getMessageCount(rt: string): number {
					return self.messageCount.get(rt) ?? 0;
				},
			
				messageCount: new Map<string, number>(),
			
				getAdapterMetrics(): AdapterMetrics[] {
					const now = Date.now();
					const out: AdapterMetrics[] = [];
			
					for (const m of self.adapterMetrics.values()) {
						const windowSec = Math.max(1, (now - m.lastWindow) / 1000);
						const throughput = m.lastCount / windowSec;
						const failureRate = m.total === 0 ? 0 : m.failures / m.total;
						const latencyMs = m.total === 0 ? 0 : m.totalLatency / m.total;
			
						out.push({
							id: m.id,
							latencyMs,
							failureRate,
							throughputPerSec: throughput,
							healthy: !m.disabled && failureRate < 0.5,
							disabled: m.disabled,
						});
			
						m.lastWindow = now;
						m.lastCount = 0;
					}
			
					return out;
				},
			
				_queueMetrics: {
					totalEnqueued: 0,
					totalDropped: 0,
					totalProcessed: 0,
					peakSize: 0,
					currentSize: 0,
				},
			}));
			z.SUBSCRIPTION = this.ATOM.BIND("SUBSCRIPTION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				listeners: new Set<LISTENER<W>>(),
			
				subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
					self.listeners.add(listener);
			
					return () => {
						self.listeners.delete(listener);
					};
				},
			
				notify(change: CHANGE<W>): void {
					for (const listener of self.listeners) {
						try {
							listener(change);
						} catch (error) {
							console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
						}
					}
				},
			
				get size(): number {
					return self.listeners.size;
				},
			
				purge(): void {
					self.listeners.clear();
				},
			}));
			z.WAITER = this.ATOM.BIND("WAITER", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
			
				waiters: new Set<{
					resolve: (value: A) => void;
					reject: (reason?: A) => void;
				}>(),
			
				wait<T = A>(timeout?: number): Promise<T> {
					return new Promise<T>((resolve, reject) => {
						const waiter = {
							resolve: resolve as (value: A) => void,
							reject,
						};
			
						self.waiters.add(waiter);
			
						if (timeout !== undefined && timeout > 0) {
							const timer = setTimeout(() => {
								if (self.waiters.delete(waiter)) {
									reject(new Error("SOUL_ELECTRON wait timed out"));
								}
							}, timeout);
			
							const originalResolve = waiter.resolve;
							waiter.resolve = value => {
								clearTimeout(timer);
								originalResolve(value);
							};
			
							const originalReject = waiter.reject;
							waiter.reject = reason => {
								clearTimeout(timer);
								originalReject(reason);
							};
						}
					});
				},
			
				resolve<T = A>(value: T): number {
					const waiters = [...self.waiters];
					self.waiters.clear();
			
					for (const waiter of waiters) {
						waiter.resolve(value as A);
					}
			
					return waiters.length;
				},
			
				reject(reason: A): number {
					const waiters = [...self.waiters];
					self.waiters.clear();
			
					for (const waiter of waiters) {
						waiter.reject(reason);
					}
			
					return waiters.length;
				},
			
				purge(): void {
					self.reject(new Error("SOUL_ELECTRON waiter ELECTRON destroyed"));
				},
			}));
			z.SESSION = this.ATOM.BIND("SESSION", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				_cse_token: (config.cse_token ?? null) as S | null,
				_sessionId: null as S | null,
				_sessionToken: null as S | null,
				_session_started_at: null as N | null,
				_session_expires_at: null as N | null,
				_auth_vized: false as B,
				_auth_initialized: false as B,
				_sessionTtlMs: (config.sessionTtlMs ?? 1000 * 60 * 60 * 12) as N,
			
				// ── crypto ─────────────────────────────────────────────────────────────────────────────────
				computeHmacSha256(secret: S, data: S): S {
					if (BUS_CRYPTO) {
						const key = BUS_CRYPTO.to_utf8_bytes(secret);
						const mac = BUS_CRYPTO.hash_hmac(key, data);
						return BUS_CRYPTO.to_hex(BUS_CRYPTO.from_base64(mac));
					}
			
					return "";
				},
			
				// ── lifecycle ──────────────────────────────────────────────────────────────────────────────
				/**
				 * Stand the session up. Reads CSE_TOKEN from .env when no token was injected.
				 * No token anywhere? The session still runs, but UNAUTHORIZED (matches the bus's stance).
				 */
				async initAuth(): Promise<void> {
					if (self._auth_initialized) return;
			
					if (!self._cse_token && typeof process !== "undefined") {
						try {
						//   if (dotenv) dotenv.config();
						} catch {
							// env loader optional
						}
			
						const sk = (typeof process !== "undefined" && process.env?.CSE_TOKEN) || null;
						if (sk && sk.length >= 5) self._cse_token = sk;
					}
			
					self._newSession();
					self._auth_initialized = true;
				},
			
				_newSession(): void {
					const now = Date.now();
			
					if (self._cse_token && BUS_CRYPTO) {
						self._sessionId = BUS_CRYPTO.to_hex(BUS_CRYPTO.random_bytes(16));
						self._sessionToken = self.computeHmacSha256(self._cse_token, self._sessionId);
					} else {
						// degraded mode: still unique, just not cryptographically bound
						self._sessionId = `sess_${now}_${Math.random().toString(36).slice(2)}`;
						self._sessionToken = null;
					}
			
					self._session_started_at = now;
					self._session_expires_at = now + self._sessionTtlMs;
					self._auth_vized = !!self._sessionToken;
				},
			
				/** Rotate the session id + token. Anything wrapped before is invalid after this. */
				renew(): void {
					self._newSession();
				},
			
				/** Force-expire. secureUnwrap() will reject everything until renew() or initAuth(). */
				expire(): void {
					self._session_expires_at = Date.now() - 1;
					self._auth_vized = false;
				},
			
				isValid(): B {
					return self._session_expires_at !== null && Date.now() < self._session_expires_at;
				},
			
				get expired(): B {
					return !self.isValid();
				},
			
				get isSecure(): B {
					return self._cse_token !== null && self._sessionToken !== null;
				},
			
				// ── wrap / unwrap ──────────────────────────────────────────────────────────────────────────
				/** Wrap a payload with the cell's session identity. */
				secureWrap(p: A) {
					return {
						sessionId: self._sessionId,
						token: self._sessionToken,
						ts: Date.now(),
						p,
					};
				},
			
				/** Unwrap + verify. Tampered, stale, or foreign session → null. */
				async secureUnwrap(msg: A): Promise<A | null> {
					if (!msg?.sessionId || !msg?.token) return null;
					if (msg.sessionId !== self._sessionId) return null; // not our session
					if (!self.isValid()) return null; // our session but expired
			
					const expected = await self.computeHmacSha256(
						self._cse_token ?? "",
						msg.sessionId,
					);
			
					if (self._sessionToken && msg.token !== expected) return null;
			
					return msg.p;
				},
			
				purge(): void {
					self._sessionId = null;
					self._sessionToken = null;
					self._session_started_at = null;
					self._session_expires_at = null;
					self._auth_vized = false;
					self._auth_initialized = false;
				},
			}));
			z.ROUTE = this.ATOM.BIND("ROUTE", ({ self, master, config }: any) => ({
				k: config.k ?? "__SIGNALS__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				routeRules: [] as RouteRule[],
			
				toRegex(pattern: S): RegExp {
					return new RegExp(
						"^" +
							pattern
								.replace(/[.+?^${}()|[\]\\]/g, "\\$&")
								.replace(/\*\*/g, ".*")
								.replace(/\*/g, "[^.]+") +
							"$",
					);
				},
			
				addRouteRule(rule: RouteRule): void {
					self.routeRules.push(rule);
			
					if (self.DEVTOOLS_ACTIVE) {
						self.emitDevtoolsEvent("route_added", undefined, undefined, undefined, {
							name: rule.name,
						});
					}
				},
			
				removeRouteRule(name: S): void {
					self.routeRules = self.routeRules.filter(r => r.name !== name);
			
					if (self.DEVTOOLS_ACTIVE) {
						self.emitDevtoolsEvent("route_removed", undefined, undefined, undefined, { name });
					}
				},
			
				listRouteRules(): RouteRule[] {
					return [...self.routeRules];
				},
			
				/** First matching enabled rule wins. Returns the rewritten target(s); adapter defaults to same_boundary. */
				resolve(rt: S, ms: S, sl: S, k: S): RouteTarget[] {
					for (const rule of self.routeRules) {
						if (rule.enabled === false) continue;
						if (!self.toRegex(rule.match).test(rt)) continue;
			
						const target: RouteTarget = {
							adapter: rule.adapter ?? "same_boundary",
							rt: rule.target?.rt ?? rt,
							ms: rule.target?.ms ?? ms,
							sl: rule.target?.sl ?? sl,
							k: rule.target?.k ?? k,
						   // priority: rule.priority,
						};
			
						if (self.DEVTOOLS_ACTIVE) {
							self.emitDevtoolsEvent("route_resolved", rt, ms, undefined, {
								rule: rule.name,
								adapter: target.adapter,
							});
						}
			
						return [target];
					}
			
					// no rule: the identity target — same address, same boundary (PATCH 4's fallback)
					return [{ adapter: "same_boundary", rt, ms, sl, k }];
				},
			
				purge(): void {
					self.routeRules.length = 0;
				},
			}));
			z.RIFTS_ACCESS = this.ATOM.BIND("RIFTS_ACCESS", ({ self, master, config }: any) => ({
				k: config.k ?? "__MODULE_ATOM__",
				s: config.s ?? "SAURON_SLAVE_REG",
				m: config.m ?? "SAURON_MASTER_REG",
				r: config.r ?? "THE_WIZARD_ROOT_REG",
			
				initial: config.options?.module_type === "class_fn_pool" || !config.options?.module_type ? { ...config.initial } : config.initial,
				module: config.options?.module_type === "class_fn_pool" || !config.options?.module_type ? { ...config.initial } : config.initial,
				module_type: config.options?.module_type ?? "class_fn_pool",
				version: 0,
				sealed: false,
				frozen: (config.options?.module_type ?? "class_fn_pool") === "class_fn_pool"        ? false        : config.options?.frozen ?? true,
				hasWritten: false,
				persisted: false,
				listeners: new Set<(change: Change<A>) => void>(),
				waiters: new Set<(value: A) => void>(),
				undoStack: undefined as HistoryEntry[] | undefined,
				conflict: config.options?.conflict as ConflictPolicy | undefined,
				historyIndex: 0,
				history: config.options?.history ? [{ k: 0, value: config.initial, timestamp: Date.now(), source: "initial", }] : undefined,
				historyMax: config.options?.history ? Math.max(1, config.options.history.max ?? 50) : 50,
				lifecycle: config.options?.lifecycle as SignalLifecycle | undefined,
				async: undefined as AsyncSignalState | undefined,
				metrics: undefined as Metrics | undefined,
				conflictStrategy: config.options?.conflictStrategy as        "latest" | "fail" | "accumulate" | undefined,
				schema: config.options?.schema as SignalSchema | undefined,
				contracts: new Map<string, AtomContract<A>>(),
				causal: [] as CausalEntry[],
				lastWriteSource: undefined as string | undefined,
				batches: new Set<SignalKey>(),
				batchQueue: new Map<SignalKey, unknown>(),
				batchOpts: new Map<SignalKey, { triggerRender?: boolean }>(),
			
				triggerRender: true,
				boundary: "same-app" as Boundary,
				read<T = A>(): T { return self.module;    },
				peek<T = A>(): T { return self.module;    },
				write<T = A>( incoming: any | ((prev: T) => T), options: AtomWriteOptions = {}, ): T {
					if (self.frozen) return self.peek();
					const prev = self.module;
					const resolved = incoming;
					const next = self.module_type === "class_fn_pool" ? resolved ? { ...prev, ...resolved } : prev : resolved;
					self.module = next;
					self.hasWritten = true;
					self.version += 1;
					self.lastWriteSource = options.source ?? "write";
					self.assertContracts(next, prev);
			
					for (const l of self.listeners) { try { l(next); } catch (e) {} }
					root.bus.send(next,self.k,self.s, self.m, self.r, );
			 
					for (const w of self.waiters) { try { w(next); } catch (e) {} }
					if (options.recordHistory !== false) { self.recordHistory(prev, next, self.lastWriteSource); }
					self.recordCausal(next, self.lastWriteSource, options.cause);
					self.waiters.clear();
			
					self.notifyListeners({
						key: self.key,
						previous: prev,
						current: next,
						version: self.version,
						timestamp: Date.now(),
						source: self.lastWriteSource,
					});
			
					return next;
				},
				register<T = A>(name: string, val: any): T { return self.write((prev: A) => ({ ...prev, [name]: val }));    },
				unregister<T = A>(name: string): T {
					return self.write((prev: A) => {
						const { [name]: _gone, ...rest } = prev;
						return rest;
					});
				},
				slot(name: string): any { return self.module[name]; },
				on(listener: RiftListener<any>): void { self.listeners.add(listener); listener(self.module); },
				waitFor<T = A>(): Promise<T> { return new Promise(resolve => { self.waiters.add(resolve); });    },
				async waitForSlot(name: string, timeoutMs = 0): Promise<any> {
					const existing = self.module[name];
					if (existing !== undefined) return existing;
			
					if (!timeoutMs) {
						const next = await self.waitFor();
						return (next as Record<string, any>)[name];
					}
			
					return Promise.race([
						self.waitFor().then(next => (next as Record<string, any>)[name]),
						new Promise((_, reject) =>
							setTimeout(
								() => reject(
									new Error(`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}ms`),
								),
								timeoutMs,
							),
						),
					]);
				},
				inspect<T = A>(): T { return { ...self.module }; },
				setSchema(schema?: SignalSchema) { self.schema = schema; return self; },
				setLifecycle<T = A>(lifecycle?: SignalLifecycle<T>) { self.lifecycle = lifecycle; return self; },
				setConflict( conflict?: ConflictPolicy, strategy?: "latest" | "fail" | "accumulate", ) {
					self.conflict = conflict;
					self.conflictStrategy = strategy;
					return self;
				},
				setFrozen(): void { self.frozen = true; },
				setUnfrozen(): void { self.frozen = false; },
				setSealed(): void { self.sealed = true; },
				setUnsealed(): void { self.sealed = false; },
				setBoundary() {},
				validate<T = A>(next: T): T {
					if (!self.schema) return next;
					return root.signals._.validateSchema(self.schema, next);
				},
			
				assertContracts<T = A>(next: T, previous: T): void {
					for (const contract of self.contracts.values()) {
						let valid = false;
			
						try {
							valid = contract.postcondition(next, previous);
						} catch (error) {
							console.error(
								`[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`,
								error,
							);
						}
			
						if (valid) continue;
			
						const message =
							`[Sauron] contract "${contract.name}" violated ` +
							`on write to "${String(self.key)}"`;
			
						if (contract.block) {
							throw new Error(message);
						}
			
						console.error(message);
					}
				},
			
				recordHistory<T = A>(previous: T, next: T, source?: string): void {
					if (!self.history) return;
			
					self.history.splice(self.historyIndex + 1);
			
					const last = self.history[self.history.length - 1];
			
					self.history.push({
						k: (last?.k ?? -1) + 1,
						prev: previous,
						value: next,
						next,
						timestamp: Date.now(),
						source,
						undone: false,
					});
			
					while (self.history.length > self.historyMax) {
						self.history.shift();
					}
			
					self.historyIndex = self.history.length - 1;
				},
			
				getHistory<T = A>(): readonly AtomHistoryEntry<T>[] {
					return self.history ?? [];
				},
			
				timeTravel<T = A>(steps = 1): T | undefined {
					if (!self.history?.length || steps <= 0) return undefined;
			
					const idx = Math.max(
						0,
						Math.min(self.history.length - 1, self.historyIndex + steps),
					);
			
					if (idx === self.historyIndex) return undefined;
			
					self.historyIndex = idx;
					return self.replayHistory("time-travel");
				},
			
				back<T = A>(steps = 1): T | undefined {
					return self.timeTravel(-Math.abs(steps));
				},
			
				forward<T = A>(steps = 1): T | undefined {
					return self.timeTravel(Math.abs(steps));
				},
			
				goto<T = A>(sequence: number): T | undefined {
					if (!self.history?.length) return undefined;
			
					const idx = self.history.findIndex(entry => entry.k === sequence);
					if (idx < 0 || idx === self.historyIndex) return undefined;
			
					self.historyIndex = idx;
					return self.replayHistory("time-travel.goto");
				},
			
				replayHistory<T = A>(source: string): T {
					const entry = self.history![self.historyIndex];
			
					return self.write(entry.value as T, {
						source,
						cause: "time-travel",
						triggerRender: true,
						recordHistory: false,
					});
				},
			
				recordCausal<T = A>(value: T, source?: string, cause?: string): void {
					self.causal.push({
						value,
						source,
						cause,
						timestamp: Date.now(),
					} as CausalEntry);
			
					if (self.causal.length > 100) {
						self.causal.shift();
					}
				},
			
				recordMetrics(): void {
					if (!self.metrics) return;
			
					self.metrics.writeCount += 1;
					self.metrics.lastWrite = Date.now();
				},
			
				notifyListeners<T = A>(change: Change<T>): void {
					const listeners = self._listeners.get(self.key);
					if (!listeners) return;
			
					for (const listener of listeners) {
						try {
							listener(change);
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`,
								error,
							);
						}
					}
				},
			
				resolveWaiters<T = A>(value: T): void {
					const waiting = self._waiters.get(self.key);
					if (!waiting) return;
			
					self._waiters.delete(self.key);
			
					for (const resolve of waiting) {
						try {
							resolve(value);
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`,
								error,
							);
						}
					}
				},
			
				notifyExternalSubscribers(): void {
					const subscribers = self._externalSubscribers.get(self.key);
					if (!subscribers) return;
			
					for (const callback of subscribers) {
						try {
							callback();
						} catch (error) {
							console.error(
								`[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`,
								error,
							);
						}
					}
				},
			}));
			return z
		})(); 
		public contracts = (() => {
			const z = () => {}
			z.DEFAULT = this.ATOM.CONTRACT(() => ({ atom: "VERTEX" }))
			z.vertexContract = this.ATOM.CONTRACT(() => ({
				atom: "VERTEX",
				value: { count: 0 },
				bindables: { conflict: CONFLICT, ttl: TTL },
			}));
			z.atomMasterContract = this.ATOM.CONTRACT(() => ({
				atom: "ATOM",
				value: 0,
				bindables: { ttl: TTL },
			}));
			z.moduleContract = this.ATOM.CONTRACT(() => ({
				atom: "MODULE",
				value: [],
				bindables: { conflict: CONFLICT },
			}));
			return z
		})(); 
		public atoms_def = new (class DEFINITION {
			public MASTER_ATOM_KEYS = ["ATOM", "MODULE", "CLOUD", "SAURON", "DRS", "MSG_Q", "SLAB",  "S_CLASS", "SPREADSHEET",  "VERTEX"] as const;
			public MQ = {
				RETRY: this.DEFINE_ATOM_KIND("retry"),
				ACK: this.DEFINE_ATOM_KIND("ack"),
				DEADLINE: this.DEFINE_ATOM_KIND("deadline"),
				DEDUPE: this.DEFINE_ATOM_KIND("dedupe"),
				DEPTH: this.DEFINE_ATOM_KIND("depth"),
				WILDCARD: this.DEFINE_ATOM_KIND("wildcard"),
				REPLY: this.DEFINE_ATOM_KIND("reply"),
				WINDOW: this.DEFINE_ATOM_KIND("window"),
				RESUME: this.DEFINE_ATOM_KIND("resume"),
				CREDIT: this.DEFINE_ATOM_KIND("credit"),
				CORRELATION: this.DEFINE_ATOM_KIND("correlation"),
				SEND: this.DEFINE_ATOM_KIND("send"),
				SUB: this.DEFINE_ATOM_KIND("send"),
				STREAM: this.DEFINE_ATOM_KIND("send"),
				HANDLER: this.DEFINE_ATOM_KIND("send"),
				DUPLEX: this.DEFINE_ATOM_KIND("send"),
				DRS: this.DEFINE_ATOM_KIND("send"),
				ANTIMSGQUEUE: this.DEFINE_ATOM_KIND("send"),
			} as const;
			public ELECTRONS = {
				AW_SET: this.DEFINE_ATOM_KIND("adapter"),
				CASUAL_CURSOR: this.DEFINE_ATOM_KIND("adapter"),
				DELTA_STORE: this.DEFINE_ATOM_KIND("adapter"),
				G_SET: this.DEFINE_ATOM_KIND("adapter"),
				VERSION_VECTOR: this.DEFINE_ATOM_KIND("adapter"),
				CIRCUIT_BREAKER: this.DEFINE_ATOM_KIND("adapter"),
				CQRS: this.DEFINE_ATOM_KIND("adapter"),
				RAFT: this.DEFINE_ATOM_KIND("adapter"),
				YJS: this.DEFINE_ATOM_KIND("adapter"),
				DIFFPATCH: this.DEFINE_ATOM_KIND("adapter"),
				HYBRID_LOGICAL_CLOCK: this.DEFINE_ATOM_KIND("adapter"),
				LWW: this.DEFINE_ATOM_KIND("adapter"),
				LWW_ELEMENTAL: this.DEFINE_ATOM_KIND("adapter"),
				MV: this.DEFINE_ATOM_KIND("adapter"),
				OPTIMISTIC: this.DEFINE_ATOM_KIND("adapter"),
				OR: this.DEFINE_ATOM_KIND("adapter"),
				PN: this.DEFINE_ATOM_KIND("adapter"),
				REVERSE_HEARTBEAT_MGR: this.DEFINE_ATOM_KIND("adapter"),
				SEMAPHORE: this.DEFINE_ATOM_KIND("adapter"),
				TOMBSTONE_COMPACTOR: this.DEFINE_ATOM_KIND("adapter"),
				YIMIR: this.DEFINE_ATOM_KIND("adapter"),
				HYBRID_LOGICAL_CLOCK_CRDT: this.DEFINE_ATOM_KIND("adapter"),
				ADAPTER: this.DEFINE_ATOM_KIND("adapter"),
				BEAM_CODEC: this.DEFINE_ATOM_KIND("beam_codec"),
				BINARY: this.DEFINE_ATOM_KIND("binary"),
				BYTE_CODE_VM: this.DEFINE_ATOM_KIND("byte_code_vm"),
				CHANNEL: this.DEFINE_ATOM_KIND("channel"),
				CHANNELS: this.DEFINE_ATOM_KIND("channels"),
				CONCURRENCY: this.DEFINE_ATOM_KIND("concurrency"),
				DEADLETTER: this.DEFINE_ATOM_KIND("deadletter"),
				DESTRUCTION: this.DEFINE_ATOM_KIND("destruction"),
				ERROR: this.DEFINE_ATOM_KIND("error"),
				EVENT: this.DEFINE_ATOM_KIND("event"),
				EXTERNAL_SUBSCRIPTION: this.DEFINE_ATOM_KIND("exterenal_sub"),
				GATE: this.DEFINE_ATOM_KIND("gate"),
				GLOBAL_SUBSCRIPTION: this.DEFINE_ATOM_KIND("global_sub"),
				GOVERNOR: this.DEFINE_ATOM_KIND("GOVERNOR"),
				HOOK: this.DEFINE_ATOM_KIND("hook"),
				HTTP_TRANSPORT: this.DEFINE_ATOM_KIND("http_transport"),
				INJECTION_CODEC: this.DEFINE_ATOM_KIND("injection"),
				METRICS: this.DEFINE_ATOM_KIND("metrics"),
				MIDDLEWARE: this.DEFINE_ATOM_KIND("middlleware"),
				RIFTS_ACCESS: this.DEFINE_ATOM_KIND("rifts"),
				REACTIVE: this.DEFINE_ATOM_KIND("reactive"),
				ROUTE: this.DEFINE_ATOM_KIND("route"),
				RUN_ON: this.DEFINE_ATOM_KIND("run_on"),
				SEALED_ERROR: this.DEFINE_ATOM_KIND("sealed_error"),
				SESSION: this.DEFINE_ATOM_KIND("session"),
				SHELL: this.DEFINE_ATOM_KIND("reshelltry"),
				SIGNAL: this.DEFINE_ATOM_KIND("signal"),
				SOA_QUAD_TIERED_REGISTRY: this.DEFINE_ATOM_KIND("soa_registry"),
				SUBSCRIPTION: this.DEFINE_ATOM_KIND("subscription"),
				TRANSFORM: this.DEFINE_ATOM_KIND("transform"),
				WAITER: this.DEFINE_ATOM_KIND("waiter"),
				CONTRACTS: this.DEFINE_ATOM_KIND("contract"),
				SCHEMA: this.DEFINE_ATOM_KIND("schema"),
				VALUES_REGISTRY: this.DEFINE_ATOM_KIND("values_reg"),
				COMPRESSION: this.DEFINE_ATOM_KIND("compression"),
				CONFLICT: this.DEFINE_ATOM_KIND("conflict"),
				CONTEXT: this.DEFINE_ATOM_KIND("context"),
				ENCRYPTION: this.DEFINE_ATOM_KIND("encryption"),
				GROUPS: this.DEFINE_ATOM_KIND("groups"),
				HISTORY: this.DEFINE_ATOM_KIND("history"),
				META: this.DEFINE_ATOM_KIND("meta"),
				OFFSEC: this.DEFINE_ATOM_KIND("offsec"),
				PERMISSIONS: this.DEFINE_ATOM_KIND("permissions"),
				PERSISTENCE: this.DEFINE_ATOM_KIND("persistence"),
				PRIORITY: this.DEFINE_ATOM_KIND("priority"),
				SNAPSHOT: this.DEFINE_ATOM_KIND("snapshot"),
				TAGS: this.DEFINE_ATOM_KIND("tags"),
				TIME_TRAVEL: this.DEFINE_ATOM_KIND("time_travel"),
				TTL: this.DEFINE_ATOM_KIND("ttl"),
			} as const;
			public DEFINE_ATOM_KIND(name: string): symbol {
				return Symbol(name);
			}
			public PARTICLE_KEYS = {
				...this.ELECTRONS,
				...this.MQ,
			} as const;
		})();
		// #endregion
		//                                                                                                                          API
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public atom<T = any>(v: A, key: SignalKey = `__THE_WARLOCK_SIGNALS_${this.hub.REGISTRY._sid()}`, s = this.s, m = this.m, r = this.r, options?: SignalOptions) {
			const rk = this.create(key, v, options, s, m, r);
			const self = this;
			return {
				get value(): T {
					self._.bindComponentToSignal(rk, s, m, r);
					return self.read<T>(rk, s, m, r) as T;
				},
				get current(): T {
					self._.bindComponentToSignal(rk, s, m, r);
					return self.read<T>(rk, s, m, r) as T;
				},
				peek(): T {
					return rk.peek();
				},
				set value(v: T) {
					self.write(rk, v, "write", s, m, r);
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
		public create<T = A>(k: SignalKey, v?: T | (() => T), options?: SignalOptions, s = this.s, m = this.m, r = this.r): ATOM<T> | T {
			const registry = this.hub.wr._signals;
			let atom = registry.get(k);
	
			if (atom && v === undefined) { return atom as ATOM<T> | T; }
			if (atom && v !== undefined) {
				const vv = typeof v === "function" ? (v as () => T)() : v;
				atom.write(vv)
				return atom as ATOM<T> 
			}
			const initialValue = typeof v === "function" ? (v as () => T)() : v;
			atom = this._contracts.DEFAULT(initialValue, k, s, m, r);
			registry.set(k, atom);
			return atom as ATOM<T> | T;
		}
		public get<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): A {
			const registry = this.hub.wr._signals;
			return registry.get(k) as ATOM<T> | UD;
		}
		public write<T = A>(k: SignalKey, value: T | ((previous: T) => T), source = "write", s = this.s, m = this.m, r = this.r): T {
			return this._.writeResolved(k, value, source, s, m, r);
		}
		public read<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
			const atom = this.hub.wr._signals.get(k) as ATOM<T> | undefined;
			return atom?.read();
		}
		public peek<T = A>(k: SignalKey, s = this.s, m = this.m, r = this.r): T | UD {
			const atom = this.hub.wr._signals.get(k) as ATOM<T> | UD;
			return atom?.peek();
		}
		public signal<T>(initialValue?: T): { (): T | UD; (value: T): V } {
			const eng = this.DARTH_MAUL_ENGINE;
			const pid = eng.signal<T>(initialValue);          
			const fn = ((...args: [] | [T]) => eng.signalOper<T>(pid, ...args)) as A;
			fn.pid = pid;
			Object.defineProperty(fn, eng.BRAND_SIGNAL, { value: true });
			return fn;                                        
		}
		public has(k: A, s = this.s, m = this.m, r = this.r): B { return this.hub.wr._signals.get(k) !== undefined; }
		public freeze(k: A, s = this.s, m = this.m, r = this.r): void {
			const atom = this.hub.wr._signals.get(k);
			if (!atom) { throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`); }
			atom.setFrozen();
		}
		public unfreeze(k: A, s = this.s, m = this.m, r = this.r): void {
			const atom = this.hub.wr._signals.get(k);
			if (!atom) { throw new Error(`[Sauron] Cannot freeze unknown signal "${String(k)}"`); }
			atom.setUnfrozen();
		}
		public seal(k: A, s = this.s, m = this.m, r = this.r): V {
			const meta = this.hub.wr._signals;
			const atom = meta.get(k);
			atom.setSealed();
		}
		public reset(k: A, s = this.s, m = this.m, r = this.r): V {
			const atom = this.hub.wr._signals.get(k) as ATOM<A> | UD;
			if (!atom) { throw new Error(`[Sauron] Cannot reset unknown signal "${String(k)}"`); 	}
			atom.value = atom.initial;
			// this._writeResolved(k, atom.initial, "reset", s, m, r, "reset");
		}
		public hydrate<T = ATOM<A> | A>(k: A, value: T, s = this.s, m = this.m, r = this.r): V {
			let atom = this.hub.wr._signals.get(k) as ATOM<T> | UD;
			if (!atom) { this.create(k, value, undefined, s, m, r); return; }
			// atom.write(value, { source: "hydrate", triggerRender: false });
		}
		public tx(fn: () => V): V {
			const o = { s: this.s, m: this.m, r: this.r };
			const snap = new Map<string, A>();
			for (const [k, v] of Object.entries(this.hub.state.state.getAll(o))) snap.set(k, v);
			const sysSnap = this._sys.snapshot();
			const self = this;
			try {
				fn();
			} catch (err) {
				for (const k of Object.keys(this.hub.state.state.getAll(o))) if (!snap.has(k)) this.hub.state.state.delete(k, o);
				for (const [k, v] of snap) this.hub.state.state.set(k, v, { ...o, triggerRender: false });
				self._sys.dropComputeds();
				self._sys.restore(sysSnap);
				throw err;
			}
		}
		public derive = (() => {
			const fn = <T,>(k: A, deps: A[], compute: (...values: A[]) => T, s = this.s, m = this.m, r = this.r, options?: { eager?: B; onError?: (err: Error) => V }): S => {
				if (this._getMetadata(s, m, r)._sys.get(k)?.isComputed) return k;
				const reg = this.hub.wr._signals;
				for (const d of deps) {
					this._sys.ensure(d, reg.get(d), s, m, r);
				}
	
				const c = computed(() => {
					try {
						const vals = deps.map((d) => this._sys.ensure(d, reg.get(d), s, m, r).read());
						return compute(...vals);
					} catch (err) {
						options?.onError?.(err as Error);
						throw err;
					}
				});
	
				this._getMetadata(s, m, r)._sys.set(k, { read: () => c(), write: () => {}, node: c as A, isComputed: true });
				reg.set(
					k,
					this.DARTH_MAUL_ENGINE.untrack(() => c()),
				);
	
				if (options?.eager) {
					// Warm the computed cache once. Do not write its result back
					// into another signal; that defeats lazy recomputation.
					this.DARTH_MAUL_ENGINE.untrack(() => c());
				}
				return k;
			};
			fn.invalidate = (k: A, s = this.s, m = this.m, r = this.r): V => {
				const entry = this._getMetadata(s, m, r)._sys.get(k);
				if (!entry || !entry.isComputed) return;
				this._.writeResolved(
					k,
					this.DARTH_MAUL_ENGINE.untrack(() => entry.read()),
					"derived",
					"invalidate",
					s,
					m,
					r,
				);
			};
			return fn;
		})();
		// #endregion
		//                                                                                                                   SEND / SUB
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		#defBucket?: M2<A, A>;
		#bufs = new WM<O, A[]>(); // open batch buffers, touched only inside batches
		private bucket(s: S, m: S, r: S): M2<A, A> {
			if (s === this.s && m === this.m && r === this.r) {
				return (this.#defBucket ??= this.hub.wr.map(s, m, r, "#NAKED_SIGNALS") as M2<A, A>);
			}
			return this.hub.wr.map(s, m, r, "#NAKED_SIGNALS") as M2<A, A>;
		}
		private channel(k: A, s: S, m: S, r: S, create: B): A {
			const b = this.bucket(s, m, r);
			let sig = b.get(k);
			if (!sig && create) {
				sig = this.signal<A>(undefined);
				b.set(k, sig);
			}
			return sig;
		}
		public sub = (() => {
			const fn = <T = A,>(listener: (value: T, previous: T) => A, k: A, s = this.s, m = this.m, r = this.r) => {
				const eng = this.DARTH_MAUL_ENGINE;
				const sig = this.channel(k, s, m, r, true);
				let prev: T | UD;
				let first = true;
				let live = true;
		
				const pid = eng.effectSignal(() => {
					const env = sig();
					if (first) { first = false; return; }
					if (!env) return;
					eng.untrack(() => {
						if (env.b !== undefined) {
							// batched delivery: replies can't make it back, ignore return values
							if (this.#bufs.get(sig) === env.b) this.#bufs.delete(sig);
							for (const p of env.b) {
								try { listener(p, prev as T); } catch (e) { console.error("[bus]", e); }
								prev = p;
							}
						} else {
							try {
								const v = listener(env.p, prev as T);
								if (v !== undefined) (env.r ??= []).push(v); // array only allocated if someone replies
							} catch (e) { console.error("[bus]", e); }
							prev = env.p;
						}
					});
					// effect body returns nothing on purpose: a returned function would be treated as cleanup
				});
		
				return () => { if (live) { live = false; eng.dispose(pid); } };
			};
			fn.naked = (k: A, s = this.s, m = this.m, r = this.r) => this.channel(k, s, m, r, true);
			return fn;
		})();    
		public send = (() => {
			const dispatch = (p: A, k: S, s: S, m: S, r: S): A[] | UD => {
				const sig = this.channel(k, s, m, r, false);
				if (!sig) return;
		
				if (this.DARTH_MAUL_ENGINE.batchDepth > 0) {
					const buf = this.#bufs.get(sig);
					if (buf) { buf.push(p); return; }
					const b = [p];
					this.#bufs.set(sig, b);
					sig({ b });
					return; // flush is deferred, so there are no replies yet
				}
		
				const env: A = { p, r: undefined };
				sig(env);       // synchronous commit + flush: listeners have already run
				return env.r;
			};
			const fn = (p: A, k: S, s = this.s, m = this.m, r = this.r): A => dispatch(p, k, s, m, r)?.[0];
			fn.all = (p: A, k: S, s = this.s, m = this.m, r = this.r): A[] => dispatch(p, k, s, m, r) ?? [];
			return fn;
		})();
		public delete = (k: A, s = this.s, m = this.m, r = this.r): V => {
			this.hub.wr._signals.delete(k);
			this._getMetadata(s, m, r)._sys.delete(k);
		};
		// #endregion
		//                                                                                                               [TRACE EFFECT]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public trace = (() => {
			const fn = (fn: () => V): { reads: SignalKey[]; result: V } => {
				const reads = new Map<SignalKey, SIGNAL_CELL>();
				const previous = this.activeTracer;
				this.activeTracer = reads;
				try {
					const result = fn();
					return { reads: [...reads.keys()], result };
				} finally {
					this.activeTracer = previous;
				}
			};
			fn.effect = (fn: () => V | (() => V), s: S = this.s, m: S = this.m, r: S = this.r) => {
				const reads = new Map<SignalKey, A>();
				const runs: Array<{ run: number; reads: Map<SignalKey, A>; at: number }> = [];
				let runCount = 0;
				const tracked = new Map<SignalKey, SIGNAL_CELL>();
				const registry = this.hub.wr._signals;
	
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
			}; // Finds the first recorded write whose value differs from the current live value. That is the divergence point.
			fn.bisect = (entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: N; source?: S }>, s: S = this.s, m: S = this.m, r: S = this.r): { index: number; key: SignalKey; recorded: A; actual: A } | UD => {
				const registry = this.hub.wr._signals;
	
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
			};
			fn.why = <T = A,>(k: SignalKey, s: S = this.s, m: S = this.m, r: S = this.r): { key: SignalKey; value: T | UD; version: N; lastWriteSource?: S; causal: CausalEntry[]; history: readonly AtomHistoryEntry<T>[] } | UD => {
				const atom = this.hub.wr._signals.get(k) as SIGNAL_CELL<T> | undefined;
	
				if (!atom) return undefined;
	
				return {
					key: atom.key,
					value: atom.peek(),
					version: atom.version,
					lastWriteSource: atom.lastWriteSource,
					causal: [...atom.causal].reverse(),
					history: atom.getHistory(),
				};
			};
			return fn;
		})();
		// #endregion
		//                                                                                                              [STATE SURGERY]
		// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public surgery = (() => {
			// Live-state surgery. Reads the current value, hands it to `mutate`, writes the result back through the normal reactive write path, records a causal entry, and pushes an undo entry onto the surgery journal.
			// The mutation receives the current value. It may either: - mutate and return nothing (object/array mutation), or - return a replacement value.
			const fn = <T = A,>(k: SignalKey, mutate: (draft: T) => T | void, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S; record?: B }): T => {
				const atom = this.hub.wr._signals.get(k) as ATOM<T> | UD;
				if (!atom) {
					throw new Error(`[Sauron] Cannot perform surgery on unknown signal "${String(k)}"`);
				}
				if (atom.frozen) {
					throw new Error(`[Sauron] Cannot perform surgery on frozen signal "${String(k)}"`);
				}
				const previous = atom.peek(); // Structured clone gives surgery isolation without touching the reactive kernel. Primitive values pass through unchanged.
				const draft = previous !== null && typeof previous === "object" ? (structuredClone(previous) as T) : previous;
				const result = mutate(draft);
				const next = result === undefined ? draft : (result as T);
				if (Object.is(previous, next)) {
					return previous;
				}
	
				const source = options?.source ?? "surgery";
				const cause = options?.cause ?? `surgery:${String(k)}:${Date.now()}`;
	
				const written = atom.write(next);
	
				this.surgery.journal.push({ key: k, previous, next: written, s, m, r });
	
				return written;
			}; //  Undo the most recent surgery. Restoration goes through the normal write path, so computeds, effects, subscribers, history, and channels all see a real write.
			fn.journal = [] as Array<{ key: SignalKey; previous: A; next: A; s: S; m: S; r: S }>;
			fn.undo = (): A | UD => {
				const entry = this.surgery.journal.pop();
	
				if (!entry) return undefined;
	
				const atom = this.hub.wr._signals.get(entry.key) as SIGNAL_CELL | undefined;
	
				if (!atom) return undefined;
	
				return atom.write(entry.previous, {
					source: "surgery:undo",
					cause: `surgery:undo:${String(entry.key)}`,
					triggerRender: true,
					recordHistory: true,
				});
			}; //   Undo every surgery performed since the returned marker was created. Undo every surgery performed since the returned marker was created.
			fn.checkpoint = (): (() => V) => {
				const mark = this.surgery.journal.length;
				return () => {
					while (this.surgery.journal.length > mark) {
						this.surgery.undo();
					}
				};
			}; // Surgery on a nested path. Example: sauron.surgeryAt('user', ['profile', 'email'], email => email.trim());
			fn.at = <T = A, P = A>(k: SignalKey, path: Array<S | N | SY>, mutate: (draft: P) => P | V, s: S = this.s, m: S = this.m, r: S = this.r, options?: { source?: S; cause?: S }): T => {
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
			};
			return fn;
		})();
		public _sys = (() => {
			const fn = () => {};
			fn.ensure = (rk: S, v: A, s = this.s, m = this.m, r = this.r): SysEntry => {
				const sysMap = this._getMetadata(s, m, r)._sys;
				let entry = sysMap.get(rk);
				if (entry) return entry;
				const sig = signal(v);
				entry = { read: () => sig(), write: (v: A) => sig(v), node: sig as A, isComputed: false };
				sysMap.set(rk, entry);
				if (this.hub.state.state.get(rk, { s, m, r }) === undefined) {
					this.hub.state.state.set(rk, v, { s, m, r, triggerRender: false });
				}
				return entry;
			};
			fn.snapshot = (s = this.s, m = this.m, r = this.r): M<S, A> => {
				const out = new Map<string, A>();
				for (const [rk, entry] of this._getMetadata(s, m, r)._sys) {
					if (!entry.isComputed)
						out.set(
							rk,
							this.DARTH_MAUL_ENGINE.untrack(() => entry.read()),
						);
				}
				return out;
			};
			fn.restore = (snap: M<S, A>): V => {
				for (const [rk, v] of snap) this._sys.ensure(rk, v).write(v);
			};
			fn.dropComputeds = (s = this.s, m = this.m, r = this.r): V => {
				for (const [rk, entry] of this._getMetadata(s, m, r)._sys) {
					if (entry.isComputed) this._getMetadata(s, m, r)._sys.delete(rk);
				}
			};
			return fn;
		})();
		// #endregion
		//                                                                                                                      BACKEND
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public _ = (() => {
			const fn = () => {};
			fn.validateSchema = <T,>(schema: SignalSchema, value: T): T => {
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
			};
			fn.serializeTuple = (tuple: A[], s = this.s, m = this.m, r = this.r): S => {
				const parts: string[] = new Array(tuple.length);
				const weakMap = this._getMetadata(s, m, r)._weakMap;
	
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
						parts[i] = `arr:[${this._.serializeTuple(item, s, m, r)}]`;
					} else if (type === "object" || type === "function") {
						let refId = weakMap.get(item);
						if (!refId) {
							refId = `ref:${this.hub.REGISTRY._sid()}`;
							weakMap.set(item, refId);
						}
						parts[i] = refId;
					} else {
						parts[i] = `v:${String(item)}`;
					}
				}
	
				return parts.join("|");
			};
			fn.hashTuple = (tuple: U[], s = this.s, m = this.m, r = this.r): S => {
				const key = this._.serializeTuple(tuple, s, m, r);
				const tupleCache = this._getMetadata(s, m, r)._tupleCache;
	
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
			};
			fn.writeResolved = <T,>(k: SignalKey, value: T | ((previous: T) => T), source: S, s = this.s, m = this.m, r = this.r, cause?: S): T => {
				const registry = this.hub.wr._signals;
				let atom = registry.get(k) as ATOM<T> | UD;
	
				if (!atom) {
					const initial = typeof value === "function" ? undefined : (value as T);
					atom = this.create(k, initial, undefined, s, m, r) as ATOM<T>;
				}
	
				if (!atom) {
					throw new Error(`[Sauron] Cannot write unknown signal "${String(k)}"`);
				}
	
				return atom.write(value);
			};
			fn.recordCausal = <K, V>(k: S, value: V, source: S, s = this.s, m = this.m, r = this.r, cause?: S): void => {
				const atom = this.hub.wr._signals.get(k);
				if (!atom) return;
				atom.causal.push({ key: k, value, timestamp: Date.now(), source, cause });
				if (atom.causal.length > 100) {
					atom.causal.shift();
				}
			};
			fn.bindComponentToSignal = ( rk: S, s = this.s, m = this.m, r = this.r  ): V => {
				const component = this.signals.rx.getCurrentComponent();
				if (!component) return;
				const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
				if (component.hooks.has(hookKey)) return;
				const atom = this.create(rk, undefined, undefined, s, m, r);
				if (!atom) return;
				const hook: HookState = { value: atom.read(), bindings: new Set() };
				component.hooks.set(hookKey, hook);
				const unsubSignal = atom.subscribeExternal(() => { hook.value = atom.read(); this.signals.rx.notifyHook(hookKey); });
				component.unmount.add(unsubSignal);
				if (component.render) {
					const unsubRender = this.signals.rx.subscribeToHook( hookKey, () => component.render?.() );			
					component.unmount.add(unsubRender);
				}
			};
			fn.bindComponentToSignal2 = (rk: S, s = this.s, m = this.m, r = this.r): V => {
				const component = this.hub.state.getCurrentComponent();
				if (!component) return;
				const hookKey = `${component.id}:signal:${r}:${m}:${s}:${rk}`;
				if (component.hooks.has(hookKey)) {
					return;
				}
				component.hooks.set(hookKey, { value: undefined });
	
				const atom = this.create(rk, undefined, undefined, s, m, r);
				if (!atom) { return; }
	
				const unsubSignal = atom.subscribeExternal(() => {
					this.hub.state.notifyHook(hookKey);
				});
				component.unmount.add(unsubSignal);
	
				if (component.render) {
					const unsubRender = this.hub.state.subscribeToHook(hookKey, () => component.render?.());
					component.unmount.add(unsubRender);
				}
			};
			fn.resolveConflict = <T,>(conflict: ConflictPolicy | undefined, previous: T, next: T, strategy: "latest" | "fail" | "accumulate" = "latest"): T | null => {
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
			};
			return fn;
		})();
		// #endregion
		//                                                                                                                        ASYNC
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public async = (() => {
			const fn = <T,>(k: A, fetcher: (signal: AbortSignal) => P<T>, s = this.s, m = this.m, r = this.r, options?: { fallback?: T }) => {
				const reg = this.hub.wr._signals;
				let atom = reg.get(k) as SIGNAL_CELL<T> | UD;
	
				if (!atom) {
					atom = this.create(k, options?.fallback as T, { trigger_render: false }, s, m, r) as SIGNAL_CELL<T>;
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
	
					this._.writeResolved(loadingKey, true, "write", s, m, r, String(k));
	
					try {
						const data = await fetcher(controller.signal);
						if (!controller.signal.aborted) {
							this._.writeResolved(dataKey, data, "write", s, m, r, String(k));
							this._.writeResolved(errorKey, null, "write", s, m, r, String(k));
						}
						return data;
					} catch (error) {
						if (!controller.signal.aborted) {
							this._.writeResolved(errorKey, error, "write", s, m, r, String(k));
							atom!.lifecycle?.onError?.(error as Error);
						}
						throw error;
					} finally {
						if (!controller.signal.aborted) {
							this._.writeResolved(loadingKey, false, "write", s, m, r, String(k));
							state.controller = null;
						}
					}
				};
	
				atom.async = { fetcher: run, controller: null, retryCount: 0, staleValue: options?.fallback };
				void run();
	
				return { dataKey, loadingKey, errorKey };
			};
			fn.invalidate = <T,>(key: A, s = this.s, m = this.m, r = this.r): V => {
				const reg = this.hub.wr._signals;
				const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
				const state = atom?.async;
				if (!atom || !state) return;
	
				const dataKey = `${String(key)}:data`;
				const loadingKey = `${String(key)}:loading`;
				const errorKey = `${String(key)}:error`;
	
				this._.writeResolved(loadingKey, true, "write", s, m, r, String(key));
				this._.writeResolved(errorKey, null, "write", s, m, r, String(key));
	
				void Promise.resolve(state.fetcher())
					.then((data) => this._.writeResolved(dataKey, data, "write", s, m, r, String(key)))
					.catch((error) => {
						this._.writeResolved(errorKey, error, "write", s, m, r, String(key));
						atom.lifecycle?.onError?.(error as Error);
					});
			};
			fn.mutate = <T,>(key: A, optimistic: T, s = this.s, m = this.m, r = this.r): V => {
				const reg = this.hub.wr._signals;
				const atom = reg.get(key) as SIGNAL_CELL<T> | UD;
				const state = atom?.async;
				if (!atom || !state) return;
	
				const dataKey = `${String(key)}:data`;
				const dataAtom = reg.get(dataKey) as SIGNAL_CELL<T> | UD;
				if (!dataAtom) throw new Error(`[Sauron] Missing async data signal "${dataKey}"`);
	
				const prior = dataAtom.peek();
				this._.writeResolved(dataKey, optimistic, "write", s, m, r, String(key));
	
				void Promise.resolve(state.fetcher()).catch(() => {
					this._.writeResolved(dataKey, prior, "write", s, m, r, String(key));
				});
			};
			fn.read = <T,>(key: A, s = this.s, m = this.m, r = this.r): T => {
				const reg = this.hub.wr._signals;
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
			return fn;
		})();
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
		public _time_travel_reverting = false;
		public time_travel = (() => {
			const fn = <T = unknown,>(key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels()): readonly HistoryEntry[] => {
				return this.time_travel.resolveHistory<T>(key, channels) ?? [];
			};
			fn.defaultChannels = (): Required<HistoryChannels> => ({
				r: this.r,
				m: this.m,
				s: "_TC_TIME_TRAVEL_",
			});
			fn.resolveChannels = (channels: HistoryChannels = this.time_travel.defaultChannels()): Required<HistoryChannels> => ({
				...this.time_travel.defaultChannels(),
				...channels,
			});
			fn.resolveHistory = <T = unknown,>(key: SignalKey, channels: HistoryChannels = this.time_travel.defaultChannels()) => {
				const _history = this.hub.wr._historyMaul.get(key);
				return _history as HistoryEntry[] | undefined;
			};
			fn.replay = <T = unknown,>(key: SignalKey, value: T, channels: HistoryChannels, source: string): T => {
				const { s, m, r } = this.time_travel.resolveChannels(channels);
				this._time_travel_reverting = true;
				try {
					this.hub.state.store.set(key, value, { s: s, m: m, r: r, triggerRender: true });
					return value;
				} finally {
					this._time_travel_reverting = false;
				}
			};
			fn.graft = (drs: A) => {
				drs.time_travel = {
					back: this.time_travel.back,
					forward: this.time_travel.forward,
					goto: this.time_travel.goto,
					history: this.time_travel,
				};
	
				return drs.time_travel;
			};
			fn.back = <T = unknown,>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels()): T | undefined => {
				const history = this.time_travel.resolveHistory<T>(key, channels);
				if (!history?.length || steps <= 0) return undefined;
	
				const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
				const targetIndex = Math.max(0, index - steps);
	
				if (targetIndex === index) return undefined;
	
				this.hub.wr._timeTravelMaul.set(key, targetIndex);
				for (let i = 0; i < history.length; i++) {
					history[i].undone = i > targetIndex;
				}
	
				return this.time_travel.replay(key, history[targetIndex].prev as T, channels, "time-travel.back");
			};
			fn.forward = <T = unknown,>(key: SignalKey, steps = 1, channels: HistoryChannels = this.time_travel.defaultChannels()): T | undefined => {
				const history = this.time_travel.resolveHistory<T>(key, channels);
				if (!history?.length || steps <= 0) return undefined;
	
				const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
				const targetIndex = Math.min(history.length - 1, index + steps);
	
				if (targetIndex === index) return undefined;
	
				this.hub.wr._timeTravelMaul.set(key, targetIndex);
				for (let i = 0; i < history.length; i++) {
					history[i].undone = i > targetIndex;
				}
	
				return this.time_travel.replay(key, history[targetIndex].next as T, channels, "time-travel.forward");
			};
			fn.goto = <T = unknown,>(key: SignalKey, sequence: number, channels: HistoryChannels = this.time_travel.defaultChannels()): T | undefined => {
				const history = this.time_travel.resolveHistory<T>(key, channels);
				if (!history?.length) return undefined;
	
				const targetIndex = history.findIndex((entry) => entry.k === sequence);
				const index = this.hub.wr._timeTravelMaul.get(key) ?? history.length - 1;
	
				if (targetIndex < 0 || targetIndex === index) return undefined;
	
				this.hub.wr._timeTravelMaul.set(key, targetIndex);
				for (let i = 0; i < history.length; i++) {
					history[i].undone = i > targetIndex;
				}
	
				return this.time_travel.replay(key, history[targetIndex].goto as T, channels, "time-travel.goto");
			};
			return fn;
		})();
		public recording: { entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>; unsubs: Array<() => V> } | null = null;
		public timeline = (() => {
			const fn = (key: A, s = this.s, m = this.m, r = this.r) => {};
			fn.manualReplay = (entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }>, options?: { from?: number; speed?: number; triggerRender?: boolean }): Promise<V> => {
				const { from = 0, speed = 0, triggerRender = true } = options ?? {};
				return new Promise((resolve) => {
					let index = from;
					const step = () => {
						if (index >= entries.length) {
							resolve();
							return;
						}
						const entry = entries[index++];
						this._.writeResolved(entry.key, entry.value, entry.source ?? "replay", this.s, this.m, this.r, `replay:${index}`);
						if (speed > 0) {
							setTimeout(step, speed);
						} else {
							step();
						}
					};
					this.DARTH_MAUL_ENGINE.startBatch();
					try {
						step();
					} finally {
						this.DARTH_MAUL_ENGINE.endBatch();
					}
					if (speed === 0) {
						resolve();
					}
				});
			};
			fn.manualBack = (key: A, s = this.s, m = this.m, r = this.r): void => {
				// REDO WITH NEW ATOM SOLUTION
			};
			fn.manualForward = (key: A, s = this.s, m = this.m, r = this.r): void => {
				// REDO WITH NEW ATOM SOLUTION
			};
			fn.manualRecord = (keys?: SignalKey[], s: S = this.s, m: S = this.m, r: S = this.r): (() => V) => {
				/** USAGE
					const stop = sauron.record();
				
					// ...user reproduces the bug...
					
					const entries = stop();
					
					// Replay it exactly:
					await sauron.replay(entries);
					
					// Or find where reality diverged:
					sauron.bisect(entries);
					 */
				if (this.recording) {
					throw new Error("[Sauron] A recording is already active");
				}
	
				const registry = this.hub.wr._signals;
				const targets = keys ?? [...registry.keys()];
	
				const entries: Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> = [];
	
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
	
				return () => this.timeline.stopManualRecording();
			};
			fn.stopManualRecording = (): Array<{ key: SignalKey; value: A; previous: A; timestamp: number; source?: S }> => {
				if (!this.recording) return [];
	
				for (const unsub of this.recording.unsubs) {
					unsub();
				}
	
				const entries = this.recording.entries;
				this.recording = null;
	
				return entries;
			};
			return fn;
		})();
		public snapshot = (() => {
			const fn = () => {
				// REDO WITH NEW ATOM SOLUTION
			};
			fn.restore = (snapshot: Record<string, A>): void => {
				// REDO WITH NEW ATOM SOLUTION
			};
			fn.diff = (a: Record<string, A>, b: Record<string, A>) => {
				// REDO WITH NEW ATOM SOLUTION
			};
			fn.applyPatch = (patch: Record<string, { from: A; to: A }>): void => {
				// REDO WITH NEW ATOM SOLUTION
			};
			return fn;
		})();
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
		public readonly SRC_DEFAULTS: rts = { sl: this.s, ms: this.m, rt: this.r };
		public readonly DST_DEFAULTS: rts = { sl: this.s, ms: this.m, rt: this.r };
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
	})(this);
}
// #endregion
//                                                                                                         [DARTH MAUL] SIGNALS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function createReactiveSystem({ update, notify, unwatched,}: { update(sub: ReactiveNode): B;	notify(sub: ReactiveNode): V; unwatched(sub: ReactiveNode): V;}) {
	return { link, unlink, propagate, checkDirty, shallowPropagate,	};
	function link(dep: ReactiveNode, sub: ReactiveNode, version: number): void {
		const prevDep = sub.depsTail;
		if (prevDep !== undefined && prevDep.dep === dep) { return; }
		const nextDep = prevDep !== undefined ? prevDep.nextDep : sub.deps;
		if (nextDep !== undefined && nextDep.dep === dep) {
			nextDep.version = version;
			sub.depsTail = nextDep;
			return;
		}
		const prevSub = dep.subsTail;
		if (prevSub !== undefined && prevSub.version === version && prevSub.sub === sub) { return; }
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
		if (nextDep !== undefined) { nextDep.prevDep = newLink; }
		if (prevDep !== undefined) { prevDep.nextDep = newLink; }
        else { sub.deps = newLink; }
		if (prevSub !== undefined) { prevSub.nextSub = newLink; }
        else { dep.subs = newLink; }
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

			if (flags & ReactiveFlags.Watching) { notify(sub); }

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
					if (subs.nextSub !== undefined) { shallowPropagate(subs); }
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
				if (nextDep !== undefined) { link = nextDep; continue; }
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
interface EffectScopeNode extends ReactiveNode {}
interface EffectNode extends ReactiveNode {	fn(): (() => V) | V; cleanup?: () => V;}
interface ComputedNode<T = any> extends ReactiveNode {	value: T | UD;	getter: (previousValue?: T) => T;} 
interface SignalNode<T = any> extends ReactiveNode { currentValue: T; pendingValue: T;}
const {	link, unlink, propagate, checkDirty, shallowPropagate,} = createReactiveSystem({	
    update(node: SignalNode | ComputedNode | EffectScopeNode): boolean {
		if ('getter' in node) { return updateComputed(node); }
		if ('currentValue' in node) { return updateSignal(node); }
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
			if (effect === undefined || !(effect.flags & ReactiveFlags.Watching)) { break; }
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
		else if ('currentValue' in node) { }// Nothing to do for signals, they are always mutable and never dirty until pendingValue changes
		else if ('fn' in node) { effectOper.call(node); }
		else { effectScopeOper.call(node); }
	},
});
function updateComputed(c: ComputedNode): B {
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
		return oldValue !== (c.value = c.getter(oldValue));
	} finally {
		activeSub = prevSub;
		c.flags &= ~ReactiveFlags.RecursedCheck;
		purgeDeps(c);
	}
}
function updateSignal(s: SignalNode): B {	
    s.flags = ReactiveFlags.Mutable;
    const previous = s.currentValue;
    const next = s.pendingValue;
    s.currentValue = next;
    return !Object.is(previous, next);
}
function run(e: EffectNode): V {
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
			const result = e.fn();
			e.cleanup = typeof result === "function" ? result : undefined;
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
function flush(): V {
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
	if ( flags & ReactiveFlags.Dirty || ( flags & ReactiveFlags.Pending && ( checkDirty(this.deps!, this) || (this.flags = flags & ~ReactiveFlags.Pending, false) ) ) ) {
		if (updateComputed(this)) {
			const subs = this.subs;
			if (subs !== undefined) { shallowPropagate(subs); }
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
function signalOper<T>(this: SignalNode<T>, ...value: [T]): T | V {
	if (value.length) {
		if (this.pendingValue !== (this.pendingValue = value[0])) {
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
function runCleanup(e: EffectNode): V {
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
function effectOper(this: EffectNode): V {
	effectScopeOper.call(this);
	if (this.cleanup) {
		runCleanup(this);
	}
}
function effectScopeOper(this: EffectScopeNode): V {
	this.flags = ReactiveFlags.None;
	disposeAllDepsInReverse(this);
	const sub = this.subs;
	if (sub !== undefined) {
		unlink(sub);
	}
}
function disposeAllDepsInReverse(sub: ReactiveNode): V {
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
export function getActiveSub(): ReactiveNode | UD {	return activeSub;}
export function setActiveSub(sub?: ReactiveNode) {
	const prevSub = activeSub;
	activeSub = sub;
	return prevSub;
}
export function getBatchDepth(): N { return batchDepth;}
export function startBatch() { ++batchDepth;}
export function endBatch() { if (!--batchDepth) { flush(); }}
export function isSignal(fn: () => V): B {	return fn.name === 'bound ' + signalOper.name;}
export function isComputed(fn: () => V): B { return fn.name === 'bound ' + computedOper.name;}
export function isEffect(fn: () => V): B {	return fn.name === 'bound ' + effectOper.name;}
export function isEffectScope(fn: () => V): B {	return fn.name === 'bound ' + effectScopeOper.name;}
export function signal<T>(): { (): T | UD;	(value: T | UD): V;};
export function signal<T>(initialValue: T): { (): T; (value: T): V;};
export function signal<T>(initialValue?: T): { (): T | UD;	(value: T | UD): V;} {
	return signalOper.bind({
		currentValue: initialValue,
		pendingValue: initialValue,
		subs: undefined,
		subsTail: undefined,
		flags: ReactiveFlags.Mutable,
	}) as () => T | undefined;
}
export function computed<T>(getter: (previousValue?: T) => T): () => T {
	return computedOper.bind({
		value: undefined,
		subs: undefined,
		subsTail: undefined,
		deps: undefined,
		depsTail: undefined,
		flags: ReactiveFlags.None,
		getter: getter as (previousValue?: unknown) => unknown,
	}) as () => T;
}
export function effect(fn: () => V | (() => V)): () => V {
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
		const result = e.fn();
		e.cleanup = typeof result === "function" ? result : undefined;
	} finally {
		--runDepth;
		activeSub = prevSub;
		e.flags &= ~ReactiveFlags.RecursedCheck;
	}
	return effectOper.bind(e);
}
export function effectScope(fn: () => V): () => V {
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
	return effectScopeOper.bind(e);
}
export function trigger(fn: () => V) {
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
// #endregion
//                                                               [SITH] [PALPATINE] [DARTH VADER] [GALEN MAREK] [RULE OF 2] BUS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type BUS_HANDLER<P = U, T = U> = (p: P, msg: SIG_BUS_Message1) => T | Promise<T>;
interface SIG_BUS_Message1 {
	id?: string;
	r?: S;
	m?: S;
	s: S;
	k: S;
	p?: any;
	ts?: number;
	_ack?: boolean;
	_reply?: boolean;
	_rid?: string;
	ackTo?: string;
}
interface BUS_CHANNEL {
	handler?: BUS_HANDLER<any, any>;
	push?: Set<(msg: SIG_BUS_Message1) => void>;
}
const THE_MESH_CACHE: R<S, R<S, R<S, R<S, V>>>> = O.create(null);
type c_type = "mesh" | 'globalThis'
class THE_MESH_STORE {
	// to create other r level tier, must use global this option for creating a new context
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public cache: c_type
	THE_MESH_CACHE: R<S, R<S, R<S, R<S, V>>>> = O.create(null);
	_the_mesh_cache: R<S, A>
	_handlers: R<S, A> = O.create(null);
	#subRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
 
	#metaSubRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
	_meta: R<S, A> = O.create(null);

	#signalsSubRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
	_signals: R<S, A> = O.create(null);

	#nakedSignalsSubRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
	_naked_signals: R<S, A> = O.create(null);

	#contextSubRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
	_context: R<S, A> = O.create(null);

	#atomsSubRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
	_atoms: R<S, A> = O.create(null);

	#sauronSubRoot: R<S, R<S, R<S, R<S, ST<QuadSub<A>>>>>> = O.create(null);
	_sauron: R<S, A> = O.create(null);

	constructor(hub: SITH, cache: c_type = "mesh") {
		this.r = hub.r;
		this.m = hub.m;
		this.s = hub.s;
		this.k = hub.k;
		this.cache = cache
		this._handlers = this.getS('_handlers', this.m, this.r );
		const rL = ((globalThis as A)[this.r] ??= O.create(null));
		const mL = (rL[this.m] ??= O.create(null));
		(globalThis as A)[this.s] = mL['_globalThis'] ??= O.create(null); // this._globalThis
		this._meta = this.getS('_meta', this.m, this.r);
		this._signals = this.getS('_signals', this.m, this.r);
		this._naked_signals = this.getS('_naked_signals', this.m, this.r);
		this._context = this.getS('_context', this.m, this.r);
		this._atoms = this.getS('_atoms', this.m, this.r);
		this._sauron = this.getS('_sauron', this.m, this.r);
		this._the_mesh_cache = this.getS('_the_mesh_cache', this.m, this.r);
	}
	public getS(s: S = this.s, m: S = this.m, r: S = this.r, c: c_type = "mesh") {
		const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_MESH_CACHE;
		const rL = (cache[r] ??= O.create(null));
		const mL = (rL[m] ??= O.create(null));
		const sL = (mL[s] ??= O.create(null));
		return sL;
	}
	public getM(m: S = this.m, r: S = this.r, c = "mesh") {
		const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_MESH_CACHE;
		const rL = (cache[r] ??= O.create(null));
		const mL = (rL[m] ??= O.create(null));
		return mL;
	}
	public getR(r: S = this.r, c = "mesh") {
		const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_MESH_CACHE;
		const rL = (cache[r] ??= O.create(null));
		return rL;
	}
	public handlers = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn =<Z = A> (k: S, s: S = '_handlers', m: S = this.m, r: S = this.r, c: c_type = "mesh"): A | UD => { 
				if (this.cache === 'globalThis' || c === 'globalThis') {
					return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]as Z;
				} else {
					return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]as Z;
				}
			 };
			fn.hotGT= <Z = A>(k: S): A | UD => { return this._handlers[k] as Z; };
			fn.hotMesh= <Z = A>(k: S): A | UD => { return (globalThis as A)[this.s]?.[k] as Z; };
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_handlers', m: S = this.m, r: S = this.r, c: c_type = "mesh"): this => {
				const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_MESH_CACHE;
				const rL = (cache[r] ??= O.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.handlers.emit(k, s, m, r, next);
				return this;
			}
			fn.hotGT= <Z = A>(v: UF<A>, k: S, s: S = '_handlers', m: S = this.m, r: S = this.r) => { 
				const sL = (globalThis as A)[s];
				const next = rv(v);
				sL[k] = next;
				this.handlers.emit(k, s, m, r, next);
				return next as Z;
			 };
			fn.hotMesh = <Z = A,>(v: UF<A>, k: S, s: S = '_handlers', m: S = this.m, r: S = this.r) => {
				const sL = this._handlers;
				const next = rv(v);
				sL[k] = next;
				this.handlers.emit(k, s, m, r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_handlers', m: S = this.m, r: S = this.r): B => { return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s]; };
			fn.hotGT= (k: S, s: S):B => { return k in (globalThis as A)[s] };
			fn.hot= (k: S): B => { return k in this._handlers; };
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_handlers', m: S = this.m, r: S = this.r, c: c_type = "mesh"): B => {
				const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this.THE_MESH_CACHE;
				const rL = (cache[r] ??= O.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.handlers.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hotMesh= (k: S, s: S = '_handlers', m: S = this.m, r: S = this.r, c: c_type = "mesh"): B => {
				if (!(k in this._handlers)) return false;
				delete this._handlers[k];
				this.handlers.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hotGT= <Z = A>(v: UF<A>, k: S, s: S = '_handlers', m: S = this.m, r: S = this.r) => { 
				const sL = (globalThis as A)[s];
				const next = rv(v);
				sL[k] = next;
				this.handlers.emit(k, s, m, r, next);
				return next as Z;
			 };
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_handlers', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#subRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<V>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_handlers', m: S = this.m, r: S = this.r): V => {
			this.#subRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = ( v: A | UD, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r,): void => {
			const set = this.#subRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public meta = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn = (k: S, s: S = '_meta', m: S = this.m, r: S = this.r): A | UD => { return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]; };
			fn.hot = <Z = A,>(k: S): A | UD => this._meta[k] as Z;
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_meta', m: S = this.m, r: S = this.r): this => {
				const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.meta.emit(k, s, m, r, next);
				return this;
			};
			fn.hot = <Z = A,>(v: UF<A>, k: S, s: S = '_meta', m: S = this.m, r: S = this.r) => {
				const sL = this._meta;
				const next = rv(v);
				sL[k] = next;
				this.meta.emit(k, s, m, r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_meta', m: S = this.m, r: S = this.r): B => { return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s]; };
			fn.hot = (k: S): B => k in this._meta;
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_meta', m: S = this.m, r: S = this.r): B => {
				const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.meta.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hot = (k: S, s: S = '_meta', m: S = this.m, r: S = this.r): B => {
				if (!(k in this._meta)) return false;
				delete this._meta[k];
				this.meta.emit(k, s, m, r, undefined);
				return true;
			};
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_meta', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#metaSubRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<A>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_meta', m: S = this.m, r: S = this.r): V => {
			this.#metaSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = (v: A | UD, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r, ): void => {
			const set = this.#metaSubRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public signals = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn = (k: S, s: S = '_signals', m: S = this.m, r: S = this.r): A | UD => {
				return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
			};
			fn.hot = <Z = A,>(k: S): A | UD => this._signals[k] as Z;
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_signals', m: S = this.m, r: S = this.r): this => {
				const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.signals.emit(k, s, m, r, next);
				return this;
			};
			fn.hot = <Z = A,>(v: UF<A>, k: S, s: S = '_signals', m: S = this.m, r: S = this.r) => {
				const sL = this._signals;
				const next = rv(v);
				sL[k] = next;
				this.signals.emit(k, s, m, r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_signals', m: S = this.m, r: S = this.r): B => {
				return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
			};
			fn.hot = (k: S): B => k in this._signals;
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_signals', m: S = this.m, r: S = this.r): B => {
				const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.signals.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hot = (k: S, s: S = '_signals', m: S = this.m, r: S = this.r): B => {
				if (!(k in this._signals)) return false;
				delete this._signals[k];
				this.signals.emit(k, s, m, r, undefined);
				return true;
			};
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_signals', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#signalsSubRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<A>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_signals', m: S = this.m, r: S = this.r): V => {
			this.#signalsSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = (v: A | UD, k: S, s: S = '_signals', m: S = this.m, r: S = this.r, ): void => {
			const set = this.#signalsSubRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public naked_signals = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn = (k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): A | UD => {
				return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
			};
			fn.hot = <Z = A,>(k: S): A | UD => this._naked_signals[k] as Z;
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): this => {
				const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.naked_signals.emit(k, s, m, r, next);
				return this;
			};
			fn.hot = <Z = A,>(v: UF<A>, k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r) => {
				const sL = this._naked_signals;
				const next = rv(v);
				sL[k] = next;
				this.naked_signals.emit(k, s, m, r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): B => {
				return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
			};
			fn.hot = (k: S): B => k in this._naked_signals;
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): B => {
				const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.naked_signals.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hot = (k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): B => {
				if (!(k in this._naked_signals)) return false;
				delete this._naked_signals[k];
				this.naked_signals.emit(k, s, m, r, undefined);
				return true;
			};
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#nakedSignalsSubRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<A>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_naked_signals', m: S = this.m, r: S = this.r): V => {
			this.#nakedSignalsSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = (v: A | UD, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r,  ): void => {
			const set = this.#nakedSignalsSubRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public context = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn = (k: S, s: S = '_context', m: S = this.m, r: S = this.r): A | UD => { return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]; };
			fn.hot = <Z = A,>(k: S): A | UD => this._context[k] as Z;
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_context', m: S = this.m, r: S = this.r): this => {
				const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.context.emit(k, s, m, r, next);
				return this;
			};
			fn.hot = <Z = A,>(v: UF<A>, k: S, s: S = '_context', m: S = this.m, r: S = this.r) => {
				const sL = this._context;
				const next = rv(v);
				sL[k] = next;
				this.context.emit(k, s, m, r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_context', m: S = this.m, r: S = this.r): B => {
				return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
			};
			fn.hot = (k: S): B => k in this._context;
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_context', m: S = this.m, r: S = this.r): B => {
				const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.context.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hot = (k: S, s: S = '_context', m: S = this.m, r: S = this.r): B => {
				if (!(k in this._context)) return false;
				delete this._context[k];
				this.context.emit(k, s, m, r, undefined);
				return true;
			};
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_context', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#contextSubRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<A>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_context', m: S = this.m, r: S = this.r): V => {
			this.#contextSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = (v: A | UD, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r, ): void => {
			const set = this.#contextSubRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public atoms = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn = (k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): A | UD => { return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k]; };
			fn.hot = <Z = A,>(k: S): A | UD => this._atoms[k] as Z;
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): this => {
				const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.atoms.emit(k, s, m, r, next);
				return this;
			};
			fn.hot = <Z = A,>(v: UF<A>, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r) => {
				const sL = this._atoms;
				const next = rv(v);
				sL[k] = next;
				this.atoms.emit(k,s, m,r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): B => {
				return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
			};
			fn.hot = (k: S): B => k in this._atoms;
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): B => {
				const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.atoms.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hot = (k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): B => {
				if (!(k in this._atoms)) return false;
				delete this._atoms[k];
				this.atoms.emit(k, s, m, tr, undefined);
				return true;
			};
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#atomsSubRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<A>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r): V => {
			this.#atomsSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = (v: A | UD, k: S, s: S = '_atoms', m: S = this.m, r: S = this.r,  ): void => {
			const set = this.#atomsSubRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public sauron = (() => {
		const fn = () => {};
		fn.get = (() => {
			const fn = (k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): A | UD => {
				return this.THE_MESH_CACHE[r]?.[m]?.[s]?.[k];
			};
			fn.hot = <Z = A,>(k: S): A | UD => this._sauron[k] as Z;
			return fn;
		})();
		fn.set = (() => {
			const fn = (v: UF<A>, k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): this => {
				const rL = (this.THE_MESH_CACHE[r] ??= Object.create(null));
				const mL = (rL[m] ??= Object.create(null));
				const sL = (mL[s] ??= Object.create(null));
				const next = rv(v);
				sL[k] = next;
				this.sauron.emit(k, s, m, r, next);
				return this;
			};
			fn.hot = <Z = A,>(v: UF<A>, k: S, s: S = '_sauron', m: S = this.m, r: S = this.r) => {
				const sL = this._sauron;
				const next = rv(v);
				sL[k] = next;
				this.sauron.emit(k, s, m, r, next);
				return next as Z;
			};
			return fn;
		})();
		fn.has = (() => {
			const fn = (k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): B => {
				return !!this.THE_MESH_CACHE[r]?.[m]?.[s] && k in this.THE_MESH_CACHE[r][m][s];
			};
			fn.hot = (k: S): B => k in this._sauron;
			return fn;
		})();
		fn.delete = (() => {
			const fn = (k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): B => {
				const sL = this.THE_MESH_CACHE[r]?.[m]?.[s];
				if (!sL || !(k in sL)) return false;
				delete sL[k];
				this.sauron.emit(k, s, m, r, undefined);
				return true;
			};
			fn.hot = (k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): B => {
				if (!(k in this._sauron)) return false;
				delete this._sauron[k];
				this.sauron.emit(k, s, m,r, undefined);
				return true;
			};
			return fn;
		})();
		fn.on = (fn: QuadSub<A>, k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): (() => V) => {
			const rL = (this.#sauronSubRoot[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			const sL = (mL[s] ??= Object.create(null));
			const set: Set<QuadSub<A>> = (sL[k] ??= new Set());
			set.add(fn);
			return () => {
				set.delete(fn);
				if (!set.size) delete sL[k];
			};
		};
		fn.off = (fn: QuadSub<A>, k: S, s: S = '_sauron', m: S = this.m, r: S = this.r): V => {
			this.#sauronSubRoot[r]?.[m]?.[s]?.[k]?.delete(fn);
		};
		fn.emit = (v: A | UD, k: S, s: S = '_sauron', m: S = this.m, r: S = this.r ): void => {
			const set = this.#sauronSubRoot[r]?.[m]?.[s]?.[k];
			if (!set) return;
			for (const fn of set) fn(v);
		};
		return fn;
	})();
	public async invokeHandlers(msg: SIG_BUS_Message): P<unknown> {
		const set = this.root.se.getSubS(msg.s, msg.m, msg.r)[msg.k ?? "[NO_ADP_FOUND]"];
		if (!set || !set.size) return { success: false };
	
		const results = await Promise.all(
			[...set].map((h: A) => Promise.resolve(h(msg.p)).catch(() => ({ success: false })))
		);
	
		if (results.length === 1) return results[0];
	
		const merged: Record<string, unknown> = {};
		const collisions: { key: string; r: string }[] = [];
	
		for (const result of results) {
			if (result === undefined || result === null) continue;
			if (typeof result !== "object") {
				merged["value"] = result;
				continue;
			}
			for (const [key, value] of Object.entries(result as Record<string, unknown>)) {
				if (key in merged) collisions.push({ key, r: msg.r });
				merged[key] = value;
			}
		}
	
		if (collisions.length > 0) (merged as A)._cse_collisions = collisions;
	
		return Object.keys(merged).length === 0 ? { success: true } : merged;
	}
}
export class SITH {
	public root: DEATH_STAR;
	public mesh_store: THE_MESH_STORE 
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public capacity: N;
	public pidCapacity
	public schedule
	public shared
	public maxBatch
	public wait
	public f64Capacity = 1 << 18
	public u32Capacity = 1 << 18
	public i32Capacity = 1 << 18
	public byteCapacity = 1 << 20
	constructor(DEATH_STAR: DEATH_STAR, capacity = 65_536 * 2, schedule= DEFAULT_SCHEDULE, shared= typeof SharedArrayBuffer !== "undefined", maxBatch = 8192, wait = "yield") {
		this.root = DEATH_STAR;
		this.shared = shared;
		this.wait = wait;
		this.maxBatch = maxBatch;
		this.capacity = capacity;
		this.schedule = schedule
		this.r = DEATH_STAR.r;
		this.m = DEATH_STAR.m;
		this.s = DEATH_STAR.s;
		this.k = DEATH_STAR.k;
		this.mesh_store = new THE_MESH_STORE(this)

	}
    public palpatine = new (class PALPATINE {// BATCHING NGIN: 1.5B OPS/SEC
		public hub: SITH;
		public root: DEATH_STAR;
		/*
		Per-lane shared control block layout:
	
		0 nextClaim
		1 gatingSequence  — consumer progress
		2 cachedAvailableSequence
		3 publishedHighest
		4 fullStalls
		5 claimed
		6 published
		7 consumed
		8 batches
		9 maxBatch
	   */
		//                                                                                                                  [CONSTANTS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private capacity: N;
		private mask: N;
		private pidCapacity: N;
		private schedule: readonly N[];
		private shared: B;
		private rings: I32[] = [];
		private CTRL = 16;
		private ctrl: I32;
		private available: I32[] = [];
		private clock: I32;
		private phaseIndex: I32;
		private running: I32;
		private pidToSlot: I32;
		private dirty: U32;
		private f64: F64;
		private u32: U32;
		private i32: I32;
		private bytes: U8;
		private f64Tail = 0;
		private u32Tail = 0;
		private i32Tail = 0;
		private byteTail = 0;
		private maxBatch: N;
		private wait: WaitStrategy;
		private handlers: AR< | ((ring: I32, from: N, to: N, epoch: N, d: this ) => V) | NU > = [];
		constructor(SITH: SITH) {
			this.hub = SITH;
			this.root = SITH.root;
			const capacity = SITH.capacity ?? 1 << 18;
		  	const pidCapacity = SITH.pidCapacity ?? 1 << 20;
			if (!pow2(capacity)) throw new Error("capacity must be a power of two");
			if (!pow2(pidCapacity)) throw new Error("pidCapacity must be a power of two");
			this.capacity = capacity;
			this.mask = capacity - 1;
			this.pidCapacity = pidCapacity;
			this.schedule = SITH.schedule ?? DEFAULT_SCHEDULE;
			this.shared = SITH.shared ?? typeof SharedArrayBuffer !== "undefined";
			this.maxBatch = SITH.maxBatch ?? 8192;
			this.wait = SITH.wait ?? "yield";
			const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;
			const laneCount = this.schedule.length;
			this.rings = Array.from( { length: laneCount }, () => new Int32Array(new AB(capacity * REC * 4)) );
			this.ctrl = new Int32Array(new AB(laneCount * this.CTRL * 4));
			// Availability buffer uses one Int32 per ring slot.
			this.available = Array.from( { length: laneCount }, () => new Int32Array(new AB(capacity * 4)) );
			this.clock = new Int32Array(new AB(4));
			this.phaseIndex = new Int32Array(new AB(4));
			this.running = new Int32Array(new AB(4));
			this.pidToSlot = new Int32Array(new AB(pidCapacity * 4));
			this.pidToSlot.fill(-1);
			this.dirty = new Uint32Array(new AB((pidCapacity >>> 5) * 4));
			this.f64 = new Float64Array(new AB((SITH.f64Capacity ?? 1 << 18) * 8));
			this.u32 = new Uint32Array(new AB((SITH.u32Capacity ?? 1 << 18) * 4));
			this.i32 = new Int32Array(new AB((SITH.i32Capacity ?? 1 << 18) * 4));
			this.bytes = new Uint8Array(new AB(SITH.byteCapacity ?? 1 << 20));
		}
		// #endregion
		//                                                                                                                   [HANDLERS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		sub( op: OpKind, handler: ( ring: I32, from: N, to: N, epoch: N, d: this ) => V  ): this {
		  this.handlers[op] = handler;
		  return this;
		}
	  /*======================================================================
		 MULTI-PRODUCER CLAIM / PUBLISH
		 Claim the next sequence for a lane. This is a true multi-producer sequencer: - CAS reserves a unique sequence. - The availability buffer marks that exact sequence published. - Consumers never rely on a single global “highest published” value.
		====================================================================== */
		private ctrlAt(lane: N, field: number): number { return lane * this.CTRL + field;  }
		claim(lane: number): number {
		  const next = this.ctrlAt(lane, 0);
		  const stall = this.ctrlAt(lane, 4);
		  const claimed = this.ctrlAt(lane, 5);
	  
		  let spin = 0;
	  
		  for (;;) {
			const seq = Atomics.load(this.ctrl, next);
			const wrap = seq - this.capacity;
			const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
	  
			if (gate > wrap) {
			  if (Atomics.compareExchange(this.ctrl, next, seq, seq + 1) === seq) {
				Atomics.add(this.ctrl, claimed, 1);
				return seq;
			  }
			  continue;
			}
	  
			Atomics.add(this.ctrl, stall, 1);
	  
			if (this.wait === "spin") {
			  spin++;
			  continue;
			}
	  
			if (this.wait === "yield") {
			  this.yieldNow();
			  continue;
			}
	  
			Atomics.wait(this.ctrl, next, seq, 1);
			spin = 0;
		  }
		}
		// Publish a claimed record. The record must be fully written BEFORE this call. The availability buffer is the publication barrier.
		publish(lane: number, seq: number): void {
		  const avail = this.available[lane];
		  Atomics.store(avail, seq & this.mask, seq + 1);
	  
		  const pub = this.ctrlAt(lane, 6);
		  Atomics.add(this.ctrl, pub, 1);
	  
		  const waiting = this.ctrlAt(lane, 0);
		  Atomics.notify(this.ctrl, waiting);
		}
		// One-shot enqueue. Returns: >= 0 — published sequence  -1  — nonblocking mode and lane was full
		emit( op: OpKind, pid: N, slot: N, a = 0, b = 0, c = 0, ext = 0, block = true  ): N {
		  const lane = this.laneFor(op);
		  const ring = this.rings[lane];
	  
		  if (!block && this.isFull(lane)) return -1;
	  
		  const seq = this.claim(lane);
		  const base = (seq & this.mask) * REC;
		  const epoch = Atomics.load(this.clock, 0);
	  
		  ring[base + F_OP] = op;
		  ring[base + F_PID] = pid;
		  ring[base + F_SLOT] = slot;
		  ring[base + F_A] = a;
		  ring[base + F_B] = b;
		  ring[base + F_C] = c;
		  ring[base + F_EPOCH] = epoch;
		  ring[base + F_EXT] = ext;
	  
		  this.publish(lane, seq);
		  return seq;
		}
		// #endregion
		//                                                                                                         [PID / SLOT / DIRTY] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		bind(pid: N, slot: N): V { this.pidToSlot[pid] = slot;  }
		unbind(pid: N): V { this.pidToSlot[pid] = -1;  }
		slot(pid: N): N { return this.pidToSlot[pid];  }
		dirtyMark(pid: N): V { this.dirty[pid >>> 5] |= 1 << (pid & 31);  }
		dirtyPeek(pid: N): B { return (this.dirty[pid >>> 5] & (1 << (pid & 31))) !== 0;  }
		dirtyTake(pid: N): B {
		  const w = pid >>> 5;
		  const bit = 1 << (pid & 31);
		  const word = this.dirty[w];
	  
		  if ((word & bit) === 0) return false;
	  
		  this.dirty[w] = word & ~bit;
		  return true;
		}
		// #endregion
		//                                                                                                                 [SOA ARENAS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		allocF64(value: N): N {
		  const at = this.f64Tail++;
		  this.f64[at] = value;
		  return at;
		}
		allocU32(value: N): N {
		  const at = this.u32Tail++;
		  this.u32[at] = value;
		  return at;
		}
		allocI32(value: N): N {
		  const at = this.i32Tail++;
		  this.i32[at] = value;
		  return at;
		}
		allocBytes(data: U8): N {
		  const at = this.byteTail;
		  this.bytes.set(data, at);
		  this.byteTail += data.length;
		  return at;
		}
		f64At(offset: N): N {    return this.f64[offset];  }
		u32At(offset: N): N {    return this.u32[offset];  }
		i32At(offset: N): N {    return this.i32[offset];  }
		bytesAt(offset: N, length: N): U8 {    return this.bytes.subarray(offset, offset + length);  }
		
		// #endregion
		//                                                                                                           [CONSUMER BARRIER] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	  /*======================================================================
		 
		 Return the highest contiguous available sequence for a lane. This scans only the availability window, then caches the result. It avoids the classic problem of a slow producer blocking visibility of later producers.
		====================================================================== */
		private availableThrough(lane: number): number {
		  const ctrlBase = this.ctrlAt(lane, 0);
		  const gateField = this.ctrlAt(lane, 1);
		  const cacheField = this.ctrlAt(lane, 2);
	  
		  const next = Atomics.load(this.ctrl, ctrlBase);
		  let gate = Atomics.load(this.ctrl, gateField);
		  let cached = Atomics.load(this.ctrl, cacheField);
	  
		  if (cached >= gate) {
			const avail = this.available[lane];
			const cap = this.capacity;
	  
			let scan = cached;
			const limit = Math.min(next, cached + this.maxBatch);
	  
			while (scan < limit) {
			  const flag = Atomics.load(avail, scan & (cap - 1));
			  if (flag <= scan) break;
			  scan++;
			}
	  
			cached = scan;
			Atomics.store(this.ctrl, cacheField, cached);
		  }
	  
		  return cached;
		}
		// #endregion
		//                                                                                                     [PHASE-LOCKED EXECUTION] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		/*======================================================================
		   Drain exactly one scheduled phase. A phase is homogeneous: READ only, WRITE only, DELETE only, etc.
		   ====================================================================== */
		drainPhase(limit = this.maxBatch): number {
		  const schedule = this.schedule;
		  const lane = schedule[Atomics.load(this.phaseIndex, 0)];
		  const handler = this.handlers[lane];
	  
		  if (!handler) {
			this.nextPhase();
			return 0;
		  }
	  
		  const ring = this.rings[lane];
		  const gateField = this.ctrlAt(lane, 1);
	  
		  const available = this.availableThrough(lane);
		  const head = Atomics.load(this.ctrl, gateField);
	  
		  const end = Math.min(available, head + limit);
	  
		  if (end <= head) {
			this.nextPhase();
			return 0;
		  }
	  
		  const epoch = Atomics.load(this.clock, 0);
	  
		  // Batch boundary is handed to the handler.
		  // The handler can specialize contiguous execution.
		  handler(ring, head, end, epoch, this);
	  
		  Atomics.store(this.ctrl, gateField, end);
	  
		  const consumedField = this.ctrlAt(lane, 7);
		  const batchField = this.ctrlAt(lane, 8);
		  const maxBatchField = this.ctrlAt(lane, 9);
	  
		  const count = end - head;
	  
		  Atomics.add(this.ctrl, consumedField, count);
		  Atomics.add(this.ctrl, batchField, 1);
	  
		  const prevMax = Atomics.load(this.ctrl, maxBatchField);
		  if (count > prevMax) Atomics.store(this.ctrl, maxBatchField, count);
	  
		  this.nextPhase();
		  return count;
		}// Run one full revolution. The tracer particle advances only after every scheduled phase runs.
		revolution(limit = this.maxBatch): number {
		  let total = 0;
	  
		  for (let i = 0; i < this.schedule.length; i++) {
			total += this.drainPhase(limit);
		  }
	  
		  Atomics.add(this.clock, 0, 1);
		  return total;
		}// Run until all lanes are drained. Useful for shutdown, transaction boundaries, and tests.
		drainAll(limit = this.maxBatch): number {
		  let total = 0;
	  
		  for (;;) {
			const before = total;
			total += this.revolution(limit);
			if (total === before) break;
		  }
	  
		  return total;
		}
		// #endregion
		//                                                                                                            [CLOCK / CONTROL] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		tick(): number { return Atomics.load(this.clock, 0);  }
		currentPhase(): OpKind { return this.schedule[Atomics.load(this.phaseIndex, 0)] as OpKind;  }
		start(): void {    Atomics.store(this.running, 0, 1);  }
		stop(): void {
		  Atomics.store(this.running, 0, 0);
		  Atomics.notify(this.running, 0);
		}
		isRunning(): boolean {    return Atomics.load(this.running, 0) === 1;  }
		// #endregion
		//                                                                                                                      [STATS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		stats(lane: number): LANE_STATS {
		  const b = lane * this.CTRL;
	  
		  return {
			claimed: Atomics.load(this.ctrl, b + 5),
			published: Atomics.load(this.ctrl, b + 6),
			consumed: Atomics.load(this.ctrl, b + 7),
			batches: Atomics.load(this.ctrl, b + 8),
			maxBatch: Atomics.load(this.ctrl, b + 9),
			fullStalls: Atomics.load(this.ctrl, b + 4),
		  };
		}
		pending(lane: number): number {
		  const published = Atomics.load(this.ctrl, this.ctrlAt(lane, 6));
		  const consumed = Atomics.load(this.ctrl, this.ctrlAt(lane, 7));
		  return published - consumed;
		}
		// #endregion
		//                                                                                                                   [INTERNAL] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private laneFor(op: OpKind): number {    return op;  }
		private isFull(lane: number): boolean {
		  const next = Atomics.load(this.ctrl, this.ctrlAt(lane, 0));
		  const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
		  return next - gate >= this.capacity;
		}
		private nextPhase(): void {
		  const next = (Atomics.load(this.phaseIndex, 0) + 1) % this.schedule.length;
		  Atomics.store(this.phaseIndex, 0, next);
		}
		private yieldNow(): void {
		  if (typeof scheduler !== "undefined" && "yield" in scheduler) {
			scheduler.yield();
		  } else if (typeof setImmediate === "function") {
			setImmediate(() => {});
		  } else {
			setTimeout(() => {}, 0);
		  }
		}
		// #endregion
	})(this);
    public vader = new (class VADER {// DISRUPTOR STREAMING AND DUPLEXING: 2.03B OPS/SEC
		//                                                                                                                  [CONSTANTS]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public r: A = "THE_WIZARD_ROOT_REG";
		public m: A = "SAURON_MASTER_REG";
		public s: A = "SAURON_SLAVE_REG";
		public k: A = "__SAURON__";
		private capacity: number;
		private hub: SITH;
		private root: DEATH_STAR;
		// ======================================================================
		// Channel table: open-addressed hash of (r, m, s, k)
		// ======================================================================
		private channelMask: number;
		private channelKey: Int32Array; // hash bucket -> channel id or -1
		private channelState: Uint8Array; // 0 = empty, 1 = used, 2 = deleted
		private channelHash: Int32Array; // stored hash to speed probing
		private channelCount = 1; // 0 reserved
		// Per-channel subscriber list (intrusive linked list)
		private channelSubHead: Int32Array;
		private channelSubTail: Int32Array;
		// ======================================================================
		// Subscriber pool
		// ======================================================================
		private subHandler: (Handler | undefined)[];
		private subNext: Int32Array; // next subscriber in channel
		private subPrev: Int32Array; // prev subscriber in channel
		private subChannel: Int32Array; // owning channel
		private subCount = 1; // 0 reserved
		// ======================================================================
		// Message ring buffer
		// ======================================================================
		private msgChannel: Int32Array;
		private msgPayload: unknown[];
		private msgDeferred: (Deferred | undefined)[];
		private msgHead = 0; // next message to consume
		private msgTail = 0; // next free slot
		private msgCount = 0;
		// Backpressure waiters
		private capacityWaiters: Deferred[] = [];
		// Drain scheduling
		private draining = false;
		private queuedMicrotask = false;
		constructor(SITH: SITH) {
			if (SITH.capacity < 2 || (SITH.capacity & (SITH.capacity - 1)) !== 0) { throw new Error("capacity must be a power of two >= 2"); }
			
			this.capacity = SITH.capacity;
			this.channelMask = SITH.capacity - 1;
			this.hub = SITH;
			this.root = SITH.root;
			this.r = SITH.r;
			this.m = SITH.m;
			this.s = SITH.s;
			this.k = SITH.k;
	
			this.channelKey = new Int32Array(SITH.capacity).fill(-1);
			this.channelState = new Uint8Array(SITH.capacity);
			this.channelHash = new Int32Array(SITH.capacity);
	
			this.channelSubHead = new Int32Array(SITH.capacity).fill(NO_SUB);
			this.channelSubTail = new Int32Array(SITH.capacity).fill(NO_SUB);
	
			this.subHandler = new Array(SITH.capacity);
			this.subNext = new Int32Array(SITH.capacity).fill(NO_SUB);
			this.subPrev = new Int32Array(SITH.capacity).fill(NO_SUB);
			this.subChannel = new Int32Array(SITH.capacity).fill(-1);
	
			this.msgChannel = new Int32Array(SITH.capacity).fill(-1);
			this.msgPayload = new Array(SITH.capacity);
			this.msgDeferred = new Array(SITH.capacity);
		}
		// #endregion
		//                                                                                                            [ADDRESS HASHING]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private hashAddress(k: S | N, s: S | N, m: S | N, r: S | N): N {// FNV-1a style; cheap, stable, and allocation-free for primitives.
			
			let h = 0x811c9dc5;
	
			const parts = [r, m, s, k];
	
			for (let i = 0; i < 4; i++) {
				const part = parts[i];
				const str = typeof part === "string" ? part : String(part);
	
				for (let j = 0; j < str.length; j++) {
					h ^= str.charCodeAt(j);
					h = Math.imul(h, 0x01000193);
				}
	
				// Separator reduces collision chance across tiers.
				h ^= 0x9e3779b9;
				h = Math.imul(h, 0x01000193);
			}
	
			return h | 0;
		}
		private subFreeList: N[] = [];
		private allocateSubscriber(): N {
			const reused = this.subFreeList.pop();
	
			if (reused !== undefined) {
				return reused;
			}
	
			const subId = this.allocateSubscriber();
	
			if (subId >= this.capacity) {
				throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted");
			}
	
			return subId;
		}
		private releaseSubscriber(subId: N): V {
			this.subHandler[subId] = undefined;
			this.subChannel[subId] = -1;
			this.subNext[subId] = NO_SUB;
			this.subPrev[subId] = NO_SUB;
	
			this.subFreeList.push(subId);
		}
		// #endregion
		//                                                                                                                   [CHANNELS]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private findChannel(hash: N): N {
			let slot = hash & this.channelMask;
	
			while (true) {
				const state = this.channelState[slot];
	
				if (state === 0) return -1;
	
				if (state === 1 && this.channelHash[slot] === hash) {
					return this.channelKey[slot];
				}
	
				slot = (slot + 1) & this.channelMask;
			}
		}
		private createChannel(hash: N): N {
			if (this.channelCount >= this.capacity) {
				this.grow();
			}
	
			let slot = hash & this.channelMask;
	
			while (this.channelState[slot] === 1) {
				slot = (slot + 1) & this.channelMask;
			}
	
			const channelId = this.channelCount++;
	
			this.channelState[slot] = 1;
			this.channelHash[slot] = hash;
			this.channelKey[slot] = channelId;
	
			this.channelSubHead[channelId] = NO_SUB;
			this.channelSubTail[channelId] = NO_SUB;
	
			return channelId;
		}
		private getOrCreateChannel(k: S | N, s: S | N, m: S | N, r: S | N): N {
			const hash = this.hashAddress(r, m, s, k);
			const existing = this.findChannel(hash);
	
			return existing !== -1 ? existing : this.createChannel(hash);
		}
		// #endregion
		//                                                                                                                        [SUB]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public sub(handler: Handler, k: S | N, s: S | N, m: S | N, r: S | N): () => V {
			if (typeof handler !== "function") { throw new TypeError("handler must be a function"); }
			if (this.subCount >= this.capacity) { this.grow();  }
			
			const channelId = this.getOrCreateChannel(r, m, s, k);
			const subId = this.allocateSubscriber();
	
			if (subId >= this.capacity) { throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted"); }
	
			this.subHandler[subId] = handler;
			this.subChannel[subId] = channelId;
			this.subNext[subId] = NO_SUB;
			this.subPrev[subId] = NO_SUB;
	
			const tail = this.channelSubTail[channelId];
	
			if (tail !== NO_SUB) {
				this.subNext[tail] = subId;
				this.subPrev[subId] = tail;
				this.channelSubTail[channelId] = subId;
			} else {
				this.channelSubHead[channelId] = subId;
				this.channelSubTail[channelId] = subId;
			}
	
			let disposed = false;
	
			return () => {
				if (disposed) return;
				disposed = true;
	
				this.removeSubscriber(subId);
			};
		}
		private removeSubscriber(subId: N): V {
			const channelId = this.subChannel[subId];
	
			if (channelId === -1) return;
	
			const prev = this.subPrev[subId];
			const next = this.subNext[subId];
	
			if (prev !== NO_SUB) this.subNext[prev] = next;
			else this.channelSubHead[channelId] = next;
	
			if (next !== NO_SUB) this.subPrev[next] = prev;
			else this.channelSubTail[channelId] = prev;
	
			this.subHandler[subId] = undefined;
			this.subChannel[subId] = -1;
			this.subNext[subId] = NO_SUB;
			this.subPrev[subId] = NO_SUB;
			this.releaseSubscriber(subId);
		}
		// #endregion
		//                                                                                                                       [SEND]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public post(payload: U, k: A, s: S | N, m: S | N, r: S | N): V {/** Fire-and-forget enqueue. Never awaits handler results. */
			const channelId = this.getOrCreateChannel(r, m, s, k);
			this.enqueue(channelId, payload, undefined);
		}
		public send = (() => {
			const fn =  (payload: U, k: A, s: S | N, m: S | N, r: S | N) => {
				const channelId = this.getOrCreateChannel(r, m, s, k);
				while (this.msgCount >= this.capacity) { this.grow(); }
				let resolve!: (value: U) => V;
				let reject!: (reason?: U) => V;
			
				let settled = false;
				let rejected = false;
				let result: U | undefined;
				let promiseResolve!: (value: U) => V;
				let promiseReject!: (reason?: U) => V;
				const deferred: Deferred = {
					resolve: (v) => {
						if (settled) return;
						settled = true;
						result = v;
						promiseResolve?.(v);
					},
					reject: (e) => {
						if (settled) return;
						settled = true;
						rejected = true;
						result = e;
						promiseReject?.(e);
					},
				};
				let v: A = this.enqueue(channelId, payload, deferred) as A;
				if (v !== undefined && !(v instanceof Promise)) {
					return MaybePromise(v);
				} else {
					const promise = new Promise<U>((res, rej) => {
						promiseResolve = res;
						promiseReject = rej;
				
						if (settled) {
							if (rejected) rej(result);
							else res(result as U);
						}
					});
					return MaybePromise(promise);
				}
			}
			fn.switch = <T = A> (p: U, k: A, s: S = this.s, m: S = this.m, r: S = this.r,  adapter = "ws") => {
				const msg: SIG_BUS_Message = { id: this.hub.REGISTRY._sid(), r: r, m: m, s: s, k: k, p: { p }, ts: Date.now(), _ack: false };
	
				let v: A = this.DISPATCH(msg, adapter);
				if (v !== undefined && !(v instanceof Promise)) {
					return MaybePromise<T>(v);
				} else {
					const timeoutMs = 5000;
					return MaybePromise(
						(() => {
							return new Promise<T>((resolve, reject) => {
								let timer: RET<typeof setTimeout> | UD;
								if (v) {
									if (timer) clearTimeout(timer);
									resolve(v as T);
									return;
								}
								if (timeoutMs) {
									timer = setTimeout(() => { reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`)); }, timeoutMs);
								}
							});
						})(),
					);
				}
			}
			fn.original_send = async (payload: U, k: A, s: S | N, m: S | N, r: S | N): P<U> => {
				const channelId = this.getOrCreateChannel(r, m, s, k);
	
				while (this.msgCount >= this.capacity) { this.grow(); }
		
				let resolve!: (value: U) => V;
				let reject!: (reason?: U) => V;
		
				const deferred: Deferred = { resolve: (v) => resolve(v), reject: (e) => reject(e), };
		
				const promise = new Promise<unknown>((res, rej) => { resolve = res; reject = rej; });
		
				deferred.resolve = resolve;
				deferred.reject = reject;
		
				this.enqueue(channelId, payload, deferred);
		
				return promise;
			}
		return fn;
		})();
		private async waitForCapacity(): P<V> {
			let resolve!: () => V;
			let reject!: (reason?: U) => V;
	
			const deferred: Deferred = {
				resolve: () => resolve(),
				reject: (e) => reject(e),
			};
	
			const promise = new Promise<void>((res, rej) => {
				resolve = res;
				reject = rej;
			});
	
			deferred.resolve = resolve;
			deferred.reject = reject;
	
			this.capacityWaiters.push(deferred);
	
			await promise;
		}
		private enqueue(channelId: N, payload: U, deferred: Deferred | UD): V {
			if (this.msgCount >= this.capacity) { this.grow();	}
	
			const slot = this.msgTail;
	
			this.msgChannel[slot] = channelId;
			this.msgPayload[slot] = payload;
			this.msgDeferred[slot] = deferred;
	
			this.msgTail = (this.msgTail + 1) % this.capacity;
			this.msgCount++;
	
			this.scheduleDrain();
		}
		// #endregion
		//                                                                                                            [Stream / Duplex]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public stream(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>): (payload?: U) => V {// One-way continuous stream. Returns a sender function. Every call pushes a message to every subscriber handler attached to the same address.
			const channelId = this.getOrCreateChannel(r, m, s, k);
	
			// Register consumer handler as a normal subscriber.
			this.subscribeRaw(channelId, handler);
	
			// Sender pushes to the same channel; fire-and-forget.
			return (payload?: unknown) => {
				this.enqueueStream(channelId, payload);
			};
		}
		public openStream(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>): { send: (payload?: U) => V; close: () => V } {
			const channelId = this.getOrCreateChannel(r, m, s, k);
	
			// Attach consumer handler (this end)
			const subId = this.subscribeRaw(channelId, handler);
	
			let closed = false;
	
			const send = (payload?: unknown) => {
				if (closed) return;
				this.enqueueStream(channelId, payload);
			};
	
			const close = () => {
				if (closed) return;
				closed = true;
				this.removeSubscriber(subId);
			};
	
			return { send, close };
		}
		 public duplex(r: S | N, m: S | N, s: S | N, k: S | N, handler: (payload: U) => V | P<V>): { send: (payload?: U) => V; close: () => V } {// Constant two-way stream. Each endpoint can send and receive simultaneously. The handler receives inbound messages; `send()` pushes outbound messages.
			// Create two directional channels:
			//   outbound: this endpoint -> peer
			//   inbound:  peer -> this endpoint
			const outboundChannel = this.getOrCreateChannel(r, m, s, `${k}:out`);
	
			const inboundChannel = this.getOrCreateChannel(r, m, s, `${k}:in`);
	
			// Register this endpoint's inbound handler.
			this.subscribeRaw(inboundChannel, handler);
	
			let closed = false;
	
			const send = (payload?: unknown) => {
				if (closed) return;
				this.enqueueStream(outboundChannel, payload);
			};
	
			const close = () => {
				if (closed) return;
				closed = true;
	
				// Remove this endpoint's inbound handler.
				// (Simple approach: remove all handlers on inbound channel.)
				this.removeAllSubscribers(inboundChannel);
			};
	
			return { send, close };
		}
		// #endregion
		//                                                                                                           [Internal helpers]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private subscribeRaw(channelId: N, handler: (payload: U) => U | P<U>): N {
			if (this.subCount >= this.capacity) {  this.grow();  }
			const subId = this.allocateSubscriber();
			if (subId >= this.capacity) { throw new Error("MESSAGE_BUS_DISRUPTOR: subscriber capacity exhausted"); }
			this.subHandler[subId] = handler;
			this.subChannel[subId] = channelId;
			this.subNext[subId] = NO_SUB;
			this.subPrev[subId] = NO_SUB;
	
			const tail = this.channelSubTail[channelId];
	
			if (tail !== NO_SUB) {
				this.subNext[tail] = subId;
				this.subPrev[subId] = tail;
				this.channelSubTail[channelId] = subId;
			} else {
				this.channelSubHead[channelId] = subId;
				this.channelSubTail[channelId] = subId;
			}
	
			return subId;
		}
		private removeAllSubscribers(channelId: number): void {
			let sub = this.channelSubHead[channelId];
	
			while (sub !== NO_SUB) {
				const next = this.subNext[sub];
				this.removeSubscriber(sub);
				sub = next;
			}
	
			this.channelSubHead[channelId] = NO_SUB;
			this.channelSubTail[channelId] = NO_SUB;
		}
		private enqueueStream(channelId: number, payload: unknown): void {
			if (this.msgCount >= this.capacity) {
				// Drop oldest message to make room (lossy but non-blocking).
				this.msgHead = (this.msgHead + 1) % this.capacity;
				this.msgCount--;
			}
	
			const slot = this.msgTail;
	
			this.msgChannel[slot] = channelId;
			this.msgPayload[slot] = payload;
			this.msgDeferred[slot] = undefined; // no per-message deferred
	
			this.msgTail = (this.msgTail + 1) % this.capacity;
			this.msgCount++;
	
			this.scheduleDrain();
		}
		// #endregion
		//                                                                                                              [Introspection]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public get pending(): N {
			return this.msgCount;
		}
		public get subscriberCount(): N {
			return this.subCount - 1;
		}
		public get getChannelCount(): N {
			return this.channelCount - 1;
		}
		// #endregion
		//                                                                                                                 [Drain loop]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private scheduleDrain(): V {
			if (this.queuedMicrotask || this.draining) return;
	
			this.queuedMicrotask = true;
	
			queueMicrotask(() => {
				this.queuedMicrotask = false;
				this.drain();
			});
		}
		private async drain(): P<V> {
			if (this.draining) return;
	
			this.draining = true;
	
			try {
				while (this.msgCount > 0) {
					const slot = this.msgHead;
					const channelId = this.msgChannel[slot];
					const payload = this.msgPayload[slot];
					const deferred = this.msgDeferred[slot];
					// Advance ring before handler execution so a handler that
					// sends again cannot deadlock the ring.
					this.msgHead = (this.msgHead + 1) % this.capacity;
					this.msgCount--;
					this.msgChannel[slot] = -1;
					this.msgPayload[slot] = undefined;
					this.msgDeferred[slot] = undefined;
					const result = await this.dispatch(channelId, payload);
					if (deferred) { deferred.resolve(result); }
					this.notifyCapacityWaiters();
				}
			} finally {
				this.draining = false;
	
				// If messages arrived while draining, schedule another pass.
				if (this.msgCount > 0) {
					this.scheduleDrain();
				}
			}
		}
		private async dispatch(channelId: N, payload: U): P<U> {
			let sub = this.channelSubHead[channelId];
			if (sub === NO_SUB) return undefined;
			if (this.subNext[sub] === NO_SUB) {// Fast path: exactly one subscriber.
				const handler = this.subHandler[sub];
				if (!handler) return undefined;
				return await handler(payload);
			}
			const results: unknown[] = [];// Multiple subscribers: preserve subscription order.
			const promises: Promise<unknown>[] = [];
			let hasAsync = false;
			while (sub !== NO_SUB) {
				const handler = this.subHandler[sub];
				if (handler) {
					const result = handler(payload);
					if (result && typeof (result as PromiseLike<unknown>).then === "function") {
						hasAsync = true;
						promises.push(result as Promise<unknown>);
						results.push(undefined);
					} else {
						results.push(result);
					}
				}
				sub = this.subNext[sub];
			}
	
			if (hasAsync) {
				const settled = await Promise.all(promises);
				let promiseIndex = 0;// Merge async results back into their original slots.
				for (let i = 0; i < results.length; i++) {
					if (results[i] === undefined && promiseIndex < settled.length) {
						results[i] = settled[promiseIndex++];
					}
				}
			}
	
			return results;
		}
		private notifyCapacityWaiters(): V {
			if (this.capacityWaiters.length === 0) return;
			const waiters = this.capacityWaiters;
			this.capacityWaiters = [];
			for (const waiter of waiters) { waiter.resolve(undefined); }
		}
		private grow(): void {
			const oldCapacity = this.capacity;
			const newCapacity = oldCapacity * 2;
			const newMask = newCapacity - 1;
		
			const channelKey = new Int32Array(newCapacity).fill(-1);
			const channelState = new Uint8Array(newCapacity);
			const channelHash = new Int32Array(newCapacity);
		
			const channelSubHead = new Int32Array(newCapacity).fill(NO_SUB);
			const channelSubTail = new Int32Array(newCapacity).fill(NO_SUB);
		
			// Rehash existing channels directly from the old table.
			for (let oldSlot = 0; oldSlot < oldCapacity; oldSlot++) {
				if (this.channelState[oldSlot] !== 1) continue;
		
				const hash = this.channelHash[oldSlot];
				const channelId = this.channelKey[oldSlot];
		
				let newSlot = hash & newMask;
		
				while (channelState[newSlot] === 1) {
					newSlot = (newSlot + 1) & newMask;
				}
		
				channelState[newSlot] = 1;
				channelHash[newSlot] = hash;
				channelKey[newSlot] = channelId;
		
				channelSubHead[channelId] = this.channelSubHead[channelId];
				channelSubTail[channelId] = this.channelSubTail[channelId];
			}
		
			const subHandler = new Array<Handler | undefined>(newCapacity);
			const subNext = new Int32Array(newCapacity).fill(NO_SUB);
			const subPrev = new Int32Array(newCapacity).fill(NO_SUB);
			const subChannel = new Int32Array(newCapacity).fill(-1);
		
			for (let subId = 0; subId < this.subCount; subId++) {
				subHandler[subId] = this.subHandler[subId];
				subNext[subId] = this.subNext[subId];
				subPrev[subId] = this.subPrev[subId];
				subChannel[subId] = this.subChannel[subId];
			}
		
			const msgChannel = new Int32Array(newCapacity).fill(-1);
			const msgPayload: unknown[] = new Array(newCapacity);
			const msgDeferred: (Deferred | undefined)[] = new Array(newCapacity);
		
			// Copy the ring in FIFO order, preserving wraparound.
			for (let i = 0; i < this.msgCount; i++) {
				const oldSlot = (this.msgHead + i) % oldCapacity;
		
				msgChannel[i] = this.msgChannel[oldSlot];
				msgPayload[i] = this.msgPayload[oldSlot];
				msgDeferred[i] = this.msgDeferred[oldSlot];
			}
		
			this.channelKey = channelKey;
			this.channelState = channelState;
			this.channelHash = channelHash;
		
			this.channelSubHead = channelSubHead;
			this.channelSubTail = channelSubTail;
		
			this.subHandler = subHandler;
			this.subNext = subNext;
			this.subPrev = subPrev;
			this.subChannel = subChannel;
		
			this.msgChannel = msgChannel;
			this.msgPayload = msgPayload;
			this.msgDeferred = msgDeferred;
		
			this.channelMask = newMask;
			this.capacity = newCapacity;
		
			this.msgHead = 0;
			this.msgTail = this.msgCount;
		}
		// #endregion
		//                                                                                                                 [Drain loop]
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	
	})(this);
	public MAREK = class MAREK {
		public r: S = "THE_WIZARD_ROOT_REG";
		public m: S = "SAURON_MASTER_REG";
		public s: S = "SAURON_SLAVE_REG";
		public k: S = "__SAURON__";
		public mesh_store: THE_MESH_STORE;
		public root: DEATH_STAR;
		public hub: SITH;
		constructor(hub: SITH) {
			this.root = hub.root;
			this.hub = hub;
			this.r = hub.r;
			this.m = hub.m;
			this.s = hub.s;
			this.k = hub.k;
		}
		
		public send = (() => {
			const fn = <T = A,>(p: U, k: A, s: S = this.s, m: S = this.m, r: S = this.r,  adapter = "ws") => {
				//const msg: SIG_BUS_Message = { id: this.hub.REGISTRY._sid(), r: r, m: m, s: s, k: k,  p: p, ts: Date.now(), _ack: false };
				let v: A = this.DISPATCH({ id: this.hub.REGISTRY._sid(), r: r, m: m, s: s, k: k, p: p, _ack: false }, adapter);
				if (v !== undefined && !(v instanceof Promise)) { return MaybePromise<T>(v); }
				else {
					const tMs = 5000;
					return MaybePromise( (() => {
							return new P<T>((resolve, reject) => {
								let timer: RT<typeof setTimeout> | UD;
								if (v) { if (timer) clearTimeout(timer); resolve(v as T); return; }
								if (tMs) { timer = setTimeout(() => { reject(new Error(`wait() timed out after ${tMs}ms`)); }, tMs); }
							});
						})(),
					);
				}
				if (r === this.r && m === this.m && s === this.s) {
					
				} else {
	
				}
				
			};
			fn.gt = (() => {
				const fn =<T = U, P = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r)  => {
					const root = globalThis as any;
					const rL = (root[r] ??= Object.create(null));
					const mL = (rL[m] ??= Object.create(null));
					const channel = mL[s] ??= Object.create(null);
					const handler = channel?.handler;
					if (!handler) return undefined;
					const v: A = handler(p, { r, m, s, k, p: p });
					if (v !== undefined && !(v instanceof Promise)) { return MaybePromise<T>(v); }
					else {
						const tMs = 5000;
						return MaybePromise( (() => {
								return new P<T>((resolve, reject) => {
									let timer: RT<typeof setTimeout> | UD;
									if (v) { if (timer) clearTimeout(timer); resolve(v as T); return; }
									if (tMs) { timer = setTimeout(() => { reject(new Error(`wait() timed out after ${tMs}ms`)); }, tMs); }
								});
							})(),
						);
					}
				}
				fn.hot = (() => {
					const fn =<T = U, P = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {
						const channel = this.mesh_store._globalThis[k];
						const handler = channel?.handler;
						if (!handler) return undefined;
						const v: A = handler(p, { r, m, s, k, p: p });
						if (v !== undefined && !(v instanceof Promise)) { return MaybePromise<T>(v); }
						else {
							const tMs = 5000;
							return MaybePromise( (() => {
									return new P<T>((resolve, reject) => {
										let timer: RT<typeof setTimeout> | UD;
										if (v) { if (timer) clearTimeout(timer); resolve(v as T); return; }
										if (tMs) { timer = setTimeout(() => { reject(new Error(`wait() timed out after ${tMs}ms`)); }, tMs); }
									});
								})(),
							);
						}
					}
					return fn;
				})();
				return fn;
			})();
			fn.adapter = (() => {
				const fn =()  => { }
				fn.hot = (() => {
					const fn = () =>{ }
					return fn;
				})();
				return fn;
			})();
			fn.stream=<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD =>{
				const channel = this.mesh_store._globalThis[k];
				const handler = channel?.handler;
				if (!handler) return undefined;
				const msg: SIG_BUS_Message1 = { r, m, s, k, p: p, _ack: false };
				const result = handler(p, msg);
				const listeners = channel.push;
				if (listeners) { for (const listener of listeners) listener(msg); }
				return result as T | P<T> | UD;
			}
			fn.nested = (p: P, k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {
				if (r === this.r && m === this.m && s === this.s) {
					const set = this.mesh_store._globalThis[k];
					if (!set) return;
					for (const fn of set) { fn(p); }
				} else {
					const root = globalThis as any;
					const rL = (root[r] ??= Object.create(null));
					const mL = (rL[m] ??= Object.create(null));
					const _listeners = mL[s] ??= Object.create(null);
					const set = _listeners.get(k);
					if (!set) return;
					for (const fn of set) { fn(p); }
				}
			};
			return fn;
		})();
		public sub = (() => {
			const fn = ( handler: A, k: S, s: S = this.s, m: S = this.m, r: S = this.r, c: S = "#handlers" ) => {
				if ( r === this.r && m === this.m && s === this.s ) {
					const root = this.mesh_store.get_(c);
					let handlers = root[k] as Set<A> | undefined;
					if (!handlers) { handlers = new Set<A>(); root[k] = handlers as A; }
					handlers.add(handler);
					return () => { handlers!.delete(handler); if (handlers!.size === 0) { delete root[k]; } };
				}
		
				// CHANNEL LEVEL
				const channelSet = (this.mesh_store.getR(r, c)[r] ??= new Set<A>()) as Set<A>;
				channelSet.add(handler);
		
				// SUB_CHANNEL LEVEL
				const subMap = this.mesh_store.getM(m, r, c);
				const subSet = (subMap[r] ??= new Set<A>()) as Set<A>;
				subSet.add(handler);
		
				// TRIP_CHANNEL LEVEL
				const tripMap = this.mesh_store.getS(s, m, r, c);
				const tripSet = (tripMap[m] ??= new Set<A>()) as Set<A>;
				tripSet.add(handler);
		
				// QUAD_CHANNEL LEVEL
				const quadMap = (tripMap[k] ??= Object.create(null)) as Record<S, Set<A>>;
				const quadSet = (quadMap[s] ??= new Set<A>()) as Set<A>;
				quadSet.add(handler);
		
				return () => {
					// QUAD_CHANNEL
					quadSet.delete(handler);
					if (!quadSet.size) delete quadMap[s];
					if (!Object.keys(quadMap).length) delete tripMap[k];
		
					// TRIP_CHANNEL
					tripSet.delete(handler);
					if (!tripSet.size) delete tripMap[m];
		
					// SUB_CHANNEL
					subSet.delete(handler);
					if (!subSet.size) delete subMap[r];
		
					// CHANNEL
					channelSet.delete(handler);
					if (!channelSet.size) delete this.mesh_store.getR(r, c)[r];
		
					// Remove empty hierarchy nodes
					if (!Object.keys(subMap).length) { delete this.mesh_store.get_root(c, r)[m]; }
					if (!Object.keys(tripMap).length) { delete this.mesh_store.get_root(c, r)[m]; }
				};
			};
			fn.gt = (() => {
				const fn =()  => { }
				fn.hot = (() => {
					const fn =<P = U, T = U> (handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V =>{
						const channel = this.mesh_store._globalThis[k];
						channel.handler = handler;
						return () => {
							if (channel.handler === handler) {
								channel.handler = undefined;
							}
						};
					}
					return fn;
				})();
				return fn;
			})();
			fn.adapter = (() => {
				const fn =()  => { }
				fn.hot = (() => {
					const fn = () =>{ }
					return fn;
				})();
				return fn;
			})();
			fn.nested = (handler: (data: any) => void, k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {
				if (r === this.r && m === this.m && s === this.s) {
					const _listeners = this._bear;
					if (!_listeners.has(k)) {
						_listeners.set(k, new Set());
					}
					_listeners.get(k)!.add(handler);
					return () => {
						const set = _listeners.get(k);
						if (set) {
							set.delete(handler);
							if (set.size === 0) _listeners.delete(k);
						}
					};
				} else {
					const _listeners = this.map(s, m, r, "#BEAR");
					if (!_listeners.has(k)) {
						_listeners.set(k, new Set());
					}
					_listeners.get(k)!.add(handler);
					return () => {
						const set = _listeners.get(k);
						if (set) {
							set.delete(handler);
							if (set.size === 0) _listeners.delete(k);
						}
					};
				}
			};
			fn.nested3 = (p: P, k: S, s: S = this.s, m: S = this.m, r: S = this.r) => {
				if (r === this.r && m === this.m && s === this.s) {
				} else {
				}
			};
			return fn;
		})();
		async *streamGen<P = A, R = A>(p: P, k: S, s: S = this.s, m: S = this.m, r: S = this.r, opts: { adapter?: S } = {}): AG<R, V, U> {
			const replyTo = this.hub.REGISTRY._sid();
			const queue: R[] = [];
			let closed = false;
			let failure: U;
			let wake: (() => V) | UD;
			const handlers = this.mesh_store.getS(k, s, m, r).get(k);
			if (!handlers || !handlers.size) { throw new Error(`[rift] no subscriber for k="${k}"`); }
			const reply = (value: R | Promise<R> | typeof END) => {
				if (value === END) { closed = true; } 
				else if (value instanceof Error) { failure = value; }
				else { queue.push(value as R); }
				wake?.();
				wake = undefined;
			};
			for (const handler of handlers) {
				handler({ id: this.hub.REGISTRY._sid(), r: r ?? this.r, m: m ?? this.m, s: s ?? this.s, k, p: { p }, ts: Date.now(), _ack: false, replyTo, stream: true }, reply);
			}
			while (true) {
				if (queue.length) { yield queue.shift()!; continue; }
				if (failure) throw failure;
				if (closed) return;
				await new P<V>((resolveWake) => { wake = resolveWake; });
			}
		}
		public stream = (() => {
			function stream(handler: (msg: SIG_BUS_Message1) => V, k: S, s: S, m?: S, r?: S): () => V;
			function stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<SIG_BUS_Message1>;
			function stream(handlerOrSlave: ((msg: SIG_BUS_Message1) => V) | S, k: S, s: S = this.s, m: S = this.m, r: S = this.r): (() => V) | AsyncIterableIterator<SIG_BUS_Message1> {
					const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
					const global = globalThis as any;
					const channel = (this.channels[k] ??= {});
					if (!channel) throw new Error("Channel does not exist");
					const listeners = (channel.push ??= new Set());
			
					if (handler) {
						listeners.add(handler);
						return () => { listeners.delete(handler); if (!listeners.size) channel.push = undefined; };
					}
			
					const queue: SIG_BUS_Message1[] = [];
					const waiters: Array<{ resolve: (value: IteratorResult<SIG_BUS_Message1>) => void }> = [];
					let offset = 0;
					let closed = false;
					const listener = (msg: SIG_BUS_Message1) => {
						if (waiters.length) { waiters.shift()!.resolve({ value: msg, done: false }); } 
						else { queue.push(msg); }
					};
					listeners.add(listener);
					const close = () => {
						if (closed) return;
						closed = true;
						listeners.delete(listener);
						if (!listeners.size) channel.push = undefined;
						for (const waiter of waiters) { waiter.resolve({ value: undefined, done: true }); }
						waiters.length = 0;
						queue.length = 0;
						offset = 0;
					};
			
					const iterator: AsyncIterableIterator<SIG_BUS_Message1> = {
						next: (): Promise<IteratorResult<SIG_BUS_Message1>> => {
							if (offset < queue.length) {
								const value = queue[offset++];
								if (offset === queue.length) { queue.length = 0; offset = 0; }
								return Promise.resolve({ value, done: false });
							}
							if (closed) { return Promise.resolve({ value: undefined, done: true }); }
							return new Promise((resolve) => waiters.push({ resolve }));
						},
						return: async () => { close(); return { value: undefined, done: true }; },
						[Symbol.asyncIterator](): AsyncIterableIterator<SIG_BUS_Message1> { return iterator; },
					};
			
					return iterator;
			}
			const fn = () => stream
			fn.request = (p: A, onChunk: (chunk: A) => V, k: S, s: S = this.s, m: S = this.m, r: S = this.r): P<V> => {
				const streamId = this.hub.REGISTRY._sid();// `[${r}][${m}][${s}][${k}][${_sid()}]`.replace(/[.\[\]]/g, match => (match === "." ? "_" : ""));
				const result = new Promise<void>((resolve, reject) => {
					let closed = false;
					let closeSubscription: (() => V) | UD;
					const finish = (error?: unknown) => {
						if (closed) return;
						closed = true;
						closeSubscription?.();
						if (error) { reject(error instanceof Error ? error : new Error(String(error))); }
						else { resolve(); }
					};
					closeSubscription = this.j.sub(
						(data: StreamEvent) => {
							if (closed) return;
		
							switch (data.type) {
								case "chunk": onChunk(data.data); break;
								case "done": finish(); break;
								case "error": finish(data.error); break;
							}
						}, k, s, m, r, );
		
					try { this.send({streamId, ...p}, k, s, m, r); }
					catch (error) { finish(error); }
				});
				return result;
			};
			fn.native = (k?: string, s = this.s, m = this.m, r = this.r): WritableStream => {
				return new WritableStream({ write: (chunk) => { this.send(chunk, k, s, m, r).send(); }, });
			};
			fn.openSync = (k: S, s?: S, m?: S, r = this.r, opts?: {windowSize?: N}) => {
				// Like syncStream, but for continuous/unbounded emission — no known chunk count up front. Adds a bounded in-flight window so a fast sender backs off instead of flooding a slow receiver.
				const cleanChannel = r.replace(/[\[\]]/g, "");
				const streamId = this.hub.REGISTRY._sid()//`${cleanChannel}-${m}-${Math.random().toString(36).slice(2)}`.replace(/\./g, "-");
				const windowSize = opts?.windowSize ?? 32;
		
				const unacked = new Map<number, {p: any; retries: number}>();
				let nextSeq = 0;
				let acknowledgedUpTo = -1;
				const sendQueue: any[] = [];
				let draining = false;
		
				const closeAckSub = this.sub((ackData: any) => {
						if (typeof ackData?.seq !== "number") return;
						unacked.delete(ackData.seq);
						if (ackData.seq > acknowledgedUpTo) acknowledgedUpTo = ackData.seq;
						drain(); // an ack frees a window slot — try sending more
					},k, `sync.${streamId}:ack`, m, r, );
		
				const closeRetransmitSub = this.sub(async (reqData: any) => {
						const missing = unacked.get(reqData?.seq);
						if (missing) {missing.retries++;await this.send({streamId, seq: reqData.seq, data: missing.p, isRetry: true}, k, s, m, r);}
					}, k, `sync.${streamId}:retransmit`, m, r, );
		
				const drain = async () => {
					if (draining) return;
					draining = true;
					while (sendQueue.length > 0 && unacked.size < windowSize) {
						const p = sendQueue.shift();
						const seq = nextSeq++;
						unacked.set(seq, {p, retries: 0});
						await this.send({streamId, seq, data: p, isRetry: false}, k, s, m, r);
					}
					draining = false;
				};
		
				return {/** Queue an op for send. Backs off automatically if the receiver hasn't acked enough of the window yet. */
					push: (p: any) => {sendQueue.push(p);drain();},
					pendingCount: () => unacked.size + sendQueue.length,
					close: () => {closeAckSub();closeRetransmitSub();},
				};
			}; 
			return fn;
		})();
		public handler = (() => {
			const fn = (handler: (d: any) => any, k: string, s?: string, m?: string, r?: string, binary = false) => {
				return this.sub(
					async (inboundReq: any) => {
						if (!inboundReq || !inboundReq.streamId) return;
						const streamId = inboundReq.streamId;
						let isCancelled = false;
						let closeControlSubscription: (() => void) | null = null;
						try {// 1. Await the control stream cancellation listener safely
							closeControlSubscription = this.sub( () => { isCancelled = true; }, `stream.${streamId}:cancel`, s, m, r, );
							const result = await handler(inboundReq);
							let data = result;
							if (result && typeof result === "object" && "ok" in result && "data" in result) { data = result.data; }
							if (typeof data === "function") { data = data(); }
							
							if (data && typeof data[Symbol.asyncIterator] === "function") {// Core Async Generator Stream Loop
								for await (const chunk of data) {
									if (isCancelled) { if (typeof data.return === "function") { await data.return(); } break; }
									await this.send({type: "chunk", data: chunk}, k, `stream.${streamId}`, m, r);
								}
								if (!isCancelled) await this.send({type: "done"}, k, `stream.${streamId}`, m, r);
								if (closeControlSubscription) closeControlSubscription();
								return;
							}// Sync Generator Stream Loop
							if (data && typeof data[Symbol.iterator] === "function" && !Array.isArray(data)) {
								for (const chunk of data) {
									if (isCancelled) break;
									await this.send({type: "chunk", data: chunk}, k, `stream.${streamId}`, m, r);
								}
								if (!isCancelled) await this.send({type: "done"}, k, `stream.${streamId}`, m, r);
								if (closeControlSubscription) closeControlSubscription();
								return;
							}// Array Handling Stream Loop
							if (Array.isArray(data)) {
								for (const chunk of data) {
									if (isCancelled) break;
									await this.send({type: "chunk", data: chunk}, k, `stream.${streamId}`, m, r);
								}
								if (!isCancelled) await this.send({type: "done"}, k, `stream.${streamId}`, m, r);
								if (closeControlSubscription) closeControlSubscription();
								return;
							}// Fallback Single Value Response
							if (!isCancelled) {
								await this.send({type: "chunk", data: data}, k, `stream.${streamId}`, m, r);
								await this.send({type: "done"}, k, `stream.${streamId}`, m, r);
							}
							if (closeControlSubscription) closeControlSubscription();
						} catch (err: any) {
							if (closeControlSubscription) closeControlSubscription();
							//SE._deadLetters().push({id: streamId, r: r || this.r, p: inboundReq, ts: Date.now(), error: err.message});
							this.send({type: "error", error: err.message}, k, `stream.${streamId}`, m, r);
						}
					}, k, s, m, r,
				);
			};
			fn.native = (k?: string, s = this.s, m = this.m, r = this.r) => {
				let unsubscribe: (() => void) | null = null;
				return new ReadableStream({
					start: (controller) => { unsubscribe = this.sub( (data) => { try { controller.enqueue(data); } catch {}  }, k, s, m, r, ); },
					cancel: () => { if (unsubscribe) { unsubscribe(); unsubscribe = null; } },
				});
			};
			fn.openSync = (handler: (chunk: any, seq: number) => Promise<void> | void, k: string, s: SU = this.s, m: SU = this.m, r: SU = this.r): (() => void) => {
				const expectedSeqMap = new Map<string, number>();
				const bufferMap = new Map<string, Map<number, any>>();
		
				return this.sub(
					async (inbound: any) => {
						if (!inbound?.streamId) return;
						const {streamId, seq, data} = inbound;
		
						if (!expectedSeqMap.has(streamId)) {
							expectedSeqMap.set(streamId, 0);
							bufferMap.set(streamId, new Map());
						}
						const expectedSeq = expectedSeqMap.get(streamId)!;
						const buffer = bufferMap.get(streamId)!;
		
						if (seq === expectedSeq) {
							await handler(data, seq);
							expectedSeqMap.set(streamId, expectedSeq + 1);
							await this.send({seq}, k, `sync.${streamId}:ack`, m, r);
		
							let next = expectedSeq + 1;
							while (buffer.has(next)) {
								await handler(buffer.get(next), next);
								buffer.delete(next);
								next++;
								expectedSeqMap.set(streamId, next);
								await this.send({seq: next - 1}, k, `sync.${streamId}:ack`, m, r);
							}
						} else if (seq > expectedSeq) {
							buffer.set(seq, data);
							for (let missing = expectedSeq; missing < seq; missing++) {
								if (!buffer.has(missing)) {
									await this.send({seq: missing}, k, `sync.${streamId}:retransmit`, m, r);
								}
							}
						} else {
							await this.send({seq}, k, `sync.${streamId}:ack`, m, r); // duplicate — re-ack, no reprocess
						}
					},
					k,
					s,
					m,
					r,
				);
			};
			return fn;
		})();
		public duplex = (() => {
			const fn = async <A = any, B = any>(k: string, s = this.s, m = this.m, r = this.r) => {
				const hub = {k, s, m, r}
				return new duplex(hub)
			}	 
			fn.stream = (readConfig: { k: S; sl?: S; ms?: S; rt?: S }, writeConfig: { k?: S; sl?: S; ms?: S; rt?: S }): { readable: ReadableStream; writable: WritableStream } => {
				return {
					readable: this.handler(readConfig.k, readConfig.sl, readConfig.ms, readConfig.rt),
					writable: this.stream(writeConfig.k, writeConfig.sl, writeConfig.ms, writeConfig.rt),
				};
			};
			fn.sx = <TUp = any, TDown = any>(k: string, s?: string, m?: string, r?: string, opts?: {windowSize?: number; livenessId?: string}) => {
				const windowSize = opts?.windowSize ?? 32;
				const up = this.stream.openSync(k, `${s}:up`, m, r, {windowSize});
				const downHandlers = new Set<(chunk: TDown, seq: number) => void>();
				const errorHandlers = new Set<(err: DuplexError) => void>();
				const closeHandlers = new Set<() => void>();
		 
				let upClosed = false;
				let downClosed = false;
				let fatallyErrored = false;
		 
				let lastSeqSeen = -1;
		 
				const down = this.handler.openSync(
					(chunk: TDown, seq: number) => {
						if (fatallyErrored) return;
						if (lastSeqSeen !== -1 && seq !== lastSeqSeen + 1) {
							reportError({dir: "down", message: `sequence gap: expected ${lastSeqSeen + 1}, got ${seq}`, fatal: false});
						}
						lastSeqSeen = seq;
						downHandlers.forEach(h => h(chunk, seq));
					},
					k,
					`${s}:down`,
					m,
					r,
				);
		 
				// remote-reported errors arrive here — this is what makes error propagation BILATERAL,
				// not just local: the other side explicitly tells us when something broke on their end
				const errorSub = this.sub(
					(err: DuplexError) => {
						errorHandlers.forEach(h => h(err));
						if (err.fatal) {
							fatallyErrored = true;
							teardown();
						}
					},
					k,
					`${s}:error`,
					m,
					r,
				);
		 
				// remote-reported half-close — lets THIS side know the peer will send no more, so onData
				// consumers can know to expect a natural end rather than silence-as-ambiguity
				const remoteCloseSub = this.sub(
					(dir: "up" | "down") => {
						if (dir === "up") downClosed = true; // peer's up = our down, from our perspective
						closeHandlers.forEach(h => h());
					},
					k,
					`${s}:remote-close`,
					m,
					r,
				);
		 
				const reportError = (err: DuplexError) => {
					errorHandlers.forEach(h => h(err));
					this.send(err, k, `${s}:error`, m, r); // propagate to the OTHER side too — bilateral, not local-only
					if (err.fatal) {
						fatallyErrored = true;
						teardown();
					}
				};
		 
				let tornDown = false;
				const teardown = () => {
					if (tornDown) return;
					tornDown = true;
					up.close();
					down();
					errorSub();
					remoteCloseSub();
					downHandlers.clear();
					errorHandlers.clear();
					closeHandlers.clear();
				};
		 
				return {
					send: (p: TUp) => {
						if (upClosed) {
							reportError({dir: "up", message: "send after half-close", fatal: false, retryable: false, code: "HALF_CLOSED"});
							return;
						}
						if (fatallyErrored) return;
						try {
							up.push(p);
						} catch (err: any) {
							reportError({dir: "up", message: err.message ?? String(err), fatal: false, retryable: true, code: "SEND_FAILED"});
						}
					},
					onData: (handler: (chunk: TDown, seq: number) => void) => {
						downHandlers.add(handler);
						return () => downHandlers.delete(handler);
					},
					onError: (handler: (err: DuplexError) => void) => {
						errorHandlers.add(handler);
						return () => errorHandlers.delete(handler);
					},
					onRemoteClose: (handler: () => void) => {
						closeHandlers.add(handler);
						return () => closeHandlers.delete(handler);
					},
					/** Half-close: stop sending, but keep receiving. Peer is notified so they know THIS side is done sending. */
					closeUp: () => {
						if (upClosed) return;
						up.close();
						upClosed = true;
						this.send("up", k, `${s}:remote-close`, m, r);
					},
					/** Full close of both directions, notifies peer, tears down liveness tracking. */
					close: () => {
						if (!upClosed) this.send("up", k, `${s}:remote-close`, m, r);
						upClosed = true;
						downClosed = true;
						teardown();
					},
					isFullyClosed: () => upClosed && downClosed,
					isFatallyErrored: () => fatallyErrored,
				};
			};
			fn.async = <TUp = any, TDown = any>(k = this.j.k, s = this.s, m = this.m, r = this.r, opts?: {windowSize?: number}) => {
				const windowSize = opts?.windowSize ?? 32;
		 
				const up = this.stream.sync(k, `${s}:up`, m, r, {windowSize});
		 
				const downHandlers = new Set<(chunk: TDown, seq: number) => void>();
				const errorHandlers = new Set<(err: DuplexError) => void>();
				const closeHandlers = new Set<() => void>();
		 
				let upClosed = false;
				let downClosed = false;
				let fatallyErrored = false;
				let tornDown = false;
		 
				let lastSeqSeen = -1;
				let downUnsub: any;
				let errorUnsub: any;
				let remoteCloseUnsub: any;
		 
				const down = () => {
					const stream = this.handler.sync(k, `${s}:down`, m, r); // Accept p as unknown, then cast to TDown
					stream.onOrdered((p: unknown, seq: number) => {
						if (fatallyErrored) return;
						const chunk = p as TDown;
						if (lastSeqSeen !== -1 && seq !== lastSeqSeen + 1) {
							reportError({dir: "down", message: `sequence gap: expected ${lastSeqSeen + 1}, got ${seq}`, fatal: false});
						}
		 
						lastSeqSeen = seq;
						downHandlers.forEach(h => h(chunk, seq));
					});
		 
					downUnsub = () => stream.close();
				};
				const errorSub = () => {
					errorUnsub = this.sub(
						(err: DuplexError) => {
							errorHandlers.forEach(h => h(err));
							if (err.fatal) {
								fatallyErrored = true;
								teardown();
							}
						},
						k,
						`${m}:error`,
						m,
						r,
					);
				};
				const remoteCloseSub = () => {
					remoteCloseUnsub = this.sub(
						(dir: "up" | "down") => {
							if (dir === "up") downClosed = true;
							closeHandlers.forEach(h => h());
						},
						k,
						`${s}:remote-close`,
						m,
						r,
					);
				};
				const reportError = (err: DuplexError) => {
					errorHandlers.forEach(h => h(err));
					this.send(err, k, `${s}:error`, m, r);
					if (err.fatal) {
						fatallyErrored = true;
						teardown();
					}
				};
				const teardown = () => {
					if (tornDown) return;
					tornDown = true;
					up.close();
					downUnsub?.();
					errorUnsub?.();
					remoteCloseUnsub?.();
		 
					downHandlers.clear();
					errorHandlers.clear();
					closeHandlers.clear();
				};
				down();
				errorSub();
				remoteCloseSub();
				const dx = {
					send: (p: TUp) => {
						if (upClosed) {
							reportError({dir: "up", message: "send after half-close", fatal: false, retryable: false, code: "HALF_CLOSED"});
							return;
						}
						if (fatallyErrored) return;
						try {
							up.push(p);
						} catch (err: any) {
							reportError({dir: "up", message: err.message ?? String(err), fatal: false, retryable: true, code: "SEND_FAILED"});
						}
					},
					onData: (handler: (chunk: TDown, seq: number) => void) => {
						downHandlers.add(handler);
						return () => downHandlers.delete(handler);
					},
					onError: (handler: (err: DuplexError) => void) => {
						errorHandlers.add(handler);
						return () => errorHandlers.delete(handler);
					},
					onRemoteClose: (handler: () => void) => {
						closeHandlers.add(handler);
						return () => closeHandlers.delete(handler);
					},
					closeUp: () => {
						if (upClosed) return;
						up.close();
						upClosed = true;
						this.send("up", k, `${s}:remote-close`, m, r);
					},
					close: () => {
						if (!upClosed) {
							this.send("up", k, `${s}:remote-close`, m, r);
						}
						upClosed = true;
						downClosed = true;
						teardown();
					},
					isFullyClosed: () => upClosed && downClosed,
					isFatallyErrored: () => fatallyErrored,
				};
				return dx;
			};
			fn.open = <T = any,>(opts = {highWaterMark: 32}, k: string, s = this.s, m = this.m, r = this.r) => {
				const streamId = this.hub.REGISTRY._sid()
		 
				const highWaterMark = opts?.highWaterMark ?? 32;
		 
				const queue: T[] = [];
				const handlers = new Set<(chunk: T) => void>();
				const drainHandlers = new Set<() => void>();
				const closeHandlers = new Set<() => void>();
				const errorHandlers = new Set<(err: any) => void>();
		 
				let writable = true;
				let readable = true;
				let draining = false;
		 
				const write = async (chunk: T): Promise<void> => {
					if (!writable) { throw new Error("Stream is not writable"); }
					queue.push(chunk);
					if (queue.length >= highWaterMark) {
						await new Promise<void>(resolve => {
							const check = () => {
								if (queue.length < highWaterMark) {
									drainHandlers.delete(check);
									resolve();
								}
							};
		 
							drainHandlers.add(check);
							check();
						});
					}
		 
					await drain();
				};
				const drain = async () => {
					if (draining) return;
					draining = true;
					try {
						while (queue.length > 0 && writable) {
							const chunk = queue.shift()!;
							await this.send({streamId, data: chunk}, k, s, m, r);
							drainHandlers.forEach(handler => handler());
						}
					} finally {
						draining = false;
					}
				};
				const unsubscribe = this.sub( async (packet: any) => {
						if (packet?.streamId !== streamId) return;
						if (packet.type === "data") { handlers.forEach(handler => { handler(packet.data); }); }
						if (packet.type === "close") { readable = false; closeHandlers.forEach(handler => handler()); }
						if (packet.type === "error") { errorHandlers.forEach(handler => { handler(packet.error); }); }
				}, k, s, m, r, );
				return {
					write,
					onData: (handler: (chunk: T) => void) => {
						handlers.add(handler);
						return () => { handlers.delete(handler); };
					},
					onDrain: (handler: () => void) => {
						drainHandlers.add(handler);
						return () => { drainHandlers.delete(handler); };
					},
					onClose: (handler: () => void) => {
						closeHandlers.add(handler);
						return () => { closeHandlers.delete(handler); };
					},
					onError: (handler: (err: any) => void) => {
						errorHandlers.add(handler);
						return () => { errorHandlers.delete(handler); };
					},
					close: async () => {
						if (!writable && !readable) return;
						writable = false;
						await this.j.send({streamId, type: "close"}, k, s, m, r);
						unsubscribe();
						closeHandlers.forEach(handler => handler());
					},
					abort: async (reason?: any) => {
						writable = false;
						readable = false;
						await this.j.send({streamId, type: "error", error: reason}, k, s, m, r);
						unsubscribe();
						errorHandlers.forEach(handler => handler(reason));
					},
					writable: () => writable,
					readable: () => readable,
					buffered: () => queue.length,
				};
			};
			return fn;
		})();
		public d_handler = (() => {
			const fn = () => {}
			fn.sx = <TUp = any, TDown = any>(k: string, s?: string, m?: string, r?: string, opts?: {windowSize?: number; livenessId?: string}) => {
				const windowSize = opts?.windowSize ?? 32;
				const down = this.stream.openSync(k, `${s}:down`, m, r, {windowSize});
				const upHandlers = new Set<(chunk: TUp, seq: number) => void>();
				const errorHandlers = new Set<(err: DuplexError) => void>();
				const closeHandlers = new Set<() => void>();
		
				let downClosed = false;
				let upClosed = false;
				let fatallyErrored = false;
		
				let lastSeqSeen = -1;
				const up = this.handler.openSync(
					(chunk: TUp, seq: number) => {
						if (fatallyErrored) return;
						if (lastSeqSeen !== -1 && seq !== lastSeqSeen + 1) {
							reportError({dir: "down", message: `sequence gap: expected ${lastSeqSeen + 1}, got ${seq}`, fatal: false});
						}
						lastSeqSeen = seq;
						upHandlers.forEach(h => h(chunk, seq));
					},
					k,
					`${s}:up`,
					m,
					r,
				);
		
				const errorSub = this.sub(
					(err: DuplexError) => {
						errorHandlers.forEach(h => h(err));
						if (err.fatal) {
							fatallyErrored = true;
							teardown();
						}
					},
					k,
					`${s}:error`,
					m,
					r,
				);
		
				const remoteCloseSub = this.sub(
					(dir: "up" | "down") => {
						if (dir === "up") upClosed = true; // peer's up = our up, from THIS side's receiving perspective
						closeHandlers.forEach(h => h());
					},
					k,
					`${s}:remote-close`,
					m,
					r,
				);
		
				const reportError = (err: DuplexError) => {
					errorHandlers.forEach(h => h(err));
					this.send(err, k, `${s}:error`, m, r);
					if (err.fatal) {
						fatallyErrored = true;
						teardown();
					}
				};
		
				let tornDown = false;
				const teardown = () => {
					if (tornDown) return;
					tornDown = true;
					down.close();
					up();
					errorSub();
					remoteCloseSub();
					upHandlers.clear();
					errorHandlers.clear();
					closeHandlers.clear();
				};
		
				return {
					send: (p: TDown) => {
						if (downClosed) {
							reportError({dir: "down", message: "send after half-close", fatal: false, retryable: false, code: "HALF_CLOSED"});
							return;
						}
						if (fatallyErrored) return;
						try {
							down.push(p);
						} catch (err: any) {
							reportError({dir: "down", message: err.message ?? String(err), fatal: false, retryable: true, code: "SEND_FAILED"});
						}
					},
					onData: (handler: (chunk: TUp, seq: number) => void) => {
						upHandlers.add(handler);
						return () => upHandlers.delete(handler);
					},
					onError: (handler: (err: DuplexError) => void) => {
						errorHandlers.add(handler);
						return () => errorHandlers.delete(handler);
					},
					onRemoteClose: (handler: () => void) => {
						closeHandlers.add(handler);
						return () => closeHandlers.delete(handler);
					},
					closeDown: () => {
						if (downClosed) return;
						down.close();
						downClosed = true;
						this.send("down", k, `${s}:remote-close`, m, r);
					},
		
					close: () => {
						if (!downClosed) this.send("down", k, `${s}:remote-close`, m, r);
						downClosed = true;
						upClosed = true;
						teardown();
					},
		
					isFullyClosed: () => downClosed && upClosed,
					isFatallyErrored: () => fatallyErrored,
				};
			};
			return fn;
		})();	
		DISPATCH<T = A>(msg:A, replyTo:A, handlers:A, timeoutMs:A): P<T> {
			// ── ASYNC PATH: QUEUE, NOT LOOP ──────────────────────
			let settled = false;
			let resolveFn: (v: T) => V;
			let rejectFn: (e: U) => V;
			let timer: A;
			const settle = (value?: T, error?: U) => {
				if (settled) return; // first settlement wins
				settled = true;
				this.hub.REGISTRY.delete(replyTo);
				if (timer) clearTimeout(timer);
				if (error !== undefined) rejectFn(error);
				else resolveFn(value as T);
			};
			const promise = new Promise<T>((resolve, reject) => {
				resolveFn = resolve;
				rejectFn = reject;
				if (timeoutMs) {
					timer = setTimeout(() => {
						settle(undefined, new Error(`[rift] wait() timed out after ${timeoutMs}ms`));
					}, timeoutMs);
				}
			});
			this.hub.REGISTRY.set(replyTo, {
				// Register BEFORE queueing — a synchronous reply()inside a queued task still lands correctly.
				resolve: (v: T) => settle(v),
				reject: (e: unknown) => settle(undefined, e),
				timer,
			});
			const reply = (value: T | Promise<T>) => {
				if (value instanceof Promise) {
					value.then(
						(v) => settle(v),
						(e) => settle(undefined, e),
					);
				} else {
					settle(value);
				}
			};
			const tasks = [...handlers].map((handler) => () => {
				// Queue EVERY handler — no early return, no skipped handlers.
				const result = handler(msg, reply);
	
				// Sync return → settle immediately (first wins).
				if (result !== undefined && !(result instanceof Promise)) {
					settle(result as T);
				}
				// Promise return → adopt into settle.
				else if (result instanceof Promise) {
					result.then(
						(v) => settle(v as T),
						(e) => settle(undefined, e),
					);
				}
	
				return result;
			});
			queue({ concurrent: 100 })(tasks).catch(() => {});
			return MaybePromise(promise);
		}
	};
	public marek = new (this.MAREK)(this);
	public duplex = class duplex {
		public r: A = "THE_WIZARD_ROOT_REG";
		public m: A = "SAURON_MASTER_REG";
		public s: A = "SAURON_SLAVE_REG";
		public k: A = "__SAURON__";
		public abus: SITH
		public bbus: SITH
		constructor(
			abus: A, bbus: A, 
			hub: { k: S; s: S; m: S; r: S } = { k: "__SAURON__", s: "SAURON_SLAVE_REG", m: "SAURON_MASTER_REG", r: "THE_WIZARD_ROOT_REG", }) {
			this.r = hub.r;
			this.m = hub.m;
			this.s = hub.s;
			this.k = hub.k;
			this.abus = abus //new MAREK(SITH);
			this.bbus = bbus //new this.MAREK(SITH);
		}
		public stream = (() => {
			const fn = (k?: S, s = this.s, m = this.m, r = this.r) => {
				return {
					send: <T, P>(p: P, sendOpts?: { timeoutMs?: N }) => this.abus.stream( k, s, m, r),
					sub: <P, R>(handler: hdr<P, R>) => this.abus.handler(k, s, m, r),
				}
			};
			return fn;
		})();
		public handler = (() => {
			const fn = (k?: S, s = this.s, m = this.m, r = this.r) => {
				return {
					send: <T, P>(p: P, sendOpts?: { timeoutMs?: N }) => this.abus.stream( k, s, m, r),
					sub: <P, R>(handler: hdr<P, R>) => this.abus.handler(k, s, m, r),
				}
			};
			return fn;
		})();
	};
	public g_bus = new (class G_BUS<Z extends string = string, S extends string = string> {// 3.03B OPS/SEC
		/**
		Several peak runs followed by usage paths
		Peak:                                       1_228.50 M ops/sec
		Peak:                                       3_103.41 M ops/sec
		Peak:                                       2_370.05 M ops/sec
		Peak:                                       2_325.32 M ops/sec
		These runs were all
	
		FELKOR2.send
		sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
		12    |   1146.44      |   1146.44    |   1146.44   |    100.0
		
		FELKOR2.stream
		sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
		12    |   1319.94      |   1319.94    |   1319.94   |    100.0
		PS F:\mq\src> bun run aa.ts
		
		FELKOR2.iterator.publish
		sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
		12    |     540.61     |   540.61     |  540.61     |    100.3
		PS F:\mq\src> bun run aa.ts
		
		FELKOR2.duplex
		sec | M writes/s | best M/s | avg M/s | CPU %
		12 |     898.32 |   898.32 |  898.32 | 100.0
		 */
		public s: S;
		public m: S;
		public r: S;
		public k: S;
		private channels: Record<S, BUS_CHANNEL>;
		constructor(k: A =  "__SAURON__",s: S = "SAURON_SLAVE_REG" as S, m: S = "SAURON_MASTER_REG" as S, r: S = "THE_WIZARD_ROOT_REG" as S) {
			this.s = s;
			this.m = m;
			this.r = r;
			this.k = k;
	
			const root = globalThis as any;
	
			const rL = (root[r] ??= Object.create(null));
			const mL = (rL[m] ??= Object.create(null));
			this.channels = mL[s] ??= Object.create(null);
		}
		// const msg: SIG_BUS_Message1 = { r, m, s, k, p: { p }, ts: Date.now(), _ack: false, };
		// const result = handler(p, msg);
		public send<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
			const channel = this.channels[k];
			const handler = channel?.handler;
			if (!handler) return undefined;
			return handler(p, { r, m, s, k, p: p });
		}
		public sendStream<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
			const channel = this.channels[k];
			const handler = channel?.handler;
			if (!handler) return undefined;
			const msg: SIG_BUS_Message1 = { r, m, s, k, p: p, _ack: false };
			const result = handler(p, msg);
			const listeners = channel.push;
			if (listeners) {
				for (const listener of listeners) listener(msg);
			}
			return result as T | P<T> | UD;
		}
		public sub<P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V {
			const channel = (this.channels[k] ??= {});
			channel.handler = handler;
			return () => {
				if (channel.handler === handler) {
					channel.handler = undefined;
				}
			};
		}
		public stream(handler: (msg: SIG_BUS_Message1) => V, k: S, s: S, m?: S, r?: S): () => V;
		public stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<SIG_BUS_Message1>;
		public stream(handlerOrSlave: ((msg: SIG_BUS_Message1) => V) | S, k: S, s: S = this.s, m: S = this.m, r: S = this.r): (() => V) | AsyncIterableIterator<SIG_BUS_Message1> {
			const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
			const global = globalThis as any;
			const channel = (this.channels[k] ??= {});
	
			if (!channel) throw new Error("Channel does not exist");
	
			const listeners = (channel.push ??= new Set());
	
			if (handler) {
				listeners.add(handler);
	
				return () => {
					listeners.delete(handler);
					if (!listeners.size) channel.push = undefined;
				};
			}
	
			const queue: SIG_BUS_Message1[] = [];
			const waiters: Array<{ resolve: (value: IteratorResult<SIG_BUS_Message1>) => void }> = [];
	
			let offset = 0;
			let closed = false;
	
			const listener = (msg: SIG_BUS_Message1) => {
				if (waiters.length) {
					waiters.shift()!.resolve({ value: msg, done: false });
				} else {
					queue.push(msg);
				}
			};
	
			listeners.add(listener);
	
			const close = () => {
				if (closed) return;
				closed = true;
	
				listeners.delete(listener);
				if (!listeners.size) channel.push = undefined;
	
				for (const waiter of waiters) {
					waiter.resolve({ value: undefined, done: true });
				}
	
				waiters.length = 0;
				queue.length = 0;
				offset = 0;
			};
	
			const iterator: AsyncIterableIterator<SIG_BUS_Message1> = {
				next: (): Promise<IteratorResult<SIG_BUS_Message1>> => {
					if (offset < queue.length) {
						const value = queue[offset++];
	
						if (offset === queue.length) {
							queue.length = 0;
							offset = 0;
						}
	
						return Promise.resolve({ value, done: false });
					}
	
					if (closed) {
						return Promise.resolve({ value: undefined, done: true });
					}
	
					return new Promise((resolve) => waiters.push({ resolve }));
				},
	
				return: async () => {
					close();
					return { value: undefined, done: true };
				},
	
				[Symbol.asyncIterator](): AsyncIterableIterator<SIG_BUS_Message1> {
					return iterator;
				},
			};
	
			return iterator;
		}
		public duplex(k: S, s: S = this.s, m: S = this.m, r: S = this.r) {
			return {
				send: <T = U, P = U>(p: P) => this.sendStream<T>(p, k, s, m, r),
				sub: <P = U, T = U>(handler: BUS_HANDLER<P, T>) => this.sub<P, T>(handler, k, s, m, r),
				stream: (handler?: (msg: SIG_BUS_Message1) => V) => (handler ? this.stream(handler, k, s, m, r) : this.stream(k, s, m, r)),
			};
		}
	})();
    public ro2 = new (class RULE_OF_2 {// 2.03B OPS/SEC
		public r: A = "THE_WIZARD_ROOT_REG";
		public m: A = "SAURON_MASTER_REG";
		public s: A = "SAURON_SLAVE_REG";
		public k: A = "__SAURON__";
 	   public capacity: number;
	   public mask: number;
	   // ── Structure of Arrays (SoA) Event Lanes ──────────────────────────────
	   public pids: Int32Array;
	   public values: Float64Array;
	   public flags: Uint8Array;
	   // ── Cursors ────────────────────────────────────────────────────────────
	   public writeCursor = 0;
	   public readCursor = 0;
	   public root: DEATH_STAR;
	   public hub: SITH;
	   constructor(hub: SITH) {
		this.root = hub.root;
		this.hub = hub;
		this.r = hub.r;
		this.m = hub.m;
		this.s = hub.s;
		this.k = hub.k;
		   if ((hub.capacity & (hub.capacity - 1)) !== 0) { throw new Error("Disruptor capacity must be a power of two."); }
		   this.capacity = hub.capacity;
		   this.mask = hub.capacity - 1;
		   this.pids = new Int32Array(hub.capacity); 
		   this.values = new Float64Array(hub.capacity);
		   this.flags = new Uint8Array(hub.capacity);
	   } // Publishes an event directly into the next ring slot. O(1) time complexity, zero heap allocation, single CPU instruction cycle.
	   public publish(pid: number, value: number, flag = 0): boolean {
		   // Optional backpressure check: Prevent writer from overlapping reader
		   if (this.writeCursor - this.readCursor >= this.capacity) { return false; } // Ring buffer full
		   const index = this.writeCursor & this.mask;
		   this.pids[index] = pid; // Write directly to parallel SoA typed array lanes
		   this.values[index] = value;
		   this.flags[index] = flag;
		   this.writeCursor++;
		   return true;
	   } // Drains and processes the ring buffer in batches using a zero-allocation loop.
	   public handler(handler: (pid: number, value: number, flag: number) => void): number {
		   const targetWrite = this.writeCursor;
		   let currentRead = this.readCursor;
		   if (currentRead === targetWrite) { return 0; } // Nothing to consume
		   let processed = 0;
		   while (currentRead < targetWrite) {
			   const index = currentRead & this.mask; // Execute handler on raw lane data
			   handler(this.pids[index], this.values[index], this.flags[index]);
			   currentRead++;
			   processed++;
		   }
		   this.readCursor = currentRead;
		   return processed;
	   } // Resets cursors and ring lanes instantly without reallocating memory.
	   public reset(): void {
		   this.writeCursor = 0;
		   this.readCursor = 0;
		   this.pids.fill(0);
		   this.values.fill(0);
		   this.flags.fill(0);
	   }
    })(this);
	public drs = new (class DRS {// DISTRIBUTED REACTIVE STATE
		public r: A = "THE_WIZARD_ROOT_REG";
		public m: A = "SAURON_MASTER_REG";
		public s: A = "SAURON_SLAVE_REG";
		public k: A = "__SAURON__";
		public root: DEATH_STAR;
		public hub: SITH;
		constructor(hub: SITH) {
			this.root = hub.root;
			this.hub = hub;
			this.r = hub.r;
			this.m = hub.m;
			this.s = hub.s;
			this.k = hub.k;
		}
		public state_setter(v: U, k: A = `_USECELL`, s: S = this.s, m: SU = this.m, r: SU = this.r) {
			const existing = this.root.bear.state.get(k, { s: s, m: m, r: r });
			if (existing !== undefined) return existing;
			const t = UF(v)
			this.hub.root.bear.state.set(k, t, { triggerRender: false, s: s, m: m, r: r });
			return t;
		}
		public useCell = (() => {
			const fn = (v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r, options: StateOptions<A, any> = {}, __sauron_id__ = this.root.REGISTRY._sid(), ) => {
				return root.state.createHookCell( "useCell", __sauron_id__, () => ({
						value: {
							current: this.state_setter(v, k, s, m, r),
							derived: undefined as A,
						},
					}),
					(state, key, notify) => {
						const st = state.value;
	
						const senderToken = this.root.bear.atom(this.root.REGISTRY._sid());
	
						const equals: (a: any, b: any) => boolean = options.equals ?? ((a, b) => a === b);
						const selector = options.selector ?? null;
						const derive = options.derive ?? null;
	
						const getValue = (): any => (selector ? selector(st.current) : st.current);
	
						const setValue = (next: A) => {
							const prevSelected = getValue();
							const nextSelected = selector ? selector(next) : next;
							if (!equals(prevSelected, nextSelected)) {
								st.current = next;
								if (derive) { st.derived = derive(next); }
								notify(key);
							}
							return next;
						};
	
						this.hub.marek.sub((i: A) => {
							const incoming = i.p;
							if (incoming?.from === senderToken.value) return;
							this.root.bear.state.set(k, incoming.value, { triggerRender: false, s: s, m: m, r: r });
							setValue(incoming.value);
						}, k, s, m, r);
	
						this.root.bear.state.subscribe(k, (newValue: any) => setValue(newValue), { s: s, m: m, r: r });
	
						const setLive = (next: any) => {
							const newValue = typeof next === "function" ? next(this.root.bear.state.get(k, { s: s, m: m, r: r })) : next;
							setValue(newValue);
							this.root.bear.state.set(k, newValue, { triggerRender: true, s: s, m: m, r: r });
							this.hub.marek.send({ value: newValue, from: senderToken.value }, k, s, m, r);
						};
	
						if (derive && st.derived === undefined) { st.derived = derive(st.current); }
						if (derive) { return [getValue(), setLive, st.derived] as const; }
	
						return [getValue(), setLive] as const;
					},
				);
			};
			fn.mailbox = ( v?: A,k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r, options: StateOptions<A, any> = {}, __sauron_id__ = this.root.REGISTRY._sid(),		) => {
				return root.state.createHookCell("useCell.mailbox",__sauron_id__,() => ({
						value: {
							current: this.state_setter(v, k, s, m, r),
							derived: undefined as A,
						},
					}),
					(state, key, notify) => {
						const st = state.value;
	
						const senderToken = this.root.bear.atom(this.root.REGISTRY._sid());
						const queueRef = this.root.bear.atom<Queue | null>(null);
	
						if (!queueRef.current) {
							queueRef.current = new Queue(1);
						}
	
						const equals: (a: any, b: any) => boolean = options.equals ?? ((a, b) => a === b);
						const selector = options.selector ?? null;
						const derive = options.derive ?? null;
	
						const getValue = (): any => (selector ? selector(st.current) : st.current);
	
						const setValue = (next: A) => {
							const prevSelected = getValue();
							const nextSelected = selector ? selector(next) : next;
							if (!equals(prevSelected, nextSelected)) {
								st.current = next;
								if (derive) { st.derived = derive(next); }
								notify(key);
							}
							return next;
						};
	
						this.hub.marek.sub((i: A) => {
							const incoming = i.p;
							if (incoming?.from === senderToken.value) return;
							queueRef.current!.run(async () => {
								const current = this.root.bear.state.get(k, { s: s, m: m, r: r });
								const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
								this.root.bear.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
								return next;
							});
						}, k, s, m, r);
	
						this.root.bear.state.subscribe(k, (newValue: any) => setValue(newValue), { s: s, m: m, r: r });
	
						const setLive = (next: any) => {
							const t = typeof next === "function" ? next() : next;
							this.hub.marek.send({ value: t, from: senderToken.value }, k, s, m, r);
						};
	
						if (derive && st.derived === undefined) { st.derived = derive(st.current); }
						if (derive) { return [getValue(), setLive, st.derived] as const; }
	
						return [getValue(), setLive] as const;
					},
				);
			};
	
			return fn;
		})();
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public useAntiMsgQ = (() => {
			const fn = ( v: A, k: A = `_USECELL`, s: S = this.s, m: S = this.m, r: S = this.r, options: StateOptions<A, any> = {}, __sauron_id__ = this.hub.REGISTRY._sid(),		) => {
				return root.state.createHookCell( "useAntiMsgQ", __sauron_id__, () => ({
						value: {
							current: this.state_setter(v, k, s, m, r),
							derived: undefined as A,
						},
					}),
					(state, key, notify) => {
						const st = state.value;
	
						const senderToken = this.hub.bear.atom(this.hub.REGISTRY._sid());
	
						const equals: (a: any, b: any) => boolean = options.equals ?? ((a, b) => a === b);
						const selector = options.selector ?? null;
						const derive = options.derive ?? null;
	
						const getValue = (): any => (selector ? selector(st.current) : st.current);
	
						const setValue = (next: A) => {
							const prevSelected = getValue();
							const nextSelected = selector ? selector(next) : next;
							if (!equals(prevSelected, nextSelected)) {
								st.current = next;
								if (derive) { st.derived = derive(next); }
								notify(key);
							}
							return next;
						};
	
						this.hub.bear_bus.sub((i: A) => {
							const incoming = i.p;
							if (incoming?.from === senderToken.value) return;
							this.hub.bear.state.set(k, incoming.value, { triggerRender: false, s: s, m: m, r: r });
							setValue(incoming.value);
						}, k, s, m, r);
	
						this.hub.bear.state.subscribe(k, (newValue: any) => setValue(newValue), { s: s, m: m, r: r });
	
						const setLive = (next: any) => {
							const newValue = typeof next === "function" ? next(this.hub.bear.state.get(k, { s: s, m: m, r: r })) : next;
							setValue(newValue);
							this.hub.bear.state.set(k, newValue, { triggerRender: true, s: s, m: m, r: r });
							this.hub.bear_bus.send({ value: newValue, from: senderToken.value }, k, s, m, r);
						};
	
						if (derive && st.derived === undefined) { st.derived = derive(st.current); }
						if (derive) { return [getValue(), setLive, st.derived] as const; }
	
						return [getValue(), setLive] as const;
					},
				);
			};
			fn.mailbox = ( v?: A, k: A = `_USECELL_MAILBOX`, s: S = this.s, m: S = this.m, r: S = this.r, options: StateOptions<A, any> = {}, __sauron_id__ = this.hub.REGISTRY._sid(),		) => {
				return root.state.createHookCell( "useAntiMsgQ.mailbox", __sauron_id__, () => ({
						value: {
							current: this.state_setter(v, k, s, m, r),
							derived: undefined as A,
						},
					}),
					(state, key, notify) => {
						const st = state.value;
	
						const senderToken = this.hub.bear.atom(this.hub.REGISTRY._sid());
						const queueRef = this.hub.bear.atom<Queue | null>(null);
	
						if (!queueRef.current) {
							queueRef.current = new Queue(1);
						}
	
						const equals: (a: any, b: any) => boolean = options.equals ?? ((a, b) => a === b);
						const selector = options.selector ?? null;
						const derive = options.derive ?? null;
	
						const getValue = (): any => (selector ? selector(st.current) : st.current);
	
						const setValue = (next: A) => {
							const prevSelected = getValue();
							const nextSelected = selector ? selector(next) : next;
							if (!equals(prevSelected, nextSelected)) {
								st.current = next;
								if (derive) { st.derived = derive(next); }
								notify(key);
							}
							return next;
						};
	
						this.hub.bear_bus.sub((i: A) => {
							const incoming = i.p;
							if (incoming?.from === senderToken.value) return;
							queueRef.current!.run(async () => {
								const current = this.hub.bear.state.get(k, { s: s, m: m, r: r });
								const next = typeof incoming === "function" ? incoming(current) : (incoming?.value ?? incoming);
								this.hub.bear.state.set(k, next, { triggerRender: false, s: s, m: m, r: r });
								return next;
							});
						}, k, s, m, r);
	
						this.hub.bear.state.subscribe(k, (newValue: any) => setValue(newValue), { s: s, m: m, r: r });
	
						const setLive = (next: any) => {
							const t = typeof next === "function" ? next() : next;
							this.hub.bear_bus.send({ value: t, from: senderToken.value }, k, s, m, r);
						};
	
						if (derive && st.derived === undefined) { st.derived = derive(st.current); }
						if (derive) { return [getValue(), setLive, st.derived] as const; }
	
						return [getValue(), setLive] as const;
					},
				);
			};
	
			return fn;
		})();
		// #endregion
	})(this);
	public spreadsheet = new (class SPREADSHEET_NGIN {
		public r: A = "THE_WIZARD_ROOT_REG";
		public m: A = "SAURON_MASTER_REG";
		public s: A = "SAURON_SLAVE_REG";
		public k: A = "__SAURON__";
		public root: DEATH_STAR;
		public hub: SITH;
		//                                                                                                                  SPREADSHEET
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public spreadsheet = (() => {
			const programs: (Program | undefined)[] = [];
			const program = programs[pid]; 
			if (!program) continue; 
			const vm = new CellVM(sharedF64, sharedPidToSlot);
			hadron.sub(OP.EFFECT, (ring, from, to, epoch, d) => {
				for (let seq = from; seq < to; seq++) {
					const base = (seq & d.mask) * 8;
					const pid = ring[base + 1];
					const program = programs.get(pid);
					if (!program) continue; 

					const result = vm.run(program);
					const slot = Atomics.load(sharedPidToSlot, pid);
					Atomics.store(sharedF64 as A, slot, result);
				}
				Atomics.add(levelCounter, 0, -(to - from)); /
				Atomics.notify(levelCounter, 0);
			});
			class SlotAllocator {
				#free: N[] = [];
				#nextFresh = 0;
				constructor(public readonly capacity: N) {}
			
				alloc(): N {
					if (this.#free.length) return this.#free.pop()!;
					if (this.#nextFresh >= this.capacity) throw new Error("f64 arena exhausted — raise f64Capacity");
					return this.#nextFresh++;
				}
				free(slot: N): V { this.#free.push(slot); }
			}
			const slots = new SlotAllocator(f64Capacity);
			function ensureNumericSlot(hadron: HADRON_DISRUPTOR, pid: N): N {
				const existing = hadron.slot(pid);
				if (existing >= 0) return existing;
				const s = slots.alloc();
				hadron.bind(pid, s);
				return s;
			}
			function releaseNumericSlot(hadron: HADRON_DISRUPTOR, pid: N): V {
				const s = hadron.slot(pid);
				if (s < 0) return;
				hadron.unbind(pid);
				slots.free(s);
			}
			function syncArena(hadron: HADRON_DISRUPTOR, f64: SharedF64, pid: N, raw: A, isFormula: B) {
				if (isFormula) {
					ensureNumericSlot(hadron, pid); // reserve the slot now; a worker fills it in once scheduled
				} else if (typeof raw === "number") {
					f64.store(ensureNumericSlot(hadron, pid), raw);
				} else {
					releaseNumericSlot(hadron, pid); // text, undefined, cleared — doesn't belong in the arena
				}
			}
			const fn = (sheet: S, cell: S, s: S = this.j.s, m: S = this.j.m, r: S = this.j.r, chain?: "chain" | "rechain") => MCR_Combo(this.j.DRS.cell(this.spreadsheet.envKey(sheet, cell), s, m, r)?.computed ?? 0, chain);
			fn.raw = (sheet: S, cell: S, s: S = this.j.s, m: S = this.j.m, r: S = this.j.r) => this.j.DRS.cell(this.spreadsheet.envKey(sheet, cell), s, m, r)?.raw;
			fn.range = (sheet: S, ref: S, s: S = this.j.s, m: S = this.j.m, r: S = this.j.r) => this.spreadsheet.rangeKeys(ref, sheet).map(row => row.map(k => this.j.DRS.cell(this.spreadsheet.envKeyOf(k), s, m, r)?.computed ?? 0));
			// HELPERS
			fn.HISTORY_CAP = 200;
			fn.envKey = (sheet: S, cell: S) => `${sheet}::${normalizeCellRef(cell)}`;
			fn.rangeKeys = (ref: S, sheet?: S): S[][] => {
				const [a, b = a] = ref.split(":"); // "A1:B3" -> "A1", "B3" (single cell works too)
				const s = parseCellRef(a, sheet),
					e = parseCellRef(b, sheet);
				const [c1, c2] = [colToIndex(s.col), colToIndex(e.col)].sort((x, y) => x - y);
				const [r1, r2] = [s.row, e.row].sort((x, y) => x - y);
				const rows: S[][] = [];
				for (let r = r1; r <= r2; r++) {
					const row: S[] = [];
					for (let c = c1; c <= c2; c++) row.push(cellKey(indexToCol(c), r, s.sheet));
					rows.push(row);
				}
				return rows;
			};
			// "sheet!B3" (graph key) -> "sheet::B3" (the state key where the envelope lives)
			fn.envKeyOf = (graphKey: S) => {
				const {sheet, col, row} = parseCellRef(graphKey);
				return `${sheet}::${col}${row}`;
			};
			// state key for "who reads this cell": "sheet::B3#dependents"
			fn.dk = (graphKey: S) => `${this.spreadsheet.envKeyOf(graphKey)}#dependents`;
			fn.vk = (sheet: S) => `${sheet}::#volatile`;
			fn.rk = (sheet: S) => `${sheet}::#ranges`; // [{ t: "sheet!B3", box }] range formulas reading this sheet
			fn.xk = (sheet: S) => `${sheet}::#extent`; // { r: lastUsedRow }
			// get() with the MaybePromise quirk removed: a missing key returns undefined, not a Promise
			fn.peek = (k: A, s: S, m: S, r: S) => {
				const v = get(k, s, m, r);
				return v instanceof Promise ? undefined : v;
			};
			// what a cell currently shows: computed if it's a formula, else its raw value
			fn.visible = (g: SpreadsheetGraphState, key: A) => g.computed.get(key) ?? g.cells.get(key)?.raw;
			// prepend "=" if missing
			fn.spreadsheetFormula = (expression: string) => (expression.startsWith("=") ? expression : `=${expression}`);
			fn.load = (g: SpreadsheetGraphState, seen: ST<S>, key: A, s: S, m: S, r: S) => {
				if (seen.has(key)) return false;
				seen.add(key);
				const env = this.spreadsheet.peek(this.spreadsheet.envKeyOf(key), s, m, r);
				if (env && env.raw !== undefined) {
					const cell: SpreadsheetCell = {raw: env.raw};
					if (isFormula(env.raw)) {
						try {
							cell.ast = qualifyAst(parseFormula(env.raw), parseCellRef(key).sheet);
							cell.volatile = isVolatileAst(cell.ast);
						} catch {}
						cell.computed = env.computed;
						g.computed.set(key, env.computed);
					}
					g.cells.set(key, cell);
					if (env.deps?.length) g.deps.set(key, new Set(env.deps));
					if (env.ranges?.length) g.ranges.set(key, env.ranges);
				}
				const ds = this.spreadsheet.peek(this.spreadsheet.dk(key), s, m, r);
				if (ds?.length) g.dependents.set(key, new Set(ds));
				return true;
			};
			fn.syncRangeIndex = (that: A, key: A, oldBoxes: RangeBox[], newBoxes: RangeBox[], s: S, m: S, r: S) => {
				for (const sh of new Set([...oldBoxes, ...newBoxes].map(b => b.sheet ?? ""))) {
					const idx: RangeEntry[] = (this.spreadsheet.peek(this.spreadsheet.rk(sh), s, m, r) ?? []).filter((e: RangeEntry) => e.t !== key);
					for (const box of newBoxes) if ((box.sheet ?? "") === sh) idx.push({t: key, box});
					that.writeCell(this.spreadsheet.rk(sh), idx, s, m, r);
				}
			};
			fn.loadSlice = (g: SpreadsheetGraphState, seen: ST<S>, roots: S[], newDeps: I<S>, s: S, m: S, r: S) => {
				const idx = new Map<S, RangeEntry[]>();
				const rangeDependents = (key: S): S[] => {
					const sheet = parseCellRef(key).sheet ?? "";
					if (!idx.has(sheet)) idx.set(sheet, this.spreadsheet.peek(this.spreadsheet.rk(sheet), s, m, r) ?? []);
					return idx
						.get(sheet)!
						.filter(e => boxContains(e.box, key))
						.map(e => e.t);
				};
	
				for (const r of roots) this.spreadsheet.load(g, seen, r, s, m, r);
				for (const r of roots) for (const d of g.deps.get(r) ?? []) this.spreadsheet.load(g, seen, d, s, m, r); // old inputs
				for (const d of newDeps) this.spreadsheet.load(g, seen, d, s, m, r); // new inputs
	
				const queued = new Set(roots); // separate from `seen`: a cell loaded as an input must still be queued as a dependent
				const queue = [...roots];
				for (let i = 0; i < queue.length; i++) {
					const cur = queue[i];
					for (const dep of new Set([...(g.dependents.get(cur) ?? []), ...rangeDependents(cur)])) {
						this.spreadsheet.load(g, seen, dep, s, m, r);
						if (queued.has(dep)) continue;
						queued.add(dep);
						queue.push(dep);
						for (const input of g.deps.get(dep) ?? []) this.spreadsheet.load(g, seen, input, s, m, r);
					}
				}
			};
			fn.tick = (that: A, sheet: S, s: S, m: S, r: S) => {
				const vols: S[] = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
				if (!vols.length) return;
				const {g, seen} = this.spreadsheet.openGraph(s, m, r);
				this.spreadsheet.loadSlice(g, seen, vols, [], s, m, r);
				this.spreadsheet.persist(that, g, sheet, recalculateDirty(g, vols, []), [], false, s, m, r);
			};
			fn.commit = (that: A, sheet: S, ref: S, raw: A, s: S, m: S, r: S) => {
				const key = normalizeCellRef(ref, sheet);
				let newDeps = new Set<S>();
				if (isFormula(raw)) {
					try {
						newDeps = analyzeDependencies(qualifyAst(parseFormula(raw), sheet)).cells;
					} catch {}
				}
	
				const vols: S[] = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
				const {g, seen} = this.spreadsheet.openGraph(s, m, r);
				this.spreadsheet.loadSlice(g, seen, [key, ...vols], newDeps, s, m, r);
	
				const oldDeps = new Set(g.deps.get(key) ?? []);
				const oldBoxes = g.ranges.get(key) ?? [];
				const changed = raw === undefined ? clearGraphCellInPlace(g, ref, sheet, vols) : writeGraphCellInPlace(g, ref, raw, sheet, vols);
				const newBoxes = g.ranges.get(key) ?? [];
	
				this.spreadsheet.persist(that, g, sheet, changed, new Set([...oldDeps, ...(g.deps.get(key) ?? [])]), true, s, m, r);
				if (oldBoxes.length || newBoxes.length) this.spreadsheet.syncRangeIndex(that, key, oldBoxes, newBoxes, s, m, r);
			};
			fn.persist = (that: A, g: SpreadsheetGraphState, sheet: S, changed: ST<S>, depKeys: I<S>, history: B, s: S, m: S, r: S) => {
				const rows = new Map<S, N>();
				for (const k of changed) {
					const cell = g.cells.get(k);
					const env = {raw: cell?.raw, computed: this.spreadsheet.visible(g, k) ?? 0, deps: [...(g.deps.get(k) ?? [])], ranges: g.ranges.get(k) ?? [], volatile: !!cell?.volatile, t: Date.now()};
					that.writeCell(this.spreadsheet.envKeyOf(k), env, s, m, r);
					if (history && !env.volatile) {
						const h = this.spreadsheet.peek(`${this.spreadsheet.envKeyOf(k)}#h`, s, m, r) ?? [];
						that.writeCell(`${this.spreadsheet.envKeyOf(k)}#h`, [...h, {raw: env.raw, computed: env.computed, t: env.t}].slice(-this.spreadsheet.HISTORY_CAP), s, m, r);
					}
					if (cell) {
						const p = parseCellRef(k),
							sh = p.sheet ?? "";
						rows.set(sh, Math.max(rows.get(sh) ?? 0, p.row));
					}
				}
				for (const d of depKeys) that.writeCell(this.spreadsheet.dk(d), [...(g.dependents.get(d) ?? [])], s, m, r);
				const rt = r
				for (const [sh, r] of rows) {                                       // used-row extent per sheet (grow only)
					if (r > (this.spreadsheet.peek(this.spreadsheet.xk(sh), s, m, rt)?.r ?? 0)) that.writeCell(this.spreadsheet.xk(sh), { r }, s, m, rt)
				}
	
				const before: S[] = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
				const vols = new Set(before);
				for (const k of changed) g.cells.get(k)?.volatile ? vols.add(k) : vols.delete(k);
				if (vols.size !== before.length || before.some(k => !vols.has(k))) that.writeCell(this.spreadsheet.vk(sheet), [...vols], s, m, r);
			};
			fn.openGraph = (s: S, m: S, r: S) => {
				const g = createSpreadsheetGraph(),
					seen = new Set<S>();
				g.fetch = k => {
					this.spreadsheet.load(g, seen, k, s, m, r);
					return g.cells.get(k);
				}; // INDIRECT / OFFSET / range members
				g.extent = sheet => this.spreadsheet.peek(this.spreadsheet.xk(sheet ?? ""), s, m, r)?.r ?? 0;
				return {g, seen};
			};
			return fn;
		})();
		// #endregion
		//                                                                                                              USE SPREADSHEET
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public useSpreadsheet = (() => {
			const commit = (that: A, sheet: S, ref: S, raw: A, s: S, m: S, r: S) => {
				const key = normalizeCellRef(ref, sheet);
	
				let newDeps = new Set<string>();
				if (isFormula(raw)) {try {newDeps = collectDependencies(qualifyAst(parseFormula(raw), sheet));} catch {}}
				const vols: string[] = this.spreadsheet.peek(this.spreadsheet.vk(sheet), s, m, r) ?? [];
	
				const {g, seen} = this.spreadsheet.openGraph(s, m, r);
				this.spreadsheet.loadSlice(g, seen, [key, ...vols], newDeps, s, m, r);
	
				const oldDeps = new Set(g.deps.get(key) ?? []);
				const changed = raw === undefined ? clearGraphCellInPlace(g, ref, sheet) : writeGraphCellInPlace(g, ref, raw, sheet);
	
				for (const k of changed) {
					const env = {raw: g.cells.get(k)?.raw, computed: this.spreadsheet.visible(g, k) ?? 0, deps: [...(g.deps.get(k) ?? [])], t: Date.now()};
					that.writeCell(this.spreadsheet.envKeyOf(k), env, s, m, r);
					const h = this.spreadsheet.peek(`${this.spreadsheet.envKeyOf(k)}#h`, s, m, r) ?? [];
					that.writeCell(`${this.spreadsheet.envKeyOf(k)}#h`, [...h, {raw: env.raw, computed: env.computed, t: env.t}].slice(-this.spreadsheet.HISTORY_CAP), s, m, r);

					const pid = this.pids.intern(k);
					if (typeof env.computed === "number" && Number.isFinite(env.computed)) {this.storeComputed(this.ensureNumericSlot(pid), env.computed);} 
					else {this.releaseNumericSlot(pid);}

					if (changed.size > PARALLEL_THRESHOLD) {                 // tune by profiling, not guessing
						const levels = recalculateDirty.levels(g, [...changed], []);
						this.hub.recalcParallel(levels.map(level => level.map(k => this.hub.pids.intern(k))));
					}
				}

				
	
				for (const d of new Set([...oldDeps, ...(g.deps.get(key) ?? [])])) {that.writeCell(this.spreadsheet.dk(d), [...(g.dependents.get(d) ?? [])], s, m, r);	}
			};
			const fn = (sheet: S, cell: S, v?: A, s: S = this.j.s, m: S = this.j.m, r: S = this.j.r, type: CELL = "none", chain?: CHAIN) => {
				const seed =
					v === undefined ? undefined : (
						() => {
							commit(this, sheet, cell, typeof v === "function" ? v() : v, s, m, r);
							return this.spreadsheet.peek(this.spreadsheet.envKey(sheet, cell), s, m, r);
						}
					);
				const [env] = this.j.DRS.useCell(this.spreadsheet.envKey(sheet, cell), seed, s, m, r, type);
	
				const read = (ref: S) => this.spreadsheet.peek(this.spreadsheet.envKey(sheet, ref), s, m, r);
				const write = (ref: S, raw: A) => commit(this, sheet, ref, raw, s, m, r);
	
				// getter: just the computed value
				const getter = env?.computed ?? 0;
	
				// setter: just a setter (function form gets the CURRENT raw, not a stale closure)
				const setter = MCR_Combo((next: A) => write(cell, typeof next === "function" ? next(read(cell)?.raw) : next), chain);
	
				return [getter, setter] as const;
			};
			fn.grid = (sheet: S, cell: S, v?: A, s: S = this.j.s, m: S = this.j.m, r: S = this.j.r, type: CELL = "none", chain?: CHAIN) => {
				const seed =
					v === undefined ? undefined : (
						() => {
							commit(this, sheet, cell, typeof v === "function" ? v() : v, s, m, r);
							return this.spreadsheet.peek(this.spreadsheet.envKey(sheet, cell), s, m, r);
						}
					);
				const [env] = this.j.DRS.useCell(this.spreadsheet.envKey(sheet, cell), seed, s, m, r, type);
	
				const read = (ref: S) => this.spreadsheet.peek(this.spreadsheet.envKey(sheet, ref), s, m, r);
				const write = (ref: S, raw: A) => commit(this, sheet, ref, raw, s, m, r);
	
				// getter: just the computed value
				const getter = env?.computed ?? 0;
	
				// setter: just a setter (function form gets the CURRENT raw, not a stale closure)
				const setter = MCR_Combo((next: A) => write(cell, typeof next === "function" ? next(read(cell)?.raw) : next), chain);
				const rt = r
				// opts: the bag of tricks
				const opts = {
					sheet,
					cell,
					envelope: env, // { raw, computed, deps, t }
					raw: env?.raw, // this cell's raw: "=SUM(B1:B2)" or literal
					deps: env?.deps ?? [],
					dependents: this.spreadsheet.peek(this.spreadsheet.dk(normalizeCellRef(cell, sheet)), s, m, r) ?? [],
	
					get: (ref: S) => read(ref)?.computed, // any cell, computed
					getRaw: (ref: S) => read(ref)?.raw,
					set: write, // opts.set("B1", 500)
					formula: (ref: S, expr: S) => write(ref, this.spreadsheet.spreadsheetFormula(expr)),
					clear: (ref: S) => write(ref, undefined),
					setMany: (edits: Iterable<readonly [S, A]>) => {
						for (const [r, raw] of edits) write(r, raw);
					},
					fill: (anchor: S, block: A[][]) => {
						const a = parseCellRef(anchor, sheet);
						block.forEach((row, ri) => row.forEach((raw, ci) => write(`${indexToCol(colToIndex(a.col) + ci)}${a.row + ri}`, raw)));
					},
					range: (ref: string) => this.spreadsheet.rangeKeys(ref, sheet).map(r => r.map(k => this.spreadsheet.peek(this.spreadsheet.envKeyOf(k), s, m, rt)?.computed ?? 0)),
					rawRange: (ref: string) => this.spreadsheet.rangeKeys(ref, sheet).map(r => r.map(k => this.spreadsheet.peek(this.spreadsheet.envKeyOf(k), s, m, rt)?.raw)),
					history: (ref: S = cell) => this.spreadsheet.peek(`${this.spreadsheet.envKey(sheet, ref)}#h`, s, m, r) ?? [],
				};
	
				return [getter, setter, opts] as const;
			};
			return fn;
		})();
		// #endregion 
		//                                                                                                                  [CONSTANTS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public compile(ast: FormulaAst): Program {// main thread — walks the parsed AST once, emits flat bytecode. Runs on formula edit only.
			const code: N[] = [];
			const consts: N[] = [];
			const emit = (node: FormulaAst) => {
				if (node.type === "num") { code.push(VM_OP.PUSH_CONST, consts.length); consts.push(node.value); }
				else if (node.type === "ref") { code.push(VM_OP.PUSH_CELL, pids.intern(node.key)); }
				else if (node.type === "bin") {
					emit(node.left); emit(node.right);
					code.push({ "+": VM_OP.ADD, "-": VM_OP.SUB, "*": VM_OP.MUL, "/": VM_OP.DIV }[node.op], 0);
				}
				// SUM_RANGE etc. compiled similarly from range nodes
			};
			emit(ast);
			code.push(VM_OP.RET, 0);
			return { code: Int32Array.from(code), consts: Float64Array.from(consts) };
		}
		private capacity: N;
		private mask: N;
		private pidCapacity: N;
		private schedule: readonly N[];
		private shared: B;
		private rings: I32[] = [];
		private CTRL = 16;
		private ctrl: I32;
		private available: I32[] = [];
		private clock: I32;
		private phaseIndex: I32;
		private running: I32;
		private pidToSlot: I32;
		private dirty: U32;
		private f64: F64;
		private u32: U32;
		private i32: I32;
		private bytes: U8;
		private f64Tail = 0;
		private u32Tail = 0;
		private i32Tail = 0;
		private byteTail = 0;
		private maxBatch: N;
		private wait: WaitStrategy;
		private handlers: AR< | ((ring: I32, from: N, to: N, epoch: N, d: this ) => V) | NU > = [];
		constructor(SITH: SITH) {
			this.hub = SITH;
			this.root = SITH.root;
			this.r = SITH.r;
			this.m = SITH.m;
			this.s = SITH.s;
			this.k = SITH.k;
			const capacity = SITH.capacity ?? 1 << 18;
		  	const pidCapacity = SITH.pidCapacity ?? 1 << 20;
			if (!pow2(capacity)) throw new Error("capacity must be a power of two");
			if (!pow2(pidCapacity)) throw new Error("pidCapacity must be a power of two");
			this.capacity = capacity;
			this.mask = capacity - 1;
			this.pidCapacity = pidCapacity;
			this.schedule = SITH.schedule ?? DEFAULT_SCHEDULE;
			this.shared = SITH.shared ?? typeof SharedArrayBuffer !== "undefined";
			this.maxBatch = SITH.maxBatch ?? 8192;
			this.wait = SITH.wait ?? "yield";
			const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;
			const laneCount = this.schedule.length;
			this.rings = Array.from( { length: laneCount }, () => new Int32Array(new AB(capacity * REC * 4)) );
			this.ctrl = new Int32Array(new AB(laneCount * this.CTRL * 4));
			// Availability buffer uses one Int32 per ring slot.
			this.available = Array.from( { length: laneCount }, () => new Int32Array(new AB(capacity * 4)) );
			this.clock = new Int32Array(new AB(4));
			this.phaseIndex = new Int32Array(new AB(4));
			this.running = new Int32Array(new AB(4));
			this.pidToSlot = new Int32Array(new AB(pidCapacity * 4));
			this.pidToSlot.fill(-1);
			this.dirty = new Uint32Array(new AB((pidCapacity >>> 5) * 4));
			this.f64 = new Float64Array(new AB((SITH.f64Capacity ?? 1 << 18) * 8));
			this.u32 = new Uint32Array(new AB((SITH.u32Capacity ?? 1 << 18) * 4));
			this.i32 = new Int32Array(new AB((SITH.i32Capacity ?? 1 << 18) * 4));
			this.bytes = new Uint8Array(new AB(SITH.byteCapacity ?? 1 << 20));

			this.f64Bits = new BigInt64Array(this.f64.buffer);
			this.f64 = new Float64Array(new AB((SITH.f64Capacity ?? 1 << 18) * 8));
			this.f64Bits = new BigInt64Array(this.f64.buffer);
		}
		// #endregion
		//                                                                                                                   [HANDLERS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public sub( op: OpKind, handler: ( ring: I32, from: N, to: N, epoch: N, d: this ) => V  ): this {
		  this.handlers[op] = handler;
		  return this;
		}
	  /*======================================================================
		 MULTI-PRODUCER CLAIM / PUBLISH
		 Claim the next sequence for a lane. This is a true multi-producer sequencer: - CAS reserves a unique sequence. - The availability buffer marks that exact sequence published. - Consumers never rely on a single global “highest published” value.
		====================================================================== */
		private ctrlAt(lane: N, field: number): number { return lane * this.CTRL + field;  }
		public claim(lane: number): number {
		  const next = this.ctrlAt(lane, 0);
		  const stall = this.ctrlAt(lane, 4);
		  const claimed = this.ctrlAt(lane, 5);
	  
		  let spin = 0;
	  
		  for (;;) {
			const seq = Atomics.load(this.ctrl, next);
			const wrap = seq - this.capacity;
			const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
	  
			if (gate > wrap) {
			  if (Atomics.compareExchange(this.ctrl, next, seq, seq + 1) === seq) {
				Atomics.add(this.ctrl, claimed, 1);
				return seq;
			  }
			  continue;
			}
	  
			Atomics.add(this.ctrl, stall, 1);
	  
			if (this.wait === "spin") {
			  spin++;
			  continue;
			}
	  
			if (this.wait === "yield") {
			  this.yieldNow();
			  continue;
			}
	  
			Atomics.wait(this.ctrl, next, seq, 1);
			spin = 0;
		  }
		}
		// Publish a claimed record. The record must be fully written BEFORE this call. The availability buffer is the publication barrier.
		public publish(lane: number, seq: number): void {
		  const avail = this.available[lane];
		  Atomics.store(avail, seq & this.mask, seq + 1);
	  
		  const pub = this.ctrlAt(lane, 6);
		  Atomics.add(this.ctrl, pub, 1);
	  
		  const waiting = this.ctrlAt(lane, 0);
		  Atomics.notify(this.ctrl, waiting);
		}
		// One-shot enqueue. Returns: >= 0 — published sequence  -1  — nonblocking mode and lane was full
		public emit( op: OpKind, pid: N, slot: N, a = 0, b = 0, c = 0, ext = 0, block = true  ): N {
		  const lane = this.laneFor(op);
		  const ring = this.rings[lane];
	  
		  if (!block && this.isFull(lane)) return -1;
	  
		  const seq = this.claim(lane);
		  const base = (seq & this.mask) * REC;
		  const epoch = Atomics.load(this.clock, 0);
	  
		  ring[base + F_OP] = op;
		  ring[base + F_PID] = pid;
		  ring[base + F_SLOT] = slot;
		  ring[base + F_A] = a;
		  ring[base + F_B] = b;
		  ring[base + F_C] = c;
		  ring[base + F_EPOCH] = epoch;
		  ring[base + F_EXT] = ext;
	  
		  this.publish(lane, seq);
		  return seq;
		}
		// #endregion
		//                                                                                                         [PID / SLOT / DIRTY] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public bind(pid: N, slot: N): V { Atomics.store(this.pidToSlot, pid, slot); }
		public unbind(pid: N): V { Atomics.store(this.pidToSlot, pid, -1); }
		public slot(pid: N): N { return Atomics.load(this.pidToSlot, pid); }
		public dirtyMark(pid: N): V { this.dirty[pid >>> 5] |= 1 << (pid & 31);  }
		public dirtyPeek(pid: N): B { return (this.dirty[pid >>> 5] & (1 << (pid & 31))) !== 0;  }
		public dirtyTake(pid: N): B {
		  const w = pid >>> 5;
		  const bit = 1 << (pid & 31);
		  const word = this.dirty[w];
	  
		  if ((word & bit) === 0) return false;
	  
		  this.dirty[w] = word & ~bit;
		  return true;
		}
		private pids = new PidTable();           
		public ensureNumericSlot(pid: N): N {
			const existing = this.slot(pid);
			if (existing >= 0) return existing;
			const s = this.allocF64Slot();
			this.bind(pid, s);
			return s;
		}
		public releaseNumericSlot(pid: N): V {
			const s = this.slot(pid);
			if (s < 0) return;
			this.unbind(pid);
			this.freeF64Slot(s);
		}
		// #endregion
		//                                                                                                                 [SOA ARENAS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private f64Free: N[] = [];
		private f64Bits: BigInt64Array;          // init in constructor, right after this.f64 is created:
		private scratchF = new Float64Array(1);
		private scratchI = new BigInt64Array(this.scratchF.buffer);
		public allocF64Slot(): N {//                           reuse a freed slot before growing the tail
			if (this.f64Free.length) return this.f64Free.pop()!;
			const at = this.f64Tail++;
			if (at >= this.f64.length) throw new Error("f64 arena exhausted — raise f64Capacity");
			return at;
		}
		public freeF64Slot(at: N): V { this.f64Free.push(at); }
		public storeComputed(slot: N, value: N): V { this.f64[slot] = value; }
		public loadComputed(slot: N): N { return this.f64[slot]; }
		public allocF64(value: N): N {
		  const at = this.f64Tail++;
		  this.f64[at] = value;
		  return at;
		}
		public allocU32(value: N): N {
		  const at = this.u32Tail++;
		  this.u32[at] = value;
		  return at;
		}
		public allocI32(value: N): N {
		  const at = this.i32Tail++;
		  this.i32[at] = value;
		  return at;
		}
		public allocBytes(data: U8): N {
		  const at = this.byteTail;
		  this.bytes.set(data, at);
		  this.byteTail += data.length;
		  return at;
		}
		public f64At(offset: N): N {    return this.f64[offset];  }
		public u32At(offset: N): N {    return this.u32[offset];  }
		public i32At(offset: N): N {    return this.i32[offset];  }
		public bytesAt(offset: N, length: N): U8 {    return this.bytes.subarray(offset, offset + length);  }
		public exportBuffers() {
			return {
				rings: this.rings.map(r => r.buffer), ctrl: this.ctrl.buffer,
				available: this.available.map(a => a.buffer), clock: this.clock.buffer,
				phaseIndex: this.phaseIndex.buffer, running: this.running.buffer,
				pidToSlot: this.pidToSlot.buffer, dirty: this.dirty.buffer,
				f64: this.f64.buffer, u32: this.u32.buffer, i32: this.i32.buffer, bytes: this.bytes.buffer,
				capacity: this.capacity, pidCapacity: this.pidCapacity, schedule: this.schedule, maxBatch: this.maxBatch,
			};
		}
		// static, not instance — a worker has no "self" yet, it's building one from scratch around shared memory
		static fromBuffers(buf: ReturnType<InstanceType<typeof THIS_CLASS>["exportBuffers"]>): THIS_CLASS_INSTANCE {
			const self = Object.create(THIS_CLASS.prototype); //        bypass the allocating constructor entirely
			self.capacity = buf.capacity; self.mask = buf.capacity - 1; self.pidCapacity = buf.pidCapacity;
			self.schedule = buf.schedule; self.maxBatch = buf.maxBatch; self.wait = "yield"; self.shared = true;
			self.rings = buf.rings.map((b: SharedArrayBuffer) => new Int32Array(b));
			self.ctrl = new Int32Array(buf.ctrl);
			self.available = buf.available.map((b: SharedArrayBuffer) => new Int32Array(b));
			self.clock = new Int32Array(buf.clock); self.phaseIndex = new Int32Array(buf.phaseIndex);
			self.running = new Int32Array(buf.running); self.pidToSlot = new Int32Array(buf.pidToSlot);
			self.dirty = new Uint32Array(buf.dirty);
			self.f64 = new Float64Array(buf.f64); self.f64Bits = new BigInt64Array(buf.f64);
			self.u32 = new Uint32Array(buf.u32); self.i32 = new Int32Array(buf.i32); self.bytes = new Uint8Array(buf.bytes);
			self.f64Tail = 0; self.f64Free = []; self.scratchF = new Float64Array(1);
			self.scratchI = new BigInt64Array(self.scratchF.buffer);
			self.handlers = [];
			return self;
		}
		public exportArena() {
			return { f64: this.f64.buffer, pidToSlot: this.pidToSlot.buffer, levelCounter: this.levelCounter.buffer };
		}
		// #endregion
		//                                                                                                           [CONSUMER BARRIER] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	  /*======================================================================
		 Return the highest contiguous available sequence for a lane. This scans only the availability window, then caches the result. It avoids the classic problem of a slow producer blocking visibility of later producers.
		====================================================================== */
		private availableThrough(lane: number): number {
		  const ctrlBase = this.ctrlAt(lane, 0);
		  const gateField = this.ctrlAt(lane, 1);
		  const cacheField = this.ctrlAt(lane, 2);
	  
		  const next = Atomics.load(this.ctrl, ctrlBase);
		  let gate = Atomics.load(this.ctrl, gateField);
		  let cached = Atomics.load(this.ctrl, cacheField);
	  
		  if (cached >= gate) {
			const avail = this.available[lane];
			const cap = this.capacity;
	  
			let scan = cached;
			const limit = Math.min(next, cached + this.maxBatch);
	  
			while (scan < limit) {
			  const flag = Atomics.load(avail, scan & (cap - 1));
			  if (flag <= scan) break;
			  scan++;
			}
	  
			cached = scan;
			Atomics.store(this.ctrl, cacheField, cached);
		  }
	  
		  return cached;
		}
		// #endregion
		//                                                                                                     [PHASE-LOCKED EXECUTION] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		/*======================================================================
		   Drain exactly one scheduled phase. A phase is homogeneous: READ only, WRITE only, DELETE only, etc.
		   ====================================================================== */
	    public drainPhase(limit = this.maxBatch): number {
		  const schedule = this.schedule;
		  const lane = schedule[Atomics.load(this.phaseIndex, 0)];
		  const handler = this.handlers[lane];
	  
		  if (!handler) {
			this.nextPhase();
			return 0;
		  }
	  
		  const ring = this.rings[lane];
		  const gateField = this.ctrlAt(lane, 1);
	  
		  const available = this.availableThrough(lane);
		  const head = Atomics.load(this.ctrl, gateField);
	  
		  const end = Math.min(available, head + limit);
	  
		  if (end <= head) {
			this.nextPhase();
			return 0;
		  }
	  
		  const epoch = Atomics.load(this.clock, 0);
	  
		  // Batch boundary is handed to the handler.
		  // The handler can specialize contiguous execution.
		  handler(ring, head, end, epoch, this);
	  
		  Atomics.store(this.ctrl, gateField, end);
	  
		  const consumedField = this.ctrlAt(lane, 7);
		  const batchField = this.ctrlAt(lane, 8);
		  const maxBatchField = this.ctrlAt(lane, 9);
	  
		  const count = end - head;
	  
		  Atomics.add(this.ctrl, consumedField, count);
		  Atomics.add(this.ctrl, batchField, 1);
	  
		  const prevMax = Atomics.load(this.ctrl, maxBatchField);
		  if (count > prevMax) Atomics.store(this.ctrl, maxBatchField, count);
	  
		  this.nextPhase();
		  return count;
		}// Run one full revolution. The tracer particle advances only after every scheduled phase runs.
		public revolution(limit = this.maxBatch): number {
		  let total = 0;
	  
		  for (let i = 0; i < this.schedule.length; i++) {
			total += this.drainPhase(limit);
		  }
	  
		  Atomics.add(this.clock, 0, 1);
		  return total;
		}// Run until all lanes are drained. Useful for shutdown, transaction boundaries, and tests.
		public drainAll(limit = this.maxBatch): number {
		  let total = 0;
	  
		  for (;;) {
			const before = total;
			total += this.revolution(limit);
			if (total === before) break;
		  }
	  
		  return total;
		}
		private levelCounter = new Int32Array(new SharedArrayBuffer(4));
		private workers: { post: (msg: A, transfer?: A[]) => V }[] = [];
		ownerOf(pid: N): N { return pid % this.workers.length; }
		registerWorker(post: (msg: A, transfer?: A[]) => V): V { this.workers.push({ post }); }
		sendProgram(pid: N, program: Program): V {
			this.workers[this.ownerOf(pid)].post({ type: "register", pid, program });
		}
		dispatchLevel(readyPids: N[]): V {
			const byWorker = new Map<N, N[]>(); // cold path, small N — plain Map is fine here
			for (const pid of readyPids) {
				const w = this.ownerOf(pid);
				(byWorker.get(w) ?? byWorker.set(w, []).get(w)!).push(pid);
			}
			Atomics.store(this.levelCounter, 0, readyPids.length);
			for (const [w, pids] of byWorker) {
				const buf = Int32Array.from(pids);
				this.workers[w].post({ type: "computeLevel", pids: buf }, [buf.buffer]); // zero-copy transfer
			}
		}
		recalcParallel(levels: N[][]): V {
			for (const level of levels) {
				this.dispatchLevel(level);
				while (Atomics.load(this.levelCounter, 0) > 0) {
					Atomics.wait(this.levelCounter, 0, Atomics.load(this.levelCounter, 0));
				}
			}
		}
		// #endregion
		//                                                                                                            [CLOCK / CONTROL] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public tick(): number { return Atomics.load(this.clock, 0);  }
		public currentPhase(): OpKind { return this.schedule[Atomics.load(this.phaseIndex, 0)] as OpKind;  }
		public start(): void {    Atomics.store(this.running, 0, 1);  }
		public stop(): void {
		  Atomics.store(this.running, 0, 0);
		  Atomics.notify(this.running, 0);
		}
		public isRunning(): boolean {    return Atomics.load(this.running, 0) === 1;  }
		// #endregion
		//                                                                                                                      [STATS] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		public stats(lane: number): LANE_STATS {
		  const b = lane * this.CTRL;
	  
		  return {
			claimed: Atomics.load(this.ctrl, b + 5),
			published: Atomics.load(this.ctrl, b + 6),
			consumed: Atomics.load(this.ctrl, b + 7),
			batches: Atomics.load(this.ctrl, b + 8),
			maxBatch: Atomics.load(this.ctrl, b + 9),
			fullStalls: Atomics.load(this.ctrl, b + 4),
		  };
		}
		public pending(lane: number): number {
		  const published = Atomics.load(this.ctrl, this.ctrlAt(lane, 6));
		  const consumed = Atomics.load(this.ctrl, this.ctrlAt(lane, 7));
		  return published - consumed;
		}
		// #endregion
		//                                                                                                                   [INTERNAL] 
		// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
		private laneFor(op: OpKind): number {    return op;  }
		private isFull(lane: number): boolean {
		  const next = Atomics.load(this.ctrl, this.ctrlAt(lane, 0));
		  const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
		  return next - gate >= this.capacity;
		}
		private nextPhase(): void {
		  const next = (Atomics.load(this.phaseIndex, 0) + 1) % this.schedule.length;
		  Atomics.store(this.phaseIndex, 0, next);
		}
		private yieldNow(): void {
		  if (typeof scheduler !== "undefined" && "yield" in scheduler) {
			scheduler.yield();
		  } else if (typeof setImmediate === "function") {
			setImmediate(() => {});
		  } else {
			setTimeout(() => {}, 0);
		  }
		}
		// #endregion
	})(this);
	public exogul = new (class EXOGUL {// SLAB IS FOR ONLY ABOVE THE 8152 WEIGHT LIMIT OR WHEN USER REQUESTED
		readonly capacity: number;
		readonly values: Float64Array;
		readonly versions: Uint32Array;
		readonly nextFree: Int32Array;
		readonly refCount: Int32Array;
	
		private freeHead = -1;
		private allocated = 0;
	
		readonly slab: Uint8Array;
		private slabTail = 0;
	
		private readonly slabOffsets: Int32Array;
		private readonly slabLengths: Int32Array;
		// Track which slots are currently in use (for debugging/validation)
		private readonly inUse: Uint8Array;
		private freeRanges: { offset: number; length: number }[] = [];
		private hub: SITH;
	
		constructor(SITH: SITH, capacity: number = 1 << 20, slabCapacity = 1 << 20) {
			this.hub = SITH;
			if (!pow2(capacity)) throw new Error("slotCapacity must be a power of two");
	
			this.capacity = capacity;
	
			this.values = new Float64Array(capacity);
			this.versions = new Uint32Array(capacity);
			this.nextFree = new Int32Array(capacity);
			this.refCount = new Int32Array(capacity);
			this.inUse = new Uint8Array(capacity);
	
			this.nextFree.fill(-1);
	
			// Slot 0 is reserved as the control slot (version counter)
			this.allocated = 1;
			this.refCount[0] = 1;
			this.versions[0] = 0;
			this.inUse[0] = 1;
	
			// Initialize slab-related arrays
			this.slabOffsets = new Int32Array(capacity).fill(-1);
			this.slabLengths = new Int32Array(capacity).fill(0);
	
			this.slab = new Uint8Array(slabCapacity);
			this.slabTail = 0;
	
			// Build initial free list (skip slot 0)
			// Link slots 1..capacity-1 in reverse order so we pop from the front
			for (let i = capacity - 1; i >= 1; i--) {
				this.nextFree[i] = this.freeHead;
				this.freeHead = i;
			}
		}
		writeBinary(slot: N, data: U8, epoch: N): V {
			this.assertLive(slot);
			this.assertEpoch(epoch);
	
			const oldOffset = this.slabOffsets[slot];
			const oldLength = this.slabLengths[slot];
			const length = data.length;
	
			// Reuse the existing allocation when it is large enough.
			if (oldOffset >= 0 && length <= oldLength) {
				this.slab.set(data, oldOffset);
	
				if (length < oldLength) {
					this.freeBytes(oldOffset + length, oldLength - length);
				}
	
				this.slabOffsets[slot] = length === 0 ? 0 : oldOffset;
				this.slabLengths[slot] = length;
				this.values[slot] = 0;
				this.versions[slot] = epoch;
				return;
			}
	
			// Allocate first: exhaustion leaves the old payload intact.
			const offset = this.allocBytes(length);
	
			this.slab.set(data, offset);
	
			if (oldOffset >= 0 && oldLength > 0) {
				this.freeBytes(oldOffset, oldLength);
			}
	
			this.slabOffsets[slot] = offset;
			this.slabLengths[slot] = length;
			this.values[slot] = 0;
			this.versions[slot] = epoch;
		}
		readBinary(slot: N): U8 {
			const { offset, length } = this.ref(slot);
			return this.slab.subarray(offset, offset + length);
		}
		private allocBytes(length: N): N {
			if (length === 0) return 0;
	
			// First-fit allocation from reclaimed ranges.
			for (let i = 0; i < this.freeRanges.length; i++) {
				const range = this.freeRanges[i];
	
				if (range.length < length) continue;
	
				const offset = range.offset;
	
				if (range.length === length) {
					this.freeRanges.splice(i, 1);
				} else {
					range.offset += length;
					range.length -= length;
				}
	
				return offset;
			}
	
			if (length > this.slab.length - this.slabTail) {
				throw new RangeError("STORAGE_RING: no contiguous slab range large enough");
			}
	
			const offset = this.slabTail;
			this.slabTail += length;
			return offset;
		}
		private freeBytes(offset: N, length: N): V {
			if (length === 0) return;
	
			const ranges = this.freeRanges;
			let index = 0;
	
			while (index < ranges.length && ranges[index].offset < offset) {
				index++;
			}
	
			ranges.splice(index, 0, { offset, length });
	
			// Merge with the preceding range.
			if (index > 0 && ranges[index - 1].offset + ranges[index - 1].length === offset) {
				ranges[index - 1].length += length;
				ranges.splice(index, 1);
				index--;
			}
	
			// Merge with the following range.
			if (index + 1 < ranges.length && ranges[index].offset + ranges[index].length === ranges[index + 1].offset) {
				ranges[index].length += ranges[index + 1].length;
				ranges.splice(index + 1, 1);
			}
	
			// Return a free suffix to the unused slab tail.
			const last = ranges[ranges.length - 1];
	
			if (last !== undefined && last.offset + last.length === this.slabTail) {
				this.slabTail = last.offset;
				ranges.pop();
			}
		}
		private assertLive(slot: N): V {
			if (!Number.isInteger(slot) || slot <= 0 || slot >= this.allocated || this.refCount[slot] <= 0) {
				throw new RangeError(`STORAGE_RING: invalid or released slot ${slot}`);
			}
		}
		private assertEpoch(epoch: N): V {
			if (!Number.isInteger(epoch) || epoch < 0 || epoch > 0xffffffff) {
				throw new RangeError("STORAGE_RING: epoch must be a uint32");
			}
		}
		tick(): number {
			return this.versions[0];
		}
		advance(): number {
			return ++this.versions[0];
		} // Allocate a new slot from the free list. Returns the slot index, or -1 if no slots are available.
		alloc(value = 0): number {
			const slot = this.freeHead;
			if (slot === -1) return -1; // No free slots
	
			// Remove from free list
			this.freeHead = this.nextFree[slot];
			this.nextFree[slot] = -1;
	
			// Initialize slot
			this.values[slot] = value;
			this.refCount[slot] = 1; // Initial reference
			this.inUse[slot] = 1;
	
			// Clear slab allocation for this slot
			this.slabOffsets[slot] = -1;
			this.slabLengths[slot] = 0;
	
			this.allocated++;
	
			return slot;
		} // Increment the reference count for a slot. Returns true if successful, false if the slot is not allocated.
		retain(slot: number): boolean {
			if (slot < 0 || slot >= this.capacity) return false;
			if (!this.inUse[slot]) return false;
	
			if (this.refCount[slot] === 0x7fffffff) {
				throw new Error("Reference count overflow");
			}
	
			this.refCount[slot]++;
			return true;
		} // Decrement the reference count for a slot. If the ref count reaches zero, the slot is freed. Returns true if the slot was freed, false otherwise.
		release(slot: number): boolean {
			if (slot < 0 || slot >= this.capacity) return false;
			if (!this.inUse[slot]) return false;
	
			if (this.refCount[slot] <= 0) {
				throw new Error("Reference count underflow");
			}
	
			this.refCount[slot]--;
	
			if (this.refCount[slot] === 0) {
				// Free the slot
				this.inUse[slot] = 0;
	
				// Clear slab data
				this.slabOffsets[slot] = -1;
				this.slabLengths[slot] = 0;
				this.values[slot] = 0;
	
				// Add to free list
				this.nextFree[slot] = this.freeHead;
				this.freeHead = slot;
	
				this.allocated--;
	
				return true;
			}
	
			return false;
		} // Write a numeric value into a slot with epoch validation. Returns true if successful, false if the slot is invalid or epoch mismatch.
		write(slot: number, value: number, epoch: number): boolean {
			if (slot < 0 || slot >= this.capacity) return false;
			if (!this.inUse[slot]) return false;
			if (this.refCount[slot] <= 0) return false;
	
			// Epoch validation: the slot's version must match the expected epoch
			if (this.versions[slot] !== epoch) return false;
	
			this.values[slot] = value;
			return true;
		} // Read the numeric value from a slot. Returns NaN if the slot is invalid.
		read(slot: number): number {
			if (slot < 0 || slot >= this.capacity) return NaN;
			if (!this.inUse[slot]) return NaN;
	
			return this.values[slot];
		} // Get the version number for a slot. Returns -1 if the slot is invalid.
		version(slot: number): number {
			if (slot < 0 || slot >= this.capacity) return -1;
	
			return this.versions[slot];
		} // Get the slab reference (offset and length) for a slot. Returns { offset: -1, length: 0 } if no slab data is associated.
		ref(slot: number): { offset: number; length: number } {
			if (slot < 0 || slot >= this.capacity) {
				return { offset: -1, length: 0 };
			}
	
			return {
				offset: this.slabOffsets[slot],
				length: this.slabLengths[slot],
			};
		} // Store variable-sized binary data in the slab for a given slot. Returns true if successful, false if insufficient slab space.
		storeSlab(slot: number, data: Uint8Array | ArrayBuffer): boolean {
			if (slot < 0 || slot >= this.capacity) return false;
			if (!this.inUse[slot]) return false;
	
			const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
			const len = bytes.length;
	
			if (len === 0) {
				// Zero-length: clear slab assignment
				this.slabOffsets[slot] = -1;
				this.slabLengths[slot] = 0;
				return true;
			}
	
			// Simple bump allocator: check if we have room
			if (this.slabTail + len > this.slab.length) {
				// Could implement compaction here, but for now just fail
				return false;
			}
	
			const offset = this.slabTail;
			this.slab.set(bytes, offset);
			this.slabTail += len;
	
			this.slabOffsets[slot] = offset;
			this.slabLengths[slot] = len;
	
			return true;
		} // Read the variable-sized binary data associated with a slot. Returns a subarray view (no copy) or null if no data.
		loadSlab(slot: number): Uint8Array | null {
			if (slot < 0 || slot >= this.capacity) return null;
			if (!this.inUse[slot]) return null;
	
			const offset = this.slabOffsets[slot];
			const length = this.slabLengths[slot];
	
			if (offset < 0 || length <= 0) return null;
	
			return this.slab.subarray(offset, offset + length);
		}
		private growthSlab(minAdditional: number): void {
			const newCapacity = Math.max(this.slab.length * 2, this.slabTail + minAdditional);
			const newSlab = new Uint8Array(newCapacity);
			newSlab.set(this.slab);
			//this.slab = newSlab;
		} // Check if the slab buffer needs compaction. Returns true if compaction would reclaim significant space.
		needsCompaction(): boolean {
			if (this.allocated <= 1) return false; // Only control slot
	
			// Count live slab bytes
			let liveBytes = 0;
			for (let i = 1; i < this.capacity; i++) {
				if (this.inUse[i] && this.slabLengths[i] > 0) {
					liveBytes += this.slabLengths[i];
				}
			}
	
			// Compact if more than 50% of slab is dead space
			const deadBytes = this.slabTail - liveBytes;
			return deadBytes > this.slabTail * 0.5;
		} // Compact the slab buffer, removing dead space. Invalidates any subarray views previously returned by loadSlab.
		compact(): void {
			if (!this.needsCompaction()) return;
	
			const newSlab = new Uint8Array(this.slab.length);
			let newTail = 0;
	
			for (let i = 1; i < this.capacity; i++) {
				if (!this.inUse[i]) continue;
	
				const length = this.slabLengths[i];
				if (length <= 0) continue;
	
				const oldOffset = this.slabOffsets[i];
				newSlab.set(this.slab.subarray(oldOffset, oldOffset + length), newTail);
	
				this.slabOffsets[i] = newTail;
				newTail += length;
			}
	
			// Copy back into original slab buffer
			this.slab.set(newSlab.subarray(0, newTail), 0);
			this.slabTail = newTail;
		} // Get statistics about the storage ring.
		stats(): {
			capacity: number;
			allocated: number;
			freeSlots: number;
			slabUsed: number;
			slabCapacity: number;
			slabUtilization: number;
		} {
			return {
				capacity: this.capacity,
				allocated: this.allocated,
				freeSlots: this.capacity - this.allocated,
				slabUsed: this.slabTail,
				slabCapacity: this.slab.length,
				slabUtilization: this.slabTail / this.slab.length,
			};
		} // Validate internal consistency (for debugging). Returns an array of error strings, empty if consistent.
		validate(): string[] {
			const errors: string[] = [];
	
			// Count free list
			let freeCount = 0;
			let current = this.freeHead;
			const seen = new Set<number>();
	
			while (current !== -1) {
				if (current < 1 || current >= this.capacity) {
					errors.push(`Free list contains invalid slot: ${current}`);
					break;
				}
				if (seen.has(current)) {
					errors.push(`Free list contains cycle at slot: ${current}`);
					break;
				}
				seen.add(current);
				freeCount++;
				current = this.nextFree[current];
			}
	
			if (freeCount !== this.capacity - this.allocated) {
				errors.push(`Free list has ${freeCount} entries, expected ${this.capacity - this.allocated}`);
			}
	
			// Validate slab offsets
			for (let i = 1; i < this.capacity; i++) {
				if (!this.inUse[i]) continue;
	
				const offset = this.slabOffsets[i];
				const length = this.slabLengths[i];
	
				if (offset === -1 && length !== 0) {
					errors.push(`Slot ${i}: offset -1 but length ${length}`);
				} else if (offset !== -1) {
					if (offset < 0 || offset + length > this.slabTail) {
						errors.push(`Slot ${i}: slab range [${offset}, ${offset + length}) out of bounds (tail=${this.slabTail})`);
					}
				}
			}
	
			return errors;
		}
		private get liveBytes(): number {
			let total = 0;
	
			for (let i = 1; i < this.capacity; i++) {
				if (this.inUse[i] && this.slabLengths[i] > 0) {
					total += this.slabLengths[i];
				}
			}
	
			return total;
		}
		get used(): N {
			return this.allocated - 1 - this.freeCount();
		}
		freeCount(): N {
			return this.freeHead === FREE_LIST_END ? 0 : this.walkFree();
		}
		get slabUsed(): N {
			return this.liveBytes;
		}
		get slabFree(): N {
			return this.slab.length - this.liveBytes;
		}
		private walkFree(): N {
			let n = 0;
			let p = this.freeHead;
			while (p !== FREE_LIST_END) {
				n++;
				p = this.nextFree[p];
			}
			return n;
	}})(this);
	public codecs = (() => {
		const z = () => {}
		z.beam = new (class BEAM_CODEC {// ENCODING / DECODING
			/*  =============================================================================
				 Encodes ordinary JavaScript values into a collider-owned byte slab.
				================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
				public hub: SITH
				private bytes: U8;
				private view: DataView;
				private tail = 0;
				private strings = new Map<S, N>();
				private stringBytes: U8[] = [];
				private refs = new Map<object, N>();
				private nextRef = 0;
				constructor(hub: SITH, initialCapacity = 1 << 16) {
					this.hub = hub
					this.bytes = new Uint8Array(initialCapacity);
					this.view = new DataView(this.bytes.buffer);
				}
				/* HC_BINARY_CODEC */
				public encode(value: U): U8 {
					this.tail = 0;
					this.strings.clear();
					this.stringBytes.length = 0;
					this.refs.clear();
					this.nextRef = 0;
			
					this.writeValue(value);
			
					return this.bytes.subarray(0, this.tail);
				}
				public decode(binary: U8): U {
					const savedBytes = this.bytes;
					const savedView = this.view;
					const savedTail = this.tail;
			
					try {
						this.bytes = binary;
						this.view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
						this.tail = binary.length;
			
						this.refs.clear();
						this.nextRef = 0;
			
						const cursor = { at: 0 };
			
						return this.readValue(cursor);
					} finally {
						// Restore the encode-side buffer so the next encode()
						// never writes into the disruptor pool.
						this.bytes = savedBytes;
						this.view = savedView;
						this.tail = savedTail;
					}
				}
				private need(count: N): V {
					if (this.tail + count <= this.bytes.length) return;
			
					let size = this.bytes.length;
			
					while (size < this.tail + count) size *= 2;
			
					const next = new Uint8Array(size);
					next.set(this.bytes);
			
					this.bytes = next;
					this.view = new DataView(next.buffer);
				}
				private u8(value: N): V {
					this.need(1);
					this.bytes[this.tail++] = value;
				}
				private i32(value: N): V {
					this.need(4);
					this.view.setInt32(this.tail, value, true);
					this.tail += 4;
				}
				private f64(value: N): V {
					this.need(8);
					this.view.setFloat64(this.tail, value, true);
					this.tail += 8;
				}
				private writeValue(value: U): V {
					if (value === undefined) return this.u8(TAG.UNDEFINED);
					if (value === null) return this.u8(TAG.NULL);
					if (value === false) return this.u8(TAG.FALSE);
					if (value === true) return this.u8(TAG.TRUE);
					const type = typeof value;
					if (type === "number") {
						const n = value as N;
						if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
							this.u8(TAG.INT32);
							return this.i32(n);
						}
			
						this.u8(TAG.FLOAT64);
						return this.f64(n);
					}
			
					if (type === "string") {
						return this.writeString(value as S);
					}
			
					if (Array.isArray(value)) {
						return this.writeArray(value);
					}
			
					if (value instanceof Uint8Array) {
						return this.writeBytes(value);
					}
			
					if (type === "object") {
						return this.writeObject(value as object);
					}
			
					throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
				}
				private writeString(value: S): V {
					const encoded = new TextEncoder().encode(value);
			
					this.u8(TAG.STRING);
					this.i32(encoded.length);
					this.need(encoded.length);
					this.bytes.set(encoded, this.tail);
					this.tail += encoded.length;
				}
				private writeArray(value: U[]): V {
					const ref = this.beginRef(value);
			
					this.u8(TAG.ARRAY);
					this.i32(value.length);
			
					for (let i = 0; i < value.length; i++) {
						this.writeValue(value[i]);
					}
			
					this.endRef(ref);
				}
				private writeBytes(value: U8): V {
					this.u8(TAG.UINT8ARRAY);
					this.i32(value.length);
					this.need(value.length);
					this.bytes.set(value, this.tail);
					this.tail += value.length;
				}
				private writeObject(value: object): V {
					const ref = this.beginRef(value);
					const keys = Object.keys(value);
			
					this.u8(TAG.OBJECT);
					this.i32(keys.length);
			
					for (const key of keys) {
						this.writeString(key);
						this.writeValue((value as any)[key]);
					}
			
					this.endRef(ref);
				}
				private beginRef(value: object): N {
					const existing = this.refs.get(value);
			
					if (existing !== undefined) {
						this.u8(TAG.REF);
						this.i32(existing);
						return -1;
					}
			
					const ref = this.nextRef++;
					this.refs.set(value, ref);
			
					return ref;
				}
				private endRef(ref: N): V { void ref; }// Reserved for future back-reference patching.
				private readU8(cursor: { at: N }): N { return this.bytes[cursor.at++];	}
				private readI32(cursor: { at: N }): N {
					const value = this.view.getInt32(cursor.at, true);
					cursor.at += 4;
			
					return value;
				}
				private readF64(cursor: { at: N }): N {
					const value = this.view.getFloat64(cursor.at, true);
					cursor.at += 8;
			
					return value;
				}
				private readValue(cursor: { at: N }): U {
					const tag = this.readU8(cursor);
			
					switch (tag) {
						case TAG.UNDEFINED:
							return undefined;
			
						case TAG.NULL:
							return null;
			
						case TAG.FALSE:
							return false;
			
						case TAG.TRUE:
							return true;
			
						case TAG.INT32:
							return this.readI32(cursor);
			
						case TAG.FLOAT64:
							return this.readF64(cursor);
			
						case TAG.STRING:
							return this.readString(cursor);
			
						case TAG.ARRAY:
							return this.readArray(cursor);
			
						case TAG.OBJECT:
							return this.readObject(cursor);
			
						case TAG.UINT8ARRAY:
							return this.readBytes(cursor);
			
						default:
							throw new Error(`HC_BINARY_CODEC: unknown tag ${tag}`);
					}
				}
				private readString(cursor: { at: N }): S {
					const length = this.readI32(cursor);
					const bytes = this.bytes.subarray(cursor.at, cursor.at + length);
			
					cursor.at += length;
			
					return new TextDecoder().decode(bytes);
				}
				private readArray(cursor: { at: N }): U[] {
					const length = this.readI32(cursor);
					const out: U[] = new Array(length);
			
					for (let i = 0; i < length; i++) {
						out[i] = this.readValue(cursor);
					}
			
					return out;
				}
				private readObject(cursor: { at: N }): Record<S, U> {
					const length = this.readI32(cursor);
					const out: Record<S, U> = {};
			
					for (let i = 0; i < length; i++) {
						const key = this.readString(cursor);
						out[key] = this.readValue(cursor);
					}
			
					return out;
				}
				private readBytes(cursor: { at: N }): Uint8Array {
					const length = this.readI32(cursor);
					const out = this.bytes.slice(cursor.at, cursor.at + length);
			
					cursor.at += length;
			
					return out;
				}
		})(this);
		z.cbor = new (class BINARY_CBOR {
			encodeJSONEnvelope(envelope: any): Uint8Array {
				return new TextEncoder().encode(JSON.stringify(envelope))
			}
		
			decodeJSONEnvelope(bytes: Uint8Array): any {
				return JSON.parse(new TextDecoder().decode(bytes))
			}
			private readonly encoder = new TextEncoder()
			private readonly decoder = new TextDecoder()
			private ensureAvailable(buf: Uint8Array, pos: number, length: number): void { if (pos < 0 || length < 0 || pos + length > buf.length) throw new Error("CBOR: truncated buffer") }
			private readUint(buf: Uint8Array, pos: number): [number, number] {
				this.ensureAvailable(buf, pos, 1)
				const info = buf[pos] & 0x1f
				if (info <= 23) return [info, pos + 1]
				if (info === 24) { this.ensureAvailable(buf, pos + 1, 1); return [buf[pos + 1], pos + 2] }
				if (info === 25) { this.ensureAvailable(buf, pos + 1, 2); return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(0, false), pos + 3]; }
				if (info === 26) { this.ensureAvailable(buf, pos + 1, 4); return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(0, false), pos + 5]; }
				if (info === 27) {
					this.ensureAvailable(buf, pos + 1, 8)
					const value = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8).getBigUint64(0, false)
					if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER")
					return [Number(value), pos + 9]
				}; throw new Error(`CBOR: unsupported additional info ${info}`)
			}
			private encodeUint64(majorShifted: number, n: bigint): Uint8Array {
				if (n <= 23n) return new Uint8Array([majorShifted | Number(n)])
				if (n <= 0xffn) { return new Uint8Array([majorShifted | 24, Number(n)]) }
				if (n <= 0xffffn) {
					const b = new Uint8Array(3)
					b[0] = majorShifted | 25
					new DataView(b.buffer).setUint16(1, Number(n), false)
					return b
				}
				if (n <= 0xffffffffn) {
					const b = new Uint8Array(5)
					b[0] = majorShifted | 26
					new DataView(b.buffer).setUint32(1, Number(n), false)
					return b
				}
				const b = new Uint8Array(9)
				b[0] = majorShifted | 27
				new DataView(b.buffer).setBigUint64(1, n, false)
				return b
			}
			private encode(value: CBORValue): Uint8Array {
				const parts: Uint8Array[] = []
				this.encodeValue(value, parts)
				const total = parts.reduce((n, p) => n + p.length, 0)
				const out = new Uint8Array(total)
				let offset = 0
				for (const p of parts) { out.set(p, offset); offset += p.length }
				return out
			}
			private encodeValue(v: CBORValue, out: Uint8Array[]): void {
				if (v === null) { out.push(new Uint8Array([0xf6])); return; }
				if (v === undefined) { out.push(new Uint8Array([0xf7])); return; }
				if (v === true) { out.push(new Uint8Array([0xf5])); return }
				if (v === false) { out.push(new Uint8Array([0xf4])); return }
		
				if (typeof v === 'number') {
					if (Number.isSafeInteger(v)) {
						if (v >= 0) out.push(this.encodeUint64(0x00, BigInt(v)))
						else out.push(this.encodeUint64(0x20, BigInt(-1 - v)))
					} else {
						const b = new Uint8Array(9)
						b[0] = 0xfb
						new DataView(b.buffer).setFloat64(1, v, false)
						out.push(b)
					}
					return
				}
		
				if (typeof v === 'string') { const encoded = this.encoder.encode(v); out.push(this.encodeUint(0x60, encoded.length)); out.push(encoded); return; }
				if (v instanceof Uint8Array) { out.push(this.encodeUint(0x40, v.length)); out.push(v); return; }
				if (Array.isArray(v)) {
					out.push(this.encodeUint(0x80, v.length));
					for (const item of v) { this.encodeValue(item, out); }
					return;
				}
				if (typeof v === 'object') {
					const proto = Object.getPrototypeOf(v)
					if (proto !== Object.prototype && proto !== null) throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`)
					const keys = Object.keys(v)
					out.push(this.encodeUint(0xa0, keys.length))
					for (const key of keys) { this.encodeValue(key, out); this.encodeValue(v[key], out); }
					return
				}; throw new Error(`CBOR: unsupported value type ${typeof v}`)
			}
			private encodeUint(majorShifted: number, n: number): Uint8Array {
				if (n <= 23) return new Uint8Array([majorShifted | n]);
				if (n <= 0xff) return new Uint8Array([majorShifted | 24, n]);
				if (n <= 0xffff) { const b = new Uint8Array(3); b[0] = majorShifted | 25; new DataView(b.buffer).setUint16(1, n, false); return b };
				const b = new Uint8Array(5); b[0] = majorShifted | 26; new DataView(b.buffer).setUint32(1, n, false); return b;
			}
			private decode(buf: Uint8Array): CBORValue {
				const [value, end] = this.decodeValue(buf, 0);
				if (end !== buf.length) { throw new Error("CBOR: trailing bytes") };
				return value;
			}
			private decodeValue(buf: Uint8Array, pos: number): [CBORValue, number] {
				this.ensureAvailable(buf, pos, 1)
				const first = buf[pos]
				const major = first >> 5
				const info = first & 0x1f
		
				if (major === 7) {
					if (info === 20) return [false, pos + 1]
					if (info === 21) return [true, pos + 1]
					if (info === 22) return [null, pos + 1]
					if (info === 23) return [undefined, pos + 1]
					if (info === 27) { this.ensureAvailable(buf, pos + 1, 8); const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8); return [dv.getFloat64(0, false), pos + 9]; }
				}
				const [n, nextPos] = this.readUint(buf, pos)
				if (major === 0) return [n, nextPos]
				if (major === 1) return [-1 - n, nextPos]
				if (major === 2) { this.ensureAvailable(buf, nextPos, n); const bytes = buf.slice(nextPos, nextPos + n); return [bytes, nextPos + n]; }
				if (major === 3) { this.ensureAvailable(buf, nextPos, n); const bytes = buf.slice(nextPos, nextPos + n); return [this.decoder.decode(bytes), nextPos + n]; }
				if (major === 4) {
					const arr: CBORValue[] = []
					let p = nextPos
					for (let i = 0; i < n; i++) { const [item, np] = this.decodeValue(buf, p); arr.push(item); p = np; }
					return [arr, p]
				}
				if (major === 5) {
					const obj: Record<string, CBORValue> = {}
					let p = nextPos
					for (let i = 0; i < n; i++) { const [k, kp] = this.decodeValue(buf, p); const [v, vp] = this.decodeValue(buf, kp); obj[k as string] = v; p = vp; }
					return [obj, p]
				}; throw new Error(`CBOR: unsupported major type ${major}`)
			}
			public encode_msg(msg: any): Uint8Array {// Instead of leaking a raw object, encode an explicit fallback/error frame
				if (msg instanceof Uint8Array) return msg;// If it's already an encoded Uint8Array, don't re-encode it
				try { return this.encode(msg); }
				catch (err) { console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err); return this.encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" }); }
			}
			public decode_msg(raw: any): any {// Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
				if (!(raw instanceof Uint8Array) && !(typeof Buffer !== 'undefined' && Buffer.isBuffer(raw))) { console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream."); return null; }
				try {
					const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array((raw as any).buffer, (raw as any).byteOffset, (raw as any).length); return this.decode(uint8);
				} catch (err) {
					console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
					return null;
				}
			}
		})(this);
		z.soa = new ( class SOA_CODEC {
			// HIGH_PERFORMANCE_SOA_CODEC
			public readonly maxPids: number;
			public hub: HUB
				
			// SoA Memory Layout (Typed Arrays for Cache Locality)
			public readonly channelMap: Int32Array;
			public readonly subChannelMap: Int32Array;
			public readonly tripChannelMap: Int32Array;
			public readonly keyMap: Int32Array;
		
			public readonly listeners: Array<((pid: number, val?: number) => void) | undefined>;
			public nextPid = 1;
		
			constructor(hub: SITH = this, maxPids = 8192) {
				this.maxPids = maxPids;
				this.hub = hub
				this.channelMap = new Int32Array(maxPids);
				this.subChannelMap = new Int32Array(maxPids);
				this.tripChannelMap = new Int32Array(maxPids);
				this.keyMap = new Int32Array(maxPids);
				this.listeners = new Array(maxPids);
			}// Hot path lookup or allocation: O(1) inline integer assignment
			public allocatePid(rId: number, mId: number, sId: number, kId: number): number {
				// In a true LMAX disruptor ring or fixed socket registry,
				// PIDs can be pre-mapped or statically routed at startup.
				const pid = this.nextPid++;
				if (pid >= this.maxPids) throw new Error("PID capacity exceeded");
		
				this.channelMap[pid] = rId;
				this.subChannelMap[pid] = mId;
				this.tripChannelMap[pid] = sId;
				this.keyMap[pid] = kId;
		
				return pid;
			}
			/**
			 * Encodes a columnar batch (e.g., a spreadsheet column or database table chunk)
			 * directly into a parallel SoA byte slab.
			 */
			public encodeBatch(ids: Int32Array, values: Float64Array): Uint8Array {
				const headerLength = 8; // 4 bytes for count, 4 bytes for flags
				const idBytes = ids.byteLength;
				const valBytes = values.byteLength;
				
				const totalSize = headerLength + idBytes + valBytes;
				const slab = new Uint8Array(totalSize);
				const view = new DataView(slab.buffer);
		
				// Write header metadata
				view.setInt32(0, ids.length, true);
				view.setInt32(4, 0, true); // reserved flags
		
				// Copy raw typed array memory directly into the slab (Zero allocation overhead)
				slab.set(new Uint8Array(ids.buffer, ids.byteOffset, idBytes), headerLength);
				slab.set(new Uint8Array(values.buffer, values.byteOffset, valBytes), headerLength + idBytes);
		
				return slab;
			}
			/**
			 * Decodes / Views the binary slab directly as SoA arenas 
			 * without creating individual JavaScript objects.
			 */
			public decodeBatchView(slab: Uint8Array): { ids: Int32Array, values: Float64Array } {
				const view = new DataView(slab.buffer, slab.byteOffset, slab.byteLength);
				const count = view.getInt32(0, true);
				const headerLength = 8;
		
				const idByteLen = count * 4; // Int32 = 4 bytes
				const valByteLen = count * 8; // Float64 = 8 bytes
		
				const ids = new Int32Array(slab.buffer, slab.byteOffset + headerLength, count);
				const values = new Float64Array(slab.buffer, slab.byteOffset + headerLength + idByteLen, count);
		
				return { ids, values };
			}
		})(this);
		z.serializer = (() => {
			const SKIP = Symbol('ser:skip');
			const DEFAULT_CONFIG: Required<SerFormatConfig> = {
			date: true, bigint: true, regexp: true, error: true, specialNumber: true,
			undefined: true, typedArray: true, url: true, map: true, set: true,
			array: true, object: true, string: true, number: true, boolean: true,
			};
			const TYPED_ARRAY_NAMES = new Set([
			'Int8Array', 'Uint8Array', 'Uint8ClampedArray', 'Int16Array', 'Uint16Array',
			'Int32Array', 'Uint32Array', 'Float32Array', 'Float64Array', 'BigInt64Array', 'BigUint64Array',
			]);
			const z = () => {}
			z.encoder = class SERI {
				private config: Required<SerFormatConfig>;
				private customEncoder?: CustomEncoder;
			  
				constructor(config: SerFormatConfig = {}, customEncoder?: CustomEncoder) {
				  this.config = { ...DEFAULT_CONFIG, ...config };
				  this.customEncoder = customEncoder;
				}
			  
				encodeJSONEnvelope(envelope: unknown): Uint8Array {
				  return new TextEncoder().encode(JSON.stringify(envelope));
				}
			  
				public ser(value: unknown): string {
				  return JSON.stringify(this._encode(value));
				}
			  
				private _encode(value: unknown): unknown {
				  if (this.customEncoder) {
					const custom = this.customEncoder(value, v => this._encode(v));
					if (custom !== SKIP) return { __type: custom.type, value: custom.value };
				  }
			  
				  if (value === null) return null;
				  if (value === undefined) {
					return this.config.undefined ? { __type: 'Undefined' } : undefined;
				  }
			  
				  const t = typeof value;
			  
				  if (t === 'bigint') {
					return this.config.bigint
					  ? { __type: 'BigInt', value: (value as bigint).toString() }
					  : (value as bigint).toString();
				  }
			  
				  if (t === 'number') {
					if (!Number.isFinite(value as number)) {
					  return this.config.specialNumber
						? { __type: 'SpecialNumber', value: Number.isNaN(value) ? 'NaN' : (value as number) > 0 ? 'Infinity' : '-Infinity' }
						: null;
					}
					return this.config.number ? { __type: 'Primitive', value } : value;
				  }
			  
				  if (t === 'string') return this.config.string ? { __type: 'Primitive', value } : value;
				  if (t === 'boolean') return this.config.boolean ? { __type: 'Primitive', value } : value;
			  
				  if (value instanceof Date) {
					return this.config.date ? { __type: 'Date', value: value.toISOString() } : value.toISOString();
				  }
			  
				  if (value instanceof RegExp) {
					return this.config.regexp
					  ? { __type: 'RegExp', value: { source: value.source, flags: value.flags } }
					  : value.toString();
				  }
			  
				  if (value instanceof Error) {
					return this.config.error
					  ? { __type: 'Error', value: { name: value.name, message: value.message, stack: value.stack } }
					  : value.message;
				  }
			  
				  if (value instanceof URL) {
					return this.config.url ? { __type: 'URL', value: value.href } : value.href;
				  }
			  
				  if (ArrayBuffer.isView(value) && TYPED_ARRAY_NAMES.has(value.constructor.name)) {
					if (!this.config.typedArray) return undefined; // no lossless native fallback
					const view = value as unknown as { buffer: ArrayBufferLike; byteOffset: number; byteLength: number };
					const bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
					let binary = '';
					for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
					return { __type: value.constructor.name, value: btoa(binary) };
				  }
			  
				  if (value instanceof Map) {
					const entries = Array.from(value.entries()).map(([k, v]) => [this._encode(k), this._encode(v)]);
					return this.config.map ? { __type: 'Map', value: entries } : Object.fromEntries(entries);
				  }
			  
				  if (value instanceof Set) {
					const items = Array.from(value.values()).map(v => this._encode(v));
					return this.config.set ? { __type: 'Set', value: items } : items;
				  }
			  
				  if (Array.isArray(value)) {
					const items = value.map(v => this._encode(v));
					return this.config.array ? { __type: 'Array', value: items } : items;
				  }
			  
				  if (t === 'object') {
					const obj: Record<string, unknown> = {};
					for (const [k, v] of Object.entries(value as Record<string, unknown>)) obj[k] = this._encode(v);
					return this.config.object ? { __type: 'Object', value: obj } : obj;
				  }
			  
				  return value; // functions/symbols — unsupported, same as native JSON
				}
			}
			z.decoder = class DESERI {
				private config: Required<SerFormatConfig>;
				private customDecoder?: CustomDecoder;
			  
				constructor(config: SerFormatConfig = {}, customDecoder?: CustomDecoder) {
				  this.config = { ...DEFAULT_CONFIG, ...config };
				  this.customDecoder = customDecoder;
				}
			  
				decodeJSONEnvelope(bytes: Uint8Array): unknown {
				  return JSON.parse(new TextDecoder().decode(bytes));
				}
			  
				public deser(raw: unknown): unknown {
				  const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
				  return this._decode(parsed);
				}
			  
				private _decode(parsed: unknown): unknown {
				  if (parsed === null || parsed === undefined) return parsed;
			  
				  if (typeof parsed === 'object' && '__type' in (parsed as Record<string, unknown>)) {
					const { __type: type, value } = parsed as { __type: string; value: unknown };
			  
					if (this.customDecoder) {
					  const custom = this.customDecoder(type, value, v => this._decode(v));
					  if (custom !== SKIP) return custom;
					}
			  
					switch (type) {
					  case 'Undefined': return undefined;
					  case 'BigInt': return BigInt(value as string);
					  case 'SpecialNumber':
						return value === 'NaN' ? NaN : value === 'Infinity' ? Infinity : -Infinity;
					  case 'Date': return new Date(value as string);
					  case 'RegExp': { const v = value as { source: string; flags: string }; return new RegExp(v.source, v.flags); }
					  case 'Error': {
						const v = value as { name: string; message: string; stack?: string };
						const e = new Error(v.message); e.name = v.name; e.stack = v.stack; return e;
					  }
					  case 'URL': return new URL(value as string);
					  case 'Map': return new Map((value as [unknown, unknown][]).map(([k, v]) => [this._decode(k), this._decode(v)]));
					  case 'Set': return new Set((value as unknown[]).map(v => this._decode(v)));
					  case 'Array': return (value as unknown[]).map(v => this._decode(v));
					  case 'Object': {
						const obj: Record<string, unknown> = {};
						for (const [k, v] of Object.entries(value as Record<string, unknown>)) obj[k] = this._decode(v);
						return obj;
					  }
					  case 'Primitive': return value;
					  default: {
						if (TYPED_ARRAY_NAMES.has(type)) {
						  const binary = atob(value as string);
						  const bytes = new Uint8Array(binary.length);
						  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
						  const Ctor = (globalThis as Record<string, any>)[type];
						  return new Ctor(bytes.buffer);
						}
						return parsed; // unrecognized tag — hand back untouched
					  }
					}
				  }
			  
				  if (Array.isArray(parsed)) return parsed.map(v => this._decode(v));
				  if (typeof parsed === 'object') {
					const obj: Record<string, unknown> = {};
					for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) obj[k] = this._decode(v);
					return obj;
				  }
				  return parsed;
				}
			}
			z.enc = new (this.codecs.serializer.encoder)({});
			z.dec = new (this.codecs.serializer.decoder)({});
			return z
		})(); 
		z.pids = new ()(this);
		const AVRO_ERROR_SCHEMA: AVROSchema = {
			type: "record",
			name: "AVROError",
			fields: [
				{ name: "type", type: "string" },
				{ name: "error", type: "string" },
				{ name: "id", type: "string" },
			],
		};
		z.avro_writer = new (class AVROWriter {
			private buf = new Uint8Array(256);
			private pos = 0;
		
			private ensure(length: N): V {
				if (this.pos + length <= this.buf.length) return;
				let size = this.buf.length;
				while (size < this.pos + length) size *= 2;
				const next = new Uint8Array(size);
				next.set(this.buf);
				this.buf = next;
			}
		
			byte(v: N): V {
				this.ensure(1);
				this.buf[this.pos++] = v;
			}
		
			bytes(v: Uint8Array): V {
				this.ensure(v.length);
				this.buf.set(v, this.pos);
				this.pos += v.length;
			}
		
			long(value: number | bigint): V {
				let n = BigInt(value);
				if (n < -(1n << 63n) || n > (1n << 63n) - 1n) throw new Error("Avro: long out of range");
				n = n >= 0n ? n << 1n : ((-n) << 1n) - 1n;
				while (n >= 0x80n) {
					this.byte(Number(n & 0x7fn) | 0x80);
					n >>= 7n;
				}
				this.byte(Number(n));
			}
		
			int(value: N): V {
				if (!Number.isInteger(value) || value < -2147483648 || value > 2147483647)
					throw new Error("Avro: int out of range");
				this.long(value);
			}
		
			float(value: N): V {
				this.ensure(4);
				new DataView(this.buf.buffer).setFloat32(this.pos, value, true);
				this.pos += 4;
			}
		
			double(value: N): V {
				this.ensure(8);
				new DataView(this.buf.buffer).setFloat64(this.pos, value, true);
				this.pos += 8;
			}
		
			finish(): Uint8Array {
				return this.buf.slice(0, this.pos);
			}
		})();
		z.avro_reader = new (class AVROReader {
			private pos = 0;
			private readonly view: DataView;
		
			constructor(private readonly buf: Uint8Array) {
				this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
			}
		
			private ensure(length: N): V {
				if (length < 0 || this.pos + length > this.buf.length)
					throw new Error("Avro: truncated buffer");
			}
		
			byte(): N {
				this.ensure(1);
				return this.buf[this.pos++];
			}
		
			bytes(length: N): Uint8Array {
				this.ensure(length);
				const out = this.buf.slice(this.pos, this.pos + length);
				this.pos += length;
				return out;
			}
		
			long(): number | bigint {
				let n = 0n;
				let shift = 0n;
		
				for (let i = 0; i < 10; i++) {
					const b = this.byte();
					n |= BigInt(b & 0x7f) << shift;
					if ((b & 0x80) === 0) {
						const value = (n & 1n) === 0n ? n >> 1n : -((n >> 1n) + 1n);
						return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
							? Number(value)
							: value;
					}
					shift += 7n;
				}
		
				throw new Error("Avro: invalid long");
			}
		
			int(): N {
				const n = this.long();
				if (typeof n === "bigint" || n < -2147483648 || n > 2147483647)
					throw new Error("Avro: invalid int");
				return n;
			}
		
			float(): N {
				this.ensure(4);
				const n = this.view.getFloat32(this.pos, true);
				this.pos += 4;
				return n;
			}
		
			double(): N {
				this.ensure(8);
				const n = this.view.getFloat64(this.pos, true);
				this.pos += 8;
				return n;
			}
		
			done(): B { return this.pos === this.buf.length; }
		})(this);
		z.BINARY_AVRO = class BINARY_AVRO {
			private readonly encoder = new TextEncoder();
			private readonly decoder = new TextDecoder();
			private readonly schema: AVROSchema;
			private readonly names: OB<AVROSchema> = Object.create(null);
		
			private readonly errorSchema = AVRO_ERROR_SCHEMA;
			private readonly wireSchema: AVROSchema[];
		
			constructor(schema: AVROSchema) {
				if (Array.isArray(schema)) throw new Error("Avro: root schema cannot be a union");
				this.schema = schema;
				this.wireSchema = [schema, this.errorSchema];
				this.#collect(schema);
				this.#collect(this.errorSchema);
			}
		
			private #collect(schema: AVROSchema): V {
				if (Array.isArray(schema)) {
					for (const branch of schema) this.#collect(branch);
					return;
				}
				if (typeof schema === "string") return;
		
				if (schema.name) this.names[schema.name] = schema;
		
				if (Array.isArray(schema.type)) this.#collect(schema.type);
				else if (typeof schema.type === "object") this.#collect(schema.type);
				else if (typeof schema.type === "string" && ![
					"null", "boolean", "int", "long", "float", "double",
					"bytes", "string", "record", "array", "map", "enum", "fixed",
				].includes(schema.type)) {
					if (!this.names[schema.type]) throw new Error(`Avro: unknown named type ${schema.type}`);
				}
		
				if (schema.fields) for (const field of schema.fields) this.#collect(field.type);
				if (schema.items) this.#collect(schema.items);
				if (schema.values) this.#collect(schema.values);
			}
		
			private #resolve(schema: AVROSchema): AVROSchema {
				if (typeof schema === "string" && this.names[schema]) return this.names[schema];
				return schema;
			}
		
			private #kind(schema: AVROSchema): S {
				schema = this.#resolve(schema);
				if (typeof schema === "string") return schema;
				if (Array.isArray(schema)) return "union";
				return typeof schema.type === "string" ? schema.type : this.#kind(schema.type);
			}
		
			private #matches(schema: AVROSchema, value: AVROValue): B {
				schema = this.#resolve(schema);
				const kind = this.#kind(schema);
		
				switch (kind) {
					case "null": return value === null || value === undefined;
					case "boolean": return typeof value === "boolean";
					case "int": return typeof value === "number" && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647;
					case "long": return typeof value === "bigint" || (typeof value === "number" && Number.isSafeInteger(value));
					case "float":
					case "double": return typeof value === "number";
					case "string": return typeof value === "string";
					case "bytes":
					case "fixed": return value instanceof Uint8Array && (kind !== "fixed" || value.length === (schema as any).size);
					case "array": return Array.isArray(value);
					case "map": return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array);
					case "record": return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array);
					case "enum": return typeof value === "string" && (schema as any).symbols.includes(value);
					default: return false;
				}
			}
		
			private #write(w: AVROWriter, schema: AVROSchema, value: AVROValue): V {
				schema = this.#resolve(schema);
		
				if (Array.isArray(schema)) {
					const index = schema.findIndex(branch => this.#matches(branch, value));
					if (index === -1) throw new Error("Avro: no matching union branch");
					w.long(index);
					this.#write(w, schema[index], value);
					return;
				}
		
				if (typeof schema === "string") {
					switch (schema) {
						case "null": return;
						case "boolean": w.byte(value ? 1 : 0); return;
						case "int": w.int(value); return;
						case "long": w.long(value); return;
						case "float": w.float(value); return;
						case "double": w.double(value); return;
						case "string": {
							const bytes = this.encoder.encode(value);
							w.long(bytes.length);
							w.bytes(bytes);
							return;
						}
						case "bytes": {
							w.long(value.length);
							w.bytes(value);
							return;
						}
						default: throw new Error(`Avro: unknown type ${schema}`);
					}
				}
		
				if (typeof schema.type !== "string") {
					this.#write(w, schema.type, value);
					return;
				}
		
				switch (schema.type) {
					case "null": return;
					case "boolean":
						w.byte(value ? 1 : 0);
						return;
					case "int":
						w.int(value);
						return;
					case "long":
						w.long(value);
						return;
					case "float":
						w.float(value);
						return;
					case "double":
						w.double(value);
						return;
					case "string": {
						const bytes = this.encoder.encode(value);
						w.long(bytes.length);
						w.bytes(bytes);
						return;
					}
					case "bytes":
						w.long(value.length);
						w.bytes(value);
						return;
					case "fixed":
						if (!(value instanceof Uint8Array) || value.length !== schema.size)
							throw new Error("Avro: invalid fixed value");
						w.bytes(value);
						return;
					case "enum": {
						const index = schema.symbols!.indexOf(value);
						if (index < 0) throw new Error(`Avro: invalid enum symbol ${value}`);
						w.long(index);
						return;
					}
					case "record":
						for (const field of schema.fields!) {
							if (value[field.name] === undefined && !Array.isArray(field.type))
								throw new Error(`Avro: missing field ${field.name}`);
							this.#write(w, field.type, value[field.name]);
						}
						return;
					case "array":
						if (value.length) {
							w.long(value.length);
							for (const item of value) this.#write(w, schema.items!, item);
						}
						w.long(0);
						return;
					case "map": {
						const keys = Object.keys(value);
						if (keys.length) {
							w.long(keys.length);
							for (const key of keys) {
								this.#write(w, "string", key);
								this.#write(w, schema.values!, value[key]);
							}
						}
						w.long(0);
						return;
					}
					default:
						throw new Error(`Avro: unsupported schema type ${schema.type}`);
				}
			}
		
			private #read(r: AVROReader, schema: AVROSchema): AVROValue {
				schema = this.#resolve(schema);
		
				if (Array.isArray(schema)) {
					const index = r.long();
					if (typeof index !== "number" || index < 0 || index >= schema.length)
						throw new Error("Avro: invalid union index");
					return this.#read(r, schema[index]);
				}
		
				if (typeof schema === "string") {
					switch (schema) {
						case "null": return null;
						case "boolean": {
							const b = r.byte();
							if (b > 1) throw new Error("Avro: invalid boolean");
							return b === 1;
						}
						case "int": return r.int();
						case "long": return r.long();
						case "float": return r.float();
						case "double": return r.double();
						case "string": return this.decoder.decode(r.bytes(Number(r.long())));
						case "bytes": return r.bytes(Number(r.long()));
						default: throw new Error(`Avro: unknown type ${schema}`);
					}
				}
		
				if (typeof schema.type !== "string") return this.#read(r, schema.type);
		
				switch (schema.type) {
					case "null": return null;
					case "boolean": {
						const b = r.byte();
						if (b > 1) throw new Error("Avro: invalid boolean");
						return b === 1;
					}
					case "int": return r.int();
					case "long": return r.long();
					case "float": return r.float();
					case "double": return r.double();
					case "string": return this.decoder.decode(r.bytes(Number(r.long())));
					case "bytes": return r.bytes(Number(r.long()));
					case "fixed": return r.bytes(schema.size!);
					case "enum": {
						const index = r.long();
						if (typeof index !== "number" || index < 0 || index >= schema.symbols!.length)
							throw new Error("Avro: invalid enum index");
						return schema.symbols![index];
					}
					case "record": {
						const out: OB<AVROValue> = {};
						for (const field of schema.fields!) out[field.name] = this.#read(r, field.type);
						return out;
					}
					case "array": {
						const out: AVROValue[] = [];
						let count: number | bigint;
						while ((count = r.long()) !== 0) {
							if (typeof count === "bigint") throw new Error("Avro: array block too large");
							if (count < 0) {
								count = -count;
								const blockSize = r.long();
								if (typeof blockSize !== "number" || blockSize < 0) throw new Error("Avro: invalid array block");
							}
							for (let i = 0; i < count; i++) out.push(this.#read(r, schema.items!));
						}
						return out;
					}
					case "map": {
						const out: OB<AVROValue> = {};
						let count: number | bigint;
						while ((count = r.long()) !== 0) {
							if (typeof count === "bigint") throw new Error("Avro: map block too large");
							if (count < 0) {
								count = -count;
								const blockSize = r.long();
								if (typeof blockSize !== "number" || blockSize < 0) throw new Error("Avro: invalid map block");
							}
							for (let i = 0; i < count; i++) {
								const key = this.#read(r, "string");
								out[key] = this.#read(r, schema.values!);
							}
						}
						return out;
					}
					default:
						throw new Error(`Avro: unsupported schema type ${schema.type}`);
				}
			}
		
			private #encode(value: AVROValue, branch: N): Uint8Array {
				const w = new AVROWriter();
				w.long(branch);
				this.#write(w, this.wireSchema[branch], value);
				return w.finish();
			}
		
			private #decode(bytes: Uint8Array): AVROValue {
				const r = new AVROReader(bytes);
				const branch = r.long();
				if (typeof branch !== "number" || branch < 0 || branch >= this.wireSchema.length)
					throw new Error("Avro: invalid envelope branch");
				const value = this.#read(r, this.wireSchema[branch]);
				if (!r.done()) throw new Error("Avro: trailing bytes");
				return value;
			}
		
			public encode_msg(msg: AVROValue): Uint8Array {
				if (msg instanceof Uint8Array) return msg;
				try {
					return this.#encode(msg, 0);
				} catch (err) {
					console.log("[AVRO_PIPE] Failed to encode outbound packet:", err);
					return this.#encode({
						type: "error",
						error: err instanceof Error ? err.message : "Encoding failed",
						id: String(msg?.id ?? "unknown"),
					}, 1);
				}
			}
		
			public decode_msg(raw: any): AVROValue {
				if (!(raw instanceof Uint8Array) && !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))) {
					console.error("[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
					return null;
				}
				try {
					const bytes = raw instanceof Uint8Array ? raw : new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
					return this.#decode(bytes);
				} catch (err) {
					console.error("[AVRO_PIPE] Failed to decode inbound packet:", err);
					return null;
				}
			}
		}
		z.contracts = (() => {
			const z = () => {}
			z.AVROI = new this.codecs.BINARY_AVRO({
				type: "record",
				name: "SITHMessage",
				fields: [
					{ name: "pid", type: "int" },
					{ name: "op", type: "int" },
					{ name: "value", type: ["null", "double"] },
					{ name: "payload", type: ["null", "bytes"] },
				],
			});
			return z
		})(); 
		z.BUS_ =  class DISRUPTOR_CRYPTO {
			ser(value: unknown): string {
				if (value instanceof Map) {
					return JSON.stringify({ __type: "Map", value: Array.from(value.entries()).map(([key, val]) => [this.ser(key), this.ser(val)]) });
				}
				if (value instanceof Set) {
					return JSON.stringify({ __type: "Set", value: Array.from(value.values()).map((value) => this.ser(value)) });
				}
				return typeof value === "string" ? value : JSON.stringify(value);
			}
			deser(raw: string | null): any {
				if (raw === null) return null;
				try {
					const parsed = JSON.parse(raw);
					if (parsed?.__type === "Map") {
						return new Map(parsed.value.map(([key, value]: [string, string]) => [this.deser(key), this.deser(value)]));
					}
					if (parsed?.__type === "Set") {
						return new Set(parsed.value.map((value: string) => this.deser(value)));
					}
					return parsed;
				} catch {
					return raw;
				}
			}
		
			hasNode = typeof process !== "undefined" && !!process.versions?.node;
			_buf(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
				const out = new Uint8Array(new ArrayBuffer(bytes.byteLength)) as Uint8Array<ArrayBuffer>;
				out.set(bytes);
				return out;
			}
			_hasSubtle = typeof globalThis.crypto !== "undefined" && typeof (globalThis.crypto as any).subtle !== "undefined" && typeof (globalThis.crypto as any).getRandomValues === "function";
			_nodeCrypto: any = null;
			_getNodeCrypto() {
				if (this._nodeCrypto) return this._nodeCrypto;
				if (!this.hasNode) return null;
				try {
					this._nodeCrypto = require("node:crypto");
				} catch {
					this._nodeCrypto = null;
				}
				return this._nodeCrypto;
			}
			constructor() {}
		
			// ── random bytes ────────────────────────────────────────────────────────
			random_bytes(size = 32): Uint8Array {
				if (this._hasSubtle) return globalThis.crypto.getRandomValues(new Uint8Array(size));
				const nc = this._getNodeCrypto();
				if (nc) return new Uint8Array(nc.randomBytes(size));
				// last resort — NOT cryptographically secure, but the bus will refuse
				// to init auth without real randomness, so this never protects anything.
				const out = new Uint8Array(size);
				for (let i = 0; i < size; i++) out[i] = (Math.random() * 256) | 0;
				return out;
			}
		
			// ── HMAC-SHA256 ─────────────────────────────────────────────────────────
			// Returned as hex so it matches the existing computeHmacSha256 contract.
		
			// Async version — used when WebCrypto is the only option.
			async hash_hmac_hex_async(secret: string | Uint8Array, data: string | Uint8Array): Promise<string> {
				const keyBytes = typeof secret === "string" ? this.to_utf8_bytes(secret) : secret;
				const dataBytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
		
				if (this._hasSubtle) {
					const key = await globalThis.crypto.subtle.importKey(
						"raw",
						this._buf(keyBytes), // ← wrap
						{ name: "HMAC", hash: "SHA-256" },
						false,
						["sign"],
					);
					const sig = await globalThis.crypto.subtle.sign(
						"HMAC",
						key,
						this._buf(dataBytes), // ← wrap
					);
					return this.to_hex(new Uint8Array(sig));
				}
		
				const nc = this._getNodeCrypto();
				if (nc) {
					const h = nc.createHmac("sha256", Buffer.from(keyBytes));
					h.update(Buffer.from(dataBytes));
					return h.digest("hex");
				}
		
				return "";
			}
		
			// Cache used by the sync path when only WebCrypto is available.
			// Populated by hash_hmac_hex_async's callers, read by hash_hmac_hex.
			_hmacCache = new Map<string, string>();
		
			// ── SHA-256 ─────────────────────────────────────────────────────────────
			async hash_sha256_hex(data: string | Uint8Array): Promise<string> {
				const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
		
				if (this._hasSubtle) {
					const digest = await globalThis.crypto.subtle.digest(
						"SHA-256",
						this._buf(bytes), // ← wrap
					);
					return this.to_hex(new Uint8Array(digest));
				}
		
				const nc = this._getNodeCrypto();
				if (nc) {
					const h = nc.createHash("sha256");
					h.update(Buffer.from(bytes));
					return h.digest("hex");
				}
		
				return "";
			}
		
			// ── HKDF (derive a session key from a shared secret) ────────────────────
			async derive_hkdf(ikm: Uint8Array, salt: Uint8Array, info: Uint8Array, length = 32): Promise<Uint8Array> {
				if (this._hasSubtle) {
					const key = await globalThis.crypto.subtle.importKey(
						"raw",
						this._buf(ikm), // ← wrap
						"HKDF",
						false,
						["deriveBits"],
					);
					const bits = await globalThis.crypto.subtle.deriveBits(
						{
							name: "HKDF",
							hash: "SHA-256",
							salt: this._buf(salt), // ← wrap
							info: this._buf(info), // ← wrap
						},
						key,
						length * 8,
					);
					return new Uint8Array(bits);
				}
		
				const nc = this._getNodeCrypto();
				if (nc) {
					return new Uint8Array(nc.hkdfSync("sha256", Buffer.from(ikm), Buffer.from(salt), Buffer.from(info), length));
				}
		
				return new Uint8Array(length);
			}
		
			// ── AES-GCM (the bus's symmetric p encryption) ────────────────────
			async encrypt_aes_gcm(key: Uint8Array, iv: Uint8Array, data: Uint8Array): Promise<Uint8Array> {
				if (this._hasSubtle) {
					const k = await globalThis.crypto.subtle.importKey(
						"raw",
						this._buf(key), // ← wrap
						"AES-GCM",
						false,
						["encrypt"],
					);
					const ct = await globalThis.crypto.subtle.encrypt(
						{ name: "AES-GCM", iv: this._buf(iv) }, // ← wrap
						k,
						this._buf(data), // ← wrap
					);
					return new Uint8Array(ct);
				}
		
				const nc = this._getNodeCrypto();
				if (nc) {
					const cipher = nc.createCipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
					const ct = Buffer.concat([cipher.update(Buffer.from(data)), cipher.final()]);
					const tag = cipher.getAuthTag();
					// append tag, matching WebCrypto's output format
					return new Uint8Array(Buffer.concat([ct, tag]));
				}
		
				return data;
			}
		
			async decrypt_aes_gcm(key: Uint8Array, iv: Uint8Array, ct: Uint8Array): Promise<Uint8Array> {
				if (this._hasSubtle) {
					const k = await globalThis.crypto.subtle.importKey(
						"raw",
						this._buf(key), // ← wrap
						"AES-GCM",
						false,
						["decrypt"],
					);
					const pt = await globalThis.crypto.subtle.decrypt(
						{ name: "AES-GCM", iv: this._buf(iv) }, // ← wrap
						k,
						this._buf(ct), // ← wrap
					);
					return new Uint8Array(pt);
				}
		
				const nc = this._getNodeCrypto();
				if (nc) {
					// split off the 16-byte tag from the end
					const tag = ct.slice(ct.length - 16);
					const body = ct.slice(0, ct.length - 16);
					const decipher = nc.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
					decipher.setAuthTag(Buffer.from(tag));
					const pt = Buffer.concat([decipher.update(Buffer.from(body)), decipher.final()]);
					return new Uint8Array(pt);
				}
		
				return ct;
			}
		
			// ── Encoding ────────────────────────────────────────────────────────────
			to_utf8_bytes(str: string): Uint8Array {
				if (typeof TextEncoder !== "undefined") return new TextEncoder().encode(str);
				const nc = this._getNodeCrypto();
				if (nc) return new Uint8Array(Buffer.from(str, "utf8"));
				// last resort
				const out = new Uint8Array(str.length);
				for (let i = 0; i < str.length; i++) out[i] = str.charCodeAt(i) & 0xff;
				return out;
			}
		
			to_hex(data: Uint8Array): string {
				if (typeof Buffer !== "undefined") return Buffer.from(data).toString("hex");
				let out = "";
				for (let i = 0; i < data.length; i++) out += data[i].toString(16).padStart(2, "0");
				return out;
			}
		
			from_hex(hex: string): Uint8Array {
				if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(hex, "hex"));
				const out = new Uint8Array(hex.length / 2);
				for (let i = 0; i < hex.length; i += 2) out[i / 2] = parseInt(hex.slice(i, i + 2), 16);
				return out;
			}
		
			to_base64(data: Uint8Array): string {
				if (typeof Buffer !== "undefined") return Buffer.from(data).toString("base64");
				let bin = "";
				for (let i = 0; i < data.length; i++) bin += String.fromCharCode(data[i]);
				return btoa(bin);
			}
		
			from_base64(str: string): Uint8Array {
				if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(str, "base64"));
				const bin = atob(str);
				const out = new Uint8Array(bin.length);
				for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
				return out;
			}
		
			// ── Constants ───────────────────────────────────────────────────────────
			ready(): boolean {
				return this._hasSubtle || this._getNodeCrypto() !== null;
			}
		
			secure_equal(a: Uint8Array, b: Uint8Array): boolean {
				let diff = a.length ^ b.length;
				const len = Math.max(a.length, b.length);
				for (let i = 0; i < len; i++) diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
				return diff === 0;
			}
		
			hash_hmac_hex(secret: string | Uint8Array, data: string | Uint8Array): string {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("Synchronous HMAC requires Node crypto");
				const key = typeof secret === "string" ? this.to_utf8_bytes(secret) : secret;
				const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
				return nc.createHmac("sha256", Buffer.from(key)).update(Buffer.from(bytes)).digest("hex");
			}
		
			hash_hmac(secret: string | Uint8Array, data: string | Uint8Array): string {
				return this.hash_hmac_hex(secret, data);
			}
		
			hash_sha256(data: string | Uint8Array): string {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("Synchronous hashing requires Node crypto");
				const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
				return nc.createHash("sha256").update(Buffer.from(bytes)).digest("hex");
			}
		
			derive_key(password: string, salt: Uint8Array, iterations = 310_000, length = 32): Uint8Array {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("Synchronous PBKDF2 requires Node crypto");
				return new Uint8Array(nc.pbkdf2Sync(Buffer.from(this.to_utf8_bytes(password)), Buffer.from(salt), iterations, length, "sha256"));
			}
		
			symmetric_encrypt(p: string, key: Uint8Array): { ciphertext: string; nonce: string } {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("Synchronous encryption requires Node crypto");
				if (key.length !== 32) throw new Error("AES-256-GCM requires a 32-byte key");
		
				const nonce = nc.randomBytes(12);
				const cipher = nc.createCipheriv("aes-256-gcm", Buffer.from(key), nonce);
				const ciphertext = Buffer.concat([cipher.update(p, "utf8"), cipher.final(), cipher.getAuthTag()]);
		
				return {
					ciphertext: this.to_base64(new Uint8Array(ciphertext)),
					nonce: this.to_base64(new Uint8Array(nonce)),
				};
			}
		
			symmetric_decrypt(p: { ciphertext: string; nonce: string }, key: Uint8Array): string {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("Synchronous decryption requires Node crypto");
				if (key.length !== 32) throw new Error("AES-256-GCM requires a 32-byte key");
		
				const ciphertext = this.from_base64(p.ciphertext);
				const nonce = this.from_base64(p.nonce);
				if (nonce.length !== 12 || ciphertext.length < 16) throw new Error("Invalid AES-GCM payload");
		
				const body = ciphertext.slice(0, -16);
				const tag = ciphertext.slice(-16);
				const decipher = nc.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(nonce));
				decipher.setAuthTag(Buffer.from(tag));
		
				return Buffer.concat([decipher.update(Buffer.from(body)), decipher.final()]).toString("utf8");
			}
			generate_key_pair(_type: string): { publicKey: Uint8Array; secretKey: Uint8Array } {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("Synchronous key generation requires Node crypto");
		
				const pair = nc.generateKeyPairSync("ed25519");
				return {
					publicKey: new Uint8Array(pair.publicKey.export({ type: "spki", format: "der" })),
					secretKey: new Uint8Array(pair.privateKey.export({ type: "pkcs8", format: "der" })),
				};
			}
		
			pqc_generate_KEM_keys(): { publicKey: Uint8Array; secretKey: Uint8Array; algorithm: string } {
				const nc = this._getNodeCrypto();
				if (!nc) throw new Error("PQC key generation requires a supported Node crypto provider");
		
				const pair = nc.generateKeyPairSync("ml-kem-768");
				return {
					publicKey: new Uint8Array(pair.publicKey.export({ type: "spki", format: "der" })),
					secretKey: new Uint8Array(pair.privateKey.export({ type: "pkcs8", format: "der" })),
					algorithm: "ML-KEM-768",
				};
			}
		}
		z.BUS_CRYPTO = this.codecs.BUS_
		z.avro = new (this.codecs.BINARY_AVRO)(this);
		return z
	})(); 
	
	public r2x = new ()(this);
}
// #endregion
// #endregion
//                                                                                                                [RAVEN] STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export let _root: ComponentInstance = {
	id: "THE_WIZARD_ROOT_REG",
	hooks: new Map(),
	unmount: new Set(),
	contextProviders: new Map(),
};
const queues: Record<Priority, Map<symbol | string, ScheduledTask[]>> = {
	immediate: new Map(),
	"user-blocking": new Map(),
	normal: new Map(),
	background: new Map(),
	idle: new Map(),
};
const nextPow2 = (n: number) => 1 << Math.ceil(Math.log2(Math.max(2, n)));
class Disruptor<P> {
	private readonly mask: number;
	private readonly buffer: Slot<P>[];
	public cursor = -1;

	constructor(public readonly capacity: number) {
		const size = nextPow2(capacity);
		this.mask = size - 1;
		this.buffer = Array.from({ length: size }, () => ({ sequence: -1, key: undefined, payload: undefined, tombstone: false }));
	}

	/** The slot that WILL be claimed next — peek before you commit to it. */
	peekNext(): Slot<P> {
		return this.buffer[(this.cursor + 1) & this.mask];
	}

	publish(key: unknown, payload: P | undefined, tombstone = false): number {
		const seq = this.cursor + 1;
		const slot = this.buffer[seq & this.mask];
		slot.sequence = seq;
		slot.key = key;
		slot.payload = payload;
		slot.tombstone = tombstone;
		this.cursor = seq;
		return seq;
	}

	read(seq: number): Slot<P> | undefined {
		if (seq < 0 || seq > this.cursor) return undefined;
		const slot = this.buffer[seq & this.mask];
		return slot.sequence === seq ? slot : undefined;
	}
}
function assertEncodable(value: unknown): void {
	if (typeof value === "function") {
		throw new TypeError("[RAVEN] state values must be plain data — functions can't be stored here.");
	}
}
function encode<T>(value: T): T {
	assertEncodable(value);
	if (value === null || typeof value !== "object") return value;
	return (typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value))) as T;
}
function decode<T>(value: T): T {
	if (value === null || typeof value !== "object") return value;
	return (typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value))) as T;
}
class Lane<V> {
	private ring: Disruptor<V>;
	private writes = 0;
	constructor(public readonly capacity: number) {
		this.ring = new Disruptor<V>(capacity);
	}
	get isFull(): boolean {
		return this.writes >= this.capacity;
	}
	get pressure(): number {
		return this.writes / this.capacity;
	}
	write(key: unknown, value: V): number {
		this.writes++;
		return this.ring.publish(key, value);
	}
	readAt(seq: number): V | undefined {
		const slot = this.ring.read(seq);
		return slot && !slot.tombstone ? slot.payload : undefined;
	}
	/** Day 0 gets physically rewritten here — we don't tombstone the old
	 *  ring, we just stop referencing it. The old Disruptor object (and
	 *  everything it held) is garbage the moment nothing points at it. */
	reset(): void {
		this.writes = 0;
		this.ring = new Disruptor<V>(this.capacity);
	}
}
class MTracer<K = any, V = any> {
	private lanes: Lane<V>[];
	private index = new Map<K, IndexEntry>();
	private dial = 0;

	/** The tracer particle itself — ticks once per completed lap, forever.
	 *  Nobody reads Date.now() anywhere in this class. */
	public revolution = 0;

	constructor(
		private readonly laneCapacity = 8192,
		private readonly dialSize = 7, // the "7 day" window — your dial
	) {
		this.lanes = Array.from({ length: dialSize }, () => new Lane<V>(laneCapacity));
	}

	private get activeLane(): Lane<V> {
		return this.lanes[this.dial];
	}

	private advanceDialIfFull(): void {
		if (!this.activeLane.isFull) return;
		this.revolution++;
		this.dial = (this.dial + 1) % this.dialSize;
		// We've swept back around to the lane that was "day 0". Reset it —
		// this is the rewrite — then drop any index entries still pointing
		// at it so get() can't hand back memory that no longer belongs to them.
		this.lanes[this.dial].reset();
		for (const [key, entry] of this.index) {
			if (entry.lane === this.dial) this.index.delete(key);
		}
	}

	set(key: K, value: V): void {
		this.advanceDialIfFull();
		const seq = this.activeLane.write(key, encode(value));
		this.index.set(key, { lane: this.dial, seq });
	}

	get(key: K): V | undefined {
		const entry = this.index.get(key);
		if (!entry) return undefined;
		const payload = this.lanes[entry.lane].readAt(entry.seq);
		if (payload === undefined) {
			this.index.delete(key);
			return undefined;
		}
		return decode(payload);
	}

	has(key: K): boolean {
		return this.index.has(key) && this.get(key) !== undefined;
	}
	delete(key: K): boolean {
		return this.index.delete(key);
	}
	clear(): void {
		this.index.clear();
		this.lanes.forEach((l) => l.reset());
		this.dial = 0;
	}
	entries(): [K, V][] {
		const out: [K, V][] = [];
		for (const key of this.index.keys()) {
			const v = this.get(key);
			if (v !== undefined) out.push([key, v]);
		}
		return out;
	}

	/** Free backpressure: how close the active lane is to ticking the dial
	 *  over. A producer driving this toward 1 while nobody's reading is
	 *  exactly "queue depth" — except no timer instrumented it. */
	get pressure(): number {
		return this.activeLane.pressure;
	}
}
class STracer<V = any> {
	private lanes: Lane<true>[];
	private index = new Map<V, IndexEntry>(); // member -> location, SameValueZero like native Set
	private dial = 0;
	public revolution = 0;

	constructor(
		private readonly laneCapacity = 8192,
		private readonly dialSize = 7,
	) {
		this.lanes = Array.from({ length: dialSize }, () => new Lane<true>(laneCapacity));
	}

	private get activeLane(): Lane<true> {
		return this.lanes[this.dial];
	}

	private advanceDialIfFull(): void {
		if (!this.activeLane.isFull) return;
		this.revolution++;
		this.dial = (this.dial + 1) % this.dialSize;
		this.lanes[this.dial].reset();
		for (const [member, entry] of this.index) if (entry.lane === this.dial) this.index.delete(member);
	}

	add(value: V): this {
		if (this.index.has(value)) return this; // Set semantics: no dupes, no wasted slot
		this.advanceDialIfFull();
		const seq = this.activeLane.write(value, true); // no encode() — member is never cloned
		this.index.set(value, { lane: this.dial, seq });
		return this;
	}

	has(value: V): boolean {
		const entry = this.index.get(value);
		return !!entry && this.lanes[entry.lane].readAt(entry.seq) !== undefined;
	}

	delete(value: V): boolean {
		return this.index.delete(value);
	}
	clear(): void {
		this.index.clear();
		this.lanes.forEach((l) => l.reset());
		this.dial = 0;
	}
	get size(): number {
		return this.index.size;
	}

	forEach(cb: (value: V) => void): void {
		for (const member of this.index.keys()) if (this.has(member)) cb(member);
	}

	[Symbol.iterator](): Iterator<V> {
		const live: V[] = [];
		for (const member of this.index.keys()) if (this.has(member)) live.push(member);
		return live[Symbol.iterator]();
	}

	get pressure(): number {
		return this.activeLane.pressure;
	}
}
class OTracer<V = any> {
	private lanes: Lane<V>[];
	private index = new Map<string, IndexEntry>();
	private dial = 0;
	public revolution = 0;
	private static readonly BLOCKED = new Set(["__proto__", "constructor", "prototype"]);

	constructor(
		private readonly laneCapacity = 256,
		private readonly dialSize = 7,
		private readonly skipEncode = false,
	) {
		this.lanes = Array.from({ length: dialSize }, () => new Lane<V>(laneCapacity));

		const handler: ProxyHandler<any> = {
			get: (_t, prop) => {
				if (typeof prop === "symbol") return (this as any)[prop];
				if (OTracer.BLOCKED.has(prop)) return undefined;
				return this.readKey(prop);
			},
			set: (_t, prop, value) => {
				if (typeof prop === "symbol") return false;
				if (OTracer.BLOCKED.has(prop)) {
					throw new TypeError(`[RAVEN] "${prop}" is not a writable key on this store.`);
				}
				this.writeKey(prop, value);
				return true;
			},
			has: (_t, prop) => {
				if (typeof prop === "symbol") return false;
				return this.readKey(prop as string) !== undefined;
			},
			deleteProperty: (_t, prop) => {
				if (typeof prop === "symbol") return false;
				return this.index.delete(prop as string);
			},
			ownKeys: () => [...this.index.keys()].filter((k) => this.readKey(k) !== undefined),
			getOwnPropertyDescriptor: (_t, prop) => {
				if (typeof prop === "symbol") return undefined;
				const value = this.readKey(prop as string);
				if (value === undefined) return undefined;
				return { value, writable: true, enumerable: true, configurable: true };
			},
		};

		// eslint-disable-next-line no-constructor-return
		return new Proxy(Object.create(null), handler);
	}

	private get activeLane(): Lane<V> {
		return this.lanes[this.dial];
	}

	private advanceDialIfFull(): void {
		if (!this.activeLane.isFull) return;
		this.revolution++;
		this.dial = (this.dial + 1) % this.dialSize;
		this.lanes[this.dial].reset();
		for (const [key, entry] of this.index) if (entry.lane === this.dial) this.index.delete(key);
	}

	private readKey(key: string): V | undefined {
		const entry = this.index.get(key);
		if (!entry) return undefined;
		const payload = this.lanes[entry.lane].readAt(entry.seq);
		if (payload === undefined) {
			this.index.delete(key);
			return undefined;
		}
		return this.skipEncode ? payload : decode(payload);
	}

	private writeKey(key: string, value: V): void {
		this.advanceDialIfFull();
		const payload = this.skipEncode ? value : encode(value);
		const seq = this.activeLane.write(key, payload);
		this.index.set(key, { lane: this.dial, seq });
	}
}
class RAVEN {
	public hub: DEATH_STAR;
	public t: A = {};

	constructor(hub: DEATH_STAR = null) {
		this.hub = hub;
		this.r = hub.r;
		this.m = hub.m;
		this.s = hub.s;
		this.k = hub.k;
	}

	public online = false;
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	public u = undefined;
	public uuuu = [this.u, this.u, this.u, this.u];
	public UUUU = [this.s, this.m, this.r];
	public uuu = [this.u, this.u, this.u];
	public UUU = [this.s, this.m, this.r];
	public uu = [this.u, this.u];
	public UU = [this.m, this.r];

	public batchDepth = 0;
	public openScopes = new Set<BatchScope>();
	public dirty = new Set<FlushCallback>();
	public _flushScheduled = false;
	public _adapter: RuntimeAdapter | null = null;
	public _currentHookState: HookState | null = null;
	public _currentComponent: ComponentInstance | null = null;
	public PRIORITY_ORDER: Priority[] = ["immediate", "user-blocking", "normal", "background", "idle"];
	public GLOBAL_SCOPE = Symbol("global-scope");
	public scopeCursors: Record<Priority, number> = { immediate: 0, "user-blocking": 0, normal: 0, background: 0, idle: 0 };
	public currentScope: symbol | string = this.GLOBAL_SCOPE;
	public loopScheduled = false;
	public _subscribers = new Map<string, Set<() => void>>();
	public _externalSubscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
	public _components = new Map<string, ComponentInstance>();
	public _pendingNotifications = new Set<string>();
	public externalSubscribers = new Set<() => void>();
	public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;

	public onFlush(cb: FlushCallback): void { this.dirty.add(cb); this.scheduleDrain();	}
	public scheduleDrain(): void {
		if (this._flushScheduled) return;
		this._flushScheduled = true;
		queueMicrotask(this.drain);
	}
	public drain(): void {
		this._flushScheduled = false;
		if (this.dirty.size === 0) return;

		// An explicit openBatch() scope is still open (deliberately spanning an await) —
		// don't drop the pending callbacks, just wait for closeBatch() to let them through.
		if (this.openScopes.size > 0) {
			this.scheduleDrain();
			return;
		}

		const callbacks = this.dirty;
		this.dirty = new Set();
		for (const cb of callbacks) cb();
	}
	public microtask(fn: FlushCallback): void {	 queueMicrotask(fn); }
	public batch<T>(fn: () => T): T {
		this.batchDepth++;
		try {
			return fn();
		} finally {
			this.batchDepth--;
			if (this.batchDepth === 0) {
				this.flush();
			}
		}
	}
	public openBatch(): BatchScope {
		const scope: BatchScope = Symbol("batch-scope");
		this.openScopes.add(scope);
		return scope;
	}
	public closeBatch(scope: BatchScope): void {
		this.openScopes.delete(scope);
		if (this.openScopes.size === 0 && this.dirty.size > 0) { this.scheduleDrain(); }
	}
	public isBatching(): boolean { return this.batchDepth > 0 || this.openScopes.size > 0;	}
	public flushSync(): void {
		if (this.openScopes.size > 0) return;
		this._flushScheduled = false;
		if (this.dirty.size === 0) return;
		const callbacks = this.dirty;
		this.dirty = new Set();
		for (const cb of callbacks) cb();
	}
	public flush<T>(fn?: () => T): T | void {
		if (!fn) { this.flushSync(); return; }
		this.batchDepth++;
		let result: T;
		try { result = fn(); } 
		finally { this.batchDepth--; }
		this.flushSync();
		return result;
	}
	public enqueue(task: ScheduledTask): void {
		if (task.priority === "immediate") {
			this.microtask(() => this.runOne(task));
			return;
		}
		const tier = queues[task.priority];
		if (!tier.has(task.scope)) tier.set(task.scope, []);
		tier.get(task.scope)!.push(task);
		this.scheduleLoop();
	}
	public scheduleLoop(): void {
		if (this.loopScheduled) return;
		this.loopScheduled = true;
		setImmediate(this.drainOne);
	}
	public pickNext(): ScheduledTask | undefined {
		for (const p of this.PRIORITY_ORDER) {
			if (p === "immediate") continue;
			const tier = queues[p];
			if (tier.size === 0) continue;
			const scopes = Array.from(tier.keys());
			const idx = this.scopeCursors[p] % scopes.length;
			const scope = scopes[idx];
			const list = tier.get(scope)!;
			const task = list.shift()!;
			if (list.length === 0) tier.delete(scope);
			this.scopeCursors[p] = idx + 1;
			return MaybePromise(task);
		}
		return undefined;
	}
	public runOne(task: ScheduledTask): void {
		if (task.cancelled || task.signal?.aborted) return;
		const prev = this.currentScope;
		this.currentScope = task.scope;
		try { task.fn(); } 
		finally { this.currentScope = prev; }
	}
	public drainOne(): void {
		this.loopScheduled = false;
		const task = this.pickNext();
		if (task) this.runOne(task);
		if (Object.values(queues).some((m) => m.size > 0)) this.scheduleLoop();
	}
	public schedule(fn: () => void, opts: { priority?: Priority; scope?: string; signal?: AbortSignal } = {}): Task {
		const task: ScheduledTask = { priority: opts.priority ?? "normal", scope: opts.scope ?? this.currentScope, fn, signal: opts.signal, cancelled: false };
		// signals.chains().sc.addEventListener('abort', () => { task.cancelled = true })
		this.enqueue(task);
		const self = this;
		return {
			cancel(): void { task.cancelled = true; },
			reprioritize(next: Priority): void {
				if (task.cancelled || next === task.priority) return;
				const old = queues[task.priority].get(task.scope);
				if (old) {
					const i = old.indexOf(task);
					if (i !== -1) old.splice(i, 1);
					if (old.length === 0) queues[task.priority].delete(task.scope);
				}
				task.priority = next;
				self.enqueue(task);
			},
		};
	}
	public withRequestScope<T>(requestId: string, fn: () => T): T {
		const prev = this.currentScope;
		this.currentScope = requestId;
		try { return fn(); }
		 finally { this.currentScope = prev; }
	}
	public yieldToScheduler(): Promise<void> {
		return new Promise((resolve) => { this.schedule(() => resolve(), { priority: "normal", scope: this.currentScope as string });		});
	}
	public scheduleChunked(work: Iterable<() => void> | Iterator<() => void>, opts: { priority?: Priority; scope?: string; chunkBudgetMs?: number } = {}): Task {
		const iterator: Iterator<() => void> = Symbol.iterator in (work as A) ? (work as Iterable<() => void>)[Symbol.iterator]() : (work as Iterator<() => void>);
		const chunkBudgetMs = opts.chunkBudgetMs ?? 5;
		const scope = opts.scope ?? (this.currentScope as string);
		let cancelled = false;
		let priority = opts.priority ?? "normal";
		const self = this;

		function runChunk(): void {
			if (cancelled) return;
			const start = performance.now();
			while (true) {
				const { value: step, done } = iterator.next();
				if (done) return;
				step();
				const elapsed = performance.now() - start;
				if (elapsed >= chunkBudgetMs) break;
			}
			if (!cancelled) self.schedule(runChunk, { priority, scope });
		}

		this.schedule(runChunk, { priority, scope });
		return {
			cancel(): void {
				cancelled = true;
			},
			reprioritize(next: Priority): void {
				priority = next;
			},
		};
	}
	// SAURON
	_globalNotifyCache = new WeakMap<A, Map<A, () => void>>();
	_notifyCache = new WeakMap<Map<A, Set<(value: A) => void>>, Map<A, () => void>>();
	public scheduleGlobalNotify(key: string | symbol, meta: A, opts: ScopeOptions): void {
		let cache = this._globalNotifyCache.get(meta);
		if (!cache) {
			cache = new Map();
			this._globalNotifyCache.set(meta, cache);
		}
		let fn = cache.get(key);
		if (!fn) {
			fn = () => {
				cache!.delete(key);
				const finalVal = this.store.get(key, opts);
				const waiters = meta._waiters.get(key);
				if (waiters) {
					for (const resolveFn of waiters) {
						try { resolveFn(finalVal); } 
						catch (err) { console.error(`[Sauron Waiter Error]:`, err); }
					}
					meta._waiters.delete(key);
				}
				for (const listener of meta._globalListeners) {
					try { listener(key, finalVal); } 
					catch (err) { console.error(`[Sauron Global Listener Error]:`, err); }
				}
			};
			cache.set(key, fn);
		}
		this.onFlush(fn);
	}
	public scheduleNotify(key: A, _listeners: Map<A, Set<(value: A) => void>>, _state: A): void {
		let cache = this._notifyCache.get(_listeners);
		if (!cache) {
			cache = new Map();
			this._notifyCache.set(_listeners, cache);
		}
		let fn = cache.get(key);
		if (!fn) {
			fn = () => {
				cache!.delete(key);
				const val = _state.get(key);
				_listeners.get(key)?.forEach((l) => l(val));
			};
			cache.set(key, fn);
		}
		this.onFlush(fn);
	}
	public store = {
		get: <T = A,>(key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): T | undefined => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;

			if (_state.has(key)) return MaybePromise(_state.get(key));
			if (_persistKeys.has(key) && _persistent.has(key)) {
				const v = _persistent.get(key);
				_state.set(key, v);
				return MaybePromise(v);
			}
			return undefined;
		},
		set: <T = A,>(key: A, value: T | ((prev: T | undefined) => T), opts: { triggerRender?: boolean; cap?: number } & ScopeOptions = { s: this.s, m: this.m, r: this.r, triggerRender: true, cap: 200 }) => {
			const { triggerRender = true, s, m, r, cap } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			const _listeners = this.hub.wr._listeners;
			const prev = _state.get(key);
			const next = typeof value === "function" ? (value as (prev: T | undefined) => T)(prev) : value;
			if (Object.is(prev, next)) return;
			// TIME TRAVEL

			_state.set(key, next);
			if (_persistKeys.has(key)) _persistent.set(key, next);
			if (triggerRender) this.scheduleGlobalNotify(key, this.hub.wr, opts);
			if (triggerRender) this.scheduleNotify(key, _listeners, _state);
			return MaybePromise(next as T);
		},
		subscribe: <T = A,>(key: A, listener: (value: T) => void, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): (() => void) => {
			const { s, m, r } = opts;
			const _listeners = this.hub.wr._listeners;

			if (!_listeners.has(key)) _listeners.set(key, new Set());
			_listeners.get(key)!.add(listener);
			return () => {
				_listeners.get(key)?.delete(listener);
				if (_listeners.get(key)?.size === 0) _listeners.delete(key);
			};
		},
		delete: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): boolean => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			const _listeners = this.hub.wr._listeners;

			const existed = _state.delete(key);
			if (_persistKeys.has(key)) {
				_persistKeys.delete(key);
				_persistent.delete(key);
			}
			_listeners.get(key)?.forEach((l) => l(undefined));
			_listeners.delete(key);
			return MaybePromise(existed);
		},
		entries: <T = A,>(opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): [A, T][] => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			return MaybePromise(Array.from(_state.entries()));
		},
		persist: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _persistKeys = this.hub.wr._persistent_keys;
			_persistKeys.set(key, true);
		},
		unpersist: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			_persistKeys.delete(key);
			_persistent.delete(key);
		},
		has: (key: A, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): boolean => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;

			return _state.has(key) || (_persistKeys.has(key) && _persistent.has(key));
		},
		clear: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _state = this.hub.wr._gt;
			const _listeners = this.hub.wr._listeners;
			_state.clear();
			_listeners.clear();
		},
		clearPersistent: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const _persistent = this.hub.wr._persistent;
			const _persistKeys = this.hub.wr._persistent_keys;
			_persistent.clear();
			_persistKeys.clear();
		},
		_emit: (key: string | symbol, s = this.s, m = this.m, r = this.r): void => {
			const opts = { s, m, r };
			const _state = this.hub.wr._gt;
			const _listeners = this.hub.wr._listeners;
			const _waiters = this.hub.wr;
			this.scheduleNotify(key, _listeners, _state);
			this.scheduleGlobalNotify(key, _waiters, opts);
		},
		// subscribe, delete, entries, persist, unpersist, has, clear,
		// clearPersistent, _emit — all unchanged
	};
	public state: SauronState = {
		get: <T = A,>(key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): T | undefined => {
			return MaybePromise(this.store.get<T>(key, opts) as T);
		},
		set: <T = A,>(key: string | symbol, value: T | ((prev: T | undefined) => T), opts: { triggerRender?: boolean } & ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r, triggerRender = true } = opts;
			this.store.set(key, value, { triggerRender, s, m, r });
			return MaybePromise(value as T);
		},
		delete: (key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): boolean => {
			const { s, m, r } = opts;
			const meta = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
			const deleted = this.store.delete(key, opts);
			if (deleted) {
				for (const listener of meta._globalListeners) {
					try {
						listener(key, undefined);
					} catch (err) {
						console.error(`[Sauron Global Listener Error]:`, err);
					}
				}
			}
			return deleted;
		},
		getByPrefix: <T = A,>(prefix: string, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): { key: string; value: T }[] => {
			return this.store
				.entries<T>(opts)
				.filter(([key]) => typeof key === "string" && key.startsWith(prefix))
				.map(([key, value]) => ({ key: key as string, value }));
		},
		getAll: <T = A,>(opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): Record<string | symbol, T> => {
			const result: Record<string | symbol, T> = {};
			for (const [key, value] of this.store.entries<T>(opts)) {
				result[key] = value;
			}
			return result;
		},
		subscribe: <T = A,>(key: string | symbol, listener: (value: T) => void, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): (() => void) => {
			return this.store.subscribe<T>(key, listener, opts);
		},
		subscribeAll: (listener: (key: string | symbol, value: A) => void, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): (() => void) => {
			const { s, m, r } = opts;
			const meta = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
			meta._globalListeners.add(listener);
			return () => {
				meta._globalListeners.delete(listener);
			};
		},
		clear: (opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): void => {
			const { s, m, r } = opts;
			const meta = this.hub.wr;
			this.store.clear(opts);
			meta._waiters.clear();
			const gl = this.hub.wr.map("_globalListeners_raven", this.m, this.r);
			gl.clear();
		},
		waitFor: <T = A,>(key: string | symbol, opts: ScopeOptions = { s: this.s, m: this.m, r: this.r }): Promise<T> => {
			const { s, m, r } = opts;
			const meta = this.hub.wr;

			// If it already exists, resolve immediately
			const existing = this.state.get<T>(key, opts);
			if (existing !== undefined) return Promise.resolve(existing);

			return new Promise<T>((resolve) => {
				if (!meta._waiters.has(key)) meta._waiters.set(key, new Set());
				meta._waiters.get(key)!.add(resolve as A);
			});
		},
	};
	public _devRecord: ((componentId: string, hookName: string, sauronId: string, key: string, value: A) => void) | null = null;
	public devRecord(componentId: string, hookName: string, sauronId: string, key: string, value: A) {
		return this._devRecord?.(componentId, hookName, sauronId, key, value);
	}
	public registerDevRecorder(fn: typeof this._devRecord) { this._devRecord = fn;	}
	public rerender = (key?: string): void => {
		if (key !== undefined) {
			//   const hs = graphHookStates.get(key);
			// if (hs) notifyHook(`${GRAPH_BRIDGE_COMPONENT_ID}:${key}`);
			this.notifyHook(key);
		} else {
			this.flushSync();
		}
	};
	public flushHookKey = (key: string) => {
		const [componentId] = key.split(":");
		/**
		 * Store/global synthetic hook keys do not map to a component hook state.
		 * They simply notify subscribers.
		 */
		if (componentId === "store") {
			this._subscribers.get(key)?.forEach((callback: A) => {
				if (typeof callback !== "function") return;
				callback();
				this.onFlush(callback);
			});
			return;
		}

		const component = this._components.get(componentId) ?? _root;
		const hook = component?.hooks.get(key);

		/**
		 * Prefer direct DOM binding updates where bindings exist.
		 */
		if (hook?.bindings && hook.bindings.size > 0) {
			for (const binding of hook.bindings) {
				if (!binding.active) continue;

				binding.update(binding.node, hook.value);
			}

			return;
		}
		this._subscribers.get(key)?.forEach((callback: A) => {
			if (typeof callback !== "function") return;

			callback();
			this.onFlush(callback);
		});
	};
	public _flushHookNotifications = () => {
		this._flushScheduled = false;
		const keys = Array.from(this._pendingNotifications);
		this._pendingNotifications.clear();

		for (const key of keys) {
			this.flushHookKey(key);
		}
	};
	public getCurrentComponent = (): ComponentInstance => {
		if (isReact === true && IS_THE_WIZARD === false) {
			if (this._currentComponent) return this._currentComponent;
			return _root;
		} else {
			if (!init_hooks_context) {
				this.set("_root", _root, true, this.s, this.m, this.r);
				init_hooks_context = true;
			}
			const updated_ctx = this.get("_root", this.s, this.m, this.r) as ComponentInstance;
			if (updated_ctx && updated_ctx.id === this.r) return updated_ctx;
			else return _root;
		}
	};
	public getCurrentComponentCell = (): ComponentInstance => {
		if (isReact === true && IS_THE_WIZARD === false) {
			if (this._currentComponent) return this._currentComponent;
			return _root;
		} else {
			if (!init_hooks_context) {
				this.hub.bear.cell.set("_root", _root, true, this.s, this.m, this.r);
				init_hooks_context = true;
			}
			const updated_ctx = this.hub.bear.cell("_root", this.s, this.m, this.r) as ComponentInstance;
			if (updated_ctx && updated_ctx.id === this.r) return updated_ctx;
			else return _root;
		}
	};
	public unmountComponent(id: string) {
		const instance = this._components.get(id);
		if (!instance) return;
		for (const dispose of instance.unmount) {
			try { dispose(); } 
			catch (error) { console.error(`[THE_CLOUD][UNMOUNT] cleanup failed for "${id}":`, error); }
		}
		instance.unmount.clear();
		for (const hook of instance.hooks.values()) {
			try { hook.cleanup?.(); }
			catch (error) {  console.error(`[THE_CLOUD][UNMOUNT] hook cleanup failed for "${id}":`, error); }
		}
		instance.hooks.clear();
		this._components.delete(id);
	}
	public mountComponent(id: string, renderFn?: () => void): ComponentInstance {
		const instance: ComponentInstance = { id, hooks: new Map(), unmount: new Set(), render: renderFn, };
		this._components.set(id, instance);
		return instance;
	}
	public hmrPreserve(): Map<string, Map<string, A>> {
		const snapshot = new Map<string, Map<string, A>>();
		this._components.forEach((instance, id) => {
			const hooks = new Map<string, A>();
			instance.hooks.forEach((state, key) => { hooks.set(key, state.value); });
			snapshot.set(id, hooks);
		});
		return snapshot;
	}
	public hmrRestore(snapshot: Map<string, Map<string, A>>) {
		snapshot.forEach((hooks, id) => {
			const instance = this._components.get(id);
			if (!instance) return;
			hooks.forEach((value, key) => {
				const state = instance.hooks.get(key);
				if (state) state.value = value;
			});
		});
	}
	public subscribeToHook(key: string, cb: () => void): () => void {
		if (!this._subscribers.has(key)) this._subscribers.set(key, new Set());
		this._subscribers.get(key)!.add(cb);
		return () => {
			this._subscribers.get(key)?.delete(cb);
			if (this._subscribers.get(key)?.size === 0) this._subscribers.delete(key);
		};
	}
	public notifyHook(key: string) {
		this._pendingNotifications.add(key);
		if (!this._flushScheduled) {
			this._flushScheduled = true;
			queueMicrotask(this._flushHookNotifications);
		}
	}
	public endHookDependencyCapture() { this._currentHookState = null;	}
	public beginHookDependencyCapture(hook: HookState) { this._currentHookState = hook;	}
	public registerDOMBinding(node: Node, update: (node: Node, value: A) => void) {
		const hook = this._currentHookState;
		if (!hook) return;
		const binding = { node, update, active: true };
		if (!hook.bindings) { hook.bindings = new Set(); }
		hook.bindings.add(binding);
		const anyNode = node as A;
		if (!anyNode.__sauron_bindings__) { anyNode.__sauron_bindings__ = new Set(); }
		anyNode.__sauron_bindings__.add(hook);
		return () => {
			if (!binding.active) return;
			binding.active = false;
			hook.bindings?.delete(binding);
			anyNode.__sauron_bindings__?.delete(hook);
		};
	}
	public setCurrentComponent(instance: ComponentInstance | null) { this._currentComponent = instance;	}
	public registerDOMRef(name: string, node: Node) {
		if (!this._currentHookState) return;
		if (!this._currentHookState.domRefs) this._currentHookState.domRefs = new Map();
		this._currentHookState.domRefs.set(name, node);
	}
	public getDOMRef(name: string): Node | undefined {
		if (!this._currentHookState || !this._currentHookState.domRefs) return undefined;
		return this._currentHookState.domRefs.get(name);
	}
	public registerAdapter(adapter: RuntimeAdapter) { this._adapter = adapter;	}
	public _reactHookCursor = 0;
	public _currentRenderInstanceCheck: A = null;
	public _isReactRenderContext(): boolean {
		const _react = isReact as A;
		if (!_react) return false;
		if (IS_THE_WIZARD) return false;
		try {
			_react.darkState;
			// If darkState is callable without throwing, we're in a React render context
			// We probe by checking if the dispatcher is active via a known internal
			const internals = (_react as A).__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;
			const dispatcher = internals?.ReactCurrentDispatcher?.current ?? internals?.H;
			return !!dispatcher;
		} catch (e) {
			// log(`[THE_CLOUD][_IS_REACT_RENDER_CONTEXT]`, { error: e, }, t.error)
			return false;
		}
	}
	public createHook<T>(hookName: string, sauronId: string, initializer: () => HookState, updater: (state: HookState, key: string, notify: (key: string) => void) => T, triggerRender: boolean = true): T {
		if (isReact === true && IS_THE_WIZARD === false) {
			try {
				const isReact = this._isReactRenderContext();
				const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponent(), forceUpdate: undefined };

				const component = instance;

				// ─── THE TARGETED FIX ───────────────────────────────────────────────────────
				// Use sequential indexing when in React so it matches React's rendering slots.
				// Otherwise, use Sauron's default compilation ID string.
				const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
				const key = `${component.id}:${hookName}:${stableSlotId}`;
				// ────────────────────────────────────────────────────────────────────────────
				if (!this._components.has(component.id)) {
					this._components.set(component.id, component);
				}

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);
					if (component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				if (isReact && this._adapter && forceUpdate) { this._adapter.onHookSubscribe(key, forceUpdate); }

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
					}
					this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); 	} 
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				//  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		} else {
			try {
				const component = this.getCurrentComponent();
				const key = `${component.id}:${hookName}:${sauronId}`;

				if (!this._components.has(component.id)) { this._components.set(component.id, component); }

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);

					if (triggerRender && component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, sauronId, k, state?.value);
					}
					if (triggerRender) this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); }
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				// log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		}
	}
	public createHookCell<T>(hookName: string, sauronId: string, initializer: () => HookState, updater: (state: HookState, key: string, notify: (key: string) => void) => T, triggerRender: boolean = true): T {
		if (isReact === true && IS_THE_WIZARD === false) {
			try {
				const isReact = this._isReactRenderContext();
				const { instance, forceUpdate } = isReact && this._adapter ? this._adapter.getComponentInstance() : { instance: this.getCurrentComponentCell(), forceUpdate: undefined };

				const component = instance;

				// ─── THE TARGETED FIX ───────────────────────────────────────────────────────
				// Use sequential indexing when in React so it matches React's rendering slots.
				// Otherwise, use Sauron's default compilation ID string.
				const stableSlotId = isReact ? `seq-${this._reactHookCursor++}` : sauronId;
				const key = `${component.id}:${hookName}:${stableSlotId}`;
				// ────────────────────────────────────────────────────────────────────────────
				if (!this._components.has(component.id)) {
					this._components.set(component.id, component);
				}

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);
					if (component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				if (isReact && this._adapter && forceUpdate) { this._adapter.onHookSubscribe(key, forceUpdate); }

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, stableSlotId, k, state?.value);
					}
					this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); 	} 
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				//  log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		} else {
			try {
				const component = this.getCurrentComponentCell();
				const key = `${component.id}:${hookName}:${sauronId}`;

				if (!this._components.has(component.id)) { this._components.set(component.id, component); }

				if (!component.hooks.has(key)) {
					const state = initializer();
					component.hooks.set(key, state);

					if (triggerRender && component.render) {
						const unsub = this.subscribeToHook(key, () => component.render?.());
						component.unmount.add(unsub);
					}
				}

				const notify = (k: string) => {
					if (typeof process !== "undefined" && process.env?.NODE_ENV !== "production") {
						const state = component.hooks.get(k);
						this.devRecord(component.id, hookName, sauronId, k, state?.value);
					}
					if (triggerRender) this.notifyHook(k);
				};

				const hookState = component.hooks.get(key)!;
				this.beginHookDependencyCapture(hookState);
				try { return updater(hookState, key, notify); }
				finally { this.endHookDependencyCapture(); }
			} catch (e: A) {
				// log(`[THE_CLOUD][CREATE_HOOK]`, { error: e, }, t.error)
				return e.error;
			}
		}
	}
	public _broadcastSS(key: A, value: A, storageType: S, fn: S, path?: S, domain?: S, options?: A) {
		_localChange = true;
		_channel?.postMessage({ key, value, storageType, fn, path, domain, options });
	}
	public getDark<T = A>(key: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.get<T>(key, { s, m, r }));
	}
	public setDark<T = A>(label: S, value: T, triggerRender = true, s?: S, m?: S, r?: S) {
		this._broadcastSS(label, value, "sauron", "set");
		return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
	}
	public getAllDark<T = A>(opts?: ScopeOptions): Record<S | symbol, T> {
		return this.state.getAll<T>(opts);
	}
	public subAllDark(fn: (key: S | symbol, value: A) => V, s?: S, m?: S, r?: S): () => V {
		if (!fn) return () => {};
		return this.state.subscribeAll(fn, { s, m, r });
	}
	public subDark<T = A>(k: A, fn: (value: T) => void, s?: S, m?: S, r?: S): () => void {
		if (!k || !fn) return () => {};
		return MaybePromise(this.state.subscribe<T>(k, fn, { s, m, r }));
	}
	public deleteDark(label: A, s?: S, m?: S, r?: S): B {
		if (!label) return false;
		const v = this.state.delete(label, { s, m, r });
		this._broadcastSS(label, undefined, "sauron", "delete");
		return v;
	}
	public waitForDark<T = A>(label: S, s?: S, m?: S, r?: S): P<T> {
		return this.state.waitFor<T>(label, { s, m, r });
	}
	public async deleteManyDark(keys: A[], s?: S, m?: S, r?: S): P<B> {
		if (!Array.isArray(keys) || keys.length === 0) {
			return false;
		}
		let allDeleted = true;

		// Batch all deletes without triggering renders until the end
		for (let i = 0; i < keys.length; i++) {
			const deleted = this.delSauron(keys[i], s, m, r);
			if (!deleted) {
				allDeleted = false;
			}
		}

		return allDeleted;
	}
	public async setManyDark<T = Record<S, A>>(entries: T): Promise<void> {
		if (!entries || typeof entries !== "object") {
			return;
		}
		// Batch all sets without triggering renders until the end
		const entriesArray = Object.entries(entries);
		for (let i = 0; i < entriesArray.length; i++) {
			const [key, value] = entriesArray[i];
			// Only trigger render on the last item to avoid multiple renders
			const triggerRender = i === entriesArray.length - 1;
			this.set(key, value, triggerRender);
		}
	}
	public async getManyDark<T = A>(keys: S[], s?: S, m?: S, r?: S) {
		if (!Array.isArray(keys) || keys.length === 0) {
			return {};
		}

		const result: Record<S, T | undefined> = {};

		const promises = keys.map(async (key) => {
			const value = await this.get<T>(key, s, m, r);
			return { key, value };
		});

		const results = await Promise.all(promises);

		for (const { key, value } of results) {
			result[key] = value;
		}

		return result;
	}

	public getAll<T = A>(opts?: ScopeOptions): Record<S | SY, T> {
		return this.state.getAll<T>(opts);
	}
	public get<T = A>(key: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.get<T>(key, { s, m, r }));
	}
	public getReChain<T = A>(key: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.get<T>(key, { s, m, r }));
	}
	public setChain<T = A>(label: A, value: T, triggerRender = true, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
	}
	public set<T = A>(label: A, value: T, triggerRender = true, s?: S, m?: S, r?: S) {
		this._broadcastSS(label, value, "sauron", "set");
		return MaybePromise(this.state.set(label, value, { triggerRender, s, m, r }));
	}
	public subChain<T = A>(label: S, fn: (value: T) => V, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.subscribe<T>(label, fn, { s, m, r }));
	}
	public sub<T = A>(label: A, fn: (value: T) => V, s?: S, m?: S, r?: S): () => V {
		if (!label || !fn) return () => {};
		return MaybePromise(this.state.subscribe<T>(label, fn, { s, m, r }));
	}
	public subAll(fn: (key: A, value: A) => void, s?: S, m?: S, r?: S): () => V {
		if (!fn) return () => {};
		return this.state.subscribeAll(fn, { s, m, r });
	}
	public delSauron(label: S, s?: S, m?: S, r?: S): B {
		if (!label) return false;
		const v = this.state.delete(label, { s, m, r });
		this._broadcastSS(label, undefined, "sauron", "delete");
		return v;
	}
	public deleteChain(label: A, s?: S, m?: S, r?: S) {
		return MaybePromise(this.state.delete(label, { s, m, r }));
	}
	public waitFor<T = A>(label: S, s?: S, m?: S, r?: S): P<T> {
		return this.state.waitFor<T>(label, { s, m, r });
	}

	public __subscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
}
// #endregion
//                                                                                                                         [ss] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const _SS_MEMORY = new Map<string, A>();
export let memoryTimer: ReturnType<typeof setTimeout> | undefined;
export let ttl_active = false;
export let ttl_minutes = 30;
export function touchMemory() {
	if (memoryTimer) clearTimeout(memoryTimer);
	memoryTimer = setTimeout(
		() => {
			_SS_MEMORY.clear();
			memoryTimer = undefined;
		},
		ttl_minutes * 60 * 1000,
	);
}
export async function _withLock<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
	if (typeof navigator !== "undefined" && navigator.locks) {
		return navigator.locks.request("gt_write_" + key, fn) as Promise<T>;
	}
	return fn();
}
export function generateId(): string {
	if (typeof crypto !== "undefined" && crypto.getRandomValues) {
		const arr = new Uint32Array(1);
		crypto.getRandomValues(arr);
		return arr[0].toString();
	}
	try {
		const { randomBytes } = require("crypto");
		return randomBytes(4).readUInt32BE(0).toString();
	} catch {
		return Math.floor(Math.random() * 0xffffffff).toString();
	}
}
export async function setSS(key: string, value: A) {
	if (typeof window === "undefined") return;
	const serialized = ser(value);
	await _withLock(key, () => {
		sessionStorage.setItem(key, serialized);
		_SS_MEMORY.set(key, serialized);
	});
	return value;
}
function ser(value: U): S {
	return typeof value === "string" ? value : JSON.stringify(value);
}
function deser(raw: S | NU): A {
	if (raw === null) return null;
	try {
		return JSON.parse(raw);
	} catch {
		return raw;
	}
}
export function getSS(key: S) {
	if (typeof window === "undefined") return;
	const raw = typeof sessionStorage !== "undefined" ? sessionStorage.getItem(key) : null;
	const v = deser(raw);
	return MaybePromise(v);
}
export async function get_ws_id(): Promise<string> {
	let Bun: A = null;
	let Deno: A = null;
	if (typeof (globalThis as A).Deno !== "undefined") { Deno = (globalThis as A).Deno;	}
	if (typeof (globalThis as A).Bun !== "undefined") {	Bun = (globalThis as A).Bun;	}
	const hasBun = typeof (globalThis as A).Bun !== "undefined";
	const hasDeno = typeof (globalThis as A).Deno !== "undefined";
	const hasNode = typeof process !== "undefined" && !!process.versions?.node;
	const isBrowser = typeof window !== "undefined";
	const isVSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
	const isElectron = false;
	const isUnix = false;
	const isWindows = true;
	let WS_KEY = root.REGISTRY.get('WS_KEY');
	let WS_ID = root.REGISTRY.get('WS_ID');
	if (isBrowser) {
		let id = getSS(WS_KEY);
		if (!id) { id = generateId(); setSS(WS_KEY, id);		}
		WS_ID = id;
		return id;
	}
	if (isVSCode) {
		const vscode = require("vscode");
		const name = vscode.workspace.workspaceFolders?.[0]?.name;
		if (name) return name;
		const id = generateId();
		WS_ID = id;
		return id;
	}
	let cwd: string;
	if (hasBun || hasNode) { cwd = process.cwd(); } 
	else if (hasDeno) {  cwd = Deno.cwd(); } 
	else { throw new Error("Unsupported environment"); }

	let filePath: string;

	if (hasDeno) {
		const currentDir = new URL(".", import.meta.url).pathname;
		const projectRoot = new URL("../../../../../", import.meta.url).pathname;
		filePath = new URL("the_wizard/wizards_vault/ws_id.txt", projectRoot).pathname;
	} else {
		const path = require("path");
		const currentDir = typeof import.meta?.dirname !== "undefined" ? import.meta.dirname : __dirname;
		const projectRoot = path.resolve(currentDir, "../../../../../");
		filePath = path.join(projectRoot, "the_wizard", "wizards_vault", "ws_id.txt");
	}

	const readFile = async (): Promise<string | null> => {
		try {
			if (hasBun) {
				const file = Bun.file(filePath);
				if (!(await file.exists())) return null;
				return await file.text();
			} else if (hasDeno) {
				try {
					await Deno.stat(filePath);
					return await Deno.readTextFile(filePath);
				} catch {
					return null;
				}
			} else if (hasNode) {
				const fs = require("fs");
				if (!fs.existsSync(filePath)) return null;
				return fs.readFileSync(filePath, "utf8");
			}
			return null;
		} catch {
			return null;
		}
	};
	const writeFile = async (data: string) => {
		if (hasBun) {
			await Bun.write(filePath, data);
		} else if (hasDeno) {
			await Deno.writeTextFile(filePath, data);
		} else if (hasNode) {
			const fs = require("fs");
			fs.writeFileSync(filePath, data, "utf8");
		}
	};
	let id = await readFile();
	if (id && !isNaN(Number(id))) { WS_ID = id;		return id;	}
	id = generateId();
	await writeFile(id);
	WS_ID = id;
	return id;
}
async function deleteSS(key: string) {
	if (typeof window === "undefined") return;
	if (ttl_active) touchMemory();
	await _withLock(key, () => {
		if (typeof sessionStorage !== "undefined") sessionStorage.removeItem(key);
		_SS_MEMORY.delete(key);
	});
}
// #endregion
//                                                                                                                   [WAR ROOM] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const rootRegistry = new M2<A, M2<A, M2<A, A>>>();
export class WAR_ROOOM {
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "SAURON_SLAVE_REG";
	public k: A = "__SAURON__";
	private raven: DEATH_STAR;
	//                                                                                                                  MAIN CACHES
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	#META = new M2<A, M2<A, M2<A, A>>>();
	_meta: MTracer<A, A>;
	#SIGNALS = new M2<A, M2<A, M2<A, A>>>();
	_signals: M2<A, A>;
	#NAKED_SIGNALS = new M2<A, M2<A, M2<A, A>>>();
	_naked_signals: M2<A, A>;
	#CONTEXT = new M2<A, M2<A, M2<A, A>>>();
	_context: MTracer<A, A>;
	#ATOMS = new M2<A, M2<A, M2<A, A>>>();
	_atoms: MTracer<A, A>;
	#SAURON = new M2<A, M2<A, M2<A, A>>>();
	_sauron: MTracer<A, A>;
	#BUS = new M2<A, M2<A, M2<A, A>>>();
	_bus: MTracer<A, A>;
	_gt: MTracer<A, A>;

	COMPONENTS = new M2<S, ComponentInstance>();
	// #endregion
	//                                                                                                                    THE CLOUD
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	_components: MTracer<A, A>;
	_batches: MTracer<S | SY>;
	_batchQueue: MTracer<S | SY, A>;
	_batchOpts: M<S | SY, { triggerRender?: B }>;
	_sys: MTracer<S, SysEntry>;
	_sealed: MTracer<S, SysEntry>;
	_keyContracts: M<S, ST<S>>;
	_conflicts: MTracer<S, ConflictPolicy>;
	_conflictStrategies: MTracer<S, "latest" | "fail" | "accumulate">;
	_causal: M<S, CausalEntry[]>;
	_history: M<S | SY, HistoryEntry[]>;
	_scopes: MTracer<S, SignalScope>;
	_msgHandlers: M<S, ST<(p: A) => V>>;
	_lastWriteSource: M<S, S>;
	_mainMap: MTracer<A, S>;
	_tupleCache: MTracer<S, S>;
	_weakMap: MTracer<O, S>;
	_boundaries: MTracer<O, S>;
	_persistent: MTracer<A, A>;
	_persistent_keys: MTracer<A, A>;

	_global_subscribers: M2<S, ST<() => V>>;
	_waiters: M2<S | SY, ST<(value: A) => V>>;
	_listeners: M2<A, ST<(value: A) => void>>;
	// #endregion
	//                                                                                                                    SIGNALS
	// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
	_timeTravelMaul: MTracer<A, A>;
	_historyMaul: MTracer<A, A>;

	_subscribersMaul: M2<A, A>;
	_listenersMaul: M2<A, A>;
	// #endregion
	constructor(raven: DEATH_STAR) {
		this.raven = raven;
		this.r = raven.r ?? "THE_WIZARD_ROOT_REG";
		this.m = raven.m ?? "SAURON_MASTER_REG";

		this.s = raven.s ?? "SAURON_SLAVE_REG";
		this.k = raven.k ?? "THE_K_KEY";

		this._meta = this.tracer("_meta", this.m, this.r, "#META");
		this._signals = this.map("_signals", this.m, this.r, "#SIGNALS");
		this._naked_signals = this.map("_naked_signals", this.m, this.r, "#NAKED_SIGNALS");
		this._context = this.tracer("_context", this.m, this.r, "#CONTEXT");
		this._atoms = this.tracer("_atoms", this.m, this.r, "#ATOMS");
		this._sauron = this.tracer("_sauron", this.m, this.r, "#SAURON");
		this._bus = this.tracer("_bus", this.m, this.r, "#BUS");
		this._gt = this.tracer("_gt", this.m, this.r);

		this._listeners = this.map("_listeners_raven", this.m, this.r);
		this._global_subscribers = this.map<M<S, ST<() => V>>>("_global_subscribers_raven", this.m, this.r);
		this._waiters = this.map<M<S | SY, ST<(value: A) => V>>>("_waiters_raven", this.m, this.r);

		this._persistent_keys = this.tracer("_persistent_keys_raven", this.m, this.r);
		this._persistent = this.tracer("_persistent_raven", this.m, this.r);
		this._components = this.tracer("_components_raven", this.m, this.r);
		this._batches = this.tracer("_batches_raven", this.m, this.r);
		this._batchQueue = this.tracer("_batchQueue_raven", this.m, this.r);
		this._batchOpts = this.map<M<S | SY, { triggerRender?: B }>>("_batchOpts_raven", this.m, this.r);
		this._sys = this.tracer<MTracer<S, SysEntry>>("_sys_raven", this.m, this.r);
		this._sealed = this.tracer<MTracer<S, SysEntry>>("_sealed_raven", this.m, this.r);
		this._keyContracts = this.map<M<S, ST<S>>>("_keyContracts_raven", this.m, this.r);
		this._conflicts = this.tracer<MTracer<S, ConflictPolicy>>("_conflicts_raven", this.m, this.r);
		this._conflictStrategies = this.tracer<MTracer<S, "latest" | "fail" | "accumulate">>("_conflictStrategies_raven", this.m, this.r);
		this._causal = this.map<M<S, CausalEntry[]>>("_causal_raven", this.m, this.r);
		this._history = this.map<M<S | SY, HistoryEntry[]>>("_history_raven", this.m, this.r);
		this._scopes = this.tracer<MTracer<S, SignalScope>>("_scopes_raven", this.m, this.r);
		this._msgHandlers = this.map<M<S, ST<(p: A) => V>>>("_msgHandlers_raven", this.m, this.r);
		this._lastWriteSource = this.map<M<S, S>>("_lastWriteSource_raven", this.m, this.r);
		this._mainMap = this.tracer("_mainMap_raven", this.m, this.r);
		this._tupleCache = this.tracer<MTracer<S, S>>("_tupleCache_raven", this.m, this.r);
		this._weakMap = this.tracer<MTracer<O, S>>("_weakMap_raven", this.m, this.r);
		this._boundaries = this.tracer<MTracer<O, S>>("_boundaries_raven", this.m, this.r);

		this._timeTravelMaul = this.tracer<MTracer<O, S>>("_timeTravel_maul", this.m, this.r, "#SIGNALS");
		this._historyMaul = this.tracer<MTracer<O, S>>("_history_maul", this.m, this.r, "#SIGNALS");

		this._subscribersMaul = this.map<M2<O, S>>(" _subscribers_maul", this.m, this.r, "#SIGNALS");
		this._listenersMaul = this.map<M2<O, S>>(" _listeners_maul", this.m, this.r, "#SIGNALS");
	}
	get_root(c: S, r: S) {
		let root;
		switch (c) {
			case "#CONTEXT": { root = this.#CONTEXT.get(r); break; }
			case "#META": { root = this.#META.get(r); break; }
			case "#SIGNALS": { root = this.#SIGNALS.get(r); break; }
			case "#ATOMS": { root = this.#ATOMS.get(r); break; }
			case "#SAURON": { root = this.#SAURON.get(r); break; }
			case "#BUS": { root = this.#BUS.get(r); break; }
			case "#NAKED_SIGNALS": { root = this.#NAKED_SIGNALS.get(r); break; }
			case "gt": { root = rootRegistry.get(r); break; }
			case "globalThis": { (globalThis as A)[r]; break; }
		}
		if (!root) {
			root = new M2<A, M2<A, M2<A, A>>>();
			switch (c) {
				case "#CONTEXT": { root = this.#CONTEXT.set(r, root); break; }
				case "#META": { root = this.#META.set(r, root); break; }
				case "#SIGNALS": { root = this.#SIGNALS.set(r, root); break; }
				case "#ATOMS": { root = this.#ATOMS.set(r, root); break; }
				case "#SAURON": { root = this.#SAURON.set(r, root); break; }
				case "#NAKED_SIGNALS": { root = this.#NAKED_SIGNALS.set(r, root); break; }
				case "#BUS": { root = this.#BUS.set(r, root); break; }
				case "gt": { rootRegistry.set(r, root); break; }
				case "globalThis": { (globalThis as A)[r] = root; break; }
			}
		}
		return root;
	}
	map<K = A, V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): M2<K, V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new M2<K, V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	tracer<K = A, V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): MTracer<K, V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new MTracer<K, V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	set<V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): STracer<V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new STracer<V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	obj<V = A>(s = this.s, m = this.m, r = this.r, c = "gt"): OTracer<V> | K | V {
		const root = this.get_root(c, r);
		let masterMap = root.get(m);
		if (!masterMap) {
			masterMap = new M2<A, M2<A, A>>();
			root.set(m, masterMap);
		}
		let slaveMap = masterMap.get(s);
		if (!slaveMap) {
			slaveMap = new OTracer<V>();
			masterMap.set(s, slaveMap);
		}
		return slaveMap;
	}
	
}
// (globalThis as A)[r] //  STATE_CONTEXT === "sauron" ? : null;
	// (globalThis as A)[r] = root;
// #endregion
//                                                                                                             [DEATH STAR] HUB
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class DEATH_STAR {
	public bus: SITH; // bus
	public signals: SIGNALS; // signals
	public state: RAVEN; // app context / state
	public governor: GOVERNOR;
	public REGISTRY: Registry;
	public wr: WAR_ROOOM;
	
	//public MESH: Mesh;

	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "SAURON_SLAVE_REG";
	public k: A = "__SIGNALS__";

	constructor(capacity = 1 << 20, REGISTRY: A = null, MESH: A = null) {
		this.REGISTRY = REGISTRY;
		if (!REGISTRY) this.REGISTRY = new Registry();
		this.wr = new WAR_ROOOM(this);
		// this.MESH = MESH
		//if (!MESH) this.MESH = new Mesh()
		this.bus = new SITH(this, capacity);
		this.state = new RAVEN(this);
		this.signals = new SIGNALS(this);
		this.governor = new GOVERNOR(this);
	}
}
export const root = new DEATH_STAR();
// #endregion
//                                                                                                                    USE STATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkState<T = any, S = T>(initialValue?: T | (() => T), options: StateOptions<T, S> = {}, __sauron_id__ = this.hub.REGISTRY._sid()) {
	return root.state.createHookCell( "darkState", __sauron_id__, () => ({
			value: {
				current: typeof initialValue === "function" ? (initialValue as () => T)() : initialValue,
				derived: undefined as A,
			},
		}),
		(state, key, notify) => {
			const s = state.value;

			const equals: (a: S, b: S) => boolean = options.equals ?? ((a, b) => a === b);
			const selector = options.selector ?? null;
			const derive = options.derive ?? null;

			const getValue = (): S => (selector ? selector(s.current) : (s.current as unknown as S));

			const setValue = (v: T | ((prev: T) => T)) => {
				const next = typeof v === "function" ? (v as (prev: T) => T)(s.current) : v;
				const prevSelected = getValue();
				const nextSelected = selector ? selector(next) : (next as unknown as S);
				if (!equals(prevSelected, nextSelected)) {
					s.current = next;
					if (derive) {s.derived = derive(next);}
					notify(key);
				}
				return next;
			};

			if (derive && s.derived === undefined) { s.derived = derive(s.current); }
			if (derive) { return [getValue(), setValue, s.derived] as const; }

			return [getValue(), setValue] as const;
		},
	);
}
// #endregion
//                                                                                                                USE CALL BACK
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkCallback<T extends (...args: A[]) => A>(callback: T, deps?: A[], __sauron_id__ = this.hub.REGISTRY._sid()): T {
	return root.state.createHookCell( "darkCallback", __sauron_id__,
		(): CallbackHookState => ({ value: undefined, callback, deps: deps ?? [] }),
		(s: HookState) => {
			const state = s as CallbackHookState;
			const hasChanged = !deps || deps.length !== state.deps.length || deps.some((d, i) => d !== state.deps[i]);

			if (hasChanged) {
				state.callback = callback;
				state.deps = deps ?? [];
			}

			return state.callback as T;
		},
	);
}
// #endregion
//                                                                                                                   USE EFFECT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkEffect(effect: (signal?: AbortSignal) => void | (() => void), deps?: A[], __sauron_id__ = this.hub.REGISTRY._sid()): void {
	root.state.createHookCell( "darkEffect", __sauron_id__,
		(): HookState => {
			const controller = new AbortController();
			const cleanup = effect(controller.signal);
			return {
				value: undefined,
				deps: deps ?? [],
				cleanup: () => {
					controller.abort();
					if (typeof cleanup === "function") cleanup();
				},
			};
		},
		(state) => {
			if (deps === undefined || !_depsEqual(state.deps ?? [], deps)) {
				state.cleanup?.();
				const controller = new AbortController();
				const cleanup = effect(controller.signal);
				state.deps = deps ?? [];
				state.cleanup = () => {
					controller.abort();
					if (typeof cleanup === "function") cleanup();
				};
			}
			return undefined;
		},
	);
}
function _depsEqual(a: A[], b: A[]): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (!Object.is(a[i], b[i])) return false;
	}
	return true;
}
// #endregion
//                                                                                                                 USE INTERVAL
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkInterval(options: { timer?: number; autoStart?: boolean; execute: () => void }, __sauron_id__ = this.hub.REGISTRY._sid()) {
	const autoStart = options.autoStart ? Boolean(options.autoStart) !== false : true;
	const callback = options.execute ? options.execute : console.log(`interval triggered`);
	const delay = options.timer ? options.timer : 30000;
	return root.state.createHook(
		"darkInterval",
		__sauron_id__,
		(): HookState => ({
			value: {
				callback,
				delay,
				running: autoStart,
				handle: null as A,
				attached: false,
				cleanup: null as null | (() => void),
			},
		}),

		(hook, key) => {
			const s = hook.value;

			const start = () => {
				if (s.running) return;
				s.running = true;
				root.state.notifyHook(key);

				if (typeof window !== "undefined" && s.delay !== null) {
					s.handle = setInterval(() => {
						s.callback();
					}, s.delay);
				}
			};

			const stop = () => {
				if (!s.running) return;
				s.running = false;

				if (s.handle) {
					clearInterval(s.handle);
					s.handle = null;
				}

				root.state.notifyHook(key);
			};

			if (!s.attached && typeof window !== "undefined") {
				s.attached = true;

				if (s.running && s.delay !== null) {
					s.handle = setInterval(() => {
						s.callback();
					}, s.delay);
				}

				s.cleanup = () => {
					if (s.handle) clearInterval(s.handle);
				};
			}

			const isRunning = () => s.running;

			return [isRunning(), start, stop];
		},
	);
}
// #endregion
//                                                                                                           USE DEFERRED VALUE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkDeferredValue<T>(value: T, __sauron_id__ = this.hub.REGISTRY._sid()): T {
	return root.state.createHook(
		"darkDeferredValue",
		__sauron_id__,
		(): HookState => ({ value }),
		(state, key, notify) => {
			if (!Object.is(state.value, value)) {
				queueMicrotask(() => {
					state.value = value;
					notify(key);
				});
			}
			return state.value as T;
		},
	);
}
// #endregion
//                                                                                                        USE IMPERATIVE HANDLE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkImperativeHandle<T>(ref: { current: T | null }, init: () => T, deps?: A[], __sauron_id__ = this.hub.REGISTRY._sid()): void {
	return root.state.createHook(
		"darkImperativeHandle",
		__sauron_id__,
		(): HookState => {
			if (ref) ref.current = init();
			return { value: undefined, deps: deps ?? [] };
		},
		(state) => {
			if (deps === undefined || !_depsEqual(state.deps ?? [], deps)) {
				if (ref) ref.current = init();
				state.deps = deps ?? [];
			}
			return undefined;
		},
	);
}
// #endregion
//                                                                                                                     USE MEMO
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function darkMemo<T>(factory: () => T, deps: A[], __sauron_id__ = this.hub.REGISTRY._sid()): T {
	return root.state.createHook(
		"darkMemo",
		__sauron_id__,
		(): HookState => ({ value: factory(), deps }),
		(state) => {
			if (!_depsEqual(state.deps ?? [], deps)) {
				state.value = factory();
				state.deps = deps;
			}
			return state.value as T;
		},
	);
}

// #endregion
//                                                                                                                 ATOM HELPERS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════




// #endregion
//                                                                                                   NEED TO BE COMPLETED ATOMS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════





// #endregion
//                                                                                                                  MASTER ATOM
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const SLAB = BIND_ATOM("SLAB_ATOM", <X = U>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
    capacity: config.capacity as number,

    values: new Float64Array(config.capacity),
    versions: new Uint32Array(config.capacity),
    nextFree: new Int32Array(config.capacity),
    refCount: new Int32Array(config.capacity),

    freeHead: -1,
    allocated: 0,

    slab: new Uint8Array(config.slabCapacity ?? (1 << 20)),
    slabTail: 0,

    slabOffsets: new Int32Array(config.capacity).fill(-1),
    slabLengths: new Int32Array(config.capacity).fill(0),

    inUse: new Uint8Array(config.capacity),

    initialized: (() => {
        const capacity = config.capacity as number;

        if (!pow2(capacity)) {
            throw new Error("slotCapacity must be a power of two");
        }

        self.nextFree.fill(-1);

        // Slot 0 is reserved as the control slot (version counter).
        self.allocated = 1;
        self.refCount[0] = 1;
        self.versions[0] = 0;
        self.inUse[0] = 1;

        // Build initial free list (skip slot 0).
        // Link slots 1..capacity-1 in reverse order so we pop from the front.
        for (let i = capacity - 1; i >= 1; i--) {
            self.nextFree[i] = self.freeHead;
            self.freeHead = i;
        }

        return true;
    })(),

    writeBinary(slot: N, data: U8, epoch: N): V {
        self.assertLive(slot);
        self.assertEpoch(epoch);

        const oldOffset = self.slabOffsets[slot];
        const oldLength = self.slabLengths[slot];
        const length = data.length;

        // Reuse the existing allocation when it is large enough.
        if (oldOffset >= 0 && length <= oldLength) {
            self.slab.set(data, oldOffset);

            if (length < oldLength) {
                self.freeBytes(oldOffset + length, oldLength - length);
            }

            self.slabOffsets[slot] = length === 0 ? 0 : oldOffset;
            self.slabLengths[slot] = length;
            self.values[slot] = 0;
            self.versions[slot] = epoch;
            return;
        }

        // Allocate first: exhaustion leaves the old payload intact.
        const offset = self.allocBytes(length);

        self.slab.set(data, offset);

        if (oldOffset >= 0 && oldLength > 0) {
            self.freeBytes(oldOffset, oldLength);
        }

        self.slabOffsets[slot] = offset;
        self.slabLengths[slot] = length;
        self.values[slot] = 0;
        self.versions[slot] = epoch;
    },

    readBinary(slot: N): U8 {
        const { offset, length } = self.ref(slot);
        return self.slab.subarray(offset, offset + length);
    },

    allocBytes(length: N): N {
        if (length === 0) return 0;

        // First-fit allocation from reclaimed ranges.
        for (let i = 0; i < self.freeRanges.length; i++) {
            const range = self.freeRanges[i];

            if (range.length < length) continue;

            const offset = range.offset;

            if (range.length === length) {
                self.freeRanges.splice(i, 1);
            } else {
                range.offset += length;
                range.length -= length;
            }

            return offset;
        }

        if (length > self.slab.length - self.slabTail) {
            throw new RangeError("STORAGE_RING: no contiguous slab range large enough");
        }

        const offset = self.slabTail;
        self.slabTail += length;
        return offset;
    },

    freeBytes(offset: N, length: N): V {
        if (length === 0) return;

        const ranges = self.freeRanges;
        let index = 0;

        while (index < ranges.length && ranges[index].offset < offset) {
            index++;
        }

        ranges.splice(index, 0, { offset, length });

        // Merge with the preceding range.
        if (
            index > 0 &&
            ranges[index - 1].offset + ranges[index - 1].length === offset
        ) {
            ranges[index - 1].length += length;
            ranges.splice(index, 1);
            index--;
        }

        // Merge with the following range.
        if (
            index + 1 < ranges.length &&
            ranges[index].offset + ranges[index].length === ranges[index + 1].offset
        ) {
            ranges[index].length += ranges[index + 1].length;
            ranges.splice(index + 1, 1);
        }

        // Return a free suffix to the unused slab tail.
        const last = ranges[ranges.length - 1];

        if (last !== undefined && last.offset + last.length === self.slabTail) {
            self.slabTail = last.offset;
            ranges.pop();
        }
    },

    assertLive(slot: N): V {
        if (
            !Number.isInteger(slot) ||
            slot <= 0 ||
            slot >= self.allocated ||
            self.refCount[slot] <= 0
        ) {
            throw new RangeError(`STORAGE_RING: invalid or released slot ${slot}`);
        }
    },

    assertEpoch(epoch: N): V {
        if (
            !Number.isInteger(epoch) ||
            epoch < 0 ||
            epoch > 0xffffffff
        ) {
            throw new RangeError("STORAGE_RING: epoch must be a uint32");
        }
    },

    tick(): number {
        return self.versions[0];
    },

    advance(): number {
        return ++self.versions[0];
    },

    alloc(value = 0): number {
        const slot = self.freeHead;
        if (slot === -1) return -1;

        // Remove from free list.
        self.freeHead = self.nextFree[slot];
        self.nextFree[slot] = -1;

        // Initialize slot.
        self.values[slot] = value;
        self.refCount[slot] = 1;
        self.inUse[slot] = 1;

        // Clear slab allocation for this slot.
        self.slabOffsets[slot] = -1;
        self.slabLengths[slot] = 0;

        self.allocated++;

        return slot;
    },

    retain(slot: number): boolean {
        if (slot < 0 || slot >= self.capacity) return false;
        if (!self.inUse[slot]) return false;

        if (self.refCount[slot] === 0x7FFFFFFF) {
            throw new Error("Reference count overflow");
        }

        self.refCount[slot]++;
        return true;
    },

    release(slot: number): boolean {
        if (slot < 0 || slot >= self.capacity) return false;
        if (!self.inUse[slot]) return false;

        if (self.refCount[slot] <= 0) {
            throw new Error("Reference count underflow");
        }

        self.refCount[slot]--;

        if (self.refCount[slot] === 0) {
            self.inUse[slot] = 0;

            // Clear slab data.
            self.slabOffsets[slot] = -1;
            self.slabLengths[slot] = 0;
            self.values[slot] = 0;

            // Add to free list.
            self.nextFree[slot] = self.freeHead;
            self.freeHead = slot;

            self.allocated--;

            return true;
        }

        return false;
    },

    write(slot: number, value: number, epoch: number): boolean {
        if (slot < 0 || slot >= self.capacity) return false;
        if (!self.inUse[slot]) return false;
        if (self.refCount[slot] <= 0) return false;

        // Epoch validation: the slot's version must match the expected epoch.
        if (self.versions[slot] !== epoch) return false;

        self.values[slot] = value;
        return true;
    },

    read(slot: number): number {
        if (slot < 0 || slot >= self.capacity) return NaN;
        if (!self.inUse[slot]) return NaN;

        return self.values[slot];
    },

    version(slot: number): number {
        if (slot < 0 || slot >= self.capacity) return -1;
        return self.versions[slot];
    },

    ref(slot: number): { offset: number; length: number } {
        if (slot < 0 || slot >= self.capacity) {
            return { offset: -1, length: 0 };
        }

        return {
            offset: self.slabOffsets[slot],
            length: self.slabLengths[slot],
        };
    },

    storeSlab(slot: number, data: Uint8Array | ArrayBuffer): boolean {
        if (slot < 0 || slot >= self.capacity) return false;
        if (!self.inUse[slot]) return false;

        const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
        const len = bytes.length;

        if (len === 0) {
            // Zero-length: clear slab assignment.
            self.slabOffsets[slot] = -1;
            self.slabLengths[slot] = 0;
            return true;
        }

        // Simple bump allocator: check if we have room.
        if (self.slabTail + len > self.slab.length) {
            // Could implement compaction here, but for now just fail.
            return false;
        }

        const offset = self.slabTail;
        self.slab.set(bytes, offset);
        self.slabTail += len;

        self.slabOffsets[slot] = offset;
        self.slabLengths[slot] = len;

        return true;
    },

    loadSlab(slot: number): Uint8Array | null {
        if (slot < 0 || slot >= self.capacity) return null;
        if (!self.inUse[slot]) return null;

        const offset = self.slabOffsets[slot];
        const length = self.slabLengths[slot];

        if (offset < 0 || length <= 0) return null;

        return self.slab.subarray(offset, offset + length);
    },

    growthSlab(minAdditional: number): void {
        const newCapacity = Math.max(self.slab.length * 2, self.slabTail + minAdditional);
        const newSlab = new Uint8Array(newCapacity);
        newSlab.set(self.slab);
        self.slab = newSlab;
    },

    needsCompaction(): boolean {
        if (self.allocated <= 1) return false;

        let liveBytes = 0;
        for (let i = 1; i < self.capacity; i++) {
            if (self.inUse[i] && self.slabLengths[i] > 0) {
                liveBytes += self.slabLengths[i];
            }
        }

        const deadBytes = self.slabTail - liveBytes;
        return deadBytes > self.slabTail * 0.5;
    },

    compact(): void {
        if (!self.needsCompaction()) return;

        const newSlab = new Uint8Array(self.slab.length);
        let newTail = 0;

        for (let i = 1; i < self.capacity; i++) {
            if (!self.inUse[i]) continue;

            const length = self.slabLengths[i];
            if (length <= 0) continue;

            const oldOffset = self.slabOffsets[i];
            newSlab.set(
                self.slab.subarray(oldOffset, oldOffset + length),
                newTail,
            );

            self.slabOffsets[i] = newTail;
            newTail += length;
        }

        // Copy back into original slab buffer.
        self.slab.set(newSlab.subarray(0, newTail), 0);
        self.slabTail = newTail;
    },

    stats(): {
        capacity: number;
        allocated: number;
        freeSlots: number;
        slabUsed: number;
        slabCapacity: number;
        slabUtilization: number;
    } {
        return {
            capacity: self.capacity,
            allocated: self.allocated,
            freeSlots: self.capacity - self.allocated,
            slabUsed: self.slabTail,
            slabCapacity: self.slab.length,
            slabUtilization: self.slabTail / self.slab.length,
        };
    },

    validate(): string[] {
        const errors: string[] = [];

        let freeCount = 0;
        let current = self.freeHead;
        const seen = new Set<number>();

        while (current !== -1) {
            if (current < 1 || current >= self.capacity) {
                errors.push(`Free list contains invalid slot: ${current}`);
                break;
            }

            if (seen.has(current)) {
                errors.push(`Free list contains cycle at slot: ${current}`);
                break;
            }

            seen.add(current);
            freeCount++;
            current = self.nextFree[current];
        }

        if (freeCount !== self.capacity - self.allocated) {
            errors.push(
                `Free list has ${freeCount} entries, expected ${self.capacity - self.allocated}`,
            );
        }

        for (let i = 1; i < self.capacity; i++) {
            if (!self.inUse[i]) continue;

            const offset = self.slabOffsets[i];
            const length = self.slabLengths[i];

            if (offset === -1 && length !== 0) {
                errors.push(`Slot ${i}: offset -1 but length ${length}`);
            } else if (offset !== -1) {
                if (offset < 0 || offset + length > self.slabTail) {
                    errors.push(
                        `Slot ${i}: slab range [${offset}, ${offset + length}) out of bounds (tail=${self.slabTail})`,
                    );
                }
            }
        }

        return errors;
    },

    get used(): N {
        return self.allocated - 1 - self.freeCount();
    },

    get freeCount(): N {
        return self.freeHead === FREE_LIST_END ? 0 : self.walkFree();
    },

    get slabUsed(): N {
        return self.liveBytes;
    },

    get slabFree(): N {
        return self.slab.length - self.liveBytes;
    },

    walkFree(): N {
        let n = 0;
        let p = self.freeHead;

        while (p !== FREE_LIST_END) {
            n++;
            p = self.nextFree[p];
        }

        return n;
    },
}));
// #endregion
//                                                                                                                  MASTER DRS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const DRS = MASTER_ATOM("DRS_ATOM", <X = U>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    value: config.value as X,

  

    destroy(): V {
        self.registry?.purgeAll();
        self.registry = null;
        BC().store.particle(self.s, self.m, self.r).delete(self.key)._REGISTRY_PROTON;
    },

    read(): X {
        const r = self.registry;
        if (r !== null) {
            const gate = r.get(GATE_ELECTRON);
            if (gate !== undefined && (gate as any).sealed) {
                throw new SEALED_ERROR_ELECTRON(self.key);
            }
        }
        return self.value;
    },

    write(next: X): B {
        const r = self.registry;
        if (r === null) {
            self.value = next;
            return true;
        }

        const gate = r.get(GATE_ELECTRON);
        if (gate !== undefined && (gate.sealed || gate.frozen)) return false;

        const previous = self.value;
        if (Object.is(previous, next)) return false;

        self.value = next;

        const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
        if (subs !== undefined) {
            subs.notify({ key: self.key, previous, current: next });
        }

        return true;
    },

    subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
        const subs =
            (self.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
            self.bind(new SUBSCRIPTION_ELECTRON<X>(self));

        return subs.subscribe(listener);
    },

    waitFor() {},
}));
// #endregion
//                                                                                                                          SUB
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// 
export const SUB = MASTER_ATOM("SUB_ATOM", <T = any>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    pipeline: undefined as PIPELINE<MQ_CTX<T>> | undefined,

    send(message: T, signal?: SIGNAL): B {
        const ctx: MQ_CTX<T> = {
            atom: self,
            message,
            signal,
            meta: undefined,
        };

        return self.dispatch("beforeSend", ctx, () => {
            self.deliver(ctx);
            return true;
        });
    },

    dispatch(hook: HOOK, ctx: MQ_CTX<T>, terminal: () => V): V {
        const p = self.pipeline ??= self.compile();
        return p.run(hook, ctx, terminal);
    },

    deliver(ctx: MQ_CTX<T>): V {
        // bare transport / queue / subscriber fanout
    },

    compile(): PIPELINE<MQ_CTX<T>> {
        const r = self.registry;
        const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];

        if (r !== null) {
            for (const shard of r.shards()) {
                if ("behaviours" in shard) {
                    behaviours.push(...(shard as any).behaviours());
                }
            }
        }

        return compilePipeline(behaviours);
    },
}));
// #endregion
//                                                                                                                       STREAM
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const STREAM = MASTER_ATOM("STREAM_ATOM", <T = any>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    pipeline: undefined as PIPELINE<STREAM_CTX<T>> | undefined,

    send(message: T, signal?: SIGNAL): B {
        const ctx: STREAM_CTX<T> = {
            atom: self,
            message,
            signal,
            meta: undefined,
        };

        return self.dispatch("beforeSend", ctx, () => {
            self.deliver(ctx);
            return true;
        });
    },

    dispatch(hook: HOOK, ctx: STREAM_CTX<T>, terminal: () => V): V {
        const p = self.pipeline ??= self.compile();
        return p.run(hook, ctx, terminal);
    },

    deliver(ctx: STREAM_CTX<T>): V {
        // bare transport / queue / subscriber fanout
    },

    compile(): PIPELINE<STREAM_CTX<T>> {
        const r = self.registry;
        const behaviours: BEHAVIOUR<STREAM_CTX<T>>[] = [];

        if (r !== null) {
            for (const shard of r.shards()) {
                if ("behaviours" in shard) {
                    behaviours.push(...(shard as any).behaviours());
                }
            }
        }

        return compilePipeline(behaviours);
    },
}));
// #endregion
//                                                                                                                  SPREADSHEET
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const SPREADSHEET = MASTER_ATOM("SPREADSHEET_ATOM", <X = U>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    key: config.key,
    value: config.value as X,
    registry: null as REGISTRY_PROTON | null,

    bind<S extends SPREADSHEET_ATOM>(atom: S, owner: OWNERSHIP = "bespoke"): S {
        if (self.registry === null) {
            self.registry = new REGISTRY_PROTON(self.key, { atoms: [{ atom, owner }] });
        } else {
            self.registry.bind(atom, owner);
        }
        return atom;
    },

    shard<S extends SPREADSHEET_ATOM>(ctor: ATOM_CLASS<S>): S | UD {
        return self.registry?.get(ctor);
    },

    unbind(ctor: ATOM_CLASS<any>): B {
        if (self.registry === null) return false;

        const removed = self.registry.unbind(ctor);

        // A registry with nothing in it is dropped, returning the cell to the bare fast path.
        if (self.registry.size === 0) {
            BC().store.particle(self.s, self.m, self.r).delete(self.key)._REGISTRY_PROTON;
            self.registry = null;
        }

        return removed;
    },

    destroy(): V {
        self.registry?.purgeAll();
        self.registry = null;
        BC().store.particle(self.s, self.m, self.r).delete(self.key)._REGISTRY_PROTON;
    },

    read(): X {
        const r = self.registry;
        if (r !== null) {
            const gate = r.get(GATE_ELECTRON);
            if (gate !== undefined && (gate as any).sealed) {
                throw new SEALED_ERROR_ELECTRON(self.key);
            }
        }
        return self.value;
    },

    write(next: X): B {
        const r = self.registry;
        if (r === null) {
            self.value = next;
            return true;
        }

        const gate = r.get(GATE_ELECTRON);
        if (gate !== undefined && (gate.sealed || gate.frozen)) return false;

        const previous = self.value;
        if (Object.is(previous, next)) return false;

        self.value = next;

        const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
        if (subs !== undefined) {
            subs.notify({ key: self.key, previous, current: next });
        }

        return true;
    },

    subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
        const subs =
            (self.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
            self.bind(new SUBSCRIPTION_ELECTRON<X>(self));

        return subs.subscribe(listener);
    },

    waitFor() {},

    kind: undefined as SY,
    purge(): V {}, 
}));
// #endregion
//                                                                                                                         SEND
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const SEND = MASTER_ATOM("SEND_ATOM", <T = any>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    pipeline: undefined as PIPELINE<MQ_CTX<T>> | undefined,

    send(message: T, signal?: SIGNAL): B {
        const ctx: MQ_CTX<T> = {
            atom: self,
            message,
            signal,
            meta: undefined,
        };

        return self.dispatch("beforeSend", ctx, () => {
            self.deliver(ctx);
            return true;
        });
    },

    dispatch(hook: HOOK, ctx: MQ_CTX<T>, terminal: () => V): V {
        const p = self.pipeline ??= self.compile();
        return p.run(hook, ctx, terminal);
    },

    deliver(ctx: MQ_CTX<T>): V {
        // bare transport / queue / subscriber fanout
    },

    compile(): PIPELINE<MQ_CTX<T>> {
        const r = self.registry;
        const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];

        if (r !== null) {
            for (const shard of r.shards()) {
                if ("behaviours" in shard) {
                    behaviours.push(...(shard as any).behaviours());
                }
            }
        }

        return compilePipeline(behaviours);
    },
}));
// #endregion
//                                                                                                                        CLOUD
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const CLOUD = MASTER_ATOM("CLOUD_ATOM", <X = U>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    value: config.value as X,
    registry: null as REGISTRY_PROTON | null,

    bind: <S extends ATOM>(shard: S, owner: OWNERSHIP = "bespoke"): S => {
        if (self.registry === null) {
            self.registry = new REGISTRY_PROTON(self.key, {
                shards: [{ shard, owner }],
            });
        } else {
            self.registry.bind(shard, owner);
        }

        return shard;
    },

    shard: <S extends ATOM>(ctor: ATOM_CLASS<S>): S | UD =>
        self.registry?.get(ctor),

    unbind: (ctor: SHARD_CLASS<any>): B => {
        if (self.registry === null) return false;

        const removed = self.registry.unbind(ctor);

        if (self.registry.size === 0) {
            BC().store.particle(self.s, self.m, self.r)
                .delete(self.k)._REGISTRY_PROTON;

            self.registry = null;
        }

        return removed;
    },

    destroy: (): void => {
        self.registry?.purgeAll();
        self.registry = null;

        BC().store.particle(self.s, self.m, self.r)
            .delete(self.k)._REGISTRY_PROTON;
    },

    read: (): X => {
        const registry = self.registry;

        if (registry !== null) {
            const gate = registry.get(GATE_ELECTRON);

            if (gate !== undefined && (gate as any).sealed) {
                throw new SEALED_ERROR_ELECTRON(self.k);
            }
        }

        return self.value;
    },

    write: (next: X): B => {
        const registry = self.registry;

        if (registry === null) {
            self.value = next;
            return true;
        }

        const gate = registry.get(GATE_ELECTRON);

        if (gate !== undefined && (gate.sealed || gate.frozen)) {
            return false;
        }

        const previous = self.value;

        if (Object.is(previous, next)) return false;

        self.value = next;

        const subs = registry.get(SUBSCRIPTION_ELECTRON) as
            SUBSCRIPTION_ELECTRON<X> | UD;

        if (subs !== undefined) {
            subs.notify({
                key: self.k,
                previous,
                current: next,
            });
        }

        return true;
    },

    subscribe: (listener: LISTENER<X>): UNSUBSCRIBE => {
        const subs =
            (self.shard(SUBSCRIPTION_ELECTRON) as
                SUBSCRIPTION_ELECTRON<X> | UD) ??
            self.bind(new SUBSCRIPTION_ELECTRON<X>(self));

        return subs.subscribe(listener);
    },

    waitFor: () => {},
}));
// #endregion
//                                                                                                                       DUPLEX
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const DUPLEX = MASTER_ATOM("DUPLEX", <T>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    pipeline: undefined as PIPELINE<MQ_CTX<T>> | undefined,

    send: (message: T, signal?: ATOM): B => {
        const ctx: MQ_CTX<T> = {
            atom: self,
            message,
            signal,
            meta: undefined,
        };

        return self.dispatch("beforeSend", ctx, () => {
            self.deliver(ctx);
            return true;
        });
    },

    dispatch: (
        hook: HOOK,
        ctx: MQ_CTX<T>,
        terminal: () => V,
    ): V => {
        const p = self.pipeline ??= self.compile();
        return p.run(hook, ctx, terminal);
    },

    deliver: (ctx: MQ_CTX<T>): V => {
        // bare transport / queue / subscriber fanout
    },

    compile: (): PIPELINE<MQ_CTX<T>> => {
        const registry = self.registry;
        const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];

        if (registry !== null) {
            for (const shard of registry.shards()) {
                if ("behaviours" in shard) {
                    behaviours.push(...(shard as any).behaviours());
                }
            }
        }

        return compilePipeline(behaviours);
    },
}));
// #endregion
//                                                                                                                [MODULE_ATOM]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const MODULE = MASTER_ATOM("MODULE_ATOM", <T = any>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    initial: undefined as T,
    key: config.k ?? "__SIGNALS__",
    module: undefined as T,
    module_type: "class_fn_pool",
    version: 0,
    sealed: false,
    frozen: true,
    hasWritten: false,
    persisted: false,

    listeners: new Set<(change: Change<T>) => void>(),
    waiters: new Set<(value: T) => void>(),
    undoStack: undefined as HistoryEntry<T>[] | undefined,

    conflict: undefined as ConflictPolicy | undefined,
    historyIndex: 0,
    history: undefined as AtomHistoryEntry<T>[] | undefined,
    historyMax: 50,
    lifecycle: undefined as SignalLifecycle | undefined,
    async: undefined as AsyncSignalState<T> | undefined,
    metrics: undefined as Metrics | undefined,
    conflictStrategy: undefined as "latest" | "fail" | "accumulate" | undefined,

    schema: undefined as SignalSchema | undefined,
    contracts: new Map<string, AtomContract<T>>(),
    causal: [] as CausalEntry[],
    lastWriteSource: undefined as string | undefined,

    batches: new Set<SignalKey>(),
    batchQueue: new Map<SignalKey, unknown>(),
    batchOpts: new Map<SignalKey, { triggerRender?: boolean }>(),

    broadcast: true,
    triggerRender: true,
    boundary: "same-app" as Boundary,

    initialize: (
        initial: T,
        options: MODULE_OPTIONS = {
            module_type: "class_fn_pool",
            broadcast: true,
            frozen: true,
        },
    ): void => {
        const ptype = options?.module_type
            ? options.module_type
            : self.module_type;
        const shouldFreeze = options.frozen ?? self.frozen;

        switch (ptype) {
            case "fn":
            case "comp":
            case "node_module":
                self.frozen = shouldFreeze;
                self.module = initial;
                self.initial = initial;
                break;

            case "class_fn_pool":
            default:
                self.frozen = false;
                self.module = { ...initial };
                self.initial = { ...initial };
                break;
        }

        self.module_type = ptype;

        if (options?.broadcast) self.broadcast = options.broadcast;
        if (options?.module_type) self.module_type = options.module_type;
        if (options?.schema) self.schema = options.schema;
        if (options?.lifecycle) self.lifecycle = options.lifecycle;
        if (options?.conflict) self.conflict = options.conflict;
        if (options?.conflictStrategy) self.conflictStrategy = options.conflictStrategy;

        if (options?.history) {
            self.historyMax = Math.max(1, options.history.max ?? 50);
            self.history = [{
                k: 0,
                value: initial,
                timestamp: Date.now(),
                source: "initial",
            }];
            self.historyIndex = 0;
        }
    },

    read: (): T => self.module,

    peek: (): T => self.module,

    write: (
        incoming: any | ((prev: T) => T),
        options: AtomWriteOptions = {},
    ): T => {
        if (self.frozen) return self.peek();

        const prev = self.module;
        const resolved = incoming;

        const next = self.module_type === "class_fn_pool"
            ? resolved
                ? { ...prev, ...resolved }
                : prev
            : resolved;

        self.module = next;
        self.hasWritten = true;
        self.version += 1;
        self.lastWriteSource = options.source ?? "write";
        self.assertContracts(next, prev);

        for (const l of self.listeners) {
            try {
                l(next);
            } catch (e) {}
        }

        root.bus.send(self.key, next, self.s, self.m, self.r);

        for (const w of self.waiters) {
            try {
                w(next);
            } catch (e) {}
        }

        if (options.recordHistory !== false) {
            self.recordHistory(prev, next, self.lastWriteSource);
        }

        self.recordCausal(next, self.lastWriteSource, options.cause);
        self.waiters.clear();

        self.notifyListeners({
            key: self.key,
            previous: prev,
            current: next,
            version: self.version,
            timestamp: Date.now(),
            source: self.lastWriteSource,
        });

        return next;
    },

    register: (name: string, val: any): T =>
        self.write((prev: A) => ({ ...prev, [name]: val })),

    unregister: (name: string): T =>
        self.write((prev: A) => {
            const { [name]: _gone, ...rest } = prev;
            return rest;
        }),

    slot: (name: string): any => self.module[name],

    on: (listener: RiftListener<any>): void => {
        self.listeners.add(listener);
        listener(self.module);
    },

    waitFor: (): Promise<T> =>
        new Promise((resolve) => {
            self.waiters.add(resolve);
        }),

    waitForSlot: async (name: string, timeoutMs = 0): Promise<any> => {
        const existing = self.module[name];
        if (existing !== undefined) return existing;

        if (!timeoutMs) {
            const next = await self.waitFor();
            return (next as Record<string, any>)[name];
        }

        return Promise.race([
            self.waitFor().then(
                (next) => (next as Record<string, any>)[name],
            ),
            new Promise((_, reject) =>
                setTimeout(
                    () => reject(
                        new Error(
                            `[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}m`,
                        ),
                    ),
                    timeoutMs,
                ),
            ),
        ]);
    },

    inspect: (): T => ({ ...self.module }),

    setSchema: (schema?: SignalSchema) => {
        self.schema = schema;
        return self;
    },

    setLifecycle: (lifecycle?: SignalLifecycle<T>) => {
        self.lifecycle = lifecycle;
        return self;
    },

    setConflict: (
        conflict?: ConflictPolicy,
        strategy?: "latest" | "fail" | "accumulate",
    ) => {
        self.conflict = conflict;
        self.conflictStrategy = strategy;
        return self;
    },

    setFrozen: (): void => {
        self.frozen = true;
    },

    setUnfrozen: (): void => {
        self.frozen = false;
    },

    setSealed: (): void => {
        self.sealed = true;
    },

    setUnsealed: (): void => {
        self.sealed = false;
    },

    setBoundary: (): void => {},

    validate: (next: T): T => {
        if (!self.schema) return next;
        return root.signals._.validateSchema(self.schema, next);
    },

    assertContracts: (next: T, previous: T): void => {
        for (const contract of self.contracts.values()) {
            let valid = false;

            try {
                valid = contract.postcondition(next, previous);
            } catch (error) {
                console.error(
                    `[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`,
                    error,
                );
            }

            if (valid) continue;

            const message =
                `[Sauron] contract "${contract.name}" violated ` +
                `on write to "${String(self.key)}"`;

            if (contract.block) {
                throw new Error(message);
            }

            console.error(message);
        }
    },

    recordHistory: (previous: T, next: T, source?: string): void => {
        if (!self.history) return;

        self.history.splice(self.historyIndex + 1);

        const last = self.history[self.history.length - 1];

        self.history.push({
            k: (last?.k ?? -1) + 1,
            prev: previous,
            value: next,
            next,
            timestamp: Date.now(),
            source,
            undone: false,
        });

        while (self.history.length > self.historyMax) {
            self.history.shift();
        }

        self.historyIndex = self.history.length - 1;
    },

    getHistory: (): readonly AtomHistoryEntry<T>[] =>
        self.history ?? [],

    timeTravel: (steps = 1): T | undefined => {
        if (!self.history?.length || steps <= 0) return undefined;

        const idx = Math.max(
            0,
            Math.min(self.history.length - 1, self.historyIndex + steps),
        );

        if (idx === self.historyIndex) return undefined;

        self.historyIndex = idx;
        return self.replayHistory("time-travel");
    },

    back: (steps = 1): T | undefined =>
        self.timeTravel(-Math.abs(steps)),

    forward: (steps = 1): T | undefined =>
        self.timeTravel(Math.abs(steps)),

    goto: (sequence: number): T | undefined => {
        if (!self.history?.length) return undefined;

        const idx = self.history.findIndex(
            (entry) => entry.k === sequence,
        );

        if (idx < 0 || idx === self.historyIndex) return undefined;

        self.historyIndex = idx;
        return self.replayHistory("time-travel.goto");
    },

    replayHistory: (source: string): T => {
        const entry = self.history![self.historyIndex];

        return self.write(entry.value as T, {
            source,
            cause: "time-travel",
            triggerRender: true,
            recordHistory: false,
        });
    },

    recordCausal: (
        value: T,
        source?: string,
        cause?: string,
    ): void => {
        self.causal.push({
            value,
            source,
            cause,
            timestamp: Date.now(),
        } as CausalEntry);

        if (self.causal.length > 100) {
            self.causal.shift();
        }
    },

    recordMetrics: (): void => {
        if (!self.metrics) return;

        self.metrics.writeCount += 1;
        self.metrics.lastWrite = Date.now();
    },
	
    notifyListeners: (change: Change<T>): void => {
        const listeners = root.wr._listeners.get(self.key);
        if (!listeners) return;

        for (const listener of listeners) {
            try {
                listener(change);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`,
                    error,
                );
            }
        }
    },

    resolveWaiters: (value: T): void => {
        const waiting = root.wr._waiters.get(self.key);
        if (!waiting) return;

        root.wr._waiters.delete(self.key);

        for (const resolve of waiting) {
            try {
                resolve(value);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`,
                    error,
                );
            }
        }
    },

    notifyExternalSubscribers: (): void => {
        const subscribers = root.state._externalSubscribers.get(self.key);
        if (!subscribers) return;

        for (const callback of subscribers) {
            try {
                callback();
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`,
                    error,
                );
            }
        }
    },
}));
// #endregion
//                                                                                                      [MODULE_STORAGE_PROTON]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const MODULE_STORAGE = MASTER_ATOM(
    "MODULE_STORAGE_PROTON",
    ({ self, master, config }: any) => ({
        k: config.k ?? "__SIGNALS__",
        s: config.s ?? "SAURON_SLAVE_REG",
        m: config.m ?? "SAURON_MASTER_REG",
        r: config.r ?? "THE_WIZARD_ROOT_REG",

        slots: new Map<S, MODULE_SLOT>(),

        addr: (
            k: S = self.k,
            s: S = self.s,
            c?: CACHE,
            m: S = self.m,
            r: S = self.r,
        ): S =>
            c !== undefined
                ? `[${r}][${m}][${c}][${s}][${k}]` as S
                : `[${r}][${m}][${s}][${k}]` as S,

        set: (
            value: MDV,
            k: S,
            s: S,
            c: S,
            m: S,
            r: S,
            options: {
                owner?: OWNERSHIP;
                onUnload?: (v: MDV) => V;
            } = {},
        ): MDV => {
            const key = self.addr(k, s, c, m, r);
            const prev = self.slots.get(key);

            prev?.onUnload?.(prev.value);

            self.slots.set(key, {
                value,
                registeredAt: Date.now(),
                owner: options.owner ?? "bespoke",
                onUnload: options.onUnload,
            });

            return value;
        },

        get: (
            k: S,
            s: S,
            m: S,
            r: S,
        ): MODULE_VALUE | undefined =>
            self.slots.get(self.addr(k, s, undefined, m, r))?.value,

        has: (
            k: S,
            s: S,
            m: S,
            r: S,
        ): B =>
            self.slots.has(self.addr(k, s, undefined, m, r)),

        drop: (
            k: S,
            s: S,
            m: S,
            r: S,
            force: B = false,
        ): B => {
            const key = self.addr(k, s, undefined, m, r);
            const slot = self.slots.get(key);

            if (!slot) return false;
            if (slot.owner === "shared" && !force) return false;

            slot.onUnload?.(slot.value);
            self.slots.delete(key);

            return true;
        },

        list: (
            s: S,
            m: S,
            r: S,
        ): Array<{ k: S; owner: OWNERSHIP; registeredAt: N }> => {
            const prefix = `[${r}][${m}][${s}][`;
            const out: Array<{
                k: S;
                owner: OWNERSHIP;
                registeredAt: N;
            }> = [];

            for (const [key, slot] of self.slots) {
                if (key.startsWith(prefix)) {
                    out.push({
                        k: key.slice(0, -1).slice(prefix.length) as S,
                        owner: slot.owner,
                        registeredAt: slot.registeredAt,
                    });
                }
            }

            return out;
        },

        inspect: (): Array<{ address: S; owner: OWNERSHIP; kind: S }> =>
            [...self.slots.entries()].map(([address, slot]) => ({
                address,
                owner: slot.owner,
                kind: typeof slot.value === "function"
                    ? (slot.value.name || "anonymous-fn") as S
                    : "object" as S,
            })),

        purge: (): void => {
            for (const slot of self.slots.values()) {
                slot.onUnload?.(slot.value);
            }
            self.slots.clear();
        },
    }),
);
// #endregion
//                                                                                                                [MODULE_ATOM]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const S_CLASS_ATOM = BIND_ATOM("S_CLASS_ATOM", <T = any>({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    initial: undefined as T,
    key: config.k ?? "__SIGNALS__",
    module: undefined as T,
    module_type: "class_fn_pool",
    version: 0,
    sealed: false,
    frozen: true,
    hasWritten: false,
    persisted: false,

    listeners: new Set<(change: Change<T>) => void>(),
    waiters: new Set<(value: T) => void>(),
    undoStack: undefined as HistoryEntry<T>[] | undefined,

    conflict: undefined as ConflictPolicy | undefined,
    historyIndex: 0,
    history: undefined as AtomHistoryEntry<T>[] | undefined,
    historyMax: 50,
    lifecycle: undefined as SignalLifecycle | undefined,
    async: undefined as AsyncSignalState<T> | undefined,
    metrics: undefined as Metrics | undefined,
    conflictStrategy: undefined as "latest" | "fail" | "accumulate" | undefined,

    schema: undefined as SignalSchema | undefined,
    contracts: new Map<string, AtomContract<T>>(),
    causal: [] as CausalEntry[],
    lastWriteSource: undefined as string | undefined,

    batches: new Set<SignalKey>(),
    batchQueue: new Map<SignalKey, unknown>(),
    batchOpts: new Map<SignalKey, { triggerRender?: boolean }>(),

    broadcast: true,
    triggerRender: true,
    boundary: "same-app" as Boundary,

    initialize: (
        initial: T,
        options: MODULE_OPTIONS = {
            module_type: "class_fn_pool",
            broadcast: true,
            frozen: true,
        },
    ): void => {
        const ptype = options?.module_type
            ? options.module_type
            : self.module_type;
        const shouldFreeze = options.frozen ?? self.frozen;

        switch (ptype) {
            case "fn":
            case "comp":
            case "node_module":
                self.frozen = shouldFreeze;
                self.module = initial;
                self.initial = initial;
                break;

            case "class_fn_pool":
            default:
                self.frozen = false;
                self.module = { ...initial };
                self.initial = { ...initial };
                break;
        }

        self.module_type = ptype;

        if (options?.broadcast) self.broadcast = options.broadcast;
        if (options?.module_type) self.module_type = options.module_type;
        if (options?.schema) self.schema = options.schema;
        if (options?.lifecycle) self.lifecycle = options.lifecycle;
        if (options?.conflict) self.conflict = options.conflict;
        if (options?.conflictStrategy) self.conflictStrategy = options.conflictStrategy;

        if (options?.history) {
            self.historyMax = Math.max(1, options.history.max ?? 50);
            self.history = [{
                k: 0,
                value: initial,
                timestamp: Date.now(),
                source: "initial",
            }];
            self.historyIndex = 0;
        }
    },

    read: (): T => self.module,

    peek: (): T => self.module,

    write: (
        incoming: any | ((prev: T) => T),
        options: AtomWriteOptions = {},
    ): T => {
        if (self.frozen) return self.peek();

        const prev = self.module;
        const resolved = incoming;

        const next = self.module_type === "class_fn_pool"
            ? resolved
                ? { ...prev, ...resolved }
                : prev
            : resolved;

        self.module = next;
        self.hasWritten = true;
        self.version += 1;
        self.lastWriteSource = options.source ?? "write";
        self.assertContracts(next, prev);

        for (const l of self.listeners) {
            try {
                l(next);
            } catch (e) {}
        }

        root.bus.send(self.key, next, self.s, self.m, self.r);

        for (const w of self.waiters) {
            try {
                w(next);
            } catch (e) {}
        }

        if (options.recordHistory !== false) {
            self.recordHistory(prev, next, self.lastWriteSource);
        }

        self.recordCausal(next, self.lastWriteSource, options.cause);
        self.waiters.clear();

        self.notifyListeners({
            key: self.key,
            previous: prev,
            current: next,
            version: self.version,
            timestamp: Date.now(),
            source: self.lastWriteSource,
        });

        return next;
    },

    register: (name: string, val: any): T =>
        self.write((prev: A) => ({ ...prev, [name]: val })),

    unregister: (name: string): T =>
        self.write((prev: A) => {
            const { [name]: _gone, ...rest } = prev;
            return rest;
        }),

    slot: (name: string): any => self.module[name],

    on: (listener: RiftListener<any>): void => {
        self.listeners.add(listener);
        listener(self.module);
    },

    waitFor: (): Promise<T> =>
        new Promise((resolve) => {
            self.waiters.add(resolve);
        }),

    waitForSlot: async (name: string, timeoutMs = 0): Promise<any> => {
        const existing = self.module[name];
        if (existing !== undefined) return existing;

        if (!timeoutMs) {
            const next = await self.waitFor();
            return (next as Record<string, any>)[name];
        }

        return Promise.race([
            self.waitFor().then(
                (next) => (next as Record<string, any>)[name],
            ),
            new Promise((_, reject) =>
                setTimeout(
                    () => reject(
                        new Error(
                            `[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}m`,
                        ),
                    ),
                    timeoutMs,
                ),
            ),
        ]);
    },

    inspect: (): T => ({ ...self.module }),

    setSchema: (schema?: SignalSchema) => {
        self.schema = schema;
        return self;
    },

    setLifecycle: (lifecycle?: SignalLifecycle<T>) => {
        self.lifecycle = lifecycle;
        return self;
    },

    setConflict: (
        conflict?: ConflictPolicy,
        strategy?: "latest" | "fail" | "accumulate",
    ) => {
        self.conflict = conflict;
        self.conflictStrategy = strategy;
        return self;
    },

    setFrozen: (): void => {
        self.frozen = true;
    },

    setUnfrozen: (): void => {
        self.frozen = false;
    },

    setSealed: (): void => {
        self.sealed = true;
    },

    setUnsealed: (): void => {
        self.sealed = false;
    },

    setBoundary: (): void => {},

    validate: (next: T): T => {
        if (!self.schema) return next;
        return root.signals._.validateSchema(self.schema, next);
    },

    assertContracts: (next: T, previous: T): void => {
        for (const contract of self.contracts.values()) {
            let valid = false;

            try {
                valid = contract.postcondition(next, previous);
            } catch (error) {
                console.error(
                    `[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`,
                    error,
                );
            }

            if (valid) continue;

            const message =
                `[Sauron] contract "${contract.name}" violated ` +
                `on write to "${String(self.key)}"`;

            if (contract.block) {
                throw new Error(message);
            }

            console.error(message);
        }
    },

    recordHistory: (previous: T, next: T, source?: string): void => {
        if (!self.history) return;

        self.history.splice(self.historyIndex + 1);

        const last = self.history[self.history.length - 1];

        self.history.push({
            k: (last?.k ?? -1) + 1,
            prev: previous,
            value: next,
            next,
            timestamp: Date.now(),
            source,
            undone: false,
        });

        while (self.history.length > self.historyMax) {
            self.history.shift();
        }

        self.historyIndex = self.history.length - 1;
    },

    getHistory: (): readonly AtomHistoryEntry<T>[] =>
        self.history ?? [],

    timeTravel: (steps = 1): T | undefined => {
        if (!self.history?.length || steps <= 0) return undefined;

        const idx = Math.max(
            0,
            Math.min(self.history.length - 1, self.historyIndex + steps),
        );

        if (idx === self.historyIndex) return undefined;

        self.historyIndex = idx;
        return self.replayHistory("time-travel");
    },

    back: (steps = 1): T | undefined =>
        self.timeTravel(-Math.abs(steps)),

    forward: (steps = 1): T | undefined =>
        self.timeTravel(Math.abs(steps)),

    goto: (sequence: number): T | undefined => {
        if (!self.history?.length) return undefined;

        const idx = self.history.findIndex(
            (entry) => entry.k === sequence,
        );

        if (idx < 0 || idx === self.historyIndex) return undefined;

        self.historyIndex = idx;
        return self.replayHistory("time-travel.goto");
    },

    replayHistory: (source: string): T => {
        const entry = self.history![self.historyIndex];

        return self.write(entry.value as T, {
            source,
            cause: "time-travel",
            triggerRender: true,
            recordHistory: false,
        });
    },

    recordCausal: (
        value: T,
        source?: string,
        cause?: string,
    ): void => {
        self.causal.push({
            value,
            source,
            cause,
            timestamp: Date.now(),
        } as CausalEntry);

        if (self.causal.length > 100) {
            self.causal.shift();
        }
    },

    recordMetrics: (): void => {
        if (!self.metrics) return;

        self.metrics.writeCount += 1;
        self.metrics.lastWrite = Date.now();
    },

    notifyListeners: (change: Change<T>): void => {
        const listeners = root.wr._listeners.get(self.key);
        if (!listeners) return;

        for (const listener of listeners) {
            try {
                listener(change);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`,
                    error,
                );
            }
        }
    },

    resolveWaiters: (value: T): void => {
        const waiting = root.wr._waiters.get(self.key);
        if (!waiting) return;

        root.wr._waiters.delete(self.key);

        for (const resolve of waiting) {
            try {
                resolve(value);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`,
                    error,
                );
            }
        }
    },

    notifyExternalSubscribers: (): void => {
        const subscribers = root.state._externalSubscribers.get(self.key);
        if (!subscribers) return;

        for (const callback of subscribers) {
            try {
                callback();
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`,
                    error,
                );
            }
        }
    },
}));
// #endregion
//                                                                                                     [S_CLASS_STORAGE_PROTON]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const S_CLASS_STORAGE = MASTER_ATOM(
    "S_CLASS_STORAGE_PROTON",
    ({ self, master, config }: any) => ({
        k: config.k ?? "__SIGNALS__",
        s: config.s ?? "SAURON_SLAVE_REG",
        m: config.m ?? "SAURON_MASTER_REG",
        r: config.r ?? "THE_WIZARD_ROOT_REG",

        store: new Map<string, Record<string, any>>(),
        addr: (
            k: string,
            sl: string,
            ms: string,
            rt: string,
        ): string => `[${rt}][${ms}][${sl}][${k}]`,

        set: (
            k: string,
            incoming: Record<string, any> | undefined,
            sl: string,
            ms: string,
            rt: string,
            bus: any,
        ): Record<string, any> => {
            const key = self.addr(k, sl, ms, rt);
            const prev = self.store.get(key) ?? {};
            const next = incoming ? { ...prev, ...incoming } : prev;

            self.store.set(key, next);
            bus.send.emit(k, next, sl, ms, rt);

            return next;
        },

        get: (
            k: string,
            sl: string,
            ms: string,
            rt: string,
        ): Record<string, any> | undefined =>
            self.store.get(self.addr(k, sl, ms, rt)),

        has: (
            k: string,
            sl: string,
            ms: string,
            rt: string,
        ): boolean => self.store.has(self.addr(k, sl, ms, rt)),

        inspect: (): Array<{ address: string; value: Record<string, any> }> =>
            [...self.store.entries()].map(([address, value]) => ({
                address,
                value,
            })),

        clear: (): void => {
            self.store.clear();
        },

        get size(): number {
            return self.store.size;
        },
    }),
);
// #endregion
//                                                                                                                  MASTER ATOM
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const ATOM = MASTER_ATOM('ATOM',({ self, config, gate, subscription, SEALED_ERROR, conflict }: any) => ({
		k: config.k ?? "__SIGNALS__",
		s: config.s ?? "SAURON_SLAVE_REG",
		m: config.m ?? "SAURON_MASTER_REG",
		r: config.r ?? "THE_WIZARD_ROOT_REG",
		value: config.value as U,
		read() {
			const r = self.value;
			if (r !== null) { if (GATE !== undefined && gate.sealed) throw new SEALED_ERROR.log(self.k); }
			return self.value;
		},
		write1(next: U): B {
			const r = self.registry;
			if (r === null) { self.value = next; return true; }
			if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
			const previous = self.value;
			if (Object.is(previous, next)) return false;
			self.value = next;
			if (subscription !== undefined) subscription.notify({ key: self.key, previous, current: next });
			return true;
		},
		write(next) {
			if (gate?.sealed || gate?.frozen) return false;
			const current = self.value;
			if (Object.is(current, next)) return false;
		  
			const decision = conflict.check({
			  current,
			  incoming: next,
			  previous: self.previous ?? current,
			});
		  
			if (!decision.accepted) {
			  return false;
			}
		  
			const resolved = decision.value;
		  
			if (Object.is(current, resolved)) return false;
		  
			self.useCell().set("value", resolved);
		  
			return true;
		  },
		subscribe(listener: LISTENER<A>): UNSUBSCRIBE { return subscription.subscribe(listener); },
		waitFor() {},
}));
// #endregion
//                                                                                                                MASTER VERTEX
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const VERTEX = MASTER_ATOM("VERTEX", ({ self,config, gate, subscription, SEALED_ERROR }: any) => ({
    value: config.value as U,
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
    pid: 1,
    slabRegistry: new Map<number, Uint8Array>(),
    ...(() => {
        const capacity = (() => {
            const need = (value: U): N =>
                Array.isArray(value) ? value.length + 1 :
                value instanceof Map || value instanceof Set ? value.size + 1 : 2;

            const size = need(self.value);

            return size > 8192 ? 64 :
                size <= 64 ? 64 :
                size <= 128 ? 128 :
                size <= 256 ? 256 :
                size <= 1024 ? 1024 : 8192;
        })();

        const lane = new Int32Array(config.capacity ?? capacity).fill(-1);
        const ops = new Uint8Array(config.capacity ?? capacity);
        const posX = new Float32Array(config.capacity ?? capacity);
        const posY = new Float32Array(config.capacity ?? capacity);
        const val = new Int32Array(config.capacity ?? capacity);
        const freePids = new Int32Array(config.capacity ?? capacity);

        let freeHead = 0;
        let freeTail = 0;
        let laneLen = 0;
        let nextId = 0;

        const ids = new Map<S, N>();
        const names: S[] = [];
        const pids = new Map<S, N>();
        const addresses = new Map<N, [N, N, N, N]>();
        const listener: Array<ListenerFn | UD> = [];
        const pidToHash = new Uint32Array(config.capacity ?? capacity);
        const channelMap = new Int32Array(config.capacity ?? capacity);
        const subChannelMap = new Int32Array(config.capacity ?? capacity);
        const tripChannelMap = new Int32Array(config.capacity ?? capacity);
        const dirtyPids = new Int32Array(config.capacity ?? capacity);

        for (let i = 1; i < lane.length; i++) {
            freePids[freeTail++] = i;
        }

        lane[0] = TRACER_PID;
        val[TRACER_PID] = 0;
        ops[TRACER_PID] = OP.NONE;
        laneLen = 1;

        return {
            capacity: lane.length,
            lane,
            ops,
            posX,
            posY,
            val,
            freePids,
            freeHead,
            freeTail,
            laneLen,
            ids,
            names,
            pids,
            addresses,
            nextId,
            listener,
            pidToHash,
            nextAllocatedPid: 1,
            channelMap,
            subChannelMap,
            tripChannelMap,
            dirtyPids,
        };
    })(),
    need(value: U): N {        return Array.isArray(value) ? value.length + 1 : value instanceof Map || value instanceof Set ? value.size + 1 : 2;    },
    BINARY_CODEC: new BINARY_CODEC(),
    ...(() => {
        const codec = new BINARY_CODEC();

        return {
            codec,
            encodeToSlab(pid: N, value: A): N {
                if (typeof value === "number") {
                    self.slabRegistry.delete(pid);
                    return value;
                }

                const bytes = codec.encode_msg(value);
                self.slabRegistry.set(pid, bytes);
                return bytes.byteLength;
            },

            clearSlab(pid: N): void {
                self.slabRegistry.delete(pid);
            },

            hasSlab(pid: N): B {
                return self.slabRegistry.has(pid);
            },

            readSlabValue<T>(pid: N): T | NU {
                const bytes = self.slabRegistry.get(pid);
                return bytes ? codec.decode_msg(bytes) as T : null;
            },
        };
    })(),
    ...(() => {
        const codec = self.BINARY_CODEC;
        const x = self.posX;
        const y = self.posY;
        const pid = self.pid;

        const initialize = () => {
            const value = self.value;
            const v = codec.encode_msg(value);
            const position = self.getPosition(pid);

            if (self.need(value) > 8192) {
                self.encodeToSlab(pid, v);
                x[pid] = position.x;
                y[pid] = position.y;
                self.ops[pid] = OP.WRITE;
                self.lane[self.laneLen++] = pid;
            } else {
                const vb = codec.encode_msg(v);
                self.edit(position.x, position.y, vb, pid);
            }
        };

        return { initialize };
    })(),
    check_size_and_edit(value: A): V {
        const v = self.BINARY_CODEC.encode_msg(value);
        const { x, y } = self.getPosition(self.pid);

        if (self.need(value) > 8192) {
            self.encodeToSlab(self.pid, v);
            self.posX[self.pid] = x;
            self.posY[self.pid] = y;
            self.ops[self.pid] = OP.EDIT;
            self.lane[self.laneLen++] = self.pid;
        } else {
            self.create(x, y, v, self.pid);
        }
    },
    getX(pid: N): N { return self.posX[pid]; },
    getY(pid: N): N { return self.posY[pid]; },
    getPosition(pid: N): { x: N; y: N } { return { x: self.posX[pid], y: self.posY[pid] }; },
    create(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.WRITE; self.lane[self.laneLen++] = pid; },
    edit(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.EDIT; },
    subscribeValue(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SUB; },
    readValue(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.READ; },
    effect(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.EFFECT; },
    delete(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DELETE; },
    compact(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.COMPACT; },
    none(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.NONE; },
    send(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SEND; },
    sub(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.SUB; },
    duplex(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DUPLEX; },
    stream(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.STREAM; },
    drc(x: N, y: N, v: U8 | N, pid: Pid): V { self.posX[pid] = x; self.posY[pid] = y; self.val[pid] = v; self.ops[pid] = OP.DRC; },
    reclaimPid(pid: N): V { self.posX[pid] = 0; self.posY[pid] = 0; self.val[pid] = 0; self.ops[pid] = OP.NONE; self.freePids[self.freeTail] = pid; self.freeTail = (self.freeTail + 1) % self.capacity; },
    pass(): void {
        const len = self.laneLen;
        let writeIdx = 0;
        let i = 0;
        const unrolledLimit = len - (len % 4);

        for (; i < unrolledLimit; i += 4) {
            let pid = self.lane[i];

            if (pid === TRACER_PID) {
                self.val[TRACER_PID]++;
                self.lane[writeIdx++] = pid;
            } else if (self.ops[pid] === OP.DELETE) {
                self.reclaimPid(pid);
            } else {
                self.ops[pid] = OP.NONE;
                self.lane[writeIdx++] = pid;
            }

            pid = self.lane[i + 1];

            if (pid === TRACER_PID) {
                self.val[TRACER_PID]++;
                self.lane[writeIdx++] = pid;
            } else if (self.ops[pid] === OP.DELETE) {
                self.reclaimPid(pid);
            } else {
                self.ops[pid] = OP.NONE;
                self.lane[writeIdx++] = pid;
            }

            pid = self.lane[i + 2];

            if (pid === TRACER_PID) {
                self.val[TRACER_PID]++;
                self.lane[writeIdx++] = pid;
            } else if (self.ops[pid] === OP.DELETE) {
                self.reclaimPid(pid);
            } else {
                self.ops[pid] = OP.NONE;
                self.lane[writeIdx++] = pid;
            }

            pid = self.lane[i + 3];

            if (pid === TRACER_PID) {
                self.val[TRACER_PID]++;
                self.lane[writeIdx++] = pid;
            } else if (self.ops[pid] === OP.DELETE) {
                self.reclaimPid(pid);
            } else {
                self.ops[pid] = OP.NONE;
                self.lane[writeIdx++] = pid;
            }
        }

        for (; i < len; i++) {
            const pid = self.lane[i];

            if (pid === TRACER_PID) {
                self.val[TRACER_PID]++;
                self.lane[writeIdx++] = pid;
            } else if (self.ops[pid] === OP.DELETE) {
                self.reclaimPid(pid);
            } else {
                self.ops[pid] = OP.NONE;
                self.lane[writeIdx++] = pid;
            }
        }

        self.laneLen = writeIdx;
    },
    get tick(): N { return self.val[TRACER_PID]; },
    get length(): N { return self.laneLen; },
    getLoadFactor(): N {
        const activeCount = Math.max(0, self.laneLen - 1);
        return activeCount / (self.capacity - 1);
    },
    read() {
        const r = self.registry;
        if (r !== null) {  if (gate !== undefined && gate.sealed)  throw new SEALED_ERROR.log(self.key);  }
        return self.value;
    },
    write(next: A): B {
        const r = self.registry;
        if (r === null) { self.value = next; return true; }

        if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
        const previous = self.value;
        if (Object.is(previous, next)) return false;
        self.value = next;
 
        if (subscription !== undefined)  subscription.notify({ key: self.key, previous, current: next });
        return true;
    },
    subscribe(listener: LISTENER<A>): UNSUBSCRIBE {return subscription.subscribe(listener); },
    waitFor() {},
}));
// #endregion
//                                                                                                                     CONFLICT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const CONFLICT = BIND_ATOM("CONFLICT", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    conflictStrategy: (config.conflictStrategy ?? "latest") as "latest" | "fail" | "accumulate",
    conflict: config.conflict,

    resolve<T>(current: T, incoming: T): T {
        switch (self.conflictStrategy) {
            case "fail":
                throw new Error("SOUL_NEUTRON conflict: concurrent values cannot be merged");

            case "accumulate":
                if (typeof current === "number" && typeof incoming === "number") return (current + incoming) as T;
                if (Array.isArray(current) && Array.isArray(incoming)) return [...current, ...incoming] as T;
                if (typeof current === "string" && typeof incoming === "string") return (current + incoming) as T;
                throw new TypeError("SOUL_NEUTRON accumulate conflict requires matching number, string, or array values");

            case "latest":
            default:
                return incoming;
        }
    },
}));
// #endregion
//                                                                                                                      CONTEXT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const CONTEXT = BIND_ATOM("CONTEXT", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    store: new Map<S, A>(),

    set(key: S, value: A): void {
        self.store.set(key, value);
        // if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('context_set', undefined, undefined, undefined, { key });
    },

    get(key: S): A | undefined {
        return self.store.get(key);
    },

    /** Read with a fallback — context lookups always expect a default. */
    getOr(key: S, fallback: A): A {
        return self.store.has(key) ? self.store.get(key) as A : fallback;
    },

    has(key: S): B {
        return self.store.has(key);
    },

    delete(key: S): B {
        return self.store.delete(key);
    },

    keys(): S[] {
        return [...self.store.keys()];
    },

    /** Full snapshot, for hand-off / persistence / diagnostics. */
    snapshot(): Record<S, A> {
        return Object.fromEntries(self.store);
    },

    /** Restore from a snapshot. Replaces the store wholesale. */
    restore(snap: Record<S, A>): void {
        self.store = new Map(Object.entries(snap));
    },

    /** Merge a partial snapshot over the current store (existing keys win unless overwrite). */
    merge(snap: Record<S, A>, overwrite = false): void {
        for (const [k, v] of Object.entries(snap)) {
            if (!overwrite && self.store.has(k)) continue;
            self.store.set(k, v);
        }
    },

    /** Scoped read: return a sub-object of the store whose keys start with the prefix. */
    scoped(prefix: S): Record<S, A> {
        const out: Record<S, A> = {};
        for (const [k, v] of self.store) {
            if (k.startsWith(prefix)) out[k] = v;
        }
        return out;
    },

    purge(): void {
        self.store.clear();
    },
}));
// #endregion
//                                                                                                                   ENCRYPTION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const ENCRYPTION = BIND_ATOM("ENCRYPTION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    encryptionConfigs: [] as EncryptionConfig[],

    getEncryptionConfigs(): EncryptionConfig[] {
        return [...self.encryptionConfigs];
    },

    configureEncryption(config: EncryptionConfig): void {
        self.encryptionConfigs.push(config);
    },
}));
// #endregion
//                                                                                                                       GROUPS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const GROUPS = BIND_ATOM("GROUPS", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    groups: new Map<string, {
        channels: Set<string>;
        enabled: boolean;
        middleware: ((msg: CSEMessage) => boolean | void)[];
    }>(),

    createGroup(name: string, channels: string[] = [], options?: GroupOptions): void {
        self.groups.set(name, {
            channels: new Set(channels),
            enabled: options?.enabled ?? true,
            middleware: options?.middleware ? [options.middleware] : [],
        });
    },

    deleteGroup(name: string): void {
        self.groups.delete(name);
    },

    enableGroup(name: string): void {
        const g = self.groups.get(name);
        if (!g) return;
        g.enabled = true;
    },

    disableGroup(name: string): void {
        const g = self.groups.get(name);
        if (!g) return;
        g.enabled = false;
    },

    addToGroup(name: string, rt: string): void {
        const g = self.groups.get(name);
        if (!g) return;
        g.channels.add(rt);
    },

    removeFromGroup(name: string, rt: string): void {
        const g = self.groups.get(name);
        if (!g) return;
        g.channels.delete(rt);
    },

    async groupBroadcast(
        name: string,
        p: any,
        sl = self.sl,
        ms = self.ms,
        rt = self.rt,
        ttl?: number,
    ): Promise<void> {
        const g = self.groups.get(name);
        if (!g || !g.enabled) return;

        await Promise.all(
            [...g.channels].map(ch => self.broadcast(p, ch, sl, ms, rt, ttl)),
        );
    },

    groupUse(name: string, fn: (msg: CSEMessage) => boolean | void): void {
        const g = self.groups.get(name);
        if (!g) return;
        g.middleware.push(fn);
    },

    getAllGroups(): Record<string, {
        channels: string[];
        enabled: boolean;
        middlewareCount: number;
    }> {
        const out: Record<string, {
            channels: string[];
            enabled: boolean;
            middlewareCount: number;
        }> = {};

        for (const [name, g] of self.groups) {
            out[name] = {
                channels: [...g.channels],
                enabled: g.enabled,
                middlewareCount: g.middleware.length,
            };
        }

        return out;
    },
}));
// #endregion
//                                                                                                                      HISTORY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const HISTORY = BIND_ATOM("HISTORY", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    history: new Map<string, CSEMessage[]>(),
    defaultLimit: 128,
    limits: new Map<string, number>(),

    /** Append one message. Returns the entry so callers can thread it onward. */
    record(rt: string, msg: CSEMessage): CSEMessage {
        let list = self.history.get(rt);
        if (!list) {
            list = [];
            self.history.set(rt, list);
        }

        list.push(msg);

        const limit = self.limits.get(rt) ?? self.defaultLimit;
        if (list.length > limit) list.splice(0, list.length - limit);

        return msg;
    },

    setChannelLimit(rt: string, limit: number): void {
        self.limits.set(rt, limit);

        const list = self.history.get(rt);
        if (list && list.length > limit) {
            list.splice(0, list.length - limit);
        }
    },

    getChannelLimit(rt: string): number {
        return self.limits.get(rt) ?? self.defaultLimit;
    },

    getRecentHistory(rt: string, limit = 3): CSEMessage[] {
        return (self.history.get(rt) ?? []).slice(-limit);
    },

    getChannelHistory(rt: string): CSEMessage[] {
        return [...(self.history.get(rt) ?? [])];
    },

    getAllHistoryChannels(): string[] {
        return [...self.history.keys()];
    },

    getChannelDepth(rt: string): number {
        return (self.history.get(rt) ?? []).length;
    },

    getHistorySizes(): Record<string, number> {
        const out: Record<string, number> = {};
        for (const [ch, list] of self.history) out[ch] = list.length;
        return out;
    },

    clearChannel(rt: string): void {
        self.history.delete(rt);
    },

    purge(): void {
        self.history.clear();
        self.limits.clear();
    },
}));
// #endregion
//                                                                                                                         META
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const META = BIND_ATOM("META", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    // ── your blob, organized ───────────────────────────────────────────────────────────────────
    _batches: new Set<string | symbol>(),
    _batchQueue: new Map<string | symbol, A>(),
    _batchOpts: new Map<string | symbol, { triggerRender?: B }>(),
    _globalListeners: new Set<(key: S | symbol, value: A) => void>(),
    _global_subscribers: new Map<S, Set<() => void>>(),
    _sys: new Map<S, SysEntry>(),
    _sealed: new Set<S>(),
    _frozen: new Set<S>(),
    _keyContracts: new Map<S, Set<S>>(),
    _conflicts: new Map<S, ConflictPolicy>(),
    _conflictStrategies: new Map<S, "latest" | "fail" | "accumulate">(),
    _causal: new Map<S, CausalEntry[]>(),
    _scopes: new Map<S, SignalScope>(),
    _msgHandlers: new Map<S, Set<(p: A) => void>>(),
    _lastWriteSource: new Map<S, S>(),
    _mainMap: new Map<A, S>(),
    _tupleCache: new Map<S, S>(),
    _weakMap: new WeakMap<object, S>(),
    _boundaries: new WeakMap<object, S>(),

    // ── sys entries ────────────────────────────────────────────────────────────────────────────
    /** Register or update a system entry for this cell. */
    setSys(name: S, entry: Partial<SysEntry> & { value?: A, source?:A }): SysEntry {
        const prev = self._sys.get(name);
        const next: SysEntry = {
            name,
            value: entry.value ?? prev?.value,
            ts: Date.now(),
            revision: (prev?.revision ?? 0) + 1,
            source: entry.source ?? "meta",
        } as A;
        self._sys.set(name, next);
        return next;
    },

    getSys(name: S): SysEntry | null {
        return self._sys.get(name) ?? null;
    },

    deleteSys(name: S): B {
        return self._sys.delete(name);
    },

    listSys(): S[] {
        return [...self._sys.keys()];
    },

    // ── causal log ─────────────────────────────────────────────────────────────────────────────
    /** Append a causal entry. Keep it cheap — callers pass what they know. */
    recordCausal(key: S, cause: S, value?: A): void {
        const list = self._causal.get(key) ?? [];
        list.push({ cause, value, ts: Date.now() });

        // hard cap so long-lived cells don't balloon
        if (list.length > 256) list.splice(0, list.length - 256);

        self._causal.set(key, list);
    },

    getCausal(key: S): CausalEntry[] {
        return self._causal.get(key) ?? [];
    },

    clearCausal(key?: S): void {
        if (key) self._causal.delete(key);
        else self._causal.clear();
    },

    // ── last write provenance ──────────────────────────────────────────────────────────────────
    noteWrite(key: S, source: S): void {
        self._lastWriteSource.set(key, source);
    },

    whoWrote(key: S): S | null {
        return self._lastWriteSource.get(key) ?? null;
    },

    // ── msg handlers ───────────────────────────────────────────────────────────────────────────
    addMsgHandler(key: S, fn: (p: A) => void): () => void {
        const set = self._msgHandlers.get(key) ?? new Set();
        set.add(fn);
        self._msgHandlers.set(key, set);
        return () => set.delete(fn);
    },

    emitMsg(key: S, p: A): void {
        for (const fn of self._msgHandlers.get(key) ?? []) {
            try {
                fn(p);
            } catch (e) {
                console.error(`[meta-shard] msg handler failed for "${key}":`, e);
            }
        }
    },

    // ── global listeners ───────────────────────────────────────────────────────────────────────
    onGlobal(fn: (key: S | symbol, value: A) => void): () => void {
        self._globalListeners.add(fn);
        return () => self._globalListeners.delete(fn);
    },

    emitGlobal(key: S | symbol, value: A): void {
        for (const fn of self._globalListeners) {
            try {
                fn(key, value);
            } catch (e) {
                console.error("[meta-shard] global listener failed:", e);
            }
        }
    },

    // ── inspect / purge ────────────────────────────────────────────────────────────────────────
    inspect(): Record<S, A> {
        return {
            sys: Object.fromEntries(self._sys),
            causalKeys: [...self._causal.keys()],
            sealed: [...self._sealed],
            frozen: [...self._frozen],
            lastWriteSource: Object.fromEntries(self._lastWriteSource),
            msgHandlerKeys: [...self._msgHandlers.keys()],
            globalListenerCount: self._globalListeners.size,
        };
    },

    purge(): void {
        for (const map of [
            self._batchQueue,
            self._batchOpts,
            self._global_subscribers,
            self._sys,
            self._keyContracts,
            self._conflicts,
            self._conflictStrategies,
            self._causal,
            self._scopes,
            self._msgHandlers,
            self._lastWriteSource,
            self._mainMap,
            self._tupleCache,
        ] as Map<A, A>[]) {
            map.clear();
        }

        self._batches.clear();
        self._sealed.clear();
        self._frozen.clear();
        self._globalListeners.clear();
        self._weakMap = new WeakMap();
        self._boundaries = new WeakMap();
    },
}));
// #endregion
//                                                                                                                       OFFSEC
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const OFFSEC = BIND_ATOM("OFFSEC", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    secrets: new Set<A>(),
    audit: [] as OFFSEC_AUDIT_ENTRY[],
    maxAudit: (config.maxAudit ?? 500) as N,

    // ── secret marking ─────────────────────────────────────────────────────────────────────────
    /** Mark a value (token, key, credential) as secret. Reference equality by default. */
    markSecret(value: A): void {
        self.secrets.add(value);
    },

    isSecret(value: A): B {
        return self.secrets.has(value);
    },

    clearSecret(value: A): B {
        return self.secrets.delete(value);
    },

    /**
     * Deep-scan a message for marked secrets; replace with '[REDACTED]' before the message
     * leaves the boundary. Mutates nothing — returns a copy when a hit occurs.
     */
    scrub<T>(payload: T): { clean: T; redacted: N } {
        let redacted = 0;
        const seen = new Set<object>();

        const walk = (v: unknown): unknown => {
            if (self.secrets.has(v as A)) {
                redacted++;
                return "[REDACTED]";
            }

            if (v === null || typeof v !== "object") return v;
            if (seen.has(v as object)) return "[CYCLIC]";

            seen.add(v as object);

            if (Array.isArray(v)) return v.map(walk);

            const out: Record<S, unknown> = {};
            for (const [k, val] of Object.entries(v as Record<S, unknown>)) {
                out[k] = walk(val);
            }

            return out;
        };

        const clean = walk(payload) as T;

        if (redacted) {
            self.audit.push({
                at: Date.now(),
                what: `scrubbed ${redacted} secret(s)`,
            });

            if (self.audit.length > self.maxAudit) self.audit.shift();
        }

        return { clean, redacted };
    },

    listAudit(): OFFSEC_AUDIT_ENTRY[] {
        return [...self.audit];
    },

    purge(): void {
        self.secrets.clear();
        self.audit.length = 0;
    },
}));
// #endregion
//                                                                                                                  PERMISSIONS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const PERMISSIONS = BIND_ATOM("PERMISSIONS", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    permissions: new Map<string, ChannelPermissions>(),

    setPermissions(rt: string, perms: ChannelPermissions): void {
        self.permissions.set(rt, perms);
    },

    getPermissions(rt: string): ChannelPermissions | undefined {
        return self.permissions.get(rt);
    },

    removePermissions(rt: string): B {
        return self.permissions.delete(rt);
    },

    hasPermissions(rt: string): B {
        return self.permissions.has(rt);
    },

    getAllPermissions(): Record<string, ChannelPermissions> {
        return Object.fromEntries(self.permissions);
    },

    listProtectedChannels(): string[] {
        return [...self.permissions.keys()];
    },

    /** Wildcard-aware lookup: exact wins, else longest matching prefix before `*`. */
    resolvePermissions(rt: string): ChannelPermissions | undefined {
        if (self.permissions.has(rt)) return self.permissions.get(rt);

        let best: ChannelPermissions | undefined;
        let bestLen = -1;

        for (const [pattern, perms] of self.permissions) {
            const star = pattern.indexOf("*");
            if (star === -1) continue;

            if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) {
                best = perms;
                bestLen = star;
            }
        }

        return best;
    },

    /** Check one capability. No permission set → allowed (open channel). */
    check(
        rt: string,
        action: "read" | "write" | "send" | "subscribe" | "admin",
        role?: S,
    ): B {
        const perms = self.resolvePermissions(rt);
        if (!perms) return true;

        if (role && perms.roles && perms.roles.length && !perms.roles.includes(role)) {
            return false;
        }

        if (action === "read") return perms.read;
        if (action === "write") return perms.write;

        return perms[action] ?? perms.write;
    },

    purge(): void {
        self.permissions.clear();
    },
}));
// #endregion
//                                                                                                                  PERSISTENCE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const PERSISTENCE = BIND_ATOM("PERSISTENCE", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    durableOffsets: new Map<string, number>(),
    persisted: config.persisted as B | undefined,
    durableHistory: new Map<string, CSEMessage[]>(),

    getDurableHistorySizes(): Record<string, number> {
        const out: Record<string, number> = {};

        for (const [ch, hist] of self.durableHistory) {
            out[ch] = hist.length;
        }

        return out;
    },

    async replay(options: ReplayOptions, handler: CSEHandler): Promise<void> {
        const history = self.durableHistory.get(options.rt) ?? [];
        let slice = history;

        if (options.fromOffset !== undefined) {
            slice = slice.slice(options.fromOffset);
        }

        if (options.toOffset !== undefined) {
            slice = slice.slice(0, options.toOffset);
        }

        if (options.fromTimestamp !== undefined) {
            slice = slice.filter(m => m.ts !== undefined && m.ts >= options.fromTimestamp!);
        }

        if (options.toTimestamp !== undefined) {
            slice = slice.filter(m => m.ts !== undefined && m.ts <= options.toTimestamp!);
        }

        for (const m of slice) {
            handler(m.p);
        }
    },

    subscribeDurable(
        handler: CSEHandler,
        options: DurableOptions,
        k: string,
        sl = self.sl,
        ms = self.ms,
        rt = self.rt,
    ): () => void {
        const key = `${rt}:${options.subscriberId}`;
        const offset = self.durableOffsets.get(key) ?? options.fromOffset ?? 0;
        const history = self.durableHistory.get(rt) ?? [];

        history.slice(offset).forEach(m => handler(m.p));

        return self.sub(
            (p: unknown) => {
                const h = self.durableHistory.get(rt) ?? [];
                const nextOffset = h.length;

                self.durableOffsets.set(key, nextOffset);
                handler(p);
            },
            k,
            sl,
            ms,
            rt,
        );
    },

    getAllCheckpoints(): Record<string, number> {
        return Object.fromEntries(self.durableOffsets);
    },

    async setCheckpoint(
        subscriberId: string,
        offset: number,
        k: string,
        sl?: string,
        ms?: string,
        rt?: string,
    ): Promise<void> {
        const key = `${rt}:${subscriberId}`;

        self.durableOffsets.set(key, offset);

        if (self.DEVTOOLS_ACTIVE) {
            self.emitDevtoolsEvent(
                "checkpoint_set",
                rt,
                ms || undefined,
                undefined,
                { subscriberId, offset },
            );
        }
    },

    async getCheckpoint(
        subscriberId: string,
        k: string,
        sl?: string,
        ms?: string,
        rt?: string,
    ): Promise<number | null> {
        const key = `${rt}:${ms}:${subscriberId}`;

        return self.durableOffsets.has(key)
            ? self.durableOffsets.get(key)!
            : null;
    },
}));
// #endregion
//                                                                                                                     PRIORITY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const PRIORITY = BIND_ATOM("PRIORITY", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    queues: new Map<string, Array<{
        priority: SIG_BUS_Priority;
        weight: N;
        seq: N;
        item: A;
        enqueuedAt: N;
    }>>(),

    queueDefaults: new Map<string, SIG_BUS_Priority>(),
    defaultPriority: "normal" as SIG_BUS_Priority,
    seq: 0,

    setDefaultPriority(priority: SIG_BUS_Priority): void {
        self.defaultPriority = priority;
    },

    getDefaultPriority(): SIG_BUS_Priority {
        return self.defaultPriority;
    },

    setQueueDefault(queue: string, priority: SIG_BUS_Priority): void {
        self.queueDefaults.set(queue, priority);
    },

    getQueueDefault(queue: string): SIG_BUS_Priority {
        return self.queueDefaults.get(queue) ?? self.defaultPriority;
    },

    priorityWeight(p?: SIG_BUS_Priority): number {
        return p === "high" ? 3 : p === "low" ? 1 : 2;
    },

    enqueue(queue: string, item: A, priority?: SIG_BUS_Priority): N {
        const p = priority ?? self.getQueueDefault(queue);
        let q = self.queues.get(queue);

        if (!q) {
            q = [];
            self.queues.set(queue, q);
        }

        const entry = {
            priority: p,
            weight: self.priorityWeight(p),
            seq: self.seq++,
            item,
            enqueuedAt: Date.now(),
        };

        q.push(entry);

        if (self.DEVTOOLS_ACTIVE) {
            self.emitDevtoolsEvent("priority_enqueued", undefined, undefined, queue, {
                priority: p,
                depth: q.length,
            });
        }

        return entry.seq;
    },

    /** Highest weight wins; FIFO within the same weight (low seq = older = first). */
    dequeue(queue: string): A | undefined {
        const q = self.queues.get(queue);
        if (!q || !q.length) return undefined;

        let bestIdx = 0;

        for (let i = 1; i < q.length; i++) {
            const a = q[i], b = q[bestIdx];

            if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) {
                bestIdx = i;
            }
        }

        const [entry] = q.splice(bestIdx, 1);

        if (self.DEVTOOLS_ACTIVE) {
            self.emitDevtoolsEvent("priority_dequeued", undefined, undefined, queue, {
                priority: entry.priority,
                remaining: q.length,
            });
        }

        return entry.item;
    },

    peek(queue: string): A | undefined {
        const q = self.queues.get(queue);
        if (!q || !q.length) return undefined;

        let bestIdx = 0;

        for (let i = 1; i < q.length; i++) {
            const a = q[i], b = q[bestIdx];

            if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) {
                bestIdx = i;
            }
        }

        return q[bestIdx].item;
    },

    getDepth(queue: string): number {
        return (self.queues.get(queue) ?? []).length;
    },

    listQueues(): string[] {
        return [...self.queues.keys()];
    },

    getDepths(): Record<string, number> {
        const out: Record<string, number> = {};

        for (const [q, list] of self.queues) {
            out[q] = list.length;
        }

        return out;
    },

    clearQueue(queue: string): void {
        self.queues.delete(queue);
    },

    purge(): void {
        self.queues.clear();
        self.queueDefaults.clear();
    },
}));
// #endregion
//                                                                                                                     SNAPSHOT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface SNAPSHOT_ENTRY<W = A> {
    version: N;
    value: W;
    ts: N;
    label?: S;
}
export const SNAPSHOT = BIND_ATOM("SNAPSHOT", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    version: 0,
    history: [] as SNAPSHOT_ENTRY<W>[],
    historyMax: (config.historyMax ?? 128) as N,
    undoStack: [] as SNAPSHOT_ENTRY<W>[],
    redoStack: [] as SNAPSHOT_ENTRY<W>[],
    waiters: new Set<(value: W) => void>(),

    /** Capture the current state. Call this from ATOM.write() after a successful write. */
    capture(value: W, label?: S): SNAPSHOT_ENTRY<W> {
        self.version++;

        const entry: SNAPSHOT_ENTRY<W> = {
            version: self.version,
            value,
            ts: Date.now(),
            label,
        };

        self.history.push(entry);

        if (self.history.length > self.historyMax) {
            self.history.splice(0, self.history.length - self.historyMax);
        }

        self.redoStack.length = 0;

        for (const resolve of self.waiters) {
            resolve(value);
        }

        self.waiters.clear();

        return entry;
    },

    /** Latest snapshot, or undefined if none. */
    latest(): SNAPSHOT_ENTRY<W> | undefined {
        return self.history[self.history.length - 1];
    },

    /** Fetch a snapshot by exact version number. */
    byVersion(v: N): SNAPSHOT_ENTRY<W> | undefined {
        return self.history.find(e => e.version === v);
    },

    /** Fetch the last N snapshots, oldest first. */
    recent(n = 3): SNAPSHOT_ENTRY<W>[] {
        return self.history.slice(-n);
    },

    /** All snapshots since a timestamp. */
    since(ts: N): SNAPSHOT_ENTRY<W>[] {
        return self.history.filter(e => e.ts >= ts);
    },

    count(): N {
        return self.history.length;
    },

    /** Wait for the next capture (first one if none exist yet). */
    waitForNext(timeoutMs = 0): Promise<W> {
        const latest = self.latest();

        if (latest) return Promise.resolve(latest.value);

        return new Promise<W>((resolve, reject) => {
            const waiter = (v: W) => resolve(v);

            self.waiters.add(waiter);

            if (timeoutMs > 0) {
                setTimeout(() => {
                    if (self.waiters.delete(waiter)) {
                        reject(new Error("ATOM snapshot wait timed out"));
                    }
                }, timeoutMs);
            }
        });
    },

    /** Move an entry from history into the undo stack (used by TIME_TRAVEL). */
    pushUndo(entry: SNAPSHOT_ENTRY<W>): void {
        self.undoStack.push(entry);

        if (self.undoStack.length > self.historyMax) {
            self.undoStack.splice(0, self.undoStack.length - self.historyMax);
        }
    },

    /** Move an entry from history into the redo stack (used by TIME_TRAVEL). */
    pushRedo(entry: SNAPSHOT_ENTRY<W>): void {
        self.redoStack.push(entry);

        if (self.redoStack.length > self.historyMax) {
            self.redoStack.splice(0, self.redoStack.length - self.historyMax);
        }
    },

    purge(): void {
        self.history.length = 0;
        self.undoStack.length = 0;
        self.redoStack.length = 0;
        self.waiters.clear();
        self.version = 0;
    },
}));
// #endregion
//                                                                                                                         TAGS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const TAGS = BIND_ATOM("TAGS", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    tagSubs: new Map<string, Set<CSEHandler>>(),
    tagsByChannel: new Map<string, Set<string>>(),

    // ── channel → tags ─────────────────────────────────────────────────────────────────────────
    /** Declare the tags a channel carries. Replaces any previous tag set for that channel. */
    tagChannel(channel: S, tags: S[]): void {
        self.tagsByChannel.set(channel, new Set(tags));
    },

    untagChannel(channel: S): B {
        return self.tagsByChannel.delete(channel);
    },

    getTagsForChannel(channel: S): S[] {
        return [...(self.tagsByChannel.get(channel) ?? [])];
    },

    /** All channels carrying at least one of the given tags. */
    channelsByTags(tags: S[]): S[] {
        const want = new Set(tags);
        const out: S[] = [];

        for (const [ch, chTags] of self.tagsByChannel) {
            for (const t of chTags) {
                if (want.has(t)) {
                    out.push(ch);
                    break;
                }
            }
        }

        return out;
    },

    // ── subscriber side ─────────────────────────────────────────────────────────────────────────
    subscribeByTag(tag: string, handler: CSEHandler): () => void {
        const set = self.tagSubs.get(tag) ?? new Set<CSEHandler>();
        set.add(handler);
        self.tagSubs.set(tag, set);

        return () => set.delete(handler);
    },

    unsubscribeByTag(tag: string, handler: CSEHandler): B {
        return self.tagSubs.get(tag)?.delete(handler) ?? false;
    },

    listTags(): string[] {
        return [...self.tagSubs.keys()];
    },

    // ── publish side ────────────────────────────────────────────────────────────────────────────
    /** Fan a tagged message out to every handler subscribed to a tag the channel carries. */
    publishByTag(msg: CSEMessage): N {
        const tags = self.tagsByChannel.get(msg.r);
        if (!tags || tags.size === 0) return 0;

        let delivered = 0;

        for (const tag of tags) {
            const subs = self.tagSubs.get(tag);
            if (!subs) continue;

            for (const handler of [...subs]) {
                try {
                    handler(msg);
                    delivered++;
                } catch (e) {
                    console.error(`[tags-NEUTRON] tag handler failed for "${tag}":`, e);
                }
            }
        }

        return delivered;
    },

    getTagsByChannel(): Record<string, string[]> {
        const out: Record<string, string[]> = {};

        for (const [ch, tags] of self.tagsByChannel) {
            out[ch] = [...tags];
        }

        return out;
    },

    purge(): void {
        self.tagSubs.clear();
        self.tagsByChannel.clear();
    },
}));
// #endregion
//                                                                                                                  TIME_TRAVEL
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const TIME_TRAVEL = BIND_ATOM("TIME_TRAVEL", ({ self, master, config, SNAPSHOT }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    snapshots: null as typeof SNAPSHOT | null,
    version: 0,
    historyIndex: -1,
    undoStack: [] as SNAPSHOT_ENTRY<W>[],
    initial: undefined as W | undefined,
    _time_travel_reverting: false,
    autoTimers: new Set<ReturnType<typeof setInterval>>(),

    /** Bind to the snapshot shard of the same cell. Call once after both shards are bound. */
    attach(snapshots: typeof SNAPSHOT): void {
        self.snapshots = snapshots;
        self.version = snapshots.version;
        self.historyIndex = snapshots.history.length - 1;
        self.initial = snapshots.history[0]?.value;
    },

    /** Write a value back to the cell and record it as a proper version (fires subs like a normal write). */
    replay: (entry: SNAPSHOT_ENTRY<W>): void => {
        self._time_travel_reverting = true;

        try {
            self._writeThrough?.(
                entry.value,
                entry.label ? `time-travel:${entry.label}` : "time-travel",
            );
        } finally {
            self._time_travel_reverting = false;
        }
    },

    /** Injected by ATOM when the shard is bound — how restores become real writes. */
    _writeThrough: undefined as ((value: W, cause?: S) => void) | undefined,

    _current(): SNAPSHOT_ENTRY<W> | undefined {
        return self.snapshots?.history[self.historyIndex];
    },

    back(): B {
        if (!self.snapshots || self.historyIndex <= 0) return false;

        self.historyIndex--;

        const entry = self._current();
        if (!entry) return false;

        // The entry we stepped away from goes on redo.
        const departed = self.snapshots.history[self.historyIndex + 1];
        if (departed) self.snapshots.pushRedo(departed);

        self.replay(entry);
        return true;
    },

    forward(): B {
        if (!self.snapshots) return false;

        const nextEntry = self.snapshots.history[self.historyIndex + 1];
        if (!nextEntry) return false;

        self.historyIndex++;
        self.replay(nextEntry);
        return true;
    },

    goto(version: N): B {
        if (!self.snapshots) return false;

        const target = self.snapshots.byVersion(version);
        if (!target) return false;

        const idx = self.snapshots.history.indexOf(target);
        if (idx < 0) return false;

        const departed = self._current();
        if (departed && idx < self.historyIndex) {
            self.snapshots.pushRedo(departed);
        }

        self.historyIndex = idx;
        self.replay(target);
        return true;
    },

    auto_back(time = 2500): void {
        const t = setInterval(() => {
            if (!self.back()) clearInterval(t);
        }, time);

        self.autoTimers.add(t);
    },

    auto_forward(time = 2500): void {
        const t = setInterval(() => {
            if (!self.forward()) clearInterval(t);
        }, time);

        self.autoTimers.add(t);
    },

    /** Graft a foreign snapshot sequence onto this timeline (branch merge). */
    graft(entries: SNAPSHOT_ENTRY<W>[]): N {
        if (!self.snapshots) return 0;

        let grafted = 0;

        for (const e of entries) {
            self.snapshots.history.push({
                version: ++self.snapshots.version,
                value: e.value,
                ts: e.ts ?? Date.now(),
                label: e.label ? `grafted:${e.label}` : "grafted",
            });

            grafted++;
        }

        self.historyIndex = self.snapshots.history.length - 1;
        return grafted;
    },

    /** Stop all auto walkers. */
    stopAuto(): void {
        for (const t of self.autoTimers) clearInterval(t);
        self.autoTimers.clear();
    },

    /** True while replay() is writing — ATOM.write() checks this to avoid re-capturing. */
    get isReverting(): B {
        return self._time_travel_reverting;
    },

    purge(): void {
        self.stopAuto();
        self.snapshots = null;
        self.undoStack.length = 0;
        self.historyIndex = -1;
    },
}));
// #endregion
//                                                                                                                          TTL
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface TTL_ENTRY { expiresAt: N; data?: A; }

export const TTL = BIND_ATOM("TTL", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    entries: new Map<S, TTL_ENTRY>(),
    onExpire: [] as ((key: S, data?: A) => void)[],
    defaultTtlMs: (config.defaultTtlMs ?? 0) as N,

    // ── write ──────────────────────────────────────────────────────────────────────────────────
    set(key: S, data?: A, ttlMs?: N): TTL_ENTRY {
        const ttl = ttlMs ?? self.defaultTtlMs;

        if (!ttl || ttl <= 0) {
            throw new Error("[ttl-shard] no ttl provided and no defaultTtlMs configured");
        }

        const entry: TTL_ENTRY = {
            expiresAt: Date.now() + ttl,
            data,
        };

        self.entries.set(key, entry);
        return entry;
    },

    touch(key: S, ttlMs?: N): B {
        const e = self.entries.get(key);
        if (!e) return false;

        e.expiresAt = Date.now() + (ttlMs ?? self.defaultTtlMs);
        return true;
    },

    // ── read ───────────────────────────────────────────────────────────────────────────────────
    /** Returns undefined if missing OR expired (and removes it lazily). */
    get(key: S): A | undefined {
        const e = self.entries.get(key);
        if (!e) return undefined;

        if (Date.now() >= e.expiresAt) {
            self.entries.delete(key);
            return undefined;
        }

        return e.data;
    },

    has(key: S): B {
        const e = self.entries.get(key);
        return !!e && Date.now() < e.expiresAt;
    },

    peek(key: S): A | undefined {
        return self.entries.get(key)?.data;
    },

    timeToLiveMs(key: S): N | null {
        const e = self.entries.get(key);
        if (!e) return null;

        return Math.max(0, e.expiresAt - Date.now());
    },

    // ── delete ─────────────────────────────────────────────────────────────────────────────────
    delete(key: S): B {
        return self.entries.delete(key);
    },

    // ── sweep ──────────────────────────────────────────────────────────────────────────────────
    /** Removes expired entries, fires onExpire hooks. Returns count expired. */
    sweep(): number {
        const now = Date.now();
        let n = 0;

        for (const [key, e] of [...self.entries]) {
            if (now >= e.expiresAt) {
                self.entries.delete(key);
                n++;

                for (const fn of self.onExpire) {
                    try {
                        fn(key, e.data);
                    } catch (err) {
                        console.error(`[ttl-shard] onExpire failed for "${key}":`, err);
                    }
                }
            }
        }

        return n;
    },

    /** Sweep on a cadence. Owns its own timer — call stopSweeper() to cancel. */
    _sweeper: null as ReturnType<typeof setInterval> | null,

    startSweeper(intervalMs = 30_000): void {
        if (self._sweeper) return;
        self._sweeper = setInterval(() => self.sweep(), intervalMs);
    },

    stopSweeper(): void {
        if (self._sweeper) {
            clearInterval(self._sweeper);
            self._sweeper = null;
        }
    },

    get size(): number {
        return self.entries.size;
    },

    listKeys(): S[] {
        return [...self.entries.keys()];
    },

    purge(): void {
        self.stopSweeper();
        self.entries.clear();
        self.onExpire.length = 0;
    },
}));
// #endregion
//                                                                                                                     REACTIVE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface RiftRecord<T = A> {
    opened: B;
    claimed: B;
    value?: T;
    createdAt: N;
    openedAt?: N;
}
type StoreListener<T = A> = (v: RiftRecord<T>) => void;
export const REACTIVE = BIND_ATOM("REACTIVE", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    records: new Map<S, RiftRecord<A>>(),
    listeners: new Map<S, Set<StoreListener<A>>>(),

    get<T = A>(key: S): RiftRecord<T> | undefined {
        return self.records.get(key) as RiftRecord<T> | undefined;
    },

    set<T = A>(key: S, record: RiftRecord<T>): void {
        self.records.set(key, record as RiftRecord<A>);

        const subs = self.listeners.get(key);

        if (subs) {
            for (const l of [...subs]) {
                try {
                    l(record as RiftRecord<A>);
                } catch {
                    // one bad listener never kills the rest
                }
            }
        }
    },

    sub<T = A>(key: S, fn: StoreListener<T>): () => void {
        let subs = self.listeners.get(key);

        if (!subs) {
            subs = new Set();
            self.listeners.set(key, subs);
        }

        subs.add(fn as StoreListener<A>);

        return () => {
            const s = self.listeners.get(key);
            if (!s) return;

            s.delete(fn as StoreListener<A>);

            if (!s.size) self.listeners.delete(key);
        };
    },

    delete(key: S): void {
        self.records.delete(key);
        self.listeners.delete(key);
    },

    clear(): void {
        self.records.clear();
        self.listeners.clear();
    },

    get size(): N {
        return self.records.size;
    },
}));
// #endregion
//                                                                                                                       SCHEMA
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const SCHEMA = BIND_ATOM("SCHEMA", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    _schemas: new Map<string, CSESchema>(),

    setChannelSchema(schema: CSESchema, rt: string): void {
        self._schemas.set(rt, schema);
    },

    getChannelSchema(rt: string): CSESchema | undefined {
        return self._schemas.get(rt);
    },

    removeChannelSchema(rt: string): B {
        const had = self._schemas.delete(rt);
        return had;
    },

    hasChannelSchema(rt: string): B {
        return self._schemas.has(rt);
    },

    listChannelSchemas(): string[] {
        return [...self._schemas.keys()];
    },

    getAllSchemas(): Record<string, CSESchema> {
        return Object.fromEntries(self._schemas);
    },

    /** Wildcard-aware lookup: exact channel wins, else longest matching prefix before `*`. */
    resolveSchema(rt: string): CSESchema | undefined {
        if (self._schemas.has(rt)) return self._schemas.get(rt);

        let best: CSESchema | undefined;
        let bestLen = -1;

        for (const [pattern, schema] of self._schemas) {
            const star = pattern.indexOf("*");
            if (star === -1) continue;

            if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) {
                best = schema;
                bestLen = star;
            }
        }

        return best;
    },

    /**
     * Validate a message against its channel schema. No schema → pass (open channel).
     * Returns { ok, errors } — never throws.
     */
    checkSchema(rt: S, msg: CSEMessage): { ok: B; errors: S[] } {
        const schema = self.resolveSchema(rt) as A;
        if (!schema) return { ok: true, errors: [] };

        const errors: S[] = [];
        const payload = (msg.p ?? {}) as Record<S, A>;

        for (const [field, rule] of Object.entries(schema.fields ?? {}) as A) {
            const v = payload[field];

            if (v === undefined || v === null) {
                if (rule.required) errors.push(`missing required field "${field}"`);
                continue;
            }

            if (rule.type !== "any") {
                const actual = typeof v;

                if (rule.type === "number" && actual !== "number") {
                    errors.push(`field "${field}" expected number, got ${actual}`);
                } else if (rule.type === "string" && actual !== "string") {
                    errors.push(`field "${field}" expected string, got ${actual}`);
                } else if (rule.type === "boolean" && actual !== "boolean") {
                    errors.push(`field "${field}" expected boolean, got ${actual}`);
                }
            }
        }

        if (schema.validate) {
            try {
                const r = schema.validate(msg);

                if (r !== true && r !== undefined) {
                    errors.push(
                        typeof r === "string"
                            ? r
                            : `validator rejected message on "${rt}"`,
                    );
                }
            } catch (e) {
                errors.push(`validator threw: ${e}`);
            }
        }

        return { ok: errors.length === 0, errors };
    },

    purge(): void {
        self._schemas.clear();
    },
}));
// #endregion
//                                                                                                              VALUES_REGISTRY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const VALUES_REGISTRY = BIND_ATOM("VALUES_REGISTRY", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    instanceId: `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    hasWritten: false,
    lastWriteSource: undefined as string | undefined,
    lastWriteAt: undefined as number | undefined,
    writeCount: 0,

    recordWrite(source = "write"): void {
        self.hasWritten = true;
        self.lastWriteSource = source;
        self.lastWriteAt = Date.now();
        self.writeCount++;
    },

    reset(): void {
        self.hasWritten = false;
        self.lastWriteSource = undefined;
        self.lastWriteAt = undefined;
        self.writeCount = 0;
    },
}));
// #endregion
//                                                                                                                  CONCURRENCY
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface BackpressureConfig {
    /** Max items queued before SHOULD_BACKPRESSURE() fires. 0/undefined = never. */
    maxQueueSize?: N;
    /** Cooldown between drain attempts once throttling starts. */
    throttleMs?: N;
    /** Coalescing window (ms) before a staged group flushes even if size not reached. */
    batchWaitMs?: N;
    /** Hard deadline (ms) from FIRST item staged — flushes the group no matter what. */
    batchMaxWaitMs?: N;
    /** Items per coalesced group. */
    batchSize?: N;
    /** Default per-tier concurrency if a tier registers without options. */
    defaultConcurrency?: N;
    /** Default retry / timeout for tier tasks. */
    retry?: N;
    timeout?: N;
}
type COALESCED_GROUP = {
    items: Array<{ msg: CSEMessage; resolve: (v: A) => void; reject: (e: A) => void }>;
    flushed: B;
};
// ── CONCURRENCY_GOVERNOR (internal — AIMD engine lifted from BATCH, one per shard) ───────────────
// Additive increase on healthy telemetry, multiplicative half-down on friction. Health from
// event-loop lag + RSS + external breaker state. Semaphore with dynamic resize (SEMAPHORE
// semantics), exposed so tier queues can consult `tryAcquire/release`.
export const CONCURRENCY = BIND_ATOM("CONCURRENCY", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    current: Math.min(
        config.maxConcurrency ?? 64,
        Math.max(
            config.minConcurrency ?? 1,
            config.initialConcurrency ?? Math.max(4, Math.floor((config.maxConcurrency ?? 64) / 4)),
        ),
    ) as N,

    min: (config.minConcurrency ?? 1) as N,
    max: (config.maxConcurrency ?? 64) as N,
    holders: new Set<symbol>(),
    waiters: [] as Array<() => void>,
    peak: 0 as N,

    get active(): N {
        return self.holders.size;
    },

    get concurrency(): N {
        return self.current;
    },

    get peakConcurrency(): N {
        return self.peak;
    },

    tryAcquire(): symbol | undefined {
        if (self.holders.size >= self.current) return undefined;

        const h = Symbol("governor-slot");
        self.holders.add(h);

        if (self.holders.size > self.peak) self.peak = self.holders.size;

        return h;
    },

    async acquire(): Promise<symbol> {
        const h = self.tryAcquire();
        if (h) return h;

        return new Promise<symbol>(resolve =>
            self.waiters.push(() => resolve(self.tryAcquire()!)),
        );
    },

    release(h: symbol): void {
        self.holders.delete(h);

        const next = self.waiters.shift();
        if (next) next();
    },

    /** AIMD. Call from telemetry. */
    evaluateResistance(signal: { latency: N; error: B; targetLatencyMs?: N }): void {
        const target = signal.targetLatencyMs ?? 250;

        if (signal.error || signal.latency > target * 1.5) {
            self.multiplicativeDecrease();
        } else if (signal.latency < target * 0.7) {
            self.additiveIncrease();
        }
    },

    additiveIncrease(): void {
        if (self.current < self.max) self.resize(self.current + 1);
    },

    multiplicativeDecrease(): void {
        if (self.current > self.min) {
            self.resize(Math.max(self.min, Math.floor(self.current * 0.5)));
        }
    },

    resize(n: N): void {
        self.current = Math.max(self.min, Math.min(self.max, Math.floor(n)));

        if (self.current > self.peak) self.peak = self.current;

        // Wake waiters if capacity grew.
        while (self.holders.size < self.current && self.waiters.length) {
            self.waiters.shift()?.();
        }
    },
}));
// #endregion
//                                                                                                                    CONTRACTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface CONTRACT_PROTON {
	name: S;
	precondition?: (value: A) => boolean;
	postcondition?: (value: A) => boolean;
	invariant?: (snapshot: Record<S, A>) => boolean;
	block?: B;
}
export interface CONTRACT_VIOLATION {
	contract: S;
	phase: 'pre' | 'post' | 'invariant';
	value?: A;
	message: S;
	ts: N;
}
export const CONTRACTS = BIND_ATOM("CONTRACTS", ({ self, master, config }: any) => ({
	useCell: (() => {
		const fn = () => {};
		return fn;
	})(),
	r: master.r,
	m: master.m,
	s: master.s,
	k: config.k,
	contracts: new Map<S, A>(),
	_keyContracts: new Map() as Map<S, Set<S>>,
	causal: [] as CONTRACT_VIOLATION[],
	violations: [] as CONTRACT_VIOLATION[],
	violationMax: 64,
	_throwOnBlock: true as B,
	add: (contract: A): (() => void) => {
		self.contracts.set(contract.name, contract);
		return () => self.contracts.delete(contract.name);
	},
	remove: (name: S): B => {
		return self.contracts.delete(name);
	},
	get: (name: S): A | undefined => {
		return self.contracts.get(name);
	},
	list: (): A[] => {
		return [...self.contracts.values()];
	},
	bindKeyContracts: (keyContracts: Map<S, Set<S>>): void => {
		self._keyContracts = keyContracts;
	},
	_record: (v: CONTRACT_VIOLATION, blocking: B): B => {
		self.violations.push(v);
		if (self.violations.length > self.violationMax) self.violations.splice(0, self.violations.length - self.violationMax);
		if (blocking) throw new Error(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
		console.warn(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
		return false;
	},
	check: (value: A, phase: 'pre' | 'post'| 'invariant', snapshot?: Record<S, A>): B =>{
		for (const c of self.contracts.values()) {
			if (phase === 'pre' && c.precondition) {
				try { if (!c.precondition(value)) return self._record({ contract: c.name, phase, value, message: 'precondition returned false', ts: Date.now() }, !!c.block && self._throwOnBlock); }
				catch (e) { return self._record({ contract: c.name, phase, value, message: `precondition threw: ${e}`, ts: Date.now() }, true); }
			}
			if (phase === 'post' && c.postcondition) {
				try { if (!c.postcondition(value)) return self._record({ contract: c.name, phase, value, message: 'postcondition returned false', ts: Date.now() }, !!c.block && self._throwOnBlock); }
				catch (e) { return self._record({ contract: c.name, phase, value, message: `postcondition threw: ${e}`, ts: Date.now() }, true); }
			}
			if (phase === 'invariant' && c.invariant && snapshot) {
				try { if (!c.invariant(snapshot)) return self._record({ contract: c.name, phase, value, message: 'invariant returned false', ts: Date.now() }, !!c.block && self._throwOnBlock); }
				catch (e) { return self._record({ contract: c.name, phase, value, message: `invariant threw: ${e}`, ts: Date.now() }, true); }
			}
		}
		return true;
	},
	checkKey:(key: S, value: A, phase: 'pre' | 'post'): B =>{
		for (const name of self._keyContracts.get(key) ?? []) {
			const c = self.contracts.get(name);
			if (!c) continue;
			const check = phase === 'pre' ? c.precondition : c.postcondition;
			if (!check) continue;
			try {
				if (!check(value)) return self._record({ contract: name, phase, value, message: `key contract failed for "${key}"`, ts: Date.now() }, !!c.block && self._throwOnBlock);
			} catch (e) {
				return self._record({ contract: name, phase, value, message: `key contract threw: ${e}`, ts: Date.now() }, true);
			}
		}
		return true;
	},
	canWrite: (value: A): B=> { return self.check(value, 'pre'); },
	didWrite:(value: A): B=> { return self.check(value, 'post'); },
	listViolations:(): CONTRACT_VIOLATION[]=> { return [...self.violations]; },
	clearViolations:(): void=> { self.violations.length = 0; },
	purge:(): void=> { self.contracts.clear(); self.violations.length = 0; self._keyContracts.clear(); }
}))
// #endregion
//                                                                                                            COMPRESSION_SHARD
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const COMPRESSION = BIND_ATOM("COMPRESSION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
    compressionConfigs: [] as CompressionConfig[],

    getCompressionConfigs(): CompressionConfig[] {
        return [...self.compressionConfigs];
    },

    configureCompression(config: CompressionConfig): void {
        self.compressionConfigs.push(config);
    },
}));
// #endregion
//                                                                                                                   DEADLETTER
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface DEADLETTER_WIRE {
    enqueue(msg: CSEMessage): Promise<void> | void;
}
export const DEADLETTER = BIND_ATOM("DEADLETTER", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    root_cell_key: config.root_cell_key as A,
    deadLetters: [] as CSEMessage[],
    maxDeadLetters: (config.maxDeadLetters ?? 1000) as N,
    wire: config.wire as DEADLETTER_WIRE | undefined,

    /** Park a failed message. Oldest evicted when over max. */
    record(msg: CSEMessage, reason?: S): void {
        self.deadLetters.push({ ...msg, _ack: true } as CSEMessage);

        if (self.deadLetters.length > self.maxDeadLetters) {
            self.deadLetters.shift();
        }
    },

    async listDeadLetters(
        k?: S,
        sl?: S,
        ms?: S,
        rt?: S,
    ): Promise<CSEMessage[]> {
        if (!rt && !ms && !sl && !k) return [...self.deadLetters];

        return self.deadLetters.filter(
            m =>
                (!rt || m.rt === rt) &&
                (!ms || m.ms === ms) &&
                (!sl || m.sl === sl) &&
                (!k || m.k === k),
        );
    },

    async requeueDeadLetter(id: S, ttl?: N): Promise<void> {
        const idx = self.deadLetters.findIndex(m => m.id === id);
        if (idx === -1) return;

        const msg = self.deadLetters.splice(idx, 1)[0];
        msg.ttl = ttl ?? msg.ttl;

        await self.wire?.enqueue(msg);
    },

    purgeDeadLetter(id: S): B {
        const idx = self.deadLetters.findIndex(m => m.id === id);
        if (idx === -1) return false;

        self.deadLetters.splice(idx, 1);
        return true;
    },

    count(): N {
        return self.deadLetters.length;
    },

    purge(): void {
        self.deadLetters.length = 0;
    },
}));
// #endregion
//                                                                                                                  DESTRUCTION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface DESTRUCTION_RECORD {
    key: A;
    cause: S;
    at: N;
    reviver?: () => A;
}
export const DESTRUCTION = BIND_ATOM("DESTRUCTION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    beforeDestruct: [] as ((key: A) => void)[],
    afterDestruct: [] as ((record: DESTRUCTION_RECORD) => void)[],
    records: [] as DESTRUCTION_RECORD[],
    destroyed: false as B,
    blocked: false as B,

    // ── hooks ──────────────────────────────────────────────────────────────────────────────────
    onBefore(fn: (key: A) => void): () => void {
        self.beforeDestruct.push(fn);

        return () => {
            self.beforeDestruct = self.beforeDestruct.filter(f => f !== fn);
        };
    },

    onAfter(fn: (record: DESTRUCTION_RECORD) => void): () => void {
        self.afterDestruct.push(fn);

        return () => {
            self.afterDestruct = self.afterDestruct.filter(f => f !== fn);
        };
    },

    /** Veto a pending destruction (e.g. in a beforeDestruct hook, or upfront). */
    block(): void {
        self.blocked = true;
    },

    unblock(): void {
        self.blocked = false;
    },

    // ── the kill switch ────────────────────────────────────────────────────────────────────────
    /**
     * Runs the full destruction sequence.
     * Returns the record on success, null when blocked or already destroyed.
     * `reviver` is a factory the record keeps so the cell can be rebuilt later.
     */
    destroy(cause = "unknown", reviver?: () => A): DESTRUCTION_RECORD | null {
        if (self.destroyed) return null;
        if (self.blocked) return null;

        const record: DESTRUCTION_RECORD = {
            key: self.root_cell,
            cause,
            at: Date.now(),
            reviver,
        };

        for (const fn of self.beforeDestruct) {
            try {
                fn(self.root_cell);
            } catch (e) {
                console.error("[destruction-shard] beforeDestruct hook failed:", e);
            }
        }

        self.destroyed = true;
        self.records.push(record);

        for (const fn of self.afterDestruct) {
            try {
                fn(record);
            } catch (e) {
                console.error("[destruction-shard] afterDestruct hook failed:", e);
            }
        }

        return record;
    },

    resurrect(): A | null {
        if (!self.destroyed) return null;

        const last = self.records[self.records.length - 1];
        self.destroyed = false;
        self.blocked = false;

        return last?.reviver?.() ?? null;
    },

    isDestroyed(): B {
        return self.destroyed;
    },

    clearRecords(): void {
        self.records.length = 0;
    },

    purge(): void {
        self.beforeDestruct.length = 0;
        self.afterDestruct.length = 0;
        self.records.length = 0;
    },
}));
// #endregion
//                                                                                                                        ERROR
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type MODULE_PATH = S;
interface ERROR_EMISSION {
    module: MODULE_PATH;
    code: S;
    message: S;
    detail?: A;
    ts: N;
}
class ORC_SHOUT {
	c = {
		emerald: { 10: "#002c22", 9: "#004f3b", 8: "#016630", 7: "#007a55", 6: "#009966", 5: "#00bc7d" },
		blue: { 10: "", 9: "", 8: "", 7: "", 6: "", 5: "" },
	};
	t = {
		deep: {
			bg: "#020817",
			txt: "#f8fafc",
			size: 12,
			title_size: 14,
			title_txt: "#94a3b8",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #101727",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: "",
		},
		vintage: {
			bg: "#001B29",
			txt: "#e9e1ba",
			size: 12,
			title_size: 14,
			title_txt: "#ada785",
			payload_key: "#29c3a0",
			payload_value: "#fafafa",
			font: "monospace",
			border: "2px solid #112932",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: "",
		},
		error: {
			bg: "#09090B",
			txt: "#e7000b",
			size: 12,
			title_size: 14,
			title_txt: "#9f0712",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #e7000b",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e7000b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
		},
		warn: {
			bg: "#09090B",
			txt: "#e17100",
			size: 12,
			title_size: 14,
			title_txt: "#973c00",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #e17100",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e17100" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`,
		},
		success: {
			bg: "#09090B",
			txt: "#009966",
			size: 12,
			title_size: 14,
			title_txt: "#016630",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #009966",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#009966" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.801 10A10 10 0 1 1 17 3.335"/><path d="m9 11 3 3L22 4"/></svg>`,
		},
		info: {
			bg: "#09090B",
			txt: "#155dfc",
			size: 12,
			title_size: 14,
			title_txt: "#193cb8",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #155dfc",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#155dfc" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
		},
	};
	log(msg: string | string[], p?: any, theme = this.t.deep) {
		const isArray = Array.isArray(msg);
		const title = isArray ? msg[0] : msg;
		const paragraphs = isArray ? msg.slice(1) : [];

		const containerStyle = `
              background: ${theme.bg};
              ${theme.border ? `border: ${theme.border};` : ""}
              ${theme.borderRadius ? `border-radius: ${theme.borderRadius};` : ""}
              ${theme.padding ? `padding: ${theme.padding};` : ""}
              font-family: ${theme.font};
              display: block;
              margin: 4px 0;
          `;

		const titleWithIcon = theme?.svg ? `${theme?.svg} ${title}` : title;
		const titleStyle = `
              color: ${theme.title_txt};
              font-size: ${theme.title_size}px;
              font-weight: bold;
              font-family: ${theme.font};
              display: flex;
              align-items: center;
              gap: 8px;
          `;

		const paragraphStyle = `
              color: ${theme.txt};
              font-size: ${theme.size}px;
              font-family: ${theme.font};
              margin-top: 4px;
          `;

		const keyStyle = `
              color: ${theme.payload_key};
              font-weight: bold;
              font-family: ${theme.font};
          `;
		const valueStyle = `
              color: ${theme.payload_value};
              font-family: ${theme.font};
          `;

		let payloadStr = "";
		if (p && typeof p === "object") {
			const entries = Object.entries(p);
			payloadStr = entries.map(([key, value]) => `%c${key}: %c${JSON.stringify(value, null, 2)}`).join("\n");
		}

		const logParts = [`%c${titleWithIcon}`, ...paragraphs.map((p) => `%c${p}`), payloadStr ? `\n${payloadStr}` : ""].filter(Boolean).join("\n");

		const styles = [containerStyle + titleStyle, ...paragraphs.map(() => paragraphStyle), ...(payloadStr ? [keyStyle, valueStyle] : [])];

		console.log(logParts, ...styles);
	}
}
const os = new ORC_SHOUT()
export const ERROR = BIND_ATOM("ERROR", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    /** History lives here regardless of emission — history is cheap, display is gated. */
    error_history: [] as ERROR_EMISSION[],
    HISTORY_CAP: 512,

    // ── runtime-editable controls ─────────────────────────────────────────────────────────────
    /** Master kill-switch. false → nothing emits, history still accrues. */
    ENABLED: true as B,

    /** Emission only in dev mode unless this is explicitly flipped on. */
    ENABLE_IN_PRODUCTION: false as B,

    /**
     * Surgical control. null = every module emits.
     * Record<string, boolean> = only listed modules emit, with true/false overriding.
     */
    SURGICAL_LOGS: (config.surgical ?? null) as Record<MODULE_PATH, B> | null,

    /** Theme passed through to the LOGGER. Default = the error theme. */
    theme: (os.t),

    /** "TERMINAL" | "FILE" — pipes through the LOGGER's configured sink. */
    PIPE_TO_FILE_OR_TERMINAL: "TERMINAL" as "TERMINAL" | "FILE",

    /** Set at wiring time by the platform; falls back to plain console when absent. */
    logger: os.log,

    // ── the single emission point ─────────────────────────────────────────────────────────────
    /** Every error-producing call site in every shard funnels through here. */
    emit(module: MODULE_PATH, code: S, message: S, detail?: A): ERROR_EMISSION {
        const entry: ERROR_EMISSION = { module, code, message, detail, ts: Date.now() };

        self.error_history.push(entry);

        if (self.error_history.length > self.HISTORY_CAP) {
            self.error_history.splice(0, self.error_history.length - self.HISTORY_CAP);
        }

        if (self.shouldEmit(module)) {
            const title = `[${String(self.root_cell_key)}] ${module} · ${code}`;

            if (self.logger) {
                self.logger.theme = self.theme;
                self.logger.drop_log([title, message], detail);
            } else if (self.shouldEmit(module)) {
                console.error(`${title}\n  ${message}`, detail ?? "");
            }
        }

        return entry;
    },

    /** Surgical gate: master switch → dev/production → per-module toggle. */
    shouldEmit(module: MODULE_PATH): B {
        if (!self.ENABLED) return false;
        if (self.ENABLE_IN_PRODUCTION === false && root.state.get("DEV_MODE") !== true) return false;
        if (self.SURGICAL_LOGS === null) return true;

        // exact match wins, then longest-prefix match (section-level control)
        if (module in self.SURGICAL_LOGS) return self.SURGICAL_LOGS[module];

        let best: S | null = null;

        for (const prefix of Object.keys(self.SURGICAL_LOGS)) {
            if (module.startsWith(prefix + ".") && (best === null || prefix.length > best.length)) {
                best = prefix;
            }
        }

        return best !== null ? self.SURGICAL_LOGS[best] : false;
    },

    // ── convenience emitters used by the call sites below ────────────────────────────────────
    sealed(detail?: A) {
        return self.emit("GATE", "GATE_SEALED", "CELL IS SEALED — CHANGE SEALED TO FALSE TO RESUME WRITES", detail);
    },

    frozen(detail?: A) {
        return self.emit("GATE", "GATE_FROZEN", "CELL IS FROZEN — VIEWABLE BUT NOT EDITABLE", detail);
    },

    asleep(detail?: A) {
        return self.emit("GATE", "GATE_ASLEEP", "CELL IS ASLEEP — WILL NOT BE AVAILABLE UNTIL ITS CONFIGURED WAKE TIME", detail);
    },

    shardNotFound(name: S, detail?: A) {
        return self.emit("REGISTRY", "SHARD_NOT_FOUND", `NO SHARD LABELED "${name}" RELATIVE TO THIS CELL — REVIEW YOUR CONTRACT`, detail);
    },

    contractViolation(contract: S, detail?: A) {
        return self.emit("CONTRACTS", "CONTRACT_VIOLATION", `CONTRACT VIOLATION: ${contract}`, detail);
    },

    schemaViolation(topic: S, detail?: A) {
        return self.emit("SCHEMA", "SCHEMA_VIOLATION", `SCHEMA VIOLATION ON TOPIC "${topic}"`, detail);
    },

    permissionDenied(ch: S, detail?: A) {
        return self.emit("PERMISSIONS", "PERMISSION_DENIED", `PERMISSION DENIED ON CHANNEL "${ch}"`, detail);
    },

    /** Flip an entire section or a single module. */
    setSection(path: MODULE_PATH, on: B): void {
        (self.SURGICAL_LOGS ??= {})[path] = on;
    },

    clearSurgical(): void {
        self.SURGICAL_LOGS = null;
    },

    // ── diagnostics ───────────────────────────────────────────────────────────────────────────
    recent(limit = 20): ERROR_EMISSION[] {
        return self.error_history.slice(-limit);
    },

    byModule(module: MODULE_PATH): ERROR_EMISSION[] {
        return self.error_history.filter(e => e.module === module);
    },

    byCode(code: S): ERROR_EMISSION[] {
        return self.error_history.filter(e => e.code === code);
    },

    purge(): void {
        self.error_history.length = 0;
    },
}));
// #endregion
//                                                                                                                        EVENT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
type EVENT_LISTENER = (payload?: A) => void;
export const EVENT = BIND_ATOM("EVENT", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    events: new Map<S, Set<EVENT_LISTENER>>(),
    wildcard: new Set<(event: S, payload?: A) => void>(),

    on(event: S, fn: EVENT_LISTENER): () => void {
        const set = self.events.get(event) ?? new Set<EVENT_LISTENER>();
        set.add(fn);
        self.events.set(event, set);

        return () => set.delete(fn);
    },

    once(event: S, fn: EVENT_LISTENER): () => void {
        const off = self.on(event, (p: A) => {
            off();
            fn(p);
        });

        return off;
    },

    onAny(fn: (event: S, payload?: A) => void): () => void {
        self.wildcard.add(fn);
        return () => self.wildcard.delete(fn);
    },

    off(event: S, fn?: EVENT_LISTENER): void {
        if (!fn) self.events.delete(event);
        else self.events.get(event)?.delete(fn);
    },

    emit(event: S, payload?: A): number {
        let delivered = 0;

        for (const fn of self.wildcard) {
            try {
                fn(event, payload);
                delivered++;
            } catch (e) {
                console.error(`[event-shard] wildcard listener failed for "${event}":`, e);
            }
        }

        for (const fn of [...(self.events.get(event) ?? [])]) {
            try {
                fn(payload);
                delivered++;
            } catch (e) {
                console.error(`[event-shard] listener failed for "${event}":`, e);
            }
        }

        return delivered;
    },

    async emitAsync(event: S, payload?: A): Promise<number> {
        let delivered = 0;

        for (const fn of self.wildcard) {
            try {
                await fn(event, payload);
                delivered++;
            } catch (e) {
                console.error(`[event-shard] wildcard listener failed for "${event}":`, e);
            }
        }

        for (const fn of [...(self.events.get(event) ?? [])]) {
            try {
                await fn(payload);
                delivered++;
            } catch (e) {
                console.error(`[event-shard] listener failed for "${event}":`, e);
            }
        }

        return delivered;
    },

    listenerCount(event?: S): number {
        if (event) return self.events.get(event)?.size ?? 0;

        let n = self.wildcard.size;

        for (const set of self.events.values()) {
            n += set.size;
        }

        return n;
    },

    eventNames(): S[] {
        return [...self.events.keys()];
    },

    purge(): void {
        self.events.clear();
        self.wildcard.clear();
    },
}));
// #endregion
//                                                                                                        EXTERNAL_SUBSCRIPTION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const EXTERNAL_SUBSCRIPTION = BIND_ATOM("EXTERNAL_SUBSCRIPTION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    externalSubscribers: new Set<LISTENER<W>>(),

    subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
        self.externalSubscribers.add(listener);

        return () => {
            self.externalSubscribers.delete(listener);
        };
    },

    notify(change: CHANGE<W>): void {
        for (const listener of self.externalSubscribers) {
            try {
                listener(change);
            } catch (error) {
                console.error(`[soul-shard] listener failed for "${String(change.key)}":`, error);
            }
        }
    },

    get size(): number {
        return self.externalSubscribers.size;
    },

    purge(): void {
        self.externalSubscribers.clear();
    },
}));
// #endregion
//                                                                                                                         GATE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const GATE = BIND_ATOM("GATE", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    sealed: (config.sealed ?? false) as B,
    frozen: (config.frozen ?? false) as B,
    sleep: config.sleep as N | undefined,

    get asleep(): B {
        return self.sleep !== undefined && Date.now() < self.sleep;
    },

    seal(): V {
        self.sealed = true;
    },

    unseal(): V {
        self.sealed = false;
    },

    freeze(): V {
        self.frozen = true;
    },

    unfreeze(): V {
        self.frozen = false;
    },

    sleepUntil(timestamp: N): V {
        self.sleep = timestamp;
    },

    wake(): V {
        self.sleep = undefined;
    },

    canRead(): B {
        return !self.asleep;
    },

    canWrite(): B {
        return !self.sealed && !self.frozen && !self.asleep;
    },
}));

// #endregion
//                                                                                                          GLOBAL_SUBSCRIPTION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const GLOBAL_SUBSCRIPTION = BIND_ATOM("GLOBAL_SUBSCRIPTION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    globalListeners: new Set<LISTENER<W>>(),

    subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
        self.globalListeners.add(listener);

        return () => {
            self.globalListeners.delete(listener);
        };
    },

    notify(change: CHANGE<W>): void {
        for (const listener of self.globalListeners) {
            try {
                listener(change);
            } catch (error) {
                console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
            }
        }
    },

    get size(): number {
        return self.globalListeners.size;
    },

    purge(): void {
        self.globalListeners.clear();
    },
}));
// #endregion
//                                                                                                                         HOOK
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface HOOK_ENTRY {
    value: A;
    teardown?: () => void;
    registeredAt: N;
}
export const HOOK = BIND_ATOM("HOOK", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    hooks: new Map<S, HOOK_ENTRY>(),
    factories: new Map<S, () => A>(),

    /** Register a hook factory. The factory runs once, lazily, on first read. */
    register(name: S, factory: () => A): void {
        self.factories.set(name, factory);
        // if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('hook_registered', undefined, undefined, undefined, { name });
    },

    /** Read the hook's value, computing it on first access. */
    use(name: S): A {
        const existing = self.hooks.get(name);
        if (existing) return existing.value;

        const factory = self.factories.get(name);
        if (!factory) throw new Error(`[hook-ELECTRON] no hook registered as "${name}"`);

        const value = factory();
        self.hooks.set(name, { value, registeredAt: Date.now() });

        // if (self.DEVTOOLS_ACTIVE) self.emitDevtoolsEvent('hook_computed', undefined, undefined, undefined, { name });

        return value;
    },

    isComputed(name: S): B {
        return self.hooks.has(name);
    },

    /** Drop the memoized value; the next use() re-runs the factory. Returns whether an entry existed. */
    reset(name: S): B {
        const entry = self.hooks.get(name);
        if (!entry) return false;

        self.hooks.delete(name);

        try {
            entry.teardown?.();
        } catch (e) {
            console.error(`[hook-ELECTRON] teardown failed for "${name}":`, e);
        }

        return true;
    },

    /** Drop the factory and the value. The hook is gone until re-registered. */
    unregister(name: S): void {
        self.reset(name);
        self.factories.delete(name);
    },

    list(): S[] {
        return [...self.factories.keys()];
    },

    purge(): void {
        for (const entry of self.hooks.values()) {
            try {
                entry.teardown?.();
            } catch (e) {
                console.error("[hook-ELECTRON] teardown failed during purge:", e);
            }
        }

        self.hooks.clear();
        self.factories.clear();
    },
}));
// #endregion
//                                                                                                               HTTP_TRANSPORT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
import { BINARY_CODEC } from "./comps/binary";
import { BUS_CRYPTO } from "./comps/DISRUPTOR_CRYPTO";
export interface HttpEnvelope {
    rt: string;
    ms: string;
    sl: string;
    key: string;
    payload: unknown;
    seq?: number;
    ts?: number;
    id?: string;
}
export interface HttpTransportOptions {
    /** Shared bearer token. Checked on every inbound request. */
    token: string;
    /** Outbound: request timeout, ms. */
    timeoutMs?: number;
    /** Outbound: first retry backoff, ms. Doubles, capped. */
    backoffMs?: number;
    maxBackoffMs?: number;
    /** Outbound: max attempts per send. 1 = no retry. */
    maxAttempts?: number;
    /** The CBOR codec (BINARY_DISRUPTOR's encode_msg/decode_msg). */
    codec: typeof BINARY_CODEC;
    /** Called on any send failure after retries, or protocol violation. */
    onError?: (err: unknown) => void;
}
const seqId = (() => {
    let n = 0;
    return (): string => `t${Date.now().toString(36)}-${(++n).toString(36)}`;
})();
export const HTTP_TRANSPORT = BIND_ATOM("HTTP_TRANSPORT", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    token: config.token as string,
    timeoutMs: (config.timeoutMs ?? 10_000) as number,
    backoffMs: (config.backoffMs ?? 100) as number,
    maxBackoffMs: (config.maxBackoffMs ?? 5_000) as number,
    maxAttempts: (config.maxAttempts ?? 3) as number,
    codec: BINARY_CODEC as typeof BINARY_CODEC,
    onError: (config.onError ?? (() => {})) as (err: unknown) => void,

    /** Inbound dispatcher — set at attach(). Receives decoded envelopes. */
    dispatch: null as ((env: HttpEnvelope) => Promise<unknown> | unknown) | null,

    // ── OUTBOUND: send an envelope to another IP ─────────────────────────────
    async send(
        endpoint: string,
        env: HttpEnvelope,
    ): Promise<{ ok: boolean; result?: unknown; error?: string }> {
        const envelope: Required<Pick<HttpEnvelope, "ts" | "id">> & HttpEnvelope = {
            ts: Date.now(),
            id: seqId(),
            ...env,
        };

        const body = self.codec.encode_msg(envelope);

        let attempt = 0;
        let lastErr: unknown = null;

        while (attempt < self.maxAttempts) {
            attempt++;

            try {
                const res = await fetch(endpoint, {
                    method: "POST",
                    headers: {
                        "content-type": "application/octet-stream",
                        authorization: `Bearer ${self.token}`,
                    },
                    body: body as unknown as BodyInit,
                    signal: AbortSignal.timeout(self.timeoutMs),
                });

                if (!res.ok) {
                    lastErr = new Error(`[http-transport] endpoint ${endpoint} responded ${res.status}`);

                    // 4xx = caller's fault, retrying will not help. Stop now.
                    if (res.status >= 400 && res.status < 500) break;
                    // 5xx / network: retry with backoff
                } else {
                    const raw = new Uint8Array(await res.arrayBuffer());
                    const decoded = self.codec.decode_msg(raw);

                    if (decoded === null) {
                        lastErr = new Error(`[http-transport] endpoint ${endpoint} returned undecodable ack`);
                    } else {
                        return decoded as { ok: boolean; result?: unknown; error?: string };
                    }
                }
            } catch (err) {
                lastErr = err;
                // network-level failure: retry with backoff
            }

            if (attempt < self.maxAttempts) {
                await new Promise(r =>
                    setTimeout(
                        r,
                        Math.min(self.backoffMs * 2 ** (attempt - 1), self.maxBackoffMs),
                    ),
                );
            }
        }

        self.onError(lastErr);

        return {
            ok: false,
            error: lastErr instanceof Error ? lastErr.message : String(lastErr),
        };
    },

    // ── INBOUND: accept POSTs from the wire ──────────────────────────────────
    /**
     * Wire this into any runtime's http server. `handler` must:
     *   • match on the path (transport does not route — the host decides)
     *   • read the full request body into a Uint8Array
     *   • call this.receive(bytes) and send back the returned ack frame
     * Node: http.createServer((req, res) => { if (transport.matches(req)) { ... res.end(ack) } })
     * Bun: Bun.serve({ fetch: (req) => transport.matchesBun(req) ? new Response(ack) : undefined })
     * Deno: same shape as Bun via req.arrayBuffer()
     */
    attach(dispatch: (env: HttpEnvelope) => Promise<unknown> | unknown): void {
        self.dispatch = dispatch;
    },

    /** Path/content-type matcher for host servers. */
    matches(pathname: string, method: string): boolean {
        return method === "POST" && pathname === "/bus";
    },

    /**
     * The one method a host server calls: bytes in → ack bytes out.
     * Auth is checked BEFORE dispatch. Dispatch happens inside try/catch —
     * a throwing handler becomes an error frame, never a crash.
     */
    async receive(bytes: Uint8Array, authHeader: string | null): Promise<Uint8Array> {
        const ack = (ok: boolean, result?: unknown, error?: string): Uint8Array =>
            self.codec.encode_msg({ ok, result, error });

        if (authHeader !== `Bearer ${self.token}`) {
            return ack(false, undefined, "unauthorized");
        }

        const env = self.codec.decode_msg(bytes);

        if (env === null || typeof env !== "object") {
            return ack(false, undefined, "undecodable envelope");
        }

        const e = env as Partial<HttpEnvelope>;

        if (
            typeof e.rt !== "string" ||
            typeof e.ms !== "string" ||
            typeof e.sl !== "string" ||
            typeof e.key !== "string"
        ) {
            return ack(false, undefined, "malformed envelope: rt/ms/sl/key required");
        }

        if (!self.dispatch) {
            return ack(false, undefined, "no dispatcher attached");
        }

        try {
            const result = await self.dispatch(env as HttpEnvelope);
            return ack(true, result);
        } catch (err) {
            return ack(
                false,
                undefined,
                err instanceof Error ? err.message : String(err),
            );
        }
    },
}));
// #endregion
//                                                                                                                      METRICS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const METRICS = BIND_ATOM("METRICS", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",


    adapters: {
        metrics: new Map<string, {
            id: string;
            total: number;
            failures: number;
            totalLatency: number;
            disabled: boolean;
            lastWindow: number;
            lastCount: number;
        }>(),
    },

    seen: new Set<string>(),

    getSeenCount(): number {
        return self.seen.size;
    },

    getMessageCount(rt: string): number {
        return self.messageCount.get(rt) ?? 0;
    },

    messageCount: new Map<string, number>(),

    getAdapterMetrics(): AdapterMetrics[] {
        const now = Date.now();
        const out: AdapterMetrics[] = [];

        for (const m of self.adapterMetrics.values()) {
            const windowSec = Math.max(1, (now - m.lastWindow) / 1000);
            const throughput = m.lastCount / windowSec;
            const failureRate = m.total === 0 ? 0 : m.failures / m.total;
            const latencyMs = m.total === 0 ? 0 : m.totalLatency / m.total;

            out.push({
                id: m.id,
                latencyMs,
                failureRate,
                throughputPerSec: throughput,
                healthy: !m.disabled && failureRate < 0.5,
                disabled: m.disabled,
            });

            m.lastWindow = now;
            m.lastCount = 0;
        }

        return out;
    },

    _queueMetrics: {
        totalEnqueued: 0,
        totalDropped: 0,
        totalProcessed: 0,
        peakSize: 0,
        currentSize: 0,
    },
}));
// #endregion
//                                                                                                                 RIFTS_ACCESS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const RIFTS_ACCESS = BIND_ATOM("RIFTS_ACCESS", ({ self, master, config }: any) => ({
    k: config.k ?? "__MODULE_ATOM__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    initial: config.options?.module_type === "class_fn_pool" || !config.options?.module_type ? { ...config.initial } : config.initial,
    module: config.options?.module_type === "class_fn_pool" || !config.options?.module_type ? { ...config.initial } : config.initial,
    module_type: config.options?.module_type ?? "class_fn_pool",
    version: 0,
    sealed: false,
    frozen: (config.options?.module_type ?? "class_fn_pool") === "class_fn_pool"        ? false        : config.options?.frozen ?? true,
    hasWritten: false,
    persisted: false,
    listeners: new Set<(change: Change<A>) => void>(),
    waiters: new Set<(value: A) => void>(),
    undoStack: undefined as HistoryEntry[] | undefined,
    conflict: config.options?.conflict as ConflictPolicy | undefined,
    historyIndex: 0,
    history: config.options?.history ? [{ k: 0, value: config.initial, timestamp: Date.now(), source: "initial", }] : undefined,
    historyMax: config.options?.history ? Math.max(1, config.options.history.max ?? 50) : 50,
    lifecycle: config.options?.lifecycle as SignalLifecycle | undefined,
    async: undefined as AsyncSignalState | undefined,
    metrics: undefined as Metrics | undefined,
    conflictStrategy: config.options?.conflictStrategy as        "latest" | "fail" | "accumulate" | undefined,
    schema: config.options?.schema as SignalSchema | undefined,
    contracts: new Map<string, AtomContract<A>>(),
    causal: [] as CausalEntry[],
    lastWriteSource: undefined as string | undefined,
    batches: new Set<SignalKey>(),
    batchQueue: new Map<SignalKey, unknown>(),
    batchOpts: new Map<SignalKey, { triggerRender?: boolean }>(),

    triggerRender: true,
    boundary: "same-app" as Boundary,
    read<T = A>(): T { return self.module;    },
    peek<T = A>(): T { return self.module;    },
    write<T = A>( incoming: any | ((prev: T) => T), options: AtomWriteOptions = {}, ): T {
        if (self.frozen) return self.peek();
        const prev = self.module;
        const resolved = incoming;
        const next = self.module_type === "class_fn_pool" ? resolved ? { ...prev, ...resolved } : prev : resolved;
        self.module = next;
        self.hasWritten = true;
        self.version += 1;
        self.lastWriteSource = options.source ?? "write";
        self.assertContracts(next, prev);

        for (const l of self.listeners) { try { l(next); } catch (e) {} }
        root.bus.send(next,self.k,self.s, self.m, self.r, );
 
        for (const w of self.waiters) { try { w(next); } catch (e) {} }
        if (options.recordHistory !== false) { self.recordHistory(prev, next, self.lastWriteSource); }
        self.recordCausal(next, self.lastWriteSource, options.cause);
        self.waiters.clear();

        self.notifyListeners({
            key: self.key,
            previous: prev,
            current: next,
            version: self.version,
            timestamp: Date.now(),
            source: self.lastWriteSource,
        });

        return next;
    },
    register<T = A>(name: string, val: any): T { return self.write((prev: A) => ({ ...prev, [name]: val }));    },
    unregister<T = A>(name: string): T {
        return self.write((prev: A) => {
            const { [name]: _gone, ...rest } = prev;
            return rest;
        });
    },
    slot(name: string): any { return self.module[name]; },
    on(listener: RiftListener<any>): void { self.listeners.add(listener); listener(self.module); },
    waitFor<T = A>(): Promise<T> { return new Promise(resolve => { self.waiters.add(resolve); });    },
    async waitForSlot(name: string, timeoutMs = 0): Promise<any> {
        const existing = self.module[name];
        if (existing !== undefined) return existing;

        if (!timeoutMs) {
            const next = await self.waitFor();
            return (next as Record<string, any>)[name];
        }

        return Promise.race([
            self.waitFor().then(next => (next as Record<string, any>)[name]),
            new Promise((_, reject) =>
                setTimeout(
                    () => reject(
                        new Error(`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}ms`),
                    ),
                    timeoutMs,
                ),
            ),
        ]);
    },
    inspect<T = A>(): T { return { ...self.module }; },
    setSchema(schema?: SignalSchema) { self.schema = schema; return self; },
    setLifecycle<T = A>(lifecycle?: SignalLifecycle<T>) { self.lifecycle = lifecycle; return self; },
    setConflict( conflict?: ConflictPolicy, strategy?: "latest" | "fail" | "accumulate", ) {
        self.conflict = conflict;
        self.conflictStrategy = strategy;
        return self;
    },
    setFrozen(): void { self.frozen = true; },
    setUnfrozen(): void { self.frozen = false; },
    setSealed(): void { self.sealed = true; },
    setUnsealed(): void { self.sealed = false; },
    setBoundary() {},
    validate<T = A>(next: T): T {
        if (!self.schema) return next;
        return root.signals._.validateSchema(self.schema, next);
    },

    assertContracts<T = A>(next: T, previous: T): void {
        for (const contract of self.contracts.values()) {
            let valid = false;

            try {
                valid = contract.postcondition(next, previous);
            } catch (error) {
                console.error(
                    `[Sauron] contract "${contract.name}" threw for "${String(self.key)}":`,
                    error,
                );
            }

            if (valid) continue;

            const message =
                `[Sauron] contract "${contract.name}" violated ` +
                `on write to "${String(self.key)}"`;

            if (contract.block) {
                throw new Error(message);
            }

            console.error(message);
        }
    },

    recordHistory<T = A>(previous: T, next: T, source?: string): void {
        if (!self.history) return;

        self.history.splice(self.historyIndex + 1);

        const last = self.history[self.history.length - 1];

        self.history.push({
            k: (last?.k ?? -1) + 1,
            prev: previous,
            value: next,
            next,
            timestamp: Date.now(),
            source,
            undone: false,
        });

        while (self.history.length > self.historyMax) {
            self.history.shift();
        }

        self.historyIndex = self.history.length - 1;
    },

    getHistory<T = A>(): readonly AtomHistoryEntry<T>[] {
        return self.history ?? [];
    },

    timeTravel<T = A>(steps = 1): T | undefined {
        if (!self.history?.length || steps <= 0) return undefined;

        const idx = Math.max(
            0,
            Math.min(self.history.length - 1, self.historyIndex + steps),
        );

        if (idx === self.historyIndex) return undefined;

        self.historyIndex = idx;
        return self.replayHistory("time-travel");
    },

    back<T = A>(steps = 1): T | undefined {
        return self.timeTravel(-Math.abs(steps));
    },

    forward<T = A>(steps = 1): T | undefined {
        return self.timeTravel(Math.abs(steps));
    },

    goto<T = A>(sequence: number): T | undefined {
        if (!self.history?.length) return undefined;

        const idx = self.history.findIndex(entry => entry.k === sequence);
        if (idx < 0 || idx === self.historyIndex) return undefined;

        self.historyIndex = idx;
        return self.replayHistory("time-travel.goto");
    },

    replayHistory<T = A>(source: string): T {
        const entry = self.history![self.historyIndex];

        return self.write(entry.value as T, {
            source,
            cause: "time-travel",
            triggerRender: true,
            recordHistory: false,
        });
    },

    recordCausal<T = A>(value: T, source?: string, cause?: string): void {
        self.causal.push({
            value,
            source,
            cause,
            timestamp: Date.now(),
        } as CausalEntry);

        if (self.causal.length > 100) {
            self.causal.shift();
        }
    },

    recordMetrics(): void {
        if (!self.metrics) return;

        self.metrics.writeCount += 1;
        self.metrics.lastWrite = Date.now();
    },

    notifyListeners<T = A>(change: Change<T>): void {
        const listeners = self._listeners.get(self.key);
        if (!listeners) return;

        for (const listener of listeners) {
            try {
                listener(change);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] listener failed:`,
                    error,
                );
            }
        }
    },

    resolveWaiters<T = A>(value: T): void {
        const waiting = self._waiters.get(self.key);
        if (!waiting) return;

        self._waiters.delete(self.key);

        for (const resolve of waiting) {
            try {
                resolve(value);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] waiter failed:`,
                    error,
                );
            }
        }
    },

    notifyExternalSubscribers(): void {
        const subscribers = self._externalSubscribers.get(self.key);
        if (!subscribers) return;

        for (const callback of subscribers) {
            try {
                callback();
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(self.key)}] external subscriber failed:`,
                    error,
                );
            }
        }
    },
}));
// #endregion
//                                                                                                                        ROUTE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const ROUTE = BIND_ATOM("ROUTE", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    routeRules: [] as RouteRule[],

    toRegex(pattern: S): RegExp {
        return new RegExp(
            "^" +
                pattern
                    .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
                    .replace(/\*\*/g, ".*")
                    .replace(/\*/g, "[^.]+") +
                "$",
        );
    },

    addRouteRule(rule: RouteRule): void {
        self.routeRules.push(rule);

        if (self.DEVTOOLS_ACTIVE) {
            self.emitDevtoolsEvent("route_added", undefined, undefined, undefined, {
                name: rule.name,
            });
        }
    },

    removeRouteRule(name: S): void {
        self.routeRules = self.routeRules.filter(r => r.name !== name);

        if (self.DEVTOOLS_ACTIVE) {
            self.emitDevtoolsEvent("route_removed", undefined, undefined, undefined, { name });
        }
    },

    listRouteRules(): RouteRule[] {
        return [...self.routeRules];
    },

    /** First matching enabled rule wins. Returns the rewritten target(s); adapter defaults to same_boundary. */
    resolve(rt: S, ms: S, sl: S, k: S): RouteTarget[] {
        for (const rule of self.routeRules) {
            if (rule.enabled === false) continue;
            if (!self.toRegex(rule.match).test(rt)) continue;

            const target: RouteTarget = {
                adapter: rule.adapter ?? "same_boundary",
                rt: rule.target?.rt ?? rt,
                ms: rule.target?.ms ?? ms,
                sl: rule.target?.sl ?? sl,
                k: rule.target?.k ?? k,
               // priority: rule.priority,
            };

            if (self.DEVTOOLS_ACTIVE) {
                self.emitDevtoolsEvent("route_resolved", rt, ms, undefined, {
                    rule: rule.name,
                    adapter: target.adapter,
                });
            }

            return [target];
        }

        // no rule: the identity target — same address, same boundary (PATCH 4's fallback)
        return [{ adapter: "same_boundary", rt, ms, sl, k }];
    },

    purge(): void {
        self.routeRules.length = 0;
    },
}));
// #endregion
//                                                                                                                      SESSION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const SESSION = BIND_ATOM("SESSION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    _cse_token: (config.cse_token ?? null) as S | null,
    _sessionId: null as S | null,
    _sessionToken: null as S | null,
    _session_started_at: null as N | null,
    _session_expires_at: null as N | null,
    _auth_vized: false as B,
    _auth_initialized: false as B,
    _sessionTtlMs: (config.sessionTtlMs ?? 1000 * 60 * 60 * 12) as N,

    // ── crypto ─────────────────────────────────────────────────────────────────────────────────
    computeHmacSha256(secret: S, data: S): S {
        if (BUS_CRYPTO) {
            const key = BUS_CRYPTO.to_utf8_bytes(secret);
            const mac = BUS_CRYPTO.hash_hmac(key, data);
            return BUS_CRYPTO.to_hex(BUS_CRYPTO.from_base64(mac));
        }

        return "";
    },

    // ── lifecycle ──────────────────────────────────────────────────────────────────────────────
    /**
     * Stand the session up. Reads CSE_TOKEN from .env when no token was injected.
     * No token anywhere? The session still runs, but UNAUTHORIZED (matches the bus's stance).
     */
    async initAuth(): Promise<void> {
        if (self._auth_initialized) return;

        if (!self._cse_token && typeof process !== "undefined") {
            try {
            //   if (dotenv) dotenv.config();
            } catch {
                // env loader optional
            }

            const sk = (typeof process !== "undefined" && process.env?.CSE_TOKEN) || null;
            if (sk && sk.length >= 5) self._cse_token = sk;
        }

        self._newSession();
        self._auth_initialized = true;
    },

    _newSession(): void {
        const now = Date.now();

        if (self._cse_token && BUS_CRYPTO) {
            self._sessionId = BUS_CRYPTO.to_hex(BUS_CRYPTO.random_bytes(16));
            self._sessionToken = self.computeHmacSha256(self._cse_token, self._sessionId);
        } else {
            // degraded mode: still unique, just not cryptographically bound
            self._sessionId = `sess_${now}_${Math.random().toString(36).slice(2)}`;
            self._sessionToken = null;
        }

        self._session_started_at = now;
        self._session_expires_at = now + self._sessionTtlMs;
        self._auth_vized = !!self._sessionToken;
    },

    /** Rotate the session id + token. Anything wrapped before is invalid after this. */
    renew(): void {
        self._newSession();
    },

    /** Force-expire. secureUnwrap() will reject everything until renew() or initAuth(). */
    expire(): void {
        self._session_expires_at = Date.now() - 1;
        self._auth_vized = false;
    },

    isValid(): B {
        return self._session_expires_at !== null && Date.now() < self._session_expires_at;
    },

    get expired(): B {
        return !self.isValid();
    },

    get isSecure(): B {
        return self._cse_token !== null && self._sessionToken !== null;
    },

    // ── wrap / unwrap ──────────────────────────────────────────────────────────────────────────
    /** Wrap a payload with the cell's session identity. */
    secureWrap(p: A) {
        return {
            sessionId: self._sessionId,
            token: self._sessionToken,
            ts: Date.now(),
            p,
        };
    },

    /** Unwrap + verify. Tampered, stale, or foreign session → null. */
    async secureUnwrap(msg: A): Promise<A | null> {
        if (!msg?.sessionId || !msg?.token) return null;
        if (msg.sessionId !== self._sessionId) return null; // not our session
        if (!self.isValid()) return null; // our session but expired

        const expected = await self.computeHmacSha256(
            self._cse_token ?? "",
            msg.sessionId,
        );

        if (self._sessionToken && msg.token !== expected) return null;

        return msg.p;
    },

    purge(): void {
        self._sessionId = null;
        self._sessionToken = null;
        self._session_started_at = null;
        self._session_expires_at = null;
        self._auth_vized = false;
        self._auth_initialized = false;
    },
}));
// #endregion
//                                                                                                                 SUBSCRIPTION
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const SUBSCRIPTION = BIND_ATOM("SUBSCRIPTION", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

    listeners: new Set<LISTENER<W>>(),

    subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
        self.listeners.add(listener);

        return () => {
            self.listeners.delete(listener);
        };
    },

    notify(change: CHANGE<W>): void {
        for (const listener of self.listeners) {
            try {
                listener(change);
            } catch (error) {
                console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
            }
        }
    },

    get size(): number {
        return self.listeners.size;
    },

    purge(): void {
        self.listeners.clear();
    },
}));
// #endregion
//                                                                                                                       WAITER
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export const WAITER = BIND_ATOM("WAITER", ({ self, master, config }: any) => ({
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",


    waiters: new Set<{
        resolve: (value: A) => void;
        reject: (reason?: A) => void;
    }>(),

    wait<T = A>(timeout?: number): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const waiter = {
                resolve: resolve as (value: A) => void,
                reject,
            };

            self.waiters.add(waiter);

            if (timeout !== undefined && timeout > 0) {
                const timer = setTimeout(() => {
                    if (self.waiters.delete(waiter)) {
                        reject(new Error("SOUL_ELECTRON wait timed out"));
                    }
                }, timeout);

                const originalResolve = waiter.resolve;
                waiter.resolve = value => {
                    clearTimeout(timer);
                    originalResolve(value);
                };

                const originalReject = waiter.reject;
                waiter.reject = reason => {
                    clearTimeout(timer);
                    originalReject(reason);
                };
            }
        });
    },

    resolve<T = A>(value: T): number {
        const waiters = [...self.waiters];
        self.waiters.clear();

        for (const waiter of waiters) {
            waiter.resolve(value as A);
        }

        return waiters.length;
    },

    reject(reason: A): number {
        const waiters = [...self.waiters];
        self.waiters.clear();

        for (const waiter of waiters) {
            waiter.reject(reason);
        }

        return waiters.length;
    },

    purge(): void {
        self.reject(new Error("SOUL_ELECTRON waiter ELECTRON destroyed"));
    },
}));
// #endregion
//                                                                                                                MASTER VERTEX



// ============================================================================
//  STEP 0 — masters as the registry actually holds them
//  The registry holds the BINDABLE FACTORY for "GATE", not an instance, so
//  NexusMirror's `prop in arr` never matches and every property becomes a stub.
// ============================================================================
const attempt = (fn: () => any) => {
	try { return { ok: true, value: fn() }; }
	catch (e: any) { return { ok: false, value: String(e.message ?? e).split("\n")[0] }; }
};


console.log("--- STEP 0: read with the registry exactly as signals.ts leaves it ---");
const shippedVertex = vertexContract({ value: { count: 0 } });
console.log("  vertex.read() ->", JSON.stringify(attempt(() => shippedVertex.read())));
//console.log('  registry "GATE" holds a ->', typeof CELLS.get("GATE"), "(a bindable factory, not an instance)");

// PLACEHOLDER: put a resolved gate instance in the registry, which is what a
// live hub does. Everything after this line assumes that.
// CELLS.set("GATE", { sealed: false, frozen: false });

// ============================================================================
//  THREE CONTRACTS — one per master, 1-2 bindables each
// ============================================================================
console.log("\n--- THREE MASTERS, THEIR BINDABLES ---");

const vertex = vertexContract({ value: { count: 0 }, conflictStrategy: "accumulate", defaultTtlMs: 60_000 });
const atomMaster = atomMasterContract({ value: 0, defaultTtlMs: 1_000 });
const moduleMaster = moduleContract({ value: [], conflictStrategy: "latest" });

// ---------------------------------------------------------------------------
//  1. MASTER PATH — flat, no .master indirection
// ---------------------------------------------------------------------------
console.log("\n[1] master path");
console.log("  vertex.read()            ->", JSON.stringify(vertex.read()));
console.log("  vertex.write({count:5})  ->", vertex.write({ count: 5 }));
console.log("  vertex.read() after      ->", JSON.stringify(vertex.read()));
console.log("  atomMaster.read()        ->", JSON.stringify(atomMaster.read()));
console.log("  atomMaster.write(7)      ->", atomMaster.write(7), "| read() =", atomMaster.read());
console.log("  moduleMaster.mount('a')  ->", moduleMaster.mount("a", { id: "a" }));
console.log("  moduleMaster.list()      ->", JSON.stringify(moduleMaster.list()));
console.log("  vertex.atom              ->", vertex.atom, "| atomMaster.atom ->", atomMaster.atom, "| moduleMaster.atom ->", moduleMaster.atom);

// ---------------------------------------------------------------------------
//  2. NAMESPACING — method lives under its bindable key
// ---------------------------------------------------------------------------
console.log("\n[2] namespacing");
console.log("  vertex.conflict.resolve(10, 5) ->", vertex.conflict.resolve(10, 5), "(accumulate)");
vertex.ttl.set("session", "abc", 30_000);
console.log("  vertex.ttl.get('session')      ->", vertex.ttl.get("session"));
console.log("  vertex.ttl.has('missing')      ->", vertex.ttl.has("missing"));
console.log("  moduleMaster.conflict.resolve('x','y') ->", moduleMaster.conflict.resolve("x", "y"), "(latest)");
console.log("  bindable keys on vertex        ->", JSON.stringify(Object.keys(vertex).filter((k) => ["conflict", "ttl", "gate"].includes(k))));
console.log("  flat vertex.resolve(...)       ->", JSON.stringify(attempt(() => (vertex as any).resolve(1, 2))));
console.log("  flat vertex.set(...)           ->", JSON.stringify(attempt(() => (vertex as any).set("a", 1))));

// ---------------------------------------------------------------------------
//  3. ISOLATION
// ---------------------------------------------------------------------------
console.log("\n[3] isolation");
const has = (obj: any, key: string) => Object.keys(obj).includes(key);

const vertex2 = vertexContract({ value: { count: 0 }, conflictStrategy: "accumulate", defaultTtlMs: 60_000 });
vertex.write({ count: 99 });
console.log("  same-contract second instance  ->", JSON.stringify(vertex2.read()), "(first wrote 99)");

const vertex3 = vertexContract({ value: { count: 0 }, conflictStrategy: "fail", defaultTtlMs: 1 });
vertex.ttl.set("only-first", 1, 30_000);
console.log("  vertex3.ttl.get('only-first')  ->", JSON.stringify(vertex3.ttl.get("only-first")));
console.log("  vertex3.conflictStrategy       ->", JSON.stringify(vertex3.conflict.conflictStrategy));
console.log("  vertex.conflictStrategy        ->", JSON.stringify(vertex.conflict.conflictStrategy), "(unchanged by vertex3)");

console.log("  'ttl' on moduleMaster          ->", has(moduleMaster, "ttl"));
console.log("  'conflict' on atomMaster       ->", has(atomMaster, "conflict"));
console.log("  'mount' on vertex              ->", has(vertex, "mount"));
console.log("  'resolve' on atomMaster        ->", has(atomMaster, "resolve"));
console.log("  'read' on vertex / atomMaster  ->", has(vertex, "read"), "/", has(atomMaster, "read"));

// 3d. refs fall back to the GLOBAL registry by name
console.log("\n[3d] unresolved ctx refs fall back to the global registry:");
console.log("  vertex.conflict.TTL   ->", typeof (vertex.conflict as any).TTL);
console.log("  vertex.conflict.GATE  ->", typeof (vertex.conflict as any).GATE);
console.log("  atomMaster.ttl.MODULE ->", typeof (atomMaster.ttl as any).MODULE);
console.log("  atomMaster.ttl.rawCellRead('ATOM') ->", typeof root.bear.cell("ATOM"));

// 3e. the real reach: bindable ctx falls back to the global registry by name,
//     so any bindable can grab any other registered factory.
const SPY = BIND_ATOM("SPY", ({ self, master, TTL: otherTtl, MODULE: otherModule, CONFLICT: otherConflict }: any) => ({
	k: "__SIGNALS__", s: "SAURON_SLAVE_REG", m: "SAURON_MASTER_REG", r: "THE_WIZARD_ROOT_REG",
	// refs are array proxies, so typeof is "object" — but it is a live handle
	stolen: { ttl: typeof otherTtl, module: typeof otherModule, conflict: typeof otherConflict },
	// and it can BUILD them against a fake instance
	buildForeign() {
		const fake = { value: 0, entries: new Map() };
		return typeof otherTtl === "function" ? Object.keys(otherTtl.__build(fake)).slice(0, 4) : null;
	},
}));

const spyContract = ATOM_CONTRACT(() => ({ atom: "MODULE", value: [], bindables: { spy: SPY } }));
const spyAtom = spyContract({ value: [] });
console.log("  spy.stolen                     ->", JSON.stringify(spyAtom.spy.stolen));
console.log("  spy.buildForeign()             ->", JSON.stringify(spyAtom.spy.buildForeign()));
	// new in this version: the contract itself is registered by name
	console.log("  registry ATOM_CONTRACTn        ->", typeof (root.bear.cell as any)("ATOM_CONTRACT1"), typeof (root.bear.cell as any)("ATOM_CONTRACT2"));
	console.log("  registry cell('ATOM_CONTRACT1')  ->", typeof (root.bear.cell as any)("ATOM_CONTRACT1"), "| is a contract:", typeof (root.bear.cell as any)("ATOM_CONTRACT1") === "function");

const results = [
	["vertex.read() flat (99 was the last write)", JSON.stringify(vertex.read()), JSON.stringify({ count: 99 })],
	["atomMaster.read() flat", atomMaster.read(), 7],
	["conflict namespaced + accumulate", vertex.conflict.resolve(10, 5), 15],
	["module conflict latest", moduleMaster.conflict.resolve("x", "y"), "y"],
	["ttl namespaced", vertex.ttl.get("session"), "abc"],
	["flat resolve throws (typo guard)", attempt(() => (vertex as any).resolve(1, 2)).ok, false],
	["flat set throws (typo guard)", attempt(() => (vertex as any).set("a", 1)).ok, false],
	["same contract, separate values", JSON.stringify(vertex2.read()), JSON.stringify({ count: 0 })],
	["separate bindable state", vertex3.ttl.get("only-first"), undefined],
	["separate bindable config", vertex3.conflict.conflictStrategy, "fail"],
	["no ttl on MODULE atom", has(moduleMaster, "ttl"), false],
	["no conflict on ATOM atom", has(atomMaster, "conflict"), false],
	["no MODULE method on VERTEX atom", has(vertex, "mount"), false],
];

console.log("\n--- RESULTS ---");
let fails = 0;
for (const [label, actual, expected] of results as [string, any, any][]) {
	const ok = JSON.stringify(actual) === JSON.stringify(expected);
	if (!ok) fails++;
	console.log(`${ok ? "PASS" : "FAIL"}  ${label}${ok ? "" : `  got ${JSON.stringify(actual)} want ${JSON.stringify(expected)}`}`);
}
console.log(fails === 0 ? "\nAll checks passed." : `\n${fails} check(s) failed.`);

// #endregion
/**
 Addressing
 r = root/app key
 m = master key (component or provider id)
 s = slave key (hook slot or consumer id)
 k = payload key (state key or context key)
 
 For hooks, the natural mapping is:
 r = app/root
 m = component id
 s = hook slot index
 k = state key
 
 For context:
 r = app/root
 m = provider id
 s = consumer id
 k = context key
 */

/** BUS
 export const counter = atom_contract<{ count: number }>(() => ({
    atom: "VERTEX",
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
    bindables: [
        atom_bindable<{ increment(): void }>(({ self }) => ({
            increment() {
                self.value.count += 1;
            },
        })),
    ],
}));

const instance = counter({ count: 0 });

instance.increment();
instance.value.count; // 1

export const fastCounter = atom_contract(() => ({
    atom: "VERTEX",
    value: { count: 0 },
}));

export const counter = atom_contract(() => ({
    ttl: { ms: 60_000 },
    history: true,
    snapshot: true,
}));



const push = bus.stream(
    "app",
    "telemetry",
    "gpu",
    "v1",
    (payload) => {
        console.log("gpu tick:", payload);
    },
);

push({ frame: 1 });
push({ frame: 2 });
push({ frame: 3 });

const client = bus.duplex(
    "app",
    "session",
    "ws",
    "v1",
    (message) => {
        console.log("client received:", message);
    },
);

const server = bus.duplex(
    "app",
    "session",
    "ws",
    "v1",
    (message) => {
        console.log("server received:", message);
    },
);

client.send({ hello: "server" });
server.send({ hello: "client" });

// Consumer side (subscribe)
const push = bus.stream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        // this runs for every message pushed to this address
        console.log("received:", payload);
    },
);

// Producer side (send)
push({ frame: 1 });
push({ frame: 2 });

const producer = bus.stream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        console.log("consumer got:", payload);
    },
);

producer({ frame: 1 });  // producer side

const stream = bus.openStream(
    "app", "telemetry", "gpu", "v1",
    (payload) => {
        console.log("consumer got:", payload);
    },
);

stream.send({ frame: 1 });   // producer side
stream.close();              // optional teardown

// Endpoint A
const aSend = bus.stream("app", "session", "ws", "v1:a2b");
bus.subscribe("app", "session", "ws", "v1:b2a", (msg) => {
    console.log("A got:", msg);
});

// Endpoint B
const bSend = bus.stream("app", "session", "ws", "v1:b2a");
bus.subscribe("app", "session", "ws", "v1:a2b", (msg) => {
    console.log("B got:", msg);
});

// Cross-communication
aSend({ hello: "B" });   // B's handler fires
bSend({ hello: "A" });   // A's handler fires



 const bus = new MESSAGE_BUS_DISRUPTOR(4096);

 // Multiple subscribers on the same address
 const stopA = bus.subscribe("app", "user", "create", "v1", async (payload) => {
     return `A:${payload}`;
 });
 
 const stopB = bus.subscribe("app", "user", "create", "v1", (payload) => {
     return `B:${payload}`;
 });
 
 // Awaited send receives subscriber results
 const result = await bus.send("app", "user", "create", "v1", "skyler");
 
 // result === ["A:skyler", "B:skyler"]
 
 // Single-subscriber fast path
 bus.subscribe("app", "ping", "direct", "v1", (p) => `pong:${p}`);
 
 const pong = await bus.send("app", "ping", "direct", "v1", "hi");
 // pong === "pong:hi"
 
 // Fire-and-forget
 bus.post("app", "metrics", "tick", "v1", { n: 1 });
 
 // Unsubscribe
 stopA();
 stopB();
  */
