/* THE_FORCE.ts
 *
 * Pull-based reactive signals engine.
 * Optimized for real UI workloads:
 * - writes invalidate, they do not eagerly update the world
 * - computeds recompute only when read
 * - effects run once per flush
 * - direct DOM bindings write only on actual value change
 * - four-tier channel format: r -> m -> s -> k
 */
//                                                                                                                      IMPORTS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
'use strict';
// #endregion
//                                                                                                                       TYPES
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const FORCE: unique symbol = Symbol('force');
export type KeyListener = (key: string, value: any, prev: any) => void;
const defined = (v: unknown): boolean => v !== undefined;
/* Flags */
export type Unsub = () => void;
export type Listener<T = any> = (value: T, prev: T | undefined, key: string) => void;
const NONE = 0;
const DIRTY = 1;
const DISPOSED = 2;
const RUNNING = 4;
const HAS_ERROR = 8;
/* Node kinds */
const SIGNAL = 0;
const COMPUTED = 1;
const EFFECT = 2;
interface Link {
    source: Node;
    target: Node;
    version: number;

    prevSource: Link | undefined;
    nextSource: Link | undefined;

    prevTarget: Link | undefined;
    nextTarget: Link | undefined;
}
interface Node {
    kind: 0 | 1 | 2;
    flags: number;
    version: number;

    /** Dependencies read by this node. */
    sources: Link | undefined;
    sourcesTail: Link | undefined;

    /** Consumers that read this node. */
    targets: Link | undefined;
    targetsTail: Link | undefined;

    /** Effect scheduling list. */
    nextQueuedEffect: EffectNode | undefined;
}
interface SignalNode<T = unknown> extends Node {
    kind: 0;
    value: T;
}
interface ComputedNode<T = unknown> extends Node {
    kind: 1;
    value: T | undefined;
    compute: (previous: T | undefined) => T;
}
interface EffectNode extends Node {
    kind: 2;
    run: () => unknown | (() => unknown);
    cleanup: (() => unknown) | undefined;
}
type AnyNode = SignalNode | ComputedNode | EffectNode;
export interface ChannelMessage<P = U> {
    id?: string;
	r?: S;
	m?: S;
	s: S;
	k: S;
	p: U;
    ts?: N;
    _ack?: boolean;
	_reply?: boolean;
	_rid?: string;
	ackTo?: string;
}
type BUS_HANDLER<P = U, T = U> = (p?: P, msg?: ChannelMessage) => T | Promise<T>;
export type ChannelHandler<P = A, T = A> = (payload?: P, message?: ChannelMessage<P>,) => T | Promise<T>;
interface Channel {
    handler: ChannelHandler | undefined;
    streamHead: StreamLink | undefined;
    streamTail: StreamLink | undefined;
}
interface StreamLink {
    fn: (message: ChannelMessage) => void;
    prev: StreamLink | undefined;
    next: StreamLink | undefined;
}
type hdr<P = A, R = A> = (	msg: ChannelMessage<P>,	reply: (value: R | Promise<R>) => V,  ) => A;
type c_type = "mesh" | 'globalThis'
export const END = Symbol("rift:end");
const EMPTY: any[] = Object.freeze([]) as any;
export interface KV {
	getItem(k: string): string | null;
	setItem(k: string, v: string): void;
	removeItem(k: string): void;
	readonly length?: number;
	key?(i: number): string | null;
}
export type BodyMsg =
	| { o: 's'; k: string; v: any }
	| { o: 'd'; k: string }
	| { o: 'm'; kv: [string, any][] }
	| { o: 'x'; ks: string[] }
	| { o: 'c' };
export type Msg = BodyMsg & { i: string };
export interface Transport {
	post(msg: Msg): void;
	listen(cb: (msg: Msg) => void): void;
	close(): void;
}
// #endregion
//                                                                                                                      SIGNALS
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

/* ---------------------------------------------------------------------- *
 * Engine state
 * ---------------------------------------------------------------------- */
let activeTarget: Node | undefined;
let activeUntrackedDepth = 0;
let batchDepth = 0;
let cycle = 0;
let effectQueueHead: EffectNode | undefined;
let effectQueueTail: EffectNode | undefined;
let flushScheduled = false;
/* ---------------------------------------------------------------------- *
 * Equality
 * ---------------------------------------------------------------------- */
function changed(previous: unknown, next: unknown): boolean {
    return previous !== next && !(previous !== previous && next !== next);
}
/* ---------------------------------------------------------------------- *
 * Link graph
 * ---------------------------------------------------------------------- */
function link(source: Node, target: Node, version: number): void {
    const previousSourceLink = target.sourcesTail;
    if (previousSourceLink !== undefined && previousSourceLink.source === source  ) {  previousSourceLink.version = version;  return;  }
    const nextSourceLink =  previousSourceLink !== undefined ? previousSourceLink.nextSource : target.sources;
    if ( nextSourceLink !== undefined &&  nextSourceLink.source === source  ) {
        nextSourceLink.version = version;
        target.sourcesTail = nextSourceLink;
        return;
    }
    const previousTargetLink = source.targetsTail;
    const newLink: Link = {
        source,
        target,
        version,
        prevSource: previousSourceLink,
        nextSource: nextSourceLink,
        prevTarget: previousTargetLink,
        nextTarget: undefined,
    };

    target.sourcesTail = newLink;
    source.targetsTail = newLink;
    if (nextSourceLink !== undefined) {  nextSourceLink.prevSource = newLink; }
    else { target.sources = newLink; }
    if (previousSourceLink !== undefined) {  previousSourceLink.nextSource = newLink;  }
    if (previousTargetLink !== undefined) {  previousTargetLink.nextTarget = newLink; } 
    else {  source.targets = newLink;  }
}
function unlink(linkNode: Link, target: Node = linkNode.target): Link | UD {
    const source = linkNode.source;
    const previousSource = linkNode.prevSource;
    const nextSource = linkNode.nextSource;
    const previousTarget = linkNode.prevTarget;
    const nextTarget = linkNode.nextTarget;
    if (nextSource !== undefined) {nextSource.prevSource = previousSource;} else {target.sourcesTail = previousSource;}
    if (previousSource !== undefined) {previousSource.nextSource = nextSource;} else {target.sources = nextSource;}
    if (nextTarget !== undefined) {nextTarget.prevTarget = previousTarget;} else {source.targetsTail = previousTarget;}
    if (previousTarget !== undefined) {previousTarget.nextTarget = nextTarget;} else {source.targets = nextTarget;}
    return nextSource;
}
function clearSources(target: Node): void {
    let linkNode = target.sourcesTail;
    while (linkNode !== undefined) {
        const previous = linkNode.prevSource;
        unlink(linkNode, target);
        linkNode = previous;
    }
    target.sources = undefined;
    target.sourcesTail = undefined;
}
function removeStaleSources(target: Node):void {
    const tail = target.sourcesTail;
    let linkNode = tail !== undefined ? tail.nextSource : target.sources;
    while (linkNode !== undefined) {linkNode = unlink(linkNode, target);}
}
/* ---------------------------------------------------------------------- *
 * Effect queue
 * ---------------------------------------------------------------------- */
function enqueueEffect(effect: EffectNode): void {
    if (effect.nextQueuedEffect !== undefined) return;
    if (effect === effectQueueTail) return;
    effect.nextQueuedEffect = effect;
    if (effectQueueTail === undefined) {
        effectQueueHead = effect;
        effectQueueTail = effect;
        return;
    }
    effectQueueTail.nextQueuedEffect = effect;
    effectQueueTail = effect;
}
function scheduleFlush(): void {
    if (flushScheduled) return;
    flushScheduled = true;
    queueMicrotask(flushEffects);
}
function flushEffects():void {
    flushScheduled = false;
    let effect = effectQueueHead;
    while (effect !== undefined) {
        const next = effect.nextQueuedEffect;
        effect.nextQueuedEffect = undefined;
        runEffect(effect);
        effect = next;
    }
    effectQueueHead = undefined;
    effectQueueTail = undefined;
}
/* ---------------------------------------------------------------------- *
 * Evaluation
 * ---------------------------------------------------------------------- */
function readComputed<T>(computed: ComputedNode<T>): T {
    if ((computed.flags & DISPOSED) !== NONE) {throw new Error('Cannot read a disposed computed.');    }
    const subscriber = activeTarget;
    if (subscriber !== undefined) {link(computed, subscriber, cycle);    }
    if ((computed.flags & DIRTY) === NONE) {return computed.value as T;    }
    return recomputeComputed(computed);
}
function recomputeComputed<T>(computed: ComputedNode<T>): T {
    const previousValue = computed.value;
    computed.flags |= RUNNING;
    computed.flags &= ~DIRTY;
    const previousTarget = activeTarget;
    activeTarget = computed;
    clearSources(computed);
    try {
        cycle++;
        computed.value = computed.compute(previousValue);
    } finally {
        activeTarget = previousTarget;
        computed.flags &= ~RUNNING;
        removeStaleSources(computed);
    }
    const nextValue = computed.value;
    if (!changed(previousValue, nextValue)) {return nextValue; }
    computed.version = cycle;
    let targetLink = computed.targets;// Notify direct effects only. Computeds remain lazy.
    while (targetLink !== undefined) {
        const target = targetLink.target;
        if (target.kind === EFFECT) {enqueueEffect(target as EffectNode);}
        targetLink = targetLink.nextTarget;
    }
    return nextValue;
}
function runEffect(effect: EffectNode): void {
    if ((effect.flags & DISPOSED) !== NONE) return;
    if (effect.cleanup !== undefined) {
        const cleanup = effect.cleanup;
        effect.cleanup = undefined;
        const previousTarget = activeTarget;
        activeTarget = undefined;
        try {cleanup();} finally {activeTarget = previousTarget;}
    }
    clearSources(effect);
    effect.flags |= RUNNING;
    effect.flags &= ~DIRTY;
    const previousTarget = activeTarget;
    activeTarget = effect;
    try {
        cycle++;
        const result = effect.run();
        effect.cleanup = typeof result === 'function' ? (result as () => unknown) : undefined;
    } finally {
        activeTarget = previousTarget;
        effect.flags &= ~RUNNING;
        removeStaleSources(effect);
    }
}
/* ---------------------------------------------------------------------- *
 * Public signal
 * ---------------------------------------------------------------------- */
export interface Signal<T = U> {
    (): T;
    (value: T): void;
    peek(): T;
    node: SignalNode<T>;
}
export function signal<T>(initialValue?: T): Signal<T> {
    const node: SignalNode<T> = {
        kind: SIGNAL,
        flags: NONE,
        version: 0,
        value: initialValue as T,
        sources: undefined,
        sourcesTail: undefined,
        targets: undefined,
        targetsTail: undefined,
        nextQueuedEffect: undefined,
    };

    const fn = ((value?: T) => {
        if (arguments.length === 0) return node.value;
        const previous = node.value;
        if (!changed(previous, value as T)) return;
        node.value = value as T;
        node.version = ++cycle;
        let targetLink = node.targets;
        while (targetLink !== undefined) {
            const target = targetLink.target;
            if (target.kind === EFFECT) {enqueueEffect(target as EffectNode);}
            targetLink = targetLink.nextTarget;
        }
        if (batchDepth === 0) scheduleFlush();
    }) as Signal<T>;
    fn.node = node;
    fn.peek = () => node.value;
    return fn;
}
/* ---------------------------------------------------------------------- *
 * Public computed
 * ---------------------------------------------------------------------- */
export interface Computed<T = U> {
    (): T;
    node: ComputedNode<T>;
}
export function computed<T>(compute: (previous?: T) => T,): Computed<T> {
    const node: ComputedNode<T> = {
        kind: COMPUTED,
        flags: DIRTY,
        version: 0,
        value: undefined,
        compute,
        sources: undefined,
        sourcesTail: undefined,
        targets: undefined,
        targetsTail: undefined,
        nextQueuedEffect: undefined,
    };
    const fn = () => readComputed(node) as T;
    fn.node = node;
    return fn as Computed<T>;
}
/* ---------------------------------------------------------------------- *
 * Public effect
 * ---------------------------------------------------------------------- */
export function effect(fn: () => U | (() => U),): () => void {
    const node: EffectNode = {
        kind: EFFECT,
        flags: NONE,
        version: 0,
        sources: undefined,
        sourcesTail: undefined,
        targets: undefined,
        targetsTail: undefined,
        nextQueuedEffect: undefined,
        run: fn,
        cleanup: undefined,
    };
    const previousTarget = activeTarget; 
    activeTarget = node;
    try {
        cycle++;
        const result = fn();
        node.cleanup = typeof result === 'function' ? (result as () => unknown) : undefined;
    } finally {
        activeTarget = previousTarget;
    }
    return () => {
        if ((node.flags & DISPOSED) !== NONE) return;
        node.flags |= DISPOSED;
        if (node.cleanup !== undefined) {
            const cleanup = node.cleanup;
            node.cleanup = undefined;
            cleanup();
        }
        clearSources(node);
    };
}
/* ---------------------------------------------------------------------- *
 * Batching
 * ---------------------------------------------------------------------- */
export function batch<T>(fn: () => T): T {
    batchDepth++;

    try {
        return fn();
    } finally {
        batchDepth--;

        if (batchDepth === 0) flushEffects();
    }
}
export function startBatch(): void  {
    batchDepth++;
}
export function endBatch(): void {
    if (--batchDepth === 0) flushEffects();
}
export function untracked<T>(fn: () => T): T {
    activeUntrackedDepth++;

    const previousTarget = activeTarget;
    activeTarget = undefined;

    try {
        return fn();
    } finally {
        activeTarget = previousTarget;
        activeUntrackedDepth--;
    }
}
/* ---------------------------------------------------------------------- *
 * DOM
 * ---------------------------------------------------------------------- */
export function bindText<T>(source: Signal<T> | Computed<T>,text: Text,): () => void {
    const node = (source as A).node as AnyNode;

    return effect(() => {
        const value = (source as () => T)();

        const next = value == null ? '' : String(value);

        if (text.data !== next) {
            text.data = next;
        }
    });
}
export function bindAttribute<T>(source: Signal<T> | Computed<T>, element: Element, attribute: S,): () => void {
    return effect(() => {
        const value = (source as () => T)();
        if (value == null || value === false) {
            if (element.hasAttribute(attribute)) {
                element.removeAttribute(attribute);
            }
            return;
        }
        const next = value === true ? '' : String(value);
        if (element.getAttribute(attribute) !== next) {element.setAttribute(attribute, next);}
    });
}
export function bind<T>(source: Signal<T> | Computed<T>,node: Node,write: (value: T, node: Node) => V,): () => void {
    return effect(() => {write((source as () => T)(), node);});
}
// #endregion
//                                                                                                                         SLOT
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

// #endregion
//                                                                                                                  BUS / CACHE
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class SIGNALS {
    public r: S = "THE_WIZARD_ROOT_REG";
    public m: S = "SAURON_MASTER_REG";
    public s: S = "SAURON_SLAVE_REG";
    public k = 'key'
    cache: c_type = "mesh"

	public bus = new (class SIGNAL_BUS {
        private _bus: Record<string, Record<string, Record<string, Record<string, Channel>>> > = Object.create(null);
        private _hot_bus: Record<string, Channel> = Object.create(null);/** Flat cache populated only through the tiered registration API. */
        public cache: c_type
        #counter: Uint32Array;
        #epoch = 0;
        public _byteid(): string {if (++this.#counter[0] === 0) this.#epoch++; return `${this.#epoch}:${this.#counter[0]}`;	}
        public _sid(): string { return this._byteid(); }
        public b(): string { return this._byteid(); }
        public r = 'THE_WIZARD_ROOT_REG'
        public m = 'SAURON_MASTER_REG'
        public s = 'SAURON_SLAVE_REG'
        public k = 'key'
        constructor(hub: SIGNALS) {
            this.r = hub.r
            this.m = hub.m
            this.s = hub.s
            this.k = hub.k
            this.cache = hub.cache
		    this._hot_bus = this.getS('_bus', this.m, this.r);
        }
        public send = (() => {
            const z = <T, P>(payload: P, key: string, s: string = this.s, m: string = this.m, r: string = this.r, ): T | undefined => {
                const channel = this._bus[r]?.[m]?.[s]?.[key];
                const handler = channel?.handler;
                if (handler === undefined) return undefined;
                return handler(payload, {
                    r: this.r,
                    m: this.m,
                    s: this.s,
                    k: key,
                    p: payload,
                }) as T | undefined;
            }
            z.hot = <T, P>(payload: P, key: string): T | undefined => {
                const handler = this._hot_bus[key]?.handler;
                if (handler === undefined) return undefined;
                return handler(payload) as T | undefined;
            }
            z.stream=<T = U>(p: U, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c:S='mesh'): T | P<T> | UD =>{
                const root =  c === 'globalThis' ? globalThis as any : this._bus;
                const rL = (root[r] ??= Object.create(null));
                const mL = (rL[m] ??= Object.create(null));
                const _listeners = mL[s] ??= Object.create(null);
                const handler = _listeners[k]?.handler;
                if (!handler) return undefined;
                const msg = { r, m, s, k, p: p, _ack: false };
                const result = handler(p, msg);
                if (handler) { for (const listener of handler) listener(msg); }
                return result as T | P<T> | UD;
            }
            return z;
        })();
        public sub = (() => {
            const fn = ( handler: A, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c: c_type = "mesh" ): () => V => {
                // CHANNEL LEVEL
                const channelSet = (this.getR(r, c)[r] ??= new Set<A>()) as Set<A>;
                channelSet.add(handler);
        
                // SUB_CHANNEL LEVEL
                const subMap = this.getM(m, r, c);
                const subSet = (subMap[r] ??= new Set<A>()) as Set<A>;
                subSet.add(handler);
        
                // TRIP_CHANNEL LEVEL
                const tripMap = this.getS(s, m, r, c);
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
                    if (!channelSet.size) delete this.getR(r, c)[r];
        
                    // Remove empty hierarchy nodes
                    if (!Object.keys(subMap).length) { delete this.getR(c, r)[m]; }
                    if (!Object.keys(tripMap).length) { delete this.getR(c, r)[m]; }
                };
            };
            fn.hot = (handler: A, k: S ) => {
                const root = this._hot_bus[k]?.handler
                let handlers = root[k] as Set<A> | undefined;
                if (!handlers) { handlers = new Set<A>(); root[k] = handlers as A; }
                handlers.add(handler);
                return () => { handlers!.delete(handler); if (handlers!.size === 0) { delete root[k]; } };
            }
            fn.gt = (() => {
                const fn =<P = U, T = U> (handler: BUS_HANDLER<P, T>, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c:S='mesh'): () => V=> {
                    const root =  c === 'globalThis' ? globalThis as any : this._bus;
                    const rL = (root[r] ??= O.create(null));
                    const mL = (rL[m] ??= O.create(null));
                    const sl = mL[s] ??= O.create(null); 
                      const channel = (sl[k] ??= Object.create(null));
                    channel.handler = handler;
                        return () => {
                            if (channel.handler === handler) {
                                channel.handler = undefined;
                            }
                        };
                 }
                fn.hot = (() => {
                    const fn = <P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = '_globalThis', ): (() => V) => {
                        const channel = this._hot_bus[k];
                        channel.handler = handler;
    
                        return () => {
                            if (channel.handler === handler) {
                                channel.handler = undefined;
                            }
                        }
                    }   
                    return fn;
                })();
                return fn;
            })();
            fn.adapter = (() => {
                const fn =( handler: A, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c: c_type = "mesh")  => { }
                fn.hot = (() => {
                    const fn = () =>{ }
                    return fn;
                })();
                return fn;
            })();
            fn.nested = (handler: (data: any) => void, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, c:S='mesh') => {
                if (r === this.r && m === this.m && s === this.s) {
                    const root =  this._hot_bus[k]?.handler; 
                    const _listeners = root[k];
                    if (!_listeners.has(k)) {
                        _listeners.set(k, new Set());
                    }
                    _listeners[k]!.add(handler);
                    return () => {
                        const set = _listeners.get(k);
                        if (set) {
                            set.delete(handler);
                            if (set.size === 0) _listeners.delete(k);
                        }
                    };
                } else {
                    const root = c === 'globalThis' ? globalThis as any : this._bus;
                    const rL = (root[r] ??= Object.create(null));
                    const mL = (rL[m] ??= Object.create(null));
                    let _listeners = mL[s] ??= Object.create(null);
                    _listeners = _listeners[k]
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
            fn.nested3 = (p: P, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r) => {
                if (r === this.r && m === this.m && s === this.s) {
                } else {
                }
            };
            return fn;
        })();
        public subSlow <P, T>(key: string, handler: ChannelHandler<P, T>, s: string = '_bus', m: string = this.m, r: string = this.r, ): () => void {
            const channel = this._channel(key, s, m, r);
            channel.handler = handler;
            this._hot_bus[key] = channel;
            return () => {if (channel.handler === handler) channel.handler = undefined;};
        }
        public hotSend<T, P>(payload: P, key: string): T | undefined {
            const handler = this._hot_bus[key]?.handler;
            if (handler === undefined) return undefined;
            return handler(payload) as T | undefined;
        }
        public hotSub<P, T>(key: string, handler: ChannelHandler<P, T>, s: string = '_bus', m: string = this.m, r: string = this.r, ): () => void {
            const channel =  this._hot_bus[key]
            channel.handler = handler;
            this._hot_bus[key] = channel;
            return () => {if (channel.handler === handler) channel.handler = undefined;};
        }
        public stream = (() => {
            const bus = this;
            function stream(handler: (msg: ChannelMessage) => V, k: S, s: S, m?: S, r?: S): () => V;
            function stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<ChannelMessage>;
            function stream(
                handlerOrSlave: ((msg: ChannelMessage) => V) | S,
                k: S,
                s: S = "_bus" as S,
                m: S = bus.m,
                r: S = bus.r
            ): (() => V) | AsyncIterableIterator<ChannelMessage> {
                const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
                const channel = (bus._hot_bus[k] ??= {
                    handler: undefined,
                    streamHead: undefined,
                    streamTail: undefined
                });
        
                if (handler) {
                    const link: StreamLink = {
                        fn: handler,
                        prev: channel.streamTail,
                        next: undefined
                    };
        
                    if (channel.streamTail) {
                        channel.streamTail.next = link;
                    } else {
                        channel.streamHead = link;
                    }
        
                    channel.streamTail = link;
        
                    return () => {
                        if (link.prev) {
                            link.prev.next = link.next;
                        } else {
                            channel.streamHead = link.next;
                        }
        
                        if (link.next) {
                            link.next.prev = link.prev;
                        } else {
                            channel.streamTail = link.prev;
                        }
        
                        link.prev = undefined;
                        link.next = undefined;
                    };
                }
        
                const queue: ChannelMessage[] = [];
                const waiters: Array<{
                    resolve: (value: IteratorResult<ChannelMessage>) => void
                }> = [];
        
                let offset = 0;
                let closed = false;
        
                const listener = (msg: ChannelMessage) => {
                    if (waiters.length) {
                        waiters.shift()!.resolve({ value: msg, done: false });
                    } else {
                        queue.push(msg);
                    }
                };
        
                const link: StreamLink = {
                    fn: listener,
                    prev: channel.streamTail,
                    next: undefined
                };
        
                if (channel.streamTail) {
                    channel.streamTail.next = link;
                } else {
                    channel.streamHead = link;
                }
        
                channel.streamTail = link;
        
                const close = () => {
                    if (closed) return;
                    closed = true;
        
                    if (link.prev) {
                        link.prev.next = link.next;
                    } else {
                        channel.streamHead = link.next;
                    }
        
                    if (link.next) {
                        link.next.prev = link.prev;
                    } else {
                        channel.streamTail = link.prev;
                    }
        
                    link.prev = undefined;
                    link.next = undefined;
        
                    for (const waiter of waiters) {
                        waiter.resolve({ value: undefined, done: true });
                    }
        
                    waiters.length = 0;
                    queue.length = 0;
                    offset = 0;
                };
        
                const iterator: AsyncIterableIterator<ChannelMessage> = {
                    next: (): Promise<IteratorResult<ChannelMessage>> => {
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
        
                    [Symbol.asyncIterator](): AsyncIterableIterator<ChannelMessage> {
                        return iterator;
                    }
                };
        
                return iterator;
            }
            const fn = () => stream
            fn.request = (p: A, onChunk: (chunk: A) => V, k: S, s: S = '_bus', m: S = this.m, r: S = this.r): P<V> => {
                const streamId = this._sid();// `[${r}][${m}][${s}][${k}][${_sid()}]`.replace(/[.\[\]]/g, match => (match === "." ? "_" : ""));
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
                    closeSubscription = this.sub(
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
            fn.native = (k?: string, s = '_bus', m = this.m, r = this.r): WritableStream => {
                return new WritableStream({ write: (chunk) => { this.send(chunk, k, s, m, r); }, });
            };
            fn.openSync = (k: S, s?: S, m?: S, r = this.r, opts?: {windowSize?: N}) => {
                const streamId = this._sid()//`${cleanChannel}-${m}-${Math.random().toString(36).slice(2)}`.replace(/\./g, "-");
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
                        if (missing) {missing.retries++;await this.hotSend({streamId, seq: reqData.seq, data: missing.p, isRetry: true}, k);}
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
            fn.sync = (k: S, s = '_bus', m = this.m, r = this.r, opts: SyncStreamOptions = {}): SyncStreamSendHandle => {
                const windowSize = opts.windowSize ?? 8;
                const retransmitMs = opts.retransmitMs ?? 5000;
                const inFlight = new Map<number, Outbound>();
                let nextSeq = 0;
                let closed = false;
                self;
                const unsub = this.sub( (data: unknown) => {
                        const d = data as {seq?: number} | null;
                        if (!d || typeof d.seq !== "number") return;
                        const o = inFlight.get(d.seq);
                        if (o && !o.acked) {
                            o.acked = true;
                            if (o.timer) clearTimeout(o.timer);
                            inFlight.delete(d.seq);
                        }
                    },`${k}:syncAck`,s,m,r,);
                const unsubData = this.sub( (data: unknown) => {
                        const d = data as {sync?: boolean; seq?: number} | null;
                        if (d && d.sync === true && typeof d.seq === "number") {
                            this.send({seq: d.seq}, `${k}:syncAck`, s, m, r);
                        }
                    }, k, s, m, r, );
        
                const push = (p: unknown): void => {
                    if (closed) throw new Error(`syncStream ${r}: push after close`);
                    if (inFlight.size >= windowSize) { throw new Error(`syncStream ${r}: window full (${windowSize})`); }
                    const o: Outbound = {seq: nextSeq++, p, acked: false, sentAt: Date.now()};
                    inFlight.set(o.seq, o);
                    const send = () => this.send({sync: true, seq: o.seq, p: o.p}, k, s, m, r);
                    send();
                    const arm = () => {o.timer = setTimeout(() => {if (!o.acked) { send(); arm(); } }, retransmitMs); };
                    arm();
                };
        
                const handle: SyncStreamSendHandle = {
                    push,
                    pendingCount: () => inFlight.size,
                    close: () => {
                        closed = true;
                        for (const o of inFlight.values()) if (o.timer) clearTimeout(o.timer);
                        unsub();
                        unsubData();
                    },
                };
                return handle;
            };
            fn.hot = (key: string, fn: (message: ChannelMessage) => void, s: string = '_bus', m: string = this.m, r: string = this.r, ): () => void => {
                    const channel = this._hot_bus[key]
                    const streamLink: StreamLink = { fn, prev: channel.streamTail, next: undefined, };
                    if (channel.streamTail !== undefined) {channel.streamTail.next = streamLink;}
                    else {channel.streamHead = streamLink;}
                    channel.streamTail = streamLink;
                    return () => {
                        const previous = streamLink.prev;
                        const next = streamLink.next;
            
                        if (next !== undefined) next.prev = previous;
                        else channel.streamTail = previous;
            
                        if (previous !== undefined) previous.next = next;
                        else channel.streamHead = next;
                    };
            }
            return fn;
        })();
        public streamSlow(key: string, fn: (message: ChannelMessage) => void, s: string = '_bus', m: string = this.m, r: string = this.r, ): () => void {
            const channel = this._channel(key, s, m, r);
            const streamLink: StreamLink = { fn, prev: channel.streamTail, next: undefined, };
            if (channel.streamTail !== undefined) {  channel.streamTail.next = streamLink;  }
            else {   channel.streamHead = streamLink;  }
            channel.streamTail = streamLink;
            return () => {
                const previous = streamLink.prev;
                const next = streamLink.next;
    
                if (next !== undefined) next.prev = previous;
                else channel.streamTail = previous;
    
                if (previous !== undefined) previous.next = next;
                else channel.streamHead = next;
            };
        }
        public hotStream(key: string, fn: (message: ChannelMessage) => void, s: string = '_bus', m: string = this.m, r: string = this.r, ): () => void {
            const channel = this._hot_bus[key]
            const streamLink: StreamLink = { fn, prev: channel.streamTail, next: undefined, };
            if (channel.streamTail !== undefined) {channel.streamTail.next = streamLink;}
            else {channel.streamHead = streamLink;}
            channel.streamTail = streamLink;
            return () => {
                const previous = streamLink.prev;
                const next = streamLink.next;
    
                if (next !== undefined) next.prev = previous;
                else channel.streamTail = previous;
    
                if (previous !== undefined) previous.next = next;
                else channel.streamHead = next;
            };
        }
        async *streamGen<P = A, R = A>(p: P, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r, opts: { adapter?: S } = {}): AG<R, V, U> {
                    const replyTo = this._sid();
                    const queue: R[] = [];
                    let closed = false;
                    let failure: U;
                    let wake: (() => V) | UD;
                    const handlers = this._hot_bus[k]
                    if (!handlers || !Object.keys(handlers).length) {throw new Error(`[rift] no subscriber for k="${k}"`);}
                    const reply = (value: R | Promise<R> | typeof END) => {
                        if (value === END) { closed = true; } 
                        else if (value instanceof Error) { failure = value; }
                        else { queue.push(value as R); }
                        wake?.();
                        wake = undefined;
                    };
                    for (const id in handlers) {
                        handlers[id]({id: this._sid(), r: r ?? this.r, m: m ?? this.m, s: s ?? '_bus', k, p: { p }, _ack: false, replyTo, stream: true, }, reply);
                    }
                
                    while (true) {
                        if (queue.length) { yield queue.shift()!; continue; }
                        if (failure) throw failure;
                        if (closed) return;
                        await new P<V>((resolveWake) => { wake = resolveWake; });
                    }
        }    
        public handler = (() => {
            const fn = (handler: (d: A) => A, k: S = this.k, s: S = '_globalThis', m: S = this.m, r: S = this.r, binary = false): (() => V) => {
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
            fn.native = (k?: S, s = '_globalThis', m = this.m, r = this.r) => {
                    let unsubscribe: (() => void) | null = null;
                    return new ReadableStream({
                        start: (controller) => { unsubscribe = this.sub( (data) => { try { controller.enqueue(data); } catch {}  }, k, s, m, r, ); },
                        cancel: () => { if (unsubscribe) { unsubscribe(); unsubscribe = null; } },
                    });
            };
            fn.openSync = (handler: (chunk: A, seq: N) => Promise<V> | V, k: S, s: S = '_globalThis', m: S = this.m, r: S = this.r): (() => V) => {
                    const expectedSeqMap = new Map<S, N>();
                    const bufferMap = new Map<S, Map<N, A>>();
            
                    return this.sub(
                        async (inbound: A) => {
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
            fn.sync = (k: string, s = '_globalThis', m = this.m, r = this.r): SyncStreamHandler => {
                    const seqs = new Map<number, unknown>();
                    const waiters = new Set<(p: unknown, seq: number) => void>();
                    const completes = new Set<() => void>();
                    let nextDeliver = 0;
                    let done = false;
            
                    const deliver = (): void => {
                        while (seqs.has(nextDeliver)) {
                            const p = seqs.get(nextDeliver)!;
                            seqs.delete(nextDeliver);
                            for (const cb of [...waiters]) {
                                try {
                                    cb(p, nextDeliver);
                                } catch {
                                    /* Consumable errors isolated */
                                }
                            }
                            nextDeliver++;
                        }
                    };
            
                    const unsub = this.sub(
                        (data: unknown) => {
                            const d = data as {sync?: boolean; seq?: number; p?: unknown; complete?: boolean} | null;
                            if (!d || d.sync !== true) return;
                            if (d.complete === true) {
                                done = true;
                                for (const cb of [...completes]) {
                                    try {
                                        cb();
                                    } catch {
                                        /* ignore */
                                    }
                                }
                                return;
                            }
                            if (typeof d.seq !== "number") return;
                            if (d.seq >= nextDeliver && !seqs.has(d.seq)) {
                                seqs.set(d.seq, d.p);
                                deliver();
                            }
                        },
                        k,
                        s,
                        m,
                        r,
                    );
            
                    return {
                        onOrdered(cb) {
                            waiters.add(cb);
                        },
                        onComplete(cb) {
                            completes.add(cb);
                        },
                        close() {
                            unsub();
                            waiters.clear();
                            completes.clear();
                        },
                    };
            };
            return fn;
        })();
        public hotPublish(message: ChannelMessage): void {
            let streamLink = this._hot_bus[message.k]?.streamHead;
            while (streamLink !== undefined) {
                streamLink.fn(message);
                streamLink = streamLink.next;
            }
        }
        public duplex(key: string) {
            const bus = this;
            return {
                send: <T, P>(payload: P) => bus.hotSend<T, P>(payload, key),
                sub: <P, T>(handler: ChannelHandler<P, T>) => bus.subSlow(key, handler),
                stream: (fn: (message: ChannelMessage) => void) => bus.streamSlow(key, fn),
            };
        }
        public duoDuplex = class duplex {
                public r: A = "THE_WIZARD_ROOT_REG";
                public m: A = "SAURON_MASTER_REG";
                public s: A = "SAURON_SLAVE_REG";
                public k: A = "__SAURON__";
                public hub: SIGNAL_BUS
                public abus: A
                public bbus: A
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
                    const fn = (k?: S, s = '_bus', m = this.m, r = this.r) => {
                        return {
                            send: <T, P>(p: P, sendOpts?: { timeoutMs?: N }) => this.abus.stream( k, s, m, r),
                            sub: <P, R>(handler: hdr<P, R>) => this.abus.handler(k, s, m, r),
                        }
                    };
                    return fn;
                })();
                public handler = (() => {
                    const fn = (k?: S, s = '_bus', m = this.m, r = this.r) => {
                        return {
                            send: <T, P>(p: P, sendOpts?: { timeoutMs?: N }) => this.abus.stream( k, s, m, r),
                            sub: <P, R>(handler: hdr<P, R>) => this.abus.handler(k, s, m, r),
                        }
                    };
                    return fn;
                })();
        };
        private _channel(key: string,s: string,m: string,r: string,): Channel {
            const root = (this._bus[r] ??= Object.create(null));
            const master = (root[m] ??= Object.create(null));
            const slave = (master[s] ??= Object.create(null));
            return (slave[key] ??= {
                handler: undefined,
                streamHead: undefined,
                streamTail: undefined,
            });
        }
        public getS(s: S = '_bus', m: S = this.m, r: S = this.r, c: c_type = "mesh") {
            const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this._bus;
            const rL = (cache[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            return sL;
        }
        public getM(m: S = this.m, r: S = this.r, c = "mesh") {
            const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this._bus;
            const rL = (cache[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            return mL;
        }
        public getR(r: S = this.r, c = "mesh") {
            const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this._bus;
            const rL = (cache[r] ??= Object.create(null));
            return rL;
        }
    })(this);
    public store = new (class STORE {
        private _store: Record<string, Record<string, Record<string, Record<string, A>>> > = Object.create(null);
        private _hot_store: Record<string, Slot> = Object.create(null);
        public r = 'THE_WIZARD_ROOT_REG'
        public m = 'SAURON_MASTER_REG'
        public s = 'SAURON_SLAVE_REG'
        public k = 'key'
        public cache: c_type
        private readonly err: (e: unknown) => void;
        private readonly pp: string;
        private readonly ser: (v: any) => string;
        private readonly de: (s: string) => any;
        private readonly st: KV | null;
        private readonly bf: ((k: string) => boolean) | null;
        private readonly id: string;
        private tx: Transport | null;
        private cnt = 0;
	    private ga: KeyListener[] = EMPTY;
        private pa: { p: string; f: KeyListener }[] = EMPTY;
        private dirty: Slot[] = [];
        private pq = false;
        private q: Record<string, any> = Object.create(null);

        constructor(hub: SIGNALS) {
            this.r = hub.r
            this.m = hub.m
            this.s = hub.s
            this.k = hub.k
            this.cache = hub.cache
		    this._hot_store = this.getS('_store', this.m, this.r);
            this.pp =  'sig:';
		this.ser =  ((v: any) => JSON.stringify(v));
		this.de = ((s: string) => JSON.parse(s));
		this.bf =  null;
		this.id = Math.random().toString(36).slice(2) + Date.now().toString(36);
		const ch = 'signals' 
		this.tx =  bcTransport(ch) 
		if (this.tx) this.tx.listen((msg) => this._receive(msg));
        }
        public signal = (() => {
                const fn = <T = A>(v: T | (() => T), k: SignalKey, s = this.s, m = this.m, r = this.r, c = "signal_cells") => {
                    const rL = this.getR(c, r);
                    const mL = (rL[m] ??= Object.create(null));
                    const registry = (mL[s] ??= Object.create(null));
                    let atom = registry[k];
            
                    if (atom && v === undefined) { return atom; }
                    if (atom && v !== undefined) {
                        const t = UF(v);
                        atom = signal(t);
                        const e = this._pinSlot(k, atom);
                        registry[k] = e;
                        return t;
                    }
            
                    const initialValue = UF(v);
                    atom = signal(initialValue);
                    const e = this._pinSlot(k, atom);
                    registry[k] = e;
                    return e;
                };
                fn.hot = <T = A>(  v: T | (() => T),k: SignalKey,) => {
                    const registry = this._hot_store;
                    let e = registry[k];
                
                    if (e !== undefined) {
                        const atom = e.v as Signal<T>;
                        if (v !== undefined) atom(UF(v));
                        return atom;
                    }
                
                    const atom = signal<T>(UF(v));
                    e = this._pinSlot(k, atom);
                    return atom;
                };
                return fn;
        })();
        atom<T = A>(k: S, init?: T): Atom<T> {
            return new Atom<T>(this, this._pinSlot(k, UF(init)));
        }
        get<X extends keyof M & string>(k: S): M[X] | undefined {
            const e = this._hot_store[k];
            return e === undefined ? undefined : e.v;
        }
        getOr<X extends keyof M & string>(k: S, fallback: M[X]): M[X] {
            const e = this._hot_store[k];
            return e !== undefined && e.has ? e.v : fallback;
        }
        peek<X extends keyof M & string>(k: S): M[X] | undefined {
            const e = this._hot_store[k];
            return e === undefined ? undefined : e.v;
        }
        read<X extends keyof M & string>(k: S): M[X] | undefined {
            const e = this._hot_store[k];
            return e === undefined ? undefined : e.v;
        }
        hot<X extends keyof M & string>(k: S, v?: M[X]): M[X] | undefined {
            if (arguments.length > 1) {
                this._w(this._ent(k), v, true);
                return v;
            }
            const e = this._hot_store[k];
            return e === undefined ? undefined : e.v;
        }
        getMany<X extends keyof M & string>(keys: readonly X[]): (M[X] | undefined)[] {
            const out = new Array(keys.length);
            for (let i = 0; i < keys.length; i++) {
                const e = this._hot_store[keys[i]];
                out[i] = e === undefined ? undefined : e.v;
            }
            return out;
        }
        pick<X extends keyof M & string>(keys: readonly S[]): Partial<Pick<M, X>> {
            const out: any = Object.create(null);
            for (let i = 0; i < keys.length; i++) {
                const k = keys[i];
                const e = this._hot_store[k];
                if (e !== undefined && e.has) out[k] = e.v;
            }
            return out;
        }
        has(k: string): boolean {
            const e = this._hot_store[k];
            return e !== undefined && e.has;
        }
        get size(): number {return this.cnt;}
        keys(): string[] {
            const out: string[] = [];
            for (const k in this._hot_store) {
                const e = this._hot_store[k];
                if (e.has) out.push(k);
            }
            return out;
        }
        values(): any[] {
            const out: any[] = [];
            for (const k in this._hot_store) {
                const e = this._hot_store[k];
                if (e.has) out.push(e.v);
            }
            return out;
        }
        entries(): [string, any][] {
            const out: [string, any][] = [];
            for (const k in this._hot_store) {
                const e = this._hot_store[k];
                if (e.has) out.push([k, e.v]);
            }
            return out;
        }
        getAll(): Partial<M> {
            const out: any = Object.create(null);
            for (const k in this._hot_store) {
                const e = this._hot_store[k];
                if (e.has) out[k] = e.v;
            }
            return out;
        }
        getByPrefix(prefix: string, strip = false): Record<string, any> {
            const out: any = Object.create(null);
            const n = prefix.length;
            for (const k in this._hot_store) {
                const e = this._hot_store[k];
                if (e.has && k.startsWith(prefix)) out[strip ? k.slice(n) : k] = e.v;
            }
            return out;
        }
        keysByPrefix(prefix: string): string[] {
            const out: string[] = [];
            for (const k in this._hot_store) {
                const e = this._hot_store[k];
                if (e.has && k.startsWith(prefix)) out.push(k);
            }
            return out;
        }
        set<X extends keyof M & string>(k: S, v: M[X]): boolean {
            let e = this._hot_store[k];
            if (e === undefined) e = this._mk(k);
            return this._w(e, v, true);
        }
        write<X extends keyof M & string>(k: S, v: M[X]): boolean {
            return this.set(k, v);
        }
        setLocal<X extends keyof M & string>(k: S, v: M[X]): boolean {
            let e = this._hot_store[k];
            if (e === undefined) e = this._mk(k);
            return this._w(e, v, false);
        }
        setMany(o: Partial<M>): number {
            const kv: [string, any][] = [];
            let n = 0;
            batch(() => {
                const ks = Object.keys(o);
                for (let i = 0; i < ks.length; i++) {
                    const k = ks[i];
                    if (this._sm(k, (o as any)[k], kv)) n++;
                }
            });
            if (kv.length !== 0 && this.tx !== null) this._post({ o: 'm', kv });
            return n;
        }
        create<T = A>(k: S, init: T | (() => T)): T {
            let e = this._hot_store[k];
            if (e !== undefined && e.has) return e.v;
            if (e === undefined) e = this._mk(k);
            this._w(e, typeof init === 'function' ? (init as () => T)() : init, true);
            return e.v;
        }
        update<X extends keyof M & string>(k: S, fn: (prev: M[X] | undefined) => M[X]): M[X] {
            let e = this._hot_store[k];
            if (e === undefined) e = this._mk(k);
            const next = fn(e.v);
            this._w(e, next, true);
            return next;
        }
        swap<X extends keyof M & string>(k: S, v: M[X]): M[X] | undefined {
            let e = this._hot_store[k];
            if (e === undefined) e = this._mk(k);
            const prev = e.v;
            this._w(e, v, true);
            return prev;
        }
        edit<X extends keyof M & string>(k: S, fn: (v: M[X]) => M[X] | void): boolean {
            const e = this._hot_store[k];
            if (e === undefined || !e.has) return false;
            const r = fn(e.v);
            if (r !== undefined && r !== e.v) this._w(e, r, true);
            else this._touch(e);
            return true;
        }
        touch(k: string): boolean {
            const e = this._hot_store[k];
            if (e === undefined || !e.has) return false;
            this._touch(e);
            return true;
        }
        _emit(k: string): void {
            const e = this._hot_store[k];
            if (e !== undefined) this._n(e, FORCE);
        }
        delete(k: string): boolean {
            const e = this._hot_store[k];
            if (e === undefined || !e.has) return false;
            this._d(e, true);
            return true;
        }
        deleteMany(keys: readonly string[]): number {
            const ks: string[] = [];
           batch(() => {
                for (let i = 0; i < keys.length; i++) {
                    const e = this._hot_store[keys[i]];
                    if (e !== undefined && e.has) {
                        this._d(e, false);
                        if (this._ok(e.k)) ks.push(e.k);
                    }
                }
            });
            if (ks.length !== 0 && this.tx !== null) this._post({ o: 'x', ks });
            return ks.length;
        }
        deleteByPrefix(prefix: string): number {
            return this.deleteMany(this.keysByPrefix(prefix));
        }
        clear(): void { this._clear(true);	}
        sub<T =A>(k: S, fn: Listener<T>, immediate = false): Unsub {
            const e = this._ent(k);
            const n = e.s.slice();
            n.push(fn);
            e.s = n;
            if (immediate && e.has) {
                try {
                    fn(e.v, undefined, k);
                } catch (x) {
                    this.err(x);
                }
            }
            let live = true;
            return () => {
                if (!live) return;
                live = false;
                const s = e.s;
                const i = s.indexOf(fn);
                if (i < 0) return;
                if (s.length === 1) e.s = EMPTY;
                else {
                    const c = s.slice();
                    c.splice(i, 1);
                    e.s = c;
                }
                if (!e.has) this._gc(e);
            };
        }
        _receive(msg: Msg): void {
                if (msg.i === this.id) return;
                switch (msg.o) {
                    case 's': {
                        this._w(this._ent(msg.k), msg.v, false);
                        break;
                    }
                    case 'd': {
                        const e = this.m[msg.k];
                        if (e !== undefined && e.has) this._d(e, false);
                        break;
                    }
                    case 'm': {
                        const kv = msg.kv;
                        batch(() => {
                            for (let i = 0; i < kv.length; i++) this._w(this._ent(kv[i][0]), kv[i][1], false);
                        });
                        break;
                    }
                    case 'x': {
                        const ks = msg.ks;
                        batch(() => {
                            for (let i = 0; i < ks.length; i++) {
                                const e = this.m[ks[i]];
                                if (e !== undefined && e.has) this._d(e, false);
                            }
                        });
                        break;
                    }
                    case 'c':
                        this._clear(false);
                        break;
                }
        }
        once<X extends keyof M & string>(k: S, fn: Listener<M[X]>): Unsub {
            const off: Unsub = this.sub(k, (v, p, key) => {
                off();
                fn(v, p, key);
            });
            return off;
        }
        subAll(fn: KeyListener): Unsub {
            const n = this.ga.slice();
            n.push(fn);
            this.ga = n;
            return () => {
                const i = this.ga.indexOf(fn);
                if (i < 0) return;
                if (this.ga.length === 1) this.ga = EMPTY;
                else {
                    const c = this.ga.slice();
                    c.splice(i, 1);
                    this.ga = c;
                }
            };
        }
        subPrefix(prefix: string, fn: KeyListener): Unsub {
            const rec = { p: prefix, f: fn };
            const n = this.pa.slice();
            n.push(rec);
            this.pa = n;
            return () => {
                const i = this.pa.indexOf(rec);
                if (i < 0) return;
                if (this.pa.length === 1) this.pa = EMPTY;
                else {
                    const c = this.pa.slice();
                    c.splice(i, 1);
                    this.pa = c;
                }
            };
        }
        subMany(keys: readonly (keyof M & string)[], fn: KeyListener): Unsub;
        subMany(map: { [S in keyof M & string]?: Listener<M[S]> }): Unsub;
        subMany(a: any, fn?: KeyListener): Unsub {
            const offs: Unsub[] = [];
            if (Array.isArray(a)) {
                for (let i = 0; i < a.length; i++) {
                    offs.push(this.sub(a[i], (v, p, k) => (fn as KeyListener)(k, v, p)));
                }
            } else {
                const ks = Object.keys(a);
                for (let i = 0; i < ks.length; i++) offs.push(this.sub(ks[i] as any, a[ks[i]]));
            }
            return () => {
                for (let i = 0; i < offs.length; i++) offs[i]();
            };
        }
        waitFor<X extends keyof M & string>(
            k: S,
            pred: (v: M[X]) => boolean = defined,
            o?: { timeout?: number; signal?: AbortSignal },
        ): Promise<M[X]> {
            const cur = this._hot_store[k];
            if (cur !== undefined && cur.has && pred(cur.v)) return Promise.resolve(cur.v);
            return new Promise<M[X]>((res, rej) => {
                let off: Unsub | undefined;
                let t: ReturnType<typeof setTimeout> | undefined;
                const sig = o?.signal;
                const done = () => {
                    if (off) off();
                    if (t !== undefined) clearTimeout(t);
                    if (sig) sig.removeEventListener('abort', ab);
                };
                const ab = () => {
                    done();
                    rej((sig as any).reason ?? new Error(`waitFor("${k}") aborted`));
                };
                off = this.sub(k, (v) => {
                    if (pred(v)) {
                        done();
                        res(v);
                    }
                });
                if (o?.timeout !== undefined) {
                    const ms = o.timeout;
                    t = setTimeout(() => {
                        done();
                        rej(new Error(`waitFor("${k}") timed out after ${ms}ms`));
                    }, ms);
                }
                if (sig) {
                    if (sig.aborted) ab();
                    else sig.addEventListener('abort', ab, { once: true });
                }
            });
        }
        derive<X extends keyof M & string>(
            out: S,
            sources: readonly (keyof M & string)[],
            fn: (...v: any[]) => M[X],
        ): Unsub {
            const run = () => {
                const args = new Array(sources.length);
                for (let i = 0; i < sources.length; i++) {
                    const e = this._hot_store[sources[i]];
                    args[i] = e === undefined ? undefined : e.v;
                }
                this.set(out, fn(...args));
            };
            const offs = sources.map((s) => this.sub(s, run));
            run();
            return () => offs.forEach((o) => o());
        }
        persist<X extends keyof M & string>(k: S, init?: M[X]): M[X] | undefined {
            const e = this._ent(k);
            const st = this.st;
            if (st !== null) {
                let raw: string | null = null;
                try {
                    raw = st.getItem(this.pp + k);
                } catch (x) {
                    this.err(x);
                }
                if (raw !== null) {
                    let ok = false;
                    let val: any;
                    try {
                        val = this.de(raw);
                        ok = true;
                    } catch (x) {
                        this.err(x);
                    }
                    if (ok) {
                        e.p = false;
                        this._w(e, val, false);
                        e.p = true;
                        return e.v;
                    }
                }
            }
            e.p = true;
            if (!e.has && init !== undefined) this._w(e, init, true);
            else if (e.has) this._dirty(e);
            return e.v;
        }
        persistMany(keys: readonly (keyof M & string)[]): void {
            for (let i = 0; i < keys.length; i++) this.persist(keys[i]);
        }
        unpersist(k: string, keepStorage = false): void {
            const e = this._hot_store[k];
            if (e !== undefined) {
                e.p = false;
                const i = this.dirty.indexOf(e);
                if (i !== -1) this.dirty.splice(i, 1);
            }
            if (!keepStorage && this.st !== null) {
                try {
                    this.st.removeItem(this.pp + k);
                } catch (x) {
                    this.err(x);
                }
            }
            if (e !== undefined && !e.has) this._gc(e);
        }
        unpersistMany(keys: readonly string[], keepStorage = false): void {
            for (let i = 0; i < keys.length; i++) this.unpersist(keys[i], keepStorage);
        }
        isPersisted(k: string): boolean {
            const e = this._hot_store[k];
            return e !== undefined && e.p;
        }
        hydrate(): number {
            const st = this.st;
            if (st === null || typeof st.key !== 'function' || st.length === undefined) return 0;
            const names: string[] = [];
            for (let i = 0; i < st.length; i++) {
                const full = st.key(i);
                if (full !== null && full.startsWith(this.pp)) names.push(full.slice(this.pp.length));
            }
            batch(() => {
                for (let i = 0; i < names.length; i++) this.persist(names[i] as any);
            });
            return names.length;
        }
        _broadcast(msg: BodyMsg): void {
            this._post(msg);
        }
        flush(): void {
            this.pq = false;
            const st = this.st;
            const dirty = this.dirty;
            this.dirty = [];
    
            if (st !== null) {
                for (let i = 0; i < dirty.length; i++) {
                    const e = dirty[i];
                    try {
                        if (e.has && e.v !== undefined) st.setItem(this.pp + e.k, this.ser(e.v));
                        else st.removeItem(this.pp + e.k);
                    } catch (x) {
                        this.err(x);
                    }
                }
            }
        }
        _w(e: Slot, v: any, bc: boolean): boolean {
            const prev = e.v;
            if (e.has) {
                if (v === prev || (v !== v && prev !== prev)) return false;
            } else {
                e.has = true;
                this.cnt++;
            }
            e.v = v;
            if (e.p) this._dirty(e);
            if (bc && this.tx !== null && this._ok(e.k)) this._post({ o: 's', k: e.k, v });
            if (batchDepth !== 0) {
                if (!Object.prototype.hasOwnProperty.call(this.q, e.k)) this.q[e.k] = prev;
            } else {
                this._fire(e, prev);
            }
            return true;
        }
        private _d(e: Slot, bc: boolean): void {
            const prev = e.v;
            e.v = undefined;
            e.has = false;
            this.cnt--;
            if (e.p) this._dirty(e);
            if (bc && this.tx !== null && this._ok(e.k)) this._post({ o: 'd', k: e.k });
            if (batchDepth !== 0) {
                if (!Object.prototype.hasOwnProperty.call(this.q, e.k)) this.q[e.k] = prev;
            } else {
                this._fire(e, prev);
                this._gc(e);
            }
        }
        private _touch(e: Slot): void {
            if (e.p) this._dirty(e);
            if (this.tx !== null && this._ok(e.k)) this._post({ o: 's', k: e.k, v: e.v });
            this._n(e, FORCE);
        }
        private _n(e: Slot, prev: any): void {
            if (batchDepth !== 0) {
                if (!Object.prototype.hasOwnProperty.call(this.q, e.k)) this.q[e.k] = prev;
            } else {
                this._fire(e, prev === FORCE ? e.v : prev);
            }
        }
        private _fire(e: Slot, prev: any): void {
            const v = e.v;
            const k = e.k;
            const s = e.s;
            for (let i = 0; i < s.length; i++) {
                try {
                    s[i](v, prev, k);
                } catch (x) {
                    this.err(x);
                }
            }
            const g = this.ga;
            for (let i = 0; i < g.length; i++) {
                try {
                    g[i](k, v, prev);
                } catch (x) {
                    this.err(x);
                }
            }
            const p = this.pa;
            if (p.length !== 0) {
                for (let i = 0; i < p.length; i++) {
                    if (k.startsWith(p[i].p)) {
                        try {
                            p[i].f(k, v, prev);
                        } catch (x) {
                            this.err(x);
                        }
                    }
                }
            }
        }
        private _flushQ(): void {
            const q = this.q;
            const ks = Object.keys(q);
            if (ks.length === 0) return;
    
            this.q = Object.create(null);
    
            for (let i = 0; i < ks.length; i++) {
                const k = ks[i];
                const e = this._hot_store[k];
                const prev = q[k];
                if (e === undefined) continue;
    
                if (prev === FORCE) this._fire(e, e.v);
                else if (!(e.v === prev || (e.v !== e.v && prev !== prev))) this._fire(e, prev);
    
                if (!e.has) this._gc(e);
            }
        }
        private _clear(bc: boolean): void {
            batch(() => {
                for (const k in this._hot_store) {
                    const e = this._hot_store[k];
                    if (e.has) this._d(e, false);
                }
            });
            if (bc && this.tx !== null) this._post({ o: 'c' });
        }
        private _sm(k: string, v: any, kv: [string, any][]): boolean {
            let e = this._hot_store[k];
            if (e === undefined) e = this._mk(k);
            if (this._w(e, v, false)) {
                if (this._ok(k)) kv.push([k, v]);
                return true;
            }
            return false;
        }
        private _mk(k: string): Slot {
            const e = new Slot(k);
            this._hot_store[k] = e;
            return e;
        }
        private _ent(k: string): Slot {
            let e = this._hot_store[k];
            if (e === undefined) e = this._mk(k);
            return e;
        }
        private _pinSlot(k: string, init: any): Slot {
            const e = this._ent(k);
            e.pin++;
            if (init !== undefined && !e.has) this._w(e, init, true);
            return e;
        }
        _gc(e: Slot): void {
            if (!e.has && e.s === EMPTY && e.pin === 0 && !e.p && batchDepth === 0 && this._hot_store[e.k] === e) {
                delete this._hot_store[e.k];
            }
        }
        _unpin(e: Slot): void {
            if (e.pin > 0) e.pin--;
            if (!e.has) this._gc(e);
        }
        private _dirty(e: Slot): void {
            if (this.dirty.indexOf(e) === -1) this.dirty.push(e);
            if (!this.pq) {
                this.pq = true;
                queueMicrotask(() => {
                    if (this.pq) this.flush();
                });
            }
        }
        private _ok(k: string): boolean {
            return this.bf === null || this.bf(k);
        }
        private _post(m: BodyMsg): void {
            const tx = this.tx;
            if (tx === null) return;
            try {
                tx.post({ ...m, i: this.id } as Msg);
            } catch (x) {
                this.err(x);
            }
        }
        public getS(s: S = '_store', m: S = this.m, r: S = this.r, c: c_type = "mesh") {
            const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this._store;
            const rL = (cache[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            const sL = (mL[s] ??= Object.create(null));
            return sL;
        }
        public getM(m: S = this.m, r: S = this.r, c = "mesh") {
            const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this._store;
            const rL = (cache[r] ??= Object.create(null));
            const mL = (rL[m] ??= Object.create(null));
            return mL;
        }
        public getR(r: S = this.r, c = "mesh") {
            const cache = this.cache === 'globalThis' || c === 'globalThis' ? (globalThis as A) : this._store;
            const rL = (cache[r] ??= Object.create(null));
            return rL;
        }
    })(this);
}
// #endregion
//                                                                                                                         ATOM
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Slot {
	v: any = undefined;
	s: Listener[] = EMPTY;
	has = false;
	p = false; // persisted
	pin = 0; // held by signal()/atom() handles
	readonly k: S;
	constructor(k: S) {
		this.k = k;
	}
}
export class Atom<T> {
	private readonly _s: SIGNALS["store"];
	private readonly _e: Slot;
	constructor(s: SIGNALS["store"], e: Slot) {
		this._s = s;
		this._e = e;
	}
	get key(): string {return this._e.k; }
	get value(): T {return this._e.v;}
	set value(v: T) {this._s._w(this._e, v, true); }
	get current(): T {	return this._e.v; }
	set current(v: T) { this._s._w(this._e, v, true); }
	get crt(): T { return this._e.v; }
	set crt(v: T) { this._s._w(this._e, v, true); }
	get set(): T { return this._e.v; }
	set set(v: T) { this._s._w(this._e, v, true); }
	peek(): T { return this._e.v; }
	get(): T { return this._e.v; }
	update(fn: (prev: T) => T): T {
		const n = fn(this._e.v);
		this._s._w(this._e, n, true);
		return n;
	}
	sub(fn: Listener<T>, immediate = false): Unsub { return this._s.sub(this._e.k, fn, immediate); }
	release(): void { this._s._unpin(this._e);	}
}
function bcTransport(name: string): Transport | null {
    const BC = (globalThis as any).BroadcastChannel as typeof BroadcastChannel | undefined;
    if (!BC) return null;
    const c = new BC(name);
    return {
        post: (m) => c.postMessage(m),
        listen: (cb) => {
            c.onmessage = (ev: MessageEvent) => cb(ev.data as Msg);
        },
        close: () => c.close(),
    };
}
// #endregion


