/* sauron-signals-fast.ts
 *
 * Maximum-throughput reactive signals engine.
 * Preserves the four-tier channel format: r -> m -> s -> k.
 *
 * Design:
 * - Push-pull propagation.
 * - Doubly linked dependency/subscriber links.
 * - Stable monomorphic node shapes.
 * - No Map/Set in reactive hot paths.
 * - Reusable effect queue.
 * - Direct DOM binding writes.
 */

'use strict';

/* ---------------------------------------------------------------------- *
 * Bitflags
 * ---------------------------------------------------------------------- */

const NONE = 0;
const WATCHING = 1;
const DIRTY = 2;
const PENDING = 4;
const RECURSED = 8;
const RECURSED_CHECK = 16;
const MUTABLE = 32;
const HAS_CHILD_EFFECT = 64;

const RUNNING = WATCHING | RECURSED_CHECK;

/* ---------------------------------------------------------------------- *
 * Core node shapes
 * ---------------------------------------------------------------------- */

interface Link {
    version: number;
    dep: ReactiveNode;
    sub: ReactiveNode;
    prevDep: Link | undefined;
    nextDep: Link | undefined;
    prevSub: Link | undefined;
    nextSub: Link | undefined;
}

interface ReactiveNode {
    flags: number;
    deps: Link | undefined;
    depsTail: Link | undefined;
    subs: Link | undefined;
    subsTail: Link | undefined;
    kind: A
}

interface SignalNode<T = unknown> extends ReactiveNode {
    kind: 0;
    value: T;
    pendingValue: T;
    node?: SignalNode<T>;
    peek?(): T;
}
export interface Signal<T = unknown> {
    (): T;
    (value: T): void;
    readonly node: SignalNode<T>;
    peek(): T;
}
interface ComputedNode<T = unknown> extends ReactiveNode {
    kind: 1;
    value: T | undefined;
    getter: (previous: T | undefined) => T;
}

interface EffectNode extends ReactiveNode {
    kind: 2;
    fn: () => unknown | (() => unknown);
    cleanup: (() => unknown) | undefined;
}

interface ScopeNode extends ReactiveNode {
    kind: 3;
}

type AnyNode = SignalNode | ComputedNode | EffectNode | ScopeNode;

/* ---------------------------------------------------------------------- *
 * Engine state
 * ---------------------------------------------------------------------- */

let activeSub: ReactiveNode | undefined;
let runDepth = 0;
let batchDepth = 0;
let cycle = 0;

let queue: (EffectNode | undefined)[] = new Array(1024);
let queueLength = 0;
let queueIndex = 0;

/* ---------------------------------------------------------------------- *
 * Fast equality
 * ---------------------------------------------------------------------- */
/** NaN-safe equality without calling Object.is on the common path. */
function changed(previous: unknown, next: unknown): boolean {
    return previous !== next && !(previous !== previous && next !== next);
}
/* ---------------------------------------------------------------------- *
 * Link management
 * ---------------------------------------------------------------------- */
function link(dep: ReactiveNode, sub: ReactiveNode, version: number): void {
    const previousDep = sub.depsTail;

    if (previousDep !== undefined && previousDep.dep === dep) return;

    const nextDep = previousDep !== undefined ? previousDep.nextDep : sub.deps;

    if (nextDep !== undefined && nextDep.dep === dep) {
        nextDep.version = version;
        sub.depsTail = nextDep;
        return;
    }

    const previousSub = dep.subsTail;

    if (
        previousSub !== undefined &&
        previousSub.version === version &&
        previousSub.sub === sub
    ) {
        return;
    }

    const newLink: Link = {
        version,
        dep,
        sub,
        prevDep: previousDep,
        nextDep,
        prevSub: previousSub,
        nextSub: undefined,
    };

    sub.depsTail = newLink;
    dep.subsTail = newLink;

    if (nextDep !== undefined) nextDep.prevDep = newLink;

    if (previousDep !== undefined) previousDep.nextDep = newLink;
    else sub.deps = newLink;

    if (previousSub !== undefined) previousSub.nextSub = newLink;
    else dep.subs = newLink;
}
function unlink(linkNode: Link, sub: ReactiveNode = linkNode.sub): Link | undefined {
    const dep = linkNode.dep;
    const previousDep = linkNode.prevDep;
    const nextDep = linkNode.nextDep;
    const previousSub = linkNode.prevSub;
    const nextSub = linkNode.nextSub;

    if (nextDep !== undefined) nextDep.prevDep = previousDep;
    else sub.depsTail = previousDep;

    if (previousDep !== undefined) previousDep.nextDep = nextDep;
    else sub.deps = nextDep;

    if (nextSub !== undefined) nextSub.prevSub = previousSub;
    else dep.subsTail = previousSub;

    if (previousSub !== undefined) previousSub.nextSub = nextSub;
    else dep.subs = nextSub;

    return nextDep;
}
function disposeDependenciesReverse(sub: ReactiveNode): void {
    let linkNode = sub.depsTail;

    while (linkNode !== undefined) {
        const previous = linkNode.prevDep;
        unlink(linkNode, sub);
        linkNode = previous;
    }
}
function purgeDependencies(sub: ReactiveNode): void {
    const tail = sub.depsTail;
    let linkNode = tail !== undefined ? tail.nextDep : sub.deps;

    while (linkNode !== undefined) {
        linkNode = unlink(linkNode, sub);
    }
}
/* ---------------------------------------------------------------------- *
 * Propagation
 * ---------------------------------------------------------------------- */
interface LinkStack {
    value: Link | undefined;
    prev: LinkStack | undefined;
}
function propagate(startLink: Link, innerWrite: boolean): void {
    let current: Link | undefined = startLink;
    let next: Link | undefined = startLink.nextSub;
    let stack: LinkStack | undefined;

    top: do {
        const linkNode: Link = current!;
        const sub = linkNode.sub;
        let flags = sub.flags;

        if (
            (flags & (RECURSED_CHECK | RECURSED | DIRTY | PENDING)) === NONE
        ) {
            sub.flags = flags | PENDING;

            if (innerWrite) sub.flags |= RECURSED;
        } else if ((flags & (RECURSED_CHECK | RECURSED)) === NONE) {
            flags = NONE;
        } else if ((flags & RECURSED_CHECK) === NONE) {
            sub.flags = (flags & ~RECURSED) | PENDING;
        } else if (
            (flags & (DIRTY | PENDING)) === NONE &&
            isValidLink(linkNode, sub)
        ) {
            sub.flags = flags | RECURSED | PENDING;
            flags &= MUTABLE;
        } else {
            flags = NONE;
        }

        if ((flags & WATCHING) !== NONE) notifyEffect(sub as EffectNode);

        if ((flags & MUTABLE) !== NONE) {
            const subSubs = sub.subs;

            if (subSubs !== undefined) {
                current = subSubs;

                if (subSubs.nextSub !== undefined) {
                    stack = { value: next, prev: stack };
                    next = subSubs.nextSub;
                }

                continue;
            }
        }

        current = next;

        if (current !== undefined) {
            next = current.nextSub;
            continue;
        }

        while (stack !== undefined) {
            current = stack.value;
            stack = stack.prev;

            if (current !== undefined) {
                next = current.nextSub;
                continue top;
            }
        }

        break;
    } while (true);
}
function shallowPropagate(startLink: Link): void {
    let linkNode: Link | undefined = startLink;

    do {
        const sub = linkNode.sub;
        const flags = sub.flags;

        if ((flags & (PENDING | DIRTY)) === PENDING) {
            sub.flags = flags | DIRTY;

            if ((flags & (WATCHING | RECURSED_CHECK)) === WATCHING) {
                notifyEffect(sub as EffectNode);
            }
        }
    } while ((linkNode = linkNode.nextSub) !== undefined);
}
function isValidLink(checkLink: Link, sub: ReactiveNode): boolean {
    let linkNode = sub.depsTail;

    while (linkNode !== undefined) {
        if (linkNode === checkLink) return true;
        linkNode = linkNode.prevDep;
    }

    return false;
}
function checkDirty(startLink: Link, startSub: ReactiveNode): boolean {
    let linkNode = startLink;
    let sub = startSub;
    let stack: { value: Link; prev: any } | undefined;
    let depth = 0;
    let dirty = false;

    top: do {
        const dep = linkNode.dep;
        const flags = dep.flags;

        if ((sub.flags & DIRTY) !== NONE) {
            dirty = true;
        } else if (
            (flags & (MUTABLE | DIRTY)) === (MUTABLE | DIRTY)
        ) {
            const subs = dep.subs!;

            if (updateNode(dep)) {
                if (subs.nextSub !== undefined) shallowPropagate(subs);
                dirty = true;
            }
        } else if (
            (flags & (MUTABLE | PENDING)) === (MUTABLE | PENDING)
        ) {
            stack = { value: linkNode, prev: stack };
            linkNode = dep.deps!;
            sub = dep;
            depth++;
            continue;
        }

        if (!dirty) {
            const nextDep = linkNode.nextDep;

            if (nextDep !== undefined) {
                linkNode = nextDep;
                continue;
            }
        }

        while (depth-- > 0) {
            linkNode = stack!.value;
            stack = stack!.prev;

            if (dirty) {
                const subs = sub.subs!;

                if (updateNode(sub)) {
                    if (subs.nextSub !== undefined) shallowPropagate(subs);
                    sub = linkNode.sub;
                    continue;
                }

                dirty = false;
            } else {
                sub.flags &= ~PENDING;
            }

            sub = linkNode.sub;
            const nextDep = linkNode.nextDep;

            if (nextDep !== undefined) {
                linkNode = nextDep;
                continue top;
            }
        }

        return dirty && sub.flags !== NONE;
    } while (true);
}
/* ---------------------------------------------------------------------- *
 * Queue
 * ---------------------------------------------------------------------- */
function notifyEffect(effect: EffectNode): void {
    let insertIndex = queueLength;
    let firstInserted = insertIndex;

    do {
        queue[insertIndex++] = effect;
        effect.flags &= ~WATCHING;

        const parentLink = effect.subs;
        effect = parentLink?.sub as EffectNode;

        if (
            effect === undefined ||
            (effect.flags & WATCHING) === NONE
        ) {
            break;
        }
    } while (true);

    queueLength = insertIndex;

    while (firstInserted < --insertIndex) {
        const left = queue[firstInserted];
        queue[firstInserted++] = queue[insertIndex];
        queue[insertIndex] = left;
    }
}
function ensureQueueCapacity(): void {
    if (queueLength < queue.length) return;

    const next = new Array(queue.length * 2);
    for (let i = 0; i < queueLength; i++) next[i] = queue[i];
    queue = next;
}
function flush(): void {
    try {
        while (queueIndex < queueLength) {
            const effect = queue[queueIndex]!;
            queue[queueIndex++] = undefined;
            runEffect(effect);
        }
    } finally {
        while (queueIndex < queueLength) {
            const effect = queue[queueIndex]!;
            queue[queueIndex++] = undefined;
            effect.flags |= WATCHING | RECURSED;
        }

        queueIndex = 0;
        queueLength = 0;
    }
}
/* ---------------------------------------------------------------------- *
 * Updates
 * ---------------------------------------------------------------------- */
function updateNode(node: AnyNode): boolean {
    if (node.kind === 1) return updateComputed(node as ComputedNode);
    if (node.kind === 0) return updateSignal(node as SignalNode);

    (node as ScopeNode).flags = MUTABLE;
    return true;
}
function updateSignal(signal: SignalNode): boolean {
    signal.flags = MUTABLE;

    const previous = signal.value;
    const next = signal.pendingValue;
    signal.value = next;

    return changed(previous, next);
}
function updateComputed(computed: ComputedNode): boolean {
    if ((computed.flags & HAS_CHILD_EFFECT) !== NONE) {
        let linkNode = computed.depsTail;

        while (linkNode !== undefined) {
            const previous = linkNode.prevDep;
            const dep = linkNode.dep;

            if (dep.kind !== 0 && dep.kind !== 1) {
                unlink(linkNode, computed);
            }

            linkNode = previous;
        }
    }

    computed.depsTail = undefined;
    computed.flags = MUTABLE | RECURSED_CHECK;

    const previousSub = activeSub;
    activeSub = computed;

    try {
        cycle++;
        const previousValue = computed.value;
        const nextValue = computed.getter(previousValue);
        computed.value = nextValue;

        return changed(previousValue, nextValue);
    } finally {
        activeSub = previousSub;
        computed.flags &= ~RECURSED_CHECK;
        purgeDependencies(computed);
    }
}
function runEffect(effect: EffectNode): void {
    const flags = effect.flags;

    const shouldRun =
        (flags & DIRTY) !== NONE ||
        (
            (flags & PENDING) !== NONE &&
            checkDirty(effect.deps!, effect)
        );

    if (!shouldRun) {
        if (effect.deps !== undefined) {
            effect.flags = WATCHING | (flags & HAS_CHILD_EFFECT);
        }
        return;
    }

    if ((effect.flags & HAS_CHILD_EFFECT) !== NONE) {
        let linkNode = effect.depsTail;

        while (linkNode !== undefined) {
            const previous = linkNode.prevDep;
            const dep = linkNode.dep;

            if (dep.kind !== 0 && dep.kind !== 1) {
                unlink(linkNode, effect);
            }

            linkNode = previous;
        }
    }

    if (effect.cleanup !== undefined) {
        const cleanup = effect.cleanup;
        effect.cleanup = undefined;

        const previousSub = activeSub;
        activeSub = undefined;

        try {
            cleanup();
        } finally {
            activeSub = previousSub;
        }

        if (effect.flags === NONE) return;
    }

    effect.depsTail = undefined;
    effect.flags = RUNNING;

    const previousSub = activeSub;
    activeSub = effect;

    try {
        cycle++;
        runDepth++;

        const result = effect.fn();
        effect.cleanup = typeof result === 'function' ? result : undefined;
    } finally {
        runDepth--;
        activeSub = previousSub;
        effect.flags &= ~RECURSED_CHECK;
        purgeDependencies(effect);
    }
}
/* ---------------------------------------------------------------------- *
 * Public signal
 * ---------------------------------------------------------------------- */
function signalRead<T>(signal: SignalNode<T>): T {
    if ((signal.flags & DIRTY) !== NONE) {
        if (updateSignal(signal)) {
            const subs = signal.subs;
            if (subs !== undefined) shallowPropagate(subs);
        }
    }

    const subscriber = activeSub;

    if (subscriber !== undefined) {
        link(signal, subscriber, cycle);
    }

    return signal.value;
}
function signalWrite<T>(signal: SignalNode<T>, next: T): void {
    const previous = signal.pendingValue;

    if (!changed(previous, next)) return;

    signal.pendingValue = next;
    signal.flags = MUTABLE | DIRTY;

    const subs = signal.subs;

    if (subs !== undefined) {
        propagate(subs, runDepth > 0);

        if (batchDepth === 0) flush();
    }
}

export function signal<T>(initialValue?: T): Signal<T> {
    const node: SignalNode<T> = {
        kind: 0,
        value: initialValue as T,
        pendingValue: initialValue as T,
        flags: MUTABLE,
        deps: undefined,
        depsTail: undefined,
        subs: undefined,
        subsTail: undefined,
    };

    const readWrite = function (value?: T) {
        if (arguments.length === 0) return signalRead(node);
        signalWrite(node, value as T);
    } as Signal<T>;

    (readWrite as any).node = node;
    readWrite.peek = () => {
        if ((node.flags & DIRTY) !== NONE && updateSignal(node)) {
            const subs = node.subs;
            if (subs !== undefined) shallowPropagate(subs);
        }
        return node.value;
    };

    return readWrite;
}
/* ---------------------------------------------------------------------- *
 * Computed
 * ---------------------------------------------------------------------- */
export interface Computed<T = unknown> {
    (): T;
    readonly node: ComputedNode<T>;
}
export function computed<T>(getter: (previous?: T) => T): Computed<T> {
    const node: ComputedNode<T> = {
        kind: 1,
        value: undefined,
        getter,
        flags: NONE,
        deps: undefined,
        depsTail: undefined,
        subs: undefined,
        subsTail: undefined,
    };

    const read = () => {
        const flags = node.flags;

        if (
            (flags & DIRTY) !== NONE ||
            (
                (flags & PENDING) !== NONE &&
                (
                    checkDirty(node.deps!, node) ||
                    ((node.flags = flags & ~PENDING), false)
                )
            )
        ) {
            if (updateComputed(node)) {
                const subs = node.subs;
                if (subs !== undefined) shallowPropagate(subs);
            }
        } else if (flags === NONE) {
            node.flags = MUTABLE | RECURSED_CHECK;

            const previousSub = activeSub;
            activeSub = node;

            try {
                node.value = getter();
            } finally {
                activeSub = previousSub;
                node.flags &= ~RECURSED_CHECK;
            }
        }

        const subscriber = activeSub;

        if (subscriber !== undefined) {
            link(node, subscriber, cycle);
        }

        return node.value as T;
    };

    (read as any).node = node;

    return read as Computed<T>;
}
/* ---------------------------------------------------------------------- *
 * Effects
 * ---------------------------------------------------------------------- */
export function effect(fn: () => unknown | (() => unknown)): () => void {
    const node: EffectNode = {
        kind: 2,
        fn,
        cleanup: undefined,
        flags: RUNNING,
        deps: undefined,
        depsTail: undefined,
        subs: undefined,
        subsTail: undefined,
    };

    const previousSub = activeSub;

    if (previousSub !== undefined) {
        link(node, previousSub, 0);
        previousSub.flags |= HAS_CHILD_EFFECT;
    }

    try {
        activeSub = node;
        runDepth++;
        const result = fn();
        node.cleanup = typeof result === 'function' ? result : undefined;
    } finally {
        runDepth--;
        activeSub = previousSub;
        node.flags &= ~RECURSED_CHECK;
    }

    return () => {
        if (node.cleanup !== undefined) {
            const cleanup = node.cleanup;
            node.cleanup = undefined;
            cleanup();
        }

        disposeDependenciesReverse(node);
        node.flags = NONE;
    };
}
export function effectScope(fn: () => unknown): () => void {
    const node: ScopeNode = {
        kind: 3,
        flags: MUTABLE,
        deps: undefined,
        depsTail: undefined,
        subs: undefined,
        subsTail: undefined,
    };

    const previousSub = activeSub;

    if (previousSub !== undefined) {
        link(node, previousSub, 0);
        previousSub.flags |= HAS_CHILD_EFFECT;
    }

    try {
        activeSub = node;
        fn();
    } finally {
        activeSub = previousSub;
    }

    return () => {
        node.flags = NONE;
        disposeDependenciesReverse(node);

        const parentLink = node.subs;
        if (parentLink !== undefined) unlink(parentLink);
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

        if (batchDepth === 0) flush();
    }
}
export function startBatch(): void {
    batchDepth++;
}
export function endBatch(): void {
    if (--batchDepth === 0) flush();
}
export function untracked<T>(fn: () => T): T {
    const previousSub = activeSub;
    activeSub = undefined;

    try {
        return fn();
    } finally {
        activeSub = previousSub;
    }
}
/* ---------------------------------------------------------------------- *
 * DOM
 * ---------------------------------------------------------------------- */
export interface DOMBinding<T = unknown> {
    node: Node;
    write: (value: T, node: Node) => void;
    signal: SignalNode<T>;
    link: Link | undefined;
}
export function bindText<T>(signal: Signal<T>, text: Text): () => void {
    const node = (signal as any).node as SignalNode<T>;

    const write = (value: T, target: Node) => {
        (target as Text).data = value == null ? '' : String(value);
    };

    return bind(node, text, write);
}
export function bindAttribute<T>(
    signal: Signal<T>,
    element: Element,
    attribute: string,
): () => void {
    const node = (signal as any).node as SignalNode<T>;

    const write = (value: T, target: Node) => {
        const el = target as Element;

        if (value == null || value === false) {
            el.removeAttribute(attribute);
            return;
        }

        el.setAttribute(attribute, value === true ? '' : String(value));
    };

    return bind(node, element, write);
}
export function bind<T>(
    signal: Signal<T>,
    node: Node,
    write: (value: T, node: Node) => void,
): () => void {
    const signalNode = (signal as any).node as SignalNode<T>;

    const binding: DOMBinding<T> = {
        node,
        write,
        signal: signalNode,
        link: undefined,
    };

    const previousSub = activeSub;
    const watcher: EffectNode = {
        kind: 2,
        fn: () => write(signalNode.value, node),
        cleanup: undefined,
        flags: RUNNING,
        deps: undefined,
        depsTail: undefined,
        subs: undefined,
        subsTail: undefined,
    };

    const previousWatcherSub = activeSub;
    activeSub = watcher;

    try {
        runDepth++;
        link(signalNode, watcher, cycle);
        write(signalNode.value, node);
    } finally {
        runDepth--;
        activeSub = previousWatcherSub;
    }

    activeSub = previousSub;

    return () => {
        disposeDependenciesReverse(watcher);
        watcher.flags = NONE;
    };
}
/* ---------------------------------------------------------------------- *
 * Four-tier channel format: r -> m -> s -> k
 * ---------------------------------------------------------------------- */
export interface ChannelMessage<P = unknown> {
    r: string;
    m: string;
    s: string;
    k: string;
    p: P;
    ts?: number;
}
export interface ChannelHandler<P = unknown, T = unknown> {
    (payload?: P, message?: ChannelMessage<P>): T | Promise<T>;
}
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

export class SIGNAL_BUS {
    /** root -> master -> slave -> key -> channel */
    private _roots: R<S, R<S, R<S, R<S, Channel>>>> =Object.create(null);
    /**
     * Flat hot registry. Populated exclusively through the tiered API,
     * so the four-tier format remains authoritative while hot sends
     * avoid three object lookups.
     */
    private _hot: R<S, Channel> = Object.create(null);

    constructor(
        public readonly r = 'THE_WIZARD_ROOT_REG',
        public readonly m = 'SAURON_MASTER_REG',
        public readonly s = 'SAURON_SLAVE_REG',
    ) {}/** Canonical tiered registration. */
    public sub<P, T>(key: S, handler: ChannelHandler<P, T>, s: S = this.s, m: S = this.m, r: S = this.r, ): () => V {
        const channel = this._channel(key, s, m, r);
        channel.handler = handler;
        this._hot[key] = channel;
        return () => {if (channel.handler === handler) channel.handler = undefined;};
    }/** Canonical tiered send. */
    public send<T, P>(payload: P, key: S, s: S = this.s, m: S = this.m, r: S = this.r,    ): T | UD {
        const channel = this._roots[r]?.[m]?.[s]?.[key];
        const handler = channel?.handler;
        if (handler === undefined) return undefined;
        return handler(payload, {r, m, s, k: key, p: payload}) as T | UD;
    }/** Zero-tier-resolution hot send. */
    public hotSend<T, P>(payload: P, key: S): T | UD {
        const handler = this._hot[key]?.handler;
        if (handler === undefined) return undefined;
        return handler(payload) as T | undefined;
    }/** Canonical tiered stream subscription. */
    public stream(key: S, fn: (message: ChannelMessage) => V, s: S = this.s, m: S = this.m, r: S = this.r, ): () => V {
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
    }/** Hot stream publish: direct linked-list traversal. */
    public hotPublish(message: ChannelMessage): void {
        let linkNode = this._hot[message.k]?.streamHead;
        while (linkNode !== undefined) {
            linkNode.fn(message);
            linkNode = linkNode.next;
        }
    } /** Hot duplex: direct send + stream surface. */
    public duplex(key: string) {
        const bus = this;

        return {
            send: <T, P>(payload: P) => bus.hotSend<T, P>(payload, key),
            sub: <P, T>(handler: ChannelHandler<P, T>) =>
                bus.sub<P, T>(key, handler),
            stream: (fn: (message: ChannelMessage) => void) =>
                bus.stream(key, fn),
        };
    }
    private _channel(key: S, s: S, m: S, r: S, ): Channel {
        const root = (this._roots[r] ??= Object.create(null));
        const master = (root[m] ??= Object.create(null));
        const slave = (master[s] ??= Object.create(null));
        return (slave[key] ??= {
            handler: undefined,
            streamHead: undefined,
            streamTail: undefined,
        });
    }
}
