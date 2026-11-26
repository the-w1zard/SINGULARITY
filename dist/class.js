//                                                                                                                      [FLAGS]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const NONE = 0;
const MUTABLE = 1;
const WATCHING = 2;
const DIRTY = 4;
const PENDING = 8;
const QUEUED = 16;
const DISPOSED = 32;
const RUNNING = 64;
const DIRECT = 128;
const COMPUTED = 256;
// #endregion
//                                                                                                               [ENGINE STATE]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
let activeTarget;
let batchDepth = 0;
let cycle = 0;
let isFlushing = false;
let effectHead;
let effectTail;
function sameValue(a, b) {
    return a === b || (a !== a && b !== b) || Object.is(a, b);
}
let _signals_root = {
    id: "THE_WIZARD_SIGNALS_ROOT",
    hooks: Object.create(null),
    unmount: new Set(),
    contextProviders: Object.create(null),
};
let _rootSeq = 0;
const rx = Object.create(null);
const str = (v) => (v == null ? '' : String(v));
// #endregion
//                                                                                                                         [RX]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class SignalRx {
    rxid() {
        return `${_signals_root.id}:rx${++_rootSeq}`;
    }
    constructor() { }
    get(id) { return rx[id].signal.get(); }
    set(id, v) {
        const b = rx[id];
        if (!b)
            return;
        b.signal.set(v);
    }
    makeRx(id, b) {
        const fn = (...args) => {
            if (args.length === 0)
                return this.get(id);
            this.set(id, args[0]);
        };
        const r = fn;
        Object.defineProperties(r, { id: { value: id }, node: { value: b.node }, el: { value: b.el } });
        rx[id] = b;
        b.disposeEffect = effect(() => { b.write(b.signal.get()); });
        r.attach = (parent) => { for (const n of b.owned)
            parent.appendChild(n); return r; };
        r.dispose = () => {
            b.disposeEffect?.();
            delete rx[id];
            _signals_root.unmount.delete(r.dispose);
            for (const n of b.owned)
                n.parentNode?.removeChild(n);
        };
        _signals_root.unmount.add(r.dispose);
        return r;
    }
    ele(initialValue, injectDom = true) {
        const id = this.rxid();
        const el = injectDom ? document.createElement('span') : null;
        el?.setAttribute('data-sid', id);
        return this.makeRx(id, {
            signal: signal(initialValue),
            node: el, el, owned: el ? [el] : [],
            write(v) { if (el)
                el.textContent = str(v); },
        });
    }
    txt(initialValue, node, injectDom = true) {
        const id = this.rxid();
        let text = node ?? null;
        const owned = [];
        if (!text && injectDom)
            owned.push(document.createComment(id), (text = document.createTextNode('')));
        return this.makeRx(id, {
            signal: signal(initialValue),
            node: text, el: text?.parentElement ?? null, owned,
            write(v) { if (text)
                text.data = str(v); },
        });
    }
    atr(initialValue, node, attributeName = 'value', injectDom = true) {
        const id = this.rxid();
        if (node && injectDom && !node.hasAttribute('data-sid'))
            node.setAttribute('data-sid', id);
        const live = node instanceof HTMLInputElement && attributeName === 'value' ? node : null;
        return this.makeRx(id, {
            signal: signal(initialValue),
            node, el: node ?? null, owned: [],
            write(v) {
                if (!node)
                    return;
                if (v == null || v === false)
                    node.removeAttribute(attributeName);
                else
                    node.setAttribute(attributeName, v === true ? '' : str(v));
                if (live && live.value !== str(v) && document.activeElement !== live)
                    live.value = str(v);
            },
        });
    }
    scanRoot(root = document, componentId = _signals_root.id) {
        const out = Object.create(null);
        for (const el of root.querySelectorAll('[data-sid]')) {
            out[el.getAttribute('data-sid')] = el.getAttribute('value') ?? el.textContent;
        }
        const w = document.createTreeWalker(root, NodeFilter.SHOW_COMMENT);
        while (w.nextNode()) {
            const c = w.currentNode;
            if (c.data.startsWith('rx'))
                out[c.data] = c.nextSibling?.textContent ?? null;
        }
        return out;
    }
    unmountAll() {
        for (const dispose of [..._signals_root.unmount])
            dispose();
    }
}
// #endregion
//                                                                                                                     [SIGNAL]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Signal {
    flags = MUTABLE;
    value;
    version = 0;
    targets;
    targetsTail;
    constructor(value) { this.value = value; }
    get() {
        if (activeTarget !== undefined) {
            track(this, activeTarget);
        }
        return this.value;
    }
    peek() { return this.value; }
    set(value) {
        if (Object.is(this.value, value)) {
            return;
        }
        this.value = value;
        this.version = ++cycle;
        const target = this.targets;
        if (target === undefined) {
            return;
        }
        const subscriber = target.target;
        if (target.nextTarget === undefined && batchDepth === 0 && !isFlushing) {
            const subFlags = subscriber.flags;
            if ((subFlags & WATCHING) !== NONE) {
                const effect = subscriber;
                effect.flags |= DIRTY;
                effect.runEffect();
                return;
            }
            if ((subFlags & COMPUTED) !== NONE && (subFlags & (DIRTY | PENDING | DISPOSED)) === NONE && subscriber.targets === undefined) {
                subscriber.flags = subFlags | PENDING;
                return;
            }
        }
        propagate(target);
        if (batchDepth === 0 && !isFlushing && effectHead !== undefined) {
            flushEffects();
        }
    }
    dispose() {
        clearSources(this);
        clearTargets(this);
    }
}
// #endregion
//                                                                                                                   [COMPUTED]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Computed {
    flags = DIRTY | COMPUTED;
    value;
    version = 0;
    compute;
    sources;
    sourcesTail;
    targets;
    targetsTail;
    constructor(compute) {
        this.value = undefined;
        this.compute = compute;
    }
    get1() {
        const flags = this.flags;
        if (flags === COMPUTED) {
            if (activeTarget !== undefined)
                track(this, activeTarget);
            return this.value;
        }
        if ((flags & DIRTY) !== NONE) {
            this.recompute();
        }
        else if ((flags & PENDING) !== NONE) {
            if (checkDirty(this))
                this.recompute();
            else
                this.flags &= ~PENDING;
        }
        if (activeTarget !== undefined)
            track(this, activeTarget);
        return this.value;
    }
    get() {
        if (this.flags !== COMPUTED) {
            this.refresh();
        }
        if (activeTarget !== undefined) {
            track(this, activeTarget);
        }
        return this.value;
    }
    refresh() {
        const flags = this.flags;
        if ((flags & DIRTY) !== NONE) {
            this.recompute();
        }
        else if ((flags & PENDING) !== NONE) {
            if (checkDirty(this)) {
                this.recompute();
            }
            else {
                this.flags &= ~PENDING;
            }
        }
    }
    recompute() {
        const previousValue = this.value;
        this.flags = (this.flags & ~(PENDING | DIRTY)) | RUNNING;
        const previousTarget = activeTarget;
        activeTarget = this;
        const previousTail = this.sourcesTail;
        this.sourcesTail = undefined;
        try {
            this.value = this.compute(previousValue);
        }
        catch (error) {
            this.flags |= DIRTY;
            throw error;
        }
        finally {
            activeTarget = previousTarget;
            if (this.sourcesTail !== previousTail)
                removeStaleSources(this);
            this.flags &= ~RUNNING;
        }
        if (!Object.is(previousValue, this.value)) {
            this.version = ++cycle;
            shallowPropagate(this.targets);
        }
        return this.value;
    }
    dispose() {
        if ((this.flags & DISPOSED) !== NONE) {
            return;
        }
        this.flags |= DISPOSED;
        clearSources(this);
        clearTargets(this);
    }
}
// #endregion
//                                                                                                                     [EFFECT]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Effect {
    flags = WATCHING;
    run;
    cleanup;
    sources;
    sourcesTail;
    nextQueuedEffect;
    constructor(run) { this.run = run; }
    runEffect() {
        if ((this.flags & DIRTY) !== NONE) {
            this.execute();
            return;
        }
        if ((this.flags & PENDING) !== NONE && (this.flags & DIRECT) === NONE && checkDirty(this)) {
            this.execute();
            return;
        }
        this.flags &= ~PENDING;
    }
    execute() {
        if (this.cleanup !== undefined) {
            const cleanup = this.cleanup;
            this.cleanup = undefined;
            const previousTarget = activeTarget;
            activeTarget = undefined;
            try {
                cleanup();
            }
            finally {
                activeTarget = previousTarget;
            }
        }
        this.flags &= ~(PENDING | DIRTY);
        const previousTarget = activeTarget;
        activeTarget = this;
        const previousTail = this.sourcesTail;
        this.sourcesTail = undefined;
        try {
            const result = this.run();
            this.cleanup = typeof result === 'function' ? result : undefined;
        }
        finally {
            activeTarget = previousTarget;
            if (this.sourcesTail !== previousTail) {
                removeStaleSources(this);
            }
        }
    }
    dispose() {
        if ((this.flags & DISPOSED) !== NONE) {
            return;
        }
        this.flags |= DISPOSED;
        if (this.cleanup !== undefined) {
            const cleanup = this.cleanup;
            this.cleanup = undefined;
            const previousTarget = activeTarget;
            activeTarget = undefined;
            cleanup();
            activeTarget = previousTarget;
        }
        clearSources(this);
    }
}
// #endregion
//                                                                                                                   [TRACKING]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
function track(source, target) {
    const tail = target.sourcesTail;
    if (tail !== undefined && tail.source === source) {
        if (tail.version !== source.version) {
            tail.version = source.version;
        }
        return;
    }
    let link = tail !== undefined ? tail.nextSource : target.sources;
    while (link !== undefined) {
        if (link.source === source) {
            if (link.version !== source.version) {
                link.version = source.version;
            }
            target.sourcesTail = link;
            return;
        }
        link = link.nextSource;
    }
    link = target.sources;
    while (link !== tail) {
        if (link.source === source) {
            unlink(link);
            const previousTargetLink = source.targetsTail;
            link.prevSource = tail;
            link.nextSource = tail !== undefined ? tail.nextSource : target.sources;
            link.prevTarget = previousTargetLink;
            link.nextTarget = undefined;
            target.sourcesTail = link;
            if (tail !== undefined) {
                tail.nextSource = link;
            }
            else {
                target.sources = link;
            }
            if (link.nextSource !== undefined) {
                link.nextSource.prevSource = link;
            }
            source.targetsTail = link;
            if (previousTargetLink !== undefined) {
                previousTargetLink.nextTarget = link;
            }
            else {
                source.targets = link;
            }
            if (link.version !== source.version) {
                link.version = source.version;
            }
            return;
        }
        link = link.nextSource;
    }
    const previousTargetLink = source.targetsTail;
    link = {
        source,
        target,
        version: source.version,
        prevSource: tail,
        nextSource: tail !== undefined ? tail.nextSource : target.sources,
        prevTarget: previousTargetLink,
        nextTarget: undefined,
    };
    target.sourcesTail = link;
    if (tail !== undefined) {
        tail.nextSource = link;
    }
    else {
        target.sources = link;
    }
    if (link.nextSource !== undefined) {
        link.nextSource.prevSource = link;
    }
    source.targetsTail = link;
    if (previousTargetLink !== undefined) {
        previousTargetLink.nextTarget = link;
    }
    else {
        source.targets = link;
    }
}
function removeStaleSources(target) {
    const tail = target.sourcesTail;
    let link = tail !== undefined ? tail.nextSource : target.sources;
    while (link !== undefined) {
        const next = link.nextSource;
        unlink(link);
        link = next;
    }
}
function clearSources(target) {
    let link = target.sources;
    while (link !== undefined) {
        const next = link.nextSource;
        unlink(link);
        link = next;
    }
    target.sources = undefined;
    target.sourcesTail = undefined;
}
function clearTargets(target) {
    let link = target.targets;
    while (link !== undefined) {
        const next = link.nextTarget;
        unlink(link);
        link = next;
    }
    target.targets = undefined;
    target.targetsTail = undefined;
}
function unlink(link) {
    const source = link.source;
    const target = link.target;
    const previousSource = link.prevSource;
    const nextSource = link.nextSource;
    const previousTarget = link.prevTarget;
    const nextTarget = link.nextTarget;
    if (nextSource !== undefined) {
        nextSource.prevSource = previousSource;
    }
    else {
        target.sourcesTail = previousSource;
    }
    if (previousSource !== undefined) {
        previousSource.nextSource = nextSource;
    }
    else {
        target.sources = nextSource;
    }
    if (nextTarget !== undefined) {
        nextTarget.prevTarget = previousTarget;
    }
    else {
        source.targetsTail = previousTarget;
    }
    if (previousTarget !== undefined) {
        previousTarget.nextTarget = nextTarget;
    }
    else {
        source.targets = nextTarget;
    }
}
// #endregion
//                                                                                                                [PROPAGATION]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const propagateStack = [];
let propagateStackLength = 0;
function propagate(startLink) {
    let link = startLink;
    propagateStackLength = 0;
    while (true) {
        while (link !== undefined) {
            const target = link.target;
            const flags = target.flags;
            if ((flags & (DIRTY | PENDING | DISPOSED)) !== NONE) {
                link = link.nextTarget;
                continue;
            }
            target.flags = flags | PENDING;
            if ((flags & WATCHING) !== NONE) {
                enqueueEffect(target);
            }
            else if ((flags & COMPUTED) !== NONE) {
                const computedTarget = target;
                if (computedTarget.targets !== undefined) {
                    const next = link.nextTarget;
                    if (next !== undefined) {
                        propagateStack[propagateStackLength++] = next;
                    }
                    link = computedTarget.targets;
                    continue;
                }
            }
            link = link.nextTarget;
        }
        if (propagateStackLength === 0) {
            return;
        }
        link = propagateStack[--propagateStackLength];
    }
}
function shallowPropagate(startLink) {
    let link = startLink;
    while (link !== undefined) {
        const target = link.target;
        const flags = target.flags;
        if ((flags & RUNNING) === NONE && (flags & (DIRTY | PENDING | DISPOSED)) === PENDING) {
            target.flags = flags | DIRTY;
            if ((flags & WATCHING) !== NONE) {
                enqueueEffect(target);
            }
        }
        link = link.nextTarget;
    }
}
const checkDirtyStack = [];
let checkDirtyStackLength = 0;
function checkDirty(target) {
    let link = target.sources;
    checkDirtyStackLength = 0;
    while (true) {
        while (link !== undefined) {
            const source = link.source;
            const sourceFlags = source.flags;
            if ((sourceFlags & DIRTY) !== NONE) {
                return true;
            }
            if (link.version !== source.version) {
                return true;
            }
            if ((sourceFlags & MUTABLE) === NONE && (sourceFlags & PENDING) !== NONE) {
                const next = link.nextSource;
                if (next !== undefined) {
                    checkDirtyStack[checkDirtyStackLength++] = next;
                }
                link = source.sources;
                continue;
            }
            link = link.nextSource;
        }
        if (checkDirtyStackLength === 0) {
            return false;
        }
        link = checkDirtyStack[--checkDirtyStackLength];
    }
}
// #endregion
//                                                                                                               [EFFECT QUEUE] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
function enqueueEffect(effect) {
    if ((effect.flags & QUEUED) !== NONE || (effect.flags & DISPOSED) !== NONE) {
        return;
    }
    effect.flags |= QUEUED;
    if (effectTail === undefined) {
        effectHead = effect;
        effectTail = effect;
        return;
    }
    effectTail.nextQueuedEffect = effect;
    effectTail = effect;
}
function flushEffects() {
    if (effectHead === undefined)
        return;
    if (isFlushing)
        return;
    isFlushing = true;
    let firstError;
    try {
        while (effectHead !== undefined) {
            const head = effectHead;
            effectHead = undefined;
            effectTail = undefined;
            let effect = head;
            while (effect !== undefined) {
                const next = effect.nextQueuedEffect;
                effect.nextQueuedEffect = undefined;
                effect.flags &= ~QUEUED;
                try {
                    effect.runEffect();
                }
                catch (error) {
                    firstError ??= error;
                }
                effect = next;
            }
        }
    }
    finally {
        isFlushing = false;
    }
    // if (firstError !== undefined) { throw firstError; }
}
// #endregion
//                                                                                                                 [PUBLIC API]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function signal(initialValue) { return new Signal(initialValue); }
export function computed(compute) { return new Computed(compute); }
export function effect(fn) {
    const node = new Effect(fn);
    node.flags |= DIRTY;
    node.runEffect();
    return () => node.dispose();
}
export function batch(fn) {
    batchDepth++;
    try {
        return fn();
    }
    finally {
        if (--batchDepth === 0) {
            flushEffects();
        }
    }
}
export function startBatch() { batchDepth++; }
export function endBatch() { if (--batchDepth === 0) {
    flushEffects();
} }
export function untracked(fn) {
    const previousTarget = activeTarget;
    activeTarget = undefined;
    try {
        return fn();
    }
    finally {
        activeTarget = previousTarget;
    }
}
// #endregion 
function track1(source, target) {
    const tail = target.sourcesTail;
    // Fast path: same source as the immediately preceding read.
    if (tail !== undefined && tail.source === source) {
        if (tail.version !== source.version) {
            tail.version = source.version;
        }
        return;
    }
    // Fast path: the next expected source in a repeated read order.
    let link = tail !== undefined ? tail.nextSource : target.sources;
    while (link !== undefined) {
        if (link.source === source) {
            if (link.version !== source.version) {
                link.version = source.version;
            }
            target.sourcesTail = link;
            return;
        }
        link = link.nextSource;
    }
    // The source is already tracked, but appears before the current tail.
    // This happens when the same dependencies are read in the same order
    // on every evaluation, such as C -> B -> A.
    link = target.sources;
    while (link !== tail) {
        if (link.source === source) {
            // Move the existing link to the tail (most-recently-used position).
            unlink(link);
            const previousTargetLink = source.targetsTail;
            link.prevSource = tail;
            link.nextSource =
                tail !== undefined ? tail.nextSource : target.sources;
            link.prevTarget = previousTargetLink;
            link.nextTarget = undefined;
            target.sourcesTail = link;
            if (tail !== undefined) {
                tail.nextSource = link;
            }
            else {
                target.sources = link;
            }
            if (link.nextSource !== undefined) {
                link.nextSource.prevSource = link;
            }
            source.targetsTail = link;
            if (previousTargetLink !== undefined) {
                previousTargetLink.nextTarget = link;
            }
            else {
                source.targets = link;
            }
            if (link.version !== source.version) {
                link.version = source.version;
            }
            return;
        }
        link = link.nextSource;
    }
    // Create a new link.
    const previousTargetLink = source.targetsTail;
    link = {
        source,
        target,
        version: source.version,
        prevSource: tail,
        nextSource: tail !== undefined ? tail.nextSource : target.sources,
        prevTarget: previousTargetLink,
        nextTarget: undefined,
    };
    target.sourcesTail = link;
    if (tail !== undefined) {
        tail.nextSource = link;
    }
    else {
        target.sources = link;
    }
    if (link.nextSource !== undefined) {
        link.nextSource.prevSource = link;
    }
    source.targetsTail = link;
    if (previousTargetLink !== undefined) {
        previousTargetLink.nextTarget = link;
    }
    else {
        source.targets = link;
    }
}
/**
 *
 *
 *   get1() {
    const flags = this.flags;
    if ((flags & (DISPOSED | RUNNING)) !== NONE) {
      if ((flags & DISPOSED) !== NONE) throw new Error('Cannot read a disposed computed.');
      throw new Error('Circular computed dependency.');
    }
    if ((flags & DIRTY) !== NONE) {
      this.recompute();
    } else if ((flags & PENDING) !== NONE) {
      if (checkDirty(this)) this.recompute();
      else this.flags &= ~PENDING;
    }
    if (activeTarget !== undefined) track(this, activeTarget);
    return this.value;
  }
 *   set1(value: T): void {
if (Object.is(this.value, value)) {
  return;
}

this.value = value;
this.version = ++cycle;

const target = this.targets;

if (target === undefined) {
  return;
}

const subscriber = target.target;

if (
  target.nextTarget === undefined &&
  batchDepth === 0 &&
  !isFlushing &&
  (subscriber.flags & WATCHING) !== NONE
) {
  const effect = subscriber as Effect;

  effect.flags |= DIRTY;
  effect.runEffect();

  return;
}

propagate(target);

if (batchDepth === 0 && !isFlushing && effectHead !== undefined) {
  flushEffects();
}
}
   if ((flags & (DISPOSED | RUNNING)) !== NONE) {
if ((flags & DISPOSED) !== NONE) throw new Error('Cannot read a disposed computed.');
throw new Error('Circular computed dependency.');
}
 */
