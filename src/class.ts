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
let activeTarget: SubscriberNode | UD;
let batchDepth = 0;
let cycle = 0;
let isFlushing = false;
let effectHead: Effect | UD;
let effectTail: Effect | UD;
// #endregion
//                                                                                                                [GRAPH TYPES]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
interface SourceNode {
  flags: N;
  version: N;

  targets: Link | UD;
  targetsTail: Link | UD;
}
interface SubscriberNode {
  flags: N;

  sources: Link | UD;
  sourcesTail: Link | UD;
}
interface Link {
  source: SourceNode;
  target: SubscriberNode;

  version: N;

  prevSource: Link | UD;
  nextSource: Link | UD;

  prevTarget: Link | UD;
  nextTarget: Link | UD;
}
function sameValue(a: U, b: U): B {
  return a === b || (a !== a && b !== b) || Object.is(a, b);
}
export interface RxInterface<T> {
	(): T;
	(value: T): V;
	readonly id: S;
	readonly node: Node | NU;
	readonly el: Element | NU;
	attach(parent: Node): this;
	dispose(): V;
}
interface Binding<T = U> {
  signal: Signal<T>;
  node: Node | Element | NU;
  el: Node | Element | NU;
  owned: Node[];
  write(v: T): V;
  disposeEffect?: () => V;
}
// #endregion
//                                                                                                 [REACTIVE DOM BINDING LAYER]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface ComponentRoot {
  id: S;
  hooks: R<S, U>;
  unmount: ST<() => V>;
  contextProviders: R<S, U>;
}
let _signals_root: ComponentRoot = {
	id: "THE_WIZARD_SIGNALS_ROOT",
	hooks: Object.create(null) as R<S, A>,
	unmount: new Set(),
	contextProviders: Object.create(null) as R<S, A>,
}; 
let _rootSeq = 0;
const rx: R<S, Binding> = Object.create(null);
const str = (v: U) => (v == null ? '' : String(v));
// #endregion
//                                                                                                                         [RX]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Rx<X = A> {
  private rxid(): S {
    return `${_signals_root.id}:rx${++_rootSeq}`;
  }
  constructor() { }
  public get<T = X | A>(id: S): T { return rx[id]!.signal.get() as T; }
  public set<T = X | A>(id: S, v: T): V {
    const b = rx[id];
    if (!b) return;
    b.signal.set(v);
  }
  private makeRx<T = X | A>(id: S, b: Binding<T>): RxInterface<T> {
    const fn = (...args: [] | [T]) => {
      if (args.length === 0) return this.get<T>(id);
      this.set(id, args[0]);
    };

    const r = fn as unknown as RxInterface<T>;
    Object.defineProperties(r, { id: { value: id }, node: { value: b.node }, el: { value: b.el } });

    rx[id] = b;

    b.disposeEffect = effect(() => { b.write(b.signal.get()); });

    r.attach = (parent) => { for (const n of b.owned) parent.appendChild(n); return r; };
    r.dispose = () => {
      b.disposeEffect?.();
      delete rx[id];
      _signals_root.unmount.delete(r.dispose);
      for (const n of b.owned) n.parentNode?.removeChild(n);
    };

    _signals_root.unmount.add(r.dispose);

    return r;
  }
  public ele<T = X | A>(): RxInterface<T | UD>;
  public ele<T = X | A>(initialValue: T, injectDom?: B): RxInterface<T>;
  public ele<T = X | A>(initialValue?: T, injectDom = true): RxInterface<T | UD> {
    const id = this.rxid();
    const el = injectDom ? document.createElement('span') : null;
    el?.setAttribute('data-sid', id);

    return this.makeRx(id, {
      signal: signal(initialValue),
      node: el, el, owned: el ? [el] : [],
      write(v) { if (el) el.textContent = str(v); },
    });
  }
  public txt<T = X | A>(): RxInterface<T | UD>;
  public txt<T = X | A>(initialValue: T, node?: Node, injectDom?: B): RxInterface<T>;
  public txt<T = X | A>(initialValue?: T, node?: Node, injectDom = true): RxInterface<T | UD> {
    const id = this.rxid();
    let text = (node as Text | null) ?? null;
    const owned: Node[] = [];

    if (!text && injectDom) owned.push(document.createComment(id), (text = document.createTextNode('')));

    return this.makeRx(id, {
      signal: signal(initialValue),
      node: text, el: text?.parentElement ?? null, owned,
      write(v) { if (text) text.data = str(v); },
    });
  }
  public atr<T = X | A>(): RxInterface<T | UD>;
  public atr<T = X | A>(initialValue: T, node?: Element, attributeName?: S, injectDom?: B): RxInterface<T>;
  public atr<T = X | A>(initialValue?: T, node?: Element, attributeName = 'value', injectDom = true): RxInterface<T | UD> {
    const id = this.rxid();
    if (node && injectDom && !node.hasAttribute('data-sid')) node.setAttribute('data-sid', id);
    const live = node instanceof HTMLInputElement && attributeName === 'value' ? node : null;

    return this.makeRx(id, {
      signal: signal(initialValue),
      node, el: node ?? null, owned: [],
      write(v) {
        if (!node) return;
        if (v == null || v === false) node.removeAttribute(attributeName);
        else node.setAttribute(attributeName, v === true ? '' : str(v));
        if (live && live.value !== str(v) && document.activeElement !== live) live.value = str(v);
      },
    });
  }
  public scanRoot(root: ParentNode = document, componentId = _signals_root.id) {
    const out: Record<string, string | null> = Object.create(null);

    for (const el of root.querySelectorAll('[data-sid]')) {
      out[el.getAttribute('data-sid')!] = el.getAttribute('value') ?? el.textContent;
    }

    const w = document.createTreeWalker(root, NodeFilter.SHOW_COMMENT);
    while (w.nextNode()) {
      const c = w.currentNode as Comment;
      if (c.data.startsWith('rx')) out[c.data] = c.nextSibling?.textContent ?? null;
    }

    return out;
  }
  public unmountAll(): void {
    for (const dispose of [..._signals_root.unmount]) dispose();
  }
}
// #endregion
//                                                                                                                     [SIGNAL]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Signal<T> implements SourceNode {
  flags = MUTABLE;
  value: T;
  version = 0;
  targets: Link | UD;
  targetsTail: Link | UD;
  constructor(value: T) { this.value = value;  }
  get(): T {
    if (activeTarget !== undefined) { track(this, activeTarget);  }
    return this.value;
  }
  peek(): T {  return this.value; }
  set(value: T): V {
    if (Object.is(this.value, value)) { return; }
    this.value = value;
    this.version = ++cycle;
    const target = this.targets;
    if (target === undefined) { return; }
    const subscriber = target.target;
    if (target.nextTarget === undefined && batchDepth === 0 && !isFlushing) {
      const subFlags = subscriber.flags;
      if ((subFlags & WATCHING) !== NONE) {
        const effect = subscriber as Effect;
        effect.flags |= DIRTY;
        effect.runEffect();
        return;
      }
      if ((subFlags & COMPUTED) !== NONE &&  (subFlags & (DIRTY | PENDING | DISPOSED)) === NONE &&  (subscriber as Computed<unknown>).targets === undefined ) {
        subscriber.flags = subFlags | PENDING;
        return;
      }
    }
    propagate(target);
    if (batchDepth === 0 && !isFlushing && effectHead !== undefined) { flushEffects(); }
  }
  dispose(): V {
    clearSources(this as unknown as SubscriberNode);
    clearTargets(this);
  }
}
// #endregion
//                                                                                                                   [COMPUTED]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Computed<T> implements SourceNode, SubscriberNode {
  flags = DIRTY | COMPUTED;
  value: T;
  version = 0;
  compute: (previous: T) => T;
  sources: Link | UD;
  sourcesTail: Link | UD;
  targets: Link | UD;
  targetsTail: Link | UD;
  constructor(compute: (previous: T | UD) => T) {
    this.value = undefined as T;
    this.compute = compute as (previous: T) => T;
  }
  get1() {
    const flags = this.flags;
    if (flags === COMPUTED) {
      if (activeTarget !== undefined) track(this, activeTarget);
      return this.value;
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
  get(): T {
    if (this.flags !== COMPUTED) {this.refresh(); }
    if (activeTarget !== undefined) {track(this, activeTarget); }
    return this.value;
  }
  
  private refresh(): V {
    const flags = this.flags;
  
    if ((flags & DIRTY) !== NONE) {
      this.recompute();
    } else if ((flags & PENDING) !== NONE) {
      if (checkDirty(this)) {
        this.recompute();
      } else {
        this.flags &= ~PENDING;
      }
    }
  }
  recompute(): T {
    const previousValue = this.value;
    this.flags = (this.flags & ~(PENDING | DIRTY)) | RUNNING;
    const previousTarget = activeTarget;
    activeTarget = this;
    const previousTail = this.sourcesTail;
    this.sourcesTail = undefined;
    try {
      this.value = this.compute(previousValue);
    } catch (error) {
      this.flags |= DIRTY;
      throw error;
    } finally {
      activeTarget = previousTarget;
      if (this.sourcesTail !== previousTail) removeStaleSources(this);
      this.flags &= ~RUNNING;
    }
    if (!Object.is(previousValue, this.value)) {
      this.version = ++cycle;
      shallowPropagate(this.targets);
    }
    return this.value;
  }
  dispose(): V {
    if ((this.flags & DISPOSED) !== NONE) { return; }
    this.flags |= DISPOSED;
    clearSources(this);
    clearTargets(this);
  }
}
// #endregion
//                                                                                                                     [EFFECT]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export class Effect implements SubscriberNode {
  flags = WATCHING;
  run: () => U | (() => U);
  cleanup: (() => U) | UD;
  sources: Link | UD;
  sourcesTail: Link | UD;
  nextQueuedEffect: Effect | UD;
  constructor(run: () => U | (() => U)) { this.run = run;  }
  runEffect(): V {
    if ((this.flags & DIRTY) !== NONE) { this.execute();  return; }
    if ((this.flags & PENDING) !== NONE && (this.flags & DIRECT) === NONE && checkDirty(this)) { this.execute(); return; }
    this.flags &= ~PENDING;
  }
  execute(): V {
    if (this.cleanup !== undefined) {
      const cleanup = this.cleanup;
      this.cleanup = undefined;
      const previousTarget = activeTarget;
      activeTarget = undefined;
      try { cleanup(); } 
      finally {activeTarget = previousTarget;}
    }
    this.flags &= ~(PENDING | DIRTY);
    const previousTarget = activeTarget;
    activeTarget = this;
    const previousTail = this.sourcesTail;
    this.sourcesTail = undefined;
    try {
      const result = this.run();
      this.cleanup = typeof result === 'function' ? (result as () => U) : undefined;
    } finally {
      activeTarget = previousTarget;
      if (this.sourcesTail !== previousTail) { removeStaleSources(this); }
    }
  }
  dispose(): V {
    if ((this.flags & DISPOSED) !== NONE) { return; }
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

function track(source: SourceNode, target: SubscriberNode,): V {
  const tail = target.sourcesTail;
 if (tail !== undefined && tail.source === source) { if (tail.version !== source.version) { tail.version = source.version; } return; }
  let link = tail !== undefined ? tail.nextSource : target.sources;
  while (link !== undefined) {
    if (link.source === source) {
      if (link.version !== source.version) { link.version = source.version; }
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
      if (tail !== undefined) {tail.nextSource = link;} 
      else {target.sources = link;}
      if (link.nextSource !== undefined) {link.nextSource.prevSource = link;}
      source.targetsTail = link;
      if (previousTargetLink !== undefined) { previousTargetLink.nextTarget = link; } 
      else { source.targets = link;}
      if (link.version !== source.version) {link.version = source.version;}
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
  if (tail !== undefined) {tail.nextSource = link;} 
  else {target.sources = link;}
  if (link.nextSource !== undefined) { link.nextSource.prevSource = link; }
  source.targetsTail = link;
  if (previousTargetLink !== undefined) {previousTargetLink.nextTarget = link; } 
  else { source.targets = link; }
}
function removeStaleSources(target: SubscriberNode): V {
  const tail = target.sourcesTail;
  let link = tail !== undefined ? tail.nextSource : target.sources;
  while (link !== undefined) {
    const next = link.nextSource;
    unlink(link);
    link = next;
  }
}
function clearSources(target: SubscriberNode): V {
  let link = target.sources;
  while (link !== undefined) {
    const next = link.nextSource;
    unlink(link);
    link = next;
  }
  target.sources = undefined;
  target.sourcesTail = undefined;
}
function clearTargets(target: SourceNode): V {
  let link = target.targets;
  while (link !== undefined) {
    const next = link.nextTarget;
    unlink(link);
    link = next;
  }
  target.targets = undefined;
  target.targetsTail = undefined;
}
function unlink(link: Link): V {
  const source = link.source;
  const target = link.target;
  const previousSource = link.prevSource;
  const nextSource = link.nextSource;
  const previousTarget = link.prevTarget;
  const nextTarget = link.nextTarget;
  if (nextSource !== undefined) {nextSource.prevSource = previousSource;} 
  else {target.sourcesTail = previousSource; }
  if (previousSource !== undefined) {previousSource.nextSource = nextSource; } 
  else {target.sources = nextSource;}
  if (nextTarget !== undefined) {nextTarget.prevTarget = previousTarget;} 
  else {source.targetsTail = previousTarget;}
  if (previousTarget !== undefined) {previousTarget.nextTarget = nextTarget; } 
  else {source.targets = nextTarget;}
}
// #endregion
//                                                                                                                [PROPAGATION]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const propagateStack: Link[] = [];
let propagateStackLength = 0;

function propagate(startLink: Link | UD): V {
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
      if ((flags & WATCHING) !== NONE) { enqueueEffect(target as Effect);} 
      else if ((flags & COMPUTED) !== NONE) {
        const computedTarget = target as Computed<unknown>;
        if (computedTarget.targets !== undefined) {
          const next = link.nextTarget;
          if (next !== undefined) {propagateStack[propagateStackLength++] = next; }
          link = computedTarget.targets;
          continue;
        }
      }
      link = link.nextTarget;
    }
    if (propagateStackLength === 0) { return; }
    link = propagateStack[--propagateStackLength];
  }
}
function shallowPropagate(startLink: Link | UD): V {
  let link = startLink;
  while (link !== undefined) {
    const target = link.target;
    const flags = target.flags;
    if ((flags & RUNNING) === NONE && (flags & (DIRTY | PENDING | DISPOSED)) === PENDING ) {
      target.flags = flags | DIRTY;
      if ((flags & WATCHING) !== NONE) { enqueueEffect(target as Effect); }
    }
    link = link.nextTarget;
  }
}
const checkDirtyStack: Link[] = [];
let checkDirtyStackLength = 0;

function checkDirty(target: SubscriberNode): B {
  let link = target.sources;
  checkDirtyStackLength = 0;
  while (true) {
    while (link !== undefined) {
      const source = link.source;
      const sourceFlags = source.flags;
      if ((sourceFlags & DIRTY) !== NONE) { return true; }
      if (link.version !== source.version) { return true; }
      if ((sourceFlags & MUTABLE) === NONE &&  (sourceFlags & PENDING) !== NONE ) {
        const next = link.nextSource;
        if (next !== undefined) { checkDirtyStack[checkDirtyStackLength++] = next; }
        link = (source as Computed<unknown>).sources;
        continue;
      }
      link = link.nextSource;
    }
    if (checkDirtyStackLength === 0) { return false; }
    link = checkDirtyStack[--checkDirtyStackLength];
  }
}
// #endregion
//                                                                                                               [EFFECT QUEUE] 
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
function enqueueEffect(effect: Effect): V {
  if ((effect.flags & QUEUED) !== NONE || (effect.flags & DISPOSED) !== NONE  ) { return; }
  effect.flags |= QUEUED;
  if (effectTail === undefined) {
    effectHead = effect;
    effectTail = effect;
    return;
  }
  effectTail.nextQueuedEffect = effect;
  effectTail = effect;
}
function flushEffects(): V {
    if (effectHead === undefined) return; 
    if (isFlushing) return;
    isFlushing = true;
    let firstError: unknown;
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
          try {  effect.runEffect(); } 
          catch (error) { firstError ??= error; }
          effect = next;
        }
      }
    } finally {
      isFlushing = false;
    }
}
// #endregion
//                                                                                                                 [PUBLIC API]
// #region ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export function signal<T>(initialValue: T): Signal<T> {  return new Signal(initialValue);}
export function computed<T>(compute: (previous: T | UD) => T,): Computed<T> {  return new Computed(compute);}
export function effect( fn: () => unknown | (() => U),): () => V {
  const node = new Effect(fn);
  node.flags |= DIRTY;
  node.runEffect();
  return () => node.dispose();
}
export function batch<T>(fn: () => T): T {
  batchDepth++;
  try { return fn(); } 
  finally { if (--batchDepth === 0) { flushEffects(); }  }
}
export function startBatch(): V {  batchDepth++;}
export function endBatch(): V { if (--batchDepth === 0) { flushEffects(); }}
export function untracked<T>(fn: () => T): T {
  const previousTarget = activeTarget;
  activeTarget = undefined;
  try { return fn(); } 
  finally {  activeTarget = previousTarget; }
}
// #endregion 


function track1(  source: SourceNode,  target: SubscriberNode,): V {
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
      } else {
        target.sources = link;
      }

      if (link.nextSource !== undefined) {
        link.nextSource.prevSource = link;
      }

      source.targetsTail = link;

      if (previousTargetLink !== undefined) {
        previousTargetLink.nextTarget = link;
      } else {
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
  } else {
    target.sources = link;
  }

  if (link.nextSource !== undefined) {
    link.nextSource.prevSource = link;
  }

  source.targetsTail = link;

  if (previousTargetLink !== undefined) {
    previousTargetLink.nextTarget = link;
  } else {
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
