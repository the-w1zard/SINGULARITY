/* ------------------------------------------------------------------ *
 * Flags
 * ------------------------------------------------------------------ */
const NONE = 0;
const MUTABLE = 1;
const WATCHING = 2;
const DIRTY = 4;
const PENDING = 8;
const QUEUED = 16;
const DISPOSED = 32;
const RUNNING = 64;
const DIRECT = 128;
/* ------------------------------------------------------------------ *
 * Engine state
 * ------------------------------------------------------------------ */
let activeTarget: SubscriberNode | undefined;
let batchDepth = 0;
let cycle = 0;
let isFlushing = false;

let effectHead: Effect | undefined;
let effectTail: Effect | undefined;

/* ------------------------------------------------------------------ *
 * Graph types
 * ------------------------------------------------------------------ */
interface SourceNode {
  flags: number;
  version: number;

  targets: Link | undefined;
  targetsTail: Link | undefined;
}

interface SubscriberNode {
  flags: number;

  sources: Link | undefined;
  sourcesTail: Link | undefined;
}

interface Link {
  source: SourceNode;
  target: SubscriberNode;

  /**
   * The source version observed by target.
   */
  version: number;

  prevSource: Link | undefined;
  nextSource: Link | undefined;

  prevTarget: Link | undefined;
  nextTarget: Link | undefined;
}

/* ------------------------------------------------------------------ *
 * Signal
 * ------------------------------------------------------------------ */
export class Signal<T> implements SourceNode {
  flags = MUTABLE;

  value: T;
  version = 0;

  targets: Link | undefined;
  targetsTail: Link | undefined;

  constructor(value: T) {
    this.value = value;
  }

  get(): T {
    if (activeTarget !== undefined) {
      track(this, activeTarget);
    }

    return this.value;
  }

  peek(): T {
    return this.value;
  }

  set1(value: T): void {
    if (value === this.value) return;
  
    this.value = value;
    this.version = ++cycle;
  
    const target = this.targets;
  
    if (target === undefined) return;
  
    if (
      batchDepth === 0 &&
      !isFlushing &&
      target.nextTarget === undefined &&
      (target.target.flags & WATCHING) !== NONE
    ) {
      // One direct effect: run it synchronously.
      const effect = target.target as Effect;
      effect.flags |= DIRTY;
      effect.runEffect();
      return;
    }
  
    propagate(target);
  
    if (batchDepth === 0 && !isFlushing) {
      flushEffects();
    }
  }
  set(value: T): void {
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
  
    // Fast path: exactly one direct effect subscriber.
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
  
    if (batchDepth === 0 && !isFlushing) {
      flushEffects();
    }
  }
  dispose(): void {
    clearSources(this as unknown as SubscriberNode);
    clearTargets(this);
  }
}

/* ------------------------------------------------------------------ *
 * Computed
 * ------------------------------------------------------------------ */
export class Computed<T> implements SourceNode, SubscriberNode {
  flags = DIRTY;

  value: T;
  version = 0;

  compute: (previous: T) => T;

  sources: Link | undefined;
  sourcesTail: Link | undefined;

  targets: Link | undefined;
  targetsTail: Link | undefined;

  constructor(compute: (previous: T | undefined) => T) {
    this.value = undefined as T;
    this.compute = compute as (previous: T) => T;
  }

  get(): T {
    if ((this.flags & DISPOSED) !== NONE) {
      throw new Error('Cannot read a disposed computed.');
    }
  
    if ((this.flags & RUNNING) !== NONE) {
      throw new Error('Circular computed dependency.');
    }
  
    // freshness resolution
    if ((this.flags & DIRTY) !== NONE) {
      this.recompute();
    } else if (
      (this.flags & PENDING) !== NONE &&
      checkDirty(this)
    ) {
      this.recompute();
    } else {
      this.flags &= ~PENDING;
    }
  
    if (activeTarget !== undefined) {
      track(this, activeTarget);
    }
  
    return this.value;
  }

  recompute(): T {
    const previousValue = this.value;
  
    this.flags = (this.flags & ~PENDING) | RUNNING;
    this.flags &= ~DIRTY;
  
    const previousTarget = activeTarget;
    activeTarget = this;
    const previousTail = this.sourcesTail;

    this.sourcesTail = undefined;
    const currentTail = this.sourcesTail;

    try {
      cycle++;
  
      this.value = this.compute(previousValue);
    } catch (error) {
      // Keep the node dirty so the next read retries.
      this.flags |= DIRTY;
      throw error;
    } finally {
      activeTarget = previousTarget;
      this.flags &= ~RUNNING;
  
      if (currentTail === previousTail) {
        // Same dependency tail: no stale links to remove.
        } else {
        removeStaleSources(this);
        }
    }
  
    if (!Object.is(previousValue, this.value)) {
      this.version = cycle;
  
      if (this.targets !== undefined) {
        shallowPropagate(this.targets);
      }
    }
  
    return this.value;
  }

  dispose(): void {
    if ((this.flags & DISPOSED) !== NONE) {
      return;
    }

    this.flags |= DISPOSED;

    clearSources(this);
    clearTargets(this);
  }
}

/* ------------------------------------------------------------------ *
 * Effect
 * ------------------------------------------------------------------ */
export class Effect implements SubscriberNode {
  flags = WATCHING;

  run: () => unknown | (() => unknown);
  cleanup: (() => unknown) | undefined;

  sources: Link | undefined;
  sourcesTail: Link | undefined;

  nextQueuedEffect: Effect | undefined;

  constructor(run: () => unknown | (() => unknown)) {
    this.run = run;
  }

  runEffect(): void {
    if ((this.flags & DIRTY) !== NONE) {
      this.execute();
      return;
    }
  
    if (
      (this.flags & PENDING) !== NONE &&
      (this.flags & DIRECT) === NONE &&
      checkDirty(this)
    ) {
      this.execute();
      return;
    }
  
    this.flags &= ~PENDING;
  }

  execute(): void {
    if (this.cleanup !== undefined) {
      const cleanup = this.cleanup;
      this.cleanup = undefined;
  
      const previousTarget = activeTarget;
      activeTarget = undefined;
  
      try {
        cleanup();
      } finally {
        activeTarget = previousTarget;
      }
    }
  
    this.flags &= ~(PENDING | DIRTY);
  
    const previousTarget = activeTarget;
    activeTarget = this;
  
    this.sourcesTail = undefined;
  
    try {
      cycle++;
  
      const result = this.run();
  
      this.cleanup =
        typeof result === 'function'
          ? (result as () => unknown)
          : undefined;
    } finally {
      activeTarget = previousTarget;
  
      removeStaleSources(this);
    }
  }

  dispose(): void {
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

/* ------------------------------------------------------------------ *
 * Tracking
 * ------------------------------------------------------------------ */
function track(
  source: SourceNode,
  target: SubscriberNode,
): void {
  const tail = target.sourcesTail;

  // Same source as the immediately preceding read.
  if (tail !== undefined && tail.source === source) {
    tail.version = source.version;
    return;
  }

  // Reuse an existing link after the retained tail.
  let link = tail !== undefined ? tail.nextSource : target.sources;

  while (link !== undefined) {
    if (link.source === source) {
      link.version = source.version;
      target.sourcesTail = link;
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

function removeStaleSources(target: SubscriberNode): void {
  const tail = target.sourcesTail;

  let link = tail !== undefined ? tail.nextSource : target.sources;

  while (link !== undefined) {
    const next = link.nextSource;
    unlink(link);
    link = next;
  }
}

function clearSources(target: SubscriberNode): void {
  let link = target.sources;

  while (link !== undefined) {
    const next = link.nextSource;
    unlink(link);
    link = next;
  }

  target.sources = undefined;
  target.sourcesTail = undefined;
}

function clearTargets(target: SourceNode): void {
  let link = target.targets;

  while (link !== undefined) {
    const next = link.nextTarget;
    unlink(link);
    link = next;
  }

  target.targets = undefined;
  target.targetsTail = undefined;
}

function unlink(link: Link): void {
  const source = link.source;
  const target = link.target;

  const previousSource = link.prevSource;
  const nextSource = link.nextSource;

  const previousTarget = link.prevTarget;
  const nextTarget = link.nextTarget;

  if (nextSource !== undefined) {
    nextSource.prevSource = previousSource;
  } else {
    target.sourcesTail = previousSource;
  }

  if (previousSource !== undefined) {
    previousSource.nextSource = nextSource;
  } else {
    target.sources = nextSource;
  }

  if (nextTarget !== undefined) {
    nextTarget.prevTarget = previousTarget;
  } else {
    source.targetsTail = previousTarget;
  }

  if (previousTarget !== undefined) {
    previousTarget.nextTarget = nextTarget;
  } else {
    source.targets = nextTarget;
  }
}

/* ------------------------------------------------------------------ *
 * Propagation
 * ------------------------------------------------------------------ */
interface Frame {
  link: Link;
  previous: Frame | undefined;
}

function propagate(startLink: Link | undefined): void {
    let link = startLink;
    let stack: Frame | undefined;
  
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
          enqueueEffect(target as Effect);
        } else if (target instanceof Computed) {
          const computedTarget = target as Computed<unknown>;
  
          if (computedTarget.targets !== undefined) {
            const next = link.nextTarget;
  
            if (next !== undefined) {
              stack = { link: next, previous: stack };
            }
  
            link = computedTarget.targets;
            continue;
          }
        }
  
        link = link.nextTarget;
      }
  
      if (stack === undefined) {
        return;
      }
  
      link = stack.link;
      stack = stack.previous;
    }
  }

function shallowPropagate(startLink: Link | undefined): void {
  let link = startLink;

  while (link !== undefined) {
    const target = link.target;
    const flags = target.flags;

    if ((flags & (DIRTY | PENDING | DISPOSED)) === PENDING) {
      target.flags = flags | DIRTY;

      if ((flags & WATCHING) !== NONE) {
        enqueueEffect(target as Effect);
      }
    }

    link = link.nextTarget;
  }
}

function checkDirty(target: SubscriberNode): boolean {
    let link = target.sources;
    let stack: Frame | undefined;
  
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
  
        if (
          (sourceFlags & MUTABLE) === NONE &&
          (sourceFlags & PENDING) !== NONE
        ) {
          const next = link.nextSource;
  
          if (next !== undefined) {
            stack = { link: next, previous: stack };
          }
  
          link = (source as Computed<unknown>).sources;
          continue;
        }
  
        link = link.nextSource;
      }
  
      if (stack === undefined) {
        return false;
      }
  
      link = stack.link;
      stack = stack.previous;
    }
  }

function refreshComputed(node: Computed<unknown>): void {
  if ((node.flags & DISPOSED) !== NONE) {
    return;
  }

  if ((node.flags & DIRTY) !== NONE) {
    node.recompute();
    return;
  }

  if (
    (node.flags & PENDING) !== NONE &&
    checkDirty(node)
  ) {
    node.recompute();
    return;
  }

  node.flags &= ~PENDING;
}

/* ------------------------------------------------------------------ *
 * Effect queue
 * ------------------------------------------------------------------ */
function enqueueEffect(effect: Effect): void {
  if (
    (effect.flags & QUEUED) !== NONE ||
    (effect.flags & DISPOSED) !== NONE
  ) {
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

function flushEffects(): void {
    if (isFlushing) {
      return;
    }
  
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
  
          try {
            effect.runEffect();
          } catch (error) {
            firstError ??= error;
          }
  
          effect = next;
        }
      }
    } finally {
      isFlushing = false;
    }
  
    if (firstError !== undefined) {
      throw firstError;
    }
  }

/* ------------------------------------------------------------------ *
 * Public functions
 * ------------------------------------------------------------------ */
export function signal<T>(initialValue: T): Signal<T> {
  return new Signal(initialValue);
}

export function computed<T>(
  compute: (previous: T | undefined) => T,
): Computed<T> {
  return new Computed(compute);
}

export function effect(
  fn: () => unknown | (() => unknown),
): () => void {
  const node = new Effect(fn);

  // Initial run and rerun use the same execution path.
  node.flags |= DIRTY;
  node.runEffect();

  return () => node.dispose();
}

export function batch<T>(fn: () => T): T {
  batchDepth++;

  try {
    return fn();
  } finally {
    if (--batchDepth === 0) {
      flushEffects();
    }
  }
}

export function startBatch(): void {
  batchDepth++;
}

export function endBatch(): void {
  if (--batchDepth === 0) {
    flushEffects();
  }
}

export function untracked<T>(fn: () => T): T {
  const previousTarget = activeTarget;
  activeTarget = undefined;

  try {
    return fn();
  } finally {
    activeTarget = previousTarget;
  }
}
