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
export interface Rx<T> {
    (): T;
    (value: T): void;
    readonly id: string;
    readonly node: Node | null;
    readonly el: Element | null;
    attach(parent: Node): this;
    dispose(): void;
}
export interface ComponentRoot {
    id: string;
    hooks: Record<string, unknown>;
    unmount: Set<() => void>;
    contextProviders: Record<string, unknown>;
}
export declare class SignalRx<X = any> {
    private rxid;
    constructor();
    get<T = X | any>(id: string): T;
    set<T = X | any>(id: string, v: T): void;
    private makeRx;
    ele<T = X | any>(): Rx<T | undefined>;
    ele<T = X | any>(initialValue: T, injectDom?: boolean): Rx<T>;
    txt<T = X | any>(): Rx<T | undefined>;
    txt<T = X | any>(initialValue: T, node?: Node, injectDom?: boolean): Rx<T>;
    atr<T = X | any>(): Rx<T | undefined>;
    atr<T = X | any>(initialValue: T, node?: Element, attributeName?: string, injectDom?: boolean): Rx<T>;
    scanRoot(root?: ParentNode, componentId?: string): Record<string, string>;
    unmountAll(): void;
}
export declare class Signal<T> implements SourceNode {
    flags: number;
    value: T;
    version: number;
    targets: Link | undefined;
    targetsTail: Link | undefined;
    constructor(value: T);
    get(): T;
    peek(): T;
    set(value: T): void;
    dispose(): void;
}
export declare class Computed<T> implements SourceNode, SubscriberNode {
    flags: number;
    value: T;
    version: number;
    compute: (previous: T) => T;
    sources: Link | undefined;
    sourcesTail: Link | undefined;
    targets: Link | undefined;
    targetsTail: Link | undefined;
    constructor(compute: (previous: T | undefined) => T);
    get1(): T;
    get(): T;
    private refresh;
    recompute(): T;
    dispose(): void;
}
export declare class Effect implements SubscriberNode {
    flags: number;
    run: () => unknown | (() => unknown);
    cleanup: (() => unknown) | undefined;
    sources: Link | undefined;
    sourcesTail: Link | undefined;
    nextQueuedEffect: Effect | undefined;
    constructor(run: () => unknown | (() => unknown));
    runEffect(): void;
    execute(): void;
    dispose(): void;
}
export declare function signal<T>(initialValue: T): Signal<T>;
export declare function computed<T>(compute: (previous: T | undefined) => T): Computed<T>;
export declare function effect(fn: () => unknown | (() => unknown)): () => void;
export declare function batch<T>(fn: () => T): T;
export declare function startBatch(): void;
export declare function endBatch(): void;
export declare function untracked<T>(fn: () => T): T;
export {};
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
//# sourceMappingURL=class.d.ts.map