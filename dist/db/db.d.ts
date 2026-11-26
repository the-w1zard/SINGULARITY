import { PendingWorkerRequest, StoredEntry, type Durability, type FastKVOptions } from "./types";
export declare const rv: <T>(v: UF<T>) => T;
export declare const sameValue: (a: A, b: A) => B;
export declare class A2<T = A> extends Array<T> implements A2<T> {
    constructor(...items: T[]);
    static get [Symbol.species](): ArrayConstructor;
    static from<T>(arrayLike: ArrayLike<T> | Iterable<T>): A2<T>;
    static of<T>(...items: T[]): A2<T>;
    first(): T | UD;
    last(): T | UD;
    isEmpty(): B;
    isNotEmpty(): B;
    clear(): this;
    clone(): A2<T>;
    unique(): A2<T>;
    compact(): A2<NonNullable<T>>;
    take(count: N): A2<T>;
    takeLast(count: N): A2<T>;
    drop(count: N): A2<T>;
    dropLast(count: N): A2<T>;
    chunk(size: N): A2<A2<T>>;
    flatten<X>(depth?: N): A2<X>;
    flatMap<U>(callback: (value: T, index: N, array: this) => U | readonly U[]): A2<U>;
    distinctBy<K>(callback: (value: T, index: N) => K): A2<T>;
    groupBy<K extends PropertyKey>(callback: (value: T, index: N) => K): Map<K, A2<T>>;
    partition(callback: (value: T, index: N) => B): [A2<T>, A2<T>];
    insert(index: N, ...items: T[]): this;
    removeAt(index: N): T | UD;
    remove(value: T): B;
    removeAll(value: T): N;
    replaceAt(index: N, value: T): T | UD;
    move(from: N, to: N): this;
    swap(first: N, second: N): this;
    sample(): T | UD;
    shuffle(): this;
    countBy(callback: (value: T, index: N) => B): N;
    sum(callback?: (value: T, index: N) => N): N;
    average(callback?: (value: T, index: N) => N): N;
    min(callback?: (value: T, index: N) => N): T | UD;
    max(callback?: (value: T, index: N) => N): T | UD;
    equals(array: readonly T[]): B;
    includesAll(values: readonly T[]): B;
    intersects(values: readonly T[]): B;
}
export declare class WAL_KVDB {
    #private;
    r: S;
    m: S;
    s: S;
    k: S;
    hot_cache: Record<string, unknown>;
    waiters: Record<S, A>;
    entries: Record<S, StoredEntry>;
    pending: Record<S, PendingWorkerRequest>;
    dirtyPages: Record<N, Uint8Array>;
    private _onError;
    constructor(k?: S, s?: S, m?: S, r?: S, options?: Partial<FastKVOptions>);
    get: {
        <T = any>(k: S, s?: S, m?: S, r?: S): any;
        many(keys: S[]): P<V>;
        hot<T = any>(k: S): unknown;
    };
    hot<T = A>(k: S): unknown;
    set: {
        (k: S, value: A, s?: S, m?: S, r?: S, options?: {
            durable: boolean;
            silent?: boolean;
            origin?: string;
        }): P<V>;
        many(entries: Record<S, A>): P<V>;
    };
    delete: {
        (key: S, options?: {
            durable?: B;
        }): P<B>;
        many(keys: S[]): P<V>;
    };
    update: {
        <T = any>(key: S, updater: (prev: T | UD) => T): Promise<T>;
        many(): void;
    };
    has(key: S): B;
    send: {
        (): void;
        many(): void;
    };
    subscribe<T = A>(key: S, cb: Subscriber<T>): Teardown; /** Selector-based subscription — only fires when the selected slice changes. */
    select<T = A, S = A>(key: A, selector: Selector<T | UD, S>, cb: Subscriber<S>, chain?: CHAIN): Teardown;
    private _emit;
    private _peek;
    waitFor<T>(key: S): Promise<T>;
    open(): P<V>;
    flush(): P<V>;
    checkpoint(): P<V>;
    compact(): P<V>;
    close(): P<V>;
    get stats(): {
        keys: StoredEntry;
        pages: number;
        freePages: number;
        dirtyPages: number;
        walBytes: number;
        checkpointSequence: bigint;
        nextSequence: bigint;
        durability: Durability;
    };
}
/**
    const watchHandlers = new Map<S, Map<S, (value: A) => V>>();
        
case "watch": {
  const watchKey = k!;
  const handler = (val: any) => {
    stream.send({
      key: watchKey,
      value: val !== undefined ? JSON.stringify(val) : null,
      event: "watch",
    });
  };

  if (!watchHandlers.has("watch")) {
    watchHandlers.set("watch", new Map());
  }
  watchHandlers.get("watch")!.set(watchKey, handler);

  await db.watch(watchKey, handler);
  result = { subscribed: true, key: watchKey };
  break;
}
case "unwatch": {
  const watchKey = k!;
  const handlers = watchHandlers.get("watch");
  if (handlers && handlers.has(watchKey)) {
    handlers.delete(watchKey);
    result = { unsubscribed: true, key: watchKey };
  } else {
    result = { unsubscribed: false, error: "No such watch" };
  }
  break;
} */
//# sourceMappingURL=db.d.ts.map