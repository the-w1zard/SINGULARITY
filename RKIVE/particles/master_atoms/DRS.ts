import { ATOM_CLASS, ATOM_KEY, BC, GATE_ELECTRON, REGISTRY_PROTON, SEALED_ERROR_ELECTRON, SUBSCRIPTION_ELECTRON } from "../contract_factory/CONTROL_ROOM";
//                                                                                                  ATOM
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class DRS_ATOM<X = U> {
	public key: ATOM_KEY;
	public value: X;
	public registry: REGISTRY_PROTON | null = null;
	constructor(key: ATOM_KEY, value: X) {
		this.key = key;
		this.value = value;
	} /** Binds a shard. The registry shard is created here, the first time it is needed. */
	bind<S extends ATOM>(shard: S, owner: OWNERSHIP = "bespoke"): S {
		if (this.registry === null) {
			this.registry = new REGISTRY_PROTON(this.key, { shards: [{ shard, owner }] });
		} else {
			this.registry.bind(shard, owner);
		}
		return shard;
	}
	shard<S extends ATOM>(ctor: ATOM_CLASS<S>): S | UD {
		return this.registry?.get(ctor);
	}
	unbind(ctor: SHARD_CLASS<any>): B {
		if (this.registry === null) return false;
		const removed = this.registry.unbind(ctor);
		// A registry with nothing in it is dropped, returning the cell to the bare fast path.
		if (this.registry.size === 0) {
			BC().store.particle(s, m, r).delete(this.key)._REGISTRY_PROTON;
			this.registry = null;
		}
		return removed;
	}/** Deletes the cell's bespoke shards. Shared shards are untouched. */
	destroy(): V {
		this.registry?.purgeAll();
		this.registry = null;
		BC().store.particle(s, m, r).delete(this.key)._REGISTRY_PROTON;
	}
	read(): X {
		const r = this.registry;
		if (r !== null) {
			const gate = r.get(GATE_ELECTRON);
			if (gate !== undefined && (gate as any).sealed) throw new SEALED_ERROR_ELECTRON(this.key);
		}
		return this.value;
	}
	write(next: X): B {
		const r = this.registry;
		if (r === null) {
			this.value = next;
			return true;
		}
		const gate = r.get(GATE_ELECTRON);
		if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
		const previous = this.value;
		if (Object.is(previous, next)) return false;
		this.value = next;
		const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
		if (subs !== undefined) subs.notify({ key: this.key, previous, current: next });
		return true;
	}
	subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
		const subs =
			(this.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
			this.bind(new SUBSCRIPTION_ELECTRON<X>(this));
		return subs.subscribe(listener);
	}
	waitFor() { }
}
export interface ATOM {
	readonly kind: SY;
	purge?(): V;/** Called when the shard is purged (cell destroyed while bespoke, or unbound). */
}
// #endregion
class OLD_DRC_CELL<T = A> {// DISTRIBUTED REACTIVE CELL
    public initial: T;
    public key: SOUL_KEY;
    public node: CURRENT_SOUL_SHARD<T>;// The one canonical value source. Dependency tracking, computed propagation, effects, and batching.

    public globalListeners = new Set<(key: SignalKey, value: unknown) => void>();
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

    public boundary: Boundary = 'same-app'

    constructor(key: SignalKey, initial: T, options?: SignalOptions) {
        this.key = key;
        this.initial = initial;

        this.node = root.SIGNALS.signal(initial) as AlienSignal<T>;

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
        /**
         * Tracking read:
         * Alien records computed/effect dependencies here.
         */
        return this.node();
    }
    public peek(): T {
        /**
         * Non-tracking read if Alien exposes `peek`.
         */
        return this.node.peek?.() ?? this.node();
    }
    public write(incoming: T | ((previous: T) => T), options: AtomWriteOptions = {}): T {
        if (this.frozen) return this.peek();

        const previous = this.peek();
        let next = typeof incoming === "function" ? (incoming as (previous: T) => T)(previous) : incoming;

        next = this.validate(next);

        if (!Object.is(previous, next) && this.conflict) {
            const resolved = root.SIGNALS._resolveConflict(this.conflict, previous, next, this.conflictStrategy,);
            if (resolved === null) return previous;
            next = resolved;
        }

        this.assertContracts(next, previous);
        if (Object.is(previous, next)) return previous;

        this.hasWritten = true;
        this.node(next);
        this.version += 1;
        this.lastWriteSource = options.source ?? "write";

        if (options.recordHistory !== false) { this.recordHistory(previous, next, this.lastWriteSource); }

        this.recordCausal(next, this.lastWriteSource, options.cause);
        this.recordMetrics();
        this.lifecycle?.onWrite?.(next, previous);

        this.notifyListeners({ key: this.key, previous, current: next, version: this.version, timestamp: Date.now(), source: this.lastWriteSource, });

        this.resolveWaiters(next);

        if (options.triggerRender ?? this.triggerRender) {
            this.notifyExternalSubscribers();
        }

        return next;
    }
    public subscribe(listener: (change: Change<T>) => void,): Unsubscribe {
        let listeners = root.TH._listeners.get(this.key);
        if (!listeners) {
            listeners = new Set();
            root.TH._listeners.set(this.key, listeners);
        }
        listeners.add(listener);
        return () => {
            listeners!.delete(listener);

            if (listeners!.size === 0) {
                root.TH._listeners.delete(this.key);
            }
        };
    }
    public subscribeExternal(callback: () => void,): Unsubscribe {
        let _externalSubscribers = root.TH._externalSubscribers.get(this.key);
        if (!_externalSubscribers) {
            _externalSubscribers = new Set();
            root.TH._externalSubscribers.set(this.key, _externalSubscribers);
        }
        _externalSubscribers.add(callback);

        return () => {
            _externalSubscribers!.delete(callback);

            if (_externalSubscribers!.size === 0) {
                root.TH._externalSubscribers.delete(this.key);
            }
        };
    }
    public waitForNext(): Promise<T> {
        return new Promise<T>(resolve => {
            root.TH._waiters.set(this.key, new Set([...(root.TH._waiters.get(this.key) ?? []), resolve]));
        });
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
    public setBoundary() {

    }
    private validate(next: T): T {
        if (!this.schema) {
            return next;
        }

        return root.SIGNALS._validateSchema(this.schema, next);
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
    private notifyListeners(change: Change<T>) {
        const listeners = root.TH._listeners.get(this.key);

        if (!listeners) return;

        for (const listener of listeners) {
            try {
                listener(change);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(this.key)}] listener failed:`,
                    error,
                );
            }
        }

    }
    private resolveWaiters(value: T) {
        const waiting = root.TH._waiters.get(this.key);

        if (!waiting) return;

        root.TH._waiters.delete(this.key);

        for (const resolve of waiting) {
            try {
                resolve(value);
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(this.key)}] waiter failed:`,
                    error,
                );
            }
        }

    }
    private notifyExternalSubscribers() {
        const subscribers = root.TH._externalSubscribers.get(this.key);

        if (!subscribers) return;

        for (const callback of subscribers) {
            try {
                callback();
            } catch (error) {
                console.error(
                    `[Sauron:SIGNAL_CELL:${String(this.key)}] external subscriber failed:`,
                    error,
                );
            }
        }

    }
}
