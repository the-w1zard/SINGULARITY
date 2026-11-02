//                                                                                             [IMPORTS]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
import type { ATOM_KEY, ATOM, OWNERSHIP, ATOM_TYPE, ROOT, ATOM_CLASS, CHANGE, LISTENER, UNSUBSCRIBE, CONSTITUENT_ROLE, ELECTRON_SHELL, SLOT } from "../contract_factory/CONTROL_ROOM";
// #endregion
//                                                                                               [TYPES]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export type MODULE_VALUE = Function   // anything non-serializable lives here
type MDV =MODULE_VALUE
interface MODULE_SLOT {
	value: MODULE_VALUE;
	registeredAt: number;
	owner: OWNERSHIP; // "bespoke" | "shared"
	/** Optional hook fired right before the slot is replaced or removed. */
	onUnload?: (value: MODULE_VALUE) => void;
}

// #endregion
//                                                                                         [MODULE_ATOM]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class MODULE_ATOM<T=any> {
    public initial: any;
    public key: any;
    public module: any;
    public module_type =  'class_fn_pool'
    public version = 0;
    public sealed = false;
    public frozen = true;
    public hasWritten = false;
    public persisted = false;

    
    public r: S = THE_WIZARD_ROOT_REG
    public m: S = SAURON_MASTER_REG
    public s: S = '__DISRUPTOR__'
    public k: A = '__MODULE_ATOM__'

    private listeners = new Set<(change: Change<T>) => void>();
private waiters = new Set<(value: T) => void>();
public undoStack?: HistoryEntry<T>[];

    public conflict?: ConflictPolicy;
    public historyIndex = 0;
    public history?: AtomHistoryEntry<T>[];
    public historyMax = 50;
    public lifecycle?: SignalLifecycle;
    public async?: AsyncSignalState<T>;
    public metrics?: Metrics;
    public conflictStrategy?: "latest" | "fail" | "accumulate";

    public schema?: SignalSchema;
    public contracts = new Map<string, AtomContract<T>>();
    public causal: CausalEntry[] = [];
    public lastWriteSource?: string;

    public batches = new Set<SignalKey>();
    public batchQueue = new Map<SignalKey, unknown>();
    public batchOpts = new Map<SignalKey, { triggerRender?: boolean }>();

    public broadcast = true
    public triggerRender = true
    public boundary: Boundary = 'same-app'
   constructor(
    initial: T , 
    k: A= THE_SINGULARITY_TEST, 
    s: S = THE_SINGULARITY_SL_CH, 
    m: S =  this.m, 
    r: S = root.JEDI.r ,
     options: MODULE_OPTIONS = { module_type: 'class_fn_pool', broadcast: true, frozen: true, }
    ) {
        this.key = k
        this.s = s 
        this.m = m 
        this.r = r 
const ptype = options?.module_type ? options?.module_type: this.module_type
const shouldFreeze = options.frozen ?? this.frozen;
        switch (ptype) {
            case 'fn': 
 this.frozen = shouldFreeze
        this.module =initial;
        this.initial = initial;
            break;
            case 'comp': 
 this.frozen = shouldFreeze
        this.module =initial;
        this.initial = initial;
            break;
            case 'node_module': 
 this.frozen = shouldFreeze
        this.module =initial;
        this.initial = initial;
            break;
            case 'class_fn_pool':
                default: 
 this.frozen = false 
        this.module = { ...initial };
        this.initial = { ...initial };
            break;
        }
this.module_type = ptype
        if (options?.broadcast) this.broadcast = options.broadcast;
        if (options?.module_type) this.module_type = options.module_type;
        if (options?.schema) this.schema = options.schema;
        if (options?.lifecycle) this.lifecycle = options.lifecycle;
        if (options?.conflict) this.conflict = options.conflict;
        if (options?.conflictStrategy) this.conflictStrategy = options.conflictStrategy;
        if (options?.history) {
            this.historyMax = Math.max(1, options.history.max ?? 50);
            this.history = [{
                 k: 0,
                 value: initial,
                 timestamp: Date.now(), 
                 source: "initial",
                 // next?:A;
                 // prev?: any;
                 // undone?:any;
            }];
            this.historyIndex = 0;
        }
    } 
    public read(): T {        return this.module;    }
    public peek(): T {        return this.module;    }
	public write(incoming: any | ((prev: T) => T), options: AtomWriteOptions = {}):T {
		// Merge-write: {...prev, ...incoming}. Incoming wins collisions.
		if (this.frozen) return this.peek();
		const prev  = this.module;
		const resolved =   incoming;
		// Shallow merge — spread incoming OVER prev, never replace.
		const next = this.module_type === "class_fn_pool"
    ? resolved
        ? { ...prev, ...resolved }
        : prev
    : resolved;
		this.module = next;
		this.hasWritten = true;
		this.version += 1;
		this.lastWriteSource = options.source ?? "write";
		this.assertContracts(next, prev);
		// EAGER NOTIFY — every listener, every write, always.
		for (const l of this.listeners) {
			try {
				l(next);
			} catch (e) {}
		}
		// EAGER BUS BROADCAST — the whole integration is this one call.
		// Swap emit → broadcast if you want channel fan-out instead.
		root.JEDI.send?.emit?.(this.key, next, this.s, this.m, this.r);
		// Release anyone blocked on waitFor / waitForSlot.
		for (const w of this.waiters) {
			try {
				w(next);
			} catch (e) {}
		}
		if (options.recordHistory !== false) {
			this.recordHistory(prev, next, this.lastWriteSource);
		}
		this.recordCausal(next, this.lastWriteSource, options.cause);
		this.waiters.clear();
		this.notifyListeners({ key: this.key, previous: prev, current: next, version: this.version, timestamp: Date.now(), source: this.lastWriteSource });
		return next;
	}
public register(name: string, val: any): T {
		return this.write((prev:A) => ({ ...prev, [name]: val }));
	}
	public unregister(name: string): T {
		return this.write((prev:A) => {
			const { [name]: _gone, ...rest } = prev;
			return rest;
		});
	}
	public slot(name: string): any {
		return this.module[name];
	}
	public on(listener: RiftListener<any>): void {
		this.listeners.add(listener);
		listener(this.module);
	}
	public waitFor(): Promise<T> {
		return new Promise((resolve) => {
			this.waiters.add(resolve);
		});
	}// Await the next write.
		public async waitForSlot(name: string, timeoutMs = 0): Promise<any> {
		// Await the arrival of a slot that hasn't been stood up yet.
		const existing = this.module[name];
		if (existing !== undefined) return existing;

		if (!timeoutMs) {
			const next = await this.waitFor();
			return (next as Record<string, any>)[name];
		}

		return Promise.race([this.waitFor().then((next) => (next as Record<string, any>)[name]), new Promise((_, reject) => setTimeout(() => reject(new Error(`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}m`)), timeoutMs))]);
	}
	public inspect(): T {
		return { ...this.module };
	}// Whole-container dump, on request, for reference only.
    	public setSchema(schema?: SignalSchema) {
		this.schema = schema;
		return this;
	}
	public setLifecycle(lifecycle?: SignalLifecycle<T>) {
		this.lifecycle = lifecycle;
		return this;
	}
	public setConflict(conflict?: ConflictPolicy, strategy?: "latest" | "fail" | "accumulate") {
		this.conflict = conflict;
		this.conflictStrategy = strategy;
		return this;
	}
	public setFrozen(): void {
		this.frozen = true;
	}
	public setUnfrozen(): void {
		this.frozen = false;
	}
	public setSealed(): void {
		this.sealed = true;
	}
	public setUnsealed(): void {
		this.sealed = false;
	}
	public setBoundary() {}
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
				console.error(`[Sauron] contract "${contract.name}" threw for "${String(this.key)}":`, error);
			}

			if (valid) continue;

			const message = `[Sauron] contract "${contract.name}" violated ` + `on write to "${String(this.key)}"`;

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
	public getHistory(): readonly AtomHistoryEntry<T>[] {
		return this.history ?? [];
	}
	public timeTravel(steps = 1): T | undefined {
		if (!this.history?.length || steps <= 0) return undefined;
		const idx = Math.max(0, Math.min(this.history.length - 1, this.historyIndex + steps));
		if (idx === this.historyIndex) return undefined;
		this.historyIndex = idx;
		return this.replayHistory("time-travel");
	}
	public back(steps = 1): T | undefined {
		return this.timeTravel(-Math.abs(steps));
	}
	public forward(steps = 1): T | undefined {
		return this.timeTravel(Math.abs(steps));
	}
	public goto(sequence: number): T | undefined {
		if (!this.history?.length) return undefined;
		const idx = this.history.findIndex((entry) => entry.k === sequence);
		if (idx < 0 || idx === this.historyIndex) return undefined;
		this.historyIndex = idx;
		return this.replayHistory("time-travel.goto");
	}
	private replayHistory(source: string): T {
    const entry = this.history![this.historyIndex];
    return this.write(entry.value as T, {
        source,
        cause: "time-travel",
        triggerRender: true,
        recordHistory: false,
    });
}
	private recordCausal(value: T, source?: string, cause?: string) {
		this.causal.push({ value, source, cause, timestamp: Date.now() } as CausalEntry);
		if (this.causal.length > 100) {
			this.causal.shift();
		}
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
				console.error(`[Sauron:SIGNAL_CELL:${String(this.key)}] listener failed:`, error);
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
				console.error(`[Sauron:SIGNAL_CELL:${String(this.key)}] waiter failed:`, error);
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
				console.error(`[Sauron:SIGNAL_CELL:${String(this.key)}] external subscriber failed:`, error);
			}
		}
	}
}
// #endregion
//                                                                               [MODULE_STORAGE_PROTON]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class MODULE_STORAGE_PROTON {
/*  ===================================================================================================================================
	 Module storage is for ANYTHING THAT CANNOT BE SERIALIZED OR ENCODED:
	 - components
	 - functions
	 - classes
	 - modules
	 - live objects
	 - closures
	 - refs.
	 It is a bare-metal registry keyed by the 3-tier channel address — NOT a signal, NOT serializable, never enters persistence / 
	 snapshots / time travel.
	============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [MODULE_STORAGE_PROTON] */
	public root_cell_key: ATOM_KEY;
	// addr → slot. addr is `[r][m][s][name]`, same convention as S_CLASS_STORAGE_PROTON.
	public r: S
	public m: S
	public c: S
	public s: S
	public k: S
	private slots = new Map<S, MODULE_SLOT>();
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, U> = {}) {
		this.root_cell_key = root_cell_key;
	}
	private addr(k: S = this.k, s: S = this.s, c?: CACHE, m: S = this.m, r: S = this.r): S {
		return c !== undefined ? `[${r}][${m}][${c}][${s}][${k}]` : `[${r}][${m}][${s}][${k}]`;
	}
	/** Register a module / component / class / fn under a named slot at a 3-tier address. */
	public set(value: MDV, k: S, s: S, c: S, m: S, r: S, options: { owner?: OWNERSHIP; onUnload?: (v: MDV) => V } = {},	): MDV {
		const key = this.addr(k, s, c, m, r);
		const prev = this.slots.get(key);
		prev?.onUnload?.(prev.value); // clean unload of the old tenant, if it declared one

		this.slots.set(key, {
			value,
			registeredAt: Date.now(),
			owner: options.owner ?? "bespoke",
			onUnload: options.onUnload,
		});
		return value;
	}

	/** Read a module. Non-reactive by design — modules are not signals. */
	public get(k: S, s: S, m: S, r: S): MODULE_VALUE | undefined {
		return this.slots.get(this.addr(k, s, m, r))?.value;
	}

	public has(k: S, s: S, m: S, r: S): B {
		return this.slots.has(this.addr(k, s, m, r));
	}

	/** Remove one slot. Runs its onUnload. Shared slots are refused unless force is passed. */
	public drop(k: S, s: S, m: S, r: S, force: B = false): B {
		const key = this.addr(k, s, m, r);
		const slot = this.slots.get(key);
		if (!slot) return false;
		if (slot.owner === "shared" && !force) return false;
		slot.onUnload?.(slot.value);
		this.slots.delete(key);
		return true;
	}

	/** List every slot under one (r, m, s) scope — modules living in this workspace/category. */
	public list(s: S, m: S, r: S): Array<{ k: S; owner: OWNERSHIP; registeredAt: N }> {
		const prefix = `[${r}][${m}][${s}][`;
		const out: Array<{ k: S; owner: OWNERSHIP; registeredAt: N }> = [];
		for (const [key, slot] of this.slots) {
			if (key.startsWith(prefix)) out.push({ k: key.slice(0, -1).slice(prefix.length), owner: slot.owner, registeredAt: slot.registeredAt });
		}
		return out;
	}

	/** Reference dump, on request, for diagnostics only. */
	public inspect(): Array<{ address: S; owner: OWNERSHIP; kind: S }> {
		return [...this.slots.entries()].map(([address, s]) => ({
			address,
			owner: s.owner,
			kind: typeof s.value === "function" ? (s.value.name || "anonymous-fn") : "object",
		}));
	}

	purge(): void {
		for (const slot of this.slots.values()) slot.onUnload?.(slot.value);
		this.slots.clear();
	}
}
// #endregion
