// electrons/modules.ts

export class RIFTS_ACCESS<T=any> {
    public initial: any;
    public key: any;
    public module: any;
    public module_type =  'class_fn_pool'
    public version = 0;
    public sealed = false;
    public frozen = true;
    public hasWritten = false;
    public persisted = false;

    
    public rt: S = THE_WIZARD_ROOT_REG
    public ms: S = SAURON_MASTER_REG
    public sl: S = '__DISRUPTOR__'
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
    sl: S = THE_SINGULARITY_SL_CH, 
    ms: S =  this.ms, 
    rt: S = root.DISRUPTOR.rt ,
     options: MODULE_OPTIONS = { module_type: 'class_fn_pool', broadcast: true, frozen: true, }
    ) {
        this.key = k
        this.sl = sl 
        this.ms = ms 
        this.rt = rt 
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
        root.DISRUPTOR.send?.emit?.(this.key, next, this.sl, this.ms, this.rt);
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

        return Promise.race([this.waitFor().then((next) => (next as Record<string, any>)[name]), new Promise((_, reject) => setTimeout(() => reject(new Error(`[ModuleAtom] waitForSlot("${name}") timed out after ${timeoutMs}ms`)), timeoutMs))]);
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
