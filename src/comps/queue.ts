import { useState, useCallback, useEffect } from '#sauron'
//                                                                                                 QUEUE
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
let queuedLength = 0;

interface QueueHandle<T, A = unknown> {
    // ---- Enqueue (the callable part) ----------------------------------
    (task: () => Promise<T>, options?: { priority?: number }): Promise<T | T[]>;

    // ---- Control -------------------------------------------------------
    start(): this;
    stop(): this;
    pause(): this;
    resume(): this;
    clear(): this;

    // ---- Introspection -------------------------------------------------
    readonly pending: number;
    readonly active: number;
    readonly size: number;
    readonly isEmpty: boolean;
    readonly isRunning: boolean;

    // ---- Optional eventing (add if your impl has it) -------------------
    // on(event: 'complete', cb: (result: T) => void): this;
    // on(event: 'error', cb: (err: Error, task: A) => void): this;
    // off(event: string, cb: (...args: any[]) => void): this;
}
export function useQueue<T = A>(optionsOrConcurrent?: number | QueueOptions): UseQueueResult<T> {
    const queueRef = atom(null) as A;
    const [results, setResults] = useState<T[]>([]);
    const [errors, setErrors] = useState<Error[]>([]);
    const [state, setState] = useState({
        pending: 0,
        active: 0,
        size: 0,
        isEmpty: true,
        isRunning: false
    });
    const updateState = useCallback(() => {
        if (!queueRef.current) return;
        setState({
            pending: queueRef.current.pending,
            active: queueRef.current.active,
            size: queueRef.current.size,
            isEmpty: queueRef.current.isEmpty,
            isRunning: queueRef.current.isRunning
        });
    }, []);

    if (!queueRef.current) {
        const opts = typeof optionsOrConcurrent === 'number'
            ? { concurrent: optionsOrConcurrent }
            : optionsOrConcurrent || {};

        queueRef.current = queue({
            ...opts,
            onComplete: (result: T) => {
                setResults((prev: any) => [...prev, result]);
                if (opts.onComplete) opts.onComplete(result);
                updateState();
            },
            onError: (error: Error, task: A) => {
                setErrors((prev: any) => [...prev, error]);
                if (opts.onError) opts.onError(error, task);
                updateState();
            }
        });
    }



    const add = useCallback(async (task: () => Promise<T>, options?: { priority?: number }) => {
        if (!queueRef.current) throw new Error('Queue not initialized');
        updateState();
        const result = await queueRef.current(task, options);
        updateState();
        return result as T;
    }, [updateState]);

    const start = useCallback(() => {
        queueRef.current?.start();
        updateState();
    }, [updateState]);

    const stop = useCallback(() => {
        queueRef.current?.stop();
        updateState();
    }, [updateState]);

    const clear = useCallback(() => {
        queueRef.current?.clear();
        setResults([]);
        setErrors([]);
        updateState();
    }, [updateState]);

    const flush = useCallback(() => {
        queueRef.current?.flush();
        updateState();
    }, [updateState]);

    useEffect(() => {
        updateState();
    }, [updateState]);

    return {
        add,
        results,
        errors,
        pending: state.pending,
        active: state.active,
        size: state.size,
        isEmpty: state.isEmpty,
        isRunning: state.isRunning,
        start,
        stop,
        clear,
        flush
    };
}
export class TIERED_QUEUE2 {
    private tiers: Map<number, ReturnType<typeof queue>> = new Map();
    private tierOrder: number[] = [];
    private tierResults: Map<number, A[]> = new Map();

    constructor(
        private config: {
            defaultConcurrent?: number;
            retry?: number;
            timeout?: number;
            onTierComplete?: (tier: number, results: A[]) => void;
            onTierError?: (tier: number, error: Error) => void;
        } = {}
    ) { }
    REG(tier: number, options?: { concurrent?: number; retry?: number; timeout?: number }) {
        if (!this.tiers.has(tier)) {
            const tierQueue = queue({
                concurrent: options?.concurrent ?? this.config.defaultConcurrent ?? 100,
                retry: options?.retry ?? this.config.retry ?? 0,
                timeout: options?.timeout ?? this.config.timeout ?? 30000,
                priority: false,
                onError: (error, task) => {
                    console.log(`✘[QUEUE-TIERED][FAILED][${tier}]:[${task.label}]:[${error}]`);
                    this.config.onTierError?.(tier, error);
                },
                onComplete: (result) => {
                    console.log(`✔[QUEUE-TIERED][COMPLETED][${tier}]`);
                }
            });
            this.tiers.set(tier, tierQueue);
            this.tierResults.set(tier, []);
            this.tierOrder.push(tier);
            this.tierOrder.sort((a, b) => a - b);
        }
        return this;
    }
    async ADD<T>(tier: number, label: string, task: () => Promise<T>): Promise<T | undefined> {
        this.REG(tier);
        const tierQueue = this.tiers.get(tier)!;
        const taskKey = `T${tier}-${label}`;
        try {
            const result = await tierQueue(
                async () => {
                    const startTime = Date.now();
                    try {
                        const result = await task();
                        const duration = Date.now() - startTime;
                        console.log(`✔[QUEUE][DONE][T${tier}]:[${label}] ⏱️${duration}ms`);
                        return result;
                    } catch (error) {
                        const duration = Date.now() - startTime;
                        console.log(`✘[QUEUE][FAIL][T${tier}]:[${label}] ⏱️${duration}ms Error: ${error}`);
                        throw error;
                    }
                },
                { label }
            ) as Promise<T>;
            this.tierResults.get(tier)!.push(result);
            return result;
        } catch (error) {
            console.log(`✘[QUEUE-TIERED][FAILED][${tier}]:[${label}]:[${error}]`);
            return undefined;
        }
    }
    async ADD_BATCH<T>(tier: number, tasks: Array<{ label: string; task: () => Promise<T> }>): Promise<(T | undefined)[]> {
        const batchKey = `Batch-T${tier}`;
        const startTime = Date.now();
        const results = await Promise.allSettled(
            tasks.map(task => this.ADD(tier, task.label, task.task))
        );
        const duration = Date.now() - startTime;
        const successCount = results.filter(r => r.status === 'fulfilled').length;
        const failCount = results.filter(r => r.status === 'rejected').length;
        console.log(`✔✘[BATCH][DONE][T${tier}]: ✔${successCount} ✘${failCount} ⏱️${duration}ms`);
        return results.map(result => {
            if (result.status === 'fulfilled') {
                return result.value;
            }
            console.error('Unexpected rejection:', result.reason);
            return undefined;
        });
    }
    /** Executes tiers in order — tasks WITHIN each tier run concurrently. Reactive via onIdle(), no polling. */
    async execute(): Promise<Map<number, A[]>> {
        const results = new Map<number, A[]>();

        for (const tier of this.tierOrder) {
            const tierQueue = this.tiers.get(tier)!;
            if (tierQueue.isPausedState) {
                tierQueue.start();
            }
            await tierQueue.onIdle();
            const tierRes = this.tierResults.get(tier) ?? [];
            this.config.onTierComplete?.(tier, []);
            results.set(tier, tierRes);
        }

        return results;
    }
    getStatus(): { tier: number; pending: number; active: number }[] {
        return this.tierOrder.map(tier => ({
            tier,
            pending: this.tiers.get(tier)?.pending ?? 0,
            active: this.tiers.get(tier)?.active ?? 0
        }));
    }
    clear() {
        this.tiers.forEach(tierQueue => tierQueue.clear());
        this.tierResults.forEach(arr => arr.length = 0);
    }
}
export class Queue {
    private options: Required<QueueOptions>;
    private tasks: QueueTask<A>[] = [];
    private running: QueueTask<A>[] = [];
    private isPaused = false;
    private idleListeners: Set<() => void> = new Set();

    constructor(optionsOrConcurrent: number | QueueOptions = 1) {
        const opts = typeof optionsOrConcurrent === 'number'            ? { concurrent: optionsOrConcurrent }            : optionsOrConcurrent;

        this.options = {
            concurrent: opts.concurrent ?? 1,
            retry: opts.retry ?? 0,
            timeout: opts.timeout ?? 0,
            priority: opts.priority ?? false,
            onError: opts.onError ?? (() => { }),
            onComplete: opts.onComplete ?? (() => { }),
            onChange: opts.onChange ?? (() => { })
        };
    }
    async run<T>(taskOrTasks: (() => Promise<T>) | (() => Promise<T>)[], taskOptions?: { priority?: number; label?: string }): Promise<T | T[]> {
        if (Array.isArray(taskOrTasks)) {
            return Promise.all(taskOrTasks.map(fn => this.add(fn, taskOptions)));
        }
        return this.add(taskOrTasks, taskOptions);
    }
    private add<T = any>(fn: () => Promise<T>, taskOptions?: { priority?: number; label?: string }): Promise<T> {
        return new Promise((resolve, reject) => {
              let insertIndex = queuedLength;
        let firstInsertedIndex = insertIndex;
            const task: QueueTask<T> = { fn, priority: taskOptions?.priority ?? 0, label: taskOptions?.label, resolve, reject };

        if (this.options.priority) {
    const insertIndex = this.tasks.findIndex(
        t => (t.priority ?? 0) < (task.priority ?? 0)
    );

    if (insertIndex === -1) {
        this.tasks.push(task);
    } else {
        this.tasks.splice(insertIndex, 0, task);
    }
} else {
    this.tasks.push(task);
}


            this.options.onChange();
            this.process();
        });
    }
    private async process2() {
        if (this.isPaused) return;
        if (this.running.length >= this.options.concurrent) return;
        if (this.tasks.length === 0) return;

        const task = this.tasks.shift()!;
        this.running.push(task);
        this.options.onChange();

        try {
            const result = await this.executeWithRetry(task.fn);
            task.resolve(result);
            this.options.onComplete(result);
        } catch (error) {
            task.reject(error as Error);
            this.options.onError(error as Error, task);
        } finally {
            this.running = this.running.filter(t => t !== task);
            this.options.onChange();
            this.maybeFireIdle();

            this.process();
        }
    }
    private process() {
        if (this.isPaused) return;

        while (
            !this.isPaused &&
            this.running.length < this.options.concurrent &&
            this.tasks.length > 0
        ) {
            const task = this.tasks.shift()!;

            this.running.push(task);
            this.options.onChange();

            void this.runTask(task);
        }
    }

    private async runTask(task: QueueTask<A>) {
        try {
            const result = await this.executeWithRetry(task.fn);

            task.resolve(result);
            this.options.onComplete(result);
        } catch (error) {
            task.reject(error as Error);
            this.options.onError(error as Error, task);
        } finally {
            this.running = this.running.filter((running) => running !== task);

            this.options.onChange();
            this.maybeFireIdle();

            // Backfill the slot immediately.
            this.process();
        }
    }
    private async executeWithRetry<T>(fn: () => Promise<T>, attempt = 0): Promise<T> {
        try {
            if (this.options.timeout > 0) {
                return await this.withTimeout(fn(), this.options.timeout);
            }
            return await fn();
        } catch (error) {
            if (attempt < this.options.retry) {
                return this.executeWithRetry(fn, attempt + 1);
            }
            throw error;
        }
    }
    private withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            let settled = false;
            const timer = setTimeout(() => {
                if (settled) return;
                settled = true;
                reject(new Error('Timeout'));
            }, ms);

            promise.then(
                (v) => { if (!settled) { settled = true; clearTimeout(timer); resolve(v); } },
                (e) => { if (!settled) { settled = true; clearTimeout(timer); reject(e); } }
            );
        });
    }
    /** Resolves immediately if already idle, otherwise resolves the next time the queue fully empties. No polling. */
    onIdle(): Promise<void> {
        if (this.tasks.length === 0 && this.running.length === 0 && !this.isPaused) return Promise.resolve();
        return new Promise((resolve) => { this.idleListeners.add(resolve); });
    }
    removeAt(index: number) {
        if (index < 0 || index >= this.tasks.length) return;
        const [task] = this.tasks.splice(index, 1);
        task.reject(new Error('Removed from queue'));
        this.options.onChange();
    }
    moveUp(index: number) {
        if (index <= 0 || index >= this.tasks.length) return;
        [this.tasks[index - 1], this.tasks[index]] = [this.tasks[index], this.tasks[index - 1]];
        this.options.onChange();
    }
    moveDown(index: number) {
        if (index < 0 || index >= this.tasks.length - 1) return;
        [this.tasks[index], this.tasks[index + 1]] = [this.tasks[index + 1], this.tasks[index]];
        this.options.onChange();
    }
    moveToNext(index: number) {
        if (index <= 0 || index >= this.tasks.length) return;
        const [task] = this.tasks.splice(index, 1);
        this.tasks.unshift(task);
        this.options.onChange();
    }
    moveToLast(index: number) {
        if (index < 0 || index >= this.tasks.length - 1) return;
        const [task] = this.tasks.splice(index, 1);
        this.tasks.push(task);
        this.options.onChange();
    }
    start() { this.isPaused = false; this.maybeFireIdle(); this.process(); }
    stop() { this.isPaused = true; }
    private maybeFireIdle() {
        if (this.tasks.length === 0 && this.running.length === 0 && !this.isPaused) {
            this.idleListeners.forEach(resolve => resolve());
            this.idleListeners.clear();
        }
    }
    clear() {
        this.tasks = [];
        this.options.onChange();
        this.maybeFireIdle();
    }
    flush() {
        const pending = [...this.tasks];
        this.tasks = [];
        pending.forEach(task => task.reject(new Error('Queue flushed')));
        this.options.onChange();
        this.maybeFireIdle();
    }
    peek() { return this.tasks[0]; }
    peekAll() { return [...this.tasks]; }
    getRunning() { return [...this.running]; }
    get size() { return this.tasks.length; }
    get isEmpty() { return this.tasks.length === 0; }
    get isRunning() { return this.running.length > 0; }
    get isPausedState() { return this.isPaused; }
    get pending() { return this.tasks.length; }
    get active() { return this.running.length; }
    setOptions(newOptions: Partial<QueueOptions>) { this.options = { ...this.options, ...newOptions }; }
}
export function queue(options?: number | QueueOptions): QueueFunction {
    const q = new Queue(options);

    const runFn = <T>(taskOrTasks: (() => Promise<T>) | (() => Promise<T>)[], taskOptions?: { priority?: number; label?: string }) => {
        return q.run(taskOrTasks, taskOptions);
    };

    runFn.start = () => q.start();
    runFn.stop = () => q.stop();
    runFn.clear = () => q.clear();
    runFn.flush = () => q.flush();
    runFn.onIdle = () => q.onIdle();
    runFn.peek = () => q.peek();
    runFn.peekAll = () => q.peekAll();
    runFn.getRunning = () => q.getRunning();
    runFn.removeAt = (i: number) => q.removeAt(i);
    runFn.moveUp = (i: number) => q.moveUp(i);
    runFn.moveDown = (i: number) => q.moveDown(i);
    runFn.moveToNext = (i: number) => q.moveToNext(i);
    runFn.moveToLast = (i: number) => q.moveToLast(i);
    runFn.setOptions = (opts: Partial<QueueOptions>) => q.setOptions(opts);

    Object.defineProperty(runFn, 'size', { get: () => q.size });
    Object.defineProperty(runFn, 'isEmpty', { get: () => q.isEmpty });
    Object.defineProperty(runFn, 'isRunning', { get: () => q.isRunning });
    Object.defineProperty(runFn, 'isPausedState', { get: () => q.isPausedState });
    Object.defineProperty(runFn, 'pending', { get: () => q.pending });
    Object.defineProperty(runFn, 'active', { get: () => q.active });

    return runFn as QueueFunction;
}
export class TIERED_QUEUE {
    private tiers = new Map<number, ReturnType<typeof queue>>();
    private tierOrder: number[] = [];
    private stagedTasks = new Map<number, TierTask[]>();// Tasks waiting to be released for their priority batch. `ADD()` writes here. `execute()` drains it.
    private tierResults = new Map<number, A[]>();// Final successful values, grouped by tier.

    constructor(
        private config: {
            defaultConcurrent?: number;
            retry?: number;
            timeout?: number;
            onTierComplete?: (tier: number, results: A[]) => void;
            onTierError?: (tier: number, error: Error) => void;
        } = {},
    ) { }
	// registerTier
    REG(tier: number, options?: TierOptions) {
        if (!this.tiers.has(tier)) {
            const tierQueue = queue({
                concurrent: options?.concurrent ?? this.config.defaultConcurrent ?? 100,
                retry: options?.retry ?? this.config.retry ?? 0,
                timeout: options?.timeout ?? this.config.timeout ?? 30_000,
                priority: false,

                onError: (error, task) => { console.log(`✘[QUEUE-TIERED][FAILED][${tier}] [${task.label}]` ); this.config.onTierError?.(tier, error); },
                onComplete: () => { console.log(`✔[QUEUE-TIERED][COMPLETED][${tier}] ` ) },
            });// Important: A tier queue is dormant until `execute()` releases its batch.
            tierQueue.stop();
            this.tiers.set(tier, tierQueue);
            this.stagedTasks.set(tier, []);
            this.tierResults.set(tier, []);
            this.tierOrder.push(tier);
            this.tierOrder.sort((a, b) => a - b);
        }

        return this;
    }// Registers a task in its priority batch. It intentionally does NOT run now. The returned promise resolves/rejects when `execute()` eventually reaches this task's batch.
    // addToTier
	ADD<T=A>(tier: N, label: S, task: () => P<T>, options?: TierOptions,): P<T | UD> {
        this.REG(tier, options);
        return new Promise<T | UD>((resolve, reject) => {
            this.stagedTasks.get(tier)!.push({ label, task, resolve, reject, }as TierTask<U>);
        });
    }// Adds many tasks to one priority batch. All tasks in this call—and all previous tasks added at this same priority—run in the same concurrent batch when execute() reaches it.
	// addBatchToTier 
	ADD_BATCH<T>(tier: N, tasks: Array<{ label: S; task: () => Promise<T>; }>, options?: TierOptions,): P<(T | UD)[]> {
        return Promise.all(
            tasks.map(({ label, task }) =>
                this.ADD(tier, label, task, options),
            ),
        );
    }// Runs priority batches in ascending numeric order:
    // priority 0 → all concurrent tasks settle
    // priority 1 → all concurrent tasks settle
    // priority 2 → all concurrent tasks settle
    async execute(): Promise<Map<N, A[]>> {
        const allResults = new Map<N, A[]>();
        for (const tier of this.tierOrder) {
            const tierQueue = this.tiers.get(tier)!;
            const staged = this.stagedTasks.get(tier) ?? [];
            const tierResults = this.tierResults.get(tier)!;
            if (staged.length === 0) { allResults.set(tier, [...tierResults]); continue; }
            console.log(`▶[QUEUE-TIERED][START][${tier}] ${staged.length} task(s)`)
            const pending = staged.splice(0, staged.length);// Queue all members of the batch BEFORE start(). This guarantees every task in the batch is available together. `Queue.process()` then chooses how many can actually run at once, according to its `concurrent` setting.
            const scheduled = pending.map((entry) => {
                return tierQueue(
                    async () => {
                        const startedAt = Date.now();
                        try {
                            const value = await entry.task();
                            const duration = Date.now() - startedAt;
                            tierResults.push(value);
                            entry.resolve(value);
                            console.log(`✔[QUEUE][DONE][T${tier}] [${entry.label}] ⏱️${duration}ms`)
                            return value;
                        } catch (error) {
                            const duration = Date.now() - startedAt;
                            console.log(`✘[QUEUE][FAIL][T${tier}]:[${entry.label}] ⏱️${duration}ms Error: ${error} `)
                            entry.resolve(undefined);// Preserve your old return convention: failed ADD calls resolve undefined rather than reject. If you prefer normal Promise semantics, replace this line with `entry.reject(error)`.
                            throw error;
                        }
                    },
                    { label: entry.label },
                );
            });
            tierQueue.start();// Release this exact priority batch only after all members have been queued.
            await Promise.allSettled(scheduled); //  Do not use Promise.all here: one rejected task would cause the next priority batch to start early.
            await tierQueue.onIdle();//  This is mostly redundant after `allSettled`, but retains the queue's own authoritative idle lifecycle.
            this.config.onTierComplete?.(tier, [...tierResults]);
            allResults.set(tier, [...tierResults]);
            console.log(`✔[QUEUE-TIERED][BATCH-DONE][${tier}]: ${tierResults.length} successful result(s)`)
        }
        return allResults;
    }
    getStatus() {
        return this.tierOrder.map((tier) => {
            const tierQueue = this.tiers.get(tier)!;
            return {
                tier,
                staged: this.stagedTasks.get(tier)?.length ?? 0,
                pending: tierQueue.pending,
                active: tierQueue.active,
                completed: this.tierResults.get(tier)?.length ?? 0,
            };
        });
    }// Cancels tasks that have not reached an active queue yet. Existing active tasks cannot generally be force-cancelled unless their `task()` implementation supports AbortSignal.
    clear() {
        for (const [tier, staged] of this.stagedTasks) {
            for (const entry of staged) { entry.reject(new Error(`Tier ${tier} cleared before execution`)); }
            staged.length = 0;
        }
        for (const tierQueue of this.tiers.values()) { tierQueue.clear(); }
        for (const results of this.tierResults.values()) { results.length = 0; }
    }
}
// #endregion
