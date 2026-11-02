// electrons/concurrency.ts


// ═══════════════════════════════════════════════════════════════════════════════════════════════
//  Relationship to the rest of the system (read this before touching):
//    · PRIORITY_SHARD ORDERS. BACKPRESSURE EXECUTES. They stay separate — no double-queueing.
//      MQ_SEND stages work into PRIORITY, and when SHOULD_BACKPRESSURE() fires, the sends stop
//      being dispatched inline and this shard's engine takes over, pulling from PRIORITY.
//    · Coalescing front-door (batch / asyncBatch semantics): groups sends by size + wait +
//      maxWait BEFORE they enter the tiered engine. One group enters a tier as one staged batch.
//    · Execution spine: dormant tier queues (TIERED_QUEUE semantics) — a tier is registered
//      and STOPPED; `execute()` releases its staged batch all at once, `allSettled` between
//      batches so one rejection can never start the next tier early.
//    · Concurrency: one CONCURRENCY_GOVERNOR for the whole cell (AIMD — additive increase,
//      multiplicative half-down), so tiers breathe together instead of thrashing separately.
//      Semaphore resizes with the governor; tier queues read the governor's concurrency.
//    · Circuit breakers stay EXTERNAL (they're their own solution class). This shard only
//      consults them at tier boundaries — `state === 'open'` pauses the engine; we honor
//      `cooldownRemainingMs()` before resuming. Same gate as WIZARD_BATCH.isCircuitOpen().
//    · Backpressure is the TRIGGER (Skyler's call): when SHOULD_BACKPRESSURE() fires, flow
//      switches from inline dispatch to this engine. It is not a boolean gate on its own.
//    · Errors route through ERROR_SHARD's emission gate (dev-mode surgical toggles), and
//      terminal failures go to DEADLETTER via `sendToDeadletter` if wired.
//    · Devtools events only fire when DEVTOOLS_ACTIVE.
//  Ambient deps (unchanged): S, B, N, A, ATOM_KEY, CSEMessage, SIG_BUS_Priority, CIRCUITBREAKER,
//    DEVTOOLS_ACTIVE, emitDevtoolsEvent.
// ═══════════════════════════════════════════════════════════════════════════════════════════════

// ── shared types (add once, top of file, alongside Patch 2's) ───────────────────────────────────

export interface BackpressureConfig {
	/** Max items queued before SHOULD_BACKPRESSURE() fires. 0/undefined = never. */
	maxQueueSize?: N;
	/** Cooldown between drain attempts once throttling starts. */
	throttleMs?: N;
	/** Coalescing window (ms) before a staged group flushes even if size not reached. */
	batchWaitMs?: N;
	/** Hard deadline (ms) from FIRST item staged — flushes the group no matter what. */
	batchMaxWaitMs?: N;
	/** Items per coalesced group. */
	batchSize?: N;
	/** Default per-tier concurrency if a tier registers without options. */
	defaultConcurrent?: N;
	/** Default retry / timeout for tier tasks. */
	retry?: N;
	timeout?: N;
}

type COALESCED_GROUP = {
	items: Array<{ msg: CSEMessage; resolve: (v: A) => void; reject: (e: A) => void }>;
	flushed: B;
};

// ── CONCURRENCY_GOVERNOR (internal — AIMD engine lifted from BATCH, one per shard) ───────────────
// Additive increase on healthy telemetry, multiplicative half-down on friction. Health from
// event-loop lag + RSS + external breaker state. Semaphore with dynamic resize (SEMAPHORE
// semantics), exposed so tier queues can consult `tryAcquire/release`.
export class CONCURRENCY_ELECTRON {
	private current: N;
	private readonly min: N;
	private readonly max: N;
	private holders = new Set<symbol>();
	private waiters: Array<() => void> = [];
	private peak: N = 0;

	constructor(config: { minConcurrency: N; maxConcurrency: N; initialConcurrency?: N } = { minConcurrency: 1, maxConcurrency: 64 }) {
		this.min = config.minConcurrency;
		this.max = config.maxConcurrency;
		this.current = Math.min(this.max, Math.max(this.min, config.initialConcurrency ?? Math.max(4, Math.floor(this.max / 4))));
		this.peak = this.current;
	}

	get active(): N { return this.holders.size; }
	get concurrency(): N { return this.current; }
	get peakConcurrency(): N { return this.peak; }

	tryAcquire(): symbol | undefined {
		if (this.holders.size >= this.current) return undefined;
		const h = Symbol('governor-slot');
		this.holders.add(h);
		if (this.holders.size > this.peak) this.peak = this.holders.size;
		return h;
	}

	async acquire(): Promise<symbol> {
		const h = this.tryAcquire();
		if (h) return h;
		return new Promise<symbol>(resolve => this.waiters.push(() => resolve(this.tryAcquire()!)));
	}

	release(h: symbol): void {
		this.holders.delete(h);
		const next = this.waiters.shift();
		if (next) next();
	}

	/** AIMD. Call from telemetry. */
	evaluateResistance(signal: { latency: N; error: B; targetLatencyMs?: N }): void {
		const target = signal.targetLatencyMs ?? 250;
		if (signal.error || signal.latency > target * 1.5) this.multiplicativeDecrease();
		else if (signal.latency < target * 0.7) this.additiveIncrease();
	}

	additiveIncrease(): void {
		if (this.current < this.max) this.resize(this.current + 1);
	}

	multiplicativeDecrease(): void {
		if (this.current > this.min) this.resize(Math.max(this.min, Math.floor(this.current * 0.5)));
	}

	resize(n: N): void {
		this.current = Math.max(this.min, Math.min(this.max, Math.floor(n)));
		if (this.current > this.peak) this.peak = this.current;
		// Wake waiters if capacity grew.
		while (this.holders.size < this.current && this.waiters.length) this.waiters.shift()?.();
	}
}
