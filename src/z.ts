export const DOMAIN = {
    STATE: 0,
    CONTEXT: 1,
    SIGNALS: 2,
    ATOMS: 3,
    SEND: 4,
    SUB: 5,
    DUPLEX: 6,
    STREAM: 7,
    DRS: 8,
    SPREADSHEET: 9,
    DB: 10,
} as const;
const OP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	COMPACT: 4,
	NONE: 5,
    EDIT: 6,
	COMPUTED:7,
	EFFECTSCOPE: 8,
	EXTERNAL: 9,
	OBSERVER: 10,
	ASYNCEFFECT: 11,
} as const;
export type Domain = (typeof DOMAIN)[keyof typeof DOMAIN];
export const DOMAIN_COUNT = 11;

export type HANDLER = (    tracerParticle: TracerParticle,    particle: Uint8Array,) => void | Promise<void>;

export type TracerParticle = { epoch: number;};


class GOVERNOR {
	/* ==========================================================================
	   The disruptor runs hot on its own.
	   The public API never touches passes.
	   ========================================================================== */
	  private tier: 0 | 1 | 2 | 3 = 0;          // 25 / 50 / 75 / 100
	  private running = false;
	  private hibernating = false;
	
	  private lastAction = 0;
	  private windowActions = 0;
	  private windowStart = 0;
	
	  private readonly IDLE_MS = 15_000;
	  private readonly TIERS = [
		256,    // tier 0 -> 25%
		1_024,   // tier 1 -> 50%
		4_096,   // tier 2 -> 75%
		16_384,  // tier 3 -> 100%
	  ];
	  constructor(private readonly root: SIGNALS) {}
	  wake(): void {
		this.lastAction = Date.now();
	
		if (this.hibernating) {
		  this.hibernating = false;
		}
	
		if (!this.running) {
		  this.running = true;
		  queueMicrotask(() => this.loop());
		}
	  }
	  private async loop(): Promise<void> {
		while (this.running) {
		  const now = Date.now();
	
		  // Hibernation: 15s of true silence.
		  if (now - this.lastAction >= this.IDLE_MS) {
			this.hibernating = true;
			this.running = false;
			this.tier = 0;
			return;
		  }
	
		  // Drain one revolution at the current tier.
		  this.root.RX_S.pass();
	
		  // Sample load and adjust tier.
		  this.windowActions++;
	
		  if (this.windowStart === 0) {
			this.windowStart = now;
		  }
	
		  const windowMs = now - this.windowStart;
	
		  if (windowMs >= 1_000) {
			const rate = (this.windowActions / windowMs) * 1_000;
	
			this.tier = this.tierFor(rate);
	
			this.windowActions = 0;
			this.windowStart = now;
		  }
	
		  // Yield so the loop never starves the JS runtime.
		  await Promise.resolve();
		}
	  }
	  private tierFor(rate: number): 0 | 1 | 2 | 3 {
		if (rate >= this.TIERS[2]) return 3;
		if (rate >= this.TIERS[1]) return 2;
		if (rate >= this.TIERS[0]) return 1;
		return 0;
	  }
	  currentTier(): number {
		return this.tier;
	  }
	  isHibernating(): boolean {
		return this.hibernating;
	  }
}
export class Disruptor {
    private readonly lanes: Lane[];
    private readonly handlers: Array<HANDLER | undefined>;

    constructor(private readonly laneCapacity = 1024) {
        this.lanes = [];
        this.handlers = new Array<HANDLER | undefined>(DOMAIN_COUNT);

        for (let i = 0; i < DOMAIN_COUNT; i++) {
            this.lanes.push(new Lane(laneCapacity));
        }
    }

    public on(domain: Domain, handler: HANDLER): this {
        this.handlers[domain] = handler;
        return this;
    }

    public publish(domain: Domain, frame: Uint8Array): boolean {
        return this.lanes[domain].publish(frame);
    }

    /**
     * One pass.
     *
     * Called by GOVERNOR—not by application code.
     * Drains each nonempty lane exactly once.
     * Frames emitted during this pass wait for the next pass.
     */
    public pass(): number {
        const particle: TracerParticle = {
            epoch: 0,
        };

        let processed = 0;

        for (let domain = 0; domain < DOMAIN_COUNT; domain++) {
            const lane = this.lanes[domain];
            const count = lane.readable();

            if (count === 0) {
                continue;
            }

            const handler = this.handlers[domain];

            if (!handler) {
                continue;
            }

            const frames = lane.drain(count);

            for (const frame of frames) {
                handler(particle, frame);
            }

            processed += count;
        }

        particle.epoch++;

        return processed;
    }
}
export class Lane {
    public readonly capacity: number;
    public readonly mask: number;

    private readonly frames: Array<Uint8Array | undefined>;
    private head = 0;
    private free: number;

    constructor(capacity = 1024) {
        if (
            !Number.isInteger(capacity) ||
            capacity < 2 ||
            capacity > 1 << 30 ||
            (capacity & (capacity - 1)) !== 0
        ) {
            throw new Error(
                "Lane capacity must be a power of two between 2 and 2^30."
            );
        }

        this.capacity = capacity;
        this.mask = capacity - 1;
        this.free = capacity;

        this.frames = new Array<Uint8Array | undefined>(capacity);
    }

    public publish(frame: Uint8Array): boolean {
        if (this.free === 0) {
            return false;
        }

        this.frames[this.head] = frame;
        this.head = (this.head + 1) & this.mask;
        this.free--;

        return true;
    }

    public readable(): number {
        return this.capacity - this.free;
    }

    public drain(count: number): Uint8Array[] {
        const out: Uint8Array[] = [];
        let cursor = (this.head - count) & this.mask;

        for (let i = 0; i < count; i++) {
            const frame = this.frames[cursor];

            if (frame !== undefined) {
                out.push(frame);
                this.frames[cursor] = undefined;
            }

            cursor = (cursor + 1) & this.mask;
        }

        this.free += count;

        return out;
    }
}
