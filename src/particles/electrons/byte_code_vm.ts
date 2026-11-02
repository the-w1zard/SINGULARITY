// electrons/byte_code_vm.ts

export const BC = {
    HALT: 0,

    LOAD_EVENT_FIELD: 1,
    LOAD_F64: 2,
    STORE_F64: 3,

    ADD_F64: 4,
    SUB_F64: 5,
    MUL_F64: 6,
    DIV_F64: 7,

    LOAD_IMMEDIATE_F64: 8,

    MARK_DIRTY: 9,
    EMIT: 10,
    EMIT_F64: 11,

    JUMP: 12,
    JUMP_IF_ZERO: 13,
    JUMP_IF_NOT_ZERO: 14,
} as const;
export class BYTE_CODE_VM {
	public k: ATOM_KEY;

    private pc = 0;
    private acc = 0;

    constructor(
        private readonly values: F64,
        private readonly event: F64,
    ) {}

    run(code: I32, pid: N, slot: N): V {
        this.pc = 0;
        this.acc = 0;

        for (;;) {
            const op = code[this.pc++];

            switch (op) {
                case BC.HALT:
                    return;

                case BC.LOAD_EVENT_FIELD: {
                    const field = code[this.pc++];
                    this.acc = this.event[field];
                    break;
                }

                case BC.STORE_F64: {
                    const target = code[this.pc++];
                    this.values[target] = this.acc;
                    break;
                }

                case BC.EMIT: {
                    const lane = code[this.pc++];
                    this.d.emitRaw(lane, pid, slot, this.acc | 0);
                    break;
                }

                case BC.ADD_F64: {
                    const operand = code[this.pc++];
                    this.acc += this.values[operand];
                    break;
                }

                case BC.JUMP: {
                    this.pc = code[this.pc++];
                    break;
                }

                case BC.JUMP_IF_ZERO: {
                    const target = code[this.pc++];

                    if (this.acc === 0) {
                        this.pc = target;
                    }

                    break;
                }

                default:
                    throw new Error(`BYTECODE_VM: unknown opcode ${op} at pc=${this.pc - 1}`);
            }
        }
    }
}

class BEAM_CONTROLLER {
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
      constructor() {}
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
        //  this.hub.collider.pass();
    
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
