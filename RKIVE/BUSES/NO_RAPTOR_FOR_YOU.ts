// bus-bench.ts
// FIGHT=... bun --expose-gc bus-bench.ts
import { YODA } from "./YODA";
import { signal as vader_signal, computed as vader_computed, effect as vader_effect, effectScope as vader_effectsope, batch as vader_batch, startBatch as vader_startbatch, endBatch as vader_endbatch, untracked as vader_untracled, bindText as vader_bindtext, bindAttribute as vader_bindattr, bind as vader_bind, SIGNAL_BUS as vader_signalsbus } from "./VADER";
import { signal as f_sig, computed as f_computed, effect as f_effect, batch as f_batch, startBatch as f_startbatch, endBatch as f_endbatch, untracked as f_untracked, bindText as f_bindtxt, bindAttribute as f_bindatr, bind as f_bind, SIGNALS as f_bus, ChannelMessage } from "./THE_FORCE";
import { SKYWALKER } from "./SKYWALKER";
import { SITH } from "./SITH";
import { PALPATINE } from "./PALPATINE";
import { JEDI } from "./JEDI";
import { startBatch,   signal, computed, effect,batch } from "./class";

import { signal as preact_signal, computed as preact_computed, batch as preact_batch, effect as preact_effect, action, createModel, type Model, type ModelConstructor, type ModelFactory, Signal, type ReadonlySignal, untracked as preact_untracked, type SignalOptions, type EffectOptions } from "@preact/signals-core";
import { SIDIUS } from "./SIDIUS";
import { persistedSignal } from "@kamod-ch/signals";


const CAPACITY = 1 << 18;
const DURATION = 12;
const REPORT_MS = 12;
const BATCH_SIZE = 100_000; // 500_000
const TOTAL_BATCHES = 100; // 500
const RUN_BARE_METAL_CPU_TEST: Boolean = true;

const pid = 1;
let seq = 0;
const v = `${++seq}`;

type fighter =
| "yoda"
| "vader"
| "the_force"
| "SKYWALKER"
| "SITH"
| "PALPATINE"
| "SIDIUS"
| "JEDI"
| "alien"
| "preact"
| "alien2"
| "preact2"
| "alien3"
|"SIGNAL_ENGINE"
|"REACTIVE_SYSTEM"

const FIGHT: fighter = (process.env.FIGHT as fighter) ?? "SIGNAL_ENGINE";


type SIG = { r: S; m: S; s: S; k: S; p: U };
type H = (p: U, msg?: SIG) => number;
type BUS_CHANNEL = { handler?: H };

const r = "THE_WIZARD_ROOT_REG";
const m = "SAURON_MASTER_REG";
const s = "SAURON_SLAVE_REG";
const k = "__SAURON__";

const handler = (p) => p.p + 1;
const handle2 = (p) => p.p + 1;
const handleFlat = (p) => p.p + 1;

// ─────────────────────────────────────────────────────────────
// BENCH
// ─────────────────────────────────────────────────────────────

const dur = 1000 * DURATION;
const report = 1000 * REPORT_MS;

function BARE_METAL_CPU(name: string, write: () => void): void {
  const batchTimes = new Float64Array(TOTAL_BATCHES);
  for (let i = 0; i < 2_000_000; i++) write();

  const globalStart = process.hrtime.bigint();
  for (let b = 0; b < TOTAL_BATCHES; b++) {
    const batchStart = process.hrtime.bigint();
    for (let i = 0; i < BATCH_SIZE; i++) write();
    batchTimes[b] = Number(process.hrtime.bigint() - batchStart);
  }
  const globalEnd = process.hrtime.bigint();

  const totalOps = BATCH_SIZE * TOTAL_BATCHES;
  let bestOpsPerSec = 0;
  for (let b = 0; b < TOTAL_BATCHES; b++) {
    const ops = BATCH_SIZE / (batchTimes[b] / 1e9);
    if (ops > bestOpsPerSec) bestOpsPerSec = ops;
  }
  const avg = totalOps / (Number(globalEnd - globalStart) / 1e9);

  console.log(`------------ ${name} ---------------`);
  console.log(`Peak (Best Batch):                      ${(bestOpsPerSec / 1e6).toFixed(2)} M ops/sec`);
  console.log(`Average:                                ${(avg / 1e6).toFixed(2)} M ops/sec`);
  console.log(`---------------------------------------------------------`);
}3

function BENCH(name: string, write: () => void): void {
  let windowOps = 0, totalOps = 0, best = 0, elapsedTotal = 0;
  let start = process.hrtime.bigint();
  let windowStart = start;
  let cpuStart = process.cpuUsage();

  console.log(`\n${name}`);
  console.log("sec | M writes/s | best M/s | avg M/s | CPU %");

  while (elapsedTotal < dur) {
    for (let i = 0; i < 100_000; i++) write();
    windowOps += 100_000; totalOps += 100_000;
    const now = process.hrtime.bigint();
    const elapsedUs = Number(now - windowStart) / 1_000;
    elapsedTotal = Number(now - start) / 1e6;
    if (elapsedUs >= report * 1_000) {
      const mp = windowOps / elapsedUs;
      const avg = totalOps / (Number(now - start) / 1e9) / 1e6;
      const cpuNow = process.cpuUsage();
      const cpuUs = cpuNow.user - cpuStart.user + cpuNow.system - cpuStart.system;
      best = Math.max(best, mp);
      console.log(
        String(Math.floor(elapsedTotal / 1_000)).padStart(3), "|",
        mp.toFixed(2).padStart(10), "|",
        best.toFixed(2).padStart(8), "|",
        avg.toFixed(7).padStart(7), "|",
        ((cpuUs / elapsedUs) * 100).toFixed(1).padStart(5)
      );
      windowOps = 0; windowStart = now; cpuStart = cpuNow;
    }
  }
}

const bench = RUN_BARE_METAL_CPU_TEST === false ? BENCH : BARE_METAL_CPU;

// ─────────────────────────────────────────────────────────────
// FIGHTERS
// ─────────────────────────────────────────────────────────────
const TAG = {
	UNDEFINED: 0,
	NULL: 1,
	FALSE: 2,
	TRUE: 3,
	INT32: 4,
	FLOAT64: 5,
	STRING: 6,
	ARRAY: 7,
	OBJECT: 8,
	UINT8ARRAY: 9,
	REF: 10,
} as const;
export class BEAM_CODEC {
	/* =============================================================================
     Encodes ordinary JavaScript values into a collider-owned byte slab.
	 ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC] */
	private bytes: U8;
	private view: DataView;
	private tail = 0;

	private strings = new Map<S, N>();
	private stringBytes: U8[] = [];
	private refs = new Map<object, N>();
	private nextRef = 0;

	constructor(initialCapacity = 1 << 16) {
		this.bytes = new Uint8Array(initialCapacity);
		this.view = new DataView(this.bytes.buffer);
	}
	encode(value: U): U8 {
		this.tail = 0;
		this.strings.clear();
		this.stringBytes.length = 0;
		this.refs.clear();
		this.nextRef = 0;

		this.writeValue(value);

		return this.bytes.subarray(0, this.tail);
	}
	/* HC_BINARY_CODEC */
	decode(binary: U8): U {
		const savedBytes = this.bytes;
		const savedView = this.view;
		const savedTail = this.tail;

		try {
			this.bytes = binary;
			this.view = new DataView(binary.buffer, binary.byteOffset, binary.byteLength);
			this.tail = binary.length;

			this.refs.clear();
			this.nextRef = 0;

			const cursor = { at: 0 };

			return this.readValue(cursor);
		} finally {
			// Restore the encode-side buffer so the next encode()
			// never writes into the disruptor pool.
			this.bytes = savedBytes;
			this.view = savedView;
			this.tail = savedTail;
		}
	}
	private need(count: N): V {
		if (this.tail + count <= this.bytes.length) return;

		let size = this.bytes.length;

		while (size < this.tail + count) size *= 2;

		const next = new Uint8Array(size);
		next.set(this.bytes);

		this.bytes = next;
		this.view = new DataView(next.buffer);
	}
	private u8(value: N): V {
		this.need(1);
		this.bytes[this.tail++] = value;
	}
	private i32(value: N): V {
		this.need(4);
		this.view.setInt32(this.tail, value, true);
		this.tail += 4;
	}
	private f64(value: N): V {
		this.need(8);
		this.view.setFloat64(this.tail, value, true);
		this.tail += 8;
	}
	private writeValue(value: U): V {
		if (value === undefined) return this.u8(TAG.UNDEFINED);
		if (value === null) return this.u8(TAG.NULL);
		if (value === false) return this.u8(TAG.FALSE);
		if (value === true) return this.u8(TAG.TRUE);
		const type = typeof value;
		if (type === "number") {
			const n = value as N;
			if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
				this.u8(TAG.INT32);
				return this.i32(n);
			}

			this.u8(TAG.FLOAT64);
			return this.f64(n);
		}

		if (type === "string") {
			return this.writeString(value as S);
		}

		if (Array.isArray(value)) {
			return this.writeArray(value);
		}

		if (value instanceof Uint8Array) {
			return this.writeBytes(value);
		}

		if (type === "object") {
			return this.writeObject(value as object);
		}

		throw new Error(`HC_BINARY_CODEC: unsupported value type: ${type}`);
	}
	private writeString(value: S): V {
		const encoded = new TextEncoder().encode(value);

		this.u8(TAG.STRING);
		this.i32(encoded.length);
		this.need(encoded.length);
		this.bytes.set(encoded, this.tail);
		this.tail += encoded.length;
	}
	private writeArray(value: U[]): V {
		const ref = this.beginRef(value);

		this.u8(TAG.ARRAY);
		this.i32(value.length);

		for (let i = 0; i < value.length; i++) {
			this.writeValue(value[i]);
		}

		this.endRef(ref);
	}
	private writeBytes(value: U8): V {
		this.u8(TAG.UINT8ARRAY);
		this.i32(value.length);
		this.need(value.length);
		this.bytes.set(value, this.tail);
		this.tail += value.length;
	}
	private writeObject(value: object): V {
		const ref = this.beginRef(value);
		const keys = Object.keys(value);

		this.u8(TAG.OBJECT);
		this.i32(keys.length);

		for (const key of keys) {
			this.writeString(key);
			this.writeValue((value as any)[key]);
		}

		this.endRef(ref);
	}
	private beginRef(value: object): N {
		const existing = this.refs.get(value);

		if (existing !== undefined) {
			this.u8(TAG.REF);
			this.i32(existing);
			return -1;
		}

		const ref = this.nextRef++;
		this.refs.set(value, ref);

		return ref;
	}
	private endRef(ref: N): V {
		// Reserved for future back-reference patching.
		void ref;
	}
	private readU8(cursor: { at: N }): N {
		return this.bytes[cursor.at++];
	}
	private readI32(cursor: { at: N }): N {
		const value = this.view.getInt32(cursor.at, true);
		cursor.at += 4;

		return value;
	}
	private readF64(cursor: { at: N }): N {
		const value = this.view.getFloat64(cursor.at, true);
		cursor.at += 8;

		return value;
	}
	private readValue(cursor: { at: N }): U {
		const tag = this.readU8(cursor);

		switch (tag) {
			case TAG.UNDEFINED:
				return undefined;

			case TAG.NULL:
				return null;

			case TAG.FALSE:
				return false;

			case TAG.TRUE:
				return true;

			case TAG.INT32:
				return this.readI32(cursor);

			case TAG.FLOAT64:
				return this.readF64(cursor);

			case TAG.STRING:
				return this.readString(cursor);

			case TAG.ARRAY:
				return this.readArray(cursor);

			case TAG.OBJECT:
				return this.readObject(cursor);

			case TAG.UINT8ARRAY:
				return this.readBytes(cursor);

			default:
				throw new Error(`HC_BINARY_CODEC: unknown tag ${tag}`);
		}
	}
	private readString(cursor: { at: N }): S {
		const length = this.readI32(cursor);
		const bytes = this.bytes.subarray(cursor.at, cursor.at + length);

		cursor.at += length;

		return new TextDecoder().decode(bytes);
	}
	private readArray(cursor: { at: N }): U[] {
		const length = this.readI32(cursor);
		const out: U[] = new Array(length);

		for (let i = 0; i < length; i++) {
			out[i] = this.readValue(cursor);
		}

		return out;
	}
	private readObject(cursor: { at: N }): Record<S, U> {
		const length = this.readI32(cursor);
		const out: Record<S, U> = {};

		for (let i = 0; i < length; i++) {
			const key = this.readString(cursor);
			out[key] = this.readValue(cursor);
		}

		return out;
	}
	private readBytes(cursor: { at: N }): Uint8Array {
		const length = this.readI32(cursor);
		const out = this.bytes.slice(cursor.at, cursor.at + length);

		cursor.at += length;

		return out;
	}
}



if (FIGHT === "SIGNAL_ENGINE") {
    /* ------------------------------------------------------------------ *
     * SIGNAL
     * ------------------------------------------------------------------ */

    const readSignal = signal(1);

    bench("SIGNAL_ENGINE.signal.get", () => {
        readSignal.get();
    });

    bench("SIGNAL_ENGINE.signal.peek", () => {
        readSignal.peek();
    });

    let writeValue = 0;
    bench("SIGNAL_ENGINE.signal.set", () => {
        readSignal.set(++writeValue);
    });

    bench("SIGNAL_ENGINE.signal.get.set", () => {
        readSignal.get();
        readSignal.set(++writeValue);
    });

    /* ------------------------------------------------------------------ *
     * COMPUTED
     * ------------------------------------------------------------------ */

    const computedSource = signal(1);
    const cachedComputed = computed(() => computedSource.get() * 2);

    cachedComputed.get();

    bench("SIGNAL_ENGINE.computed.get.cached", () => {
        cachedComputed.get();
    });

    bench("SIGNAL_ENGINE.computed.invalidate", () => {
        computedSource.set(++writeValue);
    });

    bench("SIGNAL_ENGINE.computed.recompute", () => {
        computedSource.set(++writeValue);
        cachedComputed.get();
    });

    const chainSource = signal(1);
    const chain1 = computed(() => chainSource.get() + 1);
    const chain2 = computed(() => chain1.get() + 1);
    const chain3 = computed(() => chain2.get() + 1);

    chain3.get();

    bench("SIGNAL_ENGINE.computed.chain.cached", () => {
        chain3.get();
    });

    bench("SIGNAL_ENGINE.computed.chain.recompute", () => {
        chainSource.set(++writeValue);
        chain3.get();
    });

    /* ------------------------------------------------------------------ *
     * EFFECT
     * ------------------------------------------------------------------ */

    let effectValue = 0;

    bench("SIGNAL_ENGINE.effect.create-dispose", () => {
        const dispose = effect(() => {
            effectValue++;
        });

        dispose();
    });

    const notifySignal = signal(0);
    let notifyCount = 0;

    const disposeNotify = effect(() => {
        notifySignal.get();
        notifyCount++;
    });

    bench("SIGNAL_ENGINE.effect.notify", () => {
        batch(() => {
            notifySignal.set(++writeValue);
        });
    });

    disposeNotify();

    const cleanupSignal = signal(0);
    let cleanupCount = 0;

    const disposeCleanup = effect(() => {
        cleanupSignal.get();

        return () => {
            cleanupCount++;
        };
    });

    bench("SIGNAL_ENGINE.effect.cleanup-rerun", () => {
        batch(() => {
            cleanupSignal.set(++writeValue);
        });
    });

    disposeCleanup();

    /* ------------------------------------------------------------------ *
     * DEPENDENCY TRACKING
     * ------------------------------------------------------------------ */

    const dependencySource = signal(1);
    let dependencyResult = 0;

    const disposeDependency = effect(() => {
        dependencyResult = dependencySource.get();
    });

    bench("SIGNAL_ENGINE.dependency.track", () => {
        batch(() => {
            dependencySource.set(++writeValue);
        });
    });

    disposeDependency();

    /* ------------------------------------------------------------------ *
     * FAN-OUT
     * ------------------------------------------------------------------ */

    const fanout16 = signal(0);
    const fanout16Disposers: (() => void)[] = [];

    for (let i = 0; i < 16; i++) {
        fanout16Disposers.push(
            effect(() => {
                fanout16.get();
            }),
        );
    }

    bench("SIGNAL_ENGINE.fanout.16", () => {
        batch(() => {
            fanout16.set(++writeValue);
        });
    });

    for (const dispose of fanout16Disposers) dispose();

    const fanout64 = signal(0);
    const fanout64Disposers: (() => void)[] = [];

    for (let i = 0; i < 64; i++) {
        fanout64Disposers.push(
            effect(() => {
                fanout64.get();
            }),
        );
    }

    bench("SIGNAL_ENGINE.fanout.64", () => {
        batch(() => {
            fanout64.set(++writeValue);
        });
    });

    for (const dispose of fanout64Disposers) dispose();

    /* ------------------------------------------------------------------ *
     * BATCHING
     * ------------------------------------------------------------------ */

    const batchA = signal(0);
    const batchB = signal(0);
    const batchC = signal(0);

    bench("SIGNAL_ENGINE.batch.3-writes", () => {
        batch(() => {
            batchA.set(++writeValue);
            batchB.set(++writeValue);
            batchC.set(++writeValue);
        });
    });

    const batchSource = signal(0);
    let batchEffectCount = 0;

    const disposeBatch = effect(() => {
        batchSource.get();
        batchEffectCount++;
    });

    bench("SIGNAL_ENGINE.batch.effect.3-writes", () => {
        batch(() => {
            batchSource.set(++writeValue);
            batchSource.set(++writeValue);
            batchSource.set(++writeValue);
        });
    });

    disposeBatch();

    /* ------------------------------------------------------------------ *
     * GRAPH
     * ------------------------------------------------------------------ */

    const diamondSource = signal(1);
    const diamondLeft = computed(() => diamondSource.get() + 1);
    const diamondRight = computed(() => diamondSource.get() + 2);
    const diamondResult = computed(
        () => diamondLeft.get() + diamondRight.get(),
    );

    diamondResult.get();

    bench("SIGNAL_ENGINE.graph.diamond.cached", () => {
        diamondResult.get();
    });

    bench("SIGNAL_ENGINE.graph.diamond.recompute", () => {
        diamondSource.set(++writeValue);
        diamondResult.get();
    });

    const branchSource = signal(1);
    const branches = Array.from(
        { length: 16 },
        (_, i) => computed(() => branchSource.get() + i),
    );
    const branchResult = computed(
        () => branches.reduce((total, branch) => total + branch.get(), 0),
    );

    branchResult.get();

    bench("SIGNAL_ENGINE.graph.branch.16.cached", () => {
        branchResult.get();
    });

    bench("SIGNAL_ENGINE.graph.branch.16.recompute", () => {
        branchSource.set(++writeValue);
        branchResult.get();
    });

    /* ------------------------------------------------------------------ *
     * KEEP FIXTURES LIVE
     * ------------------------------------------------------------------ */

    void effectValue;
    void cleanupCount;
    void notifyCount;
    void dependencyResult;
    void batchEffectCount;
}





