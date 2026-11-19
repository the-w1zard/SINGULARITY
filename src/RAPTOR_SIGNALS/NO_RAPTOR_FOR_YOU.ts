// bus-bench.ts
// FIGHT=... bun --expose-gc bus-bench.ts
import { DARTH_BINKS_RAPTOR } from './DARTH_BINKS_RAPTOR'
import { DARTH_JAR_JAR_BINKS_RAPTOR } from './DARTH_JAR_JAR_BINKS_RAPTOR'
import { DARTH_MAUL_ENGINE } from './DARTH_MAUL_ENGINE'
import { MAGE_RAPTOR } from './MAGE_RAPTOR'
import { MAREK_RAPTOR } from './MAREK_RAPTOR'
import { PALPATINE } from './PALPATINE'
import { SIGNALS_DISRUPTOR } from './SIGNALS_DISRUPTOR'
import { SIGNALS_RAPTOR } from './SIGNALS_RAPTOR'
import { WARLOCK_NGIN } from './WARLOCK_NGIN'
import { WARLOCK_RAPTOR } from './WARLOCK_RAPTOR'
import { BINARY_REACTOR2 } from './numeric_array/BINARY_REACTOR2'
import { BINARY_REACTOR } from './numeric_array/BINARY_REACTOR'
import {  getActiveSub, setActiveSub, getBatchDepth, startBatch, endBatch, isSignal, isComputed, isEffect, isEffectScope, signal, computed, effect, effectScope, trigger, } from './signals_alien'
import {	signal as preact_signal, 	computed as preact_computed, 	batch as preact_batch, 	effect as preact_effect, 	action, 	createModel, 	type Model, 	type ModelConstructor, 	type ModelFactory, 	Signal, 	type ReadonlySignal, 	untracked as preact_untracked, 	type SignalOptions, 	type EffectOptions, } from "@preact/signals-core";





const CAPACITY = 1 << 18;
const DURATION = 12;
const REPORT_MS = 12;
const BATCH_SIZE = 100_000; // 500_000
const TOTAL_BATCHES = 100; // 500
const RUN_BARE_METAL_CPU_TEST: Boolean = true;

const pid = 1;
let seq = 0;
const v = ++seq;

type fighter =
| "DARTH_BINKS_RAPTOR"
| "DARTH_JAR_JAR_BINKS_RAPTOR"
| "DARTH_MAUL_ENGINE"
| "MAGE_RAPTOR"
| "MAREK_RAPTOR"
| "PALPATINE"
| "SIGNALS_DISRUPTOR"
| "SIGNALS_RAPTOR"
| "WARLOCK_NGIN"
| "WARLOCK_RAPTOR"
| "BINARY_REACTOR2"
| "BINARY_REACTOR"
| "alien"
| "preact"

const FIGHT: fighter = (process.env.FIGHT as fighter) ?? "DARTH_BINKS_RAPTOR";


type SIG = { r: S; m: S; s: S; k: S; p: U };
type H = (p: U, msg?: SIG) => number;
type BUS_CHANNEL = { handler?: H };

const r = "THE_WIZARD_ROOT_REG";
const m = "SAURON_MASTER_REG";
const s = "SAURON_SLAVE_REG";
const k0 = "__SAURON__";

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
}

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
if (FIGHT === "DARTH_BINKS_RAPTOR") {
    // 1. SIGNAL READ
	{
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const s = d.signal(v);
		let result = 0;

		bench("SIGNAL_READ", () => {
			result = s();
		});

		if (result !== v) throw new Error("SIGNAL_READ failed");
	}

	// 2. SIGNAL WRITE
	{
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const s = d.signal(0);
		let next = 0;

		bench("SIGNAL_WRITE", () => {
			s.write(++next);
		});

		if (s() !== next) throw new Error("SIGNAL_WRITE failed");
	}

	// 3. WRITE + NOTIFY
	{
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const s = d.signal(0);
		let notifications = 0;
		let next = 0;

		d.effect(() => {
			s();
			notifications++;
		});

		notifications = 0;

		bench("WRITE_NOTIFY", () => {
			s.write(++next);
		});

		if (notifications !== next) throw new Error("WRITE_NOTIFY failed");
	}

	// 4. FAN-OUT
	for (const size of [10, 100, 1_000, 10_000]) {
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const s = d.signal(0);
		let notifications = 0;
		let next = 0;

		for (let i = 0; i < size; i++) {
			d.effect(() => {
				s();
				notifications++;
			});
		}

		notifications = 0;

		bench(`FAN_OUT_${size}`, () => {
			s.write(++next);
		});

		if (notifications !== next * size) {
			throw new Error(`FAN_OUT_${size} failed`);
		}
	}

	// 5. FAN-IN
	{
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const inputs = Array.from({ length: 100 }, () => d.signal(0));

		const total = d.computed(() => {
			let sum = 0;

			for (const input of inputs) {
				sum += input();
			}

			return sum;
		});

		total();

		let next = 0;
		let result = 0;

		bench("FAN_IN_100", () => {
			const value = ++next;

			inputs[value % inputs.length].write(value);
			result = total();
		});

		if (result !== next) throw new Error("FAN_IN failed");
	}

	// 6. DEPENDENCY CHAIN
	for (const size of [10, 100, 500]) {
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const source = d.signal(0);

		let previous = source;

		for (let i = 0; i < size; i++) {
			const dependency = previous;
			previous = d.computed(() => dependency() + 1);
		}

		const resultSignal = previous;

		resultSignal();

		let next = 0;
		let result = 0;

		bench(`DEPENDENCY_CHAIN_${size}`, () => {
			source.write(++next);
			result = resultSignal();
		});

		if (result !== next + size) {
			throw new Error(`DEPENDENCY_CHAIN_${size} failed`);
		}
	}

	// 7. DEPENDENCY GRAPH
	{
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const source = d.signal(0);

		const left = d.computed(() => source() * 2);
		const right = d.computed(() => source() * 3);
		const merged = d.computed(() => left() + right());

		merged();

		let next = 0;
		let result = 0;

		bench("DEPENDENCY_GRAPH", () => {
			source.write(++next);
			result = merged();
		});

		if (result !== next * 5) {
			throw new Error("DEPENDENCY_GRAPH failed");
		}
	}

	// 8. BATCH WRITES
	{
		const d = new DARTH_BINKS_RAPTOR(CAPACITY);
		const inputs = Array.from({ length: 100 }, () => d.signal(0));

		const total = d.computed(() => {
			let sum = 0;

			for (const input of inputs) {
				sum += input();
			}

			return sum;
		});

		total();

		let next = 0;
		let result = 0;

		bench("BATCH_WRITES_100", () => {
			const value = ++next;

			d.batch(() => {
				for (let i = 0; i < inputs.length; i++) {
					inputs[i].write(value + i);
				}
			});

			result = total();
		});

		const expected = 100 * next + 4950;

		if (result !== expected) {
			throw new Error("BATCH_WRITES_100 failed");
		}
	}

	// 9. SUBSCRIBE / UNSUBSCRIBE
	// Not benchmarked: effect disposal does not reclaim its PID.
	// Repeated registration/disposal would exhaust the disruptor.

	// 10. EFFECTS
	{
		const size = 100;
		const d = new DARTH_BINKS_RAPTOR(1024);
		const s = d.signal(0);

		let executions = 0;
		let cleanups = 0;
		let next = 0;

		for (let i = 0; i < size; i++) {
			d.effect(() => {
				s();
				executions++;

				return () => {
					cleanups++;
				};
			});
		}

		executions = 0;
		cleanups = 0;

		bench("EFFECTS_100", () => {
			s.write(++next);
		});

		if (executions !== next * size || cleanups !== next * size) {
			throw new Error("EFFECTS_100 failed");
		}
	}

	// 11. DOM BINDING
	// Not benchmarked: DARTH_BINKS_RAPTOR currently exposes no DOM binding API.
}
else if (FIGHT === "DARTH_JAR_JAR_BINKS_RAPTOR") {

  // 1. SIGNAL READ
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const s = d.signal(1);

      bench("RAPTOR.signal.read", () => {
          d.signalOper<number>(s);
      });
  }

  // 2. SIGNAL WRITE
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const s = d.signal(0);
      let value = 0;

      bench("RAPTOR.signal.write", () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 3. WRITE + NOTIFICATION
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const s = d.signal(0);
      let notifications = 0;
      let value = 0;

      d.effectSignal(() => {
          d.signalOper<number>(s);
          notifications++;
      });

      bench("RAPTOR.signal.write-notify", () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 4. FAN-OUT
  for (const count of [10, 100, 1_000, 10_000]) {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const s = d.signal(0);
      let notifications = 0;
      let value = 0;

      for (let i = 0; i < count; i++) {
          d.effectSignal(() => {
              d.signalOper<number>(s);
              notifications++;
          });
      }

      bench(`RAPTOR.fan-out.${count}`, () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 5. FAN-IN
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const inputs = Array.from({ length: 100 }, () => d.signal(0));

      const total = d.computed(() => {
          let sum = 0;

          for (let i = 0; i < inputs.length; i++) {
              sum += d.signalOper<number>(inputs[i])!;
          }

          return sum;
      });

      d.computedOper(total);

      let value = 0;

      bench("RAPTOR.fan-in.100", () => {
          d.signalOper(inputs[0], value ^= 1);
          d.computedOper(total);
      });
  }

  // 6. DEPENDENCY CHAIN
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const source = d.signal(0);
      let previous = source;

      for (let i = 0; i < 128; i++) {
          const dependency = previous;

          previous = d.computed(() => {
              return d.isSignal(dependency)
                  ? d.signalOper<number>(dependency)! + 1
                  : d.computedOper<number>(dependency) + 1;
          });
      }

      const tail = previous;
      d.computedOper(tail);

      let value = 0;

      bench("RAPTOR.dependency-chain.128", () => {
          d.signalOper(source, value ^= 1);
          d.computedOper(tail);
      });
  }

  // 7. DEPENDENCY GRAPH
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const inputs = Array.from({ length: 64 }, () => d.signal(0));

      const branches: number[] = [];

      for (let i = 0; i < 16; i++) {
          const offset = i * 4;

          branches.push(d.computed(() =>
              d.signalOper<number>(inputs[offset])! +
              d.signalOper<number>(inputs[offset + 1])! +
              d.signalOper<number>(inputs[offset + 2])! +
              d.signalOper<number>(inputs[offset + 3])!
          ));
      }

      const root = d.computed(() => {
          let sum = 0;

          for (let i = 0; i < branches.length; i++) { sum += d.computedOper(branches[i]); }

          return sum;
      });

      d.computedOper(root);

      let value = 0;

      bench("RAPTOR.dependency-graph.64x16", () => {
          d.signalOper(inputs[0], value ^= 1);
          d.computedOper(root);
      });
  }

  // 8. BATCH WRITES
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const signals = Array.from({ length: 100 }, () => d.signal(0));
      let value = 0;

      bench("RAPTOR.batch.100", () => {
          value ^= 1;

          d.batch(() => {
              for (let i = 0; i < signals.length; i++) {
                  d.signalOper(signals[i], value);
              }
          });
      });
  }

  // 9. SUBSCRIBE / UNSUBSCRIBE CHURN
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const s = d.signal(1);

      bench("RAPTOR.subscription.churn", () => {
          const effect = d.effectSignal(() => {
              d.signalOper<number>(s);

              return () => {};
          });

          d.dispose(effect);
      });
  }

  // 10. EFFECT EXECUTION + CLEANUP
  {
      const d = new DARTH_JAR_JAR_BINKS_RAPTOR(CAPACITY);
      const s = d.signal(0);

      let executions = 0;
      let cleanups = 0;
      let value = 0;

      d.effectSignal(() => {
          d.signalOper<number>(s);
          executions++;

          return () => {
              cleanups++;
          };
      });

      bench("RAPTOR.effect.cleanup", () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 11. DOM BINDING
  // Not exposed by DARTH_JAR_JAR_BINKS_RAPTOR.
  // No DOM benchmark can be measured against this implementation.
}
else if (FIGHT === "DARTH_MAUL_ENGINE") {

  // 1. SIGNAL READ
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const s = d.signal(1);

      bench("MAUL.signal.read", () => {
          d.signalOper<number>(s);
      });
  }

  // 2. SIGNAL WRITE
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const s = d.signal(0);
      let value = 0;

      bench("MAUL.signal.write", () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 3. WRITE + NOTIFICATION
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const s = d.signal(0);
      let value = 0;
      let runs = 0;

      d.effectSignal(() => {
          d.signalOper<number>(s);
          runs++;
      });

      bench("MAUL.signal.write-notify", () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 4. FAN-OUT
  for (const count of [10, 100, 1_000, 10_000]) {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const s = d.signal(0);
      let value = 0;

      for (let i = 0; i < count; i++) {
          d.effectSignal(() => {
              d.signalOper<number>(s);
          });
      }

      bench(`MAUL.fan-out.${count}`, () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 5. FAN-IN
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const inputs = Array.from({ length: 100 }, () => d.signal(0));

      const total = d.computed(() => {
          let sum = 0;

          for (let i = 0; i < inputs.length; i++) {
              sum += d.signalOper<number>(inputs[i])!;
          }

          return sum;
      });

      d.computedOper(total);
      let value = 0;

      bench("MAUL.fan-in.100", () => {
          d.signalOper(inputs[0], value ^= 1);
          d.computedOper(total);
      });
  }

  // 6. DEPENDENCY CHAIN
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const source = d.signal(0);
      let previous = source;

      for (let i = 0; i < 128; i++) {
          const dependency = previous;

          previous = d.computed(() =>
              d.isSignal(dependency)
                  ? d.signalOper<number>(dependency)! + 1
                  : d.computedOper<number>(dependency) + 1
          );
      }

      const tail = previous;
      d.computedOper(tail);
      let value = 0;

      bench("MAUL.dependency-chain.128", () => {
          d.signalOper(source, value ^= 1);
          d.computedOper(tail);
      });
  }

  // 7. DEPENDENCY GRAPH
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const inputs = Array.from({ length: 64 }, () => d.signal(0));
      const branches: number[] = [];

      for (let i = 0; i < 16; i++) {
          const offset = i * 4;

          branches.push(d.computed(() =>
              d.signalOper<number>(inputs[offset])! +
              d.signalOper<number>(inputs[offset + 1])! +
              d.signalOper<number>(inputs[offset + 2])! +
              d.signalOper<number>(inputs[offset + 3])!
          ));
      }

      const root = d.computed(() => {
          let sum = 0;

          for (let i = 0; i < branches.length; i++) {
              sum += d.computedOper(branches[i]);
          }

          return sum;
      });

      d.computedOper(root);
      let value = 0;

      bench("MAUL.dependency-graph.64x16", () => {
          d.signalOper(inputs[0], value ^= 1);
          d.computedOper(root);
      });
  }

  // 8. BATCH WRITES
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const signals = Array.from({ length: 100 }, () => d.signal(0));
      let value = 0;

      bench("MAUL.batch.100", () => {
          value ^= 1;
          d.startBatch();

          for (let i = 0; i < signals.length; i++) {
              d.signalOper(signals[i], value);
          }

          d.endBatch();
      });
  }

  // 9. SUBSCRIBE / UNSUBSCRIBE CHURN
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const s = d.signal(1);

      bench("MAUL.subscription.churn", () => {
          const effect = d.effectSignal(() => {
              d.signalOper<number>(s);
          });

          d.dispose(effect);
      });
  }

  // 10. EFFECT EXECUTION
  {
      const d = new DARTH_MAUL_ENGINE(null, CAPACITY);
      const s = d.signal(0);
      let value = 0;

      d.effectSignal(() => {
          d.signalOper<number>(s);
      });

      bench("MAUL.effect.execution", () => {
          d.signalOper(s, value ^= 1);
      });
  }

  // 11. DOM BINDING
  // DARTH_MAUL_ENGINE does not expose a DOM binding API.
}
else if (FIGHT === "MAGE_RAPTOR") {
  const CAPACITY = 1 << 15;

  // 1. Signal read
  {
    const d = new MAGE_RAPTOR(CAPACITY);
    const pid = d.signal(42);

    bench("MAGE_RAPTOR.signal.read", () => {
      d.signalOper<number>(pid);
    });
  }

  // 2. Signal write
  {
    const d = new MAGE_RAPTOR(CAPACITY);
    const pid = d.signal(0);
    let value = 0;

    bench("MAGE_RAPTOR.signal.write", () => {
      value ^= 1;
      d.signalOper(pid, value);
    });
  }

  // 3. Write + single-effect notification
  {
    const d = new MAGE_RAPTOR(CAPACITY);
    const pid = d.signal(0);
    let value = 0;
    let notifications = 0;

    d.effectSignal(() => {
      d.signalOper<number>(pid);
      notifications++;
    });

    bench("MAGE_RAPTOR.write.notify", () => {
      value ^= 1;
      d.signalOper(pid, value);
    });
  }

  // 4. Fan-out: one signal notifying 10, 100, 1k, and 10k effects
  {
    for (const count of [10, 100, 1_000, 10_000]) {
      const d = new MAGE_RAPTOR(CAPACITY);
      const pid = d.signal(0);
      let value = 0;
      let notifications = 0;

      for (let i = 0; i < count; i++) {
        d.effectSignal(() => {
          d.signalOper<number>(pid);
          notifications++;
        });
      }

      bench(`MAGE_RAPTOR.fanout.${count}`, () => {
        value ^= 1;
        d.signalOper(pid, value);
      });
    }
  }

  // 5. Fan-in: many signals feeding one computed
  {
    const COUNT = 1_024;
    const d = new MAGE_RAPTOR(CAPACITY);
    const signals = new Int32Array(COUNT);

    for (let i = 0; i < COUNT; i++) {
      signals[i] = d.signal(0);
    }

    const total = d.computed(() => {
      let sum = 0;

      for (let i = 0; i < COUNT; i++) {
        sum += d.signalOper<number>(signals[i]) as number;
      }

      return sum;
    });

    d.computedOper<number>(total);

    let value = 0;

    bench("MAGE_RAPTOR.fanin.1024", () => {
      d.batch(() => {
        value ^= 1;

        for (let i = 0; i < COUNT; i++) {
          d.signalOper(signals[i], value);
        }
      });

      d.computedOper<number>(total);
    });
  }

  // 6. Dependency chain: 1,024 computed nodes
  {
    const DEPTH = 1_024;
    const d = new MAGE_RAPTOR(CAPACITY);
    const base = d.signal(0);

    let previous = base;

    for (let i = 0; i < DEPTH; i++) {
      const source = previous;

      previous = d.computed(() => {
        const value = d.isSignal(source)
          ? d.signalOper<number>(source) as number
          : d.computedOper<number>(source);

        return value + 1;
      });
    }

    const result = previous;
    d.computedOper<number>(result);

    let value = 0;

    bench("MAGE_RAPTOR.dependency.chain.1024", () => {
      value ^= 1;
      d.signalOper(base, value);
      d.computedOper<number>(result);
    });
  }

  // 7. Dependency graph: layered fan-in / fan-out / join
  {
    const d = new MAGE_RAPTOR(CAPACITY);
    const sources = new Int32Array(64);

    for (let i = 0; i < sources.length; i++) {
      sources[i] = d.signal(0);
    }

    const layer1 = new Int32Array(32);

    for (let i = 0; i < layer1.length; i++) {
      const offset = i * 2;

      layer1[i] = d.computed(() => {
        let sum = 0;

        for (let j = 0; j < 8; j++) {
          sum += d.signalOper<number>(
            sources[(offset + j) % sources.length],
          ) as number;
        }

        return sum;
      });
    }

    const layer2 = new Int32Array(8);

    for (let i = 0; i < layer2.length; i++) {
      const offset = i * 4;

      layer2[i] = d.computed(() => {
        let sum = 0;

        for (let j = 0; j < 4; j++) {
          sum += d.computedOper<number>(
            layer1[(offset + j) % layer1.length],
          );
        }

        return sum;
      });
    }

    const result = d.computed(() => {
      let sum = 0;

      for (let i = 0; i < layer2.length; i++) {
        sum += d.computedOper<number>(layer2[i]);
      }

      return sum;
    });

    d.computedOper<number>(result);

    let value = 0;

    bench("MAGE_RAPTOR.dependency.graph", () => {
      d.batch(() => {
        value ^= 1;

        for (let i = 0; i < 8; i++) {
          d.signalOper(sources[i], value);
        }
      });

      d.computedOper<number>(result);
    });
  }

  // 8. Batch writes: 256 independent signals
  {
    const COUNT = 256;
    const d = new MAGE_RAPTOR(CAPACITY);
    const signals = new Int32Array(COUNT);
    const values = new Int32Array(COUNT);

    for (let i = 0; i < COUNT; i++) {
      signals[i] = d.signal(0);
    }

    bench("MAGE_RAPTOR.batch.write.256", () => {
      d.batch(() => {
        for (let i = 0; i < COUNT; i++) {
          values[i] ^= 1;
          d.signalOper(signals[i], values[i]);
        }
      });
    });
  }

  // 9. Subscribe / unsubscribe churn
  {
    const d = new MAGE_RAPTOR(CAPACITY);
    const pid = d.signal(0);

    bench("MAGE_RAPTOR.subscription.churn", () => {
      const effect = d.effectSignal(() => {
        d.signalOper<number>(pid);
      });

      d.dispose(effect);
    });
  }

  // 10. Effect execution: one effect tracking 64 signals
  {
    const COUNT = 64;
    const d = new MAGE_RAPTOR(CAPACITY);
    const signals = new Int32Array(COUNT);

    for (let i = 0; i < COUNT; i++) {
      signals[i] = d.signal(0);
    }

    let executions = 0;
    let result = 0;

    d.effectSignal(() => {
      let sum = 0;

      for (let i = 0; i < COUNT; i++) {
        sum += d.signalOper<number>(signals[i]) as number;
      }

      result = sum;
      executions++;
    });

    let value = 0;
    let index = 0;

    bench("MAGE_RAPTOR.effects.64-dependencies", () => {
      value ^= 1;
      d.signalOper(signals[index], value);
      index = (index + 1) & (COUNT - 1);
    });
  }

  // 11. DOM binding
  // Unsupported: MAGE_RAPTOR exposes no DOM-binding API.
}
else if (FIGHT === "MAREK_RAPTOR") {
	const payload = { p: 1 };
	const channel = { r: "BENCH_ROOT", m: "BENCH_MASTER", s: "BENCH_SLAVE", k: "BENCH_CHANNEL" };

	// 1. Channel read
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		b.set(channel, payload);
		let result: any;

		bench("MAREK_RAPTOR.READ", () => {
			result = b.get(channel);
		});
	}

	// 2. Channel write, no subscribers
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		let value = 0;

		bench("MAREK_RAPTOR.WRITE", () => {
			b.set(channel, value++);
		});
	}

	// 3. Fan-out: 10 subscribers
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		for (let i = 0; i < 10; i++) b.sub(channel, () => {});

		let value = 0;
		bench("MAREK_RAPTOR.FANOUT_10", () => {
			b.set(channel, value++);
		});
	}

	// 4. Fan-out: 100 subscribers
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		for (let i = 0; i < 100; i++) b.sub(channel, () => {});

		let value = 0;
		bench("MAREK_RAPTOR.FANOUT_100", () => {
			b.set(channel, value++);
		});
	}

	// 5. Fan-out: 1,000 subscribers
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		for (let i = 0; i < 1_000; i++) b.sub(channel, () => {});

		let value = 0;
		bench("MAREK_RAPTOR.FANOUT_1K", () => {
			b.set(channel, value++);
		});
	}

	// 6. Multi-channel dispatch
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		const channels = Array.from({ length: 1_000 }, (_, i) => ({
			r: channel.r,
			m: channel.m,
			s: channel.s,
			k: `CHANNEL_${i}`,
		}));

		for (const c of channels) b.sub(c, () => {});

		let index = 0;
		let value = 0;

		bench("MAREK_RAPTOR.MULTI_CHANNEL", () => {
			b.set(channels[index++ % channels.length], value++);
		});
	}

	// 7. Send dispatch
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		b.sub(channel, () => {});

		let value = 0;
		bench("MAREK_RAPTOR.SEND", () => {
			b.send(channel, value++, { reply: false });
		});
	}

	// 8. Stream dispatch
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		b.stream(channel, () => {});

		let value = 0;
		bench("MAREK_RAPTOR.STREAM", () => {
			b.emitStream(channel, value++);
		});
	}

	// 9. Duplex emit
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		const d = b.duplex(channel, () => {});

		let value = 0;
		bench("MAREK_RAPTOR.DUPLEX_EMIT", () => {
			d.emit(value++);
		});

		d.close();
	}

	// 10. Effect dependency tracking and rerun
	{
		const b = new MAREK_RAPTOR(CAPACITY);
		let result: any;

		const dispose = b.effect(() => {
			result = b.get(channel);
		});

		let value = 0;
		bench("MAREK_RAPTOR.EFFECT", () => {
			b.set(channel, value++);
		});

		dispose();
	}

	// 11. Subscribe / unsubscribe churn
	{
		const b = new MAREK_RAPTOR(CAPACITY);

		bench("MAREK_RAPTOR.SUB_CHURN", () => {
			const dispose = b.sub(channel, () => {});
			dispose();
		});
	}
}
else if (FIGHT === "PALPATINE") {
	// 1. SIGNAL READ
	{
		const b = new PALPATINE(CAPACITY);
		const pid = b.signal(1);
		let result = 0;

		bench("PALPATINE.SIGNAL_READ", () => {
			result = b.signalOper<number>(pid)!;
		});
	}

	// 2. SIGNAL WRITE
	{
		const b = new PALPATINE(CAPACITY);
		const pid = b.signal(0);
		let value = 0;

		bench("PALPATINE.SIGNAL_WRITE", () => {
			b.signalOper(pid, ++value);
		});
	}

	// 3. SIGNAL WRITE + SINGLE EFFECT NOTIFICATION
	{
		const b = new PALPATINE(CAPACITY);
		const pid = b.signal(0);
		let result = 0;

		const effect = b.effectSignal(() => {
			result = b.signalOper<number>(pid)!;
		});

		let value = 0;

		bench("PALPATINE.WRITE_NOTIFY", () => {
			b.signalOper(pid, ++value);
		});

		b.dispose(effect);
	}

	// 4. FAN-OUT: 10 EFFECT SUBSCRIBERS
	{
		const b = new PALPATINE(CAPACITY);
		const pid = b.signal(0);
		let result = 0;

		const effects = new Array<Pid>(10);

		for (let i = 0; i < effects.length; i++) {
			effects[i] = b.effectSignal(() => {
				result += b.signalOper<number>(pid)!;
			});
		}

		let value = 0;

		bench("PALPATINE.FANOUT_10", () => {
			b.signalOper(pid, ++value);
		});

		for (const effect of effects) b.dispose(effect);
	}

	// 5. FAN-IN: 32 SIGNALS -> ONE COMPUTED
	{
		const b = new PALPATINE(CAPACITY);
		const signals = Array.from({ length: 32 }, () => b.signal(0));

		const computed = b.computed(() => {
			let total = 0;

			for (const pid of signals) {
				total += b.signalOper<number>(pid)!;
			}

			return total;
		});

		b.computedOper<number>(computed);

		let value = 0;
		let result = 0;
		let index = 0;

		bench("PALPATINE.FAN_IN_32", () => {
			const pid = signals[index++ & 31];
			b.signalOper(pid, ++value);
			result = b.computedOper<number>(computed);
		});
	}

	// 6. DEPENDENCY CHAIN: 64 COMPUTED NODES
	{
		const b = new PALPATINE(CAPACITY);
		const root = b.signal(0);

		let previous = root;

		for (let i = 0; i < 64; i++) {
			const dep = previous;

			previous = b.computed(() => {
				return b.signalOper<number>(dep)! + 1;
			});
		}

		const last = previous;
		b.computedOper<number>(last);

		let value = 0;
		let result = 0;

		bench("PALPATINE.DEPENDENCY_CHAIN_64", () => {
			b.signalOper(root, ++value);
			result = b.computedOper<number>(last);
		});
	}

	// 7. DEPENDENCY GRAPH: 16 BRANCHES -> ONE AGGREGATE
	{
		const b = new PALPATINE(CAPACITY);
		const root = b.signal(0);
		const branches: Pid[] = [];

		for (let i = 0; i < 16; i++) {
			branches.push(
				b.computed(() => {
					return b.signalOper<number>(root)! + i;
				}),
			);
		}

		const aggregate = b.computed(() => {
			let total = 0;

			for (const pid of branches) {
				total += b.computedOper<number>(pid);
			}

			return total;
		});

		b.computedOper<number>(aggregate);

		let value = 0;
		let result = 0;

		bench("PALPATINE.DEPENDENCY_GRAPH_16", () => {
			b.signalOper(root, ++value);
			result = b.computedOper<number>(aggregate);
		});
	}

	// 8. BATCH: 32 SIGNAL WRITES -> ONE EFFECT FLUSH
	{
		const b = new PALPATINE(CAPACITY);
		const signals = Array.from({ length: 32 }, () => b.signal(0));
		const effects: Pid[] = [];

		let result = 0;

		for (const pid of signals) {
			effects.push(
				b.effectSignal(() => {
					result += b.signalOper<number>(pid)!;
				}),
			);
		}

		let value = 0;

		bench("PALPATINE.BATCH_32", () => {
			b.startBatch();

			for (const pid of signals) {
				b.signalOper(pid, ++value);
			}

			b.endBatch();
		});

		for (const effect of effects) b.dispose(effect);
	}

	// 9. EFFECT SUBSCRIBE / DISPOSE CHURN
	{
		const b = new PALPATINE(CAPACITY);
		const pid = b.signal(0);

		let result = 0;

		bench("PALPATINE.EFFECT_CHURN", () => {
			const effect = b.effectSignal(() => {
				result += b.signalOper<number>(pid)!;
			});

			b.dispose(effect);
		});
	}

	// 10. EFFECT EXECUTION + CLEANUP
	{
		const b = new PALPATINE(CAPACITY);
		const pid = b.signal(0);

		let executions = 0;
		let cleanups = 0;

		const effect = b.effectSignal(() => {
			b.signalOper<number>(pid)!;
			executions++;

			return () => {
				cleanups++;
			};
		});

		let value = 0;

		bench("PALPATINE.EFFECT_CLEANUP", () => {
			b.signalOper(pid, ++value);
		});

		b.dispose(effect);
	}

	// 11. DOM BINDING
	// Unsupported: PALPATINE exposes no DOM binding API.
	console.log("PALPATINE.DOM_BINDING: UNSUPPORTED");
}
else if (FIGHT === "SIGNALS_DISRUPTOR") {
	// 1. SIGNAL READ
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const pid = b.signal(1);
		let result = 0;

		bench("SIGNALS_DISRUPTOR.SIGNAL_READ", () => {
			result = b.signalOper<number>(pid)!;
		});
	}

	// 2. SIGNAL WRITE
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const pid = b.signal(0);
		let value = 0;

		bench("SIGNALS_DISRUPTOR.SIGNAL_WRITE", () => {
			b.signalOper(pid, ++value);
		});
	}

	// 3. WRITE + EFFECT NOTIFICATION
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		b.effectSignal(() => {
			result = b.signalOper<number>(source)!;
		});

		let value = 0;

		bench("SIGNALS_DISRUPTOR.WRITE_NOTIFY", () => {
			b.signalOper(source, ++value);
		});
	}

	// 4. FAN-OUT: ONE SIGNAL -> 10 EFFECTS
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		for (let i = 0; i < 10; i++) {
			b.effectSignal(() => {
				result += b.signalOper<number>(source)!;
			});
		}

		let value = 0;

		bench("SIGNALS_DISRUPTOR.FANOUT_10", () => {
			b.signalOper(source, ++value);
		});
	}

	// 5. FAN-IN: 32 SIGNALS -> ONE COMPUTED
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const signals = Array.from({ length: 32 }, (_, i) => b.signal(i));
		const total = b.computed(() => {
			let sum = 0;
			for (let i = 0; i < 32; i++) {
				sum += b.signalOper<number>(signals[i])!;
			}
			return sum;
		});

		let index = 0;
		let value = 32;

		bench("SIGNALS_DISRUPTOR.FANIN_32", () => {
			b.signalOper(signals[index++ & 31], ++value);
			b.computedOper<number>(total);
		});
	}

	// 6. DEPENDENCY CHAIN: 64 COMPUTED NODES
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const root = b.signal(0);
		let previous = root;

		for (let i = 0; i < 64; i++) {
			const dep = previous;
			previous = b.computed(() => b.signalOper<number>(dep)! + 1);
		}

		const leaf = previous;
		let value = 0;

		bench("SIGNALS_DISRUPTOR.DEPENDENCY_CHAIN_64", () => {
			b.signalOper(root, ++value);
			b.computedOper<number>(leaf);
		});
	}

	// 7. DEPENDENCY GRAPH: 16 BRANCHES -> ONE AGGREGATE
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const root = b.signal(0);

		const branches = Array.from({ length: 16 }, () =>
			b.computed(() => b.signalOper<number>(root)! + 1),
		);

		const aggregate = b.computed(() => {
			let sum = 0;
			for (let i = 0; i < branches.length; i++) {
				sum += b.computedOper<number>(branches[i]);
			}
			return sum;
		});

		let value = 0;

		bench("SIGNALS_DISRUPTOR.DEPENDENCY_GRAPH_16", () => {
			b.signalOper(root, ++value);
			b.computedOper<number>(aggregate);
		});
	}

	// 8. BATCH: 32 SIGNAL WRITES + 32 EFFECTS
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const signals = Array.from({ length: 32 }, () => b.signal(0));
		let result = 0;

		for (let i = 0; i < 32; i++) {
			const pid = signals[i];
			b.effectSignal(() => {
				result += b.signalOper<number>(pid)!;
			});
		}

		let value = 0;

		bench("SIGNALS_DISRUPTOR.BATCH_32", () => {
			b.startBatch();
			for (let i = 0; i < 32; i++) {
				b.signalOper(signals[i], ++value);
			}
			b.endBatch();
		});
	}

	// 9. EFFECT CREATION + DISPOSAL
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		bench("SIGNALS_DISRUPTOR.EFFECT_CHURN", () => {
			const pid = b.effectSignal(() => {
				result = b.signalOper<number>(source)!;
			});
			b.dispose(pid);
		});
	}

	// 10. EFFECT RERUN + CLEANUP
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const source = b.signal(0);
		let runs = 0;
		let cleanups = 0;

		b.effectSignal(() => {
			b.signalOper<number>(source);
			runs++;
			return () => {
				cleanups++;
			};
		});

		let value = 0;

		bench("SIGNALS_DISRUPTOR.EFFECT_CLEANUP", () => {
			b.signalOper(source, ++value);
		});
	}

	// 11. TRIGGER: TRANSIENT DEPENDENCY TRACKING + NOTIFICATION
	{
		const b = new SIGNALS_DISRUPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		b.effectSignal(() => {
			result = b.signalOper<number>(source)!;
		});

		bench("SIGNALS_DISRUPTOR.TRIGGER", () => {
			b.trigger(() => {
				b.signalOper<number>(source);
			});
		});
	}
}
else if (FIGHT === "SIGNALS_RAPTOR") {
	// 1. SIGNAL READ
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const pid = b.signal(1);
		let result = 0;

		bench("SIGNALS_RAPTOR.SIGNAL_READ", () => {
			result = b.signalOper<number>(pid)!;
		});
	}

	// 2. SIGNAL WRITE
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const pid = b.signal(0);
		let value = 0;

		bench("SIGNALS_RAPTOR.SIGNAL_WRITE", () => {
			b.signalOper(pid, ++value);
		});
	}

	// 3. WRITE + UPDATE
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const pid = b.signal(0);
		let value = 0;

		bench("SIGNALS_RAPTOR.WRITE_UPDATE", () => {
			b.signalOper(pid, ++value);
			b.update(pid);
		});
	}

	// 4. FAN-OUT: ONE SIGNAL -> 10 EFFECTS
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		for (let i = 0; i < 10; i++) {
			b.effectSignal(() => {
				result += b.signalOper<number>(source)!;
			});
		}

		let value = 0;

		bench("SIGNALS_RAPTOR.FANOUT_10", () => {
			b.signalOper(source, ++value);
		});
	}

	// 5. FAN-IN: 32 SIGNALS -> ONE COMPUTED
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const signals = Array.from({ length: 32 }, (_, i) => b.signal(i));

		const total = b.computed(() => {
			let sum = 0;
			for (let i = 0; i < 32; i++) {
				sum += b.signalOper<number>(signals[i])!;
			}
			return sum;
		});

		b.computedOper<number>(total);

		let index = 0;
		let value = 32;

		bench("SIGNALS_RAPTOR.FANIN_32", () => {
			b.signalOper(signals[index++ & 31], ++value);
			b.computedOper<number>(total);
		});
	}

	// 6. DEPENDENCY CHAIN: 64 COMPUTED NODES
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const root = b.signal(0);
		let previous = root;

		for (let i = 0; i < 64; i++) {
			const dep = previous;
			previous = b.computed(() => b.signalOper<number>(dep)! + 1);
		}

		const leaf = previous;
		b.computedOper<number>(leaf);

		let value = 0;

		bench("SIGNALS_RAPTOR.DEPENDENCY_CHAIN_64", () => {
			b.signalOper(root, ++value);
			b.computedOper<number>(leaf);
		});
	}

	// 7. DEPENDENCY GRAPH: 16 COMPUTED BRANCHES -> ONE AGGREGATE
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const root = b.signal(0);

		const branches = Array.from({ length: 16 }, () =>
			b.computed(() => b.signalOper<number>(root)! + 1),
		);

		const aggregate = b.computed(() => {
			let sum = 0;
			for (let i = 0; i < branches.length; i++) {
				sum += b.computedOper<number>(branches[i]);
			}
			return sum;
		});

		b.computedOper<number>(aggregate);

		let value = 0;

		bench("SIGNALS_RAPTOR.DEPENDENCY_GRAPH_16", () => {
			b.signalOper(root, ++value);
			b.computedOper<number>(aggregate);
		});
	}

	// 8. BATCH: 32 SIGNAL WRITES -> 32 EFFECTS
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const signals = Array.from({ length: 32 }, () => b.signal(0));
		let result = 0;

		for (let i = 0; i < 32; i++) {
			const pid = signals[i];
			b.effectSignal(() => {
				result += b.signalOper<number>(pid)!;
			});
		}

		let value = 0;

		bench("SIGNALS_RAPTOR.BATCH_32", () => {
			b.startBatch();

			for (let i = 0; i < 32; i++) {
				b.signalOper(signals[i], ++value);
			}

			b.endBatch();
		});
	}

	// 9. EFFECT CREATION + DISPOSAL
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		bench("SIGNALS_RAPTOR.EFFECT_CHURN", () => {
			const pid = b.effectSignal(() => {
				result = b.signalOper<number>(source)!;
			});

			b.dispose(pid);
		});
	}

	// 10. EFFECT RERUN + CLEANUP
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const source = b.signal(0);
		let runs = 0;
		let cleanups = 0;

		b.effectSignal(() => {
			b.signalOper<number>(source);
			runs++;

			return () => {
				cleanups++;
			};
		});

		let value = 0;

		bench("SIGNALS_RAPTOR.EFFECT_CLEANUP", () => {
			b.signalOper(source, ++value);
		});
	}

	// 11. TRIGGER: TRANSIENT DEPENDENCY TRACKING
	{
		const b = new SIGNALS_RAPTOR(CAPACITY);
		const source = b.signal(0);
		let result = 0;

		bench("SIGNALS_RAPTOR.TRIGGER", () => {
			b.trigger(() => {
				result = b.signalOper<number>(source)!;
			});
		});
	}
}
else if (FIGHT === "WARLOCK_NGIN") {
	const b = new WARLOCK_NGIN(CAPACITY);

	const source = b.signal(0);
	const read = b.signal(0);
	const write = b.signal(0);

	b.PASS();

	// 1. Signal read: public get + lane execution.
	bench("WARLOCK_NGIN.signal.read", () => {
		b.get(read);
		b.PASS();
	});

	// 2. Signal write: public set + lane execution.
	bench("WARLOCK_NGIN.signal.write", () => {
		b.set(write, 1);
		b.PASS();
	});

	// 3. Operation admission + execution.
	bench("WARLOCK_NGIN.OP.admit-pass", () => {
		b.OP(0, 0, 0, read, "READ");
		b.PASS();
	});

	// 4. PASS with no pending operations.
	bench("WARLOCK_NGIN.PASS.empty", () => {
		b.PASS();
	});

	// 5. Dependency link creation and removal.
	const linkSource = b.signal(0);
	const linkTarget = b.signal(0);
	b.PASS();

	bench("WARLOCK_NGIN.link-unlink", () => {
		b.link(linkSource, linkTarget);
		b.PASS();
		b.unlink(linkSource, linkTarget);
		b.PASS();
	});

	// 6. Existing channel lookup.
	const channelAddress = {
		r: "THE_WIZARD_ROOT_REG",
		m: "SAURON_MASTER_REG",
		s: "SAURON_SLAVE_REG",
		k: "__WARLOCK_BENCH__",
	};

	b.channel(channelAddress);

	bench("WARLOCK_NGIN.channel.lookup", () => {
		b.channel(channelAddress);
	});

	// 7. Subscription registration.
	const subAddress = {
		r: "THE_WIZARD_ROOT_REG",
		m: "SAURON_MASTER_REG",
		s: "SAURON_SLAVE_REG",
		k: "__WARLOCK_SUB_BENCH__",
	};

	const subHandler = () => undefined;

	bench("WARLOCK_NGIN.sub1", () => {
		b.sub1(subAddress, subHandler);
		b.PASS();
	});

	// 8. Computed execution.
	const computedPid = b.computed(() => 1);
	b.PASS();

	bench("WARLOCK_NGIN.computed", () => {
		b.OP(0, 0, 0, computedPid, "COMPUTED");
		b.PASS();
	});

	// 9. Effect execution.
	const effectPid = b.effect(() => undefined);
	b.PASS();

	bench("WARLOCK_NGIN.effect", () => {
		b.OP(0, 0, 0, effectPid, "EFFECT");
		b.PASS();
	});

	// 10. Batch-depth operation admission and execution.
	bench("WARLOCK_NGIN.batch", () => {
		b.OP(0, 0, 0, source, "START_BATCH");
		b.OP(0, 0, 0, source, "END_BATCH");
		b.PASS();
	});

	// 11. Binary codec encode.
	const codec = new BEAM_CODEC();
	const payload = {
		id: 12345,
		value: 987.654,
		active: true,
		name: "WARLOCK_NGIN",
	};

	bench("WARLOCK_NGIN.codec.encode", () => {
		codec.encode(payload);
	});

	// 12. Binary codec decode.
	const encoded = codec.encode(payload);

	bench("WARLOCK_NGIN.codec.decode", () => {
		codec.decode(encoded);
	});
}
else if (FIGHT === "WARLOCK_RAPTOR") {
	const b = new WARLOCK_RAPTOR(1 << 22);

	// 1. Signal read.
	const read = b.signal(0);

	bench("WARLOCK_RAPTOR.signal.read", () => {
		read();
	});

	// 2. Signal write.
	const write = b.signal(0);

	let writeValue = 0;

	bench("WARLOCK_RAPTOR.signal.write", () => {
		write(++writeValue);
	});

	// 3. Signal write: unchanged value.
	const unchanged = b.signal(0);

	bench("WARLOCK_RAPTOR.signal.write.unchanged", () => {
		unchanged(0);
	});

	// 4. Computed read: cached.
	const computedSource = b.signal(1);
	const computed = b.computed(() => computedSource() * 2);

	computed();

	bench("WARLOCK_RAPTOR.computed.read.cached", () => {
		computed();
	});

	// 5. Computed read: invalidated and recomputed.
	const dirtySource = b.signal(1);
	const dirtyComputed = b.computed(() => dirtySource() * 2);

	let dirtyValue = 0;

	bench("WARLOCK_RAPTOR.computed.read.dirty", () => {
		dirtySource(++dirtyValue);
		dirtyComputed();
	});

	// 6. Effect execution: initial execution.
	bench("WARLOCK_RAPTOR.effect.create", () => {
		b.effect(() => undefined);
	});

	// 7. Effect notification and execution.
	const effectSource = b.signal(0);
	let effectRuns = 0;

	b.effect(() => {
		effectSource();
		effectRuns++;
	});

	let effectValue = 0;

	bench("WARLOCK_RAPTOR.effect.notify", () => {
		effectSource(++effectValue);
	});

	// 8. Dependency tracking: one dependency.
	const trackedSource = b.signal(0);

	const trackedComputed = b.computed(() => {
		return trackedSource() + 1;
	});

	bench("WARLOCK_RAPTOR.dependency.track", () => {
		trackedComputed();
	});

	// 9. Batch: multiple writes, one scheduler pass.
	const batchA = b.signal(0);
	const batchB = b.signal(0);
	const batchC = b.signal(0);

	let batchValue = 0;

	bench("WARLOCK_RAPTOR.batch.3-writes", () => {
		const next = ++batchValue;

		b.batch(() => {
			batchA(next);
			batchB(next);
			batchC(next);
		});
	});

	// 10. Untracked signal read.
	const untrackedSource = b.signal(0);

	bench("WARLOCK_RAPTOR.untrack.read", () => {
		b.untrack(() => untrackedSource());
	});

	// 11. Spatial lane write.
	let spatialValue = 0;

	bench("WARLOCK_RAPTOR.spatial.edit", () => {
		b.check_size_and_edit(
			++spatialValue,
			spatialValue,
			1,
			spatialValue,
		);
	});

	// 12. Spatial lane read.
	bench("WARLOCK_RAPTOR.spatial.read", () => {
		b.getX(1);
		b.getY(1);
	});

	// 13. Spatial position object allocation.
	bench("WARLOCK_RAPTOR.spatial.getPosition", () => {
		b.getPosition(1);
	});

	// 14. Scheduler pass.
	bench("WARLOCK_RAPTOR.pass", () => {
		b.pass();
	});
}
else if (FIGHT === "BINARY_REACTOR2") {
	const b = new BINARY_REACTOR2();

	const source = b.atom(1, "source");
	const existing = b.atom(1, "existing");
	const address = {
		r: "__ROOT__",
		m: "__MASTER__",
		s: "__SLAVE__",
		k: "existing",
	};

	const computed = b.computed(
		() => source.peek() * 2,
		"computed",
	);

	computed.get();

	// 1. Atom read: dependency-free.
	bench("BINARY_REACTOR2.atom.peek", () => {
		existing.peek();
	});

	// 2. Atom read: public tracked-read path.
	bench("BINARY_REACTOR2.atom.get", () => {
		existing.get();
	});

	// 3. Atom version read.
	bench("BINARY_REACTOR2.atom.version", () => {
		existing.version;
	});

	// 4. Atom write: unchanged-value fast path.
	bench("BINARY_REACTOR2.atom.set.unchanged", () => {
		existing.set(1);
	});

	// 5. Atom update: unchanged-value fast path.
	bench("BINARY_REACTOR2.atom.update.unchanged", () => {
		existing.update(v => v);
	});

	// 6. Address lookup: existing cell.
	bench("BINARY_REACTOR2.atom.lookup", () => {
		b.atom(1, address.k, address.s, address.m, address.r);
	});

	// 7. Subscriber registration and disposal.
	const listener = (_previous: unknown, _next: unknown) => {};

	bench("BINARY_REACTOR2.atom.subscribe-dispose", () => {
		const dispose = existing.subscribe(listener);
		dispose();
	});

	// 8. Computed read: cached value.
	bench("BINARY_REACTOR2.computed.get.cached", () => {
		computed.get();
	});

	// 9. Computed peek: cached value without dependency tracking.
	bench("BINARY_REACTOR2.computed.peek.cached", () => {
		computed.peek();
	});

	// 10. Effect allocation, initial execution, and disposal.
	bench("BINARY_REACTOR2.effect.create-dispose", () => {
		const dispose = b.effect(() => {});
		dispose();
	});

	// 11. Effect dependency tracking, edge allocation, and cleanup.
	bench("BINARY_REACTOR2.effect.track-dispose", () => {
		const dispose = b.effect(() => {
			source.get();
		});
		dispose();
	});

	// 12. Empty batch.
	bench("BINARY_REACTOR2.batch.empty", () => {
		b.batch(() => {});
	});

	// 13. Batch with an unchanged write.
	bench("BINARY_REACTOR2.batch.unchanged-write", () => {
		b.batch(() => {
			existing.set(1);
		});
	});
}
else if (FIGHT === "BINARY_REACTOR") {
	const b = new BINARY_REACTOR();

	// 1. Atom address lookup
	const lookup = b.atom(0, "lookup");
	bench("BINARY_REACTOR.atom.lookup", () => {
		b.atom(0, "lookup");
	});

	// 2. Tracked read
	const read = b.atom(1, "read");
	bench("BINARY_REACTOR.atom.get", () => {
		read.get();
	});

	// 3. Untracked read
	const peek = b.atom(1, "peek");
	bench("BINARY_REACTOR.atom.peek", () => {
		peek.peek();
	});

	// 4. Write and propagation
	const write = b.atom(0, "write");
	let writeValue = 0;
	bench("BINARY_REACTOR.atom.set", () => {
		writeValue ^= 1;
		write.set(writeValue);
	});

	// 5. Read-modify-write
	const update = b.atom(0, "update");
	bench("BINARY_REACTOR.atom.update", () => {
		update.update(v => v + 1);
	});

	// 6. Listener notification
	const notify = b.atom(0, "notify");
	let notifyValue = 0;
	let notifications = 0;
	notify.subscribe(() => {
		notifications++;
	});
	bench("BINARY_REACTOR.atom.notify", () => {
		notifyValue ^= 1;
		notify.set(notifyValue);
	});

	// 7. Subscription registration and disposal
	const subscribe = b.atom(0, "subscribe");
	bench("BINARY_REACTOR.atom.subscribe.dispose", () => {
		const dispose = subscribe.subscribe(() => {});
		dispose();
	});

	// 8. Effect dependency tracking and rerun
	const effectSource = b.atom(0, "effect-source");
	let effectValue = 0;
	let effectRuns = 0;
	const disposeEffect = b.effect(() => {
		effectValue = effectSource.get() as number;
		effectRuns++;
	});
	let effectWrite = 0;
	bench("BINARY_REACTOR.effect.rerun", () => {
		effectWrite ^= 1;
		effectSource.set(effectWrite);
	});

	// 9. Computed validation and recomputation
	const computedSource = b.atom(0, "computed-source");
	const computed = b.computed(
		() => (computedSource.get() as number) * 2,
		"computed-output"
	);
	computed.get();
	let computedWrite = 0;
	bench("BINARY_REACTOR.computed.recompute", () => {
		computedWrite ^= 1;
		computedSource.set(computedWrite);
		computed.get();
	});

	// 10. Batched writes and propagation
	const batchA = b.atom(0, "batch-a");
	const batchB = b.atom(0, "batch-b");
	let batchValue = 0;
	bench("BINARY_REACTOR.batch.two-writes", () => {
		batchValue ^= 1;
		b.batch(() => {
			batchA.set(batchValue);
			batchB.set(batchValue);
		});
	});

	// 11. Effect allocation, dependency tracking, and disposal
	const lifecycleSource = b.atom(0, "lifecycle-source");
	bench("BINARY_REACTOR.effect.create.dispose", () => {
		const dispose = b.effect(() => {
			lifecycleSource.get();
		});
		dispose();
	});

	// Keep setup handles alive for the duration of the benchmark run.
	void lookup;
	void disposeEffect;
	void computed;
	void notifications;
	void effectValue;
	void effectRuns;
}
else if (FIGHT === "alien") {
	// 1. Signal read
	const read = signal(1);
	bench("ALIEN.signal.read", () => {
		read();
	});

	// 2. Signal write
	const write = signal(0);
	let writeValue = 0;
	bench("ALIEN.signal.write", () => {
		writeValue ^= 1;
		write(writeValue);
	});

	// 3. Signal write + effect notification
	const notifySource = signal(0);
	let notifyValue = 0;
	let notifyRuns = 0;
	const disposeNotify = effect(() => {
		notifySource();
		notifyRuns++;
	});
	let notifyWrite = 0;
	bench("ALIEN.signal.write.notify", () => {
		notifyWrite ^= 1;
		notifySource(notifyWrite);
	});

	// 4. Fan-out: one signal, 16 dependent effects
	const fanoutSource = signal(0);
	let fanoutRuns = 0;
	const disposeFanout: (() => void)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeFanout.push(effect(() => {
			fanoutSource();
			fanoutRuns++;
		}));
	}
	let fanoutValue = 0;
	bench("ALIEN.signal.fanout.16", () => {
		fanoutValue ^= 1;
		fanoutSource(fanoutValue);
	});

	// 5. Fan-in: one effect, 16 dependent signals
	const faninSources = Array.from({ length: 16 }, () => signal(0));
	let faninRuns = 0;
	const disposeFanin = effect(() => {
		for (let i = 0; i < faninSources.length; i++) {
			faninSources[i]();
		}
		faninRuns++;
	});
	let faninValue = 0;
	bench("ALIEN.signal.fanin.16", () => {
		faninValue ^= 1;
		faninSources[0](faninValue);
	});

	// 6. Computed read without recomputation
	const cleanSource = signal(1);
	const cleanComputed = computed(() => cleanSource() * 2);
	cleanComputed();
	bench("ALIEN.computed.read.clean", () => {
		cleanComputed();
	});

	// 7. Computed invalidation and recomputation
	const computedSource = signal(0);
	const computedValue = computed(() => computedSource() * 2);
	computedValue();
	let computedWrite = 0;
	bench("ALIEN.computed.recompute", () => {
		computedWrite ^= 1;
		computedSource(computedWrite);
		computedValue();
	});

	// 8. Dependency chain: source -> 8 computed nodes
	const chainSource = signal(0);
	let chainValue = computed(() => chainSource() + 1);
	for (let i = 1; i < 8; i++) {
		const previous = chainValue;
		chainValue = computed(() => previous() + 1);
	}
	chainValue();
	let chainWrite = 0;
	bench("ALIEN.computed.chain.8", () => {
		chainWrite ^= 1;
		chainSource(chainWrite);
		chainValue();
	});

	// 9. Batched writes with one dependent effect
	const batchA = signal(0);
	const batchB = signal(0);
	let batchRuns = 0;
	const disposeBatch = effect(() => {
		batchA();
		batchB();
		batchRuns++;
	});
	let batchValue = 0;
	bench("ALIEN.batch.two-writes", () => {
		batchValue ^= 1;
		startBatch();
		try {
			batchA(batchValue);
			batchB(batchValue);
		} finally {
			endBatch();
		}
	});

	// 10. Effect creation, dependency tracking, and disposal
	const lifecycleSource = signal(0);
	bench("ALIEN.effect.create.dispose", () => {
		const dispose = effect(() => {
			lifecycleSource();
		});
		dispose();
	});

	// 11. Effect rerun with cleanup
	const cleanupSource = signal(0);
	let cleanupRuns = 0;
	let cleanupCalls = 0;
	const disposeCleanup = effect(() => {
		cleanupSource();
		cleanupRuns++;
		return () => {
			cleanupCalls++;
		};
	});
	let cleanupValue = 0;
	bench("ALIEN.effect.cleanup.rerun", () => {
		cleanupValue ^= 1;
		cleanupSource(cleanupValue);
	});

	// Keep effects alive throughout their respective measurements.
	void disposeNotify;
	void disposeFanin;
	void disposeBatch;
	void disposeCleanup;
	void disposeFanout;
	void notifyRuns;
	void fanoutRuns;
	void faninRuns;
	void batchRuns;
	void cleanupRuns;
	void cleanupCalls;
}
else if (FIGHT === "preact") {
	// 1. Signal read
	const read = preact_signal(1);
	bench("PREACT_SIGNALS.signal.read", () => {
		read.value;
	});

	// 2. Signal write
	const write = preact_signal(0);
	let writeValue = 0;
	bench("PREACT_SIGNALS.signal.write", () => {
		writeValue ^= 1;
		write.value = writeValue;
	});

	// 3. Signal write + effect notification
	const notifySource = preact_signal(0);
	let notifyRuns = 0;
	const disposeNotify = preact_effect(() => {
		notifySource.value;
		notifyRuns++;
	});
	let notifyValue = 0;
	bench("PREACT_SIGNALS.signal.write.notify", () => {
		notifyValue ^= 1;
		notifySource.value = notifyValue;
	});

	// 4. Fan-out: one signal, 16 dependent effects
	const fanoutSource = preact_signal(0);
	let fanoutRuns = 0;
	const disposeFanout: (() => void)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeFanout.push(preact_effect(() => {
			fanoutSource.value;
			fanoutRuns++;
		}));
	}
	let fanoutValue = 0;
	bench("PREACT_SIGNALS.signal.fanout.16", () => {
		fanoutValue ^= 1;
		fanoutSource.value = fanoutValue;
	});

	// 5. Fan-in: one effect, 16 dependent signals
	const faninSources = Array.from({ length: 16 }, () => preact_signal(0));
	let faninRuns = 0;
	const disposeFanin = preact_effect(() => {
		for (let i = 0; i < faninSources.length; i++) {
			faninSources[i].value;
		}
		faninRuns++;
	});
	let faninValue = 0;
	bench("PREACT_SIGNALS.signal.fanin.16", () => {
		faninValue ^= 1;
		faninSources[0].value = faninValue;
	});

	// 6. Computed read without recomputation
	const cleanSource = preact_signal(1);
	const cleanComputed = preact_computed(() => cleanSource.value * 2);
	cleanComputed.value;
	bench("PREACT_SIGNALS.computed.read.clean", () => {
		cleanComputed.value;
	});

	// 7. Computed invalidation and recomputation
	const computedSource = preact_signal(0);
	const computedValue = preact_computed(() => computedSource.value * 2);
	computedValue.value;
	let computedWrite = 0;
	bench("PREACT_SIGNALS.computed.recompute", () => {
		computedWrite ^= 1;
		computedSource.value = computedWrite;
		computedValue.value;
	});

	// 8. Dependency chain: source -> 8 computed nodes
	const chainSource = preact_signal(0);
	let chainValue = preact_computed(() => chainSource.value + 1);
	for (let i = 1; i < 8; i++) {
		const previous = chainValue;
		chainValue = preact_computed(() => previous.value + 1);
	}
	chainValue.value;
	let chainWrite = 0;
	bench("PREACT_SIGNALS.computed.chain.8", () => {
		chainWrite ^= 1;
		chainSource.value = chainWrite;
		chainValue.value;
	});

	// 9. Batched writes with one dependent effect
	const batchA = preact_signal(0);
	const batchB = preact_signal(0);
	let batchRuns = 0;
	const disposeBatch = preact_effect(() => {
		batchA.value;
		batchB.value;
		batchRuns++;
	});
	let batchValue = 0;
	bench("PREACT_SIGNALS.batch.two-writes", () => {
		batchValue ^= 1;
		preact_batch(() => {
			batchA.value = batchValue;
			batchB.value = batchValue;
		});
	});

	// 10. Effect creation, dependency tracking, and disposal
	const lifecycleSource = preact_signal(0);
	bench("PREACT_SIGNALS.effect.create.dispose", () => {
		const dispose = preact_effect(() => {
			lifecycleSource.value;
		});
		dispose();
	});

	// 11. Effect rerun with cleanup
	const cleanupSource = preact_signal(0);
	let cleanupRuns = 0;
	let cleanupCalls = 0;
	const disposeCleanup = preact_effect(() => {
		cleanupSource.value;
		cleanupRuns++;
		return () => {
			cleanupCalls++;
		};
	});
	let cleanupValue = 0;
	bench("PREACT_SIGNALS.effect.cleanup.rerun", () => {
		cleanupValue ^= 1;
		cleanupSource.value = cleanupValue;
	});

	void preact_untracked;
	void disposeNotify;
	void disposeFanin;
	void disposeBatch;
	void disposeCleanup;
	void disposeFanout;
	void notifyRuns;
	void fanoutRuns;
	void faninRuns;
	void batchRuns;
	void cleanupRuns;
	void cleanupCalls;
}











