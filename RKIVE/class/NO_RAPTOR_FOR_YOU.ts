// bus-bench.ts
// FIGHT=... bun --expose-gc bus-bench.ts

import { startBatch,   signal, computed, effect,batch } from "./class";
import { startBatch as alien_startBatch,   signal as alien_signal, computed as alien_computed, effect as alien_effect, endBatch as alien_endBatch, setActiveSub as alien_setActiveSub } from "./signals_alien";


import { signal as preact_signal, computed as preact_computed, batch as preact_batch, effect as preact_effect, action, createModel, type Model, type ModelConstructor, type ModelFactory, Signal, type ReadonlySignal, untracked as preact_untracked, type SignalOptions, type EffectOptions } from "@preact/signals-core";
import { persistedSignal } from "@kamod-ch/signals";

type fighter =

| "alien"
|"SIGNAL_ENGINE"

const FIGHT: fighter = (process.env.FIGHT as fighter) ?? "SIGNAL_ENGINE";

// #region

const CAPACITY = 1 << 18;
const DURATION = 12;
const REPORT_MS = 1;
const BATCH_SIZE = 100_000; // 500_000
const TOTAL_BATCHES = 100; // 500
const RUN_BARE_METAL_CPU_TEST: Boolean = true;

const pid = 1;
let seq = 0;
const v = `${++seq}`;

// ─────────────────────────────────────────────────────────────
// DEFERRED LOGGING
// All output is buffered in memory and only flushed to the real
// console once every benchmark has finished running, so that
// console.log's own overhead never leaks into a measured loop.
// ─────────────────────────────────────────────────────────────

const logBuffer: string[] = [];

function log(...args: unknown[]): void {
  logBuffer.push(args.map((a) => String(a)).join(" "));
}

function flushLogs(): void {
  for (const line of logBuffer) console.log(line);
  logBuffer.length = 0;
}




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
  //for (let i = 0; i < 2_000_000; i++) write();

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

  log(`${name}`);
  log(`${(bestOpsPerSec / 1e6).toFixed(2)}`);
  log(`${(avg / 1e6).toFixed(2)}`);
}

function BENCH(name: string, write: () => void): void {
  let windowOps = 0, totalOps = 0, best = 0, elapsedTotal = 0;
  let start = process.hrtime.bigint();
  let windowStart = start;
  let cpuStart = process.cpuUsage();

  log(`\n${name}`);
  log("sec | M writes/s | best M/s | avg M/s | CPU %");

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
      log(
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

// #endregion




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

if (FIGHT === "alien") {
    const batch = (fn: () => void) => {
        alien_startBatch();
        try {
            fn();
        } finally {
            alien_endBatch();
        }
    };

    const readSignal = alien_signal(1);

    bench("alien.get", () => {
        readSignal();
    });

    bench("alien.peek", () => {
        const previous = alien_setActiveSub(undefined);
        try {
            readSignal();
        } finally {
            alien_setActiveSub(previous);
        }
    });

    let writeValue = 0;

    bench("alien.set", () => {
        readSignal(++writeValue);
    });

    bench("alien.get.set", () => {
        readSignal();
        readSignal(++writeValue);
    });

    /* ------------------------------------------------------------------ *
     * COMPUTED
     * ------------------------------------------------------------------ */

    const computedSource = alien_signal(1);
    const cachedComputed = alien_computed(() => computedSource() * 2);

    bench("alien.computed", () => {
        cachedComputed();
    });

    bench("alien.computed.invalidate", () => {
        computedSource(++writeValue);
    });

    bench("alien.computed.recompute", () => {
        computedSource(++writeValue);
        cachedComputed();
    });

    const chainSource = alien_signal(1);
    const chain1 = alien_computed(() => chainSource() + 1);
    const chain2 = alien_computed(() => chain1() + 1);
    const chain3 = alien_computed(() => chain2() + 1);

    chain3();

    bench("alien.computed.chain.cached", () => {
        chain3();
    });

    bench("alien.computed.chain.recompute", () => {
        chainSource(++writeValue);
        chain3();
    });

    /* ------------------------------------------------------------------ *
     * EFFECT
     * ------------------------------------------------------------------ */

    let effectValue = 0;

    bench("alien.effect.create-dispose", () => {
        const dispose = effect(() => {
            effectValue++;
        });
        dispose();
    });

    const notifySignal = alien_signal(0);
    let notifyCount = 0;

    const disposeNotify = alien_effect(() => {
        notifySignal();
        notifyCount++;
    });

    bench("alien.effect.notify", () => {
        batch(() => {
            notifySignal(++writeValue);
        });
    });

    disposeNotify();

    const cleanupSignal = alien_signal(0);
    let cleanupCount = 0;

    const disposeCleanup = alien_effect(() => {
        cleanupSignal();
        return () => {
            cleanupCount++;
        };
    });

    bench("alien.effect.cleanup-rerun", () => {
        batch(() => {
            cleanupSignal(++writeValue);
        });
    });

    disposeCleanup();

    /* ------------------------------------------------------------------ *
     * DEPENDENCY TRACKING
     * ------------------------------------------------------------------ */

    const dependencySource = alien_signal(1);
    let dependencyResult = 0;

    const disposeDependency = alien_effect(() => {
        dependencyResult = dependencySource();
    });

    bench("alien.dependency.track", () => {
        batch(() => {
            dependencySource(++writeValue);
        });
    });

    disposeDependency();

    /* ------------------------------------------------------------------ *
     * FAN-OUT
     * ------------------------------------------------------------------ */

    const fanout16 = alien_signal(0);
    const fanout16Disposers: (() => void)[] = [];

    for (let i = 0; i < 16; i++) {
        fanout16Disposers.push(
            alien_effect(() => {
                fanout16();
            }),
        );
    }

    bench("alien.fanout.16", () => {
        batch(() => {
            fanout16(++writeValue);
        });
    });

    for (const dispose of fanout16Disposers) dispose();

    const fanout64 = alien_signal(0);
    const fanout64Disposers: (() => void)[] = [];

    for (let i = 0; i < 64; i++) {
        fanout64Disposers.push(
            alien_effect(() => {
                fanout64();
            }),
        );
    }

    bench("alien.fanout.64", () => {
        batch(() => {
            fanout64(++writeValue);
        });
    });

    for (const dispose of fanout64Disposers) dispose();

    /* ------------------------------------------------------------------ *
     * BATCHING
     * ------------------------------------------------------------------ */

    const batchA = alien_signal(0);
    const batchB = alien_signal(0);
    const batchC = alien_signal(0);

    bench("alien.batch.3-writes", () => {
        batch(() => {
            batchA(++writeValue);
            batchB(++writeValue);
            batchC(++writeValue);
        });
    });

    const batchSource = alien_signal(0);
    let batchEffectCount = 0;

    const disposeBatch = alien_effect(() => {
        batchSource();
        batchEffectCount++;
    });

    bench("alien.batch.effect.3-writes", () => {
        batch(() => {
            batchSource(++writeValue);
            batchSource(++writeValue);
            batchSource(++writeValue);
        });
    });

    disposeBatch();

    /* ------------------------------------------------------------------ *
     * GRAPH
     * ------------------------------------------------------------------ */

    const diamondSource = alien_signal(1);
    const diamondLeft = alien_computed(() => diamondSource() + 1);
    const diamondRight = alien_computed(() => diamondSource() + 2);
    const diamondResult = alien_computed(
        () => diamondLeft() + diamondRight(),
    );

    diamondResult();

    bench("alien.graph.diamond.cached", () => {
        diamondResult();
    });

    bench("alien.graph.diamond.recompute", () => {
        diamondSource(++writeValue);
        diamondResult();
    });

    const branchSource = alien_signal(1);
    const branches = Array.from(
        { length: 16 },
        (_, i) => alien_computed(() => branchSource() + i),
    );
    const branchResult = alien_computed(
        () => branches.reduce((total, branch) => total + branch(), 0),
    );

    branchResult();

    bench("alien.graph.branch.16.cached", () => {
        branchResult();
    });

    bench("alien.graph.branch.16.recompute", () => {
        branchSource(++writeValue);
        branchResult();
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


flushLogs();

