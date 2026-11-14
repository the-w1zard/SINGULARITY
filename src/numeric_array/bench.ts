/**
 * PART 2 — THROUGHPUT BENCHMARK
 *
 * Every scenario runs for a fixed wall-clock window (10s by default) and keeps
 * doing as many operations as it can. Ops are counted per *public API call*
 * (one atom.set, one get, one batch, ...), so the numbers stay comparable.
 *
 * Run:  npx tsx Benchmark.ts
 *       npx tsx Benchmark.ts --ms=10000
 *       BENCH_MS=30000 npx tsx Benchmark.ts
 *       npx tsx Benchmark.ts --only=computed
 *
 * Env:
 *   BENCH_MS                 per-scenario window, default 10000
 *   BENCH_MEMORY_GUARD_MB    abort a scenario past this heap size, default 1024
 *   BENCH_WARMUP_MS          warmup per scenario, default 400
 */

import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { BINARY_REACTOR } from "./BINARY_REACTOR";

function arg(name: string): string | undefined {
    const prefix = `--${name}=`;
    const found = process.argv.find((value) => value.startsWith(prefix));

    return found ? found.slice(prefix.length) : undefined;
}

const DURATION_MS = Number(arg("ms") ?? process.env.BENCH_MS ?? 10_000);
const WARMUP_MS = Number(arg("warmup") ?? process.env.BENCH_WARMUP_MS ?? 400);
const MEMORY_GUARD_MB = Number(
    process.env.BENCH_MEMORY_GUARD_MB ?? 1024
);
const ONLY = arg("only")?.toLowerCase();

interface Scenario {
    name: string;
    note?: string;
    make: () => { op: () => void; inspect?: () => Record<string, number> };
}

interface Result {
    name: string;
    note?: string;
    seconds: number;
    ops: number;
    opsPerSecond: number;
    nanosecondsPerOp: number;
    heapDeltaMb: number;
    aborted: boolean;
    inspect?: Record<string, number>;
}

const scenarios: Scenario[] = [
    {
        name: "atom.set — number, no subscribers",
        note: "raw write path: encode, version bump, invalidate",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(0, "counter");
            let index = 0;

            return {
                op: () => void a.set(++index),
                inspect: () => ({
                    handles: (b as unknown as { objectValues: unknown[] }).objectValues.length
                })
            };
        }
    },
    {
        name: "atom.update — number, no subscribers",
        note: "read, callback, write",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(0, "counter");

            return { op: () => void a.update((value) => value + 1) };
        }
    },
    {
        name: "atom.get — tracked read, no active effect",
        note: "deref of an object handle",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(123, "read");
            let sink = 0;

            return { op: () => void (sink += a.get()) };
        }
    },
    {
        name: "atom.peek — untracked read",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(123, "read");
            let sink = 0;

            return { op: () => void (sink += a.peek()) };
        }
    },
    {
        name: "set → 1 effect",
        note: "write + flush + effect run + dependency re-tracking",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(0, "source");
            let seen = 0;
            let index = 0;

            b.effect(() => void (seen = a.get()));

            return { op: () => void a.set(++index) };
        }
    },
    {
        name: "set → 16 effects (fan-out)",
        note: "one write, sixteen synchronous effect runs",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(0, "source");
            const seen = new Array(16).fill(0);
            let index = 0;

            seen.forEach((_unused, slot) => {
                b.effect(() => void (seen[slot] = a.get()));
            });

            return { op: () => void a.set(++index) };
        }
    },
    {
        name: "computed chain — depth 8, write + read",
        note: "set root, then peek the leaf: eight recomputations",
        make: () => {
            const b = new BINARY_REACTOR();
            const root = b.atom(0, "root");
            let node = b.computed(() => root.get() + 1, "level0");

            for (let level = 1; level < 8; level++) {
                const previous = node;

                node = b.computed(() => previous.get() + 1, `level${level}`);
            }

            const leaf = node;

            leaf.peek();

            let index = 0;

            return { op: () => void (root.set(++index), leaf.peek()) };
        }
    },
    {
        name: "computed chain — depth 8, write only",
        note: "lazy propagation cost: invalidation without evaluation",
        make: () => {
            const b = new BINARY_REACTOR();
            const root = b.atom(0, "root");
            let node = b.computed(() => root.get() + 1, "level0");

            for (let level = 1; level < 8; level++) {
                const previous = node;

                node = b.computed(() => previous.get() + 1, `level${level}`);
            }

            let index = 0;

            return { op: () => void root.set(++index) };
        }
    },
    {
        name: "diamond — root → (2) → join → effect",
        note: "glitch-free dedupe of a shared ancestor",
        make: () => {
            const b = new BINARY_REACTOR();
            const root = b.atom(0, "root");
            const left = b.computed(() => root.get() + 1, "left");
            const right = b.computed(() => root.get() * 2, "right");
            const join = b.computed(() => left.get() + right.get(), "join");
            let seen = 0;
            let index = 0;

            b.effect(() => void (seen = join.get()));

            return { op: () => void root.set(++index) };
        }
    },
    {
        name: "batch — 16 writes, one flush",
        note: "ops counted as batches; multiply by 16 for writes/sec",
        make: () => {
            const b = new BINARY_REACTOR();
            const atoms = Array.from({ length: 16 }, (_unused, slot) =>
                b.atom(0, `batched${slot}`)
            );
            let index = 0;

            b.effect(() => {
                for (const atom of atoms) atom.get();
            });

            return {
                op: () => {
                    index++;

                    b.batch(() => {
                        for (const atom of atoms) atom.set(index);
                    });
                }
            };
        }
    },
    {
        name: "atom.set — fresh object payload",
        note: "allocates and encodes a new object per write",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom<{ index: number }>({ index: 0 }, "payload");
            let index = 0;

            return {
                op: () => void a.set({ index: ++index }),
                inspect: () => ({
                    handles: (b as unknown as { objectValues: unknown[] }).objectValues.length
                })
            };
        }
    },
    {
        name: "addressing — set across 512 distinct cells",
        note: "hash + open addressing + rehash pressure",
        make: () => {
            const b = new BINARY_REACTOR();
            const atoms = Array.from({ length: 512 }, (_unused, slot) =>
                b.atom(0, `addressed${slot}`, `slave${slot % 8}`, `master${slot % 4}`, `root${slot % 2}`)
            );
            let index = 0;

            return { op: () => void atoms[index++ & 511].set(index) };
        }
    },
    {
        name: "subscribe + dispose — per-op churn",
        note: "listener array push/splice only",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(0, "source");

            return {
                op: () => {
                    const dispose = a.subscribe((_previous, value) => void value);

                    dispose();
                }
            };
        }
    },
    {
        name: "effect create + dispose — per-op churn",
        note: "execution slots, dependency edges, free lists",
        make: () => {
            const b = new BINARY_REACTOR();
            const a = b.atom(0, "source");

            return {
                op: () => {
                    const dispose = b.effect(() => void a.get());

                    dispose();
                }
            };
        }
    }
];

function formatOps(value: number): string {
    if (value >= 1e9) return `${(value / 1e9).toFixed(2)}B`;
    if (value >= 1e6) return `${(value / 1e6).toFixed(2)}M`;
    if (value >= 1e3) return `${(value / 1e3).toFixed(2)}K`;

    return value.toFixed(0);
}

function formatNumber(value: number): string {
    return Math.round(value).toLocaleString("en-US");
}

function measure(scenario: Scenario): Result {
    const { op, inspect } = scenario.make();
    const guardBytes = MEMORY_GUARD_MB * 1024 * 1024;

    // Warm-up: let the JIT settle before the clock starts.
    const warmupEnd = performance.now() + WARMUP_MS;

    while (performance.now() < warmupEnd) {
        for (let index = 0; index < 64; index++) op();
    }

    const heapStart = process.memoryUsage().heapUsed;
    let ops = 0;
    let chunk = 1024;
    let aborted = false;

    const start = performance.now();
    let now = start;

    while (now - start < DURATION_MS) {
        for (let index = 0; index < chunk; index++) op();

        ops += chunk;

        now = performance.now();

        // Grow the chunk until a single pass takes a measurable slice of time.
        const slice = (now - start) / Math.max(1, ops / chunk);

        if (slice < 5 && chunk < 1 << 20) chunk *= 2;

        if (process.memoryUsage().heapUsed > guardBytes) {
            aborted = true;
            break;
        }
    }

    const seconds = (now - start) / 1000;
    const heapDelta = process.memoryUsage().heapUsed - heapStart;

    return {
        name: scenario.name,
        note: scenario.note,
        seconds,
        ops,
        opsPerSecond: ops / seconds,
        nanosecondsPerOp: (seconds * 1e9) / ops,
        heapDeltaMb: heapDelta / (1024 * 1024),
        aborted,
        inspect: inspect ? inspect() : undefined
    };
}

function pad(value: string, width: number, right = false): string {
    const plain = value.replace(/\u001b\[[0-9;]*m/g, "");

    if (plain.length >= width) return value;

    const filler = " ".repeat(width - plain.length);

    return right ? filler + value : value + filler;
}

function bar(label: string): void {
    console.log(`\n${label}`);
    console.log("─".repeat(96));
}

function main(): void {
    const selected = ONLY
        ? scenarios.filter((scenario) =>
              scenario.name.toLowerCase().includes(ONLY)
          )
        : scenarios;

    console.log("BINARY_REACTOR — throughput benchmark");
    console.log(
        `node ${process.version} · ${selected.length} scenarios · ${(
            DURATION_MS / 1000
        ).toFixed(1)}s per scenario · warmup ${WARMUP_MS}ms`
    );

    const results: Result[] = [];

    for (const scenario of selected) {
        process.stdout.write(`  running ${scenario.name} ... `);

        const result = measure(scenario);

        results.push(result);

        console.log(
            `${formatOps(result.opsPerSecond)} ops/s` +
                (result.aborted ? " (aborted: heap guard)" : "")
        );
    }

    bar("RESULTS");

    console.log(
        pad("scenario", 52) +
            pad("ops/s", 12, true) +
            pad("ns/op", 10, true) +
            pad("ops", 14, true) +
            pad("heap Δ", 10, true)
    );

    for (const result of results) {
        console.log(
            pad(result.name, 52) +
                pad(formatNumber(result.opsPerSecond), 12, true) +
                pad(result.nanosecondsPerOp.toFixed(1), 10, true) +
                pad(formatNumber(result.ops), 14, true) +
                pad(`${result.heapDeltaMb.toFixed(1)} MB`, 10, true) +
                (result.aborted ? "  ⚠ heap guard" : "")
        );
    }

    const withNotes = results.filter((result) => result.note);

    if (withNotes.length) {
        console.log("");

        for (const result of withNotes) {
            console.log(`  ${result.name}`);
            console.log(`    ${result.note}`);
        }
    }

    const inspected = results.filter((result) => result.inspect);

    if (inspected.length) {
        bar("INTERNAL GROWTH");

        for (const result of inspected) {
            const entries = Object.entries(result.inspect ?? {})
                .map(([key, value]) => `${key}=${formatNumber(value)}`)
                .join("  ");

            console.log(`  ${pad(result.name, 52)}${entries}`);
        }
    }

    const fastest = results.reduce((best, result) =>
        result.opsPerSecond > best.opsPerSecond ? result : best
    );
    const totalOps = results.reduce((sum, result) => sum + result.ops, 0);

    bar("SUMMARY");

    console.log(
        `  peak      ${formatNumber(fastest.opsPerSecond)} ops/s  (${fastest.name})`
    );
    console.log(`  total     ${formatNumber(totalOps)} ops across all scenarios`);
    console.log(
        `  wall      ${(
            results.reduce((sum, result) => sum + result.seconds, 0)
        ).toFixed(1)}s measured`
    );

    const outputPath = "Benchmark Results.json";

    writeFileSync(
        outputPath,
        JSON.stringify(
            {
                generatedAt: new Date().toISOString(),
                node: process.version,
                durationMs: DURATION_MS,
                warmupMs: WARMUP_MS,
                results
            },
            null,
            2
        )
    );

    console.log(`\n  saved      ${resolve(outputPath)}`);
    console.log("─".repeat(96));
}

main();
