// bench_run.ts
import { performance } from 'node:perf_hooks';

import * as preactModule from './signals_preact';
import * as alienModule from './signals_alien';
import { SIGNALS_DISRUPTOR } from './signals_disruptor';

const ITERATIONS = 20_000;
const CHAIN_DEPTH = 1_000;
const WRITES = 5_000;

console.log(`\n==================================================`);
console.log(` HEAD-TO-HEAD BENCHMARK: Preact vs Alien vs Disruptor`);
console.log(` Workload: ${ITERATIONS} allocs, ${CHAIN_DEPTH}-node chain, ${WRITES} cascading writes`);
console.log(`==================================================\n`);

// 1. Preact Benchmark
let start = performance.now();

if (typeof (alienModule as any).run === 'function') {
    (alienModule as any).run(ITERATIONS, CHAIN_DEPTH, WRITES);
} else {
    const sig = (alienModule as any).signal || (alienModule as any).default?.signal;
    const comp = (alienModule as any).computed || (alienModule as any).default?.computed;
    if (sig && comp) {
        const signals = [];
        for (let i = 0; i < ITERATIONS; i++) signals.push(sig(i));
        let root = sig(1);
        let curr = root;
        for (let i = 0; i < CHAIN_DEPTH; i++) {
            const prev = curr;
            curr = comp(() => prev() + 1);
        }
        for (let w = 0; w < WRITES; w++) {
            root(w);
            curr();
        }
    }
}
const alienTime = performance.now() - start;
console.log(`[signals_alien.ts]     Time: ${alienTime.toFixed(2)}ms`);






// 2. Alien Benchmark
start = performance.now();
if (typeof (preactModule as any).run === 'function') {
    (preactModule as any).run(ITERATIONS, CHAIN_DEPTH, WRITES);
} else {
    const sig = (preactModule as any).signal || (preactModule as any).default?.signal;
    const comp = (preactModule as any).computed || (preactModule as any).default?.computed;
    if (sig && comp) {
        const signals = [];
        for (let i = 0; i < ITERATIONS; i++) signals.push(sig(i));
        let root = sig(1);
        let curr = root;
        for (let i = 0; i < CHAIN_DEPTH; i++) {
            const prev = curr;
            curr = comp(() => prev.value + 1);
        }
        for (let w = 0; w < WRITES; w++) {
            root.value = w;
            curr.value;
        }
    }
}
const preactTime = performance.now() - start;
console.log(`[signals_preact.ts]   Time: ${preactTime.toFixed(2)}ms`);

// 3. Disruptor Benchmark
start = performance.now();
const disruptor = new SIGNALS_DISRUPTOR(65_536);
const pids: number[] = [];
for (let i = 0; i < ITERATIONS; i++) {
    pids.push(disruptor.signal(i));
}
let dRoot = disruptor.signal(1);
let dCurr = dRoot;
for (let i = 0; i < CHAIN_DEPTH; i++) {
    const prev = dCurr;
    dCurr = disruptor.computed(() => disruptor.signalOper<number>(prev)! + 1);
}
for (let w = 0; w < WRITES; w++) {
    disruptor.signalOper(dRoot, w);
    disruptor.computedOper(dCurr);
}
const disruptorTime = performance.now() - start;
console.log(`[signals_disruptor.ts] Time: ${disruptorTime.toFixed(2)}ms`);

console.log(`\n--------------------------------------------------`);
console.log(` disruptor vs Preact file: ${(preactTime / disruptorTime).toFixed(2)}x faster`);
console.log(` disruptor vs Alien file:  ${(alienTime / disruptorTime).toFixed(2)}x faster`);
console.log(`==================================================\n`);
