import { performance } from 'node:perf_hooks';
import { SIGNALS_DISRUPTOR } from './signals3';
import { WarlockDisruptor } from './signals_disruptor2';
import { WARLOCK2 } from './signals4';
import { signal as as_, computed as ac, effect as ae } from './signals_alien';
import { signal as ps, computed as pc, effect as pe } from './signals_preact';

// ============ PART 1: chain ============
const ITERATIONS = 20_000, DEPTH = 1_000, WRITES = 5_000;
const cap = 1 << 22;
const d = new SIGNALS_DISRUPTOR(cap);
const wd = new WARLOCK2(cap);
console.log(cap, 'cap');

function chainDisruptor(): number {
  for (let i = 0; i < ITERATIONS; i++) d.signal(i);
  let root = d.signal(1), curr = root;
  for (let i = 0; i < DEPTH; i++) {
    const prev = curr;
    curr = d.computed(() => d.signalOper<number>(prev)! + 1);
  }
  const t0 = performance.now();
  for (let w = 0; w < WRITES; w++) { d.signalOper(root, w); d.computedOper(curr); }
  return performance.now() - t0;
}

function chainWarlock(): number {
    // Warm-up / allocation loop
    for (let i = 0; i < ITERATIONS; i++) {
        wd.signal(i);
    }

    const root = wd.signal(1) as Pid;
    let curr: Pid = root;

    for (let i = 0; i < DEPTH; i++) {
        const prev = curr;

        curr = wd.computed(() => {
            return (wd.signalOper<number>(prev) as number) + 1;
        }) as Pid;
    }

    const t0 = performance.now();

    for (let w = 0; w < WRITES; w++) {
        // Write to root signal
        wd.signalOper(root, w);

        // Force evaluation of the deepest computed
        wd.computedOper<number>(curr);
    }

    return performance.now() - t0;
}



function chainPreact(): number {
  for (let i = 0; i < ITERATIONS; i++) ps(i);
  const root = ps(1); let curr: any = root;
  for (let i = 0; i < DEPTH; i++) { const prev = curr; curr = pc(() => prev.value + 1); }
  const t0 = performance.now();
  for (let w = 0; w < WRITES; w++) { root.value = w; curr.value; }
  return performance.now() - t0;
}

function chainAlien(): number {
  for (let i = 0; i < ITERATIONS; i++) as_(i);
  const root = as_(1); let curr: any = root;
  for (let i = 0; i < DEPTH; i++) { const prev = curr; curr = ac(() => prev() + 1); }
  const t0 = performance.now();
  for (let w = 0; w < WRITES; w++) { root(w); curr(); }
  return performance.now() - t0;
}

const cD = chainDisruptor();
const cW = chainWarlock();
const cP = chainPreact();
const cA = chainAlien();

console.log(`\n=== CHAIN: ${ITERATIONS} allocs, ${DEPTH}-deep, ${WRITES} writes ===`);
console.log(`disruptor: ${cD.toFixed(2)}ms`);
console.log(`warlock:   ${cW.toFixed(2)}ms`);
console.log(`preact:    ${cP.toFixed(2)}ms`);
console.log(`alien:     ${cA.toFixed(2)}ms`);
console.log(`disruptor vs preact: ${(cP / cD).toFixed(2)}x`);
console.log(`disruptor vs alien:  ${(cA / cD).toFixed(2)}x`);
console.log(`warlock vs preact:   ${(cP / cW).toFixed(2)}x`);
console.log(`warlock vs alien:    ${(cA / cW).toFixed(2)}x`);

// ============ PART 2: fan-out ============
const FAN_N = 100_000;
const FAN_WRITES = 1000;

function fanDisruptor(): number {
  const s = d.signal(0);
  const cs: number[] = [];
  for (let i = 0; i < FAN_N; i++) cs.push(d.computed(() => d.signalOper<number>(s)! + 1));
  const t0 = performance.now();
  for (let w = 0; w < FAN_WRITES; w++) {
    d.signalOper(s, w);
    for (let i = 0; i < FAN_N; i++) d.computedOper(cs[i]);
  }
  return performance.now() - t0;
}

function fanWarlock(): number {
    const s = wd.signal(0) as Pid;

    const cs: Pid[] = [];

    for (let i = 0; i < FAN_N; i++) {
        cs.push(
            wd.computed(() => {
                return (wd.signalOper<number>(s) as number) + 1;
            }) as Pid,
        );
    }

    const t0 = performance.now();

    for (let w = 0; w < FAN_WRITES; w++) {
        // Write once
        wd.signalOper(s, w);

        // Read every fan-out computed
        for (let i = 0; i < FAN_N; i++) {
            wd.computedOper<number>(cs[i]);
        }
    }

    return performance.now() - t0;
}

function fanAlien(): number {
  const s = as_(0);
  const cs: any[] = [];
  for (let i = 0; i < FAN_N; i++) cs.push(ac(() => s() + 1));
  for (let i = 0; i < FAN_N; i++) cs[i]();
  const t0 = performance.now();
  for (let w = 0; w < FAN_WRITES; w++) {
    s(w);
    for (let i = 0; i < FAN_N; i++) cs[i]();
  }
  return performance.now() - t0;
}

function fanPreact(): number {
  const s = ps(0);
  const cs: any[] = [];
  for (let i = 0; i < FAN_N; i++) cs.push(pc(() => s.value + 1));
  for (let i = 0; i < FAN_N; i++) cs[i].value;
  const t0 = performance.now();
  for (let w = 0; w < FAN_WRITES; w++) {
    s.value = w;
    for (let i = 0; i < FAN_N; i++) cs[i].value;
  }
  return performance.now() - t0;
}

const fD = fanDisruptor();
const fW = fanWarlock();
const fP = fanPreact();
const fA = fanAlien();

console.log(`\n=== FAN-OUT: 1 signal x ${FAN_N} computeds, ${FAN_WRITES} writes, read-all each ===`);
console.log(`disruptor: ${fD.toFixed(2)}ms`);
console.log(`warlock:   ${fW.toFixed(2)}ms`);
console.log(`preact:    ${fP.toFixed(2)}ms`);
console.log(`alien:     ${fA.toFixed(2)}ms`);
console.log(`disruptor vs preact: ${(fP / fD).toFixed(2)}x`);
console.log(`disruptor vs alien:  ${(fA / fD).toFixed(2)}x`);
console.log(`warlock vs preact:   ${(fP / fW).toFixed(2)}x`);
console.log(`warlock vs alien:    ${(fA / fW).toFixed(2)}x\n`);

// ============ PART 3: propagate sweep ============
function sweepDisruptor(w: number, h: number, iters: number): number {
  const src = d.signal(1);
  for (let i = 0; i < w; i++) {
    let last = src;
    for (let j = 0; j < h; j++) {
      const prev = last;
      last = d.computed(() => d.signalOper<number>(prev)! + 1);
      const node = last;
      d.effectSignal(() => { d.computedOper(node); });
    }
  }
  const t0 = performance.now();
  for (let k = 0; k < iters; k++) d.signalOper(src, d.signalOper<number>(src)! + 1);
  return performance.now() - t0;
}

function sweepWarlock(w: number, h: number, iters: number): number {
  const ww = new WarlockDisruptor(w * h * 4 + w * 8 + 256);
  const src = ww.signal(1);
  for (let i = 0; i < w; i++) {
    let last: any = src;
    for (let j = 0; j < h; j++) {
      const prev = last;
      last = ww.computed(() => prev() + 1);
      const node = last;
      ww.effect(() => { node(); });
    }
  }
  const t0 = performance.now();
  for (let k = 0; k < iters; k++) src(src() + 1);
  return performance.now() - t0;
}

function sweepAlien(w: number, h: number, iters: number): number {
  const src = as_(1);
  for (let i = 0; i < w; i++) {
    let last: any = src;
    for (let j = 0; j < h; j++) {
      const prev = last;
      last = ac(() => prev() + 1);
      const node = last;
      ae(() => { node(); });
    }
  }
  const t0 = performance.now();
  for (let k = 0; k < iters; k++) src(src() + 1);
  return performance.now() - t0;
}

function sweepPreact(w: number, h: number, iters: number): number {
  const src = ps(1);
  for (let i = 0; i < w; i++) {
    let last: any = src;
    for (let j = 0; j < h; j++) {
      const prev = last;
      last = pc(() => prev.value + 1);
      const node = last;
      pe(() => { node.value; });
    }
  }
  const t0 = performance.now();
  for (let k = 0; k < iters; k++) src.value = src.value + 1;
  return performance.now() - t0;
}

const ITERS = 1000;
console.log(`\n=== PROPAGATE SWEEP (${ITERS} writes per test, time in ms, lower = better) ===\n`);
for (const w of [1, 10, 100]) {
  for (const h of [1, 10, 100]) {
    const tD = sweepDisruptor(w, h, ITERS);
    const tW = sweepWarlock(w, h, ITERS);
    const tA = sweepAlien(w, h, ITERS);
    const tP = sweepPreact(w, h, ITERS);
    console.log(`--- ${w} branches, ${h} levels deep ---`);
    console.log(`  Disruptor : ${tD.toFixed(2)} ms`);
    console.log(`  Warlock   : ${tW.toFixed(2)} ms`);
    console.log(`  Alien     : ${tA.toFixed(2)} ms`);
    console.log(`  Preact    : ${tP.toFixed(2)} ms`);
    console.log(`  Disruptor is ${(tA / tD).toFixed(1)}x faster than Alien, ${(tP / tD).toFixed(1)}x faster than Preact`);
    console.log(`  Warlock   is ${(tA / tW).toFixed(1)}x faster than Alien, ${(tP / tW).toFixed(1)}x faster than Preact`);
    console.log('');
  }
}



