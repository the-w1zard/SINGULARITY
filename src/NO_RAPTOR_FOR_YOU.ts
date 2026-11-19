// bus-bench.ts
// FIGHT=... bun --expose-gc bus-bench.ts

const DURATION = 12;
const REPORT_MS = 12;
const BATCH_SIZE = 500_000;
const TOTAL_BATCHES = 500;
const RUN_BARE_METAL_CPU_TEST: Boolean = true;

const pid = 1;
let seq = 0;
const v = ++seq;

type fighter =
  | "BASELINE"
  | "BASELINE_FLAT"
  | "DIRECT_REF"
  | "G_BUS"
  | "BUS_CHANNEL"
  | "BUS_CHANNEL_REUSE"
  | "BUS_FIELD"
  | "BUS_FIELD_REUSE"
  | "BUS_FIELD2"

const FIGHT: fighter = (process.env.FIGHT as fighter) ?? "BUS_FIELD2";


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
// BUSES
// ─────────────────────────────────────────────────────────────
// 1. BASELINE — no bus, envelope allocated
class BASELINE {
  call(p: U): number {
    return handler(p);
  }
}
// 2. BASELINE_FLAT — no bus, no envelope
class BASELINE_FLAT {
  call(p: U): number {
    return handleFlat(p);
  }
}
// 3. DIRECT_REF — handler captured at boot, no this, no lookup
const directRef = handleFlat;
function DIRECT_REF(p: U): number {
  return directRef(p);
}
// 4. G_BUS — your original (channels[k].handler, allocates envelope)
class G_BUS {
  public s = s; public m = m; public r = r; public k = k0;
  public channels: Record<S, BUS_CHANNEL> = Object.create(null);
  constructor() { this.channels[k0] = { handler: handler }; }
  send(p: U): number {
    const h = this.channels[k0]?.handler;
    return h(p);
  }
}
// 5. BUS_CHANNEL — channels[k].handler, envelope
class BUS_CHANNEL_CLASS {
  public s = s; public m = m; public r = r; public k = k0;
  public channels: Record<S, BUS_CHANNEL> = Object.create(null);
  constructor() { this.channels[k0] = { handler: handler }; }
  send(p: U): number {
    const h = this.channels[k0]?.handler;
    return h(p);
  }
}
// 6. BUS_CHANNEL_REUSE — channels[k].handler, reused envelope
class BUS_CHANNEL_REUSE {
  public s = s; public m = m; public r = r; public k = k0;
  public channels: Record<S, BUS_CHANNEL> = Object.create(null);
  private _msg: SIG = { r, m, s, k: k0, p: { p: 0 } };
  constructor() { this.channels[k0] = { handler: handler }; }
  send(p: U): number {
    const h = this.channels[k0]?.handler;
    this._msg.p = p;
    return h(p, this._msg);
  }
}
// 7. BUS_FIELD — this.update(p), no lookup, field call
class BUS_FIELD {
    public s = s; public m = m; public r = r; public k = k0;
    public update: H = handler;
    public channels: Record<S, BUS_CHANNEL> = Object.create(null);
    constructor() { this.channels[k0] = { handler: handler }; }
    emit_update(p: U): number { return this.update(p); }
    emit(p: U): number { return this.channels[this.k].handler!(p); }
  }
// 8. BUS_FIELD_REUSE — this.update(p, reused msg)
class BUS_FIELD_REUSE {
  public s = s; public m = m; public r = r; public k = k0;
  public update: H = handle2;
  private _msg: SIG = { r, m, s, k: k0, p: { p: 0 } };
  emit_update(p: U): number {
    this._msg.p = p;
    return this.update(p);
  }

  
}

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

  console.log(`\n------------ ${name} ---------------`);
  console.log(`Peak (Best Batch):                      ${(bestOpsPerSec / 1e6).toFixed(2)} M ops/sec`);
  console.log(`Average:                                ${(avg / 1e6).toFixed(2)} M ops/sec`);
  console.log(`---------------------------------------------------------\n`);
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

if (FIGHT === "BASELINE") {
  const b = new BASELINE();
  bench("BASELINE.call", () => { b.call({ p: v }); });
}
else if (FIGHT === "BASELINE_FLAT") {
  const b = new BASELINE_FLAT();
  bench("BASELINE_FLAT.call", () => { b.call({ p: v }); });
}
else if (FIGHT === "DIRECT_REF") {
  bench("DIRECT_REF", () => { DIRECT_REF({ p: v }); });
}
else if (FIGHT === "G_BUS") {
  const bus = new G_BUS();
  bench("G_BUS.send", () => { bus.send({ p: v }); });
}
else if (FIGHT === "BUS_CHANNEL") {
  const bus = new BUS_CHANNEL_CLASS();
  bench("BUS_CHANNEL_CLASS.send", () => { bus.send({ p: v }); });
}
else if (FIGHT === "BUS_CHANNEL_REUSE") {
  const bus = new BUS_CHANNEL_REUSE();
  const payload = { p: 0 };
  bench("BUS_CHANNEL_REUSE.send", () => { payload.p = v; bus.send(payload); });
}
else if (FIGHT === "BUS_FIELD") {
  const bus = new BUS_FIELD();
  bench("BUS_FIELD.emit_update", () => { bus.emit_update(v

  ); });
}
else if (FIGHT === "BUS_FIELD2") {
  const bus = new BUS_FIELD();
  bench("BUS_FIELD.emit_update", () => { bus.emit(v); });
}
else if (FIGHT === "BUS_FIELD_REUSE") {
  const bus = new BUS_FIELD_REUSE();
  const payload = { p: 0 };
  bench("BUS_FIELD_REUSE.emit_update", () => { payload.p = v; bus.emit_update(payload); });
}
