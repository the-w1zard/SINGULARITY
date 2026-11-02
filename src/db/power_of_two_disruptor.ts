export class POWER_OF_TWO {
/*  =============================================================================
     power_of_two_disruptor.ts:
 
     --- DisruptorSoA.editDirect ---
     Peak (Best Batch): 848.46 M ops/sec
     Average:           725.71 M ops/sec
 
     0 pass
     0 fail
     Ran 0 tests across 1 file. [641.00ms]
    =============================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [POWER_OF_TWO] */
    public readonly capacity: number;
    public readonly mask: number;
    // ── Structure of Arrays (SoA) Event Lanes ──────────────────────────────
    public readonly pids: Int32Array;
    public readonly values: Float64Array;
    public readonly flags: Uint8Array;
    // ── Cursors ────────────────────────────────────────────────────────────
    public writeCursor = 0;
    public readCursor = 0;

    constructor(capacity = 65536) {// Enforce power-of-two for bitwise masking
        if ((capacity & (capacity - 1)) !== 0) {throw new Error("Disruptor capacity must be a power of two.");}
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.pids = new Int32Array(capacity);// Pre-allocate contiguous typed memory blocks (Zero GC churn)
        this.values = new Float64Array(capacity);
        this.flags = new Uint8Array(capacity);
    }// Publishes an event directly into the next ring slot. O(1) time complexity, zero heap allocation, single CPU instruction cycle.
    public publish(pid: number, value: number, flag = 0): boolean {// Optional backpressure check: Prevent writer from overlapping reader
        if (this.writeCursor - this.readCursor >= this.capacity) {return false;}// Ring buffer full
        const index = this.writeCursor & this.mask;
        this.pids[index] = pid;// Write directly to parallel SoA typed array lanes
        this.values[index] = value;
        this.flags[index] = flag;
        this.writeCursor++;
        return true;
    }// Drains and processes the ring buffer in batches using a zero-allocation loop.
    public consume(handler: (pid: number, value: number, flag: number) => void): number {
        const targetWrite = this.writeCursor;
        let currentRead = this.readCursor;
        if (currentRead === targetWrite) {return 0;}// Nothing to consume 
        let processed = 0;
        while (currentRead < targetWrite) {
            const index = currentRead & this.mask;// Execute handler on raw lane data
            handler(this.pids[index],this.values[index],this.flags[index]);
            currentRead++;
            processed++;
        }
        this.readCursor = currentRead;
        return processed;
    }// Resets cursors and ring lanes instantly without reallocating memory.
    public reset(): void {
        this.writeCursor = 0;
        this.readCursor = 0;
        this.pids.fill(0);
        this.values.fill(0);
        this.flags.fill(0);
    }
}
const RUN_BARE_METAL_CPU_TEST = false
/* =============================================================================
 this bench has been optimized to allow your fn to run at the absolute fucking
 by not impeding on its results. This bench does not provide a running log
 as the test takes place. Instead, it reads the current write speeds in a typed
 format, pushing them into an array and only once the test has concluded, 
 does it then take the information out, avg the results, determine what your 
 highest figure was, and reports its findings.
 opts for bare metal cpu test
=========================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BARE METAL CPU BENCH] */
const BATCH_SIZE = 500_000 //1_000_000; //                         1 million ops per batch 
//                                                    to keep overhead near zero
const TOTAL_BATCHES = 800;//                                Total batches to run
/* =============================================================================
 Once the test starts, reports what the running avgs and highest figure attained.
 set for as long as you need. If testing anything that will pull in figures in
 excess of 1-10 mil ops/sec, THE TEST NEEDS TO BE ATLEAST 10 SECS. Preferably
 20-30 seconds. Anything less than 10... is just not enough time since v8
 switches over into its turbo runtime at the 6 second mark... at which time
 it dips, a lot, and then starts to climb again. At or around the 10 sec mark
 is when... your engine has hit its peak and will not go any higher than the
 results you have attained at this point within the test. If you don't want
 to do 20-30 secs, I would set it to 12 as a min.
======================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BENCH] */
const DURATION =  12;
const REPORT_MS = 1;
const BATCH = 1;
const CAPACITY = 1024
// 1024;
// 65536 
// #endregion
//                                                                                CONSTANTS: pid, v, ssq
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const pid = 1;
let seq = 0;
const v = ++seq;
// #endregion
//                                                                                                 BENCH
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const dur = 1000 * DURATION;
const report = 1000 * REPORT_MS;
function BARE_METAL_CPU(name: string, write: () => void): void {
    const DURATION = dur
    const REPORT_MS = report
    const BATCH = 1;
    const CAPACITY = 1024;
    const totalOps = BATCH_SIZE * TOTAL_BATCHES;
    const batchTimes = new Float64Array(TOTAL_BATCHES);// Pre-allocate typed arrays so there's zero GC or allocation during the run
    for (let i = 0; i < 2_000_000; i++) {        write();    }// Warm-up pass
    const globalStart = process.hrtime.bigint();
    for (let b = 0; b < TOTAL_BATCHES; b++) {// --- HOT LOOP ---
        const batchStart = process.hrtime.bigint();
        for (let i = 0; i < BATCH_SIZE; i++) {write();}
        batchTimes[b] = Number(process.hrtime.bigint() - batchStart);
    }// --- END HOT LOOP ---
    const globalEnd = process.hrtime.bigint();
    let bestOpsPerSec = 0;// Post-processing: Calculate highest peak and average AFTER everything is done
    for (let b = 0; b < TOTAL_BATCHES; b++) {
        const seconds = batchTimes[b] / 1e9;
        const opsPerSec = BATCH_SIZE / seconds;
        if (opsPerSec > bestOpsPerSec) {
            bestOpsPerSec = opsPerSec;
        }
    }
    const totalSeconds = Number(globalEnd - globalStart) / 1e9;
    const avgOpsPerSec = totalOps / totalSeconds;
    console.log(`\n--- ${name} ---`);
    console.log(`Peak (Best Batch): ${(bestOpsPerSec / 1e6).toFixed(2)} M ops/sec`);
    console.log(`Average:           ${(avgOpsPerSec / 1e6).toFixed(2)} M ops/sec`);
}
function BENCH(name: string, write: () => void): void {
    let windowOps = 0;
    let totalOps = 0;
    let best = 0;
    let elapsedTotal = 0;

    let start = process.hrtime.bigint();
    let windowStart = start;
    let cpuStart = process.cpuUsage();

    console.log(`\n${name}`);
    console.log("sec | M writes/s | best M/s | avg M/s | CPU %");

    while (elapsedTotal < dur) {
        for (let i = 0; i < 100_000; i++) {
            write();
        }

        windowOps += 100_000;
        totalOps += 100_000;

        const now = process.hrtime.bigint();
        const elapsedUs = Number(now - windowStart) / 1_000;
        elapsedTotal = Number(now - start) / 1e6;

        if (elapsedUs >= report * 1_000) {
            const mp = windowOps / elapsedUs;
            const avg = totalOps / (Number(now - start) / 1e9) / 1e6;
            const cpuNow = process.cpuUsage();
            const cpuUs =
                cpuNow.user - cpuStart.user +
                cpuNow.system - cpuStart.system;

            best = Math.max(best, mp);

            console.log(
                String(Math.floor(elapsedTotal / 1_000)).padStart(3), "|",
                mp.toFixed(2).padStart(10), "|",
                best.toFixed(2).padStart(8), "|",
                avg.toFixed(2).padStart(7), "|",
                (cpuUs / elapsedUs * 100).toFixed(1).padStart(5),
            );

            windowOps = 0;
            windowStart = now;
            cpuStart = cpuNow;
        }
    }
}
const bench = RUN_BARE_METAL_CPU_TEST === false ? BENCH : BARE_METAL_CPU 
// #endregion
//                                                                                                  TEST
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const DISRUPTOR = new DisruptorSoA(CAPACITY)

bench("DisruptorSoA.editDirect", () => {	
    DISRUPTOR.publish(pid , v );
    //DISRUPTOR.consume((pid, value, flag) => {      });
}); 


