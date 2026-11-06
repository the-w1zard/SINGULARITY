/*  ============================================================================================================================
	 YOU ARE REQUIED TO READ THIS ENTIRE FILE BEFORE YOU EVEN START
	 We must make the solution feel as if everything that the user interacts with is simple and easy, which means BECAUSE IT IS
	 STILL A JAVASCRIPT APPLICATION/PLATFORM, THE USERS WILL INTERACT WITH THIS AS IF... IT IS A JAVASCRIPT APPLICATION.
	 Meaning that:
	 1. whenever we take any data in, we have to encode it with one fo the endcoder or convert it over to binary. 
	 2. when the data reachs back to the user, we have to give it back to them, the same shape andn form that they gave it to us
	 3. jsut because there is a bit of complexity under the first layer, doesn't mean we expose that complexity back to the 
	 user

	 Which means, the same usage paths that have been forged out, ie easy path everywhere making the best ux and dx humanly 
	 possible, THAT HAS TO CONTINUE... 

	 Darth vader is the disruptor we are using for signals, state and so on so we have to prep that first, and then 
	 prep the signals solution in order to fit it.
	 EVERY VAALUE THAT DARTH VADER IS ASSIGNED TO HAVE, MUST GO THROUGH DARTH VADER. 
	 We have a binary slab for ONLY WHEN VALUES EXCEED 8152 WEIGHT LIMIT, OR WHEN THE USER SPCIFICALLY SPECIFIES FOR THAT
	 STORAGE MEDIUM.

	 SO FIRST TASK, LETS JUST FOCUS ON THAT... LETS GET THE DISRUPTOR READY FIRST...
	 so it needs all the functionality a reactive platform requires, so we need reads, rights
	 this disruptor is going to be the most complex, architecural wise anyways, due to the fact that... in place of how a typical
	 disruptor runs where lets say it has 16 ops to complete, right?
	 that would mean the disruptor would have 15 passes of the lane sequentually to complete...
	 we cannot afford to do that... that is just simply too ineffecient, since this disruptor has hit a value of 2.032 billion ops/sec
	 2.032 BILLLION OPS PER SECOND... so instead of 16 operations back to back... we are going to go... 16 WIDE... so we need
	 to create a class that will house darth vaders 16 wide lane disruptor... WHAT THAT MEANS IS THAT, A CLASS NEEDS TO BE
	 CREATED... AND THAT CLASS WILL CREATE... 16 DARTH VADER INSTANCES, ONE FOR EACH OP... EACH DARTH VADER WILL HAVE ITS OWN QUEUE
	 EVERYTHING... MAKING IT SUPPER EFFEICEINT
	 WE HAVE cbor and avro, the latter being for the bus obviously...
	 we have map replacements, we have boolean replacment, we have set replacments we have replacemnts for th enested mappings
	 if u know of a faster encoder let me know


	 IN OTHER WORDS WHAT THIS MEANS IS.... THE API ON DARTH VADER... NEEDS TO RESEMBLE... AN API... FOUND ON A REACTIVE SOLUTOIN... OK?
	 TO CONVERT 4 CHANNEL FORMAT TO PIDS... SCROLL FUCKING DOWN.... ITS THE VERY FIRST FUCKING CLASS

	 YOU ARE NOT ALLOWED TO BUILD A BINARY SLAB OR A SLAB OF ANY KIND. WHEN I SAY SLAB, A SLAB IS A STORAGE MEDIUM SO NO YOUR NOT ALLOW ED TO BUILD THAT.
	 BEFORE YOU GO OFF AND WILDLY BUILD FUCKING 4 CODEC... THAT WE ALREADY HAVE OR SIMPLY DO NOT NEED... U HAVE TO GET APPROVAL FIRST 
	 AS WE ALREADY HAVE SEVERAL CODECS... OK?
	 BEFORE YOU START TOCUHING THE CODE... U NEED TO ANSWER THIS QUESTION, DO U UNDERSTAND WHAT THE FUCK IT IS... THAT U ARE SUPPOSED TO DO?
	==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [SITH] */
	/*  ==== SITH - THE POWER OF 2 =============================================================================================
	 SITH - THE POWER OF 2
     Extremely fitting really, as this is the path that fiends raw power and is forever on the quest to acquire as much as 
     they possibly can.
     JEDI being the full featured, knows everything message/bus. SITH instead runs bare metal. No features, no anything of 
     any kind. Fastest path that just happens to run on 13 disruptors, which is explained below.
     900 is responsible for higher throughput items since it has a slightly higher max ops per sec, so state, spreadsheet,
     drs and so on. 
     The power of 2 will specialize on the bus side of things. This is also how the rest of the app gains its disruptor,
     same app boundary layer.
     While Sith master and apprentice, takes care of the same node, different app layer... feature two... the power of 2
     disruptors. Using jedi to stand themselves up. This part has already been completed.

     With sith... we are going... full tilt... PURE FUCKING BINARY BABY...
     that means... sauron is getting replaced with a binary stand in...
     the cloud, is getting replaced with all binary counterparts...
     spreadsheet is being moved into SITH... AND also being converted to 100% binary...
     NO USE OF MAPS, NO USE OF JSON... since we already have secured replacements for them. Everything stored in binary,
     or encoded in binary. 
     IN ADDITION to that, there is also an entire disruptor... binary... signals variant also coming into play...
     As we are truly getting demented with just how far do we push it...
     using soa typed arrays, disruptors, full binary map replacements... everything slow... must be fucking gone...
     

     there is currently some implemeantions in the notebook... we can or not, choose to use them... but we need to start
     replacing everything one by one... and we are goiong to start at the top...
     one distinction though... this is still a javascript app... so, the user will still supply javascript like
     data values... so we have to encode everything on the way in, including converting the 4 scoped channel format...
     into pids... and then from pids... back to the 4 channel scoped format and giving them back their original value 
     that it was before we encoded it...
     another hard rule, whatever we do convert... the usage must remain the same... yes, this is binary... yes it 
     is far more confusing and complicated... but we cannot dump that on the user... 
	==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [SITH] */
/*  ==== BINARY STRATS ========================================================================================================
	 When you are building a custom, zero-dependency, high-throughput JavaScript platform handling everything from a reactive state engine and distributed state sync to a message queue and spreadsheet engine, **CBOR** and **Avro** serve completely different masters.

	 If you are evaluating them alongside custom byte slabs (`BEAM_CODEC`), here is how they split the workload across your architecture:

	 1. CBOR (Concise Binary Object Representation / e.g., `cbor-x`)

	    - What it’s used for: Dynamic, schema-less, JSON-like object transport and local persistence.
	    - Where it fits in your platform: 
	    - Inter-thread / Worker Communication: Passing arbitrary configuration payloads or state patches between Web Workers or Node worker threads without the overhead of JSON stringify/parse.
	    - Local Workspace / State Persistence: Dumping ad-hoc user documents, workspace settings, or cache graphs to disk or IndexedDB where schemas change fluidly and you don't want to enforce rigid compilation.
	    - Network Sync / WebSockets: Streaming dynamic JSON-compatible objects back and forth across the wire when the structure isn't fixed ahead of time.

	 Why it fits your vibe: Libraries like `cbor-x` use shared-structure packing (remembering object keys so they don't repeat them over and over). It acts as a high-speed drop-in replacement for JSON, but drops binary data straight into `Uint8Array` containers natively.

	 2. Apache Avro (`avsc` / schema-driven binary)

	    - What it’s used for:** Rigid, schema-enforced, streaming data contracts.
		- Where it fits in your platform:**
		- The Message Queue (MQ) & Event Bus Payload:** Because Avro relies on a strict schema (JSON-defined types), the encoded binary payload drops all field names entirely—it's *just* the raw values packed consecutively. This makes payloads microscopic.
		- Distributed State Sync (`useDRS`) & Replication Logs:** When nodes in your distributed system are shipping state mutation logs or database WAL (Write-Ahead Log) entries to one another, Avro guarantees that both ends agree precisely on the layout.
		- Schema Evolution:** If a spreadsheet column type or database schema changes tomorrow, Avro handles backward/forward compatibility natively out of the box.

	 The Catch in JS:** Pure JavaScript Avro implementations (`avsc`) have to parse schemas and manage object mappings, which introduces CPU overhead unless you are sliding the raw binary blocks directly into typed arrays.
 
	 How They Map to Your Engine Stack

	 Best Codec Match Per Component in Your Platform

	 - Spreadsheet Engine & Database Columns
	 	- BEAM_CODEC - Zero-copy. You want raw contiguous `Float64Array` / `Int32Array` memory chunks that feed directly into SIMD-style math loops with zero decoding penalty.

	 - Message Queue & Distributed Replication Log
	 	- Avro - Strict schemas, zero field-name bloat, maximum compression on the wire, and bulletproof schema evolution across distributed nodes.

	 - Client-Server State Sync & Local Storage Cache
	 	- CBOR - Flexible, schema-less data structures, fast JS performance, handles arbitrary nested maps/objects without crashing when the data shape mutates.

	 You've essentially built a three-tier weaponry system: **SoA Byte Slabs** for absolute bare-metal execution inside the hot loop, **Avro** for strict schema-driven transport across the message queue, and **CBOR** for high-speed flexible data serialization on the edges.
	==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [SITH] */
//                                                                                               IMPORTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
// import  Store from "./sith/store";
import Store from "@the-w1zard/store"
// #endregion
//                                                                                                 [BUS]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
/*  =====================================================================================================
	 HADRON_DISRUPTOR
	 What it moves: BUS
	 How it works: Pure parallel typed arrays (Float64Array, Uint32Array). Zero parsing, zero object 
	 allocations, zero decoding. Data drops straight into pre-allocated memory arenas where the CPU 
	 can rip through math and propagation loops via direct index lookups.
    ================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE HOT PATH ENGINE DISRUPTOR / ZERO COPY ARENA] */
/* 	--------------------------------------------------------------------------
	 Record layout — 8 x Int32 = 32 bytes.
  
	 0 opcode
	 1 pid
	 2 slot
	 3 a — primary payload / f64 arena offset / u32 arena offset
	 4 b — secondary payload / length / count
	 5 c — tertiary payload / flags
	 6 epoch — tracer clock at publication
	 7 reserved / checksum / extension
	-------------------------------------------------------------------------- */
class HADRON_DISRUPTOR {
	readonly capacity: N;
	readonly mask: N;
	readonly pidCapacity: N;
	readonly schedule: readonly N[];
	readonly shared: B;
	private readonly rings: I32[] = [];
	/*
	  Per-lane shared control block layout:
  
	  0 nextClaim
	  1 gatingSequence  — consumer progress
	  2 cachedAvailableSequence
	  3 publishedHighest
	  4 fullStalls
	  5 claimed
	  6 published
	  7 consumed
	  8 batches
	  9 maxBatch
	 */
	private static readonly CTRL = 16;
	private readonly ctrl: I32;
	private readonly available: I32[] = [];
	private readonly clock: I32;
	private readonly phaseIndex: I32;
	private readonly running: I32;
	private readonly pidToSlot: I32;
	private readonly dirty: U32;
	private readonly f64: F64;
	private readonly u32: U32;
	private readonly i32: I32;
	private readonly bytes: U8;
	private f64Tail = 0;
	private u32Tail = 0;
	private i32Tail = 0;
	private byteTail = 0;
	private readonly maxBatch: N;
	private readonly wait: WaitStrategy;
	private handlers: AR< | ((ring: I32, from: N, to: N, epoch: N, d: HADRON_DISRUPTOR ) => V) | NU > = [];
	constructor(options: HADRON_OPTIONS = {}) {
	  const capacity = options.capacity ?? 1 << 18;
	  const pidCapacity = options.pidCapacity ?? 1 << 20;
  
	  if (!pow2(capacity)) throw new Error("capacity must be a power of two");
	  if (!pow2(pidCapacity)) throw new Error("pidCapacity must be a power of two");
  
	  this.capacity = capacity;
	  this.mask = capacity - 1;
	  this.pidCapacity = pidCapacity;
  
	  this.schedule = options.schedule ?? DEFAULT_SCHEDULE;
	  this.shared = options.shared ?? typeof SharedArrayBuffer !== "undefined";
  
	  this.maxBatch = options.maxBatch ?? 8192;
	  this.wait = options.wait ?? "yield";
  
	  const AB = this.shared ? SharedArrayBuffer : ArrayBuffer;
  
	  const laneCount = this.schedule.length;
  
	  this.rings = Array.from(
		{ length: laneCount },
		() => new Int32Array(new AB(capacity * REC * 4))
	  );
  
	  this.ctrl = new Int32Array(new AB(laneCount * HADRON_DISRUPTOR.CTRL * 4));
  
	  // Availability buffer uses one Int32 per ring slot.
	  this.available = Array.from(
		{ length: laneCount },
		() => new Int32Array(new AB(capacity * 4))
	  );
  
	  this.clock = new Int32Array(new AB(4));
	  this.phaseIndex = new Int32Array(new AB(4));
	  this.running = new Int32Array(new AB(4));
  
	  this.pidToSlot = new Int32Array(new AB(pidCapacity * 4));
	  this.pidToSlot.fill(-1);
  
	  this.dirty = new Uint32Array(new AB((pidCapacity >>> 5) * 4));
  
	  this.f64 = new Float64Array(new AB((options.f64Capacity ?? 1 << 18) * 8));
	  this.u32 = new Uint32Array(new AB((options.u32Capacity ?? 1 << 18) * 4));
	  this.i32 = new Int32Array(new AB((options.i32Capacity ?? 1 << 18) * 4));
	  this.bytes = new Uint8Array(new AB(options.byteCapacity ?? 1 << 20));
	}
  /*======================================================================
	 HANDLERS
	====================================================================== */
	sub( op: OpKind, handler: ( ring: I32, from: N, to: N, epoch: N, d: HADRON_DISRUPTOR ) => V  ): this {
	  this.handlers[op] = handler;
	  return this;
	}
  /*======================================================================
	 MULTI-PRODUCER CLAIM / PUBLISH
	 Claim the next sequence for a lane. This is a true multi-producer sequencer: - CAS reserves a unique sequence. - The availability buffer marks that exact sequence published. - Consumers never rely on a single global “highest published” value.
	====================================================================== */
	private ctrlAt(lane: N, field: number): number { return lane * HADRON_DISRUPTOR.CTRL + field;  }
	claim(lane: number): number {
	  const next = this.ctrlAt(lane, 0);
	  const stall = this.ctrlAt(lane, 4);
	  const claimed = this.ctrlAt(lane, 5);
  
	  let spin = 0;
  
	  for (;;) {
		const seq = Atomics.load(this.ctrl, next);
		const wrap = seq - this.capacity;
		const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
  
		if (gate > wrap) {
		  if (Atomics.compareExchange(this.ctrl, next, seq, seq + 1) === seq) {
			Atomics.add(this.ctrl, claimed, 1);
			return seq;
		  }
		  continue;
		}
  
		Atomics.add(this.ctrl, stall, 1);
  
		if (this.wait === "spin") {
		  spin++;
		  continue;
		}
  
		if (this.wait === "yield") {
		  this.yieldNow();
		  continue;
		}
  
		Atomics.wait(this.ctrl, next, seq, 1);
		spin = 0;
	  }
	}
	// Publish a claimed record. The record must be fully written BEFORE this call. The availability buffer is the publication barrier.
	publish(lane: number, seq: number): void {
	  const avail = this.available[lane];
	  Atomics.store(avail, seq & this.mask, seq + 1);
  
	  const pub = this.ctrlAt(lane, 6);
	  Atomics.add(this.ctrl, pub, 1);
  
	  const waiting = this.ctrlAt(lane, 0);
	  Atomics.notify(this.ctrl, waiting);
	}
	// One-shot enqueue. Returns: >= 0 — published sequence  -1  — nonblocking mode and lane was full
	emit( op: OpKind, pid: N, slot: N, a = 0, b = 0, c = 0, ext = 0, block = true  ): N {
	  const lane = this.laneFor(op);
	  const ring = this.rings[lane];
  
	  if (!block && this.isFull(lane)) return -1;
  
	  const seq = this.claim(lane);
	  const base = (seq & this.mask) * REC;
	  const epoch = Atomics.load(this.clock, 0);
  
	  ring[base + F_OP] = op;
	  ring[base + F_PID] = pid;
	  ring[base + F_SLOT] = slot;
	  ring[base + F_A] = a;
	  ring[base + F_B] = b;
	  ring[base + F_C] = c;
	  ring[base + F_EXT] = ext;
  
	  this.publish(lane, seq);
	  return seq;
	}
  /*======================================================================
	 PID / SLOT / DIRTY
	====================================================================== */
	bind(pid: N, slot: N): V { this.pidToSlot[pid] = slot;  }
	unbind(pid: N): V { this.pidToSlot[pid] = -1;  }
	slot(pid: N): N { return this.pidToSlot[pid];  }
	dirtyMark(pid: N): V { this.dirty[pid >>> 5] |= 1 << (pid & 31);  }
	dirtyPeek(pid: N): B { return (this.dirty[pid >>> 5] & (1 << (pid & 31))) !== 0;  }
	dirtyTake(pid: N): B {
	  const w = pid >>> 5;
	  const bit = 1 << (pid & 31);
	  const word = this.dirty[w];
  
	  if ((word & bit) === 0) return false;
  
	  this.dirty[w] = word & ~bit;
	  return true;
	}
  /*======================================================================
	 SOA ARENAS
	====================================================================== */
	allocF64(value: N): N {
	  const at = this.f64Tail++;
	  this.f64[at] = value;
	  return at;
	}
	allocU32(value: N): N {
	  const at = this.u32Tail++;
	  this.u32[at] = value;
	  return at;
	}
	allocI32(value: N): N {
	  const at = this.i32Tail++;
	  this.i32[at] = value;
	  return at;
	}
	allocBytes(data: U8): N {
	  const at = this.byteTail;
	  this.bytes.set(data, at);
	  this.byteTail += data.length;
	  return at;
	}
	f64At(offset: N): N {    return this.f64[offset];  }
	u32At(offset: N): N {    return this.u32[offset];  }
	i32At(offset: N): N {    return this.i32[offset];  }
	bytesAt(offset: N, length: N): U8 {    return this.bytes.subarray(offset, offset + length);  }
  
  /*======================================================================
	 CONSUMER BARRIER
	 Return the highest contiguous available sequence for a lane. This scans only the availability window, then caches the result. It avoids the classic problem of a slow producer blocking visibility of later producers.
	====================================================================== */
	private availableThrough(lane: number): number {
	  const ctrlBase = this.ctrlAt(lane, 0);
	  const gateField = this.ctrlAt(lane, 1);
	  const cacheField = this.ctrlAt(lane, 2);
  
	  const next = Atomics.load(this.ctrl, ctrlBase);
	  let gate = Atomics.load(this.ctrl, gateField);
	  let cached = Atomics.load(this.ctrl, cacheField);
  
	  if (cached >= gate) {
		const avail = this.available[lane];
		const cap = this.capacity;
  
		let scan = cached;
		const limit = Math.min(next, cached + this.maxBatch);
  
		while (scan < limit) {
		  const flag = Atomics.load(avail, scan & (cap - 1));
		  if (flag <= scan) break;
		  scan++;
		}
  
		cached = scan;
		Atomics.store(this.ctrl, cacheField, cached);
	  }
  
	  return cached;
	}/*======================================================================
	   PHASE-LOCKED EXECUTION
	   Drain exactly one scheduled phase. A phase is homogeneous: READ only, WRITE only, DELETE only, etc.
	   ====================================================================== */
	drainPhase(limit = this.maxBatch): number {
	  const schedule = this.schedule;
	  const lane = schedule[Atomics.load(this.phaseIndex, 0)];
	  const handler = this.handlers[lane];
  
	  if (!handler) {
		this.nextPhase();
		return 0;
	  }
  
	  const ring = this.rings[lane];
	  const gateField = this.ctrlAt(lane, 1);
  
	  const available = this.availableThrough(lane);
	  const head = Atomics.load(this.ctrl, gateField);
  
	  const end = Math.min(available, head + limit);
  
	  if (end <= head) {
		this.nextPhase();
		return 0;
	  }
  
	  const epoch = Atomics.load(this.clock, 0);
  
	  // Batch boundary is handed to the handler.
	  // The handler can specialize contiguous execution.
	  handler(ring, head, end, epoch, this);
  
	  Atomics.store(this.ctrl, gateField, end);
  
	  const consumedField = this.ctrlAt(lane, 7);
	  const batchField = this.ctrlAt(lane, 8);
	  const maxBatchField = this.ctrlAt(lane, 9);
  
	  const count = end - head;
  
	  Atomics.add(this.ctrl, consumedField, count);
	  Atomics.add(this.ctrl, batchField, 1);
  
	  const prevMax = Atomics.load(this.ctrl, maxBatchField);
	  if (count > prevMax) Atomics.store(this.ctrl, maxBatchField, count);
  
	  this.nextPhase();
	  return count;
	}// Run one full revolution. The tracer particle advances only after every scheduled phase runs.
	revolution(limit = this.maxBatch): number {
	  let total = 0;
  
	  for (let i = 0; i < this.schedule.length; i++) {
		total += this.drainPhase(limit);
	  }
  
	  Atomics.add(this.clock, 0, 1);
	  return total;
	}// Run until all lanes are drained. Useful for shutdown, transaction boundaries, and tests.
	drainAll(limit = this.maxBatch): number {
	  let total = 0;
  
	  for (;;) {
		const before = total;
		total += this.revolution(limit);
		if (total === before) break;
	  }
  
	  return total;
	}
  /*======================================================================
	 CLOCK / CONTROL
	====================================================================== */
	tick(): number { return Atomics.load(this.clock, 0);  }
	currentPhase(): OpKind { return this.schedule[Atomics.load(this.phaseIndex, 0)];  }
	start(): void {    Atomics.store(this.running, 0, 1);  }
	stop(): void {
	  Atomics.store(this.running, 0, 0);
	  Atomics.notify(this.running, 0);
	}
	isRunning(): boolean {    return Atomics.load(this.running, 0) === 1;  }
  /*======================================================================
	 STATS
	====================================================================== */
	stats(lane: number): LANE_STATS {
	  const b = lane * HADRON_DISRUPTOR.CTRL;
  
	  return {
		claimed: Atomics.load(this.ctrl, b + 5),
		published: Atomics.load(this.ctrl, b + 6),
		consumed: Atomics.load(this.ctrl, b + 7),
		batches: Atomics.load(this.ctrl, b + 8),
		maxBatch: Atomics.load(this.ctrl, b + 9),
		fullStalls: Atomics.load(this.ctrl, b + 4),
	  };
	}
	pending(lane: number): number {
	  const published = Atomics.load(this.ctrl, this.ctrlAt(lane, 6));
	  const consumed = Atomics.load(this.ctrl, this.ctrlAt(lane, 7));
	  return published - consumed;
	}
  /*======================================================================
	 INTERNAL
	====================================================================== */
	private laneFor(op: OpKind): number {    return op;  }
	private isFull(lane: number): boolean {
	  const next = Atomics.load(this.ctrl, this.ctrlAt(lane, 0));
	  const gate = Atomics.load(this.ctrl, this.ctrlAt(lane, 1));
	  return next - gate >= this.capacity;
	}
	private nextPhase(): void {
	  const next = (Atomics.load(this.phaseIndex, 0) + 1) % this.schedule.length;
	  Atomics.store(this.phaseIndex, 0, next);
	}
	private yieldNow(): void {
	  if (typeof scheduler !== "undefined" && "yield" in scheduler) {
		scheduler.yield();
	  } else if (typeof setImmediate === "function") {
		setImmediate(() => {});
	  } else {
		setTimeout(() => {}, 0);
	  }
	}
}
/*  ============================================================================================
	 What it moves: Message queue events, distributed replication logs (useDRS), database WAL entries, 
	 and inter-node state sync packets.
	 How it works: Rigid, schema-enforced binary streams. Field names are stripped entirely from the 
	 wire, leaving micro-optimized byte payloads that guarantee strict structural contracts across 
	 network boundaries.
    ======= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DISTRIBUTED STATE & MSGING DISRUPTOR (SCHEMA/AVRO PIPLINE)] */
type AVROField = { name: S; type: AVROSchema };
type AVROSchema = S | AVROSchema[] | {
		type: S | AVROSchema[];
		name?: S;
		namespace?: S;
		fields?: AVROField[];
		items?: AVROSchema;
		values?: AVROSchema;
		symbols?: AR<S>;
		size?: N;
};
type AVROValue = any;
class AVROWriter {
		private buf = new Uint8Array(256);
		private pos = 0;
		private ensure=(length: N): V =>{
			if (this.pos + length <= this.buf.length) return;
			let size = this.buf.length;
			while (size < this.pos + length) size *= 2;
			const next = new Uint8Array(size);
			next.set(this.buf);
			this.buf = next;
		}
		get offset(): number {
			return this.pos;
		}
		byte =(v: N): V =>{
			this.ensure(1);
			this.buf[this.pos++] = v;
		}
		bytes=(v: Uint8Array): V=> {
			this.ensure(v.length);
			this.buf.set(v, this.pos);
			this.pos += v.length;
		}
		long=(value: number | bigint): V =>{
			let n = BigInt(value);
			if (n < -(1n << 63n) || n > (1n << 63n) - 1n) throw new Error("Avro: long out of range");
			n = n >= 0n ? n << 1n : ((-n) << 1n) - 1n;
			while (n >= 0x80n) {
				this.byte(Number(n & 0x7fn) | 0x80);
				n >>= 7n;
			}
			this.byte(Number(n));
		}
		int=(value: N): V=> {
			if (!Number.isInteger(value) || value < -2147483648 || value > 2147483647)
				throw new Error("Avro: int out of range");
			this.long(value);
		}
		float=(value: N): V=> {
			this.ensure(4);
			new DataView(this.buf.buffer).setFloat32(this.pos, value, true);
			this.pos += 4;
		}
		double=(value: N): V =>{
			this.ensure(8);
			new DataView(this.buf.buffer).setFloat64(this.pos, value, true);
			this.pos += 8;
		}
		finish = (): Uint8Array =>{
			return this.buf.slice(0, this.pos);
		}
}
class AVROReader {
		private pos = 0;
		private readonly view: DataView;

		constructor(private readonly buf: Uint8Array) {
			this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
		}

		private ensure(length: N): V {
			if (length < 0 || this.pos + length > this.buf.length)
				throw new Error("Avro: truncated buffer");
		}

		byte(): N {
			this.ensure(1);
			return this.buf[this.pos++];
		}

		bytes(length: N): Uint8Array {
			this.ensure(length);
			const out = this.buf.slice(this.pos, this.pos + length);
			this.pos += length;
			return out;
		}

		long(): number | bigint {
			let n = 0n;
			let shift = 0n;

			for (let i = 0; i < 10; i++) {
				const b = this.byte();
				n |= BigInt(b & 0x7f) << shift;
				if ((b & 0x80) === 0) {
					const value = (n & 1n) === 0n ? n >> 1n : -((n >> 1n) + 1n);
					return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
						? Number(value)
						: value;
				}
				shift += 7n;
			}

			throw new Error("Avro: invalid long");
		}

		int(): N {
			const n = this.long();
			if (typeof n === "bigint" || n < -2147483648 || n > 2147483647)
				throw new Error("Avro: invalid int");
			return n;
		}

		float(): N {
			this.ensure(4);
			const n = this.view.getFloat32(this.pos, true);
			this.pos += 4;
			return n;
		}

		double(): N {
			this.ensure(8);
			const n = this.view.getFloat64(this.pos, true);
			this.pos += 8;
			return n;
		}

		done(): B { return this.pos === this.buf.length; }
}
const AVRO_ERROR_SCHEMA: AVROSchema = {
		type: "record",
		name: "AVROError",
		fields: [
			{ name: "type", type: "string" },
			{ name: "error", type: "string" },
			{ name: "id", type: "string" },
		],
};
class BINARY_AVRO {
		private readonly encoder = new TextEncoder();
		private readonly decoder = new TextDecoder();
		private readonly schema: AVROSchema;
		private readonly names: OB<AVROSchema> = Object.create(null);
		private readonly errorSchema = AVRO_ERROR_SCHEMA;
		private readonly wireSchema: AVROSchema[];
		constructor(schema: AVROSchema) {
			if (Array.isArray(schema)) throw new Error("Avro: root schema cannot be a union");
			this.schema = schema;
			this.wireSchema = [schema, this.errorSchema];
			this.#collect(schema);
			this.#collect(this.errorSchema);
		}
		 #collect(schema: AVROSchema): V {
			if (Array.isArray(schema)) {
				for (const branch of schema) this.#collect(branch);
				return;
			}
			if (typeof schema === "string") return;

			if (schema.name) this.names[schema.name] = schema;

			if (Array.isArray(schema.type)) this.#collect(schema.type);
			else if (typeof schema.type === "object") this.#collect(schema.type);
			else if (typeof schema.type === "string" && ![
				"null", "boolean", "int", "long", "float", "double",
				"bytes", "string", "record", "array", "map", "enum", "fixed",
			].includes(schema.type)) {
				if (!this.names[schema.type]) throw new Error(`Avro: unknown named type ${schema.type}`);
			}

			if (schema.fields) for (const field of schema.fields) this.#collect(field.type);
			if (schema.items) this.#collect(schema.items);
			if (schema.values) this.#collect(schema.values);
		}
		 #resolve(schema: AVROSchema): AVROSchema {
			if (typeof schema === "string" && this.names[schema]) return this.names[schema];
			return schema;
		}
		 #kind(schema: AVROSchema): S {
			schema = this.#resolve(schema);
			if (typeof schema === "string") return schema;
			if (Array.isArray(schema)) return "union";
			return typeof schema.type === "string" ? schema.type : this.#kind(schema.type);
		}
		 #matches(schema: AVROSchema, value: AVROValue): B {
			schema = this.#resolve(schema);
			const kind = this.#kind(schema);

			switch (kind) {
				case "null": return value === null || value === undefined;
				case "boolean": return typeof value === "boolean";
				case "int": return typeof value === "number" && Number.isInteger(value) && value >= -2147483648 && value <= 2147483647;
				case "long": return typeof value === "bigint" || (typeof value === "number" && Number.isSafeInteger(value));
				case "float":
				case "double": return typeof value === "number";
				case "string": return typeof value === "string";
				case "bytes":
				case "fixed": return value instanceof Uint8Array && (kind !== "fixed" || value.length === (schema as any).size);
				case "array": return Array.isArray(value);
				case "map": return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array);
				case "record": return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array);
				case "enum": return typeof value === "string" && (schema as any).symbols.includes(value);
				default: return false;
			}
		}
		 #write(w: AVROWriter, schema: AVROSchema, value: AVROValue): V {
			schema = this.#resolve(schema);

			if (Array.isArray(schema)) {
				const index = schema.findIndex(branch => this.#matches(branch, value));
				if (index === -1) throw new Error("Avro: no matching union branch");
				w.long(index);
				this.#write(w, schema[index], value);
				return;
			}

			if (typeof schema === "string") {
				switch (schema) {
					case "null": return;
					case "boolean": w.byte(value ? 1 : 0); return;
					case "int": w.int(value); return;
					case "long": w.long(value); return;
					case "float": w.float(value); return;
					case "double": w.double(value); return;
					case "string": {
						const bytes = this.encoder.encode(value);
						w.long(bytes.length);
						w.bytes(bytes);
						return;
					}
					case "bytes": {
						w.long(value.length);
						w.bytes(value);
						return;
					}
					default: throw new Error(`Avro: unknown type ${schema}`);
				}
			}

			if (typeof schema.type !== "string") {
				this.#write(w, schema.type, value);
				return;
			}

			switch (schema.type) {
				case "null": return;
				case "boolean":
					w.byte(value ? 1 : 0);
					return;
				case "int":
					w.int(value);
					return;
				case "long":
					w.long(value);
					return;
				case "float":
					w.float(value);
					return;
				case "double":
					w.double(value);
					return;
				case "string": {
					const bytes = this.encoder.encode(value);
					w.long(bytes.length);
					w.bytes(bytes);
					return;
				}
				case "bytes":
					w.long(value.length);
					w.bytes(value);
					return;
				case "fixed":
					if (!(value instanceof Uint8Array) || value.length !== schema.size)
						throw new Error("Avro: invalid fixed value");
					w.bytes(value);
					return;
				case "enum": {
					const index = schema.symbols!.indexOf(value);
					if (index < 0) throw new Error(`Avro: invalid enum symbol ${value}`);
					w.long(index);
					return;
				}
				case "record":
					for (const field of schema.fields!) {
						if (value[field.name] === undefined && !Array.isArray(field.type))
							throw new Error(`Avro: missing field ${field.name}`);
						this.#write(w, field.type, value[field.name]);
					}
					return;
				case "array":
					if (value.length) {
						w.long(value.length);
						for (const item of value) this.#write(w, schema.items!, item);
					}
					w.long(0);
					return;
				case "map": {
					const keys = Object.keys(value);
					if (keys.length) {
						w.long(keys.length);
						for (const key of keys) {
							this.#write(w, "string", key);
							this.#write(w, schema.values!, value[key]);
						}
					}
					w.long(0);
					return;
				}
				default:
					throw new Error(`Avro: unsupported schema type ${schema.type}`);
			}
		}
		 #read(r: AVROReader, schema: AVROSchema): AVROValue {
			schema = this.#resolve(schema);

			if (Array.isArray(schema)) {
				const index = r.long();
				if (typeof index !== "number" || index < 0 || index >= schema.length)
					throw new Error("Avro: invalid union index");
				return this.#read(r, schema[index]);
			}

			if (typeof schema === "string") {
				switch (schema) {
					case "null": return null;
					case "boolean": {
						const b = r.byte();
						if (b > 1) throw new Error("Avro: invalid boolean");
						return b === 1;
					}
					case "int": return r.int();
					case "long": return r.long();
					case "float": return r.float();
					case "double": return r.double();
					case "string": return this.decoder.decode(r.bytes(Number(r.long())));
					case "bytes": return r.bytes(Number(r.long()));
					default: throw new Error(`Avro: unknown type ${schema}`);
				}
			}

			if (typeof schema.type !== "string") return this.#read(r, schema.type);

			switch (schema.type) {
				case "null": return null;
				case "boolean": {
					const b = r.byte();
					if (b > 1) throw new Error("Avro: invalid boolean");
					return b === 1;
				}
				case "int": return r.int();
				case "long": return r.long();
				case "float": return r.float();
				case "double": return r.double();
				case "string": return this.decoder.decode(r.bytes(Number(r.long())));
				case "bytes": return r.bytes(Number(r.long()));
				case "fixed": return r.bytes(schema.size!);
				case "enum": {
					const index = r.long();
					if (typeof index !== "number" || index < 0 || index >= schema.symbols!.length)
						throw new Error("Avro: invalid enum index");
					return schema.symbols![index];
				}
				case "record": {
					const out: OB<AVROValue> = {};
					for (const field of schema.fields!) out[field.name] = this.#read(r, field.type);
					return out;
				}
				case "array": {
					const out: AVROValue[] = [];
					let count: number | bigint;
					while ((count = r.long()) !== 0) {
						if (typeof count === "bigint") throw new Error("Avro: array block too large");
						if (count < 0) {
							count = -count;
							const blockSize = r.long();
							if (typeof blockSize !== "number" || blockSize < 0) throw new Error("Avro: invalid array block");
						}
						for (let i = 0; i < count; i++) out.push(this.#read(r, schema.items!));
					}
					return out;
				}
				case "map": {
					const out: OB<AVROValue> = {};
					let count: number | bigint;
					while ((count = r.long()) !== 0) {
						if (typeof count === "bigint") throw new Error("Avro: map block too large");
						if (count < 0) {
							count = -count;
							const blockSize = r.long();
							if (typeof blockSize !== "number" || blockSize < 0) throw new Error("Avro: invalid map block");
						}
						for (let i = 0; i < count; i++) {
							const key = this.#read(r, "string");
							out[key] = this.#read(r, schema.values!);
						}
					}
					return out;
				}
				default:
					throw new Error(`Avro: unsupported schema type ${schema.type}`);
			}
		}
		 #encode(value: AVROValue, branch: N): Uint8Array {
			const w = new AVROWriter();
			w.long(branch);
			this.#write(w, this.wireSchema[branch], value);
			return w.finish();
		}

		 #decode(bytes: Uint8Array): AVROValue {
			const r = new AVROReader(bytes);
			const branch = r.long();
			if (typeof branch !== "number" || branch < 0 || branch >= this.wireSchema.length)
				throw new Error("Avro: invalid envelope branch");
			const value = this.#read(r, this.wireSchema[branch]);
			if (!r.done()) throw new Error("Avro: trailing bytes");
			return value;
		}

		public encode_msg(msg: AVROValue): Uint8Array {
			if (msg instanceof Uint8Array) return msg;
			try {
				return this.#encode(msg, 0);
			} catch (err) {
				console.log("[AVRO_PIPE] Failed to encode outbound packet:", err);
				return this.#encode({
					type: "error",
					error: err instanceof Error ? err.message : "Encoding failed",
					id: String(msg?.id ?? "unknown"),
				}, 1);
			}
		}

		public decode_msg(raw: any): AVROValue {
			if (!(raw instanceof Uint8Array) && !(typeof Buffer !== "undefined" && Buffer.isBuffer(raw))) {
				console.error("[AVRO_PIPE] Protocol violation: Expected encoded Uint8Array data stream.");
				return null;
			}
			try {
				const bytes = raw instanceof Uint8Array
					? raw
					: new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
				return this.#decode(bytes);
			} catch (err) {
				console.error("[AVRO_PIPE] Failed to decode inbound packet:", err);
				return null;
			}
		}
}
const AVROI = new BINARY_AVRO({
		type: "record",
		name: "SITHMessage",
		fields: [
			{ name: "pid", type: "int" },
			{ name: "op", type: "int" },
			{ name: "value", type: ["null", "double"] },
			{ name: "payload", type: ["null", "bytes"] },
		],
}); 

(globalThis as A).AVRO = AVROI;
// #endregion
//                                                                                         [SPREADSHEET]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
/*  =====================================================================================================
	 POWER_OF_TWO
	 What it moves: SPREADSHEET
	 How it works: Pure parallel typed arrays (Float64Array, Uint32Array). Zero parsing, zero object 
	 allocations, zero decoding. Data drops straight into pre-allocated memory arenas where the CPU 
	 can rip through math and propagation loops via direct index lookups.
    ================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE HOT PATH ENGINE DISRUPTOR / ZERO COPY ARENA] */
class POWER_OF_TWO {// will take care of bus fucntionality
	/*  =============================================================================
     power_of_two_disruptor.ts:
 
     --- DisruptorSoA.editDirect ---
     Peak (Best Batch): 848.46 M ops/sec
     Average:           725.71 M ops/sec
 
     0 pass
     0 fail
     Ran 0 tests across 1 file. [641.00ms]
    =============================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [POWER_OF_TWO] */
    public hub: HUB
	public readonly capacity: number;
	public readonly mask: number;
	// ── Structure of Arrays (SoA) Event Lanes ──────────────────────────────
	public readonly pids: Int32Array;
	public readonly values: Float64Array;
	public readonly flags: Uint8Array;
	// ── Cursors ────────────────────────────────────────────────────────────
	public writeCursor = 0;
	public readCursor = 0;
	constructor(hub: HUB, capacity = 65536) {// Enforce power-of-two for bitwise masking
		if ((capacity & (capacity - 1)) !== 0) { throw new Error("Disruptor capacity must be a power of two."); }
        this.hub = hub
		this.capacity = capacity;
		this.mask = capacity - 1;
		this.pids = new Int32Array(capacity); // Pre-allocate contiguous typed memory blocks (Zero GC churn)
		this.values = new Float64Array(capacity);
		this.flags = new Uint8Array(capacity);
	} // Publishes an event directly into the next ring slot. O(1) time complexity, zero heap allocation, single CPU instruction cycle.
	public publish(pid: number, value: number, flag = 0): boolean {
		// Optional backpressure check: Prevent writer from overlapping reader
		if (this.writeCursor - this.readCursor >= this.capacity) {
			return false;
		} // Ring buffer full
		const index = this.writeCursor & this.mask;
		this.pids[index] = pid; // Write directly to parallel SoA typed array lanes
		this.values[index] = value;
		this.flags[index] = flag;
		this.writeCursor++;
		return true;
	} // Drains and processes the ring buffer in batches using a zero-allocation loop.
	public consume(handler: (pid: number, value: number, flag: number) => void): number {
		const targetWrite = this.writeCursor;
		let currentRead = this.readCursor;
		if (currentRead === targetWrite) {
			return 0;
		} // Nothing to consume
		let processed = 0;
		while (currentRead < targetWrite) {
			const index = currentRead & this.mask; // Execute handler on raw lane data
			handler(this.pids[index], this.values[index], this.flags[index]);
			currentRead++;
			processed++;
		}
		this.readCursor = currentRead;
		return processed;
	} // Resets cursors and ring lanes instantly without reallocating memory.
	public reset(): void {
		this.writeCursor = 0;
		this.readCursor = 0;
		this.pids.fill(0);
		this.values.fill(0);
		this.flags.fill(0);
	}
}
// =====================================================================================================
// CODECS
// =====================================================================================================
class SOA_CODEC {
	// HIGH_PERFORMANCE_SOA_CODEC
	public readonly maxPids: number;
    public hub: HUB
        
	// SoA Memory Layout (Typed Arrays for Cache Locality)
	public readonly channelMap: Int32Array;
	public readonly subChannelMap: Int32Array;
	public readonly tripChannelMap: Int32Array;
	public readonly keyMap: Int32Array;

	public readonly listeners: Array<((pid: number, val?: number) => void) | undefined>;
	public nextPid = 1;

	constructor(hub: HUB, maxPids = 8192) {
		this.maxPids = maxPids;
        this.hub = hub
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
		this.keyMap = new Int32Array(maxPids);
		this.listeners = new Array(maxPids);
	}// Hot path lookup or allocation: O(1) inline integer assignment
	public allocatePid(rId: number, mId: number, sId: number, kId: number): number {
		// In a true LMAX disruptor ring or fixed socket registry,
		// PIDs can be pre-mapped or statically routed at startup.
		const pid = this.nextPid++;
		if (pid >= this.maxPids) throw new Error("PID capacity exceeded");

		this.channelMap[pid] = rId;
		this.subChannelMap[pid] = mId;
		this.tripChannelMap[pid] = sId;
		this.keyMap[pid] = kId;

		return pid;
	}
	/**
     * Encodes a columnar batch (e.g., a spreadsheet column or database table chunk)
     * directly into a parallel SoA byte slab.
     */
    public encodeBatch(ids: Int32Array, values: Float64Array): Uint8Array {
        const headerLength = 8; // 4 bytes for count, 4 bytes for flags
        const idBytes = ids.byteLength;
        const valBytes = values.byteLength;
        
        const totalSize = headerLength + idBytes + valBytes;
        const slab = new Uint8Array(totalSize);
        const view = new DataView(slab.buffer);

        // Write header metadata
        view.setInt32(0, ids.length, true);
        view.setInt32(4, 0, true); // reserved flags

        // Copy raw typed array memory directly into the slab (Zero allocation overhead)
        slab.set(new Uint8Array(ids.buffer, ids.byteOffset, idBytes), headerLength);
        slab.set(new Uint8Array(values.buffer, values.byteOffset, valBytes), headerLength + idBytes);

        return slab;
    }
    /**
     * Decodes / Views the binary slab directly as SoA arenas 
     * without creating individual JavaScript objects.
     */
    public decodeBatchView(slab: Uint8Array): { ids: Int32Array, values: Float64Array } {
        const view = new DataView(slab.buffer, slab.byteOffset, slab.byteLength);
        const count = view.getInt32(0, true);
        const headerLength = 8;

        const idByteLen = count * 4; // Int32 = 4 bytes
        const valByteLen = count * 8; // Float64 = 8 bytes

        const ids = new Int32Array(slab.buffer, slab.byteOffset + headerLength, count);
        const values = new Float64Array(slab.buffer, slab.byteOffset + headerLength + idByteLen, count);

        return { ids, values };
    }
}
// #endregion
//                                                                                     TYPES & CONSTANTS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
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
	DRC: 12,
} as const;
const busOP = {
	SEND: 12,
	SUB: 13,
	DUPLEX: 14,
	STREAM: 15,
} as const;
const spreadsheetOP = {
	READ: 0,
	WRITE: 1,
	DELETE: 2,
	EFFECT: 3,
	NONE: 4,
    EDIT: 5,
	COMPUTED:6,
} as const;
interface SithMetadataEntry {
    _batches: Set<S | SY>;
    _batchQueue: M<S | SY, A>;
    _batchOpts: M<S | SY, { triggerRender?: B }>;
    _globalListeners: Set<(key: S | SY, value: A) => V>;
    _global_subscribers: M<S, Set<() => V>>;
    _waiters: M<S | SY, Set<(value: A) => V>>;
    _sys: M<S, A>;
    _sealed: Set<S>;
    _frozen: Set<S>;
    _keyContracts: M<S, Set<S>>;
    _conflicts: M<S, A>;
    _conflictStrategies: Map<S, "latest" | "fail" | "accumulate">;
    _causal: M<S, A[]>;
    _history: M<S | SY, A[]>;
    _scopes: M<S, A>;
    _msgHandlers: M<S, Set<(p: A) => V>>;
    _lastWriteSource: M<S, S>;
    _mainMap: M<A, S>;
    _tupleCache: M<S, S>;
    _weakMap: WeakMap<O, S>;
    _boundaries: WeakMap<O, S>;
}
const REC = 8;
const F_OP = 0;
const F_PID = 1;
const F_SLOT = 2;
const F_A = 3;
const F_B = 4;
const F_C = 5;
const F_EXT = 7;
const DEFAULT_SCHEDULE = [OP.READ, OP.WRITE, OP.READ, OP.WRITE, OP.DELETE, OP.EFFECT, OP.SEND, OP.SUB, OP.DUPLEX, OP.STREAM, OP.DRC, OP.COMPACT] as const;
const pow2 = (n: N) => n > 0 && (n & (n - 1)) === 0;
const PAYLOAD = {
	SCALAR: 0,
	BINARY: 1,
} as const;
type HC_HANDLER = (p: U) => U | P<U>;
type PID = N;
type SLOT = N;
type OpKind = (typeof OP)[keyof typeof OP];
type WaitStrategy = "spin" | "yield" | "park";
interface HADRON_OPTIONS {
	capacity?: N;
	pidCapacity?: N;
	f64Capacity?: N;
	u32Capacity?: N;
	i32Capacity?: N;
	byteCapacity?: N;
	maxBatch?: N;
	wait?: WaitStrategy;
	shared?: B;
	schedule?: readonly N[];
}
interface HADRON_COLLIDER_OPTIONS {
	ringCapacity?: N;
	slotCapacity?: N;
	pidCapacity?: N;
	maxBatch?: N;
	soaCapacity?: N;
}
type DisruptorHandler = (pid: N, value: N, flag: N) => V;
const MAX_PID = 0xffffff;
interface LANE_STATS {
	published: N;
	consumed: N;
	batches: N;
	maxBatch: N;
	fullStalls: N;
	claimed: N;
}
interface ScopeComponents {
	r: N;
	m: N;
	s: N;
}
const FLYWEIGHT_SCOPE: ScopeComponents = { r: 0, m: 0, s: 0 };
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
// #endregion
//                                                                    [DARTH VADER] - STATE DRS AND MORE
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
/*  =====================================================================================================
	 DARTH_VADER
	 What it moves: HOT PATH IE, STATE, SIGNALS, DRS
	 Selected from the benchmark suite as the absolute lightning-fast SPSC ring achieving up to 1.23B - 
	 2.01B ops/sec. Uses packed bitwise metadata (pid << 8 | flag) and zero-wrapping free-slot counters 
	 to maintain extreme hot-path throughput. INSTEAD OF DOING 12 ROTATIONS ON THE SAME DISRUPTOR... OR 
	 12 PASSES, WE ARE DOING ONE PASS... 12 WIDE
    ================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [THE HOT PATH ENGINE DISRUPTOR / ZERO COPY ARENA] */
const queues: Record<Priority, Map<symbol | string, ScheduledTask[]>> = {
    immediate: new Map(), 'user-blocking': new Map(), normal: new Map(), background: new Map(), idle: new Map()
}
let _localChange = false
let _channel: BroadcastChannel | null = null
const TRACER_PID = 0;
// ============================================================================
// HADRON_COLLIDER — self-contained reactive core
// ============================================================================


const SLAB_THRESHOLD = 8192;

const DEFAULT_SOA_CAPACITY = 1 << 16;
const DEFAULT_SLAB_CAPACITY = 1 << 20;
const DEFAULT_MAX_PIDS = 1 << 20;

type PID = number;
type SLOT = number;

interface HADRON_COLLIDER_OPTIONS {
    soaCapacity?: number;
    slabCapacity?: number;
    maxPids?: number;
    initialByteCapacity?: number;
}

interface SithMetadataEntry {
    _globalListeners: Set<(key: string | symbol, value: any) => void>;
    _waiters: Map<string | symbol, Set<(value: any) => void>>;
    _history: Map<string | symbol, any[]>;
    _scopes: Map<number, Map<any, any>>;
    _listeners: Map<number, Map<any, Set<(value: any) => void>>>;
    _persistent: Map<number, Map<any, any>>;
    _persistKeys: Map<number, Set<any>>;
}

interface ScopeOptions {
    sl_reg: string;
    ms_reg: string;
    globalKey: string;
}

type ReactiveFlags = number;

interface ReactiveNode {
    deps?: Link;
    depsTail?: Link;
    subs?: Link;
    subsTail?: Link;
    flags: ReactiveFlags;
}

interface Link {
    version: number;
    dep: ReactiveNode;
    sub: ReactiveNode;
    prevSub?: Link;
    nextSub?: Link;
    prevDep?: Link;
    nextDep?: Link;
}

interface EffectNode extends ReactiveNode {
    fn: () => void | (() => void);
    cleanup: void | (() => void);
}

interface ComputedNode<T = any> extends ReactiveNode {
    value: T | undefined;
    getter: (previous?: T) => T;
    dirty: boolean;
}

interface SignalNode<T = any> extends ReactiveNode {
    currentValue: T;
    pendingValue: T;
}

const ReactiveFlags = {
    NONE: 0,
    SIGNAL: 1 << 0,
    COMPUTED: 1 << 1,
    EFFECT: 1 << 2,
    DIRTY: 1 << 3,
    RUNNING: 1 << 4,
    DISPOSED: 1 << 5,
} as const;

function pow2(value: number): boolean {
    return value > 0 && (value & (value - 1)) === 0;
}

export class HADRON_COLLIDER {
    // ── identity ──────────────────────────────────────────────────────────
    public readonly instanceId = `hadron_${Date.now()}_${Math.random().toString(36).slice(2)}`;

    // ── SOA particle storage ──────────────────────────────────────────────
    public readonly soaCapacity: number;
    public readonly maxPids: number;

    public readonly ops: Uint8Array;
    public readonly storage: Uint8Array;
    public readonly expiresAt: Float64Array;

    public readonly values: Float64Array;
    public readonly versions: Uint32Array;

    public readonly refCount: Int32Array;
    public readonly nextFree: Int32Array;

    public readonly slabOffsets: Int32Array;
    public readonly slabLengths: Int32Array;

    private readonly lane: Int32Array;
    private readonly freePids: Int32Array;

    private laneLen = 1;
    private freePidHead = 0;
    private freePidTail = 0;

    private allocatedSlots = 1;
    private freeSlotHead = -1;

    private slabTail = 0;
    private slabBytes: Uint8Array;

    // ── binary codec state ────────────────────────────────────────────────
    private codecBytes: Uint8Array;
    private codecView: DataView;
    private codecTail = 0;

    private readonly encoder = new TextEncoder();
    private readonly decoder = new TextDecoder();

    // ── reactive graph ────────────────────────────────────────────────────
    private activeSub: ReactiveNode | undefined;
    private activeEffectStack: EffectNode[] = [];

    private effects: EffectNode[] = [];
    private computeds: ComputedNode[] = [];
    private signals: SignalNode[] = [];

    // ── scheduler / flush ─────────────────────────────────────────────────
    private dirty = new Set<() => void>();
    private flushScheduled = false;
    private batchDepth = 0;
    private openScopes = new Set<symbol>();

    private taskQueues = new Map<string, Map<string, Array<() => void>>>();
    private taskScheduled = false;

    // ── Sauron registry ───────────────────────────────────────────────────
    private stringToId = new Map<string, number>();
    private idToString: string[] = [];
    private addressToPid = new Map<string, PID>();

    private metadataRegistry: SithMetadataEntry[] = [];
    private nextPid: PID = 1;

    public readonly multiFlyweight = { r: "", m: "", s: "", k: "" };

    public readonly GLOBAL_SCOPE = Symbol("hadron-global-scope");
    private currentScope: string | symbol = this.GLOBAL_SCOPE;

    constructor(options: HADRON_COLLIDER_OPTIONS = {}) {
        const soaCapacity = options.soaCapacity ?? DEFAULT_SOA_CAPACITY;
        const slabCapacity = options.slabCapacity ?? DEFAULT_SLAB_CAPACITY;
        const maxPids = options.maxPids ?? DEFAULT_MAX_PIDS;
        const initialByteCapacity = options.initialByteCapacity ?? 1 << 16;

        if (!pow2(soaCapacity)) throw new Error("HADRON_COLLIDER: soaCapacity must be a power of two");
        if (!pow2(slabCapacity)) throw new Error("HADRON_COLLIDER: slabCapacity must be a power of two");

        this.soaCapacity = soaCapacity;
        this.maxPids = maxPids;

        this.lane = new Int32Array(soaCapacity).fill(-1);
        this.ops = new Uint8Array(soaCapacity).fill(OP.NONE);
        this.storage = new Uint8Array(soaCapacity);
        this.expiresAt = new Float64Array(soaCapacity);

        this.values = new Float64Array(soaCapacity);
        this.versions = new Uint32Array(soaCapacity);
        this.refCount = new Int32Array(soaCapacity);
        this.nextFree = new Int32Array(soaCapacity).fill(-1);

        this.slabOffsets = new Int32Array(soaCapacity);
        this.slabLengths = new Int32Array(soaCapacity);

        this.freePids = new Int32Array(soaCapacity);

        for (let pid = 1; pid < soaCapacity; pid++) {
            this.freePids[this.freePidTail++] = pid;
        }

        this.lane[0] = TRACER_PID;
        this.refCount[0] = 1;

        this.slabBytes = new Uint8Array(slabCapacity);
        this.codecBytes = new Uint8Array(initialByteCapacity);
        this.codecView = new DataView(this.codecBytes.buffer);
    }

    // =========================================================================
    // PID + SLOT ALLOCATION
    // =========================================================================

    public allocPid(): PID {
        if (this.freePidHead === this.freePidTail) {
            throw new Error("HADRON_COLLIDER: out of PIDs");
        }

        const pid = this.freePids[this.freePidHead];
        this.freePidHead = (this.freePidHead + 1) % this.soaCapacity;

        this.ops[pid] = OP.NONE;
        this.storage[pid] = 0;
        this.expiresAt[pid] = 0;
        this.values[pid] = 0;
        this.versions[pid] = 0;
        this.slabOffsets[pid] = 0;
        this.slabLengths[pid] = 0;
        this.refCount[pid] = 1;

        return pid;
    }

    public reclaimPid(pid: PID): void {
        if (pid === TRACER_PID) return;

        const next = (this.freePidTail + 1) % this.soaCapacity;

        if (next === this.freePidHead) {
            throw new Error("HADRON_COLLIDER: PID free list full");
        }

        this.ops[pid] = OP.NONE;
        this.storage[pid] = 0;
        this.expiresAt[pid] = 0;
        this.values[pid] = 0;
        this.slabOffsets[pid] = 0;
        this.slabLengths[pid] = 0;
        this.refCount[pid] = 0;

        this.freePids[this.freePidTail] = pid;
        this.freePidTail = next;
    }

    private allocSlot(value = 0): SLOT {
        let slot = this.freeSlotHead;

        if (slot >= 0) {
            this.freeSlotHead = this.nextFree[slot];
            this.nextFree[slot] = -1;
        } else {
            if (this.allocatedSlots >= this.soaCapacity) {
                throw new Error("HADRON_COLLIDER: out of slots");
            }

            slot = this.allocatedSlots++;
        }

        this.values[slot] = value;
        this.versions[slot] = 0;
        this.refCount[slot] = 1;
        this.slabOffsets[slot] = 0;
        this.slabLengths[slot] = 0;

        return slot;
    }

    private releaseSlot(slot: SLOT): void {
        if (slot <= 0) return;

        const count = --this.refCount[slot];

        if (count > 0) return;

        this.values[slot] = 0;
        this.versions[slot] = 0;
        this.slabOffsets[slot] = 0;
        this.slabLengths[slot] = 0;

        this.nextFree[slot] = this.freeSlotHead;
        this.freeSlotHead = slot;
    }

    // =========================================================================
    // CORE SIGNAL API
    // =========================================================================

    public signal<T>(initial: T): SignalNode<T> {
        const pid = this.allocPid();

        const node: SignalNode<T> = {
            flags: ReactiveFlags.SIGNAL,
            currentValue: initial,
            pendingValue: initial,
        };

        this.signals[pid] = node;
        this.values[pid] = typeof initial === "number" ? initial : 0;

        return node;
    }

    public readSignal<T>(node: SignalNode<T>): T {
        if (this.activeSub) {
            this.link(node, this.activeSub);
        }

        return node.currentValue;
    }

    public writeSignal<T>(node: SignalNode<T>, value: T | ((previous: T) => T)): T {
        const previous = node.currentValue;
        const next = typeof value === "function" ? (value as (previous: T) => T)(previous) : value;

        if (Object.is(previous, next)) return next;

        node.pendingValue = next;
        node.flags |= ReactiveFlags.DIRTY;

        this.scheduleFlush(() => {
            node.currentValue = node.pendingValue;
            node.flags &= ~ReactiveFlags.DIRTY;

            if (typeof node.currentValue === "number") {
                this.writeNumber(node as unknown as PID, node.currentValue);
            }

            this.notifySubscribers(node);
        });

        return next;
    }

    public computed<T>(getter: (previous?: T) => T): ComputedNode<T> {
        const node: ComputedNode<T> = {
            flags: ReactiveFlags.COMPUTED | ReactiveFlags.DIRTY,
            value: undefined,
            getter,
            dirty: true,
        };

        this.computeds.push(node);

        return node;
    }

    public readComputed<T>(node: ComputedNode<T>): T {
        if (this.activeSub) {
            this.link(node, this.activeSub);
        }

        if (node.dirty) {
            this.evaluateComputed(node);
        }

        return node.value as T;
    }

    private evaluateComputed<T>(node: ComputedNode<T>): void {
        if (node.flags & ReactiveFlags.RUNNING) {
            throw new Error("HADRON_COLLIDER: circular computed dependency");
        }

        const previousSub = this.activeSub;

        node.flags |= ReactiveFlags.RUNNING;
        this.activeSub = node;

        try {
            const next = node.getter(node.value);

            if (!Object.is(node.value, next)) {
                node.value = next;
                this.notifySubscribers(node);
            }

            node.dirty = false;
        } finally {
            this.activeSub = previousSub;
            node.flags &= ~ReactiveFlags.RUNNING;
        }
    }

    public effect(fn: () => void | (() => void)): () => void {
        const node: EffectNode = {
            flags: ReactiveFlags.EFFECT,
            fn,
            cleanup: undefined,
        };

        this.effects.push(node);
        this.runEffect(node);

        return () => this.disposeEffect(node);
    }

    private runEffect(node: EffectNode): void {
        if (node.flags & ReactiveFlags.DISPOSED) return;

        if (typeof node.cleanup === "function") {
            node.cleanup();
            node.cleanup = undefined;
        }

        const previousSub = this.activeSub;

        this.activeSub = node;
        this.activeEffectStack.push(node);

        try {
            node.cleanup = node.fn() ?? undefined;
        } finally {
            this.activeEffectStack.pop();
            this.activeSub = previousSub;
        }
    }

    private disposeEffect(node: EffectNode): void {
        node.flags |= ReactiveFlags.DISPOSED;

        if (typeof node.cleanup === "function") {
            node.cleanup();
            node.cleanup = undefined;
        }

        this.unlinkNode(node);
    }

    // =========================================================================
    // REACTIVE GRAPH
    // =========================================================================

    private link(dep: ReactiveNode, sub: ReactiveNode): void {
        let link = dep.subs;

        while (link) {
            if (link.sub === sub) return;
            link = link.nextSub;
        }

        const newLink: Link = {
            version: 0,
            dep,
            sub,
            prevSub: dep.subsTail,
            nextSub: undefined,
            prevDep: sub.depsTail,
            nextDep: undefined,
        };

        if (dep.subsTail) {
            dep.subsTail.nextSub = newLink;
        } else {
            dep.subs = newLink;
        }

        dep.subsTail = newLink;

        if (sub.depsTail) {
            sub.depsTail.nextDep = newLink;
        } else {
            sub.deps = newLink;
        }

        sub.depsTail = newLink;
    }

    private unlinkNode(node: ReactiveNode): void {
        let link = node.deps;

        while (link) {
            const next = link.nextDep;

            if (link.prevSub) link.prevSub.nextSub = link.nextSub;
            else link.dep.subs = link.nextSub;

            if (link.nextSub) link.nextSub.prevSub = link.prevSub;
            else link.dep.subsTail = link.prevSub;

            link = next;
        }

        node.deps = undefined;
        node.depsTail = undefined;
    }

    private notifySubscribers(node: ReactiveNode): void {
        let link = node.subs;

        while (link) {
            const sub = link.sub;

            if (sub.flags & ReactiveFlags.COMPUTED) {
                const computed = sub as ComputedNode;
                computed.dirty = true;

                this.scheduleFlush(() => {
                    this.readComputed(computed);
                });
            }

            if (sub.flags & ReactiveFlags.EFFECT) {
                const effect = sub as EffectNode;

                this.scheduleFlush(() => {
                    this.runEffect(effect);
                });
            }

            link = link.nextSub;
        }
    }

    // =========================================================================
    // NUMERIC / EXPIRY STORAGE
    // =========================================================================

    public writeNumber(pid: PID, value: number): void {
        if (pid < 0 || pid >= this.soaCapacity) {
            throw new Error("HADRON_COLLIDER: invalid PID");
        }

        this.values[pid] = value;
        this.versions[pid]++;
        this.ops[pid] = OP.WRITE;
        this.storage[pid] = 0;

        this.pushLane(pid);
    }

    public readNumber(pid: PID): number {
        return this.values[pid];
    }

    public setExpiry(pid: PID, expiresAt: number): void {
        this.expiresAt[pid] = expiresAt;
    }

    public sweepExpired(now: number): number {
        let released = 0;

        for (let pid = 1; pid < this.soaCapacity; pid++) {
            const expiresAt = this.expiresAt[pid];

            if (expiresAt !== 0 && now > expiresAt) {
                this.expiresAt[pid] = 0;
                this.deleteParticle(pid);
                released++;
            }
        }

        return released;
    }

    public deleteParticle(pid: PID): void {
        if (pid === TRACER_PID) return;

        this.ops[pid] = OP.DELETE;
        this.pushLane(pid);
    }

    private pushLane(pid: PID): void {
        for (let i = 0; i < this.laneLen; i++) {
            if (this.lane[i] === pid) return;
        }

        if (this.laneLen >= this.soaCapacity) {
            throw new Error("HADRON_COLLIDER: SOA lane full");
        }

        this.lane[this.laneLen++] = pid;
    }

    public pass(): number {
        const length = this.laneLen;
        let writeIndex = 0;
        let processed = 0;

        for (let i = 0; i < length; i++) {
            const pid = this.lane[i];

            if (pid === TRACER_PID) {
                this.lane[writeIndex++] = pid;
                continue;
            }

            const op = this.ops[pid];

            if (op === OP.DELETE) {
                this.releaseSlot(pid);
                this.reclaimPid(pid);
            } else {
                this.ops[pid] = OP.NONE;
                this.lane[writeIndex++] = pid;
            }

            processed++;
        }

        this.laneLen = writeIndex;

        return processed;
    }

    public get length(): number {
        return Math.max(0, this.laneLen - 1);
    }

    public getLoadFactor(): number {
        return this.length / Math.max(1, this.soaCapacity - 1);
    }

    // =========================================================================
    // BINARY SLAB — ONLY FOR PAYLOADS OVER 8192 BYTES
    // =========================================================================

    public allocBinary(binary: Uint8Array): SLOT {
        if (binary.byteLength <= SLAB_THRESHOLD) {
            throw new Error(
                `HADRON_COLLIDER: slab reserved for payloads above ${SLAB_THRESHOLD} bytes`
            );
        }

        const slot = this.allocSlot(0);
        const ref = this.allocBytes(binary);

        this.slabOffsets[slot] = ref.offset;
        this.slabLengths[slot] = ref.length;

        return slot;
    }

    public writeBinary(slot: SLOT, binary: Uint8Array): void {
        if (binary.byteLength <= SLAB_THRESHOLD) {
            throw new Error(
                `HADRON_COLLIDER: slab reserved for payloads above ${SLAB_THRESHOLD} bytes`
            );
        }

        const ref = this.allocBytes(binary);

        this.slabOffsets[slot] = ref.offset;
        this.slabLengths[slot] = ref.length;
        this.versions[slot]++;
    }

    public readBinary(slot: SLOT): Uint8Array | null {
        const offset = this.slabOffsets[slot];
        const length = this.slabLengths[slot];

        if (length === 0) return null;

        return this.readBytes(offset, length);
    }

    public binaryRef(slot: SLOT): { offset: number; length: number } {
        return {
            offset: this.slabOffsets[slot],
            length: this.slabLengths[slot],
        };
    }

    public allocBytes(data: Uint8Array): { offset: number; length: number } {
        if (this.slabTail + data.byteLength > this.slabBytes.length) {
            throw new Error("HADRON_COLLIDER: slab overflow");
        }

        const offset = this.slabTail;

        this.slabBytes.set(data, offset);
        this.slabTail += data.byteLength;

        return { offset, length: data.byteLength };
    }

    public readBytes(offset: number, length: number): Uint8Array {
        if (offset < 0 || length < 0 || offset + length > this.slabTail) {
            throw new Error("HADRON_COLLIDER: slab read out of bounds");
        }

        return this.slabBytes.subarray(offset, offset + length);
    }

    // =========================================================================
    // BINARY CODEC
    // =========================================================================

    public js_codec = (() => {
        const collider = this;

        const need = (count: number): void => {
            if (collider.codecTail + count <= collider.codecBytes.length) return;

            let size = collider.codecBytes.length;

            while (size < collider.codecTail + count) size *= 2;

            const next = new Uint8Array(size);
            next.set(collider.codecBytes);

            collider.codecBytes = next;
            collider.codecView = new DataView(next.buffer);
        };

        const u8 = (value: number): void => {
            need(1);
            collider.codecBytes[collider.codecTail++] = value;
        };

        const i32 = (value: number): void => {
            need(4);
            collider.codecView.setInt32(collider.codecTail, value, true);
            collider.codecTail += 4;
        };

        const f64 = (value: number): void => {
            need(8);
            collider.codecView.setFloat64(collider.codecTail, value, true);
            collider.codecTail += 8;
        };

        const writeString = (value: string): void => {
            const encoded = collider.encoder.encode(value);

            u8(TAG.STRING);
            i32(encoded.length);
            need(encoded.length);

            collider.codecBytes.set(encoded, collider.codecTail);
            collider.codecTail += encoded.length;
        };

        const writeBytes = (value: Uint8Array): void => {
            u8(TAG.UINT8ARRAY);
            i32(value.length);
            need(value.length);

            collider.codecBytes.set(value, collider.codecTail);
            collider.codecTail += value.length;
        };

        const seen = new Set<object>();

        const writeValue = (value: unknown): void => {
            if (value === undefined) return u8(TAG.UNDEFINED);
            if (value === null) return u8(TAG.NULL);
            if (value === false) return u8(TAG.FALSE);
            if (value === true) return u8(TAG.TRUE);

            const type = typeof value;

            if (type === "number") {
                const n = value as number;

                if (Number.isSafeInteger(n) && n >= -2147483648 && n <= 2147483647) {
                    u8(TAG.INT32);
                    return i32(n);
                }

                u8(TAG.FLOAT64);
                return f64(n);
            }

            if (type === "string") {
                return writeString(value as string);
            }

            if (value instanceof Uint8Array) {
                return writeBytes(value);
            }

            if (Array.isArray(value)) {
                if (seen.has(value)) {
                    throw new Error("HADRON_COLLIDER: circular reference");
                }

                seen.add(value);

                u8(TAG.ARRAY);
                i32(value.length);

                for (const item of value) writeValue(item);

                seen.delete(value);
                return;
            }

            if (type === "object") {
                const object = value as Record<string, unknown>;

                if (seen.has(object)) {
                    throw new Error("HADRON_COLLIDER: circular reference");
                }

                seen.add(object);

                const keys = Object.keys(object);

                u8(TAG.OBJECT);
                i32(keys.length);

                for (const key of keys) {
                    writeString(key);
                    writeValue(object[key]);
                }

                seen.delete(object);
                return;
            }

            throw new Error(`HADRON_COLLIDER: unsupported value type: ${type}`);
        };

        const readU8 = (cursor: { at: number }): number => {
            return collider.codecBytes[cursor.at++];
        };

        const readI32 = (cursor: { at: number }): number => {
            const value = collider.codecView.getInt32(cursor.at, true);
            cursor.at += 4;
            return value;
        };

        const readF64 = (cursor: { at: number }): number => {
            const value = collider.codecView.getFloat64(cursor.at, true);
            cursor.at += 8;
            return value;
        };

        const readString = (cursor: { at: number }): string => {
            const length = readI32(cursor);
            const bytes = collider.codecBytes.subarray(cursor.at, cursor.at + length);
            cursor.at += length;

            return collider.decoder.decode(bytes);
        };

        const readBytes = (cursor: { at: number }): Uint8Array => {
            const length = readI32(cursor);
            const bytes = collider.codecBytes.slice(cursor.at, cursor.at + length);
            cursor.at += length;

            return bytes;
        };

        const readValue = (cursor: { at: number }): unknown => {
            const tag = readU8(cursor);

            switch (tag) {
                case TAG.UNDEFINED: return undefined;
                case TAG.NULL: return null;
                case TAG.FALSE: return false;
                case TAG.TRUE: return true;
                case TAG.INT32: return readI32(cursor);
                case TAG.FLOAT64: return readF64(cursor);
                case TAG.STRING: return readString(cursor);
                case TAG.UINT8ARRAY: return readBytes(cursor);

                case TAG.ARRAY: {
                    const length = readI32(cursor);
                    const out: unknown[] = new Array(length);

                    for (let i = 0; i < length; i++) {
                        out[i] = readValue(cursor);
                    }

                    return out;
                }

                case TAG.OBJECT: {
                    const length = readI32(cursor);
                    const out: Record<string, unknown> = {};

                    for (let i = 0; i < length; i++) {
                        const key = readString(cursor);
                        out[key] = readValue(cursor);
                    }

                    return out;
                }

                default:
                    throw new Error(`HADRON_COLLIDER: unknown tag ${tag}`);
            }
        };

        return {
            encode: (value: unknown): Uint8Array => {
                collider.codecTail = 0;
                seen.clear();

                writeValue(value);

                return collider.codecBytes.subarray(0, collider.codecTail);
            },

            decode: (binary: Uint8Array): unknown => {
                const savedBytes = collider.codecBytes;
                const savedView = collider.codecView;
                const savedTail = collider.codecTail;

                try {
                    collider.codecBytes = binary;
                    collider.codecView = new DataView(
                        binary.buffer,
                        binary.byteOffset,
                        binary.byteLength
                    );

                    const cursor = { at: 0 };

                    return readValue(cursor);
                } finally {
                    collider.codecBytes = savedBytes;
                    collider.codecView = savedView;
                    collider.codecTail = savedTail;
                }
            },
        };
    })();

    // =========================================================================
    // SAURON STATE
    // =========================================================================

    private defaultScope: ScopeOptions = {
        sl_reg: "SAURON_SLAVE_REG",
        ms_reg: "SAURON_MASTER_REG",
        globalKey: "THE_WIZARD_ROOT_REG",
    };

    private intern(value: string): number {
        let id = this.stringToId.get(value);

        if (id === undefined) {
            id = this.idToString.length;
            this.stringToId.set(value, id);
            this.idToString[id] = value;
        }

        return id;
    }

    private getScopePid(
        sl_reg: string,
        ms_reg: string,
        globalKey: string
    ): PID {
        const address = `${sl_reg}:${ms_reg}:${globalKey}`;

        let pid = this.addressToPid.get(address);

        if (pid === undefined) {
            pid = this.nextPid++;

            if (pid >= this.maxPids) {
                throw new Error("HADRON_COLLIDER: max PIDs exceeded");
            }

            this.addressToPid.set(address, pid);
        }

        return pid;
    }

    public getMetadata(
        sl_reg: string,
        ms_reg: string,
        globalKey: string
    ): SithMetadataEntry {
        const pid = this.getScopePid(sl_reg, ms_reg, globalKey);

        let meta = this.metadataRegistry[pid];

        if (!meta) {
            meta = {
                _globalListeners: new Set(),
                _waiters: new Map(),
                _history: new Map(),
                _scopes: new Map(),
                _listeners: new Map(),
                _persistent: new Map(),
                _persistKeys: new Map(),
            };

            this.metadataRegistry[pid] = meta;
        }

        return meta;
    }

    private getScopeMap<T>(
        meta: SithMetadataEntry,
        pid: PID,
        create = true
    ): Map<any, T> {
        let map = meta._scopes.get(pid) as Map<any, T> | undefined;

        if (!map && create) {
            map = new Map();
            meta._scopes.set(pid, map);
        }

        return map as Map<any, T>;
    }

    private getListenerMap(
        meta: SithMetadataEntry,
        pid: PID
    ): Map<any, Set<(value: any) => void>> {
        let map = meta._listeners.get(pid);

        if (!map) {
            map = new Map();
            meta._listeners.set(pid, map);
        }

        return map;
    }

    public state = {
        get: <T = any>(
            key: string | symbol,
            opts: ScopeOptions = this.defaultScope
        ): T | undefined => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            const state = this.getScopeMap<T>(meta, pid);
            const persistent = this.getScopeMap<T>(meta, pid + 1_000_000);
            const persistKeys = this.getScopeMap<boolean>(meta, pid + 2_000_000);

            if (state.has(key)) return state.get(key);

            if (persistKeys.has(key) && persistent.has(key)) {
                const value = persistent.get(key);
                state.set(key, value);
                return value;
            }

            return undefined;
        },

        set: <T = any>(
            key: string | symbol,
            value: T | ((previous: T | undefined) => T),
            opts: ScopeOptions & { triggerRender?: boolean; history?: boolean } = this.defaultScope
        ): T => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            const state = this.getScopeMap<T>(meta, pid);
            const persistent = this.getScopeMap<T>(meta, pid + 1_000_000);
            const persistKeys = this.getScopeMap<boolean>(meta, pid + 2_000_000);
            const listeners = this.getListenerMap(meta, pid);

            const previous = state.get(key);
            const next = typeof value === "function"
                ? (value as (previous: T | undefined) => T)(previous)
                : value;

            if (Object.is(previous, next)) return next as T;

            if (opts.history !== false) {
                let history = meta._history.get(key);

                if (!history) {
                    history = [];
                    meta._history.set(key, history);
                }

                history.push({
                    previous,
                    next,
                    ts: Date.now(),
                });

                if (history.length > 200) history.shift();
            }

            state.set(key, next);

            if (persistKeys.has(key)) {
                persistent.set(key, next);
            }

            this.scheduleFlush(() => {
                const listenerSet = listeners.get(key);

                if (listenerSet) {
                    for (const listener of listenerSet) {
                        listener(next);
                    }
                }

                for (const globalListener of meta._globalListeners) {
                    globalListener(key, next);
                }

                const waiters = meta._waiters.get(key);

                if (waiters) {
                    for (const resolve of waiters) {
                        resolve(next);
                    }

                    meta._waiters.delete(key);
                }
            });

            return next as T;
        },

        delete: (
            key: string | symbol,
            opts: ScopeOptions = this.defaultScope
        ): boolean => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            const state = this.getScopeMap<any>(meta, pid);
            const persistent = this.getScopeMap<any>(meta, pid + 1_000_000);
            const persistKeys = this.getScopeMap<boolean>(meta, pid + 2_000_000);
            const listeners = this.getListenerMap(meta, pid);

            const existed = state.delete(key);

            if (persistKeys.has(key)) {
                persistKeys.delete(key);
                persistent.delete(key);
            }

            const listenerSet = listeners.get(key);

            if (listenerSet) {
                for (const listener of listenerSet) {
                    listener(undefined);
                }

                listeners.delete(key);
            }

            return existed;
        },

        has: (
            key: string | symbol,
            opts: ScopeOptions = this.defaultScope
        ): boolean => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            return (
                this.getScopeMap<any>(meta, pid).has(key) ||
                this.getScopeMap<any>(meta, pid + 1_000_000).has(key)
            );
        },

        entries: <T = any>(
            opts: ScopeOptions = this.defaultScope
        ): [string | symbol, T][] => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            return Array.from(this.getScopeMap<T>(meta, pid).entries());
        },

        subscribe: <T = any>(
            key: string | symbol,
            listener: (value: T) => void,
            opts: ScopeOptions = this.defaultScope
        ): () => void => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            const listeners = this.getListenerMap(meta, pid);

            let listenerSet = listeners.get(key);

            if (!listenerSet) {
                listenerSet = new Set();
                listeners.set(key, listenerSet);
            }

            listenerSet.add(listener);

            return () => {
                listenerSet!.delete(listener);

                if (listenerSet!.size === 0) {
                    listeners.delete(key);
                }
            };
        },

        subscribeAll: (
            listener: (key: string | symbol, value: any) => void,
            opts: ScopeOptions = this.defaultScope
        ): () => void => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);

            meta._globalListeners.add(listener);

            return () => {
                meta._globalListeners.delete(listener);
            };
        },

        waitFor: <T = any>(
            key: string | symbol,
            opts: ScopeOptions = this.defaultScope
        ): Promise<T> => {
            const existing = this.state.get<T>(key, opts);

            if (existing !== undefined) {
                return Promise.resolve(existing);
            }

            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);

            return new Promise<T>((resolve) => {
                let waiters = meta._waiters.get(key);

                if (!waiters) {
                    waiters = new Set();
                    meta._waiters.set(key, waiters);
                }

                waiters.add(resolve as (value: any) => void);
            });
        },

        clear: (opts: ScopeOptions = this.defaultScope): void => {
            const meta = this.getMetadata(opts.sl_reg, opts.ms_reg, opts.globalKey);
            const pid = this.getScopePid(opts.sl_reg, opts.ms_reg, opts.globalKey);

            this.getScopeMap<any>(meta, pid).clear();
            this.getListenerMap(meta, pid).clear();

            meta._waiters.clear();
        },
    };

    // =========================================================================
    // BATCHING + SCHEDULER
    // =========================================================================

    public onFlush(callback: () => void): void {
        this.dirty.add(callback);
        this.scheduleFlush();
    }

    public scheduleFlush(callback?: () => void): void {
        if (callback) {
            this.dirty.add(callback);
        }

        if (this.flushScheduled) return;

        this.flushScheduled = true;

        queueMicrotask(() => this.drainFlush());
    }

    private drainFlush(): void {
        this.flushScheduled = false;

        if (this.openScopes.size > 0) {
            this.scheduleFlush();
            return;
        }

        if (this.dirty.size === 0) return;

        const callbacks = Array.from(this.dirty);

        this.dirty.clear();

        for (const callback of callbacks) {
            callback();
        }
    }

    public batch<T>(fn: () => T): T {
        this.batchDepth++;

        try {
            return fn();
        } finally {
            this.batchDepth--;

            if (this.batchDepth === 0) {
                this.flushSync();
            }
        }
    }

    public openBatch(): symbol {
        const scope = Symbol("hadron-batch-scope");

        this.openScopes.add(scope);

        return scope;
    }

    public closeBatch(scope: symbol): void {
        this.openScopes.delete(scope);

        if (this.openScopes.size === 0) {
            this.flushSync();
        }
    }

    public flushSync(): void {
        if (this.openScopes.size > 0) return;

        if (this.dirty.size === 0) return;

        const callbacks = Array.from(this.dirty);

        this.dirty.clear();

        for (const callback of callbacks) {
            callback();
        }
    }

    public schedule(
        fn: () => void,
        options: { priority?: string; scope?: string } = {}
    ): { cancel: () => void } {
        const priority = options.priority ?? "normal";
        const scope = options.scope ?? String(this.currentScope);

        let tier = this.taskQueues.get(priority);

        if (!tier) {
            tier = new Map();
            this.taskQueues.set(priority, tier);
        }

        let queue = tier.get(scope);

        if (!queue) {
            queue = [];
            tier.set(scope, queue);
        }

        const task = { cancelled: false, fn };

        queue.push(task.fn);

        this.scheduleTaskLoop();

        return {
            cancel: () => {
                task.cancelled = true;
            },
        };
    }

    private scheduleTaskLoop(): void {
        if (this.taskScheduled) return;

        this.taskScheduled = true;

        queueMicrotask(() => this.drainTasks());
    }

    private drainTasks(): void {
        this.taskScheduled = false;

        const priorities = ["user-blocking", "normal", "background", "idle"];

        for (const priority of priorities) {
            const tier = this.taskQueues.get(priority);

            if (!tier || tier.size === 0) continue;

            for (const [scope, queue] of tier) {
                while (queue.length > 0) {
                    const task = queue.shift();
                    if (task) task();
                }

                if (queue.length === 0) {
                    tier.delete(scope);
                }
            }
        }

        for (const tier of this.taskQueues.values()) {
            if (tier.size > 0) {
                this.scheduleTaskLoop();
                break;
            }
        }
    }
}

export default HADRON_COLLIDER;
/**
 const registry = new UnifiedRegistry(8192);
 const disruptor = new DisruptorSoA(8192);
 
 // 1. Register a Single String
 const pidSingle = registry.resolveSingle("user:logged_in");
 
 // 2. Register a Multi-Channel address
 const pidMulti = registry.resolveMulti("THE_WIZARD", "SAURON_MASTER", "SLAVE_01", "STATE_MUTATION");
 
 // 3. Publish both raw PIDs into the Disruptor (Hot path - 0 allocations)
 disruptor.publish(pidSingle, 1, 0);
 disruptor.publish(pidMulti, 99.5, 1);
 
 // 4. Consume and decode depending on what kind of PID it is
 while (disruptor.poll((pid, value, flag) => {
	 // Check type lane or call decode based on your routing context
	 if (registry.typeMap[pid] === 0) {
		 const key = registry.decodeSingle(pid);
		 console.log(`[SINGLE] Key: "${key}" | Value: ${value}`);
	 } else {
		 const scope = registry.decodeMulti(pid);
		 console.log(`[MULTI] Channels: ${scope.r} > ${scope.m} > ${scope.s} > ${scope.k} | Value: ${value}`);
	 }
 })) {
	 // Drains ring buffer
 }
  */
// #endregion
//                                                                                                   HUB
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class HUB {
	public BEAM_CODEC: BEAM_CODEC;
	public BINARY: BINARY_CBOR;
	public SITH_DISRUPTOR_CODEC: SITH_DISRUPTOR_CODEC;
	public SOA_CODEC: SOA_CODEC;
	public SITH_SAURON: SITH_SAURON;
	public STORAGE_RING: STORAGE_RING;
    public r: S 
    public m: S
    public s: S 
	public maxParticles = 8192; // 1024 
	constructor(options: HADRON_COLLIDER_OPTIONS = {}) {
		const slotCapacity = options.slotCapacity ?? 1 << 20;
		const ringCapacity = options.ringCapacity ?? 1 << 18;
		const pidCapacity = options.pidCapacity ?? 1 << 20;
        this.r = Store.get<S>('THE_WIZARD_ROOT_REG') ?? 'THE_WIZARD_ROOT_REG'
        this.m = Store.get<S>('SAURON_MASTER_REG') ?? 'SAURON_MASTER_REG'
        this.s = Store.get<S>('SAURON_SLAVE_REG') ?? 'SAURON_SLAVE_REG'

		this.BEAM_CODEC = new BEAM_CODEC(this);
		this.BINARY = new BINARY_CBOR();
		this.SITH_DISRUPTOR_CODEC = new SITH_DISRUPTOR_CODEC(this, this.maxParticles, this.s, this.m, this.r);
		this.SITH_SAURON = new SITH_SAURON(this);
		this.SOA_CODEC = new SOA_CODEC(this);
		this.STORAGE_RING = new STORAGE_RING(this, slotCapacity);
	}


}
// #endregion
//                                                                                          STORAGE_RING
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
// WARNING!@@!@!@!!@!@ 
// THIS STORAGE RING IS ONLY... AND I FUCKING MEAN ONLY... FOR PAYLOADS ABOVE THE 8192 WEIGHT LIMIT... 
// FUCKING FAT ASSES OR WHEN SPECIFIED BY THE USER... EVERY SINGLE OTHER INSTANCE... EVERY VALUE... 
// IS PLACED ON THE DISRUPTOR... 
// HARD FUCKING REQUIREMENT, DO NOT PASS GO, DO NOT COLLECT 200 WHICH MEANS AINT NO FUCKING WAY WE ARE
// STORING ANYTHING HERE THAT DOES NOT MEET THOSE REQUIREMENTS...
class STORAGE_RING {
    public hub: HUB
	readonly capacity: number;
	readonly values: Float64Array;
	readonly versions: Uint32Array;
	readonly nextFree: Int32Array;
	readonly refCount: Int32Array;

	private freeHead = -1;
	private allocated = 0;

	readonly slab: Uint8Array;
	private slabTail = 0;

	private readonly slabOffsets: Int32Array;
	private readonly slabLengths: Int32Array;
	// Track which slots are currently in use (for debugging/validation)
	private readonly inUse: Uint8Array;
	constructor(hub: HUB, capacity: number, slabCapacity = 1 << 20) {
		if (!pow2(capacity)) throw new Error("slotCapacity must be a power of two");
        this.hub = hub
		this.capacity = capacity;

		this.values = new Float64Array(capacity);
		this.versions = new Uint32Array(capacity);
		this.nextFree = new Int32Array(capacity);
		this.refCount = new Int32Array(capacity);
		this.inUse = new Uint8Array(capacity);

		this.nextFree.fill(-1);

		// Slot 0 is reserved as the control slot (version counter)
		this.allocated = 1;
		this.refCount[0] = 1;
		this.versions[0] = 0;
		this.inUse[0] = 1;

		// Initialize slab-related arrays
		this.slabOffsets = new Int32Array(capacity).fill(-1);
		this.slabLengths = new Int32Array(capacity).fill(0);

		this.slab = new Uint8Array(slabCapacity);
		this.slabTail = 0;

		// Build initial free list (skip slot 0)
		// Link slots 1..capacity-1 in reverse order so we pop from the front
		for (let i = capacity - 1; i >= 1; i--) {
			this.nextFree[i] = this.freeHead;
			this.freeHead = i;
		}
	}
	writeBinary(slot: N, data: U8, epoch: N): V {
		this.assertLive(slot);
		this.assertEpoch(epoch);

		const oldOffset = this.slabOffsets[slot];
		const oldLength = this.slabLengths[slot];
		const length = data.length;

		// Reuse the existing allocation when it is large enough.
		if (oldOffset >= 0 && length <= oldLength) {
			this.slab.set(data, oldOffset);

			if (length < oldLength) {
				this.freeBytes(oldOffset + length, oldLength - length);
			}

			this.slabOffsets[slot] = length === 0 ? 0 : oldOffset;
			this.slabLengths[slot] = length;
			this.values[slot] = 0;
			this.versions[slot] = epoch;
			return;
		}

		// Allocate first: exhaustion leaves the old payload intact.
		const offset = this.allocBytes(length);

		this.slab.set(data, offset);

		if (oldOffset >= 0 && oldLength > 0) {
			this.freeBytes(oldOffset, oldLength);
		}

		this.slabOffsets[slot] = offset;
		this.slabLengths[slot] = length;
		this.values[slot] = 0;
		this.versions[slot] = epoch;
	}
	readBinary(slot: N): U8 {
		const { offset, length } = this.ref(slot);
		return this.slab.subarray(offset, offset + length);
	}
	private allocBytes(length: N): N {
		if (length === 0) return 0;

		// First-fit allocation from reclaimed ranges.
		for (let i = 0; i < this.freeRanges.length; i++) {
			const range = this.freeRanges[i];

			if (range.length < length) continue;

			const offset = range.offset;

			if (range.length === length) {
				this.freeRanges.splice(i, 1);
			} else {
				range.offset += length;
				range.length -= length;
			}

			return offset;
		}

		if (length > this.slab.length - this.slabTail) {
			throw new RangeError("STORAGE_RING: no contiguous slab range large enough");
		}

		const offset = this.slabTail;
		this.slabTail += length;
		return offset;
	}
	private freeBytes(offset: N, length: N): V {
		if (length === 0) return;

		const ranges = this.freeRanges;
		let index = 0;

		while (index < ranges.length && ranges[index].offset < offset) {
			index++;
		}

		ranges.splice(index, 0, { offset, length });

		// Merge with the preceding range.
		if (index > 0 && ranges[index - 1].offset + ranges[index - 1].length === offset) {
			ranges[index - 1].length += length;
			ranges.splice(index, 1);
			index--;
		}

		// Merge with the following range.
		if (index + 1 < ranges.length && ranges[index].offset + ranges[index].length === ranges[index + 1].offset) {
			ranges[index].length += ranges[index + 1].length;
			ranges.splice(index + 1, 1);
		}

		// Return a free suffix to the unused slab tail.
		const last = ranges[ranges.length - 1];

		if (last !== undefined && last.offset + last.length === this.slabTail) {
			this.slabTail = last.offset;
			ranges.pop();
		}
	}
	private assertLive(slot: N): V {
		if (!Number.isInteger(slot) || slot <= 0 || slot >= this.allocated || this.refCount[slot] <= 0) {
			throw new RangeError(`STORAGE_RING: invalid or released slot ${slot}`);
		}
	}
	private assertEpoch(epoch: N): V {
		if (!Number.isInteger(epoch) || epoch < 0 || epoch > 0xffffffff) {
			throw new RangeError("STORAGE_RING: epoch must be a uint32");
		}
	}
	tick(): number {
		return this.versions[0];
	}
	advance(): number {
		return ++this.versions[0];
	} // Allocate a new slot from the free list. Returns the slot index, or -1 if no slots are available.
	alloc(value = 0): number {
		const slot = this.freeHead;
		if (slot === -1) return -1; // No free slots

		// Remove from free list
		this.freeHead = this.nextFree[slot];
		this.nextFree[slot] = -1;

		// Initialize slot
		this.values[slot] = value;
		this.refCount[slot] = 1; // Initial reference
		this.inUse[slot] = 1;

		// Clear slab allocation for this slot
		this.slabOffsets[slot] = -1;
		this.slabLengths[slot] = 0;

		this.allocated++;

		return slot;
	} // Increment the reference count for a slot. Returns true if successful, false if the slot is not allocated.
	retain(slot: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;

		if (this.refCount[slot] === 0x7fffffff) {
			throw new Error("Reference count overflow");
		}

		this.refCount[slot]++;
		return true;
	} // Decrement the reference count for a slot. If the ref count reaches zero, the slot is freed. Returns true if the slot was freed, false otherwise.
	release(slot: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;

		if (this.refCount[slot] <= 0) {
			throw new Error("Reference count underflow");
		}

		this.refCount[slot]--;

		if (this.refCount[slot] === 0) {
			// Free the slot
			this.inUse[slot] = 0;

			// Clear slab data
			this.slabOffsets[slot] = -1;
			this.slabLengths[slot] = 0;
			this.values[slot] = 0;

			// Add to free list
			this.nextFree[slot] = this.freeHead;
			this.freeHead = slot;

			this.allocated--;

			return true;
		}

		return false;
	} // Write a numeric value into a slot with epoch validation. Returns true if successful, false if the slot is invalid or epoch mismatch.
	write(slot: number, value: number, epoch: number): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;
		if (this.refCount[slot] <= 0) return false;

		// Epoch validation: the slot's version must match the expected epoch
		if (this.versions[slot] !== epoch) return false;

		this.values[slot] = value;
		return true;
	} // Read the numeric value from a slot. Returns NaN if the slot is invalid.
	read(slot: number): number {
		if (slot < 0 || slot >= this.capacity) return NaN;
		if (!this.inUse[slot]) return NaN;

		return this.values[slot];
	} // Get the version number for a slot. Returns -1 if the slot is invalid.
	version(slot: number): number {
		if (slot < 0 || slot >= this.capacity) return -1;

		return this.versions[slot];
	} // Get the slab reference (offset and length) for a slot. Returns { offset: -1, length: 0 } if no slab data is associated.
	ref(slot: number): { offset: number; length: number } {
		if (slot < 0 || slot >= this.capacity) {
			return { offset: -1, length: 0 };
		}

		return {
			offset: this.slabOffsets[slot],
			length: this.slabLengths[slot],
		};
	} // Store variable-sized binary data in the slab for a given slot. Returns true if successful, false if insufficient slab space.
	storeSlab(slot: number, data: Uint8Array | ArrayBuffer): boolean {
		if (slot < 0 || slot >= this.capacity) return false;
		if (!this.inUse[slot]) return false;

		const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
		const len = bytes.length;

		if (len === 0) {
			// Zero-length: clear slab assignment
			this.slabOffsets[slot] = -1;
			this.slabLengths[slot] = 0;
			return true;
		}

		// Simple bump allocator: check if we have room
		if (this.slabTail + len > this.slab.length) {
			// Could implement compaction here, but for now just fail
			return false;
		}

		const offset = this.slabTail;
		this.slab.set(bytes, offset);
		this.slabTail += len;

		this.slabOffsets[slot] = offset;
		this.slabLengths[slot] = len;

		return true;
	} // Read the variable-sized binary data associated with a slot. Returns a subarray view (no copy) or null if no data.
	loadSlab(slot: number): Uint8Array | null {
		if (slot < 0 || slot >= this.capacity) return null;
		if (!this.inUse[slot]) return null;

		const offset = this.slabOffsets[slot];
		const length = this.slabLengths[slot];

		if (offset < 0 || length <= 0) return null;

		return this.slab.subarray(offset, offset + length);
	}
	private growthSlab(minAdditional: number): void {
		const newCapacity = Math.max(this.slab.length * 2, this.slabTail + minAdditional);
		const newSlab = new Uint8Array(newCapacity);
		newSlab.set(this.slab);
		//this.slab = newSlab;
	} // Check if the slab buffer needs compaction. Returns true if compaction would reclaim significant space.
	needsCompaction(): boolean {
		if (this.allocated <= 1) return false; // Only control slot

		// Count live slab bytes
		let liveBytes = 0;
		for (let i = 1; i < this.capacity; i++) {
			if (this.inUse[i] && this.slabLengths[i] > 0) {
				liveBytes += this.slabLengths[i];
			}
		}

		// Compact if more than 50% of slab is dead space
		const deadBytes = this.slabTail - liveBytes;
		return deadBytes > this.slabTail * 0.5;
	} // Compact the slab buffer, removing dead space. Invalidates any subarray views previously returned by loadSlab.
	compact(): void {
		if (!this.needsCompaction()) return;

		const newSlab = new Uint8Array(this.slab.length);
		let newTail = 0;

		for (let i = 1; i < this.capacity; i++) {
			if (!this.inUse[i]) continue;

			const length = this.slabLengths[i];
			if (length <= 0) continue;

			const oldOffset = this.slabOffsets[i];
			newSlab.set(this.slab.subarray(oldOffset, oldOffset + length), newTail);

			this.slabOffsets[i] = newTail;
			newTail += length;
		}

		// Copy back into original slab buffer
		this.slab.set(newSlab.subarray(0, newTail), 0);
		this.slabTail = newTail;
	} // Get statistics about the storage ring.
	stats(): {
		capacity: number;
		allocated: number;
		freeSlots: number;
		slabUsed: number;
		slabCapacity: number;
		slabUtilization: number;
	} {
		return {
			capacity: this.capacity,
			allocated: this.allocated,
			freeSlots: this.capacity - this.allocated,
			slabUsed: this.slabTail,
			slabCapacity: this.slab.length,
			slabUtilization: this.slabTail / this.slab.length,
		};
	} // Validate internal consistency (for debugging). Returns an array of error strings, empty if consistent.
	validate(): string[] {
		const errors: string[] = [];

		// Count free list
		let freeCount = 0;
		let current = this.freeHead;
		const seen = new Set<number>();

		while (current !== -1) {
			if (current < 1 || current >= this.capacity) {
				errors.push(`Free list contains invalid slot: ${current}`);
				break;
			}
			if (seen.has(current)) {
				errors.push(`Free list contains cycle at slot: ${current}`);
				break;
			}
			seen.add(current);
			freeCount++;
			current = this.nextFree[current];
		}

		if (freeCount !== this.capacity - this.allocated) {
			errors.push(`Free list has ${freeCount} entries, expected ${this.capacity - this.allocated}`);
		}

		// Validate slab offsets
		for (let i = 1; i < this.capacity; i++) {
			if (!this.inUse[i]) continue;

			const offset = this.slabOffsets[i];
			const length = this.slabLengths[i];

			if (offset === -1 && length !== 0) {
				errors.push(`Slot ${i}: offset -1 but length ${length}`);
			} else if (offset !== -1) {
				if (offset < 0 || offset + length > this.slabTail) {
					errors.push(`Slot ${i}: slab range [${offset}, ${offset + length}) out of bounds (tail=${this.slabTail})`);
				}
			}
		}

		return errors;
	}
	get used(): N {
		return this.allocated - 1 - this.freeCount();
	}
	get freeCount(): N {
		return this.freeHead === FREE_LIST_END ? 0 : this.walkFree();
	}
	get slabUsed(): N {
		return this.liveBytes;
	}
	get slabFree(): N {
		return this.slab.length - this.liveBytes;
	}
	private walkFree(): N {
		let n = 0;
		let p = this.freeHead;
		while (p !== FREE_LIST_END) {
			n++;
			p = this.nextFree[p];
		}
		return n;
	}
}
// #endregion
//                                                                                 CONCURRENT_BYTE_BATCH
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class CONCURRENT_BYTE_BATCH implements PARTICLE_QUEUE {
	/*  ==========================================================================================================================
     - Tier ordering: original runs numeric tiers sequentially; remake runs all tiers concurrently.
     - Concurrency: original enforces a per-tier concurrency limit; remake's Promise.all can run the entire batch simultaneously, ignoring tier.concurrency.
     - Staging: original holds tasks until execute() releases a tier; remake queues particle IDs until drain().
     - Task lifecycle: original supports retries, timeouts, error handling, and collected results; remake lacks those mechanisms.	======================================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DIRECT_SLOT_INDEX] */
	private queues = new Map<S, { pids: Pid[]; resolvers: AR<(v: U) => V> }>();
	constructor(
		private tiers: TransformTier[],
		private collider: A, //HC4
	) {
		for (const t of tiers) this.queues.set(t.name, { pids: [], resolvers: [] });
	}
	tier(name: S): TransformTier | UD {
		return this.tiers.find((t) => t.name === name);
	}
	submit(pid: Pid, tierName: S, work?: (p: PARTICLE) => U): P<U> {
		let q = this.queues.get(tierName);
		if (!q) {
			// implicit tier: any tier name referenced before registration gets
			// created on demand with sensible defaults (concurrency 4, batch 8)
			const tier: TransformTier = { name: tierName, concurrency: 4, batch: 8, run: (p: A) => p.payload };
			this.tiers.push(tier);
			q = { pids: [], resolvers: [] };
			this.queues.set(tierName, q);
		}
		return new Promise((resolve) => {
			q.pids.push(pid);
			q.resolvers.push(resolve);
			if (work) this.collider.markTierWork(pid, tierName, work);
		});
	}
	async drain(): P<V> {
		// tiers run concurrently; within a tier, batches run concurrently
		await Promise.all(
			this.tiers.map(async (tier) => {
				const q = this.queues.get(tier.name)!;
				while (q.pids.length > 0) {
					const batch = q.pids.splice(0, Math.min(tier.batch, q.pids.length));
					const resolvers = q.resolvers.splice(0, batch.length);
					await Promise.all(
						batch.map(async (pid, i) => {
							const particle = this.collider.particleAt(pid);
							if (particle) await tier.run(particle);
							resolvers[i]?.(particle?.payload);
							this.collider.clearTierWork(pid);
						}),
					);
					if (tier.concurrency === 1) continue; // preserve order for serial tiers
				}
			}),
		);
	}
	get pending(): N {
		let n = 0;
		for (const q of this.queues.values()) n += q.pids.length;
		return n;
	}
}
// #endregion
//                                                                 QUEUED_TIERED_CONCURRENT_BYTE_BATCHES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class QUEUED_TIERED_CONCURRENT_BYTE_BATCHES implements PARTICLE_QUEUE {
	/*  ==========================================================================================================================
     groups particle work into named tiers, processing batches concurrently across tiers and resolving each submission when
     its particle is processed. Each tier supports configurable batch sizes and concurrency settings, with implicit tier 
     creation for unregistered names.
    ================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [QUEUED_TIERED_CONCURRENT_BYTE_BATCHES] */
	private queues = new Map<S, { pids: Pid[]; resolvers: AR<(v: U) => V> }>();
	constructor(
		private tiers: TransformTier[],
		private collider: A, //HC4
	) {
		for (const t of tiers) this.queues.set(t.name, { pids: [], resolvers: [] });
	}
	tier(name: S): TransformTier | UD {
		return this.tiers.find((t) => t.name === name);
	}
	submit(pid: Pid, tierName: S, work?: (p: PARTICLE) => U): P<U> {
		let q = this.queues.get(tierName);
		if (!q) {
			this.tiers.push({ name: tierName, concurrency: 4, batch: 8, run: (p: A) => p.payload });
			q = { pids: [], resolvers: [] };
			this.queues.set(tierName, q);
		}
		return new Promise((resolve) => {
			q!.pids.push(pid);
			q!.resolvers.push(resolve);
			if (work) this.collider.markTierWork(pid, tierName, work);
		});
	}
	async drain(): P<V> {
		for (let t = 0; t < this.tiers.length; t++) {
			const tier = this.tiers[t];
			const q = this.queues.get(tier.name)!;
			const concurrency = Math.max(1, Math.floor(tier.concurrency) || 1);
			const batchSize = Math.max(1, Math.floor(tier.batch) || 1);

			while (q.pids.length > 0) {
				const batch = q.pids.splice(0, batchSize);
				const resolvers = q.resolvers.splice(0, batch.length);
				let next = 0;

				await Promise.all(
					Array.from({ length: Math.min(concurrency, batch.length) }, async () => {
						while (next < batch.length) {
							const i = next++;
							const pid = batch[i];
							const particle = this.collider.particleAt(pid);

							try {
								if (particle) await tier.run(particle);
								resolvers[i]?.(particle?.payload as U);
							} catch {
								resolvers[i]?.(undefined as U);
							} finally {
								this.collider.clearTierWork(pid);
							}
						}
					}),
				);
			}
		}
	}
	get pending(): N {
		let n = 0;
		for (const q of this.queues.values()) n += q.pids.length;
		return n;
	}
}
// #endregion
export const hub = new HUB();
//                                                                                         BYTE_ATOM_REG
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

/**
 x Darth Andeddu
 x Darth Atrius
 X Darth Bane
 x Darth Caldoth
 x Darth Desolous
 x Exar Kun
 - Exim Panshard
 x Felkor
 x Gol the Wither-King
 - Darth Krall 
 - Locphet
 X Darth Maul
 - Mindran
 - Darth Momin
 - Darth Ravi
 X Naga Sadow
 - Neer
 X Darth Nihilus
 - Darth Noctyss
 - Ommin
 - Darth Phobos
 x Darth Plagueis
 - Darth Quellus
 x Darth Revan
 - Ry Nymbis
 - Darth Sanguis
 - Darth Shaa
 - Darth Momin
 - Darth Siberus
 - Darth Sidious
 - Sissiri
 - Darth Skrye
 - Count Dooku
 - the Stranger
 - Darth Tanis
 - Tenebrous
 - Darth Vader
 - Darth Vitiate
 - Darth Wrend
 - Darth Zannah
 */
/**
 * 	public BYTE_MAP = (() => {
return new (class BYTE_MAP {
	readonly pidToSlot: I32;
	constructor(maxParticles: N) {this.pidToSlot = new Int32Array(maxParticles).fill(-1);}
	set(pid: N, slot: N): V {this.pidToSlot[pid] = slot;}
	get(pid: N): N {return this.pidToSlot[pid];}
	delete(pid: N): V {this.pidToSlot[pid] = -1;}
})(this.maxParticles);
})();

 */


class DARTH_VADER {
	public readonly capacity: number;
	public readonly mask: number;
	public readonly meta: Int32Array;
	public readonly values: Float64Array;
	private head = 0;
	private free: number;
	constructor(capacity = 65536) {
		if (!Number.isInteger(capacity) || capacity < 2 || capacity > 1 << 30 || (capacity & (capacity - 1)) !== 0) {
			throw new Error("Disruptor capacity must be a power of two between 2 and 2^30.");
		}
		this.capacity = capacity;
		this.mask = capacity - 1;
		this.free = capacity;
		this.meta = new Int32Array(capacity);
		this.values = new Float64Array(capacity);
	}
	public publish(pid: number, value: number, flag = 0): boolean {
		const f = this.free;
		if (f === 0) {
			return false;
		}
		const h = this.head;
		this.meta[h] = (pid << 8) | (flag & 255);
		this.values[h] = value;
		this.head = (h + 1) & this.mask;
		this.free = f - 1;
		return true;
	}
	public consume(handler: DisruptorHandler): number {
		const n = this.capacity - this.free;
		if (n === 0) {
			return 0;
		}
		const meta = this.meta;
		const values = this.values;
		const mask = this.mask;
		let r = (this.head - n) & mask;
		for (let k = 0; k < n; k++) {
			const m = meta[r];
			handler(m >>> 8, values[r], m & 255);
			r = (r + 1) & mask;
		}
		this.free = this.capacity;
		return n;
	}
	// ── Zero-callback batch path ──────────────────────────────────────────
	//   const n = ring.readable();
	//   for (let k = 0, i = ring.tail(); k < n; k++, i = (i + 1) & ring.mask) {
	//     const m = ring.meta[i]; use(m >>> 8, ring.values[i], m & 255);
	//   }
	//   ring.advance(n);
	/** Number of events published and not yet consumed. */
	public readable(): number {
		return this.capacity - this.free;
	}/** Lane index of the oldest unconsumed event (only meaningful when readable() > 0). */
	public tail(): number {
		return (this.head - (this.capacity - this.free)) & this.mask;
	}/** Mark `n` events (n <= readable()) as consumed after reading them straight from the lanes. */
	public advance(n: number): void {
		this.free += n;
	}/** Free slots remaining. */
	public remaining(): number {
		return this.free;
	}/** Empties the ring. Lanes are not zeroed: stale slots are never readable. */
	public reset(): void {
		this.head = 0;
		this.free = this.capacity;
	}
}
class SITH_SAURON {
	public hub: HUB;
	public r: A = "THE_WIZARD_ROOT_REG";
	public m: A = "SAURON_MASTER_REG";
	public s: A = "__SITH_SAURON__";
	public k: A = "__BINARY__";
	//                                                                                           NESTED MAPS
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	private stringToId = new Map<string, number>();
	private idToString: string[] = [];
	private addressToPidMap = new Map<string, number>();
	public nextPid = 1;
	// SoA Memory Lanes (Indexed by PID)
	public readonly typeMap: Uint8Array; // 0 = Single String, 1 = Multi-Channel
	public readonly channelMap: Int32Array; // r channel
	public readonly subChannelMap: Int32Array; // m channel
	public readonly tripChannelMap: Int32Array; // s channel
	public readonly keyMap: Int32Array; // k channel / single key string ID
	// Zero-alloc flyweight for multi-channel decoding
	public readonly multiFlyweight = { r: "", m: "", s: "", k: "" };
	private metadataRegistry: SithMetadataEntry[] = [];
	constructor(hub: HUB, maxPids = 65536) {
		this.hub = hub;
		this.typeMap = new Uint8Array(maxPids);
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
		this.keyMap = new Int32Array(maxPids);
	}// Input Style B: Register 3-4 channel strings -> returns a PID.
	// Decode PID back into a Single String (if registered that way).
	public getNestedMap(r: string, m: string, s: string, k = ""): number {
		const addressKey = `${r}:${m}:${s}:${k}`;
		let pid = this.addressToPidMap.get(addressKey);
		if (pid === undefined) {
			pid = this.nextPid++;
			this.addressToPidMap.set(addressKey, pid);

			this.typeMap[pid] = 1; // Mark as Multi-Channel
			this.channelMap[pid] = this.intern(r);
			this.subChannelMap[pid] = this.intern(m);
			this.tripChannelMap[pid] = this.intern(s);
			this.keyMap[pid] = this.intern(k);
		}
		return pid;
	} // Input Style A: Register a SINGLE string key -> returns a PID.
	public getSingleMap(key: string): number {
		let pid = this.addressToPidMap.get(key);
		if (pid === undefined) {
			pid = this.nextPid++;
			this.addressToPidMap.set(key, pid);

			this.typeMap[pid] = 0; // Mark as Single String
			this.keyMap[pid] = this.intern(key);
		}
		return pid;
	}
	public getMetadata(pid: number): SithMetadataEntry {// PURE BINARY REPLACEMENT FOR getMetadata: Resolves the 4-channel scope via CVRT_CHS_AND_PIDS to get a numeric pid,then indexes directly into a flat array with zero map traversal overhead.
		let meta = this.metadataRegistry[pid];
		if (!meta) {
			meta = {
				_batches: new Set<string | symbol>(),
				_batchQueue: new Map<string | symbol, A>(),
				_batchOpts: new Map<string | symbol, { triggerRender?: boolean }>(),
				_globalListeners: new Set<(key: string | symbol, value: A) => void>(),
				_global_subscribers: new Map<string, Set<() => void>>(),
				_waiters: new Map<string | symbol, Set<(value: A) => void>>(),
				_sys: new Map<string, A>(),
				_sealed: new Set<string>(),
				_frozen: new Set<string>(),
				_keyContracts: new Map<string, Set<string>>(),
				_conflicts: new Map<string, A>(),
				_conflictStrategies: new Map<string, "latest" | "fail" | "accumulate">(),
				_causal: new Map<string, A[]>(),
				_history: new Map<string | symbol, A[]>(),
				_scopes: new Map<string, A>(),
				_msgHandlers: new Map<string, Set<(p: A) => void>>(),
				_lastWriteSource: new Map<string, string>(),
				_mainMap: new Map<A, string>(),
				_tupleCache: new Map<string, string>(),
				_weakMap: new WeakMap<object, string>(),
				_boundaries: new WeakMap<object, string>(),
			};
			this.metadataRegistry[pid] = meta;
		}
		return meta;
	}
	private intern(val: string): number {
		let id = this.stringToId.get(val);
		if (id === undefined) {
			id = this.idToString.length;
			this.stringToId.set(val, id);
			this.idToString[id] = val;
		}
		return id;
	}
	public decodeSingle(pid: number): string {
		return this.idToString[this.keyMap[pid]] ?? "";
	}// Decode PID back into 3-4 Channels (zero-alloc flyweight mutation).
	public decodeMulti(pid: number): typeof this.multiFlyweight {
		this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
		this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
		this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
		this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
		return this.multiFlyweight;
	}
	// #endregion
	//                                                                                             CONSTANTS
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
    public batchDepth = 0;
    public openScopes = new Set<BatchScope>();
    public dirty = new Set<FlushCallback>();
    public _flushScheduled = false;
    public _adapter: RuntimeAdapter | null = null;
    public _currentHookState: HookState | null = null;
    public _currentComponent: ComponentInstance | null = null;
    public PRIORITY_ORDER: Priority[] = ["immediate", "user-blocking", "normal", "background", "idle"];
    public GLOBAL_SCOPE = Symbol("global-scope");
    public scopeCursors: Record<Priority, number> = { immediate: 0, "user-blocking": 0, normal: 0, background: 0, idle: 0 };
    public currentScope: symbol | string = this.GLOBAL_SCOPE;
    public loopScheduled = false;
    public _subscribers = new Map<string, Set<() => void>>();
    public __subscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();
    public _externalSubscribers = new Map<string, Map<string, Map<string, Map<SignalKey, Set<(value: A) => void>>>>>();

    public _components = new Map<string, ComponentInstance>();
    public _pendingNotifications = new Set<string>();
    public externalSubscribers = new Set<() => void>();
    public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
	// #endregion
	//                                                                                  SITH_DISRUPTOR_CODEC
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

	// #endregion
	//                                                                                  SITH_DISRUPTOR_CODEC
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
}
class SITH_DISRUPTOR_CODEC {//  Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested  Map trees with direct PID-indexed arrays.
		public hub: HUB
	
		private ids = new Map<S, N>();
		private pids = new Map<S, N>(); // "a:b:c:d" → pid
		private addresses = new Map<N, [N, N, N, N]>(); // pid → [a,b,c,d]
		public readonly hashToPidMap = new Map<N, N>();
		public _flushScheduled = false;
	
		private names: S[] = [];
		private nextId = 0;
		// ── hot path (was SITH_DISRUPTOR_CODEC) — SoA, per-pid, high volume ──
		public readonly maxPids: N;
		public readonly listeners: Array<ListenerFn | UD>;
		public readonly pidToHash: U32; // keep? see note
		public nextAllocatedPid = 1; // 0 = TRACER_PID
		public readonly channelMap: I32;
		public readonly subChannelMap: I32;
		public readonly tripChannelMap: I32;
		public readonly dirtyPids: I32;
		public dirtyCount = 0;
		public freeList: N[] = [];
		public THE_WIZARD_ROOT_REG;
		public SAURON_MASTER_REG;
		public SAURON_SLAVE_REG;
		constructor(hub: HUB,maxPids = 8192, THE_WIZARD_ROOT_REG: S, SAURON_MASTER_REG: S, SAURON_SLAVE_REG: S) {
			this.hub = hub
			this.maxPids = maxPids;
			this.listeners = new Array(maxPids);
			this.dirtyPids = new Int32Array(maxPids);
			this.pidToHash = new Uint32Array(maxPids);
			this.channelMap = new Int32Array(maxPids);
			this.subChannelMap = new Int32Array(maxPids);
			this.tripChannelMap = new Int32Array(maxPids);
			this.THE_WIZARD_ROOT_REG = THE_WIZARD_ROOT_REG;
			this.SAURON_MASTER_REG = SAURON_MASTER_REG;
			this.SAURON_SLAVE_REG = SAURON_SLAVE_REG;
		}
		public getScopeHashForPid(pid: N): N {
			return this.pidToHash[pid];
		}
		public getExistingPid(scopeHash: N): N {
			console.log("getExistingPid");
			return this.hashToPidMap.get(scopeHash) ?? -1;
		}
		public hashComponent(val: A, fallback: S): N {
			const str = String(val ?? fallback);
			let hash = 0x811c9dc5;
			for (let i = 0; i < str.length; i++) {
				hash ^= str.charCodeAt(i);
				hash = Math.imul(hash, 0x01000193) >>> 0;
			}
			return hash;
		}
		public getScopeHash(key: S | SY, sl_reg?: A, ms_reg?: A, globalKey?: A): N {
			const ch = this.hashComponent(globalKey, this.THE_WIZARD_ROOT_REG);
			const sub = this.hashComponent(ms_reg, this.SAURON_MASTER_REG);
			const trip = this.hashComponent(sl_reg, this.SAURON_SLAVE_REG);
			const k = this.hashComponent(key, "KEY");
			// Combine hash channels deterministically
			return (ch ^ sub ^ trip ^ k) >>> 0;
		}
	
		public allocateScopePid(scopeHash: N, key: A, sl_reg?: A, ms_reg?: A, globalKey?: A): N {
			let pid = this.hashToPidMap.get(scopeHash);
			if (pid !== undefined) return pid;
	
			pid = this.freeList.pop() ?? this.nextAllocatedPid++;
	
			if (pid >= this.maxPids) throw new Error("Hadron Notification Hub: PID capacity exceeded");
	
			this.hashToPidMap.set(scopeHash, pid);
			this.pidToHash[pid] = scopeHash;
	
			// Populate SoA lanes for zero-alloc reverse lookups
			this.channelMap[pid] = this.hashComponent(globalKey, this.THE_WIZARD_ROOT_REG);
			this.subChannelMap[pid] = this.hashComponent(ms_reg, this.SAURON_MASTER_REG);
			this.tripChannelMap[pid] = this.hashComponent(sl_reg, this.SAURON_SLAVE_REG);
	
			return pid;
		}
		private intern(value: S): N {
			let id = this.ids.get(value);
			if (id === undefined) {
				id = this.nextId++;
				this.ids.set(value, id);
				this.names[id] = value;
			}
			return id;
		}
		/** Exact identity: encode address → pid (interned, no collisions). */
		public encode(pid_key: S, sl: S, ms: S, rt: S): N {
			const a = this.intern(rt);
			const b = this.intern(ms);
			const c = this.intern(sl);
			const d = this.intern(pid_key);
			const addressKey = `${a}:${b}:${c}:${d}`;
	
			let pid = this.pids.get(addressKey);
			if (pid === undefined) {
				pid = this.nextAllocatedPid++;
				if (pid >= this.maxPids) throw new Error("SITH_DISRUPTOR_CODEC: PID capacity exceeded");
				this.pids.set(addressKey, pid);
				this.addresses.set(pid, [a, b, c, d]);
				// populate SoA lanes from the interned ids (no re-hash)
				this.channelMap[pid] = a;
				this.subChannelMap[pid] = b;
				this.tripChannelMap[pid] = c;
			}
			return pid;
		}
		/** Exact decode: pid → address. */
		public decode(pid: N): { k: S; s: S; m: S; r: S } {
			const addr = this.addresses.get(pid);
			if (!addr) throw new Error(`SITH_DISRUPTOR_CODEC: unknown pid ${pid}`);
			const [a, b, c, d] = addr;
			return { r: this.names[a], m: this.names[b], s: this.names[c], k: this.names[d] };
		}
		public parseScopeKey(pid: N): ScopeComponents {
			FLYWEIGHT_SCOPE.r = this.channelMap[pid];
			FLYWEIGHT_SCOPE.m = this.subChannelMap[pid];
			FLYWEIGHT_SCOPE.s = this.tripChannelMap[pid];
			return FLYWEIGHT_SCOPE;
		}
		public markDirty(pid: N): V {
			this.dirtyPids[this.dirtyCount++] = pid;
		}
		public subscribe(pid: N, fn: ListenerFn): () => V {
			this.listeners[pid] = fn;
			return () => {
				this.listeners[pid] = undefined;
			};
		}
		public flushNotifications(): V {
			this._flushScheduled = false;
			for (let i = 0; i < this.dirtyCount; i++) {
				const pid = this.dirtyPids[i];
				const fn = this.listeners[pid];
				if (fn !== undefined) fn(pid);
			}
			this.dirtyCount = 0;
		}
		public notifyHook(key: S): V {
			const pid = this.encode(key, "", "", "");
			this.markDirty(pid);
			if (!this._flushScheduled) {
				this._flushScheduled = true;
				queueMicrotask(() => {
					this._flushScheduled = false;
					this.flushNotifications();
				});
			}
		}
		public notifyPid(pid: N, value: N): V {
			const fn = this.listeners[pid];
			if (fn !== undefined) fn(pid, value);
		}
		public releasePid(pid: N): V {
			const addr = this.addresses.get(pid);
			if (addr) {
				this.pids.delete(`${addr[0]}:${addr[1]}:${addr[2]}:${addr[3]}`);
				this.addresses.delete(pid);
			}
			this.pidToHash[pid] = 0;
		}
}
class BEAM_CODEC {// Encodes ordinary JavaScript values into a collider-owned byte slab.
    public hub: HUB
	private bytes: U8;
	private view: DataView;
	private tail = 0;
	private strings = new Map<S, N>();
	private stringBytes: U8[] = [];
	private refs = new Map<object, N>();
	private nextRef = 0;
	constructor(hub: HUB, initialCapacity = 1 << 16) {
        this.hub = hub
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
	private endRef(ref: N): V { void ref; }// Reserved for future back-reference patching.
	private readU8(cursor: { at: N }): N { return this.bytes[cursor.at++];	}
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
type CBORValue = undefined | null | boolean | number | string | Uint8Array | CBORValue[] | { [k: string]: CBORValue }
class BINARY_CBOR {
	   encodeJSONEnvelope(envelope: any): Uint8Array {
		   return new TextEncoder().encode(JSON.stringify(envelope))
	   }
   
	   decodeJSONEnvelope(bytes: Uint8Array): any {
		   return JSON.parse(new TextDecoder().decode(bytes))
	   }
	   private readonly encoder = new TextEncoder()
	   private readonly decoder = new TextDecoder()
	   private ensureAvailable(buf: Uint8Array, pos: number, length: number): void { if (pos < 0 || length < 0 || pos + length > buf.length) throw new Error("CBOR: truncated buffer") }
	   private readUint(buf: Uint8Array, pos: number): [number, number] {
		   this.ensureAvailable(buf, pos, 1)
		   const info = buf[pos] & 0x1f
		   if (info <= 23) return [info, pos + 1]
		   if (info === 24) { this.ensureAvailable(buf, pos + 1, 1); return [buf[pos + 1], pos + 2] }
		   if (info === 25) { this.ensureAvailable(buf, pos + 1, 2); return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 2).getUint16(0, false), pos + 3]; }
		   if (info === 26) { this.ensureAvailable(buf, pos + 1, 4); return [new DataView(buf.buffer, buf.byteOffset + pos + 1, 4).getUint32(0, false), pos + 5]; }
		   if (info === 27) {
			   this.ensureAvailable(buf, pos + 1, 8)
			   const value = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8).getBigUint64(0, false)
			   if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("CBOR: integer exceeds Number.MAX_SAFE_INTEGER")
			   return [Number(value), pos + 9]
		   }; throw new Error(`CBOR: unsupported additional info ${info}`)
	   }
	   private encodeUint64(majorShifted: number, n: bigint): Uint8Array {
		   if (n <= 23n) return new Uint8Array([majorShifted | Number(n)])
		   if (n <= 0xffn) { return new Uint8Array([majorShifted | 24, Number(n)]) }
		   if (n <= 0xffffn) {
			   const b = new Uint8Array(3)
			   b[0] = majorShifted | 25
			   new DataView(b.buffer).setUint16(1, Number(n), false)
			   return b
		   }
		   if (n <= 0xffffffffn) {
			   const b = new Uint8Array(5)
			   b[0] = majorShifted | 26
			   new DataView(b.buffer).setUint32(1, Number(n), false)
			   return b
		   }
		   const b = new Uint8Array(9)
		   b[0] = majorShifted | 27
		   new DataView(b.buffer).setBigUint64(1, n, false)
		   return b
	   }
	   private encode(value: CBORValue): Uint8Array {
		   const parts: Uint8Array[] = []
		   this.encodeValue(value, parts)
		   const total = parts.reduce((n, p) => n + p.length, 0)
		   const out = new Uint8Array(total)
		   let offset = 0
		   for (const p of parts) { out.set(p, offset); offset += p.length }
		   return out
	   }
	   private encodeValue(v: CBORValue, out: Uint8Array[]): void {
		   if (v === null) { out.push(new Uint8Array([0xf6])); return; }
		   if (v === undefined) { out.push(new Uint8Array([0xf7])); return; }
		   if (v === true) { out.push(new Uint8Array([0xf5])); return }
		   if (v === false) { out.push(new Uint8Array([0xf4])); return }
   
		   if (typeof v === 'number') {
			   if (Number.isSafeInteger(v)) {
				   if (v >= 0) out.push(this.encodeUint64(0x00, BigInt(v)))
				   else out.push(this.encodeUint64(0x20, BigInt(-1 - v)))
			   } else {
				   const b = new Uint8Array(9)
				   b[0] = 0xfb
				   new DataView(b.buffer).setFloat64(1, v, false)
				   out.push(b)
			   }
			   return
		   }
   
		   if (typeof v === 'string') { const encoded = this.encoder.encode(v); out.push(this.encodeUint(0x60, encoded.length)); out.push(encoded); return; }
		   if (v instanceof Uint8Array) { out.push(this.encodeUint(0x40, v.length)); out.push(v); return; }
		   if (Array.isArray(v)) {
			   out.push(this.encodeUint(0x80, v.length));
			   for (const item of v) { this.encodeValue(item, out); }
			   return;
		   }
		   if (typeof v === 'object') {
			   const proto = Object.getPrototypeOf(v)
			   if (proto !== Object.prototype && proto !== null) throw new Error(`CBOR: unsupported object type ${v.constructor?.name ?? "unknown"}`)
			   const keys = Object.keys(v)
			   out.push(this.encodeUint(0xa0, keys.length))
			   for (const key of keys) { this.encodeValue(key, out); this.encodeValue(v[key], out); }
			   return
		   }; throw new Error(`CBOR: unsupported value type ${typeof v}`)
	   }
	   private encodeUint(majorShifted: number, n: number): Uint8Array {
		   if (n <= 23) return new Uint8Array([majorShifted | n]);
		   if (n <= 0xff) return new Uint8Array([majorShifted | 24, n]);
		   if (n <= 0xffff) { const b = new Uint8Array(3); b[0] = majorShifted | 25; new DataView(b.buffer).setUint16(1, n, false); return b };
		   const b = new Uint8Array(5); b[0] = majorShifted | 26; new DataView(b.buffer).setUint32(1, n, false); return b;
	   }
	   private decode(buf: Uint8Array): CBORValue {
		   const [value, end] = this.decodeValue(buf, 0);
		   if (end !== buf.length) { throw new Error("CBOR: trailing bytes") };
		   return value;
	   }
	   private decodeValue(buf: Uint8Array, pos: number): [CBORValue, number] {
		   this.ensureAvailable(buf, pos, 1)
		   const first = buf[pos]
		   const major = first >> 5
		   const info = first & 0x1f
   
		   if (major === 7) {
			   if (info === 20) return [false, pos + 1]
			   if (info === 21) return [true, pos + 1]
			   if (info === 22) return [null, pos + 1]
			   if (info === 23) return [undefined, pos + 1]
			   if (info === 27) { this.ensureAvailable(buf, pos + 1, 8); const dv = new DataView(buf.buffer, buf.byteOffset + pos + 1, 8); return [dv.getFloat64(0, false), pos + 9]; }
		   }
		   const [n, nextPos] = this.readUint(buf, pos)
		   if (major === 0) return [n, nextPos]
		   if (major === 1) return [-1 - n, nextPos]
		   if (major === 2) { this.ensureAvailable(buf, nextPos, n); const bytes = buf.slice(nextPos, nextPos + n); return [bytes, nextPos + n]; }
		   if (major === 3) { this.ensureAvailable(buf, nextPos, n); const bytes = buf.slice(nextPos, nextPos + n); return [this.decoder.decode(bytes), nextPos + n]; }
		   if (major === 4) {
			   const arr: CBORValue[] = []
			   let p = nextPos
			   for (let i = 0; i < n; i++) { const [item, np] = this.decodeValue(buf, p); arr.push(item); p = np; }
			   return [arr, p]
		   }
		   if (major === 5) {
			   const obj: Record<string, CBORValue> = {}
			   let p = nextPos
			   for (let i = 0; i < n; i++) { const [k, kp] = this.decodeValue(buf, p); const [v, vp] = this.decodeValue(buf, kp); obj[k as string] = v; p = vp; }
			   return [obj, p]
		   }; throw new Error(`CBOR: unsupported major type ${major}`)
	   }
	   public encode_msg(msg: any): Uint8Array {// Instead of leaking a raw object, encode an explicit fallback/error frame
		   if (msg instanceof Uint8Array) return msg;// If it's already an encoded Uint8Array, don't re-encode it
		   try { return this.encode(msg); }
		   catch (err) { console.log("[BINARY_PIPE] Failed to encode outbound packet strictly:", err); return this.encode({ type: "error", error: "Encoding failed", id: msg?.id || "unknown" }); }
	   }
	   public decode_msg(raw: any): any {// Guard: If it didn't arrive as binary, it's a protocol violation for this core layer
		   if (!(raw instanceof Uint8Array) && !(typeof Buffer !== 'undefined' && Buffer.isBuffer(raw))) { console.error("[BINARY_PIPE] Protocol violation: Expected encoded Uint8Array data stream."); return null; }
		   try {
			   const uint8 = raw instanceof Uint8Array ? raw : new Uint8Array((raw as any).buffer, (raw as any).byteOffset, (raw as any).length); return this.decode(uint8);
		   } catch (err) {
			   console.error("[BINARY_PIPE] Failed to decode inbound packet:", err);
			   return null;
		   }
	   }
}
// #endregion

