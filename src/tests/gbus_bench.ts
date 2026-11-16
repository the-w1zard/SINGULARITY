'use strict';
/**
 * bench3.js - FELKOR2 (direct-call bus) under different handlers. Plain JS, node + bun.
 *
 *   node bench3.js            run every mode, each in a fresh process
 *   bun  bench3.js
 *   DURATION=12 node bench3.js
 *
 * Same hot-loop shape as your BENCH: a write() closure called 100,000 times per chunk,
 * 1-second windows, peak = best window, avg = total / wall time. Every write() is one
 * send() that invokes a handler, so every op is a real delivery.
 *
 * Modes (what the handler does with the message):
 *   trivial   (p) => p                      handler ignores msg; msg object is dead
 *   reads     reads msg fields into a sink  msg used, but does not outlive the call
 *   escape    stores msg in a 1024-slot array  msg really gets allocated and retained
 *   multi8    8 channels, 8 different handlers, keys rotate  (polymorphic call site)
 *   stream0   sendStream, handler only
 *   stream1   sendStream, handler + 1 push listener that reads msg
 */
const { spawnSync } = require('child_process');

const DURATION = Number(process.env.DURATION || 12);
const pid = 1, v = 1;


type BUS_HANDLER<P = U, T = U> = (p: P, msg: SIG_BUS_Message1) => T | Promise<T>;
interface SIG_BUS_Message1 {
	id?: string;
	r?: S;
	m?: S;
	s: S;
	k: S;
	p?: any;
	ts?: number;
	_ack?: boolean;
	_reply?: boolean;
	_rid?: string;
	ackTo?: string;
}
interface BUS_CHANNEL {
	handler?: BUS_HANDLER<any, any>;
	push?: Set<(msg: SIG_BUS_Message1) => void>;
}
export class FELKOR2<Z extends string = string, S extends string = string> {
	/**
	Several peak runs followed by usage paths
	Peak:                                       1_228.50 M ops/sec
	Peak:                                       3_103.41 M ops/sec
	Peak:                                       2_370.05 M ops/sec
	Peak:                                       2_325.32 M ops/sec
	
	These runs were all


	FELKOR2.send
	sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
	12    |   1146.44      |   1146.44    |   1146.44   |    100.0
	
	FELKOR2.stream
	sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
	12    |   1319.94      |   1319.94    |   1319.94   |    100.0
	PS F:\mq\src> bun run aa.ts
	
	FELKOR2.iterator.publish
	sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
	12    |     540.61     |   540.61     |  540.61     |    100.3
	PS F:\mq\src> bun run aa.ts
	
	FELKOR2.duplex
	sec | M writes/s | best M/s | avg M/s | CPU %
	12 |     898.32 |   898.32 |  898.32 | 100.0
	 */
	public s: S;
	public m: S;
	public r: S;
	public k: S;
	private channels: Record<S, BUS_CHANNEL>;
	constructor(k: A =  "__SAURON__",s: S = "SAURON_SLAVE_REG" as S, m: S = "SAURON_MASTER_REG" as S, r: S = "THE_WIZARD_ROOT_REG" as S) {
		this.s = s;
		this.m = m;
		this.r = r;
		this.k = k;

		const root = globalThis as any;

		const rL = (root[r] ??= Object.create(null));
		const mL = (rL[m] ??= Object.create(null));
		this.channels = mL[s] ??= Object.create(null);
	}
	// const msg: SIG_BUS_Message1 = { r, m, s, k, p: { p }, ts: Date.now(), _ack: false, };
	// const result = handler(p, msg);
	public send<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
		const channel = this.channels[k];
		const handler = channel?.handler;
		if (!handler) return undefined;
		return handler(p, { r, m, s, k, p: p });
	}
	public sendStream<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
		const channel = this.channels[k];
		const handler = channel?.handler;
		if (!handler) return undefined;
		const msg: SIG_BUS_Message1 = { r, m, s, k, p: p, _ack: false };
		const result = handler(p, msg);
		const listeners = channel.push;
		if (listeners) {
			for (const listener of listeners) listener(msg);
		}
		return result as T | P<T> | UD;
	}
	public sub<P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V {
		const channel = (this.channels[k] ??= {});
		channel.handler = handler;
		return () => {
			if (channel.handler === handler) {
				channel.handler = undefined;
			}
		};
	}
	public stream(handler: (msg: SIG_BUS_Message1) => V, k: S, s: S, m?: S, r?: S): () => V;
	public stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<SIG_BUS_Message1>;
	public stream(handlerOrSlave: ((msg: SIG_BUS_Message1) => V) | S, k: S, s: S = this.s, m: S = this.m, r: S = this.r): (() => V) | AsyncIterableIterator<SIG_BUS_Message1> {
		const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
		const global = globalThis as any;
		const channel = (this.channels[k] ??= {});

		if (!channel) throw new Error("Channel does not exist");

		const listeners = (channel.push ??= new Set());

		if (handler) {
			listeners.add(handler);

			return () => {
				listeners.delete(handler);
				if (!listeners.size) channel.push = undefined;
			};
		}

		const queue: SIG_BUS_Message1[] = [];
		const waiters: Array<{ resolve: (value: IteratorResult<SIG_BUS_Message1>) => void }> = [];

		let offset = 0;
		let closed = false;

		const listener = (msg: SIG_BUS_Message1) => {
			if (waiters.length) {
				waiters.shift()!.resolve({ value: msg, done: false });
			} else {
				queue.push(msg);
			}
		};

		listeners.add(listener);

		const close = () => {
			if (closed) return;
			closed = true;

			listeners.delete(listener);
			if (!listeners.size) channel.push = undefined;

			for (const waiter of waiters) {
				waiter.resolve({ value: undefined, done: true });
			}

			waiters.length = 0;
			queue.length = 0;
			offset = 0;
		};

		const iterator: AsyncIterableIterator<SIG_BUS_Message1> = {
			next: (): Promise<IteratorResult<SIG_BUS_Message1>> => {
				if (offset < queue.length) {
					const value = queue[offset++];

					if (offset === queue.length) {
						queue.length = 0;
						offset = 0;
					}

					return Promise.resolve({ value, done: false });
				}

				if (closed) {
					return Promise.resolve({ value: undefined, done: true });
				}

				return new Promise((resolve) => waiters.push({ resolve }));
			},

			return: async () => {
				close();
				return { value: undefined, done: true };
			},

			[Symbol.asyncIterator](): AsyncIterableIterator<SIG_BUS_Message1> {
				return iterator;
			},
		};

		return iterator;
	}
	public duplex(k: S, s: S = this.s, m: S = this.m, r: S = this.r) {
		return {
			send: <T = U, P = U>(p: P) => this.sendStream<T>(p, k, s, m, r),
			sub: <P = U, T = U>(handler: BUS_HANDLER<P, T>) => this.sub<P, T>(handler, k, s, m, r),
			stream: (handler?: (msg: SIG_BUS_Message1) => V) => (handler ? this.stream(handler, k, s, m, r) : this.stream(k, s, m, r)),
		};
	}
}
// ---------------------------------------------------------------- child: one mode
function child(mode) {
  const bus = new FELKOR2();
  const acc = new Int32Array(4);
  const keep = new Array(1024).fill(null); let kn = 0;
  let write;

  if (mode === 'trivial') {
    bus.sub((p) => p, 'k');
    write = () => { bus.send(v, 'k'); };
  } else if (mode === 'reads') {
    bus.sub((p, msg) => { acc[0] = (acc[0] + p + msg.k.length + msg.s.length) | 0; return p; }, 'k');
    write = () => { bus.send(v, 'k'); };
  } else if (mode === 'escape') {
    bus.sub((p, msg) => { keep[kn++ & 1023] = msg; return p; }, 'k');
    write = () => { bus.send(v, 'k'); };
  } else if (mode === 'multi8') {
    const hs = [(p) => p + 1, (p) => p + 2, (p) => p * 3, (p) => p - 4, (p) => p ^ 5, (p) => p | 6, (p) => p & 7, (p) => p + 8];
    const keys = hs.map((_, i) => 'k' + i);
    hs.forEach((h, i) => bus.sub(h, keys[i]));
    let i = 0;
    write = () => { bus.send(v, keys[i]); i = (i + 1) & 7; };
  } else if (mode === 'roundtrip') {
    // sub replies with a value derived from the message; caller consumes the reply
    bus.sub((p, msg) => p + msg.k.length, 'k');
    write = () => { acc[2] = (acc[2] + bus.send(v, 'k')) | 0; };
  } else if (mode === 'replyobj') {
    // sub returns a NEW message object; caller reads a field off the reply
    bus.sub((p, msg) => ({ id: msg.k, p: p + 1, ok: true }), 'k');
    write = () => { acc[2] = (acc[2] + bus.send(v, 'k').p) | 0; };
  } else if (mode === 'stream0') {
    bus.sub((p) => p, 'k');
    write = () => { bus.sendStream(v, 'k'); };
  } else if (mode === 'stream1') {
    bus.sub((p) => p, 'k');
    bus.stream((msg) => { acc[1] = (acc[1] + msg.p) | 0; }, 'k');
    write = () => { bus.sendStream(v, 'k'); };
  } else throw new Error('unknown mode ' + mode);

  const ns = (ms) => BigInt(Math.round(ms)) * 1000000n;
  const warmEnd = process.hrtime.bigint() + ns(0);
  while (process.hrtime.bigint() < warmEnd) for (let i = 0; i < 100000; i++) write();

  const wins = new Float64Array(100000); let nw = 0;
  const t0 = process.hrtime.bigint(), end = t0 + ns(DURATION * 1000);
  let wStart = t0, wOps = 0, total = 0, now = t0;
  do {
    for (let i = 0; i < 100000; i++) write();
    wOps += 100000; total += 100000;
    now = process.hrtime.bigint();
    if (now - wStart >= 1000000000n) { wins[nw++] = wOps / (Number(now - wStart) / 1e9); wOps = 0; wStart = now; }
  } while (now < end);
  const avg = total / (Number(now - t0) / 1e9);
  let peak = 0; for (let i = 0; i < nw; i++) if (wins[i] > peak) peak = wins[i];
  if (nw === 0) peak = avg;
  console.log(JSON.stringify({ mode, peak, avg, check: acc[0] + acc[1] + acc[2] + kn }));
}

// ---------------------------------------------------------------- parent
function parent() {
  const rt = typeof Bun !== 'undefined' ? 'bun ' + Bun.version : 'node ' + process.version;
  console.log(`${rt} | ${DURATION}s per run (+1s warmup) | each op = one send() that runs a handler\n`);
  console.log('mode'.padEnd(10), 'peak M/s'.padStart(10), 'avg M/s'.padStart(10));
  for (const mode of ['trivial', 'roundtrip', 'replyobj', 'reads', 'escape', 'multi8', 'stream0', 'stream1']) {
    const out = spawnSync(process.execPath, [__filename, mode], { env: process.env, encoding: 'utf8' });
    let j;
    try { j = JSON.parse(out.stdout.trim().split('\n').pop()); } catch (e) { console.log(mode, 'FAILED', out.stderr); continue; }
    console.log(mode.padEnd(10), (j.peak / 1e6).toFixed(0).padStart(10), (j.avg / 1e6).toFixed(0).padStart(10));
  }
}

if (process.argv.length >= 3) child(process.argv[2]); else parent();
