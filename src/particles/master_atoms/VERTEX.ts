//                                                                                                VERTEX

import { BINARY_CODEC } from "../electrons/binary";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const TRACER_PID = 0;
const OP = {
	SUBSCRIBE: 0,
	READ: 1,
	WRITE: 2,
	DELETE: 3,
	EFFECT: 4,
	COMPACT: 5,
	NONE: 6,
	SEND: 7,
	SUB: 8,
	DUPLEX: 9,
	STREAM: 10,
	DRC: 11,
	EDIT: 12,
} as const;

/** sparse update: 1-bit-per-particle dirty bitmask + compaction. */
/** clock: 4-byte bytecode sequence register [OP, EPOCH, STATE, SEQ]. */
/**pid -> slot: flat Int32Array direct-lookup (no Map, no GC). */
// #endregion
//                                                                                             interface
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
interface ScopeComponents {
	r: N;
	m: N;
	s: N;
}
const FLYWEIGHT_SCOPE: ScopeComponents = { r: 0, m: 0, s: 0 };
export class VERTEX<X = U> {
	/* =====================================================================================================
   -------- DisruptorSoA.edit --------
   Peak (Best Batch): 902.06 M ops/sec
   Average:           701.57 M ops/sec

   This one being a BADASS MOTHERFUCKA...
   Related, and maybe require the use of the following bindable, or just reachable electors 
   and / or protons:
   - byte code vm
   - storage ring: slab storage
   - governer: includes hibernation... which allows us to keep the vertex on and HOT... for about 20-30 secs... 
   IF YOU RUN IT HOT, IT WILL FUCKING RUN HOT... SO HOTTTTT, YOU WILL GET NORTH OF 300 MILL OPS P/S 
   DEPENDING ON YOUR WORKSTATION AND WHAT YOU CURRENTLY HAVE RUNNING AND WHERE YOU ARE RUNNING IT. This version
   of the disruptor is the actual version where it had hit 416 mill ops p/s, while yes... * shrugs * it saw sooommee
   optimizations, but I never obsessed about it so... I KNOW IT CAN GO HIGHER. At some point, I might get bored
   and really ring it out all the performance, the next step because of how this is designed, is multicore.
   Yes... just yes... the full tilt, bare metal... drag racing multi core version... will be here, why wouldn't it?
   WHAT DOES THIS GIVE US?...
   This gives us... the HOTTEST, FASTEST writes, that I know of right now in front end... LIKE ACTUALLY AVAILABLE 
   TO USE IN AN APP... with it being JUST THAT, is fucking awesome, but whenever you run into a care where 
   you use the same value, or the same group of values, over and over again... this is here so that you can
   hammer it out at 400 million fucking operations a second, lol
   - beam codec: Encodes ordinary JavaScript values into a collider-owned byte slab.
   - BINARY
   - INJECTION CODEV
   - SCOPE REGISTRY 
  ======================================================== ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [BINARY_CODEC]  */
	public value: U;
	public k: A = "__SIGNALS__";
	public s: S;
	public m: S;
	public r: S;
	public pid: N = 1;
	private readonly slabRegistry = new Map<number, Uint8Array>();
	// #endregion
	//                                                                                                   SOA
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	private readonly capacity: N;
	private readonly lane: I32;
	private readonly ops: U8;
	public readonly posX: F32;
	public readonly posY: F32;
	public readonly val: I32;
	private readonly freePids: I32;
	private freeHead = 0;
	private freeTail = 0;
	private laneLen = 0;
	private ids = new Map<S, N>();
	private names: S[] = [];
	private pids = new Map<S, N>(); // "a:b:c:d" → pid
	private addresses = new Map<N, [N, N, N, N]>(); // pid → [a,b,c,d]
	private nextId = 0;
	public readonly maxPids: N;
	public readonly listeners: Array<ListenerFn | UD>;
	public readonly pidToHash: U32;
	public nextAllocatedPid = 1; // 0 = TRACER_PID
	public readonly channelMap: I32;
	public readonly subChannelMap: I32;
	public readonly tripChannelMap: I32;
	public readonly dirtyPids: I32;
	need(value: U) {
		return Array.isArray(value) ? value.length + 1 : value instanceof Map || value instanceof Set ? value.size + 1 : 2;
	}
	BINARY_CODEC: BINARY_CODEC;
	constructor(value: X, k: A = "__SIGNALS__", s: S = "SAURON_SLAVE_REG", m: S = "SAURON_MASTER_REG", r: S = "THE_WIZARD_ROOT_REG", capacity = 128) {
		this.k = k;
		this.s = s;
		this.m = m;
		this.r = r;
		this.pid = 1; /// BC().resolveSingle(k);

		this.capacity = this.need(value) > 8192 ? 64 : this.need(value) <= 64 ? 64 : this.need(value) <= 128 ? 128 : this.need(value) <= 256 ? 256 : this.need(value) <= 1024 ? 1024 : 8192;
		if (capacity !== undefined) {
			this.capacity = capacity;
		}

		this.lane = new Int32Array(this.capacity).fill(-1);
		this.ops = new Uint8Array(this.capacity);
		this.posX = new Float32Array(this.capacity);
		this.posY = new Float32Array(this.capacity);
		this.val = new Int32Array(this.capacity);
		this.freePids = new Int32Array(this.capacity);

		for (let i = 1; i < this.capacity; i++) {
			this.freePids[this.freeTail++] = i;
		}

		this.lane[0] = TRACER_PID;
		this.val[TRACER_PID] = 0;
		this.ops[TRACER_PID] = OP.NONE;
		this.laneLen = 1;
		// this.edit(v, v, v, pid)
		this.BINARY_CODEC = new BINARY_CODEC();
		const v = this.BINARY_CODEC.encode_msg(value);
		const { x, y } = this.getPosition(this.pid);
		if (this.need(value) > 8192) {
			this.encodeToSlab(this.pid, v);
			this.posX[this.pid] = x;
			this.posY[this.pid] = y;
			this.ops[this.pid] = OP.WRITE;
			this.lane[this.laneLen++] = this.pid;
		} else {
			const vb = this.BINARY_CODEC.encode_msg(v);
			this.edit(x, y, vb, this.pid);
		}
		// else { this.edit(x, y, v, this.pid) }
	}
	public check_size_and_edit(value: A) {
		const v = this.BINARY_CODEC.encode_msg(value);
		const { x, y } = this.getPosition(this.pid);

		if (this.need(value) > 8192) {
			this.encodeToSlab(this.pid, v);
			this.posX[this.pid] = x;
			this.posY[this.pid] = y;
			this.ops[this.pid] = OP.EDIT;
			this.lane[this.laneLen++] = this.pid;
		} else {
			this.create(x, y, v, this.pid);
		}
	}
	public getX(pid: N): number {
		return this.posX[pid];
	}
	public getY(pid: N): number {
		return this.posY[pid];
	}
	public getPosition(pid: N): { x: number; y: number } {
		return { x: this.posX[pid], y: this.posY[pid] };
	}
	public create(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.WRITE;
		this.lane[this.laneLen++] = pid;
	}
	public edit(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		//this.posX[pid] = v;
		this.posY[pid] = y;
		// this.posY[pid] = v;
		this.val[pid] = 1;
		this.ops[pid] = OP.EDIT;
	}
	public subscribeValue(x: N, y: N, value: U8 | N, pid: Pid): V {
		// REGULAR, SUBSCRIBE TO THE VALUE IN THE DISRUPTOR
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.SUBSCRIBE;
	}
	public readValue(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.READ;
	}
	public effect(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.EFFECT;
	}
	public delete(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.DELETE;
	}
	public compact(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.COMPACT;
	}
	public none(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.NONE;
	}
	public send(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.SEND;
	}
	public sub(x: N, y: N, value: U8 | N, pid: Pid): V {
		// MESSAGE QUEUE / BUS SUBSCRIBE
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.SUB;
	}
	public duplex(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.DUPLEX;
	}
	public stream(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.STREAM;
	}
	public drc(x: N, y: N, value: U8 | N, pid: Pid): V {
		this.posX[pid] = x;
		this.posY[pid] = y;
		this.val[pid] = value;
		this.ops[pid] = OP.DRC;
	}
	private reclaimPid(pid: N): V {
		this.posX[pid] = 0;
		this.posY[pid] = 0;
		this.val[pid] = 0;
		this.ops[pid] = OP.NONE;
		this.freePids[this.freeTail] = pid;
		this.freeTail = (this.freeTail + 1) % this.capacity;
	}
	pass(): V {
		const len = this.laneLen;
		let writeIdx = 0;
		let i = 0;
		const unrolledLimit = len - (len % 4);

		for (; i < unrolledLimit; i += 4) {
			let pid = this.lane[i];
			if (pid === TRACER_PID) {
				this.val[TRACER_PID]++;
				this.lane[writeIdx++] = pid;
			} else if (this.ops[pid] === OP.DELETE) {
				this.reclaimPid(pid);
			} else {
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
			}

			pid = this.lane[i + 1];
			if (pid === TRACER_PID) {
				this.val[TRACER_PID]++;
				this.lane[writeIdx++] = pid;
			} else if (this.ops[pid] === OP.DELETE) {
				this.reclaimPid(pid);
			} else {
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
			}

			pid = this.lane[i + 2];
			if (pid === TRACER_PID) {
				this.val[TRACER_PID]++;
				this.lane[writeIdx++] = pid;
			} else if (this.ops[pid] === OP.DELETE) {
				this.reclaimPid(pid);
			} else {
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
			}

			pid = this.lane[i + 3];
			if (pid === TRACER_PID) {
				this.val[TRACER_PID]++;
				this.lane[writeIdx++] = pid;
			} else if (this.ops[pid] === OP.DELETE) {
				this.reclaimPid(pid);
			} else {
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
			}
		}

		for (; i < len; i++) {
			const pid = this.lane[i];
			if (pid === TRACER_PID) {
				this.val[TRACER_PID]++;
				this.lane[writeIdx++] = pid;
			} else if (this.ops[pid] === OP.DELETE) {
				this.reclaimPid(pid);
			} else {
				this.ops[pid] = OP.NONE;
				this.lane[writeIdx++] = pid;
			}
		}

		this.laneLen = writeIdx;
	}
	get tick(): N {
		return this.val[TRACER_PID];
	}
	get length(): N {
		return this.laneLen;
	}
	public getLoadFactor(): N {
		const activeCount = Math.max(0, this.laneLen - 1);
		return activeCount / (this.capacity - 1);
	}
	public hasSlab(pid: N): B {
		return this.slabRegistry.has(pid);
	}
	public encodeToSlab(pid: N, value: A): N {
		if (typeof value === "number") {
			this.slabRegistry.delete(pid);
			return value;
		}
		const bytes = this.BINARY_CODEC.encode_msg(value);
		this.slabRegistry.set(pid, bytes);
		return bytes.byteLength;
	}
	public clearSlab(pid: N): V {
		this.slabRegistry.delete(pid);
	}
	public readSlabValue<T>(pid: N): T | NU {
		const bytes = this.slabRegistry.get(pid);
		return bytes ? (this.BINARY_CODEC.decode_msg(bytes) as T) : null;
	}
	bind<S extends ATOM>(shard: S, owner: OWNERSHIP = "bespoke"): S {
		if (this.registry === null) {
			this.registry = new REGISTRY_PROTON(this.key, { shards: [{ shard, owner }] });
		} else {
			this.registry.bind(shard, owner);
		}
		return shard;
	}
	read() {
		if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
		const r = this.registry;
		if (r !== null) {
			const gate = r.get(GATE_ELECTRON);
			if (gate !== undefined && (gate as any).sealed) throw new SEALED_ERROR_ELECTRON(this.key);
		}
		return this.value;
	}
	write(next: X): B {
		const r = this.registry;
		if (r === null) {
			this.value = next;
			return true;
		}
		const gate = r.get(GATE_ELECTRON);
		if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
		const previous = this.value;
		if (Object.is(previous, next)) return false;
		this.value = next;
		const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
		if (subs !== undefined) subs.notify({ key: this.key, previous, current: next });
		return true;
	}
	subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
		const subs = (this.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ?? this.bind(new SUBSCRIPTION_ELECTRON<X>(this));
		return subs.subscribe(listener);
	}
	waitFor() {}
	// #endregion
}

// #endregion
