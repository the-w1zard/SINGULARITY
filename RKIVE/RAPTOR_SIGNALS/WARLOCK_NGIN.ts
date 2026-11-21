type Payload = unknown;
type SubHandler = (    payload: Payload,    reply: (value: Payload) => void,) => Payload | void;
type StreamSource = () => Iterable<Payload> | AsyncIterable<Payload>;
type StreamHandler = (chunk: Payload) => void;
type ExecHandler = (pid: Pid) => void;
interface ChannelAddress {
    r: S;
    m: S;
    s: S;
    k: S;
}
const TRACER_PID = 0;
const OP = {
    // Control / inspection lanes
    READ: 0,
    CHECK_DIRTY: 1,
    IS_VALID_LINK: 2,
    GET_ACTIVE_SUB: 3,
    SET_ACTIVE_SUB: 4,
    GET_BATCH_DEPTH: 5,
    START_BATCH: 6,
    END_BATCH: 7,

    // Value update lanes
    SIGNAL_OPER: 8,
    UPDATE_SIGNAL: 9,
    WRITE: 10,
    EDIT: 11,

    // Computed lanes
    COMPUTED: 12,
    COMPUTED_OPER: 13,
    UPDATE_COMPUTED: 14,

    // Effect lanes
    EFFECT: 15,
    EFFECT_OPER: 16,
    EFFECT_SCOPE_OPER: 17,

    // Execution lane
    RUN: 18,

    // Propagation lanes
    NOTIFY: 19,
    PROPAGATE: 20,
    SHALLOW_PROPAGATE: 21,

    // Channel lanes
    SEND: 22,
    SUB: 23,
    DUPLEX: 24,
    STREAM: 25,
    DRC: 26,
    TRANSFORM: 27,

    // Link maintenance lanes
    LINK: 28,
    UNLINK: 29,
    PURGE_DEPS: 30,
    COMPACT: 31,
    DISPOSE_ALL_DEPS_IN_REVERSE: 32,

    // Lifecycle lanes
    FLUSH: 33,
    CLEANUP: 34,
    NONE: 35,
    DELETE: 36,
} as const;
const STATE = {
    EMPTY: 0,
    READY: 1,
    DIRTY: 2,
    RUNNING: 3,
    DISPOSED: 4,
} as const;
const PAYLOAD_TYPE = {
    UNDEFINED: 0,
    NULL: 1,
    FALSE: 2,
    TRUE: 3,
    NUMBER: 4,
    STRING: 5,
    ARRAY: 6,
    OBJECT: 7,
} as const;
const KIND = {
    SIGNAL: 0,
    COMPUTED: 1,
    EFFECT: 2,
    CHANNEL: 3,
} as const;
const IN_LANE = 0x80;     
const OP_MASK = 0x7F;    
type OPERATIONS = | "READ"| "WRITE"| "DELETE"| "EFFECT"| "COMPACT"| "NONE"| "EDIT"| "COMPUTED"| "LINK"| "UNLINK"| "NOTIFY"| "PROPAGATE"| "SHALLOW_PROPAGATE"| "RUN"| "FLUSH"| "DISPOSE_ALL_DEPS_IN_REVERSE"| "GET_ACTIVE_SUB"| "SET_ACTIVE_SUB"| "START_BATCH"| "END_BATCH"| "PURGE_DEPS"| "CLEANUP"| "SEND"| "SUB"| "DUPLEX"| "STREAM"| "DRC"| "TRANSFORM"| "CHECK_DIRTY"| "IS_VALID_LINK"| "COMPUTED_OPER"| "SIGNAL_OPER"| "EFFECT_OPER"| "EFFECT_SCOPE_OPER"| "GET_BATCH_DEPTH"| "UPDATE_SIGNAL"| "UPDATE_COMPUTED"

export class WARLOCK_NGIN {
	public r: S = "THE_WIZARD_ROOT_REG";
	public m: S = "SAURON_MASTER_REG";
	public s: S = "SAURON_SLAVE_REG";
	public k: S = "__SAURON__";
	private readonly capacity: N;
	private readonly lane: I32;
	private readonly ops: U8;
	public readonly posX: F32;
	public readonly posY: F32;
	public readonly val: I32;
	private readonly freePids: I32;
	private freeTail = 0;
	private laneLen = 0;
	// Lifecycle / reactivity columns
	private readonly state: U8;
	private readonly version: I32;
	private readonly queued: U8;
	// Direct execution columns
	private readonly subscriberOffset: I32;
	private readonly execPath: I32;
	private readonly sourceOffset: I32;
	private readonly streamHandlerOffset: I32;
	// Channel identity: r / m / s / k
	private readonly chanR: I32;
	private readonly chanM: I32;
	private readonly chanS: I32;
	private readonly chanK: I32;
	// Encoded payload location: p
	private readonly payloadPtr: I32;
	private readonly payloadLen: I32;
	// Flat link slots
	private readonly linkHead: I32;
	private readonly linkNext: I32;
	private readonly linkTarget: I32;
	private readonly linkOwner: I32;
	private readonly freeLinks: I32;
	private freeLinkTail = 0;
	private readonly linkCapacity: N;
	// Channel index
	private readonly hashSlots: I32;
	private readonly hashMask = (1 << 20) - 1;
	// Payload arena
	private bytes: U8;
	private byteLength = 0;
	// Direct execution paths
	private handlers: Array<SubHandler | StreamSource | StreamHandler | ExecHandler | null	> = [];
	private freeHandlers: N[] = [];
	// String interning for r / m / s / k
	private strings: string[] = [];
	private stringIds = new Map<string, N>();
	// Active subscriber and batching
	private activeSub = -1;
	private batchDepth = 0;
	// Pending send continuations
	private pending = new Map<N, (value: Payload) => void>();
	private sendSeq = 1;
	private readonly laneCount = 37;
	private lanePids: Array<I32>;
	private laneX: Array<I32>;
	private laneY: Array<I32>;
	private laneV: Array<I32>;
	private laneRead: I32;
	private laneWrite: I32;
	private laneCaps: I32;
	private initialLaneCapacity: N;
	private readonly kind: U8;
	private freePayloadPtr: N[] = [];
	private freePayloadLen: N[] = [];
	private branch:  Record<S, A> = Object.create(null);
	pid_codec: CONVERT_CHANNELS_AND_PIDS
	beam_codec: BEAM_CODEC
	constructor(capacity = 1024, k :A = "__SAURON__" , s:A = "SAURON_SLAVE_REG", m:A = "SAURON_MASTER_REG" ,r:A= "THE_WIZARD_ROOT_REG" ) 	{
		this.r =r;
		this.m = m;
		this.s = s;
		this.k = k;
		this.pid_codec = new CONVERT_CHANNELS_AND_PIDS(capacity)
		this.beam_codec = new BEAM_CODEC(capacity)

		const rr = this.pid_codec.resolveSingle(r)
		const mm = this.pid_codec.resolveSingle(m)
		const ss = this.pid_codec.resolveSingle(s)
		const root = this.branch;
		const rL = (root[rr] ??= Object.create(null));
		const mL = (rL[mm] ??= Object.create(null));
		this.branch = mL[ss] ??= Object.create(null);

		this.capacity = capacity;
		this.lane = new Int32Array(capacity).fill(-1);
		this.ops = new Uint8Array(capacity);
		this.posX = new Float32Array(capacity);
		this.posY = new Float32Array(capacity);
		this.val = new Int32Array(capacity);
		this.freePids = new Int32Array(capacity);

		for (let i = 1; i < capacity; i++) {
			this.freePids[this.freeTail++] = i;
		}
		this.initialLaneCapacity = Math.min(capacity, 4096);

		this.lanePids = new Array<I32>(this.laneCount);
		this.laneX = new Array<I32>(this.laneCount);
		this.laneY = new Array<I32>(this.laneCount);
		this.laneV = new Array<I32>(this.laneCount);

		this.laneRead = new Int32Array(this.laneCount);
		this.laneWrite = new Int32Array(this.laneCount);
		this.laneCaps = new Int32Array(this.laneCount);

		for (let op = 0; op < this.laneCount; op++) {
			this.lanePids[op] = new Int32Array(this.initialLaneCapacity);
			this.laneX[op] = new Int32Array(this.initialLaneCapacity);
			this.laneY[op] = new Int32Array(this.initialLaneCapacity);
			this.laneV[op] = new Int32Array(this.initialLaneCapacity);

			this.laneRead[op] = 0;
			this.laneWrite[op] = 0;
			this.laneCaps[op] = this.initialLaneCapacity;
		}
		this.kind = new Uint8Array(capacity);
		this.lane[0] = TRACER_PID;
		this.val[TRACER_PID] = 0;
		this.ops[TRACER_PID] = OP.NONE;
		this.laneLen = 1;

		this.state = new Uint8Array(capacity);
		this.version = new Int32Array(capacity);
		this.queued = new Uint8Array(capacity);

		this.subscriberOffset = new Int32Array(capacity).fill(-1);
		this.execPath = new Int32Array(capacity).fill(-1);
		this.sourceOffset = new Int32Array(capacity).fill(-1);
		this.streamHandlerOffset = new Int32Array(capacity).fill(-1);

		this.chanR = new Int32Array(capacity).fill(-1);
		this.chanM = new Int32Array(capacity).fill(-1);
		this.chanS = new Int32Array(capacity).fill(-1);
		this.chanK = new Int32Array(capacity).fill(-1);

		this.payloadPtr = new Int32Array(capacity).fill(-1);
		this.payloadLen = new Int32Array(capacity);

		this.linkCapacity = capacity * 4;

		this.linkHead = new Int32Array(capacity).fill(-1);
		this.linkNext = new Int32Array(this.linkCapacity).fill(-1);
		this.linkTarget = new Int32Array(this.linkCapacity);
		this.linkOwner = new Int32Array(this.linkCapacity);

		this.freeLinks = new Int32Array(this.linkCapacity);

		for (let i = this.linkCapacity - 1; i >= 0; i--) {
			this.freeLinks[this.freeLinkTail++] = i;
		}

		this.hashSlots = new Int32Array(1 << 20).fill(-1);

		this.bytes = new Uint8Array(1 << 20);
	}
	private reclaimPid(pid: N): V {
		if (this.state[pid] === STATE.DISPOSED) {
			return;
		}
	
		this.posX[pid] = 0;
		this.posY[pid] = 0;
		this.val[pid] = 0;
		this.ops[pid] = OP.NONE;
	
		this.state[pid] = STATE.DISPOSED;
	
		const handler = this.execPath[pid];
	
		if (handler >= 0) {
			this.handlers[handler] = null;
			this.freeHandlers.push(handler);
		}
	
		const subscriber = this.subscriberOffset[pid];
	
		if (subscriber >= 0) {
			this.handlers[subscriber] = null;
			this.freeHandlers.push(subscriber);
		}
	
		const source = this.sourceOffset[pid];
	
		if (source >= 0) {
			this.handlers[source] = null;
			this.freeHandlers.push(source);
		}
	
		const streamHandler = this.streamHandlerOffset[pid];
	
		if (streamHandler >= 0) {
			this.handlers[streamHandler] = null;
			this.freeHandlers.push(streamHandler);
		}
	
		if (this.kind[pid] === KIND.CHANNEL) {
			this.removeChannel(pid);
		}
	
		this.purgeLinks(pid);
	
		this.freePids[this.freeTail++] = pid;
	}
	public laneSlice(): Int32Array {
		let total = 0;
	
		for (let lane = 0; lane < this.laneCount; lane++) {
			total += this.laneWrite[lane] - this.laneRead[lane];
		}
	
		const output = new Int32Array(total);
		let cursor = 0;
	
		for (let lane = 0; lane < this.laneCount; lane++) {
			const read = this.laneRead[lane];
			const write = this.laneWrite[lane];
			const pids = this.lanePids[lane];
	
			for (let i = read; i < write; i++) {
				output[cursor++] = pids[i];
			}
		}
	
		return output;
	}
    public getX(pid: N): N { return this.posX[pid];	}
	public getY(pid: N): N { return this.posY[pid];	}
	public getPosition(pid: N): { x: N; y: N } { return { x: this.posX[pid], y: this.posY[pid] };	}
	private growLane(lane: N): V {
		const oldCapacity = this.laneCaps[lane];
		const newCapacity = oldCapacity * 2;
	
		const oldPids = this.lanePids[lane];
		const oldX = this.laneX[lane];
		const oldY = this.laneY[lane];
		const oldV = this.laneV[lane];
	
		const newPids = new Int32Array(newCapacity);
		const newX = new Int32Array(newCapacity);
		const newY = new Int32Array(newCapacity);
		const newV = new Int32Array(newCapacity);
	
		const count = this.laneWrite[lane];
	
		for (let i = 0; i < count; i++) {
			newPids[i] = oldPids[i];
			newX[i] = oldX[i];
			newY[i] = oldY[i];
			newV[i] = oldV[i];
		}
	
		this.lanePids[lane] = newPids;
		this.laneX[lane] = newX;
		this.laneY[lane] = newY;
		this.laneV[lane] = newV;
	
		this.laneCaps[lane] = newCapacity;
	}
	private admit(
		pid: N,
		op: N,
		x: N = 0,
		y: N = 0,
		v: N = 0,
	): V {
		const lane = op;
		const write = this.laneWrite[lane];
	
		if (write >= this.laneCaps[lane]) {
			this.growLane(lane);
		}
	
		this.lanePids[lane][write] = pid;
		this.laneX[lane][write] = x;
		this.laneY[lane][write] = y;
		this.laneV[lane][write] = v;
	
		this.laneWrite[lane] = write + 1;
	
		this.ops[pid] = op | IN_LANE;
	}
	OP(
		x: N,
		y: N,
		v: N,
		pid: Pid,
		op: OPERATIONS = "READ",
	): V {
		this.admit(pid, OP[op], x, y, v);
	}
	PASS(): V {
		let active = true;
	
		while (active) {
			active = false;
	
			for (let lane = 0; lane < this.laneCount; lane++) {
				const read = this.laneRead[lane];
				const write = this.laneWrite[lane];
	
				if (read < write) {
					active = true;
	
					this.executeOne(lane);
				}
	
				if (this.laneRead[lane] === this.laneWrite[lane]) {
					this.laneRead[lane] = 0;
					this.laneWrite[lane] = 0;
				}
			}
		}
	}
	private executeOne(lane: N): V {
		const entry = this.laneRead[lane];
	
		const pid = this.lanePids[lane][entry];
		const x = this.laneX[lane][entry];
		const y = this.laneY[lane][entry];
		const v = this.laneV[lane][entry];
	
		this.laneRead[lane] = entry + 1;
	
		switch (lane) {
			case OP.READ: {
				this.activeSub = pid;
	
				break;
			}
	
			case OP.CHECK_DIRTY: {
				this.val[pid] = this.state[x] === STATE.DIRTY ? 1 : 0;
	
				break;
			}
	
			case OP.IS_VALID_LINK: {
				let current = this.linkHead[x];
				let valid = 0;
	
				while (current !== -1) {
					if (this.linkOwner[current] === y) {
						valid = 1;
						break;
					}
	
					current = this.linkNext[current];
				}
	
				this.val[pid] = valid;
	
				break;
			}
	
			case OP.GET_ACTIVE_SUB: {
				this.val[pid] = this.activeSub;
	
				break;
			}
	
			case OP.SET_ACTIVE_SUB: {
				this.activeSub = v;
	
				break;
			}
	
			case OP.GET_BATCH_DEPTH: {
				this.val[pid] = this.batchDepth;
	
				break;
			}
	
			case OP.START_BATCH: {
				this.batchDepth++;
	
				break;
			}
	
			case OP.END_BATCH: {
				if (this.batchDepth > 0) {
					this.batchDepth--;
				}
	
				break;
			}
	
			case OP.SIGNAL_OPER: {
				this.state[pid] = STATE.READY;
				this.version[pid]++;
	
				break;
			}
	
			case OP.UPDATE_SIGNAL: {
				this.version[pid]++;
				this.state[pid] = STATE.READY;
	
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.WRITE: {

				this.val[pid] = v;
				this.version[pid]++;
				this.state[pid] = STATE.READY;
	
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.EDIT: {
				this.version[pid]++;
				this.state[pid] = STATE.READY;
	
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.COMPUTED: {
				this.state[pid] = STATE.RUNNING;
	
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				this.version[pid]++;
				this.state[pid] = STATE.READY;
	
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.COMPUTED_OPER: {
				this.state[pid] = STATE.RUNNING;
	
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				this.version[pid]++;
				this.state[pid] = STATE.READY;
	
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.UPDATE_COMPUTED: {
				this.state[pid] = STATE.RUNNING;
	
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				this.version[pid]++;
				this.state[pid] = STATE.READY;
	
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.EFFECT: {
				this.state[pid] = STATE.RUNNING;
	
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.EFFECT_OPER: {
				this.state[pid] = STATE.RUNNING;
	
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.EFFECT_SCOPE_OPER: {
				const previousActiveSub = this.activeSub;
	
				this.activeSub = pid;
				this.state[pid] = STATE.RUNNING;
	
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				this.activeSub = previousActiveSub;
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.RUN: {
				const path = this.execPath[pid];
	
				if (path >= 0) {
					const handler = this.handlers[path] as ExecHandler | null;
	
					if (handler) {
						handler(pid);
					}
				}
	
				if (this.kind[pid] === KIND.COMPUTED) {
					this.version[pid]++;
					this.notifyDependents(pid);
				}
	
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.NOTIFY: {
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.PROPAGATE: {
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.SHALLOW_PROPAGATE: {
				this.notifyDependents(pid);
	
				break;
			}
	
			case OP.SEND: {
				const handler = this.handlers[
					this.subscriberOffset[pid]
				] as SubHandler | null;
			
				const sendId = v;
			
				const resolve = this.pending.get(sendId);
			
				if (!handler || !resolve) {
					if (resolve) {
						this.pending.delete(sendId);
						resolve(undefined);
					}
			
					break;
				}
				const payload = this.val[pid] as Payload;
				//const payload = this.decodePayload(pid);
			
				const reply = (value: Payload): void => {
					const pendingResolve = this.pending.get(sendId);
			
					if (pendingResolve) {
						this.pending.delete(sendId);
						pendingResolve(value);
					}
				};
			
				const returned = handler(payload, reply);
			
				if (returned !== undefined) {
					reply(returned);
				}
			
				break;
			}
	
			case OP.SUB: {
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.DUPLEX: {
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.STREAM: {
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.DRC: {
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.TRANSFORM: {
				this.state[pid] = STATE.READY;
	
				break;
			}
	
			case OP.LINK: {
				const link = this.allocLink();
	
				this.linkOwner[link] = y;
				this.linkNext[link] = this.linkHead[x];
				this.linkHead[x] = link;
	
				break;
			}
	
			case OP.UNLINK: {
				let current = this.linkHead[x];
				let previous = -1;
	
				while (current !== -1) {
					if (this.linkOwner[current] === y) {
						if (previous === -1) {
							this.linkHead[x] = this.linkNext[current];
						} else {
							this.linkNext[previous] = this.linkNext[current];
						}
	
						this.freeLink(current);
	
						break;
					}
	
					previous = current;
					current = this.linkNext[current];
				}
	
				break;
			}
	
			case OP.PURGE_DEPS: {
				this.purgeLinks(pid);
	
				break;
			}
	
			case OP.COMPACT: {
				this.purgeLinks(pid);
	
				break;
			}
	
			case OP.DISPOSE_ALL_DEPS_IN_REVERSE: {
				this.purgeLinks(pid);
	
				break;
			}
	
			case OP.FLUSH: {
				break;
			}
	
			case OP.CLEANUP: {
				this.cleanupPid(pid);
	
				break;
			}
	
			case OP.NONE: {
				break;
			}
	
			case OP.DELETE: {
				this.reclaimPid(pid);
	
				break;
			}
		}
	
		this.ops[pid] = OP.NONE;
	}
	private cleanupPid(pid: N): V {
		this.purgeLinks(pid);
	
		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;
	
		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;
	
		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;
	
		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;
	}
	private notifyDependents(pid: N): V {
		let link = this.linkHead[pid];
	
		while (link !== -1) {
			const target = this.linkOwner[link];
	
			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;
	
				this.admit(target, OP.RUN);
			}
	
			link = this.linkNext[link];
		}
	}
	// ------------------------------------------------------------------
	// Payload arena
	// ------------------------------------------------------------------
	private ensureBytes(required: N): V {
		if (this.byteLength + required <= this.bytes.length) return;

		let size = this.bytes.length;

		while (size < this.byteLength + required) {
			size *= 2;
		}

		const next = new Uint8Array(size);

		next.set(this.bytes.subarray(0, this.byteLength));

		this.bytes = next;
	}
	private writeU8(value: N): V {
		this.ensureBytes(1);

		this.bytes[this.byteLength++] = value & 0xff;
	}
	private writeU32(value: N): V {
		this.ensureBytes(4);

		new DataView(
			this.bytes.buffer,
			this.bytes.byteOffset + this.byteLength,
			4,
		).setUint32(0, value, true);

		this.byteLength += 4;
	}
	private writeF64(value: N): V {
		this.ensureBytes(8);

		new DataView(
			this.bytes.buffer,
			this.bytes.byteOffset + this.byteLength,
			8,
		).setFloat64(0, value, true);

		this.byteLength += 8;
	}
	private writeString(value: string): V {
		const encoded = new TextEncoder().encode(value);

		this.writeU32(encoded.length);

		this.ensureBytes(encoded.length);

		this.bytes.set(encoded, this.byteLength);

		this.byteLength += encoded.length;
	}
	private encodePayload(value: Payload): { ptr: N; len: N } {
		const ptr = this.byteLength;

		this.encodeValue(value);

		return {
			ptr,
			len: this.byteLength - ptr,
		};
	}
	private encodeValue(value: Payload): V {
		if (value === undefined) {
			this.writeU8(PAYLOAD_TYPE.UNDEFINED);
			return;
		}

		if (value === null) {
			this.writeU8(PAYLOAD_TYPE.NULL);
			return;
		}

		if (value === false) {
			this.writeU8(PAYLOAD_TYPE.FALSE);
			return;
		}

		if (value === true) {
			this.writeU8(PAYLOAD_TYPE.TRUE);
			return;
		}

		if (typeof value === "number") {
			this.writeU8(PAYLOAD_TYPE.NUMBER);
			this.writeF64(value);
			return;
		}

		if (typeof value === "string") {
			this.writeU8(PAYLOAD_TYPE.STRING);
			this.writeString(value);
			return;
		}

		if (Array.isArray(value)) {
			this.writeU8(PAYLOAD_TYPE.ARRAY);
			this.writeU32(value.length);

			for (const item of value) {
				this.encodeValue(item);
			}

			return;
		}

		if (typeof value === "object") {
			const entries = Object.entries(value as Record<string, Payload>);

			this.writeU8(PAYLOAD_TYPE.OBJECT);
			this.writeU32(entries.length);

			for (const [key, item] of entries) {
				this.writeString(key);
				this.encodeValue(item);
			}

			return;
		}

		throw new TypeError("Unsupported payload value");
	}
	public decodePayload(pid: N): Payload {
		const ptr = this.payloadPtr[pid];

		if (ptr < 0) return undefined;

		const [value] = this.decodeAt(ptr);

		return value;
	}
	private decodeAt(offset: N): [Payload, N] {
		const type = this.bytes[offset];
		let cursor = offset + 1;

		if (type === PAYLOAD_TYPE.UNDEFINED) return [undefined, cursor];
		if (type === PAYLOAD_TYPE.NULL) return [null, cursor];
		if (type === PAYLOAD_TYPE.FALSE) return [false, cursor];
		if (type === PAYLOAD_TYPE.TRUE) return [true, cursor];

		if (type === PAYLOAD_TYPE.NUMBER) {
			const value = new DataView(
				this.bytes.buffer,
				this.bytes.byteOffset + cursor,
				8,
			).getFloat64(0, true);

			return [value, cursor + 8];
		}

		if (type === PAYLOAD_TYPE.STRING) {
			const length = new DataView(
				this.bytes.buffer,
				this.bytes.byteOffset + cursor,
				4,
			).getUint32(0, true);

			const start = cursor + 4;
			const end = start + length;

			const value = new TextDecoder().decode(
				this.bytes.subarray(start, end),
			);

			return [value, end];
		}

		if (type === PAYLOAD_TYPE.ARRAY) {
			const count = new DataView(
				this.bytes.buffer,
				this.bytes.byteOffset + cursor,
				4,
			).getUint32(0, true);

			cursor += 4;

			const output: Payload[] = [];

			for (let i = 0; i < count; i++) {
				const [item, next] = this.decodeAt(cursor);

				output.push(item);
				cursor = next;
			}

			return [output, cursor];
		}

		if (type === PAYLOAD_TYPE.OBJECT) {
			const count = new DataView(
				this.bytes.buffer,
				this.bytes.byteOffset + cursor,
				4,
			).getUint32(0, true);

			cursor += 4;

			const output: Record<string, Payload> = {};

			for (let i = 0; i < count; i++) {
				const keyLength = new DataView(
					this.bytes.buffer,
					this.bytes.byteOffset + cursor,
					4,
				).getUint32(0, true);

				const keyStart = cursor + 4;
				const keyEnd = keyStart + keyLength;

				const key = new TextDecoder().decode(
					this.bytes.subarray(keyStart, keyEnd),
				);

				cursor = keyEnd;

				const [item, next] = this.decodeAt(cursor);

				output[key] = item;
				cursor = next;
			}

			return [output, cursor];
		}

		throw new Error("Unknown payload type");
	}
	public setPayload(pid: N, payload: Payload): V {
		const oldPtr = this.payloadPtr[pid];
		const oldLen = this.payloadLen[pid];
	
		if (oldPtr >= 0) {
			this.freePayloadPtr.push(oldPtr);
			this.freePayloadLen.push(oldLen);
		}
	
		const required = 0; // this.measurePayload(payload);
	
		let ptr = -1;
	
		for (let i = this.freePayloadPtr.length - 1; i >= 0; i--) {
			if (this.freePayloadLen[i] >= required) {
				ptr = this.freePayloadPtr[i];
	
				this.freePayloadPtr.splice(i, 1);
				this.freePayloadLen.splice(i, 1);
	
				break;
			}
		}
	
		if (ptr === -1) {
			ptr = this.byteLength;
	
			this.ensureBytes(required);
	
			this.byteLength += required;
		}
	
		const savedByteLength = this.byteLength;
	
		this.byteLength = ptr;
	
		// this.encodeValue(payload);
	
		const len = this.byteLength - ptr;
	
		this.byteLength = savedByteLength;
	
		this.payloadPtr[pid] = ptr;
		this.payloadLen[pid] = len;
	}
	private measurePayload(value: Payload): N {
		const start = this.byteLength;
	
		this.encodeValue(value);
	
		const size = this.byteLength - start;
	
		this.byteLength = start;
	
		return size;
	}
	// ------------------------------------------------------------------
	// Slot allocation
	// ------------------------------------------------------------------
	private alloc(): N {
		if (this.freeTail === 0) {
			throw new Error("FUCK: pid capacity exhausted");
		}

		const pid = this.freePids[--this.freeTail];

		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;

		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;

		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;

		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;

		this.linkHead[pid] = -1;

		return pid;
	}
	// ------------------------------------------------------------------
	// Handler table
	// ------------------------------------------------------------------
	private allocHandler(    handler: SubHandler | StreamSource | StreamHandler | ExecHandler,): N {
		if (this.freeHandlers.length > 0) {
			const offset = this.freeHandlers.pop()!;

			this.handlers[offset] = handler;

			return offset;
		}

		this.handlers.push(handler);

		return this.handlers.length - 1;
	}
	public setExecHandler(pid: N, handler: ExecHandler): N {
		const offset = this.allocHandler(handler);

		this.execPath[pid] = offset;

		return offset;
	}
	// ------------------------------------------------------------------
	// String interning and channel lookup
	// ------------------------------------------------------------------
	private intern(value: string): N {
		let id = this.stringIds.get(value);

		if (id === undefined) {
			id = this.strings.length;

			this.strings.push(value);
			this.stringIds.set(value, id);
		}

		return id;
	}
	private channelHash(r: N, m: N, s: N, k: N): N {
		let hash = 2166136261;

		hash ^= r;
		hash = Math.imul(hash, 16777619);

		hash ^= m;
		hash = Math.imul(hash, 16777619);

		hash ^= s;
		hash = Math.imul(hash, 16777619);

		hash ^= k;
		hash = Math.imul(hash, 16777619);

		return hash & this.hashMask;
	}
	private findChannel(r: N, m: N, s: N, k: N): N {
		let slot = this.channelHash(r, m, s, k);

		while (this.hashSlots[slot] !== -1) {
			const pid = this.hashSlots[slot];

			if (
				this.chanR[pid] === r &&
				this.chanM[pid] === m &&
				this.chanS[pid] === s &&
				this.chanK[pid] === k
			) {
				return pid;
			}

			slot = (slot + 1) & this.hashMask;
		}

		return -1;
	}
	private insertChannel(pid: N, r: N, m: N, s: N, k: N): V {
		let slot = this.channelHash(r, m, s, k);

		while (this.hashSlots[slot] !== -1) {
			slot = (slot + 1) & this.hashMask;
		}

		this.hashSlots[slot] = pid;
	}
	public channel(address: ChannelAddress): N {
		const r = this.intern(address.r);
		const m = this.intern(address.m);
		const s = this.intern(address.s);
		const k = this.intern(address.k);

		const existing = this.findChannel(r, m, s, k);

		if (existing !== -1) {
			return existing;
		}

		const pid = this.alloc();

		this.chanR[pid] = r;
		this.chanM[pid] = m;
		this.chanS[pid] = s;
		this.chanK[pid] = k;

		this.state[pid] = STATE.READY;

		this.insertChannel(pid, r, m, s, k);

		return pid;
	}
	private removeChannel(pid: N): V {
		const r = this.chanR[pid];
		const m = this.chanM[pid];
		const s = this.chanS[pid];
		const k = this.chanK[pid];
	
		let slot = this.channelHash(r, m, s, k);
	
		while (this.hashSlots[slot] !== -1) {
			const candidate = this.hashSlots[slot];
	
			if (
				this.chanR[candidate] === r &&
				this.chanM[candidate] === m &&
				this.chanS[candidate] === s &&
				this.chanK[candidate] === k
			) {
				this.hashSlots[slot] = -1;
	
				// Rehash the cluster after the removed slot.
				let next = (slot + 1) & this.hashMask;
	
				while (this.hashSlots[next] !== -1) {
					const movedPid = this.hashSlots[next];
	
					this.hashSlots[next] = -1;
	
					const movedR = this.chanR[movedPid];
					const movedM = this.chanM[movedPid];
					const movedS = this.chanS[movedPid];
					const movedK = this.chanK[movedPid];
	
					this.insertChannel(
						movedPid,
						movedR,
						movedM,
						movedS,
						movedK,
					);
	
					next = (next + 1) & this.hashMask;
				}
	
				return;
			}
	
			slot = (slot + 1) & this.hashMask;
		}
	}
	// ------------------------------------------------------------------
	// Link slots
	// ------------------------------------------------------------------
	public link(source: N, target: N): V {
		this.admit(source, OP.LINK, source, target, 0);
	}
	public unlink(source: N, target: N): V {
		this.admit(source, OP.UNLINK, source, target, 0);
	}
	public checkDirty(target: N): N {
		const pid = this.alloc();
	
		this.admit(pid, OP.CHECK_DIRTY, target, 0, 0);
	
		return pid;
	}
	public isValidLink(source: N, target: N): N {
		const pid = this.alloc();
	
		this.admit(pid, OP.IS_VALID_LINK, source, target, 0);
	
		return pid;
	}
	public setActiveSub(pid: N): V {
		this.admit(pid, OP.SET_ACTIVE_SUB, 0, 0, pid);
	}
	public getActiveSub(): N {
		const pid = this.alloc();
	
		this.admit(pid, OP.GET_ACTIVE_SUB, 0, 0, 0);
	
		return pid;
	}

	private allocLink(): N {
		if (this.freeLinkTail === 0) {
			throw new Error("FUCK: link capacity exhausted");
		}

		return this.freeLinks[--this.freeLinkTail];
	}
	private freeLink(link: N): V {
		this.linkNext[link] = -1;
		this.linkTarget[link] = 0;
		this.linkOwner[link] = 0;

		this.freeLinks[this.freeLinkTail++] = link;
	}
	public purgeLinks(pid: N): V {
		let current = this.linkHead[pid];

		while (current !== -1) {
			const next = this.linkNext[current];

			this.freeLink(current);

			current = next;
		}

		this.linkHead[pid] = -1;
	}
	public disposeScope(): V {
		for (let lane = 0; lane < this.laneCount; lane++) {
			this.laneRead[lane] = 0;
			this.laneWrite[lane] = 0;
		}
	
		this.pending.clear();
	
		for (let pid = 1; pid < this.capacity; pid++) {
			if (this.state[pid] !== STATE.DISPOSED) {
				this.reclaimPid(pid);
			}
		}
	}
	// ------------------------------------------------------------------
	// Reactive API
	// ------------------------------------------------------------------
	public signal(initial: Payload = undefined): N {
		const pid = this.alloc();
	
		this.kind[pid] = KIND.SIGNAL;
		this.val[pid] = initial as N;
		//this.setPayload(pid, initial);
	
		this.state[pid] = STATE.READY;
	
		this.admit(pid, OP.SIGNAL_OPER);
	
		return pid;
	}
	public get(pid: N): Payload {
		this.admit(pid, OP.READ);
		return this.val[pid] as Payload;
		//return   this.decodePayload(pid);
	}
	public set(pid: N, payload: Payload): V {
	//	this.setPayload(pid, payload);
		this.val[pid] = payload as N;
		this.admit(pid, OP.UPDATE_SIGNAL);
	}
	public effect(run: (pid: N) => void): N {
		const pid = this.alloc();

		this.kind[pid] = KIND.EFFECT;
	
		this.state[pid] = STATE.DIRTY;
	
		this.setExecHandler(pid, run);
	
		this.admit(pid, OP.EFFECT);
	
		return pid;
	}
	public computed(compute: (pid: N) => Payload): N {
	    const pid = this.alloc();

		this.kind[pid] = KIND.COMPUTED;

		this.state[pid] = STATE.DIRTY;

		this.setExecHandler(pid, (computedPid) => {
			const value = compute(computedPid);
			this.val[computedPid] = value as N;
			//this.setPayload(computedPid, value);
		});

		this.admit(pid, OP.COMPUTED);

		return pid;
	}
	// ------------------------------------------------------------------
	// Channel API
	// ------------------------------------------------------------------
	public sub1(address: ChannelAddress, handler: SubHandler): N {
		const pid = this.channel(address);

		const offset = this.allocHandler(handler);

		this.subscriberOffset[pid] = offset;
		this.execPath[pid] = offset;

		this.admit(pid, OP.SUB);

		return pid;
	}
	public sub = (() => {
		const fn = <P = U, T = U>(handler: SubHandler, k: S, s: S = this.s, m: S = this.m, r: S = this.r)=> {	}
		fn.hot = <P = U, T = U>(handler: SubHandler, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V => {
			const pid = 11//this.pid_codec.resolveSingle(k);
			const channel = (this.branch[pid] ??= Object.create(null));
			channel.handler = handler;
		
			return () => {
				if (channel.handler === handler) {
					channel.handler = undefined;
				}
			};
		}
		fn.nested = () => {	};
		fn.nested3 = () => {	};
		return fn;
	})();
	public send = (() => {
		const fn = <T = U >(p: P, k:A) => { }
		fn.hot =<T = U >(p: U, k:A): T | P<T> | UD => {
			const pid = 11//this.pid_codec.resolveSingle(k)
			const channel = this.branch[pid]
			const handler = channel?.handler;
			if (!handler) return undefined;
	  
			return handler(p, { pid, p });
		}
		fn.nested = () => {	};
		fn.nested3 = () => {	};
		return fn;
	})();
	public send1	(p: Payload, k: A, s: A,  r: A, m: A, ): Promise<Payload> {
		const rr = this.intern(r);
		const mm = this.intern(m);
		const ss = this.intern(s);
		const kk = this.intern(k);
	
		const pid = this.findChannel(rr, mm, ss, kk);
	
		if (pid === -1) { return Promise.reject( new Error( `No subscriber for ${rr}/${mm}/${ss}/${kk}`, ), ); }
	
		const sendId = this.sendSeq++;
	
		this.setPayload(pid, p);
	
		return new Promise<Payload>((resolve) => {
			this.pending.set(sendId, resolve);
	
			this.admit(pid, OP.SEND, 0, 0, sendId);
	
			this.PASS();
		});
	}
	public duplex(address: ChannelAddress, handler: SubHandler): N {
		const pid = this.channel(address);

		const offset = this.allocHandler(handler);

		this.subscriberOffset[pid] = offset;
		this.execPath[pid] = offset;

		this.admit(pid, OP.DUPLEX);

		return pid;
	}
	public stream(address: ChannelAddress, source: StreamSource): N {
		const pid = this.channel(address);

		const offset = this.allocHandler(source);

		this.sourceOffset[pid] = offset;

		this.admit(pid, OP.STREAM);

		return pid;
	}
	public onStream(address: ChannelAddress, handler: StreamHandler): N {
		const pid = this.channel(address);

		const offset = this.allocHandler(handler);

		this.streamHandlerOffset[pid] = offset;

		this.admit(pid, OP.DRC);

		return pid;
	}
	public async startStream(address: ChannelAddress): Promise<V> {
		const r = this.intern(address.r);
		const m = this.intern(address.m);
		const s = this.intern(address.s);
		const k = this.intern(address.k);

		const pid = this.findChannel(r, m, s, k);

		if (pid === -1) {
			throw new Error("No stream source for channel");
		}

		const source = this.handlers[
			this.sourceOffset[pid]
		] as StreamSource | null;

		if (!source) {
			throw new Error("Channel has no stream source");
		}

		const iterable = source();

		for await (const chunk of iterable) {
			const handler = this.handlers[
				this.streamHandlerOffset[pid]
			] as StreamHandler | null;

			if (handler) {
				handler(chunk);
			}
		}
	}

}
interface ScopeComponents {	r: N; m: N;	s: N;}
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
class CONVERT_CHANNELS_AND_PIDS {
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
    private stringToId: Record<S, A> = Object.create(null);
    private idToString: string[] = [];
    private addressToPidMap:Record<S, A> = Object.create(null);
    public nextPid = 1;
    // SoA Memory Lanes (Indexed by PID)
    public readonly typeMap: Uint8Array;       // 0 = Single String, 1 = Multi-Channel
    public readonly channelMap: Int32Array;    // r channel
    public readonly subChannelMap: Int32Array; // m channel
    public readonly tripChannelMap: Int32Array;// s channel
    public readonly keyMap: Int32Array;        // k channel / single key string ID
    // Zero-alloc flyweight for multi-channel decoding
    public readonly multiFlyweight = { r: "", m: "", s: "", k: "" };
    constructor(maxPids = 65536) {
        this.typeMap = new Uint8Array(maxPids);
        this.channelMap = new Int32Array(maxPids);
        this.subChannelMap = new Int32Array(maxPids);
        this.tripChannelMap = new Int32Array(maxPids);
        this.keyMap = new Int32Array(maxPids);
    }
    private intern(val: string): number {
		let id = this.stringToId[val];
		if (id === undefined) {
			id = this.idToString.length;
			this.stringToId[val] = id;
			this.idToString[id] = val;
		}
		return id;
	}
	
	public resolveSingle(key: string): number {
		let pid = this.addressToPidMap[key];
		if (pid === undefined) {
			pid = this.nextPid++;
			this.addressToPidMap[key] = pid;
	
			this.typeMap[pid] = 0;
			this.keyMap[pid] = this.intern(key);
		}
		return pid;
	}
	
	public resolveMulti(r: string, m: string, s: string, k = ""): number {
		const addressKey = `${r}:${m}:${s}:${k}`;
		let pid = this.addressToPidMap[addressKey];
		if (pid === undefined) {
			pid = this.nextPid++;
			this.addressToPidMap[addressKey] = pid;
	
			this.typeMap[pid] = 1;
			this.channelMap[pid] = this.intern(r);
			this.subChannelMap[pid] = this.intern(m);
			this.tripChannelMap[pid] = this.intern(s);
			this.keyMap[pid] = this.intern(k);
		}
		return pid;
	}
	
	public resolveTriple(r: string, m: string, s: string): number {
		const addressKey = `${r}:${m}:${s}`;
		let pid = this.addressToPidMap[addressKey];
		if (pid === undefined) {
			pid = this.nextPid++;
			this.addressToPidMap[addressKey] = pid;
	
			this.typeMap[pid] = 1;
			this.channelMap[pid] = this.intern(r);
			this.subChannelMap[pid] = this.intern(m);
			this.tripChannelMap[pid] = this.intern(s);
		}
		return pid;
	}// Decode PID back into 3-4 Channels (zero-alloc flyweight mutation).
    public decodeMulti(pid: number): typeof this.multiFlyweight {
        this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
        this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
        this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
        this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
        return this.multiFlyweight;
    }
}
class BEAM_CODEC {
/*  =============================================================================
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



/** 
 * 
 * 
 * 	PAS1S(): V {
const len = this.laneLen;
const appendedStart = len;

let writeIdx = 0;
let i = 0;
const unrolledLimit = len - (len % 4);

for (; i < unrolledLimit; i += 4) {
	let pid = this.lane[i];

	if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) { this.reclaimPid(pid); }
	else if ((this.ops[pid] & OP_MASK) === OP.READ) { this.activeSub = pid; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		const link = this.freeLinks[--this.freeLinkTail];

		this.linkOwner[link] = target;
		this.linkNext[link] = this.linkHead[source];
		this.linkHead[source] = link;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		let current = this.linkHead[source];
		let previous = -1;

		while (current !== -1) {
			if (this.linkOwner[current] === target) {
				if (previous === -1) {
					this.linkHead[source] = this.linkNext[current];
				} else {
					this.linkNext[previous] = this.linkNext[current];
				}

				this.linkNext[current] = -1;
				this.linkTarget[current] = 0;
				this.linkOwner[current] = 0;

				this.freeLinks[this.freeLinkTail++] = current;

				break;
			}

			previous = current;
			current = this.linkNext[current];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {
		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) { this.val[pid] = this.activeSub; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) { this.activeSub = this.val[pid]; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) { this.batchDepth++; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {
		if (this.batchDepth > 0) {
			this.batchDepth--;
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.CLEANUP) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;

		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;

		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;

		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SEND) {
		const handler = this.handlers[
			this.subscriberOffset[pid]
		] as SubHandler | null;

		if (handler) {
			const payload = this.decodePayload(pid);
			const sendId = this.val[pid];

			const reply = (value: Payload): void => {
				const resolve = this.pending.get(sendId);

				if (resolve) {
					this.pending.delete(sendId);
					resolve(value);
				}
			};

			const returned = handler(payload, reply);

			if (returned !== undefined) {
				reply(returned);
			}
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SUB) { this.state[pid] = STATE.READY; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DUPLEX) { this.state[pid] = STATE.READY; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.STREAM) { this.state[pid] = STATE.READY; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DRC) { this.state[pid] = STATE.READY; this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.TRANSFORM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.CHECK_DIRTY) {
		const target = this.posX[pid] | 0;
	
		this.val[pid] = this.state[target] === STATE.DIRTY ? 1 : 0;
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.IS_VALID_LINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;
	
		let current = this.linkHead[source];
		let valid = 0;
	
		while (current !== -1) {
			if (this.linkOwner[current] === target) {
				valid = 1;
				break;
			}
	
			current = this.linkNext[current];
		}
	
		this.val[pid] = valid;
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED_OPER) {
		this.state[pid] = STATE.RUNNING;
	
		const path = this.execPath[pid];
	
		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;
	
			if (handler) {
				handler(pid);
			}
		}
	
		this.version[pid]++;
		this.state[pid] = STATE.READY;
	
		let link = this.linkHead[pid];
	
		while (link !== -1) {
			const target = this.linkOwner[link];
	
			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;
	
				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}
	
			link = this.linkNext[link];
		}
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SIGNAL_OPER) {
		this.state[pid] = STATE.READY;
		this.version[pid]++;
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT_OPER) {
		this.state[pid] = STATE.RUNNING;
	
		const path = this.execPath[pid];
	
		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;
	
			if (handler) {
				handler(pid);
			}
		}
	
		this.state[pid] = STATE.READY;
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT_SCOPE_OPER) {
		const previousActiveSub = this.activeSub;
	
		this.activeSub = pid;
		this.state[pid] = STATE.RUNNING;
	
		const path = this.execPath[pid];
	
		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;
	
			if (handler) {
				handler(pid);
			}
		}
	
		this.activeSub = previousActiveSub;
		this.state[pid] = STATE.READY;
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.GET_BATCH_DEPTH) {
		this.val[pid] = this.batchDepth;
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UPDATE_SIGNAL) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;
	
		let link = this.linkHead[pid];
	
		while (link !== -1) {
			const target = this.linkOwner[link];
	
			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;
	
				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}
	
			link = this.linkNext[link];
		}
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UPDATE_COMPUTED) {
		this.state[pid] = STATE.RUNNING;
	
		const path = this.execPath[pid];
	
		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;
	
			if (handler) {
				handler(pid);
			}
		}
	
		this.version[pid]++;
		this.state[pid] = STATE.READY;
	
		let link = this.linkHead[pid];
	
		while (link !== -1) {
			const target = this.linkOwner[link];
	
			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;
	
				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}
	
			link = this.linkNext[link];
		}
	
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}

	pid = this.lane[i + 1];

	if (pid === TRACER_PID) {
		this.val[TRACER_PID]++;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) {
		this.reclaimPid(pid);
	}
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {
		this.activeSub = pid;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		const link = this.freeLinks[--this.freeLinkTail];

		this.linkOwner[link] = target;
		this.linkNext[link] = this.linkHead[source];
		this.linkHead[source] = link;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		let current = this.linkHead[source];
		let previous = -1;

		while (current !== -1) {
			if (this.linkOwner[current] === target) {
				if (previous === -1) {
					this.linkHead[source] = this.linkNext[current];
				} else {
					this.linkNext[previous] = this.linkNext[current];
				}

				this.linkNext[current] = -1;
				this.linkTarget[current] = 0;
				this.linkOwner[current] = 0;

				this.freeLinks[this.freeLinkTail++] = current;

				break;
			}

			previous = current;
			current = this.linkNext[current];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {
		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {
		this.val[pid] = this.activeSub;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {
		this.activeSub = this.val[pid];

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {
		this.batchDepth++;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {
		if (this.batchDepth > 0) {
			this.batchDepth--;
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.CLEANUP) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;

		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;

		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;

		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SEND) {
		const handler = this.handlers[
			this.subscriberOffset[pid]
		] as SubHandler | null;

		if (handler) {
			const payload = this.decodePayload(pid);
			const sendId = this.val[pid];

			const reply = (value: Payload): void => {
				const resolve = this.pending.get(sendId);

				if (resolve) {
					this.pending.delete(sendId);
					resolve(value);
				}
			};

			const returned = handler(payload, reply);

			if (returned !== undefined) {
				reply(returned);
			}
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SUB) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DUPLEX) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.STREAM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DRC) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.TRANSFORM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}

	pid = this.lane[i + 2];

	if (pid === TRACER_PID) {
		this.val[TRACER_PID]++;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) {
		this.reclaimPid(pid);
	}
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {
		this.activeSub = pid;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		const link = this.freeLinks[--this.freeLinkTail];

		this.linkOwner[link] = target;
		this.linkNext[link] = this.linkHead[source];
		this.linkHead[source] = link;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		let current = this.linkHead[source];
		let previous = -1;

		while (current !== -1) {
			if (this.linkOwner[current] === target) {
				if (previous === -1) {
					this.linkHead[source] = this.linkNext[current];
				} else {
					this.linkNext[previous] = this.linkNext[current];
				}

				this.linkNext[current] = -1;
				this.linkTarget[current] = 0;
				this.linkOwner[current] = 0;

				this.freeLinks[this.freeLinkTail++] = current;

				break;
			}

			previous = current;
			current = this.linkNext[current];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {
		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {
		this.val[pid] = this.activeSub;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {
		this.activeSub = this.val[pid];

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {
		this.batchDepth++;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {
		if (this.batchDepth > 0) {
			this.batchDepth--;
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.CLEANUP) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;

		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;

		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;

		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SEND) {
		const handler = this.handlers[
			this.subscriberOffset[pid]
		] as SubHandler | null;

		if (handler) {
			const payload = this.decodePayload(pid);
			const sendId = this.val[pid];

			const reply = (value: Payload): void => {
				const resolve = this.pending.get(sendId);

				if (resolve) {
					this.pending.delete(sendId);
					resolve(value);
				}
			};

			const returned = handler(payload, reply);

			if (returned !== undefined) {
				reply(returned);
			}
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SUB) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DUPLEX) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.STREAM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DRC) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.TRANSFORM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}

	pid = this.lane[i + 3];

	if (pid === TRACER_PID) {
		this.val[TRACER_PID]++;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) {
		this.reclaimPid(pid);
	}
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {
		this.activeSub = pid;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		const link = this.freeLinks[--this.freeLinkTail];

		this.linkOwner[link] = target;
		this.linkNext[link] = this.linkHead[source];
		this.linkHead[source] = link;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		let current = this.linkHead[source];
		let previous = -1;

		while (current !== -1) {
			if (this.linkOwner[current] === target) {
				if (previous === -1) {
					this.linkHead[source] = this.linkNext[current];
				} else {
					this.linkNext[previous] = this.linkNext[current];
				}

				this.linkNext[current] = -1;
				this.linkTarget[current] = 0;
				this.linkOwner[current] = 0;

				this.freeLinks[this.freeLinkTail++] = current;

				break;
			}

			previous = current;
			current = this.linkNext[current];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {
		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {
		this.val[pid] = this.activeSub;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {
		this.activeSub = this.val[pid];

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {
		this.batchDepth++;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {
		if (this.batchDepth > 0) {
			this.batchDepth--;
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.CLEANUP) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;

		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;

		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;

		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SEND) {
		const handler = this.handlers[
			this.subscriberOffset[pid]
		] as SubHandler | null;

		if (handler) {
			const payload = this.decodePayload(pid);
			const sendId = this.val[pid];

			const reply = (value: Payload): void => {
				const resolve = this.pending.get(sendId);

				if (resolve) {
					this.pending.delete(sendId);
					resolve(value);
				}
			};

			const returned = handler(payload, reply);

			if (returned !== undefined) {
				reply(returned);
			}
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SUB) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DUPLEX) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.STREAM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DRC) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.TRANSFORM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
}

for (; i < len; i++) {
	const pid = this.lane[i];

	if (pid === TRACER_PID) {
		this.val[TRACER_PID]++;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) {
		this.reclaimPid(pid);
	}
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {
		this.activeSub = pid;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {
		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {
		this.state[pid] = STATE.RUNNING;

		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.version[pid]++;
		this.state[pid] = STATE.READY;

		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		const link = this.freeLinks[--this.freeLinkTail];

		this.linkOwner[link] = target;
		this.linkNext[link] = this.linkHead[source];
		this.linkHead[source] = link;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {
		const source = this.posX[pid] | 0;
		const target = this.posY[pid] | 0;

		let current = this.linkHead[source];
		let previous = -1;

		while (current !== -1) {
			if (this.linkOwner[current] === target) {
				if (previous === -1) {
					this.linkHead[source] = this.linkNext[current];
				} else {
					this.linkNext[previous] = this.linkNext[current];
				}

				this.linkNext[current] = -1;
				this.linkTarget[current] = 0;
				this.linkOwner[current] = 0;

				this.freeLinks[this.freeLinkTail++] = current;

				break;
			}

			previous = current;
			current = this.linkNext[current];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const target = this.linkOwner[link];

			if (this.state[target] === STATE.READY) {
				this.state[target] = STATE.DIRTY;

				if ((this.ops[target] & IN_LANE) === 0) {
					this.admit(target, OP.RUN);
				}
			}

			link = this.linkNext[link];
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {
		const path = this.execPath[pid];

		if (path >= 0) {
			const handler = this.handlers[path] as ExecHandler | null;

			if (handler) {
				handler(pid);
			}
		}

		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {
		this.val[pid] = this.activeSub;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {
		this.activeSub = this.val[pid];

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {
		this.batchDepth++;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {
		if (this.batchDepth > 0) {
			this.batchDepth--;
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.CLEANUP) {
		let link = this.linkHead[pid];

		while (link !== -1) {
			const next = this.linkNext[link];

			this.linkNext[link] = -1;
			this.linkTarget[link] = 0;
			this.linkOwner[link] = 0;

			this.freeLinks[this.freeLinkTail++] = link;

			link = next;
		}

		this.linkHead[pid] = -1;

		this.state[pid] = STATE.EMPTY;
		this.version[pid] = 0;
		this.queued[pid] = 0;

		this.subscriberOffset[pid] = -1;
		this.execPath[pid] = -1;
		this.sourceOffset[pid] = -1;
		this.streamHandlerOffset[pid] = -1;

		this.payloadPtr[pid] = -1;
		this.payloadLen[pid] = 0;

		this.chanR[pid] = -1;
		this.chanM[pid] = -1;
		this.chanS[pid] = -1;
		this.chanK[pid] = -1;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SEND) {
		const handler = this.handlers[
			this.subscriberOffset[pid]
		] as SubHandler | null;

		if (handler) {
			const payload = this.decodePayload(pid);
			const sendId = this.val[pid];

			const reply = (value: Payload): void => {
				const resolve = this.pending.get(sendId);

				if (resolve) {
					this.pending.delete(sendId);
					resolve(value);
				}
			};

			const returned = handler(payload, reply);

			if (returned !== undefined) {
				reply(returned);
			}
		}

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.SUB) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DUPLEX) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.STREAM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.DRC) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else if ((this.ops[pid] & OP_MASK) === OP.TRANSFORM) {
		this.state[pid] = STATE.READY;

		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
	else {
		this.ops[pid] = OP.NONE | IN_LANE;
		this.lane[writeIdx++] = pid;
	}
}

// Preserve pids admitted during this pass.
// They were appended after the original lane region and must survive compaction.
const appendedCount = this.laneLen - appendedStart;

if (appendedCount > 0) {
	for (let j = 0; j < appendedCount; j++) {
		this.lane[writeIdx + j] = this.lane[appendedStart + j];
	}

	writeIdx += appendedCount;
}

this.laneLen = writeIdx;
}

 private admit1(pid: N, op: N): V {
        if ((this.ops[pid] & IN_LANE) === 0) {
            if (this.laneLen < this.capacity) {
                this.lane[this.laneLen++] = pid;
                this.ops[pid] = op | IN_LANE;
            } else {
                this.ops[pid] = op;
            }
        } else {
            this.ops[pid] = op | IN_LANE;
        }
    }
 * PASS1(): V {
const len = this.laneLen;
let writeIdx = 0;
let i = 0;
const unrolledLimit = len - (len % 4);

for (; i < unrolledLimit; i += 4) {
	let pid = this.lane[i];
	if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) { this.reclaimPid(pid); }
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.CLEANUP) {  }
	else { this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }

	pid = this.lane[i + 1];
	if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) { this.reclaimPid(pid); }
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {  }
	else { this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }

	pid = this.lane[i + 2];
	if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) { this.reclaimPid(pid); }
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {  }
	else { this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }

	pid = this.lane[i + 3];
	if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) { this.reclaimPid(pid); }
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {  }
	else { this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
}

for (; i < len; i++) {
	const pid = this.lane[i];
	if (pid === TRACER_PID) { this.val[TRACER_PID]++; this.lane[writeIdx++] = pid; }
	else if ((this.ops[pid] & OP_MASK) === OP.DELETE) { this.reclaimPid(pid); }
	else if ((this.ops[pid] & OP_MASK) === OP.READ) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.WRITE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EFFECT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPACT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.EDIT) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.COMPUTED) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.LINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.UNLINK) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.NOTIFY) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SHALLOW_PROPAGATE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.RUN) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.FLUSH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.DISPOSE_ALL_DEPS_IN_REVERSE) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.GET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.SET_ACTIVE_SUB) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.START_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.END_BATCH) {  }
	else if ((this.ops[pid] & OP_MASK) === OP.PURGE_DEPS) {  }
	else { this.ops[pid] = OP.NONE | IN_LANE; this.lane[writeIdx++] = pid; }
}

this.laneLen = writeIdx;
}

READ(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.READ); }
 WRITE(x:N, y: N, v: N, pid: N): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.WRITE); }
 DELETE(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.DELETE); }
 EFFECT(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.EFFECT); }
 COMPACT(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.COMPACT); }
 NONE(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.NONE); }
 EDIT(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.EDIT); }
 COMPUTED(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.COMPUTED); }
 LINK(x: N, y: N, v: N, pid: Pid): V {  this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.LINK); }
 UNLINK(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.UNLINK); }
 NOTIFY(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.NOTIFY); }
 PROPAGATE(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.PROPAGATE); }
 SHALLOW_PROPAGATE(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.SHALLOW_PROPAGATE); }
 RUN(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.RUN); }
 FLUSH(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.FLUSH); }
 DISPOSE_ALL_DEPS_IN_REVERSE(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.DISPOSE_ALL_DEPS_IN_REVERSE); }
 GET_ACTIVE_SUB(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.GET_ACTIVE_SUB); }
 SET_ACTIVE_SUB(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.SET_ACTIVE_SUB); }
 START_BATCH(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.START_BATCH); }
 END_BATCH(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.END_BATCH); }
 PURGE_DEPS(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.PURGE_DEPS); }
 CLEANUP(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.CLEANUP); }
 SEND(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.SEND); }
 SUB(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.SUB); }
 DUPLEX(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.DUPLEX); }
 STREAM(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.STREAM); }
 DRC(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.DRC); }
 TRANSFORM(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.TRANSFORM); }
 CHECK_DIRTY(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.CHECK_DIRTY); }
 IS_VALID_LINK(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.IS_VALID_LINK); }
 COMPUTED_OPER(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.COMPUTED_OPER); }
 SIGNAL_OPER(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.SIGNAL_OPER); }
 EFFECT_OPER(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.EFFECT_OPER); }
 EFFECT_SCOPE_OPER(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.EFFECT_SCOPE_OPER); }
 GET_BATCH_DEPTH(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.GET_BATCH_DEPTH); }
 UPDATE_SIGNAL(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.UPDATE_SIGNAL); }
 UPDATE_COMPUTED(x: N, y: N, v: N, pid: Pid): V { this.posX[pid] = x; this.posY[pid] = y; this.val[pid] = v; this.admit(pid, OP.UPDATE_COMPUTED); } */
