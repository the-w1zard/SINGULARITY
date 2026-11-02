
import type { ATOM_KEY } from "../contract/CONTROL_ROOM";
import type { ATOM } from "../master_atoms/ATOM";
//                                                                                       BEAM_CONTROLLER
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const MODULE_CACHE = new M2<A, M<A, M<A, A>>>();
export class BEAM_CONTROLLER {
	public r = "_THE_WIZARD_";
	public m = "_CELL_";
	public s = "_ATOM_";
	public registry: BYTE_ATOM_REG;
	public BINARY_CODEC: BINARY_CODEC;
	public INJECTION_CODEC: INJECTION_CODEC;
	public BYTE_REG_CAP: N;

	constructor(reg_cap: N = 8192, BYTE_REG_CAP = 65536) {
		this.registry = new BYTE_ATOM_REG(reg_cap);
		this.BINARY_CODEC = new BINARY_CODEC();
		this.INJECTION_CODEC = new INJECTION_CODEC();
		this.BYTE_REG_CAP = BYTE_REG_CAP;
	}
	// #endregion
	//                                                                                       BEAM_CONTROLLER
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public cache_type: "sauron" | "disruptor" | "cache" = "sauron";
	public store = (() => {
		const fn = <K, V>(s: S = this.s, m: S = this.m, r: S = this.r): M<K, V> => {
			let root;
			switch (this.cache_type) {
				case "disruptor": {
					break;
				}
				case "cache": {
					root = MODULE_CACHE.get(r);
					break;
				}
				case "sauron":
				default: {
					root = (globalThis as A)[r];
					break;
				}
			}
			if (!root) {
				root = new Map<A, Map<A, Map<A, A>>>();
				switch (this.cache_type) {
					case "disruptor": {
						break;
					}
					case "cache": {
						MODULE_CACHE.clear();
						MODULE_CACHE.set(r, root);
						break;
					}
					case "sauron":
					default: {
						(globalThis as A)[r] = root;
						break;
					}
				}
			}

			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new Map<A, Map<A, A>>();
				root.set(m, masterMap);
			}
			let slaveMap = masterMap.get(`${s}_ATOMS`);
			if (!slaveMap) {
				slaveMap = new Map<K, V>();
				masterMap.set(`${s}_ATOMS`, slaveMap);
			}
			return slaveMap;
		};
		fn.particle = (s: S = this.s, m: S = this.m, r: S = this.r) => {
			let root;
			switch (this.cache_type) {
				case "disruptor": {
					break;
				}
				case "cache": {
					root = MODULE_CACHE.get(r);
					break;
				}
				case "sauron":
				default: {
					root = (globalThis as A)[r];
					break;
				}
			}
			if (!root) {
				root = new Map<A, Map<A, Map<A, A>>>();
				switch (this.cache_type) {
					case "disruptor": {
						break;
					}
					case "cache": {
						MODULE_CACHE.clear();
						MODULE_CACHE.set(r, root);
						break;
					}
					case "sauron":
					default: {
						(globalThis as A)[r] = root;
						break;
					}
				}
			}

			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new Map<any, Map<any, any>>();
				root.set(m, masterMap);
			}

			let metaMap = masterMap.get(s);
			if (!metaMap) {
				metaMap = {
					// MASTER ATOMS INCLUDE:
					// ATOM 
					// CLOUD
					// DRC/DRS
					// DUPLEX
					// MODULE
					// S_CLASS
					// SEND
					// SLAB
					// STREAM
					// SPREADSHEET
					// SUB
					// VERTEX
					_REGISTRY_PROTON: null,
					_VALUES_REGISTRY_PROTON: null,
					_REACTIVE_PROTON: null,
					_GATE_PROTON: null,
					_SCHEMA_PROTON: null,
					_CONTRACTS_PROTON: null,

					_CONFLICT_NEUTRON: null,
					_COMPRESSION_NEUTRON: null,
					_CONTEXT_NEUTRON: null,
					_ENCRYPTION_NEUTRON: null,
					_GROUPS_NEUTRON: null,
					_HISTORY_NEUTRON: null,
					_PERMISSIONS_NEUTRON: null,
					_META_NEUTRON: null,
					_OFF_SEC_NEUTRON: null,
					_PERSISTENCE_NEUTRON: null,
					_TAGS_NEUTRON: null,
					_SNAPSHOT_NEUTRON: null,
					_PRIORITY_NEUTRON: null,
					_TIME_TRAVEL_NEUTRON: null,
					_TTL_NEUTRON: null,

					_BACKPRESSURE_ELECTRON: null,
					_BEAM_CODEC_ELECTRON: null,
					_BINARY_ELECTRON: null,
					_BYTE_CODE_VM_ELECTRON: null,
					_CHANNEL_ELECTRON: null,
					_CHANNELS_ELECTRON: null,
					_CONCURRENCY_ELECTRON: null,
					_DEADLETTER_ELECTRON: null,
					_DESTRUCTION_ELECTRON: null,

					_ERROR_ELECTRON: null,
					_EVENT_ELECTRON: null,
					_EXTERNAL_SUBSCRIPTION_ELECTRON: null,
					_GATE_ELECTRON: null,
					_GLOBAL_SUBSCRIPTION_ELECTRON: null,
					_HTTP_TRANSPO_ELECTRON: null,
					_INJECTION_CODEC_ELECTRON: null,
					_METRICS_ELECTRON: null,
					_MODULES_ELECTRON: null,
					_REACTIVE_ELECTRON: null,
					_ROUTE_ELECTRON: null,
					_SCOPE_REGISTRY_ELECTRON: null,
					_SEALED_ERROR_ELECTRON: null,

					_SESSION_ELECTRON: null,
					_SHELL_ELECTRON: null,
					_SUBSCRIPTION_ELECTRON: null,
					_WAITER_ELECTRON: null,

					_TRANSFORMS_ELECTRON: null,
					_MIDDLEWARE_ELECTRON: null,
					_ADAPTER_ELECTRON: null,
					_RUN_ON_ELECTRON: null,
				};
				masterMap.set(s, metaMap);
			}
			return metaMap;
		};
		
		return fn;
	})();
	public set = (() => {
		const fn = <T extends ATOM>(atom: T, s: S = this.s, m: S = this.m, r: S = this.r): T => {
			const atoms = this.store<ATOM_KEY, ATOM>(s, m, r);
			const current = atoms.get(atom.key);

			if (current !== undefined && current !== atom) {
				throw new Error(`master atom already registered: ${String(atom.key)}`);
			}

			atoms.set(atom.key, atom);
			return atom;
		};
		//const slots = this.store.particle(s, m, r);
		fn.particle = (k: ATOM_KEY, shard: ATOM, s: S = this.s, m: S = this.m, r: S = this.r): ATOM => {
			return this.store(s, m, r).set(atom.key, atom);
		};
		return fn;
	})();
	public at = (() => {
		const fn = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): ATOM | UD => {
			return this.store<ATOM_KEY, ATOM>(s, m, r).get(k);
		};
		fn.particle = <SHARD extends ATOM = ATOM>(k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): SHARD | UD => {
			return (this.store.particle(s, m, r)[this.store.slot_key(k)] as SHARD | null) ?? undefined;
		};
		return fn;
	})();
	public get = (() => {
		const fn = () => {};
		fn.particle = <SHARD extends ATOM = ATOM>(k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): SHARD | UD => {
			return this.at.particle<SHARD>(k, s, m, r);
		};
		return fn;
	})();
	public delete = (() => {
		const fn = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): B => {
			return this.store<ATOM_KEY, ATOM>(s, m, r).delete(k);
		};
		fn.particle = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): B => {
			const slots = this.store.particle(s, m, r);
			if (!(k in slots) || slots[k] === null) return false;
			slots[k] = null;
			return true;
		};
		return fn;
	})();
	public purge = (() => {
		const fn = () => {};
		fn.particle = (k: ATOM_KEY, s: S = this.s, m: S = this.m, r: S = this.r): N => {
			const slots = this.store.particle(s, m, r);
			let purged = 0;
			for (const key of Object.keys(slots)) {
				const shard = slots[key];
				if (shard === null) continue;
				(shard as { purge?(): V }).purge?.();
				slots[key] = null;
				purged++;
			}
			return purged;
		};
		return fn;
	})();
}
/**	
 * 
 * 	
 * 
 * private stringToId = new Map<S, N>();
private idToString: S[] = [];
private addressToPidMap = new Map<S, N>();
public nextPid = 1;

// SoA Memory Lanes (Indexed by PID)
public readonly typeMap: Uint8Array; // 0 = Single String, 1 = Multi-Channel
public readonly channelMap: Int32Array; // r channel
public readonly subChannelMap: Int32Array; // m channel
public readonly tripChannelMap: Int32Array; // s channel
public readonly keyMap: Int32Array; // k channel / single key string ID
public readonly particleMetaMap: Int32Array; // meta map

// Zero-alloc flyweight for multi-channel decoding
public readonly multiFlyweight = { r: "", m: "", s: "", k: "" };

this.typeMap = new Uint8Array(BYTE_REG_CAP);
this.particleMetaMap = new Int32Array(BYTE_REG_CAP);
this.channelMap = new Int32Array(BYTE_REG_CAP);
this.subChannelMap = new Int32Array(BYTE_REG_CAP);
this.tripChannelMap = new Int32Array(BYTE_REG_CAP);
this.keyMap = new Int32Array(BYTE_REG_CAP);public byte_store = (() => {
 // Input Style B: Register 3-4 channel strings -> returns a PID.
 const fn = (k: A, s: S = this.s, m: S = this.m, r: S = this.r): N => {
	 const addressKey = `${r}:${m}:${s}:${k}`;
	 let pid = this.addressToPidMap.get(addressKey);
	 if (pid === undefined) {
		 pid = this.nextPid++;
		 this.addressToPidMap.set(addressKey, pid);

		 this.typeMap[pid] = 1; // Mark as Multi-Channel
		 this.particleMetaMap[pid] = 2; // Mark as Multi-Channel
		 this.channelMap[pid] = this.byte_store.intern(r);
		 this.subChannelMap[pid] = this.byte_store.intern(m);
		 this.tripChannelMap[pid] = this.byte_store.intern(s);
		 this.keyMap[pid] = this.byte_store.intern(k);
	 }
	 return pid;
 };
 fn.intern = (val: S): N => {
	 let id = this.stringToId.get(val);
	 if (id === undefined) {
		 id = this.idToString.length;
		 this.stringToId.set(val, id);
		 this.idToString[id] = val;
	 }
	 return id;
 };
 fn.resolveSingle = (key: S): N => {
	 let pid = this.addressToPidMap.get(key);
	 if (pid === undefined) {
		 pid = this.nextPid++;
		 this.addressToPidMap.set(key, pid);

		 this.typeMap[pid] = 0; // Mark as Single String
		 this.keyMap[pid] = this.byte_store.intern(key);
	 }
	 return pid;
 };
 fn.decodeMulti = (pid: N): typeof this.multiFlyweight => {
	 this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
	 this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
	 this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
	 this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
	 return this.multiFlyweight;
 };
 fn.decodeSingle = (pid: N): S => {
	 return this.idToString[this.keyMap[pid]] ?? "";
 };
 fn.particle = (s: S = this.s, m: S = this.m, r: S = this.r) => {
	 const rId = fn.intern(r);
	 const mId = fn.intern(m);
	 const sId = fn.intern(s);
	 const addressKey = `${rId}:${mId}:${sId}_PARTICLE`;

	 let pid = this.addressToPidMap.get(addressKey);
	 if (pid === undefined) {
		 pid = this.nextPid++;
		 this.addressToPidMap.set(addressKey, pid);

		 if (!this.particleMetaMap) {
			 this.particleMetaMap = [];
		 }

		 this.particleMetaMap[pid] = {
			 _REGISTRY_PROTON: null,
			 _VALUES_REGISTRY_PROTON: null,
			 _REACTIVE_PROTON: null,
			 _GATE_PROTON: null,
			 _SCHEMA_PROTON: null,
			 _CONTRACTS_PROTON: null,

			 _CONFLICT_NEUTRON: null,
			 _COMPRESSION_NEUTRON: null,
			 _CONTEXT_NEUTRON: null,
			 _ENCRYPTION_NEUTRON: null,
			 _GROUPS_NEUTRON: null,
			 _HISTORY_NEUTRON: null,
			 _PERMISSIONS_NEUTRON: null,
			 _META_NEUTRON: null,
			 _OFF_SEC_NEUTRON: null,
			 _PERSISTENCE_NEUTRON: null,
			 _TAGS_NEUTRON: null,
			 _SNAPSHOT_NEUTRON: null,
			 _PRIORITY_NEUTRON: null,
			 _TIME_TRAVEL_NEUTRON: null,
			 _TTL_NEUTRON: null,

			 _BACKPRESSURE_ELECTRON: null,
			 _BEAM_CODEC_ELECTRON: null,
			 _BINARY_ELECTRON: null,
			 _BYTE_CODE_VM_ELECTRON: null,
			 _CHANNEL_ELECTRON: null,
			 _CHANNELS_ELECTRON: null,
			 _CONCURRENCY_ELECTRON: null,
			 _DEADLETTER_ELECTRON: null,
			 _DESTRUCTION_ELECTRON: null,
			 _ERROR_ELECTRON: null,
			 _EVENT_ELECTRON: null,
			 _EXTERNAL_SUBSCRIPTION_ELECTRON: null,
			 _GATE_ELECTRON: null,
			 _GLOBAL_SUBSCRIPTION_ELECTRON: null,
			 _HTTP_TRANSPO_ELECTRON: null,
			 _INJECTION_CODEC_ELECTRON: null,
			 _METRICS_ELECTRON: null,
			 _MODULES_ELECTRON: null,
			 _REACTIVE_ELECTRON: null,
			 _ROUTE_ELECTRON: null,
			 _SCOPE_REGISTRY_ELECTRON: null,
			 _SEALED_ERROR_ELECTRON: null,
			 _SESSION_ELECTRON: null,
			 _SHELL_ELECTRON: null,
			 _SUBSCRIPTION_ELECTRON: null,
			 _WAITER_ELECTRON: null,
			 _TRANSFORMS_ELECTRON: null,
			 _MIDDLEWARE_ELECTRON: null,
			 _ADAPTER_ELECTRON: null,
			 _RUN_ON_ELECTRON: null,
		 };
	 }
	 return this.particleMetaMap[pid];
 };
 return fn;
})(); */

let shard_mgr: BEAM_CONTROLLER;

export function BC(): BEAM_CONTROLLER {
	return (shard_mgr ??= new BEAM_CONTROLLER()) as BEAM_CONTROLLER;
}
// #endregion
//                                                                                  DENSE_RING_COMPACTOR
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class DENSE_RING_COMPACTOR {
	/*  ==========================================================================================================================
	 tracks dirty particles using a bitmask, allowing 32 particles to be checked in a single operation. compact() efficiently
	 extracts only active particle IDs into a dense array, resets the bitmask, and returns the active count for processing.
	==================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DENSE_RING_COMPACTOR] */
	readonly dirtyBitmask: U32;
	readonly activeIndices: U32;
	activeCount = 0;
	constructor(readonly maxParticles: N) {
		this.dirtyBitmask = new Uint32Array(Math.ceil(maxParticles / 32));
		this.activeIndices = new Uint32Array(maxParticles);
	}
	markDirty(pid: N): V {
		this.dirtyBitmask[pid >> 5] |= (1 << (pid & 31)) >>> 0;
	}
	compact(): N {
		this.activeCount = 0;
		const len = this.dirtyBitmask.length;
		for (let i = 0; i < len; i++) {
			let mask = this.dirtyBitmask[i];
			if (mask === 0) continue; // 32 cold particles skipped in 1 check
			const basePid = i << 5;
			while (mask !== 0) {
				const bitIdx = 31 - Math.clz32(mask & -mask);
				this.activeIndices[this.activeCount++] = basePid + bitIdx;
				mask &= mask - 1;
			}
			this.dirtyBitmask[i] = 0;
		}
		return this.activeCount;
	}
}
// #endregion
//                                                                                       BYTE_CODE_CLOCK
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class BYTE_CODE_CLOCK {
	/*  ==========================================================================================================================
	 maintains a lightweight 8-bit sequence counter, incrementing an epoch whenever the sequence wraps around. 
	 hasCompletedCycle() uses signed 8-bit arithmetic to detect whether the sequence has advanced since a starting point.
	========================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BYTE_CODE_CLOCK] */
	readonly register = new Uint8Array(4);
	tick(): V {
		const seq = (this.register[3] + 1) & 0xff;
		this.register[3] = seq;
		if (seq === 0) this.register[1] = (this.register[1] + 1) & 0xff;
	}
	hasCompletedCycle(startSeq: N, currentSeq: N): B {
		return ((currentSeq - startSeq) << 24) >> 24 > 0;
	}
	get seq(): N {
		return this.register[3];
	}
	get epoch(): N {
		return this.register[1];
	}
}
// #endregion
//                                                                                             BYTE_MAP
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class BYTE_MAP {
	/*  ==========================================================================================================================
	 provides O(1) particle-ID-to-slot lookups using a typed array, with -1 indicating an unmapped particle. It supports 
	 constant-time assignment, retrieval, and deletion without searching a registry.
	======================================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DIRECT_SLOT_INDEX] */
	readonly pidToSlot: I32;
	constructor(maxParticles: N) {
		this.pidToSlot = new Int32Array(maxParticles).fill(-1);
	}
	set(pid: N, slot: N): V {
		this.pidToSlot[pid] = slot;
	}
	get(pid: N): N {
		return this.pidToSlot[pid];
	}
	delete(pid: N): V {
		this.pidToSlot[pid] = -1;
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
//                                                                                         BYTE_ATOM_REG
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const FLYWEIGHT_SCOPE: ScopeComponents = { r: 0, m: 0, s: 0 };
export class BYTE_ATOM_REG {
	/*  ==========================================================================================================================
	 maps single strings or multi-channel routing paths to compact numeric PIDs using typed arrays for high-performance, 
	 zero-allocation messaging. It interns strings to enable ultra-fast encoding and decoding on performance-critical 
	 execution paths.
	========================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BYTE_ATOM_REG] */
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
	constructor(maxPids = 65536) {
		this.typeMap = new Uint8Array(maxPids);
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
		this.keyMap = new Int32Array(maxPids);
	}
	private intern(val: string): number {
		let id = this.stringToId.get(val);
		if (id === undefined) {
			id = this.idToString.length;
			this.stringToId.set(val, id);
			this.idToString[id] = val;
		}
		return id;
	} // Input Style A: Register a SINGLE string key -> returns a PID.
	public resolveSingle(key: string): number {
		let pid = this.addressToPidMap.get(key);
		if (pid === undefined) {
			pid = this.nextPid++;
			this.addressToPidMap.set(key, pid);

			this.typeMap[pid] = 0; // Mark as Single String
			this.keyMap[pid] = this.intern(key);
		}
		return pid;
	} // Input Style B: Register 3-4 channel strings -> returns a PID.
	public resolveMulti(r: string, m: string, s: string, k = ""): number {
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
	} // Decode PID back into a Single String (if registered that way).
	public decodeSingle(pid: number): string {
		return this.idToString[this.keyMap[pid]] ?? "";
	} // Decode PID back into 3-4 Channels (zero-alloc flyweight mutation).
	public decodeMulti(pid: number): typeof this.multiFlyweight {
		this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
		this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
		this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
		this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
		return this.multiFlyweight;
	}
}
// #endregion
//                                                                                          BINARY_CODEC
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const TAG = { UNDEFINED: 0, NULL: 1, FALSE: 2, TRUE: 3, INT32: 4, FLOAT64: 5, STRING: 6, ARRAY: 7, OBJECT: 8, UINT8ARRAY: 9, REF: 10 } as const;
class BINARY_CODEC {
	/*  ==========================================================================================================================
	 Encodes ordinary JavaScript values into a collider-owned byte slab. 
	============================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BINARY_CODEC] */
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
// #endregion
//                                                                                       INJECTION_CODEC
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
class INJECTION_CODEC {
	/*  ==========================================================================================================================
	 Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested Map trees with direct PID-indexed arrays.
	========================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [INJECTION_CODEC] */
	private ids = new Map<S, N>();
	private names: S[] = [];
	private pids = new Map<S, N>(); // "a:b:c:d" → pid
	private addresses = new Map<N, [N, N, N, N]>(); // pid → [a,b,c,d]
	private nextId = 0;
	public readonly maxPids: N;
	public readonly listeners: Array<ListenerFn | UD>;
	public nextAllocatedPid = 1; // 0 = TRACER_PID
	public readonly channelMap: I32;
	public readonly subChannelMap: I32;
	public readonly tripChannelMap: I32;
	public readonly dirtyPids: I32;
	public dirtyCount = 0;
	public _flushScheduled = false;

	constructor(maxPids = 8192) {
		this.maxPids = maxPids;
		this.listeners = new Array(maxPids);
		this.dirtyPids = new Int32Array(maxPids);
		this.channelMap = new Int32Array(maxPids);
		this.subChannelMap = new Int32Array(maxPids);
		this.tripChannelMap = new Int32Array(maxPids);
	}
	private intern(value: S): N {
		let id = this.ids.get(value);
		if (id === undefined) {
			id = this.nextId++;
			this.ids.set(value, id);
			this.names[id] = value;
		}
		return id;
	} /** Exact identity: encode address → pid (interned, no collisions). */
	public encode(pid_key: S, sl: S, ms: S, rt: S): N {
		const a = this.intern(rt);
		const b = this.intern(ms);
		const c = this.intern(sl);
		const d = this.intern(pid_key);
		const addressKey = `${a}:${b}:${c}:${d}`;

		let pid = this.pids.get(addressKey);
		if (pid === undefined) {
			pid = this.nextAllocatedPid++;
			if (pid >= this.maxPids) throw new Error("INJECTION_CODEC: PID capacity exceeded");
			this.pids.set(addressKey, pid);
			this.addresses.set(pid, [a, b, c, d]);
			// populate SoA lanes from the interned ids (no re-hash)
			this.channelMap[pid] = a;
			this.subChannelMap[pid] = b;
			this.tripChannelMap[pid] = c;
		}
		return pid;
	} /** Exact decode: pid → address. */
	public decode(pid: N): { key: S; sl: S; ms: S; rt: S } {
		const addr = this.addresses.get(pid);
		if (!addr) throw new Error(`INJECTION_CODEC: unknown pid ${pid}`);
		const [a, b, c, d] = addr;
		return { rt: this.names[a], ms: this.names[b], sl: this.names[c], key: this.names[d] };
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
}
// #endregion
//                                                                                                types
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
interface ScopeComponents {
	r: N;
	m: N;
	s: N;
}
type Pid = N;
interface PARTICLE_QUEUE {
	/** enqueue a particle for the matching tier; returns a drain promise */
	submit(pid: Pid, tierName: S, work?: (p: PARTICLE) => U): P<U>;
	/** drain pass: run all queued work in tiered concurrent batches */
	drain(): P<V>;
	readonly pending: N;
}
interface PARTICLE {
	pid: Pid;
	/** The value. Nothing outside of the collider holds this. */
	p: U;
	/** Queued op to be executed when this particle passes the send pass. */
	op: N;
	/** Edit fn carried with the particle (OP.EDIT), executed in the pass. */
	edit?: (p: A) => A;
}

// #endregion
