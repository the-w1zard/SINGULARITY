import Store from '#STR'
import type { ATOM_KEY } from '../contract/CONTROL_ROOM';
import type { ATOM } from '../master_atoms/ATOM';
//                                    REGISTRY_PROTON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

export class REGISTRY_PROTON {
	public root_cell_key: ATOM_KEY; // this is just the root key... not the entire fucking cell
	 
	public r = '_THE_WIZARD_'
	public m: S = "_ATOMS_";
	public s: S =  "_REGISTRY_PROTON_"; 

	constructor(
		root_cell_key: ATOM_KEY,
		 
	) {
		this.root_cell_key = root_cell_key;
 
	}
	public _REGISTRY_PROTON= null
	public _VALUES_REGISTRY_PROTON= null
	public _REACTIVE_PROTON= null
	public _GATE_PROTON= null
	public _SCHEMA_PROTON= null
	public _CONTRACTS_PROTON= null
	public _CONFLICT_NEUTRON= null
	public _COMPRESSION_NEUTRON= null
	public _CONTEXT_NEUTRON= null
	public _ENCRYPTION_NEUTRON= null
	public _GROUPS_NEUTRON= null
	public _HISTORY_NEUTRON= null
	public _PERMISSIONS_NEUTRON= null
	public _META_NEUTRON= null
	public _OFF_SEC_NEUTRON= null
	public _PERSISTENCE_NEUTRON= null
	public _TAGS_NEUTRON= null
	public _SNAPSHOT_NEUTRON= null
	public _PRIORITY_NEUTRON= null
	public _TIME_TRAVEL_NEUTRON= null
	public _TTL_NEUTRON= null
	public _BACKPRESSURE_ELECTRON= null
	public _BEAM_CODEC_ELECTRON= null
	public _BINARY_ELECTRON= null
	public _BYTE_CODE_VM_ELECTRON= null
	public _CHANNEL_ELECTRON= null
	public _CHANNELS_ELECTRON= null
	public _CONCURRENCY_ELECTRON= null
	public _DEADLETTER_ELECTRON= null
	public _DESTRUCTION_ELECTRON= null
	public _ERROR_ELECTRON= null
	public _EVENT_ELECTRON= null
	public _EXTERNAL_SUBSCRIPTION_ELECTRON= null
	public _GATE_ELECTRON= null
	public _GLOBAL_SUBSCRIPTION_ELECTRON= null
	public _HTTP_TRANSPO_ELECTRON= null
	public _INJECTION_CODEC_ELECTRON= null
	public _METRICS_ELECTRON= null
	public _MODULES_ELECTRON= null
	public _REACTIVE_ELECTRON= null
	public _ROUTE_ELECTRON= null
	public _SCOPE_REGISTRY_ELECTRON= null
	public _SEALED_ERROR_ELECTRON= null
	public _SESSION_ELECTRON= null
	public _SHELL_ELECTRON= null
	public _SUBSCRIPTION_ELECTRON= null
	public _WAITER_ELECTRON= null
	public _TRANSFORMS_ELECTRON= null
	public _MIDDLEWARE_ELECTRON= null
	public _ADAPTER_ELECTRON= null
	public _RUN_O_ELECTRON = null

	purgeAll(): void {
		
	}
}
// #endregion
export class ATOM_MAP<K, V> extends Map<K, V> {
	constructor(private readonly label = "atom") {
		super();
	}

	override set(key: K, value: V): this {
		const current = this.get(key);

		if (current !== undefined && current !== value) {
			console.warn(`[${this.label}] "${String(key)}" is already registered. Existing value preserved.`);
			return this;
		}

		return super.set(key, value);
	}
}
/**
 bind(atom: ATOM, owner: OWNERSHIP): void {
	 const ctor = atom.constructor;
	 if (this.slots.has(ctor)) throw new Error(`shard already bound: ${ctor.name}`);

	 this.slots.set(ctor, { atom, owner });
	 BC().ATOM_SET(ctor.name, atom);
 }

 unbind(ctor: ATOM_CLASS<any>): boolean {
	 const slot = this.slots.get(ctor);
	 if (!slot) return false;

	 this.slots.delete(ctor);
	 BC().ATOM_DROP(ctor.name);

	 if (slot.owner === "bespoke") {
		 (slot.atom as { purge?(): V }).purge?.();
	 }
	 return true;
 }

 get<S extends ATOM>(ctor: ATOM_CLASS<S>): S | undefined {
	 return this.slots.get(ctor)?.atom as S | undefined;
 }

 has(ctor: ATOM_CLASS<any>): boolean {
	 return this.slots.has(ctor);
 }
 get size(): S extends never ? never : number {
	 return this.slots.size;
 } */
