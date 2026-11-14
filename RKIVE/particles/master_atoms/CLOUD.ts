//                                                                                                  ATOM

import type { REGISTRY_PROTON } from "../protons/registry.js";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class CLOUD_ATOM<X = U> {
	public key: ATOM_KEY;
	public value: X;
	public registry: REGISTRY_PROTON | null = null;
	constructor(key: ATOM_KEY, value: X) {
		this.key = key;
		this.value = value;
	} /** Binds a shard. The registry shard is created here, the first time it is needed. */
	bind<S extends ATOM>(shard: S, owner: OWNERSHIP = "bespoke"): S {
		if (this.registry === null) {
			this.registry = new REGISTRY_PROTON(this.key, { shards: [{ shard, owner }] });
		} else {
			this.registry.bind(shard, owner);
		}
		return shard;
	}
	shard<S extends ATOM>(ctor: ATOM_CLASS<S>): S | UD {
		return this.registry?.get(ctor);
	}
	unbind(ctor: SHARD_CLASS<any>): B {
		if (this.registry === null) return false;
		const removed = this.registry.unbind(ctor);
		// A registry with nothing in it is dropped, returning the cell to the bare fast path.
		if (this.registry.size === 0) {
			BC().store.particle(s, m, r).delete(this.key)._REGISTRY_PROTON;
			this.registry = null;
		}
		return removed;
	}/** Deletes the cell's bespoke shards. Shared shards are untouched. */
	destroy(): V {
		this.registry?.purgeAll();
		this.registry = null;
		BC().store.particle(s, m, r).delete(this.key)._REGISTRY_PROTON;
	}
	read(): X {
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
		const subs =
			(this.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
			this.bind(new SUBSCRIPTION_ELECTRON<X>(this));
		return subs.subscribe(listener);
	}
	waitFor() { }
}
export interface ATOM {
	readonly kind: SY;
	purge?(): V;/** Called when the shard is purged (cell destroyed while bespoke, or unbound). */
}
// #endregion
