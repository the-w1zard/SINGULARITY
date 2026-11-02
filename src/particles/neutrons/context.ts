// neutrons/context.ts

//  ND                                  CONTEXT_NEUTRON
// SPECIALIZING IN THE STORAGE OF PLATFORM CONTEXT / STATE VALUES 
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class CONTEXT_NEUTRON {
	public root_cell_key: ATOM_KEY;
	private store = new Map<S, A>();

	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	public set(key: S, value: A): void {
		this.store.set(key, value);
		// if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('context_set', undefined, undefined, undefined, { key });
	}
	public get(key: S): A | undefined { return this.store.get(key); }
	/** Read with a fallback — context lookups always expect a default. */
	public getOr(key: S, fallback: A): A { return this.store.has(key) ? (this.store.get(key) as A) : fallback; }
	public has(key: S): B { return this.store.has(key); }
	public delete(key: S): B { return this.store.delete(key); }
	public keys(): S[] { return [...this.store.keys()]; }
	/** Full snapshot, for hand-off / persistence / diagnostics. */
	public snapshot(): Record<S, A> { return Object.fromEntries(this.store); }
	/** Restore from a snapshot. Replaces the store wholesale. */
	public restore(snap: Record<S, A>): void { this.store = new Map(Object.entries(snap)); }
	/** Merge a partial snapshot over the current store (existing keys win unless overwrite). */
	public merge(snap: Record<S, A>, overwrite = false): void {
		for (const [k, v] of Object.entries(snap)) {
			if (!overwrite && this.store.has(k)) continue;
			this.store.set(k, v);
		}
	}
	/** Scoped read: return a sub-object of the store whose keys start with the prefix. */
	public scoped(prefix: S): Record<S, A> {
		const out: Record<S, A> = {};
		for (const [k, v] of this.store) if (k.startsWith(prefix)) out[k] = v;
		return out;
	}

	purge(): void { this.store.clear(); }
}
// #endregion
