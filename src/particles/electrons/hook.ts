// electrons/hook.ts
//  ND                                  HOOK_ELECTRON
// SPECIALIZING IN THE STORAGE OF HOOK VALUES THAT WOULD OTHERWISE USE SAURON IN THE CONTEXT OF THE_WIZARD ANYWAYS
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface HOOK_ENTRY { value: A; teardown?: () => void; registeredAt: N; }

export class HOOK_ELECTRON {
	public root_cell_key: ATOM_KEY;
	private hooks = new Map<S, HOOK_ENTRY>();
	private factories = new Map<S, () => A>();

	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	/** Register a hook factory. The factory runs once, lazily, on first read. */
	public register(name: S, factory: () => A): void {
		this.factories.set(name, factory);
		// if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('hook_registered', undefined, undefined, undefined, { name });
	}
	/** Read the hook's value, computing it on first access. */
	public use(name: S): A {
		const existing = this.hooks.get(name);
		if (existing) return existing.value;
		const factory = this.factories.get(name);
		if (!factory) throw new Error(`[hook-ELECTRON] no hook registered as "${name}"`);
		const value = factory();
		this.hooks.set(name, { value, registeredAt: Date.now() });
		//if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('hook_computed', undefined, undefined, undefined, { name });
		return value;
	}
	public isComputed(name: S): B { return this.hooks.has(name); }
	/** Drop the memoized value; the next use() re-runs the factory. Returns the old teardown result. */
	public reset(name: S): B {
		const entry = this.hooks.get(name);
		if (!entry) return false;
		this.hooks.delete(name);
		try { entry.teardown?.(); } catch (e) { console.error(`[hook-ELECTRON] teardown failed for "${name}":`, e); }
		return true;
	}
	/** Drop the factory and the value. The hook is gone until re-registered. */
	public unregister(name: S): void { this.reset(name); this.factories.delete(name); }
	public list(): S[] { return [...this.factories.keys()]; }

	purge(): void {
		for (const entry of this.hooks.values()) { try { entry.teardown?.(); } catch (e) { console.error('[hook-ELECTRON] teardown failed during purge:', e); } }
		this.hooks.clear();
		this.factories.clear();
	}
}
// #endregion
