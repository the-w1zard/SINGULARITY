// electrons/global_subscription.ts


//                                    GLOBAL_SUBSCRIPTION_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class GLOBAL_SUBSCRIPTION_ELECTRON<W = unknown> {
	public root_cell_key: ATOM_KEY;
	private globalListeners = new Set<LISTENER<W>>();
	constructor(root_cell_key: ATOM_KEY, _config: Record<string, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
	subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
		this.globalListeners.add(listener);
		return () => {
			this.globalListeners.delete(listener);
		};
	}
	notify(change: CHANGE<W>): void {
		for (const listener of this.globalListeners) {
			try {
				listener(change);
			} catch (error) {
				console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
			}
		}
	}
	get size(): number {
		return this.globalListeners.size;
	}
	purge(): void {
		this.globalListeners.clear();
	}
}
// #endregion
