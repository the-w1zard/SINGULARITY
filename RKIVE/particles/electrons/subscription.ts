// electrons/subscription.ts


//                                    SUBSCRIPTION_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class SUBSCRIPTION<W = unknown> {
	public root_cell_key: ATOM_KEY;
	private listeners = new Set<LISTENER<W>>();
	constructor(root_cell_key: ATOM_KEY, _config: Record<string, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
	subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
		this.listeners.add(listener);
		return () => {
			this.listeners.delete(listener);
		};
	}
	notify(change: CHANGE<W>): void {
		for (const listener of this.listeners) {
			try {
				listener(change);
			} catch (error) {
				console.error(`[soul-ELECTRON] listener failed for "${String(change.key)}":`, error);
			}
		}
	}
	get size(): number {
		return this.listeners.size;
	}
	purge(): void {
		this.listeners.clear();
	}
}
// #endregion
