// electrons/external_subscription.ts


//                                    EXTERNAL_SUBSCRIPTION_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class EXTERNAL_SUBSCRIPTION_ELECTRON<W = unknown> {
	public root_cell_key: ATOM_KEY;
	private externalSubscribers = new Set<LISTENER<W>>();
	constructor(root_cell_key: ATOM_KEY, _config: Record<string, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
	subscribe(listener: LISTENER<W>): UNSUBSCRIBE {
		this.externalSubscribers.add(listener);
		return () => {
			this.externalSubscribers.delete(listener);
		};
	}
	notify(change: CHANGE<W>): void {
		for (const listener of this.externalSubscribers) {
			try {
				listener(change);
			} catch (error) {
				console.error(`[soul-shard] listener failed for "${String(change.key)}":`, error);
			}
		}
	}
	get size(): number {
		return this.externalSubscribers.size;
	}
	purge(): void {
		this.externalSubscribers.clear();
	}
}
// #endregion
