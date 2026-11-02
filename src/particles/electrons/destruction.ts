// electrons/destruction.ts

//  ND                                  DESTRUCTION_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface DESTRUCTION_RECORD {
	key: ATOM_KEY;
	cause: S;
	at: N;
	reviver?: () => A; // if provided, the cell can be resurrected with this value
}

export class DESTRUCTION_ELECTRON {
	public root_cell: ATOM_KEY;
	public beforeDestruct: ((key: ATOM_KEY) => void)[] = [];
	public afterDestruct: ((record: DESTRUCTION_RECORD) => void)[] = [];
	public records: DESTRUCTION_RECORD[] = [];
	public destroyed: B = false;
	public blocked: B = false;

	constructor(root_cell: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell = root_cell;
	}

	// ── hooks ──────────────────────────────────────────────────────────────────────────────────
	public onBefore(fn: (key: ATOM_KEY) => void): () => void {
		this.beforeDestruct.push(fn);
		return () => (this.beforeDestruct = this.beforeDestruct.filter(f => f !== fn));
	}
	public onAfter(fn: (record: DESTRUCTION_RECORD) => void): () => void {
		this.afterDestruct.push(fn);
		return () => (this.afterDestruct = this.afterDestruct.filter(f => f !== fn));
	}

	/** Veto a pending destruction (e.g. in a beforeDestruct hook, or upfront). */
	public block(): void { this.blocked = true; }
	public unblock(): void { this.blocked = false; }

	// ── the kill switch ────────────────────────────────────────────────────────────────────────
	/**
	 * Runs the full destruction sequence.
	 * Returns the record on success, null when blocked or already destroyed.
	 * `reviver` is a factory the record keeps so the cell can be rebuilt later.
	 */
	public destroy(cause = 'unknown', reviver?: () => A): DESTRUCTION_RECORD | null {
		if (this.destroyed) return null;
		if (this.blocked) return null;
		const record: DESTRUCTION_RECORD = { key: this.root_cell, cause, at: Date.now(), reviver };

		for (const fn of this.beforeDestruct) {
			try { fn(this.root_cell); } catch (e) { console.error('[destruction-shard] beforeDestruct hook failed:', e); }
		}

		this.destroyed = true;
		this.records.push(record);

		for (const fn of this.afterDestruct) {
			try { fn(record); } catch (e) { console.error('[destruction-shard] afterDestruct hook failed:', e); }
		}
		return record;
	}

	public resurrect(): A | null {
		if (!this.destroyed) return null;
		const last = this.records[this.records.length - 1];
		this.destroyed = false;
		this.blocked = false;
		return last?.reviver?.() ?? null;
	}

	public isDestroyed(): B { return this.destroyed; }
	public clearRecords(): void { this.records.length = 0; }

	purge(): void {
		this.beforeDestruct.length = 0;
		this.afterDestruct.length = 0;
		this.records.length = 0;
	}
}
// #endregion
