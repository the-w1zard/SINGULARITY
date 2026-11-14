// neutrons/snapshot.ts
//                                    SNAPSHOT_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface SNAPSHOT_ENTRY<W = A> {
	version: N;
	value: W;
	ts: N;
	label?: S;
}

export class SNAPSHOT<W = A> {
	public version = 0;
	public history: SNAPSHOT_ENTRY<W>[] = [];
	public historyMax = 128;
	public undoStack: SNAPSHOT_ENTRY<W>[] = [];
	public redoStack: SNAPSHOT_ENTRY<W>[] = [];
	public waiters: Set<(value: W) => void> = new Set();

	constructor( config: { historyMax?: N } = {}) {
		if (config.historyMax !== undefined) this.historyMax = config.historyMax;
	}

	/** Capture the current state. Call this from ATOM.write() after a successful write. */
	public capture(value: W, label?: S): SNAPSHOT_ENTRY<W> {
		this.version++;
		const entry: SNAPSHOT_ENTRY<W> = { version: this.version, value, ts: Date.now(), label };
		this.history.push(entry);
		if (this.history.length > this.historyMax) this.history.splice(0, this.history.length - this.historyMax);
		this.redoStack.length = 0; // new capture kills the redo timeline
		for (const resolve of this.waiters) resolve(value);
		this.waiters.clear();
		return entry;
	}

	/** Latest snapshot, or undefined if none. */
	public latest(): SNAPSHOT_ENTRY<W> | undefined { return this.history[this.history.length - 1]; }
	/** Fetch a snapshot by exact version number. */
	public byVersion(v: N): SNAPSHOT_ENTRY<W> | undefined { return this.history.find(e => e.version === v); }
	/** Fetch the last N snapshots, oldest first. */
	public recent(n = 3): SNAPSHOT_ENTRY<W>[] { return this.history.slice(-n); }
	/** All snapshots since a timestamp. */
	public since(ts: N): SNAPSHOT_ENTRY<W>[] { return this.history.filter(e => e.ts >= ts); }
	public count(): N { return this.history.length; }

	/** Wait for the next capture (first one if none exist yet). */
	public waitForNext(timeoutMs = 0): Promise<W> {
		const latest = this.latest();
		if (latest) return Promise.resolve(latest.value);
		return new Promise<W>((resolve, reject) => {
			const waiter = { resolve: (v: W) => resolve(v), reject };
			this.waiters.add(waiter as (value: W) => void);
			if (timeoutMs > 0) {
				setTimeout(() => { if (this.waiters.delete(waiter)) reject(new Error('ATOM snapshot wait timed out')); }, timeoutMs);
			}
		});
	}

	/** Move an entry from history into the undo stack (used by TIME_TRAVEL). */
	public pushUndo(entry: SNAPSHOT_ENTRY<W>): void {
		this.undoStack.push(entry);
		if (this.undoStack.length > this.historyMax) this.undoStack.splice(0, this.undoStack.length - this.historyMax);
	}
	/** Move an entry from history into the redo stack (used by TIME_TRAVEL). */
	public pushRedo(entry: SNAPSHOT_ENTRY<W>): void {
		this.redoStack.push(entry);
		if (this.redoStack.length > this.historyMax) this.redoStack.splice(0, this.redoStack.length - this.historyMax);
	}

	purge(): void {
		this.history.length = 0;
		this.undoStack.length = 0;
		this.redoStack.length = 0;
		this.waiters.clear();
		this.version = 0;
	}
}
// #endregion
