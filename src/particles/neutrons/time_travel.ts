// neutrons/time_travel.ts
//  ND                                   TIME_TRAVEL_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

export class TIME_TRAVEL_NEUTRON<W = A> {
	public root_cell: ATOM_KEY;
	public snapshots: SNAPSHOT_SHARD<W> | null = null;
	public version = 0;
	public historyIndex = -1;
	public undoStack: SNAPSHOT_ENTRY<W>[] = [];
	public initial?: W;
	_time_travel_reverting = false;
	private autoTimers: Set<ReturnType<typeof setInterval>> = new Set();

	constructor(root_cell: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell = root_cell;
	}

	/** Bind to the snapshot shard of the same cell. Call once after both shards are bound. */
	public attach(snapshots: SNAPSHOT_SHARD<W>): void {
		this.snapshots = snapshots;
		this.version = snapshots.version;
		this.historyIndex = snapshots.history.length - 1;
		this.initial = snapshots.history[0]?.value;
	}

	/** Write a value back to the cell and record it as a proper version (fires subs like a normal write). */
	private replay = (entry: SNAPSHOT_ENTRY<W>): void => {
		this._time_travel_reverting = true;
		try {
			this._writeThrough?.(entry.value, entry.label ? `time-travel:${entry.label}` : 'time-travel');
		} finally {
			this._time_travel_reverting = false;
		}
	};

	/** Injected by ATOM when the shard is bound — how restores become real writes. */
	public _writeThrough?: (value: W, cause?: S) => void;

	private _current(): SNAPSHOT_ENTRY<W> | undefined {
		return this.snapshots?.history[this.historyIndex];
	}

	public back(): B {
		if (!this.snapshots || this.historyIndex <= 0) return false;
		this.historyIndex--;
		const entry = this._current();
		if (!entry) return false;
		// the entry we stepped away from goes on redo
		const departed = this.snapshots.history[this.historyIndex + 1];
		if (departed) this.snapshots.pushRedo(departed);
		this.replay(entry);
		return true;
	}

	public forward(): B {
		if (!this.snapshots) return false;
		const nextEntry = this.snapshots.history[this.historyIndex + 1];
		if (!nextEntry) return false;
		this.historyIndex++;
		this.replay(nextEntry);
		return true;
	}

	public goto(version: N): B {
		if (!this.snapshots) return false;
		const target = this.snapshots.byVersion(version);
		if (!target) return false;
		const idx = this.snapshots.history.indexOf(target);
		if (idx < 0) return false;
		const departed = this._current();
		if (departed && idx < this.historyIndex) this.snapshots.pushRedo(departed);
		this.historyIndex = idx;
		this.replay(target);
		return true;
	}

	public auto_back(time = 2500): void {
		const t = setInterval(() => { if (!this.back()) clearInterval(t); }, time);
		this.autoTimers.add(t);
	}

	public auto_forward(time = 2500): void {
		const t = setInterval(() => { if (!this.forward()) clearInterval(t); }, time);
		this.autoTimers.add(t);
	}

	/** Graft a foreign snapshot sequence onto this timeline (branch merge). */
	public graft(entries: SNAPSHOT_ENTRY<W>[]): N {
		if (!this.snapshots) return 0;
		let grafted = 0;
		for (const e of entries) {
			this.snapshots.history.push({ version: ++this.snapshots.version, value: e.value, ts: e.ts ?? Date.now(), label: e.label ? `grafted:${e.label}` : 'grafted' });
			grafted++;
		}
		this.historyIndex = this.snapshots.history.length - 1;
		return grafted;
	}

	/** Stop all auto walkers. */
	public stopAuto(): void {
		for (const t of this.autoTimers) clearInterval(t);
		this.autoTimers.clear();
	}

	/** True while replay() is writing — ATOM.write() checks this to avoid re-capturing. */
	public get isReverting(): B { return this._time_travel_reverting; }

	purge(): void {
		this.stopAuto();
		this.snapshots = null;
		this.undoStack.length = 0;
		this.historyIndex = -1;
	}
}
// #endregion
