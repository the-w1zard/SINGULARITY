// electrons/gate.ts
// GATE_SHARD owns the real sealed/frozen/sleep state. META_SHARD's _sealed/_frozen mirrors
// and markSealed/markFrozen/etc are REMOVED — diagnostics query the gate directly.
// In META_SHARD, delete these members and replace the mirror accessors with:
//                                    GATE_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════

export class GATE_ELECTRON {
	public root_cell_key: ATOM_KEY;
	public sealed: B;
	public frozen: B;
	public sleep?: N;
	constructor(root_cell_key: ATOM_KEY, config: { sealed?: B; frozen?: B; sleep?: N } = {}) {
		this.root_cell_key = root_cell_key;
		this.sealed = config.sealed ?? false;
		this.frozen = config.frozen ?? false;
		this.sleep = config.sleep;
	}
	public get asleep(): B {
		return this.sleep !== undefined && Date.now() < this.sleep;
	}
	public seal(): V {
		this.sealed = true;
	}
	public unseal(): V {
		this.sealed = false;
	}
	public freeze(): V {
		this.frozen = true;
	}
	public unfreeze(): V {
		this.frozen = false;
	}
	public sleepUntil(timestamp: N): V {
		this.sleep = timestamp;
	}
	public wake(): V {
		this.sleep = undefined;
	}
	public canRead(): B {
		return !this.asleep;
	}
	public canWrite(): B {
		return !this.sealed && !this.frozen && !this.asleep;
	}
	
}
// #endregion
export interface GATE_ACCESS {
	/** Reads the cell's GATE_SHARD if one is bound. Undefined = bare fast path = always open. */
	gateOf(cell: ATOM | null | UD): GATE_ELECTRON | UD;
	isSealed(cell: ATOM | null | UD): B;
	isFrozen(cell: ATOM | null | UD): B;
}

export function makeGateAccess(): GATE_ACCESS {
	return {
		gateOf(cell) {
			return cell?.registry?.get(GATE_ELECTRON);
		},
		isSealed(cell) {
			return cell?.registry?.get(GATE_ELECTRON)?.sealed ?? false;
		},
		isFrozen(cell) {
			return cell?.registry?.get(GATE_ELECTRON)?.frozen ?? false;
		},
	};
}
// #endregion
