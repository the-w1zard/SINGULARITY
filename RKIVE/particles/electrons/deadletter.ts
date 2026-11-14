// electrons/deadletter.ts


//  ND                                  DEADLETTER_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface DEADLETTER_WIRE { enqueue(msg: CSEMessage): Promise<void> | void; }

export class DEADLETTER_ELECTRON {
	public root_cell_key: ATOM_KEY;
	public deadLetters: CSEMessage[] = [];
	public maxDeadLetters: N;
	private wire?: DEADLETTER_WIRE;

	constructor(root_cell_key: ATOM_KEY, config: { maxDeadLetters?: N; wire?: DEADLETTER_WIRE } = {}) {
		this.root_cell_key = root_cell_key;
		this.maxDeadLetters = config.maxDeadLetters ?? 1000;
		this.wire = config.wire;
	}

	/** Park a failed message. Oldest evicted when over max. */
	public record(msg: CSEMessage, reason?: S): void {
		this.deadLetters.push({ ...msg, _ack: true } as CSEMessage);
		if (this.deadLetters.length > this.maxDeadLetters) this.deadLetters.shift();
	}

	public async listDeadLetters(k?: S, sl?: S, ms?: S, rt?: S): Promise<CSEMessage[]> {
		if (!rt && !ms && !sl && !k) return [...this.deadLetters];
		return this.deadLetters.filter(m => (!rt || m.rt === rt) && (!ms || m.ms === ms) && (!sl || m.sl === sl) && (!k || m.k === k));
	}

	public async requeueDeadLetter(id: S, ttl?: N): Promise<void> {
		const idx = this.deadLetters.findIndex(m => m.id === id);
		if (idx === -1) return;
		const msg = this.deadLetters.splice(idx, 1)[0];
		msg.ttl = ttl ?? msg.ttl;
		await this.wire?.enqueue(msg);
	}

	public purgeDeadLetter(id: S): B {
		const idx = this.deadLetters.findIndex(m => m.id === id);
		if (idx === -1) return false;
		this.deadLetters.splice(idx, 1);
		return true;
	}
	public count(): N { return this.deadLetters.length; }

	purge(): void { this.deadLetters.length = 0; }
}
// #endregion
