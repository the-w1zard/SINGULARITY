// neutrons/priority.ts
//                                    PRIORITY_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class PRIORITY {
	k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
	constructor( _config: Record<S, unknown> = {}) {
	}

	private queues = new Map<string, Array<{ priority: SIG_BUS_Priority; weight: N; seq: N; item: A; enqueuedAt: N }>>();
	private queueDefaults = new Map<string, SIG_BUS_Priority>();
	private defaultPriority: SIG_BUS_Priority = 'normal';
	private seq = 0;

	public setDefaultPriority(priority: SIG_BUS_Priority): void { this.defaultPriority = priority; }
	public getDefaultPriority(): SIG_BUS_Priority { return this.defaultPriority; }
	public setQueueDefault(queue: string, priority: SIG_BUS_Priority): void { this.queueDefaults.set(queue, priority); }
	public getQueueDefault(queue: string): SIG_BUS_Priority { return this.queueDefaults.get(queue) ?? this.defaultPriority; }

	private priorityWeight(p?: SIG_BUS_Priority): number { return p === 'high' ? 3 : p === 'low' ? 1 : 2; }

	public enqueue(queue: string, item: A, priority?: SIG_BUS_Priority): N {
		const p = priority ?? this.getQueueDefault(queue);
		let q = this.queues.get(queue);
		if (!q) { q = []; this.queues.set(queue, q); }
		const entry = { priority: p, weight: this.priorityWeight(p), seq: this.seq++, item, enqueuedAt: Date.now() };
		q.push(entry);
		if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('priority_enqueued', undefined, undefined, queue, { priority: p, depth: q.length });
		return entry.seq;
	}

	/** Highest weight wins; FIFO within the same weight (low seq = older = first). */
	public dequeue(queue: string): A | undefined {
		const q = this.queues.get(queue);
		if (!q || !q.length) return undefined;
		let bestIdx = 0;
		for (let i = 1; i < q.length; i++) {
			const a = q[i], b = q[bestIdx];
			if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) bestIdx = i;
		}
		const [entry] = q.splice(bestIdx, 1);
		if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('priority_dequeued', undefined, undefined, queue, { priority: entry.priority, remaining: q.length });
		return entry.item;
	}

	public peek(queue: string): A | undefined {
		const q = this.queues.get(queue);
		if (!q || !q.length) return undefined;
		let bestIdx = 0;
		for (let i = 1; i < q.length; i++) {
			const a = q[i], b = q[bestIdx];
			if (a.weight > b.weight || (a.weight === b.weight && a.seq < b.seq)) bestIdx = i;
		}
		return q[bestIdx].item;
	}
	public getDepth(queue: string): number { return (this.queues.get(queue) ?? []).length; }
	public listQueues(): string[] { return [...this.queues.keys()]; }
	public getDepths(): Record<string, number> {
		const out: Record<string, number> = {};
		for (const [q, list] of this.queues) out[q] = list.length;
		return out;
	}
	public clearQueue(queue: string): void { this.queues.delete(queue); }

	purge(): void { this.queues.clear(); this.queueDefaults.clear(); }
}
// #endregion
