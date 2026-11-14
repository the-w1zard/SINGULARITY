// neutrons/history.ts
//                                   HISTORY_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class HISTORY {
	k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
	constructor( _config: Record<S, unknown> = {}) {
	}

	public history = new Map<string, CSEMessage[]>();
	public defaultLimit = 128;

	/** Append one message. Returns the entry so callers can thread it onward. */
	public record(rt: string, msg: CSEMessage): CSEMessage {
		let list = this.history.get(rt);
		if (!list) { list = []; this.history.set(rt, list); }
		list.push(msg);
		const limit = this.limits.get(rt) ?? this.defaultLimit;
		if (list.length > limit) list.splice(0, list.length - limit);
		return msg;
	}
	private limits = new Map<string, number>();

	public setChannelLimit(rt: string, limit: number): void {
		this.limits.set(rt, limit);
		const list = this.history.get(rt);
		if (list && list.length > limit) list.splice(0, list.length - limit);
	}
	public getChannelLimit(rt: string): number { return this.limits.get(rt) ?? this.defaultLimit; }

	public getRecentHistory(rt: string, limit = 3): CSEMessage[] { return (this.history.get(rt) ?? []).slice(-limit); }
	public getChannelHistory(rt: string): CSEMessage[] { return [...(this.history.get(rt) ?? [])]; }
	public getAllHistoryChannels(): string[] { return [...this.history.keys()]; }
	public getChannelDepth(rt: string): number { return (this.history.get(rt) ?? []).length; }
	public getHistorySizes(): Record<string, number> {
		const out: Record<string, number> = {};
		for (const [ch, list] of this.history) out[ch] = list.length;
		return out;
	}
	public clearChannel(rt: string): void { this.history.delete(rt); }

	purge(): void { this.history.clear(); this.limits.clear(); }
}
// #endregion
