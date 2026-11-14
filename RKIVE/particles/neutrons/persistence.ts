// neutrons/persistence.ts



//  ND                                  PERSISTENCE_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class PERSISTENCE {
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
	constructor( _config: Record<S, unknown> = {}) {
	}
    public durableOffsets = new Map<string, number>() // key: `${rt}:${subscriberId}`
	public persisted?: B;
    public durableHistory = new Map<string, CSEMessage[]>()
	    public getDurableHistorySizes(): Record<string, number> {
        const out: Record<string, number> = {}
        for (const [ch, hist] of this.durableHistory) out[ch] = hist.length
        return out
    }

    public async replay(options: ReplayOptions, handler: CSEHandler): Promise<void> {
        const history = this.durableHistory.get(options.rt) ?? []
        let slice = history
        if (options.fromOffset !== undefined) slice = slice.slice(options.fromOffset)
        if (options.toOffset !== undefined) slice = slice.slice(0, options.toOffset)
        if (options.fromTimestamp !== undefined) { slice = slice.filter(m => m.ts !== undefined && m.ts >= options.fromTimestamp!); }

        if (options.toTimestamp !== undefined) { slice = slice.filter(m => m.ts !== undefined && m.ts <= options.toTimestamp!); }

        for (const m of slice) handler(m.p)
    }
  public subscribeDurable(handler: CSEHandler, options: DurableOptions, k: string, sl = this.sl, ms = this.ms, rt = this.rt): () => void {
        const key = `${rt}:${options.subscriberId}`
        const offset = this.durableOffsets.get(key) ?? options.fromOffset ?? 0
        const history = this.durableHistory.get(rt) ?? []
        history.slice(offset).forEach(m => handler(m.p))
        return this.sub((p: unknown) => {
            const h = this.durableHistory.get(rt) ?? []
            const nextOffset = h.length
            this.durableOffsets.set(key, nextOffset)
            handler(p)
        }, k, sl, ms, rt)
    }
	    public getAllCheckpoints(): Record<string, number> { return Object.fromEntries(this.durableOffsets) }
	
    public async setCheckpoint(subscriberId: string, offset: number, k: string, sl?: string, ms?: string, rt?: string,): Promise<void> {
        const key = `${rt}:${subscriberId}`
        this.durableOffsets.set(key, offset)
        if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('checkpoint_set', rt, ms || undefined, undefined, { subscriberId, offset })

    }
    public async getCheckpoint(subscriberId: string, k: string, sl?: string, ms?: string, rt?: string,): Promise<number | null> {
        const key = `${rt}:${ms}:${subscriberId}`
        return this.durableOffsets.has(key) ? this.durableOffsets.get(key)! : null
    }
}
// #endregion
