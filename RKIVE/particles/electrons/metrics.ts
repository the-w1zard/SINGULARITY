// electrons/metrics.ts



//  ND                                  METRICS_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class METRICS{
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
	public adapters = {
		metrics :new Map<string, { id: string; total: number; failures: number; totalLatency: number; disabled: boolean; lastWindow: number; lastCount: number }>()
	}
    public seen = new Set<string>()
    public getSeenCount(): number { return this.seen.size }
    public getMessageCount(rt: string,): number { return this.messageCount.get(rt) ?? 0 }

    public messageCount = new Map<string, number>()
	public getAdapterMetrics(): AdapterMetrics[] {
        const now = Date.now()
        const out: AdapterMetrics[] = []
        for (const m of this.adapterMetrics.values()) {
            const windowSec = Math.max(1, (now - m.lastWindow) / 1000)
            const throughput = m.lastCount / windowSec
            const failureRate = m.total === 0 ? 0 : m.failures / m.total
            const latencyMs = m.total === 0 ? 0 : m.totalLatency / m.total
            out.push({
                id: m.id,
                latencyMs,
                failureRate,
                throughputPerSec: throughput,
                healthy: !m.disabled && failureRate < 0.5,
                disabled: m.disabled
            })
            m.lastWindow = now
            m.lastCount = 0
        }
        return out
    }
	 public _queueMetrics = {
        totalEnqueued: 0,
        totalDropped: 0,
        totalProcessed: 0,
        peakSize: 0,
        currentSize: 0
    };
}
// #endregion
