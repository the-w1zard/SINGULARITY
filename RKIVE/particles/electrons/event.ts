// electrons/event.ts
//  ND                                  EVENT_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
type EVENT_LISTENER = (payload?: A) => void;

export class EVENT {
	public root_cell: ATOM_KEY;
	private events = new Map<S, Set<EVENT_LISTENER>>();
	private wildcard = new Set<(event: S, payload?: A) => void>();

	constructor(root_cell: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell = root_cell;
	}

	public on(event: S, fn: EVENT_LISTENER): () => void {
		const set = this.events.get(event) ?? new Set<EVENT_LISTENER>();
		set.add(fn);
		this.events.set(event, set);
		return () => set.delete(fn);
	}
	public once(event: S, fn: EVENT_LISTENER): () => void {
		const off = this.on(event, (p) => { off(); fn(p); });
		return off;
	}
	public onAny(fn: (event: S, payload?: A) => void): () => void {
		this.wildcard.add(fn);
		return () => this.wildcard.delete(fn);
	}
	public off(event: S, fn?: EVENT_LISTENER): void {
		if (!fn) this.events.delete(event);
		else this.events.get(event)?.delete(fn);
	}
	public emit(event: S, payload?: A): number {
		let delivered = 0;
		for (const fn of this.wildcard) {
			try { fn(event, payload); delivered++; } catch (e) { console.error(`[event-shard] wildcard listener failed for "${event}":`, e); }
		}
		for (const fn of [...(this.events.get(event) ?? [])]) {
			try { fn(payload); delivered++; } catch (e) { console.error(`[event-shard] listener failed for "${event}":`, e); }
		}
		return delivered;
	}
	public async emitAsync(event: S, payload?: A): Promise<number> {
		let delivered = 0;
		for (const fn of this.wildcard) { try { await fn(event, payload); delivered++; } catch (e) { console.error(`[event-shard] wildcard listener failed for "${event}":`, e); } }
		for (const fn of [...(this.events.get(event) ?? [])]) { try { await fn(payload); delivered++; } catch (e) { console.error(`[event-shard] listener failed for "${event}":`, e); } }
		return delivered;
	}
	public listenerCount(event?: S): number {
		if (event) return this.events.get(event)?.size ?? 0;
		let n = this.wildcard.size;
		for (const set of this.events.values()) n += set.size;
		return n;
	}
	public eventNames(): S[] { return [...this.events.keys()]; }

	purge(): void { this.events.clear(); this.wildcard.clear(); }
}
// #endregion
