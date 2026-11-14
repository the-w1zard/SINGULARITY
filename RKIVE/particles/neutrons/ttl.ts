// neutrons/ttl.ts
//                                    TTL_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface TTL_ENTRY { expiresAt: N; data?: A; }

export class TTL {
	public entries = new Map<S, TTL_ENTRY>();
	public onExpire: ((key: S, data?: A) => void)[] = [];

	constructor( config: { defaultTtlMs?: N; onExpire?: (key: S, data?: A) => void } = {}) {
		this.defaultTtlMs = config.defaultTtlMs ?? 0; // 0 = no default; entries need explicit ttl
		if (config.onExpire) this.onExpire.push(config.onExpire);
	}
	public defaultTtlMs: N;

	// ── write ──────────────────────────────────────────────────────────────────────────────────
	public set(key: S, data?: A, ttlMs?: N): TTL_ENTRY {
		const ttl = ttlMs ?? this.defaultTtlMs;
		if (!ttl || ttl <= 0) throw new Error('[ttl-shard] no ttl provided and no defaultTtlMs configured');
		const entry: TTL_ENTRY = { expiresAt: Date.now() + ttl, data };
		this.entries.set(key, entry);
		return entry;
	}
	public touch(key: S, ttlMs?: N): B {
		const e = this.entries.get(key);
		if (!e) return false;
		e.expiresAt = Date.now() + (ttlMs ?? this.defaultTtlMs);
		return true;
	}

	// ── read ───────────────────────────────────────────────────────────────────────────────────
	/** Returns undefined if missing OR expired (and removes it lazily). */
	public get(key: S): A | undefined {
		const e = this.entries.get(key);
		if (!e) return undefined;
		if (Date.now() >= e.expiresAt) {
			this.entries.delete(key);
			return undefined;
		}
		return e.data;
	}
	public has(key: S): B {
		const e = this.entries.get(key);
		return !!e && Date.now() < e.expiresAt;
	}
	public peek(key: S): A | undefined { return this.entries.get(key)?.data; } // ignores expiry
	public timeToLiveMs(key: S): N | null {
		const e = this.entries.get(key);
		if (!e) return null;
		return Math.max(0, e.expiresAt - Date.now());
	}

	// ── delete ─────────────────────────────────────────────────────────────────────────────────
	public delete(key: S): B { return this.entries.delete(key); }

	// ── sweep ──────────────────────────────────────────────────────────────────────────────────
	/** Removes expired entries, fires onExpire hooks. Returns count expired. */
	public sweep(): number {
		const now = Date.now();
		let n = 0;
		for (const [key, e] of [...this.entries]) {
			if (now >= e.expiresAt) {
				this.entries.delete(key);
				n++;
				for (const fn of this.onExpire) {
					try { fn(key, e.data); } catch (err) { console.error(`[ttl-shard] onExpire failed for "${key}":`, err); }
				}
			}
		}
		return n;
	}
	/** Sweep on a cadence. Owns its own timer — call stopSweeper() to cancel. */
	private _sweeper: ReturnType<typeof setInterval> | null = null;
	public startSweeper(intervalMs = 30_000): void {
		if (this._sweeper) return;
		this._sweeper = setInterval(() => this.sweep(), intervalMs);
	}
	public stopSweeper(): void {
		if (this._sweeper) { clearInterval(this._sweeper); this._sweeper = null; }
	}

	public get size(): number { return this.entries.size; }
	public listKeys(): S[] { return [...this.entries.keys()]; }

	purge(): void {
		this.stopSweeper();
		this.entries.clear();
		this.onExpire.length = 0;
	}
}
// #endregion
