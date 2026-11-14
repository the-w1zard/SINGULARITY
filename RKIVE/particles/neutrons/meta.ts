// neutrons/meta.ts

//                                    META_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class META {
	k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

	// ── your blob, organized ───────────────────────────────────────────────────────────────────
	public _batches = new Set<string | symbol>();
	public _batchQueue = new Map<string | symbol, A>();
	public _batchOpts = new Map<string | symbol, { triggerRender?: B }>();
	public _globalListeners = new Set<(key: S | symbol, value: A) => void>();
	public _global_subscribers = new Map<S, Set<() => void>>();
	public _sys = new Map<S, SysEntry>();
	public _sealed = new Set<S>();
	public _frozen = new Set<S>();
	public _keyContracts = new Map<S, Set<S>>();
	public _conflicts = new Map<S, ConflictPolicy>();
	public _conflictStrategies = new Map<S, 'latest' | 'fail' | 'accumulate'>();
	public _causal = new Map<S, CausalEntry[]>();
	public _scopes = new Map<S, SignalScope>();
	public _msgHandlers = new Map<S, Set<(p: A) => void>>();
	public _lastWriteSource = new Map<S, S>();
	public _mainMap = new Map<A, S>();
	public _tupleCache = new Map<S, S>();
	public _weakMap = new WeakMap<object, S>();
	public _boundaries = new WeakMap<object, S>();

	constructor( _config: Record<S, unknown> = {}) {
	}

	// ── sys entries ────────────────────────────────────────────────────────────────────────────
	/** Register or update a system entry for this cell. */
	public setSys(name: S, entry: Partial<SysEntry> & { value?: A }): SysEntry {
		const prev = this._sys.get(name);
		const next: SysEntry = {
			name,
			value: entry.value ?? prev?.value,
			ts: Date.now(),
			revision: (prev?.revision ?? 0) + 1,
			source: entry.source ?? 'meta',
		};
		this._sys.set(name, next);
		return next;
	}
	public getSys(name: S): SysEntry | null { return this._sys.get(name) ?? null; }
	public deleteSys(name: S): B { return this._sys.delete(name); }
	public listSys(): S[] { return [...this._sys.keys()]; }

	// ── causal log ─────────────────────────────────────────────────────────────────────────────
	/** Append a causal entry. Keep it cheap — callers pass what they know. */
	public recordCausal(key: S, cause: S, value?: A): void {
		const list = this._causal.get(key) ?? [];
		list.push({ cause, value, ts: Date.now() });
		// hard cap so long-lived cells don't balloon
		if (list.length > 256) list.splice(0, list.length - 256);
		this._causal.set(key, list);
	}
	public getCausal(key: S): CausalEntry[] { return this._causal.get(key) ?? []; }
	public clearCausal(key?: S): void {
		if (key) this._causal.delete(key);
		else this._causal.clear();
	}

	 
	// ── last write provenance ──────────────────────────────────────────────────────────────────
	public noteWrite(key: S, source: S): void { this._lastWriteSource.set(key, source); }
	public whoWrote(key: S): S | null { return this._lastWriteSource.get(key) ?? null; }

	// ── msg handlers ───────────────────────────────────────────────────────────────────────────
	public addMsgHandler(key: S, fn: (p: A) => void): () => void {
		const set = this._msgHandlers.get(key) ?? new Set();
		set.add(fn);
		this._msgHandlers.set(key, set);
		return () => set.delete(fn);
	}
	public emitMsg(key: S, p: A): void {
		for (const fn of this._msgHandlers.get(key) ?? []) {
			try { fn(p); } catch (e) { console.error(`[meta-shard] msg handler failed for "${key}":`, e); }
		}
	}

	// ── global listeners ───────────────────────────────────────────────────────────────────────
	public onGlobal(fn: (key: S | symbol, value: A) => void): () => void {
		this._globalListeners.add(fn);
		return () => this._globalListeners.delete(fn);
	}
	public emitGlobal(key: S | symbol, value: A): void {
		for (const fn of this._globalListeners) {
			try { fn(key, value); } catch (e) { console.error('[meta-shard] global listener failed:', e); }
		}
	}

	// ── inspect / purge ────────────────────────────────────────────────────────────────────────
	public inspect(): Record<S, A> {
		return {
			sys: Object.fromEntries(this._sys),
			causalKeys: [...this._causal.keys()],
			sealed: [...this._sealed],
			frozen: [...this._frozen],
			lastWriteSource: Object.fromEntries(this._lastWriteSource),
			msgHandlerKeys: [...this._msgHandlers.keys()],
			globalListenerCount: this._globalListeners.size,
		};
	}

	purge(): void {
		for (const map of [this._batchQueue, this._batchOpts, this._global_subscribers, this._sys,
			this._keyContracts, this._conflicts, this._conflictStrategies, this._causal,
			this._scopes, this._msgHandlers, this._lastWriteSource, this._mainMap, this._tupleCache] as Map<A, A>[]) map.clear();
		this._batches.clear();
		this._sealed.clear();
		this._frozen.clear();
		this._globalListeners.clear();
		this._weakMap = new WeakMap();
		this._boundaries = new WeakMap();
	}
}

// #endregion
