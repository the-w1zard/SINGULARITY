//protons/contract.ts
//  ND                                  CONTRACTS_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface CONTRACT_PROTON {
	name: S;
	precondition?: (value: A) => boolean;
	postcondition?: (value: A) => boolean;
	invariant?: (snapshot: Record<S, A>) => boolean;
	block?: B;
}

export interface CONTRACT_VIOLATION {
	contract: S;
	phase: 'pre' | 'post' | 'invariant';
	value?: A;
	message: S;
	ts: N;
}

export class CONTRACTS_PROTON {
	public root_cell: ATOM_KEY;
	public contracts = new Map<S, SOUL_CONTRACT>();
	public causal: CONTRACT_VIOLATION[] = [];
	public violations: CONTRACT_VIOLATION[] = [];
	public violationMax = 64;
	private _throwOnBlock: B = true;

	constructor(root_cell: ATOM_KEY, config: { throwOnBlock?: B } = {}) {
		this.root_cell = root_cell;
		if (config.throwOnBlock !== undefined) this._throwOnBlock = config.throwOnBlock;
	}

	// ── registration ───────────────────────────────────────────────────────────────────────────
	public add(contract: SOUL_CONTRACT): () => void {
		this.contracts.set(contract.name, contract);
		return () => this.contracts.delete(contract.name);
	}
	public remove(name: S): B { return this.contracts.delete(name); }
	public get(name: S): SOUL_CONTRACT | undefined { return this.contracts.get(name); }
	public list(): SOUL_CONTRACT[] { return [...this.contracts.values()]; }

	/** Sync key-contract bindings from META_SHARD._keyContracts (key → contract names). */
	public bindKeyContracts(keyContracts: Map<S, Set<S>>): void {
		this._keyContracts = keyContracts;
	}
	private _keyContracts: Map<S, Set<S>> = new Map();

	// ── enforcement ────────────────────────────────────────────────────────────────────────────
	private _record(v: CONTRACT_VIOLATION, blocking: B): B {
		this.violations.push(v);
		if (this.violations.length > this.violationMax) this.violations.splice(0, this.violations.length - this.violationMax);
		if (blocking) throw new Error(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
		console.warn(`ATOM contract "${v.contract}" violated (${v.phase}): ${v.message}`);
		return false;
	}

	/**
	 * Full check. Returns true when every contract passes.
	 * blocking contracts throw on violation; non-blocking ones warn and count.
	 */
	public check(value: A, phase: 'pre' | 'post', snapshot?: Record<S, A>): B {
		for (const c of this.contracts.values()) {
			if (phase === 'pre' && c.precondition) {
				try { if (!c.precondition(value)) return this._record({ contract: c.name, phase, value, message: 'precondition returned false', ts: Date.now() }, !!c.block && this._throwOnBlock); }
				catch (e) { return this._record({ contract: c.name, phase, value, message: `precondition threw: ${e}`, ts: Date.now() }, true); }
			}
			if (phase === 'post' && c.postcondition) {
				try { if (!c.postcondition(value)) return this._record({ contract: c.name, phase, value, message: 'postcondition returned false', ts: Date.now() }, !!c.block && this._throwOnBlock); }
				catch (e) { return this._record({ contract: c.name, phase, value, message: `postcondition threw: ${e}`, ts: Date.now() }, true); }
			}
			if (phase === 'invariant' && c.invariant && snapshot) {
				try { if (!c.invariant(snapshot)) return this._record({ contract: c.name, phase, value, message: 'invariant returned false', ts: Date.now() }, !!c.block && this._throwOnBlock); }
				catch (e) { return this._record({ contract: c.name, phase, value, message: `invariant threw: ${e}`, ts: Date.now() }, true); }
			}
		}
		return true;
	}

	/** Check contracts bound to a specific key (META._keyContracts). */
	public checkKey(key: S, value: A, phase: 'pre' | 'post'): B {
		for (const name of this._keyContracts.get(key) ?? []) {
			const c = this.contracts.get(name);
			if (!c) continue;
			const check = phase === 'pre' ? c.precondition : c.postcondition;
			if (!check) continue;
			try {
				if (!check(value)) return this._record({ contract: name, phase, value, message: `key contract failed for "${key}"`, ts: Date.now() }, !!c.block && this._throwOnBlock);
			} catch (e) {
				return this._record({ contract: name, phase, value, message: `key contract threw: ${e}`, ts: Date.now() }, true);
			}
		}
		return true;
	}

	/** Alias used by ATOM.write() — runs pre-contracts before the value lands. */
	public canWrite(value: A): B { return this.check(value, 'pre'); }
	/** Alias used by ATOM.write() — runs post-contracts after the value landed. */
	public didWrite(value: A): B { return this.check(value, 'post'); }

	public listViolations(): CONTRACT_VIOLATION[] { return [...this.violations]; }
	public clearViolations(): void { this.violations.length = 0; }

	purge(): void { this.contracts.clear(); this.violations.length = 0; this._keyContracts.clear(); }
}
// #endregion
