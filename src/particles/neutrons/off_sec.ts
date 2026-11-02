// neutrons/off_sec.ts
//                                    OFFSEC_NEUTRON
//  Minimal and real: mark values as secret, scrub them out of messages before they leave,
//  and keep an audit trail of what was scrubbed. No pretending to be a security suite.
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface OFFSEC_AUDIT_ENTRY { at: N; what: S; channel?: S; }

export class OFFSEC_NEUTRON {
	public root_cell: ATOM_KEY;
	private secrets = new Set<A>();
	public audit: OFFSEC_AUDIT_ENTRY[] = [];
	public maxAudit: N;

	constructor(root_cell: ATOM_KEY, config: { maxAudit?: N } = {}) {
		this.root_cell = root_cell;
		this.maxAudit = config.maxAudit ?? 500;
	}

	// ── secret marking ─────────────────────────────────────────────────────────────────────────
	/** Mark a value (token, key, credential) as secret. Reference equality by default. */
	public markSecret(value: A): void { this.secrets.add(value); }
	public isSecret(value: A): B { return this.secrets.has(value); }
	public clearSecret(value: A): B { return this.secrets.delete(value); }

	/**
	 * Deep-scan a message for marked secrets; replace with '[REDACTED]' before the message
	 * leaves the boundary. Mutates nothing — returns a copy when a hit occurs.
	 */
	public scrub<T>(payload: T): { clean: T; redacted: N } {
		let redacted = 0;
		const seen = new Set<object>();
		const walk = (v: unknown): unknown => {
			if (this.secrets.has(v as A)) { redacted++; return '[REDACTED]'; }
			if (v === null || typeof v !== 'object') return v;
			if (seen.has(v as object)) return '[CYCLIC]';
			seen.add(v as object);
			if (Array.isArray(v)) return v.map(walk);
			const out: Record<S, unknown> = {};
			for (const [k, val] of Object.entries(v as Record<S, unknown>)) out[k] = walk(val);
			return out;
		};
		const clean = walk(payload) as T;
		if (redacted) {
			this.audit.push({ at: Date.now(), what: `scrubbed ${redacted} secret(s)` });
			if (this.audit.length > this.maxAudit) this.audit.shift();
		}
		return { clean, redacted };
	}

	public listAudit(): OFFSEC_AUDIT_ENTRY[] { return [...this.audit]; }

	purge(): void { this.secrets.clear(); this.audit.length = 0; }
}
// #endregion
