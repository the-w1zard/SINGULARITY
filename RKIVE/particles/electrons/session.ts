// electrons/session.ts
//                                    SESSION_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class SESSION {
	public root_cell: ATOM_KEY;
	public _cse_token: S | null = null;
	public _sessionId: S | null = null;
	public _sessionToken: S | null = null;
	public _session_started_at: N | null = null;
	public _session_expires_at: N | null = null;
	public _auth_vized: B = false;
	private _auth_initialized: B = false;
	private _sessionTtlMs: N;

	constructor(root_cell: ATOM_KEY, config: { cse_token?: S; sessionTtlMs?: N } = {}) {
		this.root_cell = root_cell;
		this._sessionTtlMs = config.sessionTtlMs ?? 1000 * 60 * 60 * 12; // 12h default
		if (config.cse_token) this._cse_token = config.cse_token;
	}

	// ── crypto ─────────────────────────────────────────────────────────────────────────────────
	private computeHmacSha256(secret: S, data: S): S {
		if (BUS_CRYPTO) {
			const key = BUS_CRYPTO.to_utf8_bytes(secret);
			const mac = BUS_CRYPTO.hash_hmac(key, data); // base64
			return BUS_CRYPTO.to_hex(BUS_CRYPTO.from_base64(mac));
		}
		return '';
	}

	// ── lifecycle ──────────────────────────────────────────────────────────────────────────────
	/**
	 * Stand the session up. Reads CSE_TOKEN from .env when no token was injected.
	 * No token anywhere? The session still runs, but UNAUTHORIZED (matches the bus's stance).
	 */
	public async initAuth(): Promise<void> {
		if (this._auth_initialized) return;
		if (!this._cse_token && typeof process !== 'undefined') {
			try { if (dotenv) dotenv.config(); } catch { /* env loader optional */ }
			const sk = (typeof process !== 'undefined' && process.env?.CSE_TOKEN) || null;
			if (sk && sk.length >= 5) this._cse_token = sk;
		}
		this._newSession();
		this._auth_initialized = true;
	}

	private _newSession(): void {
		const now = Date.now();
		if (this._cse_token && BUS_CRYPTO) {
			this._sessionId = BUS_CRYPTO.to_hex(BUS_CRYPTO.random_bytes(16));
			this._sessionToken = this.computeHmacSha256(this._cse_token, this._sessionId);
		} else {
			// degraded mode: still unique, just not cryptographically bound
			this._sessionId = `sess_${now}_${Math.random().toString(36).slice(2)}`;
			this._sessionToken = null;
		}
		this._session_started_at = now;
		this._session_expires_at = now + this._sessionTtlMs;
		this._auth_vized = !!this._sessionToken;
	}

	/** Rotate the session id + token. Anything wrapped before is invalid after this. */
	public renew(): void { this._newSession(); }

	/** Force-expire. secureUnwrap() will reject everything until renew() or initAuth(). */
	public expire(): void {
		this._session_expires_at = Date.now() - 1;
		this._auth_vized = false;
	}

	public isValid(): B {
		return this._session_expires_at !== null && Date.now() < this._session_expires_at;
	}
	public get expired(): B { return !this.isValid(); }
	public get isSecure(): B { return this._cse_token !== null && this._sessionToken !== null; }

	// ── wrap / unwrap ──────────────────────────────────────────────────────────────────────────
	/** Wrap a payload with the cell's session identity. */
	public secureWrap(p: A) {
		return { sessionId: this._sessionId, token: this._sessionToken, ts: Date.now(), p };
	}

	/** Unwrap + verify. Tampered, stale, or foreign session → null. */
	public async secureUnwrap(msg: A): Promise<A | null> {
		if (!msg?.sessionId || !msg?.token) return null;
		if (msg.sessionId !== this._sessionId) return null;   // not our session
		if (!this.isValid()) return null;                     // our session but expired
		const expected = await this.computeHmacSha256(this._cse_token ?? '', msg.sessionId);
		if (this._sessionToken && msg.token !== expected) return null;
		return msg.p;
	}

	 
	purge(): void {
		this._sessionId = null;
		this._sessionToken = null;
		this._session_started_at = null;
		this._session_expires_at = null;
		this._auth_vized = false;
		this._auth_initialized = false;
	}
}
// #endregion
