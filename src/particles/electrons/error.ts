
// electrons/error.ts


//                                    ERROR_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export type MODULE_PATH = S; // e.g. "THE_CLOUD.SIGNAL_CELL", "PRIMITIVES.RIFTS", "HADRON_COLLIDER.ENGINE"

interface ERROR_EMISSION {
	module: MODULE_PATH;
	code: S; // short stable code, e.g. "GATE_SEALED", "CONTRACT_VIOLATION"
	message: S;
	detail?: A;
	ts: N;
}

export class ERROR_ELECTRON {
	public root_cell_key: ATOM_KEY;

	/** History lives here regardless of emission — history is cheap, display is gated. */
	public error_history: ERROR_EMISSION[] = [];
	private readonly HISTORY_CAP = 512;

	// ── runtime-editable controls ─────────────────────────────────────────────────────────────
	/** Master kill-switch. false → nothing emits, history still accrues. */
	public ENABLED: B = true;
	/** Emission only in dev mode unless this is explicitly flipped on. */
	public ENABLE_IN_PRODUCTION: B = false;
	/**
	 * Surgical control. null = every module emits.
	 * Record<string, boolean> = only listed modules emit, with true/false overriding.
	 */
	public SURGICAL_LOGS: Record<MODULE_PATH, B> | null = null;
	/** Theme passed through to the LOGGER. Default = the error theme. */
	public theme: Theme = tt.error;
	/** "TERMINAL" | "FILE" — pipes through the LOGGER's configured sink. */
	public PIPE_TO_FILE_OR_TERM: "TERMINAL" | "FILE" = "TERMINAL";

	/** Set at wiring time by the platform; falls back to plain console when absent. */
	public logger: LOGGER | null = null;

	constructor(root_cell_key: ATOM_KEY, config: { logger?: LOGGER; theme?: Theme; surgical?: Record<MODULE_PATH, B> | null } = {}) {
		this.root_cell_key = root_cell_key;
		this.logger = config.logger ?? null;
		this.theme = config.theme ?? tt.error;
		this.SURGICAL_LOGS = config.surgical ?? null;
	}

	// ── the single emission point ─────────────────────────────────────────────────────────────
	/** Every error-producing call site in every shard funnels through here. */
	public emit(module: MODULE_PATH, code: S, message: S, detail?: A): ERROR_EMISSION {
		const entry: ERROR_EMISSION = { module, code, message, detail, ts: Date.now() };

		this.error_history.push(entry);
		if (this.error_history.length > this.HISTORY_CAP) this.error_history.splice(0, this.error_history.length - this.HISTORY_CAP);

		if (this.shouldEmit(module)) {
			const title = `[${String(this.root_cell_key)}] ${module} · ${code}`;
			if (this.logger) {
				this.logger.theme = this.theme;
				this.logger.drop_log([title, message], detail);
			} else if (this.shouldEmit(module)) {
				console.error(`${title}\n  ${message}`, detail ?? "");
			}
		}
		return entry;
	}

	/** Surgical gate: master switch → dev/production → per-module toggle. */
	private shouldEmit(module: MODULE_PATH): B {
		if (!this.ENABLED) return false;
		if (this.ENABLE_IN_PRODUCTION === false && get("DEV_MODE") !== true) return false;
		if (this.SURGICAL_LOGS === null) return true;
		// exact match wins, then longest-prefix match (section-level control)
		if (module in this.SURGICAL_LOGS) return this.SURGICAL_LOGS[module];
		let best: S | null = null;
		for (const prefix of Object.keys(this.SURGICAL_LOGS)) {
			if (module.startsWith(prefix + ".") && (best === null || prefix.length > best.length)) best = prefix;
		}
		return best !== null ? this.SURGICAL_LOGS[best] : false; // explicit map = default silent
	}

	// ── convenience emitters used by the call sites below ────────────────────────────────────
	sealed(detail?: A) { return this.emit("GATE", "GATE_SEALED", "CELL IS SEALED — CHANGE SEALED TO FALSE TO RESUME WRITES", detail); }
	frozen(detail?: A) { return this.emit("GATE", "GATE_FROZEN", "CELL IS FROZEN — VIEWABLE BUT NOT EDITABLE", detail); }
	asleep(detail?: A) { return this.emit("GATE", "GATE_ASLEEP", "CELL IS ASLEEP — WILL NOT BE AVAILABLE UNTIL ITS CONFIGURED WAKE TIME", detail); }
	shardNotFound(name: S, detail?: A) { return this.emit("REGISTRY", "SHARD_NOT_FOUND", `NO SHARD LABELED "${name}" RELATIVE TO THIS CELL — REVIEW YOUR CONTRACT`, detail); }
	contractViolation(contract: S, detail?: A) { return this.emit("CONTRACTS", "CONTRACT_VIOLATION", `CONTRACT VIOLATION: ${contract}`, detail); }
	schemaViolation(topic: S, detail?: A) { return this.emit("SCHEMA", "SCHEMA_VIOLATION", `SCHEMA VIOLATION ON TOPIC "${topic}"`, detail); }
	permissionDenied(ch: S, detail?: A) { return this.emit("PERMISSIONS", "PERMISSION_DENIED", `PERMISSION DENIED ON CHANNEL "${ch}"`, detail); }

	/** Flip an entire section or a single module. */
	public setSection(path: MODULE_PATH, on: B): void {
		(this.SURGICAL_LOGS ??= {})[path] = on;
	}
	public clearSurgical(): void {
		this.SURGICAL_LOGS = null;
	}

	// ── diagnostics ───────────────────────────────────────────────────────────────────────────
	public recent(limit = 20): ERROR_EMISSION[] { return this.error_history.slice(-limit); }
	public byModule(module: MODULE_PATH): ERROR_EMISSION[] { return this.error_history.filter((e) => e.module === module); }
	public byCode(code: S): ERROR_EMISSION[] { return this.error_history.filter((e) => e.code === code); }

	purge(): void { this.error_history.length = 0; }
}
// #endregion
