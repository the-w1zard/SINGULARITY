// protons/schema.ts
import type { ATOM_KEY } from "../contract/CONTROL_ROOM";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class SCHEMA_PROTON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	public _schemas = new Map<string, CSESchema>();

	public setChannelSchema(schema: CSESchema, rt: string): void {
		this._schemas.set(rt, schema);
	}
	public getChannelSchema(rt: string): CSESchema | undefined { return this._schemas.get(rt); }
	public removeChannelSchema(rt: string): B {
		const had = this._schemas.delete(rt);
		return had;
	}
	public hasChannelSchema(rt: string): B { return this._schemas.has(rt); }
	public listChannelSchemas(): string[] { return [...this._schemas.keys()]; }
	public getAllSchemas(): Record<string, CSESchema> { return Object.fromEntries(this._schemas); }

	/** Wildcard-aware lookup: exact channel wins, else longest matching prefix before `*`. */
	public resolveSchema(rt: string): CSESchema | undefined {
		if (this._schemas.has(rt)) return this._schemas.get(rt);
		let best: CSESchema | undefined;
		let bestLen = -1;
		for (const [pattern, schema] of this._schemas) {
			const star = pattern.indexOf('*');
			if (star === -1) continue;
			if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) { best = schema; bestLen = star; }
		}
		return best;
	}

	/**
	 * Validate a message against its channel schema. No schema → pass (open channel).
	 * Returns { ok, errors } — never throws.
	 */
	public checkSchema(rt: string, msg: CSEMessage): { ok: B; errors: S[] } {
		const schema = this.resolveSchema(rt);
		if (!schema) return { ok: true, errors: [] };
		const errors: S[] = [];
		const payload = (msg.p ?? {}) as Record<S, A>;
		for (const [field, rule] of Object.entries(schema.fields ?? {})) {
			const v = payload[field];
			if (v === undefined || v === null) {
				if (rule.required) errors.push(`missing required field "${field}"`);
				continue;
			}
			if (rule.type !== 'any') {
				const actual = typeof v;
				if (rule.type === 'number' && actual !== 'number') errors.push(`field "${field}" expected number, got ${actual}`);
				else if (rule.type === 'string' && actual !== 'string') errors.push(`field "${field}" expected string, got ${actual}`);
				else if (rule.type === 'boolean' && actual !== 'boolean') errors.push(`field "${field}" expected boolean, got ${actual}`);
			}
		}
		if (schema.validate) {
			try {
				const r = schema.validate(msg);
				if (r !== true && r !== undefined) errors.push(typeof r === 'string' ? r : `validator rejected message on "${rt}"`);
			} catch (e) { errors.push(`validator threw: ${e}`); }
		}
		return { ok: errors.length === 0, errors };
	}

	purge(): void { this._schemas.clear(); }
}
// #endregion
