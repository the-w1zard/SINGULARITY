// neutrons/conflict.ts
//  ND                                   CONFLICT_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class CONFLICT_NEUTRON {
	public root_cell_key: ATOM_KEY;
	public conflictStrategy?: "latest" | "fail" | "accumulate";
	public conflict?: any;
	constructor(root_cell_key: ATOM_KEY, config: { conflictStrategy?: "latest" | "fail" | "accumulate"; conflict?: any } = {}) {
		this.root_cell_key = root_cell_key;
		this.conflictStrategy = config.conflictStrategy ?? "latest";
		this.conflict = config.conflict;
	}
	public resolve<T>(current: T, incoming: T): T {
		switch (this.conflictStrategy) {
			case "fail":
				throw new Error("SOUL_NEUTRON conflict: concurrent values cannot be merged");
			case "accumulate":
				if (typeof current === "number" && typeof incoming === "number") return (current + incoming) as T;
				if (Array.isArray(current) && Array.isArray(incoming)) return [...current, ...incoming] as T;
				if (typeof current === "string" && typeof incoming === "string") return (current + incoming) as T;
				throw new TypeError("SOUL_NEUTRON accumulate conflict requires matching number, string, or array values");
			case "latest":
			default:
				return incoming;
		}
	}
}
// #endregion
