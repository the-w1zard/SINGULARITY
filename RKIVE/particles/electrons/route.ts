// electrons/route.ts


//  ND                                 ROUTE_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class ROUTE {
	public root_cell_key: ATOM_KEY;
	public routeRules: RouteRule[] = [];

	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) { this.root_cell_key = root_cell_key; }

	private static toRegex(pattern: S): RegExp {
		return new RegExp('^' + pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*\*/g, '.*').replace(/\*/g, '[^.]+') + '$');
	}

	public addRouteRule(rule: RouteRule): void {
		this.routeRules.push(rule);
		if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('route_added', undefined, undefined, undefined, { name: rule.name });
	}
	public removeRouteRule(name: S): void {
		this.routeRules = this.routeRules.filter(r => r.name !== name);
		if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('route_removed', undefined, undefined, undefined, { name });
	}
	public listRouteRules(): RouteRule[] { return [...this.routeRules]; }

	/** First matching enabled rule wins. Returns the rewritten target(s); adapter defaults to same_boundary. */
	public resolve(rt: S, ms: S, sl: S, k: S): RouteTarget[] {
		for (const rule of this.routeRules) {
			if (rule.enabled === false) continue;
			if (!ROUTE_ELECTRON.toRegex(rule.match).test(rt)) continue;
			const target: RouteTarget = {
				adapter: rule.adapter ?? 'same_boundary',
				rt: rule.target?.rt ?? rt,
				ms: rule.target?.ms ?? ms,
				sl: rule.target?.sl ?? sl,
				k: rule.target?.k ?? k,
				priority: rule.priority,
			};
			if (this.DEVTOOLS_ACTIVE) this.emitDevtoolsEvent('route_resolved', rt, ms, undefined, { rule: rule.name, adapter: target.adapter });
			return [target];
		}
		// no rule: the identity target — same address, same boundary (PATCH 4's fallback)
		return [{ adapter: 'same_boundary', rt, ms, sl, k }];
	}

	purge(): void { this.routeRules.length = 0; }
}
// #endregion
