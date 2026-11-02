// neutrons/permissions.ts
//                                    PERMISSIONS_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class PERMISSIONS_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	public permissions = new Map<string, ChannelPermissions>();

	public setPermissions(rt: string, perms: ChannelPermissions): void {
		this.permissions.set(rt, perms);
	}
	public getPermissions(rt: string): ChannelPermissions | undefined { return this.permissions.get(rt); }
	public removePermissions(rt: string): B { return this.permissions.delete(rt); }
	public hasPermissions(rt: string): B { return this.permissions.has(rt); }
	public getAllPermissions(): Record<string, ChannelPermissions> { return Object.fromEntries(this.permissions); }
	public listProtectedChannels(): string[] { return [...this.permissions.keys()]; }

	/** Wildcard-aware lookup: exact wins, else longest matching prefix before `*`. */
	public resolvePermissions(rt: string): ChannelPermissions | undefined {
		if (this.permissions.has(rt)) return this.permissions.get(rt);
		let best: ChannelPermissions | undefined;
		let bestLen = -1;
		for (const [pattern, perms] of this.permissions) {
			const star = pattern.indexOf('*');
			if (star === -1) continue;
			if (rt.startsWith(pattern.slice(0, star)) && star > bestLen) { best = perms; bestLen = star; }
		}
		return best;
	}

	/** Check one capability. No permission set → allowed (open channel). */
	public check(rt: string, action: 'read' | 'write' | 'send' | 'subscribe' | 'admin', role?: S): B {
		const perms = this.resolvePermissions(rt);
		if (!perms) return true;
		if (role && perms.roles && perms.roles.length && !perms.roles.includes(role)) return false;
		if (action === 'read') return perms.read;
		if (action === 'write') return perms.write;
		return perms[action] ?? perms.write;
	}

	purge(): void { this.permissions.clear(); }
}
// #endregion
