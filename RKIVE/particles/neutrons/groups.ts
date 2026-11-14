// neutrons/groups.ts

//  ND                                  GROUPS_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class GROUPS_NEUTRON {
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",
	constructor( _config: Record<S, unknown> = {}) {
	}
    public groups = new Map<string, { channels: Set<string>; enabled: boolean; middleware: ((msg: CSEMessage) => boolean | void)[] }>()
	    public createGroup(name: string, channels: string[] = [], options?: GroupOptions): void {
        this.groups.set(name, {
            channels: new Set(channels),
            enabled: options?.enabled ?? true,
            middleware: options?.middleware ? [options.middleware] : []
        })

    }
    public deleteGroup(name: string): void {
        this.groups.delete(name);
    }
    public enableGroup(name: string): void {
        const g = this.groups.get(name);
        if (!g) return; g.enabled = true;
    }
    public disableGroup(name: string): void {
        const g = this.groups.get(name); if (!g) return; g.enabled = false;
    }


    public addToGroup(name: string, rt: string): void {
        const g = this.groups.get(name)
        if (!g) return
        g.channels.add(rt)
    }
    public removeFromGroup(name: string, rt: string): void {
        const g = this.groups.get(name)
        if (!g) return
        g.channels.delete(rt)
    }
    public async groupBroadcast(name: string, p: any, sl = this.sl, ms = this.ms, rt = this.rt, ttl?: number): Promise<void> {
        const g = this.groups.get(name)
        if (!g || !g.enabled) return
        await Promise.all([...g.channels].map(ch => this.broadcast(p, ch, sl, ms, rt, ttl)))
    }
    public groupUse(name: string, fn: (msg: CSEMessage) => boolean | void): void {
        const g = this.groups.get(name)
        if (!g) return
        g.middleware.push(fn)
    }
	 public getAllGroups(): Record<string, { channels: string[]; enabled: boolean; middlewareCount: number }> {
        const out: Record<string, { channels: string[]; enabled: boolean; middlewareCount: number }> = {}
        for (const [name, g] of this.groups) {
            out[name] = { channels: [...g.channels], enabled: g.enabled, middlewareCount: g.middleware.length }
        }
        return out
    }
}
// #endregion
