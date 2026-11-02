// protons/values_registry.ts                                     VALUES_REGISTRY_PROTON

import type { ATOM_KEY } from "../contract/CONTROL_ROOM";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class VALUES_REGISTRY_PROTON {
    public root_cell_key: ATOM_KEY;
    
    constructor(root_cell_key: ATOM_KEY, _config: Record<string, unknown> = {}) {
        this.root_cell_key = root_cell_key;
    }
    public readonly instanceId: string = `instance_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    public hasWritten = false;
    public lastWriteSource?: string;
    public lastWriteAt?: number;
    public writeCount = 0;
    public recordWrite(source = "write"): void {
        this.hasWritten = true;
        this.lastWriteSource = source;
        this.lastWriteAt = Date.now();
        this.writeCount++;
    }
    public reset(): void {
        this.hasWritten = false;
        this.lastWriteSource = undefined;
        this.lastWriteAt = undefined;
        this.writeCount = 0;
    }
}
// #endregion
