// electrons/reactive.ts
//  ND                                  REACTIVE_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class REACTIVE_ELECTRON<W = unknown> {
	public root_cell_key: ATOM_KEY;
	public triggerRender?: B;
	public lifecycle?: A;
	public async?: P<W>;
	public batches = new Set<ATOM_KEY>();
	public batchQueue = new Map<ATOM_KEY, unknown>();
	public batchOpts = new Map<ATOM_KEY, { triggerRender?: B }>();
	public cycle = 0;
	public runDepth = 0;
	public batchDepth = 0;
	public notifyIndex = 0;
	public queuedLength = 0;
	public activeSub: ReactiveNode | undefined;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
}
// #endregion
