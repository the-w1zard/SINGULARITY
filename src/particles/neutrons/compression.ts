// neutrons/compression.ts
//  ND                                  COMPRESSION_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class COMPRESSION_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
    public compressionConfigs: CompressionConfig[] = []
    public getCompressionConfigs(): CompressionConfig[] { return [...this.compressionConfigs] }

    public configureCompression(config: CompressionConfig): void {
        this.compressionConfigs.push(config);
    }
}
// #endregion
