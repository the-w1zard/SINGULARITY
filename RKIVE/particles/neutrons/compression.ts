// neutrons/compression.ts
//  ND                                  COMPRESSION_SHARD
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class COMPRESSION_NEUTRON {
    k: config.k ?? "__SIGNALS__",
    s: config.s ?? "SAURON_SLAVE_REG",
    m: config.m ?? "SAURON_MASTER_REG",
    r: config.r ?? "THE_WIZARD_ROOT_REG",

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
