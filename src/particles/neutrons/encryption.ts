// neutrons/encryption.ts

//  ND                                  ENCRYPTION_NEUTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class ENCRYPTION_NEUTRON {
	public root_cell_key: ATOM_KEY;
	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}
    public encryptionConfigs: EncryptionConfig[] = []
    public getEncryptionConfigs(): EncryptionConfig[] { return [...this.encryptionConfigs] }
    public configureEncryption(config: EncryptionConfig): void {
        this.encryptionConfigs.push(config);
    }
}
// #endregion
