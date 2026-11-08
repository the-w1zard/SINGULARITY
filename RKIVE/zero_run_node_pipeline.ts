


const RESERVED = new Set(["value", "key", "docs", "atom"]);

export function zeroRunNodePipeline(key: string, nodeDef: any, siblings: Record<string, any>, location: any, categoryLabel: string,): any {
    nodeDef.__location = location;
    nodeDef.__definitionName = key;
    if (nodeDef.__isAtomContract) {
        nodeDef = zeroAtomContractPipeline({
          key,
          nodeDef,
          siblings,
          location,
          categoryLabel,
        });
      }
    // #endregion
    return nodeDef;
}
export function zeroAtomContractPipeline({
    key,
    nodeDef,
    location,
    categoryLabel,
  }: any) {
    const contract = nodeDef.__atomContract;
  
    const masterKind = contract.atom ?? "VERTEX";
  
    if (!MASTER_ATOM_KEYS.includes(masterKind)) {
      throw new Error(
        `[${categoryLabel}] "${key}": unknown master atom "${masterKind}".\n` +
        `Allowed: ${MASTER_ATOM_KEYS.join(", ")}\n` +
        `Defined at: ${location}`
      );
    }
  
    const value = resolveInitialValue(contract.value);
    const atomKey = contract.key ?? key;
  
    const existing = BC().ATOM_AT("root_cell", atomKey);
    if (existing) return existing;
  
    const master = createMasterAtom(masterKind, atomKey, value);
  
    BC().ATOM_PUT("root_cell", atomKey, master);
  
    const bindings = collectAtomBindings({
      contract,
      masterKey: atomKey,
      label: key,
      categoryLabel,
      location,
    });
  
    if (bindings.length > 0) {
      master.registry = new REGISTRY_PROTON(atomKey, {
        atoms: bindings,
      });
    }
  
    return master;
  }
  function collectAtomBindings({
    contract,
    masterKey,
    label,
    categoryLabel,
    location,
  }: any) {
    const bindings = [];
  
    for (const name of Object.keys(contract)) {
      if (RESERVED.has(name)) continue;
  
      const spec = contract[name];
  
      if (spec === undefined || spec === null || spec === false) continue;
  
      // 1. Bespoke particle factory
      if (typeof spec === "function") {
        bindings.push({
          atom: spec(masterKey),
          owner: "bespoke",
          role: inferRole(name),
        });
        continue;
      }
  
      // 2. Shared atom
      if (typeof spec === "object" && "root_cell_key" in spec) {
        bindings.push({
          atom: spec,
          owner: "shared",
          role: inferRole(name),
        });
        continue;
      }
  
      // 3. Platform particle
      const Particle = PARTICLE_KEYS[name];
  
      if (!Particle) {
        throw new Error(
          `\n\x1b[31m[${categoryLabel} Contract Error]\x1b[0m ` +
          `Unknown particle "${name}" on master atom "${label}".\n` +
          `  -> Available particles: ${Object.keys(PARTICLE_KEYS).join(", ")}\n` +
          `  -> Defined at: ${location}\n`
        );
      }
  
      bindings.push({
        atom: new Particle(masterKey, spec === true ? {} : spec),
        owner: "bespoke",
        role: inferRole(name),
      });
    }
  
    return bindings;
  }
  export function DEFINE_ATOM_KIND(name: string): symbol {
	return Symbol(name);
}
  function inferRole(particleName:S) {
    if (particleName in PROTONS_DEFINITION) return "proton";
    if (particleName in NEUTRONS_DEFINITION) return "neutron";
    if (particleName in ELECTRON_DEFINITION) return "electron";
    if (particleName in MQ_DEFINITION) return "neutron";
    return "electron";
  }
