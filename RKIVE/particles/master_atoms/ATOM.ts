//                                                                                                  ATOM
import { GATE_ELECTRON } from "../electrons/gate";
import { SEALED_ERROR_ELECTRON } from "../electrons/sealed_error";
import { SUBSCRIPTION_ELECTRON } from "../electrons/subscription";
import type { ATOM_CLASS, ATOM_KEY } from "../factory/types";

// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class ATOM<X = U> {
	public k: A = "__SIGNALS__";
	public s: S;
	public m: S;
	public r: S;
	public value: U;
	constructor( value: X, k: A = "__SIGNALS__", s: S = "SAURON_SLAVE_REG", m: S = "SAURON_MASTER_REG", r: S = "THE_WIZARD_ROOT_REG", c) {
		this.k = k;
		this.s = s;
		this.m = m;
		this.r = r;
		this.value = value;
	} 
	read(): X {
		const r = this.registry;
		if (r !== null) {
			const gate = r.get(GATE_ELECTRON);
			if (gate !== undefined && (gate as any).sealed) throw new SEALED_ERROR_ELECTRON(this.key);
		}
		return this.value;
	}
	write(next: X): B {
		const r = this.registry;
		if (r === null) {
			this.value = next;
			return true;
		}
		const gate = r.get(GATE_ELECTRON);
		if (gate !== undefined && (gate.sealed || gate.frozen)) return false;
		const previous = this.value;
		if (Object.is(previous, next)) return false;
		this.value = next;
		const subs = r.get(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD;
		if (subs !== undefined) subs.notify({ key: this.key, previous, current: next });
		return true;
	}
	subscribe(listener: LISTENER<X>): UNSUBSCRIBE {
		const subs =
			(this.shard(SUBSCRIPTION_ELECTRON) as SUBSCRIPTION_ELECTRON<X> | UD) ??
			this.bind(new SUBSCRIPTION_ELECTRON<X>(this));
		return subs.subscribe(listener);
	}
	waitFor() { }
}

// #endregion
