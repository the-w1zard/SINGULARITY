// cell-worker.ts
import { VM_OP, type Program } from "./vm-types";

let f64: Float64Array, pidToSlot: Int32Array, levelCounter: Int32Array, vm: CellVM;
const programs: (Program | undefined)[] = [];

export class CellVM {
	#stack = new Float64Array(256);
	constructor(private f64: Float64Array, private pidToSlot: Int32Array) {}
	run(p: Program): N {
		let sp = 0;
		const { code, consts } = p;
		for (let ip = 0; ip < code.length; ip += 2) {
			const op = code[ip], arg = code[ip + 1];
			switch (op) {
				case VM_OP.PUSH_CONST: this.#stack[sp++] = consts[arg]; break;
				case VM_OP.PUSH_CELL: {
					const slot = Atomics.load(this.pidToSlot, arg);
					this.#stack[sp++] = slot < 0 ? NaN : this.f64[slot];
					break;
				}
				case VM_OP.ADD: this.#stack[sp - 2] += this.#stack[--sp]; break;
				case VM_OP.SUB: this.#stack[sp - 2] -= this.#stack[--sp]; break;
				case VM_OP.MUL: this.#stack[sp - 2] *= this.#stack[--sp]; break;
				case VM_OP.DIV: this.#stack[sp - 2] /= this.#stack[--sp]; break;
				case VM_OP.RET: return this.#stack[0];
                case VM_OP.SUM_RANGE: { let s = 0; for (const pid of p.ranges[arg]) { const sl = Atomics.load(this.pidToSlot, pid); if (sl >= 0) s += this.f64[sl]; } this.#stack[sp++] = s; break; }
                case VM_OP.AVG_RANGE: { let s = 0, n = 0; for (const pid of p.ranges[arg]) { const sl = Atomics.load(this.pidToSlot, pid); if (sl < 0) continue; s += this.f64[sl]; n++; } this.#stack[sp++] = n ? s / n : 0; break; }
                case VM_OP.MIN_RANGE: { let m = Infinity; for (const pid of p.ranges[arg]) { const sl = Atomics.load(this.pidToSlot, pid); if (sl >= 0) m = Math.min(m, this.f64[sl]); } this.#stack[sp++] = Number.isFinite(m) ? m : 0; break; }
                case VM_OP.MAX_RANGE: { let m = -Infinity; for (const pid of p.ranges[arg]) { const sl = Atomics.load(this.pidToSlot, pid); if (sl >= 0) m = Math.max(m, this.f64[sl]); } this.#stack[sp++] = Number.isFinite(m) ? m : 0; break; }
                case VM_OP.COUNT_RANGE: { let n = 0; for (const pid of p.ranges[arg]) if (Atomics.load(this.pidToSlot, pid) >= 0) n++; this.#stack[sp++] = n; break; }
			}
		}
		return this.#stack[0];
	}
}

self.onmessage = (e) => {
	const d = e.data;
	if (d.type === "init") {
		f64 = new Float64Array(d.f64);
		pidToSlot = new Int32Array(d.pidToSlot);
		levelCounter = new Int32Array(d.levelCounter);
		vm = new CellVM(f64, pidToSlot);
		return;
	}
	if (d.type === "register") { programs[d.pid] = d.program; return; }
	if (d.type === "computeLevel") {
		const pids: Int32Array = d.pids;
		for (let i = 0; i < pids.length; i++) {
			const pid = pids[i];
			const program = programs[pid];
			if (!program) continue;
			const slot = Atomics.load(pidToSlot, pid);
			if (slot >= 0) f64[slot] = vm.run(program);
		}
		Atomics.add(levelCounter, 0, -pids.length);
		Atomics.notify(levelCounter, 0);
	}
};
