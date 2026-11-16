import { VM_OP, type Program } from "./vm-types";

let f64: Float64Array, pidToSlot: Int32Array, levelCounter: Int32Array, vm: CellVM;
const programs: (Program | undefined)[] = [];

class CellVM {
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
					const slot = Atomics.load(this.pidToSlot, arg); // kept atomic: cheap, conservative
					this.#stack[sp++] = slot < 0 ? NaN : this.f64[slot]; // plain read — barrier covers it
					break;
				}
				case VM_OP.ADD: this.#stack[sp - 2] += this.#stack[--sp]; break;
				case VM_OP.SUB: this.#stack[sp - 2] -= this.#stack[--sp]; break;
				case VM_OP.MUL: this.#stack[sp - 2] *= this.#stack[--sp]; break;
				case VM_OP.DIV: this.#stack[sp - 2] /= this.#stack[--sp]; break;
				case VM_OP.RET: return this.#stack[0];
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
			if (slot >= 0) f64[slot] = vm.run(program); // plain write, same barrier argument as above
		}
		Atomics.add(levelCounter, 0, -pids.length);
		Atomics.notify(levelCounter, 0);
	}
};
