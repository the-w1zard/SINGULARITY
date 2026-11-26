import { type Program } from "./vm-types";
export declare class CellVM {
    #private;
    private f64;
    private pidToSlot;
    constructor(f64: Float64Array, pidToSlot: Int32Array);
    run(p: Program): N;
}
//# sourceMappingURL=cell-worker.d.ts.map