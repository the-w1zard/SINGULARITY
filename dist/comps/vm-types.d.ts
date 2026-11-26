export declare const enum VM_OP {
    PUSH_CONST = 0,
    PUSH_CELL = 1,
    ADD = 2,
    SUB = 3,
    MUL = 4,
    DIV = 5,
    RET = 7,
    SUM_RANGE = 6,
    AVG_RANGE = 8,
    MIN_RANGE = 9,
    MAX_RANGE = 10,
    COUNT_RANGE = 11
}
export interface Program {
    code: Int32Array;
    consts: Float64Array;
    ranges: Int32Array[];
}
//# sourceMappingURL=vm-types.d.ts.map