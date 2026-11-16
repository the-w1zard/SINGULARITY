export const enum VM_OP { PUSH_CONST = 0, PUSH_CELL = 1, ADD = 2, SUB = 3, MUL = 4, DIV = 5, SUM_RANGE = 6, RET = 7 }
export interface Program { code: Int32Array; consts: Float64Array; }
