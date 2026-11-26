export declare const KV_RPC_OP: {
    readonly GET: 1;
    readonly SET: 2;
    readonly DELETE: 3;
    readonly UPDATE: 4;
    readonly SUBSCRIBE: 5;
    readonly UNSUBSCRIBE: 6;
    readonly SNAPSHOT: 7;
    readonly CHANGE: 8;
    readonly ERROR: 9;
};
export type KV_RPC_OP_VALUE = typeof KV_RPC_OP[keyof typeof KV_RPC_OP];
//# sourceMappingURL=op.d.ts.map