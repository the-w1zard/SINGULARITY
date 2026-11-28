export const KV_RPC_OP = {
    GET: 1,
    SET: 2,
    DELETE: 3,
    UPDATE: 4,
    SUBSCRIBE: 5,
    UNSUBSCRIBE: 6,
    SNAPSHOT: 7,
    CHANGE: 8,
    ERROR: 9
  } as const;
  
  export type KV_RPC_OP_VALUE = typeof KV_RPC_OP[keyof typeof KV_RPC_OP];
