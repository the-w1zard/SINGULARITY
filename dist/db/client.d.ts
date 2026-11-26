type PendingRequest = {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
};
export declare class WAL_KVDB_CLIENT {
    #private;
    private nextId;
    private readonly timeoutMs;
    pending: Record<string, PendingRequest>;
    s: S;
    m: S;
    r: S;
    client: A;
    constructor(client: A, s: S, m: S, r: S);
    hot_cache: Record<string, unknown>;
    get: <T = unknown>(key: string) => Promise<T | undefined>;
    set: (key: string, value: Uint8Array | string, options?: {
        durable?: boolean;
    }) => Promise<void>;
    delete: (key: string, options?: {
        durable?: boolean;
    }) => Promise<boolean>;
    update: <T = unknown>(key: string, updater: (previous: T | undefined) => T) => Promise<T>;
    has: (key: string) => Promise<boolean>;
}
export {};
//# sourceMappingURL=client.d.ts.map