type BUS_HANDLER<P = U, T = U> = (p?: P, msg?: SIG_BUS_Message1) => T | Promise<T>;
interface SIG_BUS_Message1 {
    id?: string;
    r?: S;
    m?: S;
    s: S;
    k: S;
    p?: any;
    ts?: number;
    _ack?: boolean;
    _reply?: boolean;
    _rid?: string;
    ackTo?: string;
}
export declare class G_BUS<Z extends string = string, S extends string = string> {
    /**
    Several peak runs followed by usage paths
    Peak:                                       1_228.50 M ops/sec
    Peak:                                       3_103.41 M ops/sec
    Peak:                                       2_370.05 M ops/sec
    Peak:                                       2_325.32 M ops/sec
    These runs were all

    FELKOR2.send
    sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
    12    |   1146.44      |   1146.44    |   1146.44   |    100.0
    
    FELKOR2.stream
    sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
    12    |   1319.94      |   1319.94    |   1319.94   |    100.0
    PS F:\mq\src> bun run aa.ts
    
    FELKOR2.iterator.publish
    sec   |   M writes/s   |   best M/s   |   avg M/s   |    CPU %
    12    |     540.61     |   540.61     |  540.61     |    100.3
    PS F:\mq\src> bun run aa.ts
    
    FELKOR2.duplex
    sec | M writes/s | best M/s | avg M/s | CPU %
    12 |     898.32 |   898.32 |  898.32 | 100.0
    */
    s: S;
    m: S;
    r: S;
    k: S;
    channels: Record<S, A>;
    constructor();
    send<T = U>(p: U, k: S, s?: S, m?: S, r?: S): T | P<T> | UD;
    hot_send<T = U>(p: U, k: S, path: S): T | P<T> | UD;
    sendStream<T = U>(p: U, k: S, s?: S, m?: S, r?: S): T | P<T> | UD;
    sub<P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s?: S, m?: S, r?: S): () => V;
    stream(handler: (msg: SIG_BUS_Message1) => V, k: S, s: S, m?: S, r?: S): () => V;
    stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<SIG_BUS_Message1>;
    duplex(k: S, s?: S, m?: S, r?: S): {
        send: <T = unknown, P = unknown>(p: P) => T | globalThis.P<T>;
        sub: <P = unknown, T = unknown>(handler: BUS_HANDLER<P, T>) => () => V;
        stream: (handler?: (msg: SIG_BUS_Message1) => V) => AsyncIterableIterator<SIG_BUS_Message1> | (() => V);
    };
}
export {};
//# sourceMappingURL=testt.d.ts.map