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
interface BUS_CHANNEL {
	handler?: BUS_HANDLER<any, any>;
	push?: Set<(msg: SIG_BUS_Message1) => void>;
}
export class G_BUS<Z extends string = string, S extends string = string> {// 3.03B OPS/SEC
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
    public s: S;
    public m: S;
    public r: S;
    public k: S;
    public channels: Record<S, A>= Object.create(null);
    constructor() { }
    // const msg: SIG_BUS_Message1 = { r, m, s, k, p: { p }, ts: Date.now(), _ack: false, };
    // const result = handler(p, msg);
    public send<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
        const channel = this.channels[k];
        const handler = channel?.handler;
        if (!handler) return undefined;
        return handler(p, { r, m, s, k, p: p });
    }
    public hot_send<T = U>(p: U, k: S,  path: S ): T | P<T> | UD {
        return this.channels[k]?.handler({ p: p });
    }
    public sendStream<T = U>(p: U, k: S, s: S = this.s, m: S = this.m, r: S = this.r): T | P<T> | UD {
        const channel = this.channels[k];
        const handler = channel?.handler;
        if (!handler) return undefined;
        const msg: SIG_BUS_Message1 = { r, m, s, k, p: p, _ack: false };
        const result = handler(p, msg);
        const listeners = channel.push;
        if (listeners) {
            for (const listener of listeners) listener(msg);
        }
        return result as T | P<T> | UD;
    }
    public sub<P = U, T = U>(handler: BUS_HANDLER<P, T>, k: S, s: S = this.s, m: S = this.m, r: S = this.r): () => V {
        const channel = (this.channels[k] ??= {});
        channel.handler = handler;
        return () => {
            if (channel.handler === handler) {
                channel.handler = undefined;
            }
        };
    }
    public stream(handler: (msg: SIG_BUS_Message1) => V, k: S, s: S, m?: S, r?: S): () => V;
    public stream(k: S, s: S, m?: S, r?: S): AsyncIterableIterator<SIG_BUS_Message1>;
    public stream(handlerOrSlave: ((msg: SIG_BUS_Message1) => V) | S, k: S, s: S = this.s, m: S = this.m, r: S = this.r): (() => V) | AsyncIterableIterator<SIG_BUS_Message1> {
        const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
        const global = globalThis as any;
        const channel = (this.channels[k] ??= {});

        if (!channel) throw new Error("Channel does not exist");

        const listeners = (channel.push ??= new Set());

        if (handler) {
            listeners.add(handler);

            return () => {
                listeners.delete(handler);
                if (!listeners.size) channel.push = undefined;
            };
        }

        const queue: SIG_BUS_Message1[] = [];
        const waiters: Array<{ resolve: (value: IteratorResult<SIG_BUS_Message1>) => void }> = [];

        let offset = 0;
        let closed = false;

        const listener = (msg: SIG_BUS_Message1) => {
            if (waiters.length) {
                waiters.shift()!.resolve({ value: msg, done: false });
            } else {
                queue.push(msg);
            }
        };

        listeners.add(listener);

        const close = () => {
            if (closed) return;
            closed = true;

            listeners.delete(listener);
            if (!listeners.size) channel.push = undefined;

            for (const waiter of waiters) {
                waiter.resolve({ value: undefined, done: true });
            }

            waiters.length = 0;
            queue.length = 0;
            offset = 0;
        };

        const iterator: AsyncIterableIterator<SIG_BUS_Message1> = {
            next: (): Promise<IteratorResult<SIG_BUS_Message1>> => {
                if (offset < queue.length) {
                    const value = queue[offset++];

                    if (offset === queue.length) {
                        queue.length = 0;
                        offset = 0;
                    }

                    return Promise.resolve({ value, done: false });
                }

                if (closed) {
                    return Promise.resolve({ value: undefined, done: true });
                }

                return new Promise((resolve) => waiters.push({ resolve }));
            },

            return: async () => {
                close();
                return { value: undefined, done: true };
            },

            [Symbol.asyncIterator](): AsyncIterableIterator<SIG_BUS_Message1> {
                return iterator;
            },
        };

        return iterator;
    }
    public duplex(k: S, s: S = this.s, m: S = this.m, r: S = this.r) {
        return {
            send: <T = U, P = U>(p: P) => this.sendStream<T>(p, k, s, m, r),
            sub: <P = U, T = U>(handler: BUS_HANDLER<P, T>) => this.sub<P, T>(handler, k, s, m, r),
            stream: (handler?: (msg: SIG_BUS_Message1) => V) => (handler ? this.stream(handler, k, s, m, r) : this.stream(k, s, m, r)),
        };
    }
}
