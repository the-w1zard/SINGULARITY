export class G_BUS {
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
    s;
    m;
    r;
    k;
    channels = Object.create(null);
    constructor() { }
    // const msg: SIG_BUS_Message1 = { r, m, s, k, p: { p }, ts: Date.now(), _ack: false, };
    // const result = handler(p, msg);
    send(p, k, s = this.s, m = this.m, r = this.r) {
        const channel = this.channels[k];
        const handler = channel?.handler;
        if (!handler)
            return undefined;
        return handler(p, { r, m, s, k, p: p });
    }
    hot_send(p, k, path) {
        return this.channels[k]?.handler({ p: p });
    }
    sendStream(p, k, s = this.s, m = this.m, r = this.r) {
        const channel = this.channels[k];
        const handler = channel?.handler;
        if (!handler)
            return undefined;
        const msg = { r, m, s, k, p: p, _ack: false };
        const result = handler(p, msg);
        const listeners = channel.push;
        if (listeners) {
            for (const listener of listeners)
                listener(msg);
        }
        return result;
    }
    sub(handler, k, s = this.s, m = this.m, r = this.r) {
        const channel = (this.channels[k] ??= {});
        channel.handler = handler;
        return () => {
            if (channel.handler === handler) {
                channel.handler = undefined;
            }
        };
    }
    stream(handlerOrSlave, k, s = this.s, m = this.m, r = this.r) {
        const handler = typeof handlerOrSlave === "function" ? handlerOrSlave : undefined;
        const global = globalThis;
        const channel = (this.channels[k] ??= {});
        if (!channel)
            throw new Error("Channel does not exist");
        const listeners = (channel.push ??= new Set());
        if (handler) {
            listeners.add(handler);
            return () => {
                listeners.delete(handler);
                if (!listeners.size)
                    channel.push = undefined;
            };
        }
        const queue = [];
        const waiters = [];
        let offset = 0;
        let closed = false;
        const listener = (msg) => {
            if (waiters.length) {
                waiters.shift().resolve({ value: msg, done: false });
            }
            else {
                queue.push(msg);
            }
        };
        listeners.add(listener);
        const close = () => {
            if (closed)
                return;
            closed = true;
            listeners.delete(listener);
            if (!listeners.size)
                channel.push = undefined;
            for (const waiter of waiters) {
                waiter.resolve({ value: undefined, done: true });
            }
            waiters.length = 0;
            queue.length = 0;
            offset = 0;
        };
        const iterator = {
            next: () => {
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
            [Symbol.asyncIterator]() {
                return iterator;
            },
        };
        return iterator;
    }
    duplex(k, s = this.s, m = this.m, r = this.r) {
        return {
            send: (p) => this.sendStream(p, k, s, m, r),
            sub: (handler) => this.sub(handler, k, s, m, r),
            stream: (handler) => (handler ? this.stream(handler, k, s, m, r) : this.stream(k, s, m, r)),
        };
    }
}
