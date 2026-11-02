// electrons/waiter.ts
//                                    WAITER_ELECTRON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class WAITER_ELECTRON {
    public root_cell_key: ATOM_KEY;
    public waiters: Set<{ resolve: (value: A) => void; reject: (reason?: A) => void }> = new Set();
    constructor(root_cell_key: ATOM_KEY, _config: Record<string, unknown> = {}) {
        this.root_cell_key = root_cell_key;
    }
    public wait<T = A>(timeout?: number): Promise<T> {
        return new Promise<T>((resolve, reject) => {
            const waiter = {
                resolve: resolve as (value: A) => void,
                reject,
            };
            this.waiters.add(waiter);
            if (timeout !== undefined && timeout > 0) {
                const timer = setTimeout(() => {
                    if (this.waiters.delete(waiter)) reject(new Error("SOUL_ELECTRON wait timed out"));
                }, timeout);
                const originalResolve = waiter.resolve;
                waiter.resolve = value => {
                    clearTimeout(timer);
                    originalResolve(value);
                };
                const originalReject = waiter.reject;
                waiter.reject = reason => {
                    clearTimeout(timer);
                    originalReject(reason);
                };
            }
        });
    }
    public resolve<T = A>(value: T): number {
        const waiters = [...this.waiters];
        this.waiters.clear();
        for (const waiter of waiters) waiter.resolve(value as A);
        return waiters.length;
    }
    public reject(reason: A): number {
        const waiters = [...this.waiters];
        this.waiters.clear();
        for (const waiter of waiters) waiter.reject(reason);
        return waiters.length;
    }
    public purge(): void {
        this.reject(new Error("SOUL_ELECTRON waiter ELECTRON destroyed"));
    }
}
// #endregion
