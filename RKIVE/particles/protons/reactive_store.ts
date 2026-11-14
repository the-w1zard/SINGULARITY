// protons/reactive_store.ts
export interface RiftRecord<T = A> {
    opened: B;
    claimed: B;
    value?: T;
    createdAt: N;
    openedAt?: N;
}

type StoreListener<T = A> = (v: RiftRecord<T>) => void;

export class REACTIVE {
    private records = new Map<S, RiftRecord<A>>();
    private listeners = new Map<S, Set<StoreListener<A>>>();

    get<T = A>(key: S): RiftRecord<T> | undefined { return this.records.get(key) as RiftRecord<T> | undefined; }
    set<T = A>(key: S, record: RiftRecord<T>): void {
        this.records.set(key, record as RiftRecord<A>);
        const subs = this.listeners.get(key);
        if (subs) for (const l of [...subs]) { try { l(record as RiftRecord<A>); } catch { /* one bad listener never kills the rest */ } }
    }
    sub<T = A>(key: S, fn: StoreListener<T>): () => V {
        let subs = this.listeners.get(key);
        if (!subs) { subs = new Set(); this.listeners.set(key, subs); }
        subs.add(fn as StoreListener<A>);
        return () => {
            const s = this.listeners.get(key);
            if (!s) return;
            s.delete(fn as StoreListener<A>);
            if (!s.size) this.listeners.delete(key);
        };
    }
    delete(key: S): void { this.records.delete(key); this.listeners.delete(key); }
    clear(): void { this.records.clear(); this.listeners.clear(); }
    get size(): N { return this.records.size; }
}

