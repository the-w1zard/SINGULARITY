// electrons/injection_codec.ts


export class INJECTION_CODEC_ELECTRON {
    /*  =============================================================================
         Flat, zero-heap Outbound Notification & Scope Registry. Replaces Sauron nested 
         Map trees with direct PID-indexed arrays.
        ================================= ⇦✦──⇦✦──✦──✦──←'pew pew' 🧙 [INJECTION_CODEC] */
        private ids = new Map<S, N>();
        private names: S[] = [];
        private pids = new Map<S, N>();          // "a:b:c:d" → pid
        private addresses = new Map<N, [N, N, N, N]>(); // pid → [a,b,c,d]
        private nextId = 0;
        // ── hot path (was INJECTION_CODEC) — SoA, per-pid, high volume ──
        public readonly maxPids: N;
        public readonly listeners: Array<ListenerFn | UD>;
        public readonly pidToHash: U32;          // keep? see note
        public nextAllocatedPid = 1;             // 0 = TRACER_PID
        public readonly channelMap: I32;
        public readonly subChannelMap: I32;
        public readonly tripChannelMap: I32;
        public readonly dirtyPids: I32;
        public dirtyCount = 0;
        public _flushScheduled = false;
    
        constructor(maxPids = 8192) {
            this.maxPids = maxPids;
            this.listeners = new Array(maxPids);
            this.dirtyPids = new Int32Array(maxPids);
            this.pidToHash = new Uint32Array(maxPids);
            this.channelMap = new Int32Array(maxPids);
            this.subChannelMap = new Int32Array(maxPids);
            this.tripChannelMap = new Int32Array(maxPids);
        }
    
        private intern(value: S): N {
            let id = this.ids.get(value);
            if (id === undefined) {
                id = this.nextId++;
                this.ids.set(value, id);
                this.names[id] = value;
            }
            return id;
        }
    
        /** Exact identity: encode address → pid (interned, no collisions). */
        public encode(pid_key: S, sl: S, ms: S, rt: S): N {
            const a = this.intern(rt);
            const b = this.intern(ms);
            const c = this.intern(sl);
            const d = this.intern(pid_key);
            const addressKey = `${a}:${b}:${c}:${d}`;
    
            let pid = this.pids.get(addressKey);
            if (pid === undefined) {
                pid = this.nextAllocatedPid++;
                if (pid >= this.maxPids) throw new Error("INJECTION_CODEC: PID capacity exceeded");
                this.pids.set(addressKey, pid);
                this.addresses.set(pid, [a, b, c, d]);
                // populate SoA lanes from the interned ids (no re-hash)
                this.channelMap[pid] = a;
                this.subChannelMap[pid] = b;
                this.tripChannelMap[pid] = c;
            }
            return pid;
        }
    
        /** Exact decode: pid → address. */
        public decode(pid: N): { key: S; sl: S; ms: S; rt: S } {
            const addr = this.addresses.get(pid);
            if (!addr) throw new Error(`INJECTION_CODEC: unknown pid ${pid}`);
            const [a, b, c, d] = addr;
            return { rt: this.names[a], ms: this.names[b], sl: this.names[c], key: this.names[d] };
        }
    
        public parseScopeKey(pid: N): ScopeComponents {
            FLYWEIGHT_SCOPE.r = this.channelMap[pid];
            FLYWEIGHT_SCOPE.m = this.subChannelMap[pid];
            FLYWEIGHT_SCOPE.s = this.tripChannelMap[pid];
            return FLYWEIGHT_SCOPE;
        }
    
        public markDirty(pid: N): V { this.dirtyPids[this.dirtyCount++] = pid; }
    
        public subscribe(pid: N, fn: ListenerFn): () => V {
            this.listeners[pid] = fn;
            return () => { this.listeners[pid] = undefined; };
        }
    
        public flushNotifications(): V {
            this._flushScheduled = false;
            for (let i = 0; i < this.dirtyCount; i++) {
                const pid = this.dirtyPids[i];
                const fn = this.listeners[pid];
                if (fn !== undefined) fn(pid);
            }
            this.dirtyCount = 0;
        }
    
        public notifyHook(key: S): V {
            const pid = this.encode(key, "", "", "");
            this.markDirty(pid);
            if (!this._flushScheduled) {
                this._flushScheduled = true;
                queueMicrotask(() => { this._flushScheduled = false; this.flushNotifications(); });
            }
        }
    
        public notifyPid(pid: N, value: N): V {
            const fn = this.listeners[pid];
            if (fn !== undefined) fn(pid, value);
        }
    
        public releasePid(pid: N): V {
            const addr = this.addresses.get(pid);
            if (addr) {
                this.pids.delete(`${addr[0]}:${addr[1]}:${addr[2]}:${addr[3]}`);
                this.addresses.delete(pid);
            }
            this.pidToHash[pid] = 0;
        }
    }
