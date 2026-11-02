// electrons/signal.ts

//  ND                                  ATOM_ELECTRON
// SPECIALIZING IN THE STORAGE OF ATOM RELATED VALUES, META DATA AND SO ON
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export interface ATOM_META { registeredAt: N; owner?: S; labels: Set<S>; [extra: S]: unknown; }

export class SIGNAL_ELECTRON<T=A> {
    public k: ATOM_KEY;
    public atom: T 
    public s:S 
    public m:S 
    public r:S 
    constructor(atom: T,  k: ATOM_KEY,  s: S, m:S, r:S, _config: Record<S, unknown> = {}) {
        this.atom = atom
        this.k = k;
        this.s = s;
        this.m = m;
        this.r = r;
    }
    _batches= new Set<string | symbol>()A
    _batchQueue= new Map<string | symbol, A>()
    _batchOpts= new Map<string | symbol, { triggerRender?: boolean }>()

    _listeners= new Set<(key: string | symbol, value: A) => void>()
    _global_subscribers= new Map<string, Set<() => void>>()

    _sys= new Map<string, SysEntry>()
    _sealed= new Set<string>()
    _frozen= new Set<string>()
    _keyContracts= new Map<string, Set<string>>()
    _conflicts= new Map<string, ConflictPolicy>()
    _conflictStrategies= new Map<string, 'latest' | 'fail' | 'accumulate'>()
    _causal= new Map<string, CausalEntry[]>()

    _scopes=new Map<string, SignalScope>()
    _msgHandlers= new Map<string, Set<(p: A) => void>>()
    _lastWriteSource= new Map<string, string>()
    _mainMap=new Map<A, string>()
    _tupleCache= new Map<string, string>()
    _weakMap= new WeakMap<object, string>()
    _boundary = null
    /** Write one atom. Creates the meta record on first write, updates value on later writes. */
    public get(): A | undefined {
         return this.atom
         }
    public drop(): B { 
         this.atom = null
        return true
     }
    public meta(key: S): ATOM_META | undefined { const e = this.atoms.get(key); if (!e) return undefined; return { ...e.meta, labels: new Set(e.meta.labels) }; }
    public label(key: S, label: S): B { const e = this.atoms.get(key); if (!e) return false; e.meta.labels.add(label); return true; }
    public unlabel(key: S, label: S): B { return this.atoms.get(key)?.meta.labels.delete(label) ?? false; }
    public listByLabel(label: S): S[] {
        const out: S[] = [];
        for (const [key, e] of this.atoms) if (e.meta.labels.has(label)) out.push(key);
        return out;
    }
    public list(): S[] { return [...this.atoms.keys()]; }
    public inspect(key: S): { value: A; meta: ATOM_META } | undefined { const e = this.atoms.get(key); if (!e) return undefined; return { value: e.value, meta: { ...e.meta, labels: new Set(e.meta.labels) } }; }

    purge(): void { this.atoms.clear(); }
    get value(): T {
        self._bindComponentToSignal(rk, sl, ms, rt)
        return self.read<T>(rk, sl, ms, rt) as T
    },
    public write( value: A): T {
        this.atom = typeof value === 'function' ? value() : value
        return this.atom
    }
    set value(v: T) { self.write(rk, v, 'write', sl, ms, rt) },
    peek(): T { return rk.node },
    get current(): T {
        self._bindComponentToSignal(rk, sl, ms, rt)
        return self.read<T>(rk, sl, ms, rt) as T
    },
    set current(v: T) { self.write(rk, v, 'write', sl, ms, rt) },
    set crt(v: T) { this.write(rk, v, 'write', sl, ms, rt) },
    set set(v: T) { this.write(rk, v, 'write', sl, ms, rt) },
}
// #endregion
