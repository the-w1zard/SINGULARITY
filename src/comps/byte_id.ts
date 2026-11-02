export class BYTE_ID {
/*  ==========================================================================================================================
     provides lightweight numeric and string ID generation, using a Uint32Array counter for incrementing byte-level IDs and 
     a separate counter for string IDs. It also preallocates fixed-size timestamp and resolver arrays to 
     track up to 4,096 pending operations.

     The update byteid function now shares the counter with sid, the string variant. At the smae time, a rollover counter that 
     increments whenever the 32-bit counter wraps. Keeping the fact U32Array counter while producing ids that remain unique.
    ================================================================================ ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [BYTE_ID] */
    public MAX_PENDING = 4096;
    public sentTimestamps;
    public pendingResolvers;
    public cursor = 0;
    private readonly counter: U32;
    private epoch = 0;
    public _byteid(): string {
        const g = globalThis as A;
        const c = g._sauronIdCounter ?? (g._sauronIdCounter = new Uint32Array(1));
        if (++c[0] === 0) this.epoch++;
        g._sauronIdCounter = c;
        return `${this.epoch}:${c[0]}`;
    }
    public b(): string {
        return  this._byteid()
    }
    constructor() {
        this.counter = new Uint32Array(1);
        this.sentTimestamps = new Float64Array(this.MAX_PENDING);
        this.pendingResolvers = new Array(this.MAX_PENDING);
    }
}


 function _sid(): string {
    const g = globalThis as A;
    const t = String(((g._sauronIdCounter ?? 0) as N) + 1);
    g._sauronIdCounter = t;
    return t;
}

    // public _stringIdCounter = 0;
    //  const id = this.counter[0] + 1;
