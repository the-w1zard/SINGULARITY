// electrons/soa_quad_tiered_reg.ts
// 2. THE UNIFIED REGISTRY (Handles Single Strings AND Multi-Channel Addresses)
export class SOA_QUAD_TIERED_REGISTRY_ELECTRON {
    private stringToId = new Map<string, number>();
    private idToString: string[] = [];
    private addressToPidMap = new Map<string, number>();
    public nextPid = 1;

    // SoA Memory Lanes (Indexed by PID)
    public readonly typeMap: Uint8Array;       // 0 = Single String, 1 = Multi-Channel
    public readonly channelMap: Int32Array;    // r channel
    public readonly subChannelMap: Int32Array; // m channel
    public readonly tripChannelMap: Int32Array;// s channel
    public readonly keyMap: Int32Array;        // k channel / single key string ID

    // Zero-alloc flyweight for multi-channel decoding
    public readonly multiFlyweight = { r: "", m: "", s: "", k: "" };

    constructor(maxPids = 65536) {
        this.typeMap = new Uint8Array(maxPids);
        this.channelMap = new Int32Array(maxPids);
        this.subChannelMap = new Int32Array(maxPids);
        this.tripChannelMap = new Int32Array(maxPids);
        this.keyMap = new Int32Array(maxPids);
    }

    private intern(val: string): number {
        let id = this.stringToId.get(val);
        if (id === undefined) {
            id = this.idToString.length;
            this.stringToId.set(val, id);
            this.idToString[id] = val;
        }
        return id;
    }

    /**
     * Input Style A: Register a SINGLE string key -> returns a PID.
     */
    public resolveSingle(key: string): number {
        let pid = this.addressToPidMap.get(key);
        if (pid === undefined) {
            pid = this.nextPid++;
            this.addressToPidMap.set(key, pid);

            this.typeMap[pid] = 0; // Mark as Single String
            this.keyMap[pid] = this.intern(key);
        }
        return pid;
    }

    /**
     * Input Style B: Register 3-4 channel strings -> returns a PID.
     */
    public resolveMulti(r: string, m: string, s: string, k = ""): number {
        const addressKey = `${r}:${m}:${s}:${k}`;
        let pid = this.addressToPidMap.get(addressKey);
        if (pid === undefined) {
            pid = this.nextPid++;
            this.addressToPidMap.set(addressKey, pid);

            this.typeMap[pid] = 1; // Mark as Multi-Channel
            this.channelMap[pid] = this.intern(r);
            this.subChannelMap[pid] = this.intern(m);
            this.tripChannelMap[pid] = this.intern(s);
            this.keyMap[pid] = this.intern(k);
        }
        return pid;
    }

    /**
     * Decode PID back into a Single String (if registered that way).
     */
    public decodeSingle(pid: number): string {
        return this.idToString[this.keyMap[pid]] ?? "";
    }

    /**
     * Decode PID back into 3-4 Channels (zero-alloc flyweight mutation).
     */
    public decodeMulti(pid: number): typeof this.multiFlyweight {
        this.multiFlyweight.r = this.idToString[this.channelMap[pid]] ?? "";
        this.multiFlyweight.m = this.idToString[this.subChannelMap[pid]] ?? "";
        this.multiFlyweight.s = this.idToString[this.tripChannelMap[pid]] ?? "";
        this.multiFlyweight.k = this.idToString[this.keyMap[pid]] ?? "";
        return this.multiFlyweight;
    }
}
