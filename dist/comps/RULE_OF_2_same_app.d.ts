export declare class BinaryEncoder {
    private static topicToId;
    private static idToTopic;
    static hashTopic(topic: string): number;
    static getTopicId(topic: string): number;
    static getTopicName(topicId: number): string | undefined;
    static registerTopics(topics: string[]): void;
    static resetRegistry(): void;
}
export declare class BinaryFrameSerializer {
    private static textEncoder;
    private static headerBuffer;
    private static headerView;
    private static headerBytes;
    static encodeFrame(topic: S, p: U8 | S | Record<S, U>): U8;
    /**
     * Zero-GC binary frame decode.
     * Mutates and returns a shared FLYWEIGHT_FRAME object.
     */
    static decodeFrame(frameBytes: Uint8Array): DecodedFrame;
}
export interface HeaderViews {
    headerInt32: Int32Array;
    payloadUint8: Uint8Array;
    capacity: number;
}
export interface ScopeComponents {
    r: number;
    m: number;
    s: number;
}
export interface DecodedFrame {
    topicId: number;
    topicName: string | undefined;
    p: Uint8Array;
}
export interface PeerRegistryEntry {
    addressSpaceId: string;
    processId: string;
    boundaryType: BoundaryType;
    container?: ContainerKind;
    k: string;
    s: string;
    m: string;
    r: string;
    capacityBytes: number;
    ring?: RingPairDescriptor;
}
export interface RingPairDescriptor {
    aToB: SharedArrayBuffer;
    bToA: SharedArrayBuffer;
}
export interface RingPairDescriptor {
    aToB: SharedArrayBuffer;
    bToA: SharedArrayBuffer;
}
export interface PeerRegistryEntry {
    addressSpaceId: string;
    processId: string;
    boundaryType: BoundaryType;
    container?: ContainerKind;
    k: string;
    s: string;
    m: string;
    r: string;
    capacityBytes: number;
    ring?: RingPairDescriptor;
}
export type BoundaryType = 'SAME_APP' | 'SAME_NODE' | 'CROSS_NODE';
export type ContainerKind = 'WORKER' | 'PROCESS';
export interface RingPairDescriptor {
    aToB_fast: SharedArrayBuffer;
    bToA_fast: SharedArrayBuffer;
    aToB_overflow: SharedArrayBuffer;
    bToA_overflow: SharedArrayBuffer;
}
export interface PeerRegistryEntry {
    addressSpaceId: string;
    processId: string;
    boundaryType: BoundaryType;
    container?: ContainerKind;
    k: string;
    s: string;
    m: string;
    r: string;
    capacityBytes: number;
    ring?: RingPairDescriptor;
}
export interface HeaderViews {
    headerInt32: Int32Array;
    payloadUint8: Uint8Array;
    capacity: number;
}
export declare const HEADER_SIZE_BYTES = 64;
export declare const HEADER_IDX_WRITE_HEAD = 0;
export declare const HEADER_IDX_READ_HEAD = 1;
export declare const HEADER_IDX_MUTEX = 2;
export declare const HEADER_IDX_CAPACITY = 3;
export declare const MUTEX_UNLOCKED = 0;
export declare const MUTEX_LOCKED = 1;
export declare const SENTINEL_WRAP_MARKER = 4294967295;
export declare function createSharedDepotBuffer(capacityBytes: number): SharedArrayBuffer;
export declare function mapDepotBuffer(sab: SharedArrayBuffer): HeaderViews;
export declare function validateHeader(sab: SharedArrayBuffer): boolean;
export declare function calculateFrameFootprint(payloadSizeBytes: number): number;
export declare class RING_BUFFER {
    private headerInt32;
    private payloadUint8;
    private payloadDataView;
    readonly capacity: number;
    constructor(sab: SharedArrayBuffer);
    private acquireLock;
    private releaseLock;
    write(p: Uint8Array): boolean;
    read(): Uint8Array | null;
    waitForDataAsync(timeoutMs?: number): Promise<'ok' | 'not-equal' | 'timed-out'>;
}
export declare function createSharedDisruptorBuffer(capacity: number, maxPayloadBytes: number): SharedArrayBuffer;
export declare function validateDisruptorHeader(sab: SharedArrayBuffer): boolean;
export declare class POWER_OF_TWO {
    readonly capacity: number;
    readonly mask: number;
    readonly slotPayloadBytes: number;
    readonly pids: Int32Array;
    readonly values: Float64Array;
    readonly lengths: Int32Array;
    readonly flags: Uint8Array;
    readonly payload: Uint8Array;
    private readonly header;
    constructor(sab: SharedArrayBuffer);
    publish(pid: number, value: number, flag?: number, payload?: Uint8Array): boolean;
    consume(handler: (pid: number, value: number, flag: number, payload: Uint8Array) => void): number;
    waitForDataAsync(timeoutMs?: number): Promise<'ok' | 'not-equal' | 'timed-out'>;
    reset(): void;
}
export declare class HybridChannel {
    private readonly fast;
    private readonly overflow;
    private fastDrops;
    private overflowDrops;
    constructor(fast: POWER_OF_TWO, overflow: RING_BUFFER);
    publish(topicId: number, payload: Uint8Array, value?: number, flag?: number): boolean;
    consume(handler: (topicId: number, payload: Uint8Array, value: number, flag: number) => void): number;
    drainLoop(handler: (topicId: number, payload: Uint8Array, value: number, flag: number) => void, signal?: AbortSignal): Promise<void>;
    get dropStats(): {
        fastDrops: number;
        overflowDrops: number;
    };
}
export declare class RingPair {
    readonly tx: HybridChannel;
    readonly rx: HybridChannel;
    readonly descriptor: RingPairDescriptor;
    private constructor();
    static create(capacity: number, maxPayloadBytes: number, overflowCapacityBytes: number): RingPair;
    static attach(descriptor: RingPairDescriptor): RingPair;
}
type BusSend = (msg: unknown, k: S, s: S, m: S, r: S) => void;
type BusSub = (handler: A, k: S, s: S, m: S, r: S) => void;
export declare class ControlPlaneBootstrapper {
    private localAddressSpaceId;
    private k;
    private s;
    private m;
    private r;
    private onPromote;
    private send;
    private sub;
    private peerRegistry;
    constructor(localAddressSpaceId: string, k: string, s: string, m: string, r: string, onPromote: (peer: PeerRegistryEntry, pair: RingPair) => void, send: BusSend, sub: BusSub);
    announceDepot(capacity: number, maxPayloadBytes: number, overflowCapacityBytes: number): RingPair;
    private promoteRouteToDataPlane;
}
export {};
//# sourceMappingURL=RULE_OF_2_same_app.d.ts.map