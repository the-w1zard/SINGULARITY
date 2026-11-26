export type BoundaryType = 'SAME_APP' | 'SAME_NODE' | 'CROSS_NODE';
export type ContainerKind = 'WORKER' | 'PROCESS';
export interface HeaderViews {
    headerInt32: Int32Array;
    payloadUint8: Uint8Array;
    capacity: N;
}
export interface DecodedFrame {
    topicId: N;
    topicName: S | undefined;
    p: U8;
}
export interface RingPairDescriptor {
    aToB_fast: SharedArrayBuffer;
    bToA_fast: SharedArrayBuffer;
    aToB_overflow: SharedArrayBuffer;
    bToA_overflow: SharedArrayBuffer;
}
export interface NativeRingPairDescriptor {
    name: S;
    requestBytes: N;
    responseBytes: N;
    overflowBytes: N;
    maxPayloadBytes: N;
}
export interface NativeSharedMemoryBinding {
    create(name: S, requestBytes: N, responseBytes: N, overflowBytes: N, maxPayloadBytes: N): NativeRingPairDescriptor;
    attach(descriptor: NativeRingPairDescriptor): RingPair;
}
export interface PeerRegistryEntry {
    addressSpaceId: S;
    processId: S;
    boundaryType: BoundaryType;
    container?: ContainerKind;
    k: S;
    s: S;
    m: S;
    r: S;
    capacityBytes: N;
    ring?: RingPairDescriptor;
    native?: NativeRingPairDescriptor;
}
export declare class BinaryEncoder {
    private static topicToId;
    private static idToTopic;
    static hashTopic(topic: S): N;
    static getTopicId(topic: S): N;
    static getTopicName(topicId: N): S | undefined;
    static registerTopics(topics: AR<S>): void;
    static resetRegistry(): void;
}
export declare class BinaryFrameSerializer {
    private static textEncoder;
    private static headerBuffer;
    private static headerView;
    private static headerBytes;
    static encodeFrame(topic: S, p: U8 | S | Record<S, U>): U8;
    static decodeFrame(frameBytes: U8): DecodedFrame;
}
export declare const HEADER_SIZE_BYTES = 64;
export declare const HEADER_IDX_WRITE_HEAD = 0;
export declare const HEADER_IDX_READ_HEAD = 1;
export declare const HEADER_IDX_MUTEX = 2;
export declare const HEADER_IDX_CAPACITY = 3;
export declare const MUTEX_UNLOCKED = 0;
export declare const MUTEX_LOCKED = 1;
export declare const SENTINEL_WRAP_MARKER = 4294967295;
export declare function createSharedDepotBuffer(capacityBytes: N): SharedArrayBuffer;
export declare function mapDepotBuffer(sab: SharedArrayBuffer): HeaderViews;
export declare function validateHeader(sab: SharedArrayBuffer): B;
export declare function calculateFrameFootprint(payloadSizeBytes: N): N;
export declare class RING_BUFFER {
    private headerInt32;
    private payloadUint8;
    private payloadDataView;
    readonly capacity: N;
    constructor(sab: SharedArrayBuffer);
    private acquireLock;
    private releaseLock;
    write(p: U8): B;
    read(): U8 | null;
    waitForDataAsync(timeoutMs?: N): P<'ok' | 'not-equal' | 'timed-out'>;
}
export declare function createSharedDisruptorBuffer(capacity: N, maxPayloadBytes: N): SharedArrayBuffer;
export declare function validateDisruptorHeader(sab: SharedArrayBuffer): B;
export declare class POWER_OF_TWO {
    readonly capacity: N;
    readonly mask: N;
    readonly slotPayloadBytes: N;
    readonly pids: Int32Array;
    readonly values: Float64Array;
    readonly lengths: Int32Array;
    readonly flags: Uint8Array;
    readonly payload: Uint8Array;
    private readonly header;
    constructor(sab: SharedArrayBuffer);
    publish(pid: N, value: N, flag?: N, payload?: U8): B;
    consume(handler: (pid: N, value: N, flag: N, payload: U8) => void): N;
    waitForDataAsync(timeoutMs?: N): P<'ok' | 'not-equal' | 'timed-out'>;
    reset(): void;
}
export declare class HybridChannel {
    private readonly fast;
    private readonly overflow;
    constructor(fast: POWER_OF_TWO, overflow: RING_BUFFER);
    publish(topicId: N, payload: U8, value?: N, flag?: N): B;
    consume(handler: (topicId: N, payload: U8, value: N, flag: N) => void): N;
    drainLoop(handler: (topicId: N, payload: U8, value: N, flag: N) => void, signal?: AbortSignal): Promise<void>;
}
export declare class RingPair {
    readonly tx: HybridChannel;
    readonly rx: HybridChannel;
    readonly descriptor: RingPairDescriptor;
    private constructor();
    static create(capacity: N, maxPayloadBytes: N, overflowCapacityBytes: N): RingPair;
    static attach(descriptor: RingPairDescriptor): RingPair;
}
export declare const NATIVE_SHARED_MEMORY_BRIDGE: {
    create: (name: S, requestBytes: N, responseBytes: N, overflowBytes: N, maxPayloadBytes: N) => NativeRingPairDescriptor;
    attach: (descriptor: NativeRingPairDescriptor) => RingPair;
};
type BusSend = (msg: A, k: S, s: S, m: S, r: S) => void;
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
    constructor(localAddressSpaceId: S, k: S, s: S, m: S, r: S, onPromote: (peer: PeerRegistryEntry, pair: RingPair) => void, send: BusSend, sub: BusSub);
    announceDepot(capacity: N, maxPayloadBytes: N, overflowCapacityBytes: N): RingPair;
    private promoteRouteToDataPlane;
}
export {};
//# sourceMappingURL=RULE_OF_2_same_node.d.ts.map