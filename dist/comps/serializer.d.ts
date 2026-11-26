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
export declare class BinaryEncoder {
    private static topicToIdMap;
    private static idToTopicMap;
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
    static encodeFrame(topic: string, p: Uint8Array | string | Record<string, unknown>): Uint8Array;
    static decodeFrame(frameBytes: Uint8Array): DecodedFrame;
}
//# sourceMappingURL=serializer.d.ts.map