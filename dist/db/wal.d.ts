export interface Manifest {
    checkpointSequence: bigint;
    pageCount: number;
}
export declare function encodeManifest(manifest: Manifest): Uint8Array;
export declare function decodeManifest(bytes: Uint8Array): Manifest | undefined;
export interface WalRecord {
    sequence: bigint;
    operation: number;
    key: Uint8Array;
    value: Uint8Array;
}
export declare function encodeWalRecord(record: WalRecord): Uint8Array;
export interface DecodedWalResult {
    records: WalRecord[];
    validBytes: number;
}
export declare function decodeWalRecords(bytes: Uint8Array, minimumSequence: bigint): DecodedWalResult;
export interface RecordChain {
    pageNumbers: number[];
    pages: Uint8Array[];
}
export declare function encodeRecordChain(key: Uint8Array, value: Uint8Array, allocatePage: () => number): RecordChain;
export interface DecodedRecord {
    key: Uint8Array;
    value: Uint8Array;
}
export declare function decodeRecordChain(readPage: (pageNumber: number) => Uint8Array | undefined, headPageNumber: number): DecodedRecord | undefined;
export declare function encodeFreePage(): Uint8Array;
//# sourceMappingURL=wal.d.ts.map