export declare const PAGE_SIZE = 4096;
export declare const PAGE_HEADER_SIZE = 24;
export declare const PAGE_PAYLOAD_CAPACITY: number;
export declare const NO_PAGE = 4294967295;
export declare const PAGE_FREE = 0;
export declare const PAGE_RECORD_HEAD = 1;
export declare const PAGE_RECORD_CONTINUATION = 2;
export declare const OP_PUT = 1;
export declare const OP_DELETE = 2;
export interface StoredEntry {
    pageNumber: number;
    pageNumbers: number[];
}
export interface PendingWorkerRequest {
    resolve: (response: WorkerResponse) => void;
    reject: (error: Error) => void;
}
export interface PendingBatch {
    batchId: string;
    records: Uint8Array[];
    byteLength: number;
    durable: boolean;
    waiters: Array<() => void>;
}
export declare const WAL_VERSION = 1;
export declare const MANIFEST_VERSION = 1;
export type WorkerRequestInput = Omit<Extract<WorkerRequest, {
    kind: "init";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "readPages";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "appendWal";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "checkpoint";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "compactBegin";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "compactWrite";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "compactCommit";
}>, "id"> | Omit<Extract<WorkerRequest, {
    kind: "close";
}>, "id">;
export type Durability = "buffered" | "group" | "strict";
export type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;
export type WorkerRequestWithoutId = DistributiveOmit<WorkerRequest, "id">;
export interface FastKVOptions {
    directory: string;
    /**
     * buffered: WAL write is queued; no fsync.
     * group: every WAL batch is fsynced before its waiters resolve.
     * strict: each durable write waits for the fsync of the batch containing it.
     */
    durability: Durability;
    walBatchBytes: number;
    walBatchRecords: number;
    walFlushIntervalMs: number;
    walHighWaterBytes: number;
    checkpointIntervalMs: number;
}
export interface PageWrite {
    pageNumber: number;
    buffer: Uint8Array;
}
export type WorkerRequest = {
    kind: "init";
    id: string;
    dataPath: string;
    walPath: string;
} | {
    kind: "readPages";
    id: string;
    pageNumbers: number[];
} | {
    kind: "appendWal";
    id: string;
    batchId: string;
    bytes: Uint8Array;
    durable: boolean;
} | {
    kind: "checkpoint";
    id: string;
    pages: PageWrite[];
    manifest: Uint8Array;
} | {
    kind: "compactBegin";
    id: string;
    compactPath: string;
} | {
    kind: "compactWrite";
    id: string;
    pages: PageWrite[];
} | {
    kind: "compactCommit";
    id: string;
    dataPath: string;
    compactPath: string;
    manifest: Uint8Array;
    walPath: string;
} | {
    kind: "close";
    id: string;
};
export type WorkerResponse = {
    kind: "walOk";
    id: string;
    batchId: string;
    walSize: number;
} | {
    kind: "readPagesOk";
    id: string;
    pages: Uint8Array[];
} | {
    kind: "checkpointOk";
    id: string;
} | {
    kind: "compactOk";
    id: string;
} | {
    kind: "initOk";
    id: string;
} | {
    kind: "closeOk";
    id: string;
} | {
    kind: "error";
    id: string;
    error: string;
};
export type WorkerErrorResponse = Extract<WorkerResponse, {
    kind: "error";
}>;
export declare function isWorkerError(response: WorkerResponse): response is WorkerErrorResponse;
//# sourceMappingURL=types.d.ts.map