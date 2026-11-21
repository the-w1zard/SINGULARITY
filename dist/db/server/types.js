export const PAGE_SIZE = 4096;
export const PAGE_HEADER_SIZE = 24;
export const PAGE_PAYLOAD_CAPACITY = PAGE_SIZE - PAGE_HEADER_SIZE;
export const NO_PAGE = 0xffffffff;
export const PAGE_FREE = 0;
export const PAGE_RECORD_HEAD = 1;
export const PAGE_RECORD_CONTINUATION = 2;
export const OP_PUT = 1;
export const OP_DELETE = 2;
export const WAL_VERSION = 1;
export const MANIFEST_VERSION = 1;
export function isWorkerError(response) {
    return response.kind === "error";
}
