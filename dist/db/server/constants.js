// src/constants.ts
export const PAGE_SIZE = 4096;
export const RECORD_HEADER_SIZE = 16;
export const MAX_INLINE_VALUE_SIZE = PAGE_SIZE - RECORD_HEADER_SIZE - 1024;
export const RECORD_ACTIVE = 1;
export const RECORD_FREE = 0;
export const OP_PUT = 1;
export const OP_DELETE = 2;
