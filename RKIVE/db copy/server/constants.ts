
// src/constants.ts
export const PAGE_SIZE = 4096;
export const RECORD_HEADER_SIZE = 16;
export const MAX_INLINE_VALUE_SIZE =
  PAGE_SIZE - RECORD_HEADER_SIZE - 1024;

export const RECORD_ACTIVE = 1;
export const RECORD_FREE = 0;

export const OP_PUT = 1;
export const OP_DELETE = 2;

export interface StorageOptions {
  directory: string;
  durable: boolean;
  walHighWaterBytes: number;
}

export interface PageWrite {
  pageNumber: number;
  buffer: Uint8Array;
}

export interface WorkerRequest {
  id: string;
  type: 'init' | 'append' | 'checkpoint' | 'close';
  dataPath?: string;
  walPath?: string;
  record?: Uint8Array;
  pages?: PageWrite[];
  durable?: boolean;
}

export interface WorkerResponse {
  id: string;
  ok: boolean;
  walSize?: number;
  error?: string;
}
