import {    NO_PAGE,   PAGE_FREE,    PAGE_HEADER_SIZE,    PAGE_PAYLOAD_CAPACITY,    PAGE_RECORD_CONTINUATION,    PAGE_RECORD_HEAD,    PAGE_SIZE  } from "./types";
  import { crc32 } from "./crc";
 import {    readUint32,    writeUint32  } from "./binary";
  
  export interface RecordChain {
    pageNumbers: number[];
    pages: Uint8Array[];
  }
  export function encodeRecordChain(    key: Uint8Array,    value: Uint8Array,    allocatePage: () => number  ): RecordChain {
    const payload = new Uint8Array(
      12 + key.length + value.length
    );
  
    const payloadView = new DataView(
      payload.buffer,
      payload.byteOffset,
      payload.byteLength
    );
  
    payloadView.setUint32(0, key.length, true);
    payloadView.setUint32(4, value.length, true);
    payloadView.setUint32(
      8,
      crc32(value),
      true
    );
  
    payload.set(key, 12);
    payload.set(value, 12 + key.length);
  
    const pageCount = Math.max(
      1,
      Math.ceil(payload.length / PAGE_PAYLOAD_CAPACITY)
    );
  
    const pageNumbers: number[] = [];
    const pages: Uint8Array[] = [];
  
    for (let index = 0; index < pageCount; index++) {
      const pageNumber = allocatePage();
      const page = new Uint8Array(PAGE_SIZE);
      const view = new DataView(
        page.buffer,
        page.byteOffset,
        page.byteLength
      );
  
      const payloadStart = index * PAGE_PAYLOAD_CAPACITY;
      const payloadEnd = Math.min(
        payload.length,
        payloadStart + PAGE_PAYLOAD_CAPACITY
      );
  
      const used = payloadEnd - payloadStart;
      const nextPage =
        index === pageCount - 1
          ? NO_PAGE
          : NO_PAGE; // patched after next page is allocated
  
      pageNumbers.push(pageNumber);
      pages.push(page);
  
      view.setUint32(
        0,
        index === 0 ? PAGE_RECORD_HEAD : PAGE_RECORD_CONTINUATION,
        true
      );
  
      view.setUint32(4, nextPage, true);
      view.setUint32(8, used, true);
      view.setUint32(
        12,
        crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used),
        true
      );
  
      page.set(
        payload.subarray(payloadStart, payloadEnd),
        PAGE_HEADER_SIZE
      );
    }
  
    for (let index = 0; index < pageCount - 1; index++) {
      const page = pages[index]!;
      const view = new DataView(
        page.buffer,
        page.byteOffset,
        page.byteLength
      );
  
      view.setUint32(4, pageNumbers[index + 1]!, true);
      view.setUint32(
        12,
        crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + view.getUint32(8, true)),
        true
      );
    }
  
    return {
      pageNumbers,
      pages
    };
  }
  export interface DecodedRecord {
    key: Uint8Array;
    value: Uint8Array;
  }
  export function decodeRecordChain(    readPage: (pageNumber: number) => Uint8Array | undefined,    headPageNumber: number  ): DecodedRecord | undefined {
    const chunks: Uint8Array[] = [];
    let pageNumber: number | undefined = headPageNumber;
    let isHead = true;
  
    while (
      pageNumber !== undefined &&
      pageNumber !== NO_PAGE
    ) {
      const page = readPage(pageNumber);
  
      if (!page) return undefined;
  
      const view = new DataView(
        page.buffer,
        page.byteOffset,
        page.byteLength
      );
  
      const state = view.getUint32(0, true);
      const nextPage = view.getUint32(4, true);
      const used = view.getUint32(8, true);
      const checksum = view.getUint32(12, true);
  
      const expectedState = isHead
        ? PAGE_RECORD_HEAD
        : PAGE_RECORD_CONTINUATION;
  
      if (
        state !== expectedState ||
        used > PAGE_PAYLOAD_CAPACITY ||
        checksum !== crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)
      ) {
        return undefined;
      }
  
      chunks.push(
        page.subarray(PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)
      );
  
      pageNumber = nextPage === NO_PAGE ? undefined : nextPage;
      isHead = false;
    }
  
    if (chunks.length === 0) return undefined;
  
    const payloadLength = chunks.reduce(
      (total, chunk) => total + chunk.length,
      0
    );
  
    const payload = new Uint8Array(payloadLength);
    let offset = 0;
  
    for (const chunk of chunks) {
      payload.set(chunk, offset);
      offset += chunk.length;
    }
  
    if (payload.length < 12) return undefined;
  
    const payloadView = new DataView(
      payload.buffer,
      payload.byteOffset,
      payload.byteLength
    );
  
    const keyLength = payloadView.getUint32(0, true);
    const valueLength = payloadView.getUint32(4, true);
    const valueChecksum = payloadView.getUint32(8, true);
  
    const key = payload.subarray(12, 12 + keyLength);
    const value = payload.subarray(
      12 + keyLength,
      12 + keyLength + valueLength
    );
  
    if (value.length !== valueLength) return undefined;
    if (crc32(value) !== valueChecksum) return undefined;
  
    return {
      key,
      value
    };
  }
  export function encodeFreePage(): Uint8Array {
    const page = new Uint8Array(PAGE_SIZE);
    const view = new DataView(
      page.buffer,
      page.byteOffset,
      page.byteLength
    );
  
    view.setUint32(0, PAGE_FREE, true);
    return page;
  }
