import {
    MANIFEST_VERSION
  } from "./types";
  import { crc32 } from "./crc";
  import {
    readBigUint64,
    readUint32,
    writeBigUint64,
    writeUint32
  } from "./binary";
  
  const MANIFEST_MAGIC = 0x464b5631; // FKV1
  const MANIFEST_SIZE = 24;
  
  export interface Manifest {
    checkpointSequence: bigint;
    pageCount: number;
  }
  
  export function encodeManifest(
    manifest: Manifest
  ): Uint8Array {
    const bytes = new Uint8Array(MANIFEST_SIZE);
    const view = new DataView(
      bytes.buffer,
      bytes.byteOffset,
      bytes.byteLength
    );
  
    view.setUint32(0, MANIFEST_MAGIC, true);
    view.setUint32(4, MANIFEST_VERSION, true);
    writeBigUint64(bytes, 8, manifest.checkpointSequence);
    writeUint32(bytes, 16, manifest.pageCount);
    writeUint32(bytes, 20, crc32(bytes, 0, 20));
  
    return bytes;
  }
  
  export function decodeManifest(
    bytes: Uint8Array
  ): Manifest | undefined {
    if (bytes.length < MANIFEST_SIZE) return undefined;
  
    const view = new DataView(
      bytes.buffer,
      bytes.byteOffset,
      bytes.byteLength
    );
  
    if (view.getUint32(0, true) !== MANIFEST_MAGIC) {
      return undefined;
    }
  
    if (view.getUint32(4, true) !== MANIFEST_VERSION) {
      return undefined;
    }
  
    const storedChecksum = readUint32(bytes, 20);
  
    if (storedChecksum !== crc32(bytes, 0, 20)) {
      return undefined;
    }
  
    return {
      checkpointSequence: readBigUint64(bytes, 8),
      pageCount: readUint32(bytes, 16)
    };
  }
