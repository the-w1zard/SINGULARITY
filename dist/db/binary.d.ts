export declare function encodeUtf8(value: string): Uint8Array;
export declare function decodeUtf8(bytes: Uint8Array): string;
export declare function concatBytes(parts: Uint8Array[]): Uint8Array;
export declare function readUint32(bytes: Uint8Array, offset: number): number;
export declare function writeUint32(bytes: Uint8Array, offset: number, value: number): void;
export declare function readBigUint64(bytes: Uint8Array, offset: number): bigint;
export declare function writeBigUint64(bytes: Uint8Array, offset: number, value: bigint): void;
export declare function crc32(bytes: Uint8Array, start?: number, end?: number): number;
export type AvroWriteFn = (w: FastAvroWriter, v: A) => V;
export type AvroReadFn = (r: FastAvroReader) => A;
export type AVROField = {
    name: S;
    type: AVROSchema;
};
export type AVROSchema = S | AVROSchema[] | {
    type?: S | AVROSchema[];
    name?: S;
    namespace?: S;
    fields?: AVROField[];
    items?: AVROSchema;
    values?: AVROSchema;
    symbols?: AR<S>;
    size?: N;
};
export declare class FastAvroWriter {
    buf: Buffer;
    pos: N;
    reset(): V;
    private ensure;
    byte(v: N): V;
    bytes(v: Uint8Array): V;
    long(value: N): V;
    float(value: N): V;
    double(value: N): V;
    string(v: S): V;
    finish(): Buffer;
}
export declare class FastAvroReader {
    private readonly buf;
    pos: N;
    constructor(buf: Buffer);
    byte(): N;
    bytes(length: N): Buffer;
    long(): N;
    float(): N;
    double(): N;
    string(): S;
    done(): B;
}
export declare class BINARY_AVRO_CODEGEN {
    private readonly names;
    private readonly writeRoot;
    private readonly readRoot;
    private readonly writeError;
    private readonly readError;
    private readonly writer;
    private readonly errorSchema;
    constructor(schema: AVROSchema);
    private collect;
    private resolve;
    private kindOf;
    private genMatch;
    private genWrite;
    private genRead;
    encode_msg(msg: AVROValue): Buffer;
    decode_msg(raw: A): AVROValue;
}
export declare const SAFE_ZIGZAG_LIMIT: number;
export declare const SIG_BUS_SCHEMA: AVROSchema;
export declare const WAL_KVDB_SCHEMA: AVROSchema;
export declare const KV_PAYLOAD_SCHEMA: AVROSchema;
export declare const ERROR_INFO_SCHEMA: AVROSchema;
export declare const AVRO_ERROR_SCHEMA: AVROSchema;
export declare const ENVELOPE_CODEC: BINARY_AVRO_CODEGEN;
export declare const PAYLOAD_CODEC: BINARY_AVRO_CODEGEN;
//# sourceMappingURL=binary.d.ts.map