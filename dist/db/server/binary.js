export function encodeUtf8(value) {
    return new TextEncoder().encode(value);
}
export function decodeUtf8(bytes) {
    return new TextDecoder().decode(bytes);
}
export function concatBytes(parts) {
    let length = 0;
    for (const part of parts) {
        length += part.length;
    }
    const output = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
        output.set(part, offset);
        offset += part.length;
    }
    return output;
}
export function readUint32(bytes, offset) {
    return (bytes[offset] |
        (bytes[offset + 1] << 8) |
        (bytes[offset + 2] << 16) |
        (bytes[offset + 3] << 24)) >>> 0;
}
export function writeUint32(bytes, offset, value) {
    bytes[offset] = value & 0xff;
    bytes[offset + 1] = (value >>> 8) & 0xff;
    bytes[offset + 2] = (value >>> 16) & 0xff;
    bytes[offset + 3] = (value >>> 24) & 0xff;
}
export function readBigUint64(bytes, offset) {
    let value = 0n;
    for (let i = 7; i >= 0; i--) {
        value = (value << 8n) | BigInt(bytes[offset + i]);
    }
    return value;
}
export function writeBigUint64(bytes, offset, value) {
    for (let i = 0; i < 8; i++) {
        bytes[offset + i] = Number((value >> BigInt(i * 8)) & 0xffn);
    }
}
