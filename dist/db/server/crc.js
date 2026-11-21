const CRC_TABLE = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
    let value = i;
    for (let bit = 0; bit < 8; bit++) {
        value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
    }
    CRC_TABLE[i] = value >>> 0;
}
export function crc32(bytes, start = 0, end = bytes.length) {
    let crc = 0xffffffff;
    for (let i = start; i < end; i++) {
        crc = CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
    }
    return (crc ^ 0xffffffff) >>> 0;
}
