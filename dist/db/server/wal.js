import { WAL_VERSION } from "./types";
import { crc32 } from "./crc";
import { readUint32 } from "./binary";
export function encodeWalRecord(record) {
    const body = new Uint8Array(1 +
        1 +
        8 +
        4 +
        4 +
        record.key.length +
        record.value.length);
    const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
    let offset = 0;
    view.setUint8(offset, WAL_VERSION);
    offset += 1;
    view.setUint8(offset, record.operation);
    offset += 1;
    view.setBigUint64(offset, record.sequence, true);
    offset += 8;
    view.setUint32(offset, record.key.length, true);
    offset += 4;
    view.setUint32(offset, record.value.length, true);
    offset += 4;
    body.set(record.key, offset);
    offset += record.key.length;
    body.set(record.value, offset);
    const frame = new Uint8Array(4 + body.length + 4);
    const frameView = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    frameView.setUint32(0, body.length, true);
    frame.set(body, 4);
    frameView.setUint32(4 + body.length, crc32(frame, 4, 4 + body.length), true);
    return frame;
}
export function decodeWalRecords(bytes, minimumSequence) {
    const records = [];
    let offset = 0;
    while (offset + 4 <= bytes.length) {
        const bodyLength = readUint32(bytes, offset);
        const frameEnd = offset + 4 + bodyLength + 4;
        if (bodyLength < 18 ||
            frameEnd > bytes.length) {
            break;
        }
        const storedChecksum = readUint32(bytes, offset + 4 + bodyLength);
        const actualChecksum = crc32(bytes, offset + 4, offset + 4 + bodyLength);
        if (storedChecksum !== actualChecksum) {
            break;
        }
        const body = bytes.subarray(offset + 4, offset + 4 + bodyLength);
        const bodyView = new DataView(body.buffer, body.byteOffset, body.byteLength);
        const version = bodyView.getUint8(0);
        if (version !== WAL_VERSION) {
            break;
        }
        const operation = bodyView.getUint8(1);
        const sequence = bodyView.getBigUint64(2, true);
        const keyLength = bodyView.getUint32(10, true);
        const valueLength = bodyView.getUint32(14, true);
        if (sequence <= minimumSequence) {
            offset = frameEnd;
            continue;
        }
        const keyStart = 18;
        const valueStart = keyStart + keyLength;
        const valueEnd = valueStart + valueLength;
        if (valueEnd > body.length) {
            break;
        }
        records.push({
            sequence,
            operation,
            key: body.subarray(keyStart, valueStart),
            value: body.subarray(valueStart, valueEnd)
        });
        offset = frameEnd;
    }
    return {
        records,
        validBytes: offset
    };
}
